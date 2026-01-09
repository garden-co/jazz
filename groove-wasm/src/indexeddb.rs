//! IndexedDB-backed Environment for browser persistence.
//!
//! This module provides an Environment implementation that stores all data
//! in the browser's IndexedDB, enabling persistent local-first storage.

use async_trait::async_trait;
use bytes::Bytes;
use futures::stream::{self, BoxStream, StreamExt};
use groove::{ChunkHash, ChunkStore, CommitId, CommitStore, ObjectId, SyncStateStore};
use js_sys::{Array, Uint8Array};
use std::cell::RefCell;
use std::rc::Rc;
use wasm_bindgen::prelude::*;
use web_sys::{
    IdbDatabase, IdbFactory, IdbObjectStore, IdbOpenDbRequest, IdbRequest, IdbTransaction,
    IdbTransactionMode,
};

const DB_NAME: &str = "groove";
const DB_VERSION: u32 = 3;

// Object store names
const CHUNKS_STORE: &str = "chunks";
const COMMITS_STORE: &str = "commits";
const FRONTIERS_STORE: &str = "frontiers";
const TRUNCATIONS_STORE: &str = "truncations";
const OBJECTS_STORE: &str = "objects";
const METADATA_STORE: &str = "metadata";
const UNSYNCED_STORE: &str = "unsynced";

// Metadata keys
const CATALOG_ID_KEY: &str = "catalog_id";

/// IndexedDB-backed Environment for browser persistence.
///
/// Stores chunks, commits, frontiers, and truncations in IndexedDB object stores.
/// This enables full database persistence across browser sessions.
pub struct IndexedDbEnvironment {
    db: Rc<RefCell<Option<IdbDatabase>>>,
    db_name: String,
}

impl std::fmt::Debug for IndexedDbEnvironment {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("IndexedDbEnvironment")
            .field("db_name", &self.db_name)
            .field("connected", &self.db.borrow().is_some())
            .finish()
    }
}

impl IndexedDbEnvironment {
    /// Create a new IndexedDB environment with the default database name.
    pub async fn new() -> Result<Self, JsValue> {
        Self::with_name(DB_NAME).await
    }

    /// Create a new IndexedDB environment with a custom database name.
    pub async fn with_name(name: &str) -> Result<Self, JsValue> {
        let env = Self {
            db: Rc::new(RefCell::new(None)),
            db_name: name.to_string(),
        };
        env.open().await?;
        Ok(env)
    }

    /// Open (or create) the IndexedDB database.
    async fn open(&self) -> Result<(), JsValue> {
        let window = web_sys::window().ok_or_else(|| JsValue::from_str("no window"))?;
        let idb: IdbFactory = window
            .indexed_db()?
            .ok_or_else(|| JsValue::from_str("IndexedDB not available"))?;

        let open_request: IdbOpenDbRequest = idb.open_with_u32(&self.db_name, DB_VERSION)?;

        // Set up upgrade handler for creating object stores
        let on_upgrade = Closure::once(Box::new(|event: web_sys::IdbVersionChangeEvent| {
            let target = event.target().unwrap();
            let request: IdbOpenDbRequest = target.dyn_into().unwrap();
            let db: IdbDatabase = request.result().unwrap().dyn_into().unwrap();

            // Create object stores if they don't exist
            if !db.object_store_names().contains(CHUNKS_STORE) {
                db.create_object_store(CHUNKS_STORE).unwrap();
            }
            if !db.object_store_names().contains(COMMITS_STORE) {
                db.create_object_store(COMMITS_STORE).unwrap();
            }
            if !db.object_store_names().contains(FRONTIERS_STORE) {
                db.create_object_store(FRONTIERS_STORE).unwrap();
            }
            if !db.object_store_names().contains(TRUNCATIONS_STORE) {
                db.create_object_store(TRUNCATIONS_STORE).unwrap();
            }
            if !db.object_store_names().contains(OBJECTS_STORE) {
                db.create_object_store(OBJECTS_STORE).unwrap();
            }
            if !db.object_store_names().contains(METADATA_STORE) {
                db.create_object_store(METADATA_STORE).unwrap();
            }
            if !db.object_store_names().contains(UNSYNCED_STORE) {
                db.create_object_store(UNSYNCED_STORE).unwrap();
            }
        }) as Box<dyn FnOnce(_)>);

        open_request.set_onupgradeneeded(Some(on_upgrade.as_ref().unchecked_ref()));
        on_upgrade.forget(); // prevent closure from being dropped

        // Wait for the database to open
        let db = Self::await_request(&open_request).await?;
        let db: IdbDatabase = db.dyn_into()?;

        *self.db.borrow_mut() = Some(db);
        Ok(())
    }

    /// Get the database, panics if not connected.
    fn db(&self) -> std::cell::Ref<'_, IdbDatabase> {
        std::cell::Ref::map(self.db.borrow(), |opt| {
            opt.as_ref().expect("database not connected")
        })
    }

    /// Helper to await an IDB request.
    async fn await_request(request: &IdbRequest) -> Result<JsValue, JsValue> {
        let (tx, rx) = futures::channel::oneshot::channel();
        let tx = Rc::new(RefCell::new(Some(tx)));

        let tx_success = tx.clone();
        let on_success = Closure::once(Box::new(move |_event: web_sys::Event| {
            if let Some(tx) = tx_success.borrow_mut().take() {
                let _ = tx.send(Ok(()));
            }
        }) as Box<dyn FnOnce(_)>);

        let tx_error = tx;
        let on_error = Closure::once(Box::new(move |_event: web_sys::Event| {
            if let Some(tx) = tx_error.borrow_mut().take() {
                let _ = tx.send(Err(()));
            }
        }) as Box<dyn FnOnce(_)>);

        request.set_onsuccess(Some(on_success.as_ref().unchecked_ref()));
        request.set_onerror(Some(on_error.as_ref().unchecked_ref()));

        on_success.forget();
        on_error.forget();

        rx.await
            .map_err(|_| JsValue::from_str("channel closed"))?
            .map_err(|_| request.error().unwrap_or_else(|_| None).map_or_else(
                || JsValue::from_str("unknown IDB error"),
                |e| e.into(),
            ))?;

        request.result()
    }

    /// Start a transaction on the given stores.
    fn transaction(&self, stores: &[&str], mode: IdbTransactionMode) -> Result<IdbTransaction, JsValue> {
        let db = self.db();
        let store_names = Array::new();
        for store in stores {
            store_names.push(&JsValue::from_str(store));
        }
        db.transaction_with_str_sequence_and_mode(&store_names, mode)
    }

    /// Get an object store from a transaction.
    fn object_store(tx: &IdbTransaction, name: &str) -> Result<IdbObjectStore, JsValue> {
        tx.object_store(name)
    }

    /// Encode a ChunkHash as a base64 string key.
    fn chunk_hash_to_key(hash: &ChunkHash) -> String {
        base64_encode(hash.as_bytes())
    }

    /// Decode a ChunkHash from a base64 string key.
    fn key_to_chunk_hash(key: &str) -> Option<ChunkHash> {
        let bytes = base64_decode(key)?;
        if bytes.len() == 32 {
            let mut arr = [0u8; 32];
            arr.copy_from_slice(&bytes);
            Some(ChunkHash::from_bytes(arr))
        } else {
            None
        }
    }

    /// Encode a CommitId as a base64 string key.
    fn commit_id_to_key(id: &CommitId) -> String {
        base64_encode(id.as_bytes())
    }

    /// Decode a CommitId from a base64 string key.
    fn key_to_commit_id(key: &str) -> Option<CommitId> {
        let bytes = base64_decode(key)?;
        if bytes.len() == 32 {
            let mut arr = [0u8; 32];
            arr.copy_from_slice(&bytes);
            Some(CommitId::from_bytes(arr))
        } else {
            None
        }
    }

    /// Encode a frontier/truncation key as "object_id:branch".
    fn branch_key(object_id: u128, branch: &str) -> String {
        format!("{}:{}", object_id, branch)
    }

    /// Encode an ObjectId as a string key for IndexedDB.
    fn object_id_to_key(id: &ObjectId) -> String {
        id.to_string()
    }

    /// Decode an ObjectId from a string key.
    fn key_to_object_id(key: &str) -> Option<ObjectId> {
        key.parse().ok()
    }

    /// Decode a branch key to (object_id, branch).
    fn parse_branch_key(key: &str) -> Option<(u128, String)> {
        let parts: Vec<&str> = key.splitn(2, ':').collect();
        if parts.len() == 2 {
            let object_id: u128 = parts[0].parse().ok()?;
            Some((object_id, parts[1].to_string()))
        } else {
            None
        }
    }

    /// Get the stored catalog ObjectId, if any.
    /// Returns None if no database has been initialized yet.
    pub async fn get_catalog_id(&self) -> Option<String> {
        let tx = self
            .transaction(&[METADATA_STORE], IdbTransactionMode::Readonly)
            .ok()?;
        let store = Self::object_store(&tx, METADATA_STORE).ok()?;

        let key = JsValue::from_str(CATALOG_ID_KEY);
        let request = store.get(&key).ok()?;
        let result = Self::await_request(&request).await.ok()?;

        if result.is_undefined() || result.is_null() {
            return None;
        }

        result.as_string()
    }

    /// Store the catalog ObjectId.
    pub async fn set_catalog_id(&self, catalog_id: &str) -> Result<(), JsValue> {
        let tx = self
            .transaction(&[METADATA_STORE], IdbTransactionMode::Readwrite)
            .expect("failed to start transaction");
        let store = Self::object_store(&tx, METADATA_STORE).expect("failed to get store");

        let key = JsValue::from_str(CATALOG_ID_KEY);
        let value = JsValue::from_str(catalog_id);
        let request = store.put_with_key(&value, &key).expect("failed to put");
        Self::await_request(&request).await?;

        Ok(())
    }

    /// Check if a database already exists (has a stored catalog ID).
    pub async fn has_database(&self) -> bool {
        self.get_catalog_id().await.is_some()
    }
}

// Simple base64 encoding/decoding (avoiding external deps)
fn base64_encode(data: &[u8]) -> String {
    const ALPHABET: &[u8] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
    let mut result = String::new();
    for chunk in data.chunks(3) {
        let b0 = chunk[0] as usize;
        let b1 = chunk.get(1).copied().unwrap_or(0) as usize;
        let b2 = chunk.get(2).copied().unwrap_or(0) as usize;

        result.push(ALPHABET[b0 >> 2] as char);
        result.push(ALPHABET[((b0 & 0x03) << 4) | (b1 >> 4)] as char);

        if chunk.len() > 1 {
            result.push(ALPHABET[((b1 & 0x0f) << 2) | (b2 >> 6)] as char);
        } else {
            result.push('=');
        }

        if chunk.len() > 2 {
            result.push(ALPHABET[b2 & 0x3f] as char);
        } else {
            result.push('=');
        }
    }
    result
}

fn base64_decode(s: &str) -> Option<Vec<u8>> {
    const DECODE: [i8; 128] = [
        -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1,
        -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1,
        -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, 62, -1, -1, -1, 63,
        52, 53, 54, 55, 56, 57, 58, 59, 60, 61, -1, -1, -1, -1, -1, -1,
        -1,  0,  1,  2,  3,  4,  5,  6,  7,  8,  9, 10, 11, 12, 13, 14,
        15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, -1, -1, -1, -1, -1,
        -1, 26, 27, 28, 29, 30, 31, 32, 33, 34, 35, 36, 37, 38, 39, 40,
        41, 42, 43, 44, 45, 46, 47, 48, 49, 50, 51, -1, -1, -1, -1, -1,
    ];

    let s = s.trim_end_matches('=');
    let mut result = Vec::new();

    for chunk in s.as_bytes().chunks(4) {
        let mut buf = [0u8; 4];
        for (i, &c) in chunk.iter().enumerate() {
            if c >= 128 {
                return None;
            }
            let val = DECODE[c as usize];
            if val < 0 {
                return None;
            }
            buf[i] = val as u8;
        }

        result.push((buf[0] << 2) | (buf[1] >> 4));
        if chunk.len() > 2 {
            result.push((buf[1] << 4) | (buf[2] >> 2));
        }
        if chunk.len() > 3 {
            result.push((buf[2] << 6) | buf[3]);
        }
    }

    Some(result)
}

#[async_trait(?Send)]
impl ChunkStore for IndexedDbEnvironment {
    async fn get_chunk(&self, hash: &ChunkHash) -> Option<Bytes> {
        let tx = self
            .transaction(&[CHUNKS_STORE], IdbTransactionMode::Readonly)
            .ok()?;
        let store = Self::object_store(&tx, CHUNKS_STORE).ok()?;

        let key = JsValue::from_str(&Self::chunk_hash_to_key(hash));
        let request = store.get(&key).ok()?;
        let result = Self::await_request(&request).await.ok()?;

        if result.is_undefined() || result.is_null() {
            return None;
        }

        let array: Uint8Array = result.dyn_into().ok()?;
        Some(Bytes::from(array.to_vec()))
    }

    async fn put_chunk(&self, data: Bytes) -> ChunkHash {
        let hash = ChunkHash::compute(&data);

        let tx = self
            .transaction(&[CHUNKS_STORE], IdbTransactionMode::Readwrite)
            .expect("failed to start transaction");
        let store = Self::object_store(&tx, CHUNKS_STORE).expect("failed to get store");

        let key = JsValue::from_str(&Self::chunk_hash_to_key(&hash));
        let value = Uint8Array::from(data.as_ref());
        let request = store.put_with_key(&value, &key).expect("failed to put");
        let _ = Self::await_request(&request).await;

        hash
    }

    async fn has_chunk(&self, hash: &ChunkHash) -> bool {
        self.get_chunk(hash).await.is_some()
    }
}

#[async_trait(?Send)]
impl CommitStore for IndexedDbEnvironment {
    async fn get_commit_meta(&self, id: &CommitId) -> Option<groove::CommitMeta> {
        // For now, just get the full commit and extract meta
        let commit = self.get_commit(id).await?;
        Some(groove::CommitMeta {
            id: commit.compute_id(),
            parents: commit.parents.clone(),
            author: commit.author.clone(),
            timestamp: commit.timestamp,
            content_size: commit.content.len(),
        })
    }

    async fn get_commit(&self, id: &CommitId) -> Option<groove::Commit> {
        let tx = self
            .transaction(&[COMMITS_STORE], IdbTransactionMode::Readonly)
            .ok()?;
        let store = Self::object_store(&tx, COMMITS_STORE).ok()?;

        let key = JsValue::from_str(&Self::commit_id_to_key(id));
        let request = store.get(&key).ok()?;
        let result = Self::await_request(&request).await.ok()?;

        if result.is_undefined() || result.is_null() {
            return None;
        }

        let array: Uint8Array = result.dyn_into().ok()?;
        let bytes = array.to_vec();
        deserialize_commit(&bytes)
    }

    async fn put_commit(&self, commit: &groove::Commit) -> CommitId {
        let id = commit.compute_id();
        let bytes = serialize_commit(commit);

        let tx = self
            .transaction(&[COMMITS_STORE, OBJECTS_STORE], IdbTransactionMode::Readwrite)
            .expect("failed to start transaction");

        // Store commit
        let store = Self::object_store(&tx, COMMITS_STORE).expect("failed to get store");
        let key = JsValue::from_str(&Self::commit_id_to_key(&id));
        let value = Uint8Array::from(bytes.as_slice());
        let request = store.put_with_key(&value, &key).expect("failed to put");
        let _ = Self::await_request(&request).await;

        id
    }

    async fn get_frontier(&self, object_id: u128, branch: &str) -> Vec<CommitId> {
        let tx = match self.transaction(&[FRONTIERS_STORE], IdbTransactionMode::Readonly) {
            Ok(tx) => tx,
            Err(_) => return vec![],
        };
        let store = match Self::object_store(&tx, FRONTIERS_STORE) {
            Ok(s) => s,
            Err(_) => return vec![],
        };

        let key = JsValue::from_str(&Self::branch_key(object_id, branch));
        let request = match store.get(&key) {
            Ok(r) => r,
            Err(_) => return vec![],
        };
        let result = match Self::await_request(&request).await {
            Ok(r) => r,
            Err(_) => return vec![],
        };

        if result.is_undefined() || result.is_null() {
            return vec![];
        }

        let array: Uint8Array = match result.dyn_into() {
            Ok(a) => a,
            Err(_) => return vec![],
        };
        let bytes = array.to_vec();
        deserialize_commit_ids(&bytes)
    }

    async fn set_frontier(&self, object_id: u128, branch: &str, frontier: &[CommitId]) {
        let tx = self
            .transaction(&[FRONTIERS_STORE, OBJECTS_STORE], IdbTransactionMode::Readwrite)
            .expect("failed to start transaction");

        // Store frontier
        let store = Self::object_store(&tx, FRONTIERS_STORE).expect("failed to get store");
        let key = JsValue::from_str(&Self::branch_key(object_id, branch));
        let bytes = serialize_commit_ids(frontier);
        let value = Uint8Array::from(bytes.as_slice());
        let request = store.put_with_key(&value, &key).expect("failed to put");
        let _ = Self::await_request(&request).await;

        // Track object existence
        let obj_store = Self::object_store(&tx, OBJECTS_STORE).expect("failed to get store");
        let obj_key = JsValue::from_str(&object_id.to_string());
        let obj_value = Uint8Array::new_with_length(0); // empty value, just tracking key
        let _ = obj_store.put_with_key(&obj_value, &obj_key);
    }

    async fn get_truncation(&self, object_id: u128, branch: &str) -> Option<CommitId> {
        let tx = self
            .transaction(&[TRUNCATIONS_STORE], IdbTransactionMode::Readonly)
            .ok()?;
        let store = Self::object_store(&tx, TRUNCATIONS_STORE).ok()?;

        let key = JsValue::from_str(&Self::branch_key(object_id, branch));
        let request = store.get(&key).ok()?;
        let result = Self::await_request(&request).await.ok()?;

        if result.is_undefined() || result.is_null() {
            return None;
        }

        let array: Uint8Array = result.dyn_into().ok()?;
        let bytes = array.to_vec();
        if bytes.len() == 32 {
            let mut arr = [0u8; 32];
            arr.copy_from_slice(&bytes);
            Some(CommitId::from_bytes(arr))
        } else {
            None
        }
    }

    async fn set_truncation(&self, object_id: u128, branch: &str, truncation: Option<CommitId>) {
        let tx = self
            .transaction(&[TRUNCATIONS_STORE], IdbTransactionMode::Readwrite)
            .expect("failed to start transaction");
        let store = Self::object_store(&tx, TRUNCATIONS_STORE).expect("failed to get store");

        let key = JsValue::from_str(&Self::branch_key(object_id, branch));

        if let Some(commit_id) = truncation {
            let value = Uint8Array::from(commit_id.as_bytes().as_slice());
            let request = store.put_with_key(&value, &key).expect("failed to put");
            let _ = Self::await_request(&request).await;
        } else {
            let request = store.delete(&key).expect("failed to delete");
            let _ = Self::await_request(&request).await;
        }
    }

    fn list_commits(&self, object_id: u128, branch: &str) -> BoxStream<'_, CommitId> {
        // For now, return an empty stream - we'd need to track commits per object/branch
        // In practice, commits are discovered by walking from the frontier
        let _ = (object_id, branch);
        stream::empty().boxed()
    }

    fn list_objects(&self) -> BoxStream<'_, u128> {
        // This would require async iteration over IndexedDB
        // For now, we implement it by collecting all keys synchronously
        // In practice, this is only called during enumeration
        stream::empty().boxed()
    }

    async fn list_branches(&self, object_id: u128) -> Vec<String> {
        // Scan frontiers store for keys starting with object_id
        let tx = match self.transaction(&[FRONTIERS_STORE], IdbTransactionMode::Readonly) {
            Ok(tx) => tx,
            Err(_) => return vec![],
        };
        let store = match Self::object_store(&tx, FRONTIERS_STORE) {
            Ok(s) => s,
            Err(_) => return vec![],
        };

        // We need to iterate all keys and filter
        // For now, return empty - this is complex with IndexedDB cursors
        let prefix = format!("{}:", object_id);
        let _ = (store, prefix);

        // TODO: Implement cursor-based iteration
        vec![]
    }
}

#[async_trait(?Send)]
impl SyncStateStore for IndexedDbEnvironment {
    async fn mark_unsynced(&self, object_id: ObjectId) {
        let tx = self
            .transaction(&[UNSYNCED_STORE], IdbTransactionMode::Readwrite)
            .expect("failed to start transaction");
        let store = Self::object_store(&tx, UNSYNCED_STORE).expect("failed to get store");

        let key = JsValue::from_str(&Self::object_id_to_key(&object_id));
        // Store empty value - we just need to track the key
        let value = Uint8Array::new_with_length(0);
        let request = store.put_with_key(&value, &key).expect("failed to put");
        let _ = Self::await_request(&request).await;
    }

    async fn clear_unsynced(&self, object_id: &ObjectId) {
        let tx = self
            .transaction(&[UNSYNCED_STORE], IdbTransactionMode::Readwrite)
            .expect("failed to start transaction");
        let store = Self::object_store(&tx, UNSYNCED_STORE).expect("failed to get store");

        let key = JsValue::from_str(&Self::object_id_to_key(object_id));
        let request = store.delete(&key).expect("failed to delete");
        let _ = Self::await_request(&request).await;
    }

    async fn get_unsynced_objects(&self) -> Vec<ObjectId> {
        let tx = match self.transaction(&[UNSYNCED_STORE], IdbTransactionMode::Readonly) {
            Ok(tx) => tx,
            Err(_) => return vec![],
        };
        let store = match Self::object_store(&tx, UNSYNCED_STORE) {
            Ok(s) => s,
            Err(_) => return vec![],
        };

        // Get all keys from the store
        let request = match store.get_all_keys() {
            Ok(r) => r,
            Err(_) => return vec![],
        };
        let result = match Self::await_request(&request).await {
            Ok(r) => r,
            Err(_) => return vec![],
        };

        // Convert JS array of keys to Vec<ObjectId>
        let array: Array = match result.dyn_into() {
            Ok(a) => a,
            Err(_) => return vec![],
        };

        let mut object_ids = Vec::new();
        for i in 0..array.length() {
            if let Some(key_str) = array.get(i).as_string() {
                if let Some(object_id) = Self::key_to_object_id(&key_str) {
                    object_ids.push(object_id);
                }
            }
        }
        object_ids
    }

    async fn is_unsynced(&self, object_id: &ObjectId) -> bool {
        let tx = match self.transaction(&[UNSYNCED_STORE], IdbTransactionMode::Readonly) {
            Ok(tx) => tx,
            Err(_) => return false,
        };
        let store = match Self::object_store(&tx, UNSYNCED_STORE) {
            Ok(s) => s,
            Err(_) => return false,
        };

        let key = JsValue::from_str(&Self::object_id_to_key(object_id));
        let request = match store.get(&key) {
            Ok(r) => r,
            Err(_) => return false,
        };
        let result = match Self::await_request(&request).await {
            Ok(r) => r,
            Err(_) => return false,
        };

        // If we got a result (even undefined array buffer), key exists
        !result.is_undefined()
    }
}

/// Serialize a Commit to bytes.
fn serialize_commit(commit: &groove::Commit) -> Vec<u8> {
    let mut buf = Vec::new();

    // Parents count + parent IDs
    buf.extend_from_slice(&(commit.parents.len() as u32).to_le_bytes());
    for parent in &commit.parents {
        buf.extend_from_slice(parent.as_bytes());
    }

    // Author length + author
    let author_bytes = commit.author.as_bytes();
    buf.extend_from_slice(&(author_bytes.len() as u32).to_le_bytes());
    buf.extend_from_slice(author_bytes);

    // Timestamp
    buf.extend_from_slice(&commit.timestamp.to_le_bytes());

    // Meta: Option<BTreeMap> - 0 for None, count+1 for Some
    match &commit.meta {
        None => {
            buf.extend_from_slice(&0u32.to_le_bytes());
        }
        Some(meta) => {
            buf.extend_from_slice(&((meta.len() + 1) as u32).to_le_bytes());
            for (k, v) in meta {
                let k_bytes = k.as_bytes();
                buf.extend_from_slice(&(k_bytes.len() as u32).to_le_bytes());
                buf.extend_from_slice(k_bytes);
                let v_bytes = v.as_bytes();
                buf.extend_from_slice(&(v_bytes.len() as u32).to_le_bytes());
                buf.extend_from_slice(v_bytes);
            }
        }
    }

    // Content length + content
    buf.extend_from_slice(&(commit.content.len() as u32).to_le_bytes());
    buf.extend_from_slice(&commit.content);

    buf
}

/// Deserialize a Commit from bytes.
fn deserialize_commit(data: &[u8]) -> Option<groove::Commit> {
    let mut pos = 0;

    // Parents
    if data.len() < pos + 4 {
        return None;
    }
    let parent_count = u32::from_le_bytes(data[pos..pos + 4].try_into().ok()?) as usize;
    pos += 4;

    let mut parents = Vec::with_capacity(parent_count);
    for _ in 0..parent_count {
        if data.len() < pos + 32 {
            return None;
        }
        let mut arr = [0u8; 32];
        arr.copy_from_slice(&data[pos..pos + 32]);
        parents.push(CommitId::from_bytes(arr));
        pos += 32;
    }

    // Author
    if data.len() < pos + 4 {
        return None;
    }
    let author_len = u32::from_le_bytes(data[pos..pos + 4].try_into().ok()?) as usize;
    pos += 4;

    if data.len() < pos + author_len {
        return None;
    }
    let author = String::from_utf8(data[pos..pos + author_len].to_vec()).ok()?;
    pos += author_len;

    // Timestamp
    if data.len() < pos + 8 {
        return None;
    }
    let timestamp = u64::from_le_bytes(data[pos..pos + 8].try_into().ok()?);
    pos += 8;

    // Meta: Option<BTreeMap> - 0 for None, count+1 for Some
    if data.len() < pos + 4 {
        return None;
    }
    let meta_indicator = u32::from_le_bytes(data[pos..pos + 4].try_into().ok()?) as usize;
    pos += 4;

    let meta = if meta_indicator == 0 {
        None
    } else {
        let meta_count = meta_indicator - 1;
        let mut map = std::collections::BTreeMap::new();
        for _ in 0..meta_count {
            if data.len() < pos + 4 {
                return None;
            }
            let k_len = u32::from_le_bytes(data[pos..pos + 4].try_into().ok()?) as usize;
            pos += 4;
            if data.len() < pos + k_len {
                return None;
            }
            let k = String::from_utf8(data[pos..pos + k_len].to_vec()).ok()?;
            pos += k_len;

            if data.len() < pos + 4 {
                return None;
            }
            let v_len = u32::from_le_bytes(data[pos..pos + 4].try_into().ok()?) as usize;
            pos += 4;
            if data.len() < pos + v_len {
                return None;
            }
            let v = String::from_utf8(data[pos..pos + v_len].to_vec()).ok()?;
            pos += v_len;

            map.insert(k, v);
        }
        Some(map)
    };

    // Content
    if data.len() < pos + 4 {
        return None;
    }
    let content_len = u32::from_le_bytes(data[pos..pos + 4].try_into().ok()?) as usize;
    pos += 4;

    if data.len() < pos + content_len {
        return None;
    }
    let content = data[pos..pos + content_len].to_vec().into_boxed_slice();

    Some(groove::Commit {
        parents,
        author,
        timestamp,
        meta,
        content,
    })
}

/// Serialize a list of CommitIds.
fn serialize_commit_ids(ids: &[CommitId]) -> Vec<u8> {
    let mut buf = Vec::with_capacity(4 + ids.len() * 32);
    buf.extend_from_slice(&(ids.len() as u32).to_le_bytes());
    for id in ids {
        buf.extend_from_slice(id.as_bytes());
    }
    buf
}

/// Deserialize a list of CommitIds.
fn deserialize_commit_ids(data: &[u8]) -> Vec<CommitId> {
    if data.len() < 4 {
        return vec![];
    }
    let count = u32::from_le_bytes(data[0..4].try_into().unwrap()) as usize;
    let mut ids = Vec::with_capacity(count);
    let mut pos = 4;
    for _ in 0..count {
        if data.len() < pos + 32 {
            break;
        }
        let mut arr = [0u8; 32];
        arr.copy_from_slice(&data[pos..pos + 32]);
        ids.push(CommitId::from_bytes(arr));
        pos += 32;
    }
    ids
}
