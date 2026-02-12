//! BfTree-backed Storage implementation.
//!
//! Uses a single bf-tree instance with key-encoded namespaces for all data:
//! objects, commits, ack tiers, and indices.
//!
//! Key encoding scheme (all keys are UTF-8 strings with hex-encoded binary parts):
//!
//! ```text
//! "obj:{uuid}:meta"                                       → JSON metadata
//! "obj:{uuid}:br:{branch}:tips"                           → JSON HashSet<CommitId>
//! "obj:{uuid}:br:{branch}:c:{commit_uuid}"                → JSON Commit
//! "ack:{commit_hex}"                                      → JSON HashSet<PersistenceTier>
//! "idx:{table}:{col}:{branch}:{hex_encoded_value}:{uuid}" → empty (existence is the signal)
//! ```

use std::collections::{HashMap, HashSet};
use std::ops::Bound;
#[cfg(not(target_arch = "wasm32"))]
use std::path::Path;

use bf_tree::{BfTree, Config, LeafInsertResult, LeafReadResult, ScanReturnField};

use crate::commit::{Commit, CommitId};
use crate::object::{BranchName, ObjectId};
use crate::query_manager::types::Value;
use crate::sync_manager::PersistenceTier;

use super::{LoadedBranch, Storage, StorageError, encode_value};

// ============================================================================
// Constants
// ============================================================================

/// Max key length for bf-tree config. Must accommodate index keys which are the longest.
/// Index keys: `idx:{table}:{col}:{branch}:{hex_value}:{uuid}` — typically < 200 bytes.
const MAX_KEY_LEN: usize = 256;

/// Max record size (key + value) for bf-tree config.
/// Derived from: (leaf_page_size - max_fence_len - 40) / 2 - sizeof(LeafKVMeta)
/// = (32768 - 512 - 40) / 2 - 8 = 16100
const MAX_RECORD_SIZE: usize = 16000;

/// Leaf page size. 32KB is the maximum bf-tree supports.
const LEAF_PAGE_SIZE: usize = 32768;

/// Initial read buffer size.
const INITIAL_READ_BUF: usize = MAX_RECORD_SIZE;

/// Max value size per record (key can be up to MAX_KEY_LEN).
/// Values larger than this are chunked.
const VALUE_CHUNK_SIZE: usize = 15000;

// ============================================================================
// BfTreeStorage
// ============================================================================

/// Persistent Storage backed by bf-tree.
///
/// All data lives in a single bf-tree instance with key-encoded namespaces.
/// No outbox, no scheduling — pure Storage impl. Those concerns live in
/// Scheduler and SyncSender.
pub struct BfTreeStorage {
    tree: BfTree,
}

impl BfTreeStorage {
    /// Open a file-backed BfTreeStorage at the given path.
    ///
    /// If the file exists (from a previous snapshot), data is restored.
    /// Call `flush()` before drop to persist all data.
    #[cfg(not(target_arch = "wasm32"))]
    pub fn open(path: impl AsRef<Path>, cache_size_bytes: usize) -> Result<Self, StorageError> {
        let path = path.as_ref();
        let mut config = Config::new(path, cache_size_bytes);
        Self::configure(&mut config);

        // new_from_snapshot loads existing data if the file exists,
        // otherwise creates a fresh tree.
        let tree = BfTree::new_from_snapshot(config, None)
            .map_err(|e| StorageError::IoError(format!("bf-tree open: {:?}", e)))?;
        Ok(Self { tree })
    }

    /// Create an in-memory BfTreeStorage (for testing).
    pub fn memory(cache_size_bytes: usize) -> Result<Self, StorageError> {
        let mut config = Config::new(":memory:", cache_size_bytes);
        Self::configure(&mut config);
        let tree = BfTree::with_config(config, None)
            .map_err(|e| StorageError::IoError(format!("bf-tree memory: {:?}", e)))?;
        Ok(Self { tree })
    }

    /// Open a persistent BfTreeStorage backed by OPFS (WASM only).
    ///
    /// Handles both fresh start and crash recovery (snapshot + WAL replay).
    /// Requires pre-opened OpfsVfs handles (async open happens at caller).
    #[cfg(target_arch = "wasm32")]
    pub fn with_opfs(
        tree_vfs: bf_tree::OpfsVfs,
        wal_vfs: bf_tree::OpfsVfs,
        cache_size_bytes: usize,
    ) -> Result<Self, StorageError> {
        let mut config = Config::default();
        config.cb_size_byte(cache_size_bytes);
        Self::configure(&mut config);

        let tree = BfTree::open_with_opfs(tree_vfs, wal_vfs, config)
            .map_err(|e| StorageError::IoError(format!("bf-tree OPFS: {:?}", e)))?;
        let storage = Self { tree };
        storage.log_key_stats();
        Ok(storage)
    }

    /// Log key statistics after opening storage (for debugging persistence).
    fn log_key_stats(&self) {
        let count_prefix =
            |pfx: &str| -> usize { self.tree_scan_keys(pfx).map(|v| v.len()).unwrap_or(0) };
        let obj_count = count_prefix("obj:");
        let idx_count = count_prefix("idx:");
        let ack_count = count_prefix("ack:");
        tracing::info!(obj_count, idx_count, ack_count, "BfTreeStorage opened");
        // If there are index keys, log a sample
        if idx_count > 0 {
            if let Ok(keys) = self.tree_scan_keys("idx:") {
                for key in keys.iter().take(5) {
                    tracing::debug!(key, "sample index key");
                }
            }
        }
    }

    fn configure(config: &mut Config) {
        config.cb_max_key_len(MAX_KEY_LEN);
        config.cb_max_record_size(MAX_RECORD_SIZE);
        config.leaf_page_size(LEAF_PAGE_SIZE);
        // leaf_page_size / min_record_size must not exceed 4096: 32768/8 = 4096
        config.cb_min_record_size(8);
    }

    // ========================================================================
    // Key encoding helpers
    // ========================================================================

    fn obj_meta_key(id: ObjectId) -> String {
        format!("obj:{}:meta", format_uuid(id))
    }

    fn branch_tips_key(object_id: ObjectId, branch: &BranchName) -> String {
        format!("obj:{}:br:{}:tips", format_uuid(object_id), branch)
    }

    fn commit_key(object_id: ObjectId, branch: &BranchName, commit_id: CommitId) -> String {
        format!(
            "obj:{}:br:{}:c:{}",
            format_uuid(object_id),
            branch,
            hex::encode(commit_id.0)
        )
    }

    /// Prefix for scanning all commits of a branch.
    fn commit_prefix(object_id: ObjectId, branch: &BranchName) -> String {
        format!("obj:{}:br:{}:c:", format_uuid(object_id), branch)
    }

    fn ack_key(commit_id: CommitId) -> String {
        format!("ack:{}", hex::encode(commit_id.0))
    }

    fn index_entry_key(
        table: &str,
        column: &str,
        branch: &str,
        value: &Value,
        row_id: ObjectId,
    ) -> String {
        format!(
            "idx:{}:{}:{}:{}:{}",
            table,
            column,
            branch,
            hex::encode(encode_value(value)),
            format_uuid(row_id)
        )
    }

    /// Prefix for scanning all entries with a specific index value.
    fn index_value_prefix(table: &str, column: &str, branch: &str, value: &Value) -> String {
        format!(
            "idx:{}:{}:{}:{}:",
            table,
            column,
            branch,
            hex::encode(encode_value(value))
        )
    }

    /// Prefix for scanning all entries in an index (table/col/branch).
    fn index_prefix(table: &str, column: &str, branch: &str) -> String {
        format!("idx:{}:{}:{}:", table, column, branch)
    }

    // ========================================================================
    // bf-tree read/write helpers
    // ========================================================================

    /// Insert a key-value pair. Handles chunking for large values.
    fn tree_insert(&self, key: &str, value: &[u8]) -> Result<(), StorageError> {
        let key_bytes = key.as_bytes();
        let total_record_size = key_bytes.len() + value.len();

        if total_record_size <= MAX_RECORD_SIZE {
            // Fits in a single record
            match self.tree.insert(key_bytes, value) {
                LeafInsertResult::Success => Ok(()),
                LeafInsertResult::InvalidKV(msg) => {
                    Err(StorageError::IoError(format!("bf-tree insert: {}", msg)))
                }
            }
        } else {
            // Chunk the value
            let chunks: Vec<&[u8]> = value.chunks(VALUE_CHUNK_SIZE).collect();
            let num_chunks = chunks.len();

            // Store chunk count in the main key
            let meta = format!("{{\"chunks\":{}}}", num_chunks);
            match self.tree.insert(key_bytes, meta.as_bytes()) {
                LeafInsertResult::Success => {}
                LeafInsertResult::InvalidKV(msg) => {
                    return Err(StorageError::IoError(format!(
                        "bf-tree insert chunk meta: {}",
                        msg
                    )));
                }
            }

            // Store each chunk
            for (i, chunk) in chunks.iter().enumerate() {
                let chunk_key = format!("{}:chunk:{:06}", key, i);
                match self.tree.insert(chunk_key.as_bytes(), chunk) {
                    LeafInsertResult::Success => {}
                    LeafInsertResult::InvalidKV(msg) => {
                        return Err(StorageError::IoError(format!(
                            "bf-tree insert chunk {}: {}",
                            i, msg
                        )));
                    }
                }
            }
            Ok(())
        }
    }

    /// Read a value by key. Handles chunked values transparently.
    fn tree_read(&self, key: &str) -> Result<Option<Vec<u8>>, StorageError> {
        let key_bytes = key.as_bytes();
        let mut buf = vec![0u8; INITIAL_READ_BUF];

        match self.tree.read(key_bytes, &mut buf) {
            LeafReadResult::Found(len) => {
                let data = &buf[..len as usize];
                // Check if this is a chunked value
                if let Some(num_chunks) = parse_chunk_meta(data) {
                    // Reassemble chunks
                    let mut assembled = Vec::new();
                    for i in 0..num_chunks {
                        let chunk_key = format!("{}:chunk:{:06}", key, i);
                        match self.tree_read_raw(&chunk_key)? {
                            Some(chunk) => assembled.extend_from_slice(&chunk),
                            None => {
                                return Err(StorageError::IoError(format!(
                                    "missing chunk {} for key {}",
                                    i, key
                                )));
                            }
                        }
                    }
                    Ok(Some(assembled))
                } else {
                    Ok(Some(data.to_vec()))
                }
            }
            LeafReadResult::NotFound | LeafReadResult::Deleted => Ok(None),
            LeafReadResult::InvalidKey => Err(StorageError::IoError(format!(
                "bf-tree read invalid key: {}",
                key
            ))),
        }
    }

    /// Raw read without chunk reassembly.
    fn tree_read_raw(&self, key: &str) -> Result<Option<Vec<u8>>, StorageError> {
        let key_bytes = key.as_bytes();
        let mut buf = vec![0u8; INITIAL_READ_BUF];

        match self.tree.read(key_bytes, &mut buf) {
            LeafReadResult::Found(len) => Ok(Some(buf[..len as usize].to_vec())),
            LeafReadResult::NotFound | LeafReadResult::Deleted => Ok(None),
            LeafReadResult::InvalidKey => Err(StorageError::IoError(format!(
                "bf-tree read invalid key: {}",
                key
            ))),
        }
    }

    /// Delete a key. Also deletes chunks if it was a chunked value.
    fn tree_delete(&self, key: &str) -> Result<(), StorageError> {
        // Check if chunked
        if let Some(data) = self.tree_read_raw(key)?
            && let Some(num_chunks) = parse_chunk_meta(&data)
        {
            for i in 0..num_chunks {
                let chunk_key = format!("{}:chunk:{:06}", key, i);
                self.tree.delete(chunk_key.as_bytes());
            }
        }
        self.tree.delete(key.as_bytes());
        Ok(())
    }

    /// Scan keys with a prefix, returning key-value pairs.
    fn tree_scan_prefix(&self, prefix: &str) -> Result<Vec<(String, Vec<u8>)>, StorageError> {
        let start = prefix.as_bytes();
        // End key: increment last byte of prefix to get exclusive upper bound
        let mut end = start.to_vec();
        increment_bytes(&mut end);

        let mut results = Vec::new();
        let mut buf = vec![0u8; INITIAL_READ_BUF];

        match self
            .tree
            .scan_with_end_key(start, &end, ScanReturnField::KeyAndValue)
        {
            Ok(mut iter) => {
                while let Some((key_len, val_len)) = iter.next(&mut buf) {
                    let key = String::from_utf8_lossy(&buf[..key_len]).to_string();
                    let val = buf[key_len..key_len + val_len].to_vec();
                    // Skip chunk sub-keys
                    if !key.contains(":chunk:") {
                        results.push((key, val));
                    }
                }
                Ok(results)
            }
            Err(e) => Err(StorageError::IoError(format!("bf-tree scan: {:?}", e))),
        }
    }

    /// Scan keys with a prefix, returning only keys.
    fn tree_scan_keys(&self, prefix: &str) -> Result<Vec<String>, StorageError> {
        let start = prefix.as_bytes();
        let mut end = start.to_vec();
        increment_bytes(&mut end);

        let mut results = Vec::new();
        let mut buf = vec![0u8; INITIAL_READ_BUF];

        match self
            .tree
            .scan_with_end_key(start, &end, ScanReturnField::Key)
        {
            Ok(mut iter) => {
                while let Some((key_len, _val_len)) = iter.next(&mut buf) {
                    let key = String::from_utf8_lossy(&buf[..key_len]).to_string();
                    if !key.contains(":chunk:") {
                        results.push(key);
                    }
                }
                Ok(results)
            }
            Err(e) => Err(StorageError::IoError(format!("bf-tree scan keys: {:?}", e))),
        }
    }

    /// Scan a range of keys (inclusive start, exclusive end), returning keys only.
    fn tree_scan_key_range(&self, start: &str, end: &str) -> Result<Vec<String>, StorageError> {
        let mut results = Vec::new();
        let mut buf = vec![0u8; INITIAL_READ_BUF];

        match self
            .tree
            .scan_with_end_key(start.as_bytes(), end.as_bytes(), ScanReturnField::Key)
        {
            Ok(mut iter) => {
                while let Some((key_len, _val_len)) = iter.next(&mut buf) {
                    let key = String::from_utf8_lossy(&buf[..key_len]).to_string();
                    if !key.contains(":chunk:") {
                        results.push(key);
                    }
                }
                Ok(results)
            }
            Err(e) => Err(StorageError::IoError(format!(
                "bf-tree scan range: {:?}",
                e
            ))),
        }
    }
}

// ============================================================================
// Storage trait implementation
// ============================================================================

impl Storage for BfTreeStorage {
    // ================================================================
    // Object storage
    // ================================================================

    fn create_object(
        &mut self,
        id: ObjectId,
        metadata: HashMap<String, String>,
    ) -> Result<(), StorageError> {
        let key = Self::obj_meta_key(id);
        let json = serde_json::to_vec(&metadata)
            .map_err(|e| StorageError::IoError(format!("serialize metadata: {}", e)))?;
        self.tree_insert(&key, &json)
    }

    fn load_object_metadata(
        &self,
        id: ObjectId,
    ) -> Result<Option<HashMap<String, String>>, StorageError> {
        let key = Self::obj_meta_key(id);
        match self.tree_read(&key)? {
            Some(data) => {
                let meta: HashMap<String, String> = serde_json::from_slice(&data)
                    .map_err(|e| StorageError::IoError(format!("deserialize metadata: {}", e)))?;
                Ok(Some(meta))
            }
            None => Ok(None),
        }
    }

    fn load_branch(
        &self,
        object_id: ObjectId,
        branch: &BranchName,
    ) -> Result<Option<LoadedBranch>, StorageError> {
        // Check if object exists
        let meta_key = Self::obj_meta_key(object_id);
        if self.tree_read(&meta_key)?.is_none() {
            return Ok(None);
        }

        // Load commits via prefix scan
        let commit_prefix = Self::commit_prefix(object_id, branch);
        let commit_entries = self.tree_scan_prefix(&commit_prefix)?;

        if commit_entries.is_empty() {
            // Check if tips exist (branch could exist with only tips set)
            let tips_key = Self::branch_tips_key(object_id, branch);
            if self.tree_read(&tips_key)?.is_none() {
                return Ok(None);
            }
        }

        let mut commits = Vec::new();
        for (_key, data) in &commit_entries {
            let mut commit: Commit = serde_json::from_slice(data)
                .map_err(|e| StorageError::IoError(format!("deserialize commit: {}", e)))?;

            // Load ack state for this commit
            let ack_key = Self::ack_key(commit.id());
            if let Some(ack_data) = self.tree_read(&ack_key)? {
                let tiers: HashSet<PersistenceTier> = serde_json::from_slice(&ack_data)
                    .map_err(|e| StorageError::IoError(format!("deserialize ack: {}", e)))?;
                commit.ack_state.confirmed_tiers = tiers;
            }

            commits.push(commit);
        }

        // Load tips
        let tips_key = Self::branch_tips_key(object_id, branch);
        let tails = match self.tree_read(&tips_key)? {
            Some(data) => serde_json::from_slice(&data)
                .map_err(|e| StorageError::IoError(format!("deserialize tips: {}", e)))?,
            None => HashSet::new(),
        };

        Ok(Some(LoadedBranch { commits, tails }))
    }

    fn append_commit(
        &mut self,
        object_id: ObjectId,
        branch: &BranchName,
        commit: Commit,
    ) -> Result<(), StorageError> {
        let commit_id = commit.id();

        // Store the commit
        let commit_key = Self::commit_key(object_id, branch, commit_id);
        let commit_json = serde_json::to_vec(&commit)
            .map_err(|e| StorageError::IoError(format!("serialize commit: {}", e)))?;
        self.tree_insert(&commit_key, &commit_json)?;

        // Read-modify-write tips
        let tips_key = Self::branch_tips_key(object_id, branch);
        let mut tips: HashSet<CommitId> = match self.tree_read(&tips_key)? {
            Some(data) => serde_json::from_slice(&data)
                .map_err(|e| StorageError::IoError(format!("deserialize tips: {}", e)))?,
            None => HashSet::new(),
        };

        // Remove parents from tips
        for parent in &commit.parents {
            tips.remove(parent);
        }
        // Add this commit as a tip
        tips.insert(commit_id);

        let tips_json = serde_json::to_vec(&tips)
            .map_err(|e| StorageError::IoError(format!("serialize tips: {}", e)))?;
        self.tree_insert(&tips_key, &tips_json)?;

        Ok(())
    }

    fn delete_commit(
        &mut self,
        object_id: ObjectId,
        branch: &BranchName,
        commit_id: CommitId,
    ) -> Result<(), StorageError> {
        // Delete the commit
        let commit_key = Self::commit_key(object_id, branch, commit_id);
        self.tree_delete(&commit_key)?;

        // Remove from tips
        let tips_key = Self::branch_tips_key(object_id, branch);
        if let Some(data) = self.tree_read(&tips_key)? {
            let mut tips: HashSet<CommitId> = serde_json::from_slice(&data)
                .map_err(|e| StorageError::IoError(format!("deserialize tips: {}", e)))?;
            tips.remove(&commit_id);
            let tips_json = serde_json::to_vec(&tips)
                .map_err(|e| StorageError::IoError(format!("serialize tips: {}", e)))?;
            self.tree_insert(&tips_key, &tips_json)?;
        }

        Ok(())
    }

    fn set_branch_tails(
        &mut self,
        object_id: ObjectId,
        branch: &BranchName,
        tails: Option<HashSet<CommitId>>,
    ) -> Result<(), StorageError> {
        let tips_key = Self::branch_tips_key(object_id, branch);
        match tails {
            Some(t) => {
                let json = serde_json::to_vec(&t)
                    .map_err(|e| StorageError::IoError(format!("serialize tails: {}", e)))?;
                self.tree_insert(&tips_key, &json)?;
            }
            None => {
                self.tree_delete(&tips_key)?;
            }
        }
        Ok(())
    }

    // ================================================================
    // Persistence ack storage
    // ================================================================

    fn store_ack_tier(
        &mut self,
        commit_id: CommitId,
        tier: PersistenceTier,
    ) -> Result<(), StorageError> {
        let key = Self::ack_key(commit_id);
        // Read-modify-write: load existing tiers, add new one
        let mut tiers: HashSet<PersistenceTier> = match self.tree_read(&key)? {
            Some(data) => serde_json::from_slice(&data)
                .map_err(|e| StorageError::IoError(format!("deserialize ack: {}", e)))?,
            None => HashSet::new(),
        };
        tiers.insert(tier);
        let json = serde_json::to_vec(&tiers)
            .map_err(|e| StorageError::IoError(format!("serialize ack: {}", e)))?;
        self.tree_insert(&key, &json)
    }

    // ================================================================
    // Index operations
    // ================================================================

    fn index_insert(
        &mut self,
        table: &str,
        column: &str,
        branch: &str,
        value: &Value,
        row_id: ObjectId,
    ) -> Result<(), StorageError> {
        let key = Self::index_entry_key(table, column, branch, value, row_id);
        tracing::trace!(table, column, branch, %row_id, %key, "index_insert");
        // Sentinel byte — bf-tree requires non-empty values; existence is the signal
        self.tree_insert(&key, &[0x01])
    }

    fn index_remove(
        &mut self,
        table: &str,
        column: &str,
        branch: &str,
        value: &Value,
        row_id: ObjectId,
    ) -> Result<(), StorageError> {
        let key = Self::index_entry_key(table, column, branch, value, row_id);
        self.tree.delete(key.as_bytes());
        Ok(())
    }

    fn index_lookup(
        &self,
        table: &str,
        column: &str,
        branch: &str,
        value: &Value,
    ) -> Vec<ObjectId> {
        let prefix = Self::index_value_prefix(table, column, branch, value);
        match self.tree_scan_keys(&prefix) {
            Ok(keys) => keys
                .iter()
                .filter_map(|k| parse_uuid_from_index_key(k))
                .collect(),
            Err(_) => Vec::new(),
        }
    }

    fn index_range(
        &self,
        table: &str,
        column: &str,
        branch: &str,
        start: Bound<&Value>,
        end: Bound<&Value>,
    ) -> Vec<ObjectId> {
        let base_prefix = Self::index_prefix(table, column, branch);

        // Compute start key
        let start_key = match start {
            Bound::Included(v) => {
                format!("{}{}", base_prefix, hex::encode(encode_value(v)))
            }
            Bound::Excluded(v) => {
                let encoded = hex::encode(encode_value(v));
                let mut key = format!("{}{}:", base_prefix, encoded);
                // After the last possible entry for this value (uuid suffix + separator)
                // We use the prefix with ":" which sorts after all UUIDs
                increment_string(&mut key);
                key
            }
            Bound::Unbounded => base_prefix.clone(),
        };

        // Compute end key
        let end_key = match end {
            Bound::Included(v) => {
                let encoded = hex::encode(encode_value(v));
                let mut key = format!("{}{}:", base_prefix, encoded);
                // Include all entries with this value by going past last UUID
                increment_string(&mut key);
                key
            }
            Bound::Excluded(v) => {
                format!("{}{}", base_prefix, hex::encode(encode_value(v)))
            }
            Bound::Unbounded => {
                let mut end = base_prefix.clone();
                increment_string(&mut end);
                end
            }
        };

        if start_key >= end_key {
            return Vec::new();
        }

        match self.tree_scan_key_range(&start_key, &end_key) {
            Ok(keys) => keys
                .iter()
                .filter_map(|k| parse_uuid_from_index_key(k))
                .collect(),
            Err(_) => Vec::new(),
        }
    }

    fn index_scan_all(&self, table: &str, column: &str, branch: &str) -> Vec<ObjectId> {
        let prefix = Self::index_prefix(table, column, branch);
        tracing::trace!(table, column, branch, %prefix, "index_scan_all");
        match self.tree_scan_keys(&prefix) {
            Ok(keys) => {
                let ids: Vec<ObjectId> = keys
                    .iter()
                    .filter_map(|k| parse_uuid_from_index_key(k))
                    .collect();
                tracing::trace!(
                    prefix_matches = keys.len(),
                    parsed_ids = ids.len(),
                    "index_scan_all result"
                );
                ids
            }
            Err(e) => {
                tracing::warn!(error = ?e, "index_scan_all error");
                Vec::new()
            }
        }
    }

    fn flush(&self) {
        self.tree.snapshot();
    }

    fn flush_wal(&self) {
        self.tree.flush_wal();
    }
}

// ============================================================================
// Helpers
// ============================================================================

/// Format an ObjectId as a compact hex string (no dashes).
fn format_uuid(id: ObjectId) -> String {
    hex::encode(id.uuid().as_bytes())
}

/// Parse a UUID from the last segment of an index key.
/// Key format: `idx:{table}:{col}:{branch}:{hex_value}:{uuid_hex}`
fn parse_uuid_from_index_key(key: &str) -> Option<ObjectId> {
    let uuid_hex = key.rsplit(':').next()?;
    let bytes = hex::decode(uuid_hex).ok()?;
    if bytes.len() != 16 {
        return None;
    }
    let uuid = uuid::Uuid::from_bytes(bytes.try_into().ok()?);
    Some(ObjectId(internment::Intern::new(uuid)))
}

/// Parse chunk metadata from a value. Returns Some(num_chunks) if this is chunk metadata.
fn parse_chunk_meta(data: &[u8]) -> Option<usize> {
    // Quick check: chunk meta is JSON like {"chunks":N}
    if data.len() > 50 || !data.starts_with(b"{\"chunks\":") {
        return None;
    }
    let s = std::str::from_utf8(data).ok()?;
    let parsed: serde_json::Value = serde_json::from_str(s).ok()?;
    parsed.get("chunks")?.as_u64().map(|n| n as usize)
}

/// Increment the last byte of a byte slice to create an exclusive upper bound.
/// For prefix scans: scanning [prefix, incremented_prefix) captures all keys with that prefix.
fn increment_bytes(bytes: &mut Vec<u8>) {
    for i in (0..bytes.len()).rev() {
        if bytes[i] < 0xFF {
            bytes[i] += 1;
            bytes.truncate(i + 1);
            return;
        }
    }
    // All 0xFF — push a byte
    bytes.push(0x00);
}

/// Increment the last character of a string for exclusive upper bound.
fn increment_string(s: &mut String) {
    let mut bytes = std::mem::take(s).into_bytes();
    increment_bytes(&mut bytes);
    *s = String::from_utf8(bytes).unwrap_or_default();
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use smallvec::smallvec;

    fn make_commit(content: &[u8]) -> Commit {
        Commit {
            parents: smallvec![],
            content: content.to_vec(),
            timestamp: 12345,
            author: ObjectId::new(),
            metadata: None,
            stored_state: Default::default(),
            ack_state: Default::default(),
        }
    }

    fn test_storage() -> BfTreeStorage {
        BfTreeStorage::memory(4 * 1024 * 1024).unwrap()
    }

    #[test]
    fn bftree_object_roundtrip() {
        let mut storage = test_storage();

        let id = ObjectId::new();
        let mut metadata = HashMap::new();
        metadata.insert(
            crate::metadata::MetadataKey::Table.to_string(),
            "users".to_string(),
        );
        metadata.insert("app".to_string(), "test".to_string());

        // Create
        storage.create_object(id, metadata.clone()).unwrap();

        // Load
        let loaded = storage.load_object_metadata(id).unwrap();
        assert_eq!(loaded, Some(metadata));

        // Non-existent
        let other = ObjectId::new();
        assert_eq!(storage.load_object_metadata(other).unwrap(), None);
    }

    #[test]
    fn bftree_commit_roundtrip() {
        let mut storage = test_storage();

        let id = ObjectId::new();
        let branch = BranchName::new("main");
        storage.create_object(id, HashMap::new()).unwrap();

        // No branch yet
        assert_eq!(storage.load_branch(id, &branch).unwrap(), None);

        // Append commit
        let commit = make_commit(b"first");
        let commit_id = commit.id();
        storage.append_commit(id, &branch, commit).unwrap();

        let loaded = storage.load_branch(id, &branch).unwrap().unwrap();
        assert_eq!(loaded.commits.len(), 1);
        assert!(loaded.tails.contains(&commit_id));
        assert_eq!(loaded.commits[0].content, b"first");

        // Append second commit (child of first)
        let mut commit2 = make_commit(b"second");
        commit2.parents = smallvec![commit_id];
        let commit2_id = commit2.id();
        storage.append_commit(id, &branch, commit2).unwrap();

        let loaded = storage.load_branch(id, &branch).unwrap().unwrap();
        assert_eq!(loaded.commits.len(), 2);
        assert!(!loaded.tails.contains(&commit_id)); // parent removed from tips
        assert!(loaded.tails.contains(&commit2_id));

        // Delete first commit
        storage.delete_commit(id, &branch, commit_id).unwrap();
        let loaded = storage.load_branch(id, &branch).unwrap().unwrap();
        assert_eq!(loaded.commits.len(), 1);
        assert_eq!(loaded.commits[0].content, b"second");
    }

    #[test]
    fn bftree_index_ops() {
        let mut storage = test_storage();

        let row1 = ObjectId::new();
        let row2 = ObjectId::new();
        let row3 = ObjectId::new();
        let row4 = ObjectId::new();

        // Insert entries at different values
        storage
            .index_insert("users", "age", "main", &Value::Integer(20), row1)
            .unwrap();
        storage
            .index_insert("users", "age", "main", &Value::Integer(25), row2)
            .unwrap();
        storage
            .index_insert("users", "age", "main", &Value::Integer(25), row3)
            .unwrap();
        storage
            .index_insert("users", "age", "main", &Value::Integer(30), row4)
            .unwrap();

        // Lookup exact value
        let results = storage.index_lookup("users", "age", "main", &Value::Integer(25));
        assert_eq!(results.len(), 2);
        assert!(results.contains(&row2));
        assert!(results.contains(&row3));

        // Lookup missing value
        let results = storage.index_lookup("users", "age", "main", &Value::Integer(99));
        assert!(results.is_empty());

        // Range [25, 30) — should return row2, row3 (value=25)
        let results = storage.index_range(
            "users",
            "age",
            "main",
            Bound::Included(&Value::Integer(25)),
            Bound::Excluded(&Value::Integer(30)),
        );
        assert_eq!(results.len(), 2);
        assert!(results.contains(&row2));
        assert!(results.contains(&row3));

        // Range unbounded start to exclusive 26 — should return row1, row2, row3
        let results = storage.index_range(
            "users",
            "age",
            "main",
            Bound::Unbounded,
            Bound::Excluded(&Value::Integer(26)),
        );
        assert_eq!(results.len(), 3);
        assert!(results.contains(&row1));
        assert!(results.contains(&row2));
        assert!(results.contains(&row3));

        // Range inclusive 30 to unbounded — should return row4
        let results = storage.index_range(
            "users",
            "age",
            "main",
            Bound::Included(&Value::Integer(30)),
            Bound::Unbounded,
        );
        assert_eq!(results.len(), 1);
        assert!(results.contains(&row4));

        // Scan all
        let results = storage.index_scan_all("users", "age", "main");
        assert_eq!(results.len(), 4);

        // Remove
        storage
            .index_remove("users", "age", "main", &Value::Integer(25), row2)
            .unwrap();
        let results = storage.index_lookup("users", "age", "main", &Value::Integer(25));
        assert_eq!(results.len(), 1);
        assert!(results.contains(&row3));
    }

    #[test]
    fn bftree_index_branch_isolation() {
        let mut storage = test_storage();

        let row1 = ObjectId::new();
        let row2 = ObjectId::new();

        storage
            .index_insert("users", "age", "main", &Value::Integer(25), row1)
            .unwrap();
        storage
            .index_insert("users", "age", "feature", &Value::Integer(25), row2)
            .unwrap();

        let main_results = storage.index_lookup("users", "age", "main", &Value::Integer(25));
        assert_eq!(main_results.len(), 1);
        assert!(main_results.contains(&row1));

        let feature_results = storage.index_lookup("users", "age", "feature", &Value::Integer(25));
        assert_eq!(feature_results.len(), 1);
        assert!(feature_results.contains(&row2));
    }

    #[test]
    fn bftree_ack_tier_roundtrip() {
        let mut storage = test_storage();

        let commit_id = CommitId([99u8; 32]);

        storage
            .store_ack_tier(commit_id, PersistenceTier::Worker)
            .unwrap();
        storage
            .store_ack_tier(commit_id, PersistenceTier::EdgeServer)
            .unwrap();

        // Verify by loading a branch that includes this commit
        // (ack tiers are loaded as part of load_branch)
        // For direct verification, read the ack key
        let ack_key = BfTreeStorage::ack_key(commit_id);
        let data = storage.tree_read(&ack_key).unwrap().unwrap();
        let tiers: HashSet<PersistenceTier> = serde_json::from_slice(&data).unwrap();
        assert!(tiers.contains(&PersistenceTier::Worker));
        assert!(tiers.contains(&PersistenceTier::EdgeServer));
    }

    #[test]
    fn bftree_persistence() {
        let temp_dir = tempfile::TempDir::new().unwrap();
        let db_path = temp_dir.path().join("test.bftree");

        let id = ObjectId::new();
        let mut metadata = HashMap::new();
        metadata.insert(
            crate::metadata::MetadataKey::Table.to_string(),
            "users".to_string(),
        );

        let commit_content = b"persistent data";
        let branch = BranchName::new("main");

        // Phase 1: Write data
        {
            let mut storage = BfTreeStorage::open(&db_path, 4 * 1024 * 1024).unwrap();
            storage.create_object(id, metadata.clone()).unwrap();

            let commit = make_commit(commit_content);
            storage.append_commit(id, &branch, commit).unwrap();

            // Insert index entry
            storage
                .index_insert(
                    "users",
                    "name",
                    "main",
                    &Value::Text("Alice".to_string()),
                    id,
                )
                .unwrap();

            // Flush to disk before drop
            storage.flush();
        }

        // Phase 2: Reopen and verify
        {
            let storage = BfTreeStorage::open(&db_path, 4 * 1024 * 1024).unwrap();

            // Metadata survives
            let loaded_meta = storage.load_object_metadata(id).unwrap();
            assert_eq!(loaded_meta, Some(metadata));

            // Commits survive
            let loaded_branch = storage.load_branch(id, &branch).unwrap().unwrap();
            assert_eq!(loaded_branch.commits.len(), 1);
            assert_eq!(loaded_branch.commits[0].content, commit_content);

            // Index survives
            let results =
                storage.index_lookup("users", "name", "main", &Value::Text("Alice".to_string()));
            assert_eq!(results.len(), 1);
            assert!(results.contains(&id));
        }
    }
}
