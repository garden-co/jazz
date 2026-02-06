//! WasmRuntime - Main entry point for JavaScript applications.
//!
//! Provides the core Jazz database functionality exposed to JavaScript:
//! - CRUD operations (insert, query, update, delete)
//! - Reactive subscriptions with callback-based updates
//! - Sync message handling for server communication
//!
//! # Architecture
//!
//! - `WasmIoHandler` wraps `MemoryIoHandler`, delegating all sync storage ops
//! - `WasmRuntime` wraps `Rc<RefCell<RuntimeCore<WasmIoHandler>>>`
//! - `schedule_batched_tick` uses `wasm_bindgen_futures::spawn_local` (debounced)
//! - No explicit tick loops - scheduling emerges from IoHandler

use std::cell::RefCell;
use std::collections::{HashMap, HashSet};
use std::ops::Bound;
use std::rc::{Rc, Weak};

use js_sys::Function;
use serde::Serialize;
use wasm_bindgen::prelude::*;

use groove::commit::{Commit, CommitId};
use groove::io_handler::{IoHandler, LoadedBranch, MemoryIoHandler};
use groove::object::{BranchName, ObjectId};
use groove::query_manager::encoding::decode_row;
use groove::query_manager::session::Session;
use groove::query_manager::types::{Row, RowDescriptor, Schema, Value};
use groove::runtime_core::RuntimeCore;
#[cfg(target_arch = "wasm32")]
use groove::runtime_core::{SubscriptionDelta, SubscriptionHandle};
use groove::schema_manager::{AppId, SchemaManager};
use groove::storage::{ContentHash, StorageError};
use groove::sync_manager::{
    ClientId, InboxEntry, OutboxEntry, PersistenceTier, ServerId, Source, SyncManager, SyncPayload,
};

use crate::query::parse_query;
use crate::types::{WasmSchema, WasmValue};

/// Parse a persistence tier string from JS.
fn parse_tier(tier: &str) -> Result<PersistenceTier, JsError> {
    match tier {
        "worker" => Ok(PersistenceTier::Worker),
        "edge" => Ok(PersistenceTier::EdgeServer),
        "core" => Ok(PersistenceTier::CoreServer),
        _ => Err(JsError::new(&format!(
            "Invalid tier '{}'. Must be 'worker', 'edge', or 'core'.",
            tier
        ))),
    }
}

// ============================================================================
// WasmIoHandler
// ============================================================================

/// IoHandler implementation for WASM.
///
/// Wraps `MemoryIoHandler` for all synchronous storage/index operations.
/// Adds JS callbacks for sync messages and batched tick scheduling.
pub struct WasmIoHandler {
    /// Delegate all storage/index ops to MemoryIoHandler.
    inner: MemoryIoHandler,
    /// JS callback for sync messages.
    sync_callback: Option<Function>,
    /// Debounce flag for scheduled ticks.
    scheduled: Rc<RefCell<bool>>,
    /// Weak reference back to RuntimeCore for spawned tasks.
    core_ref: Weak<RefCell<RuntimeCore<WasmIoHandler>>>,
}

impl WasmIoHandler {
    fn new() -> Self {
        Self {
            inner: MemoryIoHandler::new(),
            sync_callback: None,
            scheduled: Rc::new(RefCell::new(false)),
            core_ref: Weak::new(),
        }
    }

    fn set_core_ref(&mut self, core_ref: Weak<RefCell<RuntimeCore<WasmIoHandler>>>) {
        self.core_ref = core_ref;
    }

    fn set_sync_callback(&mut self, callback: Function) {
        self.sync_callback = Some(callback);
    }
}

impl IoHandler for WasmIoHandler {
    // ================================================================
    // Object storage — delegate to inner
    // ================================================================

    fn create_object(
        &mut self,
        id: ObjectId,
        metadata: HashMap<String, String>,
    ) -> Result<(), StorageError> {
        self.inner.create_object(id, metadata)
    }

    fn load_object_metadata(
        &self,
        id: ObjectId,
    ) -> Result<Option<HashMap<String, String>>, StorageError> {
        self.inner.load_object_metadata(id)
    }

    fn load_branch(
        &self,
        object_id: ObjectId,
        branch: &BranchName,
    ) -> Result<Option<LoadedBranch>, StorageError> {
        self.inner.load_branch(object_id, branch)
    }

    fn append_commit(
        &mut self,
        object_id: ObjectId,
        branch: &BranchName,
        commit: Commit,
    ) -> Result<(), StorageError> {
        self.inner.append_commit(object_id, branch, commit)
    }

    fn delete_commit(
        &mut self,
        object_id: ObjectId,
        branch: &BranchName,
        commit_id: CommitId,
    ) -> Result<(), StorageError> {
        self.inner.delete_commit(object_id, branch, commit_id)
    }

    fn set_branch_tails(
        &mut self,
        object_id: ObjectId,
        branch: &BranchName,
        tails: Option<HashSet<CommitId>>,
    ) -> Result<(), StorageError> {
        self.inner.set_branch_tails(object_id, branch, tails)
    }

    // ================================================================
    // Blob storage — delegate to inner
    // ================================================================

    fn store_blob(&mut self, hash: ContentHash, data: &[u8]) -> Result<(), StorageError> {
        self.inner.store_blob(hash, data)
    }

    fn load_blob(&self, hash: ContentHash) -> Result<Option<Vec<u8>>, StorageError> {
        self.inner.load_blob(hash)
    }

    fn delete_blob(&mut self, hash: ContentHash) -> Result<(), StorageError> {
        self.inner.delete_blob(hash)
    }

    // ================================================================
    // Persistence ack storage — delegate to inner
    // ================================================================

    fn store_ack_tier(
        &mut self,
        commit_id: CommitId,
        tier: PersistenceTier,
    ) -> Result<(), StorageError> {
        self.inner.store_ack_tier(commit_id, tier)
    }

    // ================================================================
    // Index operations — delegate to inner
    // ================================================================

    fn index_insert(
        &mut self,
        table: &str,
        column: &str,
        branch: &str,
        value: &Value,
        row_id: ObjectId,
    ) -> Result<(), StorageError> {
        self.inner.index_insert(table, column, branch, value, row_id)
    }

    fn index_remove(
        &mut self,
        table: &str,
        column: &str,
        branch: &str,
        value: &Value,
        row_id: ObjectId,
    ) -> Result<(), StorageError> {
        self.inner.index_remove(table, column, branch, value, row_id)
    }

    fn index_lookup(
        &self,
        table: &str,
        column: &str,
        branch: &str,
        value: &Value,
    ) -> Vec<ObjectId> {
        self.inner.index_lookup(table, column, branch, value)
    }

    fn index_range(
        &self,
        table: &str,
        column: &str,
        branch: &str,
        start: Bound<&Value>,
        end: Bound<&Value>,
    ) -> Vec<ObjectId> {
        self.inner.index_range(table, column, branch, start, end)
    }

    fn index_scan_all(&self, table: &str, column: &str, branch: &str) -> Vec<ObjectId> {
        self.inner.index_scan_all(table, column, branch)
    }

    // ================================================================
    // Sync messages — bridge to JS
    // ================================================================

    fn send_sync_message(&mut self, message: OutboxEntry) {
        if let Some(ref callback) = self.sync_callback {
            if let Ok(json) = serde_json::to_string(&message.payload) {
                let js_value = JsValue::from_str(&json);
                let _ = callback.call1(&JsValue::NULL, &js_value);
            }
        }
    }

    // ================================================================
    // Scheduling — spawn_local with debounce
    // ================================================================

    fn schedule_batched_tick(&self) {
        let mut scheduled = self.scheduled.borrow_mut();
        if !*scheduled {
            *scheduled = true;

            let core_ref = self.core_ref.clone();
            let flag = self.scheduled.clone();

            wasm_bindgen_futures::spawn_local(async move {
                *flag.borrow_mut() = false;
                if let Some(core_rc) = core_ref.upgrade() {
                    core_rc.borrow_mut().batched_tick();
                }
            });
        }
    }
}

// ============================================================================
// WasmRuntime
// ============================================================================

/// Main runtime for JavaScript applications.
///
/// Wraps `Rc<RefCell<RuntimeCore<WasmIoHandler>>>`.
/// All methods borrow the core, call RuntimeCore, and return.
/// Async scheduling happens via IoHandler.schedule_batched_tick().
#[wasm_bindgen]
pub struct WasmRuntime {
    core: Rc<RefCell<RuntimeCore<WasmIoHandler>>>,
}

#[wasm_bindgen]
impl WasmRuntime {
    /// Create a new WasmRuntime.
    ///
    /// Storage is synchronous (in-memory via MemoryIoHandler).
    ///
    /// # Arguments
    /// * `schema_json` - JSON-encoded schema definition
    /// * `app_id` - Application identifier
    /// * `env` - Environment (e.g., "dev", "prod")
    /// * `user_branch` - User's branch name (e.g., "main")
    /// * `tier` - Optional persistence tier ("worker", "edge", "core").
    ///            Set for server nodes to enable ack emission.
    #[wasm_bindgen(constructor)]
    pub fn new(
        schema_json: &str,
        app_id: &str,
        env: &str,
        user_branch: &str,
        tier: Option<String>,
    ) -> Result<WasmRuntime, JsError> {
        #[cfg(feature = "console_error_panic_hook")]
        console_error_panic_hook::set_once();

        // Parse schema
        let wasm_schema: WasmSchema = serde_json::from_str(schema_json)
            .map_err(|e| JsError::new(&format!("Invalid schema JSON: {}", e)))?;

        let schema: Schema = wasm_schema
            .try_into()
            .map_err(|e: String| JsError::new(&e))?;

        // Parse optional tier
        let persistence_tier = tier
            .as_deref()
            .map(parse_tier)
            .transpose()?;

        // Create sync manager
        let mut sync_manager = SyncManager::new();
        if let Some(t) = persistence_tier {
            sync_manager = sync_manager.with_tier(t);
        }

        // Create schema manager
        let schema_manager = SchemaManager::new(
            sync_manager,
            schema,
            AppId::from_name(app_id),
            env,
            user_branch,
        )
        .map_err(|e| JsError::new(&format!("Failed to create SchemaManager: {:?}", e)))?;

        // Create IoHandler (synchronous in-memory storage)
        let io_handler = WasmIoHandler::new();

        // Create RuntimeCore
        let core = RuntimeCore::new(schema_manager, io_handler);

        // Wrap in Rc<RefCell>
        let core_rc = Rc::new(RefCell::new(core));

        // Set the core_ref on the IoHandler
        {
            let mut core_guard = core_rc.borrow_mut();
            core_guard
                .io_handler_mut()
                .set_core_ref(Rc::downgrade(&core_rc));
        }

        Ok(WasmRuntime { core: core_rc })
    }

    /// Called by JS when a sync message arrives from the server.
    ///
    /// # Arguments
    /// * `message_json` - JSON-encoded SyncPayload
    #[wasm_bindgen(js_name = onSyncMessageReceived)]
    pub fn on_sync_message_received(&self, message_json: &str) -> Result<(), JsError> {
        let payload: SyncPayload = serde_json::from_str(message_json)
            .map_err(|e| JsError::new(&format!("Invalid sync message: {}", e)))?;

        let entry = InboxEntry {
            source: Source::Server(ServerId::new()),
            payload,
        };

        self.core.borrow_mut().park_sync_message(entry);
        Ok(())
    }

    /// Register a callback for outgoing sync messages.
    #[wasm_bindgen(js_name = onSyncMessageToSend)]
    pub fn on_sync_message_to_send(&self, callback: Function) {
        self.core
            .borrow_mut()
            .io_handler_mut()
            .set_sync_callback(callback);
    }

    // =========================================================================
    // CRUD Operations
    // =========================================================================

    /// Insert a row into a table.
    ///
    /// # Returns
    /// The new row's ObjectId as a UUID string.
    #[wasm_bindgen]
    pub fn insert(&self, table: &str, values: JsValue) -> Result<String, JsError> {
        let wasm_values: Vec<WasmValue> = serde_wasm_bindgen::from_value(values)?;
        let groove_values: Vec<Value> = wasm_values
            .into_iter()
            .map(TryInto::try_into)
            .collect::<Result<_, _>>()
            .map_err(|e: String| JsError::new(&e))?;

        let mut core = self.core.borrow_mut();
        let result = core
            .insert(table, groove_values, None)
            .map_err(|e| JsError::new(&format!("Insert failed: {:?}", e)))?;

        Ok(result.uuid().to_string())
    }

    /// Execute a query and return results as a Promise.
    ///
    /// Optional `settled_tier` holds delivery until the tier confirms.
    #[wasm_bindgen]
    pub fn query(
        &self,
        query_json: &str,
        session_json: Option<String>,
        settled_tier: Option<String>,
    ) -> Result<js_sys::Promise, JsError> {
        let query = parse_query(query_json).map_err(|e| JsError::new(&e))?;

        let session = if let Some(json) = session_json {
            Some(
                serde_json::from_str::<Session>(&json)
                    .map_err(|e| JsError::new(&format!("Invalid session JSON: {}", e)))?,
            )
        } else {
            None
        };

        let tier = settled_tier
            .as_deref()
            .map(parse_tier)
            .transpose()?;

        let future = {
            let mut core = self.core.borrow_mut();
            core.query_with_settled_tier(query, session, tier)
        };

        let promise = wasm_bindgen_futures::future_to_promise(async move {
            let results = future
                .await
                .map_err(|e| JsValue::from_str(&format!("Query failed: {:?}", e)))?;

            let wasm_results: Vec<_> = results
                .into_iter()
                .map(|(id, values)| {
                    let wasm_values: Vec<WasmValue> = values.into_iter().map(Into::into).collect();
                    serde_json::json!({
                        "id": id.uuid().to_string(),
                        "values": wasm_values
                    })
                })
                .collect();

            let serializer = serde_wasm_bindgen::Serializer::new().serialize_maps_as_objects(true);
            wasm_results
                .serialize(&serializer)
                .map_err(|e| JsValue::from_str(&format!("Serialization failed: {:?}", e)))
        });

        Ok(promise)
    }

    /// Update a row by ObjectId.
    #[wasm_bindgen]
    pub fn update(&self, object_id: &str, values: JsValue) -> Result<(), JsError> {
        let uuid = uuid::Uuid::parse_str(object_id)
            .map_err(|e| JsError::new(&format!("Invalid ObjectId: {}", e)))?;
        let oid = ObjectId::from_uuid(uuid);

        let partial_values: HashMap<String, WasmValue> = serde_wasm_bindgen::from_value(values)?;

        let updates: Vec<(String, Value)> = partial_values
            .into_iter()
            .map(|(k, v)| {
                let groove_value: Value = v.try_into()?;
                Ok((k, groove_value))
            })
            .collect::<Result<_, String>>()
            .map_err(|e: String| JsError::new(&e))?;

        let mut core = self.core.borrow_mut();
        core.update(oid, updates, None)
            .map_err(|e| JsError::new(&format!("Update failed: {:?}", e)))?;

        Ok(())
    }

    /// Delete a row by ObjectId.
    #[wasm_bindgen]
    pub fn delete(&self, object_id: &str) -> Result<(), JsError> {
        let uuid = uuid::Uuid::parse_str(object_id)
            .map_err(|e| JsError::new(&format!("Invalid ObjectId: {}", e)))?;
        let oid = ObjectId::from_uuid(uuid);

        let mut core = self.core.borrow_mut();
        core.delete(oid, None)
            .map_err(|e| JsError::new(&format!("Delete failed: {:?}", e)))?;

        Ok(())
    }

    // =========================================================================
    // Persisted CRUD Operations
    // =========================================================================

    /// Insert a row and return a Promise that resolves when the tier acks.
    ///
    /// `tier` must be one of: "worker", "edge", "core".
    #[wasm_bindgen(js_name = insertPersisted)]
    pub fn insert_persisted(
        &self,
        table: &str,
        values: JsValue,
        tier: &str,
    ) -> Result<js_sys::Promise, JsError> {
        let persistence_tier = parse_tier(tier)?;

        let wasm_values: Vec<WasmValue> = serde_wasm_bindgen::from_value(values)?;
        let groove_values: Vec<Value> = wasm_values
            .into_iter()
            .map(TryInto::try_into)
            .collect::<Result<_, _>>()
            .map_err(|e: String| JsError::new(&e))?;

        let (object_id, receiver) = {
            let mut core = self.core.borrow_mut();
            core.insert_persisted(table, groove_values, None, persistence_tier)
                .map_err(|e| JsError::new(&format!("Insert failed: {:?}", e)))?
        };

        let id_str = object_id.uuid().to_string();
        let promise = wasm_bindgen_futures::future_to_promise(async move {
            let _ = receiver.await;
            Ok(JsValue::from_str(&id_str))
        });

        Ok(promise)
    }

    /// Update a row and return a Promise that resolves when the tier acks.
    #[wasm_bindgen(js_name = updatePersisted)]
    pub fn update_persisted(
        &self,
        object_id: &str,
        values: JsValue,
        tier: &str,
    ) -> Result<js_sys::Promise, JsError> {
        let persistence_tier = parse_tier(tier)?;

        let uuid = uuid::Uuid::parse_str(object_id)
            .map_err(|e| JsError::new(&format!("Invalid ObjectId: {}", e)))?;
        let oid = ObjectId::from_uuid(uuid);

        let partial_values: HashMap<String, WasmValue> = serde_wasm_bindgen::from_value(values)?;
        let updates: Vec<(String, Value)> = partial_values
            .into_iter()
            .map(|(k, v)| {
                let groove_value: Value = v.try_into()?;
                Ok((k, groove_value))
            })
            .collect::<Result<_, String>>()
            .map_err(|e: String| JsError::new(&e))?;

        let receiver = {
            let mut core = self.core.borrow_mut();
            core.update_persisted(oid, updates, None, persistence_tier)
                .map_err(|e| JsError::new(&format!("Update failed: {:?}", e)))?
        };

        let promise = wasm_bindgen_futures::future_to_promise(async move {
            let _ = receiver.await;
            Ok(JsValue::undefined())
        });

        Ok(promise)
    }

    /// Delete a row and return a Promise that resolves when the tier acks.
    #[wasm_bindgen(js_name = deletePersisted)]
    pub fn delete_persisted(
        &self,
        object_id: &str,
        tier: &str,
    ) -> Result<js_sys::Promise, JsError> {
        let persistence_tier = parse_tier(tier)?;

        let uuid = uuid::Uuid::parse_str(object_id)
            .map_err(|e| JsError::new(&format!("Invalid ObjectId: {}", e)))?;
        let oid = ObjectId::from_uuid(uuid);

        let receiver = {
            let mut core = self.core.borrow_mut();
            core.delete_persisted(oid, None, persistence_tier)
                .map_err(|e| JsError::new(&format!("Delete failed: {:?}", e)))?
        };

        let promise = wasm_bindgen_futures::future_to_promise(async move {
            let _ = receiver.await;
            Ok(JsValue::undefined())
        });

        Ok(promise)
    }

    // =========================================================================
    // Subscriptions
    // =========================================================================

    /// Subscribe to a query with a callback.
    ///
    /// # Returns
    /// Subscription handle (f64) for later unsubscription.
    #[cfg(target_arch = "wasm32")]
    #[wasm_bindgen]
    pub fn subscribe(
        &self,
        query_json: &str,
        on_update: Function,
        session_json: Option<String>,
        settled_tier: Option<String>,
    ) -> Result<f64, JsError> {
        let query = parse_query(query_json).map_err(|e| JsError::new(&e))?;

        let session = if let Some(json) = session_json {
            Some(
                serde_json::from_str::<Session>(&json)
                    .map_err(|e| JsError::new(&format!("Invalid session JSON: {}", e)))?,
            )
        } else {
            None
        };

        let tier = settled_tier
            .as_deref()
            .map(parse_tier)
            .transpose()?;

        let callback = move |delta: SubscriptionDelta| {
            let row_to_json = |row: &Row, descriptor: &RowDescriptor| -> serde_json::Value {
                let values = decode_row(descriptor, &row.data)
                    .map(|vals| vals.into_iter().map(WasmValue::from).collect::<Vec<_>>())
                    .unwrap_or_default();
                serde_json::json!({
                    "id": row.id.uuid().to_string(),
                    "values": values
                })
            };

            let descriptor = &delta.descriptor;

            let delta_json = serde_json::json!({
                "added": delta.delta.added.iter()
                    .map(|row| row_to_json(row, descriptor))
                    .collect::<Vec<_>>(),
                "removed": delta.delta.removed.iter()
                    .map(|row| row_to_json(row, descriptor))
                    .collect::<Vec<_>>(),
                "updated": delta.delta.updated.iter()
                    .map(|(old, new)| [row_to_json(old, descriptor), row_to_json(new, descriptor)])
                    .collect::<Vec<_>>()
            });

            if let Ok(json_str) = serde_json::to_string(&delta_json) {
                let _ = on_update.call1(&JsValue::NULL, &JsValue::from_str(&json_str));
            }
        };

        let handle = self
            .core
            .borrow_mut()
            .subscribe_with_settled_tier(query, callback, session, tier)
            .map_err(|e| JsError::new(&format!("Subscribe failed: {:?}", e)))?;

        Ok(handle.0 as f64)
    }

    /// Unsubscribe from a query.
    #[cfg(target_arch = "wasm32")]
    #[wasm_bindgen]
    pub fn unsubscribe(&self, handle: f64) {
        self.core
            .borrow_mut()
            .unsubscribe(SubscriptionHandle(handle as u64));
    }

    // =========================================================================
    // Sync Operations
    // =========================================================================

    /// Add a server connection.
    #[wasm_bindgen(js_name = addServer)]
    pub fn add_server(&self) {
        let server_id = ServerId::new();
        let mut core = self.core.borrow_mut();
        core.add_server(server_id);
    }

    /// Add a client connection (for server-side use in tests).
    #[wasm_bindgen(js_name = addClient)]
    pub fn add_client(&self) -> String {
        let client_id = ClientId::new();
        let mut core = self.core.borrow_mut();
        core.add_client(client_id, None);
        client_id.0.to_string()
    }

    /// Add a client connection with full sync (for test loopback).
    #[wasm_bindgen(js_name = addClientWithFullSync)]
    pub fn add_client_with_full_sync(&self) -> String {
        let client_id = ClientId::new();
        let mut core = self.core.borrow_mut();
        core.add_client_with_full_sync(client_id, None);
        client_id.0.to_string()
    }

    // =========================================================================
    // Schema Access
    // =========================================================================

    /// Get the current schema as JSON.
    #[wasm_bindgen(js_name = getSchema)]
    pub fn get_schema(&self) -> Result<JsValue, JsError> {
        let core = self.core.borrow();
        let schema = core.current_schema();
        let wasm_schema = WasmSchema::from(schema);
        Ok(serde_wasm_bindgen::to_value(&wasm_schema)?)
    }
}
