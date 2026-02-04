//! WasmRuntime - Main entry point for JavaScript applications.
//!
//! Provides the core Jazz database functionality exposed to JavaScript:
//! - CRUD operations (insert, query, update, delete)
//! - Reactive subscriptions with callback-based updates
//! - Sync message handling for server communication
//!
//! # Architecture
//!
//! - `WasmIoHandler` implements `IoHandler` using JS callbacks and spawn_local
//! - `WasmRuntime` wraps `Rc<RefCell<RuntimeCore<WasmIoHandler>>>`
//! - Storage requests are fire-and-forget to JS; responses come via `on_storage_response`
//! - `schedule_batched_tick` uses `wasm_bindgen_futures::spawn_local` (debounced)
//! - No explicit tick loops - scheduling emerges from IoHandler

use std::cell::RefCell;
use std::collections::HashMap;
use std::rc::{Rc, Weak};

use js_sys::Function;
use serde::Serialize;
use wasm_bindgen::prelude::*;

use groove::io_handler::IoHandler;
use groove::object::ObjectId;
use groove::query_manager::encoding::decode_row;
use groove::query_manager::session::Session;
use groove::query_manager::types::{Row, RowDescriptor, Schema, Value};
use groove::runtime_core::RuntimeCore;
#[cfg(target_arch = "wasm32")]
use groove::runtime_core::{SubscriptionDelta, SubscriptionHandle};
use groove::schema_manager::{AppId, SchemaManager};
use groove::storage::StorageRequest;
use groove::sync_manager::{InboxEntry, OutboxEntry, ServerId, Source, SyncManager, SyncPayload};

use crate::query::parse_query;
use crate::types::{
    storage_request_to_wasm, wasm_response_to_storage, WasmSchema, WasmStorageResponse, WasmValue,
};

// ============================================================================
// WasmIoHandler
// ============================================================================

/// IoHandler implementation for WASM.
///
/// - Storage requests are sent to JS via callback (fire-and-forget)
/// - Sync messages are sent via callback
/// - `schedule_batched_tick` uses spawn_local (debounced)
pub struct WasmIoHandler {
    /// JS callback for storage requests (fire-and-forget).
    storage_callback: Function,
    /// JS callback for sync messages.
    sync_callback: Option<Function>,
    /// Debounce flag for scheduled ticks.
    scheduled: Rc<RefCell<bool>>,
    /// Weak reference back to RuntimeCore for spawned tasks.
    core_ref: Weak<RefCell<RuntimeCore<WasmIoHandler>>>,
}

impl WasmIoHandler {
    fn new(storage_callback: Function) -> Self {
        Self {
            storage_callback,
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
    fn send_storage_request(&mut self, request: StorageRequest) {
        // Convert to WASM type and serialize directly to JS object
        // Use serialize_maps_as_objects to convert HashMap to plain objects (not JS Map)
        let wasm_request = storage_request_to_wasm(request);
        let serializer = serde_wasm_bindgen::Serializer::new().serialize_maps_as_objects(true);
        if let Ok(js_value) = wasm_request.serialize(&serializer) {
            let _ = self.storage_callback.call1(&JsValue::NULL, &js_value);
        }
    }

    fn send_sync_message(&mut self, message: OutboxEntry) {
        if let Some(ref callback) = self.sync_callback {
            // Serialize just the payload
            if let Ok(json) = serde_json::to_string(&message.payload) {
                let js_value = JsValue::from_str(&json);
                let _ = callback.call1(&JsValue::NULL, &js_value);
            }
        }
    }

    fn schedule_batched_tick(&self) {
        // Debounce: only schedule if not already scheduled
        let mut scheduled = self.scheduled.borrow_mut();
        if !*scheduled {
            *scheduled = true;

            let core_ref = self.core_ref.clone();
            let flag = self.scheduled.clone();

            wasm_bindgen_futures::spawn_local(async move {
                // Clear the scheduled flag
                *flag.borrow_mut() = false;

                // Call batched_tick on the core
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
    /// # Arguments
    /// * `storage_callback` - JS function called with storage requests (JSON string)
    /// * `schema_json` - JSON-encoded schema definition
    /// * `app_id` - Application identifier
    /// * `env` - Environment (e.g., "dev", "prod")
    /// * `user_branch` - User's branch name (e.g., "main")
    #[wasm_bindgen(constructor)]
    pub fn new(
        storage_callback: Function,
        schema_json: &str,
        app_id: &str,
        env: &str,
        user_branch: &str,
    ) -> Result<WasmRuntime, JsError> {
        // Set up panic hook for better error messages
        #[cfg(feature = "console_error_panic_hook")]
        console_error_panic_hook::set_once();

        // Parse schema
        let wasm_schema: WasmSchema = serde_json::from_str(schema_json)
            .map_err(|e| JsError::new(&format!("Invalid schema JSON: {}", e)))?;

        let schema: Schema = wasm_schema
            .try_into()
            .map_err(|e: String| JsError::new(&e))?;

        // Create sync manager
        let sync_manager = SyncManager::new();

        // Create schema manager
        let schema_manager = SchemaManager::new(
            sync_manager,
            schema,
            AppId::from_name(app_id),
            env,
            user_branch,
        )
        .map_err(|e| JsError::new(&format!("Failed to create SchemaManager: {:?}", e)))?;

        // Create IoHandler
        let io_handler = WasmIoHandler::new(storage_callback);

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

    /// Called by JS when a storage response arrives.
    ///
    /// # Arguments
    /// * `response` - Storage response object (WasmStorageResponse)
    #[wasm_bindgen(js_name = onStorageResponse)]
    pub fn on_storage_response(&self, response: JsValue) -> Result<(), JsError> {
        let wasm_response: WasmStorageResponse = serde_wasm_bindgen::from_value(response)
            .map_err(|e| JsError::new(&format!("Invalid storage response: {}", e)))?;

        let response = wasm_response_to_storage(wasm_response)
            .map_err(|e| JsError::new(&format!("Failed to convert response: {}", e)))?;

        self.core.borrow_mut().park_storage_response(response);
        Ok(())
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
    /// # Arguments
    /// * `table` - Table name
    /// * `values` - JS array of values
    ///
    /// # Returns
    /// The new row's ObjectId as a UUID string.
    #[wasm_bindgen]
    pub fn insert(&self, table: &str, values: JsValue) -> Result<String, JsError> {
        // Parse values
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
        // immediate_tick is called by RuntimeCore::insert

        Ok(result.uuid().to_string())
    }

    /// Execute a query and return results as a Promise.
    ///
    /// # Arguments
    /// * `query_json` - JSON-encoded query specification
    /// * `session_json` - Optional JSON-encoded session for policy evaluation
    ///
    /// # Returns
    /// Promise that resolves to JSON-encoded array of `{id: string, values: Value[]}` objects.
    #[wasm_bindgen]
    pub fn query(
        &self,
        query_json: &str,
        session_json: Option<String>,
    ) -> Result<js_sys::Promise, JsError> {
        let query = parse_query(query_json).map_err(|e| JsError::new(&e))?;

        // Parse session from JSON if provided
        let session = if let Some(json) = session_json {
            Some(
                serde_json::from_str::<Session>(&json)
                    .map_err(|e| JsError::new(&format!("Invalid session JSON: {}", e)))?,
            )
        } else {
            None
        };

        // Get the query future from RuntimeCore
        let future = {
            let mut core = self.core.borrow_mut();
            core.query(query, session)
        };

        // Convert to a JS Promise
        let promise = wasm_bindgen_futures::future_to_promise(async move {
            let results = future
                .await
                .map_err(|e| JsValue::from_str(&format!("Query failed: {:?}", e)))?;

            // Convert results
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
    ///
    /// # Arguments
    /// * `object_id` - UUID string of the row to update
    /// * `values` - JS object mapping column names to new values
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
        // immediate_tick is called by RuntimeCore::update

        Ok(())
    }

    /// Delete a row by ObjectId.
    ///
    /// # Arguments
    /// * `object_id` - UUID string of the row to delete
    #[wasm_bindgen]
    pub fn delete(&self, object_id: &str) -> Result<(), JsError> {
        let uuid = uuid::Uuid::parse_str(object_id)
            .map_err(|e| JsError::new(&format!("Invalid ObjectId: {}", e)))?;
        let oid = ObjectId::from_uuid(uuid);

        let mut core = self.core.borrow_mut();
        core.delete(oid, None)
            .map_err(|e| JsError::new(&format!("Delete failed: {:?}", e)))?;
        // immediate_tick is called by RuntimeCore::delete

        Ok(())
    }

    // =========================================================================
    // Subscriptions
    // =========================================================================

    /// Subscribe to a query with a callback.
    ///
    /// # Arguments
    /// * `query_json` - JSON-encoded query specification
    /// * `on_update` - Callback function invoked with updates (receives JSON delta)
    /// * `session_json` - Optional JSON-encoded session for policy evaluation
    ///
    /// # Returns
    /// Subscription handle (u64) for later unsubscription.
    #[cfg(target_arch = "wasm32")]
    #[wasm_bindgen]
    pub fn subscribe(
        &self,
        query_json: &str,
        on_update: Function,
        session_json: Option<String>,
    ) -> Result<f64, JsError> {
        let query = parse_query(query_json).map_err(|e| JsError::new(&e))?;

        // Parse session from JSON if provided
        let session = if let Some(json) = session_json {
            Some(
                serde_json::from_str::<Session>(&json)
                    .map_err(|e| JsError::new(&format!("Invalid session JSON: {}", e)))?,
            )
        } else {
            None
        };

        // Create a Rust callback that bridges to the JS function.
        // The callback decodes row data, converts to WasmValues, and serializes.
        let callback = move |delta: SubscriptionDelta| {
            // Helper to decode a row and convert to WasmRow JSON format
            let row_to_json = |row: &Row, descriptor: &RowDescriptor| -> serde_json::Value {
                let values = decode_row(descriptor, &row.data)
                    .map(|vals| vals.into_iter().map(|v| WasmValue::from(v)).collect::<Vec<_>>())
                    .unwrap_or_default();
                serde_json::json!({
                    "id": row.id.uuid().to_string(),
                    "values": values
                })
            };

            let descriptor = &delta.descriptor;

            // Build WasmRowDelta-compatible JSON
            let delta_json = serde_json::json!({
                "added": delta.delta.added.iter()
                    .map(|row| row_to_json(row, descriptor))
                    .collect::<Vec<_>>(),
                "removed": delta.delta.removed.iter()
                    .map(|row| row_to_json(row, descriptor))
                    .collect::<Vec<_>>(),
                "updated": delta.delta.updated.iter()
                    .map(|(old, new)| [row_to_json(old, descriptor), row_to_json(new, descriptor)])
                    .collect::<Vec<_>>(),
                "pending": delta.delta.pending
            });

            if let Ok(json_str) = serde_json::to_string(&delta_json) {
                let _ = on_update.call1(&JsValue::NULL, &JsValue::from_str(&json_str));
            }
        };

        let handle = self
            .core
            .borrow_mut()
            .subscribe(query, callback, session)
            .map_err(|e| JsError::new(&format!("Subscribe failed: {:?}", e)))?;

        // Return handle as f64 since JS doesn't have u64
        Ok(handle.0 as f64)
    }

    /// Unsubscribe from a query.
    ///
    /// # Arguments
    /// * `handle` - Handle returned from `subscribe()`
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
        // immediate_tick is called by RuntimeCore::add_server
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
