//! WasmRuntime - Main entry point for JavaScript applications.
//!
//! Provides the core Jazz database functionality exposed to JavaScript:
//! - CRUD operations (insert, query, update, delete)
//! - Reactive subscriptions with callback-based updates
//! - Sync message handling for server communication

use std::cell::RefCell;
use std::collections::HashMap;
use std::rc::Rc;

use js_sys::Function;
use serde::Serialize;
use wasm_bindgen::prelude::*;

use groove::driver::Driver;
use groove::object::ObjectId;
use groove::query_manager::manager::QueryUpdate;
use groove::query_manager::types::{Schema, TableName, Value};
use groove::schema_manager::{AppId, SchemaManager};
use groove::storage::{StorageRequest, StorageResponse};
use groove::sync_manager::{InboxEntry, OutboxEntry, ServerId, Source, SyncManager, SyncPayload};

use crate::driver_bridge::{JsStorageDriver, PendingStorage, WasmDriverBridge};
use crate::query::parse_query;
use crate::types::{WasmSchema, WasmValue};

// ============================================================================
// Async Driver Wrapper
// ============================================================================

/// Driver that buffers requests for async processing.
///
/// Since the Groove `Driver` trait is synchronous but JavaScript storage is async,
/// this wrapper collects requests during synchronous operations and processes
/// them in batches during `tick()` calls.
struct AsyncDriverWrapper {
    pending: Rc<RefCell<PendingStorage>>,
}

impl AsyncDriverWrapper {
    fn new(pending: Rc<RefCell<PendingStorage>>) -> Self {
        Self { pending }
    }
}

impl Driver for AsyncDriverWrapper {
    fn process(&mut self, requests: Vec<StorageRequest>) -> Vec<StorageResponse> {
        // Queue requests for async processing
        self.pending.borrow_mut().queue_requests(requests);
        // Return any responses that were processed in a previous tick
        self.pending.borrow_mut().take_responses()
    }
}

// ============================================================================
// WasmRuntime
// ============================================================================

/// Main runtime for JavaScript applications.
///
/// Wraps the Groove SchemaManager and provides async CRUD operations,
/// reactive subscriptions, and sync message handling.
#[wasm_bindgen]
pub struct WasmRuntime {
    schema_manager: SchemaManager,
    driver_bridge: WasmDriverBridge,
    pending_storage: Rc<RefCell<PendingStorage>>,
    next_subscription_id: u32,
    subscriptions: HashMap<u32, SubscriptionState>,
    sync_message_callback: Option<Function>,
}

struct SubscriptionState {
    #[allow(dead_code)]
    query_json: String,
    callback: Function,
    #[allow(dead_code)]
    query_sub_id: groove::query_manager::QuerySubscriptionId,
}

#[wasm_bindgen]
impl WasmRuntime {
    /// Create a new WasmRuntime.
    ///
    /// # Arguments
    /// * `driver` - JavaScript storage driver implementing the StorageDriver interface
    /// * `schema_json` - JSON-encoded schema definition
    /// * `app_id` - Application identifier
    /// * `env` - Environment (e.g., "dev", "prod")
    /// * `user_branch` - User's branch name (e.g., "main")
    #[wasm_bindgen(constructor)]
    pub fn new(
        driver: JsStorageDriver,
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

        // Create pending storage for async driver
        let pending_storage = Rc::new(RefCell::new(PendingStorage::new()));

        // Create sync manager
        let sync_manager = SyncManager::new();

        // Create schema manager with async driver wrapper
        let mut async_driver = AsyncDriverWrapper::new(pending_storage.clone());
        let mut schema_manager =
            SchemaManager::new(sync_manager, schema, AppId::from_name(app_id), env, user_branch)
                .map_err(|e| JsError::new(&format!("Failed to create SchemaManager: {:?}", e)))?;

        // Load indices from driver (will queue storage requests)
        schema_manager
            .query_manager_mut()
            .load_indices_from_driver(&mut async_driver);

        Ok(WasmRuntime {
            schema_manager,
            driver_bridge: WasmDriverBridge::new(driver),
            pending_storage,
            next_subscription_id: 0,
            subscriptions: HashMap::new(),
            sync_message_callback: None,
        })
    }

    /// Process pending storage operations.
    ///
    /// This must be called periodically (via requestAnimationFrame or setInterval)
    /// to process async storage operations.
    #[wasm_bindgen]
    pub async fn tick(&mut self) -> Result<(), JsError> {
        // 1. Async storage settling (WASM-specific: driver is async)
        loop {
            // Send pending requests to JS driver and await responses
            if self.pending_storage.borrow().has_pending_requests() {
                let requests = self.pending_storage.borrow_mut().take_requests();
                let responses = self
                    .driver_bridge
                    .process_async(requests)
                    .await
                    .map_err(|e| JsError::new(&e))?;
                self.pending_storage.borrow_mut().store_responses(responses);
            }

            // Let schema manager consume responses (may queue more requests)
            let mut async_driver = AsyncDriverWrapper::new(self.pending_storage.clone());
            let made_progress = self.schema_manager
                .query_manager_mut()
                .process_storage_with_driver(&mut async_driver);

            // If no progress and no new pending requests, storage has settled
            if !made_progress && !self.pending_storage.borrow().has_pending_requests() {
                break;
            }
        }

        // 2. Use shared tick logic: process + collect outbox + collect updates
        let result = self.schema_manager.tick_settled();

        // 3. Emit sync outbox entries via JS callback (WASM-specific)
        for entry in result.outbox {
            self.emit_sync_message(&entry)?;
        }

        // 4. Emit subscription updates via JS callbacks (WASM-specific)
        self.emit_subscription_updates_from_result(&result.subscription_updates)?;

        Ok(())
    }

    /// Insert a row into a table.
    ///
    /// # Arguments
    /// * `table` - Table name
    /// * `values` - JSON-encoded array of values
    ///
    /// # Returns
    /// The new row's ObjectId as a UUID string.
    #[wasm_bindgen]
    pub async fn insert(&mut self, table: &str, values: JsValue) -> Result<String, JsError> {
        // Parse values
        let wasm_values: Vec<WasmValue> = serde_wasm_bindgen::from_value(values)?;
        let groove_values: Vec<Value> = wasm_values
            .into_iter()
            .map(TryInto::try_into)
            .collect::<Result<_, _>>()
            .map_err(|e: String| JsError::new(&e))?;

        // Perform insert
        let result = self
            .schema_manager
            .insert(table, &groove_values)
            .map_err(|e| JsError::new(&format!("Insert failed: {:?}", e)))?;

        // Process pending operations
        self.tick().await?;

        Ok(result.row_id.uuid().to_string())
    }

    /// Execute a query and return results.
    ///
    /// # Arguments
    /// * `query_json` - JSON-encoded query specification
    ///
    /// # Returns
    /// JSON-encoded array of `{id: string, values: Value[]}` objects.
    #[wasm_bindgen]
    pub async fn query(&mut self, query_json: &str) -> Result<JsValue, JsError> {
        // Parse query
        let query = parse_query(query_json).map_err(|e| JsError::new(&e))?;

        // Execute query
        let results = self
            .schema_manager
            .execute(query)
            .map_err(|e| JsError::new(&format!("Query failed: {:?}", e)))?;

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

        // Use serialize_maps_as_objects to get plain JS objects instead of Map
        let serializer = serde_wasm_bindgen::Serializer::new().serialize_maps_as_objects(true);
        Ok(wasm_results.serialize(&serializer)?)
    }

    /// Update a row by ObjectId.
    ///
    /// # Arguments
    /// * `object_id` - UUID string of the row to update
    /// * `values` - JSON-encoded object mapping column names to new values
    #[wasm_bindgen]
    pub async fn update(&mut self, object_id: &str, values: JsValue) -> Result<(), JsError> {
        // Parse object ID
        let uuid = uuid::Uuid::parse_str(object_id)
            .map_err(|e| JsError::new(&format!("Invalid ObjectId: {}", e)))?;
        let oid = ObjectId::from_uuid(uuid);

        // Parse partial values as {column: value} object
        let partial_values: HashMap<String, WasmValue> = serde_wasm_bindgen::from_value(values)?;

        // Get current row
        let (table, mut current_values) = self
            .schema_manager
            .query_manager_mut()
            .get_row(oid)
            .ok_or_else(|| JsError::new("Row not found"))?;

        // Get schema for column lookup
        let schema = self.schema_manager.current_schema();
        let table_name = TableName::new(&table);
        let table_schema = schema
            .get(&table_name)
            .ok_or_else(|| JsError::new("Table not found"))?;

        // Merge updates
        for (col_name, wasm_value) in partial_values {
            if let Some(idx) = table_schema.descriptor.column_index(&col_name) {
                let groove_value: Value = wasm_value
                    .try_into()
                    .map_err(|e: String| JsError::new(&e))?;
                current_values[idx] = groove_value;
            } else {
                return Err(JsError::new(&format!("Column '{}' not found", col_name)));
            }
        }

        // Perform update
        self.schema_manager
            .query_manager_mut()
            .update_with_session(oid, &current_values, None)
            .map_err(|e| JsError::new(&format!("Update failed: {:?}", e)))?;

        // Process pending operations
        self.tick().await?;

        Ok(())
    }

    /// Delete a row by ObjectId.
    ///
    /// # Arguments
    /// * `object_id` - UUID string of the row to delete
    #[wasm_bindgen]
    pub async fn delete(&mut self, object_id: &str) -> Result<(), JsError> {
        // Parse object ID
        let uuid = uuid::Uuid::parse_str(object_id)
            .map_err(|e| JsError::new(&format!("Invalid ObjectId: {}", e)))?;
        let oid = ObjectId::from_uuid(uuid);

        // Perform delete
        self.schema_manager
            .query_manager_mut()
            .delete_with_session(oid, None)
            .map_err(|e| JsError::new(&format!("Delete failed: {:?}", e)))?;

        // Process pending operations
        self.tick().await?;

        Ok(())
    }

    /// Subscribe to a query with a callback.
    ///
    /// The callback will be invoked with a `RowDelta` object whenever the
    /// query results change.
    ///
    /// # Arguments
    /// * `query_json` - JSON-encoded query specification
    /// * `on_update` - Callback function invoked with updates
    ///
    /// # Returns
    /// Subscription ID for later unsubscription.
    #[wasm_bindgen]
    pub async fn subscribe(
        &mut self,
        query_json: &str,
        on_update: Function,
    ) -> Result<u32, JsError> {
        // Parse query
        let query = parse_query(query_json).map_err(|e| JsError::new(&e))?;

        // Subscribe through QueryManager
        let query_sub_id = self
            .schema_manager
            .query_manager_mut()
            .subscribe_with_session(query, None)
            .map_err(|e| JsError::new(&format!("Subscribe failed: {:?}", e)))?;

        // Allocate subscription ID
        let sub_id = self.next_subscription_id;
        self.next_subscription_id += 1;

        // Store subscription state
        self.subscriptions.insert(
            sub_id,
            SubscriptionState {
                query_json: query_json.to_string(),
                callback: on_update,
                query_sub_id,
            },
        );

        Ok(sub_id)
    }

    /// Unsubscribe from a query.
    ///
    /// # Arguments
    /// * `subscription_id` - ID returned from `subscribe()`
    #[wasm_bindgen]
    pub fn unsubscribe(&mut self, subscription_id: u32) {
        if let Some(state) = self.subscriptions.remove(&subscription_id) {
            self.schema_manager
                .query_manager_mut()
                .unsubscribe(state.query_sub_id);
        }
    }

    /// Register a callback for outgoing sync messages.
    ///
    /// The callback will be invoked with a JSON-encoded sync message whenever
    /// the runtime needs to send data to the server.
    #[wasm_bindgen(js_name = onSyncMessageToSend)]
    pub fn on_sync_message_to_send(&mut self, callback: Function) {
        self.sync_message_callback = Some(callback);
    }

    /// Process an incoming sync message from the server.
    ///
    /// # Arguments
    /// * `message` - JSON-encoded SyncPayload
    #[wasm_bindgen(js_name = onSyncMessageReceived)]
    pub fn on_sync_message_received(&mut self, message: &str) -> Result<(), JsError> {
        // Parse sync payload
        let payload: SyncPayload = serde_json::from_str(message)
            .map_err(|e| JsError::new(&format!("Invalid sync message: {}", e)))?;

        // Create inbox entry with server source (server ID doesn't matter for now)
        let entry = InboxEntry {
            source: Source::Server(ServerId::new()),
            payload,
        };

        // Push to sync manager
        self.schema_manager
            .query_manager_mut()
            .sync_manager_mut()
            .push_inbox(entry);

        Ok(())
    }

    /// Add a server connection.
    #[wasm_bindgen(js_name = addServer)]
    pub fn add_server(&mut self) {
        let server_id = ServerId::new();
        self.schema_manager
            .query_manager_mut()
            .sync_manager_mut()
            .add_server(server_id);
    }

    /// Get the current schema as JSON.
    #[wasm_bindgen(js_name = getSchema)]
    pub fn get_schema(&self) -> Result<JsValue, JsError> {
        let schema = self.schema_manager.current_schema();
        let wasm_schema = WasmSchema::from(schema);
        Ok(serde_wasm_bindgen::to_value(&wasm_schema)?)
    }

}


// Private helper methods
impl WasmRuntime {
    fn emit_sync_message(&self, entry: &OutboxEntry) -> Result<(), JsError> {
        if let Some(ref callback) = self.sync_message_callback {
            // Serialize just the payload, not the full OutboxEntry
            let json = serde_json::to_string(&entry.payload)
                .map_err(|e| JsError::new(&format!("Serialize error: {}", e)))?;
            let js_value = JsValue::from_str(&json);
            callback
                .call1(&JsValue::NULL, &js_value)
                .map_err(|e| JsError::new(&format!("Callback error: {:?}", e)))?;
        }
        Ok(())
    }

    fn emit_subscription_updates_from_result(&self, updates: &[QueryUpdate]) -> Result<(), JsError> {
        for update in updates {
            // Find the subscription by query_sub_id
            for (_sub_id, state) in &self.subscriptions {
                if state.query_sub_id == update.subscription_id {
                    // Convert delta to WASM format
                    let wasm_delta = crate::types::WasmRowDelta {
                        added: update
                            .delta
                            .added
                            .iter()
                            .map(|row| {
                                // TODO: Decode row content to values using schema
                                crate::types::WasmRow {
                                    id: row.id.uuid().to_string(),
                                    values: vec![], // Placeholder - needs decoding
                                }
                            })
                            .collect(),
                        removed: update
                            .delta
                            .removed
                            .iter()
                            .map(|row| crate::types::WasmRow {
                                id: row.id.uuid().to_string(),
                                values: vec![],
                            })
                            .collect(),
                        updated: update
                            .delta
                            .updated
                            .iter()
                            .map(|(old, new)| {
                                (
                                    crate::types::WasmRow {
                                        id: old.id.uuid().to_string(),
                                        values: vec![],
                                    },
                                    crate::types::WasmRow {
                                        id: new.id.uuid().to_string(),
                                        values: vec![],
                                    },
                                )
                            })
                            .collect(),
                        pending: update.delta.pending,
                    };

                    // Invoke callback
                    let js_delta = serde_wasm_bindgen::to_value(&wasm_delta)
                        .map_err(|e| JsError::new(&e.to_string()))?;
                    state
                        .callback
                        .call1(&JsValue::NULL, &js_delta)
                        .map_err(|e| JsError::new(&format!("Callback error: {:?}", e)))?;
                    break;
                }
            }
        }

        Ok(())
    }
}
