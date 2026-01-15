//! WasmSyncedLocalNode - A synced database for browser environments.
//!
//! This module provides a WASM-friendly wrapper that integrates:
//! - `Database` for SQL operations
//! - `SyncClient` for real-time collaboration
//!
//! Usage from JavaScript:
//! ```javascript
//! const node = await WasmSyncedLocalNode.create("http://localhost:8080", "auth-token");
//! node.execute("CREATE TABLE users (id, name TEXT, email TEXT)");
//! node.execute("INSERT INTO users (name, email) VALUES ('Alice', 'alice@example.com')");
//! // Changes are automatically synced to the server and other clients
//! ```

use std::cell::RefCell;
use std::rc::Rc;
use std::sync::Arc;

use futures::StreamExt;
use js_sys::{Array, Function, Promise, Uint8Array};
use wasm_bindgen::prelude::*;
use wasm_bindgen_futures::future_to_promise;

use groove::ObjectId;
use groove::sql::{Database, ExecuteResult, encode_rows};
use groove::sync::{
    ClientEnv, ClientEnvConfig, ConnectionState, ReconnectConfig, Runtime, SseEvent,
    SubscribeRequest, SubscriptionOptions, SyncClient, calculate_reconnect_delay_with_jitter,
};

use crate::indexeddb::IndexedDbEnvironment;
use crate::runtime::WasmRuntime;
use crate::sync::WasmClientEnv;

// ============================================================================
// Connection State (JS-compatible enum)
// ============================================================================

/// Connection state for the synced node (JS-compatible).
#[wasm_bindgen]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SyncState {
    Disconnected,
    Connecting,
    Connected,
    Reconnecting,
    Stopping,
}

impl From<&ConnectionState> for SyncState {
    fn from(state: &ConnectionState) -> Self {
        match state {
            ConnectionState::Disconnected => SyncState::Disconnected,
            ConnectionState::Connecting => SyncState::Connecting,
            ConnectionState::Connected => SyncState::Connected,
            ConnectionState::Reconnecting { .. } => SyncState::Reconnecting,
            ConnectionState::Stopping => SyncState::Stopping,
        }
    }
}

// ============================================================================
// Internal State
// ============================================================================

/// Shared state for sync operations.
///
/// Uses `SyncClient` for core sync logic including:
/// - Connection state and lifecycle (Stopping for graceful shutdown)
/// - Server known state tracking
/// - Pending push queue for local writes
/// - State change and error callbacks
struct SyncedState {
    /// The sync client (owns DatabaseState, handles sync logic)
    sync_client: SyncClient<WasmClientEnv>,
    /// Database reference for SQL operations (shares Arc with sync_client)
    db: Database,
}

// ============================================================================
// WasmSyncedLocalNode
// ============================================================================

/// A synced local database for browser environments.
///
/// Combines SQL database operations with real-time sync to a server.
/// All writes are automatically pushed to the server, and incoming
/// changes from other clients are automatically applied.
#[wasm_bindgen]
pub struct WasmSyncedLocalNode {
    state: Rc<RefCell<SyncedState>>,
}

#[wasm_bindgen]
impl WasmSyncedLocalNode {
    /// Create a new synced local node with in-memory storage.
    ///
    /// @param server_url - The sync server URL (e.g., "http://localhost:8080")
    /// @param auth_token - Bearer token for authentication
    /// @param catalog_id - Optional shared catalog ID (for sync between multiple clients)
    #[wasm_bindgen(constructor)]
    pub fn new(server_url: String, auth_token: String, catalog_id: Option<String>) -> Self {
        let db = if let Some(id_str) = catalog_id {
            // Use shared catalog ID for sync
            let id = ObjectId::from_key(&id_str);
            Database::in_memory_with_catalog(id)
        } else {
            Database::in_memory()
        };

        // Create SyncClient with the database state
        let db_state = db.into_state();
        let env = WasmClientEnv::new(ClientEnvConfig::new(&server_url, &auth_token));
        let sync_client = SyncClient::new(env, Arc::clone(&db_state));
        // Recreate Database from the same state for SQL operations
        let db = Database::from_state(db_state);

        Self {
            state: Rc::new(RefCell::new(SyncedState { sync_client, db })),
        }
    }

    /// Create a synced local node with IndexedDB persistence.
    ///
    /// @param server_url - The sync server URL
    /// @param auth_token - Bearer token for authentication
    /// @param db_name - Optional database name (defaults to "groove")
    #[wasm_bindgen(js_name = withIndexedDb)]
    pub fn with_indexeddb(
        server_url: String,
        auth_token: String,
        db_name: Option<String>,
    ) -> Promise {
        future_to_promise(async move {
            let name = db_name.as_deref().unwrap_or("groove");
            let env = IndexedDbEnvironment::with_name(name).await?;
            let env = Arc::new(env);

            // Check if database already exists
            let db = if let Some(catalog_id_str) = env.get_catalog_id().await {
                // Load existing database
                let catalog_id: ObjectId = catalog_id_str
                    .parse()
                    .map_err(|e| JsValue::from_str(&format!("invalid catalog_id: {:?}", e)))?;

                Database::from_env(env.clone(), catalog_id)
                    .await
                    .map_err(|e| JsValue::from_str(&format!("failed to load database: {:?}", e)))?
            } else {
                // Create new database
                let db = Database::new(env.clone());
                let catalog_id = db.catalog_object_id();

                // Store catalog ID for future sessions
                env.set_catalog_id(&catalog_id.to_string()).await?;

                db
            };

            // Create SyncClient with the database state
            let db_state = db.into_state();
            let client_env = WasmClientEnv::new(ClientEnvConfig::new(&server_url, &auth_token));
            let sync_client = SyncClient::new(client_env, Arc::clone(&db_state));
            // Recreate Database from the same state for SQL operations
            let db = Database::from_state(db_state);

            Ok(JsValue::from(WasmSyncedLocalNode {
                state: Rc::new(RefCell::new(SyncedState { sync_client, db })),
            }))
        })
    }

    /// Set callback for sync state changes.
    ///
    /// Callback receives: (state: string)
    #[wasm_bindgen(js_name = setOnStateChange)]
    pub fn set_on_state_change(&self, callback: Function) {
        // Wrap JS Function in a Rust closure that converts ConnectionState to SyncState string
        let rust_callback: Box<dyn Fn(&ConnectionState)> = Box::new(move |state: &ConnectionState| {
            let sync_state: SyncState = state.into();
            let state_str = match sync_state {
                SyncState::Disconnected => "disconnected",
                SyncState::Connecting => "connecting",
                SyncState::Connected => "connected",
                SyncState::Reconnecting => "reconnecting",
                SyncState::Stopping => "stopping",
            };
            let _ = callback.call1(&JsValue::NULL, &JsValue::from_str(state_str));
        });
        self.state.borrow_mut().sync_client.set_on_state_change(rust_callback);
    }

    /// Set callback for sync errors.
    ///
    /// Callback receives: (message: string)
    #[wasm_bindgen(js_name = setOnError)]
    pub fn set_on_error(&self, callback: Function) {
        // Wrap JS Function in a Rust closure
        let rust_callback: Box<dyn Fn(&str)> = Box::new(move |message: &str| {
            let _ = callback.call1(&JsValue::NULL, &JsValue::from_str(message));
        });
        self.state.borrow_mut().sync_client.set_on_error(rust_callback);
    }

    /// Get current sync state.
    #[wasm_bindgen(getter, js_name = syncState)]
    pub fn sync_state(&self) -> SyncState {
        self.state.borrow().sync_client.connection_state().into()
    }

    /// Disconnect from the sync server and stop reconnection attempts.
    #[wasm_bindgen]
    pub fn disconnect(&self) {
        // Request graceful shutdown - sync loop will exit on next check
        self.state.borrow_mut().sync_client.request_stop();
    }

    /// Connect to the sync server and start receiving updates.
    ///
    /// This subscribes to the given query and starts an SSE stream
    /// to receive real-time updates from other clients. The connection
    /// automatically reconnects with exponential backoff on disconnection.
    #[wasm_bindgen]
    pub fn connect(&self, query: String) -> Promise {
        let state = Rc::clone(&self.state);

        future_to_promise(async move {
            // Reset to Connecting state (clears any previous Stopping state)
            state
                .borrow_mut()
                .sync_client
                .set_connection_state(ConnectionState::Connecting);

            // Spawn the reconnecting event loop
            let state_clone = Rc::clone(&state);
            let query_clone = query.clone();
            WasmRuntime.spawn(async move {
                sync_event_loop(&state_clone, &query_clone).await;
            });

            Ok(JsValue::TRUE)
        })
    }

    // ========================================================================
    // SQL Operations
    // ========================================================================

    /// Execute a SQL statement.
    #[wasm_bindgen]
    pub fn execute(&self, sql: &str) -> Result<JsValue, JsValue> {
        let mut state = self.state.borrow_mut();

        match state.db.execute(sql) {
            Ok(result) => {
                let js_result = match result {
                    ExecuteResult::Created(_) => serde_wasm_bindgen::to_value(&"created").unwrap(),
                    ExecuteResult::PolicyCreated { table, action } => serde_wasm_bindgen::to_value(
                        &format!("policy_created:{}:{}", table, action),
                    )
                    .unwrap(),
                    ExecuteResult::Inserted { row_id, .. } => {
                        // Queue for immediate push via SyncClient
                        state.sync_client.queue_push(row_id);
                        // Trigger push in background
                        let state_clone = Rc::clone(&self.state);
                        WasmRuntime.spawn(async move {
                            push_pending_objects(&state_clone).await;
                        });
                        serde_wasm_bindgen::to_value(&format!("inserted:{}", row_id)).unwrap()
                    }
                    ExecuteResult::Updated(count) => {
                        // TODO: Track updated objects for sync
                        serde_wasm_bindgen::to_value(&format!("updated:{}", count)).unwrap()
                    }
                    ExecuteResult::Deleted(count) => {
                        // TODO: Track deleted objects for sync
                        serde_wasm_bindgen::to_value(&format!("deleted:{}", count)).unwrap()
                    }
                };
                Ok(js_result)
            }
            Err(e) => Err(JsValue::from_str(&format!("{:?}", e))),
        }
    }

    /// Execute a SELECT query and return results as binary Uint8Array.
    #[wasm_bindgen(js_name = selectBinary)]
    pub fn select_binary(&self, sql: &str) -> Result<Uint8Array, JsValue> {
        let state = self.state.borrow();
        match state.db.query(sql) {
            Ok(rows) => {
                let binary = encode_rows(&rows);
                Ok(Uint8Array::from(binary.as_slice()))
            }
            Err(e) => Err(JsValue::from_str(&format!("{:?}", e))),
        }
    }

    /// Initialize the database schema from a SQL string.
    #[wasm_bindgen(js_name = initSchema)]
    pub fn init_schema(&self, schema: &str) -> Result<(), JsValue> {
        let state = self.state.borrow();
        for stmt in schema
            .split(';')
            .map(|s| s.trim())
            .filter(|s| !s.is_empty())
        {
            state
                .db
                .execute(stmt)
                .map_err(|e| JsValue::from_str(&format!("{:?}", e)))?;
        }
        Ok(())
    }

    /// Update a specific row's column with a string value.
    #[wasm_bindgen(js_name = updateRow)]
    pub fn update_row(
        &self,
        table: &str,
        row_id: &str,
        column: &str,
        value: &str,
    ) -> Result<bool, JsValue> {
        let id: groove::ObjectId = row_id
            .parse()
            .map_err(|e| JsValue::from_str(&format!("invalid row_id: {:?}", e)))?;
        let value_owned = value.to_string();
        let column_owned = column.to_string();
        let mut state = self.state.borrow_mut();
        let result = state
            .db
            .update_with(table, id, |builder| {
                builder
                    .set_string_by_name(&column_owned, &value_owned)
                    .build()
            })
            .map_err(|e| JsValue::from_str(&format!("{:?}", e)))?;

        // Queue for immediate push if update succeeded
        if result {
            state.sync_client.queue_push(id);
            let state_clone = Rc::clone(&self.state);
            WasmRuntime.spawn(async move {
                push_pending_objects(&state_clone).await;
            });
        }
        Ok(result)
    }

    /// Update a specific row's column with an i64 value.
    #[wasm_bindgen(js_name = updateRowI64)]
    pub fn update_row_i64(
        &self,
        table: &str,
        row_id: &str,
        column: &str,
        value: i64,
    ) -> Result<bool, JsValue> {
        let id: groove::ObjectId = row_id
            .parse()
            .map_err(|e| JsValue::from_str(&format!("invalid row_id: {:?}", e)))?;
        let column_owned = column.to_string();
        let mut state = self.state.borrow_mut();
        let result = state
            .db
            .update_with(table, id, |builder| {
                builder.set_i64_by_name(&column_owned, value).build()
            })
            .map_err(|e| JsValue::from_str(&format!("{:?}", e)))?;

        // Queue for immediate push if update succeeded
        if result {
            state.sync_client.queue_push(id);
            let state_clone = Rc::clone(&self.state);
            WasmRuntime.spawn(async move {
                push_pending_objects(&state_clone).await;
            });
        }
        Ok(result)
    }

    /// List all tables in the database.
    #[wasm_bindgen(js_name = listTables)]
    pub fn list_tables(&self) -> JsValue {
        let state = self.state.borrow();
        let tables = state.db.list_tables();
        serde_wasm_bindgen::to_value(&tables).unwrap_or(JsValue::NULL)
    }

    /// Create an incremental query subscription (delta-based).
    #[wasm_bindgen(js_name = subscribeDelta)]
    pub fn subscribe_delta(
        &self,
        sql: &str,
        callback: js_sys::Function,
    ) -> Result<SyncedQueryHandle, JsValue> {
        use groove::sql::{encode_delta, query_graph::DeltaBatch};

        let state = self.state.borrow();
        let query = state
            .db
            .incremental_query(sql)
            .map_err(|e| JsValue::from_str(&format!("{:?}", e)))?;

        let rust_callback = Box::new(move |delta_batch: &DeltaBatch| {
            let js_deltas = Array::new();

            for delta in delta_batch.iter() {
                let binary = encode_delta(delta);
                let js_array = Uint8Array::from(binary.as_slice());
                js_deltas.push(&js_array);
            }

            let _ = callback.call1(&JsValue::NULL, &js_deltas);
        });

        let listener_id = query.subscribe(rust_callback);

        Ok(SyncedQueryHandle {
            _query: query,
            listener_id,
        })
    }

    /// Create an incremental query subscription that returns full row objects.
    ///
    /// The callback receives an Array of objects with column names as keys.
    /// This maintains an internal row map and provides the complete result set on each change.
    #[wasm_bindgen(js_name = subscribeRows)]
    pub fn subscribe_rows(
        &self,
        sql: &str,
        callback: js_sys::Function,
    ) -> Result<SyncedQueryHandle, JsValue> {
        use groove::sql::query_graph::DeltaBatch;
        use std::collections::HashMap;

        let state = self.state.borrow();
        let query = state
            .db
            .incremental_query(sql)
            .map_err(|e| JsValue::from_str(&format!("{:?}", e)))?;

        // Shared state for accumulating rows
        let rows_map: Rc<RefCell<HashMap<ObjectId, JsValue>>> =
            Rc::new(RefCell::new(HashMap::new()));

        let rust_callback = {
            let rows_map = Rc::clone(&rows_map);

            Box::new(move |delta_batch: &DeltaBatch| {
                let mut map = rows_map.borrow_mut();

                for delta in delta_batch.iter() {
                    let row_id = delta.row_id();

                    if let Some(row) = delta.new_row() {
                        // Added or Updated - convert row to JS object
                        let js_obj = js_sys::Object::new();

                        // Always add the ID
                        let _ = js_sys::Reflect::set(
                            &js_obj,
                            &JsValue::from_str("id"),
                            &JsValue::from_str(&row_id.to_string()),
                        );

                        // Add all columns from the row descriptor
                        for (i, col) in row.descriptor.columns.iter().enumerate() {
                            let value = if let Some(val) = row.get(i) {
                                match val {
                                    groove::sql::row_buffer::RowValue::String(s) => {
                                        JsValue::from_str(s)
                                    }
                                    groove::sql::row_buffer::RowValue::I64(n) => {
                                        JsValue::from_str(&n.to_string())
                                    }
                                    groove::sql::row_buffer::RowValue::F64(n) => {
                                        JsValue::from_f64(n)
                                    }
                                    groove::sql::row_buffer::RowValue::Bool(b) => {
                                        JsValue::from_bool(b)
                                    }
                                    groove::sql::row_buffer::RowValue::Ref(id) => {
                                        JsValue::from_str(&id.to_string())
                                    }
                                    groove::sql::row_buffer::RowValue::I32(n) => {
                                        JsValue::from_f64(n as f64)
                                    }
                                    groove::sql::row_buffer::RowValue::U32(n) => {
                                        JsValue::from_f64(n as f64)
                                    }
                                    groove::sql::row_buffer::RowValue::Bytes(_) => {
                                        JsValue::from_str("[bytes]")
                                    }
                                    groove::sql::row_buffer::RowValue::BlobArray(_) => {
                                        JsValue::from_str("[blob_array]")
                                    }
                                    groove::sql::row_buffer::RowValue::Blob(_) => {
                                        JsValue::from_str("[blob]")
                                    }
                                    groove::sql::row_buffer::RowValue::Array(_) => {
                                        JsValue::from_str("[array]")
                                    }
                                    groove::sql::row_buffer::RowValue::Null => JsValue::NULL,
                                }
                            } else {
                                JsValue::NULL
                            };

                            let _ = js_sys::Reflect::set(
                                &js_obj,
                                &JsValue::from_str(&col.name),
                                &value,
                            );
                        }

                        map.insert(row_id, js_obj.into());
                    } else {
                        // Removed
                        map.remove(&row_id);
                    }
                }

                // Convert map to JS array
                let js_rows = Array::new();
                for row in map.values() {
                    js_rows.push(row);
                }

                let _ = callback.call1(&JsValue::NULL, &js_rows);
            })
        };

        let listener_id = query.subscribe(rust_callback);

        Ok(SyncedQueryHandle {
            _query: query,
            listener_id,
        })
    }
}

// ============================================================================
// Query Handle
// ============================================================================

/// Handle to an incremental query subscription.
#[wasm_bindgen]
pub struct SyncedQueryHandle {
    _query: groove::sql::IncrementalQuery,
    listener_id: Option<groove::ListenerId>,
}

#[wasm_bindgen]
impl SyncedQueryHandle {
    /// Unsubscribe from updates.
    #[wasm_bindgen]
    pub fn unsubscribe(&mut self) {
        if let Some(id) = self.listener_id.take() {
            self._query.unsubscribe(id);
        }
    }

    /// Get a text diagram of the query graph.
    #[wasm_bindgen]
    pub fn diagram(&self) -> String {
        self._query.diagram()
    }

    /// Free resources (no-op, but required by WasmQueryHandleLike interface).
    #[wasm_bindgen]
    pub fn free(&mut self) {
        // Resources are freed when the handle is dropped
    }
}

// ============================================================================
// Sync Helpers
// ============================================================================

/// Main sync event loop with automatic reconnection.
///
/// This mirrors the native `upstream_event_loop` in SyncedNode, providing
/// symmetric behavior between WASM and native implementations.
async fn sync_event_loop(state: &Rc<RefCell<SyncedState>>, query: &str) {
    let mut reconnect_attempt: u32 = 0;

    loop {
        // Check if graceful shutdown was requested
        if state.borrow().sync_client.is_stopping() {
            web_sys::console::log_1(&JsValue::from_str("Sync loop stopped (shutdown requested)"));
            return;
        }

        // Set appropriate state for this connection attempt
        {
            let mut state_ref = state.borrow_mut();
            if reconnect_attempt == 0 {
                state_ref.sync_client.set_connection_state(ConnectionState::Connecting);
            } else {
                state_ref.sync_client.set_connection_state(ConnectionState::Reconnecting { attempt: reconnect_attempt });
            }
        }

        // Clone the env before the async operation (can't hold borrow across await)
        let env = state.borrow().sync_client.env().clone();
        let request = SubscribeRequest {
            query: query.to_string(),
            options: SubscriptionOptions::default(),
        };

        match env.subscribe(request).await {
            Ok(mut stream) => {
                // Connection successful - reset reconnect counter
                reconnect_attempt = 0;
                state.borrow_mut().sync_client.set_connection_state(ConnectionState::Connected);

                web_sys::console::log_1(&JsValue::from_str("Connected to sync server"));

                // Process events until stream ends or shutdown is requested
                while let Some(result) = stream.next().await {
                    // Check for shutdown request
                    if state.borrow().sync_client.is_stopping() {
                        web_sys::console::log_1(&JsValue::from_str(
                            "Sync loop stopped (shutdown requested)",
                        ));
                        return;
                    }

                    match result {
                        Ok(event) => {
                            handle_sse_event(state, &event);
                        }
                        Err(e) => {
                            state.borrow().sync_client.report_error(&format!("Stream error: {}", e.message));
                            break;
                        }
                    }
                }

                // Check again before reconnecting
                if state.borrow().sync_client.is_stopping() {
                    return;
                }

                web_sys::console::log_1(&JsValue::from_str("Sync stream ended, will reconnect"));
            }
            Err(e) => {
                web_sys::console::log_1(&JsValue::from_str(&format!(
                    "Connection failed: {}",
                    e.message
                )));
                state.borrow().sync_client.report_error(&format!("Connection failed: {}", e.message));
            }
        }

        // Check before sleeping
        if state.borrow().sync_client.is_stopping() {
            return;
        }

        // Calculate delay with jitter and wait before reconnecting
        let runtime = WasmRuntime;
        let config = ReconnectConfig::default();
        let delay = calculate_reconnect_delay_with_jitter(reconnect_attempt, &config, runtime.random_f64());
        web_sys::console::log_1(&JsValue::from_str(&format!(
            "Reconnecting in {}ms (attempt {})",
            delay,
            reconnect_attempt + 1
        )));

        runtime.sleep(delay).await;
        reconnect_attempt = reconnect_attempt.saturating_add(1);
    }
}

/// Handle an SSE event from the server.
fn handle_sse_event(state: &Rc<RefCell<SyncedState>>, event: &SseEvent) {
    use groove::sync::handle_commits_event;

    match event {
        SseEvent::Commits {
            object_id,
            commits,
            frontier,
            object_meta,
        } => {
            web_sys::console::log_1(&JsValue::from_str(&format!(
                "Received {} commits for object {}",
                commits.len(),
                object_id
            )));

            // Use shared event handler for core logic (applies commits, registers rows)
            let result = {
                let state_ref = state.borrow();
                handle_commits_event(
                    &state_ref.db,
                    *object_id,
                    commits.clone(),
                    frontier.clone(),
                    object_meta.clone(),
                )
            };

            // Update server known state with the frontier from this event
            // This prevents us from sending these commits back to the server
            state
                .borrow_mut()
                .sync_client
                .update_server_known_state(*object_id, frontier.clone());

            match result {
                Ok(Some(table)) => {
                    web_sys::console::log_1(&JsValue::from_str(&format!(
                        "Row {} registered with table {}",
                        object_id, table
                    )));
                }
                Ok(None) => {
                    // No table metadata - handled by shared handler
                }
                Err(e) => {
                    web_sys::console::log_1(&JsValue::from_str(&format!(
                        "Failed to handle commits: {}",
                        e
                    )));
                }
            }
        }
        SseEvent::Excluded { object_id } => {
            web_sys::console::log_1(&JsValue::from_str(&format!(
                "Object {} excluded from subscription",
                object_id
            )));
            // Remove from server known state
            state.borrow_mut().sync_client.server_known_state.remove(object_id);
        }
        SseEvent::Truncate { object_id, .. } => {
            web_sys::console::log_1(&JsValue::from_str(&format!(
                "Truncate event for object {}",
                object_id
            )));
        }
        SseEvent::Request { object_id, .. } => {
            web_sys::console::log_1(&JsValue::from_str(&format!(
                "Request event for object {}",
                object_id
            )));
        }
        SseEvent::Error { code, message } => {
            state
                .borrow()
                .sync_client
                .report_error(&format!("SSE error {}: {}", code, message));
        }
    }
}

/// Push pending objects to the server.
async fn push_pending_objects(state: &Rc<RefCell<SyncedState>>) {
    // Get pending objects and client env
    let (pending, env) = {
        let mut state_ref = state.borrow_mut();
        let pending = state_ref.sync_client.drain_pending_push();
        let env = state_ref.sync_client.env().clone();
        (pending, env)
    };

    if pending.is_empty() {
        return;
    }

    for object_id in pending {
        // Use sync_client.create_push_request() which handles all the commit logic
        let request = {
            let state_ref = state.borrow();
            state_ref.sync_client.create_push_request(object_id, "main")
        };

        let request = match request {
            Some(req) => req,
            None => {
                web_sys::console::log_1(&JsValue::from_str(&format!(
                    "No commits to push for object {}",
                    object_id
                )));
                continue;
            }
        };

        web_sys::console::log_1(&JsValue::from_str(&format!(
            "Pushing {} commits for object {}",
            request.commits.len(),
            object_id
        )));

        match env.push(request).await {
            Ok(response) => {
                // Use sync_client.handle_push_response() to update server known state
                state.borrow_mut().sync_client.handle_push_response(&response);

                if response.accepted {
                    web_sys::console::log_1(&JsValue::from_str(&format!(
                        "Push accepted for object {}",
                        object_id
                    )));
                } else {
                    web_sys::console::log_1(&JsValue::from_str(&format!(
                        "Push rejected for object {}",
                        object_id
                    )));
                }
            }
            Err(e) => {
                state
                    .borrow()
                    .sync_client
                    .report_error(&format!("Push failed: {}", e.message));
            }
        }
    }
}

