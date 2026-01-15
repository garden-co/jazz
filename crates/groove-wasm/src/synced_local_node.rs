//! WasmSyncedLocalNode - A synced database for browser environments.
//!
//! This module provides a WASM-friendly wrapper that integrates:
//! - `Database` for SQL operations
//! - Sync capabilities for real-time collaboration
//!
//! Usage from JavaScript:
//! ```javascript
//! const node = await WasmSyncedLocalNode.create("http://localhost:8080", "auth-token");
//! node.execute("CREATE TABLE users (id, name TEXT, email TEXT)");
//! node.execute("INSERT INTO users (name, email) VALUES ('Alice', 'alice@example.com')");
//! // Changes are automatically synced to the server and other clients
//! ```

use std::cell::RefCell;
use std::collections::{HashMap, HashSet};
use std::rc::Rc;
use std::sync::Arc;

use futures::StreamExt;
use js_sys::{Array, Function, Promise, Uint8Array};
use wasm_bindgen::prelude::*;
use wasm_bindgen_futures::future_to_promise;

use groove::{CommitId, ObjectId};
use groove::sql::{Database, ExecuteResult, encode_rows};
use groove::sync::{
    ClientEnv, ClientEnvConfig, PushRequest, ReconnectConfig, Runtime, SseEvent, SubscribeRequest,
    SubscriptionOptions, calculate_reconnect_delay_with_jitter, commits_to_send,
};

use crate::indexeddb::IndexedDbEnvironment;
use crate::runtime::WasmRuntime;
use crate::sync::WasmClientEnv;

// ============================================================================
// Connection State
// ============================================================================

/// Connection state for the synced node.
#[wasm_bindgen]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SyncState {
    Disconnected,
    Connecting,
    Connected,
    Reconnecting,
}

// ============================================================================
// Internal State (shared between sync task and main thread)
// ============================================================================

/// Shared state for sync operations.
struct SyncedState {
    /// The underlying SQL database
    db: Database,
    /// Server URL for sync
    server_url: String,
    /// Auth token for sync
    auth_token: String,
    /// Current sync state
    sync_state: SyncState,
    /// Objects we're tracking for sync (have pending writes)
    pending_objects: HashSet<ObjectId>,
    /// Server's known frontier per object (to avoid re-sending commits)
    server_known_state: HashMap<ObjectId, Vec<CommitId>>,
    /// Callback for state changes
    on_state_change: Option<Function>,
    /// Callback for sync errors
    on_error: Option<Function>,
    /// Flag to stop reconnection loop
    should_disconnect: bool,
}

impl SyncedState {
    fn set_sync_state(&mut self, new_state: SyncState) {
        self.sync_state = new_state;
        if let Some(ref cb) = self.on_state_change {
            let state_str = match new_state {
                SyncState::Disconnected => "disconnected",
                SyncState::Connecting => "connecting",
                SyncState::Connected => "connected",
                SyncState::Reconnecting => "reconnecting",
            };
            let _ = cb.call1(&JsValue::NULL, &JsValue::from_str(state_str));
        }
    }

    fn report_error(&self, message: &str) {
        if let Some(ref cb) = self.on_error {
            let _ = cb.call1(&JsValue::NULL, &JsValue::from_str(message));
        }
    }

    fn client_env(&self) -> WasmClientEnv {
        WasmClientEnv::new(ClientEnvConfig::new(
            self.server_url.clone(),
            self.auth_token.clone(),
        ))
    }
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

        Self {
            state: Rc::new(RefCell::new(SyncedState {
                db,
                server_url,
                auth_token,
                sync_state: SyncState::Disconnected,
                pending_objects: HashSet::new(),
                server_known_state: HashMap::new(),
                on_state_change: None,
                on_error: None,
                should_disconnect: false,
            })),
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

            Ok(JsValue::from(WasmSyncedLocalNode {
                state: Rc::new(RefCell::new(SyncedState {
                    db,
                    server_url,
                    auth_token,
                    sync_state: SyncState::Disconnected,
                    pending_objects: HashSet::new(),
                    server_known_state: HashMap::new(),
                    on_state_change: None,
                    on_error: None,
                    should_disconnect: false,
                })),
            }))
        })
    }

    /// Set callback for sync state changes.
    ///
    /// Callback receives: (state: string)
    #[wasm_bindgen(js_name = setOnStateChange)]
    pub fn set_on_state_change(&self, callback: Function) {
        self.state.borrow_mut().on_state_change = Some(callback);
    }

    /// Set callback for sync errors.
    ///
    /// Callback receives: (message: string)
    #[wasm_bindgen(js_name = setOnError)]
    pub fn set_on_error(&self, callback: Function) {
        self.state.borrow_mut().on_error = Some(callback);
    }

    /// Get current sync state.
    #[wasm_bindgen(getter, js_name = syncState)]
    pub fn sync_state(&self) -> SyncState {
        self.state.borrow().sync_state
    }

    /// Disconnect from the sync server and stop reconnection attempts.
    #[wasm_bindgen]
    pub fn disconnect(&self) {
        let mut state = self.state.borrow_mut();
        state.should_disconnect = true;
        state.set_sync_state(SyncState::Disconnected);
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
            {
                let mut state_ref = state.borrow_mut();
                state_ref.should_disconnect = false;
                state_ref.set_sync_state(SyncState::Connecting);
            }

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
                        // Track only the row object for sync (table_rows is private per-node)
                        state.pending_objects.insert(row_id);
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

        // Track for sync if update succeeded
        if result {
            state.pending_objects.insert(id);
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

        // Track for sync if update succeeded
        if result {
            state.pending_objects.insert(id);
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
        // Check if we should stop reconnecting
        if state.borrow().should_disconnect {
            web_sys::console::log_1(&JsValue::from_str("Sync loop stopped (disconnect requested)"));
            return;
        }

        // Set appropriate state for this connection attempt
        if reconnect_attempt == 0 {
            state.borrow_mut().set_sync_state(SyncState::Connecting);
        } else {
            state.borrow_mut().set_sync_state(SyncState::Reconnecting);
        }

        let env = state.borrow().client_env();
        let request = SubscribeRequest {
            query: query.to_string(),
            options: SubscriptionOptions::default(),
        };

        match env.subscribe(request).await {
            Ok(mut stream) => {
                // Connection successful - reset reconnect counter
                reconnect_attempt = 0;
                state.borrow_mut().set_sync_state(SyncState::Connected);

                web_sys::console::log_1(&JsValue::from_str("Connected to sync server"));

                // Process events until stream ends or disconnect is requested
                while let Some(result) = stream.next().await {
                    // Check for disconnect request
                    if state.borrow().should_disconnect {
                        web_sys::console::log_1(&JsValue::from_str(
                            "Sync loop stopped (disconnect requested)",
                        ));
                        return;
                    }

                    match result {
                        Ok(event) => {
                            handle_sse_event(state, &event);
                        }
                        Err(e) => {
                            state.borrow().report_error(&format!("Stream error: {}", e.message));
                            break;
                        }
                    }
                }

                // Check again before reconnecting
                if state.borrow().should_disconnect {
                    return;
                }

                web_sys::console::log_1(&JsValue::from_str("Sync stream ended, will reconnect"));
            }
            Err(e) => {
                web_sys::console::log_1(&JsValue::from_str(&format!(
                    "Connection failed: {}",
                    e.message
                )));
                state.borrow().report_error(&format!("Connection failed: {}", e.message));
            }
        }

        // Check before sleeping
        if state.borrow().should_disconnect {
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

            // Use shared event handler for core logic
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
                .server_known_state
                .insert(*object_id, frontier.clone());

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
                .report_error(&format!("SSE error {}: {}", code, message));
        }
    }
}

/// Push pending objects to the server.
async fn push_pending_objects(state: &Rc<RefCell<SyncedState>>) {
    // Get pending objects and client env
    let (pending, env) = {
        let mut state_ref = state.borrow_mut();
        let pending: Vec<ObjectId> = state_ref.pending_objects.drain().collect();
        let env = state_ref.client_env();
        (pending, env)
    };

    if pending.is_empty() {
        return;
    }

    for object_id in pending {
        // Get commits to push from the LocalNode
        let (commits, server_frontier_was_empty) = {
            let state_ref = state.borrow();
            let node = state_ref.db.node();

            // Get server's known frontier for this object
            let server_frontier = state_ref
                .server_known_state
                .get(&object_id)
                .cloned()
                .unwrap_or_default();
            let server_frontier_was_empty = server_frontier.is_empty();

            // Get object and branch to find commits to send
            let obj = match node.get_object(object_id) {
                Some(o) => o,
                None => {
                    web_sys::console::log_1(&JsValue::from_str(&format!(
                        "Object {} not found",
                        object_id
                    )));
                    continue;
                }
            };

            let obj_read = obj.read().unwrap();
            let branch_ref = match obj_read.branch_ref("main") {
                Some(b) => b,
                None => {
                    web_sys::console::log_1(&JsValue::from_str(&format!(
                        "Branch 'main' not found for object {}",
                        object_id
                    )));
                    continue;
                }
            };

            let branch_read = branch_ref.read().unwrap();
            let local_frontier = branch_read.frontier().to_vec();

            let commits = commits_to_send(&branch_read, &local_frontier, &server_frontier);
            (commits, server_frontier_was_empty)
        };

        if commits.is_empty() {
            web_sys::console::log_1(&JsValue::from_str(&format!(
                "No commits to push for object {}",
                object_id
            )));
            continue;
        }

        web_sys::console::log_1(&JsValue::from_str(&format!(
            "Pushing {} commits for object {}",
            commits.len(),
            object_id
        )));

        // Get object metadata only for first push (when server has no known state)
        let object_meta = if server_frontier_was_empty {
            let state_ref = state.borrow();
            let node = state_ref.db.node();
            if let Some(obj) = node.get_object(object_id) {
                if let Ok(obj_read) = obj.read() {
                    obj_read.meta.clone()
                } else {
                    None
                }
            } else {
                None
            }
        } else {
            None
        };

        let request = PushRequest {
            object_id,
            commits,
            object_meta,
        };

        match env.push(request).await {
            Ok(response) => {
                if response.accepted {
                    // Update server known state with the new frontier
                    state
                        .borrow_mut()
                        .server_known_state
                        .insert(object_id, response.frontier.clone());
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
                    .report_error(&format!("Push failed: {}", e.message));
            }
        }
    }
}

