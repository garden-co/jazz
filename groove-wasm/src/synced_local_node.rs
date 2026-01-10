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
use std::collections::HashSet;
use std::rc::Rc;
use std::sync::Arc;

use futures::StreamExt;
use js_sys::{Array, Function, Promise, Uint8Array};
use wasm_bindgen::prelude::*;
use wasm_bindgen_futures::future_to_promise;

use groove::sql::{encode_rows, Database, ExecuteResult, Row};
use groove::sync::{
    commits_to_send, ClientEnv, ClientEnvConfig, PushRequest, SseEvent, SubscribeRequest,
    SubscriptionOptions,
};
use groove::ObjectId;

use crate::indexeddb::IndexedDbEnvironment;
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
    /// Callback for state changes
    on_state_change: Option<Function>,
    /// Callback for sync errors
    on_error: Option<Function>,
    /// Callback for data changes (called when sync applies changes)
    on_data_change: Option<Function>,
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

    fn notify_data_change(&self) {
        if let Some(ref cb) = self.on_data_change {
            let _ = cb.call0(&JsValue::NULL);
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
                on_state_change: None,
                on_error: None,
                on_data_change: None,
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
                    on_state_change: None,
                    on_error: None,
                    on_data_change: None,
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

    /// Set callback for data changes (called when sync applies remote changes).
    ///
    /// Callback receives: no arguments
    #[wasm_bindgen(js_name = setOnDataChange)]
    pub fn set_on_data_change(&self, callback: Function) {
        self.state.borrow_mut().on_data_change = Some(callback);
    }

    /// Get current sync state.
    #[wasm_bindgen(getter, js_name = syncState)]
    pub fn sync_state(&self) -> SyncState {
        self.state.borrow().sync_state
    }

    /// Connect to the sync server and start receiving updates.
    ///
    /// This subscribes to the given query and starts an SSE stream
    /// to receive real-time updates from other clients.
    #[wasm_bindgen]
    pub fn connect(&self, query: String) -> Promise {
        let state = Rc::clone(&self.state);

        future_to_promise(async move {
            state.borrow_mut().set_sync_state(SyncState::Connecting);

            let env = state.borrow().client_env();

            let request = SubscribeRequest {
                query,
                options: SubscriptionOptions::default(),
            };

            match env.subscribe(request).await {
                Ok(mut stream) => {
                    state.borrow_mut().set_sync_state(SyncState::Connected);

                    // Spawn a task to process incoming events
                    let state_clone = Rc::clone(&state);
                    wasm_bindgen_futures::spawn_local(async move {
                        while let Some(result) = stream.next().await {
                            match result {
                                Ok(event) => {
                                    handle_sse_event(&state_clone, &event);
                                }
                                Err(e) => {
                                    state_clone.borrow().report_error(&e.message);
                                    break;
                                }
                            }
                        }

                        state_clone.borrow_mut().set_sync_state(SyncState::Disconnected);
                    });

                    Ok(JsValue::TRUE)
                }
                Err(e) => {
                    state.borrow_mut().set_sync_state(SyncState::Disconnected);
                    Err(JsValue::from_str(&e.message))
                }
            }
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
                    ExecuteResult::Created(_) => {
                        serde_wasm_bindgen::to_value(&"created").unwrap()
                    }
                    ExecuteResult::PolicyCreated { table, action } => {
                        serde_wasm_bindgen::to_value(&format!("policy_created:{}:{}", table, action))
                            .unwrap()
                    }
                    ExecuteResult::Inserted { row_id, .. } => {
                        // Track only the row object for sync (table_rows is private per-node)
                        state.pending_objects.insert(row_id);
                        // Trigger push in background
                        let state_clone = Rc::clone(&self.state);
                        wasm_bindgen_futures::spawn_local(async move {
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
                    ExecuteResult::Selected(rows) => {
                        let row_data: Vec<Vec<String>> =
                            rows.iter().map(|row| row_to_strings(row)).collect();
                        serde_wasm_bindgen::to_value(&row_data).unwrap()
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
        match state.db.execute(sql) {
            Ok(ExecuteResult::Selected(rows)) => {
                let binary = encode_rows(&rows);
                Ok(Uint8Array::from(binary.as_slice()))
            }
            Ok(_) => Err(JsValue::from_str("expected SELECT query")),
            Err(e) => Err(JsValue::from_str(&format!("{:?}", e))),
        }
    }

    /// Initialize the database schema from a SQL string.
    #[wasm_bindgen(js_name = initSchema)]
    pub fn init_schema(&self, schema: &str) -> Result<(), JsValue> {
        let state = self.state.borrow();
        for stmt in schema.split(';').map(|s| s.trim()).filter(|s| !s.is_empty()) {
            state
                .db
                .execute(stmt)
                .map_err(|e| JsValue::from_str(&format!("{:?}", e)))?;
        }
        Ok(())
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

        let listener_id = query.subscribe_delta(rust_callback);

        Ok(SyncedQueryHandle {
            _query: query,
            listener_id,
        })
    }

    /// Subscribe to query results with full row data on each change.
    ///
    /// Callback receives: Array of row objects with {id, ...columns}
    /// This is simpler than subscribeDelta - you get the complete result set each time.
    #[wasm_bindgen(js_name = subscribeRows)]
    pub fn subscribe_rows(
        &self,
        sql: &str,
        callback: js_sys::Function,
    ) -> Result<SyncedQueryHandle, JsValue> {
        use groove::sql::{Row, Value};

        let state = self.state.borrow();

        // Get the schema for this query to know column names
        let query = state
            .db
            .incremental_query(sql)
            .map_err(|e| JsValue::from_str(&format!("{:?}", e)))?;

        // Parse the SQL to get table name for schema lookup
        let table_name = extract_table_name(sql);
        let schema = table_name.and_then(|t| state.db.get_table(&t));
        let column_names: Vec<String> = schema
            .map(|s| s.columns.iter().map(|c| c.name.clone()).collect())
            .unwrap_or_default();

        let rust_callback = Box::new(move |rows: Vec<Row>| {
            let js_rows = Array::new();

            for row in rows {
                let obj = js_sys::Object::new();

                // Add row ID
                let _ = js_sys::Reflect::set(
                    &obj,
                    &JsValue::from_str("id"),
                    &JsValue::from_str(&row.id.to_string()),
                );

                // Add column values
                for (i, value) in row.values.iter().enumerate() {
                    let col_name = column_names.get(i)
                        .map(|s: &String| s.as_str())
                        .unwrap_or("col");
                    let js_value = value_to_js_inner(value);
                    let _ = js_sys::Reflect::set(&obj, &JsValue::from_str(col_name), &js_value);
                }

                js_rows.push(&obj);
            }

            let _ = callback.call1(&JsValue::NULL, &js_rows);
        });

        let listener_id = query.subscribe(rust_callback);

        Ok(SyncedQueryHandle {
            _query: query,
            listener_id,
        })
    }
}

/// Extract table name from a simple SELECT query.
fn extract_table_name(sql: &str) -> Option<String> {
    let sql_upper = sql.to_uppercase();
    let from_idx = sql_upper.find("FROM")?;
    let after_from = &sql[from_idx + 4..].trim_start();
    let end = after_from.find(|c: char| c.is_whitespace() || c == ';').unwrap_or(after_from.len());
    Some(after_from[..end].to_string())
}

/// Convert a groove Value to JsValue.
fn value_to_js_inner(value: &groove::sql::Value) -> JsValue {
    use groove::sql::Value;
    match value {
        Value::Bool(b) => JsValue::from_bool(*b),
        Value::I32(i) => JsValue::from_f64(*i as f64),
        Value::U32(u) => JsValue::from_f64(*u as f64),
        Value::I64(i) => JsValue::from_f64(*i as f64),
        Value::F64(f) => JsValue::from_f64(*f),
        Value::String(s) => JsValue::from_str(s),
        Value::Bytes(b) => JsValue::from_str(&format!("bytes:{}", b.len())),
        Value::Ref(oid) => JsValue::from_str(&oid.to_string()),
        Value::Row(row) => JsValue::from_str(&format!("row:{}", row.id)),
        Value::Array(arr) => {
            let js_arr = Array::new();
            for v in arr {
                js_arr.push(&value_to_js_inner(v));
            }
            js_arr.into()
        }
        Value::NullableSome(inner) => value_to_js_inner(inner),
        Value::NullableNone => JsValue::NULL,
        Value::Blob(_) => JsValue::from_str("[blob]"),
        Value::BlobArray(arr) => JsValue::from_str(&format!("[blob_array:{}]", arr.len())),
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
}

// ============================================================================
// Sync Helpers
// ============================================================================

/// Handle an SSE event from the server.
fn handle_sse_event(state: &Rc<RefCell<SyncedState>>, event: &SseEvent) {
    match event {
        SseEvent::Commits {
            object_id,
            commits,
            frontier: _,
            object_meta,
        } => {
            web_sys::console::log_1(&JsValue::from_str(&format!(
                "Received {} commits for object {}",
                commits.len(),
                object_id
            )));

            // Apply commits to the LocalNode underlying the Database
            let state_ref = state.borrow();
            let node = state_ref.db.node();
            node.apply_commits(*object_id, "main", commits.clone());

            // If we received object metadata with a descriptor, register the row
            if let Some(meta) = object_meta {
                if let Some(descriptor_str) = meta.get("descriptor") {
                    web_sys::console::log_1(&JsValue::from_str(&format!(
                        "Row {} belongs to descriptor {}",
                        object_id, descriptor_str
                    )));

                    // Register the synced row with the database
                    if let Err(e) = state_ref.db.register_synced_row(*object_id, descriptor_str) {
                        web_sys::console::log_1(&JsValue::from_str(&format!(
                            "Failed to register synced row: {:?}",
                            e
                        )));
                    }
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
            state.borrow().report_error(&format!("SSE error {}: {}", code, message));
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
        let commits = {
            let state_ref = state.borrow();
            let node = state_ref.db.node();

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

            // For new objects, server frontier is empty
            let server_frontier: Vec<groove::CommitId> = vec![];

            commits_to_send(&branch_read, &local_frontier, &server_frontier)
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

        // Get object metadata for first push
        let object_meta = {
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
        };

        let request = PushRequest {
            object_id,
            commits,
            object_meta,
        };

        match env.push(request).await {
            Ok(response) => {
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
                state.borrow().report_error(&format!("Push failed: {}", e.message));
            }
        }
    }
}

fn row_to_strings(row: &Row) -> Vec<String> {
    row.values.iter().map(|v| format!("{:?}", v)).collect()
}
