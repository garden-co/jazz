//! WasmSyncedLocalNode - A synced database for browser environments.
//!
//! This module provides a WASM-friendly wrapper around `SyncedNode` that integrates:
//! - SQL database operations via `Database`
//! - Real-time sync to upstream servers
//! - Write batching/debouncing
//!
//! Usage from JavaScript:
//! ```javascript
//! const node = new WasmSyncedLocalNode("http://localhost:8080", "auth-token");
//! node.execute("CREATE TABLE users (id, name TEXT, email TEXT)");
//! node.execute("INSERT INTO users (name, email) VALUES ('Alice', 'alice@example.com')");
//! node.connect("SELECT * FROM users");  // Start syncing
//! // Changes are automatically synced to the server and other clients
//! ```

use std::cell::RefCell;
use std::rc::Rc;

use futures::FutureExt;
use js_sys::{Array, Function, Promise, Uint8Array};
use wasm_bindgen::prelude::*;
use wasm_bindgen_futures::future_to_promise;

use groove::ObjectId;
use groove::sql::{Database, ExecuteResult, encode_rows};
use groove::sync::{
    ClientEnvConfig, Shared, SubscriptionOptions, SyncedNode, UpstreamId, UpstreamState,
    run_upstream_event_loop,
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
}

impl From<&UpstreamState> for SyncState {
    fn from(state: &UpstreamState) -> Self {
        match state {
            UpstreamState::Disconnected => SyncState::Disconnected,
            UpstreamState::Connecting => SyncState::Connecting,
            UpstreamState::Connected => SyncState::Connected,
            UpstreamState::Reconnecting { .. } => SyncState::Reconnecting,
        }
    }
}

// ============================================================================
// WasmSyncedLocalNode
// ============================================================================

/// A synced local database for browser environments.
///
/// This is a thin wrapper that combines:
/// - `Database` for SQL operations
/// - `SyncedNode<WasmRuntime, WasmClientEnv>` for sync
///
/// Both share the same underlying `LocalNode` for storage.
#[wasm_bindgen]
pub struct WasmSyncedLocalNode {
    /// The database for SQL operations
    db: Rc<groove::sql::DatabaseState>,
    /// The underlying SyncedNode for sync (shared for async access)
    synced_node: Shared<SyncedNode<WasmRuntime, WasmClientEnv>>,
    /// The upstream server ID (we connect to exactly one server)
    upstream_id: UpstreamId,
    /// Callback for sync state changes
    on_state_change: Option<js_sys::Function>,
    /// Callback for sync errors
    on_error: Option<js_sys::Function>,
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

        // Get shared LocalNode for both Database and SyncedNode
        let db_state = db.into_state();
        let node_arc = db_state.node_arc();
        let synced_node = SyncedNode::new(node_arc, WasmRuntime);

        // Add the upstream server
        let env = WasmClientEnv::new(ClientEnvConfig::new(&server_url, &auth_token));
        let upstream_id = synced_node.add_upstream(env);

        // Wire up the sync callback to notify the database of incoming objects
        let db_state_for_callback = Rc::clone(&db_state);
        synced_node.set_on_objects_received(Rc::new(move |object_id, _commits, object_meta| {
            // Extract table name from metadata
            let table_name = object_meta.and_then(|meta| meta.get("table").cloned());

            if let Some(table) = table_name {
                let db = Database::from_state(Rc::clone(&db_state_for_callback));
                // Register the row in the database's mapping and notify queries
                // Note: register_synced_row_by_table takes (row_id, table_name)
                let _ = db.register_synced_row_by_table(object_id, &table);
            }
        }));

        Self {
            db: db_state,
            synced_node: Shared::new(synced_node),
            upstream_id,
            on_state_change: None,
            on_error: None,
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
            let env = Rc::new(env);

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

            // Get shared LocalNode for both Database and SyncedNode
            let db_state = db.into_state();
            let node_arc = db_state.node_arc();
            let synced_node = SyncedNode::new(node_arc, WasmRuntime);

            // Add the upstream server
            let client_env = WasmClientEnv::new(ClientEnvConfig::new(&server_url, &auth_token));
            let upstream_id = synced_node.add_upstream(client_env);

            Ok(JsValue::from(WasmSyncedLocalNode {
                db: db_state,
                synced_node: Shared::new(synced_node),
                upstream_id,
                on_state_change: None,
                on_error: None,
            }))
        })
    }

    // ========================================================================
    // Callbacks
    // ========================================================================

    /// Set callback for sync state changes.
    ///
    /// Callback signature: (state: string) => void
    /// States: "Disconnected", "Connecting", "Connected", "Reconnecting"
    #[wasm_bindgen(js_name = setOnStateChange)]
    pub fn set_on_state_change(&mut self, callback: js_sys::Function) {
        self.on_state_change = Some(callback);
    }

    /// Set callback for sync errors.
    ///
    /// Callback signature: (message: string) => void
    #[wasm_bindgen(js_name = setOnError)]
    pub fn set_on_error(&mut self, callback: js_sys::Function) {
        self.on_error = Some(callback);
    }

    /// Get current sync state.
    #[wasm_bindgen(getter, js_name = syncState)]
    pub fn sync_state(&self) -> SyncState {
        self.synced_node
            .read()
            .upstream_state(self.upstream_id)
            .map(|s| SyncState::from(&s))
            .unwrap_or(SyncState::Disconnected)
    }

    /// Connect to the sync server and start receiving updates.
    ///
    /// This subscribes to the given query and starts an SSE stream
    /// to receive real-time updates from other clients. The connection
    /// automatically reconnects with exponential backoff on disconnection.
    ///
    /// The promise resolves once the initial connection is established.
    /// The event loop continues running in the background.
    #[wasm_bindgen]
    pub fn connect(&self, query: String) -> Promise {
        let synced_node = self.synced_node.clone();
        let synced_node_for_flush = self.synced_node.clone();
        let upstream_id = self.upstream_id;

        // Create a oneshot channel to signal when connected
        let (connected_tx, connected_rx) = futures::channel::oneshot::channel();

        // Spawn the event loop in the background - it runs forever
        wasm_bindgen_futures::spawn_local(async move {
            web_sys::console::log_1(&"[WASM connect] Starting event loop".into());
            let queries = vec![(query, SubscriptionOptions::default())];

            web_sys::console::log_1(&"[WASM connect] About to call run_upstream_event_loop".into());
            // Run the event loop, passing the signal for when we connect
            let event_loop_future =
                run_upstream_event_loop(synced_node, upstream_id, queries, Some(connected_tx));
            web_sys::console::log_1(&"[WASM connect] Created future, about to await".into());
            event_loop_future.await;
            web_sys::console::log_1(&"[WASM connect] Event loop ended".into());
        });

        // Spawn a separate flush loop for pending writes
        wasm_bindgen_futures::spawn_local(async move {
            use groove::sync::push_object_standalone;

            loop {
                // Wait a bit before checking for pending writes
                gloo_timers::future::TimeoutFuture::new(100).await;

                // Get pending objects to push (with 0/0 debounce = push immediately)
                let pending: Vec<groove::ObjectId> = {
                    let node = synced_node_for_flush.read();
                    node.pending_pushes(0, 0).unwrap_or_default()
                };

                // Push each pending object
                for object_id in pending {
                    // Remove from buffer before pushing
                    {
                        let node = synced_node_for_flush.read();
                        node.remove_pending_push(&object_id);
                    }

                    // Push the object
                    let _ = push_object_standalone(
                        synced_node_for_flush.clone(),
                        upstream_id,
                        object_id,
                        "main",
                    )
                    .await;
                }
            }
        });

        // Return a promise that resolves when the event loop signals connected
        future_to_promise(async move {
            // Race between connection signal and timeout
            let timeout = gloo_timers::future::TimeoutFuture::new(10_000);

            futures::select! {
                result = connected_rx.fuse() => {
                    match result {
                        Ok(()) => Ok(JsValue::TRUE),
                        Err(_) => Err(JsValue::from_str("Event loop stopped before connecting")),
                    }
                }
                _ = timeout.fuse() => {
                    Err(JsValue::from_str("Connection timeout"))
                }
            }
        })
    }

    // ========================================================================
    // SQL Operations
    // ========================================================================

    /// Execute a SQL statement.
    ///
    /// For INSERT/UPDATE operations, this automatically pushes the affected
    /// objects to upstream servers.
    #[wasm_bindgen]
    pub fn execute(&self, sql: &str) -> Result<JsValue, JsValue> {
        web_sys::console::log_1(
            &format!("[WASM execute] 1. Starting - sql len: {}", sql.len()).into(),
        );
        let db = Database::from_state(Rc::clone(&self.db));
        web_sys::console::log_1(&"[WASM execute] 2. Got database".into());

        web_sys::console::log_1(&"[WASM execute] 3. About to call db.execute()".into());
        let execute_result = db.execute(sql);
        web_sys::console::log_1(
            &format!(
                "[WASM execute] 4. db.execute() returned: {:?}",
                execute_result.is_ok()
            )
            .into(),
        );
        match execute_result {
            Ok(result) => {
                let js_result = match &result {
                    ExecuteResult::Created(_) => serde_wasm_bindgen::to_value(&"created").unwrap(),
                    ExecuteResult::PolicyCreated { table, action } => serde_wasm_bindgen::to_value(
                        &format!("policy_created:{}:{}", table, action),
                    )
                    .unwrap(),
                    ExecuteResult::Inserted { row_id, .. } => {
                        web_sys::console::log_1(
                            &format!("[WASM execute] Inserted row: {}", row_id).into(),
                        );
                        // Queue the object for pushing via the event loop
                        // We don't spawn a separate push task to avoid RefCell contention
                        // with the event loop task. Instead, we queue the push and let the
                        // event loop handle it.
                        {
                            web_sys::console::log_1(&"[WASM execute] Getting synced_node".into());
                            let node = self.synced_node.read();
                            web_sys::console::log_1(&"[WASM execute] Calling queue_push".into());
                            node.queue_push(*row_id);
                            web_sys::console::log_1(&"[WASM execute] queue_push done".into());
                        }
                        web_sys::console::log_1(&"[WASM execute] Returning result".into());
                        serde_wasm_bindgen::to_value(&format!("inserted:{}", row_id)).unwrap()
                    }
                    ExecuteResult::Updated(count) => {
                        // TODO: Push updated rows (need row_ids from result)
                        serde_wasm_bindgen::to_value(&format!("updated:{}", count)).unwrap()
                    }
                    ExecuteResult::Deleted(count) => {
                        serde_wasm_bindgen::to_value(&format!("deleted:{}", count)).unwrap()
                    }
                };
                web_sys::console::log_1(&"[WASM execute] 5. Returning OK".into());
                Ok(js_result)
            }
            Err(e) => {
                web_sys::console::log_1(&format!("[WASM execute] 5. Error: {:?}", e).into());
                Err(JsValue::from_str(&format!("{:?}", e)))
            }
        }
    }

    /// Provision or find the viewer (current user) and set it on the database.
    ///
    /// This method looks up or creates a user row in the "users" table with the
    /// given external_id and name, then sets that user as the @viewer for
    /// subsequent INSERT/UPDATE statements.
    ///
    /// Note: This does NOT push the user row to the server immediately. The user
    /// row will be synced when connect() is called and the sync subscription
    /// includes the users table.
    ///
    /// @param external_id - The external user ID (e.g., from JWT sub claim)
    /// @param name - The user's display name
    /// @returns The user's ObjectId as a string
    #[wasm_bindgen(js_name = provisionViewer)]
    pub fn provision_viewer(&self, external_id: &str, name: &str) -> Result<String, JsValue> {
        let db = Database::from_state(Rc::clone(&self.db));

        // Try to find existing user by external_id
        let query = format!("SELECT * FROM users WHERE external_id = '{}'", external_id);

        match db.query(&query) {
            Ok(rows) if !rows.is_empty() => {
                // User exists - set as viewer and return ID
                let (user_id, _) = &rows[0];
                db.set_viewer(Some(*user_id));
                Ok(user_id.to_string())
            }
            Ok(_) => {
                // User doesn't exist - create one
                let insert = format!(
                    "INSERT INTO users (name, external_id) VALUES ('{}', '{}')",
                    name, external_id
                );
                match db.execute(&insert) {
                    Ok(ExecuteResult::Inserted { row_id, .. }) => {
                        // Set the new user as viewer
                        db.set_viewer(Some(row_id));
                        Ok(row_id.to_string())
                    }
                    Ok(_) => Err(JsValue::from_str("unexpected result from INSERT")),
                    Err(e) => Err(JsValue::from_str(&format!("INSERT failed: {:?}", e))),
                }
            }
            Err(e) => {
                // Query failed - might be because table doesn't exist yet
                // Try to create the user anyway
                let insert = format!(
                    "INSERT INTO users (name, external_id) VALUES ('{}', '{}')",
                    name, external_id
                );
                match db.execute(&insert) {
                    Ok(ExecuteResult::Inserted { row_id, .. }) => {
                        db.set_viewer(Some(row_id));
                        Ok(row_id.to_string())
                    }
                    Ok(_) => Err(JsValue::from_str("unexpected result from INSERT")),
                    Err(e2) => Err(JsValue::from_str(&format!(
                        "query failed: {:?}, insert failed: {:?}",
                        e, e2
                    ))),
                }
            }
        }
    }

    /// Execute a SELECT query and return results as binary Uint8Array.
    #[wasm_bindgen(js_name = selectBinary)]
    pub fn select_binary(&self, sql: &str) -> Result<Uint8Array, JsValue> {
        let db = Database::from_state(Rc::clone(&self.db));

        match db.query(sql) {
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
        let db = Database::from_state(Rc::clone(&self.db));

        for stmt in schema
            .split(';')
            .map(|s| s.trim())
            .filter(|s| !s.is_empty())
        {
            db.execute(stmt)
                .map_err(|e| JsValue::from_str(&format!("{:?}", e)))?;
        }
        Ok(())
    }

    /// List all tables in the database.
    #[wasm_bindgen(js_name = listTables)]
    pub fn list_tables(&self) -> JsValue {
        let db = Database::from_state(Rc::clone(&self.db));
        let tables = db.list_tables();
        serde_wasm_bindgen::to_value(&tables).unwrap_or(JsValue::NULL)
    }

    /// Create an incremental query subscription (delta-based).
    #[wasm_bindgen(js_name = subscribeDelta)]
    pub fn subscribe_delta(
        &self,
        sql: &str,
        callback: Function,
    ) -> Result<SyncedQueryHandle, JsValue> {
        use groove::sql::{encode_delta, query_graph::DeltaBatch};

        let db = Database::from_state(Rc::clone(&self.db));

        let query = db
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
        callback: Function,
    ) -> Result<SyncedQueryHandle, JsValue> {
        use groove::sql::query_graph::DeltaBatch;
        use std::collections::HashMap;

        let db = Database::from_state(Rc::clone(&self.db));

        // Use incremental_query_as if a viewer is set, otherwise use basic incremental_query
        let query = if let Some(viewer_id) = db.viewer() {
            db.incremental_query_as(sql, viewer_id)
                .map_err(|e| JsValue::from_str(&format!("{:?}", e)))?
        } else {
            db.incremental_query(sql)
                .map_err(|e| JsValue::from_str(&format!("{:?}", e)))?
        };

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
