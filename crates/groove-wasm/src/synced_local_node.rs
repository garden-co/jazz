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
use std::sync::Arc;

use js_sys::{Array, Function, Promise, Uint8Array};
use wasm_bindgen::prelude::*;
use wasm_bindgen_futures::future_to_promise;

use groove::ObjectId;
use groove::sql::{Database, ExecuteResult, encode_rows};
use groove::sync::{
    ClientEnvConfig, Shared, SubscriptionOptions, SyncedNode, UpstreamId, UpstreamState,
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
/// This is a thin wrapper around `SyncedNode<WasmRuntime, WasmClientEnv>` that:
/// - Provides JS-friendly bindings for SQL operations
/// - Manages a single upstream server connection
/// - Exposes sync state for UI updates
#[wasm_bindgen]
pub struct WasmSyncedLocalNode {
    /// The underlying SyncedNode (shared for async access)
    node: Shared<SyncedNode<WasmRuntime, WasmClientEnv>>,
    /// The upstream server ID (we connect to exactly one server)
    upstream_id: UpstreamId,
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

        let db_state = db.into_state();
        let node = SyncedNode::new(db_state, WasmRuntime);

        // Add the upstream server
        let env = WasmClientEnv::new(ClientEnvConfig::new(&server_url, &auth_token));
        let upstream_id = node.add_upstream(env);

        Self {
            node: Shared::new(node),
            upstream_id,
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

            let db_state = db.into_state();
            let node = SyncedNode::new(db_state, WasmRuntime);

            // Add the upstream server
            let client_env = WasmClientEnv::new(ClientEnvConfig::new(&server_url, &auth_token));
            let upstream_id = node.add_upstream(client_env);

            Ok(JsValue::from(WasmSyncedLocalNode {
                node: Shared::new(node),
                upstream_id,
            }))
        })
    }

    /// Get current sync state.
    #[wasm_bindgen(getter, js_name = syncState)]
    pub fn sync_state(&self) -> SyncState {
        self.node
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
    #[wasm_bindgen]
    pub fn connect(&self, query: String) -> Promise {
        let node = self.node.clone();
        let upstream_id = self.upstream_id;

        future_to_promise(async move {
            // Create queries list with default options
            let queries = vec![(query, SubscriptionOptions::default())];

            // Run the upstream event loop (handles reconnection automatically)
            node.read().upstream_event_loop(upstream_id, queries).await;

            Ok(JsValue::TRUE)
        })
    }

    // ========================================================================
    // SQL Operations
    // ========================================================================

    /// Execute a SQL statement.
    #[wasm_bindgen]
    pub fn execute(&self, sql: &str) -> Result<JsValue, JsValue> {
        let node = self.node.read();
        let db = Database::from_state(node.db_arc());

        match db.execute(sql) {
            Ok(result) => {
                let js_result = match result {
                    ExecuteResult::Created(_) => serde_wasm_bindgen::to_value(&"created").unwrap(),
                    ExecuteResult::PolicyCreated { table, action } => serde_wasm_bindgen::to_value(
                        &format!("policy_created:{}:{}", table, action),
                    )
                    .unwrap(),
                    ExecuteResult::Inserted { row_id, .. } => {
                        // Queue for push via write buffer
                        node.queue_for_push(row_id, "main");
                        serde_wasm_bindgen::to_value(&format!("inserted:{}", row_id)).unwrap()
                    }
                    ExecuteResult::Updated(count) => {
                        serde_wasm_bindgen::to_value(&format!("updated:{}", count)).unwrap()
                    }
                    ExecuteResult::Deleted(count) => {
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
        let node = self.node.read();
        let db = Database::from_state(node.db_arc());

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
        let node = self.node.read();
        let db = Database::from_state(node.db_arc());

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

        let node = self.node.read();
        let db = Database::from_state(node.db_arc());

        let result = db
            .update_with(table, id, |builder| {
                builder
                    .set_string_by_name(&column_owned, &value_owned)
                    .build()
            })
            .map_err(|e| JsValue::from_str(&format!("{:?}", e)))?;

        // Queue for push if update succeeded
        if result {
            node.queue_for_push(id, "main");
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

        let node = self.node.read();
        let db = Database::from_state(node.db_arc());

        let result = db
            .update_with(table, id, |builder| {
                builder.set_i64_by_name(&column_owned, value).build()
            })
            .map_err(|e| JsValue::from_str(&format!("{:?}", e)))?;

        // Queue for push if update succeeded
        if result {
            node.queue_for_push(id, "main");
        }
        Ok(result)
    }

    /// List all tables in the database.
    #[wasm_bindgen(js_name = listTables)]
    pub fn list_tables(&self) -> JsValue {
        let node = self.node.read();
        let db = Database::from_state(node.db_arc());
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

        let node = self.node.read();
        let db = Database::from_state(node.db_arc());

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

        let node = self.node.read();
        let db = Database::from_state(node.db_arc());

        let query = db
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
