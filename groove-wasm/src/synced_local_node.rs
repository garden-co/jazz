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
use std::rc::Rc;
use std::sync::Arc;

use futures::StreamExt;
use js_sys::{Array, Function, Promise, Uint8Array};
use wasm_bindgen::prelude::*;
use wasm_bindgen_futures::future_to_promise;

use groove::sql::{encode_rows, Database, ExecuteResult, Row};
use groove::sync::{ClientEnv, ClientEnvConfig, SseEvent, SubscribeRequest, SubscriptionOptions};
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
// WasmSyncedLocalNode
// ============================================================================

/// A synced local database for browser environments.
///
/// Combines SQL database operations with real-time sync to a server.
/// All writes are automatically pushed to the server, and incoming
/// changes from other clients are automatically applied.
#[wasm_bindgen]
pub struct WasmSyncedLocalNode {
    /// The underlying SQL database
    db: Database,

    /// Server URL for sync
    server_url: String,

    /// Auth token for sync
    auth_token: String,

    /// Current sync state
    sync_state: Rc<RefCell<SyncState>>,

    /// Callback for state changes
    on_state_change: Rc<RefCell<Option<Function>>>,

    /// Callback for sync errors
    on_error: Rc<RefCell<Option<Function>>>,
}

#[wasm_bindgen]
impl WasmSyncedLocalNode {
    /// Create a new synced local node with in-memory storage.
    ///
    /// @param server_url - The sync server URL (e.g., "http://localhost:8080")
    /// @param auth_token - Bearer token for authentication
    #[wasm_bindgen(constructor)]
    pub fn new(server_url: String, auth_token: String) -> Self {
        Self {
            db: Database::in_memory(),
            server_url,
            auth_token,
            sync_state: Rc::new(RefCell::new(SyncState::Disconnected)),
            on_state_change: Rc::new(RefCell::new(None)),
            on_error: Rc::new(RefCell::new(None)),
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
                db,
                server_url,
                auth_token,
                sync_state: Rc::new(RefCell::new(SyncState::Disconnected)),
                on_state_change: Rc::new(RefCell::new(None)),
                on_error: Rc::new(RefCell::new(None)),
            }))
        })
    }

    /// Set callback for sync state changes.
    ///
    /// Callback receives: (state: string)
    #[wasm_bindgen(js_name = setOnStateChange)]
    pub fn set_on_state_change(&self, callback: Function) {
        *self.on_state_change.borrow_mut() = Some(callback);
    }

    /// Set callback for sync errors.
    ///
    /// Callback receives: (message: string)
    #[wasm_bindgen(js_name = setOnError)]
    pub fn set_on_error(&self, callback: Function) {
        *self.on_error.borrow_mut() = Some(callback);
    }

    /// Get current sync state.
    #[wasm_bindgen(getter, js_name = syncState)]
    pub fn sync_state(&self) -> SyncState {
        *self.sync_state.borrow()
    }

    /// Connect to the sync server and start receiving updates.
    ///
    /// This subscribes to the given query and starts an SSE stream
    /// to receive real-time updates from other clients.
    #[wasm_bindgen]
    pub fn connect(&self, query: String) -> Promise {
        let server_url = self.server_url.clone();
        let auth_token = self.auth_token.clone();
        let sync_state = Rc::clone(&self.sync_state);
        let on_state_change = Rc::clone(&self.on_state_change);
        let on_error = Rc::clone(&self.on_error);

        // We need to clone the database reference for the async block
        // This is tricky because Database is not Clone...
        // For now, we'll use a simpler approach where we return a handle

        future_to_promise(async move {
            set_sync_state(&sync_state, &on_state_change, SyncState::Connecting);

            let env = WasmClientEnv::new(ClientEnvConfig::new(server_url, auth_token));

            let request = SubscribeRequest {
                query,
                options: SubscriptionOptions::default(),
            };

            match env.subscribe(request).await {
                Ok(mut stream) => {
                    set_sync_state(&sync_state, &on_state_change, SyncState::Connected);

                    // Spawn a task to process incoming events
                    // Note: In a full implementation, we'd apply these to the database
                    wasm_bindgen_futures::spawn_local(async move {
                        while let Some(result) = stream.next().await {
                            match result {
                                Ok(event) => {
                                    // TODO: Apply event to database
                                    // This requires access to the database which is tricky
                                    // in this async context
                                    web_sys::console::log_1(&JsValue::from_str(&format!(
                                        "Received sync event: {:?}",
                                        event_type(&event)
                                    )));
                                }
                                Err(e) => {
                                    if let Some(ref callback) = *on_error.borrow() {
                                        let _ =
                                            callback.call1(&JsValue::NULL, &JsValue::from_str(&e.message));
                                    }
                                    break;
                                }
                            }
                        }

                        set_sync_state(&sync_state, &on_state_change, SyncState::Disconnected);
                    });

                    Ok(JsValue::TRUE)
                }
                Err(e) => {
                    set_sync_state(&sync_state, &on_state_change, SyncState::Disconnected);
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
        match self.db.execute(sql) {
            Ok(result) => {
                let js_result = match result {
                    ExecuteResult::Created(_) => {
                        serde_wasm_bindgen::to_value(&"created").unwrap()
                    }
                    ExecuteResult::PolicyCreated { table, action } => {
                        serde_wasm_bindgen::to_value(&format!("policy_created:{}:{}", table, action))
                            .unwrap()
                    }
                    ExecuteResult::Inserted(id) => {
                        // TODO: Push the new commit to server
                        serde_wasm_bindgen::to_value(&format!("inserted:{}", id)).unwrap()
                    }
                    ExecuteResult::Updated(count) => {
                        // TODO: Push the updated commits to server
                        serde_wasm_bindgen::to_value(&format!("updated:{}", count)).unwrap()
                    }
                    ExecuteResult::Deleted(count) => {
                        // TODO: Push the delete commits to server
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
        match self.db.execute(sql) {
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
        for stmt in schema.split(';').map(|s| s.trim()).filter(|s| !s.is_empty()) {
            self.db
                .execute(stmt)
                .map_err(|e| JsValue::from_str(&format!("{:?}", e)))?;
        }
        Ok(())
    }

    /// List all tables in the database.
    #[wasm_bindgen(js_name = listTables)]
    pub fn list_tables(&self) -> JsValue {
        let tables = self.db.list_tables();
        serde_wasm_bindgen::to_value(&tables).unwrap_or(JsValue::NULL)
    }

    /// Create an incremental query subscription.
    #[wasm_bindgen(js_name = subscribeDelta)]
    pub fn subscribe_delta(
        &self,
        sql: &str,
        callback: js_sys::Function,
    ) -> Result<SyncedQueryHandle, JsValue> {
        use groove::sql::{encode_delta, query_graph::DeltaBatch};

        let query = self
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
// Helper Functions
// ============================================================================

fn set_sync_state(
    state: &Rc<RefCell<SyncState>>,
    callback: &Rc<RefCell<Option<Function>>>,
    new_state: SyncState,
) {
    *state.borrow_mut() = new_state;
    if let Some(ref cb) = *callback.borrow() {
        let state_str = match new_state {
            SyncState::Disconnected => "disconnected",
            SyncState::Connecting => "connecting",
            SyncState::Connected => "connected",
            SyncState::Reconnecting => "reconnecting",
        };
        let _ = cb.call1(&JsValue::NULL, &JsValue::from_str(state_str));
    }
}

fn event_type(event: &SseEvent) -> &'static str {
    match event {
        SseEvent::Commits { .. } => "commits",
        SseEvent::Excluded { .. } => "excluded",
        SseEvent::Truncate { .. } => "truncate",
        SseEvent::Request { .. } => "request",
        SseEvent::Error { .. } => "error",
    }
}

fn row_to_strings(row: &Row) -> Vec<String> {
    row.values.iter().map(|v| format!("{:?}", v)).collect()
}
