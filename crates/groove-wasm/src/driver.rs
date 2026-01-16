//! WASM Driver for the runtime-less sync engine.
//!
//! This module provides a WASM-friendly driver that:
//! - Owns a `SyncEngine` from groove
//! - Handles EventSource callbacks for SSE events
//! - Makes fetch() calls for push requests
//! - Calls JavaScript callbacks for notifications
//!
//! The driver is purely synchronous from the engine's perspective - all async I/O
//! is handled by JavaScript callbacks that feed events back into the engine.

use std::cell::RefCell;
use std::collections::HashMap;
use std::rc::Rc;

use js_sys::{Array, Function, Uint8Array};
use wasm_bindgen::JsCast;
use wasm_bindgen::prelude::*;
use wasm_bindgen_futures::JsFuture;
use web_sys::{EventSource, MessageEvent, Request, RequestInit, Response};

// ObjectId is used via groove::sync::*
use groove::Environment;
use groove::sql::{Database, DatabaseState};
use groove::sync::{
    ConnectionEvent, ConnectionEventKind, ConnectionState, Decode, Encode, Inboxes, Notification,
    OutboundRequest, Outboxes, PushResponse, PushResponseEvent, SseEvent, SseInboxEvent,
    StorageRequest, StreamAction, SubscribeRequestEvent, SubscriptionOptions, SyncEngine,
    TickEvent, UpstreamId,
};

// ============================================================================
// Connection State (JS-compatible enum)
// ============================================================================

/// Connection state for the synced node (JS-compatible).
#[wasm_bindgen]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WasmSyncState {
    Disconnected,
    Connecting,
    Connected,
    Reconnecting,
}

impl From<&ConnectionState> for WasmSyncState {
    fn from(state: &ConnectionState) -> Self {
        match state {
            ConnectionState::Disconnected => WasmSyncState::Disconnected,
            ConnectionState::Connecting => WasmSyncState::Connecting,
            ConnectionState::Connected => WasmSyncState::Connected,
            ConnectionState::Reconnecting { .. } => WasmSyncState::Reconnecting,
        }
    }
}

// ============================================================================
// Active Stream Tracking
// ============================================================================

/// Tracks an active EventSource connection.
struct ActiveStream {
    event_source: EventSource,
    // We need to store closures to prevent them from being dropped
    _on_message: Closure<dyn FnMut(MessageEvent)>,
    _on_error: Closure<dyn FnMut(web_sys::Event)>,
    _on_open: Closure<dyn FnMut(web_sys::Event)>,
}

// ============================================================================
// WasmSyncDriver
// ============================================================================

/// A WASM driver for the runtime-less sync engine.
///
/// This driver handles all I/O and feeds events into the sync engine.
#[wasm_bindgen]
pub struct WasmSyncDriver {
    /// The sync engine (owns LocalNode and sync state)
    engine: Rc<RefCell<SyncEngine>>,

    /// The database for SQL operations (shares LocalNode with engine)
    db: Rc<DatabaseState>,

    /// Server URL for sync
    server_url: String,

    /// Auth token for requests
    auth_token: String,

    /// Active EventSource connections
    active_streams: Rc<RefCell<HashMap<(u64, u32), ActiveStream>>>,

    /// Callback for sync state changes
    on_state_change: Option<Function>,

    /// Callback for errors
    on_error: Option<Function>,

    /// The upstream server ID (we connect to exactly one server)
    upstream_id: UpstreamId,

    /// Interval ID for tick timer
    tick_interval: Option<i32>,
}

#[wasm_bindgen]
impl WasmSyncDriver {
    /// Create a new WASM sync driver.
    ///
    /// @param server_url - The sync server URL (e.g., "http://localhost:8080")
    /// @param auth_token - Bearer token for authentication
    #[wasm_bindgen(constructor)]
    pub fn new(server_url: String, auth_token: String) -> Self {
        let db = Database::in_memory();
        let db_state = db.into_state();

        // Create engine with the database's LocalNode (shared)
        let node = db_state.node_arc();
        let engine = SyncEngine::with_local_node(node);

        // Wrap in Rc<RefCell> for shared access
        let engine = Rc::new(RefCell::new(engine));

        // Add upstream server
        let upstream_id = engine.borrow_mut().add_upstream();

        Self {
            engine,
            db: db_state,
            server_url,
            auth_token,
            active_streams: Rc::new(RefCell::new(HashMap::new())),
            on_state_change: None,
            on_error: None,
            upstream_id,
            tick_interval: None,
        }
    }

    /// Set callback for sync state changes.
    #[wasm_bindgen(js_name = setOnStateChange)]
    pub fn set_on_state_change(&mut self, callback: Function) {
        self.on_state_change = Some(callback);
    }

    /// Set callback for errors.
    #[wasm_bindgen(js_name = setOnError)]
    pub fn set_on_error(&mut self, callback: Function) {
        self.on_error = Some(callback);
    }

    /// Get current sync state.
    #[wasm_bindgen(js_name = getSyncState)]
    pub fn get_sync_state(&self) -> WasmSyncState {
        let engine = self.engine.borrow();
        if let Some(upstream) = engine.upstream(self.upstream_id) {
            WasmSyncState::from(&upstream.connection)
        } else {
            WasmSyncState::Disconnected
        }
    }

    /// Connect to the sync server with a query.
    ///
    /// @param query - SQL query to subscribe to
    #[wasm_bindgen]
    pub fn connect(&mut self, query: String) {
        // Start tick timer if not already running
        if self.tick_interval.is_none() {
            self.start_tick_timer();
        }

        // Create subscribe request
        let inboxes = Inboxes {
            subscribe_requests: vec![SubscribeRequestEvent {
                upstream_id: self.upstream_id,
                query,
                options: SubscriptionOptions::default(),
            }],
            ..Default::default()
        };

        // Run a pass
        let outboxes = self.engine.borrow_mut().pass(inboxes);

        // Handle outboxes
        self.handle_outboxes(outboxes);
    }

    /// Execute SQL statement.
    #[wasm_bindgen]
    pub fn execute(&self, sql: &str) -> Result<JsValue, JsValue> {
        use groove::sql::ExecuteResult;

        let db = Database::from_state(Rc::clone(&self.db));
        match db.execute(sql) {
            Ok(result) => {
                // Run a pass to pick up any changed objects for sync
                let outboxes = self.engine.borrow_mut().pass(Inboxes::default());
                self.handle_outboxes(outboxes);

                let obj = js_sys::Object::new();

                match &result {
                    ExecuteResult::Inserted { row_id, .. } => {
                        js_sys::Reflect::set(&obj, &"rowsAffected".into(), &JsValue::from(1u32))?;
                        js_sys::Reflect::set(
                            &obj,
                            &"lastRowId".into(),
                            &JsValue::from(row_id.to_string()),
                        )?;
                    }
                    ExecuteResult::Updated(count) => {
                        js_sys::Reflect::set(
                            &obj,
                            &"rowsAffected".into(),
                            &JsValue::from(*count as u32),
                        )?;
                    }
                    ExecuteResult::Deleted(count) => {
                        js_sys::Reflect::set(
                            &obj,
                            &"rowsAffected".into(),
                            &JsValue::from(*count as u32),
                        )?;
                    }
                    ExecuteResult::Created(_) | ExecuteResult::PolicyCreated { .. } => {
                        js_sys::Reflect::set(&obj, &"rowsAffected".into(), &JsValue::from(0u32))?;
                    }
                }

                Ok(obj.into())
            }
            Err(e) => Err(JsValue::from_str(&format!("SQL error: {:?}", e))),
        }
    }

    /// Query with string results.
    #[wasm_bindgen]
    pub fn query(&self, sql: &str) -> Result<JsValue, JsValue> {
        use groove::sql::RowValue;

        let db = Database::from_state(Rc::clone(&self.db));
        match db.query(sql) {
            Ok(rows) => {
                let array = Array::new();
                for (_id, row) in rows {
                    let row_array = Array::new();
                    for i in 0..row.descriptor.columns.len() {
                        let js_val = match row.get(i) {
                            Some(RowValue::String(s)) => JsValue::from_str(s),
                            Some(RowValue::I32(n)) => JsValue::from(n),
                            Some(RowValue::U32(n)) => JsValue::from(n),
                            Some(RowValue::I64(n)) => JsValue::from(n as f64),
                            Some(RowValue::F64(n)) => JsValue::from(n),
                            Some(RowValue::Bool(b)) => JsValue::from(b),
                            Some(RowValue::Ref(id)) => JsValue::from_str(&id.to_string()),
                            Some(RowValue::Bytes(b)) => JsValue::from(Uint8Array::from(b)),
                            Some(RowValue::Null) | None => JsValue::NULL,
                            // Complex types - convert to string representation
                            Some(RowValue::Blob(_)) => JsValue::from_str("[blob]"),
                            Some(RowValue::BlobArray(_)) => JsValue::from_str("[blob array]"),
                            Some(RowValue::Array(_)) => JsValue::from_str("[array]"),
                        };
                        row_array.push(&js_val);
                    }
                    array.push(&row_array);
                }
                Ok(array.into())
            }
            Err(e) => Err(JsValue::from_str(&format!("Query error: {:?}", e))),
        }
    }

    /// Manually trigger a tick (for testing).
    #[wasm_bindgen]
    pub fn tick(&mut self) {
        let now_ms = js_sys::Date::now() as u64;
        let inboxes = Inboxes {
            tick: Some(TickEvent { now_ms }),
            ..Default::default()
        };

        let outboxes = self.engine.borrow_mut().pass(inboxes);
        self.handle_outboxes(outboxes);
    }

    /// Disconnect from the sync server.
    #[wasm_bindgen]
    pub fn disconnect(&mut self) {
        // Stop tick timer
        if let Some(id) = self.tick_interval.take() {
            let window = web_sys::window().unwrap();
            window.clear_interval_with_handle(id);
        }

        // Close all active streams
        let streams = self.active_streams.borrow();
        for (_, stream) in streams.iter() {
            stream.event_source.close();
        }
        drop(streams);
        self.active_streams.borrow_mut().clear();
    }
}

// ============================================================================
// Internal Implementation
// ============================================================================

impl WasmSyncDriver {
    /// Start the tick timer (100ms interval).
    fn start_tick_timer(&mut self) {
        let engine = Rc::clone(&self.engine);
        let active_streams = Rc::clone(&self.active_streams);
        let on_state_change = self.on_state_change.clone();
        let on_error = self.on_error.clone();
        let server_url = self.server_url.clone();
        let auth_token = self.auth_token.clone();

        let closure = Closure::wrap(Box::new(move || {
            let now_ms = js_sys::Date::now() as u64;
            let inboxes = Inboxes {
                tick: Some(TickEvent { now_ms }),
                ..Default::default()
            };

            let outboxes = engine.borrow_mut().pass(inboxes);
            let env = engine
                .borrow()
                .local_node
                .env()
                .expect("env required")
                .clone();

            // Handle outboxes inline (can't call self methods from closure)
            handle_outboxes_impl(
                &outboxes,
                &engine,
                &active_streams,
                &server_url,
                &auth_token,
                on_state_change.as_ref(),
                on_error.as_ref(),
                env,
            );
        }) as Box<dyn FnMut()>);

        let window = web_sys::window().unwrap();
        let id = window
            .set_interval_with_callback_and_timeout_and_arguments_0(
                closure.as_ref().unchecked_ref(),
                100, // 100ms tick interval
            )
            .unwrap();

        closure.forget(); // Prevent closure from being dropped
        self.tick_interval = Some(id);
    }

    /// Handle outboxes from a pass.
    fn handle_outboxes(&self, outboxes: Outboxes) {
        let env = self
            .engine
            .borrow()
            .local_node
            .env()
            .expect("env required")
            .clone();
        handle_outboxes_impl(
            &outboxes,
            &self.engine,
            &self.active_streams,
            &self.server_url,
            &self.auth_token,
            self.on_state_change.as_ref(),
            self.on_error.as_ref(),
            env,
        );
    }
}

/// Handle outboxes (standalone function for use in closures).
fn handle_outboxes_impl(
    outboxes: &Outboxes,
    engine: &Rc<RefCell<SyncEngine>>,
    active_streams: &Rc<RefCell<HashMap<(u64, u32), ActiveStream>>>,
    server_url: &str,
    auth_token: &str,
    on_state_change: Option<&Function>,
    _on_error: Option<&Function>,
    env: Rc<dyn Environment>,
) {
    // Handle stream actions
    for action in &outboxes.stream_actions {
        match action {
            StreamAction::Open {
                upstream_id,
                subscription_id,
                query,
                options: _,
            } => {
                open_sse_stream(
                    engine,
                    active_streams,
                    server_url,
                    auth_token,
                    *upstream_id,
                    *subscription_id,
                    query,
                );
            }
            StreamAction::Close {
                upstream_id,
                subscription_id,
            } => {
                let key = (upstream_id.0, *subscription_id);
                if let Some(stream) = active_streams.borrow_mut().remove(&key) {
                    stream.event_source.close();
                }
            }
        }
    }

    // Handle outbound requests
    for request in &outboxes.requests {
        match request {
            OutboundRequest::Push {
                upstream_id,
                request,
            } => {
                send_push_request(
                    engine,
                    server_url,
                    auth_token,
                    *upstream_id,
                    request.clone(),
                );
            }
            OutboundRequest::Reconcile { .. } => {
                // TODO: Implement reconcile
            }
            OutboundRequest::Unsubscribe { .. } => {
                // TODO: Implement unsubscribe
            }
        }
    }

    // Handle storage requests
    for storage_req in &outboxes.storage {
        execute_storage_request(env.clone(), Rc::clone(engine), storage_req.clone());
    }

    // Handle notifications
    for notification in &outboxes.notifications {
        match notification {
            Notification::ConnectionStateChanged { state, .. } => {
                if let Some(callback) = on_state_change {
                    let state_str = match state {
                        ConnectionState::Disconnected => "Disconnected",
                        ConnectionState::Connecting => "Connecting",
                        ConnectionState::Connected => "Connected",
                        ConnectionState::Reconnecting { .. } => "Reconnecting",
                    };
                    let _ = callback.call1(&JsValue::NULL, &JsValue::from_str(state_str));
                }
            }
            Notification::ObjectsReceived { .. } => {
                // Database observes this internally via LocalNode callbacks
            }
        }
    }
}

/// Execute a storage request asynchronously.
/// Fire-and-forget for Put operations; Get/Load operations require sending responses back.
fn execute_storage_request(
    env: Rc<dyn Environment>,
    engine: Rc<RefCell<SyncEngine>>,
    request: StorageRequest,
) {
    wasm_bindgen_futures::spawn_local(async move {
        match request {
            StorageRequest::PutCommit { commit } => {
                env.put_commit(&commit).await;
            }
            StorageRequest::SetFrontier {
                object_id,
                branch,
                frontier,
            } => {
                env.set_frontier(object_id.into(), &branch, &frontier).await;
            }
            StorageRequest::PutChunk { data, .. } => {
                use bytes::Bytes;
                env.put_chunk(Bytes::from(data)).await;
            }
            StorageRequest::GetChunk { request_id, hash } => {
                use groove::sync::StorageResponse;
                let data = env.get_chunk(&hash).await.map(|b| b.to_vec());
                let inboxes = Inboxes {
                    storage_responses: vec![StorageResponse::ChunkLoaded {
                        request_id,
                        hash,
                        data,
                    }],
                    ..Default::default()
                };
                let outboxes = engine.borrow_mut().pass(inboxes);
                // Recursively handle any outboxes from this response
                // Note: This could cause deep recursion in edge cases
                for storage_req in outboxes.storage {
                    execute_storage_request(Rc::clone(&env), Rc::clone(&engine), storage_req);
                }
            }
            StorageRequest::LoadObject {
                request_id,
                object_id,
                branch,
            } => {
                use futures::StreamExt;
                use groove::sync::StorageResponse;

                // Load frontier
                let frontier = env.get_frontier(object_id.into(), &branch).await;

                // Load all commits for this object
                let mut commits = vec![];
                let mut stream = env.list_commits(object_id.into(), &branch);
                while let Some(commit_id) = stream.next().await {
                    if let Some(commit) = env.get_commit(&commit_id).await {
                        commits.push(commit);
                    }
                }

                // TODO: Load object_meta from somewhere (first commit's meta?)
                let object_meta = None;

                let inboxes = Inboxes {
                    storage_responses: vec![StorageResponse::ObjectLoaded {
                        request_id,
                        object_id,
                        branch,
                        object_meta,
                        frontier,
                        commits,
                    }],
                    ..Default::default()
                };
                let outboxes = engine.borrow_mut().pass(inboxes);
                for storage_req in outboxes.storage {
                    execute_storage_request(Rc::clone(&env), Rc::clone(&engine), storage_req);
                }
            }
        }
    });
}

/// Open an SSE stream to the server.
fn open_sse_stream(
    engine: &Rc<RefCell<SyncEngine>>,
    active_streams: &Rc<RefCell<HashMap<(u64, u32), ActiveStream>>>,
    server_url: &str,
    auth_token: &str,
    upstream_id: UpstreamId,
    subscription_id: u32,
    query: &str,
) {
    // Build URL with query params
    let encoded_token = js_sys::encode_uri_component(auth_token);
    let encoded_query = js_sys::encode_uri_component(query);
    let url = format!(
        "{}/sync/events?token={}&query={}",
        server_url, encoded_token, encoded_query
    );

    // Create EventSource
    let event_source = match EventSource::new(&url) {
        Ok(es) => es,
        Err(e) => {
            web_sys::console::error_1(&format!("Failed to create EventSource: {:?}", e).into());
            // Send connection failed event
            let engine_clone = Rc::clone(engine);
            let inboxes = Inboxes {
                connection_events: vec![ConnectionEvent {
                    upstream_id,
                    event: ConnectionEventKind::ConnectFailed {
                        error: format!("{:?}", e),
                    },
                }],
                ..Default::default()
            };
            let _ = engine_clone.borrow_mut().pass(inboxes);
            return;
        }
    };

    // Clone references for closures
    let engine_for_message = Rc::clone(engine);
    let engine_for_error = Rc::clone(engine);
    let engine_for_open = Rc::clone(engine);
    let active_streams_for_error = Rc::clone(active_streams);

    // Set up message handler
    let on_message = Closure::wrap(Box::new(move |event: MessageEvent| {
        if let Some(data) = event.data().as_string() {
            // Decode base64 data
            if let Ok(bytes) = base64_decode(&data) {
                if let Ok(sse_event) = SseEvent::from_bytes(&bytes) {
                    let inboxes = Inboxes {
                        sse_events: vec![SseInboxEvent {
                            upstream_id,
                            subscription_id,
                            event: sse_event,
                        }],
                        ..Default::default()
                    };
                    let _ = engine_for_message.borrow_mut().pass(inboxes);
                }
            }
        }
    }) as Box<dyn FnMut(MessageEvent)>);

    // Set up error handler
    let on_error = Closure::wrap(Box::new(move |_: web_sys::Event| {
        web_sys::console::error_1(&"SSE connection error".into());

        // Remove from active streams
        let key = (upstream_id.0, subscription_id);
        active_streams_for_error.borrow_mut().remove(&key);

        // Send stream closed event
        let inboxes = Inboxes {
            connection_events: vec![ConnectionEvent {
                upstream_id,
                event: ConnectionEventKind::StreamClosed {
                    subscription_id,
                    error: Some("Connection error".to_string()),
                },
            }],
            ..Default::default()
        };
        let _ = engine_for_error.borrow_mut().pass(inboxes);
    }) as Box<dyn FnMut(web_sys::Event)>);

    // Set up open handler
    let on_open = Closure::wrap(Box::new(move |_: web_sys::Event| {
        web_sys::console::log_1(&"SSE connection opened".into());

        let inboxes = Inboxes {
            connection_events: vec![ConnectionEvent {
                upstream_id,
                event: ConnectionEventKind::StreamOpened { subscription_id },
            }],
            ..Default::default()
        };
        let _ = engine_for_open.borrow_mut().pass(inboxes);
    }) as Box<dyn FnMut(web_sys::Event)>);

    // Attach handlers
    event_source.set_onmessage(Some(on_message.as_ref().unchecked_ref()));
    event_source.set_onerror(Some(on_error.as_ref().unchecked_ref()));
    event_source.set_onopen(Some(on_open.as_ref().unchecked_ref()));

    // Store in active streams
    let key = (upstream_id.0, subscription_id);
    active_streams.borrow_mut().insert(
        key,
        ActiveStream {
            event_source,
            _on_message: on_message,
            _on_error: on_error,
            _on_open: on_open,
        },
    );
}

/// Send a push request to the server.
fn send_push_request(
    engine: &Rc<RefCell<SyncEngine>>,
    server_url: &str,
    auth_token: &str,
    upstream_id: UpstreamId,
    request: groove::sync::PushRequest,
) {
    let engine = Rc::clone(engine);
    let url = format!("{}/sync/push", server_url);
    let auth_token = auth_token.to_string();
    let object_id = request.object_id;

    // Spawn async fetch
    wasm_bindgen_futures::spawn_local(async move {
        let result = async {
            // Encode request to binary
            let body = request.to_bytes();

            // Create fetch request
            let opts = RequestInit::new();
            opts.set_method("POST");
            opts.set_body(&Uint8Array::from(&body[..]).into());

            let request = Request::new_with_str_and_init(&url, &opts)?;
            request
                .headers()
                .set("Content-Type", "application/octet-stream")?;
            request
                .headers()
                .set("Authorization", &format!("Bearer {}", auth_token))?;

            // Execute fetch
            let window = web_sys::window().ok_or("no window")?;
            let resp_value = JsFuture::from(window.fetch_with_request(&request)).await?;
            let resp: Response = resp_value.dyn_into()?;

            if !resp.ok() {
                return Err(JsValue::from_str(&format!("HTTP {}", resp.status())));
            }

            // Read response body
            let array_buffer = JsFuture::from(resp.array_buffer()?).await?;
            let uint8_array = Uint8Array::new(&array_buffer);
            let bytes: Vec<u8> = uint8_array.to_vec();

            // Decode response
            let push_response = PushResponse::from_bytes(&bytes)
                .map_err(|e| JsValue::from_str(&format!("Decode error: {:?}", e)))?;

            Ok::<PushResponse, JsValue>(push_response)
        }
        .await;

        // Feed result back to engine
        let inboxes = Inboxes {
            push_responses: vec![PushResponseEvent {
                upstream_id,
                object_id,
                result: result.map_err(|e| format!("{:?}", e)),
            }],
            ..Default::default()
        };
        let _ = engine.borrow_mut().pass(inboxes);
    });
}

/// Decode base64 string to bytes.
fn base64_decode(input: &str) -> Result<Vec<u8>, String> {
    // Use web APIs for base64 decoding
    let window = web_sys::window().ok_or("no window")?;
    let decoded = window
        .atob(input)
        .map_err(|_| "base64 decode failed".to_string())?;

    Ok(decoded.chars().map(|c| c as u8).collect())
}
