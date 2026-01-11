//! WASM sync client for browser environments.
//!
//! Provides:
//! - `WasmClientEnv`: Implements `ClientEnv` using fetch + EventSource
//! - `WasmSyncClient`: JavaScript-friendly wrapper around `SyncClient<WasmClientEnv>`

use async_trait::async_trait;
use futures::channel::mpsc;
use futures::stream::{BoxStream, StreamExt};
use js_sys::{Array, Function, Uint8Array};
use wasm_bindgen::JsCast;
use wasm_bindgen::prelude::*;
use wasm_bindgen_futures::JsFuture;
use web_sys::{EventSource, Headers, MessageEvent, Request, RequestInit, RequestMode, Response};

use groove::sync::{
    ClientEnv, ClientEnvConfig, ClientError, Decode, Encode, PushRequest, PushResponse,
    ReconcileRequest, SseEvent, SubscribeRequest, SubscriptionOptions,
};
use groove::{Commit, CommitId, ObjectId};

// ============================================================================
// WasmClientEnv - implements ClientEnv for browser environments
// ============================================================================

/// Browser-based implementation of ClientEnv using fetch and EventSource.
pub struct WasmClientEnv {
    config: ClientEnvConfig,
}

impl WasmClientEnv {
    /// Create a new WASM client environment.
    pub fn new(config: ClientEnvConfig) -> Self {
        Self { config }
    }

    /// Helper: POST binary data using fetch.
    async fn post_binary(&self, url: &str, body: &[u8]) -> Result<Response, ClientError> {
        let headers = Headers::new().map_err(|e| ClientError::internal(format!("{:?}", e)))?;
        headers
            .set("Content-Type", "application/octet-stream")
            .map_err(|e| ClientError::internal(format!("{:?}", e)))?;
        headers
            .set(
                "Authorization",
                &format!("Bearer {}", self.config.auth_token),
            )
            .map_err(|e| ClientError::internal(format!("{:?}", e)))?;

        let body_array = Uint8Array::new_with_length(body.len() as u32);
        body_array.copy_from(body);

        let init = RequestInit::new();
        init.set_method("POST");
        init.set_headers(&headers);
        init.set_body(&body_array);
        init.set_mode(RequestMode::Cors);

        let request = Request::new_with_str_and_init(url, &init)
            .map_err(|e| ClientError::internal(format!("{:?}", e)))?;

        let window =
            web_sys::window().ok_or_else(|| ClientError::internal("No window available"))?;
        let response = JsFuture::from(window.fetch_with_request(&request))
            .await
            .map_err(|e| ClientError::internal(format!("Fetch error: {:?}", e)))?;

        response
            .dyn_into()
            .map_err(|_| ClientError::internal("Invalid response type"))
    }

    /// Helper: Get response body as bytes.
    async fn response_bytes(response: &Response) -> Result<Vec<u8>, ClientError> {
        let buffer = JsFuture::from(
            response
                .array_buffer()
                .map_err(|e| ClientError::internal(format!("{:?}", e)))?,
        )
        .await
        .map_err(|e| ClientError::internal(format!("{:?}", e)))?;

        Ok(Uint8Array::new(&buffer).to_vec())
    }
}

#[async_trait(?Send)]
impl ClientEnv for WasmClientEnv {
    async fn subscribe(
        &self,
        request: SubscribeRequest,
    ) -> Result<BoxStream<'static, Result<SseEvent, ClientError>>, ClientError> {
        // POST to /sync/subscribe to register the subscription
        let url = format!("{}/sync/subscribe", self.config.base_url);
        let body = request.to_bytes();

        let response = self.post_binary(&url, &body).await?;

        if !response.ok() {
            return Err(ClientError::new(
                response.status(),
                format!("Subscribe failed: {}", response.status_text()),
            ));
        }

        // Create channel for SSE events
        let (tx, rx) = mpsc::unbounded();

        // Open EventSource for SSE stream
        let sse_url = format!(
            "{}/sync/events?token={}",
            self.config.base_url, self.config.auth_token
        );

        let event_source = EventSource::new(&sse_url)
            .map_err(|e| ClientError::internal(format!("EventSource error: {:?}", e)))?;

        // Set up message handler
        let tx_clone = tx.clone();
        let on_message = Closure::wrap(Box::new(move |event: MessageEvent| {
            if let Some(data) = event.data().as_string() {
                // Parse SSE event data (base64 encoded binary)
                if let Some(bytes) = base64_decode(&data) {
                    match SseEvent::from_bytes(&bytes) {
                        Ok(sse_event) => {
                            let _ = tx_clone.unbounded_send(Ok(sse_event));
                        }
                        Err(e) => {
                            let _ = tx_clone.unbounded_send(Err(ClientError::internal(format!(
                                "Parse error: {}",
                                e
                            ))));
                        }
                    }
                }
            }
        }) as Box<dyn FnMut(MessageEvent)>);

        event_source.set_onmessage(Some(on_message.as_ref().unchecked_ref()));
        on_message.forget();

        // Set up error handler
        let tx_error = tx;
        let on_error = Closure::wrap(Box::new(move |_: web_sys::Event| {
            let _ = tx_error.unbounded_send(Err(ClientError::internal("SSE connection error")));
        }) as Box<dyn FnMut(web_sys::Event)>);

        event_source.set_onerror(Some(on_error.as_ref().unchecked_ref()));
        on_error.forget();

        // Return the stream
        Ok(rx.boxed())
    }

    async fn push(&self, request: PushRequest) -> Result<PushResponse, ClientError> {
        let url = format!("{}/sync/push", self.config.base_url);
        let body = request.to_bytes();

        let response = self.post_binary(&url, &body).await?;

        if !response.ok() {
            return Err(ClientError::new(
                response.status(),
                format!("Push failed: {}", response.status_text()),
            ));
        }

        let response_bytes = Self::response_bytes(&response).await?;
        PushResponse::from_bytes(&response_bytes)
            .map_err(|e| ClientError::internal(format!("Invalid response: {}", e)))
    }

    async fn reconcile(&self, request: ReconcileRequest) -> Result<SseEvent, ClientError> {
        let url = format!("{}/sync/reconcile", self.config.base_url);
        let body = request.to_bytes();

        let response = self.post_binary(&url, &body).await?;

        if !response.ok() {
            return Err(ClientError::new(
                response.status(),
                format!("Reconcile failed: {}", response.status_text()),
            ));
        }

        let response_bytes = Self::response_bytes(&response).await?;
        SseEvent::from_bytes(&response_bytes)
            .map_err(|e| ClientError::internal(format!("Invalid response: {}", e)))
    }

    async fn unsubscribe(&self, subscription_id: u32) -> Result<(), ClientError> {
        let url = format!(
            "{}/sync/unsubscribe?id={}",
            self.config.base_url, subscription_id
        );
        let response = self.post_binary(&url, &[]).await?;

        if !response.ok() {
            return Err(ClientError::new(
                response.status(),
                format!("Unsubscribe failed: {}", response.status_text()),
            ));
        }

        Ok(())
    }
}

// ============================================================================
// WasmSyncClient - JavaScript-friendly wrapper
// ============================================================================

/// Connection state for the sync client.
#[wasm_bindgen]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ConnectionState {
    Disconnected,
    Connecting,
    Connected,
}

/// WASM sync client.
///
/// Manages connection to a sync server from the browser.
/// This is a JavaScript-friendly wrapper around `SyncClient<WasmClientEnv>`.
#[wasm_bindgen]
pub struct WasmSyncClient {
    base_url: String,
    auth_token: String,
    connection_state: ConnectionState,
    event_source: Option<EventSource>,

    /// Callbacks for events
    on_commits: Option<Function>,
    on_excluded: Option<Function>,
    on_error: Option<Function>,
    on_state_change: Option<Function>,
}

// Non-wasm methods for Rust code
impl WasmSyncClient {
    /// Get a WasmClientEnv for use with SyncClient.
    /// This allows using the lower-level SyncClient API from Rust code.
    pub fn client_env(&self) -> WasmClientEnv {
        WasmClientEnv::new(ClientEnvConfig::new(
            self.base_url.clone(),
            self.auth_token.clone(),
        ))
    }
}

#[wasm_bindgen]
impl WasmSyncClient {
    /// Create a new sync client.
    ///
    /// # Arguments
    /// * `base_url` - The sync server URL (e.g., "http://localhost:8080")
    /// * `auth_token` - Bearer token for authentication
    #[wasm_bindgen(constructor)]
    pub fn new(base_url: String, auth_token: String) -> Self {
        Self {
            base_url,
            auth_token,
            connection_state: ConnectionState::Disconnected,
            event_source: None,
            on_commits: None,
            on_excluded: None,
            on_error: None,
            on_state_change: None,
        }
    }

    /// Set callback for commit events.
    ///
    /// Callback receives: (object_id: string, commits: Uint8Array[], frontier: string[])
    #[wasm_bindgen(js_name = setOnCommits)]
    pub fn set_on_commits(&mut self, callback: Function) {
        self.on_commits = Some(callback);
    }

    /// Set callback for excluded events.
    ///
    /// Callback receives: (object_id: string)
    #[wasm_bindgen(js_name = setOnExcluded)]
    pub fn set_on_excluded(&mut self, callback: Function) {
        self.on_excluded = Some(callback);
    }

    /// Set callback for error events.
    ///
    /// Callback receives: (code: number, message: string)
    #[wasm_bindgen(js_name = setOnError)]
    pub fn set_on_error(&mut self, callback: Function) {
        self.on_error = Some(callback);
    }

    /// Set callback for connection state changes.
    ///
    /// Callback receives: (state: string)
    #[wasm_bindgen(js_name = setOnStateChange)]
    pub fn set_on_state_change(&mut self, callback: Function) {
        self.on_state_change = Some(callback);
    }

    /// Get current connection state.
    #[wasm_bindgen(getter, js_name = connectionState)]
    pub fn connection_state(&self) -> ConnectionState {
        self.connection_state
    }

    /// Subscribe to a query and start receiving updates.
    ///
    /// This opens an SSE connection to receive real-time updates.
    #[wasm_bindgen]
    pub async fn subscribe(&mut self, query: String) -> Result<u32, JsValue> {
        self.set_connection_state(ConnectionState::Connecting);

        let env = self.client_env();

        // Create subscribe request
        let request = SubscribeRequest {
            query,
            options: SubscriptionOptions::default(),
        };

        // Use the ClientEnv to subscribe
        match env.subscribe(request).await {
            Ok(_stream) => {
                // TODO: Store stream and poll it for events
                self.set_connection_state(ConnectionState::Connected);
                Ok(1) // Return subscription ID
            }
            Err(e) => {
                self.set_connection_state(ConnectionState::Disconnected);
                Err(JsValue::from_str(&e.message))
            }
        }
    }

    /// Push commits for an object to the server.
    #[wasm_bindgen]
    pub async fn push(
        &mut self,
        object_id: String,
        commits_data: Array,
    ) -> Result<JsValue, JsValue> {
        let object_id_parsed = parse_object_id(&object_id)?;

        // Parse commits from JavaScript array of Uint8Arrays
        let mut commits = Vec::new();
        for i in 0..commits_data.length() {
            let commit_bytes: Uint8Array = commits_data.get(i).dyn_into()?;
            let bytes = commit_bytes.to_vec();
            let commit = Commit::from_bytes(&bytes)
                .map_err(|e| JsValue::from_str(&format!("Invalid commit: {}", e)))?;
            commits.push(commit);
        }

        // Create push request
        let request = PushRequest {
            object_id: ObjectId(object_id_parsed),
            commits,
            object_meta: None, // TODO: Include metadata for first push
        };

        let env = self.client_env();

        match env.push(request).await {
            Ok(push_response) => {
                // Return result as JS object
                let result = js_sys::Object::new();
                js_sys::Reflect::set(
                    &result,
                    &JsValue::from_str("accepted"),
                    &JsValue::from_bool(push_response.accepted),
                )?;
                js_sys::Reflect::set(
                    &result,
                    &JsValue::from_str("objectId"),
                    &JsValue::from_str(&push_response.object_id.to_string()),
                )?;

                let frontier_array = Array::new();
                for id in &push_response.frontier {
                    frontier_array.push(&JsValue::from_str(&hex::encode(id.as_bytes())));
                }
                js_sys::Reflect::set(&result, &JsValue::from_str("frontier"), &frontier_array)?;

                Ok(result.into())
            }
            Err(e) => Err(JsValue::from_str(&e.message)),
        }
    }

    /// Request reconciliation for an object.
    #[wasm_bindgen]
    pub async fn reconcile(
        &mut self,
        object_id: String,
        local_frontier: Array,
    ) -> Result<JsValue, JsValue> {
        let object_id_parsed = parse_object_id(&object_id)?;

        // Parse frontier from JavaScript array
        let mut frontier = Vec::new();
        for i in 0..local_frontier.length() {
            let id_str: String = local_frontier
                .get(i)
                .as_string()
                .ok_or_else(|| JsValue::from_str("Frontier must be strings"))?;
            let id_bytes =
                hex::decode(&id_str).map_err(|_| JsValue::from_str("Invalid hex in frontier"))?;
            if id_bytes.len() != 32 {
                return Err(JsValue::from_str("Commit ID must be 32 bytes"));
            }
            let mut arr = [0u8; 32];
            arr.copy_from_slice(&id_bytes);
            frontier.push(CommitId::from_bytes(arr));
        }

        // Create reconcile request
        let request = ReconcileRequest {
            object_id: ObjectId(object_id_parsed),
            local_frontier: frontier,
        };

        let env = self.client_env();

        match env.reconcile(request).await {
            Ok(event) => self.sse_event_to_js(&event),
            Err(e) => Err(JsValue::from_str(&e.message)),
        }
    }

    /// Disconnect from the server.
    #[wasm_bindgen]
    pub fn disconnect(&mut self) {
        if let Some(es) = self.event_source.take() {
            es.close();
        }
        self.set_connection_state(ConnectionState::Disconnected);
    }

    // Helper: Update connection state and call callback
    fn set_connection_state(&mut self, state: ConnectionState) {
        self.connection_state = state;
        if let Some(ref callback) = self.on_state_change {
            let state_str = match state {
                ConnectionState::Disconnected => "disconnected",
                ConnectionState::Connecting => "connecting",
                ConnectionState::Connected => "connected",
            };
            let _ = callback.call1(&JsValue::NULL, &JsValue::from_str(state_str));
        }
    }

    // Helper: Convert SseEvent to JavaScript value
    fn sse_event_to_js(&self, event: &SseEvent) -> Result<JsValue, JsValue> {
        let result = js_sys::Object::new();

        match event {
            SseEvent::Commits {
                object_id,
                commits,
                frontier,
                ..
            } => {
                js_sys::Reflect::set(
                    &result,
                    &JsValue::from_str("type"),
                    &JsValue::from_str("commits"),
                )?;
                js_sys::Reflect::set(
                    &result,
                    &JsValue::from_str("objectId"),
                    &JsValue::from_str(&object_id.to_string()),
                )?;

                let commits_array = Array::new();
                for commit in commits {
                    let bytes = commit.to_bytes();
                    let uint8 = Uint8Array::new_with_length(bytes.len() as u32);
                    uint8.copy_from(&bytes);
                    commits_array.push(&uint8);
                }
                js_sys::Reflect::set(&result, &JsValue::from_str("commits"), &commits_array)?;

                let frontier_array = Array::new();
                for id in frontier {
                    frontier_array.push(&JsValue::from_str(&hex::encode(id.as_bytes())));
                }
                js_sys::Reflect::set(&result, &JsValue::from_str("frontier"), &frontier_array)?;
            }
            SseEvent::Excluded { object_id } => {
                js_sys::Reflect::set(
                    &result,
                    &JsValue::from_str("type"),
                    &JsValue::from_str("excluded"),
                )?;
                js_sys::Reflect::set(
                    &result,
                    &JsValue::from_str("objectId"),
                    &JsValue::from_str(&object_id.to_string()),
                )?;
            }
            SseEvent::Truncate {
                object_id,
                truncate_at,
            } => {
                js_sys::Reflect::set(
                    &result,
                    &JsValue::from_str("type"),
                    &JsValue::from_str("truncate"),
                )?;
                js_sys::Reflect::set(
                    &result,
                    &JsValue::from_str("objectId"),
                    &JsValue::from_str(&object_id.to_string()),
                )?;
                js_sys::Reflect::set(
                    &result,
                    &JsValue::from_str("truncateAt"),
                    &JsValue::from_str(&hex::encode(truncate_at.as_bytes())),
                )?;
            }
            SseEvent::Request {
                object_id,
                commit_ids,
            } => {
                js_sys::Reflect::set(
                    &result,
                    &JsValue::from_str("type"),
                    &JsValue::from_str("request"),
                )?;
                js_sys::Reflect::set(
                    &result,
                    &JsValue::from_str("objectId"),
                    &JsValue::from_str(&object_id.to_string()),
                )?;
                let ids_array = Array::new();
                for id in commit_ids {
                    ids_array.push(&JsValue::from_str(&hex::encode(id.as_bytes())));
                }
                js_sys::Reflect::set(&result, &JsValue::from_str("commitIds"), &ids_array)?;
            }
            SseEvent::Error { code, message } => {
                js_sys::Reflect::set(
                    &result,
                    &JsValue::from_str("type"),
                    &JsValue::from_str("error"),
                )?;
                js_sys::Reflect::set(
                    &result,
                    &JsValue::from_str("code"),
                    &JsValue::from_f64(*code as f64),
                )?;
                js_sys::Reflect::set(
                    &result,
                    &JsValue::from_str("message"),
                    &JsValue::from_str(message),
                )?;
            }
        }

        Ok(result.into())
    }
}

// ============================================================================
// Helper functions
// ============================================================================

/// Parse object ID from string.
fn parse_object_id(s: &str) -> Result<u128, JsValue> {
    s.parse::<u128>()
        .map_err(|_| JsValue::from_str("Invalid object ID"))
}

/// Simple hex encoding.
mod hex {
    pub fn encode(bytes: &[u8]) -> String {
        bytes.iter().map(|b| format!("{:02x}", b)).collect()
    }

    pub fn decode(s: &str) -> Result<Vec<u8>, ()> {
        if s.len() % 2 != 0 {
            return Err(());
        }

        (0..s.len())
            .step_by(2)
            .map(|i| u8::from_str_radix(&s[i..i + 2], 16).map_err(|_| ()))
            .collect()
    }
}

/// Base64 decoding for SSE event data.
fn base64_decode(s: &str) -> Option<Vec<u8>> {
    const DECODE: [i8; 128] = [
        -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1,
        -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, 62, -1, -1,
        -1, 63, 52, 53, 54, 55, 56, 57, 58, 59, 60, 61, -1, -1, -1, -1, -1, -1, -1, 0, 1, 2, 3, 4,
        5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, -1, -1, -1,
        -1, -1, -1, 26, 27, 28, 29, 30, 31, 32, 33, 34, 35, 36, 37, 38, 39, 40, 41, 42, 43, 44, 45,
        46, 47, 48, 49, 50, 51, -1, -1, -1, -1, -1,
    ];

    let s = s.trim_end_matches('=');
    let mut result = Vec::new();

    for chunk in s.as_bytes().chunks(4) {
        let mut buf = [0u8; 4];
        for (i, &c) in chunk.iter().enumerate() {
            if c >= 128 {
                return None;
            }
            let val = DECODE[c as usize];
            if val < 0 {
                return None;
            }
            buf[i] = val as u8;
        }

        result.push((buf[0] << 2) | (buf[1] >> 4));
        if chunk.len() > 2 {
            result.push((buf[1] << 4) | (buf[2] >> 2));
        }
        if chunk.len() > 3 {
            result.push((buf[2] << 6) | buf[3]);
        }
    }

    Some(result)
}
