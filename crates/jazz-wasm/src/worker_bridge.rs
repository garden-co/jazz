//! Main-thread side of the worker bridge.
//!
//! Owns the worker `onmessage` handler, the bridge state machine, the init
//! handshake (with timeout), upstream-connected signalling, listener slots,
//! peer-channel postMessage surface, lifecycle hint forwarding, the forwarder
//! pass-through, the shutdown handshake (with timeout), and best-effort
//! `Drop` cleanup.
//!
//! ## Pre-init outbox buffering
//!
//! `attach()` closes the runtime's outbox init-gate so any outbox traffic the
//! main runtime emits before `init-ok` accumulates inside the Rust outbox
//! sender. `init()` opens the gate after `init-ok` arrives, flushing the
//! accumulated batch as one `{type:"sync",payload:[...]}` post.

#![cfg(target_arch = "wasm32")]
#![allow(dead_code)]

use std::cell::{Cell, RefCell};
use std::rc::Rc;

use futures::channel::oneshot;
use futures::future::{select, Either};
use js_sys::{Array, Function, Object, Reflect, Uint8Array};
use serde::Deserialize;
use wasm_bindgen::prelude::*;
use wasm_bindgen::JsCast;
use wasm_bindgen_futures::JsFuture;
use web_sys::{MessageEvent, Worker};

use crate::runtime::WasmRuntime;

const INIT_RESPONSE_TIMEOUT_MS: i32 = 12_000;
const SHUTDOWN_ACK_TIMEOUT_MS: i32 = 5_000;

// =============================================================================
// Public bridge
// =============================================================================

#[wasm_bindgen]
pub struct WasmWorkerBridge {
    inner: Rc<BridgeInner>,
}

#[wasm_bindgen]
impl WasmWorkerBridge {
    /// Attach a Rust bridge to an externally-constructed Worker.
    ///
    /// Per spec, options are parsed at attach time. `init()` is parameter-less.
    #[wasm_bindgen(js_name = attach)]
    pub fn attach(
        worker: Worker,
        runtime: &WasmRuntime,
        options: JsValue,
    ) -> Result<WasmWorkerBridge, JsError> {
        let opts: BridgeInitOptions = serde_wasm_bindgen::from_value(options.clone())
            .map_err(|e| JsError::new(&format!("invalid options: {e}")))?;
        let init_message = build_init_message(&opts, &options)?;
        let expects_upstream = opts.server_url.is_some();
        let runtime = runtime.clone();

        let inner = Rc::new(BridgeInner::new(
            worker.clone(),
            runtime.clone(),
            init_message,
            expects_upstream,
        ));

        // Install Rust onmessage handler.
        let on_message = {
            let inner = Rc::clone(&inner);
            Closure::<dyn FnMut(MessageEvent)>::new(move |event: MessageEvent| {
                inner.handle_message(event);
            })
        };
        worker.set_onmessage(Some(on_message.as_ref().unchecked_ref()));
        *inner.on_message_closure.borrow_mut() = Some(on_message);

        // Hand the worker to the runtime's outbox sender, with init-gate closed
        // so pre-init outbox traffic accumulates instead of leaking through.
        runtime.attach_outbox_target(worker.clone().into(), None, None, None);
        runtime.set_outbox_init_gate(false);

        // Register the worker as the upstream server for the main runtime.
        runtime
            .add_server(None, Some(1.0))
            .map_err(|e| JsError::new(&format!("addServer: {e:?}")))?;

        // Initial upstream-ready signalling.
        if expects_upstream {
            inner.mark_upstream_disconnected();
        } else {
            inner.mark_upstream_connected();
        }

        Ok(WasmWorkerBridge { inner })
    }

    /// Send the init message and resolve when the worker reports `init-ok`.
    /// Memoized — repeated calls return the same in-flight `Promise`.
    #[wasm_bindgen]
    pub fn init(&self) -> js_sys::Promise {
        if let Some(p) = self.inner.init_promise.borrow().clone() {
            return p;
        }
        let inner = Rc::clone(&self.inner);
        let promise = wasm_bindgen_futures::future_to_promise(async move { run_init(inner).await });
        *self.inner.init_promise.borrow_mut() = Some(promise.clone());
        promise
    }

    #[wasm_bindgen(js_name = updateAuth)]
    pub fn update_auth(&self, jwt_token: Option<String>) {
        if self.inner.is_disposed_like() {
            return;
        }
        let msg = Object::new();
        let _ = Reflect::set(&msg, &"type".into(), &"update-auth".into());
        if let Some(jwt) = jwt_token {
            let _ = Reflect::set(&msg, &"jwtToken".into(), &JsValue::from_str(&jwt));
        }
        let _ = self.inner.worker.post_message(&msg);
    }

    #[wasm_bindgen(js_name = sendLifecycleHint)]
    pub fn send_lifecycle_hint(&self, event: &str) {
        if self.inner.is_disposed_like() {
            return;
        }
        let msg = Object::new();
        let _ = Reflect::set(&msg, &"type".into(), &"lifecycle-hint".into());
        let _ = Reflect::set(&msg, &"event".into(), &JsValue::from_str(event));
        let _ = Reflect::set(
            &msg,
            &"sentAtMs".into(),
            &JsValue::from_f64(js_sys::Date::now()),
        );
        let _ = self.inner.worker.post_message(&msg);
    }

    #[wasm_bindgen(js_name = openPeer)]
    pub fn open_peer(&self, peer_id: &str) {
        if self.inner.is_disposed_like() {
            return;
        }
        let msg = Object::new();
        let _ = Reflect::set(&msg, &"type".into(), &"peer-open".into());
        let _ = Reflect::set(&msg, &"peerId".into(), &JsValue::from_str(peer_id));
        let _ = self.inner.worker.post_message(&msg);
    }

    #[wasm_bindgen(js_name = sendPeerSync)]
    pub fn send_peer_sync(&self, peer_id: &str, term: u32, payload: Array) {
        if self.inner.is_disposed_like() {
            return;
        }
        if payload.length() == 0 {
            return;
        }
        let msg = Object::new();
        let _ = Reflect::set(&msg, &"type".into(), &"peer-sync".into());
        let _ = Reflect::set(&msg, &"peerId".into(), &JsValue::from_str(peer_id));
        let _ = Reflect::set(&msg, &"term".into(), &JsValue::from_f64(term as f64));
        let _ = Reflect::set(&msg, &"payload".into(), &payload);
        let transfer = Array::new();
        for entry in payload.iter() {
            if let Some(arr) = entry.dyn_ref::<Uint8Array>() {
                transfer.push(&arr.buffer().into());
            }
        }
        let _ = self
            .inner
            .worker
            .post_message_with_transfer(&msg, transfer.as_ref());
    }

    #[wasm_bindgen(js_name = closePeer)]
    pub fn close_peer(&self, peer_id: &str) {
        if self.inner.is_disposed_like() {
            return;
        }
        let msg = Object::new();
        let _ = Reflect::set(&msg, &"type".into(), &"peer-close".into());
        let _ = Reflect::set(&msg, &"peerId".into(), &JsValue::from_str(peer_id));
        let _ = self.inner.worker.post_message(&msg);
    }

    #[wasm_bindgen(js_name = setServerPayloadForwarder)]
    pub fn set_server_payload_forwarder(&self, callback: Option<Function>) {
        if self.inner.is_disposed_like() {
            return;
        }
        let has_forwarder = callback.is_some();
        self.inner.has_forwarder.set(has_forwarder);
        self.inner.runtime.set_server_payload_forwarder(callback);
        // A forwarder install short-circuits the upstream wait gate (a
        // follower tab routes through the leader instead of the worker's own
        // upstream). Release any awaiters without flipping
        // `upstream_connected` — the gate is checked at call-time, and a
        // later `setServerPayloadForwarder(null)` should re-arm waiting.
        if has_forwarder {
            self.inner.release_upstream_waiters();
        }
    }

    #[wasm_bindgen(js_name = applyIncomingServerPayload)]
    pub fn apply_incoming_server_payload(&self, payload: Uint8Array) -> Result<(), JsError> {
        if self.inner.is_disposed_like() {
            return Ok(());
        }
        self.inner
            .runtime
            .on_sync_message_received(payload.into(), None)
    }

    #[wasm_bindgen(js_name = waitForUpstreamServerConnection)]
    pub async fn wait_for_upstream_server_connection(&self) -> Result<(), JsValue> {
        if !self.inner.expects_upstream.get() {
            return Ok(());
        }
        if self.inner.has_forwarder.get() {
            return Ok(());
        }
        if self.inner.upstream_connected.get() {
            return Ok(());
        }
        let promise = self.inner.upstream_ready_promise();
        JsFuture::from(promise).await.map(|_| ())
    }

    #[wasm_bindgen(js_name = replayServerConnection)]
    pub fn replay_server_connection(&self) {
        if self.inner.is_disposed_like() {
            return;
        }
        self.inner.runtime.remove_server();
        let _ = self.inner.runtime.add_server(None, None);
    }

    #[wasm_bindgen(js_name = disconnectUpstream)]
    pub fn disconnect_upstream(&self) {
        if self.inner.is_disposed_like() {
            return;
        }
        let msg = Object::new();
        let _ = Reflect::set(&msg, &"type".into(), &"disconnect-upstream".into());
        let _ = self.inner.worker.post_message(&msg);
    }

    #[wasm_bindgen(js_name = reconnectUpstream)]
    pub fn reconnect_upstream(&self) {
        if self.inner.is_disposed_like() {
            return;
        }
        let msg = Object::new();
        let _ = Reflect::set(&msg, &"type".into(), &"reconnect-upstream".into());
        let _ = self.inner.worker.post_message(&msg);
    }

    #[wasm_bindgen(js_name = acknowledgeRejectedBatch)]
    pub fn acknowledge_rejected_batch(&self, batch_id: &str) {
        if self.inner.is_disposed_like() {
            return;
        }
        let msg = Object::new();
        let _ = Reflect::set(&msg, &"type".into(), &"acknowledge-rejected-batch".into());
        let _ = Reflect::set(&msg, &"batchId".into(), &JsValue::from_str(batch_id));
        let _ = self.inner.worker.post_message(&msg);
    }

    #[wasm_bindgen(js_name = setListeners)]
    pub fn set_listeners(&self, listeners: JsValue) {
        let mut slots = self.inner.listeners.borrow_mut();
        slots.on_peer_sync = read_optional_function(&listeners, "onPeerSync");
        slots.on_auth_failure = read_optional_function(&listeners, "onAuthFailure");
        slots.on_local_batch_records_sync =
            read_optional_function(&listeners, "onLocalBatchRecordsSync");
        slots.on_mutation_error_replay =
            read_optional_function(&listeners, "onMutationErrorReplay");
    }

    /// Get the worker-assigned client id (post-init), or `null`.
    #[wasm_bindgen(js_name = getWorkerClientId)]
    pub fn get_worker_client_id(&self) -> JsValue {
        match self.inner.worker_client_id.borrow().as_deref() {
            Some(id) => JsValue::from_str(id),
            None => JsValue::NULL,
        }
    }

    #[wasm_bindgen]
    pub fn shutdown(&self) -> js_sys::Promise {
        let inner = Rc::clone(&self.inner);
        wasm_bindgen_futures::future_to_promise(async move { run_shutdown(inner).await })
    }
}

// Best-effort cleanup if the wrapper drops without an explicit `shutdown()`
// (e.g. a thrown exception during init). Spec lines 539–542.
impl Drop for WasmWorkerBridge {
    fn drop(&mut self) {
        if !self.inner.is_disposed_like() {
            self.inner.dispose_internals();
        }
        // Detach: install the noop sender, drop the server-edge, clear the
        // worker's `onmessage` slot. We do *not* post `Shutdown` from `Drop` —
        // by the time `Drop` runs in an exception path, the receiver may be
        // gone, and posting from a destructor risks structured-clone errors.
        self.inner.runtime.install_noop_sync_sender();
        self.inner.runtime.set_server_payload_forwarder(None);
        self.inner.runtime.remove_server();
        self.inner.worker.set_onmessage(None);
    }
}

async fn run_init(inner: Rc<BridgeInner>) -> Result<JsValue, JsValue> {
    if !inner.transition_init_called() {
        return Err(JsValue::from_str("WorkerBridge has been disposed"));
    }

    let (tx, rx) = oneshot::channel::<Result<String, String>>();
    *inner.init_resolver.borrow_mut() = Some(tx);

    if let Err(e) = inner.worker.post_message(&inner.init_message) {
        inner.transition_init_failed();
        return Err(JsValue::from_str(&format!("postMessage init: {e:?}")));
    }

    let timeout = make_timeout(INIT_RESPONSE_TIMEOUT_MS);
    let response = match select(rx, timeout).await {
        Either::Left((Ok(Ok(client_id)), _)) => Ok(client_id),
        Either::Left((Ok(Err(msg)), _)) => Err(msg),
        Either::Left((Err(_), _)) => Err("init resolver dropped".to_string()),
        Either::Right(_) => Err("Worker init timeout".to_string()),
    };

    match response {
        Ok(client_id) => {
            inner.transition_init_ok(client_id.clone());
            // Open the outbox init-gate so accumulated outbox traffic flushes.
            inner.runtime.open_outbox_init_gate();
            let result = Object::new();
            let _ = Reflect::set(&result, &"clientId".into(), &JsValue::from_str(&client_id));
            Ok(result.into())
        }
        Err(message) => {
            inner.transition_init_failed();
            Err(JsValue::from_str(&format!("Worker init failed: {message}")))
        }
    }
}

async fn run_shutdown(inner: Rc<BridgeInner>) -> Result<JsValue, JsValue> {
    if inner.is_disposed_like() {
        return Ok(JsValue::UNDEFINED);
    }
    inner.transition_shutdown_called();

    // Detach the outbox edge BEFORE posting shutdown. Spec line 528.
    inner.runtime.install_noop_sync_sender();
    inner.runtime.set_server_payload_forwarder(None);
    inner.runtime.remove_server();

    let (tx, rx) = oneshot::channel::<()>();
    *inner.shutdown_resolver.borrow_mut() = Some(tx);

    let msg = Object::new();
    let _ = Reflect::set(&msg, &"type".into(), &"shutdown".into());
    let _ = inner.worker.post_message(&msg);

    let timeout = make_timeout(SHUTDOWN_ACK_TIMEOUT_MS);
    let _ = select(rx, timeout).await;

    // Spec-compliant teardown: explicitly clear `worker.onmessage` so that any
    // late inbound messages don't invoke a freed Rust trampoline. `Closure::drop`
    // alone does NOT clear the JS slot — it just invalidates the call.
    inner.worker.set_onmessage(None);
    inner.transition_shutdown_finished();
    Ok(JsValue::UNDEFINED)
}

// =============================================================================
// Internal state
// =============================================================================

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum BridgeState {
    Idle,
    Initializing,
    Ready,
    Failed,
    ShuttingDown,
    Disposed,
}

#[derive(Default)]
struct Listeners {
    on_peer_sync: Option<Function>,
    on_auth_failure: Option<Function>,
    on_local_batch_records_sync: Option<Function>,
    on_mutation_error_replay: Option<Function>,
}

struct BridgeInner {
    worker: Worker,
    runtime: WasmRuntime,
    init_message: JsValue,
    state: Cell<BridgeState>,
    worker_client_id: RefCell<Option<String>>,
    listeners: RefCell<Listeners>,
    on_message_closure: RefCell<Option<Closure<dyn FnMut(MessageEvent)>>>,
    init_resolver: RefCell<Option<oneshot::Sender<Result<String, String>>>>,
    init_promise: RefCell<Option<js_sys::Promise>>,
    shutdown_resolver: RefCell<Option<oneshot::Sender<()>>>,
    expects_upstream: Cell<bool>,
    upstream_connected: Cell<bool>,
    has_forwarder: Cell<bool>,
    upstream_ready_promise: RefCell<js_sys::Promise>,
    upstream_ready_resolver: RefCell<Option<Function>>,
}

impl BridgeInner {
    fn new(
        worker: Worker,
        runtime: WasmRuntime,
        init_message: JsValue,
        expects_upstream: bool,
    ) -> Self {
        let (promise, resolver) = make_deferred_promise();
        Self {
            worker,
            runtime,
            init_message,
            state: Cell::new(BridgeState::Idle),
            worker_client_id: RefCell::new(None),
            listeners: RefCell::new(Listeners::default()),
            on_message_closure: RefCell::new(None),
            init_resolver: RefCell::new(None),
            init_promise: RefCell::new(None),
            shutdown_resolver: RefCell::new(None),
            expects_upstream: Cell::new(expects_upstream),
            upstream_connected: Cell::new(false),
            has_forwarder: Cell::new(false),
            upstream_ready_promise: RefCell::new(promise),
            upstream_ready_resolver: RefCell::new(Some(resolver)),
        }
    }

    fn is_disposed_like(&self) -> bool {
        matches!(
            self.state.get(),
            BridgeState::Disposed | BridgeState::ShuttingDown
        )
    }

    fn transition_init_called(&self) -> bool {
        match self.state.get() {
            BridgeState::Idle | BridgeState::Failed => {
                self.state.set(BridgeState::Initializing);
                true
            }
            // Memoized: repeated init() calls re-await the same Promise.
            BridgeState::Initializing | BridgeState::Ready => true,
            _ => false,
        }
    }

    fn transition_init_ok(&self, client_id: String) {
        if self.state.get() == BridgeState::Initializing {
            *self.worker_client_id.borrow_mut() = Some(client_id);
            self.state.set(BridgeState::Ready);
        }
    }

    fn transition_init_failed(&self) {
        if self.state.get() == BridgeState::Initializing {
            self.state.set(BridgeState::Failed);
        }
    }

    fn transition_shutdown_called(&self) {
        match self.state.get() {
            BridgeState::Disposed | BridgeState::ShuttingDown => {}
            _ => self.state.set(BridgeState::ShuttingDown),
        }
    }

    fn transition_shutdown_finished(&self) {
        if self.state.get() != BridgeState::Disposed {
            self.state.set(BridgeState::Disposed);
            self.dispose_internals();
        }
    }

    fn dispose_internals(&self) {
        *self.listeners.borrow_mut() = Listeners::default();
        *self.on_message_closure.borrow_mut() = None;
    }

    fn mark_upstream_connected(&self) {
        self.upstream_connected.set(true);
        self.release_upstream_waiters();
    }

    fn release_upstream_waiters(&self) {
        let resolver = self.upstream_ready_resolver.borrow_mut().take();
        if let Some(resolver) = resolver {
            let _ = resolver.call0(&JsValue::NULL);
        }
    }

    fn mark_upstream_disconnected(&self) {
        if !self.expects_upstream.get() {
            self.upstream_connected.set(true);
            return;
        }
        if !self.upstream_connected.get() && self.upstream_ready_resolver.borrow().is_some() {
            return;
        }
        let (promise, resolver) = make_deferred_promise();
        self.upstream_connected.set(false);
        *self.upstream_ready_promise.borrow_mut() = promise;
        *self.upstream_ready_resolver.borrow_mut() = Some(resolver);
    }

    fn upstream_ready_promise(&self) -> js_sys::Promise {
        self.upstream_ready_promise.borrow().clone()
    }

    // -------------------------------------------------------------------------
    // Worker → main message dispatch
    // -------------------------------------------------------------------------

    fn handle_message(&self, event: MessageEvent) {
        let data = event.data();
        let Some(type_str) = Reflect::get(&data, &"type".into())
            .ok()
            .and_then(|v| v.as_string())
        else {
            return;
        };

        match type_str.as_str() {
            "ready" => {}
            "init-ok" => {
                let client_id = Reflect::get(&data, &"clientId".into())
                    .ok()
                    .and_then(|v| v.as_string())
                    .unwrap_or_default();
                if let Some(tx) = self.init_resolver.borrow_mut().take() {
                    let _ = tx.send(Ok(client_id));
                }
            }
            "error" => {
                let msg = Reflect::get(&data, &"message".into())
                    .ok()
                    .and_then(|v| v.as_string())
                    .unwrap_or_default();
                if let Some(tx) = self.init_resolver.borrow_mut().take() {
                    let _ = tx.send(Err(msg));
                }
            }
            "upstream-connected" => self.mark_upstream_connected(),
            "upstream-disconnected" => self.mark_upstream_disconnected(),
            "auth-failed" => {
                let cb = self.listeners.borrow().on_auth_failure.clone();
                if let Some(cb) = cb {
                    let reason = Reflect::get(&data, &"reason".into()).unwrap_or(JsValue::NULL);
                    let _ = cb.call1(&JsValue::NULL, &reason);
                }
            }
            "local-batch-records-sync" => {
                let cb = self.listeners.borrow().on_local_batch_records_sync.clone();
                if let Some(cb) = cb {
                    let batches = Reflect::get(&data, &"batches".into()).unwrap_or(JsValue::NULL);
                    let _ = cb.call1(&JsValue::NULL, &batches);
                }
            }
            "mutation-error-replay" => {
                let cb = self.listeners.borrow().on_mutation_error_replay.clone();
                if let Some(cb) = cb {
                    let batch = Reflect::get(&data, &"batch".into()).unwrap_or(JsValue::NULL);
                    let _ = cb.call1(&JsValue::NULL, &batch);
                }
            }
            "peer-sync" => {
                let cb = self.listeners.borrow().on_peer_sync.clone();
                if let Some(cb) = cb {
                    let batch = Object::new();
                    let peer_id = Reflect::get(&data, &"peerId".into()).unwrap_or(JsValue::NULL);
                    let term = Reflect::get(&data, &"term".into()).unwrap_or(JsValue::NULL);
                    let payload = Reflect::get(&data, &"payload".into()).unwrap_or(JsValue::NULL);
                    let _ = Reflect::set(&batch, &"peerId".into(), &peer_id);
                    let _ = Reflect::set(&batch, &"term".into(), &term);
                    let _ = Reflect::set(&batch, &"payload".into(), &payload);
                    let _ = cb.call1(&JsValue::NULL, &batch.into());
                }
            }
            "sync" => self.handle_sync_to_main(&data),
            "shutdown-ok" => {
                if let Some(tx) = self.shutdown_resolver.borrow_mut().take() {
                    let _ = tx.send(());
                }
            }
            _ => {}
        }
    }

    fn handle_sync_to_main(&self, data: &JsValue) {
        let Ok(payload) = Reflect::get(data, &"payload".into()) else {
            return;
        };
        let Ok(arr) = payload.dyn_into::<Array>() else {
            return;
        };
        for entry in arr.iter() {
            if entry.is_instance_of::<Uint8Array>() || entry.is_string() {
                let _ = self.runtime.on_sync_message_received(entry, None);
            } else {
                let inner_payload =
                    Reflect::get(&entry, &"payload".into()).unwrap_or(JsValue::NULL);
                let sequence = Reflect::get(&entry, &"sequence".into())
                    .ok()
                    .and_then(|v| v.as_f64());
                let _ = self
                    .runtime
                    .on_sync_message_received(inner_payload, sequence);
            }
        }
    }
}

// =============================================================================
// Helpers
// =============================================================================

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct BridgeInitOptions {
    schema_json: String,
    app_id: String,
    env: String,
    user_branch: String,
    db_name: String,
    server_url: Option<String>,
    jwt_token: Option<String>,
    admin_secret: Option<String>,
    fallback_wasm_url: Option<String>,
    log_level: Option<String>,
    telemetry_collector_url: Option<String>,
}

fn build_init_message(opts: &BridgeInitOptions, original: &JsValue) -> Result<JsValue, JsError> {
    let msg = Object::new();
    let _ = Reflect::set(&msg, &"type".into(), &"init".into());
    let _ = Reflect::set(
        &msg,
        &"schemaJson".into(),
        &JsValue::from_str(&opts.schema_json),
    );
    let _ = Reflect::set(&msg, &"appId".into(), &JsValue::from_str(&opts.app_id));
    let _ = Reflect::set(&msg, &"env".into(), &JsValue::from_str(&opts.env));
    let _ = Reflect::set(
        &msg,
        &"userBranch".into(),
        &JsValue::from_str(&opts.user_branch),
    );
    let _ = Reflect::set(&msg, &"dbName".into(), &JsValue::from_str(&opts.db_name));
    let _ = Reflect::set(&msg, &"clientId".into(), &"".into());
    if let Some(url) = &opts.server_url {
        let _ = Reflect::set(&msg, &"serverUrl".into(), &JsValue::from_str(url));
    }
    if let Some(jwt) = &opts.jwt_token {
        let _ = Reflect::set(&msg, &"jwtToken".into(), &JsValue::from_str(jwt));
    }
    if let Some(secret) = &opts.admin_secret {
        let _ = Reflect::set(&msg, &"adminSecret".into(), &JsValue::from_str(secret));
    }
    if let Some(fallback) = &opts.fallback_wasm_url {
        let _ = Reflect::set(
            &msg,
            &"fallbackWasmUrl".into(),
            &JsValue::from_str(fallback),
        );
    }
    if let Some(level) = &opts.log_level {
        let _ = Reflect::set(&msg, &"logLevel".into(), &JsValue::from_str(level));
    }
    if let Some(url) = &opts.telemetry_collector_url {
        let _ = Reflect::set(
            &msg,
            &"telemetryCollectorUrl".into(),
            &JsValue::from_str(url),
        );
    }
    let runtime_sources =
        Reflect::get(original, &"runtimeSources".into()).unwrap_or(JsValue::UNDEFINED);
    if !runtime_sources.is_undefined() {
        let _ = Reflect::set(&msg, &"runtimeSources".into(), &runtime_sources);
    }
    Ok(msg.into())
}

fn read_optional_function(value: &JsValue, name: &str) -> Option<Function> {
    let v = Reflect::get(value, &JsValue::from_str(name)).ok()?;
    if v.is_undefined() || v.is_null() {
        return None;
    }
    v.dyn_into::<Function>().ok()
}

fn make_deferred_promise() -> (js_sys::Promise, Function) {
    let resolver_cell: Rc<RefCell<Option<Function>>> = Rc::new(RefCell::new(None));
    let resolver_cell_clone = Rc::clone(&resolver_cell);
    let promise = js_sys::Promise::new(&mut |resolve, _reject| {
        *resolver_cell_clone.borrow_mut() = Some(resolve);
    });
    let resolver = resolver_cell
        .borrow_mut()
        .take()
        .expect("Promise executor runs synchronously");
    (promise, resolver)
}

fn make_timeout(ms: i32) -> JsFuture {
    let global = js_sys::global();
    let set_timeout: Function = Reflect::get(&global, &"setTimeout".into())
        .expect("setTimeout exists")
        .dyn_into()
        .expect("setTimeout is a function");
    let promise = js_sys::Promise::new(&mut |resolve, _reject| {
        let _ = set_timeout.call2(&JsValue::NULL, &resolve, &JsValue::from_f64(ms as f64));
    });
    JsFuture::from(promise)
}
