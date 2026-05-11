//! Main-thread side of the dedicated-worker bridge.
//!
//! `WasmWorkerBridge::attach(worker, runtime, options)` installs the Rust
//! outbox sender on the main runtime, wires `worker.onmessage` to the Rust
//! dispatch loop, and exposes a JS-callable surface for the TypeScript adapter.
//!
//! The bridge:
//!
//! * Sends the `init` envelope as a JS object (`runtimeSources` cannot ride on
//!   postcard) and everything after as postcard-encoded `Uint8Array`.
//! * Batches outbox traffic via the shared `RustOutboxSender`, with an
//!   init-gate that holds entries back until `InitOk` arrives so the worker
//!   does not receive a `Sync` envelope before its runtime exists.
//! * Manages a deferred "upstream-ready" Promise that
//!   `waitForUpstreamServerConnection` awaits.

#![cfg(target_arch = "wasm32")]

use std::cell::{Cell, RefCell};
use std::rc::Rc;

use futures::channel::oneshot;
use futures::future::{select, Either};
use futures::FutureExt;
use js_sys::{Array, Function, Object, Promise, Reflect, Uint8Array};
use serde::Deserialize;
use serde_bytes::ByteBuf;
use wasm_bindgen::closure::Closure;
use wasm_bindgen::prelude::*;
use wasm_bindgen::JsCast;
use wasm_bindgen_futures::{future_to_promise, JsFuture};
use web_sys::{MessageEvent, Worker};

use crate::runtime::{RustOutboxSender, WasmRuntime};
use crate::worker_protocol::{
    main_to_worker_post, parse_lifecycle_event, parse_worker_to_main, MainToWorkerWire,
    ParsedWorkerToMain, SyncEntry, WorkerToMainWire,
};

const INIT_RESPONSE_TIMEOUT_MS: i32 = 12_000;
const SHUTDOWN_ACK_TIMEOUT_MS: i32 = 5_000;

// ---------------------------------------------------------------------------
// Options + state
// ---------------------------------------------------------------------------

#[derive(Debug, Deserialize, serde::Serialize)]
#[serde(rename_all = "camelCase")]
struct BridgeInitOptions {
    schema_json: String,
    app_id: String,
    env: String,
    user_branch: String,
    db_name: String,
    #[serde(default)]
    server_url: Option<String>,
    #[serde(default)]
    jwt_token: Option<String>,
    #[serde(default)]
    admin_secret: Option<String>,
    #[serde(default)]
    fallback_wasm_url: Option<String>,
    #[serde(default)]
    log_level: Option<String>,
    #[serde(default)]
    telemetry_collector_url: Option<String>,
}

#[derive(Clone, Copy, PartialEq, Eq)]
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
    sender: RustOutboxSender,
    init_message: JsValue,
    state: Cell<BridgeState>,
    worker_client_id: RefCell<Option<String>>,
    listeners: RefCell<Listeners>,
    on_message_closure: RefCell<Option<Closure<dyn FnMut(MessageEvent)>>>,
    init_resolver: RefCell<Option<oneshot::Sender<Result<String, String>>>>,
    init_promise: RefCell<Option<Promise>>,
    shutdown_resolver: RefCell<Option<oneshot::Sender<()>>>,
    expects_upstream: Cell<bool>,
    upstream_connected: Cell<bool>,
    has_forwarder: Cell<bool>,
    upstream_ready_promise: RefCell<Promise>,
    upstream_ready_resolver: RefCell<Option<Function>>,
}

#[wasm_bindgen]
pub struct WasmWorkerBridge {
    inner: Rc<BridgeInner>,
}

// ---------------------------------------------------------------------------
// Public API
// ---------------------------------------------------------------------------

#[wasm_bindgen]
impl WasmWorkerBridge {
    #[wasm_bindgen(js_name = attach)]
    pub fn attach(
        worker: Worker,
        runtime: &WasmRuntime,
        options: JsValue,
    ) -> Result<WasmWorkerBridge, JsError> {
        // 1. Parse options.
        let opts: BridgeInitOptions = serde_wasm_bindgen::from_value(options.clone())
            .map_err(|e| JsError::new(&format!("invalid bridge options: {e}")))?;
        let runtime_sources = Reflect::get(&options, &JsValue::from_str("runtimeSources"))
            .unwrap_or(JsValue::UNDEFINED);

        // 2. Build init JS-object envelope.
        let init_message = build_init_envelope(&opts, &runtime_sources)?;

        // 3. expects_upstream = server_url.is_some()
        let expects_upstream = opts.server_url.is_some();

        // 4. Construct and configure sender.
        let sender = RustOutboxSender::new(true);
        sender.attach_target(JsValue::from(worker.clone()), None, None, None);
        sender.set_init_gate(false);

        // 5. Install sender on runtime core.
        runtime
            .core
            .borrow_mut()
            .set_sync_sender(Box::new(sender.clone()));

        // 6. Deferred upstream-ready promise.
        let (upstream_promise, upstream_resolver) = make_deferred_promise();

        let inner = Rc::new(BridgeInner {
            worker: worker.clone(),
            runtime: runtime.clone(),
            sender: sender.clone(),
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
            upstream_ready_promise: RefCell::new(upstream_promise),
            upstream_ready_resolver: RefCell::new(Some(upstream_resolver)),
        });

        // 7. Install worker.onmessage.
        let inner_for_closure = Rc::clone(&inner);
        let closure = Closure::<dyn FnMut(MessageEvent)>::new(move |event: MessageEvent| {
            BridgeInner::handle_message(&inner_for_closure, event);
        });
        worker.set_onmessage(Some(closure.as_ref().unchecked_ref()));
        *inner.on_message_closure.borrow_mut() = Some(closure);

        // 8. runtime.add_server(None, Some(1.0)) — kicks off catalogue sync.
        runtime.add_server(None, Some(1.0)).map_err(|e| {
            let v: JsValue = e.into();
            JsError::new(&format!("add_server: {}", js_error_message(&v)))
        })?;

        // 9. Upstream-ready state.
        if expects_upstream {
            inner.mark_upstream_disconnected();
        } else {
            inner.mark_upstream_connected();
        }

        Ok(WasmWorkerBridge { inner })
    }

    #[wasm_bindgen]
    pub fn init(&self) -> Promise {
        if let Some(p) = self.inner.init_promise.borrow().clone() {
            return p;
        }
        let inner = Rc::clone(&self.inner);
        let attempt = inner.transition_init_called();
        match attempt {
            InitTransition::Proceed => {}
            InitTransition::Cached(promise) => return promise,
            InitTransition::Reject => {
                return Promise::reject(&JsValue::from_str("WorkerBridge has been disposed"));
            }
        }

        let (tx, rx) = oneshot::channel::<Result<String, String>>();
        *inner.init_resolver.borrow_mut() = Some(tx);

        // Post init synchronously.
        if let Err(err) = inner.worker.post_message(&inner.init_message) {
            *inner.init_resolver.borrow_mut() = None;
            inner.transition_init_failed();
            return Promise::reject(&JsValue::from_str(&format!(
                "postMessage init: {}",
                js_error_message(&err)
            )));
        }

        let inner_for_future = Rc::clone(&inner);
        let promise = future_to_promise(async move {
            let timeout = make_timeout(INIT_RESPONSE_TIMEOUT_MS);
            let rx = rx.map(|r| match r {
                Ok(v) => v,
                Err(_) => Err("init resolver dropped".to_string()),
            });
            let result = match select(Box::pin(rx), Box::pin(timeout)).await {
                Either::Left((r, _)) => r,
                Either::Right(_) => Err("Worker init timeout".to_string()),
            };
            match result {
                Ok(client_id) => {
                    if inner_for_future.transition_init_ok(client_id.clone()) {
                        // Only release the init gate while the bridge is still
                        // alive. After shutdown/dispose the noop sender is the
                        // intended sink — re-flushing would contradict that.
                        inner_for_future.sender.open_init_gate_and_flush();
                    }
                    let obj = Object::new();
                    Reflect::set(
                        &obj,
                        &JsValue::from_str("clientId"),
                        &JsValue::from_str(&client_id),
                    )
                    .ok();
                    Ok(obj.into())
                }
                Err(msg) => {
                    inner_for_future.transition_init_failed();
                    Err(JsValue::from_str(&format!("Worker init failed: {msg}")))
                }
            }
        });
        *inner.init_promise.borrow_mut() = Some(promise.clone());
        promise
    }

    #[wasm_bindgen(js_name = updateAuth)]
    pub fn update_auth(&self, jwt_token: Option<String>) {
        if self.inner.is_disposed_like() {
            return;
        }
        self.inner
            .post_wire(MainToWorkerWire::UpdateAuth { jwt_token });
    }

    #[wasm_bindgen(js_name = sendLifecycleHint)]
    pub fn send_lifecycle_hint(&self, event: &str) {
        if self.inner.is_disposed_like() {
            return;
        }
        let Some(parsed) = parse_lifecycle_event(event) else {
            tracing::warn!("unknown lifecycle event: {event}");
            return;
        };
        let sent_at_ms = js_sys::Date::now();
        self.inner.post_wire(MainToWorkerWire::LifecycleHint {
            event: parsed,
            sent_at_ms,
        });
    }

    #[wasm_bindgen(js_name = openPeer)]
    pub fn open_peer(&self, peer_id: &str) {
        if self.inner.is_disposed_like() {
            return;
        }
        self.inner.post_wire(MainToWorkerWire::PeerOpen {
            peer_id: peer_id.to_string(),
        });
    }

    #[wasm_bindgen(js_name = sendPeerSync)]
    pub fn send_peer_sync(&self, peer_id: &str, term: u32, payload: Array) {
        if self.inner.is_disposed_like() {
            return;
        }
        if payload.length() == 0 {
            return;
        }
        let mut payloads: Vec<ByteBuf> = Vec::with_capacity(payload.length() as usize);
        for entry in payload.iter() {
            if let Some(arr) = entry.dyn_ref::<Uint8Array>() {
                payloads.push(ByteBuf::from(arr.to_vec()));
            }
        }
        if payloads.is_empty() {
            return;
        }
        self.inner.post_wire(MainToWorkerWire::PeerSync {
            peer_id: peer_id.to_string(),
            term,
            payloads,
        });
    }

    #[wasm_bindgen(js_name = closePeer)]
    pub fn close_peer(&self, peer_id: &str) {
        if self.inner.is_disposed_like() {
            return;
        }
        self.inner.post_wire(MainToWorkerWire::PeerClose {
            peer_id: peer_id.to_string(),
        });
    }

    #[wasm_bindgen(js_name = setServerPayloadForwarder)]
    pub fn set_server_payload_forwarder(&self, callback: Option<Function>) {
        let has_forwarder = callback.is_some();
        self.inner.sender.set_server_payload_forwarder(callback);
        self.inner.has_forwarder.set(has_forwarder);
        if has_forwarder {
            self.inner.release_upstream_waiters();
        } else if self.inner.expects_upstream.get() && !self.inner.upstream_connected.get() {
            self.inner.rearm_upstream_ready_promise();
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
        if !self.inner.expects_upstream.get()
            || self.inner.has_forwarder.get()
            || self.inner.upstream_connected.get()
        {
            return Ok(());
        }
        let promise = self.inner.upstream_ready_promise.borrow().clone();
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
        self.inner.post_wire(MainToWorkerWire::DisconnectUpstream);
    }

    #[wasm_bindgen(js_name = reconnectUpstream)]
    pub fn reconnect_upstream(&self) {
        if self.inner.is_disposed_like() {
            return;
        }
        self.inner.post_wire(MainToWorkerWire::ReconnectUpstream);
    }

    #[wasm_bindgen(js_name = simulateCrash)]
    pub fn simulate_crash(&self) -> Promise {
        if self.inner.is_disposed_like() {
            return Promise::resolve(&JsValue::UNDEFINED);
        }
        let inner = Rc::clone(&self.inner);
        let (tx, rx) = oneshot::channel::<()>();
        *inner.shutdown_resolver.borrow_mut() = Some(tx);
        inner.post_wire(MainToWorkerWire::SimulateCrash);
        future_to_promise(async move {
            let timeout = make_timeout(SHUTDOWN_ACK_TIMEOUT_MS);
            let _ = select(Box::pin(rx.map(|_| ())), Box::pin(timeout)).await;
            Ok(JsValue::UNDEFINED)
        })
    }

    #[wasm_bindgen(js_name = acknowledgeRejectedBatch)]
    pub fn acknowledge_rejected_batch(&self, batch_id: &str) {
        if self.inner.is_disposed_like() {
            return;
        }
        self.inner
            .post_wire(MainToWorkerWire::AcknowledgeRejectedBatch {
                batch_id: batch_id.to_string(),
            });
    }

    #[wasm_bindgen(js_name = setListeners)]
    pub fn set_listeners(&self, listeners: JsValue) {
        let mut slot = self.inner.listeners.borrow_mut();
        slot.on_peer_sync = function_at(&listeners, "onPeerSync");
        slot.on_auth_failure = function_at(&listeners, "onAuthFailure");
        slot.on_local_batch_records_sync = function_at(&listeners, "onLocalBatchRecordsSync");
        slot.on_mutation_error_replay = function_at(&listeners, "onMutationErrorReplay");
    }

    #[wasm_bindgen(js_name = getWorkerClientId)]
    pub fn get_worker_client_id(&self) -> JsValue {
        match self.inner.worker_client_id.borrow().as_ref() {
            Some(id) => JsValue::from_str(id),
            None => JsValue::NULL,
        }
    }

    #[wasm_bindgen]
    pub fn shutdown(&self) -> Promise {
        let inner = Rc::clone(&self.inner);
        if inner.is_disposed_like() {
            return Promise::resolve(&JsValue::UNDEFINED);
        }
        inner.transition_shutdown_called();

        // 1. Drain main runtime outbox.
        inner.runtime.batched_tick();
        // 2. Flush sender synchronously.
        inner.sender.flush_now();
        // 3. Detach.
        inner.runtime.install_noop_sync_sender();
        inner.sender.set_server_payload_forwarder(None);
        inner.runtime.remove_server();

        let (tx, rx) = oneshot::channel::<()>();
        *inner.shutdown_resolver.borrow_mut() = Some(tx);
        inner.post_wire(MainToWorkerWire::Shutdown);

        let inner_for_future = Rc::clone(&inner);
        future_to_promise(async move {
            let timeout = make_timeout(SHUTDOWN_ACK_TIMEOUT_MS);
            let _ = select(Box::pin(rx.map(|_| ())), Box::pin(timeout)).await;
            inner_for_future.worker.set_onmessage(None);
            inner_for_future.transition_shutdown_finished();
            Ok(JsValue::UNDEFINED)
        })
    }
}

impl Drop for WasmWorkerBridge {
    fn drop(&mut self) {
        // If something already disposed us, do nothing.
        if !self.inner.is_disposed_like() {
            self.inner.dispose_internals();
        }
        self.inner.runtime.install_noop_sync_sender();
        self.inner.sender.set_server_payload_forwarder(None);
        self.inner.runtime.remove_server();
        self.inner.worker.set_onmessage(None);
    }
}

// ---------------------------------------------------------------------------
// State transitions
// ---------------------------------------------------------------------------

enum InitTransition {
    Proceed,
    Cached(Promise),
    Reject,
}

impl BridgeInner {
    fn is_disposed_like(&self) -> bool {
        matches!(
            self.state.get(),
            BridgeState::ShuttingDown | BridgeState::Disposed
        )
    }

    fn transition_init_called(self: &Rc<Self>) -> InitTransition {
        match self.state.get() {
            BridgeState::Idle => {
                self.state.set(BridgeState::Initializing);
                InitTransition::Proceed
            }
            BridgeState::Initializing | BridgeState::Ready => {
                if let Some(p) = self.init_promise.borrow().clone() {
                    InitTransition::Cached(p)
                } else {
                    InitTransition::Proceed
                }
            }
            BridgeState::Failed | BridgeState::ShuttingDown | BridgeState::Disposed => {
                InitTransition::Reject
            }
        }
    }

    /// Returns true if the transition fired; false if a terminal state
    /// (Failed/ShuttingDown/Disposed) had already been entered, in which case
    /// the caller must skip any follow-up side effects (e.g. reopening the
    /// outbox init gate).
    fn transition_init_ok(&self, client_id: String) -> bool {
        match self.state.get() {
            BridgeState::Idle | BridgeState::Initializing | BridgeState::Ready => {
                *self.worker_client_id.borrow_mut() = Some(client_id);
                self.state.set(BridgeState::Ready);
                true
            }
            BridgeState::Failed | BridgeState::ShuttingDown | BridgeState::Disposed => false,
        }
    }

    fn transition_init_failed(&self) {
        match self.state.get() {
            BridgeState::Idle | BridgeState::Initializing | BridgeState::Ready => {
                self.state.set(BridgeState::Failed);
            }
            BridgeState::Failed | BridgeState::ShuttingDown | BridgeState::Disposed => {}
        }
    }

    fn transition_shutdown_called(&self) {
        if self.state.get() != BridgeState::Disposed {
            self.state.set(BridgeState::ShuttingDown);
        }
    }

    fn transition_shutdown_finished(&self) {
        self.state.set(BridgeState::Disposed);
        *self.listeners.borrow_mut() = Listeners::default();
        *self.on_message_closure.borrow_mut() = None;
    }

    fn dispose_internals(&self) {
        self.state.set(BridgeState::Disposed);
        *self.listeners.borrow_mut() = Listeners::default();
        *self.on_message_closure.borrow_mut() = None;
    }
}

// ---------------------------------------------------------------------------
// Upstream-ready signalling
// ---------------------------------------------------------------------------

impl BridgeInner {
    fn mark_upstream_connected(&self) {
        self.upstream_connected.set(true);
        self.release_upstream_waiters();
    }

    fn mark_upstream_disconnected(&self) {
        if !self.expects_upstream.get() {
            self.upstream_connected.set(true);
            return;
        }
        let has_pending = self.upstream_ready_resolver.borrow().is_some();
        if has_pending {
            self.upstream_connected.set(false);
            return;
        }
        self.rearm_upstream_ready_promise();
        self.upstream_connected.set(false);
    }

    fn rearm_upstream_ready_promise(&self) {
        if self.upstream_ready_resolver.borrow().is_some() {
            return;
        }
        let (promise, resolver) = make_deferred_promise();
        *self.upstream_ready_promise.borrow_mut() = promise;
        *self.upstream_ready_resolver.borrow_mut() = Some(resolver);
    }

    fn release_upstream_waiters(&self) {
        let resolver = self.upstream_ready_resolver.borrow_mut().take();
        if let Some(f) = resolver {
            let _ = f.call0(&JsValue::NULL);
        }
    }
}

// ---------------------------------------------------------------------------
// Inbound dispatch
// ---------------------------------------------------------------------------

impl BridgeInner {
    fn handle_message(self: &Rc<Self>, event: MessageEvent) {
        let data = event.data();
        match parse_worker_to_main(&data) {
            ParsedWorkerToMain::Ready => {}
            ParsedWorkerToMain::Wire(wire) => self.dispatch_wire(wire),
            ParsedWorkerToMain::UnknownJsObject(t) => {
                tracing::warn!("ignoring unknown JS-object worker→main `{t}`");
            }
            ParsedWorkerToMain::DecodeError(e) => {
                tracing::warn!("worker→main decode error: {e}");
            }
            ParsedWorkerToMain::Malformed => {
                tracing::warn!("worker→main message neither Uint8Array nor known JS object");
            }
        }
    }

    fn dispatch_wire(self: &Rc<Self>, wire: WorkerToMainWire) {
        match wire {
            WorkerToMainWire::InitOk { client_id } => {
                if let Some(tx) = self.init_resolver.borrow_mut().take() {
                    let _ = tx.send(Ok(client_id));
                }
            }
            WorkerToMainWire::Error { message } => {
                if let Some(tx) = self.init_resolver.borrow_mut().take() {
                    let _ = tx.send(Err(message));
                } else {
                    tracing::warn!("worker reported error after init: {message}");
                }
            }
            WorkerToMainWire::UpstreamConnected => self.mark_upstream_connected(),
            WorkerToMainWire::UpstreamDisconnected => self.mark_upstream_disconnected(),
            WorkerToMainWire::AuthFailed { reason } => {
                let listener = self.listeners.borrow().on_auth_failure.clone();
                if let Some(cb) = listener {
                    let _ = cb.call1(&JsValue::NULL, &JsValue::from_str(&reason));
                }
            }
            WorkerToMainWire::LocalBatchRecordsSync {
                batches_json,
                encoded_records,
            } => {
                let listener = self.listeners.borrow().on_local_batch_records_sync.clone();
                if let Some(cb) = listener {
                    if let Ok(value) = json_parse(&batches_json) {
                        attach_encoded_records(&value, &encoded_records);
                        let _ = cb.call1(&JsValue::NULL, &value);
                    }
                }
            }
            WorkerToMainWire::MutationErrorReplay { batch_json } => {
                let listener = self.listeners.borrow().on_mutation_error_replay.clone();
                if let Some(cb) = listener {
                    if let Ok(value) = json_parse(&batch_json) {
                        let _ = cb.call1(&JsValue::NULL, &value);
                    }
                }
            }
            WorkerToMainWire::PeerSync {
                peer_id,
                term,
                payloads,
            } => {
                let listener = self.listeners.borrow().on_peer_sync.clone();
                if let Some(cb) = listener {
                    let obj = Object::new();
                    Reflect::set(
                        &obj,
                        &JsValue::from_str("peerId"),
                        &JsValue::from_str(&peer_id),
                    )
                    .ok();
                    Reflect::set(
                        &obj,
                        &JsValue::from_str("term"),
                        &JsValue::from_f64(term as f64),
                    )
                    .ok();
                    let arr = Array::new();
                    for b in payloads {
                        arr.push(&Uint8Array::from(b.as_ref()).into());
                    }
                    Reflect::set(&obj, &JsValue::from_str("payload"), &arr.into()).ok();
                    let _ = cb.call1(&JsValue::NULL, &obj.into());
                }
            }
            WorkerToMainWire::Sync { payloads } => {
                for entry in payloads {
                    let (payload, sequence) = sync_entry_to_runtime_payload(entry);
                    let _ = self.runtime.on_sync_message_received(payload, sequence);
                }
            }
            WorkerToMainWire::ShutdownOk => {
                if let Some(tx) = self.shutdown_resolver.borrow_mut().take() {
                    let _ = tx.send(());
                }
            }
            WorkerToMainWire::DebugSchemaStateOk { .. }
            | WorkerToMainWire::DebugSeedLiveSchemaOk => {
                // No listener; debug responses are consumed elsewhere.
            }
        }
    }

    fn post_wire(&self, wire: MainToWorkerWire) {
        match main_to_worker_post(&wire) {
            Ok((msg, transfer)) => {
                if let Err(err) = self
                    .worker
                    .post_message_with_transfer(&msg, &transfer.into())
                {
                    tracing::warn!("post_message failed: {}", js_error_message(&err));
                }
            }
            Err(e) => tracing::warn!(
                "encode {:?}: {}",
                std::any::type_name::<MainToWorkerWire>(),
                e
            ),
        }
    }
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

fn build_init_envelope(
    opts: &BridgeInitOptions,
    runtime_sources: &JsValue,
) -> Result<JsValue, JsError> {
    let serializer = serde_wasm_bindgen::Serializer::new().serialize_maps_as_objects(true);
    let value = serde::Serialize::serialize(opts, &serializer)
        .map_err(|e| JsError::new(&format!("init serialize: {e}")))?;
    let obj: Object = value
        .dyn_into()
        .map_err(|_| JsError::new("init serialize: expected object"))?;
    let obj_ref: &JsValue = obj.as_ref();
    js_set(obj_ref, "type", &JsValue::from_str("init"));
    js_set(obj_ref, "clientId", &JsValue::from_str(""));
    if !runtime_sources.is_undefined() && !runtime_sources.is_null() {
        js_set(obj_ref, "runtimeSources", runtime_sources);
    }
    Ok(obj.into())
}

fn js_set(obj: &JsValue, key: &str, value: &JsValue) {
    let _ = Reflect::set(obj, &JsValue::from_str(key), value);
}

fn function_at(obj: &JsValue, key: &str) -> Option<Function> {
    let v = Reflect::get(obj, &JsValue::from_str(key)).ok()?;
    v.dyn_into::<Function>().ok()
}

fn make_deferred_promise() -> (Promise, Function) {
    let resolver_slot: Rc<RefCell<Option<Function>>> = Rc::new(RefCell::new(None));
    let resolver_slot_clone = Rc::clone(&resolver_slot);
    let executor = Closure::<dyn FnMut(Function, Function)>::new(
        move |resolve: Function, _reject: Function| {
            *resolver_slot_clone.borrow_mut() = Some(resolve);
        },
    );
    let promise = Promise::new(&mut |resolve, _reject| {
        let _ = executor.as_ref().unchecked_ref::<Function>().call2(
            &JsValue::NULL,
            &resolve,
            &JsValue::UNDEFINED,
        );
    });
    let resolver = resolver_slot
        .borrow_mut()
        .take()
        .expect("resolver captured");
    drop(executor);
    (promise, resolver)
}

fn make_timeout(ms: i32) -> impl std::future::Future<Output = ()> {
    let global = js_sys::global();
    let set_timeout = Reflect::get(&global, &JsValue::from_str("setTimeout"))
        .ok()
        .and_then(|v| v.dyn_into::<Function>().ok());
    let promise = Promise::new(&mut |resolve, _reject| {
        if let Some(st) = set_timeout.as_ref() {
            let _ = st.call2(&JsValue::NULL, &resolve, &JsValue::from_f64(ms as f64));
        }
    });
    async move {
        let _ = JsFuture::from(promise).await;
    }
}

fn json_parse(s: &str) -> Result<JsValue, JsValue> {
    let global = js_sys::global();
    let json = Reflect::get(&global, &JsValue::from_str("JSON"))?;
    let parse: Function = Reflect::get(&json, &JsValue::from_str("parse"))?.dyn_into()?;
    parse.call1(&JsValue::NULL, &JsValue::from_str(s))
}

/// Walk a parsed `LocalBatchRecord` JS array and attach `encodedRecord`
/// (as a `Uint8Array`) on each object whose matching `encoded_records`
/// entry is present. Mirrors the legacy TS worker's `attachEncodedLocalBatchRecord`
/// so the main runtime can hydrate optimistic rows after a worker restart.
fn attach_encoded_records(value: &JsValue, encoded_records: &[Option<ByteBuf>]) {
    let Some(arr) = value.dyn_ref::<Array>() else {
        return;
    };
    let len = arr.length() as usize;
    for (i, encoded) in encoded_records.iter().enumerate() {
        if i >= len {
            break;
        }
        let Some(bytes) = encoded else { continue };
        let entry = arr.get(i as u32);
        if !entry.is_object() {
            continue;
        }
        let _ = Reflect::set(
            &entry,
            &JsValue::from_str("encodedRecord"),
            &Uint8Array::from(bytes.as_ref()).into(),
        );
    }
}

fn sync_entry_to_runtime_payload(entry: SyncEntry) -> (JsValue, Option<f64>) {
    match entry {
        SyncEntry::BareBytes(b) => (Uint8Array::from(b.as_ref()).into(), None),
        SyncEntry::BareString(s) => (JsValue::from_str(&s), None),
        SyncEntry::SequencedBytes { payload, sequence } => (
            Uint8Array::from(payload.as_ref()).into(),
            Some(sequence as f64),
        ),
        SyncEntry::SequencedString { payload, sequence } => {
            (JsValue::from_str(&payload), Some(sequence as f64))
        }
    }
}

fn js_error_message(value: &JsValue) -> String {
    if let Some(s) = value.as_string() {
        return s;
    }
    if let Ok(msg) = Reflect::get(value, &JsValue::from_str("message")) {
        if let Some(s) = msg.as_string() {
            return s;
        }
    }
    format!("{value:?}")
}
