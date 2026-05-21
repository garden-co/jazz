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
use jazz_tools::batch_fate::{BatchFate, LocalBatchRecord};
use jazz_tools::binding_support::parse_batch_id_input;
use jazz_tools::row_histories::BatchId;
use jazz_tools::sync_manager::DurabilityTier;
use js_sys::{Array, Function, Object, Reflect, Uint8Array};
use serde::Deserialize;
use wasm_bindgen::prelude::*;
use wasm_bindgen::JsCast;
use wasm_bindgen_futures::JsFuture;
use web_sys::{MessageEvent, Worker};

use crate::runtime::{RustOutboxSender, WasmRuntime};
use crate::worker_protocol::{
    main_to_worker_post, parse_worker_to_main, MainToWorkerWire, ParsedWorkerToMain, SyncEntry,
    WorkerLifecycleEvent, WorkerToMainWire,
};

const INIT_RESPONSE_TIMEOUT_MS: i32 = 12_000;
const SHUTDOWN_ACK_TIMEOUT_MS: i32 = 5_000;

fn parse_lifecycle_event(s: &str) -> Option<WorkerLifecycleEvent> {
    Some(match s {
        "visibility-hidden" => WorkerLifecycleEvent::VisibilityHidden,
        "visibility-visible" => WorkerLifecycleEvent::VisibilityVisible,
        "pagehide" => WorkerLifecycleEvent::Pagehide,
        "freeze" => WorkerLifecycleEvent::Freeze,
        "resume" => WorkerLifecycleEvent::Resume,
        _ => return None,
    })
}

/// Build a `Uint8Array` of postcard-encoded `MainToWorkerWire` bytes plus a
/// transfer list, then post via `worker.postMessage(value, transfer)`.
fn post_wire(worker: &Worker, msg: &MainToWorkerWire) {
    let Ok((value, transfer)) = main_to_worker_post(msg) else {
        return;
    };
    let _ = worker.post_message_with_transfer(&value, transfer.as_ref());
}

fn local_batch_record_needs_fate_reconciliation(record: &LocalBatchRecord) -> bool {
    match record.latest_fate.as_ref() {
        None => true,
        Some(BatchFate::DurableDirect { confirmed_tier, .. }) => {
            *confirmed_tier < DurabilityTier::EdgeServer
        }
        Some(BatchFate::AcceptedTransaction {
            confirmed_tier,
            visible_at,
            ..
        }) => {
            *confirmed_tier < DurabilityTier::EdgeServer
                || !visible_at.is_satisfied_by(*confirmed_tier)
        }
        Some(BatchFate::Missing { .. } | BatchFate::Rejected { .. }) => false,
    }
}

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
    /// Options are parsed at attach time; `init()` is parameter-less.
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

        // Construct the outbox sender and configure it for main-side use. The
        // sender batches server-bound outbox entries into binary postcard
        // `Sync` envelopes posted to the worker. Binary encoding for server-
        // bound is required (the worker decodes via `parse_main_to_worker`).
        let sender = RustOutboxSender::new(true);
        sender.attach_target(worker.clone().into(), None, None, None);
        sender.set_init_gate(false);

        // Install the sender on the runtime's `RuntimeCore` so its outbox
        // flush routes through us. We keep the `Rc<Inner>` clone in
        // `BridgeInner` so the bridge can later flip the init-gate, swap the
        // forwarder, etc.
        runtime
            .core
            .borrow_mut()
            .set_sync_sender(Box::new(sender.clone()));

        let inner = Rc::new(BridgeInner::new(
            worker.clone(),
            runtime.clone(),
            sender,
            init_message,
            expects_upstream,
        ));
        let weak_inner = Rc::downgrade(&inner);
        runtime.set_rejected_batch_acknowledged_callback(Some(Rc::new(move |batch_id| {
            if let Some(inner) = weak_inner.upgrade() {
                inner.acknowledge_rejected_batch(batch_id);
            }
        })));

        // Install Rust onmessage handler.
        let on_message = {
            let inner = Rc::clone(&inner);
            Closure::<dyn FnMut(MessageEvent)>::new(move |event: MessageEvent| {
                inner.handle_message(event);
            })
        };
        worker.set_onmessage(Some(on_message.as_ref().unchecked_ref()));
        *inner.on_message_closure.borrow_mut() = Some(on_message);

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
    ///
    /// State setup + the init `postMessage` happen *synchronously* before
    /// this returns. Only the `init-ok | error | timeout` race awaits inside
    /// the returned `Promise`. Callers that emit a synthetic `init-ok`
    /// straight after this call (tests) don't need a microtask yield to see
    /// the resolver installed.
    #[wasm_bindgen]
    pub fn init(&self) -> js_sys::Promise {
        if let Some(p) = self.inner.init_promise.borrow().clone() {
            return p;
        }

        if !self.inner.transition_init_called() {
            return js_sys::Promise::reject(&JsValue::from_str("WorkerBridge has been disposed"));
        }

        let (tx, rx) = oneshot::channel::<Result<String, String>>();
        *self.inner.init_resolver.borrow_mut() = Some(tx);

        if let Err(e) = self.inner.worker.post_message(&self.inner.init_message) {
            self.inner.init_resolver.borrow_mut().take();
            self.inner.transition_init_failed();
            return js_sys::Promise::reject(&JsValue::from_str(&format!(
                "postMessage init: {e:?}"
            )));
        }

        let inner = Rc::clone(&self.inner);
        let promise = wasm_bindgen_futures::future_to_promise(async move {
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
                    inner.sender.open_init_gate_and_flush();
                    let result = Object::new();
                    let _ =
                        Reflect::set(&result, &"clientId".into(), &JsValue::from_str(&client_id));
                    Ok(result.into())
                }
                Err(message) => {
                    inner.transition_init_failed();
                    Err(JsValue::from_str(&format!("Worker init failed: {message}")))
                }
            }
        });
        *self.inner.init_promise.borrow_mut() = Some(promise.clone());
        promise
    }

    #[wasm_bindgen(js_name = updateAuth)]
    pub fn update_auth(&self, jwt_token: Option<String>) {
        if self.inner.is_inactive() {
            return;
        }
        post_wire(
            &self.inner.worker,
            &MainToWorkerWire::UpdateAuth { jwt_token },
        );
    }

    #[wasm_bindgen(js_name = sendLifecycleHint)]
    pub fn send_lifecycle_hint(&self, event: &str) {
        if self.inner.is_inactive() {
            return;
        }
        let Some(parsed) = parse_lifecycle_event(event) else {
            tracing::warn!("unknown lifecycle event {event}");
            return;
        };
        post_wire(
            &self.inner.worker,
            &MainToWorkerWire::LifecycleHint {
                event: parsed,
                sent_at_ms: js_sys::Date::now(),
            },
        );
    }

    #[wasm_bindgen(js_name = openPeer)]
    pub fn open_peer(&self, peer_id: &str) {
        if self.inner.is_inactive() {
            return;
        }
        post_wire(
            &self.inner.worker,
            &MainToWorkerWire::PeerOpen {
                peer_id: peer_id.to_string(),
            },
        );
    }

    #[wasm_bindgen(js_name = sendPeerSync)]
    pub fn send_peer_sync(&self, peer_id: &str, term: u32, payload: Array) {
        if self.inner.is_inactive() {
            return;
        }
        if payload.length() == 0 {
            return;
        }
        let mut payloads: Vec<serde_bytes::ByteBuf> = Vec::with_capacity(payload.length() as usize);
        for entry in payload.iter() {
            if let Some(arr) = entry.dyn_ref::<Uint8Array>() {
                payloads.push(serde_bytes::ByteBuf::from(arr.to_vec()));
            }
        }
        if payloads.is_empty() {
            return;
        }
        post_wire(
            &self.inner.worker,
            &MainToWorkerWire::PeerSync {
                peer_id: peer_id.to_string(),
                term,
                payloads,
            },
        );
    }

    #[wasm_bindgen(js_name = closePeer)]
    pub fn close_peer(&self, peer_id: &str) {
        if self.inner.is_inactive() {
            return;
        }
        post_wire(
            &self.inner.worker,
            &MainToWorkerWire::PeerClose {
                peer_id: peer_id.to_string(),
            },
        );
    }

    #[wasm_bindgen(js_name = setServerPayloadForwarder)]
    pub fn set_server_payload_forwarder(&self, callback: Option<Function>) {
        if self.inner.is_inactive() {
            return;
        }
        let has_forwarder = callback.is_some();
        self.inner.has_forwarder.set(has_forwarder);
        self.inner.sender.set_server_payload_forwarder(callback);
        if has_forwarder {
            // Forwarder install short-circuits the upstream wait gate (a
            // follower tab routes through the leader instead of the worker's
            // own upstream). Release any current awaiters without flipping
            // `upstream_connected` — the gate is checked at call-time.
            self.inner.release_upstream_waiters();
        } else if self.inner.expects_upstream.get() && !self.inner.upstream_connected.get() {
            // Forwarder removed and the upstream isn't actually live yet —
            // re-arm a fresh ready-promise so subsequent
            // `waitForUpstreamServerConnection` calls actually wait.
            self.inner.rearm_upstream_ready_promise();
        }
    }

    #[wasm_bindgen(js_name = applyIncomingServerPayload)]
    pub fn apply_incoming_server_payload(&self, payload: Uint8Array) -> Result<(), JsError> {
        if self.inner.is_inactive() {
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
        if self.inner.is_inactive() {
            return;
        }
        self.inner.runtime.remove_server();
        let _ = self.inner.runtime.add_server(None, None);
    }

    #[wasm_bindgen(js_name = disconnectUpstream)]
    pub fn disconnect_upstream(&self) {
        if self.inner.is_inactive() {
            return;
        }
        post_wire(&self.inner.worker, &MainToWorkerWire::DisconnectUpstream);
    }

    #[wasm_bindgen(js_name = reconnectUpstream)]
    pub fn reconnect_upstream(&self) {
        if self.inner.is_inactive() {
            return;
        }
        post_wire(&self.inner.worker, &MainToWorkerWire::ReconnectUpstream);
    }

    /// Test-only: post `MainToWorkerWire::SimulateCrash` to the worker. The
    /// worker host releases OPFS handles without flushing a snapshot and
    /// posts `ShutdownOk`. Returns a Promise that resolves when the ack
    /// arrives (or after `SHUTDOWN_ACK_TIMEOUT_MS` regardless). Used by
    /// browser tests to validate WAL replay.
    #[wasm_bindgen(js_name = simulateCrash)]
    pub fn simulate_crash(&self) -> js_sys::Promise {
        let inner = Rc::clone(&self.inner);
        wasm_bindgen_futures::future_to_promise(async move {
            let (tx, rx) = oneshot::channel::<Result<(), String>>();
            *inner.shutdown_resolver.borrow_mut() = Some(tx);
            post_wire(&inner.worker, &MainToWorkerWire::SimulateCrash);
            let timeout = make_timeout(SHUTDOWN_ACK_TIMEOUT_MS);
            let _ = select(rx, timeout).await;
            Ok(JsValue::UNDEFINED)
        })
    }

    #[wasm_bindgen(js_name = setListeners)]
    pub fn set_listeners(&self, listeners: JsValue) {
        let mut slots = self.inner.listeners.borrow_mut();
        slots.on_peer_sync = read_optional_function(&listeners, "onPeerSync");
        slots.on_auth_failure = read_optional_function(&listeners, "onAuthFailure");
    }

    /// Get the worker-assigned client id (post-init), or `null`.
    #[wasm_bindgen(js_name = getWorkerClientId)]
    pub fn get_worker_client_id(&self) -> JsValue {
        match self.inner.worker_client_id.borrow().as_deref() {
            Some(id) => JsValue::from_str(id),
            None => JsValue::NULL,
        }
    }

    /// Tear the bridge down. Synchronous side-effects (final outbox flush,
    /// noop sender install, edge removal, `Shutdown` posted) happen before
    /// this returns. The returned `Promise` resolves on `shutdown-ok` or
    /// after `SHUTDOWN_ACK_TIMEOUT_MS`.
    #[wasm_bindgen]
    pub fn shutdown(&self) -> js_sys::Promise {
        if self.inner.is_disposed_like() {
            return js_sys::Promise::resolve(&JsValue::UNDEFINED);
        }
        // Init failed → the worker errored before reaching its main loop and
        // will not ack a `Shutdown` post. Do the synchronous detach (same as
        // `Drop`'s exception path) and skip the 5s ack wait.
        if self.inner.state.get() == BridgeState::Failed {
            self.inner.transition_shutdown_called();
            self.inner.runtime.install_noop_sync_sender();
            self.inner.sender.set_server_payload_forwarder(None);
            self.inner.runtime.remove_server();
            self.inner.worker.set_onmessage(None);
            self.inner.transition_shutdown_finished();
            return js_sys::Promise::resolve(&JsValue::UNDEFINED);
        }
        self.inner.transition_shutdown_called();
        // Drain any pending outbox entries to the worker BEFORE swapping in
        // the noop sender and posting `Shutdown`. Otherwise a writes-then-
        // unmount sequence loses the writes: the queued microtask flush
        // would post AFTER `Shutdown`, and the worker drops the runtime on
        // `Shutdown` so the late sync never reaches OPFS.
        //
        // Two-step drain:
        // 1. `runtime.batched_tick()` synchronously moves entries from the
        //    main runtime's outbox into the `RustOutboxSender`'s
        //    `pending_sync_entries`. The runtime's own `batched_tick` is
        //    normally scheduled via `setTimeout(0)` and may not have fired
        //    yet on a fast unmount-after-write.
        // 2. `sender.flush_now()` synchronously postcard-encodes those
        //    entries and posts them to the worker.
        self.inner.runtime.batched_tick();
        self.inner.sender.flush_now();
        // Detach the outbox edge.
        self.inner.runtime.install_noop_sync_sender();
        self.inner.sender.set_server_payload_forwarder(None);
        self.inner.runtime.remove_server();

        let (tx, rx) = oneshot::channel::<Result<(), String>>();
        *self.inner.shutdown_resolver.borrow_mut() = Some(tx);
        post_wire(&self.inner.worker, &MainToWorkerWire::Shutdown);

        let inner = Rc::clone(&self.inner);
        wasm_bindgen_futures::future_to_promise(async move {
            let timeout = make_timeout(SHUTDOWN_ACK_TIMEOUT_MS);
            // Clear `worker.onmessage` so late inbound messages don't invoke
            // a freed Rust trampoline. `Closure::drop` alone does NOT clear
            // the JS slot.
            let shutdown_result = match select(rx, timeout).await {
                Either::Left((Ok(Ok(())), _)) => Ok(()),
                Either::Left((Ok(Err(message)), _)) => Err(message),
                Either::Left((Err(_), _)) => Err("shutdown resolver dropped".to_string()),
                Either::Right(_) => Ok(()),
            };
            inner.worker.set_onmessage(None);
            inner.transition_shutdown_finished();
            shutdown_result
                .map(|()| JsValue::UNDEFINED)
                .map_err(|message| JsValue::from_str(&message))
        })
    }
}

// Best-effort cleanup if the wrapper drops without an explicit `shutdown()`
// (e.g. a thrown exception during init).
impl Drop for WasmWorkerBridge {
    fn drop(&mut self) {
        // If `shutdown()` already ran, it has already installed the noop
        // sender, removed the server edge, and cleared `onmessage`. Re-doing
        // those here is not idempotent against the shared `WasmRuntime`: by
        // the time wasm-bindgen's FinalizationRegistry fires `Drop` on a
        // disposed bridge, the runtime may have been re-attached to a
        // successor bridge (see `Db.restartWorkerWithCurrentDbName`), and
        // clobbering its sender/server edge silently breaks outbox traffic.
        if self.inner.is_disposed_like() {
            return;
        }
        self.inner.dispose_internals();
        // Detach: install the noop sender, drop the server-edge, clear the
        // worker's `onmessage` slot. We do *not* post `Shutdown` from `Drop` —
        // by the time `Drop` runs in an exception path, the receiver may be
        // gone, and posting from a destructor risks structured-clone errors.
        self.inner.runtime.install_noop_sync_sender();
        self.inner.sender.set_server_payload_forwarder(None);
        self.inner.runtime.remove_server();
        self.inner.worker.set_onmessage(None);
    }
}

// `run_init` and `run_shutdown` are now inlined into the wasm-bindgen
// `init` / `shutdown` methods so synchronous setup happens eagerly. See
// the method bodies above.

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
}

struct BridgeInner {
    worker: Worker,
    runtime: WasmRuntime,
    /// `Rc<Inner>`-shared clone of the outbox sender installed on the
    /// runtime's `RuntimeCore`. The bridge mutates it directly to flip the
    /// init-gate, install/clear the server-payload forwarder, and detach on
    /// shutdown.
    sender: RustOutboxSender,
    init_message: JsValue,
    state: Cell<BridgeState>,
    worker_client_id: RefCell<Option<String>>,
    listeners: RefCell<Listeners>,
    on_message_closure: RefCell<Option<Closure<dyn FnMut(MessageEvent)>>>,
    init_resolver: RefCell<Option<oneshot::Sender<Result<String, String>>>>,
    init_promise: RefCell<Option<js_sys::Promise>>,
    shutdown_resolver: RefCell<Option<oneshot::Sender<Result<(), String>>>>,
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
        sender: RustOutboxSender,
        init_message: JsValue,
        expects_upstream: bool,
    ) -> Self {
        let (promise, resolver) = make_deferred_promise();
        Self {
            worker,
            runtime,
            sender,
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

    /// True once the bridge can no longer do useful work — either init
    /// failed, shutdown is in flight, or shutdown finished. Public methods
    /// that would otherwise post to or wait on the worker use this so they
    /// don't hang on a worker that will never reply. `Drop` deliberately
    /// uses the narrower `is_disposed_like` because a `Failed` bridge still
    /// owns its runtime/server-edge and must be torn down here.
    fn is_inactive(&self) -> bool {
        matches!(
            self.state.get(),
            BridgeState::Failed | BridgeState::Disposed | BridgeState::ShuttingDown
        )
    }

    fn acknowledge_rejected_batch(&self, batch_id: BatchId) {
        if self.is_inactive() {
            return;
        }
        post_wire(
            &self.worker,
            &MainToWorkerWire::AcknowledgeRejectedBatch {
                batch_id: batch_id.to_string(),
            },
        );
    }

    fn hydrate_worker_local_batch_records(&self, encoded_records: Vec<serde_bytes::ByteBuf>) {
        for row in encoded_records {
            match LocalBatchRecord::decode_storage_row(row.as_ref()) {
                Ok(record) => self.hydrate_worker_local_batch_record(record),
                Err(error) => tracing::warn!("decode worker local batch record: {error:?}"),
            }
        }
    }

    fn hydrate_worker_local_batch_record(&self, record: LocalBatchRecord) {
        let should_reconcile = local_batch_record_needs_fate_reconciliation(&record);
        let batch_id = record.batch_id;
        let mut core = self.runtime.core.borrow_mut();
        if let Some(BatchFate::Rejected { code, reason, .. }) = record.latest_fate.clone() {
            if let Err(error) = core.replay_batch_rejection(record.batch_id, &code, &reason) {
                tracing::warn!(
                    batch_id = ?record.batch_id,
                    %error,
                    "replay worker rejected batch during hydration failed"
                );
            }
        }
        if let Err(error) = core.hydrate_local_batch_record(record) {
            tracing::warn!("hydrate worker local batch record: {error:?}");
            return;
        }
        if should_reconcile {
            core.reconcile_local_batch_with_server(batch_id);
        }
    }

    fn replay_worker_mutation_error(&self, batch_id: &str, code: &str, reason: &str) {
        let batch_id = match parse_batch_id_input(batch_id) {
            Ok(batch_id) => batch_id,
            Err(error) => {
                tracing::warn!("decode worker mutation error batch id: {error:?}");
                return;
            }
        };
        if let Err(error) = self
            .runtime
            .core
            .borrow_mut()
            .replay_batch_rejection(batch_id, code, reason)
        {
            tracing::warn!(
                ?batch_id,
                %error,
                "replay worker mutation error failed"
            );
        }
    }

    fn transition_init_called(&self) -> bool {
        match self.state.get() {
            BridgeState::Idle => {
                self.state.set(BridgeState::Initializing);
                true
            }
            // Memoized: repeated init() calls re-await the same Promise.
            BridgeState::Initializing | BridgeState::Ready => true,
            // Failed is terminal. The JS shim's `initMessage` guard and the
            // worker host's `HOST.is_some()` short-circuit both forbid a
            // second bootstrap on the same worker, so reusing this bridge
            // for a retry would just hang to timeout. Callers must drop and
            // re-attach with a fresh worker to recover.
            BridgeState::Failed | BridgeState::ShuttingDown | BridgeState::Disposed => false,
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

    /// Re-arm a fresh upstream-ready promise. Used when a forwarder is
    /// removed while the worker's upstream still hasn't connected — fresh
    /// `waitForUpstreamServerConnection` calls need to actually block again.
    fn rearm_upstream_ready_promise(&self) {
        if self.upstream_ready_resolver.borrow().is_some() {
            return;
        }
        let (promise, resolver) = make_deferred_promise();
        *self.upstream_ready_promise.borrow_mut() = promise;
        *self.upstream_ready_resolver.borrow_mut() = Some(resolver);
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
        match parse_worker_to_main(&data) {
            ParsedWorkerToMain::Ready => {}
            ParsedWorkerToMain::Wire(wire) => self.dispatch_wire(wire),
            ParsedWorkerToMain::UnknownJsObject(t) => {
                tracing::warn!("ignoring unknown JS-object worker→main message {t}")
            }
            ParsedWorkerToMain::DecodeError(e) => {
                tracing::warn!("worker→main decode error: {e}")
            }
            ParsedWorkerToMain::Malformed => {
                tracing::warn!("worker→main message neither Uint8Array nor known JS object")
            }
        }
    }

    fn dispatch_wire(&self, wire: WorkerToMainWire) {
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
                    tracing::warn!("worker error: {message}");
                }
            }
            WorkerToMainWire::UpstreamConnected => self.mark_upstream_connected(),
            WorkerToMainWire::UpstreamDisconnected => self.mark_upstream_disconnected(),
            WorkerToMainWire::AuthFailed { reason } => {
                let cb = self.listeners.borrow().on_auth_failure.clone();
                if let Some(cb) = cb {
                    let _ = cb.call1(&JsValue::NULL, &JsValue::from_str(&reason));
                }
            }
            WorkerToMainWire::LocalBatchRecordsSync { encoded_records } => {
                self.hydrate_worker_local_batch_records(encoded_records);
            }
            WorkerToMainWire::MutationErrorReplay {
                batch_id,
                code,
                reason,
            } => {
                self.replay_worker_mutation_error(&batch_id, &code, &reason);
            }
            WorkerToMainWire::PeerSync {
                peer_id,
                term,
                payloads,
            } => {
                let cb = self.listeners.borrow().on_peer_sync.clone();
                if let Some(cb) = cb {
                    let payload_array = Array::new();
                    for entry in &payloads {
                        let arr = Uint8Array::from(entry.as_ref());
                        payload_array.push(&arr);
                    }
                    let batch = Object::new();
                    let _ = Reflect::set(&batch, &"peerId".into(), &JsValue::from_str(&peer_id));
                    let _ = Reflect::set(&batch, &"term".into(), &JsValue::from_f64(term as f64));
                    let _ = Reflect::set(&batch, &"payload".into(), &payload_array);
                    let _ = cb.call1(&JsValue::NULL, &batch.into());
                }
            }
            WorkerToMainWire::Sync { payloads } => {
                let had_payloads = !payloads.is_empty();
                for entry in payloads {
                    match entry {
                        SyncEntry::BareBytes(bytes) => {
                            let arr = Uint8Array::from(bytes.as_ref());
                            let _ = self.runtime.on_sync_message_received(arr.into(), None);
                        }
                        SyncEntry::BareString(s) => {
                            let _ = self
                                .runtime
                                .on_sync_message_received(JsValue::from_str(&s), None);
                        }
                        SyncEntry::SequencedBytes { payload, sequence } => {
                            let arr = Uint8Array::from(payload.as_ref());
                            let _ = self
                                .runtime
                                .on_sync_message_received(arr.into(), Some(sequence as f64));
                        }
                        SyncEntry::SequencedString { payload, sequence } => {
                            let _ = self.runtime.on_sync_message_received(
                                JsValue::from_str(&payload),
                                Some(sequence as f64),
                            );
                        }
                    }
                }
                // Drained worker-side messages need a tick on the main runtime
                // so subscriptions wake. Matches main's TS bridge that called
                // `runtime.batchedTick()` after each worker→main sync batch.
                if had_payloads {
                    self.runtime.batched_tick();
                }
            }
            WorkerToMainWire::ShutdownOk => {
                if let Some(tx) = self.shutdown_resolver.borrow_mut().take() {
                    let _ = tx.send(Ok(()));
                }
            }
            WorkerToMainWire::ShutdownFailed { message } => {
                if let Some(tx) = self.shutdown_resolver.borrow_mut().take() {
                    let _ = tx.send(Err(message));
                } else {
                    tracing::warn!("worker shutdown failed without pending shutdown: {message}");
                }
            }
            WorkerToMainWire::DebugSchemaStateOk { .. }
            | WorkerToMainWire::DebugSeedLiveSchemaOk => {
                // Test-only debug responses; no listener slot in the bridge.
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
    JsFuture::from(make_timeout_promise(ms))
}

fn make_timeout_promise(ms: i32) -> js_sys::Promise {
    let global = js_sys::global();
    let set_timeout: Function = Reflect::get(&global, &"setTimeout".into())
        .expect("setTimeout exists")
        .dyn_into()
        .expect("setTimeout is a function");
    js_sys::Promise::new(&mut |resolve, _reject| {
        let _ = set_timeout.call2(&JsValue::NULL, &resolve, &JsValue::from_f64(ms as f64));
    })
}
