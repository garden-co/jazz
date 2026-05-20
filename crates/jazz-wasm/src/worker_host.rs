//! Worker-side runtime host.
//!
//! Owns `self.onmessage`, the `WasmRuntime`, the peer table, the bootstrap
//! catalogue handoff, the post-init upstream connect, and the
//! shutdown/simulate-crash handlers.
//!
//! ## Init ordering
//!
//! Stage 2 ordering moves the upstream connect *before* the parked-sync drain
//! so server-bound traffic from drained main writes routes through the Rust
//! transport (not into the catalogue forwarder + dropped). Order:
//!
//!   1. open runtime, register clients
//!   2. attach outbox target (worker side)
//!   3. bootstrap catalogue (`addServer` / `removeServer` while flag is set)
//!   4. connect upstream (install Rust transport handle)
//!   5. drain pending pre-init messages (sync, peer-sync, control)
//!   6. sync retained local batch records + queue rejected-batch replay
//!   7. flip state to `Ready`
//!   8. post `init-ok`
//!
//! ## Pre-init message buffering
//!
//! The JS shim buffers every message it receives between `ready` and the Rust
//! takeover; `run_as_worker` parses each into `MainToWorkerMessage` and pushes
//! into `host.pending_messages`. Messages that arrive *during* the
//! init handshake (between Rust's `set_onmessage` and the `Ready` flip) also
//! land here via `handle_main_message`. After `Ready`, the queue drains in
//! arrival order.
//!
//! ## Reentrancy
//!
//! Three `thread_local!` cells split borrowing:
//! - `HOST`            — state machine + pending queue + closures
//! - `RUNTIME`         — `Rc<WasmRuntime>` (cloned into outbox callbacks)
//! - `MAIN_CLIENT_ID`  — runtime client id assigned to the main-thread peer.
//!                       Different cell than `HOST` so the outbox lookup does
//!                       not re-borrow `HOST` while `HOST` is already borrowed
//!                       elsewhere.

#![cfg(target_arch = "wasm32")]
#![allow(dead_code)]

use std::any::Any;
use std::cell::RefCell;
use std::collections::{HashMap, VecDeque};
use std::rc::Rc;

use jazz_tools::runtime_core::SyncSender;
use jazz_tools::sync_manager::{ClientId, Destination, OutboxEntry};
use js_sys::{Array, Reflect, Uint8Array};
use serde_bytes::ByteBuf;
use wasm_bindgen::prelude::*;
use wasm_bindgen::JsCast;
use web_sys::{DedicatedWorkerGlobalScope, MessageEvent, MessagePort};

use crate::runtime::{RustOutboxSender, WasmRuntime};
use crate::worker_protocol::{
    parse_main_to_worker, worker_to_main_post, InitPayload, MainToWorkerMessage, MainToWorkerWire,
    WorkerLifecycleEvent, WorkerToMainWire,
};

// =============================================================================
// Thread-local cells
// =============================================================================

thread_local! {
    static HOST: RefCell<Option<WorkerHost>> = const { RefCell::new(None) };
    static RUNTIME: RefCell<Option<Rc<WasmRuntime>>> = const { RefCell::new(None) };
    static MAIN_CLIENT_ID: RefCell<Option<String>> = const { RefCell::new(None) };
    /// Follower-tab peers attached via `attach-tab-port`. Keyed by the
    /// runtime client id assigned when the port was adopted. The leader's own
    /// main-thread peer lives in `MAIN_CLIENT_ID`, not here.
    static PORT_PEERS: RefCell<HashMap<String, PortPeer>> = RefCell::new(HashMap::new());
}

/// One adopted follower tab. Holds the `MessagePort`, the per-port
/// `RustOutboxSender` (configured to route messages destined for this peer's
/// client id to the port's `postMessage`), and the closure backing the
/// port's `onmessage` slot. Keyed by the runtime client id inside
/// {@link PORT_PEERS}.
struct PortPeer {
    port: MessagePort,
    sender: RustOutboxSender,
    _on_message_closure: Closure<dyn FnMut(MessageEvent)>,
}

/// Fans the runtime's outbox out across the main-thread peer and any
/// attached follower-tab peers. Messages destined for the main peer
/// (`MAIN_CLIENT_ID`) and all server-bound traffic go to the main
/// `RustOutboxSender` (which posts on the global `DedicatedWorkerGlobalScope`
/// or hands server-bound to the Rust transport). Messages destined for any
/// other registered client id route to that peer's port-bound sender.
#[derive(Clone)]
struct MultiplexedSyncSender {
    main: RustOutboxSender,
}

impl MultiplexedSyncSender {
    fn new(main: RustOutboxSender) -> Self {
        Self { main }
    }
}

impl SyncSender for MultiplexedSyncSender {
    fn send_sync_message(&self, message: OutboxEntry) {
        let routed_sender = if let Destination::Client(client_id) = &message.destination {
            let id = client_id.0.to_string();
            let main_id = MAIN_CLIENT_ID.with(|c| c.borrow().clone());
            if main_id.as_deref() == Some(&id) {
                None
            } else {
                PORT_PEERS.with(|cell| cell.borrow().get(&id).map(|p| p.sender.clone()))
            }
        } else {
            None
        };
        match routed_sender {
            Some(sender) => sender.send_sync_message(message),
            None => self.main.send_sync_message(message),
        }
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum HostState {
    Initializing,
    Ready,
    ShuttingDown,
}

struct WorkerHost {
    state: HostState,
    /// Buffered messages that arrived before `Ready`. Drained in arrival order
    /// once init completes.
    pending_messages: VecDeque<MainToWorkerMessage>,
    /// Buffered follower-tab `MessagePort`s from `attach-tab-port` messages
    /// that arrived before the runtime was open. The leader-tab supervisor
    /// claims leadership immediately upon winning the `navigator.locks`
    /// lease — so the broker can start routing follower tabs to the leader
    /// even before the leader's main thread has called `bridge.init`. The
    /// resulting `attach-tab-port` arrivals are buffered (with their
    /// transferred ports preserved by the JS shim) and processed after
    /// `run_init` transitions to `Ready`.
    pending_ports: Vec<MessagePort>,
    on_message_closure: Option<Closure<dyn FnMut(MessageEvent)>>,
    current_auth_jwt: Option<String>,
    current_admin_secret: Option<String>,
    current_ws_url: Option<String>,
    /// Last-known upstream socket state. Maintained by `set_upstream_connected`
    /// on every connect/disconnect/auth-failure. Followers attaching later
    /// read this to gate their server-tier queries on the leader's *real*
    /// upstream rather than an optimistic one-shot signal.
    upstream_connected: bool,
}

impl WorkerHost {
    fn new() -> Self {
        Self {
            state: HostState::Initializing,
            pending_messages: VecDeque::new(),
            pending_ports: Vec::new(),
            on_message_closure: None,
            current_auth_jwt: None,
            current_admin_secret: None,
            current_ws_url: None,
            upstream_connected: false,
        }
    }
}

// =============================================================================
// Public entry point
// =============================================================================

#[wasm_bindgen(js_name = runAsWorker)]
pub fn run_as_worker(init_message: JsValue, pending_messages: Array) -> Result<(), JsError> {
    if HOST.with(|h| h.borrow().is_some()) {
        return Ok(());
    }

    // Parse init synchronously.
    let init = match parse_main_to_worker(&init_message) {
        Ok(MainToWorkerMessage::Init(payload)) => payload,
        Ok(other) => {
            post_to_main(&WorkerToMainWire::Error {
                message: format!(
                    "first message must be `init`, got {}",
                    describe_main_message(&other)
                ),
            });
            return Ok(());
        }
        Err(e) => {
            post_to_main(&WorkerToMainWire::Error {
                message: format!("init parse error: {e}"),
            });
            return Ok(());
        }
    };

    let mut host = WorkerHost::new();

    // Drain JS-side pending bag: parse each, buffer ALL message types in
    // arrival order. Drop only Init duplicates (post error per spec).
    //
    // Each entry is `{ data, ports }` (see the JS shim). `attach-tab-port`
    // messages carry a transferred `MessagePort` via `ports[0]`, so we have
    // to look at the wrapper rather than parsing `data` directly with
    // `parse_main_to_worker` (which only knows postcard envelopes and the
    // `init` JS-object). For everything else we still parse `data` the same
    // way the post-init `onmessage` handler does.
    for entry in pending_messages.iter() {
        let (data, ports) = unpack_pending_entry(&entry);

        if is_attach_tab_port_message(&data) {
            match extract_first_port(&ports) {
                Some(port) => host.pending_ports.push(port),
                None => {
                    tracing::warn!("attach-tab-port arrived without a MessagePort");
                }
            }
            continue;
        }

        match parse_main_to_worker(&data) {
            Ok(MainToWorkerMessage::Init(_)) => {
                tracing::warn!("ignoring duplicate init in pending pre-bootstrap messages");
                post_to_main(&WorkerToMainWire::Error {
                    message: "ignoring duplicate init".to_string(),
                });
            }
            Ok(parsed) => host.pending_messages.push_back(parsed),
            Err(e) => tracing::warn!("malformed pending message during bootstrap: {e}"),
        }
    }

    // Install Rust onmessage. Subsequent messages during init also buffer here.
    let global = global_worker_scope();
    let on_message = Closure::<dyn FnMut(MessageEvent)>::new(move |event: MessageEvent| {
        // Out-of-band `attach-tab-port` carries a `MessagePort` via the
        // event's `ports` list and uses a plain JS object (`{type:
        // "attach-tab-port"}`) as `data`. Recognise it here rather than
        // through `parse_main_to_worker` so we don't have to teach the
        // postcard wire format about port-transfer plumbing.
        if is_attach_tab_port_message(&event.data()) {
            let ports = event.ports();
            if ports.length() == 0 {
                tracing::warn!("attach-tab-port arrived without a MessagePort");
                return;
            }
            let port_value = ports.get(0);
            match port_value.dyn_into::<MessagePort>() {
                Ok(port) => handle_attach_tab_port(port),
                Err(value) => {
                    tracing::warn!(
                        "attach-tab-port: event.ports[0] is not a MessagePort: {value:?}"
                    );
                }
            }
            return;
        }

        let data = event.data();
        match parse_main_to_worker(&data) {
            Ok(msg) => handle_main_message(msg),
            Err(e) => post_to_main(&WorkerToMainWire::Error {
                message: format!("malformed worker message: {e}"),
            }),
        }
    });
    global.set_onmessage(Some(on_message.as_ref().unchecked_ref()));
    host.on_message_closure = Some(on_message);

    HOST.with(|cell| *cell.borrow_mut() = Some(host));

    // Spawn async runtime open + init.
    wasm_bindgen_futures::spawn_local(async move {
        if let Err(e) = run_init(*init).await {
            post_to_main(&WorkerToMainWire::Error {
                message: format!("Init failed: {e}"),
            });
        }
    });

    Ok(())
}

fn describe_main_message(msg: &MainToWorkerMessage) -> &'static str {
    match msg {
        MainToWorkerMessage::Init(_) => "init",
        MainToWorkerMessage::Unknown(_) => "<unknown>",
        MainToWorkerMessage::Wire(wire) => match wire {
            MainToWorkerWire::Sync { .. } => "sync",
            MainToWorkerWire::LifecycleHint { .. } => "lifecycle-hint",
            MainToWorkerWire::UpdateAuth { .. } => "update-auth",
            MainToWorkerWire::DisconnectUpstream => "disconnect-upstream",
            MainToWorkerWire::ReconnectUpstream => "reconnect-upstream",
            MainToWorkerWire::Shutdown => "shutdown",
            MainToWorkerWire::AcknowledgeRejectedBatch { .. } => "acknowledge-rejected-batch",
            MainToWorkerWire::SimulateCrash => "simulate-crash",
            MainToWorkerWire::DebugSchemaState => "debug-schema-state",
            MainToWorkerWire::DebugSeedLiveSchema { .. } => "debug-seed-live-schema",
        },
    }
}

// =============================================================================
// Async init flow
// =============================================================================

async fn run_init(init: InitPayload) -> Result<(), String> {
    let f = &init.fields;

    // 1. Open runtime.
    let runtime = match WasmRuntime::open_persistent(
        &f.schema_json,
        &f.app_id,
        &f.env,
        &f.user_branch,
        &f.db_name,
        Some("local".to_string()),
        false,
    )
    .await
    {
        Ok(rt) => rt,
        Err(err) => {
            if is_security_error(&err) {
                tracing::warn!("OPFS unavailable (SecurityError) — falling back to ephemeral");
                WasmRuntime::open_ephemeral(
                    &f.schema_json,
                    &f.app_id,
                    &f.env,
                    &f.user_branch,
                    &f.db_name,
                    Some("local".to_string()),
                    false,
                )
                .map_err(|e| format!("ephemeral open: {e:?}"))?
            } else {
                return Err(format!("persistent open: {}", js_error_message(&err)));
            }
        }
    };

    // 2. Register main thread as a peer client.
    let main_client_id = runtime.add_client();
    runtime
        .set_client_role(&main_client_id, "peer")
        .map_err(|e| format!("setClientRole: {e:?}"))?;

    // Auth-failure callback.
    let auth_cb = Closure::<dyn FnMut(JsValue)>::new(|reason: JsValue| {
        let raw = reason.as_string().unwrap_or_default();
        set_upstream_connected(false);
        post_to_main(&WorkerToMainWire::AuthFailed {
            reason: map_auth_reason(&raw).to_string(),
        });
    })
    .into_js_value();
    runtime.on_auth_failure(auth_cb.unchecked_into());

    // 3. Stash runtime + main client id atomically.
    let runtime_rc = Rc::new(runtime);
    RUNTIME.with(|cell| *cell.borrow_mut() = Some(Rc::clone(&runtime_rc)));
    MAIN_CLIENT_ID.with(|cell| *cell.borrow_mut() = Some(main_client_id.clone()));

    // 4. Construct the main-thread outbox sender, then wrap it in a
    //    `MultiplexedSyncSender` that fans the runtime's outbox out across
    //    the main-thread peer and any follower-tab peers attached later via
    //    `attach-tab-port`. Binary encoding is required on both layers (the
    //    bridge decodes via `parse_worker_to_main`).
    let sender = RustOutboxSender::new(true);
    let global: JsValue = global_worker_scope().into();
    sender.attach_target(global, Some(main_client_id.clone()), None);
    let multiplexed = MultiplexedSyncSender::new(sender.clone());
    runtime_rc
        .core
        .borrow_mut()
        .set_sync_sender(Box::new(multiplexed));

    // 4b. Replay only mutation errors buffered from persistent storage. Live
    //     worker rejections travel to main through normal sync `BatchFate`
    //     payloads so the main runtime owns delivery and acknowledgement.
    replay_startup_mutation_errors(&runtime_rc);

    // 5. Bootstrap catalogue (addServer/removeServer dance forwards catalogue
    //    state to main via the outbox sender's bootstrap-forwarding flag).
    //    Must run BEFORE upstream connect — once a transport handle is
    //    installed, server-bound outbox traffic routes there and bypasses
    //    the bootstrap-catalogue forwarder.
    sender.set_bootstrap_catalogue_forwarding(true);
    let _ = runtime_rc.add_server(None, None);
    runtime_rc.remove_server();
    sender.set_bootstrap_catalogue_forwarding(false);

    // 6. Connect upstream BEFORE draining pending sync. Drained main writes
    //    park into the inbox and process on the next batched_tick (microtask).
    //    By that time the transport handle is installed, so any server-bound
    //    traffic generated by processing them routes via the transport rather
    //    than into the (now-closed) bootstrap-catalogue forwarder.
    if let Some(server_url) = &init.fields.server_url {
        let mut auth = serde_json::Map::new();
        if let Some(secret) = &init.fields.admin_secret {
            auth.insert(
                "admin_secret".to_string(),
                serde_json::Value::String(secret.clone()),
            );
            HOST.with(|cell| {
                if let Some(h) = cell.borrow_mut().as_mut() {
                    h.current_admin_secret = Some(secret.clone());
                }
            });
        }
        if let Some(jwt) = &init.fields.jwt_token {
            auth.insert(
                "jwt_token".to_string(),
                serde_json::Value::String(jwt.clone()),
            );
            HOST.with(|cell| {
                if let Some(h) = cell.borrow_mut().as_mut() {
                    h.current_auth_jwt = Some(jwt.clone());
                }
            });
        }
        let auth_json = serde_json::to_string(&auth).unwrap_or_else(|_| "{}".to_string());
        let ws_url = http_url_to_ws(server_url, &init.fields.app_id);
        HOST.with(|cell| {
            if let Some(h) = cell.borrow_mut().as_mut() {
                h.current_ws_url = Some(ws_url.clone());
            }
        });
        perform_upstream_connect(&runtime_rc, &ws_url, &auth_json);
    }

    // 7. Sync retained local batch records to main. Rejected-batch replay
    //    is driven by the runtime's `on_mutation_error` callback (registered
    //    in step 4b), which fires for any events that were buffered from
    //    persistent storage replay.
    sync_retained_local_batch_records(&runtime_rc);

    // 8. Flip state to Ready before draining (so message handlers process
    //    directly via the dispatch path rather than re-buffering).
    HOST.with(|cell| {
        if let Some(h) = cell.borrow_mut().as_mut() {
            h.state = HostState::Ready;
        }
    });

    // 9. Drain pending messages in arrival order. Sync parks into the
    //    runtime; control messages dispatch immediately. Parked messages
    //    process on the next microtask via batched_tick.
    drain_pending_messages();

    // 9b. Drain pre-Ready follower-tab `MessagePort`s. The leader-tab
    //     supervisor claims leadership as soon as it spawns the dedicated
    //     worker, so the broker may have already routed follower tabs to us
    //     via `attach-tab-port` messages that the JS shim buffered in
    //     `pending_messages`. We extracted their `MessagePort`s above into
    //     `host.pending_ports`; now that the runtime is open and Ready, we
    //     can finally call `handle_attach_tab_port` for each.
    drain_pending_ports();

    // 10. If a buffered `Shutdown` was drained, `handle_shutdown` already
    //     posted `ShutdownOk`, called `global.close()`, and cleared `HOST`.
    //     Don't post `InitOk` to a worker that is already closing — main
    //     has defenses (it clears `worker.onmessage` after `ShutdownOk` and
    //     `transition_init_ok` gates on `state == Initializing`), but the
    //     cleanest fix is to bail at the source.
    if HOST.with(|c| c.borrow().is_none()) {
        return Ok(());
    }

    // 11. Post init-ok last so main can rely on Ready being persistent by
    //     the time it dispatches subsequent traffic.
    post_to_main(&WorkerToMainWire::InitOk {
        client_id: main_client_id.clone(),
    });

    Ok(())
}

/// Drain `host.pending_ports`, calling `handle_attach_tab_port` for each.
/// Called once after the host transitions to `Ready` so follower-tab port
/// adoptions buffered during bootstrap can finally register their peers
/// against the now-open runtime.
fn drain_pending_ports() {
    let ports = HOST.with(|cell| {
        cell.borrow_mut()
            .as_mut()
            .map(|h| std::mem::take(&mut h.pending_ports))
            .unwrap_or_default()
    });
    for port in ports {
        handle_attach_tab_port(port);
    }
}

/// Unwrap a JS-side `{ data, ports }` entry produced by the worker shim's
/// `pendingMessages.push({ data, ports: [...event.ports] })`. The JS shim
/// adopted this wrapper shape so that `attach-tab-port` messages arriving
/// before `runAsWorker` is invoked still carry their transferred
/// `MessagePort`s — `event.data` alone loses them. Returns the inner data
/// payload plus the (possibly empty) ports list.
fn unpack_pending_entry(entry: &JsValue) -> (JsValue, Array) {
    // Defensive: if the JS side ever regresses and pushes raw data (no
    // wrapper), fall back to treating the entry as the data with no ports.
    let data = Reflect::get(entry, &JsValue::from_str("data")).ok();
    let ports = Reflect::get(entry, &JsValue::from_str("ports"))
        .ok()
        .and_then(|v| v.dyn_into::<Array>().ok());
    match (data, ports) {
        (Some(data), Some(ports)) if !data.is_undefined() => (data, ports),
        _ => (entry.clone(), Array::new()),
    }
}

fn extract_first_port(ports: &Array) -> Option<MessagePort> {
    if ports.length() == 0 {
        return None;
    }
    ports.get(0).dyn_into::<MessagePort>().ok()
}

fn drain_pending_messages() {
    loop {
        let next = HOST.with(|cell| {
            cell.borrow_mut()
                .as_mut()
                .and_then(|h| h.pending_messages.pop_front())
        });
        match next {
            Some(msg) => process_main_message(msg),
            None => break,
        }
    }
}

fn perform_upstream_connect(runtime: &Rc<WasmRuntime>, ws_url: &str, auth_json: &str) {
    match runtime.connect(ws_url.to_string(), auth_json.to_string()) {
        Ok(()) => set_upstream_connected(true),
        Err(err) => {
            tracing::error!("runtime.connect failed: {:?}", err);
            set_upstream_connected(false);
        }
    }
}

/// Record an upstream connect/disconnect transition and fan it out to every
/// consumer of the leader's socket state: the leader's own main thread *and*
/// every adopted follower-tab port. Followers gate
/// `waitForUpstreamServerConnection` on this signal, so a transition posted
/// only to the main thread would strand them on a stale view of the socket.
fn set_upstream_connected(connected: bool) {
    HOST.with(|cell| {
        if let Some(h) = cell.borrow_mut().as_mut() {
            h.upstream_connected = connected;
        }
    });
    let wire = if connected {
        WorkerToMainWire::UpstreamConnected
    } else {
        WorkerToMainWire::UpstreamDisconnected
    };
    post_to_main(&wire);
    broadcast_to_port_peers(&wire);
}

/// The wire message reflecting the leader's current upstream socket state.
/// Sent to a follower on `Init` so its bridge starts from the real state.
fn current_upstream_wire() -> WorkerToMainWire {
    let connected = HOST.with(|cell| {
        cell.borrow()
            .as_ref()
            .map(|h| h.upstream_connected)
            .unwrap_or(false)
    });
    if connected {
        WorkerToMainWire::UpstreamConnected
    } else {
        WorkerToMainWire::UpstreamDisconnected
    }
}

/// Post a message to every adopted follower-tab port.
fn broadcast_to_port_peers(msg: &WorkerToMainWire) {
    PORT_PEERS.with(|cell| {
        for peer in cell.borrow().values() {
            post_via_port(&peer.port, msg);
        }
    });
}

// =============================================================================
// Mutation-error replay
// =============================================================================

fn sync_retained_local_batch_records(runtime: &Rc<WasmRuntime>) {
    match retained_local_batch_records_payload(runtime) {
        Ok(encoded_records) => {
            post_to_main(&WorkerToMainWire::LocalBatchRecordsSync { encoded_records })
        }
        Err(err) => tracing::warn!("load retained local batch records failed: {err:?}"),
    }
}

fn retained_local_batch_records_payload(runtime: &Rc<WasmRuntime>) -> Result<Vec<ByteBuf>, String> {
    let records = runtime
        .core
        .borrow()
        .local_batch_records_for_worker_sync()
        .map_err(|err| format!("{err:?}"))?;
    let mut encoded_records = Vec::with_capacity(records.len());
    for record in &records {
        match record.encode_storage_row() {
            Ok(row) => encoded_records.push(ByteBuf::from(row)),
            Err(err) => tracing::warn!("encode local batch record for sync: {err:?}"),
        }
    }
    Ok(encoded_records)
}

/// Drain mutation errors restored from persistent storage on startup and post
/// them to main as `MutationErrorReplay`. This is intentionally one-shot:
/// live worker rejections must reach main through sync `BatchFate` payloads.
fn replay_startup_mutation_errors(runtime: &Rc<WasmRuntime>) {
    for event in runtime.drain_pending_mutation_error_events() {
        let batch_id = event.batch.batch_id.to_string();
        post_to_main(&WorkerToMainWire::MutationErrorReplay {
            batch_id: batch_id.clone(),
            code: event.code,
            reason: event.reason,
        });
        if let Err(err) = runtime.acknowledge_rejected_batch(&batch_id) {
            tracing::warn!("acknowledge startup mutation error replay: {err:?}");
        }
    }
}

// =============================================================================
// Message dispatch
// =============================================================================

/// Per-message entry. Buffers everything pre-Ready and dispatches once Ready.
fn handle_main_message(msg: MainToWorkerMessage) {
    // Init at any time other than during the initial bootstrap is a programming
    // error — post error and ignore (spec).
    if matches!(msg, MainToWorkerMessage::Init(_)) {
        post_to_main(&WorkerToMainWire::Error {
            message: "ignoring duplicate init".to_string(),
        });
        return;
    }

    let state = HOST.with(|c| c.borrow().as_ref().map(|h| h.state));
    match state {
        Some(HostState::Initializing) => {
            HOST.with(|c| {
                if let Some(h) = c.borrow_mut().as_mut() {
                    h.pending_messages.push_back(msg);
                }
            });
        }
        Some(HostState::Ready) => process_main_message(msg),
        // ShuttingDown / None: silently drop.
        _ => {}
    }
}

/// Post-Ready dispatch. Assumes runtime is open.
fn process_main_message(msg: MainToWorkerMessage) {
    let runtime = RUNTIME.with(|cell| cell.borrow().clone());

    let wire = match msg {
        MainToWorkerMessage::Init(_) => {
            post_to_main(&WorkerToMainWire::Error {
                message: "ignoring duplicate init".to_string(),
            });
            return;
        }
        MainToWorkerMessage::Unknown(t) => {
            tracing::warn!("ignoring unknown worker message type {t}");
            return;
        }
        MainToWorkerMessage::Wire(wire) => wire,
    };

    match wire {
        MainToWorkerWire::Sync { payloads } => {
            let Some(rt) = runtime.as_ref() else { return };
            let Some(main_client_id) = get_main_client_id() else {
                return;
            };
            for payload in payloads {
                let arr = Uint8Array::from(payload.as_ref());
                if let Err(err) =
                    rt.on_sync_message_received_from_client(&main_client_id, arr.into())
                {
                    tracing::warn!("onSyncMessageReceivedFromClient main: {err:?}");
                }
            }
            rt.batched_tick();
        }
        MainToWorkerWire::LifecycleHint { event, .. } => {
            handle_lifecycle_hint(event, runtime.as_ref());
        }
        MainToWorkerWire::UpdateAuth { jwt_token } => {
            update_auth(jwt_token, runtime.as_ref());
        }
        MainToWorkerWire::DisconnectUpstream => {
            if let Some(rt) = runtime.as_ref() {
                rt.disconnect();
                set_upstream_connected(false);
            }
        }
        MainToWorkerWire::ReconnectUpstream => {
            if let Some(rt) = runtime.as_ref() {
                let (ws_url, auth_json) = build_reconnect_auth();
                if let Some(url) = ws_url {
                    perform_upstream_connect(rt, &url, &auth_json);
                }
            }
        }
        MainToWorkerWire::Shutdown => handle_shutdown(runtime.as_ref(), false),
        MainToWorkerWire::SimulateCrash => handle_shutdown(runtime.as_ref(), true),
        MainToWorkerWire::AcknowledgeRejectedBatch { batch_id } => {
            if let Some(rt) = runtime.as_ref() {
                if let Err(err) = rt.acknowledge_rejected_batch(&batch_id) {
                    tracing::warn!("acknowledgeRejectedBatch: {err:?}");
                }
            }
        }
        MainToWorkerWire::DebugSchemaState => match runtime.as_ref() {
            Some(rt) => match rt.debug_schema_state() {
                Ok(state_value) => post_to_main(&WorkerToMainWire::DebugSchemaStateOk {
                    state_json: js_value_to_json(&state_value),
                }),
                Err(err) => post_to_main(&WorkerToMainWire::Error {
                    message: format!(
                        "debug-schema-state failed: {}",
                        js_error_message(&err.into())
                    ),
                }),
            },
            None => {
                post_to_main(&WorkerToMainWire::Error {
                    message: "debug-schema-state requested before worker init complete".to_string(),
                });
            }
        },
        MainToWorkerWire::DebugSeedLiveSchema { schema_json } => match runtime.as_ref() {
            Some(rt) => match rt.debug_seed_live_schema(&schema_json) {
                Ok(()) => {
                    rt.flush_wal();
                    post_to_main(&WorkerToMainWire::DebugSeedLiveSchemaOk);
                }
                Err(err) => post_to_main(&WorkerToMainWire::Error {
                    message: format!(
                        "debug-seed-live-schema failed: {}",
                        js_error_message(&err.into())
                    ),
                }),
            },
            None => {
                post_to_main(&WorkerToMainWire::Error {
                    message: "debug-seed-live-schema requested before worker init complete"
                        .to_string(),
                });
            }
        },
    }
}

fn build_reconnect_auth() -> (Option<String>, String) {
    HOST.with(|cell| {
        let guard = cell.borrow();
        let host = guard.as_ref();
        let url = host.and_then(|h| h.current_ws_url.clone());
        let mut auth = serde_json::Map::new();
        if let Some(host) = host {
            if let Some(secret) = &host.current_admin_secret {
                auth.insert(
                    "admin_secret".to_string(),
                    serde_json::Value::String(secret.clone()),
                );
            }
            if let Some(jwt) = &host.current_auth_jwt {
                auth.insert(
                    "jwt_token".to_string(),
                    serde_json::Value::String(jwt.clone()),
                );
            }
        }
        let json = serde_json::to_string(&auth).unwrap_or_else(|_| "{}".to_string());
        (url, json)
    })
}

fn handle_lifecycle_hint(event: WorkerLifecycleEvent, runtime: Option<&Rc<WasmRuntime>>) {
    match event {
        WorkerLifecycleEvent::VisibilityHidden
        | WorkerLifecycleEvent::Pagehide
        | WorkerLifecycleEvent::Freeze => {
            if let Some(rt) = runtime {
                rt.flush_wal();
            }
        }
        _ => {}
    }
}

fn update_auth(jwt: Option<String>, runtime: Option<&Rc<WasmRuntime>>) {
    HOST.with(|cell| {
        if let Some(h) = cell.borrow_mut().as_mut() {
            h.current_auth_jwt = jwt;
        }
    });
    let Some(rt) = runtime else { return };
    let mut auth = serde_json::Map::new();
    let (jwt, secret) = HOST.with(|cell| {
        let g = cell.borrow();
        let h = g.as_ref();
        (
            h.and_then(|h| h.current_auth_jwt.clone()),
            h.and_then(|h| h.current_admin_secret.clone()),
        )
    });
    if let Some(jwt) = jwt {
        auth.insert("jwt_token".to_string(), serde_json::Value::String(jwt));
    }
    if let Some(secret) = secret {
        auth.insert(
            "admin_secret".to_string(),
            serde_json::Value::String(secret),
        );
    }
    let json = serde_json::to_string(&auth).unwrap_or_else(|_| "{}".to_string());
    if let Err(err) = rt.update_auth(json) {
        tracing::error!("runtime.updateAuth failed: {err:?}");
        post_to_main(&WorkerToMainWire::AuthFailed {
            reason: "invalid".to_string(),
        });
    }
}

fn handle_shutdown(runtime: Option<&Rc<WasmRuntime>>, _simulate_crash: bool) {
    HOST.with(|cell| {
        if let Some(h) = cell.borrow_mut().as_mut() {
            h.state = HostState::ShuttingDown;
        }
    });

    if let Some(rt) = runtime {
        // Drain any parked main/peer sync messages so their writes reach
        // storage, then flush WAL so they survive a remount/replay. Without
        // this, pending entries delivered just before `Shutdown` /
        // `SimulateCrash` (e.g. a wait-then-crash sequence) get dropped
        // because the scheduled `batched_tick` (setTimeout(0)) sits behind
        // the control macrotask in the worker queue.
        //
        // `simulate_crash` keeps the same drain step. On opfs-btree
        // `flush_wal` is the only durability primitive (snapshot == WAL
        // checkpoint), so the crash flavour and the clean shutdown have
        // the same effect on storage; the distinction is preserved in case
        // a future storage backend introduces a separate snapshot path.
        rt.batched_tick();
        rt.flush_wal();
        rt.install_noop_sync_sender();
        // (No forwarder on worker side — `install_noop_sync_sender` below
        // replaces the active sender wholesale, so any future outbox emission
        // is dropped silently.)
    }

    // Clear self.onmessage explicitly. `Closure::drop` invalidates the call
    // but does not clear the JS slot — a late inbound would invoke a freed
    // trampoline.
    let global = global_worker_scope();
    global.set_onmessage(None);

    // Detach every adopted follower port so their (now-stale) closures stop
    // firing, then drop the entries.
    PORT_PEERS.with(|cell| {
        let mut map = cell.borrow_mut();
        for (_, peer) in map.drain() {
            peer.port.set_onmessage(None);
        }
    });

    RUNTIME.with(|cell| *cell.borrow_mut() = None);
    MAIN_CLIENT_ID.with(|cell| *cell.borrow_mut() = None);

    post_to_main(&WorkerToMainWire::ShutdownOk);
    global.close();
    HOST.with(|cell| *cell.borrow_mut() = None);
}

// =============================================================================
// Follower-tab port adoption
// =============================================================================

fn is_attach_tab_port_message(data: &JsValue) -> bool {
    if !data.is_object() {
        return false;
    }
    let Ok(t) = Reflect::get(data, &"type".into()) else {
        return false;
    };
    t.as_string().as_deref() == Some("attach-tab-port")
}

/// Adopt a `MessagePort` handed to us by the leader-tab supervisor via
/// `{type: "attach-tab-port"}`. Allocates a fresh runtime client id for the
/// follower behind this port, registers a per-port `RustOutboxSender` so the
/// runtime's outbox routes correctly, and installs an `onmessage` handler on
/// the port that parses each incoming message as `MainToWorkerMessage` and
/// dispatches to {@link handle_port_message}.
///
/// During bootstrap (`HostState::Initializing` — Rust owns `onmessage` but
/// `run_init` hasn't reached `Ready` yet) the port is buffered into
/// `host.pending_ports` instead of being adopted directly: the runtime isn't
/// open yet, so `runtime.add_client` would fail. The supervisor now claims
/// leadership at the broker as soon as the dedicated worker is spawned, so
/// this window is reachable in normal multi-tab use — without the buffer the
/// follower's `MessagePort` would be silently dropped and its bridge would
/// hang waiting for `init-ok`. After `run_init` flips to `Ready`,
/// `drain_pending_ports()` re-invokes this function for every buffered port.
fn handle_attach_tab_port(port: MessagePort) {
    let state = HOST.with(|c| c.borrow().as_ref().map(|h| h.state));
    match state {
        Some(HostState::Ready) => {}
        Some(HostState::Initializing) => {
            HOST.with(|cell| {
                if let Some(h) = cell.borrow_mut().as_mut() {
                    h.pending_ports.push(port);
                }
            });
            return;
        }
        _ => {
            tracing::warn!(
                "attach-tab-port arrived in unexpected host state ({:?}); ignoring",
                state,
            );
            return;
        }
    }

    let runtime = match RUNTIME.with(|c| c.borrow().clone()) {
        Some(rt) => rt,
        None => {
            tracing::warn!("attach-tab-port: no runtime available; ignoring");
            return;
        }
    };

    // Allocate the follower id in the host, not inside `runtime.add_client`.
    // `add_client` queues storage-backed catalogue replay and schedules a
    // tick, so the multiplexer must already know how to route this id before
    // the runtime sees the client.
    let client_id = ClientId::new();
    let client_id_string = client_id.0.to_string();

    // Per-port outbox sender. Configured so its `main_client_id == client_id`
    // — that way `RustOutboxSender::send_sync_message` only enqueues
    // messages destined for this peer, and the multiplexer's routing is the
    // only filter that needs to be correct.
    let sender = RustOutboxSender::new(true);
    let port_js: JsValue = port.clone().into();
    sender.attach_target(port_js, Some(client_id_string.clone()), None);

    // Install per-port message handler.
    let port_for_closure = port.clone();
    let client_id_for_closure = client_id_string.clone();
    let on_message = Closure::<dyn FnMut(MessageEvent)>::new(move |event: MessageEvent| {
        let data = event.data();
        match parse_main_to_worker(&data) {
            Ok(msg) => handle_port_message(&client_id_for_closure, &port_for_closure, msg),
            Err(e) => {
                tracing::warn!("malformed port message on follower tab: {e}");
            }
        }
    });
    port.set_onmessage(Some(on_message.as_ref().unchecked_ref()));
    port.start();

    PORT_PEERS.with(|cell| {
        cell.borrow_mut().insert(
            client_id_string.clone(),
            PortPeer {
                port: port.clone(),
                sender,
                _on_message_closure: on_message,
            },
        );
    });

    runtime.add_client_with_id(client_id);
    if let Err(err) = runtime.set_client_role(&client_id_string, "peer") {
        tracing::warn!("attach-tab-port: setClientRole failed: {err:?}");
        deregister_port_peer(&client_id_string);
    }
}

/// Dispatch one parsed `MainToWorkerMessage` arriving from a follower tab's
/// `MessagePort`. The follower's `WorkerBridge` speaks the same protocol as
/// the leader's main-thread bridge, but its `Init`, auth, and upstream
/// control messages have follower-only semantics:
///
/// - `Init`: respond with `InitOk { client_id }`, the leader's *current*
///   upstream-connection state, and an empty `LocalBatchRecordsSync`. The
///   follower gates server-tier reads on that upstream signal, so it must
///   reflect the leader's real socket — not an optimistic constant. The
///   follower's serverUrl / jwtToken / runtimeSources fields are ignored —
///   upstream and auth are owned by the leader.
/// - `Sync`: route into the runtime addressed as this port's `client_id`.
/// - `LifecycleHint`: forward to the leader's runtime so it can aggregate.
/// - `UpdateAuth`: forward to the leader-owned auth/transport state so a
///   login or token refresh on a follower tab updates the upstream socket's
///   credentials. The follower's `Db` calls `workerBridge.updateAuth(...)`
///   for every tab; without this forward, follower-side refreshes would
///   silently keep the leader on stale credentials.
/// - `DisconnectUpstream` / `ReconnectUpstream` / `Shutdown` (a follower
///   closing its bridge): drop the port peer on `Shutdown`; ignore the
///   others (upstream control is leader-only).
fn handle_port_message(client_id: &str, port: &MessagePort, msg: MainToWorkerMessage) {
    match msg {
        MainToWorkerMessage::Init(_) => {
            post_via_port(
                port,
                &WorkerToMainWire::InitOk {
                    client_id: client_id.to_string(),
                },
            );
            // The leader's *real* upstream state — not an unconditional
            // `UpstreamConnected`. The follower's bridge gates server-tier
            // reads on this; an optimistic constant would let it read
            // against a leader whose socket isn't actually up.
            post_via_port(port, &current_upstream_wire());
            post_via_port(
                port,
                &WorkerToMainWire::LocalBatchRecordsSync {
                    encoded_records: Vec::new(),
                },
            );
        }
        MainToWorkerMessage::Unknown(t) => {
            tracing::warn!("ignoring unknown port message type {t}");
        }
        MainToWorkerMessage::Wire(wire) => match wire {
            MainToWorkerWire::Sync { payloads } => {
                let Some(rt) = RUNTIME.with(|cell| cell.borrow().clone()) else {
                    return;
                };
                for payload in payloads {
                    let arr = Uint8Array::from(payload.as_ref());
                    if let Err(err) = rt.on_sync_message_received_from_client(client_id, arr.into())
                    {
                        tracing::warn!("port onSyncMessageReceivedFromClient: {err:?}");
                    }
                }
                rt.batched_tick();
            }
            MainToWorkerWire::LifecycleHint { event, .. } => {
                let runtime = RUNTIME.with(|cell| cell.borrow().clone());
                handle_lifecycle_hint(event, runtime.as_ref());
            }
            MainToWorkerWire::AcknowledgeRejectedBatch { batch_id } => {
                if let Some(rt) = RUNTIME.with(|cell| cell.borrow().clone()) {
                    if let Err(err) = rt.acknowledge_rejected_batch(&batch_id) {
                        tracing::warn!("port acknowledgeRejectedBatch: {err:?}");
                    }
                }
            }
            MainToWorkerWire::Shutdown => {
                deregister_port_peer(client_id);
                post_via_port(port, &WorkerToMainWire::ShutdownOk);
            }
            // Auth is leader-owned. The follower's `Db` still calls
            // `workerBridge.updateAuth(...)` per-tab on login/refresh, and
            // we forward it into the leader-owned credential state so the
            // upstream socket gets refreshed credentials.
            MainToWorkerWire::UpdateAuth { jwt_token } => {
                let runtime = RUNTIME.with(|cell| cell.borrow().clone());
                update_auth(jwt_token, runtime.as_ref());
            }
            // Upstream control and debug surfaces are leader-only.
            MainToWorkerWire::DisconnectUpstream
            | MainToWorkerWire::ReconnectUpstream
            | MainToWorkerWire::SimulateCrash
            | MainToWorkerWire::DebugSchemaState
            | MainToWorkerWire::DebugSeedLiveSchema { .. } => {
                tracing::debug!("ignoring follower-side control message");
            }
        },
    }
}

fn deregister_port_peer(client_id: &str) {
    let removed = PORT_PEERS.with(|cell| cell.borrow_mut().remove(client_id));
    if let Some(peer) = removed {
        // Clear the port's onmessage slot so the (now-dropped) closure is not
        // invoked again, then drop the peer (which drops the closure handle).
        peer.port.set_onmessage(None);
    }
}

fn post_via_port(port: &MessagePort, msg: &WorkerToMainWire) {
    let Ok((value, transfer)) = worker_to_main_post(msg) else {
        return;
    };
    let _ = port.post_message_with_transferable(&value, &transfer);
}

// =============================================================================
// Helpers
// =============================================================================

fn get_main_client_id() -> Option<String> {
    MAIN_CLIENT_ID.with(|cell| cell.borrow().clone())
}

fn global_worker_scope() -> DedicatedWorkerGlobalScope {
    js_sys::global()
        .dyn_into::<DedicatedWorkerGlobalScope>()
        .expect("worker host expects a DedicatedWorkerGlobalScope")
}

fn post_to_main(msg: &WorkerToMainWire) {
    let Ok((value, transfer)) = worker_to_main_post(msg) else {
        return;
    };
    let global = global_worker_scope();
    let _ = global.post_message_with_transfer(&value, transfer.as_ref());
}

/// Serialise a JS-shaped `JsValue` to JSON. Returns `"null"` on failure.
fn js_value_to_json(value: &JsValue) -> String {
    js_sys::JSON::stringify(value)
        .ok()
        .and_then(|s| s.as_string())
        .unwrap_or_else(|| "null".to_string())
}

fn is_security_error(err: &JsValue) -> bool {
    let Ok(name) = Reflect::get(err, &"name".into()) else {
        return false;
    };
    name.as_string().as_deref() == Some("SecurityError")
}

fn js_error_message(err: &JsValue) -> String {
    if let Some(s) = err.as_string() {
        return s;
    }
    if let Ok(msg) = Reflect::get(err, &"message".into()) {
        if let Some(s) = msg.as_string() {
            return s;
        }
    }
    format!("{err:?}")
}

fn map_auth_reason(reason: &str) -> &'static str {
    match reason {
        "Unauthorized" | "expired" => "expired",
        "missing" | "Missing token" => "missing",
        "disabled" | "Auth disabled" => "disabled",
        _ => "invalid",
    }
}

fn http_url_to_ws(server_url: &str, app_id: &str) -> String {
    let trimmed = server_url.trim_end_matches('/');
    let scheme = if let Some(rest) = trimmed.strip_prefix("https://") {
        ("wss://", rest)
    } else if let Some(rest) = trimmed.strip_prefix("http://") {
        ("ws://", rest)
    } else if let Some(rest) = trimmed.strip_prefix("wss://") {
        ("wss://", rest)
    } else if let Some(rest) = trimmed.strip_prefix("ws://") {
        ("ws://", rest)
    } else {
        ("ws://", trimmed)
    };
    format!("{}{}/apps/{}/ws", scheme.0, scheme.1, app_id)
}

#[cfg(test)]
mod tests {
    //! In-source tests for the worker host: the pure URL/auth helpers
    //! (`http_url_to_ws`, `map_auth_reason`) and the follower-tab `Init`
    //! handshake's upstream-state reporting (`handle_port_message`).
    use std::cell::RefCell;
    use std::rc::Rc;

    use js_sys::{Function, Reflect};
    use wasm_bindgen::prelude::*;
    use wasm_bindgen::JsCast;
    use wasm_bindgen_futures::JsFuture;
    use wasm_bindgen_test::*;
    use web_sys::{MessageChannel, MessageEvent};

    use super::{
        handle_attach_tab_port, handle_port_message, http_url_to_ws, map_auth_reason, HostState,
        MultiplexedSyncSender, WorkerHost, HOST, MAIN_CLIENT_ID, PORT_PEERS, RUNTIME,
    };
    use crate::runtime::{set_add_client_before_core_hook, RustOutboxSender, WasmRuntime};
    use crate::worker_protocol::{
        parse_worker_to_main, InitPayload, InitPayloadFields, MainToWorkerMessage,
        ParsedWorkerToMain, WorkerToMainWire,
    };

    wasm_bindgen_test_configure!(run_in_browser);

    #[wasm_bindgen_test]
    fn http_url_to_ws_normalises_https() {
        assert_eq!(
            http_url_to_ws("https://example.test", "app-1"),
            "wss://example.test/apps/app-1/ws"
        );
    }

    #[wasm_bindgen_test]
    fn http_url_to_ws_normalises_http() {
        assert_eq!(
            http_url_to_ws("http://localhost:4000", "xyz"),
            "ws://localhost:4000/apps/xyz/ws"
        );
    }

    #[wasm_bindgen_test]
    fn http_url_to_ws_passes_wss_through() {
        assert_eq!(
            http_url_to_ws("wss://relay.example", "x"),
            "wss://relay.example/apps/x/ws",
            "wss:// must NOT become wss://wss://...",
        );
    }

    #[wasm_bindgen_test]
    fn http_url_to_ws_passes_ws_through() {
        assert_eq!(
            http_url_to_ws("ws://relay.example", "x"),
            "ws://relay.example/apps/x/ws"
        );
    }

    #[wasm_bindgen_test]
    fn http_url_to_ws_strips_trailing_slash() {
        assert_eq!(
            http_url_to_ws("https://example.test/", "a"),
            "wss://example.test/apps/a/ws"
        );
        assert_eq!(
            http_url_to_ws("https://example.test///", "a"),
            "wss://example.test/apps/a/ws"
        );
    }

    #[wasm_bindgen_test]
    fn http_url_to_ws_defaults_unknown_scheme_to_ws() {
        // No recognised scheme → assume plain host:port and prefix `ws://`.
        assert_eq!(
            http_url_to_ws("example.test:4000", "a"),
            "ws://example.test:4000/apps/a/ws"
        );
    }

    #[wasm_bindgen_test]
    fn map_auth_reason_recognises_known_strings() {
        // The Rust transport currently emits these strings on auth failure.
        assert_eq!(map_auth_reason("Unauthorized"), "expired");
        assert_eq!(map_auth_reason("expired"), "expired");
        assert_eq!(map_auth_reason("missing"), "missing");
        assert_eq!(map_auth_reason("Missing token"), "missing");
        assert_eq!(map_auth_reason("disabled"), "disabled");
        assert_eq!(map_auth_reason("Auth disabled"), "disabled");
    }

    #[wasm_bindgen_test]
    fn map_auth_reason_falls_back_to_invalid() {
        // Anything not in the known set maps to `invalid` so the main
        // thread always gets one of the four `AuthFailureReason` values.
        assert_eq!(map_auth_reason(""), "invalid");
        assert_eq!(map_auth_reason("totally unrecognised"), "invalid");
        assert_eq!(map_auth_reason("Unauthorized "), "invalid"); // exact match only
    }

    fn fake_follower_init() -> MainToWorkerMessage {
        // `handle_port_message` matches `Init(_)` — the payload is unused, so
        // a blank one is sufficient.
        MainToWorkerMessage::Init(Box::new(InitPayload {
            fields: InitPayloadFields {
                schema_json: String::new(),
                app_id: String::new(),
                env: String::new(),
                user_branch: String::new(),
                db_name: String::new(),
                client_id: String::new(),
                server_url: None,
                jwt_token: None,
                admin_secret: None,
                fallback_wasm_url: None,
                log_level: None,
                telemetry_collector_url: None,
            },
            runtime_sources: JsValue::UNDEFINED,
        }))
    }

    fn reset_worker_host_cells() {
        PORT_PEERS.with(|cell| {
            let mut peers = cell.borrow_mut();
            for (_, peer) in peers.drain() {
                peer.port.set_onmessage(None);
            }
        });
        HOST.with(|cell| *cell.borrow_mut() = None);
        RUNTIME.with(|cell| *cell.borrow_mut() = None);
        MAIN_CLIENT_ID.with(|cell| *cell.borrow_mut() = None);
        set_add_client_before_core_hook(None);
    }

    /// Resolve after one `setTimeout(0)` so queued `MessagePort` deliveries
    /// (each its own task) get a turn before the next assertion.
    async fn macrotask_yield() {
        let promise = js_sys::Promise::new(&mut |resolve, _reject| {
            let set_timeout: Function = Reflect::get(&js_sys::global(), &"setTimeout".into())
                .unwrap()
                .unchecked_into();
            let _ = set_timeout.call2(&JsValue::NULL, &resolve, &JsValue::from_f64(0.0));
        });
        let _ = JsFuture::from(promise).await;
    }

    #[wasm_bindgen_test]
    async fn follower_attach_registers_port_before_storage_backed_client_replay() {
        // `RuntimeCore::add_client` queues storage-backed catalogue replay
        // and enters `immediate_tick`. The worker host must already have a
        // PORT_PEERS entry for the newly allocated follower id before that
        // call, otherwise replay addressed to the follower can fall through
        // to the worker's main sender and be dropped as non-main client
        // traffic.
        reset_worker_host_cells();

        let runtime = Rc::new(
            WasmRuntime::new(
                r#"{
                    "todos": {
                        "columns": [
                            {"name": "title", "column_type": {"type": "Text"}, "nullable": false},
                            {"name": "completed", "column_type": {"type": "Boolean"}, "nullable": false}
                        ]
                    }
                }"#,
                "test-app",
                "dev",
                "main",
                Some("local".to_string()),
                Some(true),
                None,
            )
            .expect("runtime"),
        );

        let main_client_id = runtime.add_client();
        let main_sender = RustOutboxSender::new(true);
        runtime
            .core
            .borrow_mut()
            .set_sync_sender(Box::new(MultiplexedSyncSender::new(main_sender)));
        MAIN_CLIENT_ID.with(|cell| *cell.borrow_mut() = Some(main_client_id));

        runtime
            .debug_seed_live_schema(
                r#"{
                    "todos": {
                        "columns": [
                            {"name": "title", "column_type": {"type": "Text"}, "nullable": false}
                        ]
                    }
                }"#,
            )
            .expect("seed historical schema");
        macrotask_yield().await;

        let mut host = WorkerHost::new();
        host.state = HostState::Ready;
        HOST.with(|cell| *cell.borrow_mut() = Some(host));
        RUNTIME.with(|cell| *cell.borrow_mut() = Some(Rc::clone(&runtime)));

        let channel = MessageChannel::new().expect("MessageChannel::new");
        let worker_side = channel.port1();
        let follower_side = channel.port2();

        let route_registered_before_add_client = Rc::new(RefCell::new(false));
        let route_registered_probe = Rc::clone(&route_registered_before_add_client);
        set_add_client_before_core_hook(Some(Box::new(move |client_id| {
            let id = client_id.0.to_string();
            *route_registered_probe.borrow_mut() =
                PORT_PEERS.with(|cell| cell.borrow().contains_key(&id));
        })));

        let received: Rc<RefCell<Vec<WorkerToMainWire>>> = Rc::new(RefCell::new(Vec::new()));
        let sink = Rc::clone(&received);
        let on_message = Closure::<dyn FnMut(MessageEvent)>::new(move |event: MessageEvent| {
            if let ParsedWorkerToMain::Wire(wire) = parse_worker_to_main(&event.data()) {
                sink.borrow_mut().push(wire);
            }
        });
        follower_side.set_onmessage(Some(on_message.as_ref().unchecked_ref()));
        follower_side.start();

        handle_attach_tab_port(worker_side);

        set_add_client_before_core_hook(None);
        assert!(
            *route_registered_before_add_client.borrow(),
            "follower PORT_PEERS route must exist before runtime.add_client queues replay",
        );

        for _ in 0..50 {
            if received.borrow().iter().any(
                |wire| matches!(wire, WorkerToMainWire::Sync { payloads } if !payloads.is_empty()),
            ) {
                break;
            }
            macrotask_yield().await;
        }

        let collected = received.borrow().clone();
        assert!(
            collected.iter().any(|wire| {
                matches!(wire, WorkerToMainWire::Sync { payloads } if !payloads.is_empty())
            }),
            "attach-time catalogue replay should be routed to the follower port, got {collected:?}",
        );

        follower_side.set_onmessage(None);
        drop(on_message);
        reset_worker_host_cells();
    }

    /// Drive `handle_port_message` with a follower `Init` against a worker
    /// host whose upstream socket is in the given state, and collect the
    /// `WorkerToMainWire` messages posted back over the follower's port.
    async fn follower_init_response(upstream_connected: bool) -> Vec<WorkerToMainWire> {
        let mut host = WorkerHost::new();
        host.state = HostState::Ready;
        host.upstream_connected = upstream_connected;
        HOST.with(|cell| *cell.borrow_mut() = Some(host));

        let channel = MessageChannel::new().expect("MessageChannel::new");
        let worker_side = channel.port1();
        let follower_side = channel.port2();

        let received: Rc<RefCell<Vec<WorkerToMainWire>>> = Rc::new(RefCell::new(Vec::new()));
        let sink = Rc::clone(&received);
        let on_message = Closure::<dyn FnMut(MessageEvent)>::new(move |event: MessageEvent| {
            if let ParsedWorkerToMain::Wire(wire) = parse_worker_to_main(&event.data()) {
                sink.borrow_mut().push(wire);
            }
        });
        follower_side.set_onmessage(Some(on_message.as_ref().unchecked_ref()));
        follower_side.start();

        // `Init` posts three messages back: InitOk, the upstream-state
        // signal, and an (empty) LocalBatchRecordsSync.
        handle_port_message("follower-1", &worker_side, fake_follower_init());

        for _ in 0..50 {
            if received.borrow().len() >= 3 {
                break;
            }
            macrotask_yield().await;
        }

        follower_side.set_onmessage(None);
        HOST.with(|cell| *cell.borrow_mut() = None);
        drop(on_message);
        let collected = received.borrow().clone();
        collected
    }

    #[wasm_bindgen_test]
    async fn follower_init_reports_upstream_disconnected_when_leader_offline() {
        // The leader's upstream socket is not connected. A follower attaching
        // now must be told `UpstreamDisconnected` so its bridge keeps
        // `waitForUpstreamServerConnection` blocked — otherwise the follower
        // serves server-tier reads against a leader with no upstream.
        let wires = follower_init_response(false).await;
        assert!(
            matches!(wires.first(), Some(WorkerToMainWire::InitOk { .. })),
            "expected InitOk first, got {wires:?}",
        );
        assert!(
            matches!(wires.get(1), Some(WorkerToMainWire::UpstreamDisconnected)),
            "follower Init while the leader's upstream is offline must report \
             UpstreamDisconnected, got {wires:?}",
        );
    }

    #[wasm_bindgen_test]
    async fn follower_init_reports_upstream_connected_when_leader_online() {
        // The leader's upstream socket is live — the follower may proceed.
        let wires = follower_init_response(true).await;
        assert!(
            matches!(wires.get(1), Some(WorkerToMainWire::UpstreamConnected)),
            "follower Init while the leader's upstream is online must report \
             UpstreamConnected, got {wires:?}",
        );
    }
}
