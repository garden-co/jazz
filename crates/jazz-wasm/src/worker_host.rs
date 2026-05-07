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
//! - `HOST`         — state machine + pending queue + closures
//! - `RUNTIME`      — `Rc<WasmRuntime>` (cloned into outbox callbacks)
//! - `PEER_ROUTING` — peer table, looked up by the outbox sender on each
//!                    client-bound entry. Different cell than `HOST`, so the
//!                    outbox lookup does not re-borrow `HOST` while `HOST` is
//!                    already borrowed elsewhere.

#![cfg(target_arch = "wasm32")]
#![allow(dead_code)]

use std::cell::RefCell;
use std::collections::{HashMap, VecDeque};
use std::rc::Rc;

use js_sys::{Array, Function, Object, Reflect, Uint8Array};
use wasm_bindgen::prelude::*;
use wasm_bindgen::JsCast;
use web_sys::{DedicatedWorkerGlobalScope, MessageEvent};

use crate::runtime::WasmRuntime;
use crate::worker_protocol::{
    build_auth_failed, build_debug_schema_state_ok, build_debug_seed_live_schema_ok, build_error,
    build_init_ok, build_local_batch_records_sync, build_mutation_error_replay, build_shutdown_ok,
    parse_main_to_worker, InitPayload, MainToWorkerMessage, WorkerLifecycleEvent,
};

// =============================================================================
// Thread-local cells
// =============================================================================

thread_local! {
    static HOST: RefCell<Option<WorkerHost>> = const { RefCell::new(None) };
    static RUNTIME: RefCell<Option<Rc<WasmRuntime>>> = const { RefCell::new(None) };
    static PEER_ROUTING: RefCell<PeerRouting> = RefCell::new(PeerRouting::default());
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum HostState {
    Initializing,
    Ready,
    ShuttingDown,
}

struct PeerRouting {
    main_client_id: Option<String>,
    peer_client_by_peer_id: HashMap<String, String>,
    peer_id_by_client: HashMap<String, String>,
    peer_terms: HashMap<String, u32>,
}

impl Default for PeerRouting {
    fn default() -> Self {
        Self {
            main_client_id: None,
            peer_client_by_peer_id: HashMap::new(),
            peer_id_by_client: HashMap::new(),
            peer_terms: HashMap::new(),
        }
    }
}

struct WorkerHost {
    state: HostState,
    /// Buffered messages that arrived before `Ready`. Drained in arrival order
    /// once init completes. Includes Sync / PeerSync / control messages.
    pending_messages: VecDeque<MainToWorkerMessage>,
    on_message_closure: Option<Closure<dyn FnMut(MessageEvent)>>,
    current_auth_jwt: Option<String>,
    current_admin_secret: Option<String>,
    current_ws_url: Option<String>,
    rejected_replay_queued: bool,
}

impl WorkerHost {
    fn new() -> Self {
        Self {
            state: HostState::Initializing,
            pending_messages: VecDeque::new(),
            on_message_closure: None,
            current_auth_jwt: None,
            current_admin_secret: None,
            current_ws_url: None,
            rejected_replay_queued: false,
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
            post_to_main(&build_error(&format!(
                "first message must be `init`, got {}",
                describe_main_message(&other)
            )));
            return Ok(());
        }
        Err(e) => {
            post_to_main(&build_error(&format!("init parse error: {e}")));
            return Ok(());
        }
    };

    let mut host = WorkerHost::new();

    // Drain JS-side pending bag: parse each, buffer ALL message types in
    // arrival order. Drop only Init duplicates (post error per spec).
    for entry in pending_messages.iter() {
        match parse_main_to_worker(&entry) {
            Ok(MainToWorkerMessage::Init(_)) => {
                tracing::warn!("ignoring duplicate init in pending pre-bootstrap messages");
                post_to_main(&build_error("ignoring duplicate init"));
            }
            Ok(parsed) => host.pending_messages.push_back(parsed),
            Err(e) => tracing::warn!("malformed pending message during bootstrap: {e}"),
        }
    }

    // Install Rust onmessage. Subsequent messages during init also buffer here.
    let global = global_worker_scope();
    let on_message = Closure::<dyn FnMut(MessageEvent)>::new(move |event: MessageEvent| {
        let data = event.data();
        match parse_main_to_worker(&data) {
            Ok(msg) => handle_main_message(msg),
            Err(e) => post_to_main(&build_error(&format!("malformed worker message: {e}"))),
        }
    });
    global.set_onmessage(Some(on_message.as_ref().unchecked_ref()));
    host.on_message_closure = Some(on_message);

    HOST.with(|cell| *cell.borrow_mut() = Some(host));

    // Spawn async runtime open + init.
    wasm_bindgen_futures::spawn_local(async move {
        if let Err(e) = run_init(*init).await {
            post_to_main(&build_error(&format!("Init failed: {e}")));
        }
    });

    Ok(())
}

fn describe_main_message(msg: &MainToWorkerMessage) -> &'static str {
    match msg {
        MainToWorkerMessage::Init(_) => "init",
        MainToWorkerMessage::Sync { .. } => "sync",
        MainToWorkerMessage::PeerOpen { .. } => "peer-open",
        MainToWorkerMessage::PeerSync { .. } => "peer-sync",
        MainToWorkerMessage::PeerClose { .. } => "peer-close",
        MainToWorkerMessage::LifecycleHint { .. } => "lifecycle-hint",
        MainToWorkerMessage::UpdateAuth { .. } => "update-auth",
        MainToWorkerMessage::DisconnectUpstream => "disconnect-upstream",
        MainToWorkerMessage::ReconnectUpstream => "reconnect-upstream",
        MainToWorkerMessage::Shutdown => "shutdown",
        MainToWorkerMessage::AcknowledgeRejectedBatch { .. } => "acknowledge-rejected-batch",
        MainToWorkerMessage::SimulateCrash => "simulate-crash",
        MainToWorkerMessage::DebugSchemaState => "debug-schema-state",
        MainToWorkerMessage::DebugSeedLiveSchema { .. } => "debug-seed-live-schema",
        MainToWorkerMessage::Unknown(_) => "<unknown>",
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
        post_to_main(&crate::worker_protocol::build_upstream_disconnected());
        post_to_main(&build_auth_failed(&map_auth_reason(&raw)));
    })
    .into_js_value();
    runtime.on_auth_failure(auth_cb.unchecked_into());

    // 3. Stash runtime + main client id atomically and seed the peer table.
    let runtime_rc = Rc::new(runtime);
    RUNTIME.with(|cell| *cell.borrow_mut() = Some(Rc::clone(&runtime_rc)));
    PEER_ROUTING.with(|cell| {
        cell.borrow_mut().main_client_id = Some(main_client_id.clone());
    });

    // 4. Attach outbox target.
    let global: JsValue = global_worker_scope().into();
    let peer_lookup = make_peer_routing_lookup();
    let on_main_flushed = make_on_main_sync_flushed();
    runtime_rc.attach_outbox_target(
        global,
        Some(main_client_id.clone()),
        Some(peer_lookup),
        Some(on_main_flushed),
    );

    // 5. Bootstrap catalogue (addServer/removeServer dance forwards catalogue
    //    state to main via the outbox sender's bootstrap-forwarding flag).
    //    Must run BEFORE upstream connect — once a transport handle is
    //    installed, server-bound outbox traffic routes there and bypasses
    //    the bootstrap-catalogue forwarder.
    runtime_rc.set_bootstrap_catalogue_forwarding(true);
    let _ = runtime_rc.add_server(None, None);
    runtime_rc.remove_server();
    runtime_rc.set_bootstrap_catalogue_forwarding(false);

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

    // 7. Sync retained local batch records and queue rejected-batch replay.
    sync_retained_local_batch_records(&runtime_rc);
    queue_rejected_batch_replay();

    // 8. Flip state to Ready before draining (so message handlers process
    //    directly via the dispatch path rather than re-buffering).
    HOST.with(|cell| {
        if let Some(h) = cell.borrow_mut().as_mut() {
            h.state = HostState::Ready;
        }
    });

    // 9. Drain pending messages in arrival order. Sync/PeerSync park into the
    //    runtime; control messages dispatch immediately. Parked messages
    //    process on the next microtask via batched_tick.
    drain_pending_messages();

    // 10. Post init-ok last so main can rely on Ready being persistent by
    //     the time it dispatches subsequent traffic.
    post_to_main(&build_init_ok(&main_client_id));

    Ok(())
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
        Ok(()) => post_to_main(&crate::worker_protocol::build_upstream_connected()),
        Err(err) => {
            tracing::error!("runtime.connect failed: {:?}", err);
            post_to_main(&crate::worker_protocol::build_upstream_disconnected());
        }
    }
}

fn ensure_peer_client(runtime: &Rc<WasmRuntime>, peer_id: &str) -> Result<String, String> {
    if let Some(existing) =
        PEER_ROUTING.with(|cell| cell.borrow().peer_client_by_peer_id.get(peer_id).cloned())
    {
        return Ok(existing);
    }
    let client_id = runtime.add_client();
    runtime
        .set_client_role(&client_id, "peer")
        .map_err(|e| format!("setClientRole peer: {e:?}"))?;
    PEER_ROUTING.with(|cell| {
        let mut guard = cell.borrow_mut();
        guard
            .peer_client_by_peer_id
            .insert(peer_id.to_string(), client_id.clone());
        guard
            .peer_id_by_client
            .insert(client_id.clone(), peer_id.to_string());
    });
    Ok(client_id)
}

fn close_peer(peer_id: &str) {
    PEER_ROUTING.with(|cell| {
        let mut guard = cell.borrow_mut();
        if let Some(client) = guard.peer_client_by_peer_id.remove(peer_id) {
            guard.peer_id_by_client.remove(&client);
        }
        guard.peer_terms.remove(peer_id);
    });
}

// =============================================================================
// Outbox callbacks
// =============================================================================

fn make_peer_routing_lookup() -> Function {
    Closure::<dyn Fn(JsValue) -> JsValue>::new(|client_id: JsValue| {
        let Some(client) = client_id.as_string() else {
            return JsValue::NULL;
        };
        PEER_ROUTING.with(|cell| {
            let guard = cell.borrow();
            let Some(peer_id) = guard.peer_id_by_client.get(&client) else {
                return JsValue::NULL;
            };
            let term = guard.peer_terms.get(peer_id).copied().unwrap_or(0);
            let obj = Object::new();
            let _ = Reflect::set(&obj, &"peerId".into(), &JsValue::from_str(peer_id));
            let _ = Reflect::set(&obj, &"term".into(), &JsValue::from_f64(term as f64));
            obj.into()
        })
    })
    .into_js_value()
    .unchecked_into()
}

fn make_on_main_sync_flushed() -> Function {
    Closure::<dyn Fn()>::new(|| {
        queue_rejected_batch_replay();
    })
    .into_js_value()
    .unchecked_into()
}

// =============================================================================
// Mutation-error replay
// =============================================================================

fn sync_retained_local_batch_records(runtime: &Rc<WasmRuntime>) {
    match runtime.load_local_batch_records() {
        Ok(batches) => {
            let msg = build_local_batch_records_sync(&batches);
            post_to_main(&msg);
        }
        Err(err) => tracing::warn!("loadLocalBatchRecords failed: {err:?}"),
    }
}

fn queue_rejected_batch_replay() {
    let already_queued = HOST.with(|cell| {
        let mut guard = cell.borrow_mut();
        let Some(host) = guard.as_mut() else {
            return true;
        };
        if host.rejected_replay_queued {
            return true;
        }
        host.rejected_replay_queued = true;
        false
    });
    if already_queued {
        return;
    }
    wasm_bindgen_futures::spawn_local(async {
        HOST.with(|cell| {
            if let Some(h) = cell.borrow_mut().as_mut() {
                h.rejected_replay_queued = false;
            }
        });
        let runtime = RUNTIME.with(|cell| cell.borrow().clone());
        let Some(runtime) = runtime else {
            return;
        };
        let batch_ids = match runtime.drain_rejected_batch_ids() {
            Ok(ids) => ids,
            Err(err) => {
                tracing::warn!("drainRejectedBatchIds failed: {err:?}");
                return;
            }
        };
        let Ok(arr): Result<Array, _> = batch_ids.dyn_into() else {
            return;
        };
        for entry in arr.iter() {
            let Some(batch_id) = entry.as_string() else {
                continue;
            };
            match runtime.load_local_batch_record(&batch_id) {
                Ok(batch) => {
                    if batch.is_null() || batch.is_undefined() {
                        continue;
                    }
                    if !is_rejected_settlement(&batch) {
                        continue;
                    }
                    post_to_main(&build_mutation_error_replay(&batch));
                }
                Err(err) => tracing::warn!("loadLocalBatchRecord {batch_id}: {err:?}"),
            }
        }
    });
}

fn is_rejected_settlement(batch: &JsValue) -> bool {
    let Ok(settlement) = Reflect::get(batch, &"latestSettlement".into()) else {
        return false;
    };
    if settlement.is_null() || settlement.is_undefined() {
        return false;
    }
    let Ok(kind) = Reflect::get(&settlement, &"kind".into()) else {
        return false;
    };
    kind.as_string().as_deref() == Some("rejected")
}

// =============================================================================
// Message dispatch
// =============================================================================

/// Per-message entry. Buffers everything pre-Ready and dispatches once Ready.
fn handle_main_message(msg: MainToWorkerMessage) {
    // Init at any time other than during the initial bootstrap is a programming
    // error — post error and ignore (spec).
    if matches!(msg, MainToWorkerMessage::Init(_)) {
        post_to_main(&build_error("ignoring duplicate init"));
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

    match msg {
        MainToWorkerMessage::Init(_) => {
            post_to_main(&build_error("ignoring duplicate init"));
        }
        MainToWorkerMessage::Sync { payloads } => {
            let Some(rt) = runtime.as_ref() else { return };
            let Some(main_client_id) = get_main_client_id() else {
                return;
            };
            for payload in payloads {
                let arr = Uint8Array::from(payload.as_slice());
                if let Err(err) =
                    rt.on_sync_message_received_from_client(&main_client_id, arr.into())
                {
                    tracing::warn!("onSyncMessageReceivedFromClient main: {err:?}");
                }
            }
        }
        MainToWorkerMessage::PeerOpen { peer_id } => {
            if let Some(rt) = runtime.as_ref() {
                let _ = ensure_peer_client(rt, &peer_id);
            }
        }
        MainToWorkerMessage::PeerSync {
            peer_id,
            term,
            payloads,
        } => {
            let Some(rt) = runtime.as_ref() else { return };
            match ensure_peer_client(rt, &peer_id) {
                Ok(client) => {
                    PEER_ROUTING.with(|cell| {
                        cell.borrow_mut().peer_terms.insert(peer_id.clone(), term);
                    });
                    for payload in payloads {
                        let arr = Uint8Array::from(payload.as_slice());
                        if let Err(err) =
                            rt.on_sync_message_received_from_client(&client, arr.into())
                        {
                            tracing::warn!("peer-sync route: {err:?}");
                        }
                    }
                }
                Err(err) => tracing::warn!("ensure peer client: {err}"),
            }
        }
        MainToWorkerMessage::PeerClose { peer_id } => close_peer(&peer_id),
        MainToWorkerMessage::LifecycleHint { event, .. } => {
            handle_lifecycle_hint(event, runtime.as_ref());
        }
        MainToWorkerMessage::UpdateAuth { jwt_token } => {
            update_auth(jwt_token, runtime.as_ref());
        }
        MainToWorkerMessage::DisconnectUpstream => {
            if let Some(rt) = runtime.as_ref() {
                rt.disconnect();
                post_to_main(&crate::worker_protocol::build_upstream_disconnected());
            }
        }
        MainToWorkerMessage::ReconnectUpstream => {
            if let Some(rt) = runtime.as_ref() {
                let (ws_url, auth_json) = build_reconnect_auth();
                if let Some(url) = ws_url {
                    perform_upstream_connect(rt, &url, &auth_json);
                }
            }
        }
        MainToWorkerMessage::Shutdown => handle_shutdown(runtime.as_ref(), false),
        MainToWorkerMessage::SimulateCrash => handle_shutdown(runtime.as_ref(), true),
        MainToWorkerMessage::AcknowledgeRejectedBatch { batch_id } => {
            if let Some(rt) = runtime.as_ref() {
                if let Err(err) = rt.acknowledge_rejected_batch(&batch_id) {
                    tracing::warn!("acknowledgeRejectedBatch: {err:?}");
                }
            }
        }
        MainToWorkerMessage::DebugSchemaState => match runtime.as_ref() {
            Some(rt) => match rt.debug_schema_state() {
                Ok(state_value) => post_to_main(&build_debug_schema_state_ok(&state_value)),
                Err(err) => post_to_main(&build_error(&format!(
                    "debug-schema-state failed: {}",
                    js_error_message(&err.into())
                ))),
            },
            None => {
                post_to_main(&build_error(
                    "debug-schema-state requested before worker init complete",
                ));
            }
        },
        MainToWorkerMessage::DebugSeedLiveSchema { schema_json } => match runtime.as_ref() {
            Some(rt) => match rt.debug_seed_live_schema(&schema_json) {
                Ok(()) => {
                    rt.flush_wal();
                    post_to_main(&build_debug_seed_live_schema_ok());
                }
                Err(err) => post_to_main(&build_error(&format!(
                    "debug-seed-live-schema failed: {}",
                    js_error_message(&err.into())
                ))),
            },
            None => {
                post_to_main(&build_error(
                    "debug-seed-live-schema requested before worker init complete",
                ));
            }
        },
        MainToWorkerMessage::Unknown(t) => {
            tracing::warn!("ignoring unknown worker message type {t}");
        }
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
        post_to_main(&build_auth_failed("invalid"));
    }
}

fn handle_shutdown(runtime: Option<&Rc<WasmRuntime>>, simulate_crash: bool) {
    HOST.with(|cell| {
        if let Some(h) = cell.borrow_mut().as_mut() {
            h.state = HostState::ShuttingDown;
        }
    });

    if let Some(rt) = runtime {
        if simulate_crash {
            rt.flush_wal();
        }
        rt.install_noop_sync_sender();
        rt.set_server_payload_forwarder(None);
    }

    // Clear self.onmessage explicitly. `Closure::drop` invalidates the call
    // but does not clear the JS slot — a late inbound would invoke a freed
    // trampoline.
    let global = global_worker_scope();
    global.set_onmessage(None);

    RUNTIME.with(|cell| *cell.borrow_mut() = None);
    PEER_ROUTING.with(|cell| {
        let mut g = cell.borrow_mut();
        g.peer_client_by_peer_id.clear();
        g.peer_id_by_client.clear();
        g.peer_terms.clear();
        g.main_client_id = None;
    });

    post_to_main(&build_shutdown_ok());
    global.close();
    HOST.with(|cell| *cell.borrow_mut() = None);
}

// =============================================================================
// Helpers
// =============================================================================

fn get_main_client_id() -> Option<String> {
    PEER_ROUTING.with(|cell| cell.borrow().main_client_id.clone())
}

fn global_worker_scope() -> DedicatedWorkerGlobalScope {
    js_sys::global()
        .dyn_into::<DedicatedWorkerGlobalScope>()
        .expect("worker host expects a DedicatedWorkerGlobalScope")
}

fn post_to_main(message: &JsValue) {
    let global = global_worker_scope();
    let _ = global.post_message(message);
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
    } else {
        ("ws://", trimmed)
    };
    format!("{}{}/apps/{}/ws", scheme.0, scheme.1, app_id)
}
