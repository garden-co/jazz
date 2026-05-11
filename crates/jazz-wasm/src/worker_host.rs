//! Worker-side runtime host.
//!
//! `run_as_worker(init_message, pending_messages)` is the wasm-bindgen entry
//! point invoked from the JS shim. It installs `self.onmessage` on the
//! dedicated-worker scope, parks per-thread state in `thread_local`s, and
//! asynchronously initialises the worker `WasmRuntime`.
//!
//! Detailed dispatch + lifecycle behaviour is in
//! `specs/.../implementation-spec.md` §9.

#![cfg(target_arch = "wasm32")]

use std::cell::RefCell;
use std::collections::{HashMap, VecDeque};
use std::rc::Rc;

use js_sys::{Array, Function, Object, Reflect, Uint8Array};
use wasm_bindgen::closure::Closure;
use wasm_bindgen::prelude::*;
use wasm_bindgen::JsCast;
use wasm_bindgen_futures::spawn_local;
use web_sys::{DedicatedWorkerGlobalScope, MessageEvent};

use crate::runtime::{RustOutboxSender, WasmRuntime};
use crate::worker_protocol::{
    parse_main_to_worker, worker_to_main_post, InitPayload, InitPayloadFields, MainToWorkerMessage,
    MainToWorkerWire, WorkerLifecycleEvent, WorkerToMainWire,
};

// ---------------------------------------------------------------------------
// State
// ---------------------------------------------------------------------------

#[derive(Clone, Copy, PartialEq, Eq)]
enum HostState {
    Initializing,
    Ready,
    ShuttingDown,
}

struct WorkerHost {
    state: HostState,
    pending_messages: VecDeque<MainToWorkerMessage>,
    on_message_closure: Option<Closure<dyn FnMut(MessageEvent)>>,
    current_auth_jwt: Option<String>,
    current_admin_secret: Option<String>,
    current_ws_url: Option<String>,
}

#[derive(Default)]
struct PeerRouting {
    main_client_id: Option<String>,
    peer_client_by_peer_id: HashMap<String, String>,
    peer_id_by_client: HashMap<String, String>,
    peer_terms: HashMap<String, u32>,
}

thread_local! {
    static HOST: RefCell<Option<WorkerHost>> = const { RefCell::new(None) };
    static RUNTIME: RefCell<Option<Rc<WasmRuntime>>> = const { RefCell::new(None) };
    static PEER_ROUTING: RefCell<PeerRouting> = RefCell::new(PeerRouting {
        main_client_id: None,
        peer_client_by_peer_id: HashMap::new(),
        peer_id_by_client: HashMap::new(),
        peer_terms: HashMap::new(),
    });
}

// ---------------------------------------------------------------------------
// Public entry
// ---------------------------------------------------------------------------

#[wasm_bindgen(js_name = runAsWorker)]
pub fn run_as_worker(init_message: JsValue, pending_messages: Array) -> Result<(), JsError> {
    // Idempotent guard.
    let already = HOST.with(|c| c.borrow().is_some());
    if already {
        return Ok(());
    }

    // Parse the init message synchronously so we can fail fast.
    let init_payload = match parse_main_to_worker(&init_message) {
        Ok(MainToWorkerMessage::Init(payload)) => payload,
        Ok(MainToWorkerMessage::Wire(wire)) => {
            post_to_main(&WorkerToMainWire::Error {
                message: format!("first message must be `init`, got {}", wire_type_str(&wire)),
            });
            return Ok(());
        }
        Ok(MainToWorkerMessage::Unknown(t)) => {
            post_to_main(&WorkerToMainWire::Error {
                message: format!("first message must be `init`, got `{t}`"),
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

    let mut host = WorkerHost {
        state: HostState::Initializing,
        pending_messages: VecDeque::new(),
        on_message_closure: None,
        current_auth_jwt: None,
        current_admin_secret: None,
        current_ws_url: None,
    };

    // Drain pending_messages, buffering legitimate wire messages and discarding
    // duplicate inits.
    for entry in pending_messages.iter() {
        match parse_main_to_worker(&entry) {
            Ok(MainToWorkerMessage::Init(_)) => {
                post_to_main(&WorkerToMainWire::Error {
                    message: "ignoring duplicate init".into(),
                });
            }
            Ok(msg) => host.pending_messages.push_back(msg),
            Err(e) => tracing::warn!("dropping malformed buffered message: {e}"),
        }
    }

    // Install the worker-side onmessage handler.
    let global = global_worker_scope().map_err(|e| JsError::new(&e))?;
    let closure = Closure::<dyn FnMut(MessageEvent)>::new(move |event: MessageEvent| {
        let data = event.data();
        match parse_main_to_worker(&data) {
            Ok(msg) => handle_main_message(msg),
            Err(e) => post_to_main(&WorkerToMainWire::Error {
                message: format!("malformed worker message: {e}"),
            }),
        }
    });
    global.set_onmessage(Some(closure.as_ref().unchecked_ref()));
    host.on_message_closure = Some(closure);

    HOST.with(|c| *c.borrow_mut() = Some(host));

    spawn_local(async move {
        if let Err(msg) = run_init(*init_payload).await {
            post_to_main(&WorkerToMainWire::Error {
                message: format!("Init failed: {msg}"),
            });
        }
    });

    Ok(())
}

// ---------------------------------------------------------------------------
// Initialisation
// ---------------------------------------------------------------------------

async fn run_init(init: InitPayload) -> Result<(), String> {
    let f = init.fields;

    let runtime = open_runtime(&f).await?;

    let main_client_id = runtime.add_client();
    runtime
        .set_client_role(&main_client_id, "peer")
        .map_err(|e| {
            let v: JsValue = e.into();
            js_error_message(&v)
        })?;

    let auth_cb = Closure::<dyn FnMut(JsValue)>::new(|reason: JsValue| {
        let raw = reason.as_string().unwrap_or_default();
        post_to_main(&WorkerToMainWire::UpstreamDisconnected);
        post_to_main(&WorkerToMainWire::AuthFailed {
            reason: map_auth_reason(&raw).to_string(),
        });
    })
    .into_js_value();
    runtime.on_auth_failure(auth_cb.unchecked_into());

    let mutation_cb = Closure::<dyn FnMut(JsValue)>::new(|event: JsValue| {
        let json = json_stringify(&event).unwrap_or_else(|| "{}".into());
        post_to_main(&WorkerToMainWire::MutationErrorReplay { batch_json: json });
    })
    .into_js_value();
    runtime.on_mutation_error(mutation_cb.unchecked_into());

    let runtime_rc = Rc::new(runtime);
    RUNTIME.with(|c| *c.borrow_mut() = Some(Rc::clone(&runtime_rc)));
    PEER_ROUTING.with(|c| c.borrow_mut().main_client_id = Some(main_client_id.clone()));

    // Worker-side outbox sender.
    let sender = RustOutboxSender::new(true);
    sender.attach_target(
        global_worker_scope_value(),
        Some(main_client_id.clone()),
        Some(make_peer_routing_lookup()),
        Some(make_on_main_sync_flushed()),
    );
    runtime_rc
        .core
        .borrow_mut()
        .set_sync_sender(Box::new(sender.clone()));

    // Bootstrap catalogue dance.
    sender.set_bootstrap_catalogue_forwarding(true);
    let _ = runtime_rc.add_server(None, None);
    runtime_rc.remove_server();
    sender.set_bootstrap_catalogue_forwarding(false);

    // Upstream connect (before draining pending sync messages).
    if let Some(server_url) = &f.server_url {
        let mut auth_map = serde_json::Map::new();
        if let Some(secret) = &f.admin_secret {
            auth_map.insert(
                "admin_secret".into(),
                serde_json::Value::String(secret.clone()),
            );
            HOST.with(|c| {
                if let Some(h) = c.borrow_mut().as_mut() {
                    h.current_admin_secret = Some(secret.clone());
                }
            });
        }
        if let Some(jwt) = &f.jwt_token {
            auth_map.insert("jwt_token".into(), serde_json::Value::String(jwt.clone()));
            HOST.with(|c| {
                if let Some(h) = c.borrow_mut().as_mut() {
                    h.current_auth_jwt = Some(jwt.clone());
                }
            });
        }
        let auth_json = serde_json::to_string(&auth_map).unwrap_or_else(|_| "{}".into());
        let ws_url = http_url_to_ws(server_url, &f.app_id);
        HOST.with(|c| {
            if let Some(h) = c.borrow_mut().as_mut() {
                h.current_ws_url = Some(ws_url.clone());
            }
        });
        perform_upstream_connect(&runtime_rc, &ws_url, &auth_json);
    }

    sync_retained_local_batch_records(&runtime_rc);

    HOST.with(|c| {
        if let Some(h) = c.borrow_mut().as_mut() {
            h.state = HostState::Ready;
        }
    });

    drain_pending_messages();

    post_to_main(&WorkerToMainWire::InitOk {
        client_id: main_client_id,
    });

    Ok(())
}

async fn open_runtime(f: &InitPayloadFields) -> Result<WasmRuntime, String> {
    // Try persistent first; on SecurityError fall back to ephemeral.
    let persistent = WasmRuntime::open_persistent(
        &f.schema_json,
        &f.app_id,
        &f.env,
        &f.user_branch,
        &f.db_name,
        Some("local".into()),
        false,
    )
    .await;
    match persistent {
        Ok(rt) => Ok(rt),
        Err(err) if is_security_error(&err) => {
            tracing::warn!("OPFS unavailable (SecurityError) — falling back to ephemeral");
            WasmRuntime::open_ephemeral(
                &f.schema_json,
                &f.app_id,
                &f.env,
                &f.user_branch,
                &f.db_name,
                Some("local".into()),
                false,
            )
            .map_err(|e| {
                let v: JsValue = e.into();
                format!("ephemeral open: {}", js_error_message(&v))
            })
        }
        Err(err) => Err(format!("persistent open: {}", js_error_message(&err))),
    }
}

fn perform_upstream_connect(runtime: &Rc<WasmRuntime>, ws_url: &str, auth_json: &str) {
    if let Err(err) = runtime.connect(ws_url.to_string(), auth_json.to_string()) {
        tracing::error!("upstream connect failed: {}", js_error_message(&err));
        post_to_main(&WorkerToMainWire::UpstreamDisconnected);
    } else {
        post_to_main(&WorkerToMainWire::UpstreamConnected);
    }
}

fn sync_retained_local_batch_records(runtime: &WasmRuntime) {
    match runtime.load_local_batch_records() {
        Ok(value) => {
            let json = json_stringify(&value).unwrap_or_else(|| "[]".into());
            post_to_main(&WorkerToMainWire::LocalBatchRecordsSync { batches_json: json });
        }
        Err(_) => tracing::warn!("load_local_batch_records failed"),
    }
}

// ---------------------------------------------------------------------------
// Message dispatch
// ---------------------------------------------------------------------------

fn handle_main_message(msg: MainToWorkerMessage) {
    if matches!(msg, MainToWorkerMessage::Init(_)) {
        post_to_main(&WorkerToMainWire::Error {
            message: "ignoring duplicate init".into(),
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
        _ => {}
    }
}

fn drain_pending_messages() {
    let drained: Vec<MainToWorkerMessage> = HOST.with(|c| {
        c.borrow_mut()
            .as_mut()
            .map(|h| {
                std::mem::take(&mut h.pending_messages)
                    .into_iter()
                    .collect()
            })
            .unwrap_or_default()
    });
    for msg in drained {
        process_main_message(msg);
    }
}

fn process_main_message(msg: MainToWorkerMessage) {
    let runtime = RUNTIME.with(|c| c.borrow().clone());
    let Some(runtime) = runtime else { return };
    let wire = match msg {
        MainToWorkerMessage::Init(_) => {
            post_to_main(&WorkerToMainWire::Error {
                message: "ignoring duplicate init".into(),
            });
            return;
        }
        MainToWorkerMessage::Unknown(t) => {
            tracing::warn!("unknown main→worker JS object: {t}");
            return;
        }
        MainToWorkerMessage::Wire(w) => w,
    };
    dispatch_wire(&runtime, wire);
}

fn dispatch_wire(runtime: &Rc<WasmRuntime>, wire: MainToWorkerWire) {
    match wire {
        MainToWorkerWire::Sync { payloads } => {
            let main_client_id = PEER_ROUTING.with(|c| c.borrow().main_client_id.clone());
            let Some(main_client_id) = main_client_id else {
                return;
            };
            for buf in payloads {
                let arr = Uint8Array::from(buf.as_ref());
                let _ = runtime.on_sync_message_received_from_client(&main_client_id, arr.into());
            }
        }
        MainToWorkerWire::PeerOpen { peer_id } => {
            ensure_peer_client(runtime, &peer_id);
        }
        MainToWorkerWire::PeerSync {
            peer_id,
            term,
            payloads,
        } => {
            let peer_client = ensure_peer_client(runtime, &peer_id);
            PEER_ROUTING.with(|c| {
                c.borrow_mut().peer_terms.insert(peer_id.clone(), term);
            });
            for buf in payloads {
                let arr = Uint8Array::from(buf.as_ref());
                let _ = runtime.on_sync_message_received_from_client(&peer_client, arr.into());
            }
        }
        MainToWorkerWire::PeerClose { peer_id } => {
            PEER_ROUTING.with(|c| {
                let mut g = c.borrow_mut();
                if let Some(client_id) = g.peer_client_by_peer_id.remove(&peer_id) {
                    g.peer_id_by_client.remove(&client_id);
                }
                g.peer_terms.remove(&peer_id);
            });
        }
        MainToWorkerWire::LifecycleHint { event, .. } => match event {
            WorkerLifecycleEvent::VisibilityHidden
            | WorkerLifecycleEvent::Pagehide
            | WorkerLifecycleEvent::Freeze => {
                runtime.flush_wal();
            }
            _ => {}
        },
        MainToWorkerWire::UpdateAuth { jwt_token } => {
            HOST.with(|c| {
                if let Some(h) = c.borrow_mut().as_mut() {
                    h.current_auth_jwt = jwt_token.clone();
                }
            });
            let auth_json = build_auth_json();
            if let Err(err) = runtime.update_auth(auth_json) {
                tracing::warn!("update_auth failed: {}", js_error_message(&err));
                post_to_main(&WorkerToMainWire::AuthFailed {
                    reason: "invalid".into(),
                });
            }
        }
        MainToWorkerWire::DisconnectUpstream => {
            runtime.disconnect();
            post_to_main(&WorkerToMainWire::UpstreamDisconnected);
        }
        MainToWorkerWire::ReconnectUpstream => {
            let (ws_url, auth_json) = HOST.with(|c| {
                let g = c.borrow();
                let h = g.as_ref();
                (
                    h.and_then(|h| h.current_ws_url.clone()),
                    build_auth_json_from(
                        h.and_then(|h| h.current_admin_secret.clone()),
                        h.and_then(|h| h.current_auth_jwt.clone()),
                    ),
                )
            });
            if let Some(ws_url) = ws_url {
                perform_upstream_connect(runtime, &ws_url, &auth_json);
            }
        }
        MainToWorkerWire::Shutdown => handle_shutdown(runtime, false),
        MainToWorkerWire::SimulateCrash => handle_shutdown(runtime, true),
        MainToWorkerWire::AcknowledgeRejectedBatch { batch_id } => {
            let _ = runtime.acknowledge_rejected_batch(&batch_id);
        }
        MainToWorkerWire::DebugSchemaState => match runtime.debug_schema_state() {
            Ok(state) => {
                let json = json_stringify(&state).unwrap_or_else(|| "{}".into());
                post_to_main(&WorkerToMainWire::DebugSchemaStateOk { state_json: json });
            }
            Err(err) => {
                let v: JsValue = err.into();
                post_to_main(&WorkerToMainWire::Error {
                    message: format!("debug_schema_state: {}", js_error_message(&v)),
                });
            }
        },
        MainToWorkerWire::DebugSeedLiveSchema { schema_json } => {
            match runtime.debug_seed_live_schema(&schema_json) {
                Ok(()) => {
                    runtime.flush_wal();
                    post_to_main(&WorkerToMainWire::DebugSeedLiveSchemaOk);
                }
                Err(err) => {
                    let v: JsValue = err.into();
                    post_to_main(&WorkerToMainWire::Error {
                        message: format!("debug_seed_live_schema: {}", js_error_message(&v)),
                    });
                }
            }
        }
    }
}

fn ensure_peer_client(runtime: &WasmRuntime, peer_id: &str) -> String {
    if let Some(cid) =
        PEER_ROUTING.with(|c| c.borrow().peer_client_by_peer_id.get(peer_id).cloned())
    {
        return cid;
    }
    let cid = runtime.add_client();
    if let Err(err) = runtime.set_client_role(&cid, "peer") {
        let v: JsValue = err.into();
        tracing::warn!("set_client_role(peer): {}", js_error_message(&v));
    }
    PEER_ROUTING.with(|c| {
        let mut g = c.borrow_mut();
        g.peer_client_by_peer_id
            .insert(peer_id.to_string(), cid.clone());
        g.peer_id_by_client.insert(cid.clone(), peer_id.to_string());
    });
    cid
}

fn handle_shutdown(runtime: &Rc<WasmRuntime>, _simulate_crash: bool) {
    HOST.with(|c| {
        if let Some(h) = c.borrow_mut().as_mut() {
            h.state = HostState::ShuttingDown;
        }
    });

    runtime.batched_tick();
    runtime.flush_wal();
    runtime.install_noop_sync_sender();

    if let Ok(global) = global_worker_scope() {
        global.set_onmessage(None);
    }

    RUNTIME.with(|c| *c.borrow_mut() = None);
    PEER_ROUTING.with(|c| *c.borrow_mut() = PeerRouting::default());

    post_to_main(&WorkerToMainWire::ShutdownOk);
    if let Ok(global) = global_worker_scope() {
        global.close();
    }
    HOST.with(|c| *c.borrow_mut() = None);
}

// ---------------------------------------------------------------------------
// Outbox callbacks
// ---------------------------------------------------------------------------

fn make_peer_routing_lookup() -> Function {
    let closure = Closure::<dyn FnMut(JsValue) -> JsValue>::new(|client_id: JsValue| {
        let Some(client) = client_id.as_string() else {
            return JsValue::NULL;
        };
        PEER_ROUTING.with(|c| {
            let g = c.borrow();
            let Some(peer_id) = g.peer_id_by_client.get(&client) else {
                return JsValue::NULL;
            };
            let term = g.peer_terms.get(peer_id).copied().unwrap_or(0);
            let obj = Object::new();
            let _ = Reflect::set(
                &obj,
                &JsValue::from_str("peerId"),
                &JsValue::from_str(peer_id),
            );
            let _ = Reflect::set(
                &obj,
                &JsValue::from_str("term"),
                &JsValue::from_f64(term as f64),
            );
            obj.into()
        })
    });
    let f: Function = closure.as_ref().unchecked_ref::<Function>().clone();
    closure.forget();
    f
}

fn make_on_main_sync_flushed() -> Function {
    // The legacy worker drove rejected-batch replay via `onMutationError`,
    // which the host wires in `run_init`. The post-flush hook is therefore a
    // no-op here, but the spec still requires the callback to exist.
    let closure = Closure::<dyn FnMut()>::new(|| {});
    let f: Function = closure.as_ref().unchecked_ref::<Function>().clone();
    closure.forget();
    f
}

// ---------------------------------------------------------------------------
// URL + auth helpers
// ---------------------------------------------------------------------------

/// Normalise a server URL to a WebSocket URL with the app path appended.
pub fn http_url_to_ws(server_url: &str, app_id: &str) -> String {
    let lower = server_url.to_ascii_lowercase();
    let (prefix_len, scheme): (usize, &str) = if lower.starts_with("https://") {
        (8, "wss://")
    } else if lower.starts_with("http://") {
        (7, "ws://")
    } else if lower.starts_with("wss://") {
        (6, "wss://")
    } else if lower.starts_with("ws://") {
        (5, "ws://")
    } else {
        (0, "ws://")
    };
    let mut rest = &server_url[prefix_len..];
    while rest.ends_with('/') {
        rest = &rest[..rest.len() - 1];
    }
    format!("{scheme}{rest}/apps/{app_id}/ws")
}

pub fn map_auth_reason(reason: &str) -> &'static str {
    match reason {
        "Unauthorized" | "expired" => "expired",
        "missing" | "Missing token" => "missing",
        "disabled" | "Auth disabled" => "disabled",
        _ => "invalid",
    }
}

fn build_auth_json() -> String {
    HOST.with(|c| {
        let g = c.borrow();
        let h = g.as_ref();
        build_auth_json_from(
            h.and_then(|h| h.current_admin_secret.clone()),
            h.and_then(|h| h.current_auth_jwt.clone()),
        )
    })
}

fn build_auth_json_from(admin_secret: Option<String>, jwt_token: Option<String>) -> String {
    let mut map = serde_json::Map::new();
    if let Some(s) = admin_secret {
        map.insert("admin_secret".into(), serde_json::Value::String(s));
    }
    if let Some(t) = jwt_token {
        map.insert("jwt_token".into(), serde_json::Value::String(t));
    }
    serde_json::to_string(&map).unwrap_or_else(|_| "{}".into())
}

fn is_security_error(value: &JsValue) -> bool {
    Reflect::get(value, &JsValue::from_str("name"))
        .ok()
        .and_then(|v| v.as_string())
        .map(|s| s == "SecurityError")
        .unwrap_or_else(|| js_error_message(value).contains("SecurityError"))
}

// ---------------------------------------------------------------------------
// Misc helpers
// ---------------------------------------------------------------------------

fn wire_type_str(wire: &MainToWorkerWire) -> &'static str {
    match wire {
        MainToWorkerWire::Sync { .. } => "sync",
        MainToWorkerWire::PeerOpen { .. } => "peer-open",
        MainToWorkerWire::PeerSync { .. } => "peer-sync",
        MainToWorkerWire::PeerClose { .. } => "peer-close",
        MainToWorkerWire::LifecycleHint { .. } => "lifecycle-hint",
        MainToWorkerWire::UpdateAuth { .. } => "update-auth",
        MainToWorkerWire::DisconnectUpstream => "disconnect-upstream",
        MainToWorkerWire::ReconnectUpstream => "reconnect-upstream",
        MainToWorkerWire::Shutdown => "shutdown",
        MainToWorkerWire::AcknowledgeRejectedBatch { .. } => "acknowledge-rejected-batch",
        MainToWorkerWire::SimulateCrash => "simulate-crash",
        MainToWorkerWire::DebugSchemaState => "debug-schema-state",
        MainToWorkerWire::DebugSeedLiveSchema { .. } => "debug-seed-live-schema",
    }
}

fn json_stringify(value: &JsValue) -> Option<String> {
    let global = js_sys::global();
    let json = Reflect::get(&global, &JsValue::from_str("JSON")).ok()?;
    let stringify: Function = Reflect::get(&json, &JsValue::from_str("stringify"))
        .ok()?
        .dyn_into()
        .ok()?;
    stringify
        .call1(&JsValue::NULL, value)
        .ok()
        .and_then(|v| v.as_string())
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

fn global_worker_scope() -> Result<DedicatedWorkerGlobalScope, String> {
    js_sys::global()
        .dyn_into::<DedicatedWorkerGlobalScope>()
        .map_err(|_| "not running inside a DedicatedWorkerGlobalScope".to_string())
}

fn global_worker_scope_value() -> JsValue {
    js_sys::global().into()
}

fn post_to_main(wire: &WorkerToMainWire) {
    let (msg, transfer) = match worker_to_main_post(wire) {
        Ok(t) => t,
        Err(e) => {
            tracing::warn!("worker→main encode failed: {e}");
            return;
        }
    };
    let global = match global_worker_scope() {
        Ok(g) => g,
        Err(e) => {
            tracing::warn!("worker→main post: {e}");
            return;
        }
    };
    if let Err(err) = global.post_message_with_transfer(&msg, &transfer.into()) {
        tracing::warn!("worker→main postMessage: {}", js_error_message(&err));
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[wasm_bindgen_test::wasm_bindgen_test]
    fn http_url_to_ws_normalises_https() {
        assert_eq!(
            http_url_to_ws("https://example.test", "app-1"),
            "wss://example.test/apps/app-1/ws"
        );
    }

    #[wasm_bindgen_test::wasm_bindgen_test]
    fn http_url_to_ws_normalises_http() {
        assert_eq!(
            http_url_to_ws("http://localhost:4000", "xyz"),
            "ws://localhost:4000/apps/xyz/ws"
        );
    }

    #[wasm_bindgen_test::wasm_bindgen_test]
    fn http_url_to_ws_passes_wss_through() {
        assert_eq!(
            http_url_to_ws("wss://relay.example", "x"),
            "wss://relay.example/apps/x/ws"
        );
    }

    #[wasm_bindgen_test::wasm_bindgen_test]
    fn http_url_to_ws_passes_ws_through() {
        assert_eq!(
            http_url_to_ws("ws://relay.example", "x"),
            "ws://relay.example/apps/x/ws"
        );
    }

    #[wasm_bindgen_test::wasm_bindgen_test]
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

    #[wasm_bindgen_test::wasm_bindgen_test]
    fn http_url_to_ws_defaults_unknown_scheme_to_ws() {
        assert_eq!(
            http_url_to_ws("example.test:4000", "a"),
            "ws://example.test:4000/apps/a/ws"
        );
    }

    #[wasm_bindgen_test::wasm_bindgen_test]
    fn map_auth_reason_recognises_known_strings() {
        assert_eq!(map_auth_reason("Unauthorized"), "expired");
        assert_eq!(map_auth_reason("expired"), "expired");
        assert_eq!(map_auth_reason("missing"), "missing");
        assert_eq!(map_auth_reason("Missing token"), "missing");
        assert_eq!(map_auth_reason("disabled"), "disabled");
        assert_eq!(map_auth_reason("Auth disabled"), "disabled");
    }

    #[wasm_bindgen_test::wasm_bindgen_test]
    fn map_auth_reason_falls_back_to_invalid() {
        assert_eq!(map_auth_reason(""), "invalid");
        assert_eq!(map_auth_reason("totally unrecognised"), "invalid");
        assert_eq!(map_auth_reason("Unauthorized "), "invalid");
    }
}
