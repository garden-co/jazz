//! `WasmWorkerBridge` state-machine tests using a synthetic Worker shim.
//!
//! Run with:
//!   RUSTFLAGS='--cfg=web_sys_unstable_apis --cfg getrandom_backend="wasm_js"' \
//!     wasm-pack test --headless --chrome crates/jazz-wasm
//!
//! ## Synthetic Worker
//!
//! Each test builds a JS object exposing `postMessage` / `onmessage` and
//! `unchecked_into::<web_sys::Worker>()`s it. The bridge can't tell a real
//! Worker from a duck-typed one — it only calls those two members. The shim
//! captures every outbound `postMessage` (which the bridge will be encoding
//! as postcard `Uint8Array`s after Stage 1/2/3) and exposes a helper that
//! synthesises a `MessageEvent`-shaped object and dispatches it through the
//! bridge's onmessage handler.

#![cfg(target_arch = "wasm32")]

use std::cell::RefCell;
use std::rc::Rc;

use js_sys::{Function, Object, Reflect, Uint8Array};
use serde_bytes::ByteBuf;
use wasm_bindgen::prelude::*;
use wasm_bindgen::JsCast;
use wasm_bindgen_futures::JsFuture;
use wasm_bindgen_test::*;
use web_sys::Worker;

use jazz_tools::batch_fate::{BatchFate, BatchMode, LocalBatchRecord};
use jazz_tools::row_histories::BatchId;
use jazz_tools::sync_manager::DurabilityTier;
use jazz_wasm::worker_protocol::{
    encode_main_to_worker, encode_worker_to_main, MainToWorkerWire, WorkerToMainWire,
};
use jazz_wasm::WasmRuntime;

wasm_bindgen_test_configure!(run_in_browser);

const SCHEMA_JSON: &str = r#"{
    "todos": {
        "columns": [
            {"name": "title", "column_type": {"type": "Text"}, "nullable": false},
            {"name": "completed", "column_type": {"type": "Boolean"}, "nullable": false}
        ]
    }
}"#;

// =============================================================================
// Synthetic Worker shim
// =============================================================================

struct FakeWorker {
    obj: JsValue,
    posted: Rc<RefCell<Vec<JsValue>>>,
    /// Keep the closure alive for the lifetime of the test. Without this,
    /// `into_js_value` would leak it permanently — fine in production but
    /// a slow leak in tests.
    _post_message_closure: Closure<dyn FnMut(JsValue, JsValue)>,
}

impl FakeWorker {
    fn new() -> Self {
        let posted = Rc::new(RefCell::new(Vec::<JsValue>::new()));
        let posted_clone = Rc::clone(&posted);
        let post_message_closure =
            Closure::<dyn FnMut(JsValue, JsValue)>::new(move |msg: JsValue, _transfer: JsValue| {
                posted_clone.borrow_mut().push(msg);
            });
        let obj = Object::new();
        Reflect::set(
            &obj,
            &"postMessage".into(),
            post_message_closure.as_ref().unchecked_ref(),
        )
        .unwrap();
        Reflect::set(&obj, &"onmessage".into(), &JsValue::NULL).unwrap();
        Self {
            obj: obj.into(),
            posted,
            _post_message_closure: post_message_closure,
        }
    }

    fn worker(&self) -> Worker {
        self.obj.clone().unchecked_into()
    }

    fn emit_wire(&self, msg: &WorkerToMainWire) {
        let bytes = encode_worker_to_main(msg).expect("encode worker→main");
        let arr = Uint8Array::from(bytes.as_slice());
        self.emit_data(arr.into());
    }

    fn emit_data(&self, data: JsValue) {
        let event = Object::new();
        Reflect::set(&event, &"data".into(), &data).unwrap();
        let onmessage = Reflect::get(&self.obj, &"onmessage".into()).unwrap();
        if let Ok(f) = onmessage.dyn_into::<Function>() {
            f.call1(&JsValue::NULL, &event.into())
                .expect("dispatch fake message");
        } else {
            panic!("bridge has not installed an onmessage handler");
        }
    }

    fn posted_decoded(&self) -> Vec<MainToWorkerWire> {
        self.posted
            .borrow()
            .iter()
            .filter_map(|v| {
                v.dyn_ref::<Uint8Array>()
                    .and_then(|arr| postcard::from_bytes(&arr.to_vec()).ok())
            })
            .collect()
    }

    fn last_posted_decoded(&self) -> Option<MainToWorkerWire> {
        self.posted_decoded().pop()
    }
}

// =============================================================================
// Test fixtures
// =============================================================================

fn build_options(server_url: Option<&str>) -> JsValue {
    let opts = Object::new();
    Reflect::set(&opts, &"schemaJson".into(), &SCHEMA_JSON.into()).unwrap();
    Reflect::set(&opts, &"appId".into(), &"test-app".into()).unwrap();
    Reflect::set(&opts, &"env".into(), &"dev".into()).unwrap();
    Reflect::set(&opts, &"userBranch".into(), &"main".into()).unwrap();
    Reflect::set(&opts, &"dbName".into(), &"db".into()).unwrap();
    if let Some(u) = server_url {
        Reflect::set(&opts, &"serverUrl".into(), &u.into()).unwrap();
    }
    opts.into()
}

fn fresh_runtime() -> WasmRuntime {
    WasmRuntime::new(
        SCHEMA_JSON,
        "test-app",
        "dev",
        "main",
        None,
        Some(true),
        None,
    )
    .expect("WasmRuntime::new")
}

// =============================================================================
// Tests
// =============================================================================

#[wasm_bindgen_test]
async fn init_resolves_with_client_id() {
    let fw = FakeWorker::new();
    let runtime = fresh_runtime();
    let bridge = jazz_wasm::WasmWorkerBridge::attach(fw.worker(), &runtime, build_options(None))
        .expect("attach");

    let init_promise = bridge.init();

    // Bridge should have posted a JS-object init message (the only non-binary
    // path). Find it in the captured posts.
    let posted = fw.posted.borrow();
    let init = posted
        .iter()
        .find(|v| {
            Reflect::get(v, &"type".into())
                .ok()
                .and_then(|t| t.as_string())
                .as_deref()
                == Some("init")
        })
        .cloned();
    drop(posted);
    assert!(init.is_some(), "bridge did not post init JS object");

    fw.emit_wire(&WorkerToMainWire::InitOk {
        client_id: "client-42".into(),
    });

    let result = JsFuture::from(init_promise).await.expect("init resolved");
    let client_id = Reflect::get(&result, &"clientId".into())
        .ok()
        .and_then(|v| v.as_string());
    assert_eq!(client_id.as_deref(), Some("client-42"));
    assert_eq!(
        bridge.get_worker_client_id().as_string().as_deref(),
        Some("client-42")
    );
}

#[wasm_bindgen_test]
async fn init_propagates_error() {
    let fw = FakeWorker::new();
    let runtime = fresh_runtime();
    let bridge = jazz_wasm::WasmWorkerBridge::attach(fw.worker(), &runtime, build_options(None))
        .expect("attach");

    let init_promise = bridge.init();
    fw.emit_wire(&WorkerToMainWire::Error {
        message: "schema mismatch".into(),
    });

    let result = JsFuture::from(init_promise).await;
    assert!(result.is_err(), "init should reject on Error");
    let err_str = result.err().and_then(|e| e.as_string()).unwrap_or_default();
    assert!(
        err_str.contains("schema mismatch"),
        "error message should propagate: {err_str}"
    );
}

#[wasm_bindgen_test]
async fn init_propagates_js_object_error_from_shim() {
    // Pre-handoff worker errors (WASM load failures, bootstrap failures inside
    // the JS shim) arrive as JS objects shaped `{type:"error", message}`
    // because the WASM module isn't yet loaded to encode a postcard wire.
    // The bridge must surface that message on the init Promise instead of
    // dropping it and timing out.
    let fw = FakeWorker::new();
    let runtime = fresh_runtime();
    let bridge = jazz_wasm::WasmWorkerBridge::attach(fw.worker(), &runtime, build_options(None))
        .expect("attach");

    let init_promise = bridge.init();

    let err_obj = Object::new();
    Reflect::set(&err_obj, &"type".into(), &"error".into()).unwrap();
    Reflect::set(
        &err_obj,
        &"message".into(),
        &"WASM load failed: HTTP 404".into(),
    )
    .unwrap();
    fw.emit_data(err_obj.into());

    let result = JsFuture::from(init_promise).await;
    assert!(result.is_err(), "init should reject on JS-object error");
    let err_str = result.err().and_then(|e| e.as_string()).unwrap_or_default();
    assert!(
        err_str.contains("WASM load failed: HTTP 404"),
        "shim error message should propagate: {err_str}"
    );
}

#[wasm_bindgen_test]
async fn init_is_memoized() {
    let fw = FakeWorker::new();
    let runtime = fresh_runtime();
    let bridge = jazz_wasm::WasmWorkerBridge::attach(fw.worker(), &runtime, build_options(None))
        .expect("attach");

    let p1 = bridge.init();
    let p2 = bridge.init();

    // Two init() calls must produce the same Promise so the second caller
    // can't drop the first's resolver. wasm-bindgen `js_sys::Promise` doesn't
    // implement `==` directly; compare via JS `===`.
    let same = js_sys::Reflect::has(&p1, &"then".into()).unwrap()
        && js_sys::Reflect::has(&p2, &"then".into()).unwrap()
        && JsValue::from(p1.clone()) == JsValue::from(p2.clone());
    assert!(same, "init() should return the same Promise");

    // Only one init JS-object posted.
    let init_count = fw
        .posted
        .borrow()
        .iter()
        .filter(|v| {
            Reflect::get(v, &"type".into())
                .ok()
                .and_then(|t| t.as_string())
                .as_deref()
                == Some("init")
        })
        .count();
    assert_eq!(init_count, 1, "init posted multiple times: {init_count}");

    fw.emit_wire(&WorkerToMainWire::InitOk {
        client_id: "c".into(),
    });
    JsFuture::from(p1).await.expect("first init resolved");
    JsFuture::from(p2).await.expect("second init resolved");
}

#[wasm_bindgen_test]
fn update_auth_emits_postcard_binary() {
    let fw = FakeWorker::new();
    let runtime = fresh_runtime();
    let bridge = jazz_wasm::WasmWorkerBridge::attach(fw.worker(), &runtime, build_options(None))
        .expect("attach");

    bridge.update_auth(Some("jwt-x".into()));

    let last = fw.last_posted_decoded();
    match last {
        Some(MainToWorkerWire::UpdateAuth { jwt_token }) => {
            assert_eq!(jwt_token.as_deref(), Some("jwt-x"));
        }
        other => panic!("expected UpdateAuth, got {other:?}"),
    }
}

#[wasm_bindgen_test]
fn peer_sync_fires_listener() {
    let fw = FakeWorker::new();
    let runtime = fresh_runtime();
    let bridge = jazz_wasm::WasmWorkerBridge::attach(fw.worker(), &runtime, build_options(None))
        .expect("attach");

    let captured = Rc::new(RefCell::new(Vec::<(String, u32, usize)>::new()));
    let captured_clone = Rc::clone(&captured);
    let on_peer = Closure::<dyn FnMut(JsValue)>::new(move |batch: JsValue| {
        let peer_id = Reflect::get(&batch, &"peerId".into())
            .ok()
            .and_then(|v| v.as_string())
            .unwrap_or_default();
        let term = Reflect::get(&batch, &"term".into())
            .ok()
            .and_then(|v| v.as_f64())
            .unwrap_or(0.0) as u32;
        let payload = Reflect::get(&batch, &"payload".into()).unwrap();
        let arr: js_sys::Array = payload.dyn_into().unwrap();
        captured_clone
            .borrow_mut()
            .push((peer_id, term, arr.length() as usize));
    });
    let listeners = Object::new();
    Reflect::set(
        &listeners,
        &"onPeerSync".into(),
        on_peer.as_ref().unchecked_ref(),
    )
    .unwrap();
    bridge.set_listeners(listeners.into());

    fw.emit_wire(&WorkerToMainWire::PeerSync {
        peer_id: "tab-b".into(),
        term: 7,
        payloads: vec![ByteBuf::from(vec![1, 2, 3])],
    });

    let captured = captured.borrow();
    assert_eq!(captured.len(), 1, "listener fired count");
    assert_eq!(captured[0], ("tab-b".to_string(), 7, 1));
    drop(on_peer);
}

#[wasm_bindgen_test]
async fn shutdown_resolves_on_ack() {
    let fw = FakeWorker::new();
    let runtime = fresh_runtime();
    let bridge = jazz_wasm::WasmWorkerBridge::attach(fw.worker(), &runtime, build_options(None))
        .expect("attach");

    let shutdown_promise = bridge.shutdown();

    // A binary `Shutdown` envelope must have been posted to the worker.
    let last = fw.last_posted_decoded();
    assert!(matches!(last, Some(MainToWorkerWire::Shutdown)));

    fw.emit_wire(&WorkerToMainWire::ShutdownOk);
    JsFuture::from(shutdown_promise)
        .await
        .expect("shutdown ack");
}

#[wasm_bindgen_test]
async fn shutdown_during_init_blocks_stale_init_ok_from_worker() {
    // Worker-host race: if `Shutdown` is buffered while the host is still
    // `Initializing`, `drain_pending_messages` runs `handle_shutdown` before
    // `run_init` would post `InitOk`. The worker host now bails before that
    // stale post (see worker_host.rs:367 — `HOST.is_none()` guard), but the
    // main-side bridge must remain defensive against the same race so a
    // regression on either side can't flip the bridge back to `Ready` on a
    // worker that has already closed.
    //
    // Defense being tested: after `ShutdownOk`, the bridge clears
    // `worker.onmessage` so any late binary post from a not-yet-bailed
    // worker is dropped at the slot. State guards in `transition_init_ok`
    // (`state == Initializing`) provide a second layer.
    let fw = FakeWorker::new();
    let runtime = fresh_runtime();
    let bridge = jazz_wasm::WasmWorkerBridge::attach(fw.worker(), &runtime, build_options(None))
        .expect("attach");

    // init() registers init_resolver and posts the init JS object. We do not
    // await its Promise — calling shutdown() in the same task simulates a
    // caller tearing down before init completed (e.g. test cleanup, unmount,
    // restart).
    let _init_promise = bridge.init();

    // shutdown() synchronously transitions state Initializing -> ShuttingDown
    // and posts `Shutdown` to the worker.
    let shutdown_promise = bridge.shutdown();

    // Worker acks. dispatch_wire fires shutdown_resolver; the future then
    // calls `worker.set_onmessage(None)` and transitions to Disposed.
    fw.emit_wire(&WorkerToMainWire::ShutdownOk);
    JsFuture::from(shutdown_promise)
        .await
        .expect("shutdown ack");

    // Primary defense: worker.onmessage is cleared. The FakeWorker mirrors
    // a real Worker — set_onmessage(None) clears the property. Any stale
    // `InitOk` the worker happened to post would land on a non-callable
    // slot and be dropped. (wasm-bindgen represents the cleared slot as
    // either `null` or `undefined`; both are "not a Function".)
    let onmessage_after = Reflect::get(&fw.obj, &"onmessage".into()).unwrap();
    assert!(
        onmessage_after.is_null() || onmessage_after.is_undefined(),
        "bridge.shutdown() must clear worker.onmessage to suppress stale messages, got {onmessage_after:?}"
    );

    // Defense in depth: disposed state silently drops main-originated control
    // messages — no fresh posts after shutdown completed.
    let posts_before = fw.posted.borrow().len();
    bridge.update_auth(Some("ignored".into()));
    bridge.send_lifecycle_hint("visibility-hidden");
    assert_eq!(
        fw.posted.borrow().len(),
        posts_before,
        "Disposed bridge must not post new messages"
    );
}

#[wasm_bindgen_test]
fn lifecycle_hint_emits_postcard_binary() {
    let fw = FakeWorker::new();
    let runtime = fresh_runtime();
    let bridge = jazz_wasm::WasmWorkerBridge::attach(fw.worker(), &runtime, build_options(None))
        .expect("attach");

    bridge.send_lifecycle_hint("visibility-hidden");

    let last = fw.last_posted_decoded();
    match last {
        Some(MainToWorkerWire::LifecycleHint { event, .. }) => {
            assert!(matches!(
                event,
                jazz_wasm::worker_protocol::WorkerLifecycleEvent::VisibilityHidden
            ));
        }
        other => panic!("expected LifecycleHint, got {other:?}"),
    }
}

#[wasm_bindgen_test]
fn unknown_inbound_js_object_is_dropped_quietly() {
    // A stray JS object with an unrecognised `type` field shouldn't blow up
    // — it should just be logged-and-dropped. No assertion beyond "did not
    // panic".
    let fw = FakeWorker::new();
    let runtime = fresh_runtime();
    let _bridge = jazz_wasm::WasmWorkerBridge::attach(fw.worker(), &runtime, build_options(None))
        .expect("attach");

    let stray = Object::new();
    Reflect::set(&stray, &"type".into(), &"some-future-message".into()).unwrap();
    fw.emit_data(stray.into());
}

#[wasm_bindgen_test]
fn main_to_worker_main_side_sync_envelope() {
    // Sanity: a hand-encoded `MainToWorkerWire::Sync` round-trips through
    // `encode_main_to_worker` and the wire decoders. Lock the side-encoding
    // contract independently of any send-path test.
    let bytes = encode_main_to_worker(&MainToWorkerWire::Sync {
        payloads: vec![ByteBuf::from(vec![9])],
    })
    .expect("encode");
    let decoded: MainToWorkerWire = postcard::from_bytes(&bytes).expect("decode");
    match decoded {
        MainToWorkerWire::Sync { payloads } => {
            assert_eq!(payloads.len(), 1);
            assert_eq!(&*payloads[0], &[9]);
        }
        other => panic!("expected Sync, got {other:?}"),
    }
}

// =============================================================================
// Async helpers
// =============================================================================

/// Yield once through `setTimeout(0)` so any `spawn_local`/microtask flushes
/// scheduled by the bridge or runtime get a chance to run before assertions.
async fn yield_once() {
    let promise = js_sys::Promise::new(&mut |resolve, _reject| {
        let global = js_sys::global();
        let set_timeout: Function = Reflect::get(&global, &"setTimeout".into())
            .unwrap()
            .unchecked_into();
        let _ = set_timeout.call2(&JsValue::NULL, &resolve, &JsValue::from_f64(0.0));
    });
    JsFuture::from(promise).await.expect("yield");
}

// =============================================================================
// Wire-format trio for the peer-channel API
// =============================================================================

#[wasm_bindgen_test]
fn peer_open_send_close_emit_postcard_binary() {
    let fw = FakeWorker::new();
    let runtime = fresh_runtime();
    let bridge = jazz_wasm::WasmWorkerBridge::attach(fw.worker(), &runtime, build_options(None))
        .expect("attach");

    bridge.open_peer("peer-α");
    let last = fw.last_posted_decoded();
    match last {
        Some(MainToWorkerWire::PeerOpen { peer_id }) => {
            assert_eq!(peer_id, "peer-α");
        }
        other => panic!("expected PeerOpen, got {other:?}"),
    }

    let payload_array = js_sys::Array::new();
    payload_array.push(&Uint8Array::from(&[1u8, 2, 3][..]));
    payload_array.push(&Uint8Array::from(&[4u8][..]));
    bridge.send_peer_sync("peer-α", 5, payload_array);
    let last = fw.last_posted_decoded();
    match last {
        Some(MainToWorkerWire::PeerSync {
            peer_id,
            term,
            payloads,
        }) => {
            assert_eq!(peer_id, "peer-α");
            assert_eq!(term, 5);
            assert_eq!(payloads.len(), 2);
            assert_eq!(&*payloads[0], &[1, 2, 3]);
            assert_eq!(&*payloads[1], &[4]);
        }
        other => panic!("expected PeerSync, got {other:?}"),
    }

    bridge.close_peer("peer-α");
    let last = fw.last_posted_decoded();
    match last {
        Some(MainToWorkerWire::PeerClose { peer_id }) => {
            assert_eq!(peer_id, "peer-α");
        }
        other => panic!("expected PeerClose, got {other:?}"),
    }
}

#[wasm_bindgen_test]
fn send_peer_sync_drops_empty_payload() {
    let fw = FakeWorker::new();
    let runtime = fresh_runtime();
    let bridge = jazz_wasm::WasmWorkerBridge::attach(fw.worker(), &runtime, build_options(None))
        .expect("attach");
    let posted_before = fw.posted.borrow().len();
    bridge.send_peer_sync("p", 0, js_sys::Array::new());
    assert_eq!(
        fw.posted.borrow().len(),
        posted_before,
        "empty payload should not post"
    );
}

#[wasm_bindgen_test]
fn runtime_mutation_error_emission_acknowledges_and_forwards_to_worker() {
    let fw = FakeWorker::new();
    let runtime = fresh_runtime();
    let _bridge = jazz_wasm::WasmWorkerBridge::attach(fw.worker(), &runtime, build_options(None))
        .expect("attach");
    let batch_id = "00000000000000000000000000000008";

    let seen = Rc::new(RefCell::new(false));
    let seen_clone = Rc::clone(&seen);
    let on_mutation_error = Closure::<dyn FnMut(JsValue)>::new(move |_event: JsValue| {
        *seen_clone.borrow_mut() = true;
    });
    runtime.on_mutation_error(
        on_mutation_error
            .as_ref()
            .unchecked_ref::<Function>()
            .clone(),
    );

    runtime
        .replay_batch_rejection(batch_id, "rejected", "server denied write")
        .expect("replay rejected batch");
    let posted_before = fw.posted.borrow().len();

    runtime.batched_tick();

    assert!(
        *seen.borrow(),
        "mutation error callback should receive event"
    );
    let posted = fw.posted_decoded();
    assert!(
        posted[posted_before..].iter().any(|wire| matches!(
            wire,
            MainToWorkerWire::AcknowledgeRejectedBatch { batch_id: posted_batch_id }
                if posted_batch_id == batch_id
        )),
        "mutation error emission should acknowledge through the bridge, got {posted:?}"
    );
    drop(on_mutation_error);
}

#[wasm_bindgen_test]
fn disconnect_and_reconnect_upstream_emit_postcard_binary() {
    let fw = FakeWorker::new();
    let runtime = fresh_runtime();
    let bridge = jazz_wasm::WasmWorkerBridge::attach(fw.worker(), &runtime, build_options(None))
        .expect("attach");

    bridge.disconnect_upstream();
    assert!(matches!(
        fw.last_posted_decoded(),
        Some(MainToWorkerWire::DisconnectUpstream)
    ));

    bridge.reconnect_upstream();
    assert!(matches!(
        fw.last_posted_decoded(),
        Some(MainToWorkerWire::ReconnectUpstream)
    ));
}

// =============================================================================
// Forwarder + upstream wait gate
// =============================================================================

#[wasm_bindgen_test]
async fn forwarder_routes_server_bound_through_callback() {
    let fw = FakeWorker::new();
    let runtime = fresh_runtime();
    let bridge = jazz_wasm::WasmWorkerBridge::attach(fw.worker(), &runtime, build_options(None))
        .expect("attach");

    // Complete init so the gate opens and any later flushes go via the wire.
    let init = bridge.init();
    fw.emit_wire(&WorkerToMainWire::InitOk {
        client_id: "c1".into(),
    });
    JsFuture::from(init).await.expect("init resolved");

    // Install a forwarder. From now on, any server-bound outbox traffic the
    // runtime emits goes to the callback, not to the worker.
    let captured = Rc::new(RefCell::new(Vec::<Vec<u8>>::new()));
    let captured_clone = Rc::clone(&captured);
    let forwarder = Closure::<dyn FnMut(JsValue)>::new(move |payload: JsValue| {
        if let Some(arr) = payload.dyn_ref::<Uint8Array>() {
            captured_clone.borrow_mut().push(arr.to_vec());
        }
    });
    bridge
        .set_server_payload_forwarder(Some(forwarder.as_ref().unchecked_ref::<Function>().clone()));

    // `replayServerConnection` re-runs `removeServer` + `addServer`, which
    // emits catalogue server-bound traffic through the runtime's outbox.
    // With the forwarder installed, those payloads land in `captured`.
    let posted_count_before = fw.posted.borrow().len();
    bridge.replay_server_connection();
    yield_once().await;
    let posted_count_after = fw.posted.borrow().len();

    assert!(
        !captured.borrow().is_empty(),
        "forwarder did not receive any server-bound payloads"
    );
    assert_eq!(
        posted_count_before, posted_count_after,
        "forwarder install should have suppressed worker postMessage"
    );

    // Removing the forwarder routes server-bound traffic back through the
    // worker again.
    bridge.set_server_payload_forwarder(None);
    bridge.replay_server_connection();
    yield_once().await;
    assert!(
        fw.posted.borrow().len() > posted_count_after,
        "after forwarder removal, server-bound should reach the worker"
    );

    drop(forwarder);
}

#[wasm_bindgen_test]
async fn wait_for_upstream_short_circuits_without_server_url() {
    let fw = FakeWorker::new();
    let runtime = fresh_runtime();
    let bridge = jazz_wasm::WasmWorkerBridge::attach(fw.worker(), &runtime, build_options(None))
        .expect("attach");

    // No `serverUrl` in options → `expects_upstream` is false → resolves
    // immediately.
    bridge
        .wait_for_upstream_server_connection()
        .await
        .expect("resolves immediately");
}

#[wasm_bindgen_test]
async fn wait_for_upstream_resolves_on_connected_message() {
    let fw = FakeWorker::new();
    let runtime = fresh_runtime();
    let bridge = jazz_wasm::WasmWorkerBridge::attach(
        fw.worker(),
        &runtime,
        build_options(Some("https://example.test")),
    )
    .expect("attach");

    let init = bridge.init();
    fw.emit_wire(&WorkerToMainWire::InitOk {
        client_id: "c1".into(),
    });
    JsFuture::from(init).await.expect("init resolved");

    // expects_upstream = true (serverUrl set), upstream not yet connected.
    // wait should block until we emit `UpstreamConnected`.
    let waiter = bridge.wait_for_upstream_server_connection();
    fw.emit_wire(&WorkerToMainWire::UpstreamConnected);
    waiter.await.expect("wait resolved");
}

#[wasm_bindgen_test]
async fn wait_for_upstream_short_circuits_when_forwarder_installed() {
    let fw = FakeWorker::new();
    let runtime = fresh_runtime();
    let bridge = jazz_wasm::WasmWorkerBridge::attach(
        fw.worker(),
        &runtime,
        build_options(Some("https://example.test")),
    )
    .expect("attach");

    // Forwarder install marks upstream effectively ready. wait should resolve
    // even though no upstream-connected message has arrived.
    let forwarder = Closure::<dyn FnMut(JsValue)>::new(move |_payload: JsValue| {});
    bridge
        .set_server_payload_forwarder(Some(forwarder.as_ref().unchecked_ref::<Function>().clone()));

    bridge
        .wait_for_upstream_server_connection()
        .await
        .expect("forwarder resolves");
    drop(forwarder);
}

// =============================================================================
// Listener slots
// =============================================================================

#[wasm_bindgen_test]
fn auth_failed_fires_listener() {
    let fw = FakeWorker::new();
    let runtime = fresh_runtime();
    let bridge = jazz_wasm::WasmWorkerBridge::attach(fw.worker(), &runtime, build_options(None))
        .expect("attach");

    let captured = Rc::new(RefCell::new(Vec::<String>::new()));
    let captured_clone = Rc::clone(&captured);
    let on_auth = Closure::<dyn FnMut(JsValue)>::new(move |reason: JsValue| {
        captured_clone
            .borrow_mut()
            .push(reason.as_string().unwrap_or_default());
    });
    let listeners = Object::new();
    Reflect::set(
        &listeners,
        &"onAuthFailure".into(),
        on_auth.as_ref().unchecked_ref(),
    )
    .unwrap();
    bridge.set_listeners(listeners.into());

    fw.emit_wire(&WorkerToMainWire::AuthFailed {
        reason: "expired".into(),
    });

    assert_eq!(captured.borrow().as_slice(), &["expired".to_string()]);
    drop(on_auth);
}

#[wasm_bindgen_test]
fn local_batch_records_sync_hydrates_main_runtime() {
    let fw = FakeWorker::new();
    let runtime = fresh_runtime();
    let _bridge = jazz_wasm::WasmWorkerBridge::attach(fw.worker(), &runtime, build_options(None))
        .expect("attach");

    let batch_id = BatchId::new();
    let record = LocalBatchRecord::new(
        batch_id,
        BatchMode::Direct,
        true,
        Some(BatchFate::DurableDirect {
            batch_id,
            confirmed_tier: DurabilityTier::GlobalServer,
        }),
    );
    let encoded_record = record.encode_storage_row().expect("encode record");

    fw.emit_wire(&WorkerToMainWire::LocalBatchRecordsSync {
        encoded_records: vec![ByteBuf::from(encoded_record)],
    });

    let fate = runtime
        .load_batch_fate(&batch_id.to_string())
        .expect("load fate");
    let kind = Reflect::get(&fate, &"kind".into())
        .ok()
        .and_then(|value| value.as_string());
    let tier = Reflect::get(&fate, &"confirmedTier".into())
        .ok()
        .and_then(|value| value.as_string());
    assert_eq!(kind.as_deref(), Some("durableDirect"));
    assert_eq!(tier.as_deref(), Some("global"));
}

#[wasm_bindgen_test]
fn mutation_error_replay_hydrates_main_runtime() {
    let fw = FakeWorker::new();
    let runtime = fresh_runtime();
    let _bridge = jazz_wasm::WasmWorkerBridge::attach(fw.worker(), &runtime, build_options(None))
        .expect("attach");
    let batch_id = "00000000000000000000000000000009";

    fw.emit_wire(&WorkerToMainWire::MutationErrorReplay {
        batch_id: batch_id.into(),
        code: "rejected".into(),
        reason: "boom".into(),
    });

    let fate = runtime.load_batch_fate(batch_id).expect("load fate");
    let kind = Reflect::get(&fate, &"kind".into())
        .ok()
        .and_then(|value| value.as_string());
    let code = Reflect::get(&fate, &"code".into())
        .ok()
        .and_then(|value| value.as_string());
    assert_eq!(kind.as_deref(), Some("rejected"));
    assert_eq!(code.as_deref(), Some("rejected"));
}

// =============================================================================
// Pre-init outbox buffering
// =============================================================================

#[wasm_bindgen_test]
async fn pre_init_outbox_traffic_is_buffered_until_init_ok() {
    let fw = FakeWorker::new();
    let runtime = fresh_runtime();
    let bridge = jazz_wasm::WasmWorkerBridge::attach(fw.worker(), &runtime, build_options(None))
        .expect("attach");

    // attach() calls `runtime.add_server(None, Some(1.0))`, which fires a
    // synchronous batched_tick. Catalogue traffic from the schema gets emitted
    // into the outbox sender — which has its init-gate closed and so just
    // accumulates without scheduling a flush.
    yield_once().await;

    // Before init, the only thing the worker should have seen is the init
    // JS object. No binary `Sync` envelope yet.
    let pre_init_binaries = fw.posted_decoded();
    assert!(
        pre_init_binaries.is_empty(),
        "pre-init binary posts: {pre_init_binaries:?}"
    );

    // Now drive init to completion — the bridge opens the gate and flushes
    // the accumulated entries.
    let init = bridge.init();
    fw.emit_wire(&WorkerToMainWire::InitOk {
        client_id: "c1".into(),
    });
    JsFuture::from(init).await.expect("init resolved");
    yield_once().await;

    let post_init_syncs: Vec<MainToWorkerWire> = fw
        .posted_decoded()
        .into_iter()
        .filter(|m| matches!(m, MainToWorkerWire::Sync { .. }))
        .collect();
    assert!(
        !post_init_syncs.is_empty(),
        "init-ok did not flush a binary Sync envelope; posted_decoded={:?}",
        fw.posted_decoded()
    );
}

// =============================================================================
// Misc lifecycle
// =============================================================================

#[wasm_bindgen_test]
fn ready_js_object_does_not_break_dispatch() {
    // The worker's JS shim posts `{type:"ready"}` early. Bridge must accept
    // it (treated as a no-op) and not panic or surface as an error.
    let fw = FakeWorker::new();
    let runtime = fresh_runtime();
    let _bridge = jazz_wasm::WasmWorkerBridge::attach(fw.worker(), &runtime, build_options(None))
        .expect("attach");

    let ready = Object::new();
    Reflect::set(&ready, &"type".into(), &"ready".into()).unwrap();
    fw.emit_data(ready.into());
}

#[wasm_bindgen_test]
async fn upstream_disconnected_rearms_wait() {
    let fw = FakeWorker::new();
    let runtime = fresh_runtime();
    let bridge = jazz_wasm::WasmWorkerBridge::attach(
        fw.worker(),
        &runtime,
        build_options(Some("https://example.test")),
    )
    .expect("attach");

    let init = bridge.init();
    fw.emit_wire(&WorkerToMainWire::InitOk {
        client_id: "c1".into(),
    });
    JsFuture::from(init).await.expect("init resolved");
    fw.emit_wire(&WorkerToMainWire::UpstreamConnected);

    // Connected — wait should resolve immediately.
    bridge
        .wait_for_upstream_server_connection()
        .await
        .expect("connected resolves");

    // Now disconnect — wait should re-arm and block.
    fw.emit_wire(&WorkerToMainWire::UpstreamDisconnected);
    let waiter = bridge.wait_for_upstream_server_connection();
    fw.emit_wire(&WorkerToMainWire::UpstreamConnected);
    waiter.await.expect("re-arm resolves");
}

// =============================================================================
// Drop after explicit shutdown
// =============================================================================

/// Regression test for a silent-failure path in `Db.restartWorkerWithCurrentDbName`:
/// the wasm-bindgen wrapper for a shut-down bridge sticks around until
/// `FinalizationRegistry` fires; if a successor bridge has been attached to
/// the same `WasmRuntime` in the meantime, the old `Drop` impl must not
/// reach back into the shared runtime and clobber the new sender / server
/// edge.
#[wasm_bindgen_test]
async fn drop_after_shutdown_leaves_successor_bridge_intact() {
    let runtime = fresh_runtime();

    // Bridge A: attach, init, shutdown.
    let fw_a = FakeWorker::new();
    let bridge_a =
        jazz_wasm::WasmWorkerBridge::attach(fw_a.worker(), &runtime, build_options(None))
            .expect("attach A");
    let init_a = bridge_a.init();
    fw_a.emit_wire(&WorkerToMainWire::InitOk {
        client_id: "c1".into(),
    });
    JsFuture::from(init_a).await.expect("init A");

    let shutdown_a = bridge_a.shutdown();
    fw_a.emit_wire(&WorkerToMainWire::ShutdownOk);
    JsFuture::from(shutdown_a).await.expect("shutdown A");

    // Bridge B: attach a fresh bridge on the SAME runtime with a different
    // worker.
    let fw_b = FakeWorker::new();
    let bridge_b =
        jazz_wasm::WasmWorkerBridge::attach(fw_b.worker(), &runtime, build_options(None))
            .expect("attach B");
    let init_b = bridge_b.init();
    fw_b.emit_wire(&WorkerToMainWire::InitOk {
        client_id: "c2".into(),
    });
    JsFuture::from(init_b).await.expect("init B");

    // Sanity: B's sender is wired — server-bound traffic from
    // `replay_server_connection` reaches B's worker.
    let posted_b_baseline = fw_b.posted.borrow().len();
    bridge_b.replay_server_connection();
    yield_once().await;
    assert!(
        fw_b.posted.borrow().len() > posted_b_baseline,
        "bridge B sender did not post after attach (test setup broken)"
    );

    // Now drop A. With the bug, A's Drop runs `install_noop_sync_sender` +
    // `remove_server` on the shared runtime, silently replacing B's sender
    // with a noop and tearing down B's server edge.
    let posted_a_before_drop = fw_a.posted.borrow().len();
    std::mem::drop(bridge_a);

    // Trigger more outbox traffic via B. It must still flow to B's worker.
    let posted_b_before_second = fw_b.posted.borrow().len();
    bridge_b.replay_server_connection();
    yield_once().await;
    assert!(
        fw_b.posted.borrow().len() > posted_b_before_second,
        "after dropping the disposed bridge A, B's outbox stopped flowing — \
         A's Drop clobbered the shared runtime's sender"
    );
    assert_eq!(
        fw_a.posted.borrow().len(),
        posted_a_before_drop,
        "outbox traffic leaked to A's (shut-down) worker after B was attached"
    );
}
