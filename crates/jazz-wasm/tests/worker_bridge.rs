//! Browser integration tests for `WasmWorkerBridge`.
//!
//! The bridge calls exactly two members on its `Worker` handle:
//! `postMessage(message, transfer?)` and `set_onmessage(handler)`. The harness
//! exposes both via a duck-typed JS object and downcasts to `web_sys::Worker`
//! via `unchecked_into`.

#![cfg(target_arch = "wasm32")]

use std::cell::RefCell;
use std::rc::Rc;

use js_sys::{Array, Function, Object, Promise, Reflect, Uint8Array};
use serde_bytes::ByteBuf;
use wasm_bindgen::closure::Closure;
use wasm_bindgen::prelude::*;
use wasm_bindgen::JsCast;
use wasm_bindgen_futures::JsFuture;
use wasm_bindgen_test::*;
use web_sys::Worker;

use jazz_wasm::worker_protocol::{
    encode_main_to_worker, encode_worker_to_main, MainToWorkerWire, SyncEntry,
    WorkerLifecycleEvent, WorkerToMainWire,
};
use jazz_wasm::{WasmRuntime, WasmWorkerBridge};

wasm_bindgen_test_configure!(run_in_browser);

// ---------------------------------------------------------------------------
// Fixtures
// ---------------------------------------------------------------------------

const SCHEMA_JSON: &str = r#"{
    "todos": {
        "columns": [
            {"name": "title",     "column_type": {"type": "Text"},    "nullable": false},
            {"name": "completed", "column_type": {"type": "Boolean"}, "nullable": false}
        ]
    }
}"#;

fn fresh_runtime() -> WasmRuntime {
    WasmRuntime::new(SCHEMA_JSON, "test-app", "dev", "main", None, Some(true))
        .expect("WasmRuntime::new")
}

fn build_options(server_url: Option<&str>) -> JsValue {
    let opts = Object::new();
    let _ = Reflect::set(&opts, &"schemaJson".into(), &SCHEMA_JSON.into());
    let _ = Reflect::set(&opts, &"appId".into(), &"test-app".into());
    let _ = Reflect::set(&opts, &"env".into(), &"dev".into());
    let _ = Reflect::set(&opts, &"userBranch".into(), &"main".into());
    let _ = Reflect::set(&opts, &"dbName".into(), &"db".into());
    if let Some(u) = server_url {
        let _ = Reflect::set(&opts, &"serverUrl".into(), &u.into());
    }
    opts.into()
}

struct FakeWorker {
    obj: JsValue,
    posted: Rc<RefCell<Vec<JsValue>>>,
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
        let _ = Reflect::set(
            &obj,
            &"postMessage".into(),
            post_message_closure.as_ref().unchecked_ref(),
        );
        let _ = Reflect::set(&obj, &"onmessage".into(), &JsValue::NULL);
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
        let _ = Reflect::set(&event, &"data".into(), &data);
        let onmessage = Reflect::get(&self.obj, &"onmessage".into()).unwrap_or(JsValue::NULL);
        let f: Function = onmessage
            .dyn_into()
            .expect("bridge has not installed an onmessage handler");
        f.call1(&JsValue::NULL, &event.into())
            .expect("dispatch fake message");
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

    fn posted_init_count(&self) -> usize {
        self.posted
            .borrow()
            .iter()
            .filter(|v| {
                if v.dyn_ref::<Uint8Array>().is_some() {
                    return false;
                }
                Reflect::get(v, &"type".into())
                    .ok()
                    .and_then(|t| t.as_string())
                    .map(|s| s == "init")
                    .unwrap_or(false)
            })
            .count()
    }
}

async fn yield_once() {
    let promise = Promise::new(&mut |resolve, _reject| {
        let global = js_sys::global();
        let set_timeout: Function = Reflect::get(&global, &"setTimeout".into())
            .unwrap()
            .unchecked_into();
        let _ = set_timeout.call2(&JsValue::NULL, &resolve, &JsValue::from_f64(0.0));
    });
    let _ = JsFuture::from(promise).await;
}

fn js_obj_with_type(ty: &str) -> JsValue {
    let obj = Object::new();
    let _ = Reflect::set(&obj, &"type".into(), &ty.into());
    obj.into()
}

// ---------------------------------------------------------------------------
// 14.4.1 — init resolves with client id
// ---------------------------------------------------------------------------

#[wasm_bindgen_test]
async fn init_resolves_with_client_id() {
    let fw = FakeWorker::new();
    let runtime = fresh_runtime();
    let bridge =
        WasmWorkerBridge::attach(fw.worker(), &runtime, build_options(None)).expect("attach");
    let init = bridge.init();

    assert!(fw.posted_init_count() >= 1);

    fw.emit_wire(&WorkerToMainWire::InitOk {
        client_id: "client-42".into(),
    });
    let result = JsFuture::from(init).await.expect("init resolved");
    let client_id = Reflect::get(&result, &"clientId".into())
        .unwrap()
        .as_string()
        .unwrap();
    assert_eq!(client_id, "client-42");
    assert_eq!(
        bridge.get_worker_client_id().as_string().as_deref(),
        Some("client-42")
    );
}

// ---------------------------------------------------------------------------
// 14.4.2 — init propagates error
// ---------------------------------------------------------------------------

#[wasm_bindgen_test]
async fn init_propagates_error() {
    let fw = FakeWorker::new();
    let runtime = fresh_runtime();
    let bridge =
        WasmWorkerBridge::attach(fw.worker(), &runtime, build_options(None)).expect("attach");
    let init = bridge.init();
    fw.emit_wire(&WorkerToMainWire::Error {
        message: "schema mismatch".into(),
    });
    let err = JsFuture::from(init).await.expect_err("init rejected");
    let msg = err.as_string().unwrap_or_else(|| format!("{err:?}"));
    assert!(msg.contains("schema mismatch"), "got: {msg}");
}

// ---------------------------------------------------------------------------
// 14.4.3 — init propagates JS-object error from shim
// ---------------------------------------------------------------------------

#[wasm_bindgen_test]
async fn init_propagates_js_object_error_from_shim() {
    let fw = FakeWorker::new();
    let runtime = fresh_runtime();
    let bridge =
        WasmWorkerBridge::attach(fw.worker(), &runtime, build_options(None)).expect("attach");
    let init = bridge.init();

    let err_obj = Object::new();
    let _ = Reflect::set(&err_obj, &"type".into(), &"error".into());
    let _ = Reflect::set(
        &err_obj,
        &"message".into(),
        &"WASM load failed: HTTP 404".into(),
    );
    fw.emit_data(err_obj.into());

    let err = JsFuture::from(init).await.expect_err("init rejected");
    let msg = err.as_string().unwrap_or_else(|| format!("{err:?}"));
    assert!(msg.contains("WASM load failed: HTTP 404"), "got: {msg}");
}

// ---------------------------------------------------------------------------
// 14.4.4 — init is memoised
// ---------------------------------------------------------------------------

#[wasm_bindgen_test]
async fn init_is_memoized() {
    let fw = FakeWorker::new();
    let runtime = fresh_runtime();
    let bridge =
        WasmWorkerBridge::attach(fw.worker(), &runtime, build_options(None)).expect("attach");

    let p1 = bridge.init();
    let p2 = bridge.init();
    assert!(JsValue::from(p1.clone()).eq(&JsValue::from(p2.clone())));
    assert_eq!(fw.posted_init_count(), 1);

    fw.emit_wire(&WorkerToMainWire::InitOk {
        client_id: "c".into(),
    });
    JsFuture::from(p1).await.expect("p1 resolves");
    JsFuture::from(p2).await.expect("p2 resolves");
}

// ---------------------------------------------------------------------------
// 14.4.5 — update_auth emits postcard binary
// ---------------------------------------------------------------------------

#[wasm_bindgen_test]
fn update_auth_emits_postcard_binary() {
    let fw = FakeWorker::new();
    let runtime = fresh_runtime();
    let bridge =
        WasmWorkerBridge::attach(fw.worker(), &runtime, build_options(None)).expect("attach");

    bridge.update_auth(Some("jwt-x".into()));
    let last = fw.last_posted_decoded();
    match last {
        Some(MainToWorkerWire::UpdateAuth { jwt_token }) => {
            assert_eq!(jwt_token.as_deref(), Some("jwt-x"));
        }
        other => panic!("expected UpdateAuth, got {other:?}"),
    }
}

// ---------------------------------------------------------------------------
// 14.4.6 — peer-sync inbound fires listener
// ---------------------------------------------------------------------------

#[wasm_bindgen_test]
fn peer_sync_fires_listener() {
    let fw = FakeWorker::new();
    let runtime = fresh_runtime();
    let bridge =
        WasmWorkerBridge::attach(fw.worker(), &runtime, build_options(None)).expect("attach");

    let captured: Rc<RefCell<Vec<(String, u32, u32)>>> = Rc::new(RefCell::new(Vec::new()));
    let captured_clone = Rc::clone(&captured);
    let cb = Closure::<dyn FnMut(JsValue)>::new(move |value: JsValue| {
        let peer = Reflect::get(&value, &"peerId".into())
            .unwrap()
            .as_string()
            .unwrap();
        let term = Reflect::get(&value, &"term".into())
            .unwrap()
            .as_f64()
            .unwrap() as u32;
        let payload: Array = Reflect::get(&value, &"payload".into())
            .unwrap()
            .unchecked_into();
        captured_clone
            .borrow_mut()
            .push((peer, term, payload.length()));
    });
    let listeners = Object::new();
    let _ = Reflect::set(
        &listeners,
        &"onPeerSync".into(),
        cb.as_ref().unchecked_ref(),
    );
    bridge.set_listeners(listeners.into());
    cb.forget();

    fw.emit_wire(&WorkerToMainWire::PeerSync {
        peer_id: "tab-b".into(),
        term: 7,
        payloads: vec![ByteBuf::from(vec![1, 2, 3])],
    });
    let g = captured.borrow();
    assert_eq!(g.len(), 1);
    assert_eq!(g[0], ("tab-b".to_string(), 7, 1));
}

// ---------------------------------------------------------------------------
// 14.4.7 — shutdown resolves on ack
// ---------------------------------------------------------------------------

#[wasm_bindgen_test]
async fn shutdown_resolves_on_ack() {
    let fw = FakeWorker::new();
    let runtime = fresh_runtime();
    let bridge =
        WasmWorkerBridge::attach(fw.worker(), &runtime, build_options(None)).expect("attach");
    let shutdown = bridge.shutdown();
    assert_eq!(fw.last_posted_decoded(), Some(MainToWorkerWire::Shutdown));
    fw.emit_wire(&WorkerToMainWire::ShutdownOk);
    JsFuture::from(shutdown).await.expect("shutdown ack");
}

// ---------------------------------------------------------------------------
// 14.4.8 — lifecycle hint emits postcard binary
// ---------------------------------------------------------------------------

#[wasm_bindgen_test]
fn lifecycle_hint_emits_postcard_binary() {
    let fw = FakeWorker::new();
    let runtime = fresh_runtime();
    let bridge =
        WasmWorkerBridge::attach(fw.worker(), &runtime, build_options(None)).expect("attach");
    bridge.send_lifecycle_hint("visibility-hidden");
    match fw.last_posted_decoded() {
        Some(MainToWorkerWire::LifecycleHint { event, .. }) => {
            assert_eq!(event, WorkerLifecycleEvent::VisibilityHidden);
        }
        other => panic!("expected LifecycleHint, got {other:?}"),
    }
}

// ---------------------------------------------------------------------------
// 14.4.9 — unknown inbound JS object dropped quietly
// ---------------------------------------------------------------------------

#[wasm_bindgen_test]
fn unknown_inbound_js_object_is_dropped_quietly() {
    let fw = FakeWorker::new();
    let runtime = fresh_runtime();
    let _bridge =
        WasmWorkerBridge::attach(fw.worker(), &runtime, build_options(None)).expect("attach");
    fw.emit_data(js_obj_with_type("some-future-message"));
}

// ---------------------------------------------------------------------------
// 14.4.10 — main→worker Sync envelope wire shape
// ---------------------------------------------------------------------------

#[wasm_bindgen_test]
fn main_to_worker_main_side_sync_envelope() {
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

// ---------------------------------------------------------------------------
// 14.4.11 — peer open/send/close emit postcard binary
// ---------------------------------------------------------------------------

#[wasm_bindgen_test]
fn peer_open_send_close_emit_postcard_binary() {
    let fw = FakeWorker::new();
    let runtime = fresh_runtime();
    let bridge =
        WasmWorkerBridge::attach(fw.worker(), &runtime, build_options(None)).expect("attach");

    bridge.open_peer("peer-α");
    match fw.last_posted_decoded() {
        Some(MainToWorkerWire::PeerOpen { peer_id }) => assert_eq!(peer_id, "peer-α"),
        other => panic!("expected PeerOpen, got {other:?}"),
    }

    let arr = Array::new();
    arr.push(&Uint8Array::from(&[1u8, 2, 3][..]).into());
    arr.push(&Uint8Array::from(&[4u8][..]).into());
    bridge.send_peer_sync("peer-α", 5, arr);
    match fw.last_posted_decoded() {
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
    match fw.last_posted_decoded() {
        Some(MainToWorkerWire::PeerClose { peer_id }) => assert_eq!(peer_id, "peer-α"),
        other => panic!("expected PeerClose, got {other:?}"),
    }
}

// ---------------------------------------------------------------------------
// 14.4.12 — empty peer-sync payload is dropped
// ---------------------------------------------------------------------------

#[wasm_bindgen_test]
fn send_peer_sync_drops_empty_payload() {
    let fw = FakeWorker::new();
    let runtime = fresh_runtime();
    let bridge =
        WasmWorkerBridge::attach(fw.worker(), &runtime, build_options(None)).expect("attach");
    let before = fw.posted.borrow().len();
    bridge.send_peer_sync("p", 0, Array::new());
    assert_eq!(fw.posted.borrow().len(), before);
}

// ---------------------------------------------------------------------------
// 14.4.13 — acknowledge_rejected_batch emits postcard binary
// ---------------------------------------------------------------------------

#[wasm_bindgen_test]
fn acknowledge_rejected_batch_emits_postcard_binary() {
    let fw = FakeWorker::new();
    let runtime = fresh_runtime();
    let bridge =
        WasmWorkerBridge::attach(fw.worker(), &runtime, build_options(None)).expect("attach");
    bridge.acknowledge_rejected_batch("batch-7");
    match fw.last_posted_decoded() {
        Some(MainToWorkerWire::AcknowledgeRejectedBatch { batch_id }) => {
            assert_eq!(batch_id, "batch-7");
        }
        other => panic!("expected AcknowledgeRejectedBatch, got {other:?}"),
    }
}

// ---------------------------------------------------------------------------
// 14.4.14 — disconnect/reconnect upstream emit postcard binary
// ---------------------------------------------------------------------------

#[wasm_bindgen_test]
fn disconnect_and_reconnect_upstream_emit_postcard_binary() {
    let fw = FakeWorker::new();
    let runtime = fresh_runtime();
    let bridge =
        WasmWorkerBridge::attach(fw.worker(), &runtime, build_options(None)).expect("attach");
    bridge.disconnect_upstream();
    assert_eq!(
        fw.last_posted_decoded(),
        Some(MainToWorkerWire::DisconnectUpstream)
    );
    bridge.reconnect_upstream();
    assert_eq!(
        fw.last_posted_decoded(),
        Some(MainToWorkerWire::ReconnectUpstream)
    );
}

// ---------------------------------------------------------------------------
// 14.4.15 — forwarder routes server-bound through callback
// ---------------------------------------------------------------------------

#[wasm_bindgen_test]
async fn forwarder_routes_server_bound_through_callback() {
    let fw = FakeWorker::new();
    let runtime = fresh_runtime();
    let bridge =
        WasmWorkerBridge::attach(fw.worker(), &runtime, build_options(None)).expect("attach");
    let init = bridge.init();
    fw.emit_wire(&WorkerToMainWire::InitOk {
        client_id: "c".into(),
    });
    JsFuture::from(init).await.expect("init resolved");

    let captured: Rc<RefCell<Vec<Vec<u8>>>> = Rc::new(RefCell::new(Vec::new()));
    let captured_clone = Rc::clone(&captured);
    let cb = Closure::<dyn FnMut(JsValue)>::new(move |payload: JsValue| {
        if let Some(arr) = payload.dyn_ref::<Uint8Array>() {
            captured_clone.borrow_mut().push(arr.to_vec());
        }
    });
    bridge.set_server_payload_forwarder(Some(cb.as_ref().unchecked_ref::<Function>().clone()));
    cb.forget();

    let posted_before = fw.posted.borrow().len();
    bridge.replay_server_connection();
    yield_once().await;
    assert!(!captured.borrow().is_empty(), "forwarder received nothing");
    assert_eq!(fw.posted.borrow().len(), posted_before);

    bridge.set_server_payload_forwarder(None);
    bridge.replay_server_connection();
    yield_once().await;
    assert!(
        fw.posted.borrow().len() > posted_before,
        "worker should receive traffic after forwarder removed"
    );
}

// ---------------------------------------------------------------------------
// 14.4.16 — wait short-circuits without serverUrl
// ---------------------------------------------------------------------------

#[wasm_bindgen_test]
async fn wait_for_upstream_short_circuits_without_server_url() {
    let fw = FakeWorker::new();
    let runtime = fresh_runtime();
    let bridge =
        WasmWorkerBridge::attach(fw.worker(), &runtime, build_options(None)).expect("attach");
    bridge
        .wait_for_upstream_server_connection()
        .await
        .expect("resolves immediately");
}

// ---------------------------------------------------------------------------
// 14.4.17 — wait resolves on Connected
// ---------------------------------------------------------------------------

#[wasm_bindgen_test]
async fn wait_for_upstream_resolves_on_connected_message() {
    let fw = FakeWorker::new();
    let runtime = fresh_runtime();
    let bridge = WasmWorkerBridge::attach(
        fw.worker(),
        &runtime,
        build_options(Some("https://example.test")),
    )
    .expect("attach");
    let init = bridge.init();
    fw.emit_wire(&WorkerToMainWire::InitOk {
        client_id: "c".into(),
    });
    JsFuture::from(init).await.expect("init resolved");

    let waiter = bridge.wait_for_upstream_server_connection();
    fw.emit_wire(&WorkerToMainWire::UpstreamConnected);
    waiter.await.expect("wait resolved");
}

// ---------------------------------------------------------------------------
// 14.4.18 — wait short-circuits when forwarder installed
// ---------------------------------------------------------------------------

#[wasm_bindgen_test]
async fn wait_for_upstream_short_circuits_when_forwarder_installed() {
    let fw = FakeWorker::new();
    let runtime = fresh_runtime();
    let bridge = WasmWorkerBridge::attach(
        fw.worker(),
        &runtime,
        build_options(Some("https://example.test")),
    )
    .expect("attach");
    let noop = Closure::<dyn FnMut(JsValue)>::new(|_| {});
    bridge.set_server_payload_forwarder(Some(noop.as_ref().unchecked_ref::<Function>().clone()));
    noop.forget();
    bridge
        .wait_for_upstream_server_connection()
        .await
        .expect("forwarder resolves");
}

// ---------------------------------------------------------------------------
// 14.4.19 — auth-failed fires listener
// ---------------------------------------------------------------------------

#[wasm_bindgen_test]
fn auth_failed_fires_listener() {
    let fw = FakeWorker::new();
    let runtime = fresh_runtime();
    let bridge =
        WasmWorkerBridge::attach(fw.worker(), &runtime, build_options(None)).expect("attach");
    let captured: Rc<RefCell<Vec<String>>> = Rc::new(RefCell::new(Vec::new()));
    let captured_clone = Rc::clone(&captured);
    let cb = Closure::<dyn FnMut(JsValue)>::new(move |reason: JsValue| {
        captured_clone
            .borrow_mut()
            .push(reason.as_string().unwrap_or_default());
    });
    let listeners = Object::new();
    let _ = Reflect::set(
        &listeners,
        &"onAuthFailure".into(),
        cb.as_ref().unchecked_ref(),
    );
    bridge.set_listeners(listeners.into());
    cb.forget();

    fw.emit_wire(&WorkerToMainWire::AuthFailed {
        reason: "expired".into(),
    });
    assert_eq!(*captured.borrow(), vec!["expired".to_string()]);
}

// ---------------------------------------------------------------------------
// 14.4.20 — local batch records sync listener decodes JSON
// ---------------------------------------------------------------------------

#[wasm_bindgen_test]
fn local_batch_records_sync_listener_decodes_json() {
    let fw = FakeWorker::new();
    let runtime = fresh_runtime();
    let bridge =
        WasmWorkerBridge::attach(fw.worker(), &runtime, build_options(None)).expect("attach");
    let captured: Rc<RefCell<Option<JsValue>>> = Rc::new(RefCell::new(None));
    let captured_clone = Rc::clone(&captured);
    let cb = Closure::<dyn FnMut(JsValue)>::new(move |value: JsValue| {
        *captured_clone.borrow_mut() = Some(value);
    });
    let listeners = Object::new();
    let _ = Reflect::set(
        &listeners,
        &"onLocalBatchRecordsSync".into(),
        cb.as_ref().unchecked_ref(),
    );
    bridge.set_listeners(listeners.into());
    cb.forget();

    fw.emit_wire(&WorkerToMainWire::LocalBatchRecordsSync {
        batches_json: r#"[{"batchId":"b1"}]"#.into(),
    });
    let captured = captured.borrow();
    let arr: &Array = captured
        .as_ref()
        .unwrap()
        .dyn_ref::<Array>()
        .expect("array");
    assert_eq!(arr.length(), 1);
    let batch = arr.get(0);
    let id = Reflect::get(&batch, &"batchId".into())
        .unwrap()
        .as_string()
        .unwrap();
    assert_eq!(id, "b1");
}

// ---------------------------------------------------------------------------
// 14.4.21 — mutation error replay listener decodes JSON
// ---------------------------------------------------------------------------

#[wasm_bindgen_test]
fn mutation_error_replay_listener_decodes_json() {
    let fw = FakeWorker::new();
    let runtime = fresh_runtime();
    let bridge =
        WasmWorkerBridge::attach(fw.worker(), &runtime, build_options(None)).expect("attach");
    let captured: Rc<RefCell<Option<JsValue>>> = Rc::new(RefCell::new(None));
    let captured_clone = Rc::clone(&captured);
    let cb = Closure::<dyn FnMut(JsValue)>::new(move |value: JsValue| {
        *captured_clone.borrow_mut() = Some(value);
    });
    let listeners = Object::new();
    let _ = Reflect::set(
        &listeners,
        &"onMutationErrorReplay".into(),
        cb.as_ref().unchecked_ref(),
    );
    bridge.set_listeners(listeners.into());
    cb.forget();

    fw.emit_wire(&WorkerToMainWire::MutationErrorReplay {
        batch_json: r#"{"batchId":"b9"}"#.into(),
    });
    let captured = captured.borrow();
    let value = captured.as_ref().unwrap();
    let id = Reflect::get(value, &"batchId".into())
        .unwrap()
        .as_string()
        .unwrap();
    assert_eq!(id, "b9");
}

// ---------------------------------------------------------------------------
// 14.4.22 — pre-init outbox traffic buffered until InitOk
// ---------------------------------------------------------------------------

#[wasm_bindgen_test]
async fn pre_init_outbox_traffic_is_buffered_until_init_ok() {
    let fw = FakeWorker::new();
    let runtime = fresh_runtime();
    let bridge =
        WasmWorkerBridge::attach(fw.worker(), &runtime, build_options(None)).expect("attach");
    yield_once().await;
    assert!(fw.posted_decoded().is_empty(), "no Sync envelopes pre-init");

    let init = bridge.init();
    fw.emit_wire(&WorkerToMainWire::InitOk {
        client_id: "c1".into(),
    });
    JsFuture::from(init).await.expect("init resolved");
    yield_once().await;

    let sync_count = fw
        .posted_decoded()
        .into_iter()
        .filter(|w| matches!(w, MainToWorkerWire::Sync { .. }))
        .count();
    assert!(sync_count > 0, "expected Sync after init");
}

// ---------------------------------------------------------------------------
// 14.4.23 — ready JS object does not break dispatch
// ---------------------------------------------------------------------------

#[wasm_bindgen_test]
fn ready_js_object_does_not_break_dispatch() {
    let fw = FakeWorker::new();
    let runtime = fresh_runtime();
    let _bridge =
        WasmWorkerBridge::attach(fw.worker(), &runtime, build_options(None)).expect("attach");
    fw.emit_data(js_obj_with_type("ready"));
}

// ---------------------------------------------------------------------------
// 14.4.24 — upstream disconnected re-arms wait
// ---------------------------------------------------------------------------

#[wasm_bindgen_test]
async fn upstream_disconnected_rearms_wait() {
    let fw = FakeWorker::new();
    let runtime = fresh_runtime();
    let bridge = WasmWorkerBridge::attach(
        fw.worker(),
        &runtime,
        build_options(Some("https://example.test")),
    )
    .expect("attach");
    let init = bridge.init();
    fw.emit_wire(&WorkerToMainWire::InitOk {
        client_id: "c".into(),
    });
    JsFuture::from(init).await.expect("init resolved");

    fw.emit_wire(&WorkerToMainWire::UpstreamConnected);
    bridge
        .wait_for_upstream_server_connection()
        .await
        .expect("connected resolves");

    fw.emit_wire(&WorkerToMainWire::UpstreamDisconnected);
    let waiter = bridge.wait_for_upstream_server_connection();
    fw.emit_wire(&WorkerToMainWire::UpstreamConnected);
    waiter.await.expect("re-arm resolves");
}

// Suppress unused-import warning for SyncEntry (used via wire variants).
#[allow(dead_code)]
fn _suppress_unused() {
    let _ = SyncEntry::BareBytes(ByteBuf::new());
}
