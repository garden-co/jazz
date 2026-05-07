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
    WasmRuntime::new(SCHEMA_JSON, "test-app", "dev", "main", None, Some(true))
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
