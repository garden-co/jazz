//! Exercise the foreground read gate and host timer ABI in a real browser.
#![cfg(target_arch = "wasm32")]
use jazz::ids::{AuthorSubject, NodeUuid};
use jazz::records::{OwnedRecord, RecordDescriptor, Value, ValueType};
use jazz::tools::{ColumnType, PolicyExpr, SchemaBuilder, TablePolicies, TableSchemaBuilder};
use jazz_wasm::WasmDb;
use serde::Serialize;
use std::cell::RefCell;
use std::rc::Rc;
use wasm_bindgen::{closure::Closure, JsCast, JsValue};
use wasm_bindgen_futures::JsFuture;
use wasm_bindgen_test::*;
wasm_bindgen_test_configure!(run_in_browser);

#[derive(Serialize)]
struct Config {
    identity: Identity,
    row_id_seed: Option<u64>,
    history_complete: bool,
    initial_sync_flush_every: Option<u32>,
}
#[derive(Serialize)]
struct Identity {
    node: NodeUuid,
    author: AuthorSubject,
}
fn open(node: u8) -> WasmDb {
    let policies = TablePolicies::new()
        .with_select(PolicyExpr::True)
        .with_insert(PolicyExpr::True)
        .with_update(Some(PolicyExpr::True), PolicyExpr::True)
        .with_delete(PolicyExpr::True);
    let schema = SchemaBuilder::new()
        .table(
            TableSchemaBuilder::new("notes")
                .column("text", ColumnType::Text)
                .policies(policies),
        )
        .build();
    let config = Config {
        identity: Identity {
            node: NodeUuid::from_bytes([node; 16]),
            author: AuthorSubject::authenticated("https://issuer.example", "alice").unwrap(),
        },
        row_id_seed: Some(node.into()),
        history_complete: false,
        initial_sync_flush_every: None,
    };
    WasmDb::open_memory_as_backend(
        serde_json::to_vec(&schema).unwrap(),
        postcard::to_allocvec(&config).unwrap(),
    )
    .unwrap()
}
fn insert(db: &WasmDb) {
    let descriptor = RecordDescriptor::new([("text", ValueType::String)]);
    let cells = descriptor
        .create(&[Value::String("cached".into())])
        .unwrap();
    db.insert_with_options(
        "notes".into(),
        jazz::binding_codec::encode_named_cells(&OwnedRecord::new(cells, descriptor)).unwrap(),
        JsValue::UNDEFINED,
    )
    .unwrap();
}
fn method(value: &JsValue, name: &str) -> js_sys::Function {
    js_sys::Reflect::get(value, &name.into())
        .unwrap()
        .dyn_into()
        .unwrap()
}
fn read(db: &WasmDb, opts: &str) -> JsValue {
    db.all(
        postcard::to_allocvec(&jazz::query::Query::from("notes")).unwrap(),
        js_sys::JSON::parse(opts).unwrap(),
        None,
        None,
        JsValue::NULL,
    )
    .unwrap()
}
fn poll(read: &JsValue) -> JsValue {
    method(read, "poll").call0(read).unwrap()
}
fn count(bytes: &JsValue) -> usize {
    jazz::binding_codec::decode_rows(&js_sys::Uint8Array::new(bytes).to_vec())
        .unwrap()
        .iter()
        .map(|batch| batch.rows.len())
        .sum()
}
async fn connect(tab: &WasmDb, owner: &WasmDb) -> (JsValue, JsValue) {
    let up = JsFuture::from(tab.connect_upstream().unwrap())
        .await
        .unwrap();
    let alice = AuthorSubject::authenticated("https://issuer.example", "alice").unwrap();
    let down = JsFuture::from(
        owner
            .accept_subscriber(alice.canonical().as_bytes().to_vec(), JsValue::NULL, None)
            .unwrap(),
    )
    .await
    .unwrap();
    (up, down)
}
fn transfer(from: &JsValue, to: &JsValue) {
    let frames = method(from, "recvWireFrames").call0(from).unwrap();
    method(to, "sendWireFrames").call1(to, &frames).unwrap();
}
async fn turn(tab: &WasmDb, owner: &WasmDb, up: &JsValue, down: &JsValue) {
    JsFuture::from(tab.tick()).await.unwrap();
    transfer(up, down);
    JsFuture::from(owner.tick()).await.unwrap();
    transfer(down, up);
    JsFuture::from(tab.tick()).await.unwrap();
}
async fn sleep(ms: u64) {
    let promise = js_sys::Promise::new(&mut |resolve, _| {
        method(&js_sys::global(), "setTimeout")
            .call2(&JsValue::NULL, &resolve, &JsValue::from_f64(ms as f64))
            .unwrap();
    });
    JsFuture::from(promise).await.unwrap();
}

/// Alice's foreground starts before its offline owner. Its public WASM one-shot
/// stays pending, then reads the owner's cache through ordinary wire frames.
#[wasm_bindgen_test]
async fn foreground_open_is_pending_until_owner_cache_arrives() {
    let owner = open(0x31);
    insert(&owner);
    let tab = open(0x32);
    tab.set_non_durable_client().unwrap();
    tab.set_remote_link_hint("failed".into()).unwrap();
    let pending = read(&tab, "{}");
    assert!(poll(&pending).is_null());
    let (up, down) = connect(&tab, &owner).await;
    let mut result = JsValue::NULL;
    for _ in 0..20 {
        turn(&tab, &owner, &up, &down).await;
        result = poll(&pending);
        if !result.is_null() {
            break;
        }
    }
    assert_eq!(count(&result), 1);
    JsFuture::from(tab.close()).await.unwrap();
    JsFuture::from(owner.close()).await.unwrap();
}

/// A silent authority cannot strand Alice's foreground wait: the core requests
/// the existing after:<ms> host timer, whose tick releases the owner's cache.
#[wasm_bindgen_test]
async fn quiet_foreground_wait_requests_a_real_host_timeout() {
    let owner = open(0x41);
    insert(&owner);
    // The owner's upstream is attached but never serviced during this test.
    let _silent = JsFuture::from(owner.connect_upstream().unwrap())
        .await
        .unwrap();
    let tab = open(0x42);
    tab.set_non_durable_client().unwrap();
    let wakes = Rc::new(RefCell::new(Vec::<String>::new()));
    let seen = wakes.clone();
    let callback = Closure::<dyn FnMut(String)>::new(move |wake| {
        seen.borrow_mut().push(wake);
    });
    tab.set_tick_scheduler(
        callback
            .as_ref()
            .unchecked_ref::<js_sys::Function>()
            .clone(),
    );
    let (up, down) = connect(&tab, &owner).await;
    let pending = read(&tab, r#"{"first_load_remote_wait_ms":250}"#);
    assert!(poll(&pending).is_null());
    for _ in 0..4 {
        turn(&tab, &owner, &up, &down).await;
        assert!(poll(&pending).is_null());
    }
    let delay = wakes
        .borrow()
        .iter()
        .rev()
        .find_map(|s| {
            s.strip_prefix("after:")
                .map(|ms| ms.parse::<u64>().unwrap())
        })
        .expect("host timeout requested");
    sleep(delay + 25).await;
    JsFuture::from(tab.tick()).await.unwrap();
    assert_eq!(count(&poll(&pending)), 1);
    JsFuture::from(tab.close()).await.unwrap();
    JsFuture::from(owner.close()).await.unwrap();
    drop(callback);
}

/// Enabling foreground reads must not let Alice's memory-only tab claim that a
/// mutation is durable before storage-owner write forwarding exists.
#[wasm_bindgen_test]
async fn foreground_write_is_explicitly_unsupported() {
    let tab = open(0x51);
    tab.set_non_durable_client().unwrap();
    let descriptor = RecordDescriptor::new([("text", ValueType::String)]);
    let cells = descriptor
        .create(&[Value::String("unsaved".into())])
        .unwrap();
    let error = tab
        .insert_with_options(
            "notes".into(),
            jazz::binding_codec::encode_named_cells(&OwnedRecord::new(cells, descriptor)).unwrap(),
            JsValue::UNDEFINED,
        )
        .err()
        .expect("foreground writes must reject");
    let message = js_sys::Reflect::get(&error, &JsValue::from_str("message"))
        .unwrap()
        .as_string()
        .unwrap();
    assert!(message.contains("non-durable foreground"));
    JsFuture::from(tab.close()).await.unwrap();
}
