//! Durable relay forwarding through the public WASM wire-frame and storage APIs.
#![cfg(target_arch = "wasm32")]
use jazz::db::WriteState;
use jazz::ids::{AuthorSubject, NodeUuid};
use jazz::records::{OwnedRecord, RecordDescriptor, Value, ValueType};
use jazz::tools::{ColumnType, PolicyExpr, SchemaBuilder, TablePolicies, TableSchemaBuilder};
use jazz::tx::Fate;
use jazz_wasm::{WasmDb, WasmWrite};
use serde::Serialize;
use wasm_bindgen::{JsCast, JsValue};
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
fn alice() -> AuthorSubject {
    AuthorSubject::authenticated("https://issuer.example", "alice").unwrap()
}
fn config(node: u8, authority: bool) -> Vec<u8> {
    postcard::to_allocvec(&Config {
        identity: Identity {
            node: NodeUuid::from_bytes([node; 16]),
            author: alice(),
        },
        row_id_seed: Some(node.into()),
        history_complete: authority,
        initial_sync_flush_every: None,
    })
    .unwrap()
}
fn schema() -> Vec<u8> {
    serde_json::to_vec(
        &SchemaBuilder::new()
            .table(
                TableSchemaBuilder::new("notes")
                    .column("text", ColumnType::Text)
                    .column("allowed", ColumnType::Boolean)
                    .policies(
                        TablePolicies::new()
                            .with_select(PolicyExpr::True)
                            .with_insert(PolicyExpr::eq_literal(
                                "allowed",
                                jazz::tools::Value::Boolean(true),
                            )),
                    ),
            )
            .build(),
    )
    .unwrap()
}
fn open(node: u8, authority: bool) -> WasmDb {
    WasmDb::open_memory_as_backend(schema(), config(node, authority)).unwrap()
}
fn insert(db: &WasmDb, text: &str, allowed: bool) -> WasmWrite {
    let descriptor =
        RecordDescriptor::new([("text", ValueType::String), ("allowed", ValueType::Bool)]);
    let row = descriptor
        .create(&[Value::String(text.into()), Value::Bool(allowed)])
        .unwrap();
    db.insert_with_options(
        "notes".into(),
        jazz::binding_codec::encode_named_cells(&OwnedRecord::new(row, descriptor)).unwrap(),
        JsValue::UNDEFINED,
    )
    .unwrap()
}
fn state(write: &WasmWrite) -> WriteState {
    serde_wasm_bindgen::from_value(write.write_state().unwrap()).unwrap()
}
fn method(value: &JsValue, name: &str) -> js_sys::Function {
    js_sys::Reflect::get(value, &name.into())
        .unwrap()
        .dyn_into()
        .unwrap()
}
fn transfer(from: &JsValue, to: &JsValue) {
    let frames = method(from, "recvWireFrames").call0(from).unwrap();
    method(to, "sendWireFrames").call1(to, &frames).unwrap();
}
async fn connect(client: &WasmDb, server: &WasmDb) -> (JsValue, JsValue) {
    (
        JsFuture::from(client.connect_upstream().unwrap())
            .await
            .unwrap(),
        JsFuture::from(
            server
                .accept_subscriber(alice().canonical().as_bytes().to_vec(), JsValue::NULL, None)
                .unwrap(),
        )
        .await
        .unwrap(),
    )
}
async fn tick(db: &WasmDb) {
    JsFuture::from(db.tick()).await.unwrap();
}
fn count(db: &WasmDb) -> usize {
    let rows = db
        .all(
            postcard::to_allocvec(&jazz::query::Query::from("notes")).unwrap(),
            js_sys::JSON::parse(r#"{"tier":"local-only"}"#).unwrap(),
            None,
            None,
            JsValue::NULL,
        )
        .unwrap();
    jazz::binding_codec::decode_rows(&js_sys::Uint8Array::new(&rows).to_vec())
        .unwrap()
        .iter()
        .map(|r| r.rows.len())
        .sum()
}

/// Alice's two browser replicas allocate equal transaction numbers. Bob's relay
/// must return the authority's accepted/rejected fate to the correct replica.
#[wasm_bindgen_test]
async fn browser_relay_routes_colliding_receipts_and_withholds_early_acceptance() {
    let server = open(90, true);
    let relay = open(50, false);
    let a = open(1, false);
    let b = open(2, false);
    let (ru, rd) = connect(&relay, &server).await;
    let (au, ad) = connect(&a, &relay).await;
    let (bu, bd) = connect(&b, &relay).await;
    let accepted = insert(&a, "allowed", true);
    let rejected = insert(&b, "denied", false);
    // Tx ids also encode the node; the core test pins equal numeric positions.
    tick(&a).await;
    tick(&b).await;
    transfer(&au, &ad);
    transfer(&bu, &bd);
    tick(&relay).await;
    transfer(&ad, &au);
    transfer(&bd, &bu);
    tick(&a).await;
    tick(&b).await;
    assert_eq!(state(&accepted).fate, Fate::Pending);
    assert_eq!(state(&rejected).fate, Fate::Pending);
    assert_eq!(count(&relay), 0);
    for _ in 0..20 {
        tick(&relay).await;
        transfer(&ru, &rd);
        tick(&server).await;
        transfer(&rd, &ru);
        tick(&relay).await;
        transfer(&ad, &au);
        transfer(&bd, &bu);
        tick(&a).await;
        tick(&b).await;
        transfer(&au, &ad);
        transfer(&bu, &bd);
    }
    assert_eq!(state(&accepted).fate, Fate::Accepted);
    assert!(matches!(state(&rejected).fate, Fate::Rejected(_)));
    JsFuture::from(accepted.wait("global".into(), None).unwrap())
        .await
        .unwrap();
    assert!(
        JsFuture::from(rejected.wait("global".into(), None).unwrap())
            .await
            .is_err()
    );
    assert_eq!(count(&server), 1);
    assert_eq!(count(&b), 0);
    for db in [&a, &b, &relay, &server] {
        JsFuture::from(db.close()).await.unwrap();
    }
}

/// Alice disconnects after Bob's IndexedDB relay stores her pending upload.
/// A real flush/reopen forwards the journal without Alice resending; reconnect
/// then returns the stored authority receipt to her original write handle.
#[wasm_bindgen_test]
async fn browser_indexeddb_relay_resumes_without_the_sender() {
    let owner = format!("relay-upload-{}", jazz_wasm::generate_id());
    let storage = js_sys::JSON::parse(
        &serde_json::json!({"backend":"indexeddb","path":format!("/{owner}.sqlite")}).to_string(),
    )
    .unwrap();
    let server = open(90, true);
    let client = open(1, false);
    let write = {
        let relay = WasmDb::open_browser(
            storage.clone(),
            schema(),
            config(50, false),
            owner.clone(),
            None,
        )
        .await
        .unwrap();
        relay.set_remote_link_hint("failed".into()).unwrap();
        let (up, down) = connect(&client, &relay).await;
        let write = insert(&client, &"durable payload ".repeat(24000), true);
        tick(&client).await;
        transfer(&up, &down);
        tick(&relay).await;
        assert_eq!(state(&write).fate, Fate::Pending);
        JsFuture::from(relay.flush()).await.unwrap();
        JsFuture::from(relay.close()).await.unwrap();
        write
    };
    let relay = WasmDb::open_browser(
        storage.clone(),
        schema(),
        config(50, false),
        owner.clone(),
        None,
    )
    .await
    .unwrap();
    let (up, down) = connect(&relay, &server).await;
    for _ in 0..20 {
        tick(&relay).await;
        transfer(&up, &down);
        tick(&server).await;
        transfer(&down, &up);
    }
    assert_eq!(count(&server), 1);
    let (cu, cd) = connect(&client, &relay).await;
    for _ in 0..20 {
        tick(&client).await;
        transfer(&cu, &cd);
        tick(&relay).await;
        transfer(&cd, &cu);
    }
    tick(&client).await;
    assert_eq!(state(&write).fate, Fate::Accepted);
    assert_eq!(count(&server), 1);
    for db in [&client, &relay, &server] {
        JsFuture::from(db.close()).await.unwrap();
    }
    WasmDb::destroy_browser_storage(storage, owner)
        .await
        .unwrap();
}
