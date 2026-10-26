//! Owner storage receipts through the public WASM wire and IndexedDB APIs.
#![cfg(target_arch = "wasm32")]
use jazz::db::WriteState;
use jazz::ids::{AuthorSubject, NodeUuid};
use jazz::records::{OwnedRecord, RecordDescriptor, Value, ValueType};
use jazz::tools::{ColumnType, PolicyExpr, SchemaBuilder, TablePolicies, TableSchemaBuilder};
use jazz::tx::{DurabilityTier, Fate};
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

/// Alice's memory tab receives Local only after Bob's owner flush. Local keeps
/// both writes pending: the authority may subsequently accept one and reject one.
/// The tab queues before entering foreground mode to isolate this boundary while
/// new foreground mutation admission remains guarded pending overlay recovery.
#[wasm_bindgen_test]
async fn browser_owner_receipt_is_local_but_never_terminal() {
    let server = open(90, true);
    let relay = open(50, false);
    relay.set_remote_link_hint("failed".into()).unwrap();
    let tab = open(1, false);
    tab.set_remote_link_hint("failed".into()).unwrap();
    let accepted = insert(&tab, "accepted later", true);
    let rejected = insert(&tab, "rejected later", false);
    tab.set_private_owner_client().unwrap();
    assert_eq!(state(&accepted).durability, DurabilityTier::None);
    let (tu, td) = connect(&tab, &relay).await;
    tick(&tab).await;
    transfer(&tu, &td);
    assert_eq!(state(&accepted).durability, DurabilityTier::None);
    tick(&relay).await;
    transfer(&td, &tu);
    tick(&tab).await;
    for write in [&accepted, &rejected] {
        assert_eq!(state(write).fate, Fate::Pending);
        assert_eq!(state(write).durability, DurabilityTier::Local);
        JsFuture::from(write.wait("local".into(), None).unwrap())
            .await
            .unwrap();
    }
    JsFuture::from(tab.wait_for_pending_writes("local".into()))
        .await
        .unwrap();
    assert_eq!(count(&relay), 0);
    let (ru, rd) = connect(&relay, &server).await;
    for _ in 0..20 {
        tick(&relay).await;
        transfer(&ru, &rd);
        tick(&server).await;
        transfer(&rd, &ru);
        tick(&relay).await;
        transfer(&td, &tu);
        tick(&tab).await;
        transfer(&tu, &td);
    }
    assert_eq!(state(&accepted).fate, Fate::Accepted);
    assert_eq!(state(&accepted).durability, DurabilityTier::Global);
    assert!(matches!(state(&rejected).fate, Fate::Rejected(_)));
    assert_eq!(state(&rejected).durability, DurabilityTier::None);
    assert!(JsFuture::from(rejected.wait("local".into(), None).unwrap())
        .await
        .is_err());
    for db in [&tab, &relay, &server] {
        JsFuture::from(db.close()).await.unwrap();
    }
}

/// Alice's large upload receives Bob's IndexedDB storage receipt through the
/// transport tick API. After owner reopen, retry returns Local and the journal
/// can forward the complete value to the authority without resending its chunks.
#[wasm_bindgen_test]
async fn indexeddb_owner_receipt_survives_reopen_and_retry() {
    let owner = format!("owner-stored-{}", jazz_wasm::generate_id());
    let storage = js_sys::JSON::parse(
        &serde_json::json!({"backend":"indexeddb","path":format!("/{owner}.sqlite")}).to_string(),
    )
    .unwrap();
    let server = open(90, true);
    let tab = open(1, false);
    tab.set_remote_link_hint("failed".into()).unwrap();
    let write = insert(&tab, &"durable payload ".repeat(24000), true);
    tab.set_private_owner_client().unwrap();
    {
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
        let (tu, td) = connect(&tab, &relay).await;
        tick(&tab).await;
        transfer(&tu, &td);
        // Hosts normally tick the transport binding rather than the WasmDb.
        let flushed: js_sys::Promise = method(&td, "tick").call0(&td).unwrap().dyn_into().unwrap();
        JsFuture::from(flushed).await.unwrap();
        transfer(&td, &tu);
        tick(&tab).await;
        assert_eq!(state(&write).fate, Fate::Pending);
        assert_eq!(state(&write).durability, DurabilityTier::Local);
        JsFuture::from(write.wait("local".into(), None).unwrap())
            .await
            .unwrap();
        JsFuture::from(relay.close()).await.unwrap();
    }
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
    let (tu, td) = connect(&tab, &relay).await;
    tick(&tab).await;
    transfer(&tu, &td);
    tick(&relay).await;
    transfer(&td, &tu);
    tick(&tab).await;
    assert_eq!(state(&write).durability, DurabilityTier::Local);
    let (ru, rd) = connect(&relay, &server).await;
    for _ in 0..20 {
        tick(&relay).await;
        transfer(&ru, &rd);
        tick(&server).await;
        transfer(&rd, &ru);
        tick(&relay).await;
        transfer(&td, &tu);
        tick(&tab).await;
        transfer(&tu, &td);
    }
    assert_eq!(state(&write).fate, Fate::Accepted);
    assert_eq!(count(&server), 1);
    for db in [&tab, &relay, &server] {
        JsFuture::from(db.close()).await.unwrap();
    }
    WasmDb::destroy_browser_storage(storage, owner)
        .await
        .unwrap();
}
