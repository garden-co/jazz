//! Qualified foreground writes and recovery through real browser storage.
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
    if authority {
        WasmDb::open_memory_as_backend(schema(), config(node, true)).unwrap()
    } else {
        WasmDb::open_memory(schema(), config(node, false), None).unwrap()
    }
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

async fn turn(tab: &WasmDb, owner: &WasmDb, up: &JsValue, down: &JsValue) {
    tick(tab).await;
    transfer(up, down);
    tick(owner).await;
    transfer(down, up);
    tick(tab).await;
}
async fn admit(tab: &WasmDb, owner: &WasmDb) -> (JsValue, JsValue) {
    let (up, down) = connect(tab, owner).await;
    let ready = tab.wait_for_storage_owner().unwrap();
    for _ in 0..4 {
        turn(tab, owner, &up, &down).await;
    }
    JsFuture::from(ready).await.unwrap();
    (up, down)
}

/// Alice writes only after her private worker acknowledges recovery. The actual
/// WASM owner flush supplies Local; Bob's authority accepts or rejects later.
#[wasm_bindgen_test]
async fn qualified_foreground_writes_use_real_owner_and_authority_receipts() {
    let server = open(90, true);
    let owner = open(50, false);
    owner.set_scope_isolated_storage_owner().unwrap();
    owner.set_remote_link_hint("failed".into()).unwrap();
    let tab = open(1, false);
    tab.set_private_owner_client().unwrap();
    tab.set_remote_link_hint("failed".into()).unwrap();
    let (tu, td) = admit(&tab, &owner).await;
    let yes = insert(&tab, "accepted", true);
    let no = insert(&tab, "rejected", false);
    for write in [&yes, &no] {
        assert_eq!(state(write).durability, DurabilityTier::None);
    }
    for _ in 0..4 {
        turn(&tab, &owner, &tu, &td).await;
    }
    for write in [&yes, &no] {
        assert_eq!(state(write).fate, Fate::Pending);
        assert_eq!(state(write).durability, DurabilityTier::Local);
        JsFuture::from(write.wait("local".into(), None).unwrap())
            .await
            .unwrap();
    }
    let (ou, od) = connect(&owner, &server).await;
    for _ in 0..16 {
        turn(&owner, &server, &ou, &od).await;
        turn(&tab, &owner, &tu, &td).await;
    }
    assert_eq!(state(&yes).fate, Fate::Accepted);
    assert_eq!(state(&yes).durability, DurabilityTier::Global);
    assert!(matches!(state(&no).fate, Fate::Rejected(_)));
    assert_eq!(count(&tab), 1);
    for db in [&tab, &owner, &server] {
        JsFuture::from(db.close()).await.unwrap();
    }
}

/// Alice loses her original tab after a real IndexedDB flush. A newly opened
/// worker and tab recover a large pending write via a normal propagated query,
/// then Bob's authority accepts the original upload without the old tab.
#[wasm_bindgen_test]
async fn indexeddb_private_owner_recovers_into_a_new_foreground() {
    let storage_owner = format!("private-writes-{}", jazz_wasm::generate_id());
    let storage = js_sys::JSON::parse(
        &serde_json::json!({
            "backend":"indexeddb", "path":format!("/{storage_owner}.sqlite")
        })
        .to_string(),
    )
    .unwrap();
    let text = "offline payload ".repeat(24000);
    {
        let owner = WasmDb::open_browser(
            storage.clone(),
            schema(),
            config(50, false),
            storage_owner.clone(),
            None,
        )
        .await
        .unwrap();
        owner.set_scope_isolated_storage_owner().unwrap();
        owner.set_remote_link_hint("failed".into()).unwrap();
        let tab = open(1, false);
        tab.set_private_owner_client().unwrap();
        tab.set_remote_link_hint("failed".into()).unwrap();
        let (tu, td) = admit(&tab, &owner).await;
        let write = insert(&tab, &text, true);
        for _ in 0..8 {
            turn(&tab, &owner, &tu, &td).await;
        }
        JsFuture::from(write.wait("local".into(), None).unwrap())
            .await
            .unwrap();
        assert_eq!(state(&write).fate, Fate::Pending);
        for db in [&tab, &owner] {
            JsFuture::from(db.close()).await.unwrap();
        }
    }
    let owner = WasmDb::open_browser(
        storage.clone(),
        schema(),
        config(50, false),
        storage_owner.clone(),
        None,
    )
    .await
    .unwrap();
    owner.set_scope_isolated_storage_owner().unwrap();
    owner.set_remote_link_hint("failed".into()).unwrap();
    let tab = open(2, false);
    tab.set_private_owner_client().unwrap();
    tab.set_remote_link_hint("failed".into()).unwrap();
    let (tu, td) = admit(&tab, &owner).await;
    assert_eq!(count(&tab), 0);
    let pending = tab
        .all(
            postcard::to_allocvec(&jazz::query::Query::from("notes")).unwrap(),
            JsValue::UNDEFINED,
            None,
            None,
            JsValue::NULL,
        )
        .unwrap();
    let mut result = JsValue::NULL;
    for _ in 0..40 {
        result = method(&pending, "poll").call0(&pending).unwrap();
        if !result.is_null() {
            break;
        }
        turn(&tab, &owner, &tu, &td).await;
    }
    assert!(
        !result.is_null(),
        "owner never answered the recovered query"
    );
    let rows =
        jazz::binding_codec::decode_rows(&js_sys::Uint8Array::new(&result).to_vec()).unwrap();
    assert_eq!(rows.iter().map(|batch| batch.rows.len()).sum::<usize>(), 1);
    assert_eq!(count(&tab), 1);
    JsFuture::from(tab.wait_for_pending_writes("local".into()))
        .await
        .unwrap();
    let server = open(90, true);
    let global = tab.wait_for_pending_writes("global".into());
    let (ou, od) = connect(&owner, &server).await;
    for _ in 0..16 {
        turn(&owner, &server, &ou, &od).await;
        turn(&tab, &owner, &tu, &td).await;
    }
    JsFuture::from(global).await.unwrap();
    assert_eq!(count(&server), 1);
    for db in [&tab, &owner, &server] {
        JsFuture::from(db.close()).await.unwrap();
    }
    WasmDb::destroy_browser_storage(storage, storage_owner)
        .await
        .unwrap();
}

/// Alice's pending owner-admission promise rejects when the transport or tab
/// closes. A lost worker cannot strand SDK startup after the control handshake.
#[wasm_bindgen_test]
async fn owner_admission_wait_is_cancelled_by_transport_or_runtime_close() {
    for close_runtime in [false, true] {
        let tab = open(1, false);
        tab.set_private_owner_client().unwrap();
        let relay = open(50, false);
        let (up, _) = connect(&tab, &relay).await;
        let wait = tab.wait_for_storage_owner().unwrap();
        if close_runtime {
            JsFuture::from(tab.close()).await.unwrap();
        } else {
            method(&up, "close").call0(&up).unwrap();
        }
        assert!(JsFuture::from(wait).await.is_err());
        for db in [&tab, &relay] {
            JsFuture::from(db.close()).await.unwrap();
        }
    }
}
