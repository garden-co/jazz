//! SQLite candidate authorization exercised through the real browser ABI.
#![cfg(target_arch = "wasm32")]
use jazz::db::WriteState;
use jazz::ids::{AuthorSubject, NodeUuid};
use jazz::records::{OwnedRecord, RecordDescriptor, Value, ValueType};
use jazz::tools::{
    ColumnType, OpenTransactionId, PolicyExpr, SchemaBuilder, TablePolicies, TableSchemaBuilder,
};
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
fn open(node: u8, authority: bool) -> WasmDb {
    let schema = SchemaBuilder::new()
        .table(
            TableSchemaBuilder::new("nodes")
                .fk_column("parent", "nodes")
                .column("seed", ColumnType::Boolean)
                .policies(
                    TablePolicies::new()
                        .with_select(PolicyExpr::True)
                        .with_insert(PolicyExpr::and(vec![
                            PolicyExpr::eq_session("$createdBy", vec!["user".into()]),
                            PolicyExpr::or(vec![
                                PolicyExpr::eq_literal("seed", jazz::tools::Value::Boolean(true)),
                                PolicyExpr::exists_including_created(
                                    "nodes",
                                    PolicyExpr::eq_session(
                                        "id",
                                        vec!["__jazz_outer_row".into(), "parent".into()],
                                    ),
                                ),
                            ]),
                        ])),
                ),
        )
        .build();
    let config = postcard::to_allocvec(&Config {
        identity: Identity {
            node: NodeUuid::from_bytes([node; 16]),
            author: alice(),
        },
        row_id_seed: Some(node.into()),
        history_complete: authority,
        initial_sync_flush_every: None,
    })
    .unwrap();
    if authority {
        WasmDb::open_memory_as_backend(serde_json::to_vec(&schema).unwrap(), config)
    } else {
        WasmDb::open_memory(serde_json::to_vec(&schema).unwrap(), config)
    }
    .unwrap()
}
fn stage(db: &WasmDb, tx: &str, id: u8, parent: u8, seed: bool) {
    let descriptor =
        RecordDescriptor::new([("parent", ValueType::Uuid), ("seed", ValueType::Bool)]);
    let cells = descriptor
        .create(&[
            Value::Uuid(uuid::Uuid::from_bytes([parent; 16])),
            Value::Bool(seed),
        ])
        .unwrap();
    let cells =
        jazz::binding_codec::encode_named_cells(&OwnedRecord::new(cells, descriptor)).unwrap();
    db.upsert_in_transaction(
        tx.into(),
        "nodes".into(),
        vec![id; 16],
        cells,
        JsValue::NULL,
    )
    .unwrap();
}
fn state(write: &WasmWrite) -> WriteState {
    serde_wasm_bindgen::from_value(write.write_state().unwrap()).unwrap()
}
fn rows(db: &WasmDb) -> usize {
    let value = db
        .all(
            postcard::to_allocvec(&jazz::query::Query::from("nodes")).unwrap(),
            js_sys::JSON::parse(r#"{"tier":"local-only"}"#).unwrap(),
            None,
            None,
            JsValue::NULL,
        )
        .unwrap();
    jazz::binding_codec::decode_rows(&js_sys::Uint8Array::new(&value).to_vec())
        .unwrap()
        .iter()
        .map(|batch| batch.rows.len())
        .sum()
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
/// Alice's browser sends a reverse-order unit to Bob. The ordinary authenticated
/// link admits the seeded graph and atomically rejects an ungrounded cycle.
#[wasm_bindgen_test]
async fn browser_wire_candidates_require_an_independent_seed() {
    for seeded in [true, false] {
        let client = open(1, false);
        client.set_remote_link_hint("no-server".into()).unwrap();
        let bob = open(2, true);
        let tx = OpenTransactionId::new().to_string();
        client
            .begin_transaction(tx.clone(), "serializable".into(), None, None)
            .unwrap();
        stage(&client, &tx, 1, 2, false);
        stage(&client, &tx, 2, 1, seeded);
        let write = client
            .commit_transaction(tx, Some("serializable".into()))
            .unwrap();
        let up = JsFuture::from(client.connect_upstream().unwrap())
            .await
            .unwrap();
        let down = JsFuture::from(
            bob.accept_subscriber(alice().canonical().as_bytes().to_vec(), JsValue::NULL, None)
                .unwrap(),
        )
        .await
        .unwrap();
        for _ in 0..20 {
            JsFuture::from(client.tick()).await.unwrap();
            transfer(&up, &down);
            JsFuture::from(bob.tick()).await.unwrap();
            transfer(&down, &up);
            JsFuture::from(client.tick()).await.unwrap();
            if !matches!(state(&write).fate, Fate::Pending) {
                break;
            }
        }
        assert_eq!(
            matches!(state(&write).fate, Fate::Accepted),
            seeded,
            "{:?}",
            state(&write)
        );
        assert_eq!(matches!(state(&write).fate, Fate::Rejected(_)), !seeded);
        assert_eq!(rows(&bob), if seeded { 2 } else { 0 });
        assert_eq!(rows(&client), if seeded { 2 } else { 0 });
        JsFuture::from(client.close()).await.unwrap();
        JsFuture::from(bob.close()).await.unwrap();
    }
}
/// A trusted browser host binding still enforces Alice's selected session
/// policies and the same independent proof for an authority-local transaction.
#[wasm_bindgen_test]
async fn browser_backend_session_candidates_use_the_same_proof() {
    for seeded in [true, false] {
        let bob = open(3, true);
        let tx = OpenTransactionId::new().to_string();
        bob.begin_transaction(
            tx.clone(),
            "serializable".into(),
            Some(alice().canonical().as_bytes().to_vec()),
            None,
        )
        .unwrap();
        stage(&bob, &tx, 1, 2, false);
        stage(&bob, &tx, 2, 1, seeded);
        let result = bob.commit_transaction(tx, Some("serializable".into()));
        if seeded {
            assert!(matches!(state(&result.unwrap()).fate, Fate::Accepted));
        } else {
            let error = match result {
                Err(error) => error,
                Ok(_) => panic!("ungrounded unit accepted"),
            };
            assert!(format!("{error:?}").contains("authorization_denied"));
        }
        assert_eq!(rows(&bob), if seeded { 2 } else { 0 });
        JsFuture::from(bob.close()).await.unwrap();
    }
}
