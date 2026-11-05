//! Real browser coverage of the SDK's paired accepted-content frame.
#![cfg(target_arch = "wasm32")]
use jazz::ids::{AuthorSubject, NodeUuid};
use jazz::records::{OwnedRecord, RecordDescriptor, Value, ValueType};
use jazz::tools::{
    ColumnType, OpenTransactionId, PolicyExpr, SchemaBuilder, TablePolicies, TableSchemaBuilder,
};
use jazz_wasm::WasmDb;
use serde::Serialize;
use wasm_bindgen::JsValue;
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
fn open(authority: bool) -> WasmDb {
    let schema = SchemaBuilder::new()
        .table(
            TableSchemaBuilder::new("notes")
                .column("text", ColumnType::Text)
                .policies(
                    TablePolicies::new()
                        .with_select(PolicyExpr::True)
                        .with_insert(PolicyExpr::True),
                ),
        )
        .build();
    let config = Config {
        identity: Identity {
            node: NodeUuid::from_bytes([0x79; 16]),
            author: AuthorSubject::authenticated("https://issuer.example", "alice").unwrap(),
        },
        row_id_seed: Some(1),
        history_complete: authority,
        initial_sync_flush_every: None,
    };
    WasmDb::open_memory_as_backend(
        serde_json::to_vec(&schema).unwrap(),
        postcard::to_allocvec(&config).unwrap(),
    )
    .unwrap()
}
async fn bytes(value: JsValue) -> Vec<u8> {
    let value = JsFuture::from(js_sys::Promise::resolve(&value))
        .await
        .unwrap();
    js_sys::Uint8Array::new(&value).to_vec()
}
fn query() -> Vec<u8> {
    postcard::to_allocvec(&jazz::query::Query::from("notes")).unwrap()
}
fn opts() -> JsValue {
    js_sys::JSON::parse(r#"{"tier":"remote"}"#).unwrap()
}

/// Alice reads accepted content in Chromium through allSettlementMetadata;
/// rows and original transaction metadata share the existing byte frame.
#[wasm_bindgen_test]
async fn browser_pairs_content_with_its_accepted_transaction() {
    let db = open(true);
    let tx = OpenTransactionId::new().to_string();
    db.begin_transaction(tx.clone(), "serializable".into(), None, None)
        .unwrap();
    let empty = bytes(
        db.all_settlement_metadata(query(), opts(), tx.clone(), None, JsValue::NULL, Some(true))
            .unwrap(),
    )
    .await;
    assert_eq!(empty, [2, 0, 0, 0, b'[', b']', 0]);
    db.rollback_transaction(tx.clone()).unwrap();
    let descriptor = RecordDescriptor::new([("text", ValueType::String)]);
    let record = descriptor
        .create(&[Value::String("accepted".into())])
        .unwrap();
    let cells =
        jazz::binding_codec::encode_named_cells(&OwnedRecord::new(record, descriptor)).unwrap();
    let write = db
        .insert_with_options("notes".into(), cells, JsValue::NULL)
        .unwrap();
    JsFuture::from(write.wait("global".into(), None).unwrap())
        .await
        .unwrap();
    db.begin_transaction(tx.clone(), "serializable".into(), None, None)
        .unwrap();
    let metadata = bytes(
        db.all_settlement_metadata(query(), opts(), tx.clone(), None, JsValue::NULL, None)
            .unwrap(),
    )
    .await;
    let frame = bytes(
        db.all_settlement_metadata(query(), opts(), tx.clone(), None, JsValue::NULL, Some(true))
            .unwrap(),
    )
    .await;
    let size = u32::from_le_bytes(frame[..4].try_into().unwrap()) as usize;
    assert_eq!(&frame[4..4 + size], metadata);
    let rows = bytes(
        db.all(query(), opts(), Some(tx.clone()), None, JsValue::NULL)
            .unwrap(),
    )
    .await;
    assert_eq!(frame[4 + size..], rows);
    let metadata: serde_json::Value = serde_json::from_slice(&metadata).unwrap();
    assert_eq!(metadata[0]["transactionId"], write.tx_id());
    assert_eq!(metadata[0]["position"], "1");
    let receipt = db
        .commit_transaction(tx, Some("serializable".into()))
        .unwrap();
    JsFuture::from(receipt.wait("global".into(), None).unwrap())
        .await
        .unwrap();
    JsFuture::from(db.close()).await.unwrap();
}

/// A standalone browser store has no authority evidence. Alice's failed
/// paired read must poison its transaction rather than silently commit it.
#[wasm_bindgen_test]
async fn browser_refuses_unqualified_content_and_commit() {
    let db = open(false);
    let tx = OpenTransactionId::new().to_string();
    db.begin_transaction(tx.clone(), "serializable".into(), None, None)
        .unwrap();
    assert!(db
        .all_settlement_metadata(query(), opts(), tx.clone(), None, JsValue::NULL, Some(true))
        .is_err());
    assert!(db
        .commit_transaction(tx, Some("serializable".into()))
        .is_err());
    JsFuture::from(db.close()).await.unwrap();
}
