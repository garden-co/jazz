//! Catalogue policy evaluation through the public browser binding.
#![cfg(target_arch = "wasm32")]
use jazz::ids::{AuthorSubject, NodeUuid};
use jazz::records::{OwnedRecord, RecordDescriptor, Value, ValueType};
use jazz::tools::{ColumnType, PolicyExpr, SchemaBuilder, TablePolicies, TableSchemaBuilder};
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
fn alice() -> AuthorSubject {
    AuthorSubject::authenticated("https://issuer.example", "alice").unwrap()
}
fn open(authority: bool, policy: PolicyExpr) -> WasmDb {
    let schema = SchemaBuilder::new()
        .table(
            TableSchemaBuilder::new("documents")
                .column("scopeId", ColumnType::Uuid)
                .column("text", ColumnType::Text)
                .policies(TablePolicies::new().with_select(policy)),
        )
        .build();
    let config = Config {
        identity: Identity {
            node: NodeUuid::from_bytes([0x57; 16]),
            author: alice(),
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
fn path() -> Vec<String> {
    vec!["__jazz_table_identity_v1".into(), "documents".into()]
}
fn insert(db: &WasmDb, scope: uuid::Uuid, text: &str) {
    let desc = RecordDescriptor::new([("scopeId", ValueType::Uuid), ("text", ValueType::String)]);
    let record = desc
        .create(&[Value::Uuid(scope), Value::String(text.into())])
        .unwrap();
    db.insert_with_options(
        "documents".into(),
        jazz::binding_codec::encode_named_cells(&OwnedRecord::new(record, desc)).unwrap(),
        JsValue::NULL,
    )
    .unwrap();
}
fn query() -> Vec<u8> {
    postcard::to_allocvec(&jazz::query::Query::from("documents")).unwrap()
}
/// Alice's browser read applies the accepted UUID policy through the normal host
/// binding, including a serializable read; provider claims cannot replace it.
#[wasm_bindgen_test]
async fn browser_policy_and_transaction_use_the_authority_manifest() {
    let db = open(true, PolicyExpr::eq_session("scopeId", path()));
    let bytes = JsFuture::from(db.table_identity("documents".into()).unwrap())
        .await
        .unwrap();
    let scope = uuid::Uuid::from_slice(&js_sys::Uint8Array::new(&bytes).to_vec()).unwrap();
    insert(&db, scope, "good");
    insert(&db, uuid::Uuid::from_bytes([0x66; 16]), "bad");
    let claims = js_sys::JSON::parse(
        r#"{"__jazz_table_identity_v1":"66666666-6666-6666-6666-666666666666"}"#,
    )
    .unwrap();
    for transactional in [false, true] {
        let tx = transactional.then(|| jazz::tools::OpenTransactionId::new().to_string());
        if let Some(tx) = &tx {
            db.begin_transaction(
                tx.clone(),
                "serializable".into(),
                Some(alice().canonical().as_bytes().to_vec()),
                None,
            )
            .unwrap();
        }
        let read = db
            .all(
                query(),
                JsValue::NULL,
                tx.clone(),
                Some(alice().canonical().as_bytes().to_vec()),
                claims.clone(),
            )
            .unwrap();
        let rows =
            jazz::binding_codec::decode_rows(&js_sys::Uint8Array::new(&read).to_vec()).unwrap();
        assert_eq!(rows.iter().map(|r| r.rows.len()).sum::<usize>(), 1);
        if let Some(tx) = tx {
            db.rollback_transaction(tx).unwrap();
        }
    }
    JsFuture::from(db.close()).await.unwrap();
}
/// Alice's standalone browser has no accepted catalogue. A null predicate must
/// reject before returning rows even when it is given a plausible provider claim.
#[wasm_bindgen_test]
async fn browser_missing_catalogue_cannot_authorize_a_null_predicate() {
    let db = open(false, PolicyExpr::SessionIsNull { path: path() });
    insert(&db, uuid::Uuid::from_bytes([0x66; 16]), "private");
    assert!(db
        .all(
            query(),
            JsValue::NULL,
            None,
            Some(alice().canonical().as_bytes().to_vec()),
            JsValue::NULL
        )
        .is_err());
    JsFuture::from(db.close()).await.unwrap();
}
