//! Initialization through the SDK ABI, including the real browser VFS barrier.
#![cfg(target_arch = "wasm32")]

use jazz::ids::{AuthorSubject, NodeUuid};
use jazz::records::{OwnedRecord, RecordDescriptor, Value, ValueType};
use jazz::tools::{ColumnType, PolicyExpr, SchemaBuilder, TablePolicies, TableSchemaBuilder};

use jazz::tools::OpenTransactionId;
use jazz_wasm::WasmDb;
use serde::Serialize;
use wasm_bindgen::JsValue;
use wasm_bindgen_futures::JsFuture;
use wasm_bindgen_test::*;
wasm_bindgen_test_configure!(run_in_browser);

// The public binding receives this existing postcard envelope from JavaScript.
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
fn schema() -> Vec<u8> {
    let policies = TablePolicies::new()
        .with_select(PolicyExpr::True)
        .with_insert(PolicyExpr::True)
        .with_update(Some(PolicyExpr::True), PolicyExpr::True)
        .with_delete(PolicyExpr::True);
    serde_json::to_vec(
        &SchemaBuilder::new()
            .table(
                TableSchemaBuilder::new("notes")
                    .column("text", ColumnType::Text)
                    .policies(policies),
            )
            .build(),
    )
    .unwrap()
}
fn config() -> Vec<u8> {
    postcard::to_allocvec(&Config {
        identity: Identity {
            node: NodeUuid::from_bytes([0x73; 16]),
            author: AuthorSubject::authenticated("https://issuer.example", "alice").unwrap(),
        },
        row_id_seed: Some(73),
        history_complete: false,
        initial_sync_flush_every: None,
    })
    .unwrap()
}
fn prop(value: &JsValue, key: &str) -> JsValue {
    js_sys::Reflect::get(value, &key.into()).unwrap()
}
async fn stage(db: &WasmDb, row: u8) -> String {
    let id = OpenTransactionId::new().to_string();
    db.begin_transaction(id.clone(), "serializable".into(), None, None)
        .unwrap();
    JsFuture::from(
        db.record_initialization_insert_absence(id.clone(), "notes".into(), vec![row; 16])
            .unwrap(),
    )
    .await
    .unwrap();
    let descriptor = RecordDescriptor::new([("text", ValueType::String)]);
    let cells = descriptor
        .create(&[Value::String("founder".into())])
        .unwrap();
    let cells =
        jazz::binding_codec::encode_named_cells(&OwnedRecord::new(cells, descriptor)).unwrap();
    let opts = js_sys::JSON::parse(&serde_json::json!({"rowId":vec![row;16]}).to_string()).unwrap();
    db.insert_in_transaction(id.clone(), "notes".into(), cells, opts)
        .unwrap();
    id
}
async fn seal(db: &WasmDb, row: u8) -> JsValue {
    let id = stage(db, row).await;
    JsFuture::from(db.seal_initialization_transaction(id).unwrap())
        .await
        .unwrap()
}
async fn status(db: &WasmDb, id: &str) -> serde_json::Value {
    let json = JsFuture::from(
        db.initialization_transaction_status(vec![id.into()])
            .unwrap(),
    )
    .await
    .unwrap()
    .as_string()
    .unwrap();
    serde_json::from_str(&json).unwrap()
}
/// Alice's root and schema view share single-use tokens; Mallory's independent
/// runtime cannot use one. Closing a view keeps the owner's seal usable.
#[wasm_bindgen_test]
async fn seals_are_single_use_and_owner_bound_across_views() {
    let alice = WasmDb::open_memory(schema(), config(), None).unwrap();
    let view = alice.register_schema(schema()).unwrap();
    let mallory = WasmDb::open_memory(schema(), config(), None).unwrap();
    let sealed = seal(&view, 1).await;
    let token = prop(&sealed, "token").as_string().unwrap();
    let reserved = prop(&sealed, "reservedTxId").as_string().unwrap();
    assert_eq!(
        status(&alice, &reserved).await["statuses"][0]["kind"],
        "not-observed"
    );
    assert!(mallory
        .publish_initialization_transaction(token.clone())
        .is_err());
    JsFuture::from(view.close()).await.unwrap();
    let receipt = JsFuture::from(
        alice
            .publish_initialization_transaction(token.clone())
            .unwrap(),
    )
    .await
    .unwrap();
    assert!(!receipt.is_undefined());
    assert!(alice.publish_initialization_transaction(token).is_err());
    let complete = status(&alice, &reserved).await;
    assert_eq!(complete["statuses"][0]["kind"], "complete");
    assert_eq!(complete["statuses"][0]["fate"]["kind"], "pending");
    assert_eq!(complete["statuses"][0]["durability"], "local");
    let cancelled = seal(&alice, 2).await;
    JsFuture::from(
        alice
            .cancel_initialization_transaction(prop(&cancelled, "token").as_string().unwrap())
            .unwrap(),
    )
    .await
    .unwrap();
    assert_eq!(
        status(
            &alice,
            &prop(&cancelled, "reservedTxId").as_string().unwrap()
        )
        .await["statuses"][0]["kind"],
        "not-observed"
    );
    JsFuture::from(alice.close()).await.unwrap();
    JsFuture::from(mallory.close()).await.unwrap();
}
/// Alice's reservation reaches IndexedDB before it is journalled. Publishing
/// and querying status flush the real upload unit, which survives reopening.
#[wasm_bindgen_test]
async fn indexeddb_reservation_and_publication_survive_reopen() {
    let owner = format!("initialization-{}", js_sys::Date::now());
    let alice = WasmDb::open_browser(JsValue::NULL, schema(), config(), owner.clone(), None)
        .await
        .unwrap();
    let sealed = seal(&alice, 1).await;
    let reserved = prop(&sealed, "reservedTxId").as_string().unwrap();
    let token = prop(&sealed, "token").as_string().unwrap();
    JsFuture::from(alice.publish_initialization_transaction(token).unwrap())
        .await
        .unwrap();
    assert_eq!(
        status(&alice, &reserved).await["statuses"][0]["durability"],
        "local"
    );
    JsFuture::from(alice.close()).await.unwrap();
    drop(alice);
    let reopened = WasmDb::open_browser(JsValue::NULL, schema(), config(), owner.clone(), None)
        .await
        .unwrap();
    assert_eq!(
        status(&reopened, &reserved).await["statuses"][0]["kind"],
        "complete"
    );
    let next = seal(&reopened, 2).await;
    assert_ne!(prop(&next, "reservedTxId").as_string().unwrap(), reserved);
    JsFuture::from(reopened.close()).await.unwrap();
    drop(reopened);
    WasmDb::destroy_browser_storage(JsValue::NULL, owner)
        .await
        .unwrap();
}
/// Closing Alice's owner while a seal is waiting for its browser flush consumes
/// the capability; the pending promise must not deliver a live-looking token.
#[wasm_bindgen_test]
async fn owner_close_wins_pending_seal_delivery() {
    let owner = format!("initialization-close-{}", js_sys::Date::now());
    let alice = WasmDb::open_browser(JsValue::NULL, schema(), config(), owner.clone(), None)
        .await
        .unwrap();
    let view = alice.register_schema(schema()).unwrap();
    let id = stage(&alice, 1).await;
    let pending = alice.seal_initialization_transaction(id).unwrap();
    let closing = alice.close();
    assert!(JsFuture::from(pending).await.is_err());
    JsFuture::from(closing).await.unwrap();
    assert!(view
        .seal_initialization_transaction(OpenTransactionId::new().to_string())
        .is_err());
    JsFuture::from(view.close()).await.unwrap();
    drop(view);
    drop(alice);
    WasmDb::destroy_browser_storage(JsValue::NULL, owner)
        .await
        .unwrap();
}
