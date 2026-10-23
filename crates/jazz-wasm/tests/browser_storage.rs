//! SQLite worker storage identity and retirement through the public WASM ABI.
#![cfg(target_arch = "wasm32")]
use jazz::ids::{AuthorSubject, NodeUuid};
use jazz::records::{OwnedRecord, RecordDescriptor, Value, ValueType};
use jazz::tools::{ColumnType, PolicyExpr, SchemaBuilder, TablePolicies, TableSchemaBuilder};
use jazz_wasm::WasmDb;
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
fn config() -> Vec<u8> {
    postcard::to_allocvec(&Config {
        identity: Identity {
            node: NodeUuid::from_bytes([83; 16]),
            author: AuthorSubject::authenticated("https://issuer.example", "alice").unwrap(),
        },
        row_id_seed: Some(83),
        history_complete: false,
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
                    .policies(
                        TablePolicies::new()
                            .with_select(PolicyExpr::True)
                            .with_insert(PolicyExpr::True),
                    ),
            )
            .build(),
    )
    .unwrap()
}
fn storage(path: &str, require_existing: bool) -> JsValue {
    js_sys::JSON::parse(&serde_json::json!({"backend":"indexeddb", "path":path, "requireExisting":require_existing}).to_string()).unwrap()
}
async fn open(storage: JsValue, owner: &str) -> Result<WasmDb, JsValue> {
    WasmDb::open_browser(storage, schema(), config(), owner.into(), None).await
}
fn cells() -> Vec<u8> {
    let descriptor = RecordDescriptor::new([("text", ValueType::String)]);
    let record = descriptor.create(&[Value::String("hello".into())]).unwrap();
    jazz::binding_codec::encode_named_cells(&OwnedRecord::new(record, descriptor)).unwrap()
}
fn insert(db: &WasmDb) -> Result<jazz_wasm::WasmWrite, JsValue> {
    db.insert_with_options("notes".into(), cells(), JsValue::UNDEFINED)
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
        .map(|rows| rows.rows.len())
        .sum()
}

/// Alice's punctuation-distinct owners must have different default files.
/// The fixture pins the versioned default-name mapping through an explicit reopen.
#[wasm_bindgen_test]
async fn default_owner_paths_do_not_alias_and_match_the_byte_fixture() {
    let fixture: serde_json::Value = serde_json::from_str(include_str!(
        "../../../packages/jazz-tools/fixtures/browser-sqlite-root-v1.json"
    ))
    .unwrap();
    let a = open(JsValue::UNDEFINED, "alice/a").await.unwrap();
    let b = open(JsValue::UNDEFINED, "alice?a").await.unwrap();
    insert(&a).unwrap();
    assert_eq!(count(&a), 1);
    assert_eq!(count(&b), 0);
    JsFuture::from(a.close()).await.unwrap();
    JsFuture::from(b.close()).await.unwrap();
    let path = fixture["defaultPaths"]["alice/a"].as_str().unwrap();
    let again = open(storage(path, true), "alice/a").await.unwrap();
    assert_eq!(count(&again), 1);
    JsFuture::from(again.close()).await.unwrap();
    for owner in ["alice/a", "alice?a"] {
        WasmDb::destroy_browser_storage(JsValue::UNDEFINED, owner.into())
            .await
            .unwrap();
    }
}

/// A ready sidecar cannot silently replace Alice's missing file with an empty
/// database and restart its transaction allocator under the old node identity.
#[wasm_bindgen_test]
async fn admitted_missing_files_fail_before_a_new_database_is_created() {
    let owner = format!("missing-{}", jazz_wasm::generate_id());
    let path = format!("/{owner}.sqlite");
    assert!(open(storage(&path, true), &owner).await.is_err());
    let db = open(storage(&path, false), &owner).await.unwrap();
    insert(&db).unwrap();
    JsFuture::from(db.close()).await.unwrap();
    let reopened = open(storage(&path, true), &owner).await.unwrap();
    assert_eq!(count(&reopened), 1);
    JsFuture::from(reopened.close()).await.unwrap();
    WasmDb::destroy_browser_storage(storage(&path, false), owner.clone())
        .await
        .unwrap();
    assert!(open(storage(&path, true), &owner).await.is_err());
}

/// Closing Alice's owner fences retained views, transport ticks and streaming
/// mutations before another owner reopens the same file.
#[wasm_bindgen_test]
async fn closed_owner_cannot_write_through_retained_browser_handles() {
    let owner = format!("retirement-{}", jazz_wasm::generate_id());
    let path = format!("/{owner}.sqlite");
    let db = open(storage(&path, false), &owner).await.unwrap();
    let view = db.register_schema(schema()).unwrap();
    insert(&view).unwrap();
    let upload = db
        .begin_streaming_mutation(
            "notes".into(),
            vec![7; 16],
            {
                let descriptor = RecordDescriptor::new([] as [(&str, ValueType); 0]);
                let row = descriptor.create(&[]).unwrap();
                jazz::binding_codec::encode_named_cells(&OwnedRecord::new(row, descriptor)).unwrap()
            },
            "text".into(),
            Some("insert".into()),
            None,
            None,
            None,
            None,
            None,
        )
        .unwrap();
    JsFuture::from(upload.push(vec![b'a'; 70 * 1024]))
        .await
        .unwrap();
    let link = JsFuture::from(db.connect_upstream().unwrap())
        .await
        .unwrap();
    JsFuture::from(db.close()).await.unwrap();
    assert!(insert(&view).is_err());
    assert!(JsFuture::from(upload.finish()).await.is_err());
    let tick: js_sys::Function = js_sys::Reflect::get(&link, &JsValue::from_str("tick"))
        .unwrap()
        .dyn_into()
        .unwrap();
    let promise: js_sys::Promise = tick.call0(&link).unwrap().dyn_into().unwrap();
    assert!(JsFuture::from(promise).await.is_err());
    let reopened = open(storage(&path, true), &owner).await.unwrap();
    assert_eq!(count(&reopened), 1);
    JsFuture::from(reopened.close()).await.unwrap();
    WasmDb::destroy_browser_storage(storage(&path, false), owner)
        .await
        .unwrap();
}
