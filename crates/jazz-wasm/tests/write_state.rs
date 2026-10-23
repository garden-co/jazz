//! Browser receipts qualify SQLite's local write state with its async VFS flush.
#![cfg(target_arch = "wasm32")]
use jazz::db::WriteState;
use jazz::ids::{AuthorSubject, NodeUuid};
use jazz::records::{OwnedRecord, RecordDescriptor, Value, ValueType};
use jazz::tools::{ColumnType, PolicyExpr, SchemaBuilder, TablePolicies, TableSchemaBuilder};
use jazz::tx::{DurabilityTier, Fate, RejectionReason};
use jazz_wasm::{WasmDb, WasmWrite};
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
fn insert(db: &WasmDb, id: u8) -> WasmWrite {
    let descriptor = RecordDescriptor::new([("text", ValueType::String)]);
    let cells = descriptor.create(&[Value::String("hello".into())]).unwrap();
    let cells =
        jazz::binding_codec::encode_named_cells(&OwnedRecord::new(cells, descriptor)).unwrap();
    let opts = js_sys::JSON::parse(&serde_json::json!({"rowId": vec![id;16]}).to_string()).unwrap();
    db.insert_with_options("notes".into(), cells, opts).unwrap()
}
fn state(write: &WasmWrite) -> WriteState {
    serde_wasm_bindgen::from_value(write.write_state().unwrap()).unwrap()
}
async fn wait(write: &WasmWrite, tier: &str) -> Result<JsValue, JsValue> {
    JsFuture::from(write.wait(tier.into(), None).unwrap()).await
}

/// A browser memory receipt reports pending/local, refuses unobserved global
/// acceptance, and preserves an immediate rejection across snapshots and waits.
#[wasm_bindgen_test]
async fn memory_receipts_report_fate_without_consuming_waits() {
    let db = WasmDb::open_memory(schema(), config()).unwrap();
    let mut write = insert(&db, 1);
    let snapshot = state(&write);
    assert_eq!(snapshot.fate, Fate::Pending);
    assert_eq!(snapshot.durability, DurabilityTier::Local);
    assert_eq!(snapshot.global_time, None);
    wait(&write, "local").await.unwrap();
    assert!(wait(&write, "global").await.is_err());
    assert_eq!(state(&write), snapshot);
    let rejected = insert(&db, 1);
    let rejection = state(&rejected);
    assert!(matches!(
        rejection.fate,
        Fate::Rejected(RejectionReason::MalformedCommit(_))
    ));
    assert_eq!(rejection.durability, DurabilityTier::None);
    assert!(wait(&rejected, "local").await.is_err());
    assert_eq!(state(&rejected), rejection);
    write.close();
    assert!(write.write_state().is_err());
    JsFuture::from(db.close()).await.unwrap();
}

/// IndexedDB writes report no local durability before a flush barrier. Waiting
/// for one write flushes its prefix; later writes require another barrier, and
/// reopening confirms that a locally waited write reached the backing store.
#[wasm_bindgen_test]
async fn indexeddb_local_wait_flushes_the_write_prefix() {
    let owner = format!("write-state-{}", jazz_wasm::generate_id());
    let storage = js_sys::JSON::parse(
        &serde_json::json!({"backend":"indexeddb", "path":format!("/{owner}.sqlite")}).to_string(),
    )
    .unwrap();
    let db = WasmDb::open_browser(storage.clone(), schema(), config(), owner.clone())
        .await
        .unwrap();
    let first = insert(&db, 2);
    assert_eq!(state(&first).durability, DurabilityTier::None);
    wait(&first, "none").await.unwrap();
    assert_eq!(state(&first).durability, DurabilityTier::None);
    wait(&first, "local").await.unwrap();
    assert_eq!(state(&first).durability, DurabilityTier::Local);
    let second = insert(&db, 3);
    assert_eq!(state(&first).durability, DurabilityTier::Local);
    assert_eq!(state(&second).durability, DurabilityTier::None);
    JsFuture::from(db.flush()).await.unwrap();
    assert_eq!(state(&second).durability, DurabilityTier::Local);
    drop(first);
    drop(second);
    JsFuture::from(db.close()).await.unwrap();
    drop(db);
    let reopened = WasmDb::open_browser(storage.clone(), schema(), config(), owner.clone())
        .await
        .unwrap();
    let duplicate = insert(&reopened, 2);
    assert!(matches!(state(&duplicate).fate, Fate::Rejected(_)));
    assert!(wait(&duplicate, "local").await.is_err());
    drop(duplicate);
    JsFuture::from(reopened.close()).await.unwrap();
    drop(reopened);
    WasmDb::destroy_browser_storage(storage, owner)
        .await
        .unwrap();
}
