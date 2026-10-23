//! Public catalogue ABI exercised in a real browser, including a cold wire link.
#![cfg(target_arch = "wasm32")]
use jazz::ids::{AuthorSubject, NodeUuid};
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
fn open(node: u8, authority: bool) -> WasmDb {
    let schema = SchemaBuilder::new()
        .table(
            TableSchemaBuilder::new("notes")
                .column("text", ColumnType::Text)
                .policies(TablePolicies::new().with_select(PolicyExpr::True)),
        )
        .build();
    let config = Config {
        identity: Identity {
            node: NodeUuid::from_bytes([node; 16]),
            author: AuthorSubject::authenticated("https://issuer.example", "alice").unwrap(),
        },
        row_id_seed: Some(node.into()),
        history_complete: authority,
        initial_sync_flush_every: None,
    };
    WasmDb::open_memory_as_backend(
        serde_json::to_vec(&schema).unwrap(),
        postcard::to_allocvec(&config).unwrap(),
    )
    .unwrap()
}
fn method(value: &JsValue, name: &str) -> js_sys::Function {
    js_sys::Reflect::get(value, &name.into())
        .unwrap()
        .dyn_into()
        .unwrap()
}
async fn bytes(value: js_sys::Promise) -> Vec<u8> {
    js_sys::Uint8Array::new(&JsFuture::from(value).await.unwrap()).to_vec()
}
fn transfer(from: &JsValue, to: &JsValue) {
    let frames = method(from, "recvWireFrames").call0(from).unwrap();
    method(to, "sendWireFrames").call1(to, &frames).unwrap();
}
/// Alice's empty browser replica obtains Bob's exact accepted UUIDs through
/// the regular transport; a fresh read waits while cached lookup never fetches.
#[wasm_bindgen_test]
async fn browser_cold_catalogue_coverage_and_uuid_bytes() {
    let owner = open(3, true);
    let alice = open(1, false);
    assert!(alice.table_identity("notes".into()).is_err());
    let up = JsFuture::from(alice.connect_upstream().unwrap())
        .await
        .unwrap();
    let identity = AuthorSubject::authenticated("https://issuer.example", "alice").unwrap();
    let down = JsFuture::from(
        owner
            .accept_subscriber(
                identity.canonical().as_bytes().to_vec(),
                JsValue::NULL,
                None,
            )
            .unwrap(),
    )
    .await
    .unwrap();
    let read = alice
        .cover_catalogue(
            "notes".into(),
            Some(identity.canonical().as_bytes().to_vec()),
            JsValue::NULL,
        )
        .unwrap();
    assert!(!read.is_instance_of::<js_sys::Uint8Array>());
    let mut complete = false;
    for _ in 0..10 {
        JsFuture::from(alice.tick()).await.unwrap();
        transfer(&up, &down);
        JsFuture::from(owner.tick()).await.unwrap();
        transfer(&down, &up);
        JsFuture::from(alice.tick()).await.unwrap();
        let result = method(&read, "poll").call0(&read).unwrap();
        if !result.is_undefined() {
            assert!(result.is_instance_of::<js_sys::Uint8Array>());
            assert_eq!(js_sys::Uint8Array::new(&result).length(), 0);
            complete = true;
            break;
        }
    }
    assert!(complete);
    let table = bytes(alice.table_identity("notes".into()).unwrap()).await;
    let column = bytes(
        alice
            .column_identity("notes".into(), "text".into())
            .unwrap(),
    )
    .await;
    assert_eq!(table.len(), 16);
    assert_eq!(column.len(), 16);
    assert_ne!(table, column);
    assert_eq!(
        table,
        bytes(owner.table_identity("notes".into()).unwrap()).await
    );
    assert_eq!(
        column,
        bytes(
            owner
                .column_identity("notes".into(), "text".into())
                .unwrap()
        )
        .await
    );
    assert!(bytes(alice.table_identity("absent".into()).unwrap())
        .await
        .is_empty());
    assert!(bytes(
        alice
            .column_identity("notes".into(), "absent".into())
            .unwrap()
    )
    .await
    .is_empty());
    JsFuture::from(owner.close()).await.unwrap();
    assert_eq!(
        table,
        bytes(alice.table_identity("notes".into()).unwrap()).await
    );
    JsFuture::from(alice.close()).await.unwrap();
}
/// Independent authorities mint different UUIDs; a local unpublished browser
/// view cannot acquire accepted identities merely by registering its schema.
#[wasm_bindgen_test]
async fn browser_identity_lookup_does_not_mint_for_views() {
    let alice = open(1, true);
    let bob = open(2, true);
    assert_ne!(
        bytes(alice.table_identity("notes".into()).unwrap()).await,
        bytes(bob.table_identity("notes".into()).unwrap()).await
    );
    let schema = SchemaBuilder::new()
        .table(TableSchemaBuilder::new("drafts").column("text", ColumnType::Text))
        .build();
    let view = alice
        .register_schema(serde_json::to_vec(&schema).unwrap())
        .unwrap();
    assert!(bytes(view.table_identity("drafts".into()).unwrap())
        .await
        .is_empty());
    assert!(view
        .cover_catalogue("drafts".into(), None, JsValue::NULL)
        .is_err());
    JsFuture::from(alice.close()).await.unwrap();
    JsFuture::from(bob.close()).await.unwrap();
}
