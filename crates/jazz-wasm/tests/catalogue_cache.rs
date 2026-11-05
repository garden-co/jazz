//! The SDK catalogue ABI, including actual IndexedDB persistence in Chromium.
#![cfg(target_arch = "wasm32")]
use jazz::ids::{AuthorSubject, NodeUuid};
use jazz::query::Query;
use jazz::tools::{ColumnType, PolicyExpr, SchemaBuilder, TablePolicies, TableSchemaBuilder};
use jazz_wasm::{derive_user_id, mint_local_first_token, WasmDb};
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
fn config(node: u8, authority: bool) -> Vec<u8> {
    postcard::to_allocvec(&Config {
        identity: Identity {
            node: NodeUuid::from_bytes([node; 16]),
            author: AuthorSubject::authenticated("https://issuer.example", "alice").unwrap(),
        },
        row_id_seed: Some(node.into()),
        history_complete: authority,
        initial_sync_flush_every: None,
    })
    .unwrap()
}
fn schema(table: &str) -> Vec<u8> {
    serde_json::to_vec(
        &SchemaBuilder::new()
            .table(
                TableSchemaBuilder::new(table)
                    .column("text", ColumnType::Text)
                    .policies(TablePolicies::new().with_select(PolicyExpr::True)),
            )
            .build(),
    )
    .unwrap()
}
async fn ready(db: &WasmDb) -> bool {
    JsFuture::from(db.has_authenticated_catalogue().unwrap())
        .await
        .unwrap()
        .as_bool()
        .unwrap()
}
async fn state(db: &WasmDb) -> JsValue {
    JsFuture::from(db.take_authenticated_catalogue_state().unwrap())
        .await
        .unwrap()
}
fn prop(value: &JsValue, name: &str) -> JsValue {
    js_sys::Reflect::get(value, &name.into()).unwrap()
}
fn method(value: &JsValue, name: &str) -> js_sys::Function {
    prop(value, name).dyn_into().unwrap()
}
async fn capture(db: &WasmDb) -> Vec<u8> {
    js_sys::Uint8Array::new(&prop(&state(db).await, "capture")).to_vec()
}
async fn table(db: &WasmDb) -> Vec<u8> {
    js_sys::Uint8Array::new(
        &JsFuture::from(db.table_identity("notes".into()).unwrap())
            .await
            .unwrap(),
    )
    .to_vec()
}
fn transfer(from: &JsValue, to: &JsValue) {
    let frames = method(from, "recvWireFrames").call0(from).unwrap();
    method(to, "sendWireFrames").call1(to, &frames).unwrap();
}
/// Alice obtains a capture over Bob's normal wire link, then a fresh account
/// root restores the same IDs offline. Capture moves once across typed views.
#[wasm_bindgen_test]
async fn wire_capture_and_memory_restore_match_the_sdk_abi() {
    let bob = WasmDb::open_memory_as_backend(schema("notes"), config(3, true)).unwrap();
    let alice = WasmDb::open_memory(schema("notes"), config(1, false), None).unwrap();
    assert!(!ready(&alice).await);
    let up = JsFuture::from(alice.connect_upstream().unwrap())
        .await
        .unwrap();
    let who = AuthorSubject::authenticated("https://issuer.example", "alice").unwrap();
    let down = JsFuture::from(
        bob.accept_subscriber(who.canonical().as_bytes().to_vec(), JsValue::NULL, None)
            .unwrap(),
    )
    .await
    .unwrap();
    for _ in 0..8 {
        JsFuture::from(alice.tick()).await.unwrap();
        transfer(&up, &down);
        JsFuture::from(bob.tick()).await.unwrap();
        transfer(&down, &up);
    }
    assert!(ready(&alice).await);
    assert!(ready(&alice).await);
    let pending = alice.register_schema(schema("drafts")).unwrap();
    let observation = state(&pending).await;
    assert_eq!(prop(&observation, "ready").as_bool(), Some(false));
    let bytes = js_sys::Uint8Array::new(&prop(&observation, "capture")).to_vec();
    assert!(!bytes.is_empty());
    assert!(prop(&state(&alice).await, "capture").is_undefined());
    let reopened =
        WasmDb::open_memory(schema("notes"), config(2, false), Some(bytes.clone())).unwrap();
    assert!(ready(&reopened).await);
    assert!(prop(&state(&reopened).await, "capture").is_undefined());
    assert_eq!(table(&reopened).await, table(&bob).await);
    reopened
        .validate_catalogue_capture_replacement(bytes.clone(), bytes.clone())
        .unwrap();
    let mut corrupt = bytes;
    corrupt.push(0);
    assert!(reopened
        .validate_catalogue_capture_replacement(corrupt.clone(), corrupt.clone())
        .is_err());
    assert!(WasmDb::open_memory(schema("notes"), config(4, false), Some(corrupt)).is_err());
    assert!(reopened
        .cover_catalogue("notes".into(), None, JsValue::NULL)
        .is_err());
    JsFuture::from(pending.close()).await.unwrap();
    JsFuture::from(alice.close()).await.unwrap();
    JsFuture::from(bob.close()).await.unwrap();
    JsFuture::from(reopened.close()).await.unwrap();
    assert!(reopened.has_authenticated_catalogue().is_err());
    assert!(reopened.take_authenticated_catalogue_state().is_err());
}
/// Alice's cache cannot admit a proposed schema, including an empty local query.
/// The accepted typed view remains ready and resolves its own physical IDs.
#[wasm_bindgen_test]
async fn cached_memory_open_qualifies_each_requested_schema() {
    let bob = WasmDb::open_memory_as_backend(schema("notes"), config(3, true)).unwrap();
    let bytes = capture(&bob).await;
    let alice = WasmDb::open_memory(schema("drafts"), config(1, false), Some(bytes)).unwrap();
    assert!(!ready(&alice).await);
    assert_eq!(table(&alice).await, table(&bob).await);
    let query = postcard::to_allocvec(&Query::from("drafts").limit(0)).unwrap();
    assert!(alice
        .all(query, JsValue::NULL, None, None, JsValue::NULL)
        .is_err());
    let known = alice.register_schema(schema("notes")).unwrap();
    assert!(ready(&known).await);
    assert!(!ready(&alice).await);
    JsFuture::from(known.close()).await.unwrap();
    JsFuture::from(alice.close()).await.unwrap();
    JsFuture::from(bob.close()).await.unwrap();
}
/// Alice restores the host capture into a real IndexedDB-backed SQLite store,
/// flushes and closes, then reopens without the capture. Unknown schemas stay
/// unadmitted and the exact accepted UUIDs survive the durable boundary.
#[wasm_bindgen_test]
async fn indexeddb_cache_bootstrap_and_admission_survive_reopen() {
    let bob = WasmDb::open_memory_as_backend(schema("notes"), config(3, true)).unwrap();
    let bytes = capture(&bob).await;
    let expected = table(&bob).await;
    let owner = format!("catalogue-cache-{}", js_sys::Date::now());
    let alice = WasmDb::open_browser(
        JsValue::NULL,
        schema("notes"),
        config(1, false),
        owner.clone(),
        Some(bytes),
    )
    .await
    .unwrap();
    assert!(ready(&alice).await);
    assert_eq!(table(&alice).await, expected);
    JsFuture::from(alice.flush()).await.unwrap();
    JsFuture::from(alice.close()).await.unwrap();
    drop(alice);
    let reopened = WasmDb::open_browser(
        JsValue::NULL,
        schema("drafts"),
        config(1, false),
        owner.clone(),
        None,
    )
    .await
    .unwrap();
    assert!(!ready(&reopened).await);
    assert_eq!(table(&reopened).await, expected);
    let query = postcard::to_allocvec(&Query::from("drafts").limit(0)).unwrap();
    assert!(reopened
        .all(query, JsValue::NULL, None, None, JsValue::NULL)
        .is_err());
    assert!(!capture(&reopened).await.is_empty());
    JsFuture::from(reopened.close()).await.unwrap();
    drop(reopened);
    WasmDb::destroy_browser_storage(JsValue::NULL, owner)
        .await
        .unwrap();
    JsFuture::from(bob.close()).await.unwrap();
}
/// Alice's self-signed identity proof and catalogue cache are separate inputs:
/// valid cache bytes do not repair an invalid proof; a verified proof may restore.
#[wasm_bindgen_test]
async fn self_signed_cached_open_still_verifies_the_proof() {
    let bob = WasmDb::open_memory_as_backend(schema("notes"), config(3, true)).unwrap();
    let bytes = capture(&bob).await;
    let seed = "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA";
    let app = "catalogue-cache-proof";
    let author = serde_json::to_string(&(
        AuthorSubject::LOCAL_FIRST_ISSUER,
        derive_user_id(seed.into()).unwrap(),
    ))
    .unwrap();
    let token = mint_local_first_token(
        seed.into(),
        app.into(),
        60,
        (js_sys::Date::now() / 1000.0) as u64,
    )
    .unwrap();
    assert!(WasmDb::open_memory_with_self_signed_proof(
        schema("notes"),
        config(1, false),
        "invalid".into(),
        app.into(),
        author.clone(),
        Some(bytes.clone())
    )
    .is_err());
    let alice = WasmDb::open_memory_with_self_signed_proof(
        schema("notes"),
        config(1, false),
        token,
        app.into(),
        author,
        Some(bytes),
    )
    .unwrap();
    assert!(ready(&alice).await);
    assert_eq!(table(&alice).await, table(&bob).await);
    JsFuture::from(alice.close()).await.unwrap();
    JsFuture::from(bob.close()).await.unwrap();
}
