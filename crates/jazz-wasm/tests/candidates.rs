//! Public WASM clients exchange canonical frames with a real Rust authority.
//! The authority-local selected-session case covers the shared host in WASM,
//! not an unavailable public JS backend-authority wrapper.
#![cfg(target_arch = "wasm32")]
use futures_util::FutureExt;
use jazz::binding_host::{self, Binding, Link, OpenConfig as Config, OpenIdentity as Identity};
use jazz::db::{Db, DbIdentity};
use jazz::ids::{AuthorSubject, NodeUuid};
use jazz::records::{OwnedRecord, RecordDescriptor, Value, ValueType};
use jazz::tools::{
    ColumnType, OpenTransactionId, PolicyExpr, Schema, SchemaBuilder, TablePolicies,
    TableSchemaBuilder,
};
use jazz_wasm::{WasmDb, WasmWrite};
use wasm_bindgen::{JsCast, JsValue};
use wasm_bindgen_futures::JsFuture;
use wasm_bindgen_test::*;
wasm_bindgen_test_configure!(run_in_browser);
fn alice() -> AuthorSubject {
    AuthorSubject::authenticated("https://issuer.example", "alice").unwrap()
}
fn candidate_schema() -> Schema {
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
    schema
}
fn config(node: u8) -> Vec<u8> {
    postcard::to_allocvec(&Config {
        identity: Identity {
            node: NodeUuid::from_bytes([node; 16]),
            author: alice(),
        },
        row_id_seed: Some(node.into()),
        history_complete: false,
        initial_sync_flush_every: None,
        migrations: None,
        system_node: None,
    })
    .unwrap()
}
fn client(node: u8, schema: &Schema) -> WasmDb {
    let db = WasmDb::open_memory(serde_json::to_vec(schema).unwrap(), config(node)).unwrap();
    db.set_remote_link_hint("no-server".into()).unwrap();
    db
}
async fn authority(node: u8, schema: &Schema, selected_session_host: bool) -> Binding {
    let (schema, config) =
        binding_host::decode_open(&serde_json::to_vec(schema).unwrap(), &config(node)).unwrap();
    let identity: DbIdentity = config.identity.into();
    let db = Db::open(binding_host::db_config(schema, &config, identity))
        .await
        .unwrap();
    // This is the real trusted Rust host role, distinct from backend identity.
    // The original Alice identity and schema remain unchanged; no SYSTEM open.
    db.set_authority();
    Binding::new(db, identity, selected_session_host)
}
fn node_cells(parent: u8, seed: bool) -> Vec<u8> {
    let descriptor =
        RecordDescriptor::new([("parent", ValueType::Uuid), ("seed", ValueType::Bool)]);
    let cells = descriptor
        .create(&[
            Value::Uuid(uuid::Uuid::from_bytes([parent; 16])),
            Value::Bool(seed),
        ])
        .unwrap();
    jazz::binding_codec::encode_named_cells(&OwnedRecord::new(cells, descriptor)).unwrap()
}
fn stage(db: &WasmDb, tx: &str, id: u8, parent: u8, seed: bool) {
    db.upsert_in_transaction(
        tx.into(),
        "nodes".into(),
        vec![id; 16],
        node_cells(parent, seed),
        JsValue::NULL,
    )
    .unwrap();
}
fn stage_host(db: &Binding, tx: &str, id: u8, parent: u8, seed: bool) {
    db.upsert_in_transaction(
        tx,
        "nodes",
        &[id; 16],
        &node_cells(parent, seed),
        &serde_json::Value::Null,
    )
    .unwrap();
}
async fn host_rows(db: &Binding, table: &str) -> usize {
    let author = alice().canonical().to_string();
    let bytes = db
        .all(
            postcard::to_allocvec(&jazz::query::Query::from(table)).unwrap(),
            &serde_json::json!({"tier":"local-only"}),
            None,
            Some(author.as_bytes()),
            None,
        )
        .unwrap()
        .await
        .unwrap();
    jazz::binding_codec::decode_rows(&bytes)
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
fn client_to_authority(client: &JsValue, authority: &Link) {
    let frames = js_sys::Array::from(&method(client, "recvWireFrames").call0(client).unwrap());
    authority
        .receive_checked(
            frames
                .iter()
                .map(|frame| js_sys::Uint8Array::new(&frame).to_vec()),
        )
        .unwrap();
}
fn authority_to_client(authority: &Link, client: &JsValue) {
    let frames = js_sys::Array::new();
    for frame in authority.take_outbound() {
        frames.push(&js_sys::Uint8Array::from(frame.as_slice()).into());
    }
    method(client, "sendWireFrames")
        .call1(client, &frames)
        .unwrap();
}
/// Alice's browser sends a reverse-order unit to Bob. The ordinary authenticated
/// link admits the seeded graph and atomically rejects an ungrounded cycle.
#[wasm_bindgen_test]
async fn browser_wire_candidates_require_an_independent_seed() {
    for seeded in [true, false] {
        let schema = candidate_schema();
        let client = client(1, &schema);
        let bob = authority(2, &schema, false).await;
        let tx = OpenTransactionId::new().to_string();
        client
            .begin_transaction(tx.clone(), "serializable".into(), None, None, None)
            .unwrap();
        stage(&client, &tx, 1, 2, false);
        stage(&client, &tx, 2, 1, seeded);
        let write = client
            .commit_transaction(tx, Some("serializable".into()))
            .unwrap();
        let up = JsFuture::from(client.connect_upstream().unwrap())
            .await
            .unwrap();
        let down = bob.accept_subscriber(alice(), Default::default()).unwrap();
        let settled = settle_browser(&client, &bob, &up, &down, &write, 20).await;
        assert_eq!(settled.is_ok(), seeded, "{settled:?}");
        if let Err(error) = settled {
            assert!(
                format!("{error:?}").contains("authorization_denied"),
                "{error:?}"
            );
        }
        assert_eq!(host_rows(&bob, "nodes").await, if seeded { 2 } else { 0 });
        assert_eq!(count_table(&client, "nodes"), if seeded { 2 } else { 0 });
        JsFuture::from(client.close()).await.unwrap();
        bob.close().await.unwrap();
    }
}
/// Shared Rust host Binding compiled for WASM preserves the original selected-
/// Alice authority-local contract. The original case name is retained; this
/// does not claim a public JS backend-authority wrapper.
#[wasm_bindgen_test]
async fn browser_backend_session_candidates_use_the_same_proof() {
    for seeded in [true, false] {
        let bob = authority(3, &candidate_schema(), true).await;
        let tx = OpenTransactionId::new().to_string();
        bob.begin_transaction(
            &tx,
            "serializable",
            Some(alice().canonical().as_bytes()),
            None,
        )
        .unwrap();
        stage_host(&bob, &tx, 1, 2, false);
        stage_host(&bob, &tx, 2, 1, seeded);
        let result = bob.commit_transaction(&tx, Some("serializable"));
        if seeded {
            let write = result.unwrap();
            bob.wait_for_write(&write, "global").unwrap().await.unwrap();
        } else {
            let error = match result {
                Err(error) => error,
                Ok(_) => panic!("ungrounded unit accepted"),
            };
            assert!(format!("{error:?}").contains("authorization_denied"));
        }
        assert_eq!(host_rows(&bob, "nodes").await, if seeded { 2 } else { 0 });
        bob.close().await.unwrap();
    }
}

fn inherited_schema() -> Schema {
    let outer = |column: &str, parent: &str| {
        PolicyExpr::eq_session(column, vec!["__jazz_outer_row".into(), parent.into()])
    };
    let condition = PolicyExpr::and(vec![outer("id", "node"), outer("scope", "scope")]);
    let marked = PolicyExpr::exists_including_created("nodes", condition.clone());
    let capability = PolicyExpr::and(vec![
        PolicyExpr::eq_literal("allowed", jazz::tools::Value::Boolean(true)),
        PolicyExpr::Exists {
            table: "nodes".into(),
            condition: Box::new(condition),
        },
    ]);
    let schema = SchemaBuilder::new()
        .table(
            TableSchemaBuilder::new("nodes")
                .column("seed", ColumnType::Boolean)
                .column("scope", ColumnType::Text)
                .policies(
                    TablePolicies::new()
                        .with_select(PolicyExpr::True)
                        .with_insert(PolicyExpr::eq_literal(
                            "seed",
                            jazz::tools::Value::Boolean(true),
                        ))
                        .with_update(
                            Some(PolicyExpr::InheritsReferencing {
                                operation: jazz::tools::Operation::Update,
                                source_table: "admins".into(),
                                via_column: "node".into(),
                                max_depth: None,
                            }),
                            PolicyExpr::False,
                        ),
                ),
        )
        .table(
            TableSchemaBuilder::new("admins")
                .fk_column("node", "nodes")
                .column("allowed", ColumnType::Boolean)
                .column("scope", ColumnType::Text)
                .policies(
                    TablePolicies::new()
                        .with_select(PolicyExpr::True)
                        .with_insert(marked.clone())
                        .with_update(Some(capability), PolicyExpr::False),
                ),
        )
        .table(
            TableSchemaBuilder::new("records")
                .fk_column("node", "nodes")
                .column("initial", ColumnType::Boolean)
                .column("scope", ColumnType::Text)
                .policies(
                    TablePolicies::new()
                        .with_select(PolicyExpr::True)
                        .with_insert(PolicyExpr::or(vec![
                            PolicyExpr::Inherits {
                                operation: jazz::tools::Operation::Update,
                                via_column: "node".into(),
                                max_depth: None,
                            },
                            PolicyExpr::and(vec![
                                PolicyExpr::eq_literal(
                                    "initial",
                                    jazz::tools::Value::Boolean(true),
                                ),
                                marked,
                            ]),
                        ])),
                ),
        )
        .build();
    schema
}
fn named_stage(db: &WasmDb, tx: &str, table: &str, id: u8, cells: &[(&str, Value)]) {
    let descriptor = RecordDescriptor::new(cells.iter().map(|(name, value)| {
        (
            *name,
            match value {
                Value::Uuid(_) => ValueType::Uuid,
                Value::Bool(_) => ValueType::Bool,
                Value::String(_) => ValueType::String,
                _ => panic!("fixture value"),
            },
        )
    }));
    let values = cells
        .iter()
        .map(|(_, value)| value.clone())
        .collect::<Vec<_>>();
    let row = descriptor.create(&values).unwrap();
    let bytes =
        jazz::binding_codec::encode_named_cells(&OwnedRecord::new(row, descriptor)).unwrap();
    db.upsert_in_transaction(tx.into(), table.into(), vec![id; 16], bytes, JsValue::NULL)
        .unwrap();
}
fn count_table(db: &WasmDb, table: &str) -> usize {
    let value = db
        .all(
            postcard::to_allocvec(&jazz::query::Query::from(table)).unwrap(),
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
async fn settle_browser(
    client: &WasmDb,
    bob: &Binding,
    up: &JsValue,
    down: &Link,
    write: &WasmWrite,
    max_ticks: usize,
) -> Result<(), JsValue> {
    // The actual ABI reports terminal authority fate through Global wait;
    // writeState snapshots are explicitly unsupported. A pending receipt
    // cannot satisfy either outcome, and unexpected errors are not acceptance.
    let mut settled = Box::pin(JsFuture::from(write.wait("global".into(), None).unwrap()));
    for _ in 0..max_ticks {
        JsFuture::from(client.tick()).await.unwrap();
        client_to_authority(up, down);
        bob.tick().await.unwrap();
        authority_to_client(down, up);
        JsFuture::from(client.tick()).await.unwrap();
        if let Some(result) = settled.as_mut().now_or_never() {
            return result.map(|_| ());
        }
    }
    panic!("did not settle")
}
/// Alice's browser publishes a founder graph, then uses accepted forward and
/// reverse grants for a lifecycle write. A new graph without its marked initial
/// grant rejects atomically; inherited chunked comparisons use exact evidence.
#[wasm_bindgen_test]
async fn browser_inherited_grants_preserve_creation_and_acceptance_boundaries() {
    let schema = inherited_schema();
    let client = client(7, &schema);
    let bob = authority(8, &schema, false).await;
    let up = JsFuture::from(client.connect_upstream().unwrap())
        .await
        .unwrap();
    let down = bob.accept_subscriber(alice(), Default::default()).unwrap();
    let scope = "a".repeat(70 * 1024);
    for (number, new_root, initial, accepted) in [
        (1, true, true, true),
        (2, false, false, true),
        (10, true, false, false),
    ] {
        let tx = OpenTransactionId::new().to_string();
        client
            .begin_transaction(tx.clone(), "serializable".into(), None, None, None)
            .unwrap();
        let node = if new_root { number } else { 1 };
        named_stage(
            &client,
            &tx,
            "records",
            number,
            &[
                ("node", Value::Uuid(uuid::Uuid::from_bytes([node; 16]))),
                ("initial", Value::Bool(initial)),
                ("scope", Value::String(scope.clone())),
            ],
        );
        if new_root {
            named_stage(
                &client,
                &tx,
                "admins",
                number,
                &[
                    ("node", Value::Uuid(uuid::Uuid::from_bytes([node; 16]))),
                    ("allowed", Value::Bool(true)),
                    ("scope", Value::String(scope.clone())),
                ],
            );
            named_stage(
                &client,
                &tx,
                "nodes",
                number,
                &[
                    ("seed", Value::Bool(true)),
                    ("scope", Value::String(scope.clone())),
                ],
            );
        }
        let write = client
            .commit_transaction(tx, Some("serializable".into()))
            .unwrap();
        let settled = settle_browser(&client, &bob, &up, &down, &write, 50).await;
        assert_eq!(settled.is_ok(), accepted, "{settled:?}");
        if let Err(error) = settled {
            assert!(
                format!("{error:?}").contains("authorization_denied"),
                "{error:?}"
            );
        }
    }
    assert_eq!(host_rows(&bob, "nodes").await, 1);
    assert_eq!(host_rows(&bob, "admins").await, 1);
    assert_eq!(host_rows(&bob, "records").await, 2);
    assert_eq!(count_table(&client, "records"), 2);
    JsFuture::from(client.close()).await.unwrap();
    bob.close().await.unwrap();
}
