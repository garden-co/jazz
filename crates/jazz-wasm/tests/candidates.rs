//! SQLite candidate authorization exercised through the real browser ABI.
#![cfg(target_arch = "wasm32")]
use futures_util::FutureExt;
use jazz::binding_host::{OpenConfig as Config, OpenIdentity as Identity};
use jazz::ids::{AuthorSubject, NodeUuid};
use jazz::records::{OwnedRecord, RecordDescriptor, Value, ValueType};
use jazz::tools::{
    ColumnType, OpenTransactionId, PolicyExpr, SchemaBuilder, TablePolicies, TableSchemaBuilder,
};
use jazz_wasm::{WasmDb, WasmWrite};
use wasm_bindgen::{JsCast, JsValue};
use wasm_bindgen_futures::JsFuture;
use wasm_bindgen_test::*;
wasm_bindgen_test_configure!(run_in_browser);
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
        migrations: None,
        system_node: None,
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
        let down = JsFuture::from(
            bob.accept_subscriber(alice().canonical().as_bytes().to_vec(), JsValue::NULL, None)
                .unwrap(),
        )
        .await
        .unwrap();
        let settled = settle_browser(&client, &bob, &up, &down, &write, 20).await;
        assert_eq!(settled.is_ok(), seeded, "{settled:?}");
        if let Err(error) = settled {
            assert!(
                format!("{error:?}").contains("authorization_denied"),
                "{error:?}"
            );
        }
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
            None,
        )
        .unwrap();
        stage(&bob, &tx, 1, 2, false);
        stage(&bob, &tx, 2, 1, seeded);
        let result = bob.commit_transaction(tx, Some("serializable".into()));
        if seeded {
            JsFuture::from(result.unwrap().wait("global".into(), None).unwrap())
                .await
                .unwrap();
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

fn inherited_browser(node: u8, authority: bool) -> WasmDb {
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
    let config = postcard::to_allocvec(&Config {
        identity: Identity {
            node: NodeUuid::from_bytes([node; 16]),
            author: alice(),
        },
        row_id_seed: Some(node.into()),
        history_complete: authority,
        initial_sync_flush_every: None,
        migrations: None,
        system_node: None,
    })
    .unwrap();
    let db = if authority {
        WasmDb::open_memory_as_backend(serde_json::to_vec(&schema).unwrap(), config)
    } else {
        WasmDb::open_memory(serde_json::to_vec(&schema).unwrap(), config)
    }
    .unwrap();
    if !authority {
        db.set_remote_link_hint("no-server".into()).unwrap();
    }
    db
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
    bob: &WasmDb,
    up: &JsValue,
    down: &JsValue,
    write: &WasmWrite,
    max_ticks: usize,
) -> Result<(), JsValue> {
    // The actual ABI reports terminal authority fate through Global wait;
    // writeState snapshots are explicitly unsupported. A pending receipt
    // cannot satisfy either outcome, and unexpected errors are not acceptance.
    let mut settled = Box::pin(JsFuture::from(write.wait("global".into(), None).unwrap()));
    for _ in 0..max_ticks {
        JsFuture::from(client.tick()).await.unwrap();
        transfer(up, down);
        JsFuture::from(bob.tick()).await.unwrap();
        transfer(down, up);
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
    let client = inherited_browser(7, false);
    let bob = inherited_browser(8, true);
    let up = JsFuture::from(client.connect_upstream().unwrap())
        .await
        .unwrap();
    let down = JsFuture::from(
        bob.accept_subscriber(alice().canonical().as_bytes().to_vec(), JsValue::NULL, None)
            .unwrap(),
    )
    .await
    .unwrap();
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
    assert_eq!(count_table(&bob, "nodes"), 1);
    assert_eq!(count_table(&bob, "admins"), 1);
    assert_eq!(count_table(&bob, "records"), 2);
    assert_eq!(count_table(&client, "records"), 2);
    JsFuture::from(client.close()).await.unwrap();
    JsFuture::from(bob.close()).await.unwrap();
}
