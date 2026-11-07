//! Open transaction handles stage write by write. A handle's staged writes
//! stay applied between its stages, so staging a batch replays it once
//! rather than once per write, and that overlay is undone before anything
//! else uses the store: no other read, write or handle sees it.

use crate::common;

use std::collections::BTreeMap;

use jazz::db::{Db, DbConfig, DbIdentity, InsertOptions, ReadOpts, block_on};
use jazz::ids::{AuthorSubject, NodeUuid, RowUuid};
use jazz::query::Query;
use jazz::records::Value;
use jazz::schema::JazzSchema;
use jazz::tools::{ColumnType, OpenTransactionId, SchemaBuilder, TableSchemaBuilder};

use common::{allow_all_policies, compile_schema};

fn schema() -> JazzSchema {
    compile_schema(
        &SchemaBuilder::new()
            .table(
                TableSchemaBuilder::new("notes")
                    .column("body", ColumnType::Text)
                    .policies(allow_all_policies()),
            )
            .build(),
    )
}

fn open() -> Db {
    block_on(Db::open(DbConfig::new(
        schema(),
        DbIdentity {
            node: NodeUuid::from_bytes([1; 16]),
            author: AuthorSubject::SYSTEM,
        },
    )))
    .unwrap()
}

fn row(n: u8) -> RowUuid {
    RowUuid::from_bytes([n; 16])
}

fn body(text: &str) -> BTreeMap<String, Value> {
    BTreeMap::from([("body".to_owned(), Value::String(text.to_owned()))])
}

fn bodies(rows: Vec<jazz::db::CurrentRow>) -> Vec<String> {
    let mut bodies: Vec<String> = rows
        .iter()
        .map(|row| match row.cell("body") {
            Some(Value::String(text)) => text,
            other => panic!("{other:?}"),
        })
        .collect();
    bodies.sort();
    bodies
}

fn outside(db: &Db) -> Vec<String> {
    let prepared = db.prepare_query(&Query::from("notes")).unwrap();
    bodies(block_on(db.all(&prepared, ReadOpts::default())).unwrap())
}

fn inside(db: &Db, id: OpenTransactionId) -> Vec<String> {
    let prepared = db.prepare_query(&Query::from("notes")).unwrap();
    bodies(block_on(db.tx_ref(id).all_prepared(&prepared)).unwrap())
}

fn stage_update(db: &Db, id: OpenTransactionId, n: u8, text: &str) {
    block_on(
        db.tx_ref(id)
            .update("notes", row(n), body(text), Default::default()),
    )
    .unwrap();
}

/// Two handles stage over one row while plain reads and writes run in
/// between: each handle reads its own writes, nothing else sees them, a
/// refused stage keeps the earlier ones, and the commits apply in order.
#[test]
fn a_handles_staged_writes_are_seen_only_by_that_handle() {
    let db = open();
    block_on(db.insert(
        "notes",
        body("a"),
        InsertOptions {
            row_id: Some(row(1)),
            ..Default::default()
        },
    ))
    .unwrap();

    let first = OpenTransactionId::new();
    block_on(db.begin_mergeable(first)).unwrap();
    stage_update(&db, first, 1, "first");
    block_on(db.tx_ref(first).insert(
        "notes",
        body("b"),
        InsertOptions {
            row_id: Some(row(2)),
            ..Default::default()
        },
    ))
    .unwrap();
    assert_eq!(outside(&db), ["a"]);
    assert_eq!(inside(&db, first), ["b", "first"]);

    let second = OpenTransactionId::new();
    block_on(db.begin_mergeable(second)).unwrap();
    stage_update(&db, second, 1, "second");
    assert_eq!(inside(&db, second), ["second"]);
    assert_eq!(inside(&db, first), ["b", "first"]);

    block_on(db.update("notes", row(1), body("plain"), Default::default())).unwrap();
    assert_eq!(outside(&db), ["plain"]);
    stage_update(&db, first, 2, "b, edited");
    let refused =
        block_on(
            db.tx_ref(first)
                .update("notes", row(9), body("missing"), Default::default()),
        );
    assert!(refused.is_err());
    assert_eq!(inside(&db, first), ["b, edited", "first"]);
    assert_eq!(outside(&db), ["plain"]);

    db.abandon_transaction_handle(second).unwrap();
    block_on(db.commit_mergeable_handle(first)).unwrap();
    assert_eq!(outside(&db), ["b, edited", "first"]);
}
