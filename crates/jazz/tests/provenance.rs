//! Row provenance (`$createdAt`, `$updatedAt`, `$createdBy`, `$updatedBy`):
//! engine-maintained per row version, readable, orderable and filterable by
//! name, kept by history, and never writable by the app. Not ported from Jazz.

mod common;

use std::collections::BTreeMap;

use jazz::db::{Db, DbConfig, DbIdentity, InsertOptions, ReadOpts, block_on};
use jazz::ids::{AuthorSubject, NodeUuid, RowUuid};
use jazz::query::{OrderDirection, Query, col, eq, lit};
use jazz::records::Value;
use jazz::tools::{ColumnType, OpenTransactionId, SchemaBuilder, TableSchemaBuilder};

use common::{allow_all_policies, compile_schema};

fn db() -> Db {
    let schema = compile_schema(
        &SchemaBuilder::new()
            .table(
                TableSchemaBuilder::new("notes")
                    .column("body", ColumnType::Text)
                    .policies(allow_all_policies()),
            )
            .build(),
    );
    block_on(Db::open(DbConfig::new(
        schema,
        DbIdentity {
            node: NodeUuid::from_bytes([3; 16]),
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

fn u64_cell(row: &jazz::db::CurrentRow, name: &str) -> u64 {
    match row.cells().find(|(n, _)| *n == name).unwrap().1 {
        Some(Value::Nullable(Some(v))) => match **v {
            Value::U64(v) => v,
            ref other => panic!("{other:?}"),
        },
        Some(Value::U64(v)) => *v,
        other => panic!("{name}: {other:?}"),
    }
}

#[test]
fn provenance_is_stamped_hidden_by_default_and_selectable() {
    let db = db();
    block_on(db.insert(
        "notes",
        body("a"),
        InsertOptions {
            row_id: Some(row(1)),
            ..Default::default()
        },
    ))
    .unwrap();
    let plain = db.prepare_query(&Query::from("notes")).unwrap();
    let rows = block_on(db.all(&plain, ReadOpts::default())).unwrap();
    assert!(
        rows[0].cells().all(|(name, _)| !name.starts_with('$')),
        "not in the default projection"
    );

    std::thread::sleep(std::time::Duration::from_millis(5));
    block_on(db.update("notes", row(1), body("b"), Default::default())).unwrap();
    let select = db
        .prepare_query(&Query::from("notes").select([
            "body",
            "$createdAt",
            "$updatedAt",
            "$createdBy.identity.subject",
        ]))
        .unwrap();
    let rows = block_on(db.all(&select, ReadOpts::default())).unwrap();
    let (created, updated) = (
        u64_cell(&rows[0], "$createdAt"),
        u64_cell(&rows[0], "$updatedAt"),
    );
    assert!(created > 1_700_000_000_000, "Unix ms");
    assert!(updated > created, "an update moves $updatedAt only");
}

#[test]
fn provenance_orders_and_filters_and_cannot_be_written() {
    let db = db();
    for n in 1..=3 {
        block_on(db.insert(
            "notes",
            body(&format!("n{n}")),
            InsertOptions {
                row_id: Some(row(n)),
                ..Default::default()
            },
        ))
        .unwrap();
        std::thread::sleep(std::time::Duration::from_millis(3));
    }
    let newest = db
        .prepare_query(
            &Query::from("notes")
                .order_by("$createdAt", OrderDirection::Desc)
                .limit(1),
        )
        .unwrap();
    assert_eq!(
        block_on(db.all(&newest, ReadOpts::default())).unwrap()[0].row_uuid(),
        row(3)
    );
    let (_, subject) = AuthorSubject::SYSTEM.principal_parts();
    let mine = db
        .prepare_query(&Query::from("notes").filter(eq(
            col("$createdBy.identity.subject"),
            lit(Value::String(subject)),
        )))
        .unwrap();
    assert_eq!(
        block_on(db.all(&mine, ReadOpts::default())).unwrap().len(),
        3
    );

    let forged = BTreeMap::from([("$createdAt".to_owned(), Value::U64(1))]);
    let error = block_on(db.update("notes", row(1), forged, Default::default())).unwrap_err();
    assert!(
        error.message.contains("engine-maintained"),
        "{}",
        error.message
    );
    let error = db
        .prepare_query(&Query::from("notes").order_by("$createdBy", OrderDirection::Asc))
        .err();
    let error = match error {
        Some(error) => error,
        None => block_on(
            db.all(
                &db.prepare_query(
                    &Query::from("notes").order_by("$createdBy", OrderDirection::Asc),
                )
                .unwrap(),
                ReadOpts::default(),
            ),
        )
        .unwrap_err(),
    };
    assert!(
        error.message.contains("not supported yet"),
        "{}",
        error.message
    );
}

#[test]
fn history_keeps_each_versions_provenance() {
    let db = db();
    block_on(db.insert(
        "notes",
        body("a"),
        InsertOptions {
            row_id: Some(row(1)),
            ..Default::default()
        },
    ))
    .unwrap();
    let select = db
        .prepare_query(&Query::from("notes").select(["$updatedAt"]))
        .unwrap();
    let first = u64_cell(
        &block_on(db.all(&select, ReadOpts::default())).unwrap()[0],
        "$updatedAt",
    );
    let open = OpenTransactionId::new();
    block_on(db.begin_exclusive(open)).unwrap();
    std::thread::sleep(std::time::Duration::from_millis(5));
    block_on(db.update("notes", row(1), body("b"), Default::default())).unwrap();
    let at_snapshot = block_on(db.exclusive_tx_ref(open).all_prepared(&select)).unwrap();
    assert_eq!(
        u64_cell(&at_snapshot[0], "$updatedAt"),
        first,
        "the as-of read sees the old version's stamp"
    );
    db.abandon_exclusive_handle(open).unwrap();
}
