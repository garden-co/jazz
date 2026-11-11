//! SPIKE: an open transaction's private view (PLAN §1 of the Durable
//! Objects plan): read-your-writes for inserts, updates and deletes while
//! committed writes land between the transaction's operations, isolation
//! from ordinary readers, serializable snapshots and conflicts, and cleanup
//! on commit, abandon and drop. Passes with the savepoint overlay and with
//! `JAZZ_TX_PRIVATE_LAYER=1`.

use crate::common;

use std::collections::BTreeMap;

use jazz::db::{Db, DbConfig, DbIdentity, ErrorCode, InsertOptions, ReadOpts, block_on};
use jazz::ids::{AuthorSubject, NodeUuid, RowUuid};
use jazz::query::OrderDirection;
use jazz::records::Value;
use jazz::tools::{ColumnType, OpenTransactionId, SchemaBuilder, TableSchemaBuilder};

fn private_mode() -> bool {
    std::env::var("JAZZ_TX_PRIVATE_LAYER").is_ok_and(|v| v == "1")
}

fn open() -> Db {
    let schema = common::compile_schema(
        &SchemaBuilder::new()
            .table(
                TableSchemaBuilder::new("todos")
                    .column("title", ColumnType::Text)
                    .column("done", ColumnType::Boolean)
                    .policies(common::allow_all_policies()),
            )
            .build(),
    );
    block_on(Db::open(DbConfig::new(
        schema,
        DbIdentity {
            node: NodeUuid::from_bytes([1; 16]),
            author: AuthorSubject::for_test_bytes([2; 16]),
        },
    )))
    .unwrap()
}

fn id(n: u8) -> RowUuid {
    RowUuid::from_bytes([n; 16])
}

fn todo(title: &str, done: bool) -> BTreeMap<String, Value> {
    BTreeMap::from([
        ("title".to_owned(), Value::String(title.to_owned())),
        ("done".to_owned(), Value::Bool(done)),
    ])
}

fn insert(db: &Db, n: u8, title: &str) {
    block_on(db.insert(
        "todos",
        todo(title, false),
        InsertOptions {
            row_id: Some(id(n)),
            ..Default::default()
        },
    ))
    .unwrap();
}

/// Titles of the live rows, ordered by title, as an ordinary reader sees
/// them.
fn committed(db: &Db) -> Vec<String> {
    let query = db
        .prepare_query(&db.table("todos").order_by("title", OrderDirection::Asc))
        .unwrap();
    titles(block_on(db.all(&query, ReadOpts::default())).unwrap())
}

/// The same, inside transaction `tx`.
fn in_tx(db: &Db, tx: OpenTransactionId) -> Vec<String> {
    let query = db
        .prepare_query(&db.table("todos").order_by("title", OrderDirection::Asc))
        .unwrap();
    titles(block_on(db.tx_ref(tx).all_prepared(&query)).unwrap())
}

fn titles(rows: Vec<jazz::db::CurrentRow>) -> Vec<String> {
    rows.iter()
        .map(|row| match row.cell("title") {
            Some(Value::String(title)) => title,
            other => panic!("unexpected title {other:?}"),
        })
        .collect()
}

#[test]
fn a_mergeable_transaction_reads_its_own_writes_as_committed_writes_land_between() {
    let db = open();
    insert(&db, 1, "a");
    insert(&db, 2, "b");
    insert(&db, 3, "c");
    let tx = OpenTransactionId::new();
    block_on(db.begin_mergeable(tx)).unwrap();

    block_on(db.tx_ref(tx).insert(
        "todos",
        todo("d (tx)", false),
        InsertOptions {
            row_id: Some(id(4)),
            ..Default::default()
        },
    ))
    .unwrap();
    // A committed write lands between the transaction's operations.
    insert(&db, 5, "e");
    block_on(
        db.tx_ref(tx)
            .update("todos", id(1), todo("a (tx)", true), Default::default()),
    )
    .unwrap();
    block_on(db.update(
        "todos",
        id(3),
        todo("c (outside)", false),
        Default::default(),
    ))
    .unwrap();
    block_on(db.tx_ref(tx).delete("todos", id(2), Default::default())).unwrap();
    block_on(db.delete("todos", id(5), Default::default())).unwrap();

    // The transaction sees its writes over the latest committed state.
    assert_eq!(in_tx(&db, tx), ["a (tx)", "c (outside)", "d (tx)"]);
    // Ordinary readers see none of them.
    assert_eq!(committed(&db), ["a", "b", "c (outside)"]);
    if private_mode() {
        assert_eq!(db.private_rows_for_test(), 3);
    }
    // A later write in the transaction builds on its earlier one.
    block_on(db.tx_ref(tx).update(
        "todos",
        id(4),
        todo("d (tx, again)", true),
        Default::default(),
    ))
    .unwrap();
    assert_eq!(in_tx(&db, tx), ["a (tx)", "c (outside)", "d (tx, again)"]);

    block_on(db.commit_mergeable_handle(tx)).unwrap();
    assert_eq!(committed(&db), ["a (tx)", "c (outside)", "d (tx, again)"]);
    assert_eq!(db.private_rows_for_test(), 0);
}

#[test]
fn a_staged_delete_then_reinsert_of_the_same_row_is_refused_as_before() {
    let db = open();
    insert(&db, 1, "a");
    let tx = OpenTransactionId::new();
    block_on(db.begin_mergeable(tx)).unwrap();
    block_on(db.tx_ref(tx).delete("todos", id(1), Default::default())).unwrap();
    insert(&db, 2, "b");
    let error = block_on(
        db.tx_ref(tx)
            .update("todos", id(1), todo("x", true), Default::default()),
    )
    .unwrap_err();
    assert_eq!(in_tx(&db, tx), ["b"]);
    // The refused write left nothing behind.
    block_on(db.commit_mergeable_handle(tx)).unwrap();
    assert_eq!(committed(&db), ["b"]);
    assert_ne!(error.code, ErrorCode::TransactionConflict);
}

#[test]
fn a_serializable_transaction_reads_its_snapshot_and_its_own_writes() {
    let db = open();
    insert(&db, 1, "a");
    insert(&db, 2, "b");
    let tx = OpenTransactionId::new();
    block_on(db.begin_serializable(tx)).unwrap();
    block_on(
        db.tx_ref(tx)
            .update("todos", id(1), todo("a (tx)", true), Default::default()),
    )
    .unwrap();
    // Committed after the snapshot: the transaction does not see it.
    insert(&db, 3, "c");
    block_on(db.update(
        "todos",
        id(2),
        todo("b (outside)", false),
        Default::default(),
    ))
    .unwrap();
    assert_eq!(in_tx(&db, tx), ["a (tx)", "b"]);
    block_on(db.tx_ref(tx).insert(
        "todos",
        todo("d (tx)", false),
        InsertOptions {
            row_id: Some(id(4)),
            ..Default::default()
        },
    ))
    .unwrap();
    assert_eq!(in_tx(&db, tx), ["a (tx)", "b", "d (tx)"]);
    // Its read of the whole table now differs: a conflict.
    let error = block_on(db.commit_serializable_handle(tx)).unwrap_err();
    assert_eq!(error.code, ErrorCode::TransactionConflict);
    assert_eq!(committed(&db), ["a", "b (outside)", "c"]);
    assert_eq!(db.private_rows_for_test(), 0);
}

#[test]
fn a_serializable_write_to_a_row_changed_since_the_snapshot_conflicts() {
    let db = open();
    insert(&db, 1, "a");
    let tx = OpenTransactionId::new();
    block_on(db.begin_serializable(tx)).unwrap();
    block_on(db.update(
        "todos",
        id(1),
        todo("a (outside)", false),
        Default::default(),
    ))
    .unwrap();
    let staged =
        block_on(
            db.tx_ref(tx)
                .update("todos", id(1), todo("a (tx)", true), Default::default()),
        );
    let committed_now = staged.and_then(|_| block_on(db.commit_serializable_handle(tx)));
    assert_eq!(
        committed_now.unwrap_err().code,
        ErrorCode::TransactionConflict
    );
    assert_eq!(committed(&db), ["a (outside)"]);
}

#[test]
fn abandoned_and_dropped_transactions_leave_no_private_rows() {
    let db = open();
    insert(&db, 1, "a");
    let tx = OpenTransactionId::new();
    block_on(db.begin_mergeable(tx)).unwrap();
    block_on(
        db.tx_ref(tx)
            .update("todos", id(1), todo("a (tx)", true), Default::default()),
    )
    .unwrap();
    if private_mode() {
        assert_eq!(db.private_rows_for_test(), 1);
    }
    db.abandon_transaction_handle(tx).unwrap();
    assert_eq!(db.private_rows_for_test(), 0);
    assert_eq!(committed(&db), ["a"]);

    // Two open transactions keep apart.
    let (one, two) = (OpenTransactionId::new(), OpenTransactionId::new());
    block_on(db.begin_mergeable(one)).unwrap();
    block_on(db.begin_mergeable(two)).unwrap();
    block_on(
        db.tx_ref(one)
            .update("todos", id(1), todo("a (one)", true), Default::default()),
    )
    .unwrap();
    block_on(
        db.tx_ref(two)
            .update("todos", id(1), todo("a (two)", true), Default::default()),
    )
    .unwrap();
    assert_eq!(in_tx(&db, one), ["a (one)"]);
    assert_eq!(in_tx(&db, two), ["a (two)"]);
    block_on(db.commit_mergeable_handle(two)).unwrap();
    assert_eq!(in_tx(&db, one), ["a (one)"]);
    assert_eq!(committed(&db), ["a (two)"]);
    db.abandon_transaction_handle(one).unwrap();
    assert_eq!(db.private_rows_for_test(), 0);
}
