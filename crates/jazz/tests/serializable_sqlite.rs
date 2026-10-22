//! Serializable ("exclusive") transactions over SQLite: snapshot reads as an
//! as-of cut over current + history rows, read-your-writes through the same
//! cut, and commit-time validation of the write set and every read's result
//! (so phantoms entering a predicate or window conflict, and unrelated
//! concurrent writes do not). Not ported from Jazz: these pin the prototype's
//! design from sqlite-experiment/serializable.md.

mod common;

use std::collections::BTreeMap;

use jazz::db::{Db, DbConfig, DbIdentity, ErrorCode, InsertOptions, block_on};
use jazz::ids::{AuthorSubject, NodeUuid, RowUuid};
use jazz::query::{OrderDirection, col, eq, lit};
use jazz::records::Value;
use jazz::schema::JazzSchema;
use jazz::tools::{ColumnType, OpenTransactionId, SchemaBuilder, TableSchemaBuilder};

use common::{allow_all_policies, compile_schema};

fn schema() -> JazzSchema {
    compile_schema(
        &SchemaBuilder::new()
            .table(
                TableSchemaBuilder::new("seats")
                    .column("room", ColumnType::Text)
                    .column("taken", ColumnType::Boolean)
                    .policies(allow_all_policies()),
            )
            .build(),
    )
}

fn open() -> Db {
    let schema = schema();
    block_on(Db::open(DbConfig::new(
        schema,
        DbIdentity {
            node: NodeUuid::from_bytes([1; 16]),
            author: AuthorSubject::for_test_bytes([2; 16]),
        },
    )))
    .unwrap()
}

fn seat(db: &Db, id: u8, room: &str, taken: bool) {
    block_on(db.insert(
        "seats",
        BTreeMap::from([
            ("room".to_owned(), Value::String(room.to_owned())),
            ("taken".to_owned(), Value::Bool(taken)),
        ]),
        InsertOptions {
            row_id: Some(RowUuid::from_bytes([id; 16])),
            ..Default::default()
        },
    ))
    .unwrap();
}

fn take(cells: bool) -> BTreeMap<String, Value> {
    BTreeMap::from([("taken".to_owned(), Value::Bool(cells))])
}

/// Classic write skew: "book a seat only if room A has no taken seat".
/// Another writer takes a different seat in room A after the snapshot; the
/// transaction never wrote that row, but its predicate read changed.
#[test]
fn a_phantom_entering_a_predicate_read_conflicts() {
    let db = open();
    seat(&db, 1, "A", false);
    seat(&db, 2, "A", false);
    let tx = OpenTransactionId::new();
    block_on(db.begin_serializable(tx)).unwrap();
    let taken_in_a = db
        .prepare_query(
            &db.table("seats")
                .filter(eq(col("room"), lit("A")))
                .filter(eq(col("taken"), lit(true))),
        )
        .unwrap();
    assert!(
        block_on(db.serializable_tx_ref(tx).all_prepared(&taken_in_a))
            .unwrap()
            .is_empty()
    );
    block_on(db.update(
        "seats",
        RowUuid::from_bytes([2; 16]),
        take(true),
        Default::default(),
    ))
    .unwrap();
    block_on(db.serializable_tx_ref(tx).update(
        "seats",
        RowUuid::from_bytes([1; 16]),
        take(true),
        Default::default(),
    ))
    .unwrap();
    let error = block_on(db.commit_serializable_handle(tx)).unwrap_err();
    assert_eq!(error.code, ErrorCode::TransactionConflict);
    assert_eq!(
        error.to_string(),
        "(transaction_conflict): a row read by the transaction changed since its snapshot"
    );
}

/// A concurrent write outside everything the transaction read or wrote
/// does not conflict: validation is per read result, not per table.
#[test]
fn an_unrelated_concurrent_write_commits() {
    let db = open();
    seat(&db, 1, "A", false);
    seat(&db, 3, "B", false);
    let tx = OpenTransactionId::new();
    block_on(db.begin_serializable(tx)).unwrap();
    let room_a = db
        .prepare_query(&db.table("seats").filter(eq(col("room"), lit("A"))))
        .unwrap();
    assert_eq!(
        block_on(db.serializable_tx_ref(tx).all_prepared(&room_a))
            .unwrap()
            .len(),
        1
    );
    block_on(db.update(
        "seats",
        RowUuid::from_bytes([3; 16]),
        take(true),
        Default::default(),
    ))
    .unwrap();
    block_on(db.serializable_tx_ref(tx).update(
        "seats",
        RowUuid::from_bytes([1; 16]),
        take(true),
        Default::default(),
    ))
    .unwrap();
    block_on(db.commit_serializable_handle(tx)).unwrap();
    let all = db
        .prepare_query(&db.table("seats").filter(eq(col("taken"), lit(true))))
        .unwrap();
    assert_eq!(db.read(&all).unwrap().len(), 2);
}

/// Snapshot reads keep the pre-snapshot image of a row updated (and a row
/// created) after the snapshot, while the transaction's own writes show.
#[test]
fn snapshot_reads_see_history_images_and_own_writes() {
    let db = open();
    seat(&db, 1, "A", false);
    seat(&db, 2, "A", false);
    let tx = OpenTransactionId::new();
    block_on(db.begin_serializable(tx)).unwrap();
    // After the snapshot: seat 1 changes twice, seat 4 appears, seat 2 is deleted.
    block_on(db.update(
        "seats",
        RowUuid::from_bytes([1; 16]),
        take(true),
        Default::default(),
    ))
    .unwrap();
    block_on(db.update(
        "seats",
        RowUuid::from_bytes([1; 16]),
        BTreeMap::from([("room".to_owned(), Value::String("Z".to_owned()))]),
        Default::default(),
    ))
    .unwrap();
    seat(&db, 4, "A", false);
    block_on(db.delete("seats", RowUuid::from_bytes([2; 16]), Default::default())).unwrap();

    let ordered = db
        .prepare_query(&db.table("seats").order_by("room", OrderDirection::Asc))
        .unwrap();
    let rows = block_on(db.serializable_tx_ref(tx).all_prepared(&ordered)).unwrap();
    let seen: Vec<(RowUuid, Option<Value>)> = rows
        .iter()
        .map(|row| (row.row_uuid(), row.cell(&schema().tables[0], "room")))
        .collect();
    assert_eq!(
        seen,
        vec![
            (
                RowUuid::from_bytes([1; 16]),
                Some(Value::String("A".to_owned()))
            ),
            (
                RowUuid::from_bytes([2; 16]),
                Some(Value::String("A".to_owned()))
            ),
        ]
    );

    seat(&db, 5, "Q", false);
    let tx_ref = db.serializable_tx_ref(tx);
    block_on(tx_ref.insert(
        "seats",
        BTreeMap::from([
            ("room".to_owned(), Value::String("C".to_owned())),
            ("taken".to_owned(), Value::Bool(false)),
        ]),
        InsertOptions {
            row_id: Some(RowUuid::from_bytes([6; 16])),
            ..Default::default()
        },
    ))
    .unwrap();
    assert_eq!(block_on(tx_ref.all_prepared(&ordered)).unwrap().len(), 3);
    assert!(
        block_on(tx_ref.read("seats", RowUuid::from_bytes([5; 16])))
            .unwrap()
            .is_none()
    );
    db.abandon_serializable_handle(tx).unwrap();
    // Seats 1, 4 and 5; seat 2 is deleted and seat 6 was abandoned.
    assert_eq!(db.read(&ordered).unwrap().len(), 3);
}
