//! Managed storage (the owner a Durable Object host gives the engine),
//! emulated over native SQLite.
//!
//! Transactions: managed storage cannot hold a SQL scope open between
//! engine calls, so each stage and read of an open transaction replays the
//! batch inside one callback scope and undoes it. Readers outside the
//! transaction never see its writes; its own reads do; commit applies it
//! atomically.
//!
//! Large values: the chunk cache reads what it lacks through the managed
//! backend, so chunked values read back after the object restarts (a new
//! Db over the same storage) and the cache stays bounded.

use crate::common;

use std::collections::BTreeMap;

use jazz::db::{Db, DbConfig, DbIdentity, ErrorCode, InsertOptions, block_on};
use jazz::ids::{AuthorSubject, NodeUuid, RowUuid};
use jazz::query::{col, eq, lit};
use jazz::records::Value;
use jazz::storage::Storage;
use jazz::storage::managed_emulation::EmulatedObjectSql;
use jazz::tools::{ColumnType, OpenTransactionId, SchemaBuilder, TableSchemaBuilder};

use common::{allow_all_policies, compile_schema};

fn open() -> Db {
    open_on(EmulatedObjectSql::open_in_memory().unwrap())
}

fn open_on(sql: EmulatedObjectSql) -> Db {
    let schema = compile_schema(
        &SchemaBuilder::new()
            .table(
                TableSchemaBuilder::new("seats")
                    .column("room", ColumnType::Text)
                    .column("taken", ColumnType::Boolean)
                    .nullable_column("notes", ColumnType::Text)
                    .policies(allow_all_policies()),
            )
            .build(),
    );
    block_on(Db::open_storage(
        DbConfig::new(
            schema,
            DbIdentity {
                node: NodeUuid::from_bytes([1; 16]),
                author: AuthorSubject::for_test_bytes([2; 16]),
            },
        ),
        Storage::from_backend(sql),
    ))
    .unwrap()
}

fn seat_cells(room: &str, taken: bool) -> BTreeMap<String, Value> {
    BTreeMap::from([
        ("room".to_owned(), Value::String(room.to_owned())),
        ("taken".to_owned(), Value::Bool(taken)),
    ])
}

fn row(id: u8) -> InsertOptions {
    InsertOptions {
        row_id: Some(RowUuid::from_bytes([id; 16])),
        ..Default::default()
    }
}

fn take(taken: bool) -> BTreeMap<String, Value> {
    BTreeMap::from([("taken".to_owned(), Value::Bool(taken))])
}

/// A mergeable handle reads its own staged writes across several stages;
/// the store outside it sees none of them until commit, then all of them.
#[test]
fn a_mergeable_handle_reads_its_writes_and_commits_them_atomically() {
    let db = open();
    let all = db.prepare_query(&db.table("seats")).unwrap();
    let tx = OpenTransactionId::new();
    block_on(db.begin_mergeable(tx)).unwrap();
    block_on(
        db.tx_ref(tx)
            .insert("seats", seat_cells("A", false), row(1)),
    )
    .unwrap();
    block_on(
        db.tx_ref(tx)
            .insert("seats", seat_cells("A", false), row(2)),
    )
    .unwrap();
    block_on(db.tx_ref(tx).update(
        "seats",
        RowUuid::from_bytes([1; 16]),
        take(true),
        Default::default(),
    ))
    .unwrap();
    assert_eq!(block_on(db.tx_ref(tx).all_prepared(&all)).unwrap().len(), 2);
    let taken = db
        .prepare_query(&db.table("seats").filter(eq(col("taken"), lit(true))))
        .unwrap();
    assert_eq!(
        block_on(db.tx_ref(tx).all_prepared(&taken)).unwrap().len(),
        1
    );
    assert!(db.read(&all).unwrap().is_empty());
    block_on(db.commit_mergeable_handle(tx)).unwrap();
    assert_eq!(db.read(&all).unwrap().len(), 2);
    assert_eq!(db.read(&taken).unwrap().len(), 1);
}

/// Writes committed outside an open handle between its stages are seen by
/// it (mergeable), and its staged writes stay invisible to that writer.
#[test]
fn an_open_handle_interleaves_with_outside_commits() {
    let db = open();
    let all = db.prepare_query(&db.table("seats")).unwrap();
    let tx = OpenTransactionId::new();
    block_on(db.begin_mergeable(tx)).unwrap();
    block_on(
        db.tx_ref(tx)
            .insert("seats", seat_cells("A", false), row(1)),
    )
    .unwrap();
    block_on(db.insert("seats", seat_cells("B", false), row(2))).unwrap();
    assert_eq!(db.read(&all).unwrap().len(), 1);
    assert_eq!(block_on(db.tx_ref(tx).all_prepared(&all)).unwrap().len(), 2);
    block_on(db.tx_ref(tx).update(
        "seats",
        RowUuid::from_bytes([2; 16]),
        take(true),
        Default::default(),
    ))
    .unwrap();
    block_on(db.commit_mergeable_handle(tx)).unwrap();
    let taken = db
        .prepare_query(&db.table("seats").filter(eq(col("taken"), lit(true))))
        .unwrap();
    assert_eq!(db.read(&all).unwrap().len(), 2);
    assert_eq!(db.read(&taken).unwrap().len(), 1);
}

/// A staged write the store refuses (a taken id) fails at its stage and
/// leaves the batch's earlier writes intact.
#[test]
fn a_refused_stage_leaves_the_batch_usable() {
    let db = open();
    let all = db.prepare_query(&db.table("seats")).unwrap();
    let tx = OpenTransactionId::new();
    block_on(db.begin_mergeable(tx)).unwrap();
    block_on(
        db.tx_ref(tx)
            .insert("seats", seat_cells("A", false), row(1)),
    )
    .unwrap();
    block_on(
        db.tx_ref(tx)
            .insert("seats", seat_cells("A", false), row(1)),
    )
    .unwrap_err();
    assert_eq!(block_on(db.tx_ref(tx).all_prepared(&all)).unwrap().len(), 1);
    block_on(db.commit_mergeable_handle(tx)).unwrap();
    assert_eq!(db.read(&all).unwrap().len(), 1);
}

/// Serializable write skew on managed storage: the predicate read's
/// phantom conflicts at commit and nothing of the batch lands.
#[test]
fn a_serializable_phantom_conflicts_on_managed_storage() {
    let db = open();
    block_on(db.insert("seats", seat_cells("A", false), row(1))).unwrap();
    block_on(db.insert("seats", seat_cells("A", false), row(2))).unwrap();
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
        block_on(db.tx_ref(tx).all_prepared(&taken_in_a))
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
    block_on(db.tx_ref(tx).update(
        "seats",
        RowUuid::from_bytes([1; 16]),
        take(true),
        Default::default(),
    ))
    .unwrap();
    // Its own write shows through its snapshot; the outside one does not.
    assert_eq!(
        block_on(db.tx_ref(tx).all_prepared(&taken_in_a))
            .unwrap()
            .len(),
        1
    );
    let error = block_on(db.commit_serializable_handle(tx)).unwrap_err();
    assert_eq!(error.code, ErrorCode::TransactionConflict);
    assert_eq!(db.read(&taken_in_a).unwrap().len(), 1);
}

/// The Rust callback API on managed storage: reads inside see the batch,
/// an error abandons all of it.
#[test]
fn callback_transactions_commit_and_abandon_on_managed_storage() {
    let db = open();
    let all = db.prepare_query(&db.table("seats")).unwrap();
    block_on(db.transaction(async |tx| {
        tx.insert("seats", seat_cells("A", false), row(1)).await?;
        assert_eq!(tx.all_prepared(&all).await?.len(), 1);
        Ok(())
    }))
    .unwrap();
    let failed: Result<((), _), _> = block_on(db.transaction(async |tx| {
        tx.insert("seats", seat_cells("B", false), row(2)).await?;
        assert_eq!(tx.all_prepared(&all).await?.len(), 2);
        Err(jazz::db::Error::new(ErrorCode::Query, "abandon"))
    }));
    failed.unwrap_err();
    assert_eq!(db.read(&all).unwrap().len(), 1);
}

/// Above the 64 KiB inline threshold, and several leaves long.
const LARGE: usize = 300 * 1024;

/// Deterministic text without repeats, so the chunker cuts it into many
/// content-defined leaves.
fn text(seed: u64, len: usize) -> String {
    let mut state = seed.wrapping_mul(0x9E37_79B9_7F4A_7C15) | 1;
    (0..len)
        .map(|_| {
            state ^= state << 13;
            state ^= state >> 7;
            state ^= state << 17;
            (b'a' + (state % 26) as u8) as char
        })
        .collect()
}

fn notes_of(db: &Db, id: u8) -> Value {
    let query = db.prepare_query(&db.table("seats")).unwrap();
    let rows = db.read(&query).unwrap();
    let row = rows
        .iter()
        .find(|row| row.row_uuid() == RowUuid::from_bytes([id; 16]))
        .expect("row");
    match row
        .cells()
        .find(|(name, _)| *name == "notes")
        .and_then(|(_, value)| value.cloned())
        .unwrap()
    {
        Value::Nullable(Some(value)) => *value,
        value => value,
    }
}

fn with_notes(notes: &str) -> BTreeMap<String, Value> {
    let mut cells = seat_cells("A", false);
    cells.insert("notes".to_owned(), Value::String(notes.to_owned()));
    cells
}

/// A chunked value written on managed storage reads back whole after the
/// object restarts: the new Db's cache is empty, so it reads the value's
/// epoch and leaves through the backend rather than refusing the cell.
#[test]
fn chunked_values_read_back_after_a_restart() {
    let sql = EmulatedObjectSql::open_in_memory().unwrap();
    let notes = text(7, LARGE);
    {
        let db = open_on(sql.clone());
        block_on(db.insert("seats", with_notes(&notes), row(1))).unwrap();
        assert_eq!(notes_of(&db, 1), Value::String(notes.clone()));
    }
    let db = open_on(sql);
    assert_eq!(notes_of(&db, 1), Value::String(notes.clone()));
    // And a partial update of it, after the restart, edits that value.
    block_on(db.update(
        "seats",
        RowUuid::from_bytes([1; 16]),
        take(true),
        Default::default(),
    ))
    .unwrap();
    assert_eq!(notes_of(&db, 1), Value::String(notes));
}

/// A chunked value staged in an open transaction reads back inside it and
/// lands at commit, on managed storage.
#[test]
fn chunked_values_in_an_open_transaction() {
    let sql = EmulatedObjectSql::open_in_memory().unwrap();
    let notes = text(11, LARGE);
    {
        let db = open_on(sql.clone());
        let all = db.prepare_query(&db.table("seats")).unwrap();
        let tx = OpenTransactionId::new();
        block_on(db.begin_mergeable(tx)).unwrap();
        block_on(db.tx_ref(tx).insert("seats", with_notes(&notes), row(1))).unwrap();
        let staged = block_on(db.tx_ref(tx).all_prepared(&all)).unwrap();
        assert_eq!(staged.len(), 1);
        assert!(db.read(&all).unwrap().is_empty());
        block_on(db.commit_mergeable_handle(tx)).unwrap();
        assert_eq!(notes_of(&db, 1), Value::String(notes.clone()));
    }
    let db = open_on(sql);
    assert_eq!(notes_of(&db, 1), Value::String(notes));
}
