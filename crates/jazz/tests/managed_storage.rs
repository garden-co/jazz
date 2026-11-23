//! Managed storage (what a Durable Object host gives the engine: its SQL,
//! with callback scopes as its only transactions), emulated over native
//! SQLite (`jazz::storage::managed_emulation`).
//!
//! A core there accepts finished transactions only: nothing it holds may
//! stay open between calls. Plain writes and reads work as anywhere; a
//! transaction handle's staged writes, which stay open between calls, are
//! refused with a typed error. Chunked large
//! values live in the store itself and read back after the object
//! restarts.

use crate::common;
use jazz_types::legacy_test_future::ResultFutureExt;

use std::collections::BTreeMap;

use jazz::db::{
    CurrentRow, Db, DbConfig, DbIdentity, Error, InsertOptions, ReadOpts, ReadTier, block_on,
};
use jazz::ids::{AuthorSubject, NodeUuid, RowUuid};
use jazz::query::Query;
use jazz::records::Value;
use jazz::storage::Storage;
use jazz::storage::managed_emulation::EmulatedObjectSql;
use jazz::tools::{ColumnType, OpenTransactionId, SchemaBuilder, TableSchemaBuilder};

use common::{allow_all_policies, compile_schema};

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
                author: AuthorSubject::SYSTEM,
            },
        ),
        Storage::from_backend(sql),
    ))
    .unwrap()
}

fn seat(room: &str, taken: bool) -> BTreeMap<String, Value> {
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

fn seats(db: &Db) -> Vec<CurrentRow> {
    let prepared = db.prepare_query(&Query::from("seats")).unwrap();
    block_on(db.all(&prepared, ReadOpts::for_read_tier(ReadTier::LocalOnly))).unwrap()
}

fn cell(row: &CurrentRow, column: &str) -> Value {
    match row.cell(column).unwrap() {
        Value::Nullable(Some(value)) => *value,
        value => value,
    }
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

fn is_refused_on_managed_storage(error: &Error) -> bool {
    error
        .to_string()
        .contains("is not supported on managed storage (Durable Objects) yet")
}

/// Writes and reads on managed storage, across a restart: a new Db over
/// the same storage (a Durable Object's next instance) finds what the
/// last one wrote, and nothing stays open after any call.
///
/// ```text
/// object #1: insert 1, insert 2, update 1 ── restart ── object #2: reads both
/// ```
#[test]
fn writes_read_back_after_a_restart() {
    let sql = EmulatedObjectSql::open_in_memory().unwrap();
    {
        let db = open_on(sql.clone());
        block_on(db.insert("seats", seat("A", false), row(1))).unwrap();
        block_on(db.insert("seats", seat("B", false), row(2))).unwrap();
        block_on(db.update(
            "seats",
            RowUuid::from_bytes([1; 16]),
            BTreeMap::from([("taken".to_owned(), Value::Bool(true))]),
            Default::default(),
        ))
        .unwrap();
    }
    let db = open_on(sql);
    let mut found: Vec<(Value, Value)> = seats(&db)
        .iter()
        .map(|row| (cell(row, "room"), cell(row, "taken")))
        .collect();
    found.sort_by_key(|(room, _)| format!("{room:?}"));
    assert_eq!(
        found,
        [
            (Value::String("A".into()), Value::Bool(true)),
            (Value::String("B".into()), Value::Bool(false)),
        ]
    );
}

/// A chunked value written on managed storage reads back whole after the
/// object restarts: the new Db's chunk cache is empty, so it reads the
/// value's leaves through the store rather than refusing the cell.
///
/// ```text
/// object #1: insert 300 KiB notes ── restart ── object #2: read, update, read
/// ```
#[test]
fn chunked_values_read_back_after_a_restart() {
    let sql = EmulatedObjectSql::open_in_memory().unwrap();
    let notes = text(7, LARGE);
    let mut cells = seat("A", false);
    cells.insert("notes".to_owned(), Value::String(notes.clone()));
    {
        let db = open_on(sql.clone());
        block_on(db.insert("seats", cells, row(1))).unwrap();
        assert_eq!(cell(&seats(&db)[0], "notes"), Value::String(notes.clone()));
    }
    let db = open_on(sql);
    assert_eq!(cell(&seats(&db)[0], "notes"), Value::String(notes.clone()));
    // A partial update after the restart keeps the value.
    block_on(db.update(
        "seats",
        RowUuid::from_bytes([1; 16]),
        BTreeMap::from([("taken".to_owned(), Value::Bool(true))]),
        Default::default(),
    ))
    .unwrap();
    assert_eq!(cell(&seats(&db)[0], "notes"), Value::String(notes));
}

/// A mergeable transaction keeps its staged writes applied between its
/// writes, which managed storage cannot: its first write is refused with
/// the typed error, nothing is written, and the store stays usable.
#[test]
fn a_mergeable_transaction_is_refused_on_managed_storage() {
    let db = open_on(EmulatedObjectSql::open_in_memory().unwrap());
    let refused = block_on(async {
        let tx = db.mergeable_tx().await?;
        tx.insert("seats", seat("A", true), row(1)).await?;
        tx.commit().await
    })
    .unwrap_err();
    assert!(is_refused_on_managed_storage(&refused), "{refused}");
    block_on(db.insert("seats", seat("B", false), row(2))).unwrap();
    assert_eq!(seats(&db).len(), 1);
}

/// A transaction handle keeps its staged writes applied between its
/// stages, which managed storage cannot: its first stage is refused with
/// the typed error, nothing is written, and the store stays usable.
#[test]
fn a_transaction_handles_stage_is_refused_on_managed_storage() {
    let db = open_on(EmulatedObjectSql::open_in_memory().unwrap());
    let id = OpenTransactionId::new();
    block_on(db.begin_mergeable(id)).unwrap();
    let refused = block_on(db.tx_ref(id).insert("seats", seat("A", true), row(1))).unwrap_err();
    assert!(is_refused_on_managed_storage(&refused), "{refused}");
    block_on(db.insert("seats", seat("B", false), row(2))).unwrap();
    assert_eq!(seats(&db).len(), 1);
}

/// A write that fails partway is undone whole on managed storage: its
/// scope rolls back through the host's transaction, so what it wrote
/// before failing (the large value's chunks) is not left behind. A trigger
/// in the object's SQL aborts the row's insert after its chunks are
/// written.
///
/// ```text
/// insert 1 (300 KiB) ── insert 2 (300 KiB): chunks written, row aborted ── nodes as after 1
/// ```
#[test]
fn a_write_failing_partway_leaves_nothing_on_managed_storage() {
    use jazz::storage::Backend as _;
    let sql = EmulatedObjectSql::open_in_memory().unwrap();
    let db = open_on(sql.clone());
    let mut first = seat("A", false);
    first.insert("notes".to_owned(), Value::String(text(1, LARGE)));
    block_on(db.insert("seats", first, row(1))).unwrap();
    let nodes = db.large_value_nodes_for_test().unwrap();
    let table: String = {
        let mut cursor = sql
            .query(
                "SELECT name FROM sqlite_master WHERE type = 'table' AND name LIKE '%seats%' \
                 AND name NOT LIKE '\\_jazz%' ESCAPE '\\'",
                &[],
            )
            .unwrap();
        match cursor.next().unwrap().unwrap() {
            [jazz::storage::Value::Text(name)] => String::from_utf8(name.clone()).unwrap(),
            other => panic!("{other:?}"),
        }
    };
    let quoted = table.replace('"', "\"\"");
    sql.execute_script(&format!(
        "CREATE TRIGGER refuse_second BEFORE INSERT ON \"{quoted}\" \
         WHEN (SELECT count(*) FROM \"{quoted}\") > 0 \
         BEGIN SELECT RAISE(ABORT, 'refused by the test'); END;"
    ))
    .unwrap();
    let mut second = seat("B", false);
    second.insert("notes".to_owned(), Value::String(text(2, LARGE)));
    assert!(block_on(db.insert("seats", second, row(2))).is_err());
    assert_eq!(db.large_value_nodes_for_test().unwrap(), nodes);
    assert_eq!(seats(&db).len(), 1);
}
