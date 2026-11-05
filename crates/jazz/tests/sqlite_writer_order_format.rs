//! Admission and corruption checks for the new accepted-writer metadata.
//! These do not replace the pending late-upload sync regression.
mod common;

use jazz::db::{Db, DbConfig, DbIdentity, ErrorCode, ReadOpts, block_on};
use jazz::ids::{AuthorSubject, NodeUuid, RowUuid};
use jazz::query::Query;
use jazz::records::Value;
use jazz::tools::{ColumnType, SchemaBuilder, TableSchemaBuilder};
use std::{collections::BTreeMap, path::Path};

fn config() -> DbConfig {
    DbConfig::new(
        common::compile_schema(
            &SchemaBuilder::new()
                .table(
                    TableSchemaBuilder::new("notes")
                        .column("text", ColumnType::Text)
                        .policies(common::allow_all_policies()),
                )
                .build(),
        ),
        DbIdentity {
            node: NodeUuid::from_bytes([1; 16]),
            author: AuthorSubject::SYSTEM,
        },
    )
}

fn open(path: &Path) -> Db {
    block_on(Db::open_at(config(), path, Default::default())).unwrap()
}

fn write(db: &Db, id: u8, text: &str) -> Result<u64, jazz::db::Error> {
    block_on(db.upsert(
        "notes",
        RowUuid::from_bytes([id; 16]),
        BTreeMap::from([("text".to_owned(), Value::String(text.to_owned()))]),
        Default::default(),
    ))
    .map(|write| write.mergeable_tx_id())
}

fn texts(db: &Db) -> Vec<String> {
    let query = db.prepare_query(&Query::from("notes")).unwrap();
    block_on(db.all(&query, ReadOpts::default()))
        .unwrap()
        .into_iter()
        .map(|row| match row.cell_at(0) {
            Some(Value::String(text)) => text,
            other => panic!("unexpected note: {other:?}"),
        })
        .collect()
}

fn frontier(path: &Path) -> (Vec<u8>, Vec<u8>, Vec<u8>, Vec<u8>) {
    // The store was created and closed through Db. Physical inspection is
    // needed because this internal format has no application query table.
    rusqlite::Connection::open(path)
        .unwrap()
        .query_row(
            "SELECT id,k,node,tx FROM _jazz_writer_order_v1",
            [],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?)),
        )
        .unwrap()
}

/// Alice's original writer identity and latest committed position survive a
/// public Db reopen. Physical inspection checks the new durable role, not rows
/// reconstructed through the same metadata reader.
#[test]
fn writer_order_format_survives_reopen_and_tracks_the_original_writer() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("authority.sqlite");
    let db = open(&path);
    let first = write(&db, 17, "first").unwrap();
    let origin = db.committed_transaction_id(first).unwrap();
    block_on(db.close()).unwrap();
    drop(db);
    assert_eq!(
        frontier(&path),
        (
            vec![17; 16],
            Vec::new(),
            vec![1; 16],
            origin.time.0.to_be_bytes().to_vec(),
        )
    );
    let storage = rusqlite::Connection::open(&path).unwrap();
    let profile: Vec<u8> = storage
        .query_row(
            "SELECT value FROM _jazz_writer_order_profile_v1 WHERE slot=1",
            [],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(profile, b"JAZZ-SQLITE-WRITER-ORDER\0\x01");
    drop(storage);

    let db = open(&path);
    assert_eq!(texts(&db), ["first"]);
    let next = write(&db, 17, "second").unwrap();
    let next_origin = db.committed_transaction_id(next).unwrap();
    assert!(next_origin.time > origin.time);
    assert_eq!(texts(&db), ["second"]);
    block_on(db.close()).unwrap();
    drop(db);
    assert_eq!(frontier(&path).3, next_origin.time.0.to_be_bytes());
}

/// Alice can read an earlier nonempty SQLite file, but Bob's authority cannot
/// silently initialise an empty writer history over it. A refused local write
/// leaves both the application rows and allocator position unchanged.
#[test]
fn earlier_authority_files_remain_readable_but_cannot_guess_writer_order() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("earlier.sqlite");
    let db = open(&path);
    write(&db, 17, "kept").unwrap();
    let high_water = db.seq_high_water();
    block_on(db.close()).unwrap();
    drop(db);
    // Remove only the new role to reproduce its absence in an earlier file.
    rusqlite::Connection::open(&path)
        .unwrap()
        .execute_batch(
            "DROP TABLE _jazz_writer_order_profile_v1; DROP TABLE _jazz_writer_order_v1;",
        )
        .unwrap();
    let db = open(&path);
    assert_eq!(texts(&db), ["kept"]);
    db.set_authority();
    assert_eq!(
        db.initialize_authority_catalogue().unwrap_err().code,
        ErrorCode::UnsupportedStorageCodecs
    );
    assert_eq!(
        write(&db, 18, "must-not-land").unwrap_err().code,
        ErrorCode::UnsupportedStorageCodecs
    );
    assert_eq!(texts(&db), ["kept"]);
    assert_eq!(db.seq_high_water(), high_water);
    block_on(db.close()).unwrap();
}

/// Mallory's unknown profile or trailing bytes cannot be treated as v1. The
/// corruption is injected physically because no public API writes this role.
#[test]
fn unsupported_writer_order_profiles_fail_open_without_rewriting_the_marker() {
    for marker in [
        Vec::new(),
        b"JAZZ-SQLITE-WRITER-ORDER\0\x02".to_vec(),
        b"JAZZ-SQLITE-WRITER-ORDER\0\x01\0".to_vec(),
    ] {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("corrupt.sqlite");
        let db = open(&path);
        block_on(db.close()).unwrap();
        drop(db);
        let storage = rusqlite::Connection::open(&path).unwrap();
        storage
            .execute(
                "UPDATE _jazz_writer_order_profile_v1 SET value=?1",
                [&marker],
            )
            .unwrap();
        drop(storage);
        let error = block_on(Db::open_at(config(), &path, Default::default()))
            .err()
            .expect("unsupported profile must refuse open");
        assert_eq!(error.code, ErrorCode::UnsupportedStorageCodecs);
        let saved: Vec<u8> = rusqlite::Connection::open(&path)
            .unwrap()
            .query_row(
                "SELECT value FROM _jazz_writer_order_profile_v1 WHERE slot=1",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(saved, marker);
    }
}

/// Corrupt ordering bytes cannot reset Alice's earlier position. A subsequent
/// write fails as a storage error and preserves the last committed row.
#[test]
fn corrupt_writer_order_values_do_not_reset_the_frontier() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("corrupt-frontier.sqlite");
    let db = open(&path);
    write(&db, 17, "kept").unwrap();
    block_on(db.close()).unwrap();
    drop(db);
    rusqlite::Connection::open(&path)
        .unwrap()
        .execute("UPDATE _jazz_writer_order_v1 SET tx=?1", [&[0u8; 8][..]])
        .unwrap();
    let db = open(&path);
    assert_eq!(
        write(&db, 17, "must-not-land").unwrap_err().code,
        ErrorCode::Storage
    );
    assert_eq!(texts(&db), ["kept"]);
    block_on(db.close()).unwrap();
}
