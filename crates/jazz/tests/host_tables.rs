//! Host tables: a host's own ordered key-value tables in its store's file
//! (a server's catalogue and account journal), reachable before a `Db`
//! opens on the file and through it afterwards, on the one connection.

use jazz_types::legacy_test_future::ResultFutureExt;
use std::collections::BTreeMap;

use jazz::db::{
    Db, DbConfig, DbIdentity, FileOptions, InsertOptions, ReadOpts, ReadTier, block_on,
};
use jazz::host::{HostTable, HostTables, StoreFile};
use jazz::ids::{AuthorSubject, NodeUuid};
use jazz::query::Query;
use jazz::records::Value;
use jazz::schema::JazzSchema;
use jazz::tools::{ColumnType, SchemaBuilder, TableSchemaBuilder};

use crate::common::{allow_all_policies, compile_schema};

const CATALOGUE: HostTable = HostTable::new("catalogue");
const JOURNAL: HostTable = HostTable::new("journal");

fn schema() -> JazzSchema {
    compile_schema(
        &SchemaBuilder::new()
            .table(
                TableSchemaBuilder::new("notes")
                    .column("title", ColumnType::Text)
                    .policies(allow_all_policies()),
            )
            .build(),
    )
}

fn config() -> DbConfig {
    DbConfig::new(
        schema(),
        DbIdentity {
            node: NodeUuid::from_bytes([0x5e; 16]),
            author: AuthorSubject::SYSTEM,
        },
    )
}

fn options() -> FileOptions {
    FileOptions {
        synchronous_full: true,
        ..FileOptions::default()
    }
}

fn pair(key: &[u8], value: &[u8]) -> (Vec<u8>, Vec<u8>) {
    (key.to_vec(), value.to_vec())
}

#[test]
fn host_tables_share_the_store_file_and_survive_reopening() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("jazz.sqlite");
    {
        // A new file: host tables written before any Db opens on it.
        let file = StoreFile::open(&path, options()).unwrap();
        assert_eq!(file.host_get(CATALOGUE, b"k").unwrap(), None);
        assert!(file.host_prefix(CATALOGUE, b"").unwrap().is_empty());
        file.host_put_many(CATALOGUE, &[pair(b"cat:2", b"two"), pair(b"cat:1", b"one")])
            .unwrap();
        assert_eq!(
            file.host_put_if_absent(CATALOGUE, b"cat:1", b"other")
                .unwrap(),
            Some(b"one".to_vec())
        );
        assert_eq!(
            file.host_put_if_absent(JOURNAL, b"0", b"first").unwrap(),
            None
        );
        // The Db opens on the same connection, and the host tables stay
        // reachable through it, beside its rows.
        let db = block_on(Db::open_file(config(), file)).unwrap();
        block_on(db.insert(
            "notes",
            BTreeMap::from([("title".to_owned(), Value::String("hello".to_owned()))]),
            InsertOptions::default(),
        ))
        .unwrap();
        db.host_put_many(JOURNAL, &[pair(b"1", b"second")]).unwrap();
        db.host_delete(CATALOGUE, b"cat:2").unwrap();
        block_on(db.close()).unwrap();
    }
    let db = block_on(Db::open_at(config(), &path, options())).unwrap();
    assert_eq!(
        db.host_prefix(CATALOGUE, b"cat:").unwrap(),
        vec![pair(b"cat:1", b"one")]
    );
    assert_eq!(
        db.host_prefix(JOURNAL, b"").unwrap(),
        vec![pair(b"0", b"first"), pair(b"1", b"second")]
    );
    // Host tables are not application tables; the rows are all there.
    let notes = db.prepare_query(&Query::from("notes")).unwrap();
    let rows = block_on(db.all(
        &notes,
        ReadOpts {
            tier: ReadTier::LocalOnly,
            ..ReadOpts::default()
        },
    ))
    .unwrap();
    assert_eq!(rows.len(), 1);
}

/// Alice's host tables share a file with Bob's unrelated application table.
/// Direct SQL represents Bob's application and verifies Jazz leaves it intact.
#[test]
fn unrelated_tables_survive_host_table_writes() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("jazz.sqlite");
    let other = rusqlite::Connection::open(&path).unwrap();
    other
        .execute_batch("CREATE TABLE theirs (x INTEGER); INSERT INTO theirs VALUES (7)")
        .unwrap();
    let file = StoreFile::open(&path, options()).unwrap();
    file.host_put_many(JOURNAL, &[pair(b"key", b"value")])
        .unwrap();
    assert_eq!(
        file.host_get(JOURNAL, b"key").unwrap(),
        Some(b"value".to_vec())
    );
    assert_eq!(
        other
            .query_row("SELECT x FROM theirs", [], |row| row.get::<_, i64>(0))
            .unwrap(),
        7
    );
}
