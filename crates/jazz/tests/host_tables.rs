//! Host tables: a host's own ordered key-value tables in its store's file
//! (a server's catalogue and account journal), reachable before a `Db`
//! opens on the file and through it afterwards, on the one connection.

use std::collections::BTreeMap;

use jazz::db::{
    Db, DbConfig, DbIdentity, ErrorCode, FileOptions, InsertOptions, ReadOpts, ReadTier, block_on,
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

/// Alice stores a large host value through the shared async algorithm; Bob
/// sees her existing value. The native future completes inline, and an outer rollback also undoes the shared operation's insert.
/// alice -> inline conditional insert -> outer rollback -> bob reads no claim
#[test]
fn shared_async_host_operation_stays_inline_and_obeys_outer_rollback() {
    let file = StoreFile::memory().unwrap();
    assert_eq!(
        block_on(file.host_put_if_absent(JOURNAL, b"", b"")).unwrap(),
        None
    );
    assert_eq!(
        block_on(file.host_get(JOURNAL, b"")).unwrap(),
        Some(Vec::new())
    );
    let value = vec![0xa5; 128 * 1024];
    assert_eq!(
        block_on(file.host_put_if_absent(JOURNAL, b"large", &value)).unwrap(),
        None
    );
    assert_eq!(
        block_on(file.host_put_if_absent(JOURNAL, b"large", b"bob")).unwrap(),
        Some(value.clone())
    );
    assert_eq!(
        block_on(file.host_get(JOURNAL, b"large")).unwrap(),
        Some(value)
    );
    let refused = block_on(file.host_sql(&mut |_| {
        assert_eq!(
            block_on(file.host_put_if_absent(JOURNAL, b"claim", b"alice"))?,
            None
        );
        Err(jazz::db::Error::new(ErrorCode::Storage, "host refused"))
    }));
    assert!(refused.is_err());
    assert_eq!(block_on(file.host_get(JOURNAL, b"claim")).unwrap(), None);
}

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
        assert_eq!(block_on(file.host_get(CATALOGUE, b"k")).unwrap(), None);
        assert!(
            block_on(file.host_prefix(CATALOGUE, b""))
                .unwrap()
                .is_empty()
        );
        block_on(file.host_put_many(CATALOGUE, &[pair(b"cat:2", b"two"), pair(b"cat:1", b"one")]))
            .unwrap();
        assert_eq!(
            block_on(file.host_put_if_absent(CATALOGUE, b"cat:1", b"other")).unwrap(),
            Some(b"one".to_vec())
        );
        assert_eq!(
            block_on(file.host_put_if_absent(JOURNAL, b"0", b"first")).unwrap(),
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
        block_on(db.host_put_many(JOURNAL, &[pair(b"1", b"second")])).unwrap();
        block_on(db.host_delete(CATALOGUE, b"cat:2")).unwrap();
        block_on(db.close()).unwrap();
    }
    let db = block_on(Db::open_at(config(), &path, options())).unwrap();
    assert_eq!(
        block_on(db.host_prefix(CATALOGUE, b"cat:")).unwrap(),
        vec![pair(b"cat:1", b"one")]
    );
    assert_eq!(
        block_on(db.host_prefix(JOURNAL, b"")).unwrap(),
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

#[test]
fn a_file_jazz_did_not_write_is_refused_before_any_host_table_is_touched() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("jazz.sqlite");
    rusqlite::Connection::open(&path)
        .unwrap()
        .execute_batch("CREATE TABLE theirs (x INTEGER)")
        .unwrap();
    let before = std::fs::read(&path).unwrap();
    let Err(error) = StoreFile::open(&path, options()) else {
        panic!("a foreign file must be refused");
    };
    assert_eq!(error.code, ErrorCode::UnsupportedStorageCodecs);
    assert_eq!(std::fs::read(&path).unwrap(), before);
}

/// Alice suspends while owning a host savepoint. Bob's host read, application
/// read, and insert wait for the same store; cancelling Alice rolls her write
/// back before Bob resumes. The test-only suspension models asynchronous SQL
/// without replacing the normal query, write, permission, or rollback paths.
/// alice -> write -> suspend -> bob queues -> cancel alice -> rollback -> bob
#[test]
fn suspended_storage_operation_serializes_reads_and_writes() {
    use std::future::Future;
    use std::task::{Context, Waker};

    let db = block_on(Db::open(config())).unwrap();
    let prepared = db.prepare_query(&Query::from("notes")).unwrap();
    let mut cx = Context::from_waker(Waker::noop());
    let mut alice = Box::pin(db.host_put_then_wait_for_test(
        JOURNAL,
        b"pending",
        b"alice",
        std::future::pending(),
    ));
    assert!(alice.as_mut().poll(&mut cx).is_pending());

    let mut host_read = Box::pin(db.host_get(JOURNAL, b"pending"));
    let mut read = Box::pin(db.all(
        &prepared,
        ReadOpts {
            tier: ReadTier::LocalOnly,
            ..ReadOpts::default()
        },
    ));
    let mut insert = Box::pin(db.insert(
        "notes",
        BTreeMap::from([("title".to_owned(), Value::String("bob".to_owned()))]),
        InsertOptions::default(),
    ));
    assert!(host_read.as_mut().poll(&mut cx).is_pending());
    assert!(read.as_mut().poll(&mut cx).is_pending());
    assert!(insert.as_mut().poll(&mut cx).is_pending());

    // Cancelling a queued operation must also remove its waiter safely.
    let mut cancelled = Box::pin(db.catalogue());
    assert!(cancelled.as_mut().poll(&mut cx).is_pending());
    drop(cancelled);
    drop(alice);

    assert_eq!(block_on(host_read).unwrap(), None);
    assert!(block_on(read).unwrap().is_empty());
    block_on(insert).unwrap();
    let rows = block_on(db.all(
        &prepared,
        ReadOpts {
            tier: ReadTier::LocalOnly,
            ..ReadOpts::default()
        },
    ))
    .unwrap();
    assert_eq!(rows.len(), 1);
    block_on(db.catalogue()).unwrap();
    block_on(db.close()).unwrap();
}
