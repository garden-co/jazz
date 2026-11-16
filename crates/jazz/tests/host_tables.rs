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

/// Alice and Bob share one deterministic ID sequence across schema handles.
/// IDs drawn during a suspended host write stay consumed even if it is cancelled;
/// the next insert continues that same sequence.
/// alice -> suspend storage -> alice/bob allocate -> cancel -> bob inserts
#[test]
fn row_ids_share_one_sequence_during_suspended_storage() {
    use jazz::db::{RowIdSource, SeededRowIdSource};
    use std::future::Future;
    use std::task::{Context, Waker};

    let mut expected = SeededRowIdSource::new(42);
    let db = block_on(Db::open(config().with_id_source(expected.clone()))).unwrap();
    let other = block_on(db.register_schema_view(schema())).unwrap();
    assert_eq!(db.next_row_id(), expected.next_row_id());
    let mut write = Box::pin(db.host_put_then_wait_for_test(
        JOURNAL,
        b"pending",
        b"alice",
        std::future::pending(),
    ));
    let mut cx = Context::from_waker(Waker::noop());
    assert!(write.as_mut().poll(&mut cx).is_pending());
    assert_eq!(other.next_row_id(), expected.next_row_id());
    assert_eq!(db.next_row_id(), expected.next_row_id());
    drop(write);

    let cells = || BTreeMap::from([("title".to_owned(), Value::String("bob".to_owned()))]);
    let inserted = block_on(other.insert("notes", cells(), InsertOptions::default())).unwrap();
    assert_eq!(inserted.row_uuid(), expected.next_row_id());
    let transaction = block_on(db.mergeable_tx()).unwrap();
    let staged = block_on(transaction.insert("notes", cells(), InsertOptions::default())).unwrap();
    assert_eq!(staged.row_uuid(), expected.next_row_id());
    block_on(transaction.commit()).unwrap();
    assert_eq!(other.next_row_id(), expected.next_row_id());
    let query = other.prepare_query(&Query::from("notes")).unwrap();
    let rows = block_on(other.read(&query)).unwrap();
    assert_eq!(rows.len(), 2);
    assert!(rows.iter().any(|row| row.row_uuid() == inserted.row_uuid()));
    assert!(rows.iter().any(|row| row.row_uuid() == staged.row_uuid()));
}

/// Alice's completed read is reported once while Bob's host write is paused.
/// Changing the diagnostic threshold during that pause applies to later reads.
/// alice -> query -> bob suspends -> host drains diagnostics -> bob resumes
#[test]
fn diagnostics_remain_available_during_suspended_storage() {
    use std::future::Future;
    use std::task::{Context, Waker};
    use std::time::Duration;

    let db = block_on(Db::open(config())).unwrap();
    let query = db.prepare_query(&Query::from("notes")).unwrap();
    db.set_slow_query_threshold(Some(Duration::ZERO));
    block_on(db.read(&query)).unwrap();
    let (resume, wait) = futures::channel::oneshot::channel();
    let mut write = Box::pin(
        db.host_put_then_wait_for_test(JOURNAL, b"pending", b"bob", async { wait.await.unwrap() }),
    );
    let mut cx = Context::from_waker(Waker::noop());
    assert!(write.as_mut().poll(&mut cx).is_pending());

    let (queries, dropped) = db.take_due_slow_queries().expect("completed slow read");
    assert_eq!(dropped, 0);
    assert_eq!(queries.len(), 1);
    assert_eq!(queries[0].runs, 1);
    assert_eq!(db.take_slow_queries(), (Vec::new(), 0));
    assert!(db.take_slow_query_report(None).is_empty());
    assert_eq!(db.take_merge_fallbacks(), (Vec::new(), 0));
    db.set_slow_query_threshold(None);

    resume.send(()).unwrap();
    block_on(write).unwrap();
    block_on(db.read(&query)).unwrap();
    assert!(db.take_slow_queries().0.is_empty());
    db.set_slow_query_threshold(Some(Duration::ZERO));
    block_on(db.read(&query)).unwrap();
    assert_eq!(db.take_slow_queries().0.len(), 1);
}

/// Alice's host write suspends while owning the store borrow. Another handle
/// can still check Alice/Bob admission and chunk progress without waiting.
/// The suspension hook models a pending SQL operation on the normal host path.
/// alice -> host write -> suspend -> sync checks -> resume -> committed read
#[test]
fn identity_and_chunk_progress_remain_available_during_suspended_storage() {
    use std::future::Future;
    use std::task::{Context, Waker};

    let alice = AuthorSubject::for_test_bytes([1; 16]);
    let bob = AuthorSubject::for_test_bytes([2; 16]);
    for author in [alice, AuthorSubject::SYSTEM] {
        let mut config = config();
        config.identity.author = author;
        let db = block_on(Db::open(config)).unwrap();
        let other = block_on(db.register_schema_view(schema())).unwrap();
        let answered = other.chunk_fetches_answered();
        let (resume, wait) = futures::channel::oneshot::channel();
        let mut write = Box::pin(db.host_put_then_wait_for_test(
            JOURNAL,
            b"pending",
            b"alice",
            async { wait.await.unwrap() },
        ));
        let mut cx = Context::from_waker(Waker::noop());
        assert!(write.as_mut().poll(&mut cx).is_pending());

        assert_eq!(other.chunk_fetches_answered(), answered);
        other.check_serves(alice).unwrap();
        if author == AuthorSubject::SYSTEM {
            other.check_serves(bob).unwrap();
        } else {
            assert_eq!(
                other.check_serves(bob).unwrap_err().reason,
                Some(jazz::db::Reason::IdentityMismatch),
            );
        }

        resume.send(()).unwrap();
        block_on(write).unwrap();
        assert_eq!(
            block_on(other.host_get(JOURNAL, b"pending")).unwrap(),
            Some(b"alice".to_vec()),
        );
    }
}

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

/// Alice opens Jazz beside Bob's existing SQLite data and writes host metadata.
/// Bob's table definition and contents survive initialization and reopening.
#[test]
fn a_shared_file_preserves_foreign_tables_beside_host_metadata() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("jazz.sqlite");
    let other = rusqlite::Connection::open(&path).unwrap();
    other
        .execute_batch("CREATE TABLE theirs (x INTEGER); INSERT INTO theirs VALUES (42)")
        .unwrap();
    {
        let file = StoreFile::open(&path, options()).unwrap();
        block_on(file.host_put_many(CATALOGUE, &[pair(b"alice", b"metadata")])).unwrap();
        let db = block_on(Db::open_file(config(), file)).unwrap();
        block_on(db.close()).unwrap();
    }
    let db = block_on(Db::open_at(config(), &path, options())).unwrap();
    assert_eq!(
        block_on(db.host_get(CATALOGUE, b"alice")).unwrap(),
        Some(b"metadata".to_vec())
    );
    assert_eq!(
        other
            .query_row(
                "SELECT sql FROM sqlite_master WHERE name = 'theirs'",
                [],
                |r| r.get::<_, String>(0)
            )
            .unwrap(),
        "CREATE TABLE theirs (x INTEGER)"
    );
    let values: Vec<i64> = other
        .prepare("SELECT x FROM theirs")
        .unwrap()
        .query_map([], |r| r.get(0))
        .unwrap()
        .collect::<Result<_, _>>()
        .unwrap();
    assert_eq!(values, [42]);
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

/// Alice abandons a staged batch and drops another transaction while Bob's
/// host SQL is suspended. Neither cleanup borrows execution state, and reusing
/// the first ID must not revive its cached writes.
#[test]
fn transaction_cleanup_remains_available_during_suspended_storage() {
    use jazz::tools::OpenTransactionId;
    use std::future::Future;
    use std::task::{Context, Waker};
    let db = block_on(Db::open(config())).unwrap();
    let id = OpenTransactionId::new();
    block_on(db.begin_mergeable(id)).unwrap();
    block_on(db.tx_ref(id).insert(
        "notes",
        BTreeMap::from([("title".to_owned(), Value::String("abandoned".to_owned()))]),
        Default::default(),
    ))
    .unwrap();
    let tx = block_on(db.serializable_tx()).unwrap();
    let mut write = Box::pin(db.host_put_then_wait_for_test(
        JOURNAL,
        b"pending",
        b"bob",
        std::future::pending(),
    ));
    assert!(
        write
            .as_mut()
            .poll(&mut Context::from_waker(Waker::noop()))
            .is_pending()
    );
    db.abandon_transaction_handle(id).unwrap();
    assert!(db.abandon_transaction_handle(id).is_err());
    drop(tx);
    drop(write);
    block_on(db.begin_mergeable(id)).unwrap();
    block_on(db.tx_ref(id).insert(
        "notes",
        BTreeMap::from([("title".to_owned(), Value::String("replacement".to_owned()))]),
        Default::default(),
    ))
    .unwrap();
    block_on(db.commit_mergeable_handle(id)).unwrap();
    let query = db.prepare_query(&Query::from("notes")).unwrap();
    let rows = block_on(db.read(&query)).unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(
        rows[0].cell("title"),
        Some(Value::String("replacement".to_owned()))
    );
}

/// Alice abandons a batch with a cached SQL overlay, then reuses its ID for a
/// batch of the same length. Bob must see only the replacement's staged row.
#[test]
fn reused_transaction_id_does_not_reuse_an_abandoned_overlay() {
    use jazz::tools::OpenTransactionId;
    let db = block_on(Db::open(config())).unwrap();
    let id = OpenTransactionId::new();
    for title in ["abandoned", "replacement"] {
        block_on(db.begin_mergeable(id)).unwrap();
        block_on(db.tx_ref(id).insert(
            "notes",
            BTreeMap::from([("title".to_owned(), Value::String(title.to_owned()))]),
            Default::default(),
        ))
        .unwrap();
        if title == "abandoned" {
            db.abandon_transaction_handle(id).unwrap();
        }
    }
    block_on(db.commit_mergeable_handle(id)).unwrap();
    let query = db.prepare_query(&Query::from("notes")).unwrap();
    let rows = block_on(db.read(&query)).unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(
        rows[0].cell("title"),
        Some(Value::String("replacement".to_owned()))
    );
}
