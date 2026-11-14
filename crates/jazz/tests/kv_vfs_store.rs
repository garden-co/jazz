//! Jazz's store on the Durable Object KV VFS (`jazz::kv_vfs`), over storage
//! in memory: what a Durable Object keeps survives a reopen from storage
//! alone, and the engine's in-process SQLite keeps the things that a store on
//! the object's own SQL API cannot (a transaction open across awaits, the
//! large-value reader connection).
//!
//! The rest of the engine's suites run on this VFS with `JAZZ_KV_VFS=1`.

use crate::common;

use std::cell::Cell;
use std::collections::BTreeMap;
use std::future::Future;
use std::path::Path;
use std::pin::{Pin, pin};
use std::rc::Rc;
use std::task::{Context, Poll, Waker};

use jazz::db::{Db, DbConfig, DbIdentity, FileOptions, InsertOptions, Journal, ReadOpts, block_on};
use jazz::ids::{AuthorSubject, NodeUuid, RowUuid};
use jazz::kv_vfs::testing::{MemKv, thread_vfs};
use jazz::kv_vfs::{KvVfsUtil, PAGE_SIZE};
use jazz::query::Query;
use jazz::records::Value;
use jazz::tools::{ColumnType, SchemaBuilder, TableSchemaBuilder};

use common::{allow_all_policies, compile_schema};

/// Above the 64 KiB inline threshold, and several chunk leaves long.
const LARGE: usize = 300 * 1024;

fn config() -> DbConfig {
    DbConfig::new(
        compile_schema(
            &SchemaBuilder::new()
                .table(
                    TableSchemaBuilder::new("docs")
                        .column("title", ColumnType::Text)
                        .column("body", ColumnType::Text)
                        .policies(allow_all_policies()),
                )
                .build(),
        ),
        DbIdentity {
            node: NodeUuid::from_bytes([7; 16]),
            author: AuthorSubject::SYSTEM,
        },
    )
}

fn options(vfs: &'static str) -> FileOptions {
    FileOptions {
        vfs: Some(vfs),
        journal: Journal::Memory,
        page_size: Some(PAGE_SIZE),
        ..FileOptions::default()
    }
}

/// One object: its storage, and the VFS file bound to it.
struct Object {
    kv: Rc<MemKv>,
    util: Rc<KvVfsUtil>,
    vfs: &'static str,
    path: String,
}

impl Object {
    fn new(name: &str) -> Self {
        let (vfs, util) = thread_vfs();
        Object {
            kv: Rc::new(MemKv::default()),
            util,
            vfs,
            path: format!("/kv-vfs-store-{name}.sqlite"),
        }
    }

    /// Open the store as a new isolate would: bound afresh to storage.
    fn open(&self) -> Db {
        self.util
            .attach(&self.path, self.kv.clone(), "store")
            .unwrap();
        block_on(Db::open_at(
            config(),
            Path::new(&self.path),
            options(self.vfs),
        ))
        .unwrap()
    }

    fn close(&self, db: Db) {
        block_on(db.close()).unwrap();
        drop(db);
        self.util.detach(&self.path);
    }
}

fn row(n: u8) -> RowUuid {
    RowUuid::from_bytes([n; 16])
}

fn doc(title: &str, body: String) -> BTreeMap<String, Value> {
    BTreeMap::from([
        ("title".to_owned(), Value::String(title.to_owned())),
        ("body".to_owned(), Value::String(body)),
    ])
}

fn insert(db: &Db, n: u8, title: &str, body: String) {
    block_on(db.insert(
        "docs",
        doc(title, body),
        InsertOptions {
            row_id: Some(row(n)),
            ..Default::default()
        },
    ))
    .unwrap();
}

/// Rows as (title, body), by title.
fn docs(db: &Db) -> Vec<(String, String)> {
    let prepared = db.prepare_query(&Query::from("docs")).unwrap();
    let mut rows: Vec<(String, String)> = block_on(db.all(&prepared, ReadOpts::default()))
        .unwrap()
        .iter()
        .map(|row| {
            let text = |name: &str| match row.cells().find(|(n, _)| *n == name) {
                Some((_, Some(Value::String(text)))) => text.clone(),
                other => panic!("{name}: {other:?}"),
            };
            (text("title"), text("body"))
        })
        .collect();
    rows.sort();
    rows
}

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

/// A future that stays pending until `open` is set: an await the
/// application makes in the middle of a transaction (a fetch, a timer).
struct Gate(Rc<Cell<bool>>);

impl Future for Gate {
    type Output = ();

    fn poll(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<()> {
        if self.0.get() {
            Poll::Ready(())
        } else {
            Poll::Pending
        }
    }
}

/// A store written, closed and reopened from storage alone (a new isolate)
/// reads back every row; WAL is refused at open.
#[test]
fn a_store_reopens_from_storage_alone() {
    let object = Object::new("reopen");
    let db = object.open();
    for n in 0..20 {
        insert(&db, n, &format!("doc {n:02}"), "short".to_owned());
    }
    object.close(db);

    let db = object.open();
    let rows = docs(&db);
    assert_eq!(rows.len(), 20);
    assert_eq!(rows[0].0, "doc 00");
    object.close(db);

    object
        .util
        .attach(&object.path, object.kv.clone(), "store")
        .unwrap();
    let Err(error) = block_on(Db::open_at(
        config(),
        Path::new(&object.path),
        FileOptions {
            journal: Journal::Wal,
            ..options(object.vfs)
        },
    )) else {
        panic!("WAL must be refused");
    };
    assert!(error.to_string().contains("not supported"), "{error}");
    object.util.detach(&object.path);
}

/// An application transaction stays open across an await that is pending
/// for as long as the application likes. Its savepoint lives in the
/// engine's own SQLite, so storage is not touched (no storage transaction
/// is held, other objects keep committing), it reads its own writes, and it
/// lands in storage as one commit:
///
/// ```text
/// alice: begin ─ insert a ─ await (pending) ─────────── insert b ─ commit
///                              │                                     │
/// storage:                  unchanged                        one commit {a, b}
/// bob (another object):     insert, commit ──► storage
/// ```
#[test]
fn a_transaction_stays_open_across_awaits_and_commits_once() {
    let alice = Object::new("tx-alice");
    let bob = Object::new("tx-bob");
    let alice_db = alice.open();
    let bob_db = bob.open();
    let commits = alice.kv.commits.get();

    let gate = Rc::new(Cell::new(false));
    let seen_inside = Rc::new(Cell::new(0));
    {
        let tx = alice_db.transaction(async |tx| {
            tx.insert("docs", doc("a", "first".to_owned()), Default::default())
                .await?;
            Gate(gate.clone()).await;
            let rows = tx
                .all_prepared(&alice_db.prepare_query(&Query::from("docs"))?)
                .await?;
            seen_inside.set(rows.len());
            tx.insert("docs", doc("b", "second".to_owned()), Default::default())
                .await
        });
        let mut tx = pin!(tx);
        let mut cx = Context::from_waker(Waker::noop());
        for _ in 0..10 {
            assert!(tx.as_mut().poll(&mut cx).is_pending());
        }

        // Mid-transaction: storage holds nothing of it, and another object's
        // store commits as usual.
        assert_eq!(alice.kv.commits.get(), commits);
        insert(&bob_db, 1, "bob's", "unrelated".to_owned());
        assert_eq!(docs(&bob_db).len(), 1);

        gate.set(true);
        let Poll::Ready(result) = tx.as_mut().poll(&mut cx) else {
            panic!("the transaction finishes once its await does");
        };
        result.unwrap();
        assert_eq!(seen_inside.get(), 1, "it read its own write");
        assert_eq!(alice.kv.commits.get(), commits + 1, "one storage commit");
    }

    alice.close(alice_db);
    bob.close(bob_db);
    let alice_db = alice.open();
    let titles: Vec<String> = docs(&alice_db).into_iter().map(|(t, _)| t).collect();
    assert_eq!(titles, ["a", "b"]);
    alice.close(alice_db);
}

/// A transaction that fails rolls back in the engine's SQLite: none of its
/// rows reach storage.
#[test]
fn a_failed_transaction_never_reaches_storage() {
    let object = Object::new("tx-failed");
    let db = object.open();
    insert(&db, 1, "kept", "x".to_owned());
    let stored = object.kv.data.borrow().clone();
    let commits = object.kv.commits.get();

    let result = block_on(db.transaction(async |tx| {
        tx.insert("docs", doc("dropped", "y".to_owned()), Default::default())
            .await?;
        Err::<(), _>(jazz::db::Error::new(
            jazz::db::ErrorCode::Query,
            "the application gave up",
        ))
    }));
    assert!(result.is_err());
    // SQLite still commits the emptied outer transaction, which rewrites
    // only the header page (its change counter): one row, no data.
    assert!(object.kv.commits.get() - commits <= 1);
    let changed: Vec<String> = object
        .kv
        .data
        .borrow()
        .iter()
        .filter(|(key, value)| stored.get(*key) != Some(value))
        .map(|(key, _)| key.clone())
        .collect();
    assert!(
        changed.iter().all(|key| key == "jazz-vfs:store:p:00000000"),
        "{changed:?}"
    );
    assert_eq!(docs(&db).len(), 1);
    object.close(db);
    let db = object.open();
    let titles: Vec<String> = docs(&db).into_iter().map(|(t, _)| t).collect();
    assert_eq!(titles, ["kept"], "nothing of it reached storage");
    object.close(db);
}

/// Large values are chunked into the same file and read back through the
/// engine's separate reader connection on the VFS, in the session that
/// wrote them and after a reopen from storage alone.
#[test]
fn large_values_read_back_through_the_reader_connection() {
    let object = Object::new("large");
    let values: Vec<String> = (0..4).map(|n| text(20 + n, LARGE)).collect();
    let db = object.open();
    for (n, value) in values.iter().enumerate() {
        insert(&db, n as u8 + 1, &format!("doc {n}"), value.clone());
    }
    let read: Vec<String> = docs(&db).into_iter().map(|(_, body)| body).collect();
    assert_eq!(read, values);
    object.close(db);

    let db = object.open();
    let read: Vec<String> = docs(&db).into_iter().map(|(_, body)| body).collect();
    assert_eq!(read, values);
    object.close(db);
    let bytes: usize = object.kv.data.borrow().values().map(Vec::len).sum();
    assert!(
        bytes >= 4 * LARGE,
        "the chunks are in storage ({bytes} bytes)"
    );
}

/// A write whose storage commit fails returns the error, the store refuses
/// everything after it (its memory may be ahead of storage), and a reopen
/// from storage holds exactly the commits before it:
///
/// ```text
/// insert 1, 2 ──► storage {1, 2}
/// insert 3 ──commit──✗ (storage full) ──► error; reads and writes fail
/// reopen ──► {1, 2}; insert 4 ──► storage {1, 2, 4}
/// ```
#[test]
fn a_write_whose_storage_commit_fails_fails_the_store_until_reopened() {
    let object = Object::new("failed-write");
    let db = object.open();
    insert(&db, 1, "one", "x".to_owned());
    insert(&db, 2, "two", "x".to_owned());

    object.kv.fail_commits.set(true);
    let error = block_on(db.insert(
        "docs",
        doc("three", "x".to_owned()),
        InsertOptions {
            row_id: Some(row(3)),
            ..Default::default()
        },
    ))
    .expect_err("the write's commit fails");
    // SQLite reports the VFS's error by its code only.
    assert!(error.to_string().contains("disk I/O error"), "{error}");
    object.kv.fail_commits.set(false);

    let prepared = db.prepare_query(&Query::from("docs")).unwrap();
    let error = block_on(db.all(&prepared, ReadOpts::default())).expect_err("reads fail");
    assert!(error.to_string().contains("disk I/O error"), "{error}");
    let error = block_on(db.insert("docs", doc("four", "x".to_owned()), Default::default()))
        .expect_err("writes fail");
    assert!(error.to_string().contains("disk I/O error"), "{error}");
    drop(db);
    object.util.detach(&object.path);

    let db = object.open();
    let titles: Vec<String> = docs(&db).into_iter().map(|(t, _)| t).collect();
    assert_eq!(titles, ["one", "two"]);
    insert(&db, 4, "four", "x".to_owned());
    object.close(db);
    let db = object.open();
    let titles: Vec<String> = docs(&db).into_iter().map(|(t, _)| t).collect();
    assert_eq!(titles, ["four", "one", "two"]);
    object.close(db);
}
