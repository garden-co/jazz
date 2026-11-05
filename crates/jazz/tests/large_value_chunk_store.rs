//! Large-value bytes in a chunk store (`jazz::chunk_store`): a store opened
//! with one moves the bytes of settled leaves there on tick and reads them
//! back from it, whether it answers at once (a directory) or only
//! asynchronously (an object store, as S3 or R2 from a Durable Object);
//! rows, their tree metadata and fresh nodes stay in the SQLite file, so a
//! row and the tree it names still commit together.

use crate::common;

use std::cell::RefCell;
use std::collections::{BTreeMap, VecDeque};
use std::path::{Path, PathBuf};
use std::rc::Rc;
use std::sync::{Arc, Mutex};
use std::task::Poll;

use bytes::Bytes;
use jazz::chunk_store::{ChunkStore, FileChunkStore, NodeKey, StoreFuture};
use jazz::db::{
    Db, DbConfig, DbIdentity, ErrorCode, FileOptions, InsertOptions, ReadOpts, ReadTier,
    SyncMessage, Transport, TransportError, block_on,
};
use jazz::ids::{AuthorSubject, NodeUuid, RowUuid};
use jazz::query::{Query, col, eq, lit};
use jazz::records::Value;
use jazz::tools::{ColumnType, SchemaBuilder, TableSchemaBuilder};

use common::{allow_all_policies, compile_schema};

/// Above the 64 KiB inline threshold, and several leaves long.
const LARGE: usize = 300 * 1024;

fn config(node: u8, author: AuthorSubject) -> DbConfig {
    let docs = TableSchemaBuilder::new("docs")
        .column("body", ColumnType::Text)
        .column("blob", ColumnType::Bytea)
        .policies(allow_all_policies());
    DbConfig::new(
        compile_schema(&SchemaBuilder::new().table(docs).build()),
        DbIdentity {
            node: NodeUuid::from_bytes([node; 16]),
            author,
        },
    )
}

fn open(path: &Path, store: Option<Arc<dyn ChunkStore>>) -> Result<Db, jazz::db::Error> {
    let mut config = config(0, AuthorSubject::SYSTEM);
    if let Some(store) = store {
        config = config.with_chunk_store(store);
    }
    block_on(Db::open_at(config, path, FileOptions::default()))
}

fn scratch() -> (tempfile::TempDir, PathBuf) {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("store.sqlite");
    (dir, path)
}

fn row(n: u8) -> RowUuid {
    RowUuid::from_bytes([n; 16])
}

fn user(n: u8) -> AuthorSubject {
    AuthorSubject::for_test_bytes([n; 16])
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

fn insert(db: &Db, n: u8, body: &str) {
    block_on(db.insert(
        "docs",
        BTreeMap::from([
            ("body".to_owned(), Value::String(body.to_owned())),
            ("blob".to_owned(), Value::Bytes(body.as_bytes().to_vec())),
        ]),
        InsertOptions {
            row_id: Some(row(n)),
            ..Default::default()
        },
    ))
    .unwrap();
}

fn cell(db: &Db, n: u8, column: &str) -> Result<Option<Value>, jazz::db::Error> {
    let query = Query::from("docs").filter(eq(col("id"), lit(Value::Uuid(row(n).0))));
    let rows = block_on(db.all(
        &db.prepare_query(&query).unwrap(),
        ReadOpts {
            tier: ReadTier::LocalOnly,
            ..ReadOpts::default()
        },
    ))?;
    Ok(rows.first().and_then(|row| {
        row.cells()
            .find(|(name, _)| *name == column)
            .and_then(|(_, value)| value.cloned())
    }))
}

fn body(db: &Db, n: u8) -> String {
    match cell(db, n, "body").unwrap() {
        Some(Value::String(body)) => body,
        other => panic!("row {n} body: {other:?}"),
    }
}

/// Node bytes the SQLite file still holds, and how many nodes it keeps
/// only as metadata. White-box: what moved where is the point of the
/// feature, and the public API reads the same either way.
fn file_node_bytes(path: &Path) -> (i64, i64) {
    let conn = rusqlite::Connection::open(path).unwrap();
    conn.query_row(
        "SELECT COALESCE(SUM(length(bytes)), 0), \
         COALESCE(SUM(length(bytes) = 0), 0) FROM _jazz_lv_node",
        [],
        |r| Ok((r.get(0)?, r.get(1)?)),
    )
    .unwrap()
}

fn files_in(dir: &Path) -> usize {
    walk(dir).len()
}

fn walk(dir: &Path) -> Vec<PathBuf> {
    let mut out = Vec::new();
    for entry in std::fs::read_dir(dir).unwrap().flatten() {
        let path = entry.path();
        if path.is_dir() {
            out.extend(walk(&path));
        } else {
            out.push(path);
        }
    }
    out
}

/// Objects a [`Remote`] holds, and what was asked of it.
#[derive(Default)]
struct Objects {
    map: BTreeMap<String, Vec<u8>>,
    gets: usize,
    puts: usize,
    deletes: usize,
    /// Puts fail while set, as an unreachable bucket does.
    failing: bool,
    /// Gets fail while set.
    failing_gets: bool,
}

/// A chunk store that answers only asynchronously, as S3 or R2 do: every
/// operation is pending once before it completes.
#[derive(Clone, Default)]
struct Remote(Arc<Mutex<Objects>>);

impl Remote {
    fn objects(&self) -> std::sync::MutexGuard<'_, Objects> {
        self.0.lock().unwrap()
    }
}

/// Pending once, then `value`.
fn later<'a, T: 'a>(value: impl FnOnce() -> Result<T, jazz::db::Error> + 'a) -> StoreFuture<'a, T> {
    let mut value = Some(value);
    let mut yielded = false;
    Box::pin(std::future::poll_fn(move |cx| {
        if !yielded {
            yielded = true;
            cx.waker().wake_by_ref();
            return Poll::Pending;
        }
        Poll::Ready((value.take().expect("polled once more"))())
    }))
}

impl ChunkStore for Remote {
    fn put<'a>(&'a self, node: &'a NodeKey, bytes: &'a [u8]) -> StoreFuture<'a, ()> {
        later(move || {
            let mut objects = self.objects();
            if objects.failing {
                return Err(jazz::db::Error::new(
                    ErrorCode::Storage,
                    "bucket unreachable",
                ));
            }
            objects.puts += 1;
            objects.map.insert(node.object_name(), bytes.to_vec());
            Ok(())
        })
    }

    fn get<'a>(&'a self, node: &'a NodeKey) -> StoreFuture<'a, Option<Bytes>> {
        later(move || {
            let mut objects = self.objects();
            if objects.failing_gets {
                return Err(jazz::db::Error::new(
                    ErrorCode::Storage,
                    "bucket unreachable",
                ));
            }
            objects.gets += 1;
            Ok(objects
                .map
                .get(&node.object_name())
                .cloned()
                .map(Bytes::from))
        })
    }

    fn delete<'a>(&'a self, node: &'a NodeKey) -> StoreFuture<'a, ()> {
        later(move || {
            let mut objects = self.objects();
            objects.deletes += 1;
            objects.map.remove(&node.object_name());
            Ok(())
        })
    }
}

/// The normal path with a directory store:
///
/// ```
/// insert ──► SQLite (row + nodes) ──tick──► leaves put to the directory
///                                    └────► their bytes dropped from SQLite
/// reopen ──► reads, filters and orders read leaves from the directory
/// ```
#[test]
fn settled_leaves_move_to_a_directory_and_read_back_after_reopening() {
    let (dir, path) = scratch();
    let chunks = dir.path().join("chunks");
    let store =
        || -> Option<Arc<dyn ChunkStore>> { Some(Arc::new(FileChunkStore::new(&chunks).unwrap())) };
    let values: Vec<String> = (0..3).map(|n| text(40 + n, LARGE)).collect();
    {
        let db = open(&path, store()).unwrap();
        for (n, value) in values.iter().enumerate() {
            insert(&db, n as u8 + 1, value);
        }
        let (before, moved) = file_node_bytes(&path);
        assert!(before > 3 * 2 * LARGE as i64 / 2, "{before}");
        assert_eq!(moved, 0, "nothing moves before a tick");
        block_on(db.tick()).unwrap();
        let (after, moved) = file_node_bytes(&path);
        assert!(moved > 6, "leaves moved: {moved}");
        assert!(after * 20 < before, "branches stay: {after} of {before}");
        assert_eq!(files_in(&chunks) as i64, moved);
        // Still read through the cache in this session.
        assert_eq!(body(&db, 2), values[1]);
    }
    let db = open(&path, store()).unwrap();
    for (n, value) in values.iter().enumerate() {
        assert_eq!(body(&db, n as u8 + 1), *value);
        assert_eq!(
            cell(&db, n as u8 + 1, "blob").unwrap(),
            Some(Value::Bytes(value.as_bytes().to_vec()))
        );
    }
    // An append reads the last leaf back from the directory.
    block_on(db.append_value("docs", row(1), "body", b"!".to_vec())).unwrap();
    assert_eq!(body(&db, 1), format!("{}!", values[0]));
}

/// A store whose leaves are in a chunk store is incomplete without it:
/// opening it alone is refused, never served with values missing.
#[test]
fn a_store_that_moved_leaves_refuses_to_open_without_its_chunk_store() {
    let (dir, path) = scratch();
    let chunks = dir.path().join("chunks");
    {
        let db = open(&path, Some(Arc::new(FileChunkStore::new(&chunks).unwrap()))).unwrap();
        insert(&db, 1, &text(50, LARGE));
        block_on(db.tick()).unwrap();
    }
    let error = open(&path, None).err().expect("refused");
    assert_eq!(error.code, ErrorCode::Storage);
    assert!(error.message.contains("chunk store"), "{}", error.message);
}

/// A file written before it had a chunk store gets one: the first open
/// with it queues every leaf the file holds, and ticks move them.
#[test]
fn a_store_given_a_chunk_store_later_moves_its_existing_leaves() {
    let (dir, path) = scratch();
    let chunks = dir.path().join("chunks");
    let value = text(51, LARGE);
    {
        let db = open(&path, None).unwrap();
        insert(&db, 1, &value);
    }
    let db = open(&path, Some(Arc::new(FileChunkStore::new(&chunks).unwrap()))).unwrap();
    block_on(db.tick()).unwrap();
    let (_, moved) = file_node_bytes(&path);
    assert!(moved > 2, "{moved}");
    drop(db);
    let db = open(&path, Some(Arc::new(FileChunkStore::new(&chunks).unwrap()))).unwrap();
    assert_eq!(body(&db, 1), value);
}

/// A chunk store that answers only asynchronously: a cold read records the
/// leaves it needs, the read itself awaits them and runs again; writes that
/// build on a moved value (an append reads its last leaf) do the same.
///
/// ```
/// all() ──read──✗ (leaf only in the store) ──await get──► all() ──✓
/// ```
#[test]
fn reads_and_writes_await_an_asynchronous_chunk_store() {
    let (_dir, path) = scratch();
    let remote = Remote::default();
    let value = text(52, LARGE);
    {
        let db = open(&path, Some(Arc::new(remote.clone()))).unwrap();
        insert(&db, 1, &value);
        block_on(db.tick()).unwrap();
    }
    let moved = remote.objects().map.len();
    assert!(moved > 2, "{moved}");
    assert_eq!(remote.objects().gets, 0);
    let db = open(&path, Some(Arc::new(remote.clone()))).unwrap();
    assert_eq!(body(&db, 1), value);
    assert!(remote.objects().gets >= moved, "every leaf fetched");
    let fetched = remote.objects().gets;
    // Fetched leaves are cached: reading again asks the store nothing.
    assert_eq!(body(&db, 1), value);
    assert_eq!(remote.objects().gets, fetched);
    drop(db);
    let db = open(&path, Some(Arc::new(remote.clone()))).unwrap();
    block_on(db.append_value("docs", row(1), "body", b"<end>".to_vec())).unwrap();
    assert_eq!(body(&db, 1), format!("{value}<end>"));
}

/// A put that fails leaves the node's bytes in SQLite: reads never notice,
/// the tick reports the failure, and a later tick moves the node.
#[test]
fn a_failed_put_keeps_the_bytes_and_is_retried() {
    let (_dir, path) = scratch();
    let remote = Remote::default();
    let value = text(53, LARGE);
    let db = open(&path, Some(Arc::new(remote.clone()))).unwrap();
    insert(&db, 1, &value);
    remote.objects().failing = true;
    let error = block_on(db.tick())
        .err()
        .expect("the tick reports the failure");
    assert_eq!(error.code, ErrorCode::Storage);
    assert_eq!(file_node_bytes(&path).1, 0, "nothing moved");
    assert_eq!(body(&db, 1), value);
    remote.objects().failing = false;
    block_on(db.tick()).unwrap();
    assert!(file_node_bytes(&path).1 > 2);
    drop(db);
    let db = open(&path, Some(Arc::new(remote.clone()))).unwrap();
    assert_eq!(body(&db, 1), value);
}

/// A duplex between a replica and its server.
struct Pipe {
    out: Rc<RefCell<VecDeque<SyncMessage>>>,
    inbound: Rc<RefCell<VecDeque<SyncMessage>>>,
}

impl Transport for Pipe {
    fn send(&mut self, message: SyncMessage) -> Result<(), TransportError> {
        self.out.borrow_mut().push_back(message);
        Ok(())
    }

    fn try_recv(&mut self) -> Option<SyncMessage> {
        self.inbound.borrow_mut().pop_front()
    }
}

/// A server whose leaves are only in an asynchronous chunk store serves a
/// replica's chunk requests once its tick has fetched them: the requests
/// wait, they are not answered "missing".
///
/// ```
/// alice ──ChunkRequests──► server ──(leaves only in the store)── held
///                            └─tick: get from store──► Chunks ──► alice
/// ```
#[test]
fn a_server_serves_chunk_requests_from_an_asynchronous_chunk_store() {
    let (_dir, path) = scratch();
    let remote = Remote::default();
    let value = text(54, LARGE);
    {
        let server = open(&path, Some(Arc::new(remote.clone()))).unwrap();
        insert(&server, 1, &value);
        block_on(server.tick()).unwrap();
    }
    let server = open(&path, Some(Arc::new(remote.clone()))).unwrap();
    let alice = block_on(Db::open(config(1, user(1)))).unwrap();
    let up = Rc::new(RefCell::new(VecDeque::new()));
    let down = Rc::new(RefCell::new(VecDeque::new()));
    let _ = block_on(alice.connect_upstream(Box::new(Pipe {
        out: up.clone(),
        inbound: down.clone(),
    })));
    server
        .accept_subscriber(
            Box::new(Pipe {
                out: down,
                inbound: up,
            }),
            user(1),
        )
        .unwrap();
    let _stream = block_on(alice.subscribe(
        &alice.prepare_query(&Query::from("docs")).unwrap(),
        ReadOpts::default(),
    ))
    .unwrap();
    for _ in 0..16 {
        block_on(alice.tick()).unwrap();
        block_on(server.tick()).unwrap();
    }
    block_on(alice.tick()).unwrap();
    assert_eq!(body(&alice, 1), value);
    assert!(remote.objects().gets > 0);
}

fn open_host_io(path: &Path, store: Arc<dyn ChunkStore>) -> Db {
    let config = config(0, AuthorSubject::SYSTEM)
        .with_chunk_store(store)
        .with_host_chunk_io();
    block_on(Db::open_at(config, path, FileOptions::default())).unwrap()
}

/// Run a batch on another thread, as a server runs it beside its store
/// thread.
fn run_elsewhere(
    io: jazz::chunk_store::ChunkIo,
    store: Arc<dyn ChunkStore>,
) -> jazz::chunk_store::ChunkIoDone {
    std::thread::spawn(move || block_on(io.run(&*store)))
        .join()
        .unwrap()
}

/// A store whose host runs the chunk I/O: the tick never waits on the
/// bucket, the host takes one batch at a time, runs it elsewhere and hands
/// the outcome back.
///
/// ```
/// tick ──► (no I/O)        take_chunk_io ──► puts ──thread──► bucket
///                          finish_chunk_io ◄──── outcome ◄────┘
/// ```
#[test]
fn a_host_runs_the_chunk_io_one_batch_at_a_time() {
    let (_dir, path) = scratch();
    let remote = Remote::default();
    let store: Arc<dyn ChunkStore> = Arc::new(remote.clone());
    let value = text(55, LARGE);
    let db = open_host_io(&path, store.clone());
    insert(&db, 1, &value);
    block_on(db.tick()).unwrap();
    assert_eq!(remote.objects().puts, 0, "the tick ran no I/O");
    let io = db.take_chunk_io().unwrap().expect("a batch");
    let (_, puts, _) = io.counts();
    assert!(puts > 2, "{puts}");
    assert!(db.take_chunk_io().unwrap().is_none(), "one batch is out");
    // Writes and reads go on while the batch is out.
    insert(&db, 2, &text(56, LARGE));
    assert_eq!(body(&db, 1), value);
    let done = run_elsewhere(io, store.clone());
    assert_eq!(file_node_bytes(&path).1, 0, "nothing moves before finish");
    db.finish_chunk_io(done).unwrap();
    assert_eq!(file_node_bytes(&path).1 as usize, puts);
    // The next batch moves the second value.
    let done = run_elsewhere(db.take_chunk_io().unwrap().unwrap(), store.clone());
    db.finish_chunk_io(done).unwrap();
    assert_eq!(
        file_node_bytes(&path).1 as usize,
        remote.objects().map.len()
    );
    drop(db);
    // Reopened, a direct read fetches the leaves it needs itself.
    let db = open_host_io(&path, store.clone());
    assert_eq!(body(&db, 1), value);
    assert!(remote.objects().gets > 0);
}

/// A host's failed put leaves the bytes in SQLite; finishing reports it
/// and a later batch puts the leaf again.
#[test]
fn a_host_batch_that_fails_is_retried_by_the_next() {
    let (_dir, path) = scratch();
    let remote = Remote::default();
    let store: Arc<dyn ChunkStore> = Arc::new(remote.clone());
    let value = text(57, LARGE);
    let db = open_host_io(&path, store.clone());
    insert(&db, 1, &value);
    remote.objects().failing = true;
    let done = run_elsewhere(db.take_chunk_io().unwrap().unwrap(), store.clone());
    let error = db.finish_chunk_io(done).err().expect("reported");
    assert_eq!(error.code, ErrorCode::Storage);
    assert_eq!(file_node_bytes(&path).1, 0);
    remote.objects().failing = false;
    let done = run_elsewhere(db.take_chunk_io().unwrap().unwrap(), store.clone());
    db.finish_chunk_io(done).unwrap();
    assert!(file_node_bytes(&path).1 > 2);
    drop(db);
    let db = open_host_io(&path, store);
    assert_eq!(body(&db, 1), value);
}

/// Alice, subscribed to a server's docs; what the server sends her waits
/// in the returned queue until her next tick.
struct Subscriber {
    alice: Db,
    up: Rc<RefCell<VecDeque<SyncMessage>>>,
    down: Rc<RefCell<VecDeque<SyncMessage>>>,
    _stream: Box<dyn std::any::Any>,
}

fn subscribe_to(server: &Db) -> Subscriber {
    let alice = block_on(Db::open(config(1, user(1)))).unwrap();
    let up = Rc::new(RefCell::new(VecDeque::new()));
    let down = Rc::new(RefCell::new(VecDeque::new()));
    let _ = block_on(alice.connect_upstream(Box::new(Pipe {
        out: up.clone(),
        inbound: down.clone(),
    })));
    server
        .accept_subscriber(
            Box::new(Pipe {
                out: down.clone(),
                inbound: up.clone(),
            }),
            user(1),
        )
        .unwrap();
    let stream = block_on(alice.subscribe(
        &alice.prepare_query(&Query::from("docs")).unwrap(),
        ReadOpts::default(),
    ))
    .unwrap();
    Subscriber {
        alice,
        up,
        down,
        _stream: Box::new(stream),
    }
}

/// A store at `path` whose leaves are all in `store`.
fn moved_to(path: &Path, store: &Arc<dyn ChunkStore>, value: &str) {
    let server = open(path, Some(store.clone())).unwrap();
    insert(&server, 1, value);
    block_on(server.tick()).unwrap();
}

/// A server whose host runs the chunk I/O holds a replica's chunk
/// requests until a batch has fetched the leaves, then serves them as the
/// batch is finished, without another tick.
#[test]
fn a_server_with_host_io_serves_chunk_requests_once_a_batch_fetched_them() {
    let (_dir, path) = scratch();
    let remote = Remote::default();
    let store: Arc<dyn ChunkStore> = Arc::new(remote.clone());
    let value = text(58, LARGE);
    moved_to(&path, &store, &value);
    let server = open_host_io(&path, store.clone());
    let sub = subscribe_to(&server);
    let mut fetched = 0;
    for _ in 0..16 {
        block_on(sub.alice.tick()).unwrap();
        block_on(server.tick()).unwrap();
        if let Some(io) = server.take_chunk_io().unwrap() {
            let fetches = io.counts().0;
            fetched += fetches;
            let served = sub.down.borrow().len();
            server
                .finish_chunk_io(run_elsewhere(io, store.clone()))
                .unwrap();
            if fetches > 0 {
                assert!(sub.down.borrow().len() > served, "served by the finish");
            }
        }
    }
    block_on(sub.alice.tick()).unwrap();
    assert!(fetched > 0, "the host fetched the leaves");
    assert_eq!(body(&sub.alice, 1), value);
}

/// A batch the host drops (a crashed task, a cancelled future) ends
/// there: the next batch puts the same leaves, and fetches what the
/// dropped one was to fetch. So does a batch dropped once it ran.
///
/// ```
/// take ──► drop ──► take (same puts) ──► run ──► drop ──► take ──► finish ✓
/// ```
#[test]
fn a_dropped_batch_ends_and_the_next_one_redoes_it() {
    let (_dir, path) = scratch();
    let remote = Remote::default();
    let store: Arc<dyn ChunkStore> = Arc::new(remote.clone());
    let value = text(59, LARGE);
    let db = open_host_io(&path, store.clone());
    insert(&db, 1, &value);
    let first = db.take_chunk_io().unwrap().expect("a batch");
    let puts = first.counts().1;
    assert!(puts > 2, "{puts}");
    drop(first);
    let second = db
        .take_chunk_io()
        .unwrap()
        .expect("the dropped batch ended");
    assert_eq!(second.counts().1, puts, "its puts are picked again");
    drop(run_elsewhere(second, store.clone()));
    assert_eq!(
        file_node_bytes(&path).1,
        0,
        "a dropped outcome moves nothing"
    );
    let done = run_elsewhere(db.take_chunk_io().unwrap().unwrap(), store.clone());
    db.finish_chunk_io(done).unwrap();
    assert_eq!(file_node_bytes(&path).1 as usize, puts);

    // A dropped batch's fetches go to the next one.
    drop(db);
    let server = open_host_io(&path, store.clone());
    let sub = subscribe_to(&server);
    let mut dropped = 0;
    for _ in 0..16 {
        block_on(sub.alice.tick()).unwrap();
        block_on(server.tick()).unwrap();
        let Some(io) = server.take_chunk_io().unwrap() else {
            continue;
        };
        if dropped == 0 && io.counts().0 > 0 {
            dropped = io.counts().0;
            drop(io);
            let again = server.take_chunk_io().unwrap().unwrap();
            assert!(again.counts().0 >= dropped, "{:?}", again.counts());
            server
                .finish_chunk_io(run_elsewhere(again, store.clone()))
                .unwrap();
            continue;
        }
        server
            .finish_chunk_io(run_elsewhere(io, store.clone()))
            .unwrap();
    }
    block_on(sub.alice.tick()).unwrap();
    assert!(dropped > 0);
    assert_eq!(body(&sub.alice, 1), value);
}

/// Only the store that handed a batch out finishes it, and only while it
/// is the batch that is out; an outcome with the wrong number of results
/// is refused. A store whose tick runs its own I/O hands out none. A
/// refused outcome is dropped, which ends its batch.
#[test]
fn a_batch_is_finished_only_by_the_store_that_handed_it_out() {
    let (_dir_a, path_a) = scratch();
    let (_dir_b, path_b) = scratch();
    let store: Arc<dyn ChunkStore> = Arc::new(Remote::default());
    let a = open_host_io(&path_a, store.clone());
    let b = open_host_io(&path_b, store.clone());
    insert(&a, 1, &text(60, LARGE));
    insert(&b, 1, &text(61, LARGE));
    let protocol = |error: jazz::db::Error, says: &str| {
        assert_eq!(error.code, ErrorCode::Protocol);
        assert!(error.message.contains(says), "{}", error.message);
    };

    let from_a = run_elsewhere(a.take_chunk_io().unwrap().unwrap(), store.clone());
    protocol(b.finish_chunk_io(from_a).unwrap_err(), "no batch is out");
    let from_a = run_elsewhere(
        a.take_chunk_io().unwrap().expect("the refused batch ended"),
        store.clone(),
    );
    let out_at_b = b.take_chunk_io().unwrap().unwrap();
    protocol(
        b.finish_chunk_io(from_a).unwrap_err(),
        "not the batch that is out",
    );
    b.finish_chunk_io(run_elsewhere(out_at_b, store.clone()))
        .unwrap();
    assert!(file_node_bytes(&path_b).1 > 2);

    let io = a.take_chunk_io().unwrap().unwrap();
    let counts = io.counts();
    protocol(
        io.done(Vec::new(), Vec::new(), Vec::new()).unwrap_err(),
        &format!("{counts:?}"),
    );
    let io = a.take_chunk_io().unwrap().expect("that batch ended too");
    a.finish_chunk_io(run_elsewhere(io, store.clone())).unwrap();
    assert!(file_node_bytes(&path_a).1 > 2);

    let (_dir_c, path_c) = scratch();
    let ticked = open(&path_c, Some(store)).unwrap();
    protocol(
        ticked.take_chunk_io().unwrap_err(),
        "runs its chunk I/O itself",
    );
}

/// A host that runs a batch op by op, as a JavaScript host awaiting
/// IndexedDB does, hands back one result per op.
#[test]
fn a_host_may_run_a_batch_op_by_op() {
    let (_dir, path) = scratch();
    let remote = Remote::default();
    let store: Arc<dyn ChunkStore> = Arc::new(remote.clone());
    let value = text(62, LARGE);
    let db = open_host_io(&path, store.clone());
    insert(&db, 1, &value);
    let io = db.take_chunk_io().unwrap().unwrap();
    assert_eq!(io.fetches().len(), 0);
    let put: Vec<_> = io
        .puts()
        .iter()
        .map(|(key, bytes)| block_on(store.put(key, bytes)))
        .collect();
    let deleted: Vec<_> = io
        .deletes()
        .iter()
        .map(|key| block_on(store.delete(key)))
        .collect();
    db.finish_chunk_io(io.done(Vec::new(), put, deleted).unwrap())
        .unwrap();
    assert!(file_node_bytes(&path).1 > 2);
    drop(db);
    let db = open_host_io(&path, store);
    assert_eq!(body(&db, 1), value);
}

/// Leave every large value in the file at `path` unreferenced, as a row's
/// last version going does. White-box: no public write unreferences a
/// committed value without history keeping it.
fn unreference_all(path: &Path) {
    let conn = rusqlite::Connection::open(path).unwrap();
    conn.execute("UPDATE _jazz_lv_root SET refs = 0", [])
        .unwrap();
}

fn leaves_in(path: &Path) -> i64 {
    let conn = rusqlite::Connection::open(path).unwrap();
    conn.query_row("SELECT count(*) FROM _jazz_lv_node", [], |r| r.get(0))
        .unwrap()
}

/// A host deletes reclaimed leaves from the bucket, after the reclamation
/// committed; a leaf reclaimed while its put is out is deleted by the next
/// batch, once the put landed.
///
/// ```
/// take (puts) ──► reclaim ──► finish (puts landed) ──► take (deletes) ──► finish
/// ```
#[test]
fn a_host_deletes_reclaimed_leaves_even_while_their_put_is_out() {
    let (_dir, path) = scratch();
    let remote = Remote::default();
    let store: Arc<dyn ChunkStore> = Arc::new(remote.clone());
    let db = open_host_io(&path, store.clone());
    insert(&db, 1, &text(63, LARGE));
    let out = db.take_chunk_io().unwrap().unwrap();
    let puts = out.counts().1;
    assert!(puts > 2);
    unreference_all(&path);
    // A write of the store's own, so its next tick runs a reclaim pass.
    insert(&db, 2, "small");
    block_on(db.tick()).unwrap();
    assert_eq!(leaves_in(&path), 0, "reclaimed while the puts are out");
    db.finish_chunk_io(run_elsewhere(out, store.clone()))
        .unwrap();
    assert_eq!(remote.objects().map.len(), puts, "the puts landed");
    let io = db.take_chunk_io().unwrap().unwrap();
    assert_eq!(io.counts(), (0, 0, puts));
    db.finish_chunk_io(run_elsewhere(io, store.clone()))
        .unwrap();
    assert!(remote.objects().map.is_empty(), "and were deleted after");

    // Leaves that had moved are deleted the same way.
    insert(&db, 3, &text(64, LARGE));
    let io = db.take_chunk_io().unwrap().unwrap();
    let moved = io.counts().1;
    db.finish_chunk_io(run_elsewhere(io, store.clone()))
        .unwrap();
    assert_eq!(remote.objects().map.len(), moved);
    drop(db);
    unreference_all(&path);
    let db = open_host_io(&path, store.clone());
    block_on(db.tick()).unwrap();
    let io = db.take_chunk_io().unwrap().unwrap();
    assert_eq!(io.counts(), (0, 0, moved));
    db.finish_chunk_io(run_elsewhere(io, store)).unwrap();
    assert!(remote.objects().map.is_empty());
    assert!(db.take_chunk_io().unwrap().unwrap().is_empty());
}

/// A fetch that fails is reported by the finish, and what waited on it is
/// not held: a read the server cannot serve without the leaves is refused
/// with the store's error, so the client is told. A pull is never answered
/// missing for it (the reader would not ask again). Each batch (paced by
/// the host's backoff) asks again, so once the store answers, a replica
/// subscribing again is served.
#[test]
fn a_host_fetch_that_fails_refuses_what_waits_on_it() {
    let (_dir, path) = scratch();
    let remote = Remote::default();
    let store: Arc<dyn ChunkStore> = Arc::new(remote.clone());
    let value = text(65, LARGE);
    moved_to(&path, &store, &value);
    let server = open_host_io(&path, store.clone());
    server.track_closed_subscribers();
    let sub = subscribe_to(&server);
    remote.objects().failing_gets = true;
    let mut failed = 0;
    let mut refused = false;
    for _ in 0..16 {
        assert!(
            !sub.down.borrow().iter().any(|message| matches!(
                message,
                SyncMessage::Chunks { missing, .. } if !missing.is_empty()
            )),
            "a store failure is not data the reader lacks"
        );
        refused |= sub.down.borrow().iter().any(|message| match message {
            SyncMessage::Refused { code, message, .. } => {
                *code == ErrorCode::Storage && message.contains("bucket unreachable")
            }
            _ => false,
        });
        refused |= server
            .take_closed_subscribers()
            .into_iter()
            .any(|(_, error)| error.message.contains("bucket unreachable"));
        block_on(sub.alice.tick()).unwrap();
        block_on(server.tick()).unwrap();
        let Some(io) = server.take_chunk_io().unwrap() else {
            continue;
        };
        if let Err(error) = server.finish_chunk_io(run_elsewhere(io, store.clone())) {
            assert_eq!(error.code, ErrorCode::Storage);
            failed += 1;
        }
    }
    assert!(failed >= 1, "{failed}");
    assert!(refused, "the replica is told, not left waiting");
    drop(sub);

    remote.objects().failing_gets = false;
    let sub = subscribe_to(&server);
    for _ in 0..16 {
        block_on(sub.alice.tick()).unwrap();
        block_on(server.tick()).unwrap();
        if let Some(io) = server.take_chunk_io().unwrap() {
            server
                .finish_chunk_io(run_elsewhere(io, store.clone()))
                .unwrap();
        }
    }
    block_on(sub.alice.tick()).unwrap();
    assert_eq!(body(&sub.alice, 1), value);
}

/// A replica's pull of leaves the store failed to give is answered once
/// the store is asked again and answers, on the same session: not missing,
/// which would leave the reader without them until a reconnect.
#[test]
fn a_pull_the_store_failed_is_answered_once_it_is_back() {
    let (_dir, path) = scratch();
    let remote = Remote::default();
    let store: Arc<dyn ChunkStore> = Arc::new(remote.clone());
    let value = text(69, LARGE);
    moved_to(&path, &store, &value);
    // What a replica pulls the value through: its row's version and root.
    let pull = {
        let server = open_host_io(&path, store.clone());
        let sub = subscribe_to(&server);
        let mut pull = None;
        for _ in 0..16 {
            block_on(sub.alice.tick()).unwrap();
            pull = pull.or_else(|| {
                sub.up.borrow().iter().find_map(|message| match message {
                    SyncMessage::ChunkRequests { pulls } => pulls.first().cloned(),
                    _ => None,
                })
            });
            block_on(server.tick()).unwrap();
            if let Some(io) = server.take_chunk_io().unwrap() {
                server
                    .finish_chunk_io(run_elsewhere(io, store.clone()))
                    .unwrap();
            }
        }
        pull.expect("the replica pulled the value")
    };

    // A fresh server (nothing cached) whose store fails: the pull waits.
    let server = open_host_io(&path, store.clone());
    let sub = subscribe_to(&server);
    for _ in 0..4 {
        block_on(sub.alice.tick()).unwrap();
        block_on(server.tick()).unwrap();
    }
    remote.objects().failing_gets = true;
    sub.down.borrow_mut().clear();
    let wanted = pull.nodes.clone();
    sub.up
        .borrow_mut()
        .push_back(SyncMessage::ChunkRequests { pulls: vec![pull] });
    let answered = |sub: &Subscriber| {
        let mut nodes = Vec::new();
        for message in sub.down.borrow().iter() {
            if let SyncMessage::Chunks {
                nodes: sent,
                missing,
            } = message
            {
                assert!(
                    !missing.iter().any(|node| wanted.contains(&node.hash)),
                    "a store failure is not data the reader lacks"
                );
                nodes.extend(sent.iter().map(|node| node.hash));
            }
        }
        wanted.iter().all(|hash| nodes.contains(hash))
    };
    let mut failed = 0;
    for _ in 0..8 {
        block_on(server.tick()).unwrap();
        if let Some(io) = server.take_chunk_io().unwrap()
            && server
                .finish_chunk_io(run_elsewhere(io, store.clone()))
                .is_err()
        {
            failed += 1;
        }
    }
    assert!(failed >= 1, "{failed}");
    assert!(!answered(&sub));

    // The store is back: the next batch asks again, and the pull is
    // answered on the same session.
    remote.objects().failing_gets = false;
    for _ in 0..8 {
        block_on(server.tick()).unwrap();
        if let Some(io) = server.take_chunk_io().unwrap() {
            server
                .finish_chunk_io(run_elsewhere(io, store.clone()))
                .unwrap();
        }
    }
    assert!(answered(&sub), "the pull is answered once the store is");
}

/// A leaf the store answers it does not hold is data loss, reported as
/// such; it is not remembered as lost, so once it is back (a restored
/// bucket, a fixed permission) a later read is served without a restart.
#[test]
fn a_leaf_the_store_lost_is_read_again_once_it_is_back() {
    let (_dir, path) = scratch();
    let remote = Remote::default();
    let store: Arc<dyn ChunkStore> = Arc::new(remote.clone());
    let value = text(68, LARGE);
    moved_to(&path, &store, &value);
    let saved = remote.objects().map.clone();
    remote.objects().map.clear();
    let db = open(&path, Some(store.clone())).unwrap();
    let error = cell(&db, 1, "body").unwrap_err();
    assert_eq!(error.code, ErrorCode::Storage);
    assert!(error.message.contains("missing"), "{}", error.message);
    remote.objects().map = saved;
    assert_eq!(body(&db, 1), value);
}

/// Fetches that succeed are served at once even when the batch's puts
/// fail: the finish reports the put failure, and the replica has its
/// chunks without another tick. The tick-run path serves them before its
/// puts start.
#[test]
fn fetched_chunks_are_served_even_when_the_puts_beside_them_fail() {
    for host in [true, false] {
        let (_dir, path) = scratch();
        let remote = Remote::default();
        let store: Arc<dyn ChunkStore> = Arc::new(remote.clone());
        let value = text(66, LARGE);
        moved_to(&path, &store, &value);
        let server = if host {
            open_host_io(&path, store.clone())
        } else {
            open(&path, Some(store.clone())).unwrap()
        };
        let sub = subscribe_to(&server);
        insert(&server, 2, &text(67, LARGE));
        remote.objects().failing = true;
        let mut served_with_failed_puts = false;
        for _ in 0..16 {
            block_on(sub.alice.tick()).unwrap();
            let gets = remote.objects().gets;
            let ticked = block_on(server.tick());
            let finished = match server.take_chunk_io() {
                Ok(Some(io)) => server.finish_chunk_io(run_elsewhere(io, store.clone())),
                _ => Ok(()),
            };
            let fetched = remote.objects().gets > gets;
            if fetched && (ticked.is_err() || finished.is_err()) {
                assert!(!sub.down.borrow().is_empty(), "host: {host}");
                served_with_failed_puts = true;
            }
        }
        block_on(sub.alice.tick()).unwrap();
        assert!(served_with_failed_puts, "host: {host}");
        assert_eq!(body(&sub.alice, 1), value, "host: {host}");
        assert_eq!(
            file_node_bytes(&path).1 as usize,
            remote.objects().map.len()
        );
    }
}

/// While a batch of uploads is out, the leaves a replica waits on are
/// fetched beside it: the replica is served without waiting for the
/// uploads. Fetches dropped unanswered are wanted again.
#[test]
fn reads_are_fetched_beside_a_batch_that_is_out() {
    let (_dir, path) = scratch();
    let remote = Remote::default();
    let store: Arc<dyn ChunkStore> = Arc::new(remote.clone());
    let value = text(69, LARGE);
    moved_to(&path, &store, &value);
    let server = open_host_io(&path, store.clone());
    insert(&server, 2, &text(70, LARGE));
    let uploads = server.take_chunk_io().unwrap().expect("a batch");
    assert!(uploads.counts().1 > 2, "{:?}", uploads.counts());
    let sub = subscribe_to(&server);
    let mut dropped = false;
    let mut fetched = 0;
    for _ in 0..16 {
        block_on(sub.alice.tick()).unwrap();
        block_on(server.tick()).unwrap();
        assert!(
            server.take_chunk_io().unwrap().is_none(),
            "the uploads are out"
        );
        let Some(fetches) = server.take_chunk_fetches().unwrap() else {
            continue;
        };
        assert_eq!(fetches.counts().1, 0, "no uploads among them");
        if !dropped {
            dropped = true;
            drop(fetches);
            continue;
        }
        fetched += fetches.counts().0;
        server
            .finish_chunk_fetches(run_elsewhere(fetches, store.clone()))
            .unwrap();
    }
    block_on(sub.alice.tick()).unwrap();
    assert!(dropped && fetched > 0, "{fetched}");
    assert_eq!(
        body(&sub.alice, 1),
        value,
        "served with the uploads still out"
    );
    server
        .finish_chunk_io(run_elsewhere(uploads, store.clone()))
        .unwrap();
}
