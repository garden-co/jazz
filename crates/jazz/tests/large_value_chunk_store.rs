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
