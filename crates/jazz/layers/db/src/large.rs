//! Chunked large values in SQLite cells.
//!
//! `string`, `json` and `bytes` columns keep Groove's large-value format
//! (`jazz_types::large_values`): a value above the inline threshold is a
//! descriptor over an immutable, content-defined chunk tree. Every such
//! column stores both arms as SQLite TEXT, so one collation sees them all:
//!
//! - an inline string or JSON cell is its UTF-8 bytes, unchanged;
//! - an inline bytes cell is [`BYTES`] followed by the raw bytes;
//! - a chunked cell is [`CHUNKED`] followed by the canonical descriptor.
//!
//! Neither tag byte occurs in UTF-8, so every cell says which arm it is.
//! The `jazz` collation and the `jz_` SQL functions read cells as their
//! logical values, fetching only the chunks they need. Chunks live in
//! `_jazz_lv_node` in the same database, so a row and the chunks it
//! references commit in one transaction; per-root reference counts are kept
//! by triggers on every table that has a large-capable column.

use std::cmp::Ordering;
use std::collections::{BTreeSet, HashMap};
use std::ffi::{c_int, c_void};
use std::sync::{Arc, Mutex};

use bytes::Bytes;
use jazz_types::large_values::{
    self as lv, AttemptError, ChunkInputs, ChunkNode, LargeValueKind, LargeValueRef,
    chunks::ChunkRequest,
};
use rusqlite::functions::FunctionFlags;
use rusqlite::types::ValueRef;
use rusqlite::{Connection, InterruptHandle, OptionalExtension, ffi};

use crate::options::{Error, ErrorCode};
use crate::values::{Column, Table, quote, storage};

/// First byte of an inline bytes cell.
pub(crate) const BYTES: u8 = 0xFE;
/// First byte of a chunked cell.
pub(crate) const CHUNKED: u8 = 0xFF;

/// Logical bytes read per step when two values are compared or scanned.
const WINDOW: u64 = 64 * 1024;
/// Verified chunk bytes a file store keeps cached, beyond those pinned by
/// the open transaction.
const CACHE_BYTES: usize = 64 * 1024 * 1024;

/// The chunk tables. Nodes are keyed by their random locator and verified
/// against the object hash a parent or descriptor commits to. A branch's
/// edges to its children are kept apart from the children themselves, so
/// nodes may arrive in any order (a replica fetches parents first, an
/// upload may send leaves first) and a node is held while any stored
/// branch, cell root, pushed-node receipt or local upload lease names it.
/// A root's `base` is the descriptor of its tree (no edit tail) once this
/// store built or validated it; later descriptors over the same root are
/// checked against it instead of walking the tree again.
/// `_jazz_lv_receipt` holds nodes a client pushed ahead of the commit that
/// references them, until `expires` (ms); `_jazz_lv_unpushed` holds nodes
/// this replica wrote that its upstream may not have yet; `_jazz_lv_upload`
/// leases a local streaming upload's nodes (`_jazz_lv_upload_node`) until
/// its row commits, it is aborted, or it goes untouched past the staging
/// policy's maximum age.
pub(crate) const DDL: &str = "\
CREATE TABLE IF NOT EXISTS _jazz_lv_node (locator BLOB PRIMARY KEY, hash BLOB NOT NULL, \
 bytes BLOB NOT NULL) WITHOUT ROWID;
CREATE TABLE IF NOT EXISTS _jazz_lv_edge (child BLOB NOT NULL, parent BLOB NOT NULL, \
 PRIMARY KEY (child, parent)) WITHOUT ROWID;
CREATE INDEX IF NOT EXISTS _jazz_lv_edge_parent ON _jazz_lv_edge (parent);
CREATE TABLE IF NOT EXISTS _jazz_lv_root (root BLOB PRIMARY KEY, durable INTEGER NOT NULL \
 DEFAULT 0, base BLOB) WITHOUT ROWID;
CREATE TABLE IF NOT EXISTS _jazz_lv_receipt (locator BLOB PRIMARY KEY, \
 expires INTEGER NOT NULL) WITHOUT ROWID;
CREATE INDEX IF NOT EXISTS _jazz_lv_receipt_expires ON _jazz_lv_receipt (expires);
CREATE TABLE IF NOT EXISTS _jazz_lv_unpushed (locator BLOB PRIMARY KEY) WITHOUT ROWID;
CREATE TABLE IF NOT EXISTS _jazz_lv_upload (id BLOB PRIMARY KEY, touched INTEGER NOT NULL) \
 WITHOUT ROWID;
CREATE TABLE IF NOT EXISTS _jazz_lv_upload_node (id BLOB NOT NULL, locator BLOB NOT NULL, \
 PRIMARY KEY (id, locator)) WITHOUT ROWID;
CREATE INDEX IF NOT EXISTS _jazz_lv_upload_node_locator ON _jazz_lv_upload_node (locator);
CREATE TABLE IF NOT EXISTS _jazz_lv_reclaim (root BLOB PRIMARY KEY) WITHOUT ROWID;
CREATE TRIGGER IF NOT EXISTS _jazz_lv_root_unreferenced AFTER UPDATE ON _jazz_lv_root \
 WHEN NEW.durable = 0 BEGIN \
 INSERT OR IGNORE INTO _jazz_lv_reclaim (root) VALUES (NEW.root); END;
";

/// Whether `ty` (or the type a nullable wraps) is stored by this module.
pub(crate) fn large_capable(column: &Column) -> bool {
    use crate::records::ValueType;
    let mut ty = &column.ty;
    while let ValueType::Nullable(inner) = ty {
        ty = inner;
    }
    matches!(ty, ValueType::String | ValueType::Bytes)
}

/// The column's declared kind.
pub(crate) fn kind(column: &Column) -> LargeValueKind {
    use crate::records::ValueType;
    let mut ty = &column.ty;
    while let ValueType::Nullable(inner) = ty {
        ty = inner;
    }
    match ty {
        ValueType::Bytes => LargeValueKind::Bytes,
        _ if column.json => LargeValueKind::Json,
        _ => LargeValueKind::String,
    }
}

/// The triggers that count every chunked cell of `table`'s large-capable
/// columns, in the current and history tables, against its root.
pub(crate) fn triggers(table: &Table, columns: &[&Column]) -> String {
    let mut sql = String::new();
    for target in [table.name.clone(), format!("{}__h", table.name)] {
        for column in columns {
            let name = quote(&column.name);
            let trigger = |what: &str| quote(&format!("{target}__lv_{what}_{}", column.name));
            let add = |cell: &str| {
                format!(
                    "INSERT INTO _jazz_lv_root (root, durable) SELECT r, 1 FROM \
                     (SELECT jz_root({cell}) AS r) WHERE r IS NOT NULL \
                     ON CONFLICT (root) DO UPDATE SET durable = durable + 1;"
                )
            };
            let sub = |cell: &str| {
                format!(
                    "UPDATE _jazz_lv_root SET durable = durable - 1 WHERE root = jz_root({cell});"
                )
            };
            let on = quote(&target);
            sql.push_str(&format!(
                "CREATE TRIGGER IF NOT EXISTS {} AFTER INSERT ON {on} \
                 WHEN jz_root(NEW.{name}) IS NOT NULL BEGIN {} END;\n\
                 CREATE TRIGGER IF NOT EXISTS {} AFTER DELETE ON {on} \
                 WHEN jz_root(OLD.{name}) IS NOT NULL BEGIN {} END;\n\
                 CREATE TRIGGER IF NOT EXISTS {} AFTER UPDATE OF {name} ON {on} \
                 WHEN jz_root(OLD.{name}) IS NOT jz_root(NEW.{name}) BEGIN \
                 {} {} END;\n",
                trigger("ins"),
                add(&format!("NEW.{name}")),
                trigger("del"),
                sub(&format!("OLD.{name}")),
                trigger("upd"),
                sub(&format!("OLD.{name}")),
                add(&format!("NEW.{name}")),
            ));
        }
    }
    sql
}

/// An inline bytes cell.
pub(crate) fn inline_bytes(bytes: &[u8]) -> Vec<u8> {
    let mut cell = Vec::with_capacity(bytes.len() + 1);
    cell.push(BYTES);
    cell.extend_from_slice(bytes);
    cell
}

/// A chunked cell.
pub(crate) fn chunked_cell(value: &LargeValueRef) -> Result<Vec<u8>, Error> {
    let mut cell = vec![CHUNKED];
    cell.extend(lv::encode_large_value_ref(value).map_err(corrupt)?);
    Ok(cell)
}

/// A stored cell of a large-capable column, as its logical value.
#[derive(Debug)]
pub(crate) enum Logical<'a> {
    Inline(&'a [u8]),
    Chunked(Box<LargeValueRef>),
}

impl Logical<'_> {
    pub(crate) fn len(&self) -> u64 {
        match self {
            Logical::Inline(bytes) => bytes.len() as u64,
            Logical::Chunked(value) => value.byte_length,
        }
    }
}

/// Read a TEXT cell as its logical value.
pub(crate) fn logical(cell: &[u8]) -> Result<Logical<'_>, Error> {
    Ok(match cell.first() {
        Some(&BYTES) => Logical::Inline(&cell[1..]),
        Some(&CHUNKED) => Logical::Chunked(Box::new(
            lv::decode_large_value_ref(&cell[1..]).map_err(corrupt)?,
        )),
        _ => Logical::Inline(cell),
    })
}

fn corrupt(error: lv::Error) -> Error {
    Error::new(ErrorCode::Storage, format!("large value: {error}"))
}

const MISSING: &str = "large-value chunks are not available locally yet; they were requested \
     from upstream, so retry after the next sync tick";

pub(crate) fn missing() -> Error {
    Error::new(ErrorCode::NotObserved, MISSING)
}

/// Whether `error` is a statement that needed chunks this store lacks.
pub(crate) fn is_missing(error: &Error) -> bool {
    error.code == ErrorCode::NotObserved && error.message == MISSING
}

/// Verified chunks this process has read or written, and the way to read
/// the rest. Shared by the database connection's collation and functions,
/// which run inside SQLite's B-tree code and cannot use that connection, and
/// by Rust code that decodes cells.
pub(crate) struct Chunks {
    state: Mutex<State>,
    interrupt: InterruptHandle,
}

struct State {
    nodes: HashMap<[u8; 32], Cached>,
    /// Unpinned cached bytes, and the bound on them (`None` for an
    /// in-memory store, whose cache is its only readable copy).
    bytes: usize,
    budget: Option<usize>,
    clock: u64,
    /// A second, read-only connection to a file store.
    reader: Option<Connection>,
    /// Chunks a statement needed and nothing local holds.
    demand: BTreeSet<ChunkRequest>,
}

struct Cached {
    hash: [u8; 32],
    bytes: Bytes,
    /// Written by the open transaction: the reader connection cannot see it
    /// until commit, so it must not be evicted before then.
    pinned: bool,
    used: u64,
}

impl Chunks {
    /// Chunks of `conn`, which is in memory (`path` is `None`) or the file
    /// at `path`. Registers the collation and functions on `conn`.
    pub(crate) fn install(
        conn: &Connection,
        path: Option<&std::path::Path>,
    ) -> Result<Arc<Self>, Error> {
        let reader = match path {
            None => None,
            Some(path) => {
                let reader = Connection::open_with_flags(
                    path,
                    rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY
                        | rusqlite::OpenFlags::SQLITE_OPEN_NO_MUTEX,
                )
                .map_err(storage)?;
                Some(reader)
            }
        };
        let chunks = Arc::new(Self {
            state: Mutex::new(State {
                nodes: HashMap::new(),
                bytes: 0,
                budget: path.map(|_| CACHE_BYTES),
                clock: 0,
                reader,
                demand: BTreeSet::new(),
            }),
            interrupt: conn.get_interrupt_handle(),
        });
        register(conn, &chunks)?;
        Ok(chunks)
    }

    /// Cache a node the open transaction wrote.
    pub(crate) fn put(&self, request: &ChunkRequest, bytes: Bytes) {
        let mut state = self.state.lock().expect("chunk cache");
        state.clock += 1;
        let used = state.clock;
        let old = state.nodes.insert(
            *request.locator.as_bytes(),
            Cached {
                hash: request.object_hash,
                bytes,
                pinned: true,
                used,
            },
        );
        if let Some(old) = old
            && !old.pinned
        {
            state.bytes -= old.bytes.len();
        }
    }

    /// Cache a prepared value's nodes ahead of storing them.
    pub(crate) fn cache_pinned(&self, prepared: &lv::PreparedLargeValue) {
        for chunk in &prepared.staged_chunks {
            self.put(
                &ChunkRequest {
                    object_hash: chunk.node_ref.object_hash.0,
                    locator: chunk.node_ref.locator,
                },
                Bytes::from(chunk.encoded.clone()),
            );
        }
    }

    /// Forget a reclaimed node.
    pub(crate) fn forget(&self, locator: &[u8; 32]) {
        let mut state = self.state.lock().expect("chunk cache");
        if let Some(old) = state.nodes.remove(locator)
            && !old.pinned
        {
            state.bytes -= old.bytes.len();
        }
    }

    /// The open transaction ended: its nodes are readable from the file (or
    /// were rolled back, and are unreferenced but still correct bytes).
    pub(crate) fn unpin(&self) {
        let mut state = self.state.lock().expect("chunk cache");
        let mut released = 0;
        for node in state.nodes.values_mut() {
            if node.pinned {
                node.pinned = false;
                released += node.bytes.len();
            }
        }
        state.bytes += released;
        state.evict();
    }

    /// Whether the cache is the store's only readable copy of its nodes.
    pub(crate) fn in_memory(&self) -> bool {
        self.state.lock().expect("chunk cache").budget.is_none()
    }

    /// The chunks statements needed since the last call.
    pub(crate) fn take_demand(&self) -> BTreeSet<ChunkRequest> {
        std::mem::take(&mut self.state.lock().expect("chunk cache").demand)
    }

    /// The verified bytes of one node, if anything local holds it.
    fn get(&self, request: &ChunkRequest) -> Result<Option<Bytes>, Error> {
        let mut state = self.state.lock().expect("chunk cache");
        state.clock += 1;
        let used = state.clock;
        if let Some(node) = state.nodes.get_mut(request.locator.as_bytes()) {
            if node.hash != request.object_hash {
                return Err(corrupt(lv::Error::ObjectHashMismatch));
            }
            node.used = used;
            return Ok(Some(node.bytes.clone()));
        }
        let Some(reader) = &state.reader else {
            return Ok(None);
        };
        let found: Option<(Vec<u8>, Vec<u8>)> = reader
            .prepare_cached("SELECT hash, bytes FROM _jazz_lv_node WHERE locator = ?1")
            .and_then(|mut statement| {
                statement
                    .query_row([request.locator.as_bytes().as_slice()], |r| {
                        Ok((r.get(0)?, r.get(1)?))
                    })
                    .optional()
            })
            .map_err(storage)?;
        let Some((hash, bytes)) = found else {
            return Ok(None);
        };
        if hash != request.object_hash || lv::object_hash(&bytes).0 != request.object_hash {
            return Err(corrupt(lv::Error::ObjectHashMismatch));
        }
        let bytes = Bytes::from(bytes);
        state.bytes += bytes.len();
        state.nodes.insert(
            *request.locator.as_bytes(),
            Cached {
                hash: request.object_hash,
                bytes: bytes.clone(),
                pinned: false,
                used,
            },
        );
        state.evict();
        Ok(Some(bytes))
    }

    /// Run an attempt to completion against local chunks. A chunk nothing
    /// local holds is recorded as demand and fails the read.
    pub(crate) fn run<T>(
        &self,
        attempt: impl FnMut(&mut ChunkInputs) -> Result<T, AttemptError>,
    ) -> Result<T, Error> {
        self.run_reading(attempt, |_| {})
    }

    /// [`Self::run`], reporting the size of each node it reads.
    fn run_reading<T>(
        &self,
        mut attempt: impl FnMut(&mut ChunkInputs) -> Result<T, AttemptError>,
        read: impl Fn(usize),
    ) -> Result<T, Error> {
        let mut inputs = ChunkInputs::default();
        loop {
            match attempt(&mut inputs) {
                Ok(value) => return Ok(value),
                Err(AttemptError::LargeValue(error)) => return Err(corrupt(error)),
                Err(AttemptError::Blocked) => {
                    let mut absent = Vec::new();
                    for request in inputs.take_missing_chunks() {
                        match self.get(&request)? {
                            Some(bytes) => {
                                read(bytes.len());
                                inputs.install_verified_chunk(request, bytes)
                            }
                            None => absent.push(request),
                        }
                    }
                    if !absent.is_empty() {
                        self.state
                            .lock()
                            .expect("chunk cache")
                            .demand
                            .extend(absent);
                        return Err(missing());
                    }
                }
            }
        }
    }

    /// The whole logical value of a cell.
    pub(crate) fn materialize(&self, cell: &[u8]) -> Result<Vec<u8>, Error> {
        match logical(cell)? {
            Logical::Inline(bytes) => Ok(bytes.to_vec()),
            Logical::Chunked(value) => self.run(|inputs| lv::materialize_attempt(&value, inputs)),
        }
    }

    /// Logical bytes `range` of a value.
    fn range(&self, value: &Logical<'_>, range: std::ops::Range<u64>) -> Result<Vec<u8>, Error> {
        match value {
            Logical::Inline(bytes) => Ok(bytes[range.start as usize..range.end as usize].to_vec()),
            Logical::Chunked(value) => {
                self.run(|inputs| lv::byte_range_attempt(value, range.clone(), inputs))
            }
        }
    }

    /// Compare two cells of one column by logical bytes, reading chunks
    /// only up to the first difference.
    pub(crate) fn compare(&self, a: &[u8], b: &[u8]) -> Result<Ordering, Error> {
        if a.first() != Some(&CHUNKED) && b.first() != Some(&CHUNKED) {
            return Ok(a.cmp(b));
        }
        if a == b {
            return Ok(Ordering::Equal);
        }
        let (a, b) = (logical(a)?, logical(b)?);
        if let (Logical::Chunked(x), Logical::Chunked(y)) = (&a, &b)
            && x.root == y.root
            && x.edit_tail == y.edit_tail
        {
            return Ok(Ordering::Equal);
        }
        let (len_a, len_b) = (a.len(), b.len());
        let mut at = 0;
        while at < len_a.min(len_b) {
            let end = (at + WINDOW).min(len_a.min(len_b));
            let ordering = self.range(&a, at..end)?.cmp(&self.range(&b, at..end)?);
            if ordering != Ordering::Equal {
                return Ok(ordering);
            }
            at = end;
        }
        Ok(len_a.cmp(&len_b))
    }

    /// Whether a value contains `needle`, scanning it window by window.
    pub(crate) fn contains(&self, haystack: &[u8], needle: &[u8]) -> Result<bool, Error> {
        let needle = self.materialize(needle)?;
        let haystack = logical(haystack)?;
        if let Logical::Inline(bytes) = haystack {
            return Ok(find(bytes, &needle));
        }
        let len = haystack.len();
        if (needle.len() as u64) > len {
            return Ok(false);
        }
        // Each window overlaps the last by one byte less than the needle,
        // so a match across a window boundary is still found.
        let overlap = needle.len().saturating_sub(1) as u64;
        let mut at = 0;
        loop {
            let end = (at + WINDOW.max(overlap + 1)).min(len);
            if find(&self.range(&haystack, at..end)?, &needle) {
                return Ok(true);
            }
            if end == len {
                return Ok(false);
            }
            at = end - overlap;
        }
    }

    fn record_missing_and_interrupt(&self) {
        self.interrupt.interrupt();
    }
}

impl State {
    fn evict(&mut self) {
        let Some(budget) = self.budget else {
            return;
        };
        if self.bytes <= budget {
            return;
        }
        let mut victims: Vec<([u8; 32], u64, usize)> = self
            .nodes
            .iter()
            .filter(|(_, node)| !node.pinned)
            .map(|(locator, node)| (*locator, node.used, node.bytes.len()))
            .collect();
        victims.sort_by_key(|(_, used, _)| *used);
        for (locator, _, len) in victims {
            if self.bytes <= budget * 3 / 4 {
                break;
            }
            self.nodes.remove(&locator);
            self.bytes -= len;
        }
    }
}

fn find(haystack: &[u8], needle: &[u8]) -> bool {
    needle.is_empty()
        || haystack
            .windows(needle.len())
            .any(|window| window == needle)
}

/// Register the `jazz` collation and the `jz_` functions on `conn`.
fn register(conn: &Connection, chunks: &Arc<Chunks>) -> Result<(), Error> {
    // rusqlite's collation API hands the comparator lossy UTF-8; cells here
    // are not UTF-8, so the collation is registered on the raw handle.
    let user = Arc::into_raw(chunks.clone()) as *mut c_void;
    // SAFETY: `user` is an owned `Arc<Chunks>` that `destroy` releases when
    // SQLite drops the collation; `compare` only borrows it.
    let rc = unsafe {
        ffi::sqlite3_create_collation_v2(
            conn.handle(),
            c"jazz".as_ptr(),
            ffi::SQLITE_UTF8,
            user,
            Some(compare),
            Some(destroy),
        )
    };
    if rc != ffi::SQLITE_OK {
        // SAFETY: SQLite did not take ownership of `user` on failure.
        unsafe { drop(Arc::from_raw(user as *const Chunks)) };
        return Err(Error::new(
            ErrorCode::Storage,
            format!("registering the jazz collation failed ({rc})"),
        ));
    }

    let flags = FunctionFlags::SQLITE_UTF8 | FunctionFlags::SQLITE_DETERMINISTIC;
    conn.create_scalar_function("jz_root", 1, flags, |ctx| {
        Ok(match ctx.get_raw(0) {
            ValueRef::Text(cell) if cell.first() == Some(&CHUNKED) => match logical(cell) {
                Ok(Logical::Chunked(value)) => Some(value.root.locator.as_bytes().to_vec()),
                _ => None,
            },
            _ => None,
        })
    })
    .map_err(storage)?;

    let shared = chunks.clone();
    conn.create_scalar_function("jz_contains", 2, flags, move |ctx| {
        let (haystack, needle) = (ctx.get_raw(0), ctx.get_raw(1));
        let text = |value: ValueRef<'_>| -> Option<Vec<u8>> {
            Some(match value {
                ValueRef::Null => return None,
                ValueRef::Text(cell) => return Some(cell.to_vec()),
                ValueRef::Blob(blob) => blob.to_vec(),
                ValueRef::Integer(v) => v.to_string().into_bytes(),
                ValueRef::Real(v) => v.to_string().into_bytes(),
            })
        };
        let (Some(haystack), Some(needle)) = (text(haystack), text(needle)) else {
            return Ok(None);
        };
        match shared.contains(&haystack, &needle) {
            Ok(found) => Ok(Some(found as i64)),
            Err(error) if error.code == ErrorCode::NotObserved => {
                shared.record_missing_and_interrupt();
                Ok(None)
            }
            Err(error) => Err(rusqlite::Error::UserFunctionError(error.message.into())),
        }
    })
    .map_err(storage)?;
    Ok(())
}

unsafe extern "C" fn compare(
    user: *mut c_void,
    len_a: c_int,
    a: *const c_void,
    len_b: c_int,
    b: *const c_void,
) -> c_int {
    // SAFETY: SQLite passes the pointer registered in `register`, alive
    // until `destroy`, and two buffers of the given lengths.
    let (chunks, a, b) = unsafe {
        let slice = |ptr: *const c_void, len: c_int| {
            if len <= 0 || ptr.is_null() {
                &[][..]
            } else {
                std::slice::from_raw_parts(ptr.cast::<u8>(), len as usize)
            }
        };
        (&*(user as *const Chunks), slice(a, len_a), slice(b, len_b))
    };
    let ordering = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| chunks.compare(a, b)));
    match ordering {
        Ok(Ok(ordering)) => ordering as c_int,
        // A collation must answer. The interrupt fails the statement, so
        // this answer is never used: a write rolls back, a read is retried
        // once its chunks have arrived.
        _ => {
            chunks.record_missing_and_interrupt();
            0
        }
    }
}

unsafe extern "C" fn destroy(user: *mut c_void) {
    // SAFETY: `user` is the `Arc` leaked in `register`.
    unsafe { drop(Arc::from_raw(user as *const Chunks)) };
}

/// Store a prepared value's new nodes and cache them. A replica (`replica`)
/// also marks them unpushed, for its next upload.
pub(crate) fn store(
    conn: &Connection,
    chunks: &Chunks,
    prepared: &lv::PreparedLargeValue,
    replica: bool,
) -> Result<(), Error> {
    for chunk in &prepared.staged_chunks {
        store_chunk(conn, Some(chunks), chunk)?;
        if replica {
            mark_unpushed(conn, &chunk.node_ref.locator)?;
        }
    }
    record_base(conn, &prepared.value_ref)
}

/// Record `value`'s base descriptor on its root as built or validated here.
fn record_base(conn: &Connection, value: &LargeValueRef) -> Result<(), Error> {
    let base = lv::tail_base_descriptor(value).map_err(corrupt)?;
    let encoded = lv::encode_large_value_ref(&base).map_err(corrupt)?;
    conn.prepare_cached(
        "INSERT INTO _jazz_lv_root (root, base) VALUES (?1, ?2) \
         ON CONFLICT (root) DO UPDATE SET base = excluded.base",
    )
    .and_then(|mut statement| {
        statement.execute(rusqlite::params![
            value.root.locator.as_bytes().as_slice(),
            encoded
        ])
    })
    .map_err(storage)?;
    Ok(())
}

fn mark_unpushed(conn: &Connection, locator: &lv::Locator) -> Result<(), Error> {
    conn.prepare_cached("INSERT OR IGNORE INTO _jazz_lv_unpushed (locator) VALUES (?1)")
        .and_then(|mut statement| statement.execute([locator.as_bytes().as_slice()]))
        .map_err(storage)?;
    Ok(())
}

/// Store one node, caching it pinned when `cache` is given. A new branch
/// records an edge to each child, whether or not the child is stored yet.
fn store_chunk(
    conn: &Connection,
    cache: Option<&Chunks>,
    chunk: &lv::StagedChunk,
) -> Result<(), Error> {
    let locator = chunk.node_ref.locator.as_bytes();
    let hash = chunk.node_ref.object_hash.0;
    let inserted = conn
        .prepare_cached(
            "INSERT INTO _jazz_lv_node (locator, hash, bytes) VALUES (?1, ?2, ?3) \
             ON CONFLICT (locator) DO NOTHING",
        )
        .and_then(|mut insert| {
            insert.execute(rusqlite::params![
                locator.as_slice(),
                hash.as_slice(),
                chunk.encoded
            ])
        })
        .map_err(storage)?;
    if let Some(chunks) = cache {
        chunks.put(
            &ChunkRequest {
                object_hash: hash,
                locator: chunk.node_ref.locator,
            },
            Bytes::from(chunk.encoded.clone()),
        );
    }
    if inserted == 0 {
        return Ok(());
    }
    let node = lv::decode_node_untyped_authenticated(chunk.node_ref.object_hash, &chunk.encoded)
        .map_err(corrupt)?;
    if let ChunkNode::Branch { children, .. } = node {
        let mut edge = conn
            .prepare_cached("INSERT OR IGNORE INTO _jazz_lv_edge (child, parent) VALUES (?1, ?2)")
            .map_err(storage)?;
        for child in children {
            edge.execute([
                child.node_ref.locator.as_bytes().as_slice(),
                locator.as_slice(),
            ])
            .map_err(storage)?;
        }
    }
    Ok(())
}

/// Open a local streaming upload's lease. Until [`end_upload`] or
/// [`expire_uploads`], every node staged under `id` is held from
/// reclamation, so the row that will reference the finished value never
/// races the reclaimer.
pub(crate) fn begin_upload(conn: &Connection, id: &[u8; 32], now: i64) -> Result<(), Error> {
    conn.execute(
        "INSERT INTO _jazz_lv_upload (id, touched) VALUES (?1, ?2)",
        rusqlite::params![id.as_slice(), now],
    )
    .map_err(storage)?;
    Ok(())
}

/// Renew `id`'s lease; false once it was ended or expired.
pub(crate) fn touch_upload(conn: &Connection, id: &[u8; 32], now: i64) -> Result<bool, Error> {
    let renewed = conn
        .prepare_cached("UPDATE _jazz_lv_upload SET touched = ?2 WHERE id = ?1")
        .and_then(|mut statement| statement.execute(rusqlite::params![id.as_slice(), now]))
        .map_err(storage)?;
    Ok(renewed > 0)
}

/// Store the nodes an upload finalized, held by its lease. Outside a
/// transaction the nodes commit at once and a file store reads them back
/// from the file, so they are not cached; an in-memory store (whose cache
/// is its only readable copy) or an open transaction caches them pinned.
pub(crate) fn stage_upload(
    conn: &Connection,
    chunks: &Chunks,
    id: &[u8; 32],
    staged: &[lv::StagedChunk],
    replica: bool,
) -> Result<(), Error> {
    let cache = (!conn.is_autocommit() || chunks.in_memory()).then_some(chunks);
    conn.execute_batch("SAVEPOINT jazz_lv_upload")
        .map_err(storage)?;
    let result = staged.iter().try_for_each(|chunk| {
        store_chunk(conn, cache, chunk)?;
        if replica {
            mark_unpushed(conn, &chunk.node_ref.locator)?;
        }
        conn.prepare_cached(
            "INSERT OR IGNORE INTO _jazz_lv_upload_node (id, locator) VALUES (?1, ?2)",
        )
        .and_then(|mut statement| {
            statement.execute([id.as_slice(), chunk.node_ref.locator.as_bytes().as_slice()])
        })
        .map_err(storage)?;
        Ok(())
    });
    match result {
        Ok(()) => conn
            .execute_batch("RELEASE jazz_lv_upload")
            .map_err(storage),
        Err(error) => {
            let _ = conn.execute_batch("ROLLBACK TO jazz_lv_upload; RELEASE jazz_lv_upload");
            Err(error)
        }
    }
}

/// Record the finished value's tail-free base, so binding it trusts it.
pub(crate) fn finish_upload(conn: &Connection, value: &LargeValueRef) -> Result<(), Error> {
    record_base(conn, value)
}

/// Drop `id`'s lease. Its nodes are queued for reclamation, which keeps
/// whatever a committed row now references.
pub(crate) fn end_upload(conn: &Connection, id: &[u8; 32]) -> Result<(), Error> {
    conn.execute(
        "INSERT OR IGNORE INTO _jazz_lv_reclaim (root) \
         SELECT locator FROM _jazz_lv_upload_node WHERE id = ?1",
        [id.as_slice()],
    )
    .and_then(|_| {
        conn.execute(
            "DELETE FROM _jazz_lv_upload_node WHERE id = ?1",
            [id.as_slice()],
        )
    })
    .and_then(|_| conn.execute("DELETE FROM _jazz_lv_upload WHERE id = ?1", [id.as_slice()]))
    .map_err(storage)?;
    Ok(())
}

/// End every upload not touched since `before` (ms); returns how many.
pub(crate) fn expire_uploads(conn: &Connection, before: i64) -> Result<usize, Error> {
    let ids: Vec<Vec<u8>> = conn
        .prepare("SELECT id FROM _jazz_lv_upload WHERE touched <= ?1")
        .and_then(|mut statement| {
            statement
                .query_map([before], |row| row.get(0))?
                .collect::<Result<_, _>>()
        })
        .map_err(storage)?;
    for id in &ids {
        let id: [u8; 32] = id
            .as_slice()
            .try_into()
            .map_err(|_| Error::new(ErrorCode::Storage, "large value: malformed upload id"))?;
        end_upload(conn, &id)?;
    }
    Ok(ids.len())
}

/// One stored node: (object hash, encoded bytes).
pub(crate) fn node(
    conn: &Connection,
    locator: &lv::Locator,
) -> Result<Option<(Vec<u8>, Vec<u8>)>, Error> {
    conn.prepare_cached("SELECT hash, bytes FROM _jazz_lv_node WHERE locator = ?1")
        .and_then(|mut statement| {
            statement
                .query_row([locator.as_bytes().as_slice()], |r| {
                    Ok((r.get(0)?, r.get(1)?))
                })
                .optional()
        })
        .map_err(storage)
}

fn request_of(node_ref: &lv::NodeRef) -> ChunkRequest {
    ChunkRequest {
        object_hash: node_ref.object_hash.0,
        locator: node_ref.locator,
    }
}

/// Authenticate `bytes` as the node `request` names, or fail.
fn authenticate(request: &ChunkRequest, bytes: &[u8]) -> Result<ChunkNode, Error> {
    lv::decode_node_untyped_authenticated(lv::ContentHash(request.object_hash), bytes)
        .map_err(corrupt)
}

/// Visit `value`'s stored tree depth-first. `descend` sees each node that
/// is stored (with whether it is) and says whether to visit its children.
fn walk(
    conn: &Connection,
    value: &LargeValueRef,
    mut descend: impl FnMut(&ChunkRequest, Option<&[u8]>) -> Result<bool, Error>,
) -> Result<(), Error> {
    let mut stack = vec![request_of(&value.root)];
    let mut seen = BTreeSet::new();
    while let Some(request) = stack.pop() {
        if !seen.insert(request.locator) {
            continue;
        }
        let stored = node(conn, &request.locator)?;
        let bytes = stored
            .as_ref()
            .filter(|(hash, _)| hash.as_slice() == request.object_hash)
            .map(|(_, bytes)| bytes.as_slice());
        if !descend(&request, bytes)? {
            continue;
        }
        if let Some(bytes) = bytes
            && let ChunkNode::Branch { children, .. } = authenticate(&request, bytes)?
        {
            stack.extend(children.iter().map(|child| request_of(&child.node_ref)));
        }
    }
    Ok(())
}

/// The nodes of `value` this store lacks and can name now: the frontier of
/// its stored tree. Empty when the whole tree is local.
pub(crate) fn missing_frontier(
    conn: &Connection,
    value: &LargeValueRef,
) -> Result<Vec<ChunkRequest>, Error> {
    let mut missing = Vec::new();
    walk(conn, value, |request, bytes| {
        if bytes.is_none() {
            missing.push(request.clone());
        }
        Ok(bytes.is_some())
    })?;
    Ok(missing)
}

/// The nodes of `value` this replica wrote and has not pushed upstream.
/// A node the upstream holds was validated there with its whole subtree,
/// so the walk stops at the first pushed node on each path.
pub(crate) fn unpushed(
    conn: &Connection,
    value: &LargeValueRef,
) -> Result<Vec<(ChunkRequest, Vec<u8>)>, Error> {
    let mut out = Vec::new();
    walk(conn, value, |request, bytes| {
        let Some(bytes) = bytes else {
            return Ok(false);
        };
        let pending: bool = conn
            .prepare_cached("SELECT EXISTS (SELECT 1 FROM _jazz_lv_unpushed WHERE locator = ?1)")
            .and_then(|mut statement| {
                statement.query_row([request.locator.as_bytes().as_slice()], |r| r.get(0))
            })
            .map_err(storage)?;
        if pending {
            out.push((request.clone(), bytes.to_vec()));
        }
        Ok(pending)
    })?;
    Ok(out)
}

/// The upstream accepted (or finally refused) the commit these nodes were
/// pushed for.
pub(crate) fn mark_pushed(conn: &Connection, locators: &[lv::Locator]) -> Result<(), Error> {
    let mut statement = conn
        .prepare_cached("DELETE FROM _jazz_lv_unpushed WHERE locator = ?1")
        .map_err(storage)?;
    for locator in locators {
        statement
            .execute([locator.as_bytes().as_slice()])
            .map_err(storage)?;
    }
    Ok(())
}

/// Store a node fetched from upstream, after authenticating it against the
/// hash this replica asked for.
pub(crate) fn install_fetched(
    conn: &Connection,
    chunks: &Chunks,
    request: &ChunkRequest,
    bytes: Vec<u8>,
) -> Result<(), Error> {
    authenticate(request, &bytes)?;
    store_chunk(
        conn,
        Some(chunks),
        &lv::StagedChunk {
            node_ref: lv::NodeRef {
                object_hash: lv::ContentHash(request.object_hash),
                locator: request.locator,
            },
            encoded: bytes,
        },
    )
}

/// Store a node a client pushed ahead of its commit, held by a receipt
/// until `expires`.
pub(crate) fn receive_pushed(
    conn: &Connection,
    chunks: &Chunks,
    request: &ChunkRequest,
    bytes: Vec<u8>,
    expires: i64,
) -> Result<(), Error> {
    if let Some((hash, _)) = node(conn, &request.locator)?
        && hash.as_slice() != request.object_hash
    {
        return Err(Error::new(
            ErrorCode::WriteRejected,
            "opaque locator already names different content",
        ));
    }
    install_fetched(conn, chunks, request, bytes)?;
    conn.prepare_cached(
        "INSERT INTO _jazz_lv_receipt (locator, expires) VALUES (?1, ?2) \
         ON CONFLICT (locator) DO UPDATE SET expires = MAX(expires, excluded.expires)",
    )
    .and_then(|mut statement| {
        statement.execute(rusqlite::params![
            request.locator.as_bytes().as_slice(),
            expires
        ])
    })
    .map_err(storage)?;
    Ok(())
}

/// Drop receipts that expired by `now`; their nodes are reclaimed unless a
/// commit referenced them meanwhile.
pub(crate) fn expire_receipts(conn: &Connection, now: i64) -> Result<(), Error> {
    conn.execute_batch(&format!(
        "INSERT OR IGNORE INTO _jazz_lv_reclaim (root) \
         SELECT locator FROM _jazz_lv_receipt WHERE expires <= {now}; \
         DELETE FROM _jazz_lv_receipt WHERE expires <= {now};"
    ))
    .map_err(storage)
}

struct Stored<'a>(&'a Connection);

impl lv::chunks::ChunkProvider for Stored<'_> {
    fn get(
        &self,
        request: ChunkRequest,
    ) -> lv::chunks::ChunkFuture<'_, Result<Bytes, lv::chunks::ChunkError>> {
        let found = match node(self.0, &request.locator) {
            Ok(Some((hash, bytes))) if hash.as_slice() == request.object_hash => {
                lv::record_finalize_validation_bytes(bytes.len());
                Ok(Bytes::from(bytes))
            }
            Ok(_) => Err(lv::chunks::ChunkError::Unavailable),
            Err(error) => Err(lv::chunks::ChunkError::Backend(error.to_string())),
        };
        Box::pin(std::future::ready(found))
    }
}

/// Admit an uploaded descriptor: its tree must be complete here and honest
/// (structure, metrics, logical hash and content), and its edit tail must
/// replay against it. A root validated once is trusted afterwards, so an
/// append re-checks only its tail.
pub(crate) fn admit(
    conn: &Connection,
    chunks: &Chunks,
    value: &LargeValueRef,
) -> Result<(), Error> {
    let rejected = |what: String| Error::new(ErrorCode::WriteRejected, what);
    let base = lv::tail_base_descriptor(value).map_err(|e| rejected(e.to_string()))?;
    let encoded = lv::encode_large_value_ref(&base).map_err(corrupt)?;
    let known: Option<Option<Vec<u8>>> = conn
        .prepare_cached("SELECT base FROM _jazz_lv_root WHERE root = ?1")
        .and_then(|mut statement| {
            statement
                .query_row([value.root.locator.as_bytes().as_slice()], |r| r.get(0))
                .optional()
        })
        .map_err(storage)?;
    match known.flatten() {
        Some(known) if known == encoded => {}
        Some(_) => {
            return Err(rejected(
                "the descriptor disagrees with its stored tree".into(),
            ));
        }
        None => {
            crate::options::block_on(lv::visit_reachable_chunks(&base, &Stored(conn), |_| {}))
                .map_err(|e| rejected(format!("large value: {e}")))?;
            let mut validator =
                lv::LogicalValueValidator::new(&base).map_err(|e| rejected(e.to_string()))?;
            let mut offset = 0;
            while offset < base.byte_length {
                let end = (offset + WINDOW).min(base.byte_length);
                let window = chunks.run_reading(
                    |inputs| lv::byte_range_attempt(&base, offset..end, inputs),
                    lv::record_finalize_validation_bytes,
                )?;
                validator
                    .push(&window)
                    .map_err(|e| rejected(e.to_string()))?;
                offset = end;
            }
            validator
                .finish(&base)
                .map_err(|e| rejected(e.to_string()))?;
            record_base(conn, &base)?;
        }
    }
    if !value.edit_tail.is_empty() {
        chunks.run_reading(
            |inputs| lv::validate_edit_tail_attempt(value, inputs),
            lv::record_finalize_validation_bytes,
        )?;
    }
    Ok(())
}

/// Whether the root `value` names is stored here.
pub(crate) fn root_is_local(conn: &Connection, value: &LargeValueRef) -> Result<bool, Error> {
    conn.query_row(
        "SELECT EXISTS (SELECT 1 FROM _jazz_lv_node WHERE locator = ?1)",
        [value.root.locator.as_bytes().as_slice()],
        |r| r.get(0),
    )
    .map_err(storage)
}

/// Delete up to `budget` unreferenced nodes. A root is reclaimed once no
/// cell references it and no parent branch holds it; deleting a branch
/// releases its children, which are reclaimed in turn. Returns the number
/// of nodes deleted.
pub(crate) fn reclaim(conn: &Connection, chunks: &Chunks, budget: usize) -> Result<usize, Error> {
    let mut deleted = 0;
    let mut queue: Vec<Vec<u8>> = conn
        .prepare_cached("SELECT root FROM _jazz_lv_reclaim LIMIT ?1")
        .and_then(|mut statement| {
            statement
                .query_map([budget as i64], |r| r.get(0))?
                .collect::<Result<_, _>>()
        })
        .map_err(storage)?;
    for root in &queue {
        conn.execute("DELETE FROM _jazz_lv_reclaim WHERE root = ?1", [root])
            .map_err(storage)?;
    }
    while let Some(locator) = queue.pop() {
        if deleted >= budget {
            // Leave the rest for the next pass.
            conn.execute(
                "INSERT OR IGNORE INTO _jazz_lv_reclaim (root) VALUES (?1)",
                [&locator],
            )
            .map_err(storage)?;
            continue;
        }
        let referenced: bool = conn
            .query_row(
                "SELECT EXISTS (SELECT 1 FROM _jazz_lv_root WHERE root = ?1 \
                 AND durable > 0) \
                 OR EXISTS (SELECT 1 FROM _jazz_lv_edge WHERE child = ?1) \
                 OR EXISTS (SELECT 1 FROM _jazz_lv_receipt WHERE locator = ?1) \
                 OR EXISTS (SELECT 1 FROM _jazz_lv_upload_node WHERE locator = ?1)",
                [&locator],
                |r| r.get(0),
            )
            .map_err(storage)?;
        if referenced {
            continue;
        }
        let node: Option<(Vec<u8>, Vec<u8>)> = conn
            .query_row(
                "SELECT hash, bytes FROM _jazz_lv_node WHERE locator = ?1",
                [&locator],
                |r| Ok((r.get(0)?, r.get(1)?)),
            )
            .optional()
            .map_err(storage)?;
        conn.execute("DELETE FROM _jazz_lv_root WHERE root = ?1", [&locator])
            .map_err(storage)?;
        let Some((hash, bytes)) = node else {
            continue;
        };
        conn.execute("DELETE FROM _jazz_lv_node WHERE locator = ?1", [&locator])
            .map_err(storage)?;
        if let Ok(key) = <[u8; 32]>::try_from(locator.as_slice()) {
            chunks.forget(&key);
        }
        deleted += 1;
        let hash =
            <[u8; 32]>::try_from(hash.as_slice()).map_err(|_| corrupt(lv::Error::MalformedNode))?;
        let node = lv::decode_node_untyped_authenticated(lv::ContentHash(hash), &bytes)
            .map_err(corrupt)?;
        if let ChunkNode::Branch { children, .. } = node {
            conn.execute("DELETE FROM _jazz_lv_edge WHERE parent = ?1", [&locator])
                .map_err(storage)?;
            for child in children {
                queue.push(child.node_ref.locator.as_bytes().to_vec());
            }
        }
    }
    Ok(deleted)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn text(seed: u64, len: usize) -> Vec<u8> {
        let mut state = seed.wrapping_mul(0x9E37_79B9_7F4A_7C15) | 1;
        (0..len)
            .map(|_| {
                state ^= state << 13;
                state ^= state >> 7;
                state ^= state << 17;
                b'a' + (state % 26) as u8
            })
            .collect()
    }

    fn chunks() -> (Connection, Arc<Chunks>) {
        let conn = Connection::open_in_memory().unwrap();
        let chunks = Chunks::install(&conn, None).unwrap();
        conn.execute_batch(DDL).unwrap();
        (conn, chunks)
    }

    fn chunked(
        conn: &Connection,
        chunks: &Chunks,
        kind: LargeValueKind,
        logical: &[u8],
    ) -> Vec<u8> {
        let prepared = lv::prepare(kind, logical).unwrap();
        store(conn, chunks, &prepared, false).unwrap();
        chunked_cell(&prepared.value_ref).unwrap()
    }

    /// Every physical form of a value: inline, chunked, and chunked as a
    /// shorter base plus an appended edit tail.
    fn forms(
        conn: &Connection,
        chunks: &Chunks,
        kind: LargeValueKind,
        logical: &[u8],
    ) -> Vec<Vec<u8>> {
        let inline = match kind {
            LargeValueKind::Bytes => inline_bytes(logical),
            _ => logical.to_vec(),
        };
        let mut forms = vec![inline];
        if !logical.is_empty() {
            forms.push(chunked(conn, chunks, kind, logical));
        }
        if logical.len() > 10 {
            let (base, tail) = logical.split_at(logical.len() - 7);
            let prepared = lv::prepare(kind, base).unwrap();
            store(conn, chunks, &prepared, false).unwrap();
            let lv::TailOutcome::Updated(appended) =
                lv::append_tail(&prepared.value_ref, tail.to_vec()).unwrap()
            else {
                panic!("a short append fits the tail");
            };
            forms.push(chunked_cell(&appended).unwrap());
        }
        forms
    }

    /// The collation must be a total order identical to `memcmp` over
    /// logical bytes, or indexes are corrupted.
    #[test]
    fn collation_matches_the_logical_oracle_across_forms() {
        let (conn, chunks) = chunks();
        let big = text(1, 200 * 1024);
        let mut deep = big.clone();
        deep[150_000] = b'A';
        let mut logical = [
            Vec::new(),
            b"a".to_vec(),
            big[..9].to_vec(),
            big[..70_000].to_vec(),
            big.clone(),
            deep,
            [big.as_slice(), b"zz"].concat(),
            text(2, 90_000),
        ];
        logical.sort();
        for kind in [LargeValueKind::String, LargeValueKind::Bytes] {
            let cells: Vec<(usize, Vec<u8>)> = logical
                .iter()
                .enumerate()
                .flat_map(|(rank, value)| {
                    forms(&conn, &chunks, kind, value)
                        .into_iter()
                        .map(move |cell| (rank, cell))
                })
                .collect();
            for (rank_a, a) in &cells {
                for (rank_b, b) in &cells {
                    assert_eq!(
                        chunks.compare(a, b).unwrap(),
                        rank_a.cmp(rank_b),
                        "{kind:?}: values {rank_a} and {rank_b}"
                    );
                }
            }
        }
        assert!(chunks.take_demand().is_empty());
    }

    #[test]
    fn an_absent_chunk_is_demand_not_an_answer() {
        let (conn, chunks) = chunks();
        let cell = chunked(&conn, &chunks, LargeValueKind::String, &text(3, 100_000));
        let (_, empty) = self::chunks();
        assert_eq!(
            empty.materialize(&cell).unwrap_err().code,
            ErrorCode::NotObserved
        );
        assert!(!empty.take_demand().is_empty());
    }

    #[test]
    fn unreferenced_trees_are_reclaimed_whole() {
        let (conn, chunks) = chunks();
        conn.execute_batch(
            "CREATE TABLE t (id INTEGER PRIMARY KEY, body COLLATE jazz);
             CREATE TABLE t__h (id INTEGER, body COLLATE jazz);",
        )
        .unwrap();
        let table = Table {
            name: "t".to_owned(),
            columns: Vec::new(),
            references: Default::default(),
            policies: Default::default(),
            indexes: Vec::new(),
            branch_by: Vec::new(),
            provenance: Vec::new(),
            stored_provenance: Vec::new(),
        };
        let column = Column {
            name: "body".to_owned(),
            ty: crate::records::ValueType::String,
            default: None,
            json: false,
        };
        conn.execute_batch(&triggers(&table, &[&column])).unwrap();
        let count = |sql: &str| conn.query_row(sql, [], |r| r.get::<_, i64>(0)).unwrap();
        // Cells are TEXT, as the engine binds them.
        let text_cell = |cell: Vec<u8>| crate::values::Sql::Text(cell);
        let one = text_cell(chunked(
            &conn,
            &chunks,
            LargeValueKind::String,
            &text(4, 400_000),
        ));
        let two = text_cell(chunked(
            &conn,
            &chunks,
            LargeValueKind::String,
            &text(5, 400_000),
        ));
        let nodes = count("SELECT count(*) FROM _jazz_lv_node");
        assert!(nodes > 8);
        conn.execute("INSERT INTO t (id, body) VALUES (1, ?1)", [&one])
            .unwrap();
        conn.execute("INSERT INTO t__h (id, body) VALUES (1, ?1)", [&one])
            .unwrap();
        conn.execute("INSERT INTO t (id, body) VALUES (2, ?1)", [&two])
            .unwrap();
        // The tree of `two` is referenced once; `one` twice.
        conn.execute("UPDATE t SET body = 'inline' WHERE id = 2", [])
            .unwrap();
        conn.execute("DELETE FROM t WHERE id = 1", []).unwrap();
        while reclaim(&conn, &chunks, 3).unwrap() > 0 {}
        let left = count("SELECT count(*) FROM _jazz_lv_node");
        assert!(
            left > 0 && left < nodes,
            "only `two`'s tree goes: {left} of {nodes}"
        );
        conn.execute("DELETE FROM t__h", []).unwrap();
        while reclaim(&conn, &chunks, 3).unwrap() > 0 {}
        assert_eq!(count("SELECT count(*) FROM _jazz_lv_node"), 0);
        assert_eq!(count("SELECT count(*) FROM _jazz_lv_reclaim"), 0);
    }

    #[test]
    fn admission_checks_descriptors_against_their_tree() {
        let (conn, chunks) = chunks();
        let body = text(6, 300_000);
        let built = lv::prepare(LargeValueKind::String, &body).unwrap();
        // Pushed by a client: stored under receipts, never validated here.
        for chunk in &built.staged_chunks {
            receive_pushed(
                &conn,
                &chunks,
                &request_of(&chunk.node_ref),
                chunk.encoded.clone(),
                0,
            )
            .unwrap();
        }
        let mut forged = built.value_ref.clone();
        forged.byte_length += 1;
        assert!(admit(&conn, &chunks, &forged).is_err(), "dishonest metrics");
        admit(&conn, &chunks, &built.value_ref).unwrap();
        // Once validated, the root's base is trusted and checked directly.
        assert!(admit(&conn, &chunks, &forged).is_err());
        let appended = match lv::append_tail(&built.value_ref, b"!".to_vec()).unwrap() {
            lv::TailOutcome::Updated(value) => value,
            other => panic!("{other:?}"),
        };
        admit(&conn, &chunks, &appended).unwrap();

        // A tree with a node missing is refused.
        let other = lv::prepare(LargeValueKind::String, &text(7, 300_000)).unwrap();
        for chunk in other.staged_chunks.iter().skip(1) {
            receive_pushed(
                &conn,
                &chunks,
                &request_of(&chunk.node_ref),
                chunk.encoded.clone(),
                0,
            )
            .unwrap();
        }
        assert!(admit(&conn, &chunks, &other.value_ref).is_err());
    }

    #[test]
    fn expired_receipts_release_unreferenced_pushes() {
        let (conn, chunks) = chunks();
        let built = lv::prepare(LargeValueKind::String, &text(8, 300_000)).unwrap();
        for chunk in &built.staged_chunks {
            receive_pushed(
                &conn,
                &chunks,
                &request_of(&chunk.node_ref),
                chunk.encoded.clone(),
                1_000,
            )
            .unwrap();
        }
        let count = || {
            conn.query_row("SELECT count(*) FROM _jazz_lv_node", [], |r| {
                r.get::<_, i64>(0)
            })
            .unwrap()
        };
        let pushed = count();
        assert!(pushed > 2);
        expire_receipts(&conn, 999).unwrap();
        while reclaim(&conn, &chunks, 64).unwrap() > 0 {}
        assert_eq!(count(), pushed, "receipts still hold the nodes");
        expire_receipts(&conn, 1_000).unwrap();
        while reclaim(&conn, &chunks, 64).unwrap() > 0 {}
        assert_eq!(count(), 0, "nothing referenced them");
    }

    #[test]
    fn ended_uploads_keep_only_what_a_row_references() {
        let (conn, chunks) = chunks();
        let count = || {
            conn.query_row("SELECT count(*) FROM _jazz_lv_node", [], |r| {
                r.get::<_, i64>(0)
            })
            .unwrap()
        };
        let kept = lv::prepare(LargeValueKind::String, &text(9, 300_000)).unwrap();
        let dropped = lv::prepare(LargeValueKind::String, &text(10, 300_000)).unwrap();
        for (id, built) in [([1; 32], &kept), ([2; 32], &dropped)] {
            begin_upload(&conn, &id, 1_000).unwrap();
            stage_upload(&conn, &chunks, &id, &built.staged_chunks, false).unwrap();
        }
        let staged = count();
        // An expiry pass before the leases age out keeps both uploads.
        assert_eq!(expire_uploads(&conn, 999).unwrap(), 0);
        while reclaim(&conn, &chunks, 64).unwrap() > 0 {}
        assert_eq!(count(), staged, "the leases hold every staged node");
        // The first upload's row commits; the second is abandoned.
        conn.execute(
            "INSERT INTO _jazz_lv_root (root, durable) VALUES (?1, 1)",
            [kept.value_ref.root.locator.as_bytes().as_slice()],
        )
        .unwrap();
        end_upload(&conn, &[1; 32]).unwrap();
        assert!(touch_upload(&conn, &[2; 32], 1_500).unwrap());
        assert_eq!(expire_uploads(&conn, 1_500).unwrap(), 1);
        assert!(!touch_upload(&conn, &[2; 32], 2_000).unwrap());
        while reclaim(&conn, &chunks, 64).unwrap() > 0 {}
        assert_eq!(count(), kept.staged_chunks.len() as i64);
    }
}
