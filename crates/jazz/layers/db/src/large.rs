//! Chunked large values in SQLite cells.
//!
//! `string`, `json` and `bytes` columns use the large-value format of
//! `jazz_types::large_values`: a value above the inline threshold is a
//! descriptor over an immutable, content-defined chunk tree. Every such
//! column stores both arms as SQLite TEXT, so one collation sees them all:
//!
//! - an inline string or JSON cell is its UTF-8 bytes, unchanged;
//! - an inline bytes cell is [`BYTES`] followed by the raw bytes;
//! - a chunked cell is [`CHUNKED`], the root locator, the cell's sort
//!   prefix (its length as five ASCII digits, then the first
//!   [`PREFIX_BYTES`] logical bytes in inline-cell form) and the canonical
//!   descriptor.
//!
//! Neither tag byte occurs in UTF-8, so every cell says which arm it is.
//! Everything SQL needs is at a fixed offset or length-prefixed, so the
//! schema, the refcount triggers and every compiled query use only SQL
//! built-ins and run on any SQLite (vanilla builds such as Cloudflare D1
//! or Durable Objects included): no collation, no functions. Ordering and
//! ranges read [`key_sql`], which is the cell itself when inline and the
//! sort prefix when chunked; the rare questions the prefix cannot answer
//! (values sharing it, `contains`, literals over the inline threshold) are
//! answered in Rust before the statement runs and handed to it through
//! `_jazz_lv_scratch`. Chunks live in `_jazz_lv_node` in the same
//! database, so a row and the chunks it references commit in one
//! transaction; per-root reference counts are kept by triggers on every
//! table that has a large-capable column. With a chunk store
//! ([`crate::chunk_store`]) the bytes of settled leaves move out of the
//! file on tick; their metadata stays.

use crate::values::CachedRow;
use std::cmp::Ordering;
use std::collections::{BTreeSet, HashMap};
use std::sync::{Arc, Mutex};

use bytes::Bytes;
use jazz_types::large_values::{
    self as lv, AttemptError, ChunkInputs, ChunkNode, LargeValueKind, LargeValueRef,
    chunks::ChunkRequest,
};
use rusqlite::{Connection, OptionalExtension};

use crate::chunk_store::{ChunkStore, NodeKey};
use crate::options::{Error, ErrorCode, Reason};
use crate::values::{Column, Table, quote, storage};

#[cfg(feature = "testing")]
thread_local! {
    static FULL_MATERIALIZATIONS: std::cell::Cell<usize> = const { std::cell::Cell::new(0) };
}

/// How many chunked values have been rebuilt whole on this thread, so tests
/// can tell a range read from a full materialization. Per thread, so tests
/// running in parallel in one process do not count each other's reads.
#[cfg(feature = "testing")]
pub fn full_materializations_for_test() -> usize {
    FULL_MATERIALIZATIONS.with(std::cell::Cell::get)
}

/// First byte of an inline bytes cell.
pub(crate) const BYTES: u8 = 0xFE;
/// First byte of a chunked cell.
pub(crate) const CHUNKED: u8 = 0xFF;
/// Logical bytes a chunked cell's sort prefix holds (fewer for text, which
/// is cut on a code-point boundary). Ranges against literals up to
/// [`EXACT_RANGE_BYTES`] are answered by the prefix alone.
pub(crate) const PREFIX_BYTES: usize = 4096;
pub(crate) const EXACT_RANGE_BYTES: usize = PREFIX_BYTES - 3;
/// Byte offsets (1-based, as SQL `substr` counts) in a chunked cell.
const ROOT_AT: usize = 2;
const PREFIX_LEN_AT: usize = ROOT_AT + 32;
const PREFIX_AT: usize = PREFIX_LEN_AT + 5;

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
/// references them, until that commit lands or `expires` (ms); `_jazz_lv_unpushed` holds nodes
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
CREATE TABLE IF NOT EXISTS _jazz_lv_scratch (slot INTEGER NOT NULL, cell BLOB NOT NULL, \
 v BLOB, PRIMARY KEY (slot, cell)) WITHOUT ROWID;
CREATE TRIGGER IF NOT EXISTS _jazz_lv_root_unreferenced AFTER UPDATE ON _jazz_lv_root \
 WHEN NEW.durable = 0 BEGIN \
 INSERT OR IGNORE INTO _jazz_lv_reclaim (root) VALUES (NEW.root); END;
";

/// The chunk-store queues, in a store opened with a chunk store
/// ([`crate::chunk_store`]); a store without one never has them, so its file
/// is exactly what it was before chunk stores existed.
///
/// `_jazz_lv_offload` queues leaves whose bytes are still in
/// `_jazz_lv_node`: `state` 0 is queued, 1 is a put that may have reached
/// the chunk store (recorded before the put starts, so a crash or a
/// reclamation mid-put still deletes what it wrote). Once the put
/// succeeds the node's bytes become empty, which no encoded node is, and
/// the queue row goes. `_jazz_lv_offload_delete` holds reclaimed leaves to
/// delete from the chunk store, after the reclamation committed.
const OFFLOAD_DDL: &str = "\
CREATE TABLE IF NOT EXISTS _jazz_lv_offload (locator BLOB PRIMARY KEY, state INTEGER NOT NULL) \
 WITHOUT ROWID;
CREATE TABLE IF NOT EXISTS _jazz_lv_offload_delete (locator BLOB PRIMARY KEY, \
 hash BLOB NOT NULL) WITHOUT ROWID;
";

/// Prepare a store for its chunk store, or refuse to open a store that
/// has one without it. The first open with a chunk store queues every leaf
/// the file already holds.
pub(crate) fn open_offload(conn: &Connection, chunks: &Chunks) -> Result<(), Error> {
    let exists: bool = conn
        .query_row(
            "SELECT EXISTS (SELECT 1 FROM sqlite_master WHERE type = 'table' \
             AND name = '_jazz_lv_offload')",
            [],
            |r| r.get(0),
        )
        .map_err(storage)?;
    match (exists, chunks.offload_store().is_some()) {
        (true, false) => Err(unconfigured()),
        (false, false) | (true, true) => Ok(()),
        (false, true) => crate::values::in_savepoint(conn, "jazz_lv_offload", || {
            crate::db::execute_script(conn, OFFLOAD_DDL)?;
            conn.execute(
                "INSERT INTO _jazz_lv_offload (locator, state) SELECT n.locator, 0 \
                 FROM _jazz_lv_node n WHERE NOT EXISTS \
                 (SELECT 1 FROM _jazz_lv_edge e WHERE e.parent = n.locator)",
                [],
            )
            .map_err(storage)?;
            Ok(())
        }),
    }
}

/// Up to `limit` queued leaves that are settled (no unpushed upload,
/// receipt or upload lease holds them), marked as being put. Their bytes
/// must reach the chunk store before [`offloaded`] drops them here.
pub(crate) fn begin_offload(
    conn: &Connection,
    limit: usize,
) -> Result<Vec<(NodeKey, Vec<u8>)>, Error> {
    crate::values::in_savepoint(conn, "jazz_lv_offload", || {
        let picked: Vec<(Vec<u8>, Vec<u8>, Vec<u8>)> = conn
            .prepare_cached(
                "SELECT o.locator, n.hash, n.bytes FROM _jazz_lv_offload o \
                 JOIN _jazz_lv_node n ON n.locator = o.locator \
                 WHERE NOT EXISTS (SELECT 1 FROM _jazz_lv_unpushed WHERE locator = o.locator) \
                 AND NOT EXISTS (SELECT 1 FROM _jazz_lv_receipt WHERE locator = o.locator) \
                 AND NOT EXISTS (SELECT 1 FROM _jazz_lv_upload_node WHERE locator = o.locator) \
                 LIMIT ?1",
            )
            .and_then(|mut statement| {
                statement
                    .query_map([limit as i64], |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)))?
                    .collect::<Result<_, _>>()
            })
            .map_err(storage)?;
        let mut mark = conn
            .prepare_cached("UPDATE _jazz_lv_offload SET state = 1 WHERE locator = ?1")
            .map_err(storage)?;
        let mut out = Vec::with_capacity(picked.len());
        for (locator, hash, bytes) in picked {
            mark.execute([&locator]).map_err(storage)?;
            out.push((key(&locator, &hash)?, bytes));
        }
        Ok(out)
    })
}

/// A leaf's bytes reached the chunk store: drop them here, unless it was
/// reclaimed (and perhaps stored again) while the put ran.
pub(crate) fn offloaded(conn: &Connection, node: &NodeKey) -> Result<(), Error> {
    let locator = node.locator.as_slice();
    conn.prepare_cached(
        "UPDATE _jazz_lv_node SET bytes = X'' WHERE locator = ?1 AND EXISTS \
         (SELECT 1 FROM _jazz_lv_offload WHERE locator = ?1 AND state = 1)",
    )
    .and_then(|mut statement| statement.execute([locator]))
    .and_then(|_| {
        conn.prepare_cached("DELETE FROM _jazz_lv_offload WHERE locator = ?1 AND state = 1")
    })
    .and_then(|mut statement| statement.execute([locator]))
    .map_err(storage)?;
    Ok(())
}

/// Up to `limit` reclaimed leaves to delete from the chunk store.
pub(crate) fn pending_deletes(conn: &Connection, limit: usize) -> Result<Vec<NodeKey>, Error> {
    let rows: Vec<(Vec<u8>, Vec<u8>)> = conn
        .prepare_cached("SELECT locator, hash FROM _jazz_lv_offload_delete LIMIT ?1")
        .and_then(|mut statement| {
            statement
                .query_map([limit as i64], |r| Ok((r.get(0)?, r.get(1)?)))?
                .collect::<Result<_, _>>()
        })
        .map_err(storage)?;
    rows.iter()
        .map(|(locator, hash)| key(locator, hash))
        .collect()
}

/// A reclaimed leaf is gone from the chunk store.
pub(crate) fn deleted(conn: &Connection, node: &NodeKey) -> Result<(), Error> {
    conn.prepare_cached("DELETE FROM _jazz_lv_offload_delete WHERE locator = ?1")
        .and_then(|mut statement| statement.execute([node.locator.as_slice()]))
        .map_err(storage)?;
    Ok(())
}

fn key(locator: &[u8], hash: &[u8]) -> Result<NodeKey, Error> {
    let malformed = || corrupt(lv::Error::MalformedNode);
    Ok(NodeKey {
        locator: locator.try_into().map_err(|_| malformed())?,
        hash: hash.try_into().map_err(|_| malformed())?,
    })
}

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
/// columns, in the current and history tables, against its root, and a
/// partial index per column of the rows holding one. Plain SQL: the root
/// sits at a fixed offset in a chunked cell.
///
/// One trigger per event and table covers every column: each schema
/// object costs SQLite a scan of `sqlite_schema` when it is created, so a
/// trigger per column made opening a wide schema quadratic. A store that
/// gains large-capable columns later adds a set for them, named after the
/// first (`set`), beside the table's earlier ones.
pub(crate) fn triggers(table: &Table, columns: &[&Column], set: Option<&str>) -> String {
    let mut sql = String::new();
    if columns.is_empty() {
        return sql;
    }
    let names: Vec<String> = columns.iter().map(|c| quote(&c.stored)).collect();
    let any = |test: &dyn Fn(&str) -> String| {
        names
            .iter()
            .map(|name| test(name))
            .collect::<Vec<_>>()
            .join(" OR ")
    };
    // Each statement acts only on a chunked cell: a NULL root matches no
    // row and inserts none.
    let retain = |cell: &str, unless: Option<&str>| {
        let unless = unless.map_or(String::new(), |old| {
            format!(" AND r IS NOT {}", root_sql(old))
        });
        format!(
            "INSERT INTO _jazz_lv_root (root, durable) SELECT r, 1 FROM \
             (SELECT {} AS r) WHERE r IS NOT NULL{unless} \
             ON CONFLICT (root) DO UPDATE SET durable = durable + 1;",
            root_sql(cell)
        )
    };
    let release = |cell: &str, unless: Option<&str>| {
        let unless = unless.map_or(String::new(), |new| {
            format!(" AND {} IS NOT {}", root_sql(cell), root_sql(new))
        });
        format!(
            "UPDATE _jazz_lv_root SET durable = durable - 1 WHERE root = {}{unless};",
            root_sql(cell)
        )
    };
    for target in [table.stored.clone(), format!("{}__h", table.stored)] {
        let on = quote(&target);
        let trigger = |what: &str| match set {
            None => quote(&format!("{target}__lv_{what}")),
            Some(set) => quote(&format!("{target}__lv_{what}_{set}")),
        };
        for (column, name) in columns.iter().zip(&names) {
            // Only chunked rows: what a prep scans, near-empty otherwise.
            sql.push_str(&format!(
                "CREATE INDEX IF NOT EXISTS {} ON {on} (id) WHERE {};\n",
                quote(&format!("{target}__lv_{}", column.stored)),
                chunked_sql(name)
            ));
        }
        let each = |statement: &dyn Fn(&str) -> String| -> String {
            names
                .iter()
                .map(|name| statement(name))
                .collect::<Vec<_>>()
                .join(" ")
        };
        sql.push_str(&format!(
            "CREATE TRIGGER IF NOT EXISTS {ins} AFTER INSERT ON {on} WHEN {} BEGIN {} END;\n\
             CREATE TRIGGER IF NOT EXISTS {del} AFTER DELETE ON {on} WHEN {} BEGIN {} END;\n\
             CREATE TRIGGER IF NOT EXISTS {upd} AFTER UPDATE OF {} ON {on} WHEN {} BEGIN {} END;\n",
            any(&|name| chunked_sql(&format!("NEW.{name}"))),
            each(&|name| retain(&format!("NEW.{name}"), None)),
            any(&|name| chunked_sql(&format!("OLD.{name}"))),
            each(&|name| release(&format!("OLD.{name}"), None)),
            names.join(", "),
            any(&|name| format!(
                "{} IS NOT {}",
                root_sql(&format!("OLD.{name}")),
                root_sql(&format!("NEW.{name}"))
            )),
            each(&|name| format!(
                "{} {}",
                release(&format!("OLD.{name}"), Some(&format!("NEW.{name}"))),
                retain(&format!("NEW.{name}"), Some(&format!("OLD.{name}")))
            )),
            ins = trigger("ins"),
            del = trigger("del"),
            upd = trigger("upd"),
        ));
    }
    sql
}

/// SQL: whether the cell `cell` is chunked.
pub(crate) fn chunked_sql(cell: &str) -> String {
    format!("(substr(CAST({cell} AS BLOB), 1, 1) = X'FF')")
}

/// SQL: a chunked cell's root locator; NULL for any other cell.
pub(crate) fn root_sql(cell: &str) -> String {
    format!(
        "(CASE WHEN {} THEN substr(CAST({cell} AS BLOB), {ROOT_AT}, 32) END)",
        chunked_sql(cell)
    )
}

/// SQL: the sort key of a large-capable cell, the expression its indexes,
/// orderings and ranges use. An inline cell is its own key; a chunked
/// cell's key is its sort prefix, which is a prefix of its logical value
/// in the same encoding, so keys order as the values do except between
/// chunked values that share the whole prefix.
pub(crate) fn key_sql(cell: &str) -> String {
    let blob = format!("CAST({cell} AS BLOB)");
    format!(
        "(CASE WHEN {} THEN CAST(substr({blob}, {PREFIX_AT}, \
         CAST(CAST(substr({blob}, {PREFIX_LEN_AT}, 5) AS TEXT) AS INTEGER)) AS TEXT) \
         ELSE {cell} END)",
        chunked_sql(cell)
    )
}

/// The sort prefix of a value of `kind` whose logical bytes start with
/// `head` (at least [`PREFIX_BYTES`] of them, or all), in inline-cell form.
pub(crate) fn sort_prefix(kind: LargeValueKind, head: &[u8]) -> Vec<u8> {
    let mut end = head.len().min(PREFIX_BYTES);
    match kind {
        LargeValueKind::Bytes => {
            let mut prefix = Vec::with_capacity(end + 1);
            prefix.push(BYTES);
            prefix.extend_from_slice(&head[..end]);
            prefix
        }
        LargeValueKind::String | LargeValueKind::Json => {
            // Text is valid UTF-8: back up to a code-point boundary.
            while end < head.len() && end > 0 && (head[end] & 0xC0) == 0x80 {
                end -= 1;
            }
            head[..end].to_vec()
        }
    }
}

/// An inline bytes cell.
pub(crate) fn inline_bytes(bytes: &[u8]) -> Vec<u8> {
    let mut cell = Vec::with_capacity(bytes.len() + 1);
    cell.push(BYTES);
    cell.extend_from_slice(bytes);
    cell
}

/// A chunked cell. `prefix` is [`sort_prefix`] of its logical value.
pub(crate) fn chunked_cell(value: &LargeValueRef, prefix: &[u8]) -> Result<Vec<u8>, Error> {
    let descriptor = lv::encode_large_value_ref(value).map_err(corrupt)?;
    let mut cell = Vec::with_capacity(PREFIX_AT - 1 + prefix.len() + descriptor.len());
    cell.push(CHUNKED);
    cell.extend_from_slice(value.root.locator.as_bytes());
    cell.extend_from_slice(format!("{:05}", prefix.len()).as_bytes());
    cell.extend_from_slice(prefix);
    cell.extend(descriptor);
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
            lv::decode_large_value_ref(chunked_descriptor(cell)?).map_err(corrupt)?,
        )),
        _ => Logical::Inline(cell),
    })
}

/// The descriptor bytes of a chunked cell.
fn chunked_descriptor(cell: &[u8]) -> Result<&[u8], Error> {
    let bad = || corrupt(lv::Error::MalformedScalar);
    let len = cell
        .get(PREFIX_LEN_AT - 1..PREFIX_AT - 1)
        .and_then(|digits| std::str::from_utf8(digits).ok())
        .and_then(|digits| digits.parse::<usize>().ok())
        .ok_or_else(bad)?;
    cell.get(PREFIX_AT - 1 + len..).ok_or_else(bad)
}

fn corrupt(error: lv::Error) -> Error {
    Error::new(ErrorCode::Storage, format!("large value: {error}"))
}

const MISSING: &str = "large-value chunks are not available locally yet; they were requested \
     from upstream, so retry after the next sync tick";

pub(crate) fn missing() -> Error {
    Error::with_reason(ErrorCode::NotObserved, Reason::MissingChunks, MISSING)
}

/// An offloaded node its chunk store does not hold: data loss, not demand.
fn lost() -> Error {
    Error::new(
        ErrorCode::Storage,
        "large value: a node this store moved to its chunk store is missing from it",
    )
}

fn unconfigured() -> Error {
    Error::new(
        ErrorCode::Storage,
        "large value: this store keeps nodes in a chunk store, but none was configured; \
         open it with the chunk store it was written with",
    )
}

/// Whether `error` is a statement that needed chunks this store lacks.
pub(crate) fn is_missing(error: &Error) -> bool {
    error.reason == Some(Reason::MissingChunks)
}

/// Verified chunks this process has read or written, and the way to read
/// the rest. Shared by the database connection's commit hooks and by Rust
/// code that decodes cells.
pub(crate) struct Chunks {
    state: Mutex<State>,
    /// Where settled leaf bytes live when not in the file
    /// ([`crate::chunk_store`]).
    store: Option<Arc<dyn ChunkStore>>,
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
    /// Offloaded nodes a statement needed that the chunk store could not
    /// answer at once: the next fetch reads them.
    fetch: BTreeSet<ChunkRequest>,
    /// Offloaded nodes the chunk store answered it does not hold.
    lost: BTreeSet<ChunkRequest>,
    /// A fetch is running: one at a time, so a delete never overtakes the
    /// put it undoes.
    pumping: bool,
}

/// Where a node's verified bytes are.
enum Local {
    Found(Bytes),
    /// Nothing here holds it: demand on the upstream.
    Absent,
    /// The chunk store holds it, and answers only asynchronously.
    Fetching,
}

struct Cached {
    hash: [u8; 32],
    bytes: Bytes,
    /// Written by the open transaction: the reader connection cannot see it
    /// until commit, so it must not be evicted before then.
    pinned: bool,
    /// Fetched from the chunk store for reads that wait on it: kept until
    /// no read is waiting, so a read needing many nodes is not starved by
    /// evicting the first it fetched.
    held: bool,
    used: u64,
}

impl Chunks {
    /// Chunks of a store in memory (`path` is `None`) or the file at
    /// `path`, on its VFS.
    pub(crate) fn install(
        path: Option<(&std::path::Path, Option<&'static str>)>,
        store: Option<Arc<dyn ChunkStore>>,
    ) -> Result<Arc<Self>, Error> {
        let reader = match path {
            None => None,
            Some((path, vfs)) => {
                let flags = rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY
                    | rusqlite::OpenFlags::SQLITE_OPEN_NO_MUTEX;
                let reader = match vfs {
                    None => Connection::open_with_flags(path, flags),
                    Some(vfs) => Connection::open_with_flags_and_vfs(path, flags, vfs),
                }
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
                fetch: BTreeSet::new(),
                lost: BTreeSet::new(),
                pumping: false,
            }),
            store,
        });
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
                held: false,
                used,
            },
        );
        if let Some(old) = old
            && !old.pinned
        {
            state.bytes -= old.bytes.len();
        }
    }

    /// The chunk store, if this store has one.
    pub(crate) fn store(&self) -> Option<&Arc<dyn ChunkStore>> {
        self.store.as_ref()
    }

    /// The chunk store settled leaves move to, if they move: a store in
    /// memory keeps its nodes in its cache whatever holds them, so it keeps
    /// them in SQLite too.
    pub(crate) fn offload_store(&self) -> Option<&Arc<dyn ChunkStore>> {
        self.store.as_ref().filter(|_| !self.in_memory())
    }

    /// Record that a read waits on an offloaded node.
    fn want(&self, request: &ChunkRequest) {
        let mut state = self.state.lock().expect("chunk cache");
        state.fetch.insert(request.clone());
    }

    /// Whether reads wait on nodes only the chunk store can answer.
    pub(crate) fn has_fetch(&self) -> bool {
        !self.state.lock().expect("chunk cache").fetch.is_empty()
    }

    /// Start a fetch: the nodes reads are waiting on, or `None` if another
    /// fetch is running. With nothing waiting, nodes kept for earlier
    /// waiting reads may be evicted again.
    pub(crate) fn begin_fetch(&self) -> Option<Vec<ChunkRequest>> {
        let mut state = self.state.lock().expect("chunk cache");
        if state.pumping {
            return None;
        }
        state.pumping = true;
        if state.fetch.is_empty() {
            for node in state.nodes.values_mut() {
                node.held = false;
            }
            state.evict();
        }
        Some(std::mem::take(&mut state.fetch).into_iter().collect())
    }

    /// End a fetch [`Self::begin_fetch`] started.
    pub(crate) fn end_fetch(&self) {
        self.state.lock().expect("chunk cache").pumping = false;
    }

    /// Install what the chunk store answered for an offloaded node.
    pub(crate) fn install_from_store(
        &self,
        request: &ChunkRequest,
        bytes: Option<Bytes>,
    ) -> Result<(), Error> {
        let mut state = self.state.lock().expect("chunk cache");
        let Some(bytes) = bytes else {
            state.lost.insert(request.clone());
            return Ok(());
        };
        if lv::object_hash(&bytes).0 != request.object_hash {
            return Err(corrupt(lv::Error::ObjectHashMismatch));
        }
        state.clock += 1;
        let used = state.clock;
        state.bytes += bytes.len();
        let old = state.nodes.insert(
            *request.locator.as_bytes(),
            Cached {
                hash: request.object_hash,
                bytes,
                pinned: false,
                held: true,
                used,
            },
        );
        if let Some(old) = old
            && !old.pinned
        {
            state.bytes -= old.bytes.len();
        }
        Ok(())
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

    /// Close the read-only connection. The store's own connection must be
    /// the last one on the file when it closes: only the last connection
    /// checkpoints the WAL into the file and removes it, and a read-only one
    /// cannot.
    pub(crate) fn close_reader(&self) {
        self.state.lock().expect("chunk cache").reader = None;
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

    /// Whether a read recorded chunks to request, beyond those `settled`
    /// accounts for, that the next send has not taken yet.
    pub(crate) fn has_demand_beyond(&self, settled: impl Fn(&ChunkRequest) -> bool) -> bool {
        let state = self.state.lock().expect("chunk cache");
        state.demand.iter().any(|request| !settled(request))
    }

    /// The chunks statements needed since the last call.
    pub(crate) fn take_demand(&self) -> BTreeSet<ChunkRequest> {
        std::mem::take(&mut self.state.lock().expect("chunk cache").demand)
    }

    /// The verified bytes of one node, if anything local holds it.
    fn get(&self, request: &ChunkRequest) -> Result<Local, Error> {
        let mut state = self.state.lock().expect("chunk cache");
        state.clock += 1;
        let used = state.clock;
        if let Some(node) = state.nodes.get_mut(request.locator.as_bytes()) {
            if node.hash != request.object_hash {
                return Err(corrupt(lv::Error::ObjectHashMismatch));
            }
            node.used = used;
            return Ok(Local::Found(node.bytes.clone()));
        }
        let Some(reader) = &state.reader else {
            return Ok(Local::Absent);
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
            return Ok(Local::Absent);
        };
        if hash != request.object_hash {
            return Err(corrupt(lv::Error::ObjectHashMismatch));
        }
        let bytes = if bytes.is_empty() {
            // Offloaded: its bytes are in the chunk store.
            if state.lost.contains(request) {
                return Err(lost());
            }
            let Some(store) = &self.store else {
                return Err(unconfigured());
            };
            match store.get_now(&NodeKey::from(request)) {
                None => return Ok(Local::Fetching),
                Some(Ok(None)) => {
                    state.lost.insert(request.clone());
                    return Err(lost());
                }
                Some(Err(error)) => return Err(error),
                Some(Ok(Some(bytes))) => bytes,
            }
        } else {
            Bytes::from(bytes)
        };
        if lv::object_hash(&bytes).0 != request.object_hash {
            return Err(corrupt(lv::Error::ObjectHashMismatch));
        }
        state.bytes += bytes.len();
        state.nodes.insert(
            *request.locator.as_bytes(),
            Cached {
                hash: request.object_hash,
                bytes: bytes.clone(),
                pinned: false,
                held: false,
                used,
            },
        );
        state.evict();
        Ok(Local::Found(bytes))
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
                    let mut fetching = Vec::new();
                    for request in inputs.take_missing_chunks() {
                        match self.get(&request)? {
                            Local::Found(bytes) => {
                                read(bytes.len());
                                inputs.install_verified_chunk(request, bytes)
                            }
                            Local::Absent => absent.push(request),
                            Local::Fetching => fetching.push(request),
                        }
                    }
                    if !absent.is_empty() || !fetching.is_empty() {
                        let mut state = self.state.lock().expect("chunk cache");
                        state.demand.extend(absent);
                        state.fetch.extend(fetching);
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
            Logical::Chunked(value) => self.value_bytes(&value),
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

    /// The whole logical value of a descriptor.
    pub(crate) fn value_bytes(&self, value: &LargeValueRef) -> Result<Vec<u8>, Error> {
        #[cfg(feature = "testing")]
        FULL_MATERIALIZATIONS.with(|count| count.set(count.get() + 1));
        self.run(|inputs| lv::materialize_attempt(value, inputs))
    }

    /// The sort prefix of a chunked value, read from its first chunks.
    pub(crate) fn prefix(&self, value: &LargeValueRef) -> Result<Vec<u8>, Error> {
        // Three bytes past the prefix let text back up to a boundary.
        let end = value.byte_length.min(PREFIX_BYTES as u64 + 3);
        let head = self.range(&Logical::Chunked(Box::new(value.clone())), 0..end)?;
        Ok(sort_prefix(value.kind, &head))
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
            .filter(|(_, node)| !node.pinned && !node.held)
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

/// The distinct chunked cells of `table.column`, current and history.
fn chunked_cells(conn: &Connection, table: &str, column: &str) -> Result<Vec<Vec<u8>>, Error> {
    let cell = quote(column);
    let mut out = Vec::new();
    for target in [table.to_owned(), format!("{table}__h")] {
        let sql = format!(
            "SELECT CAST({cell} AS BLOB) FROM {} WHERE {}",
            quote(&target),
            chunked_sql(&cell)
        );
        let mut statement = conn.prepare_cached(&sql).map_err(storage)?;
        let rows = statement
            .query_map([], |r| r.get::<_, Vec<u8>>(0))
            .map_err(storage)?;
        for row in rows {
            out.push(row.map_err(storage)?);
        }
    }
    out.sort();
    out.dedup();
    Ok(out)
}

/// The sort key a chunked cell carries.
fn cell_prefix(cell: &[u8]) -> Result<&[u8], Error> {
    let descriptor = chunked_descriptor(cell)?;
    let end = cell.len() - descriptor.len();
    Ok(&cell[PREFIX_AT - 1..end])
}

/// The logical bytes a cell carries itself: an inline cell's value, or a
/// chunked cell's sort prefix, both in inline-cell form.
fn head(cell: &[u8]) -> Result<&[u8], Error> {
    if cell.first() == Some(&CHUNKED) {
        cell_prefix(cell)
    } else {
        Ok(cell)
    }
}

/// `cell op literal` decided without reading chunks, when the bytes both
/// carry differ or (for equality) their lengths do.
fn shortcut(
    cell: &[u8],
    literal: &[u8],
    op: &crate::tools::CmpOp,
) -> Result<Option<Ordering>, Error> {
    use crate::tools::CmpOp;
    let (a, b) = (head(cell)?, head(literal)?);
    let n = a.len().min(b.len());
    if a[..n] != b[..n] {
        return Ok(Some(a[..n].cmp(&b[..n])));
    }
    let (len_a, len_b) = (logical(cell)?.len(), logical(literal)?.len());
    if matches!(op, CmpOp::Eq | CmpOp::Ne) && len_a != len_b {
        return Ok(Some(len_a.cmp(&len_b)));
    }
    Ok(None)
}

/// Orders `cells` that agree on their first `from` logical bytes into runs of
/// equal values, reading each cell's next window once per level instead of
/// once per comparison.
fn order_runs(
    chunks: &Chunks,
    cells: Vec<Vec<u8>>,
    from: u64,
    runs: &mut Vec<Vec<Vec<u8>>>,
) -> Result<(), Error> {
    if cells.len() < 2 {
        runs.push(cells);
        return Ok(());
    }
    let to = from + from.max(8 * 1024);
    let mut keyed = Vec::with_capacity(cells.len());
    for cell in cells {
        let value = logical(&cell)?;
        let len = value.len();
        let piece = chunks.range(&value, from.min(len)..to.min(len))?;
        keyed.push((piece, len <= to, cell));
    }
    // A value that ends inside the window is less than one that agrees on it
    // and goes on.
    keyed.sort_by(|a, b| a.0.cmp(&b.0).then(b.1.cmp(&a.1)));
    let mut keyed = keyed.into_iter().peekable();
    while let Some((piece, ended, cell)) = keyed.next() {
        let mut run = vec![cell];
        while let Some(next) = keyed.next_if(|next| next.0 == piece && next.1 == ended) {
            run.push(next.2);
        }
        if ended {
            runs.push(run);
        } else {
            order_runs(chunks, run, to, runs)?;
        }
    }
    Ok(())
}

/// Answer one prep into `_jazz_lv_scratch`. Returns whether a rank prep
/// found chunked values that share a sort key.
pub(crate) fn answer(
    conn: &Connection,
    chunks: &Chunks,
    prep: &crate::compile::Prep,
) -> Result<bool, Error> {
    use crate::compile::{MatchTest, Prep};
    use crate::tools::CmpOp;
    let mut insert = conn
        .prepare_cached(
            "INSERT OR REPLACE INTO _jazz_lv_scratch (slot, cell, v) VALUES (?1, ?2, ?3)",
        )
        .map_err(storage)?;
    let slot = prep.slot();
    match prep {
        Prep::Match {
            table,
            column,
            test,
            ..
        } => {
            for cell in chunked_cells(conn, table, column)? {
                let pass = match test {
                    MatchTest::Contains(needle) => chunks.contains(&cell, needle)?,
                    MatchTest::Cmp(op, literal) => {
                        let ordering = shortcut(&cell, literal, op)?
                            .map_or_else(|| chunks.compare(&cell, literal), Ok)?;
                        match op {
                            CmpOp::Eq => ordering.is_eq(),
                            CmpOp::Ne => ordering.is_ne(),
                            CmpOp::Lt => ordering.is_lt(),
                            CmpOp::Le => ordering.is_le(),
                            CmpOp::Gt => ordering.is_gt(),
                            CmpOp::Ge => ordering.is_ge(),
                        }
                    }
                };
                if pass {
                    insert
                        .execute(rusqlite::params![slot, cell, Option::<Vec<u8>>::None])
                        .map_err(storage)?;
                }
            }
            Ok(false)
        }
        Prep::Rank { table, column, .. } => {
            let mut groups: HashMap<Vec<u8>, Vec<Vec<u8>>> = HashMap::new();
            for cell in chunked_cells(conn, table, column)? {
                groups
                    .entry(cell_prefix(&cell)?.to_vec())
                    .or_default()
                    .push(cell);
            }
            let mut ties = false;
            for (_, cells) in groups {
                if cells.len() < 2 {
                    continue;
                }
                ties = true;
                let mut runs = Vec::new();
                order_runs(chunks, cells, 0, &mut runs)?;
                // Cells of one logical value share a rank.
                for (rank, run) in runs.iter().enumerate() {
                    for cell in run {
                        insert
                            .execute(rusqlite::params![slot, cell, rank as i64])
                            .map_err(storage)?;
                    }
                }
            }
            Ok(ties)
        }
        Prep::Class { columns, .. } => {
            for (table, column) in columns {
                for cell in chunked_cells(conn, table, column)? {
                    let class = blake3::hash(&chunks.materialize(&cell)?);
                    insert
                        .execute(rusqlite::params![slot, cell, class.as_bytes().to_vec()])
                        .map_err(storage)?;
                }
            }
            Ok(false)
        }
    }
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
        store_chunk(conn, chunks, true, chunk)?;
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

pub(crate) fn mark_unpushed(conn: &Connection, locator: &lv::Locator) -> Result<(), Error> {
    conn.prepare_cached("INSERT OR IGNORE INTO _jazz_lv_unpushed (locator) VALUES (?1)")
        .and_then(|mut statement| statement.execute([locator.as_bytes().as_slice()]))
        .map_err(storage)?;
    Ok(())
}

/// Store one node, caching it pinned when `cache` says so. A new branch
/// records an edge to each child, whether or not the child is stored yet;
/// a new leaf is queued for the chunk store, if there is one.
/// The caller verified `chunk` against its hash (or produced it).
fn store_chunk(
    conn: &Connection,
    chunks: &Chunks,
    cache: bool,
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
    if cache {
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
    let node = lv::decode_node_untyped_verified(&chunk.encoded).map_err(corrupt)?;
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
    } else if chunks.offload_store().is_some() {
        // Stored again after it was reclaimed: the chunk store keeps it.
        conn.prepare_cached("DELETE FROM _jazz_lv_offload_delete WHERE locator = ?1")
            .and_then(|mut statement| statement.execute([locator.as_slice()]))
            .and_then(|_| {
                conn.prepare_cached(
                    "INSERT OR IGNORE INTO _jazz_lv_offload (locator, state) VALUES (?1, 0)",
                )
            })
            .and_then(|mut statement| statement.execute([locator.as_slice()]))
            .map_err(storage)?;
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
    let cache = !conn.is_autocommit() || chunks.in_memory();
    crate::values::in_savepoint(conn, "jazz_lv_upload", || {
        staged.iter().try_for_each(|chunk| {
            store_chunk(conn, chunks, cache, chunk)?;
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
        })
    })
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

/// A stored node's verified bytes, read from the chunk store if they moved
/// there; `None` if this store does not hold the node. A node the chunk
/// store answers only asynchronously is demand for the next fetch, and
/// fails the caller as missing.
pub(crate) fn stored_bytes(
    conn: &Connection,
    chunks: &Chunks,
    request: &ChunkRequest,
) -> Result<Option<Vec<u8>>, Error> {
    let Some((hash, bytes)) = node(conn, &request.locator)? else {
        return Ok(None);
    };
    if hash.as_slice() != request.object_hash {
        return Ok(None);
    }
    if !bytes.is_empty() {
        return Ok(Some(bytes));
    }
    match chunks.get(request)? {
        Local::Found(bytes) => Ok(Some(bytes.to_vec())),
        Local::Absent => Ok(None),
        Local::Fetching => {
            chunks.want(request);
            Err(missing())
        }
    }
}

/// The nodes of `value` this replica wrote and has not pushed upstream.
/// A node the upstream holds was validated there with its whole subtree,
/// so the walk stops at the first pushed node on each path.
pub(crate) fn unpushed(
    conn: &Connection,
    chunks: &Chunks,
    value: &LargeValueRef,
) -> Result<Vec<(ChunkRequest, Vec<u8>)>, Error> {
    let mut out = Vec::new();
    let mut stack = vec![request_of(&value.root)];
    let mut seen = BTreeSet::new();
    while let Some(request) = stack.pop() {
        if !seen.insert(request.locator) {
            continue;
        }
        let pending: bool = conn
            .prepare_cached("SELECT EXISTS (SELECT 1 FROM _jazz_lv_unpushed WHERE locator = ?1)")
            .and_then(|mut statement| {
                statement.query_row([request.locator.as_bytes().as_slice()], |r| r.get(0))
            })
            .map_err(storage)?;
        if !pending {
            continue;
        }
        let Some(bytes) = stored_bytes(conn, chunks, &request)? else {
            continue;
        };
        // Stored nodes were verified on the way in.
        if let ChunkNode::Branch { children, .. } =
            lv::decode_node_untyped_verified(&bytes).map_err(corrupt)?
        {
            stack.extend(children.iter().map(|child| request_of(&child.node_ref)));
        }
        out.push((request, bytes));
    }
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
        chunks,
        true,
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

/// Drop the receipts of the pushed nodes under `root`, which a commit now
/// references: the cell holds the tree from here on. Walks only nodes that
/// still hold a receipt, so an append settles just the nodes it pushed.
pub(crate) fn settle_receipts(conn: &Connection, root: &lv::Locator) -> Result<(), Error> {
    conn.prepare_cached(
        "WITH RECURSIVE pushed (locator) AS ( \
           SELECT locator FROM _jazz_lv_receipt WHERE locator = ?1 \
           UNION SELECT e.child FROM _jazz_lv_edge e JOIN pushed p ON e.parent = p.locator \
           WHERE EXISTS (SELECT 1 FROM _jazz_lv_receipt WHERE locator = e.child)) \
         DELETE FROM _jazz_lv_receipt WHERE locator IN (SELECT locator FROM pushed)",
    )
    .and_then(|mut statement| statement.execute([root.as_bytes().as_slice()]))
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

struct Stored<'a> {
    conn: &'a Connection,
    chunks: &'a Chunks,
    /// A node is only in the chunk store, which answers asynchronously.
    fetching: std::cell::Cell<bool>,
}

impl lv::chunks::ChunkProvider for Stored<'_> {
    fn get(
        &self,
        request: ChunkRequest,
    ) -> lv::chunks::ChunkFuture<'_, Result<Bytes, lv::chunks::ChunkError>> {
        let found = match stored_bytes(self.conn, self.chunks, &request) {
            Ok(Some(bytes)) => {
                lv::record_finalize_validation_bytes(bytes.len());
                Ok(Bytes::from(bytes))
            }
            Ok(None) => Err(lv::chunks::ChunkError::Unavailable),
            Err(error) if is_missing(&error) => {
                self.fetching.set(true);
                Err(lv::chunks::ChunkError::Unavailable)
            }
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
            let stored = Stored {
                conn,
                chunks,
                fetching: std::cell::Cell::new(false),
            };
            let visited =
                crate::options::block_on(lv::visit_reachable_chunks(&base, &stored, |_| {}));
            if stored.fetching.get() {
                return Err(missing());
            }
            visited.map_err(|e| rejected(format!("large value: {e}")))?;
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
    conn.cached_row(
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
            .cached_row(
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
            .cached_row(
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
        if chunks.offload_store().is_some() {
            // A leaf whose bytes are in the chunk store, or may be (a put
            // in flight), is deleted there once this deletion commits.
            let put: bool = conn
                .cached_row(
                    "SELECT EXISTS (SELECT 1 FROM _jazz_lv_offload WHERE locator = ?1 \
                     AND state = 1)",
                    [&locator],
                    |r| r.get(0),
                )
                .map_err(storage)?;
            if bytes.is_empty() || put {
                conn.execute(
                    "INSERT OR REPLACE INTO _jazz_lv_offload_delete (locator, hash) \
                     VALUES (?1, ?2)",
                    [&locator, &hash],
                )
                .map_err(storage)?;
            }
            conn.execute(
                "DELETE FROM _jazz_lv_offload WHERE locator = ?1",
                [&locator],
            )
            .map_err(storage)?;
            if bytes.is_empty() {
                // Only leaves are offloaded: no children to release.
                continue;
            }
        }
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
        let chunks = Chunks::install(None, None).unwrap();
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
        chunked_cell(&prepared.value_ref, &sort_prefix(kind, logical)).unwrap()
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
            forms.push(chunked_cell(&appended, &sort_prefix(kind, logical)).unwrap());
        }
        forms
    }

    /// Comparison must be a total order identical to `memcmp` over logical
    /// bytes, and the SQL sort key must agree with it wherever two keys
    /// differ, or indexes and orderings are wrong.
    #[test]
    fn comparison_and_sort_keys_match_the_logical_oracle_across_forms() {
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
            let key = |cell: &[u8]| -> Vec<u8> {
                conn.query_row(
                    &format!("SELECT CAST({} AS BLOB)", key_sql("CAST(?1 AS TEXT)")),
                    [cell],
                    |r| r.get(0),
                )
                .unwrap()
            };
            for (rank_a, a) in &cells {
                for (rank_b, b) in &cells {
                    assert_eq!(
                        chunks.compare(a, b).unwrap(),
                        rank_a.cmp(rank_b),
                        "{kind:?}: values {rank_a} and {rank_b}"
                    );
                    // Stored cells are canonical: chunked exactly when the
                    // value is over the inline threshold.
                    let canonical = |rank: usize, cell: &[u8]| {
                        (logical[rank].len() > lv::INLINE_VALUE_MAX_BYTES)
                            == (cell.first() == Some(&CHUNKED))
                    };
                    if !canonical(*rank_a, a) || !canonical(*rank_b, b) {
                        continue;
                    }
                    let (key_a, key_b) = (key(a), key(b));
                    if key_a != key_b {
                        assert_eq!(
                            key_a.cmp(&key_b),
                            rank_a.cmp(rank_b),
                            "{kind:?}: keys of values {rank_a} and {rank_b}"
                        );
                    }
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
            "CREATE TABLE t (id INTEGER PRIMARY KEY, body);
             CREATE TABLE t__h (id INTEGER, body);",
        )
        .unwrap();
        let table = Table {
            name: "t".to_owned(),
            stored: "t".to_owned(),
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
            stored: "body".to_owned(),
            ty: crate::records::ValueType::String,
            default: None,
            json: false,
            merge: Default::default(),
        };
        conn.execute_batch(&triggers(&table, &[&column], None))
            .unwrap();
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
    fn one_trigger_set_counts_every_column_and_later_sets_add_columns() {
        let (conn, chunks) = chunks();
        conn.execute_batch(
            "CREATE TABLE t (id INTEGER PRIMARY KEY, a, b);
             CREATE TABLE t__h (id INTEGER, a, b);",
        )
        .unwrap();
        let table = Table {
            name: "t".to_owned(),
            stored: "t".to_owned(),
            columns: Vec::new(),
            references: Default::default(),
            policies: Default::default(),
            indexes: Vec::new(),
            branch_by: Vec::new(),
            provenance: Vec::new(),
            stored_provenance: Vec::new(),
        };
        let column = |name: &str| Column {
            name: name.to_owned(),
            stored: name.to_owned(),
            ty: crate::records::ValueType::String,
            default: None,
            json: false,
            merge: Default::default(),
        };
        let (a, b, c) = (column("a"), column("b"), column("c"));
        conn.execute_batch(&triggers(&table, &[&a, &b], None))
            .unwrap();
        let count = |sql: &str| conn.query_row(sql, [], |r| r.get::<_, i64>(0)).unwrap();
        // One insert, delete and update trigger per table and history table.
        assert_eq!(
            count(
                "SELECT count(*) FROM sqlite_master WHERE type = 'trigger' AND tbl_name IN ('t', 't__h')"
            ),
            6
        );
        let cell = |seed| {
            crate::values::Sql::Text(chunked(
                &conn,
                &chunks,
                LargeValueKind::String,
                &text(seed, 300_000),
            ))
        };
        let (one, two, three) = (cell(7), cell(8), cell(9));
        let durable = |cell: &crate::values::Sql| {
            let crate::values::Sql::Text(bytes) = cell else {
                unreachable!()
            };
            conn.query_row(
                &format!(
                    "SELECT coalesce(sum(durable), 0) FROM _jazz_lv_root WHERE root = {}",
                    root_sql("?1")
                ),
                [bytes],
                |r| r.get::<_, i64>(0),
            )
            .unwrap()
        };
        conn.execute("INSERT INTO t (id, a, b) VALUES (1, ?1, ?2)", [&one, &two])
            .unwrap();
        assert_eq!((durable(&one), durable(&two)), (1, 1));
        // Swapping one column's tree releases the old and retains the new;
        // an unchanged column keeps its count.
        conn.execute("UPDATE t SET a = ?1, b = b WHERE id = 1", [&three])
            .unwrap();
        assert_eq!((durable(&one), durable(&two), durable(&three)), (0, 1, 1));
        // A column added later gets its own set beside the first.
        conn.execute_batch("ALTER TABLE t ADD COLUMN c; ALTER TABLE t__h ADD COLUMN c;")
            .unwrap();
        conn.execute_batch(&triggers(&table, &[&c], Some("c")))
            .unwrap();
        assert_eq!(
            count(
                "SELECT count(*) FROM sqlite_master WHERE type = 'trigger' AND tbl_name IN ('t', 't__h')"
            ),
            12
        );
        conn.execute("UPDATE t SET c = ?1 WHERE id = 1", [&one])
            .unwrap();
        assert_eq!((durable(&one), durable(&two), durable(&three)), (1, 1, 1));
        conn.execute("DELETE FROM t WHERE id = 1", []).unwrap();
        assert_eq!((durable(&one), durable(&two), durable(&three)), (0, 0, 0));
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
    fn a_commit_settles_the_receipts_of_the_tree_it_references() {
        let (conn, chunks) = chunks();
        let receipts = || {
            conn.query_row("SELECT count(*) FROM _jazz_lv_receipt", [], |r| {
                r.get::<_, i64>(0)
            })
            .unwrap()
        };
        let push = |built: &lv::PreparedLargeValue| {
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
        };
        let referenced = lv::prepare(LargeValueKind::String, &text(11, 300_000)).unwrap();
        let pending = lv::prepare(LargeValueKind::String, &text(12, 300_000)).unwrap();
        push(&referenced);
        push(&pending);
        let pending_nodes = pending.staged_chunks.len() as i64;
        assert!(receipts() > pending_nodes);
        settle_receipts(&conn, &referenced.value_ref.root.locator).unwrap();
        // Only the other push, still ahead of its commit, keeps receipts,
        // so expiring them later touches nothing the commit holds.
        assert_eq!(receipts(), pending_nodes);
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

    /// A file store with a directory chunk store, and a table `t` whose
    /// `body` cells are counted against their roots.
    struct Offloading {
        dir: std::path::PathBuf,
        path: std::path::PathBuf,
        conn: Connection,
        chunks: Arc<Chunks>,
        store: Arc<dyn ChunkStore>,
    }

    impl Drop for Offloading {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.dir);
        }
    }

    fn offloading(name: &str) -> Offloading {
        let dir =
            std::env::temp_dir().join(format!("jazz-lv-offload-{}-{name}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("store.sqlite");
        let conn = Connection::open(&path).unwrap();
        let store: Arc<dyn ChunkStore> =
            Arc::new(crate::chunk_store::FileChunkStore::new(dir.join("chunks")).unwrap());
        let chunks = Chunks::install(Some((&path, None)), Some(store.clone())).unwrap();
        conn.execute_batch(DDL).unwrap();
        open_offload(&conn, &chunks).unwrap();
        conn.execute_batch(
            "CREATE TABLE t (id INTEGER PRIMARY KEY, body);
             CREATE TABLE t__h (id INTEGER, body);",
        )
        .unwrap();
        let table = Table {
            name: "t".to_owned(),
            stored: "t".to_owned(),
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
            stored: "body".to_owned(),
            ty: crate::records::ValueType::String,
            default: None,
            json: false,
            merge: Default::default(),
        };
        conn.execute_batch(&triggers(&table, &[&column], None))
            .unwrap();
        Offloading {
            dir,
            path,
            conn,
            chunks,
            store,
        }
    }

    impl Offloading {
        /// Store a chunked value in row `id`.
        fn insert(&self, id: i64, prepared: &lv::PreparedLargeValue, logical: &[u8]) -> Vec<u8> {
            store(&self.conn, &self.chunks, prepared, false).unwrap();
            let cell = chunked_cell(
                &prepared.value_ref,
                &sort_prefix(LargeValueKind::String, logical),
            )
            .unwrap();
            self.conn
                .execute(
                    "INSERT INTO t (id, body) VALUES (?1, ?2)",
                    rusqlite::params![id, crate::values::Sql::Text(cell.clone())],
                )
                .unwrap();
            cell
        }

        fn put(&self, batch: &[(NodeKey, Vec<u8>)]) {
            for (key, bytes) in batch {
                crate::options::block_on(self.store.put(key, bytes)).unwrap();
            }
        }

        fn drop_bytes(&self, batch: &[(NodeKey, Vec<u8>)]) {
            for (key, _) in batch {
                offloaded(&self.conn, key).unwrap();
            }
        }

        fn delete_pending(&self) -> Vec<NodeKey> {
            let pending = pending_deletes(&self.conn, 1_000).unwrap();
            for key in &pending {
                crate::options::block_on(self.store.delete(key)).unwrap();
                deleted(&self.conn, key).unwrap();
            }
            pending
        }

        fn stored(&self, key: &NodeKey) -> bool {
            crate::options::block_on(self.store.get(key))
                .unwrap()
                .is_some()
        }

        fn count(&self, sql: &str) -> i64 {
            self.conn.query_row(sql, [], |r| r.get(0)).unwrap()
        }

        fn reclaim_all(&self) {
            while reclaim(&self.conn, &self.chunks, 1_000).unwrap() > 0 {}
        }
    }

    fn keys(batch: &[(NodeKey, Vec<u8>)]) -> BTreeSet<NodeKey> {
        batch.iter().map(|(key, _)| *key).collect()
    }

    // The tests below are white-box: the windows between a SQLite commit
    // and a chunk-store operation (a put in flight while its node is
    // reclaimed, a node stored again while its delete waits, a crash
    // between a put and dropping the bytes) cannot be reached
    // deterministically through `Db`.

    /// Only leaves move; a moved leaf reads back from the chunk store into a
    /// cold cache; a reclaimed one is deleted there only after the
    /// reclamation, and nothing else is.
    #[test]
    fn moved_leaves_read_back_and_are_deleted_after_their_reclamation() {
        let o = offloading("read-back");
        let logical = text(11, 400_000);
        let prepared = lv::prepare(LargeValueKind::String, &logical).unwrap();
        let cell = o.insert(1, &prepared, &logical);
        let batch = begin_offload(&o.conn, 1_000).unwrap();
        let leaves = o.count(
            "SELECT count(*) FROM _jazz_lv_node n WHERE NOT EXISTS \
             (SELECT 1 FROM _jazz_lv_edge WHERE parent = n.locator)",
        );
        assert_eq!(batch.len() as i64, leaves, "every leaf, no branch");
        assert!(leaves > 4);
        o.put(&batch);
        o.drop_bytes(&batch);
        assert_eq!(
            o.count("SELECT count(*) FROM _jazz_lv_node WHERE bytes = X''"),
            leaves
        );
        assert_eq!(o.count("SELECT count(*) FROM _jazz_lv_offload"), 0);
        let cold = Chunks::install(Some((&o.path, None)), Some(o.store.clone())).unwrap();
        assert_eq!(cold.materialize(&cell).unwrap(), logical);
        assert!(cold.take_demand().is_empty());
        // A second value keeps its leaves when the first goes.
        let other = text(12, 300_000);
        let kept = lv::prepare(LargeValueKind::String, &other).unwrap();
        o.insert(2, &kept, &other);
        o.conn.execute("DELETE FROM t WHERE id = 1", []).unwrap();
        o.reclaim_all();
        assert!(batch.iter().all(|(key, _)| o.stored(key)), "not before");
        let deleted: BTreeSet<NodeKey> = o.delete_pending().into_iter().collect();
        assert_eq!(deleted, keys(&batch));
        assert!(batch.iter().all(|(key, _)| !o.stored(key)));
        assert!(
            o.count("SELECT count(*) FROM _jazz_lv_node") > 0,
            "the other value stays"
        );
    }

    /// A leaf reclaimed while its put is in flight: the put still lands,
    /// dropping the bytes finds no node, and the queued delete removes
    /// what the put wrote.
    #[test]
    fn a_leaf_reclaimed_mid_put_is_deleted_after_the_put_lands() {
        let o = offloading("mid-put");
        let logical = text(13, 300_000);
        let prepared = lv::prepare(LargeValueKind::String, &logical).unwrap();
        o.insert(1, &prepared, &logical);
        let batch = begin_offload(&o.conn, 1_000).unwrap();
        o.conn.execute("DELETE FROM t WHERE id = 1", []).unwrap();
        o.reclaim_all();
        assert_eq!(o.count("SELECT count(*) FROM _jazz_lv_node"), 0);
        o.put(&batch);
        o.drop_bytes(&batch);
        let deleted: BTreeSet<NodeKey> = o.delete_pending().into_iter().collect();
        assert_eq!(deleted, keys(&batch));
        assert!(batch.iter().all(|(key, _)| !o.stored(key)));
    }

    /// A crash after a put but before its bytes were dropped: the leaf is
    /// still marked, so the next pass puts it again (puts are idempotent).
    #[test]
    fn a_put_interrupted_before_the_bytes_drop_is_put_again() {
        let o = offloading("crash");
        let logical = text(14, 300_000);
        let prepared = lv::prepare(LargeValueKind::String, &logical).unwrap();
        o.insert(1, &prepared, &logical);
        let first = begin_offload(&o.conn, 1_000).unwrap();
        o.put(&first);
        let again = begin_offload(&o.conn, 1_000).unwrap();
        assert_eq!(keys(&again), keys(&first));
        o.put(&again);
        o.drop_bytes(&again);
        assert_eq!(o.count("SELECT count(*) FROM _jazz_lv_offload"), 0);
    }

    /// A reclaimed leaf stored again (a replica fetching it back, a client
    /// pushing it again) before its delete ran keeps its new bytes: the
    /// delete is cancelled and the leaf is queued to move again.
    #[test]
    fn a_leaf_stored_again_while_its_delete_waits_is_kept() {
        let o = offloading("restored");
        let logical = text(15, 300_000);
        let prepared = lv::prepare(LargeValueKind::String, &logical).unwrap();
        o.insert(1, &prepared, &logical);
        let batch = begin_offload(&o.conn, 1_000).unwrap();
        o.put(&batch);
        o.drop_bytes(&batch);
        o.conn.execute("DELETE FROM t WHERE id = 1", []).unwrap();
        o.reclaim_all();
        assert_eq!(
            o.count("SELECT count(*) FROM _jazz_lv_offload_delete"),
            batch.len() as i64
        );
        let cell = o.insert(2, &prepared, &logical);
        assert!(o.delete_pending().is_empty());
        assert_eq!(keys(&begin_offload(&o.conn, 1_000).unwrap()), keys(&batch));
        let cold = Chunks::install(Some((&o.path, None)), Some(o.store.clone())).unwrap();
        assert_eq!(cold.materialize(&cell).unwrap(), logical);
    }

    /// Leaves still held by an upload lease, a receipt or an unpushed mark
    /// stay in the file until they settle.
    #[test]
    fn leaves_still_being_staged_or_pushed_stay_in_the_file() {
        let o = offloading("held");
        let logical = text(16, 300_000);
        let prepared = lv::prepare(LargeValueKind::String, &logical).unwrap();
        begin_upload(&o.conn, &[7; 32], 1_000).unwrap();
        stage_upload(&o.conn, &o.chunks, &[7; 32], &prepared.staged_chunks, true).unwrap();
        assert!(begin_offload(&o.conn, 1_000).unwrap().is_empty());
        end_upload(&o.conn, &[7; 32]).unwrap();
        assert!(
            begin_offload(&o.conn, 1_000).unwrap().is_empty(),
            "unpushed"
        );
        let locators: Vec<lv::Locator> = prepared
            .staged_chunks
            .iter()
            .map(|chunk| chunk.node_ref.locator)
            .collect();
        mark_pushed(&o.conn, &locators).unwrap();
        assert!(!begin_offload(&o.conn, 1_000).unwrap().is_empty());
    }
}
