//! Chunked large values in SQLite cells (SPEC §19).
//!
//! `string`, `json` and `bytes` columns use the large-value format of
//! `jazz_types::large_values`. Each cell keeps its SQLite storage class:
//!
//! - an inline string or JSON value is TEXT, its UTF-8 bytes;
//! - an inline bytes value is a BLOB, its bytes;
//! - a chunked value (above the inline threshold) is an INTEGER,
//!   `epoch * EPOCH_CELL_SPAN + n`: the tree of `_jazz_lv_epoch` row
//!   `epoch` with that epoch's first `n` edits applied.
//!
//! `typeof(cell) = 'integer'` is the chunked arm, so the schema, the
//! refcount triggers and every compiled query use only SQL built-ins and
//! run on any SQLite (vanilla builds such as Cloudflare D1 or Durable
//! Objects included): no collation, no functions. Queries never read a
//! chunked value's bytes: such a cell equals only itself, takes part in no
//! range or `contains`, and sorts before every inline value (SPEC 19.5).
//! Epochs and chunks live in the same
//! database, so a row, its epoch and the chunks they reference commit in
//! one transaction; reference counts are kept by triggers on every table
//! that has a large-capable column. With a chunk store
//! ([`crate::chunk_store`]) the bytes of settled leaves move out of the
//! file on tick; their metadata stays.

use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::sync::{Arc, Mutex};

use bytes::Bytes;
use jazz_sql::Cell as SqlCell;
use jazz_types::large_values::{
    self as lv, AttemptError, ChunkInputs, ChunkNode, LargeValueKind, LargeValueRef,
    chunks::ChunkRequest, epoch,
};
#[cfg(all(test, feature = "sqlite"))]
use rusqlite::Connection;

use crate::chunk_store::{ChunkStore, NodeKey};
use crate::options::{Error, ErrorCode, Reason};
use crate::values::{Column, Sql, Table, quote};

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

/// Cells of one epoch: a chunked cell is `epoch * SPAN + n`.
const SPAN: i64 = epoch::EPOCH_CELL_SPAN;
/// Unpinned epochs a file store keeps cached.
const CACHE_EPOCHS: usize = 4096;

/// Logical bytes read per step when a value is walked window by window.
const WINDOW: u64 = 64 * 1024;
/// Verified chunk bytes a file store keeps cached, beyond those pinned by
/// the open transaction.
const CACHE_BYTES: usize = 64 * 1024 * 1024;

/// The chunk tables. Nodes are keyed by their object hash, and each read
/// verifies a node against the hash its parent or descriptor names. A branch's
/// edges to its children are kept apart from the children themselves, so
/// nodes may arrive in any order (a replica fetches parents first, an
/// upload may send leaves first) and a node is held while any stored
/// branch, epoch, pushed-node receipt or local upload lease names it.
/// A root's `base` is the descriptor of its tree (no edit tail) once this
/// store built or validated it; later descriptors over the same root are
/// checked against it instead of walking the tree again; `refs` counts
/// the epochs over it.
/// `_jazz_lv_epoch` holds a tree (`root`, and `base`, its descriptor) and
/// the edits applied on top of it (`edits`, `count` of them, encoded as
/// [`epoch`] does); `refs` counts the cells naming it. Edits are only ever
/// appended, so a cell's value never changes. An epoch no cell names is
/// deleted, releasing its root.
/// `_jazz_lv_receipt` holds nodes a session (`author`, its identity) pushed
/// ahead of the commit that references them, until that commit lands or
/// `expires` (ms); a receipt is the pusher's proof that it holds the node; `_jazz_lv_unpushed` holds nodes
/// this replica wrote that its upstream may not have yet; `_jazz_lv_upload`
/// leases a local streaming upload's nodes (`_jazz_lv_upload_node`) until
/// its row commits, it is aborted, or it goes untouched past the staging
/// policy's maximum age. `_jazz_lv_via` names, for a root a replica
/// received, the row version (`tbl` as the wire names it, `id`, `key`,
/// `seq`) its pulls cite: the upstream answers a pull only for nodes of a
/// version the reader may read (SPEC 19.8).
pub(crate) const DDL: &str = "\
CREATE TABLE IF NOT EXISTS _jazz_lv_node (hash BLOB PRIMARY KEY, bytes BLOB NOT NULL) \
 WITHOUT ROWID;
CREATE TABLE IF NOT EXISTS _jazz_lv_edge (child BLOB NOT NULL, parent BLOB NOT NULL, \
 PRIMARY KEY (child, parent)) WITHOUT ROWID;
CREATE INDEX IF NOT EXISTS _jazz_lv_edge_parent ON _jazz_lv_edge (parent);
CREATE TABLE IF NOT EXISTS _jazz_lv_root (root BLOB PRIMARY KEY, refs INTEGER NOT NULL \
 DEFAULT 0, base BLOB) WITHOUT ROWID;
CREATE TABLE IF NOT EXISTS _jazz_lv_epoch (id INTEGER PRIMARY KEY, root BLOB NOT NULL, \
 base BLOB NOT NULL, edits BLOB NOT NULL, count INTEGER NOT NULL, \
 refs INTEGER NOT NULL DEFAULT 0);
CREATE INDEX IF NOT EXISTS _jazz_lv_epoch_root ON _jazz_lv_epoch (root);
CREATE TABLE IF NOT EXISTS _jazz_lv_receipt (hash BLOB NOT NULL, author BLOB NOT NULL, \
 expires INTEGER NOT NULL, PRIMARY KEY (hash, author)) WITHOUT ROWID;
CREATE INDEX IF NOT EXISTS _jazz_lv_receipt_expires ON _jazz_lv_receipt (expires);
CREATE TABLE IF NOT EXISTS _jazz_lv_unpushed (hash BLOB PRIMARY KEY) WITHOUT ROWID;
CREATE TABLE IF NOT EXISTS _jazz_lv_upload (id BLOB PRIMARY KEY, touched INTEGER NOT NULL) \
 WITHOUT ROWID;
CREATE TABLE IF NOT EXISTS _jazz_lv_upload_node (id BLOB NOT NULL, hash BLOB NOT NULL, \
 PRIMARY KEY (id, hash)) WITHOUT ROWID;
CREATE INDEX IF NOT EXISTS _jazz_lv_upload_node_hash ON _jazz_lv_upload_node (hash);
CREATE TABLE IF NOT EXISTS _jazz_lv_reclaim (root BLOB PRIMARY KEY) WITHOUT ROWID;
CREATE TABLE IF NOT EXISTS _jazz_lv_via (root BLOB PRIMARY KEY, tbl TEXT NOT NULL, \
 id BLOB NOT NULL, key BLOB NOT NULL, t INTEGER NOT NULL) WITHOUT ROWID;
CREATE TRIGGER IF NOT EXISTS _jazz_lv_root_unreferenced AFTER UPDATE OF refs ON _jazz_lv_root \
 WHEN NEW.refs = 0 BEGIN \
 INSERT INTO _jazz_lv_reclaim (root) SELECT NEW.root \
 WHERE NOT EXISTS (SELECT 1 FROM _jazz_lv_reclaim WHERE root = NEW.root); END;
CREATE TRIGGER IF NOT EXISTS _jazz_lv_epoch_ins AFTER INSERT ON _jazz_lv_epoch BEGIN \
 INSERT INTO _jazz_lv_root (root, refs) VALUES (NEW.root, 1) \
 ON CONFLICT (root) DO UPDATE SET refs = refs + 1; END;
CREATE TRIGGER IF NOT EXISTS _jazz_lv_epoch_del AFTER DELETE ON _jazz_lv_epoch BEGIN \
 UPDATE _jazz_lv_root SET refs = refs - 1 WHERE root = OLD.root; END;
CREATE TRIGGER IF NOT EXISTS _jazz_lv_epoch_free AFTER UPDATE OF refs ON _jazz_lv_epoch \
 WHEN NEW.refs <= 0 BEGIN DELETE FROM _jazz_lv_epoch WHERE id = NEW.id; END;
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
CREATE TABLE IF NOT EXISTS _jazz_lv_offload (hash BLOB PRIMARY KEY, state INTEGER NOT NULL) \
 WITHOUT ROWID;
CREATE TABLE IF NOT EXISTS _jazz_lv_offload_delete (hash BLOB PRIMARY KEY) WITHOUT ROWID;
";

/// Prepare a store for its chunk store, or refuse to open a store that
/// has one without it. The first open with a chunk store queues every leaf
/// the file already holds. The caller owns the enclosing transaction.
pub(crate) async fn open_offload<S: jazz_sql::Sql>(
    conn: &mut S,
    chunks: &Chunks,
) -> Result<(), Error>
where
    S::Error: std::fmt::Display,
{
    use jazz_sql::Row as _;
    let exists: bool = conn.query_one(
        "SELECT EXISTS (SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = '_jazz_lv_offload')",
        jazz_sql::params![], |r| r.get(0),
    ).await.map_err(crate::values::provider_error)?;
    match (exists, chunks.offload_store().is_some()) {
        (true, false) => Err(unconfigured()),
        (false, false) | (true, true) => Ok(()),
        (false, true) => {
            conn.execute_script(OFFLOAD_DDL)
                .await
                .map_err(crate::values::provider_error)?;
            conn.execute(
                "INSERT INTO _jazz_lv_offload (hash, state) SELECT n.hash, 0 FROM _jazz_lv_node n WHERE NOT EXISTS (SELECT 1 FROM _jazz_lv_edge e WHERE e.parent = n.hash)",
                jazz_sql::params![],
            ).await.map_err(crate::values::provider_error)?;
            Ok(())
        }
    }
}

/// Up to `limit` queued leaves that are settled (no unpushed upload,
/// receipt or upload lease holds them), marked as being put. Their bytes
/// must reach the chunk store before [`offloaded`] drops them here.
/// Selection and marking must share a caller-owned transaction.
pub(crate) async fn begin_offload<S: jazz_sql::Sql>(
    conn: &mut S,
    limit: usize,
) -> Result<Vec<(NodeKey, Vec<u8>)>, Error>
where
    S::Error: std::fmt::Display,
{
    use jazz_sql::Row as _;
    let picked: Vec<(Vec<u8>, Vec<u8>)> = conn.query_rows(
        "SELECT o.hash, n.bytes FROM _jazz_lv_offload o JOIN _jazz_lv_node n ON n.hash = o.hash \
         WHERE NOT EXISTS (SELECT 1 FROM _jazz_lv_unpushed WHERE hash = o.hash) \
         AND NOT EXISTS (SELECT 1 FROM _jazz_lv_receipt WHERE hash = o.hash) \
         AND NOT EXISTS (SELECT 1 FROM _jazz_lv_upload_node WHERE hash = o.hash) LIMIT ?1",
        jazz_sql::params![limit as i64], |r| Ok((r.get(0)?, r.get(1)?)),
    ).await.map_err(crate::values::provider_error)?;
    let mut out = Vec::with_capacity(picked.len());
    for (hash, bytes) in picked {
        conn.execute(
            "UPDATE _jazz_lv_offload SET state = 1 WHERE hash = ?1",
            jazz_sql::params![hash],
        )
        .await
        .map_err(crate::values::provider_error)?;
        out.push((key(&hash)?, bytes));
    }
    Ok(out)
}

/// A leaf's bytes reached the chunk store: drop them here, unless it was
/// reclaimed (and perhaps stored again) while the put ran.
pub(crate) async fn offloaded<S: jazz_sql::Sql>(conn: &mut S, node: &NodeKey) -> Result<(), Error>
where
    S::Error: std::fmt::Display,
{
    let hash = node.hash.as_slice();
    conn.execute("UPDATE _jazz_lv_node SET bytes = X'' WHERE hash = ?1 AND EXISTS (SELECT 1 FROM _jazz_lv_offload WHERE hash = ?1 AND state = 1)", jazz_sql::params![hash]).await.map_err(crate::values::provider_error)?;
    conn.execute(
        "DELETE FROM _jazz_lv_offload WHERE hash = ?1 AND state = 1",
        jazz_sql::params![hash],
    )
    .await
    .map_err(crate::values::provider_error)?;
    Ok(())
}

/// Up to `limit` reclaimed leaves to delete from the chunk store.
pub(crate) async fn pending_deletes<S: jazz_sql::Sql>(
    conn: &mut S,
    limit: usize,
) -> Result<Vec<NodeKey>, Error>
where
    S::Error: std::fmt::Display,
{
    use jazz_sql::Row as _;
    let rows: Vec<Vec<u8>> = conn
        .query_rows(
            "SELECT hash FROM _jazz_lv_offload_delete LIMIT ?1",
            jazz_sql::params![limit as i64],
            |r| r.get(0),
        )
        .await
        .map_err(crate::values::provider_error)?;
    rows.iter().map(|hash| key(hash)).collect()
}

/// A reclaimed leaf is gone from the chunk store.
pub(crate) async fn deleted<S: jazz_sql::Sql>(conn: &mut S, node: &NodeKey) -> Result<(), Error>
where
    S::Error: std::fmt::Display,
{
    conn.execute(
        "DELETE FROM _jazz_lv_offload_delete WHERE hash = ?1",
        jazz_sql::params![node.hash.as_slice()],
    )
    .await
    .map_err(crate::values::provider_error)?;
    Ok(())
}

fn key(hash: &[u8]) -> Result<NodeKey, Error> {
    Ok(NodeKey {
        hash: hash
            .try_into()
            .map_err(|_| corrupt(lv::Error::MalformedNode))?,
    })
}

/// Whether `ty` (or the type a nullable wraps) is stored by this module.
/// An enum column never is: it stores tags, and a name is never long (it
/// is one of the column's variants).
pub(crate) fn large_capable(column: &Column) -> bool {
    use crate::records::ValueType;
    if column.is_enum() {
        return false;
    }
    let mut ty = &column.ty;
    while let ValueType::Nullable(inner) = ty {
        ty = inner;
    }
    matches!(ty, ValueType::String | ValueType::Bytes)
}

/// The most logical bytes a value of `column` keeps inline; a longer one
/// is chunked. Text-merge columns chunk early
/// ([`lv::TEXT_MERGE_INLINE_MAX_BYTES`]), every other column at
/// [`lv::INLINE_VALUE_MAX_BYTES`].
pub(crate) fn inline_max(column: &Column) -> usize {
    if column.merge.text_format().is_some() {
        lv::TEXT_MERGE_INLINE_MAX_BYTES
    } else {
        lv::INLINE_VALUE_MAX_BYTES
    }
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
/// columns, in the current and history tables, against its epoch. Plain
/// SQL: a chunked cell is an integer naming its epoch. They are also how a
/// reopened store knows which stored columns are large-capable
/// ([`stored_large_columns`]).
///
/// One trigger per event and table covers every column: each schema
/// object costs SQLite a scan of `sqlite_schema` when it is created, so a
/// trigger per column made opening a wide schema quadratic. A store that
/// gains large-capable columns later adds a set for them, named after the
/// first (`set`), beside the table's earlier ones. An update retains every
/// new epoch before it releases any old one, so an epoch that moves from
/// one column to another is never freed in between.
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
    // Each statement acts only on a chunked cell: a NULL epoch matches no
    // row.
    let count = |cell: &str, by: &str, unless: Option<&str>| {
        let unless = unless.map_or(String::new(), |other| {
            format!(" AND {} IS NOT {}", epoch_sql(cell), epoch_sql(other))
        });
        format!(
            "UPDATE _jazz_lv_epoch SET refs = refs {by} 1 WHERE id = {}{unless};",
            epoch_sql(cell)
        )
    };
    for target in [table.stored.clone(), format!("{}__h", table.stored)] {
        let on = quote(&target);
        let trigger = |what: &str| match set {
            None => quote(&format!("{target}__lv_{what}")),
            Some(set) => quote(&format!("{target}__lv_{what}_{set}")),
        };
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
             CREATE TRIGGER IF NOT EXISTS {upd} AFTER UPDATE OF {} ON {on} WHEN {} BEGIN {} {} END;\n",
            any(&|name| chunked_sql(&format!("NEW.{name}"))),
            each(&|name| count(&format!("NEW.{name}"), "+", None)),
            any(&|name| chunked_sql(&format!("OLD.{name}"))),
            each(&|name| count(&format!("OLD.{name}"), "-", None)),
            names.join(", "),
            any(&|name| format!(
                "{} IS NOT {}",
                epoch_sql(&format!("OLD.{name}")),
                epoch_sql(&format!("NEW.{name}"))
            )),
            each(&|name| count(
                &format!("NEW.{name}"),
                "+",
                Some(&format!("OLD.{name}"))
            )),
            each(&|name| count(
                &format!("OLD.{name}"),
                "-",
                Some(&format!("NEW.{name}"))
            )),
            ins = trigger("ins"),
            del = trigger("del"),
            upd = trigger("upd"),
        ));
    }
    sql
}

/// The stored columns of `table` that the refcount triggers of
/// [`triggers`] watch: its large-capable columns, under every schema
/// version the store has held. Read from the `AFTER UPDATE OF` column
/// list of each `__lv_upd` trigger.
pub(crate) async fn stored_large_columns<S: jazz_sql::Sql>(
    conn: &mut S,
    table: &str,
) -> Result<BTreeSet<String>, Error>
where
    S::Error: std::fmt::Display,
{
    use jazz_sql::Row as _;
    let prefix = format!("{table}__lv_upd");
    let sqls: Vec<(String, String)> = conn
        .query_rows(
            "SELECT name, sql FROM sqlite_master WHERE type = 'trigger' AND tbl_name = ?1",
            jazz_sql::params![table],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .await
        .map_err(crate::values::provider_error)?;
    let mut columns = BTreeSet::new();
    for (name, sql) in sqls {
        if !name.starts_with(&prefix) {
            continue;
        }
        let watched = update_of_columns(&sql).ok_or_else(|| {
            Error::new(
                ErrorCode::Storage,
                format!("large value: unreadable refcount trigger {name}"),
            )
        })?;
        columns.extend(watched);
    }
    Ok(columns)
}

/// The column list of a trigger [`triggers`] wrote:
/// `CREATE TRIGGER [IF NOT EXISTS] "name" AFTER UPDATE OF "a", "b" ON ...`.
/// The quoted name is skipped as a whole, so no name can fake the list.
fn update_of_columns(sql: &str) -> Option<Vec<String>> {
    let rest = sql
        .trim_start()
        .strip_prefix("CREATE TRIGGER ")?
        .trim_start();
    let rest = rest.strip_prefix("IF NOT EXISTS ").unwrap_or(rest);
    let (_name, rest) = quoted(rest)?;
    let mut rest = rest.trim_start().strip_prefix("AFTER UPDATE OF ")?;
    let mut columns = Vec::new();
    loop {
        let (column, after) = quoted(rest)?;
        columns.push(column);
        match after.trim_start().strip_prefix(',') {
            Some(more) => rest = more,
            None => return Some(columns),
        }
    }
}

/// A leading `"..."` identifier (doubled quotes escaped) and what follows.
fn quoted(sql: &str) -> Option<(String, &str)> {
    let mut rest = sql.trim_start().strip_prefix('"')?;
    let mut name = String::new();
    loop {
        let end = rest.find('"')?;
        name.push_str(&rest[..end]);
        rest = &rest[end + 1..];
        match rest.strip_prefix('"') {
            Some(escaped) => {
                name.push('"');
                rest = escaped;
            }
            None => return Some((name, rest)),
        }
    }
}

/// SQL: whether the cell `cell` is chunked.
pub(crate) fn chunked_sql(cell: &str) -> String {
    format!("(typeof({cell}) = 'integer')")
}

/// SQL: a chunked cell's epoch; NULL for any other cell.
pub(crate) fn epoch_sql(cell: &str) -> String {
    format!("(CASE WHEN {} THEN {cell} / {SPAN} END)", chunked_sql(cell))
}

/// An inline cell of `kind`.
pub(crate) fn inline_cell(kind: LargeValueKind, bytes: Vec<u8>) -> Sql {
    match kind {
        LargeValueKind::Bytes => Sql::Blob(bytes),
        LargeValueKind::String | LargeValueKind::Json => Sql::Text(bytes),
    }
}

/// A chunked cell's epoch and prefix length.
fn split(cell: i64) -> Result<(i64, usize), Error> {
    if cell < 0 {
        return Err(corrupt(lv::Error::MalformedScalar));
    }
    Ok((cell / SPAN, (cell % SPAN) as usize))
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
    /// The host runs chunk-store I/O ([`crate::db::Db::take_chunk_io`]),
    /// not the tick.
    host_io: bool,
}

/// Batch ids, unique in the process, so a batch handed back to a store
/// that did not hand it out is refused.
static NEXT_BATCH: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(1);

struct State {
    nodes: HashMap<[u8; 32], Cached>,
    /// Epochs read or written here, by id. An epoch only grows, so a cached
    /// copy answers every cell that names a prefix it holds.
    epochs: HashMap<i64, CachedEpoch>,
    /// Unpinned cached bytes, and the bound on them (`None` for an
    /// in-memory store, which retains its cached nodes).
    bytes: usize,
    budget: Option<usize>,
    clock: u64,
    /// Chunks a statement needed and nothing local holds.
    demand: BTreeSet<ChunkRequest>,
    /// The base-tree byte ranges reads of a value (by root) waited on:
    /// the next pull asks for each range whole (SPEC 19.8).
    spans: HashMap<[u8; 32], std::ops::Range<u64>>,
    /// Offloaded nodes a statement needed that the chunk store could not
    /// answer at once: the next fetch reads them.
    fetch: BTreeSet<ChunkRequest>,
    /// Offloaded nodes a fetch is out for, in either of a host's lanes or
    /// a read's own: not handed to another fetch until it lands
    /// ([`Chunks::landed`]) or is dropped ([`Chunks::want_again`]). A read
    /// that waits on one while it is out keeps it in `fetch`.
    in_flight: BTreeSet<ChunkRequest>,
    /// Offloaded nodes the host's last batch failed to fetch, with why:
    /// reads that need them are refused with that error rather than left
    /// waiting, until the next batch starts and asks again. A store that
    /// failed for a while (a token, a permission, the network) serves
    /// again once it is fixed.
    failed: BTreeMap<ChunkRequest, Error>,
    /// The batch of chunk-store work that is out, if one is: one at a
    /// time, so a delete never overtakes the put it undoes.
    batch: Option<u64>,
    /// Counts fetches answered (installed or failed) and failures
    /// forgotten: a read that waited on offloaded nodes may answer now.
    answered: u64,
}

/// Where a node's verified bytes are.
enum Local {
    Found(Bytes),
    /// Nothing here holds it: demand on the upstream.
    Absent,
    /// The chunk store holds it, and answers only asynchronously.
    Fetching,
}

/// One epoch row.
#[derive(Clone)]
struct CachedEpoch {
    root: [u8; 32],
    base: Arc<LargeValueRef>,
    edits: Bytes,
    count: usize,
    pinned: bool,
    used: u64,
}

struct Cached {
    hash: [u8; 32],
    bytes: Bytes,
    /// Written by the open transaction: retained until its owner settles it.
    pinned: bool,
    /// Fetched from the chunk store for reads that wait on it: kept until
    /// no read is waiting, so a read needing many nodes is not starved by
    /// evicting the first it fetched.
    held: bool,
    used: u64,
}

impl Chunks {
    /// A cache contains no database handle. Durable stores bound unpinned
    /// entries; in-memory stores retain them. All reads use the operation's SQL
    /// provider, including when the cache starts cold.
    pub(crate) fn new(
        durable: bool,
        store: Option<Arc<dyn ChunkStore>>,
        host_io: bool,
    ) -> Arc<Self> {
        Arc::new(Self {
            state: Mutex::new(State {
                nodes: HashMap::new(),
                epochs: HashMap::new(),
                bytes: 0,
                budget: durable.then_some(CACHE_BYTES),
                clock: 0,
                demand: BTreeSet::new(),
                spans: HashMap::new(),
                fetch: BTreeSet::new(),
                in_flight: BTreeSet::new(),
                failed: BTreeMap::new(),
                batch: None,
                answered: 0,
            }),
            store,
            host_io,
        })
    }

    /// Cache a node the open transaction wrote.
    pub(crate) fn put(&self, request: &ChunkRequest, bytes: Bytes) {
        let mut state = self.state.lock().expect("chunk cache");
        state.clock += 1;
        let used = state.clock;
        let old = state.nodes.insert(
            request.object_hash,
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

    /// Whether the host runs chunk-store I/O.
    pub(crate) fn host_io(&self) -> bool {
        self.host_io
    }

    /// The chunk store settled leaves move to, if they move: a store in
    /// memory keeps its nodes in its cache whatever holds them, so it keeps
    /// them in SQLite too.
    pub(crate) fn offload_store(&self) -> Option<&Arc<dyn ChunkStore>> {
        self.store.as_ref().filter(|_| !self.in_memory())
    }

    /// Want `requests` again: their fetch was dropped before it was
    /// answered.
    pub(crate) fn want_again(&self, requests: Vec<ChunkRequest>) {
        let mut state = self.state.lock().expect("chunk cache");
        for request in &requests {
            state.in_flight.remove(request);
        }
        state.fetch.extend(requests);
    }

    /// The fetches of `requests` were answered (installed, or failed): they
    /// are no longer out, and what waited on them has its answer.
    pub(crate) fn landed(&self, requests: &[ChunkRequest]) {
        let mut state = self.state.lock().expect("chunk cache");
        state.answered += 1;
        for request in requests {
            state.in_flight.remove(request);
            state.fetch.remove(request);
        }
    }

    /// Record that a read waits on an offloaded node.
    fn want(&self, request: &ChunkRequest) {
        let mut state = self.state.lock().expect("chunk cache");
        state.fetch.insert(request.clone());
    }

    /// Whether reads wait on nodes only the chunk store can answer, to be
    /// fetched or out.
    pub(crate) fn has_fetch(&self) -> bool {
        let state = self.state.lock().expect("chunk cache");
        !state.fetch.is_empty() || !state.in_flight.is_empty()
    }

    /// Start a batch: its id and the nodes reads are waiting on, or `None`
    /// if another batch is out. With nothing waiting, nodes kept for
    /// earlier waiting reads may be evicted again.
    /// `fetches: false` takes none of them (the host holds its fetches
    /// back), and keeps what was kept.
    pub(crate) fn begin_fetch(&self, fetches: bool) -> Option<(u64, Vec<ChunkRequest>)> {
        let mut state = self.state.lock().expect("chunk cache");
        if state.batch.is_some() {
            return None;
        }
        let batch = NEXT_BATCH.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        state.batch = Some(batch);
        if !fetches {
            return Some((batch, Vec::new()));
        }
        if state.fetch.is_empty() && state.in_flight.is_empty() {
            for node in state.nodes.values_mut() {
                node.held = false;
            }
            state.evict();
        }
        Some((batch, state.take_wanted()))
    }

    /// The nodes reads wait on, for a read that fetches them itself. Unlike
    /// [`Self::begin_fetch`] this runs beside a host's batch: a fetch never
    /// reorders a put and the delete that undoes it.
    pub(crate) fn take_fetch(&self) -> Vec<ChunkRequest> {
        self.state.lock().expect("chunk cache").take_wanted()
    }

    /// [`State::answered`].
    pub(crate) fn answered(&self) -> u64 {
        self.state.lock().expect("chunk cache").answered
    }

    /// The batch that is out, if one is.
    pub(crate) fn outstanding(&self) -> Option<u64> {
        self.state.lock().expect("chunk cache").batch
    }

    /// End `batch`, which [`Self::begin_fetch`] started; `unfetched` (what
    /// it was to fetch and did not) is fetched by the next one. Puts it
    /// marked are picked again by the next batch, and deletes stay queued
    /// until one succeeds, so a batch that ends without running loses
    /// nothing.
    pub(crate) fn end_fetch(&self, batch: u64, unfetched: Vec<ChunkRequest>) {
        let mut state = self.state.lock().expect("chunk cache");
        if state.batch == Some(batch) {
            state.batch = None;
            for request in &unfetched {
                state.in_flight.remove(request);
            }
            state.fetch.extend(unfetched);
        }
    }

    /// Forget the last batch's failed fetches, so reads of them ask again;
    /// whether there were any.
    pub(crate) fn clear_failed(&self) -> bool {
        let mut state = self.state.lock().expect("chunk cache");
        let any = !state.failed.is_empty();
        state.failed.clear();
        if any {
            state.answered += 1;
        }
        any
    }

    /// Refuse reads of `request` with `error` until the next batch starts:
    /// the host's batch failed to fetch it.
    pub(crate) fn fetch_failed(&self, request: &ChunkRequest, error: Error) {
        let mut state = self.state.lock().expect("chunk cache");
        state.failed.insert(request.clone(), error);
    }

    /// Install what the chunk store answered for an offloaded node; one it
    /// answered it does not hold is data loss.
    pub(crate) fn install_from_store(
        &self,
        request: &ChunkRequest,
        bytes: Option<Bytes>,
    ) -> Result<(), Error> {
        let Some(bytes) = bytes else {
            return Err(lost());
        };
        let mut state = self.state.lock().expect("chunk cache");
        if lv::object_hash(&bytes).0 != request.object_hash {
            return Err(corrupt(lv::Error::ObjectHashMismatch));
        }
        state.clock += 1;
        let used = state.clock;
        state.bytes += bytes.len();
        let old = state.nodes.insert(
            request.object_hash,
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
                },
                Bytes::from(chunk.encoded.clone()),
            );
        }
    }

    /// Forget a reclaimed node, and the epochs over it (which were
    /// deleted before it could be).
    pub(crate) fn forget(&self, hash: &[u8; 32]) {
        let mut state = self.state.lock().expect("chunk cache");
        if let Some(old) = state.nodes.remove(hash)
            && !old.pinned
        {
            state.bytes -= old.bytes.len();
        }
        state.epochs.retain(|_, epoch| epoch.root != *hash);
    }

    /// Cache an epoch the open transaction wrote.
    fn put_epoch(&self, id: i64, base: &LargeValueRef, edits: &[u8], count: usize) {
        let mut state = self.state.lock().expect("chunk cache");
        state.clock += 1;
        let used = state.clock;
        state.epochs.insert(
            id,
            CachedEpoch {
                root: *base.root.object_hash.as_bytes(),
                base: Arc::new(base.clone()),
                edits: Bytes::copy_from_slice(edits),
                count,
                pinned: true,
                used,
            },
        );
    }

    /// The open transaction ended: its nodes are readable from the file (or
    /// were rolled back, and are unreferenced but still correct bytes).
    pub(crate) fn unpin(&self) {
        let mut state = self.state.lock().expect("chunk cache");
        for epoch in state.epochs.values_mut() {
            epoch.pinned = false;
        }
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

    /// Whether the store uses the unbounded in-memory cache policy.
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

    /// Demand for nodes a session pulled that this relay lacks, with the
    /// span of `root`'s tree the pull asked for.
    pub(crate) fn add_demand(
        &self,
        requests: impl IntoIterator<Item = ChunkRequest>,
        span: Option<([u8; 32], std::ops::Range<u64>)>,
    ) {
        let mut state = self.state.lock().expect("chunk cache");
        state.demand.extend(requests);
        if let Some((root, span)) = span {
            state.note_span(root, span);
        }
    }

    /// The base-tree ranges reads waited on, by root.
    pub(crate) fn take_spans(&self) -> HashMap<[u8; 32], std::ops::Range<u64>> {
        std::mem::take(&mut self.state.lock().expect("chunk cache").spans)
    }

    /// Read through the operation's SQL provider. Never hold the cache mutex
    /// across SQL: an async provider may suspend for durable storage.
    async fn get_sql<S: jazz_sql::Sql>(
        &self,
        conn: &mut S,
        request: &ChunkRequest,
    ) -> Result<Local, Error>
    where
        S::Error: std::fmt::Display,
    {
        {
            let mut state = self.state.lock().expect("chunk cache");
            state.clock += 1;
            let used = state.clock;
            if let Some(node) = state.nodes.get_mut(&request.object_hash) {
                if node.hash != request.object_hash {
                    return Err(corrupt(lv::Error::ObjectHashMismatch));
                }
                node.used = used;
                return Ok(Local::Found(node.bytes.clone()));
            }
        }
        let Some(bytes) = node(conn, &request.object_hash).await? else {
            return Ok(Local::Absent);
        };
        if bytes.is_empty() {
            return self.offloaded(request);
        }
        let bytes = Bytes::from(bytes);
        if lv::object_hash(&bytes).0 != request.object_hash {
            return Err(corrupt(lv::Error::ObjectHashMismatch));
        }
        let mut state = self.state.lock().expect("chunk cache");
        state.clock += 1;
        let used = state.clock;
        // A fetch may have filled the cache while SQL was suspended. Keep
        // its pin/accounting rather than replacing it with an unpinned copy.
        if let Some(node) = state.nodes.get_mut(&request.object_hash) {
            if node.hash != request.object_hash {
                return Err(corrupt(lv::Error::ObjectHashMismatch));
            }
            node.used = used;
            return Ok(Local::Found(node.bytes.clone()));
        }
        state.bytes += bytes.len();
        state.nodes.insert(
            request.object_hash,
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

    /// Read bytes already known (by the SQL provider) to be offloaded.
    /// This path consults only the cache and chunk store, never a second SQL connection.
    fn offloaded(&self, request: &ChunkRequest) -> Result<Local, Error> {
        let mut state = self.state.lock().expect("chunk cache");
        state.clock += 1;
        let used = state.clock;
        if let Some(node) = state.nodes.get_mut(&request.object_hash) {
            if node.hash != request.object_hash {
                return Err(corrupt(lv::Error::ObjectHashMismatch));
            }
            node.used = used;
            return Ok(Local::Found(node.bytes.clone()));
        }
        if let Some(error) = state.failed.get(request) {
            return Err(error.clone());
        }
        let Some(store) = &self.store else {
            return Err(unconfigured());
        };
        let bytes = match store.get_now(&NodeKey::from(request)) {
            None => return Ok(Local::Fetching),
            Some(Ok(None)) => return Err(lost()),
            Some(Err(error)) => return Err(error),
            Some(Ok(Some(bytes))) => bytes,
        };
        if lv::object_hash(&bytes).0 != request.object_hash {
            return Err(corrupt(lv::Error::ObjectHashMismatch));
        }
        state.bytes += bytes.len();
        state.nodes.insert(
            request.object_hash,
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
}

impl State {
    /// The nodes reads want that no fetch is out for, now out. Those
    /// already out stay wanted: they are answered when theirs lands.
    fn take_wanted(&mut self) -> Vec<ChunkRequest> {
        let in_flight = &self.in_flight;
        let (out, wanted): (BTreeSet<_>, BTreeSet<_>) = std::mem::take(&mut self.fetch)
            .into_iter()
            .partition(|request| in_flight.contains(request));
        self.fetch = out;
        self.in_flight.extend(wanted.iter().cloned());
        wanted.into_iter().collect()
    }

    /// Widen the span the next pull of `root`'s tree asks for.
    fn note_span(&mut self, root: [u8; 32], span: std::ops::Range<u64>) {
        self.spans
            .entry(root)
            .and_modify(|held| *held = held.start.min(span.start)..held.end.max(span.end))
            .or_insert(span);
    }

    fn evict(&mut self) {
        let Some(budget) = self.budget else {
            return;
        };
        if self.epochs.len() > CACHE_EPOCHS {
            let mut victims: Vec<(i64, u64)> = self
                .epochs
                .iter()
                .filter(|(_, epoch)| !epoch.pinned)
                .map(|(id, epoch)| (*id, epoch.used))
                .collect();
            victims.sort_by_key(|(_, used)| *used);
            let excess = self.epochs.len() - CACHE_EPOCHS * 3 / 4;
            for (id, _) in victims.into_iter().take(excess) {
                self.epochs.remove(&id);
            }
        }
        if self.bytes <= budget {
            return;
        }
        let mut victims: Vec<([u8; 32], u64, usize)> = self
            .nodes
            .iter()
            .filter(|(_, node)| !node.pinned && !node.held)
            .map(|(hash, node)| (*hash, node.used, node.bytes.len()))
            .collect();
        victims.sort_by_key(|(_, used, _)| *used);
        for (hash, _, len) in victims {
            if self.bytes <= budget * 3 / 4 {
                break;
            }
            self.nodes.remove(&hash);
            self.bytes -= len;
        }
    }
}

fn describe_epoch(epoch: &CachedEpoch, n: usize) -> Result<LargeValueRef, Error> {
    if n > epoch.count {
        return Err(corrupt(lv::Error::MalformedScalar));
    }
    let base = &epoch.base;
    let (tail, cursor) = epoch::decode_edits(
        base.kind,
        base.byte_length,
        base.utf16_length,
        &epoch.edits,
        Some(n),
    )
    .map_err(corrupt)?;
    let mut value = (**base).clone();
    value.edit_tail = tail;
    value.byte_length = cursor.len;
    value.utf16_length = base.utf16_length.map(|_| cursor.utf16_len);
    Ok(value)
}

/// Read a cell's descriptor through its operation's SQL provider. Only the
/// referenced epoch is loaded; cache pins are owned by the transaction.
pub(crate) async fn descriptor<S: jazz_sql::Sql>(
    conn: &mut S,
    chunks: &Chunks,
    cell: i64,
) -> Result<LargeValueRef, Error>
where
    S::Error: std::fmt::Display,
{
    use jazz_sql::Row as _;
    let (id, n) = split(cell)?;
    {
        let mut state = chunks.state.lock().expect("chunk cache");
        state.clock += 1;
        let used = state.clock;
        if let Some(epoch) = state.epochs.get_mut(&id) {
            epoch.used = used;
            return describe_epoch(epoch, n);
        }
    }
    let found: Option<(Vec<u8>, Vec<u8>, i64)> = conn
        .query_optional(
            "SELECT base, edits, count FROM _jazz_lv_epoch WHERE id = ?1",
            jazz_sql::params![id],
            |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)),
        )
        .await
        .map_err(crate::values::provider_error)?;
    let gone = || corrupt(lv::Error::MalformedScalar);
    let (base, edits, count) = found.ok_or_else(gone)?;
    let base = lv::decode_large_value_ref(&base).map_err(corrupt)?;
    let count = usize::try_from(count).map_err(|_| gone())?;
    let mut state = chunks.state.lock().expect("chunk cache");
    state.clock += 1;
    let used = state.clock;
    let epoch = state.epochs.entry(id).or_insert_with(|| CachedEpoch {
        root: *base.root.object_hash.as_bytes(),
        base: Arc::new(base),
        edits: Bytes::from(edits),
        count,
        pinned: false,
        used,
    });
    epoch.used = used;
    let value = describe_epoch(epoch, n)?;
    state.evict();
    Ok(value)
}

/// Inline bytes stay borrowed; a chunked cell awaits its epoch metadata.
pub(crate) async fn logical<'a, S: jazz_sql::Sql>(
    conn: &mut S,
    chunks: &Chunks,
    cell: SqlCell<'a>,
) -> Result<Logical<'a>, Error>
where
    S::Error: std::fmt::Display,
{
    match cell {
        SqlCell::Text(bytes) | SqlCell::Blob(bytes) => Ok(Logical::Inline(bytes)),
        SqlCell::Integer(cell) => Ok(Logical::Chunked(Box::new(
            descriptor(conn, chunks, cell).await?,
        ))),
        SqlCell::Null | SqlCell::Real(_) => Err(corrupt(lv::Error::MalformedScalar)),
    }
}

/// Run a bounded chunk operation using the owning SQL connection.
pub(crate) async fn read<S: jazz_sql::Sql, T>(
    conn: &mut S,
    chunks: &Chunks,
    attempt: impl FnMut(&mut ChunkInputs) -> Result<T, AttemptError>,
    span: Option<(&LargeValueRef, std::ops::Range<u64>)>,
) -> Result<T, Error>
where
    S::Error: std::fmt::Display,
{
    Stored {
        conn: futures::lock::Mutex::new(conn),
        chunks,
        fetching: std::cell::Cell::new(false),
    }
    .read_attempt(attempt, |_| {}, span)
    .await
}

/// A chunk operation that validates an edit reports malformed input as a
/// rejected write, preserving storage/fetch failures from the provider.
pub(crate) async fn write<S: jazz_sql::Sql, T>(
    conn: &mut S,
    chunks: &Chunks,
    mut attempt: impl FnMut(&mut ChunkInputs) -> Result<T, AttemptError>,
) -> Result<T, Error>
where
    S::Error: std::fmt::Display,
{
    let mut refused = None;
    let result = read(
        conn,
        chunks,
        |inputs| match attempt(inputs) {
            Err(AttemptError::LargeValue(error)) => {
                refused = Some(error.clone());
                Err(AttemptError::LargeValue(error))
            }
            other => other,
        },
        None,
    )
    .await;
    match refused {
        Some(error) => Err(Error::new(ErrorCode::WriteRejected, error.to_string())),
        None => result,
    }
}

/// Materialize only the requested value, retaining range demand for sync.
pub(crate) async fn value_bytes<S: jazz_sql::Sql>(
    conn: &mut S,
    chunks: &Chunks,
    value: &LargeValueRef,
) -> Result<Vec<u8>, Error>
where
    S::Error: std::fmt::Display,
{
    #[cfg(feature = "testing")]
    FULL_MATERIALIZATIONS.with(|count| count.set(count.get() + 1));
    read(
        conn,
        chunks,
        |inputs| lv::materialize_attempt(value, inputs),
        Some((value, 0..value.byte_length)),
    )
    .await
}

pub(crate) async fn materialize<S: jazz_sql::Sql>(
    conn: &mut S,
    chunks: &Chunks,
    cell: SqlCell<'_>,
) -> Result<Vec<u8>, Error>
where
    S::Error: std::fmt::Display,
{
    match logical(conn, chunks, cell).await? {
        Logical::Inline(bytes) => Ok(bytes.to_vec()),
        Logical::Chunked(value) => value_bytes(conn, chunks, &value).await,
    }
}

/// Store a prepared value's new nodes and cache them. A replica (`replica`)
/// also marks them unpushed, for its next upload.
pub(crate) async fn store<S: jazz_sql::Sql>(
    conn: &mut S,
    chunks: &Chunks,
    prepared: &lv::PreparedLargeValue,
    replica: bool,
) -> Result<(), Error>
where
    S::Error: std::fmt::Display,
{
    for chunk in &prepared.staged_chunks {
        store_chunk(conn, chunks, true, chunk).await?;
        if replica {
            mark_unpushed(conn, &chunk.node_ref.object_hash).await?;
        }
    }
    record_base(conn, &prepared.value_ref).await
}

/// Record `value`'s base descriptor on its root as built or validated here.
async fn record_base<S: jazz_sql::Sql>(conn: &mut S, value: &LargeValueRef) -> Result<(), Error>
where
    S::Error: std::fmt::Display,
{
    let base = lv::tail_base_descriptor(value).map_err(corrupt)?;
    let encoded = lv::encode_large_value_ref(&base).map_err(corrupt)?;
    conn.execute(
        "INSERT INTO _jazz_lv_root (root, base) VALUES (?1, ?2) \
         ON CONFLICT (root) DO UPDATE SET base = excluded.base",
        jazz_sql::params![value.root.object_hash.as_bytes().as_slice(), encoded],
    )
    .await
    .map_err(crate::values::provider_error)?;
    Ok(())
}

/// The chunked cell of `value`, whose tail fits one epoch: the epoch over
/// its tree whose edits start with its tail, else one whose edits its tail
/// extends (appended to), else a new epoch. An epoch only grows, so every
/// cell already naming one keeps its value.
pub(crate) async fn intern<S: jazz_sql::Sql>(
    conn: &mut S,
    chunks: &Chunks,
    value: &LargeValueRef,
) -> Result<i64, Error>
where
    S::Error: std::fmt::Display,
{
    use jazz_sql::Row as _;
    let base = lv::tail_base_descriptor(value).map_err(corrupt)?;
    let encoded_base = lv::encode_large_value_ref(&base).map_err(corrupt)?;
    let n = value.edit_tail.len();
    let edits = epoch::encode_edits(
        value.kind,
        base.byte_length,
        base.utf16_length,
        &value.edit_tail,
    )
    .map_err(corrupt)?;
    if n > epoch::EPOCH_MAX_EDITS || edits.len() > epoch::EPOCH_EDIT_BYTES {
        return Err(Error::new(
            ErrorCode::Storage,
            "large value: an edit tail outgrew its epoch without consolidating",
        ));
    }
    let root = value.root.object_hash.as_bytes().as_slice();
    let candidates: Vec<(i64, Vec<u8>, i64)> = conn
        .query_rows(
            "SELECT id, edits, count FROM _jazz_lv_epoch WHERE root = ?1 AND base = ?2",
            jazz_sql::params![root, encoded_base],
            |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)),
        )
        .await
        .map_err(crate::values::provider_error)?;
    let mut extend: Option<(i64, i64)> = None;
    for (id, held, count) in &candidates {
        // Edits are self-delimiting and concatenated, so a byte prefix of
        // whole edits is a prefix of edits.
        if held.starts_with(&edits) {
            return Ok(id * SPAN + n as i64);
        }
        if edits.starts_with(held) && extend.is_none_or(|(_, most)| *count > most) {
            extend = Some((*id, *count));
        }
    }
    let id = match extend {
        Some((id, _)) => {
            conn.execute(
                "UPDATE _jazz_lv_epoch SET edits = ?2, count = ?3 WHERE id = ?1",
                jazz_sql::params![id, edits, n as i64],
            )
            .await
            .map_err(crate::values::provider_error)?;
            id
        }
        None => {
            // Drain RETURNING through completion: a native SQLite write can
            // report an error after returning its row (for example at commit).
            let mut inserted = None;
            conn.query(
                "INSERT INTO _jazz_lv_epoch (root, base, edits, count) VALUES (?1, ?2, ?3, ?4) RETURNING id",
                jazz_sql::params![root, encoded_base, edits, n as i64],
                |r| {
                    inserted = Some(r.get::<i64>(0)?);
                    Ok(std::ops::ControlFlow::Continue(()))
                },
            ).await.map_err(crate::values::provider_error)?;
            inserted.ok_or_else(|| {
                Error::new(
                    ErrorCode::Storage,
                    "large value: epoch insert returned no id",
                )
            })?
        }
    };
    chunks.put_epoch(id, &base, &edits, n);
    Ok(id * SPAN + n as i64)
}

pub(crate) async fn mark_unpushed<S: jazz_sql::Sql>(
    conn: &mut S,
    hash: &lv::ContentHash,
) -> Result<(), Error>
where
    S::Error: std::fmt::Display,
{
    conn.execute(
        "INSERT OR IGNORE INTO _jazz_lv_unpushed (hash) VALUES (?1)",
        jazz_sql::params![hash.as_bytes().as_slice()],
    )
    .await
    .map_err(crate::values::provider_error)?;
    Ok(())
}

/// Store one node, caching it pinned when `cache` says so. A new branch
/// records an edge to each child, whether or not the child is stored yet;
/// a new leaf is queued for the chunk store, if there is one.
/// The caller verified `chunk` against its hash (or produced it).
async fn store_chunk<S: jazz_sql::Sql>(
    conn: &mut S,
    chunks: &Chunks,
    cache: bool,
    chunk: &lv::StagedChunk,
) -> Result<(), Error>
where
    S::Error: std::fmt::Display,
{
    let hash = chunk.node_ref.object_hash.0;
    let inserted = conn
        .execute(
            "INSERT INTO _jazz_lv_node (hash, bytes) VALUES (?1, ?2) ON CONFLICT (hash) DO NOTHING",
            jazz_sql::params![hash.as_slice(), chunk.encoded],
        )
        .await
        .map_err(crate::values::provider_error)?;
    if cache {
        chunks.put(
            &ChunkRequest { object_hash: hash },
            Bytes::from(chunk.encoded.clone()),
        );
    }
    if inserted == 0 {
        return Ok(());
    }
    let node = lv::decode_node_untyped_verified(&chunk.encoded).map_err(corrupt)?;
    if let ChunkNode::Branch { children, .. } = node {
        for child in children {
            conn.execute(
                "INSERT OR IGNORE INTO _jazz_lv_edge (child, parent) VALUES (?1, ?2)",
                jazz_sql::params![
                    child.node_ref.object_hash.as_bytes().as_slice(),
                    hash.as_slice()
                ],
            )
            .await
            .map_err(crate::values::provider_error)?;
        }
    } else if chunks.offload_store().is_some() {
        conn.execute(
            "DELETE FROM _jazz_lv_offload_delete WHERE hash = ?1",
            jazz_sql::params![hash.as_slice()],
        )
        .await
        .map_err(crate::values::provider_error)?;
        conn.execute(
            "INSERT OR IGNORE INTO _jazz_lv_offload (hash, state) VALUES (?1, 0)",
            jazz_sql::params![hash.as_slice()],
        )
        .await
        .map_err(crate::values::provider_error)?;
    }
    Ok(())
}

/// Open a local streaming upload's lease. Until [`end_upload`] or
/// [`expire_uploads`], every node staged under `id` is held from
/// reclamation, so the row that will reference the finished value never
/// races the reclaimer.
pub(crate) async fn begin_upload<S: jazz_sql::Sql>(
    conn: &mut S,
    id: &[u8; 32],
    now: i64,
) -> Result<(), Error>
where
    S::Error: std::fmt::Display,
{
    conn.execute(
        "INSERT INTO _jazz_lv_upload (id, touched) VALUES (?1, ?2)",
        jazz_sql::params![id.as_slice(), now],
    )
    .await
    .map_err(crate::values::provider_error)?;
    Ok(())
}

/// Renew `id`'s lease; false once it was ended or expired.
pub(crate) async fn touch_upload<S: jazz_sql::Sql>(
    conn: &mut S,
    id: &[u8; 32],
    now: i64,
) -> Result<bool, Error>
where
    S::Error: std::fmt::Display,
{
    Ok(conn
        .execute(
            "UPDATE _jazz_lv_upload SET touched = ?2 WHERE id = ?1",
            jazz_sql::params![id.as_slice(), now],
        )
        .await
        .map_err(crate::values::provider_error)?
        > 0)
}

/// Store an upload batch inside a caller-owned transaction. Set `cache` to
/// retain the staged nodes in the cache until the transaction settles.
/// The owner must release cache pins when the transaction settles.
pub(crate) async fn stage_upload<S: jazz_sql::Sql>(
    conn: &mut S,
    chunks: &Chunks,
    id: &[u8; 32],
    staged: &[lv::StagedChunk],
    replica: bool,
    cache: bool,
) -> Result<(), Error>
where
    S::Error: std::fmt::Display,
{
    for chunk in staged {
        store_chunk(conn, chunks, cache, chunk).await?;
        if replica {
            mark_unpushed(conn, &chunk.node_ref.object_hash).await?;
        }
        conn.execute(
            "INSERT OR IGNORE INTO _jazz_lv_upload_node (id, hash) VALUES (?1, ?2)",
            jazz_sql::params![
                id.as_slice(),
                chunk.node_ref.object_hash.as_bytes().as_slice()
            ],
        )
        .await
        .map_err(crate::values::provider_error)?;
    }
    Ok(())
}

/// Record the finished value's tail-free base, so binding it trusts it.
pub(crate) async fn finish_upload<S: jazz_sql::Sql>(
    conn: &mut S,
    value: &LargeValueRef,
) -> Result<(), Error>
where
    S::Error: std::fmt::Display,
{
    record_base(conn, value).await
}

/// Drop `id`'s lease. Its nodes are queued for reclamation, which keeps
/// whatever a committed row now references.
pub(crate) async fn end_upload<S: jazz_sql::Sql>(conn: &mut S, id: &[u8; 32]) -> Result<(), Error>
where
    S::Error: std::fmt::Display,
{
    for sql in [
        "INSERT OR IGNORE INTO _jazz_lv_reclaim (root) SELECT hash FROM _jazz_lv_upload_node WHERE id = ?1",
        "DELETE FROM _jazz_lv_upload_node WHERE id = ?1",
        "DELETE FROM _jazz_lv_upload WHERE id = ?1",
    ] {
        conn.execute(sql, jazz_sql::params![id.as_slice()])
            .await
            .map_err(crate::values::provider_error)?;
    }
    Ok(())
}

/// End every upload not touched since `before` (ms); returns how many.
pub(crate) async fn expire_uploads<S: jazz_sql::Sql>(
    conn: &mut S,
    before: i64,
) -> Result<usize, Error>
where
    S::Error: std::fmt::Display,
{
    use jazz_sql::Row as _;
    let ids: Vec<Vec<u8>> = conn
        .query_rows(
            "SELECT id FROM _jazz_lv_upload WHERE touched <= ?1",
            jazz_sql::params![before],
            |r| r.get(0),
        )
        .await
        .map_err(crate::values::provider_error)?;
    for id in &ids {
        let id: [u8; 32] = id
            .as_slice()
            .try_into()
            .map_err(|_| Error::new(ErrorCode::Storage, "large value: malformed upload id"))?;
        end_upload(conn, &id).await?;
    }
    Ok(ids.len())
}

/// One stored node's encoded bytes (empty once they moved to the chunk
/// store).
pub(crate) async fn node<S: jazz_sql::Sql>(
    conn: &mut S,
    hash: &[u8; 32],
) -> Result<Option<Vec<u8>>, Error>
where
    S::Error: std::fmt::Display,
{
    use jazz_sql::Row as _;
    conn.query_optional(
        "SELECT bytes FROM _jazz_lv_node WHERE hash = ?1",
        jazz_sql::params![hash.as_slice()],
        |r| r.get(0),
    )
    .await
    .map_err(crate::values::provider_error)
}

fn request_of(node_ref: &lv::NodeRef) -> ChunkRequest {
    ChunkRequest {
        object_hash: node_ref.object_hash.0,
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
pub(crate) async fn stored_bytes<S: jazz_sql::Sql>(
    conn: &mut S,
    chunks: &Chunks,
    request: &ChunkRequest,
) -> Result<Option<Vec<u8>>, Error>
where
    S::Error: std::fmt::Display,
{
    let Some(bytes) = node(conn, &request.object_hash).await? else {
        return Ok(None);
    };
    if !bytes.is_empty() {
        return Ok(Some(bytes));
    }
    match chunks.offloaded(request)? {
        Local::Found(bytes) => Ok(Some(bytes.to_vec())),
        Local::Absent => Ok(None),
        Local::Fetching => {
            chunks.want(request);
            Err(missing())
        }
    }
}

/// The nodes of `value` to push for a commit: all but those under the
/// trees of `named` (roots of the settled version the commit names as its
/// source), which the upstream counts as the writer's (SPEC 19.4). A
/// node held by the upstream for others proves nothing, so nothing else
/// is skipped. `Err` lists the nodes this replica must fetch first.
pub(crate) async fn to_push<S: jazz_sql::Sql>(
    conn: &mut S,
    chunks: &Chunks,
    value: &LargeValueRef,
    named: &BTreeSet<[u8; 32]>,
) -> Result<Result<Vec<(ChunkRequest, Vec<u8>)>, Vec<ChunkRequest>>, Error>
where
    S::Error: std::fmt::Display,
{
    let covered = named_nodes(conn, named).await?;
    let mut out = Vec::new();
    let mut absent = Vec::new();
    let mut stack = vec![request_of(&value.root)];
    let mut seen = BTreeSet::new();
    while let Some(request) = stack.pop() {
        if covered.contains(&request.object_hash) || !seen.insert(request.object_hash) {
            continue;
        }
        let bytes = match stored_bytes(conn, chunks, &request).await {
            Ok(Some(bytes)) => bytes,
            Ok(None) => {
                absent.push(request);
                continue;
            }
            // In the chunk store: a fetch is under way.
            Err(error) if is_missing(&error) => {
                absent.push(request);
                continue;
            }
            Err(error) => return Err(error),
        };
        // Stored nodes were verified on the way in.
        if let ChunkNode::Branch { children, .. } =
            lv::decode_node_untyped_verified(&bytes).map_err(corrupt)?
        {
            stack.extend(children.iter().map(|child| request_of(&child.node_ref)));
        }
        out.push((request, bytes));
    }
    Ok(if absent.is_empty() {
        Ok(out)
    } else {
        Err(absent)
    })
}

/// The upstream accepted (or finally refused) the commit these nodes were
/// pushed for.
pub(crate) async fn mark_pushed<S: jazz_sql::Sql>(
    conn: &mut S,
    hashes: &[lv::ContentHash],
) -> Result<(), Error>
where
    S::Error: std::fmt::Display,
{
    for hash in hashes {
        conn.execute(
            "DELETE FROM _jazz_lv_unpushed WHERE hash = ?1",
            jazz_sql::params![hash.as_bytes().as_slice()],
        )
        .await
        .map_err(crate::values::provider_error)?;
    }
    Ok(())
}

/// Store a node fetched from upstream, after authenticating it against the
/// hash this replica asked for.
pub(crate) async fn install_fetched<S: jazz_sql::Sql>(
    conn: &mut S,
    chunks: &Chunks,
    request: &ChunkRequest,
    bytes: Vec<u8>,
) -> Result<(), Error>
where
    S::Error: std::fmt::Display,
{
    authenticate(request, &bytes)?;
    store_chunk(
        conn,
        chunks,
        true,
        &lv::StagedChunk {
            node_ref: lv::NodeRef {
                object_hash: lv::ContentHash(request.object_hash),
            },
            encoded: bytes,
        },
    )
    .await
}

/// Store a node a session (`author`) pushed ahead of its commit, held by
/// the session's receipt until `expires`.
pub(crate) async fn receive_pushed<S: jazz_sql::Sql>(
    conn: &mut S,
    chunks: &Chunks,
    request: &ChunkRequest,
    bytes: Vec<u8>,
    expires: i64,
    author: &[u8],
) -> Result<(), Error>
where
    S::Error: std::fmt::Display,
{
    install_fetched(conn, chunks, request, bytes).await?;
    conn.execute(
        "INSERT INTO _jazz_lv_receipt (hash, author, expires) VALUES (?1, ?2, ?3) \
         ON CONFLICT (hash, author) DO UPDATE SET expires = MAX(expires, excluded.expires)",
        jazz_sql::params![request.object_hash.as_slice(), author, expires],
    )
    .await
    .map_err(crate::values::provider_error)?;
    Ok(())
}

/// Drop `author`'s receipts of the pushed nodes under `root`, which its
/// commit now references: the cell holds the tree from here on. Walks only
/// nodes that still hold its receipt, so an append settles just the nodes
/// it pushed.
pub(crate) async fn settle_receipts<S: jazz_sql::Sql>(
    conn: &mut S,
    root: &lv::ContentHash,
    author: &[u8],
) -> Result<(), Error>
where
    S::Error: std::fmt::Display,
{
    conn.execute(
        "WITH RECURSIVE pushed (hash) AS ( \
           SELECT hash FROM _jazz_lv_receipt WHERE hash = ?1 AND author = ?2 \
           UNION SELECT e.child FROM _jazz_lv_edge e JOIN pushed p ON e.parent = p.hash \
           WHERE EXISTS (SELECT 1 FROM _jazz_lv_receipt WHERE hash = e.child AND author = ?2)) \
         DELETE FROM _jazz_lv_receipt WHERE author = ?2 AND hash IN (SELECT hash FROM pushed)",
        jazz_sql::params![root.as_bytes().as_slice(), author],
    )
    .await
    .map_err(crate::values::provider_error)?;
    Ok(())
}

/// Drop receipts that expired by `now`; their nodes are reclaimed unless a
/// commit referenced them meanwhile. Servers run this every tick: a cached
/// probe, and the writes only once a receipt expired.
pub(crate) async fn expire_receipts<S: jazz_sql::Sql>(conn: &mut S, now: i64) -> Result<(), Error>
where
    S::Error: std::fmt::Display,
{
    use jazz_sql::Row as _;
    if !conn
        .query_one(
            "SELECT EXISTS (SELECT 1 FROM _jazz_lv_receipt WHERE expires <= ?1)",
            jazz_sql::params![now],
            |r| r.get::<bool>(0),
        )
        .await
        .map_err(crate::values::provider_error)?
    {
        return Ok(());
    }
    conn.execute(
        "INSERT OR IGNORE INTO _jazz_lv_reclaim (root) \
         SELECT hash FROM _jazz_lv_receipt WHERE expires <= ?1",
        jazz_sql::params![now],
    )
    .await
    .map_err(crate::values::provider_error)?;
    conn.execute(
        "DELETE FROM _jazz_lv_receipt WHERE expires <= ?1",
        jazz_sql::params![now],
    )
    .await
    .map_err(crate::values::provider_error)?;
    Ok(())
}

struct Stored<'a, S> {
    conn: futures::lock::Mutex<&'a mut S>,
    chunks: &'a Chunks,
    /// A node is only in the chunk store, which answers asynchronously.
    fetching: std::cell::Cell<bool>,
}

impl<S: jazz_sql::Sql> lv::chunks::ChunkProvider for Stored<'_, S>
where
    S::Error: std::fmt::Display,
{
    fn get(
        &self,
        request: ChunkRequest,
    ) -> lv::chunks::ChunkFuture<'_, Result<Bytes, lv::chunks::ChunkError>> {
        Box::pin(async move {
            let mut conn = self.conn.lock().await;
            match stored_bytes(&mut **conn, self.chunks, &request).await {
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
            }
        })
    }
}

impl<S: jazz_sql::Sql> Stored<'_, S>
where
    S::Error: std::fmt::Display,
{
    /// Validate one bounded window or edit tail using the same provider as
    /// admission. Cache hits retain the native path's chunk reuse.
    async fn run<T>(
        &self,
        attempt: impl FnMut(&mut ChunkInputs) -> Result<T, AttemptError>,
    ) -> Result<T, Error> {
        self.read_attempt(attempt, lv::record_finalize_validation_bytes, None)
            .await
    }

    async fn read_attempt<T>(
        &self,
        mut attempt: impl FnMut(&mut ChunkInputs) -> Result<T, AttemptError>,
        read: impl Fn(usize),
        span: Option<(&LargeValueRef, std::ops::Range<u64>)>,
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
                        let mut conn = self.conn.lock().await;
                        match self.chunks.get_sql(&mut **conn, &request).await? {
                            Local::Found(bytes) => {
                                read(bytes.len());
                                inputs.install_verified_chunk(request, bytes);
                            }
                            Local::Absent => absent.push(request),
                            Local::Fetching => fetching.push(request),
                        }
                    }
                    if !absent.is_empty() || !fetching.is_empty() {
                        let span =
                            span.filter(|_| !absent.is_empty())
                                .and_then(|(value, range)| {
                                    let span = lv::base_byte_span(value, range).ok()??;
                                    Some((value.root.object_hash.0, span))
                                });
                        let mut state = self.chunks.state.lock().expect("chunk cache");
                        state.demand.extend(absent);
                        state.fetch.extend(fetching);
                        if let Some((root, span)) = span {
                            state.note_span(root, span);
                        }
                        return Err(missing());
                    }
                }
            }
        }
    }
}

/// Admit an uploaded descriptor: its tree must be complete here and honest
/// (structure, metrics, logical hash and content), and its edit tail must
/// replay against it. A root validated once is trusted afterwards, so an
/// append re-checks only its tail.
pub(crate) async fn admit<S: jazz_sql::Sql>(
    conn: &mut S,
    chunks: &Chunks,
    value: &LargeValueRef,
) -> Result<(), Error>
where
    S::Error: std::fmt::Display,
{
    use jazz_sql::Row as _;
    let rejected = |what: String| Error::new(ErrorCode::WriteRejected, what);
    let base = lv::tail_base_descriptor(value).map_err(|e| rejected(e.to_string()))?;
    let encoded = lv::encode_large_value_ref(&base).map_err(corrupt)?;
    let known: Option<Option<Vec<u8>>> = conn
        .query_optional(
            "SELECT base FROM _jazz_lv_root WHERE root = ?1",
            jazz_sql::params![value.root.object_hash.as_bytes().as_slice()],
            |r| r.get(0),
        )
        .await
        .map_err(crate::values::provider_error)?;
    let stored = Stored {
        conn: futures::lock::Mutex::new(conn),
        chunks,
        fetching: std::cell::Cell::new(false),
    };
    match known.flatten() {
        Some(known) if known == encoded => {}
        Some(_) => {
            return Err(rejected(
                "the descriptor disagrees with its stored tree".into(),
            ));
        }
        None => {
            let visited = lv::visit_reachable_chunks(&base, &stored, |_| {}).await;
            if stored.fetching.get() {
                return Err(missing());
            }
            visited.map_err(|e| rejected(format!("large value: {e}")))?;
            let mut validator =
                lv::LogicalValueValidator::new(&base).map_err(|e| rejected(e.to_string()))?;
            let mut offset = 0;
            while offset < base.byte_length {
                let end = (offset + WINDOW).min(base.byte_length);
                let window = stored
                    .run(|inputs| lv::byte_range_attempt(&base, offset..end, inputs))
                    .await?;
                validator
                    .push(&window)
                    .map_err(|e| rejected(e.to_string()))?;
                offset = end;
            }
            validator
                .finish(&base)
                .map_err(|e| rejected(e.to_string()))?;
            record_base(&mut **stored.conn.lock().await, &base).await?;
        }
    }
    if !value.edit_tail.is_empty() {
        stored
            .run(|inputs| lv::validate_edit_tail_attempt(value, inputs))
            .await?;
    }
    Ok(())
}

/// Whether the root `value` names is stored here.
pub(crate) async fn root_is_local<S: jazz_sql::Sql>(
    conn: &mut S,
    value: &LargeValueRef,
) -> Result<bool, Error>
where
    S::Error: std::fmt::Display,
{
    use jazz_sql::Row as _;
    conn.query_one(
        "SELECT EXISTS (SELECT 1 FROM _jazz_lv_node WHERE hash = ?1)",
        jazz_sql::params![value.root.object_hash.as_bytes().as_slice()],
        |r| r.get::<bool>(0),
    )
    .await
    .map_err(crate::values::provider_error)
}

/// What a session writing through sync proves it holds (SPEC 19.4): the
/// nodes it pushed under its receipts, and the trees of row versions it
/// may read that its commit names (the version its deltas extend, the
/// settled version its pushes assumed).
pub(crate) struct Possession {
    /// The session's receipt author.
    pub(crate) author: Vec<u8>,
    /// Whether the session is checked: a trusted link is not.
    pub(crate) checked: bool,
    /// The roots of those readable versions' chunked values.
    pub(crate) roots: BTreeSet<[u8; 32]>,
    /// Roots this commit's cells proved through receipts. Their receipts
    /// settle once the whole commit applies, so a later cell of the same
    /// commit sharing nodes with an earlier one is proved by them too.
    pub(crate) settle: std::cell::RefCell<Vec<lv::ContentHash>>,
}

/// Whether `possession` covers `value`'s tree: every node is in a named
/// readable version's tree, or pushed under the session's receipt with
/// its children covered in turn. A node held here for anyone else proves
/// nothing, so naming a hash never reads another row's content.
pub(crate) async fn possessed<S: jazz_sql::Sql>(
    conn: &mut S,
    chunks: &Chunks,
    value: &LargeValueRef,
    possession: &Possession,
) -> Result<bool, Error>
where
    S::Error: std::fmt::Display,
{
    use jazz_sql::Row as _;
    let root = *value.root.object_hash.as_bytes();
    if !possession.checked || possession.roots.contains(&root) {
        return Ok(true);
    }
    // The named trees' nodes, walked down their stored edges once needed.
    let mut named: Option<std::collections::HashSet<[u8; 32]>> = None;
    let mut stack = vec![root];
    let mut seen = BTreeSet::new();
    while let Some(hash) = stack.pop() {
        if !seen.insert(hash) {
            continue;
        }
        let named = match &mut named {
            Some(named) => named,
            None => named.insert(named_nodes(conn, &possession.roots).await?),
        };
        if named.contains(&hash) {
            continue;
        }
        let receipt: bool = conn
            .query_one(
                "SELECT EXISTS (SELECT 1 FROM _jazz_lv_receipt WHERE hash = ?1 AND author = ?2)",
                jazz_sql::params![hash.as_slice(), possession.author],
                |r| r.get(0),
            )
            .await
            .map_err(crate::values::provider_error)?;
        if !receipt {
            return Ok(false);
        }
        let request = ChunkRequest { object_hash: hash };
        let Some(bytes) = stored_bytes(conn, chunks, &request).await? else {
            return Ok(false);
        };
        if let ChunkNode::Branch { children, .. } =
            lv::decode_node_untyped_verified(&bytes).map_err(corrupt)?
        {
            stack.extend(children.iter().map(|child| child.node_ref.object_hash.0));
        }
    }
    Ok(true)
}

/// The row version a replica's pulls of a root's nodes cite.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct Via {
    pub table: String,
    pub id: [u8; 16],
    pub key: Vec<u8>,
    /// The version's position here (as `Image::at`).
    pub at: crate::db::sync::Pos,
}

/// Record that `root` arrived in `via`, the version later pulls of its
/// nodes cite.
pub(crate) async fn record_via<S: jazz_sql::Sql>(
    conn: &mut S,
    root: &[u8; 32],
    via: &Via,
) -> Result<(), Error>
where
    S::Error: std::fmt::Display,
{
    conn.execute(
        "INSERT OR REPLACE INTO _jazz_lv_via (root, tbl, id, key, t) \
         VALUES (?1, ?2, ?3, ?4, ?5)",
        jazz_sql::params![
            root.as_slice(),
            via.table,
            via.id.as_slice(),
            via.key,
            via.at.t()
        ],
    )
    .await
    .map_err(crate::values::provider_error)?;
    Ok(())
}

/// Parents an upward walk visits before it gives up on the edges and
/// walks the tree down from its root instead.
const UPWARD_WALK: usize = 4096;

/// The received root `hash` hangs under, with the version that names it:
/// walks up the stored edges (a replica learns a node's hash only from a
/// stored parent or a descriptor).
pub(crate) async fn via_of<S: jazz_sql::Sql>(
    conn: &mut S,
    hash: &[u8; 32],
) -> Result<Option<([u8; 32], Via)>, Error>
where
    S::Error: std::fmt::Display,
{
    use jazz_sql::Row as _;
    let mut stack = vec![*hash];
    let mut seen = BTreeSet::new();
    while let Some(hash) = stack.pop() {
        if !seen.insert(hash) || seen.len() > UPWARD_WALK {
            continue;
        }
        let found = conn
            .query_optional(
                "SELECT tbl, id, key, t FROM _jazz_lv_via WHERE root = ?1",
                jazz_sql::params![hash.as_slice()],
                |r| {
                    Ok((
                        r.get::<String>(0)?,
                        r.get::<Vec<u8>>(1)?,
                        r.get::<Vec<u8>>(2)?,
                        r.get::<i64>(3)?,
                    ))
                },
            )
            .await
            .map_err(crate::values::provider_error)?;
        if let Some((table, id, key, t)) = found
            && let Ok(id) = <[u8; 16]>::try_from(id.as_slice())
        {
            return Ok(Some((
                hash,
                Via {
                    table,
                    id,
                    key,
                    at: crate::db::sync::Pos::from_t(t),
                },
            )));
        }
        let found: Vec<Vec<u8>> = conn
            .query_rows(
                "SELECT parent FROM _jazz_lv_edge WHERE child = ?1",
                jazz_sql::params![hash.as_slice()],
                |r| r.get(0),
            )
            .await
            .map_err(crate::values::provider_error)?;
        stack.extend(
            found
                .iter()
                .filter_map(|p| <[u8; 32]>::try_from(p.as_slice()).ok()),
        );
    }
    Ok(None)
}

/// Whether `hash` is a node of `root`'s tree: walked up the stored edges,
/// or down from the root once the walk up spreads too far (a node many
/// trees share). `down` caches the walk down.
async fn under<S: jazz_sql::Sql>(
    conn: &mut S,
    root: &[u8; 32],
    hash: &[u8; 32],
    down: &mut Option<std::collections::HashSet<[u8; 32]>>,
) -> Result<bool, Error>
where
    S::Error: std::fmt::Display,
{
    use jazz_sql::Row as _;
    if let Some(down) = down {
        return Ok(down.contains(hash));
    }
    let mut stack = vec![*hash];
    let mut seen = BTreeSet::new();
    while let Some(hash) = stack.pop() {
        if hash == *root {
            return Ok(true);
        }
        if !seen.insert(hash) {
            continue;
        }
        if seen.len() > UPWARD_WALK {
            let nodes = named_nodes(conn, &BTreeSet::from([*root])).await?;
            let found = nodes.contains(&hash);
            *down = Some(nodes);
            return Ok(found);
        }
        let found: Vec<Vec<u8>> = conn
            .query_rows(
                "SELECT parent FROM _jazz_lv_edge WHERE child = ?1",
                jazz_sql::params![hash.as_slice()],
                |r| r.get(0),
            )
            .await
            .map_err(crate::values::provider_error)?;
        stack.extend(
            found
                .iter()
                .filter_map(|p| <[u8; 32]>::try_from(p.as_slice()).ok()),
        );
    }
    Ok(false)
}

/// What a pull is answered with: the nodes found, the wanted nodes not
/// under the cited tree, the wanted nodes under it this store lacks, and
/// the wanted nodes left for a later answer once `budget` ran out.
pub(crate) type Pulled = (
    Vec<([u8; 32], Vec<u8>)>,
    Vec<[u8; 32]>,
    Vec<[u8; 32]>,
    Vec<[u8; 32]>,
);

/// Answer a pull of `root`'s tree (a version the reader may read): the
/// nodes of `wanted` under it and, for `span` (base-tree bytes), every
/// node on the paths covering those bytes beneath a wanted one, parents
/// first, until `budget` bytes (see [`Pulled`]). Nodes in `sent` (already
/// in this answer) are not sent again; at least one wanted node is
/// answered whatever the budget, so a pull always progresses. A node the
/// chunk store answers only asynchronously fails the pull as missing; it
/// is answered again once fetched.
pub(crate) async fn pulled<S: jazz_sql::Sql>(
    conn: &mut S,
    chunks: &Chunks,
    root: &[u8; 32],
    wanted: &[[u8; 32]],
    span: Option<std::ops::Range<u64>>,
    budget: usize,
    sent: &mut std::collections::HashSet<[u8; 32]>,
) -> Result<Pulled, Error>
where
    S::Error: std::fmt::Display,
{
    let wanted_set: BTreeSet<[u8; 32]> = wanted.iter().copied().collect();
    let mut out = Vec::new();
    let mut size = 0;
    if let Some(span) = span.filter(|span| !span.is_empty()) {
        // Walk down through the bytes the read needs, sending what lies
        // beneath a wanted node: the replica holds what is above it.
        let mut stack = vec![(*root, 0u64, wanted_set.contains(root))];
        while let Some((hash, offset, send)) = stack.pop() {
            if size >= budget {
                break;
            }
            let Some(raw) = node(conn, &hash).await? else {
                continue;
            };
            let bytes = if send {
                let Some(bytes) =
                    stored_bytes(conn, chunks, &ChunkRequest { object_hash: hash }).await?
                else {
                    continue;
                };
                bytes
            } else {
                raw
            };
            // An offloaded leaf's bytes are elsewhere: it has no children.
            if !bytes.is_empty()
                && let ChunkNode::Branch { children, .. } =
                    lv::decode_node_untyped_verified(&bytes).map_err(corrupt)?
            {
                let mut at = offset;
                let mut next = Vec::new();
                for child in children {
                    let end = at.saturating_add(child.metrics.byte_length);
                    if at < span.end && span.start < end {
                        let child = child.node_ref.object_hash.0;
                        next.push((child, at, send || wanted_set.contains(&child)));
                    }
                    at = end;
                }
                stack.extend(next.into_iter().rev());
            }
            if send && sent.insert(hash) {
                size += bytes.len();
                out.push((hash, bytes));
            }
        }
    }
    let mut down = None;
    let mut foreign = Vec::new();
    let mut absent = Vec::new();
    let mut rest = Vec::new();
    for hash in wanted {
        if sent.contains(hash) {
            continue;
        }
        if size >= budget && !out.is_empty() {
            rest.push(*hash);
            continue;
        }
        if !under(conn, root, hash, &mut down).await? {
            foreign.push(*hash);
            continue;
        }
        match stored_bytes(conn, chunks, &ChunkRequest { object_hash: *hash }).await? {
            Some(bytes) => {
                sent.insert(*hash);
                size += bytes.len();
                out.push((*hash, bytes));
            }
            None => absent.push(*hash),
        }
    }
    Ok((out, foreign, absent, rest))
}

/// Whether a parent this store holds names `hash`: a node a range pull
/// sent beneath one this replica asked for.
pub(crate) async fn has_parent<S: jazz_sql::Sql>(
    conn: &mut S,
    hash: &[u8; 32],
) -> Result<bool, Error>
where
    S::Error: std::fmt::Display,
{
    use jazz_sql::Row as _;
    conn.query_one(
        "SELECT EXISTS (SELECT 1 FROM _jazz_lv_edge WHERE child = ?1)",
        jazz_sql::params![hash.as_slice()],
        |r| r.get(0),
    )
    .await
    .map_err(crate::values::provider_error)
}

/// Every node under `roots` this store knows of, through its edges.
async fn named_nodes<S: jazz_sql::Sql>(
    conn: &mut S,
    roots: &BTreeSet<[u8; 32]>,
) -> Result<std::collections::HashSet<[u8; 32]>, Error>
where
    S::Error: std::fmt::Display,
{
    use jazz_sql::Row as _;
    let mut out = std::collections::HashSet::new();
    let mut stack: Vec<[u8; 32]> = roots.iter().copied().collect();
    while let Some(hash) = stack.pop() {
        if !out.insert(hash) {
            continue;
        }
        let found: Vec<Vec<u8>> = conn
            .query_rows(
                "SELECT child FROM _jazz_lv_edge WHERE parent = ?1",
                jazz_sql::params![hash.as_slice()],
                |r| r.get(0),
            )
            .await
            .map_err(crate::values::provider_error)?;
        for child in found {
            if let Ok(child) = <[u8; 32]>::try_from(child.as_slice()) {
                stack.push(child);
            }
        }
    }
    Ok(out)
}

/// Whether nodes wait in the reclaim queue.
pub(crate) async fn reclaim_queued<S: jazz_sql::Sql>(conn: &mut S) -> Result<bool, Error>
where
    S::Error: std::fmt::Display,
{
    use jazz_sql::Row as _;
    conn.query_one(
        "SELECT EXISTS (SELECT 1 FROM _jazz_lv_reclaim)",
        jazz_sql::params![],
        |r| r.get(0),
    )
    .await
    .map_err(crate::values::provider_error)
}

/// Delete up to `budget` unreferenced nodes. A root is reclaimed once no
/// epoch references it and no parent branch holds it; deleting a branch
/// releases its children, which are reclaimed in turn. Returns the number
/// of nodes deleted.
pub(crate) async fn reclaim<S: jazz_sql::Sql>(
    conn: &mut S,
    chunks: &Chunks,
    budget: usize,
) -> Result<usize, Error>
where
    S::Error: std::fmt::Display,
{
    use jazz_sql::Row as _;
    // An epoch interned for a write that then stored another cell is named
    // by none: free it, releasing its root.
    conn.execute(
        "DELETE FROM _jazz_lv_epoch WHERE refs <= 0",
        jazz_sql::params![],
    )
    .await
    .map_err(crate::values::provider_error)?;
    let mut deleted = 0;
    let mut queue: Vec<Vec<u8>> = conn
        .query_rows(
            "SELECT root FROM _jazz_lv_reclaim LIMIT ?1",
            jazz_sql::params![budget as i64],
            |r| r.get(0),
        )
        .await
        .map_err(crate::values::provider_error)?;
    for root in &queue {
        conn.execute(
            "DELETE FROM _jazz_lv_reclaim WHERE root = ?1",
            jazz_sql::params![root],
        )
        .await
        .map_err(crate::values::provider_error)?;
    }
    while let Some(hash) = queue.pop() {
        if deleted >= budget {
            // Leave the rest for the next pass.
            conn.execute(
                "INSERT OR IGNORE INTO _jazz_lv_reclaim (root) VALUES (?1)",
                jazz_sql::params![hash],
            )
            .await
            .map_err(crate::values::provider_error)?;
            continue;
        }
        let referenced: bool = conn
            .query_one(
                "SELECT EXISTS (SELECT 1 FROM _jazz_lv_root WHERE root = ?1 \
                 AND refs > 0) \
                 OR EXISTS (SELECT 1 FROM _jazz_lv_edge WHERE child = ?1) \
                 OR EXISTS (SELECT 1 FROM _jazz_lv_receipt WHERE hash = ?1) \
                 OR EXISTS (SELECT 1 FROM _jazz_lv_upload_node WHERE hash = ?1)",
                jazz_sql::params![hash],
                |r| r.get(0),
            )
            .await
            .map_err(crate::values::provider_error)?;
        if referenced {
            continue;
        }
        let key: [u8; 32] = hash
            .as_slice()
            .try_into()
            .map_err(|_| corrupt(lv::Error::MalformedNode))?;
        let bytes = node(conn, &key).await?;
        conn.execute(
            "DELETE FROM _jazz_lv_root WHERE root = ?1",
            jazz_sql::params![hash],
        )
        .await
        .map_err(crate::values::provider_error)?;
        conn.execute(
            "DELETE FROM _jazz_lv_via WHERE root = ?1",
            jazz_sql::params![hash],
        )
        .await
        .map_err(crate::values::provider_error)?;
        let Some(bytes) = bytes else {
            continue;
        };
        conn.execute(
            "DELETE FROM _jazz_lv_node WHERE hash = ?1",
            jazz_sql::params![hash],
        )
        .await
        .map_err(crate::values::provider_error)?;
        chunks.forget(&key);
        deleted += 1;
        if chunks.offload_store().is_some() {
            // A leaf whose bytes are in the chunk store, or may be (a put
            // in flight), is deleted there once this deletion commits.
            let put: bool = conn
                .query_one(
                    "SELECT EXISTS (SELECT 1 FROM _jazz_lv_offload WHERE hash = ?1 \
                     AND state = 1)",
                    jazz_sql::params![hash],
                    |r| r.get(0),
                )
                .await
                .map_err(crate::values::provider_error)?;
            if bytes.is_empty() || put {
                conn.execute(
                    "INSERT OR REPLACE INTO _jazz_lv_offload_delete (hash) VALUES (?1)",
                    jazz_sql::params![hash],
                )
                .await
                .map_err(crate::values::provider_error)?;
            }
            conn.execute(
                "DELETE FROM _jazz_lv_offload WHERE hash = ?1",
                jazz_sql::params![hash],
            )
            .await
            .map_err(crate::values::provider_error)?;
            if bytes.is_empty() {
                // Only leaves are offloaded: no children to release.
                continue;
            }
        }
        let node =
            lv::decode_node_untyped_authenticated(lv::ContentHash(key), &bytes).map_err(corrupt)?;
        if let ChunkNode::Branch { children, .. } = node {
            conn.execute(
                "DELETE FROM _jazz_lv_edge WHERE parent = ?1",
                jazz_sql::params![hash],
            )
            .await
            .map_err(crate::values::provider_error)?;
            for child in children {
                queue.push(child.node_ref.object_hash.as_bytes().to_vec());
            }
        }
    }
    Ok(deleted)
}

#[cfg(all(test, feature = "sqlite"))]
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
        let chunks = Chunks::new(false, None, false);
        conn.execute_batch(DDL).unwrap();
        (conn, chunks)
    }

    async fn stage_upload_native(
        conn: &Connection,
        chunks: &Chunks,
        id: &[u8; 32],
        staged: &[lv::StagedChunk],
        replica: bool,
    ) -> Result<(), Error> {
        let cache = !conn.is_autocommit() || chunks.in_memory();
        crate::values::in_savepoint_async(conn, "jazz_lv_upload", async || {
            super::stage_upload(
                &mut jazz_sql::native::Native(conn),
                chunks,
                id,
                staged,
                replica,
                cache,
            )
            .await
        })
        .await
    }

    fn open_offload_native(conn: &Connection, chunks: &Chunks) -> Result<(), Error> {
        crate::options::block_on(crate::values::in_savepoint_async(
            conn,
            "jazz_lv_offload",
            async || super::open_offload(&mut jazz_sql::native::Native(conn), chunks).await,
        ))
    }

    fn begin_offload_native(
        conn: &Connection,
        limit: usize,
    ) -> Result<Vec<(NodeKey, Vec<u8>)>, Error> {
        crate::options::block_on(crate::values::in_savepoint_async(
            conn,
            "jazz_lv_offload",
            async || super::begin_offload(&mut jazz_sql::native::Native(conn), limit).await,
        ))
    }

    /// Alice pushes a tree while every SQL statement suspends; Mallory cannot
    /// reuse Alice's receipts. Settling receipts removes that proof, while a
    /// readable source root still proves possession. This provider-level test
    /// is needed because the public Db constructor still owns native SQLite.
    #[test]
    fn suspended_chunk_storage_preserves_receipt_ownership() {
        futures::executor::block_on(async {
            let (conn, chunks) = chunks();
            let mut sql = crate::sql_test::Suspends(jazz_sql::native::Native(&conn));
            let built = lv::prepare(LargeValueKind::String, &text(47, 300_000)).unwrap();
            let value = &built.value_ref;
            store(&mut sql, &chunks, &built, true).await.unwrap();
            let cell = intern(&mut sql, &chunks, value).await.unwrap();
            assert_eq!(intern(&mut sql, &chunks, value).await.unwrap(), cell);
            let mut alice = Possession {
                author: b"alice".to_vec(),
                checked: true,
                roots: BTreeSet::new(),
                settle: Default::default(),
            };
            let mallory = Possession {
                author: b"mallory".to_vec(),
                checked: true,
                roots: BTreeSet::new(),
                settle: Default::default(),
            };
            assert!(!possessed(&mut sql, &chunks, value, &alice).await.unwrap());
            let nodes = to_push(&mut sql, &chunks, value, &BTreeSet::new())
                .await
                .unwrap()
                .unwrap();
            assert_eq!(nodes.len(), built.staged_chunks.len());
            for (request, bytes) in nodes {
                assert_eq!(
                    stored_bytes(&mut sql, &chunks, &request).await.unwrap(),
                    Some(bytes.clone())
                );
                receive_pushed(&mut sql, &chunks, &request, bytes, 100, &alice.author)
                    .await
                    .unwrap();
            }
            assert!(possessed(&mut sql, &chunks, value, &alice).await.unwrap());
            assert!(!possessed(&mut sql, &chunks, value, &mallory).await.unwrap());
            let root = value.root.object_hash.0;
            let foreign_hash = [0xff; 32];
            let (found, foreign, absent, rest) = pulled(
                &mut sql,
                &chunks,
                &root,
                &[root, foreign_hash],
                None,
                usize::MAX,
                &mut Default::default(),
            )
            .await
            .unwrap();
            assert_eq!(found.len(), 1);
            assert_eq!(found[0].0, root);
            assert_eq!(foreign, vec![foreign_hash]);
            assert!(absent.is_empty());
            assert!(rest.is_empty());

            settle_receipts(&mut sql, &value.root.object_hash, &alice.author)
                .await
                .unwrap();
            assert!(!possessed(&mut sql, &chunks, value, &alice).await.unwrap());
            alice.roots.insert(value.root.object_hash.0);
            assert!(possessed(&mut sql, &chunks, value, &alice).await.unwrap());
            assert!(
                to_push(&mut sql, &chunks, value, &alice.roots)
                    .await
                    .unwrap()
                    .unwrap()
                    .is_empty()
            );
            let mut deleted = 0;
            loop {
                let pass = reclaim(&mut sql, &chunks, 1).await.unwrap();
                assert!(pass <= 1);
                deleted += pass;
                if pass == 0 {
                    break;
                }
            }
            assert_eq!(deleted, built.staged_chunks.len());
            assert!(node(&mut sql, &root).await.unwrap().is_none());
        });
    }

    /// Alice holds a read lock while Bob interns an epoch. SQLite returns the
    /// epoch ID before discovering that commit is busy; Bob must receive the
    /// error, never a successful cell ID. This lower-level test needs a native
    /// rollback-journal connection to force the post-RETURNING failure.
    #[test]
    fn epoch_insert_reports_failure_after_returning_its_id() {
        let path = std::env::temp_dir().join(format!("jazz-epoch-{}.sqlite", uuid::Uuid::new_v4()));
        let conn = Connection::open(&path).unwrap();
        conn.execute_batch(DDL).unwrap();
        conn.busy_timeout(std::time::Duration::ZERO).unwrap();
        let chunks = Chunks::new(false, None, false);
        let built = lv::prepare(LargeValueKind::String, &text(73, 100_000)).unwrap();
        crate::options::block_on(store(
            &mut jazz_sql::native::Native(&conn),
            &chunks,
            &built,
            false,
        ))
        .unwrap();
        let reader = Connection::open(&path).unwrap();
        reader.execute_batch("BEGIN").unwrap();
        reader
            .query_row("SELECT count(*) FROM _jazz_lv_epoch", [], |r| {
                r.get::<_, i64>(0)
            })
            .unwrap();
        let error = crate::options::block_on(intern(
            &mut jazz_sql::native::Native(&conn),
            &chunks,
            &built.value_ref,
        ))
        .unwrap_err();
        assert_eq!(error.code, ErrorCode::Storage);
        reader.execute_batch("ROLLBACK").unwrap();
        assert_eq!(
            conn.query_row("SELECT count(*) FROM _jazz_lv_epoch", [], |r| r
                .get::<_, i64>(0))
                .unwrap(),
            0
        );
        let cell = crate::options::block_on(intern(
            &mut jazz_sql::native::Native(&conn),
            &chunks,
            &built.value_ref,
        ))
        .unwrap();
        assert!(cell > 0);
        drop(reader);
        drop(conn);
        std::fs::remove_file(path).unwrap();
    }

    /// Alice uploads cold chunks; Bob validates their descriptor through SQL
    /// calls that suspend, without any native cache reader. Forged metrics and
    /// incomplete trees remain rejected. This provider-level test is necessary
    /// while public Db construction still requires a native connection.
    #[test]
    fn uploaded_validation_uses_only_the_async_sql_provider() {
        futures::executor::block_on(async {
            let (conn, chunks) = chunks();
            let mut sql = crate::sql_test::Suspends(jazz_sql::native::Native(&conn));
            let built = lv::prepare(LargeValueKind::String, &text(91, 300_000)).unwrap();
            for chunk in &built.staged_chunks {
                store_chunk(&mut sql, &chunks, false, chunk).await.unwrap();
            }
            let mut forged = built.value_ref.clone();
            forged.byte_length += 1;
            assert!(admit(&mut sql, &chunks, &forged).await.is_err());
            admit(&mut sql, &chunks, &built.value_ref).await.unwrap();
            assert!(admit(&mut sql, &chunks, &forged).await.is_err());
            let appended = match lv::append_tail(&built.value_ref, b"!".to_vec()).unwrap() {
                lv::TailOutcome::Updated(value) => value,
                other => panic!("{other:?}"),
            };
            for chunk in &built.staged_chunks {
                chunks.forget(chunk.node_ref.object_hash.as_bytes());
            }
            admit(&mut sql, &chunks, &appended).await.unwrap();

            let incomplete = lv::prepare(LargeValueKind::String, &text(92, 300_000)).unwrap();
            for chunk in incomplete.staged_chunks.iter().skip(1) {
                store_chunk(&mut sql, &chunks, false, chunk).await.unwrap();
            }
            assert!(
                admit(&mut sql, &chunks, &incomplete.value_ref)
                    .await
                    .is_err()
            );
        });
    }

    /// Alice's suspended upload batch is rolled back by its owner, then retried.
    /// Bob's reclamation cannot remove the retried chunks until Alice ends her
    /// lease. A lower-level test supplies a suspending provider inside the
    /// native transaction owner while public Db remains native-only.
    /// alice -> stage -> owner rollback -> retry -> bob reclaim -> lease end
    #[test]
    fn suspended_upload_staging_obeys_its_transaction_owner() {
        futures::executor::block_on(async {
            let (conn, chunks) = chunks();
            let id = [31; 32];
            let built = lv::prepare(LargeValueKind::String, &text(93, 300_000)).unwrap();
            let mut sql = crate::sql_test::Suspends(jazz_sql::native::Native(&conn));
            begin_upload(&mut sql, &id, 100).await.unwrap();
            let error = crate::values::in_savepoint_async(&conn, "test_upload", async || {
                stage_upload(&mut sql, &chunks, &id, &built.staged_chunks, false, false).await?;
                Err::<(), _>(Error::new(ErrorCode::WriteRejected, "owner rejected batch"))
            })
            .await
            .unwrap_err();
            assert_eq!(error.code, ErrorCode::WriteRejected);
            for chunk in &built.staged_chunks {
                assert!(
                    node(&mut sql, chunk.node_ref.object_hash.as_bytes())
                        .await
                        .unwrap()
                        .is_none()
                );
            }
            crate::values::in_savepoint_async(&conn, "test_upload", async || {
                stage_upload(&mut sql, &chunks, &id, &built.staged_chunks, false, false).await
            })
            .await
            .unwrap();
            admit(&mut sql, &chunks, &built.value_ref).await.unwrap();
            assert_eq!(reclaim(&mut sql, &chunks, usize::MAX / 2).await.unwrap(), 0);
            end_upload(&mut sql, &id).await.unwrap();
            assert_eq!(
                reclaim(&mut sql, &chunks, 10_000).await.unwrap(),
                built.staged_chunks.len()
            );
        });
    }

    /// Alice extends one epoch while Bob reads its old and new cells with a
    /// cold cache and a suspending SQL provider. Each cell keeps its own edit
    /// prefix. This lower-level fixture removes the native reader entirely.
    #[test]
    fn cold_epoch_reads_keep_each_cells_edit_prefix() {
        futures::executor::block_on(async {
            let (conn, chunks) = chunks();
            let mut sql = crate::sql_test::Suspends(jazz_sql::native::Native(&conn));
            let bytes = text(94, 300_000);
            let built = lv::prepare(LargeValueKind::String, &bytes).unwrap();
            store(&mut sql, &chunks, &built, false).await.unwrap();
            let original = intern(&mut sql, &chunks, &built.value_ref).await.unwrap();
            let appended = match lv::append_tail(&built.value_ref, b"!".to_vec()).unwrap() {
                lv::TailOutcome::Updated(value) => value,
                other => panic!("{other:?}"),
            };
            let edited = intern(&mut sql, &chunks, &appended).await.unwrap();
            assert_eq!(original / SPAN, edited / SPAN);
            let cold = Chunks::new(false, None, false);
            assert_eq!(
                descriptor(&mut sql, &cold, original).await.unwrap(),
                built.value_ref
            );
            assert_eq!(descriptor(&mut sql, &cold, edited).await.unwrap(), appended);
            assert_eq!(
                materialize(&mut sql, &cold, SqlCell::Integer(original))
                    .await
                    .unwrap(),
                bytes
            );
            let mut expected = bytes;
            expected.push(b'!');
            assert_eq!(
                materialize(&mut sql, &cold, SqlCell::Integer(edited))
                    .await
                    .unwrap(),
                expected
            );
            assert!(descriptor(&mut sql, &cold, edited + 1).await.is_err());
        });
    }

    /// A reopened store learns its large-capable columns from the
    /// refcount triggers, including a set a later schema added and names
    /// that need quoting, even names that read like the column list.
    #[test]
    fn stored_large_columns_reads_every_trigger_set() {
        use crate::records::ValueType;
        let (conn, _chunks) = chunks();
        let docs = "d AFTER UPDATE OF \"x\"";
        let names = ["body", "odd \"name\", ON x", "count", "late"];
        let mut table = Table::stored_only(
            docs.to_owned(),
            names[..3].iter().map(|name| name.to_string()).collect(),
        );
        for column in &mut table.columns[..2] {
            column.ty = ValueType::String;
        }
        crate::options::block_on(crate::db::execute_script(
            &mut jazz_sql::native::Native(&conn),
            &table.ddl(),
        ))
        .unwrap();
        let mut late = Table::stored_only(docs.to_owned(), vec![names[3].to_owned()]).columns;
        late[0].ty = ValueType::Bytes;
        for target in [docs.to_owned(), format!("{docs}__h")] {
            conn.execute_batch(&format!("ALTER TABLE {} ADD COLUMN late;", quote(&target)))
                .unwrap();
        }
        crate::options::block_on(crate::db::execute_script(
            &mut jazz_sql::native::Native(&conn),
            &triggers(&table, &[&late[0]], Some("late")),
        ))
        .unwrap();
        let expected: BTreeSet<String> = [names[0], names[1], names[3]].map(str::to_owned).into();
        assert_eq!(
            crate::options::block_on(stored_large_columns(
                &mut jazz_sql::native::Native(&conn),
                docs
            ))
            .unwrap(),
            expected
        );

        // A table without large-capable columns has no triggers.
        let plain = Table::stored_only("plain".to_owned(), vec!["count".to_owned()]);
        crate::options::block_on(crate::db::execute_script(
            &mut jazz_sql::native::Native(&conn),
            &plain.ddl(),
        ))
        .unwrap();
        assert_eq!(
            crate::options::block_on(stored_large_columns(
                &mut jazz_sql::native::Native(&conn),
                "plain"
            ))
            .unwrap(),
            BTreeSet::new()
        );
    }

    fn chunked(conn: &Connection, chunks: &Chunks, kind: LargeValueKind, logical: &[u8]) -> Sql {
        let prepared = lv::prepare(kind, logical).unwrap();
        crate::options::block_on(store(
            &mut jazz_sql::native::Native(conn),
            chunks,
            &prepared,
            false,
        ))
        .unwrap();
        Sql::Integer(
            crate::options::block_on(intern(
                &mut jazz_sql::native::Native(conn),
                chunks,
                &prepared.value_ref,
            ))
            .unwrap(),
        )
    }

    /// Every physical form of a value: inline, chunked, and chunked as a
    /// shorter base plus an appended edit tail.
    fn forms(conn: &Connection, chunks: &Chunks, kind: LargeValueKind, logical: &[u8]) -> Vec<Sql> {
        let mut forms = vec![inline_cell(kind, logical.to_vec())];
        if !logical.is_empty() {
            forms.push(chunked(conn, chunks, kind, logical));
        }
        if logical.len() > 10 {
            let (base, tail) = logical.split_at(logical.len() - 7);
            let prepared = lv::prepare(kind, base).unwrap();
            crate::options::block_on(store(
                &mut jazz_sql::native::Native(conn),
                chunks,
                &prepared,
                false,
            ))
            .unwrap();
            let lv::TailOutcome::Updated(appended) =
                lv::append_tail(&prepared.value_ref, tail.to_vec()).unwrap()
            else {
                panic!("a short append fits the tail");
            };
            forms.push(Sql::Integer(
                crate::options::block_on(intern(
                    &mut jazz_sql::native::Native(conn),
                    chunks,
                    &appended,
                ))
                .unwrap(),
            ));
        }
        forms
    }

    /// Queries compare chunked cells by identity (SPEC 19.5): every form
    /// of a value reads back as it, a tree built twice from the same bytes
    /// is one cell, and a base plus an edit tail is a different cell.
    #[test]
    fn cells_of_one_value_are_one_cell_only_when_tail_free() {
        let (conn, chunks) = chunks();
        let big = text(1, 200 * 1024);
        for kind in [LargeValueKind::String, LargeValueKind::Bytes] {
            let cells = forms(&conn, &chunks, kind, &big);
            for cell in &cells {
                assert_eq!(
                    crate::options::block_on(materialize(
                        &mut jazz_sql::native::Native(&conn),
                        &chunks,
                        cell.as_ref()
                    ))
                    .unwrap(),
                    big,
                    "{kind:?}"
                );
            }
            let [_, built, appended] = cells.as_slice() else {
                panic!("three forms");
            };
            assert_eq!(*built, chunked(&conn, &chunks, kind, &big), "{kind:?}");
            assert_ne!(built, appended, "{kind:?}");
        }
        assert!(chunks.take_demand().is_empty());
    }

    #[test]
    fn interning_reuses_extends_and_forks_epochs() {
        let (conn, chunks) = chunks();
        let logical = text(20, 200_000);
        let prepared = lv::prepare(LargeValueKind::String, &logical).unwrap();
        crate::options::block_on(store(
            &mut jazz_sql::native::Native(&conn),
            &chunks,
            &prepared,
            false,
        ))
        .unwrap();
        let append = |value: &LargeValueRef, suffix: &str| match lv::append_tail(
            value,
            suffix.as_bytes().to_vec(),
        )
        .unwrap()
        {
            lv::TailOutcome::Updated(value) => value,
            other => panic!("{other:?}"),
        };
        let base = prepared.value_ref.clone();
        let one = append(&base, "a");
        let two = append(&one, "b");
        let epochs = || {
            conn.query_row("SELECT count(*) FROM _jazz_lv_epoch", [], |r| {
                r.get::<_, i64>(0)
            })
            .unwrap()
        };
        let cell_two =
            crate::options::block_on(intern(&mut jazz_sql::native::Native(&conn), &chunks, &two))
                .unwrap();
        assert_eq!(cell_two % SPAN, 2);
        // A prefix of an epoch's edits names the same epoch.
        assert_eq!(
            crate::options::block_on(intern(&mut jazz_sql::native::Native(&conn), &chunks, &one))
                .unwrap(),
            cell_two - 1
        );
        assert_eq!(
            crate::options::block_on(intern(&mut jazz_sql::native::Native(&conn), &chunks, &base))
                .unwrap(),
            cell_two - 2
        );
        // An edit on its newest version extends it.
        let three = append(&two, "c");
        assert_eq!(
            crate::options::block_on(intern(
                &mut jazz_sql::native::Native(&conn),
                &chunks,
                &three
            ))
            .unwrap(),
            cell_two + 1
        );
        assert_eq!(epochs(), 1);
        // An edit on an older version forks a new epoch holding the shared
        // prefix; the old epoch keeps its edits.
        let fork = append(&one, "z");
        let cell_fork =
            crate::options::block_on(intern(&mut jazz_sql::native::Native(&conn), &chunks, &fork))
                .unwrap();
        assert_ne!(cell_fork / SPAN, cell_two / SPAN);
        assert_eq!(cell_fork % SPAN, 2);
        assert_eq!(epochs(), 2);
        for (cell, value) in [(cell_two + 1, &three), (cell_fork, &fork)] {
            assert_eq!(
                &crate::options::block_on(descriptor(
                    &mut jazz_sql::native::Native(&conn),
                    &chunks,
                    cell
                ))
                .unwrap(),
                value
            );
            let mut expected = logical.clone();
            expected.extend(
                value
                    .edit_tail
                    .iter()
                    .flat_map(|edit| edit.insert_bytes.clone()),
            );
            assert_eq!(
                crate::options::block_on(materialize(
                    &mut jazz_sql::native::Native(&conn),
                    &chunks,
                    SqlCell::Integer(cell)
                ))
                .unwrap(),
                expected
            );
        }
        // Epochs no cell names are freed by the next reclamation.
        crate::options::block_on(reclaim(&mut jazz_sql::native::Native(&conn), &chunks, 64))
            .unwrap();
        assert_eq!(epochs(), 0);
    }

    #[test]
    fn an_absent_chunk_is_demand_not_an_answer() {
        let (conn, chunks) = chunks();
        let Sql::Integer(cell) = chunked(&conn, &chunks, LargeValueKind::String, &text(3, 100_000))
        else {
            unreachable!()
        };
        let value = crate::options::block_on(descriptor(
            &mut jazz_sql::native::Native(&conn),
            &chunks,
            cell,
        ))
        .unwrap();
        let (empty_conn, empty) = self::chunks();
        assert_eq!(
            crate::options::block_on(value_bytes(
                &mut jazz_sql::native::Native(&empty_conn),
                &empty,
                &value
            ))
            .unwrap_err()
            .code,
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
            branch_key_names: Vec::new(),
            provenance: Vec::new(),
            stored_provenance: Vec::new(),
            latest_only: false,
        };
        let column = Column {
            name: "body".to_owned(),
            stored: "body".to_owned(),
            ty: crate::records::ValueType::String,
            default: None,
            json: false,
            json_type: None,
            variants: None,
            tags: None,
            merge: Default::default(),
        };
        conn.execute_batch(&triggers(&table, &[&column], None))
            .unwrap();
        let count = |sql: &str| conn.query_row(sql, [], |r| r.get::<_, i64>(0)).unwrap();
        let one = chunked(&conn, &chunks, LargeValueKind::String, &text(4, 400_000));
        let two = chunked(&conn, &chunks, LargeValueKind::String, &text(5, 400_000));
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
        while crate::options::block_on(reclaim(&mut jazz_sql::native::Native(&conn), &chunks, 3))
            .unwrap()
            > 0
        {}
        let left = count("SELECT count(*) FROM _jazz_lv_node");
        assert!(
            left > 0 && left < nodes,
            "only `two`'s tree goes: {left} of {nodes}"
        );
        conn.execute("DELETE FROM t__h", []).unwrap();
        while crate::options::block_on(reclaim(&mut jazz_sql::native::Native(&conn), &chunks, 3))
            .unwrap()
            > 0
        {}
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
            branch_key_names: Vec::new(),
            provenance: Vec::new(),
            stored_provenance: Vec::new(),
            latest_only: false,
        };
        let column = |name: &str| Column {
            name: name.to_owned(),
            stored: name.to_owned(),
            ty: crate::records::ValueType::String,
            default: None,
            json: false,
            json_type: None,
            variants: None,
            tags: None,
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
        let values: Vec<lv::PreparedLargeValue> = (7..10)
            .map(|seed| lv::prepare(LargeValueKind::String, &text(seed, 300_000)).unwrap())
            .collect();
        for value in &values {
            crate::options::block_on(store(
                &mut jazz_sql::native::Native(&conn),
                &chunks,
                value,
                false,
            ))
            .unwrap();
        }
        // A cell is interned right before the write that stores it.
        let cell = |i: usize| {
            Sql::Integer(
                crate::options::block_on(intern(
                    &mut jazz_sql::native::Native(&conn),
                    &chunks,
                    &values[i].value_ref,
                ))
                .unwrap(),
            )
        };
        // How many cells hold the value's tree: through its epochs, which
        // hold its root.
        let refs = |i: usize| {
            let refs: i64 = conn
                .query_row(
                    "SELECT coalesce(sum(refs), 0) FROM _jazz_lv_epoch WHERE root = ?1",
                    [values[i].value_ref.root.object_hash.as_bytes().as_slice()],
                    |r| r.get(0),
                )
                .unwrap();
            let roots: i64 = conn
                .query_row(
                    "SELECT coalesce(sum(refs), 0) FROM _jazz_lv_root WHERE root = ?1",
                    [values[i].value_ref.root.object_hash.as_bytes().as_slice()],
                    |r| r.get(0),
                )
                .unwrap();
            assert_eq!(roots, i64::from(refs > 0), "one epoch holds the root");
            refs
        };
        let (one, two, three) = (0, 1, 2);
        conn.execute(
            "INSERT INTO t (id, a, b) VALUES (1, ?1, ?2)",
            [&cell(one), &cell(two)],
        )
        .unwrap();
        assert_eq!((refs(one), refs(two)), (1, 1));
        // Swapping one column's tree releases the old and retains the new;
        // an unchanged column keeps its count.
        conn.execute("UPDATE t SET a = ?1, b = b WHERE id = 1", [&cell(three)])
            .unwrap();
        assert_eq!((refs(one), refs(two), refs(three)), (0, 1, 1));
        // Moving an epoch between columns never frees it in between.
        conn.execute("UPDATE t SET a = b, b = ?1 WHERE id = 1", [&cell(one)])
            .unwrap();
        assert_eq!((refs(one), refs(two), refs(three)), (1, 1, 0));
        conn.execute("UPDATE t SET b = a, a = ?1 WHERE id = 1", [&cell(three)])
            .unwrap();
        assert_eq!((refs(one), refs(two), refs(three)), (0, 1, 1));
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
        conn.execute("UPDATE t SET c = ?1 WHERE id = 1", [&cell(one)])
            .unwrap();
        assert_eq!((refs(one), refs(two), refs(three)), (1, 1, 1));
        conn.execute("DELETE FROM t WHERE id = 1", []).unwrap();
        assert_eq!((refs(one), refs(two), refs(three)), (0, 0, 0));
    }

    #[test]
    fn admission_checks_descriptors_against_their_tree() {
        let (conn, chunks) = chunks();
        let body = text(6, 300_000);
        let built = lv::prepare(LargeValueKind::String, &body).unwrap();
        // Pushed by a client: stored under receipts, never validated here.
        for chunk in &built.staged_chunks {
            crate::options::block_on(receive_pushed(
                &mut jazz_sql::native::Native(&conn),
                &chunks,
                &request_of(&chunk.node_ref),
                chunk.encoded.clone(),
                0,
                b"",
            ))
            .unwrap();
        }
        let mut forged = built.value_ref.clone();
        forged.byte_length += 1;
        assert!(
            crate::options::block_on(admit(
                &mut jazz_sql::native::Native(&conn),
                &chunks,
                &forged
            ))
            .is_err(),
            "dishonest metrics"
        );
        crate::options::block_on(admit(
            &mut jazz_sql::native::Native(&conn),
            &chunks,
            &built.value_ref,
        ))
        .unwrap();
        // Once validated, the root's base is trusted and checked directly.
        assert!(
            crate::options::block_on(admit(
                &mut jazz_sql::native::Native(&conn),
                &chunks,
                &forged
            ))
            .is_err()
        );
        let appended = match lv::append_tail(&built.value_ref, b"!".to_vec()).unwrap() {
            lv::TailOutcome::Updated(value) => value,
            other => panic!("{other:?}"),
        };
        crate::options::block_on(admit(
            &mut jazz_sql::native::Native(&conn),
            &chunks,
            &appended,
        ))
        .unwrap();

        // A tree with a node missing is refused.
        let other = lv::prepare(LargeValueKind::String, &text(7, 300_000)).unwrap();
        for chunk in other.staged_chunks.iter().skip(1) {
            crate::options::block_on(receive_pushed(
                &mut jazz_sql::native::Native(&conn),
                &chunks,
                &request_of(&chunk.node_ref),
                chunk.encoded.clone(),
                0,
                b"",
            ))
            .unwrap();
        }
        assert!(
            crate::options::block_on(admit(
                &mut jazz_sql::native::Native(&conn),
                &chunks,
                &other.value_ref
            ))
            .is_err()
        );
    }

    #[test]
    fn expired_receipts_release_unreferenced_pushes() {
        let (conn, chunks) = chunks();
        let built = lv::prepare(LargeValueKind::String, &text(8, 300_000)).unwrap();
        for chunk in &built.staged_chunks {
            crate::options::block_on(receive_pushed(
                &mut jazz_sql::native::Native(&conn),
                &chunks,
                &request_of(&chunk.node_ref),
                chunk.encoded.clone(),
                1_000,
                b"",
            ))
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
        crate::options::block_on(expire_receipts(&mut jazz_sql::native::Native(&conn), 999))
            .unwrap();
        while crate::options::block_on(reclaim(&mut jazz_sql::native::Native(&conn), &chunks, 64))
            .unwrap()
            > 0
        {}
        assert_eq!(count(), pushed, "receipts still hold the nodes");
        crate::options::block_on(expire_receipts(&mut jazz_sql::native::Native(&conn), 1_000))
            .unwrap();
        while crate::options::block_on(reclaim(&mut jazz_sql::native::Native(&conn), &chunks, 64))
            .unwrap()
            > 0
        {}
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
                crate::options::block_on(receive_pushed(
                    &mut jazz_sql::native::Native(&conn),
                    &chunks,
                    &request_of(&chunk.node_ref),
                    chunk.encoded.clone(),
                    1_000,
                    b"",
                ))
                .unwrap();
            }
        };
        let referenced = lv::prepare(LargeValueKind::String, &text(11, 300_000)).unwrap();
        let pending = lv::prepare(LargeValueKind::String, &text(12, 300_000)).unwrap();
        push(&referenced);
        push(&pending);
        let pending_nodes = pending.staged_chunks.len() as i64;
        assert!(receipts() > pending_nodes);
        crate::options::block_on(settle_receipts(
            &mut jazz_sql::native::Native(&conn),
            &referenced.value_ref.root.object_hash,
            b"",
        ))
        .unwrap();
        // Only the other push, still ahead of its commit, keeps receipts,
        // so expiring them later touches nothing the commit holds.
        assert_eq!(receipts(), pending_nodes);
    }

    /// Recording a base on a root that is already queued for reclamation
    /// leaves the queue as it is. An upsert's update runs the triggers it
    /// fires with ABORT, whatever conflict clause they name, so a trigger
    /// that fired here and inserted the root again would fail the write.
    #[test]
    fn recording_a_base_on_a_queued_root_keeps_the_queue() {
        let (conn, chunks) = chunks();
        let built = lv::prepare(LargeValueKind::String, &text(11, 300_000)).unwrap();
        // Two uploads of the same value; the first ends before the second
        // finishes, which queues the shared root.
        for id in [[1; 32], [2; 32]] {
            crate::options::block_on(begin_upload(
                &mut jazz_sql::native::Native(&conn),
                &id,
                1_000,
            ))
            .unwrap();
            crate::options::block_on(stage_upload_native(
                &conn,
                &chunks,
                &id,
                &built.staged_chunks,
                false,
            ))
            .unwrap();
        }
        crate::options::block_on(end_upload(&mut jazz_sql::native::Native(&conn), &[1; 32]))
            .unwrap();
        crate::options::block_on(finish_upload(
            &mut jazz_sql::native::Native(&conn),
            &built.value_ref,
        ))
        .unwrap();
        crate::options::block_on(finish_upload(
            &mut jazz_sql::native::Native(&conn),
            &built.value_ref,
        ))
        .unwrap();
        let queued: i64 = conn
            .query_row(
                "SELECT count(*) FROM _jazz_lv_reclaim WHERE root = ?1",
                [built.value_ref.root.object_hash.as_bytes().as_slice()],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!(queued, 1);
        // Losing the last reference still queues the root exactly once.
        let root = built.value_ref.root.object_hash.as_bytes().to_vec();
        conn.execute("UPDATE _jazz_lv_root SET refs = 1 WHERE root = ?1", [&root])
            .unwrap();
        conn.execute("UPDATE _jazz_lv_root SET refs = 0 WHERE root = ?1", [&root])
            .unwrap();
        let queued: i64 = conn
            .query_row(
                "SELECT count(*) FROM _jazz_lv_reclaim WHERE root = ?1",
                [&root],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!(queued, 1);
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
            crate::options::block_on(begin_upload(
                &mut jazz_sql::native::Native(&conn),
                &id,
                1_000,
            ))
            .unwrap();
            crate::options::block_on(stage_upload_native(
                &conn,
                &chunks,
                &id,
                &built.staged_chunks,
                false,
            ))
            .unwrap();
        }
        let staged = count();
        // An expiry pass before the leases age out keeps both uploads.
        assert_eq!(
            crate::options::block_on(expire_uploads(&mut jazz_sql::native::Native(&conn), 999))
                .unwrap(),
            0
        );
        while crate::options::block_on(reclaim(&mut jazz_sql::native::Native(&conn), &chunks, 64))
            .unwrap()
            > 0
        {}
        assert_eq!(count(), staged, "the leases hold every staged node");
        // The first upload's row commits; the second is abandoned.
        conn.execute(
            "INSERT INTO _jazz_lv_root (root, refs) VALUES (?1, 1)",
            [kept.value_ref.root.object_hash.as_bytes().as_slice()],
        )
        .unwrap();
        crate::options::block_on(end_upload(&mut jazz_sql::native::Native(&conn), &[1; 32]))
            .unwrap();
        assert!(
            crate::options::block_on(touch_upload(
                &mut jazz_sql::native::Native(&conn),
                &[2; 32],
                1_500
            ))
            .unwrap()
        );
        assert_eq!(
            crate::options::block_on(expire_uploads(&mut jazz_sql::native::Native(&conn), 1_500))
                .unwrap(),
            1
        );
        assert!(
            !crate::options::block_on(touch_upload(
                &mut jazz_sql::native::Native(&conn),
                &[2; 32],
                2_000
            ))
            .unwrap()
        );
        while crate::options::block_on(reclaim(&mut jazz_sql::native::Native(&conn), &chunks, 64))
            .unwrap()
            > 0
        {}
        assert_eq!(count(), kept.staged_chunks.len() as i64);
    }

    /// A file store with a directory chunk store, and a table `t` whose
    /// `body` cells are counted against their roots.
    struct Offloading {
        dir: std::path::PathBuf,
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
        let chunks = Chunks::new(true, Some(store.clone()), false);
        conn.execute_batch(DDL).unwrap();
        open_offload_native(&conn, &chunks).unwrap();
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
            branch_key_names: Vec::new(),
            provenance: Vec::new(),
            stored_provenance: Vec::new(),
            latest_only: false,
        };
        let column = Column {
            name: "body".to_owned(),
            stored: "body".to_owned(),
            ty: crate::records::ValueType::String,
            default: None,
            json: false,
            json_type: None,
            variants: None,
            tags: None,
            merge: Default::default(),
        };
        conn.execute_batch(&triggers(&table, &[&column], None))
            .unwrap();
        Offloading {
            dir,
            conn,
            chunks,
            store,
        }
    }

    impl Offloading {
        /// Store a chunked value in row `id`.
        fn insert(&self, id: i64, prepared: &lv::PreparedLargeValue) -> Sql {
            crate::options::block_on(store(
                &mut jazz_sql::native::Native(&self.conn),
                &self.chunks,
                prepared,
                false,
            ))
            .unwrap();
            let cell = Sql::Integer(
                crate::options::block_on(intern(
                    &mut jazz_sql::native::Native(&self.conn),
                    &self.chunks,
                    &prepared.value_ref,
                ))
                .unwrap(),
            );
            self.conn
                .execute(
                    "INSERT INTO t (id, body) VALUES (?1, ?2)",
                    rusqlite::params![id, cell],
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
                crate::options::block_on(offloaded(&mut jazz_sql::native::Native(&self.conn), key))
                    .unwrap();
            }
        }

        fn delete_pending(&self) -> Vec<NodeKey> {
            let pending = crate::options::block_on(pending_deletes(
                &mut jazz_sql::native::Native(&self.conn),
                1_000,
            ))
            .unwrap();
            for key in &pending {
                crate::options::block_on(self.store.delete(key)).unwrap();
                crate::options::block_on(deleted(&mut jazz_sql::native::Native(&self.conn), key))
                    .unwrap();
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
            while crate::options::block_on(reclaim(
                &mut jazz_sql::native::Native(&self.conn),
                &self.chunks,
                1_000,
            ))
            .unwrap()
                > 0
            {}
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
        let cell = o.insert(1, &prepared);
        let batch = begin_offload_native(&o.conn, 1_000).unwrap();
        let leaves = o.count(
            "SELECT count(*) FROM _jazz_lv_node n WHERE NOT EXISTS \
             (SELECT 1 FROM _jazz_lv_edge WHERE parent = n.hash)",
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
        let cold = Chunks::new(true, Some(o.store.clone()), false);
        assert_eq!(
            crate::options::block_on(materialize(
                &mut jazz_sql::native::Native(&o.conn),
                &cold,
                cell.as_ref()
            ))
            .unwrap(),
            logical
        );
        assert!(cold.take_demand().is_empty());
        // A second value keeps its leaves when the first goes.
        let other = text(12, 300_000);
        let kept = lv::prepare(LargeValueKind::String, &other).unwrap();
        o.insert(2, &kept);
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
        o.insert(1, &prepared);
        let batch = begin_offload_native(&o.conn, 1_000).unwrap();
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
        o.insert(1, &prepared);
        let first = begin_offload_native(&o.conn, 1_000).unwrap();
        o.put(&first);
        let again = begin_offload_native(&o.conn, 1_000).unwrap();
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
        o.insert(1, &prepared);
        let batch = begin_offload_native(&o.conn, 1_000).unwrap();
        o.put(&batch);
        o.drop_bytes(&batch);
        o.conn.execute("DELETE FROM t WHERE id = 1", []).unwrap();
        o.reclaim_all();
        assert_eq!(
            o.count("SELECT count(*) FROM _jazz_lv_offload_delete"),
            batch.len() as i64
        );
        let cell = o.insert(2, &prepared);
        assert!(o.delete_pending().is_empty());
        assert_eq!(
            keys(&begin_offload_native(&o.conn, 1_000).unwrap()),
            keys(&batch)
        );
        let cold = Chunks::new(true, Some(o.store.clone()), false);
        assert_eq!(
            crate::options::block_on(materialize(
                &mut jazz_sql::native::Native(&o.conn),
                &cold,
                cell.as_ref()
            ))
            .unwrap(),
            logical
        );
    }

    /// Leaves still held by an upload lease, a receipt or an unpushed mark
    /// stay in the file until they settle.
    #[test]
    fn leaves_still_being_staged_or_pushed_stay_in_the_file() {
        let o = offloading("held");
        let logical = text(16, 300_000);
        let prepared = lv::prepare(LargeValueKind::String, &logical).unwrap();
        crate::options::block_on(begin_upload(
            &mut jazz_sql::native::Native(&o.conn),
            &[7; 32],
            1_000,
        ))
        .unwrap();
        crate::options::block_on(stage_upload_native(
            &o.conn,
            &o.chunks,
            &[7; 32],
            &prepared.staged_chunks,
            true,
        ))
        .unwrap();
        assert!(begin_offload_native(&o.conn, 1_000).unwrap().is_empty());
        crate::options::block_on(end_upload(&mut jazz_sql::native::Native(&o.conn), &[7; 32]))
            .unwrap();
        assert!(
            begin_offload_native(&o.conn, 1_000).unwrap().is_empty(),
            "unpushed"
        );
        let hashes: Vec<lv::ContentHash> = prepared
            .staged_chunks
            .iter()
            .map(|chunk| chunk.node_ref.object_hash)
            .collect();
        crate::options::block_on(mark_pushed(&mut jazz_sql::native::Native(&o.conn), &hashes))
            .unwrap();
        assert!(!begin_offload_native(&o.conn, 1_000).unwrap().is_empty());
    }
}
