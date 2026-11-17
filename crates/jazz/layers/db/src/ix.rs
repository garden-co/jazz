//! Specialised indexes (SPEC 20): text, vector and geo indexes as shadow
//! tables in plain SQLite, kept in step with their rows inside the same
//! transaction.
//!
//! The framework here owns everything kinds share:
//!
//! - **Storage.** `_jazz_ix` records each index (kind, encoding, canonical
//!   declaration bytes, build state); each index's shadow tables are named
//!   `_jazz_ix_<stored table>__<index>_<part>`, rowid tables only.
//! - **Maintenance.** Three triggers per indexed table (`AFTER INSERT`,
//!   `AFTER DELETE`, `AFTER UPDATE OF` the indexed columns and `_state`)
//!   record changed `rid`s in `_jazz_ix_dirty`. They see every write to the
//!   current table, so no write site knows about indexes. [`drain`] turns
//!   the queue into shadow rows: before the outermost savepoint is
//!   released (see [`crate::values::end_savepoint`]) and before any read of
//!   a shadow table.
//! - **Builds.** A new or changed declaration is built in bounded batches
//!   ([`build_step`]), each its own transaction, with `state = 0` until done.
//! - **Queries.** A kind plans a search as a candidate relation
//!   `ix(rid, score)`, either SQL ([`Candidates::Sql`]) or scored in Rust
//!   over the rows the query's other clauses match ([`Candidates::Scored`]).
//!   Scores are smaller-better everywhere, so `$score` ascending is best
//!   first.
//!
//! A kind ([`Kind`]) only maps one row's indexed values to its shadow rows
//! and an op to its candidates: the pure parts live in `jazz-index`.

use std::cell::RefCell;
use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::rc::Rc;
use std::sync::Arc;

use rusqlite::Connection;
use rusqlite::types::ValueRef;

use crate::options::{Error, ErrorCode, Reason};
use crate::query::{IndexOp, IndexSearch, Predicate, Query};
use crate::records::Value;
use crate::values::{Column, Schema, Sql, Table, quote, storage};
use jazz_model::model::public_schema::{SpecialisedIndex, SpecialisedIndexKind};

/// The registry and the dirty queue.
pub(crate) const DDL: &str = "\
CREATE TABLE IF NOT EXISTS _jazz_ix (tbl TEXT NOT NULL, name TEXT NOT NULL, kind TEXT NOT NULL, \
 encoding TEXT NOT NULL, decl BLOB NOT NULL, state INTEGER NOT NULL, built INTEGER NOT NULL DEFAULT 0, \
 PRIMARY KEY (tbl, name));
CREATE TABLE IF NOT EXISTS _jazz_ix_dirty (tbl TEXT NOT NULL, rid INTEGER NOT NULL, PRIMARY KEY (tbl, rid));
CREATE TABLE IF NOT EXISTS _jazz_ix_incomplete (tbl TEXT NOT NULL, rid INTEGER NOT NULL, PRIMARY KEY (tbl, rid));";

/// `_jazz_ix.state` of an index still being built.
pub(crate) const STATE_BUILDING: i64 = 0;
/// `_jazz_ix.state` of a built index.
pub(crate) const STATE_READY: i64 = 1;

/// Rows a build batch indexes, each batch its own transaction.
pub(crate) const BUILD_BATCH: usize = 512;

/// Rows a drain reads from the queue at a time.
const DRAIN_BATCH: usize = 512;

/// Most candidate rows a Rust-scored search scores (SPEC 20.5).
pub(crate) const VECTOR_EXACT_MAX: usize = 50_000;

/// The name prefix of an index's shadow tables.
pub(crate) fn prefix(stored_table: &str, index: &str) -> String {
    format!("_jazz_ix_{stored_table}__{index}")
}

/// One shadow table of an index: `prefix` and `part`.
pub(crate) fn shadow(prefix: &str, part: &str) -> String {
    format!("{prefix}_{part}")
}

// ---------------------------------------------------------------------------
// Kinds
// ---------------------------------------------------------------------------

/// One indexed cell, as its kind reads it: the column's logical value
/// (a large value's whole text or bytes).
#[derive(Clone, Debug, PartialEq)]
pub(crate) enum Cell {
    Null,
    Text(String),
    Bytes(Vec<u8>),
    Real(f64),
    Integer(i64),
}

/// What a kind contributes: storage, maintenance and a query operator.
///
/// Implementations are pure functions of their inputs plus SQL over their
/// own shadow tables (`INV-IX-1`): no corpus statistics, no reads of other
/// rows. Every shadow table is a rowid table named [`shadow`]`(prefix,
/// part)`.
pub(crate) trait Kind {
    /// `_jazz_ix.encoding`: every byte-level choice the shadow rows depend
    /// on. A change rebuilds the index (`INV-IX-5`).
    fn encoding(&self) -> String;
    /// `CREATE TABLE IF NOT EXISTS` (and indexes) of the shadow tables.
    fn ddl(&self, prefix: &str) -> String;
    /// Replace row `rid`'s shadow rows by those of its indexed `cells`
    /// (in [`SpecialisedIndexKind::columns`] order), or delete them
    /// (`None`: the row is gone or deleted).
    fn write(
        &self,
        conn: &Connection,
        prefix: &str,
        rid: i64,
        cells: Option<&[Cell]>,
    ) -> Result<(), Error>;
    /// Plan `op` (already checked against the declaration by query
    /// validation): its candidate relation. `bind` binds a parameter and
    /// returns its placeholder.
    fn plan(
        &self,
        prefix: &str,
        op: &IndexOp,
        bind: &mut dyn FnMut(Sql) -> String,
    ) -> Result<Candidates, Error>;
}

/// A search's candidate relation `ix(rid, score)`, score smaller-better.
pub(crate) enum Candidates {
    /// The body of `ix(rid, score) AS (...)`, reading only the kind's
    /// shadow tables.
    Sql(String),
    /// Scored in Rust: the rows the query's other clauses and policy match
    /// (filter-first, at most [`VECTOR_EXACT_MAX`]), each with its payload
    /// from one shadow table, keep the best `k` by `(score, id)`.
    Scored(Box<dyn Scorer>),
}

/// The Rust step of a [`Candidates::Scored`] search.
pub(crate) trait Scorer {
    /// The shadow table part and column holding each row's payload.
    fn payload(&self) -> (&'static str, &'static str);
    /// How many rows to keep.
    fn k(&self) -> usize;
    /// One row's score (smaller is better), from its payload.
    fn score(&self, payload: ValueRef<'_>) -> Result<f64, Error>;
}

/// The kind that implements `index`, with `columns` its stored columns.
pub(crate) fn kind_for(
    index: &SpecialisedIndex,
    columns: &[&Column],
) -> Result<Box<dyn Kind>, Error> {
    #[cfg(any(test, feature = "testing"))]
    if let Some(kind) = test_kind::override_for(index, columns) {
        return Ok(kind);
    }
    Ok(match &index.kind {
        SpecialisedIndexKind::Text {
            fields,
            analyzer,
            prefix,
        } => Box::new(text::Text::new(
            analyzer,
            fields
                .values()
                .map(|field| (field.weight, field.length))
                .collect(),
            *prefix,
        )?),
        SpecialisedIndexKind::Vector { metric, .. } => {
            let dimensions = columns.first().and_then(|column| {
                match crate::values::unwrap_nullable_type(&column.ty) {
                    crate::records::ValueType::Vector(n) => Some(*n as usize),
                    _ => None,
                }
            });
            Box::new(vector::Vector::new(*metric, dimensions))
        }
        SpecialisedIndexKind::Geo { .. } => Box::new(geo::Geo),
    })
}

// ---------------------------------------------------------------------------
// Errors (SPEC 20.11)
// ---------------------------------------------------------------------------

fn typed(code: ErrorCode, reason: Reason, message: String) -> Error {
    let mut error = Error::new(code, message);
    error.reason = Some(reason);
    error
}

pub(crate) fn server_only(table: &str, index: &str) -> Error {
    typed(
        ErrorCode::Query,
        Reason::IndexServerOnly,
        format!("index `{table}.{index}` is server-only"),
    )
}

pub(crate) fn unsupported_upstream(table: &str, index: &str) -> Error {
    typed(
        ErrorCode::Query,
        Reason::IndexSearchUnsupportedUpstream,
        format!("the server does not support index search (index `{table}.{index}`)"),
    )
}

pub(crate) fn building(table: &str, index: &str) -> Error {
    typed(
        ErrorCode::Backpressure,
        Reason::IndexBuilding,
        format!("index `{table}.{index}` is still being built; retry later"),
    )
}

pub(crate) fn vector_unbounded(table: &str, index: &str) -> Error {
    typed(
        ErrorCode::Query,
        Reason::VectorSearchUnbounded,
        format!(
            "unfiltered vector search on the server needs an ANN index (`ann: \"ivf\"`), \
             not supported yet (index `{table}.{index}`: filter the query by an indexed column)"
        ),
    )
}

pub(crate) fn candidates_over_cap(table: &str, index: &str) -> Error {
    typed(
        ErrorCode::Query,
        Reason::VectorCandidatesOverCap,
        format!(
            "a search on index `{table}.{index}` would score more than {VECTOR_EXACT_MAX} \
             candidate rows; narrow the query's other filters"
        ),
    )
}

fn search_error(table: &str, index: &str, detail: impl std::fmt::Display) -> Error {
    Error::new(
        ErrorCode::Query,
        format!("search on {table}.{index}: {detail}"),
    )
}

// ---------------------------------------------------------------------------
// The registry
// ---------------------------------------------------------------------------

/// One maintained index.
pub(crate) struct Index {
    /// The stored table it indexes.
    pub stored: String,
    pub decl: SpecialisedIndex,
    pub prefix: String,
    pub kind: Box<dyn Kind>,
    /// The indexed stored columns, in [`SpecialisedIndexKind::columns`]
    /// order.
    pub columns: Vec<Column>,
}

/// The indexes a store maintains, by stored table.
#[derive(Default)]
pub(crate) struct Indexes {
    pub by_table: BTreeMap<String, Vec<Rc<Index>>>,
    /// Every declared index, maintained or not (server-only ones on a
    /// client), by logical table and name.
    pub declared: BTreeMap<(String, String), SpecialisedIndex>,
    pub chunks: Option<Arc<crate::large::Chunks>>,
    /// The schema version and replica-ness [`reconcile`] last ran for.
    pub reconciled: Option<(crate::ids::SchemaVersionId, bool)>,
}

thread_local! {
    /// By connection (see [`crate::values`]'s undo logs): the indexes it
    /// maintains, which [`drain`] needs where only the connection is at
    /// hand (releasing the outermost savepoint).
    static REGISTRY: RefCell<HashMap<usize, Rc<RefCell<Indexes>>>> = RefCell::default();
}

fn conn_key(conn: &Connection) -> usize {
    // SAFETY: the handle is only compared, never dereferenced.
    unsafe { conn.handle() as usize }
}

/// `conn`'s registry, created empty on first use.
pub(crate) fn registry(conn: &Connection) -> Rc<RefCell<Indexes>> {
    REGISTRY.with(|registry| {
        registry
            .borrow_mut()
            .entry(conn_key(conn))
            .or_default()
            .clone()
    })
}

fn registered(conn: &Connection) -> Option<Rc<RefCell<Indexes>>> {
    REGISTRY.with(|registry| registry.borrow().get(&conn_key(conn)).cloned())
}

/// Drop `conn`'s registry as it closes.
pub(crate) fn forget(conn: &Connection) {
    REGISTRY.with(|registry| registry.borrow_mut().remove(&conn_key(conn)));
}

// ---------------------------------------------------------------------------
// Reconciling declarations with the store
// ---------------------------------------------------------------------------

struct Recorded {
    kind: String,
    encoding: String,
    decl: Vec<u8>,
}

fn recorded(conn: &Connection) -> Result<BTreeMap<(String, String), Recorded>, Error> {
    let mut statement = conn
        .prepare("SELECT tbl, name, kind, encoding, decl FROM _jazz_ix")
        .map_err(storage)?;
    let rows = statement
        .query_map([], |r| {
            Ok((
                (r.get::<_, String>(0)?, r.get::<_, String>(1)?),
                Recorded {
                    kind: r.get(2)?,
                    encoding: r.get(3)?,
                    decl: r.get(4)?,
                },
            ))
        })
        .map_err(storage)?;
    rows.collect::<Result<_, _>>().map_err(storage)
}

/// Each kind's shadow table parts (SPEC 20.2), by `_jazz_ix.kind`: what
/// dropping an index drops, whatever kind implements it now.
fn kind_parts(kind: &str) -> &'static [&'static str] {
    match kind {
        "text" => &["terms", "post"],
        "vector" => &["vec"],
        "geo" => &["geo"],
        _ => &[],
    }
}

/// Bring the store's indexes in line with `schema` (the current schema,
/// under its stored names): drop removed ones, (re)create new and changed
/// ones as building, and (re)write each indexed table's triggers. A
/// client (`replica`) leaves server-only indexes out. Runs inside the
/// caller's transaction; builds happen later, in [`build_step`].
pub(crate) fn reconcile(conn: &Connection, schema: &Schema, replica: bool) -> Result<(), Error> {
    let registry = registry(conn);
    let mut desired: BTreeMap<(String, String), Rc<Index>> = BTreeMap::new();
    let mut declared = BTreeMap::new();
    for table in schema.tables.values() {
        let Some(jazz_table) = schema.jazz.tables().iter().find(|t| t.name == table.name) else {
            continue;
        };
        for decl in &jazz_table.specialised_indexes {
            declared.insert((table.name.clone(), decl.name.clone()), decl.clone());
            if replica && decl.server_only {
                continue;
            }
            let columns: Vec<Column> = decl
                .kind
                .columns()
                .into_iter()
                .map(|name| {
                    table.column(name).cloned().ok_or_else(|| {
                        Error::new(
                            ErrorCode::Schema,
                            format!("index {}.{}: no column {name}", table.name, decl.name),
                        )
                    })
                })
                .collect::<Result<_, _>>()?;
            let kind = kind_for(decl, &columns.iter().collect::<Vec<_>>())?;
            desired.insert(
                (table.stored.clone(), decl.name.clone()),
                Rc::new(Index {
                    stored: table.stored.clone(),
                    prefix: prefix(&table.stored, &decl.name),
                    decl: decl.clone(),
                    kind,
                    columns,
                }),
            );
        }
    }
    // A store whose schemas never declared a specialised index gets none of
    // the framework's tables: opening it creates no schema objects.
    if desired.is_empty() && !framework_exists(conn)? {
        let mut registry = registry.borrow_mut();
        registry.by_table = BTreeMap::new();
        registry.declared = declared;
        return Ok(());
    }
    crate::db::execute_script(conn, DDL)?;
    let recorded = recorded(conn)?;
    let mut touched_tables = BTreeSet::new();
    for ((tbl, name), old) in &recorded {
        let keep = desired
            .get(&(tbl.clone(), name.clone()))
            .is_some_and(|index| {
                old.kind == index.decl.kind.kind_name().as_str()
                    && old.encoding == index.kind.encoding()
                    && old.decl == stored_decl_bytes(index)
            });
        if keep {
            continue;
        }
        // Removed or changed: its shadow tables go (a changed one is
        // recreated below and rebuilt).
        let prefix = prefix(tbl, name);
        let mut ddl = String::new();
        for part in kind_parts(&old.kind) {
            ddl.push_str(&format!(
                "DROP TABLE IF EXISTS {};\n",
                quote(&shadow(&prefix, part))
            ));
        }
        crate::db::execute_script(conn, &ddl)?;
        conn.execute(
            "DELETE FROM _jazz_ix WHERE tbl = ?1 AND name = ?2",
            [tbl, name],
        )
        .map_err(storage)?;
        touched_tables.insert(tbl.clone());
    }
    for ((tbl, name), index) in &desired {
        let unchanged = recorded
            .get(&(tbl.clone(), name.clone()))
            .is_some_and(|old| {
                old.encoding == index.kind.encoding() && old.decl == stored_decl_bytes(index)
            });
        if unchanged {
            continue;
        }
        crate::db::execute_script(conn, &index.kind.ddl(&index.prefix))?;
        // An empty table's index is built as soon as it exists.
        let empty: bool = conn
            .query_row(
                &format!("SELECT NOT EXISTS (SELECT 1 FROM {})", quote(tbl)),
                [],
                |r| r.get(0),
            )
            .map_err(storage)?;
        conn.execute(
            "INSERT INTO _jazz_ix (tbl, name, kind, encoding, decl, state, built) \
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, -9223372036854775808)",
            rusqlite::params![
                tbl,
                name,
                index.decl.kind.kind_name().as_str(),
                index.kind.encoding(),
                stored_decl_bytes(index),
                if empty { STATE_READY } else { STATE_BUILDING }
            ],
        )
        .map_err(storage)?;
        touched_tables.insert(tbl.clone());
    }
    let mut by_table: BTreeMap<String, Vec<Rc<Index>>> = BTreeMap::new();
    for ((tbl, _), index) in desired {
        by_table.entry(tbl).or_default().push(index);
    }
    conn.execute_batch(
        "DELETE FROM _jazz_ix_incomplete WHERE tbl NOT IN (SELECT tbl FROM _jazz_ix)",
    )
    .map_err(storage)?;
    // Triggers: every table that has (or had) an index gets the set its
    // indexes need now, or none.
    let existing: Vec<String> = conn
        .prepare(
            "SELECT DISTINCT tbl_name FROM sqlite_master WHERE type = 'trigger' \
             AND name LIKE '%\\_\\_jazz\\_ix\\_%' ESCAPE '\\'",
        )
        .and_then(|mut s| s.query_map([], |r| r.get(0))?.collect())
        .map_err(storage)?;
    let mut ddl = String::new();
    for tbl in existing
        .iter()
        .chain(by_table.keys())
        .collect::<BTreeSet<_>>()
    {
        ddl.push_str(&triggers(tbl, by_table.get(tbl).map(Vec::as_slice)));
        if !by_table.contains_key(tbl) {
            // Nothing indexes it any more: its queued rows have no index.
            conn.execute("DELETE FROM _jazz_ix_dirty WHERE tbl = ?1", [tbl])
                .map_err(storage)?;
        }
    }
    crate::db::execute_script(conn, &ddl)?;
    let _ = touched_tables;
    let mut registry = registry.borrow_mut();
    registry.by_table = by_table;
    registry.declared = declared;
    Ok(())
}

/// Whether this store has the framework's tables (some schema it opened
/// declared a specialised index).
fn framework_exists(conn: &Connection) -> Result<bool, Error> {
    conn.query_row(
        "SELECT EXISTS (SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = '_jazz_ix')",
        [],
        |r| r.get(0),
    )
    .map_err(storage)
}

/// The canonical declaration bytes `_jazz_ix.decl` records: the
/// declaration with each column under its stored name, so a column
/// rename that keeps the stored column keeps the index.
fn stored_decl_bytes(index: &Index) -> Vec<u8> {
    let logical = index.decl.kind.columns();
    let mut decl = index.decl.clone();
    decl.kind = index.decl.kind.map_columns(|name| {
        let at = logical.iter().position(|c| *c == name).expect("own column");
        index.columns[at].stored.clone()
    });
    decl.canonical_bytes()
}

/// The three queue triggers on stored table `tbl` for `indexes` (dropped
/// and recreated, so the watched columns follow the declarations), or
/// just the drops when nothing indexes it.
fn triggers(tbl: &str, indexes: Option<&[Rc<Index>]>) -> String {
    let name = |what: &str| quote(&format!("{tbl}__jazz_ix_{what}"));
    let mut sql = String::new();
    for what in ["ins", "del", "upd"] {
        sql.push_str(&format!("DROP TRIGGER IF EXISTS {};\n", name(what)));
    }
    let Some(indexes) = indexes.filter(|i| !i.is_empty()) else {
        return sql;
    };
    let mut watched: BTreeSet<String> = indexes
        .iter()
        .flat_map(|index| index.columns.iter().map(|c| c.stored.clone()))
        .collect();
    watched.insert("_state".into());
    let watched: Vec<String> = watched.iter().map(|c| quote(c)).collect();
    let on = quote(tbl);
    let table = Sql::Text(tbl.as_bytes().to_vec());
    let table = crate::values::sql_literal(Some(table)).expect("text literal");
    let enqueue = |row: &str| {
        format!("INSERT OR IGNORE INTO _jazz_ix_dirty (tbl, rid) VALUES ({table}, {row}.rid);")
    };
    sql.push_str(&format!(
        "CREATE TRIGGER {} AFTER INSERT ON {on} BEGIN {} END;\n\
         CREATE TRIGGER {} AFTER DELETE ON {on} BEGIN {} END;\n\
         CREATE TRIGGER {} AFTER UPDATE OF {} ON {on} BEGIN {} {} END;\n",
        name("ins"),
        enqueue("NEW"),
        name("del"),
        enqueue("OLD"),
        name("upd"),
        watched.join(", "),
        enqueue("OLD"),
        enqueue("NEW"),
    ));
    sql
}

// ---------------------------------------------------------------------------
// Draining and building
// ---------------------------------------------------------------------------

/// Index every queued row of `conn`'s store (`INV-IX-2`): each `rid` gets
/// its indexes' shadow rows recomputed from its current values (a missing
/// or deleted row has none), then leaves the queue. Called inside the
/// transaction that queued them.
pub(crate) fn drain(conn: &Connection) -> Result<(), Error> {
    let Some(registry) = registered(conn) else {
        return Ok(());
    };
    let registry = registry.borrow();
    if registry.by_table.is_empty() {
        return Ok(());
    }
    loop {
        let batch: Vec<(String, i64)> = conn
            .prepare_cached("SELECT tbl, rid FROM _jazz_ix_dirty LIMIT ?1")
            .and_then(|mut s| {
                s.query_map([DRAIN_BATCH as i64], |r| Ok((r.get(0)?, r.get(1)?)))?
                    .collect()
            })
            .map_err(storage)?;
        if batch.is_empty() {
            return Ok(());
        }
        for (tbl, rid) in &batch {
            if let Some(indexes) = registry.by_table.get(tbl) {
                for index in indexes {
                    write_row(conn, &registry, index, *rid)?;
                }
            }
            conn.prepare_cached("DELETE FROM _jazz_ix_dirty WHERE tbl = ?1 AND rid = ?2")
                .and_then(|mut s| s.execute(rusqlite::params![tbl, rid]))
                .map_err(storage)?;
        }
    }
}

/// Whether `conn` has queued rows its indexes have not taken in yet.
pub(crate) fn has_queued(conn: &Connection) -> Result<bool, Error> {
    if registered(conn).is_none_or(|r| r.borrow().by_table.is_empty()) {
        return Ok(false);
    }
    conn.prepare_cached("SELECT EXISTS (SELECT 1 FROM _jazz_ix_dirty)")
        .and_then(|mut s| s.query_row([], |r| r.get(0)))
        .map_err(storage)
}

/// Recompute `index`'s shadow rows for row `rid`.
fn write_row(conn: &Connection, registry: &Indexes, index: &Index, rid: i64) -> Result<(), Error> {
    let columns: String = index
        .columns
        .iter()
        .map(|c| format!(", {}", quote(&c.stored)))
        .collect();
    let sql = format!(
        "SELECT _state{columns} FROM {} WHERE rid = ?1",
        quote(&index.stored)
    );
    let mut statement = conn.prepare_cached(&sql).map_err(storage)?;
    let mut rows = statement.query([rid]).map_err(storage)?;
    let mut incomplete = false;
    let cells = match rows.next().map_err(storage)? {
        None => None,
        Some(row) => {
            let state: i64 = row.get(0).map_err(storage)?;
            if state & crate::values::STATE_DELETED != 0 {
                None
            } else {
                let mut cells = Vec::with_capacity(index.columns.len());
                for (i, column) in index.columns.iter().enumerate() {
                    let cell = row.get_ref(i + 1).map_err(storage)?;
                    match read_cell(registry, column, cell)? {
                        Some(cell) => cells.push(cell),
                        None => {
                            incomplete = true;
                            cells.push(Cell::Null);
                        }
                    }
                }
                Some(cells)
            }
        }
    };
    drop(rows);
    if incomplete {
        conn.prepare_cached("INSERT OR IGNORE INTO _jazz_ix_incomplete (tbl, rid) VALUES (?1, ?2)")
            .and_then(|mut s| s.execute(rusqlite::params![index.stored, rid]))
            .map_err(storage)?;
    }
    index.kind.write(conn, &index.prefix, rid, cells.as_deref())
}

/// Chunks arrived on this replica: every row indexed while one of its
/// values was missing them is queued again, so it is indexed by its whole
/// value (`INV-IX-1`). One still missing some is recorded again.
pub(crate) fn chunks_arrived(conn: &Connection) -> Result<(), Error> {
    if registered(conn).is_none_or(|r| r.borrow().by_table.is_empty()) {
        return Ok(());
    }
    let any: bool = conn
        .prepare_cached("SELECT EXISTS (SELECT 1 FROM _jazz_ix_incomplete)")
        .and_then(|mut s| s.query_row([], |r| r.get(0)))
        .map_err(storage)?;
    if !any {
        return Ok(());
    }
    conn.execute_batch(
        "INSERT OR IGNORE INTO _jazz_ix_dirty (tbl, rid) SELECT tbl, rid FROM _jazz_ix_incomplete; \
         DELETE FROM _jazz_ix_incomplete;",
    )
    .map_err(storage)
}

/// A stored cell as its kind reads it: the logical value, a large value
/// read whole (SPEC 19). `None` is a large value whose chunks this replica
/// has not received yet: the row is indexed without it for now and again
/// when they arrive ([`chunks_arrived`]).
fn read_cell(
    registry: &Indexes,
    column: &Column,
    cell: ValueRef<'_>,
) -> Result<Option<Cell>, Error> {
    read_whole(registry, column, cell).map_or_else(
        |error| {
            if crate::large::is_missing(&error) {
                Ok(None)
            } else {
                Err(error)
            }
        },
        |cell| Ok(Some(cell)),
    )
}

fn read_whole(registry: &Indexes, column: &Column, cell: ValueRef<'_>) -> Result<Cell, Error> {
    let value = match &registry.chunks {
        Some(chunks) => crate::values::column_from_sql(column, cell, chunks)?,
        None => None,
    };
    let mut value = match value {
        Some(value) => value,
        None => {
            return Ok(match cell {
                ValueRef::Null => Cell::Null,
                ValueRef::Integer(i) => Cell::Integer(i),
                ValueRef::Real(r) => Cell::Real(r),
                ValueRef::Text(t) => Cell::Text(String::from_utf8_lossy(t).into_owned()),
                ValueRef::Blob(b) => Cell::Bytes(b.to_vec()),
            });
        }
    };
    loop {
        return Ok(match value {
            Value::Nullable(None) => Cell::Null,
            Value::Nullable(Some(inner)) => {
                value = *inner;
                continue;
            }
            Value::String(text) => Cell::Text(text),
            Value::Bytes(bytes) => Cell::Bytes(bytes),
            Value::F64(x) => Cell::Real(x),
            Value::I64(i) => Cell::Integer(i),
            Value::I32(i) => Cell::Integer(i.into()),
            Value::U8(i) => Cell::Integer(i.into()),
            Value::U16(i) => Cell::Integer(i.into()),
            Value::U32(i) => Cell::Integer(i.into()),
            _ => match cell {
                ValueRef::Blob(b) => Cell::Bytes(b.to_vec()),
                ValueRef::Text(t) => Cell::Text(String::from_utf8_lossy(t).into_owned()),
                ValueRef::Real(r) => Cell::Real(r),
                ValueRef::Integer(i) => Cell::Integer(i),
                ValueRef::Null => Cell::Null,
            },
        });
    }
}

/// One build batch: the first index still building indexes its next
/// [`BUILD_BATCH`] rows by `rid`. Returns whether any index is still
/// building. The caller runs each step in its own transaction.
pub(crate) fn build_step(conn: &Connection) -> Result<bool, Error> {
    let Some(registry) = registered(conn) else {
        return Ok(false);
    };
    let registry = registry.borrow();
    if registry.by_table.is_empty() {
        return Ok(false);
    }
    let building: Option<(String, String, i64)> = conn
        .prepare_cached(
            "SELECT tbl, name, built FROM _jazz_ix WHERE state = ?1 ORDER BY tbl, name LIMIT 1",
        )
        .and_then(|mut s| {
            let mut rows = s.query([STATE_BUILDING])?;
            rows.next()?
                .map(|r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)))
                .transpose()
        })
        .map_err(storage)?;
    let Some((tbl, name, built)) = building else {
        return Ok(false);
    };
    let Some(index) = registry
        .by_table
        .get(&tbl)
        .and_then(|indexes| indexes.iter().find(|i| i.decl.name == name))
    else {
        // Recorded but not maintained here: reconcile drops it.
        return Ok(false);
    };
    let rids: Vec<i64> = conn
        .prepare_cached(&format!(
            "SELECT rid FROM {} WHERE rid > ?1 ORDER BY rid LIMIT ?2",
            quote(&tbl)
        ))
        .and_then(|mut s| {
            s.query_map(rusqlite::params![built, BUILD_BATCH as i64], |r| r.get(0))?
                .collect()
        })
        .map_err(storage)?;
    for rid in &rids {
        write_row(conn, &registry, index, *rid)?;
    }
    let done = rids.len() < BUILD_BATCH;
    conn.execute(
        "UPDATE _jazz_ix SET built = ?3, state = ?4 WHERE tbl = ?1 AND name = ?2",
        rusqlite::params![
            tbl,
            name,
            rids.last().copied().unwrap_or(built),
            if done { STATE_READY } else { STATE_BUILDING }
        ],
    )
    .map_err(storage)?;
    if !done {
        return Ok(true);
    }
    let more: bool = conn
        .query_row(
            "SELECT EXISTS (SELECT 1 FROM _jazz_ix WHERE state = ?1)",
            [STATE_BUILDING],
            |r| r.get(0),
        )
        .map_err(storage)?;
    Ok(more)
}

/// Whether `index` on stored table `tbl` is built.
pub(crate) fn is_ready(conn: &Connection, tbl: &str, index: &str) -> Result<bool, Error> {
    conn.prepare_cached("SELECT state FROM _jazz_ix WHERE tbl = ?1 AND name = ?2")
        .and_then(|mut s| {
            let mut rows = s.query([tbl, index])?;
            rows.next()?.map(|r| r.get::<_, i64>(0)).transpose()
        })
        .map_err(storage)
        .map(|state| state == Some(STATE_READY))
}

// ---------------------------------------------------------------------------
// Queries
// ---------------------------------------------------------------------------

/// The query's search, if it has one: a top-level filter (validation
/// refuses it anywhere else, and more than one).
pub(crate) fn search_of(query: &Query) -> Option<&IndexSearch> {
    query.filters.iter().find_map(|predicate| match predicate {
        Predicate::Search(search) => Some(search),
        _ => None,
    })
}

/// What a compiled search reads for `ix(rid, score)`.
#[derive(Clone)]
pub(crate) enum Planned {
    /// The kind's SQL candidates, planned while compiling.
    Sql { index: Rc<Index>, op: IndexOp },
    /// Rust-scored candidates, best first: `(rid, score key)`.
    Bound(Vec<(i64, i64)>),
}

/// The maintained index `search` reads on logical table `table`, after
/// the checks only the store can make: a declared index this node does
/// not maintain is server-only.
pub(crate) fn index_for(
    conn: &Connection,
    table: &Table,
    search: &IndexSearch,
) -> Result<Rc<Index>, Error> {
    let registry = registry(conn);
    let registry = registry.borrow();
    if let Some(index) = registry
        .by_table
        .get(&table.stored)
        .and_then(|indexes| indexes.iter().find(|i| i.decl.name == search.index))
    {
        return Ok(index.clone());
    }
    match registry
        .declared
        .get(&(table.name.clone(), search.index.clone()))
    {
        Some(decl) if decl.server_only => Err(server_only(&table.name, &search.index)),
        _ => Err(search_error(
            &table.name,
            &search.index,
            "this store does not maintain the index (its schema predates it)",
        )),
    }
}

/// The order-preserving integer of a Rust score: smaller scores give
/// smaller keys, for every finite `f64` (and `-0.0` = `0.0`).
pub(crate) fn score_key(score: f64) -> i64 {
    let score = if score == 0.0 { 0.0 } else { score };
    let bits = score.to_bits() as i64;
    // Negative floats order backwards as integers: flip their magnitude.
    if bits < 0 { bits ^ i64::MAX } else { bits }
}

/// The best `k` of scored candidates by `(score, id)`.
pub(crate) fn top_k(mut scored: Vec<(f64, Vec<u8>, i64)>, k: usize) -> Vec<(i64, i64)> {
    scored.sort_by(|a, b| a.0.total_cmp(&b.0).then_with(|| a.1.cmp(&b.1)));
    scored.truncate(k);
    scored
        .into_iter()
        .map(|(score, _, rid)| (rid, score_key(score)))
        .collect()
}

/// `ix(rid, score)` over bound candidates (see [`Planned::Bound`]).
pub(crate) fn bound_cte(candidates: &[(i64, i64)], bind: &mut dyn FnMut(Sql) -> String) -> String {
    let json = serde_json::to_string(candidates).expect("integer pairs serialize");
    let mark = bind(Sql::Text(json.into_bytes()));
    format!(
        "SELECT CAST(json_extract(value, '$[0]') AS INTEGER) AS rid, \
         CAST(json_extract(value, '$[1]') AS INTEGER) AS score FROM json_each({mark})"
    )
}

/// Whether `decl` is scored in Rust (vector) rather than in SQL.
pub(crate) fn is_scored(decl: &SpecialisedIndexKind) -> bool {
    matches!(decl, SpecialisedIndexKind::Vector { .. })
}

mod geo;
#[cfg(any(test, feature = "testing"))]
pub(crate) mod test_kind;
mod text;
mod vector;
