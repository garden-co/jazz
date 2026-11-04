//! Pending uploads in tables shaped like the rows they write, not as
//! encoded sync messages.
//!
//! A commit a replica has not yet had settled lives in `_jazz_pending` and
//! its child tables until its fate comes back. That can be across an
//! upgrade, so the layout must not depend on how the commit travels: a
//! change to [`SyncMessage`] or [`WireOp`] would otherwise strand every
//! offline write a user still holds. Each part of a commit is a row with
//! named columns, and each written cell is one row whose value is a native
//! SQLite value tagged with the [`Value`] kind it holds, as the main tables
//! keep cells. Only the parts that have no flat shape (branch targets,
//! bases, claims, a serializable read's query and binding, and composite
//! cell values) are JSON with named fields.
//!
//! - `_jazz_pending`: one row per commit: its writer's `(node, seq)`, its
//!   HLC `clock`, a serializable commit's `snapshot` and `own` list (both
//!   NULL for a mergeable one), and who it acts as (`acting`, `author`,
//!   `claims`).
//! - `_jazz_pending_ops`: one row per op `n`: what it does (`op`), the
//!   stored table and row id it writes, its branch `target` (or a move's
//!   `move_from` and `move_to`), its explicit timestamp `at`, its `bases`,
//!   and its access proof (`proof_key`, `proof_seq`). `has_cells` is 0 for
//!   an op without a cell list (a delete, a move, a restore of the row as
//!   it was).
//! - `_jazz_pending_cells`: one row per cell `col` of op `n`'s `list` (0:
//!   its cells, 1: an upsert's `created` cells), its kind `ty`, the number
//!   of present-nullable wrappers around it (`nulls`), and its `value`.
//! - `_jazz_pending_deltas`: one row per chunked cell `i` of op `n` that
//!   travels as edits.
//! - `_jazz_pending_reads` and `_jazz_pending_read_rows`: a serializable
//!   commit's reads and the row versions each listed.
//!
//! A refused commit's ops move to `_jazz_rejected_ops`,
//! `_jazz_rejected_cells` and `_jazz_rejected_deltas`, in the same layout.

use super::*;

// The columns of the op tables, which the retry store shares.
macro_rules! ops_columns {
    () => {
        "(tx INTEGER NOT NULL, n INTEGER NOT NULL, op TEXT NOT NULL, tbl TEXT NOT NULL, \
         id BLOB NOT NULL, target TEXT, move_from TEXT, move_to TEXT, at INTEGER, \
         has_cells INTEGER NOT NULL, bases TEXT NOT NULL, proof_key BLOB, proof_seq INTEGER, \
         PRIMARY KEY (tx, n))"
    };
}
macro_rules! cells_columns {
    () => {
        "(tx INTEGER NOT NULL, n INTEGER NOT NULL, list INTEGER NOT NULL, col TEXT NOT NULL, \
         ty TEXT NOT NULL, nulls INTEGER NOT NULL, value, PRIMARY KEY (tx, n, list, col))"
    };
}
macro_rules! deltas_columns {
    () => {
        "(tx INTEGER NOT NULL, n INTEGER NOT NULL, i INTEGER NOT NULL, col TEXT NOT NULL, \
         depth INTEGER NOT NULL, key BLOB NOT NULL, seq INTEGER NOT NULL, base INTEGER NOT NULL, \
         edits BLOB NOT NULL, PRIMARY KEY (tx, n, i))"
    };
}

/// Where pending uploads live. `_jazz_pending_rows` (the local rows a
/// commit wrote) and the trigger that forgets a settled commit's parts are
/// in [`SYNC_DDL`] beside these.
pub(crate) const PENDING_DDL: &str = concat!(
    "CREATE TABLE IF NOT EXISTS _jazz_pending (tx INTEGER PRIMARY KEY, node BLOB NOT NULL, \
     seq INTEGER NOT NULL, clock INTEGER NOT NULL, snapshot INTEGER, own TEXT, \
     acting TEXT NOT NULL, author TEXT, claims TEXT);",
    "CREATE TABLE IF NOT EXISTS _jazz_pending_ops ",
    ops_columns!(),
    ";",
    "CREATE TABLE IF NOT EXISTS _jazz_pending_cells ",
    cells_columns!(),
    ";",
    "CREATE TABLE IF NOT EXISTS _jazz_pending_deltas ",
    deltas_columns!(),
    ";",
    "CREATE TABLE IF NOT EXISTS _jazz_pending_reads (tx INTEGER NOT NULL, n INTEGER NOT NULL, \
     version BLOB NOT NULL, query TEXT NOT NULL, binding TEXT NOT NULL, \
     include_deleted INTEGER NOT NULL, PRIMARY KEY (tx, n));",
    "CREATE TABLE IF NOT EXISTS _jazz_pending_read_rows (tx INTEGER NOT NULL, n INTEGER NOT NULL, \
     i INTEGER NOT NULL, id BLOB NOT NULL, node BLOB, t INTEGER NOT NULL, PRIMARY KEY (tx, n, i));",
);

/// The retry store's copy of a refused commit's ops, in the op tables'
/// layout.
pub(crate) const REJECTED_OPS_DDL: [&str; 3] = [
    concat!(
        "CREATE TABLE IF NOT EXISTS _jazz_rejected_ops ",
        ops_columns!()
    ),
    concat!(
        "CREATE TABLE IF NOT EXISTS _jazz_rejected_cells ",
        cells_columns!()
    ),
    concat!(
        "CREATE TABLE IF NOT EXISTS _jazz_rejected_deltas ",
        deltas_columns!()
    ),
];

/// The op tables, by suffix after `_jazz_pending_` or `_jazz_rejected_`.
pub(crate) const OP_TABLES: [&str; 3] = ["ops", "cells", "deltas"];

/// A stored part that does not read back is another layout's: refused as
/// an unsupported storage format, never misread.
fn bad(detail: impl std::fmt::Display) -> Error {
    crate::format::unsupported(format_args!("a pending upload {detail}"))
}

fn to_json<T: Serialize>(value: &T) -> Result<String, Error> {
    serde_json::to_string(value).map_err(|error| {
        Error::new(
            ErrorCode::Storage,
            format!("encoding a pending upload: {error}"),
        )
    })
}

fn from_json<T: serde::de::DeserializeOwned>(what: &str, json: &str) -> Result<T, Error> {
    serde_json::from_str(json)
        .map_err(|error| bad(format_args!("{what} does not decode ({error})")))
}

/// JSON for `value` that reads back as exactly `value`. JSON has no NaN or
/// infinity, which `serde_json` would write as `null`: such a value is
/// refused when written rather than read back as something else.
fn exact_json<T: Serialize + serde::de::DeserializeOwned + PartialEq>(
    what: &str,
    value: &T,
) -> Result<String, Error> {
    let json = to_json(value)?;
    if serde_json::from_str::<T>(&json).ok().as_ref() != Some(value) {
        return Err(crate::values::unsupported(format_args!(
            "storing {what} that holds NaN or an infinite number in a pending upload"
        )));
    }
    Ok(json)
}

fn blob<const N: usize>(what: &str, bytes: Vec<u8>) -> Result<[u8; N], Error> {
    bytes
        .try_into()
        .map_err(|bytes: Vec<u8>| bad(format_args!("has a {what} of {} bytes", bytes.len())))
}

/// An op's kind as `_jazz_pending_ops.op` names it.
fn op_name(write: &WireWrite) -> &'static str {
    match write {
        WireWrite::Insert { .. } => "insert",
        WireWrite::Update { .. } => "update",
        WireWrite::Upsert { .. } => "upsert",
        WireWrite::Delete { .. } => "delete",
        WireWrite::Restore { .. } => "restore",
        WireWrite::Move { .. } => "move",
    }
}

/// Which cell list of an op a `_jazz_pending_cells` row is in.
const CELLS: i64 = 0;
const CREATED: i64 = 1;

/// A cell as `(ty, nulls, value)`: its kind, how many present-nullable
/// wrappers it sits in, and the value itself as SQLite keeps it.
fn cell_sql(value: &Value) -> Result<(&'static str, i64, SqlValue), Error> {
    let mut value = value;
    let mut nulls = 0;
    while let Value::Nullable(Some(inner)) = value {
        nulls += 1;
        value = inner;
    }
    let (ty, sql) = match value {
        Value::Nullable(None) => ("null", SqlValue::Null),
        Value::Nullable(Some(_)) => unreachable!("present wrappers are peeled above"),
        Value::U8(v) => ("u8", SqlValue::Integer(i64::from(*v))),
        Value::U16(v) => ("u16", SqlValue::Integer(i64::from(*v))),
        Value::U32(v) => ("u32", SqlValue::Integer(i64::from(*v))),
        // Bit for bit: a u64 above i64::MAX is negative here.
        Value::U64(v) => ("u64", SqlValue::Integer(*v as i64)),
        Value::U48(v) => ("u48", SqlValue::Integer(*v as i64)),
        Value::I32(v) => ("i32", SqlValue::Integer(i64::from(*v))),
        Value::I64(v) => ("i64", SqlValue::Integer(*v)),
        // Its bits, so NaN and -0.0 survive (SQLite stores NaN as NULL).
        Value::F64(v) => ("f64", SqlValue::Blob(v.to_bits().to_be_bytes().to_vec())),
        Value::Bool(v) => ("bool", SqlValue::Integer(i64::from(*v))),
        Value::String(v) => ("string", SqlValue::Text(v.clone())),
        Value::Bytes(v) => ("bytes", SqlValue::Blob(v.clone())),
        Value::Uuid(v) => ("uuid", SqlValue::Blob(v.as_bytes().to_vec())),
        Value::EnumTag(v) => ("enum_tag", SqlValue::Integer(i64::from(*v))),
        // A chunked value's descriptor, an array, tuple, record or enum.
        Value::Large(_) | Value::Array(_) | Value::Tuple(_) | Value::Record(_) | Value::Enum(_) => {
            ("json", SqlValue::Text(exact_json("a cell", value)?))
        }
    };
    Ok((ty, nulls, sql))
}

/// A `_jazz_pending_cells` row's value back.
fn sql_cell(ty: &str, nulls: i64, sql: SqlValue) -> Result<Value, Error> {
    let wrong = || bad(format_args!("has a {ty} cell holding {sql:?}"));
    let int = |sql: &SqlValue| match sql {
        SqlValue::Integer(v) => Ok(*v),
        _ => Err(wrong()),
    };
    fn narrow<T: TryFrom<i64>>(v: i64, wrong: impl Fn() -> Error) -> Result<T, Error> {
        T::try_from(v).map_err(|_| wrong())
    }
    let mut value = match ty {
        "null" if sql == SqlValue::Null => Value::Nullable(None),
        "u8" => Value::U8(narrow(int(&sql)?, wrong)?),
        "u16" => Value::U16(narrow(int(&sql)?, wrong)?),
        "u32" => Value::U32(narrow(int(&sql)?, wrong)?),
        "u64" => Value::U64(int(&sql)? as u64),
        "u48" => Value::U48(int(&sql)? as u64),
        "i32" => Value::I32(narrow(int(&sql)?, wrong)?),
        "i64" => Value::I64(int(&sql)?),
        "bool" => match int(&sql)? {
            0 => Value::Bool(false),
            1 => Value::Bool(true),
            _ => return Err(wrong()),
        },
        "enum_tag" => Value::EnumTag(narrow(int(&sql)?, wrong)?),
        "f64" => match &sql {
            SqlValue::Blob(bits) => Value::F64(f64::from_bits(u64::from_be_bytes(
                bits.as_slice().try_into().map_err(|_| wrong())?,
            ))),
            _ => return Err(wrong()),
        },
        "string" => match sql {
            SqlValue::Text(v) => Value::String(v),
            _ => return Err(wrong()),
        },
        "bytes" => match sql {
            SqlValue::Blob(v) => Value::Bytes(v),
            _ => return Err(wrong()),
        },
        "uuid" => match &sql {
            SqlValue::Blob(v) => Value::Uuid(uuid::Uuid::from_slice(v).map_err(|_| wrong())?),
            _ => return Err(wrong()),
        },
        "json" => match &sql {
            SqlValue::Text(json) => from_json("cell", json)?,
            _ => return Err(wrong()),
        },
        _ => return Err(wrong()),
    };
    for _ in 0..nulls {
        value = Value::Nullable(Some(Box::new(value)));
    }
    Ok(value)
}

/// Store pending commit `tx`: `commit` and the local rows it wrote.
pub(crate) fn write(
    conn: &crate::storage::Storage,
    tx: u64,
    commit: &SyncMessage,
    rows: &[RowKey],
) -> Result<(), Error> {
    let SyncMessage::Commit {
        id,
        clock,
        kind,
        ops,
        acting,
    } = commit
    else {
        return Err(Error::new(ErrorCode::Storage, "only a Commit is pending"));
    };
    let tx = tx as i64;
    let (snapshot, own, reads) = match kind {
        WireKind::Mergeable => (None, None, &[][..]),
        WireKind::Serializable {
            snapshot,
            reads,
            own,
        } => (
            Some(*snapshot as i64),
            Some(to_json(own)?),
            reads.as_slice(),
        ),
    };
    let (acting, author, claims) = match acting {
        Acting::Own => ("own", None, None),
        Acting::Attributed { author } => ("attributed", Some(author.as_str()), None),
        Acting::Session { author, claims } => (
            "session",
            Some(author.as_str()),
            Some(exact_json("claims", claims)?),
        ),
    };
    conn.prepare_cached(
        "INSERT INTO _jazz_pending (tx, node, seq, clock, snapshot, own, acting, author, claims) \
         VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9)",
    )
    .and_then(|mut statement| {
        statement.execute(rusqlite::params![
            tx,
            id.node.as_bytes().as_slice(),
            id.time.0 as i64,
            *clock as i64,
            snapshot,
            own,
            acting,
            author,
            claims,
        ])
    })
    .map_err(storage)?;
    for (n, op) in ops.iter().enumerate() {
        write_op(conn, tx, n as i64, op)?;
    }
    for (n, read) in reads.iter().enumerate() {
        let n = n as i64;
        let query = exact_json("a read's query", &read.query)?;
        let binding = exact_json("a read's binding", &read.binding)?;
        conn.prepare_cached(
            "INSERT INTO _jazz_pending_reads (tx, n, version, query, binding, include_deleted) \
             VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
        )
        .and_then(|mut statement| {
            statement.execute(rusqlite::params![
                tx,
                n,
                read.version.as_slice(),
                query,
                binding,
                read.include_deleted,
            ])
        })
        .map_err(storage)?;
        for (i, (id, node, t)) in read.rows.iter().enumerate() {
            conn.prepare_cached(
                "INSERT INTO _jazz_pending_read_rows (tx, n, i, id, node, t) \
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
            )
            .and_then(|mut statement| {
                statement.execute(rusqlite::params![
                    tx,
                    n,
                    i as i64,
                    id.as_slice(),
                    node.as_ref().map(|node| node.as_slice()),
                    *t as i64,
                ])
            })
            .map_err(storage)?;
        }
    }
    for (n, (table, row, key)) in rows.iter().enumerate() {
        conn.prepare_cached(
            "INSERT INTO _jazz_pending_rows (tx, n, tbl, id, key) VALUES (?1, ?2, ?3, ?4, ?5)",
        )
        .and_then(|mut statement| {
            statement.execute(rusqlite::params![
                tx,
                n as i64,
                table,
                row.0.as_bytes().as_slice(),
                key
            ])
        })
        .map_err(storage)?;
    }
    Ok(())
}

fn write_op(conn: &crate::storage::Storage, tx: i64, n: i64, op: &WireOp) -> Result<(), Error> {
    let none = || None::<&BTreeMap<String, Value>>;
    let (target, moved, at, cells, created) = match &op.write {
        WireWrite::Insert {
            cells, target, at, ..
        }
        | WireWrite::Update {
            cells, target, at, ..
        } => (Some(target), None, *at, Some(cells), none()),
        WireWrite::Upsert {
            cells,
            created,
            target,
            at,
            ..
        } => (Some(target), None, *at, Some(cells), Some(created)),
        WireWrite::Delete { target, at, .. } => (Some(target), None, *at, none(), none()),
        WireWrite::Restore {
            cells, target, at, ..
        } => (Some(target), None, *at, cells.as_ref(), none()),
        WireWrite::Move { from, to, .. } => (None, Some((from, to)), None, none(), none()),
    };
    let target = target.map(to_json).transpose()?;
    let (from, to) = match moved {
        Some((from, to)) => (Some(to_json(from)?), Some(to_json(to)?)),
        None => (None, None),
    };
    let bases = to_json(&op.bases)?;
    let (proof_key, proof_seq) = match &op.access_proof {
        Some(proof) => (Some(proof.key.as_slice()), Some(proof.seq as i64)),
        None => (None, None),
    };
    conn.prepare_cached(
        "INSERT INTO _jazz_pending_ops \
         (tx, n, op, tbl, id, target, move_from, move_to, at, has_cells, bases, proof_key, proof_seq) \
         VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13)",
    )
    .and_then(|mut statement| {
        statement.execute(rusqlite::params![
            tx,
            n,
            op_name(&op.write),
            op.write.table(),
            op.write.row().as_slice(),
            target,
            from,
            to,
            at.map(|at| at as i64),
            cells.is_some(),
            bases,
            proof_key,
            proof_seq,
        ])
    })
    .map_err(storage)?;
    for (list, cells) in [(CELLS, cells), (CREATED, created)] {
        for (column, value) in cells.into_iter().flatten() {
            let (ty, nulls, value) = cell_sql(value)?;
            conn.prepare_cached(
                "INSERT INTO _jazz_pending_cells (tx, n, list, col, ty, nulls, value) \
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)",
            )
            .and_then(|mut statement| {
                statement.execute(rusqlite::params![tx, n, list, column, ty, nulls, value])
            })
            .map_err(storage)?;
        }
    }
    for (i, delta) in op.deltas.iter().enumerate() {
        conn.prepare_cached(
            "INSERT INTO _jazz_pending_deltas (tx, n, i, col, depth, key, seq, base, edits) \
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9)",
        )
        .and_then(|mut statement| {
            statement.execute(rusqlite::params![
                tx,
                n,
                i as i64,
                delta.column,
                delta.depth,
                delta.key,
                delta.delta.seq as i64,
                delta.delta.base,
                delta.delta.edits,
            ])
        })
        .map_err(storage)?;
    }
    Ok(())
}

/// The txs of the pending commits, oldest first.
pub(crate) fn txs(conn: &crate::storage::Storage) -> Result<Vec<u64>, Error> {
    let mut statement = conn
        .prepare_cached("SELECT tx FROM _jazz_pending ORDER BY tx")
        .map_err(storage)?;
    statement
        .query_map([], |r| r.get::<_, i64>(0))
        .map_err(storage)?
        .map(|tx| tx.map(|tx| tx as u64).map_err(storage))
        .collect()
}

/// Pending commit `tx` as [`write`] stored it.
pub(crate) fn read(conn: &crate::storage::Storage, tx: u64) -> Result<SyncMessage, Error> {
    type Header = (
        Vec<u8>,
        i64,
        i64,
        Option<i64>,
        Option<String>,
        String,
        Option<String>,
        Option<String>,
    );
    let (node, seq, clock, snapshot, own, acting, author, claims): Header = conn
        .cached_row(
            "SELECT node, seq, clock, snapshot, own, acting, author, claims \
             FROM _jazz_pending WHERE tx = ?1",
            [tx as i64],
            |r| {
                Ok((
                    r.get(0)?,
                    r.get(1)?,
                    r.get(2)?,
                    r.get(3)?,
                    r.get(4)?,
                    r.get(5)?,
                    r.get(6)?,
                    r.get(7)?,
                ))
            },
        )
        .map_err(|error| match error {
            rusqlite::Error::InvalidColumnType(..) => bad(format_args!("has a malformed header")),
            error => storage(error),
        })?;
    let acting = match (acting.as_str(), author, claims) {
        ("own", None, None) => Acting::Own,
        ("attributed", Some(author), None) => Acting::Attributed { author },
        ("session", Some(author), Some(claims)) => Acting::Session {
            author,
            claims: from_json("claims", &claims)?,
        },
        (acting, ..) => return Err(bad(format_args!("acts as {acting:?}"))),
    };
    let tx = tx as i64;
    let kind = match (snapshot, own) {
        (None, None) => {
            if count(conn, "_jazz_pending_reads", tx)? > 0 {
                return Err(bad("is mergeable but has reads"));
            }
            WireKind::Mergeable
        }
        (Some(snapshot), Some(own)) => WireKind::Serializable {
            snapshot: snapshot as u64,
            reads: reads(conn, tx)?,
            own: from_json("own list", &own)?,
        },
        _ => return Err(bad("has half a serializable snapshot")),
    };
    Ok(SyncMessage::Commit {
        id: tx_id(blob("node", node)?, seq as u64),
        clock: clock as u64,
        kind,
        ops: ops(conn, "_jazz_pending", tx as u64)?,
        acting,
    })
}

fn count(conn: &crate::storage::Storage, table: &str, tx: i64) -> Result<i64, Error> {
    conn.cached_row(
        &format!("SELECT count(*) FROM {table} WHERE tx = ?1"),
        [tx],
        |r| r.get(0),
    )
    .map_err(storage)
}

fn reads(conn: &crate::storage::Storage, tx: i64) -> Result<Vec<WireRead>, Error> {
    let mut statement = conn
        .prepare_cached(
            "SELECT n, version, query, binding, include_deleted \
             FROM _jazz_pending_reads WHERE tx = ?1 ORDER BY n",
        )
        .map_err(storage)?;
    let stored = statement
        .query_map([tx], |r| {
            Ok((
                r.get::<_, i64>(0)?,
                r.get::<_, Vec<u8>>(1)?,
                r.get::<_, String>(2)?,
                r.get::<_, String>(3)?,
                r.get::<_, bool>(4)?,
            ))
        })
        .map_err(storage)?
        .collect::<Result<Vec<_>, _>>()
        .map_err(|_| bad("has a malformed read"))?;
    drop(statement);
    stored
        .into_iter()
        .map(|(n, version, query, binding, include_deleted)| {
            let mut statement = conn
                .prepare_cached(
                    "SELECT id, node, t FROM _jazz_pending_read_rows \
                     WHERE tx = ?1 AND n = ?2 ORDER BY i",
                )
                .map_err(storage)?;
            let rows = statement
                .query_map([tx, n], |r| {
                    Ok((
                        r.get::<_, Vec<u8>>(0)?,
                        r.get::<_, Option<Vec<u8>>>(1)?,
                        r.get::<_, i64>(2)?,
                    ))
                })
                .map_err(storage)?
                .map(|row| {
                    let (id, node, t) = row.map_err(|_| bad("has a malformed read row"))?;
                    Ok((
                        blob("read row id", id)?,
                        node.map(|node| blob("read row node", node)).transpose()?,
                        t as u64,
                    ))
                })
                .collect::<Result<Vec<_>, Error>>()?;
            Ok(WireRead {
                version: blob("schema version", version)?,
                query: from_json("read query", &query)?,
                binding: from_json("read binding", &binding)?,
                include_deleted,
                rows,
            })
        })
        .collect()
}

/// The ops of commit `tx` in `{prefix}_ops` and its cell and delta tables:
/// `_jazz_pending`'s or the retry store's `_jazz_rejected`.
pub(crate) fn ops(
    conn: &crate::storage::Storage,
    prefix: &str,
    tx: u64,
) -> Result<Vec<WireOp>, Error> {
    let tx = tx as i64;
    type Stored = (
        i64,
        String,
        String,
        Vec<u8>,
        Option<String>,
        Option<String>,
        Option<String>,
        Option<i64>,
        bool,
        String,
        Option<Vec<u8>>,
        Option<i64>,
    );
    let mut statement = conn
        .prepare_cached(&format!(
            "SELECT n, op, tbl, id, target, move_from, move_to, at, has_cells, bases, proof_key, proof_seq \
             FROM {prefix}_ops WHERE tx = ?1 ORDER BY n"
        ))
        .map_err(storage)?;
    let stored: Vec<Stored> = statement
        .query_map([tx], |r| {
            Ok((
                r.get(0)?,
                r.get(1)?,
                r.get(2)?,
                r.get(3)?,
                r.get(4)?,
                r.get(5)?,
                r.get(6)?,
                r.get(7)?,
                r.get(8)?,
                r.get(9)?,
                r.get(10)?,
                r.get(11)?,
            ))
        })
        .map_err(storage)?
        .collect::<Result<_, _>>()
        .map_err(|_| bad("has a malformed op"))?;
    drop(statement);
    stored
        .into_iter()
        .enumerate()
        .map(|(position, stored)| {
            let (n, op, table, row, target, from, to, at, has_cells, bases, key, seq) = stored;
            if n != position as i64 {
                return Err(bad(format_args!("is missing op {position}")));
            }
            let row = blob("row id", row)?;
            let at = at.map(|at| at as u64);
            let (mut cells, mut created) = cells(conn, prefix, tx, n)?;
            if !has_cells && !cells.is_empty() {
                return Err(bad(format_args!("has cells on op {n}, which takes none")));
            }
            let target = || match &target {
                Some(target) => from_json::<WireTarget>("branch target", target),
                None => Err(bad(format_args!("has no branch target on op {n}"))),
            };
            let cells_of = |cells: &mut BTreeMap<String, Value>| std::mem::take(cells);
            let write = match op.as_str() {
                "insert" => WireWrite::Insert {
                    table,
                    row,
                    cells: cells_of(&mut cells),
                    target: target()?,
                    at,
                },
                "update" => WireWrite::Update {
                    table,
                    row,
                    cells: cells_of(&mut cells),
                    target: target()?,
                    at,
                },
                "upsert" => WireWrite::Upsert {
                    table,
                    row,
                    cells: cells_of(&mut cells),
                    created: cells_of(&mut created),
                    target: target()?,
                    at,
                },
                "delete" => WireWrite::Delete {
                    table,
                    row,
                    target: target()?,
                    at,
                },
                "restore" => WireWrite::Restore {
                    table,
                    row,
                    cells: has_cells.then(|| cells_of(&mut cells)),
                    target: target()?,
                    at,
                },
                "move" => match (&from, &to) {
                    (Some(from), Some(to)) => WireWrite::Move {
                        table,
                        row,
                        from: from_json("move source", from)?,
                        to: from_json("move destination", to)?,
                    },
                    _ => {
                        return Err(bad(format_args!(
                            "has a move without its branches on op {n}"
                        )));
                    }
                },
                op => return Err(bad(format_args!("has an op {op:?}"))),
            };
            if !created.is_empty() {
                return Err(bad(format_args!(
                    "has created cells on op {n}, which is no upsert"
                )));
            }
            let access_proof = match (key, seq) {
                (Some(key), Some(seq)) => Some(AccessProof {
                    key,
                    seq: seq as u64,
                }),
                (None, None) => None,
                _ => return Err(bad(format_args!("has half an access proof on op {n}"))),
            };
            Ok(WireOp {
                write,
                bases: from_json("bases", &bases)?,
                access_proof,
                deltas: deltas(conn, prefix, tx, n)?,
            })
        })
        .collect()
}

type CellLists = (BTreeMap<String, Value>, BTreeMap<String, Value>);

fn cells(
    conn: &crate::storage::Storage,
    prefix: &str,
    tx: i64,
    n: i64,
) -> Result<CellLists, Error> {
    let mut statement = conn
        .prepare_cached(&format!(
            "SELECT list, col, ty, nulls, value FROM {prefix}_cells WHERE tx = ?1 AND n = ?2"
        ))
        .map_err(storage)?;
    let rows = statement
        .query_map([tx, n], |r| {
            Ok((
                r.get::<_, i64>(0)?,
                r.get::<_, String>(1)?,
                r.get::<_, String>(2)?,
                r.get::<_, i64>(3)?,
                r.get::<_, SqlValue>(4)?,
            ))
        })
        .map_err(storage)?;
    let mut lists = (BTreeMap::new(), BTreeMap::new());
    for row in rows {
        let (list, column, ty, nulls, value) = row.map_err(|_| bad("has a malformed cell"))?;
        let value = sql_cell(&ty, nulls, value)?;
        match list {
            CELLS => lists.0.insert(column, value),
            CREATED => lists.1.insert(column, value),
            list => return Err(bad(format_args!("has a cell in list {list}"))),
        };
    }
    Ok(lists)
}

fn deltas(
    conn: &crate::storage::Storage,
    prefix: &str,
    tx: i64,
    n: i64,
) -> Result<Vec<CellDelta>, Error> {
    let mut statement = conn
        .prepare_cached(&format!(
            "SELECT col, depth, key, seq, base, edits FROM {prefix}_deltas \
             WHERE tx = ?1 AND n = ?2 ORDER BY i"
        ))
        .map_err(storage)?;
    statement
        .query_map([tx, n], |r| {
            Ok(CellDelta {
                column: r.get(0)?,
                depth: r.get(1)?,
                key: r.get(2)?,
                delta: WireDelta {
                    seq: r.get::<_, i64>(3)? as u64,
                    base: r.get(4)?,
                    edits: r.get(5)?,
                },
            })
        })
        .map_err(storage)?
        .map(|delta| delta.map_err(|_| bad("has a malformed delta")))
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ids::NodeUuid;

    fn store() -> crate::storage::Storage {
        let conn = crate::storage::Storage::open_in_memory().unwrap();
        conn.execute_batch(PENDING_DDL).unwrap();
        conn.execute_batch(SYNC_DDL).unwrap();
        conn
    }

    fn selector(tenant: &str) -> BranchSelector {
        BranchSelector::new([("tenant", Value::String(tenant.to_owned()))])
    }

    fn string(value: &str) -> Value {
        Value::String(value.to_owned())
    }

    /// A cell of every kind a write can carry.
    fn every_cell() -> BTreeMap<String, Value> {
        let some = |value| Value::Nullable(Some(Box::new(value)));
        BTreeMap::from(
            [
                ("u8", Value::U8(u8::MAX)),
                ("u16", Value::U16(u16::MAX)),
                ("u32", Value::U32(u32::MAX)),
                ("u64", Value::U64(u64::MAX)),
                ("u48", Value::U48((1 << 48) - 1)),
                ("i32", Value::I32(i32::MIN)),
                ("i64", Value::I64(i64::MIN)),
                ("f64", Value::F64(-0.0)),
                ("nan", Value::F64(f64::NAN)),
                ("bool", Value::Bool(true)),
                ("string", string("héllo")),
                ("empty", string("")),
                ("bytes", Value::Bytes(vec![0, 1, 255])),
                ("uuid", Value::Uuid(uuid::Uuid::from_bytes([7; 16]))),
                ("tag", Value::EnumTag(3)),
                ("null", Value::Nullable(None)),
                ("some", some(string("x"))),
                ("some_null", some(Value::Nullable(None))),
                ("some_some", some(some(Value::U8(1)))),
                ("array", Value::Array(vec![Value::U32(1), Value::U32(2)])),
                (
                    "tuple",
                    Value::Tuple(vec![
                        Value::U64(5),
                        Value::Uuid(uuid::Uuid::from_bytes([1; 16])),
                    ]),
                ),
            ]
            .map(|(name, value)| (name.to_owned(), value)),
        )
    }

    fn every_op() -> Vec<WireOp> {
        let target = WireTarget::Root;
        let mut ops: Vec<WireOp> = vec![
            WireWrite::Insert {
                table: "notes".to_owned(),
                row: [1; 16],
                cells: every_cell(),
                target: target.clone(),
                at: Some(1_800_000_000_000),
            }
            .into(),
            WireWrite::Update {
                table: "notes".to_owned(),
                row: [1; 16],
                cells: BTreeMap::from([("string".to_owned(), string("y"))]),
                target: WireTarget::Exact(selector("a")),
                at: None,
            }
            .into(),
            WireWrite::Upsert {
                table: "notes".to_owned(),
                row: [2; 16],
                cells: BTreeMap::from([("string".to_owned(), string("z"))]),
                created: BTreeMap::from([("u8".to_owned(), Value::U8(0))]),
                target: WireTarget::View {
                    head: selector("draft"),
                    base: Some(selector("main")),
                },
                at: None,
            }
            .into(),
            WireWrite::Delete {
                table: "notes".to_owned(),
                row: [2; 16],
                target: WireTarget::FrozenView {
                    head: selector("draft"),
                    base: selector("main"),
                    snapshot: SnapshotRef {
                        owner: NodeUuid::from_bytes([3; 16]),
                        global_base: GlobalTime(9),
                        local_base: TxTime(4),
                        dots: vec![tx_id([5; 16], 6)],
                    },
                },
                at: Some(2),
            }
            .into(),
            WireWrite::Restore {
                table: "notes".to_owned(),
                row: [2; 16],
                cells: None,
                target: target.clone(),
                at: None,
            }
            .into(),
            WireWrite::Restore {
                table: "notes".to_owned(),
                row: [3; 16],
                cells: Some(BTreeMap::new()),
                target: target.clone(),
                at: None,
            }
            .into(),
            WireWrite::Move {
                table: "notes".to_owned(),
                row: [1; 16],
                from: selector("a"),
                to: selector("b"),
            }
            .into(),
        ];
        ops[0].bases = vec![
            WireBase {
                key: Vec::new(),
                on: BuiltOn::Seq(7),
            },
            WireBase {
                key: vec![1, 2],
                on: BuiltOn::Own { back: 1 },
            },
            WireBase {
                key: vec![3],
                on: BuiltOn::Commit {
                    node: [4; 16],
                    tx: 8,
                },
            },
        ];
        ops[0].access_proof = Some(AccessProof {
            key: vec![9],
            seq: 11,
        });
        ops[0].deltas = vec![CellDelta {
            column: "string".to_owned(),
            depth: 1,
            key: Vec::new(),
            delta: WireDelta {
                seq: 12,
                base: 3,
                edits: vec![1, 2, 3, 4],
            },
        }];
        ops
    }

    fn serializable_commit() -> SyncMessage {
        SyncMessage::Commit {
            id: tx_id([7; 16], 41),
            clock: crate::db::shard::hlc(1_800_000_000_000) + 3,
            kind: WireKind::Serializable {
                snapshot: 99,
                reads: vec![WireRead {
                    version: [6; 16],
                    query: crate::query::Query::from("notes"),
                    binding: BTreeMap::from([("tenant".to_owned(), string("a"))]),
                    include_deleted: true,
                    rows: vec![([1; 16], None, 5), ([2; 16], Some([7; 16]), 40)],
                }],
                own: vec![38, 39],
            },
            ops: every_op(),
            acting: Acting::Session {
                author: "jazz:account:1".to_owned(),
                claims: BTreeMap::from([("role".to_owned(), string("admin"))]),
            },
        }
    }

    /// The postcard encoding, which tells apart every field (and NaN's bits).
    fn bytes(message: &SyncMessage) -> Vec<u8> {
        postcard::to_allocvec(message).unwrap()
    }

    #[test]
    fn a_pending_commit_reads_back_exactly() {
        let conn = store();
        let commits = [
            (1, serializable_commit()),
            (
                2,
                SyncMessage::Commit {
                    id: tx_id([8; 16], 2),
                    clock: 5,
                    kind: WireKind::Mergeable,
                    ops: every_op(),
                    acting: Acting::Own,
                },
            ),
            (
                3,
                SyncMessage::Commit {
                    id: tx_id([8; 16], 3),
                    clock: 6,
                    kind: WireKind::Mergeable,
                    ops: Vec::new(),
                    acting: Acting::Attributed {
                        author: "jazz:account:2".to_owned(),
                    },
                },
            ),
        ];
        for (tx, commit) in &commits {
            write(&conn, *tx, commit, &[]).unwrap();
        }
        assert_eq!(txs(&conn).unwrap(), [1, 2, 3]);
        for (tx, commit) in &commits {
            assert_eq!(bytes(&read(&conn, *tx).unwrap()), bytes(commit), "tx {tx}");
        }
    }

    /// The layout itself, written by hand: a store holding these rows must
    /// keep reading as this commit whatever else changes. A failure here
    /// means a pending write a user holds would no longer read back.
    #[test]
    fn the_pending_layout_is_pinned() {
        let conn = store();
        conn.execute_batch(
            "INSERT INTO _jazz_pending (tx, node, seq, clock, snapshot, own, acting, author, claims) \
             VALUES (4, x'07070707070707070707070707070707', 41, 1234, NULL, NULL, 'own', NULL, NULL); \
             INSERT INTO _jazz_pending_ops \
             (tx, n, op, tbl, id, target, move_from, move_to, at, has_cells, bases, proof_key, proof_seq) \
             VALUES (4, 0, 'insert', 'notes', x'01010101010101010101010101010101', '\"Root\"', \
             NULL, NULL, 1800000000000, 1, '[{\"key\":[],\"on\":{\"Seq\":7}}]', NULL, NULL); \
             INSERT INTO _jazz_pending_cells (tx, n, list, col, ty, nulls, value) VALUES \
             (4, 0, 0, 'title', 'string', 0, 'Groceries'), \
             (4, 0, 0, 'done', 'bool', 1, 0), \
             (4, 0, 0, 'due', 'null', 0, NULL), \
             (4, 0, 0, 'weight', 'f64', 0, x'3ff8000000000000'), \
             (4, 0, 0, 'big', 'u64', 0, -1), \
             (4, 0, 0, 'owner', 'uuid', 0, x'02020202020202020202020202020202'), \
             (4, 0, 0, 'tags', 'json', 0, '{\"Array\":[{\"String\":\"home\"}]}'); \
             INSERT INTO _jazz_pending_deltas (tx, n, i, col, depth, key, seq, base, edits) \
             VALUES (4, 0, 0, 'body', 0, x'', 3, 2, x'0a0b');",
        )
        .unwrap();
        let mut op: WireOp = WireWrite::Insert {
            table: "notes".to_owned(),
            row: [1; 16],
            cells: BTreeMap::from(
                [
                    ("title", string("Groceries")),
                    ("done", Value::Nullable(Some(Box::new(Value::Bool(false))))),
                    ("due", Value::Nullable(None)),
                    ("weight", Value::F64(1.5)),
                    ("big", Value::U64(u64::MAX)),
                    ("owner", Value::Uuid(uuid::Uuid::from_bytes([2; 16]))),
                    ("tags", Value::Array(vec![string("home")])),
                ]
                .map(|(name, value)| (name.to_owned(), value)),
            ),
            target: WireTarget::Root,
            at: Some(1_800_000_000_000),
        }
        .into();
        op.bases = vec![WireBase {
            key: Vec::new(),
            on: BuiltOn::Seq(7),
        }];
        op.deltas = vec![CellDelta {
            column: "body".to_owned(),
            depth: 0,
            key: Vec::new(),
            delta: WireDelta {
                seq: 3,
                base: 2,
                edits: vec![10, 11],
            },
        }];
        let expected = SyncMessage::Commit {
            id: tx_id([7; 16], 41),
            clock: 1234,
            kind: WireKind::Mergeable,
            ops: vec![op],
            acting: Acting::Own,
        };
        assert_eq!(bytes(&read(&conn, 4).unwrap()), bytes(&expected));
    }

    #[test]
    fn a_refused_commit_keeps_its_ops_in_the_same_layout() {
        let conn = store();
        write(&conn, 1, &serializable_commit(), &[]).unwrap();
        for ddl in REJECTED_OPS_DDL {
            conn.execute(ddl, []).unwrap();
        }
        for table in OP_TABLES {
            conn.execute(
                &format!(
                    "INSERT INTO _jazz_rejected_{table} SELECT * FROM _jazz_pending_{table} WHERE tx = 1"
                ),
                [],
            )
            .unwrap();
        }
        conn.execute("DELETE FROM _jazz_pending WHERE tx = 1", [])
            .unwrap();
        for table in OP_TABLES {
            let left: i64 = conn
                .query_row(
                    &format!("SELECT count(*) FROM _jazz_pending_{table}"),
                    [],
                    |r| r.get(0),
                )
                .unwrap();
            assert_eq!(
                left, 0,
                "forgetting the commit empties _jazz_pending_{table}"
            );
        }
        let SyncMessage::Commit { ops: expected, .. } = serializable_commit() else {
            unreachable!()
        };
        let ops = ops(&conn, "_jazz_rejected", 1).unwrap();
        assert_eq!(
            postcard::to_allocvec(&ops).unwrap(),
            postcard::to_allocvec(&expected).unwrap()
        );
    }

    #[test]
    fn rows_in_another_layout_are_refused_as_another_format() {
        for edit in [
            "UPDATE _jazz_pending_cells SET ty = 'decimal' WHERE col = 'u8'",
            "UPDATE _jazz_pending_cells SET value = 'x' WHERE col = 'bool'",
            "UPDATE _jazz_pending_cells SET value = 300 WHERE col = 'u8'",
            "UPDATE _jazz_pending_cells SET list = 2 WHERE col = 'u8'",
            "UPDATE _jazz_pending_ops SET op = 'truncate' WHERE n = 0",
            "UPDATE _jazz_pending_ops SET target = '{\"Branch\":1}' WHERE n = 0",
            "UPDATE _jazz_pending_ops SET proof_seq = NULL WHERE n = 0",
            "UPDATE _jazz_pending_ops SET n = 9 WHERE n = 1",
            "UPDATE _jazz_pending SET acting = 'robot'",
            "UPDATE _jazz_pending SET snapshot = NULL",
            "UPDATE _jazz_pending SET node = x'0102'",
        ] {
            let conn = store();
            write(&conn, 1, &serializable_commit(), &[]).unwrap();
            conn.execute_batch(edit).unwrap();
            let error = read(&conn, 1).expect_err(edit);
            assert_eq!(
                error.code,
                ErrorCode::UnsupportedStorageCodecs,
                "{edit}: {error}"
            );
        }
    }

    #[test]
    fn a_claim_json_cannot_hold_is_refused_when_written() {
        let conn = store();
        let commit = SyncMessage::Commit {
            id: tx_id([8; 16], 2),
            clock: 5,
            kind: WireKind::Mergeable,
            ops: Vec::new(),
            acting: Acting::Session {
                author: "jazz:account:1".to_owned(),
                claims: BTreeMap::from([("score".to_owned(), Value::F64(f64::NAN))]),
            },
        };
        let error = write(&conn, 1, &commit, &[]).expect_err("NaN has no JSON");
        assert!(error.message.contains("not supported yet"), "{error}");
    }
}
