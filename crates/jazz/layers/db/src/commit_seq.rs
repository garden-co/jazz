//! The commit seq a store resumes after, kept in one row of `_jazz_meta`
//! (jazz#6367).
//!
//! A store numbers its commits with a seq, and a reopened store resumes
//! after the newest one it handed out. It used to recover that seq at every
//! open by scanning every table and every history table for its newest
//! accepted `_clock`. Those tables have no `_clock` index, so each scan read
//! the whole table: on network storage, a large store took tens of seconds
//! to open.
//!
//! Encoding `commit_seq.v1`:
//!
//! - The row `_jazz_meta (k = 'commit_seq.v1', v)`, where `v` is an SQLite
//!   INTEGER: the newest accepted seq (`0 <= v < band_start(PENDING)`)
//!   that any table, history table or `_jazz_transactions` row holds, or
//!   ever held. A store without the row never had one.
//! - Two triggers on each such table, `<table>__seq_v1_ins` (AFTER INSERT)
//!   and `<table>__seq_v1_upd` (AFTER UPDATE OF the seq column), raise `v`
//!   to every accepted seq written there ([`trigger_ddl`]).
//!
//! Triggers, rather than a write in each commit path: a trigger runs inside
//! the statement that writes the seq, so the row and the data commit or
//! roll back together, whatever crashes when. And triggers live in the
//! file, so they also fire for a build that knows nothing of the row. Jazz
//! Cloud runs older cores against the same stores, and a store may go back
//! and forth between versions: an older core's commits still raise the row,
//! and a later open never resumes below them.
//!
//! Invariant: for every table that has both triggers, `v` is at least the
//! newest accepted seq it holds. [`recover`] keeps it: a table without its
//! triggers (every table of a store that predates the row, or one an older
//! core created) is scanned, and covered in the same transaction that gives
//! it its triggers and raises `v` past what that scan found. If the row is
//! missing, every table is scanned, as before.
//!
//! An open only reads: a store this build opens but never writes keeps its
//! file as it was (the pinned client corpus checks that). So an open scans
//! the uncovered tables, as opens always did, and the store's first write
//! scope covers them ([`crate::db`]'s `cover_commit_seq`), scanning them
//! again inside its own transaction: what it records never depends on what
//! was written between the open and then. Until then, each open of such a
//! store scans as before.
//!
//! An older build that opens such a store reads it as it always did: the
//! format is unchanged ([`crate::format::STORAGE_FORMAT`]); it ignores the
//! row and the triggers, and still recovers its seq by the scan.

use std::collections::BTreeSet;

use crate::db::sync::{PENDING, band_start};
use crate::options::{Error, ErrorCode};
use crate::storage::Storage;
use crate::values::{quote, storage};

/// The `_jazz_meta` key of the commit seq row, named for its encoding.
pub(crate) const SEQ_META: &str = "commit_seq.v1";

/// The table whose `t` records the seq an accepted upload took, also when
/// it wrote no rows (its dedupe entry).
const TRANSACTIONS: &str = "_jazz_transactions";

/// The suffixes of a table's two triggers. Renaming them, or changing what
/// they do, is a new encoding.
const INSERT_TRIGGER: &str = "__seq_v1_ins";
const UPDATE_TRIGGER: &str = "__seq_v1_upd";

/// A table whose seqs the row covers, and its seq column.
struct Target {
    table: String,
    column: &'static str,
}

/// Every stored table and history table of `tables`, and
/// `_jazz_transactions`.
fn targets<'a>(tables: impl IntoIterator<Item = &'a String>) -> Vec<Target> {
    let mut targets: Vec<Target> = tables
        .into_iter()
        .flat_map(|table| [table.clone(), format!("{table}__h")])
        .map(|table| Target {
            table,
            column: "_clock",
        })
        .collect();
    targets.push(Target {
        table: TRANSACTIONS.to_owned(),
        column: "t",
    });
    targets
}

/// The two triggers that raise the row to every accepted seq written to
/// `table`'s `column`. Pinned byte for byte by a fixture: changing them is
/// a new encoding, under new names.
pub(crate) fn trigger_ddl(table: &str, column: &str) -> String {
    let accepted = format!(
        "NEW.{column} >= 0 AND NEW.{column} < {}",
        band_start(PENDING)
    );
    let raise = format!(
        "UPDATE _jazz_meta SET v = NEW.{column} WHERE k = '{SEQ_META}' AND v < NEW.{column};"
    );
    let on = quote(table);
    format!(
        "CREATE TRIGGER IF NOT EXISTS {ins} AFTER INSERT ON {on} WHEN {accepted} BEGIN {raise} END;\n\
         CREATE TRIGGER IF NOT EXISTS {upd} AFTER UPDATE OF {column} ON {on} WHEN {accepted} BEGIN {raise} END;\n",
        ins = quote(&format!("{table}{INSERT_TRIGGER}")),
        upd = quote(&format!("{table}{UPDATE_TRIGGER}")),
    )
}

/// What [`recover`] found.
pub(crate) struct Recovered {
    /// The seq the store resumes after.
    pub seq: u64,
    /// The tables it scanned for their newest seq, in order.
    pub scanned: Vec<String>,
}

/// Whether [`recover`] only reads, or also covers what it scanned.
#[derive(Clone, Copy, PartialEq, Eq)]
pub(crate) enum Cover {
    /// Read the row and scan the uncovered tables; write nothing.
    Read,
    /// Also give the scanned tables their triggers and raise the row past
    /// what the scans found. Run inside a write transaction, so the
    /// triggers, the scans and the row's new value commit together.
    Write,
}

/// The seq this store resumes after: its row, raised by scans of only the
/// tables the row does not cover yet (all of them without the row).
pub(crate) async fn recover<'a>(
    conn: &Storage,
    tables: impl IntoIterator<Item = &'a String>,
    cover: Cover,
) -> Result<Recovered, Error> {
    let row = read(conn).await?;
    let covered = if row.is_some() {
        covered(conn).await?
    } else {
        // Without the row no trigger kept anything: every table is read.
        BTreeSet::new()
    };
    let mut seq = row.unwrap_or(0);
    let mut scanned = Vec::new();
    let mut ddl = String::new();
    for target in targets(tables) {
        if covered.contains(&target.table) {
            continue;
        }
        let max: Option<i64> = conn
            .query_row(
                &format!(
                    "SELECT MAX({column}) FROM {table} WHERE {column} >= 0 AND {column} < ?1",
                    column = target.column,
                    table = quote(&target.table),
                ),
                [band_start(PENDING)],
                |r| r.get(0),
            )
            .await
            .map_err(storage)?;
        seq = seq.max(max.unwrap_or(0) as u64);
        ddl.push_str(&trigger_ddl(&target.table, target.column));
        scanned.push(target.table);
    }
    if cover == Cover::Read {
        return Ok(Recovered { seq, scanned });
    }
    if !ddl.is_empty() {
        conn.execute_batch(&ddl).await.map_err(storage)?;
    }
    if row != Some(seq) {
        conn.execute(
            "INSERT OR REPLACE INTO _jazz_meta (k, v) VALUES (?1, ?2)",
            crate::storage::params![SEQ_META, seq as i64],
        )
        .await
        .map_err(storage)?;
    }
    Ok(Recovered { seq, scanned })
}

/// The row's seq, if the store has one. Anything but a non-negative
/// accepted INTEGER is a store this build cannot trust to resume from, and
/// fails the open rather than reuse a seq.
async fn read(conn: &Storage) -> Result<Option<u64>, Error> {
    let value: Option<crate::storage::Value> = {
        use crate::storage::OptionalExtension;
        conn.query_row("SELECT v FROM _jazz_meta WHERE k = ?1", [SEQ_META], |r| {
            r.get(0)
        })
        .await
        .optional()
        .map_err(storage)?
    };
    match value {
        None => Ok(None),
        Some(crate::storage::Value::Integer(seq)) if (0..band_start(PENDING)).contains(&seq) => {
            Ok(Some(seq as u64))
        }
        Some(other) => Err(Error::new(
            ErrorCode::Storage,
            format!(
                "corrupt commit seq: _jazz_meta '{SEQ_META}' holds {other:?}, not an accepted \
                 seq; refusing to resume numbering from it"
            ),
        )),
    }
}

/// The tables that have both triggers.
async fn covered(conn: &Storage) -> Result<BTreeSet<String>, Error> {
    let names: Vec<(String, String)> = (async {
        let mut statement = conn.prepare(
            "SELECT name, tbl_name FROM sqlite_master WHERE type = 'trigger' \
             AND (name GLOB ('*' || ?1) OR name GLOB ('*' || ?2))",
        )?;
        {
            statement
                .query_map([INSERT_TRIGGER, UPDATE_TRIGGER], |row| {
                    Ok((row.get(0)?, row.get(1)?))
                })
                .await?
                .collect::<_>()
                .await
        }
    })
    .await
    .map_err(storage)?;
    let has = |suffix: &str, table: &str| {
        names
            .iter()
            .any(|(name, on)| on == table && *name == format!("{table}{suffix}"))
    };
    Ok(names
        .iter()
        .map(|(_, table)| table.clone())
        .filter(|table| has(INSERT_TRIGGER, table) && has(UPDATE_TRIGGER, table))
        .collect())
}
