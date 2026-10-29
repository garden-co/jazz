//! The storage format a store's SQLite file was written in.
//!
//! Every file this engine writes carries one row in `_jazz_format`: the
//! version of the layout of its tables, history, sync bookkeeping and large
//! values. A table rather than `PRAGMA user_version`, because Durable Object
//! SQLite refuses that PRAGMA. Opening admits the file before anything
//! writes to it (not even the journal mode), so a refused file is left
//! byte-for-byte as it was:
//!
//! - an empty file is new: it is stamped with [`STORAGE_FORMAT`];
//! - a file stamped with [`STORAGE_FORMAT`] opens;
//! - any other stamp, and a file with tables but no stamp (one Jazz did not
//!   write, such as a Groove SQLite-adapter root), is refused with
//!   [`ErrorCode::UnsupportedStorageCodecs`].
//!
//! The Groove engine's native stores were RocksDB directories; a host that
//! keeps its store in a data directory refuses one with
//! [`refuse_groove_store`] instead of opening an empty SQLite file beside it.
//!
//! The pinned corpus in `crates/jazz/tests/storage_corpus.rs` holds a file
//! of each released format; a change to the layout that an older file would
//! not read bumps [`STORAGE_FORMAT`] and adds its corpus.

use std::path::Path;

use rusqlite::Connection;

use crate::options::{Error, ErrorCode};
use crate::values::storage;

/// The storage format this build writes and reads.
pub const STORAGE_FORMAT: i64 = 1;

const STAMP_TABLE: &str = "_jazz_format";

/// A store this build cannot read, with what gave it away.
pub(crate) fn unsupported(detail: impl std::fmt::Display) -> Error {
    Error::new(
        ErrorCode::UnsupportedStorageCodecs,
        format!(
            "unsupported storage format: {detail}; this build reads Jazz SQLite storage format \
             {STORAGE_FORMAT}. Delete the local store and resync from Core"
        ),
    )
}

/// Admit the file `conn` opened: stamp a new one, accept one in
/// [`STORAGE_FORMAT`], refuse anything else. Reads only, unless the file is
/// new.
pub(crate) fn admit(conn: &Connection) -> Result<(), Error> {
    // A damaged header fails here (`file is not a database`), as a storage
    // error, before any write.
    let tables: Vec<String> = conn
        .prepare(
            // Durable Object SQLite keeps its own `_cf_*` tables in the file.
            "SELECT name FROM sqlite_master WHERE type = 'table' \
             AND name NOT LIKE 'sqlite\\_%' ESCAPE '\\' AND name NOT LIKE '\\_cf\\_%' ESCAPE '\\'",
        )
        .and_then(|mut statement| {
            statement
                .query_map([], |row| row.get(0))?
                .collect::<Result<_, _>>()
        })
        .map_err(storage)?;
    if tables.is_empty() {
        return conn
            .execute_batch(&format!(
                "CREATE TABLE {STAMP_TABLE} (version INTEGER NOT NULL); \
                 INSERT INTO {STAMP_TABLE} (version) VALUES ({STORAGE_FORMAT});"
            ))
            .map_err(storage);
    }
    if !tables.iter().any(|name| name == STAMP_TABLE) {
        return Err(unsupported(
            "this SQLite file has tables but no Jazz storage format stamp \
             (it was not written by Jazz's SQLite engine)",
        ));
    }
    let versions: Vec<rusqlite::types::Value> = conn
        .prepare(&format!("SELECT version FROM {STAMP_TABLE}"))
        .and_then(|mut statement| {
            statement
                .query_map([], |row| row.get(0))?
                .collect::<Result<_, _>>()
        })
        .map_err(storage)?;
    match versions.as_slice() {
        [rusqlite::types::Value::Integer(STORAGE_FORMAT)] => {
            refuse_message_outbox(conn)?;
            refuse_versioned_names(conn)?;
            refuse_per_table_fates(conn)?;
            refuse_unpositioned_versions(conn)
        }
        [rusqlite::types::Value::Integer(version)] => Err(unsupported(format!(
            "this store was written in Jazz SQLite storage format {version}"
        ))),
        other => Err(Error::new(
            ErrorCode::Storage,
            format!("corrupt storage format stamp: {STAMP_TABLE} holds {other:?}"),
        )),
    }
}

/// Refuse a development store of format 1 from before it kept pending
/// uploads a row per op (SPEC 8.3): its `_jazz_pending` holds each one as
/// an encoded message (`commit_msg`), which this build does not read.
/// Format 1 had not shipped then, so no released store has that layout.
fn refuse_message_outbox(conn: &Connection) -> Result<(), Error> {
    let old: bool = conn
        .query_row(
            "SELECT count(*) > 0 FROM sqlite_master WHERE type = 'table' \
             AND name = '_jazz_pending' AND sql LIKE '%commit\\_msg%' ESCAPE '\\'",
            [],
            |row| row.get(0),
        )
        .map_err(storage)?;
    if old {
        return Err(unsupported(
            "this development store keeps pending uploads as encoded messages, \
             a layout from before format 1 shipped",
        ));
    }
    Ok(())
}

/// Refuse a development store of format 1 from before its catalogue was
/// rows (SPEC 10.2): its `_jazz_names` is keyed by version id rather than
/// by admission number in `_jazz_versions`, and it keeps no schemas to
/// admit again. Format 1 had not shipped then, so no released store has
/// that layout. Checked before any catalogue table is created.
fn refuse_versioned_names(conn: &Connection) -> Result<(), Error> {
    let old: bool = conn
        .query_row(
            "SELECT count(*) > 0 FROM pragma_table_info('_jazz_names') WHERE name = 'version'",
            [],
            |row| row.get(0),
        )
        .map_err(storage)?;
    if old {
        return Err(unsupported(
            "this development store keeps its stored names by schema version, \
             a layout from before format 1 shipped",
        ));
    }
    Ok(())
}

/// Refuse a development store of format 1 from before a commit's record
/// was one `_jazz_transactions` row: it keeps fates in `_jazz_applied` and
/// landings in `_jazz_settled`, by 16-byte node, and its `_jazz_nodes` has
/// no interned code. Format 1 had not shipped then, so no released store
/// has that layout. Checked before any sync table is created.
fn refuse_per_table_fates(conn: &Connection) -> Result<(), Error> {
    let old: bool = conn
        .query_row(
            "SELECT count(*) > 0 FROM sqlite_master WHERE type = 'table' \
             AND name IN ('_jazz_applied', '_jazz_settled') \
             OR (EXISTS (SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = '_jazz_nodes') \
             AND NOT EXISTS (SELECT 1 FROM pragma_table_info('_jazz_nodes') WHERE name = 'n'))",
            [],
            |row| row.get(0),
        )
        .map_err(storage)?;
    if old {
        return Err(unsupported(
            "this development store keeps commit fates in `_jazz_applied` and \
             `_jazz_settled` (or nodes uninterned in `_jazz_nodes`), a layout from \
             before format 1 shipped",
        ));
    }
    Ok(())
}

/// Refuse a development store of format 1 from before every version was
/// stored at a position `(_n, _seq)`: its replica's own versions sit at
/// seqs in a reserved local range, and `_jazz_transactions` has no `sn`.
/// Format 1 had not shipped then, so no released store has that layout.
fn refuse_unpositioned_versions(conn: &Connection) -> Result<(), Error> {
    let old: bool = conn
        .query_row(
            "SELECT EXISTS (SELECT 1 FROM sqlite_master WHERE type = 'table' \
             AND name = '_jazz_transactions') \
             AND NOT EXISTS (SELECT 1 FROM pragma_table_info('_jazz_transactions') \
             WHERE name = 'sn')",
            [],
            |row| row.get(0),
        )
        .map_err(storage)?;
    if old {
        return Err(unsupported(
            "this development store numbers a replica's own versions in a reserved \
             seq range, a layout from before format 1 shipped",
        ));
    }
    Ok(())
}

/// Whether `dir` is a RocksDB directory: the Groove engine's native store.
pub fn is_groove_store(dir: &Path) -> bool {
    dir.join("CURRENT").is_file()
        && std::fs::read_dir(dir).is_ok_and(|entries| {
            entries
                .flatten()
                .any(|entry| entry.file_name().to_string_lossy().starts_with("MANIFEST-"))
        })
}

/// Refuse `dir` if it is a store the Groove engine wrote (RocksDB), with
/// [`ErrorCode::UnsupportedStorageCodecs`]; it is not read or changed.
pub fn refuse_groove_store(dir: &Path) -> Result<(), Error> {
    if is_groove_store(dir) {
        return Err(unsupported(format!(
            "{} is a store written by the Groove engine (RocksDB), before SQLite",
            dir.display()
        )));
    }
    Ok(())
}
