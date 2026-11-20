//! The storage format a store's SQLite file was written in.
//!
//! Every file this engine writes carries one row in `_jazz_format`: the
//! version of the layout of its tables, history, sync bookkeeping and large
//! values. A table rather than `PRAGMA user_version`, because Durable Object
//! SQLite refuses that PRAGMA. Opening admits the file before anything
//! writes to it (not even the journal mode), so a refused file is left
//! byte-for-byte as it was:
//!
//! - a file without Jazz metadata is new, even if it holds unrelated tables;
//! - a file stamped with [`STORAGE_FORMAT`] opens;
//! - any other stamp, unstamped Jazz metadata, or a recognized legacy Groove
//!   store is refused with
//!   [`ErrorCode::UnsupportedStorageCodecs`].
//!
//! The Groove engine's native stores were RocksDB directories; a host that
//! keeps its store in a data directory refuses one with
//! [`refuse_groove_store`] instead of opening an empty SQLite file beside it.
//!
//! The pinned corpus in `crates/jazz/tests/storage_corpus.rs` holds a file
//! of each released format; a change to the layout that an older file would
//! not read bumps [`STORAGE_FORMAT`] and adds its corpus.

use crate::storage::Storage;
use rusqlite::OptionalExtension;
use std::path::Path;

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
pub(crate) fn admit(conn: &Storage) -> Result<(), Error> {
    // Only Jazz owns this namespace; other tables may share the file.
    let stamp: Option<String> = conn
        .query_row(
            "SELECT type FROM sqlite_master WHERE name = ?1 COLLATE NOCASE",
            [STAMP_TABLE],
            |row| row.get(0),
        )
        .optional()
        .map_err(storage)?;
    if stamp.is_none() {
        let unstamped: bool = conn
            .query_row(
                "SELECT EXISTS (SELECT 1 FROM sqlite_master WHERE lower(name) GLOB '_jazz_*')",
                [],
                |row| row.get(0),
            )
            .map_err(storage)?;
        if unstamped || is_groove_sqlite(conn)? {
            return Err(unsupported(
                "Jazz storage metadata exists without its format stamp, or the file contains a legacy Groove store",
            ));
        }
        return conn.scope("jazz_format", |conn| {
            conn.execute_batch(&format!(
                "CREATE TABLE {STAMP_TABLE} (version INTEGER NOT NULL); INSERT INTO {STAMP_TABLE} (version) VALUES ({STORAGE_FORMAT});"
            )).map_err(storage)
        });
    }
    if stamp.as_deref() != Some("table") {
        return Err(unsupported("Jazz format stamp is not a table"));
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
        [rusqlite::types::Value::Integer(STORAGE_FORMAT)] => Ok(()),
        [rusqlite::types::Value::Integer(version)] => Err(unsupported(format!(
            "this store was written in Jazz SQLite storage format {version}"
        ))),
        other => Err(Error::new(
            ErrorCode::Storage,
            format!("corrupt storage format stamp: {STAMP_TABLE} holds {other:?}"),
        )),
    }
}

/// Recognize the legacy format by indexed metadata, not by the spelling of DDL.
fn is_groove_sqlite(conn: &Storage) -> Result<bool, Error> {
    let exists: bool = conn.query_row(
        "SELECT EXISTS (SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = 'meta' COLLATE NOCASE)",
        [], |row| row.get(0),
    ).map_err(storage)?;
    if !exists {
        return Ok(false);
    }
    let columns: Vec<(String, i64)> = conn
        .prepare("PRAGMA table_info('meta')")
        .and_then(|mut statement| {
            statement
                .query_map([], |row| Ok((row.get(1)?, row.get(5)?)))?
                .collect()
        })
        .map_err(storage)?;
    let primary: Vec<_> = columns.iter().filter(|(_, pk)| *pk != 0).collect();
    // Avoid scanning another application's unindexed metadata.
    if primary.len() != 1
        || !primary[0].0.eq_ignore_ascii_case("key")
        || !columns
            .iter()
            .any(|(name, _)| name.eq_ignore_ascii_case("value"))
    {
        return Ok(false);
    }
    conn.query_row(
        "SELECT EXISTS (SELECT 1 FROM meta WHERE key = 'format' AND value = ?1)",
        [b"jazz-groove-ordered-kv".as_slice()],
        |row| row.get(0),
    )
    .map_err(storage)
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
