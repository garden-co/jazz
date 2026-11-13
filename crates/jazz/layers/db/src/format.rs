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

use std::path::Path;

use jazz_sql::Row as _;

use crate::options::{Error, ErrorCode};
use crate::values::{Sql, provider_error as storage};

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
pub(crate) async fn admit<S: jazz_sql::Sql>(conn: &mut S) -> Result<(), Error>
where
    S::Error: std::fmt::Display,
{
    // Admission concerns Jazz's namespace, not ownership of the entire file.
    let stamp: Option<String> = conn
        .query_optional(
            "SELECT type FROM sqlite_master WHERE name = ?1 COLLATE NOCASE",
            &[jazz_sql::Param::Text(STAMP_TABLE)],
            |row| row.get(0),
        )
        .await
        .map_err(storage)?;
    if stamp.is_none() {
        let unstamped: bool = conn
            .query_one(
                "SELECT EXISTS (SELECT 1 FROM sqlite_master WHERE lower(name) GLOB '_jazz_*')",
                &[],
                |row| row.get(0),
            )
            .await
            .map_err(storage)?;
        if unstamped || is_groove_sqlite(conn).await? {
            return Err(unsupported(
                "Jazz storage metadata exists without its format stamp, or the file contains a legacy Groove store",
            ));
        }
        conn.execute(
            &format!("CREATE TABLE {STAMP_TABLE} (version INTEGER NOT NULL)"),
            &[],
        )
        .await
        .map_err(storage)?;
        conn.execute(
            &format!("INSERT INTO {STAMP_TABLE} (version) VALUES (?1)"),
            &[jazz_sql::Param::Integer(STORAGE_FORMAT)],
        )
        .await
        .map_err(storage)?;
        return Ok(());
    }
    if stamp.as_deref() != Some("table") {
        return Err(unsupported("Jazz format stamp is not a table"));
    }
    let versions: Vec<Sql> = conn
        .query_rows(&format!("SELECT version FROM {STAMP_TABLE}"), &[], |row| {
            row.get(0)
        })
        .await
        .map_err(storage)?;
    match versions.as_slice() {
        [Sql::Integer(STORAGE_FORMAT)] => Ok(()),
        [Sql::Integer(version)] => Err(unsupported(format!(
            "this store was written in Jazz SQLite storage format {version}"
        ))),
        other => Err(Error::new(
            ErrorCode::Storage,
            format!("corrupt storage format stamp: {STAMP_TABLE} holds {other:?}"),
        )),
    }
}

/// Recognize the legacy ordered-KV encoding by its exact metadata table layout
/// and format marker, without interpreting arbitrary tables as Jazz storage.
async fn is_groove_sqlite<S: jazz_sql::Sql>(conn: &mut S) -> Result<bool, Error>
where
    S::Error: std::fmt::Display,
{
    let ddl: Option<String> = conn
        .query_optional(
            "SELECT sql FROM sqlite_master WHERE type = 'table' AND name = 'meta'",
            &[],
            |row| row.get(0),
        )
        .await
        .map_err(storage)?;
    if ddl.as_deref()
        != Some("CREATE TABLE meta (key TEXT PRIMARY KEY, value BLOB NOT NULL) STRICT")
    {
        return Ok(false);
    }
    conn.query_one(
        "SELECT EXISTS (SELECT 1 FROM meta WHERE key = 'format' AND value = ?1)",
        &[jazz_sql::Param::Blob(b"jazz-groove-ordered-kv")],
        |row| row.get(0),
    )
    .await
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
