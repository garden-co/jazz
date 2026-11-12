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
    admit_with_provider_tables(conn, &[]).await
}

/// A host may reserve table namespaces beside Jazz. Exclusions apply only to
/// initial admission; Jazz's own stamp is always checked and never excluded.
pub(crate) async fn admit_with_provider_tables<S: jazz_sql::Sql>(
    conn: &mut S,
    prefixes: &[&str],
) -> Result<(), Error>
where
    S::Error: std::fmt::Display,
{
    if prefixes.iter().any(|prefix| {
        let prefix = prefix.to_ascii_lowercase();
        prefix.is_empty() || STAMP_TABLE.starts_with(&prefix) || prefix.starts_with("_jazz")
    }) {
        return Err(unsupported("provider table namespace overlaps Jazz"));
    }
    // A damaged header fails here (`file is not a database`), as a storage
    // error, before any write.
    let mut tables: Vec<String> = conn
        .query_rows(
            // Durable Object SQLite keeps its own `_cf_*` tables in the file.
            "SELECT name FROM sqlite_master WHERE type = 'table' \
         AND name NOT LIKE 'sqlite\\_%' ESCAPE '\\' AND name NOT LIKE '\\_cf\\_%' ESCAPE '\\'",
            &[],
            |row| row.get(0),
        )
        .await
        .map_err(storage)?;
    if !prefixes.is_empty() {
        tables.retain(|name| !prefixes.iter().any(|prefix| name.starts_with(prefix)));
    }
    if tables.is_empty() {
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
    if !tables.iter().any(|name| name == STAMP_TABLE) {
        return Err(unsupported(
            "this SQLite file has tables but no Jazz storage format stamp \
             (it was not written by Jazz's SQLite engine)",
        ));
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

#[cfg(all(test, feature = "native"))]
mod shared_file_tests {
    use super::*;

    /// Alice opens a provider-owned file; only the declared namespace is exempt
    /// from empty-file admission. This low-level test targets the format stamp,
    /// which is deliberately not exposed through row APIs.
    #[test]
    fn provider_tables_do_not_hide_foreign_or_unstamped_jazz_tables() {
        for foreign in ["foreign_table", "_jazz_meta"] {
            let db = rusqlite::Connection::open_in_memory().unwrap();
            db.execute_batch(&format!(
                "CREATE TABLE _provider_metadata (v); CREATE TABLE {foreign} (v)"
            ))
            .unwrap();
            let result = jazz_sql::inline(admit_with_provider_tables(
                &mut jazz_sql::native::Native(&db),
                &["_provider_"],
            ));
            assert!(result.is_err());
            assert_eq!(
                db.query_row(
                    "SELECT count(*) FROM sqlite_master WHERE name = '_jazz_format'",
                    [],
                    |r| r.get::<_, i64>(0)
                )
                .unwrap(),
                0
            );
        }
    }

    /// Alice's provider metadata survives initialization and Bob cannot use an
    /// exclusion to admit an unsupported Jazz encoding. Direct SQL is necessary
    /// here to construct a future-version stamp that no public writer emits.
    #[test]
    fn shared_files_keep_format_validation() {
        let db = rusqlite::Connection::open_in_memory().unwrap();
        db.execute_batch(
            "CREATE TABLE _provider_metadata (v); INSERT INTO _provider_metadata VALUES (7)",
        )
        .unwrap();
        jazz_sql::inline(admit_with_provider_tables(
            &mut jazz_sql::native::Native(&db),
            &["_provider_"],
        ))
        .unwrap();
        assert_eq!(
            db.query_row("SELECT v FROM _provider_metadata", [], |r| r
                .get::<_, i64>(0))
                .unwrap(),
            7
        );
        assert!(
            jazz_sql::inline(admit_with_provider_tables(
                &mut jazz_sql::native::Native(&db),
                &["_jazz_"]
            ))
            .is_err()
        );
        db.execute("UPDATE _jazz_format SET version = 999", [])
            .unwrap();
        assert!(
            jazz_sql::inline(admit_with_provider_tables(
                &mut jazz_sql::native::Native(&db),
                &["_provider_"]
            ))
            .is_err()
        );
    }
}
