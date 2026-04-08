//! SQLite-backed Storage implementation.
//!
//! Uses `rusqlite` with bundled SQLite. Single KV table on a WITHOUT ROWID
//! B-tree, WAL mode. Writes are batched into a lazy explicit transaction that
//! stays open across multiple calls and is committed on `flush()` / `close()`.
//! Per-operation SAVEPOINTs nested inside that transaction provide rollback
//! semantics for individual operations. Targets React Native / mobile.

use std::path::{Path, PathBuf};
use std::sync::{Mutex, MutexGuard};

use super::{
    Storage, StorageError,
    storage_core::{
        append_history_region_rows_core, load_history_query_row_version_core,
        load_history_row_version_core, load_visible_query_row_core,
        load_visible_query_row_for_tier_core, load_visible_region_entry_core,
        load_visible_region_frontier_core, load_visible_region_row_core,
        patch_row_region_rows_by_batch_core, raw_table_delete_core, raw_table_get_core,
        raw_table_put_core, raw_table_scan_prefix_core, raw_table_scan_prefix_keys_core,
        raw_table_scan_range_core, raw_table_scan_range_keys_core, scan_history_region_core,
        scan_history_row_versions_core, scan_visible_region_core,
        scan_visible_region_row_versions_core, upsert_visible_region_rows_core,
    },
};
use crate::commit::CommitId;
use crate::object::ObjectId;
use crate::row_regions::{
    HistoryScan, QueryRowVersion, RowState, StoredRowVersion, VisibleRowEntry,
};
use crate::sync_manager::DurabilityTier;

struct SqliteInner {
    conn: rusqlite::Connection,
    #[allow(dead_code)]
    path: PathBuf,
    /// Whether an explicit `BEGIN` transaction is currently open.
    write_tx_open: bool,
}

impl SqliteInner {
    /// Start a write transaction if one isn't already open.
    fn ensure_write_tx(&mut self) -> Result<(), StorageError> {
        if !self.write_tx_open {
            self.conn
                .execute_batch("BEGIN")
                .map_err(|e| StorageError::IoError(format!("sqlite begin: {e}")))?;
            self.write_tx_open = true;
        }
        Ok(())
    }

    /// Commit the open write transaction, if any.
    fn commit_write_tx(&mut self) -> Result<(), StorageError> {
        if self.write_tx_open {
            self.conn
                .execute_batch("COMMIT")
                .map_err(|e| StorageError::IoError(format!("sqlite commit: {e}")))?;
            self.write_tx_open = false;
        }
        Ok(())
    }
}

pub struct SqliteStorage {
    inner: Mutex<Option<SqliteInner>>,
}

impl SqliteStorage {
    /// Compute the lexicographic successor of `prefix` for use as an
    /// exclusive upper bound. Same logic as RocksDB's `prefix_upper_bound`.
    fn prefix_upper_bound(prefix: &[u8]) -> Option<Vec<u8>> {
        let mut bound = prefix.to_vec();
        while let Some(last) = bound.last_mut() {
            if *last < 0xFF {
                *last += 1;
                return Some(bound);
            }
            bound.pop();
        }
        None
    }

    pub fn open(path: impl AsRef<Path>) -> Result<Self, StorageError> {
        let path = path.as_ref();
        let conn = rusqlite::Connection::open(path)
            .map_err(|e| StorageError::IoError(format!("sqlite open: {e}")))?;

        conn.execute_batch(
            "PRAGMA journal_mode = WAL;
             PRAGMA synchronous = NORMAL;
             PRAGMA cache_size = -65536;
             PRAGMA busy_timeout = 5000;
             PRAGMA foreign_keys = OFF;
             CREATE TABLE IF NOT EXISTS kv (
                 key   BLOB PRIMARY KEY,
                 value BLOB NOT NULL
             ) WITHOUT ROWID;",
        )
        .map_err(|e| StorageError::IoError(format!("sqlite init: {e}")))?;

        Ok(Self {
            inner: Mutex::new(Some(SqliteInner {
                conn,
                path: path.to_path_buf(),
                write_tx_open: false,
            })),
        })
    }

    fn lock_inner(&self) -> Result<MutexGuard<'_, Option<SqliteInner>>, StorageError> {
        self.inner
            .lock()
            .map_err(|_| StorageError::IoError("sqlite storage mutex poisoned".to_string()))
    }

    fn with_inner<T>(
        &self,
        f: impl FnOnce(&SqliteInner) -> Result<T, StorageError>,
    ) -> Result<T, StorageError> {
        let inner = self.lock_inner()?;
        let inner = inner
            .as_ref()
            .ok_or_else(|| StorageError::IoError("sqlite storage already closed".to_string()))?;
        f(inner)
    }

    fn with_inner_mut<T>(
        &self,
        f: impl FnOnce(&mut SqliteInner) -> Result<T, StorageError>,
    ) -> Result<T, StorageError> {
        let mut inner = self.lock_inner()?;
        let inner = inner
            .as_mut()
            .ok_or_else(|| StorageError::IoError("sqlite storage already closed".to_string()))?;
        f(inner)
    }

    /// Run `f` inside a SQLite SAVEPOINT. Releases on success, rolls back on error.
    /// Reads within `f` see uncommitted savepoint writes because all operations
    /// share the same connection.
    fn with_savepoint<T>(
        conn: &rusqlite::Connection,
        f: impl FnOnce() -> Result<T, StorageError>,
    ) -> Result<T, StorageError> {
        conn.execute("SAVEPOINT jazz_sp", [])
            .map_err(|e| StorageError::IoError(format!("savepoint start: {e}")))?;
        match f() {
            Ok(v) => {
                conn.execute("RELEASE jazz_sp", [])
                    .map_err(|e| StorageError::IoError(format!("savepoint release: {e}")))?;
                Ok(v)
            }
            Err(e) => {
                let _ = conn.execute("ROLLBACK TO jazz_sp", []);
                let _ = conn.execute("RELEASE jazz_sp", []);
                Err(e)
            }
        }
    }

    fn get(conn: &rusqlite::Connection, key: &str) -> Result<Option<Vec<u8>>, StorageError> {
        let mut stmt = conn
            .prepare_cached("SELECT value FROM kv WHERE key = ?1")
            .map_err(|e| StorageError::IoError(format!("sqlite prepare get: {e}")))?;
        match stmt.query_row(rusqlite::params![key.as_bytes()], |row| {
            row.get::<_, Vec<u8>>(0)
        }) {
            Ok(v) => Ok(Some(v)),
            Err(rusqlite::Error::QueryReturnedNoRows) => Ok(None),
            Err(e) => Err(StorageError::IoError(format!("sqlite get: {e}"))),
        }
    }

    fn scan_prefix(
        conn: &rusqlite::Connection,
        prefix: &str,
    ) -> Result<Vec<(String, Vec<u8>)>, StorageError> {
        let prefix_bytes = prefix.as_bytes();
        let upper = Self::prefix_upper_bound(prefix_bytes)
            .ok_or_else(|| StorageError::IoError("prefix upper bound overflow".to_string()))?;
        let mut stmt = conn
            .prepare_cached("SELECT key, value FROM kv WHERE key >= ?1 AND key < ?2 ORDER BY key")
            .map_err(|e| StorageError::IoError(format!("sqlite prepare scan_prefix: {e}")))?;
        let rows = stmt
            .query_map(rusqlite::params![prefix_bytes, upper.as_slice()], |row| {
                Ok((row.get::<_, Vec<u8>>(0)?, row.get::<_, Vec<u8>>(1)?))
            })
            .map_err(|e| StorageError::IoError(format!("sqlite scan_prefix: {e}")))?;
        let mut out = Vec::new();
        for row in rows {
            let (key_bytes, value) =
                row.map_err(|e| StorageError::IoError(format!("sqlite scan_prefix row: {e}")))?;
            let key = String::from_utf8(key_bytes)
                .map_err(|e| StorageError::IoError(format!("sqlite key utf8: {e}")))?;
            out.push((key, value));
        }
        Ok(out)
    }

    fn scan_prefix_keys(
        conn: &rusqlite::Connection,
        prefix: &str,
    ) -> Result<Vec<String>, StorageError> {
        let prefix_bytes = prefix.as_bytes();
        let upper = Self::prefix_upper_bound(prefix_bytes)
            .ok_or_else(|| StorageError::IoError("prefix upper bound overflow".to_string()))?;
        let mut stmt = conn
            .prepare_cached("SELECT key FROM kv WHERE key >= ?1 AND key < ?2 ORDER BY key")
            .map_err(|e| StorageError::IoError(format!("sqlite prepare scan_prefix_keys: {e}")))?;
        let rows = stmt
            .query_map(rusqlite::params![prefix_bytes, upper.as_slice()], |row| {
                row.get::<_, Vec<u8>>(0)
            })
            .map_err(|e| StorageError::IoError(format!("sqlite scan_prefix_keys: {e}")))?;
        let mut out = Vec::new();
        for row in rows {
            let key_bytes = row
                .map_err(|e| StorageError::IoError(format!("sqlite scan_prefix_keys row: {e}")))?;
            let key = String::from_utf8(key_bytes)
                .map_err(|e| StorageError::IoError(format!("sqlite key utf8: {e}")))?;
            out.push(key);
        }
        Ok(out)
    }

    fn scan_range(
        conn: &rusqlite::Connection,
        start: &str,
        end: &str,
    ) -> Result<Vec<(String, Vec<u8>)>, StorageError> {
        let mut stmt = conn
            .prepare_cached("SELECT key, value FROM kv WHERE key >= ?1 AND key < ?2 ORDER BY key")
            .map_err(|e| StorageError::IoError(format!("sqlite prepare scan_range: {e}")))?;
        let rows = stmt
            .query_map(rusqlite::params![start.as_bytes(), end.as_bytes()], |row| {
                Ok((row.get::<_, Vec<u8>>(0)?, row.get::<_, Vec<u8>>(1)?))
            })
            .map_err(|e| StorageError::IoError(format!("sqlite scan_range: {e}")))?;
        let mut out = Vec::new();
        for row in rows {
            let (key_bytes, value) =
                row.map_err(|e| StorageError::IoError(format!("sqlite scan_range row: {e}")))?;
            let key = String::from_utf8(key_bytes)
                .map_err(|e| StorageError::IoError(format!("sqlite key utf8: {e}")))?;
            out.push((key, value));
        }
        Ok(out)
    }

    fn scan_range_keys(
        conn: &rusqlite::Connection,
        start: &str,
        end: &str,
    ) -> Result<Vec<String>, StorageError> {
        let mut stmt = conn
            .prepare_cached("SELECT key FROM kv WHERE key >= ?1 AND key < ?2 ORDER BY key")
            .map_err(|e| StorageError::IoError(format!("sqlite prepare scan_range_keys: {e}")))?;
        let rows = stmt
            .query_map(rusqlite::params![start.as_bytes(), end.as_bytes()], |row| {
                row.get::<_, Vec<u8>>(0)
            })
            .map_err(|e| StorageError::IoError(format!("sqlite scan_range_keys: {e}")))?;
        let mut out = Vec::new();
        for row in rows {
            let key_bytes =
                row.map_err(|e| StorageError::IoError(format!("sqlite scan_range_keys row: {e}")))?;
            let key = String::from_utf8(key_bytes)
                .map_err(|e| StorageError::IoError(format!("sqlite key utf8: {e}")))?;
            out.push(key);
        }
        Ok(out)
    }

    fn set(conn: &rusqlite::Connection, key: &str, value: &[u8]) -> Result<(), StorageError> {
        conn.prepare_cached("INSERT OR REPLACE INTO kv (key, value) VALUES (?1, ?2)")
            .map_err(|e| StorageError::IoError(format!("sqlite prepare set: {e}")))?
            .execute(rusqlite::params![key.as_bytes(), value])
            .map(|_| ())
            .map_err(|e| StorageError::IoError(format!("sqlite set: {e}")))
    }

    fn delete(conn: &rusqlite::Connection, key: &str) -> Result<(), StorageError> {
        conn.prepare_cached("DELETE FROM kv WHERE key = ?1")
            .map_err(|e| StorageError::IoError(format!("sqlite prepare delete: {e}")))?
            .execute(rusqlite::params![key.as_bytes()])
            .map(|_| ())
            .map_err(|e| StorageError::IoError(format!("sqlite delete: {e}")))
    }
}

impl Storage for SqliteStorage {
    fn raw_table_put(&mut self, table: &str, key: &str, value: &[u8]) -> Result<(), StorageError> {
        self.with_inner_mut(|inner| {
            inner.ensure_write_tx()?;
            Self::with_savepoint(&inner.conn, || {
                raw_table_put_core(table, key, value, |storage_key, bytes| {
                    Self::set(&inner.conn, storage_key, bytes)
                })
            })
        })
    }

    fn raw_table_delete(&mut self, table: &str, key: &str) -> Result<(), StorageError> {
        self.with_inner_mut(|inner| {
            inner.ensure_write_tx()?;
            Self::with_savepoint(&inner.conn, || {
                raw_table_delete_core(table, key, |storage_key| {
                    Self::delete(&inner.conn, storage_key)
                })
            })
        })
    }

    fn raw_table_get(&self, table: &str, key: &str) -> Result<Option<Vec<u8>>, StorageError> {
        self.with_inner(|inner| {
            raw_table_get_core(table, key, |storage_key| {
                Self::get(&inner.conn, storage_key)
            })
        })
    }

    fn raw_table_scan_prefix(
        &self,
        table: &str,
        prefix: &str,
    ) -> Result<super::RawTableRows, StorageError> {
        self.with_inner(|inner| {
            raw_table_scan_prefix_core(table, prefix, |storage_prefix| {
                Self::scan_prefix(&inner.conn, storage_prefix)
            })
        })
    }

    fn raw_table_scan_prefix_keys(
        &self,
        table: &str,
        prefix: &str,
    ) -> Result<super::RawTableKeys, StorageError> {
        self.with_inner(|inner| {
            raw_table_scan_prefix_keys_core(table, prefix, |storage_prefix| {
                Self::scan_prefix_keys(&inner.conn, storage_prefix)
            })
        })
    }

    fn raw_table_scan_range(
        &self,
        table: &str,
        start: Option<&str>,
        end: Option<&str>,
    ) -> Result<super::RawTableRows, StorageError> {
        self.with_inner(|inner| {
            raw_table_scan_range_core(table, start, end, |start_key, end_key| {
                Self::scan_range(&inner.conn, start_key, end_key)
            })
        })
    }

    fn raw_table_scan_range_keys(
        &self,
        table: &str,
        start: Option<&str>,
        end: Option<&str>,
    ) -> Result<super::RawTableKeys, StorageError> {
        self.with_inner(|inner| {
            raw_table_scan_range_keys_core(table, start, end, |start_key, end_key| {
                Self::scan_range_keys(&inner.conn, start_key, end_key)
            })
        })
    }

    fn append_history_region_rows(
        &mut self,
        table: &str,
        rows: &[StoredRowVersion],
    ) -> Result<(), StorageError> {
        self.with_inner_mut(|inner| {
            inner.ensure_write_tx()?;
            Self::with_savepoint(&inner.conn, || {
                append_history_region_rows_core(table, rows, |key, bytes| {
                    Self::set(&inner.conn, key, bytes)
                })
            })
        })
    }

    fn upsert_visible_region_rows(
        &mut self,
        table: &str,
        entries: &[VisibleRowEntry],
    ) -> Result<(), StorageError> {
        self.with_inner_mut(|inner| {
            inner.ensure_write_tx()?;
            Self::with_savepoint(&inner.conn, || {
                upsert_visible_region_rows_core(table, entries, |key, bytes| {
                    Self::set(&inner.conn, key, bytes)
                })
            })
        })
    }

    fn patch_row_region_rows_by_batch(
        &mut self,
        table: &str,
        batch_id: crate::row_regions::BatchId,
        state: Option<RowState>,
        confirmed_tier: Option<DurabilityTier>,
    ) -> Result<(), StorageError> {
        self.with_inner_mut(|inner| {
            inner.ensure_write_tx()?;
            Self::with_savepoint(&inner.conn, || {
                patch_row_region_rows_by_batch_core(
                    table,
                    batch_id,
                    state,
                    confirmed_tier,
                    |prefix| Self::scan_prefix(&inner.conn, prefix),
                    |key, bytes| Self::set(&inner.conn, key, bytes),
                )
            })
        })
    }

    fn scan_visible_region(
        &self,
        table: &str,
        branch: &str,
    ) -> Result<Vec<StoredRowVersion>, StorageError> {
        self.with_inner(|inner| {
            scan_visible_region_core(table, branch, |prefix| {
                Self::scan_prefix(&inner.conn, prefix)
            })
        })
    }

    fn load_visible_region_row(
        &self,
        table: &str,
        branch: &str,
        row_id: ObjectId,
    ) -> Result<Option<StoredRowVersion>, StorageError> {
        self.with_inner(|inner| {
            load_visible_region_row_core(table, branch, row_id, |key| Self::get(&inner.conn, key))
        })
    }

    fn load_visible_query_row(
        &self,
        table: &str,
        branch: &str,
        row_id: ObjectId,
    ) -> Result<Option<QueryRowVersion>, StorageError> {
        self.with_inner(|inner| {
            load_visible_query_row_core(table, branch, row_id, |key| Self::get(&inner.conn, key))
        })
    }

    fn load_visible_region_entry(
        &self,
        table: &str,
        branch: &str,
        row_id: ObjectId,
    ) -> Result<Option<VisibleRowEntry>, StorageError> {
        self.with_inner(|inner| {
            load_visible_region_entry_core(table, branch, row_id, |key| Self::get(&inner.conn, key))
        })
    }

    fn load_visible_query_row_for_tier(
        &self,
        table: &str,
        branch: &str,
        row_id: ObjectId,
        required_tier: DurabilityTier,
    ) -> Result<Option<QueryRowVersion>, StorageError> {
        self.with_inner(|inner| {
            load_visible_query_row_for_tier_core(table, branch, row_id, required_tier, |key| {
                Self::get(&inner.conn, key)
            })
        })
    }

    fn load_visible_region_frontier(
        &self,
        table: &str,
        branch: &str,
        row_id: ObjectId,
    ) -> Result<Option<Vec<CommitId>>, StorageError> {
        self.with_inner(|inner| {
            load_visible_region_frontier_core(table, branch, row_id, |key| {
                Self::get(&inner.conn, key)
            })
        })
    }

    fn scan_visible_region_row_versions(
        &self,
        table: &str,
        row_id: ObjectId,
    ) -> Result<Vec<StoredRowVersion>, StorageError> {
        self.with_inner(|inner| {
            scan_visible_region_row_versions_core(
                table,
                row_id,
                |prefix| Self::scan_prefix(&inner.conn, prefix),
                |key| Self::get(&inner.conn, key),
            )
        })
    }

    fn scan_history_row_versions(
        &self,
        table: &str,
        row_id: ObjectId,
    ) -> Result<Vec<StoredRowVersion>, StorageError> {
        self.with_inner(|inner| {
            scan_history_row_versions_core(table, row_id, |prefix| {
                Self::scan_prefix(&inner.conn, prefix)
            })
        })
    }

    fn load_history_row_version(
        &self,
        table: &str,
        row_id: ObjectId,
        version_id: CommitId,
    ) -> Result<Option<StoredRowVersion>, StorageError> {
        self.with_inner(|inner| {
            load_history_row_version_core(table, row_id, version_id, |key| {
                Self::get(&inner.conn, key)
            })
        })
    }

    fn load_history_query_row_version(
        &self,
        table: &str,
        row_id: ObjectId,
        version_id: CommitId,
    ) -> Result<Option<QueryRowVersion>, StorageError> {
        self.with_inner(|inner| {
            load_history_query_row_version_core(table, row_id, version_id, |key| {
                Self::get(&inner.conn, key)
            })
        })
    }

    fn scan_history_region(
        &self,
        table: &str,
        branch: &str,
        scan: HistoryScan,
    ) -> Result<Vec<StoredRowVersion>, StorageError> {
        self.with_inner(|inner| {
            scan_history_region_core(table, branch, scan, |prefix| {
                Self::scan_prefix(&inner.conn, prefix)
            })
        })
    }

    fn flush_wal(&self) {
        let Ok(mut inner) = self.lock_inner() else {
            return;
        };
        if let Some(inner) = inner.as_mut() {
            // Commit the open write transaction so writes land in the WAL
            // and survive a process crash.
            let _ = inner.commit_write_tx();
            // PASSIVE checkpoint: moves WAL pages into the main db file without
            // blocking concurrent readers.
            let _ = inner.conn.execute_batch("PRAGMA wal_checkpoint(PASSIVE)");
        }
    }

    fn flush(&self) {
        self.flush_wal();
    }

    fn close(&self) -> Result<(), StorageError> {
        let Some(mut inner) = self.lock_inner()?.take() else {
            return Ok(());
        };
        // Commit any pending writes before closing.
        inner.commit_write_tx()?;
        // Best-effort compaction before dropping the connection.
        let _ = inner.conn.execute_batch("PRAGMA wal_checkpoint(PASSIVE)");
        drop(inner);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn close_releases_lock_for_reopen() {
        let temp_dir = tempfile::TempDir::new().unwrap();
        let db_path = temp_dir.path().join("test.sqlite");
        let storage = SqliteStorage::open(&db_path).unwrap();
        storage.close().unwrap();
        let reopened = SqliteStorage::open(&db_path).unwrap();
        reopened.close().unwrap();
    }

    #[test]
    fn flush_does_not_panic() {
        use crate::object::ObjectId;
        use std::collections::HashMap;

        let dir = tempfile::TempDir::new().unwrap();
        let path = dir.path().join("test.sqlite");
        let mut storage = SqliteStorage::open(&path).unwrap();

        for _ in 0..10 {
            let id = ObjectId::new();
            let mut meta = HashMap::new();
            meta.insert("k".to_string(), "v".to_string());
            storage.put_metadata(id, meta).unwrap();
        }

        // flush() should not panic or return an error (it returns ())
        storage.flush();
    }

    #[test]
    fn operations_fail_after_close() {
        use crate::object::ObjectId;

        let dir = tempfile::TempDir::new().unwrap();
        let path = dir.path().join("test.sqlite");
        let storage = SqliteStorage::open(&path).unwrap();
        storage.close().unwrap();

        // Storage is closed but NOT yet dropped.
        // A real close() takes the inner; the next call must return Err, not succeed or panic.
        let result = storage.load_metadata(ObjectId::new());
        assert!(
            result.is_err(),
            "load_metadata should return Err after close, got Ok"
        );
    }

    #[test]
    fn storage_is_send() {
        fn assert_send<T: Send>() {}
        assert_send::<SqliteStorage>();
    }

    mod sqlite_conformance {
        use crate::storage::Storage;
        use crate::storage::sqlite::SqliteStorage;
        use crate::storage_conformance_tests_persistent;

        storage_conformance_tests_persistent!(
            sqlite,
            || {
                let dir = tempfile::TempDir::new().unwrap();
                let path = dir.path().join("test.sqlite");
                let storage = SqliteStorage::open(&path).unwrap();
                // Leak TempDir so the directory lives as long as the storage.
                std::mem::forget(dir);
                Box::new(storage) as Box<dyn Storage>
            },
            |path: &std::path::Path| {
                Box::new(SqliteStorage::open(path.join("test.sqlite")).unwrap()) as Box<dyn Storage>
            }
        );
    }
}
