//! SQLite-backed Storage implementation.
//!
//! Uses `rusqlite` with bundled SQLite. Single KV table on a WITHOUT ROWID
//! B-tree, WAL mode, SAVEPOINT-scoped writes. Targets React Native / mobile.

use std::cell::RefCell;
use std::collections::{HashMap, HashSet};
use std::ops::Bound;
use std::path::{Path, PathBuf};

use crate::commit::{Commit, CommitId};
use crate::object::{BranchName, ObjectId};
use crate::query_manager::types::Value;
use crate::sync_manager::DurabilityTier;

use super::{
    CatalogueManifest, CatalogueManifestOp, LoadedBranch, Storage, StorageError,
    storage_core::{
        append_catalogue_manifest_op_core, append_catalogue_manifest_ops_core, append_commit_core,
        create_object_core, delete_commit_core, index_insert_core, index_lookup_core,
        index_range_core, index_remove_core, index_scan_all_core, load_branch_core,
        load_catalogue_manifest_core, load_object_metadata_core, set_branch_tails_core,
        store_ack_tier_core,
    },
};

#[allow(dead_code)]
struct SqliteInner {
    conn: rusqlite::Connection,
    path: PathBuf,
}

pub struct SqliteStorage {
    inner: RefCell<Option<SqliteInner>>,
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
             PRAGMA cache_size = -2000;
             PRAGMA busy_timeout = 5000;
             PRAGMA foreign_keys = OFF;
             CREATE TABLE IF NOT EXISTS kv (
                 key   BLOB PRIMARY KEY,
                 value BLOB NOT NULL
             ) WITHOUT ROWID;",
        )
        .map_err(|e| StorageError::IoError(format!("sqlite init: {e}")))?;

        Ok(Self {
            inner: RefCell::new(Some(SqliteInner {
                conn,
                path: path.to_path_buf(),
            })),
        })
    }

    fn with_inner<T>(
        &self,
        f: impl FnOnce(&SqliteInner) -> Result<T, StorageError>,
    ) -> Result<T, StorageError> {
        let inner = self.inner.borrow();
        let inner = inner
            .as_ref()
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
        match conn.query_row(
            "SELECT value FROM kv WHERE key = ?1",
            rusqlite::params![key.as_bytes()],
            |row| row.get::<_, Vec<u8>>(0),
        ) {
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
            .prepare("SELECT key, value FROM kv WHERE key >= ?1 AND key < ?2 ORDER BY key")
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
            .prepare("SELECT key FROM kv WHERE key >= ?1 AND key < ?2 ORDER BY key")
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

    fn scan_key_range(
        conn: &rusqlite::Connection,
        start: &str,
        end: &str,
    ) -> Result<Vec<String>, StorageError> {
        let mut stmt = conn
            .prepare("SELECT key FROM kv WHERE key >= ?1 AND key < ?2 ORDER BY key")
            .map_err(|e| StorageError::IoError(format!("sqlite prepare scan_key_range: {e}")))?;
        let rows = stmt
            .query_map(rusqlite::params![start.as_bytes(), end.as_bytes()], |row| {
                row.get::<_, Vec<u8>>(0)
            })
            .map_err(|e| StorageError::IoError(format!("sqlite scan_key_range: {e}")))?;
        let mut out = Vec::new();
        for row in rows {
            let key_bytes =
                row.map_err(|e| StorageError::IoError(format!("sqlite scan_key_range row: {e}")))?;
            let key = String::from_utf8(key_bytes)
                .map_err(|e| StorageError::IoError(format!("sqlite key utf8: {e}")))?;
            out.push(key);
        }
        Ok(out)
    }

    fn set(conn: &rusqlite::Connection, key: &str, value: &[u8]) -> Result<(), StorageError> {
        conn.execute(
            "INSERT OR REPLACE INTO kv (key, value) VALUES (?1, ?2)",
            rusqlite::params![key.as_bytes(), value],
        )
        .map(|_| ())
        .map_err(|e| StorageError::IoError(format!("sqlite set: {e}")))
    }

    fn delete(conn: &rusqlite::Connection, key: &str) -> Result<(), StorageError> {
        conn.execute(
            "DELETE FROM kv WHERE key = ?1",
            rusqlite::params![key.as_bytes()],
        )
        .map(|_| ())
        .map_err(|e| StorageError::IoError(format!("sqlite delete: {e}")))
    }
}

impl Storage for SqliteStorage {
    fn create_object(
        &mut self,
        id: ObjectId,
        metadata: HashMap<String, String>,
    ) -> Result<(), StorageError> {
        self.with_inner(|inner| {
            create_object_core(id, metadata, |key, value| {
                Self::set(&inner.conn, key, value)
            })
        })
    }
    fn load_object_metadata(
        &self,
        id: ObjectId,
    ) -> Result<Option<HashMap<String, String>>, StorageError> {
        self.with_inner(|inner| load_object_metadata_core(id, |key| Self::get(&inner.conn, key)))
    }
    fn load_branch(
        &self,
        object_id: ObjectId,
        branch: &BranchName,
    ) -> Result<Option<LoadedBranch>, StorageError> {
        self.with_inner(|inner| {
            load_branch_core(
                object_id,
                branch,
                |key| Self::get(&inner.conn, key),
                |prefix| Self::scan_prefix(&inner.conn, prefix),
            )
        })
    }
    fn append_commit(
        &mut self,
        object_id: ObjectId,
        branch: &BranchName,
        commit: Commit,
    ) -> Result<(), StorageError> {
        self.with_inner(|inner| {
            Self::with_savepoint(&inner.conn, || {
                append_commit_core(
                    object_id,
                    branch,
                    commit,
                    |key| Self::get(&inner.conn, key),
                    |key, value| Self::set(&inner.conn, key, value),
                )
            })
        })
    }
    fn delete_commit(
        &mut self,
        object_id: ObjectId,
        branch: &BranchName,
        commit_id: CommitId,
    ) -> Result<(), StorageError> {
        self.with_inner(|inner| {
            Self::with_savepoint(&inner.conn, || {
                delete_commit_core(
                    object_id,
                    branch,
                    commit_id,
                    |key| Self::get(&inner.conn, key),
                    |key, value| Self::set(&inner.conn, key, value),
                    |key| Self::delete(&inner.conn, key),
                )
            })
        })
    }
    fn set_branch_tails(
        &mut self,
        object_id: ObjectId,
        branch: &BranchName,
        tails: Option<HashSet<CommitId>>,
    ) -> Result<(), StorageError> {
        self.with_inner(|inner| {
            set_branch_tails_core(
                object_id,
                branch,
                tails,
                |key, value| Self::set(&inner.conn, key, value),
                |key| Self::delete(&inner.conn, key),
            )
        })
    }
    fn store_ack_tier(
        &mut self,
        commit_id: CommitId,
        tier: DurabilityTier,
    ) -> Result<(), StorageError> {
        self.with_inner(|inner| {
            Self::with_savepoint(&inner.conn, || {
                store_ack_tier_core(
                    commit_id,
                    tier,
                    |key| Self::get(&inner.conn, key),
                    |key, value| Self::set(&inner.conn, key, value),
                )
            })
        })
    }
    fn append_catalogue_manifest_op(
        &mut self,
        app_id: ObjectId,
        op: CatalogueManifestOp,
    ) -> Result<(), StorageError> {
        self.with_inner(|inner| {
            Self::with_savepoint(&inner.conn, || {
                append_catalogue_manifest_op_core(
                    app_id,
                    op,
                    |key| Self::get(&inner.conn, key),
                    |key, value| Self::set(&inner.conn, key, value),
                )
            })
        })
    }
    fn append_catalogue_manifest_ops(
        &mut self,
        app_id: ObjectId,
        ops: &[CatalogueManifestOp],
    ) -> Result<(), StorageError> {
        self.with_inner(|inner| {
            Self::with_savepoint(&inner.conn, || {
                append_catalogue_manifest_ops_core(
                    app_id,
                    ops,
                    |key| Self::get(&inner.conn, key),
                    |key, value| Self::set(&inner.conn, key, value),
                )
            })
        })
    }
    fn load_catalogue_manifest(
        &self,
        app_id: ObjectId,
    ) -> Result<Option<CatalogueManifest>, StorageError> {
        self.with_inner(|inner| {
            load_catalogue_manifest_core(app_id, |prefix| Self::scan_prefix(&inner.conn, prefix))
        })
    }
    fn index_insert(
        &mut self,
        table: &str,
        column: &str,
        branch: &str,
        value: &Value,
        row_id: ObjectId,
    ) -> Result<(), StorageError> {
        self.with_inner(|inner| {
            index_insert_core(table, column, branch, value, row_id, |key, bytes| {
                Self::set(&inner.conn, key, bytes)
            })
        })
    }
    fn index_remove(
        &mut self,
        table: &str,
        column: &str,
        branch: &str,
        value: &Value,
        row_id: ObjectId,
    ) -> Result<(), StorageError> {
        self.with_inner(|inner| {
            index_remove_core(table, column, branch, value, row_id, |key| {
                Self::delete(&inner.conn, key)
            })
        })
    }
    fn index_lookup(
        &self,
        table: &str,
        column: &str,
        branch: &str,
        value: &Value,
    ) -> Vec<ObjectId> {
        self.with_inner(|inner| {
            Ok(index_lookup_core(table, column, branch, value, |prefix| {
                Self::scan_prefix_keys(&inner.conn, prefix)
            }))
        })
        .unwrap_or_default()
    }
    fn index_range(
        &self,
        table: &str,
        column: &str,
        branch: &str,
        start: Bound<&Value>,
        end: Bound<&Value>,
    ) -> Vec<ObjectId> {
        self.with_inner(|inner| {
            Ok(index_range_core(
                table,
                column,
                branch,
                start,
                end,
                |start_key, end_key| Self::scan_key_range(&inner.conn, start_key, end_key),
            ))
        })
        .unwrap_or_default()
    }
    fn index_scan_all(&self, table: &str, column: &str, branch: &str) -> Vec<ObjectId> {
        self.with_inner(|inner| {
            Ok(index_scan_all_core(table, column, branch, |prefix| {
                Self::scan_prefix_keys(&inner.conn, prefix)
            }))
        })
        .unwrap_or_default()
    }

    fn flush(&self) {
        if let Some(inner) = self.inner.borrow().as_ref() {
            // PASSIVE checkpoint: moves pages from WAL to DB without blocking
            // concurrent readers. This is compaction/cleanup, not a durability
            // barrier — WAL commits are already durable for process crashes.
            let _ = inner.conn.execute_batch("PRAGMA wal_checkpoint(PASSIVE)");
        }
    }

    fn close(&self) -> Result<(), StorageError> {
        let Some(inner) = self.inner.borrow_mut().take() else {
            return Ok(());
        };
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
            storage.create_object(id, meta).unwrap();
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
        let result = storage.load_object_metadata(ObjectId::new());
        assert!(
            result.is_err(),
            "load_object_metadata should return Err after close, got Ok"
        );
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
