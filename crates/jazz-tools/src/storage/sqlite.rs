//! SQLite-backed Storage implementation.
//!
//! Uses `rusqlite` with bundled SQLite. Single KV table on a WITHOUT ROWID
//! B-tree, WAL mode. Writes are batched into a lazy explicit transaction that
//! stays open across multiple calls and is committed on `flush()` / `close()`.
//! Per-operation SAVEPOINTs nested inside that transaction provide rollback
//! semantics for individual operations. Targets React Native / mobile.

use std::cell::RefCell;
use std::collections::HashMap;
use std::ops::Bound;
use std::path::{Path, PathBuf};

use crate::commit::{Commit, CommitId};
use crate::object::{BranchName, ObjectId, PrefixBatchCatalog, VisibleCommit, VisibleStateSlots};
use crate::query_manager::types::{BatchBranchKey, BatchId, QueryBranchRef, Value};
use crate::sync_manager::DurabilityTier;

use super::{
    CatalogueManifest, CatalogueManifestOp, LoadedBranch, LoadedBranchTips, PrefixBatchUpdate,
    Storage, StorageError,
    storage_core::{
        adjust_table_prefix_batch_refcount_core, append_catalogue_manifest_op_core,
        append_catalogue_manifest_ops_core, append_commit_core, create_object_core,
        index_insert_core, index_lookup_core, index_range_core, index_remove_core,
        index_scan_all_core, load_branch_core, load_branch_core_existing_object,
        load_branch_tips_core, load_branch_tips_core_existing_object, load_catalogue_manifest_core,
        load_commit_branch_core, load_object_metadata_core, load_prefix_batch_catalog_core,
        load_prefix_head_entries_core, load_prefix_leaf_head_entries_core,
        load_table_prefix_batch_keys_core, load_visible_states_core, object_exists_core,
        replace_branch_core, store_ack_tier_core, store_visible_commit_core,
    },
};

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
            inner: RefCell::new(Some(SqliteInner {
                conn,
                path: path.to_path_buf(),
                write_tx_open: false,
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

    fn with_inner_mut<T>(
        &self,
        f: impl FnOnce(&mut SqliteInner) -> Result<T, StorageError>,
    ) -> Result<T, StorageError> {
        let mut inner = self.inner.borrow_mut();
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

    fn scan_key_range(
        conn: &rusqlite::Connection,
        start: &str,
        end: &str,
    ) -> Result<Vec<String>, StorageError> {
        let mut stmt = conn
            .prepare_cached("SELECT key FROM kv WHERE key >= ?1 AND key < ?2 ORDER BY key")
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
    fn create_object(
        &mut self,
        id: ObjectId,
        metadata: HashMap<String, String>,
    ) -> Result<(), StorageError> {
        self.with_inner_mut(|inner| {
            inner.ensure_write_tx()?;
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
    fn object_exists(&self, id: ObjectId) -> Result<bool, StorageError> {
        self.with_inner(|inner| object_exists_core(id, |key| Self::get(&inner.conn, key)))
    }
    fn load_visible_states(
        &self,
        object_id: ObjectId,
    ) -> Result<Option<VisibleStateSlots>, StorageError> {
        self.with_inner(|inner| {
            load_visible_states_core(object_id, |key| Self::get(&inner.conn, key))
        })
    }
    fn store_visible_commit(
        &mut self,
        object_id: ObjectId,
        prefix: BranchName,
        visible_commit: VisibleCommit,
    ) -> Result<(), StorageError> {
        self.with_inner_mut(|inner| {
            inner.ensure_write_tx()?;
            Self::with_savepoint(&inner.conn, || {
                store_visible_commit_core(
                    object_id,
                    prefix,
                    visible_commit,
                    |key| Self::get(&inner.conn, key),
                    |key, value| Self::set(&inner.conn, key, value),
                )
            })
        })
    }
    fn load_branch(
        &self,
        object_id: ObjectId,
        branch: &QueryBranchRef,
    ) -> Result<Option<LoadedBranch>, StorageError> {
        self.with_inner(|inner| {
            load_branch_core(object_id, branch, |key| Self::get(&inner.conn, key))
        })
    }
    fn load_branch_existing_object(
        &self,
        object_id: ObjectId,
        branch: &QueryBranchRef,
    ) -> Result<Option<LoadedBranch>, StorageError> {
        self.with_inner(|inner| {
            load_branch_core_existing_object(object_id, branch, |key| Self::get(&inner.conn, key))
        })
    }
    fn load_branch_tips(
        &self,
        object_id: ObjectId,
        branch: &QueryBranchRef,
    ) -> Result<Option<LoadedBranchTips>, StorageError> {
        self.with_inner(|inner| {
            load_branch_tips_core(object_id, branch, |key| Self::get(&inner.conn, key))
        })
    }
    fn load_branch_tips_existing_object(
        &self,
        object_id: ObjectId,
        branch: &QueryBranchRef,
    ) -> Result<Option<LoadedBranchTips>, StorageError> {
        self.with_inner(|inner| {
            load_branch_tips_core_existing_object(object_id, branch, |key| {
                Self::get(&inner.conn, key)
            })
        })
    }
    fn load_commit_branch(
        &self,
        object_id: ObjectId,
        commit_id: CommitId,
    ) -> Result<Option<QueryBranchRef>, StorageError> {
        self.with_inner(|inner| {
            load_commit_branch_core(object_id, commit_id, |key| Self::get(&inner.conn, key))
        })
    }
    fn load_prefix_batch_catalog(
        &self,
        object_id: ObjectId,
        prefix: &str,
    ) -> Result<Option<PrefixBatchCatalog>, StorageError> {
        self.with_inner(|inner| {
            load_prefix_batch_catalog_core(object_id, prefix, |key| Self::get(&inner.conn, key))
        })
    }
    fn load_prefix_head_entries(
        &self,
        object_id: ObjectId,
        prefix: &str,
    ) -> Result<Vec<(BatchId, CommitId)>, StorageError> {
        self.with_inner(|inner| {
            load_prefix_head_entries_core(object_id, prefix, |key| Self::get(&inner.conn, key))
        })
    }
    fn load_prefix_leaf_head_entries(
        &self,
        object_id: ObjectId,
        prefix: &str,
    ) -> Result<Vec<(BatchId, CommitId)>, StorageError> {
        self.with_inner(|inner| {
            load_prefix_leaf_head_entries_core(object_id, prefix, |key| Self::get(&inner.conn, key))
        })
    }
    fn load_table_prefix_batch_keys(
        &self,
        table: &str,
        prefix: BranchName,
    ) -> Result<Vec<BatchBranchKey>, StorageError> {
        self.with_inner(|inner| {
            load_table_prefix_batch_keys_core(table, prefix, |key| Self::get(&inner.conn, key))
        })
    }
    fn adjust_table_prefix_batch_refcount(
        &mut self,
        table: &str,
        prefix: BranchName,
        batch_id: BatchId,
        delta: i64,
    ) -> Result<(), StorageError> {
        self.with_inner_mut(|inner| {
            inner.ensure_write_tx()?;
            let branch = QueryBranchRef::from_prefix_name_and_batch(prefix, batch_id);
            Self::with_savepoint(&inner.conn, || {
                adjust_table_prefix_batch_refcount_core(
                    table,
                    &branch,
                    delta,
                    |key| Self::get(&inner.conn, key),
                    |key, value| Self::set(&inner.conn, key, value),
                    |key| Self::delete(&inner.conn, key),
                )
            })
        })
    }
    fn append_commit(
        &mut self,
        object_id: ObjectId,
        branch: &QueryBranchRef,
        commit: &Commit,
        prefix_batch_update: Option<&PrefixBatchUpdate>,
    ) -> Result<(), StorageError> {
        self.with_inner_mut(|inner| {
            inner.ensure_write_tx()?;
            Self::with_savepoint(&inner.conn, || {
                append_commit_core(
                    object_id,
                    branch,
                    commit,
                    prefix_batch_update,
                    |key| Self::get(&inner.conn, key),
                    |key, value| Self::set(&inner.conn, key, value),
                )
            })
        })
    }
    fn replace_branch(
        &mut self,
        object_id: ObjectId,
        branch: &QueryBranchRef,
        commits: Vec<Commit>,
        tails: smolset::SmolSet<[CommitId; 2]>,
    ) -> Result<(), StorageError> {
        self.with_inner_mut(|inner| {
            inner.ensure_write_tx()?;
            Self::with_savepoint(&inner.conn, || {
                replace_branch_core(
                    object_id,
                    branch,
                    commits,
                    tails,
                    |key| Self::get(&inner.conn, key),
                    |key, value| Self::set(&inner.conn, key, value),
                    |key| Self::delete(&inner.conn, key),
                )
            })
        })
    }
    fn store_ack_tier(
        &mut self,
        commit_id: CommitId,
        tier: DurabilityTier,
    ) -> Result<(), StorageError> {
        self.with_inner_mut(|inner| {
            inner.ensure_write_tx()?;
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
        self.with_inner_mut(|inner| {
            inner.ensure_write_tx()?;
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
        self.with_inner_mut(|inner| {
            inner.ensure_write_tx()?;
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
        branch: &QueryBranchRef,
        value: &Value,
        row_id: ObjectId,
    ) -> Result<(), StorageError> {
        self.with_inner_mut(|inner| {
            inner.ensure_write_tx()?;
            let inserted = index_insert_core(
                table,
                column,
                branch,
                value,
                row_id,
                |key| Self::get(&inner.conn, key),
                |key, bytes| Self::set(&inner.conn, key, bytes),
            )?;
            if inserted && matches!(column, "_id" | "_id_deleted") {
                adjust_table_prefix_batch_refcount_core(
                    table,
                    branch,
                    1,
                    |key| Self::get(&inner.conn, key),
                    |key, bytes| Self::set(&inner.conn, key, bytes),
                    |key| Self::delete(&inner.conn, key),
                )?;
            }
            Ok(())
        })
    }
    fn index_remove(
        &mut self,
        table: &str,
        column: &str,
        branch: &QueryBranchRef,
        value: &Value,
        row_id: ObjectId,
    ) -> Result<(), StorageError> {
        self.with_inner_mut(|inner| {
            inner.ensure_write_tx()?;
            let removed = index_remove_core(
                table,
                column,
                branch,
                value,
                row_id,
                |key| Self::get(&inner.conn, key),
                |key| Self::delete(&inner.conn, key),
            )?;
            if removed && matches!(column, "_id" | "_id_deleted") {
                adjust_table_prefix_batch_refcount_core(
                    table,
                    branch,
                    -1,
                    |key| Self::get(&inner.conn, key),
                    |key, bytes| Self::set(&inner.conn, key, bytes),
                    |key| Self::delete(&inner.conn, key),
                )?;
            }
            Ok(())
        })
    }
    fn index_lookup(
        &self,
        table: &str,
        column: &str,
        branch: &QueryBranchRef,
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
        branch: &QueryBranchRef,
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
    fn index_scan_all(&self, table: &str, column: &str, branch: &QueryBranchRef) -> Vec<ObjectId> {
        self.with_inner(|inner| {
            Ok(index_scan_all_core(table, column, branch, |prefix| {
                Self::scan_prefix_keys(&inner.conn, prefix)
            }))
        })
        .unwrap_or_default()
    }

    fn flush_wal(&self) {
        if let Some(inner) = self.inner.borrow_mut().as_mut() {
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
        let Some(mut inner) = self.inner.borrow_mut().take() else {
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
