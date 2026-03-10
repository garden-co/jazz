//! SurrealKV-backed Storage implementation.
//!
//! Uses a single SurrealKV tree with key-encoded namespaces for all data:
//! objects, commits, ack tiers, catalogue manifest ops, and indices.
//!
//! Key encoding scheme (all keys are UTF-8 strings with hex-encoded binary parts):
//!
//! ```text
//! "obj:{uuid}:meta"                                       → JSON metadata
//! "obj:{uuid}:br:{branch}:tips"                           → JSON HashSet<CommitId>
//! "obj:{uuid}:br:{branch}:c:{commit_uuid}"                → JSON Commit
//! "ack:{commit_hex}"                                      → JSON HashSet<DurabilityTier>
//! "catman:{app_uuid}:op:{object_uuid}"                    → JSON CatalogueManifestOp
//! "idx:{table}:{col}:{branch}:{hex_encoded_value}:{uuid}" → empty (existence is the signal)
//! ```

use std::collections::{HashMap, HashSet};
use std::ops::Bound;
use std::path::Path;
use std::sync::OnceLock;
use std::sync::mpsc;

use surrealkv::{
    Durability as SurrealDurability, LSMIterator, Mode as SurrealMode,
    Transaction as SurrealTransaction, Tree as SurrealTree, TreeBuilder as SurrealTreeBuilder,
};
use tokio::runtime::{Builder as TokioRuntimeBuilder, Runtime as TokioRuntime};

use crate::commit::{Commit, CommitId};
use crate::object::{BranchName, ObjectId};
use crate::query_manager::types::Value;
use crate::sync_manager::DurabilityTier;

use super::{
    CatalogueManifest, CatalogueManifestOp, IndexScanDirection, LoadedBranch, OrderedIndexCursor,
    OrderedIndexScan, Storage, StorageError,
    key_codec::increment_bytes,
    storage_core::{
        OrderedScanCollector, append_catalogue_manifest_op_core,
        append_catalogue_manifest_ops_core, append_commit_core, create_object_core,
        delete_commit_core, index_insert_core, index_lookup_core, index_range_core,
        index_remove_core, index_scan_all_core, load_branch_core, load_catalogue_manifest_core,
        load_object_metadata_core, ordered_index_scan_bounds, set_branch_tails_core,
        store_ack_tier_core,
    },
};

/// Minimum memtable size for SurrealKV.
const MIN_MEMTABLE_SIZE: usize = 4 * 1024 * 1024;

/// A dedicated key used for `flush()` durability barriers.
const FLUSH_MARKER_KEY: &str = "sys:flush_marker";

pub struct SurrealKvStorage {
    tree: SurrealTree,
    runtime: &'static TokioRuntime,
}

trait EventualTxnAdapter {
    fn get(&self, key: &str) -> Result<Option<Vec<u8>>, StorageError>;
    fn set(&self, key: &str, value: &[u8]) -> Result<(), StorageError>;
    fn delete(&self, key: &str) -> Result<(), StorageError>;
}

impl EventualTxnAdapter for std::cell::RefCell<SurrealTransaction> {
    fn get(&self, key: &str) -> Result<Option<Vec<u8>>, StorageError> {
        let txn = self.borrow();
        SurrealKvStorage::txn_get(&txn, key)
    }

    fn set(&self, key: &str, value: &[u8]) -> Result<(), StorageError> {
        let mut txn = self.borrow_mut();
        SurrealKvStorage::txn_set(&mut txn, key, value)
    }

    fn delete(&self, key: &str) -> Result<(), StorageError> {
        let mut txn = self.borrow_mut();
        SurrealKvStorage::txn_delete(&mut txn, key)
    }
}

impl SurrealKvStorage {
    /// Open a file-backed SurrealKvStorage at the given path.
    pub fn open(path: impl AsRef<Path>, cache_size_bytes: usize) -> Result<Self, StorageError> {
        let runtime = shared_runtime();

        let tree = {
            let _guard = runtime.enter();
            SurrealTreeBuilder::new()
                .with_path(path.as_ref().to_path_buf())
                .with_level_count(4)
                .with_max_memtable_size(cache_size_bytes.max(MIN_MEMTABLE_SIZE))
                .without_compression()
                .build()
                .map_err(|e| StorageError::IoError(format!("surrealkv open: {}", e)))?
        };

        Ok(Self { tree, runtime })
    }

    fn begin_read_txn(&self) -> Result<SurrealTransaction, StorageError> {
        let _guard = self.runtime.enter();
        self.tree
            .begin_with_mode(SurrealMode::ReadOnly)
            .map_err(|e| StorageError::IoError(format!("surrealkv begin read txn: {}", e)))
    }

    fn begin_write_txn(
        &self,
        durability: SurrealDurability,
    ) -> Result<SurrealTransaction, StorageError> {
        let _guard = self.runtime.enter();
        self.tree
            .begin()
            .map(|txn| txn.with_durability(durability))
            .map_err(|e| StorageError::IoError(format!("surrealkv begin write txn: {}", e)))
    }

    /// Close the underlying SurrealKV tree and release lock files.
    pub fn close(&self) -> Result<(), StorageError> {
        let tree = self.tree.clone();
        let (tx, rx) = mpsc::sync_channel(1);
        self.runtime.spawn(async move {
            let _ = tx.send(tree.close().await);
        });
        let close_result = rx
            .recv()
            .map_err(|e| StorageError::IoError(format!("surrealkv close recv: {}", e)))?;
        close_result.map_err(|e| StorageError::IoError(format!("surrealkv close: {}", e)))
    }

    fn commit_txn(&self, txn: &mut SurrealTransaction) -> Result<(), StorageError> {
        futures::executor::block_on(async { txn.commit().await })
            .map_err(|e| StorageError::IoError(format!("surrealkv commit: {}", e)))
    }

    fn txn_get(txn: &SurrealTransaction, key: &str) -> Result<Option<Vec<u8>>, StorageError> {
        txn.get(key.as_bytes())
            .map_err(|e| StorageError::IoError(format!("surrealkv get: {}", e)))
    }

    fn txn_set(txn: &mut SurrealTransaction, key: &str, value: &[u8]) -> Result<(), StorageError> {
        txn.set(key.as_bytes(), value)
            .map_err(|e| StorageError::IoError(format!("surrealkv set: {}", e)))
    }

    fn txn_delete(txn: &mut SurrealTransaction, key: &str) -> Result<(), StorageError> {
        txn.delete(key.as_bytes())
            .map_err(|e| StorageError::IoError(format!("surrealkv delete: {}", e)))
    }

    fn with_eventual_write<R>(
        &self,
        op: impl FnOnce(&dyn EventualTxnAdapter) -> Result<R, StorageError>,
    ) -> Result<R, StorageError> {
        let txn = std::cell::RefCell::new(self.begin_write_txn(SurrealDurability::Eventual)?);
        let output = op(&txn)?;
        let mut txn = txn.into_inner();
        self.commit_txn(&mut txn)?;
        Ok(output)
    }

    fn scan_prefix(
        txn: &SurrealTransaction,
        prefix: &str,
    ) -> Result<Vec<(String, Vec<u8>)>, StorageError> {
        let mut end = prefix.as_bytes().to_vec();
        increment_bytes(&mut end);
        Self::scan_range(txn, prefix.as_bytes(), &end)
    }

    fn scan_range(
        txn: &SurrealTransaction,
        start: &[u8],
        end: &[u8],
    ) -> Result<Vec<(String, Vec<u8>)>, StorageError> {
        let mut iter = txn
            .range(start, end)
            .map_err(|e| StorageError::IoError(format!("surrealkv range: {}", e)))?;

        let mut out = Vec::new();
        let mut has_more = iter
            .seek_first()
            .map_err(|e| StorageError::IoError(format!("surrealkv seek_first: {}", e)))?;

        while has_more && iter.valid() {
            let key = String::from_utf8(iter.key().user_key().to_vec())
                .map_err(|e| StorageError::IoError(format!("surrealkv invalid key utf8: {}", e)))?;
            let value = iter
                .value()
                .map_err(|e| StorageError::IoError(format!("surrealkv iter value: {}", e)))?;
            out.push((key, value));
            has_more = iter
                .next()
                .map_err(|e| StorageError::IoError(format!("surrealkv iter next: {}", e)))?;
        }

        Ok(out)
    }

    fn scan_prefix_keys(
        txn: &SurrealTransaction,
        prefix: &str,
    ) -> Result<Vec<String>, StorageError> {
        Ok(Self::scan_prefix(txn, prefix)?
            .into_iter()
            .map(|(k, _)| k)
            .collect())
    }

    fn scan_key_range(
        txn: &SurrealTransaction,
        start: &str,
        end: &str,
    ) -> Result<Vec<String>, StorageError> {
        Ok(Self::scan_range(txn, start.as_bytes(), end.as_bytes())?
            .into_iter()
            .map(|(k, _)| k)
            .collect())
    }

    fn scan_ordered(
        txn: &SurrealTransaction,
        scan: OrderedIndexScan<'_>,
    ) -> Result<Vec<OrderedIndexCursor>, StorageError> {
        let Some((start_key, end_key)) = ordered_index_scan_bounds(scan) else {
            return Ok(Vec::new());
        };

        let mut collector = OrderedScanCollector::with_cursor(
            scan.direction,
            scan.take,
            scan.table,
            scan.column,
            scan.branch,
            scan.resume_after,
        );
        if !collector.should_continue() {
            return Ok(collector.finish());
        }

        let mut iter = txn
            .range(start_key.as_bytes(), end_key.as_bytes())
            .map_err(|e| StorageError::IoError(format!("surrealkv range: {}", e)))?;

        let mut has_more = match scan.direction {
            IndexScanDirection::Ascending => iter
                .seek_first()
                .map_err(|e| StorageError::IoError(format!("surrealkv seek_first: {}", e)))?,
            IndexScanDirection::Descending => iter
                .seek_last()
                .map_err(|e| StorageError::IoError(format!("surrealkv seek_last: {}", e)))?,
        };

        while has_more && iter.valid() {
            let key = std::str::from_utf8(iter.key().user_key())
                .map_err(|e| StorageError::IoError(format!("surrealkv invalid key utf8: {}", e)))?;
            if !collector.consume_key(key) {
                break;
            }

            has_more = match scan.direction {
                IndexScanDirection::Ascending => iter
                    .next()
                    .map_err(|e| StorageError::IoError(format!("surrealkv iter next: {}", e)))?,
                IndexScanDirection::Descending => iter
                    .prev()
                    .map_err(|e| StorageError::IoError(format!("surrealkv iter prev: {}", e)))?,
            };
        }

        Ok(collector.finish())
    }
}

impl Drop for SurrealKvStorage {
    fn drop(&mut self) {
        // If we're already inside a Tokio runtime, let SurrealKV's Tree::drop()
        // schedule the async close itself.
        if tokio::runtime::Handle::try_current().is_ok() {
            return;
        }

        // Best-effort close so lock files are released before drop.
        let _ = self.close();
    }
}

fn shared_runtime() -> &'static TokioRuntime {
    static RUNTIME: OnceLock<TokioRuntime> = OnceLock::new();
    RUNTIME.get_or_init(|| {
        TokioRuntimeBuilder::new_multi_thread()
            .worker_threads(2)
            .enable_all()
            .build()
            .expect("build surrealkv shared runtime")
    })
}

impl Storage for SurrealKvStorage {
    // ================================================================
    // Object storage
    // ================================================================

    fn create_object(
        &mut self,
        id: ObjectId,
        metadata: HashMap<String, String>,
    ) -> Result<(), StorageError> {
        self.with_eventual_write(|txn| {
            create_object_core(id, metadata, |key, value| txn.set(key, value))
        })
    }

    fn load_object_metadata(
        &self,
        id: ObjectId,
    ) -> Result<Option<HashMap<String, String>>, StorageError> {
        let txn = self.begin_read_txn()?;
        load_object_metadata_core(id, |key| Self::txn_get(&txn, key))
    }

    fn load_branch(
        &self,
        object_id: ObjectId,
        branch: &BranchName,
    ) -> Result<Option<LoadedBranch>, StorageError> {
        let txn = self.begin_read_txn()?;
        load_branch_core(
            object_id,
            branch,
            |key| Self::txn_get(&txn, key),
            |prefix| Self::scan_prefix(&txn, prefix),
        )
    }

    fn append_commit(
        &mut self,
        object_id: ObjectId,
        branch: &BranchName,
        commit: Commit,
    ) -> Result<(), StorageError> {
        self.with_eventual_write(|txn| {
            append_commit_core(
                object_id,
                branch,
                commit,
                |key| txn.get(key),
                |key, value| txn.set(key, value),
            )
        })
    }

    fn delete_commit(
        &mut self,
        object_id: ObjectId,
        branch: &BranchName,
        commit_id: CommitId,
    ) -> Result<(), StorageError> {
        self.with_eventual_write(|txn| {
            delete_commit_core(
                object_id,
                branch,
                commit_id,
                |key| txn.get(key),
                |key, value| txn.set(key, value),
                |key| txn.delete(key),
            )
        })
    }

    fn set_branch_tails(
        &mut self,
        object_id: ObjectId,
        branch: &BranchName,
        tails: Option<HashSet<CommitId>>,
    ) -> Result<(), StorageError> {
        self.with_eventual_write(|txn| {
            set_branch_tails_core(
                object_id,
                branch,
                tails,
                |key, value| txn.set(key, value),
                |key| txn.delete(key),
            )
        })
    }

    // ================================================================
    // Persistence ack storage
    // ================================================================

    fn store_ack_tier(
        &mut self,
        commit_id: CommitId,
        tier: DurabilityTier,
    ) -> Result<(), StorageError> {
        self.with_eventual_write(|txn| {
            store_ack_tier_core(
                commit_id,
                tier,
                |key| txn.get(key),
                |key, value| txn.set(key, value),
            )
        })
    }

    fn append_catalogue_manifest_op(
        &mut self,
        app_id: ObjectId,
        op: CatalogueManifestOp,
    ) -> Result<(), StorageError> {
        self.with_eventual_write(|txn| {
            append_catalogue_manifest_op_core(
                app_id,
                op,
                |key| txn.get(key),
                |key, value| txn.set(key, value),
            )
        })
    }

    fn append_catalogue_manifest_ops(
        &mut self,
        app_id: ObjectId,
        ops: &[CatalogueManifestOp],
    ) -> Result<(), StorageError> {
        self.with_eventual_write(|txn| {
            append_catalogue_manifest_ops_core(
                app_id,
                ops,
                |key| txn.get(key),
                |key, value| txn.set(key, value),
            )
        })
    }

    fn load_catalogue_manifest(
        &self,
        app_id: ObjectId,
    ) -> Result<Option<CatalogueManifest>, StorageError> {
        let txn = self.begin_read_txn()?;
        load_catalogue_manifest_core(app_id, |prefix| Self::scan_prefix(&txn, prefix))
    }

    // ================================================================
    // Index operations
    // ================================================================

    fn index_insert(
        &mut self,
        table: &str,
        column: &str,
        branch: &str,
        value: &Value,
        row_id: ObjectId,
    ) -> Result<(), StorageError> {
        tracing::trace!(table, column, branch, ?row_id, "index_insert");
        self.with_eventual_write(|txn| {
            index_insert_core(table, column, branch, value, row_id, |key, bytes| {
                txn.set(key, bytes)
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
        tracing::trace!(table, column, branch, ?row_id, "index_remove");
        self.with_eventual_write(|txn| {
            index_remove_core(table, column, branch, value, row_id, |key| txn.delete(key))
        })
    }

    fn index_lookup(
        &self,
        table: &str,
        column: &str,
        branch: &str,
        value: &Value,
    ) -> Vec<ObjectId> {
        tracing::trace!(table, column, branch, "index_lookup");
        let Ok(txn) = self.begin_read_txn() else {
            return Vec::new();
        };
        index_lookup_core(table, column, branch, value, |prefix| {
            Self::scan_prefix_keys(&txn, prefix)
        })
    }

    fn index_range(
        &self,
        table: &str,
        column: &str,
        branch: &str,
        start: Bound<&Value>,
        end: Bound<&Value>,
    ) -> Vec<ObjectId> {
        let Ok(txn) = self.begin_read_txn() else {
            return Vec::new();
        };
        index_range_core(table, column, branch, start, end, |start_key, end_key| {
            Self::scan_key_range(&txn, start_key, end_key)
        })
    }

    fn index_scan_ordered(&self, scan: OrderedIndexScan<'_>) -> Vec<OrderedIndexCursor> {
        let Ok(txn) = self.begin_read_txn() else {
            return Vec::new();
        };
        Self::scan_ordered(&txn, scan).unwrap_or_default()
    }

    fn index_scan_all(&self, table: &str, column: &str, branch: &str) -> Vec<ObjectId> {
        let Ok(txn) = self.begin_read_txn() else {
            return Vec::new();
        };
        index_scan_all_core(table, column, branch, |prefix| {
            Self::scan_prefix_keys(&txn, prefix)
        })
    }

    fn flush(&self) {
        let _span = tracing::debug_span!("SurrealKvStorage::flush").entered();
        let Ok(mut txn) = self.begin_write_txn(SurrealDurability::Immediate) else {
            return;
        };
        if Self::txn_set(&mut txn, FLUSH_MARKER_KEY, b"1").is_ok() {
            let _ = self.commit_txn(&mut txn);
        }
    }

    fn flush_wal(&self) {
        let _span = tracing::debug_span!("SurrealKvStorage::flush_wal").entered();
        self.flush();
    }

    fn close(&self) -> Result<(), StorageError> {
        SurrealKvStorage::close(self)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use smallvec::smallvec;

    fn make_commit(content: &[u8]) -> Commit {
        Commit {
            parents: smallvec![],
            content: content.to_vec(),
            timestamp: 12345,
            author: ObjectId::new(),
            metadata: None,
            stored_state: Default::default(),
            ack_state: Default::default(),
        }
    }

    fn test_storage() -> (tempfile::TempDir, SurrealKvStorage) {
        let temp_dir = tempfile::TempDir::new().unwrap();
        let db_path = temp_dir.path().join("test.surrealkv");
        let storage = SurrealKvStorage::open(&db_path, 8 * 1024 * 1024).unwrap();
        (temp_dir, storage)
    }

    #[test]
    fn surrealkv_object_roundtrip() {
        let (_temp_dir, mut storage) = test_storage();

        let id = ObjectId::new();
        let mut metadata = HashMap::new();
        metadata.insert(
            crate::metadata::MetadataKey::Table.to_string(),
            "users".to_string(),
        );
        metadata.insert("app".to_string(), "test".to_string());

        storage.create_object(id, metadata.clone()).unwrap();

        let loaded = storage.load_object_metadata(id).unwrap();
        assert_eq!(loaded, Some(metadata));

        let other = ObjectId::new();
        assert_eq!(storage.load_object_metadata(other).unwrap(), None);
    }

    #[test]
    fn surrealkv_commit_roundtrip() {
        let (_temp_dir, mut storage) = test_storage();

        let id = ObjectId::new();
        let branch = BranchName::new("main");
        storage.create_object(id, HashMap::new()).unwrap();

        assert_eq!(storage.load_branch(id, &branch).unwrap(), None);

        let commit = make_commit(b"first");
        let commit_id = commit.id();
        storage.append_commit(id, &branch, commit).unwrap();

        let loaded = storage.load_branch(id, &branch).unwrap().unwrap();
        assert_eq!(loaded.commits.len(), 1);
        assert!(loaded.tails.contains(&commit_id));
        assert_eq!(loaded.commits[0].content, b"first");

        let mut commit2 = make_commit(b"second");
        commit2.parents = smallvec![commit_id];
        let commit2_id = commit2.id();
        storage.append_commit(id, &branch, commit2).unwrap();

        let loaded = storage.load_branch(id, &branch).unwrap().unwrap();
        assert_eq!(loaded.commits.len(), 2);
        assert!(!loaded.tails.contains(&commit_id));
        assert!(loaded.tails.contains(&commit2_id));

        storage.delete_commit(id, &branch, commit_id).unwrap();
        let loaded = storage.load_branch(id, &branch).unwrap().unwrap();
        assert_eq!(loaded.commits.len(), 1);
        assert_eq!(loaded.commits[0].content, b"second");
    }

    #[test]
    fn surrealkv_index_ops() {
        let (_temp_dir, mut storage) = test_storage();

        let row1 = ObjectId::new();
        let row2 = ObjectId::new();
        let row3 = ObjectId::new();
        let row4 = ObjectId::new();

        storage
            .index_insert("users", "age", "main", &Value::Integer(20), row1)
            .unwrap();
        storage
            .index_insert("users", "age", "main", &Value::Integer(25), row2)
            .unwrap();
        storage
            .index_insert("users", "age", "main", &Value::Integer(25), row3)
            .unwrap();
        storage
            .index_insert("users", "age", "main", &Value::Integer(30), row4)
            .unwrap();

        let results = storage.index_lookup("users", "age", "main", &Value::Integer(25));
        assert_eq!(results.len(), 2);
        assert!(results.contains(&row2));
        assert!(results.contains(&row3));

        let results = storage.index_range(
            "users",
            "age",
            "main",
            Bound::Included(&Value::Integer(25)),
            Bound::Excluded(&Value::Integer(30)),
        );
        assert_eq!(results.len(), 2);
        assert!(results.contains(&row2));
        assert!(results.contains(&row3));

        let results = storage.index_range(
            "users",
            "age",
            "main",
            Bound::Unbounded,
            Bound::Excluded(&Value::Integer(26)),
        );
        assert_eq!(results.len(), 3);
        assert!(results.contains(&row1));
        assert!(results.contains(&row2));
        assert!(results.contains(&row3));

        let results = storage.index_range(
            "users",
            "age",
            "main",
            Bound::Included(&Value::Integer(30)),
            Bound::Unbounded,
        );
        assert_eq!(results.len(), 1);
        assert!(results.contains(&row4));

        let results = storage.index_scan_all("users", "age", "main");
        assert_eq!(results.len(), 4);

        storage
            .index_remove("users", "age", "main", &Value::Integer(25), row2)
            .unwrap();
        let results = storage.index_lookup("users", "age", "main", &Value::Integer(25));
        assert_eq!(results.len(), 1);
        assert!(results.contains(&row3));
    }

    #[test]
    fn surrealkv_ordered_index_scan_respects_direction_and_bounds() {
        let (_temp_dir, mut storage) = test_storage();

        let row20 = ObjectId::new();
        let row25a = ObjectId::new();
        let row25b = ObjectId::new();
        let row30 = ObjectId::new();
        let row35 = ObjectId::new();

        storage
            .index_insert("users", "age", "main", &Value::Integer(20), row20)
            .unwrap();
        storage
            .index_insert("users", "age", "main", &Value::Integer(25), row25b)
            .unwrap();
        storage
            .index_insert("users", "age", "main", &Value::Integer(25), row25a)
            .unwrap();
        storage
            .index_insert("users", "age", "main", &Value::Integer(30), row30)
            .unwrap();
        storage
            .index_insert("users", "age", "main", &Value::Integer(35), row35)
            .unwrap();

        let asc = storage.index_scan_ordered(OrderedIndexScan {
            table: "users",
            column: "age",
            branch: "main",
            start: Bound::Included(&Value::Integer(25)),
            end: Bound::Excluded(&Value::Integer(35)),
            direction: IndexScanDirection::Ascending,
            take: Some(3),
            resume_after: None,
        });
        assert_eq!(
            asc.iter().map(|cursor| cursor.row_id).collect::<Vec<_>>(),
            vec![row25a, row25b, row30]
        );

        let desc = storage.index_scan_ordered(OrderedIndexScan {
            table: "users",
            column: "age",
            branch: "main",
            start: Bound::Unbounded,
            end: Bound::Included(&Value::Integer(25)),
            direction: IndexScanDirection::Descending,
            take: Some(3),
            resume_after: None,
        });
        assert_eq!(
            desc.iter().map(|cursor| cursor.row_id).collect::<Vec<_>>(),
            vec![row25a, row25b, row20]
        );
    }

    #[test]
    fn surrealkv_persistence() {
        let temp_dir = tempfile::TempDir::new().unwrap();
        let db_path = temp_dir.path().join("persist.surrealkv");

        let id = ObjectId::new();
        let mut metadata = HashMap::new();
        metadata.insert(
            crate::metadata::MetadataKey::Table.to_string(),
            "users".to_string(),
        );

        let commit_content = b"persistent data";
        let branch = BranchName::new("main");

        // Phase 1: Write data
        {
            let mut storage = SurrealKvStorage::open(&db_path, 8 * 1024 * 1024).unwrap();
            storage.create_object(id, metadata.clone()).unwrap();

            let commit = make_commit(commit_content);
            storage.append_commit(id, &branch, commit).unwrap();

            storage
                .index_insert(
                    "users",
                    "name",
                    "main",
                    &Value::Text("Alice".to_string()),
                    id,
                )
                .unwrap();
            storage.flush();
        }

        // Phase 2: Reopen and verify
        {
            let storage = SurrealKvStorage::open(&db_path, 8 * 1024 * 1024).unwrap();

            let loaded_meta = storage.load_object_metadata(id).unwrap();
            assert_eq!(loaded_meta, Some(metadata));

            let loaded_branch = storage.load_branch(id, &branch).unwrap().unwrap();
            assert_eq!(loaded_branch.commits.len(), 1);
            assert_eq!(loaded_branch.commits[0].content, commit_content);

            let results =
                storage.index_lookup("users", "name", "main", &Value::Text("Alice".to_string()));
            assert_eq!(results.len(), 1);
            assert!(results.contains(&id));
        }
    }

    #[test]
    fn surrealkv_catalogue_manifest_roundtrip() {
        let (_temp_dir, mut storage) = test_storage();
        let app_id = ObjectId::new();
        let schema_object_id = ObjectId::new();
        let lens_object_id = ObjectId::new();
        let schema_hash = crate::query_manager::types::SchemaHash::from_bytes([0x11; 32]);
        let source_hash = crate::query_manager::types::SchemaHash::from_bytes([0x22; 32]);
        let target_hash = crate::query_manager::types::SchemaHash::from_bytes([0x33; 32]);

        storage
            .append_catalogue_manifest_op(
                app_id,
                crate::storage::CatalogueManifestOp::SchemaSeen {
                    object_id: schema_object_id,
                    schema_hash,
                },
            )
            .unwrap();
        storage
            .append_catalogue_manifest_op(
                app_id,
                crate::storage::CatalogueManifestOp::LensSeen {
                    object_id: lens_object_id,
                    source_hash,
                    target_hash,
                },
            )
            .unwrap();
        storage
            .append_catalogue_manifest_op(
                app_id,
                crate::storage::CatalogueManifestOp::SchemaSeen {
                    object_id: schema_object_id,
                    schema_hash,
                },
            )
            .unwrap();

        let manifest = storage.load_catalogue_manifest(app_id).unwrap().unwrap();
        assert_eq!(
            manifest.schema_seen.get(&schema_object_id),
            Some(&schema_hash)
        );
        assert_eq!(
            manifest.lens_seen.get(&lens_object_id),
            Some(&crate::storage::CatalogueLensSeen {
                source_hash,
                target_hash,
            })
        );
    }
}
