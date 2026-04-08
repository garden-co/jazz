//! opfs-btree-backed Storage implementation.
//!
//! Uses a single opfs-btree instance with key-encoded namespaces for all data:
//! raw tables, row regions, and derived indices.
//!
//! Key encoding scheme (all keys are UTF-8 strings with hex-encoded binary parts):
//!
//! ```text
//! "raw:{table}:{local_key}"                               → raw table entry
//! "row:{table}:0:{branch}:{row_uuid}"                     → encoded VisibleRowEntry
//! "row:{table}:1:{row_uuid}:{version_id}"                 → encoded StoredRowVersion
//! ```

use std::cell::RefCell;
#[cfg(not(target_arch = "wasm32"))]
use std::path::Path;

#[cfg(target_arch = "wasm32")]
use opfs_btree::OpfsFile;
#[cfg(not(target_arch = "wasm32"))]
use opfs_btree::StdFile;
use opfs_btree::{BTreeError, BTreeOptions, MemoryFile, OpfsBTree, SyncFile};

use crate::object::ObjectId;
use crate::row_regions::{HistoryScan, RowState, StoredRowVersion, VisibleRowEntry};
use crate::sync_manager::DurabilityTier;

use super::{
    Storage, StorageError,
    key_codec::increment_bytes,
    storage_core::{
        append_history_region_rows_core, load_history_row_version_core,
        load_visible_region_entry_core, load_visible_region_row_core,
        patch_row_region_rows_by_batch_core, raw_table_delete_core, raw_table_get_core,
        raw_table_put_core, raw_table_scan_prefix_core, raw_table_scan_range_core,
        scan_history_region_core, scan_history_row_versions_core, scan_visible_region_core,
        scan_visible_region_row_versions_core, upsert_visible_region_rows_core,
    },
};
use crate::commit::CommitId;

const MIN_CACHE_SIZE_BYTES: usize = 4 * 1024 * 1024;

#[derive(Clone, Debug)]
enum AnyFile {
    Memory(MemoryFile),
    #[cfg(not(target_arch = "wasm32"))]
    Std(StdFile),
    #[cfg(target_arch = "wasm32")]
    Opfs(OpfsFile),
}

impl SyncFile for AnyFile {
    fn len(&self) -> Result<u64, BTreeError> {
        match self {
            Self::Memory(file) => file.len(),
            #[cfg(not(target_arch = "wasm32"))]
            Self::Std(file) => file.len(),
            #[cfg(target_arch = "wasm32")]
            Self::Opfs(file) => file.len(),
        }
    }

    fn read_exact_at(&self, offset: u64, buf: &mut [u8]) -> Result<(), BTreeError> {
        match self {
            Self::Memory(file) => file.read_exact_at(offset, buf),
            #[cfg(not(target_arch = "wasm32"))]
            Self::Std(file) => file.read_exact_at(offset, buf),
            #[cfg(target_arch = "wasm32")]
            Self::Opfs(file) => file.read_exact_at(offset, buf),
        }
    }

    fn write_all_at(&self, offset: u64, buf: &[u8]) -> Result<(), BTreeError> {
        match self {
            Self::Memory(file) => file.write_all_at(offset, buf),
            #[cfg(not(target_arch = "wasm32"))]
            Self::Std(file) => file.write_all_at(offset, buf),
            #[cfg(target_arch = "wasm32")]
            Self::Opfs(file) => file.write_all_at(offset, buf),
        }
    }

    fn truncate(&self, len: u64) -> Result<(), BTreeError> {
        match self {
            Self::Memory(file) => file.truncate(len),
            #[cfg(not(target_arch = "wasm32"))]
            Self::Std(file) => file.truncate(len),
            #[cfg(target_arch = "wasm32")]
            Self::Opfs(file) => file.truncate(len),
        }
    }

    fn flush(&self) -> Result<(), BTreeError> {
        match self {
            Self::Memory(file) => file.flush(),
            #[cfg(not(target_arch = "wasm32"))]
            Self::Std(file) => file.flush(),
            #[cfg(target_arch = "wasm32")]
            Self::Opfs(file) => file.flush(),
        }
    }
}

pub struct OpfsBTreeStorage {
    tree: RefCell<OpfsBTree<AnyFile>>,
}

impl OpfsBTreeStorage {
    #[cfg(not(target_arch = "wasm32"))]
    pub fn open(path: impl AsRef<Path>, cache_size_bytes: usize) -> Result<Self, StorageError> {
        let file = StdFile::open(path).map_err(map_storage_err)?;
        Self::open_with_file(AnyFile::Std(file), cache_size_bytes)
    }

    pub fn memory(cache_size_bytes: usize) -> Result<Self, StorageError> {
        Self::open_with_file(AnyFile::Memory(MemoryFile::new()), cache_size_bytes)
    }

    #[cfg(target_arch = "wasm32")]
    pub fn with_opfs(file: OpfsFile, cache_size_bytes: usize) -> Result<Self, StorageError> {
        Self::open_with_file(AnyFile::Opfs(file), cache_size_bytes)
    }

    #[cfg(target_arch = "wasm32")]
    pub async fn open_opfs(namespace: &str, cache_size_bytes: usize) -> Result<Self, StorageError> {
        let file = OpfsFile::open(namespace).await.map_err(map_storage_err)?;
        Self::with_opfs(file, cache_size_bytes)
    }

    #[cfg(target_arch = "wasm32")]
    pub async fn destroy_opfs(namespace: &str) -> Result<(), StorageError> {
        OpfsFile::destroy(namespace).await.map_err(map_storage_err)
    }

    fn open_with_file(file: AnyFile, cache_size_bytes: usize) -> Result<Self, StorageError> {
        let options = Self::options(cache_size_bytes);
        let tree = OpfsBTree::open(file, options).map_err(map_storage_err)?;
        let storage = Self {
            tree: RefCell::new(tree),
        };
        Ok(storage)
    }

    fn options(cache_size_bytes: usize) -> BTreeOptions {
        BTreeOptions {
            cache_bytes: cache_size_bytes.max(MIN_CACHE_SIZE_BYTES),
            pin_internal_pages: true,
            read_coalesce_pages: 4,
            ..Default::default()
        }
    }

    fn with_tree_mut<R>(
        &self,
        f: impl FnOnce(&mut OpfsBTree<AnyFile>) -> Result<R, StorageError>,
    ) -> Result<R, StorageError> {
        let mut tree = self
            .tree
            .try_borrow_mut()
            .map_err(|_| StorageError::IoError("opfs-btree already borrowed".to_string()))?;
        f(&mut tree)
    }

    fn tree_insert(&self, key: &str, value: &[u8]) -> Result<(), StorageError> {
        self.with_tree_mut(|tree| tree.put(key.as_bytes(), value).map_err(map_storage_err))
    }

    fn tree_read(&self, key: &str) -> Result<Option<Vec<u8>>, StorageError> {
        self.with_tree_mut(|tree| tree.get(key.as_bytes()).map_err(map_storage_err))
    }

    fn tree_delete(&self, key: &str) -> Result<(), StorageError> {
        self.with_tree_mut(|tree| tree.delete(key.as_bytes()).map_err(map_storage_err))
    }

    fn tree_scan_prefix(&self, prefix: &str) -> Result<Vec<(String, Vec<u8>)>, StorageError> {
        let start = prefix.as_bytes();
        let mut end = start.to_vec();
        increment_bytes(&mut end);
        self.tree_scan_range_bytes(start, &end)
    }

    fn tree_scan_range(
        &self,
        start: &str,
        end: &str,
    ) -> Result<Vec<(String, Vec<u8>)>, StorageError> {
        self.tree_scan_range_bytes(start.as_bytes(), end.as_bytes())
    }

    fn tree_scan_range_bytes(
        &self,
        start: &[u8],
        end: &[u8],
    ) -> Result<Vec<(String, Vec<u8>)>, StorageError> {
        if start >= end {
            return Ok(Vec::new());
        }

        self.with_tree_mut(|tree| {
            let entries = tree
                .range(start, end, usize::MAX)
                .map_err(map_storage_err)?;

            entries
                .into_iter()
                .map(|(key, value)| {
                    let key = String::from_utf8(key)
                        .map_err(|e| StorageError::IoError(format!("invalid key utf8: {}", e)))?;
                    Ok((key, value))
                })
                .collect()
        })
    }
}

impl Storage for OpfsBTreeStorage {
    fn raw_table_put(&mut self, table: &str, key: &str, value: &[u8]) -> Result<(), StorageError> {
        raw_table_put_core(table, key, value, |storage_key, bytes| {
            self.tree_insert(storage_key, bytes)
        })
    }

    fn raw_table_delete(&mut self, table: &str, key: &str) -> Result<(), StorageError> {
        raw_table_delete_core(table, key, |storage_key| self.tree_delete(storage_key))
    }

    fn raw_table_get(&self, table: &str, key: &str) -> Result<Option<Vec<u8>>, StorageError> {
        raw_table_get_core(table, key, |storage_key| self.tree_read(storage_key))
    }

    fn raw_table_scan_prefix(
        &self,
        table: &str,
        prefix: &str,
    ) -> Result<super::RawTableRows, StorageError> {
        raw_table_scan_prefix_core(table, prefix, |storage_prefix| {
            self.tree_scan_prefix(storage_prefix)
        })
    }

    fn raw_table_scan_range(
        &self,
        table: &str,
        start: Option<&str>,
        end: Option<&str>,
    ) -> Result<super::RawTableRows, StorageError> {
        raw_table_scan_range_core(table, start, end, |start_key, end_key| {
            self.tree_scan_range(start_key, end_key)
        })
    }

    fn append_history_region_rows(
        &mut self,
        table: &str,
        rows: &[StoredRowVersion],
    ) -> Result<(), StorageError> {
        append_history_region_rows_core(table, rows, |key, bytes| self.tree_insert(key, bytes))
    }

    fn upsert_visible_region_rows(
        &mut self,
        table: &str,
        entries: &[VisibleRowEntry],
    ) -> Result<(), StorageError> {
        upsert_visible_region_rows_core(table, entries, |key, bytes| self.tree_insert(key, bytes))
    }

    fn patch_row_region_rows_by_batch(
        &mut self,
        table: &str,
        batch_id: crate::row_regions::BatchId,
        state: Option<RowState>,
        confirmed_tier: Option<DurabilityTier>,
    ) -> Result<(), StorageError> {
        patch_row_region_rows_by_batch_core(
            table,
            batch_id,
            state,
            confirmed_tier,
            |prefix| self.tree_scan_prefix(prefix),
            |key, bytes| self.tree_insert(key, bytes),
        )
    }

    fn scan_visible_region(
        &self,
        table: &str,
        branch: &str,
    ) -> Result<Vec<StoredRowVersion>, StorageError> {
        scan_visible_region_core(table, branch, |prefix| self.tree_scan_prefix(prefix))
    }

    fn load_visible_region_row(
        &self,
        table: &str,
        branch: &str,
        row_id: ObjectId,
    ) -> Result<Option<StoredRowVersion>, StorageError> {
        load_visible_region_row_core(table, branch, row_id, |key| self.tree_read(key))
    }

    fn load_visible_region_entry(
        &self,
        table: &str,
        branch: &str,
        row_id: ObjectId,
    ) -> Result<Option<VisibleRowEntry>, StorageError> {
        load_visible_region_entry_core(table, branch, row_id, |key| self.tree_read(key))
    }

    fn scan_visible_region_row_versions(
        &self,
        table: &str,
        row_id: ObjectId,
    ) -> Result<Vec<StoredRowVersion>, StorageError> {
        scan_visible_region_row_versions_core(table, row_id, |prefix| self.tree_scan_prefix(prefix))
    }

    fn scan_history_row_versions(
        &self,
        table: &str,
        row_id: ObjectId,
    ) -> Result<Vec<StoredRowVersion>, StorageError> {
        scan_history_row_versions_core(table, row_id, |prefix| self.tree_scan_prefix(prefix))
    }

    fn load_history_row_version(
        &self,
        table: &str,
        row_id: ObjectId,
        version_id: CommitId,
    ) -> Result<Option<StoredRowVersion>, StorageError> {
        load_history_row_version_core(table, row_id, version_id, |key| self.tree_read(key))
    }

    fn scan_history_region(
        &self,
        table: &str,
        branch: &str,
        scan: HistoryScan,
    ) -> Result<Vec<StoredRowVersion>, StorageError> {
        scan_history_region_core(table, branch, scan, |prefix| self.tree_scan_prefix(prefix))
    }

    fn flush(&self) {
        let _span = tracing::debug_span!("OpfsBTreeStorage::flush").entered();
        if let Err(error) = self.with_tree_mut(|tree| tree.checkpoint().map_err(map_storage_err)) {
            tracing::warn!(?error, "OpfsBTreeStorage flush failed");
        }
    }

    fn flush_wal(&self) {
        let _span = tracing::debug_span!("OpfsBTreeStorage::flush_wal").entered();
        // opfs-btree has no separate WAL; flush_wal maps to an incremental checkpoint.
        self.flush();
    }
}

fn map_storage_err(error: BTreeError) -> StorageError {
    StorageError::IoError(format!("opfs-btree: {}", error))
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::ops::Bound;

    use super::*;
    use crate::catalogue::CatalogueEntry;
    use crate::metadata::RowProvenance;
    use crate::query_manager::types::Value;
    use crate::row_regions::{HistoryScan, RowState, StoredRowVersion, VisibleRowEntry};
    use crate::sync_manager::DurabilityTier;

    fn make_row_version(
        row_id: ObjectId,
        branch: &str,
        updated_at: u64,
        value: &[u8],
    ) -> StoredRowVersion {
        StoredRowVersion::new(
            row_id,
            branch,
            Vec::new(),
            value.to_vec(),
            RowProvenance::for_insert(row_id.to_string(), updated_at),
            HashMap::new(),
            RowState::VisibleDirect,
            None,
        )
    }

    fn test_storage() -> OpfsBTreeStorage {
        OpfsBTreeStorage::memory(4 * 1024 * 1024).unwrap()
    }

    #[test]
    fn opfs_btree_object_roundtrip() {
        let mut storage = test_storage();

        let id = ObjectId::new();
        let mut metadata = HashMap::new();
        metadata.insert(
            crate::metadata::MetadataKey::Table.to_string(),
            "users".to_string(),
        );
        metadata.insert("app".to_string(), "test".to_string());

        storage.put_metadata(id, metadata.clone()).unwrap();

        let loaded = storage.load_metadata(id).unwrap();
        assert_eq!(loaded, Some(metadata));

        let other = ObjectId::new();
        assert_eq!(storage.load_metadata(other).unwrap(), None);
    }

    #[test]
    fn opfs_btree_row_region_roundtrip() {
        let mut storage = test_storage();

        let row_id = ObjectId::new();
        let row = make_row_version(row_id, "main", 12345, b"first");

        storage
            .append_history_region_rows("users", std::slice::from_ref(&row))
            .unwrap();
        storage
            .upsert_visible_region_rows(
                "users",
                std::slice::from_ref(&VisibleRowEntry::rebuild(
                    row.clone(),
                    std::slice::from_ref(&row),
                )),
            )
            .unwrap();

        assert_eq!(
            storage
                .load_visible_region_row("users", "main", row_id)
                .unwrap(),
            Some(row.clone())
        );
        assert_eq!(
            storage
                .scan_history_region("users", "main", HistoryScan::Row { row_id })
                .unwrap(),
            vec![row]
        );
    }

    #[test]
    fn opfs_btree_index_ops() {
        let mut storage = test_storage();

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

        let results = storage.index_lookup("users", "age", "main", &Value::Integer(99));
        assert!(results.is_empty());

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
    fn opfs_btree_index_branch_isolation() {
        let mut storage = test_storage();

        let row1 = ObjectId::new();
        let row2 = ObjectId::new();

        storage
            .index_insert("users", "age", "main", &Value::Integer(25), row1)
            .unwrap();
        storage
            .index_insert("users", "age", "feature", &Value::Integer(25), row2)
            .unwrap();

        let main_results = storage.index_lookup("users", "age", "main", &Value::Integer(25));
        assert_eq!(main_results.len(), 1);
        assert!(main_results.contains(&row1));

        let feature_results = storage.index_lookup("users", "age", "feature", &Value::Integer(25));
        assert_eq!(feature_results.len(), 1);
        assert!(feature_results.contains(&row2));
    }

    #[test]
    fn opfs_btree_row_region_patch_roundtrip() {
        let mut storage = test_storage();
        let row_id = ObjectId::new();
        let row = make_row_version(row_id, "main", 12345, b"first");

        storage
            .append_history_region_rows("users", std::slice::from_ref(&row))
            .unwrap();
        storage
            .upsert_visible_region_rows(
                "users",
                std::slice::from_ref(&VisibleRowEntry::rebuild(
                    row.clone(),
                    std::slice::from_ref(&row),
                )),
            )
            .unwrap();
        storage
            .patch_row_region_rows_by_batch(
                "users",
                row.batch_id,
                None,
                Some(DurabilityTier::EdgeServer),
            )
            .unwrap();

        assert_eq!(
            storage
                .load_visible_region_row("users", "main", row_id)
                .unwrap()
                .and_then(|row| row.confirmed_tier),
            Some(DurabilityTier::EdgeServer)
        );
    }

    #[test]
    fn opfs_btree_persistence() {
        let temp_dir = tempfile::TempDir::new().unwrap();
        let db_path = temp_dir.path().join("test.opfsbtree");

        let id = ObjectId::new();
        let row = make_row_version(id, "main", 12345, b"persistent data");
        let mut metadata = HashMap::new();
        metadata.insert(
            crate::metadata::MetadataKey::Table.to_string(),
            "users".to_string(),
        );

        {
            let mut storage = OpfsBTreeStorage::open(&db_path, 4 * 1024 * 1024).unwrap();
            storage.put_metadata(id, metadata.clone()).unwrap();
            storage
                .append_history_region_rows("users", std::slice::from_ref(&row))
                .unwrap();
            storage
                .upsert_visible_region_rows(
                    "users",
                    std::slice::from_ref(&VisibleRowEntry::rebuild(
                        row.clone(),
                        std::slice::from_ref(&row),
                    )),
                )
                .unwrap();

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

        {
            let storage = OpfsBTreeStorage::open(&db_path, 4 * 1024 * 1024).unwrap();

            let loaded_meta = storage.load_metadata(id).unwrap();
            assert_eq!(loaded_meta, Some(metadata));
            assert_eq!(
                storage
                    .load_visible_region_row("users", "main", id)
                    .unwrap(),
                Some(row)
            );

            let results =
                storage.index_lookup("users", "name", "main", &Value::Text("Alice".to_string()));
            assert_eq!(results.len(), 1);
            assert!(results.contains(&id));
        }
    }

    #[test]
    fn opfs_btree_catalogue_entry_roundtrip() {
        let mut storage = test_storage();
        let object_id = ObjectId::new();
        let metadata = HashMap::from([
            (
                crate::metadata::MetadataKey::Type.to_string(),
                crate::metadata::ObjectType::CatalogueSchema.to_string(),
            ),
            ("app_id".to_string(), ObjectId::new().to_string()),
        ]);
        let entry = CatalogueEntry {
            object_id,
            metadata: metadata.clone(),
            content: b"schema bytes".to_vec(),
        };

        storage.upsert_catalogue_entry(&entry).unwrap();

        let loaded = storage.load_catalogue_entry(object_id).unwrap();
        assert_eq!(loaded, Some(entry.clone()));

        let scanned = storage.scan_catalogue_entries().unwrap();
        assert_eq!(scanned, vec![entry]);
    }
}
