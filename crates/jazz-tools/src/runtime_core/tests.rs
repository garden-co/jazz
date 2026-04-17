use super::*;
use crate::commit::CommitId;
use crate::query_manager::policy::PolicyExpr;
use crate::query_manager::query::QueryBuilder;
use crate::query_manager::session::WriteContext;
use crate::query_manager::types::{
    ColumnType, SchemaBuilder, SchemaHash, TableName, TablePolicies, TableSchema,
};
use crate::schema_manager::AppId;
use crate::storage::{
    MemoryStorage, MetadataRows, RawTableKeys, RawTableRows, Storage, StorageError,
};
use crate::sync_manager::{
    ClientId, ClientRole, Destination, DurabilityTier, InboxEntry, OutboxEntry, ServerId, Source,
    SyncManager, SyncPayload,
};
use crate::test_row_history::load_test_row_tip_ids;
use std::collections::HashMap;
use std::sync::{Arc, Mutex};

type TestCore = RuntimeCore<MemoryStorage, NoopScheduler>;
type BoxedStorageTestCore = RuntimeCore<Box<dyn Storage>, NoopScheduler>;

struct RowRegionReadFailingStorage {
    inner: MemoryStorage,
}

#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
struct LegacyStorageCallCounts;

struct LegacyPersistenceObservingStorage {
    inner: MemoryStorage,
    _calls: Arc<Mutex<LegacyStorageCallCounts>>,
}

#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
struct RowMutationCallCounts {
    row_mutation_calls: usize,
    separate_index_mutation_calls: usize,
    flush_wal_calls: usize,
}

struct RowMutationObservingStorage {
    inner: MemoryStorage,
    calls: Arc<Mutex<RowMutationCallCounts>>,
}

impl RowRegionReadFailingStorage {
    fn new() -> Self {
        Self {
            inner: MemoryStorage::new(),
        }
    }
}

impl LegacyPersistenceObservingStorage {
    fn new(calls: Arc<Mutex<LegacyStorageCallCounts>>) -> Self {
        Self {
            inner: MemoryStorage::new(),
            _calls: calls,
        }
    }
}

impl RowMutationObservingStorage {
    fn new(calls: Arc<Mutex<RowMutationCallCounts>>) -> Self {
        Self {
            inner: MemoryStorage::new(),
            calls,
        }
    }
}

impl Storage for RowRegionReadFailingStorage {
    fn put_metadata(
        &mut self,
        id: ObjectId,
        metadata: HashMap<String, String>,
    ) -> Result<(), StorageError> {
        self.inner.put_metadata(id, metadata)
    }

    fn load_metadata(&self, id: ObjectId) -> Result<Option<HashMap<String, String>>, StorageError> {
        self.inner.load_metadata(id)
    }

    fn scan_metadata(&self) -> Result<MetadataRows, StorageError> {
        self.inner.scan_metadata()
    }

    fn scan_row_locators(&self) -> Result<crate::storage::RowLocatorRows, StorageError> {
        self.inner.scan_row_locators()
    }

    fn load_row_locator(
        &self,
        id: ObjectId,
    ) -> Result<Option<crate::storage::RowLocator>, StorageError> {
        self.inner.load_row_locator(id)
    }

    fn put_row_locator(
        &mut self,
        id: ObjectId,
        locator: Option<&crate::storage::RowLocator>,
    ) -> Result<(), StorageError> {
        self.inner.put_row_locator(id, locator)
    }

    fn raw_table_put(&mut self, table: &str, key: &str, value: &[u8]) -> Result<(), StorageError> {
        self.inner.raw_table_put(table, key, value)
    }

    fn raw_table_delete(&mut self, table: &str, key: &str) -> Result<(), StorageError> {
        self.inner.raw_table_delete(table, key)
    }

    fn raw_table_get(&self, table: &str, key: &str) -> Result<Option<Vec<u8>>, StorageError> {
        self.inner.raw_table_get(table, key)
    }

    fn raw_table_scan_prefix(
        &self,
        table: &str,
        prefix: &str,
    ) -> Result<RawTableRows, StorageError> {
        self.inner.raw_table_scan_prefix(table, prefix)
    }

    fn raw_table_scan_prefix_keys(
        &self,
        table: &str,
        prefix: &str,
    ) -> Result<RawTableKeys, StorageError> {
        self.inner.raw_table_scan_prefix_keys(table, prefix)
    }

    fn raw_table_scan_range(
        &self,
        table: &str,
        start: Option<&str>,
        end: Option<&str>,
    ) -> Result<RawTableRows, StorageError> {
        self.inner.raw_table_scan_range(table, start, end)
    }

    fn raw_table_scan_range_keys(
        &self,
        table: &str,
        start: Option<&str>,
        end: Option<&str>,
    ) -> Result<RawTableKeys, StorageError> {
        self.inner.raw_table_scan_range_keys(table, start, end)
    }

    fn append_history_region_rows(
        &mut self,
        table: &str,
        rows: &[crate::row_histories::StoredRowVersion],
    ) -> Result<(), StorageError> {
        self.inner.append_history_region_rows(table, rows)
    }

    fn append_history_region_row_bytes(
        &mut self,
        table: &str,
        rows: &[crate::storage::HistoryRowBytes<'_>],
    ) -> Result<(), StorageError> {
        self.inner.append_history_region_row_bytes(table, rows)
    }

    fn upsert_visible_region_rows(
        &mut self,
        table: &str,
        entries: &[crate::row_histories::VisibleRowEntry],
    ) -> Result<(), StorageError> {
        self.inner.upsert_visible_region_rows(table, entries)
    }

    fn patch_row_region_rows_by_batch(
        &mut self,
        table: &str,
        batch_id: crate::row_histories::BatchId,
        state: Option<crate::row_histories::RowState>,
        confirmed_tier: Option<DurabilityTier>,
    ) -> Result<(), StorageError> {
        self.inner
            .patch_row_region_rows_by_batch(table, batch_id, state, confirmed_tier)
    }

    fn scan_visible_region(
        &self,
        table: &str,
        branch: &str,
    ) -> Result<Vec<crate::row_histories::StoredRowVersion>, StorageError> {
        self.inner.scan_visible_region(table, branch)
    }

    fn load_visible_region_row(
        &self,
        _table: &str,
        _branch: &str,
        _row_id: ObjectId,
    ) -> Result<Option<crate::row_histories::StoredRowVersion>, StorageError> {
        Err(StorageError::IoError(
            "row-history reads deliberately disabled in this test".to_string(),
        ))
    }

    fn scan_visible_region_row_versions(
        &self,
        table: &str,
        row_id: ObjectId,
    ) -> Result<Vec<crate::row_histories::StoredRowVersion>, StorageError> {
        self.inner.scan_visible_region_row_versions(table, row_id)
    }

    fn scan_history_row_versions(
        &self,
        table: &str,
        row_id: ObjectId,
    ) -> Result<Vec<crate::row_histories::StoredRowVersion>, StorageError> {
        self.inner.scan_history_row_versions(table, row_id)
    }

    fn load_history_row_version_bytes(
        &self,
        table: &str,
        row_id: ObjectId,
        version_id: CommitId,
    ) -> Result<Option<Vec<u8>>, StorageError> {
        self.inner
            .load_history_row_version_bytes(table, row_id, version_id)
    }

    fn scan_history_region_bytes(
        &self,
        table: &str,
        scan: crate::row_histories::HistoryScan,
    ) -> Result<Vec<Vec<u8>>, StorageError> {
        self.inner.scan_history_region_bytes(table, scan)
    }

    fn scan_history_region(
        &self,
        table: &str,
        branch: &str,
        scan: crate::row_histories::HistoryScan,
    ) -> Result<Vec<crate::row_histories::StoredRowVersion>, StorageError> {
        self.inner.scan_history_region(table, branch, scan)
    }

    fn index_insert(
        &mut self,
        table: &str,
        column: &str,
        branch: &str,
        value: &Value,
        row_id: ObjectId,
    ) -> Result<(), StorageError> {
        self.inner
            .index_insert(table, column, branch, value, row_id)
    }

    fn index_remove(
        &mut self,
        table: &str,
        column: &str,
        branch: &str,
        value: &Value,
        row_id: ObjectId,
    ) -> Result<(), StorageError> {
        self.inner
            .index_remove(table, column, branch, value, row_id)
    }

    fn index_lookup(
        &self,
        table: &str,
        column: &str,
        branch: &str,
        value: &Value,
    ) -> Vec<ObjectId> {
        self.inner.index_lookup(table, column, branch, value)
    }

    fn index_range(
        &self,
        table: &str,
        column: &str,
        branch: &str,
        start: std::ops::Bound<&Value>,
        end: std::ops::Bound<&Value>,
    ) -> Vec<ObjectId> {
        self.inner.index_range(table, column, branch, start, end)
    }

    fn index_scan_all(&self, table: &str, column: &str, branch: &str) -> Vec<ObjectId> {
        self.inner.index_scan_all(table, column, branch)
    }

    fn flush(&self) {
        self.inner.flush();
    }

    fn flush_wal(&self) {
        self.inner.flush_wal();
    }

    fn close(&self) -> Result<(), StorageError> {
        self.inner.close()
    }
}

impl Storage for LegacyPersistenceObservingStorage {
    fn put_metadata(
        &mut self,
        id: ObjectId,
        metadata: HashMap<String, String>,
    ) -> Result<(), StorageError> {
        self.inner.put_metadata(id, metadata)
    }

    fn load_metadata(&self, id: ObjectId) -> Result<Option<HashMap<String, String>>, StorageError> {
        self.inner.load_metadata(id)
    }

    fn scan_metadata(&self) -> Result<MetadataRows, StorageError> {
        self.inner.scan_metadata()
    }

    fn scan_row_locators(&self) -> Result<crate::storage::RowLocatorRows, StorageError> {
        self.inner.scan_row_locators()
    }

    fn load_row_locator(
        &self,
        id: ObjectId,
    ) -> Result<Option<crate::storage::RowLocator>, StorageError> {
        self.inner.load_row_locator(id)
    }

    fn put_row_locator(
        &mut self,
        id: ObjectId,
        locator: Option<&crate::storage::RowLocator>,
    ) -> Result<(), StorageError> {
        self.inner.put_row_locator(id, locator)
    }

    fn raw_table_put(&mut self, table: &str, key: &str, value: &[u8]) -> Result<(), StorageError> {
        self.inner.raw_table_put(table, key, value)
    }

    fn raw_table_delete(&mut self, table: &str, key: &str) -> Result<(), StorageError> {
        self.inner.raw_table_delete(table, key)
    }

    fn raw_table_get(&self, table: &str, key: &str) -> Result<Option<Vec<u8>>, StorageError> {
        self.inner.raw_table_get(table, key)
    }

    fn raw_table_scan_prefix(
        &self,
        table: &str,
        prefix: &str,
    ) -> Result<RawTableRows, StorageError> {
        self.inner.raw_table_scan_prefix(table, prefix)
    }

    fn raw_table_scan_prefix_keys(
        &self,
        table: &str,
        prefix: &str,
    ) -> Result<RawTableKeys, StorageError> {
        self.inner.raw_table_scan_prefix_keys(table, prefix)
    }

    fn raw_table_scan_range(
        &self,
        table: &str,
        start: Option<&str>,
        end: Option<&str>,
    ) -> Result<RawTableRows, StorageError> {
        self.inner.raw_table_scan_range(table, start, end)
    }

    fn raw_table_scan_range_keys(
        &self,
        table: &str,
        start: Option<&str>,
        end: Option<&str>,
    ) -> Result<RawTableKeys, StorageError> {
        self.inner.raw_table_scan_range_keys(table, start, end)
    }

    fn append_history_region_rows(
        &mut self,
        table: &str,
        rows: &[crate::row_histories::StoredRowVersion],
    ) -> Result<(), StorageError> {
        self.inner.append_history_region_rows(table, rows)
    }

    fn append_history_region_row_bytes(
        &mut self,
        table: &str,
        rows: &[crate::storage::HistoryRowBytes<'_>],
    ) -> Result<(), StorageError> {
        self.inner.append_history_region_row_bytes(table, rows)
    }

    fn upsert_visible_region_rows(
        &mut self,
        table: &str,
        entries: &[crate::row_histories::VisibleRowEntry],
    ) -> Result<(), StorageError> {
        self.inner.upsert_visible_region_rows(table, entries)
    }

    fn patch_row_region_rows_by_batch(
        &mut self,
        table: &str,
        batch_id: crate::row_histories::BatchId,
        state: Option<crate::row_histories::RowState>,
        confirmed_tier: Option<DurabilityTier>,
    ) -> Result<(), StorageError> {
        self.inner
            .patch_row_region_rows_by_batch(table, batch_id, state, confirmed_tier)
    }

    fn scan_visible_region(
        &self,
        table: &str,
        branch: &str,
    ) -> Result<Vec<crate::row_histories::StoredRowVersion>, StorageError> {
        self.inner.scan_visible_region(table, branch)
    }

    fn load_visible_region_row(
        &self,
        table: &str,
        branch: &str,
        row_id: ObjectId,
    ) -> Result<Option<crate::row_histories::StoredRowVersion>, StorageError> {
        self.inner.load_visible_region_row(table, branch, row_id)
    }

    fn scan_visible_region_row_versions(
        &self,
        table: &str,
        row_id: ObjectId,
    ) -> Result<Vec<crate::row_histories::StoredRowVersion>, StorageError> {
        self.inner.scan_visible_region_row_versions(table, row_id)
    }

    fn scan_history_row_versions(
        &self,
        table: &str,
        row_id: ObjectId,
    ) -> Result<Vec<crate::row_histories::StoredRowVersion>, StorageError> {
        self.inner.scan_history_row_versions(table, row_id)
    }

    fn load_history_row_version_bytes(
        &self,
        table: &str,
        row_id: ObjectId,
        version_id: CommitId,
    ) -> Result<Option<Vec<u8>>, StorageError> {
        self.inner
            .load_history_row_version_bytes(table, row_id, version_id)
    }

    fn scan_history_region_bytes(
        &self,
        table: &str,
        scan: crate::row_histories::HistoryScan,
    ) -> Result<Vec<Vec<u8>>, StorageError> {
        self.inner.scan_history_region_bytes(table, scan)
    }

    fn scan_history_region(
        &self,
        table: &str,
        branch: &str,
        scan: crate::row_histories::HistoryScan,
    ) -> Result<Vec<crate::row_histories::StoredRowVersion>, StorageError> {
        self.inner.scan_history_region(table, branch, scan)
    }

    fn index_insert(
        &mut self,
        table: &str,
        column: &str,
        branch: &str,
        value: &Value,
        row_id: ObjectId,
    ) -> Result<(), StorageError> {
        self.inner
            .index_insert(table, column, branch, value, row_id)
    }

    fn index_remove(
        &mut self,
        table: &str,
        column: &str,
        branch: &str,
        value: &Value,
        row_id: ObjectId,
    ) -> Result<(), StorageError> {
        self.inner
            .index_remove(table, column, branch, value, row_id)
    }

    fn index_lookup(
        &self,
        table: &str,
        column: &str,
        branch: &str,
        value: &Value,
    ) -> Vec<ObjectId> {
        self.inner.index_lookup(table, column, branch, value)
    }

    fn index_range(
        &self,
        table: &str,
        column: &str,
        branch: &str,
        start: std::ops::Bound<&Value>,
        end: std::ops::Bound<&Value>,
    ) -> Vec<ObjectId> {
        self.inner.index_range(table, column, branch, start, end)
    }

    fn index_scan_all(&self, table: &str, column: &str, branch: &str) -> Vec<ObjectId> {
        self.inner.index_scan_all(table, column, branch)
    }

    fn flush(&self) {
        self.inner.flush();
    }

    fn flush_wal(&self) {
        self.inner.flush_wal();
    }

    fn close(&self) -> Result<(), StorageError> {
        self.inner.close()
    }
}

impl Storage for RowMutationObservingStorage {
    fn put_metadata(
        &mut self,
        id: ObjectId,
        metadata: HashMap<String, String>,
    ) -> Result<(), StorageError> {
        self.inner.put_metadata(id, metadata)
    }

    fn load_metadata(&self, id: ObjectId) -> Result<Option<HashMap<String, String>>, StorageError> {
        self.inner.load_metadata(id)
    }

    fn scan_metadata(&self) -> Result<MetadataRows, StorageError> {
        self.inner.scan_metadata()
    }

    fn scan_row_locators(&self) -> Result<crate::storage::RowLocatorRows, StorageError> {
        self.inner.scan_row_locators()
    }

    fn load_row_locator(
        &self,
        id: ObjectId,
    ) -> Result<Option<crate::storage::RowLocator>, StorageError> {
        self.inner.load_row_locator(id)
    }

    fn put_row_locator(
        &mut self,
        id: ObjectId,
        locator: Option<&crate::storage::RowLocator>,
    ) -> Result<(), StorageError> {
        self.inner.put_row_locator(id, locator)
    }

    fn raw_table_put(&mut self, table: &str, key: &str, value: &[u8]) -> Result<(), StorageError> {
        self.inner.raw_table_put(table, key, value)
    }

    fn raw_table_delete(&mut self, table: &str, key: &str) -> Result<(), StorageError> {
        self.inner.raw_table_delete(table, key)
    }

    fn raw_table_get(&self, table: &str, key: &str) -> Result<Option<Vec<u8>>, StorageError> {
        self.inner.raw_table_get(table, key)
    }

    fn raw_table_scan_prefix(
        &self,
        table: &str,
        prefix: &str,
    ) -> Result<RawTableRows, StorageError> {
        self.inner.raw_table_scan_prefix(table, prefix)
    }

    fn raw_table_scan_prefix_keys(
        &self,
        table: &str,
        prefix: &str,
    ) -> Result<RawTableKeys, StorageError> {
        self.inner.raw_table_scan_prefix_keys(table, prefix)
    }

    fn raw_table_scan_range(
        &self,
        table: &str,
        start: Option<&str>,
        end: Option<&str>,
    ) -> Result<RawTableRows, StorageError> {
        self.inner.raw_table_scan_range(table, start, end)
    }

    fn raw_table_scan_range_keys(
        &self,
        table: &str,
        start: Option<&str>,
        end: Option<&str>,
    ) -> Result<RawTableKeys, StorageError> {
        self.inner.raw_table_scan_range_keys(table, start, end)
    }

    fn append_history_region_rows(
        &mut self,
        table: &str,
        rows: &[crate::row_histories::StoredRowVersion],
    ) -> Result<(), StorageError> {
        self.inner.append_history_region_rows(table, rows)
    }

    fn append_history_region_row_bytes(
        &mut self,
        table: &str,
        rows: &[crate::storage::HistoryRowBytes<'_>],
    ) -> Result<(), StorageError> {
        self.inner.append_history_region_row_bytes(table, rows)
    }

    fn upsert_visible_region_rows(
        &mut self,
        table: &str,
        entries: &[crate::row_histories::VisibleRowEntry],
    ) -> Result<(), StorageError> {
        self.inner.upsert_visible_region_rows(table, entries)
    }

    fn apply_row_mutation(
        &mut self,
        table: &str,
        history_rows: &[crate::row_histories::StoredRowVersion],
        visible_entries: &[crate::row_histories::VisibleRowEntry],
        index_mutations: &[crate::storage::IndexMutation<'_>],
    ) -> Result<(), StorageError> {
        self.calls.lock().unwrap().row_mutation_calls += 1;
        self.inner
            .apply_row_mutation(table, history_rows, visible_entries, index_mutations)
    }

    fn patch_row_region_rows_by_batch(
        &mut self,
        table: &str,
        batch_id: crate::row_histories::BatchId,
        state: Option<crate::row_histories::RowState>,
        confirmed_tier: Option<DurabilityTier>,
    ) -> Result<(), StorageError> {
        self.inner
            .patch_row_region_rows_by_batch(table, batch_id, state, confirmed_tier)
    }

    fn scan_visible_region(
        &self,
        table: &str,
        branch: &str,
    ) -> Result<Vec<crate::row_histories::StoredRowVersion>, StorageError> {
        self.inner.scan_visible_region(table, branch)
    }

    fn load_visible_region_row(
        &self,
        table: &str,
        branch: &str,
        row_id: ObjectId,
    ) -> Result<Option<crate::row_histories::StoredRowVersion>, StorageError> {
        self.inner.load_visible_region_row(table, branch, row_id)
    }

    fn scan_visible_region_row_versions(
        &self,
        table: &str,
        row_id: ObjectId,
    ) -> Result<Vec<crate::row_histories::StoredRowVersion>, StorageError> {
        self.inner.scan_visible_region_row_versions(table, row_id)
    }

    fn scan_history_row_versions(
        &self,
        table: &str,
        row_id: ObjectId,
    ) -> Result<Vec<crate::row_histories::StoredRowVersion>, StorageError> {
        self.inner.scan_history_row_versions(table, row_id)
    }

    fn load_history_row_version_bytes(
        &self,
        table: &str,
        row_id: ObjectId,
        version_id: CommitId,
    ) -> Result<Option<Vec<u8>>, StorageError> {
        self.inner
            .load_history_row_version_bytes(table, row_id, version_id)
    }

    fn scan_history_region_bytes(
        &self,
        table: &str,
        scan: crate::row_histories::HistoryScan,
    ) -> Result<Vec<Vec<u8>>, StorageError> {
        self.inner.scan_history_region_bytes(table, scan)
    }

    fn scan_history_region(
        &self,
        table: &str,
        branch: &str,
        scan: crate::row_histories::HistoryScan,
    ) -> Result<Vec<crate::row_histories::StoredRowVersion>, StorageError> {
        self.inner.scan_history_region(table, branch, scan)
    }

    fn index_insert(
        &mut self,
        table: &str,
        column: &str,
        branch: &str,
        value: &Value,
        row_id: ObjectId,
    ) -> Result<(), StorageError> {
        self.inner
            .index_insert(table, column, branch, value, row_id)
    }

    fn index_remove(
        &mut self,
        table: &str,
        column: &str,
        branch: &str,
        value: &Value,
        row_id: ObjectId,
    ) -> Result<(), StorageError> {
        self.inner
            .index_remove(table, column, branch, value, row_id)
    }

    fn apply_index_mutations(
        &mut self,
        mutations: &[crate::storage::IndexMutation<'_>],
    ) -> Result<(), StorageError> {
        self.calls.lock().unwrap().separate_index_mutation_calls += 1;
        self.inner.apply_index_mutations(mutations)
    }

    fn index_lookup(
        &self,
        table: &str,
        column: &str,
        branch: &str,
        value: &Value,
    ) -> Vec<ObjectId> {
        self.inner.index_lookup(table, column, branch, value)
    }

    fn index_range(
        &self,
        table: &str,
        column: &str,
        branch: &str,
        start: std::ops::Bound<&Value>,
        end: std::ops::Bound<&Value>,
    ) -> Vec<ObjectId> {
        self.inner.index_range(table, column, branch, start, end)
    }

    fn index_scan_all(&self, table: &str, column: &str, branch: &str) -> Vec<ObjectId> {
        self.inner.index_scan_all(table, column, branch)
    }

    fn flush(&self) {
        self.inner.flush();
    }

    fn flush_wal(&self) {
        self.calls.lock().unwrap().flush_wal_calls += 1;
        self.inner.flush_wal();
    }

    fn close(&self) -> Result<(), StorageError> {
        self.inner.close()
    }
}

fn test_schema() -> Schema {
    SchemaBuilder::new()
        .table(
            TableSchema::builder("users")
                .column("id", ColumnType::Uuid)
                .column("name", ColumnType::Text),
        )
        .build()
}

fn schema_evolution_v1() -> Schema {
    test_schema()
}

fn schema_evolution_v2() -> Schema {
    SchemaBuilder::new()
        .table(
            TableSchema::builder("users")
                .column("id", ColumnType::Uuid)
                .column("name", ColumnType::Text)
                .column("email", ColumnType::Text),
        )
        .build()
}

fn protected_documents_schema() -> Schema {
    let policies = TablePolicies::new()
        .with_select(PolicyExpr::eq_session("owner_id", vec!["user_id".into()]))
        .with_insert(PolicyExpr::eq_session("owner_id", vec!["user_id".into()]));

    SchemaBuilder::new()
        .table(
            TableSchema::builder("documents")
                .column("owner_id", ColumnType::Text)
                .column("title", ColumnType::Text)
                .policies(policies),
        )
        .build()
}

fn defaulted_todos_schema() -> Schema {
    SchemaBuilder::new()
        .table(
            TableSchema::builder("todos")
                .column("title", ColumnType::Text)
                .column_with_default("done", ColumnType::Boolean, Value::Boolean(false)),
        )
        .build()
}

fn user_row_values(id: ObjectId, name: &str) -> Vec<Value> {
    vec![Value::Uuid(id), Value::Text(name.to_string())]
}

fn user_insert_values(id: ObjectId, name: &str) -> HashMap<String, Value> {
    HashMap::from([
        ("id".to_string(), Value::Uuid(id)),
        ("name".to_string(), Value::Text(name.to_string())),
    ])
}

fn document_insert_values(owner_id: &str, title: &str) -> HashMap<String, Value> {
    HashMap::from([
        ("owner_id".to_string(), Value::Text(owner_id.to_string())),
        ("title".to_string(), Value::Text(title.to_string())),
    ])
}

fn project_insert_values(name: &str, owner_id: &str) -> HashMap<String, Value> {
    HashMap::from([
        ("name".to_string(), Value::Text(name.to_string())),
        ("owner_id".to_string(), Value::Text(owner_id.to_string())),
    ])
}

fn todo_insert_values(
    title: &str,
    done: bool,
    description: Value,
    owner_id: &str,
    project: Value,
) -> HashMap<String, Value> {
    HashMap::from([
        ("title".to_string(), Value::Text(title.to_string())),
        ("done".to_string(), Value::Boolean(done)),
        ("description".to_string(), description),
        ("owner_id".to_string(), Value::Text(owner_id.to_string())),
        ("project".to_string(), project),
    ])
}

fn create_runtime_with_schema_and_sync_manager(
    schema: Schema,
    app_name: &str,
    sync_manager: SyncManager,
) -> TestCore {
    let app_id = AppId::from_name(app_name);
    let schema_manager = SchemaManager::new(sync_manager, schema, app_id, "dev", "main").unwrap();
    let mut core = RuntimeCore::new(schema_manager, MemoryStorage::new(), NoopScheduler);
    core.immediate_tick();
    core
}

fn create_runtime_with_schema(schema: Schema, app_name: &str) -> TestCore {
    create_runtime_with_schema_and_sync_manager(schema, app_name, SyncManager::new())
}

fn create_runtime_with_storage(schema: Schema, app_name: &str, storage: MemoryStorage) -> TestCore {
    let app_id = AppId::from_name(app_name);
    let schema_manager =
        SchemaManager::new(SyncManager::new(), schema, app_id, "dev", "main").unwrap();
    let mut core = RuntimeCore::new(schema_manager, storage, NoopScheduler);
    core.immediate_tick();
    core
}

fn create_runtime_with_boxed_storage(
    schema: Schema,
    app_name: &str,
    storage: Box<dyn Storage>,
) -> BoxedStorageTestCore {
    let app_id = AppId::from_name(app_name);
    let schema_manager =
        SchemaManager::new(SyncManager::new(), schema, app_id, "dev", "main").unwrap();
    let mut core = RuntimeCore::new(schema_manager, storage, NoopScheduler);
    core.immediate_tick();
    core
}

fn create_test_runtime() -> TestCore {
    create_runtime_with_schema(test_schema(), "test-app")
}

// ---------------------------------------------------------------------------
// install_transport tests
// ---------------------------------------------------------------------------

#[cfg(feature = "transport-websocket")]
mod install_transport_tests {
    use super::*;
    use crate::transport_manager::{AuthConfig, StreamAdapter, TickNotifier};

    struct NopTick;
    impl TickNotifier for NopTick {
        fn notify(&self) {}
    }

    struct NopStreamAdapter;
    impl StreamAdapter for NopStreamAdapter {
        type Error = &'static str;
        async fn connect(_url: &str) -> Result<Self, Self::Error> {
            futures::future::pending::<()>().await;
            unreachable!()
        }
        async fn send(&mut self, _data: &[u8]) -> Result<(), Self::Error> {
            Ok(())
        }
        async fn recv(&mut self) -> Result<Option<Vec<u8>>, Self::Error> {
            Ok(None)
        }
        async fn close(&mut self) {}
    }

    #[test]
    #[should_panic(expected = "install_transport called while a transport is already installed")]
    fn install_transport_panics_if_transport_already_installed() {
        let mut core = create_test_runtime();
        // Install once.
        let _first = crate::runtime_core::install_transport::<_, _, NopStreamAdapter, _>(
            &mut core,
            "ws://example.test/ws".to_string(),
            AuthConfig::default(),
            NopTick,
        );
        // Install a second time — must panic via debug_assert.
        let _second = crate::runtime_core::install_transport::<_, _, NopStreamAdapter, _>(
            &mut core,
            "ws://example.test/ws".to_string(),
            AuthConfig::default(),
            NopTick,
        );
    }

    #[test]
    fn install_transport_seeds_catalogue_hash_and_registers_handle() {
        let mut core = create_test_runtime();

        let _manager = crate::runtime_core::install_transport::<_, _, NopStreamAdapter, _>(
            &mut core,
            "ws://example.test/ws".to_string(),
            AuthConfig::default(),
            NopTick,
        );

        assert!(
            core.transport.is_some(),
            "transport handle should be installed"
        );
        let expected_hash = core.schema_manager().catalogue_state_hash();
        let handle_hash = core
            .transport
            .as_ref()
            .unwrap()
            .catalogue_state_hash_for_test();
        assert_eq!(
            handle_hash.as_deref(),
            Some(expected_hash.as_str()),
            "install_transport must seed the handle's catalogue_state_hash",
        );
    }

    #[test]
    fn install_transport_holds_initial_remote_query_frontier_while_connecting() {
        let mut core = create_test_runtime();

        let _manager = crate::runtime_core::install_transport::<_, _, NopStreamAdapter, _>(
            &mut core,
            "ws://example.test/ws".to_string(),
            AuthConfig::default(),
            NopTick,
        );

        let mut future = core.query_with_propagation(
            Query::new("users"),
            None,
            ReadDurabilityOptions {
                tier: Some(DurabilityTier::EdgeServer),
                local_updates: crate::query_manager::manager::LocalUpdates::Immediate,
            },
            crate::sync_manager::QueryPropagation::Full,
        );

        let waker = noop_waker();
        let mut cx = std::task::Context::from_waker(&waker);
        assert!(
            std::pin::Pin::new(&mut future).poll(&mut cx).is_pending(),
            "remote query should stay pending until the transport finishes connecting"
        );
    }

    /// Guards the fix for CI expo-e2e failing when the WS transport never
    /// completes: pending_servers must time out so initial subscriptions
    /// don't hold the first delivery forever.
    #[test]
    fn pending_server_frontier_releases_after_timeout() {
        use crate::sync_manager::PENDING_SERVER_TIMEOUT;

        let mut core = create_test_runtime();

        let _manager = crate::runtime_core::install_transport::<_, _, NopStreamAdapter, _>(
            &mut core,
            "ws://example.test/ws".to_string(),
            AuthConfig::default(),
            NopTick,
        );

        assert!(
            core.schema_manager()
                .query_manager()
                .sync_manager()
                .has_servers_or_pending_servers(),
            "pending_servers must be active immediately after install_transport"
        );

        std::thread::sleep(PENDING_SERVER_TIMEOUT + std::time::Duration::from_millis(100));

        assert!(
            !core
                .schema_manager()
                .query_manager()
                .sync_manager()
                .has_servers_or_pending_servers(),
            "pending_servers must time out so local subscriptions can deliver"
        );
    }

    /// When the transport emits `ConnectFailed` (offline DNS/TCP/TLS error
    /// before the timeout), draining the event must release the pending-server
    /// hold immediately so the user doesn't pay the timeout on cold-start.
    #[test]
    fn connect_failed_event_releases_pending_server_immediately() {
        let mut core = create_test_runtime();

        let _manager = crate::runtime_core::install_transport::<_, _, NopStreamAdapter, _>(
            &mut core,
            "ws://example.test/ws".to_string(),
            AuthConfig::default(),
            NopTick,
        );

        let server_id = core.transport.as_ref().unwrap().server_id;

        assert!(
            core.schema_manager()
                .query_manager()
                .sync_manager()
                .has_servers_or_pending_servers(),
            "pending_servers must be active immediately after install_transport"
        );

        core.handle_transport_inbound_for_test(
            server_id,
            crate::transport_manager::TransportInbound::ConnectFailed {
                reason: "dns lookup failed".into(),
            },
        );

        assert!(
            !core
                .schema_manager()
                .query_manager()
                .sync_manager()
                .has_servers_or_pending_servers(),
            "ConnectFailed must release pending_servers without waiting for the timeout"
        );
    }
}

fn documents_query_by_title(title: &str) -> Query {
    QueryBuilder::new("documents")
        .filter_eq("title", Value::Text(title.into()))
        .build()
}

fn column_index(schema: &Schema, table: &str, column: &str) -> usize {
    schema
        .get(&TableName::new(table))
        .unwrap_or_else(|| panic!("table '{table}' should exist"))
        .columns
        .column_index(column)
        .unwrap_or_else(|| panic!("column '{column}' should exist on table '{table}'"))
}

/// Helper to execute a query synchronously via subscribe/tick/unsubscribe.
fn execute_query(core: &mut TestCore, query: Query) -> Vec<(ObjectId, Vec<Value>)> {
    let sub_id = core
        .schema_manager_mut()
        .query_manager_mut()
        .subscribe(query)
        .unwrap();
    core.immediate_tick();
    let results = core
        .schema_manager_mut()
        .query_manager_mut()
        .get_subscription_results(sub_id);
    core.schema_manager_mut()
        .query_manager_mut()
        .unsubscribe_with_sync(sub_id);
    results
}

fn execute_runtime_query(
    core: &mut TestCore,
    query: Query,
    session: Option<Session>,
) -> Vec<(ObjectId, Vec<Value>)> {
    execute_runtime_query_with_propagation(
        core,
        query,
        session,
        crate::sync_manager::QueryPropagation::Full,
    )
}

fn execute_local_runtime_query(
    core: &mut TestCore,
    query: Query,
    session: Option<Session>,
) -> Vec<(ObjectId, Vec<Value>)> {
    execute_runtime_query_with_propagation(
        core,
        query,
        session,
        crate::sync_manager::QueryPropagation::LocalOnly,
    )
}

fn execute_runtime_query_with_propagation(
    core: &mut TestCore,
    query: Query,
    session: Option<Session>,
    propagation: crate::sync_manager::QueryPropagation,
) -> Vec<(ObjectId, Vec<Value>)> {
    execute_runtime_query_with_durability_and_propagation(
        core,
        query,
        session,
        ReadDurabilityOptions::default(),
        propagation,
    )
}

fn execute_runtime_query_with_durability_and_propagation(
    core: &mut TestCore,
    query: Query,
    session: Option<Session>,
    durability: ReadDurabilityOptions,
    propagation: crate::sync_manager::QueryPropagation,
) -> Vec<(ObjectId, Vec<Value>)> {
    let waker = noop_waker();
    let mut cx = std::task::Context::from_waker(&waker);

    let mut future = core.query_with_propagation(query, session, durability, propagation);

    match Pin::new(&mut future).poll(&mut cx) {
        Poll::Ready(Ok(results)) => results,
        Poll::Ready(Err(err)) => panic!("query should succeed: {err:?}"),
        Poll::Pending => panic!("query should resolve immediately"),
    }
}

fn decode_added_rows(delta: &SubscriptionDelta) -> Vec<(ObjectId, Vec<Value>)> {
    delta
        .ordered_delta
        .added
        .iter()
        .map(|row| {
            let values = decode_row(&delta.descriptor, &row.row.data).unwrap_or_else(|err| {
                panic!(
                    "subscription row {:?} should decode successfully: {err:?}",
                    row.row.id
                )
            });
            (row.row.id, values)
        })
        .collect()
}

fn pump_client_messages_to_server(
    client: &mut TestCore,
    server: &mut TestCore,
    server_id: ServerId,
    client_id: ClientId,
) {
    client.batched_tick();
    for entry in client
        .schema_manager_mut()
        .query_manager_mut()
        .sync_manager_mut()
        .take_outbox()
    {
        if entry.destination == Destination::Server(server_id) {
            server.park_sync_message(InboxEntry {
                source: Source::Client(client_id),
                payload: entry.payload,
            });
        }
    }
    server.batched_tick();
    server.immediate_tick();
}

#[allow(clippy::too_many_arguments)]
fn pump_server_with_three_clients(
    server: &mut TestCore,
    writer: &mut TestCore,
    writer_server_id: ServerId,
    writer_client_id: ClientId,
    alice_reader: &mut TestCore,
    alice_reader_server_id: ServerId,
    alice_reader_client_id: ClientId,
    bob_reader: &mut TestCore,
    bob_reader_server_id: ServerId,
    bob_reader_client_id: ClientId,
) -> Vec<OutboxEntry> {
    let mut server_outputs = Vec::new();

    for _ in 0..10 {
        let mut any_messages = false;

        writer.batched_tick();
        for entry in writer
            .schema_manager_mut()
            .query_manager_mut()
            .sync_manager_mut()
            .take_outbox()
        {
            if entry.destination == Destination::Server(writer_server_id) {
                any_messages = true;
                server.park_sync_message(InboxEntry {
                    source: Source::Client(writer_client_id),
                    payload: entry.payload,
                });
            }
        }

        alice_reader.batched_tick();
        for entry in alice_reader
            .schema_manager_mut()
            .query_manager_mut()
            .sync_manager_mut()
            .take_outbox()
        {
            if entry.destination == Destination::Server(alice_reader_server_id) {
                any_messages = true;
                server.park_sync_message(InboxEntry {
                    source: Source::Client(alice_reader_client_id),
                    payload: entry.payload,
                });
            }
        }

        bob_reader.batched_tick();
        for entry in bob_reader
            .schema_manager_mut()
            .query_manager_mut()
            .sync_manager_mut()
            .take_outbox()
        {
            if entry.destination == Destination::Server(bob_reader_server_id) {
                any_messages = true;
                server.park_sync_message(InboxEntry {
                    source: Source::Client(bob_reader_client_id),
                    payload: entry.payload,
                });
            }
        }

        server.batched_tick();
        let server_out = server
            .schema_manager_mut()
            .query_manager_mut()
            .sync_manager_mut()
            .take_outbox();
        server_outputs.extend(server_out.iter().cloned());
        for entry in server_out {
            match entry.destination {
                Destination::Client(client_id) if client_id == writer_client_id => {
                    any_messages = true;
                    writer.park_sync_message(InboxEntry {
                        source: Source::Server(writer_server_id),
                        payload: entry.payload,
                    });
                }
                Destination::Client(client_id) if client_id == alice_reader_client_id => {
                    any_messages = true;
                    alice_reader.park_sync_message(InboxEntry {
                        source: Source::Server(alice_reader_server_id),
                        payload: entry.payload,
                    });
                }
                Destination::Client(client_id) if client_id == bob_reader_client_id => {
                    any_messages = true;
                    bob_reader.park_sync_message(InboxEntry {
                        source: Source::Server(bob_reader_server_id),
                        payload: entry.payload,
                    });
                }
                _ => {}
            }
        }

        writer.batched_tick();
        writer.immediate_tick();
        alice_reader.batched_tick();
        alice_reader.immediate_tick();
        bob_reader.batched_tick();
        bob_reader.immediate_tick();

        if !any_messages {
            break;
        }
    }

    server_outputs
}

fn outbox_has_object_update_for_client(
    entries: &[OutboxEntry],
    client_id: ClientId,
    object_id: ObjectId,
) -> bool {
    entries.iter().any(|entry| {
        matches!(
            &entry.destination,
            Destination::Client(dest_client_id) if *dest_client_id == client_id
        ) && match &entry.payload {
            SyncPayload::RowVersionNeeded { row, .. }
            | SyncPayload::RowVersionCreated { row, .. } => row.row_id == object_id,
            _ => false,
        }
    })
}

#[test]
fn test_runtime_core_new() {
    let core = create_test_runtime();
    let schema = core.current_schema();
    assert!(schema.contains_key(&TableName::new("users")));
}

#[test]
fn test_runtime_core_insert_query() {
    let mut core = create_test_runtime();

    let user_id = ObjectId::new();
    let expected_values = user_row_values(user_id, "Alice");
    let (object_id, row_values) = core
        .insert("users", user_insert_values(user_id, "Alice"), None)
        .unwrap();
    assert!(!object_id.0.is_nil());
    assert_eq!(row_values, expected_values);

    core.immediate_tick();
    core.batched_tick();

    let query = Query::new("users");
    let results = execute_query(&mut core, query);
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].0, object_id);
    assert_eq!(results[0].1, row_values);
}

#[test]
fn add_server_rehydrates_visible_rows_from_storage_after_restart() {
    let mut old_runtime = create_runtime_with_schema(test_schema(), "restart-sync-test");
    let user_id = ObjectId::new();
    let (row_object_id, _) = old_runtime
        .insert("users", user_insert_values(user_id, "Alice"), None)
        .expect("insert should succeed before restart");

    let storage = old_runtime.into_storage();
    let mut restarted = create_runtime_with_storage(test_schema(), "restart-sync-test", storage);

    let server_id = ServerId::new();
    restarted.add_server(server_id);
    restarted.batched_tick();

    let messages = restarted
        .schema_manager_mut()
        .query_manager_mut()
        .sync_manager_mut()
        .take_outbox();
    let synced_row = messages.iter().find(|message| match &message.payload {
        SyncPayload::RowVersionCreated { row, .. } => row.row_id == row_object_id,
        _ => false,
    });

    assert!(
        synced_row.is_some(),
        "row visible before restart should replay to a new server after restart; messages: {}",
        messages
            .iter()
            .map(|message| format!("{:?}", message.payload))
            .collect::<Vec<_>>()
            .join(", ")
    );
}

#[test]
fn test_runtime_core_insert_materializes_schema_defaults() {
    let mut core = create_runtime_with_schema(defaulted_todos_schema(), "todos-with-defaults");

    let (object_id, row_values) = core
        .insert(
            "todos",
            HashMap::from([("title".to_string(), Value::Text("Ship it".to_string()))]),
            None,
        )
        .unwrap();
    assert!(!object_id.0.is_nil());
    let descriptor = &core.current_schema()[&TableName::new("todos")].columns;
    let title_idx = descriptor.column_index("title").unwrap();
    let done_idx = descriptor.column_index("done").unwrap();
    assert_eq!(row_values[title_idx], Value::Text("Ship it".to_string()));
    assert_eq!(row_values[done_idx], Value::Boolean(false));

    core.immediate_tick();
    core.batched_tick();

    let results = execute_query(&mut core, Query::new("todos"));
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].0, object_id);
    assert_eq!(results[0].1, row_values);
}

#[test]
fn test_runtime_core_subscription() {
    let mut core = create_test_runtime();

    let updates: Arc<Mutex<Vec<SubscriptionDelta>>> = Arc::new(Mutex::new(Vec::new()));
    let updates_clone = updates.clone();

    let query = Query::new("users");
    let handle = core
        .subscribe(
            query,
            move |delta| {
                updates_clone.lock().unwrap().push(delta);
            },
            None,
        )
        .unwrap();

    let _object_id = core
        .insert("users", user_insert_values(ObjectId::new(), "Bob"), None)
        .unwrap();

    core.immediate_tick();
    core.batched_tick();

    let updates_vec = updates.lock().unwrap();
    assert!(
        !updates_vec.is_empty(),
        "Should receive subscription update"
    );
    assert_eq!(updates_vec[0].handle, handle);

    drop(updates_vec);
    core.unsubscribe(handle);
}

#[test]
fn test_runtime_core_concurrent_inserts_from_multiple_callers() {
    use std::thread;

    let core = Arc::new(Mutex::new(create_test_runtime()));
    let workers = 8;
    let mut handles = Vec::new();

    for i in 0..workers {
        let core_ref = Arc::clone(&core);
        handles.push(thread::spawn(move || {
            let mut locked = core_ref.lock().unwrap();
            locked
                .insert(
                    "users",
                    user_insert_values(ObjectId::new(), &format!("User-{i}")),
                    None,
                )
                .unwrap();
        }));
    }

    for handle in handles {
        handle.join().expect("worker thread should complete");
    }

    let mut locked = core.lock().unwrap();
    locked.immediate_tick();
    locked.batched_tick();

    let results = execute_query(&mut locked, Query::new("users"));
    assert_eq!(
        results.len(),
        workers,
        "All concurrent inserts should be visible"
    );
}

#[test]
fn test_runtime_core_update_delete() {
    let mut core = create_test_runtime();

    let id = ObjectId::new();
    let (object_id, _row_values) = core
        .insert("users", user_insert_values(id, "Charlie"), None)
        .unwrap();
    core.immediate_tick();
    core.batched_tick();

    let updates = vec![("name".to_string(), Value::Text("Dave".to_string()))];
    core.update(object_id, updates, None).unwrap();
    core.immediate_tick();
    core.batched_tick();

    let query = Query::new("users");
    let results = execute_query(&mut core, query);
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].1[1], Value::Text("Dave".to_string()));

    core.delete(object_id, None).unwrap();
    core.immediate_tick();
    core.batched_tick();

    let query = Query::new("users");
    let results = execute_query(&mut core, query);
    assert_eq!(results.len(), 0);
}

#[test]
fn rc_user_inserted_row_stays_hidden_from_other_sessions() {
    let schema = protected_documents_schema();
    let mut client = create_runtime_with_schema(schema.clone(), "scope-bypass-test");
    let mut server = create_runtime_with_schema(schema, "scope-bypass-test");

    let alice_session = Session::new("alice");
    let title = "alice-private-doc";
    let client_id = ClientId::new();
    let server_id = ServerId::new();

    server.add_client(client_id, Some(alice_session.clone()));
    client.add_server(server_id);
    assert_eq!(
        server
            .schema_manager()
            .query_manager()
            .sync_manager()
            .get_client(client_id)
            .expect("client should be registered on server")
            .role,
        ClientRole::User,
        "test must exercise the user auth path instead of a trusted-role bypass"
    );

    // Clear any connection-startup traffic so this test only inspects the write under test.
    client.batched_tick();
    server.batched_tick();
    client
        .schema_manager_mut()
        .query_manager_mut()
        .sync_manager_mut()
        .take_outbox();
    server
        .schema_manager_mut()
        .query_manager_mut()
        .sync_manager_mut()
        .take_outbox();

    let (document_id, row_values) = client
        .insert(
            "documents",
            document_insert_values("alice", title),
            Some(&WriteContext::from_session(alice_session.clone())),
        )
        .expect("alice insert should satisfy local insert policy");

    pump_client_messages_to_server(&mut client, &mut server, server_id, client_id);

    let alice_results = execute_runtime_query(
        &mut server,
        documents_query_by_title(title),
        Some(Session::new("alice")),
    );
    assert_eq!(
        alice_results.len(),
        1,
        "alice should be able to read her row"
    );
    assert_eq!(alice_results[0].0, document_id);
    assert_eq!(alice_results[0].1, row_values);

    let bob_results = execute_runtime_query(
        &mut server,
        documents_query_by_title(title),
        Some(Session::new("bob")),
    );
    assert!(
        bob_results.is_empty(),
        "bob should not be able to read alice's row after a client-originated insert"
    );
}

#[test]
fn rc_user_subscription_does_not_forward_rows_to_other_sessions() {
    let schema = protected_documents_schema();
    let mut writer = create_runtime_with_schema(schema.clone(), "scope-bypass-subscription-test");
    let mut alice_reader =
        create_runtime_with_schema(schema.clone(), "scope-bypass-subscription-test");
    let mut bob_reader =
        create_runtime_with_schema(schema.clone(), "scope-bypass-subscription-test");
    let mut server = create_runtime_with_schema_and_sync_manager(
        schema,
        "scope-bypass-subscription-test",
        SyncManager::new().with_durability_tier(DurabilityTier::Worker),
    );

    let alice_session = Session::new("alice");
    let bob_session = Session::new("bob");
    let writer_client_id = ClientId::new();
    let writer_server_id = ServerId::new();
    let alice_reader_client_id = ClientId::new();
    let alice_reader_server_id = ServerId::new();
    let bob_reader_client_id = ClientId::new();
    let bob_reader_server_id = ServerId::new();
    let title = "alice-private-doc";

    server.add_client(writer_client_id, Some(alice_session.clone()));
    writer.add_server(writer_server_id);
    server.add_client(alice_reader_client_id, Some(alice_session.clone()));
    alice_reader.add_server(alice_reader_server_id);
    server.add_client(bob_reader_client_id, Some(bob_session.clone()));
    bob_reader.add_server(bob_reader_server_id);

    assert_eq!(
        server
            .schema_manager()
            .query_manager()
            .sync_manager()
            .get_client(writer_client_id)
            .expect("writer client should be registered on server")
            .role,
        ClientRole::User,
        "writer must use the user auth path"
    );
    assert_eq!(
        server
            .schema_manager()
            .query_manager()
            .sync_manager()
            .get_client(alice_reader_client_id)
            .expect("alice reader should be registered on server")
            .role,
        ClientRole::User,
        "alice reader must use the user auth path"
    );
    assert_eq!(
        server
            .schema_manager()
            .query_manager()
            .sync_manager()
            .get_client(bob_reader_client_id)
            .expect("bob reader should be registered on server")
            .role,
        ClientRole::User,
        "bob reader must use the user auth path"
    );

    let alice_deliveries = Arc::new(Mutex::new(Vec::<Vec<(ObjectId, Vec<Value>)>>::new()));
    let alice_deliveries_clone = alice_deliveries.clone();
    let _alice_reader_handle = alice_reader
        .subscribe(
            Query::new("documents"),
            move |delta| {
                let rows = decode_added_rows(&delta);
                if !rows.is_empty() {
                    alice_deliveries_clone.lock().unwrap().push(rows);
                }
            },
            Some(alice_session.clone()),
        )
        .expect("alice reader subscription should be created");

    let bob_deliveries = Arc::new(Mutex::new(Vec::<Vec<(ObjectId, Vec<Value>)>>::new()));
    let bob_deliveries_clone = bob_deliveries.clone();
    let _bob_reader_handle = bob_reader
        .subscribe(
            Query::new("documents"),
            move |delta| {
                let rows = decode_added_rows(&delta);
                if !rows.is_empty() {
                    bob_deliveries_clone.lock().unwrap().push(rows);
                }
            },
            Some(bob_session.clone()),
        )
        .expect("bob reader subscription should be created");

    pump_server_with_three_clients(
        &mut server,
        &mut writer,
        writer_server_id,
        writer_client_id,
        &mut alice_reader,
        alice_reader_server_id,
        alice_reader_client_id,
        &mut bob_reader,
        bob_reader_server_id,
        bob_reader_client_id,
    );

    assert_eq!(
        server
            .schema_manager()
            .query_manager()
            .sync_manager()
            .get_client(alice_reader_client_id)
            .expect("alice reader should still be connected")
            .queries
            .len(),
        1,
        "server should register alice's active query before the write"
    );
    assert_eq!(
        server
            .schema_manager()
            .query_manager()
            .sync_manager()
            .get_client(bob_reader_client_id)
            .expect("bob reader should still be connected")
            .queries
            .len(),
        1,
        "server should register bob's active query before the write"
    );

    let (document_id, row_values) = writer
        .insert(
            "documents",
            document_insert_values("alice", title),
            Some(&WriteContext::from_session(alice_session.clone())),
        )
        .expect("alice insert should succeed through the public client API");

    let server_outputs_after_write = pump_server_with_three_clients(
        &mut server,
        &mut writer,
        writer_server_id,
        writer_client_id,
        &mut alice_reader,
        alice_reader_server_id,
        alice_reader_client_id,
        &mut bob_reader,
        bob_reader_server_id,
        bob_reader_client_id,
    );

    let server_results = execute_runtime_query(
        &mut server,
        documents_query_by_title(title),
        Some(alice_session.clone()),
    );
    assert_eq!(
        server_results,
        vec![(document_id, row_values.clone())],
        "server should store the synced row for alice"
    );
    assert!(
        outbox_has_object_update_for_client(
            &server_outputs_after_write,
            alice_reader_client_id,
            document_id,
        ),
        "server should forward alice's row to an authorized downstream alice reader"
    );
    assert!(
        !outbox_has_object_update_for_client(
            &server_outputs_after_write,
            bob_reader_client_id,
            document_id,
        ),
        "server must not forward alice's row to bob on the wire"
    );

    let alice_received_rows: Vec<(ObjectId, Vec<Value>)> = alice_deliveries
        .lock()
        .unwrap()
        .iter()
        .flat_map(|rows| rows.iter().cloned())
        .collect();
    assert_eq!(
        alice_received_rows,
        vec![(document_id, row_values.clone())],
        "authorized alice reader should receive exactly the inserted row"
    );

    let leaked_rows: Vec<(ObjectId, Vec<Value>)> = bob_deliveries
        .lock()
        .unwrap()
        .iter()
        .flat_map(|rows| rows.iter().cloned())
        .collect();
    assert!(
        leaked_rows.is_empty(),
        "bob should not receive alice's row through an active downstream subscription"
    );

    let alice_reader_results = execute_local_runtime_query(
        &mut alice_reader,
        documents_query_by_title(title),
        Some(alice_session.clone()),
    );
    assert_eq!(
        alice_reader_results,
        vec![(document_id, row_values.clone())],
        "authorized alice reader should also be able to query the synced row"
    );

    let bob_reader_results = execute_local_runtime_query(
        &mut bob_reader,
        documents_query_by_title(title),
        Some(bob_session.clone()),
    );
    assert!(
        bob_reader_results.is_empty(),
        "bob's local state should stay empty after alice's write is forwarded through the server"
    );

    let mut fresh_bob_query = bob_reader.query_with_propagation(
        documents_query_by_title(title),
        Some(bob_session.clone()),
        ReadDurabilityOptions {
            tier: Some(DurabilityTier::Worker),
            local_updates: crate::query_manager::manager::LocalUpdates::Deferred,
        },
        crate::sync_manager::QueryPropagation::Full,
    );
    let waker = noop_waker();
    let mut cx = std::task::Context::from_waker(&waker);
    assert!(
        Pin::new(&mut fresh_bob_query).poll(&mut cx).is_pending(),
        "fresh bob full query should wait for Worker settlement instead of resolving from local empty state"
    );

    let server_outputs_after_fresh_bob_query = pump_server_with_three_clients(
        &mut server,
        &mut writer,
        writer_server_id,
        writer_client_id,
        &mut alice_reader,
        alice_reader_server_id,
        alice_reader_client_id,
        &mut bob_reader,
        bob_reader_server_id,
        bob_reader_client_id,
    );
    assert!(
        !outbox_has_object_update_for_client(
            &server_outputs_after_fresh_bob_query,
            bob_reader_client_id,
            document_id,
        ),
        "fresh bob full query must not cause alice's row to be sent downstream"
    );

    match Pin::new(&mut fresh_bob_query).poll(&mut cx) {
        Poll::Ready(Ok(results)) => {
            assert!(
                results.is_empty(),
                "fresh bob full query should resolve to an empty result after server settlement"
            );
        }
        Poll::Ready(Err(err)) => panic!("fresh bob full query should succeed: {err:?}"),
        Poll::Pending => panic!("fresh bob full query should resolve after Worker settlement"),
    }
}

#[test]
fn test_park_sync_message() {
    use crate::metadata::RowProvenance;
    use crate::sync_manager::{Source, SyncPayload};

    let mut core = create_test_runtime();

    let message = InboxEntry {
        source: Source::Server(ServerId::new()),
        payload: SyncPayload::RowVersionCreated {
            metadata: None,
            row: crate::row_histories::StoredRowVersion::new(
                ObjectId::new(),
                "main",
                Vec::new(),
                b"alice".to_vec(),
                RowProvenance::for_insert(ObjectId::new().to_string(), 1_000),
                HashMap::new(),
                crate::row_histories::RowState::VisibleDirect,
                None,
            ),
        },
    };
    core.park_sync_message(message);

    assert_eq!(core.parked_sync_messages.len(), 1);
}

// =========================================================================
// Durability API Tests (3-tier: A ↔ B[Worker] ↔ C[EdgeServer])
// =========================================================================

/// Three-tier RuntimeCore setup for durability tests.
struct ThreeTierRC {
    a: TestCore,
    b: TestCore,
    c: TestCore,
    a_client_of_b: ClientId,
    b_server_for_a: ServerId,
    b_client_of_c: ClientId,
    c_server_for_b: ServerId,
}

fn create_3tier_rc() -> ThreeTierRC {
    let schema = test_schema();
    let app_id = AppId::from_name("durability-test");

    // A = client (no tier)
    let sm_a = SyncManager::new();
    let mgr_a = SchemaManager::new(sm_a, schema.clone(), app_id, "dev", "main").unwrap();
    let mut a = RuntimeCore::new(mgr_a, MemoryStorage::new(), NoopScheduler);

    // B = Worker server
    let sm_b = SyncManager::new().with_durability_tier(DurabilityTier::Worker);
    let mgr_b = SchemaManager::new(sm_b, schema.clone(), app_id, "dev", "main").unwrap();
    let mut b = RuntimeCore::new(mgr_b, MemoryStorage::new(), NoopScheduler);

    // C = EdgeServer
    let sm_c = SyncManager::new().with_durability_tier(DurabilityTier::EdgeServer);
    let mgr_c = SchemaManager::new(sm_c, schema, app_id, "dev", "main").unwrap();
    let mut c = RuntimeCore::new(mgr_c, MemoryStorage::new(), NoopScheduler);

    let a_client_of_b = ClientId::new();
    let b_server_for_a = ServerId::new();
    let b_client_of_c = ClientId::new();
    let c_server_for_b = ServerId::new();

    // Topology: A ↔ B ↔ C
    {
        b.add_client(a_client_of_b, None);
        b.schema_manager_mut()
            .query_manager_mut()
            .sync_manager_mut()
            .set_client_role(a_client_of_b, ClientRole::Peer);
    }
    a.add_server(b_server_for_a);

    {
        c.add_client(b_client_of_c, None);
        c.schema_manager_mut()
            .query_manager_mut()
            .sync_manager_mut()
            .set_client_role(b_client_of_c, ClientRole::Peer);
    }
    b.add_server(c_server_for_b);

    // Initial tick + clear initial sync messages
    a.immediate_tick();
    b.immediate_tick();
    c.immediate_tick();
    a.batched_tick();
    b.batched_tick();
    c.batched_tick();
    a.schema_manager_mut()
        .query_manager_mut()
        .sync_manager_mut()
        .take_outbox();
    b.schema_manager_mut()
        .query_manager_mut()
        .sync_manager_mut()
        .take_outbox();
    c.schema_manager_mut()
        .query_manager_mut()
        .sync_manager_mut()
        .take_outbox();

    ThreeTierRC {
        a,
        b,
        c,
        a_client_of_b,
        b_server_for_a,
        b_client_of_c,
        c_server_for_b,
    }
}

/// Pump all messages between 3 RuntimeCore nodes until quiescent.
fn pump_3tier(s: &mut ThreeTierRC) {
    for _ in 0..10 {
        let mut any_messages = false;

        // A outbox → B
        s.a.batched_tick();
        let a_out =
            s.a.schema_manager_mut()
                .query_manager_mut()
                .sync_manager_mut()
                .take_outbox();
        for entry in a_out {
            if entry.destination == Destination::Server(s.b_server_for_a) {
                any_messages = true;
                s.b.park_sync_message(InboxEntry {
                    source: Source::Client(s.a_client_of_b),
                    payload: entry.payload,
                });
            }
        }

        // B process, then route outbox to A or C
        s.b.batched_tick();
        s.b.immediate_tick();
        s.b.batched_tick();
        let b_out =
            s.b.schema_manager_mut()
                .query_manager_mut()
                .sync_manager_mut()
                .take_outbox();
        for entry in b_out {
            match entry.destination {
                Destination::Client(cid) if cid == s.a_client_of_b => {
                    any_messages = true;
                    s.a.park_sync_message(InboxEntry {
                        source: Source::Server(s.b_server_for_a),
                        payload: entry.payload,
                    });
                }
                Destination::Server(sid) if sid == s.c_server_for_b => {
                    any_messages = true;
                    s.c.park_sync_message(InboxEntry {
                        source: Source::Client(s.b_client_of_c),
                        payload: entry.payload,
                    });
                }
                _ => {}
            }
        }

        // C process, then route outbox to B
        s.c.batched_tick();
        s.c.immediate_tick();
        s.c.batched_tick();
        let c_out =
            s.c.schema_manager_mut()
                .query_manager_mut()
                .sync_manager_mut()
                .take_outbox();
        for entry in c_out {
            if entry.destination == Destination::Client(s.b_client_of_c) {
                any_messages = true;
                s.b.park_sync_message(InboxEntry {
                    source: Source::Server(s.c_server_for_b),
                    payload: entry.payload,
                });
            }
        }

        // A processes incoming
        s.a.batched_tick();
        s.a.immediate_tick();

        if !any_messages {
            break;
        }
    }
}

/// Pump only A → B (one hop, no C).
fn pump_a_to_b(s: &mut ThreeTierRC) {
    s.a.batched_tick();
    let a_out =
        s.a.schema_manager_mut()
            .query_manager_mut()
            .sync_manager_mut()
            .take_outbox();
    for entry in a_out {
        if entry.destination == Destination::Server(s.b_server_for_a) {
            s.b.park_sync_message(InboxEntry {
                source: Source::Client(s.a_client_of_b),
                payload: entry.payload,
            });
        }
    }
    s.b.batched_tick();
    s.b.immediate_tick();
}

/// Route B's outbox to both A and C as appropriate.
fn route_b_outbox(s: &mut ThreeTierRC) {
    s.b.batched_tick();
    let b_out =
        s.b.schema_manager_mut()
            .query_manager_mut()
            .sync_manager_mut()
            .take_outbox();
    for entry in b_out {
        match entry.destination {
            Destination::Client(cid) if cid == s.a_client_of_b => {
                s.a.park_sync_message(InboxEntry {
                    source: Source::Server(s.b_server_for_a),
                    payload: entry.payload,
                });
            }
            Destination::Server(sid) if sid == s.c_server_for_b => {
                s.c.park_sync_message(InboxEntry {
                    source: Source::Client(s.b_client_of_c),
                    payload: entry.payload,
                });
            }
            _ => {}
        }
    }
}

/// Pump B → A (acks back).
fn pump_b_to_a(s: &mut ThreeTierRC) {
    route_b_outbox(s);
    s.a.batched_tick();
    s.a.immediate_tick();
}

/// Pump B → C (forward to edge).
fn pump_b_to_c(s: &mut ThreeTierRC) {
    route_b_outbox(s);
    s.c.batched_tick();
    s.c.immediate_tick();
}

/// Pump C → B → A (edge ack relay).
fn pump_c_to_b_to_a(s: &mut ThreeTierRC) {
    // C → B
    s.c.batched_tick();
    let c_out =
        s.c.schema_manager_mut()
            .query_manager_mut()
            .sync_manager_mut()
            .take_outbox();
    for entry in c_out {
        if entry.destination == Destination::Client(s.b_client_of_c) {
            s.b.park_sync_message(InboxEntry {
                source: Source::Server(s.c_server_for_b),
                payload: entry.payload,
            });
        }
    }
    s.b.batched_tick();
    s.b.immediate_tick();

    // B → A
    pump_b_to_a(s);
}

fn count_query_subscriptions_to_server(entries: &[OutboxEntry], server_id: ServerId) -> usize {
    entries
        .iter()
        .filter(|entry| {
            matches!(
                &entry.destination,
                Destination::Server(dest_server_id) if *dest_server_id == server_id
            ) && matches!(&entry.payload, SyncPayload::QuerySubscription { .. })
        })
        .count()
}

#[test]
fn rc_replays_downstream_query_when_upstream_added_late() {
    // Build A <-> B first (no B <-> C yet), so B processes a downstream
    // query subscription before it has any upstream server.
    let schema = test_schema();
    let app_id = AppId::from_name("query-replay-test");

    let mgr_a =
        SchemaManager::new(SyncManager::new(), schema.clone(), app_id, "dev", "main").unwrap();
    let mut a = RuntimeCore::new(mgr_a, MemoryStorage::new(), NoopScheduler);

    let mgr_b = SchemaManager::new(
        SyncManager::new().with_durability_tier(DurabilityTier::Worker),
        schema.clone(),
        app_id,
        "dev",
        "main",
    )
    .unwrap();
    let mut b = RuntimeCore::new(mgr_b, MemoryStorage::new(), NoopScheduler);

    let mgr_c = SchemaManager::new(
        SyncManager::new().with_durability_tier(DurabilityTier::EdgeServer),
        schema,
        app_id,
        "dev",
        "main",
    )
    .unwrap();
    let mut c = RuntimeCore::new(mgr_c, MemoryStorage::new(), NoopScheduler);

    let a_client_of_b = ClientId::new();
    let b_server_for_a = ServerId::new();
    let b_client_of_c = ClientId::new();
    let c_server_for_b = ServerId::new();

    {
        b.add_client(a_client_of_b, None);
        b.schema_manager_mut()
            .query_manager_mut()
            .sync_manager_mut()
            .set_client_role(a_client_of_b, ClientRole::Peer);
    }
    a.add_server(b_server_for_a);

    // Clear any startup sync traffic.
    a.immediate_tick();
    b.immediate_tick();
    c.immediate_tick();
    a.batched_tick();
    b.batched_tick();
    c.batched_tick();
    a.schema_manager_mut()
        .query_manager_mut()
        .sync_manager_mut()
        .take_outbox();
    b.schema_manager_mut()
        .query_manager_mut()
        .sync_manager_mut()
        .take_outbox();
    c.schema_manager_mut()
        .query_manager_mut()
        .sync_manager_mut()
        .take_outbox();

    // Downstream client A subscribes before B has an upstream.
    let _handle = a.subscribe(Query::new("users"), |_delta| {}, None).unwrap();

    // Deliver only A -> B messages.
    a.batched_tick();
    for entry in a
        .schema_manager_mut()
        .query_manager_mut()
        .sync_manager_mut()
        .take_outbox()
    {
        if entry.destination == Destination::Server(b_server_for_a) {
            b.park_sync_message(InboxEntry {
                source: Source::Client(a_client_of_b),
                payload: entry.payload,
            });
        }
    }
    b.batched_tick();
    b.immediate_tick();
    b.batched_tick();
    b.schema_manager_mut()
        .query_manager_mut()
        .sync_manager_mut()
        .take_outbox();

    // Bring up B <-> C after B already has active downstream query state.
    {
        c.add_client(b_client_of_c, None);
        c.schema_manager_mut()
            .query_manager_mut()
            .sync_manager_mut()
            .set_client_role(b_client_of_c, ClientRole::Peer);
    }
    b.add_server(c_server_for_b);
    b.batched_tick();

    let forwarded_query_subscriptions = b
        .schema_manager_mut()
        .query_manager_mut()
        .sync_manager_mut()
        .take_outbox()
        .into_iter()
        .filter(|entry| {
            matches!(
                &entry.destination,
                Destination::Server(server_id) if *server_id == c_server_for_b
            ) && matches!(&entry.payload, SyncPayload::QuerySubscription { .. })
        })
        .count();

    assert!(
        forwarded_query_subscriptions > 0,
        "Expected B to replay existing downstream QuerySubscription(s) when adding upstream"
    );
}

#[test]
fn rc_replays_active_queries_on_upstream_reconnect() {
    let mut s = create_3tier_rc();

    let _handle =
        s.a.subscribe(Query::new("users"), |_delta| {}, None)
            .unwrap();
    pump_a_to_b(&mut s);

    let initial_forwarded =
        s.b.schema_manager_mut()
            .query_manager_mut()
            .sync_manager_mut()
            .take_outbox();
    assert!(
        count_query_subscriptions_to_server(&initial_forwarded, s.c_server_for_b) > 0,
        "Expected initial QuerySubscription forwarding from B to C"
    );

    // Simulate upstream disconnect/reconnect.
    s.b.remove_server(s.c_server_for_b);
    s.b.add_server(s.c_server_for_b);
    s.b.batched_tick();

    let replayed_forwarded =
        s.b.schema_manager_mut()
            .query_manager_mut()
            .sync_manager_mut()
            .take_outbox();
    assert!(
        count_query_subscriptions_to_server(&replayed_forwarded, s.c_server_for_b) > 0,
        "Expected active QuerySubscription replay after upstream reconnect"
    );
}

#[test]
fn rc_does_not_replay_unsubscribed_queries_on_upstream_reconnect() {
    let mut s = create_3tier_rc();

    let handle =
        s.a.subscribe(Query::new("users"), |_delta| {}, None)
            .unwrap();
    pump_a_to_b(&mut s);

    let initial_forwarded =
        s.b.schema_manager_mut()
            .query_manager_mut()
            .sync_manager_mut()
            .take_outbox();
    assert!(
        count_query_subscriptions_to_server(&initial_forwarded, s.c_server_for_b) > 0,
        "Expected initial QuerySubscription forwarding from B to C"
    );

    s.a.unsubscribe(handle);
    pump_a_to_b(&mut s);
    s.b.schema_manager_mut()
        .query_manager_mut()
        .sync_manager_mut()
        .take_outbox(); // Drain unsubscription forwarding and unrelated traffic.

    // Reconnect upstream and ensure replay no longer includes this query.
    s.b.remove_server(s.c_server_for_b);
    s.b.add_server(s.c_server_for_b);
    s.b.batched_tick();

    let replayed_forwarded =
        s.b.schema_manager_mut()
            .query_manager_mut()
            .sync_manager_mut()
            .take_outbox();
    assert_eq!(
        count_query_subscriptions_to_server(&replayed_forwarded, s.c_server_for_b),
        0,
        "Unsubscribed query must not be replayed after upstream reconnect"
    );
}

#[test]
fn rc_insert_returns_immediately() {
    let mut s = create_3tier_rc();
    let user_id = ObjectId::new();
    let expected_values = user_row_values(user_id, "Alice");
    let (id, row_values) =
        s.a.insert("users", user_insert_values(user_id, "Alice"), None)
            .unwrap();
    assert!(!id.0.is_nil());
    assert_eq!(row_values, expected_values);

    let query = Query::new("users");
    let results = execute_query(&mut s.a, query);
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].0, id);
    assert_eq!(results[0].1, row_values);
}

#[test]
fn rc_insert_data_syncs_to_server() {
    let mut s = create_3tier_rc();
    let (id, _row_values) =
        s.a.insert("users", user_insert_values(ObjectId::new(), "Alice"), None)
            .unwrap();

    pump_a_to_b(&mut s);

    let query = Query::new("users");
    let results = execute_query(&mut s.b, query);
    assert_eq!(results.len(), 1, "Server B should have the synced row");
    assert_eq!(results[0].0, id);
}

#[test]
fn rc_insert_syncs_exact_row_version_without_row_region_reads() {
    let mut core = create_runtime_with_boxed_storage(
        test_schema(),
        "row-version-direct-sync-test",
        Box::new(RowRegionReadFailingStorage::new()),
    );
    let server_id = ServerId::new();
    core.add_server(server_id);
    core.batched_tick();
    core.schema_manager_mut()
        .query_manager_mut()
        .sync_manager_mut()
        .take_outbox();

    let (row_id, _row_values) = core
        .insert("users", user_insert_values(ObjectId::new(), "Alice"), None)
        .unwrap();
    core.batched_tick();

    let messages = core
        .schema_manager_mut()
        .query_manager_mut()
        .sync_manager_mut()
        .take_outbox();
    let row_sync = messages
        .iter()
        .find(|entry| matches!(&entry.payload, SyncPayload::RowVersionCreated { row, .. } if row.row_id == row_id))
        .expect("insert should still sync the row upstream");

    match &row_sync.payload {
        SyncPayload::RowVersionCreated { row, .. } => {
            assert_eq!(row.row_id, row_id);
        }
        other => {
            panic!("local row writes should sync using the authored row version, got {other:?}")
        }
    }
}

#[test]
fn rc_row_writes_do_not_touch_legacy_commit_storage() {
    let calls = Arc::new(Mutex::new(LegacyStorageCallCounts::default()));
    let mut core = create_runtime_with_boxed_storage(
        test_schema(),
        "row-no-legacy-commit-storage",
        Box::new(LegacyPersistenceObservingStorage::new(Arc::clone(&calls))),
    );

    let (row_id, _row_values) = core
        .insert("users", user_insert_values(ObjectId::new(), "Alice"), None)
        .unwrap();

    core.update(
        row_id,
        vec![("name".into(), Value::Text("Bob".into()))],
        None,
    )
    .unwrap();
    core.delete(row_id, None).unwrap();

    assert_eq!(
        *calls.lock().unwrap(),
        LegacyStorageCallCounts::default(),
        "row writes should persist only via row histories, not legacy branch commit storage"
    );
}

#[test]
fn rc_local_row_writes_batch_row_and_index_mutations() {
    let calls = Arc::new(Mutex::new(RowMutationCallCounts::default()));
    let mut core = create_runtime_with_boxed_storage(
        test_schema(),
        "row-batched-storage-mutation",
        Box::new(RowMutationObservingStorage::new(Arc::clone(&calls))),
    );

    let (row_id, _row_values) = core
        .insert("users", user_insert_values(ObjectId::new(), "Alice"), None)
        .unwrap();
    core.update(
        row_id,
        vec![("name".into(), Value::Text("Bob".into()))],
        None,
    )
    .unwrap();
    core.delete(row_id, None).unwrap();

    assert_eq!(
        *calls.lock().unwrap(),
        RowMutationCallCounts {
            row_mutation_calls: 3,
            separate_index_mutation_calls: 0,
            flush_wal_calls: 0,
        },
        "local row writes should persist row history, visible heads, and index changes in one storage mutation"
    );
}

#[test]
fn rc_batched_tick_skips_flush_wal_without_storage_writes() {
    let calls = Arc::new(Mutex::new(RowMutationCallCounts::default()));
    let mut core = create_runtime_with_boxed_storage(
        test_schema(),
        "row-batched-no-flush",
        Box::new(RowMutationObservingStorage::new(Arc::clone(&calls))),
    );

    core.batched_tick();

    assert_eq!(
        calls.lock().unwrap().flush_wal_calls,
        0,
        "read-only batched ticks should not flush the WAL"
    );
}

#[test]
fn rc_batched_tick_flushes_wal_after_local_write() {
    let calls = Arc::new(Mutex::new(RowMutationCallCounts::default()));
    let mut core = create_runtime_with_boxed_storage(
        test_schema(),
        "row-batched-flush-after-write",
        Box::new(RowMutationObservingStorage::new(Arc::clone(&calls))),
    );

    core.insert("users", user_insert_values(ObjectId::new(), "Alice"), None)
        .unwrap();
    core.batched_tick();

    assert_eq!(
        calls.lock().unwrap().flush_wal_calls,
        1,
        "a batched tick after a local write should flush the WAL once"
    );
}

#[test]
fn rc_batched_tick_skips_flush_wal_for_query_settled_only_message() {
    let calls = Arc::new(Mutex::new(RowMutationCallCounts::default()));
    let mut core = create_runtime_with_boxed_storage(
        test_schema(),
        "row-batched-query-settled",
        Box::new(RowMutationObservingStorage::new(Arc::clone(&calls))),
    );

    core.push_sync_inbox(InboxEntry {
        source: Source::Server(ServerId::new()),
        payload: SyncPayload::QuerySettled {
            query_id: crate::sync_manager::QueryId(1),
            tier: DurabilityTier::Worker,
            through_seq: 1,
        },
    });
    core.batched_tick();

    assert_eq!(
        calls.lock().unwrap().flush_wal_calls,
        0,
        "query-settled notifications alone should not flush the WAL"
    );
}

#[test]
fn rc_update_sync() {
    let mut s = create_3tier_rc();
    let (id, _row_values) =
        s.a.insert("users", user_insert_values(ObjectId::new(), "Alice"), None)
            .unwrap();
    pump_a_to_b(&mut s);

    s.a.update(id, vec![("name".into(), Value::Text("Bob".into()))], None)
        .unwrap();
    pump_a_to_b(&mut s);

    let query = Query::new("users");
    let results = execute_query(&mut s.b, query);
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].1[1], Value::Text("Bob".into()));
}

#[test]
fn rc_delete_sync() {
    let mut s = create_3tier_rc();
    let (id, _row_values) =
        s.a.insert("users", user_insert_values(ObjectId::new(), "Alice"), None)
            .unwrap();
    pump_a_to_b(&mut s);

    s.a.delete(id, None).unwrap();
    pump_a_to_b(&mut s);

    let query = Query::new("users");
    let results = execute_query(&mut s.b, query);
    assert_eq!(results.len(), 0, "Row should be deleted on B");
}

#[test]
fn rc_insert_persisted_resolves_on_worker_ack() {
    let mut s = create_3tier_rc();
    let user_id = ObjectId::new();
    let expected_values = user_row_values(user_id, "Alice");
    let ((id, row_values), mut receiver) =
        s.a.insert_persisted(
            "users",
            user_insert_values(user_id, "Alice"),
            None,
            DurabilityTier::Worker,
        )
        .unwrap();
    assert!(!id.0.is_nil());
    assert_eq!(row_values, expected_values);

    assert!(
        receiver.try_recv().is_err() || receiver.try_recv() == Ok(None),
        "Receiver should not be resolved before ack"
    );

    pump_a_to_b(&mut s);
    pump_b_to_a(&mut s);

    match receiver.try_recv() {
        Ok(Some(())) => {}
        Ok(None) => panic!("Receiver should be resolved after Worker ack"),
        Err(_) => panic!("Receiver was cancelled"),
    }
}

#[test]
fn rc_insert_persisted_does_not_touch_legacy_ack_storage() {
    let calls = Arc::new(Mutex::new(LegacyStorageCallCounts::default()));
    let mut core = create_runtime_with_boxed_storage(
        test_schema(),
        "row-no-legacy-ack-storage",
        Box::new(LegacyPersistenceObservingStorage::new(Arc::clone(&calls))),
    );

    let ((row_id, _row_values), mut receiver) = core
        .insert_persisted(
            "users",
            user_insert_values(ObjectId::new(), "Alice"),
            None,
            DurabilityTier::Worker,
        )
        .unwrap();

    let branch_name = core.schema_manager().branch_name();
    let version_id = core
        .storage
        .load_visible_region_row("users", branch_name.as_str(), row_id)
        .unwrap()
        .expect("persisted insert should materialize a visible row")
        .version_id();

    core.push_sync_inbox(InboxEntry {
        source: Source::Server(ServerId::new()),
        payload: SyncPayload::RowVersionStateChanged {
            row_id,
            branch_name,
            version_id,
            state: None,
            confirmed_tier: Some(DurabilityTier::Worker),
        },
    });
    core.immediate_tick();

    assert_eq!(
        receiver.try_recv(),
        Ok(Some(())),
        "row persisted receiver should resolve from row-version state changes alone"
    );
    assert_eq!(
        *calls.lock().unwrap(),
        LegacyStorageCallCounts::default(),
        "row durability updates should not touch legacy durability-ack storage"
    );
}

#[test]
fn rc_insert_persisted_ignores_row_state_changed_for_different_row_same_version_id() {
    let mut s = create_3tier_rc();
    let ((row_id, _row_values), mut receiver) =
        s.a.insert_persisted(
            "users",
            user_insert_values(ObjectId::new(), "Alice"),
            None,
            DurabilityTier::Worker,
        )
        .unwrap();

    let branch_name = s.a.schema_manager().branch_name();
    let row_version_id = *load_test_row_tip_ids(s.a.storage(), row_id, branch_name)
        .unwrap()
        .iter()
        .next()
        .expect("insert should create one visible tip");

    s.a.push_sync_inbox(InboxEntry {
        source: Source::Server(s.b_server_for_a),
        payload: SyncPayload::RowVersionStateChanged {
            row_id: ObjectId::new(),
            branch_name,
            version_id: row_version_id,
            state: None,
            confirmed_tier: Some(DurabilityTier::Worker),
        },
    });
    s.a.immediate_tick();

    assert_eq!(
        receiver.try_recv(),
        Ok(None),
        "row persisted receivers should ignore row-version acks for a different row, even if the raw version id matches"
    );
}

#[test]
fn rc_insert_persisted_holds_until_correct_tier() {
    let mut s = create_3tier_rc();
    let (_id, mut receiver) =
        s.a.insert_persisted(
            "users",
            user_insert_values(ObjectId::new(), "Alice"),
            None,
            DurabilityTier::EdgeServer,
        )
        .unwrap();

    pump_a_to_b(&mut s);
    pump_b_to_a(&mut s);

    assert_eq!(
        receiver.try_recv(),
        Ok(None),
        "Worker ack should not satisfy EdgeServer request"
    );

    pump_b_to_c(&mut s);
    pump_c_to_b_to_a(&mut s);

    match receiver.try_recv() {
        Ok(Some(())) => {}
        Ok(None) => panic!("Receiver should be resolved after EdgeServer ack"),
        Err(_) => panic!("Receiver was cancelled"),
    }
}

#[test]
fn rc_insert_persisted_higher_tier_satisfies_lower() {
    let mut s = create_3tier_rc();
    let (_id, mut receiver) =
        s.a.insert_persisted(
            "users",
            user_insert_values(ObjectId::new(), "Alice"),
            None,
            DurabilityTier::Worker,
        )
        .unwrap();

    pump_3tier(&mut s);

    match receiver.try_recv() {
        Ok(Some(())) => {}
        Ok(None) => panic!("EdgeServer ack should satisfy Worker request"),
        Err(_) => panic!("Receiver was cancelled"),
    }
}

#[test]
fn rc_update_persisted_resolves_on_ack() {
    let mut s = create_3tier_rc();
    let (id, _row_values) =
        s.a.insert("users", user_insert_values(ObjectId::new(), "Alice"), None)
            .unwrap();
    pump_a_to_b(&mut s);

    let mut receiver =
        s.a.update_persisted(
            id,
            vec![("name".into(), Value::Text("Bob".into()))],
            None,
            DurabilityTier::Worker,
        )
        .unwrap();

    pump_a_to_b(&mut s);
    pump_b_to_a(&mut s);

    match receiver.try_recv() {
        Ok(Some(())) => {}
        Ok(None) => panic!("Update receiver should be resolved after Worker ack"),
        Err(_) => panic!("Receiver was cancelled"),
    }

    let query = Query::new("users");
    let results = execute_query(&mut s.b, query);
    assert_eq!(results[0].1[1], Value::Text("Bob".into()));
}

#[test]
fn rc_delete_persisted_resolves_on_ack() {
    let mut s = create_3tier_rc();
    let (id, _row_values) =
        s.a.insert("users", user_insert_values(ObjectId::new(), "Alice"), None)
            .unwrap();
    pump_a_to_b(&mut s);

    let mut receiver =
        s.a.delete_persisted(id, None, DurabilityTier::Worker)
            .unwrap();

    pump_a_to_b(&mut s);
    pump_b_to_a(&mut s);

    match receiver.try_recv() {
        Ok(Some(())) => {}
        Ok(None) => panic!("Delete receiver should be resolved after Worker ack"),
        Err(_) => panic!("Receiver was cancelled"),
    }

    let query = Query::new("users");
    let results = execute_query(&mut s.b, query);
    assert_eq!(results.len(), 0);
}

#[test]
fn rc_multiple_persisted_inserts_independent() {
    let mut s = create_3tier_rc();

    let (_id1, mut receiver1) =
        s.a.insert_persisted(
            "users",
            user_insert_values(ObjectId::new(), "Alice"),
            None,
            DurabilityTier::Worker,
        )
        .unwrap();

    let (_id2, mut receiver2) =
        s.a.insert_persisted(
            "users",
            user_insert_values(ObjectId::new(), "Bob"),
            None,
            DurabilityTier::Worker,
        )
        .unwrap();

    pump_3tier(&mut s);

    match receiver1.try_recv() {
        Ok(Some(())) => {}
        Ok(None) => panic!("receiver1 should be resolved"),
        Err(_) => panic!("receiver1 cancelled"),
    }
    match receiver2.try_recv() {
        Ok(Some(())) => {}
        Ok(None) => panic!("receiver2 should be resolved"),
        Err(_) => panic!("receiver2 cancelled"),
    }
}

#[test]
fn rc_query_no_settled_tier_immediate() {
    let mut s = create_3tier_rc();

    let (id, _row_values) =
        s.a.insert("users", user_insert_values(ObjectId::new(), "Alice"), None)
            .unwrap();

    let mut future = s.a.query(Query::new("users"), None);

    let waker = noop_waker();
    let mut cx = std::task::Context::from_waker(&waker);
    match Pin::new(&mut future).poll(&mut cx) {
        Poll::Ready(Ok(results)) => {
            assert_eq!(results.len(), 1, "Should have one row");
            assert_eq!(results[0].0, id);
        }
        Poll::Ready(Err(e)) => panic!("Query failed: {:?}", e),
        Poll::Pending => panic!("Query with settled_tier=None should resolve immediately"),
    }
}

#[test]
fn rc_query_settled_tier_holds() {
    let mut s = create_3tier_rc();

    let (id, _row_values) =
        s.a.insert("users", user_insert_values(ObjectId::new(), "Alice"), None)
            .unwrap();

    let mut future = s.a.query_with_propagation(
        Query::new("users"),
        None,
        ReadDurabilityOptions {
            tier: Some(DurabilityTier::Worker),
            local_updates: crate::query_manager::manager::LocalUpdates::Immediate,
        },
        crate::sync_manager::QueryPropagation::Full,
    );

    let waker = noop_waker();
    let mut cx = std::task::Context::from_waker(&waker);
    assert!(
        Pin::new(&mut future).poll(&mut cx).is_pending(),
        "Query should be pending before Worker settlement"
    );

    pump_a_to_b(&mut s);
    pump_b_to_a(&mut s);

    match Pin::new(&mut future).poll(&mut cx) {
        Poll::Ready(Ok(results)) => {
            assert_eq!(results.len(), 1, "Should have one row after settlement");
            assert_eq!(results[0].0, id);
        }
        Poll::Ready(Err(e)) => panic!("Query failed: {:?}", e),
        Poll::Pending => panic!("Query should resolve after Worker QuerySettled"),
    }
}

#[test]
fn rc_query_remote_tier_immediate_local_updates_falls_back_to_local_pending_row() {
    let mut s = create_3tier_rc();

    let (id, _row_values) =
        s.a.insert("users", user_insert_values(ObjectId::new(), "Alice"), None)
            .unwrap();

    let mut future = s.a.query_with_propagation(
        Query::new("users"),
        None,
        ReadDurabilityOptions {
            tier: Some(DurabilityTier::EdgeServer),
            local_updates: crate::query_manager::manager::LocalUpdates::Immediate,
        },
        crate::sync_manager::QueryPropagation::Full,
    );

    let waker = noop_waker();
    let mut cx = std::task::Context::from_waker(&waker);
    assert!(
        Pin::new(&mut future).poll(&mut cx).is_pending(),
        "Query should wait for the initial remote frontier"
    );

    // Worker frontier completion is enough to unblock the first snapshot. With
    // immediate local updates, the locally-authored row should still be visible
    // even though it has not reached EdgeServer durability yet.
    pump_a_to_b(&mut s);
    pump_b_to_a(&mut s);

    match Pin::new(&mut future).poll(&mut cx) {
        Poll::Ready(Ok(results)) => {
            assert_eq!(results.len(), 1, "Should have one locally pending row");
            assert_eq!(results[0].0, id);
        }
        Poll::Ready(Err(e)) => panic!("Query failed: {:?}", e),
        Poll::Pending => panic!("Query should resolve once the initial frontier is complete"),
    }
}

#[test]
fn rc_query_settled_tier_empty_resolves() {
    let mut s = create_3tier_rc();

    let mut future = s.a.query_with_propagation(
        Query::new("users"),
        None,
        ReadDurabilityOptions {
            tier: Some(DurabilityTier::Worker),
            local_updates: crate::query_manager::manager::LocalUpdates::Immediate,
        },
        crate::sync_manager::QueryPropagation::Full,
    );

    let waker = noop_waker();
    let mut cx = std::task::Context::from_waker(&waker);
    assert!(
        Pin::new(&mut future).poll(&mut cx).is_pending(),
        "Query should be pending before Worker settlement"
    );

    // No rows inserted anywhere; query should still resolve once settled tier is reached.
    pump_a_to_b(&mut s);
    pump_b_to_a(&mut s);

    match Pin::new(&mut future).poll(&mut cx) {
        Poll::Ready(Ok(results)) => {
            assert_eq!(
                results.len(),
                0,
                "Settled query with no rows should resolve to empty result"
            );
        }
        Poll::Ready(Err(e)) => panic!("Query failed: {:?}", e),
        Poll::Pending => panic!("Query should resolve after Worker QuerySettled"),
    }
}

#[test]
fn query_reads_pick_row_versions_by_required_durability_tier() {
    let mut core = create_runtime_with_schema_and_sync_manager(
        test_schema(),
        "tier-aware-visible-row",
        SyncManager::new(),
    );
    let branch_name = core.schema_manager().branch_name().to_string();

    // Row history:
    //   v1 --(global)--> visible for global queries
    //    \
    //     `-- v2 --(worker)--> current head for worker queries
    let row_id = ObjectId::new();
    let (object_id, _) = core
        .insert("users", user_insert_values(row_id, "Alice-global"), None)
        .unwrap();
    core.immediate_tick();

    let first_visible = core
        .storage()
        .load_visible_region_row("users", &branch_name, object_id)
        .unwrap()
        .expect("first visible row");
    core.storage_mut()
        .patch_row_region_rows_by_batch(
            "users",
            first_visible.batch_id,
            None,
            Some(DurabilityTier::GlobalServer),
        )
        .unwrap();

    core.update(
        object_id,
        vec![("name".into(), Value::Text("Alice-worker".into()))],
        None,
    )
    .unwrap();
    core.immediate_tick();

    let second_visible = core
        .storage()
        .load_visible_region_row("users", &branch_name, object_id)
        .unwrap()
        .expect("second visible row");
    core.storage_mut()
        .patch_row_region_rows_by_batch(
            "users",
            second_visible.batch_id,
            None,
            Some(DurabilityTier::Worker),
        )
        .unwrap();

    let worker_rows = execute_runtime_query_with_durability_and_propagation(
        &mut core,
        Query::new("users"),
        None,
        ReadDurabilityOptions {
            tier: Some(DurabilityTier::Worker),
            local_updates: crate::query_manager::manager::LocalUpdates::Deferred,
        },
        crate::sync_manager::QueryPropagation::LocalOnly,
    );
    let global_rows = execute_runtime_query_with_durability_and_propagation(
        &mut core,
        Query::new("users"),
        None,
        ReadDurabilityOptions {
            tier: Some(DurabilityTier::GlobalServer),
            local_updates: crate::query_manager::manager::LocalUpdates::Deferred,
        },
        crate::sync_manager::QueryPropagation::LocalOnly,
    );

    assert_eq!(
        worker_rows,
        vec![(object_id, user_row_values(row_id, "Alice-worker"))]
    );
    assert_eq!(
        global_rows,
        vec![(object_id, user_row_values(row_id, "Alice-global"))]
    );
}

#[test]
fn rc_query_settled_before_data_should_not_drop_upstream_rows() {
    let mut s = create_3tier_rc();

    // Seed data on server B that client A has not synced yet.
    let (row_id, _row_values) =
        s.b.insert(
            "users",
            user_insert_values(ObjectId::new(), "upstream-row"),
            None,
        )
        .unwrap();
    s.b.immediate_tick();
    s.b.batched_tick();
    s.b.schema_manager_mut()
        .query_manager_mut()
        .sync_manager_mut()
        .take_outbox();

    // One-shot settled query on A should wait for Worker settlement.
    let mut future = s.a.query_with_propagation(
        Query::new("users"),
        None,
        ReadDurabilityOptions {
            tier: Some(DurabilityTier::Worker),
            local_updates: crate::query_manager::manager::LocalUpdates::Immediate,
        },
        crate::sync_manager::QueryPropagation::Full,
    );

    let waker = noop_waker();
    let mut cx = std::task::Context::from_waker(&waker);
    assert!(
        Pin::new(&mut future).poll(&mut cx).is_pending(),
        "Query should be pending before Worker settlement"
    );

    // Deliver A -> B query subscription and let B compute response traffic.
    pump_a_to_b(&mut s);
    s.b.batched_tick();
    let b_out =
        s.b.schema_manager_mut()
            .query_manager_mut()
            .sync_manager_mut()
            .take_outbox();

    // Force QuerySettled before row delivery to expose ordering assumptions.
    let mut settled_to_a = Vec::new();
    let mut rows_to_a = Vec::new();
    let mut row_state_to_a = Vec::new();
    for entry in b_out {
        if entry.destination != Destination::Client(s.a_client_of_b) {
            continue;
        }
        match entry.payload {
            payload @ SyncPayload::QuerySettled { .. } => settled_to_a.push(payload),
            payload @ SyncPayload::RowVersionNeeded { .. } => rows_to_a.push(payload),
            payload @ SyncPayload::RowVersionStateChanged { .. } => row_state_to_a.push(payload),
            _ => {}
        }
    }

    assert!(
        !settled_to_a.is_empty(),
        "Expected QuerySettled notification for A"
    );
    assert!(!rows_to_a.is_empty(), "Expected row payload for A");
    // Mirror connected stream initialization: first expected seq is 1.
    s.a.set_next_expected_server_sequence(s.b_server_for_a, 1);

    let mut next_update_seq = 1u64;
    let settled_seq_base = (rows_to_a.len() + row_state_to_a.len()) as u64 + 1;

    for (idx, payload) in settled_to_a.into_iter().enumerate() {
        s.a.park_sync_message_with_sequence(
            InboxEntry {
                source: Source::Server(s.b_server_for_a),
                payload,
            },
            settled_seq_base + idx as u64,
        );
    }
    s.a.batched_tick();
    s.a.immediate_tick();

    assert!(
        Pin::new(&mut future).poll(&mut cx).is_pending(),
        "Query should stay pending until lower sequence row payload arrives"
    );

    for payload in rows_to_a {
        s.a.park_sync_message_with_sequence(
            InboxEntry {
                source: Source::Server(s.b_server_for_a),
                payload,
            },
            next_update_seq,
        );
        next_update_seq += 1;
    }
    for payload in row_state_to_a {
        s.a.park_sync_message_with_sequence(
            InboxEntry {
                source: Source::Server(s.b_server_for_a),
                payload,
            },
            next_update_seq,
        );
        next_update_seq += 1;
    }
    s.a.batched_tick();
    s.a.immediate_tick();

    match Pin::new(&mut future).poll(&mut cx) {
        Poll::Ready(Ok(results)) => {
            assert_eq!(
                results.len(),
                1,
                "Sequenced delivery should prevent settled-before-data resolution"
            );
            assert_eq!(results[0].0, row_id);
        }
        Poll::Ready(Err(e)) => panic!("Query failed: {:?}", e),
        Poll::Pending => panic!("Query should resolve after row payload and QuerySettled"),
    }
}

#[test]
fn rc_subscribe_settled_tier() {
    let mut s = create_3tier_rc();

    let received = Arc::new(Mutex::new(Vec::<Vec<(ObjectId, Vec<Value>)>>::new()));
    let received_clone = received.clone();

    let _handle =
        s.a.subscribe_with_durability_and_propagation(
            Query::new("users"),
            move |delta| {
                let rows = decode_added_rows(&delta);
                received_clone.lock().unwrap().push(rows);
            },
            None,
            ReadDurabilityOptions {
                tier: Some(DurabilityTier::Worker),
                local_updates: crate::query_manager::manager::LocalUpdates::Deferred,
            },
            crate::sync_manager::QueryPropagation::Full,
        )
        .unwrap();

    let (id, _row_values) =
        s.a.insert("users", user_insert_values(ObjectId::new(), "Alice"), None)
            .unwrap();
    s.a.immediate_tick();

    assert!(
        received.lock().unwrap().is_empty(),
        "Callback should not fire before Worker settlement"
    );

    pump_a_to_b(&mut s);
    pump_b_to_a(&mut s);

    let calls = received.lock().unwrap();
    assert!(
        !calls.is_empty(),
        "Callback should fire after Worker QuerySettled"
    );
    let first_delivery = &calls[0];
    assert_eq!(first_delivery.len(), 1, "Should have one row");
    assert_eq!(first_delivery[0].0, id);
}

#[test]
fn rc_subscribe_remote_tier_immediate_local_updates() {
    let mut s = create_3tier_rc();

    let received = Arc::new(Mutex::new(Vec::<Vec<(ObjectId, Vec<Value>)>>::new()));
    let received_clone = received.clone();

    let _handle =
        s.a.subscribe_with_durability_and_propagation(
            Query::new("users"),
            move |delta| {
                let rows = decode_added_rows(&delta);
                received_clone.lock().unwrap().push(rows);
            },
            None,
            ReadDurabilityOptions {
                tier: Some(DurabilityTier::EdgeServer),
                local_updates: crate::query_manager::manager::LocalUpdates::Immediate,
            },
            crate::sync_manager::QueryPropagation::Full,
        )
        .unwrap();

    // Initial delivery should still wait for the initial remote frontier.
    let (first_id, _row_values) =
        s.a.insert(
            "users",
            user_insert_values(ObjectId::new(), "local-first"),
            None,
        )
        .unwrap();
    s.a.immediate_tick();

    let calls = received.lock().unwrap();
    assert!(
        calls.is_empty(),
        "Initial delivery should wait for query frontier completion"
    );
    drop(calls);

    // Worker frontier completion is enough to unblock the first snapshot. With
    // immediate local updates, the locally-authored row is shown immediately
    // while its EdgeServer durability is still pending.
    pump_a_to_b(&mut s);
    pump_b_to_a(&mut s);
    let calls = received.lock().unwrap();
    assert_eq!(
        calls.len(),
        1,
        "First callback should happen after frontier completion"
    );
    assert_eq!(
        calls[0].len(),
        1,
        "First callback should contain the local row"
    );
    assert_eq!(calls[0][0].0, first_id);
    drop(calls);

    // Reach EdgeServer settlement for the first row. This should not emit a
    // second visible delta because the row is already on screen via the local
    // pending overlay.
    pump_b_to_c(&mut s);
    pump_c_to_b_to_a(&mut s);

    let calls = received.lock().unwrap();
    assert_eq!(
        calls.len(),
        1,
        "Tier promotion should not emit a second visible delta for the same row"
    );
    drop(calls);

    // After initial delivery, local updates should callback immediately.
    let (second_id, _row_values) =
        s.a.insert(
            "users",
            user_insert_values(ObjectId::new(), "local-second"),
            None,
        )
        .unwrap();
    s.a.immediate_tick();

    let calls = received.lock().unwrap();
    assert_eq!(
        calls.len(),
        2,
        "Second local write should trigger immediate callback"
    );
    let second_delivery = &calls[1];
    assert_eq!(
        second_delivery.len(),
        1,
        "Second callback should contain one added row"
    );
    assert_eq!(second_delivery[0].0, second_id);
}

fn noop_waker() -> std::task::Waker {
    fn noop(_: *const ()) {}
    fn clone(_: *const ()) -> std::task::RawWaker {
        std::task::RawWaker::new(std::ptr::null(), &VTABLE)
    }
    static VTABLE: std::task::RawWakerVTable =
        std::task::RawWakerVTable::new(clone, noop, noop, noop);
    unsafe { std::task::Waker::from_raw(std::task::RawWaker::new(std::ptr::null(), &VTABLE)) }
}

#[test]
fn test_sync_edit_fires_callback_synchronously() {
    let mut core = create_test_runtime();

    let callback_count = Arc::new(Mutex::new(0usize));
    let count_clone = callback_count.clone();

    let query = Query::new("users");
    let _handle = core
        .subscribe(
            query,
            move |delta| {
                if !delta.ordered_delta.added.is_empty() {
                    *count_clone.lock().unwrap() += 1;
                }
            },
            None,
        )
        .unwrap();

    core.immediate_tick();
    let initial_count = *callback_count.lock().unwrap();

    let _ = core.insert(
        "users",
        user_insert_values(ObjectId::new(), "test@test.com"),
        None,
    );
    core.immediate_tick();

    let final_count = *callback_count.lock().unwrap();
    assert!(
        final_count > initial_count,
        "Callback must fire synchronously after insert when index ready"
    );
}

#[test]
fn rc_query_reads_old_schema_row_after_evolving_to_new_schema() {
    let v1 = schema_evolution_v1();
    let v2 = schema_evolution_v2();

    let mut old_runtime = create_runtime_with_schema(v1.clone(), "schema-evolution-test");
    let user_id = ObjectId::new();
    let inserted_values = HashMap::from([
        ("id".to_string(), Value::Uuid(user_id)),
        ("name".to_string(), Value::Text("Alice".to_string())),
    ]);
    let (inserted_id, _) = old_runtime.insert("users", inserted_values, None).unwrap();

    let storage = old_runtime.into_storage();

    let mut evolved_runtime = create_runtime_with_storage(v2, "schema-evolution-test", storage);
    evolved_runtime
        .add_live_schema_and_persist_catalogue(v1)
        .expect("v1 should be attachable as a live schema for v2");
    evolved_runtime.immediate_tick();

    let results = execute_runtime_query(&mut evolved_runtime, Query::new("users"), None);

    assert_eq!(
        results.len(),
        1,
        "Expected one row visible after schema evolution"
    );

    let (queried_id, queried_values) = &results[0];
    let current_schema = evolved_runtime.current_schema();
    let id_idx = column_index(current_schema, "users", "id");
    let name_idx = column_index(current_schema, "users", "name");
    let email_idx = column_index(current_schema, "users", "email");
    assert_eq!(*queried_id, inserted_id);
    assert_eq!(queried_values.len(), 3, "Row should decode in v2 shape");
    assert_eq!(queried_values[id_idx], Value::Uuid(user_id));
    assert_eq!(queried_values[name_idx], Value::Text("Alice".to_string()));
    assert_eq!(
        queried_values[email_idx],
        Value::Text(String::new()),
        "New required column should be backfilled with the lens default",
    );
}

#[test]
fn rc_update_old_schema_row_after_evolution_copies_row_to_current_schema() {
    let v1 = schema_evolution_v1();
    let v2 = schema_evolution_v2();

    let mut old_runtime = create_runtime_with_schema(v1.clone(), "schema-evolution-update-test");
    let user_id = ObjectId::new();
    let inserted_values = HashMap::from([
        ("id".to_string(), Value::Uuid(user_id)),
        ("name".to_string(), Value::Text("Alice".to_string())),
    ]);
    let (inserted_id, _) = old_runtime.insert("users", inserted_values, None).unwrap();

    let storage = old_runtime.into_storage();

    let mut evolved_runtime =
        create_runtime_with_storage(v2.clone(), "schema-evolution-update-test", storage);
    evolved_runtime
        .add_live_schema_and_persist_catalogue(v1.clone())
        .expect("v1 should be attachable as a live schema for v2");
    evolved_runtime.immediate_tick();

    evolved_runtime
        .update(
            inserted_id,
            vec![
                ("name".to_string(), Value::Text("Alice Updated".to_string())),
                (
                    "email".to_string(),
                    Value::Text("alice.updated@example.com".to_string()),
                ),
            ],
            None,
        )
        .expect("Updating an old-schema row should succeed via copy-on-write");

    let results = execute_runtime_query(&mut evolved_runtime, Query::new("users"), None);
    assert_eq!(
        results.len(),
        1,
        "Copy-on-write should preserve a single logical row"
    );

    let (queried_id, queried_values) = &results[0];
    let current_schema = evolved_runtime.current_schema();
    let id_idx = column_index(current_schema, "users", "id");
    let name_idx = column_index(current_schema, "users", "name");
    let email_idx = column_index(current_schema, "users", "email");
    assert_eq!(*queried_id, inserted_id);
    assert_eq!(
        queried_values.len(),
        3,
        "Updated row should decode in v2 shape"
    );
    assert_eq!(queried_values[id_idx], Value::Uuid(user_id));
    assert_eq!(
        queried_values[name_idx],
        Value::Text("Alice Updated".to_string())
    );
    assert_eq!(
        queried_values[email_idx],
        Value::Text("alice.updated@example.com".to_string()),
    );
}

#[test]
fn rc_delete_old_schema_row_after_evolution_hides_row_from_queries() {
    let v1 = schema_evolution_v1();
    let v2 = schema_evolution_v2();

    let mut old_runtime = create_runtime_with_schema(v1.clone(), "schema-evolution-delete-test");
    let user_id = ObjectId::new();
    let inserted_values = HashMap::from([
        ("id".to_string(), Value::Uuid(user_id)),
        ("name".to_string(), Value::Text("Alice".to_string())),
    ]);
    let (inserted_id, _) = old_runtime.insert("users", inserted_values, None).unwrap();

    let storage = old_runtime.into_storage();

    let mut evolved_runtime =
        create_runtime_with_storage(v2.clone(), "schema-evolution-delete-test", storage);
    evolved_runtime
        .add_live_schema_and_persist_catalogue(v1.clone())
        .expect("v1 should be attachable as a live schema for v2");
    evolved_runtime.immediate_tick();

    evolved_runtime
        .delete(inserted_id, None)
        .expect("Deleting an old-schema row should succeed after schema evolution");

    let results = execute_runtime_query(&mut evolved_runtime, Query::new("users"), None);
    assert_eq!(
        results.len(),
        0,
        "Deleted old-schema row should no longer be visible after schema evolution",
    );
}

/// FIXME: this is an undesired behavior. See `/todo/ideas/1_mvp/lens-hardening.md`
#[test]
fn rc_old_client_update_removes_unseen_newer_fields() {
    let v1 = schema_evolution_v1();
    let v2 = schema_evolution_v2();

    // Flow:
    // v2 client writes row with email on the v2 branch
    // v1 client reads that row through v2 -> v1 lens and updates only name
    // v2 client does not see the original email after the v1-originated update
    let mut new_runtime =
        create_runtime_with_schema(v2.clone(), "schema-evolution-backward-update-test");
    let user_id = ObjectId::new();
    let inserted_values = HashMap::from([
        ("id".to_string(), Value::Uuid(user_id)),
        ("name".to_string(), Value::Text("Alice".to_string())),
        (
            "email".to_string(),
            Value::Text("alice@example.com".to_string()),
        ),
    ]);
    let (inserted_id, _) = new_runtime.insert("users", inserted_values, None).unwrap();

    let storage = new_runtime.into_storage();

    let mut old_runtime =
        create_runtime_with_storage(v1.clone(), "schema-evolution-backward-update-test", storage);
    old_runtime
        .add_live_schema_and_persist_catalogue(v2.clone())
        .expect("v2 should be attachable as a live schema for v1");
    old_runtime.immediate_tick();

    old_runtime
        .update(
            inserted_id,
            vec![(
                "name".to_string(),
                Value::Text("Alice Updated From v1".to_string()),
            )],
            None,
        )
        .expect("Updating a newer-schema row from an old client should succeed");

    let history_bytes = old_runtime
        .storage()
        .scan_history_region_bytes(
            "users",
            crate::row_histories::HistoryScan::Row {
                row_id: inserted_id,
            },
        )
        .expect("history bytes should be readable after old-client update");
    assert!(
        history_bytes
            .iter()
            .all(|bytes| crate::row_histories::is_flat_history_row(bytes)),
        "all row-history versions should be stored as flat physical rows once their schemas are in catalogue"
    );

    let storage = old_runtime.into_storage();

    let mut reloaded_v2 =
        create_runtime_with_storage(v2.clone(), "schema-evolution-backward-update-test", storage);
    reloaded_v2
        .add_live_schema_and_persist_catalogue(v1.clone())
        .expect("v1 should be attachable as a live schema for v2");
    reloaded_v2.immediate_tick();

    let results = execute_runtime_query(&mut reloaded_v2, Query::new("users"), None);
    assert_eq!(
        results.len(),
        1,
        "Old-client update should still leave one logical row visible"
    );

    let (queried_id, queried_values) = &results[0];
    let current_schema = reloaded_v2.current_schema();
    let id_idx = column_index(current_schema, "users", "id");
    let name_idx = column_index(current_schema, "users", "name");
    let email_idx = column_index(current_schema, "users", "email");

    assert_eq!(*queried_id, inserted_id);
    assert_eq!(queried_values[id_idx], Value::Uuid(user_id));
    assert_eq!(
        queried_values[name_idx],
        Value::Text("Alice Updated From v1".to_string()),
    );
    assert_eq!(
        queried_values[email_idx],
        Value::Text("".to_string()),
        "Old-client updates remove unseen new-schema fields",
    );
}

#[test]
fn runtime_bootstraps_current_schema_into_catalogue_for_flat_row_history() {
    let schema = schema_evolution_v1();
    let schema_hash = SchemaHash::compute(&schema);
    let mut core = create_runtime_with_schema(schema.clone(), "flat-row-history-bootstrap");

    let schema_entry = core
        .storage()
        .load_catalogue_entry(schema_hash.to_object_id())
        .expect("catalogue lookup should succeed");
    assert!(
        schema_entry.is_some(),
        "runtime startup should persist the current schema into catalogue storage"
    );

    let row_id = ObjectId::new();
    let (inserted_id, _) = core
        .insert("users", user_insert_values(row_id, "Alice"), None)
        .expect("insert should succeed");

    let history_bytes = core
        .storage()
        .scan_history_region_bytes(
            "users",
            crate::row_histories::HistoryScan::Row {
                row_id: inserted_id,
            },
        )
        .expect("history bytes should be readable after insert");
    assert_eq!(history_bytes.len(), 1);
    assert!(
        crate::row_histories::is_flat_history_row(&history_bytes[0]),
        "current-schema inserts should write flat row-history bytes without manual schema seeding"
    );

    let user_descriptor = schema
        .get(&TableName::new("users"))
        .expect("users table should exist")
        .columns
        .clone();
    let decoded =
        crate::row_histories::decode_flat_history_row(&user_descriptor, &history_bytes[0])
            .expect("flat history row should decode with the catalogue-backed descriptor");
    assert_eq!(decoded.row_id, inserted_id);
    assert_eq!(decoded.data.len() > 0, true);
}

#[test]
fn test_persist_schema_then_add_server_sends_catalogue() {
    // Mirror the WASM flow EXACTLY: NO immediate_tick before persist_schema
    let schema = test_schema();
    let app_id = AppId::from_name("test-app");
    let sync_manager = SyncManager::new();
    let schema_manager = SchemaManager::new(sync_manager, schema, app_id, "dev", "main").unwrap();
    let mut core = RuntimeCore::new(schema_manager, MemoryStorage::new(), NoopScheduler);
    // NO immediate_tick() here — matches WASM openPersistent flow

    // persist_schema — stages a catalogue object before the first tick
    let schema_obj_id = core.persist_schema();

    // add_server — should call queue_full_sync_to_server which includes the catalogue
    let server_id = ServerId::new();
    core.add_server(server_id);

    // batched_tick — should flush catalogue to outbox → sync sender
    core.batched_tick();

    // Check that the catalogue was sent
    let messages = core
        .schema_manager_mut()
        .query_manager_mut()
        .sync_manager_mut()
        .take_outbox();
    let catalogue_msg = messages.iter().find(|m| {
        if let SyncPayload::CatalogueEntryUpdated { entry } = &m.payload {
            entry.object_id == schema_obj_id
                && entry
                    .metadata
                    .get(crate::metadata::MetadataKey::Type.as_str())
                    .map(|t| t == crate::metadata::ObjectType::CatalogueSchema.as_str())
                    .unwrap_or(false)
        } else {
            false
        }
    });
    let permissions_msg = messages.iter().find(|m| {
        if let SyncPayload::CatalogueEntryUpdated { entry } = &m.payload {
            entry
                .metadata
                .get(crate::metadata::MetadataKey::Type.as_str())
                .map(|t| {
                    t == crate::metadata::ObjectType::CataloguePermissions.as_str()
                        || t == crate::metadata::ObjectType::CataloguePermissionsBundle.as_str()
                        || t == crate::metadata::ObjectType::CataloguePermissionsHead.as_str()
                })
                .unwrap_or(false)
        } else {
            false
        }
    });

    assert!(
        catalogue_msg.is_some(),
        "Catalogue schema object should be in outbox after add_server + batched_tick. \
             Messages found: {}",
        messages
            .iter()
            .map(|m| format!("{:?}", m.payload))
            .collect::<Vec<_>>()
            .join(", ")
    );
    assert!(
        permissions_msg.is_none(),
        "persist_schema should not implicitly publish permissions catalogue objects"
    );
}

#[test]
fn test_publish_permissions_bundle_then_add_server_sends_head_and_bundle() {
    let schema = test_schema();
    let app_id = AppId::from_name("test-app");
    let schema_hash = SchemaHash::compute(&schema);
    let sync_manager = SyncManager::new();
    let schema_manager = SchemaManager::new(sync_manager, schema, app_id, "dev", "main").unwrap();
    let mut core = RuntimeCore::new(schema_manager, MemoryStorage::new(), NoopScheduler);

    core.persist_schema();
    core.publish_permissions_bundle(
        schema_hash,
        std::collections::HashMap::from([(
            TableName::new("users"),
            TablePolicies::new().with_select(PolicyExpr::True),
        )]),
        None,
    )
    .expect("publish permissions bundle");

    let server_id = ServerId::new();
    core.add_server(server_id);
    core.batched_tick();

    let messages = core
        .schema_manager_mut()
        .query_manager_mut()
        .sync_manager_mut()
        .take_outbox();
    let bundle_msg = messages.iter().find(|m| {
        if let SyncPayload::CatalogueEntryUpdated { entry } = &m.payload {
            entry
                .metadata
                .get(crate::metadata::MetadataKey::Type.as_str())
                .map(|t| t == crate::metadata::ObjectType::CataloguePermissionsBundle.as_str())
                .unwrap_or(false)
        } else {
            false
        }
    });
    let head_msg = messages.iter().find(|m| {
        if let SyncPayload::CatalogueEntryUpdated { entry } = &m.payload {
            entry
                .metadata
                .get(crate::metadata::MetadataKey::Type.as_str())
                .map(|t| t == crate::metadata::ObjectType::CataloguePermissionsHead.as_str())
                .unwrap_or(false)
        } else {
            false
        }
    });

    assert!(
        bundle_msg.is_some(),
        "Explicit permission publication should sync the immutable permissions bundle object"
    );
    assert!(
        head_msg.is_some(),
        "Explicit permission publication should sync the mutable permissions head object"
    );
}

#[test]
fn test_matching_catalogue_hash_skips_catalogue_replay_on_add_server() {
    let schema = test_schema();
    let app_id = AppId::from_name("test-app");
    let sync_manager = SyncManager::new();
    let schema_manager = SchemaManager::new(sync_manager, schema, app_id, "dev", "main").unwrap();
    let mut core = RuntimeCore::new(schema_manager, MemoryStorage::new(), NoopScheduler);

    let schema_obj_id = core.persist_schema();
    let (row_object_id, _) = core
        .insert("users", user_insert_values(ObjectId::new(), "Alice"), None)
        .unwrap();

    let catalogue_state_hash = core.schema_manager().catalogue_state_hash();

    let server_id = ServerId::new();
    core.add_server_with_catalogue_state_hash(server_id, Some(&catalogue_state_hash));
    core.batched_tick();

    let messages = core
        .schema_manager_mut()
        .query_manager_mut()
        .sync_manager_mut()
        .take_outbox();
    let catalogue_msg = messages.iter().find(|m| {
        matches!(
            &m.payload,
            SyncPayload::CatalogueEntryUpdated { entry } if entry.object_id == schema_obj_id
        )
    });
    let row_msg = messages.iter().find(|m| match &m.payload {
        SyncPayload::RowVersionCreated { row, .. } => row.row_id == row_object_id,
        _ => false,
    });

    assert!(
        catalogue_msg.is_none(),
        "Catalogue replay should be skipped when hashes already match"
    );
    assert!(
        row_msg.is_some(),
        "Regular row objects should still be sent during the full sync walk"
    );
}
// =========================================================================
// Foreign Key — No Write-Time Validation
// =========================================================================
//
// FK write-time existence checks are intentionally removed: in a local-first
// system with query-scoped sync, the referenced row may not be loaded yet,
// causing false violations. True referential integrity will be enforced by
// global transactions (specs/todo/b_launch/globally_consistent_transactions.md).
//
// These tests document that FK-referencing writes succeed even when the
// target row is absent from the local index. They double as scaffolding for
// when global transactions re-introduce server-side FK checks.

/// Schema that mirrors the stress-test app: projects + todos with FK.
fn fk_stress_schema() -> Schema {
    SchemaBuilder::new()
        .table(
            TableSchema::builder("projects")
                .column("name", ColumnType::Text)
                .column("owner_id", ColumnType::Text),
        )
        .table(
            TableSchema::builder("todos")
                .column("title", ColumnType::Text)
                .column("done", ColumnType::Boolean)
                .nullable_column("description", ColumnType::Text)
                .column("owner_id", ColumnType::Text)
                .nullable_fk_column("project", "projects"),
        )
        .build()
}

fn create_fk_runtime() -> TestCore {
    let schema = fk_stress_schema();
    let app_id = AppId::from_name("fk-test");
    let sync_manager = SyncManager::new();
    let schema_manager = SchemaManager::new(sync_manager, schema, app_id, "dev", "main").unwrap();
    let mut core = RuntimeCore::new(schema_manager, MemoryStorage::new(), NoopScheduler);
    core.immediate_tick();
    core
}

/// After query-scoped sync, a todo's `project` FK can reference a project
/// that was never loaded into MemoryStorage. A partial update (toggling
/// `done`) must succeed — no FK re-check.
///
/// ```text
///   MemoryStorage (after query-scoped sync)
///   ┌────────────────────────────────────────┐
///   │ projects._id index:  []     ← empty!   │
///   │ todos._id index:     [todo_1]           │
///   │                                         │
///   │ todo_1.project = project_42  → not in   │
///   │                               index     │
///   └────────────────────────────────────────┘
///
///   User toggles todo_1.done → partial update → OK (no FK check)
/// ```
#[test]
fn rc_partial_update_with_unloaded_fk_reference() {
    let mut core = create_fk_runtime();

    let (project_id, _) = core
        .insert("projects", project_insert_values("Acme", "alice"), None)
        .unwrap();

    let (todo_id, _) = core
        .insert(
            "todos",
            todo_insert_values(
                "Buy milk",
                true,
                Value::Null,
                "alice",
                Value::Uuid(project_id),
            ),
            None,
        )
        .unwrap();

    core.immediate_tick();

    // Simulate query-scoped sync: remove the project from the _id index.
    let branch = core.schema_manager().branch_name();
    core.storage
        .index_remove(
            "projects",
            "_id",
            branch.as_str(),
            &Value::Uuid(project_id),
            project_id,
        )
        .unwrap();

    // Partial update: only change `done`.
    // No FK validation → succeeds even though project is not in the index.
    core.update(
        todo_id,
        vec![("done".to_string(), Value::Boolean(false))],
        None,
    )
    .expect("partial update must succeed even when referenced project is not loaded");
}

/// Changing a FK column to a non-existent target is allowed at the local
/// write level (no FK existence check). Global transactions will enforce
/// this server-side in the future.
#[test]
fn rc_partial_update_changing_fk_to_missing_target_succeeds() {
    let mut core = create_fk_runtime();

    let (project_id, _) = core
        .insert("projects", project_insert_values("Acme", "alice"), None)
        .unwrap();

    let (todo_id, _) = core
        .insert(
            "todos",
            todo_insert_values(
                "Buy milk",
                true,
                Value::Null,
                "alice",
                Value::Uuid(project_id),
            ),
            None,
        )
        .unwrap();

    core.immediate_tick();

    // Change the FK column to a non-existent project.
    // Without global transactions this is accepted locally.
    let bogus_project = ObjectId::new();
    core.update(
        todo_id,
        vec![("project".to_string(), Value::Uuid(bogus_project))],
        None,
    )
    .expect("changing FK to non-existent target must succeed without local FK checks");
}

// =========================================================================
// Disconnect cleanup: parked message guard
// =========================================================================

#[test]
fn remove_client_blocked_by_parked_sync_messages() {
    //
    // alice ──/sync──▶ server (message parked in RuntimeCore, not yet in SyncManager inbox)
    //
    // Sweep tries to reap alice → remove_client returns false because
    // parked_sync_messages contains an entry from alice.
    //
    use crate::metadata::RowProvenance;

    let mut core = create_test_runtime();
    let alice = ClientId::new();
    core.add_client(alice, None);

    // Park a message from alice (simulates push_sync_inbox before batched_tick)
    core.park_sync_message(InboxEntry {
        source: Source::Client(alice),
        payload: SyncPayload::RowVersionCreated {
            metadata: None,
            row: crate::row_histories::StoredRowVersion::new(
                ObjectId::new(),
                "main",
                Vec::new(),
                b"alice".to_vec(),
                RowProvenance::for_insert(ObjectId::new().to_string(), 1_000),
                HashMap::new(),
                crate::row_histories::RowState::VisibleDirect,
                None,
            ),
        },
    });

    let removed = core.remove_client(alice);
    assert!(!removed, "should refuse to reap with parked messages");

    // Client state must be preserved
    assert!(
        core.schema_manager()
            .query_manager()
            .sync_manager()
            .get_client(alice)
            .is_some(),
        "alice's ClientState should be preserved"
    );
}

#[test]
fn remove_client_succeeds_after_parked_messages_drained() {
    //
    // alice ──/sync──▶ server (message parked) ──batched_tick──▶ inbox drained
    //
    // After batched_tick processes the parked message, remove_client succeeds.
    //
    use crate::metadata::RowProvenance;

    let mut core = create_test_runtime();
    let alice = ClientId::new();
    core.add_client(alice, None);

    core.park_sync_message(InboxEntry {
        source: Source::Client(alice),
        payload: SyncPayload::RowVersionCreated {
            metadata: None,
            row: crate::row_histories::StoredRowVersion::new(
                ObjectId::new(),
                "main",
                Vec::new(),
                b"alice".to_vec(),
                RowProvenance::for_insert(ObjectId::new().to_string(), 1_000),
                HashMap::new(),
                crate::row_histories::RowState::VisibleDirect,
                None,
            ),
        },
    });

    // Drain parked messages via batched_tick
    core.batched_tick();

    let removed = core.remove_client(alice);
    assert!(removed, "should succeed after parked messages are drained");

    assert!(
        core.schema_manager()
            .query_manager()
            .sync_manager()
            .get_client(alice)
            .is_none(),
        "alice should be removed"
    );
}

#[test]
fn remove_client_ignores_parked_messages_from_other_clients() {
    //
    // bob ──/sync──▶ server (message parked)
    //
    // alice disconnects → remove_client(alice) succeeds because
    // the parked message is from bob, not alice.
    //
    use crate::metadata::RowProvenance;

    let mut core = create_test_runtime();
    let alice = ClientId::new();
    let bob = ClientId::new();
    core.add_client(alice, None);
    core.add_client(bob, None);

    // Park a message from bob
    core.park_sync_message(InboxEntry {
        source: Source::Client(bob),
        payload: SyncPayload::RowVersionCreated {
            metadata: None,
            row: crate::row_histories::StoredRowVersion::new(
                ObjectId::new(),
                "main",
                Vec::new(),
                b"bob".to_vec(),
                RowProvenance::for_insert(ObjectId::new().to_string(), 1_000),
                HashMap::new(),
                crate::row_histories::RowState::VisibleDirect,
                None,
            ),
        },
    });

    let removed = core.remove_client(alice);
    assert!(removed, "alice has no parked messages — should succeed");

    assert!(
        core.schema_manager()
            .query_manager()
            .sync_manager()
            .get_client(alice)
            .is_none(),
        "alice should be removed"
    );
    assert!(
        core.schema_manager()
            .query_manager()
            .sync_manager()
            .get_client(bob)
            .is_some(),
        "bob should be preserved"
    );
}

#[cfg(feature = "transport-websocket")]
#[test]
fn auth_failure_callback_fires_on_inbound_auth_failure_event() {
    use crate::transport_manager::TransportInbound;
    use std::sync::{Arc, Mutex};

    let mut core = create_test_runtime();
    let captured: Arc<Mutex<Vec<String>>> = Arc::new(Mutex::new(Vec::new()));
    let captured_clone = Arc::clone(&captured);
    core.set_auth_failure_callback(move |reason| {
        captured_clone.lock().unwrap().push(reason);
    });

    let dummy_server_id = ServerId::new();
    core.handle_transport_inbound_for_test(
        dummy_server_id,
        TransportInbound::AuthFailure {
            reason: "Unauthorized".to_string(),
        },
    );

    assert_eq!(
        captured.lock().unwrap().as_slice(),
        &["Unauthorized".to_string()]
    );
}
