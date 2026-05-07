use super::*;
use crate::batch_fate::{CapturedFrontierMember, SealedBatchMember, SealedBatchSubmission};
use crate::query_manager::policy::PolicyExpr;
use crate::query_manager::query::QueryBuilder;
use crate::query_manager::session::WriteContext;
use crate::query_manager::types::{
    ColumnType, SchemaBuilder, SchemaHash, TableName, TablePolicies, TableSchema,
};
use crate::row_format::encode_row;
use crate::row_histories::BatchId;
use crate::schema_manager::AppId;
use crate::storage::{
    MemoryStorage, RawTableKeys, RawTableRows, RowLocator, Storage, StorageError,
};
use crate::sync_manager::{
    ClientId, ClientRole, Destination, DurabilityTier, InboxEntry, OutboxEntry, ServerId, Source,
    SyncError, SyncManager, SyncPayload,
};
use std::collections::HashMap;
use std::sync::{Arc, Mutex};

type TestCore = RuntimeCore<MemoryStorage, NoopScheduler>;
type BoxedStorageTestCore = RuntimeCore<Box<dyn Storage>, NoopScheduler>;

fn new_test_core<S: Storage, Sch: Scheduler>(
    schema_manager: SchemaManager,
    storage: S,
    scheduler: Sch,
) -> RuntimeCore<S, Sch> {
    let mut core = RuntimeCore::new(schema_manager, storage, scheduler);
    core.set_sync_sender(Box::new(VecSyncSender::new()));
    core
}

struct RowRegionReadFailingStorage {
    inner: MemoryStorage,
    fail_visible_row_reads: bool,
    fail_row_locator_scans: bool,
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
    local_batch_record_get_calls: usize,
}

struct RowMutationObservingStorage {
    inner: MemoryStorage,
    calls: Arc<Mutex<RowMutationCallCounts>>,
}

#[derive(Clone, Default)]
struct CountingScheduler {
    schedule_calls: Arc<Mutex<usize>>,
}

impl RowRegionReadFailingStorage {
    fn new() -> Self {
        Self {
            inner: MemoryStorage::new(),
            fail_visible_row_reads: true,
            fail_row_locator_scans: false,
        }
    }

    fn with_row_locator_scan_failure() -> Self {
        Self {
            inner: MemoryStorage::new(),
            fail_visible_row_reads: false,
            fail_row_locator_scans: true,
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

impl CountingScheduler {
    fn schedule_count(&self) -> usize {
        *self.schedule_calls.lock().unwrap()
    }
}

impl Scheduler for CountingScheduler {
    fn schedule_batched_tick(&self) {
        *self.schedule_calls.lock().unwrap() += 1;
    }
}

impl Storage for RowRegionReadFailingStorage {
    fn apply_encoded_row_mutation(
        &mut self,
        table: &str,
        history_rows: &[crate::storage::OwnedHistoryRowBytes],
        visible_rows: &[crate::storage::OwnedVisibleRowBytes],
        index_mutations: &[crate::storage::IndexMutation<'_>],
    ) -> Result<(), StorageError> {
        self.inner
            .apply_encoded_row_mutation(table, history_rows, visible_rows, index_mutations)
    }

    fn apply_prepared_row_mutation(
        &mut self,
        table: &str,
        history_rows: &[crate::row_histories::StoredRowBatch],
        visible_entries: &[crate::row_histories::VisibleRowEntry],
        encoded_history_rows: &[crate::storage::OwnedHistoryRowBytes],
        encoded_visible_rows: &[crate::storage::OwnedVisibleRowBytes],
        index_mutations: &[crate::storage::IndexMutation<'_>],
    ) -> Result<(), StorageError> {
        self.inner.apply_prepared_row_mutation(
            table,
            history_rows,
            visible_entries,
            encoded_history_rows,
            encoded_visible_rows,
            index_mutations,
        )
    }

    fn scan_row_locators(&self) -> Result<crate::storage::RowLocatorRows, StorageError> {
        if self.fail_row_locator_scans {
            return Err(StorageError::IoError(
                "row-locator scans deliberately disabled in this test".to_string(),
            ));
        }
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
        rows: &[crate::row_histories::StoredRowBatch],
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

    fn delete_visible_region_row(
        &mut self,
        table: &str,
        branch: &str,
        row_id: ObjectId,
    ) -> Result<(), StorageError> {
        self.inner.delete_visible_region_row(table, branch, row_id)
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

    fn patch_exact_row_batch(
        &mut self,
        table: &str,
        branch: &str,
        row_id: ObjectId,
        batch_id: crate::row_histories::BatchId,
        state: Option<crate::row_histories::RowState>,
        confirmed_tier: Option<DurabilityTier>,
    ) -> Result<bool, StorageError> {
        self.inner
            .patch_exact_row_batch(table, branch, row_id, batch_id, state, confirmed_tier)
    }

    fn patch_exact_row_batch_for_schema_hash(
        &mut self,
        table: &str,
        schema_hash: crate::query_manager::types::SchemaHash,
        branch: &str,
        row_id: ObjectId,
        batch_id: crate::row_histories::BatchId,
        state: Option<crate::row_histories::RowState>,
        confirmed_tier: Option<DurabilityTier>,
    ) -> Result<bool, StorageError> {
        self.inner.patch_exact_row_batch_for_schema_hash(
            table,
            schema_hash,
            branch,
            row_id,
            batch_id,
            state,
            confirmed_tier,
        )
    }

    fn scan_visible_region(
        &self,
        table: &str,
        branch: &str,
    ) -> Result<Vec<crate::row_histories::StoredRowBatch>, StorageError> {
        self.inner.scan_visible_region(table, branch)
    }

    fn load_visible_region_row(
        &self,
        table: &str,
        branch: &str,
        row_id: ObjectId,
    ) -> Result<Option<crate::row_histories::StoredRowBatch>, StorageError> {
        if self.fail_visible_row_reads {
            return Err(StorageError::IoError(
                "row-history reads deliberately disabled in this test".to_string(),
            ));
        }
        self.inner.load_visible_region_row(table, branch, row_id)
    }

    fn load_visible_region_frontier(
        &self,
        table: &str,
        branch: &str,
        row_id: ObjectId,
    ) -> Result<Option<Vec<crate::row_histories::BatchId>>, StorageError> {
        self.inner
            .load_visible_region_frontier(table, branch, row_id)
    }

    fn capture_family_visible_frontier(
        &self,
        target_branch_name: crate::object::BranchName,
    ) -> Result<Vec<crate::batch_fate::CapturedFrontierMember>, StorageError> {
        self.inner
            .capture_family_visible_frontier(target_branch_name)
    }

    fn scan_visible_region_row_batches(
        &self,
        table: &str,
        row_id: ObjectId,
    ) -> Result<Vec<crate::row_histories::StoredRowBatch>, StorageError> {
        self.inner.scan_visible_region_row_batches(table, row_id)
    }

    fn scan_history_row_batches(
        &self,
        table: &str,
        row_id: ObjectId,
    ) -> Result<Vec<crate::row_histories::StoredRowBatch>, StorageError> {
        self.inner.scan_history_row_batches(table, row_id)
    }

    fn load_history_row_batch(
        &self,
        table: &str,
        branch: &str,
        row_id: ObjectId,
        batch_id: crate::row_histories::BatchId,
    ) -> Result<Option<crate::row_histories::StoredRowBatch>, StorageError> {
        self.inner
            .load_history_row_batch(table, branch, row_id, batch_id)
    }

    fn load_history_query_row_batch(
        &self,
        table: &str,
        branch: &str,
        row_id: ObjectId,
        batch_id: crate::row_histories::BatchId,
    ) -> Result<Option<crate::row_histories::QueryRowBatch>, StorageError> {
        self.inner
            .load_history_query_row_batch(table, branch, row_id, batch_id)
    }

    fn load_history_row_batch_for_schema_hash(
        &self,
        table: &str,
        schema_hash: crate::query_manager::types::SchemaHash,
        branch: &str,
        row_id: ObjectId,
        batch_id: crate::row_histories::BatchId,
    ) -> Result<Option<crate::row_histories::StoredRowBatch>, StorageError> {
        self.inner.load_history_row_batch_for_schema_hash(
            table,
            schema_hash,
            branch,
            row_id,
            batch_id,
        )
    }

    fn load_history_row_batch_any_branch(
        &self,
        table: &str,
        row_id: ObjectId,
        batch_id: crate::row_histories::BatchId,
    ) -> Result<Option<crate::row_histories::StoredRowBatch>, StorageError> {
        self.inner
            .load_history_row_batch_any_branch(table, row_id, batch_id)
    }

    fn load_history_query_row_batch_any_branch(
        &self,
        table: &str,
        row_id: ObjectId,
        batch_id: crate::row_histories::BatchId,
    ) -> Result<Option<crate::row_histories::QueryRowBatch>, StorageError> {
        self.inner
            .load_history_query_row_batch_any_branch(table, row_id, batch_id)
    }

    fn row_batch_exists(
        &self,
        table: &str,
        branch: &str,
        row_id: ObjectId,
        batch_id: crate::row_histories::BatchId,
    ) -> Result<bool, StorageError> {
        self.inner.row_batch_exists(table, branch, row_id, batch_id)
    }

    fn scan_row_branch_tip_ids(
        &self,
        table: &str,
        branch: &str,
        row_id: ObjectId,
    ) -> Result<Vec<crate::row_histories::BatchId>, StorageError> {
        self.inner.scan_row_branch_tip_ids(table, branch, row_id)
    }

    fn load_history_row_batch_bytes(
        &self,
        table: &str,
        branch: &str,
        row_id: ObjectId,
        batch_id: crate::row_histories::BatchId,
    ) -> Result<Option<Vec<u8>>, StorageError> {
        self.inner
            .load_history_row_batch_bytes(table, branch, row_id, batch_id)
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
    ) -> Result<Vec<crate::row_histories::StoredRowBatch>, StorageError> {
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
    fn apply_encoded_row_mutation(
        &mut self,
        table: &str,
        history_rows: &[crate::storage::OwnedHistoryRowBytes],
        visible_rows: &[crate::storage::OwnedVisibleRowBytes],
        index_mutations: &[crate::storage::IndexMutation<'_>],
    ) -> Result<(), StorageError> {
        self.inner
            .apply_encoded_row_mutation(table, history_rows, visible_rows, index_mutations)
    }

    fn apply_prepared_row_mutation(
        &mut self,
        table: &str,
        history_rows: &[crate::row_histories::StoredRowBatch],
        visible_entries: &[crate::row_histories::VisibleRowEntry],
        encoded_history_rows: &[crate::storage::OwnedHistoryRowBytes],
        encoded_visible_rows: &[crate::storage::OwnedVisibleRowBytes],
        index_mutations: &[crate::storage::IndexMutation<'_>],
    ) -> Result<(), StorageError> {
        self.inner.apply_prepared_row_mutation(
            table,
            history_rows,
            visible_entries,
            encoded_history_rows,
            encoded_visible_rows,
            index_mutations,
        )
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
        rows: &[crate::row_histories::StoredRowBatch],
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

    fn delete_visible_region_row(
        &mut self,
        table: &str,
        branch: &str,
        row_id: ObjectId,
    ) -> Result<(), StorageError> {
        self.inner.delete_visible_region_row(table, branch, row_id)
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

    fn patch_exact_row_batch(
        &mut self,
        table: &str,
        branch: &str,
        row_id: ObjectId,
        batch_id: crate::row_histories::BatchId,
        state: Option<crate::row_histories::RowState>,
        confirmed_tier: Option<DurabilityTier>,
    ) -> Result<bool, StorageError> {
        self.inner
            .patch_exact_row_batch(table, branch, row_id, batch_id, state, confirmed_tier)
    }

    fn patch_exact_row_batch_for_schema_hash(
        &mut self,
        table: &str,
        schema_hash: crate::query_manager::types::SchemaHash,
        branch: &str,
        row_id: ObjectId,
        batch_id: crate::row_histories::BatchId,
        state: Option<crate::row_histories::RowState>,
        confirmed_tier: Option<DurabilityTier>,
    ) -> Result<bool, StorageError> {
        self.inner.patch_exact_row_batch_for_schema_hash(
            table,
            schema_hash,
            branch,
            row_id,
            batch_id,
            state,
            confirmed_tier,
        )
    }

    fn scan_visible_region(
        &self,
        table: &str,
        branch: &str,
    ) -> Result<Vec<crate::row_histories::StoredRowBatch>, StorageError> {
        self.inner.scan_visible_region(table, branch)
    }

    fn load_visible_region_row(
        &self,
        table: &str,
        branch: &str,
        row_id: ObjectId,
    ) -> Result<Option<crate::row_histories::StoredRowBatch>, StorageError> {
        self.inner.load_visible_region_row(table, branch, row_id)
    }

    fn load_visible_region_frontier(
        &self,
        table: &str,
        branch: &str,
        row_id: ObjectId,
    ) -> Result<Option<Vec<crate::row_histories::BatchId>>, StorageError> {
        self.inner
            .load_visible_region_frontier(table, branch, row_id)
    }

    fn capture_family_visible_frontier(
        &self,
        target_branch_name: crate::object::BranchName,
    ) -> Result<Vec<crate::batch_fate::CapturedFrontierMember>, StorageError> {
        self.inner
            .capture_family_visible_frontier(target_branch_name)
    }

    fn scan_visible_region_row_batches(
        &self,
        table: &str,
        row_id: ObjectId,
    ) -> Result<Vec<crate::row_histories::StoredRowBatch>, StorageError> {
        self.inner.scan_visible_region_row_batches(table, row_id)
    }

    fn scan_history_row_batches(
        &self,
        table: &str,
        row_id: ObjectId,
    ) -> Result<Vec<crate::row_histories::StoredRowBatch>, StorageError> {
        self.inner.scan_history_row_batches(table, row_id)
    }

    fn load_history_row_batch(
        &self,
        table: &str,
        branch: &str,
        row_id: ObjectId,
        batch_id: crate::row_histories::BatchId,
    ) -> Result<Option<crate::row_histories::StoredRowBatch>, StorageError> {
        self.inner
            .load_history_row_batch(table, branch, row_id, batch_id)
    }

    fn load_history_query_row_batch(
        &self,
        table: &str,
        branch: &str,
        row_id: ObjectId,
        batch_id: crate::row_histories::BatchId,
    ) -> Result<Option<crate::row_histories::QueryRowBatch>, StorageError> {
        self.inner
            .load_history_query_row_batch(table, branch, row_id, batch_id)
    }

    fn load_history_row_batch_for_schema_hash(
        &self,
        table: &str,
        schema_hash: crate::query_manager::types::SchemaHash,
        branch: &str,
        row_id: ObjectId,
        batch_id: crate::row_histories::BatchId,
    ) -> Result<Option<crate::row_histories::StoredRowBatch>, StorageError> {
        self.inner.load_history_row_batch_for_schema_hash(
            table,
            schema_hash,
            branch,
            row_id,
            batch_id,
        )
    }

    fn load_history_row_batch_any_branch(
        &self,
        table: &str,
        row_id: ObjectId,
        batch_id: crate::row_histories::BatchId,
    ) -> Result<Option<crate::row_histories::StoredRowBatch>, StorageError> {
        self.inner
            .load_history_row_batch_any_branch(table, row_id, batch_id)
    }

    fn load_history_query_row_batch_any_branch(
        &self,
        table: &str,
        row_id: ObjectId,
        batch_id: crate::row_histories::BatchId,
    ) -> Result<Option<crate::row_histories::QueryRowBatch>, StorageError> {
        self.inner
            .load_history_query_row_batch_any_branch(table, row_id, batch_id)
    }

    fn row_batch_exists(
        &self,
        table: &str,
        branch: &str,
        row_id: ObjectId,
        batch_id: crate::row_histories::BatchId,
    ) -> Result<bool, StorageError> {
        self.inner.row_batch_exists(table, branch, row_id, batch_id)
    }

    fn scan_row_branch_tip_ids(
        &self,
        table: &str,
        branch: &str,
        row_id: ObjectId,
    ) -> Result<Vec<crate::row_histories::BatchId>, StorageError> {
        self.inner.scan_row_branch_tip_ids(table, branch, row_id)
    }

    fn load_history_row_batch_bytes(
        &self,
        table: &str,
        branch: &str,
        row_id: ObjectId,
        batch_id: crate::row_histories::BatchId,
    ) -> Result<Option<Vec<u8>>, StorageError> {
        self.inner
            .load_history_row_batch_bytes(table, branch, row_id, batch_id)
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
    ) -> Result<Vec<crate::row_histories::StoredRowBatch>, StorageError> {
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
    fn apply_encoded_row_mutation(
        &mut self,
        table: &str,
        history_rows: &[crate::storage::OwnedHistoryRowBytes],
        visible_rows: &[crate::storage::OwnedVisibleRowBytes],
        index_mutations: &[crate::storage::IndexMutation<'_>],
    ) -> Result<(), StorageError> {
        self.calls.lock().unwrap().row_mutation_calls += 1;
        self.inner
            .apply_encoded_row_mutation(table, history_rows, visible_rows, index_mutations)
    }

    fn apply_prepared_row_mutation(
        &mut self,
        table: &str,
        history_rows: &[crate::row_histories::StoredRowBatch],
        visible_entries: &[crate::row_histories::VisibleRowEntry],
        encoded_history_rows: &[crate::storage::OwnedHistoryRowBytes],
        encoded_visible_rows: &[crate::storage::OwnedVisibleRowBytes],
        index_mutations: &[crate::storage::IndexMutation<'_>],
    ) -> Result<(), StorageError> {
        self.calls.lock().unwrap().row_mutation_calls += 1;
        self.inner.apply_prepared_row_mutation(
            table,
            history_rows,
            visible_entries,
            encoded_history_rows,
            encoded_visible_rows,
            index_mutations,
        )
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
        if table == "__local_batch_record" && key.starts_with("batch:") {
            self.calls.lock().unwrap().local_batch_record_get_calls += 1;
        }
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
        rows: &[crate::row_histories::StoredRowBatch],
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

    fn delete_visible_region_row(
        &mut self,
        table: &str,
        branch: &str,
        row_id: ObjectId,
    ) -> Result<(), StorageError> {
        self.inner.delete_visible_region_row(table, branch, row_id)
    }

    fn apply_row_mutation(
        &mut self,
        table: &str,
        history_rows: &[crate::row_histories::StoredRowBatch],
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

    fn patch_exact_row_batch(
        &mut self,
        table: &str,
        branch: &str,
        row_id: ObjectId,
        batch_id: crate::row_histories::BatchId,
        state: Option<crate::row_histories::RowState>,
        confirmed_tier: Option<DurabilityTier>,
    ) -> Result<bool, StorageError> {
        self.inner
            .patch_exact_row_batch(table, branch, row_id, batch_id, state, confirmed_tier)
    }

    fn patch_exact_row_batch_for_schema_hash(
        &mut self,
        table: &str,
        schema_hash: crate::query_manager::types::SchemaHash,
        branch: &str,
        row_id: ObjectId,
        batch_id: crate::row_histories::BatchId,
        state: Option<crate::row_histories::RowState>,
        confirmed_tier: Option<DurabilityTier>,
    ) -> Result<bool, StorageError> {
        self.inner.patch_exact_row_batch_for_schema_hash(
            table,
            schema_hash,
            branch,
            row_id,
            batch_id,
            state,
            confirmed_tier,
        )
    }

    fn scan_visible_region(
        &self,
        table: &str,
        branch: &str,
    ) -> Result<Vec<crate::row_histories::StoredRowBatch>, StorageError> {
        self.inner.scan_visible_region(table, branch)
    }

    fn load_visible_region_row(
        &self,
        table: &str,
        branch: &str,
        row_id: ObjectId,
    ) -> Result<Option<crate::row_histories::StoredRowBatch>, StorageError> {
        self.inner.load_visible_region_row(table, branch, row_id)
    }

    fn load_visible_region_frontier(
        &self,
        table: &str,
        branch: &str,
        row_id: ObjectId,
    ) -> Result<Option<Vec<crate::row_histories::BatchId>>, StorageError> {
        self.inner
            .load_visible_region_frontier(table, branch, row_id)
    }

    fn capture_family_visible_frontier(
        &self,
        target_branch_name: crate::object::BranchName,
    ) -> Result<Vec<crate::batch_fate::CapturedFrontierMember>, StorageError> {
        self.inner
            .capture_family_visible_frontier(target_branch_name)
    }

    fn scan_visible_region_row_batches(
        &self,
        table: &str,
        row_id: ObjectId,
    ) -> Result<Vec<crate::row_histories::StoredRowBatch>, StorageError> {
        self.inner.scan_visible_region_row_batches(table, row_id)
    }

    fn scan_history_row_batches(
        &self,
        table: &str,
        row_id: ObjectId,
    ) -> Result<Vec<crate::row_histories::StoredRowBatch>, StorageError> {
        self.inner.scan_history_row_batches(table, row_id)
    }

    fn load_history_row_batch(
        &self,
        table: &str,
        branch: &str,
        row_id: ObjectId,
        batch_id: crate::row_histories::BatchId,
    ) -> Result<Option<crate::row_histories::StoredRowBatch>, StorageError> {
        self.inner
            .load_history_row_batch(table, branch, row_id, batch_id)
    }

    fn load_history_query_row_batch(
        &self,
        table: &str,
        branch: &str,
        row_id: ObjectId,
        batch_id: crate::row_histories::BatchId,
    ) -> Result<Option<crate::row_histories::QueryRowBatch>, StorageError> {
        self.inner
            .load_history_query_row_batch(table, branch, row_id, batch_id)
    }

    fn load_history_row_batch_for_schema_hash(
        &self,
        table: &str,
        schema_hash: crate::query_manager::types::SchemaHash,
        branch: &str,
        row_id: ObjectId,
        batch_id: crate::row_histories::BatchId,
    ) -> Result<Option<crate::row_histories::StoredRowBatch>, StorageError> {
        self.inner.load_history_row_batch_for_schema_hash(
            table,
            schema_hash,
            branch,
            row_id,
            batch_id,
        )
    }

    fn load_history_row_batch_any_branch(
        &self,
        table: &str,
        row_id: ObjectId,
        batch_id: crate::row_histories::BatchId,
    ) -> Result<Option<crate::row_histories::StoredRowBatch>, StorageError> {
        self.inner
            .load_history_row_batch_any_branch(table, row_id, batch_id)
    }

    fn load_history_query_row_batch_any_branch(
        &self,
        table: &str,
        row_id: ObjectId,
        batch_id: crate::row_histories::BatchId,
    ) -> Result<Option<crate::row_histories::QueryRowBatch>, StorageError> {
        self.inner
            .load_history_query_row_batch_any_branch(table, row_id, batch_id)
    }

    fn row_batch_exists(
        &self,
        table: &str,
        branch: &str,
        row_id: ObjectId,
        batch_id: crate::row_histories::BatchId,
    ) -> Result<bool, StorageError> {
        self.inner.row_batch_exists(table, branch, row_id, batch_id)
    }

    fn scan_row_branch_tip_ids(
        &self,
        table: &str,
        branch: &str,
        row_id: ObjectId,
    ) -> Result<Vec<crate::row_histories::BatchId>, StorageError> {
        self.inner.scan_row_branch_tip_ids(table, branch, row_id)
    }

    fn load_history_row_batch_bytes(
        &self,
        table: &str,
        branch: &str,
        row_id: ObjectId,
        batch_id: crate::row_histories::BatchId,
    ) -> Result<Option<Vec<u8>>, StorageError> {
        self.inner
            .load_history_row_batch_bytes(table, branch, row_id, batch_id)
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
    ) -> Result<Vec<crate::row_histories::StoredRowBatch>, StorageError> {
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

fn session_exists_rel_teams_schema() -> Schema {
    use crate::query_manager::relation_ir::{
        ColumnRef, JoinCondition, JoinKind, PredicateCmpOp, PredicateExpr, RelExpr, RowIdRef,
        ValueRef,
    };

    let team_select_policy = PolicyExpr::ExistsRel {
        rel: RelExpr::Filter {
            input: Box::new(RelExpr::Join {
                left: Box::new(RelExpr::TableScan {
                    table: TableName::new("user_team_edges"),
                }),
                right: Box::new(RelExpr::TableScan {
                    table: TableName::new("teams"),
                }),
                on: vec![JoinCondition {
                    left: ColumnRef::scoped("user_team_edges", "team_id"),
                    right: ColumnRef::scoped("__join_0", "id"),
                }],
                join_kind: JoinKind::Inner,
            }),
            predicate: PredicateExpr::And(vec![
                PredicateExpr::Cmp {
                    left: ColumnRef::scoped("user_team_edges", "user_id"),
                    op: PredicateCmpOp::Eq,
                    right: ValueRef::SessionRef(vec!["user_id".into()]),
                },
                PredicateExpr::Cmp {
                    left: ColumnRef::scoped("__join_0", "id"),
                    op: PredicateCmpOp::Eq,
                    right: ValueRef::RowId(RowIdRef::Outer),
                },
            ]),
        },
    };

    SchemaBuilder::new()
        .table(
            TableSchema::builder("teams")
                .column("name", ColumnType::Text)
                .policies(
                    TablePolicies::new()
                        .with_select(team_select_policy)
                        .with_insert(PolicyExpr::True),
                ),
        )
        .table(
            TableSchema::builder("user_team_edges")
                .column("user_id", ColumnType::Text)
                .column("team_id", ColumnType::Uuid)
                .policies(TablePolicies::new().with_insert(PolicyExpr::True)),
        )
        .build()
}

fn structural_session_exists_rel_teams_schema() -> Schema {
    SchemaBuilder::new()
        .table(TableSchema::builder("teams").column("name", ColumnType::Text))
        .table(
            TableSchema::builder("user_team_edges")
                .column("user_id", ColumnType::Text)
                .column("team_id", ColumnType::Uuid),
        )
        .build()
}

fn users_insert_denied_authorization_schema() -> Schema {
    SchemaBuilder::new()
        .table(
            TableSchema::builder("users")
                .column("id", ColumnType::Uuid)
                .column("name", ColumnType::Text)
                .policies(TablePolicies::new().with_insert(PolicyExpr::False)),
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

fn staged_user_row(
    row_id: ObjectId,
    batch_id: BatchId,
    updated_at: u64,
    name: &str,
) -> crate::row_histories::StoredRowBatch {
    crate::row_histories::StoredRowBatch::new_with_batch_id(
        batch_id,
        row_id,
        "main",
        Vec::<BatchId>::new(),
        encode_row(
            &test_schema()[&TableName::new("users")].columns,
            &user_row_values(row_id, name),
        )
        .expect("user test row should encode"),
        crate::metadata::RowProvenance::for_insert(row_id.to_string(), updated_at),
        HashMap::new(),
        crate::row_histories::RowState::StagingPending,
        None,
    )
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
    let mut core = new_test_core(schema_manager, MemoryStorage::new(), NoopScheduler);
    core.immediate_tick();
    core
}

fn create_runtime_with_schema(schema: Schema, app_name: &str) -> TestCore {
    create_runtime_with_schema_and_sync_manager(schema, app_name, SyncManager::new())
}

fn create_runtime_with_storage(schema: Schema, app_name: &str, storage: MemoryStorage) -> TestCore {
    create_runtime_with_storage_and_sync_manager(schema, app_name, storage, SyncManager::new())
}

fn create_runtime_with_storage_and_sync_manager(
    schema: Schema,
    app_name: &str,
    storage: MemoryStorage,
    sync_manager: SyncManager,
) -> TestCore {
    let app_id = AppId::from_name(app_name);
    let schema_manager = SchemaManager::new(sync_manager, schema, app_id, "dev", "main").unwrap();
    let mut core = new_test_core(schema_manager, storage, NoopScheduler);
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
    let mut core = new_test_core(schema_manager, storage, NoopScheduler);
    core.immediate_tick();
    core
}

fn create_test_runtime() -> TestCore {
    create_runtime_with_schema(test_schema(), "test-app")
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

fn execute_runtime_query_with_local_overlay(
    core: &mut TestCore,
    query: Query,
    session: Option<Session>,
    durability: ReadDurabilityOptions,
    propagation: crate::sync_manager::QueryPropagation,
    overlay: QueryLocalOverlay,
) -> Vec<(ObjectId, Vec<Value>)> {
    let waker = noop_waker();
    let mut cx = std::task::Context::from_waker(&waker);

    let mut future =
        core.query_with_local_overlay(query, session, durability, propagation, overlay);

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
    for entry in client.sync_sender().take() {
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
        for entry in writer.sync_sender().take() {
            if entry.destination == Destination::Server(writer_server_id) {
                any_messages = true;
                server.park_sync_message(InboxEntry {
                    source: Source::Client(writer_client_id),
                    payload: entry.payload,
                });
            }
        }

        alice_reader.batched_tick();
        for entry in alice_reader.sync_sender().take() {
            if entry.destination == Destination::Server(alice_reader_server_id) {
                any_messages = true;
                server.park_sync_message(InboxEntry {
                    source: Source::Client(alice_reader_client_id),
                    payload: entry.payload,
                });
            }
        }

        bob_reader.batched_tick();
        for entry in bob_reader.sync_sender().take() {
            if entry.destination == Destination::Server(bob_reader_server_id) {
                any_messages = true;
                server.park_sync_message(InboxEntry {
                    source: Source::Client(bob_reader_client_id),
                    payload: entry.payload,
                });
            }
        }

        server.batched_tick();
        let server_out = server.sync_sender().take();
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
            SyncPayload::RowBatchNeeded { row, .. } | SyncPayload::RowBatchCreated { row, .. } => {
                row.row_id == object_id
            }
            _ => false,
        }
    })
}

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
    create_3tier_rc_with_schema(schema)
}

fn create_3tier_rc_with_schema(schema: Schema) -> ThreeTierRC {
    let app_id = AppId::from_name("durability-test");

    // A = client (no tier)
    let sm_a = SyncManager::new();
    let mgr_a = SchemaManager::new(sm_a, schema.clone(), app_id, "dev", "main").unwrap();
    let mut a = new_test_core(mgr_a, MemoryStorage::new(), NoopScheduler);

    // B = Worker server
    let sm_b = SyncManager::new().with_durability_tier(DurabilityTier::Local);
    let mgr_b = SchemaManager::new(sm_b, schema.clone(), app_id, "dev", "main").unwrap();
    let mut b = new_test_core(mgr_b, MemoryStorage::new(), NoopScheduler);

    // C = EdgeServer
    let sm_c = SyncManager::new().with_durability_tier(DurabilityTier::EdgeServer);
    let mgr_c = SchemaManager::new(sm_c, schema, app_id, "dev", "main").unwrap();
    let mut c = new_test_core(mgr_c, MemoryStorage::new(), NoopScheduler);

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
    a.sync_sender().take();
    b.sync_sender().take();
    c.sync_sender().take();

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
        let a_out = s.a.sync_sender().take();
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
        let b_out = s.b.sync_sender().take();
        for entry in b_out {
            match &entry.destination {
                Destination::Client(cid) if *cid == s.a_client_of_b => {
                    any_messages = true;
                    s.a.park_sync_message(InboxEntry {
                        source: Source::Server(s.b_server_for_a),
                        payload: entry.payload,
                    });
                }
                Destination::Server(sid) if *sid == s.c_server_for_b => {
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
        let c_out = s.c.sync_sender().take();
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
    let a_out = s.a.sync_sender().take();
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
    let b_out = s.b.sync_sender().take();
    for entry in b_out {
        match &entry.destination {
            Destination::Client(cid) if *cid == s.a_client_of_b => {
                s.a.park_sync_message(InboxEntry {
                    source: Source::Server(s.b_server_for_a),
                    payload: entry.payload,
                });
            }
            Destination::Server(sid) if *sid == s.c_server_for_b => {
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
    let c_out = s.c.sync_sender().take();
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

fn noop_waker() -> std::task::Waker {
    fn noop(_: *const ()) {}
    fn clone(_: *const ()) -> std::task::RawWaker {
        std::task::RawWaker::new(std::ptr::null(), &VTABLE)
    }
    static VTABLE: std::task::RawWakerVTable =
        std::task::RawWakerVTable::new(clone, noop, noop, noop);
    unsafe { std::task::Waker::from_raw(std::task::RawWaker::new(std::ptr::null(), &VTABLE)) }
}

mod basic;
mod fk_remove_error;
mod install_transport_tests;
mod query_subscription;
mod schema_catalogue;
mod sync_replay;
mod write_batch;
