use std::collections::BTreeMap;

use serde::Serialize;
use serde_json::Value as JsonValue;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RowView {
    pub table: String,
    pub id: String,
    pub values: BTreeMap<String, JsonValue>,
    pub created_by: String,
    pub tx_id: String,
    pub conflict_count: usize,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum RowDiff {
    Added(RowView),
    Updated {
        before: RowView,
        after: RowView,
    },
    Moved {
        row: RowView,
        before_index: usize,
        after_index: usize,
    },
    Removed(RowView),
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct BranchInfo {
    pub id: String,
    pub base_global_epoch: Option<i64>,
    pub source_branch_ids: Vec<String>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub struct StorageStats {
    pub history_rows: i64,
    pub sealed_history_rows: i64,
    pub history_blocks: i64,
    pub current_rows: i64,
    pub rejected_transactions: i64,
    pub page_count: i64,
    pub page_size: i64,
    pub freelist_pages: i64,
    pub freelist_bytes: i64,
    pub live_database_bytes: i64,
    pub database_bytes: i64,
    pub main_file_bytes: i64,
    pub wal_file_bytes: i64,
    pub shm_file_bytes: i64,
    pub total_file_bytes: i64,
    pub table_page_bytes: BTreeMap<String, i64>,
    #[serde(skip)]
    tx_nums_by_id: BTreeMap<String, i64>,
}

pub(crate) struct StorageFileBytes {
    pub main: i64,
    pub wal: i64,
    pub shm: i64,
}

pub(crate) struct StoragePageBytes {
    pub count: i64,
    pub size: i64,
    pub freelist: i64,
    pub object_bytes: BTreeMap<String, i64>,
}

pub(crate) struct StorageHistoryCounts {
    pub open_rows: i64,
    pub sealed_rows: i64,
    pub blocks: i64,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TransactionInfo {
    pub tx_id: String,
    pub global_epoch: Option<i64>,
    pub conflict_mode: String,
    pub receipt_tiers: Vec<String>,
    pub awaiting_dependency: Option<JsonValue>,
    pub rejection_code: Option<String>,
    pub rejection_detail: Option<JsonValue>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub struct HistoryCompactionStats {
    pub sealed_history_rows: i64,
    pub history_blocks: i64,
    pub sealed_transactions: i64,
    pub uncompressed_bytes: i64,
    pub compressed_bytes: i64,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub struct HistoryBlockManifest {
    pub block_id: i64,
    pub kind: String,
    pub table: String,
    pub row_id: String,
    pub min_global_epoch: i64,
    pub max_global_epoch: i64,
    pub row_count: i64,
    pub tx_count: i64,
    pub codec: String,
    pub format_version: i64,
    pub uncompressed_bytes: i64,
    pub compressed_bytes: i64,
    pub payload_sha256: String,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub struct HistoryBlockExport {
    pub manifest: HistoryBlockManifest,
    pub tx_ranges: Vec<HistoryBlockTxRange>,
    #[serde(skip)]
    pub payload: Vec<u8>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub struct HistoryBlockTxRange {
    pub node_id: String,
    pub min_local_epoch: i64,
    pub max_local_epoch: i64,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RejectionInfo {
    pub tx_id: String,
    pub code: String,
    pub detail: Option<JsonValue>,
}

#[derive(Clone, Debug, PartialEq, Serialize)]
pub struct QueryExportProfile {
    pub total_ms: f64,
    pub read_rows_ms: f64,
    pub resolve_visible_row_nums_ms: f64,
    pub repair_row_nums_ms: f64,
    pub visible_history_ms: f64,
    pub repair_visible_history_ms: f64,
    pub repair_all_history_ms: f64,
    pub policy_dependency_history_ms: f64,
    pub branch_snapshot_history_ms: f64,
    pub dedupe_history_ms: f64,
    pub reads_ms: f64,
    pub rejected_tx_ids_ms: f64,
    pub txs_ms: f64,
    pub branches_ms: f64,
    pub make_bundle_ms: f64,
    pub history_rows: usize,
    pub read_rows: usize,
    pub tx_rows: usize,
    pub branch_rows: usize,
}

#[derive(Clone, Debug, PartialEq, Serialize)]
pub struct ApplyBundleProfile {
    pub total_ms: f64,
    pub validation_ms: f64,
    pub begin_tx_ms: f64,
    pub branches_ms: f64,
    pub txs_ms: f64,
    pub reads_ms: f64,
    pub rejected_cleanup_ms: f64,
    pub query_reads_ms: f64,
    pub history_ms: f64,
    pub query_scope_repair_ms: f64,
    pub commit_ms: f64,
    pub revalidate_awaiting_ms: f64,
    pub branch_rows: usize,
    pub tx_rows: usize,
    pub read_rows: usize,
    pub query_read_rows: usize,
    pub history_rows: usize,
}

impl StorageStats {
    pub(crate) fn new(
        history_counts: StorageHistoryCounts,
        current_rows: i64,
        rejected_transactions: i64,
        page_bytes: StoragePageBytes,
        file_bytes: StorageFileBytes,
        tx_nums_by_id: BTreeMap<String, i64>,
    ) -> Self {
        let total_file_bytes = file_bytes.main + file_bytes.wal + file_bytes.shm;
        Self {
            history_rows: history_counts.open_rows,
            sealed_history_rows: history_counts.sealed_rows,
            history_blocks: history_counts.blocks,
            current_rows,
            rejected_transactions,
            page_count: page_bytes.count,
            page_size: page_bytes.size,
            freelist_pages: page_bytes.freelist,
            freelist_bytes: page_bytes.freelist * page_bytes.size,
            live_database_bytes: (page_bytes.count - page_bytes.freelist) * page_bytes.size,
            database_bytes: page_bytes.count * page_bytes.size,
            main_file_bytes: file_bytes.main,
            wal_file_bytes: file_bytes.wal,
            shm_file_bytes: file_bytes.shm,
            total_file_bytes,
            table_page_bytes: page_bytes.object_bytes,
            tx_nums_by_id,
        }
    }

    pub fn physical_tx_num_for(&self, tx_id: &str) -> Option<i64> {
        self.tx_nums_by_id.get(tx_id).copied()
    }
}
