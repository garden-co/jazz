use std::collections::BTreeMap;
use std::time::Duration;

use crate::sync::Bundle;
use crate::value::Value as JsonValue;
use serde::{Deserialize, Serialize};

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
    pub history_block_uncompressed_bytes: i64,
    pub history_block_compressed_bytes: i64,
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
    pub block_uncompressed_bytes: i64,
    pub block_compressed_bytes: i64,
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

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct HistoryCompactionPolicy {
    pub hot_tail: usize,
    pub min_versions: usize,
    pub accepted: bool,
    pub rejected: bool,
    pub max_blocks: Option<usize>,
    pub max_compressed_bytes: Option<i64>,
    pub max_duration: Option<Duration>,
    pub max_rows_per_block: Option<usize>,
}

impl HistoryCompactionPolicy {
    pub fn all(hot_tail: usize, min_versions: usize) -> Self {
        Self {
            hot_tail,
            min_versions,
            accepted: true,
            rejected: true,
            max_blocks: None,
            max_compressed_bytes: None,
            max_duration: None,
            max_rows_per_block: None,
        }
    }

    pub fn accepted_only(hot_tail: usize, min_versions: usize) -> Self {
        Self {
            hot_tail,
            min_versions,
            accepted: true,
            rejected: false,
            max_blocks: None,
            max_compressed_bytes: None,
            max_duration: None,
            max_rows_per_block: None,
        }
    }

    pub fn with_max_blocks(mut self, max_blocks: usize) -> Self {
        self.max_blocks = Some(max_blocks);
        self
    }

    pub fn with_max_duration(mut self, max_duration: Duration) -> Self {
        self.max_duration = Some(max_duration);
        self
    }

    pub fn with_max_compressed_bytes(mut self, max_compressed_bytes: i64) -> Self {
        self.max_compressed_bytes = Some(max_compressed_bytes);
        self
    }

    pub fn with_max_rows_per_block(mut self, max_rows_per_block: usize) -> Self {
        self.max_rows_per_block = Some(max_rows_per_block);
        self
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
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

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct HistoryBlockExport {
    pub manifest: HistoryBlockManifest,
    pub tx_ranges: Vec<HistoryBlockTxRange>,
    #[serde(with = "hex_bytes")]
    pub payload: Vec<u8>,
}

#[derive(Clone, Debug)]
pub struct HistoryDelta {
    pub bundle: Bundle,
    pub blocks: Vec<HistoryBlockExport>,
}

#[derive(Clone, Debug, Default)]
pub struct TopFieldHistoryDeltaOptions {
    pub order_field_name: String,
    pub limit: usize,
    pub previous_observed_ids: Vec<String>,
    pub remote_block_manifests: Vec<HistoryBlockManifest>,
}

impl TopFieldHistoryDeltaOptions {
    pub fn new(order_field_name: &str, limit: usize) -> Self {
        Self {
            order_field_name: order_field_name.to_owned(),
            limit,
            previous_observed_ids: Vec::new(),
            remote_block_manifests: Vec::new(),
        }
    }

    pub fn with_previous_observed_ids(mut self, row_ids: Vec<String>) -> Self {
        self.previous_observed_ids = row_ids;
        self
    }

    pub fn with_remote_block_manifests(mut self, manifests: Vec<HistoryBlockManifest>) -> Self {
        self.remote_block_manifests = manifests;
        self
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
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

impl Default for ApplyBundleProfile {
    fn default() -> Self {
        Self {
            total_ms: 0.0,
            validation_ms: 0.0,
            begin_tx_ms: 0.0,
            branches_ms: 0.0,
            txs_ms: 0.0,
            reads_ms: 0.0,
            rejected_cleanup_ms: 0.0,
            query_reads_ms: 0.0,
            history_ms: 0.0,
            query_scope_repair_ms: 0.0,
            commit_ms: 0.0,
            revalidate_awaiting_ms: 0.0,
            branch_rows: 0,
            tx_rows: 0,
            read_rows: 0,
            query_read_rows: 0,
            history_rows: 0,
        }
    }
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
            history_block_uncompressed_bytes: history_counts.block_uncompressed_bytes,
            history_block_compressed_bytes: history_counts.block_compressed_bytes,
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

mod hex_bytes {
    use serde::{Deserialize, Deserializer, Serializer};

    pub fn serialize<S>(bytes: &[u8], serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut out = String::with_capacity(bytes.len() * 2);
        for byte in bytes {
            use std::fmt::Write as _;
            write!(&mut out, "{byte:02x}").map_err(serde::ser::Error::custom)?;
        }
        serializer.serialize_str(&out)
    }

    pub fn deserialize<'de, D>(deserializer: D) -> Result<Vec<u8>, D::Error>
    where
        D: Deserializer<'de>,
    {
        let text = String::deserialize(deserializer)?;
        if text.len() % 2 != 0 {
            return Err(serde::de::Error::custom("hex byte string has odd length"));
        }
        let mut bytes = Vec::with_capacity(text.len() / 2);
        let raw = text.as_bytes();
        for chunk in raw.chunks_exact(2) {
            let high = hex_nibble(chunk[0]).ok_or_else(|| {
                serde::de::Error::custom("hex byte string contains non-hex digit")
            })?;
            let low = hex_nibble(chunk[1]).ok_or_else(|| {
                serde::de::Error::custom("hex byte string contains non-hex digit")
            })?;
            bytes.push((high << 4) | low);
        }
        Ok(bytes)
    }

    fn hex_nibble(byte: u8) -> Option<u8> {
        match byte {
            b'0'..=b'9' => Some(byte - b'0'),
            b'a'..=b'f' => Some(byte - b'a' + 10),
            b'A'..=b'F' => Some(byte - b'A' + 10),
            _ => None,
        }
    }
}
