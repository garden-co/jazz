use std::collections::BTreeMap;

use serde::ser::SerializeStruct;
use serde::{Deserialize, Serialize, Serializer};
use serde_json::Value as JsonValue;

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum ReadTier {
    Local,
    Edge,
    Global,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct RowView {
    pub table: String,
    pub id: String,
    pub values: BTreeMap<String, JsonValue>,
    pub created_at: i64,
    pub created_by: String,
    pub tx_id: String,
    pub conflict_count: usize,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
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

#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub struct SubscriptionDelta {
    pub all: Vec<RowView>,
    pub delta: Vec<SubscriptionRowDelta>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum SubscriptionRowDelta {
    Added {
        id: String,
        index: usize,
        item: RowView,
    },
    Removed {
        id: String,
        index: usize,
    },
    Updated {
        id: String,
        index: usize,
        item: Option<RowView>,
    },
    Moved {
        id: String,
        previous_index: usize,
        index: usize,
    },
}

impl SubscriptionDelta {
    pub fn initial(all: Vec<RowView>) -> Self {
        let delta = all
            .iter()
            .enumerate()
            .map(|(index, row)| SubscriptionRowDelta::Added {
                id: row.id.clone(),
                index,
                item: row.clone(),
            })
            .collect();
        Self { all, delta }
    }
}

impl SubscriptionRowDelta {
    pub fn kind(&self) -> u8 {
        match self {
            Self::Added { .. } => 0,
            Self::Removed { .. } => 1,
            Self::Updated { .. } | Self::Moved { .. } => 2,
        }
    }

    pub fn id(&self) -> &str {
        match self {
            Self::Added { id, .. }
            | Self::Removed { id, .. }
            | Self::Updated { id, .. }
            | Self::Moved { id, .. } => id,
        }
    }

    pub fn index(&self) -> usize {
        match self {
            Self::Added { index, .. }
            | Self::Removed { index, .. }
            | Self::Updated { index, .. }
            | Self::Moved { index, .. } => *index,
        }
    }
}

impl Serialize for SubscriptionRowDelta {
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        match self {
            SubscriptionRowDelta::Added { id, index, item } => {
                let mut state = serializer.serialize_struct("SubscriptionRowDelta", 4)?;
                state.serialize_field("kind", &0_u8)?;
                state.serialize_field("id", id)?;
                state.serialize_field("index", index)?;
                state.serialize_field("row", item)?;
                state.end()
            }
            SubscriptionRowDelta::Removed { id, index } => {
                let mut state = serializer.serialize_struct("SubscriptionRowDelta", 3)?;
                state.serialize_field("kind", &1_u8)?;
                state.serialize_field("id", id)?;
                state.serialize_field("index", index)?;
                state.end()
            }
            SubscriptionRowDelta::Updated { id, index, item } => {
                let field_count = if item.is_some() { 4 } else { 3 };
                let mut state = serializer.serialize_struct("SubscriptionRowDelta", field_count)?;
                state.serialize_field("kind", &2_u8)?;
                state.serialize_field("id", id)?;
                state.serialize_field("index", index)?;
                if let Some(item) = item {
                    state.serialize_field("row", item)?;
                }
                state.end()
            }
            SubscriptionRowDelta::Moved {
                id,
                index,
                previous_index: _,
            } => {
                let mut state = serializer.serialize_struct("SubscriptionRowDelta", 3)?;
                state.serialize_field("kind", &2_u8)?;
                state.serialize_field("id", id)?;
                state.serialize_field("index", index)?;
                state.end()
            }
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub struct BranchInfo {
    pub id: String,
    pub base_global_epoch: Option<i64>,
    pub source_branch_ids: Vec<String>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub struct StorageStats {
    pub history_rows: i64,
    pub current_rows: i64,
    pub rejected_transactions: i64,
    pub page_count: i64,
    pub page_size: i64,
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
    pub object_bytes: BTreeMap<String, i64>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
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
        history_rows: i64,
        current_rows: i64,
        rejected_transactions: i64,
        page_bytes: StoragePageBytes,
        file_bytes: StorageFileBytes,
        tx_nums_by_id: BTreeMap<String, i64>,
    ) -> Self {
        let total_file_bytes = file_bytes.main + file_bytes.wal + file_bytes.shm;
        Self {
            history_rows,
            current_rows,
            rejected_transactions,
            page_count: page_bytes.count,
            page_size: page_bytes.size,
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
