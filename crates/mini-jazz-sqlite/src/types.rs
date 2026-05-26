use std::collections::BTreeMap;

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
    Updated { before: RowView, after: RowView },
    Removed(RowView),
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TodoView {
    pub id: String,
    pub title: String,
    pub done: bool,
    pub project_id: String,
    pub project_title: Option<String>,
    pub created_by: String,
    pub tx_id: String,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct BranchInfo {
    pub id: String,
    pub base_global_epoch: Option<i64>,
    pub source_branch_ids: Vec<String>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct StorageStats {
    pub history_rows: i64,
    pub current_rows: i64,
    pub rejected_transactions: i64,
    pub page_count: i64,
    pub page_size: i64,
    pub database_bytes: i64,
    tx_nums_by_id: BTreeMap<String, i64>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TransactionInfo {
    pub tx_id: String,
    pub global_epoch: Option<i64>,
    pub conflict_mode: String,
    pub receipt_tiers: Vec<String>,
    pub rejection_code: Option<String>,
    pub rejection_detail: Option<JsonValue>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RejectionInfo {
    pub tx_id: String,
    pub code: String,
    pub detail: Option<JsonValue>,
}

impl StorageStats {
    pub(crate) fn new(
        history_rows: i64,
        current_rows: i64,
        rejected_transactions: i64,
        page_count: i64,
        page_size: i64,
        tx_nums_by_id: BTreeMap<String, i64>,
    ) -> Self {
        Self {
            history_rows,
            current_rows,
            rejected_transactions,
            page_count,
            page_size,
            database_bytes: page_count * page_size,
            tx_nums_by_id,
        }
    }

    pub fn physical_tx_num_for(&self, tx_id: &str) -> Option<i64> {
        self.tx_nums_by_id.get(tx_id).copied()
    }
}
