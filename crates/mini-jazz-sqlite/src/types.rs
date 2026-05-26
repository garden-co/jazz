use std::collections::BTreeMap;

use serde_json::Value as JsonValue;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RowView {
    pub table: String,
    pub id: String,
    pub values: BTreeMap<String, JsonValue>,
    pub created_by: String,
    pub tx_id: String,
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
pub struct StorageStats {
    pub history_rows: i64,
    pub current_rows: i64,
    pub rejected_transactions: i64,
    tx_nums_by_id: BTreeMap<String, i64>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TransactionInfo {
    pub tx_id: String,
    pub global_epoch: Option<i64>,
    pub receipt_tiers: Vec<String>,
}

impl StorageStats {
    pub(crate) fn new(
        history_rows: i64,
        current_rows: i64,
        rejected_transactions: i64,
        tx_nums_by_id: BTreeMap<String, i64>,
    ) -> Self {
        Self {
            history_rows,
            current_rows,
            rejected_transactions,
            tx_nums_by_id,
        }
    }

    pub fn physical_tx_num_for(&self, tx_id: &str) -> Option<i64> {
        self.tx_nums_by_id.get(tx_id).copied()
    }
}
