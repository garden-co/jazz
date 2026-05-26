use serde::{Deserialize, Serialize};

use serde_json::Value as JsonValue;
use std::collections::BTreeMap;

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Bundle {
    pub branches: Vec<BranchRecord>,
    pub txs: Vec<TxRecord>,
    pub reads: Vec<ReadRecord>,
    #[serde(default)]
    pub query_reads: Vec<QueryReadRecord>,
    pub history: Vec<HistoryRecord>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct BranchRecord {
    pub branch_id: String,
    pub base_global_epoch: Option<i64>,
    pub source_branch_ids: Vec<String>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct TxRecord {
    pub tx_id: String,
    pub node_id: String,
    pub local_epoch: i64,
    pub global_epoch: Option<i64>,
    pub conflict_mode: i64,
    pub outcome: i64,
    pub rejection_code: Option<String>,
    pub receipt_tiers: Vec<i64>,
    pub created_at: i64,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ReadRecord {
    pub tx_id: String,
    pub table: String,
    pub row_id: String,
    pub reason: i64,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct QueryReadRecord {
    pub branch_id: String,
    pub table: String,
    pub field: String,
    #[serde(default = "default_query_predicate_op")]
    pub op: String,
    pub value: JsonValue,
}

fn default_query_predicate_op() -> String {
    "eq".to_owned()
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct HistoryRecord {
    pub table: String,
    pub row_id: String,
    pub branch_id: String,
    pub tx_id: String,
    pub op: i64,
    pub values: BTreeMap<String, JsonValue>,
    pub created_at: i64,
    pub updated_at: i64,
    pub created_by: String,
    pub updated_by: String,
}
