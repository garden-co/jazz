use serde::{Deserialize, Serialize};

use serde_json::Value as JsonValue;
use std::collections::BTreeMap;

pub const BUNDLE_PROTOCOL_VERSION: i64 = 1;

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Bundle {
    #[serde(default = "default_bundle_protocol_version")]
    pub protocol_version: i64,
    #[serde(default = "legacy_schema_fingerprint")]
    pub schema_fingerprint: String,
    #[serde(default = "legacy_policy_fingerprint")]
    pub policy_fingerprint: String,
    pub branches: Vec<BranchRecord>,
    pub txs: Vec<TxRecord>,
    pub reads: Vec<ReadRecord>,
    #[serde(default)]
    pub query_reads: Vec<QueryReadRecord>,
    pub history: Vec<HistoryRecord>,
}

fn default_bundle_protocol_version() -> i64 {
    BUNDLE_PROTOCOL_VERSION
}

fn legacy_schema_fingerprint() -> String {
    "legacy".to_owned()
}

fn legacy_policy_fingerprint() -> String {
    "legacy".to_owned()
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct BranchRecord {
    pub branch_id: String,
    pub base_global_epoch: Option<i64>,
    pub source_branch_ids: Vec<String>,
    #[serde(default)]
    pub source_version: i64,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct TxRecord {
    pub tx_id: String,
    pub node_id: String,
    pub local_epoch: i64,
    pub global_epoch: Option<i64>,
    pub conflict_mode: i64,
    pub outcome: i64,
    #[serde(default)]
    pub auth_principal: Option<String>,
    pub rejection_code: Option<String>,
    #[serde(default)]
    pub rejection_detail: Option<JsonValue>,
    pub receipt_tiers: Vec<i64>,
    pub created_at: i64,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ReadRecord {
    pub tx_id: String,
    pub table: String,
    pub row_id: String,
    pub reason: i64,
    #[serde(default)]
    pub observed_tx_id: Option<String>,
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

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct QueryPredicateRecord {
    pub(crate) table: String,
    pub(crate) field: String,
    pub(crate) op: String,
    pub(crate) value: JsonValue,
}

impl QueryPredicateRecord {
    pub(crate) fn new(table: &str, field: &str, op: &str, value: JsonValue) -> Self {
        Self {
            table: table.to_owned(),
            field: field.to_owned(),
            op: op.to_owned(),
            value,
        }
    }
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
