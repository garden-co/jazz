use crate::Result;
use serde::{Deserialize, Serialize};

use serde_json::Value as JsonValue;
use std::collections::BTreeMap;

pub const BUNDLE_PROTOCOL_VERSION: i64 = 1;

pub fn encode_bundle(bundle: &Bundle) -> Result<Vec<u8>> {
    serde_json::to_vec(bundle).map_err(|err| crate::Error::new(format!("encode bundle: {err}")))
}

pub fn decode_bundle(encoded: &[u8]) -> Result<Bundle> {
    serde_json::from_slice(encoded)
        .map_err(|err| crate::Error::new(format!("decode bundle: {err}")))
}

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

pub fn merge_bundles(bundles: &[Bundle]) -> Result<Bundle> {
    let Some(first) = bundles.first() else {
        return Err(crate::Error::new("cannot merge empty bundle list"));
    };
    let mut merged = Bundle {
        protocol_version: first.protocol_version,
        schema_fingerprint: first.schema_fingerprint.clone(),
        policy_fingerprint: first.policy_fingerprint.clone(),
        branches: Vec::new(),
        txs: Vec::new(),
        reads: Vec::new(),
        query_reads: Vec::new(),
        history: Vec::new(),
    };
    let mut branches = BTreeMap::new();
    let mut txs = BTreeMap::new();
    let mut reads = BTreeMap::new();
    let mut query_reads = BTreeMap::new();
    let mut history = BTreeMap::new();
    for bundle in bundles {
        if bundle.protocol_version != merged.protocol_version
            || bundle.schema_fingerprint != merged.schema_fingerprint
            || bundle.policy_fingerprint != merged.policy_fingerprint
        {
            return Err(crate::Error::new(
                "cannot merge bundles with different metadata",
            ));
        }
        for record in &bundle.branches {
            branches.insert(record.branch_id.clone(), record.clone());
        }
        for record in &bundle.txs {
            txs.insert(record.tx_id.clone(), record.clone());
        }
        for record in &bundle.reads {
            reads.insert(stable_key(record)?, record.clone());
        }
        for record in &bundle.query_reads {
            query_reads.insert(stable_key(record)?, record.clone());
        }
        for record in &bundle.history {
            history.insert(stable_key(record)?, record.clone());
        }
    }
    merged.branches = branches.into_values().collect();
    merged.txs = txs.into_values().collect();
    merged.reads = reads.into_values().collect();
    merged.query_reads = query_reads.into_values().collect();
    merged.history = history.into_values().collect();
    Ok(merged)
}

fn stable_key<T: Serialize>(value: &T) -> Result<String> {
    serde_json::to_string(value).map_err(|err| crate::Error::new(err.to_string()))
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

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct BranchRecord {
    pub branch_id: String,
    pub base_global_epoch: Option<i64>,
    pub source_branch_ids: Vec<String>,
    #[serde(default)]
    pub source_version: i64,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct TxRecord {
    pub tx_id: String,
    pub node_id: String,
    pub local_epoch: i64,
    pub global_epoch: Option<i64>,
    pub conflict_mode: i64,
    pub outcome: i64,
    #[serde(default)]
    pub auth_user: Option<String>,
    pub rejection_code: Option<String>,
    #[serde(default)]
    pub rejection_detail: Option<JsonValue>,
    pub receipt_tiers: Vec<i64>,
    pub created_at: i64,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
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

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
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
