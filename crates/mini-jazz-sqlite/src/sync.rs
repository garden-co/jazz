use crate::Result;
use serde::{Deserialize, Deserializer, Serialize, Serializer};

use serde_json::Value as JsonValue;
use std::collections::BTreeMap;

pub const BUNDLE_PROTOCOL_VERSION: i64 = 1;

#[derive(Clone, Debug)]
pub struct Bundle {
    pub protocol_version: i64,
    pub schema_fingerprint: String,
    pub policy_fingerprint: String,
    pub branches: Vec<BranchRecord>,
    pub txs: Vec<TxRecord>,
    pub reads: Vec<ReadRecord>,
    pub query_reads: Vec<QueryReadRecord>,
    pub history: Vec<HistoryRecord>,
}

impl Serialize for Bundle {
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        CompactBundle::from_bundle(self)
            .map_err(serde::ser::Error::custom)?
            .serialize(serializer)
    }
}

impl<'de> Deserialize<'de> for Bundle {
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        #[derive(Deserialize)]
        #[serde(untagged)]
        enum Wire {
            Compact(CompactBundle),
            Legacy(LegacyBundle),
        }
        match Wire::deserialize(deserializer)? {
            Wire::Compact(compact) => compact.into_bundle().map_err(serde::de::Error::custom),
            Wire::Legacy(legacy) => Ok(legacy.into_bundle()),
        }
    }
}

#[derive(Deserialize)]
struct LegacyBundle {
    #[serde(default = "default_bundle_protocol_version")]
    protocol_version: i64,
    #[serde(default = "legacy_schema_fingerprint")]
    schema_fingerprint: String,
    #[serde(default = "legacy_policy_fingerprint")]
    policy_fingerprint: String,
    branches: Vec<BranchRecord>,
    txs: Vec<TxRecord>,
    reads: Vec<ReadRecord>,
    #[serde(default)]
    query_reads: Vec<QueryReadRecord>,
    history: Vec<HistoryRecord>,
}

impl LegacyBundle {
    fn into_bundle(self) -> Bundle {
        Bundle {
            protocol_version: self.protocol_version,
            schema_fingerprint: self.schema_fingerprint,
            policy_fingerprint: self.policy_fingerprint,
            branches: self.branches,
            txs: self.txs,
            reads: self.reads,
            query_reads: self.query_reads,
            history: self.history,
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct CompactBundle {
    #[serde(default = "default_bundle_protocol_version")]
    protocol_version: i64,
    #[serde(default = "legacy_schema_fingerprint")]
    schema_fingerprint: String,
    #[serde(default = "legacy_policy_fingerprint")]
    policy_fingerprint: String,
    strings: Vec<String>,
    #[serde(default)]
    branches_c: Vec<CompactBranchRecord>,
    #[serde(default)]
    txs_c: Vec<CompactTxRecord>,
    #[serde(default)]
    reads_c: Vec<CompactReadRecord>,
    #[serde(default)]
    query_reads_c: Vec<CompactQueryReadRecord>,
    #[serde(default)]
    history_c: Vec<CompactHistoryRecord>,
}

impl CompactBundle {
    fn from_bundle(bundle: &Bundle) -> Result<Self> {
        let mut strings = StringInterner::default();
        let branches_c = bundle
            .branches
            .iter()
            .map(|record| {
                Ok(CompactBranchRecord(
                    strings.intern(&record.branch_id),
                    record.base_global_epoch,
                    record
                        .source_branch_ids
                        .iter()
                        .map(|id| strings.intern(id))
                        .collect(),
                    record.source_version,
                ))
            })
            .collect::<Result<Vec<_>>>()?;
        let txs_c = bundle
            .txs
            .iter()
            .map(|record| {
                Ok(CompactTxRecord(
                    strings.intern(&record.tx_id),
                    strings.intern(&record.node_id),
                    record.local_epoch,
                    record.global_epoch,
                    record.conflict_mode,
                    record.outcome,
                    record.auth_user.as_ref().map(|user| strings.intern(user)),
                    record
                        .rejection_code
                        .as_ref()
                        .map(|code| strings.intern(code)),
                    record.rejection_detail.clone(),
                    record.receipt_tiers.clone(),
                    record.created_at,
                ))
            })
            .collect::<Result<Vec<_>>>()?;
        let reads_c = bundle
            .reads
            .iter()
            .map(|record| {
                Ok(CompactReadRecord(
                    strings.intern(&record.tx_id),
                    strings.intern(&record.table),
                    strings.intern(&record.row_id),
                    record.reason,
                    record
                        .observed_tx_id
                        .as_ref()
                        .map(|tx_id| strings.intern(tx_id)),
                ))
            })
            .collect::<Result<Vec<_>>>()?;
        let query_reads_c = bundle
            .query_reads
            .iter()
            .map(|record| {
                Ok(CompactQueryReadRecord(
                    strings.intern(&record.branch_id),
                    strings.intern(&record.table),
                    strings.intern(&record.field),
                    strings.intern(&record.op),
                    record.value.clone(),
                ))
            })
            .collect::<Result<Vec<_>>>()?;
        let history_c = bundle
            .history
            .iter()
            .map(|record| {
                Ok(CompactHistoryRecord(
                    strings.intern(&record.table),
                    strings.intern(&record.row_id),
                    strings.intern(&record.branch_id),
                    strings.intern(&record.tx_id),
                    record.op,
                    record
                        .values
                        .iter()
                        .map(|(field, value)| (strings.intern(field), value.clone()))
                        .collect(),
                    record.created_at,
                    record.updated_at,
                    strings.intern(&record.created_by),
                    strings.intern(&record.updated_by),
                ))
            })
            .collect::<Result<Vec<_>>>()?;
        Ok(Self {
            protocol_version: bundle.protocol_version,
            schema_fingerprint: bundle.schema_fingerprint.clone(),
            policy_fingerprint: bundle.policy_fingerprint.clone(),
            strings: strings.into_vec(),
            branches_c,
            txs_c,
            reads_c,
            query_reads_c,
            history_c,
        })
    }

    fn into_bundle(self) -> Result<Bundle> {
        let strings = self.strings;
        let branches = self
            .branches_c
            .into_iter()
            .map(|record| {
                let CompactBranchRecord(
                    branch_id,
                    base_global_epoch,
                    source_branch_ids,
                    source_version,
                ) = record;
                Ok(BranchRecord {
                    branch_id: get_string(&strings, branch_id)?,
                    base_global_epoch,
                    source_branch_ids: source_branch_ids
                        .into_iter()
                        .map(|idx| get_string(&strings, idx))
                        .collect::<Result<Vec<_>>>()?,
                    source_version,
                })
            })
            .collect::<Result<Vec<_>>>()?;
        let txs = self
            .txs_c
            .into_iter()
            .map(|record| {
                let CompactTxRecord(
                    tx_id,
                    node_id,
                    local_epoch,
                    global_epoch,
                    conflict_mode,
                    outcome,
                    auth_user,
                    rejection_code,
                    rejection_detail,
                    receipt_tiers,
                    created_at,
                ) = record;
                Ok(TxRecord {
                    tx_id: get_string(&strings, tx_id)?,
                    node_id: get_string(&strings, node_id)?,
                    local_epoch,
                    global_epoch,
                    conflict_mode,
                    outcome,
                    auth_user: auth_user.map(|idx| get_string(&strings, idx)).transpose()?,
                    rejection_code: rejection_code
                        .map(|idx| get_string(&strings, idx))
                        .transpose()?,
                    rejection_detail,
                    receipt_tiers,
                    created_at,
                })
            })
            .collect::<Result<Vec<_>>>()?;
        let reads = self
            .reads_c
            .into_iter()
            .map(|record| {
                let CompactReadRecord(tx_id, table, row_id, reason, observed_tx_id) = record;
                Ok(ReadRecord {
                    tx_id: get_string(&strings, tx_id)?,
                    table: get_string(&strings, table)?,
                    row_id: get_string(&strings, row_id)?,
                    reason,
                    observed_tx_id: observed_tx_id
                        .map(|idx| get_string(&strings, idx))
                        .transpose()?,
                })
            })
            .collect::<Result<Vec<_>>>()?;
        let query_reads = self
            .query_reads_c
            .into_iter()
            .map(|record| {
                let CompactQueryReadRecord(branch_id, table, field, op, value) = record;
                Ok(QueryReadRecord {
                    branch_id: get_string(&strings, branch_id)?,
                    table: get_string(&strings, table)?,
                    field: get_string(&strings, field)?,
                    op: get_string(&strings, op)?,
                    value,
                })
            })
            .collect::<Result<Vec<_>>>()?;
        let history = self
            .history_c
            .into_iter()
            .map(|record| {
                let CompactHistoryRecord(
                    table,
                    row_id,
                    branch_id,
                    tx_id,
                    op,
                    values,
                    created_at,
                    updated_at,
                    created_by,
                    updated_by,
                ) = record;
                Ok(HistoryRecord {
                    table: get_string(&strings, table)?,
                    row_id: get_string(&strings, row_id)?,
                    branch_id: get_string(&strings, branch_id)?,
                    tx_id: get_string(&strings, tx_id)?,
                    op,
                    values: values
                        .into_iter()
                        .map(|(idx, value)| Ok((get_string(&strings, idx)?, value)))
                        .collect::<Result<BTreeMap<_, _>>>()?,
                    created_at,
                    updated_at,
                    created_by: get_string(&strings, created_by)?,
                    updated_by: get_string(&strings, updated_by)?,
                })
            })
            .collect::<Result<Vec<_>>>()?;
        Ok(Bundle {
            protocol_version: self.protocol_version,
            schema_fingerprint: self.schema_fingerprint,
            policy_fingerprint: self.policy_fingerprint,
            branches,
            txs,
            reads,
            query_reads,
            history,
        })
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct CompactBranchRecord(usize, Option<i64>, Vec<usize>, i64);

#[derive(Clone, Debug, Serialize, Deserialize)]
struct CompactTxRecord(
    usize,
    usize,
    i64,
    Option<i64>,
    i64,
    i64,
    Option<usize>,
    Option<usize>,
    Option<JsonValue>,
    Vec<i64>,
    i64,
);

#[derive(Clone, Debug, Serialize, Deserialize)]
struct CompactReadRecord(usize, usize, usize, i64, Option<usize>);

#[derive(Clone, Debug, Serialize, Deserialize)]
struct CompactQueryReadRecord(usize, usize, usize, usize, JsonValue);

#[derive(Clone, Debug, Serialize, Deserialize)]
struct CompactHistoryRecord(
    usize,
    usize,
    usize,
    usize,
    i64,
    Vec<(usize, JsonValue)>,
    i64,
    i64,
    usize,
    usize,
);

#[derive(Default)]
struct StringInterner {
    strings: Vec<String>,
    by_string: BTreeMap<String, usize>,
}

impl StringInterner {
    fn intern(&mut self, value: &str) -> usize {
        if let Some(idx) = self.by_string.get(value) {
            return *idx;
        }
        let idx = self.strings.len();
        self.strings.push(value.to_owned());
        self.by_string.insert(value.to_owned(), idx);
        idx
    }

    fn into_vec(self) -> Vec<String> {
        self.strings
    }
}

fn get_string(strings: &[String], idx: usize) -> Result<String> {
    strings
        .get(idx)
        .cloned()
        .ok_or_else(|| crate::Error::new(format!("bundle string index {idx} out of range")))
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
    pub auth_user: Option<String>,
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
