use crate::Result;
use serde::{Deserialize, Serialize};

use crate::value::{Value as JsonValue, WireValue};
use std::collections::BTreeMap;

pub const BUNDLE_PROTOCOL_VERSION: i64 = 1;
const COLUMNAR_BUNDLE_MAGIC: &[u8; 4] = b"MJZC";

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

#[derive(Clone, Debug, Serialize, Deserialize)]
struct ColumnarBundle {
    protocol_version: i64,
    schema_fingerprint: String,
    policy_fingerprint: String,
    strings: Vec<String>,
    branches: ColumnarBranches,
    txs: ColumnarTxs,
    reads: ColumnarReads,
    query_reads: ColumnarQueryReads,
    history: ColumnarHistory,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
struct ColumnarBranches {
    branch_id: Vec<usize>,
    base_global_epoch: Vec<Option<i64>>,
    source_branch_ids: Vec<Vec<usize>>,
    source_version: Vec<i64>,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
struct ColumnarTxs {
    tx_id: Vec<usize>,
    node_id: Vec<usize>,
    local_epoch: Vec<i64>,
    global_epoch: Vec<Option<i64>>,
    conflict_mode: Vec<i64>,
    outcome: Vec<i64>,
    auth_user: Vec<Option<usize>>,
    rejection_code: Vec<Option<usize>>,
    rejection_detail: Vec<Option<WireValue>>,
    receipt_tiers: Vec<Vec<i64>>,
    created_at: Vec<i64>,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
struct ColumnarReads {
    tx_id: Vec<usize>,
    table: Vec<usize>,
    row_id: Vec<usize>,
    reason: Vec<i64>,
    observed_tx_id: Vec<Option<usize>>,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
struct ColumnarQueryReads {
    branch_id: Vec<usize>,
    table: Vec<usize>,
    field: Vec<usize>,
    op: Vec<usize>,
    value: Vec<WireValue>,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
struct ColumnarHistory {
    table: Vec<usize>,
    row_id: Vec<usize>,
    branch_id: Vec<usize>,
    tx_id: Vec<usize>,
    op: Vec<i64>,
    value_row: Vec<usize>,
    value_field: Vec<usize>,
    value: Vec<WireValue>,
    created_at: Vec<i64>,
    updated_at: Vec<i64>,
    created_by: Vec<usize>,
    updated_by: Vec<usize>,
}

impl ColumnarBundle {
    fn from_bundle(bundle: &Bundle) -> Self {
        let mut strings = StringInterner::default();
        let mut branches = ColumnarBranches::default();
        for record in &bundle.branches {
            branches.branch_id.push(strings.intern(&record.branch_id));
            branches.base_global_epoch.push(record.base_global_epoch);
            branches.source_branch_ids.push(
                record
                    .source_branch_ids
                    .iter()
                    .map(|id| strings.intern(id))
                    .collect(),
            );
            branches.source_version.push(record.source_version);
        }

        let mut txs = ColumnarTxs::default();
        for record in &bundle.txs {
            txs.tx_id.push(strings.intern(&record.tx_id));
            txs.node_id.push(strings.intern(&record.node_id));
            txs.local_epoch.push(record.local_epoch);
            txs.global_epoch.push(record.global_epoch);
            txs.conflict_mode.push(record.conflict_mode);
            txs.outcome.push(record.outcome);
            txs.auth_user
                .push(record.auth_user.as_ref().map(|user| strings.intern(user)));
            txs.rejection_code.push(
                record
                    .rejection_code
                    .as_ref()
                    .map(|code| strings.intern(code)),
            );
            txs.rejection_detail
                .push(record.rejection_detail.as_ref().map(WireValue::from));
            txs.receipt_tiers.push(record.receipt_tiers.clone());
            txs.created_at.push(record.created_at);
        }

        let mut reads = ColumnarReads::default();
        for record in &bundle.reads {
            reads.tx_id.push(strings.intern(&record.tx_id));
            reads.table.push(strings.intern(&record.table));
            reads.row_id.push(strings.intern(&record.row_id));
            reads.reason.push(record.reason);
            reads.observed_tx_id.push(
                record
                    .observed_tx_id
                    .as_ref()
                    .map(|tx_id| strings.intern(tx_id)),
            );
        }

        let mut query_reads = ColumnarQueryReads::default();
        for record in &bundle.query_reads {
            query_reads
                .branch_id
                .push(strings.intern(&record.branch_id));
            query_reads.table.push(strings.intern(&record.table));
            query_reads.field.push(strings.intern(&record.field));
            query_reads.op.push(strings.intern(&record.op));
            query_reads.value.push(WireValue::from(&record.value));
        }

        let mut history = ColumnarHistory::default();
        for record in &bundle.history {
            history.table.push(strings.intern(&record.table));
            history.row_id.push(strings.intern(&record.row_id));
            history.branch_id.push(strings.intern(&record.branch_id));
            history.tx_id.push(strings.intern(&record.tx_id));
            history.op.push(record.op);
            for (field, value) in &record.values {
                history.value_row.push(history.tx_id.len() - 1);
                history.value_field.push(strings.intern(field));
                history.value.push(WireValue::from(value));
            }
            history.created_at.push(record.created_at);
            history.updated_at.push(record.updated_at);
            history.created_by.push(strings.intern(&record.created_by));
            history.updated_by.push(strings.intern(&record.updated_by));
        }

        Self {
            protocol_version: bundle.protocol_version,
            schema_fingerprint: bundle.schema_fingerprint.clone(),
            policy_fingerprint: bundle.policy_fingerprint.clone(),
            strings: strings.into_vec(),
            branches,
            txs,
            reads,
            query_reads,
            history,
        }
    }

    fn into_bundle(self) -> Result<Bundle> {
        let strings = self.strings;
        let branches_len = self.branches.branch_id.len();
        let txs_len = self.txs.tx_id.len();
        let reads_len = self.reads.tx_id.len();
        let query_reads_len = self.query_reads.branch_id.len();
        let history_len = self.history.tx_id.len();
        ensure_column_len(
            "branches.base_global_epoch",
            self.branches.base_global_epoch.len(),
            branches_len,
        )?;
        ensure_column_len(
            "branches.source_branch_ids",
            self.branches.source_branch_ids.len(),
            branches_len,
        )?;
        ensure_column_len(
            "branches.source_version",
            self.branches.source_version.len(),
            branches_len,
        )?;
        ensure_column_len("txs.node_id", self.txs.node_id.len(), txs_len)?;
        ensure_column_len("txs.local_epoch", self.txs.local_epoch.len(), txs_len)?;
        ensure_column_len("txs.global_epoch", self.txs.global_epoch.len(), txs_len)?;
        ensure_column_len("txs.conflict_mode", self.txs.conflict_mode.len(), txs_len)?;
        ensure_column_len("txs.outcome", self.txs.outcome.len(), txs_len)?;
        ensure_column_len("txs.auth_user", self.txs.auth_user.len(), txs_len)?;
        ensure_column_len("txs.rejection_code", self.txs.rejection_code.len(), txs_len)?;
        ensure_column_len(
            "txs.rejection_detail",
            self.txs.rejection_detail.len(),
            txs_len,
        )?;
        ensure_column_len("txs.receipt_tiers", self.txs.receipt_tiers.len(), txs_len)?;
        ensure_column_len("txs.created_at", self.txs.created_at.len(), txs_len)?;
        ensure_column_len("reads.table", self.reads.table.len(), reads_len)?;
        ensure_column_len("reads.row_id", self.reads.row_id.len(), reads_len)?;
        ensure_column_len("reads.reason", self.reads.reason.len(), reads_len)?;
        ensure_column_len(
            "reads.observed_tx_id",
            self.reads.observed_tx_id.len(),
            reads_len,
        )?;
        ensure_column_len(
            "query_reads.table",
            self.query_reads.table.len(),
            query_reads_len,
        )?;
        ensure_column_len(
            "query_reads.field",
            self.query_reads.field.len(),
            query_reads_len,
        )?;
        ensure_column_len("query_reads.op", self.query_reads.op.len(), query_reads_len)?;
        ensure_column_len(
            "query_reads.value",
            self.query_reads.value.len(),
            query_reads_len,
        )?;
        ensure_column_len("history.table", self.history.table.len(), history_len)?;
        ensure_column_len("history.row_id", self.history.row_id.len(), history_len)?;
        ensure_column_len(
            "history.branch_id",
            self.history.branch_id.len(),
            history_len,
        )?;
        ensure_column_len("history.op", self.history.op.len(), history_len)?;
        ensure_column_len(
            "history.value_field",
            self.history.value_field.len(),
            self.history.value_row.len(),
        )?;
        ensure_column_len(
            "history.value",
            self.history.value.len(),
            self.history.value_row.len(),
        )?;
        ensure_column_len(
            "history.created_at",
            self.history.created_at.len(),
            history_len,
        )?;
        ensure_column_len(
            "history.updated_at",
            self.history.updated_at.len(),
            history_len,
        )?;
        ensure_column_len(
            "history.created_by",
            self.history.created_by.len(),
            history_len,
        )?;
        ensure_column_len(
            "history.updated_by",
            self.history.updated_by.len(),
            history_len,
        )?;

        let mut branches = Vec::with_capacity(branches_len);
        for idx in 0..branches_len {
            branches.push(BranchRecord {
                branch_id: get_string(&strings, self.branches.branch_id[idx])?,
                base_global_epoch: self.branches.base_global_epoch[idx],
                source_branch_ids: self.branches.source_branch_ids[idx]
                    .iter()
                    .copied()
                    .map(|idx| get_string(&strings, idx))
                    .collect::<Result<Vec<_>>>()?,
                source_version: self.branches.source_version[idx],
            });
        }

        let mut txs = Vec::with_capacity(txs_len);
        for idx in 0..txs_len {
            txs.push(TxRecord {
                tx_id: get_string(&strings, self.txs.tx_id[idx])?,
                node_id: get_string(&strings, self.txs.node_id[idx])?,
                local_epoch: self.txs.local_epoch[idx],
                global_epoch: self.txs.global_epoch[idx],
                conflict_mode: self.txs.conflict_mode[idx],
                outcome: self.txs.outcome[idx],
                auth_user: self.txs.auth_user[idx]
                    .map(|idx| get_string(&strings, idx))
                    .transpose()?,
                rejection_code: self.txs.rejection_code[idx]
                    .map(|idx| get_string(&strings, idx))
                    .transpose()?,
                rejection_detail: self.txs.rejection_detail[idx].clone().map(Into::into),
                receipt_tiers: self.txs.receipt_tiers[idx].clone(),
                created_at: self.txs.created_at[idx],
            });
        }

        let mut reads = Vec::with_capacity(reads_len);
        for idx in 0..reads_len {
            reads.push(ReadRecord {
                tx_id: get_string(&strings, self.reads.tx_id[idx])?,
                table: get_string(&strings, self.reads.table[idx])?,
                row_id: get_string(&strings, self.reads.row_id[idx])?,
                reason: self.reads.reason[idx],
                observed_tx_id: self.reads.observed_tx_id[idx]
                    .map(|idx| get_string(&strings, idx))
                    .transpose()?,
            });
        }

        let mut query_reads = Vec::with_capacity(query_reads_len);
        for idx in 0..query_reads_len {
            query_reads.push(QueryReadRecord {
                branch_id: get_string(&strings, self.query_reads.branch_id[idx])?,
                table: get_string(&strings, self.query_reads.table[idx])?,
                field: get_string(&strings, self.query_reads.field[idx])?,
                op: get_string(&strings, self.query_reads.op[idx])?,
                value: self.query_reads.value[idx].clone().into(),
            });
        }

        let mut history_values = vec![BTreeMap::new(); history_len];
        for idx in 0..self.history.value_row.len() {
            let row = self.history.value_row[idx];
            if row >= history_len {
                return Err(crate::Error::new(format!(
                    "bundle history value row {row} out of range"
                )));
            }
            history_values[row].insert(
                get_string(&strings, self.history.value_field[idx])?,
                self.history.value[idx].clone().into(),
            );
        }

        let mut history = Vec::with_capacity(history_len);
        for (idx, values) in history_values.iter_mut().enumerate().take(history_len) {
            history.push(HistoryRecord {
                table: get_string(&strings, self.history.table[idx])?,
                row_id: get_string(&strings, self.history.row_id[idx])?,
                branch_id: get_string(&strings, self.history.branch_id[idx])?,
                tx_id: get_string(&strings, self.history.tx_id[idx])?,
                op: self.history.op[idx],
                values: std::mem::take(values),
                created_at: self.history.created_at[idx],
                updated_at: self.history.updated_at[idx],
                created_by: get_string(&strings, self.history.created_by[idx])?,
                updated_by: get_string(&strings, self.history.updated_by[idx])?,
            });
        }

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

fn ensure_column_len(name: &str, actual: usize, expected: usize) -> Result<()> {
    if actual == expected {
        Ok(())
    } else {
        Err(crate::Error::new(format!(
            "bundle column {name} length {actual} != {expected}"
        )))
    }
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
    bincode::serialize(value)
        .map(|bytes| crate::value::bytes_to_hex(&bytes))
        .map_err(|err| crate::Error::new(err.to_string()))
}

pub fn encode_bundle(bundle: &Bundle) -> Result<Vec<u8>> {
    let payload = encode_bundle_payload(bundle)?;
    let compressed = lz4_flex::compress_prepend_size(&payload);
    let mut bytes = Vec::from(COLUMNAR_BUNDLE_MAGIC.as_slice());
    bytes.extend(compressed);
    Ok(bytes)
}

pub(crate) fn encode_bundle_payload(bundle: &Bundle) -> Result<Vec<u8>> {
    let columnar = ColumnarBundle::from_bundle(bundle);
    postcard::to_allocvec(&columnar)
        .map_err(|err| crate::Error::new(format!("encode bundle: {err}")))
}

pub fn decode_bundle(bytes: &[u8]) -> Result<Bundle> {
    if let Some(payload) = bytes.strip_prefix(COLUMNAR_BUNDLE_MAGIC) {
        let decompressed = lz4_flex::decompress_size_prepended(payload)
            .map_err(|err| crate::Error::new(format!("decompress bundle: {err}")))?;
        return decode_bundle_payload(&decompressed);
    }
    Err(crate::Error::new("unsupported bundle encoding"))
}

pub(crate) fn decode_bundle_payload(payload: &[u8]) -> Result<Bundle> {
    let columnar = postcard::from_bytes::<ColumnarBundle>(payload)
        .map_err(|err| crate::Error::new(format!("decode bundle: {err}")))?;
    columnar.into_bundle()
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct BranchRecord {
    pub branch_id: String,
    pub base_global_epoch: Option<i64>,
    pub source_branch_ids: Vec<String>,
    #[serde(default)]
    pub source_version: i64,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
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

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
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

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
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
