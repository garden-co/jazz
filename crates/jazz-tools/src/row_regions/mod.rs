use std::collections::HashMap;
use std::sync::OnceLock;

use blake3::Hasher;
use serde::{Deserialize, Serialize};
use smallvec::SmallVec;
use uuid::Uuid;

#[cfg(test)]
use crate::commit::Commit;
use crate::commit::CommitId;
use crate::metadata::{DeleteKind, MetadataKey, RowProvenance};
use crate::object::ObjectId;
use crate::query_manager::encoding::{EncodingError, decode_column, decode_row, encode_row};
use crate::query_manager::types::{
    ColumnDescriptor, ColumnType, RowBytes, RowDescriptor, SharedString, Value,
};
use crate::sync_manager::DurabilityTier;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct BatchId(pub Uuid);

impl BatchId {
    pub fn new() -> Self {
        Self(Uuid::now_v7())
    }

    pub fn from_commit_id(commit_id: CommitId) -> Self {
        Self(Uuid::new_v5(&Uuid::NAMESPACE_OID, &commit_id.0))
    }
}

impl Default for BatchId {
    fn default() -> Self {
        Self::new()
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum RowState {
    StagingPending,
    Rejected,
    VisibleDirect,
    VisibleTransactional,
}

impl RowState {
    pub fn is_visible(self) -> bool {
        matches!(self, Self::VisibleDirect | Self::VisibleTransactional)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum HistoryScan {
    Branch,
    Row { row_id: ObjectId },
    AsOf { ts: u64 },
}

fn tier_satisfies(confirmed_tier: Option<DurabilityTier>, required_tier: DurabilityTier) -> bool {
    confirmed_tier.is_some_and(|confirmed| confirmed >= required_tier)
}

fn malformed(message: impl Into<String>) -> EncodingError {
    EncodingError::MalformedData {
        message: message.into(),
    }
}

pub fn compute_row_version_id(
    branch: &str,
    parents: &[CommitId],
    data: &[u8],
    updated_at: u64,
    updated_by: &str,
    metadata: Option<&RowMetadata>,
) -> CommitId {
    let mut hasher = Hasher::new();

    hasher.update(b"row-version-v1");
    hasher.update(&(branch.len() as u64).to_le_bytes());
    hasher.update(branch.as_bytes());

    hasher.update(&(parents.len() as u64).to_le_bytes());
    for parent in parents {
        hasher.update(&parent.0);
    }

    hasher.update(&(data.len() as u64).to_le_bytes());
    hasher.update(data);

    hasher.update(&updated_at.to_le_bytes());
    hasher.update(updated_by.as_bytes());

    if let Some(metadata) = metadata {
        hasher.update(&[1u8]);
        hasher.update(&(metadata.len() as u64).to_le_bytes());
        for (key, value) in metadata.iter() {
            hasher.update(&(key.len() as u64).to_le_bytes());
            hasher.update(key.as_bytes());
            hasher.update(&(value.len() as u64).to_le_bytes());
            hasher.update(value.as_bytes());
        }
    } else {
        hasher.update(&[0u8]);
    }

    CommitId(*hasher.finalize().as_bytes())
}

fn metadata_entry_descriptor() -> &'static RowDescriptor {
    static DESCRIPTOR: OnceLock<RowDescriptor> = OnceLock::new();
    DESCRIPTOR.get_or_init(|| {
        RowDescriptor::new(vec![
            ColumnDescriptor::new("key", ColumnType::Text),
            ColumnDescriptor::new("value", ColumnType::Text),
        ])
    })
}

fn row_state_column_type() -> ColumnType {
    ColumnType::Enum {
        variants: vec![
            "staging_pending".to_string(),
            "rejected".to_string(),
            "visible_direct".to_string(),
            "visible_transactional".to_string(),
        ],
    }
}

fn confirmed_tier_column_type() -> ColumnType {
    ColumnType::Enum {
        variants: vec![
            "worker".to_string(),
            "edge".to_string(),
            "global".to_string(),
        ],
    }
}

fn stored_row_version_descriptor() -> &'static RowDescriptor {
    static DESCRIPTOR: OnceLock<RowDescriptor> = OnceLock::new();
    DESCRIPTOR.get_or_init(|| {
        RowDescriptor::new(vec![
            ColumnDescriptor::new("row_id", ColumnType::Uuid),
            ColumnDescriptor::new("version_id", ColumnType::Bytea),
            ColumnDescriptor::new("branch", ColumnType::Text),
            ColumnDescriptor::new(
                "parents",
                ColumnType::Array {
                    element: Box::new(ColumnType::Bytea),
                },
            ),
            ColumnDescriptor::new("updated_at", ColumnType::Timestamp),
            ColumnDescriptor::new("created_by", ColumnType::Text),
            ColumnDescriptor::new("created_at", ColumnType::Timestamp),
            ColumnDescriptor::new("updated_by", ColumnType::Text),
            ColumnDescriptor::new("batch_id", ColumnType::Bytea),
            ColumnDescriptor::new("state", row_state_column_type()),
            ColumnDescriptor::new("confirmed_tier", confirmed_tier_column_type()).nullable(),
            ColumnDescriptor::new("is_deleted", ColumnType::Boolean),
            ColumnDescriptor::new("data", ColumnType::Bytea),
            ColumnDescriptor::new(
                "metadata",
                ColumnType::Array {
                    element: Box::new(ColumnType::Row {
                        columns: Box::new(metadata_entry_descriptor().clone()),
                    }),
                },
            ),
        ])
    })
}

fn visible_row_entry_descriptor() -> &'static RowDescriptor {
    static DESCRIPTOR: OnceLock<RowDescriptor> = OnceLock::new();
    DESCRIPTOR.get_or_init(|| {
        RowDescriptor::new(vec![
            ColumnDescriptor::new(
                "current_row",
                ColumnType::Row {
                    columns: Box::new(stored_row_version_descriptor().clone()),
                },
            ),
            ColumnDescriptor::new(
                "branch_frontier",
                ColumnType::Array {
                    element: Box::new(ColumnType::Bytea),
                },
            ),
            ColumnDescriptor::new("worker_version_id", ColumnType::Bytea).nullable(),
            ColumnDescriptor::new("edge_version_id", ColumnType::Bytea).nullable(),
            ColumnDescriptor::new("global_version_id", ColumnType::Bytea).nullable(),
        ])
    })
}

fn row_state_to_value(state: RowState) -> Value {
    Value::Text(
        match state {
            RowState::StagingPending => "staging_pending",
            RowState::Rejected => "rejected",
            RowState::VisibleDirect => "visible_direct",
            RowState::VisibleTransactional => "visible_transactional",
        }
        .to_string(),
    )
}

fn row_state_from_value(value: &Value) -> Result<RowState, EncodingError> {
    match value {
        Value::Text(value) => match value.as_str() {
            "staging_pending" => Ok(RowState::StagingPending),
            "rejected" => Ok(RowState::Rejected),
            "visible_direct" => Ok(RowState::VisibleDirect),
            "visible_transactional" => Ok(RowState::VisibleTransactional),
            _ => Err(malformed(format!("invalid row state: {value}"))),
        },
        other => Err(malformed(format!("expected row state text, got {other:?}"))),
    }
}

fn durability_tier_to_value(tier: DurabilityTier) -> Value {
    Value::Text(
        match tier {
            DurabilityTier::Worker => "worker",
            DurabilityTier::EdgeServer => "edge",
            DurabilityTier::GlobalServer => "global",
        }
        .to_string(),
    )
}

fn durability_tier_from_value(value: &Value) -> Result<Option<DurabilityTier>, EncodingError> {
    match value {
        Value::Null => Ok(None),
        Value::Text(value) => match value.as_str() {
            "worker" => Ok(Some(DurabilityTier::Worker)),
            "edge" => Ok(Some(DurabilityTier::EdgeServer)),
            "global" => Ok(Some(DurabilityTier::GlobalServer)),
            _ => Err(malformed(format!("invalid durability tier: {value}"))),
        },
        other => Err(malformed(format!(
            "expected durability tier text or null, got {other:?}"
        ))),
    }
}

fn commit_id_to_value(commit_id: CommitId) -> Value {
    Value::Bytea(commit_id.0.to_vec())
}

fn commit_id_from_value(value: &Value) -> Result<CommitId, EncodingError> {
    match value {
        Value::Bytea(bytes) if bytes.len() == 32 => {
            let mut commit_id = [0u8; 32];
            commit_id.copy_from_slice(bytes);
            Ok(CommitId(commit_id))
        }
        Value::Bytea(bytes) => Err(malformed(format!(
            "expected 32-byte commit id, got {} bytes",
            bytes.len()
        ))),
        other => Err(malformed(format!(
            "expected commit id bytes, got {other:?}"
        ))),
    }
}

fn batch_id_to_value(batch_id: BatchId) -> Value {
    Value::Bytea(batch_id.0.as_bytes().to_vec())
}

fn batch_id_from_value(value: &Value) -> Result<BatchId, EncodingError> {
    match value {
        Value::Bytea(bytes) if bytes.len() == 16 => {
            let uuid = Uuid::from_slice(bytes)
                .map_err(|err| malformed(format!("invalid batch id uuid: {err}")))?;
            Ok(BatchId(uuid))
        }
        Value::Bytea(bytes) => Err(malformed(format!(
            "expected 16-byte batch id, got {} bytes",
            bytes.len()
        ))),
        other => Err(malformed(format!("expected batch id bytes, got {other:?}"))),
    }
}

fn optional_commit_id_to_value(commit_id: Option<CommitId>) -> Value {
    commit_id.map(commit_id_to_value).unwrap_or(Value::Null)
}

fn optional_commit_id_from_value(value: &Value) -> Result<Option<CommitId>, EncodingError> {
    match value {
        Value::Null => Ok(None),
        _ => commit_id_from_value(value).map(Some),
    }
}

fn commit_ids_to_value(commit_ids: &[CommitId]) -> Value {
    Value::Array(commit_ids.iter().copied().map(commit_id_to_value).collect())
}

fn commit_ids_from_value(value: &Value, label: &str) -> Result<Vec<CommitId>, EncodingError> {
    let Value::Array(values) = value else {
        return Err(malformed(format!("expected {label} array, got {value:?}")));
    };

    values.iter().map(commit_id_from_value).collect()
}

fn metadata_to_value(metadata: &RowMetadata) -> Value {
    Value::Array(
        metadata
            .iter()
            .map(|(key, value)| Value::Row {
                id: None,
                values: vec![Value::Text(key.to_string()), Value::Text(value.to_string())],
            })
            .collect(),
    )
}

fn metadata_from_value(value: &Value) -> Result<RowMetadata, EncodingError> {
    let Value::Array(entries) = value else {
        return Err(malformed(format!("expected metadata array, got {value:?}")));
    };

    let mut metadata = Vec::with_capacity(entries.len());
    for entry in entries {
        let Value::Row { values, .. } = entry else {
            return Err(malformed(format!(
                "expected metadata row entry, got {entry:?}"
            )));
        };
        if values.len() != 2 {
            return Err(malformed(format!(
                "expected metadata row with 2 fields, got {}",
                values.len()
            )));
        }
        let Value::Text(key) = &values[0] else {
            return Err(malformed(format!(
                "expected metadata key text, got {:?}",
                values[0]
            )));
        };
        let Value::Text(value) = &values[1] else {
            return Err(malformed(format!(
                "expected metadata value text, got {:?}",
                values[1]
            )));
        };
        metadata.push((key.clone(), value.clone()));
    }

    Ok(RowMetadata::from_entries(metadata))
}

fn expect_uuid(value: &Value, label: &str) -> Result<ObjectId, EncodingError> {
    match value {
        Value::Uuid(id) => Ok(*id),
        other => Err(malformed(format!("expected {label} uuid, got {other:?}"))),
    }
}

fn expect_text(value: &Value, label: &str) -> Result<String, EncodingError> {
    match value {
        Value::Text(value) => Ok(value.clone()),
        other => Err(malformed(format!("expected {label} text, got {other:?}"))),
    }
}

fn expect_timestamp(value: &Value, label: &str) -> Result<u64, EncodingError> {
    match value {
        Value::Timestamp(value) => Ok(*value),
        other => Err(malformed(format!(
            "expected {label} timestamp, got {other:?}"
        ))),
    }
}

fn expect_bool(value: &Value, label: &str) -> Result<bool, EncodingError> {
    match value {
        Value::Boolean(value) => Ok(*value),
        other => Err(malformed(format!(
            "expected {label} boolean, got {other:?}"
        ))),
    }
}

fn expect_bytea(value: &Value, label: &str) -> Result<Vec<u8>, EncodingError> {
    match value {
        Value::Bytea(value) => Ok(value.clone()),
        other => Err(malformed(format!("expected {label} bytes, got {other:?}"))),
    }
}

fn stored_row_version_values(row: &StoredRowVersion) -> Vec<Value> {
    vec![
        Value::Uuid(row.row_id),
        commit_id_to_value(row.version_id),
        Value::Text(row.branch.to_string()),
        Value::Array(
            row.parents
                .iter()
                .copied()
                .map(commit_id_to_value)
                .collect(),
        ),
        Value::Timestamp(row.updated_at),
        Value::Text(row.created_by.to_string()),
        Value::Timestamp(row.created_at),
        Value::Text(row.updated_by.to_string()),
        batch_id_to_value(row.batch_id),
        row_state_to_value(row.state),
        row.confirmed_tier
            .map(durability_tier_to_value)
            .unwrap_or(Value::Null),
        Value::Boolean(row.is_deleted),
        Value::Bytea(row.data.to_vec()),
        metadata_to_value(&row.metadata),
    ]
}

fn stored_row_version_from_values(values: &[Value]) -> Result<StoredRowVersion, EncodingError> {
    if values.len() != stored_row_version_descriptor().columns.len() {
        return Err(malformed(format!(
            "expected {} stored row version fields, got {}",
            stored_row_version_descriptor().columns.len(),
            values.len()
        )));
    }

    let parents = match &values[3] {
        Value::Array(values) => values
            .iter()
            .map(commit_id_from_value)
            .collect::<Result<SmallVec<[CommitId; 2]>, _>>()?,
        other => {
            return Err(malformed(format!("expected parents array, got {other:?}")));
        }
    };

    Ok(StoredRowVersion {
        row_id: expect_uuid(&values[0], "row_id")?,
        version_id: commit_id_from_value(&values[1])?,
        branch: expect_text(&values[2], "branch")?.into(),
        parents,
        updated_at: expect_timestamp(&values[4], "updated_at")?,
        created_by: expect_text(&values[5], "created_by")?.into(),
        created_at: expect_timestamp(&values[6], "created_at")?,
        updated_by: expect_text(&values[7], "updated_by")?.into(),
        batch_id: batch_id_from_value(&values[8])?,
        state: row_state_from_value(&values[9])?,
        confirmed_tier: durability_tier_from_value(&values[10])?,
        is_deleted: expect_bool(&values[11], "is_deleted")?,
        data: expect_bytea(&values[12], "data")?.into(),
        metadata: metadata_from_value(&values[13])?,
    })
}

fn stored_row_version_to_value(row: &StoredRowVersion) -> Value {
    Value::Row {
        id: None,
        values: stored_row_version_values(row),
    }
}

fn stored_row_version_from_value(value: &Value) -> Result<StoredRowVersion, EncodingError> {
    let Value::Row { values, .. } = value else {
        return Err(malformed(format!(
            "expected stored row version row, got {value:?}"
        )));
    };

    stored_row_version_from_values(values)
}

pub(crate) fn encode_stored_row_version(row: &StoredRowVersion) -> Result<Vec<u8>, EncodingError> {
    encode_row(
        stored_row_version_descriptor(),
        &stored_row_version_values(row),
    )
}

pub(crate) fn decode_stored_row_version(data: &[u8]) -> Result<StoredRowVersion, EncodingError> {
    let values = decode_row(stored_row_version_descriptor(), data)?;
    stored_row_version_from_values(&values)
}

pub(crate) fn encode_visible_row_entry(entry: &VisibleRowEntry) -> Result<Vec<u8>, EncodingError> {
    encode_row(
        visible_row_entry_descriptor(),
        &[
            stored_row_version_to_value(&entry.current_row),
            commit_ids_to_value(&entry.branch_frontier),
            optional_commit_id_to_value(entry.worker_version_id),
            optional_commit_id_to_value(entry.edge_version_id),
            optional_commit_id_to_value(entry.global_version_id),
        ],
    )
}

pub(crate) fn decode_visible_row_entry(data: &[u8]) -> Result<VisibleRowEntry, EncodingError> {
    let values = decode_row(visible_row_entry_descriptor(), data)?;
    if values.len() != visible_row_entry_descriptor().columns.len() {
        return Err(malformed(format!(
            "expected {} visible row entry fields, got {}",
            visible_row_entry_descriptor().columns.len(),
            values.len()
        )));
    }

    Ok(VisibleRowEntry {
        current_row: stored_row_version_from_value(&values[0])?,
        branch_frontier: commit_ids_from_value(&values[1], "branch_frontier")?,
        worker_version_id: optional_commit_id_from_value(&values[2])?,
        edge_version_id: optional_commit_id_from_value(&values[3])?,
        global_version_id: optional_commit_id_from_value(&values[4])?,
    })
}

pub(crate) fn decode_visible_row_frontier(data: &[u8]) -> Result<Vec<CommitId>, EncodingError> {
    let value = decode_column(visible_row_entry_descriptor(), data, 1)?;
    commit_ids_from_value(&value, "branch_frontier")
}

pub(crate) fn decode_visible_current_row(data: &[u8]) -> Result<StoredRowVersion, EncodingError> {
    let value = decode_column(visible_row_entry_descriptor(), data, 0)?;
    stored_row_version_from_value(&value)
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StoredRowVersion {
    pub row_id: ObjectId,
    pub version_id: CommitId,
    pub branch: SharedString,
    pub parents: SmallVec<[crate::commit::CommitId; 2]>,
    pub updated_at: u64,
    pub created_by: SharedString,
    pub created_at: u64,
    pub updated_by: SharedString,
    pub batch_id: BatchId,
    pub state: RowState,
    pub confirmed_tier: Option<DurabilityTier>,
    pub is_deleted: bool,
    pub data: RowBytes,
    pub metadata: RowMetadata,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct RowMetadata(SmallVec<[(String, String); 4]>);

impl RowMetadata {
    pub fn from_entries(mut entries: Vec<(String, String)>) -> Self {
        entries.sort_by(|(left_key, _), (right_key, _)| left_key.cmp(right_key));
        entries.dedup_by(|(left_key, _), (right_key, _)| left_key == right_key);
        Self(SmallVec::from_vec(entries))
    }

    pub fn from_hash_map(metadata: HashMap<String, String>) -> Self {
        Self::from_entries(metadata.into_iter().collect())
    }

    pub fn get(&self, key: &str) -> Option<&String> {
        self.0
            .iter()
            .find_map(|(entry_key, value)| (entry_key == key).then_some(value))
    }

    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    pub fn len(&self) -> usize {
        self.0.len()
    }

    pub fn iter(&self) -> impl Iterator<Item = (&str, &str)> {
        self.0
            .iter()
            .map(|(key, value)| (key.as_str(), value.as_str()))
    }
}

impl StoredRowVersion {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        row_id: ObjectId,
        branch: impl Into<String>,
        parents: impl IntoIterator<Item = CommitId>,
        data: Vec<u8>,
        provenance: RowProvenance,
        metadata: HashMap<String, String>,
        state: RowState,
        confirmed_tier: Option<DurabilityTier>,
    ) -> Self {
        let is_deleted = metadata
            .get(MetadataKey::Delete.as_str())
            .is_some_and(|value| {
                value == DeleteKind::Soft.as_str() || value == DeleteKind::Hard.as_str()
            });
        let metadata = RowMetadata::from_hash_map(metadata);
        let branch = SharedString::from(branch.into());
        let parents = parents.into_iter().collect::<SmallVec<[CommitId; 2]>>();
        let version_id = compute_row_version_id(
            &branch,
            &parents,
            &data,
            provenance.updated_at,
            &provenance.updated_by,
            (!metadata.is_empty()).then_some(&metadata),
        );

        Self {
            row_id,
            version_id,
            branch,
            parents,
            updated_at: provenance.updated_at,
            created_by: provenance.created_by.into(),
            created_at: provenance.created_at,
            updated_by: provenance.updated_by.into(),
            batch_id: BatchId::from_commit_id(version_id),
            state,
            confirmed_tier,
            is_deleted,
            data: data.into(),
            metadata,
        }
    }

    #[cfg(test)]
    pub fn from_commit(
        row_id: ObjectId,
        branch: impl Into<String>,
        _commit_id: CommitId,
        commit: &Commit,
        state: RowState,
    ) -> Self {
        let provenance = commit
            .row_provenance()
            .unwrap_or_else(|| RowProvenance::for_insert(commit.author.clone(), commit.timestamp));
        let branch = SharedString::from(branch.into());
        let metadata = RowMetadata::from_hash_map(
            commit
                .metadata
                .as_ref()
                .map(|metadata| {
                    metadata
                        .iter()
                        .map(|(key, value)| (key.clone(), value.clone()))
                        .collect()
                })
                .unwrap_or_default(),
        );
        let version_id = compute_row_version_id(
            &branch,
            &commit.parents.iter().copied().collect::<Vec<_>>(),
            &commit.content,
            provenance.updated_at,
            &provenance.updated_by,
            (!metadata.is_empty()).then_some(&metadata),
        );

        Self {
            row_id,
            version_id,
            branch,
            parents: commit.parents.iter().copied().collect(),
            updated_at: provenance.updated_at,
            created_by: provenance.created_by.into(),
            created_at: provenance.created_at,
            updated_by: provenance.updated_by.into(),
            batch_id: BatchId::from_commit_id(version_id),
            state,
            confirmed_tier: commit.ack_state.confirmed_tiers.iter().copied().max(),
            is_deleted: commit.is_soft_deleted() || commit.is_hard_deleted(),
            data: commit.content.clone().into(),
            metadata,
        }
    }

    pub fn row_provenance(&self) -> RowProvenance {
        RowProvenance {
            created_by: self.created_by.to_string(),
            created_at: self.created_at,
            updated_by: self.updated_by.to_string(),
            updated_at: self.updated_at,
        }
    }

    pub fn version_id(&self) -> CommitId {
        self.version_id
    }

    pub fn is_soft_deleted(&self) -> bool {
        self.metadata
            .get(MetadataKey::Delete.as_str())
            .map(|value| value == DeleteKind::Soft.as_str())
            .unwrap_or(false)
    }

    pub fn is_hard_deleted(&self) -> bool {
        self.metadata
            .get(MetadataKey::Delete.as_str())
            .map(|value| value == DeleteKind::Hard.as_str())
            .unwrap_or(false)
            || (self.is_deleted && self.data.is_empty())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct VisibleRowEntry {
    pub current_row: StoredRowVersion,
    pub branch_frontier: Vec<CommitId>,
    pub worker_version_id: Option<CommitId>,
    pub edge_version_id: Option<CommitId>,
    pub global_version_id: Option<CommitId>,
}

impl VisibleRowEntry {
    pub fn new(current_row: StoredRowVersion) -> Self {
        Self {
            branch_frontier: vec![current_row.version_id()],
            current_row,
            worker_version_id: None,
            edge_version_id: None,
            global_version_id: None,
        }
    }

    pub fn rebuild(current_row: StoredRowVersion, history_rows: &[StoredRowVersion]) -> Self {
        let current_version_id = current_row.version_id();
        let branch_frontier = branch_frontier(history_rows);
        let worker = latest_visible_version_for_tier(history_rows, DurabilityTier::Worker);
        let worker_version_id = worker.filter(|version_id| *version_id != current_version_id);

        let edge = latest_visible_version_for_tier(history_rows, DurabilityTier::EdgeServer);
        let edge_version_id = edge.filter(|version_id| *version_id != current_version_id);

        let global = latest_visible_version_for_tier(history_rows, DurabilityTier::GlobalServer);
        let global_version_id = global.filter(|version_id| *version_id != current_version_id);

        Self {
            current_row,
            branch_frontier,
            worker_version_id,
            edge_version_id,
            global_version_id,
        }
    }

    pub fn current_version_id(&self) -> CommitId {
        self.current_row.version_id()
    }

    pub fn version_id_for_tier(&self, tier: DurabilityTier) -> Option<CommitId> {
        let current = self.current_version_id();
        if tier_satisfies(self.current_row.confirmed_tier, tier) {
            return Some(current);
        }

        match tier {
            DurabilityTier::Worker => self.worker_version_id,
            DurabilityTier::EdgeServer => self.edge_version_id,
            DurabilityTier::GlobalServer => self.global_version_id,
        }
    }
}

fn latest_visible_version_for_tier(
    history_rows: &[StoredRowVersion],
    required_tier: DurabilityTier,
) -> Option<CommitId> {
    history_rows
        .iter()
        .filter(|row| row.state.is_visible() && tier_satisfies(row.confirmed_tier, required_tier))
        .max_by_key(|row| (row.updated_at, row.version_id()))
        .map(StoredRowVersion::version_id)
}

fn branch_frontier(history_rows: &[StoredRowVersion]) -> Vec<CommitId> {
    let mut non_tips = std::collections::BTreeSet::new();
    for row in history_rows {
        for parent in &row.parents {
            non_tips.insert(*parent);
        }
    }

    let mut tips: Vec<_> = history_rows
        .iter()
        .map(StoredRowVersion::version_id)
        .filter(|version_id| !non_tips.contains(version_id))
        .collect();
    tips.sort();
    tips.dedup();
    tips
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::metadata::RowProvenance;

    fn visible_row(updated_at: u64, confirmed_tier: Option<DurabilityTier>) -> StoredRowVersion {
        StoredRowVersion::new(
            ObjectId::new(),
            "main",
            Vec::new(),
            vec![updated_at as u8],
            RowProvenance::for_insert("alice".to_string(), updated_at),
            HashMap::new(),
            RowState::VisibleDirect,
            confirmed_tier,
        )
    }

    #[test]
    fn stored_row_version_binary_roundtrips() {
        let row = StoredRowVersion::new(
            ObjectId::from_uuid(Uuid::from_u128(7)),
            "main",
            vec![CommitId([1; 32]), CommitId([2; 32])],
            vec![1, 2, 3, 4],
            RowProvenance {
                created_by: "alice".to_string(),
                created_at: 100,
                updated_by: "bob".to_string(),
                updated_at: 123,
            },
            HashMap::from([
                ("role".to_string(), "admin".to_string()),
                ("source".to_string(), "test".to_string()),
            ]),
            RowState::VisibleTransactional,
            Some(DurabilityTier::EdgeServer),
        );

        let encoded = encode_stored_row_version(&row).expect("encode row version");
        let decoded = decode_stored_row_version(&encoded).expect("decode row version");

        assert_eq!(decoded, row);
    }

    #[test]
    fn visible_row_entry_binary_roundtrips() {
        let global = visible_row(10, Some(DurabilityTier::GlobalServer));
        let current = visible_row(30, Some(DurabilityTier::Worker));
        let entry = VisibleRowEntry {
            current_row: current,
            branch_frontier: vec![global.version_id()],
            worker_version_id: None,
            edge_version_id: Some(global.version_id()),
            global_version_id: Some(global.version_id()),
        };

        let encoded = encode_visible_row_entry(&entry).expect("encode visible row entry");
        let decoded = decode_visible_row_entry(&encoded).expect("decode visible row entry");

        assert_eq!(decoded, entry);
    }

    #[test]
    fn visible_row_entry_omits_tier_pointers_when_current_is_globally_confirmed() {
        let current = visible_row(30, Some(DurabilityTier::GlobalServer));
        let entry = VisibleRowEntry::rebuild(current.clone(), std::slice::from_ref(&current));

        assert_eq!(entry.branch_frontier, vec![current.version_id()]);
        assert_eq!(entry.worker_version_id, None);
        assert_eq!(entry.edge_version_id, None);
        assert_eq!(entry.global_version_id, None);
    }

    #[test]
    fn visible_row_entry_resolves_tier_fallback_chain() {
        let global = visible_row(10, Some(DurabilityTier::GlobalServer));
        let edge = StoredRowVersion::new(
            global.row_id,
            "main",
            vec![global.version_id()],
            vec![2],
            RowProvenance::for_update(&global.row_provenance(), "alice".to_string(), 20),
            HashMap::new(),
            RowState::VisibleDirect,
            Some(DurabilityTier::EdgeServer),
        );
        let current = StoredRowVersion::new(
            global.row_id,
            "main",
            vec![edge.version_id()],
            vec![3],
            RowProvenance::for_update(&edge.row_provenance(), "alice".to_string(), 30),
            HashMap::new(),
            RowState::VisibleDirect,
            Some(DurabilityTier::Worker),
        );
        let history = vec![global.clone(), edge.clone(), current.clone()];

        let entry = VisibleRowEntry::rebuild(current.clone(), &history);

        assert_eq!(entry.branch_frontier, vec![current.version_id()]);
        assert_eq!(entry.worker_version_id, None);
        assert_eq!(entry.edge_version_id, Some(edge.version_id()));
        assert_eq!(entry.global_version_id, Some(global.version_id()));
        assert_eq!(
            entry.version_id_for_tier(DurabilityTier::Worker),
            Some(current.version_id())
        );
        assert_eq!(
            entry.version_id_for_tier(DurabilityTier::EdgeServer),
            Some(edge.version_id())
        );
        assert_eq!(
            entry.version_id_for_tier(DurabilityTier::GlobalServer),
            Some(global.version_id())
        );
    }

    #[test]
    fn visible_row_entry_returns_none_when_no_version_meets_required_tier() {
        let current = visible_row(30, Some(DurabilityTier::Worker));
        let entry = VisibleRowEntry::rebuild(current.clone(), std::slice::from_ref(&current));

        assert_eq!(entry.branch_frontier, vec![current.version_id()]);
        assert_eq!(entry.version_id_for_tier(DurabilityTier::EdgeServer), None);
        assert_eq!(
            entry.version_id_for_tier(DurabilityTier::GlobalServer),
            None
        );
    }

    #[test]
    fn visible_row_entry_preserves_multiple_branch_tips() {
        let base = visible_row(10, Some(DurabilityTier::Worker));
        let left = StoredRowVersion::new(
            base.row_id,
            "main",
            vec![base.version_id()],
            vec![1],
            RowProvenance::for_update(&base.row_provenance(), "alice".to_string(), 20),
            HashMap::new(),
            RowState::VisibleDirect,
            Some(DurabilityTier::Worker),
        );
        let right = StoredRowVersion::new(
            base.row_id,
            "main",
            vec![base.version_id()],
            vec![2],
            RowProvenance::for_update(&base.row_provenance(), "bob".to_string(), 21),
            HashMap::new(),
            RowState::VisibleDirect,
            Some(DurabilityTier::Worker),
        );

        let entry = VisibleRowEntry::rebuild(right.clone(), &[base, left.clone(), right.clone()]);

        assert_eq!(
            entry.branch_frontier,
            vec![left.version_id(), right.version_id()]
        );
    }
}
