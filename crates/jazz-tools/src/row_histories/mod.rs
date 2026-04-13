use std::collections::HashMap;
use std::sync::OnceLock;

use blake3::Hasher;
use serde::{Deserialize, Serialize};
use smallvec::SmallVec;
use uuid::Uuid;

use crate::commit::CommitId;
use crate::metadata::{DeleteKind, MetadataKey, RowProvenance};
use crate::object::{BranchName, ObjectId};
use crate::query_manager::types::{
    ColumnDescriptor, ColumnType, RowBytes, RowDescriptor, SharedString, Value,
};
use crate::row_format::{EncodingError, column_bytes, decode_column, decode_row, encode_row};
use crate::storage::{IndexMutation, RowLocator, Storage, StorageError};
use crate::sync_manager::DurabilityTier;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct BatchId(pub Uuid);

impl BatchId {
    pub fn new() -> Self {
        Self(Uuid::now_v7())
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

/// Visibility change emitted when a row object's winning version changes.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RowVisibilityChange {
    pub object_id: ObjectId,
    pub row_locator: RowLocator,
    pub row: StoredRowVersion,
    pub previous_row: Option<StoredRowVersion>,
    pub is_new_object: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ApplyRowVersionResult {
    pub version_id: CommitId,
    pub row_locator: RowLocator,
    pub visibility_change: Option<RowVisibilityChange>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RowHistoryError {
    ObjectNotFound(ObjectId),
    ParentNotFound(CommitId),
    StorageError(StorageError),
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

fn delete_kind_column_type() -> ColumnType {
    ColumnType::Enum {
        variants: vec!["soft".to_string(), "hard".to_string()],
    }
}

fn flat_history_row_format_id() -> ObjectId {
    ObjectId::from_uuid(Uuid::from_bytes([
        0x6a, 0x61, 0x7a, 0x7a, 0x2d, 0x68, 0x69, 0x73, 0x74, 0x2d, 0x66, 0x6c, 0x61, 0x74, 0x2d,
        0x31,
    ]))
}

fn flat_history_row_marker_descriptor() -> &'static RowDescriptor {
    static DESCRIPTOR: OnceLock<RowDescriptor> = OnceLock::new();
    DESCRIPTOR.get_or_init(|| {
        RowDescriptor::new(vec![ColumnDescriptor::new(
            "_jazz_format_id",
            ColumnType::Uuid,
        )])
    })
}

fn flat_history_row_identity_descriptor() -> &'static RowDescriptor {
    static DESCRIPTOR: OnceLock<RowDescriptor> = OnceLock::new();
    DESCRIPTOR.get_or_init(|| {
        RowDescriptor::new(vec![
            ColumnDescriptor::new("_jazz_format_id", ColumnType::Uuid),
            ColumnDescriptor::new("_jazz_row_id", ColumnType::Uuid),
        ])
    })
}

fn flat_visible_row_format_id() -> ObjectId {
    ObjectId::from_uuid(Uuid::from_bytes([
        0x6a, 0x61, 0x7a, 0x7a, 0x2d, 0x76, 0x69, 0x73, 0x2d, 0x66, 0x6c, 0x61, 0x74, 0x2d, 0x31,
        0x00,
    ]))
}

fn flat_visible_row_marker_descriptor() -> &'static RowDescriptor {
    static DESCRIPTOR: OnceLock<RowDescriptor> = OnceLock::new();
    DESCRIPTOR.get_or_init(|| {
        RowDescriptor::new(vec![ColumnDescriptor::new(
            "_jazz_format_id",
            ColumnType::Uuid,
        )])
    })
}

fn flat_visible_row_identity_descriptor() -> &'static RowDescriptor {
    static DESCRIPTOR: OnceLock<RowDescriptor> = OnceLock::new();
    DESCRIPTOR.get_or_init(|| {
        RowDescriptor::new(vec![
            ColumnDescriptor::new("_jazz_format_id", ColumnType::Uuid),
            ColumnDescriptor::new("_jazz_row_id", ColumnType::Uuid),
        ])
    })
}

fn history_row_system_columns() -> Vec<ColumnDescriptor> {
    vec![
        ColumnDescriptor::new("_jazz_format_id", ColumnType::Uuid),
        ColumnDescriptor::new("_jazz_row_id", ColumnType::Uuid),
        ColumnDescriptor::new("_jazz_version_id", ColumnType::Bytea),
        ColumnDescriptor::new("_jazz_branch", ColumnType::Text),
        ColumnDescriptor::new(
            "_jazz_parents",
            ColumnType::Array {
                element: Box::new(ColumnType::Bytea),
            },
        ),
        ColumnDescriptor::new("_jazz_updated_at", ColumnType::Timestamp),
        ColumnDescriptor::new("_jazz_created_by", ColumnType::Text),
        ColumnDescriptor::new("_jazz_created_at", ColumnType::Timestamp),
        ColumnDescriptor::new("_jazz_updated_by", ColumnType::Text),
        ColumnDescriptor::new("_jazz_batch_id", ColumnType::Bytea),
        ColumnDescriptor::new("_jazz_state", row_state_column_type()),
        ColumnDescriptor::new("_jazz_confirmed_tier", confirmed_tier_column_type()).nullable(),
        ColumnDescriptor::new("_jazz_delete_kind", delete_kind_column_type()).nullable(),
        ColumnDescriptor::new("_jazz_is_deleted", ColumnType::Boolean),
        ColumnDescriptor::new(
            "_jazz_metadata",
            ColumnType::Array {
                element: Box::new(ColumnType::Row {
                    columns: Box::new(metadata_entry_descriptor().clone()),
                }),
            },
        ),
    ]
}

fn stored_row_system_values(row: &StoredRowVersion, format_id: ObjectId) -> Vec<Value> {
    vec![
        Value::Uuid(format_id),
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
        row.delete_kind
            .map(delete_kind_to_value)
            .unwrap_or(Value::Null),
        Value::Boolean(row.is_deleted),
        metadata_to_value(&row.metadata),
    ]
}

fn history_row_system_values(row: &StoredRowVersion) -> Vec<Value> {
    stored_row_system_values(row, flat_history_row_format_id())
}

fn history_row_system_column_count() -> usize {
    history_row_system_columns().len()
}

pub(crate) fn is_flat_history_row(data: &[u8]) -> bool {
    matches!(
        column_bytes(flat_history_row_marker_descriptor(), data, 0),
        Ok(Some(bytes)) if bytes == flat_history_row_format_id().uuid().as_bytes()
    )
}

pub(crate) fn flat_history_row_id(data: &[u8]) -> Result<ObjectId, EncodingError> {
    let value = decode_column(flat_history_row_identity_descriptor(), data, 1)?;
    expect_uuid(&value, "row_id")
}

fn visible_row_system_columns() -> Vec<ColumnDescriptor> {
    let mut columns = history_row_system_columns();
    columns.extend([
        ColumnDescriptor::new(
            "_jazz_branch_frontier",
            ColumnType::Array {
                element: Box::new(ColumnType::Bytea),
            },
        ),
        ColumnDescriptor::new("_jazz_worker_version_id", ColumnType::Bytea).nullable(),
        ColumnDescriptor::new("_jazz_edge_version_id", ColumnType::Bytea).nullable(),
        ColumnDescriptor::new("_jazz_global_version_id", ColumnType::Bytea).nullable(),
    ]);
    columns
}

fn visible_row_system_values(entry: &VisibleRowEntry) -> Vec<Value> {
    let mut values = stored_row_system_values(&entry.current_row, flat_visible_row_format_id());
    values.extend([
        commit_ids_to_value(&entry.branch_frontier),
        optional_commit_id_to_value(entry.worker_version_id),
        optional_commit_id_to_value(entry.edge_version_id),
        optional_commit_id_to_value(entry.global_version_id),
    ]);
    values
}

fn visible_row_system_column_count() -> usize {
    visible_row_system_columns().len()
}

pub(crate) fn is_flat_visible_row(data: &[u8]) -> bool {
    matches!(
        column_bytes(flat_visible_row_marker_descriptor(), data, 0),
        Ok(Some(bytes)) if bytes == flat_visible_row_format_id().uuid().as_bytes()
    )
}

pub(crate) fn flat_visible_row_id(data: &[u8]) -> Result<ObjectId, EncodingError> {
    let value = decode_column(flat_visible_row_identity_descriptor(), data, 1)?;
    expect_uuid(&value, "row_id")
}

/// Build the physical row descriptor used when row-history state is stored as a
/// single flat row: reserved Jazz columns first, followed by the table's user
/// columns as nullable storage columns.
pub fn history_row_physical_descriptor(user_descriptor: &RowDescriptor) -> RowDescriptor {
    let mut columns = history_row_system_columns();
    columns.extend(user_descriptor.columns.iter().cloned().map(|mut column| {
        column.nullable = true;
        column
    }));
    RowDescriptor::new(columns)
}

pub fn visible_row_physical_descriptor(user_descriptor: &RowDescriptor) -> RowDescriptor {
    let mut columns = visible_row_system_columns();
    columns.extend(user_descriptor.columns.iter().cloned().map(|mut column| {
        column.nullable = true;
        column
    }));
    RowDescriptor::new(columns)
}

fn flat_user_values(
    user_descriptor: &RowDescriptor,
    data: &RowBytes,
) -> Result<Vec<Value>, EncodingError> {
    if data.is_empty() {
        Ok(user_descriptor
            .columns
            .iter()
            .map(|_| Value::Null)
            .collect::<Vec<_>>())
    } else {
        decode_row(user_descriptor, data)
    }
}

fn flat_user_data_from_values(
    user_descriptor: &RowDescriptor,
    user_values: &[Value],
    delete_kind: Option<DeleteKind>,
    is_deleted: bool,
) -> Result<Vec<u8>, EncodingError> {
    if delete_kind == Some(DeleteKind::Hard)
        || (is_deleted && user_values.iter().all(Value::is_null))
    {
        Ok(Vec::new())
    } else {
        encode_row(user_descriptor, user_values)
    }
}

fn stored_row_version_from_flat_parts(
    user_descriptor: &RowDescriptor,
    system_values: &[Value],
    user_values: &[Value],
) -> Result<StoredRowVersion, EncodingError> {
    let delete_kind = delete_kind_from_value(&system_values[12])?;
    let is_deleted = expect_bool(&system_values[13], "is_deleted")?;
    let user_data =
        flat_user_data_from_values(user_descriptor, user_values, delete_kind, is_deleted)?;

    let parents = match &system_values[4] {
        Value::Array(values) => values
            .iter()
            .map(commit_id_from_value)
            .collect::<Result<SmallVec<[CommitId; 2]>, _>>()?,
        other => {
            return Err(malformed(format!("expected parents array, got {other:?}")));
        }
    };

    Ok(StoredRowVersion {
        row_id: expect_uuid(&system_values[1], "row_id")?,
        version_id: commit_id_from_value(&system_values[2])?,
        branch: expect_text(&system_values[3], "branch")?.into(),
        parents,
        updated_at: expect_timestamp(&system_values[5], "updated_at")?,
        created_by: expect_text(&system_values[6], "created_by")?.into(),
        created_at: expect_timestamp(&system_values[7], "created_at")?,
        updated_by: expect_text(&system_values[8], "updated_by")?.into(),
        batch_id: batch_id_from_value(&system_values[9])?,
        state: row_state_from_value(&system_values[10])?,
        confirmed_tier: durability_tier_from_value(&system_values[11])?,
        delete_kind,
        is_deleted,
        data: user_data.into(),
        metadata: metadata_from_value(&system_values[14])?,
    })
}

/// Encode a row-history version into a single flat physical row.
pub fn encode_flat_history_row(
    user_descriptor: &RowDescriptor,
    row: &StoredRowVersion,
) -> Result<Vec<u8>, EncodingError> {
    let mut values = history_row_system_values(row);
    values.extend(flat_user_values(user_descriptor, &row.data)?);

    encode_row(&history_row_physical_descriptor(user_descriptor), &values)
}

/// Decode a flat physical row back into the current `StoredRowVersion` shape.
pub fn decode_flat_history_row(
    user_descriptor: &RowDescriptor,
    data: &[u8],
) -> Result<StoredRowVersion, EncodingError> {
    let descriptor = history_row_physical_descriptor(user_descriptor);
    let values = decode_row(&descriptor, data)?;
    let (system_values, user_values) = values.split_at(history_row_system_column_count());
    stored_row_version_from_flat_parts(user_descriptor, system_values, user_values)
}

pub fn encode_flat_visible_row_entry(
    user_descriptor: &RowDescriptor,
    entry: &VisibleRowEntry,
) -> Result<Vec<u8>, EncodingError> {
    let mut values = visible_row_system_values(entry);
    values.extend(flat_user_values(user_descriptor, &entry.current_row.data)?);
    encode_row(&visible_row_physical_descriptor(user_descriptor), &values)
}

pub fn decode_flat_visible_row_entry(
    user_descriptor: &RowDescriptor,
    data: &[u8],
) -> Result<VisibleRowEntry, EncodingError> {
    let descriptor = visible_row_physical_descriptor(user_descriptor);
    let values = decode_row(&descriptor, data)?;
    let system_count = history_row_system_column_count();
    let visible_system_count = visible_row_system_column_count();
    let current_row = stored_row_version_from_flat_parts(
        user_descriptor,
        &values[..system_count],
        &values[visible_system_count..],
    )?;

    Ok(VisibleRowEntry {
        current_row,
        branch_frontier: commit_ids_from_value(&values[system_count], "branch_frontier")?,
        worker_version_id: optional_commit_id_from_value(&values[system_count + 1])?,
        edge_version_id: optional_commit_id_from_value(&values[system_count + 2])?,
        global_version_id: optional_commit_id_from_value(&values[system_count + 3])?,
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

fn delete_kind_to_value(kind: DeleteKind) -> Value {
    Value::Text(kind.as_str().to_string())
}

fn delete_kind_from_value(value: &Value) -> Result<Option<DeleteKind>, EncodingError> {
    match value {
        Value::Null => Ok(None),
        Value::Text(value) => match value.as_str() {
            "soft" => Ok(Some(DeleteKind::Soft)),
            "hard" => Ok(Some(DeleteKind::Hard)),
            _ => Err(malformed(format!("invalid delete kind: {value}"))),
        },
        other => Err(malformed(format!(
            "expected delete kind text or null, got {other:?}"
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

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct QueryRowVersion {
    pub version_id: CommitId,
    pub branch: SharedString,
    pub updated_at: u64,
    pub created_by: SharedString,
    pub created_at: u64,
    pub updated_by: SharedString,
    pub batch_id: BatchId,
    pub state: RowState,
    pub delete_kind: Option<DeleteKind>,
    pub data: RowBytes,
}

impl QueryRowVersion {
    pub fn version_id(&self) -> CommitId {
        self.version_id
    }

    pub fn row_provenance(&self) -> RowProvenance {
        RowProvenance {
            created_by: self.created_by.to_string(),
            created_at: self.created_at,
            updated_by: self.updated_by.to_string(),
            updated_at: self.updated_at,
        }
    }

    pub fn is_soft_deleted(&self) -> bool {
        self.delete_kind == Some(DeleteKind::Soft)
    }

    pub fn is_hard_deleted(&self) -> bool {
        self.delete_kind == Some(DeleteKind::Hard)
    }
}

impl From<&StoredRowVersion> for QueryRowVersion {
    fn from(row: &StoredRowVersion) -> Self {
        Self {
            version_id: row.version_id(),
            branch: row.branch.clone(),
            updated_at: row.updated_at,
            created_by: row.created_by.clone(),
            created_at: row.created_at,
            updated_by: row.updated_by.clone(),
            batch_id: row.batch_id,
            state: row.state,
            delete_kind: row.delete_kind,
            data: row.data.clone(),
        }
    }
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
    pub delete_kind: Option<DeleteKind>,
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

fn delete_kind_from_metadata(metadata: &HashMap<String, String>) -> Option<DeleteKind> {
    match metadata
        .get(MetadataKey::Delete.as_str())
        .map(String::as_str)
    {
        Some("soft") => Some(DeleteKind::Soft),
        Some("hard") => Some(DeleteKind::Hard),
        _ => None,
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
        Self::new_with_batch_id(
            BatchId::new(),
            row_id,
            branch,
            parents,
            data,
            provenance,
            metadata,
            state,
            confirmed_tier,
        )
    }

    #[allow(clippy::too_many_arguments)]
    pub fn new_with_batch_id(
        batch_id: BatchId,
        row_id: ObjectId,
        branch: impl Into<String>,
        parents: impl IntoIterator<Item = CommitId>,
        data: Vec<u8>,
        provenance: RowProvenance,
        metadata: HashMap<String, String>,
        state: RowState,
        confirmed_tier: Option<DurabilityTier>,
    ) -> Self {
        let delete_kind = delete_kind_from_metadata(&metadata);
        let is_deleted = delete_kind.is_some();
        let metadata = RowMetadata::from_hash_map(
            metadata
                .into_iter()
                .filter(|(key, _)| key != MetadataKey::Delete.as_str())
                .collect(),
        );
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
            batch_id,
            state,
            confirmed_tier,
            delete_kind,
            is_deleted,
            data: data.into(),
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
        self.delete_kind == Some(DeleteKind::Soft)
    }

    pub fn is_hard_deleted(&self) -> bool {
        self.delete_kind == Some(DeleteKind::Hard) || (self.is_deleted && self.data.is_empty())
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

#[derive(Debug, Clone)]
struct RowVersionApply {
    row_locator: RowLocator,
    previous_visible: Option<StoredRowVersion>,
    current_visible: Option<StoredRowVersion>,
    is_new_object: bool,
    visible_changed: bool,
}

fn row_locator_from_storage<H: Storage>(
    io: &H,
    object_id: ObjectId,
) -> Result<RowLocator, RowHistoryError> {
    io.load_row_locator(object_id)
        .map_err(RowHistoryError::StorageError)?
        .ok_or(RowHistoryError::ObjectNotFound(object_id))
}

fn load_branch_history<H: Storage>(
    io: &H,
    table: &str,
    object_id: ObjectId,
    branch_name: &SharedString,
) -> Result<Vec<StoredRowVersion>, RowHistoryError> {
    io.scan_history_region(
        table,
        branch_name.as_str(),
        HistoryScan::Row { row_id: object_id },
    )
    .map_err(RowHistoryError::StorageError)
}

fn rebuild_visible_entry_from_history<H: Storage>(
    io: &H,
    table: &str,
    object_id: ObjectId,
    branch_name: &SharedString,
) -> Result<Option<VisibleRowEntry>, RowHistoryError> {
    let history_rows = load_branch_history(io, table, object_id, branch_name)?;
    Ok(visible_entry_from_history_rows(&history_rows))
}

fn visible_entry_from_history_rows(history_rows: &[StoredRowVersion]) -> Option<VisibleRowEntry> {
    let current_row = history_rows
        .iter()
        .filter(|row| row.state.is_visible())
        .max_by_key(|row| (row.updated_at, row.version_id()))
        .cloned()?;

    Some(VisibleRowEntry::rebuild(current_row, history_rows))
}

fn load_previous_visible_entry<H: Storage>(
    io: &H,
    table: &str,
    object_id: ObjectId,
    branch_name: &SharedString,
) -> Result<Option<VisibleRowEntry>, RowHistoryError> {
    match io.load_visible_region_entry(table, branch_name.as_str(), object_id) {
        Ok(entry) => Ok(entry),
        Err(_) => rebuild_visible_entry_from_history(io, table, object_id, branch_name),
    }
}

fn latest_row_wins(candidate: &StoredRowVersion, current: &StoredRowVersion) -> bool {
    (candidate.updated_at, candidate.version_id()) > (current.updated_at, current.version_id())
}

fn branch_frontier_after_append(
    previous_frontier: &[CommitId],
    appended_row: &StoredRowVersion,
) -> Vec<CommitId> {
    let appended_version_id = appended_row.version_id();
    let mut frontier = previous_frontier
        .iter()
        .copied()
        .filter(|version_id| !appended_row.parents.contains(version_id))
        .collect::<Vec<_>>();
    if !frontier.contains(&appended_version_id) {
        frontier.push(appended_version_id);
    }
    frontier.sort();
    frontier.dedup();
    frontier
}

fn latest_visible_version_after_append<H: Storage>(
    io: &H,
    table: &str,
    appended_row: &StoredRowVersion,
    previous_winner_id: Option<CommitId>,
) -> Result<Option<CommitId>, RowHistoryError> {
    if !appended_row.state.is_visible() {
        return Ok(previous_winner_id);
    }

    let appended_version_id = appended_row.version_id();
    let Some(previous_winner_id) = previous_winner_id else {
        return Ok(Some(appended_version_id));
    };

    if previous_winner_id == appended_version_id {
        return Ok(Some(appended_version_id));
    }

    let Some(previous_winner) = io
        .load_history_row_version(
            table,
            appended_row.branch.as_str(),
            appended_row.row_id,
            previous_winner_id,
        )
        .map_err(RowHistoryError::StorageError)?
    else {
        return Err(RowHistoryError::StorageError(StorageError::IoError(
            format!(
                "missing history row {previous_winner_id:?} for row {}",
                appended_row.row_id
            ),
        )));
    };

    if latest_row_wins(appended_row, &previous_winner) {
        Ok(Some(appended_version_id))
    } else {
        Ok(Some(previous_winner_id))
    }
}

fn visible_entry_after_append<H: Storage>(
    io: &H,
    table: &str,
    previous_entry: Option<&VisibleRowEntry>,
    appended_row: &StoredRowVersion,
) -> Result<Option<VisibleRowEntry>, RowHistoryError> {
    let Some(previous_entry) = previous_entry else {
        return Ok(appended_row
            .state
            .is_visible()
            .then(|| VisibleRowEntry::new(appended_row.clone())));
    };

    let branch_frontier =
        branch_frontier_after_append(&previous_entry.branch_frontier, appended_row);

    if !appended_row.state.is_visible() {
        let mut next = previous_entry.clone();
        next.branch_frontier = branch_frontier;
        return Ok(Some(next));
    }

    let current_row = if latest_row_wins(appended_row, &previous_entry.current_row) {
        appended_row.clone()
    } else {
        previous_entry.current_row.clone()
    };
    let current_version_id = current_row.version_id();

    let worker_version_id = latest_visible_version_after_append(
        io,
        table,
        appended_row,
        previous_entry.version_id_for_tier(DurabilityTier::Worker),
    )?
    .filter(|version_id| *version_id != current_version_id);
    let edge_version_id = latest_visible_version_after_append(
        io,
        table,
        appended_row,
        previous_entry.version_id_for_tier(DurabilityTier::EdgeServer),
    )?
    .filter(|version_id| *version_id != current_version_id);
    let global_version_id = latest_visible_version_after_append(
        io,
        table,
        appended_row,
        previous_entry.version_id_for_tier(DurabilityTier::GlobalServer),
    )?
    .filter(|version_id| *version_id != current_version_id);

    Ok(Some(VisibleRowEntry {
        current_row,
        branch_frontier,
        worker_version_id,
        edge_version_id,
        global_version_id,
    }))
}

fn winner_after_tier_upgrade<H: Storage>(
    io: &H,
    table: &str,
    entry: &VisibleRowEntry,
    current_row: &StoredRowVersion,
    patched_row: &StoredRowVersion,
    required_tier: DurabilityTier,
) -> Result<Option<CommitId>, RowHistoryError> {
    let patched_version_id = patched_row.version_id();
    if !patched_row.state.is_visible()
        || patched_row
            .confirmed_tier
            .is_none_or(|tier| tier < required_tier)
    {
        return Ok(entry.version_id_for_tier(required_tier));
    }

    if current_row.version_id() == patched_version_id
        || current_row
            .confirmed_tier
            .is_some_and(|tier| tier >= required_tier)
    {
        return Ok(Some(current_row.version_id()));
    }

    let Some(previous_winner_id) = entry.version_id_for_tier(required_tier) else {
        return Ok(Some(patched_version_id));
    };

    if previous_winner_id == patched_version_id {
        return Ok(Some(patched_version_id));
    }

    let Some(previous_winner) = io
        .load_history_row_version(
            table,
            patched_row.branch.as_str(),
            patched_row.row_id,
            previous_winner_id,
        )
        .map_err(RowHistoryError::StorageError)?
    else {
        return Err(RowHistoryError::StorageError(StorageError::IoError(
            format!(
                "missing tier winner {previous_winner_id:?} for row {}",
                patched_row.row_id
            ),
        )));
    };

    if latest_row_wins(patched_row, &previous_winner) {
        Ok(Some(patched_version_id))
    } else {
        Ok(Some(previous_winner_id))
    }
}

fn visible_entry_after_tier_upgrade<H: Storage>(
    io: &H,
    table: &str,
    entry: VisibleRowEntry,
    patched_row: &StoredRowVersion,
) -> Result<VisibleRowEntry, RowHistoryError> {
    let current_row = if entry.current_row.version_id() == patched_row.version_id() {
        patched_row.clone()
    } else {
        entry.current_row.clone()
    };
    let current_version_id = current_row.version_id();

    let worker_version_id = winner_after_tier_upgrade(
        io,
        table,
        &entry,
        &current_row,
        patched_row,
        DurabilityTier::Worker,
    )?
    .filter(|version_id| *version_id != current_version_id);
    let edge_version_id = winner_after_tier_upgrade(
        io,
        table,
        &entry,
        &current_row,
        patched_row,
        DurabilityTier::EdgeServer,
    )?
    .filter(|version_id| *version_id != current_version_id);
    let global_version_id = winner_after_tier_upgrade(
        io,
        table,
        &entry,
        &current_row,
        patched_row,
        DurabilityTier::GlobalServer,
    )?
    .filter(|version_id| *version_id != current_version_id);

    Ok(VisibleRowEntry {
        current_row,
        branch_frontier: entry.branch_frontier,
        worker_version_id,
        edge_version_id,
        global_version_id,
    })
}

fn visibility_change_from_applied(
    object_id: ObjectId,
    applied: RowVersionApply,
) -> Option<RowVisibilityChange> {
    if !applied.visible_changed {
        return None;
    }

    let current_visible = applied.current_visible?;
    Some(RowVisibilityChange {
        object_id,
        row_locator: applied.row_locator,
        row: current_visible,
        previous_row: applied.previous_visible,
        is_new_object: applied.is_new_object,
    })
}

pub fn apply_row_version<H: Storage>(
    io: &mut H,
    object_id: ObjectId,
    branch_name: &BranchName,
    row: StoredRowVersion,
    index_mutations: &[IndexMutation<'_>],
) -> Result<ApplyRowVersionResult, RowHistoryError> {
    let row_locator = row_locator_from_storage(io, object_id)?;
    let table = row_locator.table.to_string();
    let version_id = row.version_id();
    let branch = SharedString::from(branch_name.as_str().to_string());
    let previous_entry = load_previous_visible_entry(io, &table, object_id, &branch)?;
    let previous_visible = previous_entry
        .as_ref()
        .map(|entry| entry.current_row.clone());

    for parent in &row.parents {
        if io
            .load_history_row_version(&table, branch_name.as_str(), object_id, *parent)
            .map_err(RowHistoryError::StorageError)?
            .is_none()
        {
            return Err(RowHistoryError::ParentNotFound(*parent));
        }
    }

    if io
        .load_history_row_version(&table, branch_name.as_str(), object_id, version_id)
        .map_err(RowHistoryError::StorageError)?
        .is_some()
    {
        return Ok(ApplyRowVersionResult {
            version_id,
            row_locator,
            visibility_change: None,
        });
    }

    let current_entry = visible_entry_after_append(io, &table, previous_entry.as_ref(), &row)?;
    let current_visible = current_entry
        .as_ref()
        .map(|entry| entry.current_row.clone());
    let visible_entry_changed = current_entry.as_ref() != previous_entry.as_ref();
    let visible_entries: &[VisibleRowEntry] = match (visible_entry_changed, current_entry.as_ref())
    {
        (true, Some(entry)) => std::slice::from_ref(entry),
        _ => &[],
    };
    let visible_changed = previous_visible != current_visible;
    io.apply_row_mutation(
        &table,
        std::slice::from_ref(&row),
        visible_entries,
        index_mutations,
    )
    .map_err(RowHistoryError::StorageError)?;

    let applied = RowVersionApply {
        row_locator: row_locator.clone(),
        previous_visible: previous_visible.clone(),
        current_visible,
        is_new_object: previous_visible.is_none(),
        visible_changed,
    };

    Ok(ApplyRowVersionResult {
        version_id,
        row_locator,
        visibility_change: visibility_change_from_applied(object_id, applied),
    })
}

pub fn patch_row_version_state<H: Storage>(
    io: &mut H,
    object_id: ObjectId,
    branch_name: &BranchName,
    version_id: CommitId,
    state: Option<RowState>,
    confirmed_tier: Option<DurabilityTier>,
) -> Result<Option<RowVisibilityChange>, RowHistoryError> {
    let row_locator = row_locator_from_storage(io, object_id)?;
    let table = row_locator.table.to_string();
    let branch = SharedString::from(branch_name.as_str().to_string());
    let previous_entry = load_previous_visible_entry(io, &table, object_id, &branch)?;
    let previous_visible = previous_entry
        .as_ref()
        .map(|entry| entry.current_row.clone());

    let mut patched_row = io
        .load_history_row_version(&table, branch_name.as_str(), object_id, version_id)
        .map_err(RowHistoryError::StorageError)?
        .ok_or(RowHistoryError::ObjectNotFound(object_id))?;
    if patched_row.branch.as_str() != branch_name.as_str() {
        return Ok(None);
    }

    if let Some(state) = state {
        patched_row.state = state;
    }
    patched_row.confirmed_tier = match (patched_row.confirmed_tier, confirmed_tier) {
        (Some(existing), Some(incoming)) => Some(existing.max(incoming)),
        (Some(existing), None) => Some(existing),
        (None, incoming) => incoming,
    };

    let patched_entry = match previous_entry {
        Some(entry) if state.is_none() => Some(visible_entry_after_tier_upgrade(
            io,
            &table,
            entry,
            &patched_row,
        )?),
        _ => {
            let mut history_rows = load_branch_history(io, &table, object_id, &branch)?;
            let Some(existing) = history_rows
                .iter_mut()
                .find(|candidate| candidate.version_id() == version_id)
            else {
                return Err(RowHistoryError::ObjectNotFound(object_id));
            };
            *existing = patched_row.clone();
            visible_entry_from_history_rows(&history_rows)
        }
    };
    let visible_entries: Vec<_> = patched_entry.iter().cloned().collect();
    io.apply_row_mutation(
        &table,
        std::slice::from_ref(&patched_row),
        &visible_entries,
        &[],
    )
    .map_err(RowHistoryError::StorageError)?;

    let current_visible = patched_entry
        .as_ref()
        .map(|entry| entry.current_row.clone());
    if previous_visible == current_visible {
        return Ok(None);
    }

    let Some(current_visible) = current_visible else {
        return Ok(None);
    };

    Ok(Some(RowVisibilityChange {
        object_id,
        row_locator,
        row: current_visible,
        previous_row: previous_visible.clone(),
        is_new_object: previous_visible.is_none(),
    }))
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
    use crate::row_format::decode_row;

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
    fn flat_visible_row_binary_roundtrips_user_and_system_columns() {
        let user_descriptor = RowDescriptor::new(vec![
            ColumnDescriptor::new("title", ColumnType::Text),
            ColumnDescriptor::new("done", ColumnType::Boolean).nullable(),
        ]);
        let global = StoredRowVersion::new(
            ObjectId::from_uuid(Uuid::from_u128(21)),
            "main",
            Vec::new(),
            encode_row(
                &user_descriptor,
                &[Value::Text("ship it".into()), Value::Boolean(true)],
            )
            .expect("encode global row"),
            RowProvenance::for_insert("alice".to_string(), 10),
            HashMap::from([("source".to_string(), "global".to_string())]),
            RowState::VisibleDirect,
            Some(DurabilityTier::GlobalServer),
        );
        let current = StoredRowVersion::new(
            global.row_id,
            "main",
            vec![global.version_id()],
            encode_row(
                &user_descriptor,
                &[Value::Text("ship it".into()), Value::Boolean(false)],
            )
            .expect("encode current row"),
            RowProvenance::for_update(&global.row_provenance(), "bob".to_string(), 30),
            HashMap::from([("source".to_string(), "worker".to_string())]),
            RowState::VisibleDirect,
            Some(DurabilityTier::Worker),
        );
        let entry = VisibleRowEntry {
            current_row: current,
            branch_frontier: vec![global.version_id()],
            worker_version_id: None,
            edge_version_id: Some(global.version_id()),
            global_version_id: Some(global.version_id()),
        };

        let encoded =
            encode_flat_visible_row_entry(&user_descriptor, &entry).expect("encode flat visible");
        let decoded =
            decode_flat_visible_row_entry(&user_descriptor, &encoded).expect("decode flat visible");

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

    fn user_descriptor() -> RowDescriptor {
        RowDescriptor::new(vec![
            ColumnDescriptor::new("title", ColumnType::Text),
            ColumnDescriptor::new("done", ColumnType::Boolean),
        ])
    }

    #[test]
    fn history_row_physical_descriptor_appends_nullable_user_columns() {
        let descriptor = history_row_physical_descriptor(&user_descriptor());

        let title = descriptor
            .column("title")
            .expect("physical descriptor should contain title");
        assert!(title.nullable, "physical user columns should be nullable");

        let done = descriptor
            .column("done")
            .expect("physical descriptor should contain done");
        assert!(done.nullable, "physical user columns should be nullable");
    }

    #[test]
    fn flat_history_row_binary_roundtrips_user_and_system_columns() {
        let user_descriptor = user_descriptor();
        let user_values = vec![Value::Text("Write docs".into()), Value::Boolean(false)];
        let user_data = crate::row_format::encode_row(&user_descriptor, &user_values).unwrap();
        let row = StoredRowVersion::new(
            ObjectId::from_uuid(Uuid::from_u128(42)),
            "main",
            vec![CommitId([9; 32])],
            user_data.clone(),
            RowProvenance {
                created_by: "alice".to_string(),
                created_at: 100,
                updated_by: "bob".to_string(),
                updated_at: 123,
            },
            HashMap::from([("source".to_string(), "test".to_string())]),
            RowState::VisibleTransactional,
            Some(DurabilityTier::EdgeServer),
        );

        let encoded =
            encode_flat_history_row(&user_descriptor, &row).expect("encode flat history row");
        let decoded =
            decode_flat_history_row(&user_descriptor, &encoded).expect("decode flat history row");

        assert_eq!(decoded, row);

        let physical_descriptor = history_row_physical_descriptor(&user_descriptor);
        let physical_values = decode_row(&physical_descriptor, &encoded).expect("decode values");
        assert_eq!(
            physical_values[physical_descriptor.column_index("title").unwrap()],
            Value::Text("Write docs".into())
        );
        assert_eq!(
            physical_values[physical_descriptor.column_index("done").unwrap()],
            Value::Boolean(false)
        );
    }

    #[test]
    fn flat_history_row_hard_delete_uses_null_user_columns() {
        let user_descriptor = user_descriptor();
        let deleted = StoredRowVersion::new(
            ObjectId::from_uuid(Uuid::from_u128(43)),
            "main",
            vec![CommitId([7; 32])],
            vec![],
            RowProvenance::for_insert("alice".to_string(), 100),
            HashMap::from([(
                crate::metadata::MetadataKey::Delete.to_string(),
                "hard".to_string(),
            )]),
            RowState::VisibleDirect,
            None,
        );

        let encoded =
            encode_flat_history_row(&user_descriptor, &deleted).expect("encode hard delete");
        let physical_descriptor = history_row_physical_descriptor(&user_descriptor);
        let physical_values = decode_row(&physical_descriptor, &encoded).expect("decode values");

        assert_eq!(
            physical_values[physical_descriptor.column_index("title").unwrap()],
            Value::Null
        );
        assert_eq!(
            physical_values[physical_descriptor.column_index("done").unwrap()],
            Value::Null
        );

        let decoded =
            decode_flat_history_row(&user_descriptor, &encoded).expect("decode hard delete");
        assert_eq!(decoded.data.as_ref(), &[] as &[u8]);
        assert!(decoded.is_hard_deleted());
    }

    #[test]
    fn direct_row_writes_get_distinct_batch_ids_even_when_version_ids_match() {
        let provenance = RowProvenance::for_insert("alice".to_string(), 100);
        let first = StoredRowVersion::new(
            ObjectId::from_uuid(Uuid::from_u128(101)),
            "main",
            Vec::new(),
            vec![1, 2, 3],
            provenance.clone(),
            HashMap::new(),
            RowState::VisibleDirect,
            Some(DurabilityTier::Worker),
        );
        let second = StoredRowVersion::new(
            ObjectId::from_uuid(Uuid::from_u128(202)),
            "main",
            Vec::new(),
            vec![1, 2, 3],
            provenance,
            HashMap::new(),
            RowState::VisibleDirect,
            Some(DurabilityTier::Worker),
        );

        assert_eq!(
            first.version_id(),
            second.version_id(),
            "this test intentionally holds all hashed version inputs constant across two different rows"
        );
        assert_ne!(
            first.batch_id, second.batch_id,
            "logical write batches must stay distinct across rows even when raw version ids collide"
        );
    }
}
