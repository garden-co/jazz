use std::collections::HashMap;
use std::fmt;
use std::str::FromStr;
use std::sync::OnceLock;

use blake3::Hasher;
use serde::{Deserialize, Serialize};
use smallvec::SmallVec;
use uuid::Uuid;

use crate::digest::Digest32;
use crate::metadata::{DeleteKind, MetadataKey, RowProvenance};
use crate::object::{BranchName, ObjectId};
use crate::query_manager::types::{
    ColumnDescriptor, ColumnType, RowBytes, RowDescriptor, SharedString, Value,
};
use crate::row_format::{EncodingError, column_bytes, decode_column, decode_row, encode_row};
use crate::storage::{IndexMutation, RowLocator, Storage, StorageError};
use crate::sync_manager::DurabilityTier;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
pub struct BatchId(pub [u8; 16]);

impl BatchId {
    pub fn new() -> Self {
        Self::from_uuid(Uuid::now_v7())
    }

    pub fn from_uuid(uuid: Uuid) -> Self {
        Self(*uuid.as_bytes())
    }

    pub fn as_bytes(&self) -> &[u8; 16] {
        &self.0
    }
}

impl Default for BatchId {
    fn default() -> Self {
        Self::new()
    }
}

impl fmt::Display for BatchId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", hex::encode(self.0))
    }
}

impl FromStr for BatchId {
    type Err = String;

    fn from_str(raw: &str) -> Result<Self, Self::Err> {
        let bytes = hex::decode(raw).map_err(|err| format!("invalid batch id hex: {err}"))?;
        let len = bytes.len();
        let bytes: [u8; 16] = bytes
            .try_into()
            .map_err(|_| format!("expected 16-byte batch id, got {len}"))?;
        Ok(Self(bytes))
    }
}

impl From<BatchId> for Digest32 {
    fn from(value: BatchId) -> Self {
        let mut bytes = [0u8; 32];
        bytes[..16].copy_from_slice(&value.0);
        Digest32(bytes)
    }
}

impl From<Digest32> for BatchId {
    fn from(value: Digest32) -> Self {
        let mut bytes = [0u8; 16];
        bytes.copy_from_slice(&value.0[..16]);
        Self(bytes)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum RowState {
    StagingPending,
    Superseded,
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
    pub row: StoredRowBatch,
    pub previous_row: Option<StoredRowBatch>,
    pub is_new_object: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ApplyRowBatchResult {
    pub batch_id: BatchId,
    pub row_locator: RowLocator,
    pub visibility_change: Option<RowVisibilityChange>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RowHistoryError {
    ObjectNotFound(ObjectId),
    ParentNotFound(BatchId),
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

pub fn compute_row_digest(
    branch: &str,
    parents: &[BatchId],
    data: &[u8],
    updated_at: u64,
    updated_by: &str,
    metadata: Option<&RowMetadata>,
) -> Digest32 {
    let mut hasher = Hasher::new();

    hasher.update(b"row-batch-v1");
    hasher.update(&(branch.len() as u64).to_le_bytes());
    hasher.update(branch.as_bytes());

    hasher.update(&(parents.len() as u64).to_le_bytes());
    for parent in parents {
        hasher.update(parent.as_bytes());
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

    Digest32(*hasher.finalize().as_bytes())
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
            "superseded".to_string(),
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
        ColumnDescriptor::new("_jazz_batch_id", ColumnType::Bytea),
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

fn stored_row_system_values(row: &StoredRowBatch, format_id: ObjectId) -> Vec<Value> {
    vec![
        Value::Uuid(format_id),
        Value::Uuid(row.row_id),
        batch_id_to_value(row.batch_id),
        Value::Text(row.branch.to_string()),
        Value::Array(row.parents.iter().copied().map(batch_id_to_value).collect()),
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

fn history_row_system_values(row: &StoredRowBatch) -> Vec<Value> {
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
        ColumnDescriptor::new("_jazz_worker_batch_id", ColumnType::Bytea).nullable(),
        ColumnDescriptor::new("_jazz_edge_batch_id", ColumnType::Bytea).nullable(),
        ColumnDescriptor::new("_jazz_global_batch_id", ColumnType::Bytea).nullable(),
    ]);
    columns
}

fn visible_row_system_values(entry: &VisibleRowEntry) -> Vec<Value> {
    let mut values = stored_row_system_values(&entry.current_row, flat_visible_row_format_id());
    values.extend([
        batch_ids_to_value(&entry.branch_frontier),
        optional_batch_id_to_value(entry.worker_batch_id),
        optional_batch_id_to_value(entry.edge_batch_id),
        optional_batch_id_to_value(entry.global_batch_id),
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

fn stored_row_batch_from_flat_parts(
    user_descriptor: &RowDescriptor,
    system_values: &[Value],
    user_values: &[Value],
) -> Result<StoredRowBatch, EncodingError> {
    let delete_kind = delete_kind_from_value(&system_values[12])?;
    let is_deleted = expect_bool(&system_values[13], "is_deleted")?;
    let user_data =
        flat_user_data_from_values(user_descriptor, user_values, delete_kind, is_deleted)?;

    let parents = match &system_values[4] {
        Value::Array(values) => values
            .iter()
            .map(batch_id_from_value)
            .collect::<Result<SmallVec<[BatchId; 2]>, _>>()?,
        other => {
            return Err(malformed(format!("expected parents array, got {other:?}")));
        }
    };

    let history_batch_id = batch_id_from_value(&system_values[2])?;
    let batch_id = batch_id_from_value(&system_values[9])?;
    if history_batch_id != batch_id {
        return Err(malformed(format!(
            "stored row batch id mismatch: history field {:?} != batch field {:?}",
            history_batch_id, batch_id
        )));
    }

    Ok(StoredRowBatch {
        row_id: expect_uuid(&system_values[1], "row_id")?,
        batch_id,
        branch: expect_text(&system_values[3], "branch")?.into(),
        parents,
        updated_at: expect_timestamp(&system_values[5], "updated_at")?,
        created_by: expect_text(&system_values[6], "created_by")?.into(),
        created_at: expect_timestamp(&system_values[7], "created_at")?,
        updated_by: expect_text(&system_values[8], "updated_by")?.into(),
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
    row: &StoredRowBatch,
) -> Result<Vec<u8>, EncodingError> {
    let mut values = history_row_system_values(row);
    values.extend(flat_user_values(user_descriptor, &row.data)?);

    encode_row(&history_row_physical_descriptor(user_descriptor), &values)
}

/// Decode a flat physical row back into the current `StoredRowBatch` shape.
pub fn decode_flat_history_row(
    user_descriptor: &RowDescriptor,
    data: &[u8],
) -> Result<StoredRowBatch, EncodingError> {
    let descriptor = history_row_physical_descriptor(user_descriptor);
    let values = decode_row(&descriptor, data)?;
    let (system_values, user_values) = values.split_at(history_row_system_column_count());
    stored_row_batch_from_flat_parts(user_descriptor, system_values, user_values)
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
    let current_row = stored_row_batch_from_flat_parts(
        user_descriptor,
        &values[..system_count],
        &values[visible_system_count..],
    )?;

    Ok(VisibleRowEntry {
        current_row,
        branch_frontier: batch_ids_from_value(&values[system_count], "branch_frontier")?,
        worker_batch_id: optional_batch_id_from_value(&values[system_count + 1])?,
        edge_batch_id: optional_batch_id_from_value(&values[system_count + 2])?,
        global_batch_id: optional_batch_id_from_value(&values[system_count + 3])?,
    })
}

fn row_state_to_value(state: RowState) -> Value {
    Value::Text(
        match state {
            RowState::StagingPending => "staging_pending",
            RowState::Superseded => "superseded",
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
            "superseded" => Ok(RowState::Superseded),
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

fn batch_id_to_value(batch_id: BatchId) -> Value {
    Value::Bytea(batch_id.as_bytes().to_vec())
}

fn batch_id_from_value(value: &Value) -> Result<BatchId, EncodingError> {
    match value {
        Value::Bytea(bytes) if bytes.len() == 16 => {
            let bytes: [u8; 16] = bytes
                .as_slice()
                .try_into()
                .map_err(|_| malformed("invalid 16-byte batch id"))?;
            Ok(BatchId(bytes))
        }
        Value::Bytea(bytes) => Err(malformed(format!(
            "expected 16-byte batch id, got {} bytes",
            bytes.len()
        ))),
        other => Err(malformed(format!("expected batch id bytes, got {other:?}"))),
    }
}

fn optional_batch_id_to_value(batch_id: Option<BatchId>) -> Value {
    batch_id.map(batch_id_to_value).unwrap_or(Value::Null)
}

fn optional_batch_id_from_value(value: &Value) -> Result<Option<BatchId>, EncodingError> {
    match value {
        Value::Null => Ok(None),
        _ => batch_id_from_value(value).map(Some),
    }
}

fn batch_ids_to_value(batch_ids: &[BatchId]) -> Value {
    Value::Array(batch_ids.iter().copied().map(batch_id_to_value).collect())
}

fn batch_ids_from_value(value: &Value, label: &str) -> Result<Vec<BatchId>, EncodingError> {
    let Value::Array(values) = value else {
        return Err(malformed(format!("expected {label} array, got {value:?}")));
    };

    values.iter().map(batch_id_from_value).collect()
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
pub struct QueryRowBatch {
    pub batch_id: BatchId,
    pub branch: SharedString,
    pub updated_at: u64,
    pub created_by: SharedString,
    pub created_at: u64,
    pub updated_by: SharedString,
    pub state: RowState,
    pub delete_kind: Option<DeleteKind>,
    pub data: RowBytes,
}

impl QueryRowBatch {
    pub fn batch_id(&self) -> BatchId {
        self.batch_id
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

impl From<&StoredRowBatch> for QueryRowBatch {
    fn from(row: &StoredRowBatch) -> Self {
        Self {
            batch_id: row.batch_id,
            branch: row.branch.clone(),
            updated_at: row.updated_at,
            created_by: row.created_by.clone(),
            created_at: row.created_at,
            updated_by: row.updated_by.clone(),
            state: row.state,
            delete_kind: row.delete_kind,
            data: row.data.clone(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StoredRowBatch {
    pub row_id: ObjectId,
    pub batch_id: BatchId,
    pub branch: SharedString,
    pub parents: SmallVec<[BatchId; 2]>,
    pub updated_at: u64,
    pub created_by: SharedString,
    pub created_at: u64,
    pub updated_by: SharedString,
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

impl StoredRowBatch {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        row_id: ObjectId,
        branch: impl Into<String>,
        parents: impl IntoIterator<Item = BatchId>,
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
        parents: impl IntoIterator<Item = BatchId>,
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
        let parents = parents.into_iter().collect::<SmallVec<[BatchId; 2]>>();

        Self {
            row_id,
            batch_id,
            branch,
            parents,
            updated_at: provenance.updated_at,
            created_by: provenance.created_by.into(),
            created_at: provenance.created_at,
            updated_by: provenance.updated_by.into(),
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

    pub fn batch_id(&self) -> BatchId {
        self.batch_id
    }

    pub fn content_digest(&self) -> Digest32 {
        compute_row_digest(
            &self.branch,
            &self.parents,
            &self.data,
            self.updated_at,
            &self.updated_by,
            (!self.metadata.is_empty()).then_some(&self.metadata),
        )
    }

    pub fn accepted_transaction_output(&self, confirmed_tier: DurabilityTier) -> Self {
        let mut row = self.clone();
        row.parents = self.parents.clone();
        row.state = RowState::VisibleTransactional;
        row.confirmed_tier = Some(confirmed_tier);
        row
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
    pub current_row: StoredRowBatch,
    pub branch_frontier: Vec<BatchId>,
    pub worker_batch_id: Option<BatchId>,
    pub edge_batch_id: Option<BatchId>,
    pub global_batch_id: Option<BatchId>,
}

impl VisibleRowEntry {
    pub fn new(current_row: StoredRowBatch) -> Self {
        Self {
            branch_frontier: vec![current_row.batch_id()],
            current_row,
            worker_batch_id: None,
            edge_batch_id: None,
            global_batch_id: None,
        }
    }

    pub fn rebuild(current_row: StoredRowBatch, history_rows: &[StoredRowBatch]) -> Self {
        let current_batch_id = current_row.batch_id();
        let branch_frontier = branch_frontier(history_rows);
        let worker = latest_visible_version_for_tier(history_rows, DurabilityTier::Worker);
        let worker_batch_id = worker.filter(|batch_id| *batch_id != current_batch_id);

        let edge = latest_visible_version_for_tier(history_rows, DurabilityTier::EdgeServer);
        let edge_batch_id = edge.filter(|batch_id| *batch_id != current_batch_id);

        let global = latest_visible_version_for_tier(history_rows, DurabilityTier::GlobalServer);
        let global_batch_id = global.filter(|batch_id| *batch_id != current_batch_id);

        Self {
            current_row,
            branch_frontier,
            worker_batch_id,
            edge_batch_id,
            global_batch_id,
        }
    }

    pub fn current_batch_id(&self) -> BatchId {
        self.current_row.batch_id()
    }

    pub fn batch_id_for_tier(&self, tier: DurabilityTier) -> Option<BatchId> {
        let current = self.current_batch_id();
        if tier_satisfies(self.current_row.confirmed_tier, tier) {
            return Some(current);
        }

        match tier {
            DurabilityTier::Worker => self.worker_batch_id,
            DurabilityTier::EdgeServer => self.edge_batch_id,
            DurabilityTier::GlobalServer => self.global_batch_id,
        }
    }
}

#[derive(Debug, Clone)]
struct RowBatchApply {
    row_locator: RowLocator,
    previous_visible: Option<StoredRowBatch>,
    current_visible: Option<StoredRowBatch>,
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
) -> Result<Vec<StoredRowBatch>, RowHistoryError> {
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

fn visible_entry_from_history_rows(history_rows: &[StoredRowBatch]) -> Option<VisibleRowEntry> {
    let current_row = history_rows
        .iter()
        .filter(|row| row.state.is_visible())
        .max_by_key(|row| (row.updated_at, row.batch_id()))
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

fn latest_row_wins(candidate: &StoredRowBatch, current: &StoredRowBatch) -> bool {
    (candidate.updated_at, candidate.batch_id()) > (current.updated_at, current.batch_id())
}

fn branch_frontier_after_append(
    previous_frontier: &[BatchId],
    appended_row: &StoredRowBatch,
) -> Vec<BatchId> {
    let appended_batch_id = appended_row.batch_id();
    let mut frontier = previous_frontier
        .iter()
        .copied()
        .filter(|batch_id| !appended_row.parents.contains(batch_id))
        .collect::<Vec<_>>();
    if !frontier.contains(&appended_batch_id) {
        frontier.push(appended_batch_id);
    }
    frontier.sort();
    frontier.dedup();
    frontier
}

fn latest_visible_version_after_append<H: Storage>(
    io: &H,
    table: &str,
    appended_row: &StoredRowBatch,
    previous_winner_id: Option<BatchId>,
) -> Result<Option<BatchId>, RowHistoryError> {
    if !appended_row.state.is_visible() {
        return Ok(previous_winner_id);
    }

    let appended_batch_id = appended_row.batch_id();
    let Some(previous_winner_id) = previous_winner_id else {
        return Ok(Some(appended_batch_id));
    };

    if previous_winner_id == appended_batch_id {
        return Ok(Some(appended_batch_id));
    }

    let Some(previous_winner) = io
        .load_history_row_batch(
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
        Ok(Some(appended_batch_id))
    } else {
        Ok(Some(previous_winner_id))
    }
}

fn visible_entry_after_append<H: Storage>(
    io: &H,
    table: &str,
    previous_entry: Option<&VisibleRowEntry>,
    appended_row: &StoredRowBatch,
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
    let current_batch_id = current_row.batch_id();

    let worker_batch_id = latest_visible_version_after_append(
        io,
        table,
        appended_row,
        previous_entry.batch_id_for_tier(DurabilityTier::Worker),
    )?
    .filter(|batch_id| *batch_id != current_batch_id);
    let edge_batch_id = latest_visible_version_after_append(
        io,
        table,
        appended_row,
        previous_entry.batch_id_for_tier(DurabilityTier::EdgeServer),
    )?
    .filter(|batch_id| *batch_id != current_batch_id);
    let global_batch_id = latest_visible_version_after_append(
        io,
        table,
        appended_row,
        previous_entry.batch_id_for_tier(DurabilityTier::GlobalServer),
    )?
    .filter(|batch_id| *batch_id != current_batch_id);

    Ok(Some(VisibleRowEntry {
        current_row,
        branch_frontier,
        worker_batch_id,
        edge_batch_id,
        global_batch_id,
    }))
}

fn winner_after_tier_upgrade<H: Storage>(
    io: &H,
    table: &str,
    entry: &VisibleRowEntry,
    current_row: &StoredRowBatch,
    patched_row: &StoredRowBatch,
    required_tier: DurabilityTier,
) -> Result<Option<BatchId>, RowHistoryError> {
    let patched_batch_id = patched_row.batch_id();
    if !patched_row.state.is_visible()
        || patched_row
            .confirmed_tier
            .is_none_or(|tier| tier < required_tier)
    {
        return Ok(entry.batch_id_for_tier(required_tier));
    }

    if current_row.batch_id() == patched_batch_id
        || current_row
            .confirmed_tier
            .is_some_and(|tier| tier >= required_tier)
    {
        return Ok(Some(current_row.batch_id()));
    }

    let Some(previous_winner_id) = entry.batch_id_for_tier(required_tier) else {
        return Ok(Some(patched_batch_id));
    };

    if previous_winner_id == patched_batch_id {
        return Ok(Some(patched_batch_id));
    }

    let Some(previous_winner) = io
        .load_history_row_batch(
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
        Ok(Some(patched_batch_id))
    } else {
        Ok(Some(previous_winner_id))
    }
}

fn visible_entry_after_tier_upgrade<H: Storage>(
    io: &H,
    table: &str,
    entry: VisibleRowEntry,
    patched_row: &StoredRowBatch,
) -> Result<VisibleRowEntry, RowHistoryError> {
    let current_row = if entry.current_row.batch_id() == patched_row.batch_id() {
        patched_row.clone()
    } else {
        entry.current_row.clone()
    };
    let current_batch_id = current_row.batch_id();

    let worker_batch_id = winner_after_tier_upgrade(
        io,
        table,
        &entry,
        &current_row,
        patched_row,
        DurabilityTier::Worker,
    )?
    .filter(|batch_id| *batch_id != current_batch_id);
    let edge_batch_id = winner_after_tier_upgrade(
        io,
        table,
        &entry,
        &current_row,
        patched_row,
        DurabilityTier::EdgeServer,
    )?
    .filter(|batch_id| *batch_id != current_batch_id);
    let global_batch_id = winner_after_tier_upgrade(
        io,
        table,
        &entry,
        &current_row,
        patched_row,
        DurabilityTier::GlobalServer,
    )?
    .filter(|batch_id| *batch_id != current_batch_id);

    Ok(VisibleRowEntry {
        current_row,
        branch_frontier: entry.branch_frontier,
        worker_batch_id,
        edge_batch_id,
        global_batch_id,
    })
}

fn visibility_change_from_applied(
    object_id: ObjectId,
    applied: RowBatchApply,
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

fn supersede_older_staging_rows_for_batch<H: Storage>(
    io: &mut H,
    table: &str,
    object_id: ObjectId,
    branch_name: &BranchName,
    batch_id: BatchId,
) -> Result<(), RowHistoryError> {
    let branch = SharedString::from(branch_name.as_str().to_string());
    let history_rows = load_branch_history(io, table, object_id, &branch)?;
    let mut pending_rows = history_rows
        .into_iter()
        .filter(|row| row.batch_id == batch_id && matches!(row.state, RowState::StagingPending))
        .collect::<Vec<_>>();

    if pending_rows.len() <= 1 {
        return Ok(());
    }

    pending_rows.sort_by_key(|row| (row.updated_at, row.batch_id()));
    pending_rows.pop();

    for row in pending_rows {
        let _ = patch_row_batch_state(
            io,
            object_id,
            branch_name,
            row.batch_id(),
            Some(RowState::Superseded),
            None,
        )?;
    }

    Ok(())
}

pub fn apply_row_batch<H: Storage>(
    io: &mut H,
    object_id: ObjectId,
    branch_name: &BranchName,
    row: StoredRowBatch,
    index_mutations: &[IndexMutation<'_>],
) -> Result<ApplyRowBatchResult, RowHistoryError> {
    let row_locator = row_locator_from_storage(io, object_id)?;
    let table = row_locator.table.to_string();
    let batch_id = row.batch_id();
    let branch = SharedString::from(branch_name.as_str().to_string());
    let previous_entry = load_previous_visible_entry(io, &table, object_id, &branch)?;
    let previous_visible = previous_entry
        .as_ref()
        .map(|entry| entry.current_row.clone());

    for parent in &row.parents {
        if io
            .load_history_row_batch(&table, branch_name.as_str(), object_id, *parent)
            .map_err(RowHistoryError::StorageError)?
            .is_none()
        {
            return Err(RowHistoryError::ParentNotFound(*parent));
        }
    }

    if let Some(existing_row) = io
        .load_history_row_batch(&table, branch_name.as_str(), object_id, batch_id)
        .map_err(RowHistoryError::StorageError)?
        && existing_row == row
    {
        return Ok(ApplyRowBatchResult {
            batch_id,
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

    if matches!(row.state, RowState::StagingPending) {
        supersede_older_staging_rows_for_batch(io, &table, object_id, branch_name, row.batch_id)?;
    }

    let applied = RowBatchApply {
        row_locator: row_locator.clone(),
        previous_visible: previous_visible.clone(),
        current_visible,
        is_new_object: previous_visible.is_none(),
        visible_changed,
    };

    Ok(ApplyRowBatchResult {
        batch_id,
        row_locator,
        visibility_change: visibility_change_from_applied(object_id, applied),
    })
}

pub fn patch_row_batch_state<H: Storage>(
    io: &mut H,
    object_id: ObjectId,
    branch_name: &BranchName,
    batch_id: BatchId,
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
        .load_history_row_batch(&table, branch_name.as_str(), object_id, batch_id)
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
                .find(|candidate| candidate.batch_id() == batch_id)
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
    history_rows: &[StoredRowBatch],
    required_tier: DurabilityTier,
) -> Option<BatchId> {
    history_rows
        .iter()
        .filter(|row| row.state.is_visible() && tier_satisfies(row.confirmed_tier, required_tier))
        .max_by_key(|row| (row.updated_at, row.batch_id()))
        .map(StoredRowBatch::batch_id)
}

fn branch_frontier(history_rows: &[StoredRowBatch]) -> Vec<BatchId> {
    let mut non_tips = std::collections::BTreeSet::new();
    for row in history_rows {
        for parent in &row.parents {
            non_tips.insert(*parent);
        }
    }

    let mut tips: Vec<_> = history_rows
        .iter()
        .map(StoredRowBatch::batch_id)
        .filter(|batch_id| !non_tips.contains(batch_id))
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

    fn visible_row(updated_at: u64, confirmed_tier: Option<DurabilityTier>) -> StoredRowBatch {
        StoredRowBatch::new(
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
        let global = StoredRowBatch::new(
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
        let current = StoredRowBatch::new(
            global.row_id,
            "main",
            vec![global.batch_id()],
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
            branch_frontier: vec![global.batch_id()],
            worker_batch_id: None,
            edge_batch_id: Some(global.batch_id()),
            global_batch_id: Some(global.batch_id()),
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

        assert_eq!(entry.branch_frontier, vec![current.batch_id()]);
        assert_eq!(entry.worker_batch_id, None);
        assert_eq!(entry.edge_batch_id, None);
        assert_eq!(entry.global_batch_id, None);
    }

    #[test]
    fn visible_row_entry_resolves_tier_fallback_chain() {
        let global = visible_row(10, Some(DurabilityTier::GlobalServer));
        let edge = StoredRowBatch::new(
            global.row_id,
            "main",
            vec![global.batch_id()],
            vec![2],
            RowProvenance::for_update(&global.row_provenance(), "alice".to_string(), 20),
            HashMap::new(),
            RowState::VisibleDirect,
            Some(DurabilityTier::EdgeServer),
        );
        let current = StoredRowBatch::new(
            global.row_id,
            "main",
            vec![edge.batch_id()],
            vec![3],
            RowProvenance::for_update(&edge.row_provenance(), "alice".to_string(), 30),
            HashMap::new(),
            RowState::VisibleDirect,
            Some(DurabilityTier::Worker),
        );
        let history = vec![global.clone(), edge.clone(), current.clone()];

        let entry = VisibleRowEntry::rebuild(current.clone(), &history);

        assert_eq!(entry.branch_frontier, vec![current.batch_id()]);
        assert_eq!(entry.worker_batch_id, None);
        assert_eq!(entry.edge_batch_id, Some(edge.batch_id()));
        assert_eq!(entry.global_batch_id, Some(global.batch_id()));
        assert_eq!(
            entry.batch_id_for_tier(DurabilityTier::Worker),
            Some(current.batch_id())
        );
        assert_eq!(
            entry.batch_id_for_tier(DurabilityTier::EdgeServer),
            Some(edge.batch_id())
        );
        assert_eq!(
            entry.batch_id_for_tier(DurabilityTier::GlobalServer),
            Some(global.batch_id())
        );
    }

    #[test]
    fn visible_row_entry_returns_none_when_no_version_meets_required_tier() {
        let current = visible_row(30, Some(DurabilityTier::Worker));
        let entry = VisibleRowEntry::rebuild(current.clone(), std::slice::from_ref(&current));

        assert_eq!(entry.branch_frontier, vec![current.batch_id()]);
        assert_eq!(entry.batch_id_for_tier(DurabilityTier::EdgeServer), None);
        assert_eq!(entry.batch_id_for_tier(DurabilityTier::GlobalServer), None);
    }

    #[test]
    fn visible_row_entry_preserves_multiple_branch_tips() {
        let base = visible_row(10, Some(DurabilityTier::Worker));
        let left = StoredRowBatch::new(
            base.row_id,
            "main",
            vec![base.batch_id()],
            vec![1],
            RowProvenance::for_update(&base.row_provenance(), "alice".to_string(), 20),
            HashMap::new(),
            RowState::VisibleDirect,
            Some(DurabilityTier::Worker),
        );
        let right = StoredRowBatch::new(
            base.row_id,
            "main",
            vec![base.batch_id()],
            vec![2],
            RowProvenance::for_update(&base.row_provenance(), "bob".to_string(), 21),
            HashMap::new(),
            RowState::VisibleDirect,
            Some(DurabilityTier::Worker),
        );

        let entry = VisibleRowEntry::rebuild(right.clone(), &[base, left.clone(), right.clone()]);

        assert_eq!(
            entry.branch_frontier,
            vec![left.batch_id(), right.batch_id()]
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
        let row = StoredRowBatch::new(
            ObjectId::from_uuid(Uuid::from_u128(42)),
            "main",
            vec![BatchId([9; 16])],
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
        let deleted = StoredRowBatch::new(
            ObjectId::from_uuid(Uuid::from_u128(43)),
            "main",
            vec![BatchId([7; 16])],
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
    fn direct_row_writes_use_batch_identity() {
        let provenance = RowProvenance::for_insert("alice".to_string(), 100);
        let first = StoredRowBatch::new(
            ObjectId::from_uuid(Uuid::from_u128(101)),
            "main",
            Vec::new(),
            vec![1, 2, 3],
            provenance.clone(),
            HashMap::new(),
            RowState::VisibleDirect,
            Some(DurabilityTier::Worker),
        );

        assert_eq!(
            first.batch_id(),
            first.batch_id,
            "direct visible rows should publish under their batch identity"
        );
    }
}
