//! Descriptor builders + flat-row encoders/decoders.

use std::collections::HashMap;
use std::sync::{Arc, Mutex, OnceLock};

use blake3::Hasher;
use smallvec::SmallVec;

use crate::digest::Digest32;
use crate::metadata::DeleteKind;
use crate::object::ObjectId;
use crate::query_manager::types::{ColumnDescriptor, ColumnType, RowBytes, RowDescriptor, Value};
use crate::row_format::{
    CompiledRowLayout, EncodingError, column_bytes_with_layout, column_is_null_with_layout,
    compiled_row_layout, decode_row, encode_row_with_prefix_and_projected_tail,
    project_row_with_layout,
};
use crate::sync_manager::DurabilityTier;

use super::types::{BatchId, RowMetadata, RowState, StoredRowBatch, VisibleRowEntry};

pub(super) fn tier_satisfies(
    confirmed_tier: Option<DurabilityTier>,
    required_tier: DurabilityTier,
) -> bool {
    confirmed_tier.is_some_and(|confirmed| confirmed >= required_tier)
}

pub(super) fn malformed(message: impl Into<String>) -> EncodingError {
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

fn metadata_entry_layout() -> &'static Arc<CompiledRowLayout> {
    static LAYOUT: OnceLock<Arc<CompiledRowLayout>> = OnceLock::new();
    LAYOUT.get_or_init(|| compiled_row_layout(metadata_entry_descriptor()))
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
            "local".to_string(),
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

fn history_row_system_columns() -> Vec<ColumnDescriptor> {
    vec![
        ColumnDescriptor::new(
            "_jazz_parents",
            ColumnType::Array {
                element: Box::new(ColumnType::BatchId),
            },
        )
        .nullable(),
        ColumnDescriptor::new("_jazz_updated_at", ColumnType::Timestamp),
        ColumnDescriptor::new("_jazz_created_by", ColumnType::Text),
        ColumnDescriptor::new("_jazz_created_at", ColumnType::Timestamp),
        ColumnDescriptor::new("_jazz_updated_by", ColumnType::Text),
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
        )
        .nullable(),
    ]
}

fn history_row_system_values(row: &StoredRowBatch) -> Vec<Value> {
    vec![
        Value::Array(row.parents.iter().copied().map(batch_id_to_value).collect()),
        Value::Timestamp(row.updated_at),
        Value::Text(row.created_by.to_string()),
        Value::Timestamp(row.created_at),
        Value::Text(row.updated_by.to_string()),
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

const HISTORY_ROW_SYSTEM_COLUMN_COUNT: usize = 10;

fn visible_row_system_columns() -> Vec<ColumnDescriptor> {
    let mut columns = vec![
        ColumnDescriptor::new("_jazz_batch_id", ColumnType::BatchId),
        ColumnDescriptor::new("_jazz_updated_at", ColumnType::Timestamp),
        ColumnDescriptor::new("_jazz_created_by", ColumnType::Text),
        ColumnDescriptor::new("_jazz_created_at", ColumnType::Timestamp),
        ColumnDescriptor::new("_jazz_updated_by", ColumnType::Text),
        ColumnDescriptor::new("_jazz_state", row_state_column_type()),
        ColumnDescriptor::new("_jazz_confirmed_tier", confirmed_tier_column_type()).nullable(),
        ColumnDescriptor::new("_jazz_delete_kind", delete_kind_column_type()).nullable(),
    ];
    columns.extend([
        ColumnDescriptor::new(
            "_jazz_branch_frontier",
            ColumnType::Array {
                element: Box::new(ColumnType::BatchId),
            },
        )
        .nullable(),
        ColumnDescriptor::new("_jazz_worker_batch_id", ColumnType::BatchId).nullable(),
        ColumnDescriptor::new("_jazz_edge_batch_id", ColumnType::BatchId).nullable(),
        ColumnDescriptor::new("_jazz_global_batch_id", ColumnType::BatchId).nullable(),
        ColumnDescriptor::new(
            "_jazz_winner_batch_pool",
            ColumnType::Array {
                element: Box::new(ColumnType::BatchId),
            },
        )
        .nullable(),
        ColumnDescriptor::new("_jazz_current_winner_ordinals", ColumnType::Bytea).nullable(),
        ColumnDescriptor::new("_jazz_worker_winner_ordinals", ColumnType::Bytea).nullable(),
        ColumnDescriptor::new("_jazz_edge_winner_ordinals", ColumnType::Bytea).nullable(),
        ColumnDescriptor::new("_jazz_global_winner_ordinals", ColumnType::Bytea).nullable(),
        ColumnDescriptor::new("_jazz_merge_artifacts", ColumnType::Bytea).nullable(),
    ]);
    columns
}

fn visible_row_system_values(entry: &VisibleRowEntry) -> Vec<Value> {
    let mut values = vec![
        batch_id_to_value(entry.current_row.batch_id),
        Value::Timestamp(entry.current_row.updated_at),
        Value::Text(entry.current_row.created_by.to_string()),
        Value::Timestamp(entry.current_row.created_at),
        Value::Text(entry.current_row.updated_by.to_string()),
        row_state_to_value(entry.current_row.state),
        entry
            .current_row
            .confirmed_tier
            .map(durability_tier_to_value)
            .unwrap_or(Value::Null),
        entry
            .current_row
            .delete_kind
            .map(delete_kind_to_value)
            .unwrap_or(Value::Null),
    ];
    values.extend([
        visible_frontier_to_value(entry),
        optional_batch_id_to_value(entry.worker_batch_id),
        optional_batch_id_to_value(entry.edge_batch_id),
        optional_batch_id_to_value(entry.global_batch_id),
        winner_batch_pool_to_value(&entry.winner_batch_pool),
        optional_winner_ordinals_to_value(entry.current_winner_ordinals.as_deref()),
        optional_winner_ordinals_to_value(entry.worker_winner_ordinals.as_deref()),
        optional_winner_ordinals_to_value(entry.edge_winner_ordinals.as_deref()),
        optional_winner_ordinals_to_value(entry.global_winner_ordinals.as_deref()),
        entry
            .merge_artifacts
            .as_ref()
            .map(|bytes| Value::Bytea(bytes.clone()))
            .unwrap_or(Value::Null),
    ]);
    values
}

const VISIBLE_ROW_SYSTEM_COLUMN_COUNT: usize = 18;

#[derive(Debug, Clone)]
pub(crate) struct FlatRowCodecs {
    user_descriptor: Arc<RowDescriptor>,
    user_layout: Arc<CompiledRowLayout>,
    history_descriptor: Arc<RowDescriptor>,
    history_layout: Arc<CompiledRowLayout>,
    history_user_projection: Vec<(usize, usize)>,
    visible_descriptor: Arc<RowDescriptor>,
    visible_layout: Arc<CompiledRowLayout>,
    visible_user_projection: Vec<(usize, usize)>,
}

fn flat_row_codecs_cache() -> &'static Mutex<HashMap<[u8; 32], Arc<FlatRowCodecs>>> {
    static CACHE: OnceLock<Mutex<HashMap<[u8; 32], Arc<FlatRowCodecs>>>> = OnceLock::new();
    CACHE.get_or_init(|| Mutex::new(HashMap::new()))
}

pub(crate) fn flat_row_codecs(user_descriptor: &RowDescriptor) -> Arc<FlatRowCodecs> {
    let key = user_descriptor.content_hash();
    {
        let guard = flat_row_codecs_cache()
            .lock()
            .expect("flat row codec cache poisoned");
        if let Some(codecs) = guard.get(&key) {
            return codecs.clone();
        }
    }

    let user_descriptor = Arc::new(user_descriptor.clone());
    let history_descriptor = Arc::new(history_row_physical_descriptor(user_descriptor.as_ref()));
    let visible_descriptor = Arc::new(visible_row_physical_descriptor(user_descriptor.as_ref()));
    let history_system_count = HISTORY_ROW_SYSTEM_COLUMN_COUNT;
    let visible_system_count = VISIBLE_ROW_SYSTEM_COLUMN_COUNT;
    let user_projection_len = user_descriptor.columns.len();
    let codecs = Arc::new(FlatRowCodecs {
        user_descriptor: user_descriptor.clone(),
        user_layout: compiled_row_layout(user_descriptor.as_ref()),
        history_layout: compiled_row_layout(history_descriptor.as_ref()),
        history_descriptor,
        history_user_projection: (0..user_projection_len)
            .map(|index| (history_system_count + index, index))
            .collect(),
        visible_layout: compiled_row_layout(visible_descriptor.as_ref()),
        visible_descriptor,
        visible_user_projection: (0..user_projection_len)
            .map(|index| (visible_system_count + index, index))
            .collect(),
    });

    flat_row_codecs_cache()
        .lock()
        .expect("flat row codec cache poisoned")
        .insert(key, codecs.clone());
    codecs
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

pub(super) fn flat_user_values(
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

/// Encode a row-history version into a single flat physical row.
pub fn encode_flat_history_row(
    user_descriptor: &RowDescriptor,
    row: &StoredRowBatch,
) -> Result<Vec<u8>, EncodingError> {
    let codecs = flat_row_codecs(user_descriptor);
    encode_row_with_prefix_and_projected_tail(
        codecs.history_descriptor.as_ref(),
        codecs.history_layout.as_ref(),
        &history_row_system_values(row),
        user_descriptor,
        codecs.user_layout.as_ref(),
        &row.data,
    )
}

/// Decode a flat physical row back into the current `StoredRowBatch` shape.
pub fn decode_flat_history_row(
    user_descriptor: &RowDescriptor,
    row_id: ObjectId,
    branch: &str,
    batch_id: BatchId,
    data: &[u8],
) -> Result<StoredRowBatch, EncodingError> {
    let codecs = flat_row_codecs(user_descriptor);
    decode_flat_history_row_with_codecs(codecs.as_ref(), row_id, branch, batch_id, data)
}

pub fn encode_flat_visible_row_entry(
    user_descriptor: &RowDescriptor,
    entry: &VisibleRowEntry,
) -> Result<Vec<u8>, EncodingError> {
    let codecs = flat_row_codecs(user_descriptor);
    encode_row_with_prefix_and_projected_tail(
        codecs.visible_descriptor.as_ref(),
        codecs.visible_layout.as_ref(),
        &visible_row_system_values(entry),
        user_descriptor,
        codecs.user_layout.as_ref(),
        &entry.current_row.data,
    )
}

pub fn decode_flat_visible_row_entry(
    user_descriptor: &RowDescriptor,
    row_id: ObjectId,
    branch: &str,
    data: &[u8],
) -> Result<VisibleRowEntry, EncodingError> {
    let codecs = flat_row_codecs(user_descriptor);
    decode_flat_visible_row_entry_with_codecs(codecs.as_ref(), row_id, branch, data)
}

fn visible_frontier_to_value(entry: &VisibleRowEntry) -> Value {
    if entry.branch_frontier.len() == 1 && entry.branch_frontier[0] == entry.current_row.batch_id()
    {
        Value::Null
    } else {
        batch_ids_to_value(&entry.branch_frontier)
    }
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

fn durability_tier_to_value(tier: DurabilityTier) -> Value {
    Value::Text(
        match tier {
            DurabilityTier::Local => "local",
            DurabilityTier::EdgeServer => "edge",
            DurabilityTier::GlobalServer => "global",
        }
        .to_string(),
    )
}

fn delete_kind_to_value(kind: DeleteKind) -> Value {
    Value::Text(kind.as_str().to_string())
}

fn batch_id_to_value(batch_id: BatchId) -> Value {
    Value::BatchId(*batch_id.as_bytes())
}

fn optional_batch_id_to_value(batch_id: Option<BatchId>) -> Value {
    batch_id.map(batch_id_to_value).unwrap_or(Value::Null)
}

fn batch_ids_to_value(batch_ids: &[BatchId]) -> Value {
    Value::Array(batch_ids.iter().copied().map(batch_id_to_value).collect())
}

fn winner_batch_pool_to_value(batch_ids: &[BatchId]) -> Value {
    if batch_ids.is_empty() {
        Value::Null
    } else {
        batch_ids_to_value(batch_ids)
    }
}

fn encode_winner_ordinals(ordinals: &[u16]) -> Vec<u8> {
    let mut bytes = Vec::with_capacity(ordinals.len() * 2);
    for ordinal in ordinals {
        bytes.extend_from_slice(&ordinal.to_le_bytes());
    }
    bytes
}

fn optional_winner_ordinals_to_value(ordinals: Option<&[u16]>) -> Value {
    ordinals
        .map(|ordinals| Value::Bytea(encode_winner_ordinals(ordinals)))
        .unwrap_or(Value::Null)
}

fn decode_batch_ids_array_bytes(data: &[u8], label: &str) -> Result<Vec<BatchId>, EncodingError> {
    if data.len() < 4 {
        return Err(malformed(format!("{label} array too short for count")));
    }

    let count = u32::from_le_bytes(data[..4].try_into().unwrap()) as usize;
    let expected_len = 4 + count * 16;
    if data.len() != expected_len {
        return Err(malformed(format!(
            "{label} batch-id array expected {expected_len} bytes, got {}",
            data.len()
        )));
    }

    let mut batch_ids = Vec::with_capacity(count);
    for index in 0..count {
        let start = 4 + index * 16;
        let end = start + 16;
        batch_ids.push(BatchId(data[start..end].try_into().unwrap()));
    }
    Ok(batch_ids)
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

fn decode_required_column_bytes<'a>(
    descriptor: &RowDescriptor,
    layout: &CompiledRowLayout,
    data: &'a [u8],
    column_index: usize,
    label: &str,
) -> Result<&'a [u8], EncodingError> {
    column_bytes_with_layout(descriptor, layout, data, column_index)?.ok_or_else(|| {
        malformed(format!(
            "expected {label} column '{}' to be non-null",
            descriptor.columns[column_index].name
        ))
    })
}

fn decode_text_bytes(bytes: &[u8], label: &str) -> Result<String, EncodingError> {
    std::str::from_utf8(bytes)
        .map(|raw| raw.to_string())
        .map_err(|err| malformed(format!("expected {label} utf8 text: {err}")))
}

fn decode_timestamp_bytes(bytes: &[u8], label: &str) -> Result<u64, EncodingError> {
    if bytes.len() != 8 {
        return Err(malformed(format!(
            "expected {label} timestamp to be 8 bytes, got {}",
            bytes.len()
        )));
    }
    Ok(u64::from_le_bytes(bytes.try_into().unwrap()))
}

fn decode_bool_bytes(bytes: &[u8], label: &str) -> Result<bool, EncodingError> {
    if bytes.len() != 1 {
        return Err(malformed(format!(
            "expected {label} boolean to be 1 byte, got {}",
            bytes.len()
        )));
    }
    Ok(bytes[0] != 0)
}

fn decode_row_state_bytes(bytes: &[u8]) -> Result<RowState, EncodingError> {
    match bytes {
        [0] => Ok(RowState::StagingPending),
        [1] => Ok(RowState::Superseded),
        [2] => Ok(RowState::Rejected),
        [3] => Ok(RowState::VisibleDirect),
        [4] => Ok(RowState::VisibleTransactional),
        b"staging_pending" => Ok(RowState::StagingPending),
        b"superseded" => Ok(RowState::Superseded),
        b"rejected" => Ok(RowState::Rejected),
        b"visible_direct" => Ok(RowState::VisibleDirect),
        b"visible_transactional" => Ok(RowState::VisibleTransactional),
        _ => Err(malformed(format!(
            "invalid row state bytes '{}'",
            String::from_utf8_lossy(bytes)
        ))),
    }
}

fn decode_optional_durability_tier_bytes(
    bytes: Option<&[u8]>,
) -> Result<Option<DurabilityTier>, EncodingError> {
    match bytes {
        None => Ok(None),
        Some([0]) => Ok(Some(DurabilityTier::Local)),
        Some([1]) => Ok(Some(DurabilityTier::EdgeServer)),
        Some([2]) => Ok(Some(DurabilityTier::GlobalServer)),
        Some(b"local") => Ok(Some(DurabilityTier::Local)),
        Some(b"edge") => Ok(Some(DurabilityTier::EdgeServer)),
        Some(b"global") => Ok(Some(DurabilityTier::GlobalServer)),
        Some(bytes) => Err(malformed(format!(
            "invalid durability tier bytes '{}'",
            String::from_utf8_lossy(bytes)
        ))),
    }
}

fn decode_optional_delete_kind_bytes(
    bytes: Option<&[u8]>,
) -> Result<Option<DeleteKind>, EncodingError> {
    match bytes {
        None => Ok(None),
        Some([0]) => Ok(Some(DeleteKind::Soft)),
        Some([1]) => Ok(Some(DeleteKind::Hard)),
        Some(b"soft") => Ok(Some(DeleteKind::Soft)),
        Some(b"hard") => Ok(Some(DeleteKind::Hard)),
        Some(bytes) => Err(malformed(format!(
            "invalid delete kind bytes '{}'",
            String::from_utf8_lossy(bytes)
        ))),
    }
}

fn decode_required_batch_id_bytes(bytes: &[u8], label: &str) -> Result<BatchId, EncodingError> {
    if bytes.len() != 16 {
        return Err(malformed(format!(
            "expected {label} batch id to be 16 bytes, got {}",
            bytes.len()
        )));
    }
    Ok(BatchId(bytes.try_into().unwrap()))
}

fn decode_optional_batch_id_bytes(bytes: Option<&[u8]>) -> Result<Option<BatchId>, EncodingError> {
    bytes
        .map(|bytes| decode_required_batch_id_bytes(bytes, "optional"))
        .transpose()
}

fn decode_optional_winner_ordinals_bytes(
    bytes: Option<&[u8]>,
    label: &str,
    expected_len: usize,
) -> Result<Option<Vec<u16>>, EncodingError> {
    let Some(bytes) = bytes else {
        return Ok(None);
    };
    if bytes.len() != expected_len * 2 {
        return Err(malformed(format!(
            "{label} expected {} bytes for {expected_len} columns, got {}",
            expected_len * 2,
            bytes.len()
        )));
    }
    let mut ordinals = Vec::with_capacity(expected_len);
    for chunk in bytes.chunks_exact(2) {
        ordinals.push(u16::from_le_bytes([chunk[0], chunk[1]]));
    }
    Ok(Some(ordinals))
}

fn decode_metadata_entry_row_bytes(bytes: &[u8]) -> Result<(String, String), EncodingError> {
    if bytes.is_empty() {
        return Err(malformed("metadata row id flag missing"));
    }
    let row_bytes = match bytes[0] {
        0 => &bytes[1..],
        1 => {
            if bytes.len() < 17 {
                return Err(malformed("metadata row id too short"));
            }
            &bytes[17..]
        }
        other => {
            return Err(malformed(format!(
                "metadata row id flag must be 0 or 1, got {other}"
            )));
        }
    };

    let descriptor = metadata_entry_descriptor();
    let layout = metadata_entry_layout().as_ref();
    let key = decode_text_bytes(
        decode_required_column_bytes(descriptor, layout, row_bytes, 0, "metadata key")?,
        "metadata key",
    )?;
    let value = decode_text_bytes(
        decode_required_column_bytes(descriptor, layout, row_bytes, 1, "metadata value")?,
        "metadata value",
    )?;
    Ok((key, value))
}

fn decode_metadata_entries_array_bytes(
    bytes: &[u8],
) -> Result<Vec<(String, String)>, EncodingError> {
    if bytes.len() < 4 {
        return Err(malformed("metadata array too short for count"));
    }

    let count = u32::from_le_bytes(bytes[0..4].try_into().unwrap()) as usize;
    if count == 0 {
        return Ok(Vec::new());
    }

    let offset_table_start = 4;
    let offset_table_size = (count - 1) * 4;
    let data_start = offset_table_start + offset_table_size;
    if data_start > bytes.len() {
        return Err(malformed("metadata array offset table truncated"));
    }

    let mut entries = Vec::with_capacity(count);
    for index in 0..count {
        let start = if index == 0 {
            data_start
        } else {
            let offset_pos = offset_table_start + (index - 1) * 4;
            u32::from_le_bytes(bytes[offset_pos..offset_pos + 4].try_into().unwrap()) as usize
                + data_start
        };
        let end = if index + 1 < count {
            let offset_pos = offset_table_start + index * 4;
            u32::from_le_bytes(bytes[offset_pos..offset_pos + 4].try_into().unwrap()) as usize
                + data_start
        } else {
            bytes.len()
        };

        if end > bytes.len() || start > end {
            return Err(malformed("metadata array element bounds invalid"));
        }

        entries.push(decode_metadata_entry_row_bytes(&bytes[start..end])?);
    }

    Ok(entries)
}

fn decode_metadata_bytes(bytes: Option<&[u8]>) -> Result<RowMetadata, EncodingError> {
    let Some(bytes) = bytes else {
        return Ok(RowMetadata::default());
    };
    Ok(RowMetadata::from_entries(
        decode_metadata_entries_array_bytes(bytes)?,
    ))
}

pub(super) fn project_user_row_data_from_physical(
    physical_descriptor: &RowDescriptor,
    physical_layout: &CompiledRowLayout,
    user_descriptor: &RowDescriptor,
    projection: &[(usize, usize)],
    data: &[u8],
    delete_kind: Option<DeleteKind>,
    is_deleted: bool,
) -> Result<Vec<u8>, EncodingError> {
    if delete_kind == Some(DeleteKind::Hard) {
        return Ok(Vec::new());
    }

    let all_user_columns_null = projection
        .iter()
        .try_fold(true, |all_null, (src_index, _)| {
            if !all_null {
                return Ok(false);
            }
            column_is_null_with_layout(physical_descriptor, physical_layout, data, *src_index)
        })?;
    if is_deleted && all_user_columns_null {
        return Ok(Vec::new());
    }

    project_row_with_layout(
        physical_descriptor,
        physical_layout,
        data,
        user_descriptor,
        projection,
    )
}

pub(crate) fn decode_flat_history_row_with_codecs(
    codecs: &FlatRowCodecs,
    row_id: ObjectId,
    branch: &str,
    batch_id: BatchId,
    data: &[u8],
) -> Result<StoredRowBatch, EncodingError> {
    let descriptor = codecs.history_descriptor.as_ref();
    let layout = codecs.history_layout.as_ref();
    let delete_kind =
        decode_optional_delete_kind_bytes(column_bytes_with_layout(descriptor, layout, data, 7)?)?;
    let is_deleted = decode_bool_bytes(
        decode_required_column_bytes(descriptor, layout, data, 8, "is_deleted")?,
        "is_deleted",
    )?;
    let user_data = project_user_row_data_from_physical(
        descriptor,
        layout,
        codecs.user_descriptor.as_ref(),
        &codecs.history_user_projection,
        data,
        delete_kind,
        is_deleted,
    )?;

    let parents = match column_bytes_with_layout(descriptor, layout, data, 0)? {
        None => SmallVec::new(),
        Some(bytes) => SmallVec::from_vec(decode_batch_ids_array_bytes(bytes, "parents")?),
    };

    Ok(StoredRowBatch {
        row_id,
        batch_id,
        branch: branch.into(),
        parents,
        updated_at: decode_timestamp_bytes(
            decode_required_column_bytes(descriptor, layout, data, 1, "updated_at")?,
            "updated_at",
        )?,
        created_by: decode_text_bytes(
            decode_required_column_bytes(descriptor, layout, data, 2, "created_by")?,
            "created_by",
        )?
        .into(),
        created_at: decode_timestamp_bytes(
            decode_required_column_bytes(descriptor, layout, data, 3, "created_at")?,
            "created_at",
        )?,
        updated_by: decode_text_bytes(
            decode_required_column_bytes(descriptor, layout, data, 4, "updated_by")?,
            "updated_by",
        )?
        .into(),
        state: decode_row_state_bytes(decode_required_column_bytes(
            descriptor, layout, data, 5, "state",
        )?)?,
        confirmed_tier: decode_optional_durability_tier_bytes(column_bytes_with_layout(
            descriptor, layout, data, 6,
        )?)?,
        delete_kind,
        is_deleted,
        data: user_data.into(),
        metadata: decode_metadata_bytes(column_bytes_with_layout(descriptor, layout, data, 9)?)?,
    })
}

pub(crate) fn decode_flat_visible_row_entry_with_codecs(
    codecs: &FlatRowCodecs,
    row_id: ObjectId,
    branch: &str,
    data: &[u8],
) -> Result<VisibleRowEntry, EncodingError> {
    let descriptor = codecs.visible_descriptor.as_ref();
    let layout = codecs.visible_layout.as_ref();
    let batch_id = decode_required_batch_id_bytes(
        decode_required_column_bytes(descriptor, layout, data, 0, "batch_id")?,
        "batch_id",
    )?;
    let delete_kind =
        decode_optional_delete_kind_bytes(column_bytes_with_layout(descriptor, layout, data, 7)?)?;
    let is_deleted = delete_kind.is_some();
    let current_row = StoredRowBatch {
        row_id,
        batch_id,
        branch: branch.into(),
        parents: SmallVec::new(),
        updated_at: decode_timestamp_bytes(
            decode_required_column_bytes(descriptor, layout, data, 1, "updated_at")?,
            "updated_at",
        )?,
        created_by: decode_text_bytes(
            decode_required_column_bytes(descriptor, layout, data, 2, "created_by")?,
            "created_by",
        )?
        .into(),
        created_at: decode_timestamp_bytes(
            decode_required_column_bytes(descriptor, layout, data, 3, "created_at")?,
            "created_at",
        )?,
        updated_by: decode_text_bytes(
            decode_required_column_bytes(descriptor, layout, data, 4, "updated_by")?,
            "updated_by",
        )?
        .into(),
        state: decode_row_state_bytes(decode_required_column_bytes(
            descriptor, layout, data, 5, "state",
        )?)?,
        confirmed_tier: decode_optional_durability_tier_bytes(column_bytes_with_layout(
            descriptor, layout, data, 6,
        )?)?,
        delete_kind,
        is_deleted,
        data: project_user_row_data_from_physical(
            descriptor,
            layout,
            codecs.user_descriptor.as_ref(),
            &codecs.visible_user_projection,
            data,
            delete_kind,
            is_deleted,
        )?
        .into(),
        metadata: RowMetadata::default(),
    };
    let current_batch_id = current_row.batch_id();

    Ok(VisibleRowEntry {
        current_row,
        branch_frontier: match column_bytes_with_layout(descriptor, layout, data, 8)? {
            None => vec![current_batch_id],
            Some(bytes) => decode_batch_ids_array_bytes(bytes, "branch_frontier")?,
        },
        worker_batch_id: decode_optional_batch_id_bytes(column_bytes_with_layout(
            descriptor, layout, data, 9,
        )?)?,
        edge_batch_id: decode_optional_batch_id_bytes(column_bytes_with_layout(
            descriptor, layout, data, 10,
        )?)?,
        global_batch_id: decode_optional_batch_id_bytes(column_bytes_with_layout(
            descriptor, layout, data, 11,
        )?)?,
        winner_batch_pool: match column_bytes_with_layout(descriptor, layout, data, 12)? {
            None => Vec::new(),
            Some(bytes) => decode_batch_ids_array_bytes(bytes, "winner_batch_pool")?,
        },
        current_winner_ordinals: decode_optional_winner_ordinals_bytes(
            column_bytes_with_layout(descriptor, layout, data, 13)?,
            "current_winner_ordinals",
            codecs.user_descriptor.columns.len(),
        )?,
        worker_winner_ordinals: decode_optional_winner_ordinals_bytes(
            column_bytes_with_layout(descriptor, layout, data, 14)?,
            "worker_winner_ordinals",
            codecs.user_descriptor.columns.len(),
        )?,
        edge_winner_ordinals: decode_optional_winner_ordinals_bytes(
            column_bytes_with_layout(descriptor, layout, data, 15)?,
            "edge_winner_ordinals",
            codecs.user_descriptor.columns.len(),
        )?,
        global_winner_ordinals: decode_optional_winner_ordinals_bytes(
            column_bytes_with_layout(descriptor, layout, data, 16)?,
            "global_winner_ordinals",
            codecs.user_descriptor.columns.len(),
        )?,
        merge_artifacts: column_bytes_with_layout(descriptor, layout, data, 17)?
            .map(|bytes| bytes.to_vec()),
    })
}
