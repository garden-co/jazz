//! Synchronous Storage trait and implementations.
//!
//! This is the foundation of the sync storage architecture. All storage
//! and index operations are synchronous - they return immediately with results.
//!
//! # Design: Single-threaded
//!
//! No `Send + Sync` bounds on Storage. Each thread (main, worker) has its own
//! Storage instance. Cross-thread communication uses the sync protocol over
//! postMessage, not shared mutable state.

#[cfg(test)]
pub mod conformance;
mod key_codec;
mod opfs_btree;
mod storage_core;
pub use opfs_btree::OpfsBTreeStorage;
#[cfg(all(feature = "rocksdb", not(target_arch = "wasm32")))]
mod rocksdb;
#[cfg(all(feature = "rocksdb", not(target_arch = "wasm32")))]
pub use rocksdb::RocksDBStorage;
#[cfg(all(feature = "sqlite", not(target_arch = "wasm32")))]
mod sqlite;
#[cfg(all(feature = "sqlite", not(target_arch = "wasm32")))]
pub use sqlite::SqliteStorage;

use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};
use std::ops::Bound;

use serde::{Deserialize, Serialize};
use smolset::SmolSet;

use crate::batch_fate::{
    BatchSettlement, CapturedFrontierMember, LocalBatchRecord, SealedBatchSubmission,
};
use crate::catalogue::CatalogueEntry;
use crate::digest::Digest32;
use crate::metadata::MetadataKey;
use crate::object::{BranchName, ObjectId};
use crate::query_manager::types::{
    ColumnDescriptor, ColumnType, ComposedBranchName, RowDescriptor, SchemaHash, SharedString,
    Value,
};
use crate::row_format::{decode_row, encode_row};
use crate::row_histories::{
    BatchId, HistoryScan, QueryRowBatch, RowState, StoredRowBatch, VisibleRowEntry,
};
use crate::sync_manager::DurabilityTier;

// ============================================================================
// Storage Types
// ============================================================================

type EncodedHistoryRowKey = (ObjectId, SharedString, BatchId);
type EncodedTableRowHistories = BTreeMap<EncodedHistoryRowKey, Vec<u8>>;

/// Errors from storage operations.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum StorageError {
    NotFound,
    IoError(String),
    IndexKeyTooLarge {
        table: String,
        column: String,
        branch: String,
        key_bytes: usize,
        max_key_bytes: usize,
    },
}

impl std::fmt::Display for StorageError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            StorageError::NotFound => write!(f, "not found"),
            StorageError::IoError(message) => write!(f, "{message}"),
            StorageError::IndexKeyTooLarge {
                table,
                column,
                branch,
                key_bytes,
                max_key_bytes,
            } => write!(
                f,
                "indexed value too large for {table}.{column} on branch {branch}: index key would be {key_bytes} bytes (max {max_key_bytes})"
            ),
        }
    }
}

impl std::error::Error for StorageError {}

pub(crate) fn validate_index_value_size(
    table: &str,
    column: &str,
    branch: &str,
    value: &Value,
) -> Result<(), StorageError> {
    key_codec::validate_index_entry_size(table, column, branch, value)
}

pub type MetadataRows = Vec<(ObjectId, HashMap<String, String>)>;
pub type RowLocatorRows = Vec<(ObjectId, RowLocator)>;
pub type RawTableRows = Vec<(String, Vec<u8>)>;
pub type RawTableKeys = Vec<String>;

const METADATA_TABLE: &str = "__metadata";
const ROW_LOCATOR_TABLE: &str = "__row_locator";
const LOCAL_BATCH_RECORD_TABLE: &str = "__local_batch_record";
const AUTHORITATIVE_BATCH_SETTLEMENT_TABLE: &str = "__authoritative_batch_settlement";
const SEALED_BATCH_SUBMISSION_TABLE: &str = "__sealed_batch_submission";
const ROW_NAMESPACE_HEADER_TABLE: &str = "__row_namespace_header";
const BRANCH_ORD_REGISTRY_TABLE: &str = "__branch_ord_registry";
const BRANCH_ORD_REGISTRY_KEY: &str = "registry";
const ROW_STORAGE_FORMAT_V1: i32 = 1;
const BRANCH_ORD_REGISTRY_FORMAT_V1: i32 = 1;

pub type BranchOrd = i32;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RowLocator {
    pub table: SharedString,
    pub origin_schema_hash: Option<SchemaHash>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
pub enum RowNamespaceKind {
    Visible,
    History,
}

impl RowNamespaceKind {
    fn as_str(self) -> &'static str {
        match self {
            Self::Visible => "visible",
            Self::History => "history",
        }
    }

    fn from_str(raw: &str) -> Result<Self, StorageError> {
        match raw {
            "visible" => Ok(Self::Visible),
            "history" => Ok(Self::History),
            other => Err(StorageError::IoError(format!(
                "unknown row namespace kind '{other}'"
            ))),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct RowNamespaceId {
    pub kind: RowNamespaceKind,
    pub table_name: SharedString,
    pub schema_hash: SchemaHash,
}

impl RowNamespaceId {
    pub fn new(
        kind: RowNamespaceKind,
        table_name: impl Into<String>,
        schema_hash: SchemaHash,
    ) -> Self {
        Self {
            kind,
            table_name: table_name.into().into(),
            schema_hash,
        }
    }

    pub fn raw_table_name(&self) -> String {
        format!(
            "rowns:{}:{}:{}",
            self.kind.as_str(),
            self.table_name,
            self.schema_hash
        )
    }

    fn parse_raw_table_name(raw: &str) -> Result<Self, StorageError> {
        let Some(rest) = raw.strip_prefix("rowns:") else {
            return Err(StorageError::IoError(format!(
                "invalid row namespace id '{raw}'"
            )));
        };
        let mut parts = rest.splitn(3, ':');
        let kind =
            RowNamespaceKind::from_str(parts.next().ok_or_else(|| {
                StorageError::IoError(format!("invalid row namespace id '{raw}'"))
            })?)?;
        let table_name = parts
            .next()
            .ok_or_else(|| StorageError::IoError(format!("invalid row namespace id '{raw}'")))?;
        let schema_hash = parts
            .next()
            .and_then(SchemaHash::from_hex)
            .ok_or_else(|| StorageError::IoError(format!("invalid row namespace id '{raw}'")))?;
        Ok(Self::new(kind, table_name, schema_hash))
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RowNamespaceHeader {
    pub storage_format_version: i32,
    pub schema_hash: SchemaHash,
    pub table_name: SharedString,
}

impl RowNamespaceHeader {
    pub fn v1(table_name: impl Into<String>, schema_hash: SchemaHash) -> Self {
        Self {
            storage_format_version: ROW_STORAGE_FORMAT_V1,
            schema_hash,
            table_name: table_name.into().into(),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct BranchOrdRegistryEntry {
    branch_ord: BranchOrd,
    branch_name: BranchName,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct BranchOrdRegistry {
    storage_format_version: i32,
    next_ord: BranchOrd,
    entries: Vec<BranchOrdRegistryEntry>,
}

impl Default for BranchOrdRegistry {
    fn default() -> Self {
        Self {
            storage_format_version: BRANCH_ORD_REGISTRY_FORMAT_V1,
            next_ord: 1,
            entries: Vec::new(),
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub enum IndexMutation<'a> {
    Insert {
        table: &'a str,
        column: &'a str,
        branch: &'a str,
        value: Value,
        row_id: ObjectId,
    },
    Remove {
        table: &'a str,
        column: &'a str,
        branch: &'a str,
        value: Value,
        row_id: ObjectId,
    },
}

pub struct HistoryRowBytes<'a> {
    pub namespace_raw_table: &'a str,
    pub branch: &'a str,
    pub row_id: ObjectId,
    pub batch_id: BatchId,
    pub bytes: &'a [u8],
}

pub(crate) struct OwnedHistoryRowBytes {
    pub namespace: RowNamespaceId,
    pub namespace_raw_table: String,
    pub branch: String,
    pub row_id: ObjectId,
    pub batch_id: BatchId,
    pub bytes: Vec<u8>,
}

pub struct VisibleRowBytes<'a> {
    pub namespace_raw_table: &'a str,
    pub branch: &'a str,
    pub row_id: ObjectId,
    pub bytes: &'a [u8],
}

pub(crate) struct OwnedVisibleRowBytes {
    pub namespace: RowNamespaceId,
    pub namespace_raw_table: String,
    pub branch: String,
    pub row_id: ObjectId,
    pub bytes: Vec<u8>,
}

fn metadata_raw_key(id: ObjectId) -> String {
    hex::encode(id.uuid().as_bytes())
}

fn decode_metadata_raw_key(key: &str) -> Result<ObjectId, StorageError> {
    let bytes = hex::decode(key)
        .map_err(|err| StorageError::IoError(format!("invalid metadata key '{key}': {err}")))?;
    let uuid = uuid::Uuid::from_slice(&bytes)
        .map_err(|err| StorageError::IoError(format!("invalid metadata uuid '{key}': {err}")))?;
    Ok(ObjectId::from_uuid(uuid))
}

fn encode_metadata(metadata: &HashMap<String, String>) -> Result<Vec<u8>, StorageError> {
    serde_json::to_vec(metadata)
        .map_err(|err| StorageError::IoError(format!("serialize metadata: {err}")))
}

fn decode_metadata(bytes: &[u8]) -> Result<HashMap<String, String>, StorageError> {
    serde_json::from_slice(bytes)
        .map_err(|err| StorageError::IoError(format!("deserialize metadata: {err}")))
}

pub(crate) fn row_locator_from_metadata(metadata: &HashMap<String, String>) -> Option<RowLocator> {
    Some(RowLocator {
        table: metadata.get(MetadataKey::Table.as_str())?.clone().into(),
        origin_schema_hash: metadata
            .get(MetadataKey::OriginSchemaHash.as_str())
            .and_then(|raw_hash| SchemaHash::from_hex(raw_hash)),
    })
}

pub(crate) fn metadata_from_row_locator(locator: &RowLocator) -> HashMap<String, String> {
    let mut metadata = HashMap::from([(MetadataKey::Table.to_string(), locator.table.to_string())]);
    if let Some(origin_schema_hash) = locator.origin_schema_hash {
        metadata.insert(
            MetadataKey::OriginSchemaHash.to_string(),
            origin_schema_hash.to_string(),
        );
    }
    metadata
}

fn encode_row_locator(locator: &RowLocator) -> Result<Vec<u8>, StorageError> {
    postcard::to_allocvec(locator)
        .map_err(|err| StorageError::IoError(format!("serialize row locator: {err}")))
}

fn decode_row_locator(bytes: &[u8]) -> Result<RowLocator, StorageError> {
    postcard::from_bytes(bytes)
        .map_err(|err| StorageError::IoError(format!("deserialize row locator: {err}")))
}

fn local_batch_record_key(batch_id: BatchId) -> String {
    format!("batch:{}", hex::encode(batch_id.as_bytes()))
}

fn decode_local_batch_record_key(key: &str) -> Result<BatchId, StorageError> {
    let Some(hex_id) = key.strip_prefix("batch:") else {
        return Err(StorageError::IoError(format!(
            "invalid local batch record key '{key}'"
        )));
    };
    let bytes = hex::decode(hex_id).map_err(|err| {
        StorageError::IoError(format!("invalid local batch record key '{key}': {err}"))
    })?;
    let bytes: [u8; 16] = bytes.try_into().map_err(|_| {
        StorageError::IoError(format!(
            "invalid local batch record batch id '{key}': expected 16 bytes, got {}",
            hex_id.len() / 2
        ))
    })?;
    Ok(BatchId(bytes))
}

fn branch_ord_registry_storage_descriptor() -> RowDescriptor {
    RowDescriptor::new(vec![
        ColumnDescriptor::new("storage_format_version", ColumnType::Integer),
        ColumnDescriptor::new("next_ord", ColumnType::Integer),
        ColumnDescriptor::new(
            "entries",
            ColumnType::Array {
                element: Box::new(ColumnType::Row {
                    columns: Box::new(RowDescriptor::new(vec![
                        ColumnDescriptor::new("branch_ord", ColumnType::Integer),
                        ColumnDescriptor::new("branch_name", ColumnType::Text),
                    ])),
                }),
            },
        ),
    ])
}

fn encode_branch_ord_registry(registry: &BranchOrdRegistry) -> Result<Vec<u8>, StorageError> {
    let mut entries = registry.entries.clone();
    entries.sort_by_key(|entry| entry.branch_ord);
    encode_row(
        &branch_ord_registry_storage_descriptor(),
        &[
            Value::Integer(registry.storage_format_version),
            Value::Integer(registry.next_ord),
            Value::Array(
                entries
                    .into_iter()
                    .map(|entry| Value::Row {
                        id: None,
                        values: vec![
                            Value::Integer(entry.branch_ord),
                            Value::Text(entry.branch_name.as_str().to_string()),
                        ],
                    })
                    .collect(),
            ),
        ],
    )
    .map_err(|err| StorageError::IoError(format!("encode branch ord registry: {err}")))
}

fn decode_branch_ord_registry(bytes: &[u8]) -> Result<BranchOrdRegistry, StorageError> {
    let values = decode_row(&branch_ord_registry_storage_descriptor(), bytes)
        .map_err(|err| StorageError::IoError(format!("decode branch ord registry: {err}")))?;
    let [storage_format_version, next_ord, entries] = values.as_slice() else {
        return Err(StorageError::IoError(
            "unexpected branch ord registry shape".to_string(),
        ));
    };

    let Value::Integer(storage_format_version) = storage_format_version else {
        return Err(StorageError::IoError(
            "branch ord registry storage_format_version must be Integer".to_string(),
        ));
    };
    if *storage_format_version != BRANCH_ORD_REGISTRY_FORMAT_V1 {
        return Err(StorageError::IoError(format!(
            "unsupported branch ord registry storage format version {}",
            storage_format_version
        )));
    }

    let Value::Integer(next_ord) = next_ord else {
        return Err(StorageError::IoError(
            "branch ord registry next_ord must be Integer".to_string(),
        ));
    };
    if *next_ord < 1 {
        return Err(StorageError::IoError(format!(
            "branch ord registry next_ord must be >= 1, got {}",
            next_ord
        )));
    }

    let Value::Array(entries) = entries else {
        return Err(StorageError::IoError(
            "branch ord registry entries must be Array".to_string(),
        ));
    };

    let mut seen_branch_ords = HashSet::new();
    let mut seen_branch_names = HashSet::new();
    let mut decoded_entries = entries
        .iter()
        .map(|entry| match entry {
            Value::Row { values, .. } => {
                let [branch_ord, branch_name] = values.as_slice() else {
                    return Err(StorageError::IoError(
                        "expected branch ord registry row to have two values".to_string(),
                    ));
                };
                let branch_ord = match branch_ord {
                    Value::Integer(branch_ord) => *branch_ord,
                    other => {
                        return Err(StorageError::IoError(format!(
                            "expected branch ord registry branch_ord integer, got {other:?}"
                        )));
                    }
                };
                let branch_name = match branch_name {
                    Value::Text(branch_name) => BranchName::new(branch_name.clone()),
                    other => {
                        return Err(StorageError::IoError(format!(
                            "expected branch ord registry branch_name text, got {other:?}"
                        )));
                    }
                };
                if branch_ord < 1 {
                    return Err(StorageError::IoError(format!(
                        "branch ord registry branch ord must be >= 1, got {branch_ord}"
                    )));
                }
                if !seen_branch_ords.insert(branch_ord) {
                    return Err(StorageError::IoError(format!(
                        "duplicate branch ord registry branch ord {branch_ord}"
                    )));
                }
                if !seen_branch_names.insert(branch_name) {
                    return Err(StorageError::IoError(format!(
                        "duplicate branch ord registry branch name {}",
                        branch_name.as_str()
                    )));
                }
                Ok(BranchOrdRegistryEntry {
                    branch_ord,
                    branch_name,
                })
            }
            other => Err(StorageError::IoError(format!(
                "expected branch ord registry entry row, got {other:?}"
            ))),
        })
        .collect::<Result<Vec<_>, StorageError>>()?;
    decoded_entries.sort_by_key(|entry| entry.branch_ord);

    Ok(BranchOrdRegistry {
        storage_format_version: *storage_format_version,
        next_ord: *next_ord,
        entries: decoded_entries,
    })
}

fn load_branch_ord_registry<H: Storage + ?Sized>(
    storage: &H,
) -> Result<BranchOrdRegistry, StorageError> {
    match storage.raw_table_get(BRANCH_ORD_REGISTRY_TABLE, BRANCH_ORD_REGISTRY_KEY)? {
        Some(bytes) => decode_branch_ord_registry(&bytes),
        None => Ok(BranchOrdRegistry::default()),
    }
}

fn store_branch_ord_registry<H: Storage + ?Sized>(
    storage: &mut H,
    registry: &BranchOrdRegistry,
) -> Result<(), StorageError> {
    let bytes = encode_branch_ord_registry(registry)?;
    storage.raw_table_put(BRANCH_ORD_REGISTRY_TABLE, BRANCH_ORD_REGISTRY_KEY, &bytes)
}

fn row_namespace_header_storage_descriptor() -> RowDescriptor {
    RowDescriptor::new(vec![
        ColumnDescriptor::new("storage_format_version", ColumnType::Integer),
        ColumnDescriptor::new("schema_hash", ColumnType::Bytea),
        ColumnDescriptor::new("table_name", ColumnType::Text),
    ])
}

fn encode_row_namespace_header(header: &RowNamespaceHeader) -> Result<Vec<u8>, StorageError> {
    encode_row(
        &row_namespace_header_storage_descriptor(),
        &[
            Value::Integer(header.storage_format_version),
            Value::Bytea(header.schema_hash.as_bytes().to_vec()),
            Value::Text(header.table_name.to_string()),
        ],
    )
    .map_err(|err| StorageError::IoError(format!("encode row namespace header: {err}")))
}

fn decode_row_namespace_header(bytes: &[u8]) -> Result<RowNamespaceHeader, StorageError> {
    let values = decode_row(&row_namespace_header_storage_descriptor(), bytes)
        .map_err(|err| StorageError::IoError(format!("decode row namespace header: {err}")))?;
    let [storage_format_version, schema_hash, table_name] = values.as_slice() else {
        return Err(StorageError::IoError(
            "unexpected row namespace header shape".to_string(),
        ));
    };

    let Value::Integer(storage_format_version) = storage_format_version else {
        return Err(StorageError::IoError(
            "row namespace header storage_format_version must be Integer".to_string(),
        ));
    };
    let Value::Bytea(schema_hash) = schema_hash else {
        return Err(StorageError::IoError(
            "row namespace header schema_hash must be Bytea".to_string(),
        ));
    };
    let schema_hash: [u8; 32] = schema_hash.as_slice().try_into().map_err(|_| {
        StorageError::IoError(format!(
            "row namespace header schema_hash must be 32 bytes, got {}",
            schema_hash.len()
        ))
    })?;
    let Value::Text(table_name) = table_name else {
        return Err(StorageError::IoError(
            "row namespace header table_name must be Text".to_string(),
        ));
    };

    Ok(RowNamespaceHeader {
        storage_format_version: *storage_format_version,
        schema_hash: SchemaHash::from_bytes(schema_hash),
        table_name: table_name.to_string().into(),
    })
}

fn history_row_namespace_id(table: &str, schema_hash: SchemaHash) -> RowNamespaceId {
    RowNamespaceId::new(RowNamespaceKind::History, table, schema_hash)
}

fn visible_row_namespace_id(table: &str, schema_hash: SchemaHash) -> RowNamespaceId {
    RowNamespaceId::new(RowNamespaceKind::Visible, table, schema_hash)
}

fn load_user_descriptor_for_schema_hash<H: Storage + ?Sized>(
    storage: &H,
    table_name: &str,
    schema_hash: SchemaHash,
) -> Result<RowDescriptor, StorageError> {
    load_history_user_descriptor_for_schema_hash(storage, table_name, schema_hash)?.ok_or_else(
        || {
            StorageError::IoError(format!(
                "missing catalogue descriptor for table {table_name} at schema {schema_hash}"
            ))
        },
    )
}

fn row_namespace_ids_for_table<H: Storage + ?Sized>(
    storage: &H,
    kind: RowNamespaceKind,
    table: &str,
) -> Result<Vec<RowNamespaceId>, StorageError> {
    Ok(storage
        .scan_row_namespace_headers()?
        .into_iter()
        .map(|(namespace, _)| namespace)
        .filter(|namespace| namespace.kind == kind && namespace.table_name == table)
        .collect())
}

fn row_namespace_ids_for_exact_row<H: Storage + ?Sized>(
    storage: &H,
    kind: RowNamespaceKind,
    table: &str,
    row_id: ObjectId,
) -> Result<Vec<RowNamespaceId>, StorageError> {
    if let Some(row_locator) = storage.load_row_locator(row_id)?
        && let Some(schema_hash) = row_locator.origin_schema_hash
    {
        let mut exact = vec![RowNamespaceId::new(kind, table, schema_hash)];
        if row_locator.table.as_str() != table {
            exact.push(RowNamespaceId::new(
                kind,
                row_locator.table.as_str(),
                schema_hash,
            ));
        }
        exact.sort_by_key(|left| left.raw_table_name());
        exact.dedup();
        return Ok(exact);
    }

    row_namespace_ids_for_table(storage, kind, table)
}

fn sealed_batch_submission_storage_descriptor_with_branch_ords() -> RowDescriptor {
    RowDescriptor::new(vec![
        ColumnDescriptor::new("batch_id", ColumnType::Bytea),
        ColumnDescriptor::new("target_branch_ord", ColumnType::Integer),
        ColumnDescriptor::new("batch_digest", ColumnType::Bytea),
        ColumnDescriptor::new(
            "members",
            ColumnType::Array {
                element: Box::new(ColumnType::Row {
                    columns: Box::new(RowDescriptor::new(vec![
                        ColumnDescriptor::new("object_id", ColumnType::Bytea),
                        ColumnDescriptor::new("row_digest", ColumnType::Bytea),
                    ])),
                }),
            },
        ),
        ColumnDescriptor::new(
            "captured_frontier",
            ColumnType::Array {
                element: Box::new(ColumnType::Row {
                    columns: Box::new(RowDescriptor::new(vec![
                        ColumnDescriptor::new("object_id", ColumnType::Bytea),
                        ColumnDescriptor::new("branch_ord", ColumnType::Integer),
                        ColumnDescriptor::new("batch_id", ColumnType::Bytea),
                    ])),
                }),
            },
        ),
    ])
}

fn local_batch_record_storage_descriptor_with_branch_ords() -> RowDescriptor {
    RowDescriptor::new(vec![
        ColumnDescriptor::new("batch_id", ColumnType::Bytea),
        ColumnDescriptor::new("mode", ColumnType::Text),
        ColumnDescriptor::new("requested_tier", ColumnType::Text),
        ColumnDescriptor::new("sealed", ColumnType::Boolean),
        ColumnDescriptor::new(
            "members",
            ColumnType::Array {
                element: Box::new(ColumnType::Row {
                    columns: Box::new(RowDescriptor::new(vec![
                        ColumnDescriptor::new("object_id", ColumnType::Bytea),
                        ColumnDescriptor::new("table_name", ColumnType::Text),
                        ColumnDescriptor::new("branch_ord", ColumnType::Integer),
                        ColumnDescriptor::new("schema_hash", ColumnType::Bytea),
                        ColumnDescriptor::new("row_digest", ColumnType::Bytea),
                    ])),
                }),
            },
        ),
        ColumnDescriptor::new("sealed_submission", ColumnType::Bytea).nullable(),
        ColumnDescriptor::new("latest_settlement", ColumnType::Bytea).nullable(),
    ])
}

fn encode_batch_mode(mode: crate::batch_fate::BatchMode) -> &'static str {
    match mode {
        crate::batch_fate::BatchMode::Direct => "direct",
        crate::batch_fate::BatchMode::Transactional => "transactional",
    }
}

fn decode_batch_mode(raw: &str) -> Result<crate::batch_fate::BatchMode, StorageError> {
    match raw {
        "direct" => Ok(crate::batch_fate::BatchMode::Direct),
        "transactional" => Ok(crate::batch_fate::BatchMode::Transactional),
        other => Err(StorageError::IoError(format!(
            "unknown batch mode '{other}'"
        ))),
    }
}

fn encode_durability_tier(tier: DurabilityTier) -> &'static str {
    match tier {
        DurabilityTier::Worker => "worker",
        DurabilityTier::EdgeServer => "edge",
        DurabilityTier::GlobalServer => "global",
    }
}

fn decode_durability_tier(raw: &str) -> Result<DurabilityTier, StorageError> {
    match raw {
        "worker" => Ok(DurabilityTier::Worker),
        "edge" => Ok(DurabilityTier::EdgeServer),
        "global" => Ok(DurabilityTier::GlobalServer),
        other => Err(StorageError::IoError(format!(
            "unknown durability tier '{other}'"
        ))),
    }
}

fn load_history_user_descriptor_for_schema_hash<H: Storage + ?Sized>(
    storage: &H,
    table_hint: &str,
    schema_hash: SchemaHash,
) -> Result<Option<crate::query_manager::types::RowDescriptor>, StorageError> {
    let Some(entry) = storage.load_catalogue_entry(schema_hash.to_object_id())? else {
        return Ok(None);
    };
    let schema = crate::schema_manager::encoding::decode_schema(&entry.content)
        .map_err(|err| StorageError::IoError(format!("decode schema for row history: {err}")))?;

    let hinted_table_name = crate::query_manager::types::TableName::new(table_hint);
    Ok(schema
        .get(&hinted_table_name)
        .map(|table_schema| table_schema.columns.clone()))
}

fn required_history_user_descriptor_and_schema_hash_for_row<H: Storage + ?Sized>(
    storage: &H,
    table_hint: &str,
    row: &StoredRowBatch,
) -> Result<(SchemaHash, crate::query_manager::types::RowDescriptor), StorageError> {
    let row_data_matches = |descriptor: &crate::query_manager::types::RowDescriptor| {
        row.data.is_empty() || crate::row_format::decode_row(descriptor, &row.data).is_ok()
    };

    if let Some(row_locator) = storage.load_row_locator(row.row_id)?
        && let Some(origin_schema_hash) = row_locator.origin_schema_hash
    {
        if let Some(descriptor) = load_history_user_descriptor_for_schema_hash(
            storage,
            row_locator.table.as_str(),
            origin_schema_hash,
        )? && row_data_matches(&descriptor)
        {
            return Ok((origin_schema_hash, descriptor));
        }

        if row_locator.table.as_str() != table_hint
            && let Some(descriptor) = load_history_user_descriptor_for_schema_hash(
                storage,
                table_hint,
                origin_schema_hash,
            )?
            && row_data_matches(&descriptor)
        {
            return Ok((origin_schema_hash, descriptor));
        }
    }
    Err(StorageError::IoError(format!(
        "missing exact schema hash for history row {} in table {} on branch {} (row_locator={:?})",
        row.row_id,
        table_hint,
        row.branch,
        storage.load_row_locator(row.row_id)?,
    )))
}

pub(crate) fn encode_history_row_bytes_for_storage<H: Storage + ?Sized>(
    storage: &H,
    table: &str,
    rows: &[StoredRowBatch],
) -> Result<Vec<OwnedHistoryRowBytes>, StorageError> {
    rows.iter()
        .map(|row| {
            let (schema_hash, user_descriptor) =
                required_history_user_descriptor_and_schema_hash_for_row(storage, table, row)?;
            let namespace = history_row_namespace_id(table, schema_hash);
            let bytes = crate::row_histories::encode_flat_history_row(&user_descriptor, row)
                .map_err(|err| StorageError::IoError(format!("encode flat history row: {err}")))?;

            Ok(OwnedHistoryRowBytes {
                namespace_raw_table: namespace.raw_table_name(),
                namespace,
                branch: row.branch.to_string(),
                row_id: row.row_id,
                batch_id: row.batch_id(),
                bytes,
            })
        })
        .collect()
}

pub(crate) fn encode_visible_row_bytes_for_storage<H: Storage + ?Sized>(
    storage: &H,
    table: &str,
    entries: &[VisibleRowEntry],
) -> Result<Vec<OwnedVisibleRowBytes>, StorageError> {
    entries
        .iter()
        .map(|entry| {
            let (schema_hash, user_descriptor) =
                required_history_user_descriptor_and_schema_hash_for_row(
                    storage,
                    table,
                    &entry.current_row,
                )?;
            let namespace = visible_row_namespace_id(table, schema_hash);
            let bytes =
                crate::row_histories::encode_flat_visible_row_entry(&user_descriptor, entry)
                    .map_err(|err| {
                        StorageError::IoError(format!("encode flat visible row: {err}"))
                    })?;

            Ok(OwnedVisibleRowBytes {
                namespace_raw_table: namespace.raw_table_name(),
                namespace,
                branch: entry.current_row.branch.to_string(),
                row_id: entry.current_row.row_id,
                bytes,
            })
        })
        .collect()
}

fn decode_history_row_bytes_in_namespace<H: Storage + ?Sized>(
    storage: &H,
    namespace: &RowNamespaceId,
    row_id: ObjectId,
    branch: &str,
    batch_id: BatchId,
    bytes: &[u8],
) -> Result<StoredRowBatch, StorageError> {
    let user_descriptor = load_user_descriptor_for_schema_hash(
        storage,
        namespace.table_name.as_str(),
        namespace.schema_hash,
    )?;
    crate::row_histories::decode_flat_history_row(&user_descriptor, row_id, branch, batch_id, bytes)
        .map_err(|err| StorageError::IoError(format!("decode flat history row: {err}")))
}

fn decode_visible_row_entry_bytes_in_namespace<H: Storage + ?Sized>(
    storage: &H,
    namespace: &RowNamespaceId,
    row_id: ObjectId,
    branch: &str,
    bytes: &[u8],
) -> Result<VisibleRowEntry, StorageError> {
    let user_descriptor = load_user_descriptor_for_schema_hash(
        storage,
        namespace.table_name.as_str(),
        namespace.schema_hash,
    )?;
    crate::row_histories::decode_flat_visible_row_entry(&user_descriptor, row_id, branch, bytes)
        .map_err(|err| StorageError::IoError(format!("decode flat visible row: {err}")))
}

pub(super) fn scan_history_row_bytes_with_storage<H: Storage + ?Sized>(
    storage: &H,
    table: &str,
    scan: HistoryScan,
) -> Result<Vec<OwnedHistoryRowBytes>, StorageError> {
    let namespaces = row_namespace_ids_for_table(storage, RowNamespaceKind::History, table)?;
    let prefix = match scan {
        HistoryScan::Branch | HistoryScan::AsOf { .. } => {
            key_codec::history_namespace_row_prefix(None)
        }
        HistoryScan::Row { row_id } => key_codec::history_namespace_row_prefix(Some(row_id)),
    };
    let mut rows = Vec::new();
    for namespace in namespaces {
        let namespace_raw_table = namespace.raw_table_name();
        for (key, bytes) in storage.raw_table_scan_prefix(&namespace_raw_table, &prefix)? {
            let (row_id, branch, batch_id) = key_codec::decode_history_namespace_row_key(&key)?;
            rows.push(OwnedHistoryRowBytes {
                namespace: namespace.clone(),
                namespace_raw_table: namespace_raw_table.clone(),
                branch,
                row_id,
                batch_id,
                bytes,
            });
        }
    }
    Ok(rows)
}

pub(super) fn scan_visible_row_bytes_with_storage<H: Storage + ?Sized>(
    storage: &H,
    table: &str,
    branch: &str,
) -> Result<Vec<OwnedVisibleRowBytes>, StorageError> {
    let namespaces = row_namespace_ids_for_table(storage, RowNamespaceKind::Visible, table)?;
    let prefix = key_codec::visible_namespace_row_prefix(branch);
    let mut rows = Vec::new();
    for namespace in namespaces {
        let namespace_raw_table = namespace.raw_table_name();
        for (key, bytes) in storage.raw_table_scan_prefix(&namespace_raw_table, &prefix)? {
            let (decoded_branch, row_id) = key_codec::decode_visible_namespace_row_key(&key)?;
            if decoded_branch != branch {
                return Err(StorageError::IoError(format!(
                    "visible namespace row key '{key}' decoded unexpected branch '{decoded_branch}'"
                )));
            }
            rows.push(OwnedVisibleRowBytes {
                namespace: namespace.clone(),
                namespace_raw_table: namespace_raw_table.clone(),
                branch: decoded_branch,
                row_id,
                bytes,
            });
        }
    }
    Ok(rows)
}

pub(super) fn load_history_row_batch_row_bytes_with_storage<H: Storage + ?Sized>(
    storage: &H,
    table: &str,
    branch: &str,
    row_id: ObjectId,
    batch_id: BatchId,
) -> Result<Option<OwnedHistoryRowBytes>, StorageError> {
    let namespaces =
        row_namespace_ids_for_exact_row(storage, RowNamespaceKind::History, table, row_id)?;
    let key = key_codec::history_namespace_row_key(row_id, branch, batch_id);
    for namespace in namespaces {
        let namespace_raw_table = namespace.raw_table_name();
        if let Some(bytes) = storage.raw_table_get(&namespace_raw_table, &key)? {
            return Ok(Some(OwnedHistoryRowBytes {
                namespace,
                namespace_raw_table,
                branch: branch.to_string(),
                row_id,
                batch_id,
                bytes,
            }));
        }
    }
    Ok(None)
}

pub(super) fn load_history_row_batch_bytes_in_namespace_with_storage<H: Storage + ?Sized>(
    storage: &H,
    namespace: &RowNamespaceId,
    branch: &str,
    row_id: ObjectId,
    batch_id: BatchId,
) -> Result<Option<Vec<u8>>, StorageError> {
    let key = key_codec::history_namespace_row_key(row_id, branch, batch_id);
    storage.raw_table_get(&namespace.raw_table_name(), &key)
}

pub(super) fn load_visible_region_row_bytes_with_storage<H: Storage + ?Sized>(
    storage: &H,
    table: &str,
    branch: &str,
    row_id: ObjectId,
) -> Result<Option<OwnedVisibleRowBytes>, StorageError> {
    let namespaces =
        row_namespace_ids_for_exact_row(storage, RowNamespaceKind::Visible, table, row_id)?;
    let key = key_codec::visible_namespace_row_key(branch, row_id);
    for namespace in namespaces {
        let namespace_raw_table = namespace.raw_table_name();
        if let Some(bytes) = storage.raw_table_get(&namespace_raw_table, &key)? {
            return Ok(Some(OwnedVisibleRowBytes {
                namespace,
                namespace_raw_table,
                branch: branch.to_string(),
                row_id,
                bytes,
            }));
        }
    }
    Ok(None)
}

pub(super) fn scan_visible_region_row_batch_branches_with_storage<H: Storage + ?Sized>(
    storage: &H,
    table: &str,
    row_id: ObjectId,
) -> Result<Vec<String>, StorageError> {
    let namespaces = row_namespace_ids_for_table(storage, RowNamespaceKind::History, table)?;
    let prefix = key_codec::history_namespace_row_prefix(Some(row_id));
    let mut branches = Vec::new();
    for namespace in namespaces {
        for key in storage.raw_table_scan_prefix_keys(&namespace.raw_table_name(), &prefix)? {
            let (_decoded_row_id, branch, _batch_id) =
                key_codec::decode_history_namespace_row_key(&key)?;
            branches.push(branch);
        }
    }
    branches.sort();
    branches.dedup();
    Ok(branches)
}

pub(crate) fn patch_row_region_rows_by_batch_with_storage<H: Storage + ?Sized>(
    storage: &mut H,
    table: &str,
    batch_id: crate::row_histories::BatchId,
    state: Option<RowState>,
    confirmed_tier: Option<DurabilityTier>,
) -> Result<(), StorageError> {
    let history_rows = scan_history_row_bytes_with_storage(storage, table, HistoryScan::Branch)?
        .into_iter()
        .map(|row| {
            decode_history_row_bytes_in_namespace(
                storage,
                &row.namespace,
                row.row_id,
                row.branch.as_str(),
                row.batch_id,
                &row.bytes,
            )
        })
        .collect::<Result<Vec<_>, _>>()?;

    let mut patched_history = Vec::new();
    let mut history_by_visible_row = HashMap::<(String, ObjectId), Vec<StoredRowBatch>>::new();
    let mut affected_visible_rows = HashSet::<(String, ObjectId)>::new();

    for mut row in history_rows {
        if row.batch_id == batch_id {
            if let Some(state) = state {
                row.state = state;
            }
            row.confirmed_tier = match (row.confirmed_tier, confirmed_tier) {
                (Some(existing), Some(incoming)) => Some(existing.max(incoming)),
                (Some(existing), None) => Some(existing),
                (None, incoming) => incoming,
            };
            affected_visible_rows.insert((row.branch.to_string(), row.row_id));
            patched_history.push(row.clone());
        }

        history_by_visible_row
            .entry((row.branch.to_string(), row.row_id))
            .or_default()
            .push(row);
    }

    if !patched_history.is_empty() {
        storage.append_history_region_rows(table, &patched_history)?;
    }

    let mut rebuilt_visible_entries = Vec::new();
    let mut rows_without_visible_head = Vec::new();
    for (branch, row_id) in &affected_visible_rows {
        let Some(existing_entry) = storage.load_visible_region_entry(table, branch, *row_id)?
        else {
            continue;
        };

        let history_rows = history_by_visible_row
            .remove(&(branch.clone(), *row_id))
            .unwrap_or_default();
        if let Some(current_row) = history_rows
            .iter()
            .find(|row| row.batch_id() == existing_entry.current_row.batch_id())
            .cloned()
        {
            rebuilt_visible_entries.push(VisibleRowEntry::rebuild(current_row, &history_rows));
            continue;
        }

        let mut current = existing_entry.current_row.clone();
        if current.batch_id == batch_id {
            if let Some(state) = state {
                current.state = state;
            }
            current.confirmed_tier = match (current.confirmed_tier, confirmed_tier) {
                (Some(existing), Some(incoming)) => Some(existing.max(incoming)),
                (Some(existing), None) => Some(existing),
                (None, incoming) => incoming,
            };
        }

        if current.state.is_visible() {
            rebuilt_visible_entries.push(VisibleRowEntry::rebuild(current, &history_rows));
        } else {
            rows_without_visible_head.push((branch.clone(), *row_id));
        }
    }

    if !rebuilt_visible_entries.is_empty() {
        storage.upsert_visible_region_rows(table, &rebuilt_visible_entries)?;
    }
    for (branch, row_id) in rows_without_visible_head {
        storage.delete_visible_region_row(table, &branch, row_id)?;
    }

    Ok(())
}

pub(crate) fn patch_exact_row_batch_with_storage<H: Storage + ?Sized>(
    storage: &mut H,
    table: &str,
    branch: &str,
    row_id: ObjectId,
    batch_id: crate::row_histories::BatchId,
    state: Option<RowState>,
    confirmed_tier: Option<DurabilityTier>,
) -> Result<bool, StorageError> {
    let Some(mut row) = storage.load_history_row_batch(table, branch, row_id, batch_id)? else {
        return Ok(false);
    };

    if let Some(state) = state {
        row.state = state;
    }
    row.confirmed_tier = match (row.confirmed_tier, confirmed_tier) {
        (Some(existing), Some(incoming)) => Some(existing.max(incoming)),
        (Some(existing), None) => Some(existing),
        (None, incoming) => incoming,
    };
    storage.append_history_region_rows(table, std::slice::from_ref(&row))?;

    let Some(existing_entry) = storage.load_visible_region_entry(table, branch, row_id)? else {
        return Ok(true);
    };

    let history_rows = storage.scan_history_row_batches(table, row_id)?;
    let current_batch_id = existing_entry.current_row.batch_id();
    let Some(current_row) = history_rows
        .iter()
        .find(|candidate| candidate.branch == branch && candidate.batch_id() == current_batch_id)
        .cloned()
    else {
        storage.delete_visible_region_row(table, branch, row_id)?;
        return Ok(true);
    };

    if current_row.state.is_visible() {
        storage.upsert_visible_region_rows(
            table,
            &[VisibleRowEntry::rebuild(current_row, &history_rows)],
        )?;
    } else {
        storage.delete_visible_region_row(table, branch, row_id)?;
    }

    Ok(true)
}

fn branch_matches_transaction_family(
    branch_name: BranchName,
    target_branch_name: BranchName,
) -> bool {
    match (
        ComposedBranchName::parse(&branch_name),
        ComposedBranchName::parse(&target_branch_name),
    ) {
        (Some(branch), Some(target)) => {
            branch.matches_env_and_branch(&target.env, &target.user_branch)
        }
        _ => branch_name == target_branch_name,
    }
}

fn encode_sealed_batch_submission_with_branch_ords<H: Storage + ?Sized>(
    storage: &mut H,
    submission: &SealedBatchSubmission,
) -> Result<Vec<u8>, StorageError> {
    let target_branch_ord = storage.resolve_or_alloc_branch_ord(submission.target_branch_name)?;
    let member_values = submission
        .members
        .iter()
        .map(|member| {
            Ok(Value::Row {
                id: None,
                values: vec![
                    Value::Bytea(member.object_id.uuid().as_bytes().to_vec()),
                    Value::Bytea(member.row_digest.0.to_vec()),
                ],
            })
        })
        .collect::<Result<Vec<_>, StorageError>>()?;
    let frontier_values = submission
        .captured_frontier
        .iter()
        .map(|member| {
            let branch_ord = storage.resolve_or_alloc_branch_ord(member.branch_name)?;
            Ok(Value::Row {
                id: None,
                values: vec![
                    Value::Bytea(member.object_id.uuid().as_bytes().to_vec()),
                    Value::Integer(branch_ord),
                    Value::Bytea(member.batch_id.0.to_vec()),
                ],
            })
        })
        .collect::<Result<Vec<_>, StorageError>>()?;
    encode_row(
        &sealed_batch_submission_storage_descriptor_with_branch_ords(),
        &[
            Value::Bytea(submission.batch_id.as_bytes().to_vec()),
            Value::Integer(target_branch_ord),
            Value::Bytea(submission.batch_digest.0.to_vec()),
            Value::Array(member_values),
            Value::Array(frontier_values),
        ],
    )
    .map_err(|err| StorageError::IoError(format!("encode sealed batch submission: {err}")))
}

fn decode_sealed_batch_submission_with_branch_ords<H: Storage + ?Sized>(
    storage: &H,
    bytes: &[u8],
) -> Result<SealedBatchSubmission, StorageError> {
    let values = decode_row(
        &sealed_batch_submission_storage_descriptor_with_branch_ords(),
        bytes,
    )
    .map_err(|err| StorageError::IoError(format!("decode sealed batch submission: {err}")))?;
    let [
        batch_id,
        target_branch_ord,
        batch_digest,
        members,
        captured_frontier,
    ] = values.as_slice()
    else {
        return Err(StorageError::IoError(
            "unexpected sealed batch submission shape".to_string(),
        ));
    };

    let batch_id = match batch_id {
        Value::Bytea(bytes) => {
            let bytes: [u8; 16] = bytes.as_slice().try_into().map_err(|_| {
                StorageError::IoError(format!(
                    "decode sealed batch id: expected 16 bytes, got {}",
                    bytes.len()
                ))
            })?;
            BatchId(bytes)
        }
        other => {
            return Err(StorageError::IoError(format!(
                "expected sealed batch id bytes, got {other:?}"
            )));
        }
    };
    let target_branch_ord = match target_branch_ord {
        Value::Integer(raw) => *raw,
        other => {
            return Err(StorageError::IoError(format!(
                "expected target branch ord integer, got {other:?}"
            )));
        }
    };
    let target_branch_name = storage
        .load_branch_name_by_ord(target_branch_ord)?
        .ok_or_else(|| {
            StorageError::IoError(format!(
                "missing branch name for target branch ord {target_branch_ord}"
            ))
        })?;
    let batch_digest = match batch_digest {
        Value::Bytea(bytes) => Digest32(bytes.as_slice().try_into().map_err(|_| {
            StorageError::IoError(format!(
                "expected sealed batch digest to be 32 bytes, got {}",
                bytes.len()
            ))
        })?),
        other => {
            return Err(StorageError::IoError(format!(
                "expected sealed batch digest bytes, got {other:?}"
            )));
        }
    };

    let members = match members {
        Value::Array(elements) => elements
            .iter()
            .map(|element| match element {
                Value::Row { values, .. } => {
                    let [object_id, row_digest] = values.as_slice() else {
                        return Err(StorageError::IoError(
                            "expected sealed batch member row to have two values".to_string(),
                        ));
                    };
                    let object_id = match object_id {
                        Value::Bytea(bytes) => uuid::Uuid::from_slice(bytes)
                            .map(ObjectId::from_uuid)
                            .map_err(|err| {
                                StorageError::IoError(format!(
                                    "decode sealed batch member object id uuid: {err}"
                                ))
                            })?,
                        other => {
                            return Err(StorageError::IoError(format!(
                                "expected sealed batch member object id bytes, got {other:?}"
                            )));
                        }
                    };
                    let row_digest = match row_digest {
                        Value::Bytea(bytes) => Digest32(bytes.as_slice().try_into().map_err(
                            |_| {
                                StorageError::IoError(format!(
                                    "expected sealed batch member row digest to be 32 bytes, got {}",
                                    bytes.len()
                                ))
                            },
                        )?),
                        other => {
                            return Err(StorageError::IoError(format!(
                                "expected sealed batch member row digest bytes, got {other:?}"
                            )));
                        }
                    };
                    Ok(crate::batch_fate::SealedBatchMember {
                        object_id,
                        row_digest,
                    })
                }
                other => Err(StorageError::IoError(format!(
                    "expected sealed batch member row, got {other:?}"
                ))),
            })
            .collect::<Result<Vec<_>, StorageError>>()?,
        other => {
            return Err(StorageError::IoError(format!(
                "expected sealed batch members array, got {other:?}"
            )));
        }
    };

    let captured_frontier = match captured_frontier {
        Value::Array(elements) => elements
            .iter()
            .map(|element| match element {
                Value::Row { values, .. } => {
                    let [object_id, branch_ord, batch_id] = values.as_slice() else {
                        return Err(StorageError::IoError(
                            "expected captured frontier row to have three values".to_string(),
                        ));
                    };
                    let object_id = match object_id {
                        Value::Bytea(bytes) => uuid::Uuid::from_slice(bytes)
                            .map(ObjectId::from_uuid)
                            .map_err(|err| {
                                StorageError::IoError(format!(
                                    "decode captured frontier object id uuid: {err}"
                                ))
                            })?,
                        other => {
                            return Err(StorageError::IoError(format!(
                                "expected captured frontier object id bytes, got {other:?}"
                            )));
                        }
                    };
                    let branch_ord = match branch_ord {
                        Value::Integer(raw) => *raw,
                        other => {
                            return Err(StorageError::IoError(format!(
                                "expected captured frontier branch ord integer, got {other:?}"
                            )));
                        }
                    };
                    let branch_name =
                        storage
                            .load_branch_name_by_ord(branch_ord)?
                            .ok_or_else(|| {
                                StorageError::IoError(format!(
                                    "missing branch name for captured frontier ord {branch_ord}"
                                ))
                            })?;
                    let batch_id = match batch_id {
                        Value::Bytea(bytes) => {
                            BatchId(bytes.as_slice().try_into().map_err(|_| {
                                StorageError::IoError(format!(
                                    "expected captured frontier batch id to be 16 bytes, got {}",
                                    bytes.len()
                                ))
                            })?)
                        }
                        other => {
                            return Err(StorageError::IoError(format!(
                                "expected captured frontier batch id bytes, got {other:?}"
                            )));
                        }
                    };
                    Ok(CapturedFrontierMember {
                        object_id,
                        branch_name,
                        batch_id,
                    })
                }
                other => Err(StorageError::IoError(format!(
                    "expected captured frontier row, got {other:?}"
                ))),
            })
            .collect::<Result<Vec<_>, StorageError>>()?,
        other => {
            return Err(StorageError::IoError(format!(
                "expected captured frontier array, got {other:?}"
            )));
        }
    };

    let submission =
        SealedBatchSubmission::new(batch_id, target_branch_name, members, captured_frontier);
    if submission.batch_digest != batch_digest {
        return Err(StorageError::IoError(format!(
            "sealed batch digest mismatch: expected {batch_digest:?}, computed {:?}",
            submission.batch_digest
        )));
    }
    Ok(submission)
}

fn encode_local_batch_record_with_branch_ords<H: Storage + ?Sized>(
    storage: &mut H,
    record: &LocalBatchRecord,
) -> Result<Vec<u8>, StorageError> {
    let latest_settlement = record
        .latest_settlement
        .as_ref()
        .map(BatchSettlement::encode_storage_row)
        .transpose()
        .map_err(|err| StorageError::IoError(format!("encode local batch settlement: {err}")))?;
    let sealed_submission = record
        .sealed_submission
        .as_ref()
        .map(|submission| encode_sealed_batch_submission_with_branch_ords(storage, submission))
        .transpose()?;
    encode_row(
        &local_batch_record_storage_descriptor_with_branch_ords(),
        &[
            Value::Bytea(record.batch_id.as_bytes().to_vec()),
            Value::Text(encode_batch_mode(record.mode).to_string()),
            Value::Text(encode_durability_tier(record.requested_tier).to_string()),
            Value::Boolean(record.sealed),
            Value::Array(
                record
                    .members
                    .iter()
                    .map(|member| {
                        let branch_ord = storage.resolve_or_alloc_branch_ord(member.branch_name)?;
                        Ok(Value::Row {
                            id: None,
                            values: vec![
                                Value::Bytea(member.object_id.uuid().as_bytes().to_vec()),
                                Value::Text(member.table_name.clone()),
                                Value::Integer(branch_ord),
                                Value::Bytea(member.schema_hash.as_bytes().to_vec()),
                                Value::Bytea(member.row_digest.0.to_vec()),
                            ],
                        })
                    })
                    .collect::<Result<Vec<_>, StorageError>>()?,
            ),
            sealed_submission.map(Value::Bytea).unwrap_or(Value::Null),
            latest_settlement.map(Value::Bytea).unwrap_or(Value::Null),
        ],
    )
    .map_err(|err| StorageError::IoError(format!("encode local batch record: {err}")))
}

fn decode_local_batch_record_with_branch_ords<H: Storage + ?Sized>(
    storage: &H,
    bytes: &[u8],
) -> Result<LocalBatchRecord, StorageError> {
    let values = decode_row(
        &local_batch_record_storage_descriptor_with_branch_ords(),
        bytes,
    )
    .map_err(|err| StorageError::IoError(format!("decode local batch record: {err}")))?;
    let [
        batch_id,
        mode,
        requested_tier,
        sealed,
        members,
        sealed_submission,
        latest_settlement,
    ] = values.as_slice()
    else {
        return Err(StorageError::IoError(
            "unexpected local batch record shape".to_string(),
        ));
    };

    let batch_id = match batch_id {
        Value::Bytea(bytes) => {
            let bytes: [u8; 16] = bytes.as_slice().try_into().map_err(|_| {
                StorageError::IoError(format!(
                    "decode batch id: expected 16 bytes, got {}",
                    bytes.len()
                ))
            })?;
            BatchId(bytes)
        }
        other => {
            return Err(StorageError::IoError(format!(
                "expected batch id bytes, got {other:?}"
            )));
        }
    };
    let mode = match mode {
        Value::Text(raw) => decode_batch_mode(raw)?,
        other => {
            return Err(StorageError::IoError(format!(
                "expected batch mode text, got {other:?}"
            )));
        }
    };
    let requested_tier = match requested_tier {
        Value::Text(raw) => decode_durability_tier(raw)?,
        other => {
            return Err(StorageError::IoError(format!(
                "expected requested tier text, got {other:?}"
            )));
        }
    };
    let sealed = match sealed {
        Value::Boolean(value) => *value,
        other => {
            return Err(StorageError::IoError(format!(
                "expected sealed boolean, got {other:?}"
            )));
        }
    };
    let members = match members {
        Value::Array(values) => values
                .iter()
                .map(|value| match value {
                    Value::Row { values, .. } => {
                    let [object_id, table_name, branch_ord, schema_hash, row_digest] =
                        values.as_slice()
                    else {
                        return Err(StorageError::IoError(
                            "expected local batch member row to have five values".to_string(),
                        ));
                    };
                    let object_id = match object_id {
                        Value::Bytea(bytes) => {
                            let uuid = uuid::Uuid::from_slice(bytes).map_err(|err| {
                                StorageError::IoError(format!(
                                    "decode local batch member object id: expected uuid bytes: {err}"
                                ))
                            })?;
                            ObjectId::from_uuid(uuid)
                        }
                        other => {
                            return Err(StorageError::IoError(format!(
                                "expected local batch member object id bytes, got {other:?}"
                            )));
                        }
                    };
                    let table_name = match table_name {
                        Value::Text(raw) => raw.clone(),
                        other => {
                            return Err(StorageError::IoError(format!(
                                "expected local batch member table name text, got {other:?}"
                            )));
                        }
                    };
                    let branch_ord = match branch_ord {
                        Value::Integer(raw) => *raw,
                        other => {
                            return Err(StorageError::IoError(format!(
                                "expected local batch member branch ord integer, got {other:?}"
                            )));
                        }
                    };
                    let branch_name = storage
                        .load_branch_name_by_ord(branch_ord)?
                        .ok_or_else(|| {
                            StorageError::IoError(format!(
                                "missing branch name for local batch member branch ord {branch_ord}"
                            ))
                        })?;
                    let schema_hash = match schema_hash {
                        Value::Bytea(bytes) => {
                            let bytes: [u8; 32] = bytes.as_slice().try_into().map_err(|_| {
                                StorageError::IoError(format!(
                                    "expected local batch member schema hash to be 32 bytes, got {}",
                                    bytes.len()
                                ))
                            })?;
                            SchemaHash::from_bytes(bytes)
                        }
                        other => {
                            return Err(StorageError::IoError(format!(
                                "expected local batch member schema hash bytes, got {other:?}"
                            )));
                        }
                    };
                    let row_digest = match row_digest {
                        Value::Bytea(bytes) => Digest32(bytes.as_slice().try_into().map_err(
                            |_| {
                                StorageError::IoError(format!(
                                    "expected local batch member row digest to be 32 bytes, got {}",
                                    bytes.len()
                                ))
                            },
                        )?),
                        other => {
                            return Err(StorageError::IoError(format!(
                                "expected local batch member row digest bytes, got {other:?}"
                            )));
                        }
                    };
                    Ok(crate::batch_fate::LocalBatchMember {
                        object_id,
                        table_name,
                        branch_name,
                        schema_hash,
                        row_digest,
                    })
                }
                other => Err(StorageError::IoError(format!(
                    "expected local batch member row, got {other:?}"
                ))),
            })
            .collect::<Result<Vec<_>, StorageError>>()?,
        other => {
            return Err(StorageError::IoError(format!(
                "expected local batch members array, got {other:?}"
            )));
        }
    };
    let sealed_submission = match sealed_submission {
        Value::Null => None,
        Value::Bytea(bytes) => Some(decode_sealed_batch_submission_with_branch_ords(
            storage, bytes,
        )?),
        other => {
            return Err(StorageError::IoError(format!(
                "expected sealed submission bytes or null, got {other:?}"
            )));
        }
    };
    let latest_settlement = match latest_settlement {
        Value::Null => None,
        Value::Bytea(bytes) => Some(BatchSettlement::decode_storage_row(bytes).map_err(|err| {
            StorageError::IoError(format!("decode local batch settlement: {err}"))
        })?),
        other => {
            return Err(StorageError::IoError(format!(
                "expected latest settlement bytes or null, got {other:?}"
            )));
        }
    };

    Ok(LocalBatchRecord {
        batch_id,
        mode,
        requested_tier,
        sealed,
        members,
        sealed_submission,
        latest_settlement,
    })
}

// ============================================================================
// Storage Trait
// ============================================================================

/// Synchronous storage for metadata, row histories, raw tables, and indices.
///
/// All operations are **synchronous** - they return immediately with results.
/// This eliminates the async response/callback pattern that permeated the
/// old architecture.
///
/// # Single-threaded
///
/// No `Send + Sync` bounds. Each thread has its own Storage instance.
/// Cross-thread communication uses the sync protocol, not shared state.
pub trait Storage {
    // ================================================================
    // Metadata storage (sync - returns immediately with result)
    // ================================================================

    /// Upsert metadata for a logical id.
    fn put_metadata(
        &mut self,
        id: ObjectId,
        metadata: HashMap<String, String>,
    ) -> Result<(), StorageError> {
        let bytes = encode_metadata(&metadata)?;
        self.raw_table_put(METADATA_TABLE, &metadata_raw_key(id), &bytes)?;

        self.put_row_locator(id, row_locator_from_metadata(&metadata).as_ref())?;

        Ok(())
    }

    /// Load metadata for a logical id. Returns None if it doesn't exist.
    fn load_metadata(&self, id: ObjectId) -> Result<Option<HashMap<String, String>>, StorageError> {
        self.raw_table_get(METADATA_TABLE, &metadata_raw_key(id))?
            .map(|bytes| decode_metadata(&bytes))
            .transpose()
    }

    /// Enumerate all persisted metadata rows.
    fn scan_metadata(&self) -> Result<MetadataRows, StorageError> {
        let mut rows = Vec::new();
        for (key, bytes) in self.raw_table_scan_prefix(METADATA_TABLE, "")? {
            rows.push((decode_metadata_raw_key(&key)?, decode_metadata(&bytes)?));
        }
        rows.sort_by_key(|(object_id, _)| *object_id);
        Ok(rows)
    }

    fn scan_row_locators(&self) -> Result<RowLocatorRows, StorageError> {
        let mut rows = Vec::new();
        for (key, bytes) in self.raw_table_scan_prefix(ROW_LOCATOR_TABLE, "")? {
            rows.push((decode_metadata_raw_key(&key)?, decode_row_locator(&bytes)?));
        }
        rows.sort_by_key(|(object_id, _)| *object_id);
        Ok(rows)
    }

    fn load_row_locator(&self, id: ObjectId) -> Result<Option<RowLocator>, StorageError> {
        self.raw_table_get(ROW_LOCATOR_TABLE, &metadata_raw_key(id))?
            .map(|bytes| decode_row_locator(&bytes))
            .transpose()
    }

    fn put_row_locator(
        &mut self,
        id: ObjectId,
        locator: Option<&RowLocator>,
    ) -> Result<(), StorageError> {
        if let Some(locator) = locator {
            let locator_bytes = encode_row_locator(locator)?;
            self.raw_table_put(ROW_LOCATOR_TABLE, &metadata_raw_key(id), &locator_bytes)
        } else {
            self.raw_table_delete(ROW_LOCATOR_TABLE, &metadata_raw_key(id))
        }
    }

    // ================================================================
    // Ordered raw-table storage
    // ================================================================

    fn raw_table_put(
        &mut self,
        _table: &str,
        _key: &str,
        _value: &[u8],
    ) -> Result<(), StorageError> {
        Err(StorageError::IoError(
            "raw table puts are not implemented for this backend yet".to_string(),
        ))
    }

    fn raw_table_delete(&mut self, _table: &str, _key: &str) -> Result<(), StorageError> {
        Err(StorageError::IoError(
            "raw table deletes are not implemented for this backend yet".to_string(),
        ))
    }

    fn raw_table_get(&self, _table: &str, _key: &str) -> Result<Option<Vec<u8>>, StorageError> {
        Err(StorageError::IoError(
            "raw table lookups are not implemented for this backend yet".to_string(),
        ))
    }

    fn raw_table_scan_prefix(
        &self,
        _table: &str,
        _prefix: &str,
    ) -> Result<RawTableRows, StorageError> {
        Err(StorageError::IoError(
            "raw table prefix scans are not implemented for this backend yet".to_string(),
        ))
    }

    fn raw_table_scan_range(
        &self,
        _table: &str,
        _start: Option<&str>,
        _end: Option<&str>,
    ) -> Result<RawTableRows, StorageError> {
        Err(StorageError::IoError(
            "raw table range scans are not implemented for this backend yet".to_string(),
        ))
    }

    fn raw_table_scan_prefix_keys(
        &self,
        table: &str,
        prefix: &str,
    ) -> Result<RawTableKeys, StorageError> {
        self.raw_table_scan_prefix(table, prefix)
            .map(|rows| rows.into_iter().map(|(key, _)| key).collect())
    }

    fn raw_table_scan_range_keys(
        &self,
        table: &str,
        start: Option<&str>,
        end: Option<&str>,
    ) -> Result<RawTableKeys, StorageError> {
        self.raw_table_scan_range(table, start, end)
            .map(|rows| rows.into_iter().map(|(key, _)| key).collect())
    }

    fn load_branch_ord(&self, branch_name: BranchName) -> Result<Option<BranchOrd>, StorageError> {
        Ok(load_branch_ord_registry(self)?
            .entries
            .into_iter()
            .find(|entry| entry.branch_name == branch_name)
            .map(|entry| entry.branch_ord))
    }

    fn load_branch_name_by_ord(
        &self,
        branch_ord: BranchOrd,
    ) -> Result<Option<BranchName>, StorageError> {
        Ok(load_branch_ord_registry(self)?
            .entries
            .into_iter()
            .find(|entry| entry.branch_ord == branch_ord)
            .map(|entry| entry.branch_name))
    }

    fn resolve_or_alloc_branch_ord(
        &mut self,
        branch_name: BranchName,
    ) -> Result<BranchOrd, StorageError> {
        let mut registry = load_branch_ord_registry(self)?;
        if let Some(existing_ord) = registry
            .entries
            .iter()
            .find(|entry| entry.branch_name == branch_name)
            .map(|entry| entry.branch_ord)
        {
            return Ok(existing_ord);
        }

        let next_ord = registry
            .next_ord
            .max(
                registry
                    .entries
                    .iter()
                    .map(|entry| entry.branch_ord)
                    .max()
                    .unwrap_or(0)
                    .saturating_add(1),
            )
            .max(1);
        registry.entries.push(BranchOrdRegistryEntry {
            branch_ord: next_ord,
            branch_name,
        });
        registry.entries.sort_by_key(|entry| entry.branch_ord);
        registry.next_ord = next_ord.saturating_add(1);
        store_branch_ord_registry(self, &registry)?;
        Ok(next_ord)
    }

    fn upsert_catalogue_entry(&mut self, entry: &CatalogueEntry) -> Result<(), StorageError> {
        let bytes = entry
            .encode_storage_row()
            .map_err(|err| StorageError::IoError(format!("encode catalogue entry: {err}")))?;
        self.raw_table_put(
            "catalogue",
            &key_codec::catalogue_entry_key(entry.object_id),
            &bytes,
        )
    }

    fn load_catalogue_entry(
        &self,
        object_id: ObjectId,
    ) -> Result<Option<CatalogueEntry>, StorageError> {
        match self.raw_table_get("catalogue", &key_codec::catalogue_entry_key(object_id))? {
            Some(bytes) => CatalogueEntry::decode_storage_row(object_id, &bytes)
                .map(Some)
                .map_err(|err| StorageError::IoError(format!("decode catalogue entry: {err}"))),
            None => Ok(None),
        }
    }

    fn scan_catalogue_entries(&self) -> Result<Vec<CatalogueEntry>, StorageError> {
        let mut entries = Vec::new();
        for (key, bytes) in
            self.raw_table_scan_prefix("catalogue", key_codec::catalogue_entry_prefix())?
        {
            let Some(hex_id) = key.strip_prefix(key_codec::catalogue_entry_prefix()) else {
                continue;
            };
            let bytes_id = hex::decode(hex_id).map_err(|err| {
                StorageError::IoError(format!("invalid catalogue entry key '{key}': {err}"))
            })?;
            let uuid = uuid::Uuid::from_slice(&bytes_id).map_err(|err| {
                StorageError::IoError(format!("invalid catalogue entry uuid '{key}': {err}"))
            })?;
            let object_id = ObjectId::from_uuid(uuid);
            let entry = CatalogueEntry::decode_storage_row(object_id, &bytes)
                .map_err(|err| StorageError::IoError(format!("decode catalogue entry: {err}")))?;
            entries.push(entry);
        }
        entries.sort_by_key(|entry| entry.object_id);
        Ok(entries)
    }

    fn upsert_row_namespace_header(
        &mut self,
        namespace: &RowNamespaceId,
        header: &RowNamespaceHeader,
    ) -> Result<(), StorageError> {
        let bytes = encode_row_namespace_header(header)?;
        self.raw_table_put(
            ROW_NAMESPACE_HEADER_TABLE,
            &namespace.raw_table_name(),
            &bytes,
        )
    }

    fn load_row_namespace_header(
        &self,
        namespace: &RowNamespaceId,
    ) -> Result<Option<RowNamespaceHeader>, StorageError> {
        self.raw_table_get(ROW_NAMESPACE_HEADER_TABLE, &namespace.raw_table_name())?
            .map(|bytes| decode_row_namespace_header(&bytes))
            .transpose()
    }

    fn scan_row_namespace_headers(
        &self,
    ) -> Result<Vec<(RowNamespaceId, RowNamespaceHeader)>, StorageError> {
        let mut rows = Vec::new();
        for (key, bytes) in self.raw_table_scan_prefix(ROW_NAMESPACE_HEADER_TABLE, "rowns:")? {
            let namespace = RowNamespaceId::parse_raw_table_name(&key)?;
            let header = decode_row_namespace_header(&bytes)?;
            if header.schema_hash != namespace.schema_hash
                || header.table_name != namespace.table_name
            {
                return Err(StorageError::IoError(format!(
                    "row namespace header key/row mismatch for {key}"
                )));
            }
            rows.push((namespace, header));
        }
        rows.sort_by_key(|(namespace, _)| namespace.raw_table_name());
        Ok(rows)
    }

    fn upsert_local_batch_record(&mut self, record: &LocalBatchRecord) -> Result<(), StorageError> {
        let bytes = encode_local_batch_record_with_branch_ords(self, record)?;
        self.raw_table_put(
            LOCAL_BATCH_RECORD_TABLE,
            &local_batch_record_key(record.batch_id),
            &bytes,
        )
    }

    fn load_local_batch_record(
        &self,
        batch_id: BatchId,
    ) -> Result<Option<LocalBatchRecord>, StorageError> {
        match self.raw_table_get(LOCAL_BATCH_RECORD_TABLE, &local_batch_record_key(batch_id))? {
            Some(bytes) => decode_local_batch_record_with_branch_ords(self, &bytes).map(Some),
            None => Ok(None),
        }
    }

    fn delete_local_batch_record(&mut self, batch_id: BatchId) -> Result<(), StorageError> {
        self.raw_table_delete(LOCAL_BATCH_RECORD_TABLE, &local_batch_record_key(batch_id))
    }

    fn scan_local_batch_records(&self) -> Result<Vec<LocalBatchRecord>, StorageError> {
        let mut records = Vec::new();
        for (key, bytes) in self.raw_table_scan_prefix(LOCAL_BATCH_RECORD_TABLE, "batch:")? {
            let batch_id = decode_local_batch_record_key(&key)?;
            let record = decode_local_batch_record_with_branch_ords(self, &bytes)?;
            if record.batch_id != batch_id {
                return Err(StorageError::IoError(format!(
                    "local batch record key/row mismatch for {key}"
                )));
            }
            records.push(record);
        }
        records.sort_by_key(|record| record.batch_id);
        Ok(records)
    }

    fn upsert_sealed_batch_submission(
        &mut self,
        submission: &SealedBatchSubmission,
    ) -> Result<(), StorageError> {
        let bytes = encode_sealed_batch_submission_with_branch_ords(self, submission)?;
        self.raw_table_put(
            SEALED_BATCH_SUBMISSION_TABLE,
            &local_batch_record_key(submission.batch_id),
            &bytes,
        )
    }

    fn load_sealed_batch_submission(
        &self,
        batch_id: BatchId,
    ) -> Result<Option<SealedBatchSubmission>, StorageError> {
        match self.raw_table_get(
            SEALED_BATCH_SUBMISSION_TABLE,
            &local_batch_record_key(batch_id),
        )? {
            Some(bytes) => decode_sealed_batch_submission_with_branch_ords(self, &bytes).map(Some),
            None => Ok(None),
        }
    }

    fn delete_sealed_batch_submission(&mut self, batch_id: BatchId) -> Result<(), StorageError> {
        self.raw_table_delete(
            SEALED_BATCH_SUBMISSION_TABLE,
            &local_batch_record_key(batch_id),
        )
    }

    fn scan_sealed_batch_submissions(&self) -> Result<Vec<SealedBatchSubmission>, StorageError> {
        let mut submissions = Vec::new();
        for (key, bytes) in self.raw_table_scan_prefix(SEALED_BATCH_SUBMISSION_TABLE, "batch:")? {
            let batch_id = decode_local_batch_record_key(&key)?;
            let submission = decode_sealed_batch_submission_with_branch_ords(self, &bytes)?;
            if submission.batch_id != batch_id {
                return Err(StorageError::IoError(format!(
                    "sealed batch submission key/row mismatch for {key}"
                )));
            }
            submissions.push(submission);
        }
        submissions.sort_by_key(|submission| submission.batch_id);
        Ok(submissions)
    }

    fn upsert_authoritative_batch_settlement(
        &mut self,
        settlement: &BatchSettlement,
    ) -> Result<(), StorageError> {
        let bytes = settlement.encode_storage_row().map_err(|err| {
            StorageError::IoError(format!("encode authoritative batch settlement: {err}"))
        })?;
        self.raw_table_put(
            AUTHORITATIVE_BATCH_SETTLEMENT_TABLE,
            &local_batch_record_key(settlement.batch_id()),
            &bytes,
        )
    }

    fn load_authoritative_batch_settlement(
        &self,
        batch_id: BatchId,
    ) -> Result<Option<BatchSettlement>, StorageError> {
        match self.raw_table_get(
            AUTHORITATIVE_BATCH_SETTLEMENT_TABLE,
            &local_batch_record_key(batch_id),
        )? {
            Some(bytes) => BatchSettlement::decode_storage_row(&bytes)
                .map(Some)
                .map_err(|err| {
                    StorageError::IoError(format!("decode authoritative batch settlement: {err}"))
                }),
            None => Ok(None),
        }
    }

    fn scan_authoritative_batch_settlements(&self) -> Result<Vec<BatchSettlement>, StorageError> {
        let mut settlements = Vec::new();
        for (key, bytes) in
            self.raw_table_scan_prefix(AUTHORITATIVE_BATCH_SETTLEMENT_TABLE, "batch:")?
        {
            let batch_id = decode_local_batch_record_key(&key)?;
            let settlement = BatchSettlement::decode_storage_row(&bytes).map_err(|err| {
                StorageError::IoError(format!("decode authoritative batch settlement: {err}"))
            })?;
            if settlement.batch_id() != batch_id {
                return Err(StorageError::IoError(format!(
                    "authoritative batch settlement key/row mismatch for {key}"
                )));
            }
            settlements.push(settlement);
        }
        settlements.sort_by_key(|settlement| settlement.batch_id().0);
        Ok(settlements)
    }

    // ================================================================
    // Row-history storage
    // ================================================================

    fn append_history_region_row_bytes(
        &mut self,
        _table: &str,
        _rows: &[HistoryRowBytes<'_>],
    ) -> Result<(), StorageError> {
        Err(StorageError::IoError(
            "raw row-history appends are not implemented for this backend yet".to_string(),
        ))
    }

    fn load_history_row_batch_bytes(
        &self,
        table: &str,
        branch: &str,
        row_id: ObjectId,
        batch_id: BatchId,
    ) -> Result<Option<Vec<u8>>, StorageError> {
        Ok(
            load_history_row_batch_row_bytes_with_storage(self, table, branch, row_id, batch_id)?
                .map(|row| row.bytes),
        )
    }

    fn scan_history_region_bytes(
        &self,
        table: &str,
        scan: HistoryScan,
    ) -> Result<Vec<Vec<u8>>, StorageError> {
        Ok(scan_history_row_bytes_with_storage(self, table, scan)?
            .into_iter()
            .map(|row| row.bytes)
            .collect())
    }

    fn append_history_region_rows(
        &mut self,
        table: &str,
        rows: &[StoredRowBatch],
    ) -> Result<(), StorageError> {
        let encoded_rows = encode_history_row_bytes_for_storage(self, table, rows)?;
        let mut seen_namespaces = HashSet::new();
        for row in &encoded_rows {
            if seen_namespaces.insert(row.namespace.raw_table_name()) {
                self.upsert_row_namespace_header(
                    &row.namespace,
                    &RowNamespaceHeader::v1(table, row.namespace.schema_hash),
                )?;
            }
        }
        let borrowed_rows = encoded_rows
            .iter()
            .map(|row| HistoryRowBytes {
                namespace_raw_table: row.namespace_raw_table.as_str(),
                branch: row.branch.as_str(),
                row_id: row.row_id,
                batch_id: row.batch_id,
                bytes: &row.bytes,
            })
            .collect::<Vec<_>>();
        self.append_history_region_row_bytes(table, &borrowed_rows)
    }

    fn upsert_visible_region_row_bytes(
        &mut self,
        _table: &str,
        _rows: &[VisibleRowBytes<'_>],
    ) -> Result<(), StorageError> {
        Err(StorageError::IoError(
            "raw visible-row upserts are not implemented for this backend yet".to_string(),
        ))
    }

    fn load_visible_region_row_bytes(
        &self,
        table: &str,
        branch: &str,
        row_id: ObjectId,
    ) -> Result<Option<Vec<u8>>, StorageError> {
        Ok(
            load_visible_region_row_bytes_with_storage(self, table, branch, row_id)?
                .map(|row| row.bytes),
        )
    }

    fn scan_visible_region_bytes(
        &self,
        table: &str,
        branch: &str,
    ) -> Result<Vec<Vec<u8>>, StorageError> {
        Ok(scan_visible_row_bytes_with_storage(self, table, branch)?
            .into_iter()
            .map(|row| row.bytes)
            .collect())
    }

    fn upsert_visible_region_rows(
        &mut self,
        table: &str,
        entries: &[VisibleRowEntry],
    ) -> Result<(), StorageError> {
        let encoded_rows = encode_visible_row_bytes_for_storage(self, table, entries)?;
        let mut seen_namespaces = HashSet::new();
        for row in &encoded_rows {
            if seen_namespaces.insert(row.namespace.raw_table_name()) {
                self.upsert_row_namespace_header(
                    &row.namespace,
                    &RowNamespaceHeader::v1(table, row.namespace.schema_hash),
                )?;
            }
        }
        let borrowed_rows = encoded_rows
            .iter()
            .map(|row| VisibleRowBytes {
                namespace_raw_table: row.namespace_raw_table.as_str(),
                branch: row.branch.as_str(),
                row_id: row.row_id,
                bytes: &row.bytes,
            })
            .collect::<Vec<_>>();
        self.upsert_visible_region_row_bytes(table, &borrowed_rows)
    }

    fn delete_visible_region_row(
        &mut self,
        table: &str,
        branch: &str,
        row_id: ObjectId,
    ) -> Result<(), StorageError> {
        let key = key_codec::visible_namespace_row_key(branch, row_id);
        for namespace in
            row_namespace_ids_for_exact_row(self, RowNamespaceKind::Visible, table, row_id)?
        {
            self.raw_table_delete(&namespace.raw_table_name(), &key)?;
        }
        Ok(())
    }

    fn patch_exact_row_batch(
        &mut self,
        table: &str,
        branch: &str,
        row_id: ObjectId,
        batch_id: crate::row_histories::BatchId,
        state: Option<RowState>,
        confirmed_tier: Option<DurabilityTier>,
    ) -> Result<bool, StorageError> {
        patch_exact_row_batch_with_storage(
            self,
            table,
            branch,
            row_id,
            batch_id,
            state,
            confirmed_tier,
        )
    }

    fn patch_row_region_rows_by_batch(
        &mut self,
        _table: &str,
        _batch_id: crate::row_histories::BatchId,
        _state: Option<RowState>,
        _confirmed_tier: Option<DurabilityTier>,
    ) -> Result<(), StorageError> {
        Err(StorageError::IoError(
            "row-history patching is not implemented for this backend yet".to_string(),
        ))
    }

    fn apply_row_mutation(
        &mut self,
        table: &str,
        history_rows: &[StoredRowBatch],
        visible_entries: &[VisibleRowEntry],
        index_mutations: &[IndexMutation<'_>],
    ) -> Result<(), StorageError> {
        if !history_rows.is_empty() {
            self.append_history_region_rows(table, history_rows)?;
        }
        if !visible_entries.is_empty() {
            self.upsert_visible_region_rows(table, visible_entries)?;
        }
        if !index_mutations.is_empty() {
            self.apply_index_mutations(index_mutations)?;
        }
        Ok(())
    }

    fn scan_visible_region(
        &self,
        table: &str,
        branch: &str,
    ) -> Result<Vec<StoredRowBatch>, StorageError> {
        let mut rows = scan_visible_row_bytes_with_storage(self, table, branch)?
            .into_iter()
            .map(|row| {
                decode_visible_row_entry_bytes_in_namespace(
                    self,
                    &row.namespace,
                    row.row_id,
                    row.branch.as_str(),
                    &row.bytes,
                )
                .map(|entry| entry.current_row)
            })
            .collect::<Result<Vec<_>, _>>()?;
        rows.sort_by_key(|row| (row.branch.clone(), row.row_id));
        Ok(rows)
    }

    fn load_visible_region_row(
        &self,
        table: &str,
        branch: &str,
        row_id: ObjectId,
    ) -> Result<Option<StoredRowBatch>, StorageError> {
        load_visible_region_row_bytes_with_storage(self, table, branch, row_id)?
            .map(|row| {
                decode_visible_row_entry_bytes_in_namespace(
                    self,
                    &row.namespace,
                    row.row_id,
                    row.branch.as_str(),
                    &row.bytes,
                )
                .map(|entry| entry.current_row)
            })
            .transpose()
    }

    fn load_visible_query_row(
        &self,
        table: &str,
        branch: &str,
        row_id: ObjectId,
    ) -> Result<Option<QueryRowBatch>, StorageError> {
        Ok(self
            .load_visible_region_row(table, branch, row_id)?
            .as_ref()
            .map(QueryRowBatch::from))
    }

    fn load_visible_region_row_for_tier(
        &self,
        table: &str,
        branch: &str,
        row_id: ObjectId,
        required_tier: DurabilityTier,
    ) -> Result<Option<StoredRowBatch>, StorageError> {
        let Some(entry) = self.load_visible_region_entry(table, branch, row_id)? else {
            return Ok(None);
        };

        let Some(batch_id) = entry.batch_id_for_tier(required_tier) else {
            return Ok(None);
        };
        if batch_id == entry.current_batch_id() {
            return Ok(Some(entry.current_row));
        }

        self.load_history_row_batch(table, branch, row_id, batch_id)
    }

    fn load_visible_query_row_for_tier(
        &self,
        table: &str,
        branch: &str,
        row_id: ObjectId,
        required_tier: DurabilityTier,
    ) -> Result<Option<QueryRowBatch>, StorageError> {
        Ok(self
            .load_visible_region_row_for_tier(table, branch, row_id, required_tier)?
            .as_ref()
            .map(QueryRowBatch::from))
    }

    fn load_visible_region_entry(
        &self,
        table: &str,
        branch: &str,
        row_id: ObjectId,
    ) -> Result<Option<VisibleRowEntry>, StorageError> {
        load_visible_region_row_bytes_with_storage(self, table, branch, row_id)?
            .map(|row| {
                decode_visible_row_entry_bytes_in_namespace(
                    self,
                    &row.namespace,
                    row.row_id,
                    row.branch.as_str(),
                    &row.bytes,
                )
            })
            .transpose()
    }

    fn load_visible_region_frontier(
        &self,
        table: &str,
        branch: &str,
        row_id: ObjectId,
    ) -> Result<Option<Vec<BatchId>>, StorageError> {
        Ok(self
            .load_visible_region_entry(table, branch, row_id)?
            .map(|entry| entry.branch_frontier))
    }

    fn capture_family_visible_frontier(
        &self,
        target_branch_name: BranchName,
    ) -> Result<Vec<CapturedFrontierMember>, StorageError> {
        let branch_names = load_branch_ord_registry(self)?
            .entries
            .into_iter()
            .map(|entry| entry.branch_name)
            .collect::<Vec<_>>();
        let family_branches: Vec<_> = branch_names
            .into_iter()
            .filter(|branch_name| {
                branch_matches_transaction_family(*branch_name, target_branch_name)
            })
            .collect();
        if family_branches.is_empty() {
            return Ok(Vec::new());
        }

        let tables = self
            .scan_row_namespace_headers()?
            .into_iter()
            .map(|(namespace, _header)| namespace)
            .filter(|namespace| namespace.kind == RowNamespaceKind::Visible)
            .map(|namespace| namespace.table_name)
            .collect::<BTreeSet<_>>();

        let mut frontier = Vec::new();
        for table in tables {
            for branch_name in &family_branches {
                for current_row in self.scan_visible_region(&table, branch_name.as_str())? {
                    frontier.push(CapturedFrontierMember {
                        object_id: current_row.row_id,
                        branch_name: *branch_name,
                        batch_id: current_row.batch_id(),
                    });
                }
            }
        }

        frontier.sort_by(|left, right| {
            left.object_id
                .uuid()
                .as_bytes()
                .cmp(right.object_id.uuid().as_bytes())
                .then_with(|| left.branch_name.as_str().cmp(right.branch_name.as_str()))
                .then_with(|| left.batch_id.0.cmp(&right.batch_id.0))
        });
        frontier.dedup();
        Ok(frontier)
    }

    fn load_history_row_batch(
        &self,
        table: &str,
        branch: &str,
        row_id: ObjectId,
        batch_id: BatchId,
    ) -> Result<Option<StoredRowBatch>, StorageError> {
        load_history_row_batch_row_bytes_with_storage(self, table, branch, row_id, batch_id)?
            .map(|row| {
                decode_history_row_bytes_in_namespace(
                    self,
                    &row.namespace,
                    row.row_id,
                    row.branch.as_str(),
                    row.batch_id,
                    &row.bytes,
                )
            })
            .transpose()
    }

    fn load_history_row_batch_for_schema_hash(
        &self,
        table: &str,
        schema_hash: SchemaHash,
        branch: &str,
        row_id: ObjectId,
        batch_id: BatchId,
    ) -> Result<Option<StoredRowBatch>, StorageError> {
        let namespace = history_row_namespace_id(table, schema_hash);
        load_history_row_batch_bytes_in_namespace_with_storage(
            self, &namespace, branch, row_id, batch_id,
        )?
        .map(|bytes| {
            decode_history_row_bytes_in_namespace(
                self, &namespace, row_id, branch, batch_id, &bytes,
            )
        })
        .transpose()
    }

    fn load_history_query_row_batch(
        &self,
        table: &str,
        branch: &str,
        row_id: ObjectId,
        batch_id: BatchId,
    ) -> Result<Option<QueryRowBatch>, StorageError> {
        load_history_row_batch_row_bytes_with_storage(self, table, branch, row_id, batch_id)?
            .map(|row| {
                decode_history_row_bytes_in_namespace(
                    self,
                    &row.namespace,
                    row.row_id,
                    row.branch.as_str(),
                    row.batch_id,
                    &row.bytes,
                )
                .map(|decoded| QueryRowBatch::from(&decoded))
            })
            .transpose()
    }

    fn load_history_row_batch_any_branch(
        &self,
        table: &str,
        row_id: ObjectId,
        batch_id: BatchId,
    ) -> Result<Option<StoredRowBatch>, StorageError> {
        let mut matches = self
            .scan_history_row_batches(table, row_id)?
            .into_iter()
            .filter(|row| row.batch_id() == batch_id);
        let Some(first_match) = matches.next() else {
            return Ok(None);
        };
        if let Some(second_match) = matches.next() {
            return Err(StorageError::IoError(format!(
                "ambiguous row history version {batch_id:?} for row {row_id}: found branches {} and {}",
                first_match.branch, second_match.branch
            )));
        }
        Ok(Some(first_match))
    }

    fn load_history_query_row_batch_any_branch(
        &self,
        table: &str,
        row_id: ObjectId,
        batch_id: BatchId,
    ) -> Result<Option<QueryRowBatch>, StorageError> {
        Ok(self
            .load_history_row_batch_any_branch(table, row_id, batch_id)?
            .as_ref()
            .map(QueryRowBatch::from))
    }

    fn row_batch_exists(
        &self,
        table: &str,
        branch: &str,
        row_id: ObjectId,
        batch_id: BatchId,
    ) -> Result<bool, StorageError> {
        Ok(self
            .load_history_row_batch(table, branch, row_id, batch_id)?
            .is_some())
    }

    #[allow(clippy::too_many_arguments)]
    fn patch_exact_row_batch_for_schema_hash(
        &mut self,
        table: &str,
        schema_hash: SchemaHash,
        branch: &str,
        row_id: ObjectId,
        batch_id: BatchId,
        state: Option<RowState>,
        confirmed_tier: Option<DurabilityTier>,
    ) -> Result<bool, StorageError> {
        let Some(mut current_row) = self.load_history_row_batch_for_schema_hash(
            table,
            schema_hash,
            branch,
            row_id,
            batch_id,
        )?
        else {
            return Ok(false);
        };

        if let Some(state) = state {
            current_row.state = state;
        }
        if let Some(confirmed_tier) = confirmed_tier {
            current_row.confirmed_tier = Some(match current_row.confirmed_tier {
                Some(existing) => existing.max(confirmed_tier),
                None => confirmed_tier,
            });
        }

        let history_rows = self.scan_history_row_batches(table, row_id)?;
        let mut patched_history = history_rows.clone();
        if let Some(existing) = patched_history
            .iter_mut()
            .find(|row| row.branch == branch && row.row_id == row_id && row.batch_id() == batch_id)
        {
            *existing = current_row.clone();
        }
        self.append_history_region_rows(table, &patched_history)?;

        let Some(existing_entry) = self.load_visible_region_entry(table, branch, row_id)? else {
            return Ok(true);
        };
        let current_batch_id = existing_entry.current_row.batch_id();
        let Some(current_row) = patched_history
            .iter()
            .find(|candidate| {
                candidate.branch == branch && candidate.batch_id() == current_batch_id
            })
            .cloned()
        else {
            self.delete_visible_region_row(table, branch, row_id)?;
            return Ok(true);
        };

        if current_row.state.is_visible() {
            self.upsert_visible_region_rows(
                table,
                &[VisibleRowEntry::rebuild(current_row, &patched_history)],
            )?;
        } else {
            self.delete_visible_region_row(table, branch, row_id)?;
        }

        Ok(true)
    }

    fn scan_row_branch_tip_ids(
        &self,
        table: &str,
        branch: &str,
        row_id: ObjectId,
    ) -> Result<Vec<BatchId>, StorageError> {
        if let Some(frontier) = self.load_visible_region_frontier(table, branch, row_id)? {
            return Ok(frontier);
        }

        let branch_rows = self
            .scan_history_row_batches(table, row_id)?
            .into_iter()
            .filter(|row| row.branch == branch)
            .collect::<Vec<_>>();

        let mut non_tips = SmolSet::<[BatchId; 2]>::new();
        for row in &branch_rows {
            for parent in &row.parents {
                non_tips.insert(*parent);
            }
        }

        let mut tips: Vec<_> = branch_rows
            .into_iter()
            .map(|row| row.batch_id())
            .filter(|batch_id| !non_tips.contains(batch_id))
            .collect();
        tips.sort();
        tips.dedup();
        Ok(tips)
    }

    fn scan_visible_region_row_batches(
        &self,
        _table: &str,
        _row_id: ObjectId,
    ) -> Result<Vec<StoredRowBatch>, StorageError> {
        Err(StorageError::IoError(
            "visible-row history scans are not implemented for this backend yet".to_string(),
        ))
    }

    fn scan_history_row_batches(
        &self,
        table: &str,
        row_id: ObjectId,
    ) -> Result<Vec<StoredRowBatch>, StorageError> {
        let mut rows: Vec<StoredRowBatch> =
            scan_history_row_bytes_with_storage(self, table, HistoryScan::Row { row_id })?
                .into_iter()
                .map(|row| {
                    decode_history_row_bytes_in_namespace(
                        self,
                        &row.namespace,
                        row.row_id,
                        row.branch.as_str(),
                        row.batch_id,
                        &row.bytes,
                    )
                })
                .collect::<Result<_, _>>()?;
        rows.sort_by_key(|row| (row.branch.clone(), row.updated_at, row.batch_id()));
        Ok(rows)
    }

    fn scan_history_region(
        &self,
        table: &str,
        branch: &str,
        scan: HistoryScan,
    ) -> Result<Vec<StoredRowBatch>, StorageError> {
        let scanned: Vec<StoredRowBatch> = scan_history_row_bytes_with_storage(self, table, scan)?
            .into_iter()
            .map(|row| {
                decode_history_row_bytes_in_namespace(
                    self,
                    &row.namespace,
                    row.row_id,
                    row.branch.as_str(),
                    row.batch_id,
                    &row.bytes,
                )
            })
            .collect::<Result<_, _>>()?;

        let mut rows: Vec<StoredRowBatch> = match scan {
            HistoryScan::Branch | HistoryScan::Row { .. } => scanned
                .into_iter()
                .filter(|row| row.branch == branch)
                .collect(),
            HistoryScan::AsOf { ts } => {
                let mut latest_per_row: BTreeMap<ObjectId, StoredRowBatch> = BTreeMap::new();
                for row in scanned {
                    if row.branch != branch || row.updated_at > ts || !row.state.is_visible() {
                        continue;
                    }
                    match latest_per_row.get(&row.row_id) {
                        Some(existing)
                            if (existing.updated_at, existing.batch_id())
                                >= (row.updated_at, row.batch_id()) => {}
                        _ => {
                            latest_per_row.insert(row.row_id, row);
                        }
                    }
                }
                latest_per_row.into_values().collect()
            }
        };
        rows.sort_by_key(|row| (row.branch.clone(), row.updated_at, row.batch_id()));
        Ok(rows)
    }

    // ================================================================
    // Index operations (built on ordered raw tables)
    // ================================================================

    fn index_insert(
        &mut self,
        table: &str,
        column: &str,
        branch: &str,
        value: &Value,
        row_id: ObjectId,
    ) -> Result<(), StorageError> {
        let raw_table = key_codec::index_raw_table(table, column, branch);
        let key = key_codec::index_entry_key(table, column, branch, value, row_id)?;
        self.raw_table_put(&raw_table, &key, &[0x01])
    }

    fn index_remove(
        &mut self,
        table: &str,
        column: &str,
        branch: &str,
        value: &Value,
        row_id: ObjectId,
    ) -> Result<(), StorageError> {
        let key = match key_codec::index_entry_key(table, column, branch, value, row_id) {
            Ok(key) => key,
            Err(StorageError::IndexKeyTooLarge { .. }) => return Ok(()),
            Err(error) => return Err(error),
        };
        let raw_table = key_codec::index_raw_table(table, column, branch);
        self.raw_table_delete(&raw_table, &key)
    }

    fn apply_index_mutations(
        &mut self,
        mutations: &[IndexMutation<'_>],
    ) -> Result<(), StorageError> {
        for mutation in mutations {
            match mutation {
                IndexMutation::Insert {
                    table,
                    column,
                    branch,
                    value,
                    row_id,
                } => self.index_insert(table, column, branch, value, *row_id)?,
                IndexMutation::Remove {
                    table,
                    column,
                    branch,
                    value,
                    row_id,
                } => self.index_remove(table, column, branch, value, *row_id)?,
            }
        }
        Ok(())
    }

    fn index_lookup(
        &self,
        table: &str,
        column: &str,
        branch: &str,
        value: &Value,
    ) -> Vec<ObjectId> {
        let raw_table = key_codec::index_raw_table(table, column, branch);
        if is_double_zero(value) {
            let mut result = HashSet::new();
            for zero in &[Value::Double(0.0), Value::Double(-0.0)] {
                let Ok(prefix) = key_codec::index_value_prefix(table, column, branch, zero) else {
                    continue;
                };
                if let Ok(keys) = self.raw_table_scan_prefix_keys(&raw_table, &prefix) {
                    for key in keys {
                        if let Some(id) = key_codec::parse_uuid_from_index_key(&key) {
                            result.insert(id);
                        }
                    }
                }
            }
            return result.into_iter().collect();
        }

        let Ok(prefix) = key_codec::index_value_prefix(table, column, branch, value) else {
            return Vec::new();
        };
        self.raw_table_scan_prefix_keys(&raw_table, &prefix)
            .map(|keys| {
                keys.into_iter()
                    .filter_map(|key| key_codec::parse_uuid_from_index_key(&key))
                    .collect()
            })
            .unwrap_or_default()
    }

    fn index_range(
        &self,
        table: &str,
        column: &str,
        branch: &str,
        start: Bound<&Value>,
        end: Bound<&Value>,
    ) -> Vec<ObjectId> {
        let raw_table = key_codec::index_raw_table(table, column, branch);
        let Some((start_key, end_key)) =
            key_codec::index_range_scan_bounds(table, column, branch, start, end)
        else {
            return Vec::new();
        };

        self.raw_table_scan_range_keys(&raw_table, start_key.as_deref(), end_key.as_deref())
            .map(|keys| {
                keys.into_iter()
                    .filter_map(|key| key_codec::parse_uuid_from_index_key(&key))
                    .collect()
            })
            .unwrap_or_default()
    }

    fn index_scan_all(&self, table: &str, column: &str, branch: &str) -> Vec<ObjectId> {
        let raw_table = key_codec::index_raw_table(table, column, branch);
        self.raw_table_scan_prefix_keys(&raw_table, "")
            .map(|keys| {
                keys.into_iter()
                    .filter_map(|key| key_codec::parse_uuid_from_index_key(&key))
                    .collect()
            })
            .unwrap_or_default()
    }

    /// Flush buffered data to persistent storage. No-op for in-memory storage.
    fn flush(&self) {}

    /// Flush only the WAL buffer (not the snapshot). No-op for storage without WAL.
    fn flush_wal(&self) {}

    /// Close and release storage resources (e.g. file locks). No-op by default.
    fn close(&self) -> Result<(), StorageError> {
        Ok(())
    }
}

// Box<Storage> is used to allow for dynamic dispatch of the Storage trait.
impl<T: Storage + ?Sized> Storage for Box<T> {
    fn put_metadata(
        &mut self,
        id: ObjectId,
        metadata: HashMap<String, String>,
    ) -> Result<(), StorageError> {
        (**self).put_metadata(id, metadata)
    }

    fn load_metadata(&self, id: ObjectId) -> Result<Option<HashMap<String, String>>, StorageError> {
        (**self).load_metadata(id)
    }

    fn scan_metadata(&self) -> Result<MetadataRows, StorageError> {
        (**self).scan_metadata()
    }

    fn scan_row_locators(&self) -> Result<RowLocatorRows, StorageError> {
        (**self).scan_row_locators()
    }

    fn load_row_locator(&self, id: ObjectId) -> Result<Option<RowLocator>, StorageError> {
        (**self).load_row_locator(id)
    }

    fn put_row_locator(
        &mut self,
        id: ObjectId,
        locator: Option<&RowLocator>,
    ) -> Result<(), StorageError> {
        (**self).put_row_locator(id, locator)
    }

    fn raw_table_put(&mut self, table: &str, key: &str, value: &[u8]) -> Result<(), StorageError> {
        (**self).raw_table_put(table, key, value)
    }

    fn raw_table_delete(&mut self, table: &str, key: &str) -> Result<(), StorageError> {
        (**self).raw_table_delete(table, key)
    }

    fn raw_table_get(&self, table: &str, key: &str) -> Result<Option<Vec<u8>>, StorageError> {
        (**self).raw_table_get(table, key)
    }

    fn raw_table_scan_prefix(
        &self,
        table: &str,
        prefix: &str,
    ) -> Result<RawTableRows, StorageError> {
        (**self).raw_table_scan_prefix(table, prefix)
    }

    fn raw_table_scan_prefix_keys(
        &self,
        table: &str,
        prefix: &str,
    ) -> Result<RawTableKeys, StorageError> {
        (**self).raw_table_scan_prefix_keys(table, prefix)
    }

    fn raw_table_scan_range(
        &self,
        table: &str,
        start: Option<&str>,
        end: Option<&str>,
    ) -> Result<RawTableRows, StorageError> {
        (**self).raw_table_scan_range(table, start, end)
    }

    fn raw_table_scan_range_keys(
        &self,
        table: &str,
        start: Option<&str>,
        end: Option<&str>,
    ) -> Result<RawTableKeys, StorageError> {
        (**self).raw_table_scan_range_keys(table, start, end)
    }

    fn upsert_catalogue_entry(&mut self, entry: &CatalogueEntry) -> Result<(), StorageError> {
        (**self).upsert_catalogue_entry(entry)
    }

    fn load_catalogue_entry(
        &self,
        object_id: ObjectId,
    ) -> Result<Option<CatalogueEntry>, StorageError> {
        (**self).load_catalogue_entry(object_id)
    }

    fn scan_catalogue_entries(&self) -> Result<Vec<CatalogueEntry>, StorageError> {
        (**self).scan_catalogue_entries()
    }

    fn upsert_local_batch_record(&mut self, record: &LocalBatchRecord) -> Result<(), StorageError> {
        (**self).upsert_local_batch_record(record)
    }

    fn load_local_batch_record(
        &self,
        batch_id: BatchId,
    ) -> Result<Option<LocalBatchRecord>, StorageError> {
        (**self).load_local_batch_record(batch_id)
    }

    fn delete_local_batch_record(&mut self, batch_id: BatchId) -> Result<(), StorageError> {
        (**self).delete_local_batch_record(batch_id)
    }

    fn scan_local_batch_records(&self) -> Result<Vec<LocalBatchRecord>, StorageError> {
        (**self).scan_local_batch_records()
    }

    fn upsert_sealed_batch_submission(
        &mut self,
        submission: &SealedBatchSubmission,
    ) -> Result<(), StorageError> {
        (**self).upsert_sealed_batch_submission(submission)
    }

    fn load_sealed_batch_submission(
        &self,
        batch_id: BatchId,
    ) -> Result<Option<SealedBatchSubmission>, StorageError> {
        (**self).load_sealed_batch_submission(batch_id)
    }

    fn delete_sealed_batch_submission(&mut self, batch_id: BatchId) -> Result<(), StorageError> {
        (**self).delete_sealed_batch_submission(batch_id)
    }

    fn scan_sealed_batch_submissions(&self) -> Result<Vec<SealedBatchSubmission>, StorageError> {
        (**self).scan_sealed_batch_submissions()
    }

    fn upsert_authoritative_batch_settlement(
        &mut self,
        settlement: &BatchSettlement,
    ) -> Result<(), StorageError> {
        (**self).upsert_authoritative_batch_settlement(settlement)
    }

    fn load_authoritative_batch_settlement(
        &self,
        batch_id: BatchId,
    ) -> Result<Option<BatchSettlement>, StorageError> {
        (**self).load_authoritative_batch_settlement(batch_id)
    }

    fn scan_authoritative_batch_settlements(&self) -> Result<Vec<BatchSettlement>, StorageError> {
        (**self).scan_authoritative_batch_settlements()
    }

    fn append_history_region_row_bytes(
        &mut self,
        table: &str,
        rows: &[HistoryRowBytes<'_>],
    ) -> Result<(), StorageError> {
        (**self).append_history_region_row_bytes(table, rows)
    }

    fn load_history_row_batch_bytes(
        &self,
        table: &str,
        branch: &str,
        row_id: ObjectId,
        batch_id: BatchId,
    ) -> Result<Option<Vec<u8>>, StorageError> {
        (**self).load_history_row_batch_bytes(table, branch, row_id, batch_id)
    }

    fn scan_history_region_bytes(
        &self,
        table: &str,
        scan: HistoryScan,
    ) -> Result<Vec<Vec<u8>>, StorageError> {
        (**self).scan_history_region_bytes(table, scan)
    }

    fn append_history_region_rows(
        &mut self,
        table: &str,
        rows: &[StoredRowBatch],
    ) -> Result<(), StorageError> {
        (**self).append_history_region_rows(table, rows)
    }

    fn upsert_visible_region_rows(
        &mut self,
        table: &str,
        entries: &[VisibleRowEntry],
    ) -> Result<(), StorageError> {
        (**self).upsert_visible_region_rows(table, entries)
    }

    fn delete_visible_region_row(
        &mut self,
        table: &str,
        branch: &str,
        row_id: ObjectId,
    ) -> Result<(), StorageError> {
        (**self).delete_visible_region_row(table, branch, row_id)
    }

    fn upsert_visible_region_row_bytes(
        &mut self,
        table: &str,
        rows: &[VisibleRowBytes<'_>],
    ) -> Result<(), StorageError> {
        (**self).upsert_visible_region_row_bytes(table, rows)
    }

    fn load_visible_region_row_bytes(
        &self,
        table: &str,
        branch: &str,
        row_id: ObjectId,
    ) -> Result<Option<Vec<u8>>, StorageError> {
        (**self).load_visible_region_row_bytes(table, branch, row_id)
    }

    fn scan_visible_region_bytes(
        &self,
        table: &str,
        branch: &str,
    ) -> Result<Vec<Vec<u8>>, StorageError> {
        (**self).scan_visible_region_bytes(table, branch)
    }

    fn patch_row_region_rows_by_batch(
        &mut self,
        table: &str,
        batch_id: crate::row_histories::BatchId,
        state: Option<RowState>,
        confirmed_tier: Option<DurabilityTier>,
    ) -> Result<(), StorageError> {
        (**self).patch_row_region_rows_by_batch(table, batch_id, state, confirmed_tier)
    }

    fn patch_exact_row_batch(
        &mut self,
        table: &str,
        branch: &str,
        row_id: ObjectId,
        batch_id: crate::row_histories::BatchId,
        state: Option<RowState>,
        confirmed_tier: Option<DurabilityTier>,
    ) -> Result<bool, StorageError> {
        (**self).patch_exact_row_batch(table, branch, row_id, batch_id, state, confirmed_tier)
    }

    fn apply_row_mutation(
        &mut self,
        table: &str,
        history_rows: &[StoredRowBatch],
        visible_entries: &[VisibleRowEntry],
        index_mutations: &[IndexMutation<'_>],
    ) -> Result<(), StorageError> {
        (**self).apply_row_mutation(table, history_rows, visible_entries, index_mutations)
    }

    fn scan_visible_region(
        &self,
        table: &str,
        branch: &str,
    ) -> Result<Vec<StoredRowBatch>, StorageError> {
        (**self).scan_visible_region(table, branch)
    }

    fn load_visible_region_row(
        &self,
        table: &str,
        branch: &str,
        row_id: ObjectId,
    ) -> Result<Option<StoredRowBatch>, StorageError> {
        (**self).load_visible_region_row(table, branch, row_id)
    }

    fn load_visible_query_row(
        &self,
        table: &str,
        branch: &str,
        row_id: ObjectId,
    ) -> Result<Option<QueryRowBatch>, StorageError> {
        (**self).load_visible_query_row(table, branch, row_id)
    }

    fn load_visible_region_row_for_tier(
        &self,
        table: &str,
        branch: &str,
        row_id: ObjectId,
        required_tier: DurabilityTier,
    ) -> Result<Option<StoredRowBatch>, StorageError> {
        (**self).load_visible_region_row_for_tier(table, branch, row_id, required_tier)
    }

    fn load_visible_query_row_for_tier(
        &self,
        table: &str,
        branch: &str,
        row_id: ObjectId,
        required_tier: DurabilityTier,
    ) -> Result<Option<QueryRowBatch>, StorageError> {
        (**self).load_visible_query_row_for_tier(table, branch, row_id, required_tier)
    }

    fn load_visible_region_frontier(
        &self,
        table: &str,
        branch: &str,
        row_id: ObjectId,
    ) -> Result<Option<Vec<BatchId>>, StorageError> {
        (**self).load_visible_region_frontier(table, branch, row_id)
    }

    fn capture_family_visible_frontier(
        &self,
        target_branch_name: BranchName,
    ) -> Result<Vec<CapturedFrontierMember>, StorageError> {
        (**self).capture_family_visible_frontier(target_branch_name)
    }

    fn scan_visible_region_row_batches(
        &self,
        table: &str,
        row_id: ObjectId,
    ) -> Result<Vec<StoredRowBatch>, StorageError> {
        (**self).scan_visible_region_row_batches(table, row_id)
    }

    fn scan_history_row_batches(
        &self,
        table: &str,
        row_id: ObjectId,
    ) -> Result<Vec<StoredRowBatch>, StorageError> {
        (**self).scan_history_row_batches(table, row_id)
    }

    fn load_history_query_row_batch(
        &self,
        table: &str,
        branch: &str,
        row_id: ObjectId,
        batch_id: BatchId,
    ) -> Result<Option<QueryRowBatch>, StorageError> {
        (**self).load_history_query_row_batch(table, branch, row_id, batch_id)
    }

    fn load_history_row_batch(
        &self,
        table: &str,
        branch: &str,
        row_id: ObjectId,
        batch_id: BatchId,
    ) -> Result<Option<StoredRowBatch>, StorageError> {
        (**self).load_history_row_batch(table, branch, row_id, batch_id)
    }

    fn load_history_row_batch_for_schema_hash(
        &self,
        table: &str,
        schema_hash: SchemaHash,
        branch: &str,
        row_id: ObjectId,
        batch_id: BatchId,
    ) -> Result<Option<StoredRowBatch>, StorageError> {
        (**self).load_history_row_batch_for_schema_hash(
            table,
            schema_hash,
            branch,
            row_id,
            batch_id,
        )
    }

    fn load_history_row_batch_any_branch(
        &self,
        table: &str,
        row_id: ObjectId,
        batch_id: BatchId,
    ) -> Result<Option<StoredRowBatch>, StorageError> {
        (**self).load_history_row_batch_any_branch(table, row_id, batch_id)
    }

    fn load_history_query_row_batch_any_branch(
        &self,
        table: &str,
        row_id: ObjectId,
        batch_id: BatchId,
    ) -> Result<Option<QueryRowBatch>, StorageError> {
        (**self).load_history_query_row_batch_any_branch(table, row_id, batch_id)
    }

    fn row_batch_exists(
        &self,
        table: &str,
        branch: &str,
        row_id: ObjectId,
        batch_id: BatchId,
    ) -> Result<bool, StorageError> {
        (**self).row_batch_exists(table, branch, row_id, batch_id)
    }

    fn patch_exact_row_batch_for_schema_hash(
        &mut self,
        table: &str,
        schema_hash: SchemaHash,
        branch: &str,
        row_id: ObjectId,
        batch_id: BatchId,
        state: Option<RowState>,
        confirmed_tier: Option<DurabilityTier>,
    ) -> Result<bool, StorageError> {
        (**self).patch_exact_row_batch_for_schema_hash(
            table,
            schema_hash,
            branch,
            row_id,
            batch_id,
            state,
            confirmed_tier,
        )
    }

    fn scan_row_branch_tip_ids(
        &self,
        table: &str,
        branch: &str,
        row_id: ObjectId,
    ) -> Result<Vec<BatchId>, StorageError> {
        (**self).scan_row_branch_tip_ids(table, branch, row_id)
    }

    fn scan_history_region(
        &self,
        table: &str,
        branch: &str,
        scan: HistoryScan,
    ) -> Result<Vec<StoredRowBatch>, StorageError> {
        (**self).scan_history_region(table, branch, scan)
    }

    fn index_insert(
        &mut self,
        table: &str,
        column: &str,
        branch: &str,
        value: &Value,
        row_id: ObjectId,
    ) -> Result<(), StorageError> {
        (**self).index_insert(table, column, branch, value, row_id)
    }

    fn index_remove(
        &mut self,
        table: &str,
        column: &str,
        branch: &str,
        value: &Value,
        row_id: ObjectId,
    ) -> Result<(), StorageError> {
        (**self).index_remove(table, column, branch, value, row_id)
    }

    fn apply_index_mutations(
        &mut self,
        mutations: &[IndexMutation<'_>],
    ) -> Result<(), StorageError> {
        (**self).apply_index_mutations(mutations)
    }

    fn index_lookup(
        &self,
        table: &str,
        column: &str,
        branch: &str,
        value: &Value,
    ) -> Vec<ObjectId> {
        (**self).index_lookup(table, column, branch, value)
    }

    fn index_range(
        &self,
        table: &str,
        column: &str,
        branch: &str,
        start: Bound<&Value>,
        end: Bound<&Value>,
    ) -> Vec<ObjectId> {
        (**self).index_range(table, column, branch, start, end)
    }

    fn index_scan_all(&self, table: &str, column: &str, branch: &str) -> Vec<ObjectId> {
        (**self).index_scan_all(table, column, branch)
    }

    fn flush(&self) {
        (**self).flush();
    }

    fn flush_wal(&self) {
        (**self).flush_wal();
    }

    fn close(&self) -> Result<(), StorageError> {
        (**self).close()
    }
}

// ============================================================================
// MemoryStorage - In-memory implementation for testing and main thread
// ============================================================================

/// Ordered raw-table rows keyed by their local storage key.
type RawTableEntries = BTreeMap<String, Vec<u8>>;

#[derive(Debug, Clone, Default)]
struct TableRowHistories {
    visible: BTreeMap<SharedString, BTreeMap<ObjectId, VisibleRowEntry>>,
    history: BTreeMap<(ObjectId, SharedString, BatchId), StoredRowBatch>,
}

impl TableRowHistories {
    fn history_rows_for(&self, branch: &str, row_id: ObjectId) -> Vec<StoredRowBatch> {
        let mut rows: Vec<_> = self
            .history
            .iter()
            .filter(|((history_row_id, history_branch, _), _)| {
                *history_row_id == row_id && history_branch.as_str() == branch
            })
            .map(|(_, row)| row.clone())
            .collect();
        rows.sort_by_key(|row| (row.updated_at, row.batch_id()));
        rows
    }

    fn rebuild_visible_entry(&mut self, branch: &str, row_id: ObjectId) {
        let Some(current_row) = self
            .visible
            .get(branch)
            .and_then(|rows| rows.get(&row_id))
            .map(|entry| entry.current_row.clone())
        else {
            return;
        };
        let branch_key = current_row.branch.clone();

        let mut history_rows = self.history_rows_for(branch, row_id);
        if !history_rows
            .iter()
            .any(|row| row.batch_id() == current_row.batch_id())
        {
            history_rows.push(current_row.clone());
        }

        self.visible
            .entry(branch_key)
            .or_default()
            .insert(row_id, VisibleRowEntry::rebuild(current_row, &history_rows));
    }
}

/// In-memory Storage for testing and main-thread use.
///
/// Stores objects and raw tables in HashMaps/BTreeMaps. No persistence.
/// This is sufficient for:
/// - All jazz unit tests
/// - All jazz integration tests
/// - Main thread in browser (acts as cache of worker state)
#[derive(Default)]
pub struct MemoryStorage {
    /// Ordered raw-table storage.
    raw_tables: HashMap<String, RawTableEntries>,
    /// Decoded row locators keyed by logical row id.
    row_locators: HashMap<ObjectId, RowLocator>,
    /// Row-history storage keyed by table.
    row_histories: HashMap<String, TableRowHistories>,
    /// Raw encoded row-history bytes keyed by table, row id, branch, and batch id.
    row_history_bytes: HashMap<String, EncodedTableRowHistories>,
}

impl MemoryStorage {
    /// Create a new empty MemoryStorage.
    pub fn new() -> Self {
        Self::default()
    }
}

// ============================================================================
// Value Encoding for Index Keys
// ============================================================================
//
// Values must be encoded so lexicographic byte ordering equals semantic ordering.
// This enables range queries via BTreeMap::range().

/// Returns true if the value is Double(0.0) or Double(-0.0).
///
/// IEEE 754 defines -0.0 == 0.0, but they have distinct bit patterns and
/// therefore distinct index encodings. Query operations must check both.
pub(crate) fn is_double_zero(value: &Value) -> bool {
    matches!(value, Value::Double(f) if *f == 0.0)
}

/// Encode a Value into bytes that sort correctly for range queries.
pub(crate) fn encode_value(value: &Value) -> Vec<u8> {
    match value {
        Value::Null => vec![0x00], // Null sorts first

        Value::Boolean(b) => {
            // false (0x01) < true (0x02)
            vec![0x01, if *b { 0x02 } else { 0x01 }]
        }

        Value::Integer(n) => {
            // Flip sign bit so negative < positive, big-endian for correct ordering
            let mut bytes = vec![0x02];
            bytes.extend_from_slice(&((*n as i64) ^ i64::MIN).to_be_bytes());
            bytes
        }

        Value::BigInt(n) => {
            // Flip sign bit so negative < positive, big-endian for correct ordering
            let mut bytes = vec![0x03];
            bytes.extend_from_slice(&(*n ^ i64::MIN).to_be_bytes());
            bytes
        }

        Value::Double(f) => {
            let mut bytes = vec![0x09];
            let bits = f.to_bits();
            // Flip for lexicographic ordering: if sign bit set, flip all bits;
            // otherwise flip only the sign bit.
            let ordered = if bits & (1u64 << 63) != 0 {
                !bits
            } else {
                bits ^ (1u64 << 63)
            };
            bytes.extend_from_slice(&ordered.to_be_bytes());
            bytes
        }

        Value::Timestamp(ts) => {
            // Unsigned, big-endian (already sorts correctly)
            let mut bytes = vec![0x04];
            bytes.extend_from_slice(&ts.to_be_bytes());
            bytes
        }

        Value::Text(s) => {
            // UTF-8 bytes sort correctly for ASCII; good enough for now
            let mut bytes = vec![0x05];
            bytes.extend_from_slice(s.as_bytes());
            bytes
        }

        Value::Uuid(id) => {
            // UUID bytes (UUIDv7 is time-ordered)
            let mut bytes = vec![0x06];
            bytes.extend_from_slice(id.uuid().as_bytes());
            bytes
        }

        Value::Bytea(bytes_value) => {
            // Raw bytes for exact-match index semantics.
            let mut bytes = vec![0x09];
            bytes.extend_from_slice(bytes_value);
            bytes
        }

        Value::Array(_) => {
            // Arrays use serialized bytes for equality semantics.
            // The durable key codec hashes oversized segments if needed.
            let mut bytes = vec![0x07];
            let json = serde_json::to_string(value).unwrap_or_default();
            bytes.extend_from_slice(json.as_bytes());
            bytes
        }

        Value::Row { .. } => {
            // Rows use serialized bytes for equality semantics.
            // The durable key codec hashes oversized segments if needed.
            let mut bytes = vec![0x08];
            let json = serde_json::to_string(value).unwrap_or_default();
            bytes.extend_from_slice(json.as_bytes());
            bytes
        }
    }
}

impl Storage for MemoryStorage {
    fn put_metadata(
        &mut self,
        id: ObjectId,
        metadata: HashMap<String, String>,
    ) -> Result<(), StorageError> {
        let bytes = encode_metadata(&metadata)?;
        self.raw_tables
            .entry(METADATA_TABLE.to_string())
            .or_default()
            .insert(metadata_raw_key(id), bytes);

        self.put_row_locator(id, row_locator_from_metadata(&metadata).as_ref())?;

        Ok(())
    }

    fn patch_exact_row_batch_for_schema_hash(
        &mut self,
        table: &str,
        _schema_hash: SchemaHash,
        branch: &str,
        row_id: ObjectId,
        batch_id: BatchId,
        state: Option<RowState>,
        confirmed_tier: Option<DurabilityTier>,
    ) -> Result<bool, StorageError> {
        self.patch_exact_row_batch(table, branch, row_id, batch_id, state, confirmed_tier)
    }

    fn put_row_locator(
        &mut self,
        id: ObjectId,
        locator: Option<&RowLocator>,
    ) -> Result<(), StorageError> {
        if let Some(locator) = locator {
            let locator_bytes = encode_row_locator(locator)?;
            self.raw_tables
                .entry(ROW_LOCATOR_TABLE.to_string())
                .or_default()
                .insert(metadata_raw_key(id), locator_bytes);
            self.row_locators.insert(id, locator.clone());
        } else {
            if let Some(rows) = self.raw_tables.get_mut(ROW_LOCATOR_TABLE) {
                rows.remove(&metadata_raw_key(id));
            }
            self.row_locators.remove(&id);
        }

        Ok(())
    }

    fn load_row_locator(&self, id: ObjectId) -> Result<Option<RowLocator>, StorageError> {
        Ok(self.row_locators.get(&id).cloned())
    }

    fn raw_table_put(&mut self, table: &str, key: &str, value: &[u8]) -> Result<(), StorageError> {
        self.raw_tables
            .entry(table.to_string())
            .or_default()
            .insert(key.to_string(), value.to_vec());
        Ok(())
    }

    fn raw_table_delete(&mut self, table: &str, key: &str) -> Result<(), StorageError> {
        if let Some(rows) = self.raw_tables.get_mut(table) {
            rows.remove(key);
        }
        Ok(())
    }

    fn raw_table_get(&self, table: &str, key: &str) -> Result<Option<Vec<u8>>, StorageError> {
        Ok(self
            .raw_tables
            .get(table)
            .and_then(|rows| rows.get(key))
            .cloned())
    }

    fn raw_table_scan_prefix(
        &self,
        table: &str,
        prefix: &str,
    ) -> Result<RawTableRows, StorageError> {
        let Some(rows) = self.raw_tables.get(table) else {
            return Ok(Vec::new());
        };
        Ok(rows
            .range(prefix.to_string()..)
            .take_while(|(key, _)| key.starts_with(prefix))
            .map(|(key, value)| (key.clone(), value.clone()))
            .collect())
    }

    fn raw_table_scan_prefix_keys(
        &self,
        table: &str,
        prefix: &str,
    ) -> Result<RawTableKeys, StorageError> {
        let Some(rows) = self.raw_tables.get(table) else {
            return Ok(Vec::new());
        };
        Ok(rows
            .range(prefix.to_string()..)
            .take_while(|(key, _)| key.starts_with(prefix))
            .map(|(key, _)| key.clone())
            .collect())
    }

    fn raw_table_scan_range(
        &self,
        table: &str,
        start: Option<&str>,
        end: Option<&str>,
    ) -> Result<RawTableRows, StorageError> {
        let Some(rows) = self.raw_tables.get(table) else {
            return Ok(Vec::new());
        };

        let start = start.map(str::to_string);
        let end = end.map(str::to_string);

        Ok(match (start.as_ref(), end.as_ref()) {
            (Some(start), Some(end)) => rows
                .range(start.clone()..end.clone())
                .map(|(key, value)| (key.clone(), value.clone()))
                .collect(),
            (Some(start), None) => rows
                .range(start.clone()..)
                .map(|(key, value)| (key.clone(), value.clone()))
                .collect(),
            (None, Some(end)) => rows
                .range(..end.clone())
                .map(|(key, value)| (key.clone(), value.clone()))
                .collect(),
            (None, None) => rows
                .iter()
                .map(|(key, value)| (key.clone(), value.clone()))
                .collect(),
        })
    }

    fn raw_table_scan_range_keys(
        &self,
        table: &str,
        start: Option<&str>,
        end: Option<&str>,
    ) -> Result<RawTableKeys, StorageError> {
        let Some(rows) = self.raw_tables.get(table) else {
            return Ok(Vec::new());
        };

        let start = start.map(str::to_string);
        let end = end.map(str::to_string);

        Ok(match (start.as_ref(), end.as_ref()) {
            (Some(start), Some(end)) => rows
                .range(start.clone()..end.clone())
                .map(|(key, _)| key.clone())
                .collect(),
            (Some(start), None) => rows
                .range(start.clone()..)
                .map(|(key, _)| key.clone())
                .collect(),
            (None, Some(end)) => rows
                .range(..end.clone())
                .map(|(key, _)| key.clone())
                .collect(),
            (None, None) => rows.keys().cloned().collect(),
        })
    }

    fn append_history_region_rows(
        &mut self,
        table: &str,
        rows: &[StoredRowBatch],
    ) -> Result<(), StorageError> {
        let encoded_rows = encode_history_row_bytes_for_storage(self, table, rows)?;
        let regions = self.row_histories.entry(table.to_string()).or_default();
        let raw_regions = self.row_history_bytes.entry(table.to_string()).or_default();
        for row in rows {
            regions.history.insert(
                (row.row_id, row.branch.clone(), row.batch_id()),
                row.clone(),
            );
        }
        for row in encoded_rows {
            raw_regions.insert(
                (row.row_id, row.branch.clone().into(), row.batch_id),
                row.bytes,
            );
        }
        Ok(())
    }

    fn append_history_region_row_bytes(
        &mut self,
        table: &str,
        rows: &[HistoryRowBytes<'_>],
    ) -> Result<(), StorageError> {
        let regions = self.row_history_bytes.entry(table.to_string()).or_default();
        for row in rows {
            regions.insert(
                (row.row_id, row.branch.to_string().into(), row.batch_id),
                row.bytes.to_vec(),
            );
        }
        Ok(())
    }

    fn upsert_visible_region_rows(
        &mut self,
        table: &str,
        entries: &[VisibleRowEntry],
    ) -> Result<(), StorageError> {
        let regions = self.row_histories.entry(table.to_string()).or_default();
        for entry in entries {
            regions
                .visible
                .entry(entry.current_row.branch.clone())
                .or_default()
                .insert(entry.current_row.row_id, entry.clone());
        }
        Ok(())
    }

    fn delete_visible_region_row(
        &mut self,
        table: &str,
        branch: &str,
        row_id: ObjectId,
    ) -> Result<(), StorageError> {
        let Some(regions) = self.row_histories.get_mut(table) else {
            return Ok(());
        };
        if let Some(rows) = regions.visible.get_mut(branch) {
            rows.remove(&row_id);
            if rows.is_empty() {
                regions.visible.remove(branch);
            }
        }
        Ok(())
    }

    fn patch_row_region_rows_by_batch(
        &mut self,
        table: &str,
        batch_id: crate::row_histories::BatchId,
        state: Option<RowState>,
        confirmed_tier: Option<DurabilityTier>,
    ) -> Result<(), StorageError> {
        let Some(regions) = self.row_histories.get_mut(table) else {
            return Ok(());
        };

        let mut affected_visible_rows = HashSet::new();
        for row in regions.history.values_mut() {
            if row.batch_id == batch_id {
                if let Some(state) = state {
                    row.state = state;
                }
                row.confirmed_tier = match (row.confirmed_tier, confirmed_tier) {
                    (Some(existing), Some(incoming)) => Some(existing.max(incoming)),
                    (Some(existing), None) => Some(existing),
                    (None, incoming) => incoming,
                };
                affected_visible_rows.insert((row.branch.clone(), row.row_id));
            }
        }

        for branch_rows in regions.visible.values_mut() {
            for entry in branch_rows.values_mut() {
                let row = &mut entry.current_row;
                if row.batch_id == batch_id {
                    if let Some(state) = state {
                        row.state = state;
                    }
                    row.confirmed_tier = match (row.confirmed_tier, confirmed_tier) {
                        (Some(existing), Some(incoming)) => Some(existing.max(incoming)),
                        (Some(existing), None) => Some(existing),
                        (None, incoming) => incoming,
                    };
                    affected_visible_rows.insert((row.branch.clone(), row.row_id));
                }
            }
        }

        for (branch, row_id) in affected_visible_rows {
            regions.rebuild_visible_entry(&branch, row_id);
        }

        Ok(())
    }

    fn scan_visible_region(
        &self,
        table: &str,
        branch: &str,
    ) -> Result<Vec<StoredRowBatch>, StorageError> {
        let Some(regions) = self.row_histories.get(table) else {
            return Ok(Vec::new());
        };

        Ok(regions
            .visible
            .get(branch)
            .map(|rows| {
                rows.values()
                    .map(|entry| entry.current_row.clone())
                    .collect()
            })
            .unwrap_or_default())
    }

    fn load_visible_region_row(
        &self,
        table: &str,
        branch: &str,
        row_id: ObjectId,
    ) -> Result<Option<StoredRowBatch>, StorageError> {
        Ok(self.row_histories.get(table).and_then(|regions| {
            regions
                .visible
                .get(branch)
                .and_then(|rows| rows.get(&row_id))
                .map(|entry| entry.current_row.clone())
        }))
    }

    fn load_visible_query_row(
        &self,
        table: &str,
        branch: &str,
        row_id: ObjectId,
    ) -> Result<Option<QueryRowBatch>, StorageError> {
        Ok(self.row_histories.get(table).and_then(|regions| {
            regions
                .visible
                .get(branch)
                .and_then(|rows| rows.get(&row_id))
                .map(|entry| QueryRowBatch::from(&entry.current_row))
        }))
    }

    fn load_visible_region_entry(
        &self,
        table: &str,
        branch: &str,
        row_id: ObjectId,
    ) -> Result<Option<VisibleRowEntry>, StorageError> {
        Ok(self
            .row_histories
            .get(table)
            .and_then(|regions| regions.visible.get(branch))
            .and_then(|rows| rows.get(&row_id).cloned()))
    }

    fn load_visible_region_frontier(
        &self,
        table: &str,
        branch: &str,
        row_id: ObjectId,
    ) -> Result<Option<Vec<BatchId>>, StorageError> {
        Ok(self.row_histories.get(table).and_then(|regions| {
            regions
                .visible
                .get(branch)
                .and_then(|rows| rows.get(&row_id))
                .map(|entry| entry.branch_frontier.clone())
        }))
    }

    fn capture_family_visible_frontier(
        &self,
        target_branch_name: BranchName,
    ) -> Result<Vec<CapturedFrontierMember>, StorageError> {
        let family_branches: BTreeSet<_> = self
            .row_histories
            .values()
            .flat_map(|regions| regions.visible.keys())
            .filter_map(|branch_name| {
                let branch_name = BranchName::new(branch_name.as_str());
                branch_matches_transaction_family(branch_name, target_branch_name)
                    .then_some(branch_name.as_str().to_string())
            })
            .collect();
        if family_branches.is_empty() {
            return Ok(Vec::new());
        }

        let mut frontier = Vec::new();
        for regions in self.row_histories.values() {
            for branch_name in &family_branches {
                let Some(rows) = regions.visible.get(branch_name.as_str()) else {
                    continue;
                };
                for entry in rows.values() {
                    frontier.push(CapturedFrontierMember {
                        object_id: entry.current_row.row_id,
                        branch_name: BranchName::new(branch_name),
                        batch_id: entry.current_row.batch_id(),
                    });
                }
            }
        }

        frontier.sort_by(|left, right| {
            left.object_id
                .uuid()
                .as_bytes()
                .cmp(right.object_id.uuid().as_bytes())
                .then_with(|| left.branch_name.as_str().cmp(right.branch_name.as_str()))
                .then_with(|| left.batch_id.0.cmp(&right.batch_id.0))
        });
        frontier.dedup();
        Ok(frontier)
    }

    fn load_visible_region_row_for_tier(
        &self,
        table: &str,
        branch: &str,
        row_id: ObjectId,
        required_tier: DurabilityTier,
    ) -> Result<Option<StoredRowBatch>, StorageError> {
        let Some(regions) = self.row_histories.get(table) else {
            return Ok(None);
        };
        let Some(entry) = regions
            .visible
            .get(branch)
            .and_then(|rows| rows.get(&row_id))
        else {
            return Ok(None);
        };

        let Some(batch_id) = entry.batch_id_for_tier(required_tier) else {
            return Ok(None);
        };
        if batch_id == entry.current_batch_id() {
            return Ok(Some(entry.current_row.clone()));
        }

        Ok(regions
            .history_rows_for(branch, row_id)
            .into_iter()
            .find(|row| row.batch_id() == batch_id))
    }

    fn load_visible_query_row_for_tier(
        &self,
        table: &str,
        branch: &str,
        row_id: ObjectId,
        required_tier: DurabilityTier,
    ) -> Result<Option<QueryRowBatch>, StorageError> {
        let Some(regions) = self.row_histories.get(table) else {
            return Ok(None);
        };
        let Some(entry) = regions
            .visible
            .get(branch)
            .and_then(|rows| rows.get(&row_id))
        else {
            return Ok(None);
        };

        let Some(batch_id) = entry.batch_id_for_tier(required_tier) else {
            return Ok(None);
        };
        if batch_id == entry.current_batch_id() {
            return Ok(Some(QueryRowBatch::from(&entry.current_row)));
        }

        Ok(regions
            .history_rows_for(branch, row_id)
            .into_iter()
            .find(|row| row.batch_id() == batch_id)
            .map(|row| QueryRowBatch::from(&row)))
    }

    fn scan_visible_region_row_batches(
        &self,
        table: &str,
        row_id: ObjectId,
    ) -> Result<Vec<StoredRowBatch>, StorageError> {
        let Some(regions) = self.row_histories.get(table) else {
            return Ok(Vec::new());
        };

        let mut rows: Vec<_> = regions
            .visible
            .values()
            .filter_map(|branch_rows| branch_rows.get(&row_id))
            .map(|entry| entry.current_row.clone())
            .collect();
        rows.sort_by_key(|row| row.branch.clone());
        Ok(rows)
    }

    fn scan_history_row_batches(
        &self,
        table: &str,
        row_id: ObjectId,
    ) -> Result<Vec<StoredRowBatch>, StorageError> {
        let Some(regions) = self.row_histories.get(table) else {
            return Ok(Vec::new());
        };

        let mut rows: Vec<_> = regions
            .history
            .iter()
            .filter(|((history_row_id, _, _), _)| *history_row_id == row_id)
            .map(|(_, row)| row.clone())
            .collect();
        rows.sort_by_key(|row| (row.branch.clone(), row.updated_at, row.batch_id()));
        Ok(rows)
    }

    fn load_history_row_batch_bytes(
        &self,
        table: &str,
        branch: &str,
        row_id: ObjectId,
        batch_id: BatchId,
    ) -> Result<Option<Vec<u8>>, StorageError> {
        Ok(self.row_history_bytes.get(table).and_then(|regions| {
            regions
                .get(&(row_id, branch.to_string().into(), batch_id))
                .cloned()
        }))
    }

    fn scan_history_region_bytes(
        &self,
        table: &str,
        scan: HistoryScan,
    ) -> Result<Vec<Vec<u8>>, StorageError> {
        let Some(regions) = self.row_history_bytes.get(table) else {
            return Ok(Vec::new());
        };

        Ok(match scan {
            HistoryScan::Branch | HistoryScan::AsOf { .. } => {
                regions.values().cloned().collect::<Vec<_>>()
            }
            HistoryScan::Row { row_id } => regions
                .iter()
                .filter(|((history_row_id, _, _), _)| *history_row_id == row_id)
                .map(|(_, bytes)| bytes.clone())
                .collect::<Vec<_>>(),
        })
    }

    fn load_visible_region_row_bytes(
        &self,
        table: &str,
        branch: &str,
        row_id: ObjectId,
    ) -> Result<Option<Vec<u8>>, StorageError> {
        let Some(entry) = self
            .row_histories
            .get(table)
            .and_then(|regions| regions.visible.get(branch))
            .and_then(|rows| rows.get(&row_id))
            .cloned()
        else {
            return Ok(None);
        };

        let encoded =
            encode_visible_row_bytes_for_storage(self, table, std::slice::from_ref(&entry))?;
        Ok(encoded.into_iter().next().map(|row| row.bytes))
    }

    fn scan_visible_region_bytes(
        &self,
        table: &str,
        branch: &str,
    ) -> Result<Vec<Vec<u8>>, StorageError> {
        let Some(entries) = self
            .row_histories
            .get(table)
            .and_then(|regions| regions.visible.get(branch))
        else {
            return Ok(Vec::new());
        };

        let entries = entries.values().cloned().collect::<Vec<_>>();
        Ok(encode_visible_row_bytes_for_storage(self, table, &entries)?
            .into_iter()
            .map(|row| row.bytes)
            .collect())
    }

    fn load_history_row_batch(
        &self,
        table: &str,
        branch: &str,
        row_id: ObjectId,
        batch_id: BatchId,
    ) -> Result<Option<StoredRowBatch>, StorageError> {
        Ok(self.row_histories.get(table).and_then(|regions| {
            regions
                .history
                .get(&(row_id, branch.to_string().into(), batch_id))
                .cloned()
        }))
    }

    fn load_history_row_batch_for_schema_hash(
        &self,
        table: &str,
        _schema_hash: SchemaHash,
        branch: &str,
        row_id: ObjectId,
        batch_id: BatchId,
    ) -> Result<Option<StoredRowBatch>, StorageError> {
        self.load_history_row_batch(table, branch, row_id, batch_id)
    }

    fn load_history_query_row_batch(
        &self,
        table: &str,
        branch: &str,
        row_id: ObjectId,
        batch_id: BatchId,
    ) -> Result<Option<QueryRowBatch>, StorageError> {
        Ok(self
            .row_histories
            .get(table)
            .and_then(|regions| {
                regions
                    .history
                    .get(&(row_id, branch.to_string().into(), batch_id))
            })
            .map(QueryRowBatch::from))
    }

    fn scan_history_region(
        &self,
        table: &str,
        branch: &str,
        scan: HistoryScan,
    ) -> Result<Vec<StoredRowBatch>, StorageError> {
        let Some(regions) = self.row_histories.get(table) else {
            return Ok(Vec::new());
        };

        let mut rows: Vec<StoredRowBatch> = match scan {
            HistoryScan::Branch => regions
                .history
                .iter()
                .filter(|(_, row)| row.branch == branch)
                .map(|(_, row)| row.clone())
                .collect(),
            HistoryScan::Row { row_id } => regions
                .history
                .iter()
                .filter(|((history_row_id, _, _), row)| {
                    row.branch == branch && *history_row_id == row_id
                })
                .map(|(_, row)| row.clone())
                .collect(),
            HistoryScan::AsOf { ts } => {
                let mut latest_per_row: BTreeMap<ObjectId, StoredRowBatch> = BTreeMap::new();
                for ((row_id, _, _), row) in &regions.history {
                    if row.branch != branch || row.updated_at > ts || !row.state.is_visible() {
                        continue;
                    }
                    match latest_per_row.get(row_id) {
                        Some(existing)
                            if (existing.updated_at, existing.batch_id())
                                >= (row.updated_at, row.batch_id()) => {}
                        _ => {
                            latest_per_row.insert(*row_id, row.clone());
                        }
                    }
                }
                latest_per_row.into_values().collect()
            }
        };

        rows.sort_by_key(|row| {
            (
                row.branch.clone(),
                row.row_id,
                row.updated_at,
                row.batch_id(),
            )
        });
        Ok(rows)
    }
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use crate::metadata::RowProvenance;
    use crate::object::BranchName;
    use crate::query_manager::types::{
        ColumnDescriptor, ColumnType, RowDescriptor, SchemaBuilder, SchemaHash, TableSchema,
    };
    use crate::row_format::encode_row;
    use crate::row_histories::{decode_flat_history_row, encode_flat_history_row};
    use crate::test_row_history::persist_test_schema;

    fn users_test_descriptor() -> RowDescriptor {
        RowDescriptor::new(vec![ColumnDescriptor::new("value", ColumnType::Text)])
    }

    fn users_test_schema() -> crate::query_manager::types::Schema {
        SchemaBuilder::new()
            .table(TableSchema::builder("users").column("value", ColumnType::Text))
            .build()
    }

    fn seed_users_schema(storage: &mut MemoryStorage) -> SchemaHash {
        persist_test_schema(storage, &users_test_schema())
    }

    fn seed_users_row(storage: &mut MemoryStorage, row_id: ObjectId, schema_hash: SchemaHash) {
        storage
            .put_row_locator(
                row_id,
                Some(&RowLocator {
                    table: "users".into(),
                    origin_schema_hash: Some(schema_hash),
                }),
            )
            .unwrap();
    }

    fn make_users_row_batch(
        row_id: ObjectId,
        branch: &str,
        value: &str,
        provenance: RowProvenance,
        state: crate::row_histories::RowState,
        durability: Option<DurabilityTier>,
        parents: Vec<BatchId>,
    ) -> crate::row_histories::StoredRowBatch {
        crate::row_histories::StoredRowBatch::new(
            row_id,
            branch,
            parents,
            encode_row(&users_test_descriptor(), &[Value::Text(value.to_string())]).unwrap(),
            provenance,
            HashMap::new(),
            state,
            durability,
        )
    }

    struct FailOnNthRawPutStorage {
        inner: MemoryStorage,
        fail_on_put_number: usize,
        raw_puts_seen: usize,
    }

    impl FailOnNthRawPutStorage {
        fn new(fail_on_put_number: usize) -> Self {
            Self {
                inner: MemoryStorage::new(),
                fail_on_put_number,
                raw_puts_seen: 0,
            }
        }
    }

    impl Storage for FailOnNthRawPutStorage {
        fn raw_table_put(
            &mut self,
            table: &str,
            key: &str,
            value: &[u8],
        ) -> Result<(), StorageError> {
            self.raw_puts_seen += 1;
            if self.raw_puts_seen == self.fail_on_put_number {
                return Err(StorageError::IoError(format!(
                    "simulated raw_table_put failure #{:?} for {table}:{key}",
                    self.fail_on_put_number
                )));
            }
            self.inner.raw_table_put(table, key, value)
        }

        fn raw_table_delete(&mut self, table: &str, key: &str) -> Result<(), StorageError> {
            self.inner.raw_table_delete(table, key)
        }

        fn raw_table_get(&self, table: &str, key: &str) -> Result<Option<Vec<u8>>, StorageError> {
            self.inner.raw_table_get(table, key)
        }

        fn raw_table_scan_prefix(
            &self,
            table: &str,
            prefix: &str,
        ) -> Result<RawTableRows, StorageError> {
            self.inner.raw_table_scan_prefix(table, prefix)
        }

        fn raw_table_scan_range(
            &self,
            table: &str,
            start: Option<&str>,
            end: Option<&str>,
        ) -> Result<RawTableRows, StorageError> {
            self.inner.raw_table_scan_range(table, start, end)
        }
    }

    #[test]
    fn encode_value_ordering() {
        // Null < Boolean < Integer < BigInt < Timestamp < Text < Uuid

        let null = encode_value(&Value::Null);
        let bool_false = encode_value(&Value::Boolean(false));
        let bool_true = encode_value(&Value::Boolean(true));
        let int_neg = encode_value(&Value::Integer(-100));
        let int_zero = encode_value(&Value::Integer(0));
        let int_pos = encode_value(&Value::Integer(100));

        assert!(null < bool_false);
        assert!(bool_false < bool_true);
        assert!(bool_true < int_neg);
        assert!(int_neg < int_zero);
        assert!(int_zero < int_pos);
    }

    #[test]
    fn real_encode_value_ordering() {
        let neg_inf = encode_value(&Value::Double(f64::NEG_INFINITY));
        let neg_big = encode_value(&Value::Double(-1000.0));
        let neg_small = encode_value(&Value::Double(-0.001));
        let neg_zero = encode_value(&Value::Double(-0.0));
        let pos_zero = encode_value(&Value::Double(0.0));
        let pos_small = encode_value(&Value::Double(0.001));
        let pos_big = encode_value(&Value::Double(1000.0));
        let pos_inf = encode_value(&Value::Double(f64::INFINITY));

        assert!(neg_inf < neg_big);
        assert!(neg_big < neg_small);
        assert!(neg_small < neg_zero);
        assert!(neg_zero < pos_zero);
        assert!(pos_zero < pos_small);
        assert!(pos_small < pos_big);
        assert!(pos_big < pos_inf);
    }

    #[test]
    fn real_cross_type_ordering() {
        // Double should sort after all existing types (tag 0x09 > 0x08)
        let row = encode_value(&Value::Row {
            id: None,
            values: vec![],
        });
        let double = encode_value(&Value::Double(0.0));

        assert!(row < double);
    }

    // ----------------------------------------------------------------
    // Negative zero IEEE 754 semantics: -0.0 and 0.0 are equal per the
    // standard, so index lookups and range queries must treat them as
    // the same value even though they have distinct bit patterns.
    // ----------------------------------------------------------------

    #[test]
    fn real_negative_zero_exact_lookup() {
        // Store a value as -0.0, look it up with 0.0 (and vice versa).
        let mut storage = MemoryStorage::new();

        let row_neg = ObjectId::new();
        let row_pos = ObjectId::new();

        storage
            .index_insert("prices", "amount", "main", &Value::Double(-0.0), row_neg)
            .unwrap();
        storage
            .index_insert("prices", "amount", "main", &Value::Double(0.0), row_pos)
            .unwrap();

        // Looking up 0.0 should find both (IEEE 754: -0.0 == 0.0)
        let results = storage.index_lookup("prices", "amount", "main", &Value::Double(0.0));
        assert_eq!(results.len(), 2, "lookup 0.0 should match both zeros");
        assert!(results.contains(&row_neg));
        assert!(results.contains(&row_pos));

        // Looking up -0.0 should also find both
        let results = storage.index_lookup("prices", "amount", "main", &Value::Double(-0.0));
        assert_eq!(results.len(), 2, "lookup -0.0 should match both zeros");
        assert!(results.contains(&row_neg));
        assert!(results.contains(&row_pos));
    }

    #[test]
    fn branch_ord_allocation_commits_atomically_in_one_put() {
        let mut storage = FailOnNthRawPutStorage::new(2);
        let main = BranchName::new("dev-aaaaaaaaaaaa-main");

        let branch_ord = storage
            .resolve_or_alloc_branch_ord(main)
            .expect("branch ord allocation should fit in one raw_table_put");

        assert_eq!(branch_ord, 1);
        assert_eq!(storage.raw_puts_seen, 1);
        assert_eq!(storage.load_branch_ord(main).unwrap(), Some(branch_ord));
        assert_eq!(
            storage.load_branch_name_by_ord(branch_ord).unwrap(),
            Some(main)
        );
    }

    #[test]
    fn real_negative_zero_range_gte() {
        // WHERE amount >= 0.0 should include -0.0 (equal per IEEE 754)
        let mut storage = MemoryStorage::new();

        let row_neg_zero = ObjectId::new();
        let row_pos_zero = ObjectId::new();
        let row_negative = ObjectId::new();

        storage
            .index_insert(
                "prices",
                "amount",
                "main",
                &Value::Double(-0.0),
                row_neg_zero,
            )
            .unwrap();
        storage
            .index_insert(
                "prices",
                "amount",
                "main",
                &Value::Double(0.0),
                row_pos_zero,
            )
            .unwrap();
        storage
            .index_insert(
                "prices",
                "amount",
                "main",
                &Value::Double(-1.0),
                row_negative,
            )
            .unwrap();

        // >= 0.0 should include -0.0 and 0.0, but not -1.0
        let results = storage.index_range(
            "prices",
            "amount",
            "main",
            Bound::Included(&Value::Double(0.0)),
            Bound::Unbounded,
        );
        assert!(
            results.contains(&row_neg_zero),
            ">= 0.0 should include -0.0"
        );
        assert!(results.contains(&row_pos_zero), ">= 0.0 should include 0.0");
        assert!(
            !results.contains(&row_negative),
            ">= 0.0 should exclude -1.0"
        );
    }

    #[test]
    fn real_negative_zero_range_lt() {
        // WHERE amount < 0.0 should exclude -0.0 (equal per IEEE 754, not strictly less)
        let mut storage = MemoryStorage::new();

        let row_neg_zero = ObjectId::new();
        let row_negative = ObjectId::new();

        storage
            .index_insert(
                "prices",
                "amount",
                "main",
                &Value::Double(-0.0),
                row_neg_zero,
            )
            .unwrap();
        storage
            .index_insert(
                "prices",
                "amount",
                "main",
                &Value::Double(-1.0),
                row_negative,
            )
            .unwrap();

        // < 0.0 should exclude -0.0 but include -1.0
        let results = storage.index_range(
            "prices",
            "amount",
            "main",
            Bound::Unbounded,
            Bound::Excluded(&Value::Double(0.0)),
        );
        assert!(
            !results.contains(&row_neg_zero),
            "< 0.0 should exclude -0.0"
        );
        assert!(results.contains(&row_negative), "< 0.0 should include -1.0");
    }

    #[test]
    fn memory_storage_catalogue_entry_upsert_overwrites_existing() {
        let mut storage = MemoryStorage::new();
        let object_id = ObjectId::new();
        let initial = CatalogueEntry {
            object_id,
            metadata: HashMap::from([(
                crate::metadata::MetadataKey::Type.to_string(),
                crate::metadata::ObjectType::CatalogueSchema.to_string(),
            )]),
            content: b"v1".to_vec(),
        };
        let updated = CatalogueEntry {
            object_id,
            metadata: HashMap::from([(
                crate::metadata::MetadataKey::Type.to_string(),
                crate::metadata::ObjectType::CatalogueSchema.to_string(),
            )]),
            content: b"v2".to_vec(),
        };

        storage.upsert_catalogue_entry(&initial).unwrap();
        storage.upsert_catalogue_entry(&updated).unwrap();

        let loaded = storage.load_catalogue_entry(object_id).unwrap();
        assert_eq!(loaded, Some(updated.clone()));
        assert_eq!(storage.scan_catalogue_entries().unwrap(), vec![updated]);
    }

    #[test]
    fn memory_storage_row_histories_visible_and_history_round_trip() {
        use crate::row_histories::{HistoryScan, RowState, VisibleRowEntry};

        let mut storage = MemoryStorage::new();
        let schema_hash = seed_users_schema(&mut storage);
        let row_id = ObjectId::new();
        seed_users_row(&mut storage, row_id, schema_hash);
        let version = make_users_row_batch(
            row_id,
            "dev/main",
            "alice",
            crate::metadata::RowProvenance::for_insert("alice".to_string(), 10),
            RowState::VisibleDirect,
            Some(DurabilityTier::Worker),
            Vec::new(),
        );

        storage
            .append_history_region_rows("users", &[version.clone()])
            .unwrap();
        storage
            .upsert_visible_region_rows(
                "users",
                &[VisibleRowEntry::rebuild(
                    version.clone(),
                    std::slice::from_ref(&version),
                )],
            )
            .unwrap();

        let visible = storage.scan_visible_region("users", "dev/main").unwrap();
        let history_by_row = storage.scan_history_row_batches("users", row_id).unwrap();
        let history = storage
            .scan_history_region("users", "dev/main", HistoryScan::Row { row_id })
            .unwrap();

        assert_eq!(visible, vec![version.clone()]);
        assert_eq!(history_by_row, vec![version.clone()]);
        assert_eq!(history, vec![version]);
    }

    #[test]
    fn exact_history_row_load_uses_row_locator_schema_hash_not_branch_short_hash() {
        let mut storage = MemoryStorage::new();
        let schema_hash = seed_users_schema(&mut storage);
        let row_id = ObjectId::new();
        seed_users_row(&mut storage, row_id, schema_hash);

        let branch = "dev-deadbeefcafe-main";
        assert_ne!(schema_hash.short(), "deadbeefcafe");

        let row = make_users_row_batch(
            row_id,
            branch,
            "alice",
            crate::metadata::RowProvenance::for_insert("alice".to_string(), 10),
            crate::row_histories::RowState::VisibleDirect,
            Some(DurabilityTier::Worker),
            Vec::new(),
        );

        storage
            .append_history_region_rows("users", std::slice::from_ref(&row))
            .unwrap();

        let loaded =
            Storage::load_history_row_batch(&storage, "users", branch, row_id, row.batch_id())
                .unwrap();

        assert_eq!(loaded, Some(row));
    }

    #[test]
    fn visible_branch_scan_unions_namespaces_without_branch_hash_matching() {
        use crate::row_histories::VisibleRowEntry;

        let mut storage = MemoryStorage::new();
        let schema_hash = seed_users_schema(&mut storage);
        let row_id = ObjectId::new();
        seed_users_row(&mut storage, row_id, schema_hash);

        let branch = "dev-deadbeefcafe-main";
        assert_ne!(schema_hash.short(), "deadbeefcafe");

        let row = make_users_row_batch(
            row_id,
            branch,
            "alice",
            crate::metadata::RowProvenance::for_insert("alice".to_string(), 10),
            crate::row_histories::RowState::VisibleDirect,
            Some(DurabilityTier::Worker),
            Vec::new(),
        );

        storage
            .append_history_region_rows("users", std::slice::from_ref(&row))
            .unwrap();
        storage
            .upsert_visible_region_rows(
                "users",
                &[VisibleRowEntry::rebuild(
                    row.clone(),
                    std::slice::from_ref(&row),
                )],
            )
            .unwrap();

        let loaded = storage.scan_visible_region("users", branch).unwrap();
        assert_eq!(loaded, vec![row.clone()]);

        let point_loaded = storage
            .load_visible_region_row("users", branch, row_id)
            .unwrap();
        assert_eq!(point_loaded, Some(row));
    }

    #[test]
    fn memory_storage_visible_entries_track_older_tier_winners() {
        use crate::row_histories::{RowState, VisibleRowEntry};

        let mut storage = MemoryStorage::new();
        let schema_hash = seed_users_schema(&mut storage);
        let row_id = ObjectId::new();
        seed_users_row(&mut storage, row_id, schema_hash);

        let globally_confirmed = make_users_row_batch(
            row_id,
            "dev/main",
            "v1",
            crate::metadata::RowProvenance::for_insert("alice".to_string(), 10),
            RowState::VisibleDirect,
            Some(DurabilityTier::GlobalServer),
            Vec::new(),
        );
        let current_worker = make_users_row_batch(
            row_id,
            "dev/main",
            "v2",
            crate::metadata::RowProvenance {
                created_by: "alice".to_string(),
                created_at: 10,
                updated_by: "alice".to_string(),
                updated_at: 20,
            },
            RowState::VisibleDirect,
            Some(DurabilityTier::Worker),
            vec![globally_confirmed.batch_id()],
        );

        storage
            .append_history_region_rows(
                "users",
                &[globally_confirmed.clone(), current_worker.clone()],
            )
            .unwrap();
        storage
            .upsert_visible_region_rows(
                "users",
                std::slice::from_ref(&VisibleRowEntry::rebuild(
                    current_worker.clone(),
                    &[globally_confirmed.clone(), current_worker.clone()],
                )),
            )
            .unwrap();

        let visible = storage
            .load_visible_region_row("users", "dev/main", row_id)
            .unwrap();
        let entry = storage
            .row_histories
            .get("users")
            .and_then(|regions| regions.visible.get("dev/main"))
            .and_then(|rows| rows.get(&row_id))
            .cloned()
            .expect("visible entry");

        assert_eq!(visible, Some(current_worker.clone()));
        assert_eq!(entry.current_row, current_worker);
        assert_eq!(entry.worker_batch_id, None);
        assert_eq!(entry.edge_batch_id, Some(globally_confirmed.batch_id()));
        assert_eq!(entry.global_batch_id, Some(globally_confirmed.batch_id()));
    }

    #[test]
    fn raw_history_bytes_roundtrip_flat_rows_outside_storage() {
        let mut storage = MemoryStorage::new();
        let user_descriptor = RowDescriptor::new(vec![
            ColumnDescriptor::new("title", ColumnType::Text),
            ColumnDescriptor::new("done", ColumnType::Boolean),
        ]);
        let row_id = ObjectId::new();
        let row = crate::row_histories::StoredRowBatch::new(
            row_id,
            "main",
            Vec::new(),
            encode_row(
                &user_descriptor,
                &[Value::Text("Ship flat rows".into()), Value::Boolean(false)],
            )
            .unwrap(),
            RowProvenance::for_insert("alice".to_string(), 100),
            HashMap::new(),
            crate::row_histories::RowState::VisibleDirect,
            None,
        );
        let encoded = encode_flat_history_row(&user_descriptor, &row).unwrap();

        storage
            .append_history_region_row_bytes(
                "tasks",
                &[HistoryRowBytes {
                    namespace_raw_table: "rowns:history:tasks:test",
                    branch: row.branch.as_str(),
                    row_id,
                    batch_id: row.batch_id(),
                    bytes: &encoded,
                }],
            )
            .unwrap();

        let loaded = storage
            .load_history_row_batch_bytes("tasks", row.branch.as_str(), row_id, row.batch_id())
            .unwrap()
            .expect("history bytes should load");
        assert_eq!(
            decode_flat_history_row(
                &user_descriptor,
                row_id,
                row.branch.as_str(),
                row.batch_id(),
                &loaded,
            )
            .unwrap(),
            row
        );

        let scanned = storage
            .scan_history_region_bytes("tasks", HistoryScan::Row { row_id })
            .unwrap();
        assert_eq!(scanned.len(), 1);
        assert_eq!(
            decode_flat_history_row(
                &user_descriptor,
                row_id,
                row.branch.as_str(),
                row.batch_id(),
                &scanned[0],
            )
            .unwrap(),
            row
        );
    }

    #[test]
    fn typed_history_appends_use_flat_rows_when_schema_is_known() {
        use crate::catalogue::CatalogueEntry;
        use crate::metadata::{MetadataKey, ObjectType};
        use crate::query_manager::types::{SchemaBuilder, SchemaHash, TableSchema, Value};
        use crate::schema_manager::encoding::encode_schema;

        let mut storage = MemoryStorage::new();
        let schema = SchemaBuilder::new()
            .table(
                TableSchema::builder("tasks")
                    .column("title", ColumnType::Text)
                    .nullable_column("done", ColumnType::Boolean),
            )
            .build();
        let schema_hash = SchemaHash::compute(&schema);
        let user_descriptor = schema[&"tasks".into()].columns.clone();
        let row_id = ObjectId::new();
        let row_locator = RowLocator {
            table: "tasks".into(),
            origin_schema_hash: Some(schema_hash),
        };

        storage
            .upsert_catalogue_entry(&CatalogueEntry {
                object_id: schema_hash.to_object_id(),
                metadata: HashMap::from([(
                    MetadataKey::Type.to_string(),
                    ObjectType::CatalogueSchema.to_string(),
                )]),
                content: encode_schema(&schema),
            })
            .unwrap();
        storage.put_row_locator(row_id, Some(&row_locator)).unwrap();

        let row = crate::row_histories::StoredRowBatch::new(
            row_id,
            "main",
            Vec::new(),
            encode_row(
                &user_descriptor,
                &[Value::Text("Ship flat rows".into()), Value::Boolean(false)],
            )
            .unwrap(),
            RowProvenance::for_insert("alice".to_string(), 100),
            HashMap::new(),
            crate::row_histories::RowState::VisibleDirect,
            None,
        );

        storage
            .append_history_region_rows("tasks", std::slice::from_ref(&row))
            .unwrap();

        let encoded = storage
            .load_history_row_batch_bytes("tasks", row.branch.as_str(), row_id, row.batch_id())
            .unwrap()
            .expect("history bytes should load");
        assert_eq!(
            decode_flat_history_row(
                &user_descriptor,
                row_id,
                row.branch.as_str(),
                row.batch_id(),
                &encoded,
            )
            .unwrap(),
            row
        );
    }

    #[test]
    fn typed_history_appends_require_catalogue_backed_descriptor() {
        use crate::query_manager::types::{SchemaBuilder, TableSchema, Value};

        let mut storage = MemoryStorage::new();
        let schema = SchemaBuilder::new()
            .table(TableSchema::builder("tasks").column("title", ColumnType::Text))
            .build();
        let user_descriptor = schema[&"tasks".into()].columns.clone();
        let row_id = ObjectId::new();
        let row = crate::row_histories::StoredRowBatch::new(
            row_id,
            "main",
            Vec::new(),
            encode_row(&user_descriptor, &[Value::Text("Needs schema".into())]).unwrap(),
            RowProvenance::for_insert("alice".to_string(), 100),
            HashMap::new(),
            crate::row_histories::RowState::VisibleDirect,
            None,
        );

        let error = storage
            .append_history_region_rows("tasks", std::slice::from_ref(&row))
            .expect_err("typed history writes should require a catalogue-backed descriptor");

        assert!(
            matches!(error, StorageError::IoError(ref message) if message.contains("missing catalogue-backed row descriptor")),
            "unexpected error: {error:?}"
        );
    }

    mod memory_conformance {
        use crate::storage::MemoryStorage;
        use crate::storage::Storage;

        crate::storage_conformance_tests!(memory, || {
            Box::new(MemoryStorage::new()) as Box<dyn Storage>
        });
    }
}
