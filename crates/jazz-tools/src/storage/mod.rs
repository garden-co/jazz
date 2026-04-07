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
#[cfg(all(feature = "fjall", not(target_arch = "wasm32")))]
mod fjall;
#[cfg(all(feature = "fjall", not(target_arch = "wasm32")))]
pub use fjall::FjallStorage;
#[cfg(all(feature = "rocksdb", not(target_arch = "wasm32")))]
mod rocksdb;
#[cfg(all(feature = "rocksdb", not(target_arch = "wasm32")))]
pub use rocksdb::RocksDBStorage;
#[cfg(all(feature = "sqlite", not(target_arch = "wasm32")))]
mod sqlite;
#[cfg(all(feature = "sqlite", not(target_arch = "wasm32")))]
pub use sqlite::SqliteStorage;

use std::collections::{BTreeMap, HashMap, HashSet};
use std::ops::Bound;

use serde::{Deserialize, Serialize};

use crate::catalogue::CatalogueEntry;
use crate::object::ObjectId;
use crate::query_manager::types::Value;
use crate::row_regions::{HistoryScan, RowState, StoredRowVersion};
use crate::sync_manager::DurabilityTier;

// ============================================================================
// Storage Types
// ============================================================================

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
pub type RawTableRows = Vec<(String, Vec<u8>)>;

const METADATA_TABLE: &str = "__metadata";

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

// ============================================================================
// Storage Trait
// ============================================================================

/// Synchronous storage for metadata, row regions, raw tables, and indices.
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
        self.raw_table_put(METADATA_TABLE, &metadata_raw_key(id), &bytes)
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

    // ================================================================
    // Row-region storage
    // ================================================================

    fn append_history_region_rows(
        &mut self,
        _table: &str,
        _rows: &[StoredRowVersion],
    ) -> Result<(), StorageError> {
        Err(StorageError::IoError(
            "row-region history appends are not implemented for this backend yet".to_string(),
        ))
    }

    fn upsert_visible_region_rows(
        &mut self,
        _table: &str,
        _rows: &[StoredRowVersion],
    ) -> Result<(), StorageError> {
        Err(StorageError::IoError(
            "row-region visible upserts are not implemented for this backend yet".to_string(),
        ))
    }

    fn patch_row_region_rows_by_batch(
        &mut self,
        _table: &str,
        _batch_id: crate::row_regions::BatchId,
        _state: Option<RowState>,
        _confirmed_tier: Option<DurabilityTier>,
    ) -> Result<(), StorageError> {
        Err(StorageError::IoError(
            "row-region history patching is not implemented for this backend yet".to_string(),
        ))
    }

    fn scan_visible_region(
        &self,
        _table: &str,
        _branch: &str,
    ) -> Result<Vec<StoredRowVersion>, StorageError> {
        Err(StorageError::IoError(
            "row-region visible scans are not implemented for this backend yet".to_string(),
        ))
    }

    fn load_visible_region_row(
        &self,
        _table: &str,
        _branch: &str,
        _row_id: ObjectId,
    ) -> Result<Option<StoredRowVersion>, StorageError> {
        Err(StorageError::IoError(
            "row-region visible lookups are not implemented for this backend yet".to_string(),
        ))
    }

    fn scan_visible_region_row_versions(
        &self,
        _table: &str,
        _row_id: ObjectId,
    ) -> Result<Vec<StoredRowVersion>, StorageError> {
        Err(StorageError::IoError(
            "row-region visible row scans are not implemented for this backend yet".to_string(),
        ))
    }

    fn scan_history_row_versions(
        &self,
        _table: &str,
        _row_id: ObjectId,
    ) -> Result<Vec<StoredRowVersion>, StorageError> {
        Err(StorageError::IoError(
            "row-region history row scans are not implemented for this backend yet".to_string(),
        ))
    }

    fn scan_history_region(
        &self,
        _table: &str,
        _branch: &str,
        _scan: HistoryScan,
    ) -> Result<Vec<StoredRowVersion>, StorageError> {
        Err(StorageError::IoError(
            "row-region history scans are not implemented for this backend yet".to_string(),
        ))
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
        validate_index_value_size(table, column, branch, value)?;
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
                if let Ok(keys) = self.raw_table_scan_prefix(&raw_table, &prefix) {
                    for (key, _) in keys {
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
        self.raw_table_scan_prefix(&raw_table, &prefix)
            .map(|keys| {
                keys.into_iter()
                    .filter_map(|(key, _)| key_codec::parse_uuid_from_index_key(&key))
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

        self.raw_table_scan_range(&raw_table, start_key.as_deref(), end_key.as_deref())
            .map(|keys| {
                keys.into_iter()
                    .filter_map(|(key, _)| key_codec::parse_uuid_from_index_key(&key))
                    .collect()
            })
            .unwrap_or_default()
    }

    fn index_scan_all(&self, table: &str, column: &str, branch: &str) -> Vec<ObjectId> {
        let raw_table = key_codec::index_raw_table(table, column, branch);
        self.raw_table_scan_prefix(&raw_table, "")
            .map(|keys| {
                keys.into_iter()
                    .filter_map(|(key, _)| key_codec::parse_uuid_from_index_key(&key))
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

    fn raw_table_scan_range(
        &self,
        table: &str,
        start: Option<&str>,
        end: Option<&str>,
    ) -> Result<RawTableRows, StorageError> {
        (**self).raw_table_scan_range(table, start, end)
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

    fn append_history_region_rows(
        &mut self,
        table: &str,
        rows: &[StoredRowVersion],
    ) -> Result<(), StorageError> {
        (**self).append_history_region_rows(table, rows)
    }

    fn upsert_visible_region_rows(
        &mut self,
        table: &str,
        rows: &[StoredRowVersion],
    ) -> Result<(), StorageError> {
        (**self).upsert_visible_region_rows(table, rows)
    }

    fn patch_row_region_rows_by_batch(
        &mut self,
        table: &str,
        batch_id: crate::row_regions::BatchId,
        state: Option<RowState>,
        confirmed_tier: Option<DurabilityTier>,
    ) -> Result<(), StorageError> {
        (**self).patch_row_region_rows_by_batch(table, batch_id, state, confirmed_tier)
    }

    fn scan_visible_region(
        &self,
        table: &str,
        branch: &str,
    ) -> Result<Vec<StoredRowVersion>, StorageError> {
        (**self).scan_visible_region(table, branch)
    }

    fn load_visible_region_row(
        &self,
        table: &str,
        branch: &str,
        row_id: ObjectId,
    ) -> Result<Option<StoredRowVersion>, StorageError> {
        (**self).load_visible_region_row(table, branch, row_id)
    }

    fn scan_visible_region_row_versions(
        &self,
        table: &str,
        row_id: ObjectId,
    ) -> Result<Vec<StoredRowVersion>, StorageError> {
        (**self).scan_visible_region_row_versions(table, row_id)
    }

    fn scan_history_row_versions(
        &self,
        table: &str,
        row_id: ObjectId,
    ) -> Result<Vec<StoredRowVersion>, StorageError> {
        (**self).scan_history_row_versions(table, row_id)
    }

    fn scan_history_region(
        &self,
        table: &str,
        branch: &str,
        scan: HistoryScan,
    ) -> Result<Vec<StoredRowVersion>, StorageError> {
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
struct TableRowRegions {
    visible: BTreeMap<(String, ObjectId), StoredRowVersion>,
    history: BTreeMap<(String, ObjectId, u64), StoredRowVersion>,
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
    /// Row-region storage keyed by table.
    row_regions: HashMap<String, TableRowRegions>,
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

    fn append_history_region_rows(
        &mut self,
        table: &str,
        rows: &[StoredRowVersion],
    ) -> Result<(), StorageError> {
        let regions = self.row_regions.entry(table.to_string()).or_default();
        for row in rows {
            regions.history.insert(
                (row.branch.clone(), row.row_id, row.updated_at),
                row.clone(),
            );
        }
        Ok(())
    }

    fn upsert_visible_region_rows(
        &mut self,
        table: &str,
        rows: &[StoredRowVersion],
    ) -> Result<(), StorageError> {
        let regions = self.row_regions.entry(table.to_string()).or_default();
        for row in rows {
            regions
                .visible
                .insert((row.branch.clone(), row.row_id), row.clone());
        }
        Ok(())
    }

    fn patch_row_region_rows_by_batch(
        &mut self,
        table: &str,
        batch_id: crate::row_regions::BatchId,
        state: Option<RowState>,
        confirmed_tier: Option<DurabilityTier>,
    ) -> Result<(), StorageError> {
        let Some(regions) = self.row_regions.get_mut(table) else {
            return Ok(());
        };

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
            }
        }

        for row in regions.visible.values_mut() {
            if row.batch_id == batch_id {
                if let Some(state) = state {
                    row.state = state;
                }
                row.confirmed_tier = match (row.confirmed_tier, confirmed_tier) {
                    (Some(existing), Some(incoming)) => Some(existing.max(incoming)),
                    (Some(existing), None) => Some(existing),
                    (None, incoming) => incoming,
                };
            }
        }

        Ok(())
    }

    fn scan_visible_region(
        &self,
        table: &str,
        branch: &str,
    ) -> Result<Vec<StoredRowVersion>, StorageError> {
        let Some(regions) = self.row_regions.get(table) else {
            return Ok(Vec::new());
        };

        Ok(regions
            .visible
            .iter()
            .filter(|((row_branch, _), _)| row_branch == branch)
            .map(|(_, row)| row.clone())
            .collect())
    }

    fn load_visible_region_row(
        &self,
        table: &str,
        branch: &str,
        row_id: ObjectId,
    ) -> Result<Option<StoredRowVersion>, StorageError> {
        Ok(self
            .row_regions
            .get(table)
            .and_then(|regions| regions.visible.get(&(branch.to_string(), row_id)).cloned()))
    }

    fn scan_visible_region_row_versions(
        &self,
        table: &str,
        row_id: ObjectId,
    ) -> Result<Vec<StoredRowVersion>, StorageError> {
        let Some(regions) = self.row_regions.get(table) else {
            return Ok(Vec::new());
        };

        let mut rows: Vec<_> = regions
            .visible
            .iter()
            .filter(|((_, visible_row_id), _)| *visible_row_id == row_id)
            .map(|(_, row)| row.clone())
            .collect();
        rows.sort_by_key(|row| row.branch.clone());
        Ok(rows)
    }

    fn scan_history_row_versions(
        &self,
        table: &str,
        row_id: ObjectId,
    ) -> Result<Vec<StoredRowVersion>, StorageError> {
        let Some(regions) = self.row_regions.get(table) else {
            return Ok(Vec::new());
        };

        let mut rows: Vec<_> = regions
            .history
            .iter()
            .filter(|((_, history_row_id, _), _)| *history_row_id == row_id)
            .map(|(_, row)| row.clone())
            .collect();
        rows.sort_by_key(|row| (row.branch.clone(), row.updated_at));
        Ok(rows)
    }

    fn scan_history_region(
        &self,
        table: &str,
        branch: &str,
        scan: HistoryScan,
    ) -> Result<Vec<StoredRowVersion>, StorageError> {
        let Some(regions) = self.row_regions.get(table) else {
            return Ok(Vec::new());
        };

        let mut rows: Vec<StoredRowVersion> = match scan {
            HistoryScan::Branch => regions
                .history
                .iter()
                .filter(|((row_branch, _, _), _)| row_branch == branch)
                .map(|(_, row)| row.clone())
                .collect(),
            HistoryScan::Row { row_id } => regions
                .history
                .iter()
                .filter(|((row_branch, history_row_id, _), _)| {
                    row_branch == branch && *history_row_id == row_id
                })
                .map(|(_, row)| row.clone())
                .collect(),
            HistoryScan::AsOf { ts } => {
                let mut latest_per_row: BTreeMap<ObjectId, StoredRowVersion> = BTreeMap::new();
                for ((row_branch, row_id, updated_at), row) in &regions.history {
                    if row_branch != branch || *updated_at > ts || !row.state.is_visible() {
                        continue;
                    }
                    latest_per_row.insert(*row_id, row.clone());
                }
                latest_per_row.into_values().collect()
            }
        };

        rows.sort_by_key(|row| (row.branch.clone(), row.row_id, row.updated_at));
        Ok(rows)
    }
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;

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
    fn memory_storage_row_regions_visible_and_history_round_trip() {
        use crate::row_regions::{BatchId, HistoryScan, RowState, StoredRowVersion};

        let mut storage = MemoryStorage::new();
        let row_id = ObjectId::new();
        let batch_id = BatchId::new();

        let version = StoredRowVersion {
            row_id,
            branch: "dev/main".to_string(),
            parents: Vec::new(),
            updated_at: 10,
            created_by: "alice".to_string(),
            created_at: 10,
            updated_by: "alice".to_string(),
            batch_id,
            state: RowState::VisibleDirect,
            confirmed_tier: Some(DurabilityTier::Worker),
            is_deleted: false,
            data: b"alice".to_vec(),
            metadata: HashMap::new(),
        };

        storage
            .append_history_region_rows("users", &[version.clone()])
            .unwrap();
        storage
            .upsert_visible_region_rows("users", &[version.clone()])
            .unwrap();

        let visible = storage.scan_visible_region("users", "dev/main").unwrap();
        let history_by_row = storage.scan_history_row_versions("users", row_id).unwrap();
        let history = storage
            .scan_history_region("users", "dev/main", HistoryScan::Row { row_id })
            .unwrap();

        assert_eq!(visible, vec![version.clone()]);
        assert_eq!(history_by_row, vec![version.clone()]);
        assert_eq!(history, vec![version]);
    }

    mod memory_conformance {
        use crate::storage::MemoryStorage;
        use crate::storage::Storage;

        crate::storage_conformance_tests!(memory, || {
            Box::new(MemoryStorage::new()) as Box<dyn Storage>
        });
    }
}
