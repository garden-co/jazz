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

use std::collections::{BTreeMap, HashMap, HashSet};
use std::ops::Bound;

use serde::{Deserialize, Serialize};
use smolset::SmolSet;

use crate::catalogue::CatalogueEntry;
use crate::commit::CommitId;
use crate::metadata::MetadataKey;
use crate::object::ObjectId;
use crate::query_manager::types::{SchemaHash, SharedString, Value};
use crate::row_regions::{
    HistoryScan, QueryRowVersion, RowState, StoredRowVersion, VisibleRowEntry,
};
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
pub type RowLocatorRows = Vec<(ObjectId, RowLocator)>;
pub type RawTableRows = Vec<(String, Vec<u8>)>;
pub type RawTableKeys = Vec<String>;

const METADATA_TABLE: &str = "__metadata";
const ROW_LOCATOR_TABLE: &str = "__row_locator";

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RowLocator {
    pub table: SharedString,
    pub origin_schema_hash: Option<SchemaHash>,
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
        _entries: &[VisibleRowEntry],
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

    fn apply_row_mutation(
        &mut self,
        table: &str,
        history_rows: &[StoredRowVersion],
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

    fn load_visible_query_row(
        &self,
        table: &str,
        branch: &str,
        row_id: ObjectId,
    ) -> Result<Option<QueryRowVersion>, StorageError> {
        Ok(self
            .load_visible_region_row(table, branch, row_id)?
            .as_ref()
            .map(QueryRowVersion::from))
    }

    fn load_visible_region_row_for_tier(
        &self,
        table: &str,
        branch: &str,
        row_id: ObjectId,
        required_tier: DurabilityTier,
    ) -> Result<Option<StoredRowVersion>, StorageError> {
        let Some(entry) = self.load_visible_region_entry(table, branch, row_id)? else {
            return Ok(None);
        };

        let Some(version_id) = entry.version_id_for_tier(required_tier) else {
            return Ok(None);
        };
        if version_id == entry.current_version_id() {
            return Ok(Some(entry.current_row));
        }

        self.load_history_row_version(table, row_id, version_id)
    }

    fn load_visible_query_row_for_tier(
        &self,
        table: &str,
        branch: &str,
        row_id: ObjectId,
        required_tier: DurabilityTier,
    ) -> Result<Option<QueryRowVersion>, StorageError> {
        Ok(self
            .load_visible_region_row_for_tier(table, branch, row_id, required_tier)?
            .as_ref()
            .map(QueryRowVersion::from))
    }

    fn load_visible_region_entry(
        &self,
        table: &str,
        branch: &str,
        row_id: ObjectId,
    ) -> Result<Option<VisibleRowEntry>, StorageError> {
        let Some(current_row) = self.load_visible_region_row(table, branch, row_id)? else {
            return Ok(None);
        };

        let history_rows = self
            .scan_history_row_versions(table, row_id)?
            .into_iter()
            .filter(|row| row.branch == branch)
            .collect::<Vec<_>>();

        Ok(Some(VisibleRowEntry::rebuild(current_row, &history_rows)))
    }

    fn load_visible_region_frontier(
        &self,
        table: &str,
        branch: &str,
        row_id: ObjectId,
    ) -> Result<Option<Vec<CommitId>>, StorageError> {
        Ok(self
            .load_visible_region_entry(table, branch, row_id)?
            .map(|entry| entry.branch_frontier))
    }

    fn load_history_row_version(
        &self,
        table: &str,
        row_id: ObjectId,
        version_id: CommitId,
    ) -> Result<Option<StoredRowVersion>, StorageError> {
        Ok(self
            .scan_history_row_versions(table, row_id)?
            .into_iter()
            .find(|row| row.version_id() == version_id))
    }

    fn load_history_query_row_version(
        &self,
        table: &str,
        row_id: ObjectId,
        version_id: CommitId,
    ) -> Result<Option<QueryRowVersion>, StorageError> {
        Ok(self
            .load_history_row_version(table, row_id, version_id)?
            .as_ref()
            .map(QueryRowVersion::from))
    }

    fn row_version_exists(
        &self,
        table: &str,
        branch: &str,
        row_id: ObjectId,
        version_id: CommitId,
    ) -> Result<bool, StorageError> {
        Ok(self
            .scan_history_row_versions(table, row_id)?
            .into_iter()
            .any(|row| row.branch == branch && row.version_id() == version_id))
    }

    fn scan_row_branch_tip_ids(
        &self,
        table: &str,
        branch: &str,
        row_id: ObjectId,
    ) -> Result<Vec<CommitId>, StorageError> {
        if let Some(frontier) = self.load_visible_region_frontier(table, branch, row_id)? {
            return Ok(frontier);
        }

        let branch_rows = self
            .scan_history_row_versions(table, row_id)?
            .into_iter()
            .filter(|row| row.branch == branch)
            .collect::<Vec<_>>();

        let mut non_tips = SmolSet::<[CommitId; 2]>::new();
        for row in &branch_rows {
            for parent in &row.parents {
                non_tips.insert(*parent);
            }
        }

        let mut tips: Vec<_> = branch_rows
            .into_iter()
            .map(|row| row.version_id())
            .filter(|version_id| !non_tips.contains(version_id))
            .collect();
        tips.sort();
        tips.dedup();
        Ok(tips)
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
        entries: &[VisibleRowEntry],
    ) -> Result<(), StorageError> {
        (**self).upsert_visible_region_rows(table, entries)
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

    fn apply_row_mutation(
        &mut self,
        table: &str,
        history_rows: &[StoredRowVersion],
        visible_entries: &[VisibleRowEntry],
        index_mutations: &[IndexMutation<'_>],
    ) -> Result<(), StorageError> {
        (**self).apply_row_mutation(table, history_rows, visible_entries, index_mutations)
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

    fn load_visible_query_row(
        &self,
        table: &str,
        branch: &str,
        row_id: ObjectId,
    ) -> Result<Option<QueryRowVersion>, StorageError> {
        (**self).load_visible_query_row(table, branch, row_id)
    }

    fn load_visible_region_row_for_tier(
        &self,
        table: &str,
        branch: &str,
        row_id: ObjectId,
        required_tier: DurabilityTier,
    ) -> Result<Option<StoredRowVersion>, StorageError> {
        (**self).load_visible_region_row_for_tier(table, branch, row_id, required_tier)
    }

    fn load_visible_query_row_for_tier(
        &self,
        table: &str,
        branch: &str,
        row_id: ObjectId,
        required_tier: DurabilityTier,
    ) -> Result<Option<QueryRowVersion>, StorageError> {
        (**self).load_visible_query_row_for_tier(table, branch, row_id, required_tier)
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

    fn load_history_query_row_version(
        &self,
        table: &str,
        row_id: ObjectId,
        version_id: CommitId,
    ) -> Result<Option<QueryRowVersion>, StorageError> {
        (**self).load_history_query_row_version(table, row_id, version_id)
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
struct TableRowRegions {
    visible: BTreeMap<SharedString, BTreeMap<ObjectId, VisibleRowEntry>>,
    history: BTreeMap<(ObjectId, CommitId), StoredRowVersion>,
}

impl TableRowRegions {
    fn history_rows_for(&self, branch: &str, row_id: ObjectId) -> Vec<StoredRowVersion> {
        let mut rows: Vec<_> = self
            .history
            .iter()
            .filter(|((history_row_id, _), row)| *history_row_id == row_id && row.branch == branch)
            .map(|(_, row)| row.clone())
            .collect();
        rows.sort_by_key(|row| (row.updated_at, row.version_id()));
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
            .any(|row| row.version_id() == current_row.version_id())
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
        rows: &[StoredRowVersion],
    ) -> Result<(), StorageError> {
        let regions = self.row_regions.entry(table.to_string()).or_default();
        for row in rows {
            regions
                .history
                .insert((row.row_id, row.version_id()), row.clone());
        }
        Ok(())
    }

    fn upsert_visible_region_rows(
        &mut self,
        table: &str,
        entries: &[VisibleRowEntry],
    ) -> Result<(), StorageError> {
        let regions = self.row_regions.entry(table.to_string()).or_default();
        for entry in entries {
            regions
                .visible
                .entry(entry.current_row.branch.clone())
                .or_default()
                .insert(entry.current_row.row_id, entry.clone());
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
    ) -> Result<Vec<StoredRowVersion>, StorageError> {
        let Some(regions) = self.row_regions.get(table) else {
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
    ) -> Result<Option<StoredRowVersion>, StorageError> {
        Ok(self.row_regions.get(table).and_then(|regions| {
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
    ) -> Result<Option<QueryRowVersion>, StorageError> {
        Ok(self.row_regions.get(table).and_then(|regions| {
            regions
                .visible
                .get(branch)
                .and_then(|rows| rows.get(&row_id))
                .map(|entry| QueryRowVersion::from(&entry.current_row))
        }))
    }

    fn load_visible_region_entry(
        &self,
        table: &str,
        branch: &str,
        row_id: ObjectId,
    ) -> Result<Option<VisibleRowEntry>, StorageError> {
        Ok(self
            .row_regions
            .get(table)
            .and_then(|regions| regions.visible.get(branch))
            .and_then(|rows| rows.get(&row_id).cloned()))
    }

    fn load_visible_region_frontier(
        &self,
        table: &str,
        branch: &str,
        row_id: ObjectId,
    ) -> Result<Option<Vec<CommitId>>, StorageError> {
        Ok(self.row_regions.get(table).and_then(|regions| {
            regions
                .visible
                .get(branch)
                .and_then(|rows| rows.get(&row_id))
                .map(|entry| entry.branch_frontier.clone())
        }))
    }

    fn load_visible_region_row_for_tier(
        &self,
        table: &str,
        branch: &str,
        row_id: ObjectId,
        required_tier: DurabilityTier,
    ) -> Result<Option<StoredRowVersion>, StorageError> {
        let Some(regions) = self.row_regions.get(table) else {
            return Ok(None);
        };
        let Some(entry) = regions
            .visible
            .get(branch)
            .and_then(|rows| rows.get(&row_id))
        else {
            return Ok(None);
        };

        let Some(version_id) = entry.version_id_for_tier(required_tier) else {
            return Ok(None);
        };
        if version_id == entry.current_version_id() {
            return Ok(Some(entry.current_row.clone()));
        }

        Ok(regions
            .history_rows_for(branch, row_id)
            .into_iter()
            .find(|row| row.version_id() == version_id))
    }

    fn load_visible_query_row_for_tier(
        &self,
        table: &str,
        branch: &str,
        row_id: ObjectId,
        required_tier: DurabilityTier,
    ) -> Result<Option<QueryRowVersion>, StorageError> {
        let Some(regions) = self.row_regions.get(table) else {
            return Ok(None);
        };
        let Some(entry) = regions
            .visible
            .get(branch)
            .and_then(|rows| rows.get(&row_id))
        else {
            return Ok(None);
        };

        let Some(version_id) = entry.version_id_for_tier(required_tier) else {
            return Ok(None);
        };
        if version_id == entry.current_version_id() {
            return Ok(Some(QueryRowVersion::from(&entry.current_row)));
        }

        Ok(regions
            .history_rows_for(branch, row_id)
            .into_iter()
            .find(|row| row.version_id() == version_id)
            .map(|row| QueryRowVersion::from(&row)))
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
            .values()
            .filter_map(|branch_rows| branch_rows.get(&row_id))
            .map(|entry| entry.current_row.clone())
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
            .filter(|((history_row_id, _), _)| *history_row_id == row_id)
            .map(|(_, row)| row.clone())
            .collect();
        rows.sort_by_key(|row| (row.branch.clone(), row.updated_at, row.version_id()));
        Ok(rows)
    }

    fn load_history_row_version(
        &self,
        table: &str,
        row_id: ObjectId,
        version_id: CommitId,
    ) -> Result<Option<StoredRowVersion>, StorageError> {
        Ok(self
            .row_regions
            .get(table)
            .and_then(|regions| regions.history.get(&(row_id, version_id)).cloned()))
    }

    fn load_history_query_row_version(
        &self,
        table: &str,
        row_id: ObjectId,
        version_id: CommitId,
    ) -> Result<Option<QueryRowVersion>, StorageError> {
        Ok(self
            .row_regions
            .get(table)
            .and_then(|regions| regions.history.get(&(row_id, version_id)))
            .map(QueryRowVersion::from))
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
                .filter(|(_, row)| row.branch == branch)
                .map(|(_, row)| row.clone())
                .collect(),
            HistoryScan::Row { row_id } => regions
                .history
                .iter()
                .filter(|((history_row_id, _), row)| {
                    row.branch == branch && *history_row_id == row_id
                })
                .map(|(_, row)| row.clone())
                .collect(),
            HistoryScan::AsOf { ts } => {
                let mut latest_per_row: BTreeMap<ObjectId, StoredRowVersion> = BTreeMap::new();
                for ((row_id, _), row) in &regions.history {
                    if row.branch != branch || row.updated_at > ts || !row.state.is_visible() {
                        continue;
                    }
                    match latest_per_row.get(row_id) {
                        Some(existing)
                            if (existing.updated_at, existing.version_id())
                                >= (row.updated_at, row.version_id()) => {}
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
                row.version_id(),
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
        use crate::row_regions::{HistoryScan, RowState, StoredRowVersion, VisibleRowEntry};

        let mut storage = MemoryStorage::new();
        let row_id = ObjectId::new();
        let version = StoredRowVersion::new(
            row_id,
            "dev/main",
            Vec::new(),
            b"alice".to_vec(),
            crate::metadata::RowProvenance::for_insert("alice".to_string(), 10),
            HashMap::new(),
            RowState::VisibleDirect,
            Some(DurabilityTier::Worker),
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
        let history_by_row = storage.scan_history_row_versions("users", row_id).unwrap();
        let history = storage
            .scan_history_region("users", "dev/main", HistoryScan::Row { row_id })
            .unwrap();

        assert_eq!(visible, vec![version.clone()]);
        assert_eq!(history_by_row, vec![version.clone()]);
        assert_eq!(history, vec![version]);
    }

    #[test]
    fn memory_storage_visible_entries_track_older_tier_winners() {
        use crate::row_regions::{RowState, StoredRowVersion, VisibleRowEntry};

        let mut storage = MemoryStorage::new();
        let row_id = ObjectId::new();

        let globally_confirmed = StoredRowVersion::new(
            row_id,
            "dev/main",
            Vec::new(),
            b"v1".to_vec(),
            crate::metadata::RowProvenance::for_insert("alice".to_string(), 10),
            HashMap::new(),
            RowState::VisibleDirect,
            Some(DurabilityTier::GlobalServer),
        );
        let current_worker = StoredRowVersion::new(
            row_id,
            "dev/main",
            vec![globally_confirmed.version_id()],
            b"v2".to_vec(),
            crate::metadata::RowProvenance {
                created_by: "alice".to_string(),
                created_at: 10,
                updated_by: "alice".to_string(),
                updated_at: 20,
            },
            HashMap::new(),
            RowState::VisibleDirect,
            Some(DurabilityTier::Worker),
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
            .row_regions
            .get("users")
            .and_then(|regions| regions.visible.get("dev/main"))
            .and_then(|rows| rows.get(&row_id))
            .cloned()
            .expect("visible entry");

        assert_eq!(visible, Some(current_worker.clone()));
        assert_eq!(entry.current_row, current_worker);
        assert_eq!(entry.worker_version_id, None);
        assert_eq!(entry.edge_version_id, Some(globally_confirmed.version_id()));
        assert_eq!(
            entry.global_version_id,
            Some(globally_confirmed.version_id())
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
