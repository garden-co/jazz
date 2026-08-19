//! In-memory implementation of the ordered key/value storage trait.

use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};

use serde::{Deserialize, Serialize};

use super::{
    ColumnFamilyName, Error, OrderedKvStorage, OwnedWriteOperation, ReadyStorageCursor,
    ReopenableStorage, StorageFuture, StorageScan, Value, apply_storage_delta, key_codec,
};

const MEMORY_STORAGE_SNAPSHOT_VERSION: u16 = 1;

type ColumnFamilies = BTreeMap<String, BTreeMap<Vec<u8>, Vec<u8>>>;
type SharedColumnFamilies = Arc<Mutex<ColumnFamilies>>;

#[derive(Debug, thiserror::Error)]
pub enum MemoryStorageSnapshotError {
    #[error("failed to encode memory storage snapshot: {0}")]
    Encode(postcard::Error),
    #[error("failed to decode memory storage snapshot: {0}")]
    Decode(postcard::Error),
    #[error("unsupported memory storage snapshot version {found}; expected {expected}")]
    UnsupportedVersion { found: u16, expected: u16 },
}

#[derive(Serialize, Deserialize)]
struct MemoryStorageSnapshot {
    version: u16,
    column_families: ColumnFamilies,
}

/// Ordered in-memory storage for tests, examples, benches, and wasm probes.
///
/// The store follows the same column-family contract as `RocksDbStorage`: reads
/// and writes to unknown families return [`Error::ColumnFamilyNotFound`], while
/// [`ReopenableStorage::reopen`] creates any newly requested families.
#[derive(Clone, Default)]
pub struct MemoryStorage {
    inner: SharedColumnFamilies,
}

impl MemoryStorage {
    /// Construct storage with the supplied column families.
    pub fn new(column_families: &[&str]) -> Self {
        let storage = Self::default();
        storage.ensure_column_families(column_families);
        storage
    }

    fn ensure_column_families(&self, column_families: &[&str]) {
        let mut inner = self.inner.lock().expect("memory storage mutex poisoned");
        for cf in column_families {
            inner.entry((*cf).to_owned()).or_default();
        }
    }

    fn with_cf<T>(
        &self,
        cf: &ColumnFamilyName,
        f: impl FnOnce(&BTreeMap<Vec<u8>, Vec<u8>>) -> T,
    ) -> Result<T, Error> {
        let inner = self.inner.lock().expect("memory storage mutex poisoned");
        let values = inner
            .get(cf)
            .ok_or_else(|| Error::ColumnFamilyNotFound(cf.to_owned()))?;
        Ok(f(values))
    }

    /// Export the full in-memory contents as compact versioned bytes.
    pub fn export_snapshot(&self) -> Result<Vec<u8>, MemoryStorageSnapshotError> {
        let snapshot = MemoryStorageSnapshot {
            version: MEMORY_STORAGE_SNAPSHOT_VERSION,
            column_families: self
                .inner
                .lock()
                .expect("memory storage mutex poisoned")
                .clone(),
        };
        postcard::to_allocvec(&snapshot).map_err(MemoryStorageSnapshotError::Encode)
    }

    /// Replace the full in-memory contents from versioned snapshot bytes.
    pub fn import_snapshot(&self, bytes: &[u8]) -> Result<(), MemoryStorageSnapshotError> {
        let snapshot: MemoryStorageSnapshot =
            postcard::from_bytes(bytes).map_err(MemoryStorageSnapshotError::Decode)?;
        if snapshot.version != MEMORY_STORAGE_SNAPSHOT_VERSION {
            return Err(MemoryStorageSnapshotError::UnsupportedVersion {
                found: snapshot.version,
                expected: MEMORY_STORAGE_SNAPSHOT_VERSION,
            });
        }
        *self.inner.lock().expect("memory storage mutex poisoned") = snapshot.column_families;
        Ok(())
    }
}

impl OrderedKvStorage for MemoryStorage {
    fn get(&self, cf: String, key: Vec<u8>) -> StorageFuture<'_, Result<Option<Value>, Error>> {
        Box::pin(async move { self.with_cf(&cf, |values| values.get(&key).cloned()) })
    }

    fn approximate_class_bytes(&self, cf: String) -> StorageFuture<'_, Result<Option<u64>, Error>> {
        Box::pin(async move {
            self.with_cf(&cf, |values| {
                values
                    .iter()
                    .map(|(key, value)| key.len().saturating_add(value.len()) as u64)
                    .sum::<u64>()
            })
            .map(Some)
        })
    }

    fn set(
        &self,
        cf: String,
        key: Vec<u8>,
        value: Vec<u8>,
    ) -> StorageFuture<'_, Result<(), Error>> {
        Box::pin(async move {
            let mut inner = self.inner.lock().expect("memory storage mutex poisoned");
            let values = inner.get_mut(&cf).ok_or(Error::ColumnFamilyNotFound(cf))?;
            values.insert(key, value);
            Ok(())
        })
    }

    fn delete(&self, cf: String, key: Vec<u8>) -> StorageFuture<'_, Result<(), Error>> {
        Box::pin(async move {
            let mut inner = self.inner.lock().expect("memory storage mutex poisoned");
            let values = inner.get_mut(&cf).ok_or(Error::ColumnFamilyNotFound(cf))?;
            values.remove(&key);
            Ok(())
        })
    }

    fn scan_range(
        &self,
        cf: String,
        start: Vec<u8>,
        end: Vec<u8>,
    ) -> StorageFuture<'_, Result<StorageScan<'_>, Error>> {
        Box::pin(async move {
            let values = self.with_cf(&cf, |values| {
                values
                    .range(start..end)
                    .map(|(key, value)| (key.clone(), value.clone()))
                    .collect()
            })?;
            Ok(Box::new(ReadyStorageCursor::new(values)) as StorageScan<'_>)
        })
    }

    fn scan_prefix(
        &self,
        cf: String,
        prefix: Vec<u8>,
    ) -> StorageFuture<'_, Result<StorageScan<'_>, Error>> {
        Box::pin(async move {
            let values = self.with_cf(&cf, |values| {
                values
                    .range(prefix.clone()..)
                    .take_while(|(key, _)| key.starts_with(&prefix))
                    .map(|(key, value)| (key.clone(), value.clone()))
                    .collect()
            })?;
            Ok(Box::new(ReadyStorageCursor::new(values)) as StorageScan<'_>)
        })
    }

    fn last_with_prefix(
        &self,
        cf: String,
        prefix: Vec<u8>,
    ) -> StorageFuture<'_, Result<Option<super::KeyValue>, Error>> {
        Box::pin(async move {
            self.with_cf(&cf, |values| {
                if let Some(upper) = key_codec::prefix_upper_bound(&prefix) {
                    values
                        .range(prefix..upper)
                        .next_back()
                        .map(|(key, value)| (key.clone(), value.clone()))
                } else {
                    values
                        .range(prefix..)
                        .next_back()
                        .map(|(key, value)| (key.clone(), value.clone()))
                }
            })
        })
    }

    fn last_with_prefix_before_or_at(
        &self,
        cf: String,
        prefix: Vec<u8>,
        upper: Vec<u8>,
    ) -> StorageFuture<'_, Result<Option<super::KeyValue>, Error>> {
        Box::pin(async move {
            self.with_cf(&cf, |values| {
                values
                    .range(prefix.clone()..=upper)
                    .rev()
                    .find(|(key, _)| key.starts_with(&prefix))
                    .map(|(key, value)| (key.clone(), value.clone()))
            })
        })
    }

    fn write_many(
        &self,
        operations: Vec<OwnedWriteOperation>,
    ) -> StorageFuture<'_, Result<(), Error>> {
        Box::pin(async move {
            let mut inner = self.inner.lock().expect("memory storage mutex poisoned");
            // Validate and evaluate against a per-key prospective overlay
            // before changing the store. This preserves batch atomicity and
            // read-your-prior-operation semantics without cloning each table.
            let mut planned = BTreeMap::<(String, Vec<u8>), Option<Vec<u8>>>::new();
            for operation in operations {
                match operation {
                    OwnedWriteOperation::Set { cf, key, value } => {
                        if !inner.contains_key(&cf) {
                            return Err(Error::ColumnFamilyNotFound(cf));
                        }
                        planned.insert((cf, key), Some(value));
                    }
                    OwnedWriteOperation::Delete { cf, key } => {
                        if !inner.contains_key(&cf) {
                            return Err(Error::ColumnFamilyNotFound(cf));
                        }
                        planned.insert((cf, key), None);
                    }
                    OwnedWriteOperation::Delta { cf, key, delta } => {
                        let Some(values) = inner.get(&cf) else {
                            return Err(Error::ColumnFamilyNotFound(cf));
                        };
                        let planned_key = (cf, key);
                        let encoded = delta.encode()?;
                        let existing = match planned.get(&planned_key) {
                            Some(Some(value)) => Some(value.as_slice()),
                            Some(None) => None,
                            None => values.get(&planned_key.1).map(Vec::as_slice),
                        };
                        let merged = apply_storage_delta(existing, &encoded)?;
                        planned.insert(planned_key, Some(merged));
                    }
                }
            }
            for ((cf, key), value) in planned {
                let values = inner.get_mut(&cf).expect("column family was validated");
                match value {
                    Some(value) => {
                        values.insert(key, value);
                    }
                    None => {
                        values.remove(&key);
                    }
                }
            }
            Ok(())
        })
    }

    fn column_family_names(&self) -> Option<Vec<String>> {
        Some(
            self.inner
                .lock()
                .expect("memory storage mutex poisoned")
                .keys()
                .cloned()
                .collect(),
        )
    }
}

impl ReopenableStorage for MemoryStorage {
    fn reopen(self, column_families: Vec<String>) -> StorageFuture<'static, Result<Self, Error>> {
        Box::pin(async move {
            let column_families = column_families
                .iter()
                .map(String::as_str)
                .collect::<Vec<_>>();
            self.ensure_column_families(&column_families);
            Ok(self)
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn snapshot_round_trip_preserves_column_families_and_values() {
        let storage = MemoryStorage::new(&["rows", "meta"]);
        storage.set("rows", b"a", b"one").unwrap();
        storage.set("rows", b"b", b"two").unwrap();
        storage.set("meta", b"schema", b"v1").unwrap();

        let snapshot = storage.export_snapshot().unwrap();
        let restored = MemoryStorage::default();
        restored.import_snapshot(&snapshot).unwrap();

        assert_eq!(restored.get("rows", b"a").unwrap(), Some(b"one".to_vec()));
        assert_eq!(restored.get("rows", b"b").unwrap(), Some(b"two".to_vec()));
        assert_eq!(
            restored.get("meta", b"schema").unwrap(),
            Some(b"v1".to_vec())
        );
    }

    #[test]
    fn import_snapshot_replaces_existing_contents() {
        let source = MemoryStorage::new(&["rows"]);
        source.set("rows", b"a", b"one").unwrap();
        let snapshot = source.export_snapshot().unwrap();

        let target = MemoryStorage::new(&["other"]);
        target.set("other", b"stale", b"value").unwrap();
        target.import_snapshot(&snapshot).unwrap();

        assert_eq!(target.get("rows", b"a").unwrap(), Some(b"one".to_vec()));
        assert!(matches!(
            target.get("other", b"stale"),
            Err(Error::ColumnFamilyNotFound(_))
        ));
    }

    #[test]
    fn approximate_class_bytes_sums_keys_and_values_exactly() {
        let storage = MemoryStorage::new(&["rows"]);
        storage.set("rows", b"a", b"one").unwrap();
        storage.set("rows", b"bb", b"two").unwrap();

        assert_eq!(storage.approximate_class_bytes("rows").unwrap(), Some(9));
    }
}
