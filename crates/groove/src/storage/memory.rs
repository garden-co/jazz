//! In-memory implementation of the ordered key/value storage trait.

use std::cell::RefCell;
use std::collections::BTreeMap;
use std::rc::Rc;

use serde::{Deserialize, Serialize};

use super::{
    ColumnFamilyName, Error, Key, OrderedKvStorage, ReopenableStorage, ScanVisitor, Value,
    WriteOperation, apply_storage_delta, key_codec,
};

/// On-disk format version for exported snapshots.
const MEMORY_STORAGE_SNAPSHOT_VERSION: u16 = 1;

/// The whole store: each column family is an ordered key → value map.
type ColumnFamilies = BTreeMap<String, BTreeMap<Vec<u8>, Vec<u8>>>;
/// Shared, interior-mutable handle so clones of [`MemoryStorage`] share one
/// backing store.
type SharedColumnFamilies = Rc<RefCell<ColumnFamilies>>;

/// Failures when exporting or importing a [`MemoryStorage`] snapshot.
#[derive(Debug, thiserror::Error)]
pub enum MemoryStorageSnapshotError {
    /// Serialization failed.
    #[error("failed to encode memory storage snapshot: {0}")]
    Encode(postcard::Error),
    /// Deserialization failed.
    #[error("failed to decode memory storage snapshot: {0}")]
    Decode(postcard::Error),
    /// The snapshot's version byte is not one this build understands.
    #[error("unsupported memory storage snapshot version {found}; expected {expected}")]
    UnsupportedVersion { found: u16, expected: u16 },
}

/// The serialized form of a whole store: a version tag plus its column
/// families.
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

    /// Creates any of the named column families that do not exist yet.
    fn ensure_column_families(&self, column_families: &[&str]) {
        let mut inner = self.inner.borrow_mut();
        for cf in column_families {
            inner.entry((*cf).to_owned()).or_default();
        }
    }

    /// Runs `f` over one column family's map, or fails with
    /// [`Error::ColumnFamilyNotFound`] — the shared read path for the
    /// scan/get methods.
    fn with_cf<T>(
        &self,
        cf: &ColumnFamilyName,
        f: impl FnOnce(&BTreeMap<Vec<u8>, Vec<u8>>) -> T,
    ) -> Result<T, Error> {
        let inner = self.inner.borrow();
        let values = inner
            .get(cf)
            .ok_or_else(|| Error::ColumnFamilyNotFound(cf.to_owned()))?;
        Ok(f(values))
    }

    /// Export the full in-memory contents as compact versioned bytes.
    pub fn export_snapshot(&self) -> Result<Vec<u8>, MemoryStorageSnapshotError> {
        let snapshot = MemoryStorageSnapshot {
            version: MEMORY_STORAGE_SNAPSHOT_VERSION,
            column_families: self.inner.borrow().clone(),
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
        *self.inner.borrow_mut() = snapshot.column_families;
        Ok(())
    }
}

impl OrderedKvStorage for MemoryStorage {
    fn get(&self, cf: &ColumnFamilyName, key: &Key) -> Result<Option<Value>, Error> {
        self.with_cf(cf, |values| values.get(key).cloned())
    }

    fn approximate_class_bytes(&self, cf: &ColumnFamilyName) -> Result<Option<u64>, Error> {
        self.with_cf(cf, |values| {
            values
                .iter()
                .map(|(key, value)| key.len().saturating_add(value.len()) as u64)
                .sum::<u64>()
        })
        .map(Some)
    }

    fn set(&self, cf: &ColumnFamilyName, key: &Key, value: &[u8]) -> Result<(), Error> {
        let mut inner = self.inner.borrow_mut();
        let values = inner
            .get_mut(cf)
            .ok_or_else(|| Error::ColumnFamilyNotFound(cf.to_owned()))?;
        values.insert(key.to_vec(), value.to_vec());
        Ok(())
    }

    fn delete(&self, cf: &ColumnFamilyName, key: &Key) -> Result<(), Error> {
        let mut inner = self.inner.borrow_mut();
        let values = inner
            .get_mut(cf)
            .ok_or_else(|| Error::ColumnFamilyNotFound(cf.to_owned()))?;
        values.remove(key);
        Ok(())
    }

    fn scan_range(
        &self,
        cf: &ColumnFamilyName,
        start: &Key,
        end: &Key,
        visit: &mut ScanVisitor<'_>,
    ) -> Result<(), Error> {
        let values = self.with_cf(cf, |values| {
            values
                .range(start.to_vec()..end.to_vec())
                .map(|(key, value)| (key.clone(), value.clone()))
                .collect::<Vec<_>>()
        })?;
        for (key, value) in values {
            visit(&key, &value)?;
        }
        Ok(())
    }

    fn scan_prefix(
        &self,
        cf: &ColumnFamilyName,
        prefix: &Key,
        visit: &mut ScanVisitor<'_>,
    ) -> Result<(), Error> {
        let values = self.with_cf(cf, |values| {
            values
                .range(prefix.to_vec()..)
                .take_while(|(key, _)| key.starts_with(prefix))
                .map(|(key, value)| (key.clone(), value.clone()))
                .collect::<Vec<_>>()
        })?;
        for (key, value) in values {
            visit(&key, &value)?;
        }
        Ok(())
    }

    fn last_with_prefix(
        &self,
        cf: &ColumnFamilyName,
        prefix: &Key,
    ) -> Result<Option<super::KeyValue>, Error> {
        self.with_cf(cf, |values| {
            if let Some(upper) = key_codec::prefix_upper_bound(prefix) {
                values
                    .range(prefix.to_vec()..upper)
                    .next_back()
                    .map(|(key, value)| (key.clone(), value.clone()))
            } else {
                values
                    .range(prefix.to_vec()..)
                    .next_back()
                    .map(|(key, value)| (key.clone(), value.clone()))
            }
        })
    }

    fn last_with_prefix_before_or_at(
        &self,
        cf: &ColumnFamilyName,
        prefix: &Key,
        upper: &Key,
    ) -> Result<Option<super::KeyValue>, Error> {
        self.with_cf(cf, |values| {
            values
                .range(prefix.to_vec()..=upper.to_vec())
                .rev()
                .find(|(key, _)| key.starts_with(prefix))
                .map(|(key, value)| (key.clone(), value.clone()))
        })
    }

    fn write_many(&self, operations: &[WriteOperation<'_>]) -> Result<(), Error> {
        {
            let inner = self.inner.borrow();
            for operation in operations {
                let cf = match operation {
                    WriteOperation::Set { cf, .. }
                    | WriteOperation::Delete { cf, .. }
                    | WriteOperation::Delta { cf, .. } => *cf,
                };
                if !inner.contains_key(cf) {
                    return Err(Error::ColumnFamilyNotFound(cf.to_owned()));
                }
            }
        }

        let mut inner = self.inner.borrow_mut();
        for operation in operations {
            match operation {
                WriteOperation::Set { cf, key, value } => {
                    inner
                        .get_mut(*cf)
                        .expect("validated column family")
                        .insert((*key).to_vec(), (*value).to_vec());
                }
                WriteOperation::Delete { cf, key } => {
                    inner
                        .get_mut(*cf)
                        .expect("validated column family")
                        .remove(*key);
                }
                WriteOperation::Delta { cf, key, delta } => {
                    let values = inner.get_mut(*cf).expect("validated column family");
                    let encoded = delta.encode()?;
                    let merged =
                        apply_storage_delta(values.get(*key).map(Vec::as_slice), &encoded)?;
                    values.insert((*key).to_vec(), merged);
                }
            }
        }
        Ok(())
    }

    fn column_family_names(&self) -> Option<Vec<String>> {
        Some(self.inner.borrow().keys().cloned().collect())
    }
}

impl ReopenableStorage for MemoryStorage {
    fn reopen(self, column_families: &[&str]) -> Result<Self, Error> {
        self.ensure_column_families(column_families);
        Ok(self)
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
