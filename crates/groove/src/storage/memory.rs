//! In-memory implementation of the ordered key/value storage trait.

use std::cell::RefCell;
use std::collections::BTreeMap;
use std::rc::Rc;

use serde::{Deserialize, Serialize};

use super::{
    ColumnFamilyName, Error, Key, OrderedKvStorage, ReopenableStorage, ScanVisitor, Value,
    WriteOperation,
};

const MEMORY_STORAGE_SNAPSHOT_VERSION: u16 = 1;

type ColumnFamilies = BTreeMap<String, BTreeMap<Vec<u8>, Vec<u8>>>;
type SharedColumnFamilies = Rc<RefCell<ColumnFamilies>>;

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
        let mut inner = self.inner.borrow_mut();
        for cf in column_families {
            inner.entry((*cf).to_owned()).or_default();
        }
    }

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

    fn write_many(&self, operations: &[WriteOperation<'_>]) -> Result<(), Error> {
        {
            let inner = self.inner.borrow();
            for operation in operations {
                let cf = match operation {
                    WriteOperation::Set { cf, .. } | WriteOperation::Delete { cf, .. } => *cf,
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
}
