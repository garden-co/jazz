//! In-memory implementation of the ordered key/value storage trait.

use std::collections::BTreeMap;
use std::ops::Bound;
#[cfg(test)]
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use serde::{Deserialize, Serialize};

use super::{
    ColumnFamilyName, Error, KeyValue, OrderedKvStorage, OwnedWriteOperation, ReopenableStorage,
    ScanBounds, ScanDirection, ScanRequest, StorageFuture, StorageScan, Value, WriteManyOutcome,
};

const MEMORY_STORAGE_SNAPSHOT_VERSION: u16 = 1;

type ColumnFamilies = BTreeMap<String, BTreeMap<Vec<u8>, Vec<u8>>>;
type SharedColumnFamilies = Arc<Mutex<ColumnFamilies>>;

/// The in-memory backend keeps only one cursor batch cloned at a time.  This
/// matters even for test storage: transaction overlays may need to read past
/// their logical limit for staged deletes, but still carry a finite physical
/// budget. A snapshotting memory scan would defeat that bounded-traversal
/// contract.
struct MemoryStorageCursor {
    storage: MemoryStorage,
    cf: String,
    bounds: ScanBounds,
    direction: ScanDirection,
    remaining: Option<usize>,
    last_key: Option<Vec<u8>>,
    done: bool,
}

impl MemoryStorageCursor {
    fn batch_limit(&self) -> Option<usize> {
        self.remaining.map_or(Some(256), |remaining| {
            (remaining > 0).then_some(remaining.min(256))
        })
    }

    fn next_values(&mut self, limit: usize) -> Result<Vec<KeyValue>, Error> {
        let last_key = self.last_key.clone();
        let values = self.storage.with_cf(&self.cf, |values| {
            let entries: Vec<KeyValue> = match (&self.bounds, self.direction, &last_key) {
                (ScanBounds::Prefix(prefix), ScanDirection::Forward, None) => values
                    .range(prefix.clone()..)
                    .take_while(|(key, _)| key.starts_with(prefix))
                    .take(limit)
                    .map(|(key, value)| (key.clone(), value.clone()))
                    .collect(),
                (ScanBounds::Prefix(prefix), ScanDirection::Forward, Some(last_key)) => values
                    .range((Bound::Excluded(last_key.clone()), Bound::Unbounded))
                    .take_while(|(key, _)| key.starts_with(prefix))
                    .take(limit)
                    .map(|(key, value)| (key.clone(), value.clone()))
                    .collect(),
                (ScanBounds::Prefix(prefix), ScanDirection::Reverse, None) => {
                    let start = Bound::Included(prefix.clone());
                    let end = super::prefix_successor(prefix)
                        .map(Bound::Excluded)
                        .unwrap_or(Bound::Unbounded);
                    values
                        .range((start, end))
                        .rev()
                        .take(limit)
                        .map(|(key, value)| (key.clone(), value.clone()))
                        .collect()
                }
                (ScanBounds::Prefix(prefix), ScanDirection::Reverse, Some(last_key)) => values
                    .range((
                        Bound::Included(prefix.clone()),
                        Bound::Excluded(last_key.clone()),
                    ))
                    .rev()
                    .take_while(|(key, _)| key.starts_with(prefix))
                    .take(limit)
                    .map(|(key, value)| (key.clone(), value.clone()))
                    .collect(),
                (ScanBounds::Range { start, end }, ScanDirection::Forward, None) => values
                    .range(start.clone()..end.clone())
                    .take(limit)
                    .map(|(key, value)| (key.clone(), value.clone()))
                    .collect(),
                (ScanBounds::Range { end, .. }, ScanDirection::Forward, Some(last_key)) => values
                    .range((
                        Bound::Excluded(last_key.clone()),
                        Bound::Excluded(end.clone()),
                    ))
                    .take(limit)
                    .map(|(key, value)| (key.clone(), value.clone()))
                    .collect(),
                (ScanBounds::Range { start, end }, ScanDirection::Reverse, None) => values
                    .range(start.clone()..end.clone())
                    .rev()
                    .take(limit)
                    .map(|(key, value)| (key.clone(), value.clone()))
                    .collect(),
                (ScanBounds::Range { start, .. }, ScanDirection::Reverse, Some(last_key)) => values
                    .range((
                        Bound::Included(start.clone()),
                        Bound::Excluded(last_key.clone()),
                    ))
                    .rev()
                    .take(limit)
                    .map(|(key, value)| (key.clone(), value.clone()))
                    .collect(),
            };
            entries
        })?;
        #[cfg(test)]
        self.storage
            .scan_entries_materialized
            .fetch_add(values.len(), Ordering::Relaxed);
        if let Some((last_key, _)) = values.last() {
            self.last_key = Some(last_key.clone());
        } else {
            self.done = true;
        }
        Ok(values)
    }
}

impl super::StorageCursor for MemoryStorageCursor {
    fn next_batch(
        &mut self,
    ) -> super::StorageFuture<'_, Result<Option<Vec<(Vec<u8>, Vec<u8>)>>, Error>> {
        Box::pin(async move {
            if self.done {
                return Ok(None);
            }
            let Some(limit) = self.batch_limit() else {
                self.done = true;
                return Ok(None);
            };
            let values = self.next_values(limit)?;
            if values.is_empty() {
                return Ok(None);
            }
            if let Some(remaining) = &mut self.remaining {
                *remaining -= values.len();
            }
            Ok(Some(values))
        })
    }
}

#[derive(Debug, thiserror::Error)]
pub enum MemoryStorageSnapshotError {
    #[error("failed to encode memory storage snapshot: {0}")]
    Encode(postcard::Error),
    #[error("failed to decode memory storage snapshot: {0}")]
    Decode(postcard::Error),
    #[error("unsupported memory storage snapshot version {found}; expected {expected}")]
    UnsupportedVersion { found: u16, expected: u16 },
    #[error("memory storage snapshot has an invalid physical column-family name: {0}")]
    InvalidColumnFamily(#[from] Error),
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
    #[cfg(test)]
    scan_entries_materialized: Arc<AtomicUsize>,
}

impl MemoryStorage {
    /// Construct storage with the supplied portable column-family names.
    pub fn new(column_families: &[&str]) -> Result<Self, Error> {
        super::validate_physical_storage_names(column_families)?;
        let storage = Self::default();
        storage.ensure_column_families(column_families);
        Ok(storage)
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

    #[cfg(test)]
    pub(crate) fn take_scan_entries_materialized(&self) -> usize {
        self.scan_entries_materialized.swap(0, Ordering::Relaxed)
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
        super::validate_physical_storage_names(snapshot.column_families.keys())?;
        *self.inner.lock().expect("memory storage mutex poisoned") = snapshot.column_families;
        Ok(())
    }
}

impl OrderedKvStorage for MemoryStorage {
    fn get(&self, cf: String, key: Vec<u8>) -> StorageFuture<'_, Result<Option<Value>, Error>> {
        Box::pin(async move { self.with_cf(&cf, |values| values.get(&key).cloned()) })
    }

    fn put_if_absent(
        &self,
        cf: String,
        key: Vec<u8>,
        value: Vec<u8>,
    ) -> StorageFuture<'_, Result<Option<Value>, Error>> {
        Box::pin(async move {
            let mut inner = self.inner.lock().expect("memory storage mutex poisoned");
            let values = inner
                .get_mut(&cf)
                .ok_or_else(|| Error::ColumnFamilyNotFound(cf.clone()))?;
            if let Some(existing) = values.get(&key) {
                return Ok(Some(existing.clone()));
            }
            values.insert(key, value);
            Ok(None)
        })
    }

    fn compare_and_delete(
        &self,
        cf: String,
        key: Vec<u8>,
        expected: Vec<u8>,
    ) -> StorageFuture<'_, Result<bool, Error>> {
        Box::pin(async move {
            let mut inner = self.inner.lock().expect("memory storage mutex poisoned");
            let values = inner
                .get_mut(&cf)
                .ok_or_else(|| Error::ColumnFamilyNotFound(cf.clone()))?;
            if values.get(&key) != Some(&expected) {
                return Ok(false);
            }
            values.remove(&key);
            Ok(true)
        })
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

    fn scan(&self, request: ScanRequest) -> StorageFuture<'_, Result<StorageScan<'_>, Error>> {
        Box::pin(async move {
            let ScanRequest {
                cf,
                bounds,
                direction,
                max_items,
            } = request;
            // Validate eagerly so even a zero-item request reports a missing
            // column family, while the cursor itself remains lazy.
            self.with_cf(&cf, |_| ())?;
            Ok(Box::new(MemoryStorageCursor {
                storage: self.clone(),
                cf,
                bounds,
                direction,
                remaining: max_items,
                last_key: None,
                done: false,
            }) as StorageScan<'_>)
        })
    }

    fn last_with_prefix(
        &self,
        cf: String,
        prefix: Vec<u8>,
    ) -> StorageFuture<'_, Result<Option<super::KeyValue>, Error>> {
        Box::pin(async move {
            self.with_cf(&cf, |values| {
                if let Some(upper) = super::prefix_successor(&prefix) {
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

    fn write_many_outcome(
        &self,
        operations: Vec<OwnedWriteOperation>,
    ) -> StorageFuture<'_, WriteManyOutcome> {
        Box::pin(async move {
            // Memory validates every operation under the same mutex before
            // changing its map, so an error is proven not to have committed.
            match self.write_many(operations).await {
                Ok(()) => WriteManyOutcome::Committed,
                Err(error) => WriteManyOutcome::Uncommitted(error),
            }
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
            super::validate_physical_storage_names(&column_families)?;
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
    use crate::storage::collect_scan;

    #[test]
    fn fallible_open_uses_portable_physical_name_contract() {
        assert!(MemoryStorage::new(&["records"]).is_ok());
        assert!(MemoryStorage::new(&["records\0evil"]).is_err());
        let too_long = "a".repeat(super::super::MAX_APPLICATION_STORAGE_NAME_BYTES + 1);
        assert!(MemoryStorage::new(&[too_long.as_str()]).is_err());
    }

    #[futures_test::test]
    async fn lazy_reverse_prefix_scan_keeps_its_prefix_across_batches() {
        let storage = MemoryStorage::new(&["rows"]).expect("valid memory storage families");
        for index in 0..300 {
            let key = format!("a/{index:03}").into_bytes();
            storage.set("rows".into(), key.clone(), key).await.unwrap();
        }
        // This key is lexicographically between the prefix's first key and
        // its later keys, so a reverse continuation that only bounds by the
        // previous key would leak it on its second batch.
        storage
            .set(
                "rows".into(),
                b"a0-not-in-prefix".to_vec(),
                b"wrong".to_vec(),
            )
            .await
            .unwrap();

        let rows = collect_scan(
            storage
                .scan(ScanRequest::prefix("rows".into(), b"a/".to_vec()).reversed())
                .await
                .unwrap(),
        )
        .await
        .unwrap();
        assert_eq!(rows.len(), 300);
        assert_eq!(rows.first().unwrap().0, b"a/299");
        assert_eq!(rows.last().unwrap().0, b"a/000");
        assert!(rows.iter().all(|(key, _)| key.starts_with(b"a/")));
    }

    #[futures_test::test]
    async fn lazy_scan_observes_a_later_committed_value_in_its_next_batch() {
        let storage = MemoryStorage::new(&["rows"]).expect("valid memory storage families");
        for index in 0..257 {
            let key = format!("row:{index:03}").into_bytes();
            storage
                .set("rows".into(), key.clone(), b"before".to_vec())
                .await
                .unwrap();
        }

        let mut scan = storage
            .scan(ScanRequest::prefix("rows".into(), b"row:".to_vec()))
            .await
            .unwrap();
        assert_eq!(scan.next_batch().await.unwrap().unwrap().len(), 256);
        storage
            .set("rows".into(), b"row:256".to_vec(), b"after".to_vec())
            .await
            .unwrap();
        assert_eq!(
            scan.next_batch().await.unwrap(),
            Some(vec![(b"row:256".to_vec(), b"after".to_vec())]),
            "MemoryStorage is deliberately live between lazy cursor batches"
        );
    }

    #[futures_test::test]
    async fn snapshot_round_trip_preserves_column_families_and_values() {
        let storage = MemoryStorage::new(&["rows", "meta"]).expect("valid memory storage families");
        storage
            .set("rows".into(), b"a".to_vec(), b"one".to_vec())
            .await
            .unwrap();
        storage
            .set("rows".into(), b"b".to_vec(), b"two".to_vec())
            .await
            .unwrap();
        storage
            .set("meta".into(), b"schema".to_vec(), b"v1".to_vec())
            .await
            .unwrap();

        let snapshot = storage.export_snapshot().unwrap();
        let restored = MemoryStorage::default();
        restored.import_snapshot(&snapshot).unwrap();

        assert_eq!(
            restored.get("rows".into(), b"a".to_vec()).await.unwrap(),
            Some(b"one".to_vec())
        );
        assert_eq!(
            restored.get("rows".into(), b"b".to_vec()).await.unwrap(),
            Some(b"two".to_vec())
        );
        assert_eq!(
            restored
                .get("meta".into(), b"schema".to_vec())
                .await
                .unwrap(),
            Some(b"v1".to_vec())
        );
    }

    #[futures_test::test]
    async fn import_snapshot_replaces_existing_contents() {
        let source = MemoryStorage::new(&["rows"]).expect("valid memory storage families");
        source
            .set("rows".into(), b"a".to_vec(), b"one".to_vec())
            .await
            .unwrap();
        let snapshot = source.export_snapshot().unwrap();

        let target = MemoryStorage::new(&["other"]).expect("valid memory storage families");
        target
            .set("other".into(), b"stale".to_vec(), b"value".to_vec())
            .await
            .unwrap();
        target.import_snapshot(&snapshot).unwrap();

        assert_eq!(
            target.get("rows".into(), b"a".to_vec()).await.unwrap(),
            Some(b"one".to_vec())
        );
        assert!(matches!(
            target.get("other".into(), b"stale".to_vec()).await,
            Err(Error::ColumnFamilyNotFound(_))
        ));
    }

    #[futures_test::test]
    async fn import_snapshot_rejects_invalid_families_without_replacing_state() {
        let storage = MemoryStorage::new(&["rows"]).expect("valid memory storage families");
        storage
            .set("rows".into(), b"keep".to_vec(), b"value".to_vec())
            .await
            .unwrap();
        let snapshot = MemoryStorageSnapshot {
            version: MEMORY_STORAGE_SNAPSHOT_VERSION,
            column_families: BTreeMap::from([("rows\0evil".to_owned(), BTreeMap::new())]),
        };
        let bytes = postcard::to_allocvec(&snapshot).unwrap();

        assert!(matches!(
            storage.import_snapshot(&bytes),
            Err(MemoryStorageSnapshotError::InvalidColumnFamily(_))
        ));
        assert_eq!(
            storage.get("rows".into(), b"keep".to_vec()).await.unwrap(),
            Some(b"value".to_vec())
        );
    }

    #[futures_test::test]
    async fn approximate_class_bytes_sums_keys_and_values_exactly() {
        let storage = MemoryStorage::new(&["rows"]).expect("valid memory storage families");
        storage
            .set("rows".into(), b"a".to_vec(), b"one".to_vec())
            .await
            .unwrap();
        storage
            .set("rows".into(), b"bb".to_vec(), b"two".to_vec())
            .await
            .unwrap();

        assert_eq!(
            storage
                .approximate_class_bytes("rows".into())
                .await
                .unwrap(),
            Some(9)
        );
    }
}
