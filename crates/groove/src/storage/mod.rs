//! Ordered key/value storage seam used behind record stores.
//!
//! This module owns the backing-implementation contract: column families, point
//! reads, ordered range/prefix scans, reverse/prefix helpers, and atomic write
//! batches. Storage backends only need to provide [`OrderedKvStorage`]. Higher
//! layers should work through record-store handles such as [`RecordStore`] and
//! directly exposed direct stores rather than reaching through to column
//! families or raw ordered-KV operations.
//!
//! The storage layer deliberately does not know about schemas, query graphs,
//! records beyond typed convenience wrappers, or Jazz semantics. The RocksDB
//! implementation lives in [`rocksdb_storage`]; higher layers decide when a
//! batch is durable and how storage writes relate to an IVM tick.

#[cfg_attr(not(target_arch = "wasm32"), allow(dead_code))]
mod key_codec;
mod memory;
#[cfg(target_arch = "wasm32")]
mod opfs;
#[cfg(feature = "rocksdb")]
#[path = "rocksdb.rs"]
pub mod rocksdb_storage;

use std::cell::RefCell;
use std::collections::BTreeMap;

use crate::records::{Record, RecordDescriptor};
use thiserror::Error;

pub use memory::MemoryStorage;
#[cfg(target_arch = "wasm32")]
pub use opfs::OpfsStorage;
#[cfg(feature = "rocksdb")]
pub use rocksdb_storage::{Durability, RocksDbStorage};

pub type ColumnFamilyName = str;
pub type Key = [u8];
pub type Value = Vec<u8>;
pub type KeyValue = (Vec<u8>, Vec<u8>);
/// Callback form used by scans so storage implementations do not have to
/// materialize large ranges before the caller can process them.
pub type ScanVisitor<'visitor> =
    dyn for<'a, 'b> FnMut(&'a [u8], &'b [u8]) -> Result<(), Error> + 'visitor;

/// Backing-implementation interface for ordered key/value storage.
///
/// This is the only trait a storage backend must implement. Its column-family
/// names are backing details consumed by record-store plumbing; higher layers
/// should use typed record-store handles instead of calling these methods
/// directly. The trait intentionally exposes batch atomicity but no higher
/// transaction semantics; `commit_batch` owns the tick ordering above this
/// layer.
pub trait OrderedKvStorage {
    fn get(&self, cf: &ColumnFamilyName, key: &Key) -> Result<Option<Value>, Error>;
    fn set(&self, cf: &ColumnFamilyName, key: &Key, value: &[u8]) -> Result<(), Error>;
    fn delete(&self, cf: &ColumnFamilyName, key: &Key) -> Result<(), Error>;
    fn scan_range(
        &self,
        cf: &ColumnFamilyName,
        start: &Key,
        end: &Key,
        visit: &mut ScanVisitor<'_>,
    ) -> Result<(), Error>;
    fn scan_prefix(
        &self,
        cf: &ColumnFamilyName,
        prefix: &Key,
        visit: &mut ScanVisitor<'_>,
    ) -> Result<(), Error>;
    fn scan_prefix_reverse(
        &self,
        cf: &ColumnFamilyName,
        prefix: &Key,
        visit: &mut ScanVisitor<'_>,
    ) -> Result<(), Error> {
        let mut values = Vec::new();
        self.scan_prefix(cf, prefix, &mut |key, value| {
            values.push((key.to_vec(), value.to_vec()));
            Ok(())
        })?;
        for (key, value) in values.into_iter().rev() {
            visit(&key, &value)?;
        }
        Ok(())
    }
    fn last_with_prefix(
        &self,
        cf: &ColumnFamilyName,
        prefix: &Key,
    ) -> Result<Option<KeyValue>, Error> {
        let mut last = None;
        self.scan_prefix(cf, prefix, &mut |key, value| {
            last = Some((key.to_vec(), value.to_vec()));
            Ok(())
        })?;
        Ok(last)
    }
    fn last_with_prefix_before_or_at(
        &self,
        cf: &ColumnFamilyName,
        prefix: &Key,
        upper: &Key,
    ) -> Result<Option<KeyValue>, Error> {
        let mut last = None;
        self.scan_prefix(cf, prefix, &mut |key, value| {
            if key <= upper {
                last = Some((key.to_vec(), value.to_vec()));
            }
            Ok(())
        })?;
        Ok(last)
    }
    fn write_many(&self, operations: &[WriteOperation<'_>]) -> Result<(), Error>;

    fn range(&self, cf: &ColumnFamilyName, start: &Key, end: &Key) -> Result<Vec<KeyValue>, Error> {
        let mut values = Vec::new();
        self.scan_range(cf, start, end, &mut |key, value| {
            values.push((key.to_vec(), value.to_vec()));
            Ok(())
        })?;
        Ok(values)
    }

    fn prefix(&self, cf: &ColumnFamilyName, prefix: &Key) -> Result<Vec<KeyValue>, Error> {
        let mut values = Vec::new();
        self.scan_prefix(cf, prefix, &mut |key, value| {
            values.push((key.to_vec(), value.to_vec()));
            Ok(())
        })?;
        Ok(values)
    }
}

/// Storage that can be reconstructed with an expanded table/column-family set.
pub trait ReopenableStorage: OrderedKvStorage + Sized {
    fn reopen(self, column_families: &[&str]) -> Result<Self, Error>;
}

/// Typed view over one storage column family.
pub struct RecordStore<'a, S> {
    storage: &'a S,
    /// One table or durable index column family.
    column_family: &'a str,
    /// Interprets stored bytes without copying until a caller asks for values.
    descriptor: &'a RecordDescriptor,
}

impl<'a, S> RecordStore<'a, S>
where
    S: OrderedKvStorage,
{
    pub fn new(storage: &'a S, column_family: &'a str, descriptor: &'a RecordDescriptor) -> Self {
        Self {
            storage,
            column_family,
            descriptor,
        }
    }

    pub fn descriptor(&self) -> &RecordDescriptor {
        self.descriptor
    }

    pub fn column_family(&self) -> &str {
        self.column_family
    }

    pub fn get_raw(&self, key: &Key) -> Result<Option<Vec<u8>>, Error> {
        self.storage.get(self.column_family, key)
    }

    pub fn get(&self, key: &Key) -> Result<Option<Record<'_>>, Error> {
        self.get_raw(key)
            .map(|record| record.map(|record| self.descriptor.bind_owned(record)))
    }

    pub fn range(&self, start: &Key, end: &Key) -> Result<Vec<KeyValue>, Error> {
        self.storage.range(self.column_family, start, end)
    }

    pub fn prefix(&self, prefix: &Key) -> Result<Vec<KeyValue>, Error> {
        self.storage.prefix(self.column_family, prefix)
    }

    pub fn scan_range(
        &self,
        start: &Key,
        end: &Key,
        visit: &mut ScanVisitor<'_>,
    ) -> Result<(), Error> {
        self.storage
            .scan_range(self.column_family, start, end, visit)
    }

    pub fn scan_prefix(&self, prefix: &Key, visit: &mut ScanVisitor<'_>) -> Result<(), Error> {
        self.storage.scan_prefix(self.column_family, prefix, visit)
    }

    pub fn scan_prefix_reverse(
        &self,
        prefix: &Key,
        visit: &mut ScanVisitor<'_>,
    ) -> Result<(), Error> {
        self.storage
            .scan_prefix_reverse(self.column_family, prefix, visit)
    }

    pub fn last_with_prefix(&self, prefix: &Key) -> Result<Option<KeyValue>, Error> {
        self.storage.last_with_prefix(self.column_family, prefix)
    }

    pub fn last_with_prefix_before_or_at(
        &self,
        prefix: &Key,
        upper: &Key,
    ) -> Result<Option<KeyValue>, Error> {
        self.storage
            .last_with_prefix_before_or_at(self.column_family, prefix, upper)
    }

    pub fn set<'op>(&'op self, key: &'op Key, record: &'op [u8]) -> WriteOperation<'op> {
        WriteOperation::set(self.column_family, key, record)
    }

    pub fn delete<'op>(&'op self, key: &'op Key) -> WriteOperation<'op> {
        WriteOperation::delete(self.column_family, key)
    }

    pub fn write_many(&self, operations: &[WriteOperation<'_>]) -> Result<(), Error> {
        self.storage.write_many(operations)
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum WriteOperation<'a> {
    /// Borrowed operation so callers can build a RocksDB batch without cloning
    /// already-owned encoded records.
    Set {
        cf: &'a str,
        key: &'a Key,
        value: &'a [u8],
    },
    Delete {
        cf: &'a str,
        key: &'a Key,
    },
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum OwnedWriteOperation {
    Set {
        cf: String,
        key: Vec<u8>,
        value: Vec<u8>,
    },
    Delete {
        cf: String,
        key: Vec<u8>,
    },
}

impl OwnedWriteOperation {
    pub fn as_write_operation(&self) -> WriteOperation<'_> {
        match self {
            Self::Set { cf, key, value } => WriteOperation::set(cf, key, value),
            Self::Delete { cf, key } => WriteOperation::delete(cf, key),
        }
    }

    fn cf(&self) -> &str {
        match self {
            Self::Set { cf, .. } | Self::Delete { cf, .. } => cf,
        }
    }

    fn key(&self) -> &[u8] {
        match self {
            Self::Set { key, .. } | Self::Delete { key, .. } => key,
        }
    }
}

pub struct StagedWriteOverlay<'a, S> {
    base: &'a S,
    staged_writes: &'a RefCell<Vec<OwnedWriteOperation>>,
}

impl<'a, S> StagedWriteOverlay<'a, S> {
    pub fn new(base: &'a S, staged_writes: &'a RefCell<Vec<OwnedWriteOperation>>) -> Self {
        Self {
            base,
            staged_writes,
        }
    }

    pub fn stage(&self, operation: OwnedWriteOperation) {
        self.staged_writes.borrow_mut().push(operation);
    }

    pub fn drain_into(&self, target: &mut Vec<OwnedWriteOperation>) {
        target.extend(self.staged_writes.borrow_mut().drain(..));
    }
}

impl<S> OrderedKvStorage for StagedWriteOverlay<'_, S>
where
    S: OrderedKvStorage,
{
    fn get(&self, cf: &ColumnFamilyName, key: &Key) -> Result<Option<Value>, Error> {
        for operation in self.staged_writes.borrow().iter().rev() {
            if operation.cf() == cf && operation.key() == key {
                return match operation {
                    OwnedWriteOperation::Set { value, .. } => Ok(Some(value.clone())),
                    OwnedWriteOperation::Delete { .. } => Ok(None),
                };
            }
        }
        self.base.get(cf, key)
    }

    fn set(&self, cf: &ColumnFamilyName, key: &Key, value: &[u8]) -> Result<(), Error> {
        self.stage(OwnedWriteOperation::Set {
            cf: cf.to_owned(),
            key: key.to_vec(),
            value: value.to_vec(),
        });
        Ok(())
    }

    fn delete(&self, cf: &ColumnFamilyName, key: &Key) -> Result<(), Error> {
        self.stage(OwnedWriteOperation::Delete {
            cf: cf.to_owned(),
            key: key.to_vec(),
        });
        Ok(())
    }

    fn scan_range(
        &self,
        cf: &ColumnFamilyName,
        start: &Key,
        end: &Key,
        visit: &mut ScanVisitor<'_>,
    ) -> Result<(), Error> {
        let mut values = self.base.range(cf, start, end)?;
        overlay_values(
            &mut values,
            cf,
            |key| key >= start && key < end,
            self.staged_writes,
        );
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
        let mut values = self.base.prefix(cf, prefix)?;
        overlay_values(
            &mut values,
            cf,
            |key| key.starts_with(prefix),
            self.staged_writes,
        );
        for (key, value) in values {
            visit(&key, &value)?;
        }
        Ok(())
    }

    fn scan_prefix_reverse(
        &self,
        cf: &ColumnFamilyName,
        prefix: &Key,
        visit: &mut ScanVisitor<'_>,
    ) -> Result<(), Error> {
        let staged = staged_prefix_overlay_desc(cf, prefix, self.staged_writes);
        let mut staged_index = 0;
        self.base
            .scan_prefix_reverse(cf, prefix, &mut |base_key, base_value| {
                while let Some((staged_key, staged_value)) = staged.get(staged_index) {
                    if staged_key.as_slice() <= base_key {
                        break;
                    }
                    if let Some(value) = staged_value {
                        visit(staged_key, value)?;
                    }
                    staged_index += 1;
                }

                if let Some((staged_key, staged_value)) = staged.get(staged_index)
                    && staged_key.as_slice() == base_key
                {
                    if let Some(value) = staged_value {
                        visit(staged_key, value)?;
                    }
                    staged_index += 1;
                    return Ok(());
                }

                visit(base_key, base_value)
            })?;

        while let Some((staged_key, staged_value)) = staged.get(staged_index) {
            if let Some(value) = staged_value {
                visit(staged_key, value)?;
            }
            staged_index += 1;
        }
        Ok(())
    }

    fn last_with_prefix(
        &self,
        cf: &ColumnFamilyName,
        prefix: &Key,
    ) -> Result<Option<KeyValue>, Error> {
        let mut has_staged_delete = false;
        for operation in self.staged_writes.borrow().iter() {
            if operation.cf() != cf || !operation.key().starts_with(prefix) {
                continue;
            }
            match operation {
                OwnedWriteOperation::Set { .. } => {}
                OwnedWriteOperation::Delete { .. } => has_staged_delete = true,
            }
        }

        if !has_staged_delete {
            let largest_staged_set = staged_prefix_overlay_desc(cf, prefix, self.staged_writes)
                .into_iter()
                .find_map(|(key, value)| value.map(|value| (key, value)));
            return match (self.base.last_with_prefix(cf, prefix)?, largest_staged_set) {
                (Some(base), Some(staged)) if staged.0 >= base.0 => Ok(Some(staged)),
                (Some(base), _) => Ok(Some(base)),
                (None, staged) => Ok(staged),
            };
        }

        let mut values = self.base.prefix(cf, prefix)?;
        overlay_values(
            &mut values,
            cf,
            |key| key.starts_with(prefix),
            self.staged_writes,
        );
        Ok(values.pop())
    }

    fn write_many(&self, operations: &[WriteOperation<'_>]) -> Result<(), Error> {
        for operation in operations {
            match operation {
                WriteOperation::Set { cf, key, value } => self.set(cf, key, value)?,
                WriteOperation::Delete { cf, key } => self.delete(cf, key)?,
            }
        }
        Ok(())
    }
}

fn overlay_values(
    values: &mut Vec<KeyValue>,
    cf: &ColumnFamilyName,
    include: impl Fn(&[u8]) -> bool,
    staged_writes: &RefCell<Vec<OwnedWriteOperation>>,
) {
    let mut overlay = values
        .drain(..)
        .map(|(key, value)| (key, Some(value)))
        .collect::<BTreeMap<_, _>>();
    for operation in staged_writes.borrow().iter() {
        if operation.cf() != cf || !include(operation.key()) {
            continue;
        }
        match operation {
            OwnedWriteOperation::Set { key, value, .. } => {
                overlay.insert(key.clone(), Some(value.clone()));
            }
            OwnedWriteOperation::Delete { key, .. } => {
                overlay.insert(key.clone(), None);
            }
        }
    }
    values.extend(
        overlay
            .into_iter()
            .filter_map(|(key, value)| value.map(|value| (key, value))),
    );
}

fn staged_prefix_overlay_desc(
    cf: &ColumnFamilyName,
    prefix: &Key,
    staged_writes: &RefCell<Vec<OwnedWriteOperation>>,
) -> Vec<(Vec<u8>, Option<Vec<u8>>)> {
    let mut overlay = BTreeMap::new();
    for operation in staged_writes.borrow().iter() {
        if operation.cf() != cf || !operation.key().starts_with(prefix) {
            continue;
        }
        match operation {
            OwnedWriteOperation::Set { key, value, .. } => {
                overlay.insert(key.clone(), Some(value.clone()));
            }
            OwnedWriteOperation::Delete { key, .. } => {
                overlay.insert(key.clone(), None);
            }
        }
    }
    overlay.into_iter().rev().collect()
}

impl<'a> WriteOperation<'a> {
    pub fn set(cf: &'a str, key: &'a Key, value: &'a [u8]) -> Self {
        Self::Set { cf, key, value }
    }

    pub fn delete(cf: &'a str, key: &'a Key) -> Self {
        Self::Delete { cf, key }
    }
}

#[derive(Debug, Error)]
pub enum Error {
    #[error("column family not found: {0}")]
    ColumnFamilyNotFound(String),
    #[error("invalid storage key: {0}")]
    InvalidStorageKey(String),
    #[cfg(feature = "rocksdb")]
    #[error(transparent)]
    RocksDb(#[from] ::rocksdb::Error),
    #[cfg(target_arch = "wasm32")]
    #[error(transparent)]
    Opfs(#[from] opfs_btree::BTreeError),
}

#[cfg(test)]
pub(crate) mod conformance {
    use super::*;

    pub(crate) fn persistence_order_and_batch_atomicity<S>(storage: S)
    where
        S: OrderedKvStorage,
    {
        storage.set("records", b"user:2", b"two").unwrap();
        storage.set("records", b"user:1", b"one").unwrap();
        storage.set("records", b"user:10", b"ten").unwrap();
        storage.set("records", &[0xff, 0x00], b"ff-zero").unwrap();
        storage.set("records", &[0xff, 0x01], b"ff-one").unwrap();

        assert_eq!(
            storage.range("records", b"user:", b"user;").unwrap(),
            vec![
                (b"user:1".to_vec(), b"one".to_vec()),
                (b"user:10".to_vec(), b"ten".to_vec()),
                (b"user:2".to_vec(), b"two".to_vec()),
            ]
        );
        assert_eq!(
            storage.prefix("records", &[0xff]).unwrap(),
            vec![
                (vec![0xff, 0x00], b"ff-zero".to_vec()),
                (vec![0xff, 0x01], b"ff-one".to_vec()),
            ]
        );

        let error = storage
            .write_many(&[
                WriteOperation::set("records", b"user:3", b"three"),
                WriteOperation::set("missing", b"user:4", b"four"),
            ])
            .unwrap_err();
        assert!(matches!(error, Error::ColumnFamilyNotFound(_)));
        assert_eq!(storage.get("records", b"user:3").unwrap(), None);

        storage
            .write_many(&[
                WriteOperation::set("records", b"user:3", b"three"),
                WriteOperation::delete("records", b"user:2"),
            ])
            .unwrap();
        assert_eq!(
            storage.prefix("records", b"user:").unwrap(),
            vec![
                (b"user:1".to_vec(), b"one".to_vec()),
                (b"user:10".to_vec(), b"ten".to_vec()),
                (b"user:3".to_vec(), b"three".to_vec()),
            ]
        );
    }

    pub(crate) fn reopen_preserves_data_and_adds_families<S>(storage: S)
    where
        S: ReopenableStorage,
    {
        storage.set("records", b"1", b"record").unwrap();

        let storage = storage.reopen(&["records", "indices"]).unwrap();
        storage.set("indices", b"name:record", b"1").unwrap();

        assert_eq!(
            storage.get("records", b"1").unwrap(),
            Some(b"record".to_vec())
        );
        assert_eq!(
            storage.get("indices", b"name:record").unwrap(),
            Some(b"1".to_vec())
        );
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::records::{Value, ValueType};
    use std::cell::Cell;

    #[test]
    fn get_set_and_delete_values() {
        let temp_dir = tempfile::tempdir().unwrap();
        let storage = RocksDbStorage::open(temp_dir.path(), &["records"]).unwrap();

        storage.set("records", b"a", b"one").unwrap();
        assert_eq!(storage.get("records", b"a").unwrap(), Some(b"one".to_vec()));

        storage.delete("records", b"a").unwrap();
        assert_eq!(storage.get("records", b"a").unwrap(), None);
    }

    #[test]
    fn wal_no_sync_durability_mode_keeps_wal_writes_enabled() {
        let temp_dir = tempfile::tempdir().unwrap();
        let storage = RocksDbStorage::open_with_durability(
            temp_dir.path(),
            &["records"],
            Durability::WalNoSync,
        )
        .unwrap();

        storage.set("records", b"a", b"one").unwrap();

        assert_eq!(storage.get("records", b"a").unwrap(), Some(b"one".to_vec()));
    }

    #[test]
    fn range_returns_ordered_values_between_start_and_end() {
        let temp_dir = tempfile::tempdir().unwrap();
        let storage = RocksDbStorage::open(temp_dir.path(), &["records"]).unwrap();

        storage.set("records", b"a", b"one").unwrap();
        storage.set("records", b"b", b"two").unwrap();
        storage.set("records", b"c", b"three").unwrap();

        assert_eq!(
            storage.range("records", b"a", b"c").unwrap(),
            vec![
                (b"a".to_vec(), b"one".to_vec()),
                (b"b".to_vec(), b"two".to_vec())
            ]
        );
    }

    #[test]
    fn prefix_returns_ordered_values_with_matching_prefix() {
        let temp_dir = tempfile::tempdir().unwrap();
        let storage = RocksDbStorage::open(temp_dir.path(), &["records"]).unwrap();

        storage.set("records", b"user:1", b"a").unwrap();
        storage.set("records", b"user:2", b"b").unwrap();
        storage.set("records", b"view:1", b"c").unwrap();

        assert_eq!(
            storage.prefix("records", b"user:").unwrap(),
            vec![
                (b"user:1".to_vec(), b"a".to_vec()),
                (b"user:2".to_vec(), b"b".to_vec())
            ]
        );
    }

    #[test]
    fn prefix_handles_prefixes_without_a_finite_upper_bound() {
        let temp_dir = tempfile::tempdir().unwrap();
        let storage = RocksDbStorage::open(temp_dir.path(), &["records"]).unwrap();

        storage.set("records", &[0xfe], b"before").unwrap();
        storage.set("records", &[0xff, 0x00], b"a").unwrap();
        storage.set("records", &[0xff, 0x01], b"b").unwrap();

        assert_eq!(
            storage.prefix("records", &[0xff]).unwrap(),
            vec![
                (vec![0xff, 0x00], b"a".to_vec()),
                (vec![0xff, 0x01], b"b".to_vec())
            ]
        );
    }

    #[test]
    fn direct_operations_report_missing_column_families() {
        let temp_dir = tempfile::tempdir().unwrap();
        let storage = RocksDbStorage::open(temp_dir.path(), &["records"]).unwrap();

        assert!(matches!(
            storage.get("missing", b"a"),
            Err(Error::ColumnFamilyNotFound(cf)) if cf == "missing"
        ));
        assert!(matches!(
            storage.set("missing", b"a", b"one"),
            Err(Error::ColumnFamilyNotFound(cf)) if cf == "missing"
        ));
        assert!(matches!(
            storage.delete("missing", b"a"),
            Err(Error::ColumnFamilyNotFound(cf)) if cf == "missing"
        ));
        assert!(matches!(
            storage.range("missing", b"a", b"z"),
            Err(Error::ColumnFamilyNotFound(cf)) if cf == "missing"
        ));
        assert!(matches!(
            storage.prefix("missing", b"a"),
            Err(Error::ColumnFamilyNotFound(cf)) if cf == "missing"
        ));
        assert!(matches!(
            storage.scan_range("missing", b"a", b"z", &mut |_, _| Ok(())),
            Err(Error::ColumnFamilyNotFound(cf)) if cf == "missing"
        ));
        assert!(matches!(
            storage.scan_prefix("missing", b"a", &mut |_, _| Ok(())),
            Err(Error::ColumnFamilyNotFound(cf)) if cf == "missing"
        ));
    }

    #[test]
    fn scans_visit_ordered_values_without_materializing_in_storage_api() {
        let temp_dir = tempfile::tempdir().unwrap();
        let storage = RocksDbStorage::open(temp_dir.path(), &["records"]).unwrap();

        storage.set("records", b"a", b"one").unwrap();
        storage.set("records", b"b", b"two").unwrap();
        storage.set("records", b"c", b"three").unwrap();

        let mut visited = Vec::new();
        storage
            .scan_range("records", b"a", b"c", &mut |key, value| {
                visited.push((key.to_vec(), value.to_vec()));
                Ok(())
            })
            .unwrap();

        assert_eq!(
            visited,
            vec![
                (b"a".to_vec(), b"one".to_vec()),
                (b"b".to_vec(), b"two".to_vec())
            ]
        );
    }

    #[test]
    fn write_many_writes_all_operations_atomically() {
        let temp_dir = tempfile::tempdir().unwrap();
        let storage = RocksDbStorage::open(temp_dir.path(), &["records", "indices"]).unwrap();

        storage
            .write_many(&[
                WriteOperation::set("records", b"1", b"record"),
                WriteOperation::set("indices", b"name:record", b"1"),
            ])
            .unwrap();

        assert_eq!(
            storage.get("records", b"1").unwrap(),
            Some(b"record".to_vec())
        );
        assert_eq!(
            storage.get("indices", b"name:record").unwrap(),
            Some(b"1".to_vec())
        );
    }

    #[test]
    fn staged_overlay_reads_staged_sets_and_deletes_before_base_storage() {
        let storage = MemoryStorage::new(&["indices"]);
        storage.set("indices", b"a", b"base-a").unwrap();
        storage.set("indices", b"b", b"base-b").unwrap();
        let staged = RefCell::new(vec![
            OwnedWriteOperation::Set {
                cf: "indices".to_owned(),
                key: b"a".to_vec(),
                value: b"staged-a".to_vec(),
            },
            OwnedWriteOperation::Delete {
                cf: "indices".to_owned(),
                key: b"b".to_vec(),
            },
            OwnedWriteOperation::Set {
                cf: "indices".to_owned(),
                key: b"c".to_vec(),
                value: b"staged-c".to_vec(),
            },
        ]);
        let overlay = StagedWriteOverlay::new(&storage, &staged);

        assert_eq!(
            overlay.get("indices", b"a").unwrap(),
            Some(b"staged-a".to_vec())
        );
        assert_eq!(overlay.get("indices", b"b").unwrap(), None);
        assert_eq!(
            overlay.get("indices", b"c").unwrap(),
            Some(b"staged-c".to_vec())
        );
        assert_eq!(
            overlay.prefix("indices", b"").unwrap(),
            vec![
                (b"a".to_vec(), b"staged-a".to_vec()),
                (b"c".to_vec(), b"staged-c".to_vec()),
            ]
        );
        assert_eq!(
            storage.get("indices", b"a").unwrap(),
            Some(b"base-a".to_vec())
        );
    }

    #[test]
    fn write_many_fails_without_writing_when_column_family_is_missing() {
        let temp_dir = tempfile::tempdir().unwrap();
        let storage = RocksDbStorage::open(temp_dir.path(), &["records"]).unwrap();

        let error = storage
            .write_many(&[
                WriteOperation::set("records", b"1", b"record"),
                WriteOperation::set("missing", b"2", b"nope"),
            ])
            .unwrap_err();

        assert!(matches!(error, Error::ColumnFamilyNotFound(_)));
        assert_eq!(storage.get("records", b"1").unwrap(), None);
    }

    #[test]
    fn write_many_can_mix_sets_and_deletes_atomically() {
        let temp_dir = tempfile::tempdir().unwrap();
        let storage = RocksDbStorage::open(temp_dir.path(), &["records"]).unwrap();

        storage.set("records", b"old", b"value").unwrap();
        storage
            .write_many(&[
                WriteOperation::set("records", b"new", b"value"),
                WriteOperation::delete("records", b"old"),
            ])
            .unwrap();

        assert_eq!(
            storage.get("records", b"new").unwrap(),
            Some(b"value".to_vec())
        );
        assert_eq!(storage.get("records", b"old").unwrap(), None);
    }

    #[test]
    fn memory_storage_orders_scans_and_errors_on_missing_column_families() {
        let storage = MemoryStorage::new(&["records"]);
        storage.set("records", b"b", b"two").unwrap();
        storage.set("records", b"a", b"one").unwrap();
        storage.set("records", b"aa", b"one-one").unwrap();

        assert!(matches!(
            storage.get("missing", b"a"),
            Err(Error::ColumnFamilyNotFound(_))
        ));
        assert!(matches!(
            storage.set("missing", b"a", b"one"),
            Err(Error::ColumnFamilyNotFound(_))
        ));

        let mut range = Vec::new();
        storage
            .scan_range("records", b"a", b"b", &mut |key, value| {
                range.push((key.to_vec(), value.to_vec()));
                Ok(())
            })
            .unwrap();
        assert_eq!(
            range,
            vec![
                (b"a".to_vec(), b"one".to_vec()),
                (b"aa".to_vec(), b"one-one".to_vec())
            ]
        );

        let mut prefix = Vec::new();
        storage
            .scan_prefix("records", b"a", &mut |key, value| {
                prefix.push((key.to_vec(), value.to_vec()));
                Ok(())
            })
            .unwrap();
        assert_eq!(
            prefix,
            vec![
                (b"a".to_vec(), b"one".to_vec()),
                (b"aa".to_vec(), b"one-one".to_vec())
            ]
        );
    }

    #[test]
    fn staged_overlay_reverse_prefix_scans_match_trait_default() {
        struct DefaultReverse<'a, S>(&'a StagedWriteOverlay<'a, S>);

        impl<S> OrderedKvStorage for DefaultReverse<'_, S>
        where
            S: OrderedKvStorage,
        {
            fn get(&self, cf: &ColumnFamilyName, key: &Key) -> Result<Option<Vec<u8>>, Error> {
                self.0.get(cf, key)
            }

            fn set(&self, cf: &ColumnFamilyName, key: &Key, value: &[u8]) -> Result<(), Error> {
                self.0.set(cf, key, value)
            }

            fn delete(&self, cf: &ColumnFamilyName, key: &Key) -> Result<(), Error> {
                self.0.delete(cf, key)
            }

            fn scan_range(
                &self,
                cf: &ColumnFamilyName,
                start: &Key,
                end: &Key,
                visit: &mut ScanVisitor<'_>,
            ) -> Result<(), Error> {
                self.0.scan_range(cf, start, end, visit)
            }

            fn scan_prefix(
                &self,
                cf: &ColumnFamilyName,
                prefix: &Key,
                visit: &mut ScanVisitor<'_>,
            ) -> Result<(), Error> {
                self.0.scan_prefix(cf, prefix, visit)
            }

            fn write_many(&self, operations: &[WriteOperation<'_>]) -> Result<(), Error> {
                self.0.write_many(operations)
            }
        }

        fn assert_case(
            name: &str,
            base_rows: &[(&[u8], &[u8])],
            staged_rows: Vec<OwnedWriteOperation>,
            prefix: &[u8],
            expected: Vec<KeyValue>,
        ) {
            let storage = MemoryStorage::new(&["indices"]);
            for (key, value) in base_rows {
                storage.set("indices", key, value).unwrap();
            }
            storage.set("indices", b"view:1", b"base-view").unwrap();
            let staged = RefCell::new(staged_rows);
            let overlay = StagedWriteOverlay::new(&storage, &staged);
            let default_reverse = DefaultReverse(&overlay);

            let mut optimized = Vec::new();
            overlay
                .scan_prefix_reverse("indices", prefix, &mut |key, value| {
                    optimized.push((key.to_vec(), value.to_vec()));
                    Ok(())
                })
                .unwrap();

            let mut defaulted = Vec::new();
            default_reverse
                .scan_prefix_reverse("indices", prefix, &mut |key, value| {
                    defaulted.push((key.to_vec(), value.to_vec()));
                    Ok(())
                })
                .unwrap();

            assert_eq!(optimized, defaulted, "{name}");
            assert_eq!(optimized, expected, "{name}");
            assert_eq!(
                overlay.last_with_prefix("indices", prefix).unwrap(),
                default_reverse.last_with_prefix("indices", prefix).unwrap(),
                "{name}"
            );
        }

        assert_case(
            "mixed staged overrides and deletes",
            &[
                (b"user:1", b"base-1"),
                (b"user:2", b"base-2"),
                (b"user:4", b"base-4"),
            ],
            vec![
                OwnedWriteOperation::Set {
                    cf: "indices".to_owned(),
                    key: b"user:2".to_vec(),
                    value: b"staged-2".to_vec(),
                },
                OwnedWriteOperation::Delete {
                    cf: "indices".to_owned(),
                    key: b"user:4".to_vec(),
                },
                OwnedWriteOperation::Set {
                    cf: "indices".to_owned(),
                    key: b"user:3".to_vec(),
                    value: b"staged-3".to_vec(),
                },
                OwnedWriteOperation::Set {
                    cf: "indices".to_owned(),
                    key: b"view:2".to_vec(),
                    value: b"staged-view".to_vec(),
                },
            ],
            b"user:",
            vec![
                (b"user:3".to_vec(), b"staged-3".to_vec()),
                (b"user:2".to_vec(), b"staged-2".to_vec()),
                (b"user:1".to_vec(), b"base-1".to_vec()),
            ],
        );

        assert_case(
            "staged delete of base last key",
            &[
                (b"user:1", b"base-1"),
                (b"user:2", b"base-2"),
                (b"user:4", b"base-4"),
            ],
            vec![OwnedWriteOperation::Delete {
                cf: "indices".to_owned(),
                key: b"user:4".to_vec(),
            }],
            b"user:",
            vec![
                (b"user:2".to_vec(), b"base-2".to_vec()),
                (b"user:1".to_vec(), b"base-1".to_vec()),
            ],
        );

        assert_case(
            "empty staged buffer",
            &[(b"user:1", b"base-1"), (b"user:2", b"base-2")],
            Vec::new(),
            b"user:",
            vec![
                (b"user:2".to_vec(), b"base-2".to_vec()),
                (b"user:1".to_vec(), b"base-1".to_vec()),
            ],
        );

        assert_case(
            "staged-only prefix",
            &[(b"user:1", b"base-1")],
            vec![OwnedWriteOperation::Set {
                cf: "indices".to_owned(),
                key: b"team:1".to_vec(),
                value: b"staged-team".to_vec(),
            }],
            b"team:",
            vec![(b"team:1".to_vec(), b"staged-team".to_vec())],
        );

        assert_case(
            "base empty for prefix",
            &[(b"view:1", b"base-view")],
            vec![
                OwnedWriteOperation::Set {
                    cf: "indices".to_owned(),
                    key: b"user:1".to_vec(),
                    value: b"staged-1".to_vec(),
                },
                OwnedWriteOperation::Set {
                    cf: "indices".to_owned(),
                    key: b"user:3".to_vec(),
                    value: b"staged-3".to_vec(),
                },
            ],
            b"user:",
            vec![
                (b"user:3".to_vec(), b"staged-3".to_vec()),
                (b"user:1".to_vec(), b"staged-1".to_vec()),
            ],
        );
    }

    #[test]
    fn staged_overlay_last_with_prefix_no_delete_uses_one_base_seek() {
        struct CountingStorage<S> {
            inner: S,
            prefix_scans: Cell<usize>,
            reverse_prefix_scans: Cell<usize>,
            last_with_prefix_calls: Cell<usize>,
        }

        impl<S> CountingStorage<S> {
            fn new(inner: S) -> Self {
                Self {
                    inner,
                    prefix_scans: Cell::new(0),
                    reverse_prefix_scans: Cell::new(0),
                    last_with_prefix_calls: Cell::new(0),
                }
            }
        }

        impl<S> OrderedKvStorage for CountingStorage<S>
        where
            S: OrderedKvStorage,
        {
            fn get(&self, cf: &ColumnFamilyName, key: &Key) -> Result<Option<Vec<u8>>, Error> {
                self.inner.get(cf, key)
            }

            fn set(&self, cf: &ColumnFamilyName, key: &Key, value: &[u8]) -> Result<(), Error> {
                self.inner.set(cf, key, value)
            }

            fn delete(&self, cf: &ColumnFamilyName, key: &Key) -> Result<(), Error> {
                self.inner.delete(cf, key)
            }

            fn scan_range(
                &self,
                cf: &ColumnFamilyName,
                start: &Key,
                end: &Key,
                visit: &mut ScanVisitor<'_>,
            ) -> Result<(), Error> {
                self.inner.scan_range(cf, start, end, visit)
            }

            fn scan_prefix(
                &self,
                cf: &ColumnFamilyName,
                prefix: &Key,
                visit: &mut ScanVisitor<'_>,
            ) -> Result<(), Error> {
                self.prefix_scans.set(self.prefix_scans.get() + 1);
                self.inner.scan_prefix(cf, prefix, visit)
            }

            fn scan_prefix_reverse(
                &self,
                cf: &ColumnFamilyName,
                prefix: &Key,
                visit: &mut ScanVisitor<'_>,
            ) -> Result<(), Error> {
                self.reverse_prefix_scans
                    .set(self.reverse_prefix_scans.get() + 1);
                self.inner.scan_prefix_reverse(cf, prefix, visit)
            }

            fn last_with_prefix(
                &self,
                cf: &ColumnFamilyName,
                prefix: &Key,
            ) -> Result<Option<KeyValue>, Error> {
                self.last_with_prefix_calls
                    .set(self.last_with_prefix_calls.get() + 1);
                self.inner.last_with_prefix(cf, prefix)
            }

            fn write_many(&self, operations: &[WriteOperation<'_>]) -> Result<(), Error> {
                self.inner.write_many(operations)
            }
        }

        let storage = CountingStorage::new(MemoryStorage::new(&["indices"]));
        storage.set("indices", b"user:1", b"base-1").unwrap();
        storage.set("indices", b"user:2", b"base-2").unwrap();
        let staged = RefCell::new(vec![
            OwnedWriteOperation::Set {
                cf: "indices".to_owned(),
                key: b"user:3".to_vec(),
                value: b"staged-3".to_vec(),
            },
            OwnedWriteOperation::Set {
                cf: "indices".to_owned(),
                key: b"user:0".to_vec(),
                value: b"staged-0".to_vec(),
            },
        ]);
        let overlay = StagedWriteOverlay::new(&storage, &staged);

        assert_eq!(
            overlay.last_with_prefix("indices", b"user:").unwrap(),
            Some((b"user:3".to_vec(), b"staged-3".to_vec()))
        );
        assert_eq!(storage.last_with_prefix_calls.get(), 1);
        assert_eq!(storage.prefix_scans.get(), 0);
        assert_eq!(storage.reverse_prefix_scans.get(), 0);
    }

    #[test]
    fn memory_storage_write_many_validates_column_families_before_writing() {
        let storage = MemoryStorage::new(&["records"]);
        let error = storage
            .write_many(&[
                WriteOperation::set("records", b"1", b"record"),
                WriteOperation::set("missing", b"2", b"nope"),
            ])
            .unwrap_err();

        assert!(matches!(error, Error::ColumnFamilyNotFound(_)));
        assert_eq!(storage.get("records", b"1").unwrap(), None);
    }

    #[test]
    fn memory_storage_conforms_to_order_and_atomic_batch_contract() {
        let storage = MemoryStorage::new(&["records"]);
        conformance::persistence_order_and_batch_atomicity(storage);
    }

    #[test]
    fn memory_storage_reopen_adds_column_families_without_losing_data() {
        let storage = MemoryStorage::new(&["records"]);
        conformance::reopen_preserves_data_and_adds_families(storage);
    }

    #[test]
    fn record_store_writes_and_reads_typed_records() {
        let temp_dir = tempfile::tempdir().unwrap();
        let storage = RocksDbStorage::open(temp_dir.path(), &["records"]).unwrap();
        let descriptor = RecordDescriptor::new([("id", ValueType::U64)]);
        let store = RecordStore::new(&storage, "records", &descriptor);
        let key = b"1".as_slice();
        let record = descriptor.create(&[Value::U64(42)]).unwrap();
        let op = store.set(key, &record);

        storage.write_many(&[op]).unwrap();

        let stored = store.get(key).unwrap().unwrap();
        assert_eq!(stored.get_idx(0).unwrap(), Value::U64(42));
    }
}
