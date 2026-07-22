//! SlateDB-backed [`AsyncOrderedKvStorage`] implementation (prototype).
//!
//! SlateDB is an async LSM engine over `object_store`; this backend runs it on
//! a `LocalFileSystem` store rooted at a local directory. SlateDB has no native
//! column families, so families are flattened into one ordered keyspace with
//! the same [`key_codec`] prefix scheme the opfs-btree backend uses. Writes use
//! `await_durable: false` so each batch does not stall on the WAL flush
//! interval; `close` flushes before shutting down. Delta operations are applied
//! read-modify-write against pre-batch state via [`apply_storage_delta`],
//! matching the memory and btree backends.

use std::collections::BTreeSet;
use std::path::PathBuf;
use std::sync::Arc;

use ::slatedb::config::{PutOptions, WriteOptions};
use ::slatedb::object_store::ObjectStore;
use ::slatedb::object_store::local::LocalFileSystem;

use super::key_codec;
use super::{
    AsyncOrderedKvStorage, Error, KeyValue, OwnedWriteOperation, SyncBridgeStorage,
    apply_storage_delta,
};

impl From<::slatedb::Error> for Error {
    fn from(err: ::slatedb::Error) -> Self {
        Error::SlateDb(Box::new(err))
    }
}

pub struct SlateDbStorage {
    db: ::slatedb::Db,
    column_families: BTreeSet<String>,
}

fn write_options() -> WriteOptions {
    WriteOptions {
        await_durable: false,
        ..WriteOptions::default()
    }
}

impl SlateDbStorage {
    /// Open (or create) a SlateDB database rooted at `path`.
    pub async fn open(path: impl Into<PathBuf>, column_families: &[&str]) -> Result<Self, Error> {
        let path = path.into();
        std::fs::create_dir_all(&path)
            .map_err(|err| Error::SlateDbBackend(format!("create data dir: {err}")))?;
        let object_store: Arc<dyn ObjectStore> = Arc::new(
            LocalFileSystem::new_with_prefix(&path)
                .map_err(|err| Error::SlateDbBackend(format!("open local object store: {err}")))?,
        );
        let db = ::slatedb::Db::builder("groove", object_store)
            .build()
            .await?;
        Ok(Self {
            db,
            column_families: column_families.iter().map(|cf| (*cf).to_owned()).collect(),
        })
    }

    /// Open behind the sync bridge, yielding a storage that satisfies the
    /// engine's synchronous [`OrderedKvStorage`] seam.
    ///
    /// [`OrderedKvStorage`]: super::OrderedKvStorage
    pub fn open_bridged(
        path: impl Into<PathBuf>,
        column_families: &[&str],
    ) -> Result<SyncBridgeStorage, Error> {
        let path = path.into();
        let column_families = column_families
            .iter()
            .map(|cf| (*cf).to_owned())
            .collect::<Vec<_>>();
        SyncBridgeStorage::open(move || async move {
            let refs = column_families
                .iter()
                .map(String::as_str)
                .collect::<Vec<_>>();
            Self::open(path, &refs).await
        })
    }

    fn ensure_cf(&self, cf: &str) -> Result<(), Error> {
        if self.column_families.contains(cf) {
            Ok(())
        } else {
            Err(Error::ColumnFamilyNotFound(cf.to_owned()))
        }
    }

    fn encoded_key(&self, cf: &str, key: &[u8]) -> Result<Vec<u8>, Error> {
        self.ensure_cf(cf)?;
        key_codec::encode_column_family_key(cf, key)
    }

    async fn collect_encoded_range(
        &self,
        start: Vec<u8>,
        end: Vec<u8>,
    ) -> Result<Vec<KeyValue>, Error> {
        if start >= end {
            // slatedb panics on logically empty scan ranges; equal-bound scans
            // are legal at this seam and must yield nothing.
            return Ok(Vec::new());
        }
        let mut entries = Vec::new();
        let mut iterator = self.db.scan(start..end).await?;
        while let Some(kv) = iterator.next().await? {
            let (_, user_key) = key_codec::decode_column_family_key(&kv.key)?;
            entries.push((user_key.to_vec(), kv.value.to_vec()));
        }
        Ok(entries)
    }
}

impl AsyncOrderedKvStorage for SlateDbStorage {
    async fn get(&self, cf: &str, key: &[u8]) -> Result<Option<Vec<u8>>, Error> {
        let key = self.encoded_key(cf, key)?;
        Ok(self.db.get(&key).await?.map(|value| value.to_vec()))
    }

    async fn set(&self, cf: &str, key: &[u8], value: &[u8]) -> Result<(), Error> {
        let key = self.encoded_key(cf, key)?;
        self.db
            .put_with_options(&key, value, &PutOptions::default(), &write_options())
            .await?;
        Ok(())
    }

    async fn delete(&self, cf: &str, key: &[u8]) -> Result<(), Error> {
        let key = self.encoded_key(cf, key)?;
        self.db.delete_with_options(&key, &write_options()).await?;
        Ok(())
    }

    async fn scan_range(&self, cf: &str, start: &[u8], end: &[u8]) -> Result<Vec<KeyValue>, Error> {
        let start = self.encoded_key(cf, start)?;
        let end = self.encoded_key(cf, end)?;
        self.collect_encoded_range(start, end).await
    }

    async fn scan_prefix(&self, cf: &str, prefix: &[u8]) -> Result<Vec<KeyValue>, Error> {
        let start = self.encoded_key(cf, prefix)?;
        let end = key_codec::prefix_upper_bound(&start).unwrap_or_else(|| vec![0xFF]);
        self.collect_encoded_range(start, end).await
    }

    async fn write_many(&self, operations: Vec<OwnedWriteOperation>) -> Result<(), Error> {
        for operation in &operations {
            let cf = match operation {
                OwnedWriteOperation::Set { cf, .. }
                | OwnedWriteOperation::Delete { cf, .. }
                | OwnedWriteOperation::Delta { cf, .. } => cf,
            };
            self.ensure_cf(cf)?;
        }
        let mut batch = ::slatedb::WriteBatch::new();
        for operation in &operations {
            match operation {
                OwnedWriteOperation::Set { cf, key, value } => {
                    batch.put(self.encoded_key(cf, key)?, value);
                }
                OwnedWriteOperation::Delete { cf, key } => {
                    batch.delete(self.encoded_key(cf, key)?);
                }
                OwnedWriteOperation::Delta { cf, key, delta } => {
                    // Same semantics as the memory/btree backends: deltas merge
                    // against pre-batch state, not earlier ops in this batch.
                    let encoded_key = self.encoded_key(cf, key)?;
                    let existing = self.db.get(&encoded_key).await?;
                    let merged = apply_storage_delta(existing.as_deref(), &delta.encode()?)?;
                    batch.put(encoded_key, merged);
                }
            }
        }
        self.db.write_with_options(batch, &write_options()).await?;
        Ok(())
    }

    async fn close(&self) -> Result<(), Error> {
        self.db.flush().await?;
        self.db.close().await?;
        Ok(())
    }

    async fn reopen(mut self, column_families: Vec<String>) -> Result<Self, Error> {
        // Column families are virtual key prefixes; expanding the set needs no
        // database reopen (same as the btree backend).
        self.column_families.extend(column_families);
        Ok(self)
    }

    fn column_family_names(&self) -> Option<Vec<String>> {
        Some(self.column_families.iter().cloned().collect())
    }
}

#[cfg(test)]
mod tests {
    // These are intentionally storage-level tests: byte-lexicographic ordering,
    // batch atomicity, and delta-merge semantics at the ordered-KV seam are not
    // meaningfully observable through public database APIs, and every other
    // backend certifies against the same shared conformance harness.
    use std::cell::RefCell;
    use std::collections::BTreeMap;
    use std::ops::Bound;

    use super::super::conformance;
    use super::*;

    fn bridged_storage(column_families: &[&str]) -> SyncBridgeStorage {
        let dir = tempfile::tempdir().unwrap();
        SlateDbStorage::open_bridged(dir.keep(), column_families).unwrap()
    }

    #[test]
    fn slatedb_conforms_to_order_and_atomic_batch_contract() {
        let storage = bridged_storage(&["records"]);
        conformance::persistence_order_and_batch_atomicity(storage);
    }

    #[test]
    fn slatedb_conforms_to_delta_append_contract() {
        let storage = bridged_storage(&["records"]);
        conformance::delta_append_current_winner_observes_merged_state(storage);
    }

    #[test]
    fn slatedb_reopen_adds_column_families_without_losing_data() {
        let storage = bridged_storage(&["records"]);
        conformance::reopen_preserves_data_and_adds_families(storage);
    }

    /// Minimal in-memory async backend so bridge bugs can be told apart from
    /// slatedb bugs.
    struct ShimStorage {
        families: RefCell<BTreeMap<String, BTreeMap<Vec<u8>, Vec<u8>>>>,
    }

    impl ShimStorage {
        fn new(column_families: &[&str]) -> Self {
            Self {
                families: RefCell::new(
                    column_families
                        .iter()
                        .map(|cf| ((*cf).to_owned(), BTreeMap::new()))
                        .collect(),
                ),
            }
        }

        fn ensure_cf(&self, cf: &str) -> Result<(), Error> {
            if self.families.borrow().contains_key(cf) {
                Ok(())
            } else {
                Err(Error::ColumnFamilyNotFound(cf.to_owned()))
            }
        }
    }

    impl AsyncOrderedKvStorage for ShimStorage {
        async fn get(&self, cf: &str, key: &[u8]) -> Result<Option<Vec<u8>>, Error> {
            self.ensure_cf(cf)?;
            Ok(self.families.borrow()[cf].get(key).cloned())
        }

        async fn set(&self, cf: &str, key: &[u8], value: &[u8]) -> Result<(), Error> {
            self.ensure_cf(cf)?;
            self.families
                .borrow_mut()
                .get_mut(cf)
                .unwrap()
                .insert(key.to_vec(), value.to_vec());
            Ok(())
        }

        async fn delete(&self, cf: &str, key: &[u8]) -> Result<(), Error> {
            self.ensure_cf(cf)?;
            self.families.borrow_mut().get_mut(cf).unwrap().remove(key);
            Ok(())
        }

        async fn scan_range(
            &self,
            cf: &str,
            start: &[u8],
            end: &[u8],
        ) -> Result<Vec<KeyValue>, Error> {
            self.ensure_cf(cf)?;
            Ok(self.families.borrow()[cf]
                .range::<[u8], _>((Bound::Included(start), Bound::Excluded(end)))
                .map(|(key, value)| (key.clone(), value.clone()))
                .collect())
        }

        async fn scan_prefix(&self, cf: &str, prefix: &[u8]) -> Result<Vec<KeyValue>, Error> {
            self.ensure_cf(cf)?;
            Ok(self.families.borrow()[cf]
                .range::<[u8], _>((Bound::Included(prefix), Bound::Unbounded))
                .take_while(|(key, _)| key.starts_with(prefix))
                .map(|(key, value)| (key.clone(), value.clone()))
                .collect())
        }

        async fn write_many(&self, operations: Vec<OwnedWriteOperation>) -> Result<(), Error> {
            for operation in &operations {
                let cf = match operation {
                    OwnedWriteOperation::Set { cf, .. }
                    | OwnedWriteOperation::Delete { cf, .. }
                    | OwnedWriteOperation::Delta { cf, .. } => cf,
                };
                self.ensure_cf(cf)?;
            }
            for operation in &operations {
                match operation {
                    OwnedWriteOperation::Set { cf, key, value } => {
                        self.set(cf, key, value).await?;
                    }
                    OwnedWriteOperation::Delete { cf, key } => {
                        self.delete(cf, key).await?;
                    }
                    OwnedWriteOperation::Delta { cf, key, delta } => {
                        let existing = self.get(cf, key).await?;
                        let merged = apply_storage_delta(existing.as_deref(), &delta.encode()?)?;
                        self.set(cf, key, &merged).await?;
                    }
                }
            }
            Ok(())
        }

        async fn close(&self) -> Result<(), Error> {
            Ok(())
        }

        async fn reopen(self, column_families: Vec<String>) -> Result<Self, Error> {
            {
                let mut families = self.families.borrow_mut();
                for cf in column_families {
                    families.entry(cf).or_default();
                }
            }
            Ok(self)
        }

        fn column_family_names(&self) -> Option<Vec<String>> {
            Some(self.families.borrow().keys().cloned().collect())
        }
    }

    fn bridged_shim(column_families: &[&str]) -> SyncBridgeStorage {
        let column_families = column_families
            .iter()
            .map(|cf| (*cf).to_owned())
            .collect::<Vec<_>>();
        SyncBridgeStorage::open(move || async move {
            let refs = column_families
                .iter()
                .map(String::as_str)
                .collect::<Vec<_>>();
            Ok(ShimStorage::new(&refs))
        })
        .unwrap()
    }

    #[test]
    fn bridge_conforms_with_in_memory_async_backend() {
        let storage = bridged_shim(&["records"]);
        conformance::persistence_order_and_batch_atomicity(storage);
    }

    #[test]
    fn bridge_reopen_adds_column_families_without_losing_data() {
        let storage = bridged_shim(&["records"]);
        conformance::reopen_preserves_data_and_adds_families(storage);
    }
}
