//! BTree-backed ordered key/value storage.

use std::cell::RefCell;
use std::collections::BTreeSet;
use std::rc::Rc;

#[cfg(target_arch = "wasm32")]
use opfs_btree::OpfsFile;
#[cfg(not(target_arch = "wasm32"))]
use opfs_btree::StdFile;
pub use opfs_btree::SyncPolicy as BtreeSyncPolicy;
use opfs_btree::{BTreeOptions, OpfsBTree, SyncFile};

use super::{
    ColumnFamilyName, Error, Key, OrderedKvStorage, ScanVisitor, Value, WriteOperation,
    apply_storage_delta, key_codec,
};

#[derive(Clone)]
pub struct BtreeStorage<F: SyncFile> {
    tree: Rc<RefCell<OpfsBTree<F>>>,
    column_families: Rc<RefCell<BTreeSet<String>>>,
}

#[cfg(target_arch = "wasm32")]
pub type OpfsStorage = BtreeStorage<OpfsFile>;

#[cfg(not(target_arch = "wasm32"))]
pub type NativeBtreeStorage = BtreeStorage<StdFile>;

fn browser_fidelity_options() -> BTreeOptions {
    BTreeOptions {
        pin_internal_pages: true,
        read_coalesce_pages: 4,
        ..Default::default()
    }
}

fn browser_fidelity_options_with_sync_policy(sync_policy: BtreeSyncPolicy) -> BTreeOptions {
    BTreeOptions {
        sync_policy,
        ..browser_fidelity_options()
    }
}

impl<F> BtreeStorage<F>
where
    F: SyncFile,
{
    pub fn from_file(file: F, column_families: &[&str]) -> Result<Self, Error> {
        let tree = OpfsBTree::open(file, browser_fidelity_options())?;
        Self::from_tree(tree, column_families)
    }

    fn from_tree(tree: OpfsBTree<F>, column_families: &[&str]) -> Result<Self, Error> {
        Ok(Self {
            tree: Rc::new(RefCell::new(tree)),
            column_families: Rc::new(RefCell::new(
                column_families.iter().map(|cf| (*cf).to_owned()).collect(),
            )),
        })
    }

    fn ensure_cf(&self, cf: &ColumnFamilyName) -> Result<(), Error> {
        if self.column_families.borrow().contains(cf) {
            Ok(())
        } else {
            Err(Error::ColumnFamilyNotFound(cf.to_owned()))
        }
    }

    fn encoded_key(&self, cf: &ColumnFamilyName, key: &Key) -> Result<Vec<u8>, Error> {
        self.ensure_cf(cf)?;
        key_codec::encode_column_family_key(cf, key)
    }

    fn prevalidate_write_many(&self, operations: &[WriteOperation<'_>]) -> Result<(), Error> {
        let column_families = self.column_families.borrow();
        for operation in operations {
            let cf = match operation {
                WriteOperation::Set { cf, .. }
                | WriteOperation::Delete { cf, .. }
                | WriteOperation::Delta { cf, .. } => *cf,
            };
            if !column_families.contains(cf) {
                return Err(Error::ColumnFamilyNotFound(cf.to_owned()));
            }
        }
        Ok(())
    }
}

#[cfg(not(target_arch = "wasm32"))]
impl NativeBtreeStorage {
    /// Open the native BTree backend at `path`.
    ///
    /// This uses the same opfs-btree layout and options as the browser OPFS
    /// backend, but stores the tree in a native [`StdFile`]. It is intended for
    /// native harnesses that need browser-storage fidelity rather than RocksDB
    /// tuning.
    pub fn open(
        path: impl AsRef<std::path::Path>,
        column_families: &[&str],
    ) -> Result<Self, Error> {
        Self::from_file(StdFile::open(path)?, column_families)
    }

    pub fn open_with_sync_policy(
        path: impl AsRef<std::path::Path>,
        column_families: &[&str],
        sync_policy: BtreeSyncPolicy,
    ) -> Result<Self, Error> {
        let tree = OpfsBTree::open(
            StdFile::open(path)?,
            browser_fidelity_options_with_sync_policy(sync_policy),
        )?;
        Self::from_tree(tree, column_families)
    }
}

#[cfg(target_arch = "wasm32")]
impl OpfsStorage {
    pub async fn open(namespace: &str, column_families: &[&str]) -> Result<Self, Error> {
        let file = OpfsFile::open(namespace).await?;
        let tree = OpfsBTree::open(
            file,
            browser_fidelity_options_with_sync_policy(BtreeSyncPolicy::OnClose),
        )?;
        Self::from_tree(tree, column_families)
    }

    pub async fn destroy(namespace: &str) -> Result<(), Error> {
        Ok(OpfsFile::destroy(namespace).await?)
    }
}

impl<F> OrderedKvStorage for BtreeStorage<F>
where
    F: SyncFile,
{
    fn get(&self, cf: &ColumnFamilyName, key: &Key) -> Result<Option<Value>, Error> {
        let key = self.encoded_key(cf, key)?;
        Ok(self.tree.borrow_mut().get(&key)?)
    }

    fn set(&self, cf: &ColumnFamilyName, key: &Key, value: &[u8]) -> Result<(), Error> {
        let key = self.encoded_key(cf, key)?;
        Ok(self.tree.borrow_mut().put(&key, value)?)
    }

    fn delete(&self, cf: &ColumnFamilyName, key: &Key) -> Result<(), Error> {
        let key = self.encoded_key(cf, key)?;
        Ok(self.tree.borrow_mut().delete(&key)?)
    }

    fn close(&self) -> Result<(), Error> {
        Ok(self.tree.borrow_mut().close()?)
    }

    fn scan_range(
        &self,
        cf: &ColumnFamilyName,
        start: &Key,
        end: &Key,
        visit: &mut ScanVisitor<'_>,
    ) -> Result<(), Error> {
        let start = self.encoded_key(cf, start)?;
        let end = self.encoded_key(cf, end)?;
        for (key, value) in self.tree.borrow_mut().range(&start, &end, usize::MAX)? {
            let (_, user_key) = key_codec::decode_column_family_key(&key)?;
            visit(user_key, &value)?;
        }
        Ok(())
    }

    fn scan_prefix(
        &self,
        cf: &ColumnFamilyName,
        prefix: &Key,
        visit: &mut ScanVisitor<'_>,
    ) -> Result<(), Error> {
        let start = self.encoded_key(cf, prefix)?;
        let end = key_codec::prefix_upper_bound(&start).unwrap_or_else(|| vec![0xFF]);
        for (key, value) in self.tree.borrow_mut().range(&start, &end, usize::MAX)? {
            let (_, user_key) = key_codec::decode_column_family_key(&key)?;
            visit(user_key, &value)?;
        }
        Ok(())
    }

    fn scan_prefix_reverse(
        &self,
        cf: &ColumnFamilyName,
        prefix: &Key,
        visit: &mut ScanVisitor<'_>,
    ) -> Result<(), Error> {
        let start = self.encoded_key(cf, prefix)?;
        let end = key_codec::prefix_upper_bound(&start).unwrap_or_else(|| vec![0xFF]);
        for (key, value) in self
            .tree
            .borrow_mut()
            .range_reverse(&start, &end, usize::MAX)?
        {
            let (_, user_key) = key_codec::decode_column_family_key(&key)?;
            visit(user_key, &value)?;
        }
        Ok(())
    }

    fn last_with_prefix_before_or_at(
        &self,
        cf: &ColumnFamilyName,
        prefix: &Key,
        upper: &Key,
    ) -> Result<Option<super::KeyValue>, Error> {
        let start = self.encoded_key(cf, prefix)?;
        let upper_user_key = upper;
        let upper = self.encoded_key(cf, upper_user_key)?;
        let mut end = upper.clone();
        end.push(0);
        for (key, value) in self.tree.borrow_mut().range_reverse(&start, &end, 1)? {
            let (_, user_key) = key_codec::decode_column_family_key(&key)?;
            if user_key.starts_with(prefix) && user_key <= upper_user_key {
                return Ok(Some((user_key.to_vec(), value)));
            }
        }
        Ok(None)
    }

    fn write_many(&self, operations: &[WriteOperation<'_>]) -> Result<(), Error> {
        self.prevalidate_write_many(operations)?;
        let mut encoded_operations = Vec::with_capacity(operations.len());
        for operation in operations {
            encoded_operations.push(match operation {
                WriteOperation::Set { cf, key, value } => {
                    (self.encoded_key(cf, key)?, Some((*value).to_vec()))
                }
                WriteOperation::Delete { cf, key } => (self.encoded_key(cf, key)?, None),
                WriteOperation::Delta { cf, key, delta } => {
                    let key = self.encoded_key(cf, key)?;
                    let existing = self.tree.borrow_mut().get(&key)?;
                    let encoded = delta.encode()?;
                    let merged = apply_storage_delta(existing.as_deref(), &encoded)?;
                    (key, Some(merged))
                }
            });
        }

        let mut tree = self.tree.borrow_mut();
        for (key, value) in encoded_operations {
            if let Some(value) = value {
                tree.put(&key, &value)?;
            } else {
                tree.delete(&key)?;
            }
        }
        Ok(tree.flush_wal()?)
    }
}

impl<F> super::ReopenableStorage for BtreeStorage<F>
where
    F: SyncFile,
{
    fn reopen(self, column_families: &[&str]) -> Result<Self, Error> {
        self.column_families
            .borrow_mut()
            .extend(column_families.iter().map(|cf| (*cf).to_owned()));
        Ok(self)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use opfs_btree::{BTreeOptions, MemoryFile};

    fn memory_storage(column_families: &[&str]) -> BtreeStorage<MemoryFile> {
        BtreeStorage::from_file(MemoryFile::new(), column_families).unwrap()
    }

    #[test]
    fn write_many_prevalidates_column_families_before_writing() {
        let storage = memory_storage(&["records"]);

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
    fn memory_file_conforms_to_order_and_atomic_batch_contract() {
        let storage = memory_storage(&["records"]);

        super::super::conformance::persistence_order_and_batch_atomicity(storage);
    }

    #[test]
    fn memory_file_conforms_to_delta_append_contract() {
        let storage = memory_storage(&["records"]);
        super::super::conformance::delta_append_current_winner_observes_merged_state(storage);
    }

    #[test]
    fn write_many_flushes_replayable_wal_without_eager_checkpoint() {
        // This is intentionally a storage-level test: public database APIs can
        // observe persistence, but not whether browser-fidelity BTree batches
        // use the WAL path instead of rewriting checkpoint pages per batch.
        let file = MemoryFile::new();
        let storage = BtreeStorage::from_file(file.clone(), &["records"]).unwrap();

        storage
            .write_many(&[WriteOperation::set("records", b"1", b"record")])
            .unwrap();

        let checkpoint_len = storage.tree.borrow().checkpoint_state().total_pages
            * BTreeOptions::default().page_size as u64;
        assert!(
            file.len().unwrap() > checkpoint_len,
            "write_many should append a WAL tail rather than checkpoint eagerly",
        );

        let reopened = BtreeStorage::from_file(file.clone(), &["records"]).unwrap();
        assert_eq!(
            reopened.get("records", b"1").unwrap(),
            Some(b"record".to_vec())
        );
        drop(reopened);

        storage.close().unwrap();
        let checkpoint_len = storage.tree.borrow().checkpoint_state().total_pages
            * BTreeOptions::default().page_size as u64;
        assert_eq!(file.len().unwrap(), checkpoint_len);
    }

    #[cfg(not(target_arch = "wasm32"))]
    fn native_storage(column_families: &[&str]) -> NativeBtreeStorage {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.keep().join("native.btree");
        NativeBtreeStorage::open(path, column_families).unwrap()
    }

    #[cfg(not(target_arch = "wasm32"))]
    #[test]
    fn native_btree_conforms_to_order_and_atomic_batch_contract() {
        let storage = native_storage(&["records"]);
        super::super::conformance::persistence_order_and_batch_atomicity(storage);
    }

    #[cfg(not(target_arch = "wasm32"))]
    #[test]
    fn native_btree_conforms_to_delta_append_contract() {
        let storage = native_storage(&["records"]);
        super::super::conformance::delta_append_current_winner_observes_merged_state(storage);
    }

    #[cfg(not(target_arch = "wasm32"))]
    #[test]
    fn native_btree_reopen_adds_column_families_without_losing_data() {
        let storage = native_storage(&["records"]);
        super::super::conformance::reopen_preserves_data_and_adds_families(storage);
    }
}
