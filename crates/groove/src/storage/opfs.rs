//! OPFS-backed ordered key/value storage for browser wasm.

use std::cell::RefCell;
use std::collections::BTreeSet;
use std::rc::Rc;

use opfs_btree::{BTreeOptions, OpfsBTree, OpfsFile};

use super::{
    ColumnFamilyName, Error, Key, OrderedKvStorage, ScanVisitor, Value, WriteOperation,
    apply_storage_delta, key_codec,
};

#[derive(Clone)]
pub struct OpfsStorage {
    tree: Rc<RefCell<OpfsBTree<OpfsFile>>>,
    column_families: Rc<RefCell<BTreeSet<String>>>,
}

impl OpfsStorage {
    pub async fn open(namespace: &str, column_families: &[&str]) -> Result<Self, Error> {
        let file = OpfsFile::open(namespace).await?;
        let tree = OpfsBTree::open(
            file,
            BTreeOptions {
                pin_internal_pages: true,
                read_coalesce_pages: 4,
                ..Default::default()
            },
        )?;
        Ok(Self {
            tree: Rc::new(RefCell::new(tree)),
            column_families: Rc::new(RefCell::new(
                column_families.iter().map(|cf| (*cf).to_owned()).collect(),
            )),
        })
    }

    pub async fn destroy(namespace: &str) -> Result<(), Error> {
        Ok(OpfsFile::destroy(namespace).await?)
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

impl OrderedKvStorage for OpfsStorage {
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
            visit(&user_key, &value)?;
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
            visit(&user_key, &value)?;
        }
        Ok(())
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
                tree.put(&key, value)?;
            } else {
                tree.delete(&key)?;
            }
        }
        Ok(tree.checkpoint()?)
    }
}

#[cfg(all(test, target_arch = "wasm32"))]
mod tests {
    use super::*;

    #[test]
    fn write_many_prevalidates_column_families_before_writing() {
        let storage = OpfsStorage {
            tree: Rc::new(RefCell::new(
                OpfsBTree::open(opfs_btree::MemoryFile::new(), BTreeOptions::default()).unwrap(),
            )),
            column_families: Rc::new(RefCell::new(["records".to_owned()].into())),
        };

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
        let storage = OpfsStorage {
            tree: Rc::new(RefCell::new(
                OpfsBTree::open(opfs_btree::MemoryFile::new(), BTreeOptions::default()).unwrap(),
            )),
            column_families: Rc::new(RefCell::new(["records".to_owned()].into())),
        };

        super::super::conformance::persistence_order_and_batch_atomicity(storage);
    }
}

impl super::ReopenableStorage for OpfsStorage {
    fn reopen(self, column_families: &[&str]) -> Result<Self, Error> {
        self.column_families
            .borrow_mut()
            .extend(column_families.iter().map(|cf| (*cf).to_owned()));
        Ok(self)
    }
}
