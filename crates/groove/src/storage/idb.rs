//! Async IDBTree adapter for Groove's ordered storage contract.

use std::cell::RefCell;
use std::collections::BTreeSet;
use std::rc::Rc;

use idb_tree::{IdbTree, Options, PageStore, WriteOperation};

use super::{
    ColumnFamilyName, Error, Key, OrderedKvStorage, OwnedWriteOperation, ReadyStorageCursor,
    ScanBounds, ScanDirection, ScanRequest, StorageFuture, StorageScan, Value, apply_storage_delta,
    key_codec,
};

#[derive(Clone)]
pub struct IdbStorage<S> {
    tree: IdbTree<S>,
    column_families: Rc<RefCell<BTreeSet<String>>>,
}

impl<S> IdbStorage<S>
where
    S: PageStore + Clone,
{
    pub async fn open(store: S, column_families: &[&str]) -> Result<Self, Error> {
        Ok(Self {
            tree: IdbTree::open(store, Options::default()).await?,
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

    fn decode_rows(rows: Vec<idb_tree::KeyValue>) -> Result<Vec<super::KeyValue>, Error> {
        rows.into_iter()
            .map(|(key, value)| {
                let (_, user_key) = key_codec::decode_column_family_key(&key)?;
                Ok((user_key.to_vec(), value))
            })
            .collect()
    }

    fn prevalidate_write_many(&self, operations: &[OwnedWriteOperation]) -> Result<(), Error> {
        for operation in operations {
            let cf = match operation {
                OwnedWriteOperation::Set { cf, .. }
                | OwnedWriteOperation::Delete { cf, .. }
                | OwnedWriteOperation::Delta { cf, .. } => cf,
            };
            self.ensure_cf(cf)?;
        }
        Ok(())
    }
}

impl<S> OrderedKvStorage for IdbStorage<S>
where
    S: PageStore + Clone + 'static,
{
    fn get(&self, cf: String, key: Vec<u8>) -> StorageFuture<'_, Result<Option<Value>, Error>> {
        Box::pin(async move {
            let key = self.encoded_key(&cf, &key)?;
            Ok(self.tree.get(&key).await?)
        })
    }

    fn set(
        &self,
        cf: String,
        key: Vec<u8>,
        value: Vec<u8>,
    ) -> StorageFuture<'_, Result<(), Error>> {
        Box::pin(async move {
            let key = self.encoded_key(&cf, &key)?;
            self.tree.put(key, value).await?;
            self.tree.flush().await?;
            Ok(())
        })
    }

    fn delete(&self, cf: String, key: Vec<u8>) -> StorageFuture<'_, Result<(), Error>> {
        Box::pin(async move {
            let key = self.encoded_key(&cf, &key)?;
            self.tree.delete(&key).await?;
            self.tree.flush().await?;
            Ok(())
        })
    }

    fn close(&self) -> StorageFuture<'_, Result<(), Error>> {
        Box::pin(async move {
            self.tree.flush().await?;
            Ok(())
        })
    }

    fn flush_write_boundary(&self) -> StorageFuture<'_, Result<(), Error>> {
        Box::pin(async move {
            self.tree.flush().await?;
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
            if max_items == Some(0) {
                self.encoded_key(&cf, &[])?;
                return Ok(Box::new(ReadyStorageCursor::new(Vec::new())) as StorageScan<'_>);
            }
            let (start, end) = match bounds {
                ScanBounds::Range { start, end } => {
                    (self.encoded_key(&cf, &start)?, self.encoded_key(&cf, &end)?)
                }
                ScanBounds::Prefix(prefix) => {
                    let start = self.encoded_key(&cf, &prefix)?;
                    let end = key_codec::prefix_upper_bound(&start).unwrap_or_else(|| vec![0xff]);
                    (start, end)
                }
            };
            let limit = max_items.unwrap_or(usize::MAX);
            let rows = match direction {
                ScanDirection::Forward => self.tree.range_limit(&start, &end, limit).await?,
                ScanDirection::Reverse => self.tree.range_reverse(&start, &end, limit).await?,
            };
            Ok(Box::new(ReadyStorageCursor::new(Self::decode_rows(rows)?)) as StorageScan<'_>)
        })
    }

    fn last_with_prefix(
        &self,
        cf: String,
        prefix: Vec<u8>,
    ) -> StorageFuture<'_, Result<Option<super::KeyValue>, Error>> {
        Box::pin(async move {
            let start = self.encoded_key(&cf, &prefix)?;
            let end = key_codec::prefix_upper_bound(&start).unwrap_or_else(|| vec![0xff]);
            let row = self
                .tree
                .range_reverse(&start, &end, 1)
                .await?
                .into_iter()
                .next();
            Ok(Self::decode_rows(row.into_iter().collect())?.pop())
        })
    }

    fn last_with_prefix_before_or_at(
        &self,
        cf: String,
        prefix: Vec<u8>,
        upper: Vec<u8>,
    ) -> StorageFuture<'_, Result<Option<super::KeyValue>, Error>> {
        Box::pin(async move {
            let start = self.encoded_key(&cf, &prefix)?;
            let mut end = self.encoded_key(&cf, &upper)?;
            end.push(0);
            let row = self
                .tree
                .range_reverse(&start, &end, 1)
                .await?
                .into_iter()
                .next();
            let Some(row) = row else {
                return Ok(None);
            };
            let decoded = Self::decode_rows(vec![row])?.pop();
            Ok(decoded.filter(|(key, _)| key.starts_with(&prefix) && key <= &upper))
        })
    }

    fn write_many(
        &self,
        operations: Vec<OwnedWriteOperation>,
    ) -> StorageFuture<'_, Result<(), Error>> {
        Box::pin(async move {
            self.prevalidate_write_many(&operations)?;
            let mut writes = Vec::with_capacity(operations.len());
            for operation in operations {
                writes.push(match operation {
                    OwnedWriteOperation::Set { cf, key, value } => WriteOperation::Set {
                        key: self.encoded_key(&cf, &key)?,
                        value,
                    },
                    OwnedWriteOperation::Delete { cf, key } => WriteOperation::Delete {
                        key: self.encoded_key(&cf, &key)?,
                    },
                    OwnedWriteOperation::Delta { cf, key, delta } => {
                        let key = self.encoded_key(&cf, &key)?;
                        let existing = self.tree.get(&key).await?;
                        let encoded = delta.encode()?;
                        let value = apply_storage_delta(existing.as_deref(), &encoded)?;
                        WriteOperation::Set { key, value }
                    }
                });
            }
            self.tree.write_many(writes).await?;
            self.tree.flush().await?;
            Ok(())
        })
    }

    fn column_family_names(&self) -> Option<Vec<String>> {
        Some(self.column_families.borrow().iter().cloned().collect())
    }
}

impl<S> super::ReopenableStorage for IdbStorage<S>
where
    S: PageStore + Clone + 'static,
{
    fn reopen(self, column_families: Vec<String>) -> StorageFuture<'static, Result<Self, Error>> {
        Box::pin(async move {
            self.column_families.borrow_mut().extend(column_families);
            Ok(self)
        })
    }
}

#[cfg(test)]
mod tests {
    use idb_tree::MemoryPageStore;

    use super::*;

    // Storage-level conformance is intentionally tested here because ordering,
    // atomic encoded batches, and reopen are backend contracts below Jazz's
    // public schema/query surface.
    #[test]
    fn conforms_to_order_atomicity_and_reopen_contracts() {
        futures::executor::block_on(async {
            let storage = IdbStorage::open(MemoryPageStore::default(), &["records"])
                .await
                .unwrap();
            super::super::conformance::persistence_order_and_batch_atomicity(storage.clone()).await;
            super::super::conformance::delta_append_current_winner_observes_merged_state(
                storage.clone(),
            )
            .await;
            super::super::conformance::reopen_preserves_data_and_adds_families(storage).await;
        });
    }

    #[test]
    fn bounded_scan_stops_after_requested_prefix_entries_in_both_directions() {
        futures::executor::block_on(async {
            let storage = IdbStorage::open(MemoryPageStore::default(), &["records"])
                .await
                .unwrap();
            for key in [b"a/1", b"a/2", b"a/3", b"b/1"] {
                storage
                    .set("records".into(), key.to_vec(), key.to_vec())
                    .await
                    .unwrap();
            }
            let forward = super::super::collect_scan(
                storage
                    .scan(ScanRequest::prefix("records".into(), b"a/".to_vec()).with_max_items(2))
                    .await
                    .unwrap(),
            )
            .await
            .unwrap();
            assert_eq!(forward.len(), 2);
            assert_eq!(forward[0].0, b"a/1");
            assert_eq!(forward[1].0, b"a/2");

            let reverse = super::super::collect_scan(
                storage
                    .scan(
                        ScanRequest::prefix("records".into(), b"a/".to_vec())
                            .reversed()
                            .with_max_items(2),
                    )
                    .await
                    .unwrap(),
            )
            .await
            .unwrap();
            assert_eq!(
                reverse.iter().map(|entry| &entry.0).collect::<Vec<_>>(),
                vec![b"a/3", b"a/2"]
            );
        });
    }
}
