//! Async IDBTree adapter for Groove's ordered storage contract.

use std::cell::RefCell;
use std::collections::{BTreeMap, BTreeSet};
use std::rc::Rc;
use std::task::Poll;

use futures::lock::Mutex;
use idb_tree::{IdbTree, Options, PageStore, WriteOperation};

use super::{
    ColumnFamilyName, Error, Key, OrderedKvStorage, OwnedWriteOperation, ReadyStorageCursor,
    ScanBounds, ScanDirection, ScanRequest, StorageFuture, StorageScan, Value, apply_storage_delta,
    key_codec,
};

// A noisy neighbouring tab must not turn a single logical write into an
// unbounded request that holds this handle's mutation gate forever. Eight
// replays accommodates ordinary tab races while keeping the worst case small
// and observable to callers.
const MAX_GENERATION_CONFLICT_RETRIES: usize = 8;
const MAX_CONFLICT_BACKOFF_YIELDS: usize = 16;

#[derive(Clone)]
pub struct IdbStorage<S> {
    tree: Rc<RefCell<IdbTree<S>>>,
    store: S,
    column_families: Rc<RefCell<BTreeSet<String>>>,
    mutation_gate: Rc<Mutex<()>>,
}

impl<S> IdbStorage<S>
where
    S: PageStore + Clone,
{
    pub async fn open(store: S, column_families: &[&str]) -> Result<Self, Error> {
        Ok(Self {
            tree: Rc::new(RefCell::new(
                IdbTree::open(store.clone(), Options::default()).await?,
            )),
            store,
            column_families: Rc::new(RefCell::new(
                column_families.iter().map(|cf| (*cf).to_owned()).collect(),
            )),
            mutation_gate: Rc::new(Mutex::new(())),
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

    fn tree(&self) -> IdbTree<S> {
        self.tree.borrow().clone()
    }

    async fn reopen_after_generation_conflict(&self) -> Result<(), Error> {
        // An independent browser tab owns a distinct IdbTree cache and can
        // commit between our read and flush. Discard this stale cache rather
        // than replaying its dirty pages, then recompute the whole logical
        // batch from the newly durable tree.
        let tree = IdbTree::open(self.store.clone(), Options::default()).await?;
        *self.tree.borrow_mut() = tree;
        Ok(())
    }

    fn is_generation_conflict(error: &Error) -> bool {
        matches!(
            error,
            Error::IdbTree(idb_tree::Error::GenerationConflict(_))
        )
    }

    async fn yield_once() {
        let mut yielded = false;
        futures::future::poll_fn(move |cx| {
            if yielded {
                Poll::Ready(())
            } else {
                yielded = true;
                cx.waker().wake_by_ref();
                Poll::Pending
            }
        })
        .await;
    }

    async fn back_off_after_generation_conflict(retry: usize) {
        // This is intentionally executor-cooperative rather than wall-clock
        // sleeping: IDB is driven by the browser event loop, and yielding lets
        // the winning tab finish without imposing a timer dependency on native
        // test stores. The exponential schedule is capped with the retry cap.
        let yields = (1usize << retry.min(4)).min(MAX_CONFLICT_BACKOFF_YIELDS);
        for _ in 0..yields {
            Self::yield_once().await;
        }
    }

    async fn write_many_once(
        &self,
        tree: &IdbTree<S>,
        operations: &[OwnedWriteOperation],
    ) -> Result<(), Error> {
        // Resolve each key's prospective value before changing the tree so
        // deltas observe earlier operations in this ordered batch.
        let mut planned = BTreeMap::<Vec<u8>, Option<Vec<u8>>>::new();
        for operation in operations {
            match operation {
                OwnedWriteOperation::Set { cf, key, value } => {
                    planned.insert(self.encoded_key(cf, key)?, Some(value.clone()));
                }
                OwnedWriteOperation::Delete { cf, key } => {
                    planned.insert(self.encoded_key(cf, key)?, None);
                }
                OwnedWriteOperation::Delta { cf, key, delta } => {
                    let key = self.encoded_key(cf, key)?;
                    let encoded = delta.encode()?;
                    let value = match planned.get(&key) {
                        Some(existing) => apply_storage_delta(existing.as_deref(), &encoded)?,
                        None => {
                            let existing = tree.get(&key).await?;
                            apply_storage_delta(existing.as_deref(), &encoded)?
                        }
                    };
                    planned.insert(key, value);
                }
            }
        }
        let writes = planned
            .into_iter()
            .map(|(key, value)| match value {
                Some(value) => WriteOperation::Set { key, value },
                None => WriteOperation::Delete { key },
            })
            .collect();
        tree.write_many(writes).await?;
        tree.flush().await?;
        Ok(())
    }

    async fn write_many_replaying_generation_conflicts(
        &self,
        operations: &[OwnedWriteOperation],
    ) -> Result<(), Error> {
        let mut retries = 0;
        loop {
            let tree = self.tree();
            match self.write_many_once(&tree, operations).await {
                Ok(()) => return Ok(()),
                Err(error) if Self::is_generation_conflict(&error) => {
                    if retries == MAX_GENERATION_CONFLICT_RETRIES {
                        // The failed attempt has staged writes in this tree's
                        // cache. Reopen even on the terminal path so a caller
                        // cannot observe a failed, non-durable write through a
                        // later get on this handle.
                        self.reopen_after_generation_conflict().await?;
                        return Err(Error::IdbGenerationContention { retries });
                    }
                    retries += 1;
                    self.reopen_after_generation_conflict().await?;
                    Self::back_off_after_generation_conflict(retries).await;
                }
                Err(error) => return Err(error),
            }
        }
    }
}

impl<S> OrderedKvStorage for IdbStorage<S>
where
    S: PageStore + Clone + 'static,
{
    fn get(&self, cf: String, key: Vec<u8>) -> StorageFuture<'_, Result<Option<Value>, Error>> {
        Box::pin(async move {
            let key = self.encoded_key(&cf, &key)?;
            Ok(self.tree().get(&key).await?)
        })
    }

    fn set(
        &self,
        cf: String,
        key: Vec<u8>,
        value: Vec<u8>,
    ) -> StorageFuture<'_, Result<(), Error>> {
        Box::pin(async move {
            let operations = vec![OwnedWriteOperation::Set { cf, key, value }];
            self.prevalidate_write_many(&operations)?;
            let _guard = self.mutation_gate.lock().await;
            self.write_many_replaying_generation_conflicts(&operations)
                .await
        })
    }

    fn delete(&self, cf: String, key: Vec<u8>) -> StorageFuture<'_, Result<(), Error>> {
        Box::pin(async move {
            let operations = vec![OwnedWriteOperation::Delete { cf, key }];
            self.prevalidate_write_many(&operations)?;
            let _guard = self.mutation_gate.lock().await;
            self.write_many_replaying_generation_conflicts(&operations)
                .await
        })
    }

    fn close(&self) -> StorageFuture<'_, Result<(), Error>> {
        Box::pin(async move {
            let _guard = self.mutation_gate.lock().await;
            self.tree().flush().await?;
            Ok(())
        })
    }

    fn flush_write_boundary(&self) -> StorageFuture<'_, Result<(), Error>> {
        Box::pin(async move {
            let _guard = self.mutation_gate.lock().await;
            self.tree().flush().await?;
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
            let tree = self.tree();
            let rows = match direction {
                ScanDirection::Forward => tree.range_limit(&start, &end, limit).await?,
                ScanDirection::Reverse => tree.range_reverse(&start, &end, limit).await?,
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
                .tree()
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
                .tree()
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
            let _guard = self.mutation_gate.lock().await;
            self.write_many_replaying_generation_conflicts(&operations)
                .await
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
    use std::cell::Cell;
    use std::task::Poll;

    use idb_tree::{BoxFuture, Commit, MemoryPageStore, Metadata};

    use super::super::{CurrentWinnerDelta, MemoryStorage, StorageDelta};
    use super::*;

    #[derive(Clone, Default)]
    struct YieldingCommitPageStore {
        inner: MemoryPageStore,
    }

    impl PageStore for YieldingCommitPageStore {
        fn load_metadata(&self) -> BoxFuture<'_, Result<Option<Metadata>, String>> {
            self.inner.load_metadata()
        }

        fn read_page(&self, page_id: u64) -> BoxFuture<'_, Result<Option<Vec<u8>>, String>> {
            self.inner.read_page(page_id)
        }

        fn commit<'a>(&'a self, commit: &'a Commit) -> BoxFuture<'a, Result<Metadata, String>> {
            Box::pin(async move {
                let mut yielded = false;
                futures::future::poll_fn(move |cx| {
                    if yielded {
                        Poll::Ready(())
                    } else {
                        yielded = true;
                        cx.waker().wake_by_ref();
                        Poll::Pending
                    }
                })
                .await;
                self.inner.commit(commit).await
            })
        }
    }

    #[derive(Clone)]
    struct ConflictInjectingPageStore {
        inner: MemoryPageStore,
        conflicts_remaining: Rc<Cell<usize>>,
    }

    impl ConflictInjectingPageStore {
        fn with_conflicts(conflicts: usize) -> Self {
            Self {
                inner: MemoryPageStore::default(),
                conflicts_remaining: Rc::new(Cell::new(conflicts)),
            }
        }
    }

    impl PageStore for ConflictInjectingPageStore {
        fn load_metadata(&self) -> BoxFuture<'_, Result<Option<Metadata>, String>> {
            self.inner.load_metadata()
        }

        fn read_page(&self, page_id: u64) -> BoxFuture<'_, Result<Option<Vec<u8>>, String>> {
            self.inner.read_page(page_id)
        }

        fn commit<'a>(&'a self, commit: &'a Commit) -> BoxFuture<'a, Result<Metadata, String>> {
            let remaining = self.conflicts_remaining.get();
            if remaining == 0 {
                return self.inner.commit(commit);
            }
            self.conflicts_remaining.set(remaining - 1);
            Box::pin(async {
                Err("generation changed: deterministic injected conflict".to_owned())
            })
        }
    }

    fn winner_record(time: u64, node: u8, payload: &[u8]) -> Vec<u8> {
        let mut record = Vec::with_capacity(24 + payload.len());
        record.extend(time.to_le_bytes());
        record.extend([node; 16]);
        record.extend(payload);
        record
    }

    fn winner_delta(record: Vec<u8>) -> StorageDelta {
        let tx_time = u64::from_le_bytes(record[..8].try_into().unwrap());
        let mut tx_node_uuid = [0; 16];
        tx_node_uuid.copy_from_slice(&record[8..24]);
        StorageDelta::current_winner(CurrentWinnerDelta {
            tx_time,
            tx_node_uuid,
            parents: Vec::new(),
            tx_time_offset: 0,
            tx_node_uuid_offset: 8,
            record,
        })
        .unwrap()
    }

    async fn durable_idb_and_memory_values(
        operations: Vec<OwnedWriteOperation>,
    ) -> (Option<Vec<u8>>, Option<Vec<u8>>) {
        let page_store = MemoryPageStore::default();
        let idb = IdbStorage::open(page_store.clone(), &["records"])
            .await
            .unwrap();
        let memory = MemoryStorage::new(&["records"]);

        memory.write_many(operations.clone()).await.unwrap();
        idb.write_many(operations).await.unwrap();
        drop(idb);

        let reopened = IdbStorage::open(page_store, &["records"]).await.unwrap();
        let key = b"same-key".to_vec();
        (
            reopened.get("records".into(), key.clone()).await.unwrap(),
            memory.get("records".into(), key).await.unwrap(),
        )
    }

    #[test]
    fn set_then_delta_in_one_batch_matches_memory_after_reopen() {
        futures::executor::block_on(async {
            let set_winner = winner_record(20, 1, b"set-winner");
            let delta_loser = winner_record(10, 2, b"delta-loser");
            let (durable, memory) = durable_idb_and_memory_values(vec![
                OwnedWriteOperation::set("records", b"same-key", set_winner.clone()),
                OwnedWriteOperation::delta("records", b"same-key", winner_delta(delta_loser)),
            ])
            .await;

            assert_eq!(memory, Some(set_winner.clone()));
            assert_eq!(durable, memory);
        });
    }

    #[test]
    fn delta_then_delta_in_one_batch_matches_memory_after_reopen() {
        futures::executor::block_on(async {
            let first_winner = winner_record(20, 1, b"first-winner");
            let second_loser = winner_record(10, 2, b"second-loser");
            let (durable, memory) = durable_idb_and_memory_values(vec![
                OwnedWriteOperation::delta(
                    "records",
                    b"same-key",
                    winner_delta(first_winner.clone()),
                ),
                OwnedWriteOperation::delta("records", b"same-key", winner_delta(second_loser)),
            ])
            .await;

            assert_eq!(memory, Some(first_winner.clone()));
            assert_eq!(durable, memory);
        });
    }

    #[test]
    fn overlapping_write_many_calls_are_serialized_across_clones() {
        futures::executor::block_on(async {
            let page_store = YieldingCommitPageStore::default();
            let storage = IdbStorage::open(page_store.clone(), &["records"])
                .await
                .unwrap();
            let memory = MemoryStorage::new(&["records"]);
            let first_winner = winner_record(20, 1, b"first-winner");
            let second_winner = winner_record(30, 2, b"second-winner");
            let first = vec![OwnedWriteOperation::delta(
                "records",
                b"same-key",
                winner_delta(first_winner),
            )];
            let second = vec![OwnedWriteOperation::delta(
                "records",
                b"same-key",
                winner_delta(second_winner),
            )];

            memory.write_many(first.clone()).await.unwrap();
            memory.write_many(second.clone()).await.unwrap();
            let first_storage = storage.clone();
            let second_storage = storage.clone();
            let (first_result, second_result) = futures::join!(
                first_storage.write_many(first),
                second_storage.write_many(second),
            );
            first_result.unwrap();
            second_result.unwrap();
            drop((first_storage, second_storage, storage));

            let reopened = IdbStorage::open(page_store, &["records"]).await.unwrap();
            let key = b"same-key".to_vec();
            assert_eq!(
                reopened.get("records".into(), key.clone()).await.unwrap(),
                memory.get("records".into(), key).await.unwrap(),
            );
        });
    }

    #[test]
    fn independent_handles_retry_conditional_writes_and_preserve_the_first_winner() {
        futures::executor::block_on(async {
            let page_store = MemoryPageStore::default();
            let first = IdbStorage::open(page_store.clone(), &["records"])
                .await
                .unwrap();
            let second = IdbStorage::open(page_store.clone(), &["records"])
                .await
                .unwrap();
            let first_write = first.write_many(vec![OwnedWriteOperation::delta(
                "records",
                b"same-key",
                StorageDelta::set_if_absent(b"first authenticated bytes".to_vec()),
            )]);
            let second_write = second.write_many(vec![OwnedWriteOperation::delta(
                "records",
                b"same-key",
                StorageDelta::set_if_absent(b"second conflicting bytes".to_vec()),
            )]);
            let (first_result, second_result) = futures::join!(first_write, second_write);
            first_result.unwrap();
            second_result.unwrap();

            let observer = IdbStorage::open(page_store, &["records"]).await.unwrap();
            assert_eq!(
                observer
                    .get("records".into(), b"same-key".to_vec())
                    .await
                    .unwrap(),
                Some(b"first authenticated bytes".to_vec())
            );
        });
    }

    #[test]
    fn independent_handles_retry_stale_conditional_delete_against_the_new_mapping() {
        futures::executor::block_on(async {
            let page_store = MemoryPageStore::default();
            let seed = IdbStorage::open(page_store.clone(), &["records"])
                .await
                .unwrap();
            let key = b"same-key".to_vec();
            let old = b"old authenticated bytes".to_vec();
            let new = b"new authenticated bytes".to_vec();
            seed.set("records".into(), key.clone(), old.clone())
                .await
                .unwrap();

            let stale = IdbStorage::open(page_store.clone(), &["records"])
                .await
                .unwrap();
            assert_eq!(
                stale.get("records".into(), key.clone()).await.unwrap(),
                Some(old.clone())
            );
            let replacement = IdbStorage::open(page_store.clone(), &["records"])
                .await
                .unwrap();
            let replacement_write = replacement.write_many(vec![
                OwnedWriteOperation::delete("records", key.clone()),
                OwnedWriteOperation::delta(
                    "records",
                    key.clone(),
                    StorageDelta::set_if_absent(new.clone()),
                ),
            ]);
            let stale_delete = stale.write_many(vec![OwnedWriteOperation::delta(
                "records",
                key.clone(),
                StorageDelta::delete_if_value_matches(old),
            )]);
            let (replacement_result, stale_result) =
                futures::join!(replacement_write, stale_delete);
            replacement_result.unwrap();
            stale_result.unwrap();

            let observer = IdbStorage::open(page_store, &["records"]).await.unwrap();
            assert_eq!(
                observer.get("records".into(), key).await.unwrap(),
                Some(new)
            );
        });
    }

    #[test]
    fn repeated_generation_conflicts_reopen_and_replay_the_logical_write() {
        futures::executor::block_on(async {
            let page_store = ConflictInjectingPageStore::with_conflicts(3);
            let storage = IdbStorage::open(page_store.clone(), &["records"])
                .await
                .unwrap();

            storage
                .set(
                    "records".into(),
                    b"replayed-key".to_vec(),
                    b"replayed-value".to_vec(),
                )
                .await
                .unwrap();
            assert_eq!(page_store.conflicts_remaining.get(), 0);
            assert_eq!(
                storage
                    .get("records".into(), b"replayed-key".to_vec())
                    .await
                    .unwrap(),
                Some(b"replayed-value".to_vec())
            );
        });
    }

    #[test]
    fn repeated_generation_conflicts_stop_at_the_retry_cap_without_leaking_writes() {
        futures::executor::block_on(async {
            let page_store =
                ConflictInjectingPageStore::with_conflicts(MAX_GENERATION_CONFLICT_RETRIES + 1);
            let storage = IdbStorage::open(page_store, &["records"]).await.unwrap();

            let error = storage
                .set(
                    "records".into(),
                    b"failed-key".to_vec(),
                    b"failed-value".to_vec(),
                )
                .await
                .expect_err("the conflict cap must return to the caller");
            assert!(matches!(
                error,
                Error::IdbGenerationContention {
                    retries: MAX_GENERATION_CONFLICT_RETRIES
                }
            ));
            assert_eq!(
                storage
                    .get("records".into(), b"failed-key".to_vec())
                    .await
                    .unwrap(),
                None,
                "the stale cache from the final failed attempt must be discarded"
            );
        });
    }

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
            super::super::conformance::conditional_delete_delta_matches_the_durable_value(
                storage.clone(),
            )
            .await;
            super::super::conformance::former_rocksdb_tombstone_bytes_remain_an_ordinary_value(
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
