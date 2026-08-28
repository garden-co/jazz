//! Async IDBTree adapter for Groove's ordered storage contract.

use std::cell::{Cell, RefCell};
use std::collections::BTreeSet;
use std::rc::Rc;
use std::task::Poll;

use futures::lock::Mutex;
use idb_tree::{IdbTree, Options, PageStore, WriteOperation};

use super::{
    ColumnFamilyName, Error, Key, OrderedKvStorage, OwnedWriteOperation, ReadyStorageCursor,
    ScanBounds, ScanDirection, ScanRequest, StorageFuture, StorageScan, Value, WriteManyOutcome,
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
    needs_reset: Rc<Cell<bool>>,
}

impl<S> IdbStorage<S>
where
    S: PageStore + Clone,
{
    pub async fn open(store: S, column_families: &[&str]) -> Result<Self, Error> {
        super::validate_physical_storage_names(column_families)?;
        Ok(Self {
            tree: Rc::new(RefCell::new(
                IdbTree::open(store.clone(), Options::default()).await?,
            )),
            store,
            column_families: Rc::new(RefCell::new(
                column_families.iter().map(|cf| (*cf).to_owned()).collect(),
            )),
            mutation_gate: Rc::new(Mutex::new(())),
            needs_reset: Rc::new(Cell::new(false)),
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
                OwnedWriteOperation::Set { cf, .. } | OwnedWriteOperation::Delete { cf, .. } => cf,
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
        self.needs_reset.set(false);
        Ok(())
    }

    async fn ensure_ready(&self) -> Result<(), Error> {
        if self.needs_reset.get() {
            self.reopen_after_generation_conflict().await?;
        }
        Ok(())
    }

    async fn discard_failed_tree(&self, error: Error) -> Error {
        self.needs_reset.set(true);
        match self.reopen_after_generation_conflict().await {
            Ok(()) => error,
            Err(reset_error) => reset_error,
        }
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
        let writes = operations
            .iter()
            .map(|operation| match operation {
                OwnedWriteOperation::Set { cf, key, value } => Ok(WriteOperation::Set {
                    key: self.encoded_key(cf, key)?,
                    value: value.clone(),
                }),
                OwnedWriteOperation::Delete { cf, key } => Ok(WriteOperation::Delete {
                    key: self.encoded_key(cf, key)?,
                }),
            })
            .collect::<Result<Vec<_>, Error>>()?;
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
                Err(error) => return Err(self.discard_failed_tree(error).await),
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
            let _guard = self.mutation_gate.lock().await;
            self.ensure_ready().await?;
            let key = self.encoded_key(&cf, &key)?;
            Ok(self.tree().get(&key).await?)
        })
    }

    fn put_if_absent(
        &self,
        cf: String,
        key: Vec<u8>,
        value: Vec<u8>,
    ) -> StorageFuture<'_, Result<Option<Value>, Error>> {
        Box::pin(async move {
            self.ensure_cf(&cf)?;
            let _guard = self.mutation_gate.lock().await;
            self.ensure_ready().await?;
            let encoded_key = self.encoded_key(&cf, &key)?;
            for retry in 0..=MAX_GENERATION_CONFLICT_RETRIES {
                let tree = self.tree();
                if let Some(existing) = tree.get(&encoded_key).await? {
                    return Ok(Some(existing));
                }
                let operations = vec![OwnedWriteOperation::Set {
                    cf: cf.clone(),
                    key: key.clone(),
                    value: value.clone(),
                }];
                match self.write_many_once(&tree, &operations).await {
                    Ok(()) => return Ok(None),
                    Err(error) if Self::is_generation_conflict(&error) => {
                        self.reopen_after_generation_conflict().await?;
                        if retry == MAX_GENERATION_CONFLICT_RETRIES {
                            return Err(Error::IdbGenerationContention {
                                retries: MAX_GENERATION_CONFLICT_RETRIES,
                            });
                        }
                        Self::back_off_after_generation_conflict(retry).await;
                    }
                    Err(error) => return Err(self.discard_failed_tree(error).await),
                }
            }
            unreachable!("bounded retry loop returns")
        })
    }

    fn compare_and_delete(
        &self,
        cf: String,
        key: Vec<u8>,
        expected: Vec<u8>,
    ) -> StorageFuture<'_, Result<bool, Error>> {
        Box::pin(async move {
            self.ensure_cf(&cf)?;
            let _guard = self.mutation_gate.lock().await;
            self.ensure_ready().await?;
            let encoded_key = self.encoded_key(&cf, &key)?;
            for retry in 0..=MAX_GENERATION_CONFLICT_RETRIES {
                let tree = self.tree();
                if tree.get(&encoded_key).await?.as_deref() != Some(expected.as_slice()) {
                    return Ok(false);
                }
                let operations = vec![OwnedWriteOperation::Delete {
                    cf: cf.clone(),
                    key: key.clone(),
                }];
                match self.write_many_once(&tree, &operations).await {
                    Ok(()) => return Ok(true),
                    Err(error) if Self::is_generation_conflict(&error) => {
                        self.reopen_after_generation_conflict().await?;
                        if retry == MAX_GENERATION_CONFLICT_RETRIES {
                            return Err(Error::IdbGenerationContention {
                                retries: MAX_GENERATION_CONFLICT_RETRIES,
                            });
                        }
                        Self::back_off_after_generation_conflict(retry).await;
                    }
                    Err(error) => return Err(self.discard_failed_tree(error).await),
                }
            }
            unreachable!("bounded retry loop returns")
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
            self.ensure_ready().await?;
            self.write_many_replaying_generation_conflicts(&operations)
                .await
        })
    }

    fn delete(&self, cf: String, key: Vec<u8>) -> StorageFuture<'_, Result<(), Error>> {
        Box::pin(async move {
            let operations = vec![OwnedWriteOperation::Delete { cf, key }];
            self.prevalidate_write_many(&operations)?;
            let _guard = self.mutation_gate.lock().await;
            self.ensure_ready().await?;
            self.write_many_replaying_generation_conflicts(&operations)
                .await
        })
    }

    fn close(&self) -> StorageFuture<'_, Result<(), Error>> {
        Box::pin(async move {
            let _guard = self.mutation_gate.lock().await;
            self.ensure_ready().await?;
            match self.tree().flush().await {
                Ok(()) => Ok(()),
                Err(error) => Err(self.discard_failed_tree(error.into()).await),
            }
        })
    }

    fn flush_write_boundary(&self) -> StorageFuture<'_, Result<(), Error>> {
        Box::pin(async move {
            let _guard = self.mutation_gate.lock().await;
            self.ensure_ready().await?;
            match self.tree().flush().await {
                Ok(()) => Ok(()),
                Err(error) => Err(self.discard_failed_tree(error.into()).await),
            }
        })
    }

    fn scan(&self, request: ScanRequest) -> StorageFuture<'_, Result<StorageScan<'_>, Error>> {
        Box::pin(async move {
            let _guard = self.mutation_gate.lock().await;
            self.ensure_ready().await?;
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
                    let end = super::prefix_successor(&start).unwrap_or_else(|| vec![0xff]);
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
            let _guard = self.mutation_gate.lock().await;
            self.ensure_ready().await?;
            let start = self.encoded_key(&cf, &prefix)?;
            let end = super::prefix_successor(&start).unwrap_or_else(|| vec![0xff]);
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
            let _guard = self.mutation_gate.lock().await;
            self.ensure_ready().await?;
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
            self.ensure_ready().await?;
            self.write_many_replaying_generation_conflicts(&operations)
                .await
        })
    }

    fn write_many_outcome(
        &self,
        operations: Vec<OwnedWriteOperation>,
    ) -> StorageFuture<'_, WriteManyOutcome> {
        Box::pin(async move {
            if let Err(error) = self.prevalidate_write_many(&operations) {
                return WriteManyOutcome::Uncommitted(error);
            }
            match self.write_many(operations).await {
                Ok(()) => WriteManyOutcome::Committed,
                Err(error) => WriteManyOutcome::PossiblyCommitted(error),
            }
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
            super::validate_physical_storage_names(&column_families)?;
            self.column_families.borrow_mut().extend(column_families);
            Ok(self)
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use idb_tree::{BoxFuture, Commit, MemoryPageStore, Metadata};

    #[futures_test::test]
    async fn open_rejects_nonportable_physical_name_before_tree_open() {
        assert!(
            IdbStorage::open(MemoryPageStore::default(), &["records\0evil"])
                .await
                .is_err()
        );
    }

    #[derive(Clone)]
    struct ConflictInjectingPageStore {
        inner: MemoryPageStore,
        conflicts_remaining: Rc<Cell<usize>>,
    }

    #[derive(Clone, Default)]
    struct CommitErrorPageStore {
        inner: MemoryPageStore,
        fail_next_commit: Rc<Cell<bool>>,
        fail_next_reopen: Rc<Cell<bool>>,
    }

    impl PageStore for CommitErrorPageStore {
        fn load_metadata(&self) -> BoxFuture<'_, Result<Option<Metadata>, String>> {
            if self.fail_next_reopen.replace(false) {
                return Box::pin(async { Err("deterministic reset failure".to_owned()) });
            }
            self.inner.load_metadata()
        }

        fn read_page(&self, page_id: u64) -> BoxFuture<'_, Result<Option<Vec<u8>>, String>> {
            self.inner.read_page(page_id)
        }

        fn commit<'a>(&'a self, commit: &'a Commit) -> BoxFuture<'a, Result<Metadata, String>> {
            if self.fail_next_commit.replace(false) {
                return Box::pin(async { Err("deterministic commit failure".to_owned()) });
            }
            self.inner.commit(commit)
        }
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
    fn generic_commit_error_discards_dirty_pages_before_later_writes() {
        futures::executor::block_on(async {
            let pages = CommitErrorPageStore::default();
            let storage = IdbStorage::open(pages.clone(), &["records"]).await.unwrap();
            storage
                .set("records".into(), b"key".to_vec(), b"old".to_vec())
                .await
                .unwrap();
            pages.fail_next_commit.set(true);
            let error = storage
                .set("records".into(), b"key".to_vec(), b"failed".to_vec())
                .await
                .unwrap_err();
            assert!(error.to_string().contains("deterministic commit failure"));
            assert_eq!(
                storage
                    .get("records".into(), b"key".to_vec())
                    .await
                    .unwrap(),
                Some(b"old".to_vec())
            );
            let fresh = IdbStorage::open(pages.clone(), &["records"]).await.unwrap();
            assert_eq!(
                fresh.get("records".into(), b"key".to_vec()).await.unwrap(),
                Some(b"old".to_vec())
            );
            storage
                .set("records".into(), b"later".to_vec(), b"ok".to_vec())
                .await
                .unwrap();
            let reopened = IdbStorage::open(pages, &["records"]).await.unwrap();
            assert_eq!(
                reopened
                    .get("records".into(), b"key".to_vec())
                    .await
                    .unwrap(),
                Some(b"old".to_vec())
            );
            assert_eq!(
                reopened
                    .get("records".into(), b"later".to_vec())
                    .await
                    .unwrap(),
                Some(b"ok".to_vec())
            );
        });
    }

    #[test]
    fn cache_reset_failure_wins_over_the_original_commit_error() {
        futures::executor::block_on(async {
            let pages = CommitErrorPageStore::default();
            let storage = IdbStorage::open(pages.clone(), &["records"]).await.unwrap();
            storage
                .set("records".into(), b"key".to_vec(), b"old".to_vec())
                .await
                .unwrap();
            pages.fail_next_commit.set(true);
            pages.fail_next_reopen.set(true);
            let error = storage
                .set("records".into(), b"key".to_vec(), b"failed".to_vec())
                .await
                .unwrap_err();
            assert!(error.to_string().contains("deterministic reset failure"));
            assert_eq!(
                storage
                    .get("records".into(), b"key".to_vec())
                    .await
                    .unwrap(),
                Some(b"old".to_vec())
            );
            storage
                .set("records".into(), b"later".to_vec(), b"ok".to_vec())
                .await
                .unwrap();
            let fresh = IdbStorage::open(pages, &["records"]).await.unwrap();
            assert_eq!(
                fresh.get("records".into(), b"key".to_vec()).await.unwrap(),
                Some(b"old".to_vec())
            );
            assert_eq!(
                fresh
                    .get("records".into(), b"later".to_vec())
                    .await
                    .unwrap(),
                Some(b"ok".to_vec())
            );
        });
    }

    #[test]
    fn independent_handles_preserve_one_conditional_winner() {
        futures::executor::block_on(async {
            let pages = MemoryPageStore::default();
            let first = IdbStorage::open(pages.clone(), &["records"]).await.unwrap();
            let second = IdbStorage::open(pages.clone(), &["records"]).await.unwrap();
            let (a, b) = futures::join!(
                first.put_if_absent("records".into(), b"locator".to_vec(), b"receipt-a".to_vec(),),
                second.put_if_absent("records".into(), b"locator".to_vec(), b"receipt-b".to_vec(),),
            );
            let a = a.unwrap();
            let b = b.unwrap();
            assert_ne!(a.is_none(), b.is_none(), "exactly one handle installs");
            drop((first, second));
            let reopened = IdbStorage::open(pages, &["records"]).await.unwrap();
            let winner = reopened
                .get("records".into(), b"locator".to_vec())
                .await
                .unwrap()
                .unwrap();
            assert!(winner == b"receipt-a" || winner == b"receipt-b");
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
            super::super::conformance::atomic_conditionals_preserve_winners_and_reject_stale_deletes(
                storage.clone(),
            )
            .await;
            super::super::conformance::invalid_batch_is_proven_uncommitted(storage.clone()).await;
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
