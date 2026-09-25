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

/// A write future dropped part-way (a cancelled task, a torn-down page) may
/// leave staged writes, or a commit of unknown outcome, in the tree. Force the
/// next operation to reload from the store instead of serving or committing
/// that state. Errors returned normally are handled by the caller.
struct ResetIfCancelled<'a>(Option<&'a Cell<bool>>);

impl<'a> ResetIfCancelled<'a> {
    fn arm(needs_reset: &'a Cell<bool>) -> Self {
        Self(Some(needs_reset))
    }

    fn disarm(mut self) {
        self.0 = None;
    }
}

impl Drop for ResetIfCancelled<'_> {
    fn drop(&mut self) {
        if let Some(needs_reset) = self.0.take() {
            needs_reset.set(true);
        }
    }
}

#[derive(Clone)]
pub struct IdbStorage<S> {
    tree: Rc<RefCell<IdbTree<S>>>,
    column_families: Rc<RefCell<BTreeSet<String>>>,
    mutation_gate: Rc<Mutex<()>>,
    needs_reset: Rc<Cell<bool>>,
    tree_epoch: Rc<Cell<u64>>,
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
            column_families: Rc::new(RefCell::new(
                column_families.iter().map(|cf| (*cf).to_owned()).collect(),
            )),
            mutation_gate: Rc::new(Mutex::new(())),
            needs_reset: Rc::new(Cell::new(false)),
            tree_epoch: Rc::new(Cell::new(0)),
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

    // A retained query future may be polled once and then parked until its
    // owner gets another turn. It must not keep the mutation gate while cold
    // page I/O is pending: that owner may first need a different storage read.
    // Hydrate without the gate, discard that speculative result, and retry
    // against the current tree under the gate. Only a resident, serialized
    // attempt may return a value, including after a failed writer reset.
    //
    // While a writer holds the gate (staging a batch or awaiting its IndexedDB
    // commit), a read does not wait for it: it answers read-committed from
    // the last durable generation, whose pages stay resident until the next
    // commit lands. Writes still in flight are not committed, so they are not
    // visible; read-your-writes within a transaction comes from the caller's
    // staged-write overlay. Only when that generation is not resident, or the
    // tree needs a reset, does the read queue behind the writer as before.
    async fn read_resident<T, F, R>(&self, read: F) -> Result<T, Error>
    where
        F: Fn(IdbTree<S>) -> R,
        R: std::future::Future<Output = Result<T, idb_tree::Error>>,
    {
        loop {
            if self.mutation_gate.try_lock().is_none()
                && !self.needs_reset.get()
                && let Some(value) = Self::poll_read_committed(&read, &self.tree())
            {
                return Ok(value);
            }
            let guard = self.mutation_gate.lock().await;
            self.ensure_ready().await?;
            let epoch = self.tree_epoch.get();
            let mut pending = std::pin::pin!(read(self.tree()));
            let attempt = std::future::poll_fn(|cx| Poll::Ready(pending.as_mut().poll(cx))).await;
            match attempt {
                Poll::Ready(result) => return result.map_err(Error::from),
                Poll::Pending => {
                    drop(guard);
                    if let Err(error) = pending.await {
                        let _guard = self.mutation_gate.lock().await;
                        self.ensure_ready().await?;
                        if self.tree_epoch.get() == epoch {
                            return Err(error.into());
                        }
                    }
                }
            }
        }
    }

    /// One synchronous attempt against the committed generation. Any miss or
    /// error falls back to the serialized path, which reports it if real.
    fn poll_read_committed<T, F, R>(read: &F, tree: &IdbTree<S>) -> Option<T>
    where
        F: Fn(IdbTree<S>) -> R,
        R: std::future::Future<Output = Result<T, idb_tree::Error>>,
    {
        let mut pending = std::pin::pin!(read(tree.read_committed()));
        let waker = futures::task::noop_waker();
        let mut cx = std::task::Context::from_waker(&waker);
        match pending.as_mut().poll(&mut cx) {
            Poll::Ready(Ok(value)) => Some(value),
            Poll::Ready(Err(_)) | Poll::Pending => None,
        }
    }

    async fn reopen_after_generation_conflict(&self) -> Result<(), Error> {
        // An independent browser tab owns a distinct IdbTree cache and can
        // commit between our read and flush. Discard this stale cache rather
        // than replaying its dirty pages, then recompute the whole logical
        // batch from the newly durable tree.
        self.tree().reload().await?;
        self.tree_epoch.set(self.tree_epoch.get().wrapping_add(1));
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
        let cancelled = ResetIfCancelled::arm(&self.needs_reset);
        tree.write_many(writes).await?;
        tree.flush().await?;
        cancelled.disarm();
        Ok(())
    }

    async fn flush_tree(&self) -> Result<(), Error> {
        let cancelled = ResetIfCancelled::arm(&self.needs_reset);
        let result = self.tree().flush().await;
        cancelled.disarm();
        match result {
            Ok(()) => Ok(()),
            Err(error) => Err(self.discard_failed_tree(error.into()).await),
        }
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
    fn compare_value(
        &self,
        cf: String,
        key: Vec<u8>,
        expected: Vec<u8>,
    ) -> StorageFuture<'_, Result<super::ValueComparison, Error>> {
        Box::pin(async move {
            let key = self.encoded_key(&cf, &key)?;
            let result = self
                .read_resident(|tree| {
                    let (key, expected) = (key.clone(), expected.clone());
                    async move { tree.value_equals(&key, &expected).await }
                })
                .await?;
            Ok(match result {
                None => super::ValueComparison::Absent,
                Some(true) => super::ValueComparison::Identical,
                Some(false) => super::ValueComparison::Different,
            })
        })
    }

    fn get(&self, cf: String, key: Vec<u8>) -> StorageFuture<'_, Result<Option<Value>, Error>> {
        Box::pin(async move {
            let key = self.encoded_key(&cf, &key)?;
            self.read_resident(|tree| {
                let key = key.clone();
                async move { tree.get(&key).await }
            })
            .await
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
            self.flush_tree().await
        })
    }

    fn flush_write_boundary(&self) -> StorageFuture<'_, Result<(), Error>> {
        Box::pin(async move {
            let _guard = self.mutation_gate.lock().await;
            self.ensure_ready().await?;
            self.flush_tree().await
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
            if max_items == Some(0) || bounds.is_empty_range() {
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
            let rows = self
                .read_resident(|tree| {
                    let (start, end) = (start.clone(), end.clone());
                    async move {
                        match direction {
                            ScanDirection::Forward => tree.range_limit(&start, &end, limit).await,
                            ScanDirection::Reverse => tree.range_reverse(&start, &end, limit).await,
                        }
                    }
                })
                .await?;
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
            let end = super::prefix_successor(&start).unwrap_or_else(|| vec![0xff]);
            let row = self
                .read_resident(|tree| {
                    let (start, end) = (start.clone(), end.clone());
                    async move { tree.range_reverse(&start, &end, 1).await }
                })
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
                .read_resident(|tree| {
                    let (start, end) = (start.clone(), end.clone());
                    async move { tree.range_reverse(&start, &end, 1).await }
                })
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
        pause_next_read:
            Rc<RefCell<Option<futures::channel::oneshot::Receiver<Result<(), String>>>>>,
        pause_next_commit:
            Rc<RefCell<Option<futures::channel::oneshot::Receiver<Result<(), String>>>>>,
        /// Like IndexedDB, the next commit lands in the store even if the
        /// caller stops waiting for its acknowledgement.
        pause_after_next_commit:
            Rc<RefCell<Option<futures::channel::oneshot::Receiver<Result<(), String>>>>>,
    }

    impl PageStore for CommitErrorPageStore {
        fn load_metadata(&self) -> BoxFuture<'_, Result<Option<Metadata>, String>> {
            if self.fail_next_reopen.replace(false) {
                return Box::pin(async { Err("deterministic reset failure".to_owned()) });
            }
            self.inner.load_metadata()
        }

        fn read_page(&self, page_id: u64) -> BoxFuture<'_, Result<Option<Vec<u8>>, String>> {
            let pause = self.pause_next_read.borrow_mut().take();
            Box::pin(async move {
                if let Some(pause) = pause {
                    pause
                        .await
                        .map_err(|_| "read pause cancelled".to_owned())??;
                }
                self.inner.read_page(page_id).await
            })
        }

        fn commit<'a>(&'a self, commit: &'a Commit) -> BoxFuture<'a, Result<Metadata, String>> {
            if self.fail_next_commit.replace(false) {
                return Box::pin(async { Err("deterministic commit failure".to_owned()) });
            }
            let pause = self.pause_next_commit.borrow_mut().take();
            Box::pin(async move {
                if let Some(pause) = pause {
                    pause
                        .await
                        .map_err(|_| "commit pause cancelled".to_owned())??;
                }
                let committed = self.inner.commit(commit).await;
                let acknowledge = self.pause_after_next_commit.borrow_mut().take();
                if let Some(acknowledge) = acknowledge {
                    acknowledge
                        .await
                        .map_err(|_| "acknowledgement cancelled".to_owned())??;
                }
                committed
            })
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

    /// A retained cold query must not block a second storage read or writer.
    /// This storage-level receipt controls a single page future, a scheduling
    /// boundary which cannot be asserted deterministically through a server.
    #[test]
    fn parked_cold_reads_release_the_gate_and_recheck_after_a_write() {
        futures::executor::block_on(async {
            for read_kind in 0..5 {
                let pages = CommitErrorPageStore::default();
                let writer = IdbStorage::open(pages.clone(), &["records"]).await.unwrap();
                writer
                    .set("records".into(), b"key".to_vec(), b"before".to_vec())
                    .await
                    .unwrap();
                let storage = IdbStorage::open(pages.clone(), &["records"]).await.unwrap();
                let (release, paused) = futures::channel::oneshot::channel();
                *pages.pause_next_read.borrow_mut() = Some(paused);
                let mut first = Box::pin(async {
                    match read_kind {
                        0 => storage.get("records".into(), b"key".to_vec()).await,
                        1 | 2 => {
                            let mut request = ScanRequest::prefix("records".into(), b"k".to_vec());
                            if read_kind == 2 {
                                request.direction = ScanDirection::Reverse;
                            }
                            let mut scan = storage.scan(request).await?;
                            Ok(scan
                                .next_batch()
                                .await?
                                .and_then(|rows| rows.into_iter().next().map(|(_, value)| value)))
                        }
                        3 => Ok(storage
                            .last_with_prefix("records".into(), b"k".to_vec())
                            .await?
                            .map(|(_, value)| value)),
                        _ => Ok(storage
                            .last_with_prefix_before_or_at(
                                "records".into(),
                                b"k".to_vec(),
                                b"key".to_vec(),
                            )
                            .await?
                            .map(|(_, value)| value)),
                    }
                });
                assert!(futures::poll!(first.as_mut()).is_pending());
                let mut second = storage.get("records".into(), b"key".to_vec());
                assert!(
                    matches!(futures::poll!(second.as_mut()), Poll::Ready(Ok(Some(value))) if value == b"before"),
                    "read {read_kind} retained the gate while parked"
                );
                storage
                    .set("records".into(), b"key".to_vec(), b"after".to_vec())
                    .await
                    .unwrap();
                release.send(Ok(())).unwrap();
                assert_eq!(first.await.unwrap(), Some(b"after".to_vec()));
            }
        });
    }

    /// A resident read issued while a writer awaits its IndexedDB commit must
    /// neither wait for that commit nor see the uncommitted write: it reads
    /// the last committed generation. Internal because commit interleaving is
    /// a storage-adapter boundary no client API can pause deterministically.
    #[test]
    fn resident_reads_during_a_commit_read_committed_without_waiting() {
        futures::executor::block_on(async {
            for commit_succeeds in [true, false] {
                let pages = CommitErrorPageStore::default();
                let storage = IdbStorage::open(pages.clone(), &["records"]).await.unwrap();
                storage
                    .set("records".into(), b"key".to_vec(), b"before".to_vec())
                    .await
                    .unwrap();

                let (release, paused) = futures::channel::oneshot::channel();
                *pages.pause_next_commit.borrow_mut() = Some(paused);
                let mut write = Box::pin(storage.write_many(vec![
                    OwnedWriteOperation::Set {
                        cf: "records".into(),
                        key: b"key".to_vec(),
                        value: b"after".to_vec(),
                    },
                    OwnedWriteOperation::Set {
                        cf: "records".into(),
                        key: b"new".to_vec(),
                        value: b"staged".to_vec(),
                    },
                ]));
                assert!(futures::poll!(write.as_mut()).is_pending());

                let mut get = storage.get("records".into(), b"key".to_vec());
                assert!(
                    matches!(futures::poll!(get.as_mut()), Poll::Ready(Ok(Some(value))) if value == b"before"),
                    "a resident read waited for, or saw, the in-flight commit"
                );
                let mut absent = storage.get("records".into(), b"new".to_vec());
                assert!(matches!(
                    futures::poll!(absent.as_mut()),
                    Poll::Ready(Ok(None))
                ));
                let mut scan = Box::pin(async {
                    storage
                        .scan(ScanRequest::prefix("records".into(), Vec::new()))
                        .await?
                        .next_batch()
                        .await
                });
                assert!(matches!(
                    futures::poll!(scan.as_mut()),
                    Poll::Ready(Ok(Some(rows))) if rows == vec![(b"key".to_vec(), b"before".to_vec())]
                ));

                if commit_succeeds {
                    release.send(Ok(())).unwrap();
                    write.await.unwrap();
                    assert_eq!(
                        storage
                            .get("records".into(), b"key".to_vec())
                            .await
                            .unwrap(),
                        Some(b"after".to_vec())
                    );
                    assert_eq!(
                        storage
                            .get("records".into(), b"new".to_vec())
                            .await
                            .unwrap(),
                        Some(b"staged".to_vec())
                    );
                } else {
                    release.send(Err("disk unavailable".into())).unwrap();
                    assert!(write.await.is_err());
                    assert_eq!(
                        storage
                            .get("records".into(), b"key".to_vec())
                            .await
                            .unwrap(),
                        Some(b"before".to_vec())
                    );
                    assert_eq!(
                        storage
                            .get("records".into(), b"new".to_vec())
                            .await
                            .unwrap(),
                        None
                    );
                }
            }
        });
    }

    /// A write future dropped while its IndexedDB commit is pending (a
    /// cancelled task, a torn-down page) must not leave its uncommitted rows
    /// readable or wedge later writes. Whether that commit landed is decided
    /// by the store alone.
    #[test]
    fn a_write_cancelled_mid_commit_is_never_read_uncommitted_and_storage_recovers() {
        futures::executor::block_on(async {
            for lands_anyway in [false, true] {
                let pages = CommitErrorPageStore::default();
                let storage = IdbStorage::open(pages.clone(), &["records"]).await.unwrap();
                storage
                    .set("records".into(), b"key".to_vec(), b"before".to_vec())
                    .await
                    .unwrap();

                let (_hold, paused) = futures::channel::oneshot::channel();
                if lands_anyway {
                    *pages.pause_after_next_commit.borrow_mut() = Some(paused);
                } else {
                    *pages.pause_next_commit.borrow_mut() = Some(paused);
                }
                let mut write = Box::pin(storage.write_many(vec![
                    OwnedWriteOperation::Set {
                        cf: "records".into(),
                        key: b"key".to_vec(),
                        value: b"after".to_vec(),
                    },
                    OwnedWriteOperation::Set {
                        cf: "records".into(),
                        key: b"new".to_vec(),
                        value: b"staged".to_vec(),
                    },
                ]));
                assert!(futures::poll!(write.as_mut()).is_pending());
                drop(write);

                let (key, new): (&[u8], Option<&[u8]>) = if lands_anyway {
                    (b"after", Some(b"staged"))
                } else {
                    (b"before", None)
                };
                let get = |key: &'static [u8]| storage.get("records".into(), key.to_vec());
                assert_eq!(get(b"key").await.unwrap().as_deref(), Some(key));
                assert_eq!(get(b"new").await.unwrap().as_deref(), new);

                storage
                    .set("records".into(), b"later".to_vec(), b"write".to_vec())
                    .await
                    .unwrap();
                let reopened = IdbStorage::open(pages.clone(), &["records"]).await.unwrap();
                let rows = reopened
                    .scan(ScanRequest::prefix("records".into(), Vec::new()))
                    .await
                    .unwrap()
                    .next_batch()
                    .await
                    .unwrap()
                    .unwrap();
                let mut expected = vec![
                    (b"key".to_vec(), key.to_vec()),
                    (b"later".to_vec(), b"write".to_vec()),
                ];
                if let Some(new) = new {
                    expected.push((b"new".to_vec(), new.to_vec()));
                }
                assert_eq!(rows, expected, "lands_anyway={lands_anyway}");
            }
        });
    }

    /// A failed writer replaces its dirty tree while a cold read is parked.
    /// The old read's late failure must not poison a healthy durable retry.
    #[test]
    fn cold_read_retries_after_a_failed_writer_replaces_its_tree() {
        futures::executor::block_on(async {
            let pages = CommitErrorPageStore::default();
            let writer = IdbStorage::open(pages.clone(), &["records"]).await.unwrap();
            writer
                .set("records".into(), b"key".to_vec(), b"durable".to_vec())
                .await
                .unwrap();
            let storage = IdbStorage::open(pages.clone(), &["records"]).await.unwrap();
            let (release, paused) = futures::channel::oneshot::channel();
            *pages.pause_next_read.borrow_mut() = Some(paused);
            let mut read = storage.get("records".into(), b"key".to_vec());
            assert!(futures::poll!(read.as_mut()).is_pending());
            pages.fail_next_commit.set(true);
            assert!(
                storage
                    .set("records".into(), b"key".to_vec(), b"uncommitted".to_vec())
                    .await
                    .is_err()
            );
            release.send(Err("old tree page failed".into())).unwrap();
            assert_eq!(read.await.unwrap(), Some(b"durable".to_vec()));
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
    // public schema/query surface. MemoryPageStore exercises the IDB adapter
    // protocol, not a browser IndexedDB/OPFS physical-store receipt (#2160).
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
