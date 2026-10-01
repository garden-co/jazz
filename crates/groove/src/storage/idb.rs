//! Async IDBTree adapter for Groove's ordered storage contract.

use std::cell::{Cell, RefCell};
use std::collections::BTreeSet;
use std::rc::Rc;
use std::sync::Arc;
use std::task::Poll;

use futures::future::{FutureExt, LocalBoxFuture, Shared};
use futures::lock::{Mutex, MutexGuard};
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

/// An owned mutation job dropped part-way (for example, when the last storage
/// handle is released) may leave staged writes or an unknown commit outcome.
/// Dropping its caller alone does not cancel the job. Force the next operation
/// to reload instead of serving or committing abandoned state. Errors through
/// `?` also drop this guard; failed writes already reload before returning.
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
    state: Rc<IdbState<S>>,
    mutations: Rc<RefCell<MutationDriver>>,
}

// The active job captures only this state, never its slot in MutationDriver.
// Dropping all storage handles therefore also releases a parked commit.
struct IdbState<S> {
    tree: RefCell<IdbTree<S>>,
    column_families: RefCell<BTreeSet<String>>,
    mutation_gate: Arc<Mutex<()>>,
    needs_reset: Cell<bool>,
    tree_epoch: Cell<u64>,
    admission: Option<super::StorageAdmission>,
}

#[derive(Default)]
struct MutationDriver {
    next_id: u64,
    active: Option<(u64, Shared<LocalBoxFuture<'static, ()>>)>,
}

impl<S> IdbStorage<S>
where
    S: PageStore + Clone,
{
    pub async fn open(store: S, column_families: &[&str]) -> Result<Self, Error> {
        Self::open_inner(store, column_families, None, false).await
    }

    pub async fn open_admitted(
        store: S,
        column_families: &[&str],
        admission: super::StorageAdmission,
    ) -> Result<Self, Error> {
        Self::open_inner(store, column_families, Some(admission), false).await
    }

    /// Restricted to the preflight phase; does not initialize an absent tree.
    pub async fn open_read_only(store: S, column_families: &[&str]) -> Result<Self, Error> {
        Self::open_inner(store, column_families, None, true).await
    }

    async fn open_inner(
        store: S,
        column_families: &[&str],
        admission: Option<super::StorageAdmission>,
        read_only: bool,
    ) -> Result<Self, Error> {
        super::validate_physical_storage_names(column_families)?;
        let tree = if read_only {
            IdbTree::open_read_only(store, Options::default()).await?
        } else {
            IdbTree::open(store, Options::default()).await?
        };
        let mut families: BTreeSet<String> =
            column_families.iter().map(|cf| (*cf).to_owned()).collect();
        if read_only {
            let mut start = Vec::new();
            while let Some(key) = tree.next_key(&start).await? {
                let (cf, _) = key_codec::decode_column_family_key(&key)?;
                families.insert(cf.to_owned());
                let prefix = key_codec::encode_column_family_key(cf, &[])?;
                let Some(next) = super::prefix_successor(&prefix) else {
                    break;
                };
                start = next;
            }
        }
        Ok(Self {
            state: Rc::new(IdbState {
                tree: RefCell::new(tree),
                column_families: RefCell::new(families),
                mutation_gate: Arc::new(Mutex::new(())),
                needs_reset: Cell::new(false),
                tree_epoch: Cell::new(0),
                admission,
            }),
            mutations: Rc::default(),
        })
    }
}

impl<S> IdbState<S>
where
    S: PageStore + Clone,
{
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

impl<S> IdbStorage<S>
where
    S: PageStore + Clone + 'static,
{
    fn clear_completed_mutation(&self, id: u64) {
        let mut mutations = self.mutations.borrow_mut();
        if mutations
            .active
            .as_ref()
            .is_some_and(|(active, _)| *active == id)
        {
            mutations.active = None;
        }
    }

    async fn finish_mutations(&self) {
        loop {
            let active = self.mutations.borrow().active.clone();
            let Some((id, completion)) = active else {
                return;
            };
            completion.await;
            self.clear_completed_mutation(id);
        }
    }

    // A retained query may stop polling a journal write while the foreground
    // owner needs another read. Any dependent operation can finish the started
    // mutation, including its flush/reset, without resuming that query.
    async fn mutate<T, F, R>(&self, operation: F) -> Result<T, Error>
    where
        T: 'static,
        F: FnOnce(Rc<IdbState<S>>) -> R + 'static,
        R: Future<Output = Result<T, Error>> + 'static,
    {
        self.finish_mutations().await;
        let guard = Arc::clone(&self.state.mutation_gate).lock_owned().await;
        // Registration is after gate acquisition: cancelling a queued caller
        // must never leave its unstarted operation behind.
        let result = Rc::new(RefCell::new(None));
        let output = Rc::clone(&result);
        let state = Rc::clone(&self.state);
        let completion = async move {
            let outcome = match state.ensure_ready().await {
                Ok(()) => operation(state).await,
                Err(error) => Err(error),
            };
            drop(guard);
            *output.borrow_mut() = Some(outcome);
        }
        .boxed_local()
        .shared();
        let id = {
            let mut mutations = self.mutations.borrow_mut();
            let id = mutations.next_id;
            mutations.next_id = id.wrapping_add(1);
            mutations.active = Some((id, completion.clone()));
            id
        };
        completion.await;
        self.clear_completed_mutation(id);
        result
            .borrow_mut()
            .take()
            .expect("completed mutation has a result")
    }

    async fn read_gate(&self) -> Result<MutexGuard<'_, ()>, Error> {
        self.finish_mutations().await;
        if self.state.needs_reset.get() {
            // A reset can itself suspend. Give it the same cooperative owner
            // rather than parking a read with the mutation gate held.
            self.mutate(|_| async { Ok(()) }).await?;
        }
        Ok(self.state.mutation_gate.lock().await)
    }

    // Cold reads release the gate during speculative hydration, then recheck
    // against the current tree. Only a resident serialized attempt is visible.
    async fn read_resident<T, F, R>(&self, read: F) -> Result<T, Error>
    where
        F: Fn(IdbTree<S>) -> R,
        R: Future<Output = Result<T, idb_tree::Error>>,
    {
        loop {
            let guard = self.read_gate().await?;
            let epoch = self.state.tree_epoch.get();
            let mut pending = std::pin::pin!(read(self.state.tree()));
            let attempt = std::future::poll_fn(|cx| Poll::Ready(pending.as_mut().poll(cx))).await;
            match attempt {
                Poll::Ready(result) => return result.map_err(Error::from),
                Poll::Pending => {
                    drop(guard);
                    if let Err(error) = pending.await {
                        let _guard = self.read_gate().await?;
                        if self.state.tree_epoch.get() == epoch {
                            return Err(error.into());
                        }
                    }
                }
            }
        }
    }
}

impl<S> OrderedKvStorage for IdbStorage<S>
where
    S: PageStore + Clone + 'static,
{
    fn admission(&self) -> Result<super::StorageAdmission, Error> {
        self.state
            .admission
            .clone()
            .ok_or(Error::UnsupportedAdmission)
    }

    fn compare_value(
        &self,
        cf: String,
        key: Vec<u8>,
        expected: Vec<u8>,
    ) -> StorageFuture<'_, Result<super::ValueComparison, Error>> {
        Box::pin(async move {
            let key = self.state.encoded_key(&cf, &key)?;
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
            let key = self.state.encoded_key(&cf, &key)?;
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
            self.state.ensure_cf(&cf)?;
            self.mutate(move |state| async move {
                let encoded_key = state.encoded_key(&cf, &key)?;
                let operations = vec![OwnedWriteOperation::Set { cf, key, value }];
                for retry in 0..=MAX_GENERATION_CONFLICT_RETRIES {
                    let tree = state.tree();
                    if let Some(existing) = tree.get(&encoded_key).await? {
                        return Ok(Some(existing));
                    }
                    match state.write_many_once(&tree, &operations).await {
                        Ok(()) => return Ok(None),
                        Err(error) if IdbState::<S>::is_generation_conflict(&error) => {
                            state.reopen_after_generation_conflict().await?;
                            if retry == MAX_GENERATION_CONFLICT_RETRIES {
                                return Err(Error::IdbGenerationContention {
                                    retries: MAX_GENERATION_CONFLICT_RETRIES,
                                });
                            }
                            IdbState::<S>::back_off_after_generation_conflict(retry).await;
                        }
                        Err(error) => return Err(state.discard_failed_tree(error).await),
                    }
                }
                unreachable!("bounded retry loop returns")
            })
            .await
        })
    }

    fn compare_and_delete(
        &self,
        cf: String,
        key: Vec<u8>,
        expected: Vec<u8>,
    ) -> StorageFuture<'_, Result<bool, Error>> {
        Box::pin(async move {
            self.state.ensure_cf(&cf)?;
            self.mutate(move |state| async move {
                let encoded_key = state.encoded_key(&cf, &key)?;
                let operations = vec![OwnedWriteOperation::Delete { cf, key }];
                for retry in 0..=MAX_GENERATION_CONFLICT_RETRIES {
                    let tree = state.tree();
                    if tree.get(&encoded_key).await?.as_deref() != Some(expected.as_slice()) {
                        return Ok(false);
                    }
                    match state.write_many_once(&tree, &operations).await {
                        Ok(()) => return Ok(true),
                        Err(error) if IdbState::<S>::is_generation_conflict(&error) => {
                            state.reopen_after_generation_conflict().await?;
                            if retry == MAX_GENERATION_CONFLICT_RETRIES {
                                return Err(Error::IdbGenerationContention {
                                    retries: MAX_GENERATION_CONFLICT_RETRIES,
                                });
                            }
                            IdbState::<S>::back_off_after_generation_conflict(retry).await;
                        }
                        Err(error) => return Err(state.discard_failed_tree(error).await),
                    }
                }
                unreachable!("bounded retry loop returns")
            })
            .await
        })
    }

    fn set(
        &self,
        cf: String,
        key: Vec<u8>,
        value: Vec<u8>,
    ) -> StorageFuture<'_, Result<(), Error>> {
        self.write_many(vec![OwnedWriteOperation::Set { cf, key, value }])
    }

    fn delete(&self, cf: String, key: Vec<u8>) -> StorageFuture<'_, Result<(), Error>> {
        self.write_many(vec![OwnedWriteOperation::Delete { cf, key }])
    }

    fn close(&self) -> StorageFuture<'_, Result<(), Error>> {
        self.flush_write_boundary()
    }

    fn flush_write_boundary(&self) -> StorageFuture<'_, Result<(), Error>> {
        Box::pin(self.mutate(|state| async move { state.flush_tree().await }))
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
                self.state.encoded_key(&cf, &[])?;
                return Ok(Box::new(ReadyStorageCursor::new(Vec::new())) as StorageScan<'_>);
            }
            let (start, end) = match bounds {
                ScanBounds::Range { start, end } => (
                    self.state.encoded_key(&cf, &start)?,
                    self.state.encoded_key(&cf, &end)?,
                ),
                ScanBounds::Prefix(prefix) => {
                    let start = self.state.encoded_key(&cf, &prefix)?;
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
            Ok(
                Box::new(ReadyStorageCursor::new(IdbState::<S>::decode_rows(rows)?))
                    as StorageScan<'_>,
            )
        })
    }

    fn last_with_prefix(
        &self,
        cf: String,
        prefix: Vec<u8>,
    ) -> StorageFuture<'_, Result<Option<super::KeyValue>, Error>> {
        Box::pin(async move {
            let start = self.state.encoded_key(&cf, &prefix)?;
            let end = super::prefix_successor(&start).unwrap_or_else(|| vec![0xff]);
            let row = self
                .read_resident(|tree| {
                    let (start, end) = (start.clone(), end.clone());
                    async move { tree.range_reverse(&start, &end, 1).await }
                })
                .await?
                .into_iter()
                .next();
            Ok(IdbState::<S>::decode_rows(row.into_iter().collect())?.pop())
        })
    }

    fn last_with_prefix_before_or_at(
        &self,
        cf: String,
        prefix: Vec<u8>,
        upper: Vec<u8>,
    ) -> StorageFuture<'_, Result<Option<super::KeyValue>, Error>> {
        Box::pin(async move {
            let start = self.state.encoded_key(&cf, &prefix)?;
            let mut end = self.state.encoded_key(&cf, &upper)?;
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
            let decoded = IdbState::<S>::decode_rows(vec![row])?.pop();
            Ok(decoded.filter(|(key, _)| key.starts_with(&prefix) && key <= &upper))
        })
    }

    fn write_many(
        &self,
        operations: Vec<OwnedWriteOperation>,
    ) -> StorageFuture<'_, Result<(), Error>> {
        Box::pin(async move {
            self.state.prevalidate_write_many(&operations)?;
            self.mutate(move |state| async move {
                state
                    .write_many_replaying_generation_conflicts(&operations)
                    .await
            })
            .await
        })
    }

    fn write_many_outcome(
        &self,
        operations: Vec<OwnedWriteOperation>,
    ) -> StorageFuture<'_, WriteManyOutcome> {
        Box::pin(async move {
            if let Err(error) = self.state.prevalidate_write_many(&operations) {
                return WriteManyOutcome::Uncommitted(error);
            }
            match self.write_many(operations).await {
                Ok(()) => WriteManyOutcome::Committed,
                Err(error) => WriteManyOutcome::PossiblyCommitted(error),
            }
        })
    }

    fn column_family_names(&self) -> Option<Vec<String>> {
        Some(
            self.state
                .column_families
                .borrow()
                .iter()
                .cloned()
                .collect(),
        )
    }
}

impl<S> super::ReopenableStorage for IdbStorage<S>
where
    S: PageStore + Clone + 'static,
{
    fn reopen(self, column_families: Vec<String>) -> StorageFuture<'static, Result<Self, Error>> {
        Box::pin(async move {
            super::validate_physical_storage_names(&column_families)?;
            self.state
                .column_families
                .borrow_mut()
                .extend(column_families);
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
            let pause = self.pause_next_commit.borrow_mut().take();
            let fail = self.fail_next_commit.replace(false);
            Box::pin(async move {
                if let Some(pause) = pause {
                    pause
                        .await
                        .map_err(|_| "commit pause cancelled".to_owned())??;
                }
                if fail {
                    return Err("deterministic commit failure".to_owned());
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

    /// A query can retain a journal write while the foreground owner awaits a
    /// different storage read. Do not repoll that original query in this receipt.
    #[futures_test::test]
    async fn foreground_read_finishes_a_parked_idb_mutation() {
        let pages = CommitErrorPageStore::default();
        let storage = IdbStorage::open(pages.clone(), &["records"]).await.unwrap();
        let (release, paused) = futures::channel::oneshot::channel();
        *pages.pause_next_commit.borrow_mut() = Some(paused);
        let mut writer = storage.put_if_absent(
            "records".into(),
            b"journal".to_vec(),
            b"pending-install".to_vec(),
        );
        assert!(futures::poll!(writer.as_mut()).is_pending());
        let mut reader = storage.get("records".into(), b"journal".to_vec());
        assert!(futures::poll!(reader.as_mut()).is_pending());
        release.send(Ok(())).unwrap();
        match futures::poll!(reader.as_mut()) {
            Poll::Ready(Ok(value)) => assert_eq!(value, Some(b"pending-install".to_vec())),
            result => panic!("foreground read did not finish the parked mutation: {result:?}"),
        }
        assert_eq!(writer.await.unwrap(), None);
        let reopened = IdbStorage::open(pages, &["records"]).await.unwrap();
        assert_eq!(
            reopened
                .get("records".into(), b"journal".to_vec())
                .await
                .unwrap(),
            Some(b"pending-install".to_vec())
        );
    }

    #[futures_test::test]
    async fn cancelled_idb_mutations_settle_only_the_started_write() {
        let pages = CommitErrorPageStore::default();
        let storage = IdbStorage::open(pages.clone(), &["records"]).await.unwrap();
        storage
            .set("records".into(), b"key".to_vec(), b"before".to_vec())
            .await
            .unwrap();
        drop(storage.set("records".into(), b"unpolled".to_vec(), b"absent".to_vec()));
        let (release, paused) = futures::channel::oneshot::channel();
        *pages.pause_next_commit.borrow_mut() = Some(paused);
        let mut writer = storage.set("records".into(), b"key".to_vec(), b"after".to_vec());
        assert!(futures::poll!(writer.as_mut()).is_pending());
        let mut queued = storage.set("records".into(), b"queued".to_vec(), b"absent".to_vec());
        assert!(futures::poll!(queued.as_mut()).is_pending());
        drop(queued);
        drop(writer);
        let mut reader = storage.get("records".into(), b"key".to_vec());
        assert!(futures::poll!(reader.as_mut()).is_pending());
        release.send(Ok(())).unwrap();
        match futures::poll!(reader.as_mut()) {
            Poll::Ready(Ok(value)) => assert_eq!(value, Some(b"after".to_vec())),
            result => panic!("cancelled started mutation did not settle: {result:?}"),
        }
        let reopened = IdbStorage::open(pages, &["records"]).await.unwrap();
        assert_eq!(
            reopened
                .get("records".into(), b"key".to_vec())
                .await
                .unwrap(),
            Some(b"after".to_vec())
        );
        assert_eq!(
            reopened
                .get("records".into(), b"queued".to_vec())
                .await
                .unwrap(),
            None
        );
        assert_eq!(
            reopened
                .get("records".into(), b"unpolled".to_vec())
                .await
                .unwrap(),
            None
        );
    }

    #[futures_test::test]
    async fn assisted_idb_failure_preserves_the_owner_error_and_resets_reads() {
        for fail_reset in [false, true] {
            let pages = CommitErrorPageStore::default();
            let storage = IdbStorage::open(pages.clone(), &["records"]).await.unwrap();
            storage
                .set("records".into(), b"key".to_vec(), b"before".to_vec())
                .await
                .unwrap();
            let (release, paused) = futures::channel::oneshot::channel();
            *pages.pause_next_commit.borrow_mut() = Some(paused);
            pages.fail_next_commit.set(true);
            pages.fail_next_reopen.set(fail_reset);
            let mut writer = storage.write_many_outcome(vec![OwnedWriteOperation::Set {
                cf: "records".into(),
                key: b"key".to_vec(),
                value: b"failed".to_vec(),
            }]);
            assert!(futures::poll!(writer.as_mut()).is_pending());
            let mut reader = storage.get("records".into(), b"key".to_vec());
            assert!(futures::poll!(reader.as_mut()).is_pending());
            release.send(Ok(())).unwrap();
            match futures::poll!(reader.as_mut()) {
                Poll::Ready(Ok(value)) => assert_eq!(value, Some(b"before".to_vec())),
                result => panic!("read did not recover from an assisted write failure: {result:?}"),
            }
            let WriteManyOutcome::PossiblyCommitted(error) = writer.await else {
                panic!("failed started writer lost its conservative outcome");
            };
            assert!(error.to_string().contains(if fail_reset {
                "deterministic reset failure"
            } else {
                "deterministic commit failure"
            }));
            let reopened = IdbStorage::open(pages, &["records"]).await.unwrap();
            assert_eq!(
                reopened
                    .get("records".into(), b"key".to_vec())
                    .await
                    .unwrap(),
                Some(b"before".to_vec())
            );
        }
    }

    #[futures_test::test]
    async fn late_idb_owner_cannot_clear_a_newer_mutation() {
        let pages = CommitErrorPageStore::default();
        let storage = IdbStorage::open(pages.clone(), &["records"]).await.unwrap();
        let (release_first, first_pause) = futures::channel::oneshot::channel();
        *pages.pause_next_commit.borrow_mut() = Some(first_pause);
        let mut first = storage.put_if_absent("records".into(), b"key".to_vec(), b"first".to_vec());
        assert!(futures::poll!(first.as_mut()).is_pending());
        release_first.send(Ok(())).unwrap();
        assert_eq!(
            storage
                .get("records".into(), b"key".to_vec())
                .await
                .unwrap(),
            Some(b"first".to_vec())
        );
        let (release_second, second_pause) = futures::channel::oneshot::channel();
        *pages.pause_next_commit.borrow_mut() = Some(second_pause);
        let mut second =
            storage.compare_and_delete("records".into(), b"key".to_vec(), b"first".to_vec());
        assert!(futures::poll!(second.as_mut()).is_pending());
        assert_eq!(first.await.unwrap(), None);
        let mut reader = storage.get("records".into(), b"key".to_vec());
        assert!(futures::poll!(reader.as_mut()).is_pending());
        release_second.send(Ok(())).unwrap();
        match futures::poll!(reader.as_mut()) {
            Poll::Ready(Ok(value)) => assert_eq!(value, None),
            result => panic!("late owner erased the current mutation driver: {result:?}"),
        }
        assert!(second.await.unwrap());
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

    /// Resident point and scan reads help settle a parked writer before
    /// exposing either its complete committed batch or the reset predecessor.
    /// The adapter seam allows deterministic commit success/error interleaving.
    #[test]
    fn resident_reads_finish_a_parked_commit_before_exposing_its_outcome() {
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
                let mut write = storage.write_many(vec![
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
                ]);
                assert!(futures::poll!(write.as_mut()).is_pending());

                let mut get = storage.get("records".into(), b"key".to_vec());
                assert!(futures::poll!(get.as_mut()).is_pending());
                let mut absent = storage.get("records".into(), b"new".to_vec());
                assert!(futures::poll!(absent.as_mut()).is_pending());
                let mut scan = Box::pin(async {
                    storage
                        .scan(ScanRequest::prefix("records".into(), Vec::new()))
                        .await?
                        .next_batch()
                        .await
                });
                assert!(futures::poll!(scan.as_mut()).is_pending());

                release
                    .send(if commit_succeeds {
                        Ok(())
                    } else {
                        Err("disk unavailable".into())
                    })
                    .unwrap();
                let (key, new): (&[u8], Option<&[u8]>) = if commit_succeeds {
                    (b"after", Some(b"staged"))
                } else {
                    (b"before", None)
                };
                // Drive only dependent reads; the initiating writer stays parked.
                assert_eq!(get.await.unwrap().as_deref(), Some(key));
                assert_eq!(absent.await.unwrap().as_deref(), new);
                let mut expected = vec![(b"key".to_vec(), key.to_vec())];
                if let Some(new) = new {
                    expected.push((b"new".to_vec(), new.to_vec()));
                }
                assert_eq!(scan.await.unwrap(), Some(expected));
                match write.await {
                    Ok(()) => assert!(commit_succeeds),
                    Err(error) => {
                        assert!(!commit_succeeds);
                        assert!(error.to_string().contains("disk unavailable"));
                    }
                }
            }
        });
    }

    /// Releasing every handle cancels the owned job and releases its paused I/O.
    /// Reopening observes only store-defined bytes: the old batch before commit,
    /// or the landed batch when its acknowledgement was lost. This adapter seam
    /// controls both outcomes without depending on browser scheduling.
    #[test]
    fn dropping_all_idb_handles_releases_a_parked_job_and_reopens_durable_state() {
        futures::executor::block_on(async {
            for lands_anyway in [false, true] {
                let pages = CommitErrorPageStore::default();
                let storage = IdbStorage::open(pages.clone(), &["records"]).await.unwrap();
                storage
                    .set("records".into(), b"key".to_vec(), b"before".to_vec())
                    .await
                    .unwrap();

                let retained = storage.clone();
                let (release, paused) = futures::channel::oneshot::channel();
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
                drop(storage);
                assert!(
                    !release.is_canceled(),
                    "a surviving handle must retain the job"
                );
                drop(retained);
                assert!(
                    release.is_canceled(),
                    "the last handle must release the job"
                );
                let storage = IdbStorage::open(pages.clone(), &["records"]).await.unwrap();

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
