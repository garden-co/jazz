//! Deterministically controlled storage used by async and failure-path tests.

use std::cell::RefCell;
use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::future::poll_fn;
use std::rc::Rc;
use std::task::{Poll, Waker};

use super::{
    Error, KeyValue, MemoryStorage, OrderedKvStorage, OwnedWriteOperation, ReadyStorageCursor,
    ReopenableStorage, ScanBounds, ScanDirection, ScanRequest, StorageCursor, StorageFuture,
    StorageScan, Value, WriteManyOutcome,
};

#[derive(Clone, Debug, PartialEq, Eq)]
enum ResidentRegion {
    Range {
        cf: String,
        start: Vec<u8>,
        end: Vec<u8>,
    },
    Prefix {
        cf: String,
        prefix: Vec<u8>,
    },
}

impl ResidentRegion {
    fn column_family(&self) -> &str {
        match self {
            Self::Range { cf, .. } | Self::Prefix { cf, .. } => cf,
        }
    }

    fn contains(&self, cf: &str, key: &[u8]) -> bool {
        match self {
            Self::Range {
                cf: region_cf,
                start,
                end,
            } => region_cf == cf && key >= start.as_slice() && key < end.as_slice(),
            Self::Prefix {
                cf: region_cf,
                prefix,
            } => region_cf == cf && key.starts_with(prefix),
        }
    }

    fn covers(&self, other: &Self) -> bool {
        match (self, other) {
            (
                Self::Range { cf, start, end },
                Self::Range {
                    cf: other_cf,
                    start: other_start,
                    end: other_end,
                },
            ) => cf == other_cf && start <= other_start && end >= other_end,
            (
                Self::Prefix { cf, prefix },
                Self::Prefix {
                    cf: other_cf,
                    prefix: other_prefix,
                },
            ) => cf == other_cf && other_prefix.starts_with(prefix),
            _ => false,
        }
    }
}

#[derive(Default)]
struct ResidentState {
    values: BTreeMap<(String, Vec<u8>), Value>,
    points: BTreeSet<(String, Vec<u8>)>,
    regions: Vec<ResidentRegion>,
}

impl ResidentState {
    fn get(&self, cf: &str, key: &[u8]) -> Option<Option<Value>> {
        (self.points.contains(&(cf.to_owned(), key.to_vec()))
            || self.regions.iter().any(|region| region.contains(cf, key)))
        .then(|| self.values.get(&(cf.to_owned(), key.to_vec())).cloned())
    }

    fn install_point(&mut self, cf: String, key: Vec<u8>, value: Option<Value>) {
        match value {
            Some(value) => {
                self.values.insert((cf.clone(), key.clone()), value);
            }
            None => {
                self.values.remove(&(cf.clone(), key.clone()));
            }
        }
        self.points.insert((cf, key));
    }

    fn install_region(&mut self, region: ResidentRegion, rows: &[KeyValue]) {
        let stale = self
            .values
            .keys()
            .filter(|(cf, key)| region.contains(cf, key))
            .cloned()
            .collect::<Vec<_>>();
        for key in stale {
            self.values.remove(&key);
        }
        for (key, value) in rows {
            let cf = region.column_family().to_owned();
            self.values.insert((cf, key.clone()), value.clone());
        }
        if !self.regions.contains(&region) {
            self.regions.push(region);
        }
    }

    fn rows_for(&self, region: &ResidentRegion, reverse: bool) -> Option<Vec<KeyValue>> {
        let covered = self.regions.iter().any(|resident| resident.covers(region));
        if !covered {
            return None;
        }
        let mut rows: Vec<KeyValue> = match region {
            ResidentRegion::Range { cf, start, end } => self
                .values
                .range((cf.clone(), start.clone())..(cf.clone(), end.clone()))
                .map(|((_, key), value)| (key.clone(), value.clone()))
                .collect(),
            ResidentRegion::Prefix { cf, prefix } => self
                .values
                .range((cf.clone(), prefix.clone())..)
                .take_while(|((resident_cf, key), _)| resident_cf == cf && key.starts_with(prefix))
                .map(|((_, key), value)| (key.clone(), value.clone()))
                .collect(),
        };
        if reverse {
            rows.reverse();
        }
        Some(rows)
    }

    fn invalidate(&mut self, cf: &str, key: &[u8]) {
        self.values.remove(&(cf.to_owned(), key.to_vec()));
        self.points.remove(&(cf.to_owned(), key.to_vec()));
        self.regions.retain(|region| !region.contains(cf, key));
    }

    fn evict_column_family(&mut self, cf: &str) {
        self.values.retain(|(resident_cf, _), _| resident_cf != cf);
        self.points.retain(|(resident_cf, _)| resident_cf != cf);
        self.regions.retain(|region| region.column_family() != cf);
    }

    fn evict_scans(&mut self, cf: &str) {
        self.regions.retain(|region| region.column_family() != cf);
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub enum TestStorageOperation {
    Get,
    Set,
    Delete,
    ScanOpen,
    ScanBatch,
    WriteMany,
    Close,
    SetWriteFlushCadence,
    FlushWriteBoundary,
    ApproximateClassBytes,
    Reopen,
}

struct ControlState {
    yield_before_ready: bool,
    paused: bool,
    paused_operations: BTreeSet<TestStorageOperation>,
    permits: usize,
    observed: Vec<TestStorageOperation>,
    poll_counts: BTreeMap<TestStorageOperation, usize>,
    point_read_count: usize,
    waiters: Vec<Waker>,
    failures: BTreeMap<TestStorageOperation, VecDeque<Error>>,
    definitely_uncommitted_failures: BTreeMap<TestStorageOperation, VecDeque<Error>>,
    lost_write_many_acknowledgements: usize,
}

impl Default for ControlState {
    fn default() -> Self {
        Self {
            yield_before_ready: true,
            paused: false,
            paused_operations: BTreeSet::new(),
            permits: 0,
            observed: Vec::new(),
            poll_counts: BTreeMap::new(),
            point_read_count: 0,
            waiters: Vec::new(),
            failures: BTreeMap::new(),
            definitely_uncommitted_failures: BTreeMap::new(),
            lost_write_many_acknowledgements: 0,
        }
    }
}

/// Controller held by tests independently from the storage under test.
#[derive(Clone, Default)]
pub struct TestStorageControl {
    state: Rc<RefCell<ControlState>>,
}

impl TestStorageControl {
    /// Fail the next selected operation after any configured suspension.
    pub fn fail_next(&self, operation: TestStorageOperation) {
        self.state
            .borrow_mut()
            .failures
            .entry(operation)
            .or_default()
            .push_back(Error::Backend {
                backend: "test",
                message: format!("injected {operation:?} failure"),
            });
    }

    /// Fail a batch before its atomic write begins, proving no commit occurred.
    pub fn fail_next_uncommitted(&self, operation: TestStorageOperation) {
        self.state
            .borrow_mut()
            .definitely_uncommitted_failures
            .entry(operation)
            .or_default()
            .push_back(Error::Backend {
                backend: "test",
                message: format!("injected definitely-uncommitted {operation:?} failure"),
            });
    }

    /// Let the next batch commit, then lose its acknowledgement.
    pub fn lose_next_write_many_acknowledgement(&self) {
        self.state.borrow_mut().lost_write_many_acknowledgements += 1;
    }

    /// Make subsequent storage progress require explicit permits.
    pub fn pause(&self) {
        self.state.borrow_mut().paused = true;
    }

    /// Suspend only the selected operation while all other storage work keeps
    /// completing immediately.
    pub fn pause_on(&self, operation: TestStorageOperation) {
        self.state.borrow_mut().paused_operations.insert(operation);
    }

    pub fn resume_operation(&self, operation: TestStorageOperation) {
        let waiters = {
            let mut state = self.state.borrow_mut();
            state.paused_operations.remove(&operation);
            std::mem::take(&mut state.waiters)
        };
        for waiter in waiters {
            waiter.wake();
        }
    }

    /// Allow one pending or future storage suspension point to proceed.
    pub fn release_one(&self) {
        self.release(1);
    }

    /// Allow `count` pending or future storage suspension points to proceed.
    pub fn release(&self, count: usize) {
        let waiters = {
            let mut state = self.state.borrow_mut();
            state.permits = state.permits.saturating_add(count);
            std::mem::take(&mut state.waiters)
        };
        for waiter in waiters {
            waiter.wake();
        }
    }

    /// Return to immediate completion and wake every pending operation.
    pub fn resume(&self) {
        let waiters = {
            let mut state = self.state.borrow_mut();
            state.paused = false;
            state.paused_operations.clear();
            state.permits = 0;
            std::mem::take(&mut state.waiters)
        };
        for waiter in waiters {
            waiter.wake();
        }
    }

    pub fn observed(&self) -> Vec<TestStorageOperation> {
        self.state.borrow().observed.clone()
    }

    pub fn take_observed(&self) -> Vec<TestStorageOperation> {
        std::mem::take(&mut self.state.borrow_mut().observed)
    }

    /// Number of point reads requested from this storage handle, including
    /// reads satisfied immediately from its retained resident state.
    pub fn point_read_count(&self) -> usize {
        self.state.borrow().point_read_count
    }

    fn record_point_read(&self) {
        self.state.borrow_mut().point_read_count += 1;
    }

    fn take_definitely_uncommitted_failure(
        &self,
        operation: TestStorageOperation,
    ) -> Option<Error> {
        self.state
            .borrow_mut()
            .definitely_uncommitted_failures
            .get_mut(&operation)
            .and_then(VecDeque::pop_front)
    }

    fn take_lost_write_many_acknowledgement(&self) -> bool {
        let mut state = self.state.borrow_mut();
        if state.lost_write_many_acknowledgements == 0 {
            false
        } else {
            state.lost_write_many_acknowledgements -= 1;
            true
        }
    }

    /// Number of times an operation's controlled suspension point was polled.
    ///
    /// Unlike [`Self::observed`], this includes repeated polls of one pending
    /// storage future. It lets callers prove that a synchronous adapter gave a
    /// yielding operation exactly one resident turn rather than spinning it.
    pub fn poll_count(&self, operation: TestStorageOperation) -> usize {
        self.state
            .borrow()
            .poll_counts
            .get(&operation)
            .copied()
            .unwrap_or_default()
    }

    /// Number of controlled storage-future polls across every operation.
    pub fn total_poll_count(&self) -> usize {
        self.state.borrow().poll_counts.values().sum()
    }

    async fn before(&self, operation: TestStorageOperation) -> Result<(), Error> {
        let mut recorded = false;
        let mut yielded = false;
        poll_fn(|cx| {
            let mut state = self.state.borrow_mut();
            *state.poll_counts.entry(operation).or_default() += 1;
            if !recorded {
                state.observed.push(operation);
                recorded = true;
            }
            if state.yield_before_ready && !yielded {
                yielded = true;
                cx.waker().wake_by_ref();
                return Poll::Pending;
            }
            if !state.paused && !state.paused_operations.contains(&operation) {
                return Poll::Ready(());
            }
            if state.permits > 0 {
                state.permits -= 1;
                return Poll::Ready(());
            }
            if !state
                .waiters
                .iter()
                .any(|waiter| waiter.will_wake(cx.waker()))
            {
                state.waiters.push(cx.waker().clone());
            }
            Poll::Pending
        })
        .await;
        match self
            .state
            .borrow_mut()
            .failures
            .get_mut(&operation)
            .and_then(VecDeque::pop_front)
        {
            Some(error) => Err(error),
            None => Ok(()),
        }
    }
}

/// In-memory ordered storage whose suspension points are controlled by tests.
///
/// It implements the production storage contract directly. Cold operations
/// yield once before completing; reads of retained resident data are ready on
/// their first poll. The controller can hold cold suspension points for
/// ordering and failure-path tests.
#[derive(Clone)]
pub struct YieldingStorage<S> {
    inner: S,
    control: TestStorageControl,
    resident: Rc<RefCell<ResidentState>>,
}

pub type TestStorage = YieldingStorage<MemoryStorage>;

impl YieldingStorage<MemoryStorage> {
    pub fn new(column_families: &[&str]) -> Self {
        Self::wrap(MemoryStorage::new(column_families).expect("valid memory storage families"))
    }

    pub fn controlled(column_families: &[&str]) -> (Self, TestStorageControl) {
        let storage = Self::new(column_families);
        (storage.clone(), storage.control.clone())
    }
}

impl<S> YieldingStorage<S> {
    pub fn wrap(inner: S) -> Self {
        Self {
            inner,
            control: TestStorageControl::default(),
            resident: Rc::new(RefCell::new(ResidentState::default())),
        }
    }

    pub fn control(&self) -> TestStorageControl {
        self.control.clone()
    }

    /// Evict all retained read results, making subsequent reads cold again.
    pub fn evict_all(&self) {
        *self.resident.borrow_mut() = ResidentState::default();
    }

    /// Evict retained results for one column family without disturbing
    /// unrelated resident work.
    pub fn evict_column_family(&self, cf: &str) {
        self.resident.borrow_mut().evict_column_family(cf);
    }

    /// Evict complete scan snapshots while retaining individually known
    /// points in the same column family.
    pub fn evict_scans(&self, cf: &str) {
        self.resident.borrow_mut().evict_scans(cf);
    }
}

struct TestStorageCursor<'a> {
    inner: StorageScan<'a>,
    control: TestStorageControl,
    resident: Rc<RefCell<ResidentState>>,
    region: ResidentRegion,
    rows: Vec<KeyValue>,
    remaining: Option<usize>,
}

impl StorageCursor for TestStorageCursor<'_> {
    fn next_batch(&mut self) -> StorageFuture<'_, Result<Option<Vec<KeyValue>>, Error>> {
        Box::pin(async move {
            if matches!(self.remaining, Some(0)) {
                return Ok(None);
            }
            self.control.before(TestStorageOperation::ScanBatch).await?;
            let batch = self.inner.next_batch().await?;
            if let Some(batch) = &batch {
                self.rows.extend(batch.iter().cloned());
                if let Some(remaining) = &mut self.remaining {
                    *remaining = remaining.saturating_sub(batch.len());
                }
            } else {
                self.resident
                    .borrow_mut()
                    .install_region(self.region.clone(), &self.rows);
            }
            Ok(batch)
        })
    }
}

impl<S> OrderedKvStorage for YieldingStorage<S>
where
    S: OrderedKvStorage,
{
    fn get(&self, cf: String, key: Vec<u8>) -> StorageFuture<'_, Result<Option<Value>, Error>> {
        self.control.record_point_read();
        if let Some(value) = self.resident.borrow().get(&cf, &key) {
            return Box::pin(async move { Ok(value) });
        }
        Box::pin(async move {
            self.control.before(TestStorageOperation::Get).await?;
            let value = self.inner.get(cf.clone(), key.clone()).await?;
            self.resident
                .borrow_mut()
                .install_point(cf, key, value.clone());
            Ok(value)
        })
    }

    fn put_if_absent(
        &self,
        cf: String,
        key: Vec<u8>,
        value: Vec<u8>,
    ) -> StorageFuture<'_, Result<Option<Value>, Error>> {
        Box::pin(async move {
            self.control.before(TestStorageOperation::WriteMany).await?;
            let installed = value.clone();
            let existing = self
                .inner
                .put_if_absent(cf.clone(), key.clone(), value)
                .await?;
            self.resident.borrow_mut().install_point(
                cf,
                key,
                Some(existing.clone().unwrap_or(installed)),
            );
            Ok(existing)
        })
    }

    fn compare_and_delete(
        &self,
        cf: String,
        key: Vec<u8>,
        expected: Vec<u8>,
    ) -> StorageFuture<'_, Result<bool, Error>> {
        Box::pin(async move {
            self.control.before(TestStorageOperation::WriteMany).await?;
            let removed = self
                .inner
                .compare_and_delete(cf.clone(), key.clone(), expected)
                .await?;
            self.resident.borrow_mut().invalidate(&cf, &key);
            Ok(removed)
        })
    }

    fn set(
        &self,
        cf: String,
        key: Vec<u8>,
        value: Vec<u8>,
    ) -> StorageFuture<'_, Result<(), Error>> {
        Box::pin(async move {
            self.control.before(TestStorageOperation::Set).await?;
            self.inner
                .set(cf.clone(), key.clone(), value.clone())
                .await?;
            self.resident
                .borrow_mut()
                .install_point(cf, key, Some(value));
            Ok(())
        })
    }

    fn delete(&self, cf: String, key: Vec<u8>) -> StorageFuture<'_, Result<(), Error>> {
        Box::pin(async move {
            self.control.before(TestStorageOperation::Delete).await?;
            self.inner.delete(cf.clone(), key.clone()).await?;
            self.resident.borrow_mut().install_point(cf, key, None);
            Ok(())
        })
    }

    fn close(&self) -> StorageFuture<'_, Result<(), Error>> {
        Box::pin(async move {
            self.control.before(TestStorageOperation::Close).await?;
            self.inner.close().await
        })
    }

    fn set_write_flush_cadence(&self, every: usize) -> StorageFuture<'_, Result<(), Error>> {
        Box::pin(async move {
            self.control
                .before(TestStorageOperation::SetWriteFlushCadence)
                .await?;
            self.inner.set_write_flush_cadence(every).await
        })
    }

    fn flush_write_boundary(&self) -> StorageFuture<'_, Result<(), Error>> {
        Box::pin(async move {
            self.control
                .before(TestStorageOperation::FlushWriteBoundary)
                .await?;
            self.inner.flush_write_boundary().await
        })
    }

    fn approximate_class_bytes(&self, cf: String) -> StorageFuture<'_, Result<Option<u64>, Error>> {
        Box::pin(async move {
            self.control
                .before(TestStorageOperation::ApproximateClassBytes)
                .await?;
            self.inner.approximate_class_bytes(cf).await
        })
    }

    fn scan(&self, request: ScanRequest) -> StorageFuture<'_, Result<StorageScan<'_>, Error>> {
        let remaining = request.max_items;
        let region = match &request.bounds {
            ScanBounds::Range { start, end } => ResidentRegion::Range {
                cf: request.cf.clone(),
                start: start.clone(),
                end: end.clone(),
            },
            ScanBounds::Prefix(prefix) => ResidentRegion::Prefix {
                cf: request.cf.clone(),
                prefix: prefix.clone(),
            },
        };
        if let Some(mut rows) = self
            .resident
            .borrow()
            .rows_for(&region, request.direction == ScanDirection::Reverse)
        {
            if let Some(max_items) = request.max_items {
                rows.truncate(max_items);
            }
            return Box::pin(async move {
                Ok(Box::new(ReadyStorageCursor::new(rows)) as StorageScan<'_>)
            });
        }
        Box::pin(async move {
            self.control.before(TestStorageOperation::ScanOpen).await?;
            let inner = self.inner.scan(request).await?;
            Ok(Box::new(TestStorageCursor {
                inner,
                control: self.control.clone(),
                resident: Rc::clone(&self.resident),
                region,
                rows: Vec::new(),
                remaining,
            }) as StorageScan<'_>)
        })
    }

    fn write_many(
        &self,
        operations: Vec<OwnedWriteOperation>,
    ) -> StorageFuture<'_, Result<(), Error>> {
        Box::pin(async move {
            self.control.before(TestStorageOperation::WriteMany).await?;
            self.inner.write_many(operations.clone()).await?;
            let mut resident = self.resident.borrow_mut();
            for operation in operations {
                match operation {
                    OwnedWriteOperation::Set { cf, key, value } => {
                        resident.install_point(cf, key, Some(value));
                    }
                    OwnedWriteOperation::Delete { cf, key } => {
                        resident.install_point(cf, key, None);
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
            if let Err(error) = self.control.before(TestStorageOperation::WriteMany).await {
                return WriteManyOutcome::PossiblyCommitted(error);
            }
            if let Some(error) = self
                .control
                .take_definitely_uncommitted_failure(TestStorageOperation::WriteMany)
            {
                return WriteManyOutcome::Uncommitted(error);
            }
            match self.inner.write_many_outcome(operations.clone()).await {
                WriteManyOutcome::Committed => {
                    let mut resident = self.resident.borrow_mut();
                    for operation in operations {
                        match operation {
                            OwnedWriteOperation::Set { cf, key, value } => {
                                resident.install_point(cf, key, Some(value));
                            }
                            OwnedWriteOperation::Delete { cf, key } => {
                                resident.install_point(cf, key, None);
                            }
                        }
                    }
                    if self.control.take_lost_write_many_acknowledgement() {
                        WriteManyOutcome::PossiblyCommitted(Error::Backend {
                            backend: "test",
                            message: "injected acknowledgement loss after write_many commit"
                                .to_owned(),
                        })
                    } else {
                        WriteManyOutcome::Committed
                    }
                }
                WriteManyOutcome::Uncommitted(error) => WriteManyOutcome::Uncommitted(error),
                WriteManyOutcome::PossiblyCommitted(error) => {
                    WriteManyOutcome::PossiblyCommitted(error)
                }
            }
        })
    }

    fn column_family_names(&self) -> Option<Vec<String>> {
        self.inner.column_family_names()
    }
}

impl<S> ReopenableStorage for YieldingStorage<S>
where
    S: ReopenableStorage + 'static,
{
    fn reopen(self, column_families: Vec<String>) -> StorageFuture<'static, Result<Self, Error>> {
        Box::pin(async move {
            self.control.before(TestStorageOperation::Reopen).await?;
            Ok(Self {
                inner: self.inner.reopen(column_families).await?,
                control: self.control,
                resident: Rc::new(RefCell::new(ResidentState::default())),
            })
        })
    }
}
