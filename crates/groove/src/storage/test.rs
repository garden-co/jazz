//! Deterministically controlled storage used by async and failure-path tests.

use std::cell::RefCell;
use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::future::poll_fn;
use std::rc::Rc;
use std::task::{Poll, Waker};

use super::{
    Error, KeyValue, MemoryStorage, OrderedKvStorage, OwnedWriteOperation, ReadyStorageCursor,
    ReopenableStorage, StorageCursor, StorageFuture, StorageScan, Value,
};

#[derive(Clone, Debug, PartialEq, Eq)]
enum ResidentRegion {
    Point {
        cf: String,
        key: Vec<u8>,
    },
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
    fn contains(&self, cf: &str, key: &[u8]) -> bool {
        match self {
            Self::Point {
                cf: region_cf,
                key: region_key,
            } => region_cf == cf && region_key == key,
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
                Self::Point { cf, key },
                Self::Point {
                    cf: other_cf,
                    key: other_key,
                },
            ) => cf == other_cf && key == other_key,
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
    regions: Vec<ResidentRegion>,
}

impl ResidentState {
    fn get(&self, cf: &str, key: &[u8]) -> Option<Option<Value>> {
        self.regions
            .iter()
            .any(|region| region.contains(cf, key))
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
        let region = ResidentRegion::Point { cf, key };
        if !self.regions.contains(&region) {
            self.regions.push(region);
        }
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
            let cf = match &region {
                ResidentRegion::Point { cf, .. }
                | ResidentRegion::Range { cf, .. }
                | ResidentRegion::Prefix { cf, .. } => cf.clone(),
            };
            self.values.insert((cf, key.clone()), value.clone());
        }
        if !self.regions.contains(&region) {
            self.regions.push(region);
        }
    }

    fn rows_for(&self, region: &ResidentRegion, reverse: bool) -> Option<Vec<KeyValue>> {
        self.regions
            .iter()
            .any(|resident| resident.covers(region))
            .then(|| {
                let mut rows = self
                    .values
                    .iter()
                    .filter_map(|((cf, key), value)| {
                        region
                            .contains(cf, key)
                            .then_some((key.clone(), value.clone()))
                    })
                    .collect::<Vec<_>>();
                if reverse {
                    rows.reverse();
                }
                rows
            })
    }

    fn invalidate(&mut self, cf: &str, key: &[u8]) {
        self.values.remove(&(cf.to_owned(), key.to_vec()));
        self.regions.retain(|region| !region.contains(cf, key));
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
    waiters: Vec<Waker>,
    failures: BTreeMap<TestStorageOperation, VecDeque<Error>>,
}

impl Default for ControlState {
    fn default() -> Self {
        Self {
            yield_before_ready: true,
            paused: false,
            paused_operations: BTreeSet::new(),
            permits: 0,
            observed: Vec::new(),
            waiters: Vec::new(),
            failures: BTreeMap::new(),
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

    async fn before(&self, operation: TestStorageOperation) -> Result<(), Error> {
        let mut recorded = false;
        let mut yielded = false;
        poll_fn(|cx| {
            let mut state = self.state.borrow_mut();
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
        Self::wrap(MemoryStorage::new(column_families))
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
}

struct TestStorageCursor<'a> {
    inner: StorageScan<'a>,
    control: TestStorageControl,
    resident: Rc<RefCell<ResidentState>>,
    region: ResidentRegion,
    rows: Vec<KeyValue>,
}

impl StorageCursor for TestStorageCursor<'_> {
    fn next_batch(&mut self) -> StorageFuture<'_, Result<Option<Vec<KeyValue>>, Error>> {
        Box::pin(async move {
            self.control.before(TestStorageOperation::ScanBatch).await?;
            let batch = self.inner.next_batch().await?;
            if let Some(batch) = &batch {
                self.rows.extend(batch.iter().cloned());
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

    fn scan_range(
        &self,
        cf: String,
        start: Vec<u8>,
        end: Vec<u8>,
    ) -> StorageFuture<'_, Result<StorageScan<'_>, Error>> {
        let region = ResidentRegion::Range {
            cf: cf.clone(),
            start: start.clone(),
            end: end.clone(),
        };
        if let Some(rows) = self.resident.borrow().rows_for(&region, false) {
            return Box::pin(async move {
                Ok(Box::new(ReadyStorageCursor::new(rows)) as StorageScan<'_>)
            });
        }
        Box::pin(async move {
            self.control.before(TestStorageOperation::ScanOpen).await?;
            let inner = self.inner.scan_range(cf, start, end).await?;
            Ok(Box::new(TestStorageCursor {
                inner,
                control: self.control.clone(),
                resident: Rc::clone(&self.resident),
                region,
                rows: Vec::new(),
            }) as StorageScan<'_>)
        })
    }

    fn scan_prefix(
        &self,
        cf: String,
        prefix: Vec<u8>,
    ) -> StorageFuture<'_, Result<StorageScan<'_>, Error>> {
        let region = ResidentRegion::Prefix {
            cf: cf.clone(),
            prefix: prefix.clone(),
        };
        if let Some(rows) = self.resident.borrow().rows_for(&region, false) {
            return Box::pin(async move {
                Ok(Box::new(ReadyStorageCursor::new(rows)) as StorageScan<'_>)
            });
        }
        Box::pin(async move {
            self.control.before(TestStorageOperation::ScanOpen).await?;
            let inner = self.inner.scan_prefix(cf, prefix).await?;
            Ok(Box::new(TestStorageCursor {
                inner,
                control: self.control.clone(),
                resident: Rc::clone(&self.resident),
                region,
                rows: Vec::new(),
            }) as StorageScan<'_>)
        })
    }

    fn scan_prefix_reverse(
        &self,
        cf: String,
        prefix: Vec<u8>,
    ) -> StorageFuture<'_, Result<StorageScan<'_>, Error>> {
        let region = ResidentRegion::Prefix {
            cf: cf.clone(),
            prefix: prefix.clone(),
        };
        if let Some(rows) = self.resident.borrow().rows_for(&region, true) {
            return Box::pin(async move {
                Ok(Box::new(ReadyStorageCursor::new(rows)) as StorageScan<'_>)
            });
        }
        Box::pin(async move {
            self.control.before(TestStorageOperation::ScanOpen).await?;
            let inner = self.inner.scan_prefix_reverse(cf, prefix).await?;
            Ok(Box::new(TestStorageCursor {
                inner,
                control: self.control.clone(),
                resident: Rc::clone(&self.resident),
                region,
                rows: Vec::new(),
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
                    OwnedWriteOperation::Delta { cf, key, .. } => {
                        resident.invalidate(&cf, &key);
                    }
                }
            }
            Ok(())
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
