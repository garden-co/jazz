//! Deterministically controlled storage used by async and failure-path tests.

use std::cell::RefCell;
use std::future::poll_fn;
use std::rc::Rc;
use std::task::{Poll, Waker};

use super::{
    Error, KeyValue, MemoryStorage, OrderedKvStorage, OwnedWriteOperation, ReopenableStorage,
    StorageCursor, StorageFuture, StorageScan, Value,
};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
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

#[derive(Default)]
struct ControlState {
    paused: bool,
    permits: usize,
    observed: Vec<TestStorageOperation>,
    waiters: Vec<Waker>,
}

/// Controller held by tests independently from the storage under test.
#[derive(Clone, Default)]
pub struct TestStorageControl {
    state: Rc<RefCell<ControlState>>,
}

impl TestStorageControl {
    /// Make subsequent storage progress require explicit permits.
    pub fn pause(&self) {
        self.state.borrow_mut().paused = true;
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

    async fn before(&self, operation: TestStorageOperation) {
        let mut recorded = false;
        poll_fn(|cx| {
            let mut state = self.state.borrow_mut();
            if !recorded {
                state.observed.push(operation);
                recorded = true;
            }
            if !state.paused {
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
        .await
    }
}

/// In-memory ordered storage whose suspension points are controlled by tests.
///
/// It implements the production storage contract directly. Immediate and
/// suspended behavior therefore exercise the same Groove call path.
#[derive(Clone)]
pub struct TestStorage {
    inner: MemoryStorage,
    control: TestStorageControl,
}

impl TestStorage {
    pub fn new(column_families: &[&str]) -> Self {
        Self {
            inner: MemoryStorage::new(column_families),
            control: TestStorageControl::default(),
        }
    }

    pub fn controlled(column_families: &[&str]) -> (Self, TestStorageControl) {
        let storage = Self::new(column_families);
        (storage.clone(), storage.control.clone())
    }

    pub fn control(&self) -> TestStorageControl {
        self.control.clone()
    }
}

struct TestStorageCursor {
    inner: StorageScan,
    control: TestStorageControl,
}

impl StorageCursor for TestStorageCursor {
    fn next_batch(&mut self) -> StorageFuture<'_, Result<Option<Vec<KeyValue>>, Error>> {
        Box::pin(async move {
            self.control.before(TestStorageOperation::ScanBatch).await;
            self.inner.next_batch().await
        })
    }
}

impl OrderedKvStorage for TestStorage {
    fn get(&self, cf: String, key: Vec<u8>) -> StorageFuture<'_, Result<Option<Value>, Error>> {
        Box::pin(async move {
            self.control.before(TestStorageOperation::Get).await;
            self.inner.get(cf, key).await
        })
    }

    fn set(
        &self,
        cf: String,
        key: Vec<u8>,
        value: Vec<u8>,
    ) -> StorageFuture<'_, Result<(), Error>> {
        Box::pin(async move {
            self.control.before(TestStorageOperation::Set).await;
            self.inner.set(cf, key, value).await
        })
    }

    fn delete(&self, cf: String, key: Vec<u8>) -> StorageFuture<'_, Result<(), Error>> {
        Box::pin(async move {
            self.control.before(TestStorageOperation::Delete).await;
            self.inner.delete(cf, key).await
        })
    }

    fn close(&self) -> StorageFuture<'_, Result<(), Error>> {
        Box::pin(async move {
            self.control.before(TestStorageOperation::Close).await;
            self.inner.close().await
        })
    }

    fn set_write_flush_cadence(&self, every: usize) -> StorageFuture<'_, Result<(), Error>> {
        Box::pin(async move {
            self.control
                .before(TestStorageOperation::SetWriteFlushCadence)
                .await;
            self.inner.set_write_flush_cadence(every).await
        })
    }

    fn flush_write_boundary(&self) -> StorageFuture<'_, Result<(), Error>> {
        Box::pin(async move {
            self.control
                .before(TestStorageOperation::FlushWriteBoundary)
                .await;
            self.inner.flush_write_boundary().await
        })
    }

    fn approximate_class_bytes(&self, cf: String) -> StorageFuture<'_, Result<Option<u64>, Error>> {
        Box::pin(async move {
            self.control
                .before(TestStorageOperation::ApproximateClassBytes)
                .await;
            self.inner.approximate_class_bytes(cf).await
        })
    }

    fn scan_range(
        &self,
        cf: String,
        start: Vec<u8>,
        end: Vec<u8>,
    ) -> StorageFuture<'_, Result<StorageScan, Error>> {
        Box::pin(async move {
            self.control.before(TestStorageOperation::ScanOpen).await;
            let inner = self.inner.scan_range(cf, start, end).await?;
            Ok(Box::new(TestStorageCursor {
                inner,
                control: self.control.clone(),
            }) as StorageScan)
        })
    }

    fn scan_prefix(
        &self,
        cf: String,
        prefix: Vec<u8>,
    ) -> StorageFuture<'_, Result<StorageScan, Error>> {
        Box::pin(async move {
            self.control.before(TestStorageOperation::ScanOpen).await;
            let inner = self.inner.scan_prefix(cf, prefix).await?;
            Ok(Box::new(TestStorageCursor {
                inner,
                control: self.control.clone(),
            }) as StorageScan)
        })
    }

    fn scan_prefix_reverse(
        &self,
        cf: String,
        prefix: Vec<u8>,
    ) -> StorageFuture<'_, Result<StorageScan, Error>> {
        Box::pin(async move {
            self.control.before(TestStorageOperation::ScanOpen).await;
            let inner = self.inner.scan_prefix_reverse(cf, prefix).await?;
            Ok(Box::new(TestStorageCursor {
                inner,
                control: self.control.clone(),
            }) as StorageScan)
        })
    }

    fn write_many(
        &self,
        operations: Vec<OwnedWriteOperation>,
    ) -> StorageFuture<'_, Result<(), Error>> {
        Box::pin(async move {
            self.control.before(TestStorageOperation::WriteMany).await;
            self.inner.write_many(operations).await
        })
    }

    fn column_family_names(&self) -> Option<Vec<String>> {
        self.inner.column_family_names()
    }
}

impl ReopenableStorage for TestStorage {
    fn reopen(self, column_families: Vec<String>) -> StorageFuture<'static, Result<Self, Error>> {
        Box::pin(async move {
            self.control.before(TestStorageOperation::Reopen).await;
            Ok(Self {
                inner: self.inner.reopen(column_families).await?,
                control: self.control,
            })
        })
    }
}
