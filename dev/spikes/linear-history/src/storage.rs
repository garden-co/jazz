//! Storage helpers: a ready-future executor and an op-counting adapter.

use std::cell::Cell;
use std::future::Future;
use std::rc::Rc;
use std::task::{Context, Poll, Waker};

use groove::storage::{
    Error, OrderedKvStorage, OwnedWriteOperation, ReadyStorageCursor, ScanRequest, StorageFuture,
    StorageScan, Value, collect_scan,
};

/// Drive a storage future. Memory and RocksDB futures are always ready; this
/// spike does not model a suspended (IndexedDB page-miss) read.
pub fn block_on<F: Future>(future: F) -> F::Output {
    let mut future = std::pin::pin!(future);
    let mut cx = Context::from_waker(Waker::noop());
    loop {
        if let Poll::Ready(value) = future.as_mut().poll(&mut cx) {
            return value;
        }
    }
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct OpCounts {
    pub point_reads: u64,
    pub scans: u64,
    pub scanned_items: u64,
    pub write_batches: u64,
    pub writes: u64,
    pub written_bytes: u64,
}

impl OpCounts {
    pub fn since(self, earlier: Self) -> Self {
        Self {
            point_reads: self.point_reads - earlier.point_reads,
            scans: self.scans - earlier.scans,
            scanned_items: self.scanned_items - earlier.scanned_items,
            write_batches: self.write_batches - earlier.write_batches,
            writes: self.writes - earlier.writes,
            written_bytes: self.written_bytes - earlier.written_bytes,
        }
    }
}

#[derive(Default)]
struct Counters {
    point_reads: Cell<u64>,
    scans: Cell<u64>,
    scanned_items: Cell<u64>,
    write_batches: Cell<u64>,
    writes: Cell<u64>,
    written_bytes: Cell<u64>,
}

fn bump(cell: &Cell<u64>, by: u64) {
    cell.set(cell.get() + by);
}

/// Wraps a backend and counts logical storage operations, the same unit as
/// the todo-profile `storage_write_count` receipts (keys + values for bytes).
pub struct Counting<S> {
    inner: S,
    counters: Rc<Counters>,
}

#[derive(Clone)]
pub struct CountHandle(Rc<Counters>);

impl CountHandle {
    pub fn snapshot(&self) -> OpCounts {
        let c = &self.0;
        OpCounts {
            point_reads: c.point_reads.get(),
            scans: c.scans.get(),
            scanned_items: c.scanned_items.get(),
            write_batches: c.write_batches.get(),
            writes: c.writes.get(),
            written_bytes: c.written_bytes.get(),
        }
    }
}

impl<S> Counting<S> {
    pub fn new(inner: S) -> (Self, CountHandle) {
        let counters = Rc::new(Counters::default());
        (
            Self {
                inner,
                counters: Rc::clone(&counters),
            },
            CountHandle(counters),
        )
    }

    pub fn inner(&self) -> &S {
        &self.inner
    }
}

impl<S: OrderedKvStorage> OrderedKvStorage for Counting<S> {
    fn get(&self, cf: String, key: Vec<u8>) -> StorageFuture<'_, Result<Option<Value>, Error>> {
        bump(&self.counters.point_reads, 1);
        self.inner.get(cf, key)
    }

    fn put_if_absent(
        &self,
        cf: String,
        key: Vec<u8>,
        value: Vec<u8>,
    ) -> StorageFuture<'_, Result<Option<Value>, Error>> {
        self.inner.put_if_absent(cf, key, value)
    }

    fn compare_and_delete(
        &self,
        cf: String,
        key: Vec<u8>,
        expected: Vec<u8>,
    ) -> StorageFuture<'_, Result<bool, Error>> {
        self.inner.compare_and_delete(cf, key, expected)
    }

    fn set(
        &self,
        cf: String,
        key: Vec<u8>,
        value: Vec<u8>,
    ) -> StorageFuture<'_, Result<(), Error>> {
        bump(&self.counters.writes, 1);
        bump(
            &self.counters.written_bytes,
            (key.len() + value.len()) as u64,
        );
        self.inner.set(cf, key, value)
    }

    fn delete(&self, cf: String, key: Vec<u8>) -> StorageFuture<'_, Result<(), Error>> {
        bump(&self.counters.writes, 1);
        bump(&self.counters.written_bytes, key.len() as u64);
        self.inner.delete(cf, key)
    }

    fn flush_write_boundary(&self) -> StorageFuture<'_, Result<(), Error>> {
        self.inner.flush_write_boundary()
    }

    fn scan(&self, request: ScanRequest) -> StorageFuture<'_, Result<StorageScan<'_>, Error>> {
        Box::pin(async move {
            bump(&self.counters.scans, 1);
            let items = collect_scan(self.inner.scan(request).await?).await?;
            bump(&self.counters.scanned_items, items.len() as u64);
            Ok(Box::new(ReadyStorageCursor::new(items)) as StorageScan<'_>)
        })
    }

    fn write_many(
        &self,
        operations: Vec<OwnedWriteOperation>,
    ) -> StorageFuture<'_, Result<(), Error>> {
        bump(&self.counters.write_batches, 1);
        bump(&self.counters.writes, operations.len() as u64);
        let bytes: usize = operations
            .iter()
            .map(|op| match op {
                OwnedWriteOperation::Set { key, value, .. } => key.len() + value.len(),
                OwnedWriteOperation::Delete { key, .. } => key.len(),
            })
            .sum();
        bump(&self.counters.written_bytes, bytes as u64);
        self.inner.write_many(operations)
    }
}
