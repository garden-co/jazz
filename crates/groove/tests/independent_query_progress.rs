#![cfg(feature = "test")]

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use futures::executor::block_on;
use futures::task::{ArcWake, waker};
use groove::db::{Database, GraphBuilder};
use groove::records::Value;
use groove::schema::{
    ColumnSchema, ColumnType, DatabaseSchema, IntegerKeyType, PrimaryKey, TableSchema,
};
use groove::storage::{
    Error as StorageError, MemoryStorage, OrderedKvStorage, OwnedWriteOperation, ScanRequest,
    StorageFuture, StorageScan, TestStorage, TestStorageOperation, YieldingStorage,
};

#[derive(Default)]
struct OwnerWake(AtomicUsize);

impl ArcWake for OwnerWake {
    fn wake_by_ref(wake: &Arc<Self>) {
        wake.0.fetch_add(1, Ordering::AcqRel);
    }
}

fn schema() -> DatabaseSchema {
    DatabaseSchema::new(["slow", "fast"].map(|name| {
        TableSchema::new(name, [ColumnSchema::new("id", ColumnType::U64)])
            .with_primary_key(PrimaryKey::new("id", IntegerKeyType::U64))
    }))
}

#[test]
fn ready_query_delivers_while_earlier_query_waits_for_storage() {
    let (storage, control) = TestStorage::controlled(&["slow", "fast"]);
    let mut database = block_on(Database::new(schema(), storage.clone())).unwrap();
    let mut batch = database.open_batch();
    batch.insert("slow", vec![Value::U64(1)]);
    batch.insert("fast", vec![Value::U64(2)]);
    block_on(database.commit_batch(batch)).unwrap();
    // Warm only the later query's pages; the earlier query remains cold.
    assert_eq!(
        block_on(database.query_graph(GraphBuilder::table("fast")))
            .unwrap()
            .to_values()
            .unwrap(),
        vec![(vec![Value::U64(2)], 1)]
    );
    storage.evict_column_family("slow");
    control.pause_on(TestStorageOperation::ScanOpen);
    let owner = Arc::new(OwnerWake::default());
    let owner_waker = waker(Arc::clone(&owner));
    let slow = database
        .subscribe_with_waker([("rows", GraphBuilder::table("slow"))], Some(&owner_waker))
        .unwrap();
    for _ in 0..4 {
        block_on(database.drive_ready_progress_with_waker(Some(&owner_waker))).unwrap();
    }
    assert!(slow.try_recv().is_err());
    let fast = database
        .subscribe_with_waker([("rows", GraphBuilder::table("fast"))], Some(&owner_waker))
        .unwrap();
    for _ in 0..32 {
        if owner.0.swap(0, Ordering::AcqRel) == 0 {
            break;
        }
        block_on(database.drive_ready_progress_with_waker(Some(&owner_waker))).unwrap();
    }
    let ready = fast
        .try_recv()
        .expect("a ready query must not wait for an unrelated cold query");
    assert_eq!(
        ready.sinks["rows"].to_values().unwrap(),
        vec![(vec![Value::U64(2)], 1)]
    );
    assert!(
        slow.try_recv().is_err(),
        "the first query is still waiting on storage"
    );
    let paused_polls = control.poll_count(TestStorageOperation::ScanOpen);
    owner.0.store(0, Ordering::Release);
    for _ in 0..16 {
        block_on(database.drive_ready_progress_with_waker(Some(&owner_waker))).unwrap();
    }
    assert_eq!(
        control.poll_count(TestStorageOperation::ScanOpen),
        paused_polls,
        "idle owner turns do not repoll a sleeping storage request"
    );
    assert_eq!(
        owner.0.load(Ordering::Acquire),
        0,
        "a sleeping query does not schedule busy owner turns"
    );
    control.resume();
    block_on(database.drive_progress()).unwrap();
    let ready = slow.try_recv().unwrap();
    assert_eq!(
        ready.sinks["rows"].to_values().unwrap(),
        vec![(vec![Value::U64(1)], 1)]
    );
}

// Distinct controls are essential: resuming one storage operation must not
// spuriously wake the other query before its own I/O completion.
#[derive(Clone)]
struct IndependentlyControlledReads {
    memory: MemoryStorage,
    slow: TestStorage,
    fast: TestStorage,
}

impl IndependentlyControlledReads {
    fn new() -> Self {
        let memory = MemoryStorage::new(&["slow", "fast"]).unwrap();
        Self {
            slow: YieldingStorage::wrap(memory.clone()),
            fast: YieldingStorage::wrap(memory.clone()),
            memory,
        }
    }
}

impl OrderedKvStorage for IndependentlyControlledReads {
    fn get(
        &self,
        cf: String,
        key: Vec<u8>,
    ) -> StorageFuture<'_, Result<Option<Vec<u8>>, StorageError>> {
        if cf == "slow" {
            self.slow.get(cf, key)
        } else {
            self.fast.get(cf, key)
        }
    }
    fn scan(
        &self,
        request: ScanRequest,
    ) -> StorageFuture<'_, Result<StorageScan<'_>, StorageError>> {
        if request.cf == "slow" {
            self.slow.scan(request)
        } else {
            self.fast.scan(request)
        }
    }
    fn set(
        &self,
        cf: String,
        key: Vec<u8>,
        value: Vec<u8>,
    ) -> StorageFuture<'_, Result<(), StorageError>> {
        self.memory.set(cf, key, value)
    }
    fn delete(&self, cf: String, key: Vec<u8>) -> StorageFuture<'_, Result<(), StorageError>> {
        self.memory.delete(cf, key)
    }
    fn put_if_absent(
        &self,
        cf: String,
        key: Vec<u8>,
        value: Vec<u8>,
    ) -> StorageFuture<'_, Result<Option<Vec<u8>>, StorageError>> {
        self.memory.put_if_absent(cf, key, value)
    }
    fn compare_and_delete(
        &self,
        cf: String,
        key: Vec<u8>,
        expected: Vec<u8>,
    ) -> StorageFuture<'_, Result<bool, StorageError>> {
        self.memory.compare_and_delete(cf, key, expected)
    }
    fn write_many(
        &self,
        operations: Vec<OwnedWriteOperation>,
    ) -> StorageFuture<'_, Result<(), StorageError>> {
        self.memory.write_many(operations)
    }
}

#[test]
fn sleeping_queries_retain_the_current_owner_after_an_earlier_query_yields() {
    let storage = IndependentlyControlledReads::new();
    let slow_control = storage.slow.control();
    let fast_control = storage.fast.control();
    let mut database = block_on(Database::new(schema(), storage.clone())).unwrap();
    let mut batch = database.open_batch();
    batch.insert("slow", vec![Value::U64(1)]);
    batch.insert("fast", vec![Value::U64(2)]);
    block_on(database.commit_batch(batch)).unwrap();
    storage.slow.evict_all();
    storage.fast.evict_all();
    slow_control.take_observed();
    fast_control.take_observed();
    slow_control.pause_on(TestStorageOperation::ScanOpen);
    fast_control.pause_on(TestStorageOperation::ScanOpen);
    let old_owner = Arc::new(OwnerWake::default());
    let old_waker = waker(Arc::clone(&old_owner));
    let slow = database
        .subscribe_with_waker([("rows", GraphBuilder::table("slow"))], Some(&old_waker))
        .unwrap();
    let fast = database
        .subscribe_with_waker([("rows", GraphBuilder::table("fast"))], Some(&old_waker))
        .unwrap();
    for _ in 0..16 {
        block_on(database.drive_ready_progress_with_waker(Some(&old_waker))).unwrap();
    }
    assert!(
        slow_control
            .observed()
            .contains(&TestStorageOperation::ScanOpen)
    );
    assert!(
        fast_control
            .observed()
            .contains(&TestStorageOperation::ScanOpen)
    );
    assert!(slow.try_recv().is_err());
    assert!(fast.try_recv().is_err());

    // The new owner returns while the earlier scan starts its next read.
    // The later read must retain this owner even though it wasn't polled.
    slow_control.resume_operation(TestStorageOperation::ScanOpen);
    let owner = Arc::new(OwnerWake::default());
    let owner_waker = waker(Arc::clone(&owner));
    block_on(database.drive_ready_progress_with_waker(Some(&owner_waker))).unwrap();
    owner.0.store(0, Ordering::Release);
    fast_control.resume_operation(TestStorageOperation::ScanOpen);
    assert!(
        owner.0.load(Ordering::Acquire) > 0,
        "the later read wakes the current owner, not the previous opening task"
    );
    for _ in 0..32 {
        if owner.0.swap(0, Ordering::AcqRel) == 0 {
            break;
        }
        block_on(database.drive_ready_progress_with_waker(Some(&owner_waker))).unwrap();
    }
    assert_eq!(
        fast.try_recv().unwrap().sinks["rows"].to_values().unwrap(),
        vec![(vec![Value::U64(2)], 1)]
    );
    assert_eq!(
        slow.try_recv().unwrap().sinks["rows"].to_values().unwrap(),
        vec![(vec![Value::U64(1)], 1)]
    );
    assert!(!database.has_pending_progress());
}

impl groove::storage::ReopenableStorage for IndependentlyControlledReads {
    fn reopen(
        self,
        column_families: Vec<String>,
    ) -> StorageFuture<'static, Result<Self, StorageError>> {
        Box::pin(async move {
            let memory = self.memory.clone().reopen(column_families).await?;
            self.slow.evict_all();
            self.fast.evict_all();
            Ok(Self { memory, ..self })
        })
    }
}
