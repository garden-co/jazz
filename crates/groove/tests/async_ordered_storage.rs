//! Contract tests live at the public storage seam because suspension and
//! first-poll readiness are backend behavior, not an IVM implementation detail.

use std::cell::{Cell, RefCell};
use std::rc::Rc;
use std::sync::Arc;
use std::task::{Context, Poll, Wake, Waker};

use groove::storage::async_ordered::{
    OrderedKvStorage, OwnedScanRequest, OwnedStorageOperation, OwnedStorageRequest,
    OwnedStorageResponse, ScanDirection, StorageRequestId,
};
use groove::storage::{Error, MemoryStorage, OwnedWriteOperation};
use groove::{
    db::{Database, GraphBuilder, PersistenceQueue, PollableDatabase},
    records::Value,
    schema::{ColumnSchema, ColumnType, DatabaseSchema, IntegerKeyType, PrimaryKey, TableSchema},
};

struct NoopWake;

impl Wake for NoopWake {
    fn wake(self: Arc<Self>) {}
}

fn poll_request(
    storage: &mut dyn OrderedKvStorage,
    request: &OwnedStorageRequest,
) -> Poll<Result<OwnedStorageResponse, Error>> {
    let waker = Waker::from(Arc::new(NoopWake));
    let mut context = Context::from_waker(&waker);
    storage.poll_request(request, &mut context)
}

#[derive(Clone, Default)]
struct OperationGate(Rc<Cell<bool>>);

impl OperationGate {
    fn release(&self) {
        self.0.set(true);
    }

    fn is_released(&self) -> bool {
        self.0.get()
    }
}

struct ControlledStorage {
    storage: MemoryStorage,
    gate: OperationGate,
}

struct FailingCommitStorage;

impl OrderedKvStorage for FailingCommitStorage {
    fn poll_request(
        &mut self,
        _request: &OwnedStorageRequest,
        _context: &mut Context<'_>,
    ) -> Poll<Result<OwnedStorageResponse, Error>> {
        Poll::Ready(Err(Error::Backend {
            backend: "controlled",
            message: "injected commit failure".to_owned(),
        }))
    }

    fn cancel_request(&mut self, _request: StorageRequestId) -> Result<(), Error> {
        Ok(())
    }
}

struct OrderedControlledStorage {
    storage: MemoryStorage,
    permitted: Rc<Cell<usize>>,
    seen: Rc<RefCell<Vec<StorageRequestId>>>,
}

impl OrderedControlledStorage {
    fn new(
        column_families: &[&str],
    ) -> (Self, Rc<Cell<usize>>, Rc<RefCell<Vec<StorageRequestId>>>) {
        let permitted = Rc::new(Cell::new(0));
        let seen = Rc::new(RefCell::new(Vec::new()));
        (
            Self {
                storage: MemoryStorage::new(column_families),
                permitted: Rc::clone(&permitted),
                seen: Rc::clone(&seen),
            },
            permitted,
            seen,
        )
    }
}

impl OrderedKvStorage for OrderedControlledStorage {
    fn poll_request(
        &mut self,
        request: &OwnedStorageRequest,
        context: &mut Context<'_>,
    ) -> Poll<Result<OwnedStorageResponse, Error>> {
        let position = {
            let mut seen = self.seen.borrow_mut();
            seen.iter()
                .position(|id| *id == request.id())
                .unwrap_or_else(|| {
                    seen.push(request.id());
                    seen.len() - 1
                })
        };
        if position >= self.permitted.get() {
            return Poll::Pending;
        }
        self.storage.poll_request(request, context)
    }

    fn cancel_request(&mut self, _request: StorageRequestId) -> Result<(), Error> {
        Ok(())
    }
}

impl ControlledStorage {
    fn new(column_families: &[&str]) -> (Self, OperationGate) {
        let gate = OperationGate::default();
        (
            Self {
                storage: MemoryStorage::new(column_families),
                gate: gate.clone(),
            },
            gate,
        )
    }
}

impl OrderedKvStorage for ControlledStorage {
    fn poll_request(
        &mut self,
        request: &OwnedStorageRequest,
        context: &mut Context<'_>,
    ) -> Poll<Result<OwnedStorageResponse, Error>> {
        if !self.gate.is_released() {
            return Poll::Pending;
        }
        self.storage.poll_request(request, context)
    }

    fn cancel_request(&mut self, _request: StorageRequestId) -> Result<(), Error> {
        Ok(())
    }
}

#[test]
fn memory_storage_inherits_first_poll_read_write_and_scan_readiness() {
    let mut storage = MemoryStorage::new(&["rows"]);

    assert!(matches!(
        poll_request(
            &mut storage,
            &OwnedStorageRequest::new(OwnedStorageOperation::Commit(vec![
                OwnedWriteOperation::Set {
                    cf: "rows".to_owned(),
                    key: b"b".to_vec(),
                    value: b"two".to_vec(),
                },
                OwnedWriteOperation::Set {
                    cf: "rows".to_owned(),
                    key: b"a".to_vec(),
                    value: b"one".to_vec(),
                }
            ]))
        ),
        Poll::Ready(Ok(OwnedStorageResponse::Committed))
    ));

    let request = OwnedStorageRequest::new(OwnedStorageOperation::Get {
        column_family: "rows".to_owned(),
        key: b"a".to_vec(),
    });
    let Poll::Ready(Ok(OwnedStorageResponse::Value(value))) = poll_request(&mut storage, &request)
    else {
        panic!("memory get must complete successfully on its first poll");
    };
    assert_eq!(value, Some(b"one".to_vec()));

    let Poll::Ready(Ok(OwnedStorageResponse::Rows(rows))) = poll_request(
        &mut storage,
        &OwnedStorageRequest::new(OwnedStorageOperation::Scan(OwnedScanRequest::prefix(
            "rows",
            Vec::new(),
        ))),
    ) else {
        panic!("memory scan must complete successfully on its first poll");
    };
    assert_eq!(
        rows,
        vec![
            (b"a".to_vec(), b"one".to_vec()),
            (b"b".to_vec(), b"two".to_vec()),
        ]
    );
}

#[test]
fn controlled_storage_retains_owned_commit_until_async_release() {
    let (mut storage, gate) = ControlledStorage::new(&["rows"]);
    let request = OwnedStorageRequest::new(OwnedStorageOperation::Commit(vec![
        OwnedWriteOperation::Set {
            cf: "rows".to_owned(),
            key: b"owned-key".to_vec(),
            value: b"owned-value".to_vec(),
        },
    ]));
    assert!(poll_request(&mut storage, &request).is_pending());
    gate.release();
    assert!(matches!(
        poll_request(&mut storage, &request),
        Poll::Ready(Ok(OwnedStorageResponse::Committed))
    ));

    let request = OwnedStorageRequest::new(OwnedStorageOperation::Scan(OwnedScanRequest {
        column_family: "rows".to_owned(),
        bounds: groove::storage::async_ordered::OwnedScanBounds::Prefix(Vec::new()),
        direction: ScanDirection::Forward,
    }));
    let Poll::Ready(Ok(OwnedStorageResponse::Rows(rows))) = poll_request(&mut storage, &request)
    else {
        panic!("released controlled scan must complete");
    };
    assert_eq!(rows, vec![(b"owned-key".to_vec(), b"owned-value".to_vec())]);
}

#[test]
fn pending_durable_commit_does_not_delay_local_query_or_subscription_visibility() {
    let schema = DatabaseSchema::new([TableSchema::new(
        "rows",
        [
            ColumnSchema::new("id", ColumnType::U64),
            ColumnSchema::new("value", ColumnType::String),
        ],
    )
    .with_primary_key(PrimaryKey::new("id", IntegerKeyType::U64))]);
    let resident = Database::new(schema, MemoryStorage::new(&["rows", "indices"])).unwrap();
    let (durable_storage, gate) = ControlledStorage::new(&["rows", "indices"]);
    let mut database = PollableDatabase::new(resident, Box::new(durable_storage));
    let subscription = database
        .resident_mut()
        .subscribe_one_sink(GraphBuilder::table("rows"))
        .unwrap();
    assert!(subscription.recv().unwrap().is_empty());

    let mut batch = database.resident().open_batch();
    batch.insert(
        "rows",
        vec![Value::U64(1), Value::String("visible locally".into())],
    );
    database.commit_batch(batch).unwrap();

    let local_delta = subscription.recv().unwrap().to_values().unwrap();
    assert_eq!(
        local_delta,
        vec![(
            vec![Value::U64(1), Value::String("visible locally".into())],
            1,
        )]
    );
    let local_rows = database
        .resident()
        .primary_key_scan("rows", &[Value::U64(1)])
        .unwrap();
    assert_eq!(
        local_rows[0].get("value").unwrap(),
        Value::String("visible locally".into())
    );

    let waker = Waker::from(Arc::new(NoopWake));
    let mut context = Context::from_waker(&waker);
    assert!(database.poll_persistence(&mut context).is_pending());
    assert!(database.has_pending_persistence());

    // A suspended durable backend does not create a second core mode: the
    // already-open local subscription and resident one-shot state remain live.
    assert_eq!(
        database
            .resident()
            .primary_key_scan("rows", &[Value::U64(1)])
            .unwrap()[0]
            .get("value")
            .unwrap(),
        Value::String("visible locally".into())
    );

    gate.release();
    assert!(matches!(
        database.poll_persistence(&mut context),
        Poll::Ready(Ok(()))
    ));
    assert!(!database.has_pending_persistence());
}

#[test]
fn failed_async_persistence_keeps_published_delta_but_poisons_later_database_use() {
    let schema = DatabaseSchema::new([TableSchema::new(
        "rows",
        [
            ColumnSchema::new("id", ColumnType::U64),
            ColumnSchema::new("value", ColumnType::String),
        ],
    )
    .with_primary_key(PrimaryKey::new("id", IntegerKeyType::U64))]);
    let resident = Database::new(schema, MemoryStorage::new(&["rows", "indices"])).unwrap();
    let mut database = PollableDatabase::new(resident, Box::new(FailingCommitStorage));
    let subscription = database
        .resident_mut()
        .subscribe_one_sink(GraphBuilder::table("rows"))
        .unwrap();
    assert!(subscription.recv().unwrap().is_empty());

    let mut batch = database.resident().open_batch();
    batch.insert(
        "rows",
        vec![Value::U64(1), Value::String("optimistic".into())],
    );
    database.commit_batch(batch).unwrap();
    assert_eq!(subscription.recv().unwrap().to_values().unwrap().len(), 1);

    let waker = Waker::from(Arc::new(NoopWake));
    let mut context = Context::from_waker(&waker);
    assert!(matches!(
        database.poll_persistence(&mut context),
        Poll::Ready(Err(_))
    ));
    assert!(matches!(
        database
            .resident()
            .primary_key_scan("rows", &[Value::U64(1)]),
        Err(groove::db::Error::DatabasePoisoned)
    ));
}

#[test]
fn later_visible_commits_cannot_overtake_a_pending_durable_predecessor() {
    let schema = DatabaseSchema::new([TableSchema::new(
        "rows",
        [
            ColumnSchema::new("id", ColumnType::U64),
            ColumnSchema::new("value", ColumnType::String),
        ],
    )
    .with_primary_key(PrimaryKey::new("id", IntegerKeyType::U64))]);
    let resident = Database::new(schema, MemoryStorage::new(&["rows", "indices"])).unwrap();
    let (persistence, permitted, seen) = OrderedControlledStorage::new(&["rows", "indices"]);
    let mut database = PollableDatabase::new(resident, Box::new(persistence));

    for id in [1, 2] {
        let mut batch = database.resident().open_batch();
        batch.insert(
            "rows",
            vec![Value::U64(id), Value::String(format!("row {id}"))],
        );
        database.commit_batch(batch).unwrap();
    }
    assert_eq!(
        database
            .resident()
            .primary_key_scan("rows", &[])
            .unwrap()
            .len(),
        2
    );

    let waker = Waker::from(Arc::new(NoopWake));
    let mut context = Context::from_waker(&waker);
    assert!(database.poll_persistence(&mut context).is_pending());
    assert_eq!(seen.borrow().len(), 1, "only the queue head may be polled");

    permitted.set(1);
    assert!(database.poll_persistence(&mut context).is_pending());
    assert_eq!(
        seen.borrow().len(),
        2,
        "the successor starts only after its predecessor commits"
    );

    permitted.set(2);
    assert!(matches!(
        database.poll_persistence(&mut context),
        Poll::Ready(Ok(()))
    ));
}

#[test]
fn cancelling_after_local_publication_poisons_instead_of_rolling_back() {
    let schema = DatabaseSchema::new([TableSchema::new(
        "rows",
        [
            ColumnSchema::new("id", ColumnType::U64),
            ColumnSchema::new("value", ColumnType::String),
        ],
    )
    .with_primary_key(PrimaryKey::new("id", IntegerKeyType::U64))]);
    let resident = Database::new(schema, MemoryStorage::new(&["rows", "indices"])).unwrap();
    let (persistence, _gate) = ControlledStorage::new(&["rows", "indices"]);
    let mut database = PollableDatabase::new(resident, Box::new(persistence));
    let mut batch = database.resident().open_batch();
    batch.insert(
        "rows",
        vec![Value::U64(1), Value::String("already observed".into())],
    );
    database.commit_batch(batch).unwrap();

    database.cancel_pending_persistence().unwrap();
    assert!(matches!(
        database.resident().primary_key_scan("rows", &[]),
        Err(groove::db::Error::DatabasePoisoned)
    ));
}

#[test]
fn multi_batch_unit_completes_only_after_its_final_durable_batch() {
    let schema = DatabaseSchema::new([TableSchema::new(
        "rows",
        [
            ColumnSchema::new("id", ColumnType::U64),
            ColumnSchema::new("value", ColumnType::String),
        ],
    )
    .with_primary_key(PrimaryKey::new("id", IntegerKeyType::U64))]);
    let mut resident = Database::new(schema, MemoryStorage::new(&["rows", "indices"])).unwrap();
    let mut receipts = Vec::new();
    for id in [1, 2] {
        let mut batch = resident.open_batch();
        batch.insert(
            "rows",
            vec![Value::U64(id), Value::String(format!("row {id}"))],
        );
        receipts.push(resident.commit_batch_for_async_persistence(batch).unwrap());
    }

    let (storage, permitted, _) = OrderedControlledStorage::new(&["rows", "indices"]);
    let mut queue = PersistenceQueue::new(Box::new(storage));
    let unit = queue.enqueue_unit(receipts);
    let waker = Waker::from(Arc::new(NoopWake));
    let mut context = Context::from_waker(&waker);

    assert!(queue.poll(&mut context).is_pending());
    permitted.set(1);
    assert!(
        queue.poll(&mut context).is_pending(),
        "the first durable batch cannot complete the containing unit"
    );
    permitted.set(2);
    assert!(matches!(
        queue.poll(&mut context),
        Poll::Ready(Ok(completed)) if completed == vec![unit]
    ));
}
