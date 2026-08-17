//! Contract tests live at the public storage seam because suspension and
//! first-poll readiness are backend behavior, not an IVM implementation detail.

use std::cell::Cell;
use std::rc::Rc;
use std::sync::Arc;
use std::task::{Context, Poll, Wake, Waker};

use groove::storage::pollable::{
    OwnedScanRequest, OwnedStorageOperation, OwnedStorageRequest, OwnedStorageResponse,
    PollableOrderedKvStorage, ScanDirection,
};
use groove::storage::{Error, MemoryStorage, OwnedWriteOperation};
use groove::{
    db::{Database, GraphBuilder},
    records::Value,
    schema::{ColumnSchema, ColumnType, DatabaseSchema, IntegerKeyType, PrimaryKey, TableSchema},
};

struct NoopWake;

impl Wake for NoopWake {
    fn wake(self: Arc<Self>) {}
}

fn poll_request(
    storage: &mut dyn PollableOrderedKvStorage,
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

impl PollableOrderedKvStorage for ControlledStorage {
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
        bounds: groove::storage::pollable::OwnedScanBounds::Prefix(Vec::new()),
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
    let mut database = Database::new(schema, MemoryStorage::new(&["rows", "indices"])).unwrap();
    let subscription = database
        .subscribe_one_sink(GraphBuilder::table("rows"))
        .unwrap();
    assert!(subscription.recv().unwrap().is_empty());

    let mut batch = database.open_batch();
    batch.insert(
        "rows",
        vec![Value::U64(1), Value::String("visible locally".into())],
    );
    let persistence = database.commit_batch_for_async_persistence(batch).unwrap();

    let local_delta = subscription.recv().unwrap().to_values().unwrap();
    assert_eq!(
        local_delta,
        vec![(
            vec![Value::U64(1), Value::String("visible locally".into())],
            1,
        )]
    );
    let local_rows = database.primary_key_scan("rows", &[Value::U64(1)]).unwrap();
    assert_eq!(
        local_rows[0].get("value").unwrap(),
        Value::String("visible locally".into())
    );

    let (mut durable_storage, gate) = ControlledStorage::new(&["rows", "indices"]);
    let durable_commit =
        OwnedStorageRequest::new(OwnedStorageOperation::Commit(persistence.into_operations()));
    assert!(poll_request(&mut durable_storage, &durable_commit).is_pending());

    // A suspended durable backend does not create a second core mode: the
    // already-open local subscription and resident one-shot state remain live.
    assert_eq!(
        database.primary_key_scan("rows", &[Value::U64(1)]).unwrap()[0]
            .get("value")
            .unwrap(),
        Value::String("visible locally".into())
    );

    gate.release();
    assert!(matches!(
        poll_request(&mut durable_storage, &durable_commit),
        Poll::Ready(Ok(OwnedStorageResponse::Committed))
    ));
}
