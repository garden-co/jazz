//! Contract tests live at the public storage seam because suspension and
//! first-poll readiness are backend behavior, not an IVM implementation detail.

use std::cell::Cell;
use std::rc::Rc;
use std::sync::Arc;
use std::task::{Context, Poll, Wake, Waker};

use groove::storage::async_ordered::{
    ImmediateStorage, OrderedKvStorage, OwnedScanRequest, OwnedStorageOperation,
    OwnedStorageRequest, OwnedStorageResponse, ScanDirection, StorageRequestId,
};
use groove::storage::{Error, MemoryStorage, OwnedWriteOperation};
use groove::{
    db::Database,
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
    storage: ImmediateStorage<MemoryStorage>,
    gate: OperationGate,
}

impl ControlledStorage {
    fn new(column_families: &[&str]) -> (Self, OperationGate) {
        let gate = OperationGate::default();
        (
            Self {
                storage: ImmediateStorage::new(MemoryStorage::new(column_families)),
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
    let mut storage = ImmediateStorage::new(MemoryStorage::new(&["rows"]));

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
fn lazy_table_schema_and_first_row_publish_together() {
    let initial = TableSchema::new(
        "rows",
        [
            ColumnSchema::new("id", ColumnType::U64),
            ColumnSchema::new("value", ColumnType::String),
        ],
    )
    .with_primary_key(PrimaryKey::new("id", IntegerKeyType::U64));
    let lazy = TableSchema::new(
        "lazy_rows",
        [
            ColumnSchema::new("id", ColumnType::U64),
            ColumnSchema::new("value", ColumnType::String),
        ],
    )
    .with_primary_key(PrimaryKey::new("id", IntegerKeyType::U64));
    let mut database = Database::new(
        DatabaseSchema::new([initial]),
        MemoryStorage::new(&["rows", "lazy_rows", "indices"]),
    )
    .unwrap();
    let registration = database.prepare_table_registration(lazy).unwrap();
    let mut batch = database.open_batch();
    batch.insert(
        "lazy_rows",
        vec![Value::U64(7), Value::String("prepared".to_owned())],
    );
    let prepared = database
        .prepare_batch_storage_inputs_with_table_registrations(
            &batch,
            std::slice::from_ref(&registration),
        )
        .unwrap();

    assert!(database.table_schema("lazy_rows").is_err());
    database
        .commit_prepared_batch_with_table_registrations(vec![registration], prepared)
        .unwrap();
    let rows = database.primary_key_scan("lazy_rows", &[]).unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(
        rows[0].get("value").unwrap(),
        Value::String("prepared".to_owned())
    );
}
