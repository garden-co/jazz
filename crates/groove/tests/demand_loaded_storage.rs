use std::cell::Cell;
use std::rc::Rc;
use std::task::{Context, Poll, Waker};

use groove::db::{Database, DemandDrivenDatabase};
use groove::records::Value;
use groove::schema::{
    ColumnSchema, ColumnType, DatabaseSchema, IntegerKeyType, PrimaryKey, TableSchema,
};
use groove::storage::pollable::{
    OwnedStorageOperation, OwnedStorageRequest, OwnedStorageResponse, PollableOrderedKvStorage,
    StorageRequestId,
};
use groove::storage::{DemandLoadedStorage, Error, MemoryStorage, OrderedKvStorage};

struct GatedStorage {
    inner: MemoryStorage,
    released: Rc<Cell<bool>>,
    polls: Rc<Cell<usize>>,
}

impl PollableOrderedKvStorage for GatedStorage {
    fn poll_request(
        &mut self,
        request: &OwnedStorageRequest,
        context: &mut Context<'_>,
    ) -> Poll<Result<OwnedStorageResponse, Error>> {
        self.polls.set(self.polls.get() + 1);
        if !self.released.get() {
            Poll::Pending
        } else {
            self.inner.poll_request(request, context)
        }
    }

    fn cancel_request(&mut self, _request: StorageRequestId) -> Result<(), Error> {
        Ok(())
    }
}

fn schema() -> DatabaseSchema {
    DatabaseSchema::new([TableSchema::new(
        "rows",
        [
            ColumnSchema::new("id", ColumnType::U64),
            ColumnSchema::new("value", ColumnType::String),
        ],
    )
    .with_primary_key(PrimaryKey::new("id", IntegerKeyType::U64))])
}

#[test]
fn query_requests_only_its_missing_range_then_retries_from_cache() {
    let schema = schema();
    let durable = MemoryStorage::new(&["rows", "indices"]);
    let mut seeded = Database::new(schema.clone(), durable).unwrap();
    let mut batch = seeded.open_batch();
    batch.insert("rows", vec![Value::U64(7), Value::String("durable".into())]);
    seeded.commit_batch(batch).unwrap();
    let mut durable = seeded.into_storage();

    let cache = DemandLoadedStorage::new(&["rows", "indices"]);
    let database = Database::new(schema, cache.clone()).unwrap();
    let error = database
        .primary_key_scan("rows", &[])
        .expect_err("an unfilled working set must not masquerade as empty storage");
    let groove::db::Error::Storage(error) = error else {
        panic!("query miss must retain its storage demand")
    };
    let groove::storage::Error::NotResident { request } = *error else {
        panic!("query miss must be an owned storage request")
    };
    let request = OwnedStorageRequest::new(*request);
    let mut context = Context::from_waker(Waker::noop());
    let Poll::Ready(Ok(response)) = durable.poll_request(&request, &mut context) else {
        panic!("memory durability must answer through the same first-poll path")
    };
    cache.admit(request.operation().clone(), response).unwrap();

    let rows = database.primary_key_scan("rows", &[]).unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(
        rows[0].get("value").unwrap(),
        Value::String("durable".into())
    );
}

#[test]
fn stale_fetch_completion_cannot_overwrite_a_synchronous_local_write() {
    let cache = DemandLoadedStorage::new(&["rows"]);
    let request = OwnedStorageOperation::Get {
        column_family: "rows".to_owned(),
        key: b"row-1".to_vec(),
    };
    assert!(matches!(
        cache.get("rows", b"row-1"),
        Err(groove::storage::Error::NotResident { .. })
    ));

    cache.set("rows", b"row-1", b"local").unwrap();
    cache
        .admit(
            request,
            OwnedStorageResponse::Value(Some(b"stale durable".to_vec())),
        )
        .unwrap();
    assert_eq!(
        cache.get("rows", b"row-1").unwrap(),
        Some(b"local".to_vec())
    );
}

#[test]
fn pollable_query_fetches_on_demand_and_then_reads_resident_state() {
    let schema = schema();
    let durable = MemoryStorage::new(&["rows", "indices"]);
    let mut seeded = Database::new(schema.clone(), durable).unwrap();
    let mut batch = seeded.open_batch();
    batch.insert("rows", vec![Value::U64(8), Value::String("lazy".into())]);
    seeded.commit_batch(batch).unwrap();
    let released = Rc::new(Cell::new(false));
    let polls = Rc::new(Cell::new(0));
    let durable = GatedStorage {
        inner: seeded.into_storage(),
        released: Rc::clone(&released),
        polls: Rc::clone(&polls),
    };
    let mut database = DemandDrivenDatabase::new(schema, Box::new(durable)).unwrap();
    let mut context = Context::from_waker(Waker::noop());
    let read = |database: &Database<DemandLoadedStorage>| database.primary_key_scan("rows", &[]);

    assert!(database.poll_read(&mut context, read).is_pending());
    assert_eq!(polls.get(), 1);
    released.set(true);
    let Poll::Ready(Ok(rows)) = database.poll_read(&mut context, read) else {
        panic!("released range demand must resume the query")
    };
    assert_eq!(rows.len(), 1);
    let polls_after_hydration = polls.get();
    assert!(matches!(
        database.poll_read(&mut context, read),
        Poll::Ready(Ok(_))
    ));
    assert_eq!(
        polls.get(),
        polls_after_hydration,
        "the same one-shot must be served entirely from its resident working set"
    );
}
