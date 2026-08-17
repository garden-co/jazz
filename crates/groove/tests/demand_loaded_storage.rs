use std::cell::Cell;
use std::rc::Rc;
use std::task::{Context, Poll, Waker};

use groove::db::{Database, DemandDrivenDatabase, GraphBuilder};
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

fn referencing_schema() -> DatabaseSchema {
    DatabaseSchema::new([
        TableSchema::new(
            "rows",
            [
                ColumnSchema::new("id", ColumnType::U64),
                ColumnSchema::new("related_id", ColumnType::U64),
                ColumnSchema::new("value", ColumnType::String),
            ],
        )
        .with_primary_key(PrimaryKey::new("id", IntegerKeyType::U64)),
        TableSchema::new(
            "related",
            [
                ColumnSchema::new("id", ColumnType::U64),
                ColumnSchema::new("label", ColumnType::String),
            ],
        )
        .with_primary_key(PrimaryKey::new("id", IntegerKeyType::U64)),
    ])
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

#[test]
fn write_preflight_loads_inputs_before_the_single_real_ivm_tick() {
    let schema = schema();
    let cache = DemandLoadedStorage::new(&["rows", "indices"]);
    cache
        .admit(
            OwnedStorageOperation::Scan(groove::storage::pollable::OwnedScanRequest::prefix(
                "rows",
                Vec::new(),
            )),
            OwnedStorageResponse::Rows(Vec::new()),
        )
        .unwrap();
    let mut database = Database::new(schema, cache.clone()).unwrap();
    let subscription = database
        .subscribe_one_sink(GraphBuilder::table("rows"))
        .unwrap();
    assert!(subscription.recv().unwrap().is_empty());
    let mut durable = MemoryStorage::new(&["rows", "indices"]);
    let mut batch = database.open_batch();
    batch.insert("rows", vec![Value::U64(11), Value::String("input".into())]);
    let mut context = Context::from_waker(Waker::noop());

    loop {
        match database.preflight_batch_storage_inputs(&batch) {
            Ok(()) => break,
            Err(groove::db::Error::Storage(error)) => {
                let groove::storage::Error::NotResident { request } = *error else {
                    panic!("preflight failed for a reason other than missing input")
                };
                let request = OwnedStorageRequest::new(*request);
                let Poll::Ready(Ok(response)) = durable.poll_request(&request, &mut context) else {
                    panic!("memory input fetch must be immediately ready")
                };
                cache.admit(request.operation().clone(), response).unwrap();
            }
            Err(error) => panic!("unexpected preflight error: {error:?}"),
        }
    }
    assert!(
        subscription.try_recv().is_err(),
        "preflight must not mutate or publish the real IVM"
    );
    database.commit_batch(batch).unwrap();
    assert_eq!(subscription.recv().unwrap().to_values().unwrap().len(), 1);
}

#[test]
fn opened_subscription_inherits_synchronous_write_visibility_over_async_storage() {
    let schema = schema();
    let released = Rc::new(Cell::new(false));
    let polls = Rc::new(Cell::new(0));
    let durable = GatedStorage {
        inner: MemoryStorage::new(&["rows", "indices"]),
        released: Rc::clone(&released),
        polls,
    };
    let mut database = DemandDrivenDatabase::new(schema, Box::new(durable)).unwrap();
    let mut context = Context::from_waker(Waker::noop());
    let mut graph = Some(GraphBuilder::table("rows"));
    let graph_nodes_before_open = database.resident().runtime_stats().graph_nodes;
    assert!(
        database
            .poll_subscribe_one_sink(&mut context, &mut graph)
            .is_pending()
    );
    assert_eq!(
        database.resident().runtime_stats().active_subscriptions,
        0,
        "a cold preflight must not partially register the real subscription"
    );
    assert_eq!(
        database.resident().runtime_stats().graph_nodes,
        graph_nodes_before_open,
        "a suspended opening must release every staged graph node"
    );
    released.set(true);
    let Poll::Ready(Ok(subscription)) = database.poll_subscribe_one_sink(&mut context, &mut graph)
    else {
        panic!("released empty opening must register exactly once")
    };
    assert!(graph.is_none());
    assert!(subscription.recv().unwrap().is_empty());

    released.set(false);
    let mut batch = Some(database.resident().open_batch());
    batch
        .as_mut()
        .unwrap()
        .insert("rows", vec![Value::U64(12), Value::String("typed".into())]);
    let Poll::Ready(Ok(persistence)) = database.poll_commit_batch(&mut context, &mut batch) else {
        panic!("the opened working set must make the local write first-poll ready")
    };
    assert!(batch.is_none());
    assert_eq!(
        subscription.recv().unwrap().to_values().unwrap().len(),
        1,
        "the callback delta must already be queued when the write returns Ready"
    );
    assert_eq!(
        database
            .resident()
            .primary_key_scan("rows", &[Value::U64(12)])
            .unwrap()
            .len(),
        1,
        "a new one-shot must synchronously observe the resident write"
    );
    database.enqueue_persistence(persistence);
    assert!(database.poll_persistence(&mut context).is_pending());
    assert_eq!(
        database
            .resident()
            .primary_key_scan("rows", &[Value::U64(12)])
            .unwrap()
            .len(),
        1,
        "pending durability cannot hide already-published local state"
    );
    released.set(true);
    assert!(matches!(
        database.poll_persistence(&mut context),
        Poll::Ready(Ok(()))
    ));
}

#[test]
fn direct_write_is_synchronous_while_an_unloaded_reference_may_suspend() {
    let schema = referencing_schema();
    let released = Rc::new(Cell::new(true));
    let polls = Rc::new(Cell::new(0));
    let durable = GatedStorage {
        inner: MemoryStorage::new(&["rows", "related", "indices"]),
        released: Rc::clone(&released),
        polls,
    };
    let mut database = DemandDrivenDatabase::new(schema, Box::new(durable)).unwrap();
    let mut context = Context::from_waker(Waker::noop());

    // Opening the direct-row view loads only that source. The related source
    // deliberately remains absent from the working set.
    let Poll::Ready(Ok(_)) = database.poll_read(&mut context, |database| {
        database.primary_key_scan("rows", &[])
    }) else {
        panic!("released direct-row opening must load its range")
    };
    let subscription = database
        .resident_mut()
        .subscribe_one_sink(GraphBuilder::table("rows"))
        .unwrap();
    assert!(subscription.recv().unwrap().is_empty());

    released.set(false);
    let mut batch = Some(database.resident().open_batch());
    batch.as_mut().unwrap().insert(
        "rows",
        vec![
            Value::U64(21),
            Value::U64(99),
            Value::String("direct".into()),
        ],
    );
    let Poll::Ready(Ok(persistence)) = database.poll_commit_batch(&mut context, &mut batch) else {
        panic!("a direct write over its resident source must be first-poll ready")
    };
    assert_eq!(
        subscription.recv().unwrap().to_values().unwrap().len(),
        1,
        "the direct-row callback must fire before the referenced row is loaded"
    );
    assert_eq!(
        database
            .resident()
            .primary_key_scan("rows", &[Value::U64(21)])
            .unwrap()
            .len(),
        1,
        "a new direct one-shot must immediately see the write"
    );

    assert!(
        database
            .poll_read(&mut context, |database| database
                .primary_key_scan("related", &[Value::U64(99)]))
            .is_pending(),
        "expanding through an unloaded reference is allowed to suspend"
    );
    database.enqueue_persistence(persistence);
    assert!(database.poll_persistence(&mut context).is_pending());
}
