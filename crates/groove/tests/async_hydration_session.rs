#![cfg(feature = "test")]

use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll};

use futures::executor::block_on;
use futures::task::noop_waker;
use groove::db::{Database, GraphBuilder};
use groove::ivm::ProjectField;
use groove::records::{RecordDescriptor, Value};
use groove::schema::{
    ColumnSchema, ColumnType, DatabaseSchema, IndexSchema, IntegerKeyType, PrimaryKey, TableSchema,
};
use groove::storage::{TestStorage, TestStorageOperation};

fn schema() -> DatabaseSchema {
    DatabaseSchema::new([TableSchema::new(
        "albums",
        [
            ColumnSchema::new("id", ColumnType::U64),
            ColumnSchema::new("title", ColumnType::String),
        ],
    )
    .with_primary_key(PrimaryKey::new("id", IntegerKeyType::U64))])
}

#[test]
fn cancelled_hydration_publishes_no_subscription_or_partial_session() {
    let (storage, control) = TestStorage::controlled(&["albums"]);
    let mut database = block_on(Database::new(schema(), storage)).unwrap();
    let mut batch = database.open_batch();
    batch.insert(
        "albums",
        vec![Value::U64(1), Value::String("Kind of Blue".into())],
    );
    block_on(database.commit_batch(batch)).unwrap();
    let before = database.runtime_stats();

    control.take_observed();
    control.pause();
    let mut install = Box::pin(database.subscribe_one_sink(GraphBuilder::table("albums")));
    let waker = noop_waker();
    let mut context = Context::from_waker(&waker);
    assert!(matches!(
        Pin::new(&mut install).poll(&mut context),
        Poll::Pending
    ));
    assert!(control.observed().contains(&TestStorageOperation::ScanOpen));
    drop(install);

    let after = database.runtime_stats();
    assert_eq!(after.graph_nodes, before.graph_nodes);
    assert_eq!(after.active_subscriptions, before.active_subscriptions);
    control.resume();
    let subscription =
        block_on(database.subscribe_one_sink(GraphBuilder::table("albums"))).unwrap();
    assert_eq!(subscription.recv().unwrap().deltas.len(), 1);
}

#[test]
fn cancelled_prepared_bind_discards_binding_tick_and_subscription_state() {
    let (storage, control) = TestStorage::controlled(&["albums"]);
    let mut database = block_on(Database::new(schema(), storage)).unwrap();
    let mut batch = database.open_batch();
    batch.insert(
        "albums",
        vec![Value::U64(1), Value::String("Kind of Blue".into())],
    );
    block_on(database.commit_batch(batch)).unwrap();

    let binding = RecordDescriptor::new([("id", ColumnType::U64)]);
    let graph = GraphBuilder::join(
        GraphBuilder::binding_source("album_by_id", binding),
        GraphBuilder::table("albums"),
        ["id"],
        ["id"],
    )
    .project_fields([
        ProjectField::renamed("right.id", "id"),
        ProjectField::renamed("right.title", "title"),
    ]);
    let shape = block_on(database.prepare_one_sink(graph, "album_by_id", binding, ["id"])).unwrap();

    control.take_observed();
    control.pause();
    let mut install = Box::pin(database.bind_shape_one_sink(shape.id(), &[Value::U64(1)]));
    let waker = noop_waker();
    let mut context = Context::from_waker(&waker);
    assert!(matches!(
        Pin::new(&mut install).poll(&mut context),
        Poll::Pending
    ));
    drop(install);

    assert_eq!(database.runtime_stats().active_subscriptions, 0);
    control.resume();
    let subscription =
        block_on(database.bind_shape_one_sink(shape.id(), &[Value::U64(1)])).unwrap();
    assert_eq!(subscription.recv().unwrap().deltas.len(), 1);
}

#[test]
fn cancelled_one_shot_query_discards_ephemeral_graph_and_hydration_state() {
    let (storage, control) = TestStorage::controlled(&["albums"]);
    let mut database = block_on(Database::new(schema(), storage)).unwrap();
    let mut batch = database.open_batch();
    batch.insert(
        "albums",
        vec![Value::U64(1), Value::String("Kind of Blue".into())],
    );
    block_on(database.commit_batch(batch)).unwrap();
    let before = database.runtime_stats();

    control.take_observed();
    control.pause();
    let mut query = Box::pin(database.query_graph(GraphBuilder::table("albums")));
    let waker = noop_waker();
    let mut context = Context::from_waker(&waker);
    assert!(matches!(
        Pin::new(&mut query).poll(&mut context),
        Poll::Pending
    ));
    drop(query);

    let after = database.runtime_stats();
    assert_eq!(after.graph_nodes, before.graph_nodes);
    assert_eq!(after.active_subscriptions, before.active_subscriptions);
    control.resume();
    let records = block_on(database.query_graph(GraphBuilder::table("albums"))).unwrap();
    assert_eq!(records.deltas.len(), 1);
}

#[test]
fn cancelled_storage_commit_does_not_publish_the_staged_tick() {
    let (storage, control) = TestStorage::controlled(&["albums"]);
    let mut database = block_on(Database::new(schema(), storage)).unwrap();
    let subscription =
        block_on(database.subscribe_one_sink(GraphBuilder::table("albums"))).unwrap();
    assert!(subscription.recv().unwrap().is_empty());

    let mut batch = database.open_batch();
    batch.insert(
        "albums",
        vec![Value::U64(1), Value::String("Kind of Blue".into())],
    );
    control.take_observed();
    control.pause();
    let mut commit = Box::pin(database.commit_batch(batch));
    let waker = noop_waker();
    let mut context = Context::from_waker(&waker);
    for _ in 0..16 {
        assert!(matches!(
            Pin::new(&mut commit).poll(&mut context),
            Poll::Pending
        ));
        if control
            .observed()
            .contains(&TestStorageOperation::WriteMany)
        {
            break;
        }
        control.release_one();
    }
    assert!(
        control
            .observed()
            .contains(&TestStorageOperation::WriteMany)
    );
    drop(commit);

    assert!(subscription.try_recv().is_err());
    control.resume();
    let rows = block_on(database.query_graph(GraphBuilder::table("albums"))).unwrap();
    assert!(rows.is_empty());

    let mut retry = database.open_batch();
    retry.insert(
        "albums",
        vec![Value::U64(1), Value::String("Kind of Blue".into())],
    );
    block_on(database.commit_batch(retry)).unwrap();
    assert_eq!(subscription.recv().unwrap().deltas.len(), 1);
}

#[test]
fn cancelled_live_index_backfill_publishes_neither_schema_nor_durable_rows() {
    let (storage, control) = TestStorage::controlled(&["albums", "indices"]);
    let mut database = block_on(Database::new(schema(), storage)).unwrap();
    let mut batch = database.open_batch();
    batch.insert(
        "albums",
        vec![Value::U64(1), Value::String("Kind of Blue".into())],
    );
    block_on(database.commit_batch(batch)).unwrap();
    let before = database.runtime_stats();

    control.take_observed();
    control.pause();
    let index = IndexSchema::new("albums_by_title", ["title"]);
    let mut registration = Box::pin(database.register_table_index("albums", index.clone()));
    let waker = noop_waker();
    let mut context = Context::from_waker(&waker);
    for _ in 0..16 {
        assert!(matches!(
            Pin::new(&mut registration).poll(&mut context),
            Poll::Pending
        ));
        if control
            .observed()
            .contains(&TestStorageOperation::WriteMany)
        {
            break;
        }
        control.release_one();
    }
    assert!(
        control
            .observed()
            .contains(&TestStorageOperation::WriteMany),
        "live index backfill must cross the atomic durable-write boundary"
    );
    drop(registration);

    let after = database.runtime_stats();
    assert_eq!(after.graph_nodes, before.graph_nodes);
    control.resume();
    assert!(
        block_on(database.index_get(
            "albums",
            "albums_by_title",
            &[Value::String("Kind of Blue".into())],
        ))
        .is_err()
    );

    block_on(database.register_table_index("albums", index)).unwrap();
    let rows = block_on(database.index_get(
        "albums",
        "albums_by_title",
        &[Value::String("Kind of Blue".into())],
    ))
    .unwrap();
    assert_eq!(rows.len(), 1);
}
