#![cfg(feature = "test")]

use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll};

use futures::executor::block_on;
use futures::task::noop_waker;
use groove::db::{Database, GraphBuilder, PrimaryKeyValue};
use groove::ivm::{AggregateExpr, AggregateFunction, PlanExpr, ProjectField, PublicationId};
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

fn indexed_schema() -> DatabaseSchema {
    DatabaseSchema::new([TableSchema::new(
        "albums",
        [
            ColumnSchema::new("id", ColumnType::U64),
            ColumnSchema::new("title", ColumnType::String),
        ],
    )
    .with_primary_key(PrimaryKey::new("id", IntegerKeyType::U64))
    .with_index(IndexSchema::new("albums_by_title", ["title"]))])
}

fn edges_schema() -> DatabaseSchema {
    DatabaseSchema::new([TableSchema::new(
        "edges",
        [
            ColumnSchema::new("id", ColumnType::U64),
            ColumnSchema::new("src", ColumnType::U64),
            ColumnSchema::new("dst", ColumnType::U64),
        ],
    )
    .with_primary_key(PrimaryKey::new("id", IntegerKeyType::U64))])
}

fn indexed_edges_schema() -> DatabaseSchema {
    DatabaseSchema::new([TableSchema::new(
        "edges",
        [
            ColumnSchema::new("id", ColumnType::U64),
            ColumnSchema::new("src", ColumnType::U64),
            ColumnSchema::new("dst", ColumnType::U64),
        ],
    )
    .with_primary_key(PrimaryKey::new("id", IntegerKeyType::U64))
    .with_index(IndexSchema::new("edges_by_src", ["src", "dst"]))])
}

fn two_metrics_schema() -> DatabaseSchema {
    DatabaseSchema::new(["left_metrics", "right_metrics"].map(|table| {
        TableSchema::new(
            table,
            [
                ColumnSchema::new("id", ColumnType::U64),
                ColumnSchema::new("bucket", ColumnType::U64),
                ColumnSchema::new("score", ColumnType::U64),
            ],
        )
        .with_primary_key(PrimaryKey::new("id", IntegerKeyType::U64))
    }))
}

fn metric_aggregate(table: &str) -> GraphBuilder {
    GraphBuilder::aggregate(
        GraphBuilder::table(table),
        ["bucket"],
        [AggregateExpr {
            function: AggregateFunction::Sum,
            expression: Some(PlanExpr::Field("score".to_owned())),
            distinct: false,
            output_name: Some("sum_score".to_owned()),
        }],
    )
}

fn edge_pairs() -> GraphBuilder {
    GraphBuilder::table("edges").project(["src", "dst"])
}

fn reachability_graph() -> GraphBuilder {
    let descriptor = RecordDescriptor::new([("src", ColumnType::U64), ("dst", ColumnType::U64)]);
    let frontier = GraphBuilder::frontier_source("frontier", descriptor);
    let step = GraphBuilder::join(frontier, edge_pairs(), ["dst"], ["src"]).project_fields([
        ProjectField::renamed("left.src", "src"),
        ProjectField::renamed("right.dst", "dst"),
    ]);
    GraphBuilder::recursive(edge_pairs(), step, "frontier", 16)
}

fn indexed_reachability_graph() -> GraphBuilder {
    let descriptor =
        RecordDescriptor::new([("key", ColumnType::Bytes), ("value", ColumnType::Bytes)]);
    GraphBuilder::recursive(
        GraphBuilder::index("edges", "edges_by_src"),
        GraphBuilder::frontier_source("frontier", descriptor),
        "frontier",
        16,
    )
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
fn hash_equal_hydration_roots_share_one_in_flight_storage_request() {
    let (storage, control) = TestStorage::controlled(&["albums"]);
    let mut database = block_on(Database::new(schema(), storage)).unwrap();

    control.take_observed();
    let subscription = block_on(database.subscribe([
        ("left", GraphBuilder::table("albums")),
        ("right", GraphBuilder::table("albums")),
    ]))
    .unwrap();
    assert!(
        subscription
            .recv()
            .unwrap()
            .sinks
            .values()
            .all(|sink| sink.is_empty())
    );
    assert_eq!(
        control
            .observed()
            .iter()
            .filter(|operation| **operation == TestStorageOperation::ScanOpen)
            .count(),
        1
    );
}

#[test]
fn blocked_index_source_retains_its_storage_request_across_polls() {
    let (storage, control) = TestStorage::controlled(&["albums", "indices"]);
    let mut database = block_on(Database::new(indexed_schema(), storage)).unwrap();
    let mut batch = database.open_batch();
    batch.insert(
        "albums",
        vec![Value::U64(1), Value::String("Kind of Blue".into())],
    );
    block_on(database.commit_batch(batch)).unwrap();

    control.take_observed();
    control.pause_on(TestStorageOperation::ScanOpen);
    let mut install =
        Box::pin(database.subscribe_one_sink(GraphBuilder::index("albums", "albums_by_title")));
    let waker = noop_waker();
    let mut context = Context::from_waker(&waker);
    assert!(matches!(install.as_mut().poll(&mut context), Poll::Pending));
    assert_eq!(
        control
            .observed()
            .iter()
            .filter(|operation| **operation == TestStorageOperation::ScanOpen)
            .count(),
        1
    );

    control.resume_operation(TestStorageOperation::ScanOpen);
    let subscription = block_on(install).unwrap();
    assert_eq!(subscription.recv().unwrap().deltas.len(), 1);
    assert_eq!(
        control
            .observed()
            .iter()
            .filter(|operation| **operation == TestStorageOperation::ScanOpen)
            .count(),
        1,
        "resuming evaluation must poll the retained request, not recreate it"
    );
}

#[test]
fn recursive_hydration_reuses_the_sessions_table_snapshot() {
    let (storage, control) = TestStorage::controlled(&["edges"]);
    let mut database = block_on(Database::new(edges_schema(), storage)).unwrap();
    let mut batch = database.open_batch();
    batch.insert("edges", vec![Value::U64(1), Value::U64(1), Value::U64(2)]);
    batch.insert("edges", vec![Value::U64(2), Value::U64(2), Value::U64(3)]);
    block_on(database.commit_batch(batch)).unwrap();

    control.take_observed();
    let subscription = block_on(database.subscribe_one_sink(reachability_graph())).unwrap();
    assert_eq!(subscription.recv().unwrap().deltas.len(), 3);
    assert_eq!(
        control
            .observed()
            .iter()
            .filter(|operation| **operation == TestStorageOperation::ScanOpen)
            .count(),
        1,
        "recursive seed, fixpoint, and arrangement seeding must reuse the session request: {:?}",
        control.observed()
    );
}

#[test]
fn blocked_recursive_index_source_retains_the_sessions_request() {
    let (storage, control) = TestStorage::controlled(&["edges", "indices"]);
    let mut database = block_on(Database::new(indexed_edges_schema(), storage)).unwrap();
    let mut batch = database.open_batch();
    batch.insert("edges", vec![Value::U64(1), Value::U64(1), Value::U64(2)]);
    batch.insert("edges", vec![Value::U64(2), Value::U64(2), Value::U64(3)]);
    block_on(database.commit_batch(batch)).unwrap();

    control.take_observed();
    control.pause_on(TestStorageOperation::ScanOpen);
    let mut install = Box::pin(database.subscribe_one_sink(indexed_reachability_graph()));
    let waker = noop_waker();
    let mut context = Context::from_waker(&waker);
    if let Poll::Ready(result) = install.as_mut().poll(&mut context) {
        match result {
            Ok(_) => panic!("recursive install completed before the paused request"),
            Err(error) => panic!("recursive install failed early: {error:?}"),
        }
    }
    assert_eq!(
        control
            .observed()
            .iter()
            .filter(|operation| **operation == TestStorageOperation::ScanOpen)
            .count(),
        1
    );

    control.resume_operation(TestStorageOperation::ScanOpen);
    let subscription = block_on(install).unwrap();
    assert_eq!(subscription.recv().unwrap().deltas.len(), 2);
    assert_eq!(
        control
            .observed()
            .iter()
            .filter(|operation| **operation == TestStorageOperation::ScanOpen)
            .count(),
        1,
        "recursive fixpoint retries must reuse the retained index request"
    );
}

#[test]
fn recursive_retraction_loads_before_mutating_the_tick() {
    let (storage, control) = TestStorage::controlled(&["edges"]);
    let mut database = block_on(Database::new(edges_schema(), storage)).unwrap();
    let mut batch = database.open_batch();
    batch.insert("edges", vec![Value::U64(1), Value::U64(1), Value::U64(2)]);
    batch.insert("edges", vec![Value::U64(2), Value::U64(2), Value::U64(3)]);
    block_on(database.commit_batch(batch)).unwrap();
    let subscription = block_on(database.subscribe_one_sink(reachability_graph())).unwrap();
    assert_eq!(subscription.recv().unwrap().deltas.len(), 3);

    control.take_observed();
    control.pause_on(TestStorageOperation::ScanOpen);
    let mut batch = database.open_batch();
    batch.delete("edges", PrimaryKeyValue::U64(2));
    let mut commit = Box::pin(database.commit_batch(batch));
    let waker = noop_waker();
    let mut context = Context::from_waker(&waker);
    assert!(matches!(commit.as_mut().poll(&mut context), Poll::Pending));
    assert_eq!(
        control
            .observed()
            .iter()
            .filter(|operation| **operation == TestStorageOperation::ScanOpen)
            .count(),
        1
    );

    control.resume_operation(TestStorageOperation::ScanOpen);
    block_on(commit).unwrap();
    assert_eq!(subscription.recv().unwrap().deltas.len(), 2);
    assert_eq!(
        control
            .observed()
            .iter()
            .filter(|operation| **operation == TestStorageOperation::ScanOpen)
            .count(),
        1,
        "recompute must consume the request retained before tick mutation"
    );
}

#[test]
fn completed_stateful_branch_is_not_reapplied_while_sibling_is_blocked() {
    let (storage, control) = TestStorage::controlled(&["left_metrics", "right_metrics"]);
    let mut database = block_on(Database::new(two_metrics_schema(), storage)).unwrap();
    let mut batch = database.open_batch();
    batch.insert(
        "left_metrics",
        vec![Value::U64(1), Value::U64(10), Value::U64(7)],
    );
    batch.insert(
        "right_metrics",
        vec![Value::U64(1), Value::U64(20), Value::U64(9)],
    );
    block_on(database.commit_batch(batch)).unwrap();

    control.take_observed();
    control.pause_on(TestStorageOperation::ScanOpen);
    let graph = GraphBuilder::union([
        metric_aggregate("left_metrics"),
        metric_aggregate("right_metrics"),
    ]);
    let mut install = Box::pin(database.subscribe_one_sink(graph));
    let waker = noop_waker();
    let mut context = Context::from_waker(&waker);
    assert!(matches!(install.as_mut().poll(&mut context), Poll::Pending));
    assert_eq!(
        control
            .observed()
            .iter()
            .filter(|operation| **operation == TestStorageOperation::ScanOpen)
            .count(),
        2,
        "the work queue discovers both blocked siblings in one pass"
    );

    control.release_one();
    assert!(matches!(install.as_mut().poll(&mut context), Poll::Pending));
    assert_eq!(
        control
            .observed()
            .iter()
            .filter(|operation| **operation == TestStorageOperation::ScanOpen)
            .count(),
        2,
        "resuming one request must not rediscover either sibling"
    );

    control.resume_operation(TestStorageOperation::ScanOpen);
    let subscription = block_on(install).unwrap();
    let initial = subscription.recv().unwrap().to_values().unwrap();
    assert_eq!(initial.len(), 2);
    assert!(initial.iter().any(|(row, weight)| {
        *weight == 1
            && row
                == &vec![
                    Value::U64(10),
                    Value::Nullable(Some(Box::new(Value::U64(7)))),
                ]
    }));
    assert!(initial.iter().any(|(row, weight)| {
        *weight == 1
            && row
                == &vec![
                    Value::U64(20),
                    Value::Nullable(Some(Box::new(Value::U64(9)))),
                ]
    }));
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
fn committed_terminal_output_carries_its_durable_publication_identity() {
    let (storage, _) = TestStorage::controlled(&["albums"]);
    let mut database = block_on(Database::new(schema(), storage)).unwrap();
    let subscription =
        block_on(database.subscribe_one_sink(GraphBuilder::table("albums"))).unwrap();
    assert!(subscription.recv().unwrap().is_empty());

    let mut first = database.open_batch();
    first.insert(
        "albums",
        vec![Value::U64(1), Value::String("Kind of Blue".into())],
    );
    block_on(database.commit_batch(first)).unwrap();
    let first_update = subscription.recv_with_publication().unwrap();
    assert_eq!(first_update.publication, Some(PublicationId(1)));
    assert_eq!(
        database.durable_publication_frontier(),
        Some(PublicationId(1))
    );

    let mut second = database.open_batch();
    second.insert(
        "albums",
        vec![Value::U64(2), Value::String("Bitches Brew".into())],
    );
    block_on(database.commit_batch(second)).unwrap();
    let second_update = subscription.recv_with_publication().unwrap();
    assert_eq!(second_update.publication, Some(PublicationId(2)));
    assert_eq!(
        database.durable_publication_frontier(),
        Some(PublicationId(2))
    );
}

#[test]
fn resident_publication_is_queryable_and_tagged_while_persistence_is_suspended() {
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
    let published = block_on(database.publish_batch(batch)).unwrap();
    assert_eq!(published.publication(), PublicationId(1));
    let update = subscription.recv_with_publication().unwrap();
    assert_eq!(update.publication, Some(PublicationId(1)));
    assert_eq!(update.deltas.deltas.len(), 1);
    assert_eq!(database.durable_publication_frontier(), None);

    control.pause_on(TestStorageOperation::WriteMany);
    let mut persistence = Box::pin(published.persist());
    let waker = noop_waker();
    let mut context = Context::from_waker(&waker);
    assert!(matches!(
        Pin::new(&mut persistence).poll(&mut context),
        Poll::Pending
    ));
    assert!(
        control
            .observed()
            .contains(&TestStorageOperation::WriteMany)
    );

    let rows = block_on(database.query_graph(GraphBuilder::table("albums"))).unwrap();
    assert_eq!(rows.deltas.len(), 1);
    assert_eq!(database.durable_publication_frontier(), None);

    drop(persistence);
    let rows = block_on(database.query_graph(GraphBuilder::table("albums"))).unwrap();
    assert_eq!(rows.deltas.len(), 1);
    assert_eq!(database.durable_publication_frontier(), None);

    control.resume_operation(TestStorageOperation::WriteMany);
    let persistence = block_on(published.persist());
    assert_eq!(
        database.settle_publication(persistence).unwrap(),
        PublicationId(1)
    );
    assert_eq!(
        database.durable_publication_frontier(),
        Some(PublicationId(1))
    );
}

#[test]
fn durable_frontier_does_not_pass_an_earlier_unsettled_publication() {
    let (storage, _) = TestStorage::controlled(&["albums"]);
    let mut database = block_on(Database::new(schema(), storage)).unwrap();

    let mut first = database.open_batch();
    first.insert(
        "albums",
        vec![Value::U64(1), Value::String("Kind of Blue".into())],
    );
    let first = block_on(database.publish_batch(first)).unwrap();

    let mut second = database.open_batch();
    second.insert(
        "albums",
        vec![Value::U64(2), Value::String("Bitches Brew".into())],
    );
    let second = block_on(database.publish_batch(second)).unwrap();

    let first_persistence = block_on(first.persist());
    let second_persistence = block_on(second.persist());
    database.settle_publication(second_persistence).unwrap();
    assert_eq!(database.durable_publication_frontier(), None);
    let rows = block_on(database.query_graph(GraphBuilder::table("albums"))).unwrap();
    assert_eq!(rows.deltas.len(), 2);

    database.settle_publication(first_persistence).unwrap();
    assert_eq!(
        database.durable_publication_frontier(),
        Some(PublicationId(2))
    );
}

#[test]
fn later_same_key_persistence_waits_for_its_predecessor() {
    let (storage, control) = TestStorage::controlled(&["albums"]);
    let mut database = block_on(Database::new(schema(), storage)).unwrap();

    let mut first = database.open_batch();
    first.insert(
        "albums",
        vec![Value::U64(1), Value::String("Kind of Blue".into())],
    );
    let first = block_on(database.publish_batch(first)).unwrap();

    let mut second = database.open_batch();
    second.update(
        "albums",
        vec![Value::U64(1), Value::String("Blue in Green".into())],
    );
    let second = block_on(database.publish_batch(second)).unwrap();

    control.take_observed();
    let mut second_persistence = Box::pin(second.persist());
    let waker = noop_waker();
    let mut context = Context::from_waker(&waker);
    assert!(matches!(
        Pin::new(&mut second_persistence).poll(&mut context),
        Poll::Pending
    ));
    assert!(
        !control
            .observed()
            .contains(&TestStorageOperation::WriteMany)
    );

    let first_persistence = block_on(first.persist());
    let second_persistence = block_on(second_persistence);
    database.settle_publication(second_persistence).unwrap();
    assert_eq!(database.durable_publication_frontier(), None);
    database.settle_publication(first_persistence).unwrap();
    assert_eq!(
        database.durable_publication_frontier(),
        Some(PublicationId(2))
    );

    let rows = block_on(database.query_graph(GraphBuilder::table("albums"))).unwrap();
    assert_eq!(
        rows.to_values().unwrap(),
        vec![(
            vec![Value::U64(1), Value::String("Blue in Green".into())],
            1
        )]
    );
}

#[test]
fn publishing_an_insert_into_a_resident_table_does_not_wait_for_storage() {
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
    control.pause_on(TestStorageOperation::WriteMany);
    let mut publication = Box::pin(database.publish_batch(batch));
    let waker = noop_waker();
    let mut context = Context::from_waker(&waker);
    let published = match Pin::new(&mut publication).poll(&mut context) {
        Poll::Ready(Ok(published)) => published,
        Poll::Ready(Err(error)) => panic!("resident publication failed: {error}"),
        Poll::Pending => panic!(
            "resident publication suspended on storage: {:?}",
            control.observed()
        ),
    };
    drop(publication);
    assert_eq!(published.publication(), PublicationId(1));
    assert_eq!(
        subscription.recv_with_publication().unwrap().publication,
        Some(PublicationId(1))
    );
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
