#![cfg(feature = "test")]

use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::task::{Context, Poll};

use futures::executor::block_on;
use futures::task::{ArcWake, noop_waker, waker};
use groove::db::{
    Database, Error as DatabaseError, GraphBuilder, NotificationTiming, PrimaryKeyValue,
};
use groove::ivm::{
    AggregateExpr, AggregateFunction, LiteralValue, PlanExpr, ProjectField, PublicationId,
    StaticScanSpec,
};
use groove::records::{RecordDescriptor, Value, ValueType, VariantRecord};
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

fn variant_indexed_schema() -> DatabaseSchema {
    DatabaseSchema::new([TableSchema::new(
        "albums",
        [
            ColumnSchema::new("id", ColumnType::U64),
            ColumnSchema::new("title", ColumnType::String),
        ],
    )
    .with_primary_key(PrimaryKey::new("id", IntegerKeyType::U64))
    .with_index(IndexSchema::new("albums_by_title", ["title"]))
    .with_variant(1, ["title", "id"])])
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

fn albums_and_edges_schema() -> DatabaseSchema {
    DatabaseSchema::new([
        TableSchema::new(
            "albums",
            [
                ColumnSchema::new("id", ColumnType::U64),
                ColumnSchema::new("title", ColumnType::String),
            ],
        )
        .with_primary_key(PrimaryKey::new("id", IntegerKeyType::U64)),
        TableSchema::new(
            "edges",
            [
                ColumnSchema::new("id", ColumnType::U64),
                ColumnSchema::new("src", ColumnType::U64),
                ColumnSchema::new("dst", ColumnType::U64),
            ],
        )
        .with_primary_key(PrimaryKey::new("id", IntegerKeyType::U64)),
    ])
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
    let subscription =
        block_on(database.subscribe_one_sink(GraphBuilder::table("albums"))).unwrap();
    assert!(subscription.try_recv().is_err());
    let mut progress = Box::pin(database.drive_progress());
    let waker = noop_waker();
    let mut context = Context::from_waker(&waker);
    assert!(matches!(
        Pin::new(&mut progress).poll(&mut context),
        Poll::Pending
    ));
    assert!(control.observed().contains(&TestStorageOperation::ScanOpen));
    drop(progress);
    assert!(database.unsubscribe(subscription.id()));
    drop(subscription);

    // This is necessarily an internal async-storage test: the contract is
    // that cancelling a subscription also drops its private suspended work,
    // so a runtime owner must not remain blocked on a cold scan with no
    // externally observable subscriber.
    let mut cancellation_progress = Box::pin(database.drive_progress());
    assert!(matches!(
        Pin::new(&mut cancellation_progress).poll(&mut context),
        Poll::Ready(Ok(()))
    ));
    drop(cancellation_progress);

    let after = database.runtime_stats();
    assert_eq!(after.graph_nodes, before.graph_nodes);
    assert_eq!(after.active_subscriptions, before.active_subscriptions);
    control.resume();
    let subscription =
        block_on(database.subscribe_one_sink(GraphBuilder::table("albums"))).unwrap();
    assert_eq!(
        block_on(database.next_subscription(&subscription))
            .unwrap()
            .deltas
            .len(),
        1
    );
}

#[test]
fn cancelling_cold_hydration_releases_a_later_shared_subscription() {
    // This is necessarily an internal async-storage test: it exercises the
    // runtime's temporal handoff between two private hydration sessions that
    // share graph nodes. Public delivery alone cannot prove that cancellation
    // released the successor rather than merely leaving the old session alive.
    let (storage, control) = TestStorage::controlled(&["albums"]);
    let mut database = block_on(Database::new(schema(), storage)).unwrap();
    let mut batch = database.open_batch();
    batch.insert(
        "albums",
        vec![Value::U64(1), Value::String("Kind of Blue".into())],
    );
    block_on(database.commit_batch(batch)).unwrap();

    control.pause();
    let first = block_on(database.subscribe_one_sink(GraphBuilder::table("albums"))).unwrap();
    let second = block_on(database.subscribe_one_sink(GraphBuilder::table("albums"))).unwrap();
    let mut progress = Box::pin(database.drive_progress());
    let waker = noop_waker();
    let mut context = Context::from_waker(&waker);
    assert!(matches!(
        Pin::new(&mut progress).poll(&mut context),
        Poll::Pending
    ));
    drop(progress);

    assert!(database.unsubscribe(first.id()));
    drop(first);
    control.resume();
    block_on(database.drive_progress()).unwrap();
    assert_eq!(
        block_on(database.next_subscription(&second))
            .unwrap()
            .deltas
            .len(),
        1
    );
}

#[test]
fn write_during_cold_subscription_hydration_is_delivered_exactly_once() {
    let (storage, control) = TestStorage::controlled(&["albums"]);
    let mut database = block_on(Database::new(schema(), storage)).unwrap();
    let mut seed = database.open_batch();
    seed.insert(
        "albums",
        vec![Value::U64(1), Value::String("Kind of Blue".into())],
    );
    block_on(database.commit_batch(seed)).unwrap();

    control.take_observed();
    control.pause_on(TestStorageOperation::ScanOpen);
    let subscription =
        block_on(database.subscribe_one_sink(GraphBuilder::table("albums"))).unwrap();
    assert!(subscription.try_recv().is_err());

    let mut progress = Box::pin(database.drive_progress());
    let waker = noop_waker();
    let mut context = Context::from_waker(&waker);
    assert!(matches!(
        progress.as_mut().poll(&mut context),
        Poll::Pending
    ));
    drop(progress);

    let mut write = database.open_batch();
    write.insert(
        "albums",
        vec![Value::U64(2), Value::String("Blue Train".into())],
    );
    let publication = block_on(database.apply_batch(write)).unwrap();

    control.resume_operation(TestStorageOperation::ScanOpen);
    block_on(database.drive_progress()).unwrap();

    let mut cardinality: Vec<(Vec<Value>, i64)> = Vec::new();
    let first = subscription.recv().unwrap();
    for (row, weight) in first.to_values().unwrap() {
        if let Some((_, current)) = cardinality.iter_mut().find(|(seen, _)| *seen == row) {
            *current += weight;
        } else {
            cardinality.push((row, weight));
        }
    }
    while let Ok(update) = subscription.try_recv() {
        for (row, weight) in update.to_values().unwrap() {
            if let Some((_, current)) = cardinality.iter_mut().find(|(seen, _)| *seen == row) {
                *current += weight;
            } else {
                cardinality.push((row, weight));
            }
        }
    }
    assert_eq!(cardinality.len(), 2);
    assert!(cardinality.contains(&(vec![Value::U64(1), Value::String("Kind of Blue".into())], 1,)));
    assert!(
        cardinality.contains(&(vec![Value::U64(2), Value::String("Blue Train".into())], 1,)),
        "the hydration snapshot and queued incremental update must not overlap"
    );

    let persistence = block_on(publication.persist());
    database.finish_persistence(persistence).unwrap();
}

#[test]
fn hash_equal_hydration_roots_share_one_in_flight_storage_request() {
    let (storage, control) = TestStorage::controlled(&["albums"]);
    let mut database = block_on(Database::new(schema(), storage)).unwrap();

    control.take_observed();
    let subscription = database
        .subscribe([
            ("left", GraphBuilder::table("albums")),
            ("right", GraphBuilder::table("albums")),
        ])
        .unwrap();
    assert!(
        block_on(database.next_multisink_subscription(&subscription))
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
    let subscription =
        block_on(database.subscribe_one_sink(GraphBuilder::index("albums", "albums_by_title")))
            .unwrap();
    let mut progress = Box::pin(database.drive_progress());
    let waker = noop_waker();
    let mut context = Context::from_waker(&waker);
    assert!(matches!(
        progress.as_mut().poll(&mut context),
        Poll::Pending
    ));
    assert_eq!(
        control
            .observed()
            .iter()
            .filter(|operation| **operation == TestStorageOperation::ScanOpen)
            .count(),
        1
    );

    control.resume_operation(TestStorageOperation::ScanOpen);
    block_on(progress).unwrap();
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
fn projected_indexed_rows_discover_and_hydrate_referenced_rows() {
    let schema = variant_indexed_schema();
    let (storage, control) = TestStorage::controlled(&schema.column_families());
    let mut database = block_on(Database::new(schema, storage.clone())).unwrap();
    let physical = RecordDescriptor::new([("title", ValueType::String), ("id", ValueType::U64)]);
    let output = RecordDescriptor::new([("id", ValueType::U64), ("title", ValueType::String)]);
    database
        .define_variant_projection("albums", "logical-album", output)
        .unwrap();
    database
        .register_variant_projection_case(
            "albums",
            "logical-album",
            1,
            [ProjectField::named("id"), ProjectField::named("title")],
        )
        .unwrap();
    let mut batch = database.open_batch();
    batch.insert(
        "albums",
        VariantRecord::create(
            1,
            physical,
            &[Value::String("Kind of Blue".into()), Value::U64(1)],
        )
        .unwrap(),
    );
    batch.insert(
        "albums",
        VariantRecord::create(
            1,
            physical,
            &[Value::String("Kind of Blue".into()), Value::U64(2)],
        )
        .unwrap(),
    );
    block_on(database.commit_batch(batch)).unwrap();

    storage.evict_column_family("albums");
    storage.evict_column_family("indices");
    control.take_observed();
    control.pause_on(TestStorageOperation::ScanOpen);
    control.pause_on(TestStorageOperation::Get);
    let graph = GraphBuilder::variant_index_scan(
        "albums",
        "albums_by_title",
        "logical-album",
        StaticScanSpec::Prefix(vec![LiteralValue::String("Kind of Blue".into())]),
    );
    let subscription = block_on(database.subscribe_one_sink(graph)).unwrap();
    let mut progress = Box::pin(database.drive_progress());
    let waker = noop_waker();
    let mut context = Context::from_waker(&waker);
    assert!(matches!(
        progress.as_mut().poll(&mut context),
        Poll::Pending
    ));
    assert!(control.observed().contains(&TestStorageOperation::ScanOpen));
    assert!(!control.observed().contains(&TestStorageOperation::Get));

    control.resume_operation(TestStorageOperation::ScanOpen);
    for _ in 0..8 {
        assert!(matches!(
            progress.as_mut().poll(&mut context),
            Poll::Pending
        ));
        if control.observed().contains(&TestStorageOperation::Get) {
            break;
        }
    }
    assert_eq!(
        control
            .observed()
            .iter()
            .filter(|operation| **operation == TestStorageOperation::Get)
            .count(),
        2,
        "all sibling row loads must be discovered and started together"
    );

    control.resume_operation(TestStorageOperation::Get);
    block_on(progress).unwrap();
    assert_eq!(
        subscription.recv().unwrap().to_values().unwrap(),
        vec![
            (vec![Value::U64(1), Value::String("Kind of Blue".into())], 1,),
            (vec![Value::U64(2), Value::String("Kind of Blue".into())], 1,)
        ]
    );

    let mut batch = database.open_batch();
    batch.insert(
        "albums",
        VariantRecord::create(
            1,
            physical,
            &[Value::String("Kind of Blue".into()), Value::U64(3)],
        )
        .unwrap(),
    );
    batch.insert(
        "albums",
        VariantRecord::create(
            1,
            physical,
            &[Value::String("Blue Train".into()), Value::U64(4)],
        )
        .unwrap(),
    );
    block_on(database.commit_batch(batch)).unwrap();
    assert_eq!(
        subscription.recv().unwrap().to_values().unwrap(),
        vec![(vec![Value::U64(3), Value::String("Kind of Blue".into())], 1,)],
        "incremental table deltas must use the same index predicate"
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
    assert_eq!(
        block_on(database.next_subscription(&subscription))
            .unwrap()
            .deltas
            .len(),
        3
    );
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
    let subscription = block_on(database.subscribe_one_sink(indexed_reachability_graph())).unwrap();
    let mut progress = Box::pin(database.drive_progress());
    let waker = noop_waker();
    let mut context = Context::from_waker(&waker);
    if let Poll::Ready(result) = progress.as_mut().poll(&mut context) {
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
    block_on(progress).unwrap();
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
    let mut database = block_on(Database::new(edges_schema(), storage.clone())).unwrap();
    let mut batch = database.open_batch();
    batch.insert("edges", vec![Value::U64(1), Value::U64(1), Value::U64(2)]);
    batch.insert("edges", vec![Value::U64(2), Value::U64(2), Value::U64(3)]);
    block_on(database.commit_batch(batch)).unwrap();
    let subscription = block_on(database.subscribe_one_sink(reachability_graph())).unwrap();
    assert_eq!(
        block_on(database.next_subscription(&subscription))
            .unwrap()
            .deltas
            .len(),
        3
    );

    storage.evict_scans("edges");
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
    block_on(database.drive_progress()).unwrap();
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
fn resident_terminal_publishes_while_independent_recursive_terminal_is_blocked() {
    let (storage, control) = TestStorage::controlled(&["albums", "edges"]);
    let mut database = block_on(Database::new(albums_and_edges_schema(), storage.clone())).unwrap();
    let mut seed = database.open_batch();
    seed.insert("edges", vec![Value::U64(1), Value::U64(1), Value::U64(2)]);
    seed.insert("edges", vec![Value::U64(2), Value::U64(2), Value::U64(3)]);
    let seeded = block_on(database.apply_batch(seed)).unwrap();
    let persistence = block_on(seeded.persist());
    database.finish_persistence(persistence).unwrap();

    let albums = block_on(database.subscribe_one_sink(GraphBuilder::table("albums"))).unwrap();
    assert!(
        block_on(database.next_subscription(&albums))
            .unwrap()
            .is_empty()
    );
    let reach = block_on(database.subscribe_one_sink(reachability_graph())).unwrap();
    assert_eq!(
        block_on(database.next_subscription(&reach))
            .unwrap()
            .deltas
            .len(),
        3
    );

    storage.evict_scans("edges");
    control.take_observed();
    control.pause_on(TestStorageOperation::ScanOpen);
    let mut batch = database.open_batch();
    batch.insert(
        "albums",
        vec![Value::U64(1), Value::String("Speak No Evil".into())],
    );
    batch.delete("edges", PrimaryKeyValue::U64(2));
    let mut publication = Box::pin(database.apply_batch(batch));
    let waker = noop_waker();
    let mut context = Context::from_waker(&waker);
    let Poll::Ready(published) = publication.as_mut().poll(&mut context) else {
        panic!("resident publication must not wait for independent hydration");
    };
    let published = published.unwrap();
    let publication_id = published.publication();

    assert_eq!(albums.try_recv().unwrap().deltas.len(), 1);
    assert!(reach.try_recv().is_err());

    control.resume_operation(TestStorageOperation::ScanOpen);
    drop(publication);
    let resumed = block_on(database.next_subscription_with_publication(&reach)).unwrap();
    assert_eq!(resumed.publication, Some(publication_id));
    assert_eq!(resumed.deltas.deltas.len(), 2);
    let persistence = block_on(published.persist());
    database.finish_persistence(persistence).unwrap();
}

#[test]
fn after_persistence_batch_holds_notifications_until_receipt_is_finished() {
    let storage = TestStorage::new(&["albums"]);
    let mut database = block_on(Database::new(schema(), storage)).unwrap();
    let subscription =
        block_on(database.subscribe_one_sink(GraphBuilder::table("albums"))).unwrap();
    assert!(
        block_on(database.next_subscription(&subscription))
            .unwrap()
            .is_empty()
    );

    let mut batch = database.open_batch();
    batch.deliver_notifications(NotificationTiming::AfterPersistence);
    batch.insert(
        "albums",
        vec![Value::U64(1), Value::String("Speak No Evil".into())],
    );
    let applied = block_on(database.apply_batch(batch)).unwrap();
    assert!(subscription.try_recv().is_err());

    let persisted = block_on(applied.persist());
    assert!(subscription.try_recv().is_err());
    database.finish_persistence(persisted).unwrap();
    assert_eq!(subscription.recv().unwrap().deltas.len(), 1);
}

#[test]
fn hydration_failure_ends_only_affected_terminal_and_releases_later_work() {
    let (storage, control) = TestStorage::controlled(&["albums", "edges"]);
    let mut database = block_on(Database::new(albums_and_edges_schema(), storage.clone())).unwrap();
    let mut seed = database.open_batch();
    seed.insert("edges", vec![Value::U64(1), Value::U64(1), Value::U64(2)]);
    seed.insert("edges", vec![Value::U64(2), Value::U64(2), Value::U64(3)]);
    let seeded = block_on(database.apply_batch(seed)).unwrap();
    let persistence = block_on(seeded.persist());
    database.finish_persistence(persistence).unwrap();

    let albums = block_on(database.subscribe_one_sink(GraphBuilder::table("albums"))).unwrap();
    assert!(
        block_on(database.next_subscription(&albums))
            .unwrap()
            .is_empty()
    );
    let failed_reach = block_on(database.subscribe_one_sink(reachability_graph())).unwrap();
    assert_eq!(
        block_on(database.next_subscription(&failed_reach))
            .unwrap()
            .deltas
            .len(),
        3
    );
    let shared_failed_reach = block_on(database.subscribe_one_sink(reachability_graph())).unwrap();
    assert_eq!(
        block_on(database.next_subscription(&shared_failed_reach))
            .unwrap()
            .deltas
            .len(),
        3
    );

    storage.evict_scans("edges");
    control.pause_on(TestStorageOperation::ScanOpen);
    control.fail_next(TestStorageOperation::ScanOpen);
    let mut batch = database.open_batch();
    batch.insert(
        "albums",
        vec![Value::U64(1), Value::String("Speak No Evil".into())],
    );
    batch.delete("edges", PrimaryKeyValue::U64(2));
    let published = block_on(database.apply_batch(batch)).unwrap();
    assert_eq!(albums.recv().unwrap().deltas.len(), 1);

    control.resume_operation(TestStorageOperation::ScanOpen);
    assert!(matches!(
        block_on(database.next_subscription(&failed_reach)),
        Err(DatabaseError::SubscriptionFailed(_))
    ));
    assert!(matches!(
        block_on(database.next_subscription(&shared_failed_reach)),
        Err(DatabaseError::SubscriptionFailed(_))
    ));
    assert!(matches!(
        block_on(database.next_subscription(&failed_reach)),
        Err(DatabaseError::SubscriptionEnded)
    ));
    let reinstalled = block_on(database.subscribe_one_sink(reachability_graph())).unwrap();
    assert_eq!(
        block_on(database.next_subscription(&reinstalled))
            .unwrap()
            .deltas
            .len(),
        1
    );
    let persistence = block_on(published.persist());
    database.finish_persistence(persistence).unwrap();

    let mut later = database.open_batch();
    later.insert("albums", vec![Value::U64(2), Value::String("JuJu".into())]);
    let later = block_on(database.apply_batch(later)).unwrap();
    assert_eq!(albums.recv().unwrap().deltas.len(), 1);

    let persistence = block_on(later.persist());
    database.finish_persistence(persistence).unwrap();

    storage.evict_scans("edges");
    control.fail_next(TestStorageOperation::ScanOpen);
    let mut immediate_failure = database.open_batch();
    immediate_failure.insert(
        "albums",
        vec![Value::U64(3), Value::String("Adam's Apple".into())],
    );
    immediate_failure.delete("edges", PrimaryKeyValue::U64(1));
    let immediate_failure = block_on(database.apply_batch(immediate_failure)).unwrap();
    assert_eq!(albums.recv().unwrap().deltas.len(), 1);
    assert!(matches!(
        block_on(database.next_subscription(&reinstalled)),
        Err(DatabaseError::SubscriptionFailed(_))
    ));
    let persistence = block_on(immediate_failure.persist());
    database.finish_persistence(persistence).unwrap();
}

#[test]
fn resident_publication_returns_before_independent_recursive_hydration() {
    let (storage, control) = TestStorage::controlled(&["albums", "edges"]);
    let mut database = block_on(Database::new(albums_and_edges_schema(), storage)).unwrap();
    let mut seed = database.open_batch();
    seed.insert("edges", vec![Value::U64(1), Value::U64(1), Value::U64(2)]);
    seed.insert("edges", vec![Value::U64(2), Value::U64(2), Value::U64(3)]);
    block_on(database.commit_batch(seed)).unwrap();

    let albums = block_on(database.subscribe_one_sink(GraphBuilder::table("albums"))).unwrap();
    assert!(
        block_on(database.next_subscription(&albums))
            .unwrap()
            .is_empty()
    );
    let reach = block_on(database.subscribe_one_sink(reachability_graph())).unwrap();
    assert_eq!(
        block_on(database.next_subscription(&reach))
            .unwrap()
            .deltas
            .len(),
        3
    );
    control.take_observed();
    control.pause_on(TestStorageOperation::ScanOpen);

    let mut batch = database.open_batch();
    batch.insert(
        "albums",
        vec![Value::U64(1), Value::String("Speak No Evil".into())],
    );
    batch.delete("edges", PrimaryKeyValue::U64(2));
    let mut publication = Box::pin(database.apply_batch(batch));
    let waker = noop_waker();
    let mut context = Context::from_waker(&waker);
    let Poll::Ready(result) = publication.as_mut().poll(&mut context) else {
        panic!("resident publication must not wait for independent hydration");
    };
    let _published = result.unwrap();
    drop(publication);

    control.resume_operation(TestStorageOperation::ScanOpen);
    let mut query = Box::pin(database.query_graph(GraphBuilder::table("albums")));
    let Poll::Ready(rows) = query.as_mut().poll(&mut context) else {
        panic!("resident one-shot query must complete in its first poll");
    };
    let rows = rows.unwrap();
    assert_eq!(rows.deltas.len(), 1);
    drop(query);

    assert_eq!(
        block_on(database.next_subscription(&reach))
            .unwrap()
            .deltas
            .len(),
        2
    );
}

#[test]
fn later_resident_tick_runs_while_earlier_recursive_tick_is_suspended() {
    let (storage, control) = TestStorage::controlled(&["albums", "edges"]);
    let mut database = block_on(Database::new(albums_and_edges_schema(), storage.clone())).unwrap();
    let mut seed = database.open_batch();
    seed.insert("edges", vec![Value::U64(1), Value::U64(1), Value::U64(2)]);
    seed.insert("edges", vec![Value::U64(2), Value::U64(2), Value::U64(3)]);
    block_on(database.commit_batch(seed)).unwrap();

    let albums = block_on(database.subscribe_one_sink(GraphBuilder::table("albums"))).unwrap();
    assert!(
        block_on(database.next_subscription(&albums))
            .unwrap()
            .is_empty()
    );
    let reach = block_on(database.subscribe_one_sink(reachability_graph())).unwrap();
    assert_eq!(
        block_on(database.next_subscription(&reach))
            .unwrap()
            .deltas
            .len(),
        3
    );

    storage.evict_scans("edges");
    control.take_observed();
    control.pause_on(TestStorageOperation::ScanOpen);
    let mut first = database.open_batch();
    first.insert(
        "albums",
        vec![Value::U64(1), Value::String("Speak No Evil".into())],
    );
    first.delete("edges", PrimaryKeyValue::U64(2));
    let mut first = Box::pin(database.apply_batch(first));
    let waker = noop_waker();
    let mut context = Context::from_waker(&waker);
    let first_applied = match first.as_mut().poll(&mut context) {
        Poll::Ready(Ok(applied)) => applied,
        _ => panic!("resident application must complete immediately"),
    };
    drop(first);
    assert_eq!(albums.try_recv().unwrap().deltas.len(), 1);

    let mut applied = vec![first_applied];
    for id in 2..=33 {
        let mut later = database.open_batch();
        later.insert(
            "albums",
            vec![Value::U64(id), Value::String(format!("resident-{id}"))],
        );
        let mut later = Box::pin(database.apply_batch(later));
        let later_applied = match later.as_mut().poll(&mut context) {
            Poll::Ready(Ok(applied)) => applied,
            _ => panic!("later resident application must complete immediately"),
        };
        drop(later);
        applied.push(later_applied);
        assert_eq!(albums.try_recv().unwrap().deltas.len(), 1);
    }
    assert!(reach.try_recv().is_err());
    assert_eq!(
        control
            .observed()
            .iter()
            .filter(|operation| **operation == TestStorageOperation::ScanOpen)
            .count(),
        1,
        "later resident ticks must not retry the suspended request"
    );

    control.resume_operation(TestStorageOperation::ScanOpen);
    assert_eq!(
        block_on(database.next_subscription(&reach))
            .unwrap()
            .deltas
            .len(),
        2
    );
    for publication in applied {
        let persistence = block_on(publication.persist());
        database.finish_persistence(persistence).unwrap();
    }
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
    let subscription = block_on(database.subscribe_one_sink(graph)).unwrap();
    let mut progress = Box::pin(database.drive_progress());
    let waker = noop_waker();
    let mut context = Context::from_waker(&waker);
    assert!(matches!(
        progress.as_mut().poll(&mut context),
        Poll::Pending
    ));
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
    assert!(matches!(
        progress.as_mut().poll(&mut context),
        Poll::Pending
    ));
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
    block_on(progress).unwrap();
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
    let subscription =
        block_on(database.bind_shape_one_sink(shape.id(), &[Value::U64(1)])).unwrap();
    assert!(subscription.try_recv().is_err());
    let mut progress = Box::pin(database.drive_progress());
    let waker = noop_waker();
    let mut context = Context::from_waker(&waker);
    assert!(matches!(
        Pin::new(&mut progress).poll(&mut context),
        Poll::Pending
    ));
    drop(progress);
    assert!(database.unsubscribe(subscription.id()));
    drop(subscription);

    assert_eq!(database.runtime_stats().active_subscriptions, 0);
    control.resume();
    let subscription =
        block_on(database.bind_shape_one_sink(shape.id(), &[Value::U64(1)])).unwrap();
    assert_eq!(
        block_on(database.next_subscription(&subscription))
            .unwrap()
            .deltas
            .len(),
        1
    );
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
fn cancelled_started_persistence_poisoned_database_cannot_retry_or_roll_back() {
    let (storage, control) = TestStorage::controlled(&["albums"]);
    let mut database = block_on(Database::new(schema(), storage)).unwrap();
    let subscription =
        block_on(database.subscribe_one_sink(GraphBuilder::table("albums"))).unwrap();
    assert!(
        block_on(database.next_subscription(&subscription))
            .unwrap()
            .is_empty()
    );

    let mut batch = database.open_batch();
    batch.insert(
        "albums",
        vec![Value::U64(1), Value::String("Kind of Blue".into())],
    );
    let applied = block_on(database.apply_batch(batch)).unwrap();
    assert_eq!(subscription.recv().unwrap().deltas.len(), 1);
    assert_eq!(
        block_on(database.query_graph(GraphBuilder::table("albums")))
            .unwrap()
            .deltas
            .len(),
        1
    );

    control.take_observed();
    control.pause();
    let mut persistence = Box::pin(applied.persist());
    let waker = noop_waker();
    let mut context = Context::from_waker(&waker);
    for _ in 0..16 {
        assert!(matches!(
            Pin::new(&mut persistence).poll(&mut context),
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
    drop(persistence);

    assert!(subscription.try_recv().is_err());
    control.resume();
    assert!(matches!(
        database.ensure_usable(),
        Err(DatabaseError::DatabasePoisoned)
    ));
    assert!(matches!(
        block_on(database.query_graph(GraphBuilder::table("albums"))),
        Err(DatabaseError::DatabasePoisoned)
    ));
    drop(applied);
}

#[test]
fn cancelled_started_persistence_wakes_queued_publication_with_order_failure() {
    struct WakeCount(AtomicUsize);

    impl ArcWake for WakeCount {
        fn wake_by_ref(arc_self: &Arc<Self>) {
            arc_self.0.fetch_add(1, Ordering::AcqRel);
        }
    }

    let (storage, control) = TestStorage::controlled(&["albums"]);
    let mut database = block_on(Database::new(schema(), storage)).unwrap();

    let mut first = database.open_batch();
    first.insert(
        "albums",
        vec![Value::U64(1), Value::String("Kind of Blue".into())],
    );
    let first = block_on(database.apply_batch(first)).unwrap();

    let mut second = database.open_batch();
    second.insert(
        "albums",
        vec![Value::U64(2), Value::String("Blue Train".into())],
    );
    let second = block_on(database.apply_batch(second)).unwrap();

    control.pause_on(TestStorageOperation::WriteMany);
    let mut first_persistence = Box::pin(first.persist());
    let first_waker = noop_waker();
    let mut first_context = Context::from_waker(&first_waker);
    assert!(matches!(
        Pin::new(&mut first_persistence).poll(&mut first_context),
        Poll::Pending
    ));
    assert!(
        control
            .observed()
            .contains(&TestStorageOperation::WriteMany),
        "publication A must reach the storage submission before cancellation"
    );

    let wakes = Arc::new(WakeCount(AtomicUsize::new(0)));
    let second_waker = waker(Arc::clone(&wakes));
    let mut second_context = Context::from_waker(&second_waker);
    let mut second_persistence = Box::pin(second.persist());
    assert!(matches!(
        Pin::new(&mut second_persistence).poll(&mut second_context),
        Poll::Pending
    ));
    assert_eq!(wakes.0.load(Ordering::Acquire), 0);

    drop(first_persistence);

    assert_eq!(
        wakes.0.load(Ordering::Acquire),
        1,
        "cancelling A must wake B's registered publication-order waiter"
    );
    let Poll::Ready(second_result) = Pin::new(&mut second_persistence).poll(&mut second_context)
    else {
        panic!("woken publication B remained pending behind cancelled A");
    };
    drop(second_persistence);
    assert!(matches!(
        database.finish_persistence(second_result),
        Err(DatabaseError::Storage(_))
    ));
    assert!(matches!(
        database.ensure_usable(),
        Err(DatabaseError::DatabasePoisoned)
    ));
    control.resume_operation(TestStorageOperation::WriteMany);
    drop(first);
}

#[test]
fn possibly_committed_receipt_poisoned_database_before_settlement() {
    let (storage, control) = TestStorage::controlled(&["albums"]);
    let mut database = block_on(Database::new(schema(), storage)).unwrap();
    let mut batch = database.open_batch();
    batch.insert(
        "albums",
        vec![Value::U64(1), Value::String("Kind of Blue".into())],
    );
    let applied = block_on(database.apply_batch(batch)).unwrap();

    // YieldingStorage's injected error has no backend proof that the native
    // atomic boundary was not crossed, so the portable default classifies it
    // as PossiblyCommitted.
    control.fail_next(TestStorageOperation::WriteMany);
    let persisted = block_on(applied.persist());

    assert!(matches!(
        database.ensure_usable(),
        Err(DatabaseError::DatabasePoisoned)
    ));
    assert!(matches!(
        database.finish_persistence(persisted),
        Err(DatabaseError::Storage(_))
    ));
}

#[test]
fn dropping_an_unpersisted_applied_batch_poisons_further_database_work() {
    let storage = TestStorage::new(&["albums"]);
    let mut database = block_on(Database::new(schema(), storage)).unwrap();
    let mut batch = database.open_batch();
    batch.insert(
        "albums",
        vec![Value::U64(1), Value::String("Kind of Blue".into())],
    );

    let applied = block_on(database.apply_batch(batch)).unwrap();
    drop(applied);

    assert!(matches!(
        database.ensure_usable(),
        Err(DatabaseError::DatabasePoisoned)
    ));
    assert!(matches!(
        block_on(database.query_graph(GraphBuilder::table("albums"))),
        Err(DatabaseError::DatabasePoisoned)
    ));
}

#[test]
fn dropping_an_unfinished_persistence_receipt_poisons_further_database_work() {
    let storage = TestStorage::new(&["albums"]);
    let mut database = block_on(Database::new(schema(), storage)).unwrap();
    let mut batch = database.open_batch();
    batch.insert(
        "albums",
        vec![Value::U64(1), Value::String("Kind of Blue".into())],
    );

    let applied = block_on(database.apply_batch(batch)).unwrap();
    let persisted = block_on(applied.persist());
    drop(persisted);

    assert!(matches!(
        database.ensure_usable(),
        Err(DatabaseError::DatabasePoisoned)
    ));
}

#[test]
fn committed_terminal_output_carries_its_durable_publication_identity() {
    let (storage, _) = TestStorage::controlled(&["albums"]);
    let mut database = block_on(Database::new(schema(), storage)).unwrap();
    let subscription =
        block_on(database.subscribe_one_sink(GraphBuilder::table("albums"))).unwrap();
    assert!(
        block_on(database.next_subscription(&subscription))
            .unwrap()
            .is_empty()
    );

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
    let (storage, control) = TestStorage::controlled(&["albums", "indices"]);
    let mut database = block_on(Database::new(indexed_schema(), storage)).unwrap();
    let subscription =
        block_on(database.subscribe_one_sink(GraphBuilder::table("albums"))).unwrap();
    assert!(
        block_on(database.next_subscription(&subscription))
            .unwrap()
            .is_empty()
    );

    let mut batch = database.open_batch();
    batch.insert(
        "albums",
        vec![Value::U64(1), Value::String("Kind of Blue".into())],
    );
    let published = block_on(database.apply_batch(batch)).unwrap();
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
    assert!(
        block_on(database.primary_key_get_raw("albums", &[Value::U64(1)]))
            .unwrap()
            .is_some(),
        "direct point reads must observe applied resident writes",
    );
    assert_eq!(
        block_on(database.primary_key_scan_raw("albums", &[]))
            .unwrap()
            .len(),
        1,
        "direct scans must observe applied resident writes",
    );
    assert!(
        block_on(database.primary_key_last_raw("albums", &[]))
            .unwrap()
            .is_some(),
        "reverse point/prefix reads must observe applied resident writes",
    );
    assert_eq!(
        block_on(database.index_scan_raw(
            "albums",
            "albums_by_title",
            &[Value::String("Kind of Blue".into())],
        ))
        .unwrap()
        .len(),
        1,
        "direct index reads must observe applied resident writes",
    );
    assert_eq!(database.durable_publication_frontier(), None);

    control.resume_operation(TestStorageOperation::WriteMany);
    let persistence = block_on(persistence);
    assert_eq!(
        database.finish_persistence(persistence).unwrap(),
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
    let first = block_on(database.apply_batch(first)).unwrap();

    let mut second = database.open_batch();
    second.insert(
        "albums",
        vec![Value::U64(2), Value::String("Bitches Brew".into())],
    );
    let second = block_on(database.apply_batch(second)).unwrap();

    let first_persistence = block_on(first.persist());
    let second_persistence = block_on(second.persist());
    database.finish_persistence(second_persistence).unwrap();
    assert_eq!(database.durable_publication_frontier(), None);
    let rows = block_on(database.query_graph(GraphBuilder::table("albums"))).unwrap();
    assert_eq!(rows.deltas.len(), 2);

    database.finish_persistence(first_persistence).unwrap();
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
    let first = block_on(database.apply_batch(first)).unwrap();

    let mut second = database.open_batch();
    second.update(
        "albums",
        vec![Value::U64(1), Value::String("Blue in Green".into())],
    );
    let second = block_on(database.apply_batch(second)).unwrap();

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
    database.finish_persistence(second_persistence).unwrap();
    assert_eq!(database.durable_publication_frontier(), None);
    database.finish_persistence(first_persistence).unwrap();
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
    assert!(
        block_on(database.next_subscription(&subscription))
            .unwrap()
            .is_empty()
    );

    let mut batch = database.open_batch();
    batch.insert(
        "albums",
        vec![Value::U64(1), Value::String("Kind of Blue".into())],
    );
    control.take_observed();
    control.pause_on(TestStorageOperation::WriteMany);
    let mut publication = Box::pin(database.apply_batch(batch));
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
