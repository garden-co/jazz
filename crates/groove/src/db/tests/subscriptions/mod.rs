//! Table, query, prepared, routed, and structured subscription lifecycles.

use super::*;

/// A non-blocking owner turn must leave a real wake route behind for cold
/// hydration. Merely noticing pending work on a later manually-driven tick is
/// insufficient for event-driven hosts: no unrelated transport activity is
/// required to resume this subscription.
#[futures_test::test]
async fn cold_hydration_wakes_the_supplied_owner_once_without_polling() {
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};

    use futures::task::{ArcWake, waker};

    struct WakeCount(AtomicUsize);

    impl ArcWake for WakeCount {
        fn wake_by_ref(arc_self: &Arc<Self>) {
            arc_self.0.fetch_add(1, Ordering::AcqRel);
        }
    }

    let (storage, control) = TestStorage::controlled(&["albums"]);
    let mut database = Database::new(albums_schema(), storage.clone())
        .await
        .unwrap();
    let mut batch = database.open_batch();
    batch.insert(
        "albums",
        vec![Value::U64(1), Value::String("wake bridge".to_owned())],
    );
    database.commit_batch(batch).await.unwrap();
    storage.evict_all();
    control.pause_on(TestStorageOperation::ScanOpen);

    let subscription = database
        .subscribe_one_sink(GraphBuilder::table("albums"))
        .await
        .unwrap();
    assert!(
        database.has_pending_progress(),
        "a direct opening returns while its controlled ScanOpen is cold instead of awaiting it"
    );
    let wakes = Arc::new(WakeCount(AtomicUsize::new(0)));
    let owner_waker = waker(Arc::clone(&wakes));
    database
        .drive_ready_progress_with_waker(Some(&owner_waker))
        .await
        .unwrap();
    assert!(database.has_pending_progress());
    assert_eq!(wakes.0.load(Ordering::Acquire), 0);

    control.resume_operation(TestStorageOperation::ScanOpen);
    assert_eq!(
        wakes.0.load(Ordering::Acquire),
        1,
        "storage readiness schedules exactly one following owner turn"
    );
    let mut observed_wakes = wakes.0.load(Ordering::Acquire);
    for _ in 0..32 {
        database
            .drive_ready_progress_with_waker(Some(&owner_waker))
            .await
            .unwrap();
        if !database.has_pending_progress() {
            break;
        }
        let next_wakes = wakes.0.load(Ordering::Acquire);
        assert!(
            next_wakes > observed_wakes,
            "each further cold operation requests its own owner turn instead of polling hot"
        );
        observed_wakes = next_wakes;
    }
    assert!(!database.has_pending_progress());
    assert_eq!(
        expect_try_recv_vals(&subscription),
        vec![(
            vec![Value::U64(1), Value::String("wake bridge".to_owned())],
            1
        )]
    );
    let idle_wakes = wakes.0.load(Ordering::Acquire);
    database
        .drive_ready_progress_with_waker(Some(&owner_waker))
        .await
        .unwrap();
    assert_eq!(
        wakes.0.load(Ordering::Acquire),
        idle_wakes,
        "quiescent runtimes retain no storage wake and do not schedule a hot follow-up"
    );
}

/// Subscription opening performs one bounded incremental poll to publish
/// resident rows without another owner turn.  That opening poll must retain
/// the host's durable continuation when it discovers cold storage: otherwise
/// a worker can render its initial empty view yet never receive the wake that
/// lets it process a later local write or shutdown.
#[futures_test::test]
async fn cold_subscription_open_retains_the_supplied_owner_waker() {
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};

    use futures::task::{ArcWake, waker};

    struct WakeCount(AtomicUsize);

    impl ArcWake for WakeCount {
        fn wake_by_ref(arc_self: &Arc<Self>) {
            arc_self.0.fetch_add(1, Ordering::AcqRel);
        }
    }

    let (storage, control) = TestStorage::controlled(&["albums"]);
    let mut database = Database::new(albums_schema(), storage.clone())
        .await
        .unwrap();
    let mut batch = database.open_batch();
    batch.insert(
        "albums",
        vec![
            Value::U64(1),
            Value::String("opening wake bridge".to_owned()),
        ],
    );
    database.commit_batch(batch).await.unwrap();
    storage.evict_all();
    control.pause_on(TestStorageOperation::ScanOpen);

    let wakes = Arc::new(WakeCount(AtomicUsize::new(0)));
    let owner_waker = waker(Arc::clone(&wakes));
    let subscription = database
        .subscribe_with_waker(
            [("albums", GraphBuilder::table("albums"))],
            Some(&owner_waker),
        )
        .unwrap();
    assert!(database.has_pending_progress());
    assert_eq!(
        wakes.0.load(Ordering::Acquire),
        1,
        "the opening poll uses the supplied owner waker rather than Waker::noop"
    );

    // Simulate the owner turn that the opening poll just requested. It starts
    // the controlled storage operation and leaves the same durable wake route
    // attached to that cold operation.
    database
        .drive_ready_progress_with_waker(Some(&owner_waker))
        .await
        .unwrap();
    assert!(database.has_pending_progress());
    let wakes_before_resume = wakes.0.load(Ordering::Acquire);

    control.resume_operation(TestStorageOperation::ScanOpen);
    assert_eq!(
        wakes.0.load(Ordering::Acquire),
        wakes_before_resume + 1,
        "cold work opened by a subscription schedules its runtime owner"
    );

    for _ in 0..32 {
        database
            .drive_ready_progress_with_waker(Some(&owner_waker))
            .await
            .unwrap();
        if !database.has_pending_progress() {
            break;
        }
    }
    assert!(!database.has_pending_progress());
    assert!(
        subscription.try_recv().is_ok(),
        "the resumed subscription publishes its seeded snapshot"
    );
}

/// A cold hydration cannot monopolize the IVM worklist while a second
/// subscription is being registered. The owner must return after the first
/// pending evaluation, leaving storage to wake it before any later work is
/// advanced.
#[futures_test::test]
async fn cold_hydration_yields_before_later_subscription_work() {
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};

    use futures::task::{ArcWake, waker};

    struct WakeCount(AtomicUsize);

    impl ArcWake for WakeCount {
        fn wake_by_ref(arc_self: &Arc<Self>) {
            arc_self.0.fetch_add(1, Ordering::AcqRel);
        }
    }

    let (storage, control) = TestStorage::controlled(&["albums", "artists"]);
    let mut database = Database::new(albums_artists_schema(), storage.clone())
        .await
        .unwrap();
    let mut batch = database.open_batch();
    batch.insert(
        "albums",
        vec![
            Value::U64(1),
            Value::U64(1),
            Value::String("owner-turn album".to_owned()),
        ],
    );
    batch.insert(
        "artists",
        vec![Value::U64(1), Value::String("owner-turn artist".to_owned())],
    );
    database.commit_batch(batch).await.unwrap();
    storage.evict_all();
    control.pause_on(TestStorageOperation::ScanOpen);

    let wakes = Arc::new(WakeCount(AtomicUsize::new(0)));
    let owner_waker = waker(Arc::clone(&wakes));
    let albums = database
        .subscribe([("albums", GraphBuilder::table("albums"))])
        .unwrap();
    let artists = database
        .subscribe([("artists", GraphBuilder::table("artists"))])
        .unwrap();

    // The first owner turn has a cooperative IVM yield; the second reaches
    // the paused album scan. The artist scan must remain untouched.
    database
        .drive_ready_progress_with_waker(Some(&owner_waker))
        .await
        .unwrap();
    database
        .drive_ready_progress_with_waker(Some(&owner_waker))
        .await
        .unwrap();

    assert_eq!(
        control
            .observed()
            .into_iter()
            .filter(|operation| *operation == TestStorageOperation::ScanOpen)
            .count(),
        1,
        "the first cold scan yields the worklist before the later hydration can open its scan"
    );
    assert!(database.has_pending_progress());

    control.resume_operation(TestStorageOperation::ScanOpen);
    for _ in 0..32 {
        database
            .drive_ready_progress_with_waker(Some(&owner_waker))
            .await
            .unwrap();
        if !database.has_pending_progress() {
            break;
        }
    }
    assert!(!database.has_pending_progress());
    assert!(albums.try_recv().is_ok(), "the cold hydration completes");
    assert!(
        artists.try_recv().is_ok(),
        "later independent work also completes"
    );
}

/// A large entirely resident graph is CPU work, not a storage wait. The direct
/// async API owns and drains that resident continuation chain before returning;
/// the owner-loop API instead yields bounded turns and wakes its owner. Both
/// paths must produce the same stable snapshot.
#[futures_test::test]
async fn deep_resident_hydration_yields_without_changing_its_snapshot() {
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};

    use futures::task::{ArcWake, waker};

    struct WakeCount(AtomicUsize);

    impl ArcWake for WakeCount {
        fn wake_by_ref(arc_self: &Arc<Self>) {
            arc_self.0.fetch_add(1, Ordering::AcqRel);
        }
    }

    let storage = MemoryStorage::new(&["albums"]).expect("valid memory storage families");
    let mut database = Database::new(albums_schema(), storage).await.unwrap();
    let mut batch = database.open_batch();
    for id in 1..=3 {
        batch.insert(
            "albums",
            vec![Value::U64(id), Value::String(format!("resident {id}"))],
        );
    }
    database.commit_batch(batch).await.unwrap();

    let mut owner_deep_graph = GraphBuilder::table("albums");
    // Deliberately far larger than one cooperative session slice.
    for _ in 0..128 {
        owner_deep_graph = owner_deep_graph.filter(PredicateExpr::gt("id", Value::U64(0)));
    }

    let wakes = Arc::new(WakeCount(AtomicUsize::new(0)));
    let owner_waker = waker(Arc::clone(&wakes));
    let owner_deep = database
        .subscribe_with_waker([("deep", owner_deep_graph)], Some(&owner_waker))
        .unwrap();
    assert!(database.has_pending_progress());

    let mut previous_wakes = 0;
    for _ in 0..32 {
        let wakes_now = wakes.0.load(Ordering::Acquire);
        assert!(
            wakes_now > previous_wakes,
            "each bounded resident owner turn schedules its continuation rather than relying on polling"
        );
        previous_wakes = wakes_now;
        database
            .drive_ready_progress_with_waker(Some(&owner_waker))
            .await
            .unwrap();
        if !database.has_pending_progress() {
            break;
        }
    }
    assert!(!database.has_pending_progress());
    let owner_snapshot = owner_deep
        .try_recv()
        .expect("owner-loop hydration publishes its completed initial snapshot")
        .get("deep")
        .expect("the deep sink is present")
        .to_values()
        .unwrap();
    let mut owner_snapshot = owner_snapshot;
    owner_snapshot.sort_by(|a, b| format!("{a:?}").cmp(&format!("{b:?}")));

    let mut direct_deep_graph = GraphBuilder::table("albums");
    for _ in 0..128 {
        direct_deep_graph = direct_deep_graph.filter(PredicateExpr::gt("id", Value::U64(0)));
    }
    let direct_deep = database
        .subscribe_one_sink(direct_deep_graph)
        .await
        .unwrap();
    let shallow = database
        .subscribe_one_sink(GraphBuilder::table("albums"))
        .await
        .unwrap();
    assert!(
        !database.has_pending_progress(),
        "the direct async API drains all self-scheduled resident slices before returning"
    );
    let direct_snapshot = expect_try_recv_vals(&direct_deep);
    assert_eq!(
        direct_snapshot,
        expect_try_recv_vals(&shallow),
        "direct resident hydration returns a complete stable deep snapshot"
    );
    assert_eq!(
        owner_snapshot, direct_snapshot,
        "bounded owner-loop hydration preserves the deep snapshot"
    );
}

/// Cold storage is permitted to self-wake after its first poll. That wake is
/// not an IVM CPU continuation: a direct opening must leave the request
/// pending, and only a later owner turn may attach and receive its durable
/// wake route.
#[futures_test::test]
async fn direct_cold_self_wake_waits_for_a_later_owner_turn() {
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};

    use futures::task::{ArcWake, waker};

    struct WakeCount(AtomicUsize);

    impl ArcWake for WakeCount {
        fn wake_by_ref(arc_self: &Arc<Self>) {
            arc_self.0.fetch_add(1, Ordering::AcqRel);
        }
    }

    let (storage, control) = TestStorage::controlled(&["albums"]);
    let mut database = Database::new(albums_schema(), storage.clone())
        .await
        .unwrap();
    let mut batch = database.open_batch();
    batch.insert(
        "albums",
        vec![Value::U64(1), Value::String("cold wake".to_owned())],
    );
    database.commit_batch(batch).await.unwrap();
    storage.evict_all();
    control.take_observed();

    let subscription = database
        .subscribe_one_sink(GraphBuilder::table("albums"))
        .await
        .unwrap();
    assert!(database.has_pending_progress());
    assert_eq!(
        control
            .observed()
            .into_iter()
            .filter(|operation| *operation == TestStorageOperation::ScanOpen)
            .count(),
        1,
        "the direct opening polls the cold scan once but must not consume its self-wake"
    );
    assert!(
        subscription.try_recv().is_err(),
        "a self-woken cold scan must not publish during the direct opening"
    );

    // The direct call has returned. Install a real owner waker before letting
    // the retained storage operation become ready.
    control.pause_on(TestStorageOperation::ScanOpen);
    let wakes = Arc::new(WakeCount(AtomicUsize::new(0)));
    let owner_waker = waker(Arc::clone(&wakes));
    database
        .drive_ready_progress_with_waker(Some(&owner_waker))
        .await
        .unwrap();
    assert!(database.has_pending_progress());
    assert_eq!(wakes.0.load(Ordering::Acquire), 0);

    control.resume_operation(TestStorageOperation::ScanOpen);
    assert_eq!(
        wakes.0.load(Ordering::Acquire),
        1,
        "the resumed cold operation wakes the owner that actually retained it"
    );
    for _ in 0..32 {
        database
            .drive_ready_progress_with_waker(Some(&owner_waker))
            .await
            .unwrap();
        if !database.has_pending_progress() {
            break;
        }
    }
    assert!(!database.has_pending_progress());
    assert_eq!(
        expect_try_recv_vals(&subscription),
        vec![(
            vec![Value::U64(1), Value::String("cold wake".to_owned())],
            1
        )]
    );
}

/// A later direct write may create resident work, but it cannot use that call
/// to poll an earlier cold subscription past its self-wake.
#[futures_test::test]
async fn direct_write_does_not_consume_an_earlier_cold_subscription() {
    let (storage, control) = TestStorage::controlled(&["albums"]);
    let mut database = Database::new(albums_schema(), storage.clone())
        .await
        .unwrap();
    let mut seed = database.open_batch();
    seed.insert(
        "albums",
        vec![Value::U64(1), Value::String("seed".to_owned())],
    );
    database.commit_batch(seed).await.unwrap();
    storage.evict_all();
    control.take_observed();

    let subscription = database
        .subscribe_one_sink(GraphBuilder::table("albums"))
        .await
        .unwrap();
    assert!(database.has_pending_progress());
    assert!(subscription.try_recv().is_err());

    let mut write = database.open_batch();
    write.insert(
        "albums",
        vec![Value::U64(2), Value::String("later write".to_owned())],
    );
    let applied = database.apply_batch(write).await.unwrap();
    assert_eq!(
        control
            .observed()
            .into_iter()
            .filter(|operation| *operation == TestStorageOperation::ScanOpen)
            .count(),
        1,
        "the write must not repoll the self-woken cold scan synchronously"
    );
    assert!(database.has_pending_progress());
    assert!(
        subscription.try_recv().is_err(),
        "the cold subscription cannot publish as a side effect of an unrelated direct write"
    );
    drop(applied);
}

#[futures_test::test]
async fn subscribe_sends_empty_hydration_snapshot_without_writes() {
    let storage = MemoryStorage::new(&["albums"]).expect("valid memory storage families");
    let mut database = Database::new(albums_schema(), storage).await.unwrap();
    let subscription_id = database
        .subscribe_one_sink(GraphBuilder::table("albums"))
        .await
        .unwrap();

    assert!(subscription_id.try_recv().unwrap().is_empty());
    database.flush().await.unwrap();
    assert!(subscription_id.try_recv().is_err());
    assert!(
        database
            .storage
            .prefix("albums".to_owned(), Vec::new())
            .await
            .unwrap()
            .is_empty()
    );
}

#[futures_test::test]
async fn history_rows_remain_plain_across_hydration_post_write_and_reopen() {
    let schema = jazz_docs_history_schema();
    let column_families = schema.column_families();

    let storage = {
        let storage = MemoryStorage::new(&column_families).expect("valid memory storage families");
        let mut database = Database::new(schema.clone(), storage).await.unwrap();
        seed_jazz_docs_history(&mut database, 0, 12).await;

        // A history record is one ordinary row at its primary key. The exact
        // physical count makes a future hidden packer/window write observable.
        assert_eq!(
            database
                .storage
                .prefix("jazz_docs_history".to_owned(), Vec::new())
                .await
                .unwrap()
                .len(),
            12
        );

        let subscription = database
            .subscribe_one_sink(GraphBuilder::table("jazz_docs_history"))
            .await
            .unwrap();
        assert_eq!(subscription.recv().unwrap().deltas.len(), 12);

        seed_jazz_docs_history(&mut database, 12, 1).await;
        assert_eq!(subscription.recv().unwrap().deltas.len(), 1);
        assert_eq!(
            database
                .storage
                .prefix("jazz_docs_history".to_owned(), Vec::new())
                .await
                .unwrap()
                .len(),
            13
        );
        database.into_storage()
    };

    let mut database = Database::new(schema, storage).await.unwrap();
    assert_eq!(
        database
            .storage
            .prefix("jazz_docs_history".to_owned(), Vec::new())
            .await
            .unwrap()
            .len(),
        13
    );
    let subscription = database
        .subscribe_one_sink(GraphBuilder::table("jazz_docs_history"))
        .await
        .unwrap();
    assert_eq!(subscription.recv().unwrap().deltas.len(), 13);
}

fn jazz_docs_history_schema() -> DatabaseSchema {
    DatabaseSchema::new([TableSchema::new(
        "jazz_docs_history",
        [
            ColumnSchema::new("row_uuid", ColumnType::Uuid),
            ColumnSchema::new("tx_time", ColumnType::U64),
            ColumnSchema::new("tx_node", ColumnType::U64),
            ColumnSchema::new("payload", ColumnType::String),
        ],
    )
    .with_primary_key(PrimaryKey::composite([
        PrimaryKeyColumn::uuid("row_uuid"),
        PrimaryKeyColumn::integer("tx_time", IntegerKeyType::U64),
        PrimaryKeyColumn::integer("tx_node", IntegerKeyType::U64),
    ]))
    .with_index(IndexSchema::new(
        "by_tx",
        ["tx_time", "tx_node", "row_uuid"],
    ))])
}

async fn seed_jazz_docs_history(database: &mut Database, start_idx: u64, row_count: u64) {
    let mut batch = database.open_batch();
    for idx in start_idx..start_idx + row_count {
        batch.insert(
            "jazz_docs_history",
            vec![
                Value::Uuid(uuid::Uuid::from_u128(
                    0xaaaaaaaa_aaaa_aaaa_aaaa_aaaaaaaaaaaa,
                )),
                Value::U64(100 + idx),
                Value::U64(7),
                Value::String(format!("payload-{idx}")),
            ],
        );
    }
    database.commit_batch(batch).await.unwrap();
}

#[futures_test::test]
async fn rejects_unknown_tables() {
    let storage = MemoryStorage::new(&["albums"]).expect("valid memory storage families");
    let mut database = Database::new(albums_schema(), storage).await.unwrap();
    let mut batch = database.open_batch();
    batch.insert("missing", vec![Value::U64(1)]);

    assert!(matches!(
        database.commit_batch(batch).await.unwrap_err(),
        Error::TableNotFound(table) if table == "missing"
    ));
}

#[futures_test::test]
async fn invalid_batches_do_not_partially_write_valid_earlier_operations() {
    let storage = MemoryStorage::new(&["albums"]).expect("valid memory storage families");
    let mut database = Database::new(albums_schema(), storage).await.unwrap();
    let mut batch = database.open_batch();
    batch.insert(
        "albums",
        vec![Value::U64(7), Value::String("Blue Train".to_owned())],
    );
    batch.insert("missing", vec![Value::U64(1)]);

    assert!(matches!(
        database.commit_batch(batch).await,
        Err(Error::TableNotFound(table)) if table == "missing"
    ));
    assert!(
        database
            .storage
            .prefix("albums".to_owned(), Vec::new())
            .await
            .unwrap()
            .is_empty()
    );
}

#[futures_test::test]
async fn final_atomic_commit_failure_leaves_base_rows_unwritten_and_poisons_database() {
    let storage = MemoryStorage::new(&["albums"]).expect("valid memory storage families");
    let mut database = Database::new(indexed_albums_schema(), storage)
        .await
        .unwrap();

    let mut batch = database.open_batch();
    batch.insert(
        "albums",
        vec![Value::U64(7), Value::String("Blue Train".to_owned())],
    );

    assert!(matches!(
        database.commit_batch(batch).await,
        Err(Error::Storage(error)) if matches!(
            error.as_ref(),
            crate::storage::Error::ColumnFamilyNotFound(cf) if cf == "indices"
        )
    ));
    assert_eq!(
        database
            .storage
            .get("albums".to_owned(), PrimaryKeyValue::U64(7).into_bytes())
            .await
            .unwrap(),
        None
    );
    assert!(matches!(
        database.primary_key_scan("albums", &[]).await,
        Err(Error::DatabasePoisoned)
    ));
}

#[futures_test::test]
async fn atomic_commit_path_supports_indexed_join_and_recursive_workloads() {
    let indexed_storage =
        MemoryStorage::new(&["albums", "indices"]).expect("valid memory storage families");
    let mut indexed = Database::new(indexed_albums_schema(), indexed_storage)
        .await
        .unwrap();
    let mut batch = indexed.open_batch();
    batch.insert(
        "albums",
        vec![Value::U64(7), Value::String("Blue Train".to_owned())],
    );
    indexed.commit_batch(batch).await.unwrap();
    assert_eq!(
        record_values(
            indexed
                .index_scan(
                    "albums",
                    "albums_by_title",
                    &[Value::String("Blue Train".to_owned())],
                )
                .await
                .unwrap()
        ),
        [vec![Value::U64(7), Value::String("Blue Train".to_owned())]]
    );

    let join_storage =
        MemoryStorage::new(&["albums", "artists"]).expect("valid memory storage families");
    let mut joined = Database::new(albums_artists_schema(), join_storage)
        .await
        .unwrap();
    let subscription = joined
        .subscribe_one_sink(GraphBuilder::join(
            GraphBuilder::table("albums"),
            GraphBuilder::table("artists"),
            ["artist_id"],
            ["id"],
        ))
        .await
        .unwrap();
    let mut batch = joined.open_batch();
    batch.insert(
        "albums",
        vec![
            Value::U64(7),
            Value::U64(11),
            Value::String("Blue Train".to_owned()),
        ],
    );
    batch.insert(
        "artists",
        vec![Value::U64(11), Value::String("John Coltrane".to_owned())],
    );
    joined.commit_batch(batch).await.unwrap();
    assert_eq!(expect_recv_vals(&subscription).len(), 1);

    let recursive_storage = MemoryStorage::new(&["edges"]).expect("valid memory storage families");
    let mut recursive = Database::new(edges_schema(), recursive_storage)
        .await
        .unwrap();
    let subscription = recursive
        .subscribe_one_sink(reachability_graph(16))
        .await
        .unwrap();
    let mut batch = recursive.open_batch();
    batch.insert("edges", vec![Value::U64(1), Value::U64(1), Value::U64(2)]);
    batch.insert("edges", vec![Value::U64(2), Value::U64(2), Value::U64(3)]);
    recursive.commit_batch(batch).await.unwrap();
    assert_eq!(
        expect_recv_vals(&subscription),
        vec![
            (vec![Value::U64(1), Value::U64(2)], 1),
            (vec![Value::U64(1), Value::U64(3)], 1),
            (vec![Value::U64(2), Value::U64(3)], 1),
        ]
    );
}

#[futures_test::test]
async fn subscriptions_reject_unknown_tables_and_indices() {
    let storage = MemoryStorage::new(&["albums"]).expect("valid memory storage families");
    let mut database = Database::new(albums_schema(), storage).await.unwrap();

    assert!(matches!(
        database.subscribe_one_sink(GraphBuilder::table("missing")).await,
        Err(Error::IvmRuntime(IvmRuntimeError::TableNotFound(table))) if table == "missing"
    ));
    assert!(matches!(
        database.subscribe_one_sink(GraphBuilder::index("albums", "missing_idx")).await,
        Err(Error::IvmRuntime(IvmRuntimeError::IndexNotFound(index))) if index == "missing_idx"
    ));
}

#[futures_test::test]
async fn rejects_primary_key_type_mismatches_before_writing() {
    let schema = DatabaseSchema::new([TableSchema::new(
        "albums",
        [
            ColumnSchema::new("id", ColumnType::String),
            ColumnSchema::new("title", ColumnType::String),
        ],
    )
    .with_primary_key(PrimaryKey::new("id", IntegerKeyType::U64))]);
    let storage = MemoryStorage::new(&["albums"]).expect("valid memory storage families");
    let mut database = Database::new(schema, storage).await.unwrap();
    let mut batch = database.open_batch();
    batch.insert(
        "albums",
        vec![
            Value::String("not-a-u64".to_owned()),
            Value::String("Blue Train".to_owned()),
        ],
    );

    assert!(matches!(
        database.commit_batch(batch).await,
        Err(Error::PrimaryKeyTypeMismatch { table, column })
            if table == "albums" && column == "id"
    ));
    assert!(
        database
            .storage
            .prefix("albums".to_owned(), Vec::new())
            .await
            .unwrap()
            .is_empty()
    );
}

#[futures_test::test]
async fn inserts_accept_values_in_table_declaration_order_even_when_storage_order_differs() {
    let storage = MemoryStorage::new(&["albums"]).expect("valid memory storage families");
    let schema = DatabaseSchema::new([TableSchema::new(
        "albums",
        [
            ColumnSchema::new("id", ColumnType::U64),
            ColumnSchema::new("title", ColumnType::String),
            ColumnSchema::new("rating", ColumnType::F64.nullable()),
        ],
    )
    .with_primary_key(PrimaryKey::new("id", IntegerKeyType::U64))]);
    let mut database = Database::new(schema, storage).await.unwrap();

    let mut batch = database.open_batch();
    batch.insert(
        "albums",
        vec![
            Value::U64(7),
            Value::String("Blue Train".to_owned()),
            Value::Nullable(Some(Box::new(Value::F64(4.5)))),
        ],
    );
    database.commit_batch(batch).await.unwrap();

    let descriptor = database
        .ivm_runtime
        .schema()
        .table("albums")
        .unwrap()
        .record_schema();
    let stored = database
        .storage
        .get("albums".to_owned(), PrimaryKeyValue::U64(7).into_bytes())
        .await
        .unwrap()
        .unwrap();

    let stored = version_zero_payload(&stored);
    assert_eq!(descriptor.get(stored, "id").unwrap(), Value::U64(7));
    assert_eq!(
        descriptor.get(stored, "title").unwrap(),
        Value::String("Blue Train".to_owned())
    );
    assert_eq!(
        descriptor.get(stored, "rating").unwrap(),
        Value::Nullable(Some(Box::new(Value::F64(4.5))))
    );
}

#[futures_test::test]
async fn record_valued_columns_round_trip_through_table_storage() {
    let child = RecordDescriptor::new([("title", ValueType::String), ("year", ValueType::I32)]);
    let schema = DatabaseSchema::new([TableSchema::new(
        "albums",
        [
            ColumnSchema::new("id", ColumnType::U64),
            ColumnSchema::new("metadata", ColumnType::Record(Box::new(child))),
        ],
    )
    .with_primary_key(PrimaryKey::new("id", IntegerKeyType::U64))]);
    let storage =
        MemoryStorage::new(&schema.column_families()).expect("valid memory storage families");
    let mut database = Database::new(schema, storage).await.unwrap();
    let metadata = crate::records::OwnedRecord::new(
        child
            .create(&[Value::String("Blue Train".to_owned()), Value::I32(1957)])
            .unwrap(),
        child,
    );

    let mut batch = database.open_batch();
    batch.insert(
        "albums",
        vec![Value::U64(7), Value::Record(metadata.clone())],
    );
    database.commit_batch(batch).await.unwrap();

    let stored = database
        .primary_key_scan("albums", &[Value::U64(7)])
        .await
        .unwrap();
    assert_eq!(stored.len(), 1);
    assert_eq!(stored[0].get("metadata").unwrap(), Value::Record(metadata));
}

#[futures_test::test]
async fn integer_primary_keys_are_stored_with_tagged_order_preserving_keys() {
    let storage = MemoryStorage::new(&["u8_keys", "u16_keys", "u32_keys", "u64_keys"])
        .expect("valid memory storage families");
    let mut database = Database::new(integer_key_widths_schema(), storage)
        .await
        .unwrap();
    let mut batch = database.open_batch();
    batch.insert("u8_keys", vec![Value::U8(7)]);
    batch.insert("u16_keys", vec![Value::U16(0x0102)]);
    batch.insert("u32_keys", vec![Value::U32(0x0102_0304)]);
    batch.insert("u64_keys", vec![Value::U64(0x0102_0304_0506_0708)]);

    database.commit_batch(batch).await.unwrap();

    assert!(
        database
            .storage
            .get("u8_keys".to_owned(), [0x00, 0x07].to_vec())
            .await
            .unwrap()
            .is_some()
    );
    assert!(
        database
            .storage
            .get("u16_keys".to_owned(), [0x01, 0x01, 0x02].to_vec())
            .await
            .unwrap()
            .is_some()
    );
    assert!(
        database
            .storage
            .get(
                "u32_keys".to_owned(),
                [0x02, 0x01, 0x02, 0x03, 0x04].to_vec()
            )
            .await
            .unwrap()
            .is_some()
    );
    assert!(
        database
            .storage
            .get(
                "u64_keys".to_owned(),
                [0x03, 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08].to_vec()
            )
            .await
            .unwrap()
            .is_some()
    );
}

#[futures_test::test]
async fn composite_primary_keys_are_encoded_from_multiple_columns() {
    let storage = MemoryStorage::new(&["history"]).expect("valid memory storage families");
    let mut database = Database::new(composite_key_schema(), storage)
        .await
        .unwrap();
    let row_uuid = vec![1, 0, 2];
    let key = PrimaryKeyValue::Composite(vec![
        PrimaryKeyValue::Bytes(row_uuid.clone()),
        PrimaryKeyValue::U64(9),
        PrimaryKeyValue::U64(42),
    ])
    .into_bytes();

    let mut batch = database.open_batch();
    batch.insert(
        "history",
        vec![
            Value::Bytes(row_uuid),
            Value::U64(9),
            Value::U64(42),
            Value::String("first".to_owned()),
        ],
    );
    database.commit_batch(batch).await.unwrap();

    let descriptor = database
        .ivm_runtime
        .schema()
        .table("history")
        .unwrap()
        .record_schema();
    let stored = database
        .storage
        .get("history".to_owned(), key.clone())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        descriptor
            .get(version_zero_payload(&stored), "payload")
            .unwrap(),
        Value::String("first".to_owned())
    );

    let mut batch = database.open_batch();
    batch.delete(
        "history",
        PrimaryKeyValue::Composite(vec![
            PrimaryKeyValue::Bytes(vec![1, 0, 2]),
            PrimaryKeyValue::U64(9),
            PrimaryKeyValue::U64(42),
        ]),
    );
    database.commit_batch(batch).await.unwrap();

    assert!(
        database
            .storage
            .get("history".to_owned(), key.clone())
            .await
            .unwrap()
            .is_none()
    );
}

#[futures_test::test]
async fn rejects_tables_without_primary_keys() {
    let storage = MemoryStorage::new(&["logs"]).expect("valid memory storage families");
    let mut database = Database::new(
        DatabaseSchema::new([TableSchema::new(
            "logs",
            [ColumnSchema::new("message", ColumnType::String)],
        )]),
        storage,
    )
    .await
    .unwrap();
    let mut batch = database.open_batch();
    batch.insert("logs", vec![Value::String("hello".to_owned())]);

    assert!(matches!(
        database.commit_batch(batch).await.unwrap_err(),
        Error::MissingPrimaryKey(table) if table == "logs"
    ));
}

#[futures_test::test]
async fn table_subscriptions_receive_insert_update_and_delete_messages() {
    let storage = MemoryStorage::new(&["albums"]).expect("valid memory storage families");
    let mut database = Database::new(albums_schema(), storage).await.unwrap();
    let subscription_id = database
        .subscribe_one_sink(GraphBuilder::table("albums"))
        .await
        .unwrap();

    let mut batch = database.open_batch();
    batch.insert(
        "albums",
        vec![Value::U64(7), Value::String("Blue Train".to_owned())],
    );
    database.commit_batch(batch).await.unwrap();
    assert_eq!(
        expect_recv_vals(&subscription_id),
        [(vec![7_u64.into(), "Blue Train".into()], 1)]
    );

    let mut batch = database.open_batch();
    batch.update(
        "albums",
        vec![Value::U64(7), Value::String("Giant Steps".to_owned())],
    );
    database.commit_batch(batch).await.unwrap();
    assert_eq!(
        expect_recv_vals(&subscription_id),
        [
            (vec![7_u64.into(), "Blue Train".into()], -1),
            (vec![7_u64.into(), "Giant Steps".into()], 1)
        ]
    );

    let mut batch = database.open_batch();
    batch.delete("albums", PrimaryKeyValue::U64(7));
    database.commit_batch(batch).await.unwrap();
    assert_eq!(
        expect_recv_vals(&subscription_id),
        [(vec![7_u64.into(), "Giant Steps".into()], -1)]
    );
}

#[futures_test::test]
async fn dropping_subscription_receiver_unsubscribes_on_next_message() {
    let storage = MemoryStorage::new(&["albums"]).expect("valid memory storage families");
    let mut database = Database::new(albums_schema(), storage).await.unwrap();
    let subscription = database
        .subscribe_one_sink(GraphBuilder::table("albums"))
        .await
        .unwrap();
    let subscription_id = subscription.id();
    drop(subscription);

    let mut batch = database.open_batch();
    batch.insert(
        "albums",
        vec![Value::U64(7), Value::String("Blue Train".to_owned())],
    );
    database.commit_batch(batch).await.unwrap();

    assert!(!database.unsubscribe(subscription_id));
}

#[futures_test::test]
async fn dropped_subscription_receiver_can_be_pruned_without_a_later_message() {
    let storage = MemoryStorage::new(&["albums"]).expect("valid memory storage families");
    let mut database = Database::new(albums_schema(), storage).await.unwrap();
    let subscription = database
        .subscribe_one_sink(GraphBuilder::table("albums"))
        .await
        .unwrap();
    let subscription_id = subscription.id();
    assert_eq!(database.runtime_stats().active_subscriptions, 1);
    drop(subscription);

    assert_eq!(database.prune_dropped_subscriptions().await.unwrap(), 1);
    assert_eq!(database.runtime_stats().active_subscriptions, 0);
    assert!(!database.unsubscribe(subscription_id));
}

#[futures_test::test]
async fn subscribe_returns_current_rows_as_initial_message_then_future_deltas() {
    let storage = MemoryStorage::new(&["albums"]).expect("valid memory storage families");
    let mut database = Database::new(albums_schema(), storage).await.unwrap();

    let mut batch = database.open_batch();
    batch.insert(
        "albums",
        vec![Value::U64(7), Value::String("Blue Train".to_owned())],
    );
    database.commit_batch(batch).await.unwrap();

    let subscription = database
        .subscribe_one_sink(GraphBuilder::table("albums"))
        .await
        .unwrap();
    database.flush().await.unwrap();
    assert_eq!(
        expect_recv_vals(&subscription),
        [(vec![7_u64.into(), "Blue Train".into()], 1)]
    );

    let mut batch = database.open_batch();
    batch.insert(
        "albums",
        vec![Value::U64(8), Value::String("Giant Steps".to_owned())],
    );
    database.commit_batch(batch).await.unwrap();

    assert_eq!(
        expect_recv_vals(&subscription),
        [(vec![8_u64.into(), "Giant Steps".into()], 1)]
    );
}

#[futures_test::test]
async fn subscription_owns_initial_snapshot_separately_from_incremental_receiver() {
    let storage = MemoryStorage::new(&["albums"]).expect("valid memory storage families");
    let mut database = Database::new(albums_schema(), storage).await.unwrap();

    let mut batch = database.open_batch();
    batch.insert(
        "albums",
        vec![Value::U64(7), Value::String("Blue Train".to_owned())],
    );
    database.commit_batch(batch).await.unwrap();

    let subscription = database
        .subscribe_one_sink(GraphBuilder::table("albums"))
        .await
        .unwrap();
    let initial = subscription
        .take_initial()
        .expect("new terminal session owns one initial snapshot");
    assert_eq!(
        initial.to_values().unwrap(),
        [(vec![7_u64.into(), "Blue Train".into()], 1)]
    );
    assert!(subscription.take_initial().is_none());
    assert!(matches!(subscription.try_recv(), Err(TryRecvError::Empty)));

    let mut batch = database.open_batch();
    batch.insert(
        "albums",
        vec![Value::U64(8), Value::String("Giant Steps".to_owned())],
    );
    database.commit_batch(batch).await.unwrap();

    assert_eq!(
        expect_recv_vals(&subscription),
        [(vec![8_u64.into(), "Giant Steps".into()], 1)]
    );
}

#[futures_test::test]
async fn subscribe_query_filters_current_rows_in_initial_message() {
    let storage = MemoryStorage::new(&["albums"]).expect("valid memory storage families");
    let mut database = Database::new(albums_schema(), storage).await.unwrap();

    let mut batch = database.open_batch();
    batch.insert(
        "albums",
        vec![Value::U64(7), Value::String("Too Early".to_owned())],
    );
    batch.insert(
        "albums",
        vec![Value::U64(11), Value::String("Blue Train".to_owned())],
    );
    database.commit_batch(batch).await.unwrap();

    let subscription = database
        .subscribe_query(select_query(
            Select::new([SelectItem::expr(col("title"))])
                .from([TableRef::named("albums")])
                .where_(Expr::binary(
                    col("id"),
                    BinaryOp::Gt,
                    Expr::Literal(Value::U64(10)),
                )),
        ))
        .await
        .unwrap();

    database.flush().await.unwrap();
    assert_eq!(
        expect_recv_vals(&subscription),
        [(vec!["Blue Train".into()], 1)]
    );
}

#[futures_test::test]
async fn subscription_reports_incremental_query_deltas_through_database_facade() {
    let storage = MemoryStorage::new(&["albums"]).expect("valid memory storage families");
    let mut database = Database::new(albums_schema(), storage).await.unwrap();
    let subscription = database
        .subscribe_query(select_query(
            Select::new([SelectItem::expr(col("id")), SelectItem::expr(col("title"))])
                .from([TableRef::named("albums")])
                .where_(Expr::binary(
                    col("id"),
                    BinaryOp::Gt,
                    Expr::Literal(Value::U64(10)),
                )),
        ))
        .await
        .unwrap();

    assert!(subscription.recv().unwrap().is_empty());

    let mut batch = database.open_batch();
    batch.insert(
        "albums",
        vec![Value::U64(5), Value::String("Out of Scope".to_owned())],
    );
    batch.insert(
        "albums",
        vec![Value::U64(11), Value::String("Blue Train".to_owned())],
    );
    batch.insert(
        "albums",
        vec![Value::U64(13), Value::String("Giant Steps".to_owned())],
    );
    database.commit_batch(batch).await.unwrap();

    assert_eq!(
        expect_recv_vals(&subscription),
        [
            (vec![11_u64.into(), "Blue Train".into()], 1),
            (vec![13_u64.into(), "Giant Steps".into()], 1),
        ]
    );

    let mut batch = database.open_batch();
    batch.update(
        "albums",
        vec![
            Value::U64(5),
            Value::String("Still Out of Scope".to_owned()),
        ],
    );
    batch.update(
        "albums",
        vec![
            Value::U64(11),
            Value::String("Blue Train Take Two".to_owned()),
        ],
    );
    batch.delete("albums", PrimaryKeyValue::U64(13));
    database.commit_batch(batch).await.unwrap();

    // Subscription messages expose weighted result deltas, not full snapshots:
    // unchanged matching rows are absent, the updated row is retracted and
    // re-added, and base-table changes outside the query are not reported.
    assert_eq!(
        expect_recv_vals(&subscription),
        [
            (vec![11_u64.into(), "Blue Train Take Two".into()], 1),
            (vec![11_u64.into(), "Blue Train".into()], -1),
            (vec![13_u64.into(), "Giant Steps".into()], -1),
        ]
    );
}

#[futures_test::test]
async fn subscription_reports_incremental_contains_filter_deltas() {
    let storage = MemoryStorage::new(&["albums"]).expect("valid memory storage families");
    let mut database = Database::new(albums_schema(), storage).await.unwrap();
    let subscription = database
        .subscribe_one_sink(
            GraphBuilder::table("albums")
                .filter(PredicateExpr::Contains {
                    field: "title".to_owned(),
                    value: Value::String("Train".to_owned()).into(),
                })
                .project_fields([ProjectField::named("id"), ProjectField::named("title")]),
        )
        .await
        .unwrap();

    assert!(subscription.recv().unwrap().is_empty());

    let mut batch = database.open_batch();
    batch.insert(
        "albums",
        vec![Value::U64(7), Value::String("Out of Scope".to_owned())],
    );
    batch.insert(
        "albums",
        vec![Value::U64(11), Value::String("Blue Train".to_owned())],
    );
    database.commit_batch(batch).await.unwrap();

    assert_eq!(
        expect_recv_vals(&subscription),
        [(vec![11_u64.into(), "Blue Train".into()], 1)]
    );

    let mut batch = database.open_batch();
    batch.update(
        "albums",
        vec![Value::U64(11), Value::String("Blue Seven".to_owned())],
    );
    batch.update(
        "albums",
        vec![Value::U64(7), Value::String("Night Train".to_owned())],
    );
    database.commit_batch(batch).await.unwrap();

    assert_eq!(
        expect_recv_vals(&subscription),
        [
            (vec![11_u64.into(), "Blue Train".into()], -1),
            (vec![7_u64.into(), "Night Train".into()], 1),
        ]
    );
}

// This is intentionally an IVM-level test: Jazz lowers payload-enum matching
// to this internal predicate, and the regression is the filter's weighted
// incremental behavior rather than a client-facing API concern.
#[futures_test::test]
async fn payload_enum_filter_matches_selected_case_and_emits_cross_case_deltas() {
    let storage = MemoryStorage::new(&["payload_tasks"]).expect("valid memory storage families");
    let mut database = Database::new(payload_enum_tasks_schema(), storage)
        .await
        .unwrap();
    let graph = GraphBuilder::table("payload_tasks")
        .filter(PredicateExpr::EnumMatch {
            field: "state".to_owned(),
            case_tag: 0,
            payload: Box::new(PredicateExpr::eq("priority", Value::U64(1))),
        })
        .project_fields([ProjectField::named("id")]);
    let subscription = database.subscribe_one_sink(graph.clone()).await.unwrap();
    assert!(subscription.recv().unwrap().is_empty());

    let mut batch = database.open_batch();
    batch.insert(
        "payload_tasks",
        vec![
            Value::U64(1),
            Value::Nullable(Some(Box::new(open_task(1, "matching")))),
        ],
    );
    batch.insert(
        "payload_tasks",
        vec![
            Value::U64(2),
            Value::Nullable(Some(Box::new(open_task(2, "wrong payload")))),
        ],
    );
    batch.insert(
        "payload_tasks",
        vec![
            Value::U64(3),
            Value::Nullable(Some(Box::new(closed_task("wrong case")))),
        ],
    );
    batch.insert("payload_tasks", vec![Value::U64(4), Value::Nullable(None)]);
    database.commit_batch(batch).await.unwrap();
    assert_eq!(expect_recv_vals(&subscription), [(vec![1_u64.into()], 1)]);
    assert_eq!(
        database
            .query_graph(graph.clone())
            .await
            .unwrap()
            .to_values()
            .unwrap(),
        [(vec![1_u64.into()], 1)]
    );

    let mut batch = database.open_batch();
    batch.update(
        "payload_tasks",
        vec![
            Value::U64(1),
            Value::Nullable(Some(Box::new(closed_task("moved arm")))),
        ],
    );
    batch.update(
        "payload_tasks",
        vec![
            Value::U64(2),
            Value::Nullable(Some(Box::new(open_task(1, "now matching")))),
        ],
    );
    batch.update(
        "payload_tasks",
        vec![
            Value::U64(3),
            Value::Nullable(Some(Box::new(open_task(1, "changed arm")))),
        ],
    );
    database.commit_batch(batch).await.unwrap();
    assert_eq!(
        expect_recv_vals(&subscription),
        [
            (vec![1_u64.into()], -1),
            (vec![2_u64.into()], 1),
            (vec![3_u64.into()], 1),
        ]
    );

    let mut batch = database.open_batch();
    batch.update(
        "payload_tasks",
        vec![
            Value::U64(2),
            Value::Nullable(Some(Box::new(open_task(2, "no longer matching")))),
        ],
    );
    database.commit_batch(batch).await.unwrap();
    assert_eq!(expect_recv_vals(&subscription), [(vec![2_u64.into()], -1)]);
    assert_eq!(
        database
            .query_graph(graph)
            .await
            .unwrap()
            .to_values()
            .unwrap(),
        [(vec![3_u64.into()], 1)]
    );
}

mod parameters;
mod prepared;
