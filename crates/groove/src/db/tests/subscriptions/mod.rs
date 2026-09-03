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

/// A cancelled cold hydration must release every graph node it reserved for
/// temporal ordering. Some interior nodes may already have completed before
/// the storage request went cold; retaining only the incomplete nodes leaves
/// a later subscription permanently blocked behind the cancelled snapshot.
#[futures_test::test]
async fn cancelling_cold_hydration_releases_completed_barriers() {
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
            Value::String("cancelled hydration album".to_owned()),
        ],
    );
    batch.insert(
        "artists",
        vec![
            Value::U64(1),
            Value::String("later hydration artist".to_owned()),
        ],
    );
    database.commit_batch(batch).await.unwrap();

    // Prime only `albums`: the first subscription will complete this shared
    // source before its `artists` sibling reaches the deliberately cold scan.
    // A later albums-only subscriber therefore waits on a *completed* node
    // retained by the first hydration's temporal barrier.
    database
        .query_graph(GraphBuilder::table("albums"))
        .await
        .unwrap();
    storage.evict_column_family("artists");
    control.pause_on(TestStorageOperation::ScanOpen);

    let first = database
        .subscribe([
            ("albums", GraphBuilder::table("albums")),
            ("artists", GraphBuilder::table("artists")),
        ])
        .unwrap();
    let later = database
        .subscribe_one_sink(GraphBuilder::table("albums"))
        .await
        .unwrap();
    assert!(database.has_pending_progress());
    assert!(database.unsubscribe(first.id()));

    control.resume_operation(TestStorageOperation::ScanOpen);
    for _ in 0..32 {
        database.drive_ready_progress().await.unwrap();
        if !database.has_pending_progress() {
            break;
        }
    }
    assert!(
        !database.has_pending_progress(),
        "the later hydration is not left behind completed barriers from the cancelled one"
    );
    assert_eq!(
        expect_try_recv_vals(&later),
        vec![(
            vec![
                Value::U64(1),
                Value::U64(1),
                Value::String("cancelled hydration album".to_owned()),
            ],
            1,
        )],
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
    let (storage, control) = TestStorage::controlled(&["albums", "artists"]);
    let mut database = Database::new(albums_artists_schema(), storage.clone())
        .await
        .unwrap();
    let mut seed = database.open_batch();
    seed.insert(
        "albums",
        vec![
            Value::U64(1),
            Value::U64(1),
            Value::String("seed".to_owned()),
        ],
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
        "artists",
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

/// Admission of a write waits for the hydration whose graph slice it changes,
/// but must not inherit an older cold wait from an independent subscription.
#[futures_test::test]
async fn direct_write_advances_its_overlapping_hydration_past_an_unrelated_cold_one() {
    use std::future::Future;
    use std::pin::Pin;
    use std::task::{Context, Poll};

    let (storage, control) = TestStorage::controlled(&["albums", "artists"]);
    let mut database = Database::new(albums_artists_schema(), storage.clone())
        .await
        .unwrap();
    let mut seed = database.open_batch();
    seed.insert(
        "albums",
        vec![
            Value::U64(1),
            Value::U64(1),
            Value::String("album".to_owned()),
        ],
    );
    seed.insert(
        "artists",
        vec![Value::U64(1), Value::String("artist".to_owned())],
    );
    database.commit_batch(seed).await.unwrap();
    storage.evict_all();
    control.pause_on(TestStorageOperation::ScanOpen);

    let albums = database
        .subscribe_one_sink(GraphBuilder::table("albums"))
        .await
        .unwrap();
    let artists = database
        .subscribe_one_sink(GraphBuilder::table("artists"))
        .await
        .unwrap();
    assert!(database.has_pending_progress());
    // One scan may advance. Selecting by the write's graph slice must spend
    // this permit on artists rather than the older unrelated albums scan.
    control.release_one();

    let mut write = database.open_batch();
    write.insert(
        "artists",
        vec![Value::U64(2), Value::String("later artist".to_owned())],
    );
    let mut apply = Box::pin(database.apply_batch(write));
    let waker = futures::task::noop_waker();
    let mut cx = Context::from_waker(&waker);
    let mut completed = false;
    for _ in 0..32 {
        match Pin::new(&mut apply).poll(&mut cx) {
            Poll::Ready(Ok(_)) => {
                completed = true;
                break;
            }
            Poll::Ready(Err(error)) => panic!("artist write failed: {error}"),
            Poll::Pending => {}
        }
    }
    assert!(
        completed,
        "artist write must not wait for the unrelated cold albums hydration"
    );

    drop(apply);
    drop(artists);
    drop(albums);
}

/// Selecting a hydration for write admission must also select every temporal
/// predecessor it needs, even when that predecessor is outside the write's
/// directly affected graph slice.
#[futures_test::test]
async fn direct_write_advances_transitive_hydration_predecessors_without_failing_join() {
    use std::future::Future;
    use std::pin::Pin;
    use std::task::{Context, Poll};

    let (storage, control) = TestStorage::controlled(&["albums", "artists"]);
    let mut database = Database::new(albums_artists_schema(), storage.clone())
        .await
        .unwrap();
    let mut seed = database.open_batch();
    seed.insert(
        "albums",
        vec![
            Value::U64(1),
            Value::U64(1),
            Value::String("album".to_owned()),
        ],
    );
    seed.insert(
        "artists",
        vec![Value::U64(1), Value::String("artist".to_owned())],
    );
    database.commit_batch(seed).await.unwrap();

    // Retain a fully hydrated albums source, then make later storage reads
    // cold. H1 owns artists; H2 joins the warm albums source to H1's cold
    // artists source and is therefore temporally behind H1 on artists.
    let warm_albums = database
        .subscribe_one_sink(GraphBuilder::table("albums"))
        .await
        .unwrap();
    database.flush().await.unwrap();
    assert_eq!(expect_try_recv_vals(&warm_albums).len(), 1);
    storage.evict_all();
    control.pause_on(TestStorageOperation::ScanOpen);

    let artists = database
        .subscribe_one_sink(GraphBuilder::table("artists"))
        .await
        .unwrap();
    let joined = database
        .subscribe_one_sink(GraphBuilder::join(
            GraphBuilder::table("albums"),
            GraphBuilder::table("artists"),
            ["artist_id"],
            ["id"],
        ))
        .await
        .unwrap();
    control.release_one();

    let mut write = database.open_batch();
    write.insert(
        "albums",
        vec![
            Value::U64(2),
            Value::U64(1),
            Value::String("later album".to_owned()),
        ],
    );
    let mut apply = Box::pin(database.apply_batch(write));
    let waker = futures::task::noop_waker();
    let mut cx = Context::from_waker(&waker);
    for _ in 0..16 {
        assert!(
            Pin::new(&mut apply).poll(&mut cx).is_pending(),
            "the write cannot bypass its still-cold join hydration"
        );
    }

    control.resume_operation(TestStorageOperation::ScanOpen);
    let mut completed = false;
    for _ in 0..64 {
        match Pin::new(&mut apply).poll(&mut cx) {
            Poll::Ready(Ok(_)) => {
                completed = true;
                break;
            }
            Poll::Ready(Err(error)) => panic!("album write failed: {error}"),
            Poll::Pending => {}
        }
    }
    assert!(
        completed,
        "album write must advance the join's cold artists predecessor"
    );
    drop(apply);

    assert_eq!(
        expect_try_recv_vals(&joined).len(),
        1,
        "the join hydration remains live and publishes its seeded snapshot"
    );
    drop(joined);
    drop(artists);
    drop(warm_albums);
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
        expect_try_recv_vals(&subscription),
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
        expect_try_recv_vals(&subscription),
        [(vec![7_u64.into(), "Blue Train".into()], 1)]
    );

    let mut batch = database.open_batch();
    batch.insert(
        "albums",
        vec![Value::U64(8), Value::String("Giant Steps".to_owned())],
    );
    database.commit_batch(batch).await.unwrap();

    assert_eq!(
        expect_try_recv_vals(&subscription),
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

/// Mutable input sources feed the ordinary maintained graph; they are not a
/// terminal-result side channel. This public-facade test is intentionally
/// integration-level because it proves the tick, collector, and receiver see
/// one cross-source frontier rather than an intermediate replacement.
#[futures_test::test]
async fn input_source_replacements_are_atomic_idempotent_and_revoke_cleanly() {
    let mut database = Database::new(
        albums_schema(),
        MemoryStorage::new(&["albums"]).expect("valid storage families"),
    )
    .await
    .unwrap();
    let descriptor = RecordDescriptor::new([("id", ColumnType::U64)]);
    let left = database.allocate_input_source(descriptor);
    let right = database.allocate_input_source(descriptor);
    let graph = GraphBuilder::join(
        GraphBuilder::input_source(left, descriptor),
        GraphBuilder::input_source(right, descriptor),
        ["id"],
        ["id"],
    )
    .project_fields([
        ProjectField::renamed("left.id", "left_id"),
        ProjectField::renamed("right.id", "right_id"),
    ]);
    let subscription = database.subscribe_one_sink(graph).await.unwrap();
    assert!(subscription.recv().unwrap().is_empty());

    let one = descriptor.create(&[Value::U64(1)]).unwrap();
    let two = descriptor.create(&[Value::U64(2)]).unwrap();
    database
        .replace_input_sources([
            InputSourceReplacement {
                id: right,
                descriptor,
                // Deliberately unordered and duplicated: the observable set
                // is deterministic and must not multiply the join result.
                records: vec![one.clone(), one.clone()],
            },
            InputSourceReplacement {
                id: left,
                descriptor,
                records: vec![one.clone()],
            },
        ])
        .await
        .unwrap();
    assert_eq!(
        expect_recv_vals(&subscription),
        [(vec![Value::U64(1), Value::U64(1)], 1)]
    );

    let idempotent = database
        .replace_input_sources([
            InputSourceReplacement {
                id: left,
                descriptor,
                records: vec![one.clone()],
            },
            InputSourceReplacement {
                id: right,
                descriptor,
                records: vec![one.clone()],
            },
        ])
        .await
        .unwrap();
    assert_eq!(idempotent, TickMetrics::default());
    assert!(subscription.try_recv().is_err());

    database
        .replace_input_sources([
            InputSourceReplacement {
                id: left,
                descriptor,
                records: vec![two.clone()],
            },
            InputSourceReplacement {
                id: right,
                descriptor,
                records: vec![two],
            },
        ])
        .await
        .unwrap();
    assert_eq!(
        expect_recv_vals(&subscription),
        [
            (vec![Value::U64(1), Value::U64(1)], -1),
            (vec![Value::U64(2), Value::U64(2)], 1),
        ]
    );

    database
        .replace_input_sources([InputSourceReplacement {
            id: left,
            descriptor,
            records: vec![],
        }])
        .await
        .unwrap();
    assert_eq!(
        expect_recv_vals(&subscription),
        [(vec![Value::U64(2), Value::U64(2)], -1)]
    );
}

/// An input source can feed both a join and a downstream anti-join in the
/// same maintained graph (the shape required by a required nested include).
/// Replacing both sources atomically must not make the anti-join retract a
/// left row merely because the join arranged it earlier in the same tick.
#[futures_test::test]
async fn atomic_input_replacements_do_not_retract_unpublished_anti_join_rows() {
    let mut database = Database::new(
        albums_schema(),
        MemoryStorage::new(&["albums"]).expect("valid storage families"),
    )
    .await
    .unwrap();
    let left_descriptor =
        RecordDescriptor::new([("id", ColumnType::U64), ("key", ColumnType::U64)]);
    let right_descriptor = RecordDescriptor::new([("key", ColumnType::U64)]);
    let left = database.allocate_input_source(left_descriptor);
    let right = database.allocate_input_source(right_descriptor);
    let left_source = GraphBuilder::input_source(left, left_descriptor);
    let matched = GraphBuilder::join(
        left_source.clone(),
        GraphBuilder::input_source(right, right_descriptor),
        ["key"],
        ["key"],
    )
    .project_fields([
        ProjectField::renamed("left.id", "id"),
        ProjectField::renamed("left.key", "key"),
    ]);
    let visible = GraphBuilder::anti_join(left_source, matched, ["key"], ["key"]);
    let subscription = database.subscribe_one_sink(visible).await.unwrap();
    assert!(subscription.recv().unwrap().is_empty());

    let left_record = left_descriptor
        .create(&[Value::U64(1), Value::U64(7)])
        .unwrap();
    let right_record = right_descriptor.create(&[Value::U64(7)]).unwrap();

    for left_first in [true, false] {
        let left_replacement = InputSourceReplacement {
            id: left,
            descriptor: left_descriptor,
            records: vec![left_record.clone()],
        };
        let right_replacement = InputSourceReplacement {
            id: right,
            descriptor: right_descriptor,
            records: vec![right_record.clone()],
        };
        if left_first {
            database
                .replace_input_sources([left_replacement, right_replacement])
                .await
                .unwrap();
        } else {
            database
                .replace_input_sources([right_replacement, left_replacement])
                .await
                .unwrap();
        }
        assert!(
            subscription.try_recv().is_err(),
            "a matching right row suppresses the left row without a spurious retraction"
        );
        database
            .replace_input_sources([
                InputSourceReplacement {
                    id: left,
                    descriptor: left_descriptor,
                    records: vec![],
                },
                InputSourceReplacement {
                    id: right,
                    descriptor: right_descriptor,
                    records: vec![],
                },
            ])
            .await
            .unwrap();
        assert!(subscription.try_recv().is_err());
    }
}

/// Incremental runtime input changes share the replacement API's atomic
/// cross-source frontier, but do not require callers to resend untouched
/// records. This is the primitive used by receiver-side source closures.
#[futures_test::test]
async fn input_source_deltas_are_atomic_and_proportional_to_changed_records() {
    let mut database = Database::new(
        albums_schema(),
        MemoryStorage::new(&["albums"]).expect("valid storage families"),
    )
    .await
    .unwrap();
    let descriptor = RecordDescriptor::new([("id", ColumnType::U64)]);
    let left = database.allocate_input_source(descriptor);
    let right = database.allocate_input_source(descriptor);
    let graph = GraphBuilder::join(
        GraphBuilder::input_source(left, descriptor),
        GraphBuilder::input_source(right, descriptor),
        ["id"],
        ["id"],
    );
    let subscription = database.subscribe_one_sink(graph).await.unwrap();
    assert!(subscription.recv().unwrap().is_empty());
    let one = descriptor.create(&[Value::U64(1)]).unwrap();
    let two = descriptor.create(&[Value::U64(2)]).unwrap();
    database
        .replace_input_sources([
            InputSourceReplacement {
                id: left,
                descriptor,
                records: vec![one.clone()],
            },
            InputSourceReplacement {
                id: right,
                descriptor,
                records: vec![one.clone()],
            },
        ])
        .await
        .unwrap();
    assert_eq!(
        expect_recv_vals(&subscription),
        [(vec![Value::U64(1), Value::U64(1)], 1)]
    );

    let metrics = database
        .apply_input_source_deltas([
            InputSourceDelta {
                id: left,
                descriptor,
                adds: vec![two.clone()],
                removes: vec![one.clone()],
            },
            InputSourceDelta {
                id: right,
                descriptor,
                adds: vec![two.clone()],
                removes: vec![one],
            },
        ])
        .await
        .unwrap();
    assert!(metrics.records_processed >= 2);
    assert_eq!(
        expect_recv_vals(&subscription),
        [
            (vec![Value::U64(1), Value::U64(1)], -1),
            (vec![Value::U64(2), Value::U64(2)], 1),
        ],
        "one tick exposes only old and new join frontiers"
    );
    assert_eq!(
        database
            .apply_input_source_deltas([InputSourceDelta {
                id: left,
                descriptor,
                adds: vec![two],
                removes: Vec::new(),
            }])
            .await
            .unwrap(),
        TickMetrics::default(),
        "replayed set addition does not advance the input frontier"
    );
}

/// Ungrouped aggregates own one empty group. This stays in the ordinary Groove
/// graph so direct queries, maintained subscriptions, and runtime-owned
/// covered inputs all observe the same identity transition; no caller creates
/// a synthetic aggregate record for an empty source.
#[futures_test::test]
async fn input_source_ungrouped_aggregate_seeds_and_restores_its_empty_identity() {
    let mut database = Database::new(
        albums_schema(),
        MemoryStorage::new(&["albums"]).expect("valid storage families"),
    )
    .await
    .unwrap();
    let descriptor = RecordDescriptor::new([("id", ColumnType::U64)]);
    let source = database.allocate_input_source(descriptor);
    let graph = GraphBuilder::aggregate(
        GraphBuilder::input_source(source, descriptor),
        Vec::<String>::new(),
        [AggregateExpr {
            function: AggregateFunction::Count,
            expression: None,
            distinct: false,
            output_name: Some("count".to_owned()),
        }],
    );
    let subscription = database.subscribe_one_sink(graph).await.unwrap();
    assert_eq!(
        expect_recv_vals(&subscription),
        [(vec![Value::U64(0)], 1)],
        "the initial empty input has the SQL aggregate identity"
    );

    // An explicit complete empty replacement acknowledges the runtime-owned
    // source frontier but does not duplicate the identity already seeded by
    // subscription hydration.
    database
        .replace_input_sources([InputSourceReplacement {
            id: source,
            descriptor,
            records: Vec::new(),
        }])
        .await
        .unwrap();
    assert!(subscription.try_recv().is_err());

    let one = descriptor.create(&[Value::U64(1)]).unwrap();
    database
        .replace_input_sources([InputSourceReplacement {
            id: source,
            descriptor,
            records: vec![one],
        }])
        .await
        .unwrap();
    assert_eq!(
        expect_recv_vals(&subscription),
        [(vec![Value::U64(0)], -1), (vec![Value::U64(1)], 1)]
    );

    database
        .replace_input_sources([InputSourceReplacement {
            id: source,
            descriptor,
            records: Vec::new(),
        }])
        .await
        .unwrap();
    assert_eq!(
        expect_recv_vals(&subscription),
        [(vec![Value::U64(0)], 1), (vec![Value::U64(1)], -1)],
        "removing the final input restores the same empty group identity"
    );
}

/// Grouped aggregates have no group when their source is empty, unlike the
/// single logical empty group of an ungrouped aggregate above.
#[futures_test::test]
async fn input_source_grouped_aggregate_has_no_empty_group() {
    let mut database = Database::new(
        albums_schema(),
        MemoryStorage::new(&["albums"]).expect("valid storage families"),
    )
    .await
    .unwrap();
    let descriptor = RecordDescriptor::new([("bucket", ColumnType::U64)]);
    let source = database.allocate_input_source(descriptor);
    let graph = GraphBuilder::aggregate(
        GraphBuilder::input_source(source, descriptor),
        ["bucket"],
        [AggregateExpr {
            function: AggregateFunction::Count,
            expression: None,
            distinct: false,
            output_name: Some("count".to_owned()),
        }],
    );
    let subscription = database.subscribe_one_sink(graph).await.unwrap();
    assert!(subscription.recv().unwrap().is_empty());

    database
        .replace_input_sources([InputSourceReplacement {
            id: source,
            descriptor,
            records: Vec::new(),
        }])
        .await
        .unwrap();
    assert!(subscription.try_recv().is_err());
}

/// Mutable inputs share the binding delta engine with prepared shapes, but
/// their identity is not a caller-controlled binding name. In particular, a
/// user may prepare the exact string that older runtimes synthesized for the
/// next input without stealing its descriptor, records, or lifecycle.
#[futures_test::test]
async fn input_source_identity_cannot_collide_with_prepared_binding_name() {
    let mut database = Database::new(
        albums_schema(),
        MemoryStorage::new(&["albums"]).expect("valid storage families"),
    )
    .await
    .unwrap();
    let descriptor = RecordDescriptor::new([("id", ColumnType::U64)]);
    let formerly_colliding_name = database
        .ivm_runtime
        .next_input_source_legacy_binding_shape();

    let prepared = database
        .prepare_one_sink(
            GraphBuilder::binding_source(formerly_colliding_name.clone(), descriptor),
            formerly_colliding_name.clone(),
            descriptor,
            ["id"],
        )
        .await
        .unwrap();
    let prepared_subscription = database
        .bind_shape_one_sink(prepared.id(), &[Value::U64(7)])
        .await
        .unwrap();
    assert_eq!(
        expect_recv_vals(&prepared_subscription),
        [(vec![Value::U64(7)], 1)]
    );

    let input = database.allocate_input_source(descriptor);
    assert_eq!(input.legacy_binding_shape(), formerly_colliding_name);
    let input_subscription = database
        .subscribe_one_sink(GraphBuilder::input_source(input, descriptor))
        .await
        .unwrap();
    assert!(input_subscription.recv().unwrap().is_empty());

    let record = descriptor.create(&[Value::U64(11)]).unwrap();
    database
        .replace_input_sources([InputSourceReplacement {
            id: input,
            descriptor,
            records: vec![record],
        }])
        .await
        .unwrap();
    assert_eq!(
        expect_recv_vals(&input_subscription),
        [(vec![Value::U64(11)], 1)]
    );
    assert!(prepared_subscription.try_recv().is_err());

    database.retire_input_sources([input]).await.unwrap();
    assert_eq!(
        expect_recv_vals(&input_subscription),
        [(vec![Value::U64(11)], -1)]
    );

    // Retiring the mutable source cannot retire or clear the independently
    // prepared source that happens to use the same historical string.
    let second_prepared_subscription = database
        .bind_shape_one_sink(prepared.id(), &[Value::U64(13)])
        .await
        .unwrap();
    assert_eq!(
        expect_recv_vals(&second_prepared_subscription),
        [(vec![Value::U64(13)], 1)]
    );
    assert!(prepared_subscription.try_recv().is_err());
}

#[futures_test::test]
async fn input_source_batch_rejects_descriptor_conflicts_before_mutating_any_source() {
    let mut database = Database::new(
        albums_schema(),
        MemoryStorage::new(&["albums"]).expect("valid storage families"),
    )
    .await
    .unwrap();
    let u64_descriptor = RecordDescriptor::new([("id", ColumnType::U64)]);
    let string_descriptor = RecordDescriptor::new([("id", ColumnType::String)]);
    let id = database.allocate_input_source(u64_descriptor);
    let subscription = database
        .subscribe_one_sink(GraphBuilder::input_source(id, u64_descriptor))
        .await
        .unwrap();
    assert!(subscription.recv().unwrap().is_empty());
    let one = u64_descriptor.create(&[Value::U64(1)]).unwrap();
    assert!(matches!(
        database
            .replace_input_sources([
                InputSourceReplacement {
                    id,
                    descriptor: u64_descriptor,
                    records: vec![one],
                },
                InputSourceReplacement {
                    id,
                    descriptor: string_descriptor,
                    records: vec![],
                },
            ])
            .await,
        Err(Error::IvmRuntime(
            IvmRuntimeError::BindingSourceDescriptorMismatch(_)
        ))
    ));
    assert!(subscription.try_recv().is_err());

    database
        .replace_input_sources([InputSourceReplacement {
            id,
            descriptor: u64_descriptor,
            records: vec![u64_descriptor.create(&[Value::U64(1)]).unwrap()],
        }])
        .await
        .unwrap();
    assert_eq!(
        expect_try_recv_vals(&subscription),
        [(vec![Value::U64(1)], 1)]
    );

    let other = database.allocate_input_source(u64_descriptor);
    let other_subscription = database
        .subscribe_one_sink(GraphBuilder::input_source(other, u64_descriptor))
        .await
        .unwrap();
    assert!(other_subscription.recv().unwrap().is_empty());
    assert!(matches!(
        database
            .replace_input_sources([
                InputSourceReplacement {
                    id,
                    descriptor: string_descriptor,
                    records: vec![],
                },
                InputSourceReplacement {
                    id: other,
                    descriptor: u64_descriptor,
                    records: vec![u64_descriptor.create(&[Value::U64(2)]).unwrap()],
                },
            ])
            .await,
        Err(Error::IvmRuntime(
            IvmRuntimeError::BindingSourceDescriptorMismatch(_)
        ))
    ));
    assert!(subscription.try_recv().is_err());
    assert!(other_subscription.try_recv().is_err());
}

#[futures_test::test]
async fn input_sources_cannot_cross_runtime_boundaries() {
    let storage = MemoryStorage::new(&["albums"]).expect("valid storage families");
    let mut owner = Database::new(albums_schema(), storage.clone())
        .await
        .unwrap();
    let mut other = Database::new(albums_schema(), storage).await.unwrap();
    let descriptor = RecordDescriptor::new([("id", ColumnType::U64)]);
    let id = owner.allocate_input_source(descriptor);
    assert!(matches!(
        other
            .subscribe_one_sink(GraphBuilder::input_source(id, descriptor))
            .await,
        Err(Error::IvmRuntime(IvmRuntimeError::ForeignInputSource))
    ));
}

#[futures_test::test]
async fn input_source_descriptor_is_registered_at_graph_compile_and_mismatches_are_recoverable() {
    let mut database = Database::new(
        albums_schema(),
        MemoryStorage::new(&["albums"]).expect("valid storage families"),
    )
    .await
    .unwrap();
    let string_descriptor = RecordDescriptor::new([("value", ColumnType::String)]);
    let u64_descriptor = RecordDescriptor::new([("value", ColumnType::U64)]);
    let id = database.allocate_input_source(string_descriptor);
    let subscription = database
        .subscribe_one_sink(GraphBuilder::input_source(id, string_descriptor))
        .await
        .unwrap();
    assert!(subscription.recv().unwrap().is_empty());

    // A second graph cannot reinterpret an already-compiled source identity.
    assert!(matches!(
        database
            .subscribe_one_sink(GraphBuilder::input_source(id, u64_descriptor))
            .await,
        Err(Error::IvmRuntime(
            IvmRuntimeError::BindingSourceDescriptorMismatch(_)
        ))
    ));

    // The rejected replacement is preflight-only: it must neither poison the
    // database nor install refcounts under the wrong descriptor.
    assert!(matches!(
        database
            .replace_input_sources([InputSourceReplacement {
                id,
                descriptor: u64_descriptor,
                records: vec![u64_descriptor.create(&[Value::U64(7)]).unwrap()],
            }])
            .await,
        Err(Error::IvmRuntime(
            IvmRuntimeError::BindingSourceDescriptorMismatch(_)
        ))
    ));
    assert!(subscription.try_recv().is_err());

    database
        .replace_input_sources([InputSourceReplacement {
            id,
            descriptor: string_descriptor,
            records: vec![
                string_descriptor
                    .create(&[Value::String("accepted after mismatch".to_owned())])
                    .unwrap(),
            ],
        }])
        .await
        .unwrap();
    assert_eq!(
        expect_try_recv_vals(&subscription),
        [(vec![Value::String("accepted after mismatch".to_owned())], 1)]
    );
}

#[futures_test::test]
async fn retiring_input_sources_retracts_live_records_and_releases_runtime_state() {
    let mut database = Database::new(
        albums_schema(),
        MemoryStorage::new(&["albums"]).expect("valid storage families"),
    )
    .await
    .unwrap();
    let descriptor = RecordDescriptor::new([("id", ColumnType::U64)]);
    let id = database.allocate_input_source(descriptor);
    assert_eq!(database.ivm_runtime.active_input_source_state_count(), 1);
    let subscription = database
        .subscribe_one_sink(GraphBuilder::input_source(id, descriptor))
        .await
        .unwrap();
    assert!(subscription.recv().unwrap().is_empty());
    let record = descriptor.create(&[Value::U64(11)]).unwrap();
    database
        .replace_input_sources([InputSourceReplacement {
            id,
            descriptor,
            records: vec![record],
        }])
        .await
        .unwrap();
    assert_eq!(
        expect_try_recv_vals(&subscription),
        [(vec![Value::U64(11)], 1)]
    );

    database.retire_input_sources([id]).await.unwrap();
    assert_eq!(
        expect_try_recv_vals(&subscription),
        [(vec![Value::U64(11)], -1)]
    );
    assert_eq!(database.ivm_runtime.active_input_source_state_count(), 0);
    assert!(matches!(
        database.retire_input_sources([id]).await,
        Err(Error::IvmRuntime(IvmRuntimeError::InputSourceRetired))
    ));
    assert!(matches!(
        database
            .replace_input_sources([InputSourceReplacement {
                id,
                descriptor,
                records: vec![],
            }])
            .await,
        Err(Error::IvmRuntime(IvmRuntimeError::InputSourceRetired))
    ));

    // Retired identities have no tombstone map: allocation is monotone, while
    // active source state returns to its baseline after each short-lived use.
    for _ in 0..16 {
        let transient = database.allocate_input_source(descriptor);
        database.retire_input_sources([transient]).await.unwrap();
        assert_eq!(database.ivm_runtime.active_input_source_state_count(), 0);
    }
}

#[futures_test::test]
async fn input_source_retirement_rejects_foreign_runtime_identity() {
    let storage = MemoryStorage::new(&["albums"]).expect("valid storage families");
    let descriptor = RecordDescriptor::new([("id", ColumnType::U64)]);
    let mut owner = Database::new(albums_schema(), storage.clone())
        .await
        .unwrap();
    let mut other = Database::new(albums_schema(), storage).await.unwrap();
    let foreign = owner.allocate_input_source(descriptor);
    assert!(matches!(
        other.retire_input_sources([foreign]).await,
        Err(Error::IvmRuntime(IvmRuntimeError::ForeignInputSource))
    ));
}

mod parameters;
mod prepared;
