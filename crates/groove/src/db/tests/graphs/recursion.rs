//! Recursive closure, retraction, and convergence.

use super::*;
use crate::storage::IdbStorage;
use idb_tree::{BoxFuture, Commit, Metadata, PageStore};
use std::future::Future;
use std::pin::Pin;
use std::task::Poll;

#[derive(Clone, Default)]
struct YieldingPageStore(idb_tree::MemoryPageStore);

impl PageStore for YieldingPageStore {
    fn load_metadata(&self) -> BoxFuture<'_, Result<Option<Metadata>, String>> {
        Box::pin(async move {
            YieldOnce(false).await;
            self.0.load_metadata().await
        })
    }

    fn read_page(&self, page_id: u64) -> BoxFuture<'_, Result<Option<Vec<u8>>, String>> {
        Box::pin(async move {
            YieldOnce(false).await;
            self.0.read_page(page_id).await
        })
    }

    fn commit<'a>(&'a self, commit: &'a Commit) -> BoxFuture<'a, Result<Metadata, String>> {
        Box::pin(async move {
            YieldOnce(false).await;
            self.0.commit(commit).await
        })
    }
}

struct YieldOnce(bool);

impl Future for YieldOnce {
    type Output = ();

    fn poll(mut self: Pin<&mut Self>, cx: &mut std::task::Context<'_>) -> Poll<Self::Output> {
        if self.0 {
            Poll::Ready(())
        } else {
            self.0 = true;
            cx.waker().wake_by_ref();
            Poll::Pending
        }
    }
}

#[futures_test::test]
async fn deep_recursive_step_evaluates_with_constant_stack() {
    let storage = MemoryStorage::new(&["edges"]).expect("valid memory storage families");
    let mut database = Database::new(edges_schema(), storage).await.unwrap();
    let seed = GraphBuilder::table("edges").project(["src", "dst"]);
    let frontier = GraphBuilder::frontier_source(
        "frontier",
        RecordDescriptor::new([
            ("src", ColumnType::U64.clone()),
            ("dst", ColumnType::U64.clone()),
        ]),
    );
    let edge_pairs = GraphBuilder::table("edges").project(["src", "dst"]);
    let mut step = GraphBuilder::join(frontier, edge_pairs, ["dst"], ["src"]).project_fields([
        ProjectField::renamed("left.src", "src"),
        ProjectField::renamed("right.dst", "dst"),
    ]);
    for _ in 0..48 {
        step = step.filter(PredicateExpr::gt("src", Value::U64(0)));
    }
    let subscription = database
        .subscribe([(
            "result",
            GraphBuilder::recursive(seed, step, "frontier", 16),
        )])
        .unwrap();

    let mut batch = database.open_batch();
    insert_edge(&mut batch, 1, 1, 2);
    database.commit_batch(batch).await.unwrap();

    assert_eq!(
        subscription
            .recv()
            .unwrap()
            .get("result")
            .unwrap()
            .to_values()
            .unwrap(),
        vec![(vec![Value::U64(1), Value::U64(2)], 1)]
    );
}

/// A resident recursive subscription can contain a much deeper graph than the
/// outer IVM work queue sees: the recursive snapshot walks its seed/step graph
/// privately. That walk must yield through the runtime owner while retaining
/// its postorder continuation, so it neither blocks unrelated runtime work nor
/// replays already-evaluated shared children on every turn.
#[futures_test::test]
async fn deep_recursive_hydration_yields_and_preserves_its_snapshot() {
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};

    use futures::task::{ArcWake, waker};

    struct WakeCount(AtomicUsize);

    impl ArcWake for WakeCount {
        fn wake_by_ref(arc_self: &Arc<Self>) {
            arc_self.0.fetch_add(1, Ordering::AcqRel);
        }
    }

    let storage = MemoryStorage::new(&["edges"]).expect("valid memory storage families");
    let mut database = Database::new(edges_schema(), storage).await.unwrap();
    let mut batch = database.open_batch();
    insert_edge(&mut batch, 1, 1, 2);
    database.commit_batch(batch).await.unwrap();

    let seed = GraphBuilder::table("edges").project(["src", "dst"]);
    let frontier = GraphBuilder::frontier_source(
        "frontier",
        RecordDescriptor::new([
            ("src", ColumnType::U64.clone()),
            ("dst", ColumnType::U64.clone()),
        ]),
    );
    let edge_pairs = GraphBuilder::table("edges").project(["src", "dst"]);
    let mut step = GraphBuilder::join(frontier, edge_pairs, ["dst"], ["src"]).project_fields([
        ProjectField::renamed("left.src", "src"),
        ProjectField::renamed("right.dst", "dst"),
    ]);
    for _ in 0..48 {
        step = step.filter(PredicateExpr::gt("src", Value::U64(0)));
    }
    let wakes = Arc::new(WakeCount(AtomicUsize::new(0)));
    let owner_waker = waker(Arc::clone(&wakes));
    let subscription = database
        .subscribe_with_waker(
            [(
                "result",
                GraphBuilder::recursive(seed, step, "frontier", 16),
            )],
            Some(&owner_waker),
        )
        .unwrap();
    let mut previous_wakes = 0;
    let mut yielded = false;
    for _ in 0..128 {
        database
            .drive_ready_progress_with_waker(Some(&owner_waker))
            .await
            .unwrap();
        if !database.has_pending_progress() {
            break;
        }
        yielded = true;
        let wakes_now = wakes.0.load(Ordering::Acquire);
        assert!(
            wakes_now > previous_wakes,
            "each bounded recursive owner turn schedules exactly one continuation"
        );
        previous_wakes = wakes_now;
    }

    assert!(
        yielded,
        "the deep private recursion must not complete in one turn"
    );
    assert!(
        !database.has_pending_progress(),
        "the retained traversal converges without a hot-loop"
    );
    assert_eq!(
        subscription
            .recv()
            .unwrap()
            .get("result")
            .unwrap()
            .to_values()
            .unwrap(),
        vec![(vec![Value::U64(1), Value::U64(2)], 1)]
    );
}

#[futures_test::test]
async fn recursive_hydration_retains_frontiers_until_the_full_closure_is_ready() {
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};

    use futures::task::{ArcWake, waker};

    struct WakeCount(AtomicUsize);

    impl ArcWake for WakeCount {
        fn wake_by_ref(arc_self: &Arc<Self>) {
            arc_self.0.fetch_add(1, Ordering::AcqRel);
        }
    }

    let storage = MemoryStorage::new(&["edges"]).expect("valid memory storage families");
    let mut database = Database::new(edges_schema(), storage).await.unwrap();
    let mut batch = database.open_batch();
    insert_edge(&mut batch, 1, 1, 2);
    insert_edge(&mut batch, 2, 2, 3);
    insert_edge(&mut batch, 3, 3, 4);
    database.commit_batch(batch).await.unwrap();

    let wakes = Arc::new(WakeCount(AtomicUsize::new(0)));
    let owner_waker = waker(Arc::clone(&wakes));
    let subscription = database
        .subscribe_with_waker([("result", reachability_graph(16))], Some(&owner_waker))
        .unwrap();
    for _ in 0..128 {
        database
            .drive_ready_progress_with_waker(Some(&owner_waker))
            .await
            .unwrap();
        if !database.has_pending_progress() {
            break;
        }
    }
    assert!(
        !database.has_pending_progress(),
        "all retained recursive frontiers eventually settle"
    );
    assert!(
        wakes.0.load(Ordering::Acquire) > 0,
        "frontier phases hand their continuation back to the runtime owner"
    );

    let mut values = subscription
        .recv()
        .unwrap()
        .get("result")
        .unwrap()
        .to_values()
        .unwrap();
    sort_pairs_by_value(&mut values);
    assert_eq!(
        values,
        [
            (vec![Value::U64(1), Value::U64(2)], 1),
            (vec![Value::U64(1), Value::U64(3)], 1),
            (vec![Value::U64(1), Value::U64(4)], 1),
            (vec![Value::U64(2), Value::U64(3)], 1),
            (vec![Value::U64(2), Value::U64(4)], 1),
            (vec![Value::U64(3), Value::U64(4)], 1),
        ]
    );
}

/// Retractions force a full recursive snapshot during a live tick. That path
/// shares the same bounded postorder continuation as initial hydration: a
/// delete must not trade a correct retraction for an owner-turn monopoly.
#[futures_test::test]
async fn recursive_live_recompute_retains_traversal_through_retraction() {
    let storage = MemoryStorage::new(&["edges"]).expect("valid memory storage families");
    let mut database = Database::new(edges_schema(), storage).await.unwrap();
    let subscription = database
        .subscribe_one_sink(reachability_graph(16))
        .await
        .unwrap();
    for _ in 0..128 {
        database.drive_ready_progress().await.unwrap();
        if !database.has_pending_progress() {
            break;
        }
    }
    assert!(!database.has_pending_progress());

    let mut batch = database.open_batch();
    insert_edge(&mut batch, 1, 1, 2);
    insert_edge(&mut batch, 2, 2, 3);
    insert_edge(&mut batch, 3, 3, 4);
    database.commit_batch(batch).await.unwrap();
    let _initial = expect_recv_vals(&subscription);

    let mut batch = database.open_batch();
    batch.delete("edges", PrimaryKeyValue::U64(2));
    database.commit_batch(batch).await.unwrap();
    let mut values = expect_recv_vals(&subscription);
    sort_pairs_by_value(&mut values);
    assert_eq!(
        values,
        [
            (vec![Value::U64(1), Value::U64(3)], -1),
            (vec![Value::U64(1), Value::U64(4)], -1),
            (vec![Value::U64(2), Value::U64(3)], -1),
            (vec![Value::U64(2), Value::U64(4)], -1),
        ]
    );
}

#[futures_test::test]
async fn recursive_graph_subscriptions_settle_transitive_closure_in_one_tick() {
    let storage = MemoryStorage::new(&["edges"]).expect("valid memory storage families");
    let mut database = Database::new(edges_schema(), storage).await.unwrap();
    let subscription_id = database
        .subscribe_one_sink(reachability_graph(16))
        .await
        .unwrap();

    let mut batch = database.open_batch();
    insert_edge(&mut batch, 1, 1, 2);
    insert_edge(&mut batch, 2, 2, 3);
    insert_edge(&mut batch, 3, 3, 4);
    database.commit_batch(batch).await.unwrap();
    let mut values = expect_recv_vals(&subscription_id);
    sort_pairs_by_value(&mut values);

    assert_eq!(
        values,
        [
            (vec![Value::U64(1), Value::U64(2)], 1),
            (vec![Value::U64(1), Value::U64(3)], 1),
            (vec![Value::U64(1), Value::U64(4)], 1),
            (vec![Value::U64(2), Value::U64(3)], 1),
            (vec![Value::U64(2), Value::U64(4)], 1),
            (vec![Value::U64(3), Value::U64(4)], 1),
        ]
    );
}

#[futures_test::test]
async fn recursive_graph_subscriptions_settle_with_async_idb_tree_storage() {
    let page_store = YieldingPageStore::default();
    let storage = IdbStorage::open(page_store.clone(), &["edges"])
        .await
        .unwrap();
    let mut database = Database::new(edges_schema(), storage).await.unwrap();
    let subscription_id = database
        .subscribe_one_sink(reachability_graph(16))
        .await
        .unwrap();

    let mut batch = database.open_batch();
    insert_edge(&mut batch, 1, 1, 2);
    insert_edge(&mut batch, 2, 2, 3);
    insert_edge(&mut batch, 3, 3, 4);
    database.commit_batch(batch).await.unwrap();
    let mut values = expect_recv_vals(&subscription_id);
    sort_pairs_by_value(&mut values);
    assert_eq!(values.len(), 6);

    drop(database);
    let storage = IdbStorage::open(page_store, &["edges"]).await.unwrap();
    let mut reopened = Database::new(edges_schema(), storage).await.unwrap();
    let reopened_subscription = reopened
        .subscribe_one_sink(reachability_graph(16))
        .await
        .unwrap();
    reopened.drive_progress().await.unwrap();
    let mut reopened_values = expect_recv_vals(&reopened_subscription);
    sort_pairs_by_value(&mut reopened_values);
    assert_eq!(reopened_values.len(), 6);
}

#[futures_test::test]
async fn recursive_graph_subscriptions_retract_derived_paths_after_delete() {
    let storage = MemoryStorage::new(&["edges"]).expect("valid memory storage families");
    let mut database = Database::new(edges_schema(), storage).await.unwrap();
    let subscription_id = database
        .subscribe_one_sink(reachability_graph(16))
        .await
        .unwrap();

    let mut batch = database.open_batch();
    insert_edge(&mut batch, 1, 1, 2);
    insert_edge(&mut batch, 2, 2, 3);
    insert_edge(&mut batch, 3, 3, 4);
    database.commit_batch(batch).await.unwrap();
    assert_eq!(
        database
            .last_commit_metrics()
            .unwrap()
            .tick
            .recursive_recomputes,
        1
    );
    let _initial_reach = expect_recv_vals(&subscription_id);

    let mut batch = database.open_batch();
    batch.delete("edges", PrimaryKeyValue::U64(2));
    database.commit_batch(batch).await.unwrap();
    assert_eq!(
        database
            .last_commit_metrics()
            .unwrap()
            .tick
            .recursive_recomputes,
        1
    );
    let mut values = expect_recv_vals(&subscription_id);
    sort_pairs_by_value(&mut values);

    assert_eq!(
        values,
        [
            (vec![Value::U64(1), Value::U64(3)], -1),
            (vec![Value::U64(1), Value::U64(4)], -1),
            (vec![Value::U64(2), Value::U64(3)], -1),
            (vec![Value::U64(2), Value::U64(4)], -1),
        ]
    );
}

#[futures_test::test]
async fn prepared_recursive_binding_retracts_transitive_paths_after_edge_delete() {
    let storage = MemoryStorage::new(&["edges"]).expect("valid memory storage families");
    let mut database = Database::new(edges_schema(), storage).await.unwrap();
    let shape = prepared_reachability_shape(&mut database).await;
    let subscription = database
        .bind_shape_one_sink(shape.id(), &[Value::U64(1)])
        .await
        .unwrap();
    let _empty = subscription.recv().unwrap();

    let mut batch = database.open_batch();
    insert_edge(&mut batch, 1, 1, 2);
    insert_edge(&mut batch, 2, 2, 3);
    insert_edge(&mut batch, 3, 3, 4);
    database.commit_batch(batch).await.unwrap();
    let _initial = expect_recv_vals(&subscription);

    let mut batch = database.open_batch();
    batch.delete("edges", PrimaryKeyValue::U64(2));
    database.commit_batch(batch).await.unwrap();
    let mut values = expect_recv_vals(&subscription);
    sort_pairs_by_value(&mut values);

    assert_eq!(
        values,
        [
            (vec![Value::U64(1), Value::U64(3)], -1),
            (vec![Value::U64(1), Value::U64(4)], -1),
        ]
    );
}

#[futures_test::test]
async fn prepared_recursive_binding_skips_recompute_for_unrelated_table_delta() {
    let storage = MemoryStorage::new(&["edges", "docs"]).expect("valid memory storage families");
    let mut database = Database::new(edges_docs_schema(), storage).await.unwrap();
    let shape = database
        .prepare_one_sink(
            prepared_reachability_graph(GraphBuilder::table("edges"), 16),
            "prepared-reach",
            RecordDescriptor::new([("seed", ColumnType::U64.clone())]),
            ["seed".to_owned()],
        )
        .await
        .unwrap();
    let subscription = database
        .bind_shape_one_sink(shape.id(), &[Value::U64(1)])
        .await
        .unwrap();
    assert_eq!(
        expect_recv_vals(&subscription),
        [(vec![Value::U64(1), Value::U64(1)], 1)]
    );

    let mut batch = database.open_batch();
    insert_edge(&mut batch, 1, 1, 2);
    insert_edge(&mut batch, 2, 2, 3);
    database.commit_batch(batch).await.unwrap();
    let mut initial = expect_recv_vals(&subscription);
    sort_pairs_by_value(&mut initial);
    assert_eq!(
        initial,
        [
            (vec![Value::U64(1), Value::U64(2)], 1),
            (vec![Value::U64(1), Value::U64(3)], 1),
        ]
    );

    let mut batch = database.open_batch();
    batch.insert("docs", vec![Value::U64(11), Value::U64(99)]);
    database.commit_batch(batch).await.unwrap();
    assert_eq!(
        database
            .last_commit_metrics()
            .unwrap()
            .tick
            .recursive_recomputes,
        0
    );
    assert!(subscription.try_recv().is_err());
}

#[futures_test::test]
async fn prepared_recursive_binding_recomputes_for_relevant_insert_and_retraction() {
    let storage = MemoryStorage::new(&["edges"]).expect("valid memory storage families");
    let mut database = Database::new(edges_schema(), storage).await.unwrap();
    let shape = prepared_reachability_shape(&mut database).await;
    let subscription = database
        .bind_shape_one_sink(shape.id(), &[Value::U64(1)])
        .await
        .unwrap();
    assert_eq!(
        expect_recv_vals(&subscription),
        [(vec![Value::U64(1), Value::U64(1)], 1)]
    );

    let mut batch = database.open_batch();
    insert_edge(&mut batch, 1, 1, 2);
    database.commit_batch(batch).await.unwrap();
    // Sanctioned by ARC 2 step-delta recursion instruction: the insert half
    // used to pin a recompute mechanism, not semantics. Positive step-table
    // inserts now run semi-naive incrementally; retractions below still
    // recompute.
    assert_eq!(
        database
            .last_commit_metrics()
            .unwrap()
            .tick
            .recursive_recomputes,
        0
    );
    assert_eq!(
        expect_recv_vals(&subscription),
        [(vec![Value::U64(1), Value::U64(2)], 1)]
    );

    let mut batch = database.open_batch();
    batch.delete("edges", PrimaryKeyValue::U64(1));
    database.commit_batch(batch).await.unwrap();
    assert_eq!(
        database
            .last_commit_metrics()
            .unwrap()
            .tick
            .recursive_recomputes,
        1
    );
    assert_eq!(
        expect_recv_vals(&subscription),
        [(vec![Value::U64(1), Value::U64(2)], -1)]
    );
}

#[futures_test::test]
async fn prepared_recursive_positive_step_inserts_match_recompute_diff_without_recompute() {
    let storage = MemoryStorage::new(&["edges"]).expect("valid memory storage families");
    let mut database = Database::new(edges_schema(), storage).await.unwrap();
    let shape = prepared_reachability_shape(&mut database).await;
    let subscription = database
        .bind_shape_one_sink(shape.id(), &[Value::U64(1)])
        .await
        .unwrap();
    assert_eq!(
        expect_recv_vals(&subscription),
        [(vec![Value::U64(1), Value::U64(1)], 1)]
    );

    let mut edges = Vec::<(u64, u64)>::new();
    let mut previous = prepared_reachability_oracle(1, &edges);
    let inserts = seeded_positive_edge_insertions();
    for (idx, (src, dst)) in inserts.into_iter().enumerate() {
        let mut batch = database.open_batch();
        insert_edge(&mut batch, idx as u64 + 1, src, dst);
        database.commit_batch(batch).await.unwrap();
        assert_eq!(
            database
                .last_commit_metrics()
                .unwrap()
                .tick
                .recursive_recomputes,
            0,
            "positive prepared recursive step insert should not recompute at index {idx}: {src}->{dst}"
        );

        edges.push((src, dst));
        let next = prepared_reachability_oracle(1, &edges);
        let mut expected = next
            .difference(&previous)
            .map(|dst| (vec![Value::U64(1), Value::U64(*dst)], 1))
            .collect::<Vec<_>>();
        sort_pairs_by_value(&mut expected);

        if expected.is_empty() {
            assert!(
                subscription.try_recv().is_err(),
                "already-known/re-derived edge {src}->{dst} should emit no recursive delta"
            );
        } else {
            let mut actual = expect_recv_vals(&subscription);
            sort_pairs_by_value(&mut actual);
            assert_eq!(
                actual, expected,
                "positive recursive step insert {src}->{dst} must match recompute diff"
            );
        }
        previous = next;
    }
}

#[futures_test::test]
async fn prepared_recursive_binding_retracts_paths_after_first_edge_delete() {
    let storage = MemoryStorage::new(&["edges"]).expect("valid memory storage families");
    let mut database = Database::new(edges_schema(), storage).await.unwrap();
    let shape = prepared_reachability_shape(&mut database).await;
    let subscription = database
        .bind_shape_one_sink(shape.id(), &[Value::U64(1)])
        .await
        .unwrap();
    let _empty = subscription.recv().unwrap();

    let mut batch = database.open_batch();
    insert_edge(&mut batch, 1, 1, 2);
    insert_edge(&mut batch, 2, 2, 3);
    insert_edge(&mut batch, 3, 3, 4);
    database.commit_batch(batch).await.unwrap();
    let _initial = expect_recv_vals(&subscription);

    let mut batch = database.open_batch();
    batch.delete("edges", PrimaryKeyValue::U64(1));
    database.commit_batch(batch).await.unwrap();
    let mut values = expect_recv_vals(&subscription);
    sort_pairs_by_value(&mut values);

    assert_eq!(
        values,
        [
            (vec![Value::U64(1), Value::U64(2)], -1),
            (vec![Value::U64(1), Value::U64(3)], -1),
            (vec![Value::U64(1), Value::U64(4)], -1),
        ]
    );
}

#[futures_test::test]
async fn prepared_recursive_binding_retraction_recomputes_instead_of_erroring() {
    let storage = MemoryStorage::new(&["edges"]).expect("valid memory storage families");
    let mut database = Database::new(edges_schema(), storage).await.unwrap();
    let shape = prepared_reachability_shape(&mut database).await;
    let first = database
        .bind_shape_one_sink(shape.id(), &[Value::U64(1)])
        .await
        .unwrap();
    let _empty = first.recv().unwrap();

    let mut batch = database.open_batch();
    insert_edge(&mut batch, 1, 1, 2);
    insert_edge(&mut batch, 2, 2, 3);
    insert_edge(&mut batch, 3, 9, 10);
    insert_edge(&mut batch, 4, 5, 6);
    database.commit_batch(batch).await.unwrap();

    let mut initial = expect_recv_vals(&first);
    sort_pairs_by_value(&mut initial);
    assert_eq!(
        initial,
        [
            (vec![Value::U64(1), Value::U64(2)], 1),
            (vec![Value::U64(1), Value::U64(3)], 1),
        ]
    );

    let second = database
        .bind_shape_one_sink(shape.id(), &[Value::U64(9)])
        .await
        .unwrap();
    let mut next = expect_recv_vals(&second);
    sort_pairs_by_value(&mut next);
    assert_eq!(
        next,
        [
            (vec![Value::U64(9), Value::U64(9)], 1),
            (vec![Value::U64(9), Value::U64(10)], 1),
        ]
    );

    drop(first);
    let mut batch = database.open_batch();
    insert_edge(&mut batch, 5, 3, 4);
    database.commit_batch(batch).await.unwrap();

    database.flush().await.unwrap();
    assert_eq!(
        database.last_tick_metrics().unwrap().recursive_recomputes,
        1
    );

    let third = database
        .bind_shape_one_sink(shape.id(), &[Value::U64(5)])
        .await
        .unwrap();
    let mut third_values = expect_recv_vals(&third);
    sort_pairs_by_value(&mut third_values);
    assert_eq!(
        third_values,
        [
            (vec![Value::U64(5), Value::U64(5)], 1),
            (vec![Value::U64(5), Value::U64(6)], 1),
        ]
    );
}

#[futures_test::test]
async fn prepared_recursive_binding_retracts_transitive_paths_from_antijoin_input() {
    let storage =
        MemoryStorage::new(&["edges", "blockers"]).expect("valid memory storage families");
    let mut database = Database::new(edges_blockers_schema(), storage)
        .await
        .unwrap();
    let shape = prepared_reachability_with_antijoin_shape(&mut database).await;
    let subscription = database
        .bind_shape_one_sink(shape.id(), &[Value::U64(1)])
        .await
        .unwrap();
    let _empty = subscription.recv().unwrap();

    let mut batch = database.open_batch();
    insert_edge(&mut batch, 1, 1, 2);
    insert_edge(&mut batch, 2, 2, 3);
    insert_edge(&mut batch, 3, 3, 4);
    database.commit_batch(batch).await.unwrap();
    let _initial = expect_recv_vals(&subscription);

    let mut batch = database.open_batch();
    batch.insert(
        "blockers",
        vec![Value::U64(1), Value::U64(2), Value::U64(3)],
    );
    database.commit_batch(batch).await.unwrap();
    let mut values = expect_recv_vals(&subscription);
    sort_pairs_by_value(&mut values);

    assert_eq!(
        values,
        [
            (vec![Value::U64(1), Value::U64(3)], -1),
            (vec![Value::U64(1), Value::U64(4)], -1),
        ]
    );
}

#[futures_test::test]
async fn prepared_recursive_binding_retracts_first_paths_from_antijoin_input() {
    let storage =
        MemoryStorage::new(&["edges", "blockers"]).expect("valid memory storage families");
    let mut database = Database::new(edges_blockers_schema(), storage)
        .await
        .unwrap();
    let shape = prepared_reachability_with_antijoin_shape(&mut database).await;
    let subscription = database
        .bind_shape_one_sink(shape.id(), &[Value::U64(1)])
        .await
        .unwrap();
    let _empty = subscription.recv().unwrap();

    let mut batch = database.open_batch();
    insert_edge(&mut batch, 1, 1, 2);
    insert_edge(&mut batch, 2, 2, 3);
    insert_edge(&mut batch, 3, 3, 4);
    database.commit_batch(batch).await.unwrap();
    let _initial = expect_recv_vals(&subscription);

    let mut batch = database.open_batch();
    batch.insert(
        "blockers",
        vec![Value::U64(1), Value::U64(1), Value::U64(2)],
    );
    database.commit_batch(batch).await.unwrap();
    let mut values = expect_recv_vals(&subscription);
    sort_pairs_by_value(&mut values);

    assert_eq!(
        values,
        [
            (vec![Value::U64(1), Value::U64(2)], -1),
            (vec![Value::U64(1), Value::U64(3)], -1),
            (vec![Value::U64(1), Value::U64(4)], -1),
        ]
    );
}

#[futures_test::test]
async fn recursive_graph_subscriptions_collapse_duplicate_derivations() {
    let storage = MemoryStorage::new(&["edges"]).expect("valid memory storage families");
    let mut database = Database::new(edges_schema(), storage).await.unwrap();
    let subscription_id = database
        .subscribe_one_sink(reachability_graph(16))
        .await
        .unwrap();

    let mut batch = database.open_batch();
    insert_edge(&mut batch, 1, 1, 2);
    insert_edge(&mut batch, 2, 1, 3);
    insert_edge(&mut batch, 3, 2, 4);
    insert_edge(&mut batch, 4, 3, 4);
    database.commit_batch(batch).await.unwrap();
    let values = expect_recv_vals(&subscription_id);

    assert!(values.contains(&(vec![Value::U64(1), Value::U64(4)], 1)));
}

#[futures_test::test]
async fn recursive_graph_subscriptions_recompute_after_edge_update() {
    let storage = MemoryStorage::new(&["edges"]).expect("valid memory storage families");
    let mut database = Database::new(edges_schema(), storage).await.unwrap();
    let subscription_id = database
        .subscribe_one_sink(reachability_graph(16))
        .await
        .unwrap();

    let mut batch = database.open_batch();
    insert_edge(&mut batch, 1, 1, 2);
    insert_edge(&mut batch, 2, 2, 3);
    database.commit_batch(batch).await.unwrap();
    let _initial_reach = expect_recv_vals(&subscription_id);

    let mut batch = database.open_batch();
    update_edge(&mut batch, 2, 2, 4);
    database.commit_batch(batch).await.unwrap();
    let mut values = expect_recv_vals(&subscription_id);
    sort_pairs_by_value(&mut values);

    assert_eq!(
        values,
        [
            (vec![Value::U64(1), Value::U64(3)], -1),
            (vec![Value::U64(1), Value::U64(4)], 1),
            (vec![Value::U64(2), Value::U64(3)], -1),
            (vec![Value::U64(2), Value::U64(4)], 1),
        ]
    );
}

#[futures_test::test]
async fn recursive_graph_subscriptions_incrementally_extend_existing_reach_with_new_edge() {
    let storage = MemoryStorage::new(&["edges"]).expect("valid memory storage families");
    let mut database = Database::new(edges_schema(), storage).await.unwrap();
    let subscription_id = database
        .subscribe_one_sink(reachability_graph(16))
        .await
        .unwrap();

    let mut batch = database.open_batch();
    insert_edge(&mut batch, 1, 1, 2);
    database.commit_batch(batch).await.unwrap();
    assert_eq!(
        database
            .last_commit_metrics()
            .unwrap()
            .tick
            .recursive_recomputes,
        1
    );
    let _initial_reach = expect_recv_vals(&subscription_id);

    let mut batch = database.open_batch();
    insert_edge(&mut batch, 2, 2, 3);
    database.commit_batch(batch).await.unwrap();
    assert_eq!(
        database
            .last_commit_metrics()
            .unwrap()
            .tick
            .recursive_recomputes,
        0
    );
    let mut values = expect_recv_vals(&subscription_id);
    sort_pairs_by_value(&mut values);

    assert_eq!(
        values,
        [
            (vec![Value::U64(1), Value::U64(3)], 1),
            (vec![Value::U64(2), Value::U64(3)], 1),
        ]
    );
}

#[futures_test::test]
async fn recursive_graph_subscriptions_incrementally_extend_new_seed_with_existing_edge() {
    let storage = MemoryStorage::new(&["edges"]).expect("valid memory storage families");
    let mut database = Database::new(edges_schema(), storage).await.unwrap();
    let subscription_id = database
        .subscribe_one_sink(reachability_graph(16))
        .await
        .unwrap();

    let mut batch = database.open_batch();
    insert_edge(&mut batch, 1, 2, 3);
    database.commit_batch(batch).await.unwrap();
    let _initial_reach = expect_recv_vals(&subscription_id);

    let mut batch = database.open_batch();
    insert_edge(&mut batch, 2, 1, 2);
    database.commit_batch(batch).await.unwrap();
    let mut values = expect_recv_vals(&subscription_id);
    sort_pairs_by_value(&mut values);

    assert_eq!(
        values,
        [
            (vec![Value::U64(1), Value::U64(2)], 1),
            (vec![Value::U64(1), Value::U64(3)], 1),
        ]
    );
}

#[futures_test::test]
async fn recursive_graph_subscriptions_converge_on_self_cycles() {
    let storage = MemoryStorage::new(&["edges"]).expect("valid memory storage families");
    let mut database = Database::new(edges_schema(), storage).await.unwrap();
    let subscription = database
        .subscribe_one_sink(reachability_graph(2))
        .await
        .unwrap();
    let _initial = subscription.recv().unwrap();

    let mut batch = database.open_batch();
    insert_edge(&mut batch, 1, 1, 1);
    database.commit_batch(batch).await.unwrap();
    let values = subscription.recv().unwrap().to_values().unwrap();

    assert_eq!(values, [(vec![Value::U64(1), Value::U64(1)], 1)]);
}

/// The direct async Database API is itself the runtime owner. A resident
/// recursive snapshot may use several cooperative IVM turns, but callers must
/// not need to provide an external wake loop before they can consume it.
#[futures_test::test]
async fn direct_recursive_subscription_open_drives_resident_snapshot() {
    let storage = MemoryStorage::new(&["edges"]).expect("valid memory storage families");
    let mut database = Database::new(edges_schema(), storage).await.unwrap();
    let mut batch = database.open_batch();
    insert_edge(&mut batch, 1, 1, 2);
    database.commit_batch(batch).await.unwrap();

    let subscription = database
        .subscribe_one_sink(reachability_graph(16))
        .await
        .expect("direct subscription opening drives the recursive snapshot");
    assert_eq!(
        subscription.recv().unwrap().to_values().unwrap(),
        [(vec![Value::U64(1), Value::U64(2)], 1)]
    );
}

#[futures_test::test]
async fn recursive_graphs_reject_seed_and_step_output_descriptor_mismatch() {
    let storage = MemoryStorage::new(&["edges"]).expect("valid memory storage families");
    let mut database = Database::new(edges_schema(), storage).await.unwrap();
    let frontier = GraphBuilder::frontier_source(
        "frontier",
        RecordDescriptor::new([
            ("src", ColumnType::U64.clone()),
            ("dst", ColumnType::U64.clone()),
        ]),
    );
    let step = frontier.project(["src"]);
    let graph = GraphBuilder::recursive(
        GraphBuilder::table("edges").project(["src", "dst"]),
        step,
        "frontier",
        16,
    );

    assert!(matches!(
        database.subscribe_one_sink(graph).await.unwrap_err(),
        Error::IvmRuntime(IvmRuntimeError::GraphOutputMismatch)
    ));
}

#[futures_test::test]
async fn recursive_graphs_reject_nested_recursion_for_v0() {
    let storage = MemoryStorage::new(&["edges"]).expect("valid memory storage families");
    let mut database = Database::new(edges_schema(), storage).await.unwrap();
    let reach = RecordDescriptor::new([
        ("src", ColumnType::U64.clone()),
        ("dst", ColumnType::U64.clone()),
    ]);
    let graph = GraphBuilder::recursive(
        reachability_graph(16),
        GraphBuilder::frontier_source("outer-frontier", reach),
        "outer-frontier",
        4,
    );

    assert!(matches!(
        database.subscribe_one_sink(graph).await.unwrap_err(),
        Error::IvmRuntime(IvmRuntimeError::UnsupportedNestedRecursion)
    ));
}

#[futures_test::test]
async fn recursive_graphs_fail_when_frontier_exceeds_max_iters() {
    let storage = MemoryStorage::new(&["edges"]).expect("valid memory storage families");
    let mut database = Database::new(edges_schema(), storage).await.unwrap();

    let mut batch = database.open_batch();
    insert_edge(&mut batch, 1, 1, 2);
    insert_edge(&mut batch, 2, 2, 3);
    insert_edge(&mut batch, 3, 3, 4);
    database.commit_batch(batch).await.unwrap();

    assert!(matches!(
        database
            .query_graph(reachability_graph(1))
            .await
            .unwrap_err(),
        Error::IvmRuntime(IvmRuntimeError::RecursiveIterationLimit { max_iters: 1, .. })
    ));
}
