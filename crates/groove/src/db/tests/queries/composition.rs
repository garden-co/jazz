//! Composition of query operators with joins and prepared bindings.

use super::*;

#[futures_test::test]
async fn repeated_source_ticks_include_new_consumers_and_survive_subscription_churn() {
    let storage = MemoryStorage::new(&["history", "rows", "blockers"]).unwrap();
    let mut db = Database::new(history_schema(), storage).await.unwrap();
    let graph =
        GraphBuilder::arg_max_by(GraphBuilder::table("history"), ["row"], ["stamp", "node"])
            .project(["row", "stamp"]);
    let primary = db.subscribe_one_sink(graph.clone()).await.unwrap();
    assert!(primary.recv().unwrap().to_values().unwrap().is_empty());
    let mut previous = None;
    for stamp in [10, 20, 30] {
        // The second consumer is attached after earlier ticks warmed activation
        // planning for this exact table source. It must not miss later writes.
        let extra = db
            .subscribe_one_sink(
                graph
                    .clone()
                    .filter(PredicateExpr::gt("stamp", Value::U64(0))),
            )
            .await
            .unwrap();
        let row = |stamp| vec![Value::U64(1), Value::U64(stamp)];
        assert_eq!(
            extra.recv().unwrap().to_values().unwrap(),
            previous
                .map(|stamp| vec![(row(stamp), 1)])
                .unwrap_or_default()
        );
        let mut batch = db.open_batch();
        batch.insert("history", history_values(1, stamp, 1, "value"));
        db.commit_batch(batch).await.unwrap();
        for changes in [primary.recv().unwrap(), extra.recv().unwrap()] {
            let changes = changes.to_values().unwrap();
            assert_eq!(changes.len(), if previous.is_some() { 2 } else { 1 });
            assert!(changes.contains(&(row(stamp), 1)));
            if let Some(old) = previous {
                assert!(changes.contains(&(row(old), -1)));
            }
        }
        assert!(db.unsubscribe(extra.id()));
        assert_eq!(
            db.query_graph(graph.clone())
                .await
                .unwrap()
                .to_values()
                .unwrap(),
            [(row(stamp), 1)]
        );
        previous = Some(stamp);
    }
}

/// Exact public results cover shared producer state; the test-only allocation
/// counter additionally proves that resident stateful kernels do not silently
/// fall back to an async interpreter (result equality cannot prove this).
#[futures_test::test]
async fn resident_stateful_batches_share_inputs_without_async_node_frames() {
    let storage = MemoryStorage::new(&["history", "rows", "blockers"]).unwrap();
    let mut db = Database::new(history_schema(), storage).await.unwrap();
    let winners =
        GraphBuilder::arg_max_by(GraphBuilder::table("history"), ["row"], ["stamp", "node"]);
    let joined = GraphBuilder::join(winners.clone(), winners, ["row"], ["row"]).project_fields([
        ProjectField::renamed("left.row", "row"),
        ProjectField::renamed("right.stamp", "stamp"),
    ]);
    let graph = GraphBuilder::aggregate(
        GraphBuilder::union([joined.clone(), joined]),
        ["row", "stamp"],
        [AggregateExpr {
            function: AggregateFunction::Count,
            expression: None,
            distinct: false,
            output_name: Some("copies".to_owned()),
            output_identity: None,
        }],
    );
    let mut batch = db.open_batch();
    batch.insert("history", history_values(1, 10, 1, "baseline"));
    db.commit_batch(batch).await.unwrap();
    let subscription = db.subscribe_one_sink(graph.clone()).await.unwrap();
    let row = |stamp| vec![Value::U64(1), Value::U64(stamp), Value::U64(2)];
    assert_eq!(
        subscription.recv().unwrap().to_values().unwrap(),
        [(row(10), 1)]
    );
    crate::ivm::runtime::take_async_node_frame_count();
    for stamp in [20, 30, 40] {
        let mut batch = db.open_batch();
        batch.insert("history", history_values(1, stamp, 1, "replacement"));
        db.commit_batch(batch).await.unwrap();
        let changes = subscription.recv().unwrap().to_values().unwrap();
        assert_eq!(changes.len(), 2);
        assert!(changes.contains(&(row(10), -1)));
        assert!(changes.contains(&(row(stamp), 1)));
        assert_eq!(
            db.query_graph(graph.clone())
                .await
                .unwrap()
                .to_values()
                .unwrap(),
            [(row(stamp), 1)]
        );
        let mut batch = db.open_batch();
        batch.delete("history", history_key(1, stamp, 1));
        db.commit_batch(batch).await.unwrap();
        let changes = subscription.recv().unwrap().to_values().unwrap();
        assert_eq!(changes.len(), 2);
        assert!(changes.contains(&(row(stamp), -1)));
        assert!(changes.contains(&(row(10), 1)));
        assert_eq!(crate::ivm::runtime::take_async_node_frame_count(), 0);
    }
}

/// Cached structural requirements must still check live producer state behind
/// a deep stateless suffix, across mutation, one-shot probes and detachment.
#[futures_test::test]
async fn deep_shared_readiness_frontiers_recheck_winners_after_detach() {
    let storage = MemoryStorage::new(&["history", "rows", "blockers"]).unwrap();
    let mut db = Database::new(history_schema(), storage).await.unwrap();
    let mut graph =
        GraphBuilder::arg_max_by(GraphBuilder::table("history"), ["row"], ["stamp", "node"])
            .project(["row", "stamp"]);
    for _ in 0..48 {
        graph = graph.filter(PredicateExpr::gt("stamp", Value::U64(0)));
    }
    let params = RecordDescriptor::new([("row", ColumnType::U64)]);
    let prepared = db
        .prepare_one_sink(
            GraphBuilder::join(
                GraphBuilder::binding_source("deep_row", params),
                graph.clone(),
                ["row"],
                ["row"],
            )
            .project_fields([
                ProjectField::renamed("left.row", "row"),
                ProjectField::renamed("right.stamp", "stamp"),
            ]),
            "deep_row",
            params,
            ["row"],
        )
        .await
        .unwrap();
    let mut batch = db.open_batch();
    batch.insert("history", history_values(1, 10, 1, "first baseline"));
    batch.insert("history", history_values(2, 15, 1, "second baseline"));
    db.commit_batch(batch).await.unwrap();
    let row = |id, stamp| vec![Value::U64(id), Value::U64(stamp)];
    let other = db
        .bind_shape_one_sink(prepared.id(), &[Value::U64(2)])
        .await
        .unwrap();
    assert_eq!(
        other.recv().unwrap().to_values().unwrap(),
        [(row(2, 15), 1)]
    );
    for stamp in [20, 30, 40] {
        let mut batch = db.open_batch();
        batch.insert("history", history_values(1, stamp, 1, "new winner"));
        db.commit_batch(batch).await.unwrap();
        let snapshot = db
            .query_graph(graph.clone())
            .await
            .unwrap()
            .to_values()
            .unwrap();
        assert_eq!(snapshot.len(), 2);
        assert!(snapshot.contains(&(row(1, stamp), 1)));
        assert!(snapshot.contains(&(row(2, 15), 1)));
        let subscription = db
            .bind_shape_one_sink(prepared.id(), &[Value::U64(1)])
            .await
            .unwrap();
        assert_eq!(
            subscription.recv().unwrap().to_values().unwrap(),
            [(row(1, stamp), 1)]
        );
        let mut batch = db.open_batch();
        batch.delete("history", history_key(1, stamp, 1));
        db.commit_batch(batch).await.unwrap();
        let changes = subscription.recv().unwrap().to_values().unwrap();
        assert_eq!(changes.len(), 2);
        assert!(changes.contains(&(row(1, stamp), -1)));
        assert!(changes.contains(&(row(1, 10), 1)));
        assert!(matches!(other.try_recv(), Err(TryRecvError::Empty)));
        assert!(db.unsubscribe(subscription.id()));
    }
}

/// Alice takes first results before and alongside Bob's retained extrema
/// subscription, using the exact same graph and prepared parameter domain.
/// probe -> bind -> probe other binding -> delete winner -> probe -> delete runner-up.
/// Alice's private hydration must neither seed nor overwrite Bob's live state.
#[futures_test::test]
async fn first_result_lifetime_preserves_shared_retained_winners_and_bindings() {
    use crate::db::SubscriptionLifetime;

    for maximum in [false, true] {
        let storage = MemoryStorage::new(&["history", "rows", "blockers"]).unwrap();
        let mut db = Database::new(history_schema(), storage).await.unwrap();
        let mut batch = db.open_batch();
        for (row, stamp) in [(1, 10), (1, 20), (2, 30)] {
            batch.insert("history", history_values(row, stamp, 1, "candidate"));
        }
        db.commit_batch(batch).await.unwrap();
        let graph = if maximum {
            GraphBuilder::arg_max_by(GraphBuilder::table("history"), ["row"], ["stamp", "node"])
        } else {
            GraphBuilder::arg_min_by(GraphBuilder::table("history"), ["row"], ["stamp", "node"])
        }
        .project(["row", "stamp"]);
        let params = RecordDescriptor::new([("row", ColumnType::U64)]);
        let prepared = db
            .prepare_one_sink(
                GraphBuilder::join(
                    GraphBuilder::binding_source("first_result_row", params),
                    graph.clone(),
                    ["row"],
                    ["row"],
                )
                .project_fields([
                    ProjectField::renamed("left.row", "row"),
                    ProjectField::renamed("right.stamp", "stamp"),
                ]),
                "first_result_row",
                params,
                ["row"],
            )
            .await
            .unwrap();
        let (winner, next) = if maximum { (20, 10) } else { (10, 20) };
        let values = |row, stamp| vec![Value::U64(row), Value::U64(stamp)];
        let alice = db
            .bind_shape_with_lifetime(
                prepared.id(),
                &[Value::U64(1)],
                SubscriptionLifetime::FirstResult,
                None,
            )
            .await
            .unwrap();
        let snapshot = db
            .next_multisink_subscription_with_publication(&alice)
            .await
            .unwrap()
            .deltas;
        assert_eq!(
            snapshot.sinks.values().next().unwrap().to_values().unwrap(),
            [(values(1, winner), 1)]
        );
        assert!(matches!(alice.try_recv(), Err(TryRecvError::Disconnected)));
        let bob = db
            .bind_shape_one_sink(prepared.id(), &[Value::U64(1)])
            .await
            .unwrap();
        assert_eq!(
            bob.recv().unwrap().to_values().unwrap(),
            [(values(1, winner), 1)]
        );

        // A different binding must not change the installed binding source.
        let alice = db
            .bind_shape_with_lifetime(
                prepared.id(),
                &[Value::U64(2)],
                SubscriptionLifetime::FirstResult,
                None,
            )
            .await
            .unwrap();
        let snapshot = db
            .next_multisink_subscription_with_publication(&alice)
            .await
            .unwrap()
            .deltas;
        assert_eq!(
            snapshot.sinks.values().next().unwrap().to_values().unwrap(),
            [(values(2, 30), 1)]
        );
        assert!(matches!(bob.try_recv(), Err(TryRecvError::Empty)));
        let mut batch = db.open_batch();
        batch.delete("history", history_key(1, winner, 1));
        db.commit_batch(batch).await.unwrap();
        let deltas = bob.recv().unwrap().to_values().unwrap();
        assert_eq!(deltas.len(), 2);
        assert!(deltas.contains(&(values(1, winner), -1)));
        assert!(deltas.contains(&(values(1, next), 1)));

        // Exercise the unparameterized shared installer too, after mutation.
        let alice = db
            .subscribe_with_lifetime([("rows", graph)], SubscriptionLifetime::FirstResult, None)
            .unwrap();
        let snapshot = db
            .next_multisink_subscription_with_publication(&alice)
            .await
            .unwrap()
            .deltas;
        let rows = snapshot.sinks["rows"].to_values().unwrap();
        assert_eq!(rows.len(), 2);
        assert!(rows.contains(&(values(1, next), 1)));
        assert!(rows.contains(&(values(2, 30), 1)));
        let mut batch = db.open_batch();
        batch.delete("history", history_key(1, next, 1));
        db.commit_batch(batch).await.unwrap();
        assert_eq!(
            bob.recv().unwrap().to_values().unwrap(),
            [(values(1, next), -1)]
        );
        assert!(db.unsubscribe(bob.id()));
        let empty = db
            .bind_shape_with_lifetime(
                prepared.id(),
                &[Value::U64(3)],
                SubscriptionLifetime::FirstResult,
                None,
            )
            .await
            .unwrap();
        let snapshot = db
            .next_multisink_subscription_with_publication(&empty)
            .await
            .unwrap()
            .deltas;
        assert_eq!(snapshot.sinks.len(), 1);
        assert!(snapshot.is_empty(), "empty initial results still complete");
        assert!(matches!(empty.try_recv(), Err(TryRecvError::Disconnected)));
        db.retire_prepared_shape(prepared.id()).unwrap();
    }
}

/// Reusing lookup inputs inside an evaluation must not retain a binding's
/// context or watermark across later writes and fresh subscriptions.
#[futures_test::test]
async fn prepared_memo_lookup_keeps_bindings_and_write_frontiers_distinct() {
    let storage = MemoryStorage::new(&["history", "rows", "blockers"]).unwrap();
    let mut db = Database::new(history_schema(), storage).await.unwrap();
    let params = RecordDescriptor::new([("row", ColumnType::U64)]);
    let graph = GraphBuilder::join(
        GraphBuilder::binding_source("selected_row", params),
        GraphBuilder::arg_max_by(GraphBuilder::table("history"), ["row"], ["stamp", "node"]),
        ["row"],
        ["row"],
    )
    .project_fields([
        ProjectField::renamed("left.row", "row"),
        ProjectField::renamed("right.stamp", "stamp"),
    ]);
    let prepared = db
        .prepare_one_sink(graph, "selected_row", params, ["row"])
        .await
        .unwrap();
    let mut batch = db.open_batch();
    batch.insert("history", history_values(1, 10, 1, "first"));
    batch.insert("history", history_values(2, 20, 1, "second"));
    db.commit_batch(batch).await.unwrap();
    let first = db
        .bind_shape_one_sink(prepared.id(), &[Value::U64(1)])
        .await
        .unwrap();
    let second = db
        .bind_shape_one_sink(prepared.id(), &[Value::U64(2)])
        .await
        .unwrap();
    let values = |row, stamp| vec![Value::U64(row), Value::U64(stamp)];
    assert_eq!(
        first.recv().unwrap().to_values().unwrap(),
        [(values(1, 10), 1)]
    );
    assert_eq!(
        second.recv().unwrap().to_values().unwrap(),
        [(values(2, 20), 1)]
    );
    for (row, old, new, changed, unchanged) in [
        (1, 10, 30, &first, &second),
        (2, 20, 40, &second, &first),
        (1, 30, 50, &first, &second),
    ] {
        let mut batch = db.open_batch();
        batch.delete("history", history_key(row, old, 1));
        batch.insert("history", history_values(row, new, 1, "replacement"));
        db.commit_batch(batch).await.unwrap();
        let deltas = changed.recv().unwrap().to_values().unwrap();
        assert_eq!(deltas.len(), 2);
        assert!(deltas.contains(&(values(row, old), -1)));
        assert!(deltas.contains(&(values(row, new), 1)));
        assert!(matches!(unchanged.try_recv(), Err(TryRecvError::Empty)));
        let fresh = db
            .bind_shape_one_sink(prepared.id(), &[Value::U64(row)])
            .await
            .unwrap();
        assert_eq!(
            fresh.recv().unwrap().to_values().unwrap(),
            [(values(row, new), 1)]
        );
        assert!(db.unsubscribe(fresh.id()));
    }
}

/// Keeping a prepared graph (and its immutable dependency classification)
/// across detachment must not keep a stale runtime-readiness proof.
#[futures_test::test]
async fn prepared_hydration_after_detach_rebuilds_current_candidates() {
    let storage = MemoryStorage::new(&["history", "rows", "blockers"]).unwrap();
    let mut db = Database::new(history_schema(), storage).await.unwrap();
    let graph =
        GraphBuilder::arg_max_by(GraphBuilder::table("history"), ["row"], ["stamp", "node"])
            .project(["row", "stamp"]);
    let params = RecordDescriptor::new([("row", ColumnType::U64)]);
    let prepared = db
        .prepare_one_sink(
            GraphBuilder::join(
                GraphBuilder::binding_source("selected_row", params),
                graph.clone(),
                ["row"],
                ["row"],
            )
            .project_fields([
                ProjectField::renamed("left.row", "row"),
                ProjectField::renamed("right.stamp", "stamp"),
            ]),
            "selected_row",
            params,
            ["row"],
        )
        .await
        .unwrap();
    let mut batch = db.open_batch();
    batch.insert("history", history_values(1, 10, 1, "runner-up"));
    db.commit_batch(batch).await.unwrap();
    let values = |stamp| vec![Value::U64(1), Value::U64(stamp)];
    for winner in [20, 30] {
        let mut batch = db.open_batch();
        batch.insert("history", history_values(1, winner, 1, "winner"));
        db.commit_batch(batch).await.unwrap();
        assert_eq!(
            db.query_graph(graph.clone())
                .await
                .unwrap()
                .to_values()
                .unwrap(),
            [(values(winner), 1)]
        );
        let subscription = db
            .bind_shape_one_sink(prepared.id(), &[Value::U64(1)])
            .await
            .unwrap();
        assert_eq!(
            subscription.recv().unwrap().to_values().unwrap(),
            [(values(winner), 1)]
        );
        let mut batch = db.open_batch();
        batch.delete("history", history_key(1, winner, 1));
        db.commit_batch(batch).await.unwrap();
        let changes = subscription.recv().unwrap().to_values().unwrap();
        assert_eq!(changes.len(), 2);
        assert!(changes.contains(&(values(winner), -1)));
        assert!(changes.contains(&(values(10), 1)));
        assert!(db.unsubscribe(subscription.id()));
    }
}

/// Alice probes a prepared-but-unbound graph before Bob subscribes. A cached
/// winner is not evidence of a seeded candidate index: deleting that winner
/// must expose its runner-up. Further probes must not erase Bob's live state.
/// prepare -> one-shot -> bind -> delete winner -> one-shot -> delete runner-up.
#[futures_test::test]
async fn arg_by_probe_memo_seeds_later_subscription_and_preserves_live_candidates() {
    for maximum in [false, true] {
        let storage = MemoryStorage::new(&["history", "rows", "blockers"]).unwrap();
        let mut db = Database::new(history_schema(), storage).await.unwrap();
        let mut batch = db.open_batch();
        batch.insert("history", history_values(1, 10, 1, "older"));
        batch.insert("history", history_values(1, 20, 1, "newer"));
        db.commit_batch(batch).await.unwrap();
        let input = GraphBuilder::table("history");
        let graph = if maximum {
            GraphBuilder::arg_max_by(input, ["row"], ["stamp", "node"])
        } else {
            GraphBuilder::arg_min_by(input, ["row"], ["stamp", "node"])
        }
        .project(["row", "stamp"]);
        let params = RecordDescriptor::new([("row", ColumnType::U64)]);
        let prepared = db
            .prepare_one_sink(
                GraphBuilder::join(
                    GraphBuilder::binding_source("probe_row", params),
                    graph.clone(),
                    ["row"],
                    ["row"],
                )
                .project_fields([
                    ProjectField::renamed("left.row", "row"),
                    ProjectField::renamed("right.stamp", "stamp"),
                ]),
                "probe_row",
                params,
                ["row"],
            )
            .await
            .unwrap();
        let (winner, next) = if maximum { (20, 10) } else { (10, 20) };
        let values = |stamp| vec![Value::U64(1), Value::U64(stamp)];
        assert_eq!(
            db.query_graph(graph.clone())
                .await
                .unwrap()
                .to_values()
                .unwrap(),
            [(values(winner), 1)]
        );
        let sub = db
            .bind_shape_one_sink(prepared.id(), &[Value::U64(1)])
            .await
            .unwrap();
        assert_eq!(
            sub.try_recv().unwrap().to_values().unwrap(),
            [(values(winner), 1)]
        );
        let mut batch = db.open_batch();
        batch.delete("history", history_key(1, winner, 1));
        db.commit_batch(batch).await.unwrap();
        let deltas = sub.try_recv().unwrap().to_values().unwrap();
        assert_eq!(deltas.len(), 2);
        assert!(deltas.contains(&(values(winner), -1)));
        assert!(deltas.contains(&(values(next), 1)));
        assert_eq!(
            db.query_graph(graph).await.unwrap().to_values().unwrap(),
            [(values(next), 1)]
        );
        let mut batch = db.open_batch();
        batch.delete("history", history_key(1, next, 1));
        db.commit_batch(batch).await.unwrap();
        assert_eq!(
            sub.try_recv().unwrap().to_values().unwrap(),
            [(values(next), -1)]
        );
    }
}

/// Alice and Bob share an extrema query. Attaching Bob and issuing one-shot
/// reads must not double-count Alice's retained inputs. Identical rows from
/// two sources contribute twice, but the winner is always emitted once.
/// seed -> attach twice -> retract duplicates -> empty -> attach -> refill.
#[futures_test::test]
async fn arg_by_shared_hydration_preserves_multiplicity_and_empty_refill() {
    for maximum in [false, true] {
        let storage = MemoryStorage::new(&["history", "history_shadow"]).unwrap();
        let mut database = Database::new(two_history_tables_schema(), storage)
            .await
            .unwrap();
        let input = GraphBuilder::union([
            GraphBuilder::table("history"),
            GraphBuilder::table("history_shadow"),
        ]);
        let graph = if maximum {
            GraphBuilder::arg_max_by(input, ["row"], ["stamp", "node"])
        } else {
            GraphBuilder::arg_min_by(input, ["row"], ["stamp", "node"])
        };
        let older = history_values(1, 10, 1, "older");
        let newer = history_values(1, 20, 1, "newer");
        let initial = if maximum {
            newer.clone()
        } else {
            older.clone()
        };
        let mut batch = database.open_batch();
        batch.insert("history", older.clone());
        batch.insert("history", newer.clone());
        batch.insert("history_shadow", newer);
        database.commit_batch(batch).await.unwrap();
        let alice = database.subscribe_one_sink(graph.clone()).await.unwrap();
        assert_eq!(
            alice.recv().unwrap().to_values().unwrap(),
            [(initial.clone(), 1)]
        );
        let bob = database.subscribe_one_sink(graph.clone()).await.unwrap();
        assert_eq!(
            bob.recv().unwrap().to_values().unwrap(),
            [(initial.clone(), 1)]
        );
        assert_eq!(
            database
                .query_graph(graph.clone())
                .await
                .unwrap()
                .to_values()
                .unwrap(),
            [(initial.clone(), 1)]
        );

        let mut batch = database.open_batch();
        batch.delete("history", history_key(1, 20, 1));
        database.commit_batch(batch).await.unwrap();
        for sub in [&alice, &bob] {
            assert!(matches!(sub.try_recv(), Err(TryRecvError::Empty)));
        }
        let mut batch = database.open_batch();
        batch.delete("history_shadow", history_key(1, 20, 1));
        database.commit_batch(batch).await.unwrap();
        for sub in [&alice, &bob] {
            if maximum {
                assert_eq!(
                    sub.recv().unwrap().to_values().unwrap(),
                    [(initial.clone(), -1), (older.clone(), 1)]
                );
            } else {
                assert!(matches!(sub.try_recv(), Err(TryRecvError::Empty)));
            }
        }
        let mut batch = database.open_batch();
        batch.delete("history", history_key(1, 10, 1));
        database.commit_batch(batch).await.unwrap();
        for sub in [&alice, &bob] {
            assert_eq!(
                sub.recv().unwrap().to_values().unwrap(),
                [(older.clone(), -1)]
            );
        }
        let carol = database.subscribe_one_sink(graph.clone()).await.unwrap();
        assert!(carol.recv().unwrap().is_empty());
        assert!(database.query_graph(graph).await.unwrap().is_empty());
        let refill = history_values(1, 30, 1, "refill");
        let mut batch = database.open_batch();
        batch.insert("history", refill.clone());
        database.commit_batch(batch).await.unwrap();
        for sub in [&alice, &bob, &carol] {
            assert_eq!(
                sub.recv().unwrap().to_values().unwrap(),
                [(refill.clone(), 1)]
            );
        }
    }
}

#[futures_test::test]
async fn arg_max_by_feeds_join_and_anti_join() {
    let storage = MemoryStorage::new(&["history", "rows", "blockers"])
        .expect("valid memory storage families");
    let mut database = Database::new(history_schema(), storage).await.unwrap();

    let visible = database
        .subscribe_one_sink(GraphBuilder::anti_join(
            history_arg_max().project(["row", "stamp"]),
            GraphBuilder::table("blockers"),
            ["row"],
            ["row"],
        ))
        .await
        .unwrap();
    assert!(visible.recv().unwrap().is_empty());

    let mut batch = database.open_batch();
    batch.insert("rows", vec![Value::U64(1), Value::String("one".to_owned())]);
    batch.insert("history", history_values(1, 10, 1, "a"));
    database.commit_batch(batch).await.unwrap();
    assert_eq!(
        database
            .query_graph(
                GraphBuilder::join(
                    history_arg_max().project(["row", "stamp"]),
                    GraphBuilder::table("rows"),
                    ["row"],
                    ["row"],
                )
                .project_fields([
                    ProjectField::renamed("left.row", "row"),
                    ProjectField::renamed("left.stamp", "stamp"),
                    ProjectField::renamed("right.label", "label"),
                ]),
            )
            .await
            .unwrap()
            .to_values()
            .unwrap(),
        [(
            vec![
                Value::U64(1),
                Value::U64(10),
                Value::String("one".to_owned())
            ],
            1
        )]
    );
    assert_eq!(
        visible.recv().unwrap().to_values().unwrap(),
        [(vec![Value::U64(1), Value::U64(10)], 1)]
    );

    let mut batch = database.open_batch();
    batch.insert("blockers", vec![Value::U64(1)]);
    database.commit_batch(batch).await.unwrap();
    assert_eq!(
        visible.recv().unwrap().to_values().unwrap(),
        [(vec![Value::U64(1), Value::U64(10)], -1)]
    );
}

#[futures_test::test]
async fn arg_max_by_routes_through_prepared_bindings() {
    let storage = MemoryStorage::new(&["history", "rows", "blockers"])
        .expect("valid memory storage families");
    let mut database = Database::new(history_schema(), storage).await.unwrap();
    let params = RecordDescriptor::new([("row", ColumnType::U64.clone())]);
    let shape = database
        .prepare_one_sink(
            GraphBuilder::join(
                GraphBuilder::binding_source("row_param", params),
                history_arg_max().project(["row", "stamp"]),
                ["row"],
                ["row"],
            )
            .project_fields([
                ProjectField::renamed("left.row", "row"),
                ProjectField::renamed("right.stamp", "stamp"),
            ]),
            "row_param",
            params,
            ["row"],
        )
        .await
        .unwrap();
    let sub = database
        .bind_shape_one_sink(shape.id(), &[Value::U64(1)])
        .await
        .unwrap();
    assert!(sub.recv().unwrap().is_empty());

    let mut batch = database.open_batch();
    batch.insert("history", history_values(1, 10, 1, "a"));
    batch.insert("history", history_values(2, 99, 1, "ignored"));
    database.commit_batch(batch).await.unwrap();
    assert_eq!(
        sub.recv().unwrap().to_values().unwrap(),
        [(vec![Value::U64(1), Value::U64(10)], 1)]
    );
}

#[futures_test::test]
async fn arg_max_by_matches_naive_oracle_across_seeded_mutations() {
    #[derive(Clone)]
    struct Lcg(u64);
    impl Lcg {
        fn next(&mut self) -> u64 {
            self.0 = self
                .0
                .wrapping_mul(6364136223846793005)
                .wrapping_add(1442695040888963407);
            self.0
        }
        fn range(&mut self, max: u64) -> u64 {
            self.next() % max
        }
    }
    let storage = MemoryStorage::new(&["history", "rows", "blockers"])
        .expect("valid memory storage families");
    let mut database = Database::new(history_schema(), storage).await.unwrap();
    let mut rng = Lcg(0x0bad_cafe_1234_5678);
    let mut model = std::collections::BTreeMap::<(u64, u64, u64), String>::new();

    for _ in 0..160 {
        let mut batch = database.open_batch();
        for _ in 0..(1 + rng.range(4)) {
            let row = 1 + rng.range(8);
            let stamp = 1 + rng.range(32);
            let node = 1 + rng.range(4);
            let key = (row, stamp, node);
            if rng.range(5) == 0 {
                batch.delete("history", history_key(row, stamp, node));
                model.remove(&key);
            } else {
                let title = format!("v-{row}-{stamp}-{node}");
                if model.contains_key(&key) {
                    batch.update("history", history_values(row, stamp, node, &title));
                } else {
                    batch.insert("history", history_values(row, stamp, node, &title));
                }
                model.insert(key, title);
            }
        }
        database.commit_batch(batch).await.unwrap();

        let mut expected = std::collections::BTreeMap::<u64, (u64, u64, String)>::new();
        for (&(row, stamp, node), title) in &model {
            let entry = expected
                .entry(row)
                .or_insert_with(|| (stamp, node, title.clone()));
            if (stamp, node) > (entry.0, entry.1) {
                *entry = (stamp, node, title.clone());
            }
        }
        let mut expected = expected
            .into_iter()
            .map(|(row, (stamp, node, title))| (history_values(row, stamp, node, &title), 1))
            .collect::<Vec<_>>();
        expected.sort_by_key(|(values, _)| match &values[..] {
            [Value::U64(row), Value::U64(stamp), Value::U64(node), ..] => (*row, *stamp, *node),
            _ => unreachable!(),
        });

        let mut actual = database
            .query_graph(history_arg_max())
            .await
            .unwrap()
            .to_values()
            .unwrap();
        actual.sort_by_key(|(values, _)| match &values[..] {
            [Value::U64(row), Value::U64(stamp), Value::U64(node), ..] => (*row, *stamp, *node),
            _ => unreachable!(),
        });
        assert_eq!(actual, expected);
    }
}

#[futures_test::test]
async fn arg_max_by_tracks_union_of_filtered_sources() {
    let storage =
        MemoryStorage::new(&["history", "history_shadow"]).expect("valid memory storage families");
    let mut database = Database::new(two_history_tables_schema(), storage)
        .await
        .unwrap();
    let graph = GraphBuilder::arg_max_by(
        GraphBuilder::union([
            GraphBuilder::table("history").filter(PredicateExpr::gt("stamp", Value::U64(10))),
            GraphBuilder::table("history_shadow")
                .filter(PredicateExpr::gt("stamp", Value::U64(10))),
        ]),
        ["row"],
        ["stamp", "node"],
    );
    let subscription = database.subscribe_one_sink(graph.clone()).await.unwrap();
    assert!(subscription.recv().unwrap().is_empty());

    let mut batch = database.open_batch();
    batch.insert("history", history_values(1, 20, 1, "left-winner"));
    batch.insert("history_shadow", history_values(1, 30, 1, "right-winner"));
    batch.insert("history_shadow", history_values(2, 40, 1, "other"));
    database.commit_batch(batch).await.unwrap();
    assert_eq!(
        subscription.recv().unwrap().to_values().unwrap(),
        [
            (history_values(1, 30, 1, "right-winner"), 1),
            (history_values(2, 40, 1, "other"), 1),
        ]
    );

    let mut batch = database.open_batch();
    batch.delete("history_shadow", history_key(1, 30, 1));
    database.commit_batch(batch).await.unwrap();
    assert_eq!(
        subscription.recv().unwrap().to_values().unwrap(),
        [
            (history_values(1, 30, 1, "right-winner"), -1),
            (history_values(1, 20, 1, "left-winner"), 1),
        ]
    );

    let mut actual = database
        .query_graph(graph)
        .await
        .unwrap()
        .to_values()
        .unwrap();
    actual.sort_by_key(|(values, _)| match &values[..] {
        [Value::U64(row), Value::U64(stamp), Value::U64(node), ..] => (*row, *stamp, *node),
        _ => unreachable!(),
    });
    assert_eq!(
        actual,
        [
            (history_values(1, 20, 1, "left-winner"), 1),
            (history_values(2, 40, 1, "other"), 1),
        ]
    );
}

#[futures_test::test]
async fn arg_max_by_projection_reorder_preserves_tied_winner_and_retraction() {
    let storage = MemoryStorage::new(&["history", "history_shadow"]).unwrap();
    let mut database = Database::new(two_history_tables_schema(), storage)
        .await
        .unwrap();
    let source = || {
        GraphBuilder::union([
            GraphBuilder::table("history_shadow"),
            GraphBuilder::table("history"),
        ])
    };
    let declared_order_projection = database
        .subscribe_one_sink(GraphBuilder::arg_max_by(
            source().project(["row", "stamp", "node", "title"]),
            ["row"],
            ["stamp", "node"],
        ))
        .await
        .unwrap();
    let reordered_projection = database
        .subscribe_one_sink(GraphBuilder::arg_max_by(
            source().project(["row", "title", "stamp", "node"]),
            ["row"],
            ["stamp", "node"],
        ))
        .await
        .unwrap();
    assert!(declared_order_projection.recv().unwrap().is_empty());
    assert!(reordered_projection.recv().unwrap().is_empty());

    let tied_low = history_values(1, 20, 1, "tied-a");
    let tied_high = history_values(1, 20, 1, "tied-z");
    let tied_low_reordered = vec![
        Value::U64(1),
        Value::String("tied-a".to_owned()),
        Value::U64(20),
        Value::U64(1),
    ];
    let tied_high_reordered = vec![
        Value::U64(1),
        Value::String("tied-z".to_owned()),
        Value::U64(20),
        Value::U64(1),
    ];
    let mut batch = database.open_batch();
    batch.insert("history", history_values(1, 10, 9, "z-payload"));
    batch.insert("history", tied_low.clone());
    batch.insert("history_shadow", tied_high.clone());
    database.commit_batch(batch).await.unwrap();
    assert_eq!(
        declared_order_projection
            .recv()
            .unwrap()
            .to_values()
            .unwrap(),
        [(tied_low.clone(), 1)]
    );
    assert_eq!(
        reordered_projection.recv().unwrap().to_values().unwrap(),
        [(tied_low_reordered.clone(), 1)]
    );

    let mut batch = database.open_batch();
    batch.delete("history", history_key(1, 20, 1));
    database.commit_batch(batch).await.unwrap();
    assert_eq!(
        declared_order_projection
            .recv()
            .unwrap()
            .to_values()
            .unwrap(),
        [(tied_low, -1), (tied_high, 1)]
    );
    assert_eq!(
        reordered_projection.recv().unwrap().to_values().unwrap(),
        [(tied_low_reordered, -1), (tied_high_reordered, 1)]
    );
}

#[futures_test::test]
async fn arg_max_by_direct_table_and_noop_filter_publish_same_payload_replacement() {
    let storage = MemoryStorage::new(&["history", "rows", "blockers"]).unwrap();
    let mut database = Database::new(history_schema(), storage).await.unwrap();
    let direct = database
        .subscribe_one_sink(history_arg_max())
        .await
        .unwrap();
    let filtered = database
        .subscribe_one_sink(GraphBuilder::arg_max_by(
            GraphBuilder::table("history").filter(PredicateExpr::And(Vec::new())),
            ["row"],
            ["stamp", "node"],
        ))
        .await
        .unwrap();
    assert!(direct.recv().unwrap().is_empty());
    assert!(filtered.recv().unwrap().is_empty());

    let before = history_values(1, 20, 1, "before");
    let after = history_values(1, 20, 1, "after");
    let mut batch = database.open_batch();
    batch.insert("history", before.clone());
    database.commit_batch(batch).await.unwrap();
    assert_eq!(
        direct.recv().unwrap().to_values().unwrap(),
        [(before.clone(), 1)]
    );
    assert_eq!(
        filtered.recv().unwrap().to_values().unwrap(),
        [(before.clone(), 1)]
    );

    let mut batch = database.open_batch();
    batch.update("history", after.clone());
    database.commit_batch(batch).await.unwrap();
    let expected = [(before, -1), (after, 1)];
    assert_eq!(direct.recv().unwrap().to_values().unwrap(), expected);
    assert_eq!(filtered.recv().unwrap().to_values().unwrap(), expected);
}

#[futures_test::test]
async fn arg_by_snapshot_hydration_tie_breaker_is_independent_of_reversed_input_order() {
    let storage = MemoryStorage::new(&["history", "history_shadow"]).unwrap();
    let mut database = Database::new(two_history_tables_schema(), storage)
        .await
        .unwrap();
    let tied_low = history_values(1, 20, 1, "tied-a");
    let tied_high = history_values(1, 20, 1, "tied-z");
    let mut batch = database.open_batch();
    batch.insert("history_shadow", tied_high);
    batch.insert("history", tied_low);
    database.commit_batch(batch).await.unwrap();

    let input = || {
        GraphBuilder::union([
            GraphBuilder::table("history_shadow"),
            GraphBuilder::table("history"),
        ])
        .project(["row", "title", "stamp", "node"])
    };
    let expected = [(
        vec![
            Value::U64(1),
            Value::String("tied-a".to_owned()),
            Value::U64(20),
            Value::U64(1),
        ],
        1,
    )];

    let output = || {
        RecordDescriptor::new([
            ("row", ColumnType::U64.clone()),
            ("title", ColumnType::String.clone()),
            ("stamp", ColumnType::U64.clone()),
            ("node", ColumnType::U64.clone()),
        ])
    };
    let frontier = || GraphBuilder::frontier_source("frontier", output());
    let recursive = |seed| GraphBuilder::recursive(seed, frontier(), "frontier", 4);
    for graph in [
        GraphBuilder::arg_min_by(input(), ["row"], ["stamp", "node"]),
        GraphBuilder::arg_max_by(input(), ["row"], ["stamp", "node"]),
        recursive(GraphBuilder::arg_min_by(
            input(),
            ["row"],
            ["stamp", "node"],
        )),
        recursive(GraphBuilder::arg_max_by(
            input(),
            ["row"],
            ["stamp", "node"],
        )),
    ] {
        assert_eq!(
            database
                .query_graph(graph)
                .await
                .unwrap()
                .to_values()
                .unwrap(),
            expected
        );
    }
}

#[futures_test::test]
async fn arg_min_by_reordered_projection_preserves_declared_order_on_retraction() {
    let storage = MemoryStorage::new(&["history", "history_shadow"]).unwrap();
    let mut database = Database::new(two_history_tables_schema(), storage)
        .await
        .unwrap();
    let input = GraphBuilder::union([
        GraphBuilder::table("history_shadow"),
        GraphBuilder::table("history"),
    ])
    .project(["row", "title", "stamp", "node"]);
    let graph = GraphBuilder::arg_min_by(input, ["row"], ["stamp", "node"]);
    let subscription = database.subscribe_one_sink(graph).await.unwrap();
    assert!(subscription.recv().unwrap().is_empty());

    let payload_first_but_ordered_higher = history_values(1, 30, 1, "a-payload");
    let tied_low = history_values(1, 20, 1, "tied-a");
    let tied_high = history_values(1, 20, 1, "tied-z");
    let tied_low_output = vec![
        Value::U64(1),
        Value::String("tied-a".to_owned()),
        Value::U64(20),
        Value::U64(1),
    ];
    let tied_high_output = vec![
        Value::U64(1),
        Value::String("tied-z".to_owned()),
        Value::U64(20),
        Value::U64(1),
    ];
    let mut batch = database.open_batch();
    batch.insert("history_shadow", payload_first_but_ordered_higher);
    batch.insert("history", tied_low);
    batch.insert("history_shadow", tied_high);
    database.commit_batch(batch).await.unwrap();
    assert_eq!(
        subscription.recv().unwrap().to_values().unwrap(),
        [(tied_low_output.clone(), 1)]
    );

    let mut batch = database.open_batch();
    batch.delete("history", history_key(1, 20, 1));
    database.commit_batch(batch).await.unwrap();
    assert_eq!(
        subscription.recv().unwrap().to_values().unwrap(),
        [(tied_low_output, -1), (tied_high_output, 1)]
    );
}

#[futures_test::test]
async fn arg_max_by_tracks_join_filter_input() {
    let storage = MemoryStorage::new(&["history", "rows", "blockers"])
        .expect("valid memory storage families");
    let mut database = Database::new(history_schema(), storage).await.unwrap();
    let joined_history = GraphBuilder::join(
        GraphBuilder::table("history"),
        GraphBuilder::table("rows").filter(PredicateExpr::eq(
            "label",
            Value::String("visible".to_owned()),
        )),
        ["row"],
        ["row"],
    )
    .project_fields([
        ProjectField::renamed("left.row", "row"),
        ProjectField::renamed("left.stamp", "stamp"),
        ProjectField::renamed("left.node", "node"),
        ProjectField::renamed("left.title", "title"),
    ]);
    let graph = GraphBuilder::arg_max_by(joined_history, ["row"], ["stamp", "node"]);
    let subscription = database.subscribe_one_sink(graph.clone()).await.unwrap();
    assert!(subscription.recv().unwrap().is_empty());

    let mut batch = database.open_batch();
    batch.insert(
        "rows",
        vec![Value::U64(1), Value::String("visible".to_owned())],
    );
    batch.insert(
        "rows",
        vec![Value::U64(2), Value::String("hidden".to_owned())],
    );
    batch.insert("history", history_values(1, 10, 1, "old"));
    batch.insert("history", history_values(1, 20, 1, "winner"));
    batch.insert("history", history_values(2, 99, 1, "hidden"));
    database.commit_batch(batch).await.unwrap();
    assert_eq!(
        subscription.recv().unwrap().to_values().unwrap(),
        [(history_values(1, 20, 1, "winner"), 1)]
    );

    let mut batch = database.open_batch();
    batch.delete("history", history_key(1, 20, 1));
    database.commit_batch(batch).await.unwrap();
    assert_eq!(
        subscription.recv().unwrap().to_values().unwrap(),
        [
            (history_values(1, 20, 1, "winner"), -1),
            (history_values(1, 10, 1, "old"), 1),
        ]
    );

    let mut actual = database
        .query_graph(graph)
        .await
        .unwrap()
        .to_values()
        .unwrap();
    actual.sort_by_key(|(values, _)| match &values[..] {
        [Value::U64(row), Value::U64(stamp), Value::U64(node), ..] => (*row, *stamp, *node),
        _ => unreachable!(),
    });
    assert_eq!(actual, [(history_values(1, 10, 1, "old"), 1)]);
}

#[futures_test::test]
async fn predicate_or_filter_matches_either_branch() {
    let storage = MemoryStorage::new(&["albums"]).expect("valid memory storage families");
    let mut database = Database::new(albums_schema(), storage).await.unwrap();
    let graph = GraphBuilder::table("albums").filter(
        PredicateExpr::Or(vec![
            PredicateExpr::eq("title", Value::String("Kind of Blue".to_owned())),
            PredicateExpr::gt("id", Value::U64(10)),
        ])
        .canonicalize(),
    );

    let mut batch = database.open_batch();
    batch.insert(
        "albums",
        vec![Value::U64(1), Value::String("Kind of Blue".to_owned())],
    );
    batch.insert(
        "albums",
        vec![Value::U64(2), Value::String("Blue Train".to_owned())],
    );
    batch.insert(
        "albums",
        vec![Value::U64(11), Value::String("Speak No Evil".to_owned())],
    );
    database.commit_batch(batch).await.unwrap();

    let mut actual = database
        .query_graph(graph)
        .await
        .unwrap()
        .to_values()
        .unwrap();
    actual.sort_by_key(|(values, _)| match &values[..] {
        [Value::U64(id), ..] => *id,
        _ => unreachable!(),
    });
    assert_eq!(
        actual,
        [
            (
                vec![Value::U64(1), Value::String("Kind of Blue".to_owned())],
                1
            ),
            (
                vec![Value::U64(11), Value::String("Speak No Evil".to_owned())],
                1
            ),
        ]
    );
}

#[futures_test::test]
async fn arg_by_rejects_bad_primary_keys() {
    let storage = MemoryStorage::new(&["history", "rows", "blockers"])
        .expect("valid memory storage families");
    let mut database = Database::new(history_schema(), storage).await.unwrap();

    let err = database
        .subscribe_one_sink(GraphBuilder::arg_max_by(
            GraphBuilder::table("history"),
            ["row"],
            ["node", "stamp"],
        ))
        .await
        .unwrap_err();
    assert!(format!("{err}").contains("requires primary key"));
}

#[futures_test::test]
async fn unwrap_nullable_can_feed_join_key() {
    let storage = MemoryStorage::new(&["tracks", "albums", "indices"])
        .expect("valid memory storage families");
    let mut tracks_schema = indexed_tracks_schema();
    let mut albums_schema = albums_schema();
    let mut database = Database::new(
        DatabaseSchema::new([
            tracks_schema.tables.remove(0),
            albums_schema.tables.remove(0),
        ]),
        storage,
    )
    .await
    .unwrap();

    let mut batch = database.open_batch();
    batch.insert(
        "albums",
        vec![Value::U64(1), Value::String("One".to_owned())],
    );
    batch.insert(
        "albums",
        vec![Value::U64(2), Value::String("Two".to_owned())],
    );
    batch.insert("tracks", track_values(1, 7, Some(1), "Intro"));
    batch.insert("tracks", track_values(2, 7, None, "Hidden"));
    batch.insert("tracks", track_values(3, 7, Some(2), "Outro"));
    database.commit_batch(batch).await.unwrap();

    let mut values = database
        .query_graph(
            GraphBuilder::join(
                GraphBuilder::table("tracks").unwrap_nullable("disc"),
                GraphBuilder::table("albums"),
                ["disc"],
                ["id"],
            )
            .project_fields([
                ProjectField::renamed("left.id", "track_id"),
                ProjectField::renamed("right.title", "album_title"),
            ]),
        )
        .await
        .unwrap()
        .to_values()
        .unwrap();
    values.sort_by_key(|(values, _)| match &values[0] {
        Value::U64(value) => *value,
        other => panic!("expected track id, got {other:?}"),
    });
    assert_eq!(
        values,
        [
            (vec![Value::U64(1), Value::String("One".to_owned())], 1),
            (vec![Value::U64(3), Value::String("Two".to_owned())], 1),
        ]
    );
}

#[futures_test::test]
async fn unwrap_nullable_can_feed_prepared_binding_join_key() {
    let storage =
        MemoryStorage::new(&["tracks", "indices"]).expect("valid memory storage families");
    let mut database = Database::new(indexed_tracks_schema(), storage)
        .await
        .unwrap();

    let mut batch = database.open_batch();
    batch.insert("tracks", track_values(1, 7, Some(1), "Intro"));
    batch.insert("tracks", track_values(2, 7, None, "Hidden"));
    batch.insert("tracks", track_values(3, 7, Some(2), "Outro"));
    database.commit_batch(batch).await.unwrap();

    let binding_descriptor = RecordDescriptor::new([("disc", ColumnType::U64.clone())]);
    let shape = database
        .prepare_one_sink(
            GraphBuilder::join(
                GraphBuilder::binding_source("disc_param", binding_descriptor),
                GraphBuilder::table("tracks").unwrap_nullable("disc"),
                ["disc"],
                ["disc"],
            )
            .project_fields([
                ProjectField::renamed("right.id", "id"),
                ProjectField::renamed("right.disc", "disc"),
            ]),
            "disc_param",
            binding_descriptor,
            ["id"],
        )
        .await
        .unwrap();
    let disc_one = database
        .bind_shape_one_sink(shape.id(), &[Value::U64(1)])
        .await
        .unwrap();
    assert_eq!(
        expect_recv_vals(&disc_one),
        [(vec![Value::U64(1), Value::U64(1)], 1)]
    );
}

#[futures_test::test]
async fn prepared_binding_join_hydrates_anti_join_input() {
    let storage = MemoryStorage::new(&["tracks", "blockers", "indices"])
        .expect("valid memory storage families");
    let schema = DatabaseSchema::new([
        indexed_tracks_schema().tables.remove(0),
        TableSchema::new("blockers", [ColumnSchema::new("id", ColumnType::U64)])
            .with_primary_key(PrimaryKey::new("id", IntegerKeyType::U64)),
    ]);
    let mut database = Database::new(schema, storage).await.unwrap();

    let mut batch = database.open_batch();
    batch.insert("tracks", track_values(1, 7, Some(1), "Intro"));
    batch.insert("tracks", track_values(2, 7, Some(2), "Outro"));
    database.commit_batch(batch).await.unwrap();

    let binding_descriptor = RecordDescriptor::new([("disc", ColumnType::U64.clone())]);
    let visible = GraphBuilder::anti_join(
        GraphBuilder::table("tracks").unwrap_nullable("disc"),
        GraphBuilder::table("blockers"),
        ["id"],
        ["id"],
    );
    let shape = database
        .prepare_one_sink(
            GraphBuilder::join(
                GraphBuilder::binding_source("disc_param", binding_descriptor),
                visible,
                ["disc"],
                ["disc"],
            )
            .project_fields([
                ProjectField::renamed("right.id", "id"),
                ProjectField::renamed("right.disc", "disc"),
            ]),
            "disc_param",
            binding_descriptor,
            ["id"],
        )
        .await
        .unwrap();
    let disc_one = database
        .bind_shape_one_sink(shape.id(), &[Value::U64(1)])
        .await
        .unwrap();
    assert_eq!(
        expect_recv_vals(&disc_one),
        [(vec![Value::U64(1), Value::U64(1)], 1)]
    );
}

#[futures_test::test]
async fn prepared_binding_join_hydrates_filtered_unwrapped_anti_join_input() {
    let storage = MemoryStorage::new(&["items", "blockers", "indices"])
        .expect("valid memory storage families");
    let schema = DatabaseSchema::new([
        TableSchema::new(
            "items",
            [
                ColumnSchema::new("id", ColumnType::U64),
                ColumnSchema::new("owner", ColumnType::Uuid.nullable()),
                ColumnSchema::new("state", ColumnType::String.nullable()),
            ],
        )
        .with_primary_key(PrimaryKey::new("id", IntegerKeyType::U64)),
        TableSchema::new("blockers", [ColumnSchema::new("id", ColumnType::U64)])
            .with_primary_key(PrimaryKey::new("id", IntegerKeyType::U64)),
    ]);
    let mut database = Database::new(schema, storage).await.unwrap();
    let owner = uuid::Uuid::from_bytes([1; 16]);

    let mut batch = database.open_batch();
    batch.insert(
        "items",
        vec![
            Value::U64(1),
            Value::Nullable(Some(Box::new(Value::Uuid(owner)))),
            Value::Nullable(Some(Box::new(Value::String("open".to_owned())))),
        ],
    );
    batch.insert(
        "items",
        vec![
            Value::U64(2),
            Value::Nullable(Some(Box::new(Value::Uuid(owner)))),
            Value::Nullable(Some(Box::new(Value::String("done".to_owned())))),
        ],
    );
    database.commit_batch(batch).await.unwrap();

    let binding_descriptor = RecordDescriptor::new([("owner", ColumnType::Uuid.clone())]);
    let visible = GraphBuilder::anti_join(
        GraphBuilder::table("items")
            .unwrap_nullable("state")
            .filter(PredicateExpr::eq("state", Value::String("open".to_owned())))
            .unwrap_nullable("owner"),
        GraphBuilder::table("blockers"),
        ["id"],
        ["id"],
    );
    let shape = database
        .prepare_one_sink(
            GraphBuilder::join(
                GraphBuilder::binding_source("owner_param", binding_descriptor),
                visible,
                ["owner"],
                ["owner"],
            )
            .project_fields([
                ProjectField::renamed("left.owner", "owner"),
                ProjectField::renamed("right.id", "id"),
            ]),
            "owner_param",
            binding_descriptor,
            ["owner"],
        )
        .await
        .unwrap();
    let bound = database
        .bind_shape_one_sink(shape.id(), &[Value::Uuid(owner)])
        .await
        .unwrap();
    assert_eq!(
        expect_recv_vals(&bound),
        [(vec![Value::Uuid(owner), Value::U64(1)], 1)]
    );
}
