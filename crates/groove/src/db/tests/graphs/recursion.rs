//! Recursive closure, retraction, and convergence.

use super::*;

#[test]
fn recursive_graph_subscriptions_settle_transitive_closure_in_one_tick() {
    let temp_dir = tempfile::tempdir().unwrap();
    let storage = RocksDbStorage::open(temp_dir.path(), &["edges"]).unwrap();
    let mut database = Database::new(edges_schema(), storage).unwrap();
    let subscription_id = database.subscribe_one_sink(reachability_graph(16)).unwrap();

    let mut batch = database.open_batch();
    insert_edge(&mut batch, 1, 1, 2);
    insert_edge(&mut batch, 2, 2, 3);
    insert_edge(&mut batch, 3, 3, 4);
    database.commit_batch(batch).unwrap();
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

#[test]
fn recursive_graph_subscriptions_retract_derived_paths_after_delete() {
    let temp_dir = tempfile::tempdir().unwrap();
    let storage = RocksDbStorage::open(temp_dir.path(), &["edges"]).unwrap();
    let mut database = Database::new(edges_schema(), storage).unwrap();
    let subscription_id = database.subscribe_one_sink(reachability_graph(16)).unwrap();

    let mut batch = database.open_batch();
    insert_edge(&mut batch, 1, 1, 2);
    insert_edge(&mut batch, 2, 2, 3);
    insert_edge(&mut batch, 3, 3, 4);
    database.commit_batch(batch).unwrap();
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
    database.commit_batch(batch).unwrap();
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

#[test]
fn prepared_recursive_binding_retracts_transitive_paths_after_edge_delete() {
    let temp_dir = tempfile::tempdir().unwrap();
    let storage = RocksDbStorage::open(temp_dir.path(), &["edges"]).unwrap();
    let mut database = Database::new(edges_schema(), storage).unwrap();
    let shape = prepared_reachability_shape(&mut database);
    let subscription = database
        .bind_shape_one_sink(shape.id(), &[Value::U64(1)])
        .unwrap();
    let _empty = subscription.recv().unwrap();

    let mut batch = database.open_batch();
    insert_edge(&mut batch, 1, 1, 2);
    insert_edge(&mut batch, 2, 2, 3);
    insert_edge(&mut batch, 3, 3, 4);
    database.commit_batch(batch).unwrap();
    let _initial = expect_recv_vals(&subscription);

    let mut batch = database.open_batch();
    batch.delete("edges", PrimaryKeyValue::U64(2));
    database.commit_batch(batch).unwrap();
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

#[test]
fn prepared_recursive_binding_skips_recompute_for_unrelated_table_delta() {
    let temp_dir = tempfile::tempdir().unwrap();
    let storage = RocksDbStorage::open(temp_dir.path(), &["edges", "docs"]).unwrap();
    let mut database = Database::new(edges_docs_schema(), storage).unwrap();
    let shape = database
        .prepare_one_sink(
            prepared_reachability_graph(GraphBuilder::table("edges"), 16),
            "prepared-reach",
            RecordDescriptor::new([("seed", ColumnType::U64.clone())]),
            ["seed".to_owned()],
        )
        .unwrap();
    let subscription = database
        .bind_shape_one_sink(shape.id(), &[Value::U64(1)])
        .unwrap();
    assert_eq!(
        expect_recv_vals(&subscription),
        [(vec![Value::U64(1), Value::U64(1)], 1)]
    );

    let mut batch = database.open_batch();
    insert_edge(&mut batch, 1, 1, 2);
    insert_edge(&mut batch, 2, 2, 3);
    database.commit_batch(batch).unwrap();
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
    database.commit_batch(batch).unwrap();
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

#[test]
fn prepared_recursive_binding_recomputes_for_relevant_insert_and_retraction() {
    let temp_dir = tempfile::tempdir().unwrap();
    let storage = RocksDbStorage::open(temp_dir.path(), &["edges"]).unwrap();
    let mut database = Database::new(edges_schema(), storage).unwrap();
    let shape = prepared_reachability_shape(&mut database);
    let subscription = database
        .bind_shape_one_sink(shape.id(), &[Value::U64(1)])
        .unwrap();
    assert_eq!(
        expect_recv_vals(&subscription),
        [(vec![Value::U64(1), Value::U64(1)], 1)]
    );

    let mut batch = database.open_batch();
    insert_edge(&mut batch, 1, 1, 2);
    database.commit_batch(batch).unwrap();
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
    database.commit_batch(batch).unwrap();
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

#[test]
fn prepared_recursive_positive_step_inserts_match_recompute_diff_without_recompute() {
    let temp_dir = tempfile::tempdir().unwrap();
    let storage = RocksDbStorage::open(temp_dir.path(), &["edges"]).unwrap();
    let mut database = Database::new(edges_schema(), storage).unwrap();
    let shape = prepared_reachability_shape(&mut database);
    let subscription = database
        .bind_shape_one_sink(shape.id(), &[Value::U64(1)])
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
        database.commit_batch(batch).unwrap();
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

#[test]
fn prepared_recursive_binding_retracts_paths_after_first_edge_delete() {
    let temp_dir = tempfile::tempdir().unwrap();
    let storage = RocksDbStorage::open(temp_dir.path(), &["edges"]).unwrap();
    let mut database = Database::new(edges_schema(), storage).unwrap();
    let shape = prepared_reachability_shape(&mut database);
    let subscription = database
        .bind_shape_one_sink(shape.id(), &[Value::U64(1)])
        .unwrap();
    let _empty = subscription.recv().unwrap();

    let mut batch = database.open_batch();
    insert_edge(&mut batch, 1, 1, 2);
    insert_edge(&mut batch, 2, 2, 3);
    insert_edge(&mut batch, 3, 3, 4);
    database.commit_batch(batch).unwrap();
    let _initial = expect_recv_vals(&subscription);

    let mut batch = database.open_batch();
    batch.delete("edges", PrimaryKeyValue::U64(1));
    database.commit_batch(batch).unwrap();
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

#[test]
fn prepared_recursive_binding_retraction_recomputes_instead_of_erroring() {
    let temp_dir = tempfile::tempdir().unwrap();
    let storage = RocksDbStorage::open(temp_dir.path(), &["edges"]).unwrap();
    let mut database = Database::new(edges_schema(), storage).unwrap();
    let shape = prepared_reachability_shape(&mut database);
    let first = database
        .bind_shape_one_sink(shape.id(), &[Value::U64(1)])
        .unwrap();
    let _empty = first.recv().unwrap();

    let mut batch = database.open_batch();
    insert_edge(&mut batch, 1, 1, 2);
    insert_edge(&mut batch, 2, 2, 3);
    insert_edge(&mut batch, 3, 9, 10);
    insert_edge(&mut batch, 4, 5, 6);
    database.commit_batch(batch).unwrap();

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
    database.commit_batch(batch).unwrap();

    database.flush().unwrap();
    assert_eq!(
        database.last_tick_metrics().unwrap().recursive_recomputes,
        1
    );

    let third = database
        .bind_shape_one_sink(shape.id(), &[Value::U64(5)])
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

#[test]
fn prepared_recursive_binding_retracts_transitive_paths_from_antijoin_input() {
    let temp_dir = tempfile::tempdir().unwrap();
    let storage = RocksDbStorage::open(temp_dir.path(), &["edges", "blockers"]).unwrap();
    let mut database = Database::new(edges_blockers_schema(), storage).unwrap();
    let shape = prepared_reachability_with_antijoin_shape(&mut database);
    let subscription = database
        .bind_shape_one_sink(shape.id(), &[Value::U64(1)])
        .unwrap();
    let _empty = subscription.recv().unwrap();

    let mut batch = database.open_batch();
    insert_edge(&mut batch, 1, 1, 2);
    insert_edge(&mut batch, 2, 2, 3);
    insert_edge(&mut batch, 3, 3, 4);
    database.commit_batch(batch).unwrap();
    let _initial = expect_recv_vals(&subscription);

    let mut batch = database.open_batch();
    batch.insert(
        "blockers",
        vec![Value::U64(1), Value::U64(2), Value::U64(3)],
    );
    database.commit_batch(batch).unwrap();
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

#[test]
fn prepared_recursive_binding_retracts_first_paths_from_antijoin_input() {
    let temp_dir = tempfile::tempdir().unwrap();
    let storage = RocksDbStorage::open(temp_dir.path(), &["edges", "blockers"]).unwrap();
    let mut database = Database::new(edges_blockers_schema(), storage).unwrap();
    let shape = prepared_reachability_with_antijoin_shape(&mut database);
    let subscription = database
        .bind_shape_one_sink(shape.id(), &[Value::U64(1)])
        .unwrap();
    let _empty = subscription.recv().unwrap();

    let mut batch = database.open_batch();
    insert_edge(&mut batch, 1, 1, 2);
    insert_edge(&mut batch, 2, 2, 3);
    insert_edge(&mut batch, 3, 3, 4);
    database.commit_batch(batch).unwrap();
    let _initial = expect_recv_vals(&subscription);

    let mut batch = database.open_batch();
    batch.insert(
        "blockers",
        vec![Value::U64(1), Value::U64(1), Value::U64(2)],
    );
    database.commit_batch(batch).unwrap();
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

#[test]
fn recursive_graph_subscriptions_collapse_duplicate_derivations() {
    let temp_dir = tempfile::tempdir().unwrap();
    let storage = RocksDbStorage::open(temp_dir.path(), &["edges"]).unwrap();
    let mut database = Database::new(edges_schema(), storage).unwrap();
    let subscription_id = database.subscribe_one_sink(reachability_graph(16)).unwrap();

    let mut batch = database.open_batch();
    insert_edge(&mut batch, 1, 1, 2);
    insert_edge(&mut batch, 2, 1, 3);
    insert_edge(&mut batch, 3, 2, 4);
    insert_edge(&mut batch, 4, 3, 4);
    database.commit_batch(batch).unwrap();
    let values = expect_recv_vals(&subscription_id);

    assert!(values.contains(&(vec![Value::U64(1), Value::U64(4)], 1)));
}

#[test]
fn recursive_graph_subscriptions_recompute_after_edge_update() {
    let temp_dir = tempfile::tempdir().unwrap();
    let storage = RocksDbStorage::open(temp_dir.path(), &["edges"]).unwrap();
    let mut database = Database::new(edges_schema(), storage).unwrap();
    let subscription_id = database.subscribe_one_sink(reachability_graph(16)).unwrap();

    let mut batch = database.open_batch();
    insert_edge(&mut batch, 1, 1, 2);
    insert_edge(&mut batch, 2, 2, 3);
    database.commit_batch(batch).unwrap();
    let _initial_reach = expect_recv_vals(&subscription_id);

    let mut batch = database.open_batch();
    update_edge(&mut batch, 2, 2, 4);
    database.commit_batch(batch).unwrap();
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

#[test]
fn recursive_graph_subscriptions_incrementally_extend_existing_reach_with_new_edge() {
    let temp_dir = tempfile::tempdir().unwrap();
    let storage = RocksDbStorage::open(temp_dir.path(), &["edges"]).unwrap();
    let mut database = Database::new(edges_schema(), storage).unwrap();
    let subscription_id = database.subscribe_one_sink(reachability_graph(16)).unwrap();

    let mut batch = database.open_batch();
    insert_edge(&mut batch, 1, 1, 2);
    database.commit_batch(batch).unwrap();
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
    database.commit_batch(batch).unwrap();
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

#[test]
fn recursive_graph_subscriptions_incrementally_extend_new_seed_with_existing_edge() {
    let temp_dir = tempfile::tempdir().unwrap();
    let storage = RocksDbStorage::open(temp_dir.path(), &["edges"]).unwrap();
    let mut database = Database::new(edges_schema(), storage).unwrap();
    let subscription_id = database.subscribe_one_sink(reachability_graph(16)).unwrap();

    let mut batch = database.open_batch();
    insert_edge(&mut batch, 1, 2, 3);
    database.commit_batch(batch).unwrap();
    let _initial_reach = expect_recv_vals(&subscription_id);

    let mut batch = database.open_batch();
    insert_edge(&mut batch, 2, 1, 2);
    database.commit_batch(batch).unwrap();
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

#[test]
fn recursive_graph_subscriptions_converge_on_self_cycles() {
    let temp_dir = tempfile::tempdir().unwrap();
    let storage = RocksDbStorage::open(temp_dir.path(), &["edges"]).unwrap();
    let mut database = Database::new(edges_schema(), storage).unwrap();
    let subscription = database.subscribe_one_sink(reachability_graph(2)).unwrap();
    let _initial = subscription.recv().unwrap();

    let mut batch = database.open_batch();
    insert_edge(&mut batch, 1, 1, 1);
    database.commit_batch(batch).unwrap();
    let values = subscription.recv().unwrap().to_values().unwrap();

    assert_eq!(values, [(vec![Value::U64(1), Value::U64(1)], 1)]);
}

#[test]
fn recursive_graphs_reject_seed_and_step_output_descriptor_mismatch() {
    let temp_dir = tempfile::tempdir().unwrap();
    let storage = RocksDbStorage::open(temp_dir.path(), &["edges"]).unwrap();
    let mut database = Database::new(edges_schema(), storage).unwrap();
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
        database.subscribe_one_sink(graph).unwrap_err(),
        Error::IvmRuntime(IvmRuntimeError::GraphOutputMismatch)
    ));
}

#[test]
fn recursive_graphs_reject_nested_recursion_for_v0() {
    let temp_dir = tempfile::tempdir().unwrap();
    let storage = RocksDbStorage::open(temp_dir.path(), &["edges"]).unwrap();
    let mut database = Database::new(edges_schema(), storage).unwrap();
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
        database.subscribe_one_sink(graph).unwrap_err(),
        Error::IvmRuntime(IvmRuntimeError::UnsupportedNestedRecursion)
    ));
}

#[test]
fn recursive_graphs_fail_when_frontier_exceeds_max_iters() {
    let temp_dir = tempfile::tempdir().unwrap();
    let storage = RocksDbStorage::open(temp_dir.path(), &["edges"]).unwrap();
    let mut database = Database::new(edges_schema(), storage).unwrap();

    let mut batch = database.open_batch();
    insert_edge(&mut batch, 1, 1, 2);
    insert_edge(&mut batch, 2, 2, 3);
    insert_edge(&mut batch, 3, 3, 4);
    database.commit_batch(batch).unwrap();

    assert!(matches!(
        database.query_graph(reachability_graph(1)).unwrap_err(),
        Error::IvmRuntime(IvmRuntimeError::RecursiveIterationLimit { max_iters: 1, .. })
    ));
}
