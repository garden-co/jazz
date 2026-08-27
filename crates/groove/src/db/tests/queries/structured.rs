//! Structured collectors and nested result-tree behavior.

use super::*;

#[futures_test::test]
async fn collect_by_round_trips_ordered_explicit_child_ids() {
    let storage = MemoryStorage::new(&["history", "rows", "blockers"])
        .expect("valid memory storage families");
    let mut database = Database::new(history_schema(), storage).await.unwrap();
    let subscription = database
        .subscribe_one_sink(history_collect_by(3))
        .await
        .unwrap();
    assert!(subscription.recv().unwrap().is_empty());

    let mut batch = database.open_batch();
    // Deliberately not in declared stamp order.
    batch.insert("history", history_values(1, 30, 30, "third"));
    batch.insert("history", history_values(1, 10, 10, "first"));
    batch.insert("history", history_values(1, 20, 20, "second"));
    database.commit_batch(batch).await.unwrap();

    assert_eq!(
        subscription.recv().unwrap().to_values().unwrap(),
        [(
            collect_parent(1, &[(10, "first"), (20, "second"), (30, "third")]),
            1,
        )]
    );
}

#[futures_test::test]
async fn collect_by_expand_renders_selected_tuples_in_source_order() {
    let storage = MemoryStorage::new(&["history", "rows", "blockers"])
        .expect("valid memory storage families");
    let mut database = Database::new(history_schema(), storage).await.unwrap();
    let subscription = database
        .subscribe_one_sink(history_collect_by_expand(0, 3))
        .await
        .unwrap();
    assert!(subscription.recv().unwrap().is_empty());

    let mut batch = database.open_batch();
    // Deliberately not in declared stamp order. The first two columns are the
    // ordered root/child occurrence-source vector carried by the tuple.
    batch.insert("history", history_values(1, 30, 30, "third"));
    batch.insert("history", history_values(1, 10, 10, "first"));
    batch.insert("history", history_values(1, 20, 20, "second"));
    database.commit_batch(batch).await.unwrap();

    assert_eq!(
        subscription.recv().unwrap().to_values().unwrap(),
        [
            (
                vec![
                    Value::U64(1),
                    Value::U64(10),
                    Value::String("first".to_owned()),
                ],
                1,
            ),
            (
                vec![
                    Value::U64(1),
                    Value::U64(20),
                    Value::String("second".to_owned()),
                ],
                1,
            ),
            (
                vec![
                    Value::U64(1),
                    Value::U64(30),
                    Value::String("third".to_owned()),
                ],
                1,
            ),
        ]
    );
}
#[futures_test::test]
async fn collect_by_expand_diffs_only_selected_tuple_occurrences() {
    let storage = MemoryStorage::new(&["history", "rows", "blockers"])
        .expect("valid memory storage families");
    let mut database = Database::new(history_schema(), storage).await.unwrap();
    let subscription = database
        .subscribe_one_sink(history_collect_by_expand(0, 2))
        .await
        .unwrap();
    assert!(subscription.recv().unwrap().is_empty());

    let mut batch = database.open_batch();
    batch.insert("history", history_values(1, 10, 10, "first"));
    batch.insert("history", history_values(1, 20, 20, "second"));
    database.commit_batch(batch).await.unwrap();
    let _initial = subscription.recv().unwrap();

    let mut batch = database.open_batch();
    batch.insert("history", history_values(1, 5, 5, "front"));
    database.commit_batch(batch).await.unwrap();
    assert_eq!(
        subscription.recv().unwrap().to_values().unwrap(),
        [
            (
                vec![
                    Value::U64(1),
                    Value::U64(5),
                    Value::String("front".to_owned()),
                ],
                1,
            ),
            (
                vec![
                    Value::U64(1),
                    Value::U64(20),
                    Value::String("second".to_owned()),
                ],
                -1,
            ),
        ]
    );
}

#[futures_test::test]
async fn collect_by_expand_suppresses_byte_equal_selected_tuples() {
    let storage = MemoryStorage::new(&["history", "rows", "blockers"])
        .expect("valid memory storage families");
    let mut database = Database::new(history_schema(), storage).await.unwrap();
    let subscription = database
        .subscribe_one_sink(history_collect_by_expand(0, 2))
        .await
        .unwrap();
    assert!(subscription.recv().unwrap().is_empty());

    let mut batch = database.open_batch();
    batch.insert("history", history_values(1, 10, 10, "first"));
    batch.insert("history", history_values(1, 20, 20, "second"));
    database.commit_batch(batch).await.unwrap();
    let _initial = subscription.recv().unwrap();

    let mut batch = database.open_batch();
    batch.insert("history", history_values(1, 30, 30, "outside"));
    database.commit_batch(batch).await.unwrap();
    assert!(matches!(subscription.try_recv(), Err(TryRecvError::Empty)));
}

#[futures_test::test]
async fn collect_by_expand_honors_order_tie_offset_and_limit() {
    let storage = MemoryStorage::new(&["history", "rows", "blockers"])
        .expect("valid memory storage families");
    let mut database = Database::new(history_schema(), storage).await.unwrap();
    let subscription = database
        .subscribe_one_sink(history_collect_by_expand(1, 2))
        .await
        .unwrap();
    assert!(subscription.recv().unwrap().is_empty());

    let mut batch = database.open_batch();
    batch.insert("history", history_values(1, 10, 10, "first"));
    batch.insert("history", history_values(1, 10, 20, "tied-second"));
    batch.insert("history", history_values(1, 20, 30, "third"));
    batch.insert("history", history_values(1, 30, 40, "outside"));
    database.commit_batch(batch).await.unwrap();
    assert_eq!(
        subscription.recv().unwrap().to_values().unwrap(),
        [
            (
                vec![
                    Value::U64(1),
                    Value::U64(20),
                    Value::String("tied-second".to_owned()),
                ],
                1,
            ),
            (
                vec![
                    Value::U64(1),
                    Value::U64(30),
                    Value::String("third".to_owned()),
                ],
                1,
            ),
        ]
    );
}

#[futures_test::test]
async fn collect_by_expand_rejects_duplicate_occurrence_source_ids() {
    let storage = MemoryStorage::new(&["history", "rows", "blockers"])
        .expect("valid memory storage families");
    let mut database = Database::new(history_schema(), storage).await.unwrap();
    let mut batch = database.open_batch();
    batch.insert("history", history_values(1, 10, 7, "first"));
    batch.insert("history", history_values(1, 20, 7, "ambiguous"));
    database.commit_batch(batch).await.unwrap();

    let subscription = database
        .subscribe_one_sink(history_collect_by_expand(0, 3))
        .await
        .unwrap();
    let event = std::future::poll_fn(|cx| subscription.poll_next_event(cx)).await;
    assert!(matches!(
        event,
        SubscriptionEvent::Error(error)
            if matches!(
                error.source_error(),
                Some(IvmRuntimeError::DuplicateCollectByOccurrenceId)
            )
    ));
}

#[futures_test::test]
async fn collect_by_rejects_join_and_nested_collector_consumers() {
    let storage = MemoryStorage::new(&["history", "rows", "blockers"])
        .expect("valid memory storage families");
    let mut database = Database::new(history_schema(), storage).await.unwrap();
    let collector = history_collect_by(2);
    let relational_consumers = [
        GraphBuilder::join(
            collector.clone(),
            GraphBuilder::table("history"),
            ["row"],
            ["row"],
        ),
        GraphBuilder::collect_by(
            collector,
            ["row"],
            [CollectByField::named("row")],
            [CollectByField::named("row")],
            "nested",
            [TopByOrder::asc("row")],
            ["row"],
            0,
            TopByLimit::Finite(1),
        ),
    ];
    for graph in relational_consumers {
        assert!(matches!(
            database.subscribe_one_sink(graph).await,
            Err(Error::IvmRuntime(IvmRuntimeError::CollectByMustBeTerminal))
        ));
    }
}

#[futures_test::test]
async fn collect_by_rejects_filter_consumer() {
    let storage = MemoryStorage::new(&["history", "rows", "blockers"])
        .expect("valid memory storage families");
    let mut database = Database::new(history_schema(), storage).await.unwrap();
    let graph = history_collect_by(2).filter(PredicateExpr::gt("row", Value::U64(0)));

    assert!(matches!(
        database.subscribe_one_sink(graph).await,
        Err(Error::IvmRuntime(IvmRuntimeError::CollectByMustBeTerminal))
    ));
}

#[futures_test::test]
async fn collect_by_rejects_project_consumer() {
    let storage = MemoryStorage::new(&["history", "rows", "blockers"])
        .expect("valid memory storage families");
    let mut database = Database::new(history_schema(), storage).await.unwrap();
    let graph = history_collect_by(2).project(["row"]);

    assert!(matches!(
        database.subscribe_one_sink(graph).await,
        Err(Error::IvmRuntime(IvmRuntimeError::CollectByMustBeTerminal))
    ));
}

#[futures_test::test]
async fn collect_by_suppresses_unchanged_rendered_group_and_replaces_once_at_boundary() {
    let storage = MemoryStorage::new(&["history", "rows", "blockers"])
        .expect("valid memory storage families");
    let mut database = Database::new(history_schema(), storage).await.unwrap();
    let subscription = database
        .subscribe_one_sink(history_collect_by(2))
        .await
        .unwrap();
    assert!(subscription.recv().unwrap().is_empty());

    let mut batch = database.open_batch();
    batch.insert("history", history_values(1, 10, 10, "first"));
    batch.insert("history", history_values(1, 20, 20, "second"));
    database.commit_batch(batch).await.unwrap();
    let _initial = subscription.recv().unwrap();

    let mut batch = database.open_batch();
    batch.insert("history", history_values(1, 30, 30, "outside"));
    database.commit_batch(batch).await.unwrap();
    assert!(matches!(subscription.try_recv(), Err(TryRecvError::Empty)));

    let mut batch = database.open_batch();
    batch.insert("history", history_values(1, 5, 5, "front"));
    database.commit_batch(batch).await.unwrap();
    let replacement = subscription.recv().unwrap().to_values().unwrap();
    assert_eq!(replacement.len(), 2);
    assert_eq!(
        replacement[0],
        (collect_parent(1, &[(10, "first"), (20, "second")]), -1)
    );
    assert_eq!(
        replacement[1],
        (collect_parent(1, &[(5, "front"), (10, "first")]), 1)
    );
}

#[futures_test::test]
async fn collect_by_multisink_emits_descendant_terminal_operations() {
    let storage = MemoryStorage::new(&["history", "rows", "blockers"])
        .expect("valid memory storage families");
    let mut database = Database::new(history_schema(), storage).await.unwrap();
    let subscription = database
        .subscribe([("rows", history_collect_by(2))])
        .unwrap();
    let initial = subscription.recv().unwrap();
    assert!(initial.terminal_sinks.is_empty());

    let mut batch = database.open_batch();
    batch.insert("history", history_values(1, 10, 10, "first"));
    batch.insert("history", history_values(1, 20, 20, "second"));
    database.commit_batch(batch).await.unwrap();
    let initial_rows = subscription.recv().unwrap();
    assert!(matches!(
        initial_rows.terminal_sinks["rows"].operations.as_slice(),
        [crate::ivm::TerminalOperation {
            path,
            edit: TerminalEdit::Insert { .. },
            ..
        }] if path.is_empty()
    ));

    let mut batch = database.open_batch();
    batch.insert("history", history_values(1, 5, 5, "front"));
    database.commit_batch(batch).await.unwrap();
    let update = subscription.recv().unwrap();
    let operations = &update.terminal_sinks["rows"].operations;
    assert!(operations.iter().all(|operation| {
        matches!(operation.path.as_slice(), [TerminalPathSegment::Collection(field)] if field == "children")
    }));
    assert!(
        operations
            .iter()
            .any(|operation| matches!(operation.edit, TerminalEdit::Insert { index: 0, .. }))
    );
    assert!(
        operations
            .iter()
            .any(|operation| matches!(operation.edit, TerminalEdit::Remove { .. }))
    );
    assert!(
        operations
            .iter()
            .all(|operation| !matches!(operation.edit, TerminalEdit::Update { .. }))
    );
}

#[futures_test::test]
async fn one_shot_query_does_not_discard_live_collect_by_arrangement() {
    let storage = MemoryStorage::new(&["history", "rows", "blockers"])
        .expect("valid memory storage families");
    let mut database = Database::new(history_schema(), storage).await.unwrap();
    let mut batch = database.open_batch();
    batch.insert("history", history_values(1, 10, 10, "first"));
    database.commit_batch(batch).await.unwrap();

    let subscription = database
        .subscribe_one_sink(history_collect_by(2))
        .await
        .unwrap();
    assert_eq!(
        subscription.recv().unwrap().to_values().unwrap(),
        [(collect_parent(1, &[(10, "first")]), 1)]
    );

    // One-shot queries collect their ephemeral graph immediately. That GC
    // boundary must retain arrangements owned by an unrelated live terminal.
    let snapshot = database
        .query_graph(GraphBuilder::table("rows"))
        .await
        .unwrap();
    assert!(snapshot.is_empty());

    let mut batch = database.open_batch();
    batch.insert("history", history_values(1, 20, 20, "second"));
    database.commit_batch(batch).await.unwrap();
    assert_eq!(
        subscription.recv().unwrap().to_values().unwrap(),
        [
            (collect_parent(1, &[(10, "first")]), -1),
            (collect_parent(1, &[(10, "first"), (20, "second")]), 1),
        ]
    );
}

#[futures_test::test]
async fn collect_by_tree_renders_sibling_slots_and_grandchildren_with_independent_windows() {
    let storage = MemoryStorage::new(&["tree"]).expect("valid memory storage families");
    let mut database = Database::new(collect_tree_schema(), storage).await.unwrap();
    let subscription = database
        .subscribe_one_sink(collect_tree_graph())
        .await
        .unwrap();
    assert!(subscription.recv().unwrap().is_empty());

    let mut batch = database.open_batch();
    batch.insert(
        "tree",
        collect_tree_values([1, 10, 20, 100, 10, 3, 3, 9, 9]),
    );
    batch.insert(
        "tree",
        collect_tree_values([2, 10, 20, 101, 20, 1, 1, 5, 5]),
    );
    batch.insert(
        "tree",
        collect_tree_values([3, 20, 10, 200, 10, 2, 2, 7, 7]),
    );
    database.commit_batch(batch).await.unwrap();
    let initial = subscription.recv().unwrap().to_values().unwrap();
    assert_eq!(initial.len(), 1);
    let root = &initial[0].0;
    let Value::Array(children) = &root[1] else {
        panic!("children must be an array")
    };
    assert_eq!(children.len(), 2);
    let Value::Record(first_child) = &children[0] else {
        panic!("child must be a record")
    };
    let Value::Record(second_child) = &children[1] else {
        panic!("child must be a record")
    };
    assert_eq!(first_child.to_values().unwrap()[0], Value::U64(20));
    let first_child_values = first_child.to_values().unwrap();
    let Value::Array(first_grandchildren) = &first_child_values[1] else {
        panic!("grandchildren must be an array")
    };
    assert_eq!(first_grandchildren.len(), 1);
    assert_eq!(second_child.to_values().unwrap()[0], Value::U64(10));
    let second_child_values = second_child.to_values().unwrap();
    let Value::Array(second_grandchildren) = &second_child_values[1] else {
        panic!("grandchildren must be an array")
    };
    assert_eq!(
        second_grandchildren
            .iter()
            .map(|value| match value {
                Value::Record(record) => record.to_values().unwrap()[0].clone(),
                _ => panic!("grandchild must be a record"),
            })
            .collect::<Vec<_>>(),
        [Value::U64(100), Value::U64(101)]
    );
    for (slot, expected) in [(&root[2], vec![1, 2]), (&root[3], vec![7])] {
        let Value::Array(records) = slot else {
            panic!("sibling slot must be an array")
        };
        assert_eq!(
            records
                .iter()
                .map(|value| match value {
                    Value::Record(record) => match record.to_values().unwrap()[0] {
                        Value::U64(value) => value,
                        _ => panic!("sibling value must be u64"),
                    },
                    _ => panic!("sibling value must be a record"),
                })
                .collect::<Vec<_>>(),
            expected
        );
    }
}

#[futures_test::test]
async fn collect_by_tree_keeps_routed_owner_keys_internal_and_isolated() {
    let storage = MemoryStorage::new(&["routed_tree"]).expect("valid memory storage families");
    let mut database = Database::new(routed_collect_tree_schema(), storage)
        .await
        .unwrap();
    let subscription = database
        .subscribe_one_sink(routed_collect_tree_graph())
        .await
        .unwrap();
    assert!(subscription.recv().unwrap().is_empty());

    // Two bindings deliberately share rendered root and child identities. The
    // route must still keep their grandchildren isolated, while staying out
    // of every rendered record descriptor.
    let mut batch = database.open_batch();
    batch.insert(
        "routed_tree",
        vec![
            Value::U64(1),
            Value::U64(10),
            Value::U64(1),
            Value::U64(20),
            Value::U64(1),
            Value::U64(100),
            Value::U64(1),
        ],
    );
    batch.insert(
        "routed_tree",
        vec![
            Value::U64(2),
            Value::U64(10),
            Value::U64(2),
            Value::U64(20),
            Value::U64(1),
            Value::U64(200),
            Value::U64(1),
        ],
    );
    database.commit_batch(batch).await.unwrap();
    let rows = subscription.recv().unwrap().to_values().unwrap();
    assert_eq!(rows.len(), 2);

    let grandchildren = rows
        .iter()
        .map(|(root, weight)| {
            assert_eq!(*weight, 1);
            assert_eq!(root.len(), 2, "route must not be a root output field");
            let Value::Array(children) = &root[1] else {
                panic!("children must be an array");
            };
            assert_eq!(children.len(), 1);
            let Value::Record(child) = &children[0] else {
                panic!("children must contain records");
            };
            let child = child.to_values().unwrap();
            assert_eq!(child.len(), 2, "route must not be a child output field");
            let Value::Array(grandchildren) = &child[1] else {
                panic!("grandchildren must be an array");
            };
            let Value::Record(grandchild) = &grandchildren[0] else {
                panic!("grandchildren must contain records");
            };
            assert_eq!(grandchild.to_values().unwrap().len(), 1);
            grandchild.to_values().unwrap()[0].clone()
        })
        .collect::<Vec<_>>();
    assert_eq!(grandchildren, vec![Value::U64(100), Value::U64(200)]);
}

#[futures_test::test]
async fn collect_by_tree_rejects_non_grouping_internal_owner_key() {
    let graph = GraphBuilder::collect_by_tree(
        GraphBuilder::table("routed_tree"),
        ["root", "route"],
        [CollectByField::named("root")],
        [CollectBySlotBuilder::new(
            ["root", "route"],
            [CollectByField::named("child")],
            "children",
            [],
            [TopByOrder::asc("child_order")],
            ["child"],
            0,
            TopByLimit::Unbounded,
        )
        // A non-grouping raw input is not stable owner metadata and must not
        // become an implicit hidden channel.
        .with_owner_key_cols(["grandchild"])],
    );
    let storage = MemoryStorage::new(&["routed_tree"]).expect("valid memory storage families");
    let mut database = Database::new(routed_collect_tree_schema(), storage)
        .await
        .unwrap();
    assert!(matches!(
        database.subscribe_one_sink(graph).await,
        Err(Error::IvmRuntime(IvmRuntimeError::InvalidCollectBy(message)))
            if message == "a slot owner key must also be a grouping field"
    ));
}

#[futures_test::test]
async fn collect_by_tree_grandchild_change_replaces_one_whole_parent_and_suppresses_unrendered_change()
 {
    let storage = MemoryStorage::new(&["tree"]).expect("valid memory storage families");
    let mut database = Database::new(collect_tree_schema(), storage).await.unwrap();
    let subscription = database
        .subscribe_one_sink(collect_tree_graph())
        .await
        .unwrap();
    assert!(subscription.recv().unwrap().is_empty());
    let mut batch = database.open_batch();
    batch.insert(
        "tree",
        collect_tree_values([1, 10, 20, 100, 10, 3, 3, 9, 9]),
    );
    batch.insert(
        "tree",
        collect_tree_values([2, 10, 20, 101, 20, 1, 1, 5, 5]),
    );
    database.commit_batch(batch).await.unwrap();
    let _initial = subscription.recv().unwrap();

    // Planted positive: this is inside the rendered grandchild window, so the
    // parent must change. Its one -/+ pair proves delivery is whole-parent,
    // not a child delta or one replacement at each descriptor level.
    let mut batch = database.open_batch();
    batch.insert("tree", collect_tree_values([3, 10, 20, 99, 0, 2, 2, 7, 7]));
    database.commit_batch(batch).await.unwrap();
    let replacement = subscription.recv().unwrap().to_values().unwrap();
    assert_eq!(replacement.len(), 2);
    assert_eq!(replacement[0].1, -1);
    assert_eq!(replacement[1].1, 1);
    let Value::Array(old_children) = &replacement[0].0[1] else {
        panic!()
    };
    let Value::Array(new_children) = &replacement[1].0[1] else {
        panic!()
    };
    let Value::Record(old_child) = &old_children[0] else {
        panic!()
    };
    let Value::Record(new_child) = &new_children[0] else {
        panic!()
    };
    let Value::Array(old_grandchildren) = &old_child.to_values().unwrap()[1] else {
        panic!()
    };
    let Value::Array(new_grandchildren) = &new_child.to_values().unwrap()[1] else {
        panic!()
    };
    assert_eq!(old_grandchildren.len(), 2);
    assert_eq!(new_grandchildren.len(), 2);
    let Value::Record(new_first_grandchild) = &new_grandchildren[0] else {
        panic!()
    };
    assert_eq!(new_first_grandchild.to_values().unwrap()[0], Value::U64(99));

    // A third grandchild beyond this slot's selected window is byte-equal at
    // the root, so the whole rendered tree must be suppressed.
    let mut batch = database.open_batch();
    batch.insert(
        "tree",
        collect_tree_values([4, 10, 20, 999, 999, 999, 999, 1, 1]),
    );
    database.commit_batch(batch).await.unwrap();
    assert!(matches!(subscription.try_recv(), Err(TryRecvError::Empty)));
}

#[futures_test::test]
async fn collect_by_tree_rejects_depth_beyond_descriptor_bound() {
    fn nested(depth: usize) -> CollectBySlotBuilder {
        CollectBySlotBuilder::new(
            ["child"],
            [CollectByField::named("child")],
            "children",
            (depth > 0).then(|| nested(depth - 1)),
            [TopByOrder::asc("child_order")],
            ["child"],
            0,
            TopByLimit::Finite(1),
        )
    }
    let graph = GraphBuilder::collect_by_tree(
        GraphBuilder::table("tree"),
        ["root"],
        [CollectByField::named("root")],
        [CollectBySlotBuilder::new(
            ["root"],
            [CollectByField::named("child")],
            "children",
            [nested(crate::ivm::MAX_COLLECT_BY_TREE_DEPTH)],
            [TopByOrder::asc("child_order")],
            ["child"],
            0,
            TopByLimit::Finite(1),
        )],
    );
    let storage = MemoryStorage::new(&["tree"]).expect("valid memory storage families");
    let mut database = Database::new(collect_tree_schema(), storage).await.unwrap();
    assert!(matches!(
        database.subscribe_one_sink(graph).await,
        Err(Error::IvmRuntime(IvmRuntimeError::InvalidCollectBy(_)))
    ));
}

#[futures_test::test]
async fn collect_by_after_recursive_closure_keeps_recursive_state_outside_limit() {
    async fn run(chain_len: u64) -> (usize, usize, Vec<(Vec<Value>, i64)>) {
        let storage = MemoryStorage::new(&["edges"]).expect("valid memory storage families");
        let mut database = Database::new(edges_schema(), storage).await.unwrap();
        database.set_tick_runtime_stats_enabled(true);
        let subscription = database
            .subscribe_one_sink(reachability_collect_by(1))
            .await
            .unwrap();
        assert!(subscription.recv().unwrap().is_empty());
        let mut batch = database.open_batch();
        for edge in 1..chain_len {
            insert_edge(&mut batch, edge, edge, edge + 1);
        }
        database.commit_batch(batch).await.unwrap();
        let output = subscription.recv().unwrap().to_values().unwrap();
        let stats = &database.last_commit_metrics().unwrap().tick.runtime_stats;
        (
            stats.recursive_accumulated_rows,
            stats.arrangement_rows,
            output,
        )
    }

    let (small_recursive_rows, small_arrangement_rows, small_output) = run(4).await;
    let (large_recursive_rows, large_arrangement_rows, large_output) = run(6).await;
    assert!(
        small_recursive_rows > 1,
        "the collector limit must not cap closure state"
    );
    assert!(large_recursive_rows > small_recursive_rows);
    assert!(large_arrangement_rows > small_arrangement_rows);
    assert!(
        small_output.iter().all(
            |(parent, _)| matches!(parent[1], Value::Array(ref children) if children.len() == 1)
        )
    );
    assert!(
        large_output.iter().all(
            |(parent, _)| matches!(parent[1], Value::Array(ref children) if children.len() == 1)
        )
    );
}
