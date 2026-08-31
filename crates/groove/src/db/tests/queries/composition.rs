//! Composition of query operators with joins and prepared bindings.

use super::*;

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
            GraphBuilder::table("history"),
            GraphBuilder::table("history_shadow"),
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
async fn recursive_arg_max_by_uses_declared_order_and_preserves_exact_ties() {
    let storage = MemoryStorage::new(&["history", "history_shadow"]).unwrap();
    let mut database = Database::new(two_history_tables_schema(), storage)
        .await
        .unwrap();
    let mut batch = database.open_batch();
    batch.insert("history", history_values(1, 20, 1, "tied-a"));
    batch.insert("history_shadow", history_values(1, 20, 1, "tied-z"));
    database.commit_batch(batch).await.unwrap();

    let output = RecordDescriptor::new([
        ("row", ColumnType::U64.clone()),
        ("title", ColumnType::String.clone()),
        ("stamp", ColumnType::U64.clone()),
        ("node", ColumnType::U64.clone()),
    ]);
    let seed = GraphBuilder::arg_max_by(
        GraphBuilder::union([
            GraphBuilder::table("history"),
            GraphBuilder::table("history_shadow"),
        ])
        .project(["row", "title", "stamp", "node"]),
        ["row"],
        ["stamp", "node"],
    );
    let graph = GraphBuilder::recursive(
        seed,
        GraphBuilder::frontier_source("frontier", output),
        "frontier",
        4,
    );

    assert_eq!(
        database
            .query_graph(graph)
            .await
            .unwrap()
            .to_values()
            .unwrap(),
        [(
            vec![
                Value::U64(1),
                Value::String("tied-a".to_owned()),
                Value::U64(20),
                Value::U64(1),
            ],
            1,
        )]
    );
}

#[futures_test::test]
async fn arg_min_by_reordered_projection_preserves_declared_order_on_retraction() {
    let storage = MemoryStorage::new(&["history", "history_shadow"]).unwrap();
    let mut database = Database::new(two_history_tables_schema(), storage)
        .await
        .unwrap();
    let input = GraphBuilder::union([
        GraphBuilder::table("history"),
        GraphBuilder::table("history_shadow"),
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
async fn arg_max_by_rejects_unsupported_inputs_and_bad_primary_keys() {
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

    database
        .subscribe_one_sink(GraphBuilder::recursive(
            history_arg_max().project(["row", "stamp"]),
            GraphBuilder::frontier_source(
                "frontier",
                RecordDescriptor::new([
                    ("row", ColumnType::U64.clone()),
                    ("stamp", ColumnType::U64.clone()),
                ]),
            ),
            "frontier",
            4,
        ))
        .await
        .unwrap();
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
