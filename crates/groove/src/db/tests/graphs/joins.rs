//! Union, projection, join, semi-join, and anti-join maintenance.

use super::*;

#[futures_test::test]
async fn duplicate_table_subscriptions_share_graph_nodes_and_gc_eagerly() {
    let storage = MemoryStorage::new(&["albums"]).expect("valid memory storage families");
    let mut database = Database::new(albums_schema(), storage).await.unwrap();

    let first = database
        .subscribe_one_sink(GraphBuilder::table("albums"))
        .await
        .unwrap();
    let second = database
        .subscribe_one_sink(GraphBuilder::table("albums"))
        .await
        .unwrap();
    let first_output = database
        .ivm_runtime
        .subscription_output_node(first.id())
        .unwrap();
    let second_output = database
        .ivm_runtime
        .subscription_output_node(second.id())
        .unwrap();

    assert_eq!(first_output, second_output);
    assert_eq!(database.ivm_runtime.retained_node_ids().len(), 1);

    assert!(database.unsubscribe(first.id()));
    assert!(database.ivm_runtime.graph().node(first_output).is_some());

    assert!(database.unsubscribe(second.id()));
    assert!(database.ivm_runtime.graph().node(first_output).is_none());
    assert!(database.ivm_runtime.retained_node_ids().is_empty());
}

#[futures_test::test]
async fn commits_do_not_scale_with_unrelated_resident_graph_size() {
    async fn commit_with_unrelated_graphs(graph_count: u64) -> std::time::Duration {
        let storage = MemoryStorage::new(&["albums", "archived_albums"])
            .expect("valid memory storage families");
        let mut database = Database::new(two_album_tables_schema(), storage)
            .await
            .unwrap();
        let mut subscriptions = Vec::with_capacity(graph_count as usize);
        for threshold in 0..graph_count {
            subscriptions.push(
                database
                    .subscribe_one_sink(
                        GraphBuilder::table("archived_albums")
                            .filter(PredicateExpr::gt("id", Value::U64(threshold))),
                    )
                    .await
                    .unwrap(),
            );
        }

        let mut batch = database.open_batch();
        batch.insert(
            "albums",
            vec![Value::U64(1), Value::String("unrelated write".to_owned())],
        );
        database.commit_batch(batch).await.unwrap();
        std::hint::black_box(&subscriptions);
        database.last_commit_metrics().unwrap().ivm_tick_time
    }

    let small = commit_with_unrelated_graphs(1).await;
    let large = commit_with_unrelated_graphs(1_000).await;
    eprintln!(
        "unrelated_graph_commit_scaling_receipt small_us={} large_us={}",
        small.as_micros(),
        large.as_micros()
    );
    assert!(
        large <= small.saturating_mul(10) + std::time::Duration::from_micros(100),
        "an unrelated resident graph must not make commits scale with total runtime size: small={small:?}, large={large:?}"
    );
}

#[futures_test::test]
async fn subscription_install_does_not_sweep_unrelated_resident_graphs() {
    async fn install_with_unrelated_graphs(graph_count: u64) -> std::time::Duration {
        let storage = MemoryStorage::new(&["albums", "archived_albums"])
            .expect("valid memory storage families");
        let mut database = Database::new(two_album_tables_schema(), storage)
            .await
            .unwrap();
        let mut subscriptions = Vec::with_capacity(graph_count as usize + 1);
        for threshold in 0..graph_count {
            subscriptions.push(
                database
                    .subscribe_one_sink(
                        GraphBuilder::table("archived_albums")
                            .filter(PredicateExpr::gt("id", Value::U64(threshold))),
                    )
                    .await
                    .unwrap(),
            );
        }

        let start = Instant::now();
        subscriptions.push(
            database
                .subscribe_one_sink(GraphBuilder::table("albums"))
                .await
                .unwrap(),
        );
        let elapsed = start.elapsed();
        std::hint::black_box(subscriptions);
        elapsed
    }

    let small = install_with_unrelated_graphs(1).await;
    let large = install_with_unrelated_graphs(1_000).await;
    eprintln!(
        "unrelated_graph_subscription_install_scaling_receipt small_us={} large_us={}",
        small.as_micros(),
        large.as_micros()
    );
    assert!(
        large <= small.saturating_mul(10) + std::time::Duration::from_micros(250),
        "subscription install must not sweep the unrelated runtime: small={small:?}, large={large:?}"
    );
}

#[futures_test::test]
async fn union_subscriptions_receive_deltas_from_multiple_tables() {
    let storage =
        MemoryStorage::new(&["albums", "archived_albums"]).expect("valid memory storage families");
    let mut database = Database::new(two_album_tables_schema(), storage)
        .await
        .unwrap();
    let subscription_id = database
        .subscribe_one_sink(GraphBuilder::union([
            GraphBuilder::table("albums"),
            GraphBuilder::table("archived_albums"),
        ]))
        .await
        .unwrap();

    let mut batch = database.open_batch();
    batch.insert(
        "albums",
        vec![Value::U64(1), Value::String("Blue Train".to_owned())],
    );
    batch.insert(
        "archived_albums",
        vec![Value::U64(2), Value::String("Out to Lunch".to_owned())],
    );
    database.commit_batch(batch).await.unwrap();

    assert_eq!(
        expect_recv_vals(&subscription_id)
            .into_iter()
            .map(|(values, _)| values)
            .collect::<Vec<_>>(),
        [
            vec![1_u64.into(), "Blue Train".into()],
            vec![2_u64.into(), "Out to Lunch".into()]
        ]
    );
}

#[futures_test::test]
async fn union_all_subscriptions_preserve_duplicate_derivations() {
    let storage = MemoryStorage::new(&["albums"]).expect("valid memory storage families");
    let mut database = Database::new(albums_schema(), storage).await.unwrap();
    let album_titles = GraphBuilder::table("albums").project(["title"]);
    let subscription_id = database
        .subscribe_one_sink(GraphBuilder::union([album_titles.clone(), album_titles]))
        .await
        .unwrap();

    let mut batch = database.open_batch();
    batch.insert(
        "albums",
        vec![Value::U64(1), Value::String("Blue Train".to_owned())],
    );
    database.commit_batch(batch).await.unwrap();

    assert_eq!(
        expect_recv_vals(&subscription_id),
        [
            (vec!["Blue Train".into()], 1),
            (vec!["Blue Train".into()], 1)
        ]
    );
}

#[futures_test::test]
async fn filter_subscriptions_emit_only_matching_rows() {
    let storage = MemoryStorage::new(&["albums"]).expect("valid memory storage families");
    let mut database = Database::new(albums_schema(), storage).await.unwrap();
    let subscription_id = database
        .subscribe_one_sink(
            GraphBuilder::table("albums").filter(PredicateExpr::gt("id", Value::U64(10))),
        )
        .await
        .unwrap();

    let mut batch = database.open_batch();
    batch.insert(
        "albums",
        vec![Value::U64(7), Value::String("Blue Train".to_owned())],
    );
    batch.insert(
        "albums",
        vec![Value::U64(11), Value::String("Giant Steps".to_owned())],
    );
    database.commit_batch(batch).await.unwrap();

    assert_eq!(
        expect_recv_vals(&subscription_id),
        [(vec![11_u64.into(), "Giant Steps".into()], 1)]
    );
}

#[futures_test::test]
async fn project_subscriptions_emit_projected_records() {
    let storage = MemoryStorage::new(&["albums"]).expect("valid memory storage families");
    let mut database = Database::new(albums_schema(), storage).await.unwrap();
    let subscription_id = database
        .subscribe_one_sink(GraphBuilder::table("albums").project(["title"]))
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
        [(vec!["Blue Train".into()], 1)]
    );
}

#[futures_test::test]
async fn duplicate_projected_subscriptions_share_graph_nodes_and_gc_eagerly() {
    let storage = MemoryStorage::new(&["albums"]).expect("valid memory storage families");
    let mut database = Database::new(albums_schema(), storage).await.unwrap();
    let graph = GraphBuilder::table("albums")
        .filter(PredicateExpr::eq(
            "title",
            Value::String("Blue Train".to_owned()),
        ))
        .project(["title"]);

    let first = database.subscribe_one_sink(graph.clone()).await.unwrap();
    let second = database.subscribe_one_sink(graph).await.unwrap();
    let first_output = database
        .ivm_runtime
        .subscription_output_node(first.id())
        .unwrap();
    let second_output = database
        .ivm_runtime
        .subscription_output_node(second.id())
        .unwrap();

    assert_eq!(first_output, second_output);
    assert!(
        database
            .ivm_runtime
            .retained_node_ids()
            .contains(&first_output)
    );

    assert!(database.unsubscribe(first.id()));
    assert!(database.ivm_runtime.graph().node(first_output).is_some());

    assert!(database.unsubscribe(second.id()));
    assert!(database.ivm_runtime.graph().node(first_output).is_none());
    assert!(database.ivm_runtime.retained_node_ids().is_empty());
}

#[futures_test::test]
async fn join_subscriptions_match_left_deltas_against_maintained_right_state() {
    let storage =
        MemoryStorage::new(&["albums", "artists"]).expect("valid memory storage families");
    let mut database = Database::new(albums_artists_schema(), storage)
        .await
        .unwrap();
    let subscription_id = database
        .subscribe_one_sink(GraphBuilder::join(
            GraphBuilder::table("albums"),
            GraphBuilder::table("artists"),
            ["artist_id"],
            ["id"],
        ))
        .await
        .unwrap();

    let mut batch = database.open_batch();
    batch.insert(
        "artists",
        vec![Value::U64(11), Value::String("John Coltrane".to_owned())],
    );
    database.commit_batch(batch).await.unwrap();
    assert!(subscription_id.recv().unwrap().is_empty());

    let mut batch = database.open_batch();
    batch.insert(
        "albums",
        vec![
            Value::U64(7),
            Value::U64(11),
            Value::String("Blue Train".to_owned()),
        ],
    );
    database.commit_batch(batch).await.unwrap();

    assert_eq!(
        expect_recv_vals(&subscription_id),
        [(
            vec![
                7_u64.into(),
                11_u64.into(),
                "Blue Train".into(),
                11_u64.into(),
                "John Coltrane".into(),
            ],
            1
        )]
    );
}

#[futures_test::test]
async fn join_subscriptions_match_array_key_elements() {
    let storage =
        MemoryStorage::new(&["files", "file_parts"]).expect("valid memory storage families");
    let mut database = Database::new(files_parts_schema(), storage).await.unwrap();
    let subscription_id = database
        .subscribe_one_sink(GraphBuilder::join(
            GraphBuilder::table("files"),
            GraphBuilder::table("file_parts"),
            ["part_ids"],
            ["part_uuid"],
        ))
        .await
        .unwrap();

    let part_a = uuid(0xa);
    let part_b = uuid(0xb);
    let part_c = uuid(0xc);

    let mut batch = database.open_batch();
    batch.insert(
        "files",
        vec![
            Value::U64(1),
            Value::Array(vec![Value::Uuid(part_a), Value::Uuid(part_b)]),
        ],
    );
    batch.insert(
        "file_parts",
        vec![
            Value::U64(10),
            Value::Uuid(part_b),
            Value::Bytes(b"b".to_vec()),
        ],
    );
    batch.insert(
        "file_parts",
        vec![
            Value::U64(11),
            Value::Uuid(part_c),
            Value::Bytes(b"c".to_vec()),
        ],
    );
    database.commit_batch(batch).await.unwrap();

    assert_eq!(
        expect_recv_vals(&subscription_id),
        [(
            vec![
                Value::U64(1),
                Value::Array(vec![Value::Uuid(part_a), Value::Uuid(part_b)]),
                Value::U64(10),
                Value::Uuid(part_b),
                Value::Bytes(b"b".to_vec()),
            ],
            1
        )]
    );
}

#[futures_test::test]
async fn unnest_subscription_emits_one_row_per_array_element() {
    let storage =
        MemoryStorage::new(&["files", "file_parts"]).expect("valid memory storage families");
    let mut database = Database::new(files_parts_schema(), storage).await.unwrap();
    let subscription = database
        .subscribe_one_sink(
            GraphBuilder::table("files")
                .unnest("part_ids", "part_id")
                .project(["id", "part_id"]),
        )
        .await
        .unwrap();

    let part_a = uuid(0xa);
    let part_b = uuid(0xb);
    let part_c = uuid(0xc);

    let mut batch = database.open_batch();
    batch.insert(
        "files",
        vec![
            Value::U64(1),
            Value::Array(vec![Value::Uuid(part_a), Value::Uuid(part_b)]),
        ],
    );
    database.commit_batch(batch).await.unwrap();

    assert_eq!(
        expect_recv_vals(&subscription),
        [
            (vec![Value::U64(1), Value::Uuid(part_a)], 1),
            (vec![Value::U64(1), Value::Uuid(part_b)], 1),
        ]
    );

    let mut batch = database.open_batch();
    batch.delete("files", PrimaryKeyValue::U64(1));
    batch.insert(
        "files",
        vec![
            Value::U64(1),
            Value::Array(vec![Value::Uuid(part_b), Value::Uuid(part_c)]),
        ],
    );
    database.commit_batch(batch).await.unwrap();

    assert_eq!(
        expect_recv_vals(&subscription),
        [
            (vec![Value::U64(1), Value::Uuid(part_a)], -1),
            (vec![Value::U64(1), Value::Uuid(part_b)], -1),
            (vec![Value::U64(1), Value::Uuid(part_b)], 1),
            (vec![Value::U64(1), Value::Uuid(part_c)], 1),
        ]
    );
}

#[futures_test::test]
async fn join_subscriptions_match_persisted_array_key_elements() {
    let storage =
        MemoryStorage::new(&["files", "file_parts"]).expect("valid memory storage families");
    let mut database = Database::new(files_parts_schema(), storage).await.unwrap();
    let subscription_id = database
        .subscribe_one_sink(GraphBuilder::join(
            GraphBuilder::table("files"),
            GraphBuilder::table("file_parts"),
            ["part_ids"],
            ["part_uuid"],
        ))
        .await
        .unwrap();

    let part_a = uuid(0xa);
    let part_b = uuid(0xb);
    let part_c = uuid(0xc);

    let mut batch = database.open_batch();
    batch.insert(
        "files",
        vec![
            Value::U64(1),
            Value::Array(vec![
                Value::Uuid(part_b),
                Value::Uuid(part_b),
                Value::Uuid(part_a),
            ]),
        ],
    );
    batch.insert("files", vec![Value::U64(2), Value::Array(vec![])]);
    database.commit_batch(batch).await.unwrap();
    assert!(subscription_id.recv().unwrap().is_empty());

    let mut batch = database.open_batch();
    batch.insert(
        "file_parts",
        vec![
            Value::U64(10),
            Value::Uuid(part_b),
            Value::Bytes(b"b".to_vec()),
        ],
    );
    batch.insert(
        "file_parts",
        vec![
            Value::U64(11),
            Value::Uuid(part_a),
            Value::Bytes(b"a".to_vec()),
        ],
    );
    batch.insert(
        "file_parts",
        vec![
            Value::U64(12),
            Value::Uuid(part_c),
            Value::Bytes(b"c".to_vec()),
        ],
    );
    database.commit_batch(batch).await.unwrap();

    assert_eq!(
        expect_recv_vals(&subscription_id),
        [
            (
                vec![
                    Value::U64(1),
                    Value::Array(vec![
                        Value::Uuid(part_b),
                        Value::Uuid(part_b),
                        Value::Uuid(part_a),
                    ]),
                    Value::U64(10),
                    Value::Uuid(part_b),
                    Value::Bytes(b"b".to_vec()),
                ],
                1,
            ),
            (
                vec![
                    Value::U64(1),
                    Value::Array(vec![
                        Value::Uuid(part_b),
                        Value::Uuid(part_b),
                        Value::Uuid(part_a),
                    ]),
                    Value::U64(11),
                    Value::Uuid(part_a),
                    Value::Bytes(b"a".to_vec()),
                ],
                1,
            ),
        ]
    );
}

#[futures_test::test]
async fn join_subscriptions_match_nullable_array_key_elements() {
    let storage =
        MemoryStorage::new(&["files", "file_parts"]).expect("valid memory storage families");
    let mut database = Database::new(nullable_files_parts_schema(), storage)
        .await
        .unwrap();
    let subscription_id = database
        .subscribe_one_sink(GraphBuilder::join(
            GraphBuilder::table("files"),
            GraphBuilder::table("file_parts"),
            ["part_ids"],
            ["part_uuid"],
        ))
        .await
        .unwrap();

    let part_a = uuid(0xa);
    let part_b = uuid(0xb);

    let mut batch = database.open_batch();
    batch.insert(
        "files",
        vec![
            Value::U64(1),
            Value::Nullable(Some(Box::new(Value::Array(vec![
                Value::Uuid(part_a),
                Value::Uuid(part_b),
            ])))),
        ],
    );
    batch.insert(
        "file_parts",
        vec![
            Value::U64(10),
            Value::Nullable(Some(Box::new(Value::Uuid(part_b)))),
            Value::Bytes(b"b".to_vec()),
        ],
    );
    database.commit_batch(batch).await.unwrap();

    assert_eq!(
        expect_recv_vals(&subscription_id),
        [(
            vec![
                Value::U64(1),
                Value::Nullable(Some(Box::new(Value::Array(vec![
                    Value::Uuid(part_a),
                    Value::Uuid(part_b),
                ])))),
                Value::U64(10),
                Value::Nullable(Some(Box::new(Value::Uuid(part_b)))),
                Value::Bytes(b"b".to_vec()),
            ],
            1
        )]
    );
}

#[futures_test::test]
async fn index_subscriptions_expand_array_key_elements() {
    let storage = MemoryStorage::new(&["files", "indices"]).expect("valid memory storage families");
    let mut database = Database::new(indexed_files_schema(), storage)
        .await
        .unwrap();
    let subscription = database
        .subscribe_one_sink(GraphBuilder::index("files", "files_by_part_ids"))
        .await
        .unwrap();

    let part_a = uuid(0xa);
    let part_b = uuid(0xb);
    let mut batch = database.open_batch();
    batch.insert(
        "files",
        vec![
            Value::U64(1),
            Value::Array(vec![Value::Uuid(part_b), Value::Uuid(part_a)]),
        ],
    );
    database.commit_batch(batch).await.unwrap();

    assert_eq!(
        expect_recv_vals(&subscription),
        [
            (
                vec![
                    encoded_uuid_index_key(part_a, 1).into(),
                    Vec::<u8>::new().into(),
                ],
                1,
            ),
            (
                vec![
                    encoded_uuid_index_key(part_b, 1).into(),
                    Vec::<u8>::new().into(),
                ],
                1,
            ),
        ]
    );
}

#[futures_test::test]
async fn query_graph_joins_related_tables_through_database_facade() {
    let storage =
        MemoryStorage::new(&["albums", "artists"]).expect("valid memory storage families");
    let mut database = Database::new(albums_artists_schema(), storage)
        .await
        .unwrap();

    let mut batch = database.open_batch();
    batch.insert(
        "artists",
        vec![Value::U64(1), Value::String("John Coltrane".to_owned())],
    );
    batch.insert(
        "artists",
        vec![Value::U64(2), Value::String("Miles Davis".to_owned())],
    );
    batch.insert(
        "albums",
        vec![
            Value::U64(10),
            Value::U64(1),
            Value::String("Blue Train".to_owned()),
        ],
    );
    batch.insert(
        "albums",
        vec![
            Value::U64(11),
            Value::U64(2),
            Value::String("Kind of Blue".to_owned()),
        ],
    );
    batch.insert(
        "albums",
        vec![
            Value::U64(12),
            Value::U64(1),
            Value::String("Giant Steps".to_owned()),
        ],
    );
    database.commit_batch(batch).await.unwrap();

    let rows = database
        .query_graph(
            GraphBuilder::join(
                GraphBuilder::table("albums"),
                GraphBuilder::table("artists"),
                ["artist_id"],
                ["id"],
            )
            .project_fields([
                ProjectField::renamed("right.name", "artist"),
                ProjectField::renamed("left.title", "album"),
            ]),
        )
        .await
        .unwrap();

    let mut values = rows.to_values().unwrap();
    values.sort_by(|left, right| format!("{left:?}").cmp(&format!("{right:?}")));
    assert_eq!(
        values,
        [
            (
                vec![
                    Value::String("John Coltrane".to_owned()),
                    Value::String("Blue Train".to_owned()),
                ],
                1,
            ),
            (
                vec![
                    Value::String("John Coltrane".to_owned()),
                    Value::String("Giant Steps".to_owned()),
                ],
                1,
            ),
            (
                vec![
                    Value::String("Miles Davis".to_owned()),
                    Value::String("Kind of Blue".to_owned()),
                ],
                1,
            ),
        ]
    );
}

#[futures_test::test]
async fn join_subscriptions_match_right_deltas_against_maintained_left_state() {
    let storage =
        MemoryStorage::new(&["albums", "artists"]).expect("valid memory storage families");
    let mut database = Database::new(albums_artists_schema(), storage)
        .await
        .unwrap();
    let subscription_id = database
        .subscribe_one_sink(GraphBuilder::join(
            GraphBuilder::table("albums"),
            GraphBuilder::table("artists"),
            ["artist_id"],
            ["id"],
        ))
        .await
        .unwrap();

    let mut batch = database.open_batch();
    batch.insert(
        "albums",
        vec![
            Value::U64(7),
            Value::U64(11),
            Value::String("Blue Train".to_owned()),
        ],
    );
    database.commit_batch(batch).await.unwrap();
    assert!(subscription_id.recv().unwrap().is_empty());

    let mut batch = database.open_batch();
    batch.insert(
        "artists",
        vec![Value::U64(11), Value::String("John Coltrane".to_owned())],
    );
    database.commit_batch(batch).await.unwrap();

    assert_eq!(
        expect_recv_vals(&subscription_id),
        [(
            vec![
                7_u64.into(),
                11_u64.into(),
                "Blue Train".into(),
                11_u64.into(),
                "John Coltrane".into(),
            ],
            1
        )]
    );
}

#[futures_test::test]
async fn join_subscriptions_emit_update_and_delete_deltas_from_maintained_state() {
    let storage =
        MemoryStorage::new(&["albums", "artists"]).expect("valid memory storage families");
    let mut database = Database::new(albums_artists_schema(), storage)
        .await
        .unwrap();
    let subscription_id = database
        .subscribe_one_sink(GraphBuilder::join(
            GraphBuilder::table("albums"),
            GraphBuilder::table("artists"),
            ["artist_id"],
            ["id"],
        ))
        .await
        .unwrap();

    let mut batch = database.open_batch();
    batch.insert(
        "artists",
        vec![Value::U64(11), Value::String("John Coltrane".to_owned())],
    );
    batch.insert(
        "albums",
        vec![
            Value::U64(7),
            Value::U64(11),
            Value::String("Blue Train".to_owned()),
        ],
    );
    database.commit_batch(batch).await.unwrap();
    let _initial_join = expect_recv_vals(&subscription_id);

    let mut batch = database.open_batch();
    batch.update(
        "albums",
        vec![
            Value::U64(7),
            Value::U64(11),
            Value::String("Giant Steps".to_owned()),
        ],
    );
    database.commit_batch(batch).await.unwrap();

    let deltas = expect_recv_vals(&subscription_id);
    assert_eq!(deltas.len(), 2);
    assert!(deltas.contains(&(
        vec![
            7_u64.into(),
            11_u64.into(),
            "Blue Train".into(),
            11_u64.into(),
            "John Coltrane".into(),
        ],
        -1
    )));
    assert!(deltas.contains(&(
        vec![
            7_u64.into(),
            11_u64.into(),
            "Giant Steps".into(),
            11_u64.into(),
            "John Coltrane".into(),
        ],
        1
    )));

    let mut batch = database.open_batch();
    batch.delete("artists", PrimaryKeyValue::U64(11));
    database.commit_batch(batch).await.unwrap();
    assert_eq!(
        expect_recv_vals(&subscription_id),
        [(
            vec![
                7_u64.into(),
                11_u64.into(),
                "Giant Steps".into(),
                11_u64.into(),
                "John Coltrane".into(),
            ],
            -1
        )]
    );
}

#[futures_test::test]
async fn anti_join_subscriptions_emit_left_rows_without_right_matches() {
    let storage =
        MemoryStorage::new(&["albums", "artists"]).expect("valid memory storage families");
    let mut database = Database::new(albums_artists_schema(), storage)
        .await
        .unwrap();
    let subscription = database
        .subscribe_one_sink(GraphBuilder::anti_join(
            GraphBuilder::table("albums"),
            GraphBuilder::table("artists"),
            ["artist_id"],
            ["id"],
        ))
        .await
        .unwrap();

    let mut batch = database.open_batch();
    batch.insert(
        "albums",
        vec![
            Value::U64(7),
            Value::U64(11),
            Value::String("Blue Train".to_owned()),
        ],
    );
    database.commit_batch(batch).await.unwrap();

    assert_eq!(
        expect_recv_vals(&subscription),
        [(vec![7_u64.into(), 11_u64.into(), "Blue Train".into()], 1)]
    );
}

#[futures_test::test]
async fn semi_join_subscriptions_emit_left_rows_with_right_matches() {
    let storage =
        MemoryStorage::new(&["albums", "artists"]).expect("valid memory storage families");
    let mut database = Database::new(albums_artists_schema(), storage)
        .await
        .unwrap();
    let subscription = database
        .subscribe_one_sink(GraphBuilder::semi_join(
            GraphBuilder::table("albums"),
            GraphBuilder::table("artists"),
            ["artist_id"],
            ["id"],
        ))
        .await
        .unwrap();
    assert!(subscription.recv().unwrap().is_empty());

    let mut batch = database.open_batch();
    batch.insert(
        "albums",
        vec![
            Value::U64(7),
            Value::U64(11),
            Value::String("Blue Train".to_owned()),
        ],
    );
    database.commit_batch(batch).await.unwrap();
    assert!(subscription.try_recv().is_err());

    let mut batch = database.open_batch();
    batch.insert(
        "artists",
        vec![Value::U64(11), Value::String("John Coltrane".to_owned())],
    );
    database.commit_batch(batch).await.unwrap();

    assert_eq!(
        expect_recv_vals(&subscription),
        [(vec![7_u64.into(), 11_u64.into(), "Blue Train".into()], 1)]
    );
}

#[futures_test::test]
async fn semi_join_retracts_and_restores_on_right_threshold_transitions() {
    let storage =
        MemoryStorage::new(&["albums", "artists"]).expect("valid memory storage families");
    let mut database = Database::new(albums_artists_schema(), storage)
        .await
        .unwrap();
    let subscription = database
        .subscribe_one_sink(GraphBuilder::semi_join(
            GraphBuilder::table("albums"),
            GraphBuilder::table("artists"),
            ["artist_id"],
            ["id"],
        ))
        .await
        .unwrap();
    assert!(subscription.recv().unwrap().is_empty());

    let mut batch = database.open_batch();
    batch.insert(
        "artists",
        vec![Value::U64(11), Value::String("John Coltrane".to_owned())],
    );
    database.commit_batch(batch).await.unwrap();
    assert!(subscription.try_recv().is_err());

    let mut batch = database.open_batch();
    batch.insert(
        "albums",
        vec![
            Value::U64(7),
            Value::U64(11),
            Value::String("Blue Train".to_owned()),
        ],
    );
    database.commit_batch(batch).await.unwrap();
    assert_eq!(
        expect_recv_vals(&subscription),
        [(vec![7_u64.into(), 11_u64.into(), "Blue Train".into()], 1)]
    );

    let mut batch = database.open_batch();
    batch.delete("artists", PrimaryKeyValue::U64(11));
    database.commit_batch(batch).await.unwrap();
    assert_eq!(
        expect_recv_vals(&subscription),
        [(vec![7_u64.into(), 11_u64.into(), "Blue Train".into()], -1)]
    );
}

#[futures_test::test]
async fn semi_join_hydration_snapshot_filters_missing_right_matches() {
    let storage =
        MemoryStorage::new(&["albums", "artists"]).expect("valid memory storage families");
    let mut database = Database::new(albums_artists_schema(), storage)
        .await
        .unwrap();
    let mut batch = database.open_batch();
    batch.insert(
        "albums",
        vec![
            Value::U64(7),
            Value::U64(11),
            Value::String("Blue Train".to_owned()),
        ],
    );
    batch.insert(
        "albums",
        vec![
            Value::U64(8),
            Value::U64(12),
            Value::String("Unknown Session".to_owned()),
        ],
    );
    batch.insert(
        "artists",
        vec![Value::U64(11), Value::String("John Coltrane".to_owned())],
    );
    database.commit_batch(batch).await.unwrap();

    let subscription = database
        .subscribe_one_sink(GraphBuilder::semi_join(
            GraphBuilder::table("albums"),
            GraphBuilder::table("artists"),
            ["artist_id"],
            ["id"],
        ))
        .await
        .unwrap();

    assert_eq!(
        expect_recv_vals(&subscription),
        [(vec![7_u64.into(), 11_u64.into(), "Blue Train".into()], 1)]
    );
}

#[futures_test::test]
async fn anti_join_retracts_and_restores_on_right_threshold_transitions() {
    let storage =
        MemoryStorage::new(&["albums", "artists"]).expect("valid memory storage families");
    let mut database = Database::new(albums_artists_schema(), storage)
        .await
        .unwrap();
    let subscription = database
        .subscribe_one_sink(GraphBuilder::anti_join(
            GraphBuilder::table("albums"),
            GraphBuilder::table("artists"),
            ["artist_id"],
            ["id"],
        ))
        .await
        .unwrap();

    let mut batch = database.open_batch();
    batch.insert(
        "albums",
        vec![
            Value::U64(7),
            Value::U64(11),
            Value::String("Blue Train".to_owned()),
        ],
    );
    database.commit_batch(batch).await.unwrap();
    assert_eq!(expect_recv_vals(&subscription)[0].1, 1);

    let mut batch = database.open_batch();
    batch.insert(
        "artists",
        vec![Value::U64(11), Value::String("John Coltrane".to_owned())],
    );
    database.commit_batch(batch).await.unwrap();
    assert_eq!(
        expect_recv_vals(&subscription),
        [(vec![7_u64.into(), 11_u64.into(), "Blue Train".into()], -1)]
    );

    let mut batch = database.open_batch();
    batch.delete("artists", PrimaryKeyValue::U64(11));
    database.commit_batch(batch).await.unwrap();
    assert_eq!(
        expect_recv_vals(&subscription),
        [(vec![7_u64.into(), 11_u64.into(), "Blue Train".into()], 1)]
    );
}

#[futures_test::test]
async fn anti_join_only_changes_when_right_count_crosses_zero() {
    let storage = MemoryStorage::new(&["albums", "blocks"]).expect("valid memory storage families");
    let mut database = Database::new(albums_blockers_schema(), storage)
        .await
        .unwrap();
    let subscription = database
        .subscribe_one_sink(GraphBuilder::anti_join(
            GraphBuilder::table("albums"),
            GraphBuilder::table("blocks"),
            ["artist_id"],
            ["artist_id"],
        ))
        .await
        .unwrap();

    let mut batch = database.open_batch();
    batch.insert(
        "albums",
        vec![
            Value::U64(7),
            Value::U64(11),
            Value::String("Blue Train".to_owned()),
        ],
    );
    database.commit_batch(batch).await.unwrap();
    assert_eq!(expect_recv_vals(&subscription)[0].1, 1);

    let mut batch = database.open_batch();
    batch.insert("blocks", vec![Value::U64(1), Value::U64(11)]);
    batch.insert("blocks", vec![Value::U64(2), Value::U64(11)]);
    database.commit_batch(batch).await.unwrap();
    assert_eq!(
        expect_recv_vals(&subscription),
        [(vec![7_u64.into(), 11_u64.into(), "Blue Train".into()], -1)]
    );

    let mut batch = database.open_batch();
    batch.delete("blocks", PrimaryKeyValue::U64(1));
    database.commit_batch(batch).await.unwrap();
    assert!(subscription.try_recv().is_err());

    let mut batch = database.open_batch();
    batch.delete("blocks", PrimaryKeyValue::U64(2));
    database.commit_batch(batch).await.unwrap();
    assert_eq!(
        expect_recv_vals(&subscription),
        [(vec![7_u64.into(), 11_u64.into(), "Blue Train".into()], 1)]
    );
}

#[futures_test::test]
async fn anti_join_hydration_snapshot_filters_existing_right_matches() {
    let storage =
        MemoryStorage::new(&["albums", "artists"]).expect("valid memory storage families");
    let mut database = Database::new(albums_artists_schema(), storage)
        .await
        .unwrap();
    let mut batch = database.open_batch();
    batch.insert(
        "albums",
        vec![
            Value::U64(7),
            Value::U64(11),
            Value::String("Blue Train".to_owned()),
        ],
    );
    batch.insert(
        "albums",
        vec![
            Value::U64(8),
            Value::U64(12),
            Value::String("Unknown Session".to_owned()),
        ],
    );
    batch.insert(
        "artists",
        vec![Value::U64(11), Value::String("John Coltrane".to_owned())],
    );
    database.commit_batch(batch).await.unwrap();

    let subscription = database
        .subscribe_one_sink(GraphBuilder::anti_join(
            GraphBuilder::table("albums"),
            GraphBuilder::table("artists"),
            ["artist_id"],
            ["id"],
        ))
        .await
        .unwrap();

    assert_eq!(
        expect_recv_vals(&subscription),
        [(
            vec![8_u64.into(), 12_u64.into(), "Unknown Session".into()],
            1
        )]
    );
}

#[futures_test::test]
async fn anti_join_filters_identical_descriptors_before_projection() {
    let storage =
        MemoryStorage::new(&["edges", "blockers"]).expect("valid memory storage families");
    let mut database = Database::new(edges_blockers_schema(), storage)
        .await
        .unwrap();

    let mut batch = database.open_batch();
    batch.insert("edges", vec![Value::U64(1), Value::U64(4), Value::U64(3)]);
    batch.insert(
        "blockers",
        vec![Value::U64(5), Value::U64(4), Value::U64(3)],
    );
    batch.insert("edges", vec![Value::U64(2), Value::U64(8), Value::U64(4)]);
    database.commit_batch(batch).await.unwrap();

    let subscription = database
        .subscribe_one_sink(unblocked_edges_graph())
        .await
        .unwrap();
    assert_eq!(
        database
            .next_subscription(&subscription)
            .await
            .unwrap()
            .to_values()
            .unwrap(),
        [(vec![Value::U64(8), Value::U64(4)], 1)]
    );
}

#[futures_test::test]
async fn anti_join_hydration_snapshot_filters_many_existing_identical_descriptor_blockers() {
    let storage =
        MemoryStorage::new(&["edges", "blockers"]).expect("valid memory storage families");
    let mut database = Database::new(edges_blockers_schema(), storage)
        .await
        .unwrap();

    let edges = [
        (1, 8, 4),
        (3, 2, 5),
        (4, 4, 3),
        (9, 4, 7),
        (10, 6, 2),
        (11, 4, 8),
        (18, 6, 3),
        (19, 8, 1),
        (20, 7, 6),
    ];
    let blockers = [
        (5, 4, 3),
        (6, 6, 3),
        (7, 2, 3),
        (9, 3, 3),
        (13, 8, 1),
        (17, 1, 2),
        (21, 7, 1),
        (22, 2, 2),
    ];
    let mut batch = database.open_batch();
    for (id, src, dst) in edges {
        batch.insert(
            "edges",
            vec![Value::U64(id), Value::U64(src), Value::U64(dst)],
        );
    }
    for (id, src, dst) in blockers {
        batch.insert(
            "blockers",
            vec![Value::U64(id), Value::U64(src), Value::U64(dst)],
        );
    }
    database.commit_batch(batch).await.unwrap();

    let subscription = database
        .subscribe_one_sink(unblocked_edges_graph())
        .await
        .unwrap();
    assert_eq!(
        expect_recv_vals(&subscription),
        [
            (vec![Value::U64(2), Value::U64(5)], 1),
            (vec![Value::U64(4), Value::U64(7)], 1),
            (vec![Value::U64(4), Value::U64(8)], 1),
            (vec![Value::U64(6), Value::U64(2)], 1),
            (vec![Value::U64(7), Value::U64(6)], 1),
            (vec![Value::U64(8), Value::U64(4)], 1),
        ]
    );
}

#[futures_test::test]
async fn anti_join_retracts_identical_descriptor_projection_when_blocker_arrives() {
    let storage =
        MemoryStorage::new(&["edges", "blockers"]).expect("valid memory storage families");
    let mut database = Database::new(edges_blockers_schema(), storage)
        .await
        .unwrap();

    let mut batch = database.open_batch();
    batch.insert("edges", vec![Value::U64(1), Value::U64(4), Value::U64(3)]);
    database.commit_batch(batch).await.unwrap();

    let subscription = database
        .subscribe_one_sink(unblocked_edges_graph())
        .await
        .unwrap();
    assert_eq!(
        expect_recv_vals(&subscription),
        [(vec![Value::U64(4), Value::U64(3)], 1)]
    );

    let mut batch = database.open_batch();
    batch.insert(
        "blockers",
        vec![Value::U64(5), Value::U64(4), Value::U64(3)],
    );
    database.commit_batch(batch).await.unwrap();

    assert_eq!(
        expect_recv_vals(&subscription),
        [(vec![Value::U64(4), Value::U64(3)], -1)]
    );
}

#[futures_test::test]
async fn anti_join_remembers_blocker_inserted_before_matching_left_key_exists() {
    let storage =
        MemoryStorage::new(&["edges", "blockers"]).expect("valid memory storage families");
    let mut database = Database::new(edges_blockers_schema(), storage)
        .await
        .unwrap();

    let mut batch = database.open_batch();
    batch.insert("edges", vec![Value::U64(1), Value::U64(8), Value::U64(4)]);
    database.commit_batch(batch).await.unwrap();

    let subscription = database
        .subscribe_one_sink(unblocked_edges_graph())
        .await
        .unwrap();
    assert_eq!(
        expect_recv_vals(&subscription),
        [(vec![Value::U64(8), Value::U64(4)], 1)]
    );

    let mut batch = database.open_batch();
    batch.insert(
        "blockers",
        vec![Value::U64(5), Value::U64(4), Value::U64(3)],
    );
    database.commit_batch(batch).await.unwrap();
    assert!(subscription.try_recv().is_err());

    let mut batch = database.open_batch();
    batch.update("edges", vec![Value::U64(1), Value::U64(4), Value::U64(3)]);
    database.commit_batch(batch).await.unwrap();

    assert_eq!(
        expect_recv_vals(&subscription),
        [(vec![Value::U64(8), Value::U64(4)], -1)]
    );
}

#[futures_test::test]
async fn anti_join_retracts_when_right_update_moves_onto_left_key() {
    let storage =
        MemoryStorage::new(&["edges", "blockers"]).expect("valid memory storage families");
    let mut database = Database::new(edges_blockers_schema(), storage)
        .await
        .unwrap();

    let mut batch = database.open_batch();
    batch.insert("edges", vec![Value::U64(4), Value::U64(4), Value::U64(3)]);
    batch.insert(
        "blockers",
        vec![Value::U64(5), Value::U64(6), Value::U64(8)],
    );
    database.commit_batch(batch).await.unwrap();

    let subscription = database
        .subscribe_one_sink(unblocked_edges_graph())
        .await
        .unwrap();
    assert_eq!(
        expect_recv_vals(&subscription),
        [(vec![Value::U64(4), Value::U64(3)], 1)]
    );

    let mut batch = database.open_batch();
    batch.update(
        "blockers",
        vec![Value::U64(5), Value::U64(4), Value::U64(3)],
    );
    database.commit_batch(batch).await.unwrap();

    assert_eq!(
        expect_recv_vals(&subscription),
        [(vec![Value::U64(4), Value::U64(3)], -1)]
    );
}

#[futures_test::test]
async fn anti_join_resubscribe_hydrates_from_storage_after_unretained_changes() {
    let storage =
        MemoryStorage::new(&["edges", "blockers"]).expect("valid memory storage families");
    let mut database = Database::new(edges_blockers_schema(), storage)
        .await
        .unwrap();

    let mut batch = database.open_batch();
    batch.insert("edges", vec![Value::U64(1), Value::U64(4), Value::U64(3)]);
    database.commit_batch(batch).await.unwrap();

    let subscription = database
        .subscribe_one_sink(unblocked_edges_graph())
        .await
        .unwrap();
    assert_eq!(
        expect_recv_vals(&subscription),
        [(vec![Value::U64(4), Value::U64(3)], 1)]
    );
    assert!(database.unsubscribe(subscription.id()));

    let mut batch = database.open_batch();
    batch.insert(
        "blockers",
        vec![Value::U64(5), Value::U64(4), Value::U64(3)],
    );
    database.commit_batch(batch).await.unwrap();

    let subscription = database
        .subscribe_one_sink(unblocked_edges_graph())
        .await
        .unwrap();
    assert!(subscription.recv().unwrap().is_empty());
}
