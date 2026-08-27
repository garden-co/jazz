//! Parameterized shapes, binding validation, and literal-query parity.

use super::*;

#[futures_test::test]
async fn parameterized_shape_hydrates_and_routes_by_param() {
    let storage =
        MemoryStorage::new(&["albums", "artists"]).expect("valid memory storage families");
    let mut database = Database::new(albums_artists_schema(), storage)
        .await
        .unwrap();

    let mut batch = database.open_batch();
    batch.insert(
        "albums",
        vec![
            Value::U64(1),
            Value::U64(7),
            Value::String("Blue Train".to_owned()),
        ],
    );
    batch.insert(
        "albums",
        vec![
            Value::U64(2),
            Value::U64(8),
            Value::String("Kind of Blue".to_owned()),
        ],
    );
    database.commit_batch(batch).await.unwrap();

    let shape = database
        .prepare_one_sink(
            artist_album_shape_graph(),
            "artist_params",
            artist_binding_descriptor(),
            ["artist_id"],
        )
        .await
        .unwrap();
    let coltrane = database
        .bind_shape_one_sink(shape.id(), &[Value::U64(7)])
        .await
        .unwrap();
    let miles = database
        .bind_shape_one_sink(shape.id(), &[Value::U64(8)])
        .await
        .unwrap();

    assert_eq!(
        expect_try_recv_vals(&coltrane),
        vec![(
            vec![
                Value::U64(7),
                Value::U64(1),
                Value::String("Blue Train".to_owned())
            ],
            1
        )]
    );
    assert_eq!(
        expect_try_recv_vals(&miles),
        vec![(
            vec![
                Value::U64(8),
                Value::U64(2),
                Value::String("Kind of Blue".to_owned())
            ],
            1
        )]
    );

    let mut batch = database.open_batch();
    batch.insert(
        "albums",
        vec![
            Value::U64(3),
            Value::U64(7),
            Value::String("Giant Steps".to_owned()),
        ],
    );
    database.commit_batch(batch).await.unwrap();

    assert_eq!(
        expect_try_recv_vals(&coltrane),
        vec![(
            vec![
                Value::U64(7),
                Value::U64(3),
                Value::String("Giant Steps".to_owned())
            ],
            1
        )]
    );
    assert!(miles.try_recv().is_err());
}

#[futures_test::test]
async fn parameterized_shape_uses_set_semantics_with_duplicate_param_refcounts() {
    let storage =
        MemoryStorage::new(&["albums", "artists"]).expect("valid memory storage families");
    let mut database = Database::new(albums_artists_schema(), storage)
        .await
        .unwrap();

    let mut batch = database.open_batch();
    batch.insert(
        "albums",
        vec![
            Value::U64(1),
            Value::U64(7),
            Value::String("Blue Train".to_owned()),
        ],
    );
    database.commit_batch(batch).await.unwrap();

    let shape = database
        .prepare_one_sink(
            artist_album_shape_graph(),
            "artist_params",
            artist_binding_descriptor(),
            ["artist_id"],
        )
        .await
        .unwrap();
    let first = database
        .bind_shape_one_sink(shape.id(), &[Value::U64(7)])
        .await
        .unwrap();
    let second = database
        .bind_shape_one_sink(shape.id(), &[Value::U64(7)])
        .await
        .unwrap();

    assert_eq!(expect_try_recv_vals(&first).len(), 1);
    assert_eq!(expect_try_recv_vals(&second).len(), 1);

    let mut batch = database.open_batch();
    batch.insert(
        "albums",
        vec![
            Value::U64(2),
            Value::U64(7),
            Value::String("Giant Steps".to_owned()),
        ],
    );
    database.commit_batch(batch).await.unwrap();

    let first_delta = expect_try_recv_vals(&first);
    let second_delta = expect_try_recv_vals(&second);
    assert_eq!(first_delta, second_delta);
    assert_eq!(first_delta[0].1, 1);

    assert!(database.unsubscribe(first.id()));
    assert!(second.try_recv().is_err());

    let mut batch = database.open_batch();
    batch.insert(
        "albums",
        vec![
            Value::U64(3),
            Value::U64(7),
            Value::String("A Love Supreme".to_owned()),
        ],
    );
    database.commit_batch(batch).await.unwrap();

    assert_eq!(
        expect_try_recv_vals(&second),
        vec![(
            vec![
                Value::U64(7),
                Value::U64(3),
                Value::String("A Love Supreme".to_owned())
            ],
            1
        )]
    );
}

#[futures_test::test]
async fn prepared_subscription_lowers_parameter_predicates_to_shape_subscriptions() {
    let storage =
        MemoryStorage::new(&["albums", "artists"]).expect("valid memory storage families");
    let mut database = Database::new(albums_artists_schema(), storage)
        .await
        .unwrap();

    let mut batch = database.open_batch();
    batch.insert(
        "albums",
        vec![
            Value::U64(1),
            Value::U64(7),
            Value::String("Blue Train".to_owned()),
        ],
    );
    batch.insert(
        "albums",
        vec![
            Value::U64(2),
            Value::U64(8),
            Value::String("Kind of Blue".to_owned()),
        ],
    );
    database.commit_batch(batch).await.unwrap();

    let query = select_query(
        Select::new([
            SelectItem::expr(Expr::column("id")),
            SelectItem::expr(Expr::column("title")),
        ])
        .from([TableRef::named("albums")])
        .where_(Expr::binary(
            Expr::column("artist_id"),
            BinaryOp::Eq,
            Expr::parameter("artist"),
        )),
    );
    assert!(database.subscribe_query(query.clone()).await.is_err());

    let prepared = database.prepare_query(query).await.unwrap();
    assert_eq!(prepared.parameters()[0].name, "artist");
    assert_eq!(
        prepared
            .output()
            .fields()
            .iter()
            .filter_map(|field| field.name.as_deref())
            .collect::<Vec<_>>(),
        vec!["id", "title"]
    );
    let sub = database
        .bind(&prepared, &[("artist", Value::U64(7))])
        .await
        .unwrap();
    let other = database
        .bind(&prepared, &[("artist", Value::U64(8))])
        .await
        .unwrap();
    assert_eq!(
        database
            .ivm_runtime
            .subscription_output(sub.id())
            .unwrap()
            .fields()
            .iter()
            .filter_map(|field| field.name.as_deref())
            .collect::<Vec<_>>(),
        vec!["id", "title"]
    );

    assert_eq!(
        expect_try_recv_vals(&sub),
        vec![(
            vec![Value::U64(1), Value::String("Blue Train".to_owned())],
            1
        )]
    );
    assert_eq!(
        expect_try_recv_vals(&other),
        vec![(
            vec![Value::U64(2), Value::String("Kind of Blue".to_owned())],
            1
        )]
    );

    let mut batch = database.open_batch();
    batch.insert(
        "albums",
        vec![
            Value::U64(3),
            Value::U64(7),
            Value::String("Giant Steps".to_owned()),
        ],
    );
    batch.insert(
        "albums",
        vec![
            Value::U64(4),
            Value::U64(8),
            Value::String("Milestones".to_owned()),
        ],
    );
    database.commit_batch(batch).await.unwrap();
    assert_eq!(
        expect_try_recv_vals(&sub),
        vec![(
            vec![Value::U64(3), Value::String("Giant Steps".to_owned())],
            1
        )]
    );
    assert_eq!(
        expect_try_recv_vals(&other),
        vec![(
            vec![Value::U64(4), Value::String("Milestones".to_owned())],
            1
        )]
    );
}

#[futures_test::test]
async fn prepared_subscription_filters_not_equal_parameter_predicates() {
    let storage = MemoryStorage::new(&["albums"]).expect("valid memory storage families");
    let mut database = Database::new(albums_schema(), storage).await.unwrap();

    let mut batch = database.open_batch();
    batch.insert(
        "albums",
        vec![Value::U64(1), Value::String("Blue Train".to_owned())],
    );
    batch.insert(
        "albums",
        vec![Value::U64(2), Value::String("Kind of Blue".to_owned())],
    );
    database.commit_batch(batch).await.unwrap();

    let binding_descriptor = RecordDescriptor::new([("title_param", ColumnType::String.clone())]);
    let graph = GraphBuilder::join(
        GraphBuilder::binding_source("title_neq_params", binding_descriptor).project_fields([
            ProjectField::named("title_param"),
            ProjectField::literal("__route", Value::U8(0)),
        ]),
        GraphBuilder::table("albums").project_fields([
            ProjectField::named("id"),
            ProjectField::named("title"),
            ProjectField::literal("__route", Value::U8(0)),
        ]),
        ["__route"],
        ["__route"],
    )
    .project_fields([
        ProjectField::renamed("right.id", "id"),
        ProjectField::renamed("right.title", "title"),
        ProjectField::renamed("left.title_param", "title_param"),
    ])
    .filter(PredicateExpr::NeqField {
        field: "title".to_owned(),
        value_field: "title_param".to_owned(),
    });
    let prepared = database
        .prepare_one_sink(
            graph,
            "title_neq_params",
            binding_descriptor,
            ["title_param"],
        )
        .await
        .unwrap();
    let sub = database
        .bind_shape_one_sink(prepared.id(), &[Value::String("Blue Train".to_owned())])
        .await
        .unwrap();

    assert_eq!(
        expect_try_recv_vals(&sub),
        vec![(
            vec![
                Value::U64(2),
                Value::String("Kind of Blue".to_owned()),
                Value::String("Blue Train".to_owned()),
            ],
            1,
        )]
    );

    let mut batch = database.open_batch();
    batch.insert(
        "albums",
        vec![Value::U64(3), Value::String("Giant Steps".to_owned())],
    );
    batch.insert(
        "albums",
        vec![Value::U64(4), Value::String("Blue Train".to_owned())],
    );
    database.commit_batch(batch).await.unwrap();
    assert_eq!(
        expect_try_recv_vals(&sub),
        vec![(
            vec![
                Value::U64(3),
                Value::String("Giant Steps".to_owned()),
                Value::String("Blue Train".to_owned()),
            ],
            1,
        )]
    );
}

#[futures_test::test]
async fn prepare_query_requires_parameters_and_only_lowers_parameter_equalities() {
    let storage =
        MemoryStorage::new(&["albums", "artists"]).expect("valid memory storage families");
    let mut database = Database::new(albums_artists_schema(), storage)
        .await
        .unwrap();

    let no_parameters = select_query(
        Select::new([SelectItem::expr(Expr::column("id"))])
            .from([TableRef::named("albums")])
            .where_(Expr::binary(
                Expr::column("artist_id"),
                BinaryOp::Eq,
                Expr::Literal(Value::U64(7)),
            )),
    );
    assert!(matches!(
        database.prepare_query(no_parameters).await.unwrap_err(),
        Error::QueryPlanning(PlannerError::UnsupportedQuery(
            "prepare_query requires at least one query parameter"
        ))
    ));

    let non_equality_parameter = select_query(
        Select::new([SelectItem::expr(Expr::column("id"))])
            .from([TableRef::named("albums")])
            .where_(Expr::binary(
                Expr::column("artist_id"),
                BinaryOp::Gt,
                Expr::parameter("artist"),
            )),
    );
    assert!(matches!(
        database
            .prepare_query(non_equality_parameter)
            .await
            .unwrap_err(),
        Error::QueryPlanning(PlannerError::UnsupportedExpression(
            "only equality parameter predicates are supported"
        ))
    ));

    let parameter_to_parameter = select_query(
        Select::new([SelectItem::expr(Expr::column("id"))])
            .from([TableRef::named("albums")])
            .where_(Expr::binary(
                Expr::parameter("artist"),
                BinaryOp::Eq,
                Expr::parameter("other"),
            )),
    );
    assert!(matches!(
        database
            .prepare_query(parameter_to_parameter)
            .await
            .unwrap_err(),
        Error::QueryPlanning(PlannerError::UnsupportedExpression(
            "only column = parameter predicates are supported"
        ))
    ));
}

#[futures_test::test]
async fn select_literal_and_null_projections_remain_unsupported_by_query_planner() {
    let storage = MemoryStorage::new(&["albums"]).expect("valid memory storage families");
    let mut database = Database::new(albums_schema(), storage).await.unwrap();

    for expr in [Expr::Null, Expr::Literal(Value::String("x".to_owned()))] {
        let query =
            select_query(Select::new([SelectItem::expr(expr)]).from([TableRef::named("albums")]));

        assert!(matches!(
            database.subscribe_query(query).await.unwrap_err(),
            Error::QueryPlanning(PlannerError::UnsupportedExpression(
                "only column projection is currently lowerable"
            ))
        ));
    }
}

#[futures_test::test]
async fn prepared_subscription_validates_named_bindings() {
    let storage =
        MemoryStorage::new(&["albums", "artists"]).expect("valid memory storage families");
    let mut database = Database::new(albums_artists_schema(), storage)
        .await
        .unwrap();
    let prepared = database
        .prepare_query(select_query(
            Select::new([SelectItem::expr(Expr::column("id"))])
                .from([TableRef::named("albums")])
                .where_(Expr::binary(
                    Expr::column("artist_id"),
                    BinaryOp::Eq,
                    Expr::parameter("artist"),
                )),
        ))
        .await
        .unwrap();

    assert!(
        database
            .bind(&prepared, &[("other", Value::U64(7))])
            .await
            .is_err()
    );
    assert!(
        database
            .bind(&prepared, &[("artist", Value::String("nope".to_owned()))])
            .await
            .is_err()
    );
}

#[futures_test::test]
async fn graph_level_prepare_rejects_output_key_fields_not_in_output_descriptor() {
    let storage =
        MemoryStorage::new(&["albums", "artists"]).expect("valid memory storage families");
    let mut database = Database::new(albums_artists_schema(), storage)
        .await
        .unwrap();
    let binding_descriptor = RecordDescriptor::new([("artist_id", ColumnType::U64.clone())]);
    let graph = GraphBuilder::join(
        GraphBuilder::binding_source("artist_params", binding_descriptor),
        GraphBuilder::table("albums"),
        ["artist_id"],
        ["artist_id"],
    )
    .project_fields([
        ProjectField::renamed("right.artist_id", "artist_id"),
        ProjectField::renamed("right.id", "id"),
    ]);

    assert!(matches!(
        database
            .prepare_one_sink(graph, "artist_params", binding_descriptor, ["missing"]).await
            .unwrap_err(),
        Error::IvmRuntime(IvmRuntimeError::ShapeKeyFieldNotFound(field)) if field == "missing"
    ));
}

#[futures_test::test]
async fn prepared_shapes_retain_output_graph_nodes_without_subscribers() {
    let storage =
        MemoryStorage::new(&["albums", "artists"]).expect("valid memory storage families");
    let mut database = Database::new(albums_artists_schema(), storage)
        .await
        .unwrap();
    let binding_descriptor = RecordDescriptor::new([("artist_id", ColumnType::U64.clone())]);
    let graph = GraphBuilder::join(
        GraphBuilder::binding_source("artist_params", binding_descriptor),
        GraphBuilder::table("albums"),
        ["artist_id"],
        ["artist_id"],
    )
    .project_fields([
        ProjectField::renamed("right.artist_id", "artist_id"),
        ProjectField::renamed("right.id", "id"),
    ]);

    let _shape = database
        .prepare_one_sink(graph, "artist_params", binding_descriptor, ["artist_id"])
        .await
        .unwrap();
    let retained = database.ivm_runtime.retained_node_ids();
    let retained_output_nodes = retained
        .iter()
        .filter(|node| {
            database
                .ivm_runtime
                .graph()
                .node(**node)
                .is_some_and(|graph_node| graph_node.children.is_empty())
        })
        .collect::<Vec<_>>();

    assert_eq!(retained_output_nodes.len(), 1);
    assert!(
        database
            .ivm_runtime
            .graph()
            .node(*retained_output_nodes[0])
            .is_some()
    );
    assert_eq!(database.ivm_runtime.stats().active_prepared_shapes, 1);
}

#[futures_test::test]
async fn retiring_prepared_shape_releases_only_its_own_graph_after_unsubscribe() {
    let storage = TestStorage::new(&["albums", "artists"]);
    let mut database = Database::new(albums_artists_schema(), storage)
        .await
        .unwrap();
    let mut batch = database.open_batch();
    batch.insert(
        "albums",
        vec![
            Value::U64(1),
            Value::U64(7),
            Value::String("Blue Train".to_owned()),
        ],
    );
    database.commit_batch(batch).await.unwrap();
    let baseline = database.runtime_stats();

    // These deliberately share an identical binding source and graph. Retiring
    // the first must not remove the descriptor or retained graph used by its
    // sibling.
    let first = database
        .prepare_one_sink(
            artist_album_shape_graph(),
            "artist_params",
            artist_binding_descriptor(),
            ["artist_id"],
        )
        .await
        .unwrap();
    let second = database
        .prepare_one_sink(
            artist_album_shape_graph(),
            "artist_params",
            artist_binding_descriptor(),
            ["artist_id"],
        )
        .await
        .unwrap();
    let first_id = first.id();
    let second_id = second.id();
    let first_subscription = database
        .bind_shape_one_sink(first_id, &[Value::U64(7)])
        .await
        .unwrap();
    database.drive_progress().await.unwrap();
    assert!(matches!(
        database.retire_prepared_shape(first_id),
        Err(Error::IvmRuntime(IvmRuntimeError::PreparedShapeHasActiveBindings(id))) if id == first_id
    ));
    assert_eq!(
        expect_try_recv_vals(&first_subscription),
        vec![(
            vec![
                Value::U64(7),
                Value::U64(1),
                Value::String("Blue Train".to_owned())
            ],
            1,
        )]
    );
    database.unsubscribe(first_subscription.id());
    database.retire_prepared_shape(first_id).unwrap();

    assert!(matches!(
        database.bind_shape_one_sink(first_id, &[Value::U64(7)]).await,
        Err(Error::IvmRuntime(IvmRuntimeError::PreparedShapeNotFound(id))) if id == first_id
    ));
    let second_subscription = database
        .bind_shape_one_sink(second_id, &[Value::U64(7)])
        .await
        .expect("retiring a sibling must preserve its shared binding source");
    database.drive_progress().await.unwrap();
    assert_eq!(
        expect_try_recv_vals(&second_subscription),
        vec![(
            vec![
                Value::U64(7),
                Value::U64(1),
                Value::String("Blue Train".to_owned())
            ],
            1,
        )]
    );
    database.unsubscribe(second_subscription.id());
    database.retire_prepared_shape(second_id).unwrap();

    assert!(matches!(
        database.retire_prepared_shape(second_id),
        Err(Error::IvmRuntime(IvmRuntimeError::PreparedShapeNotFound(id))) if id == second_id
    ));
    assert!(matches!(
        database.bind_shape_one_sink(second_id, &[Value::U64(7)]).await,
        Err(Error::IvmRuntime(IvmRuntimeError::PreparedShapeNotFound(id))) if id == second_id
    ));
    let final_stats = database.runtime_stats();
    assert_eq!(
        final_stats.active_subscriptions,
        baseline.active_subscriptions
    );
    assert_eq!(
        final_stats.active_prepared_shapes,
        baseline.active_prepared_shapes
    );
    assert_eq!(
        final_stats.active_shape_params,
        baseline.active_shape_params
    );
    assert_eq!(final_stats.graph_nodes, baseline.graph_nodes);
    assert_eq!(final_stats.arrangement_count, baseline.arrangement_count);
}

#[futures_test::test]
async fn prepared_subscription_matches_literal_subscription_without_param_columns() {
    let storage =
        MemoryStorage::new(&["albums", "artists"]).expect("valid memory storage families");
    let mut database = Database::new(albums_artists_schema(), storage)
        .await
        .unwrap();
    let mut batch = database.open_batch();
    batch.insert(
        "albums",
        vec![
            Value::U64(1),
            Value::U64(7),
            Value::String("Blue Train".to_owned()),
        ],
    );
    database.commit_batch(batch).await.unwrap();

    let param_query = select_query(
        Select::new([
            SelectItem::expr(Expr::column("id")),
            SelectItem::expr(Expr::column("title")),
        ])
        .from([TableRef::named("albums")])
        .where_(Expr::binary(
            Expr::column("artist_id"),
            BinaryOp::Eq,
            Expr::parameter("artist"),
        )),
    );
    let literal_query = select_query(
        Select::new([
            SelectItem::expr(Expr::column("id")),
            SelectItem::expr(Expr::column("title")),
        ])
        .from([TableRef::named("albums")])
        .where_(Expr::binary(
            Expr::column("artist_id"),
            BinaryOp::Eq,
            Expr::Literal(Value::U64(7)),
        )),
    );
    let prepared = database.prepare_query(param_query).await.unwrap();
    let param_sub = database
        .bind(&prepared, &[("artist", Value::U64(7))])
        .await
        .unwrap();
    let literal_sub = database.subscribe_query(literal_query).await.unwrap();

    assert_eq!(
        expect_try_recv_vals(&param_sub),
        expect_try_recv_vals(&literal_sub)
    );

    let mut batch = database.open_batch();
    batch.insert(
        "albums",
        vec![
            Value::U64(10),
            Value::U64(7),
            Value::String("Interstellar Space".to_owned()),
        ],
    );
    database.commit_batch(batch).await.unwrap();

    assert_eq!(
        expect_try_recv_vals(&param_sub),
        expect_try_recv_vals(&literal_sub)
    );
}

#[futures_test::test]
async fn prepared_subscriptions_match_literal_subscriptions_under_seeded_interleavings() {
    for seed in [0x7117_u64, 0x5151_u64, 0xdec0de_u64] {
        run_prepared_literal_oracle(seed).await;
    }
}

async fn run_prepared_literal_oracle(mut seed: u64) {
    let storage =
        MemoryStorage::new(&["albums", "artists"]).expect("valid memory storage families");
    let mut database = Database::new(albums_artists_schema(), storage)
        .await
        .unwrap();
    let param_query = select_query(
        Select::new([
            SelectItem::expr(Expr::column("id")),
            SelectItem::expr(Expr::column("title")),
        ])
        .from([TableRef::named("albums")])
        .where_(Expr::binary(
            Expr::column("artist_id"),
            BinaryOp::Eq,
            Expr::parameter("artist"),
        )),
    );
    let prepared = database.prepare_query(param_query).await.unwrap();
    let artist = (seed % 4) + 1;
    let prepared_sub = database
        .bind(&prepared, &[("artist", Value::U64(artist))])
        .await
        .unwrap();
    let literal_query = literal_artist_query(artist);
    let literal_sub = database.subscribe_query(literal_query).await.unwrap();
    let mut prepared_rows = std::collections::BTreeMap::<(u64, String), i64>::new();
    let mut literal_rows = std::collections::BTreeMap::<(u64, String), i64>::new();
    drain_prepared_album_rows(&prepared_sub, &mut prepared_rows);
    drain_literal_album_rows(&literal_sub, &mut literal_rows);
    assert_eq!(prepared_rows, literal_rows);
    let mut known = std::collections::BTreeSet::<u64>::new();

    for step in 0..120 {
        seed = seed
            .wrapping_mul(6364136223846793005)
            .wrapping_add(1442695040888963407);
        let id = (seed % 24) + 1;
        let next_artist = ((seed >> 11) % 4) + 1;
        let title = format!("album-{step}-{id}");
        let mut batch = database.open_batch();
        if known.contains(&id) {
            if seed & 1 == 0 {
                batch.update(
                    "albums",
                    vec![
                        Value::U64(id),
                        Value::U64(next_artist),
                        Value::String(title),
                    ],
                );
            } else {
                known.remove(&id);
                batch.delete("albums", PrimaryKeyValue::U64(id));
            }
        } else {
            known.insert(id);
            batch.insert(
                "albums",
                vec![
                    Value::U64(id),
                    Value::U64(next_artist),
                    Value::String(title),
                ],
            );
        }
        database.commit_batch(batch).await.unwrap();
        drain_prepared_album_rows(&prepared_sub, &mut prepared_rows);
        drain_literal_album_rows(&literal_sub, &mut literal_rows);
        assert_eq!(
            prepared_rows, literal_rows,
            "prepared/literal mismatch after seed {seed:#x} step {step}"
        );
    }
}

fn literal_artist_query(artist: u64) -> Query {
    select_query(
        Select::new([
            SelectItem::expr(Expr::column("id")),
            SelectItem::expr(Expr::column("title")),
        ])
        .from([TableRef::named("albums")])
        .where_(Expr::binary(
            Expr::column("artist_id"),
            BinaryOp::Eq,
            Expr::Literal(Value::U64(artist)),
        )),
    )
}

fn drain_prepared_album_rows(
    subscription: &Subscription,
    rows: &mut std::collections::BTreeMap<(u64, String), i64>,
) {
    while let Ok(deltas) = subscription.try_recv() {
        for (values, weight) in deltas.to_values().unwrap() {
            let [Value::U64(id), Value::String(title)] = values.as_slice() else {
                panic!("unexpected prepared album row: {values:?}");
            };
            *rows.entry((*id, title.clone())).or_default() += weight;
        }
    }
    rows.retain(|_, weight| *weight != 0);
}

fn drain_literal_album_rows(
    subscription: &Subscription,
    rows: &mut std::collections::BTreeMap<(u64, String), i64>,
) {
    while let Ok(deltas) = subscription.try_recv() {
        for (values, weight) in deltas.to_values().unwrap() {
            let [Value::U64(id), Value::String(title)] = values.as_slice() else {
                panic!("unexpected literal album row: {values:?}");
            };
            *rows.entry((*id, title.clone())).or_default() += weight;
        }
    }
    rows.retain(|_, weight| *weight != 0);
}

#[futures_test::test]
async fn binding_sources_are_rejected_outside_prepared_shapes() {
    let storage =
        MemoryStorage::new(&["albums", "artists"]).expect("valid memory storage families");
    let mut database = Database::new(albums_artists_schema(), storage)
        .await
        .unwrap();

    assert!(
        database
            .subscribe_one_sink(artist_album_shape_graph())
            .await
            .is_err()
    );
}

#[futures_test::test]
async fn duplicate_join_subscriptions_share_state_without_double_applying_deltas() {
    let storage =
        MemoryStorage::new(&["albums", "artists"]).expect("valid memory storage families");
    let mut database = Database::new(albums_artists_schema(), storage)
        .await
        .unwrap();
    let graph = GraphBuilder::join(
        GraphBuilder::table("albums"),
        GraphBuilder::table("artists"),
        ["artist_id"],
        ["id"],
    );
    let first = database.subscribe_one_sink(graph.clone()).await.unwrap();
    let second = database.subscribe_one_sink(graph).await.unwrap();

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

    assert_eq!(
        expect_recv_vals(&first),
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
    assert_eq!(
        expect_recv_vals(&second),
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

    assert!(database.unsubscribe(first.id()));
    assert!(database.unsubscribe(second.id()));
    assert!(database.ivm_runtime.retained_node_ids().is_empty());
}
