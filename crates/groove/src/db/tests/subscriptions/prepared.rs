//! Prepared graph construction, routing, and recursive bindings.

use super::*;

#[test]
fn prepared_subscription_reports_incremental_eq_field_filter_deltas() {
    let storage = MemoryStorage::new(&["albums"]);
    let mut database = Database::new(albums_schema(), storage).unwrap();
    let binding_descriptor = RecordDescriptor::new([("wanted", ColumnType::String.clone())]);
    let routing_field = "__routing";
    let binding = GraphBuilder::binding_source("title_eq_param", binding_descriptor)
        .project_fields([
            ProjectField::named("wanted"),
            ProjectField::literal(routing_field, Value::U8(0)),
        ]);
    let albums = GraphBuilder::table("albums").project_fields([
        ProjectField::named("id"),
        ProjectField::named("title"),
        ProjectField::literal(routing_field, Value::U8(0)),
    ]);
    let graph = GraphBuilder::join(binding, albums, [routing_field], [routing_field])
        .project_fields([
            ProjectField::renamed("right.id", "id"),
            ProjectField::renamed("right.title", "title"),
            ProjectField::renamed("left.wanted", "wanted"),
        ])
        .filter(PredicateExpr::EqField {
            field: "title".to_owned(),
            value_field: "wanted".to_owned(),
        });
    let shape = database
        .prepare_one_sink(graph, "title_eq_param", binding_descriptor, ["wanted"])
        .unwrap();
    let subscription = database
        .bind_shape_one_sink(shape.id(), &[Value::String("Blue Train".to_owned())])
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
    database.commit_batch(batch).unwrap();

    assert_eq!(
        expect_recv_vals(&subscription),
        [(
            vec![
                11_u64.into(),
                "Blue Train".into(),
                Value::String("Blue Train".to_owned()),
            ],
            1,
        )]
    );
}

#[test]
fn prepared_binding_source_reuse_validates_descriptor() {
    let storage = MemoryStorage::new(&["albums"]);
    let mut database = Database::new(albums_schema(), storage).unwrap();
    let string_descriptor = RecordDescriptor::new([("wanted", ColumnType::String.clone())]);
    let string_graph = GraphBuilder::binding_source("shared_params", string_descriptor)
        .project_fields([ProjectField::named("wanted")]);

    database
        .prepare_one_sink(
            string_graph.clone(),
            "shared_params",
            string_descriptor,
            ["wanted"],
        )
        .unwrap();
    database
        .prepare_one_sink(string_graph, "shared_params", string_descriptor, ["wanted"])
        .unwrap();

    let u64_descriptor = RecordDescriptor::new([("wanted", ColumnType::U64.clone())]);
    let u64_graph = GraphBuilder::binding_source("shared_params", u64_descriptor)
        .project_fields([ProjectField::named("wanted")]);
    let err = database
        .prepare_one_sink(u64_graph, "shared_params", u64_descriptor, ["wanted"])
        .unwrap_err();
    assert!(matches!(
        err,
        Error::IvmRuntime(IvmRuntimeError::BindingSourceDescriptorMismatch(shape))
            if shape == "shared_params"
    ));
}

#[test]
fn graph_prepared_subscription_can_hide_internal_routing_fields() {
    let storage = MemoryStorage::new(&["albums"]);
    let mut database = Database::new(albums_schema(), storage).unwrap();
    let binding_descriptor = RecordDescriptor::new([("wanted", ColumnType::String.clone())]);
    let binding = GraphBuilder::binding_source("hidden_title_eq_param", binding_descriptor);
    let graph = GraphBuilder::join(
        binding,
        GraphBuilder::table("albums"),
        ["wanted"],
        ["title"],
    )
    .project_fields([
        ProjectField::renamed("right.id", "id"),
        ProjectField::renamed("right.title", "title"),
        ProjectField::renamed("left.wanted", "__routing_wanted"),
    ]);
    let shape = database
        .prepare_one_sink(
            graph,
            "hidden_title_eq_param",
            binding_descriptor,
            ["__routing_wanted"],
        )
        .unwrap();
    let public_output = RecordDescriptor::new([
        ("id", ColumnType::U64.clone()),
        ("title", ColumnType::String.clone()),
    ]);
    let subscription = database
        .bind_shape_one_sink_with_output(
            shape.id(),
            &[Value::String("Blue Train".to_owned())],
            public_output,
        )
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
    database.commit_batch(batch).unwrap();

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
        vec![Value::U64(7), Value::String("Blue Train".to_owned())],
    );
    database.commit_batch(batch).unwrap();

    assert_eq!(
        expect_recv_vals(&subscription),
        [
            (vec![11_u64.into(), "Blue Train".into()], -1),
            (vec![7_u64.into(), "Blue Train".into()], 1),
        ]
    );
}

#[test]
fn prepared_subscription_uses_route_terminal_with_clean_public_projection() {
    let storage = MemoryStorage::new(&["albums"]);
    let mut database = Database::new(albums_schema(), storage).unwrap();
    let binding_descriptor = RecordDescriptor::new([("wanted", ColumnType::String.clone())]);
    let output_graph = GraphBuilder::table("albums")
        .project_fields([ProjectField::named("id"), ProjectField::named("title")]);
    let routing_graph = GraphBuilder::join(
        GraphBuilder::binding_source("explicit_route_title_param", binding_descriptor),
        GraphBuilder::table("albums"),
        ["wanted"],
        ["title"],
    )
    .project_fields([
        ProjectField::renamed("right.id", "id"),
        ProjectField::renamed("right.title", "title"),
        ProjectField::renamed("left.wanted", "__routing_wanted"),
    ]);
    let shape = database
        .prepare_one_sink_with_routing(
            output_graph,
            routing_graph,
            "explicit_route_title_param",
            binding_descriptor,
            ["__routing_wanted"],
        )
        .unwrap();
    let subscription = database
        .bind_shape_one_sink(shape.id(), &[Value::String("Blue Train".to_owned())])
        .unwrap();

    let initial = subscription.recv().unwrap();
    assert_eq!(
        initial.descriptor,
        RecordDescriptor::new([
            ("id", ColumnType::U64.clone()),
            ("title", ColumnType::String.clone()),
        ])
    );
    assert!(initial.is_empty());

    let mut batch = database.open_batch();
    batch.insert(
        "albums",
        vec![Value::U64(7), Value::String("Out of Scope".to_owned())],
    );
    batch.insert(
        "albums",
        vec![Value::U64(11), Value::String("Blue Train".to_owned())],
    );
    database.commit_batch(batch).unwrap();

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
        vec![Value::U64(7), Value::String("Blue Train".to_owned())],
    );
    database.commit_batch(batch).unwrap();

    assert_eq!(
        expect_recv_vals(&subscription),
        [
            (vec![11_u64.into(), "Blue Train".into()], -1),
            (vec![7_u64.into(), "Blue Train".into()], 1),
        ]
    );
}

#[test]
fn prepared_collect_routes_array_bindings_before_grouping() {
    let storage = MemoryStorage::new(&["albums"]);
    let mut database = Database::new(albums_schema(), storage).unwrap();
    let mut batch = database.open_batch();
    batch.insert(
        "albums",
        vec![Value::U64(1), Value::String("one".to_owned())],
    );
    batch.insert(
        "albums",
        vec![Value::U64(2), Value::String("two".to_owned())],
    );
    batch.insert(
        "albums",
        vec![Value::U64(3), Value::String("three".to_owned())],
    );
    database.commit_batch(batch).unwrap();

    let binding_descriptor =
        RecordDescriptor::new([("wanted_ids", ValueType::Array(Box::new(ValueType::U64)))]);
    let binding = GraphBuilder::binding_source("array_collect_route", binding_descriptor)
        .unnest("wanted_ids", "wanted_id");
    let rows = GraphBuilder::join(
        binding,
        GraphBuilder::table("albums"),
        ["wanted_id"],
        ["id"],
    )
    .project_fields([
        ProjectField::renamed("right.id", "id"),
        ProjectField::renamed("right.title", "title"),
        ProjectField::renamed("left.wanted_ids", "wanted_ids"),
    ]);
    let graph = GraphBuilder::collect_root_ordered(
        rows,
        ["id", "wanted_ids"],
        [
            CollectByField::named("id"),
            CollectByField::named("title"),
            CollectByField::named("wanted_ids"),
        ],
        [TopByOrder::asc("id")],
        ["id"],
        0,
        TopByLimit::Unbounded,
    );
    let shape = database
        .prepare(
            [RoutedMultisinkTerminal::new(
                "rows",
                graph,
                ["wanted_ids"],
                ["id", "title", "wanted_ids"],
            )],
            "array_collect_route",
            binding_descriptor,
        )
        .unwrap();

    let subscription = database
        .bind_shape(
            shape.id(),
            &[Value::Array(vec![Value::U64(1), Value::U64(3)])],
        )
        .unwrap();
    let rows = subscription.recv().unwrap();
    assert_eq!(
        rows.get("rows").unwrap().to_values().unwrap(),
        [
            (
                vec![
                    Value::U64(1),
                    Value::String("one".to_owned()),
                    Value::Array(vec![Value::U64(1), Value::U64(3)]),
                ],
                1,
            ),
            (
                vec![
                    Value::U64(3),
                    Value::String("three".to_owned()),
                    Value::Array(vec![Value::U64(1), Value::U64(3)]),
                ],
                1,
            ),
        ]
    );
}

#[test]
fn prepared_subscription_routes_nullable_uuid_and_string_binding_keys() {
    let storage = MemoryStorage::new(&["docs"]);
    let mut database = Database::new(nullable_routed_docs_schema(), storage).unwrap();
    let owner = uuid(0x100);
    let other_owner = uuid(0x200);

    let mut batch = database.open_batch();
    batch.insert(
        "docs",
        vec![
            Value::U64(1),
            Value::Nullable(Some(Box::new(Value::Uuid(owner)))),
            Value::Nullable(Some(Box::new(Value::String("open".to_owned())))),
            Value::String("wanted".to_owned()),
        ],
    );
    batch.insert(
        "docs",
        vec![
            Value::U64(2),
            Value::Nullable(Some(Box::new(Value::Uuid(other_owner)))),
            Value::Nullable(Some(Box::new(Value::String("open".to_owned())))),
            Value::String("other owner".to_owned()),
        ],
    );
    batch.insert(
        "docs",
        vec![
            Value::U64(3),
            Value::Nullable(Some(Box::new(Value::Uuid(owner)))),
            Value::Nullable(Some(Box::new(Value::String("done".to_owned())))),
            Value::String("other tag".to_owned()),
        ],
    );
    database.commit_batch(batch).unwrap();

    let binding_descriptor = RecordDescriptor::new([
        (
            "owner",
            ValueType::Nullable(Box::new(ColumnType::Uuid.clone())),
        ),
        (
            "tag",
            ValueType::Nullable(Box::new(ColumnType::String.clone())),
        ),
    ]);
    let output_graph = GraphBuilder::table("docs")
        .project_fields([ProjectField::named("id"), ProjectField::named("title")]);
    let routed_docs = GraphBuilder::table("docs")
        .unwrap_nullable("owner")
        .unwrap_nullable("tag")
        .project_fields([
            ProjectField::named("id"),
            ProjectField::named("title"),
            ProjectField::nullable("owner", "owner"),
            ProjectField::nullable("tag", "tag"),
        ]);
    let routing_graph = GraphBuilder::join(
        GraphBuilder::binding_source("nullable_doc_route", binding_descriptor),
        routed_docs,
        ["owner", "tag"],
        ["owner", "tag"],
    )
    .project_fields([
        ProjectField::renamed("right.id", "id"),
        ProjectField::renamed("right.title", "title"),
        ProjectField::renamed("right.owner", "__routing_owner"),
        ProjectField::renamed("right.tag", "__routing_tag"),
    ]);

    let shape = database
        .prepare_one_sink_with_routing(
            output_graph,
            routing_graph,
            "nullable_doc_route",
            binding_descriptor,
            ["__routing_owner", "__routing_tag"],
        )
        .unwrap();
    let subscription = database
        .bind_shape_one_sink(
            shape.id(),
            &[
                Value::Nullable(Some(Box::new(Value::Uuid(owner)))),
                Value::Nullable(Some(Box::new(Value::String("open".to_owned())))),
            ],
        )
        .unwrap();

    assert_eq!(
        expect_recv_vals(&subscription),
        [(vec![Value::U64(1), Value::String("wanted".to_owned())], 1)]
    );
}

#[test]
fn prepared_nullable_binding_arg_max_emits_initial_snapshot() {
    let storage = MemoryStorage::new(&["docs"]);
    let mut database = Database::new(nullable_routed_docs_schema(), storage).unwrap();
    let join_code = "invite-code";

    let mut batch = database.open_batch();
    batch.insert(
        "docs",
        vec![
            Value::U64(1),
            Value::Nullable(None),
            Value::Nullable(Some(Box::new(Value::String(join_code.to_owned())))),
            Value::String("initial invite row".to_owned()),
        ],
    );
    database.commit_batch(batch).unwrap();

    let binding_descriptor = RecordDescriptor::new([(
        "join_code",
        ValueType::Nullable(Box::new(ValueType::String)),
    )]);
    let routed_docs = GraphBuilder::table("docs")
        .unwrap_nullable("tag")
        .project_fields([
            ProjectField::named("id"),
            ProjectField::named("title"),
            ProjectField::nullable("tag", "__route_join_code"),
        ]);
    let bound = GraphBuilder::join(
        GraphBuilder::binding_source("invite", binding_descriptor),
        routed_docs,
        ["join_code"],
        ["__route_join_code"],
    )
    .project_fields([
        ProjectField::renamed("right.__route_join_code", "join_code"),
        ProjectField::renamed("right.id", "id"),
        ProjectField::renamed("right.title", "title"),
    ]);
    let shape = database
        .prepare_one_sink(
            GraphBuilder::arg_max_by(bound, ["join_code"], ["id"]),
            "invite",
            binding_descriptor,
            ["join_code"],
        )
        .unwrap();

    let subscription = database
        .bind_shape_one_sink(
            shape.id(),
            &[Value::Nullable(Some(Box::new(Value::String(
                join_code.to_owned(),
            ))))],
        )
        .unwrap();
    assert_eq!(
        expect_recv_vals(&subscription),
        [(
            vec![
                Value::Nullable(Some(Box::new(Value::String(join_code.to_owned())))),
                Value::U64(1),
                Value::String("initial invite row".to_owned()),
            ],
            1,
        )]
    );
}

#[test]
fn prepared_subscription_routes_null_nullable_binding_keys() {
    let storage = MemoryStorage::new(&["docs"]);
    let mut database = Database::new(nullable_routed_docs_schema(), storage).unwrap();

    let mut batch = database.open_batch();
    batch.insert(
        "docs",
        vec![
            Value::U64(1),
            Value::Nullable(None),
            Value::Nullable(None),
            Value::String("null route".to_owned()),
        ],
    );
    batch.insert(
        "docs",
        vec![
            Value::U64(2),
            Value::Nullable(None),
            Value::Nullable(Some(Box::new(Value::String("open".to_owned())))),
            Value::String("partial null".to_owned()),
        ],
    );
    database.commit_batch(batch).unwrap();

    let binding_descriptor = RecordDescriptor::new([
        (
            "owner",
            ValueType::Nullable(Box::new(ColumnType::Uuid.clone())),
        ),
        (
            "tag",
            ValueType::Nullable(Box::new(ColumnType::String.clone())),
        ),
    ]);
    let output_graph = GraphBuilder::table("docs")
        .project_fields([ProjectField::named("id"), ProjectField::named("title")]);
    let binding = GraphBuilder::binding_source("nullable_doc_null_route", binding_descriptor)
        .project_fields([
            ProjectField::named("owner"),
            ProjectField::named("tag"),
            ProjectField::literal("__join", Value::U8(0)),
        ]);
    let null_routing_graph = GraphBuilder::table("docs")
        .filter(PredicateExpr::is_null("owner"))
        .filter(PredicateExpr::is_null("tag"))
        .project_fields([
            ProjectField::named("id"),
            ProjectField::named("title"),
            ProjectField::literal("__join", Value::U8(0)),
            ProjectField::null_typed(
                "__routing_owner",
                ValueType::Nullable(Box::new(ValueType::Uuid)),
            ),
            ProjectField::null_typed(
                "__routing_tag",
                ValueType::Nullable(Box::new(ValueType::String)),
            ),
        ]);
    let null_routing_graph = GraphBuilder::join(
        binding,
        null_routing_graph,
        ["owner", "tag", "__join"],
        ["__routing_owner", "__routing_tag", "__join"],
    )
    .project_fields([
        ProjectField::renamed("right.id", "id"),
        ProjectField::renamed("right.title", "title"),
        ProjectField::renamed("right.__routing_owner", "__routing_owner"),
        ProjectField::renamed("right.__routing_tag", "__routing_tag"),
    ]);

    let shape = database
        .prepare_one_sink_with_routing(
            output_graph,
            null_routing_graph,
            "nullable_doc_null_route",
            binding_descriptor,
            ["__routing_owner", "__routing_tag"],
        )
        .unwrap();
    let subscription = database
        .bind_shape_one_sink(shape.id(), &[Value::Nullable(None), Value::Nullable(None)])
        .unwrap();

    assert_eq!(
        expect_recv_vals(&subscription),
        [(
            vec![Value::U64(1), Value::String("null route".to_owned())],
            1
        )]
    );
}

#[test]
fn prepared_subscription_rejects_routing_graph_missing_clean_output_fields() {
    let storage = MemoryStorage::new(&["albums"]);
    let mut database = Database::new(albums_schema(), storage).unwrap();
    let binding_descriptor = RecordDescriptor::new([("wanted", ColumnType::String.clone())]);
    let output_graph = GraphBuilder::table("albums")
        .project_fields([ProjectField::named("id"), ProjectField::named("title")]);
    let routing_graph = GraphBuilder::join(
        GraphBuilder::binding_source("missing_route_title_param", binding_descriptor),
        GraphBuilder::table("albums"),
        ["wanted"],
        ["title"],
    )
    .project_fields([
        ProjectField::renamed("right.id", "id"),
        ProjectField::renamed("left.wanted", "__routing_wanted"),
    ]);

    assert!(matches!(
        database.prepare_one_sink_with_routing(
            output_graph,
            routing_graph,
            "missing_route_title_param",
            binding_descriptor,
            ["__routing_wanted"],
        ),
        Err(Error::IvmRuntime(IvmRuntimeError::GraphFieldNotFound(field))) if field == "title"
    ));
}

#[test]
fn prepared_subscription_with_separate_routing_hydrates_existing_rows_on_first_bind() {
    let storage = MemoryStorage::new(&["albums"]);
    let mut database = Database::new(albums_schema(), storage).unwrap();
    let mut batch = database.open_batch();
    batch.insert(
        "albums",
        vec![Value::U64(7), Value::String("Out of Scope".to_owned())],
    );
    batch.insert(
        "albums",
        vec![Value::U64(11), Value::String("Blue Train".to_owned())],
    );
    database.commit_batch(batch).unwrap();

    let binding_descriptor = RecordDescriptor::new([("wanted", ColumnType::String.clone())]);
    let output_graph = GraphBuilder::join(
        GraphBuilder::binding_source("existing_route_title_param", binding_descriptor),
        GraphBuilder::table("albums"),
        ["wanted"],
        ["title"],
    )
    .project_fields([
        ProjectField::renamed("right.id", "id"),
        ProjectField::renamed("right.title", "title"),
    ]);
    let routing_graph = GraphBuilder::join(
        GraphBuilder::binding_source("existing_route_title_param", binding_descriptor),
        GraphBuilder::table("albums"),
        ["wanted"],
        ["title"],
    )
    .project_fields([
        ProjectField::renamed("right.id", "id"),
        ProjectField::renamed("right.title", "title"),
        ProjectField::renamed("left.wanted", "__routing_wanted"),
    ]);
    let shape = database
        .prepare_one_sink_with_routing(
            output_graph,
            routing_graph,
            "existing_route_title_param",
            binding_descriptor,
            ["__routing_wanted"],
        )
        .unwrap();
    let subscription = database
        .bind_shape_one_sink(shape.id(), &[Value::String("Blue Train".to_owned())])
        .unwrap();

    assert_eq!(
        expect_recv_vals(&subscription),
        [(vec![11_u64.into(), "Blue Train".into()], 1)]
    );
}

#[test]
fn prepared_recursive_subscription_with_separate_routing_hydrates_existing_rows_on_first_bind() {
    let storage = MemoryStorage::new(&["edges"]);
    let mut database = Database::new(edges_schema(), storage).unwrap();
    let mut batch = database.open_batch();
    insert_edge(&mut batch, 1, 1, 2);
    insert_edge(&mut batch, 2, 2, 3);
    insert_edge(&mut batch, 3, 4, 5);
    database.commit_batch(batch).unwrap();

    let binding_descriptor = RecordDescriptor::new([("seed", ColumnType::U64.clone())]);
    let output_graph = prepared_reachability_graph(GraphBuilder::table("edges"), 16);

    let reach = RecordDescriptor::new([
        ("seed", ColumnType::U64.clone()),
        ("dst", ColumnType::U64.clone()),
        ("__routing_seed", ColumnType::U64.clone()),
    ]);
    let seed = GraphBuilder::binding_source("prepared-routed-reach", binding_descriptor)
        .project_fields([
            ProjectField::renamed("seed", "seed"),
            ProjectField::renamed("seed", "dst"),
            ProjectField::renamed("seed", "__routing_seed"),
        ]);
    let frontier = GraphBuilder::frontier_source("frontier", reach);
    let step = GraphBuilder::join(
        frontier,
        GraphBuilder::table("edges").project(["src", "dst"]),
        ["dst"],
        ["src"],
    )
    .project_fields([
        ProjectField::renamed("left.seed", "seed"),
        ProjectField::renamed("right.dst", "dst"),
        ProjectField::renamed("left.__routing_seed", "__routing_seed"),
    ]);
    let routing_graph = GraphBuilder::recursive(seed, step, "frontier", 16);

    let shape = database
        .prepare_one_sink_with_routing(
            output_graph,
            routing_graph,
            "prepared-routed-reach",
            binding_descriptor,
            ["__routing_seed"],
        )
        .unwrap();
    let subscription = database
        .bind_shape_one_sink(shape.id(), &[Value::U64(1)])
        .unwrap();

    let mut values = expect_recv_vals(&subscription);
    sort_pairs_by_value(&mut values);
    assert_eq!(
        values,
        [
            (vec![Value::U64(1), Value::U64(1)], 1),
            (vec![Value::U64(1), Value::U64(2)], 1),
            (vec![Value::U64(1), Value::U64(3)], 1),
        ]
    );
}

#[test]
fn prepared_recursive_subscription_joins_new_closure_to_preexisting_downstream_rows() {
    let storage = MemoryStorage::new(&["edges", "docs"]);
    let mut database = Database::new(edges_docs_schema(), storage).unwrap();
    let mut batch = database.open_batch();
    batch.insert("docs", vec![Value::U64(11), Value::U64(3)]);
    database.commit_batch(batch).unwrap();

    let binding_descriptor = RecordDescriptor::new([("seed", ColumnType::U64.clone())]);
    let reach = prepared_reachability_graph(GraphBuilder::table("edges"), 16);
    let graph = GraphBuilder::join(GraphBuilder::table("docs"), reach, ["team"], ["dst"])
        .project_fields([
            ProjectField::renamed("left.id", "id"),
            ProjectField::renamed("left.team", "team"),
            ProjectField::renamed("right.seed", "seed"),
        ]);
    let shape = database
        .prepare_one_sink(graph, "prepared-reach", binding_descriptor, ["seed"])
        .unwrap();
    let subscription = database
        .bind_shape_one_sink(shape.id(), &[Value::U64(1)])
        .unwrap();
    assert!(subscription.recv().unwrap().is_empty());

    let mut batch = database.open_batch();
    insert_edge(&mut batch, 1, 1, 2);
    insert_edge(&mut batch, 2, 2, 3);
    database.commit_batch(batch).unwrap();

    assert_eq!(
        expect_recv_vals(&subscription),
        [(vec![Value::U64(11), Value::U64(3), Value::U64(1)], 1)]
    );
}

#[test]
fn routed_prepared_recursive_subscription_joins_new_closure_to_preexisting_downstream_rows() {
    let storage = MemoryStorage::new(&["edges", "docs"]);
    let mut database = Database::new(edges_docs_schema(), storage).unwrap();
    let mut batch = database.open_batch();
    batch.insert("docs", vec![Value::U64(11), Value::U64(3)]);
    database.commit_batch(batch).unwrap();

    let binding_descriptor = RecordDescriptor::new([("seed", ColumnType::U64.clone())]);
    let reach = RecordDescriptor::new([
        ("seed", ColumnType::U64.clone()),
        ("dst", ColumnType::U64.clone()),
        ("__routing_seed", ColumnType::U64.clone()),
    ]);
    let seed = GraphBuilder::binding_source("prepared-routed-reach-docs", binding_descriptor)
        .project_fields([
            ProjectField::renamed("seed", "seed"),
            ProjectField::renamed("seed", "dst"),
            ProjectField::renamed("seed", "__routing_seed"),
        ]);
    let frontier = GraphBuilder::frontier_source("frontier", reach);
    let step = GraphBuilder::join(
        frontier,
        GraphBuilder::table("edges").project(["src", "dst"]),
        ["dst"],
        ["src"],
    )
    .project_fields([
        ProjectField::renamed("left.seed", "seed"),
        ProjectField::renamed("right.dst", "dst"),
        ProjectField::renamed("left.__routing_seed", "__routing_seed"),
    ]);
    let reach = GraphBuilder::recursive(seed, step, "frontier", 16);
    let graph = GraphBuilder::join(GraphBuilder::table("docs"), reach, ["team"], ["dst"])
        .project_fields([
            ProjectField::renamed("left.id", "id"),
            ProjectField::renamed("left.team", "team"),
            ProjectField::renamed("right.seed", "seed"),
            ProjectField::renamed("right.__routing_seed", "__routing_seed"),
        ]);
    let shape = database
        .prepare(
            [RoutedMultisinkTerminal::new(
                "docs",
                graph,
                ["__routing_seed"],
                ["id", "team", "seed"],
            )],
            "prepared-routed-reach-docs",
            binding_descriptor,
        )
        .unwrap();
    let subscription = database.bind_shape(shape.id(), &[Value::U64(1)]).unwrap();
    assert!(subscription.recv().unwrap().is_empty());

    let mut batch = database.open_batch();
    insert_edge(&mut batch, 1, 1, 2);
    insert_edge(&mut batch, 2, 2, 3);
    database.commit_batch(batch).unwrap();

    assert_eq!(
        subscription
            .recv()
            .unwrap()
            .get("docs")
            .unwrap()
            .to_values()
            .unwrap(),
        [(vec![Value::U64(11), Value::U64(3), Value::U64(1)], 1)]
    );
}

#[test]
fn routed_recursive_sibling_terminals_each_replay_positive_table_deltas() {
    fn routed_reach_graph(binding_shape: &str, route_field: &str) -> GraphBuilder {
        let binding_descriptor = RecordDescriptor::new([("seed", ColumnType::U64.clone())]);
        let reach = RecordDescriptor::new([
            ("seed", ColumnType::U64.clone()),
            ("dst", ColumnType::U64.clone()),
            (route_field, ColumnType::U64.clone()),
        ]);
        let seed =
            GraphBuilder::binding_source(binding_shape, binding_descriptor).project_fields([
                ProjectField::renamed("seed", "seed"),
                ProjectField::renamed("seed", "dst"),
                ProjectField::renamed("seed", route_field),
            ]);
        let frontier = GraphBuilder::frontier_source("frontier", reach);
        let step = GraphBuilder::join(
            frontier,
            GraphBuilder::table("edges").project(["src", "dst"]),
            ["dst"],
            ["src"],
        )
        .project_fields([
            ProjectField::renamed("left.seed", "seed"),
            ProjectField::renamed("right.dst", "dst"),
            ProjectField::renamed(format!("left.{route_field}"), route_field),
        ]);
        GraphBuilder::recursive(seed, step, "frontier", 16)
    }

    fn routed_docs_graph(binding_shape: &str, route_field: &str) -> GraphBuilder {
        let reach = routed_reach_graph(binding_shape, route_field);
        GraphBuilder::join(GraphBuilder::table("docs"), reach, ["team"], ["dst"]).project_fields([
            ProjectField::renamed("left.id", "id"),
            ProjectField::renamed("left.team", "team"),
            ProjectField::renamed("right.seed", "seed"),
            ProjectField::renamed(format!("right.{route_field}"), route_field),
        ])
    }

    let storage = MemoryStorage::new(&["edges", "docs"]);
    let mut database = Database::new(edges_docs_schema(), storage).unwrap();
    let mut batch = database.open_batch();
    batch.insert("docs", vec![Value::U64(11), Value::U64(3)]);
    database.commit_batch(batch).unwrap();

    let binding_descriptor = RecordDescriptor::new([("seed", ColumnType::U64.clone())]);
    let shape = database
        .prepare(
            [
                RoutedMultisinkTerminal::new(
                    "route_seed",
                    routed_docs_graph("prepared-sibling-reach", "__routing_seed"),
                    ["__routing_seed"],
                    ["id", "team", "seed"],
                ),
                RoutedMultisinkTerminal::new(
                    "route_claim",
                    routed_docs_graph("prepared-sibling-reach", "__jazz_claim_sub"),
                    ["__jazz_claim_sub"],
                    ["id", "team", "seed"],
                ),
            ],
            "prepared-sibling-reach",
            binding_descriptor,
        )
        .unwrap();
    let subscription = database.bind_shape(shape.id(), &[Value::U64(1)]).unwrap();
    assert!(subscription.recv().unwrap().is_empty());

    let mut batch = database.open_batch();
    insert_edge(&mut batch, 1, 1, 2);
    insert_edge(&mut batch, 2, 2, 3);
    database.commit_batch(batch).unwrap();

    let deltas = subscription.recv().unwrap();
    let expected = [(vec![Value::U64(11), Value::U64(3), Value::U64(1)], 1)];
    assert_eq!(
        deltas.get("route_seed").unwrap().to_values().unwrap(),
        expected
    );
    assert_eq!(
        deltas.get("route_claim").unwrap().to_values().unwrap(),
        expected
    );
}

#[test]
fn prepared_recursive_subscription_joins_two_simultaneous_closure_deltas() {
    let storage = MemoryStorage::new(&["edges", "docs"]);
    let mut database = Database::new(edges_docs_schema(), storage).unwrap();
    let mut batch = database.open_batch();
    batch.insert("docs", vec![Value::U64(11), Value::U64(3)]);
    database.commit_batch(batch).unwrap();

    let binding_descriptor = RecordDescriptor::new([("seed", ColumnType::U64.clone())]);
    let reach_descriptor = RecordDescriptor::new([
        ("seed", ColumnType::U64.clone()),
        ("dst", ColumnType::U64.clone()),
    ]);
    let reachable = |frontier_name: &str| {
        let seed = GraphBuilder::binding_source("prepared-double-reach", binding_descriptor)
            .project_fields([
                ProjectField::renamed("seed", "seed"),
                ProjectField::renamed("seed", "dst"),
            ]);
        let frontier = GraphBuilder::frontier_source(frontier_name, reach_descriptor);
        let step = GraphBuilder::join(
            frontier,
            GraphBuilder::table("edges").project(["src", "dst"]),
            ["dst"],
            ["src"],
        )
        .project_fields([
            ProjectField::renamed("left.seed", "seed"),
            ProjectField::renamed("right.dst", "dst"),
        ]);
        GraphBuilder::recursive(seed, step, frontier_name, 16)
    };
    let left_reach = reachable("frontier_a");
    let right_reach = reachable("frontier_b").project_fields([
        ProjectField::renamed("seed", "right_seed"),
        ProjectField::renamed("dst", "right_dst"),
    ]);
    let graph = GraphBuilder::join(GraphBuilder::table("docs"), left_reach, ["team"], ["dst"])
        .project_fields([
            ProjectField::renamed("left.id", "id"),
            ProjectField::renamed("left.team", "team"),
            ProjectField::renamed("right.seed", "seed"),
        ]);
    let graph = GraphBuilder::join(graph, right_reach, ["team"], ["right_dst"]).project_fields([
        ProjectField::renamed("left.id", "id"),
        ProjectField::renamed("left.team", "team"),
        ProjectField::renamed("left.seed", "seed"),
    ]);
    let shape = database
        .prepare_one_sink(graph, "prepared-double-reach", binding_descriptor, ["seed"])
        .unwrap();
    let subscription = database
        .bind_shape_one_sink(shape.id(), &[Value::U64(1)])
        .unwrap();
    assert!(subscription.recv().unwrap().is_empty());

    let mut batch = database.open_batch();
    insert_edge(&mut batch, 1, 1, 2);
    insert_edge(&mut batch, 2, 2, 3);
    database.commit_batch(batch).unwrap();

    assert_eq!(
        expect_recv_vals(&subscription),
        [(vec![Value::U64(11), Value::U64(3), Value::U64(1)], 1)]
    );
}

#[test]
fn prepared_recursive_grant_shape_joins_resource_and_access_added_in_one_tick() {
    fn run(split_ticks: bool) -> Vec<(Vec<Value>, i64)> {
        let storage = MemoryStorage::new(&["group_edges", "access_edges", "resources"]);
        let mut database = Database::new(grant_shape_schema(), storage).unwrap();
        let shape = prepare_grant_shape(&mut database);
        let subscription = database
            .bind_shape_one_sink(shape.id(), &[Value::U64(1)])
            .unwrap();
        assert!(subscription.recv().unwrap().is_empty());

        if split_ticks {
            let mut batch = database.open_batch();
            insert_resource(&mut batch, 10, 777);
            database.commit_batch(batch).unwrap();
            assert!(subscription.try_recv().is_err());

            let mut batch = database.open_batch();
            insert_access_edge(&mut batch, 20, 10, 1);
            database.commit_batch(batch).unwrap();
        } else {
            let mut batch = database.open_batch();
            insert_resource(&mut batch, 10, 777);
            insert_access_edge(&mut batch, 20, 10, 1);
            database.commit_batch(batch).unwrap();
        }

        expect_recv_vals(&subscription)
    }

    assert_eq!(run(false), run(true));
}

#[test]
fn prepared_recursive_grant_shape_joins_membership_step_and_resource_in_one_tick() {
    fn run(split_ticks: bool) -> Vec<(Vec<Value>, i64)> {
        let storage = MemoryStorage::new(&["group_edges", "access_edges", "resources"]);
        let mut database = Database::new(grant_shape_schema(), storage).unwrap();
        let shape = prepare_grant_shape(&mut database);
        let subscription = database
            .bind_shape_one_sink(shape.id(), &[Value::U64(1)])
            .unwrap();
        assert!(subscription.recv().unwrap().is_empty());

        if split_ticks {
            let mut batch = database.open_batch();
            insert_resource(&mut batch, 10, 777);
            insert_access_edge(&mut batch, 20, 10, 2);
            database.commit_batch(batch).unwrap();
            assert!(subscription.try_recv().is_err());

            let mut batch = database.open_batch();
            insert_group_edge(&mut batch, 30, 1, 2);
            database.commit_batch(batch).unwrap();
        } else {
            let mut batch = database.open_batch();
            insert_resource(&mut batch, 10, 777);
            insert_access_edge(&mut batch, 20, 10, 2);
            insert_group_edge(&mut batch, 30, 1, 2);
            database.commit_batch(batch).unwrap();
        }

        expect_recv_vals(&subscription)
    }

    assert_eq!(run(false), run(true));
}

#[test]
fn prepared_subscription_with_routing_can_route_output_that_already_depends_on_binding() {
    let storage = MemoryStorage::new(&["albums"]);
    let mut database = Database::new(albums_schema(), storage).unwrap();
    let mut batch = database.open_batch();
    batch.insert(
        "albums",
        vec![Value::U64(11), Value::String("Blue Train".to_owned())],
    );
    database.commit_batch(batch).unwrap();

    let binding_descriptor = RecordDescriptor::new([("wanted", ColumnType::String.clone())]);
    let output_graph = GraphBuilder::join(
        GraphBuilder::binding_source("double_route_title_param", binding_descriptor),
        GraphBuilder::table("albums"),
        ["wanted"],
        ["title"],
    )
    .project_fields([
        ProjectField::renamed("right.id", "id"),
        ProjectField::renamed("right.title", "title"),
    ]);
    let routing_graph = GraphBuilder::join(
        output_graph.clone().project_fields([
            ProjectField::named("id"),
            ProjectField::named("title"),
            ProjectField::literal("__route_join", Value::U8(0)),
        ]),
        GraphBuilder::binding_source("double_route_title_param", binding_descriptor)
            .project_fields([
                ProjectField::named("wanted"),
                ProjectField::literal("__route_join", Value::U8(0)),
            ]),
        ["__route_join"],
        ["__route_join"],
    )
    .project_fields([
        ProjectField::renamed("left.id", "id"),
        ProjectField::renamed("left.title", "title"),
        ProjectField::renamed("right.wanted", "__routing_wanted"),
    ]);
    let shape = database
        .prepare_one_sink_with_routing(
            output_graph,
            routing_graph,
            "double_route_title_param",
            binding_descriptor,
            ["__routing_wanted"],
        )
        .unwrap();
    let subscription = database
        .bind_shape_one_sink(shape.id(), &[Value::String("Blue Train".to_owned())])
        .unwrap();

    assert_eq!(
        expect_recv_vals(&subscription),
        [(vec![11_u64.into(), "Blue Train".into()], 1)]
    );
}

#[test]
fn prepared_subscription_reports_incremental_contains_field_filter_deltas() {
    let storage = MemoryStorage::new(&["albums"]);
    let mut database = Database::new(albums_schema(), storage).unwrap();
    let binding_descriptor = RecordDescriptor::new([("needle", ColumnType::String.clone())]);
    let routing_field = "__routing";
    let binding =
        GraphBuilder::binding_source("needle_param", binding_descriptor).project_fields([
            ProjectField::named("needle"),
            ProjectField::literal(routing_field, Value::U8(0)),
        ]);
    let albums = GraphBuilder::table("albums").project_fields([
        ProjectField::named("id"),
        ProjectField::named("title"),
        ProjectField::literal(routing_field, Value::U8(0)),
    ]);
    let graph = GraphBuilder::join(binding, albums, [routing_field], [routing_field])
        .project_fields([
            ProjectField::renamed("right.id", "id"),
            ProjectField::renamed("right.title", "title"),
            ProjectField::renamed("left.needle", "needle"),
        ])
        .filter(PredicateExpr::ContainsField {
            field: "title".to_owned(),
            needle_field: "needle".to_owned(),
        });
    let shape = database
        .prepare_one_sink(graph, "needle_param", binding_descriptor, ["needle"])
        .unwrap();
    let subscription = database
        .bind_shape_one_sink(shape.id(), &[Value::String("Train".to_owned())])
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
    database.commit_batch(batch).unwrap();

    assert_eq!(
        expect_recv_vals(&subscription),
        [(
            vec![
                11_u64.into(),
                "Blue Train".into(),
                Value::String("Train".to_owned()),
            ],
            1,
        )]
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
    database.commit_batch(batch).unwrap();

    assert_eq!(
        expect_recv_vals(&subscription),
        [
            (
                vec![
                    11_u64.into(),
                    "Blue Train".into(),
                    Value::String("Train".to_owned()),
                ],
                -1,
            ),
            (
                vec![
                    7_u64.into(),
                    "Night Train".into(),
                    Value::String("Train".to_owned()),
                ],
                1,
            ),
        ]
    );
}
