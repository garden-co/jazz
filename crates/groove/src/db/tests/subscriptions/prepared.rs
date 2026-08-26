//! Prepared graph construction, routing, and recursive bindings.

use super::*;

#[futures_test::test]
async fn prepared_subscription_reports_incremental_eq_field_filter_deltas() {
    let storage = MemoryStorage::new(&["albums"]);
    let mut database = Database::new(albums_schema(), storage).await.unwrap();
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
        .await
        .unwrap();
    let subscription = database
        .bind_shape_one_sink(shape.id(), &[Value::String("Blue Train".to_owned())])
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

#[futures_test::test]
async fn prepared_binding_source_reuse_validates_descriptor() {
    let storage = MemoryStorage::new(&["albums"]);
    let mut database = Database::new(albums_schema(), storage).await.unwrap();
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
        .await
        .unwrap();
    database
        .prepare_one_sink(string_graph, "shared_params", string_descriptor, ["wanted"])
        .await
        .unwrap();

    let u64_descriptor = RecordDescriptor::new([("wanted", ColumnType::U64.clone())]);
    let u64_graph = GraphBuilder::binding_source("shared_params", u64_descriptor)
        .project_fields([ProjectField::named("wanted")]);
    let err = database
        .prepare_one_sink(u64_graph, "shared_params", u64_descriptor, ["wanted"])
        .await
        .unwrap_err();
    assert!(matches!(
        err,
        Error::IvmRuntime(IvmRuntimeError::BindingSourceDescriptorMismatch(shape))
            if shape == "shared_params"
    ));
}

#[futures_test::test]
async fn graph_prepared_subscription_can_hide_internal_routing_fields() {
    let storage = MemoryStorage::new(&["albums"]);
    let mut database = Database::new(albums_schema(), storage).await.unwrap();
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
        .await
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
        vec![Value::U64(7), Value::String("Blue Train".to_owned())],
    );
    database.commit_batch(batch).await.unwrap();

    assert_eq!(
        expect_recv_vals(&subscription),
        [
            (vec![11_u64.into(), "Blue Train".into()], -1),
            (vec![7_u64.into(), "Blue Train".into()], 1),
        ]
    );
}

#[futures_test::test]
async fn prepared_subscription_uses_route_terminal_with_clean_public_projection() {
    let storage = MemoryStorage::new(&["albums"]);
    let mut database = Database::new(albums_schema(), storage).await.unwrap();
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
        .await
        .unwrap();
    let subscription = database
        .bind_shape_one_sink(shape.id(), &[Value::String("Blue Train".to_owned())])
        .await
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
        vec![Value::U64(7), Value::String("Blue Train".to_owned())],
    );
    database.commit_batch(batch).await.unwrap();

    assert_eq!(
        expect_recv_vals(&subscription),
        [
            (vec![11_u64.into(), "Blue Train".into()], -1),
            (vec![7_u64.into(), "Blue Train".into()], 1),
        ]
    );
}

#[futures_test::test]
async fn prepared_routed_collect_by_filters_flat_input_per_binding() {
    fn parent(root: u64, children: &[u64]) -> Vec<Value> {
        let child_descriptor = RecordDescriptor::new([("child", ColumnType::U64.clone())]);
        vec![
            Value::U64(root),
            Value::Array(
                children
                    .iter()
                    .map(|child| {
                        Value::Record(crate::records::OwnedRecord::new(
                            child_descriptor.create(&[Value::U64(*child)]).unwrap(),
                            child_descriptor,
                        ))
                    })
                    .collect(),
            ),
        ]
    }

    fn expect_routed_rows(
        subscription: &crate::ivm::MultisinkSubscription,
        label: &str,
    ) -> Vec<(Vec<Value>, i64)> {
        for _ in 0..100 {
            match subscription.try_recv() {
                Ok(deltas) if deltas.is_empty() => {}
                Ok(deltas) => {
                    let rows = deltas
                        .get("rows")
                        .unwrap_or_else(|| panic!("{label}: missing rows sink: {deltas:?}"));
                    return rows.to_values().unwrap();
                }
                Err(TryRecvError::Empty) => {}
                Err(error) => panic!("{label}: routed subscription disconnected: {error:?}"),
            }
            std::thread::sleep(std::time::Duration::from_millis(1));
        }
        panic!("{label}: expected rows notification");
    }

    fn expect_routed_child_insert(
        subscription: &crate::ivm::MultisinkSubscription,
        label: &str,
        expected_child: u64,
    ) {
        let child_descriptor = RecordDescriptor::new([("child", ColumnType::U64.clone())]);
        for _ in 0..100 {
            match subscription.try_recv() {
                Ok(deltas) if deltas.is_empty() => {}
                Ok(deltas) => {
                    assert!(
                        deltas.sinks.values().all(|sink| sink.is_empty()),
                        "{label}: unexpected ordinary sink deltas: {:?}",
                        deltas.sinks
                    );
                    assert_eq!(
                        deltas.terminal_sinks.len(),
                        1,
                        "{label}: unexpected terminal sinks: {:?}",
                        deltas.terminal_sinks
                    );
                    let terminal = deltas.terminal_sinks.get("rows").unwrap_or_else(|| {
                        panic!(
                            "{label}: missing rows terminal sink: {:?}",
                            deltas.terminal_sinks
                        )
                    });
                    let [operation] = terminal.operations.as_slice() else {
                        panic!("{label}: expected one terminal operation: {terminal:?}");
                    };
                    assert!(
                        matches!(
                            operation.path.as_slice(),
                            [TerminalPathSegment::Collection(field)] if field == "children"
                        ),
                        "{label}: unexpected terminal path: {:?}",
                        operation.path
                    );
                    let TerminalEdit::Insert { index, value, .. } = &operation.edit else {
                        panic!("{label}: expected child insert: {:?}", operation.edit);
                    };
                    assert_eq!(*index, 1, "{label}: unexpected child insert index");
                    let child = crate::records::OwnedRecord::new(value.clone(), child_descriptor);
                    assert_eq!(
                        child.to_values().unwrap(),
                        [Value::U64(expected_child)],
                        "{label}: inserted the wrong child"
                    );
                    return;
                }
                Err(TryRecvError::Empty) => {}
                Err(error) => panic!("{label}: routed subscription disconnected: {error:?}"),
            }
            std::thread::sleep(std::time::Duration::from_millis(1));
        }
        panic!("{label}: expected child insert notification");
    }

    fn expect_no_routed_deltas(subscription: &crate::ivm::MultisinkSubscription, label: &str) {
        for _ in 0..100 {
            match subscription.try_recv() {
                Ok(deltas) => assert!(
                    deltas.is_empty(),
                    "{label}: unexpected routed deltas: {deltas:?}"
                ),
                Err(TryRecvError::Empty) => {}
                Err(error) => panic!("{label}: routed subscription disconnected: {error:?}"),
            }
            std::thread::sleep(std::time::Duration::from_millis(1));
        }
    }

    let storage = MemoryStorage::new(&["routed_tree"]);
    let mut database = Database::new(routed_collect_tree_schema(), storage)
        .await
        .unwrap();
    let mut batch = database.open_batch();
    batch.insert(
        "routed_tree",
        vec![
            Value::U64(1),
            Value::U64(1),
            Value::U64(10),
            Value::U64(100),
            Value::U64(1),
            Value::U64(0),
            Value::U64(0),
        ],
    );
    batch.insert(
        "routed_tree",
        vec![
            Value::U64(2),
            Value::U64(1),
            Value::U64(20),
            Value::U64(200),
            Value::U64(1),
            Value::U64(0),
            Value::U64(0),
        ],
    );
    database.commit_batch(batch).await.unwrap();

    let binding_descriptor = RecordDescriptor::new([("route", ColumnType::U64.clone())]);
    let flat_input = GraphBuilder::join(
        GraphBuilder::binding_source("routed_collect_by", binding_descriptor),
        GraphBuilder::table("routed_tree"),
        ["route"],
        ["route"],
    )
    .project_fields([
        ProjectField::renamed("right.root", "root"),
        ProjectField::renamed("left.route", "route"),
        ProjectField::renamed("right.child", "child"),
        ProjectField::renamed("right.child_order", "child_order"),
    ]);
    let graph = GraphBuilder::collect_by(
        flat_input,
        ["root", "route"],
        [
            CollectByField::named("root"),
            CollectByField::named("route"),
        ],
        [CollectByField::named("child")],
        "children",
        [TopByOrder::asc("child_order")],
        ["child"],
        0,
        TopByLimit::Unbounded,
    );
    let shape = database
        .prepare(
            [RoutedMultisinkTerminal::new(
                "rows",
                graph,
                ["route"],
                ["root", "children"],
            )],
            "routed_collect_by",
            binding_descriptor,
        )
        .await
        .unwrap();
    let route_10 = database
        .bind_shape(shape.id(), &[Value::U64(10)])
        .await
        .unwrap();
    assert_eq!(
        expect_routed_rows(&route_10, "route 10 initial hydration"),
        [(parent(1, &[100]), 1)]
    );
    let route_20 = database
        .bind_shape(shape.id(), &[Value::U64(20)])
        .await
        .unwrap();
    assert_eq!(
        expect_routed_rows(&route_20, "route 20 initial hydration"),
        [(parent(1, &[200]), 1)]
    );

    let mut batch = database.open_batch();
    batch.insert(
        "routed_tree",
        vec![
            Value::U64(3),
            Value::U64(1),
            Value::U64(10),
            Value::U64(101),
            Value::U64(2),
            Value::U64(0),
            Value::U64(0),
        ],
    );
    database.commit_batch(batch).await.unwrap();

    expect_routed_child_insert(&route_10, "route 10 child insert", 101);
    expect_no_routed_deltas(&route_20, "route 20 after route 10 insert");

    let mut batch = database.open_batch();
    batch.insert(
        "routed_tree",
        vec![
            Value::U64(4),
            Value::U64(1),
            Value::U64(20),
            Value::U64(201),
            Value::U64(2),
            Value::U64(0),
            Value::U64(0),
        ],
    );
    database.commit_batch(batch).await.unwrap();

    expect_routed_child_insert(&route_20, "route 20 child insert", 201);
    expect_no_routed_deltas(&route_10, "route 10 after route 20 insert");
}

#[futures_test::test]
async fn prepared_subscription_routes_nullable_uuid_and_string_binding_keys() {
    let storage = MemoryStorage::new(&["docs"]);
    let mut database = Database::new(nullable_routed_docs_schema(), storage)
        .await
        .unwrap();
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
    database.commit_batch(batch).await.unwrap();

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
        .await
        .unwrap();
    let subscription = database
        .bind_shape_one_sink(
            shape.id(),
            &[
                Value::Nullable(Some(Box::new(Value::Uuid(owner)))),
                Value::Nullable(Some(Box::new(Value::String("open".to_owned())))),
            ],
        )
        .await
        .unwrap();

    assert_eq!(
        expect_recv_vals(&subscription),
        [(vec![Value::U64(1), Value::String("wanted".to_owned())], 1)]
    );
}

#[futures_test::test]
async fn prepared_nullable_binding_arg_max_emits_initial_snapshot() {
    let storage = MemoryStorage::new(&["docs"]);
    let mut database = Database::new(nullable_routed_docs_schema(), storage)
        .await
        .unwrap();
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
    database.commit_batch(batch).await.unwrap();

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
        .await
        .unwrap();

    let subscription = database
        .bind_shape_one_sink(
            shape.id(),
            &[Value::Nullable(Some(Box::new(Value::String(
                join_code.to_owned(),
            ))))],
        )
        .await
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

#[futures_test::test]
async fn prepared_subscription_routes_null_nullable_binding_keys() {
    let storage = MemoryStorage::new(&["docs"]);
    let mut database = Database::new(nullable_routed_docs_schema(), storage)
        .await
        .unwrap();

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
    database.commit_batch(batch).await.unwrap();

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
        .await
        .unwrap();
    let subscription = database
        .bind_shape_one_sink(shape.id(), &[Value::Nullable(None), Value::Nullable(None)])
        .await
        .unwrap();

    assert_eq!(
        expect_recv_vals(&subscription),
        [(
            vec![Value::U64(1), Value::String("null route".to_owned())],
            1
        )]
    );
}

#[futures_test::test]
async fn prepared_subscription_rejects_routing_graph_missing_clean_output_fields() {
    let storage = MemoryStorage::new(&["albums"]);
    let mut database = Database::new(albums_schema(), storage).await.unwrap();
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
        ).await,
        Err(Error::IvmRuntime(IvmRuntimeError::GraphFieldNotFound(field))) if field == "title"
    ));
}

#[futures_test::test]
async fn prepared_subscription_with_separate_routing_hydrates_existing_rows_on_first_bind() {
    let storage = MemoryStorage::new(&["albums"]);
    let mut database = Database::new(albums_schema(), storage).await.unwrap();
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
        .await
        .unwrap();
    let subscription = database
        .bind_shape_one_sink(shape.id(), &[Value::String("Blue Train".to_owned())])
        .await
        .unwrap();

    assert_eq!(
        expect_recv_vals(&subscription),
        [(vec![11_u64.into(), "Blue Train".into()], 1)]
    );
}

#[futures_test::test]
async fn prepared_recursive_subscription_with_separate_routing_hydrates_existing_rows_on_first_bind()
 {
    let storage = MemoryStorage::new(&["edges"]);
    let mut database = Database::new(edges_schema(), storage).await.unwrap();
    let mut batch = database.open_batch();
    insert_edge(&mut batch, 1, 1, 2);
    insert_edge(&mut batch, 2, 2, 3);
    insert_edge(&mut batch, 3, 4, 5);
    database.commit_batch(batch).await.unwrap();

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
        .await
        .unwrap();
    let subscription = database
        .bind_shape_one_sink(shape.id(), &[Value::U64(1)])
        .await
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

#[futures_test::test]
async fn prepared_recursive_subscription_joins_new_closure_to_preexisting_downstream_rows() {
    let storage = MemoryStorage::new(&["edges", "docs"]);
    let mut database = Database::new(edges_docs_schema(), storage).await.unwrap();
    let mut batch = database.open_batch();
    batch.insert("docs", vec![Value::U64(11), Value::U64(3)]);
    database.commit_batch(batch).await.unwrap();

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
        .await
        .unwrap();
    let subscription = database
        .bind_shape_one_sink(shape.id(), &[Value::U64(1)])
        .await
        .unwrap();
    assert!(subscription.recv().unwrap().is_empty());

    let mut batch = database.open_batch();
    insert_edge(&mut batch, 1, 1, 2);
    insert_edge(&mut batch, 2, 2, 3);
    database.commit_batch(batch).await.unwrap();

    assert_eq!(
        expect_recv_vals(&subscription),
        [(vec![Value::U64(11), Value::U64(3), Value::U64(1)], 1)]
    );
}

#[futures_test::test]
async fn routed_prepared_recursive_subscription_joins_new_closure_to_preexisting_downstream_rows() {
    let storage = MemoryStorage::new(&["edges", "docs"]);
    let mut database = Database::new(edges_docs_schema(), storage).await.unwrap();
    let mut batch = database.open_batch();
    batch.insert("docs", vec![Value::U64(11), Value::U64(3)]);
    database.commit_batch(batch).await.unwrap();

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
        .await
        .unwrap();
    let subscription = database
        .bind_shape(shape.id(), &[Value::U64(1)])
        .await
        .unwrap();
    assert!(subscription.recv().unwrap().is_empty());

    let mut batch = database.open_batch();
    insert_edge(&mut batch, 1, 1, 2);
    insert_edge(&mut batch, 2, 2, 3);
    database.commit_batch(batch).await.unwrap();

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

#[futures_test::test]
async fn routed_recursive_sibling_terminals_each_replay_positive_table_deltas() {
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
    let mut database = Database::new(edges_docs_schema(), storage).await.unwrap();
    let mut batch = database.open_batch();
    batch.insert("docs", vec![Value::U64(11), Value::U64(3)]);
    database.commit_batch(batch).await.unwrap();

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
        .await
        .unwrap();
    let subscription = database
        .bind_shape(shape.id(), &[Value::U64(1)])
        .await
        .unwrap();
    assert!(subscription.recv().unwrap().is_empty());

    let mut batch = database.open_batch();
    insert_edge(&mut batch, 1, 1, 2);
    insert_edge(&mut batch, 2, 2, 3);
    database.commit_batch(batch).await.unwrap();

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

#[futures_test::test]
async fn prepared_recursive_subscription_joins_two_simultaneous_closure_deltas() {
    let storage = MemoryStorage::new(&["edges", "docs"]);
    let mut database = Database::new(edges_docs_schema(), storage).await.unwrap();
    let mut batch = database.open_batch();
    batch.insert("docs", vec![Value::U64(11), Value::U64(3)]);
    database.commit_batch(batch).await.unwrap();

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
        .await
        .unwrap();
    let subscription = database
        .bind_shape_one_sink(shape.id(), &[Value::U64(1)])
        .await
        .unwrap();
    assert!(subscription.recv().unwrap().is_empty());

    let mut batch = database.open_batch();
    insert_edge(&mut batch, 1, 1, 2);
    insert_edge(&mut batch, 2, 2, 3);
    database.commit_batch(batch).await.unwrap();

    assert_eq!(
        expect_recv_vals(&subscription),
        [(vec![Value::U64(11), Value::U64(3), Value::U64(1)], 1)]
    );
}

#[futures_test::test]
async fn prepared_recursive_grant_shape_joins_resource_and_access_added_in_one_tick() {
    async fn run(split_ticks: bool) -> Vec<(Vec<Value>, i64)> {
        let storage = MemoryStorage::new(&["group_edges", "access_edges", "resources"]);
        let mut database = Database::new(grant_shape_schema(), storage).await.unwrap();
        let shape = prepare_grant_shape(&mut database).await;
        let subscription = database
            .bind_shape_one_sink(shape.id(), &[Value::U64(1)])
            .await
            .unwrap();
        assert!(subscription.recv().unwrap().is_empty());

        if split_ticks {
            let mut batch = database.open_batch();
            insert_resource(&mut batch, 10, 777);
            database.commit_batch(batch).await.unwrap();
            assert!(subscription.try_recv().is_err());

            let mut batch = database.open_batch();
            insert_access_edge(&mut batch, 20, 10, 1);
            database.commit_batch(batch).await.unwrap();
        } else {
            let mut batch = database.open_batch();
            insert_resource(&mut batch, 10, 777);
            insert_access_edge(&mut batch, 20, 10, 1);
            database.commit_batch(batch).await.unwrap();
        }

        expect_recv_vals(&subscription)
    }

    assert_eq!(run(false).await, run(true).await);
}

#[futures_test::test]
async fn prepared_recursive_grant_shape_joins_membership_step_and_resource_in_one_tick() {
    async fn run(split_ticks: bool) -> Vec<(Vec<Value>, i64)> {
        let storage = MemoryStorage::new(&["group_edges", "access_edges", "resources"]);
        let mut database = Database::new(grant_shape_schema(), storage).await.unwrap();
        let shape = prepare_grant_shape(&mut database).await;
        let subscription = database
            .bind_shape_one_sink(shape.id(), &[Value::U64(1)])
            .await
            .unwrap();
        assert!(subscription.recv().unwrap().is_empty());

        if split_ticks {
            let mut batch = database.open_batch();
            insert_resource(&mut batch, 10, 777);
            insert_access_edge(&mut batch, 20, 10, 2);
            database.commit_batch(batch).await.unwrap();
            assert!(subscription.try_recv().is_err());

            let mut batch = database.open_batch();
            insert_group_edge(&mut batch, 30, 1, 2);
            database.commit_batch(batch).await.unwrap();
        } else {
            let mut batch = database.open_batch();
            insert_resource(&mut batch, 10, 777);
            insert_access_edge(&mut batch, 20, 10, 2);
            insert_group_edge(&mut batch, 30, 1, 2);
            database.commit_batch(batch).await.unwrap();
        }

        expect_recv_vals(&subscription)
    }

    assert_eq!(run(false).await, run(true).await);
}

#[futures_test::test]
async fn prepared_subscription_with_routing_can_route_output_that_already_depends_on_binding() {
    let storage = MemoryStorage::new(&["albums"]);
    let mut database = Database::new(albums_schema(), storage).await.unwrap();
    let mut batch = database.open_batch();
    batch.insert(
        "albums",
        vec![Value::U64(11), Value::String("Blue Train".to_owned())],
    );
    database.commit_batch(batch).await.unwrap();

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
        .await
        .unwrap();
    let subscription = database
        .bind_shape_one_sink(shape.id(), &[Value::String("Blue Train".to_owned())])
        .await
        .unwrap();

    assert_eq!(
        expect_recv_vals(&subscription),
        [(vec![11_u64.into(), "Blue Train".into()], 1)]
    );
}

#[futures_test::test]
async fn prepared_subscription_reports_incremental_contains_field_filter_deltas() {
    let storage = MemoryStorage::new(&["albums"]);
    let mut database = Database::new(albums_schema(), storage).await.unwrap();
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
        .await
        .unwrap();
    let subscription = database
        .bind_shape_one_sink(shape.id(), &[Value::String("Train".to_owned())])
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
    database.commit_batch(batch).await.unwrap();

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
