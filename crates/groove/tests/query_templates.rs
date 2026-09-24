use groove::db::{Database, GraphBuilder};
use groove::ivm::{InputSourceReplacement, IvmRuntimeError, bind_template_graphs};
use groove::records::{RecordDescriptor, Value};
use groove::schema::{ColumnType, DatabaseSchema};
use groove::storage::MemoryStorage;

async fn database() -> Database {
    Database::new(DatabaseSchema::new([]), MemoryStorage::new(&[]).unwrap())
        .await
        .unwrap()
}

#[futures_test::test]
async fn typed_program_arguments_bind_predicates_and_routes_before_execution() {
    use groove::ivm::{
        PredicateExpr, ProjectExpr, ProjectField, TemplateScalarArgument, bind_template_arguments,
        compile_template_graphs,
    };
    use groove::records::FieldIdentity;
    use std::sync::Arc;
    let mut db = database().await;
    let source = db.allocate_input_source(descriptor());
    db.replace_input_sources([InputSourceReplacement {
        id: source,
        descriptor: descriptor(),
        records: [11, 22]
            .map(|id| descriptor().create(&[Value::U64(id)]).unwrap())
            .to_vec(),
    }])
    .await
    .unwrap();
    let graph = template()
        .filter(PredicateExpr::TemplateArgument {
            slot: 0,
            fields: vec!["id".into()],
        })
        .project_fields([
            ProjectField::named("id"),
            ProjectField {
                expression: ProjectExpr::TemplateArgument {
                    slot: 0,
                    value_type: ColumnType::String,
                },
                output_name: "route".into(),
                output_identity: FieldIdentity::Name("route".into()),
            },
        ]);
    let compiled = compile_template_graphs(&[graph.clone()]).unwrap();
    let inputs = [db
        .describe_template_input(GraphBuilder::input_source(source, descriptor()))
        .unwrap()];
    // Both ordinary and typed installation fail closed even before any rows
    // could encounter an unbound predicate or scalar.
    for unbound in [&[graph][..], compiled.as_slice()] {
        let graph = bind_template_graphs(unbound, &inputs).unwrap().remove(0);
        assert!(db.subscribe_one_sink(graph).await.is_err());
    }
    assert!(TemplateScalarArgument::new(Value::U64(7), ColumnType::String).is_err());
    let bind = |id: u64, route: &str| {
        let graphs = bind_template_arguments(
            &compiled,
            &[PredicateExpr::eq("id", Value::U64(id))],
            Arc::from([TemplateScalarArgument::new(
                Value::String(route.into()),
                ColumnType::String,
            )
            .unwrap()]),
        )
        .unwrap();
        bind_template_graphs(&graphs, &inputs).unwrap().remove(0)
    };
    let first = db.subscribe_one_sink(bind(11, "first")).await.unwrap();
    let second = db.subscribe_one_sink(bind(22, "second")).await.unwrap();
    for (subscription, id, route) in [(&first, 11, "first"), (&second, 22, "second")] {
        assert_eq!(
            db.next_subscription(subscription)
                .await
                .unwrap()
                .to_values()
                .unwrap(),
            vec![(vec![Value::U64(id), Value::String(route.into())], 1)]
        );
    }
    db.replace_input_sources([InputSourceReplacement {
        id: source,
        descriptor: descriptor(),
        records: vec![descriptor().create(&[Value::U64(22)]).unwrap()],
    }])
    .await
    .unwrap();
    assert_eq!(
        db.next_subscription(&first)
            .await
            .unwrap()
            .to_values()
            .unwrap(),
        vec![(vec![Value::U64(11), Value::String("first".into())], -1)]
    );
    assert_eq!(
        db.query_graph(bind(22, "third"))
            .await
            .unwrap()
            .to_values()
            .unwrap(),
        vec![(vec![Value::U64(22), Value::String("third".into())], 1)]
    );
    let missing = bind_template_arguments(
        &compiled,
        &[PredicateExpr::eq("id", Value::U64(22))],
        Arc::from([]),
    )
    .unwrap();
    assert!(
        db.query_graph(bind_template_graphs(&missing, &inputs).unwrap().remove(0))
            .await
            .is_err()
    );
    db.unsubscribe(first.id());
    db.unsubscribe(second.id());
}

fn descriptor() -> RecordDescriptor {
    RecordDescriptor::new([("id", ColumnType::U64)])
}

#[futures_test::test]
async fn scalar_argument_contracts_preserve_nullable_and_enum_registry_types() {
    use groove::ivm::{
        ProjectExpr, ProjectField, TemplateScalarArgument, bind_template_program,
        compile_template_graphs,
    };
    use groove::records::{FieldIdentity, ScalarEnumSchema};
    use std::sync::Arc;
    let mut db = database().await;
    let state = ColumnType::EnumTag(ScalarEnumSchema::new("state", ["open", "closed"]).unwrap());
    let maybe_id = ColumnType::Nullable(Box::new(ColumnType::Uuid));
    let fields = [("state", state.clone()), ("reader", maybe_id.clone())]
        .into_iter()
        .enumerate()
        .map(|(slot, (name, ty))| ProjectField {
            expression: ProjectExpr::TemplateArgument {
                slot: slot as u32,
                value_type: ty,
            },
            output_name: name.into(),
            output_identity: FieldIdentity::Name(name.into()),
        });
    let compiled = compile_template_graphs(&[template().project_fields(fields)]).unwrap();
    let source = db
        .describe_template_input(
            GraphBuilder::values(descriptor(), [vec![Value::U64(11)]]).unwrap(),
        )
        .unwrap();
    for (tag, reader) in [
        (0, Value::Nullable(None)),
        (
            1,
            Value::Nullable(Some(Box::new(Value::Uuid(uuid::Uuid::from_u128(17))))),
        ),
    ] {
        let graph = bind_template_program(
            &compiled,
            std::slice::from_ref(&source),
            &[],
            Arc::from([
                TemplateScalarArgument::new(Value::EnumTag(tag), state.clone()).unwrap(),
                TemplateScalarArgument::new(reader.clone(), maybe_id.clone()).unwrap(),
            ]),
        )
        .unwrap()
        .remove(0);
        assert_eq!(
            db.query_graph(graph).await.unwrap().to_values().unwrap(),
            vec![(vec![Value::EnumTag(tag), reader], 1)]
        );
    }
    let reversed = ColumnType::EnumTag(ScalarEnumSchema::new("state", ["closed", "open"]).unwrap());
    let wrong = bind_template_program(
        &compiled,
        &[source],
        &[],
        Arc::from([
            TemplateScalarArgument::new(Value::EnumTag(0), reversed).unwrap(),
            TemplateScalarArgument::new(Value::Nullable(None), maybe_id).unwrap(),
        ]),
    )
    .unwrap()
    .remove(0);
    assert!(
        db.query_graph(wrong).await.is_err(),
        "equal-sized enum tags do not prove matching registries"
    );
}

fn template() -> GraphBuilder {
    GraphBuilder::TemplateInput {
        slot: 0,
        output: descriptor(),
        input: None,
    }
    .project(["id"])
}

fn bind(db: &Database, graph: GraphBuilder) -> GraphBuilder {
    bind_template_graphs(&[template()], &[db.describe_template_input(graph).unwrap()])
        .unwrap()
        .remove(0)
}

#[futures_test::test]
async fn template_bindings_keep_private_inputs_and_lifetimes_separate() {
    let mut db = database().await;
    let first = db.allocate_input_source(descriptor());
    let second = db.allocate_input_source(descriptor());
    db.replace_input_sources([
        InputSourceReplacement {
            id: first,
            descriptor: descriptor(),
            records: vec![descriptor().create(&[Value::U64(11)]).unwrap()],
        },
        InputSourceReplacement {
            id: second,
            descriptor: descriptor(),
            records: vec![descriptor().create(&[Value::U64(22)]).unwrap()],
        },
    ])
    .await
    .unwrap();
    let first_graph = bind(&db, GraphBuilder::input_source(first, descriptor()));
    let second_graph = bind(&db, GraphBuilder::input_source(second, descriptor()));
    let first_subscription = db.subscribe_one_sink(first_graph.clone()).await.unwrap();
    let second_subscription = db.subscribe_one_sink(second_graph.clone()).await.unwrap();
    assert_eq!(
        db.next_subscription(&first_subscription)
            .await
            .unwrap()
            .to_values()
            .unwrap(),
        vec![(vec![Value::U64(11)], 1)]
    );
    assert_eq!(
        db.next_subscription(&second_subscription)
            .await
            .unwrap()
            .to_values()
            .unwrap(),
        vec![(vec![Value::U64(22)], 1)]
    );
    db.replace_input_sources([InputSourceReplacement {
        id: first,
        descriptor: descriptor(),
        records: vec![descriptor().create(&[Value::U64(33)]).unwrap()],
    }])
    .await
    .unwrap();
    let changes = db
        .next_subscription(&first_subscription)
        .await
        .unwrap()
        .to_values()
        .unwrap();
    assert_eq!(changes.len(), 2);
    assert!(changes.contains(&(vec![Value::U64(11)], -1)));
    assert!(changes.contains(&(vec![Value::U64(33)], 1)));
    assert_eq!(
        db.query_graph(second_graph)
            .await
            .unwrap()
            .to_values()
            .unwrap(),
        vec![(vec![Value::U64(22)], 1)]
    );
    db.unsubscribe(first_subscription.id());
    db.retire_input_sources([first]).await.unwrap();
    assert!(matches!(
        db.subscribe_one_sink(first_graph).await,
        Err(groove::db::Error::IvmRuntime(
            IvmRuntimeError::InputSourceRetired
        ))
    ));
}

#[futures_test::test]
async fn template_binding_rejects_missing_mismatched_and_foreign_inputs() {
    assert_eq!(
        bind_template_graphs(&[template()], &[]).unwrap_err(),
        groove::ivm::TemplateBindingError::MissingInput(0)
    );
    let db = database().await;
    assert!(db.graph_output_descriptor(&template()).is_err());
    let wrong = GraphBuilder::values(
        RecordDescriptor::new([("id", ColumnType::String)]),
        [vec![Value::String("wrong".into())]],
    )
    .unwrap();
    assert!(matches!(
        bind_template_graphs(&[template()], &[db.describe_template_input(wrong).unwrap()]),
        Err(groove::ivm::TemplateBindingError::DescriptorMismatch(0))
    ));
    let mut owner = database().await;
    let input = owner.allocate_input_source(descriptor());
    let mut foreign = database().await;
    assert!(matches!(
        foreign
            .subscribe_one_sink(bind(
                &owner,
                GraphBuilder::input_source(input, descriptor())
            ))
            .await,
        Err(groove::db::Error::IvmRuntime(
            IvmRuntimeError::ForeignInputSource
        ))
    ));
}

#[futures_test::test]
async fn declared_template_contract_is_rechecked_before_execution() {
    let mut db = database().await;
    let wrong = GraphBuilder::values(
        RecordDescriptor::new([("id", ColumnType::String)]),
        [vec![Value::String("not a number".into())]],
    )
    .unwrap();
    let declared = groove::ivm::TemplateGraphInput::with_output_contract(wrong, descriptor());
    let graph = bind_template_graphs(&[template()], &[declared])
        .unwrap()
        .remove(0);
    assert!(matches!(
        db.subscribe_one_sink(graph).await,
        Err(groove::db::Error::IvmRuntime(
            IvmRuntimeError::GraphOutputMismatch
        ))
    ));
    // A rejected declaration must not poison a later ordinary subscription.
    let valid = GraphBuilder::values(descriptor(), [vec![Value::U64(7)]]).unwrap();
    assert_eq!(
        db.query_graph(bind(&db, valid))
            .await
            .unwrap()
            .to_values()
            .unwrap(),
        vec![(vec![Value::U64(7)], 1)]
    );
}

#[futures_test::test]
async fn typed_templates_bind_fresh_rows_and_preserve_live_private_inputs() {
    let mut db = database().await;
    let blueprint = template().filter(groove::ivm::PredicateExpr::gt("id", Value::U64(10)));
    let typed = groove::ivm::compile_template_graphs(std::slice::from_ref(&blueprint)).unwrap();
    assert!(matches!(typed[0], GraphBuilder::TypedTemplate { .. }));
    let first = db.allocate_input_source(descriptor());
    let second = db.allocate_input_source(descriptor());
    let bind_input = |id| {
        bind_template_graphs(
            &typed,
            &[groove::ivm::TemplateGraphInput::with_output_contract(
                GraphBuilder::input_source(id, descriptor()),
                descriptor(),
            )],
        )
        .unwrap()
        .remove(0)
    };
    let first_graph = bind_input(first);
    let second_graph = bind_input(second);
    for (id, value) in [(first, 11), (second, 22)] {
        db.replace_input_sources([InputSourceReplacement {
            id,
            descriptor: descriptor(),
            records: vec![descriptor().create(&[Value::U64(value)]).unwrap()],
        }])
        .await
        .unwrap();
    }
    let subscription = db.subscribe_one_sink(first_graph.clone()).await.unwrap();
    assert_eq!(
        db.next_subscription(&subscription)
            .await
            .unwrap()
            .to_values()
            .unwrap(),
        vec![(vec![Value::U64(11)], 1)]
    );
    assert_eq!(
        db.query_graph(second_graph.clone())
            .await
            .unwrap()
            .to_values()
            .unwrap(),
        vec![(vec![Value::U64(22)], 1)]
    );
    db.replace_input_sources([InputSourceReplacement {
        id: first,
        descriptor: descriptor(),
        records: vec![descriptor().create(&[Value::U64(9)]).unwrap()],
    }])
    .await
    .unwrap();
    assert_eq!(
        db.next_subscription(&subscription)
            .await
            .unwrap()
            .to_values()
            .unwrap(),
        vec![(vec![Value::U64(11)], -1)]
    );
    assert_eq!(
        db.query_graph(second_graph)
            .await
            .unwrap()
            .to_values()
            .unwrap(),
        vec![(vec![Value::U64(22)], 1)]
    );
    db.unsubscribe(subscription.id());
    db.retire_input_sources([first]).await.unwrap();
    assert!(matches!(
        db.subscribe_one_sink(first_graph).await,
        Err(groove::db::Error::IvmRuntime(
            IvmRuntimeError::InputSourceRetired
        ))
    ));
}

#[futures_test::test]
async fn typed_templates_reject_forged_contracts_and_preserve_ordered_sources() {
    let mut db = database().await;
    let typed = groove::ivm::compile_template_graphs(&[template()]).unwrap();
    let wrong = GraphBuilder::values(
        RecordDescriptor::new([("id", ColumnType::String)]),
        [vec![Value::String("wrong".into())]],
    )
    .unwrap();
    let bad = bind_template_graphs(
        &typed,
        &[groove::ivm::TemplateGraphInput::with_output_contract(
            wrong,
            descriptor(),
        )],
    )
    .unwrap()
    .remove(0);
    assert!(matches!(
        db.query_graph(bad).await,
        Err(groove::db::Error::IvmRuntime(
            IvmRuntimeError::GraphOutputMismatch
        ))
    ));
    let input =
        GraphBuilder::values(descriptor(), [vec![Value::U64(22)], vec![Value::U64(11)]]).unwrap();
    let source = GraphBuilder::top_by(
        input,
        [] as [&str; 0],
        [groove::ivm::TopByOrder::asc("id")],
        ["id"],
        0,
        groove::ivm::TopByLimit::Finite(1),
    );
    let expected = db
        .query_graph(source.clone().project(["id"]))
        .await
        .unwrap()
        .to_values()
        .unwrap();
    let graph = bind_template_graphs(&typed, &[db.describe_template_input(source).unwrap()])
        .unwrap()
        .remove(0);
    assert_eq!(
        db.query_graph(graph).await.unwrap().to_values().unwrap(),
        expected
    );
    assert_eq!(expected, vec![(vec![Value::U64(11)], 1)]);
}

#[futures_test::test]
async fn typed_family_reuses_operators_without_capturing_predicates_or_rows() {
    let mut db = database().await;
    let mut cache = groove::ivm::TypedGraphTemplateCache::default();
    let shape = |second| {
        template()
            .filter(groove::ivm::PredicateExpr::eq("id", Value::U64(11)))
            .filter(groove::ivm::PredicateExpr::eq("id", Value::U64(second)))
    };
    let first = cache.compile(&[shape(11)]).unwrap().remove(0);
    let second = cache.compile(&[shape(22)]).unwrap().remove(0);
    // This public compiler-artifact assertion proves that the exact-result
    // checks below exercise reuse, not two independent compilations.
    match (&first, &second) {
        (
            GraphBuilder::TypedTemplate { program: a, .. },
            GraphBuilder::TypedTemplate { program: b, .. },
        ) => assert!(std::sync::Arc::ptr_eq(a, b)),
        _ => panic!("expected typed family reuse"),
    }
    for (graph, expected) in [
        (first.clone(), vec![(vec![Value::U64(11)], 1)]),
        (second, Vec::new()),
    ] {
        let rows = GraphBuilder::values(descriptor(), [vec![Value::U64(11)], vec![Value::U64(22)]])
            .unwrap();
        let bound = bind_template_graphs(&[graph], &[db.describe_template_input(rows).unwrap()])
            .unwrap()
            .remove(0);
        assert_eq!(
            db.query_graph(bound).await.unwrap().to_values().unwrap(),
            expected
        );
    }
    // The same typed program can run in a fresh runtime with changed data.
    let mut fresh = database().await;
    let rows = GraphBuilder::values(descriptor(), [vec![Value::U64(22)]]).unwrap();
    let graph = bind_template_graphs(&[first], &[fresh.describe_template_input(rows).unwrap()])
        .unwrap()
        .remove(0);
    assert!(
        fresh
            .query_graph(graph)
            .await
            .unwrap()
            .to_values()
            .unwrap()
            .is_empty()
    );
}

#[futures_test::test]
async fn typed_family_preserves_source_projection_context_and_bound_contracts() {
    use groove::ivm::{ProjectField, TypedGraphTemplateCache};
    use std::sync::Arc;
    let mut db = database().await;
    let mut cache = TypedGraphTemplateCache::default();
    let raw = RecordDescriptor::new([("left", ColumnType::U64), ("right", ColumnType::U64)]);
    for (field, left, right, expected) in [
        ("left", 11, 22, 11),
        ("right", 11, 22, 22),
        ("left", 33, 44, 33),
    ] {
        let source = GraphBuilder::values(raw, [vec![Value::U64(left), Value::U64(right)]])
            .unwrap()
            .project_fields([ProjectField::renamed(field, "id")]);
        let graph = GraphBuilder::TemplateInput {
            slot: 7,
            output: descriptor(),
            input: Some(Arc::new(source)),
        }
        .project(["id"]);
        let typed = cache.compile(&[graph.clone()]).unwrap().remove(0);
        assert_eq!(
            db.query_graph(typed).await.unwrap().to_values().unwrap(),
            vec![(vec![Value::U64(expected)], 1)]
        );
        assert_eq!(
            db.query_graph(graph).await.unwrap().to_values().unwrap(),
            vec![(vec![Value::U64(expected)], 1)]
        );
    }
    let forged = GraphBuilder::TemplateInput {
        slot: 7,
        output: descriptor(),
        input: Some(Arc::new(
            GraphBuilder::values(raw, [vec![Value::U64(11), Value::U64(22)]]).unwrap(),
        )),
    }
    .project(["id"]);
    assert!(matches!(
        cache.compile(&[forged]),
        Err(IvmRuntimeError::GraphOutputMismatch)
    ));
}

#[futures_test::test]
async fn source_blueprints_bind_fresh_rows_and_preserve_private_nested_inputs() {
    use groove::ivm::{ProjectField, split_template_sources};
    let mut db = database().await;
    let raw = RecordDescriptor::new([("left", ColumnType::U64), ("right", ColumnType::U64)]);
    let make_source = |left, right| {
        GraphBuilder::values(raw, [vec![Value::U64(left), Value::U64(right)]])
            .unwrap()
            .project_fields([ProjectField::renamed("right", "id")])
    };
    let first = make_source(11, 22);
    let second = make_source(33, 44);
    let (blueprints, first_inputs) =
        split_template_sources(&[&first], |g| db.describe_template_input(g)).unwrap();
    let (next_blueprints, second_inputs) =
        split_template_sources(&[&second], |g| db.describe_template_input(g)).unwrap();
    // The exact-result assertions below must exercise the same blueprint,
    // rather than retaining the first source's concrete rows in its program.
    assert_eq!(blueprints, next_blueprints);
    let typed = groove::ivm::compile_template_graphs(&blueprints).unwrap();
    for (inputs, expected) in [(first_inputs, 22), (second_inputs, 44)] {
        let bound = bind_template_graphs(&typed, &inputs).unwrap().remove(0);
        assert_eq!(
            db.query_graph(bound).await.unwrap().to_values().unwrap(),
            vec![(vec![Value::U64(expected)], 1)]
        );
    }

    let private = bind(
        &db,
        GraphBuilder::values(descriptor(), [vec![Value::U64(55)]]).unwrap(),
    );
    let (blueprints, inputs) =
        split_template_sources(&[&private], |g| db.describe_template_input(g)).unwrap();
    let typed = groove::ivm::compile_template_graphs(&blueprints).unwrap();
    let bound = bind_template_graphs(&typed, &inputs).unwrap().remove(0);
    assert_eq!(
        db.query_graph(bound).await.unwrap().to_values().unwrap(),
        vec![(vec![Value::U64(55)], 1)]
    );
}

// Matching a cached blueprint in place must accept exactly the fresh sources
// that splitting would turn into an equal blueprint, and bind the same rows.
#[futures_test::test]
async fn source_blueprint_matching_agrees_with_splitting_and_rejects_other_families() {
    use groove::ivm::{ProjectField, match_template_sources, split_template_sources};
    use std::sync::Arc;
    let mut db = database().await;
    let raw = RecordDescriptor::new([("left", ColumnType::U64), ("right", ColumnType::U64)]);
    let rows = |left, right| {
        GraphBuilder::values(raw, [vec![Value::U64(left), Value::U64(right)]]).unwrap()
    };
    let union_of = |a: GraphBuilder, b: GraphBuilder, field: &str| {
        let project = |g: GraphBuilder| g.project_fields([ProjectField::renamed(field, "id")]);
        GraphBuilder::Union {
            inputs: vec![Arc::new(project(a)), Arc::new(project(b))],
        }
    };
    let (blueprints, _) =
        split_template_sources(&[&union_of(rows(1, 2), rows(3, 4), "right")], |g| {
            db.describe_template_input(g)
        })
        .unwrap();
    let blueprint = [&blueprints[0]];

    // Same family, fresh rows: the matched inputs bind exactly like a split.
    let fresh = union_of(rows(5, 6), rows(7, 8), "right");
    let matched = match_template_sources(&[&fresh], &blueprint, |g| db.describe_template_input(g))
        .unwrap()
        .expect("same family");
    let (_, split) = split_template_sources(&[&fresh], |g| db.describe_template_input(g)).unwrap();
    for inputs in [matched, split] {
        let bound = bind_template_graphs(&blueprints, &inputs)
            .unwrap()
            .remove(0);
        let mut values = db.query_graph(bound).await.unwrap().to_values().unwrap();
        values.sort_by_key(|(row, _)| format!("{row:?}"));
        assert_eq!(
            values,
            vec![(vec![Value::U64(6)], 1), (vec![Value::U64(8)], 1)]
        );
    }

    // A different projection is a different family.
    let other = union_of(rows(5, 6), rows(7, 8), "left");
    assert!(
        match_template_sources(&[&other], &blueprint, |g| db.describe_template_input(g))
            .unwrap()
            .is_none()
    );
    // One shared leaf cannot stand for two distinct blueprint slots.
    let shared = Arc::new(rows(5, 6));
    let project = |g: &Arc<GraphBuilder>| {
        Arc::new(GraphBuilder::Project {
            input: g.clone(),
            fields: vec![ProjectField::renamed("right", "id")],
        })
    };
    let aliased = GraphBuilder::Union {
        inputs: vec![project(&shared), project(&shared)],
    };
    assert!(
        match_template_sources(&[&aliased], &blueprint, |g| db.describe_template_input(g))
            .unwrap()
            .is_none()
    );
}
