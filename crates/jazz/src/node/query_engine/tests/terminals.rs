//! Public collectors, correlated result trees, and hidden terminal facts.

use super::*;

#[test]
fn compiler_boundary_has_no_usage_or_lifecycle_mode() {
    let request = QueryProgramRequest {
        authorization_mode: QueryAuthorizationMode::TrustedServing,
        reads: QueryReadSet::primary(current_read_view()),
        policy: policy_context(),
        input: row_set_input(0x21),
        output: row_set_output(BTreeSet::from([ProgramFactKey::PolicyWitnesses])),
    };

    let err = lower_query_program(request, &mut FakeSourceResolver::default()).unwrap_err();
    assert!(matches!(
        err.gaps.as_slice(),
        [UnsupportedReason::Output(fact)] if matches!(fact.as_ref(), ProgramFactKey::PolicyWitnesses)
    ));
    assert!(
        err.explain
            .capabilities
            .iter()
            .any(|line| line.contains("requested fact is not lowered yet"))
    );
}

/// Closure hops must retain their executable parent keys even when sparse
/// public projection asks for only the root title.
///
/// system ──reads roots.{project, backup, members, owner}──► closure targets
#[test]
fn scalar_inner_include_preserves_nullable_root_carrier_descriptor() {
    // Internal lowering test: the descriptor mismatch exists between the
    // compiler-owned terminal contract and Groove's inferred runtime output,
    // before either representation reaches a public subscription API.
    let root = source("todos", SourceRole::Root);
    let target = source("todo_tags", SourceRole::Alias("include:0:0".to_owned()));
    let mut input = row_set_input(0x2b);
    input.shape.auxiliary_sources.insert(target.clone());
    input
        .shape
        .closure_paths
        .push(ClosurePath::ExplicitInclude {
            id: "include:0:todo".to_owned(),
            segments: vec![ClosurePathSegment {
                parent: root.clone(),
                target: target.clone(),
                source_field: "todo".to_owned(),
            }],
            root_gate: Some(ClosureRootGate::Inner),
        });
    let request = QueryProgramRequest {
        authorization_mode: QueryAuthorizationMode::TrustedServing,
        reads: QueryReadSet::primary(ReadView {
            read_schema: schema(0x10),
            policy_schema: schema(0x11),
            sources: BTreeMap::from([
                (root, requested_current_source(DurabilityTier::Global)),
                (target, requested_current_source(DurabilityTier::Global)),
            ]),
        }),
        policy: system_policy_context(),
        input,
        output: row_set_output(BTreeSet::new()),
    };

    let program = lower_query_program(request, &mut InlineCollectorResolver::new(None))
        .expect("scalar inner include lowers");
    let terminal = program
        .lowered
        .terminals
        .iter()
        .find(|terminal| terminal.sink == "app_rows")
        .expect("app-row terminal");
    let OutputTerminalSchema::AppRows(app_rows) = &terminal.output else {
        panic!("app-row terminal must carry its prepared descriptor");
    };
    let mut database = Database::new(
        DatabaseSchema::new([]),
        MemoryStorage::new(&[]).expect("valid memory storage families"),
    )
    .expect("inline descriptor database");
    let runtime_rows = database
        .query_graph(terminal.graph.clone())
        .expect("infer scalar include terminal output");

    assert_eq!(runtime_rows.descriptor, app_rows.descriptor);
    let todo = runtime_rows
        .descriptor
        .field_index("_app_todo")
        .expect("whole-row terminal retains the source FK");
    assert_eq!(
        runtime_rows.descriptor.fields()[todo].value_type,
        ValueType::Nullable(Box::new(ValueType::Uuid))
    );
}

#[test]
fn correlated_path_optional_app_rows_materialize_parent_rows() {
    // Internal lowering test: the maintained graph shape, not public row contents,
    // encodes whether optional array subqueries preserve childless parents.
    let request = correlated_path_request(
        CorrelationRequirement::Optional,
        row_set_output(BTreeSet::new()),
    );

    let mut resolver = FakeSourceResolver::default();
    let program =
        lower_query_program(request, &mut resolver).expect("optional path app rows should lower");

    let app_rows = &program
        .lowered
        .terminals
        .first()
        .expect("lowered terminal")
        .graph;
    assert_public_root_terminal(app_rows);
    assert!(graph_any(app_rows, &|graph| matches!(
        graph,
        GraphBuilder::Table { table, .. } if table == "resolved_todos"
    )));
    let ProgramOutputSchemas::RowSet(terminals) = &program.lowered.output;
    assert!(
        terminals
            .iter()
            .any(|terminal| matches!(terminal, OutputTerminalSchema::AppRows(_)))
    );
    assert_eq!(terminals.len(), 1);
}

#[test]
fn collector_tree_projects_authorized_child_rows_and_keeps_empty_optional_slots() {
    // Internal execution test: public result-tree delivery still deliberately
    // consumes relation-edge facts, so the new collector terminal is only
    // observable at the compiler/Groove boundary until the later carrier cut.
    let mut authority_request = collector_request(policy_context());
    authority_request.output.app_rows = None;
    authority_request.output.facts =
        BTreeSet::from([ProgramFactKey::ProgramSourceCoverage(program_scope())]);
    let mut authority_resolver = InlineCollectorResolver::new(Some("denied"));
    let authority_program = lower_query_program(authority_request, &mut authority_resolver)
        .expect("authority collector lowers source closure");
    assert!(
        authority_program
            .lowered
            .terminals
            .iter()
            .all(|terminal| terminal.sink != "app_rows")
    );
    assert!(authority_resolver.requests.iter().any(|request| {
        request.source.table == "todo_tags"
            && matches!(
                request.authorization,
                SourceAuthorizationRequest::PolicyFiltered { .. }
            )
    }));
    assert_eq!(authority_resolver.prepared_child_titles, ["allowed"]);

    let admitted_child_titles = authority_resolver.prepared_child_titles.clone();
    let mut request = collector_request(policy_context());
    request.authorization_mode = QueryAuthorizationMode::ClientLocal;
    let mut resolver = InlineCollectorResolver::with_admitted_child_rows(admitted_child_titles);
    let program = lower_query_program(request, &mut resolver).expect("client collector lowers");
    let terminal = program
        .lowered
        .terminals
        .iter()
        .find(|terminal| terminal.sink == "app_rows")
        .expect("app rows collector");
    assert!(matches!(terminal.graph, GraphBuilder::CollectBy { .. }));
    assert!(resolver.requests.iter().any(|request| {
        request.source.table == "todo_tags"
            && matches!(request.authorization, SourceAuthorizationRequest::System)
            && matches!(
                &request.requirements.app_fields,
                FieldRequirement::Fields(fields) if fields.contains("title")
            )
    }));

    let OutputTerminalSchema::AppRows(schema) = &terminal.output else {
        panic!("collector must expose app rows");
    };
    let tags_field = schema.descriptor.field_index("tags").expect("tags field");
    let rows = run_collector_graph(terminal.graph.clone());
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].1, 1);
    let Value::Array(tags) = &rows[0].0[tags_field] else {
        panic!("collector must render the named tags slot");
    };
    assert_eq!(tags.len(), 1, "denied child must not reach the tree");
    let Value::Record(tag) = &tags[0] else {
        panic!("tags slot must contain child records");
    };
    assert_eq!(
        tag.get("title").expect("child title"),
        Value::String("allowed".to_owned())
    );

    let mut empty_request = collector_request(policy_context());
    empty_request.authorization_mode = QueryAuthorizationMode::ClientLocal;
    let mut empty_resolver = InlineCollectorResolver::with_admitted_child_rows([]);
    let program =
        lower_query_program(empty_request, &mut empty_resolver).expect("empty collector lowers");
    let terminal = program
        .lowered
        .terminals
        .iter()
        .find(|terminal| terminal.sink == "app_rows")
        .expect("empty app rows collector");
    let OutputTerminalSchema::AppRows(schema) = &terminal.output else {
        panic!("collector must expose app rows");
    };
    let tags_field = schema.descriptor.field_index("tags").expect("tags field");
    let rows = run_collector_graph(terminal.graph.clone());
    assert_eq!(rows.len(), 1, "the childless parent must remain");
    let Value::Array(tags) = &rows[0].0[tags_field] else {
        panic!("collector must render the named tags slot");
    };
    assert!(tags.is_empty(), "childless parent must render tags: []");
}

#[test]
fn collector_layout_retains_public_magic_timestamp_fields_on_child_rows() {
    let mut request = collector_request(system_policy_context());
    let PayloadProjection::Tree(projection) = &mut request
        .output
        .app_rows
        .as_mut()
        .expect("app rows")
        .projection
    else {
        panic!("collector request must use a tree projection");
    };
    projection.paths[0].fields = FieldProjection::Fields(BTreeSet::from([
        "$createdAt".to_owned(),
        "$updatedAt".to_owned(),
        "title".to_owned(),
    ]));
    let program = lower_query_program(request, &mut InlineCollectorResolver::new(None))
        .expect("magic timestamp child projection should lower");
    let ProgramOutputSchemas::RowSet(outputs) = &program.lowered.output;
    let schema = outputs
        .iter()
        .find_map(|output| match output {
            OutputTerminalSchema::AppRows(schema) => Some(schema),
            OutputTerminalSchema::Fact(_) => None,
        })
        .expect("app rows descriptor");
    assert_eq!(schema.carrier, AppRowCarrier::Logical);
    let descriptor = &schema.descriptor;
    let tags = descriptor
        .fields()
        .iter()
        .find(|field| field.name.as_deref() == Some("tags"))
        .expect("tags output field");
    let ValueType::Array(row) = &tags.value_type else {
        panic!("tags must be an array");
    };
    let ValueType::Record(row) = row.as_ref() else {
        panic!("tags must contain records");
    };
    assert!(row.field_index("title").is_some());
    assert!(row.field_index("_app_title").is_none());
    assert!(row.field_index("$createdAt").is_some());
    assert!(row.field_index("$updatedAt").is_some());
    assert!(row.field_index("$createdBy").is_none());
    assert!(row.field_index("$updatedBy").is_none());
}

// Internal compiler-boundary regression: the public useAll receipt is covered
// by browser integration, while this test proves the collector retains a
// provenance order key through source preparation without adding it to the
// public row projection.
#[test]
fn collector_orders_and_slices_by_hidden_provenance_keys() {
    for (direction, expected_title) in
        [(SortDirection::Asc, "near"), (SortDirection::Desc, "later")]
    {
        let request =
            provenance_window_collector_request(ProvenanceField::CreatedAt, direction, false);
        let mut resolver = InlineCollectorResolver::with_root_rows([
            (0xd1, "early", 10),
            (0xd2, "near", 20),
            (0xd3, "later", 30),
            (0xd4, "late", 40),
        ]);
        let program = lower_query_program(request, &mut resolver)
            .expect("hidden provenance window key should lower");
        assert!(resolver.requests.iter().any(|request| {
            request.source.table == "todos"
                && request
                    .requirements
                    .metadata
                    .contains(&SourceMetadataRequirement::Provenance(
                        ProvenanceField::CreatedAt,
                    ))
        }));
        let terminal = program
            .lowered
            .terminals
            .iter()
            .find(|terminal| terminal.sink == "app_rows")
            .expect("app rows collector");
        let OutputTerminalSchema::AppRows(schema) = &terminal.output else {
            panic!("collector must expose app rows");
        };
        assert!(
            schema.descriptor.field_index("$createdAt").is_none(),
            "order keys must stay internal unless explicitly selected"
        );

        let rows = run_collector_graph(terminal.graph.clone());
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].0[1], Value::String(expected_title.to_owned()));
    }
}

#[test]
fn collector_executes_every_hidden_provenance_order_key() {
    let rows = [
        (0xd1, "first", 30, 0xa3, 70, 0xa1),
        (0xd2, "second", 10, 0xa1, 80, 0xa3),
        (0xd3, "third", 20, 0xa2, 90, 0xa2),
    ];
    for (field, direction, expected_title) in [
        (ProvenanceField::CreatedBy, SortDirection::Asc, "third"),
        (ProvenanceField::UpdatedAt, SortDirection::Desc, "second"),
        (ProvenanceField::UpdatedBy, SortDirection::Desc, "third"),
    ] {
        let request = provenance_window_collector_request(field, direction, false);
        let mut resolver = InlineCollectorResolver::with_provenance_root_rows(rows);
        let program = lower_query_program(request, &mut resolver)
            .expect("hidden provenance window key should lower");
        assert!(resolver.requests.iter().any(|request| {
            request.source.table == "todos"
                && request
                    .requirements
                    .metadata
                    .contains(&SourceMetadataRequirement::Provenance(field))
        }));
        let terminal = program
            .lowered
            .terminals
            .iter()
            .find(|terminal| terminal.sink == "app_rows")
            .expect("app rows collector");
        let OutputTerminalSchema::AppRows(schema) = &terminal.output else {
            panic!("collector must expose app rows");
        };
        assert!(
            schema
                .descriptor
                .field_index(provenance_field_name(field))
                .is_none(),
            "order keys must stay internal unless explicitly selected"
        );
        let output = run_collector_graph(terminal.graph.clone());
        assert_eq!(output.len(), 1);
        assert_eq!(output[0].0[1], Value::String(expected_title.to_owned()));
    }
}

#[test]
fn collector_uses_row_id_tie_breakers_for_hidden_provenance_windows() {
    let request =
        provenance_window_collector_request(ProvenanceField::CreatedAt, SortDirection::Asc, false);
    let mut resolver = InlineCollectorResolver::with_root_rows([
        (0xd1, "first", 10),
        (0xd2, "second", 10),
        (0xd3, "third", 10),
    ]);
    let program = lower_query_program(request, &mut resolver)
        .expect("tied hidden provenance window key should lower");
    let terminal = program
        .lowered
        .terminals
        .iter()
        .find(|terminal| terminal.sink == "app_rows")
        .expect("app rows collector");
    let output = run_collector_graph(terminal.graph.clone());
    assert_eq!(output.len(), 1);
    assert_eq!(
        output[0].0[1],
        Value::String("second".to_owned()),
        "limit/offset must select the row after the stable row-id tie breaker"
    );
}

#[test]
fn shape_default_collector_retains_current_row_provenance_window_keys() {
    let mut request = correlated_path_request(
        CorrelationRequirement::Optional,
        row_set_output(BTreeSet::new()),
    );
    add_root_provenance_window(
        &mut request,
        ProvenanceField::UpdatedAt,
        SortDirection::Desc,
    );
    request.authorization_mode = QueryAuthorizationMode::ClientLocal;
    let mut resolver = InlineCollectorResolver::with_provenance_root_rows([
        (0xd1, "first", 10, 0xa1, 20, 0xa1),
        (0xd2, "second", 20, 0xa2, 30, 0xa2),
        (0xd3, "third", 30, 0xa3, 40, 0xa3),
    ]);
    let program = lower_query_program(request, &mut resolver)
        .expect("shape-default provenance window should lower");
    let terminal = program
        .lowered
        .terminals
        .iter()
        .find(|terminal| terminal.sink == "app_rows")
        .expect("shape-default app rows collector");
    let OutputTerminalSchema::AppRows(schema) = &terminal.output else {
        panic!("collector must expose app rows");
    };
    for field in ["$createdAt", "$createdBy", "$updatedAt", "$updatedBy"] {
        assert!(schema.descriptor.field_index(field).is_some());
    }
    assert_eq!(schema.carrier, AppRowCarrier::CurrentRow);
    let output = run_collector_graph(terminal.graph.clone());
    assert_eq!(output.len(), 1);
    let title_field = schema
        .descriptor
        .field_index("_app_title")
        .expect("current row title field");
    assert_eq!(
        output[0].0[title_field],
        Value::Nullable(Some(Box::new(Value::String("second".to_owned()))))
    );
}

#[test]
fn collector_exposes_provenance_only_when_selected() {
    let request =
        provenance_window_collector_request(ProvenanceField::CreatedAt, SortDirection::Asc, true);
    let program = lower_query_program(request, &mut InlineCollectorResolver::new(None))
        .expect("selected provenance window key should lower");
    let terminal = program
        .lowered
        .terminals
        .iter()
        .find(|terminal| terminal.sink == "app_rows")
        .expect("app rows collector");
    let OutputTerminalSchema::AppRows(schema) = &terminal.output else {
        panic!("collector must expose app rows");
    };
    assert!(schema.descriptor.field_index("$createdAt").is_some());
}

fn provenance_window_collector_request(
    field: ProvenanceField,
    direction: SortDirection,
    select_created_at: bool,
) -> QueryProgramRequest {
    let mut request = collector_request(system_policy_context());
    add_root_provenance_window(&mut request, field, direction);
    let PayloadProjection::Tree(projection) = &mut request
        .output
        .app_rows
        .as_mut()
        .expect("app rows")
        .projection
    else {
        panic!("collector request must use a tree projection");
    };
    projection.paths.clear();
    let mut fields = BTreeSet::from(["title".to_owned()]);
    if select_created_at {
        fields.insert("$createdAt".to_owned());
    }
    projection.fields = FieldProjection::Fields(fields);
    request
}

fn add_root_provenance_window(
    request: &mut QueryProgramRequest,
    field: ProvenanceField,
    direction: SortDirection,
) {
    let input = request.input.shape.root.clone();
    let order = RowSetNodeId("root_provenance_order".to_owned());
    let slice = RowSetNodeId("root_provenance_slice".to_owned());
    let root_source = source("todos", SourceRole::Root);
    request.input.shape.nodes.insert(
        order.clone(),
        RowSetExpr::OrderBy {
            input,
            keys: vec![OrderKey {
                value: NormalizedValueRef::Provenance {
                    source: root_source.clone(),
                    field,
                },
                direction,
            }],
        },
    );
    request.input.shape.nodes.insert(
        slice.clone(),
        RowSetExpr::Slice {
            input: order,
            partition_by: Vec::new(),
            limit: Some(1),
            offset: 1,
            tie_breaker: vec![NormalizedValueRef::RowId(RowIdRef::Source(root_source))],
            rank_output: None,
        },
    );
    request.input.shape.root = slice;
}

fn provenance_field_name(field: ProvenanceField) -> &'static str {
    match field {
        ProvenanceField::CreatedAt => "$createdAt",
        ProvenanceField::CreatedBy => "$createdBy",
        ProvenanceField::UpdatedAt => "$updatedAt",
        ProvenanceField::UpdatedBy => "$updatedBy",
    }
}

#[test]
fn flat_collectors_bind_preserved_and_unwrapped_root_carriers() {
    let mut collect_all = collector_request(system_policy_context());
    let PayloadProjection::Tree(projection) = &mut collect_all
        .output
        .app_rows
        .as_mut()
        .expect("app rows")
        .projection
    else {
        panic!("collector request must use a tree projection");
    };
    projection.paths.clear();
    let program = lower_query_program(collect_all, &mut InlineCollectorResolver::new(None))
        .expect("flat collect-all should lower");
    let ProgramOutputSchemas::RowSet(outputs) = &program.lowered.output;
    let schema = outputs
        .iter()
        .find_map(|output| match output {
            OutputTerminalSchema::AppRows(schema) => Some(schema),
            OutputTerminalSchema::Fact(_) => None,
        })
        .expect("app rows descriptor");
    assert_eq!(schema.carrier, AppRowCarrier::CurrentRow);

    let mut projected = collector_request(system_policy_context());
    let PayloadProjection::Tree(projection) = &mut projected
        .output
        .app_rows
        .as_mut()
        .expect("app rows")
        .projection
    else {
        panic!("collector request must use a tree projection");
    };
    projection.paths.clear();
    projection.fields =
        FieldProjection::Fields(BTreeSet::from(["title".to_owned(), "todo".to_owned()]));
    let program = lower_query_program(projected, &mut InlineCollectorResolver::new(None))
        .expect("flat projected collector should lower");
    let ProgramOutputSchemas::RowSet(outputs) = &program.lowered.output;
    let schema = outputs
        .iter()
        .find_map(|output| match output {
            OutputTerminalSchema::AppRows(schema) => Some(schema),
            OutputTerminalSchema::Fact(_) => None,
        })
        .expect("app rows descriptor");
    assert_eq!(schema.carrier, AppRowCarrier::Logical);
}

#[test]
fn collector_tree_keeps_sibling_slots_distinct_and_nests_grandchildren_by_path() {
    // Internal execution test for the terminal descriptor: the public tree
    // receiver has not been switched to this carrier in this PR.
    let mut request = collector_request(system_policy_context());
    request.authorization_mode = QueryAuthorizationMode::ClientLocal;
    let parent = source("todos", SourceRole::Root);
    let tags = source("todo_tags", SourceRole::CorrelatedChild("tags".to_owned()));
    let labels = source(
        "todo_labels",
        SourceRole::CorrelatedChild("labels".to_owned()),
    );
    let notes = source("tag_notes", SourceRole::CorrelatedChild("notes".to_owned()));
    let sibling_node = RowSetNodeId("labels".to_owned());
    let nested_node = RowSetNodeId("notes".to_owned());
    let sibling_path = RowSetNodeId("labels_path".to_owned());
    let nested_path = RowSetNodeId("notes_path".to_owned());
    request.reads.primary.sources.insert(
        labels.clone(),
        requested_current_source(DurabilityTier::Global),
    );
    request.reads.primary.sources.insert(
        notes.clone(),
        requested_current_source(DurabilityTier::Global),
    );
    request.input.shape.nodes.insert(
        sibling_node.clone(),
        RowSetExpr::Source {
            source: labels.clone(),
            visibility: RowVisibility::Visible,
        },
    );
    request.input.shape.nodes.insert(
        sibling_path,
        RowSetExpr::CorrelatedPathProjection {
            input: RowSetNodeId("parent".to_owned()),
            child_input: sibling_node,
            path: ProgramPathId {
                owner: parent.clone(),
                child: labels.clone(),
            },
            correlation: PredicateExpr::Compare {
                left: NormalizedValueRef::RowId(RowIdRef::Source(parent.clone())),
                op: ComparisonOp::Eq,
                right: NormalizedValueRef::SourceField {
                    source: labels.clone(),
                    field: "todo".to_owned(),
                },
            },
            requirement: CorrelationRequirement::Optional,
        },
    );
    request.input.shape.nodes.insert(
        nested_node.clone(),
        RowSetExpr::Source {
            source: notes.clone(),
            visibility: RowVisibility::Visible,
        },
    );
    request.input.shape.nodes.insert(
        nested_path.clone(),
        RowSetExpr::CorrelatedPathProjection {
            input: RowSetNodeId("child".to_owned()),
            child_input: nested_node,
            path: ProgramPathId {
                owner: tags.clone(),
                child: notes.clone(),
            },
            correlation: PredicateExpr::Compare {
                left: NormalizedValueRef::RowId(RowIdRef::Source(tags.clone())),
                op: ComparisonOp::Eq,
                right: NormalizedValueRef::SourceField {
                    source: notes.clone(),
                    field: "todo".to_owned(),
                },
            },
            requirement: CorrelationRequirement::Optional,
        },
    );
    request
        .output
        .app_rows
        .as_mut()
        .expect("app rows")
        .projection = PayloadProjection::Tree(AppProjectionTree {
        fields: FieldProjection::All,
        paths: vec![
            app_path_projection(
                parent.clone(),
                tags.clone(),
                "tags",
                vec![app_path_projection(tags, notes, "notes", Vec::new())],
            ),
            app_path_projection(parent, labels, "labels", Vec::new()),
        ],
    });

    let mut required_request = request.clone();
    let RowSetExpr::CorrelatedPathProjection { requirement, .. } = required_request
        .input
        .shape
        .nodes
        .get_mut(&nested_path)
        .expect("nested path")
    else {
        panic!("nested node must be a correlated path");
    };
    *requirement = CorrelationRequirement::MatchCorrelationCardinality;

    let program = lower_query_program(request, &mut InlineCollectorResolver::new(None))
        .expect("nested collector lowers");
    let graph = program
        .lowered
        .terminals
        .iter()
        .find(|terminal| terminal.sink == "app_rows")
        .expect("app collector")
        .graph
        .clone();
    let OutputTerminalSchema::AppRows(schema) = &program
        .lowered
        .terminals
        .iter()
        .find(|terminal| terminal.sink == "app_rows")
        .expect("app collector")
        .output
    else {
        panic!("collector must expose app rows");
    };
    let tags_field = schema.descriptor.field_index("tags").expect("tags field");
    let labels_field = schema
        .descriptor
        .field_index("labels")
        .expect("labels field");
    let rows = run_collector_graph(graph);
    let Value::Array(tags) = &rows[0].0[tags_field] else {
        panic!("expected tags slot");
    };
    let Value::Array(labels) = &rows[0].0[labels_field] else {
        panic!("expected sibling labels slot");
    };
    assert_eq!(tags.len(), 2);
    assert_eq!(labels.len(), 1);
    let Value::Record(tag) = &tags[0] else {
        panic!("expected tag record");
    };
    let Value::Array(notes) = &tag.get("notes").expect("nested notes") else {
        panic!("expected nested notes slot");
    };
    assert_eq!(notes.len(), 1);

    let required_program =
        lower_query_program(required_request, &mut InlineCollectorResolver::new(None))
            .expect("nested required collector lowers");
    let required_graph = required_program
        .lowered
        .terminals
        .iter()
        .find(|terminal| terminal.sink == "app_rows")
        .expect("required app collector")
        .graph
        .clone();
    let OutputTerminalSchema::AppRows(required_schema) = &required_program
        .lowered
        .terminals
        .iter()
        .find(|terminal| terminal.sink == "app_rows")
        .expect("required app collector")
        .output
    else {
        panic!("collector must expose app rows");
    };
    let required_tags_field = required_schema
        .descriptor
        .field_index("tags")
        .expect("tags field");
    let required_rows = run_collector_graph(required_graph);
    let Value::Array(required_tags) = &required_rows[0].0[required_tags_field] else {
        panic!("expected required tags slot");
    };
    assert_eq!(
        required_tags.len(),
        1,
        "a child whose required nested relation is missing must be filtered"
    );
}

#[test]
fn collector_orders_nested_slots_by_hidden_provenance_keys() {
    let mut request = collector_request(system_policy_context());
    request.authorization_mode = QueryAuthorizationMode::ClientLocal;
    let tags = source("todo_tags", SourceRole::CorrelatedChild("tags".to_owned()));
    let order = RowSetNodeId("tags_provenance_order".to_owned());
    let slice = RowSetNodeId("tags_provenance_slice".to_owned());
    request.input.shape.nodes.insert(
        order.clone(),
        RowSetExpr::OrderBy {
            input: RowSetNodeId("child".to_owned()),
            keys: vec![OrderKey {
                value: NormalizedValueRef::Provenance {
                    source: tags.clone(),
                    field: ProvenanceField::CreatedAt,
                },
                direction: SortDirection::Asc,
            }],
        },
    );
    request.input.shape.nodes.insert(
        slice.clone(),
        RowSetExpr::Slice {
            input: order,
            partition_by: Vec::new(),
            limit: Some(1),
            offset: 1,
            tie_breaker: vec![NormalizedValueRef::RowId(RowIdRef::Source(tags.clone()))],
            rank_output: None,
        },
    );
    let RowSetExpr::CorrelatedPathProjection { child_input, .. } = request
        .input
        .shape
        .nodes
        .get_mut(&RowSetNodeId("path".to_owned()))
        .expect("tags path")
    else {
        panic!("tags path must remain correlated");
    };
    *child_input = slice;

    let mut resolver = InlineCollectorResolver::new(None);
    let program = lower_query_program(request, &mut resolver)
        .expect("nested hidden provenance window should lower");
    assert!(resolver.requests.iter().any(|request| {
        request.source == tags
            && request
                .requirements
                .metadata
                .contains(&SourceMetadataRequirement::Provenance(
                    ProvenanceField::CreatedAt,
                ))
    }));
    let terminal = program
        .lowered
        .terminals
        .iter()
        .find(|terminal| terminal.sink == "app_rows")
        .expect("app rows collector");
    let OutputTerminalSchema::AppRows(schema) = &terminal.output else {
        panic!("collector must expose app rows");
    };
    let tags_descriptor = schema
        .descriptor
        .fields()
        .iter()
        .find(|field| field.name.as_deref() == Some("tags"))
        .expect("tags output field");
    let ValueType::Array(tag) = &tags_descriptor.value_type else {
        panic!("tags must be an array");
    };
    let ValueType::Record(tag) = tag.as_ref() else {
        panic!("tags must contain records");
    };
    assert!(tag.field_index("$createdAt").is_none());

    let rows = run_collector_graph(terminal.graph.clone());
    let tags_field = schema.descriptor.field_index("tags").expect("tags field");
    let Value::Array(tags) = &rows[0].0[tags_field] else {
        panic!("collector must render tags slot");
    };
    assert_eq!(tags.len(), 1);
    let Value::Record(tag) = &tags[0] else {
        panic!("tags slot must contain child records");
    };
    assert_eq!(
        tag.get("title").expect("tag title"),
        Value::String("denied".to_owned()),
        "the nested slot must honor the hidden key's row-id tie breaker"
    );
}

#[test]
fn collector_tree_depth_limit_is_a_lowering_diagnostic() {
    let mut request = collector_request(system_policy_context());
    let mut owner = source("todo_tags", SourceRole::CorrelatedChild("tags".to_owned()));
    let mut parent_node = RowSetNodeId("child".to_owned());
    let mut nested_projection = Vec::new();
    for depth in (0..MAX_COLLECT_BY_TREE_DEPTH).rev() {
        let child = source(
            &format!("depth_{depth}"),
            SourceRole::CorrelatedChild(format!("depth_{depth}")),
        );
        nested_projection = vec![app_path_projection(
            owner.clone(),
            child.clone(),
            &format!("depth_{depth}"),
            nested_projection,
        )];
        owner = child;
    }
    // Rebuild in forward order so the normalized relation graph has every
    // nested path, while the projection reaches depth 17 (root + 16 children).
    owner = source("todo_tags", SourceRole::CorrelatedChild("tags".to_owned()));
    for depth in 0..MAX_COLLECT_BY_TREE_DEPTH {
        let child = source(
            &format!("depth_{depth}"),
            SourceRole::CorrelatedChild(format!("depth_{depth}")),
        );
        let child_node = RowSetNodeId(format!("depth_{depth}_source"));
        request.input.shape.nodes.insert(
            child_node.clone(),
            RowSetExpr::Source {
                source: child.clone(),
                visibility: RowVisibility::Visible,
            },
        );
        request.input.shape.nodes.insert(
            RowSetNodeId(format!("depth_{depth}_path")),
            RowSetExpr::CorrelatedPathProjection {
                input: parent_node.clone(),
                child_input: child_node.clone(),
                path: ProgramPathId {
                    owner: owner.clone(),
                    child: child.clone(),
                },
                correlation: PredicateExpr::Compare {
                    left: NormalizedValueRef::RowId(RowIdRef::Source(owner.clone())),
                    op: ComparisonOp::Eq,
                    right: NormalizedValueRef::SourceField {
                        source: child.clone(),
                        field: "todo".to_owned(),
                    },
                },
                requirement: CorrelationRequirement::Optional,
            },
        );
        request.reads.primary.sources.insert(
            child.clone(),
            requested_current_source(DurabilityTier::Global),
        );
        owner = child;
        parent_node = child_node;
    }
    let mut projection = collector_path_projection(nested_projection);
    clear_path_fields(&mut projection.paths);
    request
        .output
        .app_rows
        .as_mut()
        .expect("app rows")
        .projection = PayloadProjection::Tree(projection);

    let err = lower_query_program(request, &mut FakeSourceResolver::default())
        .expect_err("over-depth collector must fail during lowering");
    assert!(
        matches!(
            err.gaps.as_slice(),
            [UnsupportedReason::Operator(message)]
                if message.contains("association projection depth")
                    && message.contains("MAX_COLLECT_BY_TREE_DEPTH")
        ),
        "unexpected lowering error: {err:?}"
    );
}

#[test]
fn correlated_path_required_app_rows_with_root_facts_filter_and_dedup_parent_rows() {
    // Internal lowering test: the child correlation is an existence gate.
    // The semi-join retains each qualifying parent occurrence once, regardless
    // of how many matching child rows exist.
    let request = correlated_path_request(
        CorrelationRequirement::AtLeastOne,
        row_set_output(BTreeSet::from([ProgramFactKey::ResultMembership])),
    );

    let mut resolver = FakeSourceResolver::default();
    let program =
        lower_query_program(request, &mut resolver).expect("required path app rows should lower");

    let app_rows = &program
        .lowered
        .terminals
        .first()
        .expect("lowered terminal")
        .graph;
    assert_public_root_terminal(app_rows);
    assert!(graph_any(app_rows, &|graph| matches!(
        graph,
        GraphBuilder::Project { input, fields }
            if fields.iter().any(|field| field.output_name == "row_uuid")
                && matches!(
                    input.as_ref(),
                    GraphBuilder::SemiJoin {
                        left,
                        right,
                        left_on,
                        right_on,
                        comparison: groove::ivm::ValueComparison::Exact,
                    } if matches!(
                        left.as_ref(),
                        GraphBuilder::Table { table, .. } if table == "resolved_todos"
                    )
                        && matches!(
                            right.as_ref(),
                            GraphBuilder::UnwrapNullable { input, field }
                                if matches!(
                                    field,
                                    groove::ivm::FieldRef::Name(name) if name == "_app_todo"
                                )
                                    && matches!(
                                        input.as_ref(),
                                        GraphBuilder::Table { table, .. }
                                            if table == "resolved_todo_tags"
                                    )
                        )
                        && matches!(
                            left_on.first(),
                            Some(groove::ivm::FieldRef::Name(name)) if name == "row_uuid"
                        )
                        && matches!(
                            right_on.first(),
                            Some(groove::ivm::FieldRef::Name(name)) if name == "_app_todo"
                        )
                        && left_on.len() == right_on.len()
                        && left_on.iter().skip(1).eq(right_on.iter().skip(1))
                )
    )));
    assert!(!graph_any(app_rows, &|graph| matches!(
        graph,
        GraphBuilder::ArgMinBy { .. }
    )));
    let ProgramOutputSchemas::RowSet(terminals) = &program.lowered.output;
    assert!(
        terminals
            .iter()
            .any(|terminal| matches!(terminal, OutputTerminalSchema::AppRows(_)))
    );
    assert!(terminals.iter().any(|terminal| {
        matches!(
            terminal,
            OutputTerminalSchema::Fact(ProgramFactOutput {
                key: ProgramFactKey::ResultMembership,
                terminal: ProgramFactTerminal::Primary,
                schema: ProgramFactSchema::ResultMembership(_),
            })
        )
    }));
}

#[test]
fn correlated_path_cardinality_scalar_correlation_lowers_like_at_least_one() {
    // Internal lowering test: legacy relation semantics treat non-array
    // cardinality correlations as "at least one readable child", preserving
    // each qualifying parent occurrence rather than grouping by its row UUID.
    let request = correlated_path_request(
        CorrelationRequirement::MatchCorrelationCardinality,
        row_set_output(BTreeSet::new()),
    );

    let mut resolver = FakeSourceResolver::default();
    let program = lower_query_program(request, &mut resolver).expect("cardinality lowers");

    let app_rows = &program.lowered.terminals[0].graph;
    assert_public_root_terminal(app_rows);
    assert!(graph_any(app_rows, &|graph| matches!(
        graph,
        GraphBuilder::Project { input, fields }
            if fields.iter().any(|field| field.output_name == "row_uuid")
                && matches!(
                    input.as_ref(),
                    GraphBuilder::SemiJoin {
                        left,
                        right,
                        left_on,
                        right_on,
                        comparison: groove::ivm::ValueComparison::Exact,
                    } if matches!(
                        left.as_ref(),
                        GraphBuilder::Table { table, .. } if table == "resolved_todos"
                    )
                        && matches!(
                            right.as_ref(),
                            GraphBuilder::UnwrapNullable { input, field }
                                if matches!(
                                    field,
                                    groove::ivm::FieldRef::Name(name) if name == "_app_todo"
                                )
                                    && matches!(
                                        input.as_ref(),
                                        GraphBuilder::Table { table, .. }
                                            if table == "resolved_todo_tags"
                                    )
                        )
                        && matches!(
                            left_on.first(),
                            Some(groove::ivm::FieldRef::Name(name)) if name == "row_uuid"
                        )
                        && matches!(
                            right_on.first(),
                            Some(groove::ivm::FieldRef::Name(name)) if name == "_app_todo"
                        )
                        && left_on.len() == right_on.len()
                        && left_on.iter().skip(1).eq(right_on.iter().skip(1))
                )
    )));
    assert!(!graph_any(app_rows, &|graph| matches!(
        graph,
        GraphBuilder::ArgMinBy { .. }
    )));
}

#[test]
fn correlated_path_app_rows_and_relation_facts_lower_to_sibling_sinks() {
    // Internal lowering test: app rows use the parent-result graph while
    // relation facts use a sibling parent-child path graph.
    let request = correlated_path_request(
        CorrelationRequirement::Optional,
        row_set_output(BTreeSet::from([
            ProgramFactKey::RelationEdges,
            ProgramFactKey::PathCorrelationCoverage,
        ])),
    );

    let mut resolver = FakeSourceResolver::default();
    let program =
        lower_query_program(request, &mut resolver).expect("mixed path outputs should lower");

    assert_eq!(resolver.requests.len(), 2);
    let app_rows = program
        .lowered
        .terminals
        .iter()
        .find(|terminal| terminal.sink == "app_rows")
        .expect("app row terminal");
    assert_public_root_terminal(&app_rows.graph);
    assert!(graph_any(&app_rows.graph, &|graph| matches!(
        graph,
        GraphBuilder::Table { table, .. } if table == "resolved_todos"
    )));
    let relation_edges = program
        .lowered
        .terminals
        .iter()
        .find(|terminal| terminal.sink == "maintained.relation_edges")
        .expect("relation edge terminal");
    assert!(matches!(
        relation_edges.graph,
        GraphBuilder::Project {
            ref input,
            ref fields,
        } if fields.iter().any(|field| field.output_name == "source_row")
            && fields.iter().any(|field| field.output_name == "target_row")
            && fields.iter().any(|field| field.output_name == "path")
            && matches!(
                input.as_ref(),
                GraphBuilder::Join {
                    left_on,
                    right_on,
                    ..
                } if matches!(left_on.as_slice(), [groove::ivm::FieldRef::Name(name)] if name == "row_uuid")
                    && matches!(right_on.as_slice(), [groove::ivm::FieldRef::Name(name)] if name == "_app_todo")
            )
    ));
    let ProgramOutputSchemas::RowSet(terminals) = &program.lowered.output;
    assert_eq!(terminals.len(), 3);
    assert!(terminals.iter().any(|terminal| {
        matches!(
            terminal,
            OutputTerminalSchema::Fact(ProgramFactOutput {
                key: ProgramFactKey::RelationEdges,
                terminal: ProgramFactTerminal::Primary,
                schema: ProgramFactSchema::RelationEdges(_),
            })
        )
    }));
    assert!(terminals.iter().any(|terminal| {
        matches!(
            terminal,
            OutputTerminalSchema::Fact(ProgramFactOutput {
                key: ProgramFactKey::PathCorrelationCoverage,
                terminal: ProgramFactTerminal::Primary,
                schema: ProgramFactSchema::PathCorrelationCoverage(_),
            })
        )
    }));
}

#[test]
fn branch_relation_edges_lower_concrete_branch_witness_fields() {
    let request = correlated_path_request(
        CorrelationRequirement::Optional,
        row_set_output(BTreeSet::from([ProgramFactKey::RelationEdges])),
    );
    let mut resolver = FakeSourceResolver {
        branch_witnesses: true,
        ..Default::default()
    };
    let program = lower_query_program(request, &mut resolver).expect("branch relation lowers");
    let relation = program
        .lowered
        .terminals
        .iter()
        .find(|terminal| terminal.sink == "maintained.relation_edges")
        .expect("relation terminal");
    assert!(matches!(
        &relation.graph,
        GraphBuilder::Project { fields, .. }
            if fields.iter().any(|field| field.output_name == "source_branch_or_prefix")
                && fields.iter().any(|field| field.output_name == "target_branch_or_prefix")
    ));
    let ProgramOutputSchemas::RowSet(outputs) = &program.lowered.output;
    assert!(outputs.iter().any(|output| matches!(
        output,
        OutputTerminalSchema::Fact(ProgramFactOutput {
            schema: ProgramFactSchema::RelationEdges(RelationEdgeSchema { source, target, .. }),
            ..
        }) if source.branch_or_prefix_field.as_deref() == Some("source_branch_or_prefix")
            && target.branch_or_prefix_field.as_deref() == Some("target_branch_or_prefix")
    )));
}

#[test]
fn production_output_profiles_lower_for_linear_and_correlated_shapes() {
    // Internal lowering test: this pins production-shaped output requests at
    // the normalizer/lowering boundary, including app_rows: None fact profiles
    // that public API tests cannot isolate.
    for profile in [
        ProductionOutputProfile::AppRows,
        ProductionOutputProfile::AuthorizedRows,
        ProductionOutputProfile::RelationSnapshot,
        ProductionOutputProfile::MaintainedView,
    ] {
        let linear_request = QueryProgramRequest {
            authorization_mode: QueryAuthorizationMode::TrustedServing,
            reads: QueryReadSet::primary(current_read_view()),
            policy: system_policy_context(),
            input: row_set_input(0x79),
            output: production_output_request(profile, false),
        };
        lower_query_program(linear_request, &mut FakeSourceResolver::default())
            .unwrap_or_else(|err| panic!("linear {profile:?} profile should lower: {err:?}"));

        let correlated_request = correlated_path_request(
            CorrelationRequirement::Optional,
            production_output_request(profile, true),
        );
        let mut resolver = FakeSourceResolver::default();
        let result = lower_query_program(correlated_request, &mut resolver).resolve();
        match profile {
            ProductionOutputProfile::AuthorizedRows => {
                result.unwrap_or_else(|err| {
                    panic!("correlated authorized rows profile should lower: {err:?}")
                });
            }
            ProductionOutputProfile::RelationSnapshot => {
                let program = result.expect("correlated relation snapshot should lower");
                let ProgramOutputSchemas::RowSet(terminals) = &program.lowered.output;
                assert!(terminals.iter().any(|terminal| {
                    matches!(
                        terminal,
                        OutputTerminalSchema::Fact(ProgramFactOutput {
                            key: ProgramFactKey::RelationEdges,
                            ..
                        })
                    )
                }));
                assert!(terminals.iter().any(|terminal| {
                    matches!(
                        terminal,
                        OutputTerminalSchema::Fact(ProgramFactOutput {
                            key: ProgramFactKey::PathCorrelationCoverage,
                            ..
                        })
                    )
                }));
            }
            ProductionOutputProfile::MaintainedView => {
                result.unwrap_or_else(|err| {
                    panic!("correlated maintained view profile should lower: {err:?}")
                });
            }
            _ => {
                result.unwrap_or_else(|err| {
                    panic!("correlated {profile:?} profile should lower: {err:?}")
                });
            }
        }
    }
}

#[test]
fn app_rows_are_separate_from_hidden_terminal_facts() {
    let request = row_set_output(BTreeSet::from([
        ProgramFactKey::ResultMembership,
        ProgramFactKey::RelationEdges,
        ProgramFactKey::ProgramSourceCoverage(program_scope()),
    ]));

    let app_rows = request.app_rows.as_ref().expect("app rows requested");
    assert!(matches!(
        app_rows.projection,
        PayloadProjection::ShapeDefault
    ));
    assert!(request.facts.contains(&ProgramFactKey::RelationEdges));
}

#[test]
fn policy_decisions_are_dry_run_programs_not_row_values() {
    let decision = PolicyDecisionFactKey {
        role: PolicyDecisionRole::Read,
        fingerprint: vec![0x01],
    };
    let request = row_set_output(BTreeSet::from([
        ProgramFactKey::PolicyDecision {
            decision: decision.clone(),
        },
        ProgramFactKey::PolicyWitnesses,
    ]));

    assert!(
        request
            .facts
            .contains(&ProgramFactKey::PolicyDecision { decision })
    );
}

#[test]
fn policy_decisions_are_tri_state_outputs() {
    let schema = PolicyDecisionSchema {
        outcome_field: "outcome".to_owned(),
        required_input_field: Some("required_input".to_owned()),
        reason_field: Some("reason".to_owned()),
        facts: Vec::new(),
    };
    let outcomes = BTreeSet::from([
        PolicyDecisionOutcome::Allowed,
        PolicyDecisionOutcome::Denied,
        PolicyDecisionOutcome::IndeterminateRequiresInput,
        PolicyDecisionOutcome::RequiresCoverage(program_frontier()),
    ]);

    assert_eq!(schema.outcome_field, "outcome");
    assert!(outcomes.contains(&PolicyDecisionOutcome::IndeterminateRequiresInput));
    assert!(outcomes.contains(&PolicyDecisionOutcome::RequiresCoverage(program_frontier())));
}

#[test]
fn predicate_output_set_facts_carry_compared_versions() {
    let fact = ProgramFactOutput {
        key: ProgramFactKey::PredicateOutputSet {
            role: PredicateOutputSetRole::Base,
        },
        terminal: ProgramFactTerminal::Primary,
        schema: ProgramFactSchema::PredicateOutputSet(PredicateOutputSetSchema {
            role: PredicateOutputSetRole::Base,
            table_field: "table".to_owned(),
            row_field: "row_uuid".to_owned(),
            version: ResultMembershipVersionSchema::Content(ContentVersionFields {
                tx_time_field: "tx_time".to_owned(),
                tx_node_field: "tx_node".to_owned(),
            }),
            shape_id_field: "shape_id".to_owned(),
            binding_id_field: "binding_id".to_owned(),
        }),
    };

    assert_eq!(
        fact.key(),
        ProgramFactKey::PredicateOutputSet {
            role: PredicateOutputSetRole::Base
        }
    );
    assert!(matches!(
        fact.schema,
        ProgramFactSchema::PredicateOutputSet(PredicateOutputSetSchema {
            role: PredicateOutputSetRole::Base,
            ..
        })
    ));
}

#[test]
fn validation_comparison_reads_are_part_of_one_program_request() {
    let mut reads = QueryReadSet::primary(current_read_view());
    reads.fact_reads.insert(
        FactReadRole::PredicateOutputBase,
        ReadView {
            read_schema: schema(0x61),
            policy_schema: schema(0x61),
            sources: BTreeMap::from([(
                source("todos", SourceRole::Root),
                SourceExpr::SnapshotRef {
                    projection: requested_projection(),
                    data: DataSource::Current,
                    snapshot: snapshot(),
                },
            )]),
        },
    );
    reads
        .fact_reads
        .insert(FactReadRole::PredicateOutputNow, current_read_view());
    let request = QueryProgramRequest {
        authorization_mode: QueryAuthorizationMode::TrustedServing,
        reads,
        policy: policy_context(),
        input: row_set_input(0x61),
        output: row_set_output(BTreeSet::from([
            ProgramFactKey::PredicateOutputSet {
                role: PredicateOutputSetRole::Base,
            },
            ProgramFactKey::PredicateOutputSet {
                role: PredicateOutputSetRole::Now,
            },
        ])),
    };

    assert!(
        request
            .reads
            .fact_reads
            .contains_key(&FactReadRole::PredicateOutputBase)
    );
    assert!(
        request
            .reads
            .fact_reads
            .contains_key(&FactReadRole::PredicateOutputNow)
    );
}

#[test]
fn row_read_facts_distinguish_present_and_absent_reads() {
    let present = ProgramFactOutput {
        key: ProgramFactKey::PointReads { present: true },
        terminal: ProgramFactTerminal::Primary,
        schema: ProgramFactSchema::PointReads(PointReadFactSchema {
            table_field: "table".to_owned(),
            row_field: "row_uuid".to_owned(),
            presence_field: "present".to_owned(),
            observed_version_field: Some("observed_tx".to_owned()),
            base_snapshot_field: None,
        }),
    };
    let absent = ProgramFactOutput {
        key: ProgramFactKey::PointReads { present: false },
        terminal: ProgramFactTerminal::Primary,
        schema: ProgramFactSchema::PointReads(PointReadFactSchema {
            table_field: "table".to_owned(),
            row_field: "row_uuid".to_owned(),
            presence_field: "present".to_owned(),
            observed_version_field: None,
            base_snapshot_field: Some("base_snapshot".to_owned()),
        }),
    };

    assert_ne!(present, absent);
    assert_eq!(present.key(), ProgramFactKey::PointReads { present: true });
    assert_eq!(absent.key(), ProgramFactKey::PointReads { present: false });
}

#[test]
fn payload_coverage_is_split_into_small_terminal_facts() {
    let complete = ProgramFactOutput {
        key: ProgramFactKey::CompleteTxPayloadCoverage {
            batch: BatchId(vec![0x01]),
            tier: DurabilityTier::Global,
        },
        terminal: ProgramFactTerminal::Primary,
        schema: ProgramFactSchema::CompleteTxPayloadCoverage(CompleteTxPayloadCoverageSchema {
            batch: BatchIdentityFields {
                batch_id_field: "batch_id".to_owned(),
                batch_node_field: Some("batch_node".to_owned()),
            },
            tier_field: "tier".to_owned(),
            payload_digest_field: "payload_digest".to_owned(),
            fate_field: "fate".to_owned(),
        }),
    };
    let view_complete = ProgramFactKey::ViewCompleteExclusiveCoverage {
        view: program_scope(),
        result: None,
        tier: DurabilityTier::Global,
    };

    assert!(matches!(
        complete.schema,
        ProgramFactSchema::CompleteTxPayloadCoverage(CompleteTxPayloadCoverageSchema { .. })
    ));
    assert_ne!(complete.key(), view_complete);
}

#[test]
fn policy_context_carries_alpha_enforcement_mode() {
    let permissive = PolicyContext::Identity {
        mode: PolicyEnforcementMode::PermissiveLocal,
        permission_subject: author(0xc1),
        claims: BTreeMap::new(),
        attribution: None,
    };
    let enforcing = PolicyContext::Identity {
        mode: PolicyEnforcementMode::Enforcing,
        permission_subject: author(0xc1),
        claims: BTreeMap::new(),
        attribution: None,
    };

    assert_ne!(permissive, enforcing);
}
