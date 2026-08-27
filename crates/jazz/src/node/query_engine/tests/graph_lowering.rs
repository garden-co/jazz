//! Executable Groove graph lowering for linear, joined, recursive, and bound plans.

use super::*;

#[test]
fn shared_row_set_dag_reaches_owned_plan_and_groove_lowering() {
    let mut input = row_set_input(0xd9);
    let shared_source = input.shape.root.clone();
    let union = RowSetNodeId("shared-diamond".to_owned());
    input.shape.nodes.insert(
        union.clone(),
        RowSetExpr::Union {
            inputs: vec![
                UnionInput {
                    node: shared_source.clone(),
                    label: "left".to_owned(),
                },
                UnionInput {
                    node: shared_source,
                    label: "right".to_owned(),
                },
            ],
        },
    );
    input.shape.root = union;
    let request = QueryProgramRequest {
        authorization_mode: QueryAuthorizationMode::TrustedServing,
        reads: QueryReadSet::primary(current_read_view()),
        policy: system_policy_context(),
        input,
        output: RowSetOutputRequest {
            app_rows: None,
            facts: BTreeSet::from([ProgramFactKey::ResultMembership]),
        },
    };

    let program = lower_query_program(request, &mut FakeSourceResolver::default())
        .expect("shared child should become owned occurrences and lower");
    assert!(program.lowered.terminals.iter().any(|terminal| {
        graph_any(&terminal.graph, &|graph| {
            matches!(graph, GraphBuilder::Union { .. })
        })
    }));
}

#[test]
fn simple_current_table_root_query_lowers_for_local_edge_and_global_sync_outputs() {
    for tier in [
        DurabilityTier::Local,
        DurabilityTier::Edge,
        DurabilityTier::Global,
    ] {
        let request = QueryProgramRequest {
            authorization_mode: QueryAuthorizationMode::TrustedServing,
            reads: QueryReadSet::primary(current_read_view_at(tier)),
            policy: system_policy_context(),
            input: row_set_input(tier as u8 + 0x30),
            output: row_set_output(sync_facts()),
        };

        assert_eq!(
            request
                .reads
                .primary
                .source_current_tier(&source("todos", SourceRole::Root)),
            Some(tier)
        );
        assert!(request.output.app_rows.is_some());
        assert!(
            request
                .output
                .facts
                .contains(&ProgramFactKey::ResultMembership)
        );
        assert!(
            request
                .output
                .facts
                .contains(&ProgramFactKey::VersionWitnesses)
        );
        assert!(
            request
                .output
                .facts
                .contains(&ProgramFactKey::SourceCoverage(program_scope()))
        );

        let mut resolver = FakeSourceResolver::default();
        let program =
            lower_query_program(request, &mut resolver).expect("simple current root lowers");
        assert_eq!(resolver.requests.len(), 1);
        let source_request = &resolver.requests[0];
        assert_eq!(source_request.source, source("todos", SourceRole::Root));
        assert_eq!(source_request.visibility, RowVisibility::Visible);
        assert_eq!(
            source_request.requirements.app_fields,
            FieldRequirement::All
        );
        assert!(
            source_request
                .requirements
                .metadata
                .contains(&SourceMetadataRequirement::VersionWitnesses)
        );
        assert!(
            source_request
                .requirements
                .metadata
                .contains(&SourceMetadataRequirement::Coverage)
        );
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
        assert_eq!(program.lowered.parameters, ParameterDomain::default());
        assert_eq!(
            program
                .request
                .reads
                .primary
                .source_current_tier(&source("todos", SourceRole::Root)),
            Some(tier)
        );

        let ProgramOutputSchemas::RowSet(terminals) = &program.lowered.output;
        assert_eq!(terminals.len(), 5);
        assert!(terminals.iter().any(|terminal| {
            matches!(
                terminal,
                OutputTerminalSchema::AppRows(AppRowSchema {
                    descriptor,
                    hidden_fields,
                    carrier: AppRowCarrier::CurrentRow,
                    ..
                }) if descriptor.field_index("user_title").is_some()
                    && hidden_fields.is_empty()
            )
        }));
        assert!(terminals.iter().any(|terminal| {
            matches!(
                terminal,
                OutputTerminalSchema::Fact(ProgramFactOutput {
                    key: ProgramFactKey::ResultMembership,
                    terminal: ProgramFactTerminal::Primary,
                    schema: ProgramFactSchema::ResultMembership(ResultMembershipSchema {
                        version: ResultMembershipVersionSchema::Content(_),
                        ..
                    }),
                })
            )
        }));
        assert!(terminals.iter().any(|terminal| {
            matches!(
                terminal,
                OutputTerminalSchema::Fact(ProgramFactOutput {
                    key: ProgramFactKey::SourceCoverage(CoverageScope::Program),
                    terminal: ProgramFactTerminal::Primary,
                    schema: ProgramFactSchema::SourceCoverage(_),
                })
            )
        }));
        assert!(terminals.iter().any(|terminal| {
            matches!(
                terminal,
                OutputTerminalSchema::Fact(ProgramFactOutput {
                    key: ProgramFactKey::VersionWitnesses,
                    terminal: ProgramFactTerminal::VersionWitnessContent,
                    schema: ProgramFactSchema::VersionWitnesses(VersionWitnessSchemas {
                        content: Some(_),
                        ..
                    }),
                })
            )
        }));
        assert!(terminals.iter().any(|terminal| {
            matches!(
                terminal,
                OutputTerminalSchema::Fact(ProgramFactOutput {
                    key: ProgramFactKey::VersionWitnesses,
                    terminal: ProgramFactTerminal::VersionWitnessDeletion,
                    schema: ProgramFactSchema::VersionWitnesses(VersionWitnessSchemas {
                        deletion: Some(_),
                        ..
                    }),
                })
            )
        }));
        assert!(
            program
                .explain
                .capabilities
                .iter()
                .any(|line| { line.contains("table-rooted current lowering") })
        );
    }
}

#[test]
fn current_source_filter_order_slice_chain_lowers_to_groove_graph() {
    let request = QueryProgramRequest {
        authorization_mode: QueryAuthorizationMode::TrustedServing,
        reads: QueryReadSet::primary(current_read_view()),
        policy: system_policy_context(),
        input: chained_row_set_input(
            0x71,
            BTreeMap::from([("title".to_owned(), Value::String("ship".to_owned()))]),
        ),
        output: RowSetOutputRequest {
            app_rows: None,
            facts: BTreeSet::from([ProgramFactKey::ResultMembership]),
        },
    };

    let mut resolver = FakeSourceResolver::default();
    let program = lower_query_program(request, &mut resolver).expect("linear chain should lower");

    assert_eq!(resolver.requests.len(), 1);
    assert_eq!(
        resolver.requests[0].requirements.app_fields,
        FieldRequirement::Fields(BTreeSet::from(["title".to_owned()]))
    );
    assert!(matches!(
        program.lowered.terminals.first().expect("lowered terminal").graph.clone(),
        GraphBuilder::Project { input, .. }
        if matches!(
            input.as_ref(),
        GraphBuilder::TopBy {
            input,
            group_cols,
            order_cols,
            tie_cols,
            offset: 1,
            limit: groove::ivm::TopByLimit::Finite(2),
        } if group_cols.is_empty()
            && matches!(order_cols.as_slice(), [groove::ivm::TopByOrder {
                field: groove::ivm::FieldRef::Name(field),
                direction: groove::ivm::TopByDirection::Asc,
            }] if field == "user_title")
            && matches!(tie_cols.as_slice(), [groove::ivm::FieldRef::Name(field)]
                if field == "row_uuid")
            && matches!(
                input.as_ref(),
                GraphBuilder::Filter {
                    input,
                    predicate: groove::ivm::PredicateExpr::Eq { field, value },
                    ..
                } if matches!(
                    input.as_ref(),
                    GraphBuilder::Table { table, .. } if table == "resolved_todos"
                ) && field == "user_title"
                    && value == &groove::ivm::LiteralValue::String("ship".to_owned())
            )
        )
    ));
    assert_eq!(program.lowered.parameters, ParameterDomain::default());
    assert!(
        program
            .explain
            .capabilities
            .iter()
            .any(|line| { line.contains("table-rooted current lowering") })
    );
}

#[test]
fn current_source_select_projection_and_default_ordered_slice_lower() {
    let root = RowSetNodeId("root".to_owned());
    let slice = RowSetNodeId("slice".to_owned());
    let root_source = source("todos", SourceRole::Root);
    let request = QueryProgramRequest {
        authorization_mode: QueryAuthorizationMode::TrustedServing,
        reads: QueryReadSet::primary(current_read_view()),
        policy: system_policy_context(),
        input: RowSetProgramInput {
            shape: NormalizedRowSetShape {
                identity: NormalizedShapeIdentity {
                    shape_id: shape(0x74),
                    canonical: vec![0x74],
                },
                root: slice.clone(),
                result: ResultId::RealRow {
                    table: "todos".to_owned(),
                    row: ResultRowRef::Source(root_source.clone()),
                },
                auxiliary_sources: BTreeSet::new(),
                closure_paths: Vec::new(),
                join_contributions: Vec::new(),
                reachable_contributions: Vec::new(),
                nodes: BTreeMap::from([
                    (
                        root.clone(),
                        RowSetExpr::Source {
                            source: root_source.clone(),
                            visibility: RowVisibility::Visible,
                        },
                    ),
                    (
                        slice.clone(),
                        RowSetExpr::Slice {
                            input: root,
                            partition_by: Vec::new(),
                            limit: Some(3),
                            offset: 2,
                            tie_breaker: vec![NormalizedValueRef::RowId(RowIdRef::Source(
                                root_source.clone(),
                            ))],
                            rank_output: None,
                        },
                    ),
                ]),
            },
            binding: ProgramBinding {
                id: BindingId(uuid::Uuid::from_bytes([0x74; 16])),
                source_shape: None,
                extra_user_params: BTreeMap::new(),
                param_types: BTreeMap::new(),
                claim_params: BTreeMap::new(),
                values: BTreeMap::new(),
            },
        },
        output: RowSetOutputRequest {
            app_rows: Some(AppRowOutputRequest {
                public_terminal: true,
                projection: PayloadProjection::Tree(AppProjectionTree {
                    fields: FieldProjection::Fields(BTreeSet::from(["title".to_owned()])),
                    paths: Vec::new(),
                }),
            }),
            facts: BTreeSet::new(),
        },
    };

    let mut resolver = FakeSourceResolver::default();
    let program =
        lower_query_program(request, &mut resolver).expect("projected unordered slice lowers");

    assert_eq!(resolver.requests.len(), 1);
    assert_eq!(
        resolver.requests[0].requirements.app_fields,
        FieldRequirement::Fields(BTreeSet::from(["title".to_owned()]))
    );
    let app_rows = &program
        .lowered
        .terminals
        .first()
        .expect("lowered terminal")
        .graph;
    assert_public_root_terminal(app_rows);
    assert!(graph_any(app_rows, &|graph| matches!(
        graph,
        GraphBuilder::TopBy {
            input,
            group_cols,
            order_cols,
            tie_cols,
            offset: 2,
            limit: groove::ivm::TopByLimit::Finite(3),
        } if matches!(input.as_ref(), GraphBuilder::Table { table, .. } if table == "resolved_todos")
            && group_cols.is_empty()
            && matches!(order_cols.as_slice(), [groove::ivm::TopByOrder {
                field: groove::ivm::FieldRef::Name(field),
                direction: groove::ivm::TopByDirection::Asc,
            }] if field == "row_uuid")
            && tie_cols.is_empty()
    )));
}

#[test]
fn current_join_via_lowers_as_left_deep_semijoin() {
    let root = RowSetNodeId("root".to_owned());
    let join_source_node = RowSetNodeId("join-source".to_owned());
    let join_filter = RowSetNodeId("join-filter".to_owned());
    let join_node = RowSetNodeId("join".to_owned());
    let root_source = source("todos", SourceRole::Root);
    let join_source = source("todo_tags", SourceRole::Alias("join_via:0".to_owned()));
    let request = QueryProgramRequest {
        authorization_mode: QueryAuthorizationMode::TrustedServing,
        reads: QueryReadSet::primary(joined_current_read_view()),
        policy: system_policy_context(),
        input: RowSetProgramInput {
            shape: NormalizedRowSetShape {
                identity: NormalizedShapeIdentity {
                    shape_id: shape(0x73),
                    canonical: vec![0x73],
                },
                root: join_node.clone(),
                result: ResultId::RealRow {
                    table: "todos".to_owned(),
                    row: ResultRowRef::Source(root_source.clone()),
                },
                auxiliary_sources: BTreeSet::new(),
                closure_paths: Vec::new(),
                join_contributions: Vec::new(),
                reachable_contributions: Vec::new(),
                nodes: BTreeMap::from([
                    (
                        root.clone(),
                        RowSetExpr::Source {
                            source: root_source.clone(),
                            visibility: RowVisibility::Visible,
                        },
                    ),
                    (
                        join_source_node.clone(),
                        RowSetExpr::Source {
                            source: join_source.clone(),
                            visibility: RowVisibility::Visible,
                        },
                    ),
                    (
                        join_filter.clone(),
                        RowSetExpr::Filter {
                            input: join_source_node,
                            predicate: PredicateExpr::Compare {
                                left: NormalizedValueRef::SourceField {
                                    source: join_source.clone(),
                                    field: "tag".to_owned(),
                                },
                                op: ComparisonOp::Eq,
                                right: NormalizedValueRef::Literal(
                                    postcard::to_allocvec(&Value::String("ship".to_owned()))
                                        .unwrap(),
                                ),
                            },
                        },
                    ),
                    (
                        join_node.clone(),
                        RowSetExpr::Join {
                            left: root,
                            right: join_filter,
                            mode: JoinMode::Inner,
                            on: PredicateExpr::Compare {
                                left: NormalizedValueRef::RowId(RowIdRef::Source(
                                    root_source.clone(),
                                )),
                                op: ComparisonOp::Eq,
                                right: NormalizedValueRef::SourceField {
                                    source: join_source.clone(),
                                    field: "todo".to_owned(),
                                },
                            },
                        },
                    ),
                ]),
            },
            binding: ProgramBinding {
                id: BindingId(uuid::Uuid::from_bytes([0x73; 16])),
                source_shape: None,
                extra_user_params: BTreeMap::new(),
                param_types: BTreeMap::new(),
                claim_params: BTreeMap::new(),
                values: BTreeMap::new(),
            },
        },
        output: row_set_output(BTreeSet::new()),
    };

    let mut resolver = FakeSourceResolver::default();
    let program = lower_query_program(request, &mut resolver).expect("join_via should lower");

    assert_eq!(resolver.requests.len(), 2);
    assert!(resolver.requests.iter().any(|request| {
        request.source == root_source && request.requirements.app_fields == FieldRequirement::All
    }));
    assert!(resolver.requests.iter().any(|request| {
        request.source == join_source
            && request.requirements.app_fields
                == FieldRequirement::Fields(BTreeSet::from(["tag".to_owned(), "todo".to_owned()]))
    }));
    let app_rows = &program
        .lowered
        .terminals
        .first()
        .expect("lowered terminal")
        .graph;
    assert_public_root_terminal(app_rows);
    assert!(matches!(
        app_rows,
        GraphBuilder::CollectBy { collect, .. }
            if collect.group_cols.iter().any(|field| matches!(
                field,
                groove::ivm::FieldRef::Name(name)
                    if name == "__collect_root___root_join_row_0"
            )) && collect.tie_cols.iter().any(|field| matches!(
                field,
                groove::ivm::FieldRef::Name(name)
                    if name == "__collect_root___root_join_row_0"
            ))
    ));
    assert!(graph_any(app_rows, &|graph| matches!(
        graph,
        GraphBuilder::Project { input, fields }
            if fields.iter().any(|field| field.output_name == "row_uuid")
                && matches!(
                    input.as_ref(),
                    GraphBuilder::Join {
                        left,
                        right,
                        left_on,
                        right_on,
                        ..
                    } if matches!(left.as_ref(), GraphBuilder::Table { table, .. } if table == "resolved_todos")
                        && matches!(
                            right.as_ref(),
                            GraphBuilder::UnwrapNullable { input, field }
                                if matches!(field, groove::ivm::FieldRef::Name(name) if name == "user_todo")
                                    && matches!(
                                        input.as_ref(),
                                        GraphBuilder::Filter { input, predicate, .. }
                                            if matches!(
                                                input.as_ref(),
                                                GraphBuilder::Table { table, .. } if table == "resolved_todo_tags"
                                            ) && matches!(
                                                predicate,
                                                groove::ivm::PredicateExpr::Eq { field, value }
                                                    if field == "user_tag"
                                                        && value == &groove::ivm::LiteralValue::String("ship".to_owned())
                                            )
                                    )
                        )
                        && matches!(left_on.as_slice(), [groove::ivm::FieldRef::Name(name)] if name == "row_uuid")
                        && matches!(right_on.as_slice(), [groove::ivm::FieldRef::Name(name)] if name == "user_todo")
                )
    )));
}

#[test]
fn current_join_via_can_use_union_relation_input() {
    assert_current_join_via_union_relation_input(
        source("todo_tags", SourceRole::Policy("direct".to_owned())),
        source("todo_tags", SourceRole::Policy("inherited".to_owned())),
        false,
    );
}

/// A UNION may have a common aliased source even when each arm has different
/// predicates. The root source is then available, but must not be mistaken
/// for a non-union join occurrence.
#[test]
fn current_join_via_shared_alias_union_keeps_arm_and_row_carriers() {
    let shared = source("todo_tags", SourceRole::Alias("policy_branch".to_owned()));
    assert_current_join_via_union_relation_input(shared.clone(), shared, false);
}

/// The shared-alias UNION must retain its full `(arm, row)` occurrence identity
/// when an additional INNER JOIN causes the first join input to be flattened.
#[test]
fn current_join_via_shared_alias_union_keeps_carriers_through_consecutive_inner_join() {
    let shared = source("todo_tags", SourceRole::Alias("policy_branch".to_owned()));
    assert_current_join_via_union_relation_input(shared.clone(), shared, true);
}

fn assert_current_join_via_union_relation_input(
    direct_source: SourceId,
    inherited_source: SourceId,
    consecutive_inner_join: bool,
) {
    let root = RowSetNodeId("root".to_owned());
    let direct_source_node = RowSetNodeId("direct-source".to_owned());
    let direct_project = RowSetNodeId("direct-project".to_owned());
    let inherited_source_node = RowSetNodeId("inherited-source".to_owned());
    let inherited_project = RowSetNodeId("inherited-project".to_owned());
    let union_node = RowSetNodeId("authorized-union".to_owned());
    let join_node = RowSetNodeId("join".to_owned());
    let ordinary_source_node = RowSetNodeId("ordinary-source".to_owned());
    let ordinary_project = RowSetNodeId("ordinary-project".to_owned());
    let terminal_join = RowSetNodeId("terminal-join".to_owned());
    let root_source = source("todos", SourceRole::Root);
    let ordinary_source = source("todo_tags", SourceRole::Alias("ordinary".to_owned()));
    let mut sources = BTreeMap::from([
        (
            root_source.clone(),
            requested_current_source(DurabilityTier::Global),
        ),
        (
            direct_source.clone(),
            requested_current_source(DurabilityTier::Global),
        ),
        (
            inherited_source.clone(),
            requested_current_source(DurabilityTier::Global),
        ),
    ]);
    if consecutive_inner_join {
        sources.insert(
            ordinary_source.clone(),
            requested_current_source(DurabilityTier::Global),
        );
    }
    let request = QueryProgramRequest {
        authorization_mode: QueryAuthorizationMode::TrustedServing,
        reads: QueryReadSet::primary(ReadView {
            read_schema: schema(0x10),
            policy_schema: schema(0x11),
            sources,
        }),
        policy: system_policy_context(),
        input: RowSetProgramInput {
            shape: NormalizedRowSetShape {
                identity: NormalizedShapeIdentity {
                    shape_id: shape(0x7a),
                    canonical: vec![0x7a],
                },
                root: if consecutive_inner_join {
                    terminal_join.clone()
                } else {
                    join_node.clone()
                },
                result: ResultId::RealRow {
                    table: "todos".to_owned(),
                    row: ResultRowRef::Source(root_source.clone()),
                },
                auxiliary_sources: BTreeSet::new(),
                closure_paths: Vec::new(),
                join_contributions: Vec::new(),
                reachable_contributions: Vec::new(),
                nodes: {
                    let mut nodes = BTreeMap::from([
                        (
                            root.clone(),
                            RowSetExpr::Source {
                                source: root_source.clone(),
                                visibility: RowVisibility::Visible,
                            },
                        ),
                        (
                            direct_source_node.clone(),
                            RowSetExpr::Source {
                                source: direct_source.clone(),
                                visibility: RowVisibility::Visible,
                            },
                        ),
                        (
                            direct_project.clone(),
                            RowSetExpr::Project {
                                input: direct_source_node,
                                columns: vec![RowProjection {
                                    output: TypedOutputField {
                                        name: "todo".to_owned(),
                                        ty: ColumnType::Uuid,
                                    },
                                    value: NormalizedValueRef::SourceField {
                                        source: direct_source.clone(),
                                        field: "todo".to_owned(),
                                    },
                                }],
                            },
                        ),
                        (
                            inherited_source_node.clone(),
                            RowSetExpr::Source {
                                source: inherited_source.clone(),
                                visibility: RowVisibility::Visible,
                            },
                        ),
                        (
                            inherited_project.clone(),
                            RowSetExpr::Project {
                                input: inherited_source_node,
                                columns: vec![RowProjection {
                                    output: TypedOutputField {
                                        name: "todo".to_owned(),
                                        ty: ColumnType::Uuid,
                                    },
                                    value: NormalizedValueRef::SourceField {
                                        source: inherited_source.clone(),
                                        field: "todo".to_owned(),
                                    },
                                }],
                            },
                        ),
                        (
                            union_node.clone(),
                            RowSetExpr::Union {
                                inputs: vec![
                                    UnionInput {
                                        node: direct_project,
                                        label: "direct".to_owned(),
                                    },
                                    UnionInput {
                                        node: inherited_project,
                                        label: "inherited".to_owned(),
                                    },
                                ],
                            },
                        ),
                        (
                            join_node.clone(),
                            RowSetExpr::Join {
                                left: root,
                                right: union_node,
                                mode: JoinMode::Inner,
                                on: PredicateExpr::Compare {
                                    left: NormalizedValueRef::RowId(RowIdRef::Source(
                                        root_source.clone(),
                                    )),
                                    op: ComparisonOp::Eq,
                                    right: NormalizedValueRef::SourceField {
                                        source: root_source.clone(),
                                        field: "todo".to_owned(),
                                    },
                                },
                            },
                        ),
                    ]);
                    if consecutive_inner_join {
                        nodes.insert(
                            ordinary_source_node.clone(),
                            RowSetExpr::Source {
                                source: ordinary_source.clone(),
                                visibility: RowVisibility::Visible,
                            },
                        );
                        nodes.insert(
                            ordinary_project.clone(),
                            RowSetExpr::Project {
                                input: ordinary_source_node,
                                columns: vec![RowProjection {
                                    output: TypedOutputField {
                                        name: "todo".to_owned(),
                                        ty: ColumnType::Uuid,
                                    },
                                    value: NormalizedValueRef::SourceField {
                                        source: ordinary_source.clone(),
                                        field: "todo".to_owned(),
                                    },
                                }],
                            },
                        );
                        nodes.insert(
                            terminal_join,
                            RowSetExpr::Join {
                                left: join_node,
                                right: ordinary_project,
                                mode: JoinMode::Inner,
                                on: PredicateExpr::Compare {
                                    left: NormalizedValueRef::RowId(RowIdRef::Source(
                                        root_source.clone(),
                                    )),
                                    op: ComparisonOp::Eq,
                                    right: NormalizedValueRef::SourceField {
                                        source: ordinary_source,
                                        field: "todo".to_owned(),
                                    },
                                },
                            },
                        );
                    }
                    nodes
                },
            },
            binding: ProgramBinding {
                id: BindingId(uuid::Uuid::from_bytes([0x7a; 16])),
                source_shape: None,
                extra_user_params: BTreeMap::new(),
                param_types: BTreeMap::new(),
                claim_params: BTreeMap::new(),
                values: BTreeMap::new(),
            },
        },
        output: row_set_output(BTreeSet::from([ProgramFactKey::ResultMembership])),
    };

    let program = lower_query_program(request, &mut FakeSourceResolver::default())
        .expect("union relation input should lower");
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
            if fields.iter().any(|field| field.output_name == "__root_join_arm_0")
                && fields.iter().any(|field| field.output_name == "__root_join_row_0")
                && matches!(
                    input.as_ref(),
                    GraphBuilder::Join { right, right_on, .. }
                        if matches!(right.as_ref(), GraphBuilder::Union { inputs } if inputs.len() == 2)
                            && matches!(right_on.as_slice(), [groove::ivm::FieldRef::Name(name)] if name == "todo")
                )
    )));
    let membership = program
        .lowered
        .terminals
        .iter()
        .find(|terminal| terminal.sink == "maintained.result_current")
        .expect("result-membership terminal");
    let ProgramOutputSchemas::RowSet(outputs) = &program.lowered.output;
    let schema = outputs
        .iter()
        .find_map(|output| match output {
            OutputTerminalSchema::Fact(ProgramFactOutput {
                schema: ProgramFactSchema::ResultMembership(schema),
                ..
            }) => Some(schema),
            _ => None,
        })
        .expect("result-membership schema");
    assert_eq!(
        schema.occurrence_id_fields,
        ["row_uuid", "__root_join_row_0"]
    );
    assert_eq!(
        schema
            .occurrence_union_arm_fields
            .get(&0)
            .map(String::as_str),
        Some("__root_join_arm_0")
    );
    assert!(graph_any(&membership.graph, &|graph| matches!(
        graph,
        GraphBuilder::Project { fields, .. }
            if fields.iter().any(|field| field.output_name == "__root_join_arm_0")
                && fields.iter().any(|field| field.output_name == "__root_join_row_0")
    )));
    if consecutive_inner_join {
        assert!(graph_any(&membership.graph, &|graph| matches!(
            graph,
            GraphBuilder::Project { input, fields }
                if fields.iter().any(|field| field.output_name == "__root_join_arm_0")
                    && fields.iter().any(|field| field.output_name == "__root_join_row_0")
                    && matches!(input.as_ref(), GraphBuilder::Join { right, .. }
                        if !matches!(right.as_ref(), GraphBuilder::Union { .. }))
        )));
    }
}

#[test]
fn current_join_via_lowers_source_column_row_id_target_and_correlations() {
    let root = RowSetNodeId("root".to_owned());
    let join_source_node = RowSetNodeId("join-source".to_owned());
    let join_node = RowSetNodeId("join".to_owned());
    let root_source = source("todos", SourceRole::Root);
    let join_source = source("todo_tags", SourceRole::Alias("join_via:0".to_owned()));
    let request = QueryProgramRequest {
        authorization_mode: QueryAuthorizationMode::TrustedServing,
        reads: QueryReadSet::primary(joined_current_read_view()),
        policy: system_policy_context(),
        input: RowSetProgramInput {
            shape: NormalizedRowSetShape {
                identity: NormalizedShapeIdentity {
                    shape_id: shape(0x74),
                    canonical: vec![0x74],
                },
                root: join_node.clone(),
                result: ResultId::RealRow {
                    table: "todos".to_owned(),
                    row: ResultRowRef::Source(root_source.clone()),
                },
                auxiliary_sources: BTreeSet::new(),
                closure_paths: Vec::new(),
                join_contributions: vec![JoinContribution {
                    id: "join_via:0".to_owned(),
                    source: join_source.clone(),
                    input: join_source_node.clone(),
                    membership: PredicateExpr::And(vec![
                        PredicateExpr::Compare {
                            left: NormalizedValueRef::SourceField {
                                source: root_source.clone(),
                                field: "todo".to_owned(),
                            },
                            op: ComparisonOp::Eq,
                            right: NormalizedValueRef::RowId(RowIdRef::Source(join_source.clone())),
                        },
                        PredicateExpr::Compare {
                            left: NormalizedValueRef::SourceField {
                                source: root_source.clone(),
                                field: "tag".to_owned(),
                            },
                            op: ComparisonOp::Eq,
                            right: NormalizedValueRef::SourceField {
                                source: join_source.clone(),
                                field: "tag".to_owned(),
                            },
                        },
                    ]),
                }],
                reachable_contributions: Vec::new(),
                nodes: BTreeMap::from([
                    (
                        root.clone(),
                        RowSetExpr::Source {
                            source: root_source.clone(),
                            visibility: RowVisibility::Visible,
                        },
                    ),
                    (
                        join_source_node.clone(),
                        RowSetExpr::Source {
                            source: join_source.clone(),
                            visibility: RowVisibility::Visible,
                        },
                    ),
                    (
                        join_node.clone(),
                        RowSetExpr::Join {
                            left: root,
                            right: join_source_node,
                            mode: JoinMode::Inner,
                            on: PredicateExpr::And(vec![
                                PredicateExpr::Compare {
                                    left: NormalizedValueRef::SourceField {
                                        source: root_source.clone(),
                                        field: "todo".to_owned(),
                                    },
                                    op: ComparisonOp::Eq,
                                    right: NormalizedValueRef::RowId(RowIdRef::Source(
                                        join_source.clone(),
                                    )),
                                },
                                PredicateExpr::Compare {
                                    left: NormalizedValueRef::SourceField {
                                        source: root_source.clone(),
                                        field: "tag".to_owned(),
                                    },
                                    op: ComparisonOp::Eq,
                                    right: NormalizedValueRef::SourceField {
                                        source: join_source.clone(),
                                        field: "tag".to_owned(),
                                    },
                                },
                            ]),
                        },
                    ),
                ]),
            },
            binding: ProgramBinding {
                id: BindingId(uuid::Uuid::from_bytes([0x74; 16])),
                source_shape: None,
                extra_user_params: BTreeMap::new(),
                param_types: BTreeMap::new(),
                claim_params: BTreeMap::new(),
                values: BTreeMap::new(),
            },
        },
        output: row_set_output(BTreeSet::from([ProgramFactKey::ResultMembership])),
    };

    let mut resolver = FakeSourceResolver::default();
    let program = lower_query_program(request, &mut resolver)
        .expect("source-column row-id join_via with correlations should lower");

    let app_rows = program
        .lowered
        .terminals
        .iter()
        .find(|terminal| terminal.sink == "app_rows")
        .expect("app rows terminal");
    assert_public_root_terminal(&app_rows.graph);
    assert!(graph_any(&app_rows.graph, &|graph| matches!(
        graph,
        GraphBuilder::Project { input, .. }
            if matches!(
                input.as_ref(),
                GraphBuilder::Join { left, right, left_on, right_on, .. }
                    if matches!(left.as_ref(), GraphBuilder::UnwrapNullable { .. })
                        && matches!(right.as_ref(), GraphBuilder::UnwrapNullable { .. })
                        && matches!(
                            left_on.as_slice(),
                            [
                                groove::ivm::FieldRef::Name(todo),
                                groove::ivm::FieldRef::Name(tag)
                            ] if todo == "user_todo" && tag == "user_tag"
                        )
                        && matches!(
                            right_on.as_slice(),
                            [
                                groove::ivm::FieldRef::Name(row_uuid),
                                groove::ivm::FieldRef::Name(tag)
                            ] if row_uuid == "row_uuid" && tag == "user_tag"
                        )
            )
    )));
}

#[test]
fn join_contribution_membership_can_use_projected_bridge_fields() {
    let root = RowSetNodeId("root".to_owned());
    let join_source_node = RowSetNodeId("join-source".to_owned());
    let bridge_node = RowSetNodeId("bridge".to_owned());
    let app_join_node = RowSetNodeId("app-join".to_owned());
    let root_source = source("todos", SourceRole::Root);
    let join_source = source("todo_tags", SourceRole::Alias("join_via:0".to_owned()));
    let request = QueryProgramRequest {
        authorization_mode: QueryAuthorizationMode::TrustedServing,
        reads: QueryReadSet::primary(joined_current_read_view()),
        policy: system_policy_context(),
        input: RowSetProgramInput {
            shape: NormalizedRowSetShape {
                identity: NormalizedShapeIdentity {
                    shape_id: shape(0x76),
                    canonical: vec![0x76],
                },
                root: app_join_node.clone(),
                result: ResultId::RealRow {
                    table: "todos".to_owned(),
                    row: ResultRowRef::Source(root_source.clone()),
                },
                auxiliary_sources: BTreeSet::new(),
                closure_paths: Vec::new(),
                join_contributions: vec![JoinContribution {
                    id: "join_via:0".to_owned(),
                    source: join_source.clone(),
                    input: bridge_node.clone(),
                    membership: PredicateExpr::Compare {
                        left: NormalizedValueRef::RowId(RowIdRef::Source(root_source.clone())),
                        op: ComparisonOp::Eq,
                        right: NormalizedValueRef::SourceField {
                            source: join_source.clone(),
                            field: "bridge_root".to_owned(),
                        },
                    },
                }],
                reachable_contributions: Vec::new(),
                nodes: BTreeMap::from([
                    (
                        root.clone(),
                        RowSetExpr::Source {
                            source: root_source.clone(),
                            visibility: RowVisibility::Visible,
                        },
                    ),
                    (
                        join_source_node.clone(),
                        RowSetExpr::Source {
                            source: join_source.clone(),
                            visibility: RowVisibility::Visible,
                        },
                    ),
                    (
                        bridge_node.clone(),
                        RowSetExpr::Project {
                            input: join_source_node,
                            columns: vec![
                                RowProjection {
                                    output: TypedOutputField {
                                        name: "bridge_root".to_owned(),
                                        ty: ColumnType::Uuid,
                                    },
                                    value: NormalizedValueRef::SourceField {
                                        source: join_source.clone(),
                                        field: "todo".to_owned(),
                                    },
                                },
                                RowProjection {
                                    output: TypedOutputField {
                                        name: "tag".to_owned(),
                                        ty: ColumnType::String,
                                    },
                                    value: NormalizedValueRef::SourceField {
                                        source: join_source.clone(),
                                        field: "tag".to_owned(),
                                    },
                                },
                                RowProjection {
                                    output: TypedOutputField {
                                        name: "id".to_owned(),
                                        ty: ColumnType::Uuid,
                                    },
                                    value: NormalizedValueRef::RowId(RowIdRef::Source(
                                        join_source.clone(),
                                    )),
                                },
                            ],
                        },
                    ),
                    (
                        app_join_node.clone(),
                        RowSetExpr::Join {
                            left: root,
                            right: bridge_node.clone(),
                            mode: JoinMode::Inner,
                            on: PredicateExpr::Compare {
                                left: NormalizedValueRef::RowId(RowIdRef::Source(
                                    root_source.clone(),
                                )),
                                op: ComparisonOp::Eq,
                                right: NormalizedValueRef::SourceField {
                                    source: join_source.clone(),
                                    field: "bridge_root".to_owned(),
                                },
                            },
                        },
                    ),
                ]),
            },
            binding: ProgramBinding {
                id: BindingId(uuid::Uuid::from_bytes([0x76; 16])),
                source_shape: None,
                extra_user_params: BTreeMap::new(),
                param_types: BTreeMap::new(),
                claim_params: BTreeMap::new(),
                values: BTreeMap::new(),
            },
        },
        output: row_set_output(BTreeSet::from([ProgramFactKey::ResultMembership])),
    };

    let mut resolver = FakeSourceResolver::default();
    let program = lower_query_program(request, &mut resolver)
        .expect("join contribution membership should accept projected bridge fields");

    let app_rows = program
        .lowered
        .terminals
        .iter()
        .find(|terminal| terminal.sink == "app_rows")
        .expect("app rows terminal");
    assert_public_root_terminal(&app_rows.graph);
    assert!(graph_any(&app_rows.graph, &|graph| matches!(
        graph,
        GraphBuilder::Project { input, fields }
            if fields.iter().any(|field| field.output_name == "row_uuid")
                && matches!(
                    input.as_ref(),
                    GraphBuilder::Join { left_on, right_on, .. }
                        if matches!(left_on.as_slice(), [groove::ivm::FieldRef::Name(name)] if name == "row_uuid")
                            && matches!(right_on.as_slice(), [groove::ivm::FieldRef::Name(name)] if name == "bridge_root")
                )
    )));
}

#[test]
fn correlated_path_projection_lowers_with_relation_fact_schemas() {
    let parent_node = RowSetNodeId("parent".to_owned());
    let child_node = RowSetNodeId("child".to_owned());
    let path_node = RowSetNodeId("path".to_owned());
    let parent_source = source("todos", SourceRole::Root);
    let child_source = source("todo_tags", SourceRole::CorrelatedChild("tags".to_owned()));
    let path = ProgramPathId {
        owner: parent_source.clone(),
        child: child_source.clone(),
    };
    let request = QueryProgramRequest {
        authorization_mode: QueryAuthorizationMode::TrustedServing,
        reads: QueryReadSet::primary(path_current_read_view()),
        policy: system_policy_context(),
        input: RowSetProgramInput {
            shape: NormalizedRowSetShape {
                identity: NormalizedShapeIdentity {
                    shape_id: shape(0x75),
                    canonical: vec![0x75],
                },
                root: path_node.clone(),
                result: ResultId::RealRow {
                    table: "todos".to_owned(),
                    row: ResultRowRef::Source(parent_source.clone()),
                },
                auxiliary_sources: BTreeSet::new(),
                closure_paths: Vec::new(),
                join_contributions: Vec::new(),
                reachable_contributions: Vec::new(),
                nodes: BTreeMap::from([
                    (
                        parent_node.clone(),
                        RowSetExpr::Source {
                            source: parent_source.clone(),
                            visibility: RowVisibility::Visible,
                        },
                    ),
                    (
                        child_node.clone(),
                        RowSetExpr::Source {
                            source: child_source.clone(),
                            visibility: RowVisibility::Visible,
                        },
                    ),
                    (
                        path_node.clone(),
                        RowSetExpr::CorrelatedPathProjection {
                            input: parent_node,
                            child_input: child_node,
                            path,
                            correlation: PredicateExpr::Compare {
                                left: NormalizedValueRef::RowId(RowIdRef::Source(
                                    parent_source.clone(),
                                )),
                                op: ComparisonOp::Eq,
                                right: NormalizedValueRef::SourceField {
                                    source: child_source.clone(),
                                    field: "todo".to_owned(),
                                },
                            },
                            requirement: CorrelationRequirement::MatchCorrelationCardinality,
                        },
                    ),
                ]),
            },
            binding: ProgramBinding {
                id: BindingId(uuid::Uuid::from_bytes([0x75; 16])),
                source_shape: None,
                extra_user_params: BTreeMap::new(),
                param_types: BTreeMap::new(),
                claim_params: BTreeMap::new(),
                values: BTreeMap::new(),
            },
        },
        output: RowSetOutputRequest {
            app_rows: None,
            facts: BTreeSet::from([
                ProgramFactKey::RelationEdges,
                ProgramFactKey::PathCorrelationCoverage,
            ]),
        },
    };

    let mut resolver = FakeSourceResolver::default();
    let program =
        lower_query_program(request, &mut resolver).expect("correlated path should lower");

    assert_eq!(resolver.requests.len(), 2);
    assert!(resolver.requests.iter().all(|request| {
        request
            .requirements
            .metadata
            .contains(&SourceMetadataRequirement::VersionWitnesses)
    }));
    assert!(matches!(
        program.lowered.terminals.first().expect("lowered terminal").graph.clone(),
        GraphBuilder::Project { input, fields }
            if fields.iter().any(|field| field.output_name == "source_row")
                && fields.iter().any(|field| field.output_name == "target_row")
                && fields.iter().any(|field| field.output_name == "path")
                && matches!(
                    input.as_ref(),
                    GraphBuilder::Join {
                        left_on,
                        right_on,
                        ..
                    } if matches!(left_on.as_slice(), [groove::ivm::FieldRef::Name(name)] if name == "row_uuid")
                        && matches!(right_on.as_slice(), [groove::ivm::FieldRef::Name(name)] if name == "user_todo")
                )
    ));
    let ProgramOutputSchemas::RowSet(terminals) = &program.lowered.output;
    assert_eq!(terminals.len(), 2);
    assert!(terminals.iter().any(|terminal| {
        matches!(
            terminal,
            OutputTerminalSchema::Fact(ProgramFactOutput {
                key: ProgramFactKey::RelationEdges,
                terminal: ProgramFactTerminal::Primary,
                schema: ProgramFactSchema::RelationEdges(RelationEdgeSchema {
                    role_field: Some(_),
                    depth_field: None,
                    ..
                }),
            })
        )
    }));
    assert!(terminals.iter().any(|terminal| {
        matches!(
            terminal,
            OutputTerminalSchema::Fact(ProgramFactOutput {
                key: ProgramFactKey::PathCorrelationCoverage,
                terminal: ProgramFactTerminal::Primary,
                schema: ProgramFactSchema::PathCorrelationCoverage(PathCorrelationCoverageSchema {
                    expected_count_field: Some(_),
                    ..
                }),
            })
        )
    }));
}

#[test]
fn unordered_bounded_correlated_child_window_defaults_to_child_row_id_order() {
    // Internal lowering test: the public Db relation stream carries flat
    // relation edges, whose per-parent row-id comparator is materialized at
    // the terminal rather than reimplemented in the test.
    let child_node = RowSetNodeId("child".to_owned());
    let child_slice = RowSetNodeId("child_slice".to_owned());
    let path_node = RowSetNodeId("path".to_owned());
    let child_source = source("todo_tags", SourceRole::CorrelatedChild("tags".to_owned()));
    let mut request = correlated_path_request(
        CorrelationRequirement::Optional,
        row_set_output(BTreeSet::from([
            ProgramFactKey::ResultMembership,
            ProgramFactKey::RelationEdges,
        ])),
    );
    request.input.shape.nodes.insert(
        child_slice.clone(),
        RowSetExpr::Slice {
            input: child_node.clone(),
            partition_by: vec![NormalizedValueRef::SourceField {
                source: child_source.clone(),
                field: "todo".to_owned(),
            }],
            limit: Some(2),
            offset: 1,
            tie_breaker: vec![NormalizedValueRef::RowId(RowIdRef::Source(
                child_source.clone(),
            ))],
            rank_output: None,
        },
    );
    let RowSetExpr::CorrelatedPathProjection { child_input, .. } = request
        .input
        .shape
        .nodes
        .get_mut(&path_node)
        .expect("correlated path node")
    else {
        panic!("path node must remain a correlated path projection");
    };
    *child_input = child_slice;

    let mut resolver = FakeSourceResolver::default();
    let program = lower_query_program(request, &mut resolver)
        .expect("unordered bounded child window should lower");

    assert!(program.lowered.terminals.iter().any(|terminal| matches!(
        terminal.output,
        OutputTerminalSchema::Fact(ProgramFactOutput {
            key: ProgramFactKey::RelationEdges,
            ..
        })
    )));
}

#[test]
fn recursive_relation_seed_claim_lowers_from_policy_context() {
    let seed_node = RowSetNodeId("seed".to_owned());
    let frontier_node = RowSetNodeId("frontier".to_owned());
    let step_node = RowSetNodeId("step".to_owned());
    let step_join = RowSetNodeId("step-join".to_owned());
    let step_project = RowSetNodeId("step-project".to_owned());
    let relation_node = RowSetNodeId("relation".to_owned());
    let frontier = FrontierId("reachable".to_owned());
    let step_source = source("todos", SourceRole::RecursiveStep("step".to_owned()));
    let subject = author(0xa7);
    let frontier_columns = vec![
        ValueSourceColumn {
            name: "team".to_owned(),
            value: NormalizedValueRef::Claim(ClaimPath(vec!["sub".to_owned()])),
            ty: ColumnType::Uuid,
        },
        ValueSourceColumn {
            name: "reachable_team".to_owned(),
            value: NormalizedValueRef::Claim(ClaimPath(vec!["sub".to_owned()])),
            ty: ColumnType::Uuid,
        },
    ];
    let request = QueryProgramRequest {
        authorization_mode: QueryAuthorizationMode::TrustedServing,
        reads: QueryReadSet::primary(recursive_current_read_view()),
        policy: PolicyContext::Identity {
            mode: PolicyEnforcementMode::Enforcing,
            permission_subject: subject,
            claims: BTreeMap::new(),
            attribution: None,
        },
        input: RowSetProgramInput {
            shape: NormalizedRowSetShape {
                identity: NormalizedShapeIdentity {
                    shape_id: shape(0x77),
                    canonical: vec![0x77],
                },
                root: relation_node.clone(),
                result: ResultId::PathTuple {
                    path: ProgramPathId {
                        owner: step_source.clone(),
                        child: step_source.clone(),
                    },
                    revision: vec![NormalizedValueRef::FrontierColumn {
                        frontier: frontier.clone(),
                        field: "reachable_team".to_owned(),
                    }],
                },
                auxiliary_sources: BTreeSet::new(),
                closure_paths: Vec::new(),
                join_contributions: Vec::new(),
                reachable_contributions: Vec::new(),
                nodes: BTreeMap::from([
                    (
                        seed_node.clone(),
                        RowSetExpr::ValueSource {
                            shape: "reachable-claim".to_owned(),
                            columns: frontier_columns.clone(),
                            mode: ValueSourceMode::Binding,
                        },
                    ),
                    (
                        frontier_node.clone(),
                        RowSetExpr::FrontierSource {
                            frontier: frontier.clone(),
                            columns: frontier_columns,
                        },
                    ),
                    (
                        step_node.clone(),
                        RowSetExpr::Source {
                            source: step_source.clone(),
                            visibility: RowVisibility::Visible,
                        },
                    ),
                    (
                        step_join.clone(),
                        RowSetExpr::Join {
                            left: frontier_node,
                            right: step_node,
                            mode: JoinMode::Inner,
                            on: PredicateExpr::Compare {
                                left: NormalizedValueRef::FrontierColumn {
                                    frontier: frontier.clone(),
                                    field: "reachable_team".to_owned(),
                                },
                                op: ComparisonOp::Eq,
                                right: NormalizedValueRef::SourceField {
                                    source: step_source.clone(),
                                    field: "todo".to_owned(),
                                },
                            },
                        },
                    ),
                    (
                        step_project.clone(),
                        RowSetExpr::Project {
                            input: step_join,
                            columns: vec![
                                RowProjection {
                                    output: TypedOutputField {
                                        name: "team".to_owned(),
                                        ty: ColumnType::Uuid,
                                    },
                                    value: NormalizedValueRef::FrontierColumn {
                                        frontier: frontier.clone(),
                                        field: "team".to_owned(),
                                    },
                                },
                                RowProjection {
                                    output: TypedOutputField {
                                        name: "reachable_team".to_owned(),
                                        ty: ColumnType::Uuid,
                                    },
                                    value: NormalizedValueRef::SourceField {
                                        source: step_source.clone(),
                                        field: "todo".to_owned(),
                                    },
                                },
                            ],
                        },
                    ),
                    (
                        relation_node.clone(),
                        RowSetExpr::RecursiveRelation {
                            seed: seed_node,
                            step: step_project,
                            frontier: frontier.clone(),
                            frontier_key: NormalizedValueRef::FrontierColumn {
                                frontier: frontier.clone(),
                                field: "reachable_team".to_owned(),
                            },
                            dedupe_keys: vec![NormalizedValueRef::FrontierColumn {
                                frontier,
                                field: "reachable_team".to_owned(),
                            }],
                            bound: RecursionBound::MaxDepth(4),
                        },
                    ),
                ]),
            },
            binding: ProgramBinding {
                id: BindingId(uuid::Uuid::from_bytes([0x77; 16])),
                source_shape: None,
                extra_user_params: BTreeMap::new(),
                param_types: BTreeMap::new(),
                claim_params: BTreeMap::from([(
                    claim_param_field(&ClaimPath(vec!["sub".to_owned()])),
                    ProgramClaimParam {
                        path: ClaimPath(vec!["sub".to_owned()]),
                        ty: ColumnType::Uuid,
                    },
                )]),
                values: BTreeMap::new(),
            },
        },
        output: RowSetOutputRequest {
            app_rows: None,
            facts: BTreeSet::from([ProgramFactKey::RelationEdges]),
        },
    };

    let mut old_order_request = request.clone();
    old_order_request.input.binding.claim_params.clear();
    let old_order_program =
        lower_query_program(old_order_request, &mut FakeSourceResolver::default())
            .expect("old-order recursive claim seed should lower");
    let program = lower_query_program(request, &mut FakeSourceResolver::default())
        .expect("recursive claim seed should lower");
    assert_eq!(
        lowered_binding_source_fingerprint(&program),
        lowered_binding_source_fingerprint(&old_order_program),
        "pre-retarget claim discovery must not change emitted binding source names or descriptors"
    );
    let GraphBuilder::Recursive { seed, .. } = &program.lowered.terminals[0].graph else {
        panic!("expected recursive graph");
    };
    assert!(matches!(
        seed.as_ref(),
        GraphBuilder::Project { input, fields }
            if fields.iter().any(|field| field.output_name == "team")
                && fields.iter().any(|field| field.output_name == "reachable_team")
                && matches!(
                    input.as_ref(),
                    GraphBuilder::BindingSource { shape, output }
                        if shape == "reachable-claim"
                            && output.field_index(claim_param_field(&ClaimPath(vec!["sub".to_owned()])).as_str()).is_some()
                )
    ));
    assert!(program.lowered.parameters.user_params.is_empty());
    assert_eq!(
        program
            .lowered
            .parameters
            .claim_params
            .get(claim_param_field(&ClaimPath(vec!["sub".to_owned()])).as_str())
            .map(|param| (&param.path, &param.ty)),
        Some((&ClaimPath(vec!["sub".to_owned()]), &ColumnType::Uuid))
    );
    assert_eq!(
        program.lowered.parameters.routing_params,
        BTreeSet::from([claim_param_field(&ClaimPath(vec!["sub".to_owned()]))])
    );
}

#[test]
fn unbound_filter_param_reports_operator_gap() {
    let request = QueryProgramRequest {
        authorization_mode: QueryAuthorizationMode::TrustedServing,
        reads: QueryReadSet::primary(current_read_view()),
        policy: system_policy_context(),
        input: chained_row_set_input(0x72, BTreeMap::new()),
        output: row_set_output(BTreeSet::new()),
    };

    let err = lower_query_program(request, &mut FakeSourceResolver::default()).unwrap_err();
    assert!(matches!(
        err.gaps.as_slice(),
        [UnsupportedReason::Operator(message)]
            if message.contains("binding parameter 'title' is not bound")
    ));
}

#[test]
fn aggregate_over_window_fails_closed_for_maintained_lowering() {
    let request = QueryProgramRequest {
        authorization_mode: QueryAuthorizationMode::TrustedServing,
        reads: QueryReadSet::primary(current_read_view()),
        policy: system_policy_context(),
        input: aggregate_over_window_row_set_input(0x73),
        output: production_output_request(ProductionOutputProfile::MaintainedView, false),
    };

    let err = lower_query_program(request, &mut FakeSourceResolver::default()).unwrap_err();

    assert!(matches!(
        err.gaps.as_slice(),
        [UnsupportedReason::Operator(message)]
            if message.contains("aggregate over ordered/windowed input is not lowered yet")
    ));
}

#[test]
fn equality_filter_param_lowers_to_prepared_binding_join() {
    let mut input = chained_row_set_input(
        0x79,
        BTreeMap::from([("title".to_owned(), Value::String("mine".to_owned()))]),
    );
    input.binding.source_shape = Some("query-binding".to_owned());
    let request = QueryProgramRequest {
        authorization_mode: QueryAuthorizationMode::TrustedServing,
        reads: QueryReadSet::primary(current_read_view()),
        policy: system_policy_context(),
        input,
        output: row_set_output(BTreeSet::new()),
    };

    let program = lower_query_program(request, &mut FakeSourceResolver::default())
        .expect("equality param should lower");
    assert_eq!(
        program.lowered.parameters.user_params.get("title"),
        Some(&ColumnType::String)
    );
    let graph = format!("{:?}", program.lowered.terminals[0].graph);
    assert!(graph.contains("BindingSource"), "{graph}");
    assert!(graph.contains("query-binding"), "{graph}");
    assert!(graph.contains("title"), "{graph}");
    let ProgramOutputSchemas::RowSet(outputs) = &program.lowered.output;
    let app_rows = outputs
        .iter()
        .find_map(|output| match output {
            OutputTerminalSchema::AppRows(rows) => Some(rows),
            OutputTerminalSchema::Fact(_) => None,
        })
        .expect("app rows schema");
    let route = route_param_field("title");
    assert!(app_rows.descriptor.field_index(&route).is_some());
    assert!(
        app_rows.hidden_fields.contains(&route),
        "prepared binding route must remain internal to the flat collector"
    );
}

// Internal compiler-boundary test: the public query API cannot expose which
// union arm carried a route. Inspecting the lowered graph pins the prepared
// binding join that keeps a claimless arm in the policy subplan's route domain.
#[test]
fn prepared_policy_union_joins_claimless_arm_to_binding_route() {
    let root_source = source("todos", SourceRole::Root);
    let public = RowSetNodeId("public".to_owned());
    let claimed_source = RowSetNodeId("claimed_source".to_owned());
    let claimed = RowSetNodeId("claimed".to_owned());
    let union = RowSetNodeId("policy_union".to_owned());
    let claim_field = claim_param_field(&ClaimPath(vec!["sub".to_owned()]));
    let mut input = row_set_input(0xa8);
    input.shape.root = union.clone();
    input.shape.nodes = BTreeMap::from([
        (
            public.clone(),
            RowSetExpr::Source {
                source: root_source.clone(),
                visibility: RowVisibility::Visible,
            },
        ),
        (
            claimed_source.clone(),
            RowSetExpr::Source {
                source: root_source.clone(),
                visibility: RowVisibility::Visible,
            },
        ),
        (
            claimed.clone(),
            RowSetExpr::Filter {
                input: claimed_source,
                predicate: PredicateExpr::Compare {
                    left: NormalizedValueRef::SourceField {
                        source: root_source.clone(),
                        field: "title".to_owned(),
                    },
                    op: ComparisonOp::Eq,
                    right: NormalizedValueRef::Param(claim_field.clone()),
                },
            },
        ),
        (
            union,
            RowSetExpr::Union {
                inputs: vec![
                    UnionInput {
                        node: public,
                        label: "public".to_owned(),
                    },
                    UnionInput {
                        node: claimed,
                        label: "claim-sub".to_owned(),
                    },
                ],
            },
        ),
    ]);
    input.binding.source_shape = Some("prepared-policy-binding".to_owned());
    input.binding.claim_params = BTreeMap::from([(
        claim_field.clone(),
        ProgramClaimParam {
            path: ClaimPath(vec!["sub".to_owned()]),
            ty: ColumnType::String,
        },
    )]);

    let request = QueryProgramRequest {
        authorization_mode: QueryAuthorizationMode::TrustedServing,
        reads: QueryReadSet::primary(current_read_view()),
        policy: PolicyContext::AuthorizationSubplan {
            protected_source: root_source,
            role: PolicyDecisionRole::Read,
            mode: PolicyEnforcementMode::Enforcing,
            permission_subject: author(0xa8),
            claims: BTreeMap::new(),
            attribution: None,
        },
        input,
        output: RowSetOutputRequest {
            app_rows: None,
            facts: BTreeSet::from([ProgramFactKey::AuthorizedRows]),
        },
    };

    let program = lower_query_program(request, &mut FakeSourceResolver::default())
        .expect("prepared policy union lowers");
    let terminal = program
        .lowered
        .terminals
        .iter()
        .find(|terminal| terminal.sink == "policy.authorized_rows")
        .expect("authorized-rows terminal");
    assert!(matches!(
        &terminal.graph,
        GraphBuilder::Project { fields, .. }
            if fields.iter().map(|field| field.output_name.as_str()).collect::<BTreeSet<_>>()
                == BTreeSet::from(["row_uuid", claim_field.as_str()])
    ));
    assert!(graph_any(&terminal.graph, &|graph| matches!(
        graph,
        GraphBuilder::Project { input, fields }
            if fields.iter().any(|field| field.output_name == claim_field)
                && matches!(
                    input.as_ref(),
                    GraphBuilder::Join {
                        right,
                        left_on,
                        right_on,
                        comparison: groove::ivm::ValueComparison::Policy,
                        ..
                    } if left_on.is_empty()
                        && right_on.is_empty()
                        && matches!(
                            right.as_ref(),
                            GraphBuilder::BindingSource { shape, output }
                                if shape == "prepared-policy-binding"
                                    && output.field_index(claim_field.as_str()).is_some()
                        )
                )
    )));
}

/// Keeps a public assignment occurrence address through an inherited-policy
/// semi-join while Alice's allowed release and tenant-correlated checks are
/// lowered as consecutive joins.
///
/// ```text
/// alice ──insert assignment──► release + membership + organization checks
///                                  │
///                                  └──► public assignment result occurrence
/// ```
///
/// The matching authorization subplan keeps the same join inputs internal:
/// its policy proof must never expose public occurrence carriers.
#[test]
fn authorization_subplan_with_correlated_allowed_to_joins_lowers_without_occurrence_carriers() {
    let root = RowSetNodeId("assignment".to_owned());
    let release_input = RowSetNodeId("release-source".to_owned());
    let release_join = RowSetNodeId("release-allowed-to".to_owned());
    let membership_input = RowSetNodeId("membership-source".to_owned());
    let membership_join = RowSetNodeId("membership-tenant-check".to_owned());
    let organization_input = RowSetNodeId("organization-source".to_owned());
    let policy_root = RowSetNodeId("organization-tenant-check".to_owned());
    let assignment = source("assignments", SourceRole::Root);
    let release = source(
        "releases",
        SourceRole::Alias("allowed-to:release".to_owned()),
    );
    let membership = source(
        "memberships",
        SourceRole::Alias("exists:membership-tenant".to_owned()),
    );
    let organization = source(
        "organizations",
        SourceRole::Alias("exists:organization-tenant".to_owned()),
    );
    let request = QueryProgramRequest {
        authorization_mode: QueryAuthorizationMode::TrustedServing,
        reads: QueryReadSet::primary(ReadView {
            read_schema: schema(0x91),
            policy_schema: schema(0x92),
            sources: BTreeMap::from([
                (
                    assignment.clone(),
                    requested_current_source(DurabilityTier::Global),
                ),
                (
                    release.clone(),
                    requested_current_source(DurabilityTier::Global),
                ),
                (
                    membership.clone(),
                    requested_current_source(DurabilityTier::Global),
                ),
                (
                    organization.clone(),
                    requested_current_source(DurabilityTier::Global),
                ),
            ]),
        }),
        policy: PolicyContext::AuthorizationSubplan {
            protected_source: assignment.clone(),
            role: PolicyDecisionRole::Write,
            mode: PolicyEnforcementMode::Enforcing,
            permission_subject: author(0x91),
            claims: BTreeMap::new(),
            attribution: None,
        },
        input: RowSetProgramInput {
            shape: NormalizedRowSetShape {
                identity: NormalizedShapeIdentity {
                    shape_id: shape(0x91),
                    canonical: vec![0x91],
                },
                root: policy_root.clone(),
                result: ResultId::RealRow {
                    table: "assignments".to_owned(),
                    row: ResultRowRef::Source(assignment.clone()),
                },
                auxiliary_sources: BTreeSet::new(),
                closure_paths: Vec::new(),
                join_contributions: Vec::new(),
                reachable_contributions: Vec::new(),
                nodes: BTreeMap::from([
                    (
                        root.clone(),
                        RowSetExpr::Source {
                            source: assignment.clone(),
                            visibility: RowVisibility::Visible,
                        },
                    ),
                    (
                        release_input,
                        RowSetExpr::Source {
                            source: release.clone(),
                            visibility: RowVisibility::Visible,
                        },
                    ),
                    (
                        release_join.clone(),
                        RowSetExpr::Join {
                            left: root,
                            right: RowSetNodeId("release-source".to_owned()),
                            mode: JoinMode::Inner,
                            on: PredicateExpr::Compare {
                                left: NormalizedValueRef::RowId(RowIdRef::Source(
                                    assignment.clone(),
                                )),
                                op: ComparisonOp::Eq,
                                right: NormalizedValueRef::SourceField {
                                    source: release.clone(),
                                    field: "todo".to_owned(),
                                },
                            },
                        },
                    ),
                    (
                        membership_input,
                        RowSetExpr::Source {
                            source: membership.clone(),
                            visibility: RowVisibility::Visible,
                        },
                    ),
                    (
                        membership_join,
                        RowSetExpr::Join {
                            left: release_join,
                            right: RowSetNodeId("membership-source".to_owned()),
                            mode: JoinMode::Inner,
                            on: PredicateExpr::Compare {
                                left: NormalizedValueRef::RowId(RowIdRef::Source(
                                    assignment.clone(),
                                )),
                                op: ComparisonOp::Eq,
                                right: NormalizedValueRef::SourceField {
                                    source: membership,
                                    field: "todo".to_owned(),
                                },
                            },
                        },
                    ),
                    (
                        organization_input,
                        RowSetExpr::Source {
                            source: organization.clone(),
                            visibility: RowVisibility::Visible,
                        },
                    ),
                    (
                        policy_root,
                        RowSetExpr::Join {
                            left: RowSetNodeId("membership-tenant-check".to_owned()),
                            right: RowSetNodeId("organization-source".to_owned()),
                            mode: JoinMode::Inner,
                            on: PredicateExpr::Compare {
                                left: NormalizedValueRef::RowId(RowIdRef::Source(
                                    assignment.clone(),
                                )),
                                op: ComparisonOp::Eq,
                                right: NormalizedValueRef::SourceField {
                                    source: organization,
                                    field: "todo".to_owned(),
                                },
                            },
                        },
                    ),
                ]),
            },
            binding: ProgramBinding {
                id: BindingId(uuid::Uuid::from_bytes([0x91; 16])),
                source_shape: None,
                extra_user_params: BTreeMap::new(),
                param_types: BTreeMap::new(),
                claim_params: BTreeMap::new(),
                values: BTreeMap::new(),
            },
        },
        output: RowSetOutputRequest {
            app_rows: None,
            facts: BTreeSet::from([ProgramFactKey::ResultMembership]),
        },
    };

    let program = lower_query_program(request, &mut FakeSourceResolver::default())
        .expect("correlated write authorization should lower");
    let graph = format!("{:?}", program.lowered.terminals);
    assert!(
        !graph.contains("__flat_join_source_"),
        "authorization decision graph must not request public occurrence carriers: {graph}"
    );
    assert!(
        graph.contains("__policy_join_source_0_"),
        "the next correlated predicate still needs the first join's internal values: {graph}"
    );
    let OutputTerminalSchema::Fact(ProgramFactOutput {
        schema: ProgramFactSchema::ResultMembership(schema),
        ..
    }) = program
        .lowered
        .terminals
        .iter()
        .find(|terminal| terminal.sink == "maintained.result_current")
        .map(|terminal| &terminal.output)
        .expect("result-membership terminal")
    else {
        panic!("result-membership terminal must retain its schema");
    };
    assert_eq!(schema.occurrence_id_fields, vec!["row_uuid"]);

    // The source-read terminal uses an ordinary identity context, not the
    // authorization-subplan context above. Its trailing inherited-policy
    // semi-join must keep the first two public join carriers.
    let mut public_input = program.request.input.clone();
    let public_root = public_input.shape.root.clone();
    let RowSetExpr::Join { mode, .. } = public_input
        .shape
        .nodes
        .get_mut(&public_root)
        .expect("public assignment root join")
    else {
        panic!("public assignment root must be a join");
    };
    *mode = JoinMode::Semi;
    let public_request = QueryProgramRequest {
        authorization_mode: QueryAuthorizationMode::TrustedServing,
        reads: program.request.reads.clone(),
        policy: PolicyContext::Identity {
            mode: PolicyEnforcementMode::Enforcing,
            permission_subject: author(0x91),
            claims: BTreeMap::new(),
            attribution: None,
        },
        input: public_input,
        output: row_set_output(BTreeSet::new()),
    };
    let public_program = lower_query_program(public_request, &mut FakeSourceResolver::default())
        .expect("public correlated assignment read should lower");
    let public_terminal = public_program
        .lowered
        .terminals
        .iter()
        .find(|terminal| matches!(terminal.output, OutputTerminalSchema::AppRows(_)))
        .expect("public app-rows terminal");
    let public_graph = format!("{:#?}", public_terminal.graph);
    assert!(
        !public_graph.contains("__policy_join_source_"),
        "policy-proof carriers must not reach a public query terminal: {public_graph}"
    );
    let OutputTerminalSchema::AppRows(public_schema) = &public_terminal.output else {
        panic!("public app-rows terminal must retain its public descriptor");
    };
    assert!(
        public_schema
            .descriptor
            .fields()
            .iter()
            .filter_map(|field| field.name.as_deref())
            .all(|field| !field.starts_with("__policy_join_source_")),
        "private policy carriers must not appear in the public descriptor: {public_schema:#?}"
    );
    let collector_inputs = BTreeSet::from([
        "__collect_root___flat_join_source_0_row_uuid".to_owned(),
        "__collect_root___flat_join_source_1_row_uuid".to_owned(),
    ]);
    assert!(
        graph_any(&public_terminal.graph, &|graph| matches!(
            graph,
            GraphBuilder::Project { fields, .. }
                if collector_inputs.is_subset(
                    &fields
                        .iter()
                        .map(|field| field.output_name.clone())
                        .collect()
                )
        )),
        "the collector must receive every projected public occurrence carrier: {:#?}",
        public_terminal.graph
    );
}

#[test]
fn claim_filter_lowers_from_identity_policy_context() {
    let request = QueryProgramRequest {
        authorization_mode: QueryAuthorizationMode::TrustedServing,
        reads: QueryReadSet::primary(current_read_view()),
        policy: PolicyContext::Identity {
            mode: PolicyEnforcementMode::Enforcing,
            permission_subject: author(0xa1),
            claims: BTreeMap::from([("title".to_owned(), Value::String("mine".to_owned()))]),
            attribution: None,
        },
        input: claim_filtered_row_set_input(0x73, "title"),
        output: row_set_output(BTreeSet::new()),
    };

    let program =
        lower_query_program(request, &mut FakeSourceResolver::default()).expect("claim lowers");
    let graph = format!("{:?}", program.lowered.terminals[0].graph);
    assert!(graph.contains("mine"), "{graph}");
}

#[test]
fn identity_policy_context_requests_policy_filtered_sources() {
    let subject = author(0xa6);
    let request = QueryProgramRequest {
        authorization_mode: QueryAuthorizationMode::TrustedServing,
        reads: QueryReadSet::primary(current_read_view()),
        policy: PolicyContext::Identity {
            mode: PolicyEnforcementMode::Enforcing,
            permission_subject: subject,
            claims: BTreeMap::new(),
            attribution: None,
        },
        input: row_set_input(0x76),
        output: row_set_output(BTreeSet::new()),
    };

    let mut resolver = FakeSourceResolver::default();
    lower_query_program(request, &mut resolver).expect("identity policy source lowers");

    assert_eq!(resolver.requests.len(), 1);
    assert_eq!(
        resolver.requests[0].authorization,
        SourceAuthorizationRequest::PolicyFiltered {
            permission_subject: subject,
            plan: PolicyAuthorizationPlan {
                protected_source: source("todos", SourceRole::Root),
                role: PolicyDecisionRole::Read,
                protected_row_field: "row_uuid".to_owned(),
                binding_source_shape: None,
                binding_user_params: BTreeMap::new(),
                binding_claim_params: BTreeMap::new(),
            },
        }
    );
}

// Internal compiler-boundary test: this is the only place a client-local read
// can opt out, and the option is host configuration rather than query input.
#[test]
fn client_local_mode_elides_policy_filtering_even_for_identity_context() {
    let request = QueryProgramRequest {
        authorization_mode: QueryAuthorizationMode::ClientLocal,
        reads: QueryReadSet::primary(current_read_view()),
        policy: PolicyContext::Identity {
            mode: PolicyEnforcementMode::Enforcing,
            permission_subject: author(0xa6),
            claims: BTreeMap::new(),
            attribution: None,
        },
        input: row_set_input(0x77),
        output: row_set_output(BTreeSet::new()),
    };

    let mut resolver = FakeSourceResolver::default();
    lower_query_program(request, &mut resolver).expect("client-local source lowers");
    assert_eq!(resolver.requests.len(), 1);
    assert_eq!(
        resolver.requests[0].authorization,
        SourceAuthorizationRequest::System
    );
}

// Internal compiler-boundary test: public query validation already enforces
// parameter types, but this pins the lowering invariant that descriptor types
// come from that validated shape, not from the current binding value.
#[test]
fn binding_descriptor_types_do_not_depend_on_runtime_array_values() {
    fn request_for(teams: Value) -> QueryProgramRequest {
        let mut input = row_set_input(0xa7);
        input.binding.source_shape = Some("test-binding-source".to_owned());
        input.binding.param_types = BTreeMap::from([(
            "teams".to_owned(),
            ColumnType::Array(Box::new(ColumnType::Uuid)),
        )]);
        input.binding.values.insert("teams".to_owned(), teams);
        QueryProgramRequest {
            authorization_mode: QueryAuthorizationMode::TrustedServing,
            reads: QueryReadSet::primary(current_read_view()),
            policy: PolicyContext::Identity {
                mode: PolicyEnforcementMode::Enforcing,
                permission_subject: author(0xa7),
                claims: BTreeMap::new(),
                attribution: None,
            },
            input,
            output: row_set_output(BTreeSet::new()),
        }
    }

    let mut empty_resolver = FakeSourceResolver::default();
    let empty_program =
        lower_query_program(request_for(Value::Array(Vec::new())), &mut empty_resolver)
            .expect("empty array binding lowers");

    let mut non_empty_resolver = FakeSourceResolver::default();
    let non_empty_program = lower_query_program(
        request_for(Value::Array(vec![Value::Uuid(row(0xa7).0)])),
        &mut non_empty_resolver,
    )
    .expect("non-empty array binding lowers");

    assert_eq!(
        empty_program.lowered.parameters,
        non_empty_program.lowered.parameters
    );
    assert_eq!(
        empty_resolver.requests[0].authorization,
        non_empty_resolver.requests[0].authorization
    );
    assert_eq!(
        empty_resolver.requests[0].authorization,
        SourceAuthorizationRequest::PolicyFiltered {
            permission_subject: author(0xa7),
            plan: PolicyAuthorizationPlan {
                protected_source: source("todos", SourceRole::Root),
                role: PolicyDecisionRole::Read,
                protected_row_field: "row_uuid".to_owned(),
                binding_source_shape: Some("test-binding-source".to_owned()),
                binding_user_params: BTreeMap::from([(
                    "teams".to_owned(),
                    ColumnType::Array(Box::new(ColumnType::Uuid)),
                )]),
                binding_claim_params: BTreeMap::new(),
            },
        }
    );
}

#[test]
fn nested_binding_value_source_keeps_sibling_nullable_claim_route() {
    let user_id = ClaimPath(vec!["user".to_owned()]);
    let join_code = ClaimPath(vec!["join_code".to_owned()]);
    let typed_user_id = claim_param_field(&user_id);
    let typed_join_code = claim_param_field(&join_code);
    let mut input = row_set_input(0xc5);
    input.binding.source_shape = Some("test-binding-source".to_owned());
    input.binding.claim_params = BTreeMap::from([
        (
            typed_user_id.clone(),
            ProgramClaimParam {
                path: user_id.clone(),
                ty: ColumnType::String,
            },
        ),
        (
            typed_join_code.clone(),
            ProgramClaimParam {
                path: join_code,
                ty: ColumnType::String.nullable(),
            },
        ),
    ]);
    let request = QueryProgramRequest {
        authorization_mode: QueryAuthorizationMode::TrustedServing,
        reads: QueryReadSet::primary(current_read_view()),
        policy: policy_context(),
        input,
        output: row_set_output(BTreeSet::new()),
    };
    let fields = binding_value_source_projection_fields_for_test(
        &request,
        &[ValueSourceColumn {
            name: "userId".to_owned(),
            value: NormalizedValueRef::Claim(user_id),
            ty: ColumnType::String,
        }],
    )
    .expect("nested binding source lowers");
    assert!(
        fields.contains(&typed_join_code),
        "a user-id proof source must retain its sibling nullable join-code route"
    );
}

#[test]
fn missing_sub_claim_lowers_to_deny_predicate() {
    let subject = author(0xa5);
    let request = QueryProgramRequest {
        authorization_mode: QueryAuthorizationMode::TrustedServing,
        reads: QueryReadSet::primary(current_read_view()),
        policy: PolicyContext::Identity {
            mode: PolicyEnforcementMode::Enforcing,
            permission_subject: subject,
            claims: BTreeMap::new(),
            attribution: None,
        },
        input: claim_filtered_row_set_input(0x74, "sub"),
        output: row_set_output(BTreeSet::new()),
    };

    let program = lower_query_program(request, &mut FakeSourceResolver::default())
        .expect("missing sub claim lowers");
    let graph = format!("{:?}", program.lowered.terminals[0].graph);
    assert!(graph.contains("Filter"), "{graph}");
    assert!(graph.contains("Or([])"), "{graph}");
}

/// This is deliberately a lowering-level receipt: the parameter domain is an
/// internal compiler boundary, while the policy integration receipt exercises
/// the corresponding public behavior.
#[test]
fn only_ordered_scalar_claims_enter_collector_routing() {
    let scalar = ClaimPath(vec!["tier".to_owned()]);
    let array = ClaimPath(vec!["teams".to_owned()]);
    let mut input = row_set_input(0xd3);
    let root = input.shape.root.clone();
    input.shape.nodes.insert(
        root,
        RowSetExpr::ValueSource {
            shape: "claim-routing".to_owned(),
            columns: vec![
                ValueSourceColumn {
                    name: "tier".to_owned(),
                    value: NormalizedValueRef::Claim(scalar.clone()),
                    ty: ColumnType::String.nullable(),
                },
                ValueSourceColumn {
                    name: "teams".to_owned(),
                    value: NormalizedValueRef::Claim(array.clone()),
                    ty: ColumnType::Array(Box::new(ColumnType::Uuid)),
                },
            ],
            mode: ValueSourceMode::Binding,
        },
    );

    let domain = super::super::lowering::parameter_domain_for_shape_for_test(&input.shape);
    let scalar_field = claim_param_field(&scalar);
    let array_field = claim_param_field(&array);
    assert!(domain.claim_params.contains_key(&scalar_field));
    assert!(domain.claim_params.contains_key(&array_field));
    assert!(domain.routing_params.contains(&scalar_field));
    assert!(
        !domain.routing_params.contains(&array_field),
        "compound claim stays a prepared predicate input and never becomes a collector key"
    );
}

#[test]
fn missing_claim_lowers_to_deny_predicate() {
    let request = QueryProgramRequest {
        authorization_mode: QueryAuthorizationMode::TrustedServing,
        reads: QueryReadSet::primary(current_read_view()),
        policy: policy_context(),
        input: claim_filtered_row_set_input(0x75, "team"),
        output: row_set_output(BTreeSet::new()),
    };

    let program = lower_query_program(request, &mut FakeSourceResolver::default())
        .expect("missing claims lower to a deny predicate");
    let graph = format!("{:?}", program.lowered.terminals[0].graph);
    assert!(graph.contains("Filter"), "{graph}");
    assert!(graph.contains("Or([])"), "{graph}");
}
