//! Normalized program planning, semantic occurrence identity, recursion, and sharing.

use super::*;

#[test]
fn union_occurrence_labels_survive_reorder_and_unrelated_arm_insertion() {
    fn analyzed_labels(inputs: Vec<(&str, &str)>) -> Vec<String> {
        let nodes = inputs
            .iter()
            .map(|(node, label)| {
                (
                    RowSetNodeId((*node).to_owned()),
                    RowSetExpr::Source {
                        source: source(label, SourceRole::Policy((*label).to_owned())),
                        visibility: RowVisibility::Visible,
                    },
                )
            })
            .collect::<BTreeMap<_, _>>();
        let union_inputs = inputs
            .iter()
            .map(|(node, label)| UnionInput {
                node: RowSetNodeId((*node).to_owned()),
                label: (*label).to_owned(),
            })
            .collect::<Vec<_>>();
        analyzed_union_labels(&union_inputs, &nodes).expect("unique semantic labels lower")
    }

    let original = analyzed_labels(vec![("node-a", "direct"), ("node-b", "inherited")]);
    let reordered_with_insert = analyzed_labels(vec![
        ("replacement-node-b", "inherited"),
        ("new-node", "delegated"),
        ("replacement-node-a", "direct"),
    ]);

    assert_eq!(original, ["direct", "inherited"]);
    assert!(reordered_with_insert.contains(&"direct".to_owned()));
    assert!(reordered_with_insert.contains(&"inherited".to_owned()));
    assert_eq!(
        original.into_iter().collect::<BTreeSet<_>>(),
        reordered_with_insert
            .into_iter()
            .filter(|label| label != "delegated")
            .collect()
    );
}

#[test]
fn union_occurrence_rejects_duplicate_semantic_labels() {
    let first = RowSetNodeId("first".to_owned());
    let second = RowSetNodeId("second".to_owned());
    let nodes = BTreeMap::from([
        (
            first.clone(),
            RowSetExpr::Source {
                source: source("first", SourceRole::Policy("first".to_owned())),
                visibility: RowVisibility::Visible,
            },
        ),
        (
            second.clone(),
            RowSetExpr::Source {
                source: source("second", SourceRole::Policy("second".to_owned())),
                visibility: RowVisibility::Visible,
            },
        ),
    ]);
    let error = analyzed_union_labels(
        &[
            UnionInput {
                node: first,
                label: "same".to_owned(),
            },
            UnionInput {
                node: second,
                label: "same".to_owned(),
            },
        ],
        &nodes,
    )
    .expect_err("duplicate semantic arm identity must fail closed");
    assert!(format!("{error:?}").contains("duplicated"));
}

#[test]
fn union_occurrence_rejects_nul_delimited_label_collision() {
    let node = RowSetNodeId("source".to_owned());
    let nodes = BTreeMap::from([(
        node.clone(),
        RowSetExpr::Source {
            source: source("source", SourceRole::Policy("source".to_owned())),
            visibility: RowVisibility::Visible,
        },
    )]);
    let error = analyzed_union_labels(
        &[UnionInput {
            node,
            label: "outer\0inner".to_owned(),
        }],
        &nodes,
    )
    .expect_err("nested path delimiter must not occur inside a semantic label");
    assert!(format!("{error:?}").contains("NUL-free"));
}

#[test]
fn recursive_relation_has_explicit_recursive_plan_and_relation_facts() {
    let seed_node = RowSetNodeId("seed".to_owned());
    let frontier_node = RowSetNodeId("frontier".to_owned());
    let step_node = RowSetNodeId("step".to_owned());
    let step_join = RowSetNodeId("step-join".to_owned());
    let step_project = RowSetNodeId("step-project".to_owned());
    let relation_node = RowSetNodeId("relation".to_owned());
    let frontier = FrontierId("reachable".to_owned());
    let step_source = source("todos", SourceRole::RecursiveStep("step".to_owned()));
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
        ValueSourceColumn {
            name: "route".to_owned(),
            value: NormalizedValueRef::Param("route".to_owned()),
            ty: ColumnType::String,
        },
    ];
    let request = QueryProgramRequest {
        authorization_mode: QueryAuthorizationMode::TrustedServing,
        reads: QueryReadSet::primary(recursive_current_read_view()),
        policy: PolicyContext::Identity {
            mode: PolicyEnforcementMode::Enforcing,
            permission_subject: author(0x76),
            claims: BTreeMap::new(),
            attribution: None,
        },
        input: RowSetProgramInput {
            shape: NormalizedRowSetShape {
                identity: NormalizedShapeIdentity {
                    shape_id: shape(0x76),
                    canonical: vec![0x76],
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
                            shape: "reachable-binding".to_owned(),
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
                                RowProjection {
                                    output: TypedOutputField {
                                        name: "route".to_owned(),
                                        ty: ColumnType::String,
                                    },
                                    value: NormalizedValueRef::FrontierColumn {
                                        frontier: frontier.clone(),
                                        field: "route".to_owned(),
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
                                frontier: frontier.clone(),
                                field: "reachable_team".to_owned(),
                            }],
                            bound: RecursionBound::MaxDepth(4),
                        },
                    ),
                ]),
            },
            binding: ProgramBinding {
                id: BindingId(uuid::Uuid::from_bytes([0x76; 16])),
                source_shape: None,
                extra_user_params: BTreeMap::new(),
                param_types: BTreeMap::from([("route".to_owned(), ColumnType::String)]),
                claim_params: BTreeMap::from([(
                    claim_param_field(&ClaimPath(vec!["sub".to_owned()])),
                    ProgramClaimParam {
                        path: ClaimPath(vec!["sub".to_owned()]),
                        ty: ColumnType::Uuid,
                    },
                )]),
                values: BTreeMap::from([("route".to_owned(), Value::String("sync".to_owned()))]),
            },
        },
        output: RowSetOutputRequest {
            app_rows: None,
            facts: BTreeSet::from([
                ProgramFactKey::RelationEdges,
                ProgramFactKey::ResultMembership,
                ProgramFactKey::PathCorrelationCoverage,
            ]),
        },
    };

    let mut logical_arg_by_request = request.clone();
    let original_step = match logical_arg_by_request.input.shape.nodes.get(&relation_node) {
        Some(RowSetExpr::RecursiveRelation { step, .. }) => step.clone(),
        _ => panic!("expected recursive relation fixture"),
    };
    let step_arg_by = RowSetNodeId("step-arg-by".to_owned());
    logical_arg_by_request.input.shape.nodes.insert(
        step_arg_by.clone(),
        RowSetExpr::Slice {
            input: original_step,
            partition_by: Vec::new(),
            limit: Some(1),
            offset: 0,
            tie_breaker: Vec::new(),
            rank_output: None,
        },
    );
    let Some(RowSetExpr::RecursiveRelation { step, .. }) = logical_arg_by_request
        .input
        .shape
        .nodes
        .get_mut(&relation_node)
    else {
        panic!("expected recursive relation fixture");
    };
    *step = step_arg_by;
    let mut rejecting_resolver = FakeSourceResolver {
        current_rows_use_arg_by: true,
        ..FakeSourceResolver::default()
    };
    let err = lower_query_program(logical_arg_by_request, &mut rejecting_resolver)
        .expect_err("user-authored ArgBy recursion must fail during logical analysis");
    assert!(
        err.gaps.iter().any(|gap| format!("{gap:?}").contains(
            "arg_max_by and arg_min_by are not supported inside recursive seed or step graphs"
        )),
        "{err:?}"
    );
    assert!(
        rejecting_resolver.requests.is_empty(),
        "logical recursion validation must run before current-row source expansion"
    );

    let mut resolver = FakeSourceResolver {
        current_rows_use_arg_by: true,
        ..FakeSourceResolver::default()
    };
    let program =
        lower_query_program(request, &mut resolver).expect("recursive relation should lower");

    fn step_input_reads_frontier(input: &GraphBuilder) -> bool {
        match input {
            GraphBuilder::Join { left, .. } => matches!(
                left.as_ref(),
                GraphBuilder::FrontierSource { binding, output }
                    if binding.0 == "reachable"
                        && output.field_index("team").is_some()
                        && output.field_index("reachable_team").is_some()
                        && output.field_index("route").is_some()
            ),
            GraphBuilder::UnwrapNullable { input, .. } => step_input_reads_frontier(input),
            _ => false,
        }
    }

    assert!(matches!(
        program
            .lowered
            .terminals
            .iter()
            .find(|terminal| terminal.sink == "maintained.relation_edges")
            .expect("relation edge terminal")
            .graph
            .clone(),
        GraphBuilder::Recursive {
            ref seed,
            ref step,
            ref frontier,
            max_iters: 4,
            ..
        } if frontier.0 == "reachable"
            && matches!(
                seed.as_ref(),
                GraphBuilder::Project { input, fields }
                    if fields.iter().any(|field| field.output_name == "team")
                    && fields.iter().any(|field| field.output_name == "reachable_team")
                    && fields.iter().any(|field| field.output_name == "route")
                    && matches!(
                        input.as_ref(),
                        GraphBuilder::BindingSource { shape, output }
                            if shape == "reachable-binding"
                                && output.field_index("route").is_some()
                                && output.field_index("reachable_team").is_none()
                    )
            )
            && matches!(
                step.as_ref(),
                GraphBuilder::Project { input, .. }
                    if step_input_reads_frontier(input)
            )
    ));
    let recursive_terminal = program
        .lowered
        .terminals
        .iter()
        .find(|terminal| terminal.sink == "maintained.relation_edges")
        .expect("relation edge terminal");
    let GraphBuilder::Recursive { step, .. } = &recursive_terminal.graph else {
        panic!("expected recursive graph");
    };
    assert!(
        graph_any(step, &|graph| matches!(
            graph,
            GraphBuilder::ArgMaxBy { .. }
        )),
        "current-row ArgBy introduced by source expansion must remain inside the recursive step"
    );
    assert_eq!(
        program.lowered.parameters.user_params,
        BTreeMap::from([("route".to_owned(), ColumnType::String)])
    );
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
        BTreeSet::from([
            claim_param_field(&ClaimPath(vec!["sub".to_owned()])),
            route_param_field("route")
        ])
    );
    let ProgramOutputSchemas::RowSet(terminals) = &program.lowered.output;
    assert!(terminals.iter().any(|terminal| {
        matches!(
            terminal,
            OutputTerminalSchema::Fact(ProgramFactOutput {
                key: ProgramFactKey::RelationEdges,
                terminal: ProgramFactTerminal::Primary,
                schema: ProgramFactSchema::RelationEdges(RelationEdgeSchema {
                    depth_field: Some(_),
                    ..
                }),
            })
        )
    }));
    assert!(terminals.iter().any(|terminal| {
        matches!(
            terminal,
            OutputTerminalSchema::Fact(ProgramFactOutput {
                key: ProgramFactKey::ResultMembership,
                terminal: ProgramFactTerminal::Primary,
                schema: ProgramFactSchema::ResultMembership(ResultMembershipSchema {
                    routing_param_fields,
                    ..
                }),
            }) if routing_param_fields.contains(&claim_param_field(&ClaimPath(vec!["sub".to_owned()])))
                && routing_param_fields.contains(&route_param_field("route"))
        )
    }));
    let result_membership_terminal = program
        .lowered
        .terminals
        .iter()
        .find(|terminal| terminal.sink == "maintained.result_current")
        .expect("result-membership terminal");
    let result_membership_fields = graph_declared_output_fields(&result_membership_terminal.graph)
        .expect("result-membership terminal should declare output fields");
    assert!(
        result_membership_fields.contains(&claim_param_field(&ClaimPath(vec!["sub".to_owned()]))),
        "result-membership terminal must retain claim route field"
    );
    assert!(
        result_membership_fields.contains(&route_param_field("route")),
        "result-membership terminal must retain user route field"
    );
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
fn read_view_models_propagation_and_schema_lens_without_settled_result_source() {
    let root = source("todos", SourceRole::Root);
    let policy = source("todo_acl", SourceRole::Policy("read".to_owned()));
    let projection = SchemaProjection {
        schema_family: SchemaFamilySelection::ExplicitSchemaFamily(schema_family(0x33)),
        storage: StorageSchemaSelection::CompatiblePartitions,
        lens: LensSelection::Canonical,
    };
    let expr = SourceExpr::SnapshotRef {
        projection,
        data: DataSource::Branch(BranchKey::default()),
        snapshot: snapshot(),
    };
    let view = ReadView {
        read_schema: schema(0x30),
        policy_schema: schema(0x31),
        sources: BTreeMap::from([(root.clone(), expr.clone()), (policy.clone(), expr)]),
    };

    assert_eq!(view.source_current_tier(&root), None);
    assert_eq!(view.source_current_tier(&policy), None);
    assert_eq!(view.read_schema(), schema(0x30));
}

#[test]
fn sharing_key_excludes_binding_and_output_requirements() {
    let resolved_overlays = OverlayStack {
        entries: vec![
            ResolvedOverlay {
                overlay: OverlayRef::DirectBatch(BatchId(vec![0x01])),
                manifest_fingerprint: vec![0xa1],
            },
            ResolvedOverlay {
                overlay: OverlayRef::AcceptedTransaction(TxId {
                    time: TxTime::new(2_000, 0),
                    node: NodeUuid::from_bytes([0x44; 16]),
                }),
                manifest_fingerprint: vec![0xa2],
            },
            ResolvedOverlay {
                overlay: OverlayRef::OpenTransaction(OpenTransactionId([7; 16])),
                manifest_fingerprint: vec![0xa3],
            },
        ],
    };
    let base = ProgramSharingKey {
        shape_id: shape(0x44),
        reads: QueryReadSet::primary(ResolvedReadKey {
            read_schema: schema(0x40),
            policy_schema: schema(0x40),
            sources: BTreeMap::from([(
                source("todos", SourceRole::Root),
                ResolvedSourceExpr::WithOverlays {
                    input: Box::new(ResolvedSourceExpr::VisibleCurrent {
                        projection: resolved_projection(0x40),
                        data: DataSource::Current,
                        tier: DurabilityTier::Local,
                    }),
                    overlays: resolved_overlays.clone(),
                },
            )]),
        }),
        policy: PolicySharingKey::System,
    };
    let instance = ProgramInstanceKey {
        program: base.clone(),
        binding_id: BindingId(uuid::Uuid::from_bytes([0x44; 16])),
    };
    let output_a = ProgramOutputKey {
        fingerprint: vec![0x01],
    };
    let output_b = ProgramOutputKey {
        fingerprint: vec![0x02],
    };
    let output_c = output_b.clone();

    assert_eq!(base, base.clone());
    assert_eq!(instance.program, base);
    assert_ne!(output_a, output_b);
    assert_eq!(output_b, output_c);
    let current = base.reads.primary.sources.values().next().unwrap();
    assert_eq!(current.current_tier(), Some(DurabilityTier::Local));
    assert!(matches!(
        current,
        ResolvedSourceExpr::WithOverlays { overlays, .. } if overlays == &resolved_overlays
    ));
}
