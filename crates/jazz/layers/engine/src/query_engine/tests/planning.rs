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
fn logical_recursive_arg_by_is_rejected_before_source_expansion() {
    let seed = RowSetNodeId("seed".to_owned());
    let frontier_node = RowSetNodeId("frontier".to_owned());
    let source_node = RowSetNodeId("source".to_owned());
    let joined = RowSetNodeId("joined".to_owned());
    let projected = RowSetNodeId("projected".to_owned());
    let relation = RowSetNodeId("relation".to_owned());
    let frontier = FrontierId("reachable".to_owned());
    let step_source = source("todos", SourceRole::RecursiveStep("step".to_owned()));
    let key = NormalizedValueRef::FrontierColumn {
        frontier: frontier.clone(),
        field: "reachable".to_owned(),
    };
    let columns = vec![ValueSourceColumn {
        name: "reachable".to_owned(),
        value: NormalizedValueRef::Literal(
            postcard::to_allocvec(&Value::Uuid(row(0x76).0)).unwrap(),
        ),
        ty: ColumnType::Uuid,
    }];
    let mut input = row_set_input(0x76);
    input.shape.root = relation.clone();
    input.shape.result = ResultId::PathTuple {
        path: ProgramPathId {
            owner: step_source.clone(),
            child: step_source.clone(),
        },
        revision: vec![key.clone()],
    };
    input.shape.nodes = BTreeMap::from([
        (
            seed.clone(),
            RowSetExpr::ValueSource {
                shape: "literal-seed".to_owned(),
                columns: columns.clone(),
                mode: ValueSourceMode::Inline,
            },
        ),
        (
            frontier_node.clone(),
            RowSetExpr::FrontierSource {
                frontier: frontier.clone(),
                columns,
            },
        ),
        (
            source_node.clone(),
            RowSetExpr::Source {
                source: step_source.clone(),
                visibility: RowVisibility::Visible,
            },
        ),
        (
            joined.clone(),
            RowSetExpr::Join {
                left: frontier_node,
                right: source_node,
                mode: JoinMode::Inner,
                on: PredicateExpr::Compare {
                    left: key.clone(),
                    op: ComparisonOp::Eq,
                    right: NormalizedValueRef::SourceField {
                        source: step_source.clone(),
                        field: "todo".to_owned(),
                    },
                },
            },
        ),
        (
            projected.clone(),
            RowSetExpr::Project {
                input: joined,
                columns: vec![RowProjection {
                    output: TypedOutputField {
                        name: "reachable".to_owned(),
                        ty: ColumnType::Uuid,
                    },
                    value: NormalizedValueRef::SourceField {
                        source: step_source,
                        field: "todo".to_owned(),
                    },
                }],
            },
        ),
        (
            relation.clone(),
            RowSetExpr::RecursiveRelation {
                seed,
                step: projected.clone(),
                frontier,
                frontier_key: key.clone(),
                dedupe_keys: vec![key],
                bound: RecursionBound::MaxDepth(4),
            },
        ),
    ]);
    let mut request = QueryProgramRequest {
        authorization_mode: QueryAuthorizationMode::TrustedServing,
        reads: QueryReadSet::primary(recursive_current_read_view()),
        policy: system_policy_context(),
        input,
        output: RowSetOutputRequest {
            app_rows: None,
            facts: BTreeSet::from([ProgramFactKey::RelationEdges]),
        },
    };
    let mut resolver = FakeSourceResolver {
        current_rows_use_arg_by: true,
        ..FakeSourceResolver::default()
    };
    lower_query_program(request.clone(), &mut resolver)
        .expect("physical current-row ArgBy remains valid inside recursion");

    let limited = RowSetNodeId("limited".to_owned());
    request.input.shape.nodes.insert(
        limited.clone(),
        RowSetExpr::Slice {
            input: projected,
            partition_by: Vec::new(),
            limit: Some(1),
            offset: 0,
            tie_breaker: Vec::new(),
            rank_output: None,
        },
    );
    let Some(RowSetExpr::RecursiveRelation { step, .. }) =
        request.input.shape.nodes.get_mut(&relation)
    else {
        unreachable!();
    };
    *step = limited;
    query_program_source_requests(&request)
        .expect_err("logical recursive ArgBy must be rejected before sources are prepared");
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
