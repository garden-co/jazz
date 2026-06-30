use super::*;
use crate::ids::{NodeUuid, SchemaVersionId};
use crate::schema::ColumnSchema;
use crate::time::{GlobalSeq, TxTime};
use crate::tx::Snapshot;
use groove::records::ValueType;

fn schema(byte: u8) -> SchemaVersionId {
    SchemaVersionId::from_bytes([byte; 16])
}

fn row(byte: u8) -> RowUuid {
    RowUuid::from_bytes([byte; 16])
}

fn author(byte: u8) -> AuthorId {
    AuthorId::from_bytes([byte; 16])
}

fn shape(byte: u8) -> ShapeId {
    ShapeId(uuid::Uuid::from_bytes([byte; 16]))
}

fn branch(byte: u8) -> BranchId {
    BranchId::from_bytes([byte; 16])
}

fn source(table: &str, role: SourceRole) -> SourceId {
    SourceId {
        table: table.to_owned(),
        path: SourcePath {
            components: vec![role],
        },
    }
}

fn requested_projection() -> SchemaProjection<RequestedSourceStage> {
    SchemaProjection {
        schema_family: SchemaFamilySelection::Current,
        storage: StorageSchemaSelection::Single(schema(0x10)),
        lens: LensSelection::Canonical,
    }
}

fn resolved_projection(byte: u8) -> SchemaProjection<ResolvedSourceStage> {
    SchemaProjection {
        schema_family: branch(byte),
        storage: vec![ResolvedPartitionLens {
            storage_schema: schema(byte),
            lens_path_fingerprint: vec![],
        }],
        lens: (),
    }
}

fn requested_current_source(tier: DurabilityTier) -> RequestedSourceExpr {
    SourceExpr::VisibleCurrent {
        projection: requested_projection(),
        data: DataSource::Current,
        tier,
    }
}

fn normalized_shape(byte: u8) -> NormalizedRowSetShape {
    let root = RowSetNodeId("root".to_owned());
    let root_source = source("todos", SourceRole::Root);
    NormalizedRowSetShape {
        identity: NormalizedShapeIdentity {
            shape_id: shape(byte),
            canonical: vec![byte],
        },
        root: root.clone(),
        result: ResultId::RealRow {
            table: "todos".to_owned(),
            row: ResultRowRef::Source(root_source.clone()),
        },
        nodes: BTreeMap::from([(
            root,
            RowSetExpr::Source {
                source: root_source,
                visibility: RowVisibility::Visible,
            },
        )]),
    }
}

fn row_set_input(byte: u8) -> RowSetProgramInput {
    RowSetProgramInput {
        shape: normalized_shape(byte),
        binding: ProgramBinding {
            id: BindingId(uuid::Uuid::from_bytes([byte; 16])),
            values: BTreeMap::new(),
        },
    }
}

fn chained_row_set_input(byte: u8, binding_values: BTreeMap<String, Value>) -> RowSetProgramInput {
    let root = RowSetNodeId("root".to_owned());
    let filter = RowSetNodeId("filter".to_owned());
    let order = RowSetNodeId("order".to_owned());
    let slice = RowSetNodeId("slice".to_owned());
    let root_source = source("todos", SourceRole::Root);
    RowSetProgramInput {
        shape: NormalizedRowSetShape {
            identity: NormalizedShapeIdentity {
                shape_id: shape(byte),
                canonical: vec![byte],
            },
            root: slice.clone(),
            result: ResultId::RealRow {
                table: "todos".to_owned(),
                row: ResultRowRef::Source(root_source.clone()),
            },
            nodes: BTreeMap::from([
                (
                    root.clone(),
                    RowSetExpr::Source {
                        source: root_source.clone(),
                        visibility: RowVisibility::Visible,
                    },
                ),
                (
                    filter.clone(),
                    RowSetExpr::Filter {
                        input: root,
                        predicate: PredicateExpr::Compare {
                            left: NormalizedValueRef::SourceField {
                                source: root_source.clone(),
                                field: "title".to_owned(),
                            },
                            op: ComparisonOp::Eq,
                            right: NormalizedValueRef::Param("title".to_owned()),
                        },
                    },
                ),
                (
                    order.clone(),
                    RowSetExpr::OrderBy {
                        input: filter,
                        keys: vec![OrderKey {
                            value: NormalizedValueRef::SourceField {
                                source: root_source.clone(),
                                field: "title".to_owned(),
                            },
                            direction: SortDirection::Asc,
                        }],
                    },
                ),
                (
                    slice.clone(),
                    RowSetExpr::Slice {
                        input: order,
                        partition_by: Vec::new(),
                        limit: Some(2),
                        offset: 1,
                        tie_breaker: vec![NormalizedValueRef::RowId(RowIdRef::Source(root_source))],
                        rank_output: None,
                    },
                ),
            ]),
        },
        binding: ProgramBinding {
            id: BindingId(uuid::Uuid::from_bytes([byte; 16])),
            values: binding_values,
        },
    }
}

fn current_read_view() -> RequestedReadView {
    current_read_view_at(DurabilityTier::Global)
}

fn current_read_view_at(tier: DurabilityTier) -> RequestedReadView {
    let root = source("todos", SourceRole::Root);
    ReadView {
        read_schema: schema(0x10),
        policy_schema: schema(0x11),
        sources: BTreeMap::from([(root, requested_current_source(tier))]),
    }
}

fn joined_current_read_view() -> RequestedReadView {
    let root = source("todos", SourceRole::Root);
    let join = source("todo_tags", SourceRole::Alias("join_via:0".to_owned()));
    ReadView {
        read_schema: schema(0x10),
        policy_schema: schema(0x11),
        sources: BTreeMap::from([
            (root, requested_current_source(DurabilityTier::Global)),
            (join, requested_current_source(DurabilityTier::Global)),
        ]),
    }
}

fn snapshot() -> Snapshot {
    Snapshot {
        owner: NodeUuid::from_bytes([0x33; 16]),
        global_base: GlobalSeq(17),
        local_base: TxTime::new(1_000, 0),
        dots: vec![TxId {
            time: TxTime::new(1_001, 0),
            node: NodeUuid::from_bytes([0x33; 16]),
        }],
    }
}

fn policy_context() -> PolicyContext {
    PolicyContext::Identity {
        mode: PolicyEnforcementMode::Enforcing,
        permission_subject: author(0xa1),
        claims: BTreeMap::new(),
        attribution: None,
    }
}

fn system_policy_context() -> PolicyContext {
    PolicyContext::System
}

fn program_scope() -> CoverageScope {
    CoverageScope::Program
}

fn program_frontier_requirement() -> FrontierRequirement {
    FrontierRequirement::Through(ResolvedFrontier {
        tier: DurabilityTier::Global,
        stream: Some("peer-1".to_owned()),
        through: FrontierPosition::GlobalSeq(GlobalSeq(42)),
    })
}

fn program_frontier() -> CoverageFrontier {
    CoverageFrontier {
        scope: program_scope(),
        frontier: program_frontier_requirement(),
    }
}

fn row_set_output(facts: BTreeSet<ProgramFactKey>) -> RowSetOutputRequest {
    RowSetOutputRequest {
        app_rows: Some(AppRowOutputRequest {
            projection: PayloadProjection::ShapeDefault,
            large_values: Vec::new(),
        }),
        facts,
    }
}

fn sync_facts() -> BTreeSet<ProgramFactKey> {
    BTreeSet::from([
        ProgramFactKey::ResultMembership,
        ProgramFactKey::SourceCoverage(program_scope()),
        ProgramFactKey::VersionWitnesses,
    ])
}

#[derive(Default)]
struct FakeSourceResolver {
    requests: Vec<SourceRequest>,
}

impl SourceResolver for FakeSourceResolver {
    fn resolve_source(
        &mut self,
        request: &SourceRequest,
    ) -> Result<ResolvedSource, SourceResolutionError> {
        self.requests.push(request.clone());
        Ok(ResolvedSource {
            table_schema: TableSchema::new(
                request.source.table.clone(),
                Vec::<ColumnSchema>::new(),
            ),
            graph: GraphBuilder::table(format!("resolved_{}", request.source.table)),
            row_shape: SourceRowShape {
                source: request.source.clone(),
                descriptor: RecordDescriptor::new([
                    ("table", ValueType::String),
                    ("row_uuid", ValueType::Uuid),
                    ("user_title", ValueType::String),
                    ("user_todo", ValueType::Nullable(Box::new(ValueType::Uuid))),
                    ("user_tag", ValueType::Nullable(Box::new(ValueType::String))),
                    ("tx_time", ValueType::U64),
                    ("tx_node_id", ValueType::U64),
                    ("schema_version", ValueType::Uuid),
                    ("coverage", ValueType::String),
                    ("layer", ValueType::String),
                ]),
                row_uuid_field: "row_uuid".to_owned(),
                metadata: BTreeMap::from([
                    (
                        SourceMetadataRequirement::VersionWitnesses,
                        SourceMetadataFields::VersionWitnesses {
                            schema_version_field: "schema_version".to_owned(),
                            tx_time_field: "tx_time".to_owned(),
                            tx_node_field: "tx_node_id".to_owned(),
                            branch_or_prefix_field: None,
                        },
                    ),
                    (
                        SourceMetadataRequirement::Coverage,
                        SourceMetadataFields::Coverage {
                            coverage_field: "coverage".to_owned(),
                        },
                    ),
                ]),
            },
        })
    }
}

/// Internal lowering tests are kept here because the required behavior is
/// the query-engine contract itself: public black-box APIs cannot yet prove
/// that every data path routes through this compiler boundary.
#[test]
fn compiler_boundary_has_no_usage_or_lifecycle_mode() {
    let request = QueryProgramRequest {
        reads: QueryReadSet::primary(current_read_view()),
        policy: policy_context(),
        input: row_set_input(0x21),
        output: row_set_output(BTreeSet::from([ProgramFactKey::PolicyWitnesses])),
    };

    let err = lower_query_program(request, &mut FakeSourceResolver::default()).unwrap_err();
    assert!(matches!(
        err.gaps.as_slice(),
        [UnsupportedReason::Policy(message)] if message.contains("policy augmentation")
    ));
    assert!(
        err.explain
            .capabilities
            .iter()
            .any(|line| line.contains("table-rooted current"))
    );
}

#[test]
fn simple_current_table_root_query_lowers_for_local_edge_and_global_sync_outputs() {
    for tier in [
        DurabilityTier::Local,
        DurabilityTier::Edge,
        DurabilityTier::Global,
    ] {
        let request = QueryProgramRequest {
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
        assert!(matches!(
            program.lowered.graph,
            GraphBuilder::Table { ref table } if table == "resolved_todos"
        ));
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
        assert_eq!(terminals.len(), 4);
        assert!(terminals.iter().any(|terminal| {
            matches!(
                terminal,
                OutputTerminalSchema::AppRows(AppRowSchema {
                    descriptor,
                    hidden_fields,
                    ..
                }) if descriptor.field_index("user_title").is_some()
                    && hidden_fields.contains("tx_time")
                    && hidden_fields.contains("tx_node_id")
                    && hidden_fields.contains("coverage")
            )
        }));
        assert!(terminals.iter().any(|terminal| {
            matches!(
                terminal,
                OutputTerminalSchema::Fact(ProgramFactOutput {
                    key: ProgramFactKey::ResultMembership,
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
                    schema: ProgramFactSchema::SourceCoverage(_),
                })
            )
        }));
        assert!(terminals.iter().any(|terminal| {
            matches!(
                terminal,
                OutputTerminalSchema::Fact(ProgramFactOutput {
                    key: ProgramFactKey::VersionWitnesses,
                    schema: ProgramFactSchema::VersionWitnesses(VersionWitnessSchemas {
                        content: Some(_),
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
        program.lowered.graph,
        GraphBuilder::TopBy {
            ref input,
            ref group_cols,
            ref order_cols,
            ref tie_cols,
            offset: 1,
            limit: 2,
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
                } if matches!(
                    input.as_ref(),
                    GraphBuilder::Table { table } if table == "resolved_todos"
                ) && field == "user_title"
                    && value == &groove::ivm::LiteralValue::String("ship".to_owned())
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
fn current_source_select_projection_and_unordered_slice_lower() {
    let root = RowSetNodeId("root".to_owned());
    let slice = RowSetNodeId("slice".to_owned());
    let root_source = source("todos", SourceRole::Root);
    let request = QueryProgramRequest {
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
                values: BTreeMap::new(),
            },
        },
        output: RowSetOutputRequest {
            app_rows: Some(AppRowOutputRequest {
                projection: PayloadProjection::Tree(AppProjectionTree {
                    fields: FieldProjection::Fields(BTreeSet::from(["title".to_owned()])),
                    paths: Vec::new(),
                }),
                large_values: Vec::new(),
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
    assert!(matches!(
        program.lowered.graph,
        GraphBuilder::TopBy {
            ref input,
            ref group_cols,
            ref order_cols,
            ref tie_cols,
            offset: 2,
            limit: 3,
        } if matches!(input.as_ref(), GraphBuilder::Table { table } if table == "resolved_todos")
            && group_cols.is_empty()
            && order_cols.is_empty()
            && matches!(tie_cols.as_slice(), [groove::ivm::FieldRef::Name(field)]
                if field == "row_uuid")
    ));
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
    assert!(matches!(
        program.lowered.graph,
        GraphBuilder::Project { ref input, ref fields }
            if fields.iter().any(|field| field.output_name == "row_uuid")
                && matches!(
                    input.as_ref(),
                    GraphBuilder::Join {
                        left,
                        right,
                        left_on,
                        right_on,
                    } if matches!(left.as_ref(), GraphBuilder::Table { table } if table == "resolved_todos")
                        && matches!(
                            right.as_ref(),
                            GraphBuilder::UnwrapNullable { input, field }
                                if matches!(field, groove::ivm::FieldRef::Name(name) if name == "user_todo")
                                    && matches!(
                                        input.as_ref(),
                                        GraphBuilder::Filter { input, predicate }
                                            if matches!(
                                                input.as_ref(),
                                                GraphBuilder::Table { table } if table == "resolved_todo_tags"
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
    ));
}

#[test]
fn unbound_filter_param_reports_operator_gap() {
    let request = QueryProgramRequest {
        reads: QueryReadSet::primary(current_read_view()),
        policy: system_policy_context(),
        input: chained_row_set_input(0x72, BTreeMap::new()),
        output: RowSetOutputRequest {
            app_rows: None,
            facts: BTreeSet::new(),
        },
    };

    let err = lower_query_program(request, &mut FakeSourceResolver::default()).unwrap_err();
    assert!(matches!(
        err.gaps.as_slice(),
        [UnsupportedReason::Operator(message)]
            if message.contains("binding parameter 'title' is not bound")
    ));
}

#[test]
fn read_view_models_propagation_and_schema_lens_without_settled_result_source() {
    let root = source("todos", SourceRole::Root);
    let policy = source("todo_acl", SourceRole::Policy("read".to_owned()));
    let projection = SchemaProjection {
        schema_family: SchemaFamilySelection::SchemaFamilyBranch(branch(0x33)),
        storage: StorageSchemaSelection::CompatiblePartitions,
        lens: LensSelection::Canonical,
    };
    let expr = SourceExpr::SnapshotRef {
        projection,
        data: DataSource::Branch(branch(0x44)),
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
                overlay: OverlayRef::OpenTransaction(OpenTxId(7)),
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

#[test]
fn read_frontier_facts_are_outputs_not_delivery_profiles() {
    let key = ProgramSharingKey {
        shape_id: shape(0x55),
        reads: QueryReadSet::primary(ResolvedReadKey {
            read_schema: schema(0x55),
            policy_schema: schema(0x55),
            sources: BTreeMap::from([(
                source("todos", SourceRole::Root),
                ResolvedSourceExpr::VisibleCurrent {
                    projection: resolved_projection(0x55),
                    data: DataSource::Current,
                    tier: DurabilityTier::Global,
                },
            )]),
        }),
        policy: PolicySharingKey::System,
    };
    let local_output = row_set_output(BTreeSet::from([ProgramFactKey::ResultMembership]));
    let covered_output = row_set_output(BTreeSet::from([
        ProgramFactKey::ResultMembership,
        ProgramFactKey::ReadFrontierSettled(program_frontier()),
    ]));
    let local_output_key = ProgramOutputKey {
        fingerprint: vec![0x01],
    };
    let covered_output_key = ProgramOutputKey {
        fingerprint: vec![0x02],
    };

    assert_eq!(key, key.clone());
    assert_ne!(local_output, covered_output);
    assert_ne!(local_output_key, covered_output_key);
}

#[test]
fn app_rows_are_separate_from_hidden_terminal_facts() {
    let request = row_set_output(BTreeSet::from([
        ProgramFactKey::ResultMembership,
        ProgramFactKey::PathEdges,
        ProgramFactKey::SourceCoverage(program_scope()),
    ]));

    let app_rows = request.app_rows.as_ref().expect("app rows requested");
    assert!(matches!(
        app_rows.projection,
        PayloadProjection::ShapeDefault
    ));
    assert!(request.facts.contains(&ProgramFactKey::PathEdges));
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

#[test]
fn large_value_extent_schema_names_authorized_materialization_contract() {
    let member = VersionedRowRefSchema {
        row: RowRefSchema {
            source_field: "source".to_owned(),
            table_field: "table".to_owned(),
            row_field: "row_uuid".to_owned(),
        },
        version: Some(ResultMembershipVersionSchema::Content(
            ContentVersionFields {
                tx_time_field: "tx_time".to_owned(),
                tx_node_field: "tx_node".to_owned(),
            },
        )),
    };
    let schema = LargeValueExtentSchema {
        owner: member,
        column_field: "column".to_owned(),
        range_field: "range".to_owned(),
        digest_field: "digest".to_owned(),
        materialization_field: "materialization".to_owned(),
        handle_field: "handle".to_owned(),
        tier_field: "tier".to_owned(),
        source_coverage_field: "source_coverage".to_owned(),
        completeness_field: "complete".to_owned(),
    };

    assert_eq!(schema.digest_field, "digest");
    assert_eq!(schema.completeness_field, "complete");
}
