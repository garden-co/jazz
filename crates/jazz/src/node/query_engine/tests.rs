use super::*;
use crate::ids::{NodeUuid, SchemaVersionId};
use crate::schema::ColumnSchema;
use crate::time::{GlobalSeq, TxTime};
use crate::tx::Snapshot;
use groove::records::ValueType;
use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};

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

fn lowered_binding_source_fingerprint(program: &QueryProgram) -> BTreeSet<(String, u64)> {
    let mut sources = BTreeSet::new();
    for terminal in &program.lowered.terminals {
        collect_binding_source_fingerprint(&terminal.graph, &mut sources);
    }
    sources
}

fn collect_binding_source_fingerprint(graph: &GraphBuilder, sources: &mut BTreeSet<(String, u64)>) {
    match graph {
        GraphBuilder::BindingSource { shape, output } => {
            let mut hasher = DefaultHasher::new();
            format!("{output:?}").hash(&mut hasher);
            sources.insert((shape.clone(), hasher.finish()));
        }
        GraphBuilder::Recursive { seed, step, .. } => {
            collect_binding_source_fingerprint(seed, sources);
            collect_binding_source_fingerprint(step, sources);
        }
        GraphBuilder::Filter { input, .. }
        | GraphBuilder::UnwrapNullable { input, .. }
        | GraphBuilder::Unnest { input, .. }
        | GraphBuilder::Project { input, .. }
        | GraphBuilder::ArgMaxBy { input, .. }
        | GraphBuilder::ArgMinBy { input, .. }
        | GraphBuilder::TopBy { input, .. }
        | GraphBuilder::Aggregate { input, .. } => {
            collect_binding_source_fingerprint(input, sources);
        }
        GraphBuilder::Union { inputs } => {
            for input in inputs {
                collect_binding_source_fingerprint(input, sources);
            }
        }
        GraphBuilder::Join { left, right, .. }
        | GraphBuilder::SemiJoin { left, right, .. }
        | GraphBuilder::AntiJoin { left, right, .. } => {
            collect_binding_source_fingerprint(left, sources);
            collect_binding_source_fingerprint(right, sources);
        }
        GraphBuilder::Table { .. }
        | GraphBuilder::InlineRecords { .. }
        | GraphBuilder::Index { .. }
        | GraphBuilder::FrontierSource { .. } => {}
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
        auxiliary_sources: BTreeSet::new(),
        closure_paths: Vec::new(),
        join_contributions: Vec::new(),
        reachable_contributions: Vec::new(),
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
            source_shape: None,
            extra_user_params: BTreeMap::new(),
            param_types: BTreeMap::new(),
            claim_params: BTreeMap::new(),
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
            source_shape: None,
            extra_user_params: BTreeMap::new(),
            param_types: BTreeMap::from([("title".to_owned(), ColumnType::String)]),
            claim_params: BTreeMap::new(),
            values: binding_values,
        },
    }
}

fn aggregate_over_window_row_set_input(byte: u8) -> RowSetProgramInput {
    let root = RowSetNodeId("root".to_owned());
    let order = RowSetNodeId("order".to_owned());
    let slice = RowSetNodeId("slice".to_owned());
    let aggregate = RowSetNodeId("aggregate".to_owned());
    let root_source = source("todos", SourceRole::Root);
    RowSetProgramInput {
        shape: NormalizedRowSetShape {
            identity: NormalizedShapeIdentity {
                shape_id: shape(byte),
                canonical: vec![byte],
            },
            root: aggregate.clone(),
            result: ResultId::SyntheticTuple {
                identity: SyntheticIdentitySpec {
                    table: "todos_aggregate".to_owned(),
                    key_columns: Vec::new(),
                    revision_columns: vec!["count".to_owned()],
                },
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
                    order.clone(),
                    RowSetExpr::OrderBy {
                        input: root,
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
                        offset: 0,
                        tie_breaker: vec![NormalizedValueRef::RowId(RowIdRef::Source(
                            root_source.clone(),
                        ))],
                        rank_output: None,
                    },
                ),
                (
                    aggregate.clone(),
                    RowSetExpr::Aggregate {
                        input: slice,
                        group_by: Vec::new(),
                        outputs: vec![AggregateExpr {
                            output: TypedOutputField {
                                name: "count".to_owned(),
                                ty: ColumnType::U64,
                            },
                            function: AggregateFunction::Count,
                            input: None,
                        }],
                    },
                ),
            ]),
        },
        binding: ProgramBinding {
            id: BindingId(uuid::Uuid::from_bytes([byte; 16])),
            source_shape: None,
            extra_user_params: BTreeMap::new(),
            param_types: BTreeMap::new(),
            claim_params: BTreeMap::new(),
            values: BTreeMap::new(),
        },
    }
}

fn claim_filtered_row_set_input(byte: u8, claim: &str) -> RowSetProgramInput {
    let root = RowSetNodeId("root".to_owned());
    let filter = RowSetNodeId("filter".to_owned());
    let root_source = source("todos", SourceRole::Root);
    RowSetProgramInput {
        shape: NormalizedRowSetShape {
            identity: NormalizedShapeIdentity {
                shape_id: shape(byte),
                canonical: vec![byte],
            },
            root: filter.clone(),
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
                    filter.clone(),
                    RowSetExpr::Filter {
                        input: root,
                        predicate: PredicateExpr::Compare {
                            left: NormalizedValueRef::SourceField {
                                source: root_source,
                                field: "title".to_owned(),
                            },
                            op: ComparisonOp::Eq,
                            right: NormalizedValueRef::Claim(ClaimPath(vec![claim.to_owned()])),
                        },
                    },
                ),
            ]),
        },
        binding: ProgramBinding {
            id: BindingId(uuid::Uuid::from_bytes([byte; 16])),
            source_shape: None,
            extra_user_params: BTreeMap::new(),
            param_types: BTreeMap::new(),
            claim_params: BTreeMap::new(),
            values: BTreeMap::new(),
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

fn path_current_read_view() -> RequestedReadView {
    let root = source("todos", SourceRole::Root);
    let child = source("todo_tags", SourceRole::CorrelatedChild("tags".to_owned()));
    ReadView {
        read_schema: schema(0x10),
        policy_schema: schema(0x11),
        sources: BTreeMap::from([
            (root, requested_current_source(DurabilityTier::Global)),
            (child, requested_current_source(DurabilityTier::Global)),
        ]),
    }
}

fn recursive_current_read_view() -> RequestedReadView {
    let seed = source("todos", SourceRole::RecursiveSeed("seed".to_owned()));
    let step = source("todos", SourceRole::RecursiveStep("step".to_owned()));
    ReadView {
        read_schema: schema(0x10),
        policy_schema: schema(0x11),
        sources: BTreeMap::from([
            (seed, requested_current_source(DurabilityTier::Global)),
            (step, requested_current_source(DurabilityTier::Global)),
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

#[derive(Clone, Copy, Debug)]
enum ProductionOutputProfile {
    AppRows,
    AuthorizedRows,
    RelationSnapshot,
    MaintainedView,
}

fn production_output_request(
    profile: ProductionOutputProfile,
    has_relation_paths: bool,
) -> RowSetOutputRequest {
    match profile {
        ProductionOutputProfile::AppRows => row_set_output(BTreeSet::new()),
        ProductionOutputProfile::AuthorizedRows => RowSetOutputRequest {
            app_rows: None,
            facts: BTreeSet::from([ProgramFactKey::AuthorizedRows]),
        },
        ProductionOutputProfile::RelationSnapshot => RowSetOutputRequest {
            app_rows: Some(AppRowOutputRequest {
                projection: PayloadProjection::ShapeDefault,
                large_values: Vec::new(),
            }),
            facts: if has_relation_paths {
                BTreeSet::from([
                    ProgramFactKey::RelationEdges,
                    ProgramFactKey::PathCorrelationCoverage,
                ])
            } else {
                BTreeSet::new()
            },
        },
        ProductionOutputProfile::MaintainedView => RowSetOutputRequest {
            app_rows: None,
            facts: BTreeSet::from([
                ProgramFactKey::ResultMembership,
                ProgramFactKey::VersionWitnesses,
                ProgramFactKey::ReplacementWitnesses,
            ]),
        },
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
        let deletion_register = request
            .requirements
            .metadata
            .contains(&SourceMetadataRequirement::DeletionMarkers)
            .then(|| DeletionRegisterSource {
                graph: GraphBuilder::table(format!("resolved_{}_deletions", request.source.table)),
                row_uuid_field: "row_uuid".to_owned(),
            });
        let content_version = request
            .requirements
            .metadata
            .contains(&SourceMetadataRequirement::VersionPayloads)
            .then(|| ContentVersionSource {
                graph: GraphBuilder::table(format!(
                    "resolved_{}_content_versions",
                    request.source.table
                )),
                row_uuid_field: "row_uuid".to_owned(),
            });
        let mut metadata = BTreeMap::from([
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
        ]);
        if deletion_register.is_some() {
            metadata.insert(
                SourceMetadataRequirement::DeletionMarkers,
                SourceMetadataFields::DeletionMarkers {
                    deletion_state_field: "_deletion".to_owned(),
                    deletion_tx_time_field: Some("tx_time".to_owned()),
                    deletion_tx_node_field: Some("tx_node_id".to_owned()),
                },
            );
        }
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
                metadata,
            },
            routing_fields: BTreeSet::new(),
            content_version,
            deletion_register,
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
        [UnsupportedReason::Output(fact)] if matches!(fact.as_ref(), ProgramFactKey::PolicyWitnesses)
    ));
    assert!(
        err.explain
            .capabilities
            .iter()
            .any(|line| line.contains("requested fact is not lowered yet"))
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
            program.lowered.terminals.first().expect("lowered terminal").graph.clone(),
            GraphBuilder::Table { ref table, .. } if table == "resolved_todos"
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
        assert_eq!(terminals.len(), 5);
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
        program.lowered.terminals.first().expect("lowered terminal").graph.clone(),
        GraphBuilder::TopBy {
            ref input,
            ref group_cols,
            ref order_cols,
            ref tie_cols,
            offset: 2,
            limit: groove::ivm::TopByLimit::Finite(3),
        } if matches!(input.as_ref(), GraphBuilder::Table { table, .. } if table == "resolved_todos")
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
    assert!(matches!(
        program.lowered.terminals.first().expect("lowered terminal").graph.clone(),
        GraphBuilder::Project { ref input, ref fields }
            if fields.iter().any(|field| field.output_name == "row_uuid")
                && matches!(
                    input.as_ref(),
                    GraphBuilder::Join {
                        left,
                        right,
                        left_on,
                        right_on,
                    } if matches!(left.as_ref(), GraphBuilder::Table { table, .. } if table == "resolved_todos")
                        && matches!(
                            right.as_ref(),
                            GraphBuilder::UnwrapNullable { input, field }
                                if matches!(field, groove::ivm::FieldRef::Name(name) if name == "user_todo")
                                    && matches!(
                                        input.as_ref(),
                                        GraphBuilder::Filter { input, predicate }
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
    ));
}

#[test]
fn current_join_via_can_use_union_relation_input() {
    let root = RowSetNodeId("root".to_owned());
    let direct_source_node = RowSetNodeId("direct-source".to_owned());
    let direct_project = RowSetNodeId("direct-project".to_owned());
    let inherited_source_node = RowSetNodeId("inherited-source".to_owned());
    let inherited_project = RowSetNodeId("inherited-project".to_owned());
    let union_node = RowSetNodeId("authorized-union".to_owned());
    let join_node = RowSetNodeId("join".to_owned());
    let root_source = source("todos", SourceRole::Root);
    let direct_source = source("todo_tags", SourceRole::Policy("direct".to_owned()));
    let inherited_source = source("todo_tags", SourceRole::Policy("inherited".to_owned()));
    let request = QueryProgramRequest {
        reads: QueryReadSet::primary(ReadView {
            read_schema: schema(0x10),
            policy_schema: schema(0x11),
            sources: BTreeMap::from([
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
            ]),
        }),
        policy: system_policy_context(),
        input: RowSetProgramInput {
            shape: NormalizedRowSetShape {
                identity: NormalizedShapeIdentity {
                    shape_id: shape(0x7a),
                    canonical: vec![0x7a],
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
                                    source: direct_source,
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
                                    source: inherited_source,
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
                        join_node,
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
                ]),
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
        output: row_set_output(BTreeSet::new()),
    };

    let program = lower_query_program(request, &mut FakeSourceResolver::default())
        .expect("union relation input should lower");
    assert!(matches!(
        program.lowered.terminals.first().expect("lowered terminal").graph.clone(),
        GraphBuilder::Project { input, .. }
            if matches!(
                input.as_ref(),
                GraphBuilder::Join { right, right_on, .. }
                    if matches!(right.as_ref(), GraphBuilder::Union { inputs } if inputs.len() == 2)
                        && matches!(right_on.as_slice(), [groove::ivm::FieldRef::Name(name)] if name == "todo")
            )
    ));
}

#[test]
fn current_join_via_lowers_source_column_row_id_target_and_correlations() {
    let root = RowSetNodeId("root".to_owned());
    let join_source_node = RowSetNodeId("join-source".to_owned());
    let join_node = RowSetNodeId("join".to_owned());
    let root_source = source("todos", SourceRole::Root);
    let join_source = source("todo_tags", SourceRole::Alias("join_via:0".to_owned()));
    let request = QueryProgramRequest {
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

    assert!(program.lowered.terminals.iter().any(|terminal| matches!(
        terminal.graph,
        GraphBuilder::Project { ref input, .. }
            if matches!(
                input.as_ref(),
                GraphBuilder::Join { left, right, left_on, right_on }
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

    assert!(program.lowered.terminals.iter().any(|terminal| matches!(
        terminal.graph,
        GraphBuilder::Project { ref input, ref fields }
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

fn correlated_path_request(
    requirement: CorrelationRequirement,
    output: RowSetOutputRequest,
) -> QueryProgramRequest {
    let parent_node = RowSetNodeId("parent".to_owned());
    let child_node = RowSetNodeId("child".to_owned());
    let path_node = RowSetNodeId("path".to_owned());
    let parent_source = source("todos", SourceRole::Root);
    let child_source = source("todo_tags", SourceRole::CorrelatedChild("tags".to_owned()));
    let path = ProgramPathId {
        owner: parent_source.clone(),
        child: child_source.clone(),
    };
    QueryProgramRequest {
        reads: QueryReadSet::primary(path_current_read_view()),
        policy: system_policy_context(),
        input: RowSetProgramInput {
            shape: NormalizedRowSetShape {
                identity: NormalizedShapeIdentity {
                    shape_id: shape(0x78),
                    canonical: vec![0x78],
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
                        path_node,
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
                                    source: child_source,
                                    field: "todo".to_owned(),
                                },
                            },
                            requirement,
                        },
                    ),
                ]),
            },
            binding: ProgramBinding {
                id: BindingId(uuid::Uuid::from_bytes([0x78; 16])),
                source_shape: None,
                extra_user_params: BTreeMap::new(),
                param_types: BTreeMap::new(),
                claim_params: BTreeMap::new(),
                values: BTreeMap::new(),
            },
        },
        output,
    }
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

    assert!(matches!(
        program.lowered.terminals.first().expect("lowered terminal").graph.clone(),
        GraphBuilder::Table { ref table, .. } if table == "resolved_todos"
    ));
    let ProgramOutputSchemas::RowSet(terminals) = &program.lowered.output;
    assert!(
        terminals
            .iter()
            .any(|terminal| matches!(terminal, OutputTerminalSchema::AppRows(_)))
    );
    assert_eq!(terminals.len(), 1);
}

#[test]
fn correlated_path_required_app_rows_with_root_facts_filter_and_dedup_parent_rows() {
    // Internal lowering test: the graph uses the child correlation as an
    // existence filter, then collapses matching children back to one parent row.
    let request = correlated_path_request(
        CorrelationRequirement::AtLeastOne,
        row_set_output(BTreeSet::from([ProgramFactKey::ResultMembership])),
    );

    let mut resolver = FakeSourceResolver::default();
    let program =
        lower_query_program(request, &mut resolver).expect("required path app rows should lower");

    assert!(matches!(
        program.lowered.terminals.first().expect("lowered terminal").graph.clone(),
        GraphBuilder::ArgMinBy {
            ref input,
            ref group_cols,
            ref order_cols,
        } if matches!(group_cols.as_slice(), [groove::ivm::FieldRef::Name(name)] if name == "row_uuid")
            && matches!(order_cols.as_slice(), [groove::ivm::FieldRef::Name(name)] if name == "row_uuid")
            && matches!(
                input.as_ref(),
                GraphBuilder::Project { input, fields }
                    if fields.iter().any(|field| field.output_name == "row_uuid")
                        && matches!(
                            input.as_ref(),
                            GraphBuilder::Join { left_on, right_on, .. }
                                if matches!(left_on.as_slice(), [groove::ivm::FieldRef::Name(name)] if name == "row_uuid")
                                    && matches!(right_on.as_slice(), [groove::ivm::FieldRef::Name(name)] if name == "user_todo")
                        )
            )
    ));
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
    // cardinality correlations as "at least one readable child".
    let request = correlated_path_request(
        CorrelationRequirement::MatchCorrelationCardinality,
        row_set_output(BTreeSet::new()),
    );

    let mut resolver = FakeSourceResolver::default();
    let program = lower_query_program(request, &mut resolver).expect("cardinality lowers");

    assert!(matches!(
        program.lowered.terminals[0].graph,
        GraphBuilder::ArgMinBy { .. }
    ));
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
    assert!(matches!(
        app_rows.graph,
        GraphBuilder::Table { ref table, .. } if table == "resolved_todos"
    ));
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
                    && matches!(right_on.as_slice(), [groove::ivm::FieldRef::Name(name)] if name == "user_todo")
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
        let result = lower_query_program(correlated_request, &mut FakeSourceResolver::default());
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

    let mut resolver = FakeSourceResolver::default();
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
}

#[test]
fn claim_filter_lowers_from_identity_policy_context() {
    let request = QueryProgramRequest {
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
            },
        }
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
            },
        }
    );
}

#[test]
fn built_in_sub_claim_lowers_to_permission_subject() {
    let subject = author(0xa5);
    let request = QueryProgramRequest {
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
        .expect("built-in sub claim lowers");
    let graph = format!("{:?}", program.lowered.terminals[0].graph);
    assert!(graph.contains(&subject.0.to_string()), "{graph}");
}

#[test]
fn missing_claim_lowers_to_deny_predicate() {
    let request = QueryProgramRequest {
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
        ProgramFactKey::RelationEdges,
        ProgramFactKey::SourceCoverage(program_scope()),
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
