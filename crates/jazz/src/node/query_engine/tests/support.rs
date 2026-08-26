//! Shared compiler requests, resolvers, projections, and graph assertions.

use super::*;

pub(super) fn schema(byte: u8) -> SchemaVersionId {
    SchemaVersionId::from_bytes([byte; 16])
}

pub(super) fn row(byte: u8) -> RowUuid {
    RowUuid::from_bytes([byte; 16])
}

pub(super) fn author(byte: u8) -> AuthorSubject {
    AuthorSubject::for_test_bytes([byte; 16])
}

pub(super) fn shape(byte: u8) -> ShapeId {
    ShapeId(uuid::Uuid::from_bytes([byte; 16]))
}

pub(super) fn schema_family(byte: u8) -> SchemaFamilyId {
    SchemaFamilyId::from_bytes([byte; 16])
}

pub(super) fn source(table: &str, role: SourceRole) -> SourceId {
    SourceId {
        table: table.to_owned(),
        path: SourcePath {
            components: vec![role],
        },
    }
}

pub(super) fn lowered_binding_source_fingerprint(
    program: &QueryProgram,
) -> BTreeSet<(String, u64)> {
    let mut sources = BTreeSet::new();
    for terminal in &program.lowered.terminals {
        collect_binding_source_fingerprint(&terminal.graph, &mut sources);
    }
    sources
}

pub(super) fn collect_binding_source_fingerprint(
    graph: &GraphBuilder,
    sources: &mut BTreeSet<(String, u64)>,
) {
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
        | GraphBuilder::VariantProject { input, .. }
        | GraphBuilder::Unnest { input, .. }
        | GraphBuilder::Project { input, .. }
        | GraphBuilder::ArgMaxBy { input, .. }
        | GraphBuilder::ArgMinBy { input, .. }
        | GraphBuilder::TopBy { input, .. }
        | GraphBuilder::CollectBy { input, .. }
        | GraphBuilder::Aggregate { input, .. }
        | GraphBuilder::StreamingChecksum { input, .. } => {
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

/// Structural lowering tests still inspect the relational carrier below the
/// public boundary. The boundary itself is now deliberately a Groove Root
/// collector, so these tests assert that invariant and then search its
/// descendants for the operator they are specifically intended to exercise.
pub(super) fn assert_public_root_terminal(graph: &GraphBuilder) {
    assert!(matches!(
        graph,
        GraphBuilder::CollectBy { collect, .. }
            if collect.mode == groove::ivm::CollectByMode::Root
    ));
}

pub(super) fn graph_any(graph: &GraphBuilder, predicate: &impl Fn(&GraphBuilder) -> bool) -> bool {
    if predicate(graph) {
        return true;
    }
    match graph {
        GraphBuilder::Recursive { seed, step, .. } => {
            graph_any(seed, predicate) || graph_any(step, predicate)
        }
        GraphBuilder::Filter { input, .. }
        | GraphBuilder::UnwrapNullable { input, .. }
        | GraphBuilder::VariantProject { input, .. }
        | GraphBuilder::Unnest { input, .. }
        | GraphBuilder::Project { input, .. }
        | GraphBuilder::ArgMaxBy { input, .. }
        | GraphBuilder::ArgMinBy { input, .. }
        | GraphBuilder::TopBy { input, .. }
        | GraphBuilder::CollectBy { input, .. }
        | GraphBuilder::Aggregate { input, .. }
        | GraphBuilder::StreamingChecksum { input, .. } => graph_any(input, predicate),
        GraphBuilder::Union { inputs } => inputs.iter().any(|input| graph_any(input, predicate)),
        GraphBuilder::Join { left, right, .. }
        | GraphBuilder::SemiJoin { left, right, .. }
        | GraphBuilder::AntiJoin { left, right, .. } => {
            graph_any(left, predicate) || graph_any(right, predicate)
        }
        GraphBuilder::Table { .. }
        | GraphBuilder::InlineRecords { .. }
        | GraphBuilder::BindingSource { .. }
        | GraphBuilder::Index { .. }
        | GraphBuilder::FrontierSource { .. } => false,
    }
}

pub(super) fn requested_projection() -> SchemaProjection<RequestedSourceStage> {
    SchemaProjection {
        schema_family: SchemaFamilySelection::Current,
        storage: StorageSchemaSelection::Single(schema(0x10)),
        lens: LensSelection::Canonical,
    }
}

pub(super) fn resolved_projection(byte: u8) -> SchemaProjection<ResolvedSourceStage> {
    SchemaProjection {
        schema_family: schema_family(byte),
        storage: vec![ResolvedPartitionLens {
            storage_schema: schema(byte),
            lens_path_fingerprint: vec![],
        }],
        lens: (),
    }
}

pub(super) fn requested_current_source(tier: DurabilityTier) -> RequestedSourceExpr {
    SourceExpr::VisibleCurrent {
        projection: requested_projection(),
        data: DataSource::Current,
        tier,
    }
}

pub(super) fn normalized_shape(byte: u8) -> NormalizedRowSetShape {
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

pub(super) fn row_set_input(byte: u8) -> RowSetProgramInput {
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

pub(super) fn chained_row_set_input(
    byte: u8,
    binding_values: BTreeMap<String, Value>,
) -> RowSetProgramInput {
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

pub(super) fn aggregate_over_window_row_set_input(byte: u8) -> RowSetProgramInput {
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

pub(super) fn claim_filtered_row_set_input(byte: u8, claim: &str) -> RowSetProgramInput {
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

pub(super) fn current_read_view() -> RequestedReadView {
    current_read_view_at(DurabilityTier::Global)
}

pub(super) fn current_read_view_at(tier: DurabilityTier) -> RequestedReadView {
    let root = source("todos", SourceRole::Root);
    ReadView {
        read_schema: schema(0x10),
        policy_schema: schema(0x11),
        sources: BTreeMap::from([(root, requested_current_source(tier))]),
    }
}

pub(super) fn joined_current_read_view() -> RequestedReadView {
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

pub(super) fn path_current_read_view() -> RequestedReadView {
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

pub(super) fn recursive_current_read_view() -> RequestedReadView {
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

pub(super) fn snapshot() -> Snapshot {
    Snapshot {
        owner: NodeUuid::from_bytes([0x33; 16]),
        global_base: GlobalTime(17),
        local_base: TxTime::new(1_000, 0),
        dots: vec![TxId {
            time: TxTime::new(1_001, 0),
            node: NodeUuid::from_bytes([0x33; 16]),
        }],
    }
}

pub(super) fn policy_context() -> PolicyContext {
    PolicyContext::Identity {
        mode: PolicyEnforcementMode::Enforcing,
        permission_subject: author(0xa1),
        claims: BTreeMap::new(),
        attribution: None,
    }
}

pub(super) fn system_policy_context() -> PolicyContext {
    PolicyContext::System
}

pub(super) fn program_scope() -> CoverageScope {
    CoverageScope::Program
}

pub(super) fn program_frontier_requirement() -> FrontierRequirement {
    FrontierRequirement::Through(ResolvedFrontier {
        tier: DurabilityTier::Global,
        stream: Some("peer-1".to_owned()),
        through: FrontierPosition::GlobalTime(GlobalTime(42)),
    })
}

pub(super) fn program_frontier() -> CoverageFrontier {
    CoverageFrontier {
        scope: program_scope(),
        frontier: program_frontier_requirement(),
    }
}

pub(super) fn row_set_output(facts: BTreeSet<ProgramFactKey>) -> RowSetOutputRequest {
    RowSetOutputRequest {
        app_rows: Some(AppRowOutputRequest {
            public_terminal: true,
            projection: PayloadProjection::ShapeDefault,
        }),
        facts,
    }
}

#[derive(Clone, Copy, Debug)]
pub(super) enum ProductionOutputProfile {
    AppRows,
    AuthorizedRows,
    RelationSnapshot,
    MaintainedView,
}

pub(super) fn production_output_request(
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
                public_terminal: true,
                projection: PayloadProjection::ShapeDefault,
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

pub(super) fn sync_facts() -> BTreeSet<ProgramFactKey> {
    BTreeSet::from([
        ProgramFactKey::ResultMembership,
        ProgramFactKey::SourceCoverage(program_scope()),
        ProgramFactKey::VersionWitnesses,
    ])
}

#[derive(Default)]
pub(super) struct FakeSourceResolver {
    pub(super) requests: Vec<SourceRequest>,
    pub(super) branch_witnesses: bool,
}

impl SourceGraphPreparer for FakeSourceResolver {
    async fn prepare_source_graph(
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
                    branch_or_prefix_field: self.branch_witnesses.then(|| "branch_id".to_owned()),
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
        let mut descriptor_fields = vec![
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
        ];
        if self.branch_witnesses {
            descriptor_fields.push(("branch_id", ValueType::Uuid));
        }
        Ok(ResolvedSource {
            table_schema: TableSchema::new(
                request.source.table.clone(),
                [ColumnSchema::new("title", ColumnType::String)],
            ),
            graph: GraphBuilder::table(format!("resolved_{}", request.source.table)),
            row_shape: SourceRowShape {
                source: request.source.clone(),
                descriptor: RecordDescriptor::new(descriptor_fields),
                row_uuid_field: "row_uuid".to_owned(),
                metadata,
            },
            routing_fields: BTreeSet::new(),
            requires_result_payload: false,
            content_version,
            deletion_register,
        })
    }
}

/// Executes lowered collector terminals against inline source rows. This stays
/// at the compiler boundary because the current public result-tree receiver
/// still intentionally consumes relation-edge facts; the structured carrier
/// is explicitly out of scope for this change.
pub(super) struct InlineCollectorResolver {
    pub(super) requests: Vec<SourceRequest>,
    pub(super) denied_child_title: Option<&'static str>,
    root_rows: Vec<InlineCollectorRootRow>,
}

#[derive(Clone, Copy)]
struct InlineCollectorRootRow {
    id: u8,
    title: &'static str,
    created_at: u64,
    created_by: u8,
    updated_at: u64,
    updated_by: u8,
}

impl InlineCollectorResolver {
    pub(super) fn new(denied_child_title: Option<&'static str>) -> Self {
        Self {
            requests: Vec::new(),
            denied_child_title,
            root_rows: vec![InlineCollectorRootRow {
                id: 0xd1,
                title: "parent",
                created_at: 10,
                created_by: 0xa1,
                updated_at: 11,
                updated_by: 0xa1,
            }],
        }
    }

    pub(super) fn with_root_rows(
        root_rows: impl IntoIterator<Item = (u8, &'static str, u64)>,
    ) -> Self {
        Self {
            requests: Vec::new(),
            denied_child_title: None,
            root_rows: root_rows
                .into_iter()
                .map(|(id, title, created_at)| InlineCollectorRootRow {
                    id,
                    title,
                    created_at,
                    created_by: 0xa1,
                    updated_at: created_at + 1,
                    updated_by: 0xa1,
                })
                .collect(),
        }
    }

    pub(super) fn with_provenance_root_rows(
        root_rows: impl IntoIterator<Item = (u8, &'static str, u64, u8, u64, u8)>,
    ) -> Self {
        Self {
            requests: Vec::new(),
            denied_child_title: None,
            root_rows: root_rows
                .into_iter()
                .map(
                    |(id, title, created_at, created_by, updated_at, updated_by)| {
                        InlineCollectorRootRow {
                            id,
                            title,
                            created_at,
                            created_by,
                            updated_at,
                            updated_by,
                        }
                    },
                )
                .collect(),
        }
    }
}

impl SourceGraphPreparer for InlineCollectorResolver {
    async fn prepare_source_graph(
        &mut self,
        request: &SourceRequest,
    ) -> Result<ResolvedSource, SourceResolutionError> {
        self.requests.push(request.clone());
        let descriptor = RecordDescriptor::new([
            ("row_uuid", ValueType::Uuid),
            (
                "user_title",
                ValueType::Nullable(Box::new(ValueType::String)),
            ),
            ("user_todo", ValueType::Nullable(Box::new(ValueType::Uuid))),
            ("$createdAt", ValueType::U64),
            ("$createdBy", ValueType::Uuid),
            ("$updatedAt", ValueType::U64),
            ("$updatedBy", ValueType::Uuid),
        ]);
        let parent = row(0xd1).0;
        let rows = match request.source.table.as_str() {
            "todos" => self
                .root_rows
                .iter()
                .map(|root| {
                    descriptor
                        .create(&[
                            Value::Uuid(row(root.id).0),
                            Value::Nullable(Some(Box::new(Value::String(root.title.to_owned())))),
                            Value::Nullable(None),
                            Value::U64(root.created_at),
                            Value::Uuid(row(root.created_by).0),
                            Value::U64(root.updated_at),
                            Value::Uuid(row(root.updated_by).0),
                        ])
                        .expect("inline parent")
                })
                .collect(),
            "todo_tags" => [(0xd2, "allowed"), (0xd3, "denied")]
                .into_iter()
                .filter(|(_, title)| {
                    !matches!(
                        (&request.authorization, self.denied_child_title),
                        (SourceAuthorizationRequest::PolicyFiltered { .. }, Some(denied))
                            if denied == "*" || *title == denied
                    )
                })
                .map(|(id, title)| {
                    descriptor
                        .create(&[
                            Value::Uuid(row(id).0),
                            Value::Nullable(Some(Box::new(Value::String(title.to_owned())))),
                            Value::Nullable(Some(Box::new(Value::Uuid(parent)))),
                            Value::U64(20),
                            Value::Uuid(row(0xa2).0),
                            Value::U64(21),
                            Value::Uuid(row(0xa2).0),
                        ])
                        .expect("inline child")
                })
                .collect(),
            "todo_labels" => vec![
                descriptor
                    .create(&[
                        Value::Uuid(row(0xd4).0),
                        Value::Nullable(Some(Box::new(Value::String("label".to_owned())))),
                        Value::Nullable(Some(Box::new(Value::Uuid(parent)))),
                        Value::U64(30),
                        Value::Uuid(row(0xa3).0),
                        Value::U64(31),
                        Value::Uuid(row(0xa3).0),
                    ])
                    .expect("inline sibling child"),
            ],
            "tag_notes" => vec![
                descriptor
                    .create(&[
                        Value::Uuid(row(0xd5).0),
                        Value::Nullable(Some(Box::new(Value::String("note".to_owned())))),
                        Value::Nullable(Some(Box::new(Value::Uuid(row(0xd2).0)))),
                        Value::U64(40),
                        Value::Uuid(row(0xa4).0),
                        Value::U64(41),
                        Value::Uuid(row(0xa4).0),
                    ])
                    .expect("inline grandchild"),
            ],
            other => panic!("unexpected inline collector source {other}"),
        };
        Ok(ResolvedSource {
            table_schema: TableSchema::new(
                request.source.table.clone(),
                [
                    ColumnSchema::new("title", ColumnType::String),
                    ColumnSchema::new("todo", ColumnType::Nullable(Box::new(ColumnType::Uuid))),
                ],
            ),
            graph: GraphBuilder::inline_records(descriptor.clone(), rows),
            row_shape: SourceRowShape {
                source: request.source.clone(),
                descriptor,
                row_uuid_field: "row_uuid".to_owned(),
                metadata: BTreeMap::new(),
            },
            routing_fields: BTreeSet::new(),
            requires_result_payload: false,
            content_version: None,
            deletion_register: None,
        })
    }
}

pub(super) fn app_path_projection(
    owner: SourceId,
    child: SourceId,
    field: &str,
    children: Vec<AppPathProjection>,
) -> AppPathProjection {
    AppPathProjection {
        path: ProgramPathId { owner, child },
        field: field.to_owned(),
        cardinality: PathCardinality::Many,
        fields: FieldProjection::Fields(BTreeSet::from(["title".to_owned()])),
        children,
        hole_policy: PathHolePolicy::KeepParentWithHoles,
    }
}

pub(super) fn collector_path_projection(children: Vec<AppPathProjection>) -> AppProjectionTree {
    let parent_source = source("todos", SourceRole::Root);
    let child_source = source("todo_tags", SourceRole::CorrelatedChild("tags".to_owned()));
    AppProjectionTree {
        fields: FieldProjection::All,
        paths: vec![app_path_projection(
            parent_source,
            child_source,
            "tags",
            children,
        )],
    }
}

pub(super) fn clear_path_fields(paths: &mut [AppPathProjection]) {
    for path in paths {
        path.fields = FieldProjection::Fields(BTreeSet::new());
        clear_path_fields(&mut path.children);
    }
}

pub(super) fn collector_request(policy: PolicyContext) -> QueryProgramRequest {
    let mut request = correlated_path_request(
        CorrelationRequirement::Optional,
        row_set_output(BTreeSet::new()),
    );
    request.policy = policy;
    request
        .output
        .app_rows
        .as_mut()
        .expect("app rows")
        .projection = PayloadProjection::Tree(collector_path_projection(Vec::new()));
    request
}

pub(super) fn run_collector_graph(graph: GraphBuilder) -> Vec<(Vec<Value>, i64)> {
    let mut database = Database::new(DatabaseSchema::new([]), MemoryStorage::new(&[]))
        .expect("inline collector database");
    database
        .query_graph(graph)
        .expect("execute collector graph")
        .to_values()
        .expect("decode collector rows")
}

/// Internal lowering tests are kept here because the required behavior is
/// the query-engine contract itself: public black-box APIs cannot yet prove
/// that every data path routes through this compiler boundary.
pub(super) fn correlated_path_request(
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
        authorization_mode: QueryAuthorizationMode::TrustedServing,
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
