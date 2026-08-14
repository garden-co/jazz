use super::*;
use crate::ids::{NodeUuid, SchemaVersionId};
use crate::schema::ColumnSchema;
use crate::time::{GlobalSeq, TxTime};
use crate::tx::Snapshot;
use groove::db::Database;
use groove::ivm::MAX_COLLECT_BY_TREE_DEPTH;
use groove::records::ValueType;
use groove::schema::DatabaseSchema;
use groove::storage::MemoryStorage;
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
        | GraphBuilder::VariantProject { input, .. }
        | GraphBuilder::Unnest { input, .. }
        | GraphBuilder::Project { input, .. }
        | GraphBuilder::ArgMaxBy { input, .. }
        | GraphBuilder::ArgMinBy { input, .. }
        | GraphBuilder::TopBy { input, .. }
        | GraphBuilder::CollectBy { input, .. }
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

/// Structural lowering tests still inspect the relational carrier below the
/// public boundary. The boundary itself is now deliberately a Groove Root
/// collector, so these tests assert that invariant and then search its
/// descendants for the operator they are specifically intended to exercise.
fn assert_public_root_terminal(graph: &GraphBuilder) {
    assert!(matches!(
        graph,
        GraphBuilder::CollectBy { collect, .. }
            if collect.mode == groove::ivm::CollectByMode::Root
    ));
}

fn graph_any(graph: &GraphBuilder, predicate: &impl Fn(&GraphBuilder) -> bool) -> bool {
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
        | GraphBuilder::Aggregate { input, .. } => graph_any(input, predicate),
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
            public_terminal: true,
            projection: PayloadProjection::ShapeDefault,
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
                [ColumnSchema::new("title", ColumnType::String)],
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

/// Executes lowered collector terminals against inline source rows. This stays
/// at the compiler boundary because the current public result-tree receiver
/// still intentionally consumes relation-edge facts; the structured carrier
/// is explicitly out of scope for this change.
struct InlineCollectorResolver {
    requests: Vec<SourceRequest>,
    denied_child_title: Option<&'static str>,
}

impl InlineCollectorResolver {
    fn new(denied_child_title: Option<&'static str>) -> Self {
        Self {
            requests: Vec::new(),
            denied_child_title,
        }
    }
}

impl SourceResolver for InlineCollectorResolver {
    fn resolve_source(
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
            ("$updatedAt", ValueType::U64),
        ]);
        let parent = row(0xd1).0;
        let rows = match request.source.table.as_str() {
            "todos" => vec![
                descriptor
                    .create(&[
                        Value::Uuid(parent),
                        Value::Nullable(Some(Box::new(Value::String("parent".to_owned())))),
                        Value::Nullable(None),
                        Value::U64(10),
                        Value::U64(11),
                    ])
                    .expect("inline parent"),
            ],
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
                            Value::U64(21),
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
                        Value::U64(31),
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
                        Value::U64(41),
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
            content_version: None,
            deletion_register: None,
        })
    }
}

fn app_path_projection(
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

fn collector_path_projection(children: Vec<AppPathProjection>) -> AppProjectionTree {
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

fn clear_path_fields(paths: &mut [AppPathProjection]) {
    for path in paths {
        path.fields = FieldProjection::Fields(BTreeSet::new());
        clear_path_fields(&mut path.children);
    }
}

fn collector_request(policy: PolicyContext) -> QueryProgramRequest {
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

fn run_collector_graph(graph: GraphBuilder) -> Vec<(Vec<Value>, i64)> {
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
fn closure_requirements_merge_sparse_root_and_every_alias_hop_key() {
    let root = source("roots", SourceRole::Root);
    let project = source("projects", SourceRole::Alias("include:0:0".to_owned()));
    let org = source("orgs", SourceRole::Alias("include:0:1".to_owned()));
    let backup = source("projects", SourceRole::Alias("include:1:0".to_owned()));
    let backup_org = source("orgs", SourceRole::Alias("include:1:1".to_owned()));
    let member = source("profiles", SourceRole::Alias("include:2:0".to_owned()));
    let owner = source("users", SourceRole::Alias("reference:owner".to_owned()));
    let mut input = row_set_input(0x2a);
    input.shape.result = ResultId::RealRow {
        table: "roots".to_owned(),
        row: ResultRowRef::Source(root.clone()),
    };
    input.shape.nodes = BTreeMap::from([(
        input.shape.root.clone(),
        RowSetExpr::Source {
            source: root.clone(),
            visibility: RowVisibility::Visible,
        },
    )]);
    input.shape.auxiliary_sources = BTreeSet::from([
        project.clone(),
        org.clone(),
        backup.clone(),
        backup_org.clone(),
        member.clone(),
        owner.clone(),
    ]);
    input.shape.closure_paths = vec![
        ClosurePath::ExplicitInclude {
            id: "include:0:project.org".to_owned(),
            segments: vec![
                ClosurePathSegment {
                    parent: root.clone(),
                    target: project.clone(),
                    source_field: "project".to_owned(),
                },
                ClosurePathSegment {
                    parent: project.clone(),
                    target: org.clone(),
                    source_field: "org".to_owned(),
                },
            ],
            root_gate: Some(ClosureRootGate::Inner),
        },
        ClosurePath::ExplicitInclude {
            id: "include:1:backup.org".to_owned(),
            segments: vec![
                ClosurePathSegment {
                    parent: root.clone(),
                    target: backup.clone(),
                    source_field: "backup".to_owned(),
                },
                ClosurePathSegment {
                    parent: backup.clone(),
                    target: backup_org.clone(),
                    source_field: "org".to_owned(),
                },
            ],
            root_gate: Some(ClosureRootGate::Inner),
        },
        ClosurePath::ExplicitInclude {
            id: "include:2:members".to_owned(),
            segments: vec![ClosurePathSegment {
                parent: root.clone(),
                target: member.clone(),
                source_field: "members".to_owned(),
            }],
            root_gate: Some(ClosureRootGate::Required),
        },
        ClosurePath::ImplicitRootReference {
            id: "reference:owner".to_owned(),
            segment: ClosurePathSegment {
                parent: root.clone(),
                target: owner.clone(),
                source_field: "owner".to_owned(),
            },
        },
    ];
    let mut output = row_set_output(BTreeSet::new());
    output.app_rows.as_mut().expect("app rows").projection =
        PayloadProjection::Tree(AppProjectionTree {
            fields: FieldProjection::Fields(BTreeSet::from(["title".to_owned()])),
            paths: Vec::new(),
        });
    let request = QueryProgramRequest {
        authorization_mode: QueryAuthorizationMode::TrustedServing,
        reads: QueryReadSet::primary(ReadView {
            read_schema: schema(0x10),
            policy_schema: schema(0x11),
            sources: BTreeMap::from([
                (
                    root.clone(),
                    requested_current_source(DurabilityTier::Global),
                ),
                (
                    project.clone(),
                    requested_current_source(DurabilityTier::Global),
                ),
                (
                    org.clone(),
                    requested_current_source(DurabilityTier::Global),
                ),
                (
                    backup.clone(),
                    requested_current_source(DurabilityTier::Global),
                ),
                (
                    backup_org.clone(),
                    requested_current_source(DurabilityTier::Global),
                ),
                (
                    member.clone(),
                    requested_current_source(DurabilityTier::Global),
                ),
                (
                    owner.clone(),
                    requested_current_source(DurabilityTier::Global),
                ),
            ]),
        }),
        policy: system_policy_context(),
        input,
        output,
    };

    let requirements = source_requirements_for_test(&request).expect("collect closure fields");

    let expected_root = BTreeSet::from([
        "title".to_owned(),
        "project".to_owned(),
        "backup".to_owned(),
        "members".to_owned(),
        "owner".to_owned(),
    ]);
    assert!(matches!(
        requirements.get(&root).map(|requirements| &requirements.app_fields),
        Some(FieldRequirement::Fields(fields)) if *fields == expected_root
    ));
    for source in [&project, &backup] {
        assert!(matches!(
            requirements
                .get(source)
                .map(|requirements| &requirements.app_fields),
            Some(FieldRequirement::Fields(fields)) if *fields == BTreeSet::from(["org".to_owned()])
        ));
    }
}

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
    let mut database = Database::new(DatabaseSchema::new([]), MemoryStorage::new(&[]))
        .expect("inline descriptor database");
    let runtime_rows = database
        .query_graph(terminal.graph.clone())
        .expect("infer scalar include terminal output");

    assert_eq!(runtime_rows.descriptor, app_rows.descriptor);
    let todo = runtime_rows
        .descriptor
        .field_index("user_todo")
        .expect("whole-row terminal retains the source FK");
    assert_eq!(
        runtime_rows.descriptor.fields()[todo].value_type,
        ValueType::Nullable(Box::new(ValueType::Uuid))
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
            && matches!(tie_cols.as_slice(), [groove::ivm::FieldRef::Name(field)]
                if field == "row_uuid")
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
        authorization_mode: QueryAuthorizationMode::TrustedServing,
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
}

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
    let request = collector_request(policy_context());
    let mut resolver = InlineCollectorResolver::new(Some("denied"));
    let program = lower_query_program(request, &mut resolver).expect("collector lowers");
    let terminal = program
        .lowered
        .terminals
        .iter()
        .find(|terminal| terminal.sink == "app_rows")
        .expect("app rows collector");
    assert!(matches!(terminal.graph, GraphBuilder::CollectBy { .. }));
    assert!(resolver.requests.iter().any(|request| {
        request.source.table == "todo_tags"
            && matches!(
                request.authorization,
                SourceAuthorizationRequest::PolicyFiltered { .. }
            )
            && matches!(
                &request.requirements.app_fields,
                FieldRequirement::Fields(fields) if fields.contains("title")
            )
    }));

    let rows = run_collector_graph(terminal.graph.clone());
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].1, 1);
    let Value::Array(tags) = &rows[0].0[3] else {
        panic!("collector must render the named tags slot");
    };
    assert_eq!(tags.len(), 1, "denied child must not reach the tree");
    let Value::Record(tag) = &tags[0] else {
        panic!("tags slot must contain child records");
    };
    assert_eq!(
        tag.to_values().expect("child values")[1],
        Value::String("allowed".to_owned())
    );

    let empty_request = collector_request(policy_context());
    let mut empty_resolver = InlineCollectorResolver::new(Some("*"));
    let program =
        lower_query_program(empty_request, &mut empty_resolver).expect("empty collector lowers");
    let terminal = program
        .lowered
        .terminals
        .iter()
        .find(|terminal| terminal.sink == "app_rows")
        .expect("empty app rows collector");
    let rows = run_collector_graph(terminal.graph.clone());
    assert_eq!(rows.len(), 1, "the childless parent must remain");
    let Value::Array(tags) = &rows[0].0[3] else {
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
    assert!(row.field_index("user_title").is_none());
    assert!(row.field_index("$createdAt").is_some());
    assert!(row.field_index("$updatedAt").is_some());
    assert!(row.field_index("$createdBy").is_none());
    assert!(row.field_index("$updatedBy").is_none());
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
    let rows = run_collector_graph(graph);
    let Value::Array(tags) = &rows[0].0[3] else {
        panic!("expected tags slot");
    };
    let Value::Array(labels) = &rows[0].0[4] else {
        panic!("expected sibling labels slot");
    };
    assert_eq!(tags.len(), 2);
    assert_eq!(labels.len(), 1);
    let Value::Record(tag) = &tags[0] else {
        panic!("expected tag record");
    };
    let Value::Array(notes) = &tag.to_values().expect("tag values")[2] else {
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
    let required_rows = run_collector_graph(required_graph);
    let Value::Array(required_tags) = &required_rows[0].0[3] else {
        panic!("expected required tags slot");
    };
    assert_eq!(
        required_tags.len(),
        1,
        "a child whose required nested relation is missing must be filtered"
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
    // Internal lowering test: the graph uses the child correlation as an
    // existence filter, then collapses matching children back to one parent row.
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
        GraphBuilder::ArgMinBy {
            input,
            group_cols,
            order_cols,
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
    // cardinality correlations as "at least one readable child".
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
    let user_id = ClaimPath(vec!["user_id".to_owned()]);
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
fn built_in_sub_claim_lowers_to_permission_subject() {
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
        .expect("built-in sub claim lowers");
    let graph = format!("{:?}", program.lowered.terminals[0].graph);
    assert!(graph.contains(&subject.0.to_string()), "{graph}");
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
                overlay: OverlayRef::OpenTransaction(OpenBatchId([7; 16])),
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
