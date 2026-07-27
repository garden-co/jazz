use super::*;
use groove::ivm::{
    AggregateExpr as GrooveAggregateExpr, AggregateFunction as GrooveAggregateFunction,
    LiteralValue, PlanExpr as GroovePlanExpr, PredicateExpr as GroovePredicateExpr, PredicateKind,
    ProjectField, TopByLimit, TopByOrder,
};
use groove::records::ValueType;

// Groove returns RecursiveIterationLimit instead of silently truncating when
// this bound is reached before convergence.
const FIXPOINT_MAX_ITERS: usize = 128;

/// Parameter domains attached to one lowered graph.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub(crate) struct ParameterDomain {
    /// User-supplied binding parameters.
    pub(crate) user_params: BTreeMap<String, ColumnType>,
    /// Trusted claim parameters supplied by the runtime policy context.
    pub(crate) claim_params: BTreeMap<String, ClaimParameter>,
    /// Parameters retained in terminal rows for usage-site routing.
    pub(crate) routing_params: BTreeSet<String>,
}

/// One trusted claim value carried through a prepared binding source.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct ClaimParameter {
    /// Claim path resolved from the active policy context.
    pub(crate) path: ClaimPath,
    /// Column type expected by the value-source column.
    pub(crate) ty: ColumnType,
}

/// Result of lowering one query program.
pub(crate) type QueryCompileResult = CapabilityResult<QueryProgram>;

/// Lower one Jazz query program into the unified Groove-backed program.
pub(crate) fn lower_query_program(
    request: QueryProgramRequest,
    source_resolver: &mut impl SourceResolver,
) -> QueryCompileResult {
    let mut explain = ExplainPlan::default();

    let plan = match analyze_query_plan(&request) {
        Ok(plan) => plan,
        Err(gaps) => {
            explain
                .capabilities
                .push("only current-source row-set lowering is implemented".to_owned());
            return Err(Box::new(CapabilityReport {
                gaps,
                explain: explain_with_request(&request, explain),
            }));
        }
    };

    let source_requirements = source_requirements(&request, &plan)?;
    let mut resolved_sources = BTreeMap::new();
    let source_visibilities = source_visibilities(&plan);
    for (source, requirements) in source_requirements {
        let visibility = source_visibilities
            .get(&source)
            .copied()
            .unwrap_or(RowVisibility::Visible);
        let source_request = SourceRequest {
            source: source.clone(),
            visibility,
            authorization: source_authorization_for_source(&request, &source)?,
            requirements,
        };
        let resolved_source = match source_resolver.resolve_source(&source_request) {
            Ok(resolved_source) => resolved_source,
            Err(err) => {
                let mut failure_explain = explain.clone();
                failure_explain
                    .read
                    .push(format!("failed source request: {:#?}", err.request));
                return Err(Box::new(CapabilityReport {
                    gaps: vec![UnsupportedReason::Source(err.gap)],
                    explain: explain_with_request(&request, failure_explain),
                }));
            }
        };
        explain.physical.push(format!(
            "source {:?} ({:?}) -> resolved table {}",
            source,
            source_current_tier(&request, &source),
            resolved_source.table_schema.name
        ));
        resolved_sources.insert(source, resolved_source);
    }
    let resolved_root = resolved_sources
        .get(plan.root_source())
        .cloned()
        .ok_or_else(|| {
            Box::new(CapabilityReport {
                gaps: vec![UnsupportedReason::Runtime(
                    "root source was not resolved".to_owned(),
                )],
                explain: explain_with_request(&request, explain.clone()),
            })
        })?;
    explain
        .capabilities
        .push(plan.capability_label().to_owned());
    let lowered = lower_plan_steps(
        resolved_root.graph.clone(),
        &plan,
        &resolved_root,
        &resolved_sources,
        &request,
    )
    .map_err(|gap| {
        Box::new(CapabilityReport {
            gaps: vec![gap],
            explain: explain_with_request(&request, explain.clone()),
        })
    })?;

    let mut parameters = parameter_domain_for_request(&request).map_err(|gap| {
        Box::new(CapabilityReport {
            gaps: vec![gap],
            explain: explain_with_request(&request, explain.clone()),
        })
    })?;
    collect_binding_source_params(&lowered.graph, &mut parameters);
    parameters.routing_params.retain(|field| {
        route_param_from_field(field)
            .is_some_and(|param| parameters.user_params.contains_key(param))
            || claim_path_from_param_field(field)
                .is_some_and(|_| parameters.claim_params.contains_key(field))
    });

    let terminals = lowered_terminals(
        lowered.graph,
        &request,
        &plan,
        &resolved_root,
        &resolved_sources,
        &parameters.routing_params,
        &lowered.fields,
    )?;
    verify_routed_terminal_outputs(&terminals, &parameters, &request, &explain)?;
    let output = ProgramOutputSchemas::RowSet(
        terminals
            .iter()
            .map(|terminal| terminal.output.clone())
            .collect(),
    );

    for terminal in &terminals {
        collect_binding_source_params(&terminal.graph, &mut parameters);
    }
    parameters.routing_params.retain(|field| {
        route_param_from_field(field)
            .is_some_and(|param| parameters.user_params.contains_key(param))
            || claim_path_from_param_field(field)
                .is_some_and(|_| parameters.claim_params.contains_key(field))
    });
    verify_routed_terminal_outputs(&terminals, &parameters, &request, &explain)?;

    Ok(QueryProgram {
        lowered: LoweredGraph {
            terminals,
            parameters,
            output,
            maintained_terminal_tables: resolved_sources
                .values()
                .map(|source| {
                    (
                        source.table_schema.name.clone(),
                        source.table_schema.clone(),
                    )
                })
                .collect(),
        },
        request,
        explain,
    })
}

fn verify_routed_terminal_outputs(
    terminals: &[LoweredTerminal],
    parameters: &ParameterDomain,
    request: &QueryProgramRequest,
    explain: &ExplainPlan,
) -> CapabilityResult<()> {
    for terminal in terminals {
        let expected = terminal_schema_routing_fields(&terminal.output, &parameters.routing_params);
        if expected.is_empty() {
            continue;
        }
        let Some(actual) = graph_declared_output_fields(&terminal.graph) else {
            return Err(Box::new(CapabilityReport {
                gaps: vec![UnsupportedReason::Runtime(format!(
                    "routed terminal '{}' output fields could not be verified",
                    terminal.sink
                ))],
                explain: explain_with_request(request, explain.clone()),
            }));
        };
        for field in expected {
            if !actual.contains(&field) {
                return Err(Box::new(CapabilityReport {
                    gaps: vec![UnsupportedReason::Runtime(format!(
                        "routed terminal '{}' is missing route field '{}'",
                        terminal.sink, field
                    ))],
                    explain: explain_with_request(request, explain.clone()),
                }));
            }
        }
    }
    Ok(())
}

fn terminal_schema_routing_fields(
    output: &OutputTerminalSchema,
    routing_params: &BTreeSet<String>,
) -> BTreeSet<String> {
    match output {
        OutputTerminalSchema::AppRows(schema) => schema
            .hidden_fields
            .intersection(routing_params)
            .cloned()
            .collect(),
        OutputTerminalSchema::Fact(fact) => output_routing_fields(fact),
    }
}

pub(crate) fn graph_declared_output_fields(graph: &GraphBuilder) -> Option<BTreeSet<String>> {
    match graph {
        GraphBuilder::InlineRecords { output, .. }
        | GraphBuilder::FrontierSource { output, .. }
        | GraphBuilder::BindingSource { output, .. } => descriptor_named_fields(output),
        GraphBuilder::Project { fields, .. } => Some(
            fields
                .iter()
                .map(|field| field.output_name.clone())
                .collect(),
        ),
        GraphBuilder::Aggregate {
            group_cols,
            aggregates,
            ..
        } => Some(
            group_cols
                .iter()
                .map(|field| field.display_name())
                .chain(aggregates.iter().enumerate().map(|(index, aggregate)| {
                    aggregate
                        .output_name
                        .clone()
                        .unwrap_or_else(|| format!("aggregate_{index}"))
                }))
                .collect(),
        ),
        GraphBuilder::Filter { input, .. }
        | GraphBuilder::UnwrapNullable { input, .. }
        | GraphBuilder::ArgMaxBy { input, .. }
        | GraphBuilder::ArgMinBy { input, .. }
        | GraphBuilder::TopBy { input, .. }
        | GraphBuilder::SemiJoin { left: input, .. }
        | GraphBuilder::AntiJoin { left: input, .. } => graph_declared_output_fields(input),
        GraphBuilder::Unnest {
            input,
            element_field,
            ..
        } => {
            let mut fields = graph_declared_output_fields(input)?;
            fields.insert(element_field.clone());
            Some(fields)
        }
        GraphBuilder::Recursive { seed, .. } => graph_declared_output_fields(seed),
        GraphBuilder::Union { inputs } => {
            let mut iter = inputs.iter();
            let mut fields = graph_declared_output_fields(iter.next()?)?;
            for input in iter {
                fields = fields
                    .intersection(&graph_declared_output_fields(input)?)
                    .cloned()
                    .collect();
            }
            Some(fields)
        }
        GraphBuilder::Join { left, right, .. } => {
            let mut fields = BTreeSet::new();
            fields.extend(
                graph_declared_output_fields(left)?
                    .into_iter()
                    .map(|field| left_field(&field)),
            );
            fields.extend(
                graph_declared_output_fields(right)?
                    .into_iter()
                    .map(|field| right_field(&field)),
            );
            Some(fields)
        }
        GraphBuilder::Table { .. } | GraphBuilder::Index { .. } => None,
    }
}

fn descriptor_named_fields(descriptor: &RecordDescriptor) -> Option<BTreeSet<String>> {
    descriptor
        .fields()
        .iter()
        .map(|field| field.name.clone())
        .collect()
}

fn explain_with_request(request: &QueryProgramRequest, mut explain: ExplainPlan) -> ExplainPlan {
    explain.input = format!("{:?}", request.input);
    explain.read.insert(0, format!("{:?}", request.reads));
    explain.policy.insert(0, format!("{:?}", request.policy));
    explain.output.insert(0, format!("{:?}", request.output));
    explain
}

fn source_authorization_for_source(
    request: &QueryProgramRequest,
    source: &SourceId,
) -> CapabilityResult<SourceAuthorizationRequest> {
    match &request.policy {
        PolicyContext::System => Ok(SourceAuthorizationRequest::System),
        PolicyContext::AuthorizationSubplan { .. } => Ok(SourceAuthorizationRequest::System),
        PolicyContext::Identity {
            permission_subject, ..
        } => Ok(SourceAuthorizationRequest::PolicyFiltered {
            permission_subject: *permission_subject,
            plan: PolicyAuthorizationPlan {
                protected_source: source.clone(),
                role: PolicyDecisionRole::Read,
                protected_row_field: "row_uuid".to_owned(),
                binding_source_shape: request.input.binding.source_shape.clone(),
                binding_user_params: binding_user_param_types(&request.input.binding)?,
            },
        }),
    }
}

fn binding_user_param_types(
    binding: &ProgramBinding,
) -> CapabilityResult<BTreeMap<String, ColumnType>> {
    let mut params = binding.extra_user_params.clone();
    for name in binding.values.keys() {
        let Some(ty) = binding.param_types.get(name) else {
            return Err(single_gap_report(UnsupportedReason::Runtime(format!(
                "binding parameter '{name}' is missing a validated type"
            ))));
        };
        params.insert(name.clone(), ty.clone());
    }
    Ok(params)
}

fn single_gap_report(gap: UnsupportedReason) -> Box<CapabilityReport> {
    Box::new(CapabilityReport {
        gaps: vec![gap],
        explain: ExplainPlan::default(),
    })
}

fn parameter_domain(shape: &NormalizedRowSetShape) -> ParameterDomain {
    let mut domain = ParameterDomain::default();
    for node in shape.nodes.values() {
        match node {
            RowSetExpr::ValueSource {
                columns,
                mode: ValueSourceMode::Binding,
                ..
            } => {
                for column in columns {
                    if let NormalizedValueRef::Param(param) = &column.value {
                        domain.user_params.insert(param.clone(), column.ty.clone());
                        domain.routing_params.insert(route_param_field(param));
                    } else if let NormalizedValueRef::Claim(path) = &column.value {
                        let param = claim_param_field(path);
                        domain.claim_params.insert(
                            param.clone(),
                            ClaimParameter {
                                path: path.clone(),
                                ty: column.ty.clone(),
                            },
                        );
                        domain.routing_params.insert(param);
                    }
                }
            }
            RowSetExpr::Filter { predicate, .. } => {
                collect_equality_filter_route_params(predicate, &mut domain.routing_params);
            }
            RowSetExpr::ValueSource { .. }
            | RowSetExpr::FrontierSource { .. }
            | RowSetExpr::Source { .. }
            | RowSetExpr::Join { .. }
            | RowSetExpr::RecursiveRelation { .. }
            | RowSetExpr::Union { .. }
            | RowSetExpr::Distinct { .. }
            | RowSetExpr::Project { .. }
            | RowSetExpr::CorrelatedPathProjection { .. }
            | RowSetExpr::OrderBy { .. }
            | RowSetExpr::Slice { .. } => {}
            // INV-LOWER-13: aggregation is node-side post-processing; maintained
            // aggregate outputs are capability-gated in validate_output_capabilities.
            RowSetExpr::Aggregate { .. } => {}
        }
    }
    domain
}

fn parameter_domain_for_request(
    request: &QueryProgramRequest,
) -> Result<ParameterDomain, UnsupportedReason> {
    let mut domain = parameter_domain(&request.input.shape);
    if request.input.binding.claim_params.is_empty() {
        return Ok(domain);
    }

    let pre_retarget_claims = request
        .input
        .binding
        .claim_params
        .iter()
        .map(|(name, param)| {
            (
                name.clone(),
                ClaimParameter {
                    path: param.path.clone(),
                    ty: param.ty.clone(),
                },
            )
        })
        .collect::<BTreeMap<_, _>>();
    for (name, claim) in &pre_retarget_claims {
        if let Some(existing) = domain.claim_params.get(name)
            && existing != claim
        {
            return Err(UnsupportedReason::Runtime(
                "pre-retarget claim parameter domain diverged from lowered binding sources"
                    .to_owned(),
            ));
        }
    }
    for (name, claim) in pre_retarget_claims {
        domain.claim_params.insert(name.clone(), claim);
        domain.routing_params.insert(name.clone());
    }
    Ok(domain)
}

fn collect_equality_filter_route_params(predicate: &PredicateExpr, routing: &mut BTreeSet<String>) {
    match predicate {
        PredicateExpr::And(predicates) => {
            for predicate in predicates {
                collect_equality_filter_route_params(predicate, routing);
            }
        }
        PredicateExpr::Compare {
            left,
            op: ComparisonOp::Eq,
            right,
        } => {
            if source_value_ref(left)
                && let NormalizedValueRef::Param(param) = right
            {
                routing.insert(param_route_field(param));
            } else if source_value_ref(right)
                && let NormalizedValueRef::Param(param) = left
            {
                routing.insert(param_route_field(param));
            }
        }
        PredicateExpr::True
        | PredicateExpr::False
        | PredicateExpr::Compare { .. }
        | PredicateExpr::In { .. }
        | PredicateExpr::ArrayContains { .. }
        | PredicateExpr::TextContains { .. }
        | PredicateExpr::IsNull(_)
        | PredicateExpr::IsNotNull(_)
        | PredicateExpr::Or(_)
        | PredicateExpr::Not(_) => {}
    }
}

fn param_route_field(param: &str) -> String {
    if claim_path_from_param_field(param).is_some() {
        param.to_owned()
    } else {
        route_param_field(param)
    }
}

fn source_value_ref(value: &NormalizedValueRef) -> bool {
    matches!(
        value,
        NormalizedValueRef::SourceField { .. } | NormalizedValueRef::RowId(RowIdRef::Source(_))
    )
}

fn collect_binding_source_params(graph: &GraphBuilder, domain: &mut ParameterDomain) {
    match graph {
        GraphBuilder::BindingSource { output, .. } => {
            for field in output.fields() {
                let Some(name) = field.name.as_deref() else {
                    continue;
                };
                if let Some(path) = claim_path_from_param_field(name) {
                    domain
                        .claim_params
                        .entry(name.to_owned())
                        .or_insert_with(|| ClaimParameter {
                            path,
                            ty: column_type_from_value_type(&field.value_type),
                        });
                    domain.routing_params.insert(name.to_owned());
                } else {
                    domain
                        .user_params
                        .entry(name.to_owned())
                        .or_insert_with(|| column_type_from_value_type(&field.value_type));
                    domain.routing_params.insert(route_param_field(name));
                }
            }
        }
        GraphBuilder::Recursive { seed, step, .. } => {
            collect_binding_source_params(seed, domain);
            collect_binding_source_params(step, domain);
        }
        GraphBuilder::Filter { input, .. }
        | GraphBuilder::UnwrapNullable { input, .. }
        | GraphBuilder::Unnest { input, .. }
        | GraphBuilder::Project { input, .. }
        | GraphBuilder::ArgMaxBy { input, .. }
        | GraphBuilder::ArgMinBy { input, .. }
        | GraphBuilder::TopBy { input, .. }
        | GraphBuilder::Aggregate { input, .. } => collect_binding_source_params(input, domain),
        GraphBuilder::Union { inputs } => {
            for input in inputs {
                collect_binding_source_params(input, domain);
            }
        }
        GraphBuilder::Join { left, right, .. }
        | GraphBuilder::SemiJoin { left, right, .. }
        | GraphBuilder::AntiJoin { left, right, .. } => {
            collect_binding_source_params(left, domain);
            collect_binding_source_params(right, domain);
        }
        GraphBuilder::Table { .. }
        | GraphBuilder::InlineRecords { .. }
        | GraphBuilder::Index { .. }
        | GraphBuilder::FrontierSource { .. } => {}
    }
}

fn column_type_from_value_type(value_type: &ValueType) -> ColumnType {
    match value_type {
        ValueType::U8 => ColumnType::U8,
        ValueType::U16 => ColumnType::U16,
        ValueType::U32 => ColumnType::U32,
        ValueType::U64 => ColumnType::U64,
        ValueType::I64 => ColumnType::I64,
        ValueType::F64 => ColumnType::F64,
        ValueType::Bool => ColumnType::Bool,
        ValueType::String => ColumnType::String,
        ValueType::Bytes => ColumnType::Bytes,
        ValueType::Uuid => ColumnType::Uuid,
        ValueType::Enum(schema) => ColumnType::Enum(schema.clone()),
        ValueType::Tuple(members) => {
            ColumnType::Tuple(members.iter().map(column_type_from_value_type).collect())
        }
        ValueType::Array(member) => {
            ColumnType::Array(Box::new(column_type_from_value_type(member)))
        }
        ValueType::Nullable(inner) => {
            ColumnType::Nullable(Box::new(column_type_from_value_type(inner)))
        }
    }
}

#[derive(Clone, Debug)]
struct LinearCurrentRoot {
    root: LinearRoot,
    steps: Vec<LinearStep>,
}

#[derive(Clone, Debug)]
enum LinearRoot {
    Source {
        source: SourceId,
        visibility: RowVisibility,
    },
    Value {
        shape: String,
        columns: Vec<ValueSourceColumn>,
        mode: ValueSourceMode,
    },
    Frontier {
        frontier: FrontierId,
        columns: Vec<ValueSourceColumn>,
    },
}

impl LinearRoot {
    fn source(&self) -> Option<&SourceId> {
        match self {
            LinearRoot::Source { source, .. } => Some(source),
            LinearRoot::Value { .. } | LinearRoot::Frontier { .. } => None,
        }
    }
}

#[derive(Clone, Debug)]
enum AnalyzedQueryPlan {
    Linear(LinearCurrentRoot),
    Union(UnionPlan),
    CorrelatedPath(CorrelatedPathPlan),
    RecursiveRelation(RecursiveRelationPlan),
}

impl AnalyzedQueryPlan {
    fn root_source(&self) -> &SourceId {
        match self {
            AnalyzedQueryPlan::Linear(plan) => plan.root.source().expect("linear root source"),
            AnalyzedQueryPlan::Union(plan) => plan.root_source().expect("union root source"),
            AnalyzedQueryPlan::CorrelatedPath(plan) => {
                plan.parent.root.source().expect("path parent source")
            }
            AnalyzedQueryPlan::RecursiveRelation(plan) => plan
                .seed
                .root
                .source()
                .or_else(|| first_step_source(&plan.seed.steps))
                .or_else(|| plan.step.root.source())
                .or_else(|| first_step_source(&plan.step.steps))
                .expect("recursive source"),
        }
    }

    fn capability_label(&self) -> &'static str {
        match self {
            AnalyzedQueryPlan::Linear(_) => "table-rooted current lowering",
            AnalyzedQueryPlan::Union(_) => "union current lowering",
            AnalyzedQueryPlan::CorrelatedPath(_) => "correlated path projection analysis",
            AnalyzedQueryPlan::RecursiveRelation(_) => "recursive relation analysis",
        }
    }
}

fn first_step_source(steps: &[LinearStep]) -> Option<&SourceId> {
    steps.iter().find_map(|step| match step {
        LinearStep::Join { right, .. } => right.root_source(),
        LinearStep::Filter(_)
        | LinearStep::Project(_)
        | LinearStep::OrderBy(_)
        | LinearStep::Slice { .. }
        | LinearStep::Aggregate { .. } => None,
    })
}

#[derive(Clone, Debug)]
struct CorrelatedPathPlan {
    parent: LinearCurrentRoot,
    child: LinearCurrentRoot,
    path: ProgramPathId,
    correlation: PredicateExpr,
    requirement: CorrelationRequirement,
    output_steps: Vec<LinearStep>,
    siblings: Vec<CorrelatedPathPlan>,
    nested: Vec<CorrelatedPathPlan>,
}

#[derive(Clone, Debug)]
struct RecursiveRelationPlan {
    seed: LinearCurrentRoot,
    step: LinearCurrentRoot,
    frontier: FrontierId,
    frontier_key: NormalizedValueRef,
    dedupe_keys: Vec<NormalizedValueRef>,
    bound: RecursionBound,
}

#[derive(Clone, Debug)]
struct UnionPlan {
    branches: Vec<UnionBranchPlan>,
}

impl UnionPlan {
    fn root_source(&self) -> Option<&SourceId> {
        let mut sources = self
            .branches
            .iter()
            .filter_map(|branch| branch.plan.root_source());
        let first = sources.next()?;
        if sources.all(|source| source == first) {
            Some(first)
        } else {
            None
        }
    }
}

#[derive(Clone, Debug)]
struct UnionBranchPlan {
    plan: RelationInputPlan,
}

impl RecursiveRelationPlan {
    fn root_source(&self) -> Option<&SourceId> {
        self.seed
            .root
            .source()
            .or_else(|| first_step_source(&self.seed.steps))
            .or_else(|| self.step.root.source())
            .or_else(|| first_step_source(&self.step.steps))
    }

    fn seed_source(&self) -> Option<&SourceId> {
        self.seed
            .root
            .source()
            .or_else(|| first_step_source(&self.seed.steps))
    }

    fn step_source(&self) -> Option<&SourceId> {
        self.step
            .root
            .source()
            .or_else(|| first_step_source(&self.step.steps))
    }
}

#[derive(Clone, Debug)]
enum RelationInputPlan {
    Linear(LinearCurrentRoot),
    Union(UnionPlan),
    Recursive(RecursiveRelationPlan),
}

impl RelationInputPlan {
    fn root_source(&self) -> Option<&SourceId> {
        match self {
            RelationInputPlan::Linear(linear) => linear.root.source(),
            RelationInputPlan::Union(union) => union.root_source(),
            RelationInputPlan::Recursive(relation) => relation.root_source(),
        }
    }
}

#[derive(Clone, Debug)]
enum LinearStep {
    Filter(PredicateExpr),
    Join {
        right: Box<RelationInputPlan>,
        mode: JoinMode,
        on: PredicateExpr,
    },
    Project(Vec<RowProjection>),
    OrderBy(Vec<OrderKey>),
    Slice {
        partition_by: Vec<NormalizedValueRef>,
        limit: Option<u32>,
        offset: u32,
        tie_breaker: Vec<NormalizedValueRef>,
        rank_output: Option<TypedOutputField>,
    },
    Aggregate {
        group_by: Vec<NormalizedValueRef>,
        outputs: Vec<AggregateExpr>,
    },
}

fn analyze_query_plan(
    request: &QueryProgramRequest,
) -> Result<AnalyzedQueryPlan, Vec<UnsupportedReason>> {
    let mut gaps = Vec::new();

    if !request.reads.fact_reads.is_empty() {
        gaps.push(UnsupportedReason::Source(SourceGap::TransactionReadOverlay));
    }
    let analyzed = analyze_root_node(request);
    let Ok(plan) = analyzed else {
        gaps.push(analyzed.unwrap_err());
        return Err(gaps);
    };
    validate_output_capabilities(request, &plan, &mut gaps);

    for plan_source in analyzed_plan_sources(&plan) {
        let read_source = request.reads.primary.sources.get(&plan_source);
        let Some(projection) = supported_current_storage_projection(read_source) else {
            gaps.push(UnsupportedReason::Source(SourceGap::HistoricalStorageCut));
            continue;
        };
        if !matches!(projection.schema_family, SchemaFamilySelection::Current)
            || !matches!(projection.storage, StorageSchemaSelection::Single(_))
            || !matches!(projection.lens, LensSelection::Canonical)
        {
            gaps.push(UnsupportedReason::Source(SourceGap::SchemaProjection));
        }
    }

    if gaps.is_empty() { Ok(plan) } else { Err(gaps) }
}

fn validate_output_capabilities(
    request: &QueryProgramRequest,
    plan: &AnalyzedQueryPlan,
    gaps: &mut Vec<UnsupportedReason>,
) {
    if !request
        .output
        .facts
        .contains(&ProgramFactKey::ResultMembership)
        && !request
            .output
            .facts
            .contains(&ProgramFactKey::AuthorizedRows)
    {
        return;
    }
    if !request
        .output
        .facts
        .contains(&ProgramFactKey::ResultMembership)
    {
        return;
    }
    if plan_contains_aggregate(plan) {
        return;
    }
    if maintained_result_membership_window_supported(plan) {
        return;
    }
    gaps.push(UnsupportedReason::Operator(
        "maintained subscription view window shape is not lowered yet".to_owned(),
    ));
}

fn maintained_result_membership_window_supported(plan: &AnalyzedQueryPlan) -> bool {
    collect_plan_fragments(plan)
        .linears
        .iter()
        .all(|fragment| linear_window_supported(fragment.steps))
}

fn plan_contains_aggregate(plan: &AnalyzedQueryPlan) -> bool {
    collect_plan_fragments(plan).linears.iter().any(|fragment| {
        fragment
            .steps
            .iter()
            .any(|step| matches!(step, LinearStep::Aggregate { .. }))
    })
}

fn root_aggregate_step(
    plan: &AnalyzedQueryPlan,
) -> Option<(&[NormalizedValueRef], &[AggregateExpr])> {
    let AnalyzedQueryPlan::Linear(linear) = plan else {
        return None;
    };
    match linear.steps.last()? {
        LinearStep::Aggregate { group_by, outputs } => Some((group_by, outputs)),
        _ => None,
    }
}

fn linear_window_supported(steps: &[LinearStep]) -> bool {
    let mut has_order = false;
    for step in steps {
        match step {
            LinearStep::OrderBy(_) => has_order = true,
            LinearStep::Slice { limit, offset, .. } => {
                if !has_order && (*offset != 0 || !matches!(limit, None | Some(1))) {
                    return false;
                }
            }
            LinearStep::Filter(_)
            | LinearStep::Join { .. }
            | LinearStep::Project(_)
            | LinearStep::Aggregate { .. } => {}
        }
    }
    true
}

fn analyze_root_node(
    request: &QueryProgramRequest,
) -> Result<AnalyzedQueryPlan, UnsupportedReason> {
    let mut visited = BTreeSet::new();
    let root_node = request
        .input
        .shape
        .nodes
        .get(&request.input.shape.root)
        .ok_or_else(|| {
            UnsupportedReason::Operator(format!(
                "row-set root node {:?} is missing",
                request.input.shape.root
            ))
        })?;

    let plan = match root_node {
        RowSetExpr::CorrelatedPathProjection {
            input,
            child_input,
            path,
            correlation,
            requirement,
        } => {
            visited.insert(request.input.shape.root.clone());
            let parent = analyze_linear_root(input, request, &mut visited)?;
            let child = analyze_correlated_child_subplan(
                child_input,
                path,
                &request.input.shape.nodes,
                &mut visited,
            )?;
            validate_result_source(
                request,
                parent.root.source().ok_or_else(|| {
                    UnsupportedReason::Operator("path parent must be a source".to_owned())
                })?,
            )?;
            AnalyzedQueryPlan::CorrelatedPath(CorrelatedPathPlan {
                path: path.clone(),
                correlation: correlation.clone(),
                requirement: *requirement,
                output_steps: Vec::new(),
                siblings: collect_sibling_correlated_paths(
                    parent.root.source().ok_or_else(|| {
                        UnsupportedReason::Operator("path parent must be a source".to_owned())
                    })?,
                    &path.child,
                    &request.input.shape.nodes,
                    &mut visited,
                )?,
                nested: collect_nested_correlated_paths(
                    &path.child,
                    &request.input.shape.nodes,
                    &mut visited,
                )?,
                parent,
                child,
            })
        }
        RowSetExpr::RecursiveRelation {
            seed,
            step,
            frontier,
            frontier_key,
            dedupe_keys,
            bound,
        } => {
            visited.insert(request.input.shape.root.clone());
            let seed = analyze_linear_root(seed, request, &mut visited)?;
            let step = analyze_linear_subplan(step, &request.input.shape.nodes, &mut visited)?;
            match &request.input.shape.result {
                ResultId::RealRow {
                    row: ResultRowRef::Source(result_source),
                    ..
                } if seed.root.source() == Some(result_source)
                    || step.root.source() == Some(result_source) => {}
                ResultId::PathTuple { .. } => {}
                _ => {
                    return Err(UnsupportedReason::Operator(
                        "recursive relation result must be a seed/step real row or path tuple"
                            .to_owned(),
                    ));
                }
            }
            AnalyzedQueryPlan::RecursiveRelation(RecursiveRelationPlan {
                seed,
                step,
                frontier: frontier.clone(),
                frontier_key: frontier_key.clone(),
                dedupe_keys: dedupe_keys.clone(),
                bound: *bound,
            })
        }
        RowSetExpr::Union { inputs } => {
            visited.insert(request.input.shape.root.clone());
            let union = analyze_union(inputs, &request.input.shape.nodes, &mut visited)?;
            validate_result_source(
                request,
                union.root_source().ok_or_else(|| {
                    UnsupportedReason::Operator(
                        "union result branches must share one root source".to_owned(),
                    )
                })?,
            )?;
            AnalyzedQueryPlan::Union(union)
        }
        _ => {
            let mut path_visited = visited.clone();
            if let Ok(plan) =
                analyze_correlated_path_root(&request.input.shape.root, request, &mut path_visited)
            {
                let mut plan = plan;
                plan.nested = collect_nested_correlated_paths(
                    &plan.path.child,
                    &request.input.shape.nodes,
                    &mut path_visited,
                )?;
                plan.siblings = collect_sibling_correlated_paths(
                    plan.parent.root.source().ok_or_else(|| {
                        UnsupportedReason::Operator("path parent must be a source".to_owned())
                    })?,
                    &plan.path.child,
                    &request.input.shape.nodes,
                    &mut path_visited,
                )?;
                validate_result_source(
                    request,
                    plan.parent.root.source().ok_or_else(|| {
                        UnsupportedReason::Operator("path parent must be a source".to_owned())
                    })?,
                )?;
                visited = path_visited;
                AnalyzedQueryPlan::CorrelatedPath(plan)
            } else {
                let linear = analyze_linear_root(&request.input.shape.root, request, &mut visited)?;
                validate_result_source(
                    request,
                    linear.root.source().ok_or_else(|| {
                        UnsupportedReason::Operator("result must be the root source row".to_owned())
                    })?,
                )?;
                AnalyzedQueryPlan::Linear(linear)
            }
        }
    };

    if visited.len() != request.input.shape.nodes.len() {
        return Err(UnsupportedReason::Operator(
            "only connected current source/filter/join/order/slice/path/relation plans are lowered yet"
                .to_owned(),
        ));
    }
    Ok(plan)
}

fn analyze_correlated_path_root(
    node_id: &RowSetNodeId,
    request: &QueryProgramRequest,
    visited: &mut BTreeSet<RowSetNodeId>,
) -> Result<CorrelatedPathPlan, UnsupportedReason> {
    let node = request.input.shape.nodes.get(node_id).ok_or_else(|| {
        UnsupportedReason::Operator(format!("row-set node {:?} is missing", node_id))
    })?;
    visited.insert(node_id.clone());
    match node {
        RowSetExpr::CorrelatedPathProjection {
            input,
            child_input,
            path,
            correlation,
            requirement,
        } => {
            let parent = analyze_linear_root(input, request, visited)?;
            let child = analyze_correlated_child_subplan(
                child_input,
                path,
                &request.input.shape.nodes,
                visited,
            )?;
            Ok(CorrelatedPathPlan {
                parent,
                child,
                path: path.clone(),
                correlation: correlation.clone(),
                requirement: *requirement,
                output_steps: Vec::new(),
                siblings: Vec::new(),
                nested: collect_nested_correlated_paths(
                    &path.child,
                    &request.input.shape.nodes,
                    visited,
                )?,
            })
        }
        RowSetExpr::OrderBy { input, keys } => {
            let mut plan = analyze_correlated_path_root(input, request, visited)?;
            plan.output_steps.push(LinearStep::OrderBy(keys.clone()));
            Ok(plan)
        }
        RowSetExpr::Slice {
            input,
            partition_by,
            limit,
            offset,
            tie_breaker,
            rank_output,
        } => {
            let mut plan = analyze_correlated_path_root(input, request, visited)?;
            plan.output_steps.push(LinearStep::Slice {
                partition_by: partition_by.clone(),
                limit: *limit,
                offset: *offset,
                tie_breaker: tie_breaker.clone(),
                rank_output: rank_output.clone(),
            });
            Ok(plan)
        }
        RowSetExpr::Project { input, columns } => {
            let mut plan = analyze_correlated_path_root(input, request, visited)?;
            plan.output_steps.push(LinearStep::Project(columns.clone()));
            Ok(plan)
        }
        _ => Err(UnsupportedReason::Operator(
            "root is not a correlated path plan".to_owned(),
        )),
    }
}

fn collect_nested_correlated_paths(
    owner: &SourceId,
    nodes: &BTreeMap<RowSetNodeId, RowSetExpr>,
    visited: &mut BTreeSet<RowSetNodeId>,
) -> Result<Vec<CorrelatedPathPlan>, UnsupportedReason> {
    let mut paths = Vec::new();
    for (node_id, node) in nodes {
        let RowSetExpr::CorrelatedPathProjection {
            input,
            child_input,
            path,
            correlation,
            requirement,
        } = node
        else {
            continue;
        };
        if &path.owner != owner {
            continue;
        }
        visited.insert(node_id.clone());
        let mut parent_visited = BTreeSet::new();
        let parent = analyze_linear_subplan(input, nodes, &mut parent_visited)?;
        visited.extend(parent_visited);
        let mut child_visited = BTreeSet::new();
        let child = analyze_correlated_child_subplan(child_input, path, nodes, &mut child_visited)?;
        visited.extend(child_visited);
        paths.push(CorrelatedPathPlan {
            parent,
            child,
            path: path.clone(),
            correlation: correlation.clone(),
            requirement: *requirement,
            output_steps: Vec::new(),
            siblings: Vec::new(),
            nested: collect_nested_correlated_paths(&path.child, nodes, visited)?,
        });
    }
    Ok(paths)
}

fn collect_sibling_correlated_paths(
    owner: &SourceId,
    excluded_child: &SourceId,
    nodes: &BTreeMap<RowSetNodeId, RowSetExpr>,
    visited: &mut BTreeSet<RowSetNodeId>,
) -> Result<Vec<CorrelatedPathPlan>, UnsupportedReason> {
    let mut paths = Vec::new();
    for (node_id, node) in nodes {
        let RowSetExpr::CorrelatedPathProjection {
            input,
            child_input,
            path,
            correlation,
            requirement,
        } = node
        else {
            continue;
        };
        if &path.owner != owner || &path.child == excluded_child {
            continue;
        }
        visited.insert(node_id.clone());
        let mut parent_visited = BTreeSet::new();
        let parent = analyze_linear_subplan(input, nodes, &mut parent_visited)?;
        visited.extend(parent_visited);
        let mut child_visited = BTreeSet::new();
        let child = analyze_correlated_child_subplan(child_input, path, nodes, &mut child_visited)?;
        visited.extend(child_visited);
        paths.push(CorrelatedPathPlan {
            parent,
            child,
            path: path.clone(),
            correlation: correlation.clone(),
            requirement: *requirement,
            output_steps: Vec::new(),
            siblings: Vec::new(),
            nested: collect_nested_correlated_paths(&path.child, nodes, visited)?,
        });
    }
    Ok(paths)
}

fn analyze_correlated_child_subplan(
    child_input: &RowSetNodeId,
    path: &ProgramPathId,
    nodes: &BTreeMap<RowSetNodeId, RowSetExpr>,
    visited: &mut BTreeSet<RowSetNodeId>,
) -> Result<LinearCurrentRoot, UnsupportedReason> {
    if let Some(RowSetExpr::CorrelatedPathProjection {
        input,
        path: nested_path,
        ..
    }) = nodes.get(child_input)
        && nested_path.owner == path.child
    {
        return analyze_linear_subplan(input, nodes, visited);
    }
    analyze_linear_subplan(child_input, nodes, visited)
}

fn analyze_linear_root(
    node_id: &RowSetNodeId,
    request: &QueryProgramRequest,
    visited: &mut BTreeSet<RowSetNodeId>,
) -> Result<LinearCurrentRoot, UnsupportedReason> {
    let (source, steps) = analyze_current_node(node_id, &request.input.shape.nodes, visited)?;
    let mut gaps = Vec::new();
    validate_step_order(&steps, &mut gaps);
    if let Some(gap) = gaps.into_iter().next() {
        return Err(gap);
    }
    Ok(LinearCurrentRoot {
        root: source,
        steps,
    })
}

fn analyze_linear_subplan(
    node_id: &RowSetNodeId,
    nodes: &BTreeMap<RowSetNodeId, RowSetExpr>,
    visited: &mut BTreeSet<RowSetNodeId>,
) -> Result<LinearCurrentRoot, UnsupportedReason> {
    let (source, steps) = analyze_current_node(node_id, nodes, visited)?;
    let mut gaps = Vec::new();
    validate_step_order(&steps, &mut gaps);
    if let Some(gap) = gaps.into_iter().next() {
        return Err(gap);
    }
    Ok(LinearCurrentRoot {
        root: source,
        steps,
    })
}

fn validate_result_source(
    request: &QueryProgramRequest,
    source: &SourceId,
) -> Result<(), UnsupportedReason> {
    if matches!(
        request.input.shape.result,
        ResultId::RealRow {
            row: ResultRowRef::Source(ref result_source),
            ..
        } if result_source == source
    ) {
        Ok(())
    } else {
        Err(UnsupportedReason::Operator(
            "result must be the root source row".to_owned(),
        ))
    }
}

struct LinearFragment<'a> {
    root: Option<&'a LinearRoot>,
    steps: &'a [LinearStep],
}

#[derive(Default)]
struct PlanFragments<'a> {
    linears: Vec<LinearFragment<'a>>,
    correlations: Vec<&'a PredicateExpr>,
    recursives: Vec<&'a RecursiveRelationPlan>,
}

fn collect_plan_fragments(plan: &AnalyzedQueryPlan) -> PlanFragments<'_> {
    let mut fragments = PlanFragments::default();
    collect_analyzed_fragments(plan, &mut fragments);
    fragments
}

fn collect_analyzed_fragments<'a>(plan: &'a AnalyzedQueryPlan, fragments: &mut PlanFragments<'a>) {
    match plan {
        AnalyzedQueryPlan::Linear(linear) => collect_linear_fragments(linear, fragments),
        AnalyzedQueryPlan::Union(union) => collect_union_fragments(union, fragments),
        AnalyzedQueryPlan::CorrelatedPath(path) => {
            collect_correlated_path_fragments(path, fragments)
        }
        AnalyzedQueryPlan::RecursiveRelation(relation) => {
            collect_recursive_fragments(relation, fragments)
        }
    }
}

fn collect_correlated_path_fragments<'a>(
    path: &'a CorrelatedPathPlan,
    fragments: &mut PlanFragments<'a>,
) {
    collect_linear_fragments(&path.parent, fragments);
    collect_linear_fragments(&path.child, fragments);
    fragments.correlations.push(&path.correlation);
    if !path.output_steps.is_empty() {
        fragments.linears.push(LinearFragment {
            root: None,
            steps: &path.output_steps,
        });
        collect_step_relation_fragments(&path.output_steps, fragments);
    }
    for sibling in &path.siblings {
        collect_correlated_path_fragments(sibling, fragments);
    }
    for nested in &path.nested {
        collect_correlated_path_fragments(nested, fragments);
    }
}

fn collect_relation_fragments<'a>(plan: &'a RelationInputPlan, fragments: &mut PlanFragments<'a>) {
    match plan {
        RelationInputPlan::Linear(linear) => collect_linear_fragments(linear, fragments),
        RelationInputPlan::Union(union) => collect_union_fragments(union, fragments),
        RelationInputPlan::Recursive(relation) => collect_recursive_fragments(relation, fragments),
    }
}

fn collect_union_fragments<'a>(union: &'a UnionPlan, fragments: &mut PlanFragments<'a>) {
    for branch in &union.branches {
        collect_relation_fragments(&branch.plan, fragments);
    }
}

fn collect_recursive_fragments<'a>(
    relation: &'a RecursiveRelationPlan,
    fragments: &mut PlanFragments<'a>,
) {
    fragments.recursives.push(relation);
    collect_linear_fragments(&relation.seed, fragments);
    collect_linear_fragments(&relation.step, fragments);
}

fn collect_linear_fragments<'a>(linear: &'a LinearCurrentRoot, fragments: &mut PlanFragments<'a>) {
    fragments.linears.push(LinearFragment {
        root: Some(&linear.root),
        steps: &linear.steps,
    });
    collect_step_relation_fragments(&linear.steps, fragments);
}

fn collect_step_relation_fragments<'a>(steps: &'a [LinearStep], fragments: &mut PlanFragments<'a>) {
    for step in steps {
        if let LinearStep::Join { right, .. } = step {
            collect_relation_fragments(right, fragments);
        }
    }
}

fn analyzed_plan_sources(plan: &AnalyzedQueryPlan) -> BTreeSet<SourceId> {
    collect_plan_fragments(plan)
        .linears
        .into_iter()
        .filter_map(|fragment| fragment.root?.source().cloned())
        .collect()
}

fn program_sources(request: &QueryProgramRequest, plan: &AnalyzedQueryPlan) -> BTreeSet<SourceId> {
    let mut sources = analyzed_plan_sources(plan);
    sources.extend(request.input.shape.auxiliary_sources.iter().cloned());
    sources
}

fn source_visibilities(plan: &AnalyzedQueryPlan) -> BTreeMap<SourceId, RowVisibility> {
    let mut visibilities = BTreeMap::new();
    for fragment in collect_plan_fragments(plan).linears {
        if let Some(LinearRoot::Source { source, visibility }) = fragment.root {
            let entry = visibilities
                .entry(source.clone())
                .or_insert(RowVisibility::Visible);
            if *visibility > *entry {
                *entry = *visibility;
            }
        }
    }
    visibilities
}

fn source_current_tier(request: &QueryProgramRequest, source: &SourceId) -> Option<DurabilityTier> {
    request.reads.primary.sources.get(source)?.current_tier()
}

fn supported_current_storage_projection(
    source: Option<&RequestedSourceExpr>,
) -> Option<&SchemaProjection<RequestedSourceStage>> {
    match source? {
        SourceExpr::VisibleCurrent {
            projection,
            data: DataSource::Current | DataSource::Branch(_),
            tier: _,
        }
        | SourceExpr::HistoryCut {
            projection,
            data: DataSource::Current,
            position: _,
        }
        | SourceExpr::SnapshotRef {
            projection,
            data: DataSource::Current,
            snapshot: _,
        }
        | SourceExpr::SettledBindingView {
            projection,
            binding_view: _,
        } => Some(projection),
        SourceExpr::WithOverlays { input, overlays } => {
            if overlays
                .entries
                .iter()
                .all(|overlay| matches!(overlay, OverlayRef::OpenTransaction(_)))
            {
                supported_current_storage_projection(Some(input.as_ref()))
            } else {
                None
            }
        }
        _ => None,
    }
}

fn analyze_current_node(
    node_id: &RowSetNodeId,
    nodes: &BTreeMap<RowSetNodeId, RowSetExpr>,
    visited: &mut BTreeSet<RowSetNodeId>,
) -> Result<(LinearRoot, Vec<LinearStep>), UnsupportedReason> {
    if !visited.insert(node_id.clone()) {
        return Err(UnsupportedReason::Operator(format!(
            "shared row-set subgraphs are not lowered yet (node revisited): {:?}",
            node_id
        )));
    }
    let Some(node) = nodes.get(node_id) else {
        return Err(UnsupportedReason::Operator(format!(
            "row-set node {:?} is missing",
            node_id
        )));
    };

    match node {
        RowSetExpr::Source { source, visibility } => Ok((
            LinearRoot::Source {
                source: source.clone(),
                visibility: *visibility,
            },
            Vec::new(),
        )),
        RowSetExpr::ValueSource {
            shape,
            columns,
            mode,
        } => Ok((
            LinearRoot::Value {
                shape: shape.clone(),
                columns: columns.clone(),
                mode: mode.clone(),
            },
            Vec::new(),
        )),
        RowSetExpr::FrontierSource { frontier, columns } => Ok((
            LinearRoot::Frontier {
                frontier: frontier.clone(),
                columns: columns.clone(),
            },
            Vec::new(),
        )),
        RowSetExpr::Filter { input, predicate } => {
            let (source, mut steps) = analyze_current_node(input, nodes, visited)?;
            steps.push(LinearStep::Filter(predicate.clone()));
            Ok((source, steps))
        }
        RowSetExpr::OrderBy { input, keys } => {
            let (source, mut steps) = analyze_current_node(input, nodes, visited)?;
            steps.push(LinearStep::OrderBy(keys.clone()));
            Ok((source, steps))
        }
        RowSetExpr::Slice {
            input,
            partition_by,
            limit,
            offset,
            tie_breaker,
            rank_output,
        } => {
            let (source, mut steps) = analyze_current_node(input, nodes, visited)?;
            steps.push(LinearStep::Slice {
                partition_by: partition_by.clone(),
                limit: *limit,
                offset: *offset,
                tie_breaker: tie_breaker.clone(),
                rank_output: rank_output.clone(),
            });
            Ok((source, steps))
        }
        RowSetExpr::Join {
            left,
            right,
            mode,
            on,
        } => {
            let (source, mut steps) = analyze_current_node(left, nodes, visited)?;
            let right = analyze_relation_input_node(right, nodes, visited)?;
            steps.push(LinearStep::Join {
                right: Box::new(right),
                mode: *mode,
                on: on.clone(),
            });
            Ok((source, steps))
        }
        RowSetExpr::Project { input, columns } => {
            let (source, mut steps) = analyze_current_node(input, nodes, visited)?;
            steps.push(LinearStep::Project(columns.clone()));
            Ok((source, steps))
        }
        RowSetExpr::RecursiveRelation { .. } => Err(UnsupportedReason::Operator(
            "recursive relation row-set nodes are not lowered yet".to_owned(),
        )),
        RowSetExpr::Union { .. } => Err(UnsupportedReason::Operator(
            "union row-set nodes are not lowered yet".to_owned(),
        )),
        RowSetExpr::Distinct { keys, .. } => Err(UnsupportedReason::Operator(
            unsupported_marker_message(keys)
                .unwrap_or_else(|| "distinct row-set nodes are not lowered yet".to_owned()),
        )),
        RowSetExpr::CorrelatedPathProjection { input, .. } => {
            analyze_current_node(input, nodes, visited)
        }
        RowSetExpr::Aggregate {
            input,
            group_by,
            outputs,
        } => {
            let (source, mut steps) = analyze_current_node(input, nodes, visited)?;
            steps.push(LinearStep::Aggregate {
                group_by: group_by.clone(),
                outputs: outputs.clone(),
            });
            Ok((source, steps))
        }
    }
}

fn analyze_relation_input_node(
    node_id: &RowSetNodeId,
    nodes: &BTreeMap<RowSetNodeId, RowSetExpr>,
    visited: &mut BTreeSet<RowSetNodeId>,
) -> Result<RelationInputPlan, UnsupportedReason> {
    let Some(node) = nodes.get(node_id) else {
        return Err(UnsupportedReason::Operator(format!(
            "row-set node {:?} is missing",
            node_id
        )));
    };

    match node {
        RowSetExpr::Union { inputs } => {
            if !visited.insert(node_id.clone()) {
                return Err(UnsupportedReason::Operator(format!(
                    "shared row-set subgraphs are not lowered yet (node revisited): {:?}",
                    node_id
                )));
            }
            analyze_union(inputs, nodes, visited).map(RelationInputPlan::Union)
        }
        RowSetExpr::RecursiveRelation {
            seed,
            step,
            frontier,
            frontier_key,
            dedupe_keys,
            bound,
        } => {
            if !visited.insert(node_id.clone()) {
                return Err(UnsupportedReason::Operator(format!(
                    "shared row-set subgraphs are not lowered yet (node revisited): {:?}",
                    node_id
                )));
            }
            let seed = analyze_linear_subplan(seed, nodes, visited)?;
            let step = analyze_linear_subplan(step, nodes, visited)?;
            Ok(RelationInputPlan::Recursive(RecursiveRelationPlan {
                seed,
                step,
                frontier: frontier.clone(),
                frontier_key: frontier_key.clone(),
                dedupe_keys: dedupe_keys.clone(),
                bound: *bound,
            }))
        }
        _ => {
            let linear = analyze_linear_subplan(node_id, nodes, visited)?;
            validate_join_relation(&linear)?;
            Ok(RelationInputPlan::Linear(linear))
        }
    }
}

fn analyze_union(
    inputs: &[UnionInput],
    nodes: &BTreeMap<RowSetNodeId, RowSetExpr>,
    visited: &mut BTreeSet<RowSetNodeId>,
) -> Result<UnionPlan, UnsupportedReason> {
    if inputs.is_empty() {
        return Err(UnsupportedReason::Operator(
            "union row-set nodes require at least one input".to_owned(),
        ));
    }

    let mut branches = Vec::new();
    for input in inputs {
        let plan = analyze_relation_input_node(&input.node, nodes, visited)?;
        branches.push(UnionBranchPlan { plan });
    }
    Ok(UnionPlan { branches })
}

fn validate_join_relation(plan: &LinearCurrentRoot) -> Result<(), UnsupportedReason> {
    for step in &plan.steps {
        match step {
            LinearStep::Filter(_) | LinearStep::Join { .. } | LinearStep::Project(_) => {}
            LinearStep::OrderBy(_) | LinearStep::Slice { .. } | LinearStep::Aggregate { .. } => {
                return Err(UnsupportedReason::Operator(
                    "join inputs do not support order/slice/aggregate operators yet".to_owned(),
                ));
            }
        }
    }
    Ok(())
}

fn unsupported_marker_message(keys: &[NormalizedValueRef]) -> Option<String> {
    let [NormalizedValueRef::Literal(bytes)] = keys else {
        return None;
    };
    String::from_utf8(bytes.clone()).ok()
}

fn predicate_contains_param(predicate: &PredicateExpr) -> bool {
    match predicate {
        PredicateExpr::True | PredicateExpr::False => false,
        PredicateExpr::Compare { left, right, .. } => {
            value_contains_param(left) || value_contains_param(right)
        }
        PredicateExpr::In { value, options } => {
            value_contains_param(value) || options.iter().any(value_contains_param)
        }
        PredicateExpr::ArrayContains { value, needle }
        | PredicateExpr::TextContains { value, needle } => {
            value_contains_param(value) || value_contains_param(needle)
        }
        PredicateExpr::IsNull(value) | PredicateExpr::IsNotNull(value) => {
            value_contains_param(value)
        }
        PredicateExpr::And(predicates) | PredicateExpr::Or(predicates) => {
            predicates.iter().any(predicate_contains_param)
        }
        PredicateExpr::Not(predicate) => predicate_contains_param(predicate),
    }
}

fn value_contains_param(value: &NormalizedValueRef) -> bool {
    matches!(value, NormalizedValueRef::Param(_))
}

fn validate_step_order(steps: &[LinearStep], gaps: &mut Vec<UnsupportedReason>) {
    let mut seen_order = false;
    let mut seen_slice = false;
    let mut seen_aggregate = false;
    for step in steps {
        match step {
            LinearStep::Filter(_) | LinearStep::Join { .. } | LinearStep::Project(_)
                if seen_order || seen_slice || seen_aggregate =>
            {
                gaps.push(UnsupportedReason::Operator(
                    "filters/joins/projects after order/slice/aggregate are not lowered yet"
                        .to_owned(),
                ));
            }
            LinearStep::Filter(_) | LinearStep::Join { .. } | LinearStep::Project(_) => {}
            LinearStep::OrderBy(_) | LinearStep::Slice { .. } if seen_aggregate => {
                gaps.push(UnsupportedReason::Operator(
                    "order/slice after aggregate is not lowered yet".to_owned(),
                ));
            }
            LinearStep::OrderBy(_) if seen_slice => {
                gaps.push(UnsupportedReason::Operator(
                    "order-by after slice is not lowered yet".to_owned(),
                ));
            }
            LinearStep::OrderBy(_) if seen_order => {
                gaps.push(UnsupportedReason::Operator(
                    "multiple order-by nodes are not lowered yet".to_owned(),
                ));
            }
            LinearStep::OrderBy(_) => {
                seen_order = true;
            }
            LinearStep::Slice { rank_output, .. } => {
                if seen_slice {
                    gaps.push(UnsupportedReason::Operator(
                        "multiple slice nodes are not lowered yet".to_owned(),
                    ));
                }
                if rank_output.is_some() {
                    gaps.push(UnsupportedReason::Operator(
                        "slice rank outputs are not lowered yet".to_owned(),
                    ));
                }
                seen_slice = true;
            }
            LinearStep::Aggregate { .. } => {
                if seen_order || seen_slice {
                    gaps.push(UnsupportedReason::Operator(
                        "aggregate over ordered/windowed input is not lowered yet".to_owned(),
                    ));
                }
                seen_aggregate = true;
            }
        }
    }
}

fn source_requirements(
    request: &QueryProgramRequest,
    plan: &AnalyzedQueryPlan,
) -> CapabilityResult<BTreeMap<SourceId, SourceRequirements>> {
    let output = &request.output;
    let mut requirements = BTreeMap::<SourceId, SourceRequirements>::new();
    for source in program_sources(request, plan) {
        requirements.insert(source, SourceRequirements::default());
    }

    if let Some(app_rows) = &output.app_rows {
        if !matches!(
            plan,
            AnalyzedQueryPlan::Linear(_) | AnalyzedQueryPlan::CorrelatedPath(_)
        ) {
            return Err(Box::new(CapabilityReport {
                gaps: vec![UnsupportedReason::Operator(
                    "app row materialization for recursive relation projections is not lowered yet"
                        .to_owned(),
                )],
                explain: ExplainPlan {
                    capabilities: vec!["recursive relation app rows are not lowered".to_owned()],
                    ..ExplainPlan::default()
                },
            }));
        }
        let root_requirements = requirements
            .get_mut(plan.root_source())
            .expect("root source requirements were initialized");
        root_requirements.app_fields = match &app_rows.projection {
            PayloadProjection::ShapeDefault => FieldRequirement::All,
            PayloadProjection::Tree(tree) => tree.fields.clone().into(),
        };
    }

    for fact in &output.facts {
        match fact {
            ProgramFactKey::AuthorizedRows => {}
            ProgramFactKey::ResultMembership => {
                let root_requirements = requirements
                    .get_mut(plan.root_source())
                    .expect("root source requirements were initialized");
                root_requirements
                    .metadata
                    .insert(SourceMetadataRequirement::VersionWitnesses);
                root_requirements
                    .metadata
                    .insert(SourceMetadataRequirement::SettlePosition);
                for contribution in &request.input.shape.join_contributions {
                    if let Some(source_requirements) = requirements.get_mut(&contribution.source) {
                        source_requirements
                            .metadata
                            .insert(SourceMetadataRequirement::VersionWitnesses);
                        source_requirements
                            .metadata
                            .insert(SourceMetadataRequirement::SettlePosition);
                    }
                }
            }
            ProgramFactKey::VersionWitnesses | ProgramFactKey::ReplacementWitnesses => {
                for source_requirements in requirements.values_mut() {
                    source_requirements
                        .metadata
                        .insert(SourceMetadataRequirement::VersionWitnesses);
                    source_requirements
                        .metadata
                        .insert(SourceMetadataRequirement::VersionPayloads);
                    source_requirements
                        .metadata
                        .insert(SourceMetadataRequirement::DeletionMarkers);
                }
            }
            ProgramFactKey::SourceCoverage(scope) => match scope {
                CoverageScope::Program => {
                    for source_requirements in requirements.values_mut() {
                        source_requirements
                            .metadata
                            .insert(SourceMetadataRequirement::Coverage);
                    }
                }
                CoverageScope::Source(source) => {
                    let source_requirements = requirements.get_mut(source).ok_or_else(|| {
                        single_gap_report(UnsupportedReason::Source(SourceGap::Coverage))
                    })?;
                    source_requirements
                        .metadata
                        .insert(SourceMetadataRequirement::Coverage);
                }
                CoverageScope::Path(_) => {
                    let root_requirements = requirements
                        .get_mut(plan.root_source())
                        .expect("root source requirements were initialized");
                    root_requirements
                        .metadata
                        .insert(SourceMetadataRequirement::Coverage);
                }
            },
            ProgramFactKey::RelationEdges | ProgramFactKey::PathCorrelationCoverage => {
                for source_requirements in requirements.values_mut() {
                    source_requirements
                        .metadata
                        .insert(SourceMetadataRequirement::VersionWitnesses);
                }
            }
            _ => {
                return Err(Box::new(CapabilityReport {
                    gaps: vec![UnsupportedReason::Output(Box::new(fact.clone()))],
                    explain: ExplainPlan {
                        capabilities: vec!["requested fact is not lowered yet".to_owned()],
                        ..ExplainPlan::default()
                    },
                }));
            }
        }
    }

    collect_plan_requirements(plan, &mut requirements)?;

    Ok(requirements)
}

fn collect_plan_requirements(
    plan: &AnalyzedQueryPlan,
    requirements: &mut BTreeMap<SourceId, SourceRequirements>,
) -> CapabilityResult<()> {
    let fragments = collect_plan_fragments(plan);
    for fragment in &fragments.linears {
        for step in fragment.steps {
            for (source, source_requirements) in requirements.iter_mut() {
                collect_step_requirements(step, source, source_requirements)?;
            }
        }
    }
    for correlation in fragments.correlations {
        collect_predicate_requirements_for_all_sources(correlation, requirements)?;
    }
    for relation in fragments.recursives {
        if !matches!(
            relation.frontier_key,
            NormalizedValueRef::FrontierColumn { .. }
                | NormalizedValueRef::RowId(RowIdRef::Frontier(_))
                | NormalizedValueRef::Param(_)
                | NormalizedValueRef::Literal(_)
        ) {
            collect_value_requirements_for_all_sources(&relation.frontier_key, requirements)?;
        }
        for key in &relation.dedupe_keys {
            if !matches!(
                key,
                NormalizedValueRef::FrontierColumn { .. }
                    | NormalizedValueRef::RowId(RowIdRef::Frontier(_))
                    | NormalizedValueRef::Param(_)
                    | NormalizedValueRef::Literal(_)
            ) {
                collect_value_requirements_for_all_sources(key, requirements)?;
            }
        }
    }
    Ok(())
}

fn collect_predicate_requirements_for_all_sources(
    predicate: &PredicateExpr,
    requirements: &mut BTreeMap<SourceId, SourceRequirements>,
) -> CapabilityResult<()> {
    for (source, source_requirements) in requirements.iter_mut() {
        collect_predicate_requirements(predicate, source, source_requirements).map_err(|gap| {
            Box::new(CapabilityReport {
                gaps: vec![gap],
                explain: ExplainPlan {
                    capabilities: vec!["path correlation requirements are not lowered".to_owned()],
                    ..ExplainPlan::default()
                },
            })
        })?;
    }
    Ok(())
}

fn collect_value_requirements_for_all_sources(
    value: &NormalizedValueRef,
    requirements: &mut BTreeMap<SourceId, SourceRequirements>,
) -> CapabilityResult<()> {
    for (source, source_requirements) in requirements.iter_mut() {
        collect_value_requirements(value, source, source_requirements).map_err(|gap| {
            Box::new(CapabilityReport {
                gaps: vec![gap],
                explain: ExplainPlan {
                    capabilities: vec!["relation key requirements are not lowered".to_owned()],
                    ..ExplainPlan::default()
                },
            })
        })?;
    }
    Ok(())
}

impl From<FieldProjection> for FieldRequirement {
    fn from(value: FieldProjection) -> Self {
        match value {
            FieldProjection::All => FieldRequirement::All,
            FieldProjection::Fields(fields) => FieldRequirement::Fields(fields),
        }
    }
}

fn collect_step_requirements(
    step: &LinearStep,
    source: &SourceId,
    requirements: &mut SourceRequirements,
) -> CapabilityResult<()> {
    let result: Result<(), UnsupportedReason> = match step {
        LinearStep::Filter(predicate) => {
            collect_predicate_requirements(predicate, source, requirements)
        }
        LinearStep::Join { on, .. } => (|| {
            collect_predicate_requirements(on, source, requirements)?;
            Ok(())
        })(),
        LinearStep::Project(columns) => (|| {
            for column in columns {
                collect_value_requirements(&column.value, source, requirements)?;
            }
            Ok(())
        })(),
        LinearStep::OrderBy(keys) => (|| {
            for key in keys {
                collect_value_requirements(&key.value, source, requirements)?;
            }
            Ok(())
        })(),
        LinearStep::Slice {
            partition_by,
            tie_breaker,
            ..
        } => (|| {
            for value in partition_by.iter().chain(tie_breaker) {
                collect_value_requirements(value, source, requirements)?;
            }
            Ok(())
        })(),
        LinearStep::Aggregate { group_by, outputs } => (|| {
            for value in group_by {
                collect_value_requirements(value, source, requirements)?;
            }
            for aggregate in outputs {
                if let Some(input) = &aggregate.input {
                    collect_value_requirements(input, source, requirements)?;
                }
            }
            Ok(())
        })(),
    };

    result.map_err(|gap| {
        Box::new(CapabilityReport {
            gaps: vec![gap],
            explain: ExplainPlan {
                capabilities: vec!["operator source requirements are not lowered".to_owned()],
                ..ExplainPlan::default()
            },
        })
    })
}

fn collect_predicate_requirements(
    predicate: &PredicateExpr,
    source: &SourceId,
    requirements: &mut SourceRequirements,
) -> Result<(), UnsupportedReason> {
    match predicate {
        PredicateExpr::True | PredicateExpr::False => Ok(()),
        PredicateExpr::Compare { left, right, .. } => {
            collect_value_requirements(left, source, requirements)?;
            collect_value_requirements(right, source, requirements)
        }
        PredicateExpr::In { value, options } => {
            collect_value_requirements(value, source, requirements)?;
            for option in options {
                collect_value_requirements(option, source, requirements)?;
            }
            Ok(())
        }
        PredicateExpr::ArrayContains { value, needle }
        | PredicateExpr::TextContains { value, needle } => {
            collect_value_requirements(value, source, requirements)?;
            collect_value_requirements(needle, source, requirements)
        }
        PredicateExpr::IsNull(value) | PredicateExpr::IsNotNull(value) => {
            collect_value_requirements(value, source, requirements)
        }
        PredicateExpr::And(predicates) | PredicateExpr::Or(predicates) => {
            for predicate in predicates {
                collect_predicate_requirements(predicate, source, requirements)?;
            }
            Ok(())
        }
        PredicateExpr::Not(predicate) => {
            collect_predicate_requirements(predicate, source, requirements)
        }
    }
}

fn collect_value_requirements(
    value: &NormalizedValueRef,
    source: &SourceId,
    requirements: &mut SourceRequirements,
) -> Result<(), UnsupportedReason> {
    match value {
        NormalizedValueRef::SourceField {
            source: value_source,
            field,
        } => {
            if value_source != source {
                return Ok(());
            }
            add_required_app_field(requirements, field.clone());
        }
        NormalizedValueRef::Provenance {
            source: value_source,
            field,
        } => {
            if value_source != source {
                return Ok(());
            }
            requirements
                .metadata
                .insert(SourceMetadataRequirement::Provenance(*field));
        }
        NormalizedValueRef::RowId(RowIdRef::Source(value_source)) if value_source == source => {}
        NormalizedValueRef::RowId(RowIdRef::Source(value_source)) => {
            let _ = value_source;
        }
        NormalizedValueRef::Param(_)
        | NormalizedValueRef::Claim(_)
        | NormalizedValueRef::Literal(_) => {}
        NormalizedValueRef::FrontierColumn { .. }
        | NormalizedValueRef::RowId(RowIdRef::Frontier(_)) => {}
    }
    Ok(())
}

fn add_required_app_field(requirements: &mut SourceRequirements, field: String) {
    match &mut requirements.app_fields {
        FieldRequirement::None => {
            requirements.app_fields = FieldRequirement::Fields(BTreeSet::from([field]));
        }
        FieldRequirement::Fields(fields) => {
            fields.insert(field);
        }
        FieldRequirement::All => {}
    }
}

fn lower_plan_steps(
    graph: GraphBuilder,
    plan: &AnalyzedQueryPlan,
    root_source: &ResolvedSource,
    resolved_sources: &BTreeMap<SourceId, ResolvedSource>,
    request: &QueryProgramRequest,
) -> Result<LoweredRelationInput, UnsupportedReason> {
    match plan {
        AnalyzedQueryPlan::Linear(linear) => {
            lower_linear_plan_steps(graph, linear, root_source, resolved_sources, request)
        }
        AnalyzedQueryPlan::Union(union) => {
            lower_union_plan(union, Some(graph), root_source, resolved_sources, request)
        }
        AnalyzedQueryPlan::CorrelatedPath(path) => {
            lower_correlated_path_plan(graph, path, root_source, resolved_sources, request)
        }
        AnalyzedQueryPlan::RecursiveRelation(relation) => lower_recursive_relation(
            Some(graph),
            relation,
            root_source,
            resolved_sources,
            request,
        ),
    }
}

fn lower_correlated_path_plan(
    graph: GraphBuilder,
    path: &CorrelatedPathPlan,
    root_source: &ResolvedSource,
    resolved_sources: &BTreeMap<SourceId, ResolvedSource>,
    request: &QueryProgramRequest,
) -> Result<LoweredRelationInput, UnsupportedReason> {
    let parent =
        lower_linear_plan_steps(graph, &path.parent, root_source, resolved_sources, request)?;
    let child_root = path
        .child
        .root
        .source()
        .ok_or_else(|| UnsupportedReason::Operator("path child must be a source".to_owned()))?;
    let child_source = resolved_sources.get(child_root).ok_or_else(|| {
        UnsupportedReason::Runtime(format!(
            "path child source {:?} was not resolved",
            child_root
        ))
    })?;
    let child_relation_steps = path
        .child
        .steps
        .iter()
        .filter(|step| !matches!(step, LinearStep::OrderBy(_) | LinearStep::Slice { .. }))
        .cloned()
        .collect::<Vec<_>>();
    let child_relation_plan = LinearCurrentRoot {
        root: path.child.root.clone(),
        steps: child_relation_steps,
    };
    let child = lower_linear_plan_steps(
        child_source.graph.clone(),
        &child_relation_plan,
        child_source,
        resolved_sources,
        request,
    )?;
    let (parent_key, child_key) = lower_path_key_pair(
        &path.correlation,
        path.parent.root.source().ok_or_else(|| {
            UnsupportedReason::Operator("path parent must be a source".to_owned())
        })?,
        root_source,
        child_root,
        child_source,
        request,
    )?;
    let parent_key_nullable_depth = source_field_nullable_depth(root_source, &parent_key);
    let child_key_nullable_depth = source_field_nullable_depth(child_source, &child_key);
    let child =
        unwrap_join_key_if_nullable(child.graph, child_key.clone(), child_key_nullable_depth);

    let lowered = match path.requirement {
        CorrelationRequirement::Optional => Ok(LoweredRelationInput {
            graph: parent.graph,
            root_source: Some(root_source.clone()),
            fields: source_fields(root_source).collect(),
            nullable_fields: source_nullable_fields(root_source),
            nullable_field_depths: source_nullable_field_depths(root_source),
        }),
        CorrelationRequirement::AtLeastOne => {
            let parent = unwrap_join_key_if_nullable(
                parent.graph,
                parent_key.clone(),
                parent_key_nullable_depth,
            );
            let joined =
                GraphBuilder::join(parent, child, [parent_key], [child_key]).project_fields(
                    project_source_fields_from_prefix(root_source, LEFT_JOIN_PREFIX),
                );
            Ok(LoweredRelationInput {
                graph: GraphBuilder::arg_min_by(
                    joined,
                    [root_source.row_shape.row_uuid_field.clone()],
                    [root_source.row_shape.row_uuid_field.clone()],
                ),
                root_source: Some(root_source.clone()),
                fields: source_fields(root_source).collect(),
                nullable_fields: source_nullable_fields(root_source),
                nullable_field_depths: source_nullable_field_depths(root_source),
            })
        }
        CorrelationRequirement::MatchCorrelationCardinality => {
            let parent = unwrap_join_key_if_nullable(
                parent.graph,
                parent_key.clone(),
                parent_key_nullable_depth,
            );
            lower_cardinality_complete_parent_graph(
                parent,
                child,
                root_source,
                parent_key,
                child_key,
            )
            .map(|graph| LoweredRelationInput {
                graph,
                root_source: Some(root_source.clone()),
                fields: source_fields(root_source).collect(),
                nullable_fields: source_nullable_fields(root_source),
                nullable_field_depths: source_nullable_field_depths(root_source),
            })
        }
    }
    .and_then(|lowered| {
        if path.output_steps.is_empty() {
            Ok(lowered)
        } else {
            let tail = LinearCurrentRoot {
                root: path.parent.root.clone(),
                steps: path.output_steps.clone(),
            };
            lower_linear_plan_steps(lowered.graph, &tail, root_source, resolved_sources, request)
        }
    })?;
    Ok(lowered)
}

fn lower_cardinality_complete_parent_graph(
    parent: GraphBuilder,
    child: GraphBuilder,
    root_source: &ResolvedSource,
    parent_key: String,
    child_key: String,
) -> Result<GraphBuilder, UnsupportedReason> {
    let Some(parent_key_type) = source_field_type(root_source, &parent_key) else {
        return Err(UnsupportedReason::Operator(format!(
            "match-correlation-cardinality parent key {parent_key:?} is not projected"
        )));
    };
    let is_array_key = match parent_key_type {
        ValueType::Array(_) => true,
        ValueType::Nullable(inner) => matches!(inner.as_ref(), ValueType::Array(_)),
        _ => false,
    };
    if !is_array_key {
        let joined = GraphBuilder::join(parent, child, [parent_key], [child_key]).project_fields(
            project_source_fields_from_prefix(root_source, LEFT_JOIN_PREFIX),
        );
        return Ok(GraphBuilder::arg_min_by(
            joined,
            [root_source.row_shape.row_uuid_field.clone()],
            [root_source.row_shape.row_uuid_field.clone()],
        ));
    }

    let required_element_field = "__jazz_required_correlation_element";
    let required = parent
        .clone()
        .unnest(parent_key.clone(), required_element_field);
    let mut covered_fields = project_source_fields_from_prefix(root_source, LEFT_JOIN_PREFIX);
    covered_fields.push(ProjectField::renamed(
        left_field(required_element_field),
        required_element_field,
    ));
    let covered = GraphBuilder::join(
        required.clone(),
        child,
        [required_element_field],
        [child_key],
    )
    .project_fields(covered_fields);
    let missing = GraphBuilder::anti_join(
        required,
        covered,
        [
            root_source.row_shape.row_uuid_field.clone(),
            required_element_field.to_owned(),
        ],
        [
            root_source.row_shape.row_uuid_field.clone(),
            required_element_field.to_owned(),
        ],
    )
    .project_fields(project_source_fields_from_prefix(root_source, ""));
    Ok(GraphBuilder::anti_join(
        parent,
        missing,
        [root_source.row_shape.row_uuid_field.clone()],
        [root_source.row_shape.row_uuid_field.clone()],
    ))
}

fn lower_correlated_path_relation_graph(
    path: &CorrelatedPathPlan,
    root_source: &ResolvedSource,
    resolved_sources: &BTreeMap<SourceId, ResolvedSource>,
    request: &QueryProgramRequest,
) -> Result<LoweredRelationInput, UnsupportedReason> {
    let parent = lower_linear_plan_steps(
        root_source.graph.clone(),
        &path.parent,
        root_source,
        resolved_sources,
        request,
    )?;
    lower_correlated_path_relation_graph_from_parent(
        path,
        parent.graph,
        root_source,
        resolved_sources,
        request,
    )
}

fn lower_correlated_path_relation_graph_from_parent(
    path: &CorrelatedPathPlan,
    parent: GraphBuilder,
    parent_source: &ResolvedSource,
    resolved_sources: &BTreeMap<SourceId, ResolvedSource>,
    request: &QueryProgramRequest,
) -> Result<LoweredRelationInput, UnsupportedReason> {
    let child_root = path
        .child
        .root
        .source()
        .ok_or_else(|| UnsupportedReason::Operator("path child must be a source".to_owned()))?;
    let child_source = resolved_sources.get(child_root).ok_or_else(|| {
        UnsupportedReason::Runtime(format!(
            "path child source {:?} was not resolved",
            child_root
        ))
    })?;
    let child_plan = LinearCurrentRoot {
        root: path.child.root.clone(),
        steps: child_steps_for_relation_edges(&path.child.steps),
    };
    let child = lower_linear_plan_steps(
        child_source.graph.clone(),
        &child_plan,
        child_source,
        resolved_sources,
        request,
    )?;
    let (parent_key, child_key) = lower_path_key_pair(
        &path.correlation,
        path.parent.root.source().ok_or_else(|| {
            UnsupportedReason::Operator("path parent must be a source".to_owned())
        })?,
        parent_source,
        child_root,
        child_source,
        request,
    )?;
    let parent_key_nullable_depth = source_field_nullable_depth(parent_source, &parent_key);
    let child_key_nullable_depth = source_field_nullable_depth(child_source, &child_key);
    let parent = unwrap_join_key_if_nullable(parent, parent_key.clone(), parent_key_nullable_depth);
    let child =
        unwrap_join_key_if_nullable(child.graph, child_key.clone(), child_key_nullable_depth);
    Ok(LoweredRelationInput {
        graph: GraphBuilder::join(parent, child, [parent_key], [child_key]),
        root_source: None,
        fields: BTreeSet::new(),
        nullable_fields: BTreeSet::new(),
        nullable_field_depths: BTreeMap::new(),
    })
}

fn child_steps_for_relation_edges(steps: &[LinearStep]) -> Vec<LinearStep> {
    let mut previous_was_order_by = false;
    let mut filtered = Vec::with_capacity(steps.len());
    for step in steps {
        match step {
            LinearStep::Slice { .. } if !previous_was_order_by => {
                previous_was_order_by = false;
            }
            _ => {
                previous_was_order_by = matches!(step, LinearStep::OrderBy(_));
                filtered.push(step.clone());
            }
        }
    }
    filtered
}

fn unwrap_join_key_if_nullable(
    mut graph: GraphBuilder,
    field: String,
    nullable_depth: usize,
) -> GraphBuilder {
    for _ in 0..nullable_depth {
        graph = graph.unwrap_nullable(field.clone());
    }
    graph
}

fn unwrap_nullable_join_key(
    graph: GraphBuilder,
    field: String,
    nullable_depth: usize,
) -> GraphBuilder {
    unwrap_join_key_if_nullable(graph, field, nullable_depth)
}

#[derive(Clone, Debug)]
struct LoweredRelationInput {
    graph: GraphBuilder,
    root_source: Option<ResolvedSource>,
    fields: BTreeSet<String>,
    nullable_fields: BTreeSet<String>,
    nullable_field_depths: BTreeMap<String, usize>,
}

fn lower_relation_input(
    plan: &RelationInputPlan,
    resolved_sources: &BTreeMap<SourceId, ResolvedSource>,
    request: &QueryProgramRequest,
) -> Result<LoweredRelationInput, UnsupportedReason> {
    match plan {
        RelationInputPlan::Linear(linear) => {
            let source_id = linear.root.source().ok_or_else(|| {
                UnsupportedReason::Operator("linear join input must have a source".to_owned())
            })?;
            let source = resolved_sources.get(source_id).cloned().ok_or_else(|| {
                UnsupportedReason::Runtime(format!("join source {:?} was not resolved", source_id))
            })?;
            lower_linear_plan_steps(
                source.graph.clone(),
                linear,
                &source,
                resolved_sources,
                request,
            )
        }
        RelationInputPlan::Union(union) => {
            lower_union_relation_input(union, resolved_sources, request)
        }
        RelationInputPlan::Recursive(relation) => {
            let source_id = relation.root_source().ok_or_else(|| {
                UnsupportedReason::Operator(
                    "recursive join input must include a table source".to_owned(),
                )
            })?;
            let source = resolved_sources.get(source_id).cloned().ok_or_else(|| {
                UnsupportedReason::Runtime(format!(
                    "recursive join source {:?} was not resolved",
                    source_id
                ))
            })?;
            lower_recursive_relation(None, relation, &source, resolved_sources, request)
        }
    }
}

fn lower_union_relation_input(
    union: &UnionPlan,
    resolved_sources: &BTreeMap<SourceId, ResolvedSource>,
    request: &QueryProgramRequest,
) -> Result<LoweredRelationInput, UnsupportedReason> {
    let mut lowered = Vec::new();
    for branch in &union.branches {
        lowered.push(lower_relation_input(
            &branch.plan,
            resolved_sources,
            request,
        )?);
    }
    lower_union_inputs(lowered, request)
}

fn lower_union_plan(
    union: &UnionPlan,
    root_graph: Option<GraphBuilder>,
    root_source: &ResolvedSource,
    resolved_sources: &BTreeMap<SourceId, ResolvedSource>,
    request: &QueryProgramRequest,
) -> Result<LoweredRelationInput, UnsupportedReason> {
    let mut lowered = Vec::new();
    for branch in &union.branches {
        let input = match &branch.plan {
            RelationInputPlan::Linear(linear) => {
                let source_id = linear.root.source().ok_or_else(|| {
                    UnsupportedReason::Operator("union branch must have a source".to_owned())
                })?;
                if source_id != &root_source.row_shape.source {
                    return Err(UnsupportedReason::Operator(
                        "root union branches must share the query result source".to_owned(),
                    ));
                }
                let graph = if let Some(root_graph) = &root_graph {
                    root_graph.clone()
                } else {
                    resolved_sources
                        .get(source_id)
                        .ok_or_else(|| {
                            UnsupportedReason::Runtime(format!(
                                "union branch source {:?} was not resolved",
                                source_id
                            ))
                        })?
                        .graph
                        .clone()
                };
                lower_linear_plan_steps(graph, linear, root_source, resolved_sources, request)?
            }
            RelationInputPlan::Union(_) | RelationInputPlan::Recursive(_) => {
                lower_relation_input(&branch.plan, resolved_sources, request)?
            }
        };
        lowered.push(input);
    }
    lower_union_inputs(lowered, request)
}

fn lower_union_inputs(
    lowered: Vec<LoweredRelationInput>,
    request: &QueryProgramRequest,
) -> Result<LoweredRelationInput, UnsupportedReason> {
    let union_fields = lowered_union_fields(&lowered);
    let needs_alignment = lowered.iter().any(|branch| branch.fields != union_fields);
    let lowered = if needs_alignment {
        lowered
            .into_iter()
            .map(|branch| align_union_route_fields(branch, &union_fields, request))
            .collect::<Result<Vec<_>, _>>()?
    } else {
        lowered
    };
    let mut lowered = lowered.into_iter();
    let first = lowered.next().ok_or_else(|| {
        UnsupportedReason::Operator("union row-set nodes require at least one input".to_owned())
    })?;
    let mut graphs = vec![first.graph];
    let mut root_source = first.root_source;
    let fields = first.fields;
    let mut nullable_fields = first.nullable_fields;
    let mut nullable_field_depths = first.nullable_field_depths;
    for branch in lowered {
        if branch.fields != fields {
            return Err(UnsupportedReason::Operator(
                "union branches must output the same fields".to_owned(),
            ));
        }
        nullable_fields.extend(branch.nullable_fields);
        for (field, depth) in branch.nullable_field_depths {
            nullable_field_depths
                .entry(field)
                .and_modify(|existing| *existing = (*existing).max(depth))
                .or_insert(depth);
        }
        if root_source.as_ref().map(|source| &source.row_shape.source)
            != branch
                .root_source
                .as_ref()
                .map(|source| &source.row_shape.source)
        {
            root_source = None;
        }
        graphs.push(branch.graph);
    }
    Ok(LoweredRelationInput {
        graph: GraphBuilder::union(graphs),
        root_source,
        fields,
        nullable_fields,
        nullable_field_depths,
    })
}

fn lowered_union_fields(lowered: &[LoweredRelationInput]) -> BTreeSet<String> {
    lowered
        .iter()
        .flat_map(|branch| branch.fields.iter().cloned())
        .collect()
}

fn align_union_route_fields(
    mut branch: LoweredRelationInput,
    fields: &BTreeSet<String>,
    request: &QueryProgramRequest,
) -> Result<LoweredRelationInput, UnsupportedReason> {
    let route_fields = parameter_domain_for_request(request)?.routing_params;
    let missing = fields
        .difference(&branch.fields)
        .cloned()
        .collect::<BTreeSet<_>>();
    if missing.iter().any(|field| !route_fields.contains(field)) {
        return Err(UnsupportedReason::Operator(
            "union branches must output the same fields".to_owned(),
        ));
    }

    let project_fields = fields
        .iter()
        .map(|field| {
            if branch.fields.contains(field) {
                Ok(ProjectField::named(field.clone()))
            } else {
                route_literal_project_field(field, request)
            }
        })
        .collect::<Result<Vec<_>, UnsupportedReason>>()?;
    branch.graph = branch.graph.project_fields(project_fields);
    branch.fields = fields.clone();
    Ok(branch)
}

fn linear_root_fields(root: &LinearRoot) -> BTreeSet<String> {
    match root {
        LinearRoot::Source { .. } => BTreeSet::new(),
        LinearRoot::Value { columns, .. } | LinearRoot::Frontier { columns, .. } => {
            columns.iter().map(|column| column.name.clone()).collect()
        }
    }
}

fn source_fields(source: &ResolvedSource) -> impl Iterator<Item = String> + '_ {
    source
        .row_shape
        .descriptor
        .fields()
        .iter()
        .filter_map(|field| field.name.clone())
        .chain(source.routing_fields.iter().cloned())
}

fn source_nullable_fields(source: &ResolvedSource) -> BTreeSet<String> {
    source_nullable_field_depths(source).into_keys().collect()
}

fn source_nullable_field_depths(source: &ResolvedSource) -> BTreeMap<String, usize> {
    let mut depths = BTreeMap::new();
    for name in source_fields(source) {
        if let Some((field, depth)) = {
            let depth = source_field_nullable_depth(source, &name);
            (depth > 0).then_some((name, depth))
        } {
            if let Some(logical) = field.strip_prefix(USER_COLUMN_PREFIX) {
                depths.insert(logical.to_owned(), depth);
            }
            depths.insert(field, depth);
        }
    }
    depths
}

fn lower_recursive_relation(
    root_graph: Option<GraphBuilder>,
    relation: &RecursiveRelationPlan,
    root_source: &ResolvedSource,
    resolved_sources: &BTreeMap<SourceId, ResolvedSource>,
    request: &QueryProgramRequest,
) -> Result<LoweredRelationInput, UnsupportedReason> {
    let seed_root_source = relation
        .seed_source()
        .and_then(|source| resolved_sources.get(source));
    let seed_root = seed_root_source.map(|resolved| resolved.graph.clone());
    let seed_graph = seed_root
        .or(root_graph)
        .unwrap_or_else(|| root_source.graph.clone());
    let seed = lower_linear_plan_steps(
        seed_graph,
        &relation.seed,
        seed_root_source.unwrap_or(root_source),
        resolved_sources,
        request,
    )?;
    let step_source_id = relation.step_source().ok_or_else(|| {
        UnsupportedReason::Operator("recursive step must include a table source".to_owned())
    })?;
    let step_source = resolved_sources.get(step_source_id).ok_or_else(|| {
        UnsupportedReason::Runtime(format!(
            "recursive step source {:?} was not resolved",
            step_source_id
        ))
    })?;
    let step = lower_linear_plan_steps(
        step_source.graph.clone(),
        &relation.step,
        step_source,
        resolved_sources,
        request,
    )?;
    let max_iters = match relation.bound {
        RecursionBound::Fixpoint => FIXPOINT_MAX_ITERS,
        RecursionBound::MaxDepth(max_depth) => max_depth.max(1),
    };
    if seed.fields != step.fields {
        return Err(UnsupportedReason::Operator(
            "recursive seed and step outputs must have the same fields".to_owned(),
        ));
    }
    let fields = seed.fields.clone();
    Ok(LoweredRelationInput {
        graph: GraphBuilder::recursive(
            seed.graph,
            step.graph,
            relation.frontier.0.clone(),
            max_iters,
        ),
        root_source: Some(root_source.clone()),
        fields,
        nullable_fields: BTreeSet::new(),
        nullable_field_depths: BTreeMap::new(),
    })
}

fn lower_linear_plan_steps(
    graph: GraphBuilder,
    plan: &LinearCurrentRoot,
    root_source: &ResolvedSource,
    resolved_sources: &BTreeMap<SourceId, ResolvedSource>,
    request: &QueryProgramRequest,
) -> Result<LoweredRelationInput, UnsupportedReason> {
    let mut graph = match &plan.root {
        LinearRoot::Source { .. } => graph,
        LinearRoot::Value {
            shape,
            columns,
            mode,
        } => lower_value_source(shape, columns, mode, request)?,
        LinearRoot::Frontier { frontier, columns } => {
            GraphBuilder::frontier_source(frontier.0.clone(), value_source_descriptor(columns))
        }
    };
    let mut fields: BTreeSet<String> = match &plan.root {
        LinearRoot::Source { .. } => source_fields(root_source).collect(),
        LinearRoot::Value { columns, .. } | LinearRoot::Frontier { columns, .. } => {
            columns.iter().map(|column| column.name.clone()).collect()
        }
    };
    let mut nullable_fields = if matches!(plan.root, LinearRoot::Source { .. }) {
        source_nullable_fields(root_source)
    } else {
        BTreeSet::new()
    };
    let mut nullable_field_depths = if matches!(plan.root, LinearRoot::Source { .. }) {
        source_nullable_field_depths(root_source)
    } else {
        BTreeMap::new()
    };
    let mut pending_order: Option<Vec<OrderKey>> = None;
    let mut last_join_right: Option<(
        RelationInputPlan,
        BTreeSet<String>,
        BTreeMap<String, usize>,
        BTreeSet<String>,
    )> = None;
    let mut available_route_fields = if matches!(plan.root, LinearRoot::Source { .. }) {
        root_source.routing_fields.clone()
    } else {
        BTreeSet::new()
    };
    let route_fields = parameter_domain_for_request(request)?.routing_params;

    for (step_index, step) in plan.steps.iter().enumerate() {
        match step {
            LinearStep::Filter(predicate) => {
                last_join_right = None;
                let source = plan.root.source().ok_or_else(|| {
                    UnsupportedReason::Operator(
                        "filters on value/frontier sources are not lowered yet".to_owned(),
                    )
                })?;
                let (joined, residual, introduced_route_fields) =
                    lower_equality_param_filter_joins(
                        graph,
                        predicate,
                        source,
                        root_source,
                        request,
                    )?;
                graph = joined;
                fields.extend(introduced_route_fields.iter().cloned());
                available_route_fields.extend(introduced_route_fields);
                if !matches!(residual, PredicateExpr::True) {
                    let predicate = lower_predicate(&residual, source, root_source, request)?;
                    graph = graph.filter(predicate);
                }
            }
            LinearStep::Join { right, mode, on } => {
                if !matches!(mode, JoinMode::Inner | JoinMode::Semi) {
                    return Err(UnsupportedReason::Operator(
                        "join_via only lowers inner/semi joins".to_owned(),
                    ));
                }
                let lowered_right = lower_relation_input(right, resolved_sources, request)?;
                let (left_keys, right_keys) = lower_linear_join_key_pairs(
                    on,
                    &plan.root,
                    root_source,
                    right,
                    &lowered_right,
                    request,
                )?;
                if matches!(&plan.root, LinearRoot::Source { .. }) {
                    let mut unwrapped_left_keys = BTreeSet::new();
                    for left_key in &left_keys {
                        if source_field_is_nullable(root_source, left_key)
                            && unwrapped_left_keys.insert(left_key.clone())
                        {
                            graph = unwrap_nullable_join_key(
                                graph,
                                left_key.clone(),
                                source_field_nullable_depth(root_source, left_key),
                            );
                        }
                    }
                }
                let right_nullable_fields = lowered_right.nullable_fields.clone();
                let right_nullable_field_depths = lowered_right.nullable_field_depths.clone();
                let mut right_graph = lowered_right.graph;
                let mut unwrapped_right_keys = BTreeSet::new();
                for right_key in &right_keys {
                    if lowered_right.nullable_fields.contains(right_key)
                        && unwrapped_right_keys.insert(right_key.clone())
                    {
                        right_graph = unwrap_nullable_join_key(
                            right_graph,
                            right_key.clone(),
                            lowered_right
                                .nullable_field_depths
                                .get(right_key)
                                .copied()
                                .unwrap_or(1),
                        );
                    }
                }
                if *mode == JoinMode::Semi {
                    // Existence join: the right (authorization) side matters
                    // only per (join key, route fields) group — one qualifying
                    // derivation is as good as fifty, but the route fields must
                    // survive because one shared program serves every bound
                    // identity and result rows are routed by them. Project the
                    // right side down to exactly those fields and keep one
                    // maintained winner per group; rows within a group are
                    // identical post-projection, so losing one of several
                    // derivations produces no output delta and losing the last
                    // retracts the group. The join itself stays a plain inner
                    // join, so downstream field/route bookkeeping is unchanged.
                    let mut dedup_fields: Vec<String> = right_keys.clone();
                    for field in route_fields
                        .iter()
                        .filter(|field| lowered_right.fields.contains(*field))
                    {
                        if !dedup_fields.contains(field) {
                            dedup_fields.push(field.clone());
                        }
                    }
                    let projected = right_graph.project_fields(
                        dedup_fields
                            .iter()
                            .map(|field| ProjectField::named(field.clone()))
                            .collect::<Vec<_>>(),
                    );
                    let right_reduced = GraphBuilder::arg_max_by(
                        projected,
                        dedup_fields.clone(),
                        right_keys.clone(),
                    );
                    graph = GraphBuilder::join(graph, right_reduced, left_keys, right_keys);
                    // Downstream steps (Project, route retention) resolve
                    // right-prefixed fields through this; the right side now
                    // carries only the dedup fields.
                    let reduced_right_fields: BTreeSet<String> =
                        dedup_fields.iter().cloned().collect();
                    last_join_right = Some((
                        (**right).clone(),
                        right_nullable_fields
                            .intersection(&reduced_right_fields)
                            .cloned()
                            .collect(),
                        right_nullable_field_depths
                            .iter()
                            .filter(|(field, _)| reduced_right_fields.contains(*field))
                            .map(|(field, depth)| (field.clone(), *depth))
                            .collect(),
                        reduced_right_fields,
                    ));
                } else {
                    graph = GraphBuilder::join(graph, right_graph, left_keys, right_keys);
                    let right_fields = lowered_right.fields.clone();
                    last_join_right = Some((
                        (**right).clone(),
                        right_nullable_fields,
                        right_nullable_field_depths,
                        right_fields,
                    ));
                }
                let next_is_project =
                    matches!(plan.steps.get(step_index + 1), Some(LinearStep::Project(_)));
                if matches!(mode, JoinMode::Inner | JoinMode::Semi)
                    && matches!(&plan.root, LinearRoot::Source { .. })
                    && !next_is_project
                {
                    let introduced_route_fields = route_fields
                        .iter()
                        .filter(|field| lowered_right.fields.contains(*field))
                        .cloned()
                        .collect::<BTreeSet<_>>();
                    graph = graph.project_fields(project_left_source_fields_with_join_routes(
                        root_source,
                        &available_route_fields,
                        &introduced_route_fields,
                    ));
                    fields = source_fields(root_source).collect();
                    fields.extend(available_route_fields.iter().cloned());
                    fields.extend(introduced_route_fields.iter().cloned());
                    nullable_fields = source_nullable_fields(root_source);
                    nullable_field_depths = source_nullable_field_depths(root_source);
                    available_route_fields.extend(introduced_route_fields);
                    last_join_right = None;
                }
            }
            LinearStep::Project(columns) => {
                let mut unwrap_fields = BTreeSet::new();
                let project_fields = columns
                    .iter()
                    .map(|column| {
                        let field = lower_projection_field(
                            column,
                            plan,
                            root_source,
                            &fields,
                            last_join_right.as_ref(),
                            request,
                        )?;
                        unwrap_fields.extend(field.unwrap_before_project.iter().cloned());
                        Ok(field.project)
                    })
                    .collect::<Result<Vec<_>, UnsupportedReason>>()?;
                for field in unwrap_fields {
                    graph = graph.unwrap_nullable(field);
                }
                let mut project_fields = project_fields;
                let mut retained_route_fields = BTreeSet::new();
                let projected_outputs = project_fields
                    .iter()
                    .map(|field| field.output_name.clone())
                    .collect::<BTreeSet<_>>();
                match last_join_right.as_ref() {
                    Some((_, _, _, right_fields)) => {
                        for field in &available_route_fields {
                            if !projected_outputs.contains(field) {
                                project_fields
                                    .push(ProjectField::renamed(left_field(field), field.clone()));
                            }
                            retained_route_fields.insert(field.clone());
                        }
                        for field in route_fields
                            .iter()
                            .filter(|field| right_fields.contains(*field))
                        {
                            if !projected_outputs.contains(field) {
                                project_fields
                                    .push(ProjectField::renamed(right_field(field), field.clone()));
                            }
                            retained_route_fields.insert(field.clone());
                        }
                    }
                    None => {
                        for field in &available_route_fields {
                            if !projected_outputs.contains(field) {
                                project_fields.push(ProjectField::named(field.clone()));
                            }
                            retained_route_fields.insert(field.clone());
                        }
                    }
                }
                graph = graph.project_fields(project_fields);
                fields = columns
                    .iter()
                    .map(|column| column.output.name.clone())
                    .collect();
                fields.extend(retained_route_fields.iter().cloned());
                nullable_fields = BTreeSet::new();
                nullable_field_depths = BTreeMap::new();
                available_route_fields = retained_route_fields;
                last_join_right = None;
            }
            LinearStep::OrderBy(keys) => {
                last_join_right = None;
                pending_order = Some(keys.clone());
            }
            LinearStep::Slice {
                partition_by,
                limit,
                offset,
                tie_breaker,
                ..
            } => {
                last_join_right = None;
                let order = pending_order.take().unwrap_or_default();
                graph = lower_window(
                    graph,
                    &order,
                    partition_by,
                    *limit,
                    *offset,
                    tie_breaker,
                    plan,
                    root_source,
                    request,
                )?;
            }
            LinearStep::Aggregate { group_by, outputs } => {
                last_join_right = None;
                if pending_order.take().is_some() {
                    return Err(UnsupportedReason::Operator(
                        "order-by before aggregate is not lowered yet".to_owned(),
                    ));
                }
                let lowered =
                    lower_aggregate(graph, group_by, outputs, plan, root_source, request)?;
                graph = lowered.graph;
                fields = lowered.fields;
                nullable_fields = BTreeSet::new();
                nullable_field_depths = BTreeMap::new();
                available_route_fields = BTreeSet::new();
            }
        }
    }

    if let Some(order) = pending_order {
        graph = lower_window(
            graph,
            &order,
            &[],
            None,
            0,
            &[NormalizedValueRef::RowId(RowIdRef::Source(
                plan.root
                    .source()
                    .ok_or_else(|| {
                        UnsupportedReason::Operator("order fallback must be a source".to_owned())
                    })?
                    .clone(),
            ))],
            plan,
            root_source,
            request,
        )?;
    }

    Ok(LoweredRelationInput {
        graph,
        root_source: Some(root_source.clone()),
        fields,
        nullable_fields,
        nullable_field_depths,
    })
}

fn value_source_descriptor(columns: &[ValueSourceColumn]) -> RecordDescriptor {
    RecordDescriptor::new(
        columns
            .iter()
            .map(|column| (column.name.clone(), column.ty.value_type())),
    )
}

fn binding_descriptor_params_with_user_params(
    request: &QueryProgramRequest,
    additional_user_params: impl IntoIterator<Item = (String, ColumnType)>,
) -> Result<Vec<(String, ColumnType)>, UnsupportedReason> {
    let domain = parameter_domain_for_request(request)?;
    let mut user_params = request.input.binding.extra_user_params.clone();
    user_params.extend(domain.user_params);
    user_params.extend(additional_user_params);
    Ok(user_params
        .into_iter()
        .chain(
            domain
                .claim_params
                .into_iter()
                .map(|(name, param)| (name, param.ty)),
        )
        .collect())
}

fn binding_descriptor_params(
    request: &QueryProgramRequest,
) -> Result<Vec<(String, ColumnType)>, UnsupportedReason> {
    binding_descriptor_params_with_user_params(request, [])
}

fn binding_source_descriptor_with_user_params(
    request: &QueryProgramRequest,
    additional_user_params: impl IntoIterator<Item = (String, ColumnType)>,
) -> Result<RecordDescriptor, UnsupportedReason> {
    Ok(RecordDescriptor::new(
        binding_descriptor_params_with_user_params(request, additional_user_params)?
            .into_iter()
            .map(|(name, column_type)| (name, column_type.value_type())),
    ))
}

fn lower_value_source(
    shape: &str,
    columns: &[ValueSourceColumn],
    mode: &ValueSourceMode,
    request: &QueryProgramRequest,
) -> Result<GraphBuilder, UnsupportedReason> {
    let descriptor = value_source_descriptor(columns);
    match mode {
        ValueSourceMode::Binding => {
            let domain = parameter_domain_for_request(request)?;
            let params = binding_descriptor_params(request)?;
            for column in columns {
                match &column.value {
                    NormalizedValueRef::Param(param) => {
                        let Some((_, existing)) = params.iter().find(|(name, _)| name == param)
                        else {
                            return Err(UnsupportedReason::Operator(format!(
                                "binding parameter '{param}' is not part of the program parameter domain"
                            )));
                        };
                        if *existing != column.ty {
                            return Err(UnsupportedReason::Operator(format!(
                                "binding parameter '{param}' has conflicting value-source types"
                            )));
                        }
                    }
                    NormalizedValueRef::Claim(path) => {
                        let param = claim_param_field(path);
                        let Some(existing) = domain.claim_params.get(&param) else {
                            return Err(UnsupportedReason::Operator(format!(
                                "claim parameter '{param}' is not part of the program parameter domain"
                            )));
                        };
                        if existing.ty != column.ty {
                            return Err(UnsupportedReason::Operator(format!(
                                "claim parameter '{param}' has conflicting value-source types"
                            )));
                        }
                    }
                    NormalizedValueRef::Literal(_) => {}
                    _ => {
                        return Err(UnsupportedReason::Operator(
                            "binding value source columns must reference binding params, claims, or literals"
                                .to_owned(),
                        ));
                    }
                }
            }
            if params.is_empty() {
                let row = columns
                    .iter()
                    .map(|column| lower_value_source_column(column, request))
                    .collect::<Result<Vec<_>, _>>()?;
                return GraphBuilder::values(descriptor, [row]).map_err(|err| {
                    UnsupportedReason::Operator(format!(
                        "binding value source could not encode constant row: {err}"
                    ))
                });
            }
            let input_descriptor = RecordDescriptor::new(
                params
                    .iter()
                    .map(|(name, column_type)| (name.clone(), column_type.value_type())),
            );
            let projected = columns
                .iter()
                .map(|column| column.name.clone())
                .collect::<BTreeSet<_>>();
            let source_user_params = columns
                .iter()
                .filter_map(|column| match &column.value {
                    NormalizedValueRef::Param(param) if domain.user_params.contains_key(param) => {
                        Some(param.clone())
                    }
                    NormalizedValueRef::Claim(path) => {
                        let param = claim_param_field(path);
                        domain.claim_params.contains_key(&param).then_some(param)
                    }
                    _ => None,
                })
                .collect::<BTreeSet<_>>();
            let retained_routes = domain
                .user_params
                .keys()
                .filter(|param| source_user_params.contains(*param))
                .filter_map(|param| {
                    let route_field = route_param_field(param);
                    (!projected.contains(&route_field))
                        .then(|| ProjectField::renamed(param.clone(), route_field))
                })
                .chain(domain.claim_params.keys().filter_map(|param| {
                    source_user_params
                        .contains(param)
                        .then(|| (!projected.contains(param)).then(|| ProjectField::named(param)))
                        .flatten()
                }))
                .collect::<Vec<_>>();
            Ok(
                GraphBuilder::binding_source(shape.to_owned(), input_descriptor).project_fields(
                    columns
                        .iter()
                        .map(|column| {
                            Ok(match &column.value {
                                NormalizedValueRef::Param(param) => {
                                    ProjectField::renamed(param.clone(), column.name.clone())
                                }
                                NormalizedValueRef::Claim(path) => ProjectField::renamed(
                                    claim_param_field(path),
                                    column.name.clone(),
                                ),
                                NormalizedValueRef::Literal(bytes) => {
                                    let value =
                                        postcard::from_bytes::<Value>(bytes).map_err(|err| {
                                            UnsupportedReason::Operator(format!(
                                                "literal value could not be decoded: {err}"
                                            ))
                                        })?;
                                    ProjectField::literal(column.name.clone(), value)
                                }
                                _ => unreachable!("checked above"),
                            })
                        })
                        .collect::<Result<Vec<_>, _>>()?
                        .into_iter()
                        .chain(retained_routes),
                ),
            )
        }
        ValueSourceMode::Inline => {
            let row = columns
                .iter()
                .map(|column| lower_value_source_column(column, request))
                .collect::<Result<Vec<_>, _>>()?;
            GraphBuilder::values(descriptor, [row]).map_err(|err| {
                UnsupportedReason::Operator(format!("inline value source could not encode: {err}"))
            })
        }
    }
}

fn lower_value_source_column(
    column: &ValueSourceColumn,
    request: &QueryProgramRequest,
) -> Result<Value, UnsupportedReason> {
    match &column.value {
        NormalizedValueRef::Param(name) => request
            .input
            .binding
            .values
            .get(name)
            .cloned()
            .ok_or_else(|| {
                UnsupportedReason::Operator(format!("binding parameter '{name}' is not bound"))
            }),
        NormalizedValueRef::Literal(bytes) => postcard::from_bytes::<Value>(bytes).map_err(|err| {
            UnsupportedReason::Operator(format!("literal value could not be decoded: {err}"))
        }),
        NormalizedValueRef::Claim(path) => claim_value(path, &request.policy),
        _ => Err(UnsupportedReason::Operator(
            "value source columns must be binding params, literals, or claims".to_owned(),
        )),
    }
}

fn lower_path_key_pair(
    predicate: &PredicateExpr,
    parent_source_id: &SourceId,
    parent_source: &ResolvedSource,
    child_source_id: &SourceId,
    child_source: &ResolvedSource,
    request: &QueryProgramRequest,
) -> Result<(String, String), UnsupportedReason> {
    lower_bidirectional_key_pair(
        predicate,
        "correlated path projection only lowers equality correlations",
        "correlated path projection correlation must compare parent and child fields",
        |value| lower_join_key_ref(value, parent_source_id, parent_source, request),
        |value| lower_join_key_ref(value, child_source_id, child_source, request),
    )
}

fn lower_join_key_pair(
    predicate: &PredicateExpr,
    left_source_id: &SourceId,
    left_source: &ResolvedSource,
    right_source_id: &SourceId,
    right_source: &ResolvedSource,
    request: &QueryProgramRequest,
) -> Result<(String, String), UnsupportedReason> {
    lower_bidirectional_key_pair(
        predicate,
        "join_via only lowers equality join predicates",
        "join_via join predicate must compare the root row id to one join source field",
        |value| lower_join_key_ref(value, left_source_id, left_source, request),
        |value| lower_join_key_ref(value, right_source_id, right_source, request),
    )
}

fn lower_join_key_pairs(
    predicate: &PredicateExpr,
    left_source_id: &SourceId,
    left_source: &ResolvedSource,
    right_source_id: &SourceId,
    right_source: &ResolvedSource,
    request: &QueryProgramRequest,
) -> Result<(Vec<String>, Vec<String>), UnsupportedReason> {
    let pairs = match predicate {
        PredicateExpr::And(predicates) => predicates
            .iter()
            .map(|predicate| {
                lower_join_key_pair(
                    predicate,
                    left_source_id,
                    left_source,
                    right_source_id,
                    right_source,
                    request,
                )
            })
            .collect::<Result<Vec<_>, _>>()?,
        _ => vec![lower_join_key_pair(
            predicate,
            left_source_id,
            left_source,
            right_source_id,
            right_source,
            request,
        )?],
    };
    if pairs.is_empty() {
        return Err(UnsupportedReason::Operator(
            "join_via requires at least one equality join predicate".to_owned(),
        ));
    }
    Ok(pairs.into_iter().unzip())
}

fn lower_linear_join_key_pair(
    predicate: &PredicateExpr,
    left_root: &LinearRoot,
    left_source: &ResolvedSource,
    right_plan: &RelationInputPlan,
    right_output: &LoweredRelationInput,
    request: &QueryProgramRequest,
) -> Result<(String, String), UnsupportedReason> {
    lower_bidirectional_key_pair(
        predicate,
        "join_via only lowers equality join predicates",
        "join_via join predicate must compare left root and right relation fields",
        |value| lower_linear_root_key_ref(value, left_root, left_source, request),
        |value| lower_relation_key_ref(value, right_plan, right_output, request),
    )
}

fn lower_linear_join_key_pairs(
    predicate: &PredicateExpr,
    left_root: &LinearRoot,
    left_source: &ResolvedSource,
    right_plan: &RelationInputPlan,
    right_output: &LoweredRelationInput,
    request: &QueryProgramRequest,
) -> Result<(Vec<String>, Vec<String>), UnsupportedReason> {
    let pairs = match predicate {
        PredicateExpr::And(predicates) => predicates
            .iter()
            .map(|predicate| {
                lower_linear_join_key_pair(
                    predicate,
                    left_root,
                    left_source,
                    right_plan,
                    right_output,
                    request,
                )
            })
            .collect::<Result<Vec<_>, _>>()?,
        _ => vec![lower_linear_join_key_pair(
            predicate,
            left_root,
            left_source,
            right_plan,
            right_output,
            request,
        )?],
    };
    if pairs.is_empty() {
        return Err(UnsupportedReason::Operator(
            "join_via requires at least one equality join predicate".to_owned(),
        ));
    }
    Ok(pairs.into_iter().unzip())
}

fn lower_root_to_relation_key_pair(
    predicate: &PredicateExpr,
    root_source: &ResolvedSource,
    right_plan: &RelationInputPlan,
    right_output: &LoweredRelationInput,
    request: &QueryProgramRequest,
) -> Result<(String, String), UnsupportedReason> {
    lower_bidirectional_key_pair(
        predicate,
        "join contribution membership only lowers equality predicates",
        "join contribution membership must compare root fields to relation output fields",
        |value| lower_join_key_ref(value, &root_source.row_shape.source, root_source, request),
        |value| lower_relation_key_ref(value, right_plan, right_output, request),
    )
}

fn lower_root_to_relation_key_pairs(
    predicate: &PredicateExpr,
    root_source: &ResolvedSource,
    right_plan: &RelationInputPlan,
    right_output: &LoweredRelationInput,
    request: &QueryProgramRequest,
) -> Result<(Vec<String>, Vec<String>), UnsupportedReason> {
    let pairs = match predicate {
        PredicateExpr::And(predicates) => predicates
            .iter()
            .map(|predicate| {
                lower_root_to_relation_key_pair(
                    predicate,
                    root_source,
                    right_plan,
                    right_output,
                    request,
                )
            })
            .collect::<Result<Vec<_>, _>>()?,
        _ => vec![lower_root_to_relation_key_pair(
            predicate,
            root_source,
            right_plan,
            right_output,
            request,
        )?],
    };
    if pairs.is_empty() {
        return Err(UnsupportedReason::Operator(
            "join contribution membership requires at least one equality predicate".to_owned(),
        ));
    }
    Ok(pairs.into_iter().unzip())
}

fn lower_bidirectional_key_pair(
    predicate: &PredicateExpr,
    non_equality_message: &str,
    mismatch_message: &str,
    left_resolver: impl Fn(&NormalizedValueRef) -> Result<String, UnsupportedReason>,
    right_resolver: impl Fn(&NormalizedValueRef) -> Result<String, UnsupportedReason>,
) -> Result<(String, String), UnsupportedReason> {
    let PredicateExpr::Compare {
        left,
        op: ComparisonOp::Eq,
        right,
    } = predicate
    else {
        return Err(UnsupportedReason::Operator(non_equality_message.to_owned()));
    };

    match (left_resolver(left), right_resolver(right)) {
        (Ok(left_key), Ok(right_key)) => Ok((left_key, right_key)),
        (direct_left, direct_right) => match (left_resolver(right), right_resolver(left)) {
            (Ok(left_key), Ok(right_key)) => Ok((left_key, right_key)),
            (swapped_left, swapped_right) => Err(UnsupportedReason::Operator(format!(
                "{mismatch_message}; direct errors: {}, {}; swapped errors: {}, {}",
                key_pair_error(direct_left),
                key_pair_error(direct_right),
                key_pair_error(swapped_left),
                key_pair_error(swapped_right),
            ))),
        },
    }
}

fn key_pair_error(result: Result<String, UnsupportedReason>) -> String {
    match result {
        Ok(field) => format!("accepted {field:?}"),
        Err(reason) => format!("{reason:?}"),
    }
}

fn lower_relation_key_ref(
    value: &NormalizedValueRef,
    plan: &RelationInputPlan,
    output: &LoweredRelationInput,
    request: &QueryProgramRequest,
) -> Result<String, UnsupportedReason> {
    match plan {
        RelationInputPlan::Linear(linear) => {
            if linear_ends_in_projection(linear)
                && let Ok(field) = lower_named_relation_field(value, &output.fields)
            {
                return Ok(field);
            }
            if let Some(source) = &output.root_source {
                if let Some(source_id) = linear.root.source() {
                    if let Ok(key) = lower_join_key_ref(value, source_id, source, request) {
                        return Ok(key);
                    }
                }
            }
            lower_named_relation_field(value, &output.fields)
        }
        RelationInputPlan::Union(_) => lower_named_relation_field(value, &output.fields),
        RelationInputPlan::Recursive(_) => lower_named_relation_field(value, &output.fields),
    }
}

fn linear_ends_in_projection(linear: &LinearCurrentRoot) -> bool {
    matches!(linear.steps.last(), Some(LinearStep::Project(_)))
}

fn lower_named_relation_field(
    value: &NormalizedValueRef,
    fields: &BTreeSet<String>,
) -> Result<String, UnsupportedReason> {
    let field = match value {
        NormalizedValueRef::FrontierColumn { field, .. } => field,
        NormalizedValueRef::Param(param) => param,
        NormalizedValueRef::SourceField { field, .. } => field,
        NormalizedValueRef::RowId(RowIdRef::Frontier(_)) => "row_uuid",
        NormalizedValueRef::RowId(RowIdRef::Source(_))
        | NormalizedValueRef::Claim(_)
        | NormalizedValueRef::Provenance { .. }
        | NormalizedValueRef::Literal(_) => {
            return Err(UnsupportedReason::Operator(
                "join relation key must be an output field".to_owned(),
            ));
        }
    };
    if fields.contains(field) {
        Ok(field.to_owned())
    } else {
        Err(UnsupportedReason::Operator(format!(
            "join relation does not output field '{field}'"
        )))
    }
}

fn lower_linear_root_key_ref(
    value: &NormalizedValueRef,
    root: &LinearRoot,
    source: &ResolvedSource,
    request: &QueryProgramRequest,
) -> Result<String, UnsupportedReason> {
    match root {
        LinearRoot::Source {
            source: source_id, ..
        } => lower_join_key_ref(value, source_id, source, request),
        LinearRoot::Frontier { frontier, columns } => match value {
            NormalizedValueRef::FrontierColumn {
                frontier: value_frontier,
                field,
            } if value_frontier == frontier
                && columns.iter().any(|column| column.name == *field) =>
            {
                Ok(field.clone())
            }
            NormalizedValueRef::RowId(RowIdRef::Frontier(value_frontier))
                if value_frontier == frontier
                    && columns.iter().any(|column| column.name == "row_uuid") =>
            {
                Ok("row_uuid".to_owned())
            }
            _ => Err(UnsupportedReason::Operator(
                "join left key must be a frontier column".to_owned(),
            )),
        },
        LinearRoot::Value { columns, .. } => match value {
            NormalizedValueRef::Param(name)
            | NormalizedValueRef::FrontierColumn { field: name, .. }
                if columns.iter().any(|column| column.name == *name) =>
            {
                Ok(name.clone())
            }
            _ => Err(UnsupportedReason::Operator(
                "join left key must be a value-source column".to_owned(),
            )),
        },
    }
}

fn lower_projection_field(
    column: &RowProjection,
    plan: &LinearCurrentRoot,
    source: &ResolvedSource,
    fields: &BTreeSet<String>,
    last_join_right: Option<&(
        RelationInputPlan,
        BTreeSet<String>,
        BTreeMap<String, usize>,
        BTreeSet<String>,
    )>,
    request: &QueryProgramRequest,
) -> Result<ProjectionFieldPlan, UnsupportedReason> {
    let mut unwrap_before_project = BTreeSet::new();
    let project = match lower_projection_source(
        &column.value,
        plan,
        source,
        fields,
        last_join_right,
        request,
    )? {
        ProjectionSource::Field { field, nullable } => {
            if nullable && !matches!(column.output.ty.value_type(), ValueType::Nullable(_)) {
                unwrap_before_project.insert(field.clone());
            }
            ProjectField::renamed(field, column.output.name.clone())
        }
        ProjectionSource::Literal(value) => {
            ProjectField::literal(column.output.name.clone(), value)
        }
    };
    Ok(ProjectionFieldPlan {
        project,
        unwrap_before_project,
    })
}

#[derive(Clone, Debug)]
enum ProjectionSource {
    Field { field: String, nullable: bool },
    Literal(LiteralValue),
}

#[derive(Clone, Debug)]
struct ProjectionFieldPlan {
    project: ProjectField,
    unwrap_before_project: BTreeSet<String>,
}

fn lower_projection_source(
    value: &NormalizedValueRef,
    plan: &LinearCurrentRoot,
    source: &ResolvedSource,
    fields: &BTreeSet<String>,
    last_join_right: Option<&(
        RelationInputPlan,
        BTreeSet<String>,
        BTreeMap<String, usize>,
        BTreeSet<String>,
    )>,
    request: &QueryProgramRequest,
) -> Result<ProjectionSource, UnsupportedReason> {
    if let Ok(field) = lower_linear_root_key_ref(value, &plan.root, source, request) {
        let nullable = matches!(plan.root, LinearRoot::Source { .. })
            && source_field_is_nullable(source, &field);
        return Ok(ProjectionSource::Field {
            field: match last_join_right {
                Some(_) => left_field(&field),
                None => field,
            },
            nullable,
        });
    }

    if let NormalizedValueRef::Param(param) = value
        && fields.contains(param)
    {
        return Ok(ProjectionSource::Field {
            field: match last_join_right {
                Some(_) => left_field(param),
                None => param.clone(),
            },
            nullable: false,
        });
    }
    if let Some((right, nullable_fields, _, _)) = last_join_right {
        if let Some(field) = lower_relation_projection_ref(value, right, request)? {
            let nullable = nullable_fields.contains(&field);
            return Ok(ProjectionSource::Field {
                field: right_field(&field),
                nullable,
            });
        }
    }

    match lower_literal_projection_value(value, request)? {
        Some(value) => Ok(ProjectionSource::Literal(value)),
        None => Err(UnsupportedReason::Operator(
            "project value must reference the current root, last join input, or a literal"
                .to_owned(),
        )),
    }
}

fn lower_relation_projection_ref(
    value: &NormalizedValueRef,
    plan: &RelationInputPlan,
    _request: &QueryProgramRequest,
) -> Result<Option<String>, UnsupportedReason> {
    match plan {
        RelationInputPlan::Linear(linear) => {
            if matches!(linear.root, LinearRoot::Source { .. }) {
                if let Some(source_id) = linear.root.source() {
                    match value {
                        NormalizedValueRef::SourceField {
                            source: value_source,
                            field,
                        } if value_source == source_id => {
                            return Ok(Some(user_column_field(field)));
                        }
                        NormalizedValueRef::RowId(RowIdRef::Source(value_source))
                            if value_source == source_id =>
                        {
                            return Ok(Some("row_uuid".to_owned()));
                        }
                        _ => {}
                    }
                }
            }
            match value {
                NormalizedValueRef::Param(param)
                | NormalizedValueRef::FrontierColumn { field: param, .. } => {
                    Ok(Some(param.clone()))
                }
                NormalizedValueRef::Literal(_) => Ok(None),
                NormalizedValueRef::Claim(_)
                | NormalizedValueRef::SourceField { .. }
                | NormalizedValueRef::RowId(_)
                | NormalizedValueRef::Provenance { .. } => Ok(None),
            }
        }
        RelationInputPlan::Recursive(relation) => match value {
            NormalizedValueRef::FrontierColumn { frontier, field }
                if frontier == &relation.frontier =>
            {
                Ok(Some(field.clone()))
            }
            NormalizedValueRef::Param(param) => Ok(Some(param.clone())),
            NormalizedValueRef::Literal(_) => Ok(None),
            NormalizedValueRef::Claim(_)
            | NormalizedValueRef::SourceField { .. }
            | NormalizedValueRef::RowId(_)
            | NormalizedValueRef::Provenance { .. }
            | NormalizedValueRef::FrontierColumn { .. } => Ok(None),
        },
        RelationInputPlan::Union(_) => match value {
            NormalizedValueRef::Param(param)
            | NormalizedValueRef::FrontierColumn { field: param, .. }
            | NormalizedValueRef::SourceField { field: param, .. } => Ok(Some(param.clone())),
            NormalizedValueRef::RowId(_) => Ok(Some("row_uuid".to_owned())),
            NormalizedValueRef::Literal(_)
            | NormalizedValueRef::Claim(_)
            | NormalizedValueRef::Provenance { .. } => Ok(None),
        },
    }
}

fn lower_equality_param_filter_joins(
    mut graph: GraphBuilder,
    predicate: &PredicateExpr,
    source_id: &SourceId,
    source: &ResolvedSource,
    request: &QueryProgramRequest,
) -> Result<(GraphBuilder, PredicateExpr, BTreeSet<String>), UnsupportedReason> {
    let predicates = match predicate {
        PredicateExpr::And(predicates) => predicates.as_slice(),
        _ => std::slice::from_ref(predicate),
    };
    let mut residual = Vec::new();
    let mut retained_route_fields = BTreeSet::<String>::new();
    for predicate in predicates {
        let Some(join) = equality_param_join(predicate, source_id, source)? else {
            residual.push(predicate.clone());
            continue;
        };
        let Some(binding_source_shape) = &request.input.binding.source_shape else {
            residual.push(predicate.clone());
            continue;
        };
        let domain = parameter_domain_for_request(request)?;
        let is_claim_param = domain.claim_params.contains_key(&join.param);
        let binding_descriptor = if is_claim_param {
            binding_source_descriptor_with_user_params(request, [])?
        } else {
            binding_source_descriptor_with_user_params(
                request,
                [(
                    join.param.clone(),
                    column_type_from_value_type(&join.value_type),
                )],
            )?
        };
        let binding =
            GraphBuilder::binding_source(binding_source_shape.clone(), binding_descriptor);
        let route_field = if is_claim_param {
            join.param.clone()
        } else {
            route_param_field(&join.param)
        };
        let mut projection = project_source_fields_from_prefix(source, LEFT_JOIN_PREFIX);
        projection.extend(
            retained_route_fields
                .iter()
                .map(|field| ProjectField::renamed(left_field(&field), field.clone())),
        );
        projection.push(ProjectField::renamed(
            right_field(&join.param),
            route_field.clone(),
        ));
        if join.nullable {
            graph = graph.unwrap_nullable(join.field.clone());
        }
        graph = GraphBuilder::join(graph, binding, [join.field], [join.param])
            .project_fields(projection);
        retained_route_fields.insert(route_field);
    }
    let residual = match residual.len() {
        0 => PredicateExpr::True,
        1 => residual.pop().expect("one residual predicate"),
        _ => PredicateExpr::And(residual),
    };
    Ok((graph, residual, retained_route_fields))
}

struct EqualityParamJoin {
    field: String,
    param: String,
    value_type: ValueType,
    nullable: bool,
}

fn equality_param_join(
    predicate: &PredicateExpr,
    source_id: &SourceId,
    source: &ResolvedSource,
) -> Result<Option<EqualityParamJoin>, UnsupportedReason> {
    let PredicateExpr::Compare {
        left,
        op: ComparisonOp::Eq,
        right,
    } = predicate
    else {
        return Ok(None);
    };
    if let (Some((field, value_type, nullable)), NormalizedValueRef::Param(param)) =
        (source_join_field(left, source_id, source)?, right)
    {
        return Ok(Some(EqualityParamJoin {
            field,
            param: param.clone(),
            value_type,
            nullable,
        }));
    }
    match (left, source_join_field(right, source_id, source)?) {
        (NormalizedValueRef::Param(param), Some((field, value_type, nullable))) => {
            Ok(Some(EqualityParamJoin {
                field,
                param: param.clone(),
                value_type,
                nullable,
            }))
        }
        _ => Ok(None),
    }
}

fn source_join_field(
    value: &NormalizedValueRef,
    source_id: &SourceId,
    source: &ResolvedSource,
) -> Result<Option<(String, ValueType, bool)>, UnsupportedReason> {
    let field = match value {
        NormalizedValueRef::SourceField {
            source: value_source,
            field,
        } if value_source == source_id => {
            let resolved = require_source_field(source, &user_column_field(field))
                .or_else(|_| require_source_field(source, field));
            resolved?
        }
        NormalizedValueRef::RowId(RowIdRef::Source(value_source)) if value_source == source_id => {
            require_source_field(source, &source.row_shape.row_uuid_field)?
        }
        _ => return Ok(None),
    };
    let Some(value_type) = source_field_type(source, &field).cloned() else {
        return Err(UnsupportedReason::Runtime(format!(
            "source field {field:?} is missing from resolved descriptor"
        )));
    };
    let (value_type, nullable) = match value_type {
        ValueType::Nullable(inner) => ((*inner).clone(), true),
        value_type => (value_type, false),
    };
    Ok(Some((field, value_type, nullable)))
}

fn lower_literal_projection_value(
    value: &NormalizedValueRef,
    request: &QueryProgramRequest,
) -> Result<Option<LiteralValue>, UnsupportedReason> {
    match value {
        NormalizedValueRef::Literal(bytes) => {
            let value = postcard::from_bytes::<Value>(bytes).map_err(|err| {
                UnsupportedReason::Operator(format!("literal value could not be decoded: {err}"))
            })?;
            Ok(Some(value.into()))
        }
        NormalizedValueRef::Param(name) => {
            let value = request.input.binding.values.get(name).ok_or_else(|| {
                UnsupportedReason::Operator(format!("binding parameter '{name}' is not bound"))
            })?;
            Ok(Some(value.clone().into()))
        }
        _ => Ok(None),
    }
}

fn lower_join_key_ref(
    value: &NormalizedValueRef,
    source_id: &SourceId,
    source: &ResolvedSource,
    request: &QueryProgramRequest,
) -> Result<String, UnsupportedReason> {
    if let NormalizedValueRef::SourceField {
        source: value_source,
        field,
    } = value
        && value_source == source_id
        && field == "id"
    {
        return require_source_field(source, &source.row_shape.row_uuid_field);
    }
    match lower_value_ref(value, source_id, source, request)? {
        LoweredValueRef::Field(field) => Ok(field),
        LoweredValueRef::Literal(_) => Err(UnsupportedReason::Operator(
            "join_via join keys must be source fields".to_owned(),
        )),
    }
}

fn source_field_is_nullable(source: &ResolvedSource, field: &str) -> bool {
    source_field_nullable_depth(source, field) > 0
}

fn source_field_nullable_depth(source: &ResolvedSource, field: &str) -> usize {
    let mut depth = 0;
    if source_field_type(source, field)
        .is_some_and(|field_type| matches!(field_type, ValueType::Nullable(_)))
    {
        depth += 1;
    }
    let logical_field = field.strip_prefix(USER_COLUMN_PREFIX).unwrap_or(field);
    if source.table_schema.columns.iter().any(|column| {
        column.name == logical_field && matches!(column.column_type, ColumnType::Nullable(_))
    }) {
        depth += 1;
    }
    depth
}

fn source_field_type<'a>(source: &'a ResolvedSource, field: &str) -> Option<&'a ValueType> {
    source
        .row_shape
        .descriptor
        .field_index(field)
        .or_else(|| {
            let user_field = user_column_field(field);
            source.row_shape.descriptor.field_index(&user_field)
        })
        .and_then(|index| source.row_shape.descriptor.fields().get(index))
        .map(|field| &field.value_type)
}

fn project_left_source_fields_with_join_routes(
    source: &ResolvedSource,
    existing_route_fields: &BTreeSet<String>,
    introduced_route_fields: &BTreeSet<String>,
) -> Vec<ProjectField> {
    let mut fields = project_source_fields_from_prefix(source, LEFT_JOIN_PREFIX);
    fields.extend(
        existing_route_fields
            .iter()
            .map(|field| ProjectField::renamed(left_field(&field), field.clone())),
    );
    fields.extend(
        introduced_route_fields
            .iter()
            .map(|field| ProjectField::renamed(right_field(&field), field.clone())),
    );
    fields
}

fn lower_window(
    graph: GraphBuilder,
    order: &[OrderKey],
    partition_by: &[NormalizedValueRef],
    limit: Option<u32>,
    offset: u32,
    tie_breaker: &[NormalizedValueRef],
    plan: &LinearCurrentRoot,
    source: &ResolvedSource,
    request: &QueryProgramRequest,
) -> Result<GraphBuilder, UnsupportedReason> {
    let group_cols = partition_by
        .iter()
        .map(|value| lower_field_ref(value, plan, source, request, "slice partition key"))
        .collect::<Result<Vec<_>, _>>()?;
    let tie_cols = if tie_breaker.is_empty() {
        vec![source.row_shape.row_uuid_field.clone()]
    } else {
        tie_breaker
            .iter()
            .map(|value| lower_field_ref(value, plan, source, request, "slice tie-breaker"))
            .collect::<Result<Vec<_>, _>>()?
    };
    let top_by_limit = match limit {
        Some(limit) => TopByLimit::Finite(u64::from(limit)),
        None => TopByLimit::Unbounded,
    };

    if order.is_empty() {
        if offset == 0 && limit == Some(1) {
            return Ok(GraphBuilder::arg_min_by(graph, group_cols, tie_cols));
        }
        if offset == 0 && limit.is_none() {
            return Ok(graph);
        }
        return Ok(GraphBuilder::top_by(
            graph,
            group_cols,
            Vec::new(),
            tie_cols,
            u64::from(offset),
            top_by_limit,
        ));
    }

    let order_cols = order
        .iter()
        .map(|key| lower_order_key(key, plan, source, request))
        .collect::<Result<Vec<_>, _>>()?;
    Ok(GraphBuilder::top_by(
        graph,
        group_cols,
        order_cols,
        tie_cols,
        u64::from(offset),
        top_by_limit,
    ))
}

fn lower_aggregate(
    mut graph: GraphBuilder,
    group_by: &[NormalizedValueRef],
    outputs: &[AggregateExpr],
    plan: &LinearCurrentRoot,
    source: &ResolvedSource,
    request: &QueryProgramRequest,
) -> Result<LoweredRelationInput, UnsupportedReason> {
    let group_cols = group_by
        .iter()
        .map(|value| lower_field_ref(value, plan, source, request, "aggregate group key"))
        .collect::<Result<Vec<_>, _>>()?;
    let aggregates = outputs
        .iter()
        .map(|aggregate| lower_aggregate_expr(aggregate, plan, source, request))
        .collect::<Result<Vec<_>, _>>()?;
    let mut unwrap_fields = BTreeSet::new();
    for field in &group_cols {
        if source_field_is_nullable(source, field) {
            unwrap_fields.insert(field.clone());
        }
    }
    for aggregate in outputs {
        let Some(input) = &aggregate.input else {
            continue;
        };
        let field = lower_field_ref(input, plan, source, request, "aggregate input")?;
        if source_field_is_nullable(source, &field) {
            unwrap_fields.insert(field);
        }
    }
    for field in unwrap_fields {
        graph = graph.unwrap_nullable(field);
    }
    let mut fields = group_cols.iter().cloned().collect::<BTreeSet<_>>();
    fields.extend(
        outputs
            .iter()
            .map(|aggregate| logical_user_column(&aggregate.output.name).to_owned()),
    );
    Ok(LoweredRelationInput {
        graph: GraphBuilder::aggregate(graph, group_cols, aggregates),
        root_source: Some(source.clone()),
        fields,
        nullable_fields: BTreeSet::new(),
        nullable_field_depths: BTreeMap::new(),
    })
}

fn lower_aggregate_expr(
    aggregate: &AggregateExpr,
    plan: &LinearCurrentRoot,
    source: &ResolvedSource,
    request: &QueryProgramRequest,
) -> Result<GrooveAggregateExpr, UnsupportedReason> {
    let expression = aggregate
        .input
        .as_ref()
        .map(|value| {
            lower_field_ref(value, plan, source, request, "aggregate input")
                .map(GroovePlanExpr::field)
        })
        .transpose()?;
    Ok(GrooveAggregateExpr {
        function: match aggregate.function {
            AggregateFunction::Count => GrooveAggregateFunction::Count,
            AggregateFunction::Sum => GrooveAggregateFunction::Sum,
            AggregateFunction::Avg => GrooveAggregateFunction::Avg,
            AggregateFunction::Min => GrooveAggregateFunction::Min,
            AggregateFunction::Max => GrooveAggregateFunction::Max,
        },
        expression,
        distinct: false,
        output_name: Some(logical_user_column(&aggregate.output.name).to_owned()),
    })
}

fn lower_order_key(
    key: &OrderKey,
    plan: &LinearCurrentRoot,
    source: &ResolvedSource,
    request: &QueryProgramRequest,
) -> Result<TopByOrder, UnsupportedReason> {
    let field = lower_field_ref(&key.value, plan, source, request, "order key")?;
    Ok(match key.direction {
        SortDirection::Asc => TopByOrder::asc(field),
        SortDirection::Desc => TopByOrder::desc(field),
    })
}

fn lower_predicate(
    predicate: &PredicateExpr,
    source_id: &SourceId,
    source: &ResolvedSource,
    request: &QueryProgramRequest,
) -> Result<GroovePredicateExpr, UnsupportedReason> {
    let lowered = match lower_predicate_inner(predicate, source_id, source, request) {
        Err(reason) if is_unbound_claim_reason(&reason) => constant_predicate(false),
        other => other?,
    };
    Ok(lowered.canonicalize())
}

fn lower_predicate_inner(
    predicate: &PredicateExpr,
    source_id: &SourceId,
    source: &ResolvedSource,
    request: &QueryProgramRequest,
) -> Result<GroovePredicateExpr, UnsupportedReason> {
    Ok(match predicate {
        PredicateExpr::True => GroovePredicateExpr::And(Vec::new()),
        PredicateExpr::False => GroovePredicateExpr::Or(Vec::new()),
        PredicateExpr::Compare { left, op, right } => {
            lower_compare(left, *op, right, source_id, source, request)?
        }
        PredicateExpr::In { value, options } => {
            let predicates = options
                .iter()
                .map(|option| {
                    lower_compare(value, ComparisonOp::Eq, option, source_id, source, request)
                })
                .collect::<Result<Vec<_>, _>>()?;
            GroovePredicateExpr::Or(predicates)
        }
        PredicateExpr::ArrayContains { value, needle } => {
            lower_contains(value, needle, source_id, source, request)?
        }
        PredicateExpr::TextContains { .. } => {
            return Err(UnsupportedReason::Operator(
                "text containment predicates are not lowered yet".to_owned(),
            ));
        }
        PredicateExpr::IsNull(value) => lower_null_test(value, true, source_id, source, request)?,
        PredicateExpr::IsNotNull(value) => {
            lower_null_test(value, false, source_id, source, request)?
        }
        PredicateExpr::And(predicates) => GroovePredicateExpr::And(
            predicates
                .iter()
                .map(|predicate| lower_predicate(predicate, source_id, source, request))
                .collect::<Result<Vec<_>, _>>()?,
        ),
        PredicateExpr::Or(predicates) => GroovePredicateExpr::Or(
            predicates
                .iter()
                .map(|predicate| lower_predicate(predicate, source_id, source, request))
                .collect::<Result<Vec<_>, _>>()?,
        ),
        PredicateExpr::Not(predicate) => {
            lower_not_predicate(predicate, source_id, source, request)?
        }
    })
}

fn lower_not_predicate(
    predicate: &PredicateExpr,
    source_id: &SourceId,
    source: &ResolvedSource,
    request: &QueryProgramRequest,
) -> Result<GroovePredicateExpr, UnsupportedReason> {
    let lowered = match lower_not_predicate_inner(predicate, source_id, source, request) {
        Err(reason) if is_unbound_claim_reason(&reason) => constant_predicate(false),
        other => other?,
    };
    Ok(lowered.canonicalize())
}

fn lower_not_predicate_inner(
    predicate: &PredicateExpr,
    source_id: &SourceId,
    source: &ResolvedSource,
    request: &QueryProgramRequest,
) -> Result<GroovePredicateExpr, UnsupportedReason> {
    Ok(match predicate {
        PredicateExpr::True => GroovePredicateExpr::Or(Vec::new()),
        PredicateExpr::False => GroovePredicateExpr::And(Vec::new()),
        PredicateExpr::Compare { left, op, right } => lower_compare(
            left,
            invert_comparison(*op),
            right,
            source_id,
            source,
            request,
        )?,
        PredicateExpr::In { value, options } => GroovePredicateExpr::And(
            options
                .iter()
                .map(|option| {
                    lower_compare(value, ComparisonOp::Ne, option, source_id, source, request)
                })
                .collect::<Result<Vec<_>, _>>()?,
        ),
        PredicateExpr::ArrayContains { .. } | PredicateExpr::TextContains { .. } => {
            return Err(UnsupportedReason::Operator(
                "negated containment predicates are not lowered yet".to_owned(),
            ));
        }
        PredicateExpr::IsNull(value) => lower_null_test(value, false, source_id, source, request)?,
        PredicateExpr::IsNotNull(value) => {
            lower_null_test(value, true, source_id, source, request)?
        }
        PredicateExpr::And(predicates) => GroovePredicateExpr::Or(
            predicates
                .iter()
                .map(|predicate| lower_not_predicate(predicate, source_id, source, request))
                .collect::<Result<Vec<_>, _>>()?,
        ),
        PredicateExpr::Or(predicates) => GroovePredicateExpr::And(
            predicates
                .iter()
                .map(|predicate| lower_not_predicate(predicate, source_id, source, request))
                .collect::<Result<Vec<_>, _>>()?,
        ),
        PredicateExpr::Not(predicate) => lower_predicate(predicate, source_id, source, request)?,
    })
}

fn invert_comparison(op: ComparisonOp) -> ComparisonOp {
    match op {
        ComparisonOp::Eq => ComparisonOp::Ne,
        ComparisonOp::Ne => ComparisonOp::Eq,
        ComparisonOp::Lt => ComparisonOp::Gte,
        ComparisonOp::Lte => ComparisonOp::Gt,
        ComparisonOp::Gt => ComparisonOp::Lte,
        ComparisonOp::Gte => ComparisonOp::Lt,
    }
}

fn lower_compare(
    left: &NormalizedValueRef,
    op: ComparisonOp,
    right: &NormalizedValueRef,
    source_id: &SourceId,
    source: &ResolvedSource,
    request: &QueryProgramRequest,
) -> Result<GroovePredicateExpr, UnsupportedReason> {
    let left = lower_value_ref(left, source_id, source, request)?;
    let right = lower_value_ref(right, source_id, source, request)?;
    let kind = predicate_kind(op);

    match (left, right) {
        (LoweredValueRef::Field(field), LoweredValueRef::Literal(value)) => {
            let value = coerce_literal_for_source_field(value, source, &field);
            Ok(GroovePredicateExpr::from_field_literal(kind, field, value))
        }
        (LoweredValueRef::Literal(value), LoweredValueRef::Field(field)) => {
            Ok(GroovePredicateExpr::from_field_literal(
                kind.reversed(),
                field.clone(),
                coerce_literal_for_source_field(value, source, &field),
            ))
        }
        (LoweredValueRef::Field(field), LoweredValueRef::Field(value_field)) => match op {
            ComparisonOp::Eq => Ok(GroovePredicateExpr::EqField { field, value_field }),
            ComparisonOp::Ne => Ok(GroovePredicateExpr::NeqField { field, value_field }),
            _ => Err(UnsupportedReason::Operator(format!(
                "field-to-field comparison {:?} is not lowered yet",
                op
            ))),
        },
        (LoweredValueRef::Literal(left), LoweredValueRef::Literal(right)) => {
            Ok(constant_predicate(compare_literals(&left, op, &right)))
        }
    }
}

fn lower_contains(
    value: &NormalizedValueRef,
    needle: &NormalizedValueRef,
    source_id: &SourceId,
    source: &ResolvedSource,
    request: &QueryProgramRequest,
) -> Result<GroovePredicateExpr, UnsupportedReason> {
    let value = lower_value_ref(value, source_id, source, request)?;
    let needle = lower_value_ref(needle, source_id, source, request)?;
    match (value, needle) {
        (LoweredValueRef::Field(field), LoweredValueRef::Literal(value)) => {
            let value = coerce_literal_for_source_field(value, source, &field);
            Ok(GroovePredicateExpr::Contains { field, value })
        }
        (LoweredValueRef::Field(field), LoweredValueRef::Field(needle_field)) => {
            Ok(GroovePredicateExpr::ContainsField {
                field,
                needle_field,
            })
        }
        (LoweredValueRef::Literal(LiteralValue::Array(values)), LoweredValueRef::Field(field)) => {
            if values.is_empty() {
                return Ok(constant_predicate(false));
            }
            Ok(GroovePredicateExpr::Or(
                values
                    .into_iter()
                    .map(|value| GroovePredicateExpr::Eq {
                        field: field.clone(),
                        value: coerce_literal_for_source_field(value, source, &field),
                    })
                    .collect(),
            ))
        }
        _ => Err(UnsupportedReason::Operator(
            "array contains requires a source field haystack".to_owned(),
        )),
    }
}

fn coerce_literal_for_source_field(
    value: LiteralValue,
    source: &ResolvedSource,
    field: &str,
) -> LiteralValue {
    if field == source.row_shape.row_uuid_field {
        return coerce_literal_for_value_type(value, &ValueType::Uuid);
    }
    let logical_field = logical_user_column(field);
    let Some(column) = source
        .table_schema
        .columns
        .iter()
        .find(|column| column.name == logical_field)
    else {
        return value;
    };
    coerce_literal_for_value_type(value, &column.column_type.value_type())
}

fn coerce_literal_for_value_type(value: LiteralValue, value_type: &ValueType) -> LiteralValue {
    match (value, value_type) {
        (LiteralValue::String(value), ValueType::Uuid) => uuid::Uuid::parse_str(&value)
            .map(LiteralValue::Uuid)
            .unwrap_or(LiteralValue::String(value)),
        (LiteralValue::Uuid(value), ValueType::String) => LiteralValue::String(value.to_string()),
        (LiteralValue::String(value), ValueType::Enum(schema)) => schema
            .discriminant(&value)
            .map(LiteralValue::Enum)
            .unwrap_or(LiteralValue::String(value)),
        (LiteralValue::Nullable(Some(value)), value_type) => LiteralValue::Nullable(Some(
            Box::new(coerce_literal_for_value_type(*value, value_type)),
        )),
        (value, ValueType::Nullable(inner)) => {
            LiteralValue::Nullable(Some(Box::new(coerce_literal_for_value_type(value, inner))))
        }
        (LiteralValue::Array(values), ValueType::Array(inner)) => LiteralValue::Array(
            values
                .into_iter()
                .map(|value| coerce_literal_for_value_type(value, inner))
                .collect(),
        ),
        (LiteralValue::Tuple(values), ValueType::Tuple(types)) if values.len() == types.len() => {
            LiteralValue::Tuple(
                values
                    .into_iter()
                    .zip(types)
                    .map(|(value, value_type)| coerce_literal_for_value_type(value, value_type))
                    .collect(),
            )
        }
        (value, _) => value,
    }
}

fn lower_null_test(
    value: &NormalizedValueRef,
    is_null: bool,
    source_id: &SourceId,
    source: &ResolvedSource,
    request: &QueryProgramRequest,
) -> Result<GroovePredicateExpr, UnsupportedReason> {
    match lower_value_ref(value, source_id, source, request)? {
        LoweredValueRef::Field(field) if is_null => Ok(GroovePredicateExpr::IsNull { field }),
        LoweredValueRef::Field(field) => Ok(GroovePredicateExpr::IsNotNull { field }),
        LoweredValueRef::Literal(LiteralValue::Nullable(None)) => Ok(constant_predicate(is_null)),
        LoweredValueRef::Literal(_) => Ok(constant_predicate(!is_null)),
    }
}

fn predicate_kind(op: ComparisonOp) -> PredicateKind {
    match op {
        ComparisonOp::Eq => PredicateKind::Eq,
        ComparisonOp::Ne => PredicateKind::Neq,
        ComparisonOp::Lt => PredicateKind::Lt,
        ComparisonOp::Lte => PredicateKind::LtEq,
        ComparisonOp::Gt => PredicateKind::Gt,
        ComparisonOp::Gte => PredicateKind::GtEq,
    }
}

fn compare_literals(left: &LiteralValue, op: ComparisonOp, right: &LiteralValue) -> bool {
    match op {
        ComparisonOp::Eq => left == right,
        ComparisonOp::Ne => left != right,
        ComparisonOp::Lt => left < right,
        ComparisonOp::Lte => left <= right,
        ComparisonOp::Gt => left > right,
        ComparisonOp::Gte => left >= right,
    }
}

fn constant_predicate(value: bool) -> GroovePredicateExpr {
    if value {
        GroovePredicateExpr::And(Vec::new())
    } else {
        GroovePredicateExpr::Or(Vec::new())
    }
}

#[derive(Clone, Debug)]
enum LoweredValueRef {
    Field(String),
    Literal(LiteralValue),
}

fn lower_field_ref(
    value: &NormalizedValueRef,
    plan: &LinearCurrentRoot,
    source: &ResolvedSource,
    request: &QueryProgramRequest,
    context: &str,
) -> Result<String, UnsupportedReason> {
    let source_id = plan.root.source().ok_or_else(|| {
        UnsupportedReason::Operator(format!("{context} must be a root source field"))
    })?;
    match lower_value_ref(value, source_id, source, request)? {
        LoweredValueRef::Field(field) => Ok(field),
        LoweredValueRef::Literal(_) => Err(UnsupportedReason::Operator(format!(
            "{context} must be a root source field"
        ))),
    }
}

fn lower_value_ref(
    value: &NormalizedValueRef,
    source_id: &SourceId,
    source: &ResolvedSource,
    request: &QueryProgramRequest,
) -> Result<LoweredValueRef, UnsupportedReason> {
    match value {
        NormalizedValueRef::SourceField {
            source: value_source,
            field,
        } if value_source == source_id => Ok(LoweredValueRef::Field(require_source_field(
            source,
            &user_column_field(field),
        )?)),
        NormalizedValueRef::SourceField { source, .. } => Err(UnsupportedReason::Operator(
            format!("predicate references unsupported source {:?}", source),
        )),
        NormalizedValueRef::Param(name) => {
            let Some(value) = request.input.binding.values.get(name) else {
                return Err(UnsupportedReason::Operator(format!(
                    "binding parameter '{name}' is not bound"
                )));
            };
            Ok(LoweredValueRef::Literal(value.clone().into()))
        }
        NormalizedValueRef::Claim(path) => {
            let value = claim_value(path, &request.policy)?;
            Ok(LoweredValueRef::Literal(value.into()))
        }
        NormalizedValueRef::FrontierColumn { .. } => Err(UnsupportedReason::Operator(
            "frontier values are not valid in root source predicates".to_owned(),
        )),
        NormalizedValueRef::RowId(RowIdRef::Source(value_source)) if value_source == source_id => {
            Ok(LoweredValueRef::Field(require_source_field(
                source,
                &source.row_shape.row_uuid_field,
            )?))
        }
        NormalizedValueRef::RowId(RowIdRef::Source(value_source)) => {
            Err(UnsupportedReason::Operator(format!(
                "predicate references unsupported row id source {:?}",
                value_source
            )))
        }
        NormalizedValueRef::RowId(RowIdRef::Frontier(_)) => Err(UnsupportedReason::Operator(
            "frontier row ids are not valid in root source predicates".to_owned(),
        )),
        NormalizedValueRef::Provenance {
            source: value_source,
            field,
        } if value_source == source_id => Ok(LoweredValueRef::Field(require_source_field(
            source,
            provenance_source_field(*field),
        )?)),
        NormalizedValueRef::Provenance { source, .. } => Err(UnsupportedReason::Operator(format!(
            "predicate references unsupported provenance source {:?}",
            source
        ))),
        NormalizedValueRef::Literal(bytes) => {
            let value = postcard::from_bytes::<Value>(bytes).map_err(|err| {
                UnsupportedReason::Operator(format!("literal value could not be decoded: {err}"))
            })?;
            Ok(LoweredValueRef::Literal(value.into()))
        }
    }
}

fn claim_value(path: &ClaimPath, policy: &PolicyContext) -> Result<Value, UnsupportedReason> {
    let (permission_subject, claims) = match policy {
        PolicyContext::Identity {
            permission_subject,
            claims,
            ..
        }
        | PolicyContext::AuthorizationSubplan {
            permission_subject,
            claims,
            ..
        } => (permission_subject, claims),
        PolicyContext::System => {
            return Err(UnsupportedReason::Operator(
                "claim values require an identity policy context".to_owned(),
            ));
        }
    };
    let [name] = path.0.as_slice() else {
        return Err(UnsupportedReason::Operator(
            "nested claim paths are not lowered yet".to_owned(),
        ));
    };
    if let Some(value) = claims.get(name) {
        return Ok(value.clone());
    }
    match name.as_str() {
        "sub" => Ok(Value::Uuid(permission_subject.0)),
        _ => Err(UnsupportedReason::UnboundClaim(path.clone())),
    }
}

fn is_unbound_claim_reason(reason: &UnsupportedReason) -> bool {
    matches!(reason, UnsupportedReason::UnboundClaim(_))
}

fn require_source_field(source: &ResolvedSource, field: &str) -> Result<String, UnsupportedReason> {
    if source.row_shape.descriptor.field_index(field).is_some() {
        Ok(field.to_owned())
    } else {
        Err(UnsupportedReason::Operator(format!(
            "resolved source {:?} does not provide field '{field}'",
            source.row_shape.source
        )))
    }
}

fn provenance_source_field(field: ProvenanceField) -> &'static str {
    match field {
        ProvenanceField::CreatedAt => "$createdAt",
        ProvenanceField::CreatedBy => "$createdBy",
        ProvenanceField::UpdatedAt => "$updatedAt",
        ProvenanceField::UpdatedBy => "$updatedBy",
    }
}

fn has_explicit_closure_path(shape: &NormalizedRowSetShape) -> bool {
    shape
        .closure_paths
        .iter()
        .any(|path| matches!(path, ClosurePath::ExplicitInclude { .. }))
}

fn lowered_terminals(
    graph: GraphBuilder,
    request: &QueryProgramRequest,
    plan: &AnalyzedQueryPlan,
    source: &ResolvedSource,
    resolved_sources: &BTreeMap<SourceId, ResolvedSource>,
    routing_param_fields: &BTreeSet<String>,
    available_fields: &BTreeSet<String>,
) -> CapabilityResult<Vec<LoweredTerminal>> {
    if root_aggregate_step(plan).is_some() {
        return lowered_aggregate_terminals(
            graph,
            request,
            plan,
            source,
            routing_param_fields,
            available_fields,
        );
    }
    let root_route_fields = routing_param_fields
        .intersection(available_fields)
        .cloned()
        .collect::<BTreeSet<_>>();
    let mut terminals = Vec::new();
    let closure = lower_closure_membership(
        graph.clone(),
        request,
        source,
        resolved_sources,
        &root_route_fields,
    )?;
    let visible_root_with_routes = if root_route_fields.is_empty() {
        closure.visible_root.clone()
    } else {
        closure
            .visible_root
            .clone()
            .project_fields(project_source_fields_with_routes(
                source,
                &root_route_fields,
            ))
    };
    if request.output.app_rows.is_some() {
        let graph = if root_route_fields.is_empty() {
            closure.visible_root.clone()
        } else {
            visible_root_with_routes.clone()
        };
        terminals.push(LoweredTerminal {
            sink: "app_rows".to_owned(),
            graph,
            output: OutputTerminalSchema::AppRows(AppRowSchema {
                descriptor: source.row_shape.descriptor,
                hidden_fields: hidden_source_fields(&source.row_shape)
                    .into_iter()
                    .chain(root_route_fields.clone())
                    .collect(),
            }),
        });
    }

    for fact in &request.output.facts {
        if matches!(fact, ProgramFactKey::ResultMembership) {
            let output = fact_output(
                fact,
                plan,
                source,
                resolved_sources,
                routing_param_fields.clone(),
            )?;
            let result_graph = fact_terminal_graph(
                fact,
                visible_root_with_routes.clone(),
                plan,
                source,
                resolved_sources,
                request,
                output_routing_fields(&output),
            )?;
            terminals.push(LoweredTerminal {
                sink: fact_sink_name(fact),
                graph: result_graph,
                output: OutputTerminalSchema::Fact(output.clone()),
            });
            for (source_id, closure_graph) in &closure.result_members {
                let resolved_source = resolved_sources.get(&source_id).ok_or_else(|| {
                    Box::new(CapabilityReport {
                        gaps: vec![UnsupportedReason::Runtime(format!(
                            "closure member source {:?} was not resolved",
                            source_id
                        ))],
                        explain: ExplainPlan::default(),
                    })
                })?;
                let output = fact_output_with_terminal(
                    fact,
                    ProgramFactTerminal::Primary,
                    plan,
                    resolved_source,
                    resolved_sources,
                    BTreeSet::new(),
                )?;
                let graph = fact_terminal_graph(
                    fact,
                    closure_graph.clone(),
                    plan,
                    resolved_source,
                    resolved_sources,
                    request,
                    output_routing_fields(&output),
                )?;
                terminals.push(LoweredTerminal {
                    sink: scoped_fact_sink_name(fact, &source_id),
                    graph,
                    output: OutputTerminalSchema::Fact(output),
                });
            }
            if has_explicit_closure_path(&request.input.shape) {
                for contribution in &request.input.shape.join_contributions {
                    let resolved_source =
                        resolved_sources.get(&contribution.source).ok_or_else(|| {
                            Box::new(CapabilityReport {
                                gaps: vec![UnsupportedReason::Runtime(format!(
                                    "join contribution source {:?} was not resolved",
                                    contribution.source
                                ))],
                                explain: ExplainPlan::default(),
                            })
                        })?;
                    let output = fact_output_with_terminal(
                        fact,
                        ProgramFactTerminal::Primary,
                        plan,
                        resolved_source,
                        resolved_sources,
                        BTreeSet::new(),
                    )?;
                    let contribution_graph = join_contribution_membership_graph(
                        closure.visible_root.clone(),
                        contribution,
                        source,
                        resolved_source,
                        &request.input.shape.nodes,
                        resolved_sources,
                        request,
                    )?;
                    let graph = fact_terminal_graph(
                        fact,
                        contribution_graph,
                        plan,
                        resolved_source,
                        resolved_sources,
                        request,
                        output_routing_fields(&output),
                    )?;
                    terminals.push(LoweredTerminal {
                        sink: scoped_fact_sink_name(fact, &contribution.source),
                        graph,
                        output: OutputTerminalSchema::Fact(output),
                    });
                }
            }
        } else if matches!(fact, ProgramFactKey::VersionWitnesses) {
            for (source_id, resolved_source) in resolved_sources {
                let content_output = fact_output_with_terminal(
                    fact,
                    ProgramFactTerminal::VersionWitnessContent,
                    plan,
                    resolved_source,
                    resolved_sources,
                    BTreeSet::new(),
                )?;
                terminals.push(LoweredTerminal {
                    sink: scoped_fact_sink_name(fact, source_id),
                    graph: content_version_witness_graph(resolved_source, "version_content")?,
                    output: OutputTerminalSchema::Fact(content_output),
                });
                if resolved_source.deletion_register.is_none() {
                    continue;
                }
                let deletion_output = fact_output_with_terminal(
                    fact,
                    ProgramFactTerminal::VersionWitnessDeletion,
                    plan,
                    resolved_source,
                    resolved_sources,
                    BTreeSet::new(),
                )?;
                terminals.push(LoweredTerminal {
                    sink: scoped_deletion_fact_sink_name(fact, source_id),
                    graph: deletion_witness_graph_for_current_register(
                        resolved_source,
                        "version_deletion",
                    )?,
                    output: OutputTerminalSchema::Fact(deletion_output),
                });
            }
        } else if matches!(fact, ProgramFactKey::ReplacementWitnesses) {
            for (source_id, resolved_source) in resolved_sources {
                let content_output = fact_output_with_terminal(
                    fact,
                    ProgramFactTerminal::ReplacementWitnessContent,
                    plan,
                    resolved_source,
                    resolved_sources,
                    BTreeSet::new(),
                )?;
                terminals.push(LoweredTerminal {
                    sink: scoped_fact_sink_name(fact, source_id),
                    graph: content_version_witness_graph(resolved_source, "replacement_content")?,
                    output: OutputTerminalSchema::Fact(content_output),
                });
                if resolved_source.deletion_register.is_none() {
                    continue;
                }
                let deletion_output = fact_output_with_terminal(
                    fact,
                    ProgramFactTerminal::ReplacementWitnessDeletion,
                    plan,
                    resolved_source,
                    resolved_sources,
                    BTreeSet::new(),
                )?;
                terminals.push(LoweredTerminal {
                    sink: scoped_deletion_fact_sink_name(fact, source_id),
                    graph: deletion_witness_graph_for_current_register(
                        resolved_source,
                        "replacement_deletion",
                    )?,
                    output: OutputTerminalSchema::Fact(deletion_output),
                });
            }
        } else {
            let terminal_route_fields = if matches!(fact, ProgramFactKey::AuthorizedRows) {
                root_route_fields.clone()
            } else {
                BTreeSet::new()
            };
            let output = fact_output(
                fact,
                plan,
                source,
                resolved_sources,
                terminal_route_fields.clone(),
            )?;
            let terminal_graph =
                fact_input_graph(fact, graph.clone(), plan, source, resolved_sources, request)?;
            let graph = fact_terminal_graph(
                fact,
                terminal_graph,
                plan,
                source,
                resolved_sources,
                request,
                terminal_route_fields,
            )?;
            terminals.push(LoweredTerminal {
                sink: fact_sink_name(fact),
                graph,
                output: OutputTerminalSchema::Fact(output),
            });
        }
    }

    Ok(terminals)
}

fn lowered_aggregate_terminals(
    graph: GraphBuilder,
    request: &QueryProgramRequest,
    plan: &AnalyzedQueryPlan,
    source: &ResolvedSource,
    routing_param_fields: &BTreeSet<String>,
    available_fields: &BTreeSet<String>,
) -> CapabilityResult<Vec<LoweredTerminal>> {
    let mut terminals = Vec::new();
    let root_route_fields = routing_param_fields
        .intersection(available_fields)
        .cloned()
        .collect::<BTreeSet<_>>();
    let aggregate_graph = if root_route_fields.is_empty() {
        graph
    } else {
        graph.project_fields(
            available_fields
                .iter()
                .map(ProjectField::named)
                .chain(root_route_fields.iter().map(ProjectField::named))
                .collect::<Vec<_>>(),
        )
    };
    if request.output.app_rows.is_some() {
        terminals.push(LoweredTerminal {
            sink: "app_rows".to_owned(),
            graph: aggregate_graph.clone(),
            output: OutputTerminalSchema::AppRows(AppRowSchema {
                descriptor: aggregate_app_row_descriptor(plan, source)?,
                hidden_fields: root_route_fields.clone(),
            }),
        });
    }
    for fact in &request.output.facts {
        if matches!(fact, ProgramFactKey::ResultMembership) {
            let output = fact_output(
                fact,
                plan,
                source,
                &BTreeMap::new(),
                root_route_fields.clone(),
            )?;
            let graph = fact_terminal_graph(
                fact,
                aggregate_graph.clone(),
                plan,
                source,
                &BTreeMap::new(),
                request,
                output_routing_fields(&output),
            )?;
            terminals.push(LoweredTerminal {
                sink: fact_sink_name(fact),
                graph,
                output: OutputTerminalSchema::Fact(output),
            });
        }
    }
    Ok(terminals)
}

fn fact_input_graph(
    key: &ProgramFactKey,
    graph: GraphBuilder,
    plan: &AnalyzedQueryPlan,
    source: &ResolvedSource,
    resolved_sources: &BTreeMap<SourceId, ResolvedSource>,
    request: &QueryProgramRequest,
) -> CapabilityResult<GraphBuilder> {
    if matches!(
        (plan, key),
        (
            AnalyzedQueryPlan::CorrelatedPath(_),
            ProgramFactKey::RelationEdges | ProgramFactKey::PathCorrelationCoverage
        )
    ) {
        if let AnalyzedQueryPlan::CorrelatedPath(path) = plan {
            return lower_correlated_path_relation_graph(path, source, resolved_sources, request)
                .map(|lowered| lowered.graph)
                .map_err(|gap| {
                    Box::new(CapabilityReport {
                        gaps: vec![gap],
                        explain: ExplainPlan {
                            capabilities: vec![
                                "correlated path relation facts lower from the parent-child path graph"
                                    .to_owned(),
                            ],
                            ..ExplainPlan::default()
                        },
                    })
                });
        }
    }
    Ok(graph)
}

#[derive(Clone, Debug)]
struct ClosureLowering {
    visible_root: GraphBuilder,
    result_members: BTreeMap<SourceId, GraphBuilder>,
}

impl ClosureLowering {
    fn all_visible_members(&self, root_source: SourceId) -> Vec<(SourceId, GraphBuilder)> {
        std::iter::once((root_source, self.visible_root.clone()))
            .chain(
                self.result_members
                    .iter()
                    .map(|(source, graph)| (source.clone(), graph.clone())),
            )
            .collect()
    }
}

fn lower_closure_membership(
    root_graph: GraphBuilder,
    request: &QueryProgramRequest,
    root_source: &ResolvedSource,
    resolved_sources: &BTreeMap<SourceId, ResolvedSource>,
    route_fields: &BTreeSet<String>,
) -> CapabilityResult<ClosureLowering> {
    let mut visible_root = root_graph;
    for path in &request.input.shape.closure_paths {
        if let ClosurePath::ExplicitInclude {
            segments,
            root_gate: Some(root_gate),
            ..
        } = path
        {
            visible_root = required_closure_parent_graph(
                visible_root,
                segments,
                *root_gate,
                root_source,
                resolved_sources,
                route_fields,
            )?;
        }
    }
    let mut result_members = BTreeMap::<SourceId, GraphBuilder>::new();
    for path in &request.input.shape.closure_paths {
        for (_, source, graph) in closure_membership_graph_for_path(
            visible_root.clone(),
            path,
            root_source,
            resolved_sources,
            route_fields,
        )? {
            let Some(resolved_source) = resolved_sources.get(&source) else {
                continue;
            };
            let graph = graph.project_fields(project_source_fields_with_routes(
                resolved_source,
                route_fields,
            ));
            result_members
                .entry(source)
                .and_modify(|existing| {
                    *existing = GraphBuilder::union([existing.clone(), graph.clone()]);
                })
                .or_insert(graph);
        }
    }
    Ok(ClosureLowering {
        visible_root,
        result_members,
    })
}

fn reachable_contribution_membership_graph(
    visible_root: GraphBuilder,
    contribution: &ReachableContribution,
    root_source: &ResolvedSource,
    contribution_source: &ResolvedSource,
    nodes: &BTreeMap<RowSetNodeId, RowSetExpr>,
    resolved_sources: &BTreeMap<SourceId, ResolvedSource>,
    request: &QueryProgramRequest,
) -> CapabilityResult<GraphBuilder> {
    let mut visited = BTreeSet::new();
    let plan = analyze_relation_input_node(&contribution.access_input, nodes, &mut visited)
        .map_err(single_gap_report)?;
    let lowered =
        lower_relation_input(&plan, resolved_sources, request).map_err(single_gap_report)?;
    let join_field = user_column_field(&contribution.root_ref_field);
    if !lowered.fields.contains(&join_field) {
        return Err(Box::new(CapabilityReport {
            gaps: vec![UnsupportedReason::Operator(format!(
                "reachable contribution {} does not provide root reference field {join_field}",
                contribution.id
            ))],
            explain: ExplainPlan {
                capabilities: vec![
                    "reachable contribution payload requires root reference field".to_owned(),
                ],
                ..ExplainPlan::default()
            },
        }));
    }
    let mut contribution_graph = lowered.graph;
    if lowered.nullable_fields.contains(&join_field) {
        contribution_graph = unwrap_nullable_join_key(contribution_graph, join_field.clone(), 1);
    }
    Ok(GraphBuilder::join(
        visible_root,
        contribution_graph,
        [root_source.row_shape.row_uuid_field.clone()],
        [join_field],
    )
    .project_fields(project_source_fields_from_prefix(
        contribution_source,
        RIGHT_JOIN_PREFIX,
    )))
}

fn join_contribution_membership_graph(
    visible_root: GraphBuilder,
    contribution: &JoinContribution,
    root_source: &ResolvedSource,
    contribution_source: &ResolvedSource,
    nodes: &BTreeMap<RowSetNodeId, RowSetExpr>,
    resolved_sources: &BTreeMap<SourceId, ResolvedSource>,
    request: &QueryProgramRequest,
) -> CapabilityResult<GraphBuilder> {
    let mut visited = BTreeSet::new();
    let plan = analyze_relation_input_node(&contribution.input, nodes, &mut visited)
        .map_err(single_gap_report)?;
    let lowered =
        lower_relation_input(&plan, resolved_sources, request).map_err(single_gap_report)?;
    let (root_keys, join_keys) = lower_root_to_relation_key_pairs(
        &contribution.membership,
        root_source,
        &plan,
        &lowered,
        request,
    )
    .map_err(single_gap_report)?;
    if let Some(join_key) = join_keys
        .iter()
        .find(|join_key| !lowered.fields.contains(*join_key))
    {
        return Err(Box::new(CapabilityReport {
            gaps: vec![UnsupportedReason::Operator(format!(
                "join contribution {} does not provide join key field {join_key}",
                contribution.id
            ))],
            explain: ExplainPlan {
                capabilities: vec![
                    "join contribution payload requires the normalized join key".to_owned(),
                ],
                ..ExplainPlan::default()
            },
        }));
    }
    let mut contribution_graph = lowered.graph;
    let mut unwrapped_join_keys = BTreeSet::new();
    for join_key in &join_keys {
        if lowered.nullable_fields.contains(join_key)
            && unwrapped_join_keys.insert(join_key.clone())
        {
            contribution_graph = unwrap_nullable_join_key(contribution_graph, join_key.clone(), 1);
        }
    }
    Ok(
        GraphBuilder::join(visible_root, contribution_graph, root_keys, join_keys).project_fields(
            project_source_fields_from_prefix(contribution_source, RIGHT_JOIN_PREFIX),
        ),
    )
}

fn required_closure_parent_graph(
    parent_graph: GraphBuilder,
    segments: &[ClosurePathSegment],
    root_gate: ClosureRootGate,
    root_source: &ResolvedSource,
    resolved_sources: &BTreeMap<SourceId, ResolvedSource>,
    route_fields: &BTreeSet<String>,
) -> CapabilityResult<GraphBuilder> {
    required_closure_parent_graph_from_segment(
        parent_graph,
        segments,
        0,
        root_gate,
        root_source,
        resolved_sources,
        route_fields,
    )
}

fn required_closure_parent_graph_from_segment(
    parent_graph: GraphBuilder,
    segments: &[ClosurePathSegment],
    index: usize,
    root_gate: ClosureRootGate,
    parent_source: &ResolvedSource,
    resolved_sources: &BTreeMap<SourceId, ResolvedSource>,
    route_fields: &BTreeSet<String>,
) -> CapabilityResult<GraphBuilder> {
    let Some(segment) = segments.get(index) else {
        return Ok(parent_graph);
    };
    let target = resolved_sources.get(&segment.target).ok_or_else(|| {
        Box::new(CapabilityReport {
            gaps: vec![UnsupportedReason::Runtime(format!(
                "closure target source {:?} was not resolved",
                segment.target
            ))],
            explain: ExplainPlan::default(),
        })
    })?;
    let no_route_fields = BTreeSet::new();
    let target_valid = required_closure_parent_graph_from_segment(
        target.graph.clone(),
        segments,
        index + 1,
        root_gate,
        target,
        resolved_sources,
        &no_route_fields,
    )?;
    let source_key = user_column_field(&segment.source_field);
    let Some(source_key_type) = source_field_type(parent_source, &source_key) else {
        return Err(Box::new(CapabilityReport {
            gaps: vec![UnsupportedReason::Operator(format!(
                "closure source field {source_key:?} is not projected"
            ))],
            explain: ExplainPlan::default(),
        }));
    };
    let parent_row_uuid = parent_source.row_shape.row_uuid_field.clone();
    let target_row_uuid = target.row_shape.row_uuid_field.clone();
    let (required_base, required_key_type) =
        unwrap_nullable_layers(parent_graph.clone(), source_key.clone(), source_key_type);
    let required = match required_key_type {
        ValueType::Array(_) => required_base.unnest(source_key.clone(), CLOSURE_REQUIRED_ELEMENT),
        _ => required_base,
    };
    let left_key = match required_key_type {
        ValueType::Array(_) => CLOSURE_REQUIRED_ELEMENT.to_owned(),
        _ => source_key.clone(),
    };
    let mut covered_fields = project_source_fields_with_routes_from_prefix(
        parent_source,
        LEFT_JOIN_PREFIX,
        route_fields,
    );
    if left_key == CLOSURE_REQUIRED_ELEMENT {
        covered_fields.push(ProjectField::renamed(
            "left.__closure_required_element",
            CLOSURE_REQUIRED_ELEMENT,
        ));
    }
    let covered = GraphBuilder::join(
        required.clone(),
        target_valid,
        [left_key.clone()],
        [target_row_uuid.clone()],
    )
    .project_fields(covered_fields);
    if root_gate == ClosureRootGate::Inner && !matches!(required_key_type, ValueType::Array(_)) {
        return Ok(covered.project_fields(project_source_fields_with_routes(
            parent_source,
            route_fields,
        )));
    }
    let missing = if left_key == CLOSURE_REQUIRED_ELEMENT {
        GraphBuilder::anti_join(
            required.clone(),
            covered.clone(),
            [parent_row_uuid.clone(), left_key],
            [
                parent_row_uuid.clone(),
                source_key_for_required(required_key_type, &source_key),
            ],
        )
    } else {
        GraphBuilder::anti_join(
            required.clone(),
            covered.clone(),
            [left_key],
            [source_key_for_required(required_key_type, &source_key)],
        )
    }
    .project_fields(project_source_fields_with_routes(
        parent_source,
        route_fields,
    ));
    let all_required_refs_resolve = GraphBuilder::anti_join(
        parent_graph,
        missing,
        [parent_row_uuid.clone()],
        [parent_row_uuid.clone()],
    );
    if root_gate == ClosureRootGate::Required {
        return Ok(all_required_refs_resolve);
    }
    Ok(GraphBuilder::join(
        all_required_refs_resolve,
        GraphBuilder::arg_min_by(
            covered,
            [parent_row_uuid.clone()],
            [parent_row_uuid.clone()],
        ),
        [parent_row_uuid.clone()],
        [parent_row_uuid],
    )
    .project_fields(project_source_fields_with_routes_from_prefix(
        parent_source,
        LEFT_JOIN_PREFIX,
        route_fields,
    )))
}

fn source_key_for_required(source_key_type: &ValueType, source_key: &str) -> String {
    match source_key_type {
        ValueType::Array(_) => CLOSURE_REQUIRED_ELEMENT.to_owned(),
        _ => source_key.to_owned(),
    }
}

fn unwrap_nullable_layers(
    mut graph: GraphBuilder,
    field: String,
    mut value_type: &ValueType,
) -> (GraphBuilder, &ValueType) {
    while let ValueType::Nullable(inner) = value_type {
        graph = graph.unwrap_nullable(field.clone());
        value_type = inner.as_ref();
    }
    (graph, value_type)
}

fn closure_membership_graph_for_path(
    root_graph: GraphBuilder,
    path: &ClosurePath,
    root_source: &ResolvedSource,
    resolved_sources: &BTreeMap<SourceId, ResolvedSource>,
    route_fields: &BTreeSet<String>,
) -> CapabilityResult<Vec<(usize, SourceId, GraphBuilder)>> {
    let segments = closure_path_segments(path);
    let can_lower_as_parent_semijoin =
        route_fields.is_empty() && matches!(path, ClosurePath::ImplicitRootReference { .. });
    let mut current_graph = root_graph.project_fields(
        project_source_fields_with_routes(root_source, route_fields)
            .into_iter()
            .chain([ProjectField::renamed(
                root_source.row_shape.row_uuid_field.clone(),
                "__closure_root_row_uuid",
            )]),
    );
    let mut current_source = root_source.clone();
    let mut outputs = Vec::new();
    for (index, segment) in segments.iter().enumerate() {
        let target = resolved_sources.get(&segment.target).ok_or_else(|| {
            Box::new(CapabilityReport {
                gaps: vec![UnsupportedReason::Runtime(format!(
                    "closure target source {:?} was not resolved",
                    segment.target
                ))],
                explain: ExplainPlan::default(),
            })
        })?;
        let source_key = user_column_field(&segment.source_field);
        let Some(source_key_type) = source_field_type(&current_source, &source_key) else {
            return Err(Box::new(CapabilityReport {
                gaps: vec![UnsupportedReason::Operator(format!(
                    "closure source field {source_key:?} is not projected"
                ))],
                explain: ExplainPlan::default(),
            }));
        };
        let joined = if can_lower_as_parent_semijoin {
            let (source_base, source_key_type) =
                unwrap_nullable_layers(current_graph, source_key.clone(), source_key_type);
            let source_keys = match source_key_type {
                ValueType::Array(_) => {
                    source_base.unnest(source_key.clone(), CLOSURE_REQUIRED_ELEMENT)
                }
                _ => source_base,
            };
            let source_key = source_key_for_required(source_key_type, &source_key);
            GraphBuilder::semi_join(
                target.graph.clone(),
                source_keys.project_fields(vec![ProjectField::named(source_key.clone())]),
                [target.row_shape.row_uuid_field.clone()],
                [source_key],
            )
            .project_fields(project_source_fields_with_routes(target, route_fields))
        } else {
            GraphBuilder::join(
                current_graph.unwrap_nullable(source_key.clone()),
                target.graph.clone(),
                [source_key],
                [target.row_shape.row_uuid_field.clone()],
            )
            .project_fields(
                project_source_fields_from_prefix(target, RIGHT_JOIN_PREFIX)
                    .into_iter()
                    .chain([ProjectField::renamed(
                        "left.__closure_root_row_uuid",
                        "__closure_root_row_uuid",
                    )])
                    .chain(
                        route_fields
                            .iter()
                            .map(|field| ProjectField::renamed(left_field(field), field.clone())),
                    ),
            )
        };
        outputs.push((index, segment.target.clone(), joined.clone()));
        current_graph = joined;
        current_source = target.clone();
    }
    let _ = current_source;
    Ok(outputs)
}

fn closure_path_segments(path: &ClosurePath) -> Vec<&ClosurePathSegment> {
    match path {
        ClosurePath::ImplicitRootReference { segment, .. } => vec![segment],
        ClosurePath::ExplicitInclude { segments, .. } => segments.iter().collect(),
    }
}

fn project_source_fields_from_prefix(source: &ResolvedSource, prefix: &str) -> Vec<ProjectField> {
    source
        .row_shape
        .descriptor
        .fields()
        .iter()
        .filter_map(|field| field.name.as_ref())
        .map(|field| ProjectField::renamed(format!("{prefix}{field}"), field.clone()))
        .collect()
}

fn project_source_fields_with_routes(
    source: &ResolvedSource,
    route_fields: &BTreeSet<String>,
) -> Vec<ProjectField> {
    project_source_fields_with_routes_from_prefix(source, "", route_fields)
}

fn project_source_fields_with_routes_from_prefix(
    source: &ResolvedSource,
    prefix: &str,
    route_fields: &BTreeSet<String>,
) -> Vec<ProjectField> {
    let mut fields = project_source_fields_from_prefix(source, prefix);
    fields.extend(
        route_fields
            .iter()
            .map(|field| ProjectField::renamed(format!("{prefix}{field}"), field.clone())),
    );
    fields
}

fn fact_output(
    key: &ProgramFactKey,
    plan: &AnalyzedQueryPlan,
    source: &ResolvedSource,
    resolved_sources: &BTreeMap<SourceId, ResolvedSource>,
    routing_param_fields: BTreeSet<String>,
) -> CapabilityResult<ProgramFactOutput> {
    fact_output_with_terminal(
        key,
        ProgramFactTerminal::Primary,
        plan,
        source,
        resolved_sources,
        routing_param_fields,
    )
}

fn fact_output_with_terminal(
    key: &ProgramFactKey,
    terminal: ProgramFactTerminal,
    plan: &AnalyzedQueryPlan,
    source: &ResolvedSource,
    resolved_sources: &BTreeMap<SourceId, ResolvedSource>,
    routing_param_fields: BTreeSet<String>,
) -> CapabilityResult<ProgramFactOutput> {
    let schema = match key {
        ProgramFactKey::AuthorizedRows => ProgramFactSchema::AuthorizedRows(AuthorizedRowsSchema {
            row_field: source.row_shape.row_uuid_field.clone(),
            routing_param_fields,
        }),
        ProgramFactKey::ResultMembership => {
            if root_aggregate_step(plan).is_some() {
                return Ok(ProgramFactOutput {
                    key: key.clone(),
                    terminal,
                    schema: ProgramFactSchema::AggregateResult(aggregate_result_schema(
                        plan,
                        source,
                        routing_param_fields,
                    )?),
                });
            }
            let version = version_witness_fields(&source.row_shape)?;
            ProgramFactSchema::ResultMembership(ResultMembershipSchema {
                table_field: "table_name".to_owned(),
                row_field: source.row_shape.row_uuid_field.clone(),
                branch_or_prefix_field: version.branch_or_prefix_field.clone(),
                version: ResultMembershipVersionSchema::Content(ContentVersionFields {
                    tx_time_field: "content_tx_time".to_owned(),
                    tx_node_field: "content_tx_node_id".to_owned(),
                }),
                settle_position_field: settle_position_field(&source.row_shape),
                routing_param_fields,
            })
        }
        ProgramFactKey::SourceCoverage(_scope) => {
            let coverage = coverage_fields(&source.row_shape)?;
            ProgramFactSchema::SourceCoverage(SourceCoverageSchema {
                source_field: "source".to_owned(),
                table_field: "table".to_owned(),
                row_field: None,
                coverage_field: coverage.coverage_field.clone(),
                routing_param_fields: BTreeSet::new(),
            })
        }
        ProgramFactKey::VersionWitnesses => {
            let version = version_witness_fields(&source.row_shape)?;
            let witness = version_witness_schema(source, &version);
            ProgramFactSchema::VersionWitnesses(VersionWitnessSchemas {
                role_field: "event_kind".to_owned(),
                content: Some(witness.clone()),
                deletion: Some(witness),
            })
        }
        ProgramFactKey::ReplacementWitnesses => {
            let version = version_witness_fields(&source.row_shape)?;
            let witness = version_witness_schema(source, &version);
            ProgramFactSchema::ReplacementWitnesses(VersionWitnessSchemas {
                role_field: "event_kind".to_owned(),
                content: Some(witness.clone()),
                deletion: Some(witness),
            })
        }
        ProgramFactKey::RelationEdges => {
            ProgramFactSchema::RelationEdges(relation_edge_schema(plan, source, resolved_sources)?)
        }
        ProgramFactKey::PathCorrelationCoverage => ProgramFactSchema::PathCorrelationCoverage(
            path_correlation_coverage_schema(plan, source, resolved_sources)?,
        ),
        _ => {
            return Err(Box::new(CapabilityReport {
                gaps: vec![UnsupportedReason::Output(Box::new(key.clone()))],
                explain: ExplainPlan {
                    capabilities: vec!["requested fact is not lowered yet".to_owned()],
                    ..ExplainPlan::default()
                },
            }));
        }
    };

    Ok(ProgramFactOutput {
        key: key.clone(),
        terminal,
        schema,
    })
}

fn output_routing_fields(output: &ProgramFactOutput) -> BTreeSet<String> {
    match &output.schema {
        ProgramFactSchema::AuthorizedRows(schema) => schema.routing_param_fields.clone(),
        ProgramFactSchema::ResultMembership(schema) => schema.routing_param_fields.clone(),
        ProgramFactSchema::AggregateResult(schema) => schema.routing_param_fields.clone(),
        ProgramFactSchema::SourceCoverage(schema) => schema.routing_param_fields.clone(),
        ProgramFactSchema::ReadFrontierSettled(schema) => schema.routing_param_fields.clone(),
        _ => BTreeSet::new(),
    }
}

fn fact_sink_name(key: &ProgramFactKey) -> String {
    match key {
        ProgramFactKey::AuthorizedRows => "policy.authorized_rows".to_owned(),
        ProgramFactKey::ResultMembership => "maintained.result_current".to_owned(),
        ProgramFactKey::VersionWitnesses => "maintained.version_content".to_owned(),
        ProgramFactKey::ReplacementWitnesses => "maintained.replacement_content".to_owned(),
        ProgramFactKey::RelationEdges => "maintained.relation_edges".to_owned(),
        ProgramFactKey::PathCorrelationCoverage => "maintained.path_coverage".to_owned(),
        ProgramFactKey::SourceCoverage(_) => "maintained.source_coverage".to_owned(),
        other => format!("fact.{other:?}"),
    }
}

fn scoped_fact_sink_name(key: &ProgramFactKey, source: &SourceId) -> String {
    let base = fact_sink_name(key);
    let path = source_path_sink_fragment(source);
    format!("{base}.{}.{}", source.table, path)
}

fn scoped_deletion_fact_sink_name(key: &ProgramFactKey, source: &SourceId) -> String {
    let base = match key {
        ProgramFactKey::VersionWitnesses => "maintained.version_deletion",
        ProgramFactKey::ReplacementWitnesses => "maintained.replacement_deletion",
        _ => return scoped_fact_sink_name(key, source),
    };
    format!(
        "{base}.{}.{}",
        source.table,
        source_path_sink_fragment(source)
    )
}

fn source_path_sink_fragment(source: &SourceId) -> String {
    source
        .path
        .components
        .iter()
        .map(|component| match component {
            SourceRole::Root => "root".to_owned(),
            SourceRole::Alias(alias) => alias.replace(|ch: char| !ch.is_ascii_alphanumeric(), "_"),
            SourceRole::RecursiveSeed(name) => format!(
                "recursive_seed_{}",
                name.replace(|ch: char| !ch.is_ascii_alphanumeric(), "_")
            ),
            SourceRole::RecursiveStep(name) => format!(
                "recursive_step_{}",
                name.replace(|ch: char| !ch.is_ascii_alphanumeric(), "_")
            ),
            SourceRole::CorrelatedChild(name) => format!(
                "correlated_child_{}",
                name.replace(|ch: char| !ch.is_ascii_alphanumeric(), "_")
            ),
            SourceRole::Policy(name) => {
                format!(
                    "policy_{}",
                    name.replace(|ch: char| !ch.is_ascii_alphanumeric(), "_")
                )
            }
        })
        .collect::<Vec<_>>()
        .join(".")
}

fn fact_terminal_graph(
    key: &ProgramFactKey,
    graph: GraphBuilder,
    plan: &AnalyzedQueryPlan,
    source: &ResolvedSource,
    resolved_sources: &BTreeMap<SourceId, ResolvedSource>,
    request: &QueryProgramRequest,
    routing_param_fields: BTreeSet<String>,
) -> CapabilityResult<GraphBuilder> {
    match key {
        ProgramFactKey::AuthorizedRows => Ok(graph.project_fields(
            std::iter::once(ProjectField::named(source.row_shape.row_uuid_field.clone()))
                .chain(routing_param_fields.into_iter().map(ProjectField::named))
                .collect::<Vec<_>>(),
        )),
        ProgramFactKey::ResultMembership => {
            if root_aggregate_step(plan).is_some() {
                return Ok(graph.project_fields(aggregate_result_membership_fields(
                    plan,
                    source,
                    routing_param_fields,
                )?));
            }
            Ok(graph.project_fields(result_membership_fields(source, routing_param_fields)?))
        }
        ProgramFactKey::VersionWitnesses => {
            content_version_witness_graph(source, "version_content")
        }
        ProgramFactKey::ReplacementWitnesses => {
            content_version_witness_graph(source, "replacement_content")
        }
        ProgramFactKey::RelationEdges => {
            let _ = relation_edge_schema(plan, source, resolved_sources)?;
            relation_edge_graph(key, graph, plan, source, resolved_sources, request)
        }
        ProgramFactKey::PathCorrelationCoverage => {
            let _ = path_correlation_coverage_schema(plan, source, resolved_sources)?;
            Ok(graph)
        }
        ProgramFactKey::SourceCoverage(_) => {
            let coverage = coverage_fields(&source.row_shape)?;
            Ok(graph.project_fields(vec![
                ProjectField::literal(
                    "source",
                    Value::String(source.row_shape.source.table.clone()),
                ),
                ProjectField::literal("table", Value::String(source.table_schema.name.clone())),
                ProjectField::renamed(coverage.coverage_field, "coverage"),
            ]))
        }
        _ => Err(Box::new(CapabilityReport {
            gaps: vec![UnsupportedReason::Output(Box::new(key.clone()))],
            explain: ExplainPlan {
                capabilities: vec!["requested fact graph is not lowered yet".to_owned()],
                ..ExplainPlan::default()
            },
        })),
    }
}

fn route_literal_project_field(
    route_field: &str,
    request: &QueryProgramRequest,
) -> Result<ProjectField, UnsupportedReason> {
    let domain = parameter_domain_for_request(request)?;
    if let Some(path) = claim_path_from_param_field(route_field) {
        let value = claim_value(&path, &request.policy)?;
        let literal = domain
            .claim_params
            .get(route_field)
            .map(|claim| {
                coerce_literal_for_value_type(value.clone().into(), &claim.ty.value_type())
            })
            .unwrap_or_else(|| value.into());
        return Ok(ProjectField::literal(route_field.to_owned(), literal));
    }
    let Some(param) = route_param_from_field(route_field) else {
        return Err(UnsupportedReason::Runtime(format!(
            "authorization route field '{route_field}' is neither a claim nor user parameter"
        )));
    };
    let Some(value) = request.input.binding.values.get(param) else {
        return Err(UnsupportedReason::Runtime(format!(
            "authorization route field '{route_field}' refers to unbound parameter '{param}'"
        )));
    };
    let literal = domain
        .user_params
        .get(param)
        .map(|ty| coerce_literal_for_value_type(value.clone().into(), &ty.value_type()))
        .unwrap_or_else(|| value.clone().into());
    Ok(ProjectField::literal(route_field.to_owned(), literal))
}

fn relation_edge_graph(
    key: &ProgramFactKey,
    graph: GraphBuilder,
    plan: &AnalyzedQueryPlan,
    source: &ResolvedSource,
    resolved_sources: &BTreeMap<SourceId, ResolvedSource>,
    request: &QueryProgramRequest,
) -> CapabilityResult<GraphBuilder> {
    match plan {
        AnalyzedQueryPlan::CorrelatedPath(path) => {
            let mut graphs =
                correlated_relation_edge_graphs(path, graph, source, resolved_sources, request)?;
            if graphs.len() == 1 {
                Ok(graphs.remove(0))
            } else {
                Ok(GraphBuilder::union(graphs))
            }
        }
        AnalyzedQueryPlan::RecursiveRelation(_) => Ok(graph),
        AnalyzedQueryPlan::Linear(_) | AnalyzedQueryPlan::Union(_) => {
            Err(Box::new(CapabilityReport {
                gaps: vec![UnsupportedReason::Output(Box::new(key.clone()))],
                explain: ExplainPlan {
                    capabilities: vec![
                        "relation edge facts require a path or recursive relation node".to_owned(),
                    ],
                    ..ExplainPlan::default()
                },
            }))
        }
    }
}

fn correlated_relation_edge_graphs(
    path: &CorrelatedPathPlan,
    graph: GraphBuilder,
    source: &ResolvedSource,
    resolved_sources: &BTreeMap<SourceId, ResolvedSource>,
    request: &QueryProgramRequest,
) -> CapabilityResult<Vec<GraphBuilder>> {
    let target = resolved_sources.get(&path.path.child).ok_or_else(|| {
        Box::new(CapabilityReport {
            gaps: vec![UnsupportedReason::Runtime(format!(
                "path child source {:?} was not resolved",
                path.path.child
            ))],
            explain: ExplainPlan::default(),
        })
    })?;
    let mut graphs = vec![
        graph
            .clone()
            .project_fields(correlated_relation_edge_fields(source, target, path)?),
    ];
    for sibling in &path.siblings {
        let sibling_graph =
            lower_correlated_path_relation_graph(sibling, source, resolved_sources, request)
                .map_err(|gap| {
                    Box::new(CapabilityReport {
                        gaps: vec![gap],
                        explain: ExplainPlan {
                            capabilities: vec![
                        "sibling correlated path relation facts lower from the shared root graph"
                            .to_owned(),
                    ],
                            ..ExplainPlan::default()
                        },
                    })
                })?
                .graph;
        graphs.extend(correlated_relation_edge_graphs(
            sibling,
            sibling_graph,
            source,
            resolved_sources,
            request,
        )?);
    }
    for nested in &path.nested {
        let nested_parent = graph
            .clone()
            .project_fields(project_source_fields_from_prefix(target, RIGHT_JOIN_PREFIX));
        let nested_graph = lower_correlated_path_relation_graph_from_parent(
            nested,
            nested_parent,
            target,
            resolved_sources,
            request,
        )
        .map_err(|gap| {
            Box::new(CapabilityReport {
                gaps: vec![gap],
                explain: ExplainPlan {
                    capabilities: vec![
                        "nested correlated path relation facts lower from parent-child path graphs"
                            .to_owned(),
                    ],
                    ..ExplainPlan::default()
                },
            })
        })?
        .graph;
        graphs.extend(correlated_relation_edge_graphs(
            nested,
            nested_graph,
            target,
            resolved_sources,
            request,
        )?);
    }
    Ok(graphs)
}

fn correlated_relation_edge_fields(
    source: &ResolvedSource,
    target: &ResolvedSource,
    path: &CorrelatedPathPlan,
) -> CapabilityResult<Vec<ProjectField>> {
    let source_version = version_witness_fields(&source.row_shape)?;
    let target_version = version_witness_fields(&target.row_shape)?;
    Ok(vec![
        ProjectField::literal(
            "source_source",
            Value::String(source.row_shape.source.table.clone()),
        ),
        ProjectField::literal(
            "source_table",
            Value::String(source.table_schema.name.clone()),
        ),
        ProjectField::renamed(left_field(&source.row_shape.row_uuid_field), "source_row"),
        ProjectField::renamed(left_field(&source_version.tx_time_field), "source_tx_time"),
        ProjectField::renamed(
            left_field(&source_version.tx_node_field),
            "source_tx_node_id",
        ),
        ProjectField::literal("path", Value::String(correlated_relation_name(path))),
        ProjectField::literal("kind", Value::String("array_subquery".to_owned())),
        ProjectField::literal("role", Value::String("terminal".to_owned())),
        ProjectField::literal(
            "target_source",
            Value::String(target.row_shape.source.table.clone()),
        ),
        ProjectField::literal(
            "target_table",
            Value::String(target.table_schema.name.clone()),
        ),
        ProjectField::renamed(right_field(&target.row_shape.row_uuid_field), "target_row"),
        ProjectField::renamed(right_field(&target_version.tx_time_field), "target_tx_time"),
        ProjectField::renamed(
            right_field(&target_version.tx_node_field),
            "target_tx_node_id",
        ),
    ])
}

fn correlated_relation_name(path: &CorrelatedPathPlan) -> String {
    path.path
        .child
        .path
        .components
        .iter()
        .rev()
        .find_map(|role| match role {
            SourceRole::CorrelatedChild(name) => Some(
                name.split_once(':')
                    .map_or(name.as_str(), |(_, tail)| tail)
                    .to_owned(),
            ),
            _ => None,
        })
        .unwrap_or_else(|| path.path.child.table.clone())
}

fn deletion_witness_graph_for_current_register(
    source: &ResolvedSource,
    event_kind: &str,
) -> CapabilityResult<GraphBuilder> {
    let Some(register) = &source.deletion_register else {
        return Err(Box::new(CapabilityReport {
            gaps: vec![UnsupportedReason::Runtime(
                "resolved source did not provide deletion register source".to_owned(),
            )],
            explain: ExplainPlan::default(),
        }));
    };
    Ok(register
        .graph
        .clone()
        .project_fields(deletion_witness_fields_for_tagged_rows(source, event_kind)?))
}

fn content_version_witness_graph(
    source: &ResolvedSource,
    event_kind: &str,
) -> CapabilityResult<GraphBuilder> {
    let Some(content_version) = &source.content_version else {
        return Ok(source.graph.clone().project_fields(
            inline_version_witness_fields_for_tagged_rows(source, event_kind)?,
        ));
    };
    let version = version_witness_fields(&source.row_shape)?;
    Ok(GraphBuilder::semi_join(
        content_version.graph.clone(),
        source.graph.clone(),
        ["row_uuid", "tx_time", "tx_node_id"],
        [
            source.row_shape.row_uuid_field.clone(),
            version.tx_time_field.clone(),
            version.tx_node_field.clone(),
        ],
    )
    .project_fields(unprefixed_version_witness_fields_for_tagged_rows(
        source, event_kind,
    )?))
}

fn result_membership_fields(
    source: &ResolvedSource,
    routing_param_fields: BTreeSet<String>,
) -> CapabilityResult<Vec<ProjectField>> {
    let version = version_witness_fields(&source.row_shape)?;
    let settle_position = settle_position_field(&source.row_shape);
    let mut fields = vec![
        ProjectField::literal("event_kind", Value::String("result_current".to_owned())),
        ProjectField::literal(
            "table_name",
            Value::String(source.table_schema.name.clone()),
        ),
        ProjectField::named(source.row_shape.row_uuid_field.clone()),
        ProjectField::renamed(version.tx_time_field, "content_tx_time"),
        ProjectField::renamed(version.tx_node_field, "content_tx_node_id"),
    ];
    if let Some(field) = settle_position {
        fields.push(ProjectField::renamed(field, "settle_position"));
    } else {
        fields.push(ProjectField::null_typed(
            "settle_position",
            ValueType::Nullable(Box::new(ValueType::U64)),
        ));
    }
    fields.extend(routing_param_fields.into_iter().map(ProjectField::named));
    Ok(fields)
}

fn aggregate_app_row_descriptor(
    plan: &AnalyzedQueryPlan,
    source: &ResolvedSource,
) -> CapabilityResult<RecordDescriptor> {
    let (group_by, outputs) = root_aggregate_step(plan).ok_or_else(|| {
        Box::new(CapabilityReport {
            gaps: vec![UnsupportedReason::Runtime(
                "aggregate app row descriptor requested for non-aggregate plan".to_owned(),
            )],
            explain: ExplainPlan::default(),
        })
    })?;
    let mut fields = Vec::new();
    for value in group_by {
        let field = aggregate_source_field_name(value, source)?;
        let value_type = source_field_type(source, &field).cloned().ok_or_else(|| {
            Box::new(CapabilityReport {
                gaps: vec![UnsupportedReason::Runtime(format!(
                    "aggregate group field {field:?} is missing from resolved descriptor"
                ))],
                explain: ExplainPlan::default(),
            })
        })?;
        fields.push((field, value_type));
    }
    fields.extend(
        outputs
            .iter()
            .map(|output| {
                Ok((
                    logical_user_column(&output.output.name).to_owned(),
                    aggregate_output_value_type(output, source)?,
                ))
            })
            .collect::<CapabilityResult<Vec<_>>>()?,
    );
    Ok(RecordDescriptor::new(fields))
}

fn aggregate_result_schema(
    plan: &AnalyzedQueryPlan,
    source: &ResolvedSource,
    routing_param_fields: BTreeSet<String>,
) -> CapabilityResult<AggregateResultSchema> {
    let (group_by, outputs) = root_aggregate_step(plan).ok_or_else(|| {
        Box::new(CapabilityReport {
            gaps: vec![UnsupportedReason::Runtime(
                "aggregate result schema requested for non-aggregate plan".to_owned(),
            )],
            explain: ExplainPlan::default(),
        })
    })?;
    let group_key_fields = group_by
        .iter()
        .map(|value| aggregate_typed_group_field(value, source))
        .collect::<CapabilityResult<Vec<_>>>()?;
    Ok(AggregateResultSchema {
        synthetic: SyntheticResultMembershipSchema {
            table_field: "table_name".to_owned(),
            row_field: "synthetic_row".to_owned(),
            revision_field: "synthetic_revision".to_owned(),
            routing_param_fields: routing_param_fields.clone(),
        },
        group_key_fields,
        value_fields: outputs
            .iter()
            .map(|output| aggregate_typed_output_field(output, source))
            .collect::<CapabilityResult<Vec<_>>>()?,
        routing_param_fields,
    })
}

fn aggregate_result_membership_fields(
    plan: &AnalyzedQueryPlan,
    source: &ResolvedSource,
    routing_param_fields: BTreeSet<String>,
) -> CapabilityResult<Vec<ProjectField>> {
    let (group_by, outputs) = root_aggregate_step(plan).ok_or_else(|| {
        Box::new(CapabilityReport {
            gaps: vec![UnsupportedReason::Runtime(
                "aggregate result fields requested for non-aggregate plan".to_owned(),
            )],
            explain: ExplainPlan::default(),
        })
    })?;
    if group_by.len() > 1 {
        return Err(Box::new(CapabilityReport {
            gaps: vec![UnsupportedReason::Operator(
                "multi-column aggregate group result identity is not lowered yet".to_owned(),
            )],
            explain: ExplainPlan::default(),
        }));
    }
    let mut fields = vec![ProjectField::literal(
        "table_name",
        Value::String(format!("{}_aggregate", source.table_schema.name)),
    )];
    if let Some(group) = group_by.first() {
        fields.push(ProjectField::renamed(
            aggregate_source_field_name(group, source)?,
            "synthetic_row",
        ));
    } else {
        fields.push(ProjectField::literal(
            "synthetic_row",
            Value::String("global".to_owned()),
        ));
    }
    if let Some(first_output) = outputs.first() {
        fields.push(ProjectField::renamed(
            logical_user_column(&first_output.output.name),
            "synthetic_revision",
        ));
    } else {
        fields.push(ProjectField::literal(
            "synthetic_revision",
            Value::String("empty".to_owned()),
        ));
    }
    for group in group_by {
        let field = aggregate_source_field_name(group, source)?;
        fields.push(ProjectField::named(field));
    }
    fields.extend(
        outputs
            .iter()
            .map(|output| ProjectField::named(logical_user_column(&output.output.name))),
    );
    fields.extend(routing_param_fields.into_iter().map(ProjectField::named));
    Ok(fields)
}

fn aggregate_typed_group_field(
    value: &NormalizedValueRef,
    source: &ResolvedSource,
) -> CapabilityResult<TypedOutputField> {
    let field = aggregate_source_field_name(value, source)?;
    let value_type = source_field_type(source, &field).cloned().ok_or_else(|| {
        Box::new(CapabilityReport {
            gaps: vec![UnsupportedReason::Runtime(format!(
                "aggregate group field {field:?} is missing from resolved descriptor"
            ))],
            explain: ExplainPlan::default(),
        })
    })?;
    Ok(TypedOutputField {
        name: field,
        ty: column_type_from_value_type(&value_type),
    })
}

fn aggregate_typed_output_field(
    output: &AggregateExpr,
    source: &ResolvedSource,
) -> CapabilityResult<TypedOutputField> {
    Ok(TypedOutputField {
        name: logical_user_column(&output.output.name).to_owned(),
        ty: column_type_from_value_type(&aggregate_output_value_type(output, source)?),
    })
}

fn aggregate_output_value_type(
    output: &AggregateExpr,
    source: &ResolvedSource,
) -> CapabilityResult<ValueType> {
    match output.function {
        AggregateFunction::Count => Ok(ValueType::U64),
        AggregateFunction::Avg => Ok(ValueType::F64),
        AggregateFunction::Sum | AggregateFunction::Min | AggregateFunction::Max => {
            let input = output.input.as_ref().ok_or_else(|| {
                Box::new(CapabilityReport {
                    gaps: vec![UnsupportedReason::Operator(
                        "aggregate input is required for sum/min/max".to_owned(),
                    )],
                    explain: ExplainPlan::default(),
                })
            })?;
            let field = aggregate_source_field_name(input, source)?;
            source_field_type(source, &field).cloned().ok_or_else(|| {
                Box::new(CapabilityReport {
                    gaps: vec![UnsupportedReason::Runtime(format!(
                        "aggregate input field {field:?} is missing from resolved descriptor"
                    ))],
                    explain: ExplainPlan::default(),
                })
            })
        }
    }
}

fn aggregate_source_field_name(
    value: &NormalizedValueRef,
    source: &ResolvedSource,
) -> CapabilityResult<String> {
    match value {
        NormalizedValueRef::SourceField {
            source: value_source,
            field,
        } if value_source == &source.row_shape.source => {
            require_source_field(source, &user_column_field(field)).map_err(|gap| {
                Box::new(CapabilityReport {
                    gaps: vec![gap],
                    explain: ExplainPlan::default(),
                })
            })
        }
        NormalizedValueRef::RowId(RowIdRef::Source(value_source))
            if value_source == &source.row_shape.source =>
        {
            Ok(source.row_shape.row_uuid_field.clone())
        }
        _ => Err(Box::new(CapabilityReport {
            gaps: vec![UnsupportedReason::Operator(
                "aggregate group keys must be root source fields".to_owned(),
            )],
            explain: ExplainPlan::default(),
        })),
    }
}

fn version_witness_fields_for_tagged_rows(
    source: &ResolvedSource,
    event_kind: &str,
) -> CapabilityResult<Vec<ProjectField>> {
    prefixed_version_witness_fields_for_tagged_rows(source, event_kind, "right.")
}

fn unprefixed_version_witness_fields_for_tagged_rows(
    source: &ResolvedSource,
    event_kind: &str,
) -> CapabilityResult<Vec<ProjectField>> {
    prefixed_version_witness_fields_for_tagged_rows(source, event_kind, "")
}

fn prefixed_version_witness_fields_for_tagged_rows(
    source: &ResolvedSource,
    event_kind: &str,
    prefix: &str,
) -> CapabilityResult<Vec<ProjectField>> {
    if source.content_version.is_none() {
        return Err(Box::new(CapabilityReport {
            gaps: vec![UnsupportedReason::Runtime(
                "resolved source did not provide content version source".to_owned(),
            )],
            explain: ExplainPlan::default(),
        }));
    };
    let mut fields = vec![
        ProjectField::literal("event_kind", Value::String(event_kind.to_owned())),
        ProjectField::literal(
            "table_name",
            Value::String(source.table_schema.name.clone()),
        ),
        ProjectField::renamed(format!("{prefix}row_uuid"), "row_uuid"),
        ProjectField::renamed(format!("{prefix}tx_time"), "content_tx_time"),
        ProjectField::renamed(format!("{prefix}tx_node_id"), "content_tx_node_id"),
        ProjectField::renamed(format!("{prefix}tx_time"), "tx_time"),
        ProjectField::renamed(format!("{prefix}tx_node_id"), "tx_node_id"),
        ProjectField::renamed(format!("{prefix}schema_version"), "schema_version"),
        ProjectField::renamed(format!("{prefix}parents"), "parents"),
        ProjectField::renamed(format!("{prefix}created_by"), "created_by"),
        ProjectField::renamed(format!("{prefix}created_at"), "created_at"),
        ProjectField::renamed(format!("{prefix}updated_by"), "updated_by"),
        ProjectField::renamed(format!("{prefix}updated_at"), "updated_at"),
        ProjectField::null_typed("_deletion", ValueType::Nullable(Box::new(ValueType::U8))),
    ];
    fields.extend(source.table_schema.columns.iter().map(|column| {
        ProjectField::renamed(
            format!("{prefix}{}", user_column_field(&column.name)),
            table_user_column_field(&source.table_schema.name, &column.name),
        )
    }));
    Ok(fields)
}

fn inline_version_witness_fields_for_tagged_rows(
    source: &ResolvedSource,
    event_kind: &str,
) -> CapabilityResult<Vec<ProjectField>> {
    let version = version_witness_fields(&source.row_shape)?;
    let mut fields = vec![
        ProjectField::literal("event_kind", Value::String(event_kind.to_owned())),
        ProjectField::literal(
            "table_name",
            Value::String(source.table_schema.name.clone()),
        ),
        ProjectField::renamed(source.row_shape.row_uuid_field.clone(), "row_uuid"),
        ProjectField::renamed(version.tx_time_field.clone(), "content_tx_time"),
        ProjectField::renamed(version.tx_node_field.clone(), "content_tx_node_id"),
        ProjectField::renamed(version.tx_time_field, "tx_time"),
        ProjectField::renamed(version.tx_node_field, "tx_node_id"),
        ProjectField::renamed(version.schema_version_field, "schema_version"),
        ProjectField::named("parents"),
        ProjectField::named("created_by"),
        ProjectField::named("created_at"),
        ProjectField::named("updated_by"),
        ProjectField::named("updated_at"),
        ProjectField::null_typed("_deletion", ValueType::Nullable(Box::new(ValueType::U8))),
    ];
    fields.extend(source.table_schema.columns.iter().map(|column| {
        ProjectField::renamed(
            user_column_field(&column.name),
            table_user_column_field(&source.table_schema.name, &column.name),
        )
    }));
    Ok(fields)
}

fn deletion_witness_fields_for_tagged_rows(
    source: &ResolvedSource,
    event_kind: &str,
) -> CapabilityResult<Vec<ProjectField>> {
    let mut fields = vec![
        ProjectField::literal("event_kind", Value::String(event_kind.to_owned())),
        ProjectField::literal(
            "table_name",
            Value::String(source.table_schema.name.clone()),
        ),
        ProjectField::named(source.row_shape.row_uuid_field.clone()),
        ProjectField::renamed("tx_time", "content_tx_time"),
        ProjectField::renamed("tx_node_id", "content_tx_node_id"),
        ProjectField::named("tx_time"),
        ProjectField::named("tx_node_id"),
        ProjectField::named("schema_version"),
        ProjectField::named("parents"),
        ProjectField::named("created_by"),
        ProjectField::named("created_at"),
        ProjectField::named("updated_by"),
        ProjectField::named("updated_at"),
        ProjectField::nullable("_deletion", "_deletion"),
    ];
    fields.extend(source.table_schema.columns.iter().map(|column| {
        ProjectField::null_typed(
            table_user_column_field(&source.table_schema.name, &column.name),
            ValueType::Nullable(Box::new(column.column_type.clone().value_type())),
        )
    }));
    Ok(fields)
}

fn relation_edge_schema(
    plan: &AnalyzedQueryPlan,
    root_source: &ResolvedSource,
    resolved_sources: &BTreeMap<SourceId, ResolvedSource>,
) -> CapabilityResult<RelationEdgeSchema> {
    let (source, target, depth_field) = match plan {
        AnalyzedQueryPlan::CorrelatedPath(path) => {
            let child = resolved_sources.get(&path.path.child).ok_or_else(|| {
                Box::new(CapabilityReport {
                    gaps: vec![UnsupportedReason::Runtime(format!(
                        "path child source {:?} was not resolved",
                        path.path.child
                    ))],
                    explain: ExplainPlan::default(),
                })
            })?;
            return Ok(RelationEdgeSchema {
                source: prefixed_versioned_row_ref_schema(root_source, "source")?,
                path_field: "path".to_owned(),
                target: prefixed_versioned_row_ref_schema(child, "target")?,
                kind_field: "kind".to_owned(),
                depth_field: None,
                edge_id_field: None,
                branch_field: None,
                role_field: Some("role".to_owned()),
                order_field: None,
                hole_state_field: None,
            });
        }
        AnalyzedQueryPlan::RecursiveRelation(relation) => {
            let step_source = relation
                .step
                .root
                .source()
                .cloned()
                .or_else(|| first_step_source(&relation.step.steps).cloned())
                .ok_or_else(|| {
                    Box::new(CapabilityReport {
                        gaps: vec![UnsupportedReason::Runtime(
                            "recursive step source was not resolved".to_owned(),
                        )],
                        explain: ExplainPlan::default(),
                    })
                })?;
            let step = resolved_sources.get(&step_source).ok_or_else(|| {
                Box::new(CapabilityReport {
                    gaps: vec![UnsupportedReason::Runtime(format!(
                        "recursive step source {:?} was not resolved",
                        step_source
                    ))],
                    explain: ExplainPlan::default(),
                })
            })?;
            (root_source, step, Some("depth".to_owned()))
        }
        AnalyzedQueryPlan::Linear(_) | AnalyzedQueryPlan::Union(_) => {
            return Err(Box::new(CapabilityReport {
                gaps: vec![UnsupportedReason::Output(Box::new(
                    ProgramFactKey::RelationEdges,
                ))],
                explain: ExplainPlan {
                    capabilities: vec![
                        "relation edge facts require a path or recursive relation node".to_owned(),
                    ],
                    ..ExplainPlan::default()
                },
            }));
        }
    };

    Ok(RelationEdgeSchema {
        source: versioned_row_ref_schema(source)?,
        path_field: "path".to_owned(),
        target: versioned_row_ref_schema(target)?,
        kind_field: "kind".to_owned(),
        depth_field,
        edge_id_field: None,
        branch_field: None,
        role_field: Some("role".to_owned()),
        order_field: None,
        hole_state_field: None,
    })
}

fn path_correlation_coverage_schema(
    plan: &AnalyzedQueryPlan,
    root_source: &ResolvedSource,
    _resolved_sources: &BTreeMap<SourceId, ResolvedSource>,
) -> CapabilityResult<PathCorrelationCoverageSchema> {
    match plan {
        AnalyzedQueryPlan::CorrelatedPath(path) => {
            let expected_count_field = match path.requirement {
                CorrelationRequirement::MatchCorrelationCardinality => {
                    Some("expected_count".to_owned())
                }
                CorrelationRequirement::Optional | CorrelationRequirement::AtLeastOne => None,
            };
            Ok(PathCorrelationCoverageSchema {
                parent: versioned_row_ref_schema(root_source)?,
                path_field: "path".to_owned(),
                correlation_field: "correlation".to_owned(),
                expected_count_field,
                readable_count_field: "readable_count".to_owned(),
                coverage_state_field: "coverage_state".to_owned(),
            })
        }
        AnalyzedQueryPlan::RecursiveRelation(_) => Ok(PathCorrelationCoverageSchema {
            parent: versioned_row_ref_schema(root_source)?,
            path_field: "path".to_owned(),
            correlation_field: "frontier".to_owned(),
            expected_count_field: None,
            readable_count_field: "readable_count".to_owned(),
            coverage_state_field: "coverage_state".to_owned(),
        }),
        AnalyzedQueryPlan::Linear(_) | AnalyzedQueryPlan::Union(_) => {
            Err(Box::new(CapabilityReport {
                gaps: vec![UnsupportedReason::Output(Box::new(
                    ProgramFactKey::PathCorrelationCoverage,
                ))],
                explain: ExplainPlan {
                    capabilities: vec![
                        "path correlation coverage facts require a path or recursive relation node"
                            .to_owned(),
                    ],
                    ..ExplainPlan::default()
                },
            }))
        }
    }
}

fn versioned_row_ref_schema(source: &ResolvedSource) -> CapabilityResult<VersionedRowRefSchema> {
    let version = version_witness_fields(&source.row_shape)?;
    Ok(VersionedRowRefSchema {
        row: RowRefSchema {
            source_field: "source".to_owned(),
            table_field: "table".to_owned(),
            row_field: source.row_shape.row_uuid_field.clone(),
        },
        version: Some(content_version_schema(&version)),
    })
}

fn prefixed_versioned_row_ref_schema(
    _source: &ResolvedSource,
    prefix: &str,
) -> CapabilityResult<VersionedRowRefSchema> {
    Ok(VersionedRowRefSchema {
        row: RowRefSchema {
            source_field: format!("{prefix}_source"),
            table_field: format!("{prefix}_table"),
            row_field: format!("{prefix}_row"),
        },
        version: Some(ResultMembershipVersionSchema::Content(
            ContentVersionFields {
                tx_time_field: format!("{prefix}_tx_time"),
                tx_node_field: format!("{prefix}_tx_node_id"),
            },
        )),
    })
}

fn content_version_schema(version: &VersionWitnessFieldRefs) -> ResultMembershipVersionSchema {
    ResultMembershipVersionSchema::Content(ContentVersionFields {
        tx_time_field: version.tx_time_field.clone(),
        tx_node_field: version.tx_node_field.clone(),
    })
}

fn version_witness_schema(
    source: &ResolvedSource,
    version: &VersionWitnessFieldRefs,
) -> VersionWitnessSchema {
    VersionWitnessSchema {
        descriptor: source.row_shape.descriptor,
        identity: VersionIdentityFields {
            table_field: "table_name".to_owned(),
            row_field: source.row_shape.row_uuid_field.clone(),
            tx_time_field: "tx_time".to_owned(),
            tx_node_field: "tx_node_id".to_owned(),
            batch_id_field: None,
            branch_or_prefix_field: version.branch_or_prefix_field.clone(),
            row_digest_field: None,
            schema_field: "schema_version".to_owned(),
            layer_field: "layer".to_owned(),
        },
        created_by_field: "created_by".to_owned(),
        created_at_field: "created_at".to_owned(),
        updated_by_field: "updated_by".to_owned(),
        updated_at_field: "updated_at".to_owned(),
        parents_field: "parents".to_owned(),
        deletion_field: "_deletion".to_owned(),
        user_fields: source
            .table_schema
            .columns
            .iter()
            .map(|column| {
                (
                    column.name.clone(),
                    table_user_column_field(&source.table_schema.name, &column.name),
                )
            })
            .collect(),
    }
}

#[derive(Clone, Debug)]
struct VersionWitnessFieldRefs {
    schema_version_field: String,
    tx_time_field: String,
    tx_node_field: String,
    branch_or_prefix_field: Option<String>,
}

#[derive(Clone, Debug)]
struct CoverageFieldRefs {
    coverage_field: String,
}

fn version_witness_fields(row_shape: &SourceRowShape) -> CapabilityResult<VersionWitnessFieldRefs> {
    match row_shape
        .metadata
        .get(&SourceMetadataRequirement::VersionWitnesses)
    {
        Some(SourceMetadataFields::VersionWitnesses {
            schema_version_field,
            tx_time_field,
            tx_node_field,
            branch_or_prefix_field,
        }) => Ok(VersionWitnessFieldRefs {
            schema_version_field: schema_version_field.clone(),
            tx_time_field: tx_time_field.clone(),
            tx_node_field: tx_node_field.clone(),
            branch_or_prefix_field: branch_or_prefix_field.clone(),
        }),
        _ => Err(Box::new(CapabilityReport {
            gaps: vec![UnsupportedReason::Runtime(
                "resolved source did not provide version witness fields".to_owned(),
            )],
            explain: ExplainPlan::default(),
        })),
    }
}

fn settle_position_field(row_shape: &SourceRowShape) -> Option<String> {
    match row_shape
        .metadata
        .get(&SourceMetadataRequirement::SettlePosition)
    {
        Some(SourceMetadataFields::SettlePosition {
            settle_position_field,
        }) => Some(settle_position_field.clone()),
        _ => None,
    }
}

fn coverage_fields(row_shape: &SourceRowShape) -> CapabilityResult<CoverageFieldRefs> {
    match row_shape.metadata.get(&SourceMetadataRequirement::Coverage) {
        Some(SourceMetadataFields::Coverage { coverage_field }) => Ok(CoverageFieldRefs {
            coverage_field: coverage_field.clone(),
        }),
        _ => Err(Box::new(CapabilityReport {
            gaps: vec![UnsupportedReason::Runtime(
                "resolved source did not provide coverage fields".to_owned(),
            )],
            explain: ExplainPlan::default(),
        })),
    }
}

fn hidden_source_fields(row_shape: &SourceRowShape) -> BTreeSet<String> {
    let mut fields = BTreeSet::new();
    for metadata in row_shape.metadata.values() {
        match metadata {
            SourceMetadataFields::VersionWitnesses {
                schema_version_field,
                tx_time_field,
                tx_node_field,
                branch_or_prefix_field,
            } => {
                fields.insert(schema_version_field.clone());
                fields.insert(tx_time_field.clone());
                fields.insert(tx_node_field.clone());
                fields.extend(branch_or_prefix_field.clone());
            }
            SourceMetadataFields::DeletionMarkers {
                deletion_state_field,
                deletion_tx_time_field,
                deletion_tx_node_field,
            } => {
                fields.insert(deletion_state_field.clone());
                fields.extend(deletion_tx_time_field.clone());
                fields.extend(deletion_tx_node_field.clone());
            }
            SourceMetadataFields::BatchMembership {
                batch_id_field,
                branch_or_prefix_field,
                row_digest_field,
                batch_kind_field,
            } => {
                fields.insert(batch_id_field.clone());
                fields.extend(branch_or_prefix_field.clone());
                fields.insert(row_digest_field.clone());
                fields.insert(batch_kind_field.clone());
            }
            SourceMetadataFields::Coverage { coverage_field } => {
                fields.insert(coverage_field.clone());
            }
            SourceMetadataFields::SettlePosition {
                settle_position_field,
            } => {
                fields.insert(settle_position_field.clone());
            }
            SourceMetadataFields::ValidationReads { snapshot_field } => {
                fields.insert(snapshot_field.clone());
            }
            SourceMetadataFields::PolicyWitnesses {
                policy_path_field,
                edge_kind_field,
            } => {
                fields.insert(policy_path_field.clone());
                fields.insert(edge_kind_field.clone());
            }
            SourceMetadataFields::Provenance { field } => {
                fields.insert(field.clone());
            }
        }
    }
    fields
}

/// Runnable lowered query program.
#[derive(Clone, Debug)]
pub(crate) struct QueryProgram {
    /// Original request.
    pub(crate) request: QueryProgramRequest,
    /// Groove graph and its boundary contracts.
    pub(crate) lowered: LoweredGraph,
    /// Human-readable debugging and test artifact.
    pub(crate) explain: ExplainPlan,
}

/// Groove graph plus the semantic contracts needed to consume it.
#[derive(Clone, Debug)]
pub(crate) struct LoweredGraph {
    /// Executable named groove terminals emitted by this program.
    pub(crate) terminals: Vec<LoweredTerminal>,
    /// Parameter domains expected by the graph.
    pub(crate) parameters: ParameterDomain,
    /// App row and fact schemas emitted by the graph.
    pub(crate) output: ProgramOutputSchemas,
    /// Table schemas needed to decode maintained fact terminals emitted by this
    /// lowered program. This is derived from resolved query-engine sources, not
    /// recollected from the public query shape.
    pub(crate) maintained_terminal_tables: BTreeMap<String, TableSchema>,
}

/// One executable output terminal produced by query lowering.
#[derive(Clone, Debug)]
pub(crate) struct LoweredTerminal {
    /// Stable sink name for the terminal.
    pub(crate) sink: String,
    /// Executable groove graph for this terminal.
    pub(crate) graph: GraphBuilder,
    /// Typed terminal output contract.
    pub(crate) output: OutputTerminalSchema,
}
