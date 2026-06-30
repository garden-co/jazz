use super::*;
use groove::ivm::{
    LiteralValue, PredicateExpr as GroovePredicateExpr, PredicateKind, ProjectField, TopByOrder,
};
use groove::records::ValueType;

/// Parameter domains attached to one lowered graph.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub(crate) struct ParameterDomain {
    /// User-supplied binding parameters.
    pub(crate) user_params: BTreeMap<String, ColumnType>,
    /// Server-derived hidden parameters such as claims.
    pub(crate) hidden_params: BTreeMap<String, ColumnType>,
    /// Parameters retained in terminal rows for usage-site routing.
    pub(crate) routing_params: BTreeSet<String>,
}

/// Result of lowering one query program.
pub(crate) type QueryCompileResult = CapabilityResult<QueryProgram>;

/// Lower one Jazz query program into the unified Groove-backed program.
pub(crate) fn lower_query_program(
    request: QueryProgramRequest,
    source_resolver: &mut impl SourceResolver,
) -> QueryCompileResult {
    let mut explain = ExplainPlan {
        input: format!("{:?}", request.input),
        read: vec![format!("{:?}", request.reads)],
        policy: vec![format!("{:?}", request.policy)],
        output: vec![format!("{:?}", request.output)],
        capabilities: Vec::new(),
        physical: Vec::new(),
    };

    let plan = match analyze_query_plan(&request) {
        Ok(plan) => plan,
        Err(gaps) => {
            explain
                .capabilities
                .push("only current-source row-set lowering is implemented".to_owned());
            return Err(Box::new(CapabilityReport { gaps, explain }));
        }
    };

    let source_requirements = source_requirements(&request.output, &plan)?;
    let mut resolved_sources = BTreeMap::new();
    for (source, requirements) in source_requirements {
        let source_request = SourceRequest {
            source: source.clone(),
            visibility: RowVisibility::Visible,
            requirements,
        };
        let resolved_source = source_resolver
            .resolve_source(&source_request)
            .map_err(|err| {
                Box::new(CapabilityReport {
                    gaps: vec![UnsupportedReason::Source(err.gap)],
                    explain: explain.clone(),
                })
            })?;
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
                explain: explain.clone(),
            })
        })?;
    explain
        .capabilities
        .push(plan.capability_label().to_owned());
    let graph = lower_plan_steps(
        resolved_root.graph.clone(),
        &plan,
        &resolved_root,
        &resolved_sources,
        &request,
    )
    .map_err(|gap| {
        Box::new(CapabilityReport {
            gaps: vec![gap],
            explain: explain.clone(),
        })
    })?;

    Ok(QueryProgram {
        lowered: LoweredGraph {
            graph,
            parameters: parameter_domain(&request.input.shape),
            output: ProgramOutputSchemas::RowSet(output_terminals(
                &request.output,
                &plan,
                &resolved_root,
                &resolved_sources,
            )?),
        },
        request,
        explain,
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
                        domain.routing_params.insert(param.clone());
                    }
                }
            }
            RowSetExpr::ValueSource { .. }
            | RowSetExpr::FrontierSource { .. }
            | RowSetExpr::Source { .. }
            | RowSetExpr::Filter { .. }
            | RowSetExpr::Join { .. }
            | RowSetExpr::RecursiveRelation { .. }
            | RowSetExpr::Union { .. }
            | RowSetExpr::Distinct { .. }
            | RowSetExpr::Project { .. }
            | RowSetExpr::CorrelatedPathProjection { .. }
            | RowSetExpr::OrderBy { .. }
            | RowSetExpr::Slice { .. }
            | RowSetExpr::Aggregate { .. } => {}
        }
    }
    domain
}

#[derive(Clone, Debug)]
struct LinearCurrentRoot {
    root: LinearRoot,
    steps: Vec<LinearStep>,
}

#[derive(Clone, Debug)]
enum LinearRoot {
    Source(SourceId),
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
            LinearRoot::Source(source) => Some(source),
            LinearRoot::Value { .. } | LinearRoot::Frontier { .. } => None,
        }
    }
}

#[derive(Clone, Debug)]
enum AnalyzedQueryPlan {
    Linear(LinearCurrentRoot),
    CorrelatedPath(CorrelatedPathPlan),
    RecursiveRelation(RecursiveRelationPlan),
}

impl AnalyzedQueryPlan {
    fn root_source(&self) -> &SourceId {
        match self {
            AnalyzedQueryPlan::Linear(plan) => plan.root.source().expect("linear root source"),
            AnalyzedQueryPlan::CorrelatedPath(plan) => {
                plan.parent.root.source().expect("path parent source")
            }
            AnalyzedQueryPlan::RecursiveRelation(plan) => plan
                .seed
                .root
                .source()
                .or_else(|| plan.step.root.source())
                .or_else(|| first_step_source(&plan.step.steps))
                .expect("recursive source"),
        }
    }

    fn capability_label(&self) -> &'static str {
        match self {
            AnalyzedQueryPlan::Linear(_) => "table-rooted current lowering",
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
        | LinearStep::Slice { .. } => None,
    })
}

#[derive(Clone, Debug)]
struct CorrelatedPathPlan {
    parent: LinearCurrentRoot,
    child: LinearCurrentRoot,
    path: ProgramPathId,
    correlation: PredicateExpr,
    requirement: CorrelationRequirement,
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

impl RecursiveRelationPlan {
    fn root_source(&self) -> Option<&SourceId> {
        self.seed
            .root
            .source()
            .or_else(|| self.step.root.source())
            .or_else(|| first_step_source(&self.step.steps))
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
    Recursive(RecursiveRelationPlan),
}

impl RelationInputPlan {
    fn root_source(&self) -> Option<&SourceId> {
        match self {
            RelationInputPlan::Linear(linear) => linear.root.source(),
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
}

fn analyze_query_plan(
    request: &QueryProgramRequest,
) -> Result<AnalyzedQueryPlan, Vec<UnsupportedReason>> {
    let mut gaps = Vec::new();

    if !request.reads.fact_reads.is_empty() {
        gaps.push(UnsupportedReason::Source(SourceGap::TransactionReadOverlay));
    }
    if !matches!(request.policy, PolicyContext::System) {
        gaps.push(UnsupportedReason::Policy(
            "policy augmentation is not lowered yet".to_owned(),
        ));
    }

    let analyzed = analyze_root_node(request);
    let Ok(plan) = analyzed else {
        gaps.push(analyzed.unwrap_err());
        return Err(gaps);
    };

    for plan_source in analyzed_plan_sources(&plan) {
        let read_source = request.reads.primary.sources.get(&plan_source);
        let Some(SourceExpr::VisibleCurrent {
            projection,
            data: DataSource::Current,
            tier: _,
        }) = read_source
        else {
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
            let child =
                analyze_linear_subplan(child_input, &request.input.shape.nodes, &mut visited)?;
            validate_result_source(
                request,
                parent.root.source().ok_or_else(|| {
                    UnsupportedReason::Operator("path parent must be a source".to_owned())
                })?,
            )?;
            AnalyzedQueryPlan::CorrelatedPath(CorrelatedPathPlan {
                parent,
                child,
                path: path.clone(),
                correlation: correlation.clone(),
                requirement: *requirement,
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
        _ => {
            let linear = analyze_linear_root(&request.input.shape.root, request, &mut visited)?;
            validate_result_source(
                request,
                linear.root.source().ok_or_else(|| {
                    UnsupportedReason::Operator("result must be the root source row".to_owned())
                })?,
            )?;
            AnalyzedQueryPlan::Linear(linear)
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

fn analyzed_plan_sources(plan: &AnalyzedQueryPlan) -> BTreeSet<SourceId> {
    match plan {
        AnalyzedQueryPlan::Linear(linear) => linear_plan_sources(linear),
        AnalyzedQueryPlan::CorrelatedPath(path) => {
            let mut sources = linear_plan_sources(&path.parent);
            sources.extend(linear_plan_sources(&path.child));
            sources
        }
        AnalyzedQueryPlan::RecursiveRelation(relation) => {
            let mut sources = linear_plan_sources(&relation.seed);
            sources.extend(linear_plan_sources(&relation.step));
            sources
        }
    }
}

fn linear_plan_sources(plan: &LinearCurrentRoot) -> BTreeSet<SourceId> {
    let mut sources = plan
        .root
        .source()
        .cloned()
        .into_iter()
        .collect::<BTreeSet<_>>();
    sources.extend(step_sources(&plan.steps));
    sources
}

fn relation_plan_sources(plan: &RelationInputPlan) -> BTreeSet<SourceId> {
    match plan {
        RelationInputPlan::Linear(linear) => linear_plan_sources(linear),
        RelationInputPlan::Recursive(relation) => {
            let mut sources = linear_plan_sources(&relation.seed);
            sources.extend(linear_plan_sources(&relation.step));
            sources
        }
    }
}

fn step_sources(steps: &[LinearStep]) -> BTreeSet<SourceId> {
    let mut sources = BTreeSet::new();
    for step in steps {
        if let LinearStep::Join { right, .. } = step {
            sources.extend(relation_plan_sources(right));
        }
    }
    sources
}

fn source_current_tier(request: &QueryProgramRequest, source: &SourceId) -> Option<DurabilityTier> {
    match request.reads.primary.sources.get(source) {
        Some(SourceExpr::VisibleCurrent {
            data: DataSource::Current,
            tier,
            ..
        }) => Some(*tier),
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
            "row-set node {:?} participates in a cycle",
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
        RowSetExpr::Source { source, visibility } => {
            if *visibility != RowVisibility::Visible {
                return Err(UnsupportedReason::Operator(
                    "include-deleted roots are not lowered yet".to_owned(),
                ));
            }
            Ok((LinearRoot::Source(source.clone()), Vec::new()))
        }
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
        RowSetExpr::CorrelatedPathProjection { .. } => Err(UnsupportedReason::Operator(
            "correlated path projection row-set nodes are not lowered yet".to_owned(),
        )),
        RowSetExpr::Aggregate { .. } => Err(UnsupportedReason::Operator(
            "aggregate row-set nodes are not lowered yet".to_owned(),
        )),
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
                    "row-set node {:?} participates in a cycle",
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

fn validate_join_relation(plan: &LinearCurrentRoot) -> Result<(), UnsupportedReason> {
    for step in &plan.steps {
        match step {
            LinearStep::Filter(_) | LinearStep::Join { .. } | LinearStep::Project(_) => {}
            LinearStep::OrderBy(_) | LinearStep::Slice { .. } => {
                return Err(UnsupportedReason::Operator(
                    "join inputs do not support order/slice operators yet".to_owned(),
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
    for step in steps {
        match step {
            LinearStep::Filter(_) | LinearStep::Join { .. } | LinearStep::Project(_)
                if seen_order || seen_slice =>
            {
                gaps.push(UnsupportedReason::Operator(
                    "filters/joins/projects after order/slice are not lowered yet".to_owned(),
                ));
            }
            LinearStep::Filter(_) | LinearStep::Join { .. } | LinearStep::Project(_) => {}
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
        }
    }
}

fn source_requirements(
    output: &RowSetOutputRequest,
    plan: &AnalyzedQueryPlan,
) -> CapabilityResult<BTreeMap<SourceId, SourceRequirements>> {
    let mut requirements = BTreeMap::<SourceId, SourceRequirements>::new();
    for source in analyzed_plan_sources(plan) {
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
        if let Some(path_fact) = output.facts.iter().find(|fact| {
            matches!(
                (plan, fact),
                (
                    AnalyzedQueryPlan::CorrelatedPath(_),
                    ProgramFactKey::PathEdges | ProgramFactKey::PathCorrelationCoverage
                )
            )
        }) {
            return Err(Box::new(CapabilityReport {
                gaps: vec![UnsupportedReason::Output(Box::new(path_fact.clone()))],
                explain: ExplainPlan {
                    capabilities: vec![
                        "correlated path app rows lower to parent rows; path fact terminals require child path rows and cannot share that graph yet".to_owned(),
                    ],
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
            ProgramFactKey::ResultMembership | ProgramFactKey::VersionWitnesses => {
                let root_requirements = requirements
                    .get_mut(plan.root_source())
                    .expect("root source requirements were initialized");
                root_requirements
                    .metadata
                    .insert(SourceMetadataRequirement::VersionWitnesses);
            }
            ProgramFactKey::SourceCoverage(_) => {
                let root_requirements = requirements
                    .get_mut(plan.root_source())
                    .expect("root source requirements were initialized");
                root_requirements
                    .metadata
                    .insert(SourceMetadataRequirement::Coverage);
            }
            ProgramFactKey::PathEdges | ProgramFactKey::PathCorrelationCoverage => {
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
    match plan {
        AnalyzedQueryPlan::Linear(linear) => collect_linear_requirements(linear, requirements),
        AnalyzedQueryPlan::CorrelatedPath(path) => {
            collect_linear_requirements(&path.parent, requirements)?;
            collect_linear_requirements(&path.child, requirements)?;
            collect_predicate_requirements_for_all_sources(&path.correlation, requirements)
        }
        AnalyzedQueryPlan::RecursiveRelation(relation) => {
            collect_linear_requirements(&relation.seed, requirements)?;
            collect_linear_requirements(&relation.step, requirements)?;
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
            Ok(())
        }
    }
}

fn collect_linear_requirements(
    plan: &LinearCurrentRoot,
    requirements: &mut BTreeMap<SourceId, SourceRequirements>,
) -> CapabilityResult<()> {
    for step in &plan.steps {
        if let LinearStep::Join { right, .. } = step {
            collect_relation_requirements(right, requirements)?;
        }
    }
    for step in &plan.steps {
        for (source, source_requirements) in requirements.iter_mut() {
            collect_step_requirements(step, source, source_requirements)?;
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

fn collect_relation_requirements(
    plan: &RelationInputPlan,
    requirements: &mut BTreeMap<SourceId, SourceRequirements>,
) -> CapabilityResult<()> {
    match plan {
        RelationInputPlan::Linear(linear) => collect_linear_requirements(linear, requirements),
        RelationInputPlan::Recursive(relation) => {
            collect_linear_requirements(&relation.seed, requirements)?;
            collect_linear_requirements(&relation.step, requirements)
        }
    }
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
        NormalizedValueRef::Param(_) | NormalizedValueRef::Literal(_) => {}
        NormalizedValueRef::Claim(_) => {
            return Err(UnsupportedReason::Operator(
                "claim values are not lowered into Groove predicates yet".to_owned(),
            ));
        }
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

const UNBOUNDED_ORDERED_WINDOW_LIMIT: usize = usize::MAX;

fn lower_plan_steps(
    graph: GraphBuilder,
    plan: &AnalyzedQueryPlan,
    root_source: &ResolvedSource,
    resolved_sources: &BTreeMap<SourceId, ResolvedSource>,
    request: &QueryProgramRequest,
) -> Result<GraphBuilder, UnsupportedReason> {
    match plan {
        AnalyzedQueryPlan::Linear(linear) => {
            lower_linear_plan_steps(graph, linear, root_source, resolved_sources, request)
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
) -> Result<GraphBuilder, UnsupportedReason> {
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
    let child = lower_linear_plan_steps(
        child_source.graph.clone(),
        &path.child,
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

    if request.output.app_rows.is_none() {
        return Ok(GraphBuilder::join(parent, child, [parent_key], [child_key]));
    }

    match path.requirement {
        CorrelationRequirement::Optional => Ok(parent),
        CorrelationRequirement::AtLeastOne => {
            let joined = GraphBuilder::join(parent, child, [parent_key], [child_key])
                .project_fields(project_left_source_fields(root_source));
            Ok(GraphBuilder::arg_min_by(
                joined,
                [root_source.row_shape.row_uuid_field.clone()],
                [root_source.row_shape.row_uuid_field.clone()],
            ))
        }
        CorrelationRequirement::MatchCorrelationCardinality => Err(UnsupportedReason::Operator(
            "match-correlation-cardinality app rows need cardinality coverage lowering".to_owned(),
        )),
    }
}

#[derive(Clone, Debug)]
struct LoweredRelationInput {
    graph: GraphBuilder,
    root_source: Option<ResolvedSource>,
    fields: BTreeSet<String>,
    nullable_fields: BTreeSet<String>,
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
            let graph = lower_linear_plan_steps(
                source.graph.clone(),
                linear,
                &source,
                resolved_sources,
                request,
            )?;
            Ok(LoweredRelationInput {
                graph,
                fields: linear_output_fields(linear, &source, request),
                nullable_fields: linear_nullable_output_fields(linear, &source),
                root_source: Some(source),
            })
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
            let graph =
                lower_recursive_relation(None, relation, &source, resolved_sources, request)?;
            Ok(LoweredRelationInput {
                graph,
                root_source: Some(source),
                fields: recursive_output_fields(relation),
                nullable_fields: BTreeSet::new(),
            })
        }
    }
}

fn linear_output_fields(
    plan: &LinearCurrentRoot,
    root_source: &ResolvedSource,
    request: &QueryProgramRequest,
) -> BTreeSet<String> {
    if let Some(LinearStep::Project(columns)) = plan.steps.last() {
        return columns
            .iter()
            .map(|column| column.output.name.clone())
            .collect();
    }
    let mut fields: BTreeSet<String> = match &plan.root {
        LinearRoot::Source(_) => source_fields(root_source).collect(),
        LinearRoot::Value { columns, .. } | LinearRoot::Frontier { columns, .. } => {
            columns.iter().map(|column| column.name.clone()).collect()
        }
    };
    if matches!(plan.root, LinearRoot::Source(_)) {
        let routing = parameter_domain(&request.input.shape).routing_params;
        for step in &plan.steps {
            if let LinearStep::Join { right, .. } = step {
                let right_fields = relation_output_fields_for_routing(right, request);
                fields.extend(
                    routing
                        .iter()
                        .filter(|param| right_fields.contains(*param))
                        .cloned(),
                );
            }
        }
    }
    fields
}

fn linear_nullable_output_fields(
    plan: &LinearCurrentRoot,
    root_source: &ResolvedSource,
) -> BTreeSet<String> {
    if matches!(plan.steps.last(), Some(LinearStep::Project(_))) {
        return BTreeSet::new();
    }
    if !matches!(plan.root, LinearRoot::Source(_)) {
        return BTreeSet::new();
    }
    root_source
        .row_shape
        .descriptor
        .fields()
        .iter()
        .filter_map(|field| {
            let name = field.name.as_ref()?;
            matches!(&field.value_type, ValueType::Nullable(_)).then(|| name.clone())
        })
        .collect()
}

fn recursive_output_fields(relation: &RecursiveRelationPlan) -> BTreeSet<String> {
    if let Some(LinearStep::Project(columns)) = relation.step.steps.last() {
        return columns
            .iter()
            .map(|column| column.output.name.clone())
            .collect();
    }
    linear_root_fields(&relation.seed.root)
}

fn relation_output_fields_for_routing(
    plan: &RelationInputPlan,
    request: &QueryProgramRequest,
) -> BTreeSet<String> {
    match plan {
        RelationInputPlan::Recursive(relation) => recursive_output_fields(relation),
        RelationInputPlan::Linear(linear) => {
            if let Some(LinearStep::Project(columns)) = linear.steps.last() {
                return columns
                    .iter()
                    .map(|column| column.output.name.clone())
                    .collect();
            }
            let mut fields = linear_root_fields(&linear.root);
            if matches!(linear.root, LinearRoot::Source(_)) {
                let routing = parameter_domain(&request.input.shape).routing_params;
                for step in &linear.steps {
                    if let LinearStep::Join { right, .. } = step {
                        let right_fields = relation_output_fields_for_routing(right, request);
                        fields.extend(
                            routing
                                .iter()
                                .filter(|param| right_fields.contains(*param))
                                .cloned(),
                        );
                    }
                }
            }
            fields
        }
    }
}

fn linear_root_fields(root: &LinearRoot) -> BTreeSet<String> {
    match root {
        LinearRoot::Source(_) => BTreeSet::new(),
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
}

fn lower_recursive_relation(
    root_graph: Option<GraphBuilder>,
    relation: &RecursiveRelationPlan,
    root_source: &ResolvedSource,
    resolved_sources: &BTreeMap<SourceId, ResolvedSource>,
    request: &QueryProgramRequest,
) -> Result<GraphBuilder, UnsupportedReason> {
    let seed_root = relation.seed.root.source().and_then(|source| {
        resolved_sources
            .get(source)
            .map(|resolved| resolved.graph.clone())
    });
    let seed_graph = root_graph
        .or(seed_root)
        .unwrap_or_else(|| root_source.graph.clone());
    let seed = lower_linear_plan_steps(
        seed_graph,
        &relation.seed,
        root_source,
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
        RecursionBound::Fixpoint => 128,
        RecursionBound::MaxDepth(max_depth) => max_depth.max(1),
    };
    Ok(GraphBuilder::recursive(
        seed,
        step,
        relation.frontier.0.clone(),
        max_iters,
    ))
}

fn lower_linear_plan_steps(
    graph: GraphBuilder,
    plan: &LinearCurrentRoot,
    root_source: &ResolvedSource,
    resolved_sources: &BTreeMap<SourceId, ResolvedSource>,
    request: &QueryProgramRequest,
) -> Result<GraphBuilder, UnsupportedReason> {
    let mut graph = match &plan.root {
        LinearRoot::Source(_) => graph,
        LinearRoot::Value {
            shape,
            columns,
            mode,
        } => lower_value_source(shape, columns, mode, request)?,
        LinearRoot::Frontier { frontier, columns } => {
            GraphBuilder::frontier_source(frontier.0.clone(), value_source_descriptor(columns))
        }
    };
    let mut pending_order: Option<Vec<OrderKey>> = None;
    let mut last_join_right: Option<(RelationInputPlan, BTreeSet<String>)> = None;

    for step in &plan.steps {
        match step {
            LinearStep::Filter(predicate) => {
                last_join_right = None;
                let source = plan.root.source().ok_or_else(|| {
                    UnsupportedReason::Operator(
                        "filters on value/frontier sources are not lowered yet".to_owned(),
                    )
                })?;
                let predicate = lower_predicate(predicate, source, root_source, request)?;
                graph = graph.filter(predicate);
            }
            LinearStep::Join { right, mode, on } => {
                if *mode != JoinMode::Inner {
                    return Err(UnsupportedReason::Operator(
                        "join_via only lowers inner/semi joins".to_owned(),
                    ));
                }
                let lowered_right = lower_relation_input(right, resolved_sources, request)?;
                let (left_key, right_key) = lower_linear_join_key_pair(
                    on,
                    &plan.root,
                    root_source,
                    right,
                    &lowered_right,
                    request,
                )?;
                if matches!(&plan.root, LinearRoot::Source(_))
                    && source_field_is_nullable(root_source, &left_key)
                {
                    graph = graph.unwrap_nullable(left_key.clone());
                }
                let right_nullable_fields = lowered_right.nullable_fields.clone();
                let mut right_graph = lowered_right.graph;
                if lowered_right.nullable_fields.contains(&right_key) {
                    right_graph = right_graph.unwrap_nullable(right_key.clone());
                }
                graph = GraphBuilder::join(graph, right_graph, [left_key], [right_key]);
                last_join_right = Some(((**right).clone(), right_nullable_fields));
                if matches!(&plan.root, LinearRoot::Source(_)) {
                    graph = graph.project_fields(project_left_source_fields_with_routing(
                        root_source,
                        &lowered_right.fields,
                        request,
                    ));
                    last_join_right = None;
                }
            }
            LinearStep::Project(columns) => {
                let mut unwrap_fields = BTreeSet::new();
                let fields = columns
                    .iter()
                    .map(|column| {
                        let field = lower_projection_field(
                            column,
                            plan,
                            root_source,
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
                graph = graph.project_fields(fields);
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

    Ok(graph)
}

fn value_source_descriptor(columns: &[ValueSourceColumn]) -> RecordDescriptor {
    RecordDescriptor::new(
        columns
            .iter()
            .map(|column| (column.name.clone(), column.ty.value_type())),
    )
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
            let mut params = BTreeMap::<String, ColumnType>::new();
            for column in columns {
                let NormalizedValueRef::Param(param) = &column.value else {
                    return Err(UnsupportedReason::Operator(
                        "binding value source columns must reference binding params".to_owned(),
                    ));
                };
                if let Some(existing) = params.insert(param.clone(), column.ty.clone()) {
                    if existing != column.ty {
                        return Err(UnsupportedReason::Operator(format!(
                            "binding parameter '{param}' has conflicting value-source types"
                        )));
                    }
                }
            }
            let input_descriptor = RecordDescriptor::new(
                params
                    .iter()
                    .map(|(name, column_type)| (name.clone(), column_type.value_type())),
            );
            Ok(
                GraphBuilder::binding_source(shape.to_owned(), input_descriptor).project_fields(
                    columns.iter().map(|column| {
                        let NormalizedValueRef::Param(param) = &column.value else {
                            unreachable!("checked above");
                        };
                        ProjectField::renamed(param.clone(), column.name.clone())
                    }),
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
        _ => Err(UnsupportedReason::Operator(
            "value source columns must be binding params or literals".to_owned(),
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
    let PredicateExpr::Compare {
        left,
        op: ComparisonOp::Eq,
        right,
    } = predicate
    else {
        return Err(UnsupportedReason::Operator(
            "correlated path projection only lowers equality correlations".to_owned(),
        ));
    };

    match (
        lower_join_key_ref(left, parent_source_id, parent_source, request),
        lower_join_key_ref(right, child_source_id, child_source, request),
    ) {
        (Ok(parent_key), Ok(child_key)) => Ok((parent_key, child_key)),
        _ => match (
            lower_join_key_ref(right, parent_source_id, parent_source, request),
            lower_join_key_ref(left, child_source_id, child_source, request),
        ) {
            (Ok(parent_key), Ok(child_key)) => Ok((parent_key, child_key)),
            _ => Err(UnsupportedReason::Operator(
                "correlated path projection correlation must compare parent and child fields"
                    .to_owned(),
            )),
        },
    }
}

fn lower_join_key_pair(
    predicate: &PredicateExpr,
    left_source_id: &SourceId,
    left_source: &ResolvedSource,
    right_source_id: &SourceId,
    right_source: &ResolvedSource,
    request: &QueryProgramRequest,
) -> Result<(String, String), UnsupportedReason> {
    let PredicateExpr::Compare {
        left,
        op: ComparisonOp::Eq,
        right,
    } = predicate
    else {
        return Err(UnsupportedReason::Operator(
            "join_via only lowers equality join predicates".to_owned(),
        ));
    };

    match (
        lower_join_key_ref(left, left_source_id, left_source, request),
        lower_join_key_ref(right, right_source_id, right_source, request),
    ) {
        (Ok(left_key), Ok(right_key)) => Ok((left_key, right_key)),
        _ => match (
            lower_join_key_ref(right, left_source_id, left_source, request),
            lower_join_key_ref(left, right_source_id, right_source, request),
        ) {
            (Ok(left_key), Ok(right_key)) => Ok((left_key, right_key)),
            _ => Err(UnsupportedReason::Operator(
                "join_via join predicate must compare the root row id to one join source field"
                    .to_owned(),
            )),
        },
    }
}

fn lower_linear_join_key_pair(
    predicate: &PredicateExpr,
    left_root: &LinearRoot,
    left_source: &ResolvedSource,
    right_plan: &RelationInputPlan,
    right_output: &LoweredRelationInput,
    request: &QueryProgramRequest,
) -> Result<(String, String), UnsupportedReason> {
    let PredicateExpr::Compare {
        left,
        op: ComparisonOp::Eq,
        right: right_value,
    } = predicate
    else {
        return Err(UnsupportedReason::Operator(
            "join_via only lowers equality join predicates".to_owned(),
        ));
    };

    match (
        lower_linear_root_key_ref(left, left_root, left_source, request),
        lower_relation_key_ref(right_value, right_plan, right_output, request),
    ) {
        (Ok(left_key), Ok(right_key)) => Ok((left_key, right_key)),
        _ => match (
            lower_linear_root_key_ref(right_value, left_root, left_source, request),
            lower_relation_key_ref(left, right_plan, right_output, request),
        ) {
            (Ok(left_key), Ok(right_key)) => Ok((left_key, right_key)),
            _ => Err(UnsupportedReason::Operator(
                "join_via join predicate must compare left root and right relation fields"
                    .to_owned(),
            )),
        },
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
            if let Some(source) = &output.root_source {
                if let Some(source_id) = linear.root.source() {
                    if let Ok(key) = lower_join_key_ref(value, source_id, source, request) {
                        return Ok(key);
                    }
                }
            }
            lower_named_relation_field(value, &output.fields)
        }
        RelationInputPlan::Recursive(_) => lower_named_relation_field(value, &output.fields),
    }
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
        LinearRoot::Source(source_id) => lower_join_key_ref(value, source_id, source, request),
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
    last_join_right: Option<&(RelationInputPlan, BTreeSet<String>)>,
    request: &QueryProgramRequest,
) -> Result<ProjectionFieldPlan, UnsupportedReason> {
    let mut unwrap_before_project = BTreeSet::new();
    let project =
        match lower_projection_source(&column.value, plan, source, last_join_right, request)? {
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
    last_join_right: Option<&(RelationInputPlan, BTreeSet<String>)>,
    request: &QueryProgramRequest,
) -> Result<ProjectionSource, UnsupportedReason> {
    if let Ok(field) = lower_linear_root_key_ref(value, &plan.root, source, request) {
        let nullable =
            matches!(plan.root, LinearRoot::Source(_)) && source_field_is_nullable(source, &field);
        return Ok(ProjectionSource::Field {
            field: match last_join_right {
                Some(_) => format!("left.{field}"),
                None => field,
            },
            nullable,
        });
    }

    if let Some((right, nullable_fields)) = last_join_right {
        if let Some(field) = lower_relation_projection_ref(value, right, request)? {
            let nullable = nullable_fields.contains(&field);
            return Ok(ProjectionSource::Field {
                field: format!("right.{field}"),
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
            if matches!(linear.root, LinearRoot::Source(_)) {
                if let Some(source_id) = linear.root.source() {
                    match value {
                        NormalizedValueRef::SourceField {
                            source: value_source,
                            field,
                        } if value_source == source_id => {
                            return Ok(Some(format!("user_{field}")));
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
    }
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
    match lower_value_ref(value, source_id, source, request)? {
        LoweredValueRef::Field(field) => Ok(field),
        LoweredValueRef::Literal(_) => Err(UnsupportedReason::Operator(
            "join_via join keys must be source fields".to_owned(),
        )),
    }
}

fn source_field_is_nullable(source: &ResolvedSource, field: &str) -> bool {
    source
        .row_shape
        .descriptor
        .field_index(field)
        .and_then(|index| source.row_shape.descriptor.fields().get(index))
        .is_some_and(|field| matches!(&field.value_type, ValueType::Nullable(_)))
}

fn project_left_source_fields(source: &ResolvedSource) -> Vec<ProjectField> {
    source
        .row_shape
        .descriptor
        .fields()
        .iter()
        .filter_map(|field| field.name.as_ref())
        .map(|field| ProjectField::renamed(format!("left.{field}"), field.clone()))
        .collect()
}

fn project_left_source_fields_with_routing(
    source: &ResolvedSource,
    right_fields: &BTreeSet<String>,
    request: &QueryProgramRequest,
) -> Vec<ProjectField> {
    let mut fields = project_left_source_fields(source);
    let routing = parameter_domain(&request.input.shape).routing_params;
    fields.extend(routing.into_iter().filter_map(|param| {
        right_fields
            .contains(&param)
            .then(|| ProjectField::renamed(format!("right.{param}"), param))
    }));
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
            offset as usize,
            limit
                .map(|limit| limit as usize)
                .unwrap_or(UNBOUNDED_ORDERED_WINDOW_LIMIT),
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
        offset as usize,
        limit
            .map(|limit| limit as usize)
            .unwrap_or(UNBOUNDED_ORDERED_WINDOW_LIMIT),
    ))
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
    let lowered = match predicate {
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
    };
    Ok(lowered.canonicalize())
}

fn lower_not_predicate(
    predicate: &PredicateExpr,
    source_id: &SourceId,
    source: &ResolvedSource,
    request: &QueryProgramRequest,
) -> Result<GroovePredicateExpr, UnsupportedReason> {
    let lowered = match predicate {
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
    };
    Ok(lowered.canonicalize())
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
            Ok(GroovePredicateExpr::from_field_literal(kind, field, value))
        }
        (LoweredValueRef::Literal(value), LoweredValueRef::Field(field)) => Ok(
            GroovePredicateExpr::from_field_literal(kind.reversed(), field, value),
        ),
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
            Ok(GroovePredicateExpr::Contains { field, value })
        }
        (LoweredValueRef::Field(field), LoweredValueRef::Field(needle_field)) => {
            Ok(GroovePredicateExpr::ContainsField {
                field,
                needle_field,
            })
        }
        _ => Err(UnsupportedReason::Operator(
            "array contains requires a source field haystack".to_owned(),
        )),
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
            &format!("user_{field}"),
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
        NormalizedValueRef::Claim(_) => Err(UnsupportedReason::Operator(
            "claim values are not lowered into Groove predicates yet".to_owned(),
        )),
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

fn output_terminals(
    request: &RowSetOutputRequest,
    plan: &AnalyzedQueryPlan,
    source: &ResolvedSource,
    resolved_sources: &BTreeMap<SourceId, ResolvedSource>,
) -> CapabilityResult<Vec<OutputTerminalSchema>> {
    let mut terminals = Vec::new();
    if request.app_rows.is_some() {
        terminals.push(OutputTerminalSchema::AppRows(AppRowSchema {
            descriptor: source.row_shape.descriptor,
            hidden_fields: hidden_source_fields(&source.row_shape),
        }));
    }

    for fact in &request.facts {
        terminals.push(OutputTerminalSchema::Fact(fact_output(
            fact,
            plan,
            source,
            resolved_sources,
        )?));
    }

    Ok(terminals)
}

fn fact_output(
    key: &ProgramFactKey,
    plan: &AnalyzedQueryPlan,
    source: &ResolvedSource,
    resolved_sources: &BTreeMap<SourceId, ResolvedSource>,
) -> CapabilityResult<ProgramFactOutput> {
    let schema = match key {
        ProgramFactKey::ResultMembership => {
            let version = version_witness_fields(&source.row_shape)?;
            ProgramFactSchema::ResultMembership(ResultMembershipSchema {
                table_field: "table".to_owned(),
                row_field: source.row_shape.row_uuid_field.clone(),
                branch_or_prefix_field: version.branch_or_prefix_field.clone(),
                version: content_version_schema(&version),
                routing_param_fields: BTreeSet::new(),
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
            ProgramFactSchema::VersionWitnesses(VersionWitnessSchemas {
                role_field: "role".to_owned(),
                content: Some(version_witness_schema(source, &version)),
                deletion: None,
            })
        }
        ProgramFactKey::PathEdges => {
            ProgramFactSchema::PathEdges(path_edge_schema(plan, source, resolved_sources)?)
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
        schema,
    })
}

fn path_edge_schema(
    plan: &AnalyzedQueryPlan,
    root_source: &ResolvedSource,
    resolved_sources: &BTreeMap<SourceId, ResolvedSource>,
) -> CapabilityResult<PathEdgeSchema> {
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
            (root_source, child, None)
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
        AnalyzedQueryPlan::Linear(_) => {
            return Err(Box::new(CapabilityReport {
                gaps: vec![UnsupportedReason::Output(Box::new(
                    ProgramFactKey::PathEdges,
                ))],
                explain: ExplainPlan {
                    capabilities: vec![
                        "path edge facts require a path or recursive relation node".to_owned(),
                    ],
                    ..ExplainPlan::default()
                },
            }));
        }
    };

    Ok(PathEdgeSchema {
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
        AnalyzedQueryPlan::Linear(_) => Err(Box::new(CapabilityReport {
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
        })),
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
            table_field: "table".to_owned(),
            row_field: source.row_shape.row_uuid_field.clone(),
            tx_time_field: version.tx_time_field.clone(),
            tx_node_field: version.tx_node_field.clone(),
            batch_id_field: None,
            branch_or_prefix_field: version.branch_or_prefix_field.clone(),
            row_digest_field: None,
            schema_field: version.schema_version_field.clone(),
            layer_field: "layer".to_owned(),
        },
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
    /// Executable groove graph.
    pub(crate) graph: GraphBuilder,
    /// Parameter domains expected by the graph.
    pub(crate) parameters: ParameterDomain,
    /// App row and fact schemas emitted by the graph.
    pub(crate) output: ProgramOutputSchemas,
}
