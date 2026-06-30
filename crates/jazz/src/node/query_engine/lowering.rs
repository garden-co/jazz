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

    let plan = match validate_current_root(&request) {
        Ok(plan) => plan,
        Err(gaps) => {
            explain
                .capabilities
                .push("only table-rooted current lowering is implemented".to_owned());
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
        .get(&plan.root_source)
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
        .push("table-rooted current lowering".to_owned());
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
            parameters: ParameterDomain::default(),
            output: ProgramOutputSchemas::RowSet(output_terminals(
                &request.output,
                &resolved_root,
            )?),
        },
        request,
        explain,
    })
}

#[derive(Clone, Debug)]
struct LinearCurrentRoot {
    root_source: SourceId,
    tier: DurabilityTier,
    steps: Vec<LinearStep>,
}

#[derive(Clone, Debug)]
enum LinearStep {
    Filter(PredicateExpr),
    Join {
        right_source: SourceId,
        right_steps: Vec<JoinRightStep>,
        mode: JoinMode,
        on: PredicateExpr,
    },
    OrderBy(Vec<OrderKey>),
    Slice {
        partition_by: Vec<NormalizedValueRef>,
        limit: Option<u32>,
        offset: u32,
        tie_breaker: Vec<NormalizedValueRef>,
        rank_output: Option<TypedOutputField>,
    },
}

#[derive(Clone, Debug)]
enum JoinRightStep {
    Filter(PredicateExpr),
}

fn validate_current_root(
    request: &QueryProgramRequest,
) -> Result<LinearCurrentRoot, Vec<UnsupportedReason>> {
    let mut gaps = Vec::new();

    if !request.reads.fact_reads.is_empty() {
        gaps.push(UnsupportedReason::Source(SourceGap::TransactionReadOverlay));
    }
    if !matches!(request.policy, PolicyContext::System) {
        gaps.push(UnsupportedReason::Policy(
            "policy augmentation is not lowered yet".to_owned(),
        ));
    }

    let mut visited = BTreeSet::new();
    let analyzed = analyze_current_node(
        &request.input.shape.root,
        &request.input.shape.nodes,
        &mut visited,
    );
    let Ok((source, steps)) = analyzed else {
        gaps.push(analyzed.unwrap_err());
        return Err(gaps);
    };
    if visited.len() != request.input.shape.nodes.len() {
        gaps.push(UnsupportedReason::Operator(
            "only one linear source/filter/order/slice chain is lowered yet".to_owned(),
        ));
    }
    validate_step_order(&steps, &mut gaps);
    if !matches!(
        request.input.shape.result,
        ResultId::RealRow {
            row: ResultRowRef::Source(ref result_source),
            ..
        } if result_source == &source
    ) {
        gaps.push(UnsupportedReason::Operator(
            "result must be the root source row".to_owned(),
        ));
    }

    let mut tier = None;
    for plan_source in plan_sources(&source, &steps) {
        let read_source = request.reads.primary.sources.get(&plan_source);
        let Some(SourceExpr::VisibleCurrent {
            projection,
            data: DataSource::Current,
            tier: source_tier,
        }) = read_source
        else {
            gaps.push(UnsupportedReason::Source(SourceGap::HistoricalStorageCut));
            continue;
        };
        if plan_source == source {
            tier = Some(*source_tier);
        }
        if !matches!(projection.schema_family, SchemaFamilySelection::Current)
            || !matches!(projection.storage, StorageSchemaSelection::Single(_))
            || !matches!(projection.lens, LensSelection::Canonical)
        {
            gaps.push(UnsupportedReason::Source(SourceGap::SchemaProjection));
        }
    }

    if gaps.is_empty() {
        Ok(LinearCurrentRoot {
            root_source: source,
            tier: tier.expect("root source tier was validated"),
            steps,
        })
    } else {
        Err(gaps)
    }
}

fn plan_sources(root: &SourceId, steps: &[LinearStep]) -> BTreeSet<SourceId> {
    let mut sources = BTreeSet::from([root.clone()]);
    for step in steps {
        if let LinearStep::Join { right_source, .. } = step {
            sources.insert(right_source.clone());
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
) -> Result<(SourceId, Vec<LinearStep>), UnsupportedReason> {
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
            Ok((source.clone(), Vec::new()))
        }
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
            let (right_source, right_steps) = analyze_join_right_node(right, nodes, visited)?;
            steps.push(LinearStep::Join {
                right_source,
                right_steps,
                mode: *mode,
                on: on.clone(),
            });
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
        RowSetExpr::Project { .. } => Err(UnsupportedReason::Operator(
            "project row-set nodes are not lowered yet".to_owned(),
        )),
        RowSetExpr::CorrelatedPathProjection { .. } => Err(UnsupportedReason::Operator(
            "correlated path projection row-set nodes are not lowered yet".to_owned(),
        )),
        RowSetExpr::Aggregate { .. } => Err(UnsupportedReason::Operator(
            "aggregate row-set nodes are not lowered yet".to_owned(),
        )),
    }
}

fn analyze_join_right_node(
    node_id: &RowSetNodeId,
    nodes: &BTreeMap<RowSetNodeId, RowSetExpr>,
    visited: &mut BTreeSet<RowSetNodeId>,
) -> Result<(SourceId, Vec<JoinRightStep>), UnsupportedReason> {
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
                    "include-deleted join sources are not lowered yet".to_owned(),
                ));
            }
            Ok((source.clone(), Vec::new()))
        }
        RowSetExpr::Filter { input, predicate } => {
            if predicate_contains_param(predicate) {
                return Err(UnsupportedReason::Operator(
                    "join_via filters with binding parameters are not lowered without binding-source parameter support".to_owned(),
                ));
            }
            let (source, mut steps) = analyze_join_right_node(input, nodes, visited)?;
            steps.push(JoinRightStep::Filter(predicate.clone()));
            Ok((source, steps))
        }
        RowSetExpr::OrderBy { .. }
        | RowSetExpr::Slice { .. }
        | RowSetExpr::Join { .. }
        | RowSetExpr::RecursiveRelation { .. }
        | RowSetExpr::Union { .. }
        | RowSetExpr::Distinct { .. }
        | RowSetExpr::Project { .. }
        | RowSetExpr::CorrelatedPathProjection { .. }
        | RowSetExpr::Aggregate { .. } => Err(UnsupportedReason::Operator(
            "join_via right side only supports source plus filters".to_owned(),
        )),
    }
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
            LinearStep::Filter(_) | LinearStep::Join { .. } if seen_order || seen_slice => {
                gaps.push(UnsupportedReason::Operator(
                    "filters/joins after order/slice are not lowered yet".to_owned(),
                ));
            }
            LinearStep::Filter(_) | LinearStep::Join { .. } => {}
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
    plan: &LinearCurrentRoot,
) -> CapabilityResult<BTreeMap<SourceId, SourceRequirements>> {
    let mut requirements = BTreeMap::<SourceId, SourceRequirements>::new();
    requirements.insert(plan.root_source.clone(), SourceRequirements::default());
    for step in &plan.steps {
        if let LinearStep::Join { right_source, .. } = step {
            requirements.entry(right_source.clone()).or_default();
        }
    }

    let root_requirements = requirements
        .get_mut(&plan.root_source)
        .expect("root source requirements were initialized");

    if let Some(app_rows) = &output.app_rows {
        root_requirements.app_fields = match &app_rows.projection {
            PayloadProjection::ShapeDefault => FieldRequirement::All,
            PayloadProjection::Tree(tree) => tree.fields.clone().into(),
        };
    }

    for fact in &output.facts {
        match fact {
            ProgramFactKey::ResultMembership | ProgramFactKey::VersionWitnesses => {
                root_requirements
                    .metadata
                    .insert(SourceMetadataRequirement::VersionWitnesses);
            }
            ProgramFactKey::SourceCoverage(_) => {
                root_requirements
                    .metadata
                    .insert(SourceMetadataRequirement::Coverage);
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

    for step in &plan.steps {
        for (source, source_requirements) in &mut requirements {
            collect_step_requirements(step, source, source_requirements)?;
        }
    }

    Ok(requirements)
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
        LinearStep::Join {
            right_steps, on, ..
        } => (|| {
            collect_predicate_requirements(on, source, requirements)?;
            for right_step in right_steps {
                match right_step {
                    JoinRightStep::Filter(predicate) => {
                        collect_predicate_requirements(predicate, source, requirements)?;
                    }
                }
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
        | NormalizedValueRef::RowId(RowIdRef::Frontier(_)) => {
            return Err(UnsupportedReason::Operator(
                "frontier values are not valid in root source predicates".to_owned(),
            ));
        }
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
    mut graph: GraphBuilder,
    plan: &LinearCurrentRoot,
    root_source: &ResolvedSource,
    resolved_sources: &BTreeMap<SourceId, ResolvedSource>,
    request: &QueryProgramRequest,
) -> Result<GraphBuilder, UnsupportedReason> {
    let mut pending_order: Option<Vec<OrderKey>> = None;

    for step in &plan.steps {
        match step {
            LinearStep::Filter(predicate) => {
                let predicate =
                    lower_predicate(predicate, &plan.root_source, root_source, request)?;
                graph = graph.filter(predicate);
            }
            LinearStep::Join {
                right_source,
                right_steps,
                mode,
                on,
            } => {
                if *mode != JoinMode::Inner {
                    return Err(UnsupportedReason::Operator(
                        "join_via only lowers inner/semi joins".to_owned(),
                    ));
                }
                let resolved_right = resolved_sources.get(right_source).ok_or_else(|| {
                    UnsupportedReason::Runtime(format!(
                        "join source {:?} was not resolved",
                        right_source
                    ))
                })?;
                let mut right_graph = resolved_right.graph.clone();
                for right_step in right_steps {
                    match right_step {
                        JoinRightStep::Filter(predicate) => {
                            let predicate =
                                lower_predicate(predicate, right_source, resolved_right, request)?;
                            right_graph = right_graph.filter(predicate);
                        }
                    }
                }
                let (left_key, right_key) = lower_join_key_pair(
                    on,
                    &plan.root_source,
                    root_source,
                    right_source,
                    resolved_right,
                    request,
                )?;
                if source_field_is_nullable(root_source, &left_key) {
                    graph = graph.unwrap_nullable(left_key.clone());
                }
                if source_field_is_nullable(resolved_right, &right_key) {
                    right_graph = right_graph.unwrap_nullable(right_key.clone());
                }
                graph = GraphBuilder::join(graph, right_graph, [left_key], [right_key])
                    .project_fields(project_left_source_fields(root_source));
            }
            LinearStep::OrderBy(keys) => {
                pending_order = Some(keys.clone());
            }
            LinearStep::Slice {
                partition_by,
                limit,
                offset,
                tie_breaker,
                ..
            } => {
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
                plan.root_source.clone(),
            ))],
            plan,
            root_source,
            request,
        )?;
    }

    Ok(graph)
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
    match lower_value_ref(value, &plan.root_source, source, request)? {
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
    source: &ResolvedSource,
) -> CapabilityResult<Vec<OutputTerminalSchema>> {
    let mut terminals = Vec::new();
    if request.app_rows.is_some() {
        terminals.push(OutputTerminalSchema::AppRows(AppRowSchema {
            descriptor: source.row_shape.descriptor,
            hidden_fields: hidden_source_fields(&source.row_shape),
        }));
    }

    for fact in &request.facts {
        terminals.push(OutputTerminalSchema::Fact(fact_output(fact, source)?));
    }

    Ok(terminals)
}

fn fact_output(
    key: &ProgramFactKey,
    source: &ResolvedSource,
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
