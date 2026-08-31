//! Query-shape analysis and executable-plan construction.
//!
//! This stage turns normalized row-set shapes into explicit linear,
//! correlated, union, and recursive plans. It does not construct Groove
//! operators or choose the fields each source must expose.

use super::*;

/// Bound the normalized row-set tree before recursive analysis constructs an
/// owned plan.  This keeps malformed or adversarial query/policy programs
/// from exhausting the server stack in analysis, traversal, or destruction.
pub(super) const MAX_ROW_SET_NESTING_DEPTH: usize = 256;

#[derive(Clone, Debug)]
pub(super) struct LinearCurrentRoot {
    pub(super) root: LinearRoot,
    pub(super) steps: Vec<LinearStep>,
}

#[derive(Clone, Debug)]
pub(super) enum LinearRoot {
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
    pub(super) fn source(&self) -> Option<&SourceId> {
        match self {
            LinearRoot::Source { source, .. } => Some(source),
            LinearRoot::Value { .. } | LinearRoot::Frontier { .. } => None,
        }
    }
}

#[derive(Clone, Debug)]
pub(super) enum AnalyzedQueryPlan {
    Linear(LinearCurrentRoot),
    Union(UnionPlan),
    CorrelatedPath(CorrelatedPathPlan),
    RecursiveRelation(RecursiveRelationPlan),
}

impl AnalyzedQueryPlan {
    pub(super) fn root_source(&self) -> &SourceId {
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

    pub(super) fn capability_label(&self) -> &'static str {
        match self {
            AnalyzedQueryPlan::Linear(_) => "table-rooted current lowering",
            AnalyzedQueryPlan::Union(_) => "union current lowering",
            AnalyzedQueryPlan::CorrelatedPath(_) => "correlated path projection analysis",
            AnalyzedQueryPlan::RecursiveRelation(_) => "recursive relation analysis",
        }
    }
}

pub(super) fn first_step_source(steps: &[LinearStep]) -> Option<&SourceId> {
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
pub(super) struct CorrelatedPathPlan {
    pub(super) parent: LinearCurrentRoot,
    pub(super) child: LinearCurrentRoot,
    pub(super) path: ProgramPathId,
    pub(super) correlation: PredicateExpr,
    pub(super) requirement: CorrelationRequirement,
    pub(super) output_steps: Vec<LinearStep>,
    pub(super) siblings: Vec<CorrelatedPathPlan>,
    pub(super) nested: Vec<CorrelatedPathPlan>,
}

#[derive(Clone, Debug)]
pub(super) struct RecursiveRelationPlan {
    pub(super) seed: LinearCurrentRoot,
    pub(super) step: LinearCurrentRoot,
    pub(super) frontier: FrontierId,
    pub(super) frontier_key: NormalizedValueRef,
    pub(super) dedupe_keys: Vec<NormalizedValueRef>,
    pub(super) bound: RecursionBound,
}

#[derive(Clone, Debug)]
pub(super) struct UnionPlan {
    pub(super) branches: Vec<UnionBranchPlan>,
}

impl UnionPlan {
    pub(super) fn root_source(&self) -> Option<&SourceId> {
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
pub(super) struct UnionBranchPlan {
    pub(super) label: String,
    pub(super) plan: RelationInputPlan,
}

impl RecursiveRelationPlan {
    pub(super) fn root_source(&self) -> Option<&SourceId> {
        self.seed
            .root
            .source()
            .or_else(|| first_step_source(&self.seed.steps))
            .or_else(|| self.step.root.source())
            .or_else(|| first_step_source(&self.step.steps))
    }

    pub(super) fn seed_source(&self) -> Option<&SourceId> {
        self.seed
            .root
            .source()
            .or_else(|| first_step_source(&self.seed.steps))
    }

    pub(super) fn step_source(&self) -> Option<&SourceId> {
        self.step
            .root
            .source()
            .or_else(|| first_step_source(&self.step.steps))
    }
}

#[derive(Clone, Debug)]
pub(super) enum RelationInputPlan {
    Linear(LinearCurrentRoot),
    Union(UnionPlan),
    Recursive(RecursiveRelationPlan),
}

impl RelationInputPlan {
    pub(super) fn root_source(&self) -> Option<&SourceId> {
        match self {
            RelationInputPlan::Linear(linear) => linear.root.source(),
            RelationInputPlan::Union(union) => union.root_source(),
            RelationInputPlan::Recursive(relation) => relation.root_source(),
        }
    }
}

#[derive(Clone, Debug)]
pub(super) enum LinearStep {
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

pub(super) fn analyze_query_plan(
    request: &QueryProgramRequest,
) -> Result<AnalyzedQueryPlan, Vec<UnsupportedReason>> {
    let mut gaps = Vec::new();

    if let Err(gap) =
        validate_row_set_nesting(&request.input.shape.root, &request.input.shape.nodes)
    {
        return Err(vec![gap]);
    }

    if !request.reads.fact_reads.is_empty() {
        gaps.push(UnsupportedReason::Source(SourceGap::TransactionReadOverlay));
    }
    let analyzed = analyze_root_node(request);
    let Ok(plan) = analyzed else {
        gaps.push(analyzed.unwrap_err());
        return Err(gaps);
    };
    let plan = plan_with_default_result_order(plan, request);
    validate_output_capabilities(request, &plan, &mut gaps);
    validate_recursive_arg_by_capabilities(&plan, &mut gaps);

    for plan_source in analyzed_plan_sources(&plan) {
        let read_source = request.reads.primary.sources.get(&plan_source);
        let Some(projection) = supported_current_storage_projection(read_source) else {
            gaps.push(UnsupportedReason::Source(SourceGap::HistoricalStorageCut));
            continue;
        };
        if !matches!(projection.schema_family, SchemaFamilySelection::Current)
            || !matches!(
                projection.storage,
                StorageSchemaSelection::Single(_) | StorageSchemaSelection::CompatiblePartitions
            )
            || !matches!(projection.lens, LensSelection::Canonical)
        {
            gaps.push(UnsupportedReason::Source(SourceGap::SchemaProjection));
        }
    }

    if gaps.is_empty() { Ok(plan) } else { Err(gaps) }
}

fn validate_row_set_nesting(
    root: &RowSetNodeId,
    nodes: &BTreeMap<RowSetNodeId, RowSetExpr>,
) -> Result<(), UnsupportedReason> {
    let mut pending = vec![(root.clone(), false)];
    let mut active = BTreeSet::new();
    let mut heights = BTreeMap::<RowSetNodeId, usize>::new();
    while let Some((node_id, expanded)) = pending.pop() {
        if heights.contains_key(&node_id) {
            continue;
        }
        let node = nodes.get(&node_id).ok_or_else(|| {
            UnsupportedReason::Operator(format!("row-set node {node_id:?} is missing"))
        })?;
        let children: Vec<RowSetNodeId> = match node {
            RowSetExpr::Source { .. }
            | RowSetExpr::ValueSource { .. }
            | RowSetExpr::FrontierSource { .. } => Vec::new(),
            RowSetExpr::Filter { input, .. }
            | RowSetExpr::Distinct { input, .. }
            | RowSetExpr::Project { input, .. }
            | RowSetExpr::OrderBy { input, .. }
            | RowSetExpr::Slice { input, .. }
            | RowSetExpr::Aggregate { input, .. } => vec![input.clone()],
            RowSetExpr::Join { left, right, .. } => vec![left.clone(), right.clone()],
            RowSetExpr::RecursiveRelation { seed, step, .. } => vec![seed.clone(), step.clone()],
            RowSetExpr::Union { inputs } => inputs.iter().map(|input| input.node.clone()).collect(),
            RowSetExpr::CorrelatedPathProjection {
                input, child_input, ..
            } => vec![input.clone(), child_input.clone()],
        };
        if expanded {
            active.remove(&node_id);
            let height = 1 + children
                .iter()
                .filter_map(|child| heights.get(child))
                .copied()
                .max()
                .unwrap_or(0);
            if height > MAX_ROW_SET_NESTING_DEPTH {
                return Err(UnsupportedReason::Operator(format!(
                    "row-set nesting depth exceeds MAX_ROW_SET_NESTING_DEPTH ({MAX_ROW_SET_NESTING_DEPTH})"
                )));
            }
            heights.insert(node_id, height);
            continue;
        }
        if !active.insert(node_id.clone()) {
            return Err(UnsupportedReason::Operator(format!(
                "row-set nesting contains a cycle at node {node_id:?}"
            )));
        }
        pending.push((node_id, true));
        for child in children.into_iter().rev() {
            if active.contains(&child) {
                return Err(UnsupportedReason::Operator(format!(
                    "row-set nesting contains a cycle at node {child:?}"
                )));
            }
            if !heights.contains_key(&child) {
                pending.push((child, false));
            }
        }
    }
    Ok(())
}

pub(super) fn validate_output_capabilities(
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

fn validate_recursive_arg_by_capabilities(
    plan: &AnalyzedQueryPlan,
    gaps: &mut Vec<UnsupportedReason>,
) {
    if collect_plan_fragments(plan)
        .recursives
        .into_iter()
        .any(logical_recursive_plan_contains_arg_by)
    {
        gaps.push(UnsupportedReason::Operator(
            "arg_max_by and arg_min_by are not supported inside recursive seed or step graphs"
                .to_owned(),
        ));
    }
}

fn logical_recursive_plan_contains_arg_by(relation: &RecursiveRelationPlan) -> bool {
    let mut linear_plans = vec![&relation.seed, &relation.step];
    let mut relation_inputs = Vec::new();
    loop {
        if let Some(plan) = linear_plans.pop() {
            let mut pending_order_is_empty = true;
            for step in &plan.steps {
                match step {
                    LinearStep::OrderBy(keys) => pending_order_is_empty = keys.is_empty(),
                    LinearStep::Slice { limit, offset, .. } => {
                        if pending_order_is_empty && *offset == 0 && *limit == Some(1) {
                            return true;
                        }
                        pending_order_is_empty = true;
                    }
                    LinearStep::Aggregate { .. } => pending_order_is_empty = true,
                    LinearStep::Join { right, .. } => relation_inputs.push(right.as_ref()),
                    LinearStep::Filter(_) | LinearStep::Project(_) => {}
                }
            }
            continue;
        }
        let Some(input) = relation_inputs.pop() else {
            return false;
        };
        match input {
            RelationInputPlan::Linear(plan) => linear_plans.push(plan),
            RelationInputPlan::Union(union) => {
                relation_inputs.extend(union.branches.iter().map(|branch| &branch.plan));
            }
            RelationInputPlan::Recursive(nested) => {
                linear_plans.extend([&nested.seed, &nested.step]);
            }
        }
    }
}

/// Row-valued root windows have a public default order when callers do not
/// spell `order_by`: ascending source row id. Make that order part of each
/// root window's executable plan so `limit`/`offset` is a real `TopBy` window
/// instead of depending on source scan order.
///
/// Relation-edge materialization applies the equivalent child-local comparator
/// per correlation group; recursive closures intentionally have no injected
/// order (SPEC/16_maintained_subscription_views.md).
fn plan_with_default_result_order(
    mut plan: AnalyzedQueryPlan,
    request: &QueryProgramRequest,
) -> AnalyzedQueryPlan {
    let produces_root_rows = request.output.app_rows.is_some()
        || request
            .output
            .facts
            .contains(&ProgramFactKey::ResultMembership);
    if !produces_root_rows {
        return plan;
    }

    match &mut plan {
        AnalyzedQueryPlan::Linear(linear) => {
            inject_default_root_order(&linear.root, &mut linear.steps);
        }
        AnalyzedQueryPlan::CorrelatedPath(path) => {
            // For a correlated root, parent eligibility is established before
            // `output_steps`; the root order/window must therefore be applied
            // in that output tail, not to the parent input.
            inject_default_root_order(&path.parent.root, &mut path.output_steps);
        }
        AnalyzedQueryPlan::Union(_) => {}
        AnalyzedQueryPlan::RecursiveRelation(_) => {}
    }
    plan
}

fn inject_default_root_order(root: &LinearRoot, steps: &mut Vec<LinearStep>) {
    let Some(source) = root.source().cloned() else {
        return;
    };
    let Some(first_terminal) = steps.iter().position(|step| {
        matches!(
            step,
            LinearStep::OrderBy(_) | LinearStep::Slice { .. } | LinearStep::Aggregate { .. }
        )
    }) else {
        return;
    };

    // Explicit order and aggregate output own their order semantics.
    if matches!(
        steps.get(first_terminal),
        Some(LinearStep::OrderBy(_) | LinearStep::Aggregate { .. })
    ) {
        return;
    }
    steps.insert(first_terminal, default_root_order(source));
}

fn default_root_order(source: SourceId) -> LinearStep {
    LinearStep::OrderBy(vec![OrderKey {
        value: NormalizedValueRef::RowId(RowIdRef::Source(source)),
        direction: SortDirection::Asc,
    }])
}

fn maintained_result_membership_window_supported(plan: &AnalyzedQueryPlan) -> bool {
    let fragments = collect_plan_fragments(plan);
    !fragments.recursives.iter().any(|recursive| {
        recursive.seed.steps.iter().any(is_slice_step)
            || recursive.step.steps.iter().any(is_slice_step)
    }) && fragments
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

pub(super) fn root_aggregate_step(
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

fn linear_window_supported(_steps: &[LinearStep]) -> bool {
    // Relation-local slices use their declared row-id tie-breaker as the
    // default ascending comparator within the edge materializer. Root slices
    // have an explicit row-id OrderBy injected above.
    true
}

fn is_slice_step(step: &LinearStep) -> bool {
    matches!(step, LinearStep::Slice { .. })
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

pub(super) struct LinearFragment<'a> {
    pub(super) root: Option<&'a LinearRoot>,
    pub(super) steps: &'a [LinearStep],
}

#[derive(Default)]
pub(super) struct PlanFragments<'a> {
    pub(super) linears: Vec<LinearFragment<'a>>,
    pub(super) correlations: Vec<&'a PredicateExpr>,
    pub(super) recursives: Vec<&'a RecursiveRelationPlan>,
}

pub(super) fn collect_plan_fragments(plan: &AnalyzedQueryPlan) -> PlanFragments<'_> {
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

pub(super) fn program_sources(
    request: &QueryProgramRequest,
    plan: &AnalyzedQueryPlan,
) -> BTreeSet<SourceId> {
    let mut sources = analyzed_plan_sources(plan);
    sources.extend(request.input.shape.auxiliary_sources.iter().cloned());
    sources
}

pub(super) fn source_visibilities(plan: &AnalyzedQueryPlan) -> BTreeMap<SourceId, RowVisibility> {
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

pub(super) fn source_current_tier(
    request: &QueryProgramRequest,
    source: &SourceId,
) -> Option<DurabilityTier> {
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
        | SourceExpr::BranchView {
            projection,
            head: _,
            base: _,
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
            rows: _,
            requires_result_payload: _,
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
    visited.insert(node_id.clone());
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

pub(super) fn analyze_relation_input_node(
    node_id: &RowSetNodeId,
    nodes: &BTreeMap<RowSetNodeId, RowSetExpr>,
    visited: &mut BTreeSet<RowSetNodeId>,
) -> Result<RelationInputPlan, UnsupportedReason> {
    validate_row_set_nesting(node_id, nodes)?;
    let Some(node) = nodes.get(node_id) else {
        return Err(UnsupportedReason::Operator(format!(
            "row-set node {:?} is missing",
            node_id
        )));
    };

    match node {
        RowSetExpr::Union { inputs } => {
            visited.insert(node_id.clone());
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
            visited.insert(node_id.clone());
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

    let mut labels = BTreeSet::new();
    let mut branches = Vec::new();
    for input in inputs {
        if input.label.is_empty() || input.label.contains('\0') {
            return Err(UnsupportedReason::Operator(
                "union arm labels must be non-empty, NUL-free stable semantic identities"
                    .to_owned(),
            ));
        }
        if !labels.insert(input.label.as_str()) {
            return Err(UnsupportedReason::Operator(format!(
                "union arm label {:?} is duplicated; occurrence identity requires unique stable semantic arm labels",
                input.label
            )));
        }
        let plan = analyze_relation_input_node(&input.node, nodes, visited)?;
        branches.push(UnionBranchPlan {
            label: input.label.clone(),
            plan,
        });
    }
    Ok(UnionPlan { branches })
}

#[cfg(test)]
pub(crate) fn analyzed_union_labels(
    inputs: &[UnionInput],
    nodes: &BTreeMap<RowSetNodeId, RowSetExpr>,
) -> Result<Vec<String>, UnsupportedReason> {
    analyze_union(inputs, nodes, &mut BTreeSet::new()).map(|plan| {
        plan.branches
            .into_iter()
            .map(|branch| branch.label)
            .collect()
    })
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
        PredicateExpr::EnumMatch { value, payload, .. } => {
            value_contains_param(value) || predicate_contains_param(payload)
        }
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

#[cfg(test)]
mod nesting_tests {
    use super::*;

    fn nested_relation_shapes(depth: usize) -> (RowSetNodeId, BTreeMap<RowSetNodeId, RowSetExpr>) {
        let mut nodes = BTreeMap::new();
        let leaf = RowSetNodeId("leaf".to_owned());
        nodes.insert(
            leaf.clone(),
            RowSetExpr::ValueSource {
                shape: "test".to_owned(),
                columns: Vec::new(),
                mode: ValueSourceMode::Inline,
            },
        );
        let mut input = leaf.clone();
        for index in 1..depth {
            let node = RowSetNodeId(format!("nested-{index}"));
            let side = RowSetNodeId(format!("side-{index}"));
            nodes.insert(
                side.clone(),
                RowSetExpr::ValueSource {
                    shape: format!("side-{index}"),
                    columns: Vec::new(),
                    mode: ValueSourceMode::Inline,
                },
            );
            let expression = match index % 3 {
                0 => RowSetExpr::Join {
                    left: input,
                    right: side,
                    mode: JoinMode::Semi,
                    on: PredicateExpr::True,
                },
                1 => RowSetExpr::Union {
                    inputs: vec![UnionInput {
                        node: input,
                        label: format!("arm-{index}"),
                    }],
                },
                _ => {
                    let frontier = FrontierId(format!("frontier-{index}"));
                    RowSetExpr::RecursiveRelation {
                        seed: input,
                        step: side,
                        frontier: frontier.clone(),
                        frontier_key: NormalizedValueRef::FrontierColumn {
                            frontier,
                            field: "key".to_owned(),
                        },
                        dedupe_keys: Vec::new(),
                        bound: RecursionBound::MaxDepth(1),
                    }
                }
            };
            nodes.insert(node.clone(), expression);
            input = node;
        }
        (input, nodes)
    }

    #[test]
    fn row_set_nesting_accepts_the_limit_and_rejects_the_next_level() {
        let (accepted_root, accepted_nodes) = nested_relation_shapes(MAX_ROW_SET_NESTING_DEPTH);
        validate_row_set_nesting(&accepted_root, &accepted_nodes).expect("depth limit is accepted");

        let (rejected_root, rejected_nodes) = nested_relation_shapes(MAX_ROW_SET_NESTING_DEPTH + 1);
        let error = validate_row_set_nesting(&rejected_root, &rejected_nodes)
            .expect_err("depth beyond limit is rejected");
        assert!(
            matches!(error, UnsupportedReason::Operator(message) if message.contains("MAX_ROW_SET_NESTING_DEPTH (256)"))
        );
    }

    #[test]
    fn row_set_nesting_and_analysis_accept_a_shared_child_diamond() {
        let leaf = RowSetNodeId("shared".to_owned());
        let root = RowSetNodeId("root".to_owned());
        let nodes = BTreeMap::from([
            (
                leaf.clone(),
                RowSetExpr::ValueSource {
                    shape: "test".to_owned(),
                    columns: Vec::new(),
                    mode: ValueSourceMode::Inline,
                },
            ),
            (
                root.clone(),
                RowSetExpr::Join {
                    left: leaf.clone(),
                    right: leaf,
                    mode: JoinMode::Semi,
                    on: PredicateExpr::True,
                },
            ),
        ]);
        validate_row_set_nesting(&root, &nodes).expect("shared DAG is valid");
        analyze_relation_input_node(&root, &nodes, &mut BTreeSet::new())
            .expect("shared DAG is duplicated into the owned analysis plan");
    }

    #[test]
    fn row_set_nesting_rejects_an_active_path_cycle() {
        let root = RowSetNodeId("cycle".to_owned());
        let nodes = BTreeMap::from([(
            root.clone(),
            RowSetExpr::Project {
                input: root.clone(),
                columns: Vec::new(),
            },
        )]);
        let error = validate_row_set_nesting(&root, &nodes).expect_err("cycle is invalid");
        assert!(
            matches!(error, UnsupportedReason::Operator(message) if message.contains("row-set nesting contains a cycle"))
        );
    }

    #[test]
    fn direct_relation_analysis_enforces_the_nesting_limit() {
        let (root, nodes) = nested_relation_shapes(MAX_ROW_SET_NESTING_DEPTH + 1);
        let error = analyze_relation_input_node(&root, &nodes, &mut BTreeSet::new())
            .expect_err("closure callers must receive the nesting diagnostic");
        assert!(
            matches!(error, UnsupportedReason::Operator(message) if message.contains("MAX_ROW_SET_NESTING_DEPTH (256)"))
        );
    }
}
