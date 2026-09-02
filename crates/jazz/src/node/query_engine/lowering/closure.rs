//! Closure-path membership and required-include lowering.
//!
//! Keeping this boundary separate from terminal construction makes the
//! authorization-shaped graph construction easier to audit in isolation.

use super::*;

#[derive(Clone, Debug)]
pub(super) struct ClosureLowering {
    pub(super) visible_root: GraphBuilder,
    pub(super) result_members: BTreeMap<SourceId, GraphBuilder>,
}

impl ClosureLowering {
    /// The exact source relations whose rows are allowed to cross an
    /// authority-program boundary.  This is deliberately derived from the
    /// same closure that establishes public root membership: a source graph
    /// alone only applies source-local policy and can miss residual/reachable
    /// constraints owned by the complete program.
    pub(super) fn covered_source_members(
        &self,
        root_source: SourceId,
    ) -> BTreeMap<SourceId, GraphBuilder> {
        let mut members = self.result_members.clone();
        members
            .entry(root_source)
            .and_modify(|existing| {
                *existing = GraphBuilder::union([existing.clone(), self.visible_root.clone()]);
            })
            .or_insert_with(|| self.visible_root.clone());
        members
    }
}

pub(super) fn lower_closure_membership(
    root_graph: GraphBuilder,
    request: &QueryProgramRequest,
    root_source: &ResolvedSource,
    resolved_sources: &BTreeMap<SourceId, ResolvedSource>,
    route_fields: &BTreeSet<String>,
    root_carrier_fields: &BTreeSet<String>,
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
                root_carrier_fields,
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

pub(super) fn reachable_contribution_membership_graph(
    visible_root: GraphBuilder,
    contribution: &ReachableContribution,
    root_source: &ResolvedSource,
    contribution_source: &ResolvedSource,
    nodes: &BTreeMap<RowSetNodeId, RowSetExpr>,
    resolved_sources: &BTreeMap<SourceId, ResolvedSource>,
    request: &QueryProgramRequest,
    route_fields: &BTreeSet<String>,
) -> CapabilityResult<GraphBuilder> {
    let mut visited = BTreeSet::new();
    let plan = analyze_relation_input_node(&contribution.access_input, nodes, &mut visited)
        .map_err(single_gap_report)?;
    let lowered = lower_relation_input_for_contributor(&plan, resolved_sources, request)
        .map_err(single_gap_report)?;
    let join_field =
        reachable_root_reference_field(contribution_source, &contribution.root_ref_field);
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
    .project_fields(project_join_contribution_fields_with_root_routes(
        contribution_source,
        route_fields,
    )))
}

/// Return the lowered access-row field that identifies the visible root.
///
/// `ReachableVia` names the application-level `id` column even when a table
/// uses Jazz's implicit row id. In that case normalization lowers the root
/// join through `row_uuid`, rather than a nonexistent `user_id` field. The
/// contributor terminal must use the same physical coordinate; otherwise a
/// plain gather query is rejected before it can publish any receiver inputs.
/// Declared ids and ordinary application columns remain `user_*` fields.
fn reachable_root_reference_field(source: &ResolvedSource, field: &str) -> String {
    let user_field = user_column_field(field);
    if source
        .row_shape
        .descriptor
        .field_index(&user_field)
        .is_some()
        || field != "id"
    {
        user_field
    } else {
        source.row_shape.row_uuid_field.clone()
    }
}

/// Return the generic side stream owned by the one recursive step that
/// physically reads `edge_source`. This deliberately follows the normalized
/// relation plan instead of joining the final reachable frontier back to an
/// edge table: the latter cannot distinguish a max-depth frontier from a
/// node whose outgoing edges were actually evaluated.
pub(super) fn reachable_step_witness_membership_graph(
    contribution: &ReachableContribution,
    nodes: &BTreeMap<RowSetNodeId, RowSetExpr>,
    resolved_sources: &BTreeMap<SourceId, ResolvedSource>,
    request: &QueryProgramRequest,
    route_fields: &BTreeSet<String>,
) -> CapabilityResult<GraphBuilder> {
    // Only application binding routes can cross from the authority residual
    // into a receiver input.  The raw recursive step may have used trusted
    // claim carriers while evaluating policy, but those are intentionally
    // absent from the client-local descriptor.
    let receiver_routes = receiver_routing_fields(request).map_err(single_gap_report)?;
    let route_fields = route_fields
        .intersection(&receiver_routes)
        .cloned()
        .collect::<BTreeSet<_>>();
    let mut visited = BTreeSet::new();
    let plan = analyze_relation_input_node(&contribution.access_input, nodes, &mut visited)
        .map_err(single_gap_report)?;
    let recursive =
        recursive_relation_for_edge(&plan, &contribution.edge_source).ok_or_else(|| {
            single_gap_report(UnsupportedReason::Operator(format!(
                "reachable contribution {} has no recursive step for edge source {:?}",
                contribution.id, contribution.edge_source
            )))
        })?;
    let root_id = recursive.root_source().ok_or_else(|| {
        single_gap_report(UnsupportedReason::Operator(
            "recursive step witness requires a source root".to_owned(),
        ))
    })?;
    let root_source = resolved_sources.get(root_id).ok_or_else(|| {
        single_gap_report(UnsupportedReason::Runtime(format!(
            "recursive witness root {:?} was not resolved",
            root_id
        )))
    })?;
    let edge_source = resolved_sources
        .get(&contribution.edge_source)
        .ok_or_else(|| {
            single_gap_report(UnsupportedReason::Runtime(format!(
                "recursive edge witness source {:?} was not resolved",
                contribution.edge_source
            )))
        })?;
    let lowered = lower_recursive_relation_cached(
        None,
        recursive,
        root_source,
        resolved_sources,
        request,
        None,
    )
    .map_err(single_gap_report)?;
    Ok(
        GraphBuilder::recursive_step_witness(lowered.graph).project_fields(
            project_join_contribution_fields_with_root_routes(edge_source, &route_fields),
        ),
    )
}

/// Return the exact recursive seed rows that participate in a reachable
/// contributor. A receiver rebuilds the recursive relation from both its
/// initial frontier and the evaluated steps, so source coverage must name the
/// seed occurrence as well as the step witness. Reopening the seed table
/// locally would turn an authority-scoped closure into an unfiltered source
/// scan; lower the authority's seed plan and freeze only its admitted rows.
pub(super) fn reachable_seed_membership_graph(
    contribution: &ReachableContribution,
    nodes: &BTreeMap<RowSetNodeId, RowSetExpr>,
    resolved_sources: &BTreeMap<SourceId, ResolvedSource>,
    request: &QueryProgramRequest,
    route_fields: &BTreeSet<String>,
) -> CapabilityResult<(SourceId, GraphBuilder)> {
    let receiver_routes = receiver_routing_fields(request).map_err(single_gap_report)?;
    let route_fields = route_fields
        .intersection(&receiver_routes)
        .cloned()
        .collect::<BTreeSet<_>>();
    let mut visited = BTreeSet::new();
    let plan = analyze_relation_input_node(&contribution.access_input, nodes, &mut visited)
        .map_err(single_gap_report)?;
    let recursive =
        recursive_relation_for_edge(&plan, &contribution.edge_source).ok_or_else(|| {
            single_gap_report(UnsupportedReason::Operator(format!(
                "reachable contribution {} has no recursive seed for edge source {:?}",
                contribution.id, contribution.edge_source
            )))
        })?;
    let seed_id = recursive.seed_source().ok_or_else(|| {
        single_gap_report(UnsupportedReason::Operator(
            "recursive seed witness requires a source root".to_owned(),
        ))
    })?;
    let seed_source = resolved_sources.get(seed_id).ok_or_else(|| {
        single_gap_report(UnsupportedReason::Runtime(format!(
            "recursive seed source {seed_id:?} was not resolved"
        )))
    })?;
    let seed = lower_recursive_seed_membership(recursive, seed_source, resolved_sources, request)
        .map_err(single_gap_report)?;
    Ok((
        seed_id.clone(),
        seed.graph.project_fields(project_source_fields_with_routes(
            seed_source,
            &route_fields,
        )),
    ))
}

fn recursive_relation_for_edge<'a>(
    plan: &'a RelationInputPlan,
    edge_source: &SourceId,
) -> Option<&'a RecursiveRelationPlan> {
    let mut pending = vec![plan];
    while let Some(plan) = pending.pop() {
        match plan {
            RelationInputPlan::Recursive(relation)
                if relation.step_source() == Some(edge_source) =>
            {
                return Some(relation);
            }
            RelationInputPlan::Recursive(_) => {}
            RelationInputPlan::Linear(linear) => {
                for step in linear.steps.iter().rev() {
                    if let LinearStep::Join { right, .. } = step {
                        pending.push(right);
                    }
                }
            }
            RelationInputPlan::Union(union) => {
                pending.extend(union.branches.iter().rev().map(|branch| &branch.plan));
            }
        }
    }
    None
}

pub(super) fn join_contribution_membership_graph(
    visible_root: GraphBuilder,
    contribution: &JoinContribution,
    root_source: &ResolvedSource,
    contribution_source: &ResolvedSource,
    nodes: &BTreeMap<RowSetNodeId, RowSetExpr>,
    resolved_sources: &BTreeMap<SourceId, ResolvedSource>,
    request: &QueryProgramRequest,
    route_fields: &BTreeSet<String>,
) -> CapabilityResult<GraphBuilder> {
    let mut visited = BTreeSet::new();
    let plan = analyze_relation_input_node(&contribution.input, nodes, &mut visited)
        .map_err(single_gap_report)?;
    let lowered = lower_relation_input_for_contributor(&plan, resolved_sources, request)
        .map_err(single_gap_report)?;
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
            project_join_contribution_fields_with_root_routes(contribution_source, route_fields),
        ),
    )
}

/// Derive one flat-join source from the already-authorized rendered root.
///
/// Flat output projection deliberately gives every public source field a
/// scope-qualified name (`posts.author_id`, `people.id`, …).  Unlike generic
/// join contributions, the rendered root has lost its internal source field
/// names, so map the normalized predicate through that explicit public layout
/// before joining it to the post-policy right relation.
pub(super) fn flat_join_contribution_membership_graph(
    visible_root: GraphBuilder,
    contribution: &JoinContribution,
    contribution_source: &ResolvedSource,
    nodes: &BTreeMap<RowSetNodeId, RowSetExpr>,
    resolved_sources: &BTreeMap<SourceId, ResolvedSource>,
    request: &QueryProgramRequest,
    route_fields: &BTreeSet<String>,
) -> CapabilityResult<GraphBuilder> {
    let mut visited = BTreeSet::new();
    let plan = analyze_relation_input_node(&contribution.input, nodes, &mut visited)
        .map_err(single_gap_report)?;
    let lowered = lower_relation_input_for_contributor(&plan, resolved_sources, request)
        .map_err(single_gap_report)?;
    let PredicateExpr::Compare {
        left,
        op: ComparisonOp::Eq,
        right,
    } = &contribution.membership
    else {
        return Err(single_gap_report(UnsupportedReason::Operator(
            "flat join contribution membership must be equality".to_owned(),
        )));
    };
    let (visible_key, relation_key) = match (
        flat_join_visible_field(left, nodes),
        lower_relation_key_ref(right, &plan, &lowered, request),
    ) {
        (Ok(visible), Ok(relation)) => (visible, relation),
        _ => (
            flat_join_visible_field(right, nodes).map_err(single_gap_report)?,
            lower_relation_key_ref(left, &plan, &lowered, request).map_err(single_gap_report)?,
        ),
    };
    let mut relation_graph = lowered.graph;
    if lowered.nullable_fields.contains(&relation_key) {
        relation_graph = unwrap_nullable_join_key(
            relation_graph,
            relation_key.clone(),
            lowered
                .nullable_field_depths
                .get(&relation_key)
                .copied()
                .unwrap_or(1),
        );
    }
    Ok(
        GraphBuilder::join(visible_root, relation_graph, [visible_key], [relation_key])
            .project_fields(project_join_contribution_fields_with_root_routes(
                contribution_source,
                route_fields,
            )),
    )
}

/// A contributor is selected by joining its relation to the authority's
/// admitted root. Row fields are produced by the right input, while routing
/// fields are produced by the left root. Preserve both explicitly before the
/// source is frozen as a receiver input.
pub(super) fn project_join_contribution_fields_with_root_routes(
    source: &ResolvedSource,
    route_fields: &BTreeSet<String>,
) -> Vec<ProjectField> {
    // A resolved child descriptor can itself retain the root's routing
    // carrier.  That does *not* mean the child-side join input owns a
    // `right.<route>` field: the carrier belongs to the admitted left/root
    // frontier. Project it once from left and exclude the descriptor echo
    // from the right projection, keeping the join layout unambiguous.
    let mut fields = source
        .row_shape
        .descriptor
        .fields()
        .iter()
        .filter_map(|field| field.name.as_ref())
        .filter(|field| !route_fields.contains(*field))
        .map(|field| ProjectField::renamed(format!("{RIGHT_JOIN_PREFIX}{field}"), field.clone()))
        .collect::<Vec<_>>();
    fields.extend(
        route_fields.iter().map(|field| {
            ProjectField::renamed(format!("{LEFT_JOIN_PREFIX}{field}"), field.clone())
        }),
    );
    fields
}

fn flat_join_visible_field(
    value: &NormalizedValueRef,
    nodes: &BTreeMap<RowSetNodeId, RowSetExpr>,
) -> Result<String, UnsupportedReason> {
    // The rendered flat root is the compiler-owned `flat_join:output`
    // projection. Its field names intentionally preserve the query's public
    // aliases (`root.title`, `peer.title`, …), which cannot be recovered from
    // a `SourceId`: the root occurrence has no alias in its path. Resolve the
    // exact normalized value through that projection instead of reconstructing
    // a spelling from a table name. This also keeps the contributor terminal
    // aligned with future projection/layout changes.
    let Some(RowSetExpr::Project { columns, .. }) =
        nodes.get(&RowSetNodeId("flat_join:output".to_owned()))
    else {
        return Err(UnsupportedReason::Operator(
            "flat join contribution is missing its output projection".to_owned(),
        ));
    };
    columns
        .iter()
        .find(|column| &column.value == value)
        .map(|column| column.output.name.clone())
        .ok_or_else(|| {
            UnsupportedReason::Operator(
                "flat join contribution key is not retained by the output projection".to_owned(),
            )
        })
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
        // Matching an optional reference requires temporarily unwrapping its
        // nullable source cell. That unwrapped copy is only a predicate
        // witness: publishing it as the root would change the whole-row
        // terminal descriptor. Select the authoritative parent rows through
        // their stable identity so the prepared and incremental layouts keep
        // the source carrier exactly.
        let covered_roots = covered.project_fields([ProjectField::named(parent_row_uuid.clone())]);
        return Ok(GraphBuilder::semi_join(
            parent_graph,
            covered_roots,
            [parent_row_uuid.clone()],
            [parent_row_uuid],
        )
        .project_fields(project_source_fields_with_routes(
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
    // Closure lowering only needs source shape metadata while it walks the
    // path.  Keep references rather than cloning whole resolved sources
    // (which include the complete table schema and graph) into this
    // synchronous compiler stack.
    let mut current_source = root_source;
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
        if index + 1 == segments.len() {
            // The terminal graph is only consumed by the result-member
            // collector, so moving it avoids recursively cloning the entire
            // assembled closure graph on the deepest path.
            outputs.push((index, segment.target.clone(), joined));
            break;
        }
        outputs.push((index, segment.target.clone(), joined.clone()));
        current_graph = joined;
        current_source = target;
    }
    Ok(outputs)
}

pub(super) fn closure_path_segments(path: &ClosurePath) -> Vec<&ClosurePathSegment> {
    match path {
        ClosurePath::ImplicitRootReference { segment, .. } => vec![segment],
        ClosurePath::ExplicitInclude { segments, .. } => segments.iter().collect(),
    }
}
