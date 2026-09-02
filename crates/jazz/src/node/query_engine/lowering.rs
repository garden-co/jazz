use super::*;
use crate::protocol::ProgramSourceId;
mod collect_layout;
use collect_layout::*;
use groove::ivm::{
    AggregateExpr as GrooveAggregateExpr, AggregateFunction as GrooveAggregateFunction,
    CollectByField, CollectBySlotBuilder, FieldRef, LiteralValue, MAX_COLLECT_BY_TREE_DEPTH,
    PlanExpr as GroovePlanExpr, PredicateExpr as GroovePredicateExpr, PredicateKind, ProjectField,
    TopByLimit, TopByOrder,
};
use groove::records::{ValueType, collect_by_ordered_scalar};

mod closure;
use closure::{
    closure_path_segments, flat_join_contribution_membership_graph,
    join_contribution_membership_graph, lower_closure_membership,
};

// Groove returns RecursiveIterationLimit instead of silently truncating when
// this bound is reached before convergence.
const FIXPOINT_MAX_ITERS: usize = 128;
fn public_root_field_name(source: &ResolvedSource, field: &CollectFlatField) -> String {
    let source_field = field.source_field.as_deref().unwrap_or(&field.output);
    let logical = logical_user_column(source_field);
    if source
        .table_schema
        .columns
        .iter()
        .any(|column| column.name == logical)
    {
        logical_user_column(&field.output).to_owned()
    } else {
        // Collector slots already carry their public path field as their
        // physical descriptor name. Do not infer from a reserved-looking
        // prefix here: table columns may legitimately use any such name.
        field.output.clone()
    }
}

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

/// Owned declarative Groove inputs prepared before pure Jazz lowering.
pub(crate) type ResolvedQuerySources = BTreeMap<SourceId, ResolvedSource>;

/// Analyze the logical source requests for one program without preparing or
/// lowering any source. Compilation orchestration uses this to discover
/// dependent policy programs before source preparation begins.
pub(crate) fn query_program_source_requests(
    request: &QueryProgramRequest,
) -> CapabilityResult<Vec<SourceRequest>> {
    let plan = analyze_query_plan(request).map_err(|gaps| {
        Box::new(CapabilityReport {
            gaps,
            explain: explain_with_request(request, ExplainPlan::default()),
        })
    })?;
    let source_visibilities = source_visibilities(&plan);
    source_requirements(request, &plan)?
        .into_iter()
        .map(|(source, requirements)| {
            Ok(SourceRequest {
                visibility: source_visibilities
                    .get(&source)
                    .copied()
                    .unwrap_or(RowVisibility::Visible),
                authorization: source_authorization_for_source(request, &source)?,
                source,
                requirements,
            })
        })
        .collect()
}

/// Prepare concrete source descriptions, then synchronously lower the program.
///
/// This is an explicit compatibility boundary while snapshot capture and
/// physical-layout preparation remain async. Runtime production code calls
/// this function; the lowering phase itself is [`lower_resolved_query_program`].
pub(crate) async fn prepare_and_lower_query_program(
    request: QueryProgramRequest,
    source_preparer: &mut impl SourceGraphPreparer,
) -> QueryCompileResult {
    let mut explain = ExplainPlan::default();

    let _plan = match analyze_query_plan(&request) {
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

    let mut resolved_sources = BTreeMap::new();
    for source_request in query_program_source_requests(&request)? {
        let source = source_request.source.clone();
        // Source preparation is the remaining async compatibility boundary.
        // Policy programs have already been prepared by compilation
        // orchestration; this future is only for source-local snapshot and
        // physical-layout work that has not migrated to Groove yet.
        let resolved_source =
            match Box::pin(source_preparer.prepare_source_graph(&source_request)).await {
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
        // A receiver-local maintained subscription may replace an
        // authority-approved source closure through runtime-owned input
        // sources.  Those inputs are allocated by the receiving database and
        // remain wholly local; the frozen `ProgramSourceId` maps them back to
        // exactly one normalized source before this lowering boundary.
        explain.physical.push(format!(
            "source {:?} ({:?}) -> resolved table {}",
            source,
            source_current_tier(&request, &source),
            resolved_source.table_schema.name
        ));
        resolved_sources.insert(source, resolved_source);
    }
    lower_resolved_query_program(request, resolved_sources, explain)
}

/// Purely lower a Jazz request whose Groove sources have already been prepared.
///
/// This function performs no storage access, hydration, registration, or
/// evaluation and therefore must remain synchronous.
pub(crate) fn lower_resolved_query_program(
    request: QueryProgramRequest,
    resolved_sources: ResolvedQuerySources,
    mut explain: ExplainPlan,
) -> QueryCompileResult {
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

    let internal_app_rows_graph = (request.output.app_rows.is_some()
        && root_aggregate_step(&plan).is_none())
    .then(|| lowered.graph.clone());
    let terminals = lowered_terminals(
        lowered.graph,
        &request,
        &plan,
        &resolved_root,
        &resolved_sources,
        &parameters,
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
    // `resolved_sources` also contains authority-local proof and existence
    // reads. A receiver may allocate only the exact occurrences that have a
    // post-policy residual version-witness terminal.
    let covered_input_source_descriptors = covered_input_source_descriptors(&terminals)?;

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
            internal_app_rows_graph,
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
        source_descriptors: resolved_sources
            .iter()
            .map(|(source, resolved)| {
                (
                    source.program_source_id(),
                    resolved.row_shape.descriptor.clone(),
                )
            })
            .collect(),
        covered_input_source_descriptors,
        request,
        explain,
    })
}

fn covered_input_source_descriptors(
    terminals: &[LoweredTerminal],
) -> CapabilityResult<BTreeMap<ProgramSourceId, RecordDescriptor>> {
    let mut descriptors = BTreeMap::new();
    for terminal in terminals {
        let OutputTerminalSchema::Fact(ProgramFactOutput {
            key: ProgramFactKey::VersionWitnesses,
            schema: ProgramFactSchema::VersionWitnesses(schema),
            ..
        }) = &terminal.output
        else {
            continue;
        };
        let Some(witness) = schema.content.as_ref() else {
            continue;
        };
        if let Some(existing) =
            descriptors.insert(witness.source.clone(), witness.descriptor.clone())
            && existing != witness.descriptor
        {
            return Err(single_gap_report(UnsupportedReason::Runtime(
                "covered input source has conflicting compiled descriptors".to_owned(),
            )));
        }
    }
    Ok(descriptors)
}

#[cfg(test)]
pub(crate) async fn lower_query_program(
    request: QueryProgramRequest,
    source_preparer: &mut impl SourceGraphPreparer,
) -> QueryCompileResult {
    prepare_and_lower_query_program(request, source_preparer).await
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
    // Policy lowering can build deeply nested finite graphs on the server
    // shell's ordinary thread stack. Keep this structural analysis iterative:
    // it is used while installing those policies, before Groove compiles its
    // own graph representation.
    let mut outputs =
        std::collections::HashMap::<*const GraphBuilder, Option<BTreeSet<String>>>::new();
    for node in graph_builder_postorder(graph) {
        let child_output = |child: &GraphBuilder| {
            outputs
                .get(&std::ptr::from_ref(child))
                .expect("postorder visits graph children before their parent")
                .clone()
        };
        let fields = match node {
            GraphBuilder::InlineRecords { output, .. }
            | GraphBuilder::FrontierSource { output, .. }
            | GraphBuilder::BindingSource { output, .. } => descriptor_named_fields(output),
            GraphBuilder::InputSource { .. } => None,
            GraphBuilder::Project { fields, .. } => Some(
                fields
                    .iter()
                    .map(|field| field.output_name.clone())
                    .collect(),
            ),
            GraphBuilder::StreamingChecksum {
                input,
                field,
                output_field,
                ..
            } => child_output(input).and_then(|mut fields| match field {
                FieldRef::Name(field) => {
                    fields.remove(field);
                    fields.insert(output_field.clone());
                    Some(fields)
                }
                FieldRef::Resolved(_) => None,
            }),
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
            GraphBuilder::CollectBy { collect, .. } => Some(
                collect
                    .parent_fields
                    .iter()
                    .map(|field| field.output_name.clone())
                    .chain(std::iter::once(collect.collection_field.clone()))
                    .collect(),
            ),
            GraphBuilder::Filter { input, .. }
            | GraphBuilder::UnwrapNullable { input, .. }
            | GraphBuilder::VariantProject { input, .. }
            | GraphBuilder::ArgMaxBy { input, .. }
            | GraphBuilder::ArgMinBy { input, .. }
            | GraphBuilder::TopBy { input, .. }
            | GraphBuilder::SemiJoin { left: input, .. }
            | GraphBuilder::AntiJoin { left: input, .. } => child_output(input),
            GraphBuilder::Unnest {
                input,
                element_field,
                ..
            } => child_output(input).map(|mut fields| {
                fields.insert(element_field.clone());
                fields
            }),
            GraphBuilder::Recursive { seed, .. } => child_output(seed),
            GraphBuilder::RecursiveStepWitness { recursive } => match recursive.as_ref() {
                GraphBuilder::Recursive {
                    step_witness: Some(witness),
                    ..
                } => child_output(witness),
                _ => None,
            },
            GraphBuilder::Union { inputs } => inputs.split_first().and_then(|(first, rest)| {
                let mut fields = child_output(first)?;
                for input in rest {
                    fields = fields
                        .intersection(&child_output(input)?)
                        .cloned()
                        .collect();
                }
                Some(fields)
            }),
            GraphBuilder::Join { left, right, .. } => child_output(left)
                .zip(child_output(right))
                .map(|(left_fields, right_fields)| {
                    let mut fields = BTreeSet::new();
                    fields.extend(left_fields.into_iter().map(|field| left_field(&field)));
                    fields.extend(right_fields.into_iter().map(|field| right_field(&field)));
                    fields
                }),
            GraphBuilder::Table { .. } | GraphBuilder::Index { .. } => None,
        };
        outputs.insert(std::ptr::from_ref(node), fields);
    }
    outputs
        .remove(&std::ptr::from_ref(graph))
        .expect("postorder includes the graph root")
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
    // Client-local results are scoped by the upstream emission boundary, not
    // by a second, potentially stale/incomplete local policy evaluation.
    if request.authorization_mode == QueryAuthorizationMode::ClientLocal {
        if std::env::var_os("JAZZ_COVERED_INPUT_TRACE").is_some() {
            eprintln!(
                "JAZZ_COVERED_INPUT_TRACE stage=source_authorization mode=client_local source={source:?} policy={:?} authorization=system",
                request.policy,
            );
        }
        return Ok(SourceAuthorizationRequest::System);
    }
    let authorization = match &request.policy {
        PolicyContext::System => SourceAuthorizationRequest::System,
        PolicyContext::AuthorizationSubplan {
            protected_source, ..
        } if protected_source == source => SourceAuthorizationRequest::System,
        PolicyContext::Identity {
            permission_subject, ..
        } => SourceAuthorizationRequest::PolicyFiltered {
            permission_subject: *permission_subject,
            plan: PolicyAuthorizationPlan {
                protected_source: source.clone(),
                role: PolicyDecisionRole::Read,
                protected_row_field: "row_uuid".to_owned(),
                binding_source_shape: request.input.binding.source_shape.clone(),
                binding_user_params: binding_user_param_types(&request.input.binding)?,
                binding_claim_params: request.input.binding.claim_params.clone(),
            },
        },
        // Auxiliary closure and payload sources do not establish policy
        // membership, so proof compilation reads them under system authority.
        PolicyContext::AuthorizationSubplan { .. } => SourceAuthorizationRequest::System,
    };
    if std::env::var_os("JAZZ_COVERED_INPUT_TRACE").is_some() {
        eprintln!(
            "JAZZ_COVERED_INPUT_TRACE stage=source_authorization mode=trusted_serving source={source:?} policy={:?} authorization={authorization:?}",
            request.policy,
        );
    }
    Ok(authorization)
}

fn binding_user_param_types(
    binding: &ProgramBinding,
) -> CapabilityResult<BTreeMap<String, ColumnType>> {
    let mut params = binding.extra_user_params.clone();
    for name in binding.values.keys() {
        if binding.claim_params.contains_key(name) {
            continue;
        }
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
                        if claim_route_is_ordered_scalar(&column.ty) {
                            domain.routing_params.insert(param);
                        }
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

#[cfg(test)]
pub(crate) fn parameter_domain_for_shape_for_test(
    shape: &NormalizedRowSetShape,
) -> ParameterDomain {
    parameter_domain(shape)
}

fn claim_route_is_ordered_scalar(ty: &ColumnType) -> bool {
    collect_by_ordered_scalar(ty)
}

fn parameter_domain_for_request(
    request: &QueryProgramRequest,
) -> Result<ParameterDomain, UnsupportedReason> {
    let mut domain = parameter_domain(&request.input.shape);
    for (name, ty) in &request.input.binding.extra_user_params {
        if let Some(existing) = domain.user_params.get(name)
            && existing != ty
        {
            return Err(UnsupportedReason::Runtime(format!(
                "binding parameter '{name}' has inconsistent validated types"
            )));
        }
        domain.user_params.insert(name.clone(), ty.clone());
    }
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
        domain.user_params.remove(&name);
        domain.claim_params.insert(name.clone(), claim);
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
        | PredicateExpr::EnumMatch { .. }
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
    for node in graph_builder_postorder(graph) {
        let GraphBuilder::BindingSource { output, .. } = node else {
            continue;
        };
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
                        ty: field.value_type.clone(),
                    });
                if claim_route_is_ordered_scalar(&field.value_type) {
                    domain.routing_params.insert(name.to_owned());
                }
            } else {
                domain
                    .user_params
                    .entry(name.to_owned())
                    .or_insert_with(|| field.value_type.clone());
                domain.routing_params.insert(route_param_field(name));
            }
        }
    }
}

/// Jazz query lowering owns declarative sources only. A mutable Groove input
/// is a receiver-local runtime adapter, so it must be composed after lowering
/// rather than smuggled through an app/query source resolver.
fn graph_contains_input_source(graph: &GraphBuilder) -> bool {
    graph_builder_postorder(graph)
        .into_iter()
        .any(|node| matches!(node, GraphBuilder::InputSource { .. }))
}

/// Return builder nodes in child-before-parent order without consuming the
/// calling thread's stack. Builder graphs are finite by construction; shared
/// children are intentionally visited once per structural occurrence, matching
/// the previous recursive walkers.
fn graph_builder_postorder(graph: &GraphBuilder) -> Vec<&GraphBuilder> {
    let mut pending = vec![(graph, false)];
    let mut ordered = Vec::new();
    while let Some((node, visited)) = pending.pop() {
        if visited {
            ordered.push(node);
            continue;
        }
        pending.push((node, true));
        match node {
            GraphBuilder::Recursive {
                seed,
                step,
                step_witness,
                ..
            } => {
                if let Some(witness) = step_witness {
                    pending.push((witness, false));
                }
                pending.push((step, false));
                pending.push((seed, false));
            }
            GraphBuilder::RecursiveStepWitness { recursive } => pending.push((recursive, false)),
            GraphBuilder::Filter { input, .. }
            | GraphBuilder::UnwrapNullable { input, .. }
            | GraphBuilder::Unnest { input, .. }
            | GraphBuilder::VariantProject { input, .. }
            | GraphBuilder::Project { input, .. }
            | GraphBuilder::StreamingChecksum { input, .. }
            | GraphBuilder::ArgMaxBy { input, .. }
            | GraphBuilder::ArgMinBy { input, .. }
            | GraphBuilder::TopBy { input, .. }
            | GraphBuilder::CollectBy { input, .. }
            | GraphBuilder::Aggregate { input, .. } => pending.push((input, false)),
            GraphBuilder::Union { inputs } => {
                pending.extend(inputs.iter().rev().map(|input| (input.as_ref(), false)));
            }
            GraphBuilder::Join { left, right, .. }
            | GraphBuilder::SemiJoin { left, right, .. }
            | GraphBuilder::AntiJoin { left, right, .. } => {
                pending.push((right, false));
                pending.push((left, false));
            }
            GraphBuilder::Table { .. }
            | GraphBuilder::InlineRecords { .. }
            | GraphBuilder::InputSource { .. }
            | GraphBuilder::Index { .. }
            | GraphBuilder::FrontierSource { .. }
            | GraphBuilder::BindingSource { .. } => {}
        }
    }
    ordered
}

#[cfg(test)]
mod stack_receipts {
    use super::*;

    #[test]
    fn deep_declared_field_discovery_stays_on_a_server_sized_stack() {
        // This is an internal receipt because it exercises the compiler's
        // structural walk directly. Production reaches it while installing a
        // policy graph on the server shell, whose default thread stack is 2
        // MiB; the public policy scenario that exposed the bug cannot isolate
        // this walk from transport and subscription work.
        let completed = std::thread::Builder::new()
            .name("deep-query-engine-graph-receipt".to_owned())
            .stack_size(2 * 1024 * 1024)
            .spawn(|| {
                let mut graph = GraphBuilder::table("records");
                for _ in 0..8_192 {
                    graph = graph.filter(GroovePredicateExpr::gt("id", Value::U64(0)));
                }
                assert_eq!(graph_declared_output_fields(&graph), None);
                // The receipt targets the lowering walk. Dropping this
                // deliberately deep Arc chain remains independently recursive
                // in GraphBuilder itself.
                std::mem::forget(graph);
            })
            .expect("spawn server-sized stack receipt")
            .join();

        assert!(
            completed.is_ok(),
            "declared-field discovery must not recurse through deep policy graphs"
        );
    }
}

mod planning;
use planning::*;
mod requirements;
use requirements::*;
mod graph_lowering;
use graph_lowering::*;
mod terminals;
use terminals::*;

#[cfg(test)]
pub(crate) use graph_lowering::binding_value_source_projection_fields_for_test;
#[cfg(test)]
pub(crate) use graph_lowering::receiver_routing_fields;
#[cfg(test)]
pub(crate) use planning::analyzed_union_labels;
#[cfg(test)]
pub(crate) use requirements::source_requirements_for_test;

/// Runnable lowered query program.
#[derive(Clone, Debug)]
pub(crate) struct QueryProgram {
    /// Original request.
    pub(crate) request: QueryProgramRequest,
    /// Groove graph and its boundary contracts.
    pub(crate) lowered: LoweredGraph,
    /// Canonical record descriptor for every normalized source occurrence.
    /// Receiver-owned authority closures use this solely to allocate and
    /// validate local mutable Groove inputs; it is never a wire identity.
    pub(crate) source_descriptors: BTreeMap<ProgramSourceId, RecordDescriptor>,
    /// Exact post-policy source occurrences that may cross into a
    /// receiver-local maintained graph as CoveredInput.
    pub(crate) covered_input_source_descriptors: BTreeMap<ProgramSourceId, RecordDescriptor>,
    /// Human-readable debugging and test artifact.
    pub(crate) explain: ExplainPlan,
}

/// Groove graph plus the semantic contracts needed to consume it.
#[derive(Clone, Debug)]
pub(crate) struct LoweredGraph {
    /// Executable named groove terminals emitted by this program.
    pub(crate) terminals: Vec<LoweredTerminal>,
    /// Filtered current-row graph used only by synchronous one-shot
    /// materializers. Public terminals have their own exact output shape and
    /// must not be decoded as storage-backed current rows.
    pub(crate) internal_app_rows_graph: Option<GraphBuilder>,
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
