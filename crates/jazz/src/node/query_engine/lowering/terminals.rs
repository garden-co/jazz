//! Construction of typed public and maintained-query terminals.
//!
//! Terminal lowering defines result membership, structured application rows,
//! relation edges, version witnesses, aggregates, routing fields, and the
//! schemas used to decode those outputs.

use super::*;

pub(super) fn lowered_terminals(
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
    // Closure evidence is an authorization boundary only for policy-claim
    // routes. Ordinary query parameters can be absent from provenance-only
    // closure graphs; requiring those fields would either reject the program
    // or suppress raw evidence needed for local evaluation.
    let claim_route_fields = parameter_domain_for_request(request)
        .map_err(single_gap_report)?
        .claim_params
        .keys()
        .filter(|field| root_route_fields.contains(*field))
        .cloned()
        .collect::<BTreeSet<_>>();
    let root_occurrence_fields = root_join_occurrence_fields(plan, resolved_sources, request)?
        .into_iter()
        .map(|(name, _)| name)
        .filter(|name| available_fields.contains(name))
        .collect::<BTreeSet<_>>();
    let closure_root_carrier_fields = root_route_fields
        .union(&root_occurrence_fields)
        .cloned()
        .collect::<BTreeSet<_>>();
    let mut terminals = Vec::new();
    let closure = lower_closure_membership(
        graph.clone(),
        request,
        source,
        resolved_sources,
        &root_route_fields,
        &closure_root_carrier_fields,
    )?;
    // Correlated include paths can preserve routes in the graph while their
    // conservative root field set omits them. Use the graph's declared output
    // after closure lowering when choosing the fields retained by maintained
    // result-membership facts.
    let root_route_fields = graph_declared_output_fields(&closure.visible_root)
        .map(|fields| {
            routing_param_fields
                .intersection(&fields)
                .cloned()
                .collect::<BTreeSet<_>>()
        })
        .unwrap_or(root_route_fields);
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
    // Correlated path lowering can carry a route field while reporting a
    // conservative `available_fields` set for the root.  Use the parameter
    // domain rather than only `root_route_fields` to keep routed maintained
    // array queries on their existing fact-terminal path; the tree collector
    // cannot retain any routed binding fields yet.
    if let Some(app_rows) = &request.output.app_rows {
        let projected_output = projected_multisource_terminal(plan, source);
        let (graph, descriptor, hidden_fields, carrier, field_carriers, public_field_names) =
            match app_rows.projection.clone() {
                _ if !app_rows.public_terminal => (
                    closure.visible_root.clone(),
                    source.row_shape.descriptor.clone(),
                    hidden_source_fields(&source.row_shape),
                    AppRowCarrier::CurrentRow,
                    BTreeMap::new(),
                    BTreeMap::new(),
                ),
                PayloadProjection::Tree(tree) => {
                    let collected = lower_collect_by_app_rows(
                        closure.visible_root.clone(),
                        &tree,
                        plan,
                        source,
                        resolved_sources,
                        request,
                        &root_route_fields,
                        available_fields,
                    )?;
                    (
                        collected.graph,
                        collected.descriptor,
                        collected.hidden_fields,
                        collected.carrier,
                        collected.field_carriers,
                        collected.public_field_names,
                    )
                }
                _ if projected_output.is_some() => {
                    let (output_source_id, output_fields, _is_flat) = projected_output
                        .as_ref()
                        .expect("guarded projected multi-source output");
                    let output_source =
                        resolved_sources.get(output_source_id).ok_or_else(|| {
                            single_gap_report(UnsupportedReason::Runtime(format!(
                                "projected output source {output_source_id:?} was not resolved"
                            )))
                        })?;
                    let hidden_fields = hidden_source_fields(&output_source.row_shape)
                        .into_iter()
                        .chain(
                            output_fields
                                .iter()
                                .filter(|field| field.name.starts_with("__flat_join_row_"))
                                .map(|field| field.name.clone()),
                        )
                        .collect::<BTreeSet<_>>();
                    let public_fields = output_fields
                        .iter()
                        .filter(|field| !hidden_fields.contains(&field.name))
                        .collect::<Vec<_>>();
                    let descriptor = RecordDescriptor::new(
                        public_fields
                            .iter()
                            .map(|field| (field.name.clone(), field.ty.clone())),
                    );
                    let graph = graph.clone().project_fields(
                        public_fields
                            .iter()
                            .map(|field| ProjectField::named(&field.name)),
                    );
                    let public_field_names = public_fields
                        .iter()
                        .map(|field| {
                            (
                                field.name.clone(),
                                logical_user_column(&field.name).to_owned(),
                            )
                        })
                        .collect();
                    (
                        graph,
                        descriptor,
                        BTreeSet::new(),
                        AppRowCarrier::Logical,
                        BTreeMap::new(),
                        public_field_names,
                    )
                }
                _ => {
                    let collected = lower_collect_by_app_rows(
                        closure.visible_root.clone(),
                        &AppProjectionTree {
                            fields: FieldProjection::All,
                            paths: Vec::new(),
                        },
                        plan,
                        source,
                        resolved_sources,
                        request,
                        &root_route_fields,
                        available_fields,
                    )?;
                    (
                        collected.graph,
                        collected.descriptor,
                        collected.hidden_fields,
                        collected.carrier,
                        collected.field_carriers,
                        collected.public_field_names,
                    )
                }
            };
        terminals.push(LoweredTerminal {
            sink: "app_rows".to_owned(),
            graph,
            output: OutputTerminalSchema::AppRows(AppRowSchema {
                descriptor,
                hidden_fields,
                carrier,
                field_carriers,
                public_field_names,
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
                request,
                routing_param_fields.clone(),
            )?;
            // Closure membership is a root-row projection: it may discard
            // output-only columns while constructing the root closure. A
            // flat join instead needs its complete already-policy-filtered
            // tuple here, including the internal contributor ids used by the
            // occurrence address. This matters when a joined source resolves
            // through a lens (for example a renamed table on an old branch),
            // where that projection otherwise loses `__flat_join_row_N`.
            let occurrence_addressed = matches!(
                &output.schema,
                ProgramFactSchema::ResultMembership(schema)
                    if schema.occurrence_id_fields.len() > 1
            );
            let result_membership_input =
                if flat_join_payload_fields(plan).is_empty() && !occurrence_addressed {
                    visible_root_with_routes.clone()
                } else {
                    graph.clone()
                };
            let result_graph = fact_terminal_graph(
                fact,
                result_membership_input,
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
            // A flat join's one wide primary terminal is its public output.
            // The ordinary closure terminals are source-only membership facts;
            // they neither carry the tuple payload nor its contributor ids.
            // Emitting them would also ask a source graph for
            // `__flat_join_row_N`, which only exists after the wide project.
            if flat_join_payload_fields(plan).is_empty() {
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
                        request,
                        claim_route_fields.clone(),
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
                        request,
                        claim_route_fields.clone(),
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
                    request,
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
                    request,
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
                    request,
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
                    request,
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
                request,
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

/// One fully typed flat input field for the association collector.  The
/// resulting stream is deliberately source-row based: no target id is later
/// dereferenced to reconstruct a child payload.
#[derive(Clone, Debug)]
pub(super) struct CollectFlatField {
    pub(super) input: String,
    pub(super) output: String,
    pub(super) value_type: ValueType,
    pub(super) output_value_type: ValueType,
    pub(super) source_field: Option<String>,
    pub(super) is_row_id: bool,
    pub(super) is_presence: bool,
    pub(super) is_output: bool,
}

#[derive(Clone, Debug)]
pub(super) struct CollectSlotLayout {
    pub(super) path: ProgramPathId,
    pub(super) collection_field: String,
    pub(super) fields: Vec<CollectFlatField>,
    pub(super) row_id_input: String,
    pub(super) presence_input: String,
    pub(super) order_cols: Vec<TopByOrder>,
    pub(super) tie_cols: Vec<String>,
    pub(super) offset: u64,
    pub(super) limit: TopByLimit,
    pub(super) children: Vec<CollectSlotLayout>,
}

#[derive(Clone, Debug)]
pub(super) struct CollectLayout {
    pub(super) root_fields: Vec<CollectFlatField>,
    pub(super) root_occurrence_inputs: Vec<String>,
    pub(super) root_order_cols: Vec<TopByOrder>,
    pub(super) root_tie_cols: Vec<String>,
    pub(super) root_offset: u64,
    pub(super) root_limit: TopByLimit,
    pub(super) slots: Vec<CollectSlotLayout>,
}

#[derive(Clone, Debug)]
struct LoweredCollectByAppRows {
    pub(super) graph: GraphBuilder,
    pub(super) descriptor: RecordDescriptor,
    pub(super) hidden_fields: BTreeSet<String>,
    pub(super) carrier: AppRowCarrier,
    pub(super) field_carriers: BTreeMap<String, AppRowCarrier>,
    pub(super) public_field_names: BTreeMap<String, String>,
}

fn lower_collect_by_app_rows(
    visible_root: GraphBuilder,
    projection: &AppProjectionTree,
    plan: &AnalyzedQueryPlan,
    root_source: &ResolvedSource,
    resolved_sources: &BTreeMap<SourceId, ResolvedSource>,
    request: &QueryProgramRequest,
    route_fields: &BTreeSet<String>,
    available_fields: &BTreeSet<String>,
) -> CapabilityResult<LoweredCollectByAppRows> {
    let mut parameter_domain = parameter_domain_for_request(request).map_err(single_gap_report)?;
    collect_binding_source_params(&visible_root, &mut parameter_domain);
    let mut layout = collect_layout(
        projection,
        plan,
        root_source,
        resolved_sources,
        request,
        route_fields,
        &parameter_domain,
        available_fields,
    )?;
    align_collect_root_window(&mut layout, plan)?;
    align_collect_join_key_types(&mut layout.slots, plan, resolved_sources, request)?;
    if layout.slots.is_empty() {
        // A collect-all root preserves the source CurrentRow application-cell
        // wrappers. Explicit projections unwrap those cells to their declared
        // logical types. Bind that distinction here while both input and
        // output types are authoritative; consumers must never infer it from
        // the eventual descriptor's field names.
        let carrier = if layout
            .root_fields
            .iter()
            .filter(|field| field.is_output && !field.is_row_id)
            .all(|field| field.value_type == field.output_value_type)
        {
            AppRowCarrier::CurrentRow
        } else {
            AppRowCarrier::Logical
        };
        let field_carriers = layout
            .root_fields
            .iter()
            .filter(|field| field.is_output && !field.is_row_id)
            .map(|field| {
                (
                    field.output.clone(),
                    if field.value_type == field.output_value_type {
                        AppRowCarrier::CurrentRow
                    } else {
                        AppRowCarrier::Logical
                    },
                )
            })
            .collect();
        let public_field_names = layout
            .root_fields
            .iter()
            .filter(|field| field.is_output && !field.is_row_id)
            .map(|field| {
                (
                    field.output.clone(),
                    public_root_field_name(root_source, field),
                )
            })
            .collect();
        let anchor = collect_anchor_graph(visible_root, &layout)?;
        let has_window = root_linear_steps(plan).is_some_and(|steps| {
            steps
                .iter()
                .any(|step| matches!(step, LinearStep::OrderBy(_) | LinearStep::Slice { .. }))
        });
        let anchor = if has_window {
            anchor
        } else {
            GraphBuilder::top_by(
                anchor,
                Vec::<String>::new(),
                layout.root_order_cols.clone(),
                layout.root_tie_cols.clone(),
                0,
                TopByLimit::Unbounded,
            )
        };
        let root_group = layout
            .root_fields
            .iter()
            .find(|field| field.is_row_id)
            .expect("collector root retains row id")
            .input
            .clone();
        let graph = GraphBuilder::collect_root_ordered(
            anchor,
            std::iter::once(root_group)
                .chain(layout.root_occurrence_inputs.iter().cloned())
                .chain(route_fields.iter().cloned()),
            layout
                .root_fields
                .iter()
                .filter(|field| field.is_output)
                .map(|field| {
                    if field.is_row_id || field.value_type == field.output_value_type {
                        CollectByField::renamed(&field.input, &field.output)
                    } else {
                        CollectByField::renamed_unwrap_nullable(&field.input, &field.output)
                    }
                }),
            layout.root_order_cols.clone(),
            layout.root_tie_cols.clone(),
            layout.root_offset,
            layout.root_limit,
        );
        let descriptor = collect_output_descriptor(&layout)?;
        return Ok(LoweredCollectByAppRows {
            graph,
            descriptor,
            // Route parameters are retained in the collector record so the
            // maintained graph can partition results, but they are not part
            // of the app projection. Nested collectors already apply the same
            // boundary below.
            hidden_fields: route_fields.clone(),
            carrier,
            field_carriers,
            public_field_names,
        });
    }
    let root_context = root_collect_context_graph(visible_root.clone(), &layout)?;
    let mut field_carriers = layout
        .root_fields
        .iter()
        .filter(|field| field.is_output && !field.is_row_id)
        .map(|field| {
            (
                field.output.clone(),
                if field.value_type == field.output_value_type {
                    AppRowCarrier::CurrentRow
                } else {
                    AppRowCarrier::Logical
                },
            )
        })
        .collect::<BTreeMap<_, _>>();
    field_carriers.extend(
        layout
            .slots
            .iter()
            .map(|slot| (slot.collection_field.clone(), AppRowCarrier::Logical)),
    );
    let mut public_field_names = layout
        .root_fields
        .iter()
        .filter(|field| field.is_output && !field.is_row_id)
        .map(|field| {
            (
                field.output.clone(),
                public_root_field_name(root_source, field),
            )
        })
        .collect::<BTreeMap<_, _>>();
    public_field_names.extend(layout.slots.iter().map(|slot| {
        // Collection field names are public query-path identities. They are
        // carried verbatim into the terminal descriptor, so no prefix-based
        // recovery is needed at this boundary.
        (slot.collection_field.clone(), slot.collection_field.clone())
    }));
    let mut association_graphs = Vec::new();
    for slot in &layout.slots {
        let path = find_correlated_path(plan, &slot.path).ok_or_else(|| {
            single_gap_report(UnsupportedReason::Operator(format!(
                "app projection path {:?} is not a correlated path in the query shape",
                slot.path
            )))
        })?;
        association_graphs.extend(lower_collect_slot_graphs(
            slot,
            path,
            root_context.clone(),
            root_source,
            root_source,
            &layout,
            &BTreeSet::new(),
            resolved_sources,
            request,
        )?);
    }
    let anchor = collect_anchor_graph(visible_root, &layout)?;
    let input = GraphBuilder::union(std::iter::once(anchor).chain(association_graphs));
    let descriptor = collect_output_descriptor(&layout)?;
    let root_group = layout
        .root_fields
        .iter()
        .find(|field| field.is_row_id)
        .ok_or_else(|| {
            single_gap_report(UnsupportedReason::Runtime(
                "collector root projection did not retain the row id".to_owned(),
            ))
        })?
        .input
        .clone();
    let graph = GraphBuilder::collect_by_tree_ordered(
        input,
        std::iter::once(root_group.clone())
            .chain(layout.root_occurrence_inputs.iter().cloned())
            .chain(route_fields.iter().cloned()),
        layout
            .root_fields
            .iter()
            .filter(|field| field.is_output)
            .map(|field| {
                if field.is_row_id || field.value_type == field.output_value_type {
                    CollectByField::renamed(&field.input, &field.output)
                } else {
                    CollectByField::renamed_unwrap_nullable(&field.input, &field.output)
                }
            }),
        layout
            .slots
            .iter()
            .map(|slot| collect_slot_builder(slot, &root_group, route_fields)),
        layout.root_order_cols,
        layout.root_tie_cols,
        layout.root_offset,
        layout.root_limit,
    );
    let mut hidden_fields = hidden_source_fields(&root_source.row_shape);
    hidden_fields.extend(route_fields.iter().cloned());
    Ok(LoweredCollectByAppRows {
        graph,
        descriptor,
        hidden_fields,
        carrier: AppRowCarrier::Logical,
        field_carriers,
        public_field_names,
    })
}

fn align_collect_join_key_types(
    slots: &mut [CollectSlotLayout],
    plan: &AnalyzedQueryPlan,
    resolved_sources: &BTreeMap<SourceId, ResolvedSource>,
    request: &QueryProgramRequest,
) -> CapabilityResult<()> {
    for slot in slots {
        let path = find_correlated_path(plan, &slot.path).ok_or_else(|| {
            single_gap_report(UnsupportedReason::Operator(format!(
                "app projection path {:?} is not a correlated path in the query shape",
                slot.path
            )))
        })?;
        let parent_id = path.parent.root.source().ok_or_else(|| {
            single_gap_report(UnsupportedReason::Operator(
                "collector path parent must be a source".to_owned(),
            ))
        })?;
        let child_id = path.child.root.source().ok_or_else(|| {
            single_gap_report(UnsupportedReason::Operator(
                "collector path child must be a source".to_owned(),
            ))
        })?;
        let parent = resolved_sources.get(parent_id).ok_or_else(|| {
            single_gap_report(UnsupportedReason::Runtime(format!(
                "collector parent source {parent_id:?} was not resolved"
            )))
        })?;
        let child = resolved_sources.get(child_id).ok_or_else(|| {
            single_gap_report(UnsupportedReason::Runtime(format!(
                "collector child source {child_id:?} was not resolved"
            )))
        })?;
        let (_, child_key) = lower_path_key_pair(
            &path.correlation,
            parent_id,
            parent,
            child_id,
            child,
            request,
        )
        .map_err(single_gap_report)?;
        if let Some(field) = slot
            .fields
            .iter_mut()
            .find(|field| field.source_field.as_deref() == Some(child_key.as_str()))
        {
            let mut payload = &field.value_type;
            while let ValueType::Nullable(inner) = payload {
                payload = inner.as_ref();
            }
            field.value_type = ValueType::Nullable(Box::new(payload.clone()));
        }
        for step in &path.child.steps {
            match step {
                LinearStep::OrderBy(keys) => {
                    for key in keys {
                        retain_collect_slot_value(slot, &key.value, child)?;
                    }
                    slot.order_cols = keys
                        .iter()
                        .map(|key| {
                            let field = collect_slot_input_for_value(slot, &key.value)?;
                            Ok(match key.direction {
                                SortDirection::Asc => TopByOrder::asc(field),
                                SortDirection::Desc => TopByOrder::desc(field),
                            })
                        })
                        .collect::<CapabilityResult<Vec<_>>>()?;
                }
                LinearStep::Slice {
                    limit,
                    offset,
                    tie_breaker,
                    ..
                } => {
                    for value in tie_breaker {
                        retain_collect_slot_value(slot, value, child)?;
                    }
                    slot.offset = u64::from(*offset);
                    slot.limit = limit
                        .map(|limit| TopByLimit::Finite(u64::from(limit)))
                        .unwrap_or(TopByLimit::Unbounded);
                    slot.tie_cols = tie_breaker
                        .iter()
                        .map(|value| collect_slot_input_for_value(slot, value))
                        .collect::<CapabilityResult<Vec<_>>>()?;
                }
                _ => {}
            }
        }
        if slot.order_cols.is_empty() {
            slot.order_cols = vec![TopByOrder::asc(&slot.row_id_input)];
        }
        if slot.tie_cols.is_empty() {
            slot.tie_cols = vec![slot.row_id_input.clone()];
        }
        align_collect_join_key_types(&mut slot.children, plan, resolved_sources, request)?;
    }
    Ok(())
}

fn align_collect_root_window(
    layout: &mut CollectLayout,
    plan: &AnalyzedQueryPlan,
) -> CapabilityResult<()> {
    let steps = match plan {
        AnalyzedQueryPlan::Linear(linear) => linear.steps.iter().collect::<Vec<_>>(),
        AnalyzedQueryPlan::CorrelatedPath(path) => path
            .parent
            .steps
            .iter()
            .chain(&path.output_steps)
            .collect::<Vec<_>>(),
        _ => Vec::new(),
    };
    for step in steps {
        match step {
            LinearStep::OrderBy(keys) => {
                layout.root_order_cols = keys
                    .iter()
                    .map(|key| {
                        let field = collect_root_input_for_value(layout, &key.value)?;
                        Ok(match key.direction {
                            SortDirection::Asc => TopByOrder::asc(field),
                            SortDirection::Desc => TopByOrder::desc(field),
                        })
                    })
                    .collect::<CapabilityResult<Vec<_>>>()?;
            }
            LinearStep::Slice {
                limit,
                offset,
                tie_breaker,
                ..
            } => {
                layout.root_offset = u64::from(*offset);
                layout.root_limit = limit
                    .map(|limit| TopByLimit::Finite(u64::from(limit)))
                    .unwrap_or(TopByLimit::Unbounded);
                layout.root_tie_cols = tie_breaker
                    .iter()
                    .map(|value| collect_root_input_for_value(layout, value))
                    .collect::<CapabilityResult<Vec<_>>>()?;
            }
            _ => {}
        }
    }
    let row_id = layout
        .root_fields
        .iter()
        .find(|field| field.is_row_id)
        .expect("collector root retains row id")
        .input
        .clone();
    if layout.root_order_cols.is_empty() {
        layout.root_order_cols = vec![TopByOrder::asc(&row_id)];
    }
    if layout.root_tie_cols.is_empty() {
        layout.root_tie_cols = vec![row_id];
    }
    for occurrence in &layout.root_occurrence_inputs {
        if !layout.root_tie_cols.contains(occurrence) {
            layout.root_tie_cols.push(occurrence.clone());
        }
    }
    Ok(())
}

/// Keep values used to order or slice a nested collector slot in its internal
/// input row. They are deliberately not public payload fields: a path can
/// order by provenance even when its projection selects only ordinary columns.
fn retain_collect_slot_value(
    slot: &mut CollectSlotLayout,
    value: &NormalizedValueRef,
    source: &ResolvedSource,
) -> CapabilityResult<()> {
    let Some(requested_field) = collect_source_field_for_value(value) else {
        return Ok(());
    };
    let source_field = if source
        .row_shape
        .descriptor
        .field_index(requested_field)
        .is_some()
    {
        requested_field.to_owned()
    } else {
        user_column_field(requested_field)
    };
    if slot
        .fields
        .iter()
        .any(|field| field.source_field.as_deref() == Some(source_field.as_str()))
    {
        return Ok(());
    }
    let source_value_type = source_field_type(source, &source_field)
        .cloned()
        .ok_or_else(|| {
            single_gap_report(UnsupportedReason::Operator(format!(
                "collector child source {:?} does not provide window key {requested_field:?}",
                source.row_shape.source
            )))
        })?;
    let prefix = slot
        .row_id_input
        .strip_suffix(&format!("_{}", source.row_shape.row_uuid_field))
        .ok_or_else(|| {
            single_gap_report(UnsupportedReason::Runtime(format!(
                "collector child row-id input {:?} does not match source row-id field {:?}",
                slot.row_id_input, source.row_shape.row_uuid_field
            )))
        })?;
    let value_type = if matches!(source_value_type, ValueType::Nullable(_)) {
        source_value_type.clone()
    } else {
        ValueType::Nullable(Box::new(source_value_type.clone()))
    };
    slot.fields.push(CollectFlatField {
        input: format!("{prefix}_{source_field}"),
        output: source_field.clone(),
        value_type,
        output_value_type: source_value_type,
        source_field: Some(source_field),
        is_row_id: false,
        is_presence: false,
        is_output: false,
    });
    Ok(())
}

fn collect_root_input_for_value(
    layout: &CollectLayout,
    value: &NormalizedValueRef,
) -> CapabilityResult<String> {
    match collect_source_field_for_value(value) {
        Some(field) => layout
            .root_fields
            .iter()
            .find(|candidate| {
                candidate.source_field.as_deref() == Some(field)
                    || candidate
                        .source_field
                        .as_deref()
                        .is_some_and(|source| logical_user_column(source) == field)
            })
            .map(|candidate| candidate.input.clone()),
        None if matches!(value, NormalizedValueRef::RowId(RowIdRef::Source(_))) => layout
            .root_fields
            .iter()
            .find(|field| field.is_row_id)
            .map(|field| field.input.clone()),
        _ => None,
    }
    .ok_or_else(|| {
        single_gap_report(UnsupportedReason::Operator(format!(
            "collector root window key {value:?} is not present in the root projection"
        )))
    })
}

fn collect_slot_input_for_value(
    slot: &CollectSlotLayout,
    value: &NormalizedValueRef,
) -> CapabilityResult<String> {
    match collect_source_field_for_value(value) {
        Some(field) => slot
            .fields
            .iter()
            .find(|candidate| {
                candidate.source_field.as_deref() == Some(field)
                    || candidate
                        .source_field
                        .as_deref()
                        .is_some_and(|source| logical_user_column(source) == field)
            })
            .map(|candidate| candidate.input.clone()),
        None if matches!(value, NormalizedValueRef::RowId(RowIdRef::Source(_))) => {
            Some(slot.row_id_input.clone())
        }
        _ => None,
    }
    .ok_or_else(|| {
        single_gap_report(UnsupportedReason::Operator(format!(
            "collector window key {value:?} is not present in the child projection"
        )))
    })
}

/// Map a normalized field reference to the canonical name retained by a
/// resolved source. Provenance is source metadata, not a public projection
/// field, but ordered and sliced collectors still need it as an internal key.
fn collect_source_field_for_value(value: &NormalizedValueRef) -> Option<&str> {
    match value {
        NormalizedValueRef::SourceField { field, .. } => Some(field),
        NormalizedValueRef::Provenance { field, .. } => Some(match field {
            ProvenanceField::CreatedAt => "$createdAt",
            ProvenanceField::CreatedBy => "$createdBy",
            ProvenanceField::UpdatedAt => "$updatedAt",
            ProvenanceField::UpdatedBy => "$updatedBy",
        }),
        _ => None,
    }
}

pub(super) fn root_join_occurrence_fields(
    plan: &AnalyzedQueryPlan,
    resolved_sources: &BTreeMap<SourceId, ResolvedSource>,
    request: &QueryProgramRequest,
) -> CapabilityResult<Vec<(String, ValueType)>> {
    // Authorization subplans are decision programs, not public row sets. Their
    // joins prove policy predicates and cannot contribute to a result address.
    if matches!(request.policy, PolicyContext::AuthorizationSubplan { .. })
        || request
            .output
            .app_rows
            .as_ref()
            .is_some_and(|output| !output.public_terminal)
    {
        return Ok(Vec::new());
    }
    let Some(steps) = root_linear_steps(plan) else {
        return Ok(Vec::new());
    };
    let mut fields = Vec::new();
    for (step_index, step) in steps.iter().enumerate() {
        let LinearStep::Join {
            right,
            mode: JoinMode::Inner,
            ..
        } = step
        else {
            continue;
        };
        if matches!(steps.get(step_index + 1), Some(LinearStep::Project(_))) {
            continue;
        }
        if matches!(right.as_ref(), RelationInputPlan::Union(_)) {
            fields.push((format!("__root_join_arm_{step_index}"), ValueType::String));
            fields.push((format!("__root_join_row_{step_index}"), ValueType::Uuid));
            continue;
        }
        let Some(source_id) = right.root_source() else {
            continue;
        };
        let source = resolved_sources.get(source_id).ok_or_else(|| {
            single_gap_report(UnsupportedReason::Runtime(format!(
                "inner join occurrence source {source_id:?} was not resolved"
            )))
        })?;
        if !matches!(source_id.path.components.last(), Some(SourceRole::Alias(_))) {
            continue;
        }
        let exposes_row_id = match right.as_ref() {
            RelationInputPlan::Linear(linear) => !matches!(
                linear.steps.last(),
                Some(LinearStep::Project(columns))
                    if !columns.iter().any(|column| {
                        column.output.name == source.row_shape.row_uuid_field
                    })
            ),
            RelationInputPlan::Union(_) | RelationInputPlan::Recursive(_) => false,
        };
        if !exposes_row_id {
            continue;
        }
        let name = if matches!(steps.get(step_index + 1), Some(LinearStep::Join { .. })) {
            format!(
                "__flat_join_source_{step_index}_{}",
                source.row_shape.row_uuid_field
            )
        } else {
            format!("__root_join_row_{step_index}")
        };
        fields.push((name, ValueType::Uuid));
    }
    Ok(fields)
}

fn find_correlated_path<'a>(
    plan: &'a AnalyzedQueryPlan,
    path: &ProgramPathId,
) -> Option<&'a CorrelatedPathPlan> {
    let AnalyzedQueryPlan::CorrelatedPath(root) = plan else {
        return None;
    };
    find_correlated_path_in_tree(root, path)
}

pub(super) fn find_nested_correlated_path<'a>(
    parent: &'a CorrelatedPathPlan,
    path: &ProgramPathId,
) -> Option<&'a CorrelatedPathPlan> {
    parent
        .nested
        .iter()
        .find_map(|candidate| find_correlated_path_in_tree(candidate, path))
}

fn find_correlated_path_in_tree<'a>(
    path: &'a CorrelatedPathPlan,
    target: &ProgramPathId,
) -> Option<&'a CorrelatedPathPlan> {
    if &path.path == target {
        return Some(path);
    }
    path.siblings
        .iter()
        .chain(&path.nested)
        .find_map(|candidate| find_correlated_path_in_tree(candidate, target))
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
                carrier: AppRowCarrier::Logical,
                field_carriers: BTreeMap::new(),
                public_field_names: BTreeMap::new(),
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
                request,
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

pub(super) fn project_source_fields_from_prefix(
    source: &ResolvedSource,
    prefix: &str,
) -> Vec<ProjectField> {
    project_source_fields_from_prefix_rewrapping_nullable(source, prefix, None)
}

pub(super) fn project_source_fields_from_prefix_rewrapping_nullable(
    source: &ResolvedSource,
    prefix: &str,
    nullable_field: Option<&str>,
) -> Vec<ProjectField> {
    source
        .row_shape
        .descriptor
        .fields()
        .iter()
        .filter_map(|field| field.name.as_ref())
        .map(|field| {
            let source_field = format!("{prefix}{field}");
            if nullable_field == Some(field.as_str()) {
                ProjectField::nullable(source_field, field.clone())
            } else {
                ProjectField::renamed(source_field, field.clone())
            }
        })
        .collect()
}

pub(super) fn project_source_fields_with_routes(
    source: &ResolvedSource,
    route_fields: &BTreeSet<String>,
) -> Vec<ProjectField> {
    project_source_fields_with_routes_from_prefix(source, "", route_fields)
}

pub(super) fn project_source_fields_with_routes_from_prefix(
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
    request: &QueryProgramRequest,
    routing_param_fields: BTreeSet<String>,
) -> CapabilityResult<ProgramFactOutput> {
    fact_output_with_terminal(
        key,
        ProgramFactTerminal::Primary,
        plan,
        source,
        resolved_sources,
        request,
        routing_param_fields,
    )
}

fn fact_output_with_terminal(
    key: &ProgramFactKey,
    terminal: ProgramFactTerminal,
    plan: &AnalyzedQueryPlan,
    source: &ResolvedSource,
    resolved_sources: &BTreeMap<SourceId, ResolvedSource>,
    request: &QueryProgramRequest,
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
            let occurrence_id_fields =
                result_occurrence_id_fields(plan, source, resolved_sources, request)?;
            let occurrence_union_arm_fields =
                result_occurrence_union_arm_fields(plan, source, resolved_sources, request)?;
            let flat_join_payload = flat_join_payload_fields(plan);
            let payload_fields = result_payload_fields(plan, source);
            let settle_position_field = flat_join_payload
                .is_empty()
                .then(|| settle_position_field(&source.row_shape))
                .flatten();
            ProgramFactSchema::ResultMembership(ResultMembershipSchema {
                table_field: "table_name".to_owned(),
                row_field: source.row_shape.row_uuid_field.clone(),
                occurrence_id_fields,
                occurrence_union_arm_fields,
                payload_fields,
                // Version witnesses may carry either a legacy physical prefix
                // or a branch key. Only view-relative sources use that field
                // as part of public result identity; attaching a physical
                // prefix to ordinary rows would churn durable memberships and
                // receipts across schema projections.
                branch_or_prefix_field: source
                    .requires_result_payload
                    .then(|| version.branch_or_prefix_field.clone())
                    .flatten(),
                version: ResultMembershipVersionSchema::Content(ContentVersionFields {
                    tx_time_field: "content_tx_time".to_owned(),
                    tx_node_field: "content_tx_node_id".to_owned(),
                }),
                settle_position_field,
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

fn result_occurrence_id_fields(
    plan: &AnalyzedQueryPlan,
    source: &ResolvedSource,
    resolved_sources: &BTreeMap<SourceId, ResolvedSource>,
    request: &QueryProgramRequest,
) -> CapabilityResult<Vec<String>> {
    let mut fields = vec![source.row_shape.row_uuid_field.clone()];
    if &source.row_shape.source != plan.root_source() {
        return Ok(fields);
    }
    if let Some(LinearStep::Project(columns)) =
        root_linear_steps(plan).and_then(|steps| steps.last())
    {
        fields.extend(columns.iter().filter_map(|column| {
            column
                .output
                .name
                .strip_prefix("__flat_join_row_")
                .map(|_| column.output.name.clone())
        }));
    } else {
        fields.extend(
            root_join_occurrence_fields(plan, resolved_sources, request)?
                .into_iter()
                .filter_map(|(name, value_type)| (value_type == ValueType::Uuid).then_some(name)),
        );
    }
    Ok(fields)
}

fn result_occurrence_union_arm_fields(
    plan: &AnalyzedQueryPlan,
    source: &ResolvedSource,
    resolved_sources: &BTreeMap<SourceId, ResolvedSource>,
    request: &QueryProgramRequest,
) -> CapabilityResult<BTreeMap<usize, String>> {
    if &source.row_shape.source != plan.root_source() {
        return Ok(BTreeMap::new());
    }
    let fields = root_join_occurrence_fields(plan, resolved_sources, request)?;
    let mut joined_position = 0usize;
    let mut pending_arm = None;
    let mut arms = BTreeMap::new();
    for (name, value_type) in fields {
        match value_type {
            ValueType::String => pending_arm = Some(name),
            ValueType::Uuid => {
                if let Some(arm) = pending_arm.take() {
                    arms.insert(joined_position, arm);
                }
                joined_position += 1;
            }
            _ => {}
        }
    }
    Ok(arms)
}

pub(super) fn flat_join_payload_fields(plan: &AnalyzedQueryPlan) -> Vec<TypedOutputField> {
    root_linear_steps(plan)
        .and_then(|steps| match steps.last() {
            Some(LinearStep::Project(columns))
                if columns
                    .iter()
                    .any(|column| column.output.name.starts_with("__flat_join_row_")) =>
            {
                Some(columns)
            }
            _ => None,
        })
        .map(|columns| {
            columns
                .iter()
                .filter(|column| column.output.name.contains('.'))
                .map(|column| column.output.clone())
                .collect()
        })
        .unwrap_or_default()
}

fn result_payload_fields(
    plan: &AnalyzedQueryPlan,
    source: &ResolvedSource,
) -> Vec<TypedOutputField> {
    let flat_join = flat_join_payload_fields(plan);
    if !flat_join.is_empty() || !source.requires_result_payload {
        return flat_join;
    }
    let hidden = hidden_source_fields(&source.row_shape);
    source
        .row_shape
        .descriptor
        .fields()
        .iter()
        .filter_map(|field| {
            let name = field.name.as_ref()?;
            (!hidden.contains(name) && !source.routing_fields.contains(name)).then(|| {
                TypedOutputField {
                    name: name.clone(),
                    ty: field.value_type.clone(),
                }
            })
        })
        .collect()
}

fn projected_multisource_terminal(
    plan: &AnalyzedQueryPlan,
    root_source: &ResolvedSource,
) -> Option<(SourceId, Vec<TypedOutputField>, bool)> {
    let columns = root_linear_steps(plan).and_then(|steps| match steps.last() {
        Some(LinearStep::Project(columns)) => Some(columns),
        _ => None,
    })?;
    let output_source = columns.iter().find_map(|column| match &column.value {
        NormalizedValueRef::RowId(RowIdRef::Source(source))
            if column.output.name == root_source.row_shape.row_uuid_field =>
        {
            Some(source.clone())
        }
        _ => None,
    })?;
    let is_flat = columns
        .iter()
        .any(|column| column.output.name.starts_with("__flat_join_row_"));
    is_flat.then(|| {
        (
            output_source,
            columns.iter().map(|column| column.output.clone()).collect(),
            true,
        )
    })
}

fn root_linear_steps(plan: &AnalyzedQueryPlan) -> Option<&[LinearStep]> {
    match plan {
        AnalyzedQueryPlan::Linear(plan) => Some(&plan.steps),
        _ => None,
    }
}

pub(super) fn output_routing_fields(output: &ProgramFactOutput) -> BTreeSet<String> {
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
                let graph = match root_aggregate_step(plan) {
                    Some((group_by, outputs))
                        if group_by.is_empty()
                            && matches!(
                                outputs,
                                [AggregateExpr {
                                    function: AggregateFunction::Count,
                                    ..
                                }]
                            ) =>
                    {
                        graph.filter(GroovePredicateExpr::Neq {
                            field: aggregate_output_field(&outputs[0].output.name),
                            value: LiteralValue::U64(0),
                        })
                    }
                    _ => graph,
                };
                return Ok(graph.project_fields(aggregate_result_membership_fields(
                    plan,
                    source,
                    routing_param_fields,
                )?));
            }
            let mut occurrence_fields =
                result_occurrence_id_fields(plan, source, resolved_sources, request)?;
            occurrence_fields.extend(
                result_occurrence_union_arm_fields(plan, source, resolved_sources, request)?
                    .into_values(),
            );
            let flat_join_payload = flat_join_payload_fields(plan);
            Ok(graph.project_fields(result_membership_fields(
                source,
                routing_param_fields,
                &result_payload_fields(plan, source),
                &occurrence_fields,
                flat_join_payload.is_empty(),
            )?))
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

pub(super) fn route_literal_project_field(
    route_field: &str,
    request: &QueryProgramRequest,
) -> Result<ProjectField, UnsupportedReason> {
    let domain = parameter_domain_for_request(request)?;
    if let Some(path) = claim_path_from_param_field(route_field) {
        let value = claim_value(&path, &request.policy)?;
        let literal = domain
            .claim_params
            .get(route_field)
            .map(|claim| coerce_literal_for_value_type(value.clone().into(), &claim.ty.clone()))
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
        .map(|ty| coerce_literal_for_value_type(value.clone().into(), &ty.clone()))
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
            true,
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
    let mut fields = vec![
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
    ];
    if let Some(field) = source_version.branch_or_prefix_field {
        fields.push(ProjectField::renamed(
            left_field(&field),
            "source_branch_or_prefix",
        ));
    }
    if let Some(field) = target_version.branch_or_prefix_field {
        fields.push(ProjectField::renamed(
            right_field(&field),
            "target_branch_or_prefix",
        ));
    }
    Ok(fields)
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
    payload_fields: &[TypedOutputField],
    occurrence_id_fields: &[String],
    include_settle_position: bool,
) -> CapabilityResult<Vec<ProjectField>> {
    let version = version_witness_fields(&source.row_shape)?;
    let settle_position = include_settle_position
        .then(|| settle_position_field(&source.row_shape))
        .flatten();
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
    if let Some(branch_or_prefix) = version.branch_or_prefix_field {
        fields.push(ProjectField::named(branch_or_prefix));
    }
    fields.extend(
        occurrence_id_fields
            .iter()
            .filter(|field| **field != source.row_shape.row_uuid_field)
            .cloned()
            .map(ProjectField::named),
    );
    if let Some(field) = settle_position {
        fields.push(ProjectField::renamed(field, "settle_position"));
    } else {
        fields.push(ProjectField::null_typed(
            "settle_position",
            ValueType::Nullable(Box::new(ValueType::U64)),
        ));
    }
    fields.extend(routing_param_fields.into_iter().map(ProjectField::named));
    fields.extend(
        payload_fields
            .iter()
            .map(|field| ProjectField::named(field.name.clone())),
    );
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
                    aggregate_output_field(&output.output.name),
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
            replacement_field: "synthetic_replacement".to_owned(),
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
    // The synthetic table is only a protocol label. Aggregate identity is the
    // group-key value below (or the fixed global token), never a source-table
    // name or an aggregate value.
    let mut fields = vec![ProjectField::literal(
        "table_name",
        Value::String("aggregate_result".to_owned()),
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
    // This runtime-only token pairs the aggregate operator's before/after
    // records. It is wrapped as an opaque protocol type before it crosses the
    // runtime boundary, so it cannot be mistaken for row version metadata.
    if let Some(first_output) = outputs.first() {
        fields.push(ProjectField::renamed(
            aggregate_output_field(&first_output.output.name),
            "synthetic_replacement",
        ));
    } else {
        fields.push(ProjectField::literal(
            "synthetic_replacement",
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
            .map(|output| ProjectField::named(aggregate_output_field(&output.output.name))),
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
        ty: value_type,
    })
}

fn aggregate_typed_output_field(
    output: &AggregateExpr,
    source: &ResolvedSource,
) -> CapabilityResult<TypedOutputField> {
    Ok(TypedOutputField {
        name: aggregate_output_field(&output.output.name),
        ty: aggregate_output_value_type(output, source)?,
    })
}

fn aggregate_output_value_type(
    output: &AggregateExpr,
    source: &ResolvedSource,
) -> CapabilityResult<ValueType> {
    match output.function {
        AggregateFunction::Count => Ok(ValueType::U64),
        AggregateFunction::Avg => Ok(ValueType::Nullable(Box::new(ValueType::F64))),
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
            let value_type = source_field_type(source, &field).cloned().ok_or_else(|| {
                Box::new(CapabilityReport {
                    gaps: vec![UnsupportedReason::Runtime(format!(
                        "aggregate input field {field:?} is missing from resolved descriptor"
                    ))],
                    explain: ExplainPlan::default(),
                })
            })?;
            Ok(match value_type {
                ValueType::Nullable(inner) => ValueType::Nullable(inner),
                value_type => ValueType::Nullable(Box::new(value_type)),
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
        ProjectField::renamed(format!("{prefix}authored_columns"), "authored_columns"),
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
    if let Some(branch_or_prefix) =
        version_witness_fields(&source.row_shape)?.branch_or_prefix_field
    {
        fields.push(ProjectField::renamed(
            format!("{prefix}{branch_or_prefix}"),
            branch_or_prefix,
        ));
    }
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
        ProjectField::named("authored_columns"),
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
    if let Some(branch_or_prefix) = version.branch_or_prefix_field {
        fields.push(ProjectField::named(branch_or_prefix));
    }
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
        ProjectField::null_typed(
            "authored_columns",
            ValueType::Nullable(Box::new(ValueType::Bytes)),
        ),
        ProjectField::named("created_by"),
        ProjectField::named("created_at"),
        ProjectField::named("updated_by"),
        ProjectField::named("updated_at"),
        ProjectField::nullable("_deletion", "_deletion"),
    ];
    fields.extend(source.table_schema.columns.iter().map(|column| {
        ProjectField::null_typed(
            table_user_column_field(&source.table_schema.name, &column.name),
            ValueType::Nullable(Box::new(column.column_type.clone())),
        )
    }));
    if let Some(branch_or_prefix) =
        version_witness_fields(&source.row_shape)?.branch_or_prefix_field
    {
        fields.push(ProjectField::named(branch_or_prefix));
    }
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
        branch_or_prefix_field: version.branch_or_prefix_field,
    })
}

fn prefixed_versioned_row_ref_schema(
    source: &ResolvedSource,
    prefix: &str,
) -> CapabilityResult<VersionedRowRefSchema> {
    let version = version_witness_fields(&source.row_shape)?;
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
        branch_or_prefix_field: version
            .branch_or_prefix_field
            .map(|_| format!("{prefix}_branch_or_prefix")),
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
        authored_columns_field: "authored_columns".to_owned(),
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
    pub(super) schema_version_field: String,
    pub(super) tx_time_field: String,
    pub(super) tx_node_field: String,
    pub(super) branch_or_prefix_field: Option<String>,
}

#[derive(Clone, Debug)]
struct CoverageFieldRefs {
    pub(super) coverage_field: String,
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
