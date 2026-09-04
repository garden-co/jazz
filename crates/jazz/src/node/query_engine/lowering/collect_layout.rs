use super::*;

pub(super) fn collect_layout(
    projection: &AppProjectionTree,
    plan: &AnalyzedQueryPlan,
    root_source: &ResolvedSource,
    resolved_sources: &BTreeMap<SourceId, ResolvedSource>,
    request: &QueryProgramRequest,
    routing_param_fields: &BTreeSet<String>,
    parameter_domain: &ParameterDomain,
    available_fields: &BTreeSet<String>,
) -> CapabilityResult<CollectLayout> {
    let explicit_root_projection = matches!(projection.fields, FieldProjection::Fields(_));
    let mut selected_root = BTreeSet::from([root_source.row_shape.row_uuid_field.clone()]);
    match &projection.fields {
        // Shape-default app rows are `CurrentRow`s, not a user-column-only
        // projection. Retain the same canonical magic provenance and current
        // version fields that one-shot materialization exposes, so opening,
        // reset, and incremental collector output share one descriptor.
        FieldProjection::All => {
            selected_root.extend(current_row_field_names(&root_source.table_schema))
        }
        FieldProjection::Fields(fields) => selected_root.extend(
            fields
                .iter()
                .filter(|field| {
                    !crate::query::is_implicit_row_id_alias(&root_source.table_schema, field)
                })
                .map(|field| collect_projection_source_field(root_source, field)),
        ),
    }
    let mut root_fields = root_source
        .row_shape
        .descriptor
        .fields()
        .iter()
        .filter_map(|field| {
            field.name.as_ref().map(|name| CollectFlatField {
                input: format!("__collect_root_{name}"),
                output: if name == &root_source.row_shape.row_uuid_field {
                    name.clone()
                } else {
                    collect_projection_output_field(name)
                },
                value_type: field.value_type.clone(),
                output_value_type: if explicit_root_projection {
                    collect_unwrapped_output_type(root_source, name, &field.value_type)
                } else {
                    field.value_type.clone()
                },
                source_field: Some(name.clone()),
                is_row_id: name == &root_source.row_shape.row_uuid_field,
                is_presence: false,
                is_output: selected_root.contains(name),
            })
        })
        .collect::<Vec<_>>();
    let root_occurrence_inputs = root_join_occurrence_fields(plan, resolved_sources, request)?
        .into_iter()
        .filter(|(name, _)| available_fields.contains(name))
        .map(|(name, value_type)| {
            let input = format!("__collect_root_{name}");
            root_fields.push(CollectFlatField {
                input: input.clone(),
                output: name.clone(),
                value_type: value_type.clone(),
                output_value_type: value_type,
                source_field: Some(name),
                is_row_id: false,
                is_presence: false,
                is_output: false,
            });
            input
        })
        .collect::<Vec<_>>();
    for route_field in routing_param_fields {
        let value_type = if let Some(param) = route_param_from_field(route_field) {
            parameter_domain
                .user_params
                .get(param)
                .or_else(|| request.input.binding.param_types.get(param))
                .cloned()
        } else {
            parameter_domain
                .claim_params
                .get(route_field)
                .map(|claim| claim.ty.clone())
        }
        .ok_or_else(|| {
            single_gap_report(UnsupportedReason::Runtime(format!(
                "collector route field {route_field:?} has no parameter type"
            )))
        })?;
        root_fields.push(CollectFlatField {
            input: route_field.clone(),
            output: route_field.clone(),
            value_type: value_type.clone(),
            output_value_type: value_type,
            source_field: Some(route_field.clone()),
            is_row_id: false,
            is_presence: false,
            is_output: true,
        });
    }
    let mut next_slot = 0usize;
    let slots = collect_slot_layouts(&projection.paths, resolved_sources, 1, &mut next_slot)?;
    Ok(CollectLayout {
        root_fields,
        root_occurrence_inputs,
        root_order_cols: Vec::new(),
        root_tie_cols: Vec::new(),
        root_offset: 0,
        root_limit: TopByLimit::Unbounded,
        slots,
    })
}

fn collect_unwrapped_output_type(
    source: &ResolvedSource,
    source_field: &str,
    fallback: &ValueType,
) -> ValueType {
    if source_field == source.row_shape.row_uuid_field {
        return fallback.clone();
    }
    let logical = logical_app_column(source_field);
    source
        .table_schema
        .columns
        .iter()
        .find(|column| column.name == logical)
        // The collector unwraps the current-row presence cell, not the
        // column's storage representation. Keep JSON cells and catalogue-bound
        // enums as emitted by the source; public-value hydration happens later.
        .map(|_| match fallback {
            ValueType::Nullable(inner) => inner.as_ref().clone(),
            value_type => value_type.clone(),
        })
        .unwrap_or_else(|| fallback.clone())
}

fn collect_slot_layouts(
    paths: &[AppPathProjection],
    resolved_sources: &BTreeMap<SourceId, ResolvedSource>,
    depth: usize,
    next_slot: &mut usize,
) -> CapabilityResult<Vec<CollectSlotLayout>> {
    if depth > MAX_COLLECT_BY_TREE_DEPTH {
        return Err(single_gap_report(UnsupportedReason::Operator(format!(
            "association projection depth {depth} exceeds Groove MAX_COLLECT_BY_TREE_DEPTH ({MAX_COLLECT_BY_TREE_DEPTH})"
        ))));
    }
    paths
        .iter()
        .map(|path| {
            let source = resolved_sources.get(&path.path.child).ok_or_else(|| {
                single_gap_report(UnsupportedReason::Runtime(format!(
                    "app projection path child source {:?} was not resolved",
                    path.path.child
                )))
            })?;
            let slot_id = *next_slot;
            *next_slot += 1;
            let prefix = format!("__collect_path_{slot_id}");
            let mut selected = BTreeSet::from([source.row_shape.row_uuid_field.clone()]);
            match &path.fields {
                FieldProjection::All => selected.extend(
                    source
                        .table_schema
                        .columns
                        .iter()
                        .map(|column| app_column_field(&column.name)),
                ),
                FieldProjection::Fields(fields) => selected.extend(
                    fields
                        .iter()
                        .filter(|field| {
                            !crate::query::is_implicit_row_id_alias(&source.table_schema, field)
                        })
                        .map(|field| {
                            collect_projection_source_field(source, field)
                        }),
                ),
            }
            let fields = source
                .row_shape
                .descriptor
                .fields()
                .iter()
                .filter_map(|field| field.name.clone())
                .filter(|source_field| selected.contains(source_field))
                .map(|source_field| {
                    let source_value_type = source_field_type(source, &source_field).cloned().ok_or_else(|| {
                        single_gap_report(UnsupportedReason::Operator(format!(
                            "association projection source {:?} does not provide field {source_field:?}",
                            source.row_shape.source
                        )))
                    })?;
                    let is_row_id = source_field == source.row_shape.row_uuid_field;
                    let output_value_type =
                        collect_unwrapped_output_type(source, &source_field, &source_value_type);
                    let value_type = if !is_row_id
                        && !matches!(source_value_type, ValueType::Nullable(_))
                    {
                        ValueType::Nullable(Box::new(source_value_type))
                    } else {
                        source_value_type
                    };
                    Ok(CollectFlatField {
                        input: format!("{prefix}_{source_field}"),
                        output: if is_row_id {
                            source_field.clone()
                        } else {
                            collect_nested_projection_output_field(&source_field)
                        },
                        value_type,
                        output_value_type,
                        source_field: Some(source_field),
                        is_row_id,
                        is_presence: false,
                        is_output: true,
                    })
                })
                .collect::<CapabilityResult<Vec<_>>>()?;
            let row_id_input = fields
                .iter()
                .find(|field| field.is_row_id)
                .expect("collector child projection always includes its row id")
                .input
                .clone();
            let presence_input = format!("{prefix}_present");
            let children = collect_slot_layouts(&path.children, resolved_sources, depth + 1, next_slot)?;
            Ok(CollectSlotLayout {
                path: path.path.clone(),
                collection_field: path.field.clone(),
                fields,
                row_id_input,
                presence_input,
                order_cols: Vec::new(),
                tie_cols: Vec::new(),
                offset: 0,
                limit: TopByLimit::Unbounded,
                children,
            })
        })
        .collect()
}

fn collect_projection_source_field(_source: &ResolvedSource, field: &str) -> String {
    match field {
        "$createdAt" | "$createdBy" | "$updatedAt" | "$updatedBy" => field.to_owned(),
        _ => app_column_field(field),
    }
}

fn collect_projection_output_field(field: &str) -> String {
    match field {
        "$createdAt" | "$createdBy" | "$updatedAt" | "$updatedBy" => field.to_owned(),
        "created_at" => "$createdAt".to_owned(),
        "created_by" => "$createdBy".to_owned(),
        "updated_at" => "$updatedAt".to_owned(),
        "updated_by" => "$updatedBy".to_owned(),
        // The terminal owns row assembly, but its encoded record remains in
        // the canonical current-row codec namespace. Native/WASM adapters map
        // `_app_*` fields to public column names from the negotiated schema;
        // emitting logical names here makes core CurrentRow decoding silently
        // treat every selected cell as absent.
        _ => field.to_owned(),
    }
}

fn collect_nested_projection_output_field(field: &str) -> String {
    match field {
        "$createdAt" | "$createdBy" | "$updatedAt" | "$updatedBy" => field.to_owned(),
        "created_at" => "$createdAt".to_owned(),
        "created_by" => "$createdBy".to_owned(),
        "updated_at" => "$updatedAt".to_owned(),
        "updated_by" => "$updatedBy".to_owned(),
        // Nested records are public tree payloads rather than CurrentRow codec
        // records, so their user columns retain the logical schema names.
        _ => logical_app_column(field).to_owned(),
    }
}

pub(super) fn root_collect_context_graph(
    graph: GraphBuilder,
    layout: &CollectLayout,
) -> CapabilityResult<GraphBuilder> {
    let fields = layout
        .root_fields
        .iter()
        .flat_map(|field| {
            let source_field = field
                .source_field
                .as_ref()
                .expect("root collector fields retain their source field");
            [
                ProjectField::named(source_field),
                ProjectField::renamed(source_field, &field.input),
            ]
        })
        .collect::<Vec<_>>();
    Ok(graph.project_fields(fields))
}

pub(super) fn collect_anchor_graph(
    graph: GraphBuilder,
    layout: &CollectLayout,
) -> CapabilityResult<GraphBuilder> {
    Ok(graph.project_fields(collect_flat_projection(layout, None, &BTreeSet::new())?))
}

pub(super) fn lower_collect_slot_graphs(
    slot: &CollectSlotLayout,
    path: &CorrelatedPathPlan,
    parent_graph: GraphBuilder,
    parent_source: &ResolvedSource,
    _root_source: &ResolvedSource,
    layout: &CollectLayout,
    inherited_flat_fields: &BTreeSet<String>,
    resolved_sources: &BTreeMap<SourceId, ResolvedSource>,
    request: &QueryProgramRequest,
) -> CapabilityResult<Vec<GraphBuilder>> {
    let joined = lower_correlated_path_relation_graph_from_parent(
        path,
        parent_graph,
        parent_source,
        resolved_sources,
        request,
        false,
    )
    .map_err(single_gap_report)?
    .graph;
    let association = joined.clone().project_fields(collect_flat_projection(
        layout,
        Some(slot),
        inherited_flat_fields,
    )?);
    let child_source = resolved_sources.get(&slot.path.child).ok_or_else(|| {
        single_gap_report(UnsupportedReason::Runtime(format!(
            "collector child source {:?} was not resolved",
            slot.path.child
        )))
    })?;
    let context = joined.project_fields(collect_child_context_projection(
        layout,
        slot,
        child_source,
        inherited_flat_fields,
    )?);
    let mut graphs = vec![association];
    let mut child_inherited = inherited_flat_fields.clone();
    child_inherited.extend(collect_slot_flat_field_names(slot));
    for nested_slot in &slot.children {
        let nested_path =
            find_nested_correlated_path(path, &nested_slot.path).ok_or_else(|| {
                single_gap_report(UnsupportedReason::Operator(format!(
                    "app projection path {:?} is not nested below {:?}",
                    nested_slot.path, slot.path
                )))
            })?;
        graphs.extend(lower_collect_slot_graphs(
            nested_slot,
            nested_path,
            context.clone(),
            child_source,
            _root_source,
            layout,
            &child_inherited,
            resolved_sources,
            request,
        )?);
    }
    Ok(graphs)
}

fn collect_flat_projection(
    layout: &CollectLayout,
    current_slot: Option<&CollectSlotLayout>,
    inherited_flat_fields: &BTreeSet<String>,
) -> CapabilityResult<Vec<ProjectField>> {
    let mut fields = layout
        .root_fields
        .iter()
        .map(|field| match current_slot {
            Some(_) => ProjectField::renamed(left_field(&field.input), &field.input),
            None => ProjectField::renamed(
                field
                    .source_field
                    .as_ref()
                    .expect("root collector fields retain their source field"),
                &field.input,
            ),
        })
        .collect::<Vec<_>>();
    for slot in collect_all_slots(&layout.slots) {
        let is_current = current_slot.is_some_and(|current| current.path == slot.path);
        for field in &slot.fields {
            fields.push(if is_current {
                let source = right_field(
                    field
                        .source_field
                        .as_ref()
                        .expect("collector child fields retain their source field"),
                );
                if field.is_row_id {
                    ProjectField::renamed(source, &field.input)
                } else {
                    // Anchor rows have no child, so collector child payload
                    // fields are nullable. Preserve that descriptor on actual
                    // child rows as well, rather than making the union depend
                    // on whether this particular source column is nullable.
                    if field.value_type == field.output_value_type {
                        // A nullable application field needs a distinct outer
                        // anchor wrapper when the current-row source does not
                        // already carry one. CollectBy removes only that
                        // wrapper, preserving an inner application NULL.
                        ProjectField::nullable(source, &field.input)
                    } else {
                        // Current-row storage already carries the exact outer
                        // wrapper required around the logical output type.
                        ProjectField::nullable_flat(source, &field.input)
                    }
                }
            } else if inherited_flat_fields.contains(&field.input) {
                ProjectField::renamed(left_field(&field.input), &field.input)
            } else {
                collect_flat_default(field)?
            });
        }
        fields.push(if is_current {
            ProjectField::literal(&slot.presence_input, Value::Bool(true))
        } else if inherited_flat_fields.contains(&slot.presence_input) {
            ProjectField::renamed(left_field(&slot.presence_input), &slot.presence_input)
        } else {
            ProjectField::literal(&slot.presence_input, Value::Bool(false))
        });
    }
    Ok(fields)
}

fn collect_child_context_projection(
    layout: &CollectLayout,
    current_slot: &CollectSlotLayout,
    child_source: &ResolvedSource,
    inherited_flat_fields: &BTreeSet<String>,
) -> CapabilityResult<Vec<ProjectField>> {
    let mut fields = child_source
        .row_shape
        .descriptor
        .fields()
        .iter()
        .filter_map(|field| field.name.as_ref())
        .map(|name| ProjectField::renamed(right_field(name), name))
        .collect::<Vec<_>>();
    fields.extend(collect_flat_projection(
        layout,
        Some(current_slot),
        inherited_flat_fields,
    )?);
    Ok(fields)
}

fn collect_flat_default(field: &CollectFlatField) -> CapabilityResult<ProjectField> {
    if field.is_row_id {
        return Ok(ProjectField::literal(
            &field.input,
            Value::Uuid(uuid::Uuid::nil()),
        ));
    }
    if field.is_presence {
        return Ok(ProjectField::literal(&field.input, Value::Bool(false)));
    }
    Ok(ProjectField::null_typed(
        &field.input,
        field.value_type.clone(),
    ))
}

fn collect_all_slots(slots: &[CollectSlotLayout]) -> Vec<&CollectSlotLayout> {
    let mut all = Vec::new();
    for slot in slots {
        all.push(slot);
        all.extend(collect_all_slots(&slot.children));
    }
    all
}

fn collect_slot_flat_field_names(slot: &CollectSlotLayout) -> BTreeSet<String> {
    slot.fields
        .iter()
        .map(|field| field.input.clone())
        .chain(std::iter::once(slot.presence_input.clone()))
        .collect()
}

pub(super) fn collect_slot_builder(
    slot: &CollectSlotLayout,
    parent_row_id: &str,
    route_fields: &BTreeSet<String>,
) -> CollectBySlotBuilder {
    CollectBySlotBuilder::new(
        std::iter::once(parent_row_id.to_owned()).chain(route_fields.iter().cloned()),
        slot.fields
            .iter()
            .filter(|field| field.is_output)
            .map(|field| {
                if field.is_row_id || field.value_type == field.output_value_type {
                    CollectByField::renamed(&field.input, &field.output)
                } else {
                    CollectByField::renamed_unwrap_nullable(&field.input, &field.output)
                }
            }),
        &slot.collection_field,
        slot.children
            .iter()
            .map(|child| collect_slot_builder(child, &slot.row_id_input, route_fields)),
        slot.order_cols.clone(),
        slot.tie_cols.clone(),
        slot.offset,
        slot.limit,
    )
    // Route fields identify a maintained binding, but are not application
    // fields on nested records. Carry them only as execution owner keys so a
    // grandchild can still group by the same binding without exposing them in
    // the nested descriptor.
    .with_owner_key_cols(route_fields.iter().cloned())
    .with_presence_col(&slot.presence_input)
}

pub(super) fn collect_output_descriptor(
    layout: &CollectLayout,
) -> CapabilityResult<RecordDescriptor> {
    let mut fields = layout
        .root_fields
        .iter()
        .filter(|field| field.is_output)
        .map(|field| (field.output.clone(), field.output_value_type.clone()))
        .collect::<Vec<_>>();
    fields.extend(
        layout
            .slots
            .iter()
            .map(collect_slot_output_field)
            .collect::<CapabilityResult<Vec<_>>>()?,
    );
    Ok(RecordDescriptor::new(fields))
}

fn collect_slot_output_field(slot: &CollectSlotLayout) -> CapabilityResult<(String, ValueType)> {
    Ok((
        slot.collection_field.clone(),
        ValueType::Array(Box::new(ValueType::Record(Box::new(
            collect_slot_output_descriptor(slot)?,
        )))),
    ))
}

fn collect_slot_output_descriptor(slot: &CollectSlotLayout) -> CapabilityResult<RecordDescriptor> {
    let mut fields = slot
        .fields
        .iter()
        .filter(|field| field.is_output)
        .map(|field| (field.output.clone(), field.output_value_type.clone()))
        .collect::<Vec<_>>();
    fields.extend(
        slot.children
            .iter()
            .map(collect_slot_output_field)
            .collect::<CapabilityResult<Vec<_>>>()?,
    );
    Ok(RecordDescriptor::new(fields))
}
