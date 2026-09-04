//! Record descriptors, projection, enum remapping, and operator shape validation.

use super::*;
use crate::records::{DescriptorField, collect_by_ordered_scalar};

pub(super) fn extend_root_window_positions(
    descriptor: RecordDescriptor,
    window: &[WindowedRecord],
    positions: &mut BTreeMap<Vec<u8>, usize>,
) -> Result<(), IvmRuntimeError> {
    let mut index = 0usize;
    for (record, copies) in window {
        let key = encoded_record_key_part(descriptor, record, &[0])?;
        positions.entry(key).or_insert(index);
        index = index.saturating_add(usize::try_from(*copies).unwrap_or(usize::MAX));
    }
    Ok(())
}

pub(super) fn apply_root_ordering_operations(
    before: &BTreeMap<Vec<u8>, usize>,
    after: &BTreeMap<Vec<u8>, usize>,
    root_descriptor: RecordDescriptor,
    terminal: &mut TerminalDeltas,
) {
    let mut current = before
        .iter()
        .map(|(key, index)| (*index, key.clone()))
        .collect::<Vec<_>>();
    current.sort_by_key(|(index, _)| *index);
    let mut current = current.into_iter().map(|(_, key)| key).collect::<Vec<_>>();
    for operation in &mut terminal.operations {
        if !operation.path.is_empty() {
            continue;
        }
        match &mut operation.edit {
            TerminalEdit::Insert { index, key, .. } => {
                if let Some(actual) = after.get(key) {
                    *index = *actual;
                }
                if let Some(existing) = current.iter().position(|candidate| candidate == key) {
                    current.remove(existing);
                }
                current.insert((*index).min(current.len()), key.clone());
            }
            TerminalEdit::Remove { key } => {
                if let Some(existing) = current.iter().position(|candidate| candidate == key) {
                    current.remove(existing);
                }
            }
            TerminalEdit::Update { .. } | TerminalEdit::Move { .. } => {}
        }
    }

    // Payload/nested edits are applied first. Positional edits follow, so a
    // consumer never observes a move targeting a root that is not present.
    let mut desired = after
        .iter()
        .map(|(key, index)| (*index, key.clone()))
        .collect::<Vec<_>>();
    desired.sort_by_key(|(index, _)| *index);
    for (after_index, key) in desired {
        if current.get(after_index) != Some(&key)
            && let Some(existing) = current.iter().position(|candidate| candidate == &key)
        {
            current.remove(existing);
            current.insert(after_index.min(current.len()), key.clone());
            terminal.operations.push(TerminalOperation {
                root_descriptor,
                root_key: key.clone(),
                path: Vec::new(),
                edit: TerminalEdit::Move {
                    key,
                    index: after_index,
                },
            });
        }
    }
}

pub(super) fn project_descriptor(
    input: &RecordDescriptor,
    fields: &[crate::ivm::ProjectField],
) -> Result<RecordDescriptor, IvmRuntimeError> {
    fields
        .iter()
        .map(|project_field| {
            let value_type = match &project_field.expression {
                ProjectExpr::Field(source) => {
                    let source_idx = resolve_field_ref(input, source)?;
                    input
                        .fields()
                        .get(source_idx)
                        .ok_or(IvmRuntimeError::GraphFieldIndexOutOfBounds(source_idx))?
                        .value_type
                        .clone()
                }
                ProjectExpr::Literal(value) => value
                    .value_type()
                    .ok_or(IvmRuntimeError::UnsupportedOperator)?,
                ProjectExpr::TypedLiteral { value_type, .. } => value_type.clone(),
                ProjectExpr::Null(value_type) => value_type.clone(),
                ProjectExpr::Nullable(source) => {
                    let source_idx = resolve_field_ref(input, source)?;
                    let inner = input
                        .fields()
                        .get(source_idx)
                        .ok_or(IvmRuntimeError::GraphFieldIndexOutOfBounds(source_idx))?
                        .value_type
                        .clone();
                    ValueType::Nullable(Box::new(inner))
                }
                ProjectExpr::NullableFlat(source) => {
                    let source_idx = resolve_field_ref(input, source)?;
                    let inner = input
                        .fields()
                        .get(source_idx)
                        .ok_or(IvmRuntimeError::GraphFieldIndexOutOfBounds(source_idx))?
                        .value_type
                        .clone();
                    match inner {
                        ValueType::Nullable(_) => inner,
                        other => ValueType::Nullable(Box::new(other)),
                    }
                }
                ProjectExpr::EnumTagRemap { source, .. } => {
                    let source_idx = resolve_field_ref(input, source)?;
                    input
                        .fields()
                        .get(source_idx)
                        .ok_or(IvmRuntimeError::GraphFieldIndexOutOfBounds(source_idx))?
                        .value_type
                        .clone()
                }
                ProjectExpr::EnumRemap { source, .. } => {
                    let source_idx = resolve_field_ref(input, source)?;
                    input
                        .fields()
                        .get(source_idx)
                        .ok_or(IvmRuntimeError::GraphFieldIndexOutOfBounds(source_idx))?
                        .value_type
                        .clone()
                }
                ProjectExpr::RecursiveEnumRemap { target, .. } => target.clone(),
            };
            Ok(
                DescriptorField::new(project_field.output_name.clone(), value_type)
                    .with_identity(project_field.output_identity.clone()),
            )
        })
        .collect::<Result<Vec<_>, IvmRuntimeError>>()
        .map(RecordDescriptor::new_with_fields)
}

pub(super) fn collect_by_descriptor(
    input: &RecordDescriptor,
    parent_fields: &[CollectByField],
    child_fields: &[CollectByField],
    collection_field: &str,
) -> Result<RecordDescriptor, IvmRuntimeError> {
    if parent_fields.is_empty() || child_fields.is_empty() || collection_field.is_empty() {
        return Err(IvmRuntimeError::InvalidCollectBy(
            "parent projection, child projection, and collection slot are required".into(),
        ));
    }
    let mut names = HashSet::new();
    let mut output = Vec::with_capacity(parent_fields.len() + 1);
    for field in parent_fields {
        if !names.insert(field.output_name.clone()) || field.output_name == collection_field {
            return Err(IvmRuntimeError::InvalidCollectBy(
                "output field names must be unique".into(),
            ));
        }
        let index = resolve_field_ref(input, &field.field)?;
        output.push((
            field.output_name.clone(),
            collect_by_field_value_type(input, index, field)?,
        ));
    }
    let mut child_names = HashSet::new();
    let mut child = Vec::with_capacity(child_fields.len());
    for field in child_fields {
        if !child_names.insert(field.output_name.clone()) {
            return Err(IvmRuntimeError::InvalidCollectBy(
                "child output field names must be unique".into(),
            ));
        }
        let index = resolve_field_ref(input, &field.field)?;
        child.push((
            field.output_name.clone(),
            collect_by_field_value_type(input, index, field)?,
        ));
    }
    output.push((
        collection_field.to_owned(),
        ValueType::Array(Box::new(ValueType::Record(Box::new(
            RecordDescriptor::new(child),
        )))),
    ));
    Ok(RecordDescriptor::new(output))
}

pub(super) fn collect_by_root_descriptor(
    input: &RecordDescriptor,
    parent_fields: &[CollectByField],
) -> Result<RecordDescriptor, IvmRuntimeError> {
    if parent_fields.is_empty() {
        return Err(IvmRuntimeError::InvalidCollectBy(
            "root collect requires a parent projection".into(),
        ));
    }
    parent_fields
        .iter()
        .map(|field| {
            let index = resolve_field_ref(input, &field.field)?;
            Ok(DescriptorField::new(
                field.output_name.clone(),
                collect_by_field_value_type(input, index, field)?,
            ))
        })
        .collect::<Result<Vec<_>, IvmRuntimeError>>()
        .map(RecordDescriptor::new_with_fields)
}

pub(super) fn collect_by_tree_descriptor(
    input: &RecordDescriptor,
    parent_fields: &[CollectByField],
    slots: &[CollectBySlotBuilder],
) -> Result<RecordDescriptor, IvmRuntimeError> {
    if parent_fields.is_empty() || slots.is_empty() {
        return Err(IvmRuntimeError::InvalidCollectBy(
            "tree collect requires a parent projection and at least one collection slot".into(),
        ));
    }
    let mut names = HashSet::new();
    let mut output = Vec::with_capacity(parent_fields.len() + slots.len());
    for field in parent_fields {
        if !names.insert(field.output_name.clone()) {
            return Err(IvmRuntimeError::InvalidCollectBy(
                "output field names must be unique".into(),
            ));
        }
        let index = resolve_field_ref(input, &field.field)?;
        output.push((
            field.output_name.clone(),
            collect_by_field_value_type(input, index, field)?,
        ));
    }
    for slot in slots {
        if !names.insert(slot.collection_field.clone()) {
            return Err(IvmRuntimeError::InvalidCollectBy(
                "output field names must be unique".into(),
            ));
        }
        output.push((
            slot.collection_field.clone(),
            ValueType::Array(Box::new(ValueType::Record(Box::new(
                collect_by_slot_descriptor(input, slot, 1)?,
            )))),
        ));
    }
    Ok(RecordDescriptor::new(output))
}

fn collect_by_slot_descriptor(
    input: &RecordDescriptor,
    slot: &CollectBySlotBuilder,
    depth: usize,
) -> Result<RecordDescriptor, IvmRuntimeError> {
    if depth > MAX_COLLECT_BY_TREE_DEPTH {
        return Err(IvmRuntimeError::InvalidCollectBy(format!(
            "tree collect depth exceeds MAX_COLLECT_BY_TREE_DEPTH ({MAX_COLLECT_BY_TREE_DEPTH})"
        )));
    }
    if slot.group_cols.is_empty()
        || slot.child_fields.is_empty()
        || slot.collection_field.is_empty()
        || slot.order_cols.is_empty()
        || slot.tie_cols.is_empty()
    {
        return Err(IvmRuntimeError::InvalidCollectBy(
            "each tree collection slot requires group, child, name, order, and tie fields".into(),
        ));
    }
    let mut names = HashSet::new();
    let mut output = Vec::with_capacity(slot.child_fields.len() + slot.slots.len());
    for field in &slot.child_fields {
        if !names.insert(field.output_name.clone()) {
            return Err(IvmRuntimeError::InvalidCollectBy(
                "child output field names must be unique".into(),
            ));
        }
        let index = resolve_field_ref(input, &field.field)?;
        output.push((
            field.output_name.clone(),
            collect_by_field_value_type(input, index, field)?,
        ));
    }
    for nested in &slot.slots {
        if !names.insert(nested.collection_field.clone()) {
            return Err(IvmRuntimeError::InvalidCollectBy(
                "child output field names must be unique".into(),
            ));
        }
        output.push((
            nested.collection_field.clone(),
            ValueType::Array(Box::new(ValueType::Record(Box::new(
                collect_by_slot_descriptor(input, nested, depth + 1)?,
            )))),
        ));
    }
    Ok(RecordDescriptor::new(output))
}

pub(super) fn collect_by_slots(
    input: &RecordDescriptor,
    builders: &[CollectBySlotBuilder],
    owner_indices: &[usize],
    depth: usize,
) -> Result<Vec<CollectBySlot>, IvmRuntimeError> {
    if depth > MAX_COLLECT_BY_TREE_DEPTH {
        return Err(IvmRuntimeError::InvalidCollectBy(format!(
            "tree collect depth exceeds MAX_COLLECT_BY_TREE_DEPTH ({MAX_COLLECT_BY_TREE_DEPTH})"
        )));
    }
    let mut names = HashSet::new();
    builders
        .iter()
        .enumerate()
        .map(|(slot_index, builder)| {
            if !names.insert(builder.collection_field.clone()) {
                return Err(IvmRuntimeError::InvalidCollectBy(
                    "sibling collection slot names must be unique".into(),
                ));
            }
            let group_field_indices = builder
                .group_cols
                .iter()
                .map(|field| resolve_field_ref(input, field))
                .collect::<Result<Vec<_>, _>>()?;
            if group_field_indices.is_empty()
                || group_field_indices
                    .iter()
                    .any(|field| !owner_indices.contains(field))
            {
                return Err(IvmRuntimeError::InvalidCollectBy(
                    "a slot group must be available on its owning record".into(),
                ));
            }
            validate_collect_by_key_types(input, &group_field_indices)?;
            let child_fields = collect_by_projections(input, &builder.child_fields)?;
            let order_field_indices = builder
                .order_cols
                .iter()
                .map(|order| resolve_field_ref(input, &order.field))
                .collect::<Result<Vec<_>, _>>()?;
            let tie_field_indices = builder
                .tie_cols
                .iter()
                .map(|field| resolve_field_ref(input, field))
                .collect::<Result<Vec<_>, _>>()?;
            let presence_field_index = builder
                .presence_col
                .as_ref()
                .map(|field| resolve_field_ref(input, field))
                .transpose()?;
            if let Some(index) = presence_field_index
                && input.fields()[index].value_type != ValueType::Bool
            {
                return Err(IvmRuntimeError::InvalidCollectBy(
                    "a tree collection presence field must be boolean".into(),
                ));
            }
            if order_field_indices.is_empty() || tie_field_indices.is_empty() {
                return Err(IvmRuntimeError::InvalidCollectBy(
                    "order and tie fields must both be complete and non-empty".into(),
                ));
            }
            validate_collect_by_key_types(input, &order_field_indices)?;
            validate_collect_by_key_types(input, &tie_field_indices)?;
            let child_descriptor = collect_by_slot_descriptor(input, builder, depth)?;
            let child_indices = child_fields
                .iter()
                .map(|field| field.field_idx)
                .collect::<Vec<_>>();
            // Nested slots address the raw input record selected for this
            // child, not its rendered descriptor. Most owner keys are child
            // projection fields, but maintained routing keys must remain
            // internal: carrying them through the app descriptor would leak
            // binding metadata. `owner_key_cols` is the explicit internal
            // channel for those stable grouping keys.
            let mut owner_indices = child_indices.clone();
            for owner_key in &builder.owner_key_cols {
                let owner_key_index = resolve_field_ref(input, owner_key)?;
                if !group_field_indices.contains(&owner_key_index) {
                    return Err(IvmRuntimeError::InvalidCollectBy(
                        "a slot owner key must also be a grouping field".into(),
                    ));
                }
                if !owner_indices.contains(&owner_key_index) {
                    owner_indices.push(owner_key_index);
                }
            }
            let slots = collect_by_slots(input, &builder.slots, &owner_indices, depth + 1)?;
            Ok(CollectBySlot {
                group_fields: group_field_indices
                    .iter()
                    .map(|field| field_name_at(input, *field))
                    .collect::<Result<Vec<_>, _>>()?,
                group_field_indices,
                child_fields,
                child_descriptor,
                collection_field: builder.collection_field.clone(),
                collection_field_index: builder.child_fields.len() + slot_index,
                slots,
                order_fields: builder
                    .order_cols
                    .iter()
                    .zip(&order_field_indices)
                    .map(|(order, field)| {
                        Ok(TopByOrderField {
                            field: field_name_at(input, *field)?,
                            direction: order.direction,
                        })
                    })
                    .collect::<Result<Vec<_>, IvmRuntimeError>>()?,
                tie_fields: tie_field_indices
                    .iter()
                    .map(|field| field_name_at(input, *field))
                    .collect::<Result<Vec<_>, _>>()?,
                presence_field_index,
                sort_field_indices: order_field_indices
                    .iter()
                    .chain(&tie_field_indices)
                    .copied()
                    .collect(),
                sort_directions: builder
                    .order_cols
                    .iter()
                    .map(|order| order.direction)
                    .chain(std::iter::repeat_n(
                        TopByDirection::Asc,
                        tie_field_indices.len(),
                    ))
                    .collect(),
                offset: builder.offset,
                limit: builder.limit,
            })
        })
        .collect()
}

pub(super) fn collect_by_expand_descriptor(
    input: &RecordDescriptor,
    tuple_fields: &[CollectByField],
) -> Result<RecordDescriptor, IvmRuntimeError> {
    if tuple_fields.is_empty() {
        return Err(IvmRuntimeError::InvalidCollectBy(
            "expand mode requires a non-empty tuple projection".into(),
        ));
    }
    let mut names = HashSet::new();
    tuple_fields
        .iter()
        .map(|field| {
            if !names.insert(field.output_name.clone()) {
                return Err(IvmRuntimeError::InvalidCollectBy(
                    "expand tuple output field names must be unique".into(),
                ));
            }
            let index = resolve_field_ref(input, &field.field)?;
            Ok((
                field.output_name.clone(),
                collect_by_field_value_type(input, index, field)?,
            ))
        })
        .collect::<Result<Vec<_>, IvmRuntimeError>>()
        .map(RecordDescriptor::new)
}

pub(super) fn collect_by_projections(
    input: &RecordDescriptor,
    fields: &[CollectByField],
) -> Result<Vec<CollectByProjection>, IvmRuntimeError> {
    fields
        .iter()
        .map(|field| {
            Ok(CollectByProjection {
                field: field_ref_name(input, &field.field)?,
                field_idx: resolve_field_ref(input, &field.field)?,
                output_name: field.output_name.clone(),
                unwrap_nullable: field.unwrap_nullable,
            })
        })
        .collect()
}

fn collect_by_field_value_type(
    input: &RecordDescriptor,
    index: usize,
    field: &CollectByField,
) -> Result<ValueType, IvmRuntimeError> {
    let value_type = input.fields()[index].value_type.clone();
    if !field.unwrap_nullable {
        return Ok(value_type);
    }
    Ok(match value_type {
        ValueType::Nullable(inner) => *inner,
        value_type => value_type,
    })
}

pub(super) fn validate_collect_by_key_types(
    input: &RecordDescriptor,
    indices: &[usize],
) -> Result<(), IvmRuntimeError> {
    for &index in indices {
        let value_type = &input
            .fields()
            .get(index)
            .ok_or(IvmRuntimeError::GraphFieldIndexOutOfBounds(index))?
            .value_type;
        let scalar = collect_by_ordered_scalar(value_type);
        if value_type.contains_record() || !scalar {
            return Err(IvmRuntimeError::InvalidCollectBy(
                "group, order, and tie fields must be scalar ordered values without records".into(),
            ));
        }
    }
    Ok(())
}

pub(super) fn validate_collect_by_terminality(graph: &GraphBuilder) -> Result<(), IvmRuntimeError> {
    fn contains(
        children: impl IntoIterator<Item = *const GraphBuilder>,
        seen: &HashMap<usize, bool>,
    ) -> bool {
        children
            .into_iter()
            .any(|child| seen.get(&(child as usize)).copied().unwrap_or(false))
    }

    let mut contains_collect = HashMap::default();
    for node in graph.postorder() {
        let children_contain_collect = match node {
            GraphBuilder::Filter { input, .. }
            | GraphBuilder::Project { input, .. }
            | GraphBuilder::StreamingChecksum { input, .. }
            | GraphBuilder::UnwrapNullable { input, .. }
            | GraphBuilder::Unnest { input, .. }
            | GraphBuilder::VariantProject { input, .. }
            | GraphBuilder::ArgMaxBy { input, .. }
            | GraphBuilder::ArgMinBy { input, .. }
            | GraphBuilder::TopBy { input, .. }
            | GraphBuilder::CollectBy { input, .. }
            | GraphBuilder::Aggregate { input, .. } => {
                contains([input.as_ref() as *const GraphBuilder], &contains_collect)
            }
            GraphBuilder::Union { inputs } => contains(
                inputs
                    .iter()
                    .map(|input| input.as_ref() as *const GraphBuilder),
                &contains_collect,
            ),
            GraphBuilder::Join { left, right, .. }
            | GraphBuilder::SemiJoin { left, right, .. }
            | GraphBuilder::AntiJoin { left, right, .. } => contains(
                [
                    left.as_ref() as *const GraphBuilder,
                    right.as_ref() as *const GraphBuilder,
                ],
                &contains_collect,
            ),
            GraphBuilder::Recursive {
                seed,
                step,
                step_witness,
                ..
            } => {
                contains(
                    [
                        seed.as_ref() as *const GraphBuilder,
                        step.as_ref() as *const GraphBuilder,
                    ],
                    &contains_collect,
                ) || step_witness.as_ref().is_some_and(|witness| {
                    contains([witness.as_ref() as *const GraphBuilder], &contains_collect)
                })
            }
            GraphBuilder::RecursiveStepWitness { recursive } => contains(
                [recursive.as_ref() as *const GraphBuilder],
                &contains_collect,
            ),
            GraphBuilder::Table { .. }
            | GraphBuilder::InlineRecords { .. }
            | GraphBuilder::InputSource { .. }
            | GraphBuilder::Index { .. }
            | GraphBuilder::FrontierSource { .. }
            | GraphBuilder::BindingSource { .. } => false,
        };

        if children_contain_collect {
            return Err(IvmRuntimeError::CollectByMustBeTerminal);
        }
        contains_collect.insert(
            node as *const GraphBuilder as usize,
            children_contain_collect || matches!(node, GraphBuilder::CollectBy { .. }),
        );
    }
    Ok(())
}

pub(super) fn project_field_expr(
    input: &RecordDescriptor,
    field: &ProjectField,
) -> Result<PlanExpr, IvmRuntimeError> {
    match &field.expression {
        ProjectExpr::Field(source) => Ok(PlanExpr::field(field_ref_name(input, source)?)),
        ProjectExpr::Literal(value) => Ok(PlanExpr::literal(value.clone())),
        ProjectExpr::TypedLiteral { value, .. } => Ok(PlanExpr::literal(value.clone())),
        ProjectExpr::Null(value_type) => Ok(PlanExpr::null(value_type.clone())),
        ProjectExpr::Nullable(source) => Ok(PlanExpr::nullable(field_ref_name(input, source)?)),
        ProjectExpr::NullableFlat(source) => {
            Ok(PlanExpr::nullable_flat(field_ref_name(input, source)?))
        }
        ProjectExpr::EnumTagRemap { source, tags } => Ok(PlanExpr::EnumTagRemap {
            field: field_ref_name(input, source)?,
            tags: tags.clone(),
        }),
        ProjectExpr::EnumRemap { source, tags } => Ok(PlanExpr::EnumRemap {
            field: field_ref_name(input, source)?,
            tags: tags.clone(),
        }),
        ProjectExpr::RecursiveEnumRemap {
            source,
            remaps,
            omit_unrepresentable,
            ..
        } => Ok(PlanExpr::RecursiveEnumRemap {
            field: field_ref_name(input, source)?,
            remaps: remaps.clone(),
            omit_unrepresentable: *omit_unrepresentable,
        }),
    }
}

pub(super) fn project_record(
    expressions: &[ProjectionExpr],
    mapping: &[(usize, usize)],
    output_desc: RecordDescriptor,
    input_desc: &RecordDescriptor,
    input_record: &[u8],
) -> Result<Vec<u8>, IvmRuntimeError> {
    if projection_uses_raw_copy(expressions, mapping, output_desc) {
        return Ok(output_desc.project_record_raw(
            std::slice::from_ref(input_desc),
            &[input_record],
            mapping,
        )?);
    }

    let input = BorrowedRecord::new(input_record, input_desc);
    let mut values = Vec::with_capacity(expressions.len());
    for expr in expressions {
        let resolved = |field: &String| -> Result<Value, IvmRuntimeError> {
            let source_idx = resolve_field_name(input_desc, field)
                .ok_or_else(|| IvmRuntimeError::GraphFieldNotFound(field.clone()))?;
            input
                .get_idx(source_idx)
                .map_err(IvmRuntimeError::RecordEncoding)
        };
        values.push(match &expr.expression {
            PlanExpr::Field(field) => resolved(field)?,
            PlanExpr::Literal(value) => value.to_value(),
            PlanExpr::Null(_) => Value::Nullable(None),
            PlanExpr::Nullable(field) => Value::Nullable(Some(Box::new(resolved(field)?))),
            PlanExpr::NullableFlat(field) => {
                let value = resolved(field)?;
                if matches!(value, Value::Nullable(_)) {
                    value
                } else {
                    Value::Nullable(Some(Box::new(value)))
                }
            }
            PlanExpr::EnumTagRemap { field, tags } => remap_enum_tag(resolved(field)?, tags)?,
            PlanExpr::EnumRemap { field, tags } => remap_enum(resolved(field)?, tags)?,
            PlanExpr::RecursiveEnumRemap { field, remaps, .. } => {
                let source_idx = resolve_field_name(input_desc, field)
                    .ok_or_else(|| IvmRuntimeError::GraphFieldNotFound(field.clone()))?;
                let output_idx = output_desc
                    .field_index(expr.output_name.as_deref().ok_or_else(|| {
                        IvmRuntimeError::GraphFieldNotFound("<unnamed>".to_owned())
                    })?)
                    .ok_or_else(|| IvmRuntimeError::GraphFieldNotFound(field.clone()))?;
                remap_recursive_enum_value(
                    resolved(field)?,
                    &input_desc.fields()[source_idx].value_type,
                    &output_desc.fields()[output_idx].value_type,
                    remaps,
                    "root",
                )?
            }
        });
    }
    Ok(output_desc.create(&values)?)
}

pub(super) fn remap_enum_tag(value: Value, tags: &[Option<u8>]) -> Result<Value, IvmRuntimeError> {
    match value {
        Value::EnumTag(tag) => tags
            .get(usize::from(tag))
            .and_then(|tag| *tag)
            .map(Value::EnumTag)
            .ok_or(IvmRuntimeError::EnumTagProjectionAbsent { tag }),
        Value::Nullable(None) => Ok(Value::Nullable(None)),
        Value::Nullable(Some(value)) => Ok(Value::Nullable(Some(Box::new(remap_enum_tag(
            *value, tags,
        )?)))),
        _ => Err(IvmRuntimeError::EnumTagProjectionNonEnum),
    }
}

pub(super) fn remap_enum(value: Value, tags: &[Option<u32>]) -> Result<Value, IvmRuntimeError> {
    match value {
        Value::Enum(value) => {
            let tag = value.tag();
            let mapped = tags
                .get(
                    usize::try_from(tag)
                        .map_err(|_| IvmRuntimeError::EnumProjectionAbsent { tag })?,
                )
                .and_then(|tag| *tag)
                .ok_or(IvmRuntimeError::EnumProjectionAbsent { tag })?;
            Ok(Value::Enum(crate::records::EnumValue::new(
                mapped,
                value.into_record(),
            )))
        }
        Value::Nullable(None) => Ok(Value::Nullable(None)),
        Value::Nullable(Some(value)) => {
            Ok(Value::Nullable(Some(Box::new(remap_enum(*value, tags)?))))
        }
        _ => Err(IvmRuntimeError::EnumProjectionNonEnum),
    }
}

/// Re-encode an arbitrary user value across two descriptors whose enum
/// registries use different compact tags.  This is intentionally a value
/// operation rather than a byte splice: descriptor equality does not imply
/// that a physical enum tag has the same meaning in the authored descriptor.
pub(super) fn remap_recursive_enum_value(
    value: Value,
    source: &ValueType,
    target: &ValueType,
    remaps: &RecursiveEnumRemaps,
    path: &str,
) -> Result<Value, IvmRuntimeError> {
    match (value, source, target) {
        (Value::EnumTag(tag), ValueType::EnumTag(_), ValueType::EnumTag(_)) => remaps
            .scalar
            .get(path)
            .and_then(|tags| tags.get(usize::from(tag)))
            .and_then(|tag| *tag)
            .map(Value::EnumTag)
            .ok_or(IvmRuntimeError::EnumTagProjectionAbsent { tag }),
        (Value::Nullable(None), ValueType::Nullable(_), ValueType::Nullable(_)) => {
            Ok(Value::Nullable(None))
        }
        (
            Value::Nullable(Some(value)),
            ValueType::Nullable(source),
            ValueType::Nullable(target),
        ) => {
            // Jazz storage adds one nullable carrier around every user cell.
            // That carrier is not an authored enum occurrence, so a direct
            // enum stored in a nullable cell remains rooted at `path`. An
            // authored nullable enum *does* have a `path/nullable` occurrence
            // entry. Prefer that structural child whenever it exists.
            let nullable_path = format!("{path}/nullable");
            let child_path = if remap_path_with_enum_occurrence_below(remaps, &nullable_path) {
                nullable_path.as_str()
            } else {
                path
            };
            Ok(Value::Nullable(Some(Box::new(remap_recursive_enum_value(
                *value, source, target, remaps, child_path,
            )?))))
        }
        (Value::Array(values), ValueType::Array(source), ValueType::Array(target)) => {
            Ok(Value::Array(
                values
                    .into_iter()
                    .map(|value| {
                        remap_recursive_enum_value(
                            value,
                            source,
                            target,
                            remaps,
                            &format!("{path}/array"),
                        )
                    })
                    .collect::<Result<Vec<_>, _>>()?,
            ))
        }
        (Value::Tuple(values), ValueType::Tuple(source), ValueType::Tuple(target))
            if values.len() == source.len() && source.len() == target.len() =>
        {
            Ok(Value::Tuple(
                values
                    .into_iter()
                    .zip(source.iter().zip(target))
                    .enumerate()
                    .map(|(index, (value, (source, target)))| {
                        remap_recursive_enum_value(
                            value,
                            source,
                            target,
                            remaps,
                            &format!("{path}/tuple/{index}"),
                        )
                    })
                    .collect::<Result<Vec<_>, _>>()?,
            ))
        }
        (Value::Record(record), ValueType::Record(source), ValueType::Record(target))
            if source.fields().len() == target.fields().len() =>
        {
            let values = record.to_values()?;
            let values = values
                .into_iter()
                .zip(source.fields().iter().zip(target.fields()))
                .map(|(value, (source, target))| {
                    let name = source.name.as_deref().ok_or_else(|| {
                        IvmRuntimeError::RecursiveEnumProjectionDescriptorMismatch {
                            path: path.to_owned(),
                        }
                    })?;
                    remap_recursive_enum_value(
                        value,
                        &source.value_type,
                        &target.value_type,
                        remaps,
                        &format!("{path}/record/{name}"),
                    )
                })
                .collect::<Result<Vec<_>, _>>()?;
            Ok(Value::Record(OwnedRecord::new(
                target.create(&values)?,
                **target,
            )))
        }
        (Value::Enum(value), ValueType::Enum(source), ValueType::Enum(target)) => {
            let source_tag = value.tag();
            let target_tag = remaps
                .payload
                .get(path)
                .and_then(|tags| tags.get(usize::try_from(source_tag).ok()?))
                .and_then(|tag| *tag)
                .ok_or(IvmRuntimeError::EnumProjectionAbsent { tag: source_tag })?;
            let source_case = source.case(source_tag)?;
            let target_case = target.case(target_tag)?;
            if source_case.payload.fields().len() != target_case.payload.fields().len() {
                return Err(IvmRuntimeError::RecursiveEnumProjectionDescriptorMismatch {
                    path: path.to_owned(),
                });
            }
            let semantic_child_root = remaps
                .payload_children
                .get(path)
                .and_then(|paths| paths.get(usize::try_from(source_tag).ok()?))
                .and_then(|path| path.as_deref());
            let child_root = semantic_child_root
                // Older callers which have no nested payload occurrence keep
                // the historic local-tag spelling. Schema lowering supplies
                // a semantic GlobalCaseId-rooted path whenever descendants
                // are present.
                .map(str::to_owned)
                .unwrap_or_else(|| format!("{path}/case/{source_tag}"));
            let values = value.record().to_values()?;
            let values = values
                .into_iter()
                .zip(
                    source_case
                        .payload
                        .fields()
                        .iter()
                        .zip(target_case.payload.fields()),
                )
                .map(|(value, (source, target))| {
                    let name = source.name.as_deref().ok_or_else(|| {
                        IvmRuntimeError::RecursiveEnumProjectionDescriptorMismatch {
                            path: path.to_owned(),
                        }
                    })?;
                    remap_recursive_enum_value(
                        value,
                        &source.value_type,
                        &target.value_type,
                        remaps,
                        &if semantic_child_root.is_some() {
                            format!("{child_root}/record/{name}")
                        } else {
                            format!("{child_root}/{name}")
                        },
                    )
                })
                .collect::<Result<Vec<_>, _>>()?;
            Ok(Value::Enum(EnumValue::new(
                target_tag,
                OwnedRecord::new(target_case.payload.create(&values)?, target_case.payload),
            )))
        }
        (value, source, target) if source == target => Ok(value),
        _ => Err(IvmRuntimeError::RecursiveEnumProjectionDescriptorMismatch {
            path: path.to_owned(),
        }),
    }
}

fn remap_path_with_enum_occurrence_below(remaps: &RecursiveEnumRemaps, path: &str) -> bool {
    let below = format!("{path}/");
    remaps
        .scalar
        .keys()
        .chain(remaps.payload.keys())
        .chain(remaps.payload_children.keys())
        .any(|candidate| candidate == path || candidate.starts_with(&below))
}

pub(super) fn projection_uses_raw_copy(
    expressions: &[ProjectionExpr],
    mapping: &[(usize, usize)],
    output_desc: RecordDescriptor,
) -> bool {
    if expressions.is_empty() {
        // Legacy/validation-only path: normal lowering always fills expressions.
        return mapping.len() == output_desc.fields().len();
    }
    expressions.len() == output_desc.fields().len()
        && mapping.len() == output_desc.fields().len()
        && expressions
            .iter()
            .all(|expr| matches!(expr.expression, PlanExpr::Field(_)))
}

pub(super) fn raw_projection_fields(
    project: &MapProjectOp,
    input_desc: &RecordDescriptor,
    output_desc: RecordDescriptor,
) -> Result<Option<Vec<RawProjectionField>>, IvmRuntimeError> {
    if project.expressions.is_empty() || project.expressions.len() != output_desc.fields().len() {
        return Ok(None);
    }

    let fields = project
        .expressions
        .iter()
        .map(|expr| match &expr.expression {
            PlanExpr::Field(field) => resolve_field_name(input_desc, field)
                .map(|source_idx| RawProjectionField::Copy { source_idx }),
            PlanExpr::Nullable(field) => resolve_field_name(input_desc, field)
                .map(|source_idx| RawProjectionField::WrapNullable { source_idx }),
            PlanExpr::NullableFlat(field) => resolve_field_name(input_desc, field)
                .map(|source_idx| RawProjectionField::FlattenNullable { source_idx }),
            PlanExpr::Null(_) => Some(RawProjectionField::Encoded {
                bytes: encode_projection_field_value(
                    output_desc,
                    expr.output_name.as_deref(),
                    Value::Nullable(None),
                )
                .ok()?,
            }),
            PlanExpr::Literal(value) => Some(RawProjectionField::Encoded {
                bytes: encode_projection_field_value(
                    output_desc,
                    expr.output_name.as_deref(),
                    value.to_value(),
                )
                .ok()?,
            }),
            PlanExpr::EnumTagRemap { .. }
            | PlanExpr::EnumRemap { .. }
            | PlanExpr::RecursiveEnumRemap { .. } => None,
        })
        .collect::<Option<Vec<_>>>();
    Ok(fields)
}

fn encode_projection_field_value(
    output_desc: RecordDescriptor,
    output_name: Option<&str>,
    value: Value,
) -> Result<Vec<u8>, IvmRuntimeError> {
    let field_idx = if let Some(output_name) = output_name {
        output_desc
            .field_index(output_name)
            .ok_or_else(|| IvmRuntimeError::GraphFieldNotFound(output_name.to_owned()))?
    } else {
        return Err(IvmRuntimeError::GraphFieldNotFound("<unnamed>".to_owned()));
    };
    let field = output_desc
        .fields()
        .get(field_idx)
        .ok_or(IvmRuntimeError::GraphFieldIndexOutOfBounds(field_idx))?;
    records::encode_single_field_value(&value, &field.value_type).map_err(Into::into)
}

pub(super) fn resolve_field_ref(
    descriptor: &RecordDescriptor,
    field: &FieldRef,
) -> Result<usize, IvmRuntimeError> {
    match field {
        FieldRef::Name(name) => resolve_field_name(descriptor, name)
            .ok_or_else(|| IvmRuntimeError::GraphFieldNotFound(name.clone())),
        FieldRef::Resolved(index) => {
            if *index < descriptor.fields().len() {
                Ok(*index)
            } else {
                Err(IvmRuntimeError::GraphFieldIndexOutOfBounds(*index))
            }
        }
    }
}

pub(super) fn resolve_field_name(descriptor: &RecordDescriptor, name: &str) -> Option<usize> {
    descriptor.field_index(name).or_else(|| {
        descriptor
            .fields()
            .iter()
            .position(|field| field.name.as_deref() == Some(name))
    })
}

pub(super) fn field_ref_name(
    descriptor: &RecordDescriptor,
    field: &FieldRef,
) -> Result<String, IvmRuntimeError> {
    match field {
        FieldRef::Name(name) => Ok(name.clone()),
        FieldRef::Resolved(index) => field_name_at(descriptor, *index),
    }
}

pub(super) fn field_name_at(
    descriptor: &RecordDescriptor,
    index: usize,
) -> Result<String, IvmRuntimeError> {
    let field = descriptor
        .fields()
        .get(index)
        .ok_or(IvmRuntimeError::GraphFieldIndexOutOfBounds(index))?;
    field
        .name
        .clone()
        .ok_or_else(|| IvmRuntimeError::GraphFieldNotFound(format!("#{index}")))
}

pub(super) fn unwrap_nullable_descriptor(
    input: &RecordDescriptor,
    field_idx: usize,
) -> Result<RecordDescriptor, IvmRuntimeError> {
    input
        .fields()
        .iter()
        .enumerate()
        .map(|(idx, field)| {
            let value_type = if idx == field_idx {
                match &field.value_type {
                    ValueType::Nullable(inner) => (**inner).clone(),
                    other => other.clone(),
                }
            } else {
                field.value_type.clone()
            };
            Ok(DescriptorField {
                name: field.name.clone(),
                identity: field.identity.clone(),
                value_type,
            })
        })
        .collect::<Result<Vec<_>, IvmRuntimeError>>()
        .map(RecordDescriptor::new_with_fields)
}

pub(super) fn unnest_descriptor(
    input: &RecordDescriptor,
    array_field_idx: usize,
    element_field: &str,
) -> Result<RecordDescriptor, IvmRuntimeError> {
    let array_field = input
        .fields()
        .get(array_field_idx)
        .ok_or(IvmRuntimeError::GraphFieldIndexOutOfBounds(array_field_idx))?;
    let ValueType::Array(element_type) = &array_field.value_type else {
        return Err(IvmRuntimeError::UnsupportedOperator);
    };
    let mut fields = input
        .fields()
        .iter()
        .map(|field| DescriptorField {
            name: field.name.clone(),
            identity: field.identity.clone(),
            value_type: field.value_type.clone(),
        })
        .collect::<Vec<_>>();
    fields.push(DescriptorField::new(
        element_field.to_owned(),
        (**element_type).clone(),
    ));
    Ok(RecordDescriptor::new_with_fields(fields))
}

pub(super) fn variant_project_descriptor(
    input: &RecordDescriptor,
    field: &FieldRef,
    case: &str,
) -> Result<RecordDescriptor, IvmRuntimeError> {
    let field_idx = resolve_field_ref(input, field)?;
    let enum_field = input
        .fields()
        .get(field_idx)
        .ok_or(IvmRuntimeError::GraphFieldIndexOutOfBounds(field_idx))?;
    let ValueType::Enum(schema) = &enum_field.value_type else {
        return Err(IvmRuntimeError::VariantProjectFieldTypeMismatch {
            field: field_ref_name(input, field)?,
        });
    };
    Ok(schema.case(schema.tag(case)?)?.payload)
}

pub(super) fn aggregate_descriptor(
    input: &RecordDescriptor,
    group_cols: &[FieldRef],
    aggregates: &[AggregateExpr],
) -> Result<RecordDescriptor, IvmRuntimeError> {
    let mut fields = Vec::new();
    for group_col in group_cols {
        let field_idx = resolve_field_ref(input, group_col)?;
        let field = input
            .fields()
            .get(field_idx)
            .ok_or(IvmRuntimeError::GraphFieldIndexOutOfBounds(field_idx))?;
        fields.push(
            DescriptorField::new(field_ref_name(input, group_col)?, field.value_type.clone())
                .with_identity(field.identity.clone().unwrap_or_else(|| {
                    crate::records::FieldIdentity::Name(
                        field
                            .name
                            .clone()
                            .unwrap_or_else(|| field_ref_name(input, group_col).unwrap()),
                    )
                })),
        );
    }
    for (index, aggregate) in aggregates.iter().enumerate() {
        let name = aggregate
            .output_name
            .clone()
            .unwrap_or_else(|| format!("aggregate_{index}"));
        fields.push(
            DescriptorField::new(name.clone(), aggregate_output_type(input, aggregate)?)
                .with_identity(
                    aggregate
                        .output_identity
                        .clone()
                        .unwrap_or(crate::records::FieldIdentity::Name(name)),
                ),
        );
    }
    Ok(RecordDescriptor::new_with_fields(fields))
}

fn aggregate_output_type(
    input: &RecordDescriptor,
    aggregate: &AggregateExpr,
) -> Result<ValueType, IvmRuntimeError> {
    Ok(match aggregate.function {
        AggregateFunction::Count => ValueType::U64,
        AggregateFunction::Avg => ValueType::Nullable(Box::new(ValueType::F64)),
        AggregateFunction::Sum => {
            let value_type = aggregate_expr_value_type(input, aggregate)?;
            match non_nullable_type(&value_type) {
                ValueType::U8
                | ValueType::U16
                | ValueType::U32
                | ValueType::U64
                | ValueType::I32
                | ValueType::I64
                | ValueType::F64 => nullable_type(&value_type),
                _ => return Err(IvmRuntimeError::UnsupportedOperator),
            }
        }
        AggregateFunction::Min | AggregateFunction::Max => {
            let value_type = aggregate_expr_value_type(input, aggregate)?;
            nullable_type(&value_type)
        }
    })
}

fn aggregate_expr_value_type(
    input: &RecordDescriptor,
    aggregate: &AggregateExpr,
) -> Result<ValueType, IvmRuntimeError> {
    let Some(expr) = &aggregate.expression else {
        return Err(IvmRuntimeError::UnsupportedOperator);
    };
    match expr {
        PlanExpr::Field(field)
        | PlanExpr::Nullable(field)
        | PlanExpr::NullableFlat(field)
        | PlanExpr::EnumTagRemap { field, .. }
        | PlanExpr::EnumRemap { field, .. }
        | PlanExpr::RecursiveEnumRemap { field, .. } => {
            let field_idx = resolve_field_name(input, field)
                .ok_or_else(|| IvmRuntimeError::GraphFieldNotFound(field.clone()))?;
            Ok(input
                .fields()
                .get(field_idx)
                .ok_or(IvmRuntimeError::GraphFieldIndexOutOfBounds(field_idx))?
                .value_type
                .clone())
        }
        PlanExpr::Literal(literal) => literal
            .value_type()
            .ok_or(IvmRuntimeError::UnsupportedOperator),
        PlanExpr::Null(value_type) => Ok(value_type.clone()),
    }
}

fn non_nullable_type(value_type: &ValueType) -> &ValueType {
    match value_type {
        ValueType::Nullable(inner) => inner,
        other => other,
    }
}

/// Used for converting non-nullable column types to nullable column types when aggregating.
fn nullable_type(value_type: &ValueType) -> ValueType {
    ValueType::Nullable(Box::new(non_nullable_type(value_type).clone()))
}

pub(super) fn index_record_descriptor() -> RecordDescriptor {
    static DESCRIPTOR: std::sync::OnceLock<RecordDescriptor> = std::sync::OnceLock::new();
    *DESCRIPTOR.get_or_init(|| {
        RecordDescriptor::new([("key", ValueType::Bytes), ("value", ValueType::Bytes)])
    })
}

pub(super) fn variant_projection_target_name(target: &VariantProjectionTarget) -> &str {
    match target {
        VariantProjectionTarget::Named(name) | VariantProjectionTarget::SchemaIndex(name) => name,
    }
}

pub(super) fn schema_index_input_fields(
    table: &TableSchema,
    index: &IndexSchema,
) -> Result<Vec<String>, IvmRuntimeError> {
    let primary_key = table
        .primary_key
        .as_ref()
        .ok_or_else(|| IvmRuntimeError::MissingPrimaryKey(table.name.clone()))?;
    let catalogue = table.record_schema();
    let mut fields = Vec::new();
    for field in index
        .columns
        .iter()
        .chain(primary_key.columns.iter().map(|column| &column.column))
    {
        if catalogue.field_index(field).is_none() {
            return Err(IvmRuntimeError::GraphFieldNotFound(field.clone()));
        }
        if !fields.contains(field) {
            fields.push(field.clone());
        }
    }
    Ok(fields)
}

pub(super) fn schema_index_input_descriptor(
    table: &TableSchema,
    index: &IndexSchema,
) -> Result<RecordDescriptor, IvmRuntimeError> {
    let catalogue = table.record_schema();
    let fields = schema_index_input_fields(table, index)?
        .into_iter()
        .map(ProjectField::named)
        .collect::<Vec<_>>();
    project_descriptor(&catalogue, &fields)
}

pub(super) fn apply_index_by(
    index_by: &IndexByOp,
    input_descriptor: &RecordDescriptor,
    input_deltas: &[RecordDelta],
) -> Result<Vec<RecordDelta>, IvmRuntimeError> {
    let mut deltas = Vec::new();
    let scalar_key_fields = index_key_fields_are_scalar(index_by, input_descriptor)?;
    for delta in input_deltas {
        let value = if index_by.store_value {
            primary_key_value_bytes(input_descriptor, delta.raw(), &index_by.value_fields)?
        } else {
            Vec::new()
        };
        if scalar_key_fields {
            let key = scalar_index_key(index_by, input_descriptor, delta.raw())?;
            if let Some(scan) = &index_by.scan
                && !key_matches_static_scan(&key, scan)?
            {
                continue;
            }
            deltas.push(RecordDelta {
                record: index_record_descriptor()
                    .create(&[Value::Bytes(key), Value::Bytes(value)])?
                    .into(),
                weight: delta.weight,
            });
            continue;
        }

        let keys = index_keys(index_by, input_descriptor, delta.raw())?;
        for key in keys {
            if let Some(scan) = &index_by.scan
                && !key_matches_static_scan(&key, scan)?
            {
                continue;
            }
            deltas.push(RecordDelta {
                record: index_record_descriptor()
                    .create(&[Value::Bytes(key), Value::Bytes(value.clone())])?
                    .into(),
                weight: delta.weight,
            });
        }
    }
    Ok(deltas)
}

fn index_key_fields_are_scalar(
    index_by: &IndexByOp,
    input_descriptor: &RecordDescriptor,
) -> Result<bool, IvmRuntimeError> {
    for field_idx in &index_by.key_fields {
        let field = input_descriptor
            .fields()
            .get(*field_idx)
            .ok_or(IvmRuntimeError::GraphFieldIndexOutOfBounds(*field_idx))?;
        match &field.value_type {
            ValueType::Array(_) => return Ok(false),
            ValueType::Nullable(inner) if matches!(inner.as_ref(), ValueType::Array(_)) => {
                return Ok(false);
            }
            _ => {}
        }
    }
    Ok(true)
}

fn scalar_index_key(
    index_by: &IndexByOp,
    input_descriptor: &RecordDescriptor,
    record: &[u8],
) -> Result<Vec<u8>, IvmRuntimeError> {
    let mut key = Vec::new();
    for field_idx in &index_by.key_fields {
        encode_record_field_key_part(&mut key, input_descriptor, record, *field_idx)?;
    }
    if index_by.append_value_to_key {
        let value = primary_key_value_bytes(input_descriptor, record, &index_by.value_fields)?;
        key.push(0xff);
        key.extend(value);
    }
    Ok(key)
}

fn index_keys(
    index_by: &IndexByOp,
    input_descriptor: &RecordDescriptor,
    record: &[u8],
) -> Result<Vec<Vec<u8>>, IvmRuntimeError> {
    let mut keys = vec![Vec::new()];
    let mut seen = HashSet::new();

    for field_idx in &index_by.key_fields {
        let parts = record_field_key_parts(input_descriptor, record, *field_idx)?;
        if parts.is_empty() {
            return Ok(Vec::new());
        }

        let mut next_keys = Vec::with_capacity(keys.len() * parts.len());
        for key in &keys {
            for part in &parts {
                let mut next = key.clone();
                next.extend(part);
                if seen.insert(next.clone()) {
                    next_keys.push(next);
                }
            }
        }
        keys = next_keys;
        seen.clear();
    }
    if index_by.append_value_to_key {
        let value = primary_key_value_bytes(input_descriptor, record, &index_by.value_fields)?;
        // Non-unique indices append the primary key so equal index values remain
        // distinct and ordered for range scans.
        for key in &mut keys {
            key.push(0xff);
            key.extend(&value);
        }
    }
    Ok(keys)
}

pub(super) enum StaticScanBounds {
    Prefix(Vec<u8>),
    Range { start: Vec<u8>, end: Vec<u8> },
}

pub(super) fn scan_bounds(scan: &StaticScanSpec) -> Result<StaticScanBounds, IvmRuntimeError> {
    match scan {
        StaticScanSpec::Point(values)
        | StaticScanSpec::Prefix(values)
        | StaticScanSpec::PrefixLimit { prefix: values, .. } => {
            Ok(StaticScanBounds::Prefix(static_scan_key(values)?))
        }
        StaticScanSpec::Range { start, end } => Ok(StaticScanBounds::Range {
            start: static_scan_key(start)?,
            end: static_scan_key(end)?,
        }),
    }
}

pub(super) fn scan_max_items(scan: Option<&StaticScanSpec>) -> Option<usize> {
    match scan {
        Some(StaticScanSpec::PrefixLimit { max_items, .. }) => Some(*max_items),
        _ => None,
    }
}

fn static_scan_key(values: &[LiteralValue]) -> Result<Vec<u8>, IvmRuntimeError> {
    let mut key = Vec::new();
    for value in values {
        encode_key_part(&mut key, &value.to_value())?;
    }
    Ok(key)
}

pub(super) fn key_matches_static_scan(
    key: &[u8],
    scan: &StaticScanSpec,
) -> Result<bool, IvmRuntimeError> {
    Ok(match scan_bounds(scan)? {
        StaticScanBounds::Prefix(prefix) => key.starts_with(&prefix),
        StaticScanBounds::Range { start, end } => start.as_slice() <= key && key < end.as_slice(),
    })
}

pub(super) fn persisted_index_scan_bounds(
    table: &str,
    index: &str,
    scan: Option<&StaticScanSpec>,
) -> Result<StaticScanBounds, IvmRuntimeError> {
    let base = durable_index_key_prefix(table, index);
    let wrap_prefix = |logical_key: Vec<u8>| {
        let mut storage_key = base.clone();
        if !logical_key.is_empty() {
            storage_key.push(7);
            encode_ordered_bytes_without_terminal(&mut storage_key, &logical_key);
        }
        storage_key
    };
    Ok(match scan {
        None => StaticScanBounds::Prefix(base),
        Some(
            StaticScanSpec::Point(values)
            | StaticScanSpec::Prefix(values)
            | StaticScanSpec::PrefixLimit { prefix: values, .. },
        ) => StaticScanBounds::Prefix(wrap_prefix(static_scan_key(values)?)),
        Some(StaticScanSpec::Range { start, end }) => StaticScanBounds::Range {
            start: wrap_prefix(static_scan_key(start)?),
            end: wrap_prefix(static_scan_key(end)?),
        },
    })
}
