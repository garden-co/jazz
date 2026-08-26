//! Arg-by, TopBy, and structured CollectBy window maintenance.

use super::*;

type SourceRecord = (Vec<u8>, Bytes);
pub(super) type WindowedRecord = (Bytes, i64);

#[derive(Clone, Debug)]
pub(super) struct TopBySortPart {
    pub(super) key: Value,
    pub(super) direction: TopByDirection,
}

impl PartialEq for TopBySortPart {
    fn eq(&self, other: &Self) -> bool {
        self.direction == other.direction && self.cmp(other) == std::cmp::Ordering::Equal
    }
}

impl Eq for TopBySortPart {}

impl Ord for TopBySortPart {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        let ordering = match (is_sql_null_value(&self.key), is_sql_null_value(&other.key)) {
            // Windows need a total order, unlike SQL predicates where any
            // comparison involving NULL is unknown. Keep NULL first for the
            // canonical ascending order, then apply the declared direction.
            (true, true) => std::cmp::Ordering::Equal,
            (true, false) => std::cmp::Ordering::Less,
            (false, true) => std::cmp::Ordering::Greater,
            (false, false) => compare_values_sql(&self.key, &other.key, ValueComparison::Exact)
                .expect("non-null TopBy values must be comparable"),
        };
        match self.direction {
            TopByDirection::Asc => ordering,
            TopByDirection::Desc => ordering.reverse(),
        }
    }
}

impl PartialOrd for TopBySortPart {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

#[derive(Clone, Copy)]
pub(super) enum ArgByDirection {
    Min,
    Max,
}

pub(super) struct ArgBySpec<'a> {
    pub(super) group_fields: &'a [String],
    pub(super) group_field_indices: &'a [usize],
    pub(super) primary_key_field_indices: &'a [usize],
    pub(super) direction: ArgByDirection,
}

pub(super) fn arg_by_winner_from_records(
    descriptor: RecordDescriptor,
    primary_key_field_indices: &[usize],
    records: Vec<(Bytes, i64)>,
    direction: ArgByDirection,
) -> Result<Option<SourceRecord>, IvmRuntimeError> {
    let mut winner = None;
    for (record, weight) in records {
        if weight <= 0 {
            continue;
        }
        let key = encoded_record_key_part(descriptor, &record, primary_key_field_indices)?;
        let replaces =
            winner
                .as_ref()
                .is_none_or(|(winner_key, _): &SourceRecord| match direction {
                    ArgByDirection::Min => key < *winner_key,
                    ArgByDirection::Max => key > *winner_key,
                });
        if replaces {
            winner = Some((key, record));
        }
    }
    Ok(winner)
}

pub(super) fn arg_by_winner_before_from_deltas(
    descriptor: RecordDescriptor,
    primary_key_field_indices: &[usize],
    after_records: Vec<(Bytes, i64)>,
    deltas: Vec<RecordDelta>,
    direction: ArgByDirection,
) -> Result<Option<SourceRecord>, IvmRuntimeError> {
    let mut records = BTreeMap::<Vec<u8>, (Bytes, i64)>::new();
    for (record, weight) in after_records {
        let key = encoded_record_key_part(descriptor, &record, primary_key_field_indices)?;
        records.insert(key, (record, weight));
    }
    for delta in deltas {
        let key = encoded_record_key_part(descriptor, delta.raw(), primary_key_field_indices)?;
        let entry = records
            .entry(key)
            .or_insert_with(|| (delta.record.clone(), 0));
        entry.1 -= delta.weight;
    }
    let mut positive = records
        .into_iter()
        .filter_map(|(key, (record, weight))| (weight > 0).then_some((key, record)));
    Ok(match direction {
        ArgByDirection::Min => positive.next(),
        ArgByDirection::Max => positive.next_back(),
    })
}

pub(super) fn top_by_window_from_records(
    descriptor: RecordDescriptor,
    records: Vec<(Bytes, i64)>,
    top_by: &TopByOp,
) -> Result<Vec<WindowedRecord>, IvmRuntimeError> {
    let mut ranked = Vec::new();
    for (record, weight) in records {
        if weight > 0 {
            ranked.push((
                top_by_sort_key(descriptor, &record, top_by)?,
                record,
                weight,
            ));
        }
    }
    // Full record bytes are the final tie-breaker (INV-QUERY-23); the total
    // order must not depend on arrangement iteration order.
    ranked.sort_by(|left, right| left.0.cmp(&right.0).then_with(|| left.1.cmp(&right.1)));

    // Bag semantics (INV-QUERY-24/25): a record with multiplicity m occupies m
    // ordinals, and offset/limit consume copies, not distinct records.
    let mut window = Vec::new();
    let mut to_skip = top_by.offset;
    let mut remaining = match top_by.limit {
        TopByLimit::Finite(limit) => Some(limit),
        TopByLimit::Unbounded => None,
    };
    for (_, record, weight) in ranked {
        if remaining == Some(0) {
            break;
        }
        let copies = weight as u64;
        let available = copies.saturating_sub(to_skip);
        to_skip = to_skip.saturating_sub(copies);
        let taken = remaining.map_or(available, |remaining| available.min(remaining));
        if taken > 0 {
            window.push((
                record,
                i64::try_from(taken).expect("taken copies cannot exceed positive record weight"),
            ));
            if let Some(remaining) = &mut remaining {
                *remaining -= taken;
            }
        }
    }
    Ok(window)
}

pub(super) fn top_by_window_before_from_deltas(
    descriptor: RecordDescriptor,
    after_records: Vec<(Bytes, i64)>,
    deltas: Vec<RecordDelta>,
    top_by: &TopByOp,
) -> Result<Vec<WindowedRecord>, IvmRuntimeError> {
    // Reconstruct the pre-tick multiset keyed by record bytes — the same
    // identity the arrangement consolidates by. Keying by sort key would
    // collapse distinct records that tie through (order_cols, tie_cols).
    let mut records = BTreeMap::<Bytes, i64>::new();
    for (record, weight) in after_records {
        *records.entry(record).or_default() += weight;
    }
    for delta in deltas {
        *records.entry(delta.record.clone()).or_default() -= delta.weight;
    }
    top_by_window_from_records(descriptor, records.into_iter().collect(), top_by)
}

pub(super) fn records_before_deltas(
    after_records: Vec<(Bytes, i64)>,
    deltas: &[RecordDelta],
) -> Vec<(Bytes, i64)> {
    let mut records = BTreeMap::<Bytes, i64>::new();
    for (record, weight) in after_records {
        *records.entry(record).or_default() += weight;
    }
    for delta in deltas {
        *records.entry(delta.record.clone()).or_default() -= delta.weight;
    }
    records.into_iter().collect()
}

fn collect_by_projection_value_type(
    input_desc: RecordDescriptor,
    field: &CollectByProjection,
) -> Result<ValueType, IvmRuntimeError> {
    let value_type = input_desc
        .fields()
        .get(field.field_idx)
        .ok_or(IvmRuntimeError::GraphFieldIndexOutOfBounds(field.field_idx))?
        .value_type
        .clone();
    if !field.unwrap_nullable {
        return Ok(value_type);
    }
    Ok(match value_type {
        ValueType::Nullable(inner) => *inner,
        value_type => value_type,
    })
}

pub(super) fn collect_by_output_value(output_type: &ValueType, value: Value) -> Value {
    match (output_type, value) {
        (ValueType::Nullable(_), value @ Value::Nullable(_)) => value,
        (ValueType::Nullable(_), value) => Value::Nullable(Some(Box::new(value))),
        (_, value) => value,
    }
}

fn collect_by_projected_value(
    values: &[Value],
    field: &CollectByProjection,
) -> Result<Value, IvmRuntimeError> {
    let value = values
        .get(field.field_idx)
        .cloned()
        .ok_or(IvmRuntimeError::GraphFieldIndexOutOfBounds(field.field_idx))?;
    if !field.unwrap_nullable {
        return Ok(value);
    }
    match value {
        Value::Nullable(Some(value)) => Ok(*value),
        // A present child can legitimately carry an application NULL. Keep
        // it saturated at NULL; descriptor validation will reject this value
        // when the requested output field is non-nullable.
        Value::Nullable(None) => Ok(Value::Nullable(None)),
        value => Ok(value),
    }
}

pub(super) fn update_unbounded_collect_by_terminal_state(
    input_desc: RecordDescriptor,
    output_desc: RecordDescriptor,
    collect_by: &CollectByOp,
    direct_tree_slot: Option<&CollectBySlot>,
    state: &mut CollectByIncrementalState,
    deltas: &[RecordDelta],
    emit: bool,
) -> Result<Vec<TerminalOperation>, IvmRuntimeError> {
    if collect_by.mode == CollectByMode::Root {
        return update_collect_by_root_terminal_state(
            input_desc,
            output_desc,
            collect_by,
            state,
            deltas,
            emit,
        );
    }
    if !emit {
        state.groups.clear();
        state.roots.clear();
    }
    // A weighted update arrives as a post-image addition and pre-image
    // removal. Consolidate exact records and canonicalize every surviving
    // transition before emitting terminal edits: net removals first, then net
    // additions. This makes replacement (`-before, +after`) unambiguous to
    // consumers and cancels transient insert/delete pairs within one batch.
    let deltas =
        canonical_collect_by_terminal_deltas(input_desc, collect_by, direct_tree_slot, deltas)?;
    let root_groups_before = direct_tree_slot
        .is_none()
        .then(|| state.groups.keys().cloned().collect::<BTreeSet<_>>());
    let mut operations = Vec::new();
    for delta in &deltas {
        let group_key =
            encoded_record_key_part(input_desc, delta.raw(), &collect_by.group_field_indices)?;
        if let Some(presence_field) = direct_tree_slot.and_then(|slot| slot.presence_field_index)
            && !BorrowedRecord::new(delta.raw(), &input_desc).get_bool(presence_field)?
        {
            let state_key = (
                collect_by_sort_key(input_desc, delta.raw(), collect_by)?,
                delta.record.clone(),
            );
            let before_weight = state.roots.get(&state_key).copied().unwrap_or_default();
            let after_weight = before_weight + delta.weight;
            if after_weight == 0 {
                state.roots.remove(&state_key);
            } else {
                state.roots.insert(state_key.clone(), after_weight);
            }
            if !emit || (before_weight > 0) == (after_weight > 0) {
                continue;
            }
            if after_weight > 0 {
                let index = state
                    .roots
                    .range(..state_key)
                    .filter(|(_, weight)| **weight > 0)
                    .count();
                let record = collect_by_tree_parent_from_records(
                    input_desc,
                    output_desc,
                    collect_by,
                    &[(delta.record.clone(), 1)],
                )?
                .ok_or_else(|| {
                    IvmRuntimeError::InvalidCollectBy(
                        "root anchor did not render a terminal row".to_owned(),
                    )
                })?;
                operations.push(TerminalOperation {
                    root_descriptor: output_desc,
                    root_key: group_key.clone(),
                    path: Vec::new(),
                    edit: TerminalEdit::Insert {
                        index,
                        key: group_key,
                        value: record.to_vec(),
                    },
                });
            } else {
                operations.push(TerminalOperation {
                    root_descriptor: output_desc,
                    root_key: group_key.clone(),
                    path: Vec::new(),
                    edit: TerminalEdit::Remove { key: group_key },
                });
            }
            continue;
        }
        let (sort_field_indices, sort_directions) = direct_tree_slot.map_or(
            (
                collect_by.sort_field_indices.as_slice(),
                collect_by.sort_directions.as_slice(),
            ),
            |slot| {
                (
                    slot.sort_field_indices.as_slice(),
                    slot.sort_directions.as_slice(),
                )
            },
        );
        let sort_key = collect_by_sort_key_for_fields(
            input_desc,
            delta.raw(),
            sort_field_indices,
            sort_directions,
        )?;
        let state_key = (sort_key, delta.record.clone());
        let group = state.groups.entry(group_key.clone()).or_default();
        let before_weight = group.get(&state_key).copied().unwrap_or_default();
        let before_index = (before_weight > 0).then(|| {
            group
                .range(..state_key.clone())
                .filter(|(_, weight)| **weight > 0)
                .count()
        });
        let after_weight = before_weight + delta.weight;
        if after_weight == 0 {
            group.remove(&state_key);
        } else {
            group.insert(state_key.clone(), after_weight);
        }
        if !emit || (before_weight > 0) == (after_weight > 0) {
            continue;
        }
        let source_values = BorrowedRecord::new(delta.raw(), &input_desc)
            .to_values()
            .map_err(IvmRuntimeError::RecordEncoding)?;
        let child_fields = direct_tree_slot
            .map(|slot| slot.child_fields.as_slice())
            .unwrap_or(collect_by.child_fields.as_slice());
        let child_descriptor = direct_tree_slot
            .map(|slot| slot.child_descriptor)
            .unwrap_or(collect_by.child_descriptor);
        let collection_field = direct_tree_slot
            .map(|slot| slot.collection_field.as_str())
            .unwrap_or(collect_by.collection_field.as_str());
        let child_values = child_fields
            .iter()
            .enumerate()
            .map(|(index, field)| {
                Ok::<Value, IvmRuntimeError>(collect_by_output_value(
                    &child_descriptor.fields()[index].value_type,
                    collect_by_projected_value(&source_values, field)?,
                ))
            })
            .collect::<Result<Vec<_>, _>>()?;
        let child_record: Vec<u8> = child_descriptor.create(&child_values)?;
        let child_key = encoded_record_key_part(child_descriptor, &child_record, &[0])?;
        let path = vec![TerminalPathSegment::Collection(collection_field.to_owned())];
        if after_weight > 0 {
            let index = group
                .range(..state_key)
                .filter(|(_, weight)| **weight > 0)
                .count();
            operations.push(TerminalOperation {
                root_descriptor: output_desc,
                root_key: group_key,
                path,
                edit: TerminalEdit::Insert {
                    index,
                    key: child_key,
                    value: child_record,
                },
            });
        } else {
            operations.push(TerminalOperation {
                root_descriptor: output_desc,
                root_key: group_key,
                path,
                edit: TerminalEdit::Remove { key: child_key },
            });
            debug_assert!(before_index.is_some());
        }
    }
    state.groups.retain(|_, group| !group.is_empty());
    if let Some(root_groups_before) = root_groups_before {
        let root_groups_after = state.groups.keys().cloned().collect::<BTreeSet<_>>();
        operations.retain(|operation| {
            root_groups_before.contains(&operation.root_key)
                && root_groups_after.contains(&operation.root_key)
        });
        for root_key in root_groups_before.difference(&root_groups_after) {
            operations.push(TerminalOperation {
                root_descriptor: output_desc,
                root_key: root_key.clone(),
                path: Vec::new(),
                edit: TerminalEdit::Remove {
                    key: root_key.clone(),
                },
            });
        }
        for root_key in root_groups_after.difference(&root_groups_before) {
            let group = state
                .groups
                .get(root_key)
                .expect("root key came from collect state");
            let records = group
                .iter()
                .map(|((_, record), weight)| (record.clone(), *weight))
                .collect::<Vec<_>>();
            let record =
                collect_by_parent_from_records(input_desc, output_desc, collect_by, &records)?
                    .ok_or_else(|| {
                        IvmRuntimeError::InvalidCollectBy(
                            "new collect root did not render a terminal row".to_owned(),
                        )
                    })?;
            let index = root_groups_after.range(..root_key.clone()).count();
            operations.push(TerminalOperation {
                root_descriptor: output_desc,
                root_key: root_key.clone(),
                path: Vec::new(),
                edit: TerminalEdit::Insert {
                    index,
                    key: root_key.clone(),
                    value: record.to_vec(),
                },
            });
        }
    }
    Ok(operations)
}

fn canonical_collect_by_terminal_deltas(
    input_desc: RecordDescriptor,
    collect_by: &CollectByOp,
    direct_tree_slot: Option<&CollectBySlot>,
    deltas: &[RecordDelta],
) -> Result<Vec<RecordDelta>, IvmRuntimeError> {
    let (sort_field_indices, sort_directions) = direct_tree_slot.map_or(
        (
            collect_by.sort_field_indices.as_slice(),
            collect_by.sort_directions.as_slice(),
        ),
        |slot| {
            (
                slot.sort_field_indices.as_slice(),
                slot.sort_directions.as_slice(),
            )
        },
    );
    let keyed = deltas
        .iter()
        .map(|delta| {
            Ok::<_, IvmRuntimeError>((
                encoded_record_key_part(input_desc, delta.raw(), &collect_by.group_field_indices)?,
                collect_by_sort_key_for_fields(
                    input_desc,
                    delta.raw(),
                    sort_field_indices,
                    sort_directions,
                )?,
                delta.record.clone(),
                delta.weight,
            ))
        })
        .collect::<Result<Vec<_>, _>>()?;
    Ok(canonicalize_collect_by_terminal_weights(keyed)
        .into_iter()
        .map(|(_, _, record, weight)| RecordDelta { record, weight })
        .collect())
}

pub(super) fn canonicalize_collect_by_terminal_weights(
    keyed: Vec<(Vec<u8>, Vec<TopBySortPart>, Bytes, i64)>,
) -> Vec<(Vec<u8>, Vec<TopBySortPart>, Bytes, i64)> {
    let mut consolidated = BTreeMap::<(Vec<u8>, Vec<TopBySortPart>, Bytes), i64>::new();
    for (group_key, sort_key, record, weight) in keyed {
        *consolidated
            .entry((group_key, sort_key, record))
            .or_default() += weight;
    }
    let mut canonical = consolidated
        .into_iter()
        .filter_map(|((group_key, sort_key, record), weight)| {
            (weight != 0).then_some((group_key, sort_key, record, weight))
        })
        .collect::<Vec<_>>();
    canonical.sort_by(
        |(left_group, left_sort, _, left_weight), (right_group, right_sort, _, right_weight)| {
            left_group
                .cmp(right_group)
                .then_with(|| left_weight.is_positive().cmp(&right_weight.is_positive()))
                .then_with(|| left_sort.cmp(right_sort))
        },
    );
    canonical
}

fn update_collect_by_root_terminal_state(
    input_desc: RecordDescriptor,
    output_desc: RecordDescriptor,
    collect_by: &CollectByOp,
    state: &mut CollectByIncrementalState,
    deltas: &[RecordDelta],
    emit: bool,
) -> Result<Vec<TerminalOperation>, IvmRuntimeError> {
    if !emit {
        state.groups.clear();
        state.roots.clear();
    }
    let mut before = BTreeMap::<Vec<u8>, Option<Bytes>>::new();
    for delta in deltas {
        let group_key =
            encoded_record_key_part(input_desc, delta.raw(), &collect_by.group_field_indices)?;
        if emit && !before.contains_key(&group_key) {
            let rendered = state.groups.get(&group_key).map_or(Ok(None), |group| {
                let records = group
                    .iter()
                    .map(|((_, record), weight)| (record.clone(), *weight))
                    .collect::<Vec<_>>();
                collect_by_root_from_records(input_desc, output_desc, collect_by, &records)
            })?;
            before.insert(group_key.clone(), rendered);
        }
        let sort_key = collect_by_sort_key(input_desc, delta.raw(), collect_by)?;
        let state_key = (sort_key, delta.record.clone());
        let group = state.groups.entry(group_key).or_default();
        let weight = group.get(&state_key).copied().unwrap_or_default() + delta.weight;
        if weight == 0 {
            group.remove(&state_key);
        } else {
            group.insert(state_key, weight);
        }
    }
    state.groups.retain(|_, group| !group.is_empty());
    if !emit {
        return Ok(Vec::new());
    }

    let mut operations = Vec::new();
    for (root_key, before_record) in before {
        let after_record = state.groups.get(&root_key).map_or(Ok(None), |group| {
            let records = group
                .iter()
                .map(|((_, record), weight)| (record.clone(), *weight))
                .collect::<Vec<_>>();
            collect_by_root_from_records(input_desc, output_desc, collect_by, &records)
        })?;
        if before_record == after_record {
            continue;
        }
        let edit = match (before_record, after_record) {
            (None, Some(record)) => TerminalEdit::Insert {
                index: state
                    .groups
                    .keys()
                    .take_while(|key| *key < &root_key)
                    .count(),
                key: root_key.clone(),
                value: record.to_vec(),
            },
            (Some(_), Some(record)) => TerminalEdit::Update {
                key: root_key.clone(),
                value: record.to_vec(),
            },
            (Some(_), None) => TerminalEdit::Remove {
                key: root_key.clone(),
            },
            (None, None) => continue,
        };
        operations.push(TerminalOperation {
            root_descriptor: output_desc,
            root_key,
            path: Vec::new(),
            edit,
        });
    }
    Ok(operations)
}

pub(super) fn collect_by_root_from_records(
    input_desc: RecordDescriptor,
    output_desc: RecordDescriptor,
    collect_by: &CollectByOp,
    records: &[(Bytes, i64)],
) -> Result<Option<Bytes>, IvmRuntimeError> {
    let Some((parent_record, _)) = records.iter().find(|(_, weight)| *weight > 0) else {
        return Ok(None);
    };
    let parent_values = BorrowedRecord::new(parent_record, &input_desc)
        .to_values()
        .map_err(|error| {
            IvmRuntimeError::InvalidCollectBy(format!("root input decode failed: {error}"))
        })?;
    let values = collect_by
        .parent_fields
        .iter()
        .enumerate()
        .map(|(index, field)| {
            let value = collect_by_projected_value(&parent_values, field)?;
            let output_type = &output_desc.fields()[index].value_type;
            Ok::<Value, IvmRuntimeError>(collect_by_output_value(output_type, value))
        })
        .collect::<Result<Vec<_>, _>>()?;
    let record = output_desc.create(&values).map_err(|error| {
        IvmRuntimeError::InvalidCollectBy(format!("root record render failed: {error}"))
    })?;
    Ok(Some(record.into()))
}

pub(super) fn collect_by_parent_from_records(
    input_desc: RecordDescriptor,
    output_desc: RecordDescriptor,
    collect_by: &CollectByOp,
    records: &[(Bytes, i64)],
) -> Result<Option<Bytes>, IvmRuntimeError> {
    let Some((parent_record, _)) = records.iter().find(|(_, weight)| *weight > 0) else {
        return Ok(None);
    };
    let parent_values = BorrowedRecord::new(parent_record, &input_desc)
        .to_values()
        .map_err(IvmRuntimeError::RecordEncoding)?;
    let mut values = collect_by
        .parent_fields
        .iter()
        .enumerate()
        .map(|(index, field)| {
            let value = collect_by_projected_value(&parent_values, field)?;
            let output_type = &output_desc.fields()[index].value_type;
            Ok::<Value, IvmRuntimeError>(collect_by_output_value(output_type, value))
        })
        .collect::<Result<Vec<_>, _>>()?;
    let window = collect_by_window_from_records(input_desc, records, collect_by)?;
    let mut children = Vec::new();
    for (record, copies) in window {
        let source_values = BorrowedRecord::new(&record, &input_desc)
            .to_values()
            .map_err(IvmRuntimeError::RecordEncoding)?;
        let child_values = collect_by
            .child_fields
            .iter()
            .enumerate()
            .map(|(index, field)| {
                Ok::<Value, IvmRuntimeError>(collect_by_output_value(
                    &collect_by.child_descriptor.fields()[index].value_type,
                    collect_by_projected_value(&source_values, field)?,
                ))
            })
            .collect::<Result<Vec<_>, _>>()?;
        let child = OwnedRecord::new(
            collect_by.child_descriptor.create(&child_values)?,
            collect_by.child_descriptor,
        );
        for _ in 0..copies {
            children.push(Value::Record(child.clone()));
        }
    }
    values.push(Value::Array(children));
    Ok(Some(output_desc.create(&values)?.into()))
}

pub(super) fn collect_by_tree_parent_from_records(
    input_desc: RecordDescriptor,
    output_desc: RecordDescriptor,
    collect_by: &CollectByOp,
    records: &[(Bytes, i64)],
) -> Result<Option<Bytes>, IvmRuntimeError> {
    let Some((parent_record, _)) = records.iter().find(|(_, weight)| *weight > 0) else {
        return Ok(None);
    };
    let parent_values = BorrowedRecord::new(parent_record, &input_desc)
        .to_values()
        .map_err(IvmRuntimeError::RecordEncoding)?;
    let mut values = collect_by
        .parent_fields
        .iter()
        .enumerate()
        .map(|(index, field)| {
            Ok::<Value, IvmRuntimeError>(collect_by_output_value(
                &output_desc.fields()[index].value_type,
                collect_by_projected_value(&parent_values, field)?,
            ))
        })
        .collect::<Result<Vec<_>, _>>()?;
    values.extend(render_collect_by_slots(
        input_desc,
        records,
        parent_record,
        &collect_by.slots,
    )?);
    Ok(Some(output_desc.create(&values)?.into()))
}

fn render_collect_by_slots(
    input_desc: RecordDescriptor,
    records: &[(Bytes, i64)],
    owner_record: &Bytes,
    slots: &[CollectBySlot],
) -> Result<Vec<Value>, IvmRuntimeError> {
    slots
        .iter()
        .map(|slot| {
            if slot.limit == TopByLimit::Finite(0) {
                return Ok(Value::Array(Vec::new()));
            }
            let owner_key =
                encoded_record_key_part(input_desc, owner_record, &slot.group_field_indices)?;
            // A nested flat association can repeat its owning child once for
            // every grandchild. Collapse those repetitions by the rendered
            // child projection before applying this slot's order/window.
            let child_key_fields = slot
                .child_fields
                .iter()
                .map(|field| {
                    Ok((
                        field.output_name.clone(),
                        collect_by_projection_value_type(input_desc, field)?,
                    ))
                })
                .collect::<Result<Vec<_>, IvmRuntimeError>>()?;
            let child_key_descriptor = RecordDescriptor::new(child_key_fields);
            let mut candidates = BTreeMap::<Bytes, Bytes>::new();
            for (record, weight) in records {
                if *weight <= 0
                    || encoded_record_key_part(input_desc, record, &slot.group_field_indices)?
                        != owner_key
                {
                    continue;
                }
                if let Some(presence_field_index) = slot.presence_field_index
                    && !BorrowedRecord::new(record, &input_desc).get_bool(presence_field_index)?
                {
                    continue;
                }
                let source_values = BorrowedRecord::new(record, &input_desc)
                    .to_values()
                    .map_err(IvmRuntimeError::RecordEncoding)?;
                let child_values = slot
                    .child_fields
                    .iter()
                    .map(|field| collect_by_projected_value(&source_values, field))
                    .collect::<Result<Vec<_>, _>>()?;
                candidates
                    .entry(child_key_descriptor.create(&child_values)?.into())
                    .or_insert_with(|| record.clone());
            }
            let selected = collect_by_slot_window_from_records(
                input_desc,
                candidates.into_values().collect(),
                slot,
            )?;
            let mut children = Vec::with_capacity(selected.len());
            for record in selected {
                let source_values = BorrowedRecord::new(&record, &input_desc)
                    .to_values()
                    .map_err(IvmRuntimeError::RecordEncoding)?;
                let mut child_values = slot
                    .child_fields
                    .iter()
                    .enumerate()
                    .map(|(index, field)| {
                        Ok::<Value, IvmRuntimeError>(collect_by_output_value(
                            &slot.child_descriptor.fields()[index].value_type,
                            collect_by_projected_value(&source_values, field)?,
                        ))
                    })
                    .collect::<Result<Vec<_>, _>>()?;
                child_values.extend(render_collect_by_slots(
                    input_desc,
                    records,
                    &record,
                    &slot.slots,
                )?);
                children.push(Value::Record(OwnedRecord::new(
                    slot.child_descriptor.create(&child_values)?,
                    slot.child_descriptor,
                )));
            }
            Ok(Value::Array(children))
        })
        .collect()
}

fn collect_by_slot_window_from_records(
    descriptor: RecordDescriptor,
    records: Vec<Bytes>,
    slot: &CollectBySlot,
) -> Result<Vec<Bytes>, IvmRuntimeError> {
    let mut ranked = records
        .into_iter()
        .map(|record| {
            Ok((
                collect_by_sort_key_for_fields(
                    descriptor,
                    &record,
                    &slot.sort_field_indices,
                    &slot.sort_directions,
                )?,
                record,
            ))
        })
        .collect::<Result<Vec<_>, IvmRuntimeError>>()?;
    ranked.sort_by(|left, right| left.0.cmp(&right.0).then_with(|| left.1.cmp(&right.1)));
    let skip = usize::try_from(slot.offset).unwrap_or(usize::MAX);
    let take = match slot.limit {
        TopByLimit::Finite(limit) => usize::try_from(limit).unwrap_or(usize::MAX),
        TopByLimit::Unbounded => usize::MAX,
    };
    Ok(ranked
        .into_iter()
        .skip(skip)
        .take(take)
        .map(|(_, record)| record)
        .collect())
}

/// Render the selected expand window keyed by its ordered contributing source
/// ids. Keeping the key separate from the rendered bytes is what lets expand
/// suppress only a byte-equal occurrence without collapsing equal tuples from
/// different joins.
pub(super) fn collect_by_expanded_window(
    input_desc: RecordDescriptor,
    output_desc: RecordDescriptor,
    collect_by: &CollectByOp,
    records: &[(Bytes, i64)],
) -> Result<BTreeMap<Vec<u8>, Bytes>, IvmRuntimeError> {
    let window = collect_by_window_from_records(input_desc, records, collect_by)?;
    let mut expanded = BTreeMap::new();
    for (record, copies) in window {
        let occurrence =
            encoded_record_key_part(input_desc, &record, &collect_by.occurrence_id_field_indices)?;
        // The complete typed occurrence carrier must distinguish every
        // derivation. A residual weighted duplicate is therefore malformed;
        // do not silently turn that ambiguity into one row.
        if copies != 1 || expanded.contains_key(&occurrence) {
            return Err(IvmRuntimeError::DuplicateCollectByOccurrenceId);
        }
        let values = BorrowedRecord::new(&record, &input_desc)
            .to_values()
            .map_err(IvmRuntimeError::RecordEncoding)?;
        let tuple = collect_by
            .tuple_fields
            .iter()
            .map(|field| collect_by_projected_value(&values, field))
            .collect::<Result<Vec<_>, _>>()?;
        expanded.insert(occurrence, output_desc.create(&tuple)?.into());
    }
    Ok(expanded)
}

fn collect_by_window_from_records(
    descriptor: RecordDescriptor,
    records: &[(Bytes, i64)],
    collect_by: &CollectByOp,
) -> Result<Vec<WindowedRecord>, IvmRuntimeError> {
    let mut ranked = Vec::new();
    for (record, weight) in records {
        if *weight > 0 {
            ranked.push((
                collect_by_sort_key(descriptor, record, collect_by)?,
                record.clone(),
                *weight,
            ));
        }
    }
    ranked.sort_by(|left, right| left.0.cmp(&right.0).then_with(|| left.1.cmp(&right.1)));
    let mut window = Vec::new();
    let mut to_skip = collect_by.offset;
    let mut remaining = match collect_by.limit {
        TopByLimit::Finite(limit) => Some(limit),
        TopByLimit::Unbounded => None,
    };
    for (_, record, weight) in ranked {
        if remaining == Some(0) {
            break;
        }
        let copies = weight as u64;
        let available = copies.saturating_sub(to_skip);
        to_skip = to_skip.saturating_sub(copies);
        let taken = remaining.map_or(available, |remaining| available.min(remaining));
        if taken > 0 {
            window.push((
                record,
                i64::try_from(taken).expect("window copies cannot exceed positive input weight"),
            ));
            if let Some(remaining) = &mut remaining {
                *remaining -= taken;
            }
        }
    }
    Ok(window)
}

fn collect_by_sort_key(
    descriptor: RecordDescriptor,
    record: &[u8],
    collect_by: &CollectByOp,
) -> Result<Vec<TopBySortPart>, IvmRuntimeError> {
    collect_by_sort_key_for_fields(
        descriptor,
        record,
        &collect_by.sort_field_indices,
        &collect_by.sort_directions,
    )
}

fn collect_by_sort_key_for_fields(
    descriptor: RecordDescriptor,
    record: &[u8],
    sort_field_indices: &[usize],
    sort_directions: &[TopByDirection],
) -> Result<Vec<TopBySortPart>, IvmRuntimeError> {
    let values = BorrowedRecord::new(record, &descriptor)
        .to_values()
        .map_err(IvmRuntimeError::RecordEncoding)?;
    sort_field_indices
        .iter()
        .zip(sort_directions)
        .map(|(field_idx, direction)| {
            Ok(TopBySortPart {
                key: values
                    .get(*field_idx)
                    .cloned()
                    .ok_or(IvmRuntimeError::GraphFieldIndexOutOfBounds(*field_idx))?,
                direction: *direction,
            })
        })
        .collect()
}

fn top_by_sort_key(
    descriptor: RecordDescriptor,
    record: &[u8],
    top_by: &TopByOp,
) -> Result<Vec<TopBySortPart>, IvmRuntimeError> {
    collect_by_sort_key_for_fields(
        descriptor,
        record,
        &top_by.sort_field_indices,
        &top_by.sort_directions,
    )
}

pub(super) fn unbounded_top_by_preserves_ordered_membership(
    descriptor: RecordDescriptor,
    deltas: &[RecordDelta],
    top_by: &TopByOp,
) -> Result<bool, IvmRuntimeError> {
    if top_by.limit != TopByLimit::Unbounded || deltas.is_empty() {
        return Ok(false);
    }
    let mut weights = BTreeMap::<(Vec<u8>, Vec<TopBySortPart>), i64>::new();
    for delta in deltas {
        let group = encoded_record_key_part(descriptor, delta.raw(), &top_by.group_field_indices)?;
        let sort = top_by_sort_key(descriptor, delta.raw(), top_by)?;
        *weights.entry((group, sort)).or_default() += delta.weight;
    }
    Ok(weights.values().all(|weight| *weight == 0))
}

pub(super) fn diff_record_windows(
    before: Vec<WindowedRecord>,
    after: Vec<WindowedRecord>,
) -> Vec<RecordDelta> {
    let mut weights = BTreeMap::<Bytes, i64>::new();
    for (record, copies) in &before {
        *weights.entry(record.clone()).or_default() -= *copies;
    }
    for (record, copies) in &after {
        *weights.entry(record.clone()).or_default() += *copies;
    }
    let mut deltas = Vec::new();
    for (record, _) in before {
        if weights.get(&record).is_some_and(|weight| *weight < 0)
            && let Some(weight) = weights.remove(&record)
        {
            deltas.push(RecordDelta { record, weight });
        }
    }
    for (record, _) in after {
        if weights.get(&record).is_some_and(|weight| *weight > 0)
            && let Some(weight) = weights.remove(&record)
        {
            deltas.push(RecordDelta { record, weight });
        }
    }
    debug_assert!(weights.values().all(|weight| *weight == 0));
    deltas
}

pub(super) fn encoded_record_key_part(
    descriptor: RecordDescriptor,
    record: &[u8],
    field_indices: &[usize],
) -> Result<Vec<u8>, IvmRuntimeError> {
    let mut key = Vec::new();
    for field_idx in field_indices {
        let value = descriptor.get_idx(record, *field_idx)?;
        encode_runtime_primary_key_part(&mut key, &value)?;
    }
    Ok(key)
}

pub(super) fn encoded_arrangement_key_part(
    descriptor: RecordDescriptor,
    record: &[u8],
    field_indices: &[usize],
) -> Result<Vec<u8>, IvmRuntimeError> {
    let mut key = Vec::new();
    for field_idx in field_indices {
        encode_key_part(&mut key, &descriptor.get_idx(record, *field_idx)?)?;
    }
    Ok(key)
}

fn encode_runtime_primary_key_part(
    key: &mut Vec<u8>,
    value: &Value,
) -> Result<(), IvmRuntimeError> {
    match value {
        Value::U8(value) => {
            key.push(0);
            key.push(*value);
        }
        Value::U16(value) => {
            key.push(1);
            key.extend(value.to_be_bytes());
        }
        Value::U32(value) => {
            key.push(2);
            key.extend(value.to_be_bytes());
        }
        Value::U64(value) => {
            key.push(3);
            key.extend(value.to_be_bytes());
        }
        Value::I32(value) => {
            key.push(14);
            key.extend(order_preserving_i32_bits(*value).to_be_bytes());
        }
        Value::I64(value) => {
            key.push(13);
            key.extend(order_preserving_i64_bits(*value).to_be_bytes());
        }
        Value::F64(value) => {
            key.push(4);
            key.extend(ordered_f64_key(*value).to_be_bytes());
        }
        Value::Bool(value) => {
            key.push(5);
            key.push(u8::from(*value));
        }
        Value::String(value) => {
            key.push(6);
            encode_runtime_ordered_bytes(key, value.as_bytes());
        }
        Value::EnumTag(value) => {
            key.push(0);
            key.push(*value);
        }
        Value::Bytes(value) => {
            key.push(7);
            encode_runtime_ordered_bytes(key, value);
        }
        Value::Uuid(value) => {
            key.push(10);
            key.extend_from_slice(value.as_bytes());
        }
        Value::Tuple(values) => {
            key.push(11);
            for value in values {
                encode_runtime_primary_key_part(key, value)?;
            }
        }
        Value::Nullable(None) => {
            key.push(12);
            key.push(0);
        }
        Value::Nullable(Some(value)) => {
            key.push(12);
            key.push(1);
            encode_runtime_primary_key_part(key, value)?;
        }
        Value::Array(_) | Value::Record(_) | Value::Enum(_) | Value::Large(_) => {
            return Err(IvmRuntimeError::UnsupportedJoinKey);
        }
    }
    Ok(())
}

fn ordered_f64_key(value: f64) -> u64 {
    let bits = value.to_bits();
    if bits & (1 << 63) == 0 {
        bits ^ (1 << 63)
    } else {
        !bits
    }
}

fn encode_runtime_ordered_bytes(key: &mut Vec<u8>, value: &[u8]) {
    for byte in value {
        if *byte == 0 {
            key.extend([0, 0xff]);
        } else {
            key.push(*byte);
        }
    }
    key.push(0);
    key.push(0);
}
