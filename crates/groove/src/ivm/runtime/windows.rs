//! Arg-by, TopBy, and structured CollectBy window maintenance.

use super::*;

type SourceRecord = (Vec<u8>, Bytes);
pub(super) type WindowedRecord = (Bytes, i64);

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
enum TopBySortKey {
    Null,
    U8(u8),
    U16(u16),
    U32(u32),
    U64(u64),
    I32(i32),
    I64(i64),
    F64(u64),
    Bool(bool),
    String(String),
    Bytes(Vec<u8>),
    Uuid([u8; 16]),
    EnumTag(u8),
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(super) struct TopBySortPart {
    key: TopBySortKey,
    pub(super) direction: TopByDirection,
}

impl Ord for TopBySortPart {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        let ordering = self.key.cmp(&other.key);
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

pub(super) fn arg_by_candidate_replaces(
    candidate_key: &[u8],
    candidate_record: &[u8],
    winner_key: &[u8],
    winner_record: &[u8],
    direction: ArgByDirection,
) -> bool {
    match candidate_key.cmp(winner_key) {
        std::cmp::Ordering::Less => matches!(direction, ArgByDirection::Min),
        std::cmp::Ordering::Greater => matches!(direction, ArgByDirection::Max),
        std::cmp::Ordering::Equal => candidate_record < winner_record,
    }
}

pub(super) struct ArgBySpec<'a> {
    pub(super) group_fields: &'a [String],
    pub(super) group_field_indices: &'a [usize],
    pub(super) comparison_field_indices: &'a [usize],
    pub(super) direction: ArgByDirection,
}

pub(super) fn arg_by_winner_from_records(
    descriptor: RecordDescriptor,
    comparison_field_indices: &[usize],
    records: Vec<(Bytes, i64)>,
    direction: ArgByDirection,
) -> Result<Option<SourceRecord>, IvmRuntimeError> {
    // Match TopBy's total-order convention: declared fields decide the rank
    // under the operator direction, while encoded record bytes break exact
    // comparison-key ties ascending for both ArgMinBy and ArgMaxBy.
    let mut winner = None;
    for (record, weight) in records {
        if weight <= 0 {
            continue;
        }
        let key = encoded_record_key_part(descriptor, &record, comparison_field_indices)?;
        let replaces = winner
            .as_ref()
            .is_none_or(|(winner_key, winner_record): &SourceRecord| {
                arg_by_candidate_replaces(&key, &record, winner_key, winner_record, direction)
            });
        if replaces {
            winner = Some((key, record));
        }
    }
    Ok(winner)
}

pub(super) fn arg_by_winner_before_from_deltas(
    descriptor: RecordDescriptor,
    comparison_field_indices: &[usize],
    after_records: Vec<(Bytes, i64)>,
    deltas: Vec<RecordDelta>,
    direction: ArgByDirection,
) -> Result<Option<SourceRecord>, IvmRuntimeError> {
    arg_by_winner_from_records(
        descriptor,
        comparison_field_indices,
        records_before_deltas(after_records, &deltas),
        direction,
    )
}

#[cfg(test)]
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

pub(super) fn top_by_window_from_ordered_group(
    records: Option<&BTreeMap<CollectByOrderKey, i64>>,
    top_by: &TopByOp,
) -> Vec<WindowedRecord> {
    let mut window = Vec::new();
    let mut to_skip = top_by.offset;
    let mut remaining = match top_by.limit {
        TopByLimit::Finite(limit) => Some(limit),
        TopByLimit::Unbounded => None,
    };
    for ((_, record), weight) in records.into_iter().flatten() {
        if remaining == Some(0) {
            break;
        }
        if *weight <= 0 {
            continue;
        }
        let copies = *weight as u64;
        let available = copies.saturating_sub(to_skip);
        to_skip = to_skip.saturating_sub(copies);
        let taken = remaining.map_or(available, |remaining| available.min(remaining));
        if taken > 0 {
            window.push((
                record.clone(),
                i64::try_from(taken).expect("window copies cannot exceed positive input weight"),
            ));
            if let Some(remaining) = &mut remaining {
                *remaining -= taken;
            }
        }
    }
    window
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
        state.emitted_root_order.clear();
        state.emitted_root_keys.clear();
    }
    let mut before = BTreeMap::<Vec<u8>, Option<Bytes>>::new();
    let mut before_order = BTreeMap::<Vec<u8>, Option<CollectByOrderKey>>::new();
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
        if !before_order.contains_key(&group_key) {
            before_order.insert(
                group_key.clone(),
                state
                    .groups
                    .get(&group_key)
                    .and_then(collect_by_root_order_key),
            );
        }
        let sort_key = collect_by_sort_key(input_desc, delta.raw(), collect_by)?;
        let state_key = (sort_key, delta.record.clone());
        let group = state.groups.entry(group_key.clone()).or_default();
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

    // Capture the actual public sequence once before replacing changed sort
    // keys below. Terminal edits are applied sequentially, so final ranks
    // alone are insufficient for a batch that mixes moves and inserts.
    let mut public_sequence = state
        .emitted_root_order
        .values()
        .cloned()
        .collect::<Vec<_>>();

    // `groups` also retains join-maintenance state that may never be exposed
    // as a root terminal. Keep the public rank index in sync only for roots
    // which were actually materialized, so an internal group cannot shift a
    // subscriber-visible Insert or Move position.
    for (root_key, before_key) in &before_order {
        if !state.emitted_root_keys.contains(root_key) {
            continue;
        }
        if let Some(before_key) = before_key {
            state.emitted_root_order.remove(before_key);
        }
        if let Some(after_key) = state
            .groups
            .get(root_key)
            .and_then(collect_by_root_order_key)
        {
            state.emitted_root_order.insert(after_key, root_key.clone());
        }
    }

    let mut operations = Vec::new();
    // Remove first so the later positional inserts/moves index the final
    // retained sequence rather than an opaque-key snapshot.
    for (root_key, before_key) in &before_order {
        if before_key.is_some()
            && state.emitted_root_keys.contains(root_key)
            && !state.groups.contains_key(root_key)
        {
            operations.push(TerminalOperation {
                root_descriptor: output_desc,
                root_key: root_key.clone(),
                path: Vec::new(),
                edit: TerminalEdit::Remove {
                    key: root_key.clone(),
                },
            });
            state.emitted_root_keys.remove(root_key);
            public_sequence.retain(|key| key != root_key);
        }
    }

    // `emitted_root_order` is already the compiled total order. Snapshot its
    // retained entries once per batch so changed roots can find their target
    // positions without re-scanning the whole result for every Move or
    // Insert.
    let retained_final_order = state
        .emitted_root_order
        .iter()
        .filter(|(_, root_key)| state.emitted_root_keys.contains(*root_key))
        .map(|(order_key, root_key)| (order_key.clone(), root_key.clone()))
        .collect::<Vec<_>>();
    let retained_final_positions = retained_final_order
        .iter()
        .enumerate()
        .map(|(index, (_, root_key))| (root_key.clone(), index))
        .collect::<BTreeMap<_, _>>();

    // Reposition retained occurrences before inserting new ones. Their target
    // index is their rank among retained public roots only; inserting first
    // would make a final absolute rank address the wrong mutable sequence.
    let mut moves = Vec::new();
    for (root_key, before_key) in &before_order {
        let Some(before_key) = before_key else {
            continue;
        };
        if !before.get(root_key).is_some_and(Option::is_some)
            || !state.emitted_root_keys.contains(root_key)
        {
            continue;
        }
        let Some(after_key) = state
            .groups
            .get(root_key)
            .and_then(collect_by_root_order_key)
        else {
            continue;
        };
        if *before_key != after_key {
            moves.push((after_key, root_key.clone()));
        }
    }
    moves.sort_by(|(left, _), (right, _)| left.cmp(right));
    for (_, root_key) in moves {
        let index = *retained_final_positions
            .get(&root_key)
            .expect("moved root has a final retained position");
        let current_index = public_sequence
            .iter()
            .position(|key| key == &root_key)
            .expect("emitted root is present in the public sequence");
        if current_index == index {
            continue;
        }
        let root_key = public_sequence.remove(current_index);
        public_sequence.insert(index, root_key.clone());
        operations.push(TerminalOperation {
            root_descriptor: output_desc,
            root_key: root_key.clone(),
            path: Vec::new(),
            edit: TerminalEdit::Move {
                key: root_key,
                index,
            },
        });
    }

    let mut inserts = Vec::new();
    for (root_key, before_key) in &before_order {
        if before_key.is_some() || !state.groups.contains_key(root_key) {
            continue;
        }
        let order_key = state
            .groups
            .get(root_key)
            .and_then(collect_by_root_order_key)
            .expect("retained root has an order key");
        let record = state
            .groups
            .get(root_key)
            .map(|group| {
                let records = group
                    .iter()
                    .map(|((_, record), weight)| (record.clone(), *weight))
                    .collect::<Vec<_>>();
                collect_by_root_from_records(input_desc, output_desc, collect_by, &records)
            })
            .transpose()?
            .flatten()
            .ok_or_else(|| {
                IvmRuntimeError::InvalidCollectBy(
                    "new root collector group did not render a terminal row".into(),
                )
            })?;
        inserts.push((order_key, root_key.clone(), record));
    }
    inserts.sort_by(|(left, _, _), (right, _, _)| left.cmp(right));
    for (new_roots_before, (order_key, root_key, record)) in inserts.into_iter().enumerate() {
        // Insert lower-ranked new occurrences first. Each insertion joins the
        // mutable sequence before the next rank is computed. Existing roots
        // come from the one batch snapshot above; earlier new roots all sort
        // before this one because `inserts` is ordered by the same total key.
        let index = retained_final_order.partition_point(|(candidate, _)| candidate < &order_key)
            + new_roots_before;
        state.emitted_root_order.insert(order_key, root_key.clone());
        state.emitted_root_keys.insert(root_key.clone());
        public_sequence.insert(index, root_key.clone());
        operations.push(TerminalOperation {
            root_descriptor: output_desc,
            root_key: root_key.clone(),
            path: Vec::new(),
            edit: TerminalEdit::Insert {
                index,
                key: root_key,
                value: record.to_vec(),
            },
        });
    }

    for (root_key, before_record) in before {
        // `groups` can retain a root-collector maintenance group before it
        // has ever been presented to this terminal consumer. Without a prior
        // Insert, an Update would address no facade occurrence.
        if !state.emitted_root_keys.contains(&root_key) {
            continue;
        }
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
            // New roots were inserted above in final collector order.
            (None, Some(_)) => continue,
            (Some(_), Some(record)) => TerminalEdit::Update {
                key: root_key.clone(),
                value: record.to_vec(),
            },
            // Removed roots were retracted before inserts/moves above.
            (Some(_), None) => continue,
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

fn collect_by_root_order_key(
    group: &BTreeMap<CollectByOrderKey, i64>,
) -> Option<CollectByOrderKey> {
    group
        .iter()
        .find(|(_, weight)| **weight > 0)
        .map(|(order_key, _)| order_key.clone())
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
            let field = descriptor
                .fields()
                .get(*field_idx)
                .ok_or(IvmRuntimeError::GraphFieldIndexOutOfBounds(*field_idx))?;
            let value = values
                .get(*field_idx)
                .cloned()
                .ok_or(IvmRuntimeError::GraphFieldIndexOutOfBounds(*field_idx))?;
            Ok(TopBySortPart {
                key: top_by_sort_value(&field.value_type, value)?,
                direction: *direction,
            })
        })
        .collect()
}

fn top_by_sort_value(
    value_type: &ValueType,
    mut value: Value,
) -> Result<TopBySortKey, IvmRuntimeError> {
    if !collect_by_ordered_scalar(value_type) {
        return Err(IvmRuntimeError::InvalidTopBy(
            "sort field value must be an orderable scalar".to_owned(),
        ));
    }
    if is_sql_null_value(&value) {
        return Ok(TopBySortKey::Null);
    }
    let mut value_type = value_type;
    while let ValueType::Nullable(inner) = value_type {
        value_type = inner;
    }
    loop {
        match value {
            Value::Nullable(Some(inner)) => value = *inner,
            other => {
                value = other;
                break;
            }
        }
    }
    let key = match (value_type, value) {
        (ValueType::U8, Value::U8(value)) => TopBySortKey::U8(value),
        (ValueType::U16, Value::U16(value)) => TopBySortKey::U16(value),
        (ValueType::U32, Value::U32(value)) => TopBySortKey::U32(value),
        (ValueType::U64, Value::U64(value)) => TopBySortKey::U64(value),
        (ValueType::I32, Value::I32(value)) => TopBySortKey::I32(value),
        (ValueType::I64, Value::I64(value)) => TopBySortKey::I64(value),
        (ValueType::F64, Value::F64(value)) if !value.is_nan() => {
            let value = if value == 0.0 { 0.0 } else { value };
            let bits = value.to_bits();
            let ordered = if bits & (1 << 63) == 0 {
                bits ^ (1 << 63)
            } else {
                !bits
            };
            TopBySortKey::F64(ordered)
        }
        (ValueType::Bool, Value::Bool(value)) => TopBySortKey::Bool(value),
        (ValueType::String, Value::String(value)) => TopBySortKey::String(value),
        (ValueType::Bytes, Value::Bytes(value)) => TopBySortKey::Bytes(value),
        (ValueType::Uuid, Value::Uuid(value)) => TopBySortKey::Uuid(*value.as_bytes()),
        (ValueType::EnumTag(_), Value::EnumTag(value)) => TopBySortKey::EnumTag(value),
        _ => {
            return Err(IvmRuntimeError::InvalidTopBy(
                "sort field value must match its orderable scalar type".to_owned(),
            ));
        }
    };
    Ok(key)
}

pub(super) fn top_by_sort_key(
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

#[cfg(test)]
mod root_terminal_tests {
    use super::*;

    fn record_descriptor() -> RecordDescriptor {
        RecordDescriptor::new([
            ("root", ValueType::Uuid),
            ("joined", ValueType::Uuid),
            ("rank", ValueType::String),
        ])
    }

    fn collector() -> CollectByOp {
        let projection = |field: &str, field_idx| CollectByProjection {
            field: field.to_owned(),
            field_idx,
            output_name: field.to_owned(),
            unwrap_nullable: false,
        };
        CollectByOp {
            mode: CollectByMode::Root,
            group_fields: vec!["root".to_owned(), "joined".to_owned()],
            group_field_indices: vec![0, 1],
            parent_fields: vec![
                projection("root", 0),
                projection("joined", 1),
                projection("rank", 2),
            ],
            child_fields: Vec::new(),
            child_descriptor: RecordDescriptor::default(),
            collection_field: String::new(),
            collection_field_index: 0,
            slots: Vec::new(),
            tuple_fields: Vec::new(),
            occurrence_id_fields: vec!["root".to_owned(), "joined".to_owned()],
            occurrence_id_field_indices: vec![0, 1],
            order_fields: vec![TopByOrderField {
                field: "rank".to_owned(),
                direction: TopByDirection::Desc,
            }],
            tie_fields: vec!["root".to_owned(), "joined".to_owned()],
            sort_field_indices: vec![2, 0, 1],
            sort_directions: vec![
                TopByDirection::Desc,
                TopByDirection::Asc,
                TopByDirection::Asc,
            ],
            offset: 0,
            limit: TopByLimit::Unbounded,
        }
    }

    fn record(root: u8, joined: u8, rank: &str) -> Bytes {
        record_descriptor()
            .create(&[
                Value::Uuid(uuid::Uuid::from_bytes([root; 16])),
                Value::Uuid(uuid::Uuid::from_bytes([joined; 16])),
                Value::String(rank.to_owned()),
            ])
            .unwrap()
            .into()
    }

    fn delta(root: u8, joined: u8, rank: &str, weight: i64) -> RecordDelta {
        RecordDelta {
            record: record(root, joined, rank),
            weight,
        }
    }

    fn root_key(root: u8, joined: u8) -> Vec<u8> {
        encoded_record_key_part(record_descriptor(), &record(root, joined, "key"), &[0, 1]).unwrap()
    }

    fn apply_root_operations(roots: &mut Vec<(Vec<u8>, String)>, operations: &[TerminalOperation]) {
        let rank = |value: &[u8]| {
            let Value::String(rank) = BorrowedRecord::new(value, &record_descriptor())
                .get_idx(2)
                .unwrap()
            else {
                panic!("root terminal rank must decode as text");
            };
            rank
        };
        for operation in operations {
            assert!(operation.path.is_empty());
            match &operation.edit {
                TerminalEdit::Insert { index, key, value } => {
                    roots.insert(*index, (key.clone(), rank(value)));
                }
                TerminalEdit::Remove { key } => {
                    let index = roots.iter().position(|(root, _)| root == key).unwrap();
                    roots.remove(index);
                }
                TerminalEdit::Move { key, index } => {
                    let current = roots.iter().position(|(root, _)| root == key).unwrap();
                    let root = roots.remove(current);
                    roots.insert(*index, root);
                }
                TerminalEdit::Update { key, value } => {
                    let (_, existing_rank) = roots
                        .iter_mut()
                        .find(|(root, _)| root == key)
                        .expect("terminal root update addresses a present occurrence");
                    *existing_rank = rank(value);
                }
            }
        }
    }

    #[test]
    fn root_terminal_order_excludes_non_emitted_groups_and_moves_exact_occurrences() {
        let input = record_descriptor();
        let output = record_descriptor();
        let collect_by = collector();
        let mut state = CollectByIncrementalState::default();

        // A prior maintenance-only group ranks before every public root, but
        // was installed with `emit = false` and therefore has no facade
        // occurrence. It must not offset later public positions.
        assert!(
            update_collect_by_root_terminal_state(
                input.clone(),
                output.clone(),
                &collect_by,
                &mut state,
                &[delta(0xf0, 0xf1, "zzzz", 1)],
                false,
            )
            .unwrap()
            .is_empty()
        );
        assert!(
            update_collect_by_root_terminal_state(
                input.clone(),
                output.clone(),
                &collect_by,
                &mut state,
                &[delta(0xf0, 0xf1, "zzzz", -1), delta(0xf0, 0xf1, "zzzzz", 1),],
                true,
            )
            .unwrap()
            .is_empty(),
            "a non-emitted maintenance group cannot leak an unaddressable Update"
        );

        let opened = update_collect_by_root_terminal_state(
            input.clone(),
            output.clone(),
            &collect_by,
            &mut state,
            &[
                delta(0xa1, 0x11, "alpha", 1),
                delta(0xb1, 0x22, "maria", 1),
                delta(0xc1, 0x33, "zoe", 1),
            ],
            true,
        )
        .unwrap();
        assert_eq!(
            opened
                .iter()
                .filter_map(|operation| match operation.edit {
                    TerminalEdit::Insert { index, .. } => Some(index),
                    _ => None,
                })
                .collect::<Vec<_>>(),
            vec![0, 1, 2],
            "the non-emitted zzzz maintenance group cannot shift public inserts"
        );

        // Two simultaneous sort changes exercise final-index computation for
        // more than one Move. The keys remain root+joined occurrences rather
        // than collapsing to the shared public root UUID.
        let moved = update_collect_by_root_terminal_state(
            input,
            output,
            &collect_by,
            &mut state,
            &[
                delta(0xa1, 0x11, "alpha", -1),
                delta(0xa1, 0x11, "yyyy", 1),
                delta(0xc1, 0x33, "zoe", -1),
                delta(0xc1, 0x33, "aaaa", 1),
            ],
            true,
        )
        .unwrap();
        let moves = moved
            .iter()
            .filter_map(|operation| match operation.edit {
                TerminalEdit::Move { index, .. } => Some((operation.root_key.clone(), index)),
                _ => None,
            })
            .collect::<Vec<_>>();
        assert_eq!(moves.len(), 2);
        assert_eq!(moves[0].1, 0);
        assert_eq!(moves[1].1, 2);
        assert_ne!(moves[0].0, moves[1].0);
        assert!(moves.iter().all(|(key, _)| key.len() > 17));
    }

    #[test]
    fn root_terminal_mixed_remove_move_insert_mutates_to_final_public_order() {
        let input = record_descriptor();
        let output = record_descriptor();
        let collect_by = collector();
        let mut state = CollectByIncrementalState::default();
        let opened = update_collect_by_root_terminal_state(
            input.clone(),
            output.clone(),
            &collect_by,
            &mut state,
            &[
                delta(0xa1, 0x11, "zoe", 1),
                delta(0xb1, 0x22, "maria", 1),
                delta(0xd1, 0x44, "beta", 1),
            ],
            true,
        )
        .unwrap();
        let mut roots = Vec::new();
        apply_root_operations(&mut roots, &opened);
        assert_eq!(
            roots,
            vec![
                (root_key(0xa1, 0x11), "zoe".to_owned()),
                (root_key(0xb1, 0x22), "maria".to_owned()),
                (root_key(0xd1, 0x44), "beta".to_owned()),
            ]
        );

        // The batch retracts D, lowers A below B, and inserts C between them.
        // Applying the terminal stream itself must reach B,C,A; final ranks
        // emitted independently would instead produce C,B,A.
        let mixed = update_collect_by_root_terminal_state(
            input,
            output,
            &collect_by,
            &mut state,
            &[
                delta(0xd1, 0x44, "beta", -1),
                delta(0xa1, 0x11, "zoe", -1),
                delta(0xa1, 0x11, "alpha", 1),
                delta(0xc1, 0x33, "lima", 1),
            ],
            true,
        )
        .unwrap();
        assert!(
            mixed
                .iter()
                .any(|operation| matches!(operation.edit, TerminalEdit::Remove { .. }))
        );
        assert!(
            mixed
                .iter()
                .any(|operation| matches!(operation.edit, TerminalEdit::Move { .. }))
        );
        assert!(
            mixed
                .iter()
                .any(|operation| matches!(operation.edit, TerminalEdit::Insert { .. }))
        );
        apply_root_operations(&mut roots, &mixed);
        assert_eq!(
            roots,
            vec![
                (root_key(0xb1, 0x22), "maria".to_owned()),
                (root_key(0xc1, 0x33), "lima".to_owned()),
                (root_key(0xa1, 0x11), "alpha".to_owned()),
            ]
        );
    }
}
