//! Join, anti-join, and arrangement maintenance for runtime evaluation.
//!
//! This module owns [`ArrangementState`], the indexed multiset used to probe
//! joins and anti-joins incrementally. The top-level runtime stores and shares
//! arrangements by input/key/scope; this module only advances those
//! arrangements and computes output deltas for one join operator. Graph
//! descriptors live in [`crate::ivm::op_types`], and tick scheduling lives in
//! [`super`].

use std::collections::{HashMap, HashSet};

use crate::records::RecordDescriptor;

use super::{
    ArrangementUpdateMode, AsOf, IvmRuntimeError, RecordDelta, SubTick, consolidate_deltas,
    encode_key_part,
};

type JoinBucket = HashMap<Vec<u8>, i64>;
type JoinIndex = HashMap<Vec<u8>, JoinBucket>;

#[derive(Clone, Debug, Default)]
pub(super) struct JoinState;

#[derive(Clone, Debug, Default)]
pub(super) struct AntiJoinState;

#[derive(Clone, Debug, Default)]
pub(super) struct ArrangementState {
    /// key -> record multiset. Records are kept as encoded bytes so probing can
    /// build output records without rehydrating whole tables.
    index: JoinIndex,
}

impl JoinState {
    #[allow(clippy::too_many_arguments)]
    pub(super) fn apply(
        &self,
        left_arrangement: &mut AsOf<ArrangementState, SubTick>,
        right_arrangement: &mut AsOf<ArrangementState, SubTick>,
        left_descriptor: &RecordDescriptor,
        right_descriptor: &RecordDescriptor,
        output_descriptor: &RecordDescriptor,
        left_on: &[String],
        right_on: &[String],
        left_delta: &[RecordDelta],
        right_delta: &[RecordDelta],
        left_sub_tick: SubTick,
        right_sub_tick: SubTick,
        update_mode: ArrangementUpdateMode,
    ) -> Result<Vec<RecordDelta>, IvmRuntimeError> {
        if left_on.len() != right_on.len() {
            return Err(IvmRuntimeError::JoinKeyArityMismatch {
                left: left_on.len(),
                right: right_on.len(),
            });
        }

        let keyed_left_delta = keyed_join_deltas(left_descriptor, left_on, left_delta)?;
        let keyed_right_delta = keyed_join_deltas(right_descriptor, right_on, right_delta)?;
        let mut deltas = Vec::new();
        let context = JoinChangeContext {
            left_descriptor,
            right_descriptor,
            output_descriptor,
        };

        advance_arrangement(
            left_arrangement,
            &keyed_left_delta,
            left_sub_tick,
            update_mode,
        )?;
        advance_arrangement(
            right_arrangement,
            &keyed_right_delta,
            right_sub_tick,
            update_mode,
        )?;

        append_join_deltas(
            &mut deltas,
            &context,
            &keyed_left_delta,
            &right_arrangement.value().index,
            JoinProbeSide::LeftDelta,
            1,
        )?;
        append_join_deltas(
            &mut deltas,
            &context,
            &keyed_right_delta,
            &left_arrangement.value().index,
            JoinProbeSide::RightDelta,
            1,
        )?;

        // Both arrangements are now current, so the two probes above each see
        // same-tick left/right pairs. Remove one copy of that cross term.
        let left_delta_index = build_join_delta_index(&keyed_left_delta);
        append_join_deltas(
            &mut deltas,
            &context,
            &keyed_right_delta,
            &left_delta_index,
            JoinProbeSide::RightDelta,
            -1,
        )?;

        Ok(consolidate_deltas(deltas))
    }
}

impl AntiJoinState {
    #[allow(clippy::too_many_arguments)]
    pub(super) fn apply(
        &self,
        left_arrangement: &mut AsOf<ArrangementState, SubTick>,
        right_arrangement: &mut AsOf<ArrangementState, SubTick>,
        left_descriptor: &RecordDescriptor,
        right_descriptor: &RecordDescriptor,
        _output_descriptor: &RecordDescriptor,
        left_on: &[String],
        right_on: &[String],
        left_delta: &[RecordDelta],
        right_delta: &[RecordDelta],
        left_sub_tick: SubTick,
        right_sub_tick: SubTick,
        update_mode: ArrangementUpdateMode,
    ) -> Result<Vec<RecordDelta>, IvmRuntimeError> {
        if left_on.len() != right_on.len() {
            return Err(IvmRuntimeError::JoinKeyArityMismatch {
                left: left_on.len(),
                right: right_on.len(),
            });
        }

        let keyed_left_delta = keyed_join_deltas(left_descriptor, left_on, left_delta)?;
        let keyed_right_delta = keyed_join_deltas(right_descriptor, right_on, right_delta)?;
        let mut affected_keys = HashSet::<Vec<u8>>::new();
        let mut old_right_counts = HashMap::<Vec<u8>, i64>::new();
        let mut old_left_buckets = HashMap::<Vec<u8>, JoinBucket>::new();
        if update_mode == ArrangementUpdateMode::Accumulate {
            for delta in &keyed_left_delta {
                let key = &delta.key;
                if affected_keys.insert(key.clone()) {
                    old_right_counts.insert(key.clone(), right_arrangement.value().key_count(key));
                    old_left_buckets.insert(
                        key.clone(),
                        left_arrangement
                            .value()
                            .bucket(key)
                            .cloned()
                            .unwrap_or_default(),
                    );
                }
            }
            for delta in &keyed_right_delta {
                let key = &delta.key;
                if affected_keys.insert(key.clone()) {
                    old_right_counts.insert(key.clone(), right_arrangement.value().key_count(key));
                    old_left_buckets.insert(
                        key.clone(),
                        left_arrangement
                            .value()
                            .bucket(key)
                            .cloned()
                            .unwrap_or_default(),
                    );
                }
            }
        }
        advance_arrangement(
            left_arrangement,
            &keyed_left_delta,
            left_sub_tick,
            update_mode,
        )?;
        advance_arrangement(
            right_arrangement,
            &keyed_right_delta,
            right_sub_tick,
            update_mode,
        )?;

        let mut deltas = Vec::new();
        match update_mode {
            ArrangementUpdateMode::Accumulate => {
                for key in affected_keys {
                    let old_right_count = old_right_counts.get(&key).copied().unwrap_or_default();
                    let new_right_count = right_arrangement.value().key_count(&key);
                    let old_visible = if old_right_count == 0 {
                        old_left_buckets.get(&key)
                    } else {
                        None
                    };
                    let new_visible = if new_right_count == 0 {
                        left_arrangement.value().bucket(&key)
                    } else {
                        None
                    };
                    append_bucket_diff(&mut deltas, new_visible, old_visible);
                }
            }
            ArrangementUpdateMode::Replace => {
                let mut left_keys = HashSet::<Vec<u8>>::new();
                for delta in &keyed_left_delta {
                    let key = &delta.key;
                    if left_keys.insert(key.clone())
                        && right_arrangement.value().key_count(key) == 0
                    {
                        append_bucket(&mut deltas, left_arrangement.value().bucket(key), 1);
                    }
                }
            }
        }

        Ok(consolidate_deltas(deltas))
    }
}

impl ArrangementState {
    pub(super) fn row_count(&self) -> usize {
        self.index
            .values()
            .map(|bucket| bucket.values().filter(|weight| **weight != 0).count())
            .sum()
    }

    pub(super) fn encoded_bytes(&self) -> usize {
        self.index
            .iter()
            .map(|(key, bucket)| key.len() + bucket.keys().map(Vec::len).sum::<usize>())
            .sum()
    }

    fn apply_update(
        &mut self,
        deltas: &[KeyedRecordDelta<'_>],
        update_mode: ArrangementUpdateMode,
    ) {
        match update_mode {
            ArrangementUpdateMode::Accumulate => {
                apply_join_delta_to_index(&mut self.index, deltas);
            }
            ArrangementUpdateMode::Replace => {
                self.index = build_join_delta_index(deltas);
            }
        }
    }

    fn key_count(&self, key: &[u8]) -> i64 {
        self.index
            .get(key)
            .map(|bucket| bucket.values().sum())
            .unwrap_or_default()
    }

    fn bucket(&self, key: &[u8]) -> Option<&JoinBucket> {
        self.index.get(key)
    }

    pub(super) fn apply_record_deltas(
        &mut self,
        descriptor: RecordDescriptor,
        fields: &[String],
        deltas: &[RecordDelta],
        update_mode: ArrangementUpdateMode,
    ) -> Result<(), IvmRuntimeError> {
        let keyed = keyed_join_deltas(&descriptor, fields, deltas)?;
        self.apply_update(&keyed, update_mode);
        Ok(())
    }

    pub(super) fn records_for_key(&self, key: &[u8]) -> Vec<(Vec<u8>, i64)> {
        self.index
            .get(key)
            .into_iter()
            .flat_map(|bucket| bucket.iter())
            .filter_map(|(record, weight)| (*weight > 0).then_some((record.clone(), *weight)))
            .collect()
    }
}

fn advance_arrangement(
    arrangement: &mut AsOf<ArrangementState, SubTick>,
    deltas: &[KeyedRecordDelta<'_>],
    sub_tick: SubTick,
    update_mode: ArrangementUpdateMode,
) -> Result<(), IvmRuntimeError> {
    if update_mode == ArrangementUpdateMode::Accumulate && arrangement.as_of() == Some(sub_tick) {
        return Ok(());
    }
    // Replace callers provide a faithful full snapshot, so they intentionally
    // rebuild even when the stamp already matches this logical time.
    let replace_within_same_tick = update_mode == ArrangementUpdateMode::Replace
        && arrangement
            .as_of()
            .is_some_and(|current| current.tick == sub_tick.tick);
    if !replace_within_same_tick
        && arrangement
            .as_of()
            .is_some_and(|current| current > sub_tick)
    {
        return Err(IvmRuntimeError::OutOfOrderRuntimeState {
            current: format!("{:?}", arrangement.as_of().expect("checked above")),
            next: format!("{sub_tick:?}"),
        });
    }
    arrangement.value_mut().apply_update(deltas, update_mode);
    if replace_within_same_tick {
        arrangement.replace_as_of_at_least(sub_tick);
    } else {
        arrangement.mark_forward_as_of(sub_tick)?;
    }
    Ok(())
}

/// Borrowed descriptors and key fields shared while emitting join deltas.
struct JoinChangeContext<'a> {
    left_descriptor: &'a RecordDescriptor,
    right_descriptor: &'a RecordDescriptor,
    output_descriptor: &'a RecordDescriptor,
}

struct KeyedRecordDelta<'a> {
    delta: &'a RecordDelta,
    key: Vec<u8>,
}

enum JoinProbeSide {
    LeftDelta,
    RightDelta,
}

fn append_join_deltas(
    deltas: &mut Vec<RecordDelta>,
    context: &JoinChangeContext<'_>,
    delta_records: &[KeyedRecordDelta<'_>],
    stored: &JoinIndex,
    side: JoinProbeSide,
    sign: i64,
) -> Result<(), IvmRuntimeError> {
    for delta in delta_records {
        if delta.delta.weight == 0 {
            continue;
        }
        let Some(bucket) = stored.get(&delta.key) else {
            continue;
        };
        for (stored_record, right_weight) in bucket {
            if *right_weight == 0 {
                continue;
            }

            let weight = sign * delta.delta.weight * *right_weight;
            if weight == 0 {
                continue;
            }
            let (left_record, right_record) = match side {
                JoinProbeSide::LeftDelta => (delta.delta.raw(), stored_record.as_slice()),
                JoinProbeSide::RightDelta => (stored_record.as_slice(), delta.delta.raw()),
            };
            deltas.push(RecordDelta {
                record: create_join_record(
                    context.left_descriptor,
                    left_record,
                    context.right_descriptor,
                    right_record,
                    context.output_descriptor,
                )?,
                weight,
            });
        }
    }

    Ok(())
}

fn apply_join_delta_to_index(index: &mut JoinIndex, deltas: &[KeyedRecordDelta<'_>]) {
    for delta in deltas {
        let bucket = index.entry(delta.key.clone()).or_default();
        let next_weight =
            bucket.get(&delta.delta.record).copied().unwrap_or_default() + delta.delta.weight;
        if next_weight == 0 {
            bucket.remove(&delta.delta.record);
            if bucket.is_empty() {
                index.remove(&delta.key);
            }
        } else {
            bucket.insert(delta.delta.record.clone(), next_weight);
        }
    }
}

fn build_join_delta_index(deltas: &[KeyedRecordDelta<'_>]) -> JoinIndex {
    let mut index = HashMap::new();
    apply_join_delta_to_index(&mut index, deltas);
    index
}

fn keyed_join_deltas<'a>(
    descriptor: &RecordDescriptor,
    fields: &[String],
    deltas: &'a [RecordDelta],
) -> Result<Vec<KeyedRecordDelta<'a>>, IvmRuntimeError> {
    let mut keyed = Vec::new();
    for delta in deltas {
        for key in join_keys(descriptor, delta.raw(), fields)? {
            keyed.push(KeyedRecordDelta { delta, key });
        }
    }
    Ok(keyed)
}

fn append_bucket(deltas: &mut Vec<RecordDelta>, bucket: Option<&JoinBucket>, sign: i64) {
    let Some(bucket) = bucket else {
        return;
    };
    for (record, weight) in bucket {
        let weight = sign * *weight;
        if weight == 0 {
            continue;
        }
        deltas.push(RecordDelta {
            record: record.clone(),
            weight,
        });
    }
}

fn append_bucket_diff(
    deltas: &mut Vec<RecordDelta>,
    new_bucket: Option<&JoinBucket>,
    old_bucket: Option<&JoinBucket>,
) {
    if let Some(old_bucket) = old_bucket {
        append_bucket(deltas, Some(old_bucket), -1);
    }
    if let Some(new_bucket) = new_bucket {
        append_bucket(deltas, Some(new_bucket), 1);
    }
}

pub(super) fn join_keys(
    descriptor: &RecordDescriptor,
    record: &[u8],
    fields: &[String],
) -> Result<Vec<Vec<u8>>, IvmRuntimeError> {
    if fields.len() == 1 {
        let values = descriptor.get(record, &fields[0])?;
        let parts = join_key_parts(values);
        if parts.is_empty() {
            return Ok(Vec::new());
        }
        if parts.len() == 1 {
            let mut key = Vec::new();
            encode_key_part(&mut key, &parts[0])?;
            return Ok(vec![key]);
        }
        let mut keys = Vec::with_capacity(parts.len());
        let mut seen = HashSet::new();
        for value in &parts {
            let mut key = Vec::new();
            encode_key_part(&mut key, value)?;
            if !seen.contains(&key) {
                seen.insert(key.clone());
                keys.push(key);
            }
        }
        return Ok(keys);
    }

    let mut keys = vec![Vec::new()];
    let mut seen = HashSet::new();

    for field in fields {
        let values = descriptor.get(record, field)?;
        let parts = join_key_parts(values);

        if parts.is_empty() {
            return Ok(Vec::new());
        }

        let mut next_keys = Vec::with_capacity(keys.len() * parts.len());
        for key in &keys {
            for value in &parts {
                let mut next = key.clone();
                encode_key_part(&mut next, value)?;
                if !seen.contains(&next) {
                    seen.insert(next.clone());
                    next_keys.push(next);
                }
            }
        }
        keys = next_keys;
        seen.clear();
    }

    Ok(keys)
}

fn join_key_parts(value: crate::records::Value) -> Vec<crate::records::Value> {
    match value {
        crate::records::Value::Array(values) => values,
        crate::records::Value::Nullable(Some(value)) => match *value {
            crate::records::Value::Array(values) => values
                .into_iter()
                .map(|value| crate::records::Value::Nullable(Some(Box::new(value))))
                .collect(),
            value => vec![crate::records::Value::Nullable(Some(Box::new(value)))],
        },
        value => vec![value],
    }
}

pub(super) fn create_join_record(
    left_descriptor: &RecordDescriptor,
    left_record: &[u8],
    right_descriptor: &RecordDescriptor,
    right_record: &[u8],
    output_descriptor: &RecordDescriptor,
) -> Result<Vec<u8>, IvmRuntimeError> {
    let mapping = output_descriptor
        .fields()
        .iter()
        .map(|field| {
            let name = field
                .name
                .as_deref()
                .ok_or_else(|| IvmRuntimeError::GraphFieldNotFound("<unnamed>".to_owned()))?;
            if let Some(name) = name.strip_prefix("left.") {
                let field_idx = left_descriptor
                    .field_index(name)
                    .ok_or_else(|| IvmRuntimeError::GraphFieldNotFound(name.to_owned()))?;
                Ok((0, field_idx))
            } else if let Some(name) = name.strip_prefix("right.") {
                let field_idx = right_descriptor
                    .field_index(name)
                    .ok_or_else(|| IvmRuntimeError::GraphFieldNotFound(name.to_owned()))?;
                Ok((1, field_idx))
            } else {
                Err(IvmRuntimeError::GraphFieldNotFound(name.to_owned()))
            }
        })
        .collect::<Result<Vec<_>, IvmRuntimeError>>()?;
    Ok(output_descriptor.project_record_raw(
        &[*left_descriptor, *right_descriptor],
        &[left_record, right_record],
        &mapping,
    )?)
}
