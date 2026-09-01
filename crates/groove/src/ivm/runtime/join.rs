//! Join, anti-join, and arrangement maintenance for runtime evaluation.
//!
//! This module owns [`ArrangementState`], the indexed multiset used to probe
//! joins and anti-joins incrementally. The top-level runtime stores and shares
//! arrangements by input/key/scope; this module only advances those
//! arrangements and computes output deltas for one join operator. Graph
//! descriptors live in [`crate::ivm::op_types`], and tick scheduling lives in
//! [`super`].

use bytes::{Bytes, BytesMut};
use rustc_hash::{FxHashMap as HashMap, FxHashSet as HashSet};
use smallvec::SmallVec;
use std::ops::Range;
use std::rc::Rc;

use crate::{
    ivm::ValueComparison,
    records::{RecordDescriptor, ValueType},
};

use super::{
    ArrangementUpdateMode, AsOf, IvmRuntimeError, RecordDelta, SubTick, consolidate_deltas,
    encode_key_part,
};

pub(super) type JoinKey = SmallVec<[u8; 64]>;
type JoinBucket = HashMap<Bytes, i64>;
type JoinIndex = HashMap<JoinKey, JoinBucket>;

pub(super) fn touched_join_keys(
    descriptor: &RecordDescriptor,
    fields: &[String],
    deltas: &[RecordDelta],
    comparison: ValueComparison,
) -> Result<Vec<Vec<u8>>, IvmRuntimeError> {
    Ok(keyed_join_deltas(descriptor, fields, deltas, comparison)?
        .into_iter()
        .map(|delta| delta.key.into_vec())
        .collect())
}

#[derive(Clone, Debug, Default)]
pub(super) struct JoinState;

#[derive(Clone, Debug, Default)]
pub(super) struct ArrangementState {
    /// Immutable base buckets are shared between evaluator snapshots. Updates
    /// retain only touched buckets in the overlay, rather than cloning the
    /// complete join index.
    index: Rc<JoinIndex>,
    overlay: Rc<HashMap<JoinKey, Option<JoinBucket>>>,
}

#[derive(Clone, Debug, Default)]
pub(super) struct AntiJoinState;

enum JoinLookup<'a> {
    Arrangement(&'a ArrangementState),
    Index(&'a JoinIndex),
}

impl JoinLookup<'_> {
    fn bucket(&self, key: &JoinKey) -> Option<&JoinBucket> {
        match self {
            Self::Arrangement(arrangement) => match arrangement.overlay.get(key) {
                Some(bucket) => bucket.as_ref(),
                None => arrangement.index.get(key),
            },
            Self::Index(index) => index.get(key),
        }
    }
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
        // how to map the the fields from the inputs to the ouput
        // example:
        // Left album fields:
        // 0 = id
        // 1 = artist_id
        // 2 = title
        //
        // Right artist fields:
        // 0 = id
        // 1 = name
        //
        // Desire output:
        // 0 = album id
        // 1 = album title
        // 2 = artist name
        //
        // [
        //    (0, 0), // output field 0 comes from left field 0
        //    (0, 2), // output field 1 comes from left field 2
        //    (1, 1), // output field 2 comes from right field 1
        // ]
        //
        // 0 is left
        // 1 is right
        output_mapping: &[(usize, usize)],
        // left fields of join such as `["id"]
        left_on: &[String],
        // right fields of join such as `["artist_id"]
        right_on: &[String],
        comparison: ValueComparison,
        // Changed left records with signed weights
        left_delta: &[RecordDelta],
        right_delta: &[RecordDelta],
        left_sub_tick: SubTick,
        right_sub_tick: SubTick,
        update_mode: ArrangementUpdateMode,
    ) -> Result<Vec<RecordDelta>, IvmRuntimeError> {
        // Fields have to be the same:
        // left:  (country_id, artist_id)
        // right: (country_id, id)
        // This is ok!
        //
        // left:  (country_id, artist_id)
        // right: (id)
        // This is not ok
        if left_on.len() != right_on.len() {
            return Err(IvmRuntimeError::JoinKeyArityMismatch {
                left: left_on.len(),
                right: right_on.len(),
            });
        }

        // let's get the deltas left and right, adding the join keys. For example:
        // Left RecordDelta:
        // album(13, artist_id=7, "Yellow") -> +1
        //
        // Keyed left delta:
        // key = encode(7)
        // record = album(13, 7, "Yellow")
        // weight = +1
        //
        // The Key will be use to get throught the right_arrangement.index.get(&left_delta.key) fast the matching raws:
        let keyed_left_delta = keyed_join_deltas(left_descriptor, left_on, left_delta, comparison)?;
        let keyed_right_delta =
            keyed_join_deltas(right_descriptor, right_on, right_delta, comparison)?;
        let estimated_output_bytes = left_delta
            .iter()
            .chain(right_delta)
            .map(|delta| delta.record.len())
            .sum::<usize>();

        let mut output = JoinOutputBuffer {
            bytes: BytesMut::with_capacity(estimated_output_bytes),
            deltas: Vec::new(),
            variable_scratch: Vec::new(),
        };

        // Let's create the context of the Join, with all the descriptors (schema-side description needed to interpret compact record bytes)
        let context = JoinChangeContext {
            left_descriptor,
            right_descriptor,
            output_descriptor,
            output_mapping,
        };

        // Update arrangement
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

        // Replace inputs are faithful full snapshots. Once both arrangements
        // have been rebuilt, one probe produces the complete join result.
        // The incremental identity below would emit that result twice and
        // subtract one copy, only for consolidation to cancel it again.
        if update_mode == ArrangementUpdateMode::Replace {
            append_join_deltas(
                &mut output,
                &context,
                &keyed_left_delta,
                &JoinLookup::Arrangement(right_arrangement.value()),
                JoinProbeSide::LeftDelta,
                1,
            )?;
            let output_buffer = output.bytes.freeze();
            return Ok(consolidate_deltas(
                output
                    .deltas
                    .into_iter()
                    .map(|(record, weight)| RecordDelta {
                        record: output_buffer.slice(record),
                        weight,
                    })
                    .collect(),
            ));
        }

        append_join_deltas(
            &mut output,
            &context,
            &keyed_left_delta,
            &JoinLookup::Arrangement(right_arrangement.value()),
            JoinProbeSide::LeftDelta,
            1,
        )?;
        append_join_deltas(
            &mut output,
            &context,
            &keyed_right_delta,
            &JoinLookup::Arrangement(left_arrangement.value()),
            JoinProbeSide::RightDelta,
            1,
        )?;

        // Both arrangements are now current, so the two probes above each see
        // same-tick left/right pairs. Remove one copy of that cross term.
        let left_delta_index = build_join_delta_index(&keyed_left_delta);
        append_join_deltas(
            &mut output,
            &context,
            &keyed_right_delta,
            &JoinLookup::Index(&left_delta_index),
            JoinProbeSide::RightDelta,
            -1,
        )?;

        let output_buffer = output.bytes.freeze();
        Ok(consolidate_deltas(
            output
                .deltas
                .into_iter()
                .map(|(record, weight)| RecordDelta {
                    record: output_buffer.slice(record),
                    weight,
                })
                .collect(),
        ))
    }
}

impl AntiJoinState {
    #[allow(clippy::too_many_arguments)]
    pub(super) fn apply_semi(
        &self,
        left_arrangement: &mut AsOf<ArrangementState, SubTick>,
        right_arrangement: &mut AsOf<ArrangementState, SubTick>,
        left_descriptor: RecordDescriptor,
        right_descriptor: RecordDescriptor,
        _output_descriptor: &RecordDescriptor,
        left_on: &[String],
        right_on: &[String],
        comparison: ValueComparison,
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

        let keyed_left_delta =
            keyed_join_deltas(&left_descriptor, left_on, left_delta, comparison)?;
        let keyed_right_delta =
            keyed_join_deltas(&right_descriptor, right_on, right_delta, comparison)?;
        let mut affected_keys = HashSet::<JoinKey>::default();
        let mut old_right_counts = HashMap::<JoinKey, i64>::default();
        let mut old_left_buckets = HashMap::<JoinKey, JoinBucket>::default();
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
                    if old_right_count == 0 && new_right_count == 0 {
                        continue;
                    }
                    let old_visible = if old_right_count > 0 {
                        old_left_buckets.get(&key)
                    } else {
                        None
                    };
                    let new_visible = if new_right_count > 0 {
                        left_arrangement.value().bucket(&key)
                    } else {
                        None
                    };
                    append_bucket_diff(&mut deltas, new_visible, old_visible);
                }
            }
            ArrangementUpdateMode::Replace => {
                let mut left_keys = HashSet::<JoinKey>::default();
                for delta in &keyed_left_delta {
                    let key = &delta.key;
                    if left_keys.insert(key.clone()) && right_arrangement.value().key_count(key) > 0
                    {
                        append_bucket(&mut deltas, left_arrangement.value().bucket(key), 1);
                    }
                }
            }
        }

        Ok(consolidate_deltas(deltas))
    }

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
        comparison: ValueComparison,
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

        let keyed_left_delta = keyed_join_deltas(left_descriptor, left_on, left_delta, comparison)?;
        let keyed_right_delta =
            keyed_join_deltas(right_descriptor, right_on, right_delta, comparison)?;
        let mut affected_keys = HashSet::<JoinKey>::default();
        let mut old_right_counts = HashMap::<JoinKey, i64>::default();
        let mut old_left_buckets = HashMap::<JoinKey, JoinBucket>::default();
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
                let mut left_keys = HashSet::<JoinKey>::default();
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
    pub(super) fn clone_keys<'a>(&self, keys: impl IntoIterator<Item = &'a Vec<u8>>) -> Self {
        let mut index = HashMap::default();
        for key in keys {
            let key = JoinKey::from_slice(key);
            if let Some(bucket) = self.bucket(&key) {
                index.insert(key, bucket.clone());
            }
        }
        Self {
            index: Rc::new(index),
            overlay: Rc::default(),
        }
    }

    pub(super) fn replace_keys<'a>(
        &mut self,
        keys: impl IntoIterator<Item = &'a Vec<u8>>,
        replacement: Self,
    ) {
        let overlay = Rc::make_mut(&mut self.overlay);
        for key in keys {
            let key = JoinKey::from_slice(key);
            overlay.insert(key.clone(), replacement.bucket(&key).cloned());
        }
    }

    pub(super) fn row_count(&self) -> usize {
        let mut keys = self.index.keys().cloned().collect::<HashSet<_>>();
        keys.extend(self.overlay.keys().cloned());
        keys.into_iter()
            .filter_map(|key| self.bucket(&key))
            .map(|bucket| bucket.values().filter(|weight| **weight != 0).count())
            .sum()
    }

    pub(super) fn encoded_bytes(&self) -> usize {
        let mut keys = self.index.keys().cloned().collect::<HashSet<_>>();
        keys.extend(self.overlay.keys().cloned());
        keys.into_iter()
            .filter_map(|key| {
                self.bucket(&key).map(|bucket| {
                    (
                        key.len(),
                        bucket.keys().map(|record| record.len()).sum::<usize>(),
                    )
                })
            })
            .map(|(key_len, record_bytes)| key_len + record_bytes)
            .sum()
    }

    fn apply_update(
        &mut self,
        deltas: &[KeyedRecordDelta<'_>],
        update_mode: ArrangementUpdateMode,
    ) {
        match update_mode {
            ArrangementUpdateMode::Accumulate => {
                let mut buckets = HashMap::<JoinKey, JoinBucket>::default();
                for delta in deltas {
                    let bucket = buckets
                        .entry(delta.key.clone())
                        .or_insert_with(|| self.bucket(&delta.key).cloned().unwrap_or_default());
                    let next_weight = bucket.get(&delta.delta.record).copied().unwrap_or_default()
                        + delta.delta.weight;
                    if next_weight == 0 {
                        bucket.remove(&delta.delta.record);
                    } else {
                        bucket.insert(delta.delta.record.clone(), next_weight);
                    }
                }
                let overlay = Rc::make_mut(&mut self.overlay);
                for (key, bucket) in buckets {
                    overlay.insert(key, (!bucket.is_empty()).then_some(bucket));
                }
            }
            ArrangementUpdateMode::Replace => {
                self.index = Rc::new(build_join_delta_index(deltas));
                self.overlay = Rc::default();
            }
        }
    }

    fn key_count(&self, key: &[u8]) -> i64 {
        self.bucket(key)
            .map(|bucket| bucket.values().sum())
            .unwrap_or_default()
    }

    fn bucket(&self, key: &[u8]) -> Option<&JoinBucket> {
        let key = JoinKey::from_slice(key);
        match self.overlay.get(&key) {
            Some(bucket) => bucket.as_ref(),
            None => self.index.get(&key),
        }
    }

    pub(super) fn apply_record_deltas(
        &mut self,
        descriptor: RecordDescriptor,
        fields: &[String],
        deltas: &[RecordDelta],
        update_mode: ArrangementUpdateMode,
    ) -> Result<(), IvmRuntimeError> {
        let keyed = keyed_join_deltas(&descriptor, fields, deltas, ValueComparison::Exact)?;
        self.apply_update(&keyed, update_mode);
        Ok(())
    }

    pub(super) fn records_for_key(&self, key: &[u8]) -> Vec<(Bytes, i64)> {
        self.bucket(key)
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
    output_mapping: &'a [(usize, usize)],
}

/// Builds the changed rows produced by a join.
///
/// All encoded rows are kept next to each other in `bytes`. For example:
///
/// ```text
/// bytes:  [joined row A][joined row B]
/// ranges:       0..20         20..45
/// deltas: (0..20, +1), (20..45, -1)
/// ```
///
/// When the join finishes, `bytes` is frozen once. Each range then becomes the
/// `Bytes` value of one `RecordDelta`. This avoids one allocation per row.
struct JoinOutputBuffer {
    /// All encoded joined rows, stored one after another.
    bytes: BytesMut,
    deltas: Vec<(Range<usize>, i64)>,
    /// Where each row is inside `bytes`, together with its weight.
    ///
    /// For example, `(0..20, 1)` means “the row in bytes `0..20` has weight
    /// `+1`.”
    variable_scratch: Vec<(usize, Range<usize>)>,
}

struct KeyedRecordDelta<'a> {
    delta: &'a RecordDelta,
    key: JoinKey,
}

enum JoinProbeSide {
    LeftDelta,
    RightDelta,
}

fn append_join_deltas(
    output: &mut JoinOutputBuffer,
    context: &JoinChangeContext<'_>,
    delta_records: &[KeyedRecordDelta<'_>],
    stored: &JoinLookup<'_>,
    side: JoinProbeSide,
    sign: i64,
) -> Result<(), IvmRuntimeError> {
    for delta in delta_records {
        if delta.delta.weight == 0 {
            continue;
        }
        let Some(bucket) = stored.bucket(&delta.key) else {
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
                JoinProbeSide::LeftDelta => (delta.delta.raw(), stored_record.as_ref()),
                JoinProbeSide::RightDelta => (stored_record.as_ref(), delta.delta.raw()),
            };
            let record = create_join_record_into(
                left_record,
                right_record,
                context,
                &mut output.bytes,
                &mut output.variable_scratch,
            )?;
            output.deltas.push((record, weight));
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
    let mut index = HashMap::default();
    apply_join_delta_to_index(&mut index, deltas);
    index
}

fn keyed_join_deltas<'a>(
    descriptor: &RecordDescriptor,
    fields: &[String],
    deltas: &'a [RecordDelta],
    comparison: ValueComparison,
) -> Result<Vec<KeyedRecordDelta<'a>>, IvmRuntimeError> {
    if let Some(field_indices) = scalar_join_field_indices(descriptor, fields)? {
        let mut keyed = Vec::with_capacity(deltas.len());
        for delta in deltas {
            let mut key = Vec::new();
            for field_idx in &field_indices {
                let value = descriptor.get_idx(delta.raw(), *field_idx)?;
                encode_join_key_part(&mut key, &value, comparison)?;
            }
            keyed.push(KeyedRecordDelta {
                delta,
                key: JoinKey::from_vec(key),
            });
        }
        return Ok(keyed);
    }

    let mut keyed = Vec::new();
    for delta in deltas {
        for key in join_keys_with_comparison(descriptor, delta.raw(), fields, comparison)? {
            keyed.push(KeyedRecordDelta { delta, key });
        }
    }
    Ok(keyed)
}

fn scalar_join_field_indices(
    descriptor: &RecordDescriptor,
    fields: &[String],
) -> Result<Option<Vec<usize>>, IvmRuntimeError> {
    let mut indices = Vec::with_capacity(fields.len());
    for field in fields {
        let field_idx = descriptor
            .field_index(field)
            .ok_or_else(|| IvmRuntimeError::GraphFieldNotFound(field.clone()))?;
        let descriptor_field = descriptor
            .fields()
            .get(field_idx)
            .ok_or(IvmRuntimeError::GraphFieldIndexOutOfBounds(field_idx))?;
        match &descriptor_field.value_type {
            ValueType::Array(_) => return Ok(None),
            ValueType::Nullable(inner) if matches!(inner.as_ref(), ValueType::Array(_)) => {
                return Ok(None);
            }
            _ => indices.push(field_idx),
        }
    }
    Ok(Some(indices))
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
) -> Result<Vec<JoinKey>, IvmRuntimeError> {
    join_keys_with_comparison(descriptor, record, fields, ValueComparison::Exact)
}

fn join_keys_with_comparison(
    descriptor: &RecordDescriptor,
    record: &[u8],
    fields: &[String],
    comparison: ValueComparison,
) -> Result<Vec<JoinKey>, IvmRuntimeError> {
    if fields.len() == 1 {
        let values = descriptor.get(record, &fields[0])?;
        let parts = join_key_parts(values);
        if parts.is_empty() {
            return Ok(Vec::new());
        }
        if parts.len() == 1 {
            let mut key = Vec::new();
            encode_join_key_part(&mut key, &parts[0], comparison)?;
            return Ok(vec![JoinKey::from_vec(key)]);
        }
        let mut keys = Vec::with_capacity(parts.len());
        let mut seen = HashSet::default();
        for value in &parts {
            let mut key = Vec::new();
            encode_join_key_part(&mut key, value, comparison)?;
            if !seen.contains(&key) {
                seen.insert(key.clone());
                keys.push(JoinKey::from_vec(key));
            }
        }
        return Ok(keys);
    }

    let mut keys = vec![Vec::new()];
    let mut seen = HashSet::default();

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
                encode_join_key_part(&mut next, value, comparison)?;
                if !seen.contains(&next) {
                    seen.insert(next.clone());
                    next_keys.push(next);
                }
            }
        }
        keys = next_keys;
        seen.clear();
    }

    Ok(keys.into_iter().map(JoinKey::from_vec).collect())
}

/// Encode a join key with the requested comparison semantics.
fn encode_join_key_part(
    key: &mut Vec<u8>,
    value: &crate::records::Value,
    comparison: ValueComparison,
) -> Result<(), IvmRuntimeError> {
    if matches!(comparison, ValueComparison::Policy) {
        match value {
            crate::records::Value::Nullable(Some(value)) => {
                return encode_join_key_part(key, value, comparison);
            }
            crate::records::Value::U8(value) => {
                return encode_join_integer_key(key, i128::from(*value));
            }
            crate::records::Value::U16(value) => {
                return encode_join_integer_key(key, i128::from(*value));
            }
            crate::records::Value::U32(value) => {
                return encode_join_integer_key(key, i128::from(*value));
            }
            crate::records::Value::U64(value) => {
                return encode_join_integer_key(key, i128::from(*value));
            }
            crate::records::Value::I32(value) => {
                return encode_join_integer_key(key, i128::from(*value));
            }
            crate::records::Value::I64(value) => {
                return encode_join_integer_key(key, i128::from(*value));
            }
            _ => {}
        }
    }
    encode_key_part(key, value)
}

fn encode_join_integer_key(key: &mut Vec<u8>, value: i128) -> Result<(), IvmRuntimeError> {
    key.push(0xfe);
    key.extend(value.to_be_bytes());
    Ok(())
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
    let mapping = join_output_mapping(left_descriptor, right_descriptor, output_descriptor)?;
    Ok(output_descriptor.project_record_raw(
        &[*left_descriptor, *right_descriptor],
        &[left_record, right_record],
        &mapping,
    )?)
}

fn create_join_record_into(
    left_record: &[u8],
    right_record: &[u8],
    context: &JoinChangeContext<'_>,
    output: &mut BytesMut,
    variable_scratch: &mut Vec<(usize, Range<usize>)>,
) -> Result<Range<usize>, IvmRuntimeError> {
    context
        .output_descriptor
        .project_record_raw_into(
            &[*context.left_descriptor, *context.right_descriptor],
            &[left_record, right_record],
            context.output_mapping,
            output,
            variable_scratch,
        )
        .map_err(IvmRuntimeError::RecordEncoding)
}

pub(super) fn join_output_mapping(
    left_descriptor: &RecordDescriptor,
    right_descriptor: &RecordDescriptor,
    output_descriptor: &RecordDescriptor,
) -> Result<Vec<(usize, usize)>, IvmRuntimeError> {
    output_descriptor
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
        .collect::<Result<Vec<_>, IvmRuntimeError>>()
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use super::*;
    use crate::records::{RecordDescriptor, Value, ValueType};

    // Internal coverage is necessary because this verifies the arrangement-key
    // boundary directly: public query results cannot establish that ordinary
    // IVM keys retain their exact typed encoding.
    #[test]
    fn policy_join_keys_normalize_integer_widths_without_changing_exact_keys() {
        let u32 = RecordDescriptor::new(vec![("value", ValueType::U32)]);
        let i64 = RecordDescriptor::new(vec![("value", ValueType::I64)]);
        let u64 = RecordDescriptor::new(vec![("value", ValueType::U64)]);
        let f64 = RecordDescriptor::new(vec![("value", ValueType::F64)]);
        let fields = vec!["value".to_owned()];

        let u32_record = u32.create(&[Value::U32(7)]).unwrap();
        let i64_record = i64.create(&[Value::I64(7)]).unwrap();
        let large_u64_record = u64.create(&[Value::U64(i64::MAX as u64 + 1)]).unwrap();
        let max_i64_record = i64.create(&[Value::I64(i64::MAX)]).unwrap();
        let float_record = f64.create(&[Value::F64(7.0)]).unwrap();

        assert_eq!(
            join_keys_with_comparison(&u32, &u32_record, &fields, ValueComparison::Policy).unwrap(),
            join_keys_with_comparison(&i64, &i64_record, &fields, ValueComparison::Policy).unwrap(),
            "lowered join correlations match equal integer values across widths"
        );
        assert_ne!(
            join_keys_with_comparison(&u64, &large_u64_record, &fields, ValueComparison::Policy)
                .unwrap(),
            join_keys_with_comparison(&i64, &max_i64_record, &fields, ValueComparison::Policy)
                .unwrap(),
            "U64 above i64::MAX remains exact"
        );
        assert_ne!(
            join_keys_with_comparison(&u32, &u32_record, &fields, ValueComparison::Policy).unwrap(),
            join_keys_with_comparison(&f64, &float_record, &fields, ValueComparison::Policy)
                .unwrap(),
            "integer and float join keys remain type-exact"
        );
        assert_ne!(
            join_keys(&u32, &u32_record, &fields).unwrap(),
            join_keys(&i64, &i64_record, &fields).unwrap(),
            "ordinary arrangement keys retain their exact typed encoding"
        );
    }

    #[test]
    fn replace_join_matches_incremental_multiset_result() {
        let left_descriptor =
            RecordDescriptor::new([("id", ValueType::U64), ("key", ValueType::U64)]);
        let right_descriptor =
            RecordDescriptor::new([("key", ValueType::U64), ("value", ValueType::U64)]);
        let output_descriptor =
            RecordDescriptor::new([("left.id", ValueType::U64), ("right.value", ValueType::U64)]);
        let left = [
            RecordDelta {
                record: Bytes::from(
                    left_descriptor
                        .create(&[Value::U64(1), Value::U64(7)])
                        .expect("encode left row"),
                ),
                weight: 2,
            },
            RecordDelta {
                record: Bytes::from(
                    left_descriptor
                        .create(&[Value::U64(2), Value::U64(7)])
                        .expect("encode left row"),
                ),
                weight: 1,
            },
        ];
        let right = [RecordDelta {
            record: Bytes::from(
                right_descriptor
                    .create(&[Value::U64(7), Value::U64(9)])
                    .expect("encode right row"),
            ),
            weight: 3,
        }];
        let left_on = ["key".to_owned()];
        let right_on = ["key".to_owned()];
        let output_mapping = [(0, 0), (1, 1)];
        let sub_tick = SubTick {
            tick: 1,
            sub_tick: 0,
        };

        let run = |update_mode| {
            JoinState
                .apply(
                    &mut AsOf::default(),
                    &mut AsOf::default(),
                    &left_descriptor,
                    &right_descriptor,
                    &output_descriptor,
                    &output_mapping,
                    &left_on,
                    &right_on,
                    ValueComparison::Exact,
                    &left,
                    &right,
                    sub_tick,
                    sub_tick,
                    update_mode,
                )
                .expect("join snapshots")
                .into_iter()
                .map(|delta| (delta.record.to_vec(), delta.weight))
                .collect::<BTreeMap<_, _>>()
        };

        let incremental = run(ArrangementUpdateMode::Accumulate);
        let replacement = run(ArrangementUpdateMode::Replace);

        assert_eq!(replacement, incremental);
        assert_eq!(replacement.values().copied().collect::<Vec<_>>(), [6, 3]);
    }

    #[test]
    fn arrangement_snapshot_clone_shares_payload_until_first_write() {
        let mut bucket = HashMap::default();
        bucket.insert(Bytes::from_static(b"row-one"), 1);
        let mut index = HashMap::default();
        index.insert(JoinKey::from_slice(b"one"), bucket);
        let original = ArrangementState {
            index: Rc::new(index),
            overlay: Rc::default(),
        };

        let mut prepared = original.clone();
        assert!(
            Rc::ptr_eq(&original.index, &prepared.index),
            "starting an evaluation must not copy resident arrangement rows"
        );

        let mut second_bucket = HashMap::default();
        second_bucket.insert(Bytes::from_static(b"row-two"), 1);
        Rc::make_mut(&mut prepared.overlay)
            .insert(JoinKey::from_slice(b"two"), Some(second_bucket));
        assert!(Rc::ptr_eq(&original.index, &prepared.index));
        assert!(!Rc::ptr_eq(&original.overlay, &prepared.overlay));
        assert_eq!(original.row_count(), 1);
        assert_eq!(prepared.row_count(), 2);
    }
}
