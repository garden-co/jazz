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
use std::sync::{Arc, Weak};

use crate::{
    ivm::{FieldRef, ValueComparison},
    records::{RecordDescriptor, ValueType},
};

use super::{
    ArrangementUpdateMode, AsOf, IvmRuntimeError, RecordDelta, RecordDeltas, SubTick,
    consolidate_deltas, encode_key_part,
    record_projection::{resolve_field_name, resolve_field_ref},
};

pub(super) type JoinKey = SmallVec<[u8; 64]>;

/// A join key commonly owns just one encoded record. Keep that case inside
/// the existing shared bucket allocation, promoting only for distinct records.
#[derive(Clone, Debug, Default)]
enum JoinBucketMap<V> {
    #[default]
    Empty,
    One(Bytes, V),
    Many(HashMap<Bytes, V>),
}

impl<V> JoinBucketMap<V> {
    fn get(&self, record: &Bytes) -> Option<&V> {
        match self {
            Self::Empty => None,
            Self::One(key, value) => (key == record).then_some(value),
            Self::Many(records) => records.get(record),
        }
    }

    fn contains_key(&self, record: &Bytes) -> bool {
        self.get(record).is_some()
    }

    fn is_empty(&self) -> bool {
        match self {
            Self::Empty => true,
            Self::One(..) => false,
            Self::Many(records) => records.is_empty(),
        }
    }

    fn insert(&mut self, record: Bytes, value: V) {
        match self {
            Self::Empty => *self = Self::One(record, value),
            Self::One(key, current) if *key == record => *current = value,
            Self::One(..) => {
                let Self::One(old_record, old_value) = std::mem::take(self) else {
                    unreachable!("matched singleton")
                };
                *self = Self::Many(HashMap::from_iter([
                    (old_record, old_value),
                    (record, value),
                ]));
            }
            Self::Many(records) => {
                records.insert(record, value);
            }
        }
    }

    fn remove(&mut self, record: &Bytes) {
        match self {
            Self::One(key, _) if key == record => *self = Self::Empty,
            Self::Many(records) => {
                records.remove(record);
            }
            _ => {}
        }
    }

    fn iter(&self) -> impl Iterator<Item = (&Bytes, &V)> {
        let (one, many) = match self {
            Self::Empty => (None, None),
            Self::One(record, value) => (Some((record, value)), None),
            Self::Many(records) => (None, Some(records)),
        };
        one.into_iter().chain(many.into_iter().flatten())
    }

    fn into_entries(self) -> impl Iterator<Item = (Bytes, V)> {
        let (one, many) = match self {
            Self::Empty => (None, None),
            Self::One(record, value) => (Some((record, value)), None),
            Self::Many(records) => (None, Some(records)),
        };
        one.into_iter().chain(many.into_iter().flatten())
    }
}

#[derive(Clone, Debug, Default)]
struct JoinBucket {
    base: Rc<JoinBucketMap<i64>>,
    overlay: Rc<JoinBucketMap<Option<i64>>>,
    total_weight: i64,
    record_count: usize,
}

impl JoinBucket {
    #[cfg(test)]
    fn get(&self, record: &Bytes) -> Option<&i64> {
        match self.overlay.get(record) {
            Some(weight) => weight.as_ref(),
            None => self.base.get(record),
        }
    }

    #[cfg(test)]
    fn set(&mut self, record: Bytes, weight: i64) {
        let previous = self.get(&record).copied().unwrap_or_default();
        self.add_weight(&record, weight - previous);
    }

    fn add_weight(&mut self, record: &Bytes, delta: i64) -> i64 {
        // An overlay tombstone means zero, not the weight in the base.
        let overlay = Rc::make_mut(&mut self.overlay);
        let (previous, next) = if let JoinBucketMap::Many(records) = overlay {
            // Preserve one hash probe for the ordinary multi-record path.
            let weight = records
                .entry(record.clone())
                .or_insert_with(|| self.base.get(record).copied());
            let previous = weight.unwrap_or_default();
            let next = previous + delta;
            *weight = (next != 0).then_some(next);
            (previous, next)
        } else {
            let weight = overlay
                .get(record)
                .copied()
                .unwrap_or_else(|| self.base.get(record).copied());
            let previous = weight.unwrap_or_default();
            let next = previous + delta;
            overlay.insert(record.clone(), (next != 0).then_some(next));
            (previous, next)
        };
        self.total_weight += delta;
        self.record_count += usize::from(previous == 0 && next != 0);
        self.record_count -= usize::from(previous != 0 && next == 0);
        next
    }

    fn iter(&self) -> impl Iterator<Item = (&Bytes, &i64)> {
        self.base
            .iter()
            .filter_map(|(record, weight)| {
                (!self.overlay.contains_key(record)).then_some((record, weight))
            })
            .chain(
                self.overlay
                    .iter()
                    .filter_map(|(record, weight)| weight.as_ref().map(|weight| (record, weight))),
            )
    }

    fn is_empty(&self) -> bool {
        self.record_count == 0
    }

    fn commit_overlay(&mut self) {
        if self.overlay.is_empty() {
            return;
        }
        let overlay = std::mem::take(&mut self.overlay);
        let overlay = Rc::try_unwrap(overlay).unwrap_or_else(|overlay| (*overlay).clone());
        let base = Rc::make_mut(&mut self.base);
        for (record, weight) in overlay.into_entries() {
            if let Some(weight) = weight {
                base.insert(record, weight);
            } else {
                base.remove(&record);
            }
        }
    }

    #[cfg(test)]
    fn from_records(records: HashMap<Bytes, i64>) -> Self {
        Self {
            total_weight: records.values().sum(),
            record_count: records.len(),
            base: Rc::new(JoinBucketMap::Many(records)),
            overlay: Rc::default(),
        }
    }
}
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

/// Operator-local progress contains keys, never retained record buckets.
#[derive(Clone, Debug, Default)]
struct VisibilityState {
    visible: Rc<HashSet<JoinKey>>,
    changes: HashMap<JoinKey, bool>,
    consumed: Option<(SubTick, SubTick)>,
}

impl VisibilityState {
    fn contains(&self, key: &JoinKey) -> bool {
        self.changes
            .get(key)
            .copied()
            .unwrap_or_else(|| self.visible.contains(key))
    }

    fn set(&mut self, key: JoinKey, visible: bool) {
        if self.contains(&key) != visible {
            self.changes.insert(key, visible);
        }
    }

    fn commit(&mut self) {
        if self.changes.is_empty() {
            return;
        }
        let visible = Rc::make_mut(&mut self.visible);
        // This is a transaction journal, not a reusable table-sized buffer.
        // drain() leaves hydration-sized capacity behind; the next evaluation
        // then clones that empty allocation when snapshotting operator state.
        // Consume it so a committed view has no journal allocation to clone.
        for (key, present) in std::mem::take(&mut self.changes) {
            if present {
                visible.insert(key);
            } else {
                visible.remove(&key);
            }
        }
    }
}

#[derive(Clone, Debug, Default)]
pub(super) struct AntiJoinState {
    published: VisibilityState,
}

#[derive(Clone, Debug, Default)]
pub(super) struct SemiJoinState {
    published: VisibilityState,
}

#[derive(Clone, Debug, Default)]
pub(super) struct ArrangementState {
    /// Immutable base buckets are shared between evaluator snapshots. Updates
    /// retain only touched buckets in the overlay, rather than cloning the
    /// complete join index.
    index: Rc<JoinIndex>,
    overlay: Rc<HashMap<JoinKey, Option<JoinBucket>>>,
    /// One producer-owned transition, shared read-only by all consumers.
    /// Released at commit; never a retained publication snapshot.
    changes: Option<Rc<JoinIndex>>,
    /// Identity of the last complete immutable input, not a retained payload.
    /// Any content mutation invalidates this proof. A Weak keeps the Arc
    /// allocation identity from being reused without retaining its row vector.
    snapshot: Weak<RecordDeltas>,
}

/// A delta slice may optionally prove the identity of a complete snapshot.
/// Only Replace evaluation uses the identity; incremental updates never do.
pub(super) struct JoinInput<'a> {
    records: &'a [RecordDelta],
    snapshot: Option<&'a Arc<RecordDeltas>>,
}

impl<'a> JoinInput<'a> {
    pub(super) fn deltas(records: &'a [RecordDelta]) -> Self {
        Self {
            records,
            snapshot: None,
        }
    }

    pub(super) fn snapshot(records: &'a Arc<RecordDeltas>) -> Self {
        Self {
            records: &records.deltas,
            snapshot: Some(records),
        }
    }
}

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
    pub(super) fn evaluate_prepared(
        &self,
        left: ArrangementTransition<'_>,
        right: ArrangementTransition<'_>,
        projection: &crate::records::PreparedRecordCopy,
        mode: ArrangementUpdateMode,
    ) -> Result<Vec<RecordDelta>, IvmRuntimeError> {
        let mut output = JoinOutputBuffer {
            bytes: BytesMut::new(),
            deltas: Vec::new(),
        };
        if mode == ArrangementUpdateMode::Replace {
            append_join_index_deltas(
                &mut output,
                projection,
                left.current.buckets(),
                &JoinLookup::Arrangement(right.current),
                JoinProbeSide::LeftDelta,
                1,
            )?;
        } else {
            append_join_index_deltas(
                &mut output,
                projection,
                left.change_buckets(),
                &JoinLookup::Arrangement(right.current),
                JoinProbeSide::LeftDelta,
                1,
            )?;
            append_join_index_deltas(
                &mut output,
                projection,
                right.change_buckets(),
                &JoinLookup::Arrangement(left.current),
                JoinProbeSide::RightDelta,
                1,
            )?;
            // Both current indexes include this transition. Remove the repeated
            // cross term, borrowing the producer batch rather than rebuilding it.
            if let Some(left_changes) = left.changes {
                append_join_index_deltas(
                    &mut output,
                    projection,
                    right.change_buckets(),
                    &JoinLookup::Index(left_changes),
                    JoinProbeSide::RightDelta,
                    -1,
                )?;
            }
        }
        Ok(output.finish())
    }
}

/// A first-result consumer will never apply a subsequent delta. Stream its
/// left records through the right lookup without hashing/storing each complete
/// left record in a second arrangement. Consolidation preserves multiplicities
/// when a left record appears more than once or expands to several array keys.
pub(super) fn join_snapshot_with_right_arrangement(
    left: &RecordDeltas,
    left_keys: &[String],
    comparison: ValueComparison,
    right: &ArrangementState,
    projection: &crate::records::PreparedRecordCopy,
) -> Result<Vec<RecordDelta>, IvmRuntimeError> {
    let mut output = JoinOutputBuffer {
        bytes: BytesMut::new(),
        deltas: Vec::new(),
    };
    for keyed in keyed_join_deltas(&left.descriptor, left_keys, &left.deltas, comparison)? {
        let Some(bucket) = right.bucket(&keyed.key) else {
            continue;
        };
        for (record, weight) in bucket.iter() {
            let weight = keyed.delta.weight * weight;
            if weight != 0 {
                let range = projection
                    .project_into(&[keyed.delta.raw(), record.as_ref()], &mut output.bytes)?;
                output.deltas.push((range, weight));
            }
        }
    }
    Ok(output.finish())
}

/// Snapshot semi/anti joins need neither left buckets nor the visibility state
/// used to retract rows after a future change to the right-hand relation.
pub(super) fn threshold_snapshot_with_right_arrangement(
    left: &RecordDeltas,
    left_keys: &[String],
    comparison: ValueComparison,
    right: &ArrangementState,
    semi: bool,
) -> Result<Vec<RecordDelta>, IvmRuntimeError> {
    let mut output = Vec::new();
    for keyed in keyed_join_deltas(&left.descriptor, left_keys, &left.deltas, comparison)? {
        let count = right.key_count(&keyed.key);
        if if semi { count > 0 } else { count == 0 } {
            output.push(keyed.delta.clone());
        }
    }
    Ok(consolidate_deltas(output))
}

/// A consumer borrows one aligned input version and its producer's delta.
#[cfg(test)]
impl JoinState {
    #[allow(clippy::too_many_arguments)]
    fn apply(
        &self,
        left: &mut AsOf<ArrangementState, SubTick>,
        right: &mut AsOf<ArrangementState, SubTick>,
        ld: &RecordDescriptor,
        rd: &RecordDescriptor,
        output: &RecordDescriptor,
        mapping: &[(usize, usize)],
        lk: &[String],
        rk: &[String],
        comparison: ValueComparison,
        li: JoinInput<'_>,
        ri: JoinInput<'_>,
        lt: SubTick,
        rt: SubTick,
        mode: ArrangementUpdateMode,
    ) -> Result<Vec<RecordDelta>, IvmRuntimeError> {
        prepare_arrangement(
            left,
            ld,
            lk,
            comparison,
            JoinInput::deltas(li.records),
            lt,
            mode,
        )?;
        prepare_arrangement(
            right,
            rd,
            rk,
            comparison,
            JoinInput::deltas(ri.records),
            rt,
            mode,
        )?;
        let projection = crate::records::PreparedRecordCopy::new(&[*ld, *rd], *output, mapping)?;
        self.evaluate_prepared(
            ArrangementTransition::at(left, lt),
            ArrangementTransition::at(right, rt),
            &projection,
            mode,
        )
    }
}

/// A consumer borrows one aligned input version and its producer's delta.
/// A scoped input not advanced at this frontier contributes no new changes.
#[derive(Clone, Copy)]
pub(super) struct ArrangementTransition<'a> {
    current: &'a ArrangementState,
    changes: Option<&'a JoinIndex>,
    frontier: SubTick,
}

impl<'a> ArrangementTransition<'a> {
    pub(super) fn at(state: &'a AsOf<ArrangementState, SubTick>, expected: SubTick) -> Self {
        Self {
            current: state.value(),
            changes: if state.as_of() == Some(expected) {
                state.value().changes.as_deref()
            } else {
                None
            },
            frontier: expected,
        }
    }

    fn changed_keys(&self) -> impl Iterator<Item = &JoinKey> {
        self.changes.into_iter().flat_map(|changes| changes.keys())
    }

    fn change_buckets(&self) -> impl Iterator<Item = (&JoinKey, &JoinBucket)> {
        self.changes.into_iter().flat_map(|changes| changes.iter())
    }

    fn delta_bucket(&self, key: &JoinKey) -> Option<&JoinBucket> {
        self.changes.and_then(|changes| changes.get(key))
    }
}

fn threshold_transition(
    published: &mut VisibilityState,
    left: ArrangementTransition<'_>,
    right: ArrangementTransition<'_>,
    replace: bool,
    semi: bool,
) -> Vec<RecordDelta> {
    let frontier = (left.frontier, right.frontier);
    if !replace && published.consumed == Some(frontier) {
        return Vec::new();
    }
    let mut affected = HashSet::default();
    if replace {
        *published = VisibilityState::default();
        affected.extend(left.current.index.keys().cloned());
        affected.extend(left.current.overlay.keys().cloned());
    } else {
        affected.extend(left.changed_keys().cloned());
        affected.extend(right.changed_keys().cloned());
    }
    let mut output = Vec::new();
    for key in affected {
        let before = !replace && published.contains(&key);
        let count = right.current.key_count(&key);
        let after = if semi { count > 0 } else { count == 0 };
        match (before, after) {
            (true, true) => append_bucket(&mut output, left.delta_bucket(&key), 1),
            (false, false) => {}
            (false, true) => append_bucket(&mut output, left.current.bucket(&key), 1),
            (true, false) => {
                // -L_before = -L_after + delta_L. This also handles simultaneous
                // additions/retractions on both sides without retaining L_before.
                append_bucket(&mut output, left.current.bucket(&key), -1);
                append_bucket(&mut output, left.delta_bucket(&key), 1);
            }
        }
        published.set(key.clone(), after && left.current.bucket(&key).is_some());
    }
    published.consumed = Some(frontier);
    consolidate_deltas(output)
}

/// The recursive snapshot interpreter has no retained indexes. Produce two
/// temporary snapshot arrangements, then use the same read-only join kernel.
pub(super) fn threshold_snapshot(
    left: &RecordDeltas,
    right: &RecordDeltas,
    left_keys: &[String],
    right_keys: &[String],
    comparison: ValueComparison,
    semi: bool,
) -> Result<Vec<RecordDelta>, IvmRuntimeError> {
    let stamp = SubTick {
        tick: 0,
        sub_tick: 0,
    };
    let mut l = AsOf::default();
    let mut r = AsOf::default();
    prepare_arrangement(
        &mut l,
        &left.descriptor,
        left_keys,
        comparison,
        JoinInput::deltas(&left.deltas),
        stamp,
        ArrangementUpdateMode::Replace,
    )?;
    prepare_arrangement(
        &mut r,
        &right.descriptor,
        right_keys,
        comparison,
        JoinInput::deltas(&right.deltas),
        stamp,
        ArrangementUpdateMode::Replace,
    )?;
    Ok(threshold_transition(
        &mut VisibilityState::default(),
        ArrangementTransition::at(&l, stamp),
        ArrangementTransition::at(&r, stamp),
        true,
        semi,
    ))
}

impl SemiJoinState {
    pub(super) fn commit_published_overlay(&mut self) {
        self.published.commit();
    }

    pub(super) fn evaluate(
        &mut self,
        left: ArrangementTransition<'_>,
        right: ArrangementTransition<'_>,
        mode: ArrangementUpdateMode,
    ) -> Vec<RecordDelta> {
        threshold_transition(
            &mut self.published,
            left,
            right,
            mode == ArrangementUpdateMode::Replace,
            true,
        )
    }
}

impl AntiJoinState {
    pub(super) fn commit_published_overlay(&mut self) {
        self.published.commit();
    }

    pub(super) fn evaluate(
        &mut self,
        left: ArrangementTransition<'_>,
        right: ArrangementTransition<'_>,
        mode: ArrangementUpdateMode,
    ) -> Vec<RecordDelta> {
        threshold_transition(
            &mut self.published,
            left,
            right,
            mode == ArrangementUpdateMode::Replace,
            false,
        )
    }
}

#[cfg(test)]
impl SemiJoinState {
    #[allow(clippy::too_many_arguments)]
    fn apply(
        &mut self,
        left: &mut AsOf<ArrangementState, SubTick>,
        right: &mut AsOf<ArrangementState, SubTick>,
        ld: RecordDescriptor,
        rd: RecordDescriptor,
        _output: &RecordDescriptor,
        lk: &[String],
        rk: &[String],
        comparison: ValueComparison,
        li: JoinInput<'_>,
        ri: JoinInput<'_>,
        lt: SubTick,
        rt: SubTick,
        mode: ArrangementUpdateMode,
    ) -> Result<Vec<RecordDelta>, IvmRuntimeError> {
        prepare_arrangement(left, &ld, lk, comparison, li, lt, mode)?;
        prepare_arrangement(right, &rd, rk, comparison, ri, rt, mode)?;
        Ok(self.evaluate(
            ArrangementTransition::at(left, lt),
            ArrangementTransition::at(right, rt),
            mode,
        ))
    }
}

#[cfg(test)]
impl AntiJoinState {
    #[allow(clippy::too_many_arguments)]
    fn apply(
        &mut self,
        left: &mut AsOf<ArrangementState, SubTick>,
        right: &mut AsOf<ArrangementState, SubTick>,
        ld: &RecordDescriptor,
        rd: &RecordDescriptor,
        _output: &RecordDescriptor,
        lk: &[String],
        rk: &[String],
        comparison: ValueComparison,
        li: JoinInput<'_>,
        ri: JoinInput<'_>,
        lt: SubTick,
        rt: SubTick,
        mode: ArrangementUpdateMode,
    ) -> Result<Vec<RecordDelta>, IvmRuntimeError> {
        prepare_arrangement(left, ld, lk, comparison, li, lt, mode)?;
        prepare_arrangement(right, rd, rk, comparison, ri, rt, mode)?;
        Ok(self.evaluate(
            ArrangementTransition::at(left, lt),
            ArrangementTransition::at(right, rt),
            mode,
        ))
    }
}

impl ArrangementState {
    fn buckets(&self) -> impl Iterator<Item = (&JoinKey, &JoinBucket)> {
        self.index
            .iter()
            .filter(|(key, _)| !self.overlay.contains_key(*key))
            .chain(
                self.overlay
                    .iter()
                    .filter_map(|(key, bucket)| bucket.as_ref().map(|bucket| (key, bucket))),
            )
    }
    /// Fold only the touched buckets into the shared base at tick commit.
    /// Callers drop the previous live arrangement before invoking this method,
    /// making both COW maps uniquely owned in the common path.
    pub(super) fn commit_overlay(&mut self) {
        self.changes = None;
        if self.overlay.is_empty() {
            return;
        }
        let overlay = std::mem::take(&mut self.overlay);
        let overlay = Rc::try_unwrap(overlay).unwrap_or_else(|overlay| (*overlay).clone());
        let index = Rc::make_mut(&mut self.index);
        for (key, bucket) in overlay {
            // Release the previous bucket's ownership before checking whether
            // the new bucket can fold its record deltas without copying history.
            index.remove(&key);
            if let Some(mut bucket) = bucket {
                // A retained evaluator/publication snapshot may still own
                // either map. Preserve sharing rather than copy a full base
                // (or accumulated overlay) just to compact it.
                if Rc::strong_count(&bucket.base) == 1 && Rc::strong_count(&bucket.overlay) == 1 {
                    bucket.commit_overlay();
                }
                index.insert(key, bucket);
            }
        }
    }

    #[cfg(test)]
    fn replace_bucket(&mut self, key: JoinKey, bucket: Option<JoinBucket>) {
        self.snapshot = Weak::new();
        Rc::make_mut(&mut self.overlay).insert(key, bucket);
    }

    #[cfg(test)]
    fn clear(&mut self) {
        *self = Self::default();
    }

    #[cfg(test)]
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
            changes: None,
            snapshot: Weak::new(),
        }
    }

    #[cfg(test)]
    pub(super) fn replace_keys<'a>(
        &mut self,
        keys: impl IntoIterator<Item = &'a Vec<u8>>,
        replacement: Self,
    ) {
        self.snapshot = Weak::new();
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
            .map(|bucket| bucket.iter().filter(|(_, weight)| **weight != 0).count())
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
                        bucket.iter().map(|(record, _)| record.len()).sum::<usize>(),
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
        self.snapshot = Weak::new();
        // An empty batch still advances the producer's timestamp, but must not
        // retain the previous transition or allocate an empty shared index.
        self.changes = (!deltas.is_empty()).then(|| Rc::new(build_join_delta_index(deltas)));
        match update_mode {
            ArrangementUpdateMode::Accumulate => {
                if deltas.is_empty() {
                    return;
                }
                let index = &self.index;
                let overlay = Rc::make_mut(&mut self.overlay);
                for delta in deltas {
                    // A present None is an overlay tombstone, not permission
                    // to resurrect the base. Clone a base bucket only on the
                    // first touch; an already-owned overlay can mutate in place.
                    let slot = overlay
                        .entry(delta.key.clone())
                        .or_insert_with(|| index.get(&delta.key).cloned());
                    let bucket = slot.get_or_insert_with(JoinBucket::default);
                    bucket.add_weight(&delta.delta.record, delta.delta.weight);
                    if bucket.is_empty() {
                        *slot = None;
                    }
                }
            }
            ArrangementUpdateMode::Replace => {
                self.index = self.changes.clone().unwrap_or_default();
                self.overlay = Rc::default();
            }
        }
    }

    fn key_count(&self, key: &[u8]) -> i64 {
        self.bucket(key)
            .map(|bucket| bucket.total_weight)
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

fn reuses_snapshot(
    arrangement: &AsOf<ArrangementState, SubTick>,
    snapshot: Option<&Arc<RecordDeltas>>,
    sub_tick: SubTick,
    update_mode: ArrangementUpdateMode,
) -> bool {
    update_mode == ArrangementUpdateMode::Replace
        && arrangement.as_of() == Some(sub_tick)
        && snapshot.is_some_and(|snapshot| {
            std::ptr::eq(arrangement.value().snapshot.as_ptr(), Arc::as_ptr(snapshot))
        })
}

pub(super) fn prepare_arrangement(
    state: &mut AsOf<ArrangementState, SubTick>,
    descriptor: &RecordDescriptor,
    fields: &[String],
    comparison: ValueComparison,
    input: JoinInput<'_>,
    stamp: SubTick,
    mode: ArrangementUpdateMode,
) -> Result<(), IvmRuntimeError> {
    if reuses_snapshot(state, input.snapshot, stamp, mode) {
        return Ok(());
    }
    let keyed = keyed_join_deltas(descriptor, fields, input.records, comparison)?;
    advance_arrangement(state, &keyed, stamp, mode, input.snapshot)
}

fn advance_arrangement(
    arrangement: &mut AsOf<ArrangementState, SubTick>,
    deltas: &[KeyedRecordDelta<'_>],
    sub_tick: SubTick,
    update_mode: ArrangementUpdateMode,
    snapshot: Option<&Arc<RecordDeltas>>,
) -> Result<(), IvmRuntimeError> {
    if reuses_snapshot(arrangement, snapshot, sub_tick, update_mode) {
        #[cfg(feature = "cold-settle-attribution")]
        if std::env::var_os("GROOVE_TRACE_ARRANGEMENT_SNAPSHOTS").is_some() {
            eprintln!(
                "ARRANGEMENT_REUSE\t{}",
                snapshot.expect("matched snapshot").deltas.len()
            );
        }
        return Ok(());
    }
    if update_mode == ArrangementUpdateMode::Accumulate && arrangement.as_of() == Some(sub_tick) {
        return Ok(());
    }
    // Without exact snapshot identity, Replace callers intentionally rebuild
    // even when the stamp already matches this logical time.
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
    if update_mode == ArrangementUpdateMode::Replace {
        arrangement.value_mut().snapshot = snapshot.map(Arc::downgrade).unwrap_or_default();
    }
    if replace_within_same_tick {
        arrangement.replace_as_of_at_least(sub_tick);
    } else {
        arrangement.mark_forward_as_of(sub_tick)?;
    }
    Ok(())
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
    /// Where each row is inside `bytes`, together with its weight.
    ///
    /// For example, `(0..20, 1)` means “the row in bytes `0..20` has weight
    /// `+1`.”
    deltas: Vec<(Range<usize>, i64)>,
}

impl JoinOutputBuffer {
    fn finish(self) -> Vec<RecordDelta> {
        let bytes = self.bytes.freeze();
        consolidate_deltas(
            self.deltas
                .into_iter()
                .map(|(range, weight)| RecordDelta {
                    record: bytes.slice(range),
                    weight,
                })
                .collect(),
        )
    }
}

struct KeyedRecordDelta<'a> {
    delta: &'a RecordDelta,
    key: JoinKey,
}

enum JoinProbeSide {
    LeftDelta,
    RightDelta,
}

fn append_join_index_deltas<'a>(
    output: &mut JoinOutputBuffer,
    projection: &crate::records::PreparedRecordCopy,
    changes: impl Iterator<Item = (&'a JoinKey, &'a JoinBucket)>,
    stored: &JoinLookup<'_>,
    side: JoinProbeSide,
    sign: i64,
) -> Result<(), IvmRuntimeError> {
    for (key, changed) in changes {
        let Some(bucket) = stored.bucket(key) else {
            continue;
        };
        for (changed_record, changed_weight) in changed.iter() {
            for (stored_record, stored_weight) in bucket.iter() {
                let weight = sign * changed_weight * stored_weight;
                if weight == 0 {
                    continue;
                }
                let (left_record, right_record) = match side {
                    JoinProbeSide::LeftDelta => (changed_record.as_ref(), stored_record.as_ref()),
                    JoinProbeSide::RightDelta => (stored_record.as_ref(), changed_record.as_ref()),
                };
                let record =
                    projection.project_into(&[left_record, right_record], &mut output.bytes)?;
                output.deltas.push((record, weight));
            }
        }
    }
    Ok(())
}

fn apply_join_delta_to_index(index: &mut JoinIndex, deltas: &[KeyedRecordDelta<'_>]) {
    for delta in deltas {
        let bucket = index.entry(delta.key.clone()).or_default();
        let next_weight = bucket.add_weight(&delta.delta.record, delta.delta.weight);
        if next_weight == 0 && bucket.is_empty() {
            index.remove(&delta.key);
        }
    }
}

fn build_join_delta_index(deltas: &[KeyedRecordDelta<'_>]) -> JoinIndex {
    let mut index = HashMap::default();
    apply_join_delta_to_index(&mut index, deltas);
    for bucket in index.values_mut() {
        bucket.commit_overlay();
    }
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
        // Short keys are retained inline by JoinKey. Reuse the temporary encoder
        // buffer instead of allocating and discarding it for every input row.
        let mut key = Vec::new();
        for delta in deltas {
            key.clear();
            for field_idx in &field_indices {
                let value = descriptor.get_idx(delta.raw(), *field_idx)?;
                encode_join_key_part(&mut key, &value, comparison)?;
            }
            keyed.push(KeyedRecordDelta {
                delta,
                key: JoinKey::from_slice(&key),
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
        let field_idx = resolve_field_name(descriptor, field)
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

#[cfg(test)]
thread_local! {
    static APPENDED_BUCKET_RECORDS: std::cell::Cell<usize> = const { std::cell::Cell::new(0) };
}

fn append_bucket(deltas: &mut Vec<RecordDelta>, bucket: Option<&JoinBucket>, sign: i64) {
    let Some(bucket) = bucket else {
        return;
    };
    for (record, weight) in bucket.iter() {
        #[cfg(test)]
        APPENDED_BUCKET_RECORDS.with(|count| count.set(count.get() + 1));
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

pub(super) fn join_keys(
    descriptor: &RecordDescriptor,
    record: &[u8],
    fields: &[String],
    comparison: ValueComparison,
) -> Result<Vec<JoinKey>, IvmRuntimeError> {
    join_keys_with_comparison(descriptor, record, fields, comparison)
}

fn join_keys_with_comparison(
    descriptor: &RecordDescriptor,
    record: &[u8],
    fields: &[String],
    comparison: ValueComparison,
) -> Result<Vec<JoinKey>, IvmRuntimeError> {
    if fields.len() == 1 {
        let field_idx = resolve_field_name(descriptor, &fields[0])
            .ok_or_else(|| IvmRuntimeError::GraphFieldNotFound(fields[0].clone()))?;
        let values = descriptor.bind(record).get_idx(field_idx)?;
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
        let field_idx = resolve_field_name(descriptor, field)
            .ok_or_else(|| IvmRuntimeError::GraphFieldNotFound(field.clone()))?;
        let values = descriptor.bind(record).get_idx(field_idx)?;
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
                let field_idx = resolve_field_ref(left_descriptor, &FieldRef::stored_name(name))?;
                Ok((0, field_idx))
            } else if let Some(name) = name.strip_prefix("right.") {
                let field_idx = resolve_field_ref(right_descriptor, &FieldRef::stored_name(name))?;
                Ok((1, field_idx))
            } else {
                Err(IvmRuntimeError::GraphFieldNotFound(name.to_owned()))
            }
        })
        .collect::<Result<Vec<_>, IvmRuntimeError>>()
}

#[cfg(test)]
mod tests {
    // Public delta tests cover the results; only a private test can distinguish
    // an empty journal from an empty journal retaining hydration-sized buckets.
    #[test]
    fn committed_visibility_discards_journal_capacity_and_preserves_snapshots() {
        let keys = (0..5_000u64)
            .map(|value| super::JoinKey::from_slice(&value.to_le_bytes()))
            .collect::<Vec<_>>();
        let mut live = super::VisibilityState::default();
        for key in &keys {
            live.set(key.clone(), true);
        }
        live.commit();
        assert_eq!(live.changes.capacity(), 0);
        let original = live.clone();
        assert_eq!(original.changes.capacity(), 0);
        live.set(keys[0].clone(), false);
        let staged = live.clone();
        live.commit();
        assert_eq!(live.changes.capacity(), 0);
        assert!(original.contains(&keys[0]));
        assert!(!staged.contains(&keys[0]));
        assert!(!live.contains(&keys[0]));
        for key in &keys[1..] {
            assert!(live.contains(key));
        }
        live.set(keys[0].clone(), true);
        live.commit();
        assert!(live.contains(&keys[0]));
        assert!(!staged.contains(&keys[0]));
        assert_eq!(live.changes.capacity(), 0);
    }

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
            join_keys(&u32, &u32_record, &fields, ValueComparison::Exact).unwrap(),
            join_keys(&i64, &i64_record, &fields, ValueComparison::Exact).unwrap(),
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
                    JoinInput::deltas(&left),
                    JoinInput::deltas(&right),
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
    fn semi_join_publishes_recursive_right_threshold_after_shared_arrangement_advances() {
        let descriptor = RecordDescriptor::new([("id", ValueType::U64), ("route", ValueType::U64)]);
        let keys = ["id".to_owned(), "route".to_owned()];
        let record = Bytes::from(
            descriptor
                .create(&[Value::U64(7), Value::U64(11)])
                .expect("encode routed row"),
        );
        let left_snapshot = [RecordDelta {
            record: record.clone(),
            weight: 1,
        }];
        let right_add = [RecordDelta {
            record: record.clone(),
            weight: 1,
        }];
        let right_remove = [RecordDelta {
            record: record.clone(),
            weight: -1,
        }];
        let hydrated = SubTick {
            tick: 1,
            sub_tick: 0,
        };
        let added = SubTick {
            tick: 2,
            sub_tick: 0,
        };
        let removed = SubTick {
            tick: 3,
            sub_tick: 0,
        };
        let mut state = SemiJoinState::default();
        let mut left = AsOf::default();
        let mut right = AsOf::default();

        assert!(
            state
                .apply(
                    &mut left,
                    &mut right,
                    descriptor,
                    descriptor,
                    &descriptor,
                    &keys,
                    &keys,
                    ValueComparison::Exact,
                    JoinInput::deltas(&left_snapshot),
                    JoinInput::deltas(&[]),
                    hydrated,
                    hydrated,
                    ArrangementUpdateMode::Replace,
                )
                .expect("hydrate unmatched left row")
                .is_empty()
        );

        // A recursive/provenance consumer can advance the shared right
        // arrangement before this semi-join node is evaluated.
        right
            .value_mut()
            .apply_record_deltas(
                descriptor,
                &keys,
                &right_add,
                ArrangementUpdateMode::Accumulate,
            )
            .expect("advance shared recursive arrangement");
        right.mark_forward_as_of(added).expect("mark shared add");
        assert_eq!(
            state
                .apply(
                    &mut left,
                    &mut right,
                    descriptor,
                    descriptor,
                    &descriptor,
                    &keys,
                    &keys,
                    ValueComparison::Exact,
                    JoinInput::deltas(&[]),
                    JoinInput::deltas(&right_add),
                    added,
                    added,
                    ArrangementUpdateMode::Accumulate,
                )
                .expect("publish routed recursive add"),
            [RecordDelta {
                record: record.clone(),
                weight: 1,
            }]
        );

        right
            .value_mut()
            .apply_record_deltas(
                descriptor,
                &keys,
                &right_remove,
                ArrangementUpdateMode::Accumulate,
            )
            .expect("advance shared recursive arrangement");
        right
            .mark_forward_as_of(removed)
            .expect("mark shared removal");
        assert_eq!(
            state
                .apply(
                    &mut left,
                    &mut right,
                    descriptor,
                    descriptor,
                    &descriptor,
                    &keys,
                    &keys,
                    ValueComparison::Exact,
                    JoinInput::deltas(&[]),
                    JoinInput::deltas(&right_remove),
                    removed,
                    removed,
                    ArrangementUpdateMode::Accumulate,
                )
                .expect("publish routed recursive removal"),
            [RecordDelta { record, weight: -1 }]
        );
    }

    #[test]
    fn anti_join_does_not_retract_rows_only_another_consumer_arranged() {
        let descriptor = RecordDescriptor::new([("id", ValueType::U64), ("route", ValueType::U64)]);
        let keys = ["id".to_owned(), "route".to_owned()];
        let record = Bytes::from(
            descriptor
                .create(&[Value::U64(7), Value::U64(11)])
                .expect("encode routed row"),
        );
        let left_add = [RecordDelta {
            record: record.clone(),
            weight: 1,
        }];
        let right_add = [RecordDelta { record, weight: 1 }];
        let shared_left_tick = SubTick {
            tick: 2,
            sub_tick: 0,
        };
        let anti_join_tick = SubTick {
            tick: 2,
            sub_tick: 1,
        };
        let mut state = AntiJoinState::default();
        let mut left = AsOf::<ArrangementState, SubTick>::default();
        let mut right = AsOf::<ArrangementState, SubTick>::default();

        // Another terminal consuming the same source can advance the shared
        // left arrangement before this anti-join is evaluated. That does not
        // mean this anti-join has ever published the left row.
        left.value_mut()
            .apply_record_deltas(
                descriptor,
                &keys,
                &left_add,
                ArrangementUpdateMode::Accumulate,
            )
            .expect("advance shared left arrangement");
        left.mark_forward_as_of(shared_left_tick)
            .expect("mark shared left advance");

        assert!(
            state
                .apply(
                    &mut left,
                    &mut right,
                    &descriptor,
                    &descriptor,
                    &descriptor,
                    &keys,
                    &keys,
                    ValueComparison::Exact,
                    JoinInput::deltas(&[]),
                    JoinInput::deltas(&right_add),
                    anti_join_tick,
                    anti_join_tick,
                    ArrangementUpdateMode::Accumulate,
                )
                .expect("apply blocker after shared left advance")
                .is_empty(),
            "a blocker suppresses an unpubished row instead of retracting it"
        );
    }

    #[test]
    fn snapshot_identity_reuses_only_unchanged_complete_arrangements() {
        // Internal: public query results cannot prove that an index allocation
        // was reused, or that the identity proof does not retain input vectors.
        let descriptor = RecordDescriptor::new([("id", ValueType::U64)]);
        let fields = ["id".to_owned()];
        let make_snapshot = |entries: &[(u64, i64)]| {
            Arc::new(RecordDeltas {
                descriptor,
                deltas: entries
                    .iter()
                    .map(|(id, weight)| RecordDelta {
                        record: descriptor.create(&[Value::U64(*id)]).unwrap().into(),
                        weight: *weight,
                    })
                    .collect(),
            })
        };
        let tick = SubTick {
            tick: 1,
            sub_tick: 0,
        };
        let install = |state: &mut AsOf<ArrangementState, SubTick>,
                       snapshot: &Arc<RecordDeltas>| {
            let keyed = keyed_join_deltas(
                &descriptor,
                &fields,
                &snapshot.deltas,
                ValueComparison::Exact,
            )
            .unwrap();
            advance_arrangement(
                state,
                &keyed,
                tick,
                ArrangementUpdateMode::Replace,
                Some(snapshot),
            )
            .unwrap();
        };
        let snapshot = make_snapshot(&[(1, 2), (1, -1), (2, -3)]);
        let mut state = AsOf::default();
        install(&mut state, &snapshot);
        assert_eq!(
            state.value().row_count(),
            2,
            "negative weights remain represented"
        );
        let original_index = Rc::clone(&state.value().index);
        install(&mut state, &Arc::clone(&snapshot));
        assert!(Rc::ptr_eq(&original_index, &state.value().index));

        let mut staged = state.clone();
        let changed = make_snapshot(&[(3, 1)]);
        install(&mut staged, &changed);
        assert!(!Rc::ptr_eq(&original_index, &staged.value().index));
        assert_eq!(staged.value().row_count(), 1);
        assert_eq!(
            state.value().row_count(),
            2,
            "earlier snapshot stays unchanged"
        );

        let equal_but_distinct = make_snapshot(&[(1, 2), (1, -1), (2, -3)]);
        install(&mut state, &equal_but_distinct);
        assert!(
            !Rc::ptr_eq(&original_index, &state.value().index),
            "equal contents alone are not an identity proof"
        );
        let before_delta = Rc::clone(&state.value().index);
        let addition = make_snapshot(&[(4, 1)]);
        state
            .value_mut()
            .apply_record_deltas(
                descriptor,
                &fields,
                &addition.deltas,
                ArrangementUpdateMode::Accumulate,
            )
            .unwrap();
        assert_eq!(state.value().row_count(), 3);
        assert!(!reuses_snapshot(
            &state,
            Some(&equal_but_distinct),
            tick,
            ArrangementUpdateMode::Replace
        ));
        install(&mut state, &equal_but_distinct);
        assert_eq!(state.value().row_count(), 2);
        assert!(!Rc::ptr_eq(&before_delta, &state.value().index));
        assert!(!reuses_snapshot(
            &state,
            Some(&equal_but_distinct),
            SubTick {
                tick: 2,
                sub_tick: 0
            },
            ArrangementUpdateMode::Replace
        ));

        drop(equal_but_distinct);
        assert!(
            state.value().snapshot.upgrade().is_none(),
            "arrangement identity must not retain the input row vector"
        );
    }

    #[test]
    fn snapshot_identity_is_invalidated_by_partial_replacement() {
        // Internal: directly exercise every non-join mutation boundary that
        // could otherwise preserve a stale identity proof on a partial index.
        let descriptor = RecordDescriptor::new([("id", ValueType::U64)]);
        let fields = ["id".to_owned()];
        let record = Bytes::from(descriptor.create(&[Value::U64(1)]).unwrap());
        let snapshot = Arc::new(RecordDeltas {
            descriptor,
            deltas: vec![RecordDelta { record, weight: 1 }],
        });
        let keyed = keyed_join_deltas(
            &descriptor,
            &fields,
            &snapshot.deltas,
            ValueComparison::Exact,
        )
        .unwrap();
        let key = keyed[0].key.clone();
        let tick = SubTick {
            tick: 1,
            sub_tick: 0,
        };
        let mut state = AsOf::default();
        advance_arrangement(
            &mut state,
            &keyed,
            tick,
            ArrangementUpdateMode::Replace,
            Some(&snapshot),
        )
        .unwrap();
        let original = state.clone();
        assert!(reuses_snapshot(
            &state,
            Some(&snapshot),
            tick,
            ArrangementUpdateMode::Replace
        ));
        state.value_mut().replace_bucket(key.clone(), None);
        assert!(!reuses_snapshot(
            &state,
            Some(&snapshot),
            tick,
            ArrangementUpdateMode::Replace
        ));
        let keys = [key.to_vec()];
        let partial = original.value().clone_keys(keys.iter());
        assert!(partial.snapshot.upgrade().is_none());
        state = original.clone();
        state
            .value_mut()
            .replace_keys(keys.iter(), ArrangementState::default());
        assert!(!reuses_snapshot(
            &state,
            Some(&snapshot),
            tick,
            ArrangementUpdateMode::Replace
        ));
        assert_eq!(state.value().row_count(), 0);
        state = original;
        state.value_mut().clear();
        assert!(!reuses_snapshot(
            &state,
            Some(&snapshot),
            tick,
            ArrangementUpdateMode::Replace
        ));
    }

    #[test]
    fn arrangement_commit_folds_owned_bucket_history_without_copying_base() {
        // Internal work-bound proof: public result assertions cannot detect
        // a growing staged map or a copy of the resident bucket on each tick.
        let key = JoinKey::from_slice(b"history");
        let bucket = JoinBucket::from_records(
            (0..2048)
                .map(|i| (Bytes::from(format!("old-{i}")), 1))
                .collect(),
        );
        let base_ptr = Rc::as_ptr(&bucket.base);
        let mut live = ArrangementState {
            index: Rc::new(HashMap::from_iter([(key.clone(), bucket)])),
            ..ArrangementState::default()
        };
        for i in 0..64 {
            let delta = RecordDelta {
                record: Bytes::from(format!("new-{i}")),
                weight: 1,
            };
            let mut staged = live.clone();
            staged.apply_update(
                &[KeyedRecordDelta {
                    delta: &delta,
                    key: key.clone(),
                }],
                ArrangementUpdateMode::Accumulate,
            );
            assert_eq!(live.bucket(&key).unwrap().get(&delta.record), None);
            drop(live);
            staged.commit_overlay();
            let bucket = staged.bucket(&key).unwrap();
            assert_eq!(Rc::as_ptr(&bucket.base), base_ptr);
            assert!(
                bucket.overlay.is_empty(),
                "staged history survived tick {i}"
            );
            assert_eq!(bucket.get(&delta.record), Some(&1));
            live = staged;
        }
        assert_eq!(live.row_count(), 2112);
    }

    #[test]
    fn visible_threshold_update_visits_only_changed_records_at_any_bucket_size() {
        // Internal work-bound proof: consolidated public output would hide
        // a full retract/reinsert scan, so count rows visited by the kernel.
        for size in [10, 10_000] {
            let key = JoinKey::from_slice(b"group");
            let bucket = JoinBucket::from_records(
                (0..size)
                    .map(|i| (Bytes::from(format!("record-{i}")), 1))
                    .collect(),
            );
            let mut left = AsOf::new(ArrangementState {
                index: Rc::new(HashMap::from_iter([(key.clone(), bucket)])),
                ..ArrangementState::default()
            });
            let right = AsOf::default();
            let first = SubTick {
                tick: 1,
                sub_tick: 0,
            };
            left.mark_forward_as_of(first).unwrap();
            let mut state = AntiJoinState::default();
            assert_eq!(
                state
                    .evaluate(
                        ArrangementTransition::at(&left, first),
                        ArrangementTransition::at(&right, first),
                        ArrangementUpdateMode::Replace
                    )
                    .len(),
                size
            );
            state.commit_published_overlay();
            left.value_mut().commit_overlay();
            let deltas = [
                RecordDelta {
                    record: Bytes::from_static(b"record-0"),
                    weight: -1,
                },
                RecordDelta {
                    record: Bytes::from_static(b"replacement"),
                    weight: 1,
                },
            ];
            let mut staged = left.clone();
            staged.value_mut().apply_update(
                &deltas
                    .iter()
                    .map(|delta| KeyedRecordDelta {
                        delta,
                        key: key.clone(),
                    })
                    .collect::<Vec<_>>(),
                ArrangementUpdateMode::Accumulate,
            );
            let second = SubTick {
                tick: 2,
                sub_tick: 0,
            };
            staged.mark_forward_as_of(second).unwrap();
            APPENDED_BUCKET_RECORDS.with(|count| count.set(0));
            let output = state.evaluate(
                ArrangementTransition::at(&staged, second),
                ArrangementTransition::at(&right, second),
                ArrangementUpdateMode::Accumulate,
            );
            assert_eq!(output.len(), 2);
            assert!(deltas.iter().all(|delta| output.contains(delta)));
            assert_eq!(APPENDED_BUCKET_RECORDS.with(|count| count.get()), 2);
            assert_eq!(staged.value().key_count(&key), size as i64);
            // A second consumer sees the same producer delta, not a consumed log.
            let mut sibling = AntiJoinState::default();
            sibling.evaluate(
                ArrangementTransition::at(&left, first),
                ArrangementTransition::at(&right, first),
                ArrangementUpdateMode::Replace,
            );
            assert_eq!(
                sibling.evaluate(
                    ArrangementTransition::at(&staged, second),
                    ArrangementTransition::at(&right, second),
                    ArrangementUpdateMode::Accumulate
                ),
                output
            );
            // Re-entering a completed operator cannot publish a delta twice.
            assert!(
                state
                    .evaluate(
                        ArrangementTransition::at(&staged, second),
                        ArrangementTransition::at(&right, second),
                        ArrangementUpdateMode::Accumulate
                    )
                    .is_empty()
            );
        }
    }

    #[test]
    fn arrangement_commit_preserves_shared_bucket_and_folds_after_release() {
        // Internal ownership proof: compaction must not copy a shared base
        // or mutate the signed multiset retained by a publication snapshot.
        let key = JoinKey::from_slice(b"shared");
        let record = Bytes::from_static(b"row");
        let bucket = JoinBucket::from_records(HashMap::from_iter([
            (record.clone(), 2),
            (Bytes::from_static(b"unchanged"), 1),
        ]));
        let snapshot = bucket.clone();
        let base_ptr = Rc::as_ptr(&bucket.base);
        let mut live = ArrangementState {
            index: Rc::new(HashMap::from_iter([(key.clone(), bucket)])),
            ..ArrangementState::default()
        };
        for (weight, expected) in [(-2, None), (-1, Some(-1)), (4, Some(3))] {
            let delta = RecordDelta {
                record: record.clone(),
                weight,
            };
            live.apply_update(
                &[KeyedRecordDelta {
                    delta: &delta,
                    key: key.clone(),
                }],
                ArrangementUpdateMode::Accumulate,
            );
            live.commit_overlay();
            let bucket = live.bucket(&key).unwrap();
            assert_eq!(Rc::as_ptr(&bucket.base), base_ptr);
            assert_eq!(bucket.get(&record).copied(), expected);
            assert_eq!(snapshot.get(&record), Some(&2));
        }
        drop(snapshot);
        let delta = RecordDelta {
            record: record.clone(),
            weight: 1,
        };
        live.apply_update(
            &[KeyedRecordDelta {
                delta: &delta,
                key: key.clone(),
            }],
            ArrangementUpdateMode::Accumulate,
        );
        live.commit_overlay();
        let bucket = live.bucket(&key).unwrap();
        assert_eq!(Rc::as_ptr(&bucket.base), base_ptr);
        assert!(bucket.overlay.is_empty());
        assert_eq!(bucket.get(&record), Some(&4));
    }

    #[test]
    fn arrangement_snapshot_clone_shares_payload_until_first_write() {
        let mut bucket = HashMap::default();
        bucket.insert(Bytes::from_static(b"row-one"), 1);
        let mut index = HashMap::default();
        index.insert(
            JoinKey::from_slice(b"one"),
            JoinBucket::from_records(bucket),
        );
        let original = ArrangementState {
            index: Rc::new(index),
            overlay: Rc::default(),
            changes: None,
            snapshot: Weak::new(),
        };

        let mut prepared = original.clone();
        assert!(
            Rc::ptr_eq(&original.index, &prepared.index),
            "starting an evaluation must not copy resident arrangement rows"
        );

        let mut second_bucket = HashMap::default();
        second_bucket.insert(Bytes::from_static(b"row-two"), 1);
        Rc::make_mut(&mut prepared.overlay).insert(
            JoinKey::from_slice(b"two"),
            Some(JoinBucket::from_records(second_bucket)),
        );
        assert!(Rc::ptr_eq(&original.index, &prepared.index));
        assert!(!Rc::ptr_eq(&original.overlay, &prepared.overlay));
        assert_eq!(original.row_count(), 1);
        assert_eq!(prepared.row_count(), 2);
    }

    #[test]
    fn singleton_join_bucket_promotes_without_changing_snapshot_or_tombstone_semantics() {
        // Internal representation proof: public join rows cannot establish
        // that a singleton did not allocate a collection/hash its record.
        let record = Bytes::from_static(b"one");
        let other = Bytes::from_static(b"two");
        let mut bucket = JoinBucket::default();
        assert_eq!(bucket.add_weight(&record, 2), 2);
        assert!(matches!(
            bucket.overlay.as_ref(),
            JoinBucketMap::One(_, Some(2))
        ));
        bucket.commit_overlay();
        assert!(matches!(bucket.base.as_ref(), JoinBucketMap::One(_, 2)));
        assert!(matches!(bucket.overlay.as_ref(), JoinBucketMap::Empty));
        let original = bucket.clone();
        assert_eq!(bucket.add_weight(&record, -2), 0);
        assert!(matches!(
            bucket.overlay.as_ref(),
            JoinBucketMap::One(_, None)
        ));
        assert!(bucket.is_empty());
        assert_eq!(original.get(&record), Some(&2));
        assert_eq!(bucket.add_weight(&record, -1), -1);
        bucket.commit_overlay();
        assert_eq!(bucket.get(&record), Some(&-1));
        assert_eq!(bucket.add_weight(&other, 3), 3);
        bucket.commit_overlay();
        assert!(matches!(bucket.base.as_ref(), JoinBucketMap::Many(_)));
        assert_eq!(bucket.get(&record), Some(&-1));
        assert_eq!(bucket.get(&other), Some(&3));
        assert_eq!(original.get(&record), Some(&2));
        assert_eq!(original.get(&other), None);
    }

    #[test]
    fn inline_join_bucket_matches_signed_map_oracle_across_promotion_and_snapshots() {
        // Internal multiset oracle covers copy-on-write snapshots and staged
        // overrides, which are not separately observable through a query API.
        fn contents(bucket: &JoinBucket) -> std::collections::BTreeMap<Vec<u8>, i64> {
            bucket
                .iter()
                .map(|(record, weight)| (record.to_vec(), *weight))
                .collect()
        }
        for count in [1, 2, 17] {
            let records = (0..count)
                .map(|i| Bytes::from(format!("record-{i}")))
                .collect::<Vec<_>>();
            let mut bucket = JoinBucket::default();
            let mut expected = std::collections::BTreeMap::<Vec<u8>, i64>::new();
            let mut snapshots = Vec::new();
            for step in 0..600 {
                if step % 11 == 0 {
                    snapshots.push((bucket.clone(), expected.clone()));
                }
                let record = &records[(step * 13 + step / 5) % count];
                let change = ((step * 17 + 3) % 7) as i64 - 3;
                let weight = if step % 7 == 0 {
                    bucket.set(record.clone(), change);
                    change
                } else {
                    let weight =
                        expected.get(record.as_ref()).copied().unwrap_or_default() + change;
                    assert_eq!(bucket.add_weight(record, change), weight);
                    weight
                };
                if weight == 0 {
                    expected.remove(record.as_ref());
                } else {
                    expected.insert(record.to_vec(), weight);
                }
                if step % 5 == 0 {
                    bucket.commit_overlay();
                }
                assert_eq!(contents(&bucket), expected, "count={count} step={step}");
            }
            bucket.commit_overlay();
            assert_eq!(contents(&bucket), expected);
            for (snapshot, expected) in snapshots {
                assert_eq!(contents(&snapshot), expected);
            }
        }
    }

    #[test]
    fn arrangement_bucket_snapshot_stages_one_record_without_copying_base() {
        let record = Bytes::from_static(b"row-one");
        let added = Bytes::from_static(b"row-two");
        let mut records = HashMap::default();
        records.insert(record.clone(), 1);
        let live = JoinBucket::from_records(records);
        let mut staged = live.clone();
        staged.set(added.clone(), 1);
        assert!(Rc::ptr_eq(&live.base, &staged.base));
        assert_eq!(live.get(&added), None);
        drop(live);
        staged.commit_overlay();
        assert_eq!(staged.get(&record), Some(&1));
        assert_eq!(staged.get(&added), Some(&1));
    }
    // Internal because snapshot sharing and tombstone/base interaction are
    // private arrangement mechanics, not a separate public query operation.
    #[test]
    fn direct_arrangement_overlay_preserves_absence_and_snapshot_isolation() {
        let key = JoinKey::from_slice(b"key");
        let record = Bytes::from_static(b"row");
        let apply = |state: &mut ArrangementState, weights: &[i64], mode| {
            let deltas = weights
                .iter()
                .map(|weight| RecordDelta {
                    record: record.clone(),
                    weight: *weight,
                })
                .collect::<Vec<_>>();
            let keyed = deltas
                .iter()
                .map(|delta| KeyedRecordDelta {
                    delta,
                    key: key.clone(),
                })
                .collect::<Vec<_>>();
            state.apply_update(&keyed, mode);
        };
        let mut live = ArrangementState::default();
        apply(&mut live, &[2], ArrangementUpdateMode::Replace);
        live.commit_overlay();
        let original = live.clone();
        apply(&mut live, &[-2], ArrangementUpdateMode::Accumulate);
        assert!(live.bucket(&key).is_none());
        let absent = live.clone();
        apply(&mut live, &[-1, 4], ArrangementUpdateMode::Accumulate);
        assert_eq!(live.bucket(&key).unwrap().get(&record), Some(&3));
        assert_eq!(original.bucket(&key).unwrap().get(&record), Some(&2));
        assert!(absent.bucket(&key).is_none());
        apply(&mut live, &[], ArrangementUpdateMode::Accumulate);
        assert!(
            live.changes.is_none(),
            "empty advance must forget the prior delta"
        );
        assert_eq!(live.bucket(&key).unwrap().get(&record), Some(&3));
        live.commit_overlay();
        assert_eq!(live.bucket(&key).unwrap().get(&record), Some(&3));
        assert_eq!(original.bucket(&key).unwrap().get(&record), Some(&2));
        apply(&mut live, &[], ArrangementUpdateMode::Replace);
        assert!(live.changes.is_none());
        assert!(live.bucket(&key).is_none());
    }

    // Internal for the same private signed-bag/COW mechanics above.
    #[test]
    fn bucket_weight_updates_preserve_tombstones_and_shared_snapshots() {
        let record = Bytes::from_static(b"same-row");
        let original = JoinBucket::from_records(HashMap::from_iter([(record.clone(), 2)]));
        let mut staged = original.clone();
        assert_eq!(staged.add_weight(&record, -2), 0);
        assert_eq!(staged.get(&record), None);
        let absent = staged.clone();
        assert_eq!(staged.add_weight(&record, -1), -1);
        assert_eq!(staged.add_weight(&record, 4), 3);
        assert_eq!(original.get(&record), Some(&2));
        assert_eq!(absent.get(&record), None);
        assert!(Rc::ptr_eq(&original.base, &staged.base));
        staged.commit_overlay();
        assert_eq!(staged.get(&record), Some(&3));
        assert_eq!(original.get(&record), Some(&2));
        assert_eq!(absent.get(&record), None);
    }
}
