//! Per-tick evaluator state, memoization, arrangements, and recursive execution.

use super::*;
mod kernels;

// Test-only work receipt: output equivalence alone cannot detect accidental
// reintroduction of a full ancestor traversal for each ready queue slot.
#[cfg(test)]
thread_local! {
    static SUBGRAPH_WALKS: std::cell::Cell<usize> = const { std::cell::Cell::new(0) };
    static ASYNC_NODE_FRAMES: std::cell::Cell<usize> = const { std::cell::Cell::new(0) };
}

#[cfg(test)]
pub(super) fn take_subgraph_walk_count() -> usize {
    SUBGRAPH_WALKS.with(|count| count.replace(0))
}

#[cfg(test)]
pub(crate) fn take_async_node_frame_count() -> usize {
    ASYNC_NODE_FRAMES.with(|count| count.replace(0))
}

fn plan_expr_fields(expressions: &[PlanExpr]) -> BTreeSet<String> {
    expressions
        .iter()
        .filter_map(|expression| match expression {
            PlanExpr::Field(field)
            | PlanExpr::Nullable(field)
            | PlanExpr::NullableFlat(field)
            | PlanExpr::EnumTagRemap { field, .. }
            | PlanExpr::EnumRemap { field, .. }
            | PlanExpr::RecursiveEnumRemap { field, .. } => Some(field.clone()),
            PlanExpr::Literal(_) | PlanExpr::Null(_) => None,
        })
        .collect()
}
use crate::storage::StorageFuture;
use std::ops::{Deref, DerefMut};
use std::rc::Rc;

/// Ready batches never allocate an async frame. Only operators which really
/// need the general evaluator carry its boxed continuation.
pub(super) enum ReadyNodeEvaluation<'a> {
    Ready(std::future::Ready<Result<Arc<RecordDeltas>, IvmRuntimeError>>),
    Deferred(StorageFuture<'a, Result<Arc<RecordDeltas>, IvmRuntimeError>>),
}

/// Immutable candidate order; publication presence is checked on every use.
#[derive(Debug)]
pub(super) struct TerminalLineage {
    candidates: Vec<(NodeId, bool)>,
    has_public_root: bool,
}

impl std::future::Future for ReadyNodeEvaluation<'_> {
    type Output = Result<Arc<RecordDeltas>, IvmRuntimeError>;

    fn poll(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Self::Output> {
        match self.get_mut() {
            Self::Ready(value) => std::pin::Pin::new(value).poll(cx),
            Self::Deferred(future) => future.as_mut().poll(cx),
        }
    }
}

impl ReadyNodeEvaluation<'_> {
    fn ready(result: Result<Arc<RecordDeltas>, IvmRuntimeError>) -> Self {
        Self::Ready(std::future::ready(result))
    }
}

/// Frame-owned continuation: never installed as retained operator state.
/// Completed prefixes remain private across CPU yields and are discarded on
/// cancellation/failure. There is no borrowed evaluator or async stack here.
pub(super) struct PendingUnaryBatch {
    lookup: NodeMemoLookup,
    input: Arc<RecordDeltas>,
    next: usize,
    output: RecordDeltas,
    projection: Option<Arc<PreparedProjection>>,
    #[cfg(feature = "cold-settle-attribution")]
    elapsed_ns: u64,
}

const UNARY_ROWS_PER_POLL: usize = 256;

#[derive(Clone, Debug)]
pub(super) enum OperatorState {
    Stateless,
    Join(JoinState),
    SemiJoin(SemiJoinState),
    AntiJoin(AntiJoinState),
    ArgBy(AsOf<SparseGroups, SubTick>),
    TopBy(AsOf<TopByIncrementalState, SubTick>),
    Recursive(AsOf<RecursiveState, Tick>),
    CollectBy(CollectByIncrementalState),
    StreamingChecksum(Box<StreamingChecksumOperatorState>),
}

#[derive(Clone, Debug, Default)]
pub(super) struct StreamingChecksumOperatorState {
    pending: Option<PendingStreamingChecksum>,
}

#[derive(Clone, Debug)]
struct PendingStreamingChecksum {
    input: Arc<RecordDeltas>,
    next_delta: usize,
    current: Option<crate::large_values::StreamingChecksum>,
    output: Vec<RecordDelta>,
}

#[derive(Clone, Debug, Default)]
pub(super) struct CollectByIncrementalState {
    payload: Rc<CollectByIncrementalPayload>,
}

#[derive(Clone, Debug, Default)]
pub(super) struct CollectByIncrementalPayload {
    pub(super) groups: CollectByGroups,
    pub(super) roots: BTreeMap<CollectByOrderKey, i64>,
    /// Ordered public root-collector occurrence index. Unlike `groups`, whose
    /// key is the opaque output identity, this follows the compiled TopBy key
    /// and excludes maintenance-only groups that never reached a terminal.
    pub(super) emitted_root_order: Rc<RankIndex<CollectByOrderKey, Vec<u8>>>,
    /// Root terminal groups that have actually been emitted to a subscriber.
    /// Some join-maintenance rows share a sort key but are not facade roots.
    pub(super) emitted_root_keys: Rc<RankIndex<Vec<u8>, ()>>,
}

impl Deref for CollectByIncrementalState {
    type Target = CollectByIncrementalPayload;

    fn deref(&self) -> &Self::Target {
        &self.payload
    }
}

impl DerefMut for CollectByIncrementalState {
    fn deref_mut(&mut self) -> &mut Self::Target {
        Rc::make_mut(&mut self.payload)
    }
}

/// Reuse a fresh root collector's complete Insert seeds only when every group
/// has one unambiguous representative. A multi-record hash arrangement and
/// the ordered terminal collector need not choose the same representative.
fn singleton_root_hydration_snapshot(
    state: &CollectByIncrementalState,
    input: &[RecordDelta],
    operations: &[TerminalOperation],
) -> Option<Vec<RecordDelta>> {
    if input.iter().any(|delta| delta.weight <= 0) {
        return None;
    }
    let mut seeds = Vec::with_capacity(operations.len());
    for operation in operations {
        let TerminalEdit::Insert { key, value, .. } = &operation.edit else {
            return None;
        };
        if !operation.path.is_empty() || operation.root_key != *key {
            return None;
        }
        let mut records = state.groups.get(key)?.iter();
        let (_, weight) = records.next()?;
        if *weight <= 0 || records.next().is_some() {
            return None;
        }
        seeds.push((key, value));
    }
    // Terminal operations use public rank order; the relational snapshot has
    // always visited the touched groups in encoded group-key order.
    seeds.sort_unstable_by(|(left, _), (right, _)| left.cmp(right));
    Some(
        seeds
            .into_iter()
            .map(|(_, value)| RecordDelta {
                record: Bytes::copy_from_slice(value),
                weight: 1,
            })
            .collect(),
    )
}

#[cfg(test)]
mod collect_by_state_tests {
    use super::*;

    // Internal eligibility proof for opaque already-rendered seeds; signed
    // intermediate bags and malformed terminal edits are not public writes.
    #[test]
    fn singleton_root_seed_reuse_rejects_ambiguous_groups_and_non_snapshot_edits() {
        let first = Bytes::from_static(b"first input");
        let second = Bytes::from_static(b"second input");
        let mut state = CollectByIncrementalState::default();
        state
            .groups
            .get_or_default(vec![1])
            .set((Vec::new(), first.clone()), 2);
        state
            .groups
            .get_or_default(vec![2])
            .set((Vec::new(), second.clone()), 1);
        let input = vec![
            RecordDelta {
                record: first.clone(),
                weight: 1,
            },
            RecordDelta {
                record: first.clone(),
                weight: 1,
            },
            RecordDelta {
                record: second.clone(),
                weight: 1,
            },
        ];
        let operations = [2, 1].map(|key| TerminalOperation {
            root_descriptor: RecordDescriptor::default(),
            root_key: vec![key],
            path: Vec::new(),
            edit: TerminalEdit::Insert {
                index: usize::from(2 - key),
                key: vec![key],
                value: vec![key, 42],
            },
        });
        let snapshot = singleton_root_hydration_snapshot(&state, &input, &operations).unwrap();
        assert_eq!(
            snapshot,
            vec![
                RecordDelta {
                    record: Bytes::from_static(&[1, 42]),
                    weight: 1
                },
                RecordDelta {
                    record: Bytes::from_static(&[2, 42]),
                    weight: 1
                },
            ]
        );
        let mut ambiguous = state.clone();
        ambiguous
            .groups
            .get_or_default(vec![1])
            .set((Vec::new(), second), 1);
        assert!(singleton_root_hydration_snapshot(&ambiguous, &input, &operations).is_none());
        for weight in [-1, 0] {
            let mut signed = input.clone();
            signed[0].weight = weight;
            assert!(singleton_root_hydration_snapshot(&state, &signed, &operations).is_none());
        }
        let mut incremental = operations.clone();
        incremental[0].edit = TerminalEdit::Remove { key: vec![2] };
        assert!(singleton_root_hydration_snapshot(&state, &input, &incremental).is_none());
        let mut wrong_key = operations.clone();
        wrong_key[0].root_key = vec![1];
        assert!(singleton_root_hydration_snapshot(&state, &input, &wrong_key).is_none());
        assert_eq!(
            singleton_root_hydration_snapshot(&state, &input, &operations),
            Some(snapshot)
        );
    }

    #[test]
    fn collect_by_snapshot_clone_shares_payload_until_first_write() {
        let original = CollectByIncrementalState::default();
        let mut prepared = original.clone();
        assert!(Rc::ptr_eq(&original.payload, &prepared.payload));

        prepared.groups.clear();
        assert!(!Rc::ptr_eq(&original.payload, &prepared.payload));
    }

    #[test]
    fn collect_by_group_stages_only_touched_occurrences() {
        let first = (Vec::new(), Bytes::from_static(b"first"));
        let second = (Vec::new(), Bytes::from_static(b"second"));
        let mut live = CollectByGroup::default();
        live.set(first.clone(), 1);
        live.commit_overlay();

        let mut staged = live.clone();
        staged.set(second.clone(), 1);

        assert_eq!(live.get(&second), None);
        assert_eq!(staged.get(&first), Some(&1));
        assert_eq!(staged.get(&second), Some(&1));
        assert!(Rc::ptr_eq(&live.base, &staged.base));

        // Installation removes the live state first, so folding the staged
        // overlay updates the shared base without re-materializing it.
        drop(live);
        staged.commit_overlay();
        assert!(staged.overlay.is_empty());
        assert_eq!(staged.get(&first), Some(&1));
        assert_eq!(staged.get(&second), Some(&1));
    }

    // Internal: the batched rank walk must agree with per-key ranking over a
    // staged index mixing base keys, staged insertions and staged removals.
    // Public queries only see the resulting terminal order.
    #[test]
    fn sparse_groups_batched_ranks_match_per_key_ranks() {
        let order = (Vec::new(), Bytes::from_static(b"record"));
        let key = |value: u8| vec![value];
        let mut groups = SparseGroups::default();
        for value in [2, 4, 6, 8, 10] {
            groups.get_or_default(key(value)).set(order.clone(), 1);
        }
        groups.commit_overlay();
        let mut staged = groups.clone();
        for value in [1, 5, 9, 11] {
            staged.get_or_default(key(value)).set(order.clone(), 1);
        }
        for value in [4, 8] {
            staged.get_or_default(key(value)).set(order.clone(), 0);
        }
        staged.remove_empty_touched_groups([key(4), key(8)]);

        let probes = (0..=12).map(key).collect::<Vec<_>>();
        let batched = staged.count_before_each(probes.iter().map(Vec::as_slice));
        let per_key = probes
            .iter()
            .map(|probe| staged.count_before(probe))
            .collect::<Vec<_>>();
        assert_eq!(batched, per_key);
        assert_eq!(staged.count_before_each([key(12).as_slice()]), vec![7]);
    }

    #[test]
    fn sparse_groups_stage_one_group_without_cloning_the_outer_index() {
        let first = b"first".to_vec();
        let second = b"second".to_vec();
        let order = (Vec::new(), Bytes::from_static(b"record"));
        let changed_order = (Vec::new(), Bytes::from_static(b"changed-record"));
        let mut live = SparseGroups::default();
        live.get_or_default(first.clone()).set(order.clone(), 1);
        live.get_or_default(second.clone()).set(order.clone(), 1);
        live.commit_overlay();
        let first_base = Rc::as_ptr(&live.get(&first).expect("installed first group").base);

        let mut staged = live.clone();
        staged
            .get_or_default(first.clone())
            .set(changed_order.clone(), 1);

        // This is the scale canary: a tick changing one group must retain the
        // installed outer map, rather than cloning every unrelated group.
        assert!(Rc::ptr_eq(&live.base, &staged.base));
        assert_eq!(live.keys(), BTreeSet::from([first.clone(), second.clone()]));
        assert_eq!(
            staged.keys(),
            BTreeSet::from([first.clone(), second.clone()])
        );
        assert_eq!(
            staged
                .get(&first)
                .and_then(|group| group.get(&changed_order)),
            Some(&1)
        );
        assert!(staged.get(&second).is_some());

        drop(live);
        staged.commit_overlay();
        assert!(staged.overlay.is_empty());
        assert_eq!(staged.keys(), BTreeSet::from([first, second]));
        assert_eq!(
            Rc::as_ptr(&staged.get(b"first").expect("committed first group").base),
            first_base,
            "folding a changed group must not copy its untouched occurrences"
        );
    }
}

pub(super) type CollectByOrderKey = (Vec<TopBySortPart>, Bytes);
type TopByGroups = SparseGroups;
type CollectByGroups = SparseGroups;

/// Copy-on-write ordered map for the outer window-group index.  Like
/// [`CollectByGroup`], a speculative tick owns only its changed entries; the
/// (potentially very large) untouched group index remains shared with the
/// installed state until the successful install boundary.
#[derive(Clone, Debug, Default)]
pub(super) struct SparseGroups {
    base: Rc<BTreeMap<Vec<u8>, CollectByGroup>>,
    overlay: Rc<BTreeMap<Vec<u8>, Option<CollectByGroup>>>,
}

impl SparseGroups {
    pub(super) fn get(&self, key: &[u8]) -> Option<&CollectByGroup> {
        self.overlay.get(key).and_then(Option::as_ref).or_else(|| {
            (!self.overlay.contains_key(key))
                .then(|| self.base.get(key))
                .flatten()
        })
    }

    pub(super) fn get_or_default(&mut self, key: Vec<u8>) -> &mut CollectByGroup {
        let base = &self.base;
        let overlay = Rc::make_mut(&mut self.overlay);
        let group = match overlay.entry(key.clone()) {
            std::collections::btree_map::Entry::Occupied(mut entry) => {
                // A previous retraction can leave a tombstone in the staged
                // overlay. A later delta in the same tick must revive it.
                if entry.get().is_none() {
                    entry.insert(Some(base.get(&key).cloned().unwrap_or_default()));
                }
                entry.into_mut()
            }
            std::collections::btree_map::Entry::Vacant(entry) => {
                entry.insert(Some(base.get(&key).cloned().unwrap_or_default()))
            }
        };
        group.as_mut().expect("revived group is present")
    }

    pub(super) fn remove_empty_touched_groups<I>(&mut self, keys: I)
    where
        I: IntoIterator<Item = Vec<u8>>,
    {
        for key in keys {
            if self.get(&key).is_some_and(CollectByGroup::is_empty) {
                Rc::make_mut(&mut self.overlay).insert(key, None);
            }
        }
    }

    pub(super) fn clear(&mut self) {
        self.base = Rc::default();
        self.overlay = Rc::default();
    }

    /// Present group keys in ascending order: untouched base keys merged
    /// with staged insertions, skipping staged removals.
    fn present_keys(&self) -> impl Iterator<Item = &[u8]> {
        let mut base = self.base.keys().peekable();
        let mut overlay = self.overlay.iter().peekable();
        std::iter::from_fn(move || {
            loop {
                match (base.peek(), overlay.peek()) {
                    (Some(base_key), Some((overlay_key, _))) if *base_key < *overlay_key => {
                        return base.next().map(Vec::as_slice);
                    }
                    (Some(base_key), Some((overlay_key, group))) => {
                        if *base_key == *overlay_key {
                            base.next();
                        }
                        let present = group.is_some();
                        let (key, _) = overlay.next().expect("peeked");
                        if present {
                            return Some(key.as_slice());
                        }
                    }
                    (None, Some((_, group))) => {
                        let present = group.is_some();
                        let (key, _) = overlay.next().expect("peeked");
                        if present {
                            return Some(key.as_slice());
                        }
                    }
                    (Some(_), None) => return base.next().map(Vec::as_slice),
                    (None, None) => return None,
                }
            }
        })
    }

    /// Rank each of `keys` (ascending) among the present groups in one merged
    /// walk, instead of one range scan per key. Inserting many new groups in
    /// one batch is then linear in the group count, not quadratic.
    pub(super) fn count_before_each<'k>(
        &self,
        keys: impl IntoIterator<Item = &'k [u8]>,
    ) -> Vec<usize> {
        let mut present = self.present_keys().peekable();
        let mut before = 0usize;
        let mut ranks = Vec::new();
        let mut previous: Option<&[u8]> = None;
        for key in keys {
            debug_assert!(previous.is_none_or(|previous| previous <= key));
            previous = Some(key);
            while present.next_if(|candidate| *candidate < key).is_some() {
                before += 1;
            }
            ranks.push(before);
        }
        ranks
    }

    /// Rank a present group in the merged ordered map without constructing a
    /// combined snapshot of all groups.
    #[cfg(test)]
    pub(super) fn count_before(&self, key: &[u8]) -> usize {
        let retained_base = self
            .base
            .range(..key.to_vec())
            .filter(|(candidate, _)| self.overlay.get(*candidate).is_none_or(Option::is_some))
            .count();
        let inserted_overlay = self
            .overlay
            .range(..key.to_vec())
            .filter(|(candidate, group)| group.is_some() && !self.base.contains_key(*candidate))
            .count();
        retained_base + inserted_overlay
    }

    #[cfg(test)]
    pub(super) fn keys(&self) -> BTreeSet<Vec<u8>> {
        let mut keys = self.base.keys().cloned().collect::<BTreeSet<_>>();
        for (key, value) in self.overlay.iter() {
            if value.is_some() {
                keys.insert(key.clone());
            } else {
                keys.remove(key);
            }
        }
        keys
    }

    #[cfg(test)]
    pub(super) fn len(&self) -> usize {
        self.keys().len()
    }

    /// Fold after the installed state was removed.  Remove a replaced group
    /// from the uniquely-owned outer map *before* folding its inner overlay:
    /// the staged group shares that inner base with the old map entry, so
    /// folding first would make `Rc::make_mut` clone every occurrence.
    pub(super) fn commit_overlay(&mut self) {
        if self.overlay.is_empty() {
            return;
        }
        let overlay = std::mem::take(&mut self.overlay);
        let overlay = Rc::try_unwrap(overlay).unwrap_or_else(|value| (*value).clone());
        let mut base =
            Rc::try_unwrap(std::mem::take(&mut self.base)).unwrap_or_else(|value| (*value).clone());
        for (key, group) in overlay {
            // Dropping the pre-image first makes an existing staged group's
            // occurrence base uniquely owned on the successful install path.
            base.remove(&key);
            if let Some(mut group) = group {
                group.commit_overlay();
                base.insert(key, group);
            }
        }
        self.base = Rc::new(base);
    }
}

/// A collector group is copied between a live evaluator and its staged tick.
/// Keep its long-lived occurrence map immutable and record only touched keys
/// in the staged overlay.  A single child edit must not clone every child of a
/// large parent collection merely because the tick later needs atomic commit.
#[derive(Clone, Debug, Default)]
pub(super) struct CollectByGroup {
    base: Rc<BTreeMap<CollectByOrderKey, i64>>,
    overlay: Rc<BTreeMap<CollectByOrderKey, Option<i64>>>,
}

impl CollectByGroup {
    pub(super) fn get(&self, key: &CollectByOrderKey) -> Option<&i64> {
        self.overlay.get(key).and_then(Option::as_ref).or_else(|| {
            (!self.overlay.contains_key(key))
                .then(|| self.base.get(key))
                .flatten()
        })
    }

    pub(super) fn set(&mut self, key: CollectByOrderKey, weight: i64) {
        Rc::make_mut(&mut self.overlay).insert(key, (weight != 0).then_some(weight));
    }

    pub(super) fn count_before(&self, key: &CollectByOrderKey) -> usize {
        self.iter()
            .take_while(|(candidate, _)| *candidate < key)
            .filter(|(_, weight)| **weight > 0)
            .count()
    }

    pub(super) fn is_empty(&self) -> bool {
        self.iter().next().is_none()
    }

    pub(super) fn iter(&self) -> CollectByGroupIter<'_> {
        CollectByGroupIter {
            base: self.base.iter().peekable(),
            overlay: self.overlay.iter().peekable(),
        }
    }

    /// Fold this tick's sparse mutations only after the old live operator was
    /// removed, making the common commit path uniquely own the base map.
    pub(super) fn commit_overlay(&mut self) {
        if self.overlay.is_empty() {
            return;
        }
        let overlay = std::mem::take(&mut self.overlay);
        let overlay = Rc::try_unwrap(overlay).unwrap_or_else(|overlay| (*overlay).clone());
        let base = Rc::make_mut(&mut self.base);
        for (key, weight) in overlay {
            if let Some(weight) = weight {
                base.insert(key, weight);
            } else {
                base.remove(&key);
            }
        }
    }
}

pub(super) struct CollectByGroupIter<'a> {
    base: std::iter::Peekable<std::collections::btree_map::Iter<'a, CollectByOrderKey, i64>>,
    overlay:
        std::iter::Peekable<std::collections::btree_map::Iter<'a, CollectByOrderKey, Option<i64>>>,
}

impl<'a> Iterator for CollectByGroupIter<'a> {
    type Item = (&'a CollectByOrderKey, &'a i64);

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            match (self.base.peek(), self.overlay.peek()) {
                (Some((base_key, base_weight)), Some((overlay_key, overlay_weight))) => {
                    match base_key.cmp(overlay_key) {
                        std::cmp::Ordering::Less => {
                            let item = (*base_key, *base_weight);
                            self.base.next();
                            return Some(item);
                        }
                        std::cmp::Ordering::Greater => {
                            let item = (*overlay_key, *overlay_weight);
                            self.overlay.next();
                            if let (key, Some(weight)) = item {
                                return Some((key, weight));
                            }
                        }
                        std::cmp::Ordering::Equal => {
                            self.base.next();
                            let item = self.overlay.next().expect("overlay was peeked");
                            if let (key, Some(weight)) = item {
                                return Some((key, weight));
                            }
                        }
                    }
                }
                (Some((key, weight)), None) => {
                    let item = (*key, *weight);
                    self.base.next();
                    return Some(item);
                }
                (None, Some((key, weight))) => {
                    let item = (*key, *weight);
                    self.overlay.next();
                    if let (key, Some(weight)) = item {
                        return Some((key, weight));
                    }
                }
                (None, None) => return None,
            }
        }
    }
}

#[derive(Clone, Debug, Default)]
pub(super) struct TopByIncrementalState {
    groups: TopByGroups,
}

impl TopByIncrementalState {
    /// Remove only groups touched by this update so a long-lived subscription
    /// does not retain one empty map for every departed group.
    fn remove_empty_touched_groups<I>(&mut self, groups: I)
    where
        I: IntoIterator<Item = Vec<u8>>,
    {
        for group in groups {
            if self
                .groups
                .get(&group)
                .is_some_and(CollectByGroup::is_empty)
            {
                self.groups.remove_empty_touched_groups([group]);
            }
        }
    }

    pub(super) fn commit_overlays(&mut self) {
        self.groups.commit_overlay();
    }

    #[cfg(test)]
    pub(super) fn group_count(&self) -> usize {
        self.groups.len()
    }
}

pub(super) fn operator_state_for(operator: &OpType) -> OperatorState {
    match operator {
        OpType::Join(_) => OperatorState::Join(JoinState),
        OpType::SemiJoin(_) => OperatorState::SemiJoin(SemiJoinState::default()),
        OpType::AntiJoin(_) => OperatorState::AntiJoin(AntiJoinState::default()),
        OpType::Recursive(_) => OperatorState::Recursive(AsOf::new(RecursiveState::default())),
        OpType::ArgMinBy(_) | OpType::ArgMaxBy(_) => OperatorState::ArgBy(AsOf::default()),
        OpType::TopBy(_) => OperatorState::TopBy(AsOf::new(TopByIncrementalState::default())),
        OpType::CollectBy(_) => OperatorState::CollectBy(CollectByIncrementalState::default()),
        OpType::StreamingChecksum(_) => OperatorState::StreamingChecksum(Box::default()),
        _ => OperatorState::Stateless,
    }
}

pub(super) fn plan_expr_names(expressions: &[PlanExpr]) -> Vec<String> {
    expressions
        .iter()
        .filter_map(|expr| match expr {
            PlanExpr::Field(name)
            | PlanExpr::Nullable(name)
            | PlanExpr::NullableFlat(name)
            | PlanExpr::EnumTagRemap { field: name, .. }
            | PlanExpr::EnumRemap { field: name, .. }
            | PlanExpr::RecursiveEnumRemap { field: name, .. } => Some(name.clone()),
            PlanExpr::Literal(_) | PlanExpr::Null(_) => None,
        })
        .collect()
}

pub(super) fn record_deltas_digest(deltas: &RecordDeltas) -> u64 {
    let mut hasher = DefaultHasher::new();
    deltas.descriptor.hash(&mut hasher);
    for delta in &deltas.deltas {
        delta.weight.hash(&mut hasher);
        delta.record.hash(&mut hasher);
    }
    hasher.finish()
}

pub(super) fn builder_contains_recursive(graph: &GraphBuilder) -> bool {
    graph
        .postorder()
        .iter()
        .any(|node| matches!(node, GraphBuilder::Recursive { .. }))
}

pub(super) fn validate_arg_by_primary_key_indices(
    op_name: &str,
    table: &TableSchema,
    group_fields: &[usize],
    order_fields: &[usize],
    primary_key_fields: &[usize],
) -> Result<(), IvmRuntimeError> {
    let expected = group_fields
        .iter()
        .chain(order_fields.iter())
        .copied()
        .collect::<Vec<_>>();
    if primary_key_fields == expected {
        Ok(())
    } else {
        Err(IvmRuntimeError::UnsupportedArgMaxBy(format!(
            "{op_name} v1 requires primary key for {} to equal group_cols + order_cols",
            table.name
        )))
    }
}

/// Single-tick evaluator over a deduplicated graph.
#[derive(Clone, Debug, Default)]
pub(super) struct RootOrderingWindows {
    /// Every touched group's before/after window records, in evaluation
    /// order. Position maps are built only when an output applies them.
    entries: Vec<(Vec<u8>, GroupWindow)>,
    descriptor: Option<RecordDescriptor>,
    identity: Vec<usize>,
    /// Field-0 positions across all groups, for outputs without a proven
    /// identity: built once, first position wins, as before.
    field_zero: std::cell::OnceCell<RootPositions>,
    /// Each group's first and last entry, so an output reaching a few groups
    /// does not scan every touched group's windows. Reset by `record`.
    group_entries: std::cell::OnceCell<HashMap<Vec<u8>, (usize, usize)>>,
}

/// A group's before and after window records.
type WindowPair<'a> = (&'a [WindowedRecord], &'a [WindowedRecord]);

#[derive(Clone, Debug, Default)]
struct GroupWindow {
    before: Vec<WindowedRecord>,
    after: Vec<WindowedRecord>,
}

#[derive(Clone, Debug, Default)]
struct RootPositions {
    before: BTreeMap<Vec<u8>, usize>,
    after: BTreeMap<Vec<u8>, usize>,
}

impl RootOrderingWindows {
    pub(super) fn record(
        &mut self,
        descriptor: RecordDescriptor,
        top_by: &TopByOp,
        group: &[u8],
        before: &[WindowedRecord],
        after: &[WindowedRecord],
    ) {
        if self.descriptor.is_none() {
            self.descriptor = Some(descriptor);
            self.identity = top_by_identity_fields(top_by, descriptor.fields().len());
        }
        self.group_entries.take();
        self.entries.push((
            group.to_vec(),
            GroupWindow {
                before: before.to_vec(),
                after: after.to_vec(),
            },
        ));
    }

    fn positions<'a>(
        &self,
        entries: impl Iterator<Item = &'a GroupWindow>,
        key_fields: &[usize],
    ) -> Result<RootPositions, IvmRuntimeError> {
        let mut positions = RootPositions::default();
        let Some(descriptor) = self.descriptor else {
            return Ok(positions);
        };
        for window in entries {
            extend_root_window_positions(
                descriptor,
                &window.before,
                key_fields,
                &mut positions.before,
            )?;
            extend_root_window_positions(
                descriptor,
                &window.after,
                key_fields,
                &mut positions.after,
            )?;
        }
        Ok(positions)
    }

    fn field_zero(&self) -> Result<&RootPositions, IvmRuntimeError> {
        if let Some(positions) = self.field_zero.get() {
            return Ok(positions);
        }
        let positions = self.positions(self.entries.iter().map(|(_, window)| window), &[0])?;
        Ok(self.field_zero.get_or_init(|| positions))
    }

    /// A group's window across this tick: its first before and last after.
    fn group_window(&self, group: &[u8]) -> Option<WindowPair<'_>> {
        let index = self.group_entries.get_or_init(|| {
            let mut index = HashMap::<Vec<u8>, (usize, usize)>::default();
            for (position, (group, _)) in self.entries.iter().enumerate() {
                index
                    .entry(group.clone())
                    .and_modify(|(_, last)| *last = position)
                    .or_insert((position, position));
            }
            index
        });
        let &(first, last) = index.get(group)?;
        Some((&self.entries[first].1.before, &self.entries[last].1.after))
    }
}

/// Ephemeral lookup inputs, not a cached proof of producer readiness. A miss
/// may reuse these only within the same node evaluation, never across the
/// postorder traversal that can rebuild its input state.
#[derive(Clone, PartialEq, Eq)]
pub(super) struct NodeMemoLookup {
    key: EvalMemoKey,
    input_watermark: u64,
    pub(super) depends_on_context: bool,
}

pub(super) struct FrameInputs<'a> {
    pub(super) slots: &'a [usize],
}

pub(super) struct TickEvaluator<'a> {
    pub(super) schema: &'a DatabaseSchema,
    pub(super) graph: &'a IvmGraph,
    pub(super) variant_projections: &'a HashMap<VariantProjectionKey, VariantProjection>,
    pub(super) table_deltas: &'a [TableDelta],
    pub(super) binding_deltas: &'a [BindingDelta],
    pub(super) binding_snapshots: &'a BindingSnapshots,
    pub(super) current_tick: u64,
    pub(super) operator_states: &'a mut HashMap<OperatorStateKey, OperatorState>,
    pub(super) arrangement_states: &'a mut HashMap<ArrangementKey, AsOf<ArrangementState, SubTick>>,
    pub(super) arrangement_keys_by_input: &'a mut HashMap<NodeId, HashSet<ArrangementKey>>,
    pub(super) eval_memo: &'a mut EvaluationMemo,
    pub(super) eval_memo_bytes: &'a mut usize,
    pub(super) table_frontiers: &'a HashMap<String, u64>,
    pub(super) binding_frontiers: &'a HashMap<BindingSourceKey, u64>,
    pub(super) memo_use_clock: &'a mut u64,
    pub(super) node_meta: &'a mut HashMap<NodeId, NodeRuntimeMeta>,
    pub(super) storage: Option<&'a dyn OrderedKvStorage>,
    pub(super) evaluation_inputs: Option<&'a mut super::evaluation_session::EvaluationInputs>,
    pub(super) context: EvalContext,
    pub(super) metrics: &'a mut TickMetrics,
    pub(super) terminal_deltas: HashMap<NodeId, TerminalDeltas>,
    /// Exact pre/post windows captured by TopBy evaluation for terminal
    /// ordering. Kept per node so nested collection ordering cannot be
    /// confused with public root ordering.
    pub(super) root_ordering_windows: HashMap<NodeId, RootOrderingWindows>,
}

/// Borrowed runtime pieces used by recursive evaluation to run child graphs.
/// This avoids giving recursion ownership of the whole [`IvmRuntime`].
pub(super) struct GraphRuntimeView<'a> {
    pub(super) schema: &'a DatabaseSchema,
    pub(super) graph: &'a IvmGraph,
    pub(super) variant_projections: &'a HashMap<VariantProjectionKey, VariantProjection>,
    pub(super) table_deltas: &'a [TableDelta],
    pub(super) binding_deltas: &'a [BindingDelta],
    pub(super) binding_snapshots: &'a BindingSnapshots,
    pub(super) current_tick: u64,
    pub(super) operator_states: &'a mut HashMap<OperatorStateKey, OperatorState>,
    pub(super) arrangement_states: &'a mut HashMap<ArrangementKey, AsOf<ArrangementState, SubTick>>,
    pub(super) arrangement_keys_by_input: &'a mut HashMap<NodeId, HashSet<ArrangementKey>>,
    pub(super) eval_memo: &'a mut EvaluationMemo,
    pub(super) eval_memo_bytes: &'a mut usize,
    pub(super) table_frontiers: &'a HashMap<String, u64>,
    pub(super) binding_frontiers: &'a HashMap<BindingSourceKey, u64>,
    pub(super) memo_use_clock: &'a mut u64,
    pub(super) node_meta: &'a mut HashMap<NodeId, NodeRuntimeMeta>,
    pub(super) storage: &'a dyn OrderedKvStorage,
    pub(super) evaluation_inputs: Option<&'a mut super::evaluation_session::EvaluationInputs>,
    pub(super) scope: ScopeId,
    pub(super) metrics: &'a mut TickMetrics,
}

#[allow(clippy::too_many_arguments)]
fn graph_runtime_view<'a>(
    schema: &'a DatabaseSchema,
    graph: &'a IvmGraph,
    variant_projections: &'a HashMap<VariantProjectionKey, VariantProjection>,
    table_deltas: &'a [TableDelta],
    binding_deltas: &'a [BindingDelta],
    binding_snapshots: &'a BindingSnapshots,
    current_tick: u64,
    operator_states: &'a mut HashMap<OperatorStateKey, OperatorState>,
    arrangement_states: &'a mut HashMap<ArrangementKey, AsOf<ArrangementState, SubTick>>,
    arrangement_keys_by_input: &'a mut HashMap<NodeId, HashSet<ArrangementKey>>,
    eval_memo: &'a mut EvaluationMemo,
    eval_memo_bytes: &'a mut usize,
    table_frontiers: &'a HashMap<String, u64>,
    binding_frontiers: &'a HashMap<BindingSourceKey, u64>,
    memo_use_clock: &'a mut u64,
    node_meta: &'a mut HashMap<NodeId, NodeRuntimeMeta>,
    storage: &'a dyn OrderedKvStorage,
    evaluation_inputs: Option<&'a mut super::evaluation_session::EvaluationInputs>,
    scope: ScopeId,
    metrics: &'a mut TickMetrics,
) -> GraphRuntimeView<'a> {
    GraphRuntimeView {
        schema,
        graph,
        variant_projections,
        table_deltas,
        binding_deltas,
        binding_snapshots,
        current_tick,
        operator_states,
        arrangement_states,
        arrangement_keys_by_input,
        eval_memo,
        eval_memo_bytes,
        table_frontiers,
        binding_frontiers,
        memo_use_clock,
        node_meta,
        storage,
        evaluation_inputs,
        scope,
        metrics,
    }
}

impl GraphRuntimeView<'_> {
    pub(super) async fn eval_with_binding(
        &mut self,
        sub_tick: u64,
        binding: FrontierName,
        deltas: RecordDeltas,
        node: NodeId,
    ) -> Result<RecordDeltas, IvmRuntimeError> {
        let mut evaluator = TickEvaluator {
            schema: self.schema,
            graph: self.graph,
            variant_projections: self.variant_projections,
            table_deltas: self.table_deltas,
            binding_deltas: self.binding_deltas,
            binding_snapshots: self.binding_snapshots,
            current_tick: self.current_tick,
            operator_states: self.operator_states,
            arrangement_states: self.arrangement_states,
            arrangement_keys_by_input: self.arrangement_keys_by_input,
            eval_memo: self.eval_memo,
            eval_memo_bytes: self.eval_memo_bytes,
            table_frontiers: self.table_frontiers,
            binding_frontiers: self.binding_frontiers,
            memo_use_clock: self.memo_use_clock,
            node_meta: self.node_meta,
            storage: Some(self.storage),
            evaluation_inputs: None,
            context: EvalContext::with_binding(self.scope, sub_tick, binding, deltas),
            metrics: self.metrics,
            terminal_deltas: HashMap::default(),
            root_ordering_windows: HashMap::default(),
        };
        evaluator
            .update_subgraph(node)
            .await
            .map(|records| records.as_ref().clone())
    }

    pub(super) async fn eval_with_binding_and_table_deltas(
        &mut self,
        table_deltas: &[TableDelta],
        sub_tick: u64,
        binding: FrontierName,
        deltas: RecordDeltas,
        node: NodeId,
    ) -> Result<RecordDeltas, IvmRuntimeError> {
        let mut isolated_memo = EvaluationMemo::default();
        let mut isolated_memo_bytes = 0usize;
        let mut context = EvalContext::with_binding_and_arrangement_mode(
            self.scope,
            sub_tick,
            binding,
            deltas,
            ArrangementUpdateMode::Replace,
        );
        if self.evaluation_inputs.is_some() {
            context.eval_mode = EvalMode::Hydrate;
        }
        let mut evaluator = TickEvaluator {
            schema: self.schema,
            graph: self.graph,
            variant_projections: self.variant_projections,
            table_deltas,
            binding_deltas: self.binding_deltas,
            binding_snapshots: self.binding_snapshots,
            current_tick: self.current_tick,
            operator_states: self.operator_states,
            arrangement_states: self.arrangement_states,
            arrangement_keys_by_input: self.arrangement_keys_by_input,
            eval_memo: &mut isolated_memo,
            eval_memo_bytes: &mut isolated_memo_bytes,
            table_frontiers: self.table_frontiers,
            binding_frontiers: self.binding_frontiers,
            memo_use_clock: self.memo_use_clock,
            node_meta: self.node_meta,
            storage: Some(self.storage),
            evaluation_inputs: self.evaluation_inputs.as_deref_mut(),
            context,
            metrics: self.metrics,
            terminal_deltas: HashMap::default(),
            root_ordering_windows: HashMap::default(),
        };
        evaluator
            .update_subgraph(node)
            .await
            .map(|records| records.as_ref().clone())
    }

    pub(super) fn clear_operator_state_for_scope(&mut self) {
        self.operator_states
            .retain(|key, _| key.scope != self.scope);
    }

    #[cfg_attr(
        feature = "cold-settle-attribution",
        tracing::instrument(skip_all, name = "cold.phase.ivm_hydrate")
    )]
    pub(super) async fn eval_root(
        &mut self,
        node: NodeId,
    ) -> Result<RecordDeltas, IvmRuntimeError> {
        let mut evaluator = TickEvaluator {
            schema: self.schema,
            graph: self.graph,
            variant_projections: self.variant_projections,
            table_deltas: self.table_deltas,
            binding_deltas: self.binding_deltas,
            binding_snapshots: self.binding_snapshots,
            current_tick: self.current_tick,
            operator_states: self.operator_states,
            arrangement_states: self.arrangement_states,
            arrangement_keys_by_input: self.arrangement_keys_by_input,
            eval_memo: self.eval_memo,
            eval_memo_bytes: self.eval_memo_bytes,
            table_frontiers: self.table_frontiers,
            binding_frontiers: self.binding_frontiers,
            memo_use_clock: self.memo_use_clock,
            node_meta: self.node_meta,
            storage: Some(self.storage),
            evaluation_inputs: None,
            context: EvalContext {
                scope: self.scope,
                sub_tick: 0,
                bindings: HashMap::default(),
                binding_digests: HashMap::default(),
                arrangement_update_mode: ArrangementUpdateMode::Accumulate,
                eval_mode: EvalMode::Tick,
                hydrate_arrangements: false,
            },
            metrics: self.metrics,
            terminal_deltas: HashMap::default(),
            root_ordering_windows: HashMap::default(),
        };
        evaluator
            .update_subgraph(node)
            .await
            .map(|records| records.as_ref().clone())
    }
}

impl TickEvaluator<'_> {
    pub(super) fn poll_ready_node(
        &mut self,
        node: NodeId,
        pending: &mut HashMap<NodeId, PendingUnaryBatch>,
        frame_inputs: FrameInputs<'_>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<Arc<RecordDeltas>, IvmRuntimeError>> {
        use std::task::Poll;
        if let std::collections::hash_map::Entry::Vacant(entry) = pending.entry(node) {
            let lookup = match self.prepare_memo_lookup(node) {
                Ok(lookup) => lookup,
                Err(error) => return Poll::Ready(Err(error)),
            };
            match self.cached_node_records(&lookup) {
                Ok(Some(records)) => return Poll::Ready(Ok(records)),
                Err(error) => return Poll::Ready(Err(error)),
                Ok(None) => {}
            }
            let resolved = if self.supports_resident_batch(node) {
                match self.resolve_register_inputs(frame_inputs) {
                    Ok(inputs) => inputs,
                    Err(error) => return Poll::Ready(Err(error)),
                }
            } else {
                None
            };
            let ready_inputs = resolved.as_deref();
            let resident_unary = ready_inputs.and_then(|inputs| {
                matches!(
                    self.graph.node(node)?.descriptor.operator,
                    OpType::Filter(_) | OpType::MapProject(_)
                )
                .then(|| inputs.first().cloned())
                .flatten()
            });
            let unary_input = resident_unary
                .map(Ok)
                .or_else(|| self.ready_unary_input(node, &lookup));
            let input = match unary_input {
                Some(Ok(input)) => input,
                Some(Err(error)) => return Poll::Ready(Err(error)),
                None => {
                    if let Some(result) = self.compute_ready_batch(node, &lookup, ready_inputs) {
                        return Poll::Ready(result);
                    }
                    return self.compute_node(node, lookup).as_mut().poll(cx);
                }
            };
            match self.prepare_unary_batch(node, &lookup, &input) {
                Ok(Some(batch)) => {
                    entry.insert(batch);
                }
                Ok(None) => {
                    let result = self.compute_unary_input(node, &input);
                    return Poll::Ready(result.map(|result| self.memoize_result(&lookup, result)));
                }
                Err(error) => return Poll::Ready(Err(error)),
            }
        }
        let batch = pending.get_mut(&node).expect("prepared unary batch");
        let graph_node = self.graph.node(node).expect("frame retains node");
        let end = (batch.next + UNARY_ROWS_PER_POLL).min(batch.input.deltas.len());
        let input = &batch.input.deltas[batch.next..end];
        #[cfg(feature = "cold-settle-attribution")]
        let started = std::time::Instant::now();
        let result = match &graph_node.descriptor.operator {
            OpType::MapProject(project) => NodeState::update_map_project_slice(
                project,
                batch.output.descriptor,
                batch.input.descriptor,
                input,
                batch.projection.as_deref(),
                false,
            ),
            OpType::Filter(filter) => {
                let mut deltas = Vec::new();
                let result = input.iter().try_for_each(|delta| {
                    if filter
                        .predicate
                        .matches(delta.borrowed(&batch.input.descriptor), filter.comparison)?
                    {
                        deltas.push(delta.clone());
                    }
                    Ok::<_, IvmRuntimeError>(())
                });
                result.map(|()| RecordDeltas {
                    descriptor: batch.output.descriptor,
                    deltas,
                })
            }
            _ => unreachable!("prepared unary operator"),
        };
        #[cfg(feature = "cold-settle-attribution")]
        {
            batch.elapsed_ns += started.elapsed().as_nanos() as u64;
        }
        match result {
            Err(error) => {
                pending.remove(&node);
                Poll::Ready(Err(error))
            }
            Ok(result) => {
                batch.output.deltas.extend(result.deltas);
                batch.next = end;
                if end < batch.input.deltas.len() {
                    // The caller owns wakeup and distinguishes this from a
                    // registered storage request, just like recursive yields.
                    return Poll::Pending;
                }
                let batch = pending.remove(&node).expect("completed unary batch");
                #[cfg(feature = "cold-settle-attribution")]
                if let OpType::MapProject(project) = &graph_node.descriptor.operator {
                    crate::cold_settle_attribution::record_map_node(
                        node.0,
                        self.context.eval_mode == EvalMode::Hydrate,
                        batch.input.deltas.len(),
                        batch.output.deltas.len(),
                        batch.elapsed_ns,
                        || {
                            format!(
                                "inputs={:?} projection={project:?}",
                                graph_node.descriptor.inputs
                            )
                        },
                    );
                    crate::cold_settle_attribution::record_map(
                        self.context.eval_mode == EvalMode::Hydrate,
                        batch.input.deltas.len(),
                        batch.output.deltas.len(),
                    );
                }
                Poll::Ready(Ok(self.memoize_result(&batch.lookup, batch.output)))
            }
        }
    }

    fn prepare_unary_batch(
        &mut self,
        node: NodeId,
        lookup: &NodeMemoLookup,
        input: &Arc<RecordDeltas>,
    ) -> Result<Option<PendingUnaryBatch>, IvmRuntimeError> {
        if input.deltas.len() <= UNARY_ROWS_PER_POLL {
            return Ok(None);
        }
        let graph_node = self
            .graph
            .node(node)
            .ok_or(IvmRuntimeError::GraphNodeNotFound(node))?;
        let output = graph_node.descriptor.output.records();
        let projection = match &graph_node.descriptor.operator {
            OpType::MapProject(project) => {
                self.raw_projection_fields(node, project, &input.descriptor, output)?
            }
            OpType::Filter(filter) => {
                let mut referenced = BTreeSet::new();
                filter.predicate.referenced_fields(&mut referenced);
                // Indirect fields have request/error ordering semantics. Keep
                // them on the existing kernel until their resumable stages
                // can retain both materialization and predicate cursors.
                for field in referenced {
                    let Some(index) = resolve_field_name(&input.descriptor, &field) else {
                        return Ok(None);
                    };
                    if input.descriptor.fields()[index]
                        .value_type
                        .may_contain_stored_scalar()
                    {
                        return Ok(None);
                    }
                }
                None
            }
            _ => unreachable!("checked unary operator"),
        };
        if self.context.eval_mode == EvalMode::Hydrate {
            self.metrics.hydration_memo_computes += 1;
            self.metrics.hydration_memo_computed_nodes.insert(node);
        }
        Ok(Some(PendingUnaryBatch {
            lookup: lookup.clone(),
            input: Arc::clone(input),
            next: 0,
            output: RecordDeltas::empty(output),
            projection,
            #[cfg(feature = "cold-settle-attribution")]
            elapsed_ns: 0,
        }))
    }

    /// Evaluate one reachable graph slice in dependency order.
    ///
    /// `update_node` may ask for its direct inputs, but those calls are memo
    /// hits because every child has completed in the same evaluation context.
    /// Keeping graph traversal here iterative makes stack use independent of
    /// graph depth, including recursive seed/step scopes which do not use the
    /// outer tick work queue.
    #[cfg_attr(
        feature = "cold-settle-attribution",
        tracing::instrument(skip_all, name = "cold.phase.ivm_update")
    )]
    pub(super) async fn update_subgraph(
        &mut self,
        root: NodeId,
    ) -> Result<Arc<RecordDeltas>, IvmRuntimeError> {
        #[cfg(test)]
        SUBGRAPH_WALKS.with(|count| count.set(count.get() + 1));
        let mut pending = vec![(root, false)];
        let mut discovered = HashSet::new();
        let mut order = Vec::new();
        while let Some((node, expanded)) = pending.pop() {
            if expanded {
                order.push(node);
                continue;
            }
            if !discovered.insert(node) {
                continue;
            }
            let graph_node = self
                .graph
                .node(node)
                .ok_or(IvmRuntimeError::GraphNodeNotFound(node))?;
            pending.push((node, true));
            // Recursive seed and step graphs run under a frontier-scoped
            // evaluator in `update_recursive`; evaluating them here would
            // incorrectly populate root-scoped memo and operator state.
            if !matches!(graph_node.descriptor.operator, OpType::Recursive(_)) {
                pending.extend(
                    graph_node
                        .descriptor
                        .inputs
                        .iter()
                        .rev()
                        .map(|input| (*input, false)),
                );
            }
        }

        let mut result = None;
        for node in order {
            let records = self.update_ready_node(node).await?;
            if node == root {
                result = Some(records);
            }
        }
        result.ok_or(IvmRuntimeError::GraphNodeNotFound(root))
    }

    pub(super) fn apply_root_ordering(
        &self,
        ordering_node: NodeId,
        root_descriptor: RecordDescriptor,
        identity: Option<(&RootIdentity, &BTreeSet<Vec<u8>>)>,
        terminal: &mut TerminalDeltas,
    ) -> Result<(), IvmRuntimeError> {
        let Some(windows) = self.root_ordering_windows.get(&ordering_node) else {
            return Ok(());
        };
        let Some((identity, groups)) = identity else {
            let positions = windows.field_zero()?;
            apply_root_ordering_operations(
                &positions.before,
                &positions.after,
                root_descriptor,
                terminal,
            );
            return Ok(());
        };
        let Some(descriptor) = windows.descriptor else {
            return Ok(());
        };
        let key_of = |record: &[u8]| {
            if !identity.projected {
                return encoded_identity_key_part(descriptor, record, &windows.identity);
            }
            match project_window_record(self.graph, &identity.chain, descriptor, record)? {
                Some((output, projected)) => {
                    encoded_identity_key_part(output, &projected, &identity.fields)
                }
                // Never an output root; `reaching` below drops it.
                None => Ok(Vec::new()),
            }
        };
        if !identity.filtered && !identity.projected {
            for group in groups {
                if let Some((before, after)) = windows.group_window(group) {
                    apply_group_window_ordering(before, after, &key_of, root_descriptor, terminal)?;
                }
            }
            return Ok(());
        }
        // What this tick's output edits say about each root key: present
        // before (removed or updated) and present after (inserted or updated).
        let mut evidence = HashMap::<Vec<u8>, (bool, bool)>::default();
        for operation in &terminal.operations {
            let seen = evidence.entry(operation.root_key.clone()).or_default();
            match &operation.edit {
                TerminalEdit::Insert { .. } if operation.path.is_empty() => seen.1 = true,
                TerminalEdit::Remove { .. } if operation.path.is_empty() => seen.0 = true,
                _ => *seen = (true, true),
            }
        }
        // Indices are positions among the output's roots, so window rows the
        // chain drops take no position. A row whose filter reads an unloaded
        // large value is placed by this tick's edits of its key; with none, an
        // unchanged row keeps its slot and a changed one is not a root (it
        // would otherwise have an edit).
        let reaching = |window: &[WindowedRecord], other: &[WindowedRecord], after: bool| {
            let mut kept = Vec::with_capacity(window.len());
            for entry in window {
                let reaches = match window_record_reaches_output(
                    self.graph,
                    &identity.chain,
                    descriptor,
                    &entry.0,
                )? {
                    WindowReach::Yes => true,
                    WindowReach::No => false,
                    WindowReach::Unknown => match evidence.get(&key_of(&entry.0)?) {
                        Some((before, now)) => {
                            if after {
                                *now
                            } else {
                                *before
                            }
                        }
                        None => other.iter().any(|(record, _)| record == &entry.0),
                    },
                };
                if reaches {
                    kept.push(entry.clone());
                }
            }
            Ok::<_, IvmRuntimeError>(kept)
        };
        for group in groups {
            let Some((before, after)) = windows.group_window(group) else {
                continue;
            };
            let (before, after) = (
                reaching(before, after, false)?,
                reaching(after, before, true)?,
            );
            apply_group_window_ordering(&before, &after, &key_of, root_descriptor, terminal)?;
        }
        Ok(())
    }

    pub(super) fn terminal_delta_node_for_output(
        &mut self,
        node: NodeId,
    ) -> Result<Option<NodeId>, IvmRuntimeError> {
        let lineage = self.terminal_lineage(node)?;
        let mut fallback = None;
        for &(node, is_public_root) in &lineage.candidates {
            if self.terminal_deltas.contains_key(&node) {
                if is_public_root {
                    return Ok(Some(node));
                }
                fallback.get_or_insert(node);
            }
        }
        Ok((!lineage.has_public_root).then_some(fallback).flatten())
    }

    pub(super) fn terminal_deltas_for_consumer(
        &mut self,
        node: NodeId,
        last_consumer: bool,
    ) -> Option<TerminalDeltas> {
        if last_consumer {
            self.terminal_deltas.remove(&node)
        } else {
            self.terminal_deltas.get(&node).cloned()
        }
    }

    pub(super) fn output_is_structured_collect_by(
        &self,
        node: NodeId,
    ) -> Result<bool, IvmRuntimeError> {
        output_is_structured_collect_by(self.graph, node)
    }

    pub(super) fn output_has_public_root(&mut self, node: NodeId) -> Result<bool, IvmRuntimeError> {
        Ok(self.terminal_lineage(node)?.has_public_root)
    }

    fn terminal_lineage(&mut self, root: NodeId) -> Result<Arc<TerminalLineage>, IvmRuntimeError> {
        if let Some(lineage) = self
            .node_meta
            .get(&root)
            .and_then(|meta| meta.terminal_lineage.as_ref())
        {
            return Ok(Arc::clone(lineage));
        }
        let mut lineage = TerminalLineage {
            candidates: Vec::new(),
            has_public_root: false,
        };
        let mut pending = vec![root];
        let mut seen = HashSet::new();
        while let Some(node) = pending.pop() {
            if !seen.insert(node) {
                continue;
            }
            let graph_node = self
                .graph
                .node(node)
                .ok_or(IvmRuntimeError::GraphNodeNotFound(node))?;
            // CollectBy is the only operator publishing into terminal_deltas.
            // Preserve the old depth-first/input-reverse priority exactly.
            if let OpType::CollectBy(collect_by) = &graph_node.descriptor.operator {
                let public = collect_by.mode == CollectByMode::Root;
                lineage.has_public_root |= public;
                lineage.candidates.push((node, public));
            }
            pending.extend(graph_node.descriptor.inputs.iter().copied());
        }
        let lineage = Arc::new(lineage);
        self.node_meta.entry(root).or_default().terminal_lineage = Some(Arc::clone(&lineage));
        Ok(lineage)
    }

    fn node_depends_on_aggregate(&mut self, node: NodeId) -> Result<bool, IvmRuntimeError> {
        if let Some(value) = self
            .node_meta
            .get(&node)
            .and_then(|meta| meta.has_hydration_state_ancestor)
        {
            return Ok(value);
        }
        // Node descriptors and input edges are immutable while installed. The
        // metadata is retired with the node; consumer attachment and runtime
        // state cleanup do not change this ancestor classification.
        let mut ancestors = HashSet::new();
        self.graph.mark_ancestors(node, &mut ancestors);
        let mut depends = false;
        for ancestor in ancestors {
            let graph_node = self
                .graph
                .node(ancestor)
                .ok_or(IvmRuntimeError::GraphNodeNotFound(ancestor))?;
            if matches!(
                graph_node.descriptor.operator,
                OpType::Aggregate(_)
                    | OpType::ArgMinBy(_)
                    | OpType::ArgMaxBy(_)
                    | OpType::Arrange(_)
            ) {
                depends = true;
                break;
            }
        }
        self.node_meta
            .entry(node)
            .or_default()
            .has_hydration_state_ancestor = Some(depends);
        Ok(depends)
    }

    fn aggregate_arrangements_are_current(
        &mut self,
        node: NodeId,
    ) -> Result<bool, IvmRuntimeError> {
        let frontier = self.readiness_frontier(node)?;
        for &producer in frontier.iter() {
            if !self.producer_state_is_current(producer)? {
                return Ok(false);
            }
        }
        Ok(true)
    }

    fn readiness_frontier(&mut self, root: NodeId) -> Result<Arc<[NodeId]>, IvmRuntimeError> {
        if let Some(frontier) = self
            .node_meta
            .get(&root)
            .and_then(|meta| meta.readiness_frontier.as_ref())
        {
            return Ok(Arc::clone(frontier));
        }
        let mut frontier = Vec::new();
        let mut pending = vec![root];
        let mut seen = HashSet::new();
        while let Some(node) = pending.pop() {
            if !seen.insert(node) {
                continue;
            }
            let graph_node = self
                .graph
                .node(node)
                .ok_or(IvmRuntimeError::GraphNodeNotFound(node))?;
            match &graph_node.descriptor.operator {
                OpType::Recursive(_) => {
                    // Child scopes are owned by this producer. Its live proof
                    // includes completion, input generation and step readiness.
                    frontier.push(node);
                    continue;
                }
                OpType::Arrange(_)
                | OpType::ArgMinBy(_)
                | OpType::ArgMaxBy(_)
                | OpType::Aggregate(_) => frontier.push(node),
                _ => {}
            }
            // Match the original recursive walk's input order, including the
            // first occurrence of a shared producer. No dynamic fact is saved.
            pending.extend(graph_node.descriptor.inputs.iter().rev().copied());
        }
        let frontier: Arc<[NodeId]> = frontier.into();
        self.node_meta.entry(root).or_default().readiness_frontier = Some(Arc::clone(&frontier));
        Ok(frontier)
    }

    fn producer_state_is_current(&mut self, node: NodeId) -> Result<bool, IvmRuntimeError> {
        let graph = self.graph;
        let graph_node = graph
            .node(node)
            .ok_or(IvmRuntimeError::GraphNodeNotFound(node))?;
        let operator = &graph_node.descriptor.operator;
        let inputs = &graph_node.descriptor.inputs;
        if matches!(operator, OpType::Recursive(_)) {
            // The recursive operator owns its seed/step scopes. Their local
            // indexes must not be looked up using this caller's scope. Its
            // completed hydration is the proof that those child scopes are
            // ready; stale or suspended recursion still needs rebuilding.
            let key = self.operator_key(node);
            let generation = self.input_generation(node);
            return Ok(matches!(
                self.operator_states.get(&key),
                Some(OperatorState::Recursive(state))
                    if state.as_of() == Some(Tick(self.current_tick))
                        && !state.value().has_pending_hydration()
                        && state.value().step_arrangements_hydrated()
                        && state.value().hydrated_input_generation() == Some(generation)
            ));
        }
        if matches!(operator, OpType::Arrange(_)) && self.arrangement_needs_index(node) {
            let key = ArrangementKey {
                scope: self.context.scope,
                input: node,
            };
            if self.arrangement_states.get(&key).and_then(AsOf::as_of)
                != Some(self.arrangement_sub_tick(&key))
            {
                return Ok(false);
            }
        }
        if matches!(operator, OpType::ArgMinBy(_) | OpType::ArgMaxBy(_)) {
            let key = self.operator_key(node);
            let expected = SubTick {
                tick: self.current_tick,
                sub_tick: if key.scope == ScopeId::root() {
                    0
                } else {
                    self.context.sub_tick
                },
            };
            if !matches!(self.operator_states.get(&key), Some(OperatorState::ArgBy(state)) if state.as_of() == Some(expected))
            {
                return Ok(false);
            }
        }
        if let OpType::Aggregate(aggregate) = operator {
            let [input] = inputs.as_slice() else {
                return Err(IvmRuntimeError::GraphInputArityMismatch(node));
            };
            let input_desc = self
                .graph
                .node(*input)
                .ok_or(IvmRuntimeError::GraphNodeNotFound(*input))?
                .descriptor
                .output;
            let group_fields = self.aggregate_group_fields(node, aggregate);
            let arrangement_key = self.arrangement_key(
                *input,
                input_desc.records(),
                &group_fields,
                ValueComparison::Exact,
            )?;
            if self
                .arrangement_states
                .get(&arrangement_key)
                .and_then(AsOf::as_of)
                != Some(self.arrangement_sub_tick(&arrangement_key))
            {
                return Ok(false);
            }
        }
        Ok(true)
    }

    pub(super) fn update_node(&mut self, node: NodeId) -> ReadyNodeEvaluation<'_> {
        // The postorder driver has already evaluated ordinary inputs. Check
        // their memo before entering another evaluator future: even a cache
        // hit inside compute_node would recursively poll that wide future
        // beneath its parent, overflowing Safari's WebAssembly call stack.
        match self
            .prepare_memo_lookup(node)
            .and_then(|lookup| self.cached_node_records(&lookup))
        {
            Ok(Some(records)) => ReadyNodeEvaluation::ready(Ok(records)),
            Err(error) => ReadyNodeEvaluation::ready(Err(error)),
            Ok(None) => ReadyNodeEvaluation::Deferred(Box::pin(self.update_subgraph(node))),
        }
    }

    pub(super) fn prepare_memo_lookup(
        &mut self,
        node: NodeId,
    ) -> Result<NodeMemoLookup, IvmRuntimeError> {
        let prepare =
            |this: &Self, signature: &NodeInputSignature, input_watermark| NodeMemoLookup {
                key: this.memo_key(node, signature),
                input_watermark,
                depends_on_context: !signature.frontier_bindings.is_empty(),
            };
        // The common path borrows the compiled signature and its current
        // watermark together. No Arc clone or reference survives this call.
        if let Some(meta) = self.node_meta.get(&node)
            && let Some(signature) = meta.input_signature.as_deref()
        {
            return Ok(prepare(self, signature, meta.input_generation));
        }
        let signature = self.input_signature(node)?;
        Ok(prepare(self, &signature, self.input_generation(node)))
    }

    pub(super) fn cached_node_records(
        &mut self,
        lookup: &NodeMemoLookup,
    ) -> Result<Option<Arc<RecordDeltas>>, IvmRuntimeError> {
        let node = lookup.key.node;
        let current_watermark = lookup.input_watermark;
        // Readiness is necessary only when reusing a result. A missing or
        // invalidated memo will execute the producer normally below.
        if self
            .eval_memo
            .get(&lookup.key)
            .is_none_or(|entry| entry.input_watermark != current_watermark)
        {
            return Ok(None);
        }
        if !self.cached_result_state_is_current(node)? {
            return Ok(None);
        }
        if let Some(entry) = self.eval_memo.get_mut(&lookup.key)
            && entry.input_watermark == current_watermark
        {
            *self.memo_use_clock += 1;
            entry.last_used = *self.memo_use_clock;
            if self.context.eval_mode == EvalMode::Hydrate {
                self.metrics.hydration_memo_hits += 1;
            }
            return Ok(Some(Arc::clone(&entry.records)));
        }
        Ok(None)
    }

    fn cached_result_state_is_current(&mut self, node: NodeId) -> Result<bool, IvmRuntimeError> {
        // A cached record batch is not proof that its producer-owned physical
        // index exists in this scope. Hydration can reuse records from a probe,
        // and recursive child state may have been retired independently.
        let requires_producer = if matches!(
            self.graph.node(node).map(|node| &node.descriptor.operator),
            Some(OpType::Arrange(_))
        ) && self.arrangement_needs_index(node)
        {
            let key = ArrangementKey {
                scope: self.context.scope,
                input: node,
            };
            !self.arrangement_states.contains_key(&key)
        } else {
            false
        };
        let requires_state_rebuild = requires_producer
            || (self.context.hydrate_arrangements
                && self.node_depends_on_aggregate(node)?
                && !self.aggregate_arrangements_are_current(node)?)
            || (self.context.eval_mode == EvalMode::Tick
                && self.context.arrangement_update_mode == ArrangementUpdateMode::Replace);
        Ok(!requires_state_rebuild)
    }

    pub(super) fn resolve_register_inputs(
        &mut self,
        inputs: FrameInputs<'_>,
    ) -> Result<Option<smallvec::SmallVec<[Arc<RecordDeltas>; 2]>>, IvmRuntimeError> {
        let mut resolved = smallvec::SmallVec::new();
        for &slot in inputs.slots {
            let Some((key, _)) = self.eval_memo.slot(slot) else {
                return Ok(None);
            };
            let node = key.node;
            let lookup = self.prepare_memo_lookup(node)?;
            if self.eval_memo.slot(slot).is_none_or(|(key, entry)| {
                *key != lookup.key || entry.input_watermark != lookup.input_watermark
            }) || !self.cached_result_state_is_current(node)?
            {
                return Ok(None);
            }
            // Readiness is checked live above, never stored in the register.
            let (_, entry) = self.eval_memo.slot_mut(slot).expect("validated frame slot");
            *self.memo_use_clock += 1;
            entry.last_used = *self.memo_use_clock;
            if self.context.eval_mode == EvalMode::Hydrate {
                self.metrics.hydration_memo_hits += 1;
            }
            resolved.push(Arc::clone(&entry.records));
        }
        Ok(Some(resolved))
    }

    /// Execute a node whose ordinary inputs have already been driven by the
    /// caller's dependency queue (or scoped postorder driver).
    ///
    /// Do not rediscover its ancestors here. A completed input is not proof of
    /// producer-index readiness: preserve the normal memo checks, and let an
    /// operator request an input rebuild through `update_node` when necessary.
    /// Recursive operators still own their frontier-scoped child evaluation.
    pub(super) fn update_ready_node(&mut self, node: NodeId) -> ReadyNodeEvaluation<'_> {
        let lookup = match self.prepare_memo_lookup(node) {
            Ok(lookup) => lookup,
            Err(error) => return ReadyNodeEvaluation::ready(Err(error)),
        };
        match self.cached_node_records(&lookup) {
            Ok(Some(records)) => ReadyNodeEvaluation::ready(Ok(records)),
            Err(error) => ReadyNodeEvaluation::ready(Err(error)),
            Ok(None) => {
                if let Some(result) = self.compute_ready_batch(node, &lookup, None) {
                    return ReadyNodeEvaluation::ready(result);
                }
                ReadyNodeEvaluation::Deferred(self.compute_node(node, lookup))
            }
        }
    }

    fn ready_unary_input(
        &mut self,
        node: NodeId,
        lookup: &NodeMemoLookup,
    ) -> Option<Result<Arc<RecordDeltas>, IvmRuntimeError>> {
        if self.context.sub_tick > 1 && !lookup.depends_on_context {
            return None;
        }
        let graph_node = self.graph.node(node)?;
        if !matches!(
            graph_node.descriptor.operator,
            OpType::Filter(_) | OpType::MapProject(_)
        ) {
            return None;
        }
        let [input] = graph_node.descriptor.inputs.as_slice() else {
            return Some(Err(IvmRuntimeError::GraphInputArityMismatch(node)));
        };
        match self
            .prepare_memo_lookup(*input)
            .and_then(|key| self.cached_node_records(&key))
        {
            Ok(Some(input)) => Some(Ok(input)),
            Ok(None) => None,
            Err(error) => Some(Err(error)),
        }
    }

    fn compute_unary_input(
        &mut self,
        node: NodeId,
        input: &Arc<RecordDeltas>,
    ) -> Result<RecordDeltas, IvmRuntimeError> {
        let graph_node = self.graph.node(node).expect("checked unary node");
        let output = graph_node.descriptor.output.records();
        if self.context.eval_mode == EvalMode::Hydrate {
            self.metrics.hydration_memo_computes += 1;
            self.metrics.hydration_memo_computed_nodes.insert(node);
        }
        match &graph_node.descriptor.operator {
            OpType::Filter(filter) => self.compute_filter(node, filter, output, input),
            OpType::MapProject(project) => self.compute_projection(node, project, output, input),
            _ => unreachable!("checked unary kernel"),
        }
    }

    pub(super) fn memoize_result(
        &mut self,
        lookup: &NodeMemoLookup,
        result: RecordDeltas,
    ) -> Arc<RecordDeltas> {
        self.metrics.records_processed += result.deltas.len();
        self.metrics.nodes_evaluated += 1;
        let result = Arc::new(result);
        let payload_bytes = record_deltas_encoded_bytes(&result);
        *self.memo_use_clock += 1;
        if let Some(previous) = self.eval_memo.insert(
            lookup.key.clone(),
            EvalMemoEntry::new(
                Arc::clone(&result),
                lookup.input_watermark,
                payload_bytes,
                *self.memo_use_clock,
            ),
        ) {
            *self.eval_memo_bytes = self.eval_memo_bytes.saturating_sub(previous.payload_bytes);
        }
        *self.eval_memo_bytes = self.eval_memo_bytes.saturating_add(payload_bytes);
        result
    }

    fn compute_filter(
        &mut self,
        node: NodeId,
        filter: &FilterOp,
        output_desc: RecordDescriptor,
        input: &Arc<RecordDeltas>,
    ) -> Result<RecordDeltas, IvmRuntimeError> {
        if filter.predicate.supports_indirect_literal_attempt() && self.evaluation_inputs.is_some()
        {
            let inputs = self
                .evaluation_inputs
                .as_deref_mut()
                .expect("checked evaluation inputs");
            let mut deltas = Vec::new();
            for delta in &input.deltas {
                let record = delta.borrowed(&input.descriptor);
                inputs.set_chunk_scope(Some(node));
                let result = filter
                    .predicate
                    .matches_indirect_literal_attempt(record, inputs);
                inputs.set_chunk_scope(None);
                let matches = match result? {
                    Some(matches) => matches,
                    None => filter.predicate.matches(record, filter.comparison)?,
                };
                if matches {
                    deltas.push(delta.clone());
                }
            }
            Ok(RecordDeltas {
                descriptor: output_desc,
                deltas,
            })
        } else {
            let mut referenced = BTreeSet::new();
            filter.predicate.referenced_fields(&mut referenced);
            let materialized = self.materialize_indirect_fields(input, &referenced)?;
            if Arc::ptr_eq(&materialized, input) {
                return NodeState::update_filter(filter, output_desc, input);
            }
            // Materialization only serves the predicate. Emit the rows as they
            // arrived, so downstream keys (TopBy root identity, #3309) see the
            // same physical form as upstream state; publication loads indirect
            // values for every output anyway.
            let mut deltas = Vec::new();
            for (delta, loaded) in input.deltas.iter().zip(&materialized.deltas) {
                if filter
                    .predicate
                    .matches(loaded.borrowed(&materialized.descriptor), filter.comparison)?
                {
                    deltas.push(delta.clone());
                }
            }
            Ok(RecordDeltas {
                descriptor: output_desc,
                deltas,
            })
        }
    }

    fn compute_projection(
        &mut self,
        node: NodeId,
        project: &MapProjectOp,
        output_desc: RecordDescriptor,
        input: &RecordDeltas,
    ) -> Result<RecordDeltas, IvmRuntimeError> {
        #[cfg(feature = "cold-settle-attribution")]
        let projection_started = std::time::Instant::now();
        let raw_projection =
            self.raw_projection_fields(node, project, &input.descriptor, output_desc)?;
        let result = NodeState::update_map_project(
            project,
            output_desc,
            input,
            raw_projection.as_deref(),
            false,
        );
        #[cfg(feature = "cold-settle-attribution")]
        if let Ok(output) = &result {
            crate::cold_settle_attribution::record_map_node(
                node.0,
                self.context.eval_mode == EvalMode::Hydrate,
                input.deltas.len(),
                output.deltas.len(),
                projection_started.elapsed().as_nanos() as u64,
                || {
                    format!(
                        "inputs={:?} projection={project:?}",
                        self.graph.node(node).unwrap().descriptor.inputs
                    )
                },
            );
            crate::cold_settle_attribution::record_map(
                self.context.eval_mode == EvalMode::Hydrate,
                input.deltas.len(),
                output.deltas.len(),
            );
        }
        result
    }

    /// Construct the large operator future only on a memo miss. Keep the
    /// prepared lookup within this evaluation; no driver runs between it and
    /// the compute, and a blocked/yielded retry prepares a fresh lookup.
    /// Only external requests and recursive scopes need a future. Ordinary
    /// kernels share compute_batch with the scheduler's synchronous path.
    pub(super) fn compute_node(
        &mut self,
        node: NodeId,
        lookup: NodeMemoLookup,
    ) -> StorageFuture<'_, Result<Arc<RecordDeltas>, IvmRuntimeError>> {
        #[cfg(test)]
        ASYNC_NODE_FRAMES.with(|count| count.set(count.get() + 1));
        Box::pin(async move {
            self.note_hydration_compute(node);
            if self.context.sub_tick > 1 && !lookup.depends_on_context {
                let output = self
                    .graph
                    .node(node)
                    .ok_or(IvmRuntimeError::GraphNodeNotFound(node))?
                    .descriptor
                    .output
                    .records();
                return Ok(self.memoize_result(&lookup, RecordDeltas::empty(output)));
            }
            let graph_node = self
                .graph
                .node(node)
                .ok_or(IvmRuntimeError::GraphNodeNotFound(node))?;
            let output_desc = graph_node.descriptor.output.records();
            let result = match &graph_node.descriptor.operator {
                OpType::IndexSource(input)
                    if self.context.eval_mode != EvalMode::Hydrate
                        || self.evaluation_inputs.is_none() =>
                {
                    NodeState::update_index_source(
                        input,
                        self.schema,
                        self.variant_projections,
                        &output_desc,
                        self.table_deltas,
                        self.storage,
                        self.context.eval_mode,
                    )
                    .await
                }
                OpType::StreamingChecksum(checksum) => {
                    let input = self.update_unary_input(graph_node, node).await?;
                    self.update_streaming_checksum(node, checksum, output_desc, input)
                        .await
                }
                OpType::Recursive(recursive) => {
                    let (seed, step, witness) = match graph_node.descriptor.inputs.as_slice() {
                        [seed, step] => (*seed, *step, None),
                        [seed, step, witness] => (*seed, *step, Some(*witness)),
                        _ => return Err(IvmRuntimeError::GraphInputArityMismatch(node)),
                    };
                    self.update_recursive(node, recursive, output_desc, seed, step, witness)
                        .await
                }
                _ => {
                    let mut inputs = smallvec::SmallVec::<[Arc<RecordDeltas>; 2]>::new();
                    for input in &graph_node.descriptor.inputs {
                        inputs.push(self.update_node(*input).await?);
                    }
                    self.compute_batch(node, &inputs)
                }
            }?;
            Ok(self.memoize_result(&lookup, result))
        })
    }

    fn note_hydration_compute(&mut self, node: NodeId) {
        if self.context.eval_mode == EvalMode::Hydrate {
            self.metrics.hydration_memo_computes += 1;
            self.metrics.hydration_memo_computed_nodes.insert(node);
        }
    }

    /// Resolve every input before mutating operator state. A resident batch
    /// alone is insufficient: memo lookup still verifies physical producers
    /// and recursive scope stamps, including hydration Replace semantics.
    fn compute_ready_batch(
        &mut self,
        node: NodeId,
        lookup: &NodeMemoLookup,
        ready_inputs: Option<&[Arc<RecordDeltas>]>,
    ) -> Option<Result<Arc<RecordDeltas>, IvmRuntimeError>> {
        let graph_node = self.graph.node(node)?;
        if self.context.sub_tick > 1 && !lookup.depends_on_context {
            let output = graph_node.descriptor.output.records();
            self.note_hydration_compute(node);
            return Some(Ok(self.memoize_result(lookup, RecordDeltas::empty(output))));
        }
        if !self.supports_resident_batch(node) {
            return None;
        }
        if let Some(inputs) = ready_inputs {
            self.note_hydration_compute(node);
            return Some(
                self.compute_batch(node, inputs)
                    .map(|records| self.memoize_result(lookup, records)),
            );
        }
        let mut inputs = smallvec::SmallVec::<[Arc<RecordDeltas>; 2]>::new();
        for input in &graph_node.descriptor.inputs {
            match self
                .prepare_memo_lookup(*input)
                .and_then(|key| self.cached_node_records(&key))
            {
                Ok(Some(records)) => inputs.push(records),
                Ok(None) => return None,
                Err(error) => return Some(Err(error)),
            }
        }
        self.note_hydration_compute(node);
        Some(
            self.compute_batch(node, &inputs)
                .map(|records| self.memoize_result(lookup, records)),
        )
    }

    fn supports_resident_batch(&self, node: NodeId) -> bool {
        match self.graph.node(node).map(|node| &node.descriptor.operator) {
            None | Some(OpType::Recursive(_) | OpType::StreamingChecksum(_)) => false,
            Some(OpType::IndexSource(_)) => {
                self.context.eval_mode == EvalMode::Hydrate && self.evaluation_inputs.is_some()
            }
            Some(_) => true,
        }
    }

    pub(super) fn memo_key(&self, node: NodeId, signature: &NodeInputSignature) -> EvalMemoKey {
        EvalMemoKey {
            scope: self.context.scope,
            node,
            input_signature_hash: signature.hash,
            tick_epoch: match self.context.eval_mode {
                EvalMode::Tick => Some(self.current_tick),
                EvalMode::Hydrate => None,
            },
            sub_tick: self.context.sub_tick,
            context_digest: self.context_digest(signature),
        }
    }

    pub(super) fn input_generation(&self, node: NodeId) -> u64 {
        self.node_meta
            .get(&node)
            .map(|meta| meta.input_generation)
            .unwrap_or_default()
    }

    pub(super) fn context_digest(&self, signature: &NodeInputSignature) -> u64 {
        if signature.frontier_bindings.is_empty() {
            return 0;
        }
        let mut hasher = DefaultHasher::new();
        for binding in signature.frontier_bindings.iter() {
            binding.hash(&mut hasher);
            self.context
                .binding_digests
                .get(binding)
                .copied()
                .unwrap_or_default()
                .hash(&mut hasher);
        }
        hasher.finish()
    }

    fn operator_key(&self, node: NodeId) -> OperatorStateKey {
        // Recursive step evaluation must be isolated per recursive node even
        // for context-independent table/index inputs. Sibling recursive nodes
        // can evaluate the same base-table delta in one outer tick; sharing
        // root-scoped child operator state would let the first sibling advance
        // the table side and make later siblings miss the same positive edge.
        // Scoped child operator state is tick-local and is cleared before the
        // public tick exits.
        // The root evaluator naturally keeps unrelated root queries sharing
        // state. Dependency discovery cannot change either scope choice.
        OperatorStateKey {
            scope: self.context.scope,
            node,
        }
    }

    pub(super) fn input_signature(
        &mut self,
        node: NodeId,
    ) -> Result<Arc<NodeInputSignature>, IvmRuntimeError> {
        self.input_signature_inner(node, &mut HashSet::new())
    }

    fn input_signature_inner(
        &mut self,
        node: NodeId,
        seen: &mut HashSet<NodeId>,
    ) -> Result<Arc<NodeInputSignature>, IvmRuntimeError> {
        if let Some(signature) = self
            .node_meta
            .get(&node)
            .and_then(|meta| meta.input_signature.clone())
        {
            return Ok(signature);
        }
        if !seen.insert(node) {
            return Ok(Arc::new(NodeInputSignature::default()));
        }
        let graph_node = self
            .graph
            .node(node)
            .ok_or(IvmRuntimeError::GraphNodeNotFound(node))?;
        let operator = graph_node.descriptor.operator.clone();
        let inputs = graph_node.descriptor.inputs.clone();
        let mut tables = BTreeSet::new();
        let mut bindings = BTreeSet::new();
        let mut frontier_bindings = BTreeSet::new();
        match operator {
            OpType::TableSource(input) => {
                tables.insert(input.table);
            }
            OpType::IndexSource(input) => {
                tables.insert(input.table);
            }
            OpType::BindingSource(input) => {
                bindings.insert(input.key);
            }
            OpType::FrontierSource(input) => {
                frontier_bindings.insert(input.binding);
            }
            _ => {}
        };
        for input in inputs {
            let child = self.input_signature_inner(input, seen)?;
            tables.extend(child.tables.iter().cloned());
            bindings.extend(child.bindings.iter().cloned());
            frontier_bindings.extend(child.frontier_bindings.iter().cloned());
        }
        seen.remove(&node);
        let signature = Arc::new(NodeInputSignature::from_sets(
            tables,
            bindings,
            frontier_bindings,
        ));
        let depends_on_context = !signature.frontier_bindings.is_empty();
        let meta = self.node_meta.entry(node).or_default();
        meta.depends_on_context = Some(depends_on_context);
        meta.input_signature = Some(Arc::clone(&signature));
        Ok(signature)
    }

    pub(super) fn raw_projection_fields(
        &mut self,
        node: NodeId,
        project: &MapProjectOp,
        input_desc: &RecordDescriptor,
        output_desc: RecordDescriptor,
    ) -> Result<Option<Arc<PreparedProjection>>, IvmRuntimeError> {
        if let Some(cached) = self
            .node_meta
            .get(&node)
            .and_then(|meta| meta.raw_projection_fields.clone())
        {
            return Ok(cached);
        }

        let resolved = raw_projection_fields(project, input_desc, output_desc)?.map(Arc::from);
        self.node_meta
            .entry(node)
            .or_default()
            .raw_projection_fields = Some(resolved.clone());
        Ok(resolved)
    }

    pub(super) fn frontier_source(
        &self,
        frontier_source: &FrontierSourceOp,
        output: &RecordDescriptor,
    ) -> Result<RecordDeltas, IvmRuntimeError> {
        let deltas = self
            .context
            .bindings
            .get(&frontier_source.binding)
            .cloned()
            .unwrap_or_else(|| RecordDeltas::empty(*output));
        if !deltas.descriptor.registry_compatible_with(output) {
            return Err(IvmRuntimeError::GraphOutputMismatch);
        }
        Ok(deltas)
    }

    #[allow(clippy::too_many_arguments)]
    fn update_join(
        &mut self,
        node: NodeId,
        join: &JoinOp,
        output_desc: RecordDescriptor,
        left_input: NodeId,
        right_input: NodeId,
        _left: &Arc<RecordDeltas>,
        _right: &Arc<RecordDeltas>,
    ) -> Result<RecordDeltas, IvmRuntimeError> {
        let (left_on, right_on) = self.join_field_names(node, join);
        let projection = self.join_output_projection(
            node,
            join.left_descriptor,
            join.right_descriptor,
            output_desc,
        )?;
        let left_key =
            self.arrangement_key(left_input, join.left_descriptor, &left_on, join.comparison)?;
        let right_key = self.arrangement_key(
            right_input,
            join.right_descriptor,
            &right_on,
            join.comparison,
        )?;
        let left_state = self
            .arrangement_states
            .get(&left_key)
            .ok_or(IvmRuntimeError::GraphNodeNotFound(left_input))?;
        let right_state = self
            .arrangement_states
            .get(&right_key)
            .ok_or(IvmRuntimeError::GraphNodeNotFound(right_input))?;
        let deltas = JoinState.evaluate_prepared(
            super::join::ArrangementTransition::at(
                left_state,
                self.arrangement_sub_tick(&left_key),
            ),
            super::join::ArrangementTransition::at(
                right_state,
                self.arrangement_sub_tick(&right_key),
            ),
            &projection,
            self.context.arrangement_update_mode,
        )?;
        #[cfg(feature = "cold-settle-attribution")]
        crate::cold_settle_attribution::record_join(
            self.context.eval_mode == EvalMode::Hydrate,
            _left.deltas.len(),
            _right.deltas.len(),
            deltas.len(),
        );
        Ok(RecordDeltas {
            descriptor: output_desc,
            deltas,
        })
    }

    #[allow(clippy::too_many_arguments)]
    fn update_anti_join(
        &mut self,
        node: NodeId,
        join: &JoinOp,
        output_desc: RecordDescriptor,
        left_input: NodeId,
        right_input: NodeId,
        left: &Arc<RecordDeltas>,
        right: &Arc<RecordDeltas>,
    ) -> Result<RecordDeltas, IvmRuntimeError> {
        self.update_threshold_join(
            node,
            join,
            output_desc,
            left_input,
            right_input,
            left,
            right,
            false,
        )
    }

    #[allow(clippy::too_many_arguments)]
    fn update_semi_join(
        &mut self,
        node: NodeId,
        join: &JoinOp,
        output_desc: RecordDescriptor,
        left_input: NodeId,
        right_input: NodeId,
        left: &Arc<RecordDeltas>,
        right: &Arc<RecordDeltas>,
    ) -> Result<RecordDeltas, IvmRuntimeError> {
        self.update_threshold_join(
            node,
            join,
            output_desc,
            left_input,
            right_input,
            left,
            right,
            true,
        )
    }

    #[allow(clippy::too_many_arguments)]
    fn update_threshold_join(
        &mut self,
        node: NodeId,
        join: &JoinOp,
        output_desc: RecordDescriptor,
        left_input: NodeId,
        right_input: NodeId,
        _left: &Arc<RecordDeltas>,
        _right: &Arc<RecordDeltas>,
        semi: bool,
    ) -> Result<RecordDeltas, IvmRuntimeError> {
        let operator_key = self.operator_key(node);
        let (left_on, right_on) = self.join_field_names(node, join);
        let left_key =
            self.arrangement_key(left_input, join.left_descriptor, &left_on, join.comparison)?;
        let right_key = self.arrangement_key(
            right_input,
            join.right_descriptor,
            &right_on,
            join.comparison,
        )?;
        let lt = self.arrangement_sub_tick(&left_key);
        let rt = self.arrangement_sub_tick(&right_key);
        let left = self
            .arrangement_states
            .get(&left_key)
            .ok_or(IvmRuntimeError::GraphNodeNotFound(left_input))?;
        let right = self
            .arrangement_states
            .get(&right_key)
            .ok_or(IvmRuntimeError::GraphNodeNotFound(right_input))?;
        let left = super::join::ArrangementTransition::at(left, lt);
        let right = super::join::ArrangementTransition::at(right, rt);
        let operator = self.operator_states.entry(operator_key).or_insert_with(|| {
            if semi {
                OperatorState::SemiJoin(SemiJoinState::default())
            } else {
                OperatorState::AntiJoin(AntiJoinState::default())
            }
        });
        let deltas = match operator {
            OperatorState::SemiJoin(state) if semi => {
                state.evaluate(left, right, self.context.arrangement_update_mode)
            }
            OperatorState::AntiJoin(state) if !semi => {
                state.evaluate(left, right, self.context.arrangement_update_mode)
            }
            _ => return Err(IvmRuntimeError::NodeStateOperatorMismatch(node)),
        };
        #[cfg(feature = "cold-settle-attribution")]
        crate::cold_settle_attribution::record_join(
            self.context.eval_mode == EvalMode::Hydrate,
            _left.deltas.len(),
            _right.deltas.len(),
            deltas.len(),
        );
        Ok(RecordDeltas {
            descriptor: output_desc,
            deltas,
        })
    }

    fn update_arg_by(
        &mut self,
        node: NodeId,
        spec: ArgBySpec<'_>,
        output_desc: RecordDescriptor,
        input: &RecordDeltas,
    ) -> Result<RecordDeltas, IvmRuntimeError> {
        if self.context.eval_mode == EvalMode::Hydrate && !self.context.hydrate_arrangements {
            // A probe needs only the winners, not an index for future deltas.
            // Keep any installed subscription state intact. Subscription
            // hydration separately proves/seeds state before reusing a memo.
            return super::recursion::hydrated_arg_by_winners(
                input,
                output_desc,
                spec.group_field_indices,
                spec.comparison_field_indices,
                spec.direction,
            );
        }
        let operator_key = self.operator_key(node);
        let mut operator = self
            .operator_states
            .remove(&operator_key)
            .unwrap_or_else(|| OperatorState::ArgBy(AsOf::default()));
        let OperatorState::ArgBy(state) = &mut operator else {
            return Err(IvmRuntimeError::NodeStateOperatorMismatch(node));
        };
        let sub_tick = SubTick {
            tick: self.current_tick,
            sub_tick: if operator_key.scope == ScopeId::root() {
                0
            } else {
                self.context.sub_tick
            },
        };
        if state.as_of().is_some_and(|current| current > sub_tick) {
            return Err(IvmRuntimeError::OutOfOrderRuntimeState {
                current: format!("{:?}", state.as_of().expect("checked above")),
                next: format!("{sub_tick:?}"),
            });
        }
        let replace = self.context.arrangement_update_mode == ArrangementUpdateMode::Replace;
        if !replace && state.as_of() == Some(sub_tick) {
            self.operator_states.insert(operator_key, operator);
            return Ok(RecordDeltas::empty(output_desc));
        }
        // Hydration supplies the complete input, not another set of inserts.
        // Clear even an empty snapshot so a shared node retains no stale groups.
        if replace {
            state.value_mut().clear();
        }
        let mut touched_groups = BTreeMap::<Vec<u8>, Vec<RecordDelta>>::new();
        for delta in &input.deltas {
            let group_key =
                encoded_arrangement_key_part(output_desc, delta.raw(), spec.group_field_indices)?;
            touched_groups
                .entry(group_key)
                .or_default()
                .push(delta.clone());
        }

        let mut output = Vec::new();
        for (group_prefix, group_deltas) in touched_groups {
            let group = state.value_mut().get_or_default(group_prefix.clone());
            let before = arg_by_ordered_winner(group);
            for delta in group_deltas {
                let key = arg_by_order_key(output_desc, &delta, &spec)?;
                let weight = group.get(&key).copied().unwrap_or_default() + delta.weight;
                group.set(key, weight);
            }
            let after = arg_by_ordered_winner(group);
            state
                .value_mut()
                .remove_empty_touched_groups([group_prefix]);
            if before == after {
                continue;
            }
            if let Some(record) = before {
                output.push(RecordDelta { record, weight: -1 });
            }
            if let Some(record) = after {
                output.push(RecordDelta { record, weight: 1 });
            }
        }
        state.mark_forward_as_of(sub_tick)?;
        self.operator_states.insert(operator_key, operator);

        Ok(RecordDeltas {
            descriptor: output_desc,
            deltas: output,
        })
    }

    fn update_top_by(
        &mut self,
        node: NodeId,
        top_by: &TopByOp,
        output_desc: RecordDescriptor,
        input: &RecordDeltas,
    ) -> Result<RecordDeltas, IvmRuntimeError> {
        if input.deltas.is_empty() || top_by.limit == TopByLimit::Finite(0) {
            return Ok(RecordDeltas::empty(output_desc));
        }
        let operator_key = self.operator_key(node);
        let mut operator = self
            .operator_states
            .remove(&operator_key)
            .unwrap_or_else(|| operator_state_for(&OpType::TopBy(top_by.clone())));
        let OperatorState::TopBy(state) = &mut operator else {
            return Err(IvmRuntimeError::NodeStateOperatorMismatch(node));
        };
        let sub_tick = SubTick {
            tick: self.current_tick,
            sub_tick: if operator_key.scope == ScopeId::root() {
                0
            } else {
                self.context.sub_tick
            },
        };
        if state.as_of().is_some_and(|current| current > sub_tick) {
            return Err(IvmRuntimeError::OutOfOrderRuntimeState {
                current: format!("{:?}", state.as_of().expect("checked above")),
                next: format!("{sub_tick:?}"),
            });
        }
        if self.context.arrangement_update_mode == ArrangementUpdateMode::Accumulate
            && state.as_of() == Some(sub_tick)
        {
            self.operator_states.insert(operator_key, operator);
            return Ok(RecordDeltas::empty(output_desc));
        }
        let mut touched_groups = BTreeMap::<Vec<u8>, Vec<RecordDelta>>::new();
        for delta in &input.deltas {
            let group_key =
                encoded_record_key_part(output_desc, delta.raw(), &top_by.group_field_indices)?;
            touched_groups
                .entry(group_key)
                .or_default()
                .push(delta.clone());
        }

        let mut output = Vec::new();
        let replace = self.context.arrangement_update_mode == ArrangementUpdateMode::Replace;
        if !replace
            && top_by.offset == 0
            && top_by.limit == TopByLimit::Unbounded
            && !self.root_ordering_windows.contains_key(&node)
        {
            // Without a selection boundary, only touched records can change
            // membership. Structured collectors own their positional edits;
            // plain consumers requesting generic positions retain the window
            // path below. Keep the same ordered state for subsequent snapshots
            // and for a plain consumer attached on a later tick.
            for (group_prefix, group_deltas) in &touched_groups {
                let group = state
                    .value_mut()
                    .groups
                    .get_or_default(group_prefix.clone());
                output.extend(update_unbounded_top_by_group(
                    output_desc,
                    top_by,
                    group,
                    group_deltas,
                )?);
                self.metrics.top_by_delta_membership_records += group_deltas.len();
            }
            state
                .value_mut()
                .remove_empty_touched_groups(touched_groups.keys().cloned());
            state.mark_forward_as_of(sub_tick)?;
            self.operator_states.insert(operator_key, operator);
            return Ok(RecordDeltas {
                descriptor: output_desc,
                deltas: output,
            });
        }
        let before = touched_groups
            .keys()
            .map(|group| {
                Ok((
                    group.clone(),
                    if replace {
                        Vec::new()
                    } else {
                        top_by_window_from_ordered_group(state.value().groups.get(group), top_by)
                    },
                ))
            })
            .collect::<Result<BTreeMap<_, _>, IvmRuntimeError>>()?;
        if replace {
            state.value_mut().groups.clear();
        }
        for (group_prefix, group_deltas) in &touched_groups {
            let group = state
                .value_mut()
                .groups
                .get_or_default(group_prefix.clone());
            for delta in group_deltas {
                let order_key = (
                    top_by_sort_key(output_desc, delta.raw(), top_by)?,
                    delta.record.clone(),
                );
                let weight = group.get(&order_key).copied().unwrap_or_default() + delta.weight;
                group.set(order_key, weight);
            }
        }
        state
            .value_mut()
            .remove_empty_touched_groups(touched_groups.keys().cloned());
        state.mark_forward_as_of(sub_tick)?;
        for group_prefix in touched_groups.keys() {
            let before = before.get(group_prefix).cloned().unwrap_or_default();
            let after =
                top_by_window_from_ordered_group(state.value().groups.get(group_prefix), top_by);
            let position_records = before.len().saturating_add(after.len());
            if let Some(windows) = self.root_ordering_windows.get_mut(&node) {
                windows.record(output_desc, top_by, group_prefix, &before, &after);
                self.metrics.root_ordering_position_records += position_records;
            } else {
                self.metrics.root_ordering_position_records_skipped += position_records;
            }
            output.extend(diff_record_windows(before, after));
        }
        self.operator_states.insert(operator_key, operator);

        Ok(RecordDeltas {
            descriptor: output_desc,
            deltas: output,
        })
    }

    /// Render touched flat groups as complete parents. This intentionally uses
    /// a root-scope arrangement keyed by the collector input; a collector is
    /// structurally terminal, so it can never become state in a recursive step
    /// or inherit a recursive sub-tick work bound.
    #[cfg_attr(
        feature = "cold-settle-attribution",
        tracing::instrument(skip_all, name = "cold.phase.collect_results")
    )]
    fn update_collect_by(
        &mut self,
        node: NodeId,
        collect_by: &CollectByOp,
        output_desc: RecordDescriptor,
        input: &RecordDeltas,
        canonical: &RecordDeltas,
    ) -> Result<RecordDeltas, IvmRuntimeError> {
        if self.context.eval_mode == EvalMode::Hydrate {
            // Hydration supplies a complete snapshot, including when another
            // subscription already owns this shared collector. Rebuild its
            // state instead of applying that snapshot as incremental inserts;
            // otherwise a later retraction leaves a phantom duplicate behind.
            let operator_key = self.operator_key(node);
            self.operator_states.remove(&operator_key);
        }
        if input.deltas.is_empty() || collect_by.limit == TopByLimit::Finite(0) {
            return Ok(RecordDeltas::empty(output_desc));
        }
        let mut hydration_snapshot = None;
        let direct_tree_slot = match collect_by.slots.as_slice() {
            [] if collect_by.limit == TopByLimit::Unbounded => None,
            [slot]
                if slot.slots.is_empty()
                    && slot.limit == TopByLimit::Unbounded
                    && slot.reference_array_field_index.is_none() =>
            {
                Some(slot)
            }
            _ => None,
        };
        if collect_by.mode == CollectByMode::Root
            || (collect_by.mode == CollectByMode::Collect
                && (collect_by.slots.is_empty() || direct_tree_slot.is_some())
                && (collect_by.limit == TopByLimit::Unbounded || direct_tree_slot.is_some()))
        {
            let operator_key = self.operator_key(node);
            let mut operator = self
                .operator_states
                .remove(&operator_key)
                .unwrap_or_else(|| OperatorState::CollectBy(CollectByIncrementalState::default()));
            let OperatorState::CollectBy(state) = &mut operator else {
                return Err(IvmRuntimeError::NodeStateOperatorMismatch(node));
            };
            let operations = update_unbounded_collect_by_terminal_state(
                input.descriptor,
                output_desc,
                collect_by,
                direct_tree_slot,
                state,
                &input.deltas,
                matches!(self.context.eval_mode, EvalMode::Tick | EvalMode::Hydrate),
            )?;
            if self.context.eval_mode == EvalMode::Hydrate && collect_by.mode == CollectByMode::Root
            {
                // Hydrate removed the prior operator above, so these Inserts
                // cover the complete fresh state, not an incremental subset.
                hydration_snapshot =
                    singleton_root_hydration_snapshot(state, &input.deltas, &operations);
            }
            self.operator_states.insert(operator_key, operator);
            // A subscription hydration is the first transition of the same
            // collector. Retain its operations so the opening/reset consumer
            // can seed its terminal tree from the exact same root keys used
            // by all later incremental updates. Hydration still returns the
            // relational snapshot below; only a Tick suppresses that output
            // after publishing terminal edits.
            if !operations.is_empty() {
                self.terminal_deltas
                    .insert(node, TerminalDeltas { operations });
            }
            if self.context.eval_mode == EvalMode::Tick {
                return Ok(RecordDeltas::empty(output_desc));
            }
        }
        let [input_node] = self
            .graph
            .node(node)
            .ok_or(IvmRuntimeError::GraphNodeNotFound(node))?
            .descriptor
            .inputs
            .as_slice()
        else {
            return Err(IvmRuntimeError::GraphInputArityMismatch(node));
        };
        if let Some(deltas) = hydration_snapshot {
            return Ok(RecordDeltas {
                descriptor: output_desc,
                deltas,
            });
        }
        let input_desc = input.descriptor;
        let arrangement_key = self.arrangement_key(
            *input_node,
            input_desc,
            &collect_by.group_fields,
            ValueComparison::Exact,
        )?;
        // Structural validation permits only terminal filter/projection
        // adapters above CollectBy, so a non-root evaluation scope here is a
        // routed terminal adapter, never recursive relational state.
        // Root collectors own an ordered terminal index, not a second retained
        // hash index. Only ambiguous hydration needs a temporary snapshot view.
        let mut snapshot_arrangement;
        let arrangement = if collect_by.mode == CollectByMode::Root {
            snapshot_arrangement = AsOf::<ArrangementState, SubTick>::default();
            snapshot_arrangement.value_mut().apply_record_deltas(
                input_desc,
                &collect_by.group_fields,
                &input.deltas,
                ArrangementUpdateMode::Replace,
            )?;
            snapshot_arrangement
        } else {
            self.arrangement_states
                .get(&arrangement_key)
                .cloned()
                .ok_or(IvmRuntimeError::GraphNodeNotFound(*input_node))?
        };

        let mut touched_groups = BTreeMap::<Vec<u8>, Vec<RecordDelta>>::new();
        for delta in &canonical.deltas {
            let group_key =
                encoded_record_key_part(input_desc, delta.raw(), &collect_by.group_field_indices)?;
            touched_groups
                .entry(group_key)
                .or_default()
                .push(delta.clone());
        }

        let mut output = Vec::new();
        for (group_prefix, group_deltas) in touched_groups {
            let after_records = arrangement.value().records_for_key(&group_prefix);
            let before_records =
                if self.context.arrangement_update_mode == ArrangementUpdateMode::Replace {
                    Vec::new()
                } else {
                    records_before_deltas(after_records.clone(), &group_deltas)
                };
            let after_records =
                self.materialize_arranged_records(input_desc, after_records, None)?;
            let before_records =
                self.materialize_arranged_records(input_desc, before_records, None)?;
            match collect_by.mode {
                CollectByMode::Collect | CollectByMode::Root => {
                    let render = |records: &[(Bytes, i64)]| {
                        if collect_by.mode == CollectByMode::Root {
                            collect_by_root_from_records(
                                input_desc,
                                output_desc,
                                collect_by,
                                records,
                            )
                        } else if collect_by.slots.is_empty() {
                            collect_by_parent_from_records(
                                input_desc,
                                output_desc,
                                collect_by,
                                records,
                            )
                        } else {
                            collect_by_tree_parent_from_records(
                                input_desc,
                                output_desc,
                                collect_by,
                                records,
                            )
                        }
                    };
                    let before = render(&before_records)?;
                    let after = render(&after_records)?;
                    if before == after {
                        continue;
                    }
                    if let Some(record) = before {
                        output.push(RecordDelta { record, weight: -1 });
                    }
                    if let Some(record) = after {
                        output.push(RecordDelta { record, weight: 1 });
                    }
                }
                CollectByMode::Expand => {
                    let before = collect_by_expanded_window(
                        input_desc,
                        output_desc,
                        collect_by,
                        &before_records,
                    )?;
                    let after = collect_by_expanded_window(
                        input_desc,
                        output_desc,
                        collect_by,
                        &after_records,
                    )?;
                    let mut occurrences = BTreeSet::new();
                    occurrences.extend(before.keys().cloned());
                    occurrences.extend(after.keys().cloned());
                    for occurrence in occurrences {
                        match (before.get(&occurrence), after.get(&occurrence)) {
                            (Some(before), Some(after)) if before == after => {}
                            (Some(before), Some(after)) => {
                                output.push(RecordDelta {
                                    record: before.clone(),
                                    weight: -1,
                                });
                                output.push(RecordDelta {
                                    record: after.clone(),
                                    weight: 1,
                                });
                            }
                            (Some(before), None) => output.push(RecordDelta {
                                record: before.clone(),
                                weight: -1,
                            }),
                            (None, Some(after)) => output.push(RecordDelta {
                                record: after.clone(),
                                weight: 1,
                            }),
                            (None, None) => unreachable!("occurrence came from a selected window"),
                        }
                    }
                }
            }
        }
        Ok(RecordDeltas {
            descriptor: output_desc,
            deltas: output,
        })
    }

    fn update_aggregate(
        &mut self,
        node: NodeId,
        aggregate: &AggregateOp,
        output_desc: RecordDescriptor,
        input: &RecordDeltas,
        canonical: &RecordDeltas,
    ) -> Result<RecordDeltas, IvmRuntimeError> {
        let [input_node] = self
            .graph
            .node(node)
            .ok_or(IvmRuntimeError::GraphNodeNotFound(node))?
            .descriptor
            .inputs
            .as_slice()
        else {
            return Err(IvmRuntimeError::GraphInputArityMismatch(node));
        };
        let input_desc = input.descriptor;
        let group_fields = self.aggregate_group_fields(node, aggregate);
        let arrangement_key = self.arrangement_key(
            *input_node,
            input_desc,
            &group_fields,
            ValueComparison::Exact,
        )?;
        if input.deltas.is_empty() {
            // An ungrouped aggregate has one logical empty group. Its
            // identity is emitted by Groove itself on the first complete
            // empty frontier; grouped aggregates have no empty group. The
            // empty arrangement records that this identity has been seeded.
            let should_seed_empty_group = aggregate.group_key.is_empty()
                && (self.context.eval_mode == EvalMode::Hydrate
                    || !self.arrangement_states.contains_key(&arrangement_key));
            if should_seed_empty_group {
                let record = aggregate_row_from_records(input_desc, output_desc, aggregate, &[])?
                    .ok_or(IvmRuntimeError::UnsupportedOperator)?;
                return Ok(RecordDeltas {
                    descriptor: output_desc,
                    deltas: vec![RecordDelta { record, weight: 1 }],
                });
            }
            return Ok(RecordDeltas::empty(output_desc));
        }
        if self.context.eval_mode == EvalMode::Hydrate {
            let mut groups = BTreeMap::<Vec<u8>, Vec<(Bytes, i64)>>::new();
            for delta in &input.deltas {
                let group_key = encoded_record_key_part(
                    input_desc,
                    delta.raw(),
                    &aggregate.group_field_indices,
                )?;
                groups
                    .entry(group_key)
                    .or_default()
                    .push((delta.record.clone(), delta.weight));
            }
            let mut output = Vec::new();
            for records in groups.values() {
                if let Some(record) =
                    aggregate_row_from_records(input_desc, output_desc, aggregate, records)?
                {
                    output.push(RecordDelta { record, weight: 1 });
                }
            }
            return Ok(RecordDeltas {
                descriptor: output_desc,
                deltas: output,
            });
        }
        let mut touched_groups = BTreeMap::<Vec<u8>, Vec<RecordDelta>>::new();
        for delta in &canonical.deltas {
            let group_key =
                encoded_record_key_part(input_desc, delta.raw(), &aggregate.group_field_indices)?;
            touched_groups
                .entry(group_key)
                .or_default()
                .push(delta.clone());
        }
        let arrangement = self
            .arrangement_states
            .get(&arrangement_key)
            .cloned()
            .ok_or(IvmRuntimeError::GraphNodeNotFound(*input_node))?;

        let mut fields = aggregate.group_field_indices.clone();
        let expressions = aggregate
            .aggregates
            .iter()
            .filter_map(|expr| expr.expression.clone())
            .collect::<Vec<_>>();
        for field in plan_expr_fields(&expressions) {
            fields.push(
                resolve_field_name(&input_desc, &field)
                    .ok_or_else(|| IvmRuntimeError::GraphFieldNotFound(field.clone()))?,
            );
        }
        fields.sort_unstable();
        fields.dedup();

        let mut output = Vec::new();
        for group_prefix in touched_groups.keys() {
            let after_records = arrangement.value().records_for_key(group_prefix);
            let before_records = records_before_from_deltas(
                after_records.clone(),
                touched_groups
                    .get(group_prefix)
                    .cloned()
                    .unwrap_or_default(),
            );
            let after_records =
                self.materialize_arranged_records(input_desc, after_records, Some(&fields))?;
            let before_records =
                self.materialize_arranged_records(input_desc, before_records, Some(&fields))?;
            let after =
                aggregate_row_from_records(input_desc, output_desc, aggregate, &after_records)?;
            let before =
                aggregate_row_from_records(input_desc, output_desc, aggregate, &before_records)?;
            if before == after {
                continue;
            }
            if let Some(record) = before {
                output.push(RecordDelta { record, weight: -1 });
            }
            if let Some(record) = after {
                output.push(RecordDelta { record, weight: 1 });
            }
        }

        Ok(RecordDeltas {
            descriptor: output_desc,
            deltas: consolidate_deltas(output),
        })
    }

    fn arrangement_needs_index(&self, node: NodeId) -> bool {
        self.graph.node(node).is_some_and(|arrange| {
            arrange.children.iter().any(|child| {
                self.graph
                    .node(*child)
                    .is_some_and(|child| match &child.descriptor.operator {
                        OpType::Join(_)
                        | OpType::SemiJoin(_)
                        | OpType::AntiJoin(_)
                        | OpType::Aggregate(_) => true,
                        OpType::CollectBy(collect) => collect.mode != CollectByMode::Root,
                        _ => false,
                    })
            })
        })
    }

    fn arrangement_key(
        &mut self,
        input: NodeId,
        descriptor: RecordDescriptor,
        fields: &[String],
        comparison: ValueComparison,
    ) -> Result<ArrangementKey, IvmRuntimeError> {
        let arrangement = self
            .graph
            .node(input)
            .ok_or(IvmRuntimeError::GraphNodeNotFound(input))?;
        let OpType::Arrange(spec) = &arrangement.descriptor.operator else {
            return Err(IvmRuntimeError::UnsupportedOperator);
        };
        if arrangement.descriptor.output.records() != descriptor
            || spec.fields.as_slice() != fields
            || spec.comparison != comparison
        {
            return Err(IvmRuntimeError::GraphOutputMismatch);
        }
        Ok(ArrangementKey {
            scope: self.context.scope,
            input,
        })
    }

    fn join_field_names(&mut self, node: NodeId, join: &JoinOp) -> (Arc<[String]>, Arc<[String]>) {
        let meta = self.node_meta.entry(node).or_default();
        let left = meta
            .join_left_fields
            .get_or_insert_with(|| Arc::from(plan_expr_names(&join.left_key)))
            .clone();
        let right = meta
            .join_right_fields
            .get_or_insert_with(|| Arc::from(plan_expr_names(&join.right_key)))
            .clone();
        (left, right)
    }

    pub(super) fn join_output_projection(
        &mut self,
        node: NodeId,
        left_descriptor: RecordDescriptor,
        right_descriptor: RecordDescriptor,
        output_descriptor: RecordDescriptor,
    ) -> Result<Arc<crate::records::PreparedRecordCopy>, IvmRuntimeError> {
        if let Some(projection) = &self.node_meta.entry(node).or_default().join_output {
            return Ok(Arc::clone(projection));
        }
        let mapping = super::join::join_output_mapping(
            &left_descriptor,
            &right_descriptor,
            &output_descriptor,
        )?;
        let projection = Arc::new(crate::records::PreparedRecordCopy::new(
            &[left_descriptor, right_descriptor],
            output_descriptor,
            &mapping,
        )?);
        self.node_meta.entry(node).or_default().join_output = Some(Arc::clone(&projection));
        Ok(projection)
    }

    pub(super) fn aggregate_group_fields(
        &mut self,
        node: NodeId,
        aggregate: &AggregateOp,
    ) -> Arc<[String]> {
        self.node_meta
            .entry(node)
            .or_default()
            .aggregate_group_fields
            .get_or_insert_with(|| Arc::from(plan_expr_names(&aggregate.group_key)))
            .clone()
    }

    /// Opt-in diagnostic fingerprints only: these hashes are not semantic
    /// identities and must never be used to authorize snapshot reuse.
    #[cfg(feature = "cold-settle-attribution")]
    fn trace_arrangement_snapshot(&self, key: &ArrangementKey, deltas: &[RecordDelta]) {
        if self.context.arrangement_update_mode != ArrangementUpdateMode::Replace
            || std::env::var_os("GROOVE_TRACE_ARRANGEMENT_SNAPSHOTS").is_none()
        {
            return;
        }
        use std::hash::{Hash, Hasher};
        let mut fingerprint = std::collections::hash_map::DefaultHasher::new();
        let mut bytes = 0usize;
        deltas.len().hash(&mut fingerprint);
        for delta in deltas {
            delta.record.hash(&mut fingerprint);
            delta.weight.hash(&mut fingerprint);
            bytes += delta.record.len();
        }
        eprintln!(
            "ARRANGEMENT_SNAPSHOT\t{:p}\t{key:?}\t{:?}\t{}\t{bytes}\t{:016x}\t{:p}",
            self.arrangement_states,
            self.arrangement_sub_tick(key),
            deltas.len(),
            fingerprint.finish(),
            deltas.as_ptr(),
        );
    }

    fn arrangement_sub_tick(&self, key: &ArrangementKey) -> SubTick {
        SubTick {
            tick: self.current_tick,
            // Root-scope arrangements represent table time, not recursive
            // evaluator time. A recursive step at sub_tick 1 and a sibling
            // non-recursive join must therefore share the same root SubTick.
            sub_tick: if key.scope == ScopeId::root() {
                0
            } else {
                self.context.sub_tick
            },
        }
    }

    fn insert_arrangement(&mut self, key: ArrangementKey, state: AsOf<ArrangementState, SubTick>) {
        self.arrangement_keys_by_input
            .entry(key.input)
            .or_default()
            .insert(key.clone());
        self.arrangement_states.insert(key, state);
    }

    async fn update_recursive(
        &mut self,
        node: NodeId,
        recursive: &RecursiveOp,
        output_desc: RecordDescriptor,
        seed: NodeId,
        step: NodeId,
        step_witness: Option<NodeId>,
    ) -> Result<RecordDeltas, IvmRuntimeError> {
        let storage = self.storage.ok_or(IvmRuntimeError::StorageUnavailable)?;
        let operator_key = self.operator_key(node);
        let input_generation = self.input_generation(node);
        let nodes = RecursiveNodes {
            seed,
            step,
            step_witness,
        };
        if self.context.eval_mode == EvalMode::Tick {
            let state = match self.operator_states.get(&operator_key) {
                Some(OperatorState::Recursive(state)) => Some(state.value()),
                _ => None,
            };
            if let Some(root) = snapshot_requirement(
                self.graph,
                node,
                nodes,
                self.table_deltas,
                self.binding_deltas,
                state,
            )? && let Some(inputs) = self.evaluation_inputs.as_deref_mut()
            {
                require_snapshot_inputs(self.graph, inputs, root)?;
            }
        }
        // Recursive child evaluation may touch the same state maps. Remove only
        // this recursive node's state; child operator state stays available.
        let mut operator = self
            .operator_states
            .remove(&operator_key)
            .unwrap_or_else(|| OperatorState::Recursive(AsOf::new(RecursiveState::default())));
        let OperatorState::Recursive(recursive_as_of) = &mut operator else {
            return Err(IvmRuntimeError::NodeStateOperatorMismatch(node));
        };
        if self.context.eval_mode == EvalMode::Hydrate {
            if !recursive_as_of.value().has_pending_hydration()
                && recursive_as_of.value().step_arrangements_hydrated()
                && recursive_as_of.as_of() == Some(Tick(self.current_tick))
                && recursive_as_of.value().hydrated_input_generation() == Some(input_generation)
            {
                let deltas = recursive_as_of
                    .value_at(Tick(self.current_tick))?
                    .accumulated_deltas();
                self.operator_states.insert(operator_key, operator);
                return Ok(RecordDeltas {
                    descriptor: output_desc,
                    deltas,
                });
            }
            let scope = self.context.scope.child(node);
            let progress = super::recursion::resume_inputs_hydration_recompute(
                recursive_as_of.value_mut(),
                super::recursion::HydrationRecomputeContext {
                    schema: self.schema,
                    graph: self.graph,
                    variant_projections: self.variant_projections,
                    inputs: self.evaluation_inputs.as_deref_mut(),
                    table_deltas: Some(self.table_deltas),
                    storage,
                    binding_snapshots: self.binding_snapshots,
                    scope,
                    input_generation,
                },
                node,
                recursive,
                output_desc,
                nodes,
            )
            .await;
            let accumulated = match progress {
                Ok(super::recursion::HydrationRecomputeProgress::Yield) => {
                    self.operator_states.insert(operator_key, operator);
                    cooperative_operator_yield().await;
                    unreachable!("retained recursive hydration yields through operator state")
                }
                Ok(super::recursion::HydrationRecomputeProgress::ReadyForArrangementHydration) => {
                    recursive_as_of
                        .value()
                        .pending_hydration_accumulated_deltas(output_desc)
                }
                Err(error) => {
                    self.operator_states.insert(operator_key, operator);
                    return Err(error);
                }
            };
            let mut runtime = graph_runtime_view(
                self.schema,
                self.graph,
                self.variant_projections,
                self.table_deltas,
                self.binding_deltas,
                self.binding_snapshots,
                self.current_tick,
                self.operator_states,
                self.arrangement_states,
                self.arrangement_keys_by_input,
                self.eval_memo,
                self.eval_memo_bytes,
                self.table_frontiers,
                self.binding_frontiers,
                self.memo_use_clock,
                self.node_meta,
                storage,
                self.evaluation_inputs.as_deref_mut(),
                scope,
                self.metrics,
            );
            hydrate_recursive_arrangements(&mut runtime, recursive, step, accumulated.clone())
                .await?;
            if let Some(witness) = step_witness {
                hydrate_recursive_arrangements(
                    &mut runtime,
                    recursive,
                    witness,
                    RecordDeltas {
                        descriptor: output_desc,
                        deltas: recursive_as_of.value().accumulated_deltas(),
                    },
                )
                .await?;
            }
            if recursive_as_of.value().has_pending_hydration() {
                let (next, step_witness) = recursive_as_of.value_mut().finish_hydration_recompute();
                recursive_as_of.value_mut().replace_with(next);
                recursive_as_of
                    .value_mut()
                    .replace_step_witness_with(step_witness);
            }
            recursive_as_of
                .value_mut()
                .mark_step_arrangements_hydrated();
            recursive_as_of
                .value_mut()
                .mark_hydrated_input_generation(input_generation);
            recursive_as_of.mark_forward_as_of(Tick(self.current_tick))?;
            self.operator_states.insert(operator_key, operator);
            return Ok(accumulated);
        }
        let deltas = recursive_delta(
            recursive_as_of.value_mut(),
            graph_runtime_view(
                self.schema,
                self.graph,
                self.variant_projections,
                self.table_deltas,
                self.binding_deltas,
                self.binding_snapshots,
                self.current_tick,
                self.operator_states,
                self.arrangement_states,
                self.arrangement_keys_by_input,
                self.eval_memo,
                self.eval_memo_bytes,
                self.table_frontiers,
                self.binding_frontiers,
                self.memo_use_clock,
                self.node_meta,
                storage,
                self.evaluation_inputs.as_deref_mut(),
                self.context.scope.child(node),
                self.metrics,
            ),
            node,
            recursive,
            output_desc,
            nodes,
        )
        .await;
        let deltas = match deltas {
            Ok(super::recursion::RecursiveDeltaProgress::Ready(deltas)) => deltas,
            Ok(super::recursion::RecursiveDeltaProgress::Yield) => {
                self.operator_states.insert(operator_key, operator);
                cooperative_operator_yield().await;
                unreachable!("retained recursive tick yields through operator state")
            }
            Err(error) => {
                self.operator_states.insert(operator_key, operator);
                return Err(error);
            }
        };
        recursive_as_of.mark_forward_as_of(Tick(self.current_tick))?;
        recursive_as_of
            .value_mut()
            .mark_hydrated_input_generation(input_generation);
        self.operator_states.insert(operator_key, operator);
        Ok(RecordDeltas {
            descriptor: output_desc,
            deltas,
        })
    }

    fn update_recursive_step_witness(
        &mut self,
        recursive_node: NodeId,
        output_desc: RecordDescriptor,
    ) -> Result<RecordDeltas, IvmRuntimeError> {
        let key = self.operator_key(recursive_node);
        let Some(OperatorState::Recursive(state)) = self.operator_states.get(&key) else {
            return Err(IvmRuntimeError::NodeStateOperatorMismatch(recursive_node));
        };
        let deltas = if self.context.eval_mode == EvalMode::Hydrate {
            state.value().step_witness_accumulated_deltas()
        } else {
            state.value().step_witness_deltas()
        };
        Ok(RecordDeltas {
            descriptor: output_desc,
            deltas,
        })
    }

    async fn update_unary_input(
        &mut self,
        graph_node: &crate::ivm::GraphNode,
        node: NodeId,
    ) -> Result<Arc<RecordDeltas>, IvmRuntimeError> {
        let input = *graph_node
            .descriptor
            .inputs
            .first()
            .ok_or(IvmRuntimeError::GraphInputMissing(node))?;
        self.update_node(input).await
    }

    async fn update_streaming_checksum(
        &mut self,
        node: NodeId,
        checksum: &StreamingChecksumOp,
        output_desc: RecordDescriptor,
        input: Arc<RecordDeltas>,
    ) -> Result<RecordDeltas, IvmRuntimeError> {
        let operator_key = self.operator_key(node);
        let operator = self
            .operator_states
            .remove(&operator_key)
            .unwrap_or_else(|| operator_state_for(&OpType::StreamingChecksum(checksum.clone())));
        let OperatorState::StreamingChecksum(mut state) = operator else {
            return Err(IvmRuntimeError::NodeStateOperatorMismatch(node));
        };
        let replace_pending = state
            .pending
            .as_ref()
            .is_none_or(|pending| pending.input.as_ref() != input.as_ref());
        if replace_pending {
            state.pending = Some(PendingStreamingChecksum {
                input: Arc::clone(&input),
                next_delta: 0,
                current: None,
                output: Vec::with_capacity(input.deltas.len()),
            });
        }
        let pending = state.pending.as_mut().expect("initialized above");

        while pending.next_delta < pending.input.deltas.len() {
            let delta = &pending.input.deltas[pending.next_delta];
            let mut values = delta.borrowed(&pending.input.descriptor).to_values()?;
            let value = values.get(checksum.field_idx).ok_or(
                IvmRuntimeError::GraphFieldIndexOutOfBounds(checksum.field_idx),
            )?;

            let digest = match value {
                Value::String(value) => Some(*blake3::hash(value.as_bytes()).as_bytes()),
                Value::Bytes(value) => Some(*blake3::hash(value).as_bytes()),
                Value::Large(value) => {
                    if pending.current.is_none() {
                        pending.current = Some(crate::large_values::StreamingChecksum::new(
                            value.as_ref().clone(),
                            checksum.window_bytes,
                            checksum.max_bytes_per_turn,
                        )?);
                    }
                    let streaming = pending.current.as_mut().expect("initialized above");
                    if streaming.cursor().remaining_bytes() != 0 {
                        let range = streaming
                            .cursor()
                            .next_range()
                            .expect("non-complete cursor has a range");
                        let inputs = self
                            .evaluation_inputs
                            .as_deref_mut()
                            .ok_or(IvmRuntimeError::EvaluationBlocked)?;
                        inputs.set_chunk_scope(Some(node));
                        let result = crate::large_values::byte_range_attempt(
                            streaming.cursor().value(),
                            range,
                            inputs,
                        );
                        inputs.set_chunk_scope(None);
                        let bytes = match result {
                            Ok(bytes) => bytes,
                            Err(IvmRuntimeError::EvaluationBlocked) => {
                                self.operator_states
                                    .insert(operator_key, OperatorState::StreamingChecksum(state));
                                return Err(IvmRuntimeError::EvaluationBlocked);
                            }
                            Err(error) => return Err(error),
                        };
                        let should_yield = streaming.consume_window(&bytes)?;
                        inputs.release_chunks_owned_by(node);
                        if should_yield {
                            streaming.record_yield()?;
                            self.operator_states
                                .insert(operator_key, OperatorState::StreamingChecksum(state));
                            cooperative_operator_yield().await;
                            unreachable!("yielded operator futures resume through saved state")
                        }
                    }
                    if streaming.cursor().remaining_bytes() == 0 {
                        let completed = pending.current.take().expect("complete state exists");
                        Some(completed.finish()?.0.0)
                    } else {
                        None
                    }
                }
                _ => return Err(IvmRuntimeError::StreamingChecksumTypeMismatch),
            };
            let Some(digest) = digest else {
                continue;
            };
            values[checksum.field_idx] = Value::Bytes(digest.to_vec());
            pending.output.push(RecordDelta {
                record: output_desc.create(&values)?.into(),
                weight: delta.weight,
            });
            pending.next_delta += 1;
        }

        let completed = state.pending.take().expect("completed batch exists");
        self.operator_states.insert(
            operator_key,
            OperatorState::StreamingChecksum(Box::default()),
        );
        Ok(RecordDeltas {
            descriptor: output_desc,
            deltas: completed.output,
        })
    }

    /// Index identity stays in producer representation. Consumers decode only
    /// after reconstructing before/after multisets using those exact bytes.
    fn materialize_arranged_records(
        &mut self,
        descriptor: RecordDescriptor,
        records: Vec<(Bytes, i64)>,
        fields: Option<&[usize]>,
    ) -> Result<Vec<(Bytes, i64)>, IvmRuntimeError> {
        if self.evaluation_inputs.is_none() || fields.is_some_and(|fields| fields.is_empty()) {
            return Ok(records);
        }
        let input = Arc::new(RecordDeltas {
            descriptor,
            deltas: records
                .into_iter()
                .map(|(record, weight)| RecordDelta { record, weight })
                .collect(),
        });
        let output = match fields {
            Some(fields) => self.materialize_indirect_field_indices(&input, fields)?,
            None => self.materialize_indirect_input(&input)?,
        };
        Ok(output
            .deltas
            .iter()
            .map(|delta| (delta.record.clone(), delta.weight))
            .collect())
    }

    pub(super) fn materialize_indirect_input(
        &mut self,
        input: &Arc<RecordDeltas>,
    ) -> Result<Arc<RecordDeltas>, IvmRuntimeError> {
        let Some(evaluation_inputs) = self.evaluation_inputs.as_deref_mut() else {
            return Ok(Arc::clone(input));
        };
        let mut deltas = Vec::with_capacity(input.deltas.len());
        let mut changed = false;
        for delta in &input.deltas {
            let raw = crate::large_values::materialize_record_attempt(
                &input.descriptor,
                delta.raw(),
                evaluation_inputs,
            )?;
            changed |= raw.as_slice() != delta.raw();
            deltas.push(RecordDelta {
                record: raw.into(),
                weight: delta.weight,
            });
        }
        if changed {
            Ok(Arc::new(RecordDeltas {
                descriptor: input.descriptor,
                deltas,
            }))
        } else {
            Ok(Arc::clone(input))
        }
    }

    fn materialize_indirect_fields(
        &mut self,
        input: &Arc<RecordDeltas>,
        fields: &BTreeSet<String>,
    ) -> Result<Arc<RecordDeltas>, IvmRuntimeError> {
        let indices = fields
            .iter()
            .map(|field| {
                resolve_field_name(&input.descriptor, field)
                    .ok_or_else(|| IvmRuntimeError::GraphFieldNotFound(field.clone()))
            })
            .collect::<Result<Vec<_>, _>>()?;
        self.materialize_indirect_field_indices(input, &indices)
    }

    fn materialize_indirect_field_indices(
        &mut self,
        input: &Arc<RecordDeltas>,
        indices: &[usize],
    ) -> Result<Arc<RecordDeltas>, IvmRuntimeError> {
        let Some(evaluation_inputs) = self.evaluation_inputs.as_deref_mut() else {
            return Ok(Arc::clone(input));
        };
        let fields = input.descriptor.fields();
        let requires_materialization = indices.iter().try_fold(false, |required, index| {
            let field = fields
                .get(*index)
                .ok_or(crate::records::Error::FieldIndexOutOfBounds {
                    index: *index,
                    len: fields.len(),
                })?;
            Ok::<_, crate::records::Error>(required || field.value_type.may_contain_stored_scalar())
        })?;
        if !requires_materialization {
            return Ok(Arc::clone(input));
        }
        let mut deltas = Vec::with_capacity(input.deltas.len());
        let mut changed = false;
        for delta in &input.deltas {
            let raw = crate::large_values::materialize_record_fields_attempt(
                &input.descriptor,
                delta.raw(),
                indices,
                evaluation_inputs,
            )?;
            changed |= raw.as_slice() != delta.raw();
            deltas.push(RecordDelta {
                record: raw.into(),
                weight: delta.weight,
            });
        }
        if changed {
            Ok(Arc::new(RecordDeltas {
                descriptor: input.descriptor,
                deltas,
            }))
        } else {
            Ok(Arc::clone(input))
        }
    }
}

async fn cooperative_operator_yield() {
    let mut yielded = false;
    std::future::poll_fn(move |context| {
        if yielded {
            std::task::Poll::Ready(())
        } else {
            yielded = true;
            context.waker().wake_by_ref();
            std::task::Poll::Pending
        }
    })
    .await
}
