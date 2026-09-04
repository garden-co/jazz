//! Per-tick evaluator state, memoization, arrangements, and recursive execution.

use super::*;

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

#[derive(Clone, Debug)]
pub(super) enum OperatorState {
    Stateless,
    Join(JoinState),
    SemiJoin(SemiJoinState),
    AntiJoin(AntiJoinState),
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
    pub(super) emitted_root_order: BTreeMap<CollectByOrderKey, Vec<u8>>,
    /// Root terminal groups that have actually been emitted to a subscriber.
    /// Some join-maintenance rows share a sort key but are not facade roots.
    pub(super) emitted_root_keys: BTreeSet<Vec<u8>>,
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

#[cfg(test)]
mod collect_by_state_tests {
    use super::*;

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

    /// Rank a present group in the merged ordered map without constructing a
    /// combined snapshot of all groups.
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
    pub(super) before: BTreeMap<Vec<u8>, usize>,
    pub(super) after: BTreeMap<Vec<u8>, usize>,
}

pub(super) struct TickEvaluator<'a> {
    pub(super) schema: &'a DatabaseSchema,
    pub(super) graph: &'a IvmGraph,
    pub(super) variant_projections: &'a HashMap<VariantProjectionKey, VariantProjection>,
    pub(super) table_deltas: &'a [TableDelta],
    pub(super) binding_deltas: &'a [BindingDelta],
    pub(super) binding_snapshots: &'a HashMap<BindingSourceKey, RecordDeltas>,
    pub(super) current_tick: u64,
    pub(super) operator_states: &'a mut HashMap<OperatorStateKey, OperatorState>,
    pub(super) arrangement_states: &'a mut HashMap<ArrangementKey, AsOf<ArrangementState, SubTick>>,
    pub(super) arrangement_keys_by_input: &'a mut HashMap<NodeId, HashSet<ArrangementKey>>,
    pub(super) eval_memo: &'a mut HashMap<EvalMemoKey, EvalMemoEntry>,
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
    pub(super) binding_snapshots: &'a HashMap<BindingSourceKey, RecordDeltas>,
    pub(super) current_tick: u64,
    pub(super) operator_states: &'a mut HashMap<OperatorStateKey, OperatorState>,
    pub(super) arrangement_states: &'a mut HashMap<ArrangementKey, AsOf<ArrangementState, SubTick>>,
    pub(super) arrangement_keys_by_input: &'a mut HashMap<NodeId, HashSet<ArrangementKey>>,
    pub(super) eval_memo: &'a mut HashMap<EvalMemoKey, EvalMemoEntry>,
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
    binding_snapshots: &'a HashMap<BindingSourceKey, RecordDeltas>,
    current_tick: u64,
    operator_states: &'a mut HashMap<OperatorStateKey, OperatorState>,
    arrangement_states: &'a mut HashMap<ArrangementKey, AsOf<ArrangementState, SubTick>>,
    arrangement_keys_by_input: &'a mut HashMap<NodeId, HashSet<ArrangementKey>>,
    eval_memo: &'a mut HashMap<EvalMemoKey, EvalMemoEntry>,
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
        let mut isolated_memo = HashMap::default();
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
    /// Evaluate one reachable graph slice in dependency order.
    ///
    /// `update_node` may ask for its direct inputs, but those calls are memo
    /// hits because every child has completed in the same evaluation context.
    /// Keeping graph traversal here iterative makes stack use independent of
    /// graph depth, including recursive seed/step scopes which do not use the
    /// outer tick work queue.
    pub(super) async fn update_subgraph(
        &mut self,
        root: NodeId,
    ) -> Result<Arc<RecordDeltas>, IvmRuntimeError> {
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
            let records = self.update_one_node(node).await?;
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
        terminal: &mut TerminalDeltas,
    ) -> Result<(), IvmRuntimeError> {
        let Some(windows) = self.root_ordering_windows.get(&ordering_node) else {
            return Ok(());
        };
        apply_root_ordering_operations(&windows.before, &windows.after, root_descriptor, terminal);
        Ok(())
    }

    pub(super) fn terminal_delta_node_for_output(
        &self,
        node: NodeId,
    ) -> Result<Option<NodeId>, IvmRuntimeError> {
        let mut pending = vec![node];
        let mut seen = HashSet::new();
        let mut fallback = None;
        let mut has_public_root = false;
        while let Some(node) = pending.pop() {
            if !seen.insert(node) {
                continue;
            }
            let graph_node = self
                .graph
                .node(node)
                .ok_or(IvmRuntimeError::GraphNodeNotFound(node))?;
            let is_public_root = matches!(
                &graph_node.descriptor.operator,
                OpType::CollectBy(collect_by) if collect_by.mode == CollectByMode::Root
            );
            has_public_root |= is_public_root;
            if self.terminal_deltas.contains_key(&node) {
                if is_public_root {
                    return Ok(Some(node));
                }
                fallback.get_or_insert(node);
            }
            pending.extend(graph_node.descriptor.inputs.iter().copied());
        }
        Ok((!has_public_root).then_some(fallback).flatten())
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
        let mut pending = vec![node];
        let mut seen = HashSet::new();
        while let Some(node) = pending.pop() {
            if !seen.insert(node) {
                continue;
            }
            let node = self
                .graph
                .node(node)
                .ok_or(IvmRuntimeError::GraphNodeNotFound(node))?;
            match &node.descriptor.operator {
                OpType::CollectBy(collect_by) => {
                    return Ok(matches!(
                        collect_by.mode,
                        CollectByMode::Collect | CollectByMode::Root
                    ));
                }
                _ => pending.extend(node.descriptor.inputs.iter().copied()),
            }
        }
        Ok(false)
    }

    pub(super) fn output_has_public_root(&self, node: NodeId) -> Result<bool, IvmRuntimeError> {
        let mut pending = vec![node];
        let mut seen = HashSet::new();
        while let Some(node) = pending.pop() {
            if !seen.insert(node) {
                continue;
            }
            let node = self
                .graph
                .node(node)
                .ok_or(IvmRuntimeError::GraphNodeNotFound(node))?;
            if matches!(
                &node.descriptor.operator,
                OpType::CollectBy(collect_by) if collect_by.mode == CollectByMode::Root
            ) {
                return Ok(true);
            }
            pending.extend(node.descriptor.inputs.iter().copied());
        }
        Ok(false)
    }

    fn node_depends_on_aggregate(&self, node: NodeId) -> Result<bool, IvmRuntimeError> {
        let mut ancestors = HashSet::new();
        self.graph.mark_ancestors(node, &mut ancestors);
        for ancestor in ancestors {
            let graph_node = self
                .graph
                .node(ancestor)
                .ok_or(IvmRuntimeError::GraphNodeNotFound(ancestor))?;
            if matches!(graph_node.descriptor.operator, OpType::Aggregate(_)) {
                return Ok(true);
            }
        }
        Ok(false)
    }

    fn aggregate_arrangements_are_current(
        &mut self,
        node: NodeId,
    ) -> Result<bool, IvmRuntimeError> {
        self.aggregate_arrangements_are_current_inner(node, &mut HashSet::new())
    }

    fn aggregate_arrangements_are_current_inner(
        &mut self,
        node: NodeId,
        seen: &mut HashSet<NodeId>,
    ) -> Result<bool, IvmRuntimeError> {
        if !seen.insert(node) {
            return Ok(true);
        }
        let graph_node = self
            .graph
            .node(node)
            .ok_or(IvmRuntimeError::GraphNodeNotFound(node))?;
        let operator = graph_node.descriptor.operator.clone();
        let inputs = graph_node.descriptor.inputs.clone();
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
            let group_fields = self.aggregate_group_fields(node, &aggregate);
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
        for input in inputs {
            if !self.aggregate_arrangements_are_current_inner(input, seen)? {
                return Ok(false);
            }
        }
        Ok(true)
    }

    pub(super) fn update_node(
        &mut self,
        node: NodeId,
    ) -> StorageFuture<'_, Result<Arc<RecordDeltas>, IvmRuntimeError>> {
        Box::pin(self.update_subgraph(node))
    }

    fn update_one_node(
        &mut self,
        node: NodeId,
    ) -> StorageFuture<'_, Result<Arc<RecordDeltas>, IvmRuntimeError>> {
        Box::pin(async move {
            let graph_node = self
                .graph
                .node(node)
                .ok_or(IvmRuntimeError::GraphNodeNotFound(node))?;
            let signature = self.input_signature(node)?;
            let memo_key = self.memo_key(node, &signature)?;
            let current_watermark = self.input_generation(node);
            let requires_state_rebuild = (self.context.hydrate_arrangements
                && self.node_depends_on_aggregate(node)?
                && !self.aggregate_arrangements_are_current(node)?)
                || (self.context.eval_mode == EvalMode::Tick
                    && self.context.arrangement_update_mode == ArrangementUpdateMode::Replace);
            if !requires_state_rebuild
                && let Some(entry) = self.eval_memo.get_mut(&memo_key)
                && entry.input_watermark == current_watermark
            {
                *self.memo_use_clock += 1;
                entry.last_used = *self.memo_use_clock;
                if self.context.eval_mode == EvalMode::Hydrate {
                    self.metrics.hydration_memo_hits += 1;
                }
                return Ok(Arc::clone(&entry.records));
            }

            if self.context.eval_mode == EvalMode::Hydrate {
                self.metrics.hydration_memo_computes += 1;
                self.metrics.hydration_memo_computed_nodes.insert(node);
            }

            let output_desc = graph_node.descriptor.output.records();
            if self.context.sub_tick > 1 && !self.depends_on_context(node)? {
                let result = Arc::new(RecordDeltas::empty(output_desc));
                *self.memo_use_clock += 1;
                if let Some(previous) = self.eval_memo.insert(
                    memo_key,
                    EvalMemoEntry::new(
                        Arc::clone(&result),
                        current_watermark,
                        0,
                        *self.memo_use_clock,
                    ),
                ) {
                    *self.eval_memo_bytes =
                        self.eval_memo_bytes.saturating_sub(previous.payload_bytes);
                }
                return Ok(result);
            }
            let result = match &graph_node.descriptor.operator {
                OpType::TableSource(input)
                    if self.context.eval_mode == EvalMode::Hydrate
                        && self.evaluation_inputs.is_some() =>
                {
                    NodeState::update_table_source_from_inputs(
                        input,
                        self.schema,
                        self.variant_projections,
                        &output_desc,
                        self.evaluation_inputs
                            .as_deref_mut()
                            .expect("guarded evaluation inputs"),
                    )
                }
                OpType::TableSource(input) => NodeState::update_table_source(
                    input,
                    self.schema,
                    self.variant_projections,
                    &output_desc,
                    self.table_deltas,
                ),
                OpType::IndexSource(input)
                    if self.context.eval_mode == EvalMode::Hydrate
                        && self.evaluation_inputs.is_some() =>
                {
                    NodeState::update_index_source_from_inputs(
                        input,
                        self.schema,
                        self.variant_projections,
                        &output_desc,
                        self.evaluation_inputs
                            .as_deref_mut()
                            .expect("guarded evaluation inputs"),
                    )
                }
                OpType::IndexSource(input) => {
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
                OpType::InlineRecords(inline) if self.context.eval_mode == EvalMode::Hydrate => {
                    Ok(RecordDeltas {
                        descriptor: output_desc,
                        deltas: inline
                            .records
                            .iter()
                            .cloned()
                            .map(|record| RecordDelta {
                                record: record.into(),
                                weight: 1,
                            })
                            .collect(),
                    })
                }
                OpType::InlineRecords(_) => Ok(RecordDeltas::empty(output_desc)),
                OpType::BindingSource(input) => NodeState::update_binding_source(
                    input,
                    &output_desc,
                    self.binding_deltas,
                    self.binding_snapshots,
                    self.context.arrangement_update_mode,
                ),
                OpType::Arrange(_) => {
                    let [input] = graph_node.descriptor.inputs.as_slice() else {
                        return Err(IvmRuntimeError::GraphInputArityMismatch(node));
                    };
                    Ok(self.update_node(*input).await?.as_ref().clone())
                }
                OpType::FrontierSource(frontier_source) => {
                    self.frontier_source(frontier_source, &output_desc)
                }
                OpType::Filter(filter) => {
                    let input = self.update_unary_input(graph_node, node).await?;
                    if filter.predicate.supports_indirect_literal_attempt()
                        && self.evaluation_inputs.is_some()
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
                        let input = self.materialize_indirect_fields(&input, &referenced)?;
                        NodeState::update_filter(filter, output_desc, &input)
                    }
                }
                OpType::MapProject(project) => {
                    let input = self.update_unary_input(graph_node, node).await?;
                    let raw_projection =
                        self.raw_projection_fields(node, project, &input.descriptor, output_desc)?;
                    let result = NodeState::update_map_project(
                        project,
                        output_desc,
                        &input,
                        raw_projection.as_deref(),
                        false,
                    );
                    #[cfg(feature = "cold-settle-attribution")]
                    if let Ok(output) = &result {
                        crate::cold_settle_attribution::record_map(
                            self.context.eval_mode == EvalMode::Hydrate,
                            self.depends_on_dominant_child(node)?,
                            input.deltas.len(),
                            output.deltas.len(),
                        );
                    }
                    result
                }
                OpType::StreamingChecksum(checksum) => {
                    let input = self.update_unary_input(graph_node, node).await?;
                    self.update_streaming_checksum(node, checksum, output_desc, input)
                        .await
                }
                OpType::UnwrapNullable(unwrap) => {
                    let input = self.update_unary_input(graph_node, node).await?;
                    NodeState::update_unwrap_nullable(unwrap, output_desc, &input)
                }
                OpType::Unnest(unnest) => {
                    let input = self.update_unary_input(graph_node, node).await?;
                    NodeState::update_unnest(unnest, output_desc, &input)
                }
                OpType::VariantProject(variant_project) => {
                    let input = self.update_unary_input(graph_node, node).await?;
                    NodeState::update_variant_project(variant_project, output_desc, &input)
                }
                OpType::ArgMaxBy(arg_max_by) => {
                    let input = self.update_unary_input(graph_node, node).await?;
                    let input = self.materialize_indirect_field_indices(
                        &input,
                        &arg_max_by.comparison_field_indices,
                    )?;
                    self.update_arg_by(
                        node,
                        ArgBySpec {
                            group_fields: &arg_max_by.group_fields,
                            group_field_indices: &arg_max_by.group_field_indices,
                            comparison_field_indices: &arg_max_by.comparison_field_indices,
                            direction: ArgByDirection::Max,
                        },
                        output_desc,
                        &input,
                    )
                }
                OpType::ArgMinBy(arg_min_by) => {
                    let input = self.update_unary_input(graph_node, node).await?;
                    let input = self.materialize_indirect_field_indices(
                        &input,
                        &arg_min_by.comparison_field_indices,
                    )?;
                    self.update_arg_by(
                        node,
                        ArgBySpec {
                            group_fields: &arg_min_by.group_fields,
                            group_field_indices: &arg_min_by.group_field_indices,
                            comparison_field_indices: &arg_min_by.comparison_field_indices,
                            direction: ArgByDirection::Min,
                        },
                        output_desc,
                        &input,
                    )
                }
                OpType::TopBy(top_by) => {
                    let input = self.update_unary_input(graph_node, node).await?;
                    let mut fields = top_by.group_field_indices.clone();
                    fields.extend(top_by.sort_field_indices.iter().copied());
                    fields.sort_unstable();
                    fields.dedup();
                    let input = self.materialize_indirect_field_indices(&input, &fields)?;
                    self.update_top_by(node, top_by, output_desc, &input)
                }
                OpType::CollectBy(collect_by) => {
                    let input = self.update_unary_input(graph_node, node).await?;
                    let input = self.materialize_indirect_input(&input)?;
                    self.update_collect_by(node, collect_by, output_desc, &input)
                }
                OpType::Aggregate(aggregate) => {
                    let input = self.update_unary_input(graph_node, node).await?;
                    // COUNT(*) without grouping observes only row weights. Its
                    // exact result cannot depend on any scalar bytes, so retain
                    // indirect columns and issue no chunk requests.
                    let needs_values = !aggregate.group_key.is_empty()
                        || aggregate.aggregates.iter().any(|expr| {
                            expr.function != AggregateFunction::Count
                                || expr.expression.is_some()
                                || expr.distinct
                        });
                    let input = if needs_values {
                        let mut fields = aggregate.group_field_indices.clone();
                        let expression_fields = aggregate
                            .aggregates
                            .iter()
                            .filter_map(|aggregate| aggregate.expression.as_ref())
                            .cloned()
                            .collect::<Vec<_>>();
                        for field in plan_expr_fields(&expression_fields) {
                            fields.push(input.descriptor.field_index(&field).ok_or_else(|| {
                                IvmRuntimeError::GraphFieldNotFound(field.clone())
                            })?);
                        }
                        fields.sort_unstable();
                        fields.dedup();
                        self.materialize_indirect_field_indices(&input, &fields)?
                    } else {
                        input
                    };
                    self.update_aggregate(node, aggregate, output_desc, &input)
                }
                OpType::IndexBy(index_by) => {
                    let input = self.update_unary_input(graph_node, node).await?;
                    let mut fields = index_by.key_fields.clone();
                    if index_by.append_value_to_key {
                        fields.extend(index_by.value_fields.iter().copied());
                    }
                    fields.sort_unstable();
                    fields.dedup();
                    let input = self.materialize_indirect_field_indices(&input, &fields)?;
                    let trace = std::env::var_os("GROOVE_TRACE_INDEX_BY").is_some();
                    let start = trace.then(std::time::Instant::now);
                    let input_len = input.deltas.len();
                    let result = NodeState::update_index_by(index_by, output_desc, &input);
                    if trace && input_len > 0 {
                        let output_len = result
                            .as_ref()
                            .map(|records| records.deltas.len())
                            .unwrap_or(0);
                        let index_name = index_by
                            .explicit_index
                            .as_ref()
                            .map(|index| index.name.as_str())
                            .unwrap_or("<derived>");
                        let key_fields = index_by
                            .key_expressions
                            .iter()
                            .map(|expr| format!("{expr:?}"))
                            .collect::<Vec<_>>()
                            .join(",");
                        eprintln!(
                            "GROOVE_TRACE_INDEX_BY node={node:?} index={index_name} input={input_len} output={output_len} unique={} append_value_to_key={} store_value={} scan={} key_fields=[{}] elapsed_ms={:.3}",
                            index_by.unique,
                            index_by.append_value_to_key,
                            index_by.store_value,
                            index_by.scan.is_some(),
                            key_fields,
                            start.expect("trace start").elapsed().as_secs_f64() * 1000.0
                        );
                    }
                    result
                }
                OpType::Union => {
                    let input_nodes = graph_node.descriptor.inputs.clone();
                    let mut ready_inputs = Vec::with_capacity(input_nodes.len());
                    for input in input_nodes {
                        ready_inputs.push(self.update_node(input).await?);
                    }
                    NodeState::update_union(output_desc, ready_inputs)
                }
                OpType::Join(join) => {
                    let [left_input, right_input] = graph_node.descriptor.inputs.as_slice() else {
                        return Err(IvmRuntimeError::GraphInputArityMismatch(node));
                    };
                    let left = self.update_node(*left_input).await?;
                    let right = self.update_node(*right_input).await?;
                    let (left, right) = if join.residual_predicate.is_some() {
                        (
                            self.materialize_indirect_input(&left)?,
                            self.materialize_indirect_input(&right)?,
                        )
                    } else {
                        let left_fields = plan_expr_fields(&join.left_key);
                        let right_fields = plan_expr_fields(&join.right_key);
                        (
                            self.materialize_indirect_fields(&left, &left_fields)?,
                            self.materialize_indirect_fields(&right, &right_fields)?,
                        )
                    };
                    self.update_join(
                        node,
                        join,
                        output_desc,
                        *left_input,
                        *right_input,
                        &left.deltas,
                        &right.deltas,
                    )
                }
                OpType::SemiJoin(join) => {
                    let [left_input, right_input] = graph_node.descriptor.inputs.as_slice() else {
                        return Err(IvmRuntimeError::GraphInputArityMismatch(node));
                    };
                    let left = self.update_node(*left_input).await?;
                    let right = self.update_node(*right_input).await?;
                    let left_fields = plan_expr_fields(&join.left_key);
                    let right_fields = plan_expr_fields(&join.right_key);
                    let left = self.materialize_indirect_fields(&left, &left_fields)?;
                    let right = self.materialize_indirect_fields(&right, &right_fields)?;
                    self.update_semi_join(
                        node,
                        join,
                        output_desc,
                        *left_input,
                        *right_input,
                        &left.deltas,
                        &right.deltas,
                    )
                }
                OpType::AntiJoin(join) => {
                    let [left_input, right_input] = graph_node.descriptor.inputs.as_slice() else {
                        return Err(IvmRuntimeError::GraphInputArityMismatch(node));
                    };
                    let left = self.update_node(*left_input).await?;
                    let right = self.update_node(*right_input).await?;
                    let left_fields = plan_expr_fields(&join.left_key);
                    let right_fields = plan_expr_fields(&join.right_key);
                    let left = self.materialize_indirect_fields(&left, &left_fields)?;
                    let right = self.materialize_indirect_fields(&right, &right_fields)?;
                    self.update_anti_join(
                        node,
                        join,
                        output_desc,
                        *left_input,
                        *right_input,
                        &left.deltas,
                        &right.deltas,
                    )
                }
                OpType::Recursive(recursive) => {
                    let (seed, step, step_witness) = match graph_node.descriptor.inputs.as_slice() {
                        [seed, step] => (*seed, *step, None),
                        [seed, step, witness] => (*seed, *step, Some(*witness)),
                        _ => {
                            return Err(IvmRuntimeError::GraphInputArityMismatch(node));
                        }
                    };
                    self.update_recursive(node, recursive, output_desc, seed, step, step_witness)
                        .await
                }
                OpType::RecursiveStepWitness(_) => {
                    let [recursive] = graph_node.descriptor.inputs.as_slice() else {
                        return Err(IvmRuntimeError::GraphInputArityMismatch(node));
                    };
                    // Drive the owner first. Its generic side state is then
                    // the only source of this output; never re-evaluate a
                    // recursive step independently.
                    self.update_node(*recursive).await?;
                    self.update_recursive_step_witness(*recursive, output_desc)
                }
                // Durable writes are an async preparation boundary driven outside
                // this borrowed evaluator frame by `tick_durable_nodes`.
                OpType::Persist(_) => Err(IvmRuntimeError::UnsupportedOperator),
                _ => Err(IvmRuntimeError::UnsupportedOperator),
            }?;
            self.metrics.records_processed += result.deltas.len();
            let result = Arc::new(result);
            let payload_bytes = record_deltas_encoded_bytes(&result);
            *self.memo_use_clock += 1;
            if let Some(previous) = self.eval_memo.insert(
                memo_key,
                EvalMemoEntry::new(
                    Arc::clone(&result),
                    current_watermark,
                    payload_bytes,
                    *self.memo_use_clock,
                ),
            ) {
                *self.eval_memo_bytes = self.eval_memo_bytes.saturating_sub(previous.payload_bytes);
            }
            *self.eval_memo_bytes = self.eval_memo_bytes.saturating_add(payload_bytes);
            Ok(result)
        })
    }

    pub(super) fn memo_key(
        &mut self,
        node: NodeId,
        signature: &NodeInputSignature,
    ) -> Result<EvalMemoKey, IvmRuntimeError> {
        Ok(EvalMemoKey {
            scope: if self.context.scope == ScopeId::root() {
                self.operator_scope(node)?
            } else {
                self.context.scope
            },
            node,
            input_signature_hash: signature.hash,
            tick_epoch: match self.context.eval_mode {
                EvalMode::Tick => Some(self.current_tick),
                EvalMode::Hydrate => None,
            },
            sub_tick: self.context.sub_tick,
            context_digest: self.context_digest(signature),
        })
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

    fn operator_key(&mut self, node: NodeId) -> Result<OperatorStateKey, IvmRuntimeError> {
        Ok(OperatorStateKey {
            scope: self.operator_scope(node)?,
            node,
        })
    }

    fn operator_scope(&mut self, node: NodeId) -> Result<ScopeId, IvmRuntimeError> {
        // Recursive step evaluation must be isolated per recursive node even
        // for context-independent table/index inputs. Sibling recursive nodes
        // can evaluate the same base-table delta in one outer tick; sharing
        // root-scoped child operator state would let the first sibling advance
        // the table side and make later siblings miss the same positive edge.
        // Scoped child operator state is tick-local and is cleared before the
        // public tick exits.
        if self.context.scope != ScopeId::root() {
            return Ok(self.context.scope);
        }
        // Only fragments downstream of FrontierSource are scoped. Base table
        // arrangements stay global and can be reused by unrelated queries.
        if self.depends_on_context(node)? {
            Ok(self.context.scope)
        } else {
            Ok(ScopeId::root())
        }
    }

    pub(super) fn depends_on_context(&mut self, node: NodeId) -> Result<bool, IvmRuntimeError> {
        Ok(!self.input_signature(node)?.frontier_bindings.is_empty())
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
    ) -> Result<Option<Arc<[RawProjectionField]>>, IvmRuntimeError> {
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

    #[cfg(feature = "cold-settle-attribution")]
    pub(super) fn depends_on_dominant_child(&self, node: NodeId) -> Result<bool, IvmRuntimeError> {
        let graph_node = self
            .graph
            .node(node)
            .ok_or(IvmRuntimeError::GraphNodeNotFound(node))?;
        if matches!(
            &graph_node.descriptor.operator,
            OpType::TableSource(source) if source.table == "res_l_child_3"
        ) {
            return Ok(true);
        }
        // Policy lowering can replace the direct table source with an indexed
        // source. The anonymous child shape is unique in this benchmark, so
        // retain the tag through that lowering as well.
        if ["parent_id", "value_text", "value_json"]
            .into_iter()
            .all(|field| {
                graph_node
                    .descriptor
                    .output
                    .records()
                    .fields()
                    .iter()
                    .any(|candidate| candidate.name.as_deref() == Some(field))
            })
        {
            return Ok(true);
        }
        graph_node
            .descriptor
            .inputs
            .iter()
            .copied()
            .map(|input| self.depends_on_dominant_child(input))
            .collect::<Result<Vec<_>, _>>()
            .map(|dependencies| dependencies.into_iter().any(|dependency| dependency))
    }

    #[allow(clippy::too_many_arguments)]
    fn update_join(
        &mut self,
        node: NodeId,
        join: &JoinOp,
        output_desc: RecordDescriptor,
        left_input: NodeId,
        right_input: NodeId,
        left_delta: &[RecordDelta],
        right_delta: &[RecordDelta],
    ) -> Result<RecordDeltas, IvmRuntimeError> {
        let operator_key = self.operator_key(node)?;
        let operator = self
            .operator_states
            .entry(operator_key)
            .or_insert_with(|| operator_state_for(&OpType::Join(join.clone())));
        let OperatorState::Join(join_state) = operator else {
            return Err(IvmRuntimeError::NodeStateOperatorMismatch(node));
        };
        let join_state = join_state.clone();
        let (left_on, right_on) = self.join_field_names(node, join);
        let output_mapping = self.join_output_mapping(
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
        let mut left_arrangement = self
            .arrangement_states
            .remove(&left_key)
            .unwrap_or_default();
        // Pull arrangements out while applying so both sides can be mutated
        // without aliasing the arrangement map.
        let shared_arrangement_keys = if left_key == right_key {
            let mut keys = touched_join_keys(
                &join.left_descriptor,
                left_on.as_ref(),
                left_delta,
                join.comparison,
            )?;
            keys.extend(touched_join_keys(
                &join.right_descriptor,
                right_on.as_ref(),
                right_delta,
                join.comparison,
            )?);
            // `replace_keys` consumes each replacement bucket. A key can be
            // present in both sides' deltas, so pass every touched key once.
            keys.sort_unstable();
            keys.dedup();
            Some(keys)
        } else {
            None
        };
        let mut right_arrangement = if let Some(keys) = &shared_arrangement_keys {
            AsOf {
                value: left_arrangement.value().clone_keys(keys.iter()),
                as_of: left_arrangement.as_of(),
            }
        } else {
            self.arrangement_states
                .remove(&right_key)
                .unwrap_or_default()
        };
        let deltas = join_state.apply(
            &mut left_arrangement,
            &mut right_arrangement,
            &join.left_descriptor,
            &join.right_descriptor,
            &output_desc,
            &output_mapping,
            left_on.as_ref(),
            right_on.as_ref(),
            join.comparison,
            left_delta,
            right_delta,
            self.arrangement_sub_tick(&left_key),
            self.arrangement_sub_tick(&right_key),
            self.context.arrangement_update_mode,
        )?;
        if let Some(keys) = shared_arrangement_keys {
            left_arrangement
                .value_mut()
                .replace_keys(keys.iter(), right_arrangement.value().clone());
        } else {
            self.insert_arrangement(right_key, right_arrangement);
        }
        self.insert_arrangement(left_key, left_arrangement);
        #[cfg(feature = "cold-settle-attribution")]
        crate::cold_settle_attribution::record_join(
            self.context.eval_mode == EvalMode::Hydrate,
            self.depends_on_dominant_child(node)?,
            left_delta.len(),
            right_delta.len(),
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
        left_delta: &[RecordDelta],
        right_delta: &[RecordDelta],
    ) -> Result<RecordDeltas, IvmRuntimeError> {
        let operator_key = self.operator_key(node)?;
        let (left_on, right_on) = self.join_field_names(node, join);
        let left_key =
            self.arrangement_key(left_input, join.left_descriptor, &left_on, join.comparison)?;
        let right_key = self.arrangement_key(
            right_input,
            join.right_descriptor,
            &right_on,
            join.comparison,
        )?;
        // Anti-join publication is operator-local state. Take it while the
        // evaluator mutates arrangements, then restore it even if evaluation
        // rejects a malformed delta. Cloning would copy every published row
        // for each small incremental update.
        let mut join_state = match self.operator_states.remove(&operator_key) {
            None => AntiJoinState::default(),
            Some(OperatorState::AntiJoin(state)) => state,
            Some(operator) => {
                self.operator_states.insert(operator_key, operator);
                return Err(IvmRuntimeError::NodeStateOperatorMismatch(node));
            }
        };
        let mut left_arrangement = self
            .arrangement_states
            .remove(&left_key)
            .unwrap_or_default();
        let mut right_arrangement = if left_key == right_key {
            left_arrangement.clone()
        } else {
            self.arrangement_states
                .remove(&right_key)
                .unwrap_or_default()
        };
        let result = (|| {
            let deltas = join_state.apply(
                &mut left_arrangement,
                &mut right_arrangement,
                &join.left_descriptor,
                &join.right_descriptor,
                &output_desc,
                left_on.as_ref(),
                right_on.as_ref(),
                join.comparison,
                left_delta,
                right_delta,
                self.arrangement_sub_tick(&left_key),
                self.arrangement_sub_tick(&right_key),
                self.context.arrangement_update_mode,
            )?;
            if left_key == right_key {
                left_arrangement = right_arrangement;
            } else {
                self.insert_arrangement(right_key, right_arrangement);
            }
            self.insert_arrangement(left_key, left_arrangement);
            #[cfg(feature = "cold-settle-attribution")]
            crate::cold_settle_attribution::record_join(
                self.context.eval_mode == EvalMode::Hydrate,
                self.depends_on_dominant_child(node)?,
                left_delta.len(),
                right_delta.len(),
                deltas.len(),
            );
            Ok(RecordDeltas {
                descriptor: output_desc,
                deltas,
            })
        })();
        self.operator_states
            .insert(operator_key, OperatorState::AntiJoin(join_state));
        result
    }

    #[allow(clippy::too_many_arguments)]
    fn update_semi_join(
        &mut self,
        node: NodeId,
        join: &JoinOp,
        output_desc: RecordDescriptor,
        left_input: NodeId,
        right_input: NodeId,
        left_delta: &[RecordDelta],
        right_delta: &[RecordDelta],
    ) -> Result<RecordDeltas, IvmRuntimeError> {
        let operator_key = self.operator_key(node)?;
        let operator = self
            .operator_states
            .entry(operator_key.clone())
            .or_insert_with(|| operator_state_for(&OpType::SemiJoin(join.clone())));
        let OperatorState::SemiJoin(join_state) = operator else {
            return Err(IvmRuntimeError::NodeStateOperatorMismatch(node));
        };
        let mut join_state = join_state.clone();
        let (left_on, right_on) = self.join_field_names(node, join);
        let left_key =
            self.arrangement_key(left_input, join.left_descriptor, &left_on, join.comparison)?;
        let right_key = self.arrangement_key(
            right_input,
            join.right_descriptor,
            &right_on,
            join.comparison,
        )?;
        let mut left_arrangement = self
            .arrangement_states
            .remove(&left_key)
            .unwrap_or_default();
        let mut right_arrangement = if left_key == right_key {
            left_arrangement.clone()
        } else {
            self.arrangement_states
                .remove(&right_key)
                .unwrap_or_default()
        };
        let deltas = join_state.apply(
            &mut left_arrangement,
            &mut right_arrangement,
            join.left_descriptor,
            join.right_descriptor,
            &output_desc,
            left_on.as_ref(),
            right_on.as_ref(),
            join.comparison,
            left_delta,
            right_delta,
            self.arrangement_sub_tick(&left_key),
            self.arrangement_sub_tick(&right_key),
            self.context.arrangement_update_mode,
        )?;
        if left_key == right_key {
            left_arrangement = right_arrangement;
        } else {
            self.insert_arrangement(right_key, right_arrangement);
        }
        self.insert_arrangement(left_key, left_arrangement);
        self.operator_states
            .insert(operator_key, OperatorState::SemiJoin(join_state));
        #[cfg(feature = "cold-settle-attribution")]
        crate::cold_settle_attribution::record_join(
            self.context.eval_mode == EvalMode::Hydrate,
            self.depends_on_dominant_child(node)?,
            left_delta.len(),
            right_delta.len(),
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
        if input.deltas.is_empty() {
            return Ok(RecordDeltas::empty(output_desc));
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
        let arrangement_key = self.arrangement_key(
            *input_node,
            output_desc,
            spec.group_fields,
            ValueComparison::Exact,
        )?;
        let sub_tick = self.arrangement_sub_tick(&arrangement_key);
        let mut arrangement = self
            .arrangement_states
            .remove(&arrangement_key)
            .unwrap_or_default();
        let should_apply_arrangement = self.context.arrangement_update_mode
            == ArrangementUpdateMode::Replace
            || arrangement.as_of() != Some(sub_tick);
        if should_apply_arrangement {
            let replace_within_same_tick = self.context.arrangement_update_mode
                == ArrangementUpdateMode::Replace
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
            arrangement.value_mut().apply_record_deltas(
                output_desc,
                spec.group_fields,
                &input.deltas,
                self.context.arrangement_update_mode,
            )?;
            if replace_within_same_tick {
                arrangement.replace_as_of_at_least(sub_tick);
            } else {
                arrangement.mark_forward_as_of(sub_tick)?;
            }
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
            let after_records = arrangement.value().records_for_key(&group_prefix);
            let after = arg_by_winner_from_records(
                output_desc,
                spec.comparison_field_indices,
                after_records.clone(),
                spec.direction,
            )?;
            let before = arg_by_winner_before_from_deltas(
                output_desc,
                spec.comparison_field_indices,
                after_records,
                group_deltas,
                spec.direction,
            )?;
            if before == after {
                continue;
            }
            if let Some((_, record)) = before {
                output.push(RecordDelta { record, weight: -1 });
            }
            if let Some((_, record)) = after {
                output.push(RecordDelta { record, weight: 1 });
            }
        }
        self.insert_arrangement(arrangement_key, arrangement);

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
        let operator_key = self.operator_key(node)?;
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
            let windows = self.root_ordering_windows.entry(node).or_default();
            extend_root_window_positions(output_desc, &before, &mut windows.before)?;
            extend_root_window_positions(output_desc, &after, &mut windows.after)?;
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
    fn update_collect_by(
        &mut self,
        node: NodeId,
        collect_by: &CollectByOp,
        output_desc: RecordDescriptor,
        input: &RecordDeltas,
    ) -> Result<RecordDeltas, IvmRuntimeError> {
        if self.context.eval_mode == EvalMode::Hydrate {
            // Hydration supplies a complete snapshot, including when another
            // subscription already owns this shared collector. Rebuild its
            // state instead of applying that snapshot as incremental inserts;
            // otherwise a later retraction leaves a phantom duplicate behind.
            let operator_key = self.operator_key(node)?;
            self.operator_states.remove(&operator_key);
        }
        if input.deltas.is_empty() || collect_by.limit == TopByLimit::Finite(0) {
            return Ok(RecordDeltas::empty(output_desc));
        }
        let direct_tree_slot = match collect_by.slots.as_slice() {
            [] if collect_by.limit == TopByLimit::Unbounded => None,
            [slot] if slot.slots.is_empty() && slot.limit == TopByLimit::Unbounded => Some(slot),
            _ => None,
        };
        if collect_by.mode == CollectByMode::Root
            || (collect_by.mode == CollectByMode::Collect
                && (collect_by.slots.is_empty() || direct_tree_slot.is_some())
                && (collect_by.limit == TopByLimit::Unbounded || direct_tree_slot.is_some()))
        {
            let operator_key = self.operator_key(node)?;
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
        let sub_tick = self.arrangement_sub_tick(&arrangement_key);
        let mut arrangement = self
            .arrangement_states
            .remove(&arrangement_key)
            .unwrap_or_default();
        let should_apply_arrangement = self.context.arrangement_update_mode
            == ArrangementUpdateMode::Replace
            || arrangement.as_of() != Some(sub_tick);
        if should_apply_arrangement {
            let replace_within_same_tick = self.context.arrangement_update_mode
                == ArrangementUpdateMode::Replace
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
            arrangement.value_mut().apply_record_deltas(
                input_desc,
                &collect_by.group_fields,
                &input.deltas,
                self.context.arrangement_update_mode,
            )?;
            if replace_within_same_tick {
                arrangement.replace_as_of_at_least(sub_tick);
            } else {
                arrangement.mark_forward_as_of(sub_tick)?;
            }
        }

        let mut touched_groups = BTreeMap::<Vec<u8>, Vec<RecordDelta>>::new();
        for delta in &input.deltas {
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
            let before_records = records_before_deltas(after_records.clone(), &group_deltas);
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
        self.insert_arrangement(arrangement_key, arrangement);
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
                if !self.arrangement_states.contains_key(&arrangement_key) {
                    let mut arrangement = AsOf::<ArrangementState, SubTick>::default();
                    arrangement.mark_forward_as_of(self.arrangement_sub_tick(&arrangement_key))?;
                    self.insert_arrangement(arrangement_key, arrangement);
                }
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
            if self.context.hydrate_arrangements {
                let mut arrangement = AsOf::<ArrangementState, SubTick>::default();
                arrangement.value_mut().apply_record_deltas(
                    input_desc,
                    group_fields.as_ref(),
                    &input.deltas,
                    ArrangementUpdateMode::Replace,
                )?;
                arrangement.mark_forward_as_of(self.arrangement_sub_tick(&arrangement_key))?;
                self.insert_arrangement(arrangement_key, arrangement);
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
        let sub_tick = self.arrangement_sub_tick(&arrangement_key);
        let mut touched_groups = BTreeMap::<Vec<u8>, Vec<RecordDelta>>::new();
        for delta in &input.deltas {
            let group_key =
                encoded_record_key_part(input_desc, delta.raw(), &aggregate.group_field_indices)?;
            touched_groups
                .entry(group_key)
                .or_default()
                .push(delta.clone());
        }
        let current_arrangement = self.arrangement_states.get(&arrangement_key);
        let current_as_of = current_arrangement.and_then(AsOf::as_of);
        let should_apply_arrangement = self.context.arrangement_update_mode
            == ArrangementUpdateMode::Replace
            || current_as_of != Some(sub_tick);
        let replace_within_same_tick = self.context.arrangement_update_mode
            == ArrangementUpdateMode::Replace
            && current_as_of.is_some_and(|current| current.tick == sub_tick.tick);
        if !replace_within_same_tick && current_as_of.is_some_and(|current| current > sub_tick) {
            return Err(IvmRuntimeError::OutOfOrderRuntimeState {
                current: format!("{:?}", current_as_of.expect("checked above")),
                next: format!("{sub_tick:?}"),
            });
        }
        let before_groups = if self.context.arrangement_update_mode
            == ArrangementUpdateMode::Replace
            || !should_apply_arrangement
        {
            BTreeMap::new()
        } else {
            touched_groups
                .keys()
                .map(|group| {
                    (
                        group.clone(),
                        current_arrangement
                            .map(|arrangement| arrangement.value().records_for_key(group))
                            .unwrap_or_default(),
                    )
                })
                .collect::<BTreeMap<_, _>>()
        };
        let mut staged_arrangement =
            if self.context.arrangement_update_mode == ArrangementUpdateMode::Replace {
                ArrangementState::default()
            } else {
                current_arrangement
                    .map(|arrangement| arrangement.value().clone_keys(touched_groups.keys()))
                    .unwrap_or_default()
            };
        if should_apply_arrangement {
            staged_arrangement.apply_record_deltas(
                input_desc,
                group_fields.as_ref(),
                &input.deltas,
                self.context.arrangement_update_mode,
            )?;
        }

        let mut output = Vec::new();
        for group_prefix in touched_groups.keys() {
            let after_records = staged_arrangement.records_for_key(group_prefix);
            let after =
                aggregate_row_from_records(input_desc, output_desc, aggregate, &after_records)?;
            let before_records = if let Some(records) = before_groups
                .get(group_prefix)
                .filter(|records| !records.is_empty())
            {
                records.clone()
            } else {
                records_before_from_deltas(
                    after_records,
                    touched_groups
                        .get(group_prefix)
                        .cloned()
                        .unwrap_or_default(),
                )
            };
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
        if should_apply_arrangement {
            match self.context.arrangement_update_mode {
                ArrangementUpdateMode::Accumulate => {
                    let arrangement = self.arrangement_entry(arrangement_key);
                    arrangement.mark_forward_as_of(sub_tick)?;
                    arrangement
                        .value_mut()
                        .replace_keys(touched_groups.keys(), staged_arrangement);
                }
                ArrangementUpdateMode::Replace => {
                    let mut arrangement = AsOf {
                        value: staged_arrangement,
                        as_of: current_as_of,
                    };
                    if replace_within_same_tick {
                        arrangement.replace_as_of_at_least(sub_tick);
                    } else {
                        arrangement.mark_forward_as_of(sub_tick)?;
                    }
                    self.insert_arrangement(arrangement_key, arrangement);
                }
            }
        }

        Ok(RecordDeltas {
            descriptor: output_desc,
            deltas: consolidate_deltas(output),
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
            scope: self.operator_scope(input)?,
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

    pub(super) fn join_output_mapping(
        &mut self,
        node: NodeId,
        left_descriptor: RecordDescriptor,
        right_descriptor: RecordDescriptor,
        output_descriptor: RecordDescriptor,
    ) -> Result<Arc<[(usize, usize)]>, IvmRuntimeError> {
        if let Some(mapping) = &self.node_meta.entry(node).or_default().join_output_mapping {
            return Ok(mapping.clone());
        }
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
        let mapping = Arc::<[(usize, usize)]>::from(mapping);
        self.node_meta.entry(node).or_default().join_output_mapping = Some(mapping.clone());
        Ok(mapping)
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

    fn arrangement_entry(&mut self, key: ArrangementKey) -> &mut AsOf<ArrangementState, SubTick> {
        self.arrangement_keys_by_input
            .entry(key.input)
            .or_default()
            .insert(key.clone());
        self.arrangement_states.entry(key).or_default()
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
        let operator_key = self.operator_key(node)?;
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
        let key = self.operator_key(recursive_node)?;
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
        let operator_key = self.operator_key(node)?;
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
                            value.clone(),
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
                input
                    .descriptor
                    .field_index(field)
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
