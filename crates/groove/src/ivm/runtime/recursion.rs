//! Recursive operator state and scoped fixpoint evaluation.
//!
//! This module owns [`RecursiveState`], recursive frontier handling, step
//! arrangement hydration, and the bounded positive-recursion loop used by the
//! runtime. It reuses the main graph evaluator under recursive scopes rather
//! than defining separate operators. Join arrangements live in [`super::join`];
//! public ticks, subscriptions, and graph retention live in [`super`].

use bytes::Bytes;
use rustc_hash::{FxHashMap as HashMap, FxHashSet as HashSet};
use std::rc::Rc;

use crate::ivm::{IvmGraph, NodeId, OpType, RecursiveOp, StaticScanSpec, TableSourceOp};
use crate::records::RecordDescriptor;
use crate::storage::{OrderedKvStorage, ScanBounds, StorageFuture};

use super::evaluation_session::EvaluationInputs;
use super::subscriptions::BindingDelta;

use super::{
    ArrangementUpdateMode, AsOf, EvalContext, GraphRuntimeView, IvmRuntimeError, NodeState,
    RecordDelta, RecordDeltas, ScopeId, StaticScanBounds, SubTick, TableDelta, VariantProjection,
    VariantProjectionKey, consolidate_deltas, plan_expr_names, project_binding_source_deltas,
    scan_bounds,
};

#[derive(Clone, Debug, Default)]
pub(super) struct RecursiveState {
    /// Current recursive result as an encoded-record multiset.
    /// For now recursive outputs are set-style: each reachable record is kept
    /// at weight 1. Bag recursion can diverge on cycles, and non-monotone
    /// recursion needs a DRed/DBSP design before we accept negative frontiers.
    accumulated: Rc<HashMap<Bytes, i64>>,
    /// Positive incremental ticks rely on step-side arrangements already
    /// containing the full base/accumulated state after a recompute.
    step_arrangements_hydrated: bool,
    /// Input generation represented by `accumulated`. Database ticks alone do
    /// not capture same-tick binding changes made while installing a prepared
    /// subscription.
    hydrated_input_generation: Option<u64>,
    /// A retained, inputs-backed snapshot recomputation. Subscription opening
    /// owns a disposable operator future, so recursive hydration must keep its
    /// postorder traversal and frontier outside that future when it yields.
    pending_hydration: Option<Box<PendingHydrationRecompute>>,
}

const MAX_HYDRATION_TRAVERSAL_NODES_PER_POLL: usize = 32;

#[derive(Clone, Debug)]
struct PendingHydrationRecompute {
    /// A pending snapshot is tied to the exact input generation that started
    /// it. A later binding/table generation must restart from a coherent
    /// snapshot rather than blend old memoized subgraphs into new inputs.
    input_generation: u64,
    accumulated: HashMap<Bytes, i64>,
    phase: PendingHydrationPhase,
    traversal: Option<HydrationTraversal>,
}

#[derive(Clone, Debug)]
enum PendingHydrationPhase {
    Seed,
    Step {
        frontier: RecordDeltas,
        sub_tick: usize,
    },
    ReadyForArrangementHydration,
}

/// A postorder walk over one immutable graph/context snapshot. Discovering and
/// evaluating are both explicitly bounded; `memo` makes a hash-consed child
/// run once even when several parents reference it.
#[derive(Clone, Debug)]
struct HydrationTraversal {
    root: NodeId,
    context: EvalContext,
    discovery: Vec<HydrationTraversalFrame>,
    discovered: HashSet<NodeId>,
    visiting: HashSet<NodeId>,
    order: Vec<NodeId>,
    next_evaluation: usize,
    memo: HashMap<NodeId, RecordDeltas>,
}

#[derive(Clone, Copy, Debug)]
enum HydrationTraversalFrame {
    Visit(NodeId),
    Evaluate(NodeId),
}

#[derive(Clone, Debug)]
enum HydrationTraversalProgress {
    Yield,
    Ready(RecordDeltas),
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum HydrationRecomputeProgress {
    Yield,
    ReadyForArrangementHydration,
}

/// A live recursive tick either has its public delta ready or has saved its
/// snapshot traversal in [`RecursiveState`] and must yield to the owner.
pub(super) enum RecursiveDeltaProgress {
    Yield,
    Ready(Vec<RecordDelta>),
}

pub(super) struct HydrationRecomputeContext<'a> {
    pub(super) schema: &'a crate::schema::DatabaseSchema,
    pub(super) graph: &'a IvmGraph,
    pub(super) variant_projections: &'a HashMap<VariantProjectionKey, VariantProjection>,
    /// `Some` routes source reads through the session request registry; `None`
    /// is the direct-storage path used by a live tick that must still retain
    /// its bounded graph traversal across cooperative yields.
    pub(super) inputs: Option<&'a mut EvaluationInputs>,
    pub(super) table_deltas: Option<&'a [TableDelta]>,
    pub(super) storage: &'a dyn OrderedKvStorage,
    pub(super) binding_snapshots: &'a HashMap<String, RecordDeltas>,
    pub(super) scope: ScopeId,
    pub(super) input_generation: u64,
}

impl RecursiveState {
    pub(super) fn is_empty(&self) -> bool {
        self.accumulated.is_empty()
    }

    pub(super) fn step_arrangements_hydrated(&self) -> bool {
        self.step_arrangements_hydrated
    }

    pub(super) fn accumulated_row_count(&self) -> usize {
        self.accumulated
            .values()
            .filter(|weight| **weight != 0)
            .count()
    }

    pub(super) fn accumulated_encoded_bytes(&self) -> usize {
        self.accumulated.keys().map(|record| record.len()).sum()
    }

    pub(super) fn mark_step_arrangements_hydrated(&mut self) {
        self.step_arrangements_hydrated = true;
    }

    pub(super) fn hydrated_input_generation(&self) -> Option<u64> {
        self.hydrated_input_generation
    }

    pub(super) fn mark_hydrated_input_generation(&mut self, generation: u64) {
        self.hydrated_input_generation = Some(generation);
    }

    pub(super) fn has_pending_hydration(&self) -> bool {
        self.pending_hydration.is_some()
    }

    pub(super) fn has_pending_hydration_for(&self, generation: u64) -> bool {
        self.pending_hydration
            .as_ref()
            .is_some_and(|pending| pending.input_generation == generation)
    }

    fn begin_hydration_recompute(&mut self, input_generation: u64) {
        self.pending_hydration = Some(Box::new(PendingHydrationRecompute {
            input_generation,
            accumulated: HashMap::default(),
            phase: PendingHydrationPhase::Seed,
            traversal: None,
        }));
    }

    fn pending_hydration_mut(&mut self) -> &mut PendingHydrationRecompute {
        self.pending_hydration
            .as_mut()
            .expect("hydration recompute was initialized")
    }

    pub(super) fn pending_hydration_accumulated_deltas(
        &self,
        descriptor: RecordDescriptor,
    ) -> RecordDeltas {
        let pending = self
            .pending_hydration
            .as_ref()
            .expect("hydration recompute was initialized");
        RecordDeltas {
            descriptor,
            deltas: pending
                .accumulated
                .iter()
                .filter_map(|(record, weight)| {
                    (*weight > 0).then_some(RecordDelta {
                        record: record.clone(),
                        weight: *weight,
                    })
                })
                .collect(),
        }
    }

    pub(super) fn finish_hydration_recompute(&mut self) -> HashMap<Bytes, i64> {
        let pending = self
            .pending_hydration
            .take()
            .expect("hydration recompute was initialized");
        debug_assert!(matches!(
            pending.phase,
            PendingHydrationPhase::ReadyForArrangementHydration
        ));
        debug_assert!(pending.traversal.is_none());
        pending.accumulated
    }

    pub(super) fn accumulated_deltas(&self) -> Vec<RecordDelta> {
        self.accumulated
            .iter()
            .filter_map(|(record, weight)| {
                (*weight > 0).then_some(RecordDelta {
                    record: record.clone(),
                    weight: *weight,
                })
            })
            .collect()
    }

    fn accept_positive(
        &mut self,
        deltas: Vec<RecordDelta>,
    ) -> Result<Vec<RecordDelta>, IvmRuntimeError> {
        reject_non_positive_frontier_deltas(&deltas)?;
        let mut accepted = Vec::new();
        for delta in consolidate_deltas(deltas) {
            if delta.weight <= 0 {
                return Err(IvmRuntimeError::UnsupportedNonMonotoneRecursion);
            }
            // Frontier propagation is set-style for now. Recursive bag
            // semantics can diverge on cycles, so duplicate derivations collapse
            // to the already-known fact instead of increasing support counts.
            if self.accumulated.contains_key(&delta.record) {
                continue;
            }
            Rc::make_mut(&mut self.accumulated).insert(delta.record.clone(), 1);
            accepted.push(RecordDelta {
                record: delta.record,
                weight: 1,
            });
        }
        Ok(consolidate_deltas(accepted))
    }

    pub(super) fn replace_with(&mut self, next: HashMap<Bytes, i64>) -> Vec<RecordDelta> {
        let mut deltas = Vec::new();
        for (record, old_weight) in self.accumulated.iter() {
            let next_weight = next.get(record).copied().unwrap_or_default();
            let delta = next_weight - old_weight;
            if delta != 0 {
                deltas.push(RecordDelta {
                    record: record.clone(),
                    weight: delta,
                });
            }
        }
        for (record, next_weight) in &next {
            if self.accumulated.contains_key(record) || *next_weight == 0 {
                continue;
            }
            deltas.push(RecordDelta {
                record: record.clone(),
                weight: *next_weight,
            });
        }
        self.accumulated = Rc::new(next);
        self.step_arrangements_hydrated = false;
        self.pending_hydration = None;
        consolidate_deltas(deltas)
    }
}

impl HydrationTraversal {
    fn new(root: NodeId, context: EvalContext) -> Self {
        Self {
            root,
            context,
            discovery: vec![HydrationTraversalFrame::Visit(root)],
            discovered: HashSet::default(),
            visiting: HashSet::default(),
            order: Vec::new(),
            next_evaluation: 0,
            memo: HashMap::default(),
        }
    }

    async fn poll(
        &mut self,
        context: &mut HydrationRecomputeContext<'_>,
    ) -> Result<HydrationTraversalProgress, IvmRuntimeError> {
        let mut remaining = MAX_HYDRATION_TRAVERSAL_NODES_PER_POLL;
        while remaining > 0 {
            if let Some(frame) = self.discovery.pop() {
                remaining -= 1;
                match frame {
                    HydrationTraversalFrame::Visit(node) => {
                        if !self.discovered.insert(node) {
                            if self.visiting.contains(&node) {
                                return Err(IvmRuntimeError::GraphCycle(node));
                            }
                            continue;
                        }
                        self.visiting.insert(node);
                        let graph_node = context
                            .graph
                            .node(node)
                            .ok_or(IvmRuntimeError::GraphNodeNotFound(node))?;
                        self.discovery.push(HydrationTraversalFrame::Evaluate(node));
                        for input in graph_node.descriptor.inputs.iter().rev() {
                            self.discovery.push(HydrationTraversalFrame::Visit(*input));
                        }
                    }
                    HydrationTraversalFrame::Evaluate(node) => {
                        debug_assert!(self.visiting.remove(&node));
                        self.order.push(node);
                    }
                }
                continue;
            }

            let Some(node) = self.order.get(self.next_evaluation).copied() else {
                return self
                    .memo
                    .get(&self.root)
                    .cloned()
                    .map(HydrationTraversalProgress::Ready)
                    .ok_or(IvmRuntimeError::GraphNodeNotFound(self.root));
            };
            remaining -= 1;
            let mut evaluator = HydrationEvaluator {
                schema: context.schema,
                graph: context.graph,
                variant_projections: context.variant_projections,
                table_deltas: context.table_deltas,
                evaluation_inputs: context.inputs.as_deref_mut(),
                storage: context.storage,
                binding_snapshots: context.binding_snapshots,
                context: self.context.clone(),
                memo: std::mem::take(&mut self.memo),
            };
            let result = evaluator.eval_node(node).await;
            self.memo = evaluator.memo;
            let result = result?;
            self.memo.insert(node, result);
            self.next_evaluation += 1;
        }
        Ok(HydrationTraversalProgress::Yield)
    }
}

/// Resume one bounded phase of an inputs-backed recursive snapshot. The caller
/// owns the operator-state save/yield boundary; this helper only advances the
/// retained postorder traversal or moves the fixpoint frontier forward.
pub(super) async fn resume_inputs_hydration_recompute(
    recursive_state: &mut RecursiveState,
    mut context: HydrationRecomputeContext<'_>,
    node: NodeId,
    recursive: &RecursiveOp,
    output_desc: RecordDescriptor,
    seed: NodeId,
    step: NodeId,
) -> Result<HydrationRecomputeProgress, IvmRuntimeError> {
    if recursive_state
        .pending_hydration
        .as_ref()
        .is_none_or(|pending| pending.input_generation != context.input_generation)
    {
        recursive_state.begin_hydration_recompute(context.input_generation);
    }
    let phase = recursive_state.pending_hydration_mut().phase.clone();
    match phase {
        PendingHydrationPhase::Seed => {
            let traversal = recursive_state
                .pending_hydration_mut()
                .traversal
                .take()
                .unwrap_or_else(|| HydrationTraversal::new(seed, EvalContext::root()));
            let mut traversal = traversal;
            let progress = match traversal.poll(&mut context).await {
                Ok(progress) => progress,
                // The session owns request registration, but this operator
                // owns traversal continuation. Keep the exact postorder
                // stack/memo across a cold source instead of replaying every
                // resident ancestor once the request is ready.
                Err(IvmRuntimeError::EvaluationBlocked) => {
                    recursive_state.pending_hydration_mut().traversal = Some(traversal);
                    return Err(IvmRuntimeError::EvaluationBlocked);
                }
                Err(error) => return Err(error),
            };
            match progress {
                HydrationTraversalProgress::Yield => {
                    recursive_state.pending_hydration_mut().traversal = Some(traversal);
                    Ok(HydrationRecomputeProgress::Yield)
                }
                HydrationTraversalProgress::Ready(frontier) => {
                    if frontier.descriptor != output_desc {
                        return Err(IvmRuntimeError::GraphOutputMismatch);
                    }
                    let accepted = accept_positive_into_set(
                        &mut recursive_state.pending_hydration_mut().accumulated,
                        frontier.deltas,
                    )?;
                    recursive_state.pending_hydration_mut().phase = if accepted.is_empty() {
                        PendingHydrationPhase::ReadyForArrangementHydration
                    } else {
                        PendingHydrationPhase::Step {
                            frontier: RecordDeltas {
                                descriptor: output_desc,
                                deltas: accepted,
                            },
                            sub_tick: 1,
                        }
                    };
                    Ok(HydrationRecomputeProgress::Yield)
                }
            }
        }
        PendingHydrationPhase::Step { frontier, sub_tick } => {
            if sub_tick > recursive.max_iters {
                return Err(IvmRuntimeError::RecursiveIterationLimit {
                    node,
                    max_iters: recursive.max_iters,
                });
            }
            let eval_context = EvalContext::with_binding(
                context.scope,
                sub_tick as u64,
                recursive.frontier.clone(),
                frontier,
            );
            let traversal = recursive_state
                .pending_hydration_mut()
                .traversal
                .take()
                .unwrap_or_else(|| HydrationTraversal::new(step, eval_context));
            let mut traversal = traversal;
            let progress = match traversal.poll(&mut context).await {
                Ok(progress) => progress,
                Err(IvmRuntimeError::EvaluationBlocked) => {
                    recursive_state.pending_hydration_mut().traversal = Some(traversal);
                    return Err(IvmRuntimeError::EvaluationBlocked);
                }
                Err(error) => return Err(error),
            };
            match progress {
                HydrationTraversalProgress::Yield => {
                    recursive_state.pending_hydration_mut().traversal = Some(traversal);
                    Ok(HydrationRecomputeProgress::Yield)
                }
                HydrationTraversalProgress::Ready(step_delta) => {
                    if step_delta.descriptor != output_desc {
                        return Err(IvmRuntimeError::GraphOutputMismatch);
                    }
                    let accepted = accept_positive_into_set(
                        &mut recursive_state.pending_hydration_mut().accumulated,
                        step_delta.deltas,
                    )?;
                    recursive_state.pending_hydration_mut().phase = if accepted.is_empty() {
                        PendingHydrationPhase::ReadyForArrangementHydration
                    } else {
                        PendingHydrationPhase::Step {
                            frontier: RecordDeltas {
                                descriptor: output_desc,
                                deltas: accepted,
                            },
                            sub_tick: sub_tick + 1,
                        }
                    };
                    Ok(HydrationRecomputeProgress::Yield)
                }
            }
        }
        PendingHydrationPhase::ReadyForArrangementHydration => {
            Ok(HydrationRecomputeProgress::ReadyForArrangementHydration)
        }
    }
}

pub(super) async fn recursive_delta(
    recursive_state: &mut RecursiveState,
    mut runtime: GraphRuntimeView<'_>,
    node: NodeId,
    recursive: &RecursiveOp,
    output_desc: RecordDescriptor,
    seed: NodeId,
    step: NodeId,
) -> Result<RecursiveDeltaProgress, IvmRuntimeError> {
    let has_recompute_table_delta = has_recompute_table_delta_for_recursion(&runtime, seed, step)?;
    let has_table_delta = has_table_delta_for_cached_tables(&runtime, recursive);
    let has_recompute_binding_delta =
        has_recompute_binding_delta_for_recursion(&runtime, seed, step)?;
    let has_binding_deltas = !runtime.binding_deltas.is_empty();
    if has_recompute_table_delta
        || has_recompute_binding_delta
        || (!has_binding_deltas && recursive_state.is_empty())
        || !recursive_state.step_arrangements_hydrated()
    {
        // Retractions are handled by full recompute + diff until we implement
        // DRed or DBSP-style nested negative deltas.
        if !recursive_state.has_pending_hydration_for(runtime.current_tick) {
            runtime.metrics.recursive_recomputes += 1;
        }
        if std::env::var_os("JAZZ_CLOSURE_TRACE").is_some() {
            eprintln!(
                "CLOSURE_TRACE event=recursive_recompute node={node:?} scope={:?} has_recompute_table_delta={has_recompute_table_delta} has_table_delta={has_table_delta} has_recompute_binding_delta={has_recompute_binding_delta} has_binding_deltas={has_binding_deltas} state_empty={} step_hydrated={} total_recomputes={}",
                runtime.scope,
                recursive_state.is_empty(),
                recursive_state.step_arrangements_hydrated(),
                runtime.metrics.recursive_recomputes,
            );
        }
        let progress = resume_inputs_hydration_recompute(
            recursive_state,
            HydrationRecomputeContext {
                schema: runtime.schema,
                graph: runtime.graph,
                variant_projections: runtime.variant_projections,
                inputs: runtime.evaluation_inputs.as_deref_mut(),
                table_deltas: None,
                storage: runtime.storage,
                binding_snapshots: runtime.binding_snapshots,
                scope: runtime.scope,
                // The live tick's inputs are immutable while it is driven.
                // Retrying after a cooperative yield can resume its exact
                // traversal; a later tick restarts with a fresh snapshot.
                input_generation: runtime.current_tick,
            },
            node,
            recursive,
            output_desc,
            seed,
            step,
        )
        .await?;
        if progress == HydrationRecomputeProgress::Yield {
            return Ok(RecursiveDeltaProgress::Yield);
        }
        let next = recursive_state.finish_hydration_recompute();
        let accumulated = RecordDeltas {
            descriptor: output_desc,
            deltas: next
                .iter()
                .filter_map(|(record, weight)| {
                    (*weight > 0).then_some(RecordDelta {
                        record: record.clone(),
                        weight: *weight,
                    })
                })
                .collect(),
        };
        let emitted = recursive_state.replace_with(next);
        hydrate_recursive_arrangements(&mut runtime, recursive, step, accumulated).await?;
        recursive_state.mark_step_arrangements_hydrated();
        return Ok(RecursiveDeltaProgress::Ready(emitted));
    }

    let mut emitted = Vec::new();
    let seed_delta = if has_binding_deltas {
        // A new binding can make existing seed-table rows visible even when no
        // table delta occurs in this tick. Evaluate the seed over a current
        // snapshot so binding-as-data opens produce their initial frontier
        // without forcing a full recursive recompute.
        let full_table_deltas = match runtime.evaluation_inputs.as_deref_mut() {
            Some(inputs) => {
                snapshot_table_deltas_from_inputs(runtime.schema, runtime.graph, inputs, seed)?
            }
            None => {
                snapshot_table_deltas(runtime.schema, runtime.graph, runtime.storage, seed).await?
            }
        };
        runtime
            .eval_with_binding_and_table_deltas(
                &full_table_deltas,
                0,
                recursive.frontier.clone(),
                RecordDeltas::empty(output_desc),
                seed,
            )
            .await?
    } else {
        runtime.eval_root(seed).await?
    };
    let seed_delta_count = seed_delta.deltas.len();
    if seed_delta.descriptor != output_desc {
        return Err(IvmRuntimeError::GraphOutputMismatch);
    }
    let seed_frontier = recursive_state.accept_positive(seed_delta.deltas)?;
    if std::env::var_os("JAZZ_CLOSURE_TRACE").is_some() {
        eprintln!(
            "CLOSURE_TRACE event=recursive_positive node={node:?} scope={:?} seed_delta={} seed_frontier={} has_table_delta={has_table_delta} has_binding_deltas={has_binding_deltas}",
            runtime.scope,
            seed_delta_count,
            seed_frontier.len(),
        );
    }
    emitted.extend(seed_frontier.clone());

    let mut frontier = if has_table_delta {
        // Table-side positive deltas must probe the existing recursive closure
        // as well as any newly accepted seed rows. Step arrangements usually
        // provide that old closure, but maintained routed graphs can have
        // sibling recursive nodes whose arrangements are not populated on this
        // exact path. Feeding the accumulated set is conservative: duplicate
        // derivations are filtered by `accept_positive`.
        RecordDeltas {
            descriptor: output_desc,
            deltas: recursive_state.accumulated_deltas(),
        }
    } else {
        RecordDeltas {
            descriptor: output_desc,
            deltas: seed_frontier,
        }
    };
    let mut sub_tick = 1;
    let mut must_run_step = true;
    loop {
        if sub_tick > recursive.max_iters {
            return Err(IvmRuntimeError::RecursiveIterationLimit {
                node,
                max_iters: recursive.max_iters,
            });
        }
        frontier.deltas = consolidate_deltas(frontier.deltas);
        if frontier.is_empty() && !must_run_step {
            break;
        }
        must_run_step = false;
        let step_delta = runtime
            .eval_with_binding(sub_tick as u64, recursive.frontier.clone(), frontier, step)
            .await?;
        if step_delta.descriptor != output_desc {
            return Err(IvmRuntimeError::GraphOutputMismatch);
        }
        let accepted = recursive_state.accept_positive(step_delta.deltas)?;
        if accepted.is_empty() {
            break;
        }
        emitted.extend(accepted.clone());
        frontier = RecordDeltas {
            descriptor: output_desc,
            deltas: accepted,
        };
        sub_tick += 1;
    }

    Ok(RecursiveDeltaProgress::Ready(consolidate_deltas(emitted)))
}

fn has_table_delta_for_cached_tables(
    runtime: &GraphRuntimeView<'_>,
    recursive: &RecursiveOp,
) -> bool {
    runtime
        .table_deltas
        .iter()
        .any(|table_delta| recursive.read_tables.contains(&table_delta.table))
}

fn has_recompute_table_delta_for_recursion(
    runtime: &GraphRuntimeView<'_>,
    seed: NodeId,
    step: NodeId,
) -> Result<bool, IvmRuntimeError> {
    has_recompute_table_delta(runtime.graph, runtime.table_deltas, seed, step)
}

fn has_recompute_table_delta(
    graph: &IvmGraph,
    table_deltas: &[TableDelta],
    seed: NodeId,
    step: NodeId,
) -> Result<bool, IvmRuntimeError> {
    let mut tables = HashMap::<String, RecordDescriptor>::default();
    collect_table_source_names(graph, seed, &mut tables)?;
    collect_table_source_names(graph, step, &mut tables)?;
    let mut anti_join_right_tables = HashMap::<String, RecordDescriptor>::default();
    collect_anti_join_right_table_sources(graph, seed, &mut anti_join_right_tables)?;
    collect_anti_join_right_table_sources(graph, step, &mut anti_join_right_tables)?;
    Ok(table_deltas
        .iter()
        .filter(|table_delta| tables.contains_key(&table_delta.table))
        .any(|table_delta| {
            anti_join_right_tables.contains_key(&table_delta.table)
                || table_delta.deltas.iter().any(|delta| delta.weight <= 0)
        }))
}

fn has_recompute_binding_delta_for_recursion(
    runtime: &GraphRuntimeView<'_>,
    seed: NodeId,
    step: NodeId,
) -> Result<bool, IvmRuntimeError> {
    has_recompute_binding_delta(runtime.graph, runtime.binding_deltas, seed, step)
}

fn has_recompute_binding_delta(
    graph: &IvmGraph,
    binding_deltas: &[BindingDelta],
    seed: NodeId,
    step: NodeId,
) -> Result<bool, IvmRuntimeError> {
    let mut shapes = HashMap::<String, RecordDescriptor>::default();
    collect_binding_sources(graph, seed, &mut shapes)?;
    collect_binding_sources(graph, step, &mut shapes)?;
    Ok(binding_deltas
        .iter()
        .filter(|binding_delta| shapes.contains_key(&binding_delta.shape))
        .any(|binding_delta| binding_delta.deltas.iter().any(|delta| delta.weight <= 0)))
}

pub(super) fn snapshot_requirement(
    graph: &IvmGraph,
    node: NodeId,
    seed: NodeId,
    step: NodeId,
    table_deltas: &[TableDelta],
    binding_deltas: &[BindingDelta],
    state: Option<&RecursiveState>,
) -> Result<Option<NodeId>, IvmRuntimeError> {
    let has_bindings = !binding_deltas.is_empty();
    let requires_recompute = has_recompute_table_delta(graph, table_deltas, seed, step)?
        || has_recompute_binding_delta(graph, binding_deltas, seed, step)?
        || (!has_bindings && state.is_none_or(RecursiveState::is_empty))
        || state.is_none_or(|state| !state.step_arrangements_hydrated());
    Ok(if requires_recompute {
        Some(node)
    } else if has_bindings {
        Some(seed)
    } else {
        None
    })
}

pub(super) fn require_snapshot_inputs(
    graph: &IvmGraph,
    inputs: &mut EvaluationInputs,
    root: NodeId,
) -> Result<(), IvmRuntimeError> {
    let mut tables = std::collections::HashSet::<TableSnapshotSource>::new();
    collect_table_sources(graph, root, &mut tables)?;
    let mut blocked = false;
    for source in tables {
        let request = NodeState::table_source_request(&TableSourceOp {
            table: source.table,
            scan: source.scan,
            variant_projection: None,
        })?;
        if let Some(request) = request {
            match inputs.rows(request) {
                Ok(_) => {}
                Err(IvmRuntimeError::EvaluationBlocked) => blocked = true,
                Err(error) => return Err(error),
            }
        }
    }
    if blocked {
        Err(IvmRuntimeError::EvaluationBlocked)
    } else {
        Ok(())
    }
}

pub(super) async fn hydrate_recursive_arrangements(
    runtime: &mut GraphRuntimeView<'_>,
    recursive: &RecursiveOp,
    step: NodeId,
    accumulated: RecordDeltas,
) -> Result<(), IvmRuntimeError> {
    // Evaluate the step once against snapshot table deltas and the full
    // accumulated relation. The result is discarded; the purpose is to prepare
    // shared arrangements so later positive ticks can probe old state.
    let full_table_deltas = if runtime.evaluation_inputs.is_some() {
        Vec::new()
    } else {
        snapshot_table_deltas(runtime.schema, runtime.graph, runtime.storage, step).await?
    };
    if std::env::var_os("JAZZ_CLOSURE_TRACE").is_some() {
        let records = full_table_deltas
            .iter()
            .map(|delta| delta.deltas.len())
            .sum::<usize>();
        let tables = full_table_deltas
            .iter()
            .map(|delta| format!("{}:{}", delta.table, delta.deltas.len()))
            .collect::<Vec<_>>()
            .join(",");
        eprintln!(
            "CLOSURE_TRACE event=hydrate_recursive_arrangements step={step:?} tables={} records={} accumulated={}",
            tables,
            records,
            accumulated.deltas.len(),
        );
    }
    runtime
        .eval_with_binding_and_table_deltas(
            &full_table_deltas,
            0,
            recursive.frontier.clone(),
            accumulated,
            step,
        )
        .await?;
    runtime.clear_operator_state_for_scope();
    Ok(())
}

pub(super) async fn snapshot_table_deltas(
    schema: &crate::schema::DatabaseSchema,
    graph: &IvmGraph,
    storage: &dyn OrderedKvStorage,
    root: NodeId,
) -> Result<Vec<TableDelta>, IvmRuntimeError> {
    let mut tables = std::collections::HashSet::<TableSnapshotSource>::new();
    collect_table_sources(graph, root, &mut tables)?;
    let mut output = Vec::new();
    for source in tables {
        let table_schema = schema
            .table(&source.table)
            .ok_or_else(|| IvmRuntimeError::TableNotFound(source.table.clone()))?;
        let storage_descriptor = table_schema.record_schema();
        let store = super::record_store_for_table(storage, table_schema, &storage_descriptor);
        let mut cursor = match &source.scan {
            None => Some(store.scan(ScanBounds::Prefix(Vec::new())).await?),
            Some(scan) => match scan_bounds(scan)? {
                StaticScanBounds::Prefix(prefix) => {
                    Some(store.scan(ScanBounds::Prefix(prefix)).await?)
                }
                StaticScanBounds::Range { start, end } if start < end => {
                    Some(store.scan(ScanBounds::Range { start, end }).await?)
                }
                StaticScanBounds::Range { .. } => None,
            },
        };
        let mut stored_records = Vec::new();
        if let Some(cursor) = &mut cursor {
            while let Some(batch) = cursor.next_batch().await? {
                stored_records.extend(batch.into_iter().map(|(_, record)| record));
            }
        }
        let mut by_variant = HashMap::<(u32, RecordDescriptor), Vec<RecordDelta>>::default();
        for stored in stored_records {
            let (variant_tag, payload) = crate::records::split_variant_record(&stored)?;
            let descriptor = table_schema
                .record_schema_for_variant(variant_tag)
                .ok_or_else(|| IvmRuntimeError::UnknownTableVariant {
                    table: source.table.clone(),
                    version: u64::from(variant_tag),
                })?;
            by_variant
                .entry((variant_tag, descriptor))
                .or_default()
                .push(RecordDelta {
                    record: Bytes::copy_from_slice(payload),
                    weight: 1,
                });
        }
        output.extend(
            by_variant
                .into_iter()
                .map(|((variant_tag, descriptor), deltas)| TableDelta {
                    table: source.table.clone(),
                    variant_tag,
                    descriptor,
                    deltas,
                }),
        );
    }
    Ok(output)
}

fn snapshot_table_deltas_from_inputs(
    schema: &crate::schema::DatabaseSchema,
    graph: &IvmGraph,
    inputs: &mut EvaluationInputs,
    root: NodeId,
) -> Result<Vec<TableDelta>, IvmRuntimeError> {
    let mut tables = std::collections::HashSet::<TableSnapshotSource>::new();
    collect_table_sources(graph, root, &mut tables)?;
    let tables = tables.into_iter().collect::<Vec<_>>();
    require_snapshot_inputs(graph, inputs, root)?;
    let mut output = Vec::new();
    for source in tables {
        let request = NodeState::table_source_request(&TableSourceOp {
            table: source.table.clone(),
            scan: source.scan.clone(),
            variant_projection: None,
        })?;
        let stored_records = match request {
            Some(request) => inputs
                .rows(request)?
                .iter()
                .map(|(_, record)| record.as_slice())
                .collect::<Vec<_>>(),
            None => Vec::new(),
        };
        let table_schema = schema
            .table(&source.table)
            .ok_or_else(|| IvmRuntimeError::TableNotFound(source.table.clone()))?;
        let mut by_variant = HashMap::<(u32, RecordDescriptor), Vec<RecordDelta>>::default();
        for stored in stored_records {
            let (variant_tag, payload) = crate::records::split_variant_record(stored)?;
            let descriptor = table_schema
                .record_schema_for_variant(variant_tag)
                .ok_or_else(|| IvmRuntimeError::UnknownTableVariant {
                    table: source.table.clone(),
                    version: u64::from(variant_tag),
                })?;
            by_variant
                .entry((variant_tag, descriptor))
                .or_default()
                .push(RecordDelta {
                    record: Bytes::copy_from_slice(payload),
                    weight: 1,
                });
        }
        output.extend(
            by_variant
                .into_iter()
                .map(|((variant_tag, descriptor), deltas)| TableDelta {
                    table: source.table.clone(),
                    variant_tag,
                    descriptor,
                    deltas,
                }),
        );
    }
    Ok(output)
}

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
struct TableSnapshotSource {
    table: String,
    scan: Option<StaticScanSpec>,
}

fn collect_table_sources(
    graph: &IvmGraph,
    node: NodeId,
    tables: &mut std::collections::HashSet<TableSnapshotSource>,
) -> Result<(), IvmRuntimeError> {
    walk_input_graph(graph, node, |_, graph_node| {
        if let OpType::TableSource(table) = &graph_node.descriptor.operator {
            tables.insert(TableSnapshotSource {
                table: table.table.clone(),
                scan: table.scan.clone(),
            });
        }
        Ok(())
    })
}

pub(super) fn recursive_read_tables(
    graph: &IvmGraph,
    seed: NodeId,
    step: NodeId,
) -> Result<Vec<String>, IvmRuntimeError> {
    let mut tables = HashMap::<String, RecordDescriptor>::default();
    collect_table_source_names(graph, seed, &mut tables)?;
    collect_table_source_names(graph, step, &mut tables)?;
    let mut tables = tables.into_keys().collect::<Vec<_>>();
    tables.sort();
    Ok(tables)
}

fn collect_table_source_names(
    graph: &IvmGraph,
    node: NodeId,
    tables: &mut HashMap<String, RecordDescriptor>,
) -> Result<(), IvmRuntimeError> {
    walk_input_graph(graph, node, |_, graph_node| {
        if let OpType::TableSource(table) = &graph_node.descriptor.operator {
            tables
                .entry(table.table.clone())
                .or_insert_with(|| graph_node.descriptor.output.records());
        } else if let OpType::IndexSource(index) = &graph_node.descriptor.operator {
            tables
                .entry(index.table.clone())
                .or_insert_with(|| graph_node.descriptor.output.records());
        }
        Ok(())
    })
}

fn collect_binding_sources(
    graph: &IvmGraph,
    node: NodeId,
    shapes: &mut HashMap<String, RecordDescriptor>,
) -> Result<(), IvmRuntimeError> {
    walk_input_graph(graph, node, |_, graph_node| {
        if let OpType::BindingSource(binding) = &graph_node.descriptor.operator {
            shapes
                .entry(binding.shape.clone())
                .or_insert_with(|| graph_node.descriptor.output.records());
        }
        Ok(())
    })
}

fn collect_anti_join_right_table_sources(
    graph: &IvmGraph,
    node: NodeId,
    tables: &mut HashMap<String, RecordDescriptor>,
) -> Result<(), IvmRuntimeError> {
    walk_input_graph(graph, node, |node, graph_node| {
        if matches!(&graph_node.descriptor.operator, OpType::AntiJoin(_)) {
            let right = graph_node
                .descriptor
                .inputs
                .get(1)
                .ok_or(IvmRuntimeError::GraphInputMissing(node))?;
            collect_table_source_names(graph, *right, tables)?;
        }
        Ok(())
    })
}

/// Walk an IVM graph's inputs without consuming the thread stack.
///
/// Compilation can produce a finite but very deep chain from recursive policy
/// lowering. These collectors run while installing recursive nodes, on the
/// server shell's normal stack. Visiting each node at most once also preserves
/// the collectors' set/map semantics for hash-consed shared subgraphs.
fn walk_input_graph(
    graph: &IvmGraph,
    root: NodeId,
    mut visit: impl FnMut(NodeId, &crate::ivm::GraphNode) -> Result<(), IvmRuntimeError>,
) -> Result<(), IvmRuntimeError> {
    let mut pending = vec![root];
    let mut seen = HashSet::default();
    while let Some(node) = pending.pop() {
        if !seen.insert(node) {
            continue;
        }
        let graph_node = graph
            .node(node)
            .ok_or(IvmRuntimeError::GraphNodeNotFound(node))?;
        visit(node, graph_node)?;
        // Reverse push retains the recursive walk's left-to-right input order.
        pending.extend(graph_node.descriptor.inputs.iter().rev().copied());
    }
    Ok(())
}

fn accept_positive_into_set(
    multiset: &mut HashMap<Bytes, i64>,
    deltas: Vec<RecordDelta>,
) -> Result<Vec<RecordDelta>, IvmRuntimeError> {
    // Recompute must match the incremental regime above: recursive SELECTs are
    // currently monotone set fixed points, not UNION ALL bag fixed points.
    reject_non_positive_frontier_deltas(&deltas)?;
    let mut accepted = Vec::new();
    for delta in consolidate_deltas(deltas) {
        if delta.weight <= 0 {
            return Err(IvmRuntimeError::UnsupportedNonMonotoneRecursion);
        }
        if multiset.contains_key(&delta.record) {
            continue;
        }
        multiset.insert(delta.record.clone(), 1);
        accepted.push(RecordDelta {
            record: delta.record,
            weight: 1,
        });
    }
    Ok(consolidate_deltas(accepted))
}

fn reject_non_positive_frontier_deltas(deltas: &[RecordDelta]) -> Result<(), IvmRuntimeError> {
    if deltas.iter().any(|delta| delta.weight <= 0) {
        return Err(IvmRuntimeError::UnsupportedNonMonotoneRecursion);
    }
    Ok(())
}

/// Full-snapshot evaluator used by recursive recompute fallback.
struct HydrationEvaluator<'a> {
    schema: &'a crate::schema::DatabaseSchema,
    graph: &'a IvmGraph,
    variant_projections: &'a HashMap<VariantProjectionKey, VariantProjection>,
    table_deltas: Option<&'a [TableDelta]>,
    evaluation_inputs: Option<&'a mut EvaluationInputs>,
    storage: &'a dyn OrderedKvStorage,
    binding_snapshots: &'a HashMap<String, RecordDeltas>,
    context: EvalContext,
    memo: HashMap<NodeId, RecordDeltas>,
}

impl HydrationEvaluator<'_> {
    fn eval_node(
        &mut self,
        node: NodeId,
    ) -> StorageFuture<'_, Result<RecordDeltas, IvmRuntimeError>> {
        Box::pin(async move {
            if let Some(records) = self.memo.get(&node) {
                return Ok(records.clone());
            }
            let graph_node = self
                .graph
                .node(node)
                .ok_or(IvmRuntimeError::GraphNodeNotFound(node))?;
            let output_desc = graph_node.descriptor.output.records();
            match &graph_node.descriptor.operator {
                OpType::TableSource(table) => match self.evaluation_inputs.as_deref_mut() {
                    Some(inputs) => NodeState::update_table_source_from_inputs(
                        table,
                        self.schema,
                        self.variant_projections,
                        &output_desc,
                        inputs,
                    ),
                    None => match self.table_deltas {
                        Some(table_deltas) => NodeState::update_table_source(
                            table,
                            self.schema,
                            self.variant_projections,
                            &output_desc,
                            table_deltas,
                        ),
                        None => self.eval_table_source(table, output_desc).await,
                    },
                },
                OpType::IndexSource(index) => match self.evaluation_inputs.as_deref_mut() {
                    Some(inputs) => super::NodeState::update_index_source_from_inputs(
                        index,
                        self.schema,
                        self.variant_projections,
                        &output_desc,
                        inputs,
                    ),
                    None => {
                        super::NodeState::update_index_source(
                            index,
                            self.schema,
                            self.variant_projections,
                            &output_desc,
                            &[],
                            Some(self.storage),
                            super::EvalMode::Hydrate,
                        )
                        .await
                    }
                },
                OpType::InlineRecords(inline) => Ok(RecordDeltas {
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
                }),
                OpType::FrontierSource(frontier_source) => {
                    let deltas = self
                        .context
                        .bindings
                        .get(&frontier_source.binding)
                        .cloned()
                        .unwrap_or_else(|| RecordDeltas::empty(output_desc));
                    if deltas.descriptor != output_desc {
                        return Err(IvmRuntimeError::GraphOutputMismatch);
                    }
                    Ok(deltas)
                }
                OpType::BindingSource(binding_source) => {
                    let deltas = self
                        .binding_snapshots
                        .get(&binding_source.shape)
                        .cloned()
                        .unwrap_or_else(|| RecordDeltas::empty(output_desc));
                    project_binding_source_deltas(&deltas, &output_desc)
                }
                OpType::Arrange(_) => self.eval_unary_input(graph_node, node).await,
                OpType::Filter(filter) => {
                    let input = self.eval_unary_input(graph_node, node).await?;
                    NodeState::update_filter(filter, output_desc, &input)
                }
                OpType::MapProject(project) => {
                    let input = self.eval_unary_input(graph_node, node).await?;
                    let result =
                        NodeState::update_map_project(project, output_desc, &input, None, false);
                    #[cfg(feature = "cold-settle-attribution")]
                    if let Ok(output) = &result {
                        crate::cold_settle_attribution::record_map(
                            true,
                            self.depends_on_dominant_child(node)?,
                            input.deltas.len(),
                            output.deltas.len(),
                        );
                    }
                    result
                }
                OpType::UnwrapNullable(unwrap) => {
                    let input = self.eval_unary_input(graph_node, node).await?;
                    NodeState::update_unwrap_nullable(unwrap, output_desc, &input)
                }
                OpType::Unnest(unnest) => {
                    let input = self.eval_unary_input(graph_node, node).await?;
                    NodeState::update_unnest(unnest, output_desc, &input)
                }
                OpType::VariantProject(variant_project) => {
                    let input = self.eval_unary_input(graph_node, node).await?;
                    NodeState::update_variant_project(variant_project, output_desc, &input)
                }
                OpType::ArgMaxBy(arg_max_by) => {
                    let input = self.eval_unary_input(graph_node, node).await?;
                    let mut winners =
                        std::collections::BTreeMap::<Vec<u8>, (Vec<u8>, Bytes)>::new();
                    for delta in input.deltas {
                        let group_key = super::encoded_record_key_part(
                            output_desc,
                            delta.raw(),
                            &arg_max_by.group_field_indices,
                        )?;
                        let primary_key = super::encoded_record_key_part(
                            output_desc,
                            delta.raw(),
                            &arg_max_by.primary_key_field_indices,
                        )?;
                        let entry = winners
                            .entry(group_key)
                            .or_insert_with(|| (primary_key.clone(), delta.record.clone()));
                        if primary_key > entry.0 {
                            *entry = (primary_key, delta.record);
                        }
                    }
                    Ok(RecordDeltas {
                        descriptor: output_desc,
                        deltas: winners
                            .into_values()
                            .map(|(_, record)| RecordDelta { record, weight: 1 })
                            .collect(),
                    })
                }
                OpType::ArgMinBy(arg_min_by) => {
                    let input = self.eval_unary_input(graph_node, node).await?;
                    let mut winners =
                        std::collections::BTreeMap::<Vec<u8>, (Vec<u8>, Bytes)>::new();
                    for delta in input.deltas {
                        let group_key = super::encoded_record_key_part(
                            output_desc,
                            delta.raw(),
                            &arg_min_by.group_field_indices,
                        )?;
                        let primary_key = super::encoded_record_key_part(
                            output_desc,
                            delta.raw(),
                            &arg_min_by.primary_key_field_indices,
                        )?;
                        let entry = winners
                            .entry(group_key)
                            .or_insert_with(|| (primary_key.clone(), delta.record.clone()));
                        if primary_key < entry.0 {
                            *entry = (primary_key, delta.record);
                        }
                    }
                    Ok(RecordDeltas {
                        descriptor: output_desc,
                        deltas: winners
                            .into_values()
                            .map(|(_, record)| RecordDelta { record, weight: 1 })
                            .collect(),
                    })
                }
                OpType::Union => {
                    let input_nodes = graph_node.descriptor.inputs.clone();
                    let mut inputs = Vec::with_capacity(input_nodes.len());
                    for input in input_nodes {
                        inputs.push(self.eval_node(input).await?);
                    }
                    NodeState::update_union(
                        output_desc,
                        inputs.into_iter().map(std::sync::Arc::new).collect(),
                    )
                }
                OpType::IndexBy(index_by) => {
                    let input = self.eval_unary_input(graph_node, node).await?;
                    NodeState::update_index_by(index_by, output_desc, &input)
                }
                OpType::Join(join) => {
                    let [left, right] = graph_node.descriptor.inputs.as_slice() else {
                        return Err(IvmRuntimeError::GraphInputArityMismatch(node));
                    };
                    let left = self.eval_node(*left).await?;
                    let right = self.eval_node(*right).await?;
                    let left_on = plan_expr_names(&join.left_key);
                    let right_on = plan_expr_names(&join.right_key);
                    let mut right_by_key =
                        std::collections::BTreeMap::<super::join::JoinKey, Vec<&RecordDelta>>::new(
                        );
                    for right_delta in &right.deltas {
                        for key in super::join::join_keys(
                            &join.right_descriptor,
                            right_delta.raw(),
                            &right_on,
                        )? {
                            right_by_key.entry(key).or_default().push(right_delta);
                        }
                    }
                    let mut deltas = Vec::new();
                    for left_delta in &left.deltas {
                        for key in super::join::join_keys(
                            &join.left_descriptor,
                            left_delta.raw(),
                            &left_on,
                        )? {
                            let Some(matches) = right_by_key.get(&key) else {
                                continue;
                            };
                            for right_delta in matches {
                                deltas.push(RecordDelta {
                                    record: super::join::create_join_record(
                                        &join.left_descriptor,
                                        left_delta.raw(),
                                        &join.right_descriptor,
                                        right_delta.raw(),
                                        &output_desc,
                                    )?
                                    .into(),
                                    weight: left_delta.weight * right_delta.weight,
                                });
                            }
                        }
                    }
                    #[cfg(feature = "cold-settle-attribution")]
                    crate::cold_settle_attribution::record_join(
                        true,
                        self.depends_on_dominant_child(node)?,
                        left.deltas.len(),
                        right.deltas.len(),
                        deltas.len(),
                    );
                    Ok(RecordDeltas {
                        descriptor: output_desc,
                        deltas,
                    })
                }
                OpType::TopBy(_) | OpType::CollectBy(_) => {
                    Err(IvmRuntimeError::UnsupportedOperator)
                }
                OpType::AntiJoin(join) => {
                    let [left, right] = graph_node.descriptor.inputs.as_slice() else {
                        return Err(IvmRuntimeError::GraphInputArityMismatch(node));
                    };
                    let left = self.eval_node(*left).await?;
                    let right = self.eval_node(*right).await?;
                    let join_state = super::join::AntiJoinState;
                    let left_on = plan_expr_names(&join.left_key);
                    let right_on = plan_expr_names(&join.right_key);
                    let mut left_arrangement = AsOf::new(super::join::ArrangementState::default());
                    let mut right_arrangement = AsOf::new(super::join::ArrangementState::default());
                    let deltas = join_state.apply(
                        &mut left_arrangement,
                        &mut right_arrangement,
                        &join.left_descriptor,
                        &join.right_descriptor,
                        &output_desc,
                        &left_on,
                        &right_on,
                        join.comparison,
                        &left.deltas,
                        &right.deltas,
                        SubTick {
                            tick: 0,
                            sub_tick: 0,
                        },
                        SubTick {
                            tick: 0,
                            sub_tick: 0,
                        },
                        ArrangementUpdateMode::Accumulate,
                    )?;
                    #[cfg(feature = "cold-settle-attribution")]
                    crate::cold_settle_attribution::record_join(
                        true,
                        self.depends_on_dominant_child(node)?,
                        left.deltas.len(),
                        right.deltas.len(),
                        deltas.len(),
                    );
                    Ok(RecordDeltas {
                        descriptor: output_desc,
                        deltas,
                    })
                }
                // A nested recursive graph needs its own persisted frontier
                // state. v0 deliberately rejects it rather than falling back
                // to an unbounded private DFS inside this traversal.
                OpType::Recursive(_) => Err(IvmRuntimeError::UnsupportedNestedRecursion),
                OpType::Persist(_)
                | OpType::StreamingChecksum(_)
                | OpType::Distinct
                | OpType::Negate => Err(IvmRuntimeError::UnsupportedOperator),
                OpType::SemiJoin(_) | OpType::Aggregate(_) => {
                    Err(IvmRuntimeError::UnsupportedOperator)
                }
            }
        })
    }

    async fn eval_table_source(
        &self,
        table: &TableSourceOp,
        output_desc: RecordDescriptor,
    ) -> Result<RecordDeltas, IvmRuntimeError> {
        let table_schema = self
            .schema
            .table(&table.table)
            .ok_or_else(|| IvmRuntimeError::TableNotFound(table.table.clone()))?;
        let storage_descriptor = table_schema.record_schema();
        let store = super::record_store_for_table(self.storage, table_schema, &storage_descriptor);
        let mut scan = store.scan(ScanBounds::Prefix(Vec::new())).await?;
        let mut stored_records = Vec::<Vec<u8>>::new();
        while let Some(batch) = scan.next_batch().await? {
            stored_records.extend(batch.into_iter().map(|(_, record)| record));
        }
        let mut grouped = HashMap::<(u32, RecordDescriptor), Vec<RecordDelta>>::default();
        for stored in stored_records {
            let (variant_tag, payload) = crate::records::split_variant_record(&stored)?;
            let descriptor = table_schema
                .record_schema_for_variant(variant_tag)
                .ok_or_else(|| IvmRuntimeError::UnknownTableVariant {
                    table: table.table.clone(),
                    version: u64::from(variant_tag),
                })?;
            grouped
                .entry((variant_tag, descriptor))
                .or_default()
                .push(RecordDelta {
                    record: Bytes::copy_from_slice(payload),
                    weight: 1,
                });
        }
        let table_deltas = grouped
            .into_iter()
            .map(|((variant_tag, descriptor), deltas)| TableDelta {
                table: table.table.clone(),
                variant_tag,
                descriptor,
                deltas,
            })
            .collect::<Vec<_>>();
        NodeState::update_table_source(
            table,
            self.schema,
            self.variant_projections,
            &output_desc,
            &table_deltas,
        )
    }

    async fn eval_unary_input(
        &mut self,
        graph_node: &crate::ivm::GraphNode,
        node: NodeId,
    ) -> Result<RecordDeltas, IvmRuntimeError> {
        let input = *graph_node
            .descriptor
            .inputs
            .first()
            .ok_or(IvmRuntimeError::GraphInputMissing(node))?;
        self.eval_node(input).await
    }

    #[cfg(feature = "cold-settle-attribution")]
    fn depends_on_dominant_child(&self, node: NodeId) -> Result<bool, IvmRuntimeError> {
        let graph_node = self
            .graph
            .node(node)
            .ok_or(IvmRuntimeError::GraphNodeNotFound(node))?;
        if matches!(
            &graph_node.descriptor.operator,
            OpType::TableSource(source) if source.table == "res_l_child_3"
        ) || ["parent_id", "value_text", "value_json"]
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
}

#[cfg(test)]
mod tests {
    use super::*;

    fn delta(record: &[u8], weight: i64) -> RecordDelta {
        RecordDelta {
            record: record.to_vec().into(),
            weight,
        }
    }

    #[test]
    fn accept_positive_rejects_raw_non_positive_frontier_deltas_before_consolidation() {
        let mut state = RecursiveState::default();

        assert!(matches!(
            state.accept_positive(vec![delta(b"zero", 0)]),
            Err(IvmRuntimeError::UnsupportedNonMonotoneRecursion)
        ));
        assert!(matches!(
            state.accept_positive(vec![delta(b"net-zero", 1), delta(b"net-zero", -1)]),
            Err(IvmRuntimeError::UnsupportedNonMonotoneRecursion)
        ));
    }

    #[test]
    fn accept_positive_into_set_rejects_raw_non_positive_frontier_deltas_before_consolidation() {
        let mut accumulated = HashMap::default();

        assert!(matches!(
            accept_positive_into_set(&mut accumulated, vec![delta(b"zero", 0)]),
            Err(IvmRuntimeError::UnsupportedNonMonotoneRecursion)
        ));
        assert!(matches!(
            accept_positive_into_set(
                &mut accumulated,
                vec![delta(b"net-zero", 1), delta(b"net-zero", -1)]
            ),
            Err(IvmRuntimeError::UnsupportedNonMonotoneRecursion)
        ));
    }

    #[test]
    fn recursive_snapshot_clone_shares_payload_until_first_write() {
        let mut original = RecursiveState::default();
        original
            .accept_positive(vec![delta(b"original", 1)])
            .unwrap();
        let mut prepared = original.clone();
        assert!(Rc::ptr_eq(&original.accumulated, &prepared.accumulated));

        prepared
            .accept_positive(vec![delta(b"prepared", 1)])
            .unwrap();
        assert!(!Rc::ptr_eq(&original.accumulated, &prepared.accumulated));
        assert_eq!(original.accumulated_row_count(), 1);
        assert_eq!(prepared.accumulated_row_count(), 2);
    }
}
