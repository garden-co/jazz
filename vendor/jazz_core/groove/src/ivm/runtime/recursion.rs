//! Recursive operator state and scoped fixpoint evaluation.
//!
//! This module owns [`RecursiveState`], recursive frontier handling, step
//! arrangement hydration, and the bounded positive-recursion loop used by the
//! runtime. It reuses the main graph evaluator under recursive scopes rather
//! than defining separate operators. Join arrangements live in [`super::join`];
//! public ticks, subscriptions, and graph retention live in [`super`].

use std::collections::HashMap;

use crate::ivm::{IvmGraph, NodeId, OpType, RecursiveOp, TableSourceOp};
use crate::records::RecordDescriptor;
use crate::storage::{OrderedKvStorage, RecordStore};

use super::{
    ArrangementUpdateMode, AsOf, EvalContext, GraphRuntimeView, IvmRuntimeError, NodeState,
    RecordDelta, RecordDeltas, ScopePath, SubTick, TableDelta, consolidate_deltas, plan_expr_names,
};

#[derive(Clone, Debug, Default)]
pub(super) struct RecursiveState {
    /// Current recursive result as an encoded-record multiset.
    /// For now recursive outputs are set-style: each reachable record is kept
    /// at weight 1. Bag recursion can diverge on cycles, and non-monotone
    /// recursion needs a DRed/DBSP design before we accept negative frontiers.
    accumulated: HashMap<Vec<u8>, i64>,
    /// Positive incremental ticks rely on step-side arrangements already
    /// containing the full base/accumulated state after a recompute.
    step_arrangements_hydrated: bool,
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
        self.accumulated.keys().map(Vec::len).sum()
    }

    pub(super) fn mark_step_arrangements_hydrated(&mut self) {
        self.step_arrangements_hydrated = true;
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

    pub(super) fn accept_positive(
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
            self.accumulated.insert(delta.record.clone(), 1);
            accepted.push(RecordDelta {
                record: delta.record,
                weight: 1,
            });
        }
        Ok(consolidate_deltas(accepted))
    }

    pub(super) fn replace_with(&mut self, next: HashMap<Vec<u8>, i64>) -> Vec<RecordDelta> {
        let mut deltas = Vec::new();
        for (record, old_weight) in &self.accumulated {
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
        self.accumulated = next;
        self.step_arrangements_hydrated = false;
        consolidate_deltas(deltas)
    }
}

pub(super) fn recursive_delta<S>(
    recursive_state: &mut RecursiveState,
    mut runtime: GraphRuntimeView<'_, S>,
    node: NodeId,
    recursive: &RecursiveOp,
    output_desc: RecordDescriptor,
    seed: NodeId,
    step: NodeId,
) -> Result<Vec<RecordDelta>, IvmRuntimeError>
where
    S: OrderedKvStorage,
{
    let has_recompute_table_delta = has_recompute_table_delta_for_recursion(&runtime, seed, step)?;
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
        runtime.metrics.recursive_recomputes += 1;
        let next = recompute_recursive(
            runtime.graph,
            node,
            recursive,
            output_desc,
            step,
            runtime.storage,
            runtime.binding_snapshots,
            runtime.current_tick,
            runtime.scope.clone(),
        )?;
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
        hydrate_recursive_arrangements(&mut runtime, recursive, step, accumulated)?;
        recursive_state.mark_step_arrangements_hydrated();
        return Ok(emitted);
    }

    let mut emitted = Vec::new();
    let seed_delta = runtime.eval_root(seed)?;
    if seed_delta.descriptor != output_desc {
        return Err(IvmRuntimeError::GraphOutputMismatch);
    }
    let seed_frontier = recursive_state.accept_positive(seed_delta.deltas)?;
    emitted.extend(seed_frontier.clone());

    let mut frontier = RecordDeltas {
        descriptor: output_desc,
        deltas: seed_frontier,
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
        let step_delta = runtime.eval_with_binding(
            sub_tick as u64,
            recursive.frontier.clone(),
            frontier,
            step,
        )?;
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

    Ok(consolidate_deltas(emitted))
}

fn has_recompute_table_delta_for_recursion<S>(
    runtime: &GraphRuntimeView<'_, S>,
    seed: NodeId,
    step: NodeId,
) -> Result<bool, IvmRuntimeError>
where
    S: OrderedKvStorage,
{
    let mut tables = HashMap::<String, RecordDescriptor>::new();
    collect_table_sources(runtime.graph, seed, &mut tables)?;
    collect_table_sources(runtime.graph, step, &mut tables)?;
    let mut anti_join_right_tables = HashMap::<String, RecordDescriptor>::new();
    collect_anti_join_right_table_sources(runtime.graph, seed, &mut anti_join_right_tables)?;
    collect_anti_join_right_table_sources(runtime.graph, step, &mut anti_join_right_tables)?;
    Ok(runtime
        .table_deltas
        .iter()
        .filter(|table_delta| tables.contains_key(&table_delta.table))
        .any(|table_delta| {
            anti_join_right_tables.contains_key(&table_delta.table)
                || table_delta.deltas.iter().any(|delta| delta.weight <= 0)
        }))
}

fn has_recompute_binding_delta_for_recursion<S>(
    runtime: &GraphRuntimeView<'_, S>,
    seed: NodeId,
    step: NodeId,
) -> Result<bool, IvmRuntimeError>
where
    S: OrderedKvStorage,
{
    let mut shapes = HashMap::<String, RecordDescriptor>::new();
    collect_binding_sources(runtime.graph, seed, &mut shapes)?;
    collect_binding_sources(runtime.graph, step, &mut shapes)?;
    Ok(runtime
        .binding_deltas
        .iter()
        .filter(|binding_delta| shapes.contains_key(&binding_delta.shape))
        .any(|binding_delta| binding_delta.deltas.iter().any(|delta| delta.weight <= 0)))
}

pub(super) fn hydrate_recursive_arrangements<S>(
    runtime: &mut GraphRuntimeView<'_, S>,
    recursive: &RecursiveOp,
    step: NodeId,
    accumulated: RecordDeltas,
) -> Result<(), IvmRuntimeError>
where
    S: OrderedKvStorage,
{
    if accumulated.is_empty() {
        return Ok(());
    }
    // Evaluate the step once against snapshot table deltas and the full
    // accumulated relation. The result is discarded; the purpose is to prepare
    // shared arrangements so later positive ticks can probe old state.
    let full_table_deltas = snapshot_table_deltas(runtime.graph, runtime.storage, step)?;
    runtime.eval_with_binding_and_table_deltas(
        &full_table_deltas,
        0,
        recursive.frontier.clone(),
        accumulated,
        step,
    )?;
    Ok(())
}

pub(super) fn snapshot_table_deltas(
    graph: &IvmGraph,
    storage: &impl OrderedKvStorage,
    root: NodeId,
) -> Result<Vec<TableDelta>, IvmRuntimeError> {
    let mut tables = HashMap::<String, RecordDescriptor>::new();
    collect_table_sources(graph, root, &mut tables)?;
    tables
        .into_iter()
        .map(|(table, descriptor)| {
            let store = RecordStore::new(storage, &table, &descriptor);
            let mut deltas = Vec::new();
            store.scan_prefix(b"", &mut |_, record| {
                deltas.push(RecordDelta {
                    record: record.to_vec(),
                    weight: 1,
                });
                Ok(())
            })?;
            Ok(TableDelta {
                table,
                descriptor,
                deltas,
            })
        })
        .collect()
}

fn collect_table_sources(
    graph: &IvmGraph,
    node: NodeId,
    tables: &mut HashMap<String, RecordDescriptor>,
) -> Result<(), IvmRuntimeError> {
    let graph_node = graph
        .node(node)
        .ok_or(IvmRuntimeError::GraphNodeNotFound(node))?;
    if let OpType::TableSource(table) = &graph_node.descriptor.operator {
        tables
            .entry(table.table.clone())
            .or_insert_with(|| graph_node.descriptor.output);
    }
    for input in &graph_node.descriptor.inputs {
        collect_table_sources(graph, *input, tables)?;
    }
    Ok(())
}

fn collect_binding_sources(
    graph: &IvmGraph,
    node: NodeId,
    shapes: &mut HashMap<String, RecordDescriptor>,
) -> Result<(), IvmRuntimeError> {
    let graph_node = graph
        .node(node)
        .ok_or(IvmRuntimeError::GraphNodeNotFound(node))?;
    if let OpType::BindingSource(binding) = &graph_node.descriptor.operator {
        shapes
            .entry(binding.shape.clone())
            .or_insert_with(|| graph_node.descriptor.output);
    }
    for input in &graph_node.descriptor.inputs {
        collect_binding_sources(graph, *input, shapes)?;
    }
    Ok(())
}

fn collect_anti_join_right_table_sources(
    graph: &IvmGraph,
    node: NodeId,
    tables: &mut HashMap<String, RecordDescriptor>,
) -> Result<(), IvmRuntimeError> {
    let graph_node = graph
        .node(node)
        .ok_or(IvmRuntimeError::GraphNodeNotFound(node))?;
    if matches!(&graph_node.descriptor.operator, OpType::AntiJoin(_)) {
        let right = graph_node
            .descriptor
            .inputs
            .get(1)
            .ok_or(IvmRuntimeError::GraphInputMissing(node))?;
        collect_table_sources(graph, *right, tables)?;
    }
    for input in &graph_node.descriptor.inputs {
        collect_anti_join_right_table_sources(graph, *input, tables)?;
    }
    Ok(())
}

#[allow(clippy::too_many_arguments)]
pub(super) fn recompute_recursive(
    graph: &IvmGraph,
    node: NodeId,
    recursive: &RecursiveOp,
    output_desc: RecordDescriptor,
    step: NodeId,
    storage: &impl OrderedKvStorage,
    binding_snapshots: &HashMap<String, RecordDeltas>,
    _current_tick: u64,
    scope: ScopePath,
) -> Result<HashMap<Vec<u8>, i64>, IvmRuntimeError> {
    let recursive_node = graph
        .node(node)
        .ok_or(IvmRuntimeError::GraphNodeNotFound(node))?;
    let [seed, _] = recursive_node.descriptor.inputs.as_slice() else {
        return Err(IvmRuntimeError::GraphInputArityMismatch(recursive_node.id));
    };

    let mut snapshot = HydrationEvaluator {
        graph,
        storage,
        binding_snapshots,
        context: EvalContext::root(),
    };
    let mut accumulated = HashMap::<Vec<u8>, i64>::new();
    let mut frontier = snapshot.eval_node(*seed)?;
    if frontier.descriptor != output_desc {
        return Err(IvmRuntimeError::GraphOutputMismatch);
    }
    frontier.deltas = accept_positive_into_set(&mut accumulated, frontier.deltas)?;

    let mut sub_tick = 1;
    while !frontier.is_empty() {
        if sub_tick > recursive.max_iters {
            return Err(IvmRuntimeError::RecursiveIterationLimit {
                node: recursive_node.id,
                max_iters: recursive.max_iters,
            });
        }
        let context = EvalContext::with_binding(
            scope.clone(),
            sub_tick as u64,
            recursive.frontier.clone(),
            frontier,
        );
        let mut snapshot = HydrationEvaluator {
            graph,
            storage,
            binding_snapshots,
            context,
        };
        frontier = snapshot.eval_node(step)?;
        if frontier.descriptor != output_desc {
            return Err(IvmRuntimeError::GraphOutputMismatch);
        }
        frontier.deltas = accept_positive_into_set(&mut accumulated, frontier.deltas)?;
        sub_tick += 1;
    }

    Ok(accumulated)
}

fn accept_positive_into_set(
    multiset: &mut HashMap<Vec<u8>, i64>,
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
struct HydrationEvaluator<'a, S> {
    graph: &'a IvmGraph,
    storage: &'a S,
    binding_snapshots: &'a HashMap<String, RecordDeltas>,
    context: EvalContext,
}

impl<S> HydrationEvaluator<'_, S>
where
    S: OrderedKvStorage,
{
    fn eval_node(&mut self, node: NodeId) -> Result<RecordDeltas, IvmRuntimeError> {
        let graph_node = self
            .graph
            .node(node)
            .ok_or(IvmRuntimeError::GraphNodeNotFound(node))?;
        let output_desc = graph_node.descriptor.output;
        match &graph_node.descriptor.operator {
            OpType::TableSource(table) => self.eval_table_source(table, output_desc),
            OpType::InlineRecords(inline) => Ok(RecordDeltas {
                descriptor: output_desc,
                deltas: inline
                    .records
                    .iter()
                    .cloned()
                    .map(|record| RecordDelta { record, weight: 1 })
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
                if deltas.descriptor != output_desc {
                    return Err(IvmRuntimeError::GraphOutputMismatch);
                }
                Ok(deltas)
            }
            OpType::Filter(filter) => {
                let input = self.eval_unary_input(graph_node, node)?;
                NodeState::update_filter(filter, output_desc, input)
            }
            OpType::MapProject(project) => {
                let input = self.eval_unary_input(graph_node, node)?;
                NodeState::update_map_project(project, output_desc, input)
            }
            OpType::UnwrapNullable(unwrap) => {
                let input = self.eval_unary_input(graph_node, node)?;
                NodeState::update_unwrap_nullable(unwrap, output_desc, input)
            }
            OpType::ArgMaxBy(arg_max_by) => {
                let input = self.eval_unary_input(graph_node, node)?;
                let mut winners = std::collections::BTreeMap::<Vec<u8>, (Vec<u8>, Vec<u8>)>::new();
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
                let input = self.eval_unary_input(graph_node, node)?;
                let mut winners = std::collections::BTreeMap::<Vec<u8>, (Vec<u8>, Vec<u8>)>::new();
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
                let inputs = graph_node
                    .descriptor
                    .inputs
                    .iter()
                    .map(|input| self.eval_node(*input))
                    .collect::<Result<Vec<_>, _>>()?;
                NodeState::update_union(output_desc, inputs)
            }
            OpType::IndexBy(index_by) => {
                let input = self.eval_unary_input(graph_node, node)?;
                NodeState::update_index_by(index_by, output_desc, input)
            }
            OpType::Join(join) => {
                let [left, right] = graph_node.descriptor.inputs.as_slice() else {
                    return Err(IvmRuntimeError::GraphInputArityMismatch(node));
                };
                let left = self.eval_node(*left)?;
                let right = self.eval_node(*right)?;
                let left_on = plan_expr_names(&join.left_key);
                let right_on = plan_expr_names(&join.right_key);
                let mut right_by_key =
                    std::collections::BTreeMap::<Vec<u8>, Vec<&RecordDelta>>::new();
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
                                )?,
                                weight: left_delta.weight * right_delta.weight,
                            });
                        }
                    }
                }
                Ok(RecordDeltas {
                    descriptor: output_desc,
                    deltas,
                })
            }
            OpType::TopBy(_) => Err(IvmRuntimeError::UnsupportedOperator),
            OpType::AntiJoin(join) => {
                let [left, right] = graph_node.descriptor.inputs.as_slice() else {
                    return Err(IvmRuntimeError::GraphInputArityMismatch(node));
                };
                let left = self.eval_node(*left)?;
                let right = self.eval_node(*right)?;
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
                Ok(RecordDeltas {
                    descriptor: output_desc,
                    deltas,
                })
            }
            OpType::Recursive(recursive) => {
                let [_, step] = graph_node.descriptor.inputs.as_slice() else {
                    return Err(IvmRuntimeError::GraphInputArityMismatch(node));
                };
                let accumulated = recompute_recursive(
                    self.graph,
                    node,
                    recursive,
                    output_desc,
                    *step,
                    self.storage,
                    self.binding_snapshots,
                    0,
                    self.context.scope.child(node),
                )?;
                Ok(RecordDeltas {
                    descriptor: output_desc,
                    deltas: accumulated
                        .into_iter()
                        .map(|(record, weight)| RecordDelta { record, weight })
                        .collect(),
                })
            }
            OpType::Persist(_) | OpType::Distinct | OpType::Negate => {
                Err(IvmRuntimeError::UnsupportedOperator)
            }
            OpType::SemiJoin(_) | OpType::Aggregate(_) => Err(IvmRuntimeError::UnsupportedOperator),
        }
    }

    fn eval_table_source(
        &self,
        table: &TableSourceOp,
        output_desc: RecordDescriptor,
    ) -> Result<RecordDeltas, IvmRuntimeError> {
        let store = RecordStore::new(self.storage, &table.table, &output_desc);
        let mut deltas = Vec::new();
        store.scan_prefix(b"", &mut |_, record| {
            deltas.push(RecordDelta {
                record: record.to_vec(),
                weight: 1,
            });
            Ok(())
        })?;
        Ok(RecordDeltas {
            descriptor: output_desc,
            deltas,
        })
    }

    fn eval_unary_input(
        &mut self,
        graph_node: &crate::ivm::GraphNode,
        node: NodeId,
    ) -> Result<RecordDeltas, IvmRuntimeError> {
        let input = *graph_node
            .descriptor
            .inputs
            .first()
            .ok_or(IvmRuntimeError::GraphInputMissing(node))?;
        self.eval_node(input)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn delta(record: &[u8], weight: i64) -> RecordDelta {
        RecordDelta {
            record: record.to_vec(),
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
        let mut accumulated = HashMap::new();

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
}
