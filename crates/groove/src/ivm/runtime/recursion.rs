//! Recursive operator state and scoped fixpoint evaluation.
//!
//! This module owns [`RecursiveState`], recursive frontier handling, step
//! arrangement hydration, and the bounded positive-recursion loop used by the
//! runtime. It reuses the main graph evaluator under recursive scopes rather
//! than defining separate operators. Join arrangements live in [`super::join`];
//! public ticks, subscriptions, and graph retention live in [`super`].

use bytes::Bytes;
use rustc_hash::FxHashMap as HashMap;

use crate::ivm::{IvmGraph, NodeId, OpType, RecursiveOp, StaticScanSpec, TableSourceOp};
use crate::records::RecordDescriptor;
use crate::storage::OrderedKvStorage;

use super::{
    ArrangementUpdateMode, AsOf, EvalContext, GraphRuntimeView, IvmRuntimeError, NodeState,
    RecordDelta, RecordDeltas, ScopeId, StaticScanBounds, SubTick, TableDelta, consolidate_deltas,
    plan_expr_names, project_binding_source_deltas, scan_bounds,
};

/// The maintained state of one recursive (fixpoint) node.
///
/// `accumulated` is the fixpoint's current result — every row derived so far,
/// each at weight 1 (set semantics, so cycles converge instead of counting
/// support forever). `step_arrangements_hydrated` records whether the step
/// graph's join arrangements already hold the full accumulated relation, which
/// is the precondition for taking the cheap incremental path instead of a full
/// recompute.
#[derive(Clone, Debug, Default)]
pub(super) struct RecursiveState {
    /// Current recursive result as an encoded-record multiset.
    /// For now recursive outputs are set-style: each reachable record is kept
    /// at weight 1. Bag recursion can diverge on cycles, and non-monotone
    /// recursion needs a DRed/DBSP design before we accept negative frontiers.
    accumulated: HashMap<Bytes, i64>,
    /// Positive incremental ticks rely on step-side arrangements already
    /// containing the full base/accumulated state after a recompute.
    step_arrangements_hydrated: bool,
}

impl RecursiveState {
    /// `true` when nothing has been derived yet.
    pub(super) fn is_empty(&self) -> bool {
        self.accumulated.is_empty()
    }

    /// Whether the step arrangements hold the full accumulated relation
    /// (the incremental-path precondition).
    pub(super) fn step_arrangements_hydrated(&self) -> bool {
        self.step_arrangements_hydrated
    }

    /// Number of live accumulated rows (for stats).
    pub(super) fn accumulated_row_count(&self) -> usize {
        self.accumulated
            .values()
            .filter(|weight| **weight != 0)
            .count()
    }

    /// Encoded size of the accumulated relation (for stats).
    pub(super) fn accumulated_encoded_bytes(&self) -> usize {
        self.accumulated.keys().map(|record| record.len()).sum()
    }

    /// Marks the step arrangements as holding the full accumulated relation.
    pub(super) fn mark_step_arrangements_hydrated(&mut self) {
        self.step_arrangements_hydrated = true;
    }

    /// The whole accumulated relation as positive deltas — the "old closure"
    /// fed to a step when a table delta must probe it.
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

    /// Folds a frontier of newly derived rows into `accumulated`, returning
    /// only the rows that were genuinely new (the next frontier).
    ///
    /// Rows already present are dropped (set semantics), so a fixpoint over a
    /// cyclic graph terminates. Any non-positive weight — a retraction — is
    /// rejected as [`IvmRuntimeError::UnsupportedNonMonotoneRecursion`]: only
    /// monotone positive recursion is supported.
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

    /// Swaps in a freshly recomputed relation and returns the diff from the
    /// old one (retract vanished rows, insert new ones) so subscribers see
    /// the net change. Resets the hydration flag, since the step
    /// arrangements no longer match.
    pub(super) fn replace_with(&mut self, next: HashMap<Bytes, i64>) -> Vec<RecordDelta> {
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

/// Computes one tick's change to a fixpoint node.
///
/// * `recursive_state` — the node's maintained relation and hydration flag.
/// * `runtime` — borrowed evaluator state for running the seed/step graphs.
/// * `node` / `recursive` — the recursive node and its descriptor.
/// * `output_desc` — the fixpoint's row layout.
/// * `seed` / `step` — the seed and step input nodes.
///
/// Takes one of two paths. When something forces it — a retraction, a binding
/// removal, empty state, or un-hydrated step arrangements — it does a full
/// [`recompute_recursive`] and diffs the result. Otherwise it runs the
/// incremental loop: evaluate the seed's delta, then repeatedly run the step
/// over the growing frontier ([`RecursiveState::accept_positive`]) until no
/// new rows appear or `max_iters` is hit.
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
        runtime.metrics.recursive_recomputes += 1;
        if std::env::var_os("JAZZ_CLOSURE_TRACE").is_some() {
            eprintln!(
                "CLOSURE_TRACE event=recursive_recompute node={node:?} scope={:?} has_recompute_table_delta={has_recompute_table_delta} has_table_delta={has_table_delta} has_recompute_binding_delta={has_recompute_binding_delta} has_binding_deltas={has_binding_deltas} state_empty={} step_hydrated={} total_recomputes={}",
                runtime.scope,
                recursive_state.is_empty(),
                recursive_state.step_arrangements_hydrated(),
                runtime.metrics.recursive_recomputes,
            );
        }
        let next = recompute_recursive(
            runtime.schema,
            runtime.graph,
            node,
            recursive,
            output_desc,
            step,
            runtime.storage,
            runtime.binding_snapshots,
            runtime.current_tick,
            runtime.scope,
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
    let seed_delta = if has_binding_deltas {
        // A new binding can make existing seed-table rows visible even when no
        // table delta occurs in this tick. Evaluate the seed over a current
        // snapshot so binding-as-data opens produce their initial frontier
        // without forcing a full recursive recompute.
        let full_table_deltas =
            snapshot_table_deltas(runtime.schema, runtime.graph, runtime.storage, seed)?;
        runtime.eval_with_binding_and_table_deltas(
            &full_table_deltas,
            0,
            recursive.frontier.clone(),
            RecordDeltas::empty(output_desc),
            seed,
        )?
    } else {
        runtime.eval_root(seed)?
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

/// `true` when this tick changed any table the fixpoint reads (using the
/// table list cached on the descriptor).
fn has_table_delta_for_cached_tables<S>(
    runtime: &GraphRuntimeView<'_, S>,
    recursive: &RecursiveOp,
) -> bool
where
    S: OrderedKvStorage,
{
    runtime
        .table_deltas
        .iter()
        .any(|table_delta| recursive.read_tables.contains(&table_delta.table))
}

/// `true` when a table change forces a full recompute rather than an
/// incremental step: any retraction on a table the fixpoint reads, or *any*
/// change to a table on the right side of an anti join (where a new row can
/// remove existing results — non-monotone).
fn has_recompute_table_delta_for_recursion<S>(
    runtime: &GraphRuntimeView<'_, S>,
    seed: NodeId,
    step: NodeId,
) -> Result<bool, IvmRuntimeError>
where
    S: OrderedKvStorage,
{
    let mut tables = HashMap::<String, RecordDescriptor>::default();
    collect_table_source_names(runtime.graph, seed, &mut tables)?;
    collect_table_source_names(runtime.graph, step, &mut tables)?;
    let mut anti_join_right_tables = HashMap::<String, RecordDescriptor>::default();
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

/// `true` when a binding retraction touches a binding source the fixpoint
/// reads — removing a binding can drop rows, which the incremental path
/// cannot express, so a recompute is forced.
fn has_recompute_binding_delta_for_recursion<S>(
    runtime: &GraphRuntimeView<'_, S>,
    seed: NodeId,
    step: NodeId,
) -> Result<bool, IvmRuntimeError>
where
    S: OrderedKvStorage,
{
    let mut shapes = HashMap::<String, RecordDescriptor>::default();
    collect_binding_sources(runtime.graph, seed, &mut shapes)?;
    collect_binding_sources(runtime.graph, step, &mut shapes)?;
    Ok(runtime
        .binding_deltas
        .iter()
        .filter(|binding_delta| shapes.contains_key(&binding_delta.shape))
        .any(|binding_delta| binding_delta.deltas.iter().any(|delta| delta.weight <= 0)))
}

/// Primes the step graph's join arrangements with the full accumulated
/// relation, so later incremental ticks can probe the "old closure".
///
/// It runs the step once over a snapshot of its base tables plus the whole
/// accumulated frontier and throws the output away — the point is the side
/// effect of populating shared arrangements — then clears the scope's
/// per-iteration operator state.
pub(super) fn hydrate_recursive_arrangements<S>(
    runtime: &mut GraphRuntimeView<'_, S>,
    recursive: &RecursiveOp,
    step: NodeId,
    accumulated: RecordDeltas,
) -> Result<(), IvmRuntimeError>
where
    S: OrderedKvStorage,
{
    // Evaluate the step once against snapshot table deltas and the full
    // accumulated relation. The result is discarded; the purpose is to prepare
    // shared arrangements so later positive ticks can probe old state.
    let full_table_deltas =
        snapshot_table_deltas(runtime.schema, runtime.graph, runtime.storage, step)?;
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
    runtime.eval_with_binding_and_table_deltas(
        &full_table_deltas,
        0,
        recursive.frontier.clone(),
        accumulated,
        step,
    )?;
    runtime.clear_operator_state_for_scope();
    Ok(())
}

/// Reads every base table reachable from `root` in full and returns each as
/// an all-`+1` [`TableDelta`] — i.e. the current stored contents presented as
/// if freshly inserted. This is how hydration turns "read the whole table"
/// into the same delta-driven path a tick uses. Static scans on a source
/// restrict what is read.
pub(super) fn snapshot_table_deltas(
    schema: &crate::schema::DatabaseSchema,
    graph: &IvmGraph,
    storage: &impl OrderedKvStorage,
    root: NodeId,
) -> Result<Vec<TableDelta>, IvmRuntimeError> {
    let mut tables = HashMap::<TableSnapshotSource, RecordDescriptor>::default();
    collect_table_sources(graph, root, &mut tables)?;
    tables
        .into_iter()
        .map(|(source, descriptor)| {
            let table_schema = schema
                .table(&source.table)
                .ok_or_else(|| IvmRuntimeError::TableNotFound(source.table.clone()))?;
            let store = super::record_store_for_table(storage, table_schema, &descriptor);
            let mut deltas = Vec::new();
            let mut visit = |_: &[u8], record: &[u8]| {
                deltas.push(RecordDelta {
                    record: Bytes::copy_from_slice(record),
                    weight: 1,
                });
                Ok(())
            };
            match &source.scan {
                None => store.scan_prefix(b"", &mut visit)?,
                Some(scan) => match scan_bounds(scan)? {
                    StaticScanBounds::Prefix(prefix) => store.scan_prefix(&prefix, &mut visit)?,
                    StaticScanBounds::Range { start, end } => {
                        if start < end {
                            store.scan_range(&start, &end, &mut visit)?;
                        }
                    }
                },
            }
            Ok(TableDelta {
                table: source.table,
                descriptor,
                deltas,
            })
        })
        .collect()
}

/// One base table to snapshot, distinguished by its scan restriction so two
/// sources of the same table with different scans are read separately.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
struct TableSnapshotSource {
    table: String,
    scan: Option<StaticScanSpec>,
}

/// Walks the graph below `node` collecting every distinct table source to
/// snapshot, with its output layout.
fn collect_table_sources(
    graph: &IvmGraph,
    node: NodeId,
    tables: &mut HashMap<TableSnapshotSource, RecordDescriptor>,
) -> Result<(), IvmRuntimeError> {
    let graph_node = graph
        .node(node)
        .ok_or(IvmRuntimeError::GraphNodeNotFound(node))?;
    if let OpType::TableSource(table) = &graph_node.descriptor.operator {
        tables
            .entry(TableSnapshotSource {
                table: table.table.clone(),
                scan: table.scan.clone(),
            })
            .or_insert_with(|| graph_node.descriptor.output);
    }
    for input in &graph_node.descriptor.inputs {
        collect_table_sources(graph, *input, tables)?;
    }
    Ok(())
}

/// The sorted set of table names the seed and step graphs read, cached on the
/// [`RecursiveOp`] so per-tick "did a relevant table change?" checks don't
/// re-walk the graph.
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

/// Collects the names of every table read below `node` (via table *or* index
/// sources), with each table's output layout.
fn collect_table_source_names(
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
    } else if let OpType::IndexSource(index) = &graph_node.descriptor.operator {
        tables
            .entry(index.table.clone())
            .or_insert_with(|| graph_node.descriptor.output);
    }
    for input in &graph_node.descriptor.inputs {
        collect_table_source_names(graph, *input, tables)?;
    }
    Ok(())
}

/// Collects the shape names of every binding source read below `node`.
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

/// Collects the tables feeding the *right* side of any anti join below
/// `node`. A change to one of these can retract existing results, so it forces
/// a recompute (see [`has_recompute_table_delta_for_recursion`]).
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
        collect_table_source_names(graph, *right, tables)?;
    }
    for input in &graph_node.descriptor.inputs {
        collect_anti_join_right_table_sources(graph, *input, tables)?;
    }
    Ok(())
}

/// Computes a fixpoint from scratch against stored data (the recompute path).
///
/// Starts from the seed evaluated over full table snapshots, then repeatedly
/// runs the step over the newly accepted frontier — using the
/// [`HydrationEvaluator`], which reads tables in full instead of consuming
/// deltas — until no new rows appear or `max_iters` is hit. Returns the whole
/// accumulated relation.
#[allow(clippy::too_many_arguments)]
pub(super) fn recompute_recursive(
    schema: &crate::schema::DatabaseSchema,
    graph: &IvmGraph,
    node: NodeId,
    recursive: &RecursiveOp,
    output_desc: RecordDescriptor,
    step: NodeId,
    storage: &impl OrderedKvStorage,
    binding_snapshots: &HashMap<String, RecordDeltas>,
    _current_tick: u64,
    scope: ScopeId,
) -> Result<HashMap<Bytes, i64>, IvmRuntimeError> {
    let recursive_node = graph
        .node(node)
        .ok_or(IvmRuntimeError::GraphNodeNotFound(node))?;
    let [seed, _] = recursive_node.descriptor.inputs.as_slice() else {
        return Err(IvmRuntimeError::GraphInputArityMismatch(recursive_node.id));
    };

    let mut snapshot = HydrationEvaluator {
        schema,
        graph,
        storage,
        binding_snapshots,
        context: EvalContext::root(),
    };
    let mut accumulated = HashMap::<Bytes, i64>::default();
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
        let context =
            EvalContext::with_binding(scope, sub_tick as u64, recursive.frontier.clone(), frontier);
        let mut snapshot = HydrationEvaluator {
            schema,
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

/// The recompute-path analogue of [`RecursiveState::accept_positive`]: folds
/// new rows into a plain accumulator set and returns only the genuinely new
/// ones, rejecting any retraction.
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

/// Guards the monotone-recursion contract: any zero or negative weight in a
/// frontier is rejected before consolidation (so `+1` and `-1` on one row
/// cannot silently cancel into an accepted empty change).
fn reject_non_positive_frontier_deltas(deltas: &[RecordDelta]) -> Result<(), IvmRuntimeError> {
    if deltas.iter().any(|delta| delta.weight <= 0) {
        return Err(IvmRuntimeError::UnsupportedNonMonotoneRecursion);
    }
    Ok(())
}

/// Full-snapshot evaluator used by recursive recompute fallback.
///
/// A cut-down cousin of [`super::TickEvaluator`] that reads base tables in
/// full (rather than consuming deltas) and carries no memo or arrangement
/// state — exactly what the recompute path needs. It only implements the
/// operators a recursive graph can legally contain; the rest return
/// [`IvmRuntimeError::UnsupportedOperator`].
struct HydrationEvaluator<'a, S> {
    schema: &'a crate::schema::DatabaseSchema,
    graph: &'a IvmGraph,
    storage: &'a S,
    binding_snapshots: &'a HashMap<String, RecordDeltas>,
    context: EvalContext,
}

impl<S> HydrationEvaluator<'_, S>
where
    S: OrderedKvStorage,
{
    /// Recursively evaluates one node to its full snapshot output.
    fn eval_node(&mut self, node: NodeId) -> Result<RecordDeltas, IvmRuntimeError> {
        let graph_node = self
            .graph
            .node(node)
            .ok_or(IvmRuntimeError::GraphNodeNotFound(node))?;
        let output_desc = graph_node.descriptor.output;
        match &graph_node.descriptor.operator {
            OpType::TableSource(table) => self.eval_table_source(table, output_desc),
            OpType::IndexSource(index) => super::NodeState::update_index_source(
                index,
                &output_desc,
                &[],
                Some(self.storage),
                super::EvalMode::Hydrate,
            ),
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
            OpType::Filter(filter) => {
                let input = self.eval_unary_input(graph_node, node)?;
                NodeState::update_filter(filter, output_desc, &input)
            }
            OpType::MapProject(project) => {
                let input = self.eval_unary_input(graph_node, node)?;
                NodeState::update_map_project(project, output_desc, &input, None)
            }
            OpType::UnwrapNullable(unwrap) => {
                let input = self.eval_unary_input(graph_node, node)?;
                NodeState::update_unwrap_nullable(unwrap, output_desc, &input)
            }
            OpType::Unnest(unnest) => {
                let input = self.eval_unary_input(graph_node, node)?;
                NodeState::update_unnest(unnest, output_desc, &input)
            }
            OpType::ArgMaxBy(arg_max_by) => {
                let input = self.eval_unary_input(graph_node, node)?;
                let mut winners = std::collections::BTreeMap::<Vec<u8>, (Vec<u8>, Bytes)>::new();
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
                let mut winners = std::collections::BTreeMap::<Vec<u8>, (Vec<u8>, Bytes)>::new();
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
                NodeState::update_union(
                    output_desc,
                    inputs.into_iter().map(std::sync::Arc::new).collect(),
                )
            }
            OpType::IndexBy(index_by) => {
                let input = self.eval_unary_input(graph_node, node)?;
                NodeState::update_index_by(index_by, output_desc, &input)
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
                    std::collections::BTreeMap::<super::join::JoinKey, Vec<&RecordDelta>>::new();
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
                    for key in
                        super::join::join_keys(&join.left_descriptor, left_delta.raw(), &left_on)?
                    {
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
                    self.schema,
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

    /// Reads a whole base table as all-`+1` deltas.
    fn eval_table_source(
        &self,
        table: &TableSourceOp,
        output_desc: RecordDescriptor,
    ) -> Result<RecordDeltas, IvmRuntimeError> {
        let table_schema = self
            .schema
            .table(&table.table)
            .ok_or_else(|| IvmRuntimeError::TableNotFound(table.table.clone()))?;
        let store = super::record_store_for_table(self.storage, table_schema, &output_desc);
        let mut deltas = Vec::new();
        store.scan_prefix(b"", &mut |_, record| {
            deltas.push(RecordDelta {
                record: Bytes::copy_from_slice(record),
                weight: 1,
            });
            Ok(())
        })?;
        Ok(RecordDeltas {
            descriptor: output_desc,
            deltas,
        })
    }

    /// Evaluates the single input of a one-input node.
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
}
