//! Tick orchestration, hydration, memo eviction, and durable-node evaluation.

use std::collections::VecDeque;
use std::rc::Rc;

use super::evaluation_session::{EvaluationInputs, StorageRequestKey, StorageRequests};
use super::*;
use crate::storage::OwnedStorage;

/// Owned preparation state for one interruptible evaluation.
///
/// Storage suspension lives in `EvaluationWorkQueue`; the recursive evaluator
/// is entered only for a root whose complete source frontier is ready, so its
/// future is a short-lived execution frame rather than retained blocked work.
struct EvaluationSession {
    relevant_nodes: HashSet<NodeId>,
    roots: HashSet<NodeId>,
    outputs: HashMap<NodeId, RecordDeltas>,
    operator_states: HashMap<OperatorStateKey, OperatorState>,
    arrangement_states: HashMap<ArrangementKey, AsOf<ArrangementState, SubTick>>,
    arrangement_keys_by_input: HashMap<NodeId, HashSet<ArrangementKey>>,
    eval_memo: HashMap<EvalMemoKey, EvalMemoEntry>,
    eval_memo_bytes: usize,
    memo_use_clock: u64,
    node_meta: HashMap<NodeId, NodeRuntimeMeta>,
}

/// Discovers storage leaves for all reachable siblings without recursively
/// evaluating through the first blocked branch. Hash-consed nodes enter the
/// queue once, so discovery is linear in the reachable graph slice.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum EvaluationEntry {
    Waiting(usize),
    Runnable,
    Complete,
}

struct EvaluationWorkQueue {
    pending: VecDeque<NodeId>,
    visited: HashSet<NodeId>,
    entries: HashMap<NodeId, EvaluationEntry>,
    dependents: HashMap<NodeId, Vec<NodeId>>,
    storage_dependents: std::collections::BTreeMap<StorageRequestKey, Vec<NodeId>>,
    runnable: VecDeque<NodeId>,
    roots: HashSet<NodeId>,
}

impl EvaluationWorkQueue {
    fn new(roots: impl IntoIterator<Item = NodeId>) -> Self {
        let roots = roots.into_iter().collect::<VecDeque<_>>();
        Self {
            pending: roots.clone(),
            visited: HashSet::default(),
            entries: HashMap::default(),
            dependents: HashMap::default(),
            storage_dependents: std::collections::BTreeMap::new(),
            runnable: VecDeque::new(),
            roots: roots.into_iter().collect(),
        }
    }

    fn discover(mut self, graph: &IvmGraph) -> Result<(HashSet<NodeId>, Self), IvmRuntimeError> {
        while let Some(node_id) = self.pending.pop_front() {
            if !self.visited.insert(node_id) {
                continue;
            }
            let node = graph
                .node(node_id)
                .ok_or(IvmRuntimeError::GraphNodeNotFound(node_id))?;
            self.pending.extend(node.descriptor.inputs.iter().copied());
            for input in &node.descriptor.inputs {
                self.dependents.entry(*input).or_default().push(node_id);
            }
            let request = match &node.descriptor.operator {
                OpType::TableSource(source) => NodeState::table_source_request(source)?,
                OpType::IndexSource(source) => NodeState::index_source_request(source)?,
                _ => None,
            };
            if let Some(request) = request {
                self.storage_dependents
                    .entry(request)
                    .or_default()
                    .push(node_id);
                self.entries.insert(node_id, EvaluationEntry::Waiting(1));
            } else {
                self.entries.insert(
                    node_id,
                    EvaluationEntry::Waiting(node.descriptor.inputs.len()),
                );
            }
        }
        let initially_ready = self
            .entries
            .iter()
            .filter_map(|(node, entry)| (*entry == EvaluationEntry::Waiting(0)).then_some(*node))
            .collect::<Vec<_>>();
        for node in initially_ready {
            self.make_runnable(node);
        }
        let relevant_nodes = self.visited.clone();
        Ok((relevant_nodes, self))
    }

    fn requests(&self) -> impl Iterator<Item = &StorageRequestKey> {
        self.storage_dependents.keys()
    }

    fn storage_ready(&mut self, requests: impl IntoIterator<Item = StorageRequestKey>) {
        let ready_sources = requests
            .into_iter()
            .flat_map(|request| self.storage_dependents.remove(&request).unwrap_or_default())
            .collect::<Vec<_>>();
        for node in ready_sources {
            self.make_runnable(node);
        }
    }

    fn make_runnable(&mut self, node: NodeId) {
        if matches!(
            self.entries.get(&node),
            Some(EvaluationEntry::Runnable | EvaluationEntry::Complete)
        ) {
            return;
        }
        self.entries.insert(node, EvaluationEntry::Runnable);
        self.runnable.push_back(node);
    }

    fn complete(&mut self, node: NodeId) {
        self.entries.insert(node, EvaluationEntry::Complete);
        let dependents = self.dependents.get(&node).cloned().unwrap_or_default();
        for dependent in dependents {
            let Some(EvaluationEntry::Waiting(remaining)) = self.entries.get_mut(&dependent) else {
                continue;
            };
            *remaining = remaining.saturating_sub(1);
            if *remaining == 0 {
                self.make_runnable(dependent);
            }
        }
    }

    fn is_root(&self, node: NodeId) -> bool {
        self.roots.contains(&node)
    }
}

impl EvaluationSession {
    fn hydration(
        runtime: &IvmRuntime,
        roots: VecDeque<NodeId>,
    ) -> Result<(Self, EvaluationWorkQueue), IvmRuntimeError> {
        let (relevant_nodes, work_queue) =
            EvaluationWorkQueue::new(roots.iter().copied()).discover(&runtime.graph)?;
        // Installed operator state is root-scoped. Recursive child scopes are
        // scratch state and are cleared before an evaluation is installed.
        // Probe by reachable node instead of scanning state owned by unrelated
        // graphs.
        let operator_states = relevant_nodes
            .iter()
            .filter_map(|node| {
                let key = OperatorStateKey {
                    scope: ScopeId::root(),
                    node: *node,
                };
                runtime
                    .operator_states
                    .get(&key)
                    .cloned()
                    .map(|state| (key, state))
            })
            .collect();
        let mut arrangement_states = HashMap::default();
        let mut arrangement_keys_by_input = HashMap::default();
        for input in &relevant_nodes {
            let Some(keys) = runtime.arrangement_keys_by_input.get(input) else {
                continue;
            };
            for key in keys {
                if let Some(state) = runtime.arrangement_states.get(key) {
                    arrangement_states.insert(key.clone(), state.clone());
                    arrangement_keys_by_input
                        .entry(*input)
                        .or_insert_with(HashSet::default)
                        .insert(key.clone());
                }
            }
        }
        let eval_memo = runtime
            .eval_memo
            .iter()
            .filter(|(key, _)| relevant_nodes.contains(&key.node))
            .map(|(key, entry)| (key.clone(), entry.clone()))
            .collect::<HashMap<_, _>>();
        let eval_memo_bytes = eval_memo.values().map(|entry| entry.payload_bytes).sum();
        let node_meta = relevant_nodes
            .iter()
            .filter_map(|node| {
                runtime
                    .node_meta
                    .get(node)
                    .cloned()
                    .map(|meta| (*node, meta))
            })
            .collect();
        Ok((
            Self {
                relevant_nodes,
                roots: roots.into_iter().collect(),
                outputs: HashMap::default(),
                operator_states,
                arrangement_states,
                arrangement_keys_by_input,
                eval_memo,
                eval_memo_bytes,
                memo_use_clock: runtime.memo_use_clock,
                node_meta,
            },
            work_queue,
        ))
    }

    fn install(self, runtime: &mut IvmRuntime) {
        for node in &self.relevant_nodes {
            runtime.operator_states.remove(&OperatorStateKey {
                scope: ScopeId::root(),
                node: *node,
            });
        }
        runtime.operator_states.extend(self.operator_states);
        for node in &self.relevant_nodes {
            if let Some(keys) = runtime.arrangement_keys_by_input.get(node) {
                for key in keys {
                    runtime.arrangement_states.remove(key);
                }
            }
        }
        runtime.arrangement_states.extend(self.arrangement_states);
        for node in &self.relevant_nodes {
            runtime.arrangement_keys_by_input.remove(node);
        }
        runtime
            .arrangement_keys_by_input
            .extend(self.arrangement_keys_by_input);
        runtime
            .eval_memo
            .retain(|key, _| !self.relevant_nodes.contains(&key.node));
        runtime.eval_memo.extend(self.eval_memo);
        runtime.eval_memo_bytes = runtime
            .eval_memo
            .values()
            .map(|entry| entry.payload_bytes)
            .sum();
        runtime.memo_use_clock = self.memo_use_clock;
        for node in &self.relevant_nodes {
            runtime.node_meta.remove(node);
        }
        runtime.node_meta.extend(self.node_meta);
    }
}

impl IvmRuntime {
    pub async fn tick<S>(
        &mut self,
        table_deltas: Vec<TableDelta>,
        storage: &S,
    ) -> Result<TickMetrics, IvmRuntimeError>
    where
        S: OrderedKvStorage,
    {
        self.tick_with_params(table_deltas, Vec::new(), storage)
            .await
    }

    pub(super) async fn flush_pending_binding_retractions<S>(
        &mut self,
        storage: &S,
    ) -> Result<(), IvmRuntimeError>
    where
        S: OrderedKvStorage,
    {
        if !self.pending_binding_retractions.is_empty() {
            // Unsubscribe may queue routed binding retractions for the next
            // runtime tick. Snapshot hydration also needs a binding snapshot,
            // so it must first bring queued retractions into arranged state;
            // otherwise the snapshot could observe a binding as live while
            // its retraction is already committed to the lifecycle queue.
            self.tick_with_params(Vec::new(), Vec::new(), storage)
                .await?;
        }
        Ok(())
    }

    pub(crate) async fn tick_staged<S>(
        &mut self,
        table_deltas: Vec<TableDelta>,
        storage: &S,
        staged_writes: &mut Vec<OwnedWriteOperation>,
    ) -> Result<TickMetrics, IvmRuntimeError>
    where
        S: OrderedKvStorage,
    {
        debug_assert!(
            !self.defer_subscription_notifications,
            "a durable tick must not nest while computing subscription output"
        );
        let staged_overlay = RefCell::new(StagedWriteState::from(std::mem::take(staged_writes)));
        let overlay = StagedWriteOverlay::new(storage, &staged_overlay);
        self.defer_subscription_notifications = true;
        let tick = self
            .tick_with_params(table_deltas, Vec::new(), &overlay)
            .await;
        self.defer_subscription_notifications = false;
        overlay.drain_into(staged_writes);
        if tick.is_err() {
            self.staged_subscription_notifications.clear();
        }
        tick
    }

    /// Release subscription output computed by the preceding staged durable
    /// tick.  This is deliberately separate from `tick_staged`: callers must
    /// invoke it only after the matching storage transaction commits.
    pub(crate) fn publish_staged_subscription_notifications(&mut self) {
        let pending = std::mem::take(&mut self.staged_subscription_notifications);
        let mut dropped = Vec::new();
        for (subscription_id, queued) in pending {
            let Some(subscription) = self.multisink_subscriptions.get(&subscription_id) else {
                continue;
            };
            if subscription.sender.send(queued).is_err() {
                dropped.push(subscription_id);
            }
        }
        for subscription_id in dropped {
            self.unsubscribe(subscription_id);
        }
    }

    pub(crate) fn tag_staged_subscription_notifications(&mut self, publication: PublicationId) {
        for (_, queued) in &mut self.staged_subscription_notifications {
            queued.publication = Some(publication);
        }
    }

    /// Forget output from a failed staged storage transaction.  The runtime is
    /// generally poisoned by its caller after that failure, but clearing this
    /// queue makes the no-publication boundary explicit and robust to teardown.
    pub(crate) fn discard_staged_subscription_notifications(&mut self) {
        self.staged_subscription_notifications.clear();
    }

    pub(super) async fn tick_with_params<S>(
        &mut self,
        table_deltas: Vec<TableDelta>,
        mut binding_deltas: Vec<BindingDelta>,
        storage: &S,
    ) -> Result<TickMetrics, IvmRuntimeError>
    where
        S: OrderedKvStorage,
    {
        if !self.pending_binding_retractions.is_empty() {
            let mut pending = std::mem::take(&mut self.pending_binding_retractions);
            pending.extend(binding_deltas);
            binding_deltas = pending;
        }
        let negative_tables = table_deltas
            .iter()
            .filter(|delta| delta.deltas.iter().any(|record| record.weight < 0))
            .map(|delta| delta.table.as_str())
            .collect::<HashSet<_>>();
        let has_negative_bindings = binding_deltas
            .iter()
            .flat_map(|delta| &delta.deltas)
            .any(|delta| delta.weight < 0);
        let needs_recompute_inputs = !negative_tables.is_empty() || has_negative_bindings;
        let mut evaluation_inputs = if needs_recompute_inputs {
            self.load_recursive_recompute_inputs(storage, &negative_tables, has_negative_bindings)
                .await?
        } else {
            EvaluationInputs::default()
        };
        let current_tick = self.advance_tick();
        self.bump_input_frontiers(&table_deltas, &binding_deltas);
        let table_delta_records = table_deltas
            .iter()
            .map(|delta| delta.deltas.len())
            .sum::<usize>();
        self.tick_durable_nodes(&table_deltas, current_tick, storage)
            .await?;
        let mut dropped_subscriptions = Vec::new();
        let mut deferred_notifications = Vec::new();
        let mut metrics = TickMetrics {
            tick: current_tick,
            table_delta_records,
            ..TickMetrics::default()
        };
        let binding_snapshots = self.binding_snapshot_deltas();
        let mut retained_roots = self
            .node_meta
            .iter()
            .filter(|(node, meta)| {
                !meta.retainers.is_empty()
                    && self
                        .graph
                        .node(**node)
                        .is_some_and(|node| !node.is_durable())
            })
            .map(|(node, _)| *node)
            .collect::<Vec<_>>();
        retained_roots.sort_unstable();
        let mut evaluator = TickEvaluator {
            schema: &self.schema,
            graph: &self.graph,
            variant_projections: &self.variant_projections,
            table_deltas: &table_deltas,
            binding_deltas: &binding_deltas,
            binding_snapshots: &binding_snapshots,
            current_tick,
            operator_states: &mut self.operator_states,
            arrangement_states: &mut self.arrangement_states,
            arrangement_keys_by_input: &mut self.arrangement_keys_by_input,
            eval_memo: &mut self.eval_memo,
            eval_memo_bytes: &mut self.eval_memo_bytes,
            table_frontiers: &self.table_frontiers,
            binding_frontiers: &self.binding_frontiers,
            memo_use_clock: &mut self.memo_use_clock,
            node_meta: &mut self.node_meta,
            storage: Some(storage),
            evaluation_inputs: needs_recompute_inputs.then_some(&mut evaluation_inputs),
            context: EvalContext::root(),
            metrics: &mut metrics,
            terminal_deltas: HashMap::default(),
            root_ordering_windows: HashMap::default(),
        };

        for (subscription_id, subscription) in &self.multisink_subscriptions {
            let mut sinks = BTreeMap::new();
            let mut terminal_sinks = BTreeMap::new();
            for (sink, output) in &subscription.outputs {
                let records = evaluator.update_node(output.node).await?;
                if !records.deltas.is_empty()
                    && !records.descriptor.registry_compatible_with(&output.output)
                {
                    return Err(IvmRuntimeError::GraphOutputMismatch);
                }
                let structured = evaluator.output_is_structured_collect_by(output.node)?;
                let public_root = evaluator.output_has_public_root(output.node)?;
                let terminal_owned = output.root_ordering_node.is_some() || structured;
                let records = records.as_ref().clone();
                if terminal_owned {
                    let terminal = if structured {
                        if let Some(terminal) =
                            evaluator.take_terminal_deltas_for_output(output.node)?
                        {
                            Some(terminal)
                        } else if !public_root && !records.is_empty() {
                            Some(terminal_deltas_from_record_deltas(&records)?)
                        } else if output.root_ordering_node.is_some() {
                            // A root TopBy can change positions while the
                            // structured projection has no payload delta.
                            // Preserve the ordering channel with an empty
                            // terminal; `apply_root_ordering` adds only the
                            // necessary root Move edits.
                            Some(TerminalDeltas {
                                operations: Vec::new(),
                            })
                        } else {
                            None
                        }
                    } else if !records.is_empty() {
                        // The ordering node can be above a wider source row.
                        // It contributes only root positions; terminal payloads
                        // must be encoded from the public sink descriptor.
                        Some(terminal_deltas_from_record_deltas(&records)?)
                    } else if output.root_ordering_node.is_some() {
                        // An unprojected sort-key change can reorder visible
                        // roots without changing any rendered payload. Start
                        // an empty terminal so root ordering can still emit
                        // its positional Move operations.
                        Some(TerminalDeltas {
                            operations: Vec::new(),
                        })
                    } else {
                        None
                    };
                    if let Some(mut terminal) = terminal {
                        if let Some(root_ordering_node) = output.root_ordering_node {
                            evaluator.apply_root_ordering(
                                root_ordering_node,
                                output.output,
                                &mut terminal,
                            )?;
                        }
                        if !terminal.is_empty() {
                            terminal_sinks.insert(sink.clone(), terminal);
                        }
                    }
                }
                if !records.is_empty() {
                    // Keep the rendered terminal record available for legacy
                    // single-sink consumers and hydration. Structured carriers
                    // select `terminal_sinks` for incremental delivery.
                    sinks.insert(sink.clone(), records);
                }
            }
            let records = MultisinkDeltas {
                sinks,
                terminal_sinks,
            };
            if !records.is_empty() {
                evaluator.metrics.notifications_sent += 1;
                evaluator.metrics.notification_records += multisink_deltas_record_count(&records);
                evaluator.metrics.notification_encoded_bytes +=
                    multisink_deltas_encoded_bytes(&records);
            }
            let queued = QueuedMultisinkDeltas::new(records);
            if !queued.deltas.is_empty() {
                if self.defer_subscription_notifications {
                    deferred_notifications.push((*subscription_id, queued));
                } else if subscription.sender.send(queued).is_err() {
                    dropped_subscriptions.push(*subscription_id);
                }
            }
        }
        self.staged_subscription_notifications
            .extend(deferred_notifications);
        // Retained roots are background maintenance. Active subscriptions must
        // see the tick's deltas before retained-only roots can advance shared
        // recursive/operator state.
        for node in retained_roots {
            evaluator.update_node(node).await?;
        }
        drop(evaluator);
        self.operator_states
            .retain(|key, _| key.scope == ScopeId::root());

        for subscription_id in dropped_subscriptions {
            self.unsubscribe(subscription_id);
        }
        debug_assert!(self.retained_recursive_nodes_are_current(current_tick));
        self.evict_eval_memo();
        metrics.runtime_stats = if self.collect_tick_runtime_stats {
            self.stats()
        } else {
            self.cheap_stats()
        };
        Ok(metrics)
    }

    async fn load_recursive_recompute_inputs<S>(
        &self,
        storage: &S,
        negative_tables: &HashSet<&str>,
        has_negative_bindings: bool,
    ) -> Result<EvaluationInputs, IvmRuntimeError>
    where
        S: OrderedKvStorage,
    {
        let mut requests = std::collections::BTreeSet::new();
        let mut pending = self
            .graph
            .nodes()
            .iter()
            .filter_map(|(node, graph_node)| {
                let OpType::Recursive(recursive) = &graph_node.descriptor.operator else {
                    return None;
                };
                (self
                    .node_meta
                    .get(node)
                    .is_some_and(|meta| !meta.retainers.is_empty())
                    && (has_negative_bindings
                        || recursive
                            .read_tables
                            .iter()
                            .any(|table| negative_tables.contains(table.as_str()))))
                .then_some(*node)
            })
            .collect::<Vec<_>>();
        let mut visited: HashSet<NodeId> = HashSet::default();
        while let Some(node_id) = pending.pop() {
            if !visited.insert(node_id) {
                continue;
            }
            let node = self
                .graph
                .node(node_id)
                .ok_or(IvmRuntimeError::GraphNodeNotFound(node_id))?;
            pending.extend(node.descriptor.inputs.iter().copied());
            match &node.descriptor.operator {
                OpType::TableSource(source) => {
                    let request = match source.scan.as_ref().map(scan_bounds).transpose()? {
                        None => StorageRequestKey::ScanPrefix {
                            family: source.table.clone(),
                            prefix: Vec::new(),
                        },
                        Some(StaticScanBounds::Prefix(prefix)) => StorageRequestKey::ScanPrefix {
                            family: source.table.clone(),
                            prefix,
                        },
                        Some(StaticScanBounds::Range { start, end }) if start < end => {
                            StorageRequestKey::ScanRange {
                                family: source.table.clone(),
                                start,
                                end,
                            }
                        }
                        Some(StaticScanBounds::Range { .. }) => continue,
                    };
                    requests.insert(request);
                }
                OpType::IndexSource(source) => {
                    let request = match persisted_index_scan_bounds(
                        &source.table,
                        &source.index,
                        source.scan.as_ref(),
                    )? {
                        StaticScanBounds::Prefix(prefix) => StorageRequestKey::ScanPrefix {
                            family: "indices".to_owned(),
                            prefix,
                        },
                        StaticScanBounds::Range { start, end } if start < end => {
                            StorageRequestKey::ScanRange {
                                family: "indices".to_owned(),
                                start,
                                end,
                            }
                        }
                        StaticScanBounds::Range { .. } => continue,
                    };
                    requests.insert(request);
                }
                _ => {}
            }
        }
        let owned_storage = OwnedStorage::new(Rc::new(storage));
        let mut storage_requests = StorageRequests::new();
        for request in requests {
            storage_requests.request(request, &owned_storage);
        }
        while storage_requests.has_pending() {
            std::future::poll_fn(|cx| {
                if storage_requests.poll(cx) > 0 {
                    std::task::Poll::Ready(())
                } else {
                    std::task::Poll::Pending
                }
            })
            .await;
        }
        let mut inputs = EvaluationInputs::default();
        inputs.install(storage_requests.drain_ready()?);
        Ok(inputs)
    }

    fn bump_input_frontiers(
        &mut self,
        table_deltas: &[TableDelta],
        binding_deltas: &[BindingDelta],
    ) {
        let mut changed_tables = Vec::new();
        for delta in table_deltas.iter().filter(|delta| !delta.deltas.is_empty()) {
            *self.table_frontiers.entry(delta.table.clone()).or_default() += 1;
            changed_tables.push(delta.table.as_str());
        }
        let mut changed_bindings = Vec::new();
        for delta in binding_deltas
            .iter()
            .filter(|delta| !delta.deltas.is_empty())
        {
            *self
                .binding_frontiers
                .entry(delta.shape.clone())
                .or_default() += 1;
            changed_bindings.push(delta.shape.as_str());
        }
        if changed_tables.is_empty() && changed_bindings.is_empty() {
            return;
        }
        for meta in self.node_meta.values_mut() {
            let Some(signature) = meta.input_signature.as_ref() else {
                continue;
            };
            let table_changed = changed_tables.iter().any(|changed| {
                signature
                    .tables
                    .iter()
                    .any(|table| table.as_str() == *changed)
            });
            let binding_changed = changed_bindings.iter().any(|changed| {
                signature
                    .bindings
                    .iter()
                    .any(|binding| binding.as_str() == *changed)
            });
            if table_changed || binding_changed {
                meta.input_generation = meta.input_generation.wrapping_add(1);
            }
        }
    }

    fn evict_eval_memo(&mut self) {
        if self.eval_memo.keys().any(|key| key.tick_epoch.is_some()) {
            let mut retained_bytes = 0usize;
            self.eval_memo.retain(|key, entry| {
                let keep = key.tick_epoch.is_none();
                if keep {
                    retained_bytes = retained_bytes.saturating_add(entry.payload_bytes);
                }
                keep
            });
            self.eval_memo_bytes = retained_bytes;
        }
        if self.eval_memo.len() <= EVAL_MEMO_MAX_ENTRIES
            && self.eval_memo_bytes <= EVAL_MEMO_MAX_BYTES
        {
            return;
        }
        let mut entries = self
            .eval_memo
            .iter()
            .map(|(key, entry)| (key.clone(), entry.last_used))
            .collect::<Vec<_>>();
        entries.sort_unstable_by_key(|(_, last_used)| *last_used);
        for (key, _) in entries {
            if self.eval_memo.len() <= EVAL_MEMO_MAX_ENTRIES
                && self.eval_memo_bytes <= EVAL_MEMO_MAX_BYTES
            {
                break;
            }
            if let Some(entry) = self.eval_memo.remove(&key) {
                self.eval_memo_bytes = self.eval_memo_bytes.saturating_sub(entry.payload_bytes);
            }
        }
    }

    #[cfg(test)]
    fn recompute_eval_memo_bytes(&mut self) {
        self.eval_memo_bytes = self
            .eval_memo
            .values()
            .map(|entry| entry.payload_bytes)
            .sum();
    }

    #[cfg(test)]
    pub(super) fn evict_eval_memo_for_tests(&mut self, max_entries: usize, max_bytes: usize) {
        self.eval_memo.retain(|key, _| key.tick_epoch.is_none());
        self.recompute_eval_memo_bytes();
        let mut entries = self
            .eval_memo
            .iter()
            .map(|(key, entry)| (key.clone(), entry.last_used))
            .collect::<Vec<_>>();
        entries.sort_unstable_by_key(|(_, last_used)| *last_used);
        for (key, _) in entries {
            if self.eval_memo.len() <= max_entries && self.eval_memo_bytes <= max_bytes {
                break;
            }
            if let Some(entry) = self.eval_memo.remove(&key) {
                self.eval_memo_bytes = self.eval_memo_bytes.saturating_sub(entry.payload_bytes);
            }
        }
    }

    pub(super) async fn hydration_snapshot<S>(
        &mut self,
        output_node: NodeId,
        storage: &S,
        mode: HydrationMode,
    ) -> Result<RecordDeltas, IvmRuntimeError>
    where
        S: OrderedKvStorage,
    {
        self.hydration_roots([output_node], storage, mode)
            .await?
            .remove(&output_node)
            .ok_or(IvmRuntimeError::GraphNodeNotFound(output_node))
    }

    async fn hydration_roots<S>(
        &mut self,
        roots: impl IntoIterator<Item = NodeId>,
        storage: &S,
        mode: HydrationMode,
    ) -> Result<HashMap<NodeId, RecordDeltas>, IvmRuntimeError>
    where
        S: OrderedKvStorage,
    {
        let roots = roots.into_iter().collect::<VecDeque<_>>();
        let binding_snapshots = self.binding_snapshot_deltas();
        let mut metrics = TickMetrics::default();
        if roots.is_empty() {
            return Err(IvmRuntimeError::UnsupportedOperator);
        }
        let hydrate_arrangements = mode == HydrationMode::Subscription
            && roots.iter().copied().try_fold(false, |found, root| {
                Ok::<_, IvmRuntimeError>(found || self.output_depends_on_aggregate(root)?)
            })?;
        let (mut session, mut work_queue) = EvaluationSession::hydration(self, roots)?;
        let mut evaluation_inputs = EvaluationInputs::default();
        let owned_storage = OwnedStorage::new(Rc::new(storage));
        let mut storage_requests = StorageRequests::new();
        for request in work_queue.requests().cloned().collect::<Vec<_>>() {
            storage_requests.request(request, &owned_storage);
        }
        while session.outputs.len() < session.roots.len() {
            while let Some(node) = work_queue.runnable.pop_front() {
                let context = if hydrate_arrangements {
                    EvalContext::root_subscription_snapshot()
                } else {
                    EvalContext::root_snapshot()
                };
                let result = {
                    let mut evaluator = TickEvaluator {
                        schema: &self.schema,
                        graph: &self.graph,
                        variant_projections: &self.variant_projections,
                        table_deltas: &[],
                        binding_deltas: &[],
                        binding_snapshots: &binding_snapshots,
                        current_tick: self.current_tick,
                        operator_states: &mut session.operator_states,
                        arrangement_states: &mut session.arrangement_states,
                        arrangement_keys_by_input: &mut session.arrangement_keys_by_input,
                        eval_memo: &mut session.eval_memo,
                        eval_memo_bytes: &mut session.eval_memo_bytes,
                        table_frontiers: &self.table_frontiers,
                        binding_frontiers: &self.binding_frontiers,
                        memo_use_clock: &mut session.memo_use_clock,
                        node_meta: &mut session.node_meta,
                        storage: Some(storage),
                        evaluation_inputs: Some(&mut evaluation_inputs),
                        context,
                        metrics: &mut metrics,
                        terminal_deltas: HashMap::default(),
                        root_ordering_windows: HashMap::default(),
                    };
                    evaluator
                        .update_node(node)
                        .await
                        .map(|records| records.as_ref().clone())
                };
                match result {
                    Ok(records) => {
                        if work_queue.is_root(node) {
                            session.outputs.insert(node, records);
                        }
                        work_queue.complete(node);
                    }
                    Err(IvmRuntimeError::EvaluationBlocked) => {
                        return Err(IvmRuntimeError::EvaluationBlocked);
                    }
                    Err(error) => return Err(error),
                }
            }
            if session.outputs.len() == session.roots.len() {
                break;
            }
            if !storage_requests.has_pending() {
                return Err(IvmRuntimeError::EvaluationBlocked);
            }
            std::future::poll_fn(|cx| {
                if storage_requests.poll(cx) > 0 {
                    std::task::Poll::Ready(())
                } else {
                    std::task::Poll::Pending
                }
            })
            .await;
            let ready = storage_requests.drain_ready()?;
            work_queue.storage_ready(ready.keys().cloned());
            evaluation_inputs.install(ready);
        }
        let outputs = std::mem::take(&mut session.outputs);
        session.install(self);
        self.record_hydration_memo_metrics(&metrics);
        self.evict_eval_memo();
        Ok(outputs)
    }

    pub(super) async fn hydration_snapshots<S>(
        &mut self,
        outputs: &BTreeMap<String, CompiledNode>,
        storage: &S,
        mode: HydrationMode,
    ) -> Result<MultisinkDeltas, IvmRuntimeError>
    where
        S: OrderedKvStorage,
    {
        let mut seen_roots = HashSet::new();
        let roots = outputs
            .values()
            .flat_map(|output| [output.root_ordering_node, Some(output.node)])
            .flatten()
            .filter(|root| seen_roots.insert(*root))
            .collect::<Vec<_>>();
        let hydrated = self.hydration_roots(roots, storage, mode).await?;
        let mut sinks = BTreeMap::new();
        for (sink, output) in outputs {
            let ordering = match output.root_ordering_node {
                Some(node) => Some(
                    hydrated
                        .get(&node)
                        .cloned()
                        .ok_or(IvmRuntimeError::GraphNodeNotFound(node))?,
                ),
                None => None,
            };
            let mut records = hydrated
                .get(&output.node)
                .cloned()
                .ok_or(IvmRuntimeError::GraphNodeNotFound(output.node))?;
            if !records.descriptor.registry_compatible_with(&output.output) {
                return Err(IvmRuntimeError::GraphOutputMismatch);
            }
            if let Some(ordering) = &ordering {
                order_terminal_snapshot(&mut records, ordering)?;
            }
            sinks.insert(sink.clone(), records);
        }
        Ok(MultisinkDeltas {
            sinks,
            terminal_sinks: BTreeMap::new(),
        })
    }

    pub(super) async fn hydration_snapshots_for_subscription<S>(
        &mut self,
        outputs: &BTreeMap<String, CompiledNode>,
        storage: &S,
    ) -> Result<MultisinkDeltas, IvmRuntimeError>
    where
        S: OrderedKvStorage,
    {
        self.hydration_snapshots(outputs, storage, HydrationMode::Subscription)
            .await
    }

    fn output_depends_on_aggregate(&self, output_node: NodeId) -> Result<bool, IvmRuntimeError> {
        let mut ancestors = HashSet::new();
        self.graph.mark_ancestors(output_node, &mut ancestors);
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

    async fn tick_durable_nodes<S>(
        &mut self,
        table_deltas: &[TableDelta],
        current_tick: u64,
        storage: &S,
    ) -> Result<(), IvmRuntimeError>
    where
        S: OrderedKvStorage,
    {
        let durable_nodes = self
            .retained_node_ids()
            .into_iter()
            .filter(|node| self.graph.node(*node).is_some_and(|node| node.is_durable()))
            .collect::<Vec<_>>();
        let binding_snapshots = self.binding_snapshot_deltas();
        let mut metrics = TickMetrics::default();
        for node in durable_nodes {
            let graph_node = self
                .graph
                .node(node)
                .ok_or(IvmRuntimeError::GraphNodeNotFound(node))?;
            let OpType::Persist(persist) = graph_node.descriptor.operator.clone() else {
                return Err(IvmRuntimeError::UnsupportedOperator);
            };
            let [input_node] = graph_node.descriptor.inputs.as_slice() else {
                return Err(IvmRuntimeError::GraphInputArityMismatch(node));
            };
            let input = {
                let mut evaluator = TickEvaluator {
                    schema: &self.schema,
                    graph: &self.graph,
                    variant_projections: &self.variant_projections,
                    table_deltas,
                    binding_deltas: &[],
                    binding_snapshots: &binding_snapshots,
                    current_tick,
                    operator_states: &mut self.operator_states,
                    arrangement_states: &mut self.arrangement_states,
                    arrangement_keys_by_input: &mut self.arrangement_keys_by_input,
                    eval_memo: &mut self.eval_memo,
                    eval_memo_bytes: &mut self.eval_memo_bytes,
                    table_frontiers: &self.table_frontiers,
                    binding_frontiers: &self.binding_frontiers,
                    memo_use_clock: &mut self.memo_use_clock,
                    node_meta: &mut self.node_meta,
                    storage: Some(storage),
                    evaluation_inputs: None,
                    context: EvalContext::root(),
                    metrics: &mut metrics,
                    terminal_deltas: HashMap::default(),
                    root_ordering_windows: HashMap::default(),
                };
                evaluator.update_node(*input_node).await?.as_ref().clone()
            };
            apply_persist_delta(
                storage,
                &persist.storage,
                &persist.key_fields,
                persist.unique,
                &input,
            )
            .await?;
        }

        Ok(())
    }
}
