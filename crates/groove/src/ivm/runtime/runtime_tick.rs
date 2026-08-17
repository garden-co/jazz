//! Tick orchestration, hydration, memo eviction, and durable-node evaluation.

use super::*;

impl IvmRuntime {
    pub fn tick<S>(
        &mut self,
        table_deltas: Vec<TableDelta>,
        storage: &S,
    ) -> Result<TickMetrics, IvmRuntimeError>
    where
        S: ResidentStorage,
    {
        self.tick_with_params(table_deltas, Vec::new(), storage)
    }

    pub(super) fn flush_pending_binding_retractions<S>(
        &mut self,
        storage: &S,
    ) -> Result<(), IvmRuntimeError>
    where
        S: ResidentStorage,
    {
        if !self.pending_binding_retractions.is_empty() {
            // Unsubscribe may queue routed binding retractions for the next
            // runtime tick. Snapshot hydration also needs a binding snapshot,
            // so it must first bring queued retractions into arranged state;
            // otherwise the snapshot could observe a binding as live while
            // its retraction is already committed to the lifecycle queue.
            self.tick_with_params(Vec::new(), Vec::new(), storage)?;
        }
        Ok(())
    }

    pub(crate) fn tick_staged<S>(
        &mut self,
        table_deltas: Vec<TableDelta>,
        storage: &S,
        staged_writes: &mut Vec<OwnedWriteOperation>,
    ) -> Result<TickMetrics, IvmRuntimeError>
    where
        S: ResidentStorage,
    {
        debug_assert!(
            !self.defer_subscription_notifications,
            "a durable tick must not nest while computing subscription output"
        );
        let staged_overlay = RefCell::new(StagedWriteState::from(std::mem::take(staged_writes)));
        let overlay = StagedWriteOverlay::new(storage, &staged_overlay);
        self.defer_subscription_notifications = true;
        let tick = self.tick_with_params(table_deltas, Vec::new(), &overlay);
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

    /// Forget output from a failed staged storage transaction.  The runtime is
    /// generally poisoned by its caller after that failure, but clearing this
    /// queue makes the no-publication boundary explicit and robust to teardown.
    pub(crate) fn discard_staged_subscription_notifications(&mut self) {
        self.staged_subscription_notifications.clear();
    }

    pub(super) fn tick_with_params<S>(
        &mut self,
        table_deltas: Vec<TableDelta>,
        mut binding_deltas: Vec<BindingDelta>,
        storage: &S,
    ) -> Result<TickMetrics, IvmRuntimeError>
    where
        S: ResidentStorage,
    {
        if !self.pending_binding_retractions.is_empty() {
            let mut pending = std::mem::take(&mut self.pending_binding_retractions);
            pending.extend(binding_deltas);
            binding_deltas = pending;
        }
        let current_tick = self.advance_tick();
        self.bump_input_frontiers(&table_deltas, &binding_deltas);
        let table_delta_records = table_deltas
            .iter()
            .map(|delta| delta.deltas.len())
            .sum::<usize>();
        self.tick_durable_nodes(&table_deltas, current_tick, storage)?;
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
            eval_memo: &mut self.eval_memo,
            eval_memo_bytes: &mut self.eval_memo_bytes,
            table_frontiers: &self.table_frontiers,
            binding_frontiers: &self.binding_frontiers,
            memo_use_clock: &mut self.memo_use_clock,
            node_meta: &mut self.node_meta,
            storage: Some(storage),
            context: EvalContext::root(),
            metrics: &mut metrics,
            terminal_deltas: HashMap::default(),
            root_ordering_windows: HashMap::default(),
        };

        for (subscription_id, subscription) in &self.multisink_subscriptions {
            let mut sinks = BTreeMap::new();
            let mut terminal_sinks = BTreeMap::new();
            for (sink, output) in &subscription.outputs {
                let records = evaluator.update_node(output.node)?;
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
            evaluator.update_node(node)?;
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

    pub(super) fn queued_multisink_deltas(&self, deltas: MultisinkDeltas) -> QueuedMultisinkDeltas {
        QueuedMultisinkDeltas::new(deltas)
    }

    pub(super) fn hydration_snapshot<S>(
        &mut self,
        output_node: NodeId,
        storage: &S,
        mode: HydrationMode,
    ) -> Result<RecordDeltas, IvmRuntimeError>
    where
        S: ResidentStorage,
    {
        let table_deltas = snapshot_table_deltas(&self.schema, &self.graph, storage, output_node)?;
        let binding_snapshots = self.binding_snapshot_deltas();
        let context = match mode {
            HydrationMode::Ordinary => EvalContext::root_snapshot(),
            // Subscription hydration must rebuild arrangements for aggregate
            // outputs, while ordinary snapshot hydration intentionally only
            // probes them. Keep that distinction at this policy boundary so
            // both paths share the same frontier and memo lifecycle.
            HydrationMode::Subscription if self.output_depends_on_aggregate(output_node)? => {
                EvalContext::root_subscription_snapshot()
            }
            HydrationMode::Subscription => EvalContext::root_snapshot(),
        };
        let mut metrics = TickMetrics::default();
        let mut evaluator = TickEvaluator {
            schema: &self.schema,
            graph: &self.graph,
            variant_projections: &self.variant_projections,
            table_deltas: &table_deltas,
            binding_deltas: &[],
            binding_snapshots: &binding_snapshots,
            current_tick: self.current_tick,
            operator_states: &mut self.operator_states,
            arrangement_states: &mut self.arrangement_states,
            // Snapshot hydration is evaluated at the runtime's current
            // logical frontier. If a canonical fragment has already been
            // hydrated at this frontier, reusing its memoized output is an
            // attach/probe operation, not an accumulation over stale state:
            // any table or binding change that could invalidate it advances the
            // input frontier counters stored with each memo entry.
            eval_memo: &mut self.eval_memo,
            eval_memo_bytes: &mut self.eval_memo_bytes,
            table_frontiers: &self.table_frontiers,
            binding_frontiers: &self.binding_frontiers,
            memo_use_clock: &mut self.memo_use_clock,
            node_meta: &mut self.node_meta,
            storage: Some(storage),
            context,
            metrics: &mut metrics,
            terminal_deltas: HashMap::default(),
            root_ordering_windows: HashMap::default(),
        };
        let records = evaluator
            .update_node(output_node)
            .map(|records| records.as_ref().clone());
        self.record_hydration_memo_metrics(&metrics);
        self.evict_eval_memo();
        records
    }

    pub(super) fn hydration_snapshots<S>(
        &mut self,
        outputs: &BTreeMap<String, CompiledNode>,
        storage: &S,
        mode: HydrationMode,
    ) -> Result<MultisinkDeltas, IvmRuntimeError>
    where
        S: ResidentStorage,
    {
        let mut sinks = BTreeMap::new();
        for (sink, output) in outputs {
            let ordering = output
                .root_ordering_node
                .map(|node| self.hydration_snapshot(node, storage, mode))
                .transpose()?;
            let mut records = self.hydration_snapshot(output.node, storage, mode)?;
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

    pub(super) fn hydration_snapshots_for_subscription<S>(
        &mut self,
        outputs: &BTreeMap<String, CompiledNode>,
        storage: &S,
    ) -> Result<MultisinkDeltas, IvmRuntimeError>
    where
        S: ResidentStorage,
    {
        self.hydration_snapshots(outputs, storage, HydrationMode::Subscription)
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

    fn tick_durable_nodes<S>(
        &mut self,
        table_deltas: &[TableDelta],
        current_tick: u64,
        storage: &S,
    ) -> Result<(), IvmRuntimeError>
    where
        S: ResidentStorage,
    {
        let durable_nodes = self
            .retained_node_ids()
            .into_iter()
            .filter(|node| self.graph.node(*node).is_some_and(|node| node.is_durable()))
            .collect::<Vec<_>>();
        let binding_snapshots = self.binding_snapshot_deltas();
        let mut metrics = TickMetrics::default();
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
            eval_memo: &mut self.eval_memo,
            eval_memo_bytes: &mut self.eval_memo_bytes,
            table_frontiers: &self.table_frontiers,
            binding_frontiers: &self.binding_frontiers,
            memo_use_clock: &mut self.memo_use_clock,
            node_meta: &mut self.node_meta,
            storage: Some(storage),
            context: EvalContext::root(),
            metrics: &mut metrics,
            terminal_deltas: HashMap::default(),
            root_ordering_windows: HashMap::default(),
        };

        for node in durable_nodes {
            evaluator.update_node(node)?;
        }

        Ok(())
    }
}
