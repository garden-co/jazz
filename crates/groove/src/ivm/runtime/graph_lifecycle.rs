//! Graph retention, runtime statistics, garbage collection, and node initialization.

use super::*;

/// Owns unretained graph additions until an operation promotes them by adding
/// retainers. Cancellation or failure eagerly collects only ephemeral nodes;
/// hash-equal nodes retained by unrelated live operations remain installed.
pub(super) struct EphemeralGraphInstall<'a> {
    runtime: &'a mut IvmRuntime,
    committed: bool,
}

impl<'a> EphemeralGraphInstall<'a> {
    pub(super) fn new(runtime: &'a mut IvmRuntime) -> Self {
        Self {
            runtime,
            committed: false,
        }
    }

    pub(super) fn runtime(&mut self) -> &mut IvmRuntime {
        self.runtime
    }

    /// Promote this provisional installation after its roots have retainers.
    ///
    /// Successful installs need no cleanup: graph nodes are hash-shared and
    /// reachable from the newly retained roots. Cancellation and failure keep
    /// the eager cleanup path until arrangement ownership is operation-scoped.
    pub(super) fn commit(&mut self) {
        self.committed = true;
    }
}

impl Drop for EphemeralGraphInstall<'_> {
    fn drop(&mut self) {
        if self.committed {
            return;
        }
        for node in self.runtime.gc_ephemeral_nodes(0) {
            self.runtime.remove_node_runtime(node);
        }
    }
}

impl IvmRuntime {
    pub fn retained_node_ids(&self) -> HashSet<NodeId> {
        let mut retained = HashSet::new();
        let roots = self
            .graph
            .nodes()
            .values()
            .filter(|node| {
                node.is_durable()
                    || self
                        .node_meta
                        .get(&node.id)
                        .is_some_and(|meta| !meta.retainers.is_empty())
            })
            .map(|node| node.id)
            .collect::<Vec<_>>();

        for root in roots {
            self.graph.mark_ancestors(root, &mut retained);
        }

        retained
    }

    pub fn stats(&self) -> RuntimeStats {
        let mut stats = self.cheap_stats();
        for arrangement in self.arrangement_states.values() {
            stats.arrangement_rows += arrangement.value().row_count();
            stats.arrangement_encoded_bytes += arrangement.value().encoded_bytes();
        }
        for state in self.operator_states.values() {
            let OperatorState::Recursive(recursive) = state else {
                continue;
            };
            stats.recursive_state_count += 1;
            stats.recursive_accumulated_rows += recursive.value().accumulated_row_count();
            stats.recursive_accumulated_encoded_bytes +=
                recursive.value().accumulated_encoded_bytes();
        }
        stats
    }

    #[cfg(test)]
    pub(crate) fn top_by_retained_group_count(&self) -> usize {
        self.operator_states
            .values()
            .filter_map(|state| match state {
                OperatorState::TopBy(state) => Some(state.value().group_count()),
                _ => None,
            })
            .sum()
    }

    pub(super) fn cheap_stats(&self) -> RuntimeStats {
        RuntimeStats {
            graph_nodes: self.graph.nodes().len(),
            active_subscriptions: self.multisink_subscriptions.len(),
            active_prepared_shapes: self.prepared_shapes.len(),
            active_shape_params: self
                .binding_sources
                .values()
                .map(|source| source.refcounts.len())
                .sum(),
            arrangement_count: self.arrangement_states.len(),
            eval_memo_entries: self.eval_memo.len(),
            hydration_memo_entries: self
                .eval_memo
                .keys()
                .filter(|key| key.tick_epoch.is_none())
                .count(),
            eval_memo_bytes: self
                .eval_memo
                .values()
                .map(|entry| entry.payload_bytes)
                .sum(),
            hydration_memo_hits: self.hydration_memo_hits,
            hydration_memo_computes: self.hydration_memo_computes,
            hydration_memo_distinct_computed_nodes: self.hydration_memo_computed_nodes.len(),
            logical_nodes_requested: self.logical_nodes_requested,
            deduped_graph_nodes: self.graph.nodes().len(),
            ..RuntimeStats::default()
        }
    }

    pub(super) fn record_hydration_memo_metrics(&mut self, metrics: &TickMetrics) {
        self.hydration_memo_hits += metrics.hydration_memo_hits;
        self.hydration_memo_computes += metrics.hydration_memo_computes;
        self.hydration_memo_computed_nodes
            .extend(metrics.hydration_memo_computed_nodes.iter().copied());
    }

    pub(super) fn add_retainer(&mut self, id: NodeId, retainer: Retainer) -> bool {
        if self.graph.node(id).is_none() {
            return false;
        }
        let meta = self.node_meta.entry(id).or_default();
        meta.last_used_tick = self.current_tick;
        meta.retainers.insert(retainer)
    }

    pub(super) fn retain_as_subscription(
        &mut self,
        subscription_id: SubscriptionId,
        output_node: NodeId,
    ) -> bool {
        self.add_retainer(
            output_node,
            Retainer::Subscription(subscription_id.retainer_key()),
        )
    }

    pub(super) fn remove_multisink_retainers(
        &mut self,
        subscription_id: SubscriptionId,
        outputs: &BTreeMap<String, CompiledNode>,
    ) -> bool {
        let mut removed = false;
        for output in outputs.values() {
            removed |= self.remove_retainer(
                output.node,
                &Retainer::Subscription(subscription_id.retainer_key()),
            );
        }
        for node in self.gc_ephemeral_nodes(0) {
            self.remove_node_runtime(node);
        }
        removed
    }

    pub(super) fn remove_retainer(&mut self, id: NodeId, retainer: &Retainer) -> bool {
        self.node_meta
            .get_mut(&id)
            .map(|meta| meta.retainers.remove(retainer))
            .unwrap_or(false)
    }

    pub(super) fn gc_ephemeral_nodes(&mut self, ttl_ticks: u64) -> Vec<NodeId> {
        let retained = self.retained_node_ids();
        let remove_before_tick = self.current_tick.saturating_sub(ttl_ticks);
        let removable = self
            .graph
            .nodes()
            .values()
            .filter(|node| {
                !node.is_durable()
                    && !retained.contains(&node.id)
                    && self
                        .node_meta
                        .get(&node.id)
                        .is_none_or(|meta| meta.last_used_tick <= remove_before_tick)
            })
            .map(|node| node.id)
            .collect::<Vec<_>>();

        for id in &removable {
            self.graph.remove_node(*id);
        }

        removable
    }

    pub(super) fn remove_node_runtime(&mut self, node: NodeId) {
        self.operator_states.retain(|key, _| key.node != node);
        self.arrangement_states.retain(|key, _| key.input != node);
        self.arrangement_keys_by_input.remove(&node);
        self.eval_memo.retain(|key, _| key.node != node);
        self.node_meta.remove(&node);
    }

    pub(super) fn affected_recursive_nodes_are_current(
        &self,
        affected: &std::collections::HashSet<NodeId>,
        current_tick: u64,
    ) -> bool {
        affected.iter().all(|node| {
            self.operator_states
                .get(&OperatorStateKey {
                    scope: ScopeId::root(),
                    node: *node,
                })
                .is_none_or(|state| {
                    !matches!(state, OperatorState::Recursive(_))
                        || matches!(
                            state,
                            OperatorState::Recursive(recursive)
                                if recursive.as_of() == Some(Tick(current_tick))
                        )
                })
        })
    }

    pub(super) fn initialize_node_runtime(&mut self, node: NodeId) {
        self.node_meta.entry(node).or_default();
        let Some(graph_node) = self.graph.node(node) else {
            return;
        };
        let operator = &graph_node.descriptor.operator;
        let operator_state = operator_state_for(operator);
        if !matches!(operator_state, OperatorState::Stateless) {
            self.operator_states
                .entry(OperatorStateKey {
                    scope: ScopeId::root(),
                    node,
                })
                .or_insert(operator_state);
        }
    }
}
