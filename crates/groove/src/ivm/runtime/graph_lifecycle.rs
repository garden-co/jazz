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
        self.runtime.collect_unretained_ephemeral_nodes();
    }
}

impl IvmRuntime {
    /// Cache diagnostics count attempted work, not installed semantic state.
    pub fn execution_layout_stats(&self) -> ExecutionLayoutStats {
        let (builds, hits) = self.graph.execution_layout_counters();
        ExecutionLayoutStats { builds, hits }
    }

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

        // A queued evaluation has already discovered every node it may resume
        // through. Its graph slice must remain live even when the public
        // subscription that originally retained a root has just been closed.
        // Traverse each queued node after normal roots so a continuation also
        // retains the graph dependencies it may revisit on resume.
        for node in self.pending_incremental.registered_nodes() {
            self.graph.mark_ancestors(node, &mut retained);
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
            hydration_memo_entries: self.eval_memo.len() - self.eval_memo.tick_entries(),
            eval_memo_bytes: self.tracked_eval_memo_bytes(),
            hydration_memo_hits: self.hydration_memo_hits,
            hydration_memo_computes: self.hydration_memo_computes,
            hydration_memo_distinct_computed_nodes: self.hydration_memo_computed_nodes.len(),
            logical_nodes_requested: self.logical_nodes_requested,
            deduped_graph_nodes: self.graph.nodes().len(),
            ..RuntimeStats::default()
        }
    }

    /// `eval_memo_bytes` is maintained at every memo mutation; debug builds
    /// check it against a full recount.
    fn tracked_eval_memo_bytes(&self) -> usize {
        debug_assert_eq!(
            self.eval_memo_bytes,
            self.eval_memo
                .values()
                .map(|entry| entry.payload_bytes)
                .sum::<usize>()
        );
        self.eval_memo_bytes
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
            removed |= self.remove_retainer(
                output.node,
                &Retainer::Hydration(subscription_id.retainer_key()),
            );
        }
        self.collect_unretained_ephemeral_nodes();
        removed
    }

    pub(super) fn remove_retainer(&mut self, id: NodeId, retainer: &Retainer) -> bool {
        // Conservatively a GC candidate even when nothing was removed: the
        // sweep re-checks reachability, and a spare candidate costs only its
        // ancestor closure.
        self.gc_candidates.insert(id);
        self.node_meta
            .get_mut(&id)
            .map(|meta| meta.retainers.remove(retainer))
            .unwrap_or(false)
    }

    fn is_gc_root(&self, node: &crate::ivm::GraphNode, queued: &HashSet<NodeId>) -> bool {
        node.is_durable()
            || queued.contains(&node.id)
            || self
                .node_meta
                .get(&node.id)
                .is_some_and(|meta| !meta.retainers.is_empty())
    }

    /// Remove every node that no durable node, retained node or queued
    /// evaluation node reaches through its inputs.
    ///
    /// Only candidates and their ancestors can have become unreachable since
    /// the previous sweep: new nodes, and roots that lost retainers or queued
    /// work. A node in that ancestor closure stays when it is a root itself or
    /// any consumer stays; consumers outside the closure are still reachable
    /// from their unchanged roots. Returns the removed nodes and whether some
    /// candidate was only kept alive by queued evaluation work.
    fn gc_ephemeral_nodes(&mut self) -> (Vec<NodeId>, bool) {
        let mut candidates = std::mem::take(&mut self.gc_candidates);
        candidates.extend(self.graph.take_added_nodes());
        let mut closure = HashSet::new();
        for &candidate in &candidates {
            self.graph.mark_ancestors(candidate, &mut closure);
        }
        closure.retain(|id| self.graph.node(*id).is_some());
        if closure.is_empty() {
            return (Vec::new(), false);
        }
        let queued = if self.pending_incremental.is_pending() {
            self.pending_incremental.registered_nodes()
        } else {
            HashSet::new()
        };

        // Decide consumers before their inputs: an iterative post-order over
        // `children`, restricted to the closure (the graph is acyclic).
        let mut kept = HashMap::<NodeId, bool>::default();
        let mut blocked_by_queue = false;
        for &start in &closure {
            if kept.contains_key(&start) {
                continue;
            }
            let mut stack = vec![(start, false)];
            while let Some((id, children_done)) = stack.pop() {
                if kept.contains_key(&id) {
                    continue;
                }
                let node = self.graph.node(id).expect("closure holds live nodes");
                if !children_done {
                    stack.push((id, true));
                    for child in &node.children {
                        if closure.contains(child) && !kept.contains_key(child) {
                            stack.push((*child, false));
                        }
                    }
                    continue;
                }
                let root = self.is_gc_root(node, &queued);
                blocked_by_queue |= queued.contains(&id);
                let keep = root
                    || node
                        .children
                        .iter()
                        .any(|child| !closure.contains(child) || kept[child]);
                kept.insert(id, keep);
            }
        }

        let removable = kept
            .into_iter()
            .filter_map(|(id, keep)| (!keep).then_some(id))
            .collect::<Vec<_>>();
        for id in &removable {
            self.graph.remove_node(*id);
        }
        if blocked_by_queue {
            // Queued work releases its nodes without a lifecycle event, so
            // revisit these candidates on the next sweep.
            candidates.retain(|id| self.graph.node(*id).is_some());
            self.gc_candidates.extend(candidates);
        }
        (removable, blocked_by_queue)
    }

    /// Debug builds check the incremental sweep against a full reachability
    /// pass: no unreachable node may survive a sweep that nothing blocked.
    #[cfg(debug_assertions)]
    fn debug_assert_no_unreachable_nodes(&self) {
        let retained = self.retained_node_ids();
        let leaked = self
            .graph
            .nodes()
            .values()
            .filter(|node| !node.is_durable() && !retained.contains(&node.id))
            .map(|node| node.id)
            .collect::<Vec<_>>();
        debug_assert!(
            leaked.is_empty(),
            "incremental graph GC left unreachable nodes: {leaked:?}"
        );
    }

    /// Reclaim unretained nodes only while all pending evaluator queues are
    /// visible through `pending_incremental`. During a poll that queue lives
    /// in a local variable, so reclaiming then could invalidate an in-flight
    /// evaluation before it resumes.
    pub(super) fn collect_unretained_ephemeral_nodes(&mut self) {
        self.ephemeral_graph_gc_pending = true;
        if self.pending_incremental_polling {
            return;
        }
        let (removed, blocked_by_queue) = self.gc_ephemeral_nodes();
        if !removed.is_empty() {
            let removed: HashSet<_> = removed.into_iter().collect();
            // Reclaim a batch with one pass per state map, not one full scan
            // for every expired node. Shared and suspended nodes are retained
            // by gc_ephemeral_nodes before this set is constructed.
            self.operator_states
                .retain(|key, _| !removed.contains(&key.node));
            self.arrangement_states
                .retain(|key, _| !removed.contains(&key.input));
            let mut removed_bytes = 0usize;
            self.eval_memo.retain(|key, entry| {
                let keep = !removed.contains(&key.node);
                if !keep {
                    removed_bytes = removed_bytes.saturating_add(entry.payload_bytes);
                }
                keep
            });
            self.eval_memo_bytes = self.eval_memo_bytes.saturating_sub(removed_bytes);
            for node in removed {
                self.arrangement_keys_by_input.remove(&node);
                self.node_meta.remove(&node);
            }
        }
        if !blocked_by_queue {
            #[cfg(debug_assertions)]
            self.debug_assert_no_unreachable_nodes();
            self.ephemeral_graph_gc_pending = false;
        }
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
