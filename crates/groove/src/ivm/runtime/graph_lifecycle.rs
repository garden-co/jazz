//! Graph retention, runtime statistics, garbage collection, and node initialization.

use super::*;

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
        self.prune_unreferenced_arrangements();
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
        self.eval_memo.retain(|key, _| key.node != node);
        self.node_meta.remove(&node);
    }

    pub(super) fn prune_unreferenced_arrangements(&mut self) {
        let mut referenced = HashSet::new();
        for node in self.graph.nodes().values() {
            match &node.descriptor.operator {
                OpType::Join(join) | OpType::SemiJoin(join) | OpType::AntiJoin(join) => {
                    if let [left, right] = node.descriptor.inputs.as_slice() {
                        referenced.insert(ArrangementKey {
                            scope: ScopeId::root(),
                            input: *left,
                            fields: Arc::from(plan_expr_names(&join.left_key)),
                            descriptor: join.left_descriptor,
                            comparison: join.comparison,
                        });
                        referenced.insert(ArrangementKey {
                            scope: ScopeId::root(),
                            input: *right,
                            fields: Arc::from(plan_expr_names(&join.right_key)),
                            descriptor: join.right_descriptor,
                            comparison: join.comparison,
                        });
                    }
                }
                OpType::ArgMaxBy(arg_by) => {
                    if let [input] = node.descriptor.inputs.as_slice() {
                        referenced.insert(ArrangementKey {
                            scope: ScopeId::root(),
                            input: *input,
                            fields: Arc::from(arg_by.group_fields.clone()),
                            descriptor: node.descriptor.output,
                            comparison: ValueComparison::Exact,
                        });
                    }
                }
                OpType::ArgMinBy(arg_by) => {
                    if let [input] = node.descriptor.inputs.as_slice() {
                        referenced.insert(ArrangementKey {
                            scope: ScopeId::root(),
                            input: *input,
                            fields: Arc::from(arg_by.group_fields.clone()),
                            descriptor: node.descriptor.output,
                            comparison: ValueComparison::Exact,
                        });
                    }
                }
                OpType::TopBy(top_by) => {
                    if let [input] = node.descriptor.inputs.as_slice() {
                        referenced.insert(ArrangementKey {
                            scope: ScopeId::root(),
                            input: *input,
                            fields: Arc::from(top_by.group_fields.clone()),
                            descriptor: node.descriptor.output,
                            comparison: ValueComparison::Exact,
                        });
                    }
                }
                OpType::CollectBy(collect_by) => {
                    if let [input] = node.descriptor.inputs.as_slice()
                        && let Some(input_node) = self.graph.node(*input)
                    {
                        referenced.insert(ArrangementKey {
                            scope: ScopeId::root(),
                            input: *input,
                            fields: Arc::from(collect_by.group_fields.clone()),
                            descriptor: input_node.descriptor.output,
                            comparison: ValueComparison::Exact,
                        });
                    }
                }
                OpType::Aggregate(aggregate) => {
                    if let [input] = node.descriptor.inputs.as_slice()
                        && let Some(input_node) = self.graph.node(*input)
                    {
                        referenced.insert(ArrangementKey {
                            scope: ScopeId::root(),
                            input: *input,
                            fields: Arc::from(plan_expr_names(&aggregate.group_key)),
                            descriptor: input_node.descriptor.output,
                            comparison: ValueComparison::Exact,
                        });
                    }
                }
                _ => {}
            }
        }
        self.arrangement_states.retain(|key, _| {
            referenced.iter().any(|referenced| {
                referenced.input == key.input
                    && referenced.fields == key.fields
                    && referenced.descriptor == key.descriptor
            })
        });
    }

    pub(super) fn retained_recursive_nodes_are_current(&self, current_tick: u64) -> bool {
        let retained = self.retained_node_ids();
        self.operator_states.iter().all(|(key, state)| {
            !retained.contains(&key.node)
                || !matches!(state, OperatorState::Recursive(_))
                || matches!(
                    state,
                    OperatorState::Recursive(recursive) if recursive.as_of() == Some(Tick(current_tick))
                )
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
