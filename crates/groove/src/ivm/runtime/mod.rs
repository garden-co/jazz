//! Synchronous IVM graph runtime and tick-loop narrative.
//!
//! This module owns executable state for hash-consed graphs: subscriptions,
//! prepared-shape bindings, durable index nodes, per-operator state, reusable
//! join arrangements, recursive state, and per-tick memoization. The reading
//! order is tick-loop first: start at [`IvmRuntime::tick_with_params`], then
//! follow [`TickEvaluator::update_node`] to operator evaluation. Subscription
//! setup, graph insertion, retainers, and GC live after that narrative. Query
//! lowering lives in [`crate::ivm::planner`], graph identity in
//! [`crate::ivm::graph`], and storage mechanics in [`crate::storage`].

use std::cell::RefCell;
use std::collections::hash_map::DefaultHasher;
use std::collections::{BTreeMap, HashMap, HashSet};
use std::hash::{Hash, Hasher};
use std::sync::mpsc::{self, Receiver, RecvError, Sender, TryRecvError};

use crate::ivm::{
    ArgMaxByOp, ArgMinByOp, BindingSourceOp, DurableStorage, FieldRef, FilterOp, FrontierName,
    FrontierSourceOp, GraphBuilder, IndexByOp, InlineRecordsOp, IvmGraph, JoinOp, JoinOpKind,
    LiteralValue, MapProjectOp, NodeDescriptor, NodeDurability, NodeId, OpType, PersistOp,
    PlanExpr, PredicateExpr, ProjectExpr, ProjectField, ProjectionExpr, RecursiveOp, Retainer,
    TableSourceOp, TopByDirection, TopByOp, TopByOrderField, UnwrapNullableOp,
};
use crate::records::{self, BorrowedRecord, RecordDescriptor, Value, ValueType};
use crate::schema::{DatabaseSchema, IndexSchema, TableSchema};
use crate::storage::{OrderedKvStorage, OwnedWriteOperation, StagedWriteOverlay};
use thiserror::Error;

mod join;
mod persist;
mod recursion;

use join::{AntiJoinState, ArrangementState, JoinState};
use persist::apply_persist_delta;
use recursion::{
    RecursiveState, hydrate_recursive_arrangements, recompute_recursive, recursive_delta,
    snapshot_table_deltas,
};

/// Stateful executor for deduplicated IVM graphs and subscriptions.
#[derive(Clone, Debug)]
pub struct IvmRuntime {
    schema: DatabaseSchema,
    table_descriptors: HashMap<String, RecordDescriptor>,
    graph: IvmGraph,
    subscriptions: HashMap<SubscriptionId, SubscriptionState>,
    prepared_shapes: HashMap<PreparedShapeId, PreparedShapeState>,
    auto_direct_families: HashMap<AutoDirectFamilyKey, PreparedShapeId>,
    binding_sources: HashMap<String, BindingSourceState>,
    /// Binding retractions discovered while routing notifications cannot tick
    /// recursively; the next public tick drains them before user deltas run.
    pending_binding_retractions: Vec<BindingDelta>,
    /// Persistent operator state keyed by scope and node. This survives ticks;
    /// see [`EvalMemoKey`] for per-evaluation caching.
    operator_states: HashMap<OperatorStateKey, OperatorState>,
    /// Reusable indexed multisets for join inputs. These are keyed by input
    /// fragment, key fields, descriptor, and scope so similar queries can share
    /// expensive context-independent arrangements.
    arrangement_states: HashMap<ArrangementKey, AsOf<ArrangementState, SubTick>>,
    /// Per-tick/subtick memoization. Cleared after each durable/subscription
    /// pass; it must not own join or recursive state.
    eval_memo: HashMap<EvalMemoKey, RecordDeltas>,
    /// Retainers and GC age live outside operator state so stateless leaf nodes
    /// can be retained without allocating fake operator state.
    node_meta: HashMap<NodeId, NodeRuntimeMeta>,
    current_tick: u64,
    next_subscription_id: u64,
    next_shape_id: u64,
    logical_nodes_requested: u64,
    auto_direct_family_enabled: bool,
}

impl IvmRuntime {
    pub fn new(schema: DatabaseSchema) -> Result<Self, IvmRuntimeError> {
        let table_descriptors = schema
            .tables
            .iter()
            .map(|table| (table.name.clone(), table.record_schema()))
            .collect();
        let mut runtime = Self {
            schema,
            table_descriptors,
            graph: IvmGraph::new(),
            subscriptions: HashMap::new(),
            operator_states: HashMap::new(),
            arrangement_states: HashMap::new(),
            eval_memo: HashMap::new(),
            node_meta: HashMap::new(),
            current_tick: 0,
            next_subscription_id: 1,
            next_shape_id: 1,
            logical_nodes_requested: 0,
            auto_direct_family_enabled: true,
            prepared_shapes: HashMap::new(),
            auto_direct_families: HashMap::new(),
            binding_sources: HashMap::new(),
            pending_binding_retractions: Vec::new(),
        };
        runtime.add_dedup_schema_indices()?;
        Ok(runtime)
    }

    pub fn graph(&self) -> &IvmGraph {
        &self.graph
    }

    pub fn set_auto_direct_family_enabled(&mut self, enabled: bool) {
        self.auto_direct_family_enabled = enabled;
    }

    pub fn schema(&self) -> &DatabaseSchema {
        &self.schema
    }

    pub fn table(&self, table: &str) -> Option<&TableSchema> {
        self.schema.table(table)
    }

    pub fn table_descriptor(&self, table: &str) -> Option<&RecordDescriptor> {
        self.table_descriptors.get(table)
    }

    pub fn index(&self, table: &str, index_name: &str) -> Option<&IndexSchema> {
        self.table(table)?
            .indices
            .iter()
            .find(|index| index.name == index_name)
    }

    pub fn direct_record_store(
        &self,
        store: &str,
    ) -> Option<&crate::schema::DirectRecordStoreSchema> {
        self.schema.direct_record_store(store)
    }

    pub fn query_snapshot<S>(
        &mut self,
        graph: GraphBuilder,
        storage: &S,
    ) -> Result<RecordDeltas, IvmRuntimeError>
    where
        S: OrderedKvStorage,
    {
        if builder_contains_binding_source(&graph) {
            return Err(IvmRuntimeError::BindingSourceRequiresPrepare);
        }
        self.logical_nodes_requested += count_builder_nodes(&graph) as u64;
        let CompiledNode {
            output,
            node: output_node,
        } = self.add_dedup_graph(&graph)?;
        let records = self.hydration_snapshot(output_node, storage);
        for node in self.gc_ephemeral_nodes(0) {
            self.remove_node_runtime(node);
        }
        self.prune_unreferenced_arrangements();
        let records = records?;
        if records.descriptor != output {
            return Err(IvmRuntimeError::GraphOutputMismatch);
        }
        Ok(records)
    }

    pub fn tick<S>(
        &mut self,
        table_deltas: Vec<TableDelta>,
        storage: &S,
    ) -> Result<TickMetrics, IvmRuntimeError>
    where
        S: OrderedKvStorage,
    {
        self.tick_with_params(table_deltas, Vec::new(), storage)
    }

    pub(crate) fn tick_staged<S>(
        &mut self,
        table_deltas: Vec<TableDelta>,
        storage: &S,
        staged_writes: &mut Vec<OwnedWriteOperation>,
    ) -> Result<TickMetrics, IvmRuntimeError>
    where
        S: OrderedKvStorage,
    {
        let staged_overlay = RefCell::new(std::mem::take(staged_writes));
        let overlay = StagedWriteOverlay::new(storage, &staged_overlay);
        let tick = self.tick_with_params(table_deltas, Vec::new(), &overlay);
        overlay.drain_into(staged_writes);
        tick
    }

    fn tick_with_params<S>(
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
        let current_tick = self.advance_tick();
        let table_delta_records = table_deltas
            .iter()
            .map(|delta| delta.deltas.len())
            .sum::<usize>();
        self.tick_durable_nodes(&table_deltas, current_tick, storage)?;
        let mut dropped_subscriptions = Vec::new();
        let mut metrics = TickMetrics {
            tick: current_tick,
            table_delta_records,
            ..TickMetrics::default()
        };
        let binding_snapshots = self.binding_snapshot_deltas();
        let mut evaluator = TickEvaluator {
            graph: &self.graph,
            table_deltas: &table_deltas,
            binding_deltas: &binding_deltas,
            binding_snapshots: &binding_snapshots,
            current_tick,
            operator_states: &mut self.operator_states,
            arrangement_states: &mut self.arrangement_states,
            eval_memo: &mut self.eval_memo,
            storage: Some(storage),
            context: EvalContext::root(),
            metrics: &mut metrics,
        };

        for (subscription_id, subscription) in self
            .subscriptions
            .iter()
            .filter(|(_, subscription)| subscription.target.is_direct())
        {
            let SubscriptionTarget::Direct { output } = &subscription.target else {
                unreachable!("filtered to direct subscriptions");
            };
            let records = evaluator.update_node(output.node)?;
            if !records.deltas.is_empty() && records.descriptor != output.output {
                return Err(IvmRuntimeError::GraphOutputMismatch);
            }
            if !records.is_empty() {
                evaluator.metrics.notifications_sent += 1;
                evaluator.metrics.notification_records += records.deltas.len();
                evaluator.metrics.notification_encoded_bytes +=
                    record_deltas_encoded_bytes(&records);
            }
            if !records.is_empty() && subscription.sender.send(records).is_err() {
                dropped_subscriptions.push(*subscription_id);
            }
        }
        let shape_ids = self.prepared_shapes.keys().copied().collect::<Vec<_>>();
        let mut shape_outputs = Vec::new();
        for shape_id in shape_ids {
            let Some(shape) = self.prepared_shapes.get(&shape_id) else {
                continue;
            };
            if let Some(routing) = &shape.routing {
                evaluator.update_node(shape.output.node)?;
                let records = evaluator.update_node(routing.output.node)?;
                shape_outputs.push((shape_id, records));
                continue;
            }
            let records = evaluator.update_node(shape.output.node)?;
            shape_outputs.push((shape_id, records));
        }
        drop(evaluator);

        for (shape_id, records) in shape_outputs {
            let Some(shape) = self.prepared_shapes.get_mut(&shape_id) else {
                continue;
            };
            let notifications = route_shape_records(shape, records)?;
            for (subscription_id, records) in notifications {
                if !records.is_empty() {
                    metrics.notifications_sent += 1;
                    metrics.notification_records += records.deltas.len();
                    metrics.notification_encoded_bytes += record_deltas_encoded_bytes(&records);
                }
                if !records.is_empty()
                    && self
                        .subscriptions
                        .get(&subscription_id)
                        .is_some_and(|subscription| subscription.sender.send(records).is_err())
                {
                    dropped_subscriptions.push(subscription_id);
                }
            }
        }

        for subscription_id in dropped_subscriptions {
            self.unsubscribe(subscription_id);
        }
        debug_assert!(self.retained_recursive_nodes_are_current(current_tick));
        self.eval_memo.clear();
        metrics.runtime_stats = self.stats();
        Ok(metrics)
    }

    fn hydration_snapshot<S>(
        &mut self,
        output_node: NodeId,
        storage: &S,
    ) -> Result<RecordDeltas, IvmRuntimeError>
    where
        S: OrderedKvStorage,
    {
        let table_deltas = snapshot_table_deltas(&self.graph, storage, output_node)?;
        let binding_snapshots = self.binding_snapshot_deltas();
        let mut eval_memo = HashMap::new();
        let mut metrics = TickMetrics::default();
        let mut evaluator = TickEvaluator {
            graph: &self.graph,
            table_deltas: &table_deltas,
            binding_deltas: &[],
            binding_snapshots: &binding_snapshots,
            current_tick: self.current_tick,
            operator_states: &mut self.operator_states,
            arrangement_states: &mut self.arrangement_states,
            eval_memo: &mut eval_memo,
            storage: Some(storage),
            context: EvalContext::root_snapshot(),
            metrics: &mut metrics,
        };
        evaluator.update_node(output_node)
    }

    fn hydrate_shape_graph<S>(
        &mut self,
        output_node: NodeId,
        storage: &S,
    ) -> Result<(), IvmRuntimeError>
    where
        S: OrderedKvStorage,
    {
        let table_deltas = snapshot_table_deltas(&self.graph, storage, output_node)?;
        let binding_snapshots = self.binding_snapshot_deltas();
        let mut eval_memo = HashMap::new();
        let mut metrics = TickMetrics::default();
        let mut evaluator = TickEvaluator {
            graph: &self.graph,
            table_deltas: &table_deltas,
            binding_deltas: &[],
            binding_snapshots: &binding_snapshots,
            current_tick: self.current_tick,
            operator_states: &mut self.operator_states,
            arrangement_states: &mut self.arrangement_states,
            eval_memo: &mut eval_memo,
            storage: Some(storage),
            context: EvalContext {
                scope: ScopePath::root(),
                sub_tick: 0,
                bindings: HashMap::new(),
                arrangement_update_mode: ArrangementUpdateMode::Replace,
                eval_mode: EvalMode::Tick,
            },
            metrics: &mut metrics,
        };
        let mut ancestors = HashSet::new();
        evaluator.graph.mark_ancestors(output_node, &mut ancestors);
        for node in ancestors {
            evaluator.update_node(node)?;
        }
        Ok(())
    }

    fn hydrate_prepared_shape_routes<S>(
        &mut self,
        shape_id: PreparedShapeId,
        storage: &S,
    ) -> Result<(), IvmRuntimeError>
    where
        S: OrderedKvStorage,
    {
        let output_node = {
            let shape = self
                .prepared_shapes
                .get(&shape_id)
                .ok_or(IvmRuntimeError::PreparedShapeNotFound(shape_id))?;
            shape
                .routing
                .as_ref()
                .map(|routing| routing.output.node)
                .unwrap_or(shape.output.node)
        };
        let records = self.hydration_snapshot(output_node, storage)?;
        let Some(shape) = self.prepared_shapes.get_mut(&shape_id) else {
            return Err(IvmRuntimeError::PreparedShapeNotFound(shape_id));
        };
        route_shape_records(shape, records)?;
        Ok(())
    }

    fn tick_durable_nodes<S>(
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
        let mut evaluator = TickEvaluator {
            graph: &self.graph,
            table_deltas,
            binding_deltas: &[],
            binding_snapshots: &binding_snapshots,
            current_tick,
            operator_states: &mut self.operator_states,
            arrangement_states: &mut self.arrangement_states,
            eval_memo: &mut self.eval_memo,
            storage: Some(storage),
            context: EvalContext::root(),
            metrics: &mut metrics,
        };

        for node in durable_nodes {
            evaluator.update_node(node)?;
        }

        Ok(())
    }

    pub fn subscribe(
        &mut self,
        graph: GraphBuilder,
        storage: &impl OrderedKvStorage,
    ) -> Result<Subscription, IvmRuntimeError> {
        if builder_contains_binding_source(&graph) {
            return Err(IvmRuntimeError::BindingSourceRequiresPrepare);
        }
        if self.auto_direct_family_enabled
            && let Some(plan) = self.plan_auto_direct_family(&graph)?
        {
            let shape_id = if let Some(shape_id) = self.auto_direct_families.get(&plan.key).copied()
            {
                shape_id
            } else {
                let shape = self.prepare(
                    plan.graph.clone(),
                    plan.shape.clone(),
                    plan.binding_descriptor,
                    [plan.binding_field.clone()],
                    storage,
                )?;
                if let Some(state) = self.prepared_shapes.get_mut(&shape.id()) {
                    state.auto_family_key = Some(plan.key.clone());
                }
                self.auto_direct_families
                    .insert(plan.key.clone(), shape.id());
                shape.id()
            };
            return self.bind_shape_projected(
                shape_id,
                &[plan.binding_value],
                Some(plan.notification_projection),
                storage,
            );
        }
        self.logical_nodes_requested += count_builder_nodes(&graph) as u64;
        let CompiledNode {
            output,
            node: output_node,
        } = self.add_dedup_graph(&graph)?;
        let subscription_id = self.next_subscription_id();
        let (sender, receiver) = mpsc::channel();
        let retained = self.retain_as_subscription(subscription_id, output_node);
        debug_assert!(retained);
        self.subscriptions.insert(
            subscription_id,
            SubscriptionState {
                sender,
                target: SubscriptionTarget::Direct {
                    output: CompiledNode {
                        output,
                        node: output_node,
                    },
                },
            },
        );
        let initial = match self.hydration_snapshot(output_node, storage) {
            Ok(initial) => initial,
            Err(error) => {
                self.unsubscribe(subscription_id);
                return Err(error);
            }
        };
        let sent = self
            .subscriptions
            .get(&subscription_id)
            .is_some_and(|subscription| subscription.sender.send(initial).is_ok());
        if !sent {
            self.unsubscribe(subscription_id);
        }
        Ok(Subscription {
            id: subscription_id,
            receiver,
        })
    }

    pub fn prepare(
        &mut self,
        graph: GraphBuilder,
        binding_source_shape: impl Into<String>,
        binding_descriptor: RecordDescriptor,
        output_key_fields: impl IntoIterator<Item = impl Into<String>>,
        storage: &impl OrderedKvStorage,
    ) -> Result<PreparedShape, IvmRuntimeError> {
        if !self.pending_binding_retractions.is_empty() {
            self.tick_with_params(Vec::new(), Vec::new(), storage)?;
        }
        self.logical_nodes_requested += count_builder_nodes(&graph) as u64;
        let CompiledNode {
            output,
            node: output_node,
        } = self.add_dedup_graph(&graph)?;
        let key_fields = output_key_fields
            .into_iter()
            .map(|field| {
                let field = field.into();
                output
                    .field_index(&field)
                    .ok_or(IvmRuntimeError::ShapeKeyFieldNotFound(field))
            })
            .collect::<Result<Vec<_>, _>>()?;
        let shape = binding_source_shape.into();
        let shape_id = self.next_shape_id();
        self.binding_sources
            .entry(shape.clone())
            .or_insert_with(|| BindingSourceState {
                descriptor: binding_descriptor,
                refcounts: HashMap::new(),
            });
        let retained = self.add_retainer(
            output_node,
            Retainer::PreparedShape(shape_id.retainer_key()),
        );
        debug_assert!(retained);
        self.prepared_shapes.insert(
            shape_id,
            PreparedShapeState {
                shape: shape.clone(),
                binding_descriptor,
                output: CompiledNode {
                    output,
                    node: output_node,
                },
                output_key_fields: key_fields,
                routing: None,
                bindings: HashMap::new(),
                auto_family_key: None,
            },
        );
        self.hydrate_shape_graph(output_node, storage)?;
        Ok(PreparedShape { id: shape_id })
    }

    pub fn prepare_with_routing(
        &mut self,
        output_graph: GraphBuilder,
        routing_graph: GraphBuilder,
        binding_source_shape: impl Into<String>,
        binding_descriptor: RecordDescriptor,
        routing_key_fields: impl IntoIterator<Item = impl Into<String>>,
        storage: &impl OrderedKvStorage,
    ) -> Result<PreparedShape, IvmRuntimeError> {
        if !self.pending_binding_retractions.is_empty() {
            self.tick_with_params(Vec::new(), Vec::new(), storage)?;
        }
        self.logical_nodes_requested +=
            (count_builder_nodes(&output_graph) + count_builder_nodes(&routing_graph)) as u64;
        let output = self.add_dedup_graph(&output_graph)?;
        let routing_output = self.add_dedup_graph(&routing_graph)?;
        let key_fields = routing_key_fields
            .into_iter()
            .map(|field| {
                let field = field.into();
                routing_output
                    .output
                    .field_index(&field)
                    .ok_or(IvmRuntimeError::ShapeKeyFieldNotFound(field))
            })
            .collect::<Result<Vec<_>, _>>()?;
        let notification_projection =
            notification_projection_from_output(&routing_output.output, output.output)?;
        let shape = binding_source_shape.into();
        let shape_id = self.next_shape_id();
        self.binding_sources
            .entry(shape.clone())
            .or_insert_with(|| BindingSourceState {
                descriptor: binding_descriptor,
                refcounts: HashMap::new(),
            });
        let retained_output = self.add_retainer(
            output.node,
            Retainer::PreparedShape(shape_id.retainer_key()),
        );
        let retained_routing = self.add_retainer(
            routing_output.node,
            Retainer::PreparedShape(shape_id.retainer_key()),
        );
        debug_assert!(retained_output || retained_routing);
        let output_node = output.node;
        let routing_node = routing_output.node;
        self.prepared_shapes.insert(
            shape_id,
            PreparedShapeState {
                shape: shape.clone(),
                binding_descriptor,
                output,
                output_key_fields: key_fields,
                routing: Some(PreparedShapeRouting {
                    output: routing_output,
                    notification_projection,
                }),
                bindings: HashMap::new(),
                auto_family_key: None,
            },
        );
        self.hydrate_shape_graph(output_node, storage)?;
        self.hydrate_shape_graph(routing_node, storage)?;
        Ok(PreparedShape { id: shape_id })
    }

    pub fn bind_shape<S>(
        &mut self,
        shape_id: PreparedShapeId,
        binding_values: &[Value],
        storage: &S,
    ) -> Result<Subscription, IvmRuntimeError>
    where
        S: OrderedKvStorage,
    {
        self.bind_shape_projected(shape_id, binding_values, None, storage)
    }

    pub(crate) fn bind_shape_with_output<S>(
        &mut self,
        shape_id: PreparedShapeId,
        binding_values: &[Value],
        public_output: RecordDescriptor,
        storage: &S,
    ) -> Result<Subscription, IvmRuntimeError>
    where
        S: OrderedKvStorage,
    {
        let projection = self.shape_notification_projection(shape_id, public_output)?;
        self.bind_shape_projected(shape_id, binding_values, Some(projection), storage)
    }

    fn bind_shape_projected<S>(
        &mut self,
        shape_id: PreparedShapeId,
        binding_values: &[Value],
        projection: Option<ShapeNotificationProjection>,
        storage: &S,
    ) -> Result<Subscription, IvmRuntimeError>
    where
        S: OrderedKvStorage,
    {
        if !self.pending_binding_retractions.is_empty() {
            self.tick_with_params(Vec::new(), Vec::new(), storage)?;
        }
        let shape = self
            .prepared_shapes
            .get(&shape_id)
            .ok_or(IvmRuntimeError::PreparedShapeNotFound(shape_id))?;
        let binding_record = shape.binding_descriptor.create(binding_values)?;
        let binding_key = BindingKey(binding_record.clone());
        let shape_binding_exists = shape.bindings.contains_key(&binding_key);
        let subscription_id = self.next_subscription_id();
        let (sender, receiver) = mpsc::channel();
        let binding_delta = self.add_binding_ref(shape_id, binding_key.clone())?;
        let is_new_binding = !binding_delta.deltas.is_empty();
        if !is_new_binding && !shape_binding_exists {
            self.hydrate_prepared_shape_routes(shape_id, storage)?;
        }
        if let Some(shape) = self.prepared_shapes.get_mut(&shape_id) {
            shape
                .bindings
                .entry(binding_key.clone())
                .or_default()
                .senders
                .insert(subscription_id, ShapeSender { projection });
        }
        self.subscriptions.insert(
            subscription_id,
            SubscriptionState {
                sender,
                target: SubscriptionTarget::Shape {
                    shape_id,
                    binding_key: binding_key.clone(),
                },
            },
        );
        if is_new_binding {
            self.tick_with_params(vec![], vec![binding_delta], storage)?;
            let snapshot = self.shape_materialized_snapshot_for_subscription(
                shape_id,
                &binding_key,
                subscription_id,
            )?;
            if snapshot.is_empty()
                && let Some(subscription) = self.subscriptions.get(&subscription_id)
            {
                let _ = subscription.sender.send(snapshot);
            }
        } else {
            let snapshot = self.shape_materialized_snapshot_for_subscription(
                shape_id,
                &binding_key,
                subscription_id,
            )?;
            if let Some(subscription) = self.subscriptions.get(&subscription_id) {
                let _ = subscription.sender.send(snapshot);
            }
        }
        let sent = self.subscriptions.contains_key(&subscription_id);
        if !sent {
            self.unsubscribe(subscription_id);
        }
        Ok(Subscription {
            id: subscription_id,
            receiver,
        })
    }

    pub fn unsubscribe(&mut self, subscription_id: SubscriptionId) -> bool {
        let Some(subscription) = self.subscriptions.remove(&subscription_id) else {
            return false;
        };

        if let SubscriptionTarget::Shape {
            shape_id,
            binding_key,
        } = subscription.target
        {
            self.unsubscribe_shape_subscription(shape_id, &binding_key, subscription_id);
            return true;
        }

        let SubscriptionTarget::Direct { output } = subscription.target else {
            unreachable!("shape subscriptions returned above");
        };
        let removed = self.remove_retainer(
            output.node,
            &Retainer::Subscription(subscription_id.retainer_key()),
        );
        for node in self.gc_ephemeral_nodes(0) {
            self.remove_node_runtime(node);
        }
        self.prune_unreferenced_arrangements();
        removed
    }

    pub fn unsubscribe_with_storage<S>(
        &mut self,
        subscription_id: SubscriptionId,
        storage: &S,
    ) -> Result<bool, IvmRuntimeError>
    where
        S: OrderedKvStorage,
    {
        let Some(subscription) = self.subscriptions.remove(&subscription_id) else {
            return Ok(false);
        };

        if let SubscriptionTarget::Shape {
            shape_id,
            binding_key,
        } = subscription.target
        {
            self.unsubscribe_shape_subscription(shape_id, &binding_key, subscription_id);
            self.tick_with_params(Vec::new(), Vec::new(), storage)?;
            return Ok(true);
        }

        let SubscriptionTarget::Direct { output } = subscription.target else {
            unreachable!("shape subscriptions returned above");
        };
        let removed = self.remove_retainer(
            output.node,
            &Retainer::Subscription(subscription_id.retainer_key()),
        );
        for node in self.gc_ephemeral_nodes(0) {
            self.remove_node_runtime(node);
        }
        self.prune_unreferenced_arrangements();
        Ok(removed)
    }

    pub fn add_dedup_schema_indices(&mut self) -> Result<(), IvmRuntimeError> {
        for table in self.schema.tables.clone() {
            for index in &table.indices {
                self.add_dedup_schema_index(&table, index)?;
            }
        }
        Ok(())
    }

    pub fn subscription_output_node(&self, subscription_id: SubscriptionId) -> Option<NodeId> {
        match &self.subscriptions.get(&subscription_id)?.target {
            SubscriptionTarget::Direct { output } => Some(output.node),
            SubscriptionTarget::Shape { shape_id, .. } => self
                .prepared_shapes
                .get(shape_id)
                .map(|shape| shape.output.node),
        }
    }

    pub fn subscription_output(
        &self,
        subscription_id: SubscriptionId,
    ) -> Option<&RecordDescriptor> {
        match &self.subscriptions.get(&subscription_id)?.target {
            SubscriptionTarget::Direct { output } => Some(&output.output),
            SubscriptionTarget::Shape {
                shape_id,
                binding_key,
            } => self.prepared_shapes.get(shape_id).and_then(|shape| {
                shape
                    .bindings
                    .get(binding_key)
                    .and_then(|binding| binding.senders.get(&subscription_id))
                    .and_then(|sender| sender.projection.as_ref())
                    .map(|projection| &projection.descriptor)
                    .or(Some(&shape.output.output))
            }),
        }
    }

    fn shape_notification_projection(
        &self,
        shape_id: PreparedShapeId,
        descriptor: RecordDescriptor,
    ) -> Result<ShapeNotificationProjection, IvmRuntimeError> {
        let shape = self
            .prepared_shapes
            .get(&shape_id)
            .ok_or(IvmRuntimeError::PreparedShapeNotFound(shape_id))?;
        notification_projection_from_output(&shape.output.output, descriptor)
    }

    fn plan_auto_direct_family(
        &self,
        graph: &GraphBuilder,
    ) -> Result<Option<AutoDirectFamilyPlan>, IvmRuntimeError> {
        if builder_contains_recursive(graph) {
            return Ok(None);
        }
        let original_output = self.infer_builder_output(graph)?;
        let binding_field = auto_direct_binding_field(graph, &original_output, self)?;
        let Some(lifted) = lift_literal_filter(self, graph, &binding_field)? else {
            return Ok(None);
        };
        let shape_seed = "__auto_direct_shape".to_owned();
        let graph = replace_binding_shape(lifted.graph, &shape_seed);
        let shape_output = self.infer_builder_output(&graph)?;
        let notification_mapping = original_output
            .fields()
            .iter()
            .enumerate()
            .map(|(index, _)| (0, index))
            .collect::<Vec<_>>();
        let notification_expressions = original_output
            .fields()
            .iter()
            .map(|field| {
                let name = field
                    .name
                    .clone()
                    .ok_or_else(|| IvmRuntimeError::GraphFieldNotFound("<unnamed>".to_owned()))?;
                Ok(ProjectionExpr {
                    expression: PlanExpr::field(name),
                    output_name: field.name.clone(),
                })
            })
            .collect::<Result<Vec<_>, IvmRuntimeError>>()?;
        let projection = ShapeNotificationProjection {
            descriptor: original_output,
            expressions: notification_expressions,
            mapping: notification_mapping,
        };
        let mut hasher = DefaultHasher::new();
        graph.hash(&mut hasher);
        let shape = format!("auto_direct_{:016x}", hasher.finish());
        let graph = replace_binding_shape(graph, &shape);
        if shape_output.field_index(&binding_field).is_none() {
            return Ok(None);
        }
        let binding_descriptor = RecordDescriptor::new([(
            binding_field.clone(),
            lifted
                .value
                .value_type()
                .ok_or(IvmRuntimeError::UnsupportedOperator)?,
        )]);
        let key = AutoDirectFamilyKey {
            graph: graph.clone(),
            binding_descriptor,
            binding_field: binding_field.clone(),
            notification_projection: projection.clone(),
        };
        Ok(Some(AutoDirectFamilyPlan {
            key,
            graph,
            shape,
            binding_descriptor,
            binding_field,
            binding_value: lifted.value.to_value(),
            notification_projection: projection,
        }))
    }

    fn infer_builder_output(
        &self,
        graph: &GraphBuilder,
    ) -> Result<RecordDescriptor, IvmRuntimeError> {
        match graph {
            GraphBuilder::Table { table } => self
                .schema
                .table(table)
                .ok_or_else(|| IvmRuntimeError::TableNotFound(table.clone()))
                .map(TableSchema::record_schema),
            GraphBuilder::InlineRecords { output, .. } => Ok(*output),
            GraphBuilder::Index { .. } => Ok(index_record_descriptor()),
            GraphBuilder::FrontierSource { output, .. }
            | GraphBuilder::BindingSource { output, .. } => Ok(*output),
            GraphBuilder::Filter { input, .. }
            | GraphBuilder::ArgMaxBy { input, .. }
            | GraphBuilder::ArgMinBy { input, .. }
            | GraphBuilder::TopBy { input, .. } => self.infer_builder_output(input),
            GraphBuilder::UnwrapNullable { input, field } => {
                let input = self.infer_builder_output(input)?;
                let field_idx = resolve_field_ref(&input, field)?;
                unwrap_nullable_descriptor(&input, field_idx)
            }
            GraphBuilder::Project { input, fields } => {
                let input = self.infer_builder_output(input)?;
                project_descriptor(&input, fields)
            }
            GraphBuilder::Union { inputs } => {
                let mut output = None;
                for input in inputs {
                    let next = self.infer_builder_output(input)?;
                    if let Some(output) = output {
                        if output != next {
                            return Err(IvmRuntimeError::GraphOutputMismatch);
                        }
                    } else {
                        output = Some(next);
                    }
                }
                Ok(output.unwrap_or_default())
            }
            GraphBuilder::Join { left, right, .. } => {
                let left = self.infer_builder_output(left)?;
                let right = self.infer_builder_output(right)?;
                Ok(join_descriptor(&left, &right))
            }
            GraphBuilder::AntiJoin { left, .. } => self.infer_builder_output(left),
            GraphBuilder::Recursive { seed, step, .. } => {
                let seed = self.infer_builder_output(seed)?;
                let step = self.infer_builder_output(step)?;
                if seed != step {
                    return Err(IvmRuntimeError::GraphOutputMismatch);
                }
                Ok(seed)
            }
        }
    }

    fn next_subscription_id(&mut self) -> SubscriptionId {
        let id = SubscriptionId(self.next_subscription_id);
        self.next_subscription_id += 1;
        id
    }

    fn next_shape_id(&mut self) -> PreparedShapeId {
        let id = PreparedShapeId(self.next_shape_id);
        self.next_shape_id += 1;
        id
    }

    fn add_binding_ref(
        &mut self,
        shape_id: PreparedShapeId,
        binding: BindingKey,
    ) -> Result<BindingDelta, IvmRuntimeError> {
        let shape = self
            .prepared_shapes
            .get(&shape_id)
            .ok_or(IvmRuntimeError::PreparedShapeNotFound(shape_id))?;
        let source = self
            .binding_sources
            .get_mut(&shape.shape)
            .ok_or_else(|| IvmRuntimeError::BindingSourceNotFound(shape.shape.clone()))?;
        let count = source.refcounts.entry(binding.clone()).or_default();
        *count += 1;
        Ok(BindingDelta {
            shape: shape.shape.clone(),
            descriptor: source.descriptor,
            deltas: if *count == 1 {
                vec![RecordDelta {
                    record: binding.0,
                    weight: 1,
                }]
            } else {
                Vec::new()
            },
        })
    }

    fn remove_binding_ref(
        &mut self,
        shape_id: PreparedShapeId,
        binding: &BindingKey,
    ) -> Option<BindingDelta> {
        let shape = self.prepared_shapes.get(&shape_id)?;
        let source = self.binding_sources.get_mut(&shape.shape)?;
        let count = source.refcounts.get_mut(binding)?;
        *count -= 1;
        if *count > 0 {
            return Some(BindingDelta {
                shape: shape.shape.clone(),
                descriptor: source.descriptor,
                deltas: Vec::new(),
            });
        }
        source.refcounts.remove(binding);
        Some(BindingDelta {
            shape: shape.shape.clone(),
            descriptor: source.descriptor,
            deltas: vec![RecordDelta {
                record: binding.0.clone(),
                weight: -1,
            }],
        })
    }

    fn shape_materialized_snapshot(
        &self,
        shape_id: PreparedShapeId,
        param: &BindingKey,
    ) -> Result<RecordDeltas, IvmRuntimeError> {
        let shape = self
            .prepared_shapes
            .get(&shape_id)
            .ok_or(IvmRuntimeError::PreparedShapeNotFound(shape_id))?;
        let deltas = shape
            .bindings
            .get(param)
            .into_iter()
            .flat_map(|binding| binding.materialized.iter())
            .map(|(record, weight)| RecordDelta {
                record: record.clone(),
                weight: *weight,
            })
            .collect();
        Ok(RecordDeltas {
            descriptor: shape.output.output,
            deltas,
        })
    }

    fn shape_materialized_snapshot_for_subscription(
        &self,
        shape_id: PreparedShapeId,
        param: &BindingKey,
        subscription_id: SubscriptionId,
    ) -> Result<RecordDeltas, IvmRuntimeError> {
        let snapshot = self.shape_materialized_snapshot(shape_id, param)?;
        let Some(shape) = self.prepared_shapes.get(&shape_id) else {
            return Err(IvmRuntimeError::PreparedShapeNotFound(shape_id));
        };
        let projection = shape
            .bindings
            .get(param)
            .and_then(|binding| binding.senders.get(&subscription_id))
            .and_then(|sender| sender.projection.as_ref());
        let Some(projection) = projection else {
            return Ok(snapshot);
        };
        project_record_deltas(snapshot, projection)
    }

    fn binding_snapshot_deltas(&self) -> HashMap<String, RecordDeltas> {
        debug_assert!(
            self.pending_binding_retractions.is_empty(),
            "binding snapshots must not race queued binding retractions"
        );
        self.binding_sources
            .iter()
            .map(|(shape, source)| {
                (
                    shape.clone(),
                    RecordDeltas {
                        descriptor: source.descriptor,
                        deltas: source
                            .refcounts
                            .keys()
                            .map(|binding| RecordDelta {
                                record: binding.0.clone(),
                                weight: 1,
                            })
                            .collect(),
                    },
                )
            })
            .collect()
    }

    fn unsubscribe_shape_subscription(
        &mut self,
        shape_id: PreparedShapeId,
        param: &BindingKey,
        subscription_id: SubscriptionId,
    ) {
        if let Some(shape) = self.prepared_shapes.get_mut(&shape_id)
            && let Some(binding) = shape.bindings.get_mut(param)
        {
            binding.senders.remove(&subscription_id);
            if binding.is_empty() {
                shape.bindings.remove(param);
            }
        }
        if let Some(param_delta) = self.remove_binding_ref(shape_id, param)
            && !param_delta.deltas.is_empty()
        {
            self.pending_binding_retractions.push(param_delta);
        }
        self.remove_unreferenced_auto_family(shape_id);
    }

    fn remove_unreferenced_auto_family(&mut self, shape_id: PreparedShapeId) {
        let Some(shape) = self.prepared_shapes.get(&shape_id) else {
            return;
        };
        let Some(key) = shape.auto_family_key.clone() else {
            return;
        };
        if shape
            .bindings
            .values()
            .any(|binding| !binding.senders.is_empty())
        {
            return;
        }
        let shape_name = shape.shape.clone();
        let output_node = shape.output.node;
        let routing_node = shape.routing.as_ref().map(|routing| routing.output.node);
        self.prepared_shapes.remove(&shape_id);
        self.binding_sources.remove(&shape_name);
        self.auto_direct_families.remove(&key);
        self.remove_retainer(
            output_node,
            &Retainer::PreparedShape(shape_id.retainer_key()),
        );
        if let Some(routing_node) = routing_node {
            self.remove_retainer(
                routing_node,
                &Retainer::PreparedShape(shape_id.retainer_key()),
            );
        }
        for node in self.gc_ephemeral_nodes(0) {
            self.remove_node_runtime(node);
        }
        self.prune_unreferenced_arrangements();
    }

    fn advance_tick(&mut self) -> u64 {
        self.current_tick += 1;
        self.current_tick
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

        retained
    }

    pub fn stats(&self) -> RuntimeStats {
        let mut stats = RuntimeStats {
            graph_nodes: self.graph.nodes().len(),
            active_subscriptions: self.subscriptions.len(),
            active_prepared_shapes: self.prepared_shapes.len(),
            active_shape_params: self
                .binding_sources
                .values()
                .map(|source| source.refcounts.len())
                .sum(),
            arrangement_count: self.arrangement_states.len(),
            logical_nodes_requested: self.logical_nodes_requested,
            deduped_graph_nodes: self.graph.nodes().len(),
            ..RuntimeStats::default()
        };
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

    fn add_retainer(&mut self, id: NodeId, retainer: Retainer) -> bool {
        if self.graph.node(id).is_none() {
            return false;
        }
        let meta = self.node_meta.entry(id).or_default();
        meta.last_used_tick = self.current_tick;
        meta.retainers.insert(retainer)
    }

    fn retain_as_subscription(
        &mut self,
        subscription_id: SubscriptionId,
        output_node: NodeId,
    ) -> bool {
        self.add_retainer(
            output_node,
            Retainer::Subscription(subscription_id.retainer_key()),
        )
    }

    fn remove_retainer(&mut self, id: NodeId, retainer: &Retainer) -> bool {
        self.node_meta
            .get_mut(&id)
            .map(|meta| meta.retainers.remove(retainer))
            .unwrap_or(false)
    }

    fn gc_ephemeral_nodes(&mut self, ttl_ticks: u64) -> Vec<NodeId> {
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

    fn remove_node_runtime(&mut self, node: NodeId) {
        self.operator_states.retain(|key, _| key.node != node);
        self.arrangement_states.retain(|key, _| key.input != node);
        self.eval_memo.retain(|key, _| key.node != node);
        self.node_meta.remove(&node);
    }

    fn prune_unreferenced_arrangements(&mut self) {
        let referenced = self
            .graph
            .nodes()
            .values()
            .filter_map(|node| {
                let (OpType::Join(join) | OpType::AntiJoin(join)) = &node.descriptor.operator
                else {
                    return None;
                };
                let [left, right] = node.descriptor.inputs.as_slice() else {
                    return None;
                };
                Some([
                    ArrangementKey {
                        scope: ScopePath::root(),
                        input: *left,
                        fields: plan_expr_names(&join.left_key),
                        descriptor: join.left_descriptor,
                    },
                    ArrangementKey {
                        scope: ScopePath::root(),
                        input: *right,
                        fields: plan_expr_names(&join.right_key),
                        descriptor: join.right_descriptor,
                    },
                ])
            })
            .flatten()
            .collect::<HashSet<_>>();
        self.arrangement_states.retain(|key, _| {
            referenced.iter().any(|referenced| {
                referenced.input == key.input
                    && referenced.fields == key.fields
                    && referenced.descriptor == key.descriptor
            })
        });
    }

    fn retained_recursive_nodes_are_current(&self, current_tick: u64) -> bool {
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

    fn initialize_node_runtime(&mut self, node: NodeId) {
        self.node_meta.entry(node).or_default();
        let Some(graph_node) = self.graph.node(node) else {
            return;
        };
        let operator = &graph_node.descriptor.operator;
        let operator_state = operator_state_for(operator);
        if !matches!(operator_state, OperatorState::Stateless) {
            self.operator_states
                .entry(OperatorStateKey {
                    scope: ScopePath::root(),
                    node,
                })
                .or_insert(operator_state);
        }
    }

    fn add_dedup_graph(&mut self, graph: &GraphBuilder) -> Result<CompiledNode, IvmRuntimeError> {
        let inferred_output = self.infer_builder_output(graph)?;
        match graph {
            GraphBuilder::Table { table } => {
                let output = inferred_output;
                let node = self.graph.dedup_node(
                    NodeDescriptor::new(
                        OpType::TableSource(TableSourceOp {
                            table: table.clone(),
                        }),
                        [],
                        output,
                    ),
                    NodeDurability::Ephemeral,
                );
                self.initialize_node_runtime(node);
                Ok(CompiledNode { output, node })
            }
            GraphBuilder::InlineRecords { output, records } => {
                if inferred_output != *output {
                    return Err(IvmRuntimeError::GraphOutputMismatch);
                }
                let node = self.graph.dedup_node(
                    NodeDescriptor::new(
                        OpType::InlineRecords(InlineRecordsOp {
                            records: records.clone(),
                        }),
                        [],
                        *output,
                    ),
                    NodeDurability::Ephemeral,
                );
                self.initialize_node_runtime(node);
                Ok(CompiledNode {
                    output: *output,
                    node,
                })
            }
            GraphBuilder::Index { table, index } => {
                let table = self
                    .schema
                    .table(table)
                    .ok_or_else(|| IvmRuntimeError::TableNotFound(table.clone()))?
                    .clone();
                let index = table
                    .indices
                    .iter()
                    .find(|candidate| candidate.name == *index)
                    .ok_or_else(|| IvmRuntimeError::IndexNotFound(index.clone()))?
                    .clone();
                self.add_dedup_index_by(&table, &index)
            }
            GraphBuilder::FrontierSource { binding, output } => {
                let node = self.graph.dedup_node(
                    NodeDescriptor::new(
                        OpType::FrontierSource(FrontierSourceOp {
                            binding: binding.clone(),
                        }),
                        [],
                        *output,
                    ),
                    NodeDurability::Ephemeral,
                );
                self.initialize_node_runtime(node);
                Ok(CompiledNode {
                    output: inferred_output,
                    node,
                })
            }
            GraphBuilder::BindingSource { shape, output } => {
                let node = self.graph.dedup_node(
                    NodeDescriptor::new(
                        OpType::BindingSource(BindingSourceOp {
                            shape: shape.clone(),
                        }),
                        [],
                        *output,
                    ),
                    NodeDurability::Ephemeral,
                );
                self.initialize_node_runtime(node);
                Ok(CompiledNode {
                    output: inferred_output,
                    node,
                })
            }
            GraphBuilder::Recursive {
                seed,
                step,
                frontier,
                max_iters,
            } => {
                if builder_contains_recursive(seed) || builder_contains_recursive(step) {
                    return Err(IvmRuntimeError::UnsupportedNestedRecursion);
                }
                if builder_contains_arg_max_by(seed) || builder_contains_arg_max_by(step) {
                    return Err(IvmRuntimeError::UnsupportedArgMaxBy(
                        "arg_max_by is not supported inside recursive graphs".to_owned(),
                    ));
                }
                let compiled_seed = self.add_dedup_graph(seed)?;
                let compiled_step = self.add_dedup_graph(step)?;
                if compiled_seed.output != compiled_step.output
                    || compiled_seed.output != inferred_output
                {
                    return Err(IvmRuntimeError::GraphOutputMismatch);
                }
                let output = inferred_output;
                let node = self.graph.dedup_node(
                    NodeDescriptor::new(
                        OpType::Recursive(RecursiveOp {
                            frontier: frontier.clone(),
                            max_iters: *max_iters,
                        }),
                        [compiled_seed.node, compiled_step.node],
                        output,
                    ),
                    NodeDurability::Ephemeral,
                );
                self.initialize_node_runtime(node);
                Ok(CompiledNode { output, node })
            }
            GraphBuilder::ArgMaxBy {
                input,
                group_cols,
                order_cols,
            } => {
                let compiled_input = self.add_dedup_graph(input)?;
                let output = inferred_output;
                let group_field_indices = group_cols
                    .iter()
                    .map(|field| resolve_field_ref(&output, field))
                    .collect::<Result<Vec<_>, _>>()?;
                let order_field_indices = order_cols
                    .iter()
                    .map(|field| resolve_field_ref(&output, field))
                    .collect::<Result<Vec<_>, _>>()?;
                let primary_key_field_indices =
                    if let GraphBuilder::Table { table } = input.as_ref() {
                        let table_schema = self
                            .schema
                            .table(table)
                            .ok_or_else(|| IvmRuntimeError::TableNotFound(table.clone()))?
                            .clone();
                        let primary_key = table_schema
                            .primary_key
                            .as_ref()
                            .ok_or_else(|| IvmRuntimeError::MissingPrimaryKey(table.clone()))?;
                        let primary_key_field_indices = primary_key
                            .columns
                            .iter()
                            .map(|column| {
                                output.field_index(&column.column).ok_or_else(|| {
                                    IvmRuntimeError::GraphFieldNotFound(column.column.clone())
                                })
                            })
                            .collect::<Result<Vec<_>, _>>()?;
                        validate_arg_by_primary_key_indices(
                            "arg_max_by",
                            &table_schema,
                            &group_field_indices,
                            &order_field_indices,
                            &primary_key_field_indices,
                        )?;
                        primary_key_field_indices
                    } else {
                        group_field_indices
                            .iter()
                            .chain(&order_field_indices)
                            .copied()
                            .collect()
                    };
                let group_field_names = group_field_indices
                    .iter()
                    .map(|field| field_name_at(&output, *field))
                    .collect::<Result<Vec<_>, _>>()?;
                let order_field_names = order_field_indices
                    .iter()
                    .map(|field| field_name_at(&output, *field))
                    .collect::<Result<Vec<_>, _>>()?;
                let node = self.graph.dedup_node(
                    NodeDescriptor::new(
                        OpType::ArgMaxBy(ArgMaxByOp {
                            group_fields: group_field_names,
                            order_fields: order_field_names,
                            group_field_indices,
                            primary_key_field_indices,
                        }),
                        [compiled_input.node],
                        output,
                    ),
                    NodeDurability::Ephemeral,
                );
                self.initialize_node_runtime(node);
                Ok(CompiledNode { output, node })
            }
            GraphBuilder::ArgMinBy {
                input,
                group_cols,
                order_cols,
            } => {
                let compiled_input = self.add_dedup_graph(input)?;
                let output = inferred_output;
                let group_field_indices = group_cols
                    .iter()
                    .map(|field| resolve_field_ref(&output, field))
                    .collect::<Result<Vec<_>, _>>()?;
                let order_field_indices = order_cols
                    .iter()
                    .map(|field| resolve_field_ref(&output, field))
                    .collect::<Result<Vec<_>, _>>()?;
                let primary_key_field_indices =
                    if let GraphBuilder::Table { table } = input.as_ref() {
                        let table_schema = self
                            .schema
                            .table(table)
                            .ok_or_else(|| IvmRuntimeError::TableNotFound(table.clone()))?
                            .clone();
                        let primary_key = table_schema
                            .primary_key
                            .as_ref()
                            .ok_or_else(|| IvmRuntimeError::MissingPrimaryKey(table.clone()))?;
                        let primary_key_field_indices = primary_key
                            .columns
                            .iter()
                            .map(|column| {
                                output.field_index(&column.column).ok_or_else(|| {
                                    IvmRuntimeError::GraphFieldNotFound(column.column.clone())
                                })
                            })
                            .collect::<Result<Vec<_>, _>>()?;
                        validate_arg_by_primary_key_indices(
                            "arg_min_by",
                            &table_schema,
                            &group_field_indices,
                            &order_field_indices,
                            &primary_key_field_indices,
                        )?;
                        primary_key_field_indices
                    } else {
                        group_field_indices
                            .iter()
                            .chain(&order_field_indices)
                            .copied()
                            .collect()
                    };
                let group_field_names = group_field_indices
                    .iter()
                    .map(|field| field_name_at(&output, *field))
                    .collect::<Result<Vec<_>, _>>()?;
                let order_field_names = order_field_indices
                    .iter()
                    .map(|field| field_name_at(&output, *field))
                    .collect::<Result<Vec<_>, _>>()?;
                let node = self.graph.dedup_node(
                    NodeDescriptor::new(
                        OpType::ArgMinBy(ArgMinByOp {
                            group_fields: group_field_names,
                            order_fields: order_field_names,
                            group_field_indices,
                            primary_key_field_indices,
                        }),
                        [compiled_input.node],
                        output,
                    ),
                    NodeDurability::Ephemeral,
                );
                self.initialize_node_runtime(node);
                Ok(CompiledNode { output, node })
            }
            GraphBuilder::TopBy {
                input,
                group_cols,
                order_cols,
                tie_cols,
                offset,
                limit,
            } => {
                if *limit == 0 {
                    return Err(IvmRuntimeError::UnsupportedOperator);
                }
                let compiled_input = self.add_dedup_graph(input)?;
                let output = inferred_output;
                let group_field_indices = group_cols
                    .iter()
                    .map(|field| resolve_field_ref(&output, field))
                    .collect::<Result<Vec<_>, _>>()?;
                let order_field_indices = order_cols
                    .iter()
                    .map(|order| resolve_field_ref(&output, &order.field))
                    .collect::<Result<Vec<_>, _>>()?;
                let tie_field_indices = tie_cols
                    .iter()
                    .map(|field| resolve_field_ref(&output, field))
                    .collect::<Result<Vec<_>, _>>()?;
                let group_field_names = group_field_indices
                    .iter()
                    .map(|field| field_name_at(&output, *field))
                    .collect::<Result<Vec<_>, _>>()?;
                let order_fields = order_cols
                    .iter()
                    .zip(&order_field_indices)
                    .map(|(order, field_idx)| {
                        Ok(TopByOrderField {
                            field: field_name_at(&output, *field_idx)?,
                            direction: order.direction,
                        })
                    })
                    .collect::<Result<Vec<_>, IvmRuntimeError>>()?;
                let tie_field_names = tie_field_indices
                    .iter()
                    .map(|field| field_name_at(&output, *field))
                    .collect::<Result<Vec<_>, _>>()?;
                let sort_field_indices = order_field_indices
                    .iter()
                    .chain(&tie_field_indices)
                    .copied()
                    .collect::<Vec<_>>();
                let sort_directions = order_cols
                    .iter()
                    .map(|order| order.direction)
                    .chain(std::iter::repeat_n(
                        TopByDirection::Asc,
                        tie_field_indices.len(),
                    ))
                    .collect::<Vec<_>>();
                let node = self.graph.dedup_node(
                    NodeDescriptor::new(
                        OpType::TopBy(TopByOp {
                            group_fields: group_field_names,
                            group_field_indices,
                            order_fields,
                            tie_fields: tie_field_names,
                            sort_field_indices,
                            sort_directions,
                            offset: *offset,
                            limit: *limit,
                        }),
                        [compiled_input.node],
                        output,
                    ),
                    NodeDurability::Ephemeral,
                );
                self.initialize_node_runtime(node);
                Ok(CompiledNode { output, node })
            }
            GraphBuilder::Filter { input, predicate } => {
                let compiled_input = self.add_dedup_graph(input)?;
                let input_node = compiled_input.node;
                let output = inferred_output;
                let node = self.graph.dedup_node(
                    NodeDescriptor::new(
                        OpType::Filter(FilterOp {
                            predicate: predicate.clone(),
                        }),
                        [input_node],
                        output,
                    ),
                    NodeDurability::Ephemeral,
                );
                self.initialize_node_runtime(node);
                Ok(CompiledNode { output, node })
            }
            GraphBuilder::Project { input, fields } => {
                let compiled_input = self.add_dedup_graph(input)?;
                let input_node = compiled_input.node;
                let input_output = compiled_input.output;
                let output = inferred_output;
                let mapping = fields
                    .iter()
                    .filter_map(|field| {
                        field.source().map(|source| {
                            resolve_field_ref(&input_output, source).map(|idx| (0, idx))
                        })
                    })
                    .collect::<Result<Vec<_>, _>>()?;
                let node = self.graph.dedup_node(
                    NodeDescriptor::new(
                        OpType::MapProject(MapProjectOp {
                            expressions: fields
                                .iter()
                                .map(|field| {
                                    project_field_expr(&input_output, field).map(|expression| {
                                        ProjectionExpr {
                                            expression,
                                            output_name: Some(field.output_name.clone()),
                                        }
                                    })
                                })
                                .collect::<Result<Vec<_>, IvmRuntimeError>>()?,
                            mapping,
                        }),
                        [input_node],
                        output,
                    ),
                    NodeDurability::Ephemeral,
                );
                self.initialize_node_runtime(node);
                Ok(CompiledNode { output, node })
            }
            GraphBuilder::UnwrapNullable { input, field } => {
                let compiled_input = self.add_dedup_graph(input)?;
                let input_node = compiled_input.node;
                let input_output = compiled_input.output;
                let field_idx = resolve_field_ref(&input_output, field)?;
                let output = inferred_output;
                let node = self.graph.dedup_node(
                    NodeDescriptor::new(
                        OpType::UnwrapNullable(UnwrapNullableOp {
                            field: field_ref_name(&input_output, field)?,
                            field_idx,
                        }),
                        [input_node],
                        output,
                    ),
                    NodeDurability::Ephemeral,
                );
                self.initialize_node_runtime(node);
                Ok(CompiledNode { output, node })
            }
            GraphBuilder::Union { inputs } => {
                let mut input_nodes = Vec::with_capacity(inputs.len());
                for input in inputs {
                    let compiled_input = self.add_dedup_graph(input)?;
                    let input_node = compiled_input.node;
                    let input_output = compiled_input.output;
                    if inferred_output != input_output {
                        return Err(IvmRuntimeError::GraphOutputMismatch);
                    }
                    input_nodes.push(input_node);
                }
                let output = inferred_output;
                let node = self.graph.dedup_node(
                    NodeDescriptor::new(OpType::Union, input_nodes, output),
                    NodeDurability::Ephemeral,
                );
                self.initialize_node_runtime(node);
                Ok(CompiledNode { output, node })
            }
            GraphBuilder::Join {
                left,
                right,
                left_on,
                right_on,
            } => {
                let compiled_left = self.add_dedup_graph(left)?;
                let compiled_right = self.add_dedup_graph(right)?;
                let output = inferred_output;
                let left_descriptor = compiled_left.output;
                let right_descriptor = compiled_right.output;
                let left_key = left_on
                    .iter()
                    .map(|field| field_ref_name(&left_descriptor, field).map(PlanExpr::field))
                    .collect::<Result<Vec<_>, IvmRuntimeError>>()?;
                let right_key = right_on
                    .iter()
                    .map(|field| field_ref_name(&right_descriptor, field).map(PlanExpr::field))
                    .collect::<Result<Vec<_>, IvmRuntimeError>>()?;
                let node_descriptor = NodeDescriptor::new(
                    OpType::Join(JoinOp {
                        kind: JoinOpKind::Inner,
                        left_key,
                        right_key,
                        left_descriptor,
                        right_descriptor,
                        residual_predicate: None,
                    }),
                    [compiled_left.node, compiled_right.node],
                    output,
                );
                let node = self
                    .graph
                    .dedup_node(node_descriptor, NodeDurability::Ephemeral);
                self.initialize_node_runtime(node);
                Ok(CompiledNode { output, node })
            }
            GraphBuilder::AntiJoin {
                left,
                right,
                left_on,
                right_on,
            } => {
                let compiled_left = self.add_dedup_graph(left)?;
                let compiled_right = self.add_dedup_graph(right)?;
                let output = inferred_output;
                let left_descriptor = compiled_left.output;
                let right_descriptor = compiled_right.output;
                let left_key = left_on
                    .iter()
                    .map(|field| field_ref_name(&left_descriptor, field).map(PlanExpr::field))
                    .collect::<Result<Vec<_>, IvmRuntimeError>>()?;
                let right_key = right_on
                    .iter()
                    .map(|field| field_ref_name(&right_descriptor, field).map(PlanExpr::field))
                    .collect::<Result<Vec<_>, IvmRuntimeError>>()?;
                let node_descriptor = NodeDescriptor::new(
                    OpType::AntiJoin(JoinOp {
                        kind: JoinOpKind::Inner,
                        left_key,
                        right_key,
                        left_descriptor,
                        right_descriptor,
                        residual_predicate: None,
                    }),
                    [compiled_left.node, compiled_right.node],
                    output,
                );
                let node = self
                    .graph
                    .dedup_node(node_descriptor, NodeDurability::Ephemeral);
                self.initialize_node_runtime(node);
                Ok(CompiledNode { output, node })
            }
        }
    }

    fn add_dedup_schema_index(
        &mut self,
        table: &TableSchema,
        index: &IndexSchema,
    ) -> Result<NodeId, IvmRuntimeError> {
        self.logical_nodes_requested += 3;
        let table_descriptor = table.record_schema();
        let input = self.graph.dedup_node(
            NodeDescriptor::new(
                OpType::TableSource(TableSourceOp {
                    table: table.name.clone(),
                }),
                [],
                table_descriptor,
            ),
            NodeDurability::Ephemeral,
        );
        self.initialize_node_runtime(input);

        let CompiledNode {
            output: index_descriptor,
            node: index_by,
        } = self.add_dedup_index_by_from_input(table, index, input, table_descriptor)?;

        let storage = DurableStorage {
            column_family: "indices".to_owned(),
            key_prefix: durable_index_key_prefix(&table.name, &index.name),
        };
        let persist = self.graph.dedup_node(
            NodeDescriptor::new(
                OpType::Persist(PersistOp {
                    name: index.name.clone(),
                    storage: storage.clone(),
                    key_fields: vec![0],
                    unique: index.unique,
                }),
                [index_by],
                index_descriptor,
            ),
            NodeDurability::Durable { storage },
        );
        self.add_retainer(
            persist,
            Retainer::DurableSchemaObject(format!("{}.{}", table.name, index.name)),
        );
        self.initialize_node_runtime(persist);

        Ok(persist)
    }

    fn add_dedup_index_by(
        &mut self,
        table: &TableSchema,
        index: &IndexSchema,
    ) -> Result<CompiledNode, IvmRuntimeError> {
        let table_descriptor = table.record_schema();
        let input = self.graph.dedup_node(
            NodeDescriptor::new(
                OpType::TableSource(TableSourceOp {
                    table: table.name.clone(),
                }),
                [],
                table_descriptor,
            ),
            NodeDurability::Ephemeral,
        );
        self.initialize_node_runtime(input);
        self.add_dedup_index_by_from_input(table, index, input, table_descriptor)
    }

    fn add_dedup_index_by_from_input(
        &mut self,
        table: &TableSchema,
        index: &IndexSchema,
        input: NodeId,
        table_descriptor: RecordDescriptor,
    ) -> Result<CompiledNode, IvmRuntimeError> {
        let index_descriptor = index_record_descriptor();
        let key_fields = index
            .columns
            .iter()
            .map(|column| {
                table_descriptor
                    .field_index(column)
                    .ok_or_else(|| IvmRuntimeError::GraphFieldNotFound(column.clone()))
            })
            .collect::<Result<Vec<_>, _>>()?;
        let primary_key = table
            .primary_key
            .as_ref()
            .ok_or_else(|| IvmRuntimeError::MissingPrimaryKey(table.name.clone()))?;
        let primary_key_fields = primary_key
            .columns
            .iter()
            .map(|column| {
                table_descriptor
                    .field_index(&column.column)
                    .ok_or_else(|| IvmRuntimeError::GraphFieldNotFound(column.column.clone()))
            })
            .collect::<Result<Vec<_>, _>>()?;
        let index_key_covers_primary_key = primary_key
            .columns
            .iter()
            .all(|primary_key_column| index.columns.contains(&primary_key_column.column));

        let node = self.graph.dedup_node(
            NodeDescriptor::new(
                OpType::IndexBy(IndexByOp {
                    key_expressions: index
                        .columns
                        .iter()
                        .map(|column| PlanExpr::field(column.clone()))
                        .collect(),
                    value_expressions: primary_key
                        .columns
                        .iter()
                        .map(|column| PlanExpr::field(column.column.clone()))
                        .collect(),
                    explicit_index: Some(index.clone()),
                    key_fields,
                    value_fields: primary_key_fields,
                    unique: index.unique || index_key_covers_primary_key,
                    append_value_to_key: !index.unique && !index_key_covers_primary_key,
                    store_value: index.unique && !index_key_covers_primary_key,
                }),
                [input],
                index_descriptor,
            ),
            NodeDurability::Ephemeral,
        );
        self.initialize_node_runtime(node);
        Ok(CompiledNode {
            output: index_descriptor,
            node,
        })
    }
}

/// Point-in-time runtime counters for benchmark and diagnostics reporting.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct RuntimeStats {
    pub graph_nodes: usize,
    pub active_subscriptions: usize,
    pub active_prepared_shapes: usize,
    pub active_shape_params: usize,
    pub arrangement_count: usize,
    pub arrangement_rows: usize,
    pub arrangement_encoded_bytes: usize,
    pub recursive_state_count: usize,
    pub recursive_accumulated_rows: usize,
    pub recursive_accumulated_encoded_bytes: usize,
    pub logical_nodes_requested: u64,
    pub deduped_graph_nodes: usize,
}

impl RuntimeStats {
    pub fn dedupe_ratio(&self) -> f64 {
        if self.logical_nodes_requested == 0 {
            return 1.0;
        }
        self.deduped_graph_nodes as f64 / self.logical_nodes_requested as f64
    }
}

/// Metrics produced by one runtime tick.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct TickMetrics {
    pub tick: u64,
    pub table_delta_records: usize,
    pub records_processed: usize,
    pub recursive_recomputes: usize,
    pub notifications_sent: usize,
    pub notification_records: usize,
    pub notification_encoded_bytes: usize,
    pub runtime_stats: RuntimeStats,
}

/// Recursive scope path used to namespace context-dependent state.
#[derive(Clone, Debug, Default, PartialEq, Eq, Hash)]
pub(super) struct ScopePath(Vec<NodeId>);

impl ScopePath {
    fn root() -> Self {
        Self(Vec::new())
    }

    pub(super) fn child(&self, recursive_node: NodeId) -> Self {
        let mut scope = self.0.clone();
        scope.push(recursive_node);
        Self(scope)
    }
}

/// Key for operator state that must survive across ticks.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
struct OperatorStateKey {
    /// Empty for normal query execution; nested recursive scopes append their
    /// recursive node ids here.
    scope: ScopePath,
    node: NodeId,
}

/// Key for a reusable join-side arrangement.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
struct ArrangementKey {
    /// Context-independent inputs use the root scope and can be shared across
    /// unrelated subscriptions.
    scope: ScopePath,
    /// The graph fragment whose records are arranged.
    input: NodeId,
    fields: Vec<String>,
    descriptor: RecordDescriptor,
}

/// Database tick plus recursive sub-tick for scoped arrangement freshness.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub(super) struct SubTick {
    tick: u64,
    sub_tick: u64,
}

/// Logical database tick for state whose contents are only root-tick scoped.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub(super) struct Tick(u64);

/// Runtime state together with the logical time its contents reflect.
///
/// Keeping the "as of" time outside operator-specific state makes freshness
/// checks visible at shared-state access sites instead of burying them inside
/// joins, recursion, or future stateful operators.
#[derive(Clone, Debug)]
pub(super) struct AsOf<T, S> {
    value: T,
    as_of: Option<S>,
}

impl<T, S> AsOf<T, S> {
    fn new(value: T) -> Self {
        Self { value, as_of: None }
    }

    pub(super) fn value(&self) -> &T {
        &self.value
    }

    pub(super) fn value_mut(&mut self) -> &mut T {
        &mut self.value
    }

    pub(super) fn as_of(&self) -> Option<S>
    where
        S: Copy,
    {
        self.as_of
    }
}

impl<T, S> AsOf<T, S>
where
    S: Copy + Ord + std::fmt::Debug,
{
    pub(super) fn value_at(&self, expected: S) -> Result<&T, IvmRuntimeError> {
        if self.as_of == Some(expected) {
            return Ok(&self.value);
        }
        Err(IvmRuntimeError::StaleRuntimeState {
            expected: format!("{expected:?}"),
            actual: self.as_of.map(|actual| format!("{actual:?}")),
        })
    }

    pub(super) fn mark_forward_as_of(&mut self, next: S) -> Result<(), IvmRuntimeError> {
        if self.as_of.is_some_and(|current| current > next) {
            return Err(IvmRuntimeError::OutOfOrderRuntimeState {
                current: format!("{:?}", self.as_of.expect("checked above")),
                next: format!("{next:?}"),
            });
        }
        self.as_of = Some(next);
        Ok(())
    }

    pub(super) fn replace_as_of_at_least(&mut self, next: S) {
        if self.as_of.is_none_or(|current| current <= next) {
            self.as_of = Some(next);
        }
    }
}

impl<T: Default, S> Default for AsOf<T, S> {
    fn default() -> Self {
        Self::new(T::default())
    }
}

/// Whether an arrangement should consume a delta or be rebuilt from a snapshot.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub(super) enum ArrangementUpdateMode {
    #[default]
    Accumulate,
    Replace,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub(super) enum EvalMode {
    #[default]
    Tick,
    Hydrate,
}

/// Key for one cached node evaluation within a logical tick.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
struct EvalMemoKey {
    scope: ScopePath,
    node: NodeId,
    tick: u64,
    /// Recursive sub-ticks intentionally affect memoization, not operator
    /// state identity.
    sub_tick: u64,
}

/// Current scoped inputs and logical time for node evaluation.
#[derive(Clone, Debug, Default)]
struct EvalContext {
    /// Current operator-state namespace.
    scope: ScopePath,
    /// Logical time within a recursive fixed-point evaluation.
    sub_tick: u64,
    /// FrontierSource bindings, currently used for recursive frontiers.
    bindings: HashMap<FrontierName, RecordDeltas>,
    /// Hydrate preparation rebuilds arrangements instead of layering onto them.
    arrangement_update_mode: ArrangementUpdateMode,
    eval_mode: EvalMode,
}

impl EvalContext {
    fn root() -> Self {
        Self {
            scope: ScopePath::root(),
            sub_tick: 0,
            bindings: HashMap::new(),
            arrangement_update_mode: ArrangementUpdateMode::Accumulate,
            eval_mode: EvalMode::Tick,
        }
    }

    fn root_snapshot() -> Self {
        Self {
            scope: ScopePath::root(),
            sub_tick: 0,
            bindings: HashMap::new(),
            arrangement_update_mode: ArrangementUpdateMode::Replace,
            eval_mode: EvalMode::Hydrate,
        }
    }

    pub(super) fn with_binding(
        scope: ScopePath,
        sub_tick: u64,
        binding: FrontierName,
        deltas: RecordDeltas,
    ) -> Self {
        Self {
            scope,
            sub_tick,
            bindings: HashMap::from([(binding, deltas)]),
            arrangement_update_mode: ArrangementUpdateMode::Accumulate,
            eval_mode: EvalMode::Tick,
        }
    }

    pub(super) fn with_binding_and_arrangement_mode(
        scope: ScopePath,
        sub_tick: u64,
        binding: FrontierName,
        deltas: RecordDeltas,
        arrangement_update_mode: ArrangementUpdateMode,
    ) -> Self {
        Self {
            scope,
            sub_tick,
            bindings: HashMap::from([(binding, deltas)]),
            arrangement_update_mode,
            eval_mode: EvalMode::Tick,
        }
    }
}

/// Stable handle returned to callers for subscription management.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct SubscriptionId(u64);

impl SubscriptionId {
    fn retainer_key(self) -> String {
        self.0.to_string()
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct PreparedShapeId(u64);

impl PreparedShapeId {
    fn retainer_key(self) -> String {
        self.0.to_string()
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct PreparedShape {
    id: PreparedShapeId,
}

impl PreparedShape {
    pub fn id(&self) -> PreparedShapeId {
        self.id
    }
}

/// Receiving end of a live query subscription.
#[derive(Debug)]
pub struct Subscription {
    id: SubscriptionId,
    receiver: Receiver<RecordDeltas>,
}

impl Subscription {
    pub fn id(&self) -> SubscriptionId {
        self.id
    }

    pub fn recv(&self) -> Result<RecordDeltas, RecvError> {
        self.receiver.recv()
    }

    pub fn try_recv(&self) -> Result<RecordDeltas, TryRecvError> {
        self.receiver.try_recv()
    }
}

impl PredicateExpr {
    fn matches(&self, record: BorrowedRecord<'_>) -> Result<bool, IvmRuntimeError> {
        match self {
            Self::Eq { field, value } => {
                compare_record_field(record, field, value, |ord| ord.is_eq())
            }
            Self::Neq { field, value } => {
                compare_record_field(record, field, value, |ord| !ord.is_eq())
            }
            Self::Contains { field, value } => contains_record_field(record, field, value),
            Self::EqField { field, value_field } => {
                compare_record_fields(record, field, value_field, |ord| ord.is_eq())
            }
            Self::ContainsField {
                field,
                needle_field,
            } => contains_record_field_value(record, field, needle_field),
            Self::NeqField { field, value_field } => {
                compare_record_fields(record, field, value_field, |ord| !ord.is_eq())
            }
            Self::Gt { field, value } => {
                compare_record_field(record, field, value, |ord| ord.is_gt())
            }
            Self::GtEq { field, value } => {
                compare_record_field(record, field, value, |ord| ord.is_ge())
            }
            Self::Lt { field, value } => {
                compare_record_field(record, field, value, |ord| ord.is_lt())
            }
            Self::LtEq { field, value } => {
                compare_record_field(record, field, value, |ord| ord.is_le())
            }
            Self::IsNull { field } => Ok(is_sql_null_value(&record.get(field)?)),
            Self::IsNotNull { field } => Ok(!is_sql_null_value(&record.get(field)?)),
            Self::And(predicates) => predicates
                .iter()
                .map(|predicate| predicate.matches(record))
                .try_fold(true, |acc, matches| matches.map(|matches| acc && matches)),
            Self::Or(predicates) => predicates
                .iter()
                .map(|predicate| predicate.matches(record))
                .try_fold(false, |acc, matches| matches.map(|matches| acc || matches)),
        }
    }
}

/// Deltas for one base table in a committed batch.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TableDelta {
    pub table: String,
    pub descriptor: RecordDescriptor,
    pub deltas: Vec<RecordDelta>,
}

/// Weighted change to one encoded record.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RecordDelta {
    pub record: Vec<u8>,
    pub weight: i64,
}

impl RecordDelta {
    pub fn raw(&self) -> &[u8] {
        &self.record
    }

    pub fn borrowed<'a>(&'a self, descriptor: &'a RecordDescriptor) -> BorrowedRecord<'a> {
        BorrowedRecord::new(&self.record, descriptor)
    }
}

/// Runtime bookkeeping for an active subscription.
#[derive(Clone, Debug)]
struct SubscriptionState {
    /// Sending end for this subscription's notification stream.
    sender: Sender<RecordDeltas>,
    target: SubscriptionTarget,
}

#[derive(Clone, Debug)]
enum SubscriptionTarget {
    Direct {
        /// The query result node evaluated on each tick to produce notifications.
        output: CompiledNode,
    },
    Shape {
        shape_id: PreparedShapeId,
        binding_key: BindingKey,
    },
}

impl SubscriptionTarget {
    fn is_direct(&self) -> bool {
        matches!(self, Self::Direct { .. })
    }
}

#[derive(Clone, Debug)]
struct PreparedShapeState {
    shape: String,
    binding_descriptor: RecordDescriptor,
    output: CompiledNode,
    output_key_fields: Vec<usize>,
    routing: Option<PreparedShapeRouting>,
    bindings: HashMap<BindingKey, PreparedBindingState>,
    auto_family_key: Option<AutoDirectFamilyKey>,
}

#[derive(Clone, Debug)]
struct PreparedShapeRouting {
    output: CompiledNode,
    notification_projection: ShapeNotificationProjection,
}

#[derive(Clone, Debug, Default)]
struct PreparedBindingState {
    senders: HashMap<SubscriptionId, ShapeSender>,
    materialized: BTreeMap<Vec<u8>, i64>,
}

impl PreparedBindingState {
    fn is_empty(&self) -> bool {
        self.senders.is_empty() && self.materialized.is_empty()
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
struct BindingKey(Vec<u8>);

#[derive(Clone, Debug)]
struct ShapeSender {
    projection: Option<ShapeNotificationProjection>,
}

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
struct ShapeNotificationProjection {
    descriptor: RecordDescriptor,
    expressions: Vec<ProjectionExpr>,
    mapping: Vec<(usize, usize)>,
}

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
struct AutoDirectFamilyKey {
    graph: GraphBuilder,
    binding_descriptor: RecordDescriptor,
    binding_field: String,
    notification_projection: ShapeNotificationProjection,
}

struct AutoDirectFamilyPlan {
    key: AutoDirectFamilyKey,
    graph: GraphBuilder,
    shape: String,
    binding_descriptor: RecordDescriptor,
    binding_field: String,
    binding_value: Value,
    notification_projection: ShapeNotificationProjection,
}

#[derive(Clone, Debug)]
struct BindingSourceState {
    descriptor: RecordDescriptor,
    refcounts: HashMap<BindingKey, usize>,
}

#[derive(Clone, Debug)]
pub(super) struct BindingDelta {
    shape: String,
    descriptor: RecordDescriptor,
    deltas: Vec<RecordDelta>,
}

/// Result of lowering a graph-builder fragment into the deduplicated graph.
#[derive(Clone, Debug)]
struct CompiledNode {
    output: RecordDescriptor,
    node: NodeId,
}

/// Descriptor plus a batch of weighted encoded record changes.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RecordDeltas {
    pub descriptor: RecordDescriptor,
    pub deltas: Vec<RecordDelta>,
}

impl RecordDeltas {
    fn empty(descriptor: RecordDescriptor) -> Self {
        Self {
            descriptor,
            deltas: Vec::new(),
        }
    }

    pub fn is_empty(&self) -> bool {
        self.deltas.is_empty()
    }

    pub fn iter(&self) -> impl Iterator<Item = (BorrowedRecord<'_>, i64)> {
        self.deltas
            .iter()
            .map(|delta| (delta.borrowed(&self.descriptor), delta.weight))
    }

    pub fn to_values(&self) -> Result<Vec<(Vec<Value>, i64)>, records::Error> {
        self.iter()
            .map(|(record, weight)| record.to_values().map(|values| (values, weight)))
            .collect()
    }
}

fn record_deltas_encoded_bytes(deltas: &RecordDeltas) -> usize {
    deltas.deltas.iter().map(|delta| delta.record.len()).sum()
}

fn project_record_deltas(
    records: RecordDeltas,
    projection: &ShapeNotificationProjection,
) -> Result<RecordDeltas, IvmRuntimeError> {
    let deltas = records
        .deltas
        .into_iter()
        .map(|delta| {
            Ok(RecordDelta {
                record: project_record(
                    &projection.expressions,
                    &projection.mapping,
                    projection.descriptor,
                    &records.descriptor,
                    delta.raw(),
                )?,
                weight: delta.weight,
            })
        })
        .collect::<Result<Vec<_>, IvmRuntimeError>>()?;
    Ok(RecordDeltas {
        descriptor: projection.descriptor,
        deltas,
    })
}

fn notification_projection_from_output(
    source: &RecordDescriptor,
    descriptor: RecordDescriptor,
) -> Result<ShapeNotificationProjection, IvmRuntimeError> {
    let mut expressions = Vec::with_capacity(descriptor.fields().len());
    let mut mapping = Vec::with_capacity(descriptor.fields().len());
    for field in descriptor.fields() {
        let name = field
            .name
            .clone()
            .ok_or_else(|| IvmRuntimeError::GraphFieldNotFound("<unnamed>".to_owned()))?;
        let index = source
            .field_index(&name)
            .ok_or_else(|| IvmRuntimeError::GraphFieldNotFound(name.clone()))?;
        expressions.push(ProjectionExpr {
            expression: PlanExpr::field(name),
            output_name: field.name.clone(),
        });
        mapping.push((0, index));
    }
    Ok(ShapeNotificationProjection {
        descriptor,
        expressions,
        mapping,
    })
}

fn route_shape_records(
    shape: &mut PreparedShapeState,
    records: RecordDeltas,
) -> Result<Vec<(SubscriptionId, RecordDeltas)>, IvmRuntimeError> {
    if records.is_empty() {
        return Ok(Vec::new());
    }
    let route_output = shape
        .routing
        .as_ref()
        .map(|routing| routing.output.output)
        .unwrap_or(shape.output.output);
    if records.descriptor != route_output {
        return Err(IvmRuntimeError::GraphOutputMismatch);
    }
    let mapping = shape
        .output_key_fields
        .iter()
        .map(|field| (0, *field))
        .collect::<Vec<_>>();
    let mut by_subscription =
        HashMap::<SubscriptionId, (RecordDescriptor, Vec<RecordDelta>)>::new();
    for delta in records.deltas {
        let key = BindingKey(shape.binding_descriptor.project_record_raw(
            std::slice::from_ref(&records.descriptor),
            &[delta.raw()],
            &mapping,
        )?);
        let visible_delta = if let Some(routing) = &shape.routing {
            RecordDelta {
                record: project_record(
                    &routing.notification_projection.expressions,
                    &routing.notification_projection.mapping,
                    routing.notification_projection.descriptor,
                    &routing.output.output,
                    delta.raw(),
                )?,
                weight: delta.weight,
            }
        } else {
            delta.clone()
        };
        let binding = shape.bindings.entry(key.clone()).or_default();
        let next_weight = binding
            .materialized
            .get(&visible_delta.record)
            .copied()
            .unwrap_or_default()
            + visible_delta.weight;
        if next_weight == 0 {
            binding.materialized.remove(&visible_delta.record);
        } else {
            binding
                .materialized
                .insert(visible_delta.record.clone(), next_weight);
        }
        for (subscription_id, sender) in &binding.senders {
            let delta = if let Some(projection) = &sender.projection {
                RecordDelta {
                    record: project_record(
                        &projection.expressions,
                        &projection.mapping,
                        projection.descriptor,
                        &shape.output.output,
                        visible_delta.raw(),
                    )?,
                    weight: visible_delta.weight,
                }
            } else {
                visible_delta.clone()
            };
            by_subscription
                .entry(*subscription_id)
                .or_insert_with(|| {
                    (
                        sender
                            .projection
                            .as_ref()
                            .map(|projection| projection.descriptor)
                            .unwrap_or(shape.output.output),
                        Vec::new(),
                    )
                })
                .1
                .push(delta);
        }
        if binding.is_empty() {
            shape.bindings.remove(&key);
        }
    }
    Ok(by_subscription
        .into_iter()
        .map(|(subscription_id, (descriptor, deltas))| {
            (subscription_id, RecordDeltas { descriptor, deltas })
        })
        .collect())
}

fn count_builder_nodes(graph: &GraphBuilder) -> usize {
    match graph {
        GraphBuilder::Table { .. }
        | GraphBuilder::InlineRecords { .. }
        | GraphBuilder::Index { .. }
        | GraphBuilder::FrontierSource { .. }
        | GraphBuilder::BindingSource { .. } => 1,
        GraphBuilder::Recursive { seed, step, .. } => {
            1 + count_builder_nodes(seed) + count_builder_nodes(step)
        }
        GraphBuilder::Filter { input, .. }
        | GraphBuilder::Project { input, .. }
        | GraphBuilder::UnwrapNullable { input, .. }
        | GraphBuilder::ArgMaxBy { input, .. }
        | GraphBuilder::ArgMinBy { input, .. }
        | GraphBuilder::TopBy { input, .. } => 1 + count_builder_nodes(input),
        GraphBuilder::Union { inputs } => 1 + inputs.iter().map(count_builder_nodes).sum::<usize>(),
        GraphBuilder::Join { left, right, .. } | GraphBuilder::AntiJoin { left, right, .. } => {
            1 + count_builder_nodes(left) + count_builder_nodes(right)
        }
    }
}

fn builder_contains_binding_source(graph: &GraphBuilder) -> bool {
    match graph {
        GraphBuilder::BindingSource { .. } => true,
        GraphBuilder::Recursive { seed, step, .. } => {
            builder_contains_binding_source(seed) || builder_contains_binding_source(step)
        }
        GraphBuilder::Filter { input, .. }
        | GraphBuilder::Project { input, .. }
        | GraphBuilder::UnwrapNullable { input, .. }
        | GraphBuilder::ArgMaxBy { input, .. }
        | GraphBuilder::ArgMinBy { input, .. }
        | GraphBuilder::TopBy { input, .. } => builder_contains_binding_source(input),
        GraphBuilder::Union { inputs } => inputs.iter().any(builder_contains_binding_source),
        GraphBuilder::Join { left, right, .. } | GraphBuilder::AntiJoin { left, right, .. } => {
            builder_contains_binding_source(left) || builder_contains_binding_source(right)
        }
        GraphBuilder::Table { .. }
        | GraphBuilder::InlineRecords { .. }
        | GraphBuilder::Index { .. }
        | GraphBuilder::FrontierSource { .. } => false,
    }
}

#[derive(Clone)]
struct LiftedLiteralFilter {
    graph: GraphBuilder,
    value: LiteralValue,
}

const AUTO_DIRECT_BINDING_PREFIX: &str = "\0groove.auto_direct.binding.";

fn auto_direct_binding_field(
    graph: &GraphBuilder,
    output: &RecordDescriptor,
    runtime: &IvmRuntime,
) -> Result<String, IvmRuntimeError> {
    let mut occupied = HashSet::new();
    collect_builder_field_names(graph, runtime, &mut occupied)?;
    occupied.extend(
        output
            .fields()
            .iter()
            .filter_map(|field| field.name.as_ref().cloned()),
    );
    for index in 0.. {
        let candidate = format!("{AUTO_DIRECT_BINDING_PREFIX}{index}");
        if !occupied.contains(&candidate) {
            return Ok(candidate);
        }
    }
    unreachable!("unbounded hidden binding field search should always find a free name")
}

fn collect_builder_field_names(
    graph: &GraphBuilder,
    runtime: &IvmRuntime,
    occupied: &mut HashSet<String>,
) -> Result<(), IvmRuntimeError> {
    let output = runtime.infer_builder_output(graph)?;
    occupied.extend(
        output
            .fields()
            .iter()
            .filter_map(|field| field.name.as_ref().cloned()),
    );
    match graph {
        GraphBuilder::Recursive { seed, step, .. } => {
            collect_builder_field_names(seed, runtime, occupied)?;
            collect_builder_field_names(step, runtime, occupied)?;
        }
        GraphBuilder::Filter { input, .. }
        | GraphBuilder::Project { input, .. }
        | GraphBuilder::UnwrapNullable { input, .. }
        | GraphBuilder::ArgMaxBy { input, .. }
        | GraphBuilder::ArgMinBy { input, .. }
        | GraphBuilder::TopBy { input, .. } => {
            collect_builder_field_names(input, runtime, occupied)?;
        }
        GraphBuilder::Union { inputs } => {
            for input in inputs {
                collect_builder_field_names(input, runtime, occupied)?;
            }
        }
        GraphBuilder::Join { left, right, .. } | GraphBuilder::AntiJoin { left, right, .. } => {
            collect_builder_field_names(left, runtime, occupied)?;
            collect_builder_field_names(right, runtime, occupied)?;
        }
        GraphBuilder::Table { .. }
        | GraphBuilder::InlineRecords { .. }
        | GraphBuilder::Index { .. }
        | GraphBuilder::FrontierSource { .. }
        | GraphBuilder::BindingSource { .. } => {}
    }
    Ok(())
}

fn lift_literal_filter(
    runtime: &IvmRuntime,
    graph: &GraphBuilder,
    binding_field: &str,
) -> Result<Option<LiftedLiteralFilter>, IvmRuntimeError> {
    match graph {
        GraphBuilder::Filter { input, predicate } => {
            if let PredicateExpr::Eq { field, value } = predicate {
                let joined =
                    literal_filter_binding_join((**input).clone(), field, value, binding_field)?;
                let input_output = runtime.infer_builder_output(input)?;
                let mut fields = input_output
                    .fields()
                    .iter()
                    .filter_map(|field| {
                        let name = field.name.clone()?;
                        Some(ProjectField::renamed(format!("left.{name}"), name))
                    })
                    .collect::<Vec<_>>();
                fields.push(ProjectField::renamed(
                    format!("right.{binding_field}"),
                    binding_field.to_owned(),
                ));
                return Ok(Some(LiftedLiteralFilter {
                    graph: joined.project_fields(fields),
                    value: value.clone(),
                }));
            }
            if let Some(lifted) = lift_literal_filter(runtime, input, binding_field)? {
                return Ok(Some(LiftedLiteralFilter {
                    graph: GraphBuilder::Filter {
                        input: Box::new(lifted.graph),
                        predicate: predicate.clone(),
                    },
                    value: lifted.value,
                }));
            }
            Ok(None)
        }
        GraphBuilder::Project { input, fields } => {
            if let GraphBuilder::Join {
                left,
                right,
                left_on,
                right_on,
            } = input.as_ref()
            {
                if let Some(lifted) = lift_literal_filter(runtime, left, binding_field)? {
                    let joined = GraphBuilder::Join {
                        left: Box::new(lifted.graph),
                        right: right.clone(),
                        left_on: left_on.clone(),
                        right_on: right_on.clone(),
                    };
                    let mut fields =
                        project_fields_against_rewritten_input(runtime, input, &joined, fields)?;
                    append_binding_project_field(
                        &mut fields,
                        binding_field,
                        binding_project_source(&joined, binding_field),
                    );
                    return Ok(Some(LiftedLiteralFilter {
                        graph: joined.project_fields(fields),
                        value: lifted.value,
                    }));
                }
                if let Some(lifted) = lift_literal_filter(runtime, right, binding_field)? {
                    let joined = GraphBuilder::Join {
                        left: left.clone(),
                        right: Box::new(lifted.graph),
                        left_on: left_on.clone(),
                        right_on: right_on.clone(),
                    };
                    let mut fields =
                        project_fields_against_rewritten_input(runtime, input, &joined, fields)?;
                    append_binding_project_field(
                        &mut fields,
                        binding_field,
                        binding_project_source(&joined, binding_field),
                    );
                    return Ok(Some(LiftedLiteralFilter {
                        graph: joined.project_fields(fields),
                        value: lifted.value,
                    }));
                }
            }
            if let GraphBuilder::Filter {
                input: filtered_input,
                predicate: PredicateExpr::Eq { field, value },
            } = input.as_ref()
            {
                let joined = literal_filter_binding_join(
                    (**filtered_input).clone(),
                    field,
                    value,
                    binding_field,
                )?;
                let input_output = runtime.infer_builder_output(filtered_input)?;
                let mut fields = fields
                    .iter()
                    .map(|field| match &field.expression {
                        ProjectExpr::Field(source) => {
                            let source =
                                project_source_from_joined_filter_input(&input_output, source)?;
                            Ok(ProjectField::renamed(source, field.output_name.clone()))
                        }
                        ProjectExpr::Literal(value) => Ok(ProjectField::literal(
                            field.output_name.clone(),
                            value.clone(),
                        )),
                        ProjectExpr::Null(value_type) => Ok(ProjectField::null_typed(
                            field.output_name.clone(),
                            value_type.clone(),
                        )),
                        ProjectExpr::Nullable(source) => {
                            let source =
                                project_source_from_joined_filter_input(&input_output, source)?;
                            Ok(ProjectField::nullable(source, field.output_name.clone()))
                        }
                    })
                    .collect::<Result<Vec<_>, IvmRuntimeError>>()?;
                fields.push(ProjectField::renamed(
                    format!("right.{binding_field}"),
                    binding_field.to_owned(),
                ));
                return Ok(Some(LiftedLiteralFilter {
                    graph: joined.project_fields(fields),
                    value: value.clone(),
                }));
            }
            let Some(lifted) = lift_literal_filter(runtime, input, binding_field)? else {
                return Ok(None);
            };
            let mut fields = fields.clone();
            append_binding_project_field(
                &mut fields,
                binding_field,
                binding_project_source(&lifted.graph, binding_field),
            );
            Ok(Some(LiftedLiteralFilter {
                graph: GraphBuilder::Project {
                    input: Box::new(lifted.graph),
                    fields,
                },
                value: lifted.value,
            }))
        }
        GraphBuilder::Join {
            left,
            right,
            left_on,
            right_on,
        } => {
            if let Some(lifted) = lift_literal_filter(runtime, left, binding_field)? {
                let original_output = runtime.infer_builder_output(graph)?;
                let joined = GraphBuilder::Join {
                    left: Box::new(lifted.graph),
                    right: right.clone(),
                    left_on: left_on.clone(),
                    right_on: right_on.clone(),
                };
                return Ok(Some(LiftedLiteralFilter {
                    graph: project_to_output_with_binding(
                        runtime,
                        joined,
                        &original_output,
                        binding_field,
                    )?,
                    value: lifted.value,
                }));
            }
            if let Some(lifted) = lift_literal_filter(runtime, right, binding_field)? {
                let original_output = runtime.infer_builder_output(graph)?;
                let joined = GraphBuilder::Join {
                    left: left.clone(),
                    right: Box::new(lifted.graph),
                    left_on: left_on.clone(),
                    right_on: right_on.clone(),
                };
                return Ok(Some(LiftedLiteralFilter {
                    graph: project_to_output_with_binding(
                        runtime,
                        joined,
                        &original_output,
                        binding_field,
                    )?,
                    value: lifted.value,
                }));
            }
            Ok(None)
        }
        GraphBuilder::AntiJoin {
            left,
            right,
            left_on,
            right_on,
        } => {
            let Some(lifted) = lift_literal_filter(runtime, left, binding_field)? else {
                return Ok(None);
            };
            Ok(Some(LiftedLiteralFilter {
                graph: GraphBuilder::AntiJoin {
                    left: Box::new(lifted.graph),
                    right: right.clone(),
                    left_on: left_on.clone(),
                    right_on: right_on.clone(),
                },
                value: lifted.value,
            }))
        }
        GraphBuilder::Recursive { .. } => Ok(None),
        GraphBuilder::Union { .. } => Ok(None),
        GraphBuilder::UnwrapNullable { input, field } => {
            let Some(lifted) = lift_literal_filter(runtime, input, binding_field)? else {
                return Ok(None);
            };
            Ok(Some(LiftedLiteralFilter {
                graph: GraphBuilder::UnwrapNullable {
                    input: Box::new(lifted.graph),
                    field: field.clone(),
                },
                value: lifted.value,
            }))
        }
        GraphBuilder::ArgMaxBy {
            input,
            group_cols,
            order_cols,
        } => {
            let Some(lifted) = lift_literal_filter(runtime, input, binding_field)? else {
                return Ok(None);
            };
            let mut group_cols = group_cols.clone();
            group_cols.push(FieldRef::name(binding_field));
            Ok(Some(LiftedLiteralFilter {
                graph: GraphBuilder::ArgMaxBy {
                    input: Box::new(lifted.graph),
                    group_cols,
                    order_cols: order_cols.clone(),
                },
                value: lifted.value,
            }))
        }
        GraphBuilder::ArgMinBy {
            input,
            group_cols,
            order_cols,
        } => {
            let Some(lifted) = lift_literal_filter(runtime, input, binding_field)? else {
                return Ok(None);
            };
            let mut group_cols = group_cols.clone();
            group_cols.push(FieldRef::name(binding_field));
            Ok(Some(LiftedLiteralFilter {
                graph: GraphBuilder::ArgMinBy {
                    input: Box::new(lifted.graph),
                    group_cols,
                    order_cols: order_cols.clone(),
                },
                value: lifted.value,
            }))
        }
        GraphBuilder::TopBy {
            input,
            group_cols,
            order_cols,
            tie_cols,
            offset,
            limit,
        } => {
            let Some(lifted) = lift_literal_filter(runtime, input, binding_field)? else {
                return Ok(None);
            };
            let mut group_cols = group_cols.clone();
            group_cols.push(FieldRef::name(binding_field));
            Ok(Some(LiftedLiteralFilter {
                graph: GraphBuilder::TopBy {
                    input: Box::new(lifted.graph),
                    group_cols,
                    order_cols: order_cols.clone(),
                    tie_cols: tie_cols.clone(),
                    offset: *offset,
                    limit: *limit,
                },
                value: lifted.value,
            }))
        }
        GraphBuilder::Table { .. }
        | GraphBuilder::InlineRecords { .. }
        | GraphBuilder::Index { .. }
        | GraphBuilder::FrontierSource { .. }
        | GraphBuilder::BindingSource { .. } => Ok(None),
    }
}

fn literal_filter_binding_join(
    input: GraphBuilder,
    field: &str,
    value: &LiteralValue,
    binding_field: &str,
) -> Result<GraphBuilder, IvmRuntimeError> {
    let value_type = value
        .value_type()
        .ok_or(IvmRuntimeError::UnsupportedOperator)?;
    let binding = GraphBuilder::binding_source(
        "__auto_direct_shape",
        RecordDescriptor::new([(binding_field.to_owned(), value_type)]),
    );
    Ok(GraphBuilder::join(
        input,
        binding,
        [field.to_owned()],
        [binding_field.to_owned()],
    ))
}

fn project_source_from_joined_filter_input(
    input_output: &RecordDescriptor,
    source: &FieldRef,
) -> Result<String, IvmRuntimeError> {
    Ok(format!("left.{}", field_ref_name(input_output, source)?))
}

fn project_fields_against_rewritten_input(
    runtime: &IvmRuntime,
    original_input: &GraphBuilder,
    rewritten_input: &GraphBuilder,
    fields: &[ProjectField],
) -> Result<Vec<ProjectField>, IvmRuntimeError> {
    let original_output = runtime.infer_builder_output(original_input)?;
    let rewritten_output = runtime.infer_builder_output(rewritten_input)?;
    fields
        .iter()
        .map(|field| {
            let (field_ref, wrap_nullable) = match &field.expression {
                ProjectExpr::Field(field_ref) => (field_ref, false),
                ProjectExpr::Nullable(field_ref) => (field_ref, true),
                ProjectExpr::Literal(_) | ProjectExpr::Null(_) => return Ok(field.clone()),
            };
            let source = field_ref_name(&original_output, field_ref)?;
            if rewritten_output.field_index(&source).is_none() {
                return Err(IvmRuntimeError::GraphFieldNotFound(source));
            }
            if wrap_nullable {
                Ok(ProjectField::nullable(source, field.output_name.clone()))
            } else {
                Ok(ProjectField::renamed(source, field.output_name.clone()))
            }
        })
        .collect()
}

fn project_to_output_with_binding(
    runtime: &IvmRuntime,
    graph: GraphBuilder,
    original_output: &RecordDescriptor,
    binding_field: &str,
) -> Result<GraphBuilder, IvmRuntimeError> {
    let lifted_output = runtime.infer_builder_output(&graph)?;
    let mut fields = original_output
        .fields()
        .iter()
        .map(|field| {
            let name = field
                .name
                .clone()
                .ok_or_else(|| IvmRuntimeError::GraphFieldNotFound("<unnamed>".to_owned()))?;
            if lifted_output.field_index(&name).is_none() {
                return Err(IvmRuntimeError::GraphFieldNotFound(name));
            }
            Ok(ProjectField::renamed(name.clone(), name))
        })
        .collect::<Result<Vec<_>, IvmRuntimeError>>()?;
    append_binding_project_field(
        &mut fields,
        binding_field,
        binding_project_source(&graph, binding_field),
    );
    Ok(GraphBuilder::Project {
        input: Box::new(graph),
        fields,
    })
}

fn append_binding_project_field(
    fields: &mut Vec<ProjectField>,
    binding_field: &str,
    source: String,
) {
    if !fields
        .iter()
        .any(|field| field.output_name == binding_field)
    {
        fields.push(ProjectField::renamed(source, binding_field.to_owned()));
    }
}

fn binding_project_source(input: &GraphBuilder, binding_field: &str) -> String {
    match input {
        GraphBuilder::Join { left, right, .. } | GraphBuilder::AntiJoin { left, right, .. } => {
            if graph_outputs_binding(left, binding_field) {
                format!("left.{binding_field}")
            } else if graph_outputs_binding(right, binding_field) {
                format!("right.{binding_field}")
            } else {
                binding_field.to_owned()
            }
        }
        _ => binding_field.to_owned(),
    }
}

fn graph_outputs_binding(graph: &GraphBuilder, binding_field: &str) -> bool {
    match graph {
        GraphBuilder::BindingSource { output, .. }
        | GraphBuilder::FrontierSource { output, .. }
        | GraphBuilder::InlineRecords { output, .. } => output.field_index(binding_field).is_some(),
        GraphBuilder::Project { fields, .. } => fields
            .iter()
            .any(|field| field.output_name == binding_field),
        GraphBuilder::Filter { input, .. }
        | GraphBuilder::UnwrapNullable { input, .. }
        | GraphBuilder::ArgMaxBy { input, .. }
        | GraphBuilder::ArgMinBy { input, .. }
        | GraphBuilder::TopBy { input, .. } => graph_outputs_binding(input, binding_field),
        GraphBuilder::Recursive { seed, .. } => graph_outputs_binding(seed, binding_field),
        GraphBuilder::Join { left, right, .. } | GraphBuilder::AntiJoin { left, right, .. } => {
            graph_outputs_binding(left, binding_field)
                || graph_outputs_binding(right, binding_field)
        }
        GraphBuilder::Union { inputs } => inputs
            .iter()
            .any(|input| graph_outputs_binding(input, binding_field)),
        GraphBuilder::Table { .. } | GraphBuilder::Index { .. } => false,
    }
}

#[allow(dead_code)]
fn propagate_binding_through_frontier(
    graph: &GraphBuilder,
    frontier: &FrontierName,
    binding_field: &str,
    binding_type: ValueType,
) -> Option<GraphBuilder> {
    match graph {
        GraphBuilder::FrontierSource { binding, output } if binding == frontier => {
            let fields = output
                .fields()
                .iter()
                .filter_map(|field| Some((field.name.clone()?, field.value_type.clone())));
            let fields = if output.field_index(binding_field).is_some() {
                fields.collect::<Vec<_>>()
            } else {
                fields
                    .chain([(binding_field.to_owned(), binding_type.clone())])
                    .collect::<Vec<_>>()
            };
            Some(GraphBuilder::frontier_source(
                binding.0.clone(),
                RecordDescriptor::new(fields),
            ))
        }
        GraphBuilder::Filter { input, predicate } => {
            let input =
                propagate_binding_through_frontier(input, frontier, binding_field, binding_type)?;
            Some(GraphBuilder::Filter {
                input: Box::new(input),
                predicate: predicate.clone(),
            })
        }
        GraphBuilder::Project { input, fields } => {
            let input =
                propagate_binding_through_frontier(input, frontier, binding_field, binding_type)?;
            let mut fields = fields.clone();
            append_binding_project_field(
                &mut fields,
                binding_field,
                binding_project_source(&input, binding_field),
            );
            Some(GraphBuilder::Project {
                input: Box::new(input),
                fields,
            })
        }
        GraphBuilder::Join {
            left,
            right,
            left_on,
            right_on,
        } => {
            let left = propagate_binding_through_frontier(
                left,
                frontier,
                binding_field,
                binding_type.clone(),
            )
            .unwrap_or_else(|| (**left).clone());
            let right =
                propagate_binding_through_frontier(right, frontier, binding_field, binding_type)
                    .unwrap_or_else(|| (**right).clone());
            Some(GraphBuilder::Join {
                left: Box::new(left),
                right: Box::new(right),
                left_on: left_on.clone(),
                right_on: right_on.clone(),
            })
        }
        GraphBuilder::AntiJoin {
            left,
            right,
            left_on,
            right_on,
        } => {
            let left =
                propagate_binding_through_frontier(left, frontier, binding_field, binding_type)?;
            Some(GraphBuilder::AntiJoin {
                left: Box::new(left),
                right: right.clone(),
                left_on: left_on.clone(),
                right_on: right_on.clone(),
            })
        }
        GraphBuilder::Table { .. }
        | GraphBuilder::InlineRecords { .. }
        | GraphBuilder::Index { .. }
        | GraphBuilder::FrontierSource { .. }
        | GraphBuilder::BindingSource { .. }
        | GraphBuilder::Recursive { .. }
        | GraphBuilder::UnwrapNullable { .. }
        | GraphBuilder::ArgMaxBy { .. }
        | GraphBuilder::ArgMinBy { .. }
        | GraphBuilder::TopBy { .. }
        | GraphBuilder::Union { .. } => None,
    }
}

fn replace_binding_shape(graph: GraphBuilder, shape: &str) -> GraphBuilder {
    match graph {
        GraphBuilder::BindingSource { output, .. } => GraphBuilder::binding_source(shape, output),
        GraphBuilder::Recursive {
            seed,
            step,
            frontier,
            max_iters,
        } => GraphBuilder::Recursive {
            seed: Box::new(replace_binding_shape(*seed, shape)),
            step: Box::new(replace_binding_shape(*step, shape)),
            frontier,
            max_iters,
        },
        GraphBuilder::Filter { input, predicate } => GraphBuilder::Filter {
            input: Box::new(replace_binding_shape(*input, shape)),
            predicate,
        },
        GraphBuilder::Project { input, fields } => GraphBuilder::Project {
            input: Box::new(replace_binding_shape(*input, shape)),
            fields,
        },
        GraphBuilder::UnwrapNullable { input, field } => GraphBuilder::UnwrapNullable {
            input: Box::new(replace_binding_shape(*input, shape)),
            field,
        },
        GraphBuilder::ArgMaxBy {
            input,
            group_cols,
            order_cols,
        } => GraphBuilder::ArgMaxBy {
            input: Box::new(replace_binding_shape(*input, shape)),
            group_cols,
            order_cols,
        },
        GraphBuilder::ArgMinBy {
            input,
            group_cols,
            order_cols,
        } => GraphBuilder::ArgMinBy {
            input: Box::new(replace_binding_shape(*input, shape)),
            group_cols,
            order_cols,
        },
        GraphBuilder::TopBy {
            input,
            group_cols,
            order_cols,
            tie_cols,
            offset,
            limit,
        } => GraphBuilder::TopBy {
            input: Box::new(replace_binding_shape(*input, shape)),
            group_cols,
            order_cols,
            tie_cols,
            offset,
            limit,
        },
        GraphBuilder::Union { inputs } => GraphBuilder::Union {
            inputs: inputs
                .into_iter()
                .map(|input| replace_binding_shape(input, shape))
                .collect(),
        },
        GraphBuilder::Join {
            left,
            right,
            left_on,
            right_on,
        } => GraphBuilder::Join {
            left: Box::new(replace_binding_shape(*left, shape)),
            right: Box::new(replace_binding_shape(*right, shape)),
            left_on,
            right_on,
        },
        GraphBuilder::AntiJoin {
            left,
            right,
            left_on,
            right_on,
        } => GraphBuilder::AntiJoin {
            left: Box::new(replace_binding_shape(*left, shape)),
            right: Box::new(replace_binding_shape(*right, shape)),
            left_on,
            right_on,
        },
        graph => graph,
    }
}

/// Retention and GC metadata for a deduplicated graph node.
#[derive(Clone, Debug, Default)]
struct NodeRuntimeMeta {
    retainers: HashSet<Retainer>,
    last_used_tick: u64,
}

/// Namespace for stateless operator helper methods.
struct NodeState;

impl NodeState {
    fn update_table_source(
        input: &TableSourceOp,
        output_desc: &RecordDescriptor,
        table_deltas: &[TableDelta],
    ) -> Result<RecordDeltas, IvmRuntimeError> {
        let mut deltas = Vec::new();
        for delta in table_deltas
            .iter()
            .filter(|delta| delta.table == input.table)
        {
            deltas.extend(delta.deltas.iter().cloned());
        }
        Ok(RecordDeltas {
            descriptor: *output_desc,
            deltas,
        })
    }

    fn update_binding_source(
        input: &BindingSourceOp,
        output_desc: &RecordDescriptor,
        binding_deltas: &[BindingDelta],
        binding_snapshots: &HashMap<String, RecordDeltas>,
        mode: ArrangementUpdateMode,
    ) -> Result<RecordDeltas, IvmRuntimeError> {
        if mode == ArrangementUpdateMode::Replace {
            let Some(snapshot) = binding_snapshots.get(&input.shape) else {
                return Ok(RecordDeltas::empty(*output_desc));
            };
            return project_binding_source_deltas(snapshot, output_desc);
        }
        let mut deltas = Vec::new();
        for delta in binding_deltas
            .iter()
            .filter(|delta| delta.shape == input.shape)
        {
            deltas.extend(
                project_binding_source_deltas(
                    &RecordDeltas {
                        descriptor: delta.descriptor,
                        deltas: delta.deltas.clone(),
                    },
                    output_desc,
                )?
                .deltas,
            );
        }
        Ok(RecordDeltas {
            descriptor: *output_desc,
            deltas,
        })
    }

    fn update_filter(
        filter: &FilterOp,
        output_desc: RecordDescriptor,
        input: RecordDeltas,
    ) -> Result<RecordDeltas, IvmRuntimeError> {
        let predicate = &filter.predicate;
        let mut deltas = Vec::new();
        for delta in input.deltas {
            if predicate.matches(delta.borrowed(&input.descriptor))? {
                deltas.push(delta);
            }
        }
        Ok(RecordDeltas {
            descriptor: output_desc,
            deltas,
        })
    }

    fn update_map_project(
        project: &MapProjectOp,
        output_desc: RecordDescriptor,
        input: RecordDeltas,
    ) -> Result<RecordDeltas, IvmRuntimeError> {
        let deltas = input
            .deltas
            .iter()
            .map(|delta| {
                let record = project_record(
                    &project.expressions,
                    &project.mapping,
                    output_desc,
                    &input.descriptor,
                    delta.raw(),
                )?;
                Ok(RecordDelta {
                    record,
                    weight: delta.weight,
                })
            })
            .collect::<Result<Vec<_>, IvmRuntimeError>>()?;
        Ok(RecordDeltas {
            descriptor: output_desc,
            deltas,
        })
    }

    fn update_unwrap_nullable(
        unwrap: &UnwrapNullableOp,
        output_desc: RecordDescriptor,
        input: RecordDeltas,
    ) -> Result<RecordDeltas, IvmRuntimeError> {
        let mut deltas = Vec::new();
        for delta in input.deltas {
            let values = delta
                .borrowed(&input.descriptor)
                .to_values()
                .map_err(IvmRuntimeError::RecordEncoding)?;
            let Value::Nullable(value) = &values[unwrap.field_idx] else {
                return Err(IvmRuntimeError::UnsupportedOperator);
            };
            let Some(inner) = value.as_ref().map(|value| (**value).clone()) else {
                continue;
            };
            let mut output_values = values;
            output_values[unwrap.field_idx] = inner;
            deltas.push(RecordDelta {
                record: output_desc.create(&output_values)?,
                weight: delta.weight,
            });
        }
        Ok(RecordDeltas {
            descriptor: output_desc,
            deltas,
        })
    }

    fn update_union(
        output_desc: RecordDescriptor,
        inputs: Vec<RecordDeltas>,
    ) -> Result<RecordDeltas, IvmRuntimeError> {
        let mut deltas = Vec::new();
        for input in inputs {
            if input.deltas.is_empty() {
                continue;
            }
            if output_desc != input.descriptor {
                return Err(IvmRuntimeError::GraphOutputMismatch);
            }
            deltas.extend(input.deltas);
        }
        Ok(RecordDeltas {
            descriptor: output_desc,
            deltas,
        })
    }

    fn update_index_by(
        index_by: &IndexByOp,
        output_desc: RecordDescriptor,
        input: RecordDeltas,
    ) -> Result<RecordDeltas, IvmRuntimeError> {
        let deltas = apply_index_by(index_by, &input.descriptor, &input.deltas)?;
        Ok(RecordDeltas {
            descriptor: output_desc,
            deltas,
        })
    }

    fn update_persist(
        persist: &PersistOp,
        output_desc: RecordDescriptor,
        input: RecordDeltas,
        storage: &impl OrderedKvStorage,
    ) -> Result<RecordDeltas, IvmRuntimeError> {
        apply_persist_delta(
            storage,
            &persist.storage,
            &persist.key_fields,
            persist.unique,
            &input,
        )?;
        Ok(RecordDeltas {
            descriptor: output_desc,
            deltas: input.deltas,
        })
    }
}

#[derive(Clone, Debug)]
enum OperatorState {
    Stateless,
    Join(JoinState),
    AntiJoin(AntiJoinState),
    Recursive(AsOf<RecursiveState, Tick>),
}

fn operator_state_for(operator: &OpType) -> OperatorState {
    match operator {
        OpType::Join(_) => OperatorState::Join(JoinState),
        OpType::AntiJoin(_) => OperatorState::AntiJoin(AntiJoinState),
        OpType::Recursive(_) => OperatorState::Recursive(AsOf::new(RecursiveState::default())),
        _ => OperatorState::Stateless,
    }
}

fn plan_expr_names(expressions: &[PlanExpr]) -> Vec<String> {
    expressions
        .iter()
        .filter_map(|expr| match expr {
            PlanExpr::Field(name) | PlanExpr::Nullable(name) => Some(name.clone()),
            PlanExpr::Literal(_) | PlanExpr::Null(_) => None,
        })
        .collect()
}

fn builder_contains_arg_max_by(graph: &GraphBuilder) -> bool {
    match graph {
        GraphBuilder::ArgMaxBy { .. }
        | GraphBuilder::ArgMinBy { .. }
        | GraphBuilder::TopBy { .. } => true,
        GraphBuilder::Recursive { seed, step, .. } => {
            builder_contains_arg_max_by(seed) || builder_contains_arg_max_by(step)
        }
        GraphBuilder::Filter { input, .. }
        | GraphBuilder::Project { input, .. }
        | GraphBuilder::UnwrapNullable { input, .. } => builder_contains_arg_max_by(input),
        GraphBuilder::Union { inputs } => inputs.iter().any(builder_contains_arg_max_by),
        GraphBuilder::Join { left, right, .. } | GraphBuilder::AntiJoin { left, right, .. } => {
            builder_contains_arg_max_by(left) || builder_contains_arg_max_by(right)
        }
        GraphBuilder::Table { .. }
        | GraphBuilder::InlineRecords { .. }
        | GraphBuilder::Index { .. }
        | GraphBuilder::FrontierSource { .. }
        | GraphBuilder::BindingSource { .. } => false,
    }
}

fn builder_contains_recursive(graph: &GraphBuilder) -> bool {
    match graph {
        GraphBuilder::Recursive { .. } => true,
        GraphBuilder::Filter { input, .. }
        | GraphBuilder::Project { input, .. }
        | GraphBuilder::UnwrapNullable { input, .. }
        | GraphBuilder::ArgMaxBy { input, .. }
        | GraphBuilder::ArgMinBy { input, .. }
        | GraphBuilder::TopBy { input, .. } => builder_contains_recursive(input),
        GraphBuilder::Union { inputs } => inputs.iter().any(builder_contains_recursive),
        GraphBuilder::Join { left, right, .. } | GraphBuilder::AntiJoin { left, right, .. } => {
            builder_contains_recursive(left) || builder_contains_recursive(right)
        }
        GraphBuilder::Table { .. }
        | GraphBuilder::InlineRecords { .. }
        | GraphBuilder::Index { .. }
        | GraphBuilder::FrontierSource { .. }
        | GraphBuilder::BindingSource { .. } => false,
    }
}

fn validate_arg_by_primary_key_indices(
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
struct TickEvaluator<'a, S> {
    graph: &'a IvmGraph,
    table_deltas: &'a [TableDelta],
    binding_deltas: &'a [BindingDelta],
    binding_snapshots: &'a HashMap<String, RecordDeltas>,
    current_tick: u64,
    operator_states: &'a mut HashMap<OperatorStateKey, OperatorState>,
    arrangement_states: &'a mut HashMap<ArrangementKey, AsOf<ArrangementState, SubTick>>,
    eval_memo: &'a mut HashMap<EvalMemoKey, RecordDeltas>,
    storage: Option<&'a S>,
    context: EvalContext,
    metrics: &'a mut TickMetrics,
}

/// Borrowed runtime pieces used by recursive evaluation to run child graphs.
/// This avoids giving recursion ownership of the whole [`IvmRuntime`].
pub(super) struct GraphRuntimeView<'a, S> {
    pub(super) graph: &'a IvmGraph,
    pub(super) table_deltas: &'a [TableDelta],
    pub(super) binding_deltas: &'a [BindingDelta],
    pub(super) binding_snapshots: &'a HashMap<String, RecordDeltas>,
    pub(super) current_tick: u64,
    operator_states: &'a mut HashMap<OperatorStateKey, OperatorState>,
    arrangement_states: &'a mut HashMap<ArrangementKey, AsOf<ArrangementState, SubTick>>,
    eval_memo: &'a mut HashMap<EvalMemoKey, RecordDeltas>,
    pub(super) storage: &'a S,
    pub(super) scope: ScopePath,
    pub(super) metrics: &'a mut TickMetrics,
}

#[allow(clippy::too_many_arguments)]
fn graph_runtime_view<'a, S>(
    graph: &'a IvmGraph,
    table_deltas: &'a [TableDelta],
    binding_deltas: &'a [BindingDelta],
    binding_snapshots: &'a HashMap<String, RecordDeltas>,
    current_tick: u64,
    operator_states: &'a mut HashMap<OperatorStateKey, OperatorState>,
    arrangement_states: &'a mut HashMap<ArrangementKey, AsOf<ArrangementState, SubTick>>,
    eval_memo: &'a mut HashMap<EvalMemoKey, RecordDeltas>,
    storage: &'a S,
    scope: ScopePath,
    metrics: &'a mut TickMetrics,
) -> GraphRuntimeView<'a, S> {
    GraphRuntimeView {
        graph,
        table_deltas,
        binding_deltas,
        binding_snapshots,
        current_tick,
        operator_states,
        arrangement_states,
        eval_memo,
        storage,
        scope,
        metrics,
    }
}

impl<'a, S> GraphRuntimeView<'a, S>
where
    S: OrderedKvStorage,
{
    pub(super) fn eval_with_binding(
        &mut self,
        sub_tick: u64,
        binding: FrontierName,
        deltas: RecordDeltas,
        node: NodeId,
    ) -> Result<RecordDeltas, IvmRuntimeError> {
        let mut evaluator = TickEvaluator {
            graph: self.graph,
            table_deltas: self.table_deltas,
            binding_deltas: self.binding_deltas,
            binding_snapshots: self.binding_snapshots,
            current_tick: self.current_tick,
            operator_states: self.operator_states,
            arrangement_states: self.arrangement_states,
            eval_memo: self.eval_memo,
            storage: Some(self.storage),
            context: EvalContext::with_binding(self.scope.clone(), sub_tick, binding, deltas),
            metrics: self.metrics,
        };
        evaluator.update_node(node)
    }

    pub(super) fn eval_with_binding_and_table_deltas(
        &mut self,
        table_deltas: &[TableDelta],
        sub_tick: u64,
        binding: FrontierName,
        deltas: RecordDeltas,
        node: NodeId,
    ) -> Result<RecordDeltas, IvmRuntimeError> {
        let mut isolated_memo = HashMap::new();
        let mut evaluator = TickEvaluator {
            graph: self.graph,
            table_deltas,
            binding_deltas: self.binding_deltas,
            binding_snapshots: self.binding_snapshots,
            current_tick: self.current_tick,
            operator_states: self.operator_states,
            arrangement_states: self.arrangement_states,
            eval_memo: &mut isolated_memo,
            storage: Some(self.storage),
            context: EvalContext::with_binding_and_arrangement_mode(
                self.scope.clone(),
                sub_tick,
                binding,
                deltas,
                ArrangementUpdateMode::Replace,
            ),
            metrics: self.metrics,
        };
        evaluator.update_node(node)
    }

    pub(super) fn eval_root(&mut self, node: NodeId) -> Result<RecordDeltas, IvmRuntimeError> {
        let mut evaluator = TickEvaluator {
            graph: self.graph,
            table_deltas: self.table_deltas,
            binding_deltas: self.binding_deltas,
            binding_snapshots: self.binding_snapshots,
            current_tick: self.current_tick,
            operator_states: self.operator_states,
            arrangement_states: self.arrangement_states,
            eval_memo: self.eval_memo,
            storage: Some(self.storage),
            context: EvalContext {
                scope: self.scope.clone(),
                sub_tick: 0,
                bindings: HashMap::new(),
                arrangement_update_mode: ArrangementUpdateMode::Accumulate,
                eval_mode: EvalMode::Tick,
            },
            metrics: self.metrics,
        };
        evaluator.update_node(node)
    }
}

impl<S> TickEvaluator<'_, S>
where
    S: OrderedKvStorage,
{
    fn update_node(&mut self, node: NodeId) -> Result<RecordDeltas, IvmRuntimeError> {
        let memo_key = self.memo_key(node)?;
        if let Some(records) = self.eval_memo.get(&memo_key) {
            return Ok(records.clone());
        }

        let graph_node = self
            .graph
            .node(node)
            .ok_or(IvmRuntimeError::GraphNodeNotFound(node))?
            .clone();

        let output_desc = graph_node.descriptor.output;
        if self.context.sub_tick > 1 && !self.depends_on_context(node, &mut HashSet::new())? {
            let result = RecordDeltas::empty(output_desc);
            self.eval_memo.insert(memo_key, result.clone());
            return Ok(result);
        }
        let result = match &graph_node.descriptor.operator {
            OpType::TableSource(input) => {
                NodeState::update_table_source(input, &output_desc, self.table_deltas)
            }
            OpType::InlineRecords(inline) if self.context.eval_mode == EvalMode::Hydrate => {
                Ok(RecordDeltas {
                    descriptor: output_desc,
                    deltas: inline
                        .records
                        .iter()
                        .cloned()
                        .map(|record| RecordDelta { record, weight: 1 })
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
            OpType::FrontierSource(frontier_source) => {
                self.frontier_source(frontier_source, &graph_node.descriptor.output)
            }
            OpType::Filter(filter) => {
                let input = self.update_unary_input(&graph_node, node)?;
                NodeState::update_filter(filter, output_desc, input)
            }
            OpType::MapProject(project) => {
                let input = self.update_unary_input(&graph_node, node)?;
                NodeState::update_map_project(project, output_desc, input)
            }
            OpType::UnwrapNullable(unwrap) => {
                let input = self.update_unary_input(&graph_node, node)?;
                NodeState::update_unwrap_nullable(unwrap, output_desc, input)
            }
            OpType::ArgMaxBy(arg_max_by) => {
                let input = self.update_unary_input(&graph_node, node)?;
                self.update_arg_by(
                    node,
                    ArgBySpec {
                        group_fields: &arg_max_by.group_fields,
                        group_field_indices: &arg_max_by.group_field_indices,
                        primary_key_field_indices: &arg_max_by.primary_key_field_indices,
                        direction: ArgByDirection::Max,
                    },
                    output_desc,
                    input,
                )
            }
            OpType::ArgMinBy(arg_min_by) => {
                let input = self.update_unary_input(&graph_node, node)?;
                self.update_arg_by(
                    node,
                    ArgBySpec {
                        group_fields: &arg_min_by.group_fields,
                        group_field_indices: &arg_min_by.group_field_indices,
                        primary_key_field_indices: &arg_min_by.primary_key_field_indices,
                        direction: ArgByDirection::Min,
                    },
                    output_desc,
                    input,
                )
            }
            OpType::TopBy(top_by) => {
                let input = self.update_unary_input(&graph_node, node)?;
                self.update_top_by(node, top_by, output_desc, input)
            }
            OpType::IndexBy(index_by) => {
                let input = self.update_unary_input(&graph_node, node)?;
                NodeState::update_index_by(index_by, output_desc, input)
            }
            OpType::Union => {
                let inputs = graph_node
                    .descriptor
                    .inputs
                    .iter()
                    .map(|input| self.update_node(*input))
                    .collect::<Result<Vec<_>, _>>()?;
                NodeState::update_union(output_desc, inputs)
            }
            OpType::Join(join) => {
                let [left_input, right_input] = graph_node.descriptor.inputs.as_slice() else {
                    return Err(IvmRuntimeError::GraphInputArityMismatch(node));
                };
                let left = self.update_node(*left_input)?;
                let right = self.update_node(*right_input)?;
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
            OpType::AntiJoin(join) => {
                let [left_input, right_input] = graph_node.descriptor.inputs.as_slice() else {
                    return Err(IvmRuntimeError::GraphInputArityMismatch(node));
                };
                let left = self.update_node(*left_input)?;
                let right = self.update_node(*right_input)?;
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
                let [seed, step] = graph_node.descriptor.inputs.as_slice() else {
                    return Err(IvmRuntimeError::GraphInputArityMismatch(node));
                };
                self.update_recursive(node, recursive, output_desc, *seed, *step)
            }
            OpType::Persist(persist) => {
                let storage = self.storage.ok_or(IvmRuntimeError::StorageUnavailable)?;
                let input = self.update_unary_input(&graph_node, node)?;
                NodeState::update_persist(persist, output_desc, input, storage)
            }
            _ => Err(IvmRuntimeError::UnsupportedOperator),
        }?;
        self.metrics.records_processed += result.deltas.len();
        self.eval_memo.insert(memo_key, result.clone());
        Ok(result)
    }

    fn memo_key(&self, node: NodeId) -> Result<EvalMemoKey, IvmRuntimeError> {
        Ok(EvalMemoKey {
            scope: self.operator_scope(node)?,
            node,
            tick: self.current_tick,
            sub_tick: self.context.sub_tick,
        })
    }

    fn operator_key(&self, node: NodeId) -> Result<OperatorStateKey, IvmRuntimeError> {
        Ok(OperatorStateKey {
            scope: self.operator_scope(node)?,
            node,
        })
    }

    fn operator_scope(&self, node: NodeId) -> Result<ScopePath, IvmRuntimeError> {
        // Only fragments downstream of FrontierSource are scoped. Base table
        // arrangements stay global and can be reused by unrelated queries.
        if self.depends_on_context(node, &mut HashSet::new())? {
            Ok(self.context.scope.clone())
        } else {
            Ok(ScopePath::root())
        }
    }

    fn depends_on_context(
        &self,
        node: NodeId,
        seen: &mut HashSet<NodeId>,
    ) -> Result<bool, IvmRuntimeError> {
        if !seen.insert(node) {
            return Ok(false);
        }
        let graph_node = self
            .graph
            .node(node)
            .ok_or(IvmRuntimeError::GraphNodeNotFound(node))?;
        if matches!(graph_node.descriptor.operator, OpType::Recursive(_)) {
            // Nested recursive builders are rejected during graph compilation.
            // A retained recursive node reached here is an independent boundary,
            // not part of the current recursive frontier scope.
            return Ok(false);
        }
        if matches!(graph_node.descriptor.operator, OpType::FrontierSource(_)) {
            return Ok(true);
        }
        graph_node
            .descriptor
            .inputs
            .iter()
            .map(|input| self.depends_on_context(*input, seen))
            .try_fold(false, |acc, depends| depends.map(|depends| acc || depends))
    }

    fn frontier_source(
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
        if deltas.descriptor != *output {
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
        let left_on = plan_expr_names(&join.left_key);
        let right_on = plan_expr_names(&join.right_key);
        let left_key = self.arrangement_key(left_input, join.left_descriptor, left_on)?;
        let right_key = self.arrangement_key(right_input, join.right_descriptor, right_on)?;
        let mut left_arrangement = self
            .arrangement_states
            .remove(&left_key)
            .unwrap_or_default();
        // Pull arrangements out while applying so both sides can be mutated
        // without aliasing the arrangement map.
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
            &join.left_descriptor,
            &join.right_descriptor,
            &output_desc,
            &left_key.fields,
            &right_key.fields,
            left_delta,
            right_delta,
            self.arrangement_sub_tick(&left_key),
            self.arrangement_sub_tick(&right_key),
            self.context.arrangement_update_mode,
        )?;
        if left_key == right_key {
            left_arrangement = right_arrangement;
        } else {
            self.arrangement_states.insert(right_key, right_arrangement);
        }
        self.arrangement_states.insert(left_key, left_arrangement);
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
        let operator = self
            .operator_states
            .entry(operator_key)
            .or_insert_with(|| operator_state_for(&OpType::AntiJoin(join.clone())));
        let OperatorState::AntiJoin(join_state) = operator else {
            return Err(IvmRuntimeError::NodeStateOperatorMismatch(node));
        };
        let join_state = join_state.clone();
        let left_on = plan_expr_names(&join.left_key);
        let right_on = plan_expr_names(&join.right_key);
        let left_key = self.arrangement_key(left_input, join.left_descriptor, left_on)?;
        let right_key = self.arrangement_key(right_input, join.right_descriptor, right_on)?;
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
            &join.left_descriptor,
            &join.right_descriptor,
            &output_desc,
            &left_key.fields,
            &right_key.fields,
            left_delta,
            right_delta,
            self.arrangement_sub_tick(&left_key),
            self.arrangement_sub_tick(&right_key),
            self.context.arrangement_update_mode,
        )?;
        if left_key == right_key {
            left_arrangement = right_arrangement;
        } else {
            self.arrangement_states.insert(right_key, right_arrangement);
        }
        self.arrangement_states.insert(left_key, left_arrangement);
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
        input: RecordDeltas,
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
        let arrangement_key =
            self.arrangement_key(*input_node, output_desc, spec.group_fields.to_vec())?;
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
        for delta in input.deltas {
            let group_key =
                encoded_record_key_part(output_desc, delta.raw(), spec.group_field_indices)?;
            touched_groups.entry(group_key).or_default().push(delta);
        }

        let mut output = Vec::new();
        for (group_prefix, group_deltas) in touched_groups {
            let after_records = arrangement.value().records_for_key(&group_prefix);
            let after = arg_by_winner_from_records(
                output_desc,
                spec.primary_key_field_indices,
                after_records.clone(),
                spec.direction,
            )?;
            let before = arg_by_winner_before_from_deltas(
                output_desc,
                spec.primary_key_field_indices,
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
        self.arrangement_states.insert(arrangement_key, arrangement);

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
        input: RecordDeltas,
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
        let arrangement_key =
            self.arrangement_key(*input_node, output_desc, top_by.group_fields.clone())?;
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
                &top_by.group_fields,
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
        for delta in input.deltas {
            let group_key =
                encoded_record_key_part(output_desc, delta.raw(), &top_by.group_field_indices)?;
            touched_groups.entry(group_key).or_default().push(delta);
        }

        let mut output = Vec::new();
        for (group_prefix, group_deltas) in touched_groups {
            let after_records = arrangement.value().records_for_key(&group_prefix);
            let after = top_by_window_from_records(output_desc, after_records.clone(), top_by)?;
            let before =
                top_by_window_before_from_deltas(output_desc, after_records, group_deltas, top_by)?;
            output.extend(diff_record_windows(before, after));
        }
        self.arrangement_states.insert(arrangement_key, arrangement);

        Ok(RecordDeltas {
            descriptor: output_desc,
            deltas: output,
        })
    }

    fn arrangement_key(
        &self,
        input: NodeId,
        descriptor: RecordDescriptor,
        fields: Vec<String>,
    ) -> Result<ArrangementKey, IvmRuntimeError> {
        Ok(ArrangementKey {
            scope: self.operator_scope(input)?,
            input,
            fields,
            descriptor,
        })
    }

    fn arrangement_sub_tick(&self, key: &ArrangementKey) -> SubTick {
        SubTick {
            tick: self.current_tick,
            // Root-scope arrangements represent table time, not recursive
            // evaluator time. A recursive step at sub_tick 1 and a sibling
            // non-recursive join must therefore share the same root SubTick.
            sub_tick: if key.scope == ScopePath::root() {
                0
            } else {
                self.context.sub_tick
            },
        }
    }

    fn update_recursive(
        &mut self,
        node: NodeId,
        recursive: &RecursiveOp,
        output_desc: RecordDescriptor,
        seed: NodeId,
        step: NodeId,
    ) -> Result<RecordDeltas, IvmRuntimeError> {
        let storage = self.storage.ok_or(IvmRuntimeError::StorageUnavailable)?;
        let operator_key = self.operator_key(node)?;
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
            if recursive_as_of.value().step_arrangements_hydrated()
                && recursive_as_of.as_of() == Some(Tick(self.current_tick))
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
            let next = recompute_recursive(
                self.graph,
                node,
                recursive,
                output_desc,
                step,
                storage,
                self.binding_snapshots,
                self.current_tick,
                scope.clone(),
            )?;
            recursive_as_of.value_mut().replace_with(next);
            let accumulated = RecordDeltas {
                descriptor: output_desc,
                deltas: recursive_as_of.value().accumulated_deltas(),
            };
            let mut runtime = graph_runtime_view(
                self.graph,
                self.table_deltas,
                self.binding_deltas,
                self.binding_snapshots,
                self.current_tick,
                self.operator_states,
                self.arrangement_states,
                self.eval_memo,
                storage,
                scope,
                self.metrics,
            );
            hydrate_recursive_arrangements(&mut runtime, recursive, step, accumulated.clone())?;
            recursive_as_of
                .value_mut()
                .mark_step_arrangements_hydrated();
            recursive_as_of.mark_forward_as_of(Tick(self.current_tick))?;
            self.operator_states.insert(operator_key, operator);
            return Ok(accumulated);
        }
        let deltas = recursive_delta(
            recursive_as_of.value_mut(),
            graph_runtime_view(
                self.graph,
                self.table_deltas,
                self.binding_deltas,
                self.binding_snapshots,
                self.current_tick,
                self.operator_states,
                self.arrangement_states,
                self.eval_memo,
                storage,
                self.context.scope.child(node),
                self.metrics,
            ),
            node,
            recursive,
            output_desc,
            seed,
            step,
        )?;
        recursive_as_of.mark_forward_as_of(Tick(self.current_tick))?;
        self.operator_states.insert(operator_key, operator);
        Ok(RecordDeltas {
            descriptor: output_desc,
            deltas,
        })
    }

    fn update_unary_input(
        &mut self,
        graph_node: &crate::ivm::GraphNode,
        node: NodeId,
    ) -> Result<RecordDeltas, IvmRuntimeError> {
        let input = *graph_node
            .descriptor
            .inputs
            .first()
            .ok_or(IvmRuntimeError::GraphInputMissing(node))?;
        self.update_node(input)
    }
}

fn project_descriptor(
    input: &RecordDescriptor,
    fields: &[crate::ivm::ProjectField],
) -> Result<RecordDescriptor, IvmRuntimeError> {
    fields
        .iter()
        .map(|project_field| {
            let value_type = match &project_field.expression {
                ProjectExpr::Field(source) => {
                    let source_idx = resolve_field_ref(input, source)?;
                    input
                        .fields()
                        .get(source_idx)
                        .ok_or(IvmRuntimeError::GraphFieldIndexOutOfBounds(source_idx))?
                        .value_type
                        .clone()
                }
                ProjectExpr::Literal(value) => value
                    .value_type()
                    .ok_or(IvmRuntimeError::UnsupportedOperator)?,
                ProjectExpr::Null(value_type) => value_type.clone(),
                ProjectExpr::Nullable(source) => {
                    let source_idx = resolve_field_ref(input, source)?;
                    let inner = input
                        .fields()
                        .get(source_idx)
                        .ok_or(IvmRuntimeError::GraphFieldIndexOutOfBounds(source_idx))?
                        .value_type
                        .clone();
                    ValueType::Nullable(Box::new(inner))
                }
            };
            Ok((project_field.output_name.clone(), value_type))
        })
        .collect::<Result<Vec<_>, IvmRuntimeError>>()
        .map(RecordDescriptor::new)
}

fn project_field_expr(
    input: &RecordDescriptor,
    field: &ProjectField,
) -> Result<PlanExpr, IvmRuntimeError> {
    match &field.expression {
        ProjectExpr::Field(source) => Ok(PlanExpr::field(field_ref_name(input, source)?)),
        ProjectExpr::Literal(value) => Ok(PlanExpr::literal(value.clone())),
        ProjectExpr::Null(value_type) => Ok(PlanExpr::null(value_type.clone())),
        ProjectExpr::Nullable(source) => Ok(PlanExpr::nullable(field_ref_name(input, source)?)),
    }
}

fn project_record(
    expressions: &[ProjectionExpr],
    mapping: &[(usize, usize)],
    output_desc: RecordDescriptor,
    input_desc: &RecordDescriptor,
    input_record: &[u8],
) -> Result<Vec<u8>, IvmRuntimeError> {
    if projection_uses_raw_copy(expressions, mapping, output_desc) {
        return Ok(output_desc.project_record_raw(
            std::slice::from_ref(input_desc),
            &[input_record],
            mapping,
        )?);
    }

    let input = BorrowedRecord::new(input_record, input_desc);
    let mut values = Vec::with_capacity(expressions.len());
    for expr in expressions {
        values.push(match &expr.expression {
            PlanExpr::Field(field) => input.get(field)?.clone(),
            PlanExpr::Literal(value) => value.to_value(),
            PlanExpr::Null(_) => Value::Nullable(None),
            PlanExpr::Nullable(field) => Value::Nullable(Some(Box::new(input.get(field)?.clone()))),
        });
    }
    Ok(output_desc.create(&values)?)
}

fn projection_uses_raw_copy(
    expressions: &[ProjectionExpr],
    mapping: &[(usize, usize)],
    output_desc: RecordDescriptor,
) -> bool {
    if expressions.is_empty() {
        // Legacy/validation-only path: normal lowering always fills expressions.
        return mapping.len() == output_desc.fields().len();
    }
    expressions.len() == output_desc.fields().len()
        && mapping.len() == output_desc.fields().len()
        && expressions
            .iter()
            .all(|expr| matches!(expr.expression, PlanExpr::Field(_)))
}

fn resolve_field_ref(
    descriptor: &RecordDescriptor,
    field: &FieldRef,
) -> Result<usize, IvmRuntimeError> {
    match field {
        FieldRef::Name(name) => descriptor
            .field_index(name)
            .ok_or_else(|| IvmRuntimeError::GraphFieldNotFound(name.clone())),
        FieldRef::Resolved(index) => {
            if *index < descriptor.fields().len() {
                Ok(*index)
            } else {
                Err(IvmRuntimeError::GraphFieldIndexOutOfBounds(*index))
            }
        }
    }
}

fn field_ref_name(
    descriptor: &RecordDescriptor,
    field: &FieldRef,
) -> Result<String, IvmRuntimeError> {
    match field {
        FieldRef::Name(name) => Ok(name.clone()),
        FieldRef::Resolved(index) => field_name_at(descriptor, *index),
    }
}

fn field_name_at(descriptor: &RecordDescriptor, index: usize) -> Result<String, IvmRuntimeError> {
    let field = descriptor
        .fields()
        .get(index)
        .ok_or(IvmRuntimeError::GraphFieldIndexOutOfBounds(index))?;
    field
        .name
        .clone()
        .ok_or_else(|| IvmRuntimeError::GraphFieldNotFound(format!("#{index}")))
}

fn unwrap_nullable_descriptor(
    input: &RecordDescriptor,
    field_idx: usize,
) -> Result<RecordDescriptor, IvmRuntimeError> {
    input
        .fields()
        .iter()
        .enumerate()
        .map(|(idx, field)| {
            let value_type = if idx == field_idx {
                let ValueType::Nullable(inner) = &field.value_type else {
                    return Err(IvmRuntimeError::UnsupportedOperator);
                };
                (**inner).clone()
            } else {
                field.value_type.clone()
            };
            Ok((field.name.clone().unwrap_or_default(), value_type))
        })
        .collect::<Result<Vec<_>, IvmRuntimeError>>()
        .map(RecordDescriptor::new)
}

fn index_record_descriptor() -> RecordDescriptor {
    RecordDescriptor::new([("key", ValueType::Bytes), ("value", ValueType::Bytes)])
}

fn apply_index_by(
    index_by: &IndexByOp,
    input_descriptor: &RecordDescriptor,
    input_deltas: &[RecordDelta],
) -> Result<Vec<RecordDelta>, IvmRuntimeError> {
    let mut deltas = Vec::new();
    for delta in input_deltas {
        let keys = index_keys(index_by, input_descriptor, delta.raw())?;
        let value = if index_by.store_value {
            primary_key_value_bytes(input_descriptor, delta.raw(), &index_by.value_fields)?
        } else {
            Vec::new()
        };
        for key in keys {
            deltas.push(RecordDelta {
                record: index_record_descriptor()
                    .create(&[Value::Bytes(key), Value::Bytes(value.clone())])?,
                weight: delta.weight,
            });
        }
    }
    Ok(deltas)
}

fn index_keys(
    index_by: &IndexByOp,
    input_descriptor: &RecordDescriptor,
    record: &[u8],
) -> Result<Vec<Vec<u8>>, IvmRuntimeError> {
    let mut keys = vec![Vec::new()];
    let mut seen = HashSet::new();

    for field_idx in &index_by.key_fields {
        let parts = record_field_key_parts(input_descriptor, record, *field_idx)?;
        if parts.is_empty() {
            return Ok(Vec::new());
        }

        let mut next_keys = Vec::with_capacity(keys.len() * parts.len());
        for key in &keys {
            for part in &parts {
                let mut next = key.clone();
                next.extend(part);
                if seen.insert(next.clone()) {
                    next_keys.push(next);
                }
            }
        }
        keys = next_keys;
        seen.clear();
    }
    if index_by.append_value_to_key {
        let value = primary_key_value_bytes(input_descriptor, record, &index_by.value_fields)?;
        // Non-unique indices append the primary key so equal index values remain
        // distinct and ordered for range scans.
        for key in &mut keys {
            key.push(0xff);
            key.extend(&value);
        }
    }
    Ok(keys)
}

pub(crate) fn durable_index_key_prefix(table: &str, index: &str) -> Vec<u8> {
    let mut prefix = Vec::new();
    // NUL separators keep table/index names prefix-decodable without escaping.
    prefix.extend(table.as_bytes());
    prefix.push(0);
    prefix.extend(index.as_bytes());
    prefix.push(0);
    prefix
}

fn primary_key_value_bytes(
    descriptor: &RecordDescriptor,
    record: &[u8],
    primary_key_field_indices: &[usize],
) -> Result<Vec<u8>, IvmRuntimeError> {
    let mut bytes = Vec::new();
    for primary_key_field_idx in primary_key_field_indices {
        encode_record_field_key_part(&mut bytes, descriptor, record, *primary_key_field_idx)?;
    }
    Ok(bytes)
}

fn encode_record_field_key_part(
    key: &mut Vec<u8>,
    descriptor: &RecordDescriptor,
    record: &[u8],
    field_idx: usize,
) -> Result<(), IvmRuntimeError> {
    let field = descriptor
        .fields()
        .get(field_idx)
        .ok_or(IvmRuntimeError::GraphFieldIndexOutOfBounds(field_idx))?;
    match &field.value_type {
        ValueType::Enum(_) => {
            let value = descriptor.bind(record).get_enum(field_idx)?;
            encode_key_part(key, &Value::U8(value))
        }
        ValueType::Nullable(inner) if matches!(inner.as_ref(), ValueType::Enum(_)) => {
            match descriptor.bind(record).get_nullable_enum(field_idx)? {
                Some(value) => {
                    encode_key_part(key, &Value::Nullable(Some(Box::new(Value::U8(value)))))
                }
                None => encode_key_part(key, &Value::Nullable(None)),
            }
        }
        _ => {
            let field_name = field
                .name
                .as_deref()
                .ok_or_else(|| IvmRuntimeError::GraphFieldNotFound("<unnamed>".to_owned()))?;
            let value = descriptor.get(record, field_name)?;
            encode_key_part(key, &value)
        }
    }
}

fn record_field_key_parts(
    descriptor: &RecordDescriptor,
    record: &[u8],
    field_idx: usize,
) -> Result<Vec<Vec<u8>>, IvmRuntimeError> {
    let field = descriptor
        .fields()
        .get(field_idx)
        .ok_or(IvmRuntimeError::GraphFieldIndexOutOfBounds(field_idx))?;
    match &field.value_type {
        ValueType::Array(_) => {
            let field_name = field
                .name
                .as_deref()
                .ok_or_else(|| IvmRuntimeError::GraphFieldNotFound("<unnamed>".to_owned()))?;
            let Value::Array(values) = descriptor.get(record, field_name)? else {
                return Err(IvmRuntimeError::GraphFieldNotFound(field_name.to_owned()));
            };
            values
                .into_iter()
                .map(|value| {
                    let mut key = Vec::new();
                    encode_key_part(&mut key, &value)?;
                    Ok(key)
                })
                .collect()
        }
        ValueType::Nullable(inner) if matches!(inner.as_ref(), ValueType::Array(_)) => {
            let field_name = field
                .name
                .as_deref()
                .ok_or_else(|| IvmRuntimeError::GraphFieldNotFound("<unnamed>".to_owned()))?;
            match descriptor.get(record, field_name)? {
                Value::Nullable(None) => {
                    let mut key = Vec::new();
                    encode_key_part(&mut key, &Value::Nullable(None))?;
                    Ok(vec![key])
                }
                Value::Nullable(Some(value)) => match *value {
                    Value::Array(values) => values
                        .into_iter()
                        .map(|value| {
                            let mut key = Vec::new();
                            encode_key_part(&mut key, &Value::Nullable(Some(Box::new(value))))?;
                            Ok(key)
                        })
                        .collect(),
                    value => {
                        let mut key = Vec::new();
                        encode_key_part(&mut key, &Value::Nullable(Some(Box::new(value))))?;
                        Ok(vec![key])
                    }
                },
                value => {
                    let mut key = Vec::new();
                    encode_key_part(&mut key, &value)?;
                    Ok(vec![key])
                }
            }
        }
        _ => {
            let mut key = Vec::new();
            encode_record_field_key_part(&mut key, descriptor, record, field_idx)?;
            Ok(vec![key])
        }
    }
}

fn compare_record_field(
    record: BorrowedRecord<'_>,
    field: &str,
    value: &LiteralValue,
    predicate: impl FnOnce(std::cmp::Ordering) -> bool,
) -> Result<bool, IvmRuntimeError> {
    if let LiteralValue::Enum(expected) = value {
        let field_idx = record
            .descriptor()
            .field_index(field)
            .ok_or_else(|| IvmRuntimeError::GraphFieldNotFound(field.to_owned()))?;
        let actual = record.get_enum(field_idx)?;
        return Ok(actual.partial_cmp(expected).is_some_and(predicate));
    }
    let value = value.to_value();
    let actual = record.get(field)?;
    Ok(compare_values_sql(&actual, &value).is_some_and(predicate))
}

fn compare_record_fields(
    record: BorrowedRecord<'_>,
    field: &str,
    value_field: &str,
    predicate: impl FnOnce(std::cmp::Ordering) -> bool,
) -> Result<bool, IvmRuntimeError> {
    let left = record.get(field)?;
    let right = record.get(value_field)?;
    Ok(compare_values_sql(&left, &right).is_some_and(predicate))
}

fn contains_record_field(
    record: BorrowedRecord<'_>,
    field: &str,
    value: &LiteralValue,
) -> Result<bool, IvmRuntimeError> {
    let needle = value.to_value();
    let haystack = record.get(field)?;
    Ok(value_contains_sql(&haystack, &needle))
}

fn contains_record_field_value(
    record: BorrowedRecord<'_>,
    field: &str,
    needle_field: &str,
) -> Result<bool, IvmRuntimeError> {
    let haystack = record.get(field)?;
    let needle = record.get(needle_field)?;
    Ok(value_contains_sql(&haystack, &needle))
}

fn value_contains_sql(left: &Value, right: &Value) -> bool {
    match (left, right) {
        (Value::Nullable(None), _) | (_, Value::Nullable(None)) => false,
        (Value::Nullable(Some(left)), right) => value_contains_sql(left, right),
        (left, Value::Nullable(Some(right))) => value_contains_sql(left, right),
        (Value::String(left), Value::String(right)) => left.contains(right),
        (Value::Array(values), right) => values.iter().any(|value| value == right),
        _ => false,
    }
}

fn compare_values_sql(left: &Value, right: &Value) -> Option<std::cmp::Ordering> {
    match (left, right) {
        (Value::Nullable(None), _) | (_, Value::Nullable(None)) => None,
        (Value::Nullable(Some(left)), right) => compare_values_sql(left, right),
        (left, Value::Nullable(Some(right))) => compare_values_sql(left, right),
        _ => compare_values(left, right),
    }
}

fn is_sql_null_value(value: &Value) -> bool {
    match value {
        Value::Nullable(None) => true,
        Value::Nullable(Some(value)) => is_sql_null_value(value),
        _ => false,
    }
}

fn compare_values(left: &Value, right: &Value) -> Option<std::cmp::Ordering> {
    match (left, right) {
        (Value::U8(left), Value::U8(right)) => left.partial_cmp(right),
        (Value::U16(left), Value::U16(right)) => left.partial_cmp(right),
        (Value::U32(left), Value::U32(right)) => left.partial_cmp(right),
        (Value::U64(left), Value::U64(right)) => left.partial_cmp(right),
        (Value::F64(left), Value::F64(right)) => left.partial_cmp(right),
        (Value::Bool(left), Value::Bool(right)) => left.partial_cmp(right),
        (Value::Enum(left), Value::Enum(right)) => left.partial_cmp(right),
        (Value::String(left), Value::String(right)) => left.partial_cmp(right),
        (Value::Bytes(left), Value::Bytes(right)) => left.partial_cmp(right),
        (Value::Uuid(left), Value::Uuid(right)) => left.as_bytes().partial_cmp(right.as_bytes()),
        (Value::Tuple(left), Value::Tuple(right)) => left
            .iter()
            .zip(right)
            .map(|(left, right)| compare_values(left, right))
            .find(|ordering| !matches!(ordering, Some(std::cmp::Ordering::Equal)))
            .unwrap_or_else(|| left.len().partial_cmp(&right.len())),
        (Value::Array(left), Value::Array(right)) => left
            .iter()
            .zip(right)
            .map(|(left, right)| compare_values(left, right))
            .find(|ordering| !matches!(ordering, Some(std::cmp::Ordering::Equal)))
            .unwrap_or_else(|| left.len().partial_cmp(&right.len())),
        _ => None,
    }
}

fn join_descriptor(left: &RecordDescriptor, right: &RecordDescriptor) -> RecordDescriptor {
    let fields = left
        .fields()
        .iter()
        .filter_map(|field| {
            Some((
                format!("left.{}", field.name.as_ref()?),
                field.value_type.clone(),
            ))
        })
        .chain(right.fields().iter().filter_map(|field| {
            Some((
                format!("right.{}", field.name.as_ref()?),
                field.value_type.clone(),
            ))
        }))
        .collect::<Vec<_>>();

    RecordDescriptor::new(fields)
}

pub(crate) fn encode_key_part(key: &mut Vec<u8>, value: &Value) -> Result<(), IvmRuntimeError> {
    // Type tags make composite keys unambiguous. Payload bytes are chosen to
    // preserve natural ordering in RocksDB's lexicographic iterator order.
    match value {
        Value::U8(value) => {
            key.push(0);
            key.push(*value);
        }
        Value::U16(value) => {
            key.push(1);
            key.extend(value.to_be_bytes());
        }
        Value::U32(value) => {
            key.push(2);
            key.extend(value.to_be_bytes());
        }
        Value::U64(value) => {
            key.push(3);
            key.extend(value.to_be_bytes());
        }
        Value::F64(value) => {
            if value.is_nan() {
                return Err(IvmRuntimeError::RecordEncoding(
                    records::Error::InvalidF64NaN,
                ));
            }
            key.push(4);
            key.extend(order_preserving_f64_bits(*value).to_be_bytes());
        }
        Value::Bool(value) => {
            key.push(5);
            key.push(u8::from(*value));
        }
        Value::String(value) => {
            key.push(6);
            encode_ordered_bytes(key, value.as_bytes());
        }
        Value::Bytes(value) => {
            key.push(7);
            encode_ordered_bytes(key, value);
        }
        Value::Uuid(value) => {
            key.push(10);
            key.extend_from_slice(value.as_bytes());
        }
        Value::Tuple(values) => {
            key.push(11);
            for value in values {
                encode_key_part(key, value)?;
            }
        }
        Value::Enum(value) => {
            key.push(0);
            key.push(*value);
        }
        Value::Nullable(None) => {
            key.push(8);
        }
        Value::Nullable(Some(value)) => {
            key.push(9);
            encode_key_part(key, value)?;
        }
        Value::Array(_) => return Err(IvmRuntimeError::UnsupportedJoinKey),
    }
    Ok(())
}

fn order_preserving_f64_bits(value: f64) -> u64 {
    let bits = value.to_bits();
    // Flip positive signs and invert negatives; the resulting unsigned integer
    // sorts like IEEE numeric order for non-NaN values.
    if bits & (1 << 63) == 0 {
        bits ^ (1 << 63)
    } else {
        !bits
    }
}

fn encode_ordered_bytes(key: &mut Vec<u8>, value: &[u8]) {
    // 0x00 terminates the byte string; embedded NULs are escaped as 00 ff.
    for byte in value {
        if *byte == 0 {
            key.extend([0, 0xff]);
        } else {
            key.push(*byte);
        }
    }
    key.extend([0, 0]);
}

fn project_binding_source_deltas(
    input: &RecordDeltas,
    output_desc: &RecordDescriptor,
) -> Result<RecordDeltas, IvmRuntimeError> {
    if input.descriptor == *output_desc {
        return Ok(input.clone());
    }
    let mapping = output_desc
        .fields()
        .iter()
        .map(|field| {
            let name = field
                .name
                .as_ref()
                .ok_or_else(|| IvmRuntimeError::GraphFieldNotFound("<unnamed>".to_owned()))?;
            input
                .descriptor
                .field_index(name)
                .map(|index| (0, index))
                .ok_or_else(|| IvmRuntimeError::GraphFieldNotFound(name.clone()))
        })
        .collect::<Result<Vec<_>, _>>()?;
    let deltas = input
        .deltas
        .iter()
        .map(|delta| {
            Ok(RecordDelta {
                record: output_desc.project_record_raw(
                    std::slice::from_ref(&input.descriptor),
                    &[delta.raw()],
                    &mapping,
                )?,
                weight: delta.weight,
            })
        })
        .collect::<Result<Vec<_>, IvmRuntimeError>>()?;
    Ok(RecordDeltas {
        descriptor: *output_desc,
        deltas,
    })
}

fn consolidate_deltas(deltas: Vec<RecordDelta>) -> Vec<RecordDelta> {
    let mut consolidated = HashMap::<Vec<u8>, i64>::new();
    for delta in deltas {
        *consolidated.entry(delta.record).or_default() += delta.weight;
    }
    consolidated
        .into_iter()
        .filter_map(|(record, weight)| (weight != 0).then_some(RecordDelta { record, weight }))
        .collect()
}

type SourceRecord = (Vec<u8>, Vec<u8>);
type RankedRecord = (Vec<TopBySortPart>, Vec<u8>);

#[derive(Clone, Debug, PartialEq, Eq)]
struct TopBySortPart {
    key: Vec<u8>,
    direction: TopByDirection,
}

impl Ord for TopBySortPart {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        match self.direction {
            TopByDirection::Asc => self.key.cmp(&other.key),
            TopByDirection::Desc => other.key.cmp(&self.key),
        }
    }
}

impl PartialOrd for TopBySortPart {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

#[derive(Clone, Copy)]
enum ArgByDirection {
    Min,
    Max,
}

struct ArgBySpec<'a> {
    group_fields: &'a [String],
    group_field_indices: &'a [usize],
    primary_key_field_indices: &'a [usize],
    direction: ArgByDirection,
}

fn arg_by_winner_from_records(
    descriptor: RecordDescriptor,
    primary_key_field_indices: &[usize],
    records: Vec<(Vec<u8>, i64)>,
    direction: ArgByDirection,
) -> Result<Option<SourceRecord>, IvmRuntimeError> {
    let mut winner = None;
    for (record, weight) in records {
        if weight <= 0 {
            continue;
        }
        let key = encoded_record_key_part(descriptor, &record, primary_key_field_indices)?;
        let replaces =
            winner
                .as_ref()
                .is_none_or(|(winner_key, _): &SourceRecord| match direction {
                    ArgByDirection::Min => key < *winner_key,
                    ArgByDirection::Max => key > *winner_key,
                });
        if replaces {
            winner = Some((key, record));
        }
    }
    Ok(winner)
}

fn arg_by_winner_before_from_deltas(
    descriptor: RecordDescriptor,
    primary_key_field_indices: &[usize],
    after_records: Vec<(Vec<u8>, i64)>,
    deltas: Vec<RecordDelta>,
    direction: ArgByDirection,
) -> Result<Option<SourceRecord>, IvmRuntimeError> {
    let mut records = BTreeMap::<Vec<u8>, (Vec<u8>, i64)>::new();
    for (record, weight) in after_records {
        let key = encoded_record_key_part(descriptor, &record, primary_key_field_indices)?;
        records.insert(key, (record, weight));
    }
    for delta in deltas {
        let key = encoded_record_key_part(descriptor, delta.raw(), primary_key_field_indices)?;
        let entry = records
            .entry(key)
            .or_insert_with(|| (delta.record.clone(), 0));
        entry.1 -= delta.weight;
    }
    let mut positive = records
        .into_iter()
        .filter_map(|(key, (record, weight))| (weight > 0).then_some((key, record)));
    Ok(match direction {
        ArgByDirection::Min => positive.next(),
        ArgByDirection::Max => positive.next_back(),
    })
}

fn top_by_window_from_records(
    descriptor: RecordDescriptor,
    records: Vec<(Vec<u8>, i64)>,
    top_by: &TopByOp,
) -> Result<Vec<RankedRecord>, IvmRuntimeError> {
    let mut ranked = Vec::new();
    for (record, weight) in records {
        if weight > 0 {
            ranked.push((top_by_sort_key(descriptor, &record, top_by)?, record));
        }
    }
    ranked.sort_by(|left, right| left.0.cmp(&right.0));
    Ok(ranked
        .into_iter()
        .skip(top_by.offset)
        .take(top_by.limit)
        .collect())
}

fn top_by_window_before_from_deltas(
    descriptor: RecordDescriptor,
    after_records: Vec<(Vec<u8>, i64)>,
    deltas: Vec<RecordDelta>,
    top_by: &TopByOp,
) -> Result<Vec<RankedRecord>, IvmRuntimeError> {
    let mut records = BTreeMap::<Vec<u8>, (Vec<u8>, i64)>::new();
    for (record, weight) in after_records {
        let key = encoded_record_key_part(descriptor, &record, &top_by.sort_field_indices)?;
        records.insert(key, (record, weight));
    }
    for delta in deltas {
        let key = encoded_record_key_part(descriptor, delta.raw(), &top_by.sort_field_indices)?;
        let entry = records
            .entry(key)
            .or_insert_with(|| (delta.record.clone(), 0));
        entry.1 -= delta.weight;
    }
    top_by_window_from_records(
        descriptor,
        records
            .into_iter()
            .map(|(_, (record, weight))| (record, weight))
            .collect(),
        top_by,
    )
}

fn top_by_sort_key(
    descriptor: RecordDescriptor,
    record: &[u8],
    top_by: &TopByOp,
) -> Result<Vec<TopBySortPart>, IvmRuntimeError> {
    top_by
        .sort_field_indices
        .iter()
        .zip(&top_by.sort_directions)
        .map(|(field_idx, direction)| {
            Ok(TopBySortPart {
                key: encoded_record_key_part(descriptor, record, &[*field_idx])?,
                direction: *direction,
            })
        })
        .collect()
}

fn diff_record_windows(before: Vec<RankedRecord>, after: Vec<RankedRecord>) -> Vec<RecordDelta> {
    let mut weights = BTreeMap::<Vec<u8>, i64>::new();
    for (_, record) in before {
        *weights.entry(record).or_default() -= 1;
    }
    for (_, record) in after {
        *weights.entry(record).or_default() += 1;
    }
    let mut retractions = Vec::new();
    let mut insertions = Vec::new();
    for (record, weight) in weights {
        match weight.cmp(&0) {
            std::cmp::Ordering::Less => retractions.push(RecordDelta { record, weight }),
            std::cmp::Ordering::Greater => insertions.push(RecordDelta { record, weight }),
            std::cmp::Ordering::Equal => {}
        }
    }
    retractions.extend(insertions);
    retractions
}

pub(super) fn encoded_record_key_part(
    descriptor: RecordDescriptor,
    record: &[u8],
    field_indices: &[usize],
) -> Result<Vec<u8>, IvmRuntimeError> {
    let mut key = Vec::new();
    for field_idx in field_indices {
        let value = descriptor.get_idx(record, *field_idx)?;
        encode_runtime_primary_key_part(&mut key, &value);
    }
    Ok(key)
}

fn encode_runtime_primary_key_part(key: &mut Vec<u8>, value: &Value) {
    match value {
        Value::U8(value) => {
            key.push(0);
            key.push(*value);
        }
        Value::U16(value) => {
            key.push(1);
            key.extend(value.to_be_bytes());
        }
        Value::U32(value) => {
            key.push(2);
            key.extend(value.to_be_bytes());
        }
        Value::U64(value) => {
            key.push(3);
            key.extend(value.to_be_bytes());
        }
        Value::F64(value) => {
            key.push(4);
            key.extend(ordered_f64_key(*value).to_be_bytes());
        }
        Value::Bool(value) => {
            key.push(5);
            key.push(u8::from(*value));
        }
        Value::String(value) => {
            key.push(6);
            encode_runtime_ordered_bytes(key, value.as_bytes());
        }
        Value::Enum(value) => {
            key.push(0);
            key.push(*value);
        }
        Value::Bytes(value) => {
            key.push(7);
            encode_runtime_ordered_bytes(key, value);
        }
        Value::Uuid(value) => {
            key.push(10);
            key.extend_from_slice(value.as_bytes());
        }
        Value::Tuple(values) => {
            key.push(11);
            for value in values {
                encode_runtime_primary_key_part(key, value);
            }
        }
        Value::Nullable(None) => {
            key.push(12);
            key.push(0);
        }
        Value::Nullable(Some(value)) => {
            key.push(12);
            key.push(1);
            encode_runtime_primary_key_part(key, value);
        }
        Value::Array(_) => {
            unreachable!("unsupported primary-key value type was validated before encoding")
        }
    }
}

fn ordered_f64_key(value: f64) -> u64 {
    let bits = value.to_bits();
    if bits & (1 << 63) == 0 {
        bits ^ (1 << 63)
    } else {
        !bits
    }
}

fn encode_runtime_ordered_bytes(key: &mut Vec<u8>, value: &[u8]) {
    for byte in value {
        if *byte == 0 {
            key.extend([0, 0xff]);
        } else {
            key.push(*byte);
        }
    }
    key.push(0);
    key.push(0);
}

#[derive(Debug, Error)]
pub enum IvmRuntimeError {
    #[error("graph field not found: {0}")]
    GraphFieldNotFound(String),
    #[error("graph field index out of bounds: {0}")]
    GraphFieldIndexOutOfBounds(usize),
    #[error("graph node has unexpected input arity: {0:?}")]
    GraphInputArityMismatch(NodeId),
    #[error("graph node is missing input: {0:?}")]
    GraphInputMissing(NodeId),
    #[error("graph node not found: {0:?}")]
    GraphNodeNotFound(NodeId),
    #[error("graph output descriptors do not match")]
    GraphOutputMismatch,
    #[error("index not found: {0}")]
    IndexNotFound(String),
    #[error("join key arity mismatch: left={left}, right={right}")]
    JoinKeyArityMismatch { left: usize, right: usize },
    #[error("shape key field not found: {0}")]
    ShapeKeyFieldNotFound(String),
    #[error("table has no primary key: {0}")]
    MissingPrimaryKey(String),
    #[error("runtime node state missing: {0:?}")]
    NodeStateMissing(NodeId),
    #[error("runtime node state operator mismatch: {0:?}")]
    NodeStateOperatorMismatch(NodeId),
    #[error("runtime state advanced out of order: current={current}, next={next}")]
    OutOfOrderRuntimeState { current: String, next: String },
    #[error("persist node expected key/value bytes")]
    PersistRecordMismatch,
    #[error("binding sources can only be evaluated through prepared shapes")]
    BindingSourceRequiresPrepare,
    #[error("binding source not found: {0}")]
    BindingSourceNotFound(String),
    #[error(transparent)]
    RecordEncoding(#[from] records::Error),
    #[error("recursive node {node:?} exceeded iteration limit {max_iters}")]
    RecursiveIterationLimit { node: NodeId, max_iters: usize },
    #[error(transparent)]
    Storage(#[from] crate::storage::Error),
    #[error("storage unavailable for durable node")]
    StorageUnavailable,
    #[error("subscription shape not found: {0:?}")]
    PreparedShapeNotFound(PreparedShapeId),
    #[error("runtime state is stale: expected={expected}, actual={actual:?}")]
    StaleRuntimeState {
        expected: String,
        actual: Option<String>,
    },
    #[error("table not found: {0}")]
    TableNotFound(String),
    #[error("unique index violation: {index}")]
    UniqueIndexViolation { index: String },
    #[error("unsupported join key")]
    UnsupportedJoinKey,
    #[error("non-monotone recursive delta reached positive-only incremental recursion")]
    UnsupportedNonMonotoneRecursion,
    #[error("nested recursive graphs are not supported in v0")]
    UnsupportedNestedRecursion,
    #[error("unsupported arg_max_by graph: {0}")]
    UnsupportedArgMaxBy(String),
    #[error("unsupported operator")]
    UnsupportedOperator,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ivm::{AggregateFunction, AggregateOp, JoinOpKind};
    use crate::schema::{
        ColumnSchema, ColumnType, DatabaseSchema, IndexSchema, IntegerKeyType, PrimaryKey,
    };
    use crate::storage::{RecordStore, RocksDbStorage};

    fn albums_schema() -> DatabaseSchema {
        DatabaseSchema::new([TableSchema::new(
            "albums",
            [
                ColumnSchema::new("id", ColumnType::U64),
                ColumnSchema::new("title", ColumnType::String),
            ],
        )
        .with_primary_key(PrimaryKey::new("id", IntegerKeyType::U64))])
    }

    fn indexed_albums_schema() -> DatabaseSchema {
        DatabaseSchema::new([TableSchema::new(
            "albums",
            [
                ColumnSchema::new("id", ColumnType::U64),
                ColumnSchema::new("title", ColumnType::String),
            ],
        )
        .with_primary_key(PrimaryKey::new("id", IntegerKeyType::U64))
        .with_index(IndexSchema::new("albums_by_title", ["title"]))])
    }

    fn albums_artists_schema() -> DatabaseSchema {
        DatabaseSchema::new([
            TableSchema::new(
                "albums",
                [
                    ColumnSchema::new("id", ColumnType::U64),
                    ColumnSchema::new("artist_id", ColumnType::U64),
                    ColumnSchema::new("title", ColumnType::String),
                ],
            )
            .with_primary_key(PrimaryKey::new("id", IntegerKeyType::U64)),
            TableSchema::new(
                "artists",
                [
                    ColumnSchema::new("id", ColumnType::U64),
                    ColumnSchema::new("name", ColumnType::String),
                ],
            )
            .with_primary_key(PrimaryKey::new("id", IntegerKeyType::U64)),
        ])
    }

    fn edges_schema() -> DatabaseSchema {
        DatabaseSchema::new([TableSchema::new(
            "edges",
            [
                ColumnSchema::new("id", ColumnType::U64),
                ColumnSchema::new("src", ColumnType::U64),
                ColumnSchema::new("dst", ColumnType::U64),
            ],
        )
        .with_primary_key(PrimaryKey::new("id", IntegerKeyType::U64))])
    }

    fn reach_descriptor() -> RecordDescriptor {
        RecordDescriptor::new([
            ("src", ColumnType::U64.value_type()),
            ("dst", ColumnType::U64.value_type()),
        ])
    }

    fn recursive_reach_graph() -> GraphBuilder {
        let seed = GraphBuilder::table("edges").project(["src", "dst"]);
        let edge_pairs = GraphBuilder::table("edges").project(["src", "dst"]);
        let frontier = GraphBuilder::frontier_source("frontier", reach_descriptor());
        let step = GraphBuilder::join(frontier, edge_pairs, ["dst"], ["src"]).project_fields([
            crate::ivm::ProjectField::renamed("left.src", "src"),
            crate::ivm::ProjectField::renamed("right.dst", "dst"),
        ]);
        GraphBuilder::recursive(seed, step, "frontier", 16)
    }

    fn recursive_reach_from_graph(src: u64) -> GraphBuilder {
        let seed = GraphBuilder::table("edges")
            .filter(PredicateExpr::eq("src", Value::U64(src)))
            .project(["src", "dst"]);
        let edge_pairs = GraphBuilder::table("edges").project(["src", "dst"]);
        let frontier = GraphBuilder::frontier_source("frontier", reach_descriptor());
        let step = GraphBuilder::join(frontier, edge_pairs, ["dst"], ["src"]).project_fields([
            crate::ivm::ProjectField::renamed("left.src", "src"),
            crate::ivm::ProjectField::renamed("right.dst", "dst"),
        ]);
        GraphBuilder::recursive(seed, step, "frontier", 16)
    }

    fn recursive_reach_from_with_union_step_graph(src: u64) -> GraphBuilder {
        let seed = GraphBuilder::table("edges")
            .filter(PredicateExpr::eq("src", Value::U64(src)))
            .project(["src", "dst"]);
        let edge_pairs = GraphBuilder::table("edges").project(["src", "dst"]);
        let frontier = GraphBuilder::frontier_source("frontier", reach_descriptor());
        let expanded = GraphBuilder::join(frontier.clone(), edge_pairs, ["dst"], ["src"])
            .project_fields([
                crate::ivm::ProjectField::renamed("left.src", "src"),
                crate::ivm::ProjectField::renamed("right.dst", "dst"),
            ]);
        let step = GraphBuilder::union([frontier, expanded]);
        GraphBuilder::recursive(seed, step, "frontier", 16)
    }

    fn assert_auto_family_matches_direct_with_prepared_count(
        schema: DatabaseSchema,
        families: &[GraphBuilder],
        table_deltas: Vec<TableDelta>,
        storage_familied: &impl OrderedKvStorage,
        storage_direct: &impl OrderedKvStorage,
        expected_prepared_shapes: usize,
    ) {
        let mut familied = IvmRuntime::new(schema.clone()).unwrap();
        let mut direct = IvmRuntime::new(schema).unwrap();
        direct.set_auto_direct_family_enabled(false);

        let familied_subscriptions = families
            .iter()
            .cloned()
            .map(|graph| familied.subscribe(graph, storage_familied).unwrap())
            .collect::<Vec<_>>();
        let direct_subscriptions = families
            .iter()
            .cloned()
            .map(|graph| direct.subscribe(graph, storage_direct).unwrap())
            .collect::<Vec<_>>();

        assert_eq!(familied.prepared_shapes.len(), expected_prepared_shapes);
        for (familied_subscription, direct_subscription) in familied_subscriptions
            .iter()
            .zip(direct_subscriptions.iter())
        {
            assert_eq!(
                familied_subscription.recv().unwrap(),
                direct_subscription.recv().unwrap()
            );
        }

        familied
            .tick(table_deltas.clone(), storage_familied)
            .expect("familied tick");
        direct
            .tick(table_deltas, storage_direct)
            .expect("direct tick");
        for (familied_subscription, direct_subscription) in familied_subscriptions
            .iter()
            .zip(direct_subscriptions.iter())
        {
            match (
                familied_subscription.try_recv(),
                direct_subscription.try_recv(),
            ) {
                (Ok(familied), Ok(direct)) => assert_eq!(familied, direct),
                (Err(TryRecvError::Empty), Err(TryRecvError::Empty)) => {}
                (familied, direct) => {
                    panic!("familied/direct notification mismatch: {familied:?} != {direct:?}");
                }
            }
        }
    }

    fn assert_auto_family_matches_direct(
        schema: DatabaseSchema,
        families: &[GraphBuilder],
        table_deltas: Vec<TableDelta>,
        storage_familied: &impl OrderedKvStorage,
        storage_direct: &impl OrderedKvStorage,
    ) {
        assert_auto_family_matches_direct_with_prepared_count(
            schema,
            families,
            table_deltas,
            storage_familied,
            storage_direct,
            1,
        );
    }

    #[test]
    fn direct_literal_subscriptions_share_auto_family_and_keep_direct_output() {
        let schema = albums_schema();
        let mut runtime = IvmRuntime::new(schema.clone()).unwrap();
        let storage = crate::storage::MemoryStorage::new(&["albums"]);
        let first = runtime
            .subscribe(
                GraphBuilder::table("albums")
                    .filter(PredicateExpr::eq("id", Value::U64(1)))
                    .project(["title"]),
                &storage,
            )
            .unwrap();
        let second = runtime
            .subscribe(
                GraphBuilder::table("albums")
                    .filter(PredicateExpr::eq("id", Value::U64(2)))
                    .project(["title"]),
                &storage,
            )
            .unwrap();

        assert!(first.recv().unwrap().is_empty());
        assert!(second.recv().unwrap().is_empty());
        assert_eq!(runtime.prepared_shapes.len(), 1);
        assert_eq!(
            runtime
                .binding_sources
                .values()
                .next()
                .unwrap()
                .refcounts
                .len(),
            2
        );

        let albums = schema.table("albums").unwrap().record_schema();
        runtime
            .tick(
                vec![TableDelta {
                    table: "albums".to_owned(),
                    descriptor: albums,
                    deltas: vec![
                        RecordDelta {
                            record: albums
                                .create(&[Value::U64(1), Value::String("one".to_owned())])
                                .unwrap(),
                            weight: 1,
                        },
                        RecordDelta {
                            record: albums
                                .create(&[Value::U64(2), Value::String("two".to_owned())])
                                .unwrap(),
                            weight: 1,
                        },
                    ],
                }],
                &storage,
            )
            .unwrap();

        assert_eq!(
            first.recv().unwrap().to_values().unwrap(),
            vec![(vec![Value::String("one".to_owned())], 1)]
        );
        assert_eq!(
            second.recv().unwrap().to_values().unwrap(),
            vec![(vec![Value::String("two".to_owned())], 1)]
        );
    }

    #[test]
    fn project_emits_copied_literal_and_null_columns() {
        let schema = albums_schema();
        let mut runtime = IvmRuntime::new(schema.clone()).unwrap();
        let storage = crate::storage::MemoryStorage::new(&["albums"]);
        let subscription = runtime
            .subscribe(
                GraphBuilder::table("albums").project_fields([
                    ProjectField::renamed("id", "id"),
                    ProjectField::literal(
                        "event_kind",
                        LiteralValue::String("result_content".to_owned()),
                    ),
                    ProjectField::null_typed(
                        "missing_title",
                        ValueType::Nullable(Box::new(ValueType::String)),
                    ),
                ]),
                &storage,
            )
            .unwrap();

        assert!(subscription.recv().unwrap().is_empty());
        assert!(runtime.graph.nodes().values().any(|node| {
            let OpType::MapProject(project) = &node.descriptor.operator else {
                return false;
            };
            project.mapping == vec![(0, 0)]
                && project.expressions.len() == 3
                && !projection_uses_raw_copy(
                    &project.expressions,
                    &project.mapping,
                    node.descriptor.output,
                )
        }));

        let albums = schema.table("albums").unwrap().record_schema();
        runtime
            .tick(
                vec![TableDelta {
                    table: "albums".to_owned(),
                    descriptor: albums,
                    deltas: vec![
                        RecordDelta {
                            record: albums
                                .create(&[Value::U64(1), Value::String("one".to_owned())])
                                .unwrap(),
                            weight: 2,
                        },
                        RecordDelta {
                            record: albums
                                .create(&[Value::U64(2), Value::String("two".to_owned())])
                                .unwrap(),
                            weight: -3,
                        },
                    ],
                }],
                &storage,
            )
            .unwrap();

        assert_eq!(
            subscription.recv().unwrap().to_values().unwrap(),
            vec![
                (
                    vec![
                        Value::U64(1),
                        Value::String("result_content".to_owned()),
                        Value::Nullable(None),
                    ],
                    2,
                ),
                (
                    vec![
                        Value::U64(2),
                        Value::String("result_content".to_owned()),
                        Value::Nullable(None),
                    ],
                    -3,
                ),
            ]
        );
    }

    #[test]
    fn cold_project_hydration_materializes_literal_and_typed_null_columns() {
        let schema = albums_schema();
        let mut runtime = IvmRuntime::new(schema.clone()).unwrap();
        let storage = crate::storage::MemoryStorage::new(&["albums"]);
        let albums = schema.table("albums").unwrap().record_schema();
        let store = RecordStore::new(&storage, "albums", &albums);
        let first = albums
            .create(&[Value::U64(1), Value::String("one".to_owned())])
            .unwrap();
        let second = albums
            .create(&[Value::U64(2), Value::String("two".to_owned())])
            .unwrap();

        store
            .write_many(&[store.set(b"1", &first), store.set(b"2", &second)])
            .unwrap();

        let subscription = runtime
            .subscribe(
                GraphBuilder::table("albums").project_fields([
                    ProjectField::renamed("id", "id"),
                    ProjectField::literal("event_kind", LiteralValue::String("cold".to_owned())),
                    ProjectField::null_typed(
                        "missing_title",
                        ValueType::Nullable(Box::new(ValueType::String)),
                    ),
                ]),
                &storage,
            )
            .unwrap();

        let mut initial = subscription.recv().unwrap().to_values().unwrap();
        initial.sort_by_key(|(values, _)| {
            let Value::U64(id) = values[0] else {
                unreachable!()
            };
            id
        });
        assert_eq!(
            initial,
            vec![
                (
                    vec![
                        Value::U64(1),
                        Value::String("cold".to_owned()),
                        Value::Nullable(None),
                    ],
                    1,
                ),
                (
                    vec![
                        Value::U64(2),
                        Value::String("cold".to_owned()),
                        Value::Nullable(None),
                    ],
                    1,
                ),
            ]
        );
    }

    #[test]
    fn pure_copy_project_lowers_with_full_fast_mapping() {
        let schema = albums_schema();
        let mut runtime = IvmRuntime::new(schema).unwrap();
        let storage = crate::storage::MemoryStorage::new(&["albums"]);
        let subscription = runtime
            .subscribe(
                GraphBuilder::table("albums").project(["id", "title"]),
                &storage,
            )
            .unwrap();

        assert!(subscription.recv().unwrap().is_empty());
        assert!(runtime.graph.nodes().values().any(|node| {
            let OpType::MapProject(project) = &node.descriptor.operator else {
                return false;
            };
            project.mapping == vec![(0, 0), (0, 1)]
                && project.expressions.len() == 2
                && project
                    .expressions
                    .iter()
                    .all(|expr| matches!(expr.expression, PlanExpr::Field(_)))
        }));
    }

    #[test]
    fn auto_family_hidden_field_does_not_collide_with_user_column() {
        let schema = DatabaseSchema::new([TableSchema::new(
            "records",
            [
                ColumnSchema::new("id", ColumnType::U64),
                ColumnSchema::new("__auto_binding_0", ColumnType::String),
            ],
        )
        .with_primary_key(PrimaryKey::new("id", IntegerKeyType::U64))]);
        let mut runtime = IvmRuntime::new(schema.clone()).unwrap();
        let storage = crate::storage::MemoryStorage::new(&["records"]);
        let first = runtime
            .subscribe(
                GraphBuilder::table("records")
                    .filter(PredicateExpr::eq("id", Value::U64(1)))
                    .project(["__auto_binding_0"]),
                &storage,
            )
            .unwrap();
        let second = runtime
            .subscribe(
                GraphBuilder::table("records")
                    .filter(PredicateExpr::eq("id", Value::U64(2)))
                    .project(["__auto_binding_0"]),
                &storage,
            )
            .unwrap();
        assert!(first.recv().unwrap().is_empty());
        assert!(second.recv().unwrap().is_empty());
        assert_eq!(runtime.prepared_shapes.len(), 1);

        let descriptor = schema.table("records").unwrap().record_schema();
        runtime
            .tick(
                vec![TableDelta {
                    table: "records".to_owned(),
                    descriptor,
                    deltas: vec![
                        RecordDelta {
                            record: descriptor
                                .create(&[Value::U64(1), Value::String("visible-one".to_owned())])
                                .unwrap(),
                            weight: 1,
                        },
                        RecordDelta {
                            record: descriptor
                                .create(&[Value::U64(2), Value::String("visible-two".to_owned())])
                                .unwrap(),
                            weight: 1,
                        },
                    ],
                }],
                &storage,
            )
            .unwrap();

        assert_eq!(
            first.recv().unwrap().to_values().unwrap(),
            vec![(vec![Value::String("visible-one".to_owned())], 1)]
        );
        assert_eq!(
            second.recv().unwrap().to_values().unwrap(),
            vec![(vec![Value::String("visible-two".to_owned())], 1)]
        );
    }

    #[test]
    fn auto_family_multi_join_is_byte_identical_to_direct_path() {
        let schema = DatabaseSchema::new([
            TableSchema::new(
                "albums",
                [
                    ColumnSchema::new("id", ColumnType::U64),
                    ColumnSchema::new("artist_id", ColumnType::U64),
                    ColumnSchema::new("title", ColumnType::String),
                ],
            )
            .with_primary_key(PrimaryKey::new("id", IntegerKeyType::U64)),
            TableSchema::new(
                "artists",
                [
                    ColumnSchema::new("id", ColumnType::U64),
                    ColumnSchema::new("name", ColumnType::String),
                ],
            )
            .with_primary_key(PrimaryKey::new("id", IntegerKeyType::U64)),
            TableSchema::new(
                "labels",
                [
                    ColumnSchema::new("id", ColumnType::U64),
                    ColumnSchema::new("artist_id", ColumnType::U64),
                    ColumnSchema::new("label", ColumnType::String),
                ],
            )
            .with_primary_key(PrimaryKey::new("id", IntegerKeyType::U64)),
        ]);
        let graph = |artist_id| {
            let albums = GraphBuilder::table("albums")
                .filter(PredicateExpr::eq("artist_id", Value::U64(artist_id)));
            let album_artists = GraphBuilder::join(
                albums,
                GraphBuilder::table("artists"),
                ["artist_id"],
                ["id"],
            )
            .project_fields([
                ProjectField::renamed("left.id", "album_id"),
                ProjectField::renamed("left.artist_id", "artist_id"),
                ProjectField::renamed("left.title", "title"),
                ProjectField::renamed("right.name", "artist"),
            ]);
            GraphBuilder::join(
                album_artists,
                GraphBuilder::table("labels"),
                ["artist_id"],
                ["artist_id"],
            )
            .project_fields([
                ProjectField::renamed("left.title", "title"),
                ProjectField::renamed("left.artist", "artist"),
                ProjectField::renamed("right.label", "label"),
            ])
        };
        let albums = schema.table("albums").unwrap().record_schema();
        let artists = schema.table("artists").unwrap().record_schema();
        let labels = schema.table("labels").unwrap().record_schema();
        let deltas = vec![
            TableDelta {
                table: "artists".to_owned(),
                descriptor: artists,
                deltas: vec![RecordDelta {
                    record: artists
                        .create(&[Value::U64(7), Value::String("Alice".to_owned())])
                        .unwrap(),
                    weight: 1,
                }],
            },
            TableDelta {
                table: "labels".to_owned(),
                descriptor: labels,
                deltas: vec![RecordDelta {
                    record: labels
                        .create(&[
                            Value::U64(70),
                            Value::U64(7),
                            Value::String("Impulse".to_owned()),
                        ])
                        .unwrap(),
                    weight: 1,
                }],
            },
            TableDelta {
                table: "albums".to_owned(),
                descriptor: albums,
                deltas: vec![RecordDelta {
                    record: albums
                        .create(&[
                            Value::U64(700),
                            Value::U64(7),
                            Value::String("Journey".to_owned()),
                        ])
                        .unwrap(),
                    weight: 1,
                }],
            },
        ];
        let familied_storage = crate::storage::MemoryStorage::new(&["albums", "artists", "labels"]);
        let direct_storage = crate::storage::MemoryStorage::new(&["albums", "artists", "labels"]);
        assert_auto_family_matches_direct(
            schema,
            &[graph(7), graph(8)],
            deltas,
            &familied_storage,
            &direct_storage,
        );
    }

    #[test]
    fn auto_family_recursive_shape_falls_back_to_byte_identical_direct_path() {
        let schema = edges_schema();
        let edges = schema.table("edges").unwrap().record_schema();
        let deltas = vec![TableDelta {
            table: "edges".to_owned(),
            descriptor: edges,
            deltas: vec![
                RecordDelta {
                    record: edges
                        .create(&[Value::U64(1), Value::U64(1), Value::U64(2)])
                        .unwrap(),
                    weight: 1,
                },
                RecordDelta {
                    record: edges
                        .create(&[Value::U64(2), Value::U64(2), Value::U64(3)])
                        .unwrap(),
                    weight: 1,
                },
                RecordDelta {
                    record: edges
                        .create(&[Value::U64(3), Value::U64(9), Value::U64(10)])
                        .unwrap(),
                    weight: 1,
                },
            ],
        }];
        let familied_storage = crate::storage::MemoryStorage::new(&["edges"]);
        let direct_storage = crate::storage::MemoryStorage::new(&["edges"]);
        assert_auto_family_matches_direct_with_prepared_count(
            schema,
            &[recursive_reach_from_graph(1), recursive_reach_from_graph(9)],
            deltas,
            &familied_storage,
            &direct_storage,
            0,
        );
    }

    #[test]
    fn auto_family_arg_max_by_shape_is_byte_identical_to_direct_path() {
        let schema = DatabaseSchema::new([TableSchema::new(
            "scores",
            [
                ColumnSchema::new("id", ColumnType::U64),
                ColumnSchema::new("group_id", ColumnType::U64),
                ColumnSchema::new("score", ColumnType::U64),
            ],
        )
        .with_primary_key(PrimaryKey::new("id", IntegerKeyType::U64))]);
        let graph = |group_id| {
            GraphBuilder::arg_max_by(
                GraphBuilder::table("scores")
                    .filter(PredicateExpr::eq("group_id", Value::U64(group_id))),
                ["group_id"],
                ["score"],
            )
            .project(["id", "group_id", "score"])
        };
        let scores = schema.table("scores").unwrap().record_schema();
        let deltas = vec![TableDelta {
            table: "scores".to_owned(),
            descriptor: scores,
            deltas: vec![
                RecordDelta {
                    record: scores
                        .create(&[Value::U64(1), Value::U64(1), Value::U64(10)])
                        .unwrap(),
                    weight: 1,
                },
                RecordDelta {
                    record: scores
                        .create(&[Value::U64(2), Value::U64(1), Value::U64(20)])
                        .unwrap(),
                    weight: 1,
                },
                RecordDelta {
                    record: scores
                        .create(&[Value::U64(3), Value::U64(2), Value::U64(15)])
                        .unwrap(),
                    weight: 1,
                },
            ],
        }];
        let familied_storage = crate::storage::MemoryStorage::new(&["scores"]);
        let direct_storage = crate::storage::MemoryStorage::new(&["scores"]);
        assert_auto_family_matches_direct(
            schema,
            &[graph(1), graph(2)],
            deltas,
            &familied_storage,
            &direct_storage,
        );
    }

    #[test]
    fn auto_family_excluded_recursive_shape_falls_back_to_direct_path() {
        let schema = edges_schema();
        let mut runtime = IvmRuntime::new(schema).unwrap();
        let storage = crate::storage::MemoryStorage::new(&["edges"]);
        let first = runtime
            .subscribe(recursive_reach_from_with_union_step_graph(1), &storage)
            .unwrap();
        let second = runtime
            .subscribe(recursive_reach_from_with_union_step_graph(9), &storage)
            .unwrap();

        assert!(first.recv().unwrap().is_empty());
        assert!(second.recv().unwrap().is_empty());
        assert!(runtime.prepared_shapes.is_empty());
        assert!(matches!(
            runtime.subscriptions.get(&first.id()).unwrap().target,
            SubscriptionTarget::Direct { .. }
        ));
        assert!(matches!(
            runtime.subscriptions.get(&second.id()).unwrap().target,
            SubscriptionTarget::Direct { .. }
        ));
    }

    #[test]
    fn subscription_retainers_keep_output_ancestors_alive() {
        let schema = albums_schema();
        let mut runtime = IvmRuntime::new(schema).unwrap();
        let temp_dir = tempfile::tempdir().unwrap();
        let storage = RocksDbStorage::open(temp_dir.path(), &["albums"]).unwrap();
        let subscription = runtime
            .subscribe(
                GraphBuilder::table("albums")
                    .filter(PredicateExpr::gt("id", Value::U64(10)))
                    .project(["title"]),
                &storage,
            )
            .unwrap();
        let output = runtime.subscription_output_node(subscription.id()).unwrap();
        let retained = runtime.retained_node_ids();

        assert_eq!(retained.len(), 3);
        assert!(retained.contains(&output));
        assert!(runtime.graph().node(output).is_some());
    }

    #[test]
    fn unsubscribe_eagerly_collects_unretained_ephemeral_nodes_and_state() {
        let schema = albums_schema();
        let mut runtime = IvmRuntime::new(schema).unwrap();
        let temp_dir = tempfile::tempdir().unwrap();
        let storage = RocksDbStorage::open(temp_dir.path(), &["albums"]).unwrap();
        let subscription = runtime
            .subscribe(GraphBuilder::table("albums"), &storage)
            .unwrap();
        let output = runtime.subscription_output_node(subscription.id()).unwrap();

        assert_eq!(runtime.retained_node_ids().len(), 1);
        assert!(
            runtime
                .node_meta
                .get(&output)
                .is_some_and(|meta| !meta.retainers.is_empty())
        );

        assert!(runtime.unsubscribe(subscription.id()));

        assert!(runtime.retained_node_ids().is_empty());
        assert!(runtime.graph().node(output).is_none());
        assert!(!runtime.node_meta.contains_key(&output));
    }

    #[test]
    fn durable_schema_nodes_are_runtime_retainer_roots() {
        let schema = indexed_albums_schema();
        let runtime = IvmRuntime::new(schema).unwrap();
        let retained = runtime.retained_node_ids();
        let durable_nodes = retained
            .iter()
            .copied()
            .filter(|node| {
                runtime
                    .graph()
                    .node(*node)
                    .is_some_and(|node| node.is_durable())
            })
            .collect::<Vec<_>>();

        assert_eq!(durable_nodes.len(), 1);
        assert_eq!(retained.len(), 3);
    }

    #[test]
    fn unsupported_query_operator_variants_are_not_executable() {
        let schema = albums_schema();
        let storage = crate::storage::MemoryStorage::new(&["albums"]);
        let mut runtime = IvmRuntime::new(schema).unwrap();
        let input = runtime
            .add_dedup_graph(&GraphBuilder::table("albums"))
            .unwrap()
            .node;
        let output = *runtime.table_descriptor("albums").unwrap();

        let join = JoinOp {
            kind: JoinOpKind::Inner,
            left_key: vec![PlanExpr::Field("id".to_owned())],
            right_key: vec![PlanExpr::Field("id".to_owned())],
            left_descriptor: output,
            right_descriptor: output,
            residual_predicate: None,
        };
        let unsupported = [
            OpType::SemiJoin(join),
            OpType::Distinct,
            OpType::Negate,
            OpType::Aggregate(AggregateOp {
                group_key: vec![PlanExpr::Field("id".to_owned())],
                aggregates: vec![crate::ivm::AggregateExpr {
                    function: AggregateFunction::Count,
                    expression: None,
                    distinct: false,
                    output_name: Some("count".to_owned()),
                }],
            }),
        ];

        for operator in unsupported {
            let inputs = match operator {
                OpType::SemiJoin(_) => vec![input, input],
                _ => vec![input],
            };
            let node = runtime.graph.dedup_node(
                NodeDescriptor::new(operator, inputs, output),
                NodeDurability::Ephemeral,
            );
            assert!(matches!(
                runtime.hydration_snapshot(node, &storage),
                Err(IvmRuntimeError::UnsupportedOperator)
            ));
        }
    }

    #[test]
    fn stale_as_of_state_rejects_wrong_or_backward_logical_time() {
        let mut state = AsOf::<usize, SubTick>::new(7);

        assert!(matches!(
            state.value_at(SubTick {
                tick: 0,
                sub_tick: 0
            }),
            Err(IvmRuntimeError::StaleRuntimeState { .. })
        ));
        state
            .mark_forward_as_of(SubTick {
                tick: 0,
                sub_tick: 2,
            })
            .unwrap();
        assert_eq!(
            *state
                .value_at(SubTick {
                    tick: 0,
                    sub_tick: 2,
                })
                .unwrap(),
            7
        );
        assert!(matches!(
            state.value_at(SubTick {
                tick: 0,
                sub_tick: 1
            }),
            Err(IvmRuntimeError::StaleRuntimeState { .. })
        ));
        assert!(matches!(
            state.mark_forward_as_of(SubTick {
                tick: 0,
                sub_tick: 1
            }),
            Err(IvmRuntimeError::OutOfOrderRuntimeState { .. })
        ));
    }

    #[test]
    fn similar_join_subscriptions_share_context_independent_base_arrangements() {
        let schema = albums_artists_schema();
        let mut runtime = IvmRuntime::new(schema.clone()).unwrap();
        let temp_dir = tempfile::tempdir().unwrap();
        let storage = RocksDbStorage::open(temp_dir.path(), &["albums", "artists"]).unwrap();
        let _first = runtime
            .subscribe(
                GraphBuilder::join(
                    GraphBuilder::table("albums"),
                    GraphBuilder::table("artists"),
                    ["artist_id"],
                    ["id"],
                ),
                &storage,
            )
            .unwrap();
        let _second = runtime
            .subscribe(
                GraphBuilder::join(
                    GraphBuilder::table("albums").filter(PredicateExpr::gt("id", Value::U64(0))),
                    GraphBuilder::table("artists"),
                    ["artist_id"],
                    ["id"],
                ),
                &storage,
            )
            .unwrap();

        let albums = schema.table("albums").unwrap().record_schema();
        let artists = schema.table("artists").unwrap().record_schema();
        runtime
            .tick(
                vec![
                    TableDelta {
                        table: "albums".to_owned(),
                        descriptor: albums,
                        deltas: vec![RecordDelta {
                            record: albums
                                .create(&[
                                    Value::U64(7),
                                    Value::U64(11),
                                    Value::String("Blue Train".to_owned()),
                                ])
                                .unwrap(),
                            weight: 1,
                        }],
                    },
                    TableDelta {
                        table: "artists".to_owned(),
                        descriptor: artists,
                        deltas: vec![RecordDelta {
                            record: artists
                                .create(&[
                                    Value::U64(11),
                                    Value::String("John Coltrane".to_owned()),
                                ])
                                .unwrap(),
                            weight: 1,
                        }],
                    },
                ],
                &storage,
            )
            .unwrap();

        let artist_arrangements = runtime
            .arrangement_states
            .keys()
            .filter(|key| {
                key.scope == ScopePath::root()
                    && key.descriptor == artists
                    && key.fields == vec!["id".to_owned()]
            })
            .count();

        assert_eq!(artist_arrangements, 1);
        let stats = runtime.stats();
        assert_eq!(stats.arrangement_count, 3);
        assert!(stats.arrangement_rows >= 2);
        assert!(stats.arrangement_encoded_bytes > 0);
        assert!(stats.logical_nodes_requested > stats.deduped_graph_nodes as u64);
        assert!(stats.dedupe_ratio() < 1.0);
    }

    #[test]
    fn recursive_recompute_reuses_graph_nodes_without_persisting_contextual_child_state() {
        let schema = edges_schema();
        let mut runtime = IvmRuntime::new(schema.clone()).unwrap();
        let temp_dir = tempfile::tempdir().unwrap();
        let storage = RocksDbStorage::open(temp_dir.path(), &["edges"]).unwrap();
        let first = runtime
            .subscribe(recursive_reach_graph(), &storage)
            .unwrap();
        let second = runtime
            .subscribe(recursive_reach_graph(), &storage)
            .unwrap();

        assert_eq!(
            runtime.subscription_output_node(first.id()),
            runtime.subscription_output_node(second.id())
        );

        let edges = schema.table("edges").unwrap().record_schema();
        let table_delta = TableDelta {
            table: "edges".to_owned(),
            descriptor: edges,
            deltas: vec![
                RecordDelta {
                    record: edges
                        .create(&[Value::U64(1), Value::U64(1), Value::U64(2)])
                        .unwrap(),
                    weight: 1,
                },
                RecordDelta {
                    record: edges
                        .create(&[Value::U64(2), Value::U64(2), Value::U64(3)])
                        .unwrap(),
                    weight: 1,
                },
            ],
        };
        runtime.tick(vec![table_delta], &storage).unwrap();

        assert!(
            runtime
                .operator_states
                .keys()
                .all(|key| key.scope == ScopePath::root()),
            "recursive recomputation should not leave per-context child state in runtime"
        );
    }

    #[test]
    fn key_encoding_preserves_value_order_for_index_range_scans() {
        let mut encoded = [
            Value::U64(1),
            Value::U64(256),
            Value::String("aa".to_owned()),
            Value::String("b".to_owned()),
            Value::F64(f64::NEG_INFINITY),
            Value::F64(-1.0),
            Value::F64(-0.0),
            Value::F64(0.0),
            Value::F64(1.0),
            Value::F64(f64::INFINITY),
            Value::Bytes(b"a\0b".to_vec()),
            Value::Bytes(b"a\0c".to_vec()),
        ]
        .into_iter()
        .map(|value| {
            let mut key = Vec::new();
            encode_key_part(&mut key, &value).unwrap();
            (value, key)
        })
        .collect::<Vec<_>>();

        encoded.sort_by(|left, right| left.1.cmp(&right.1));

        assert_eq!(
            encoded
                .into_iter()
                .map(|(value, _)| value)
                .collect::<Vec<_>>(),
            [
                Value::U64(1),
                Value::U64(256),
                Value::F64(f64::NEG_INFINITY),
                Value::F64(-1.0),
                Value::F64(-0.0),
                Value::F64(0.0),
                Value::F64(1.0),
                Value::F64(f64::INFINITY),
                Value::String("aa".to_owned()),
                Value::String("b".to_owned()),
                Value::Bytes(b"a\0b".to_vec()),
                Value::Bytes(b"a\0c".to_vec()),
            ]
        );
        let mut key = Vec::new();
        assert!(matches!(
            encode_key_part(&mut key, &Value::F64(f64::NAN)),
            Err(IvmRuntimeError::RecordEncoding(
                records::Error::InvalidF64NaN
            ))
        ));
    }
}
