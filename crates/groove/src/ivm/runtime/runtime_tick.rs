//! Tick orchestration, hydration, memo eviction, and durable-node evaluation.

use std::collections::VecDeque;
use std::future::Future;
use std::pin::Pin;
use std::rc::Rc;
use std::sync::Mutex;
use std::task::{Context, Poll};

use super::evaluation_session::{
    EvaluationInputs, EvaluationRequestFailure, EvaluationRequestKey, EvaluationRequests,
};
use super::*;
use crate::storage::OwnedStorage;

/// Hydration can discover a large resident graph in one poll. Keep every
/// owner turn bounded so a browser worker returns to transport ingress between
/// CPU-only graph slices. Cold storage takes the separate request-pending path
/// below; its waker is intentionally not used to classify the pending reason.
const MAX_HYDRATION_RUNNABLE_NODES_PER_POLL: usize = 32;

/// Owned preparation state for one interruptible evaluation.
///
/// Storage suspension and dependency ordering live in `EvaluationWorkQueue`.
/// Every root, including retained roots without an active subscription, is
/// evaluated only after its inputs have completed.
struct EvaluationSession<'a> {
    relevant_nodes: HashSet<NodeId>,
    roots: HashSet<NodeId>,
    outputs: HashMap<NodeId, RecordDeltas>,
    pending_outputs: HashMap<NodeId, RecordDeltas>,
    operator_states: HashMap<OperatorStateKey, OperatorState>,
    arrangement_states: HashMap<ArrangementKey, AsOf<ArrangementState, SubTick>>,
    arrangement_keys_by_input: HashMap<NodeId, HashSet<ArrangementKey>>,
    eval_memo: HashMap<EvalMemoKey, EvalMemoEntry>,
    eval_memo_bytes: usize,
    memo_use_clock: u64,
    node_meta: HashMap<NodeId, NodeRuntimeMeta>,
    binding_frontiers: HashMap<String, u64>,
    storage: OwnedStorage<'a>,
    requests: EvaluationRequests<'a>,
    evaluation_inputs: EvaluationInputs,
    work_queue: EvaluationWorkQueue,
}

pub(super) struct IncrementalEvaluation<'a> {
    table_deltas: Vec<TableDelta>,
    binding_deltas: Vec<BindingDelta>,
    binding_snapshots: HashMap<String, RecordDeltas>,
    table_frontiers: HashMap<String, u64>,
    binding_frontiers: HashMap<String, u64>,
    current_tick: u64,
    metrics: TickMetrics,
    storage: OwnedStorage<'a>,
    requests: EvaluationRequests<'a>,
    evaluation_inputs: Option<EvaluationInputs>,
    work_queue: EvaluationWorkQueue,
    published_subscriptions: HashSet<SubscriptionId>,
    affected_nodes: HashSet<NodeId>,
    affected_subscriptions: HashSet<SubscriptionId>,
    /// Relational output retained while logical terminal materialization waits
    /// for immutable chunks. Re-evaluating after operator state advances can
    /// correctly yield an empty delta, so publication owns this exact value.
    pending_subscription_outputs: HashMap<NodeId, Arc<RecordDeltas>>,
    terminal_deltas: HashMap<NodeId, TerminalDeltas>,
    root_ordering_windows: HashMap<NodeId, RootOrderingWindows>,
    notification_publication: Option<PublicationId>,
    defer_notifications_until_durable: bool,
    pending_resident_publication: Option<PendingResidentPublication>,
}

#[derive(Clone)]
struct PendingResidentPublication {
    publication: Rc<Cell<Option<PublicationId>>>,
    notifications: Rc<RefCell<Vec<(SubscriptionId, QueuedMultisinkDeltas)>>>,
    completed: Rc<Cell<bool>>,
    defer_notifications_until_durable: bool,
    chunk_provider: Option<crate::chunks::OwnedChunkProvider>,
}

pub(crate) struct ResidentTick {
    pub(crate) metrics: TickMetrics,
    publication: PendingResidentPublication,
}

struct EvaluationFailure {
    kind: EvaluationFailureKind,
    affected_nodes: HashSet<NodeId>,
    error: Arc<IvmRuntimeError>,
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum EvaluationFailureKind {
    Scoped,
    Fatal,
}

impl EvaluationFailure {
    fn into_error(self) -> IvmRuntimeError {
        Arc::try_unwrap(self.error).expect("unshared evaluation failure")
    }
}

impl From<IvmRuntimeError> for EvaluationFailure {
    fn from(error: IvmRuntimeError) -> Self {
        Self {
            kind: EvaluationFailureKind::Fatal,
            affected_nodes: HashSet::default(),
            error: Arc::new(error),
        }
    }
}

#[derive(Default)]
struct PendingIncrementalState {
    evaluations: BTreeMap<u64, PendingEvaluation>,
    order: VecDeque<u64>,
    waiters_by_node: HashMap<NodeId, VecDeque<u64>>,
    next_id: u64,
    /// Why the current queue head yielded. This is runtime state rather than
    /// an inference from a caller's waker: storage futures may wake eagerly
    /// while they are still cold.
    pending_progress: Option<PendingProgress>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum PendingProgress {
    ResidentContinuation,
    Storage,
}

enum PendingEvaluation {
    Incremental(IncrementalEvaluation<'static>),
    SubscriptionHydration(PendingSubscriptionHydration),
}

struct PendingSubscriptionHydration {
    subscription_id: SubscriptionId,
    outputs: BTreeMap<String, CompiledNode>,
    initial: Arc<Mutex<Option<MultisinkDeltas>>>,
    session: EvaluationSession<'static>,
    binding_snapshots: HashMap<String, RecordDeltas>,
    hydrate_arrangements: bool,
    metrics: TickMetrics,
}

impl PendingEvaluation {
    fn work_queue_mut(&mut self) -> &mut EvaluationWorkQueue {
        match self {
            Self::Incremental(evaluation) => &mut evaluation.work_queue,
            Self::SubscriptionHydration(evaluation) => &mut evaluation.session.work_queue,
        }
    }

    fn work_queue(&self) -> &EvaluationWorkQueue {
        match self {
            Self::Incremental(evaluation) => &evaluation.work_queue,
            Self::SubscriptionHydration(evaluation) => &evaluation.session.work_queue,
        }
    }
}

#[derive(Default)]
pub(super) struct PendingIncrementalEvaluation(Rc<RefCell<PendingIncrementalState>>);

impl Clone for PendingIncrementalEvaluation {
    fn clone(&self) -> Self {
        Self(Rc::clone(&self.0))
    }
}

impl std::fmt::Debug for PendingIncrementalEvaluation {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("PendingIncrementalEvaluation")
            .field("pending", &self.0.borrow().order.len())
            .finish()
    }
}

impl PendingIncrementalEvaluation {
    pub(super) fn is_pending(&self) -> bool {
        !self.0.borrow().order.is_empty()
    }

    fn has_resident_continuation(&self) -> bool {
        self.0.borrow().pending_progress == Some(PendingProgress::ResidentContinuation)
    }
}

/// Discovers request-producing leaves for all reachable siblings without recursively
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
    request_dependents: std::collections::BTreeMap<EvaluationRequestKey, Vec<NodeId>>,
    runnable: VecDeque<NodeId>,
    roots: HashSet<NodeId>,
    completed_events: Vec<NodeId>,
    temporal_waiting: HashMap<NodeId, usize>,
}

impl EvaluationWorkQueue {
    fn new(roots: impl IntoIterator<Item = NodeId>) -> Self {
        let roots = roots.into_iter().collect::<VecDeque<_>>();
        Self {
            pending: roots.clone(),
            visited: HashSet::default(),
            entries: HashMap::default(),
            dependents: HashMap::default(),
            request_dependents: std::collections::BTreeMap::new(),
            runnable: VecDeque::new(),
            roots: roots.into_iter().collect(),
            completed_events: Vec::new(),
            temporal_waiting: HashMap::default(),
        }
    }

    fn discover_hydration(
        self,
        graph: &IvmGraph,
    ) -> Result<(HashSet<NodeId>, Self), IvmRuntimeError> {
        self.discover(graph, true, std::collections::BTreeMap::new())
    }

    fn discover_incremental(
        self,
        graph: &IvmGraph,
    ) -> Result<(HashSet<NodeId>, Self), IvmRuntimeError> {
        self.discover(graph, false, std::collections::BTreeMap::new())
    }

    fn discover(
        mut self,
        graph: &IvmGraph,
        hydrate_sources: bool,
        request_dependents: std::collections::BTreeMap<EvaluationRequestKey, Vec<NodeId>>,
    ) -> Result<(HashSet<NodeId>, Self), IvmRuntimeError> {
        let mut request_dependencies_by_node = HashMap::<NodeId, usize>::default();
        for nodes in request_dependents.values() {
            for node in nodes {
                *request_dependencies_by_node.entry(*node).or_default() += 1;
            }
        }
        self.request_dependents = request_dependents;
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
            let request = if hydrate_sources {
                match &node.descriptor.operator {
                    OpType::TableSource(source) => NodeState::table_source_request(source)?,
                    OpType::IndexSource(source) => NodeState::index_source_request(source)?,
                    _ => None,
                }
                .map(EvaluationRequestKey::Storage)
            } else {
                None
            };
            if let Some(request) = request {
                self.request_dependents
                    .entry(request)
                    .or_default()
                    .push(node_id);
                self.entries.insert(node_id, EvaluationEntry::Waiting(1));
            } else {
                self.entries.insert(
                    node_id,
                    EvaluationEntry::Waiting(
                        node.descriptor.inputs.len()
                            + request_dependencies_by_node
                                .get(&node_id)
                                .copied()
                                .unwrap_or_default(),
                    ),
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

    fn requests(&self) -> impl Iterator<Item = &EvaluationRequestKey> {
        self.request_dependents.keys()
    }

    fn requests_ready(&mut self, requests: impl IntoIterator<Item = EvaluationRequestKey>) {
        let ready_nodes = requests
            .into_iter()
            .flat_map(|request| self.request_dependents.remove(&request).unwrap_or_default())
            .collect::<Vec<_>>();
        for node in ready_nodes {
            let Some(EvaluationEntry::Waiting(remaining)) = self.entries.get_mut(&node) else {
                continue;
            };
            *remaining = remaining.saturating_sub(1);
            if *remaining == 0 {
                self.make_runnable(node);
            }
        }
    }

    fn storage_already_resident(&mut self, resident_nodes: &HashSet<NodeId>) {
        let mut ready_nodes = Vec::new();
        self.request_dependents.retain(|_, dependents| {
            dependents.retain(|node| {
                if resident_nodes.contains(node) {
                    ready_nodes.push(*node);
                    false
                } else {
                    true
                }
            });
            !dependents.is_empty()
        });
        for node in ready_nodes {
            let Some(EvaluationEntry::Waiting(remaining)) = self.entries.get_mut(&node) else {
                continue;
            };
            *remaining = remaining.saturating_sub(1);
            if *remaining == 0 {
                self.make_runnable(node);
            }
        }
    }

    fn wait_for_requests(
        &mut self,
        node: NodeId,
        requests: impl IntoIterator<Item = EvaluationRequestKey>,
    ) {
        let requests = requests.into_iter().collect::<Vec<_>>();
        self.entries
            .insert(node, EvaluationEntry::Waiting(requests.len()));
        for request in requests {
            let dependents = self.request_dependents.entry(request).or_default();
            if !dependents.contains(&node) {
                dependents.push(node);
            }
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
        self.completed_events.push(node);
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

    /// Retain a runnable node at the front of the queue after its private step
    /// deliberately yields. The step future itself is disposable; resumable
    /// operators keep their bounded continuation in operator state.
    fn requeue_yielded(&mut self, node: NodeId) {
        self.entries.insert(node, EvaluationEntry::Runnable);
        self.runnable.push_front(node);
    }

    /// A runnable node is an explicit in-memory evaluator continuation.
    /// Requests are represented separately as `Waiting`, so an eager cold
    /// storage wake cannot be mistaken for CPU work.
    fn has_resident_continuation(&self) -> bool {
        !self.runnable.is_empty()
    }

    fn is_root(&self, node: NodeId) -> bool {
        self.roots.contains(&node)
    }

    fn is_complete(&self, node: NodeId) -> bool {
        self.entries.get(&node) == Some(&EvaluationEntry::Complete)
    }

    fn roots_complete(&self) -> bool {
        self.roots.iter().all(|node| self.is_complete(*node))
    }

    fn incomplete_nodes(&self) -> impl Iterator<Item = NodeId> + '_ {
        self.entries
            .iter()
            .filter_map(|(node, entry)| (*entry != EvaluationEntry::Complete).then_some(*node))
    }

    fn downstream_closure(&self, roots: impl IntoIterator<Item = NodeId>) -> HashSet<NodeId> {
        let mut affected = HashSet::default();
        let mut pending = roots.into_iter().collect::<VecDeque<_>>();
        while let Some(node) = pending.pop_front() {
            if !affected.insert(node) {
                continue;
            }
            pending.extend(self.dependents.get(&node).into_iter().flatten().copied());
        }
        affected
    }

    fn failure_for_request(
        &self,
        request: &EvaluationRequestKey,
        failure: EvaluationRequestFailure,
    ) -> EvaluationFailure {
        let kind = if failure.publication_metadata_durability {
            EvaluationFailureKind::Fatal
        } else {
            EvaluationFailureKind::Scoped
        };
        EvaluationFailure {
            kind,
            affected_nodes: self.downstream_closure(
                self.request_dependents
                    .get(request)
                    .into_iter()
                    .flatten()
                    .copied(),
            ),
            error: Arc::new(failure.error),
        }
    }

    fn failure_for_node(&self, node: NodeId, error: IvmRuntimeError) -> EvaluationFailure {
        EvaluationFailure {
            kind: EvaluationFailureKind::Scoped,
            affected_nodes: self.downstream_closure([node]),
            error: Arc::new(error),
        }
    }

    fn abandon(&mut self, nodes: &HashSet<NodeId>) {
        self.runnable.retain(|node| !nodes.contains(node));
        self.request_dependents
            .retain(|_, dependents| !dependents.iter().all(|node| nodes.contains(node)));
        for node in nodes {
            if self.entries.contains_key(node) {
                self.entries.insert(*node, EvaluationEntry::Complete);
                self.temporal_waiting.remove(node);
            }
        }
    }

    fn add_temporal_blockers(&mut self, blockers: &HashMap<NodeId, usize>) {
        let blocked = self
            .entries
            .keys()
            .filter_map(|node| blockers.get(node).map(|count| (*node, *count)))
            .collect::<Vec<_>>();
        for (node, count) in blocked {
            if count == 0 {
                continue;
            }
            self.temporal_waiting.insert(node, count);
            match self.entries.get(&node).copied() {
                Some(EvaluationEntry::Runnable) => {
                    self.runnable.retain(|candidate| *candidate != node);
                    self.entries.insert(node, EvaluationEntry::Waiting(count));
                }
                Some(EvaluationEntry::Waiting(remaining)) => {
                    self.entries
                        .insert(node, EvaluationEntry::Waiting(remaining + count));
                }
                Some(EvaluationEntry::Complete) | None => {}
            }
        }
    }

    fn temporal_ready(&mut self, node: NodeId) {
        let Some(temporal_remaining) = self.temporal_waiting.get_mut(&node) else {
            return;
        };
        *temporal_remaining = temporal_remaining.saturating_sub(1);
        if *temporal_remaining == 0 {
            self.temporal_waiting.remove(&node);
        }
        let Some(EvaluationEntry::Waiting(remaining)) = self.entries.get_mut(&node) else {
            return;
        };
        *remaining = remaining.saturating_sub(1);
        if *remaining == 0 {
            self.make_runnable(node);
        }
    }

    fn drain_completed_events(&mut self) -> Vec<NodeId> {
        std::mem::take(&mut self.completed_events)
    }
}

impl IncrementalEvaluation<'_> {
    fn abandon(&mut self, nodes: &HashSet<NodeId>) {
        self.work_queue.abandon(nodes);
        self.terminal_deltas.retain(|node, _| !nodes.contains(node));
        self.root_ordering_windows
            .retain(|node, _| !nodes.contains(node));
    }

    fn poll(
        &mut self,
        runtime: &mut IvmRuntime,
        cx: &mut Context<'_>,
    ) -> Poll<Result<(), EvaluationFailure>> {
        self.requests.poll(cx);
        let ready = match self.requests.drain_ready() {
            Ok(ready) => ready,
            Err(error) => {
                let (request, error) = *error;
                return Poll::Ready(Err(self.work_queue.failure_for_request(&request, error)));
            }
        };
        self.work_queue.requests_ready(ready.keys().cloned());
        if let Some(inputs) = &mut self.evaluation_inputs {
            inputs.install(ready);
        }

        let mut dropped_subscriptions = Vec::new();
        let mut evaluator = TickEvaluator {
            schema: &runtime.schema,
            graph: &runtime.graph,
            variant_projections: &runtime.variant_projections,
            table_deltas: &self.table_deltas,
            binding_deltas: &self.binding_deltas,
            binding_snapshots: &self.binding_snapshots,
            current_tick: self.current_tick,
            operator_states: &mut runtime.operator_states,
            arrangement_states: &mut runtime.arrangement_states,
            arrangement_keys_by_input: &mut runtime.arrangement_keys_by_input,
            eval_memo: &mut runtime.eval_memo,
            eval_memo_bytes: &mut runtime.eval_memo_bytes,
            table_frontiers: &self.table_frontiers,
            binding_frontiers: &self.binding_frontiers,
            memo_use_clock: &mut runtime.memo_use_clock,
            node_meta: &mut runtime.node_meta,
            storage: Some(self.storage.as_ref()),
            evaluation_inputs: self.evaluation_inputs.as_mut(),
            context: EvalContext::root(),
            metrics: &mut self.metrics,
            terminal_deltas: std::mem::take(&mut self.terminal_deltas),
            root_ordering_windows: std::mem::take(&mut self.root_ordering_windows),
        };

        let mut registered_requests = false;
        while let Some(node) = self.work_queue.runnable.pop_front() {
            let result = {
                let mut future = evaluator.update_node(node);
                Pin::new(&mut future).poll(cx)
            };
            match result {
                Poll::Ready(Ok(_)) => self.work_queue.complete(node),
                Poll::Ready(Err(IvmRuntimeError::EvaluationBlocked)) => {
                    let requests = evaluator
                        .evaluation_inputs
                        .as_deref_mut()
                        .map(EvaluationInputs::take_missing)
                        .unwrap_or_default();
                    if requests.is_empty() {
                        return Poll::Ready(Err(self
                            .work_queue
                            .failure_for_node(node, IvmRuntimeError::EvaluationBlocked)));
                    }
                    for request in requests.iter().cloned() {
                        registered_requests |= self.requests.request(
                            request,
                            &self.storage,
                            Some(
                                self.pending_resident_publication
                                    .as_ref()
                                    .and_then(|pending| pending.chunk_provider.as_ref())
                                    .unwrap_or(&runtime.chunk_provider),
                            ),
                            &runtime.schema,
                        );
                    }
                    self.work_queue.wait_for_requests(node, requests);
                }
                Poll::Ready(Err(error)) => {
                    return Poll::Ready(Err(self.work_queue.failure_for_node(node, error)));
                }
                Poll::Pending => {
                    self.work_queue.requeue_yielded(node);
                    drop(evaluator);
                    cx.waker().wake_by_ref();
                    return Poll::Pending;
                }
            }
        }
        let immediately_ready = if registered_requests {
            self.requests.poll(cx)
        } else {
            0
        };
        if immediately_ready > 0 {
            // Resident requests completed synchronously. Install their results
            // and resume the queue within this same public poll so resident
            // writes retain their same-tick visibility contract.
            drop(evaluator);
            return self.poll(runtime, cx);
        }

        let mut terminal_consumers = HashMap::<NodeId, usize>::default();
        for subscription_id in &self.affected_subscriptions {
            let Some(subscription) = runtime.multisink_subscriptions.get(subscription_id) else {
                continue;
            };
            if subscription.failed || self.published_subscriptions.contains(subscription_id) {
                continue;
            }
            for output in subscription
                .outputs
                .values()
                .filter(|output| self.affected_nodes.contains(&output.node))
            {
                if let Some(node) = evaluator.terminal_delta_node_for_output(output.node)? {
                    *terminal_consumers.entry(node).or_default() += 1;
                }
            }
        }

        for subscription_id in &self.affected_subscriptions {
            let Some(subscription) = runtime.multisink_subscriptions.get(subscription_id) else {
                continue;
            };
            if subscription.failed
                || self.published_subscriptions.contains(subscription_id)
                || subscription
                    .outputs
                    .values()
                    .filter(|output| self.affected_nodes.contains(&output.node))
                    .any(|output| {
                        !self.work_queue.is_complete(output.node)
                            || output
                                .root_ordering_node
                                .is_some_and(|node| !self.work_queue.is_complete(node))
                    })
            {
                continue;
            }
            let mut prepared_outputs = Vec::new();
            for (sink, output) in &subscription.outputs {
                if !self.affected_nodes.contains(&output.node) {
                    continue;
                }
                let physical_records = if let Some(records) =
                    self.pending_subscription_outputs.get(&output.node)
                {
                    Arc::clone(records)
                } else {
                    let records = {
                        let mut future = evaluator.update_node(output.node);
                        match Pin::new(&mut future).poll(cx) {
                            Poll::Ready(result) => result?,
                            Poll::Pending => {
                                return Poll::Ready(Err(IvmRuntimeError::EvaluationBlocked.into()));
                            }
                        }
                    };
                    self.pending_subscription_outputs
                        .insert(output.node, Arc::clone(&records));
                    records
                };
                let records = match evaluator.materialize_indirect_input(&physical_records) {
                    Ok(records) => {
                        self.pending_subscription_outputs
                            .insert(output.node, Arc::clone(&records));
                        records
                    }
                    Err(IvmRuntimeError::EvaluationBlocked) => {
                        self.terminal_deltas = std::mem::take(&mut evaluator.terminal_deltas);
                        self.root_ordering_windows =
                            std::mem::take(&mut evaluator.root_ordering_windows);
                        let requests = evaluator
                            .evaluation_inputs
                            .as_deref_mut()
                            .map(EvaluationInputs::take_missing)
                            .unwrap_or_default();
                        if requests.is_empty() {
                            return Poll::Ready(Err(IvmRuntimeError::EvaluationBlocked.into()));
                        }
                        for request in requests {
                            self.requests.request(
                                request,
                                &self.storage,
                                Some(
                                    self.pending_resident_publication
                                        .as_ref()
                                        .and_then(|pending| pending.chunk_provider.as_ref())
                                        .unwrap_or(&runtime.chunk_provider),
                                ),
                                &runtime.schema,
                            );
                        }
                        drop(evaluator);
                        let mut ready = self.requests.poll(cx);
                        if ready == 0 {
                            // A provider may deliberately yield once on a cold
                            // resident request. Advance that retained future a
                            // second time before yielding the whole commit.
                            ready = self.requests.poll(cx);
                        }
                        if ready > 0 {
                            return self.poll(runtime, cx);
                        }
                        return Poll::Pending;
                    }
                    Err(error) => return Poll::Ready(Err(error.into())),
                };
                prepared_outputs.push((sink, output, records));
            }

            let mut sinks = BTreeMap::new();
            let mut terminal_sinks = BTreeMap::new();
            for (sink, output, records) in prepared_outputs {
                if !records.deltas.is_empty()
                    && !records.descriptor.registry_compatible_with(&output.output)
                {
                    return Poll::Ready(Err(IvmRuntimeError::GraphOutputMismatch.into()));
                }
                let structured = evaluator.output_is_structured_collect_by(output.node)?;
                let public_root = evaluator.output_has_public_root(output.node)?;
                let terminal_owned = output.root_ordering_node.is_some() || structured;
                let records = records.as_ref().clone();
                if terminal_owned {
                    let terminal = if structured {
                        if let Some(node) = evaluator.terminal_delta_node_for_output(output.node)? {
                            let remaining = terminal_consumers
                                .get_mut(&node)
                                .expect("terminal consumer counted before publication");
                            *remaining -= 1;
                            evaluator.terminal_deltas_for_consumer(node, *remaining == 0)
                        } else if !public_root && !records.is_empty() {
                            Some(terminal_deltas_from_record_deltas(&records)?)
                        } else if output.root_ordering_node.is_some() {
                            Some(TerminalDeltas {
                                operations: Vec::new(),
                            })
                        } else {
                            None
                        }
                    } else if !records.is_empty() {
                        Some(terminal_deltas_from_record_deltas(&records)?)
                    } else if output.root_ordering_node.is_some() {
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
            let mut queued = QueuedMultisinkDeltas::new(records);
            let notification_publication = self
                .pending_resident_publication
                .as_ref()
                .and_then(|pending| pending.publication.get())
                .or(self.notification_publication);
            queued.publication = notification_publication;
            if !queued.deltas.is_empty() {
                if let Some(pending) = &self.pending_resident_publication
                    && notification_publication.is_none()
                {
                    pending
                        .notifications
                        .borrow_mut()
                        .push((*subscription_id, queued));
                } else if self.defer_notifications_until_durable
                    && notification_publication.is_some_and(|publication| {
                        !runtime
                            .durable_notification_publications
                            .contains(&publication)
                    })
                {
                    runtime
                        .deferred_notifications
                        .entry(notification_publication.expect("checked publication"))
                        .or_default()
                        .push((*subscription_id, queued));
                } else if subscription.sender.send(queued).is_err() {
                    dropped_subscriptions.push(*subscription_id);
                }
            }
            self.published_subscriptions.insert(*subscription_id);
        }
        self.terminal_deltas = std::mem::take(&mut evaluator.terminal_deltas);
        self.root_ordering_windows = std::mem::take(&mut evaluator.root_ordering_windows);

        if self.requests.has_pending() || !self.work_queue.roots_complete() {
            return Poll::Pending;
        }

        drop(evaluator);
        runtime
            .operator_states
            .retain(|key, _| key.scope == ScopeId::root());
        for subscription_id in dropped_subscriptions {
            runtime.unsubscribe(subscription_id);
        }
        debug_assert!(
            runtime.affected_recursive_nodes_are_current(&self.affected_nodes, self.current_tick)
        );
        runtime.evict_eval_memo();
        if let Some(pending) = &self.pending_resident_publication
            && pending.publication.get().is_none()
        {
            pending.completed.set(true);
        } else if self.defer_notifications_until_durable
            && let Some(publication) = self
                .pending_resident_publication
                .as_ref()
                .and_then(|pending| pending.publication.get())
                .or(self.notification_publication)
            && !runtime
                .durable_notification_publications
                .remove(&publication)
        {
            runtime.completed_deferred_publications.insert(publication);
        }
        self.metrics.runtime_stats = if runtime.collect_tick_runtime_stats {
            runtime.stats()
        } else {
            runtime.cheap_stats()
        };
        Poll::Ready(Ok(()))
    }
}

impl<'a> EvaluationSession<'a> {
    fn hydration(
        runtime: &IvmRuntime,
        roots: VecDeque<NodeId>,
        storage: OwnedStorage<'a>,
    ) -> Result<Self, IvmRuntimeError> {
        let (relevant_nodes, mut work_queue) =
            EvaluationWorkQueue::new(roots.iter().copied()).discover_hydration(&runtime.graph)?;
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
            .collect::<HashMap<_, _>>();
        // Recursive hydration rebuilds internal arrangements from complete
        // source snapshots, so a leaf memo alone cannot satisfy those inputs.
        // Walk all recursive inputs together to keep classification linear in
        // the reachable graph slice even when it contains sibling recursion.
        let mut pending_recursive_inputs = VecDeque::new();
        for node in &relevant_nodes {
            let Some(graph_node) = runtime.graph.node(*node) else {
                continue;
            };
            if matches!(graph_node.descriptor.operator, OpType::Recursive(_)) {
                pending_recursive_inputs.extend(graph_node.descriptor.inputs.iter().copied());
            }
        }
        let mut recursive_inputs = HashSet::<NodeId>::default();
        while let Some(node) = pending_recursive_inputs.pop_front() {
            if !recursive_inputs.insert(node) {
                continue;
            }
            if let Some(graph_node) = runtime.graph.node(node) {
                pending_recursive_inputs.extend(graph_node.descriptor.inputs.iter().copied());
            }
        }
        let resident_source_nodes = node_meta
            .iter()
            .filter_map(|(node, meta)| {
                if recursive_inputs.contains(node) {
                    return None;
                }
                let signature = meta.input_signature.as_ref()?;
                let key = EvalMemoKey {
                    scope: ScopeId::root(),
                    node: *node,
                    input_signature_hash: signature.hash,
                    tick_epoch: None,
                    sub_tick: 0,
                    context_digest: 0,
                };
                eval_memo
                    .get(&key)
                    .is_some_and(|entry| entry.input_watermark == meta.input_generation)
                    .then_some(*node)
            })
            .collect::<HashSet<_>>();
        work_queue.storage_already_resident(&resident_source_nodes);
        let mut requests = EvaluationRequests::new();
        for request in work_queue.requests().cloned().collect::<Vec<_>>() {
            requests.request(
                request,
                &storage,
                Some(&runtime.chunk_provider),
                &runtime.schema,
            );
        }
        Ok(Self {
            relevant_nodes,
            roots: roots.into_iter().collect(),
            outputs: HashMap::default(),
            pending_outputs: HashMap::default(),
            operator_states,
            arrangement_states,
            arrangement_keys_by_input,
            eval_memo,
            eval_memo_bytes,
            memo_use_clock: runtime.memo_use_clock,
            node_meta,
            binding_frontiers: runtime.binding_frontiers.clone(),
            storage,
            requests,
            evaluation_inputs: EvaluationInputs::default(),
            work_queue,
        })
    }

    fn advance_binding_input(&mut self, graph: &IvmGraph, shape: &str) {
        *self.binding_frontiers.entry(shape.to_owned()).or_default() += 1;
        let affected = graph
            .affected_nodes(std::iter::empty(), std::iter::once(shape))
            .intersection(&self.relevant_nodes)
            .copied()
            .collect::<HashSet<_>>();
        for node in &affected {
            let meta = self.node_meta.entry(*node).or_default();
            meta.input_generation = meta.input_generation.wrapping_add(1);
        }
    }

    fn poll(
        &mut self,
        runtime: &IvmRuntime,
        binding_snapshots: &HashMap<String, RecordDeltas>,
        hydrate_arrangements: bool,
        metrics: &mut TickMetrics,
        cx: &mut Context<'_>,
    ) -> Poll<Result<(), IvmRuntimeError>> {
        let mut remaining_runnable_nodes = MAX_HYDRATION_RUNNABLE_NODES_PER_POLL;
        'owner_turn: loop {
            self.requests.poll(cx);
            let ready = match self.requests.drain_ready() {
                Ok(ready) => ready,
                Err(error) => return Poll::Ready(Err(error.1.error)),
            };
            self.work_queue.requests_ready(ready.keys().cloned());
            self.evaluation_inputs.install(ready);

            while let Some(node) = self.work_queue.runnable.pop_front() {
                if remaining_runnable_nodes == 0 {
                    self.work_queue.requeue_yielded(node);
                    // This is a resident CPU budget yield, not an external
                    // dependency. Arrange exactly one fresh owner turn.
                    cx.waker().wake_by_ref();
                    return Poll::Pending;
                }
                remaining_runnable_nodes -= 1;
                let context = if hydrate_arrangements {
                    EvalContext::root_subscription_snapshot()
                } else {
                    EvalContext::root_snapshot()
                };
                let result = if let Some(records) = self.pending_outputs.remove(&node) {
                    Ok(records)
                } else {
                    let mut evaluator = TickEvaluator {
                        schema: &runtime.schema,
                        graph: &runtime.graph,
                        variant_projections: &runtime.variant_projections,
                        table_deltas: &[],
                        binding_deltas: &[],
                        binding_snapshots,
                        current_tick: runtime.current_tick,
                        operator_states: &mut self.operator_states,
                        arrangement_states: &mut self.arrangement_states,
                        arrangement_keys_by_input: &mut self.arrangement_keys_by_input,
                        eval_memo: &mut self.eval_memo,
                        eval_memo_bytes: &mut self.eval_memo_bytes,
                        table_frontiers: &runtime.table_frontiers,
                        binding_frontiers: &self.binding_frontiers,
                        memo_use_clock: &mut self.memo_use_clock,
                        node_meta: &mut self.node_meta,
                        storage: Some(self.storage.as_ref()),
                        evaluation_inputs: Some(&mut self.evaluation_inputs),
                        context,
                        metrics,
                        terminal_deltas: HashMap::default(),
                        root_ordering_windows: HashMap::default(),
                    };
                    let mut evaluation = evaluator.update_node(node);
                    match Pin::new(&mut evaluation).poll(cx) {
                        Poll::Ready(result) => result.map(|records| records.as_ref().clone()),
                        Poll::Pending => {
                            self.work_queue.requeue_yielded(node);
                            cx.waker().wake_by_ref();
                            return Poll::Pending;
                        }
                    }
                };
                match result {
                    Ok(mut records) => {
                        if self.work_queue.is_root(node) {
                            let mut materialized = Vec::with_capacity(records.deltas.len());
                            let mut blocked = false;
                            for delta in &records.deltas {
                                match crate::large_values::materialize_record_attempt(
                                    &records.descriptor,
                                    delta.raw(),
                                    &mut self.evaluation_inputs,
                                ) {
                                    Ok(record) => materialized.push(RecordDelta {
                                        record: record.into(),
                                        weight: delta.weight,
                                    }),
                                    Err(IvmRuntimeError::EvaluationBlocked) => blocked = true,
                                    Err(error) => return Poll::Ready(Err(error)),
                                }
                            }
                            if blocked {
                                let requests = self.evaluation_inputs.take_missing();
                                if requests.is_empty() {
                                    return Poll::Ready(Err(IvmRuntimeError::EvaluationBlocked));
                                }
                                for request in requests.iter().cloned() {
                                    self.requests.request(
                                        request,
                                        &self.storage,
                                        Some(&runtime.chunk_provider),
                                        &runtime.schema,
                                    );
                                }
                                self.pending_outputs.insert(node, records);
                                self.work_queue.wait_for_requests(node, requests);
                                // Every newly retained future must be polled once
                                // before returning Pending so it can install the
                                // caller's waker.
                                if self.requests.poll(cx) > 0 {
                                    continue 'owner_turn;
                                }
                                if self.requests.has_pending() {
                                    // Cold storage owns the continuation now.
                                    // Do not spin through unrelated runnable
                                    // graph nodes in this owner turn.
                                    return Poll::Pending;
                                }
                                continue;
                            }
                            records.deltas = materialized;
                            self.outputs.insert(node, records);
                        }
                        self.work_queue.complete(node);
                    }
                    Err(IvmRuntimeError::EvaluationBlocked) => {
                        let requests = self.evaluation_inputs.take_missing();
                        if requests.is_empty() {
                            return Poll::Ready(Err(IvmRuntimeError::EvaluationBlocked));
                        }
                        let mut registered = false;
                        for request in requests.iter().cloned() {
                            registered |= self.requests.request(
                                request,
                                &self.storage,
                                Some(&runtime.chunk_provider),
                                &runtime.schema,
                            );
                        }
                        self.work_queue.wait_for_requests(node, requests);
                        if (registered || self.requests.has_pending()) && self.requests.poll(cx) > 0
                        {
                            continue 'owner_turn;
                        }
                        if self.requests.has_pending() {
                            // A request was polled with this owner's durable
                            // waker. Yield to it rather than scanning the rest of
                            // a potentially huge graph while it is cold.
                            return Poll::Pending;
                        }
                    }
                    Err(error) => return Poll::Ready(Err(error)),
                }
            }

            if self.outputs.len() == self.roots.len() {
                return Poll::Ready(Ok(()));
            }
            if self.requests.has_pending() {
                return Poll::Pending;
            }
            return Poll::Ready(Err(IvmRuntimeError::EvaluationBlocked));
        }
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
    pub(super) fn enqueue_subscription_hydration(
        &mut self,
        subscription_id: SubscriptionId,
        outputs: BTreeMap<String, CompiledNode>,
        storage: OwnedStorage<'static>,
        binding_snapshots: Option<HashMap<String, RecordDeltas>>,
        binding_frontier_advance: Option<&str>,
        initial: Arc<Mutex<Option<MultisinkDeltas>>>,
    ) -> Result<(), IvmRuntimeError> {
        let mut seen_roots = HashSet::new();
        let roots = outputs
            .values()
            .flat_map(|output| [output.root_ordering_node, Some(output.node)])
            .flatten()
            .filter(|root| seen_roots.insert(*root))
            .collect::<VecDeque<_>>();
        let hydrate_arrangements = roots.iter().copied().try_fold(false, |found, root| {
            Ok::<_, IvmRuntimeError>(found || self.output_depends_on_aggregate(root)?)
        })?;
        let mut session = EvaluationSession::hydration(self, roots, storage)?;
        if let Some(shape) = binding_frontier_advance {
            session.advance_binding_input(&self.graph, shape);
        }
        let temporal_blockers = {
            let pending = self.pending_incremental.0.borrow();
            pending
                .waiters_by_node
                .keys()
                .map(|node| (*node, 1))
                .collect::<HashMap<_, _>>()
        };
        session.work_queue.add_temporal_blockers(&temporal_blockers);
        let evaluation = PendingEvaluation::SubscriptionHydration(PendingSubscriptionHydration {
            subscription_id,
            outputs,
            initial,
            session,
            binding_snapshots: binding_snapshots.unwrap_or_else(|| self.binding_snapshot_deltas()),
            hydrate_arrangements,
            metrics: TickMetrics::default(),
        });
        let mut pending = self.pending_incremental.0.borrow_mut();
        let evaluation_id = pending.next_id;
        pending.next_id = pending.next_id.saturating_add(1);
        for node in evaluation.work_queue().incomplete_nodes() {
            pending
                .waiters_by_node
                .entry(node)
                .or_default()
                .push_back(evaluation_id);
        }
        pending.evaluations.insert(evaluation_id, evaluation);
        pending.order.push_back(evaluation_id);
        Ok(())
    }

    fn fail_evaluation_nodes(&mut self, failure: &EvaluationFailure) {
        self.operator_states
            .retain(|key, _| !failure.affected_nodes.contains(&key.node));
        self.eval_memo
            .retain(|key, _| !failure.affected_nodes.contains(&key.node));
        self.eval_memo_bytes = self
            .eval_memo
            .values()
            .map(|entry| entry.payload_bytes)
            .sum();
        for node in &failure.affected_nodes {
            if let Some(keys) = self.arrangement_keys_by_input.remove(node) {
                for key in keys {
                    self.arrangement_states.remove(&key);
                }
            }
            if let Some(meta) = self.node_meta.get_mut(node) {
                meta.input_signature = None;
                meta.input_generation = meta.input_generation.saturating_add(1);
            }
        }
        for subscription in self.multisink_subscriptions.values_mut() {
            if subscription.failed
                || !subscription.outputs.values().any(|output| {
                    failure.affected_nodes.contains(&output.node)
                        || output
                            .root_ordering_node
                            .is_some_and(|node| failure.affected_nodes.contains(&node))
                })
            {
                continue;
            }
            subscription.failed = true;
            subscription
                .sender
                .fail(SubscriptionError::new(Arc::clone(&failure.error)));
        }
    }

    fn fail_all_subscriptions(&mut self, error: Arc<IvmRuntimeError>) {
        for subscription in self.multisink_subscriptions.values_mut() {
            if subscription.failed {
                continue;
            }
            subscription.failed = true;
            subscription
                .sender
                .fail(SubscriptionError::new(Arc::clone(&error)));
        }
    }

    pub async fn tick<S>(
        &mut self,
        table_deltas: Vec<TableDelta>,
        storage: &S,
    ) -> Result<TickMetrics, IvmRuntimeError>
    where
        S: OrderedKvStorage,
    {
        self.tick_with_params(
            table_deltas,
            Vec::new(),
            OwnedStorage::new(Rc::new(storage)),
            None,
        )
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
            self.tick_with_params(
                Vec::new(),
                Vec::new(),
                OwnedStorage::new(Rc::new(storage)),
                None,
            )
            .await?;
        }
        Ok(())
    }

    pub(crate) async fn tick_resident_staged(
        &mut self,
        table_deltas: Vec<TableDelta>,
        storage: OwnedStorage<'static>,
        defer_notifications_until_durable: bool,
        publication_install: Option<(
            Rc<dyn crate::chunks::ChunkInstallObserver>,
            crate::chunks::PublicationInstallFailures,
        )>,
    ) -> Result<ResidentTick, IvmRuntimeError> {
        let publication = PendingResidentPublication {
            publication: Rc::new(Cell::new(None)),
            notifications: Rc::new(RefCell::new(Vec::new())),
            completed: Rc::new(Cell::new(false)),
            defer_notifications_until_durable,
            chunk_provider: publication_install.map(|(observer, failures)| {
                self.chunk_provider
                    .with_install_observer(observer, failures)
            }),
        };
        let temporal_blockers = {
            let pending = self.pending_incremental.0.borrow();
            pending
                .waiters_by_node
                .keys()
                .map(|node| (*node, 1))
                .collect()
        };
        let mut evaluation = self
            .begin_tick_with_params_and_notification_policy(
                table_deltas,
                Vec::new(),
                storage,
                None,
                defer_notifications_until_durable,
                Some(publication.clone()),
            )
            .await?;
        evaluation
            .work_queue
            .add_temporal_blockers(&temporal_blockers);
        let progress = std::future::poll_fn(|cx| {
            loop {
                let progress = evaluation.poll(self, cx);
                match progress {
                    Poll::Ready(Err(ref failure))
                        if failure.kind == EvaluationFailureKind::Scoped =>
                    {
                        self.fail_evaluation_nodes(failure);
                        evaluation.abandon(&failure.affected_nodes);
                    }
                    Poll::Pending if !evaluation.work_queue.runnable.is_empty() => {
                        // A resumable operator deliberately yielded while it
                        // remains runnable. Keep this applying future alive so
                        // its wake drives the next bounded turn. By contrast,
                        // an empty runnable queue is waiting on external
                        // requests and follows the existing detached path.
                        return Poll::Pending;
                    }
                    _ => return Poll::Ready(progress),
                }
            }
        })
        .await;
        let metrics = evaluation.metrics.clone();
        match progress {
            Poll::Ready(Ok(())) => {}
            Poll::Ready(Err(failure)) => return Err(failure.into_error()),
            Poll::Pending => {
                evaluation.work_queue.drain_completed_events();
                let mut pending = self.pending_incremental.0.borrow_mut();
                let evaluation_id = pending.next_id;
                pending.next_id = pending.next_id.saturating_add(1);
                for node in evaluation.work_queue.incomplete_nodes() {
                    pending
                        .waiters_by_node
                        .entry(node)
                        .or_default()
                        .push_back(evaluation_id);
                }
                pending
                    .evaluations
                    .insert(evaluation_id, PendingEvaluation::Incremental(evaluation));
                pending.order.push_back(evaluation_id);
            }
        };
        Ok(ResidentTick {
            metrics,
            publication,
        })
    }

    pub(crate) fn assign_resident_publication(
        &mut self,
        tick: ResidentTick,
        publication: PublicationId,
    ) -> TickMetrics {
        assert_eq!(
            tick.publication.publication.replace(Some(publication)),
            None
        );
        let mut notifications = std::mem::take(&mut *tick.publication.notifications.borrow_mut());
        for (_, queued) in &mut notifications {
            queued.publication = Some(publication);
        }
        if tick.publication.defer_notifications_until_durable {
            self.deferred_notifications
                .entry(publication)
                .or_default()
                .extend(notifications);
            if tick.publication.completed.get() {
                self.completed_deferred_publications.insert(publication);
            }
        } else {
            self.send_deferred_notifications(notifications);
        }
        tick.metrics
    }

    pub(crate) fn settle_deferred_notifications(&mut self, publication: PublicationId) {
        if self.completed_deferred_publications.remove(&publication) {
            if let Some(notifications) = self.deferred_notifications.remove(&publication) {
                self.send_deferred_notifications(notifications);
            }
            return;
        }
        self.durable_notification_publications.insert(publication);
        let Some(notifications) = self.deferred_notifications.remove(&publication) else {
            return;
        };
        self.send_deferred_notifications(notifications);
    }

    fn send_deferred_notifications(
        &mut self,
        notifications: Vec<(SubscriptionId, QueuedMultisinkDeltas)>,
    ) {
        let mut dropped = Vec::new();
        for (subscription_id, queued) in notifications {
            if self
                .multisink_subscriptions
                .get(&subscription_id)
                .is_some_and(|subscription| subscription.sender.send(queued).is_err())
            {
                dropped.push(subscription_id);
            }
        }
        for subscription_id in dropped {
            self.unsubscribe(subscription_id);
        }
    }

    pub(crate) fn discard_deferred_notifications(&mut self, publication: PublicationId) {
        self.deferred_notifications.remove(&publication);
        self.completed_deferred_publications.remove(&publication);
        self.durable_notification_publications.remove(&publication);
    }

    pub(crate) fn poll_pending_incremental(
        &mut self,
        cx: &mut Context<'_>,
    ) -> Poll<Result<(), IvmRuntimeError>> {
        let slot = Rc::clone(&self.pending_incremental.0);
        let mut state = std::mem::take(&mut *slot.borrow_mut());
        if state.order.is_empty() {
            state.pending_progress = None;
            return Poll::Ready(Ok(()));
        }
        state.pending_progress = None;
        let mut retained_order = VecDeque::new();
        while let Some(evaluation_id) = state.order.pop_front() {
            let mut evaluation = state
                .evaluations
                .remove(&evaluation_id)
                .expect("pending evaluation order references a live session");
            let progress = match &mut evaluation {
                PendingEvaluation::Incremental(incremental) => incremental.poll(self, cx),
                PendingEvaluation::SubscriptionHydration(hydration) => hydration
                    .session
                    .poll(
                        self,
                        &hydration.binding_snapshots,
                        hydration.hydrate_arrangements,
                        &mut hydration.metrics,
                        cx,
                    )
                    .map_err(|error| EvaluationFailure {
                        kind: EvaluationFailureKind::Scoped,
                        affected_nodes: hydration.session.work_queue.incomplete_nodes().collect(),
                        error: Arc::new(error),
                    }),
            };
            let completed = evaluation.work_queue_mut().drain_completed_events();
            for node in &completed {
                let Some(waiters) = state.waiters_by_node.get_mut(node) else {
                    continue;
                };
                debug_assert_eq!(waiters.front(), Some(&evaluation_id));
                waiters.pop_front();
                let successor = waiters.front().copied();
                if waiters.is_empty() {
                    state.waiters_by_node.remove(node);
                }
                if let Some(successor) = successor
                    && let Some(later) = state.evaluations.get_mut(&successor)
                {
                    later.work_queue_mut().temporal_ready(*node);
                }
            }
            match progress {
                Poll::Ready(Ok(())) => {
                    if let PendingEvaluation::SubscriptionHydration(hydration) = evaluation {
                        let snapshot = subscription_snapshot_from_hydrated(
                            &hydration.outputs,
                            &hydration.session.outputs,
                        );
                        hydration.session.install(self);
                        self.record_hydration_memo_metrics(&hydration.metrics);
                        self.evict_eval_memo();
                        match snapshot {
                            Ok(snapshot) => {
                                *hydration
                                    .initial
                                    .lock()
                                    .expect("subscription initial snapshot mutex poisoned") =
                                    Some(snapshot);
                            }
                            Err(error) => {
                                if let Some(subscription) =
                                    self.multisink_subscriptions.get(&hydration.subscription_id)
                                {
                                    subscription
                                        .sender
                                        .fail(SubscriptionError::new(Arc::new(error)));
                                }
                                self.unsubscribe(hydration.subscription_id);
                            }
                        }
                    }
                }
                Poll::Ready(Err(failure)) => {
                    if let PendingEvaluation::SubscriptionHydration(hydration) = &evaluation {
                        if let Some(subscription) =
                            self.multisink_subscriptions.get(&hydration.subscription_id)
                        {
                            subscription
                                .sender
                                .fail(SubscriptionError::new(Arc::clone(&failure.error)));
                        }
                        self.unsubscribe(hydration.subscription_id);
                    }
                    if failure.kind == EvaluationFailureKind::Fatal {
                        let IvmRuntimeError::Chunk(error) = failure.error.as_ref() else {
                            unreachable!("trusted install failures originate from chunk requests")
                        };
                        self.fail_all_subscriptions(Arc::new(IvmRuntimeError::Chunk(
                            error.clone(),
                        )));
                        state.order = retained_order;
                        *slot.borrow_mut() = state;
                        return Poll::Ready(Err(failure.into_error()));
                    }
                    self.fail_evaluation_nodes(&failure);
                    let incomplete = evaluation
                        .work_queue()
                        .incomplete_nodes()
                        .collect::<Vec<_>>();
                    for node in incomplete {
                        let Some(waiters) = state.waiters_by_node.get_mut(&node) else {
                            continue;
                        };
                        waiters.retain(|waiter| *waiter != evaluation_id);
                        let successor = waiters.front().copied();
                        if waiters.is_empty() {
                            state.waiters_by_node.remove(&node);
                        }
                        if let Some(successor) = successor
                            && let Some(later) = state.evaluations.get_mut(&successor)
                        {
                            later.work_queue_mut().temporal_ready(node);
                        }
                    }
                }
                Poll::Pending => {
                    let pending_progress = if evaluation.work_queue().has_resident_continuation() {
                        PendingProgress::ResidentContinuation
                    } else {
                        PendingProgress::Storage
                    };
                    state.evaluations.insert(evaluation_id, evaluation);
                    retained_order.push_back(evaluation_id);
                    // One owner turn advances at most one suspended
                    // evaluation. In particular, a cold subscription
                    // hydration must hand control back to the runtime owner
                    // before it can drain unrelated queued work (such as
                    // transport ingress and local write fates). The pending
                    // storage future has just received this poll's durable
                    // waker; cooperative in-memory yields wake it directly.
                    retained_order.append(&mut state.order);
                    state.order = retained_order;
                    state.pending_progress = Some(pending_progress);
                    *slot.borrow_mut() = state;
                    return Poll::Pending;
                }
            }
        }
        let done = retained_order.is_empty();
        state.order = retained_order;
        state.pending_progress = None;
        *slot.borrow_mut() = state;
        if done {
            Poll::Ready(Ok(()))
        } else {
            Poll::Pending
        }
    }

    pub(crate) fn has_pending_incremental(&self) -> bool {
        self.pending_incremental.is_pending()
    }

    /// Whether the last suspended owner turn retained a cooperative CPU
    /// continuation rather than a cold storage request.
    pub(crate) fn has_resident_continuation(&self) -> bool {
        self.pending_incremental.has_resident_continuation()
    }

    /// Drop an uninstalled hydration when its subscription is cancelled.
    ///
    /// A hydration owns cold storage futures and may be ahead of later
    /// evaluations in the per-node temporal order. Removing only the public
    /// subscription used to leave that private session parked until its
    /// storage happened to resume. Besides retaining the request, that made
    /// `drive_progress` wait for work nobody could observe. Release any
    /// successor waiting behind the cancelled session exactly as a completed
    /// predecessor would, but do not install the cancelled session's state.
    pub(super) fn cancel_pending_subscription_hydration(
        &mut self,
        subscription_id: SubscriptionId,
    ) {
        let slot = Rc::clone(&self.pending_incremental.0);
        let mut state = std::mem::take(&mut *slot.borrow_mut());
        let cancelled = state
            .evaluations
            .iter()
            .filter_map(|(evaluation_id, evaluation)| match evaluation {
                PendingEvaluation::SubscriptionHydration(hydration)
                    if hydration.subscription_id == subscription_id =>
                {
                    Some(*evaluation_id)
                }
                PendingEvaluation::Incremental(_) | PendingEvaluation::SubscriptionHydration(_) => {
                    None
                }
            })
            .collect::<Vec<_>>();

        for evaluation_id in cancelled {
            let Some(evaluation) = state.evaluations.remove(&evaluation_id) else {
                continue;
            };
            state.order.retain(|candidate| *candidate != evaluation_id);
            for node in evaluation.work_queue().incomplete_nodes() {
                let Some(waiters) = state.waiters_by_node.get_mut(&node) else {
                    continue;
                };
                let was_front = waiters.front() == Some(&evaluation_id);
                waiters.retain(|waiter| *waiter != evaluation_id);
                let successor = waiters.front().copied();
                if waiters.is_empty() {
                    state.waiters_by_node.remove(&node);
                }
                if was_front
                    && let Some(successor) = successor
                    && let Some(later) = state.evaluations.get_mut(&successor)
                {
                    later.work_queue_mut().temporal_ready(node);
                }
            }
        }
        *slot.borrow_mut() = state;
    }

    pub(crate) async fn drive_pending_incremental(&mut self) -> Result<(), IvmRuntimeError> {
        std::future::poll_fn(|cx| self.poll_pending_incremental(cx)).await
    }

    pub(super) async fn tick_with_params<'a>(
        &mut self,
        table_deltas: Vec<TableDelta>,
        binding_deltas: Vec<BindingDelta>,
        storage: OwnedStorage<'a>,
        notification_publication: Option<PublicationId>,
    ) -> Result<TickMetrics, IvmRuntimeError> {
        self.drive_pending_incremental().await?;
        let mut evaluation = self
            .begin_tick_with_params(
                table_deltas,
                binding_deltas,
                storage,
                notification_publication,
            )
            .await?;
        std::future::poll_fn(|cx| evaluation.poll(self, cx))
            .await
            .map_err(EvaluationFailure::into_error)?;
        Ok(evaluation.metrics)
    }

    async fn begin_tick_with_params<'a>(
        &mut self,
        table_deltas: Vec<TableDelta>,
        binding_deltas: Vec<BindingDelta>,
        storage: OwnedStorage<'a>,
        notification_publication: Option<PublicationId>,
    ) -> Result<IncrementalEvaluation<'a>, IvmRuntimeError> {
        self.begin_tick_with_params_and_notification_policy(
            table_deltas,
            binding_deltas,
            storage,
            notification_publication,
            false,
            None,
        )
        .await
    }

    async fn begin_tick_with_params_and_notification_policy<'a>(
        &mut self,
        table_deltas: Vec<TableDelta>,
        mut binding_deltas: Vec<BindingDelta>,
        storage: OwnedStorage<'a>,
        notification_publication: Option<PublicationId>,
        defer_notifications_until_durable: bool,
        pending_resident_publication: Option<PendingResidentPublication>,
    ) -> Result<IncrementalEvaluation<'a>, IvmRuntimeError> {
        let pending_binding_retractions = self.pending_binding_retractions.len();
        if pending_binding_retractions != 0 {
            let mut pending = self.pending_binding_retractions.clone();
            pending.extend(binding_deltas);
            binding_deltas = pending;
        }
        let changed_tables = table_deltas
            .iter()
            .map(|delta| delta.table.as_str())
            .collect::<HashSet<_>>();
        let changed_bindings = binding_deltas
            .iter()
            .map(|delta| delta.shape.as_str())
            .collect::<HashSet<_>>();
        let affected_nodes = self.graph.affected_nodes(
            changed_tables.iter().copied(),
            changed_bindings.iter().copied(),
        );
        // Do not commit tick lifecycle state until fallible durable evaluation
        // has completed. In particular, queued binding retractions must remain
        // retryable when preparing a tick encounters a storage error.
        let current_tick = self.current_tick + 1;
        let table_delta_records = table_deltas
            .iter()
            .map(|delta| delta.deltas.len())
            .sum::<usize>();
        self.tick_durable_nodes(
            &table_deltas,
            &affected_nodes,
            current_tick,
            storage.as_ref(),
        )
        .await?;
        self.current_tick = current_tick;
        self.bump_input_frontiers(&table_deltas, &binding_deltas);
        self.pending_binding_retractions
            .drain(..pending_binding_retractions);
        let metrics = TickMetrics {
            tick: current_tick,
            table_delta_records,
            ..TickMetrics::default()
        };
        let binding_snapshots = self.binding_snapshot_deltas();
        let affected_subscriptions = affected_nodes
            .iter()
            .filter_map(|node| self.subscriptions_by_output_node.get(node))
            .flatten()
            .copied()
            .collect::<HashSet<_>>();
        let mut retained_roots = affected_nodes
            .iter()
            .filter(|node| {
                self.node_meta
                    .get(node)
                    .is_some_and(|meta| !meta.retainers.is_empty())
                    && self
                        .graph
                        .node(**node)
                        .is_some_and(|node| !node.is_durable())
            })
            .copied()
            .collect::<Vec<_>>();
        retained_roots.sort_unstable();
        let mut active_roots = affected_subscriptions
            .iter()
            .filter_map(|subscription| self.multisink_subscriptions.get(subscription))
            .flat_map(|subscription| {
                subscription
                    .outputs
                    .values()
                    .filter(|output| affected_nodes.contains(&output.node))
                    .flat_map(|output| [Some(output.node), output.root_ordering_node])
                    .flatten()
            })
            .collect::<Vec<_>>();
        active_roots.sort_unstable();
        active_roots.dedup();
        active_roots.extend(retained_roots.iter().copied());
        active_roots.sort_unstable();
        active_roots.dedup();
        let requests = EvaluationRequests::new();
        let evaluation_inputs = Some(EvaluationInputs::default());
        let (_, work_queue) =
            EvaluationWorkQueue::new(active_roots).discover_incremental(&self.graph)?;
        Ok(IncrementalEvaluation {
            table_deltas,
            binding_deltas,
            binding_snapshots,
            table_frontiers: self.table_frontiers.clone(),
            binding_frontiers: self.binding_frontiers.clone(),
            current_tick,
            metrics,
            storage,
            requests,
            evaluation_inputs,
            work_queue,
            published_subscriptions: HashSet::default(),
            affected_nodes,
            affected_subscriptions,
            pending_subscription_outputs: HashMap::default(),
            terminal_deltas: HashMap::default(),
            root_ordering_windows: HashMap::default(),
            notification_publication,
            defer_notifications_until_durable,
            pending_resident_publication,
        })
    }

    pub(super) fn bump_input_frontiers(
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
        for node in self.graph.affected_nodes(
            changed_tables.iter().copied(),
            changed_bindings.iter().copied(),
        ) {
            let meta = self.node_meta.entry(node).or_default();
            meta.input_generation = meta.input_generation.wrapping_add(1);
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
        self.hydration_roots_owned(roots, OwnedStorage::new(Rc::new(storage)), mode, None, None)
            .await
    }

    async fn hydration_roots_owned<'a>(
        &mut self,
        roots: impl IntoIterator<Item = NodeId>,
        owned_storage: OwnedStorage<'a>,
        mode: HydrationMode,
        binding_snapshots: Option<HashMap<String, RecordDeltas>>,
        binding_frontier_advance: Option<&str>,
    ) -> Result<HashMap<NodeId, RecordDeltas>, IvmRuntimeError> {
        let roots = roots.into_iter().collect::<VecDeque<_>>();
        let binding_snapshots = binding_snapshots.unwrap_or_else(|| self.binding_snapshot_deltas());
        let mut metrics = TickMetrics::default();
        if roots.is_empty() {
            return Err(IvmRuntimeError::UnsupportedOperator);
        }
        let hydrate_arrangements = mode == HydrationMode::Subscription
            && roots.iter().copied().try_fold(false, |found, root| {
                Ok::<_, IvmRuntimeError>(found || self.output_depends_on_aggregate(root)?)
            })?;
        let mut session = EvaluationSession::hydration(self, roots, owned_storage)?;
        if let Some(shape) = binding_frontier_advance {
            session.advance_binding_input(&self.graph, shape);
        }
        std::future::poll_fn(|cx| {
            session.poll(
                self,
                &binding_snapshots,
                hydrate_arrangements,
                &mut metrics,
                cx,
            )
        })
        .await?;
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
        self.hydration_snapshots_with_binding_snapshots(outputs, storage, mode, None, None)
            .await
    }

    async fn hydration_snapshots_with_binding_snapshots<S>(
        &mut self,
        outputs: &BTreeMap<String, CompiledNode>,
        storage: &S,
        mode: HydrationMode,
        binding_snapshots: Option<HashMap<String, RecordDeltas>>,
        binding_frontier_advance: Option<&str>,
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
        let hydrated = self
            .hydration_roots_owned(
                roots,
                OwnedStorage::new(Rc::new(storage)),
                mode,
                binding_snapshots,
                binding_frontier_advance,
            )
            .await?;
        subscription_snapshot_from_hydrated(outputs, &hydrated)
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

    async fn tick_durable_nodes(
        &mut self,
        table_deltas: &[TableDelta],
        affected_nodes: &std::collections::HashSet<NodeId>,
        current_tick: u64,
        storage: &dyn OrderedKvStorage,
    ) -> Result<(), IvmRuntimeError> {
        let durable_nodes = affected_nodes
            .iter()
            .copied()
            .filter(|node| {
                self.graph
                    .node(*node)
                    .is_some_and(|graph_node| graph_node.is_durable())
            })
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

fn subscription_snapshot_from_hydrated(
    outputs: &BTreeMap<String, CompiledNode>,
    hydrated: &HashMap<NodeId, RecordDeltas>,
) -> Result<MultisinkDeltas, IvmRuntimeError> {
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
