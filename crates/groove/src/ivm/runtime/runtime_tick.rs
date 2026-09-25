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
use crate::storage::{OwnedStorage, StagedWriteOverlay, StagedWriteState, WriteManyOutcome};

/// Hydration can discover a large resident graph in one poll. Keep every
/// owner turn bounded so a browser worker returns to transport ingress between
/// CPU-only graph slices. Cold storage takes the separate request-pending path
/// below; its waker is intentionally not used to classify the pending reason.
const MAX_HYDRATION_RUNNABLE_NODES_PER_POLL: usize = 32;

type PersistFlush<'a> = Pin<Box<dyn Future<Output = Result<(), IvmRuntimeError>> + 'a>>;

/// A started direct flush has no rollback path. If its owner drops it before
/// receiving a definite outcome, retain that fact on the runtime so a later
/// owner cannot continue from the old evaluator state.
struct PersistFlushAttempt {
    indeterminate: Rc<Cell<bool>>,
    completed: bool,
}

impl Drop for PersistFlushAttempt {
    fn drop(&mut self) {
        if !self.completed {
            self.indeterminate.set(true);
        }
    }
}

/// Owned preparation state for one interruptible evaluation.
///
/// Storage suspension and dependency ordering live in `EvaluationWorkQueue`.
/// Every root, including retained roots without an active subscription, is
/// evaluated only after its inputs have completed.
struct EvaluationSession<'a> {
    relevant_nodes: HashSet<NodeId>,
    roots: HashSet<NodeId>,
    outputs: HashMap<NodeId, RecordDeltas>,
    pending_outputs: HashMap<NodeId, Arc<RecordDeltas>>,
    operator_states: HashMap<OperatorStateKey, OperatorState>,
    arrangement_states: HashMap<ArrangementKey, AsOf<ArrangementState, SubTick>>,
    arrangement_keys_by_input: HashMap<NodeId, HashSet<ArrangementKey>>,
    eval_memo: EvaluationMemo,
    eval_memo_bytes: usize,
    memo_use_clock: u64,
    node_meta: HashMap<NodeId, NodeRuntimeMeta>,
    /// Collector operations produced while hydrating the exact initial
    /// snapshot. These are the authoritative terminal-tree seed for a new
    /// subscription, not an incremental side channel.
    terminal_deltas: HashMap<NodeId, TerminalDeltas>,
    binding_frontiers: HashMap<BindingSourceKey, u64>,
    storage: OwnedStorage<'a>,
    requests: EvaluationRequests<'a>,
    evaluation_inputs: EvaluationInputs,
    work_queue: EvaluationWorkQueue,
    /// How root outputs present indirect values to the caller.
    root_indirect_values: RootIndirectValues,
}

pub(super) struct IncrementalEvaluation<'a> {
    table_deltas: Vec<TableDelta>,
    binding_deltas: Vec<BindingDelta>,
    binding_snapshots: Arc<BindingSnapshots>,
    table_frontiers: HashMap<String, u64>,
    binding_frontiers: HashMap<BindingSourceKey, u64>,
    current_tick: u64,
    metrics: TickMetrics,
    storage: OwnedStorage<'a>,
    requests: EvaluationRequests<'a>,
    evaluation_inputs: Option<EvaluationInputs>,
    work_queue: EvaluationWorkQueue,
    /// Graph slice whose state is staged by this evaluation. Unrelated
    /// runtime state remains live and is merged only when the tick commits.
    relevant_nodes: Arc<HashSet<NodeId>>,
    published_subscriptions: HashSet<SubscriptionId>,
    affected_subscriptions: HashSet<SubscriptionId>,
    affected_nodes: Arc<HashSet<NodeId>>,
    /// Relational output retained while logical terminal materialization waits
    /// for immutable chunks. Re-evaluating after operator state advances can
    /// correctly yield an empty delta, so publication owns this exact value.
    /// Each entry is the physical output and, once loaded, its materialized
    /// form. TopBy root keys are taken from the physical form (#3309).
    pending_subscription_outputs: HashMap<NodeId, (Arc<RecordDeltas>, Option<Arc<RecordDeltas>>)>,
    terminal_deltas: HashMap<NodeId, TerminalDeltas>,
    root_ordering_windows: HashMap<NodeId, RootOrderingWindows>,
    notification_publication: Option<PublicationId>,
    defer_notifications_until_durable: bool,
    pending_resident_publication: Option<PendingResidentPublication>,
    /// All evaluator-owned state is prepared here and installed only after the
    /// complete direct tick reaches Ready. This keeps failed ticks atomic while
    /// allowing a suspended evaluation to retain its exact continuation.
    operator_states: HashMap<OperatorStateKey, OperatorState>,
    arrangement_states: HashMap<ArrangementKey, AsOf<ArrangementState, SubTick>>,
    arrangement_keys_by_input: HashMap<NodeId, HashSet<ArrangementKey>>,
    eval_memo: EvaluationMemo,
    eval_memo_bytes: usize,
    memo_use_clock: u64,
    node_meta: HashMap<NodeId, NodeRuntimeMeta>,
    pending_binding_retractions: usize,
    pending_notifications: Vec<(SubscriptionId, QueuedMultisinkDeltas)>,
    /// Derived writes use the same sparse overlay as their evaluation reads.
    /// The owned flush future is retained across resident owner turns.
    durable_writes: Rc<RefCell<StagedWriteState>>,
    persist_flush: Option<PersistFlush<'a>>,
    /// No independent root remains after a scoped failure, so this tick must
    /// not publish its staged globals.
    discarded: bool,
    /// Affected shared terminals whose route barriers await this tick's
    /// deltas (#3288). Taken once the first frame completes.
    routed_terminals: Vec<NodeId>,
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
    /// Completed durable-node writes belong to this exact publication, even
    /// while query-only work remains queued. Sharing the sparse buffer lets
    /// the facade stage it without searching unrelated pending evaluations.
    durable_writes: Rc<RefCell<StagedWriteState>>,
}

impl ResidentTick {
    pub(crate) fn take_durable_writes(&self) -> Vec<crate::storage::OwnedWriteOperation> {
        std::mem::take(&mut *self.durable_writes.borrow_mut()).into_operations()
    }
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
}

impl PendingIncrementalState {
    /// Select each hydration whose snapshot overlaps `nodes`, plus every
    /// evaluation which is temporally ahead of it on any registered node.
    /// Repeat to closure because one predecessor may itself wait behind an
    /// evaluation on a different node.
    fn hydration_admission_evaluations(&self, nodes: &HashSet<NodeId>) -> HashSet<u64> {
        let mut selected = self
            .evaluations
            .iter()
            .filter_map(|(evaluation_id, evaluation)| {
                (matches!(evaluation, PendingEvaluation::SubscriptionHydration(_))
                    && evaluation.work_queue().overlaps(nodes))
                .then_some(*evaluation_id)
            })
            .collect::<HashSet<_>>();
        loop {
            let mut changed = false;
            for waiters in self.waiters_by_node.values() {
                let Some(last_selected) = waiters
                    .iter()
                    .rposition(|evaluation_id| selected.contains(evaluation_id))
                else {
                    continue;
                };
                for predecessor in waiters.iter().take(last_selected + 1) {
                    changed |= selected.insert(*predecessor);
                }
            }
            if !changed {
                return selected;
            }
        }
    }

    /// Release evaluations queued behind `evaluation_id` at completed graph
    /// nodes. Snapshot hydration calls this only after its isolated state is
    /// installed; incremental evaluations can release nodes as they complete.
    fn release_temporal_successors(
        &mut self,
        evaluation_id: u64,
        nodes: impl IntoIterator<Item = NodeId>,
    ) {
        for node in nodes {
            let Some(waiters) = self.waiters_by_node.get_mut(&node) else {
                continue;
            };
            debug_assert_eq!(waiters.front(), Some(&evaluation_id));
            waiters.pop_front();
            let successor = waiters.front().copied();
            if waiters.is_empty() {
                self.waiters_by_node.remove(&node);
            }
            if let Some(successor) = successor
                && let Some(later) = self.evaluations.get_mut(&successor)
            {
                later.work_queue_mut().temporal_ready(node);
            }
        }
    }
}

#[allow(clippy::large_enum_variant)]
enum PendingEvaluation {
    Incremental(IncrementalEvaluation<'static>),
    SubscriptionHydration(PendingSubscriptionHydration),
}

struct PendingSubscriptionHydration {
    subscription_id: SubscriptionId,
    outputs: BTreeMap<String, CompiledNode>,
    initial: Arc<Mutex<Option<MultisinkDeltas>>>,
    session: EvaluationSession<'static>,
    binding_snapshots: Arc<BindingSnapshots>,
    hydrate_arrangements: bool,
    lifetime: SubscriptionLifetime,
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

    fn has_resident_continuation(&self) -> bool {
        self.work_queue().has_resident_continuation()
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
        self.0
            .borrow()
            .evaluations
            .values()
            .any(PendingEvaluation::has_resident_continuation)
    }

    /// Nodes referenced by queued continuations remain graph-live until the
    /// continuation completes or is cancelled. This is queried only while the
    /// queue is installed in its shared slot; an active poll defers GC until
    /// it restores that slot.
    pub(super) fn registered_nodes(&self) -> HashSet<NodeId> {
        self.0
            .borrow()
            .evaluations
            .values()
            .flat_map(|evaluation| evaluation.work_queue().registered_nodes())
            .collect()
    }
}

/// Discovers request-producing leaves for all reachable siblings without recursively
/// evaluating through the first blocked branch. Hash-consed nodes enter the
/// queue once, so discovery is linear in the reachable graph slice.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum EvaluationEntry {
    Waiting(usize),
    Fused,
    Runnable,
    Complete,
}

struct EvaluationWorkQueue {
    unary_batches: HashMap<NodeId, evaluator::PendingUnaryBatch>,
    pipelines: HashMap<NodeId, Arc<[NodeId]>>,
    pipeline_batches: HashMap<NodeId, pipeline::PendingPipeline>,
    task_slots: Vec<usize>,
    layout: Arc<crate::ivm::execution_layout::ExecutionLayout>,
    entries: Vec<EvaluationEntry>,
    request_dependents: std::collections::BTreeMap<EvaluationRequestKey, Vec<NodeId>>,
    runnable: VecDeque<NodeId>,
    completed_events: Vec<NodeId>,
    temporal_waiting: Vec<usize>,
    /// Nodes whose completion an earlier frame of this evaluation already
    /// published (#3306). Temporal successors are released at most once per
    /// evaluation, so re-completing one of these emits no second event.
    released_by_earlier_frame: HashSet<NodeId>,
}

impl EvaluationWorkQueue {
    fn discover(
        graph: &IvmGraph,
        node_meta: &HashMap<NodeId, NodeRuntimeMeta>,
        roots: impl IntoIterator<Item = NodeId>,
        hydrate_sources: bool,
    ) -> Result<(HashSet<NodeId>, Self), IvmRuntimeError> {
        let queue = Self::discover_frame(graph, node_meta, roots, hydrate_sources)?;
        Ok((queue.layout.nodes.iter().copied().collect(), queue))
    }

    fn discover_frame(
        graph: &IvmGraph,
        node_meta: &HashMap<NodeId, NodeRuntimeMeta>,
        roots: impl IntoIterator<Item = NodeId>,
        hydrate_sources: bool,
    ) -> Result<Self, IvmRuntimeError> {
        let layout = graph
            .execution_layout(roots)
            .map_err(IvmRuntimeError::GraphNodeNotFound)?;
        let mut queue = Self {
            unary_batches: HashMap::default(),
            pipelines: HashMap::default(),
            pipeline_batches: HashMap::default(),
            task_slots: (0..layout.nodes.len()).collect(),
            entries: layout
                .input_counts
                .iter()
                .copied()
                .map(EvaluationEntry::Waiting)
                .collect(),
            temporal_waiting: vec![0; layout.nodes.len()],
            layout,
            request_dependents: std::collections::BTreeMap::new(),
            runnable: VecDeque::new(),
            completed_events: Vec::new(),
            released_by_earlier_frame: HashSet::default(),
        };
        // Contract physical tasks, not graph identity. A globally shared or
        // explicitly retained intermediate remains independently executable.
        let predecessors = queue
            .layout
            .pipeline_predecessors
            .iter()
            .map(|predecessor| {
                predecessor.filter(|slot| {
                    let id = queue.layout.nodes[*slot];
                    let node = graph.node(id).expect("layout node");
                    !node.is_durable()
                        && node.children.len() == 1
                        && node_meta
                            .get(&id)
                            .is_none_or(|meta| meta.retainers.is_empty())
                })
            })
            .collect::<Vec<_>>();
        let mut interior = vec![false; queue.layout.nodes.len()];
        for slot in predecessors.iter().flatten() {
            interior[*slot] = true;
        }
        for tail in 0..queue.layout.nodes.len() {
            if interior[tail] || predecessors[tail].is_none() {
                continue;
            }
            let mut members = vec![queue.layout.nodes[tail]];
            let mut cursor = tail;
            while let Some(previous) = predecessors[cursor] {
                queue.task_slots[previous] = tail;
                queue.entries[previous] = EvaluationEntry::Fused;
                members.push(queue.layout.nodes[previous]);
                cursor = previous;
            }
            members.reverse();
            queue
                .pipelines
                .insert(queue.layout.nodes[tail], members.into());
        }
        if hydrate_sources {
            for &slot in &queue.layout.source_slots {
                let node_id = queue.layout.nodes[slot];
                let node = graph
                    .node(node_id)
                    .ok_or(IvmRuntimeError::GraphNodeNotFound(node_id))?;
                let request = match &node.descriptor.operator {
                    OpType::TableSource(source) => NodeState::table_source_request(source)?,
                    OpType::IndexSource(source) => NodeState::index_source_request(source)?,
                    _ => unreachable!("layout source slot is not a storage source"),
                };
                if let Some(request) = request {
                    queue
                        .request_dependents
                        .entry(EvaluationRequestKey::Storage(request))
                        .or_default()
                        .push(node_id);
                    queue.entries[slot] = EvaluationEntry::Waiting(1);
                }
            }
        }
        for slot in 0..queue.entries.len() {
            if queue.entries[slot] == EvaluationEntry::Waiting(0) {
                queue.make_slot_runnable(slot);
            }
        }
        Ok(queue)
    }

    fn requests(&self) -> impl Iterator<Item = &EvaluationRequestKey> {
        self.request_dependents.keys()
    }

    /// Dispatch with direct compiled input slots. Registers belong to this
    /// private frame, never the graph cache. Producer state remains a live
    /// requirement: missing/stale registers use the normal scoped resolver.
    fn poll_node(
        &mut self,
        evaluator: &mut TickEvaluator<'_>,
        node: NodeId,
        cx: &mut Context<'_>,
    ) -> Poll<Result<Arc<RecordDeltas>, IvmRuntimeError>> {
        let slot = self.layout.slots[&node];
        if let Some(pipeline) = self.pipelines.get(&node) {
            let head = self.layout.slots[&pipeline[0]];
            evaluator.poll_pipeline(
                pipeline,
                &mut self.pipeline_batches,
                evaluator::FrameInputs {
                    slots: self.layout.inputs(head),
                },
                cx,
            )
        } else {
            evaluator.poll_ready_node(
                node,
                &mut self.unary_batches,
                evaluator::FrameInputs {
                    slots: self.layout.inputs(slot),
                },
                cx,
            )
        }
    }

    fn dependency_ready(&mut self, node: NodeId) {
        if let Some(&slot) = self.layout.slots.get(&node) {
            self.slot_dependency_ready(slot);
        }
    }

    fn slot_dependency_ready(&mut self, slot: usize) {
        let slot = self.task_slots[slot];
        let EvaluationEntry::Waiting(remaining) = &mut self.entries[slot] else {
            return;
        };
        *remaining = remaining.saturating_sub(1);
        if *remaining == 0 {
            self.make_slot_runnable(slot);
        }
    }

    fn requests_ready(&mut self, requests: impl IntoIterator<Item = EvaluationRequestKey>) {
        for request in requests {
            if let Some(nodes) = self.request_dependents.remove(&request) {
                for node in nodes {
                    self.dependency_ready(node);
                }
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
            self.dependency_ready(node);
        }
    }

    fn wait_for_requests(
        &mut self,
        node: NodeId,
        requests: impl IntoIterator<Item = EvaluationRequestKey>,
    ) {
        let requests = requests.into_iter().collect::<Vec<_>>();
        self.entries[self.layout.slots[&node]] = EvaluationEntry::Waiting(requests.len());
        for request in requests {
            let dependents = self.request_dependents.entry(request).or_default();
            if !dependents.contains(&node) {
                dependents.push(node);
            }
        }
    }

    fn make_slot_runnable(&mut self, slot: usize) {
        if matches!(
            self.entries[slot],
            EvaluationEntry::Runnable | EvaluationEntry::Complete
        ) {
            return;
        }
        self.entries[slot] = EvaluationEntry::Runnable;
        self.runnable.push_back(self.layout.nodes[slot]);
    }

    fn complete(&mut self, node: NodeId) {
        let slot = self.layout.slots[&node];
        // Publish completion only after the entire task succeeds. In
        // particular, temporal waiters must not observe an interior prefix.
        if let Some(members) = self.pipelines.get(&node) {
            for member in members.iter().copied() {
                self.entries[self.layout.slots[&member]] = EvaluationEntry::Complete;
                if !self.released_by_earlier_frame.contains(&member) {
                    self.completed_events.push(member);
                }
            }
        } else if !self.released_by_earlier_frame.contains(&node) {
            self.completed_events.push(node);
        }
        self.entries[slot] = EvaluationEntry::Complete;
        // Read compact slots directly without cloning a dependent list or
        // changing the shared layout's reference count per completed node.
        for index in 0..self.layout.dependents(slot).len() {
            let dependent = self.layout.dependents(slot)[index];
            self.slot_dependency_ready(dependent);
        }
    }

    /// Retain a runnable node after its private step deliberately yields.
    /// The disposable future is not the continuation; operator state is.
    fn requeue_yielded(&mut self, node: NodeId) {
        self.entries[self.layout.slots[&node]] = EvaluationEntry::Runnable;
        self.runnable.push_front(node);
    }

    fn has_resident_continuation(&self) -> bool {
        !self.runnable.is_empty()
    }

    fn is_root(&self, node: NodeId) -> bool {
        self.layout.roots.binary_search(&node).is_ok()
    }

    fn is_complete(&self, node: NodeId) -> bool {
        self.layout
            .slots
            .get(&node)
            .is_some_and(|&slot| self.entries[slot] == EvaluationEntry::Complete)
    }

    fn roots_complete(&self) -> bool {
        self.layout.roots.iter().all(|node| self.is_complete(*node))
    }

    fn incomplete_nodes(&self) -> impl Iterator<Item = NodeId> + '_ {
        self.entries.iter().enumerate().filter_map(|(slot, entry)| {
            (*entry != EvaluationEntry::Complete).then_some(self.layout.nodes[slot])
        })
    }

    /// A hydration's entire graph slice remains a temporal barrier until install.
    fn registered_nodes(&self) -> Vec<NodeId> {
        self.layout.nodes.clone()
    }

    fn overlaps(&self, nodes: &HashSet<NodeId>) -> bool {
        self.layout.nodes.iter().any(|node| nodes.contains(node))
    }

    fn downstream_closure(&self, roots: impl IntoIterator<Item = NodeId>) -> HashSet<NodeId> {
        let mut affected = HashSet::default();
        let mut pending = roots.into_iter().collect::<VecDeque<_>>();
        while let Some(node) = pending.pop_front() {
            if !affected.insert(node) {
                continue;
            }
            if let Some(&slot) = self.layout.slots.get(&node) {
                pending.extend(
                    self.layout
                        .dependents(slot)
                        .iter()
                        .map(|&slot| self.layout.nodes[slot]),
                );
            }
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
        self.unary_batches.retain(|node, _| !nodes.contains(node));
        self.pipeline_batches
            .retain(|node, _| !nodes.contains(node));
        self.runnable.retain(|node| !nodes.contains(node));
        self.request_dependents
            .retain(|_, dependents| !dependents.iter().all(|node| nodes.contains(node)));
        for node in nodes {
            if let Some(&slot) = self.layout.slots.get(node) {
                if let Some(members) = self.pipelines.get(node) {
                    for member in members.iter() {
                        self.entries[self.layout.slots[member]] = EvaluationEntry::Complete;
                    }
                }
                self.entries[slot] = EvaluationEntry::Complete;
                self.temporal_waiting[slot] = 0;
            }
        }
    }

    fn add_temporal_blockers(&mut self, blockers: &HashMap<NodeId, usize>) {
        for slot in 0..self.entries.len() {
            let node = self.layout.nodes[slot];
            let count = blockers.get(&node).copied().unwrap_or_default();
            if count == 0 {
                continue;
            }
            self.temporal_waiting[slot] = count;
            let task_slot = self.task_slots[slot];
            match self.entries[task_slot] {
                EvaluationEntry::Runnable => {
                    self.runnable
                        .retain(|candidate| *candidate != self.layout.nodes[task_slot]);
                    self.entries[task_slot] = EvaluationEntry::Waiting(count);
                }
                EvaluationEntry::Waiting(remaining) => {
                    self.entries[task_slot] = EvaluationEntry::Waiting(remaining + count);
                }
                EvaluationEntry::Complete => {}
                EvaluationEntry::Fused => unreachable!("canonical task slot"),
            }
        }
    }

    fn temporal_ready(&mut self, node: NodeId) {
        let Some(&slot) = self.layout.slots.get(&node) else {
            return;
        };
        if self.temporal_waiting[slot] == 0 {
            return;
        }
        self.temporal_waiting[slot] -= 1;
        self.slot_dependency_ready(slot);
    }

    fn drain_completed_events(&mut self) -> Vec<NodeId> {
        std::mem::take(&mut self.completed_events)
    }

    /// Before an evaluation first registers as a temporal waiter, nothing it
    /// completed has been released to anyone. Its incomplete nodes are about
    /// to be registered, so each must emit its completion exactly once, even
    /// one an earlier frame already evaluated.
    fn discard_unregistered_completions(&mut self) {
        self.completed_events.clear();
        self.released_by_earlier_frame.clear();
    }

    /// Nodes this frame has completed (or abandoned).
    fn complete_nodes(&self) -> impl Iterator<Item = NodeId> + '_ {
        self.entries.iter().enumerate().filter_map(|(slot, entry)| {
            (*entry == EvaluationEntry::Complete).then_some(self.layout.nodes[slot])
        })
    }

    /// Seed a later frame of the same evaluation with the earlier frame's
    /// completed nodes (#3306). `carried` tasks stay complete without being
    /// scheduled again or re-emitting completion; their dependents see them
    /// as satisfied inputs. Every node in `released` is suppressed from
    /// emitting a completion event should this frame evaluate it again.
    fn carry_earlier_frame(&mut self, carried: &HashSet<NodeId>, released: HashSet<NodeId>) {
        let tails = (0..self.entries.len())
            .filter(|&slot| {
                self.task_slots[slot] == slot
                    && match self.pipelines.get(&self.layout.nodes[slot]) {
                        Some(members) => members.iter().all(|member| carried.contains(member)),
                        None => carried.contains(&self.layout.nodes[slot]),
                    }
            })
            .collect::<Vec<_>>();
        // Mark every carried task complete before releasing dependents, so a
        // carried dependent is never made runnable by a carried input.
        for &slot in &tails {
            if let Some(members) = self.pipelines.get(&self.layout.nodes[slot]) {
                for member in members.iter() {
                    self.entries[self.layout.slots[member]] = EvaluationEntry::Complete;
                }
            }
            self.entries[slot] = EvaluationEntry::Complete;
        }
        let (entries, slots) = (&self.entries, &self.layout.slots);
        self.runnable
            .retain(|node| entries[slots[node]] != EvaluationEntry::Complete);
        for &slot in &tails {
            for index in 0..self.layout.dependents(slot).len() {
                let dependent = self.layout.dependents(slot)[index];
                self.slot_dependency_ready(dependent);
            }
        }
        self.released_by_earlier_frame = released;
    }
}

impl<'a> IncrementalEvaluation<'a> {
    /// Second frame of a routed tick (#3288): activate exactly the route
    /// barriers the shared terminals' deltas reached, with their downstream
    /// closure, as if activation had reached them directly.
    fn activate_route_barriers(
        &mut self,
        runtime: &IvmRuntime,
        touched: HashSet<NodeId>,
    ) -> Result<(), IvmRuntimeError> {
        let closure = runtime.graph.downstream_through_routes(touched);
        self.stage_newly_relevant_state(runtime, &closure)?;
        let mut roots = self.work_queue.layout.roots.clone();
        for node in &closure {
            let mut meta = self
                .node_meta
                .get(node)
                .or_else(|| runtime.node_meta.get(node))
                .cloned()
                .unwrap_or_default();
            meta.input_generation = meta.input_generation.wrapping_add(1);
            if meta
                .retainers
                .iter()
                .any(|retainer| !matches!(retainer, Retainer::Hydration(_)))
            {
                roots.push(*node);
            }
            self.node_meta.insert(*node, meta);
            for subscription in runtime
                .subscriptions_by_output_node
                .get(node)
                .into_iter()
                .flatten()
            {
                if self.affected_subscriptions.insert(*subscription) {
                    self.metrics.subscriptions_considered += 1;
                }
                if let Some(state) = runtime.multisink_subscriptions.get(subscription) {
                    for output in state.outputs.values().filter(|output| output.node == *node) {
                        roots.extend(output.root_ordering_node);
                    }
                }
            }
        }
        Arc::make_mut(&mut self.affected_nodes).extend(closure.iter().copied());
        Arc::make_mut(&mut self.relevant_nodes).extend(closure.iter().copied());
        roots.sort_unstable();
        roots.dedup();
        let mut queue =
            EvaluationWorkQueue::discover_frame(&runtime.graph, &runtime.node_meta, roots, false)?;
        // The first frame's completions were (or, via the carried events
        // below, will be) released to temporal successors exactly once. A
        // park before this frame lets a later evaluation take the head of
        // those nodes' ordering queues, so this frame must neither schedule
        // them again nor release them a second time (#3306). Only nodes the
        // barriers reach are re-evaluated.
        let released = self.work_queue.complete_nodes().collect::<HashSet<_>>();
        let carried = released
            .iter()
            .copied()
            .filter(|node| !closure.contains(node))
            .collect::<HashSet<_>>();
        queue.carry_earlier_frame(&carried, released);
        queue.completed_events = self.work_queue.drain_completed_events();
        self.eval_memo.set_layout(Arc::clone(&queue.layout));
        self.work_queue = queue;
        Ok(())
    }

    /// The first frame staged state only for the nodes its activation plan
    /// reached, and installation replaces exactly `relevant_nodes`. Barriers
    /// activated now (with their ancestors) must bring their live state into
    /// this evaluation first: a stateful node below a barrier, such as a
    /// binding's private collector (#3308), would otherwise evaluate from
    /// empty state and install that over its live state.
    fn stage_newly_relevant_state(
        &mut self,
        runtime: &IvmRuntime,
        closure: &HashSet<NodeId>,
    ) -> Result<(), IvmRuntimeError> {
        let layout = runtime
            .graph
            .execution_layout(closure.iter().copied())
            .map_err(IvmRuntimeError::GraphNodeNotFound)?;
        let relevant = Arc::make_mut(&mut self.relevant_nodes);
        for node in layout.nodes.iter().copied() {
            if !relevant.insert(node) {
                continue;
            }
            let key = OperatorStateKey {
                scope: ScopeId::root(),
                node,
            };
            if let Some(state) = runtime.operator_states.get(&key) {
                self.operator_states.insert(key, state.clone());
            }
            if let Some(keys) = runtime.arrangement_keys_by_input.get(&node) {
                for key in keys {
                    if let Some(state) = runtime.arrangement_states.get(key) {
                        self.arrangement_states.insert(key.clone(), state.clone());
                        self.arrangement_keys_by_input
                            .entry(node)
                            .or_default()
                            .insert(key.clone());
                    }
                }
            }
            if let Some(meta) = runtime.node_meta.get(&node) {
                self.node_meta.entry(node).or_insert_with(|| meta.clone());
            }
        }
        Ok(())
    }

    fn poll_storage_flush(
        &mut self,
        indeterminate: &Rc<Cell<bool>>,
        extraction_storage: Option<OwnedStorage<'a>>,
        cx: &mut Context<'_>,
    ) -> Poll<Result<(), IvmRuntimeError>> {
        if self.persist_flush.is_none() && !self.durable_writes.borrow().is_empty() {
            let operations =
                std::mem::take(&mut *self.durable_writes.borrow_mut()).into_operations();
            let storage = extraction_storage.unwrap_or_else(|| self.storage.clone());
            let indeterminate = Rc::clone(indeterminate);
            self.persist_flush = Some(Box::pin(async move {
                let mut attempt = PersistFlushAttempt {
                    indeterminate,
                    completed: false,
                };
                let result = match storage.as_ref().write_many_outcome(operations).await {
                    WriteManyOutcome::Committed => Ok(()),
                    WriteManyOutcome::Uncommitted(error) => Err(error.into()),
                    WriteManyOutcome::PossiblyCommitted(error) => {
                        attempt.indeterminate.set(true);
                        Err(error.into())
                    }
                };
                attempt.completed = true;
                result
            }));
        }
        if let Some(flush) = &mut self.persist_flush {
            match flush.as_mut().poll(cx) {
                Poll::Pending => return Poll::Pending,
                Poll::Ready(Err(error)) => return Poll::Ready(Err(error)),
                Poll::Ready(Ok(())) => self.persist_flush = None,
            }
        }
        Poll::Ready(Ok(()))
    }

    /// Base-table frontiers describe resident row visibility, not completion
    /// of every dependent evaluator branch. Publish them monotonically when
    /// this evaluation parks so independent snapshots cannot reuse an old
    /// table-source cache; retain all operator state until full installation.
    fn install_input_frontiers(&self, runtime: &mut IvmRuntime) {
        for (table, frontier) in &self.table_frontiers {
            runtime
                .table_frontiers
                .entry(table.clone())
                .and_modify(|live| *live = (*live).max(*frontier))
                .or_insert(*frontier);
        }
        for (binding, frontier) in &self.binding_frontiers {
            runtime
                .binding_frontiers
                .entry(binding.clone())
                .and_modify(|live| *live = (*live).max(*frontier))
                .or_insert(*frontier);
        }
        for (node, staged) in &self.node_meta {
            if let Some(live) = runtime.node_meta.get_mut(node) {
                live.input_generation = live.input_generation.max(staged.input_generation);
            }
        }
    }

    fn abandon(&mut self, nodes: &HashSet<NodeId>) {
        // An empty set is the explicit cancellation path used by an owner
        // which is abandoning the whole uninstalled evaluation. Scoped
        // failures carry their downstream closure and may leave independent
        // roots publishable.
        self.discarded = nodes.is_empty()
            || self
                .work_queue
                .layout
                .roots
                .iter()
                .all(|root| nodes.contains(root));
        self.work_queue.abandon(nodes);
        // A scoped failure owns only its downstream slice. Keep evaluating
        // disjoint roots in this tick; their prepared state and notifications
        // remain publishable. The staged maps share immutable bases and this
        // removes only entries produced by the failed owner.
        self.operator_states
            .retain(|key, _| !nodes.contains(&key.node));
        self.arrangement_states
            .retain(|key, _| !nodes.contains(&key.input));
        self.arrangement_keys_by_input
            .retain(|node, _| !nodes.contains(node));
        self.eval_memo.retain(|key, _| !nodes.contains(&key.node));
        self.eval_memo_bytes = self
            .eval_memo
            .values()
            .map(|entry| entry.payload_bytes)
            .sum();
        self.node_meta.retain(|node, _| !nodes.contains(node));
        Arc::make_mut(&mut self.relevant_nodes).retain(|node| !nodes.contains(node));
        Arc::make_mut(&mut self.affected_nodes).retain(|node| !nodes.contains(node));
        self.terminal_deltas.retain(|node, _| !nodes.contains(node));
        self.root_ordering_windows
            .retain(|node, _| !nodes.contains(node));
        self.pending_subscription_outputs
            .retain(|node, _| !nodes.contains(node));
    }

    fn install(&mut self, runtime: &mut IvmRuntime) {
        if self.discarded {
            return;
        }
        // Drop the committed entries before folding staged COW state. This
        // makes recursive closures and arrangement bases uniquely owned while
        // leaving unrelated graph state untouched.
        for (key, state) in &mut self.operator_states {
            runtime.operator_states.remove(key);
            if let OperatorState::Recursive(recursive) = state {
                recursive.value_mut().commit_staged_positive();
            }
            if let OperatorState::TopBy(top_by) = state {
                top_by.value_mut().commit_overlays();
            }
            if let OperatorState::ArgBy(arg_by) = state {
                arg_by.value_mut().commit_overlay();
            }
            if let OperatorState::SemiJoin(semi_join) = state {
                semi_join.commit_published_overlay();
            }
            if let OperatorState::AntiJoin(anti_join) = state {
                anti_join.commit_published_overlay();
            }
            if let OperatorState::CollectBy(collect_by) = state {
                collect_by.groups.commit_overlay();
            }
        }
        runtime
            .operator_states
            .extend(std::mem::take(&mut self.operator_states));

        for (key, state) in &mut self.arrangement_states {
            runtime.arrangement_states.remove(key);
            state.value_mut().commit_overlay();
        }
        runtime
            .arrangement_states
            .extend(std::mem::take(&mut self.arrangement_states));
        for node in self.arrangement_keys_by_input.keys() {
            runtime.arrangement_keys_by_input.remove(node);
        }
        runtime
            .arrangement_keys_by_input
            .extend(std::mem::take(&mut self.arrangement_keys_by_input));

        // Tick deltas belong to the frame, not the retained hydration cache.
        // Previously we hashed/copied them into runtime only to evict them at
        // the end of publication. Retain only actual hydration reuse entries.
        for (key, entry) in std::mem::take(&mut self.eval_memo).into_entries() {
            if key.tick_epoch.is_none() {
                runtime.eval_memo_bytes =
                    runtime.eval_memo_bytes.saturating_add(entry.payload_bytes);
                if let Some(old) = runtime.eval_memo.insert(key, entry) {
                    runtime.eval_memo_bytes =
                        runtime.eval_memo_bytes.saturating_sub(old.payload_bytes);
                }
            }
        }
        runtime.memo_use_clock = runtime.memo_use_clock.max(self.memo_use_clock);
        carry_live_node_lifecycle(&mut self.node_meta, runtime, &self.relevant_nodes);
        runtime
            .node_meta
            .extend(std::mem::take(&mut self.node_meta));
        self.install_input_frontiers(runtime);
        runtime.current_tick = runtime.current_tick.max(self.current_tick);
        runtime
            .pending_binding_retractions
            .drain(..self.pending_binding_retractions);
    }

    fn poll(
        &mut self,
        runtime: &mut IvmRuntime,
        cx: &mut Context<'_>,
    ) -> Poll<Result<(), EvaluationFailure>> {
        if self.discarded {
            return Poll::Ready(Ok(()));
        }
        let ready = self.requests.poll(cx);
        if ready == 0 {
            self.requests.poll_eager_retry(cx);
        }
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

        let dropped_subscriptions = Vec::new();
        let mut evaluator = TickEvaluator {
            schema: &runtime.schema,
            graph: &runtime.graph,
            variant_projections: &runtime.variant_projections,
            table_deltas: &self.table_deltas,
            binding_deltas: &self.binding_deltas,
            binding_snapshots: &self.binding_snapshots,
            current_tick: self.current_tick,
            operator_states: &mut self.operator_states,
            arrangement_states: &mut self.arrangement_states,
            arrangement_keys_by_input: &mut self.arrangement_keys_by_input,
            eval_memo: &mut self.eval_memo,
            eval_memo_bytes: &mut self.eval_memo_bytes,
            table_frontiers: &self.table_frontiers,
            binding_frontiers: &self.binding_frontiers,
            memo_use_clock: &mut self.memo_use_clock,
            node_meta: &mut self.node_meta,
            storage: Some(self.storage.as_ref()),
            evaluation_inputs: self.evaluation_inputs.as_mut(),
            context: EvalContext::root(),
            metrics: &mut self.metrics,
            terminal_deltas: std::mem::take(&mut self.terminal_deltas),
            root_ordering_windows: std::mem::take(&mut self.root_ordering_windows),
        };

        let mut registered_requests = false;
        while let Some(node) = self.work_queue.runnable.pop_front() {
            let result = self.work_queue.poll_node(&mut evaluator, node, cx);
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
                    self.terminal_deltas = std::mem::take(&mut evaluator.terminal_deltas);
                    self.root_ordering_windows =
                        std::mem::take(&mut evaluator.root_ordering_windows);
                    drop(evaluator);
                    cx.waker().wake_by_ref();
                    return Poll::Pending;
                }
            }
        }
        let mut immediately_ready = if registered_requests {
            self.requests.poll(cx)
        } else {
            0
        };
        if registered_requests && immediately_ready == 0 {
            // A backend may explicitly attest that a yielded read is still
            // executor-local (for example, the in-memory cursor path). Retry
            // only those requests; opaque cold storage keeps ownership of its
            // self-wake and remains pending for the real runtime owner.
            immediately_ready = self.requests.poll_eager_retry(cx);
        }
        if immediately_ready > 0 {
            // Resident requests completed synchronously. Install their results
            // and resume the queue within this same public poll so resident
            // writes retain their same-tick visibility contract.
            self.terminal_deltas = std::mem::take(&mut evaluator.terminal_deltas);
            self.root_ordering_windows = std::mem::take(&mut evaluator.root_ordering_windows);
            drop(evaluator);
            return self.poll(runtime, cx);
        }
        if !self.routed_terminals.is_empty() && self.work_queue.roots_complete() {
            let routed = std::mem::take(&mut self.routed_terminals);
            let mut touched = touched_route_barriers(&mut evaluator, &runtime.graph, &routed, cx);
            // A barrier that reaches durable state was already activated and
            // evaluated in the first frame; activating it again would bump its
            // input generation and re-evaluate it.
            touched.retain(|barrier| !self.affected_nodes.contains(barrier));
            if !touched.is_empty() {
                self.terminal_deltas = std::mem::take(&mut evaluator.terminal_deltas);
                self.root_ordering_windows = std::mem::take(&mut evaluator.root_ordering_windows);
                drop(evaluator);
                self.activate_route_barriers(runtime, touched)?;
                return self.poll(runtime, cx);
            }
        }

        // Until phase B has activated this tick's route barriers, a routed
        // subscription's barrier-gated sinks have not produced their deltas.
        // Publishing its other sinks now would mark it published, and phase B
        // would then skip it, losing the routed delta (#3288).
        let routes_pending = !self.routed_terminals.is_empty();
        let awaits_route_barriers = |subscription: &MultisinkSubscriptionState| {
            routes_pending
                && matches!(
                    &subscription.target,
                    MultisinkSubscriptionTarget::RoutedShape { route_barriers, .. }
                        if !route_barriers.is_empty()
                )
        };
        let mut terminal_consumers = HashMap::<NodeId, usize>::default();
        for subscription_id in &self.affected_subscriptions {
            let Some(subscription) = runtime.multisink_subscriptions.get(subscription_id) else {
                continue;
            };
            if subscription.failed
                || self.published_subscriptions.contains(subscription_id)
                || awaits_route_barriers(subscription)
            {
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
                || awaits_route_barriers(subscription)
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
                let (physical_records, materialized) = if let Some((physical, materialized)) =
                    self.pending_subscription_outputs.get(&output.node)
                {
                    (Arc::clone(physical), materialized.clone())
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
                        .insert(output.node, (Arc::clone(&records), None));
                    (records, None)
                };
                let materialized = match materialized {
                    Some(records) => Ok(records),
                    None => evaluator.materialize_indirect_input(&physical_records),
                };
                let records = match materialized {
                    Ok(records) => {
                        self.pending_subscription_outputs.insert(
                            output.node,
                            (Arc::clone(&physical_records), Some(Arc::clone(&records))),
                        );
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
                            // Retry only a backend that explicitly attests its
                            // reads are executor-local. A cold self-wake is
                            // not a resident continuation.
                            ready = self.requests.poll_eager_retry(cx);
                        }
                        if ready > 0 {
                            return self.poll(runtime, cx);
                        }
                        return Poll::Pending;
                    }
                    Err(error) => return Poll::Ready(Err(error.into())),
                };
                prepared_outputs.push((sink, output, physical_records, records));
            }

            let mut sinks = BTreeMap::new();
            let mut terminal_sinks = BTreeMap::new();
            for (sink, output, physical_records, records) in prepared_outputs {
                if !records.deltas.is_empty()
                    && !records.descriptor.registry_compatible_with(&output.output)
                {
                    return Poll::Ready(Err(IvmRuntimeError::GraphOutputMismatch.into()));
                }
                let structured = evaluator.output_is_structured_collect_by(output.node)?;
                let public_root = evaluator.output_has_public_root(output.node)?;
                let terminal_owned = output.root_ordering_node.is_some() || structured;
                let identity = match output.root_ordering_node {
                    Some(ordering) if !structured => {
                        root_identity_fields(evaluator.graph, output.node, ordering)?
                    }
                    _ => None,
                };
                let records = records.as_ref().clone();
                // Only the groups this output's own deltas reach take part in
                // its root ordering; the group fields lead the identity.
                let identity_groups = identity
                    .as_ref()
                    .map(|identity| {
                        physical_records
                            .deltas
                            .iter()
                            .map(|delta| {
                                encoded_identity_key_part(
                                    physical_records.descriptor,
                                    delta.raw(),
                                    &identity.fields[..identity.group_len],
                                )
                            })
                            .collect::<Result<BTreeSet<_>, _>>()
                    })
                    .transpose()?;
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
                        Some(match &identity {
                            Some(identity) => terminal_deltas_keyed_by_identity(
                                &records,
                                &physical_records,
                                &identity.fields,
                            )?,
                            None => terminal_deltas_from_record_deltas(&records)?,
                        })
                    } else if output.root_ordering_node.is_some() {
                        Some(TerminalDeltas {
                            operations: Vec::new(),
                        })
                    } else {
                        None
                    };
                    if let Some(mut terminal) = terminal {
                        // A CollectBy root terminal already owns exact
                        // occurrence keys and positional edits. Its rendered
                        // record can omit joined occurrence fields, so the
                        // generic root-ordering pass would synthesize a
                        // root-UUID-only Move that cannot address the
                        // collector's occurrence-keyed output.
                        if !structured && let Some(root_ordering_node) = output.root_ordering_node {
                            evaluator.apply_root_ordering(
                                root_ordering_node,
                                output.output,
                                identity.as_ref().zip(identity_groups.as_ref()),
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
                    && queued.publication.is_none()
                {
                    // A resident publication may have an independent cold
                    // branch still parked. Its completed subscriptions are
                    // already a valid publication slice, so hand their
                    // notifications to the resident receipt now instead of
                    // waiting for the unrelated branch to hydrate.
                    pending
                        .notifications
                        .borrow_mut()
                        .push((*subscription_id, queued));
                } else {
                    self.pending_notifications.push((*subscription_id, queued));
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
        match self.poll_storage_flush(&runtime.persistence_indeterminate, None, cx) {
            Poll::Pending => return Poll::Pending,
            Poll::Ready(result) => result?,
        }
        self.install(runtime);
        runtime
            .operator_states
            .retain(|key, _| key.scope == ScopeId::root());
        let notifications = std::mem::take(&mut self.pending_notifications);
        let mut dropped_subscriptions = dropped_subscriptions;
        for (subscription_id, queued) in notifications {
            if self.defer_notifications_until_durable
                && queued.publication.is_some_and(|publication| {
                    !runtime
                        .durable_notification_publications
                        .contains(&publication)
                })
            {
                runtime
                    .deferred_notifications
                    .entry(queued.publication.expect("checked publication"))
                    .or_default()
                    .push((subscription_id, queued));
            } else if let Some(pending) = &self.pending_resident_publication
                && queued.publication.is_none()
            {
                pending
                    .notifications
                    .borrow_mut()
                    .push((subscription_id, queued));
            } else if runtime
                .multisink_subscriptions
                .get(&subscription_id)
                .is_some_and(|subscription| subscription.sender.send(queued).is_err())
            {
                dropped_subscriptions.push(subscription_id);
            }
        }
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
        let (relevant_nodes, mut work_queue) = EvaluationWorkQueue::discover(
            &runtime.graph,
            &runtime.node_meta,
            roots.iter().copied(),
            true,
        )?;
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
        let mut eval_memo = EvaluationMemo::for_layout(Arc::clone(&work_queue.layout));
        eval_memo.extend(
            runtime
                .eval_memo
                .iter()
                .filter(|(key, _)| relevant_nodes.contains(&key.node))
                .map(|(key, entry)| (key.clone(), entry.clone())),
        );
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
            terminal_deltas: HashMap::default(),
            binding_frontiers: runtime.binding_frontiers.clone(),
            storage,
            requests,
            evaluation_inputs: EvaluationInputs::default(),
            work_queue,
            root_indirect_values: RootIndirectValues::Materialize,
        })
    }

    fn advance_binding_input(&mut self, graph: &IvmGraph, shape: &str) {
        let key = BindingSourceKey::prepared(shape);
        *self.binding_frontiers.entry(key.clone()).or_default() += 1;
        let affected = graph
            .affected_nodes_through_routes(std::iter::empty(), std::iter::once(&key))
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
        binding_snapshots: &BindingSnapshots,
        hydrate_arrangements: bool,
        metrics: &mut TickMetrics,
        cx: &mut Context<'_>,
    ) -> Poll<Result<(), IvmRuntimeError>> {
        let mut remaining_runnable_nodes = MAX_HYDRATION_RUNNABLE_NODES_PER_POLL;
        'owner_turn: loop {
            let ready = self.requests.poll(cx);
            if ready == 0 {
                self.requests.poll_eager_retry(cx);
            }
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
                        terminal_deltas: std::mem::take(&mut self.terminal_deltas),
                        root_ordering_windows: HashMap::default(),
                    };
                    let poll = self.work_queue.poll_node(&mut evaluator, node, cx);
                    self.terminal_deltas = std::mem::take(&mut evaluator.terminal_deltas);
                    match poll {
                        // A future which cooperatively yielded has not registered a
                        // storage request. Preserve the CPU-yield distinction from
                        // `IncrementalEvaluation`: requeue it for a fresh owner
                        // turn rather than pretending it is a missing input.
                        Poll::Pending => {
                            self.work_queue.requeue_yielded(node);
                            cx.waker().wake_by_ref();
                            return Poll::Pending;
                        }
                        // `EvaluationBlocked` is deliberately *ready*: the
                        // evaluator has populated `evaluation_inputs.missing`.
                        // Let the common branch below retain and poll those
                        // requests; collapsing it with `Poll::Pending` causes a
                        // self-waking replay loop that can never hydrate a large
                        // indirect literal.
                        // Interior results are already retained in the memo;
                        // roots can borrow the same immutable bytes below.
                        Poll::Ready(result) => result,
                    }
                };
                match result {
                    Ok(records) => {
                        if self.work_queue.is_root(node) {
                            let materialized_fields = self
                                .root_indirect_values
                                .materialized_field_indices(&records.descriptor);
                            let mut materialized = Vec::with_capacity(records.deltas.len());
                            let mut blocked = false;
                            for delta in &records.deltas {
                                match crate::large_values::materialize_record_borrowed_attempt(
                                    &records.descriptor,
                                    delta.raw(),
                                    materialized_fields.as_deref(),
                                    &mut self.evaluation_inputs,
                                ) {
                                    Ok(record) => materialized.push(RecordDelta {
                                        record: match record {
                                            std::borrow::Cow::Borrowed(_) => delta.record.clone(),
                                            std::borrow::Cow::Owned(record) => record.into(),
                                        },
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
                                let ready = self.requests.poll(cx);
                                if ready > 0
                                    || (ready == 0 && self.requests.poll_eager_retry(cx) > 0)
                                {
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
                            self.outputs.insert(
                                node,
                                RecordDeltas {
                                    descriptor: records.descriptor,
                                    deltas: materialized,
                                },
                            );
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
                        if registered || self.requests.has_pending() {
                            let ready = self.requests.poll(cx);
                            if ready > 0 || (ready == 0 && self.requests.poll_eager_retry(cx) > 0) {
                                continue 'owner_turn;
                            }
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
            if !self.work_queue.temporal_waiting.is_empty() {
                // Earlier evaluations own the continuation until their shared
                // nodes are installed and the temporal barriers are released.
                return Poll::Pending;
            }
            return Poll::Ready(Err(IvmRuntimeError::EvaluationBlocked));
        }
    }

    fn install(mut self, runtime: &mut IvmRuntime) {
        for node in &self.relevant_nodes {
            runtime.operator_states.remove(&OperatorStateKey {
                scope: ScopeId::root(),
                node: *node,
            });
        }
        // Session hydration also establishes long-lived collector state. Fold
        // its initially populated sparse groups now, otherwise the first
        // incremental edit would COW-clone the entire hydration overlay.
        for state in self.operator_states.values_mut() {
            if let OperatorState::TopBy(top_by) = state {
                top_by.value_mut().commit_overlays();
            }
            if let OperatorState::ArgBy(arg_by) = state {
                arg_by.value_mut().commit_overlay();
            }
            if let OperatorState::SemiJoin(semi_join) = state {
                semi_join.commit_published_overlay();
            }
            if let OperatorState::AntiJoin(anti_join) = state {
                anti_join.commit_published_overlay();
            }
            if let OperatorState::CollectBy(collect_by) = state {
                collect_by.groups.commit_overlay();
            }
        }
        runtime.operator_states.extend(self.operator_states);
        for node in &self.relevant_nodes {
            if let Some(keys) = runtime.arrangement_keys_by_input.get(node) {
                for key in keys {
                    runtime.arrangement_states.remove(key);
                }
            }
        }
        // Hydration-created arrangements are the immutable bases for the
        // next staged tick. Fold their initial overlays after removing the
        // old live entries, just as incremental installation does.
        for state in self.arrangement_states.values_mut() {
            state.value_mut().commit_overlay();
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
        runtime.eval_memo.extend(
            self.eval_memo
                .into_entries()
                .filter(|(key, _)| key.tick_epoch.is_none()),
        );
        runtime.eval_memo_bytes = runtime
            .eval_memo
            .values()
            .map(|entry| entry.payload_bytes)
            .sum();
        runtime.memo_use_clock = runtime.memo_use_clock.max(self.memo_use_clock);
        carry_live_node_lifecycle(&mut self.node_meta, runtime, &self.relevant_nodes);
        for node in &self.relevant_nodes {
            runtime.node_meta.remove(node);
        }
        runtime.node_meta.extend(self.node_meta);
    }
}

impl IvmRuntime {
    #[allow(clippy::too_many_arguments)]
    pub(super) fn enqueue_subscription_hydration(
        &mut self,
        subscription_id: SubscriptionId,
        outputs: BTreeMap<String, CompiledNode>,
        storage: OwnedStorage<'static>,
        binding_snapshots: Option<Arc<BindingSnapshots>>,
        binding_frontier_advance: Option<&str>,
        initial: Arc<Mutex<Option<MultisinkDeltas>>>,
        lifetime: SubscriptionLifetime,
        root_indirect_values: RootIndirectValues,
    ) -> Result<(), IvmRuntimeError> {
        let mut seen_roots = HashSet::new();
        let roots = outputs
            .values()
            .flat_map(|output| [output.root_ordering_node, Some(output.node)])
            .flatten()
            .filter(|root| seen_roots.insert(*root))
            .collect::<VecDeque<_>>();
        let hydrate_arrangements = lifetime == SubscriptionLifetime::Retained
            && roots.iter().copied().try_fold(false, |found, root| {
                Ok::<_, IvmRuntimeError>(found || self.output_depends_on_aggregate(root)?)
            })?;
        let mut session = EvaluationSession::hydration(self, roots, storage)?;
        session.root_indirect_values = root_indirect_values;
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
            lifetime,
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
        let changed_tables = table_deltas
            .iter()
            .map(|delta| delta.table.as_str())
            .collect::<HashSet<_>>();
        let affected_nodes = self
            .graph
            .affected_nodes(changed_tables.iter().copied(), std::iter::empty());

        // Hydration evaluates an isolated snapshot and installs that snapshot
        // atomically. Do not begin a resident tick which overlaps its graph
        // slice: beginning mutates durable evaluator state and input
        // generations before the work queue can attach temporal blockers, so
        // a later hydration install would otherwise roll those mutations back.
        std::future::poll_fn(|cx| {
            let mut resident_only = false;
            loop {
                let selected = self
                    .pending_incremental
                    .0
                    .borrow()
                    .hydration_admission_evaluations(&affected_nodes);
                if selected.is_empty() {
                    return Poll::Ready(Ok(()));
                }
                if let Poll::Ready(Err(error)) =
                    self.poll_incremental(cx, resident_only, Some(&selected))
                {
                    return Poll::Ready(Err(error));
                }
                let pending = self.pending_incremental.0.borrow();
                let remaining = pending.hydration_admission_evaluations(&affected_nodes);
                if remaining.is_empty() {
                    return Poll::Ready(Ok(()));
                }
                if !remaining.iter().any(|id| {
                    pending
                        .evaluations
                        .get(id)
                        .is_some_and(PendingEvaluation::has_resident_continuation)
                }) {
                    return Poll::Pending;
                }
                // The direct write owns CPU-only continuations needed to
                // finish an overlapping hydration. A host waker must not
                // turn resident work into an artificial async write. Never
                // re-poll cold requests or unrelated graph work here.
                resident_only = true;
            }
        })
        .await?;

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
        let durable_writes = Rc::clone(&evaluation.durable_writes);
        match progress {
            Poll::Ready(Ok(())) => {}
            Poll::Ready(Err(failure)) => return Err(failure.into_error()),
            Poll::Pending => {
                evaluation.work_queue.discard_unregistered_completions();
                evaluation.install_input_frontiers(self);
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
            durable_writes,
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
        self.poll_incremental(cx, false, None)
    }

    /// Poll only evaluations that have retained an explicit in-memory
    /// continuation. Cold evaluations are left entirely untouched: their
    /// storage futures may have self-woken, but only a runtime owner may poll
    /// them again with a durable waker.
    pub(crate) fn poll_resident_incremental(
        &mut self,
        cx: &mut Context<'_>,
    ) -> Poll<Result<(), IvmRuntimeError>> {
        self.poll_incremental(cx, true, None)
    }

    fn poll_incremental(
        &mut self,
        cx: &mut Context<'_>,
        resident_only: bool,
        selected_evaluations: Option<&HashSet<u64>>,
    ) -> Poll<Result<(), IvmRuntimeError>> {
        debug_assert!(
            !self.pending_incremental_polling,
            "pending evaluation polling is not reentrant"
        );
        self.pending_incremental_polling = true;
        let slot = Rc::clone(&self.pending_incremental.0);
        let mut state = std::mem::take(&mut *slot.borrow_mut());
        if state.order.is_empty() {
            return self.finish_pending_incremental_poll(&slot, state, Poll::Ready(Ok(())));
        }
        let mut retained_order = VecDeque::new();
        while let Some(evaluation_id) = state.order.pop_front() {
            let mut evaluation = state
                .evaluations
                .remove(&evaluation_id)
                .expect("pending evaluation order references a live session");
            if selected_evaluations.is_some_and(|selected| !selected.contains(&evaluation_id)) {
                state.evaluations.insert(evaluation_id, evaluation);
                retained_order.push_back(evaluation_id);
                continue;
            }
            if resident_only && !evaluation.has_resident_continuation() {
                state.evaluations.insert(evaluation_id, evaluation);
                retained_order.push_back(evaluation_id);
                continue;
            }
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
            // A hydration session owns a private snapshot of all reachable
            // state. Its completed interior nodes are not safe handoff points:
            // a later incremental evaluation would run against the old live
            // runtime, then lose its changes when hydration installs. Treat
            // the whole session as one temporal barrier instead.
            if matches!(evaluation, PendingEvaluation::Incremental(_)) {
                let completed = evaluation.work_queue_mut().drain_completed_events();
                state.release_temporal_successors(evaluation_id, completed);
            }
            match progress {
                Poll::Ready(Ok(())) => {
                    if let PendingEvaluation::SubscriptionHydration(hydration) = evaluation {
                        let snapshot = subscription_snapshot_from_hydrated(
                            &self.graph,
                            &hydration.outputs,
                            &hydration.session.outputs,
                            &hydration.session.terminal_deltas,
                        );
                        let completed = hydration.session.work_queue.registered_nodes();
                        // First-result evaluation borrows shared inputs, but its
                        // temporary state is not a maintenance proof. Never
                        // overwrite a live sibling's indexes with that state.
                        if hydration.lifetime == SubscriptionLifetime::Retained {
                            hydration.session.install(self);
                        }
                        state.release_temporal_successors(evaluation_id, completed);
                        self.record_hydration_memo_metrics(&hydration.metrics);
                        self.evict_eval_memo();
                        match snapshot {
                            Ok(snapshot) => {
                                *hydration
                                    .initial
                                    .lock()
                                    .expect("subscription initial snapshot mutex poisoned") =
                                    Some(snapshot);
                                if hydration.lifetime == SubscriptionLifetime::FirstResult {
                                    self.unsubscribe(hydration.subscription_id);
                                }
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
                        return self.finish_pending_incremental_poll(
                            &slot,
                            state,
                            Poll::Ready(Err(failure.into_error())),
                        );
                    }
                    // A failed first-result session has not published mutable
                    // state. Its scoped error cannot invalidate live siblings.
                    if !matches!(&evaluation, PendingEvaluation::SubscriptionHydration(hydration)
                        if hydration.lifetime == SubscriptionLifetime::FirstResult)
                    {
                        self.fail_evaluation_nodes(&failure);
                    }
                    let released_nodes =
                        if matches!(&evaluation, PendingEvaluation::SubscriptionHydration(_)) {
                            evaluation.work_queue().registered_nodes()
                        } else {
                            evaluation
                                .work_queue()
                                .incomplete_nodes()
                                .collect::<Vec<_>>()
                        };
                    for node in released_nodes {
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
                    state.evaluations.insert(evaluation_id, evaluation);
                    retained_order.push_back(evaluation_id);
                    if resident_only {
                        // A resident slice can discover cold storage. It has
                        // installed only this direct call's no-op waker, so
                        // leave it parked and continue looking for unrelated
                        // resident work later in the queue.
                        continue;
                    }
                    // One owner turn advances at most one suspended
                    // evaluation. In particular, a cold subscription
                    // hydration must hand control back to the runtime owner
                    // before it can drain unrelated queued work (such as
                    // transport ingress and local write fates). The pending
                    // storage future has just received this poll's durable
                    // waker; cooperative in-memory yields wake it directly.
                    retained_order.append(&mut state.order);
                    state.order = retained_order;
                    return self.finish_pending_incremental_poll(&slot, state, Poll::Pending);
                }
            }
        }
        let done = retained_order.is_empty();
        state.order = retained_order;
        self.finish_pending_incremental_poll(
            &slot,
            state,
            if done {
                Poll::Ready(Ok(()))
            } else {
                Poll::Pending
            },
        )
    }

    fn finish_pending_incremental_poll(
        &mut self,
        slot: &Rc<RefCell<PendingIncrementalState>>,
        state: PendingIncrementalState,
        result: Poll<Result<(), IvmRuntimeError>>,
    ) -> Poll<Result<(), IvmRuntimeError>> {
        {
            let mut slot = slot.borrow_mut();
            *slot = state;
        }
        self.pending_incremental_polling = false;
        // A lifecycle operation may have released retainers while this owner
        // turn held the queue locally. Retry only requested collection: queued
        // work protects its registered nodes, and the request remains until
        // the queue drains so completed/cancelled work is eventually freed.
        if self.ephemeral_graph_gc_pending {
            self.collect_unretained_ephemeral_nodes();
        }
        result
    }

    /// Finish already-prepared durable writes in temporal order without
    /// advancing query roots or waiting for their missing chunks. Durable nodes
    /// are fully evaluated before an incremental session enters this queue.
    pub(crate) fn poll_storage_extraction(
        &mut self,
        storage: &OwnedStorage<'static>,
        cx: &mut Context<'_>,
    ) -> Poll<Result<(), IvmRuntimeError>> {
        let mut pending = self.pending_incremental.0.borrow_mut();
        let order = pending.order.iter().copied().collect::<Vec<_>>();
        for id in order {
            if let Some(PendingEvaluation::Incremental(evaluation)) =
                pending.evaluations.get_mut(&id)
            {
                match evaluation.poll_storage_flush(
                    &self.persistence_indeterminate,
                    Some(storage.clone()),
                    cx,
                ) {
                    Poll::Pending => return Poll::Pending,
                    Poll::Ready(result) => result?,
                }
            }
        }
        Poll::Ready(Ok(()))
    }

    pub(crate) fn has_pending_storage_writes(&self) -> bool {
        self.pending_incremental.0.borrow().evaluations.values().any(|evaluation| {
            matches!(evaluation, PendingEvaluation::Incremental(evaluation)
                if evaluation.persist_flush.is_some() || !evaluation.durable_writes.borrow().is_empty())
        })
    }

    pub(crate) fn has_pending_incremental(&self) -> bool {
        self.pending_incremental.is_pending()
    }

    /// Whether an admitted evaluation can still change this receiver's
    /// terminal. An already-consumed initial batch is not a progress fence:
    /// later storage publications may still be suspended. Check the actual
    /// consumer, not global runtime idleness (an unrelated cold graph must
    /// not hold a ready subscription's opening hostage).
    pub(crate) fn subscription_has_pending_progress(&self, id: SubscriptionId) -> bool {
        // A missing/failed receiver cannot prove a completed terminal.
        if self.pending_incremental_polling
            || self
                .multisink_subscriptions
                .get(&id)
                .is_none_or(|s| s.failed)
        {
            return true;
        }
        self.pending_incremental
            .0
            .borrow()
            .evaluations
            .values()
            .any(|evaluation| {
                match evaluation {
                    PendingEvaluation::SubscriptionHydration(hydration) => {
                        hydration.subscription_id == id
                    }
                    // Even computed output may still be buffered until the
                    // evaluation commits. Do not use published_subscriptions as
                    // evidence that the receiver has actually received it.
                    PendingEvaluation::Incremental(evaluation) => {
                        evaluation.affected_subscriptions.contains(&id)
                    }
                }
            })
            || self.deferred_notifications.values().any(|notifications| {
                notifications
                    .iter()
                    .any(|(subscription, _)| *subscription == id)
            })
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
            for node in evaluation.work_queue().registered_nodes() {
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
        if self.persistence_indeterminate.get() {
            return Err(IvmRuntimeError::PersistenceOutcomeIndeterminate);
        }
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
            .map(|delta| &delta.key)
            .collect::<HashSet<_>>();
        let activation = self
            .graph
            .activation_plan(
                changed_tables.iter().copied(),
                changed_bindings.iter().copied(),
            )
            .map_err(IvmRuntimeError::GraphNodeNotFound)?;
        let affected_nodes = Arc::clone(&activation.affected);
        // Capture only the graph slice reached from changed inputs. The
        // evaluator may need unchanged sibling inputs (for example the other
        // side of a join), so discovery walks ancestors of every affected
        // node, while unrelated graph state remains in the live runtime.
        let relevant_nodes = Arc::clone(&activation.relevant);
        // Capture by graph key, never by filtering global retained maps. Root
        // state is the only durable evaluator state; recursive child scopes
        // are scratch and are removed before publication.
        let mut operator_states = relevant_nodes
            .iter()
            .filter_map(|node| {
                let key = OperatorStateKey {
                    scope: ScopeId::root(),
                    node: *node,
                };
                self.operator_states
                    .get(&key)
                    .cloned()
                    .map(|state| (key, state))
            })
            .collect::<HashMap<_, _>>();
        let mut arrangement_states = HashMap::default();
        let mut arrangement_keys_by_input = HashMap::default();
        for input in relevant_nodes.iter() {
            let Some(keys) = self.arrangement_keys_by_input.get(input) else {
                continue;
            };
            for key in keys {
                if let Some(state) = self.arrangement_states.get(key) {
                    arrangement_states.insert(key.clone(), state.clone());
                    arrangement_keys_by_input
                        .entry(*input)
                        .or_insert_with(HashSet::default)
                        .insert(key.clone());
                }
            }
        }
        // Tick memo entries are disposable. Recomputing the affected graph is
        // bounded by that graph slice and avoids a global memo scan.
        let mut eval_memo = EvaluationMemo::default();
        let mut eval_memo_bytes = 0;
        let mut memo_use_clock = self.memo_use_clock;
        let mut node_meta = relevant_nodes
            .iter()
            .filter_map(|node| self.node_meta.get(node).cloned().map(|meta| (*node, meta)))
            .collect::<HashMap<_, _>>();
        let mut table_frontiers = HashMap::default();
        let mut binding_frontiers = HashMap::default();
        for table in &activation.tables {
            if let Some(frontier) = self.table_frontiers.get(table) {
                table_frontiers.insert(table.clone(), *frontier);
            }
        }
        for binding in &activation.bindings {
            if let Some(frontier) = self.binding_frontiers.get(binding) {
                binding_frontiers.insert(binding.clone(), *frontier);
            }
        }
        let current_tick = self.current_tick + 1;
        let durable_writes = Rc::new(RefCell::new(StagedWriteState::default()));
        // Durable nodes run under `&self`, so the binding set cannot change
        // before the incremental evaluation below reuses this snapshot.
        let binding_snapshots = self.binding_snapshot_deltas();
        let table_delta_records = table_deltas
            .iter()
            .map(|delta| delta.deltas.len())
            .sum::<usize>();
        self.tick_durable_nodes(
            &table_deltas,
            &binding_snapshots,
            &activation.durable,
            current_tick,
            storage.as_ref(),
            &mut operator_states,
            &mut arrangement_states,
            &mut arrangement_keys_by_input,
            &mut eval_memo,
            &mut eval_memo_bytes,
            &mut memo_use_clock,
            &mut node_meta,
            &table_frontiers,
            &binding_frontiers,
            &durable_writes,
        )
        .await?;
        bump_input_frontiers_staged(
            &self.graph,
            &table_deltas,
            &binding_deltas,
            &mut table_frontiers,
            &mut binding_frontiers,
            &mut node_meta,
        );
        let affected_subscriptions = affected_nodes
            .iter()
            .filter_map(|node| self.subscriptions_by_output_node.get(node))
            .flatten()
            .copied()
            .collect::<HashSet<_>>();
        let metrics = TickMetrics {
            tick: current_tick,
            table_delta_records,
            subscriptions_considered: affected_subscriptions.len(),
            ..TickMetrics::default()
        };
        // Structured collectors own their positional edits. Only plain outputs
        // consume the generic before/after maps. Union demand across consumers
        // because a TopBy node can be shared by both kinds of output. Preserve
        // root_ordering_node metadata: hydration and scheduling still need it.
        let mut root_ordering_windows = HashMap::default();
        for subscription in affected_subscriptions
            .iter()
            .filter_map(|subscription| self.multisink_subscriptions.get(subscription))
        {
            for output in subscription
                .outputs
                .values()
                .filter(|output| affected_nodes.contains(&output.node))
            {
                if let Some(ordering_node) = output.root_ordering_node
                    && !output_is_structured_collect_by(&self.graph, output.node)?
                {
                    root_ordering_windows
                        .entry(ordering_node)
                        .or_insert_with(RootOrderingWindows::default);
                }
            }
        }
        // A routed TopBy runs before its barriers are known to be touched, so
        // it must collect positions for any bound output it may reach.
        for terminal in &activation.routed {
            if let Some(table) = self.graph.routes().table(*terminal) {
                for node in &table.root_ordering_nodes {
                    root_ordering_windows
                        .entry(*node)
                        .or_insert_with(RootOrderingWindows::default);
                }
            }
        }
        let retained_roots = activation
            .ephemeral
            .iter()
            .filter(|node| {
                self.node_meta.get(node).is_some_and(|meta| {
                    meta.retainers
                        .iter()
                        .any(|retainer| !matches!(retainer, Retainer::Hydration(_)))
                })
            })
            .copied()
            .collect::<Vec<_>>();
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
        active_roots.extend(retained_roots.iter().copied());
        let requests = EvaluationRequests::new();
        let evaluation_inputs = Some(EvaluationInputs::default());
        let work_queue =
            EvaluationWorkQueue::discover_frame(&self.graph, &self.node_meta, active_roots, false)?;
        eval_memo.set_layout(Arc::clone(&work_queue.layout));
        Ok(IncrementalEvaluation {
            table_deltas,
            binding_deltas,
            binding_snapshots,
            table_frontiers,
            binding_frontiers,
            current_tick,
            metrics,
            storage,
            requests,
            evaluation_inputs,
            work_queue,
            published_subscriptions: HashSet::default(),
            relevant_nodes,
            affected_nodes,
            affected_subscriptions,
            pending_subscription_outputs: HashMap::default(),
            terminal_deltas: HashMap::default(),
            root_ordering_windows,
            notification_publication,
            defer_notifications_until_durable,
            pending_resident_publication,
            operator_states,
            arrangement_states,
            arrangement_keys_by_input,
            eval_memo,
            eval_memo_bytes,
            memo_use_clock,
            node_meta,
            pending_binding_retractions,
            pending_notifications: Vec::new(),
            durable_writes,
            persist_flush: None,
            discarded: false,
            routed_terminals: activation.routed.clone(),
        })
    }

    pub(super) fn bump_input_frontiers(
        &mut self,
        table_deltas: &[TableDelta],
        binding_deltas: &[BindingDelta],
    ) {
        bump_input_frontiers_staged(
            &self.graph,
            table_deltas,
            binding_deltas,
            &mut self.table_frontiers,
            &mut self.binding_frontiers,
            &mut self.node_meta,
        );
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
        self.hydration_snapshot_with_root_values(
            output_node,
            storage,
            mode,
            RootIndirectValues::Materialize,
        )
        .await
    }

    pub(super) async fn hydration_snapshot_with_root_values<S>(
        &mut self,
        output_node: NodeId,
        storage: &S,
        mode: HydrationMode,
        root_indirect_values: RootIndirectValues,
    ) -> Result<RecordDeltas, IvmRuntimeError>
    where
        S: OrderedKvStorage,
    {
        self.hydration_roots_owned(
            [output_node],
            OwnedStorage::new(Rc::new(storage)),
            mode,
            None,
            None,
            root_indirect_values,
        )
        .await?
        .remove(&output_node)
        .ok_or(IvmRuntimeError::GraphNodeNotFound(output_node))
    }

    async fn hydration_roots_owned<'a>(
        &mut self,
        roots: impl IntoIterator<Item = NodeId>,
        owned_storage: OwnedStorage<'a>,
        mode: HydrationMode,
        binding_snapshots: Option<Arc<BindingSnapshots>>,
        binding_frontier_advance: Option<&str>,
        root_indirect_values: RootIndirectValues,
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
        session.root_indirect_values = root_indirect_values;
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
        binding_snapshots: Option<Arc<BindingSnapshots>>,
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
                RootIndirectValues::Materialize,
            )
            .await?;
        subscription_snapshot_from_hydrated(&self.graph, outputs, &hydrated, &HashMap::default())
    }

    fn output_depends_on_aggregate(&self, output_node: NodeId) -> Result<bool, IvmRuntimeError> {
        let mut ancestors = HashSet::new();
        self.graph.mark_ancestors(output_node, &mut ancestors);
        for ancestor in ancestors {
            let graph_node = self
                .graph
                .node(ancestor)
                .ok_or(IvmRuntimeError::GraphNodeNotFound(ancestor))?;
            if matches!(
                graph_node.descriptor.operator,
                OpType::Aggregate(_) | OpType::ArgMinBy(_) | OpType::ArgMaxBy(_)
            ) {
                return Ok(true);
            }
        }
        Ok(false)
    }

    #[allow(clippy::too_many_arguments)]
    async fn tick_durable_nodes(
        &self,
        table_deltas: &[TableDelta],
        binding_snapshots: &BindingSnapshots,
        durable_nodes: &[NodeId],
        current_tick: u64,
        storage: &dyn OrderedKvStorage,
        operator_states: &mut HashMap<OperatorStateKey, OperatorState>,
        arrangement_states: &mut HashMap<ArrangementKey, AsOf<ArrangementState, SubTick>>,
        arrangement_keys_by_input: &mut HashMap<NodeId, HashSet<ArrangementKey>>,
        eval_memo: &mut EvaluationMemo,
        eval_memo_bytes: &mut usize,
        memo_use_clock: &mut u64,
        node_meta: &mut HashMap<NodeId, NodeRuntimeMeta>,
        table_frontiers: &HashMap<String, u64>,
        binding_frontiers: &HashMap<BindingSourceKey, u64>,
        durable_writes: &RefCell<StagedWriteState>,
    ) -> Result<(), IvmRuntimeError> {
        let durable_overlay = StagedWriteOverlay::new(storage, durable_writes);
        let mut metrics = TickMetrics::default();
        for &node in durable_nodes {
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
                    binding_snapshots,
                    current_tick,
                    operator_states,
                    arrangement_states,
                    arrangement_keys_by_input,
                    eval_memo,
                    eval_memo_bytes,
                    table_frontiers,
                    binding_frontiers,
                    memo_use_clock,
                    node_meta,
                    storage: Some(&durable_overlay),
                    evaluation_inputs: None,
                    context: EvalContext::root(),
                    metrics: &mut metrics,
                    terminal_deltas: HashMap::default(),
                    root_ordering_windows: HashMap::default(),
                };
                evaluator.update_node(*input_node).await?.as_ref().clone()
            };
            apply_persist_delta(
                &durable_overlay,
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
    graph: &IvmGraph,
    outputs: &BTreeMap<String, CompiledNode>,
    hydrated: &HashMap<NodeId, RecordDeltas>,
    terminal_deltas: &HashMap<NodeId, TerminalDeltas>,
) -> Result<MultisinkDeltas, IvmRuntimeError> {
    let mut sinks = BTreeMap::new();
    let mut terminal_sinks = BTreeMap::new();
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
        let (has_public_collector, terminal) =
            terminal_delta_for_hydrated_output(graph, output.node, terminal_deltas)?;
        // A hydrated root collector has no before-image, but it still owns
        // the exact terminal key that later incremental edits address. Seed
        // its opening through that same terminal representation instead of
        // exposing a keyless record snapshot to a higher layer.
        let terminal = match terminal {
            Some(terminal) => Some(terminal),
            None if has_public_collector => Some(terminal_deltas_from_record_deltas(&records)?),
            None => None,
        };
        if let Some(terminal) = terminal.filter(|terminal| !terminal.is_empty()) {
            terminal_sinks.insert(sink.clone(), terminal);
        }
        sinks.insert(sink.clone(), records);
    }
    Ok(MultisinkDeltas {
        sinks,
        terminal_sinks,
    })
}

/// Locate the public collector that belongs to one output and return its
/// hydration seed operations. `Collect` renders a structured root tree while
/// `Root` renders a flat root record; both own an opaque terminal key and both
/// must seed an opening through the same terminal representation as updates.
fn terminal_delta_for_hydrated_output(
    graph: &IvmGraph,
    output: NodeId,
    terminal_deltas: &HashMap<NodeId, TerminalDeltas>,
) -> Result<(bool, Option<TerminalDeltas>), IvmRuntimeError> {
    let mut pending = vec![output];
    let mut seen = HashSet::new();
    let mut fallback = None;
    let mut has_public_collector = false;
    while let Some(node) = pending.pop() {
        if !seen.insert(node) {
            continue;
        }
        let graph_node = graph
            .node(node)
            .ok_or(IvmRuntimeError::GraphNodeNotFound(node))?;
        let is_public_collector = matches!(
            graph_node.descriptor.operator,
            OpType::CollectBy(ref collect_by)
                if matches!(collect_by.mode, CollectByMode::Collect | CollectByMode::Root)
        );
        has_public_collector |= is_public_collector;
        if let Some(terminal) = terminal_deltas.get(&node) {
            if is_public_collector {
                return Ok((true, Some(terminal.clone())));
            }
            fallback.get_or_insert_with(|| terminal.clone());
        }
        pending.extend(graph_node.descriptor.inputs.iter().copied());
    }
    Ok((
        has_public_collector,
        (!has_public_collector).then_some(fallback).flatten(),
    ))
}

/// Route barriers reached by this tick's shared-terminal deltas (#3288).
/// Any terminal whose delta cannot be read or keyed conservatively touches
/// all of its barriers, which is exactly the unrouted activation.
fn touched_route_barriers(
    evaluator: &mut TickEvaluator<'_>,
    graph: &IvmGraph,
    routed: &[NodeId],
    cx: &mut Context<'_>,
) -> HashSet<NodeId> {
    let mut touched = HashSet::default();
    for terminal in routed {
        let Some(table) = graph.routes().table(*terminal) else {
            continue;
        };
        let records = {
            let mut future = evaluator.update_node(*terminal);
            match Pin::new(&mut future).poll(cx) {
                Poll::Ready(Ok(records)) => Some(records),
                _ => None,
            }
        };
        let records =
            records.and_then(|records| evaluator.materialize_indirect_input(&records).ok());
        let Some(records) = records else {
            touched.extend(table.barriers());
            continue;
        };
        // Route fields are indices into the terminal's compiled output. A
        // delta in any other layout cannot be keyed by them safely.
        let layout_matches = graph
            .node(*terminal)
            .is_some_and(|node| node.descriptor.output.records() == records.descriptor);
        if !layout_matches {
            touched.extend(table.barriers());
            continue;
        }
        for delta in &records.deltas {
            let record = crate::records::BorrowedRecord::new(&delta.record, &records.descriptor);
            match table.key_of_record(&record) {
                Some(key) => {
                    if let Some(barriers) = table.by_key.get(&key) {
                        touched.extend(barriers.iter().copied());
                    }
                }
                None => {
                    touched.extend(table.barriers());
                    break;
                }
            }
        }
    }
    touched
}

fn bump_input_frontiers_staged(
    graph: &IvmGraph,
    table_deltas: &[TableDelta],
    binding_deltas: &[BindingDelta],
    table_frontiers: &mut HashMap<String, u64>,
    binding_frontiers: &mut HashMap<BindingSourceKey, u64>,
    node_meta: &mut HashMap<NodeId, NodeRuntimeMeta>,
) {
    let mut changed_tables = Vec::new();
    for delta in table_deltas.iter().filter(|delta| !delta.deltas.is_empty()) {
        *table_frontiers.entry(delta.table.clone()).or_default() += 1;
        changed_tables.push(delta.table.as_str());
    }
    let mut changed_bindings = Vec::new();
    for delta in binding_deltas
        .iter()
        .filter(|delta| !delta.deltas.is_empty() || delta.initializes_snapshot)
    {
        *binding_frontiers.entry(delta.key.clone()).or_default() += 1;
        changed_bindings.push(&delta.key);
    }
    if changed_tables.is_empty() && changed_bindings.is_empty() {
        return;
    }
    for &node in graph
        .affected_nodes(
            changed_tables.iter().copied(),
            changed_bindings.iter().copied(),
        )
        .iter()
    {
        let meta = node_meta.entry(node).or_default();
        meta.input_generation = meta.input_generation.wrapping_add(1);
    }
}

/// Fold graph-lifecycle state into an evaluation's `node_meta` snapshot just
/// before it replaces the live entries. Retainers are owned by lifecycle
/// operations, not by the snapshot: a subscription may subscribe or
/// unsubscribe while the evaluation is suspended, so keep their live value.
/// A node the graph no longer has was collected meanwhile; drop its snapshot
/// entry rather than resurrect metadata for a missing node.
fn carry_live_node_lifecycle(
    snapshot: &mut HashMap<NodeId, NodeRuntimeMeta>,
    runtime: &IvmRuntime,
    nodes: &HashSet<NodeId>,
) {
    for node in nodes {
        match (snapshot.get_mut(node), runtime.node_meta.get(node)) {
            (Some(meta), Some(live)) => {
                meta.retainers = live.retainers.clone();
                meta.input_generation = meta.input_generation.max(live.input_generation);
            }
            (None, Some(live)) => {
                snapshot.insert(*node, live.clone());
            }
            (Some(_), None) if runtime.graph.node(*node).is_none() => {
                snapshot.remove(node);
            }
            _ => {}
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::schema::{
        ColumnSchema, ColumnType, DatabaseSchema, IntegerKeyType, PrimaryKey, TableSchema,
    };
    use crate::storage::MemoryStorage;

    // Internal publication receipt: observing the final rows cannot detect
    // inserting every tick memo into runtime and immediately evicting it again.
    #[futures_test::test]
    async fn publication_retains_only_hydration_memos_with_exact_replacement_accounting() {
        let mut runtime = IvmRuntime::new(DatabaseSchema::new([])).unwrap();
        let descriptor = RecordDescriptor::new([("id", ValueType::U64)]);
        let node = runtime
            .add_dedup_graph(
                &GraphBuilder::values(descriptor.clone(), [vec![Value::U64(1)]]).unwrap(),
            )
            .unwrap()
            .node;
        let hydration_key = EvalMemoKey {
            scope: ScopeId::root(),
            node,
            input_signature_hash: 1,
            tick_epoch: None,
            sub_tick: 0,
            context_digest: 0,
        };
        let make_entry = |bytes| {
            EvalMemoEntry::new(
                Arc::new(RecordDeltas::empty(descriptor.clone())),
                0,
                bytes,
                0,
            )
        };
        runtime
            .eval_memo
            .insert(hydration_key.clone(), make_entry(3));
        runtime.eval_memo_bytes = 3;
        let storage = Rc::new(MemoryStorage::new(&[]).unwrap());
        let mut evaluation = runtime
            .begin_tick_with_params(Vec::new(), Vec::new(), OwnedStorage::new(storage), None)
            .await
            .unwrap();
        evaluation
            .eval_memo
            .set_layout(runtime.graph.execution_layout([node]).unwrap());
        evaluation
            .eval_memo
            .insert(hydration_key.clone(), make_entry(7));
        evaluation.eval_memo.insert(
            EvalMemoKey {
                tick_epoch: Some(evaluation.current_tick),
                ..hydration_key.clone()
            },
            make_entry(11),
        );
        evaluation.eval_memo_bytes = 18;
        evaluation.install(&mut runtime);
        assert_eq!(runtime.eval_memo.len(), 1);
        assert!(runtime.eval_memo.keys().all(|key| key.tick_epoch.is_none()));
        assert_eq!(
            runtime.eval_memo.get(&hydration_key).unwrap().payload_bytes,
            7
        );
        assert_eq!(runtime.eval_memo_bytes, 7);
    }

    // Internal mechanism receipt: identical public rows cannot prove that the
    // queue removed intermediate tasks, or that live sharing invalidates that
    // choice without invalidating immutable graph topology.
    #[test]
    fn physical_pipeline_contraction_respects_live_retained_and_shared_boundaries() {
        let mut runtime = IvmRuntime::new(DatabaseSchema::new([])).unwrap();
        let input = GraphBuilder::values(
            RecordDescriptor::new([("id", ValueType::U64)]),
            [vec![Value::U64(1)]],
        )
        .unwrap();
        let prefix = input.filter(PredicateExpr::gt("id", Value::U64(0)));
        let root = runtime
            .add_dedup_graph(
                &prefix
                    .clone()
                    .filter(PredicateExpr::gt("id", Value::U64(0))),
            )
            .unwrap()
            .node;
        let middle = runtime.graph.node(root).unwrap().descriptor.inputs[0];
        let discover = |runtime: &IvmRuntime| {
            EvaluationWorkQueue::discover(&runtime.graph, &runtime.node_meta, [root], false)
                .unwrap()
                .1
        };
        let mut queue = discover(&runtime);
        assert_eq!(queue.pipelines[&root].as_ref(), &[middle, root]);
        assert_eq!(
            queue.entries[queue.layout.slots[&middle]],
            EvaluationEntry::Fused
        );
        assert!(!queue.is_complete(middle));
        queue.complete(root);
        assert!(queue.is_complete(middle));
        assert_eq!(queue.drain_completed_events(), [middle, root]);

        runtime.add_retainer(middle, Retainer::PreparedShape("observer".into()));
        assert!(discover(&runtime).pipelines.is_empty());
        runtime.remove_retainer(middle, &Retainer::PreparedShape("observer".into()));
        assert!(!discover(&runtime).pipelines.is_empty());
        let old_layout = runtime.graph.execution_layout([root]).unwrap();
        runtime
            .add_dedup_graph(&prefix.filter(PredicateExpr::gt("id", Value::U64(2))))
            .unwrap();
        assert!(Arc::ptr_eq(
            &old_layout,
            &runtime.graph.execution_layout([root]).unwrap()
        ));
        assert!(
            discover(&runtime).pipelines.is_empty(),
            "a consumer outside this layout still owns the intermediate"
        );
    }

    #[futures_test::test]
    async fn resumed_install_preserves_live_retainer_changes() {
        let schema = DatabaseSchema::new([TableSchema::new(
            "edges",
            [
                ColumnSchema::new("id", ColumnType::U64),
                ColumnSchema::new("src", ColumnType::U64),
                ColumnSchema::new("dst", ColumnType::U64),
            ],
        )
        .with_primary_key(PrimaryKey::new("id", IntegerKeyType::U64))]);
        let mut runtime = IvmRuntime::new(schema.clone()).unwrap();
        let storage =
            Rc::new(MemoryStorage::new(&["edges"]).expect("valid memory storage families"));
        let output = runtime
            .add_dedup_graph(&GraphBuilder::table("edges").project(["src", "dst"]))
            .unwrap()
            .node;
        runtime.add_retainer(output, Retainer::PreparedShape("old".to_owned()));
        let edges = schema.table("edges").unwrap().record_schema();
        let record = edges
            .create(&[Value::U64(1), Value::U64(1), Value::U64(2)])
            .unwrap();
        let mut evaluation = runtime
            .begin_tick_with_params(
                vec![TableDelta {
                    variant_tag: 0,
                    table: "edges".to_owned(),
                    descriptor: edges,
                    deltas: vec![RecordDelta {
                        record: record.into(),
                        weight: 1,
                    }],
                }],
                Vec::new(),
                OwnedStorage::new(Rc::clone(&storage)),
                None,
            )
            .await
            .unwrap();

        runtime.add_retainer(output, Retainer::Subscription("new".to_owned()));
        evaluation.install(&mut runtime);

        let retainers = &runtime.node_meta.get(&output).unwrap().retainers;
        assert!(retainers.contains(&Retainer::PreparedShape("old".to_owned())));
        assert!(retainers.contains(&Retainer::Subscription("new".to_owned())));
    }

    #[futures_test::test]
    async fn abandoned_resident_evaluation_cannot_install_frontiers() {
        let schema = DatabaseSchema::new([TableSchema::new(
            "edges",
            [
                ColumnSchema::new("id", ColumnType::U64),
                ColumnSchema::new("src", ColumnType::U64),
                ColumnSchema::new("dst", ColumnType::U64),
            ],
        )
        .with_primary_key(PrimaryKey::new("id", IntegerKeyType::U64))]);
        let mut runtime = IvmRuntime::new(schema.clone()).unwrap();
        let storage =
            Rc::new(MemoryStorage::new(&["edges"]).expect("valid memory storage families"));
        let output = runtime
            .add_dedup_graph(&GraphBuilder::table("edges").project(["src", "dst"]))
            .unwrap()
            .node;
        runtime.add_retainer(output, Retainer::PreparedShape("retained".to_owned()));
        let edges = schema.table("edges").unwrap().record_schema();
        let record = edges
            .create(&[Value::U64(1), Value::U64(1), Value::U64(2)])
            .unwrap();
        let mut evaluation = runtime
            .begin_tick_with_params(
                vec![TableDelta {
                    variant_tag: 0,
                    table: "edges".to_owned(),
                    descriptor: edges,
                    deltas: vec![RecordDelta {
                        record: record.into(),
                        weight: 1,
                    }],
                }],
                Vec::new(),
                OwnedStorage::new(Rc::clone(&storage)),
                None,
            )
            .await
            .unwrap();
        let before_tick = runtime.current_tick;
        let before_frontiers = runtime.table_frontiers.clone();
        evaluation.abandon(&HashSet::default());
        evaluation.install(&mut runtime);

        assert_eq!(runtime.current_tick, before_tick);
        assert_eq!(runtime.table_frontiers, before_frontiers);
    }
}
