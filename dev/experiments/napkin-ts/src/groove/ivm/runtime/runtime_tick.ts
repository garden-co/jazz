// Tick orchestration, hydration, memo eviction, and durable-node evaluation.
import { KeyedMap, SortedMap, cmpStr, toHex } from "../../../support/index.js";
import {
  IvmRuntimeError,
  EVAL_MEMO_MAX_BYTES,
  EVAL_MEMO_MAX_ENTRIES,
  type IvmRuntime,
} from "./index.js";
import {
  type ArrangementKey,
  type AsOf,
  type OperatorStateKey,
  type SubTick,
  EvalContext,
  HydrationMode,
  ScopeId,
  TickMetrics,
  arrangementKeyString,
  operatorStateKeyString,
  rootStateKey,
} from "./state.js";
import {
  TerminalDeltas,
  orderTerminalSnapshot,
  terminalDeltasFromRecordDeltas,
  terminalDeltasKeyedByIdentity,
} from "./terminal.js";
import { applyPersistDelta } from "./persist.js";
import {
  type BindingDelta,
  type BindingSnapshots,
  type CompiledNode,
  type InitialSlot,
  type MultisinkSubscriptionState,
  type PublicationId,
  type QueuedMultisinkDeltas,
  type RecordDelta,
  type RecordDeltas,
  type RootIndirectValues,
  type SubscriptionId,
  type TableDelta,
  MATERIALIZE,
  MultisinkDeltas,
  SubscriptionError,
  SubscriptionLifetime,
  bindingSnapshotDeltas,
  indexSubscriptionOutputs,
  materializedFieldIndices,
  multisinkDeltasEncodedBytes,
  multisinkDeltasRecordCount,
  rootIndirectValuesEqual,
  unindexSubscriptionOutputs,
  unsubscribe,
} from "./subscriptions.js";
import {
  type NodeId,
  type IvmGraph,
  type ExecutionLayout,
  type OperatorState,
  type ArrangementState,
  type NodeRuntimeMeta,
  type BindingSourceKey,
  type OrderedKvStorage,
  type EvaluationRequestKey,
  type OwnedChunkProvider,
  BSK,
  BorrowedRecord,
  EvaluationInputs,
  EvaluationMemo,
  EvaluationRequests,
  NodeState,
  OwnedStorage,
  RootOrderingWindows,
  StagedWriteOverlay,
  StagedWriteState,
  TickEvaluator,
  ChunkError,
  cloneArrangement,
  cloneNodeMeta,
  cloneOperatorState,
  collectUnretainedEphemeralNodes,
  defaultNodeMeta,
  evaluationRequestKeyString,
  largeValues,
  outputConsumesRootPositions,
  recordHydrationMemoMetrics,
  rootIdentityFields,
  runtimeStats,
  cheapRuntimeStats,
  affectedRecursiveNodesAreCurrent,
  encodedIdentityKeyPart,
} from "../../../_extern/groove-ivm-tick.js";

// ---- poll plumbing (Rust Context/Poll/Waker) ----

export const PENDING: unique symbol = Symbol("pending");
export type Poll<T> = T | typeof PENDING;
export type Waker = () => void;
export const noopWaker: Waker = () => {};

/** `std::future::poll_fn(..).await`: re-run `f` whenever it wakes itself. */
export function pollFn<T>(f: (cx: Waker) => Poll<T>): Promise<T> {
  return new Promise((resolve, reject) => {
    let scheduled = false;
    const wake = () => {
      if (scheduled) return;
      scheduled = true;
      queueMicrotask(run);
    };
    const run = () => {
      scheduled = false;
      try {
        const r = f(wake);
        if (r !== PENDING) resolve(r);
      } catch (e) {
        reject(e);
      }
    };
    run();
  });
}

/** Owner turns stay bounded so a browser worker returns to ingress between slices. */
const MAX_HYDRATION_RUNNABLE_NODES_PER_POLL = 32;

type StateMaps = {
  operatorStates: KeyedMap<OperatorStateKey, OperatorState>;
  arrangementStates: KeyedMap<ArrangementKey, AsOf<ArrangementState, SubTick>>;
  arrangementKeysByInput: Map<NodeId, KeyedMap<ArrangementKey, ArrangementKey>>;
};

const newOperatorStates = () =>
  new KeyedMap<OperatorStateKey, OperatorState>(operatorStateKeyString);
const newArrangementStates = () =>
  new KeyedMap<ArrangementKey, AsOf<ArrangementState, SubTick>>(arrangementKeyString);
const newKeySet = () => new KeyedMap<ArrangementKey, ArrangementKey>(arrangementKeyString);

function retainKeyed<K, V>(map: KeyedMap<K, V>, keep: (k: K, v: V) => boolean): void {
  for (const [k, v] of [...map]) if (!keep(k, v)) map.delete(k);
}
function retainMap<K, V>(map: Map<K, V>, keep: (k: K, v: V) => boolean): void {
  for (const [k, v] of [...map]) if (!keep(k, v)) map.delete(k);
}
function addArrangementKey(
  map: Map<NodeId, KeyedMap<ArrangementKey, ArrangementKey>>,
  input: NodeId,
  key: ArrangementKey,
) {
  let keys = map.get(input);
  if (!keys) map.set(input, (keys = newKeySet()));
  keys.set(key, key);
}

/** Capture root-scoped state of `nodes` from the live runtime. */
function stageStateForNodes(rt: IvmRuntime, nodes: Iterable<NodeId>): StateMaps {
  const operatorStates = newOperatorStates();
  const arrangementStates = newArrangementStates();
  const arrangementKeysByInput = new Map<NodeId, KeyedMap<ArrangementKey, ArrangementKey>>();
  for (const node of nodes) {
    const key = rootStateKey(node);
    const state = rt.operatorStates.get(key);
    if (state) operatorStates.set(key, cloneOperatorState(state));
    const keys = rt.arrangementKeysByInput.get(node);
    if (!keys) continue;
    for (const [akey] of keys) {
      const astate = rt.arrangementStates.get(akey);
      if (!astate) continue;
      arrangementStates.set(akey, cloneArrangement(astate));
      addArrangementKey(arrangementKeysByInput, node, akey);
    }
  }
  return { operatorStates, arrangementStates, arrangementKeysByInput };
}

function sumPayloadBytes(memo: EvaluationMemo): number {
  let n = 0;
  for (const entry of memo.values()) n += entry.payloadBytes;
  return n;
}

// ---- evaluation failures ----

export enum EvaluationFailureKind {
  Scoped,
  Fatal,
}

export class EvaluationFailure {
  constructor(
    readonly kind: EvaluationFailureKind,
    readonly affectedNodes: Set<NodeId>,
    readonly error: IvmRuntimeError,
  ) {}
  static fatal(error: unknown): EvaluationFailure {
    if (error instanceof EvaluationFailure) return error;
    return new EvaluationFailure(
      EvaluationFailureKind.Fatal,
      new Set(),
      IvmRuntimeError.from(error),
    );
  }
}

// ---- pending publication plumbing ----

/** One root output retained while its publication waits for immutable chunks. */
class PendingSubscriptionOutput {
  materialized: [RootIndirectValues, RecordDeltas][] = [];
  constructor(readonly physical: RecordDeltas) {}
  materializedAs(representation: RootIndirectValues): RecordDeltas | undefined {
    return this.materialized.find(([c]) => rootIndirectValuesEqual(c, representation))?.[1];
  }
  recordMaterialized(representation: RootIndirectValues, records: RecordDeltas): void {
    if (!this.materializedAs(representation)) this.materialized.push([representation, records]);
  }
}

type PendingResidentPublication = {
  publication: { value: PublicationId | undefined };
  notifications: [SubscriptionId, QueuedMultisinkDeltas][];
  completed: { value: boolean };
  deferNotificationsUntilDurable: boolean;
  chunkProvider: OwnedChunkProvider | undefined;
};

type DurableWrites = { state: StagedWriteState };

export class ResidentTick {
  constructor(
    readonly metrics: TickMetrics,
    readonly publication: PendingResidentPublication,
    private readonly durableWrites: DurableWrites,
  ) {}
  takeDurableWrites() {
    const state = this.durableWrites.state;
    this.durableWrites.state = new StagedWriteState();
    return state.intoOperations();
  }
}

enum DetachOn {
  /** Any external request, storage or chunk (resident writes). */
  AnyRequest,
  /** Only a large-value chunk fetch. */
  ChunkRequest,
}

// ---- pending evaluation queue ----

class PendingIncrementalState {
  evaluations = new Map<number, PendingEvaluation>();
  order: number[] = [];
  waitersByNode = new Map<NodeId, number[]>();
  nextId = 0;

  /** Hydrations overlapping `nodes`, plus every evaluation temporally ahead of one. */
  hydrationAdmissionEvaluations(nodes: Set<NodeId>): Set<number> {
    const selected = new Set<number>();
    for (const [id, evaluation] of this.evaluations) {
      if (
        evaluation instanceof PendingSubscriptionHydration &&
        evaluation.workQueueRef().overlaps(nodes)
      )
        selected.add(id);
    }
    for (;;) {
      let changed = false;
      for (const waiters of this.waitersByNode.values()) {
        let last = -1;
        for (let i = waiters.length - 1; i >= 0; i--) {
          if (selected.has(waiters[i])) {
            last = i;
            break;
          }
        }
        for (let i = 0; i <= last; i++) {
          if (!selected.has(waiters[i])) {
            selected.add(waiters[i]);
            changed = true;
          }
        }
      }
      if (!changed) return selected;
    }
  }

  /** Release evaluations queued behind `evaluationId` at completed nodes. */
  releaseTemporalSuccessors(rt: IvmRuntime, evaluationId: number, nodes: Iterable<NodeId>): void {
    for (const node of nodes) {
      const waiters = this.waitersByNode.get(node);
      if (!waiters) continue;
      waiters.shift();
      const successor = waiters[0];
      if (!waiters.length) this.waitersByNode.delete(node);
      if (successor !== undefined) this.evaluations.get(successor)?.temporalReady(rt, node);
    }
  }

  /** Remove `evaluationId` from the waiters of `nodes`, releasing successors. */
  releaseWaiter(
    rt: IvmRuntime,
    evaluationId: number,
    nodes: Iterable<NodeId>,
    onlyIfFront: boolean,
  ): void {
    for (const node of nodes) {
      const waiters = this.waitersByNode.get(node);
      if (!waiters) continue;
      const wasFront = waiters[0] === evaluationId;
      const remaining = waiters.filter((w) => w !== evaluationId);
      if (remaining.length) this.waitersByNode.set(node, remaining);
      else this.waitersByNode.delete(node);
      const successor = remaining[0];
      if ((wasFront || !onlyIfFront) && successor !== undefined)
        this.evaluations.get(successor)?.temporalReady(rt, node);
    }
  }

  register(evaluationId: number, nodes: Iterable<NodeId>): void {
    for (const node of nodes) {
      let waiters = this.waitersByNode.get(node);
      if (!waiters) this.waitersByNode.set(node, (waiters = []));
      waiters.push(evaluationId);
    }
  }
}

type PendingEvaluation = IncrementalEvaluation | PendingSubscriptionHydration;

/** Shared slot for queued evaluations (Rc<RefCell<..>> in Rust). */
export class PendingIncrementalEvaluation {
  state = new PendingIncrementalState();
  isPending(): boolean {
    return this.state.order.length > 0;
  }
  hasResidentContinuation(): boolean {
    for (const e of this.state.evaluations.values()) if (e.hasResidentContinuation()) return true;
    return false;
  }
  /** Nodes referenced by queued continuations remain graph-live. */
  registeredNodes(): Set<NodeId> {
    const out = new Set<NodeId>();
    for (const e of this.state.evaluations.values())
      for (const n of e.workQueueRef().registeredNodes()) out.add(n);
    return out;
  }
}

// ---- work queue ----

/** Per-slot scheduling state; Waiting carries its remaining dependency count. */
const FUSED = -1;
const RUNNABLE = -2;
const COMPLETE = -3;
type EvaluationEntry = number;

/** Dependency-ordered discovery of request-producing leaves and runnable nodes. */
class EvaluationWorkQueue {
  unaryBatches = new Map<NodeId, any>();
  pipelines = new Map<NodeId, NodeId[]>();
  pipelineBatches = new Map<NodeId, any>();
  taskSlots: number[];
  entries: EvaluationEntry[];
  requestDependents = new KeyedMap<EvaluationRequestKey, NodeId[]>(evaluationRequestKeyString);
  runnable: NodeId[] = [];
  completedEvents: NodeId[] = [];
  temporalWaiting: number[];
  /** Nodes whose completion an earlier frame already published (#3306). */
  releasedByEarlierFrame = new Set<NodeId>();

  private constructor(readonly layout: ExecutionLayout) {
    this.taskSlots = layout.nodes.map((_: NodeId, i: number) => i);
    this.entries = layout.inputCounts.slice();
    this.temporalWaiting = layout.nodes.map(() => 0);
  }

  static discover(
    graph: IvmGraph,
    nodeMeta: Map<NodeId, NodeRuntimeMeta>,
    roots: Iterable<NodeId>,
    hydrateSources: boolean,
  ): [Set<NodeId>, EvaluationWorkQueue] {
    const queue = EvaluationWorkQueue.discoverFrame(graph, nodeMeta, roots, hydrateSources);
    return [new Set(queue.layout.nodes), queue];
  }

  static discoverFrame(
    graph: IvmGraph,
    nodeMeta: Map<NodeId, NodeRuntimeMeta>,
    roots: Iterable<NodeId>,
    hydrateSources: boolean,
  ): EvaluationWorkQueue {
    const layout = graph.executionLayout([...roots]);
    if (layout.missing !== undefined) throw IvmRuntimeError.of("GraphNodeNotFound", layout.missing);
    const queue = new EvaluationWorkQueue(layout);
    const n = layout.nodes.length;
    // Contract physical tasks, not graph identity: shared or retained
    // intermediates stay independently executable.
    const predecessors: (number | undefined)[] = layout.pipelinePredecessors.map(
      (p: number | undefined) => {
        if (p === undefined) return undefined;
        const id = layout.nodes[p];
        const node = graph.node(id);
        if (!node) throw new Error("layout node");
        const meta = nodeMeta.get(id);
        return !node.isDurable() &&
          node.children.length === 1 &&
          (!meta || meta.retainers.length === 0)
          ? p
          : undefined;
      },
    );
    const interior = new Array<boolean>(n).fill(false);
    for (const p of predecessors) if (p !== undefined) interior[p] = true;
    for (let tail = 0; tail < n; tail++) {
      if (interior[tail] || predecessors[tail] === undefined) continue;
      const members = [layout.nodes[tail]];
      let cursor = tail;
      for (let prev = predecessors[cursor]; prev !== undefined; prev = predecessors[cursor]) {
        queue.taskSlots[prev] = tail;
        queue.entries[prev] = FUSED;
        members.push(layout.nodes[prev]);
        cursor = prev;
      }
      members.reverse();
      queue.pipelines.set(layout.nodes[tail], members);
    }
    if (hydrateSources) {
      for (const slot of layout.sourceSlots) {
        const nodeId = layout.nodes[slot];
        const node = graph.node(nodeId);
        if (!node) throw IvmRuntimeError.of("GraphNodeNotFound", nodeId);
        const op = node.descriptor.operator;
        let request;
        if (op.kind === "TableSource") request = NodeState.tableSourceRequest(op.op);
        else if (op.kind === "IndexSource") request = NodeState.indexSourceRequest(op.op);
        else throw new Error("layout source slot is not a storage source");
        if (request !== undefined) {
          queue.requestDependents.getOrInsert({ kind: "Storage", request }, () => []).push(nodeId);
          queue.entries[slot] = 1;
        }
      }
    }
    for (let slot = 0; slot < queue.entries.length; slot++)
      if (queue.entries[slot] === 0) queue.makeSlotRunnable(slot);
    return queue;
  }

  requests(): EvaluationRequestKey[] {
    return [...this.requestDependents].map(([k]) => k);
  }

  /** Dispatch with direct compiled input slots of this private frame. */
  pollNode(evaluator: TickEvaluator, node: NodeId, cx: Waker): Poll<RecordDeltas> {
    const slot = this.layout.slots.get(node)!;
    const pipeline = this.pipelines.get(node);
    if (pipeline) {
      const head = this.layout.slots.get(pipeline[0])!;
      return evaluator.pollPipeline(
        pipeline,
        this.pipelineBatches,
        { slots: this.layout.inputs(head) },
        cx,
      );
    }
    return evaluator.pollReadyNode(
      node,
      this.unaryBatches,
      { slots: this.layout.inputs(slot) },
      cx,
    );
  }

  dependencyReady(node: NodeId): void {
    const slot = this.layout.slots.get(node);
    if (slot !== undefined) this.slotDependencyReady(slot);
  }

  slotDependencyReady(slot: number): void {
    const task = this.taskSlots[slot];
    const remaining = this.entries[task];
    if (remaining < 0) return;
    const next = Math.max(0, remaining - 1);
    this.entries[task] = next;
    if (next === 0) this.makeSlotRunnable(task);
  }

  requestsReady(requests: Iterable<EvaluationRequestKey>): void {
    for (const request of requests) {
      const nodes = this.requestDependents.get(request);
      if (!nodes) continue;
      this.requestDependents.delete(request);
      for (const node of nodes) this.dependencyReady(node);
    }
  }

  storageAlreadyResident(residentNodes: Set<NodeId>): void {
    const ready: NodeId[] = [];
    for (const [key, dependents] of [...this.requestDependents]) {
      const kept = dependents.filter((node) => {
        if (!residentNodes.has(node)) return true;
        ready.push(node);
        return false;
      });
      if (kept.length) this.requestDependents.set(key, kept);
      else this.requestDependents.delete(key);
    }
    for (const node of ready) this.dependencyReady(node);
  }

  waitForRequests(node: NodeId, requests: EvaluationRequestKey[]): void {
    this.entries[this.layout.slots.get(node)!] = requests.length;
    for (const request of requests) {
      const dependents = this.requestDependents.getOrInsert(request, () => []);
      if (!dependents.includes(node)) dependents.push(node);
    }
  }

  makeSlotRunnable(slot: number): void {
    if (this.entries[slot] === RUNNABLE || this.entries[slot] === COMPLETE) return;
    this.entries[slot] = RUNNABLE;
    this.runnable.push(this.layout.nodes[slot]);
  }

  complete(node: NodeId): void {
    const slot = this.layout.slots.get(node)!;
    // Publish completion only after the entire task succeeds.
    const members = this.pipelines.get(node);
    if (members) {
      for (const member of members) {
        this.entries[this.layout.slots.get(member)!] = COMPLETE;
        if (!this.releasedByEarlierFrame.has(member)) this.completedEvents.push(member);
      }
    } else if (!this.releasedByEarlierFrame.has(node)) this.completedEvents.push(node);
    this.entries[slot] = COMPLETE;
    for (const dependent of this.layout.dependents(slot)) this.slotDependencyReady(dependent);
  }

  /** Retain a runnable node after its private step deliberately yields. */
  requeueYielded(node: NodeId): void {
    this.entries[this.layout.slots.get(node)!] = RUNNABLE;
    this.runnable.unshift(node);
  }

  hasResidentContinuation(): boolean {
    return this.runnable.length > 0;
  }
  isRoot(node: NodeId): boolean {
    return this.layout.roots.includes(node);
  }
  isComplete(node: NodeId): boolean {
    const slot = this.layout.slots.get(node);
    return slot !== undefined && this.entries[slot] === COMPLETE;
  }
  rootsComplete(): boolean {
    return this.layout.roots.every((n: NodeId) => this.isComplete(n));
  }
  incompleteNodes(): NodeId[] {
    return this.layout.nodes.filter((_: NodeId, slot: number) => this.entries[slot] !== COMPLETE);
  }
  completeNodes(): NodeId[] {
    return this.layout.nodes.filter((_: NodeId, slot: number) => this.entries[slot] === COMPLETE);
  }
  /** A hydration's entire graph slice remains a temporal barrier until install. */
  registeredNodes(): NodeId[] {
    return this.layout.nodes.slice();
  }
  overlaps(nodes: Set<NodeId>): boolean {
    return this.layout.nodes.some((n: NodeId) => nodes.has(n));
  }

  downstreamClosure(roots: Iterable<NodeId>): Set<NodeId> {
    const affected = new Set<NodeId>();
    const pending = [...roots];
    while (pending.length) {
      const node = pending.shift()!;
      if (affected.has(node)) continue;
      affected.add(node);
      const slot = this.layout.slots.get(node);
      if (slot !== undefined)
        for (const d of this.layout.dependents(slot)) pending.push(this.layout.nodes[d]);
    }
    return affected;
  }

  failureForRequest(
    request: EvaluationRequestKey,
    failure: { publicationMetadataDurability: boolean; error: unknown },
  ): EvaluationFailure {
    return new EvaluationFailure(
      failure.publicationMetadataDurability
        ? EvaluationFailureKind.Fatal
        : EvaluationFailureKind.Scoped,
      this.downstreamClosure(this.requestDependents.get(request) ?? []),
      IvmRuntimeError.from(failure.error),
    );
  }

  failureForNode(node: NodeId, error: unknown): EvaluationFailure {
    return new EvaluationFailure(
      EvaluationFailureKind.Scoped,
      this.downstreamClosure([node]),
      IvmRuntimeError.from(error),
    );
  }

  abandon(nodes: Set<NodeId>): void {
    retainMap(this.unaryBatches, (n) => !nodes.has(n));
    retainMap(this.pipelineBatches, (n) => !nodes.has(n));
    this.runnable = this.runnable.filter((n) => !nodes.has(n));
    retainKeyed(this.requestDependents, (_, deps) => !deps.every((n) => nodes.has(n)));
    for (const node of nodes) {
      const slot = this.layout.slots.get(node);
      if (slot === undefined) continue;
      for (const member of this.pipelines.get(node) ?? [])
        this.entries[this.layout.slots.get(member)!] = COMPLETE;
      this.entries[slot] = COMPLETE;
      this.temporalWaiting[slot] = 0;
    }
  }

  addTemporalBlockers(blockers: Map<NodeId, number>): void {
    for (let slot = 0; slot < this.entries.length; slot++) {
      const count = blockers.get(this.layout.nodes[slot]) ?? 0;
      if (count === 0) continue;
      this.temporalWaiting[slot] = count;
      const task = this.taskSlots[slot];
      const entry = this.entries[task];
      if (entry === RUNNABLE) {
        const taskNode = this.layout.nodes[task];
        this.runnable = this.runnable.filter((c) => c !== taskNode);
        this.entries[task] = count;
      } else if (entry >= 0) this.entries[task] = entry + count;
      else if (entry === FUSED) throw new Error("canonical task slot");
    }
  }

  /** Whether `node` still waits on a predecessor and has not been evaluated. */
  isTemporallyWaiting(node: NodeId): boolean {
    const slot = this.layout.slots.get(node);
    return (
      slot !== undefined &&
      this.temporalWaiting[slot] > 0 &&
      this.entries[this.taskSlots[slot]] !== COMPLETE
    );
  }

  temporalReady(node: NodeId): void {
    const slot = this.layout.slots.get(node);
    if (slot === undefined || this.temporalWaiting[slot] === 0) return;
    this.temporalWaiting[slot]--;
    this.slotDependencyReady(slot);
  }

  hasTemporalWaiting(): boolean {
    return this.temporalWaiting.length > 0;
  }

  drainCompletedEvents(): NodeId[] {
    const events = this.completedEvents;
    this.completedEvents = [];
    return events;
  }

  /** Before first registering as a waiter, nothing completed has been released. */
  discardUnregisteredCompletions(): void {
    this.completedEvents = [];
    this.releasedByEarlierFrame.clear();
  }

  /** Seed a later frame with the earlier frame's completed nodes (#3306). */
  carryEarlierFrame(carried: Set<NodeId>, released: Set<NodeId>): void {
    const tails: number[] = [];
    for (let slot = 0; slot < this.entries.length; slot++) {
      if (this.taskSlots[slot] !== slot) continue;
      const members = this.pipelines.get(this.layout.nodes[slot]);
      if (members ? members.every((m) => carried.has(m)) : carried.has(this.layout.nodes[slot]))
        tails.push(slot);
    }
    // Mark every carried task complete before releasing dependents.
    for (const slot of tails) {
      for (const member of this.pipelines.get(this.layout.nodes[slot]) ?? [])
        this.entries[this.layout.slots.get(member)!] = COMPLETE;
      this.entries[slot] = COMPLETE;
    }
    this.runnable = this.runnable.filter(
      (n) => this.entries[this.layout.slots.get(n)!] !== COMPLETE,
    );
    for (const slot of tails)
      for (const dependent of this.layout.dependents(slot)) this.slotDependencyReady(dependent);
    this.releasedByEarlierFrame = released;
  }
}

// ---- incremental evaluation ----

type EvaluatorScratch = {
  operatorStates: KeyedMap<OperatorStateKey, OperatorState>;
  arrangementStates: KeyedMap<ArrangementKey, AsOf<ArrangementState, SubTick>>;
  arrangementKeysByInput: Map<NodeId, KeyedMap<ArrangementKey, ArrangementKey>>;
  evalMemo: EvaluationMemo;
  evalMemoBytes: number;
  memoUseClock: number;
  nodeMeta: Map<NodeId, NodeRuntimeMeta>;
};

/** Build a TickEvaluator over `scratch`; `takeBack` copies its scalar counters home. */
function makeEvaluator(
  rt: IvmRuntime,
  scratch: EvaluatorScratch,
  fields: {
    tableDeltas: TableDelta[];
    bindingDeltas: BindingDelta[];
    bindingSnapshots: BindingSnapshots;
    currentTick: number;
    tableFrontiers: Map<string, number>;
    bindingFrontiers: KeyedMap<BindingSourceKey, number> | Map<any, number>;
    storage: any;
    evaluationInputs: EvaluationInputs | undefined;
    context: EvalContext;
    metrics: TickMetrics;
    terminalDeltas: Map<NodeId, TerminalDeltas>;
    rootOrderingWindows: Map<NodeId, RootOrderingWindows>;
  },
): TickEvaluator {
  return new TickEvaluator({
    schema: rt.schema,
    graph: rt.graph,
    variantProjections: rt.variantProjections,
    operatorStates: scratch.operatorStates,
    arrangementStates: scratch.arrangementStates,
    arrangementKeysByInput: scratch.arrangementKeysByInput,
    evalMemo: scratch.evalMemo,
    evalMemoBytes: scratch.evalMemoBytes,
    memoUseClock: scratch.memoUseClock,
    nodeMeta: scratch.nodeMeta,
    ...fields,
  });
}
function takeBack(scratch: EvaluatorScratch, evaluator: TickEvaluator): void {
  scratch.evalMemoBytes = evaluator.evalMemoBytes;
  scratch.memoUseClock = evaluator.memoUseClock;
}

function requestChunkProvider(rt: IvmRuntime, pending: PendingResidentPublication | undefined) {
  return pending?.chunkProvider ?? rt.chunkProvider;
}

export class IncrementalEvaluation implements EvaluatorScratch {
  evaluationInputs: EvaluationInputs | undefined = new EvaluationInputs();
  requests = new EvaluationRequests();
  publishedSubscriptions = new Set<SubscriptionId>();
  /** Subscriptions whose hydration this write set aside (#3901). */
  restartedSubscriptions = new Set<SubscriptionId>();
  pendingSubscriptionOutputs = new Map<NodeId, PendingSubscriptionOutput>();
  terminalDeltas = new Map<NodeId, TerminalDeltas>();
  pendingNotifications: [SubscriptionId, QueuedMultisinkDeltas][] = [];
  persistFlush: { done: boolean; error?: unknown; waker?: Waker } | undefined;
  discarded = false;
  /** Completed nodes already written back while parked (#3815). */
  earlyInstalled = new Set<NodeId>();
  /** Completed nodes held as temporal barriers until install. */
  heldBarriers = new Set<NodeId>();

  constructor(
    public tableDeltas: TableDelta[],
    public bindingDeltas: BindingDelta[],
    public bindingSnapshots: BindingSnapshots,
    public tableFrontiers: Map<string, number>,
    public bindingFrontiers: KeyedMap<BindingSourceKey, number>,
    public currentTick: number,
    public metrics: TickMetrics,
    public storage: OwnedStorage,
    public workQueue: EvaluationWorkQueue,
    public relevantNodes: Set<NodeId>,
    public affectedSubscriptions: Set<SubscriptionId>,
    public affectedNodes: Set<NodeId>,
    public rootOrderingWindows: Map<NodeId, RootOrderingWindows>,
    public notificationPublication: PublicationId | undefined,
    public deferNotificationsUntilDurable: boolean,
    public pendingResidentPublication: PendingResidentPublication | undefined,
    public operatorStates: KeyedMap<OperatorStateKey, OperatorState>,
    public arrangementStates: KeyedMap<ArrangementKey, AsOf<ArrangementState, SubTick>>,
    public arrangementKeysByInput: Map<NodeId, KeyedMap<ArrangementKey, ArrangementKey>>,
    public evalMemo: EvaluationMemo,
    public evalMemoBytes: number,
    public memoUseClock: number,
    public nodeMeta: Map<NodeId, NodeRuntimeMeta>,
    public pendingBindingRetractions: number,
    public durableWrites: DurableWrites,
    /** Affected shared terminals whose route barriers await this tick's deltas (#3288). */
    public routedTerminals: NodeId[],
  ) {}

  workQueueRef(): EvaluationWorkQueue {
    return this.workQueue;
  }
  hasResidentContinuation(): boolean {
    return this.workQueue.hasResidentContinuation();
  }
  /** Release this evaluation's temporal wait on `node`, restaging it first (#3815). */
  temporalReady(rt: IvmRuntime, node: NodeId): void {
    if (this.workQueue.isTemporallyWaiting(node)) this.restageReleasedNode(rt, node);
    this.workQueue.temporalReady(node);
  }

  /** Second frame of a routed tick (#3288): activate the reached route barriers. */
  activateRouteBarriers(rt: IvmRuntime, touched: Set<NodeId>): void {
    const closure: Set<NodeId> = rt.graph.downstreamThroughRoutes(touched);
    this.stageNewlyRelevantState(rt, closure);
    const roots: NodeId[] = this.workQueue.layout.roots.slice();
    for (const node of closure) {
      const base = this.nodeMeta.get(node) ?? rt.nodeMeta.get(node);
      const meta = base ? cloneNodeMeta(base) : defaultNodeMeta();
      meta.inputGeneration++;
      if (meta.retainers.some((r: any) => r.kind !== "Hydration")) roots.push(node);
      this.nodeMeta.set(node, meta);
      for (const subscription of rt.subscriptionsByOutputNode.get(node) ?? []) {
        if (this.restartedSubscriptions.has(subscription)) continue;
        if (!this.affectedSubscriptions.has(subscription)) {
          this.affectedSubscriptions.add(subscription);
          this.metrics.subscriptionsConsidered++;
        }
        const state = rt.multisinkSubscriptions.get(subscription);
        if (!state) continue;
        for (const [, output] of state.outputs) {
          if (output.node === node && output.rootOrderingNode !== undefined)
            roots.push(output.rootOrderingNode);
        }
      }
    }
    for (const n of closure) {
      this.affectedNodes.add(n);
      this.relevantNodes.add(n);
    }
    const unique = [...new Set(roots)].sort((a, b) => a - b);
    const queue = EvaluationWorkQueue.discoverFrame(rt.graph, rt.nodeMeta, unique, false);
    // First-frame completions are released exactly once (#3306).
    const released = new Set(this.workQueue.completeNodes());
    const carried = new Set([...released].filter((n) => !closure.has(n)));
    queue.carryEarlierFrame(carried, released);
    queue.completedEvents = this.workQueue.drainCompletedEvents();
    this.evalMemo.setLayout(queue.layout);
    this.workQueue = queue;
  }

  /** Bring live state of newly activated barriers (and ancestors) into this evaluation. */
  stageNewlyRelevantState(rt: IvmRuntime, closure: Set<NodeId>): void {
    const layout = rt.graph.executionLayout([...closure]);
    if (layout.missing !== undefined) throw IvmRuntimeError.of("GraphNodeNotFound", layout.missing);
    for (const node of layout.nodes as NodeId[]) {
      if (this.relevantNodes.has(node)) continue;
      this.relevantNodes.add(node);
      const staged = stageStateForNodes(rt, [node]);
      for (const [k, v] of staged.operatorStates) this.operatorStates.set(k, v);
      for (const [k, v] of staged.arrangementStates) {
        this.arrangementStates.set(k, v);
        addArrangementKey(this.arrangementKeysByInput, node, k);
      }
      const meta = rt.nodeMeta.get(node);
      if (meta && !this.nodeMeta.has(node)) this.nodeMeta.set(node, cloneNodeMeta(meta));
    }
  }

  pollStorageFlush(
    indeterminate: { value: boolean },
    extractionStorage: OwnedStorage | undefined,
    cx: Waker,
  ): Poll<void> {
    if (!this.persistFlush && !this.durableWrites.state.isEmpty()) {
      const operations = this.durableWrites.state.intoOperations();
      this.durableWrites.state = new StagedWriteState();
      const storage = extractionStorage ?? this.storage;
      const flush: NonNullable<IncrementalEvaluation["persistFlush"]> = { done: false };
      this.persistFlush = flush;
      // A started direct flush has no rollback path: a possibly-committed
      // outcome marks the runtime indeterminate.
      storage
        .asRef()
        .writeManyOutcome(operations)
        .then((outcome: any) => {
          if (outcome.kind === "Uncommitted") flush.error = IvmRuntimeError.from(outcome.error);
          else if (outcome.kind === "PossiblyCommitted") {
            indeterminate.value = true;
            flush.error = IvmRuntimeError.from(outcome.error);
          }
        })
        .catch((e: unknown) => {
          indeterminate.value = true;
          flush.error = e;
        })
        .finally(() => {
          flush.done = true;
          flush.waker?.();
        });
    }
    const flush = this.persistFlush;
    if (flush) {
      if (!flush.done) {
        flush.waker = cx;
        return PENDING;
      }
      this.persistFlush = undefined;
      if (flush.error) throw flush.error;
    }
  }

  /** Publish table/binding frontiers monotonically when this evaluation parks. */
  installInputFrontiers(rt: IvmRuntime): void {
    for (const [table, frontier] of this.tableFrontiers) {
      rt.tableFrontiers.set(table, Math.max(rt.tableFrontiers.get(table) ?? frontier, frontier));
    }
    for (const [binding, frontier] of this.bindingFrontiers) {
      rt.bindingFrontiers.set(
        binding,
        Math.max(rt.bindingFrontiers.get(binding) ?? frontier, frontier),
      );
    }
    for (const [node, staged] of this.nodeMeta) {
      const live = rt.nodeMeta.get(node);
      if (live) live.inputGeneration = Math.max(live.inputGeneration, staged.inputGeneration);
    }
  }

  abandon(nodes: Set<NodeId>): void {
    // An empty set is the explicit whole-evaluation cancellation path.
    this.discarded =
      nodes.size === 0 || this.workQueue.layout.roots.every((r: NodeId) => nodes.has(r));
    this.workQueue.abandon(nodes);
    retainKeyed(this.operatorStates, (k) => !nodes.has(k.node));
    retainKeyed(this.arrangementStates, (k) => !nodes.has(k.input));
    retainMap(this.arrangementKeysByInput, (n) => !nodes.has(n));
    this.evalMemo.retain((k: any) => !nodes.has(k.node));
    this.evalMemoBytes = sumPayloadBytes(this.evalMemo);
    retainMap(this.nodeMeta, (n) => !nodes.has(n));
    for (const n of nodes) {
      this.relevantNodes.delete(n);
      this.affectedNodes.delete(n);
    }
    retainMap(this.terminalDeltas, (n) => !nodes.has(n));
    retainMap(this.rootOrderingWindows, (n) => !nodes.has(n));
    retainMap(this.pendingSubscriptionOutputs, (n) => !nodes.has(n));
  }

  /** Recursive nodes and first-frame nodes of a routed tick stay barriers until install. */
  writesBack(rt: IvmRuntime, node: NodeId): boolean {
    return (
      this.routedTerminals.length === 0 &&
      rt.graph.node(node)?.descriptor.operator.kind !== "Recursive"
    );
  }

  /** Hand completed nodes to later evaluations while parked (#3815). */
  writeBackOrHold(rt: IvmRuntime, nodes: Iterable<NodeId>): NodeId[] {
    const written: NodeId[] = [];
    for (const node of nodes) {
      if (this.writesBack(rt, node)) written.push(node);
      else this.heldBarriers.add(node);
    }
    this.installCompletedNodes(rt, written);
    return written;
  }

  /** Write the state of completed nodes back to the runtime while parked. */
  installCompletedNodes(rt: IvmRuntime, nodes: Iterable<NodeId>): void {
    if (this.discarded) return;
    const installed = new Set<NodeId>();
    for (const node of nodes) {
      if (this.earlyInstalled.has(node)) continue;
      this.earlyInstalled.add(node);
      installed.add(node);
      const key = rootStateKey(node);
      const state = this.operatorStates.get(key);
      if (state) rt.operatorStates.set(key, cloneOperatorState(state));
      const keys = this.arrangementKeysByInput.get(node);
      if (!keys) continue;
      for (const [akey] of keys) {
        if (akey.scope !== ScopeId.root()) continue;
        const astate = this.arrangementStates.get(akey);
        if (!astate) continue;
        const copy = cloneArrangement(astate);
        // The producer's transition belongs to this tick.
        copy.value.releaseChanges();
        rt.arrangementStates.set(akey, copy);
        addArrangementKey(rt.arrangementKeysByInput, node, akey);
      }
    }
    if (!installed.size) return;
    const nodeMeta = new Map<NodeId, NodeRuntimeMeta>();
    for (const node of installed) {
      const meta = this.nodeMeta.get(node);
      if (meta) nodeMeta.set(node, cloneNodeMeta(meta));
    }
    carryLiveNodeLifecycle(nodeMeta, rt, installed);
    for (const [n, m] of nodeMeta) rt.nodeMeta.set(n, m);
  }

  /** Replace the staged state for `node` with the live runtime's (#3815). */
  restageReleasedNode(rt: IvmRuntime, node: NodeId): void {
    const key = rootStateKey(node);
    const live = rt.operatorStates.get(key);
    if (live) this.operatorStates.set(key, cloneOperatorState(live));
    else this.operatorStates.delete(key);
    const owned = (k: ArrangementKey) => arrangementOwner(k) === node;
    retainKeyed(this.arrangementStates, (k) => !owned(k));
    for (const keys of this.arrangementKeysByInput.values()) retainKeyed(keys, (k) => !owned(k));
    // Child-scope arrangements are keyed by body inputs; find them by owner.
    for (const [k, state] of rt.arrangementStates) {
      if (!owned(k)) continue;
      this.arrangementStates.set(k, cloneArrangement(state));
      addArrangementKey(this.arrangementKeysByInput, k.input, k);
    }
    carryLiveNodeLifecycle(this.nodeMeta, rt, new Set([node]));
    this.evalMemo.retain((k: any) => k.node !== node && k.scope.outermost() !== node);
    this.evalMemoBytes = sumPayloadBytes(this.evalMemo);
  }

  install(rt: IvmRuntime): void {
    if (this.discarded) return;
    // Root ordering snapshots share TopBy group state; release them first.
    this.rootOrderingWindows.clear();
    retainKeyed(this.operatorStates, (k) => k.scope === ScopeId.root());
    // Nodes written back while parked may since have advanced (#3815).
    const early = this.earlyInstalled;
    this.earlyInstalled = new Set();
    if (early.size) {
      retainKeyed(this.operatorStates, (k) => !early.has(k.node));
      retainKeyed(this.arrangementStates, (k) => !early.has(arrangementOwner(k)));
      for (const keys of this.arrangementKeysByInput.values())
        retainKeyed(keys, (k) => !early.has(arrangementOwner(k)));
      retainMap(this.nodeMeta, (n) => !early.has(n));
    }
    for (const [key, state] of this.operatorStates) {
      rt.operatorStates.delete(key);
      commitOperatorState(state);
      rt.operatorStates.set(key, state);
    }
    this.operatorStates = newOperatorStates();
    for (const [key, state] of this.arrangementStates) {
      rt.arrangementStates.delete(key);
      state.value.commitOverlay();
      rt.arrangementStates.set(key, state);
    }
    this.arrangementStates = newArrangementStates();
    for (const [node, keys] of this.arrangementKeysByInput) {
      const live = rt.arrangementKeysByInput.get(node);
      if (live && early.size) {
        retainKeyed(live, (k) => early.has(arrangementOwner(k)));
        for (const [k] of keys) live.set(k, k);
      } else rt.arrangementKeysByInput.set(node, keys);
    }
    this.arrangementKeysByInput = new Map();
    this.earlyInstalled = early;
    // Tick deltas belong to the frame; retain only hydration reuse entries.
    for (const [key, entry] of this.evalMemo.intoEntries()) {
      if (key.tickEpoch === undefined) rt.insertRetainedEvalMemo(key, entry);
    }
    this.evalMemo = new EvaluationMemo();
    rt.memoUseClock = Math.max(rt.memoUseClock, this.memoUseClock);
    carryLiveNodeLifecycle(this.nodeMeta, rt, this.relevantNodes);
    for (const [n, m] of this.nodeMeta) rt.nodeMeta.set(n, m);
    this.nodeMeta = new Map();
    this.installInputFrontiers(rt);
    rt.currentTick = Math.max(rt.currentTick, this.currentTick);
    rt.pendingBindingRetractions.splice(0, this.pendingBindingRetractions);
  }

  private stash(evaluator: TickEvaluator): void {
    this.terminalDeltas = evaluator.terminalDeltas;
    this.rootOrderingWindows = evaluator.rootOrderingWindows;
    evaluator.terminalDeltas = new Map();
    evaluator.rootOrderingWindows = new Map();
    takeBack(this, evaluator);
  }

  private requestMissing(rt: IvmRuntime, requests: EvaluationRequestKey[]): boolean {
    let registered = false;
    const provider = requestChunkProvider(rt, this.pendingResidentPublication);
    for (const request of requests) {
      registered = this.requests.request(request, this.storage, provider, rt.schema) || registered;
    }
    return registered;
  }

  /** Throws EvaluationFailure; IvmRuntimeErrors become fatal failures. */
  poll(rt: IvmRuntime, cx: Waker): Poll<void> {
    try {
      return this.pollInner(rt, cx);
    } catch (e) {
      throw EvaluationFailure.fatal(e);
    }
  }

  private pollInner(rt: IvmRuntime, cx: Waker): Poll<void> {
    if (this.discarded) return;
    if (this.requests.poll(cx) === 0) this.requests.pollEagerRetry(cx);
    const drained = this.requests.drainReady();
    if (!drained.ok) throw this.workQueue.failureForRequest(drained.request, drained.failure);
    this.workQueue.requestsReady(drained.ready.keys());
    this.evaluationInputs?.install(drained.ready);

    const evaluator = makeEvaluator(rt, this, {
      tableDeltas: this.tableDeltas,
      bindingDeltas: this.bindingDeltas,
      bindingSnapshots: this.bindingSnapshots,
      currentTick: this.currentTick,
      tableFrontiers: this.tableFrontiers,
      bindingFrontiers: this.bindingFrontiers,
      storage: this.storage.asRef(),
      evaluationInputs: this.evaluationInputs,
      context: EvalContext.root(),
      metrics: this.metrics,
      terminalDeltas: this.terminalDeltas,
      rootOrderingWindows: this.rootOrderingWindows,
    });

    let registeredRequests = false;
    while (this.workQueue.runnable.length) {
      const node = this.workQueue.runnable.shift()!;
      let result: Poll<RecordDeltas>;
      try {
        result = this.workQueue.pollNode(evaluator, node, cx);
      } catch (error) {
        if (!IvmRuntimeError.is(error, "EvaluationBlocked")) {
          takeBack(this, evaluator);
          throw this.workQueue.failureForNode(node, error);
        }
        const requests = evaluator.evaluationInputs?.takeMissing() ?? [];
        if (!requests.length) {
          takeBack(this, evaluator);
          throw this.workQueue.failureForNode(node, IvmRuntimeError.of("EvaluationBlocked"));
        }
        registeredRequests = this.requestMissing(rt, requests) || registeredRequests;
        this.workQueue.waitForRequests(node, requests);
        continue;
      }
      if (result === PENDING) {
        this.workQueue.requeueYielded(node);
        this.stash(evaluator);
        cx();
        return PENDING;
      }
      this.workQueue.complete(node);
    }
    let immediatelyReady = registeredRequests ? this.requests.poll(cx) : 0;
    // Retry only requests a backend attests are executor-local.
    if (registeredRequests && immediatelyReady === 0)
      immediatelyReady = this.requests.pollEagerRetry(cx);
    if (immediatelyReady > 0) {
      // Resident requests completed synchronously: resume within this poll.
      this.stash(evaluator);
      return this.pollInner(rt, cx);
    }
    if (this.routedTerminals.length && this.workQueue.rootsComplete()) {
      const routed = this.routedTerminals;
      this.routedTerminals = [];
      const touched = touchedRouteBarriers(evaluator, rt.graph, routed, cx);
      // A barrier reaching durable state was already evaluated in frame one.
      for (const b of [...touched]) if (this.affectedNodes.has(b)) touched.delete(b);
      if (touched.size) {
        this.stash(evaluator);
        this.activateRouteBarriers(rt, touched);
        return this.pollInner(rt, cx);
      }
    }

    // Until route barriers activate, a routed subscription's gated sinks have
    // no deltas; publishing its other sinks now would lose them (#3288).
    const routesPending = this.routedTerminals.length > 0;
    const awaitsRouteBarriers = (s: MultisinkSubscriptionState) =>
      routesPending && s.target.kind === "RoutedShape" && s.target.routeBarriers.length > 0;
    const skip = (id: SubscriptionId, s: MultisinkSubscriptionState) =>
      s.failed || this.publishedSubscriptions.has(id) || awaitsRouteBarriers(s);
    const terminalConsumers = new Map<NodeId, number>();
    for (const id of this.affectedSubscriptions) {
      const s = rt.multisinkSubscriptions.get(id);
      if (!s || skip(id, s)) continue;
      for (const [, output] of s.outputs) {
        if (!this.affectedNodes.has(output.node)) continue;
        const node = evaluator.terminalDeltaNodeForOutput(output.node);
        if (node !== undefined) terminalConsumers.set(node, (terminalConsumers.get(node) ?? 0) + 1);
      }
    }

    for (const id of this.affectedSubscriptions) {
      const subscription = rt.multisinkSubscriptions.get(id);
      if (!subscription || skip(id, subscription)) continue;
      const affectedOutputs = subscription.outputs
        .valuesArray()
        .filter((o) => this.affectedNodes.has(o.node));
      if (
        affectedOutputs.some(
          (o) =>
            !this.workQueue.isComplete(o.node) ||
            (o.rootOrderingNode !== undefined && !this.workQueue.isComplete(o.rootOrderingNode)),
        )
      ) {
        continue;
      }
      const prepared: [string, CompiledNode, RecordDeltas, RecordDeltas][] = [];
      for (const [sink, output] of subscription.outputs) {
        if (!this.affectedNodes.has(output.node)) continue;
        let physical: RecordDeltas;
        let materialized: RecordDeltas | undefined;
        const pending = this.pendingSubscriptionOutputs.get(output.node);
        if (pending) {
          physical = pending.physical;
          materialized = pending.materializedAs(subscription.rootIndirectValues);
        } else {
          const records = evaluator.pollUpdateNode(output.node, cx);
          if (records === PENDING)
            throw EvaluationFailure.fatal(IvmRuntimeError.of("EvaluationBlocked"));
          this.pendingSubscriptionOutputs.set(output.node, new PendingSubscriptionOutput(records));
          physical = records;
        }
        // Each subscription publishes in the representation its snapshot used.
        let records: RecordDeltas;
        try {
          if (materialized) records = materialized;
          else {
            const fields = materializedFieldIndices(
              subscription.rootIndirectValues,
              physical.descriptor,
            );
            records =
              fields === undefined
                ? evaluator.materializeIndirectInput(physical)
                : evaluator.materializeIndirectFieldIndices(physical, fields);
          }
          this.pendingSubscriptionOutputs
            .get(output.node)
            ?.recordMaterialized(subscription.rootIndirectValues, records);
        } catch (error) {
          if (!IvmRuntimeError.is(error, "EvaluationBlocked")) throw error;
          this.stash(evaluator);
          const requests = evaluator.evaluationInputs?.takeMissing() ?? [];
          if (!requests.length) throw error;
          this.requestMissing(rt, requests);
          let ready = this.requests.poll(cx);
          // A cold self-wake is not a resident continuation.
          if (ready === 0) ready = this.requests.pollEagerRetry(cx);
          return ready > 0 ? this.pollInner(rt, cx) : PENDING;
        }
        prepared.push([sink, output, physical, records]);
      }

      const sinks = new SortedMap<string, RecordDeltas>(cmpStr);
      const terminalSinks = new SortedMap<string, TerminalDeltas>(cmpStr);
      for (const [sink, output, physical, published] of prepared) {
        if (
          published.deltas.length &&
          !published.descriptor.registryCompatibleWith(output.output)
        ) {
          throw IvmRuntimeError.of("GraphOutputMismatch");
        }
        const structured: boolean = evaluator.outputIsStructuredCollectBy(output.node);
        const publicRoot: boolean = evaluator.outputHasPublicRoot(output.node);
        const terminalOwned = output.rootOrderingNode !== undefined || structured;
        const identity =
          output.rootOrderingNode !== undefined && !structured
            ? rootIdentityFields(evaluator.graph, output.node, output.rootOrderingNode)
            : undefined;
        const records: RecordDeltas = {
          descriptor: published.descriptor,
          deltas: published.deltas.slice(),
        };
        // Only groups this output's deltas reach take part in root ordering.
        const identityGroups =
          identity &&
          new Set(
            physical.deltas.map((d) =>
              toHex(
                encodedIdentityKeyPart(
                  physical.descriptor,
                  d.record,
                  identity.fields.slice(0, identity.groupLen),
                ),
              ),
            ),
          );
        if (terminalOwned) {
          let terminal: TerminalDeltas | undefined;
          if (structured) {
            const node = evaluator.terminalDeltaNodeForOutput(output.node);
            if (node !== undefined) {
              const remaining = terminalConsumers.get(node);
              if (remaining === undefined)
                throw new Error("terminal consumer counted before publication");
              terminalConsumers.set(node, remaining - 1);
              terminal = evaluator.terminalDeltasForConsumer(node, remaining - 1 === 0);
            } else if (!publicRoot && records.deltas.length)
              terminal = terminalDeltasFromRecordDeltas(records);
            else if (output.rootOrderingNode !== undefined) terminal = new TerminalDeltas();
          } else if (records.deltas.length) {
            terminal = identity
              ? terminalDeltasKeyedByIdentity(records, physical, identity.fields)
              : terminalDeltasFromRecordDeltas(records);
          } else if (output.rootOrderingNode !== undefined) terminal = new TerminalDeltas();
          if (terminal) {
            // A CollectBy root terminal already owns occurrence keys and moves.
            if (!structured && output.rootOrderingNode !== undefined) {
              evaluator.applyRootOrdering(
                output.rootOrderingNode,
                output.output,
                identity && identityGroups ? [identity, identityGroups] : undefined,
                terminal,
              );
            }
            if (!terminal.isEmpty()) terminalSinks.set(sink, terminal);
          }
        }
        if (records.deltas.length) sinks.set(sink, records);
      }
      const deltas = new MultisinkDeltas(sinks, terminalSinks);
      if (!deltas.isEmpty()) {
        evaluator.metrics.notificationsSent++;
        evaluator.metrics.notificationRecords += multisinkDeltasRecordCount(deltas);
        evaluator.metrics.notificationEncodedBytes += multisinkDeltasEncodedBytes(deltas);
      }
      const queued: QueuedMultisinkDeltas = {
        deltas,
        publication:
          this.pendingResidentPublication?.publication.value ?? this.notificationPublication,
      };
      if (!queued.deltas.isEmpty()) {
        const pendingPublication = this.pendingResidentPublication;
        // A resident publication hands completed subscriptions' slice over now.
        if (pendingPublication && queued.publication === undefined)
          pendingPublication.notifications.push([id, queued]);
        else this.pendingNotifications.push([id, queued]);
      }
      this.publishedSubscriptions.add(id);
    }
    this.stash(evaluator);

    if (this.requests.hasPending() || !this.workQueue.rootsComplete()) return PENDING;
    if (this.pollStorageFlush(rt.persistenceIndeterminate, undefined, cx) === PENDING)
      return PENDING;
    this.install(rt);

    const dropped: SubscriptionId[] = [];
    for (const [id, queued] of this.pendingNotifications) {
      const pendingPublication = this.pendingResidentPublication;
      if (
        this.deferNotificationsUntilDurable &&
        queued.publication !== undefined &&
        !rt.durableNotificationPublications.has(queued.publication)
      ) {
        let list = rt.deferredNotifications.get(queued.publication);
        if (!list) rt.deferredNotifications.set(queued.publication, (list = []));
        list.push([id, queued]);
      } else if (pendingPublication && queued.publication === undefined) {
        pendingPublication.notifications.push([id, queued]);
      } else {
        const s = rt.multisinkSubscriptions.get(id);
        if (s && !s.sender.send(queued)) dropped.push(id);
      }
    }
    this.pendingNotifications = [];
    for (const id of dropped) unsubscribe(rt, id);
    evictEvalMemo(rt);
    const pendingPublication = this.pendingResidentPublication;
    const publication = pendingPublication?.publication.value ?? this.notificationPublication;
    if (pendingPublication && pendingPublication.publication.value === undefined) {
      pendingPublication.completed.value = true;
    } else if (
      this.deferNotificationsUntilDurable &&
      publication !== undefined &&
      !rt.durableNotificationPublications.delete(publication)
    ) {
      rt.completedDeferredPublications.add(publication);
    }
    this.metrics.runtimeStats = rt.collectTickRuntimeStats
      ? runtimeStats(rt)
      : cheapRuntimeStats(rt);
  }
}

// ---- subscription hydration session ----

/**
 * Hydration-time owner of staged state. Storage suspension and dependency
 * ordering live in EvaluationWorkQueue; every root is evaluated only after
 * its inputs completed.
 */
class EvaluationSession implements EvaluatorScratch {
  outputs = new Map<NodeId, RecordDeltas>();
  pendingOutputs = new Map<NodeId, RecordDeltas>();
  /** Collector operations seeding a new subscription's terminal tree. */
  terminalDeltas = new Map<NodeId, TerminalDeltas>();
  evaluationInputs = new EvaluationInputs();
  /** How root outputs present indirect values to the caller. */
  rootIndirectValues: RootIndirectValues = MATERIALIZE;
  /** Roots hydrated only to order another output's terminal. */
  orderingOnlyRoots = new Set<NodeId>();
  /** Nodes that stay owned by the live runtime rather than this session. */
  borrowed = new Set<NodeId>();

  private constructor(
    public relevantNodes: Set<NodeId>,
    public roots: Set<NodeId>,
    public operatorStates: KeyedMap<OperatorStateKey, OperatorState>,
    public arrangementStates: KeyedMap<ArrangementKey, AsOf<ArrangementState, SubTick>>,
    public arrangementKeysByInput: Map<NodeId, KeyedMap<ArrangementKey, ArrangementKey>>,
    public evalMemo: EvaluationMemo,
    public evalMemoBytes: number,
    public memoUseClock: number,
    public nodeMeta: Map<NodeId, NodeRuntimeMeta>,
    public bindingFrontiers: KeyedMap<BindingSourceKey, number>,
    public storage: OwnedStorage,
    public requests: EvaluationRequests,
    public workQueue: EvaluationWorkQueue,
  ) {}

  static hydration(rt: IvmRuntime, roots: NodeId[], storage: OwnedStorage): EvaluationSession {
    const [relevantNodes, workQueue] = EvaluationWorkQueue.discover(
      rt.graph,
      rt.nodeMeta,
      roots,
      true,
    );
    // Installed operator state is root-scoped; probe by reachable node.
    const staged = stageStateForNodes(rt, relevantNodes);
    const evalMemo = EvaluationMemo.forLayout(workQueue.layout);
    for (const node of relevantNodes) {
      for (const key of rt.evalMemoKeysByNode.get(node) ?? []) {
        const entry = rt.evalMemo.get(key);
        if (entry) evalMemo.insert(key, entry.clone());
      }
    }
    const nodeMeta = new Map<NodeId, NodeRuntimeMeta>();
    for (const node of relevantNodes) {
      const meta = rt.nodeMeta.get(node);
      if (meta) nodeMeta.set(node, cloneNodeMeta(meta));
    }
    // Recursive hydration rebuilds internal arrangements from complete source
    // snapshots, so a leaf memo alone cannot satisfy those inputs.
    const pending: NodeId[] = [];
    for (const node of relevantNodes) {
      const g = rt.graph.node(node);
      if (g?.descriptor.operator.kind === "Recursive") pending.push(...g.descriptor.inputs);
    }
    const recursiveInputs = new Set<NodeId>();
    while (pending.length) {
      const node = pending.shift()!;
      if (recursiveInputs.has(node)) continue;
      recursiveInputs.add(node);
      const g = rt.graph.node(node);
      if (g) pending.push(...g.descriptor.inputs);
    }
    const resident = new Set<NodeId>();
    for (const [node, meta] of nodeMeta) {
      if (recursiveInputs.has(node) || !meta.inputSignature) continue;
      const entry = evalMemo.get({
        scope: ScopeId.root(),
        node,
        inputSignatureHash: meta.inputSignature.hash,
        tickEpoch: undefined,
        subTick: 0,
        contextDigest: 0,
      });
      if (entry && entry.inputWatermark === meta.inputGeneration) resident.add(node);
    }
    workQueue.storageAlreadyResident(resident);
    const requests = new EvaluationRequests();
    for (const request of workQueue.requests())
      requests.request(request, storage, rt.chunkProvider, rt.schema);
    return new EvaluationSession(
      relevantNodes,
      new Set(roots),
      staged.operatorStates,
      staged.arrangementStates,
      staged.arrangementKeysByInput,
      evalMemo,
      sumPayloadBytes(evalMemo),
      rt.memoUseClock,
      nodeMeta,
      cloneFrontiers(rt.bindingFrontiers),
      storage,
      requests,
      workQueue,
    );
  }

  advanceBindingInput(graph: IvmGraph, shape: string): void {
    const key = BSK.prepared(shape);
    this.bindingFrontiers.set(key, (this.bindingFrontiers.get(key) ?? 0) + 1);
    const affected: Set<NodeId> = graph.affectedNodesThroughRoutes([], [key]);
    for (const node of affected) {
      if (!this.relevantNodes.has(node)) continue;
      let meta = this.nodeMeta.get(node);
      if (!meta) this.nodeMeta.set(node, (meta = defaultNodeMeta()));
      meta.inputGeneration++;
    }
  }

  private requestAll(rt: IvmRuntime, requests: EvaluationRequestKey[]): boolean {
    let registered = false;
    for (const r of requests)
      registered =
        this.requests.request(r, this.storage, rt.chunkProvider, rt.schema) || registered;
    return registered;
  }

  private pollAgain(cx: Waker): boolean {
    const ready = this.requests.poll(cx);
    return ready > 0 || this.requests.pollEagerRetry(cx) > 0;
  }

  poll(
    rt: IvmRuntime,
    bindingSnapshots: BindingSnapshots,
    hydrateArrangements: boolean,
    metrics: TickMetrics,
    cx: Waker,
  ): Poll<void> {
    let remaining = MAX_HYDRATION_RUNNABLE_NODES_PER_POLL;
    ownerTurn: for (;;) {
      if (this.requests.poll(cx) === 0) this.requests.pollEagerRetry(cx);
      const drained = this.requests.drainReady();
      if (!drained.ok) throw drained.failure.error;
      this.workQueue.requestsReady(drained.ready.keys());
      this.evaluationInputs.install(drained.ready);

      while (this.workQueue.runnable.length) {
        const node = this.workQueue.runnable.shift()!;
        if (remaining === 0) {
          // A resident CPU budget yield: arrange exactly one fresh owner turn.
          this.workQueue.requeueYielded(node);
          cx();
          return PENDING;
        }
        remaining--;
        const context = hydrateArrangements
          ? EvalContext.rootSubscriptionSnapshot()
          : EvalContext.rootSnapshot();
        let records: RecordDeltas | undefined = this.pendingOutputs.get(node);
        let error: unknown;
        if (records) this.pendingOutputs.delete(node);
        else {
          const evaluator = makeEvaluator(rt, this, {
            tableDeltas: [],
            bindingDeltas: [],
            bindingSnapshots,
            currentTick: rt.currentTick,
            tableFrontiers: rt.tableFrontiers,
            bindingFrontiers: this.bindingFrontiers,
            storage: this.storage.asRef(),
            evaluationInputs: this.evaluationInputs,
            context,
            metrics,
            terminalDeltas: this.terminalDeltas,
            rootOrderingWindows: new Map(),
          });
          let poll: Poll<RecordDeltas> | undefined;
          try {
            poll = this.workQueue.pollNode(evaluator, node, cx);
          } catch (e) {
            error = e;
          }
          this.terminalDeltas = evaluator.terminalDeltas;
          takeBack(this, evaluator);
          // A cooperative yield registered no request: requeue for a fresh turn.
          if (poll === PENDING) {
            this.workQueue.requeueYielded(node);
            cx();
            return PENDING;
          }
          records = poll;
        }
        if (records) {
          if (this.workQueue.isRoot(node)) {
            const fields = this.orderingOnlyRoots.has(node)
              ? []
              : materializedFieldIndices(this.rootIndirectValues, records.descriptor);
            const materialized: RecordDelta[] = [];
            let blocked = false;
            for (const delta of records.deltas) {
              try {
                const record = largeValues.materializeRecordBorrowedAttempt(
                  records.descriptor,
                  delta.record,
                  fields,
                  this.evaluationInputs,
                );
                materialized.push({ record: record ?? delta.record, weight: delta.weight });
              } catch (e) {
                if (!IvmRuntimeError.is(e, "EvaluationBlocked")) throw e;
                blocked = true;
              }
            }
            if (blocked) {
              const requests = this.evaluationInputs.takeMissing();
              if (!requests.length) throw IvmRuntimeError.of("EvaluationBlocked");
              this.requestAll(rt, requests);
              this.pendingOutputs.set(node, records);
              this.workQueue.waitForRequests(node, requests);
              // Every newly retained future must be polled once to install the waker.
              if (this.pollAgain(cx)) continue ownerTurn;
              // Cold storage owns the continuation now.
              if (this.requests.hasPending()) return PENDING;
              continue;
            }
            this.outputs.set(node, { descriptor: records.descriptor, deltas: materialized });
          }
          this.workQueue.complete(node);
        } else if (IvmRuntimeError.is(error, "EvaluationBlocked")) {
          const requests = this.evaluationInputs.takeMissing();
          if (!requests.length) throw IvmRuntimeError.of("EvaluationBlocked");
          const registered = this.requestAll(rt, requests);
          this.workQueue.waitForRequests(node, requests);
          if ((registered || this.requests.hasPending()) && this.pollAgain(cx)) continue ownerTurn;
          // Yield to the cold request rather than scanning the rest of the graph.
          if (this.requests.hasPending()) return PENDING;
        } else throw error;
      }

      if (this.outputs.size === this.roots.size) return;
      if (this.requests.hasPending()) return PENDING;
      // Earlier evaluations own the continuation until barriers release.
      if (this.workQueue.hasTemporalWaiting()) return PENDING;
      throw IvmRuntimeError.of("EvaluationBlocked");
    }
  }

  install(rt: IvmRuntime): void {
    if (this.borrowed.size) {
      const borrowed = this.borrowed;
      this.borrowed = new Set();
      for (const n of borrowed) this.relevantNodes.delete(n);
      retainKeyed(this.operatorStates, (k) => !borrowed.has(k.node));
      const borrowedKeys = new Set<string>();
      for (const [node, keys] of this.arrangementKeysByInput) {
        if (borrowed.has(node)) for (const [k] of keys) borrowedKeys.add(arrangementKeyString(k));
      }
      retainMap(this.arrangementKeysByInput, (n) => !borrowed.has(n));
      retainKeyed(this.arrangementStates, (k) => !borrowedKeys.has(arrangementKeyString(k)));
      this.evalMemo.retain((k: any) => !borrowed.has(k.node));
      retainMap(this.nodeMeta, (n) => !borrowed.has(n));
    }
    for (const node of this.relevantNodes) rt.operatorStates.delete(rootStateKey(node));
    // Fold initially populated collector overlays now, otherwise the first
    // incremental edit would COW-clone the entire hydration overlay.
    for (const [key, state] of this.operatorStates) {
      commitOperatorState(state, false);
      if (key.scope === ScopeId.root()) rt.operatorStates.set(key, state);
    }
    for (const node of this.relevantNodes) {
      for (const [key] of rt.arrangementKeysByInput.get(node) ?? [])
        rt.arrangementStates.delete(key);
    }
    for (const [key, state] of this.arrangementStates) {
      state.value.commitOverlay();
      rt.arrangementStates.set(key, state);
    }
    for (const node of this.relevantNodes) rt.arrangementKeysByInput.delete(node);
    for (const [n, keys] of this.arrangementKeysByInput) rt.arrangementKeysByInput.set(n, keys);
    rt.removeRetainedEvalMemosForNodes(this.relevantNodes);
    for (const [key, entry] of this.evalMemo.intoEntries()) {
      if (key.tickEpoch === undefined) rt.insertRetainedEvalMemo(key, entry);
    }
    rt.memoUseClock = Math.max(rt.memoUseClock, this.memoUseClock);
    carryLiveNodeLifecycle(this.nodeMeta, rt, this.relevantNodes);
    for (const node of this.relevantNodes) rt.nodeMeta.delete(node);
    for (const [n, m] of this.nodeMeta) rt.nodeMeta.set(n, m);
  }
}

class PendingSubscriptionHydration {
  constructor(
    readonly subscriptionId: SubscriptionId,
    readonly outputs: SortedMap<string, CompiledNode>,
    readonly initial: InitialSlot,
    public session: EvaluationSession,
    readonly bindingSnapshots: BindingSnapshots,
    /** The prepared shape whose binding frontier this hydration advanced (#3901). */
    readonly bindingFrontierAdvance: string | undefined,
    readonly hydrateArrangements: boolean,
    readonly lifetime: SubscriptionLifetime,
    public metrics: TickMetrics,
  ) {}
  workQueueRef(): EvaluationWorkQueue {
    return this.session.workQueue;
  }
  hasResidentContinuation(): boolean {
    return this.session.workQueue.hasResidentContinuation();
  }
  temporalReady(_rt: IvmRuntime, node: NodeId): void {
    this.session.workQueue.temporalReady(node);
  }
}

// ---- IvmRuntime: tick orchestration ----

function cloneFrontiers(
  map: KeyedMap<BindingSourceKey, number>,
): KeyedMap<BindingSourceKey, number> {
  const out = new KeyedMap<BindingSourceKey, number>(BSK.key);
  for (const [k, v] of map) out.set(k, v);
  return out;
}

function uniqueRoots(outputs: SortedMap<string, CompiledNode>): NodeId[] {
  const roots = new Set<NodeId>();
  for (const output of outputs.valuesArray()) {
    if (output.rootOrderingNode !== undefined) roots.add(output.rootOrderingNode);
    roots.add(output.node);
  }
  return [...roots];
}

function temporalBlockersOf(rt: IvmRuntime): Map<NodeId, number> {
  return new Map([...rt.pendingIncremental.state.waitersByNode.keys()].map((n) => [n, 1]));
}

function failSubscription(rt: IvmRuntime, id: SubscriptionId, error: IvmRuntimeError): void {
  rt.multisinkSubscriptions.get(id)?.sender.fail(new SubscriptionError(error));
}

export function enqueueSubscriptionHydration(
  rt: IvmRuntime,
  subscriptionId: SubscriptionId,
  outputs: SortedMap<string, CompiledNode>,
  storage: OwnedStorage,
  bindingSnapshots: BindingSnapshots | undefined,
  bindingFrontierAdvance: string | undefined,
  initial: InitialSlot,
  lifetime: SubscriptionLifetime,
  rootIndirectValues: RootIndirectValues,
  borrowed: Set<NodeId>,
): number {
  const roots = uniqueRoots(outputs);
  const hydrateArrangements =
    lifetime === SubscriptionLifetime.Retained &&
    roots.some((root) => outputDependsOnAggregate(rt, root));
  // An ordering root orders the terminal by row identity only; rebuilding its
  // large values would fetch chunks of columns the output drops (#3830).
  const all = outputs.valuesArray();
  const orderingOnlyRoots = new Set<NodeId>();
  for (const o of all) {
    if (o.rootOrderingNode !== undefined && all.every((x) => x.node !== o.rootOrderingNode))
      orderingOnlyRoots.add(o.rootOrderingNode);
  }
  const session = EvaluationSession.hydration(rt, roots, storage);
  session.rootIndirectValues = rootIndirectValues;
  session.orderingOnlyRoots = orderingOnlyRoots;
  if (borrowed.size) {
    // The subscription's own nodes may hold a memo predating later writes.
    for (const node of session.relevantNodes) {
      if (borrowed.has(node)) continue;
      let meta = session.nodeMeta.get(node);
      if (!meta) session.nodeMeta.set(node, (meta = defaultNodeMeta()));
      meta.inputGeneration++;
    }
  }
  session.borrowed = borrowed;
  if (bindingFrontierAdvance !== undefined)
    session.advanceBindingInput(rt.graph, bindingFrontierAdvance);
  session.workQueue.addTemporalBlockers(temporalBlockersOf(rt));
  const evaluation = new PendingSubscriptionHydration(
    subscriptionId,
    outputs,
    initial,
    session,
    bindingSnapshots ?? bindingSnapshotDeltas(rt),
    bindingFrontierAdvance,
    hydrateArrangements,
    lifetime,
    new TickMetrics(),
  );
  const pending = rt.pendingIncremental.state;
  const id = pending.nextId++;
  pending.register(id, evaluation.workQueueRef().incompleteNodes());
  pending.evaluations.set(id, evaluation);
  pending.order.push(id);
  return id;
}

function failEvaluationNodes(rt: IvmRuntime, failure: EvaluationFailure): void {
  const affected = failure.affectedNodes;
  // A recursive node also owns the state of its child scopes.
  const ownedByAffected = (scope: ScopeId) => {
    const n = scope.outermost();
    return n !== undefined && affected.has(n);
  };
  retainKeyed(rt.operatorStates, (k) => !affected.has(k.node) && !ownedByAffected(k.scope));
  rt.removeRetainedEvalMemosForNodes(affected);
  const scoped: any[] = [];
  for (const keys of rt.evalMemoKeysByNode.values())
    for (const [k] of keys) if (ownedByAffected(k.scope)) scoped.push(k);
  for (const key of scoped) rt.removeRetainedEvalMemo(key);
  for (const node of affected) {
    const keys = rt.arrangementKeysByInput.get(node);
    if (keys) {
      rt.arrangementKeysByInput.delete(node);
      for (const [k] of keys) rt.arrangementStates.delete(k);
    }
    const meta = rt.nodeMeta.get(node);
    if (meta) {
      meta.inputSignature = undefined;
      meta.inputGeneration++;
    }
  }
  const scopedKeys = [...rt.arrangementStates]
    .map(([k]) => k)
    .filter((k) => ownedByAffected(k.scope));
  for (const key of scopedKeys) {
    rt.arrangementStates.delete(key);
    const keys = rt.arrangementKeysByInput.get(key.input);
    if (keys) {
      keys.delete(key);
      if (!keys.size) rt.arrangementKeysByInput.delete(key.input);
    }
  }
  for (const s of rt.multisinkSubscriptions.values()) {
    if (
      s.failed ||
      !s.outputs
        .valuesArray()
        .some(
          (o) =>
            affected.has(o.node) ||
            (o.rootOrderingNode !== undefined && affected.has(o.rootOrderingNode)),
        )
    ) {
      continue;
    }
    s.failed = true;
    s.sender.fail(new SubscriptionError(failure.error));
  }
}

function failAllSubscriptions(rt: IvmRuntime, error: IvmRuntimeError): void {
  for (const s of rt.multisinkSubscriptions.values()) {
    if (s.failed) continue;
    s.failed = true;
    s.sender.fail(new SubscriptionError(error));
  }
}

export function tick(
  rt: IvmRuntime,
  tableDeltas: TableDelta[],
  storage: OrderedKvStorage,
): Promise<TickMetrics> {
  return tickWithParams(rt, tableDeltas, [], new OwnedStorage(storage), undefined);
}

export async function flushPendingBindingRetractions(
  rt: IvmRuntime,
  storage: OrderedKvStorage,
): Promise<void> {
  // Snapshot hydration needs queued retractions folded into arranged state first.
  if (rt.pendingBindingRetractions.length)
    await tickWithParams(rt, [], [], new OwnedStorage(storage), undefined);
}

export async function tickResidentStaged(
  rt: IvmRuntime,
  tableDeltas: TableDelta[],
  storage: OwnedStorage,
  deferNotificationsUntilDurable: boolean,
  publicationInstall: [any, any] | undefined,
): Promise<ResidentTick> {
  const publication: PendingResidentPublication = {
    publication: { value: undefined },
    notifications: [],
    completed: { value: false },
    deferNotificationsUntilDurable,
    chunkProvider:
      publicationInstall &&
      rt.chunkProvider.withInstallObserver(publicationInstall[0], publicationInstall[1]),
  };
  const [metrics, durableWrites] = await tickDetachingCold(
    rt,
    tableDeltas,
    [],
    storage,
    deferNotificationsUntilDurable,
    publication,
    DetachOn.AnyRequest,
  );
  return new ResidentTick(metrics, publication, durableWrites);
}

/**
 * Drive one tick of runtime-owned input changes without waiting for remote
 * chunks; chunk-blocked work stays queued as pending incremental progress.
 */
export async function tickBindingsDetachingCold(
  rt: IvmRuntime,
  bindingDeltas: BindingDelta[],
  storage: OwnedStorage,
): Promise<TickMetrics> {
  if (rt.persistenceIndeterminate.value)
    throw IvmRuntimeError.of("PersistenceOutcomeIndeterminate");
  const [metrics] = await tickDetachingCold(
    rt,
    [],
    bindingDeltas,
    storage,
    false,
    undefined,
    DetachOn.ChunkRequest,
  );
  return metrics;
}

async function tickDetachingCold(
  rt: IvmRuntime,
  tableDeltas: TableDelta[],
  bindingDeltas: BindingDelta[],
  storage: OwnedStorage,
  deferNotificationsUntilDurable: boolean,
  publication: PendingResidentPublication | undefined,
  detachOn: DetachOn,
): Promise<[TickMetrics, DurableWrites]> {
  const changedTables = new Set(tableDeltas.map((d) => d.table));
  // Beginning the tick folds queued retractions in; admission covers them too.
  const changedBindings = [
    ...bindingDeltas,
    ...(bindingDeltas.length ? rt.pendingBindingRetractions : []),
  ].map((d) => d.key);
  const plan = rt.graph.activationPlan(changedTables, changedBindings);
  if (plan.missing !== undefined) throw IvmRuntimeError.of("GraphNodeNotFound", plan.missing);
  const affectedNodes: Set<NodeId> = plan.affected;
  // Do not begin a resident tick overlapping a hydration's graph slice; a
  // hydration parked on a chunk fetch is set aside instead (#3901), unless
  // this is a binding tick whose snapshot would go stale.
  const abandonParked = bindingDeltas.length === 0;
  const abandoned: PendingSubscriptionHydration[] = [];
  let admittedError: unknown;
  try {
    await pollFn<void>((cx) => {
      let residentOnly = false;
      for (;;) {
        if (!rt.pendingIncremental.state.hydrationAdmissionEvaluations(affectedNodes).size) return;
        const selected = rt.pendingIncremental.state.hydrationAdmissionEvaluations(affectedNodes);
        pollIncremental(rt, cx, residentOnly, selected);
        const pending = rt.pendingIncremental.state;
        const remaining = pending.hydrationAdmissionEvaluations(affectedNodes);
        if (!remaining.size) return;
        if (![...remaining].some((id) => pending.evaluations.get(id)?.hasResidentContinuation())) {
          if (!abandonParked) return PENDING;
          const parked = [...remaining].filter((id) => {
            const e = pending.evaluations.get(id);
            return (
              e instanceof PendingSubscriptionHydration && e.session.requests.hasPendingChunk()
            );
          });
          if (!parked.length) return PENDING;
          // Local writes never wait on chunk fetches (#3901).
          abandoned.push(...abandonPendingHydrations(rt, parked));
          residentOnly = false;
          continue;
        }
        // The direct write owns CPU-only continuations of an overlapping hydration.
        residentOnly = true;
      }
    });
  } catch (e) {
    admittedError = e;
  }
  const restartStorage = storage.clone();
  const restarted = new Set(abandoned.map((h) => h.subscriptionId));
  try {
    if (admittedError !== undefined) throw admittedError;
    return await tickAdmitted(
      rt,
      tableDeltas,
      bindingDeltas,
      storage,
      deferNotificationsUntilDurable,
      publication,
      detachOn,
      restarted,
    );
  } finally {
    // Restart on every outcome: an abandoned subscription still awaits its first result.
    restartAbandonedHydrations(rt, abandoned, restartStorage);
  }
}

/** The part of tickDetachingCold after hydration admission. */
async function tickAdmitted(
  rt: IvmRuntime,
  tableDeltas: TableDelta[],
  bindingDeltas: BindingDelta[],
  storage: OwnedStorage,
  deferNotificationsUntilDurable: boolean,
  publication: PendingResidentPublication | undefined,
  detachOn: DetachOn,
  restartedSubscriptions: Set<SubscriptionId>,
): Promise<[TickMetrics, DurableWrites]> {
  const blockers = temporalBlockersOf(rt);
  const evaluation = await beginTickWithParamsAndNotificationPolicy(
    rt,
    tableDeltas,
    bindingDeltas,
    storage,
    undefined,
    deferNotificationsUntilDurable,
    publication,
  );
  evaluation.workQueue.addTemporalBlockers(blockers);
  evaluation.restartedSubscriptions = restartedSubscriptions;
  type Progress = { ok: true } | { ok: false; failure: EvaluationFailure } | { parked: true };
  const progress = await pollFn<Progress>((cx) => {
    for (;;) {
      let p: Poll<void>;
      try {
        p = evaluation.poll(rt, cx);
      } catch (e) {
        const failure = e as EvaluationFailure;
        if (failure.kind === EvaluationFailureKind.Scoped) {
          failEvaluationNodes(rt, failure);
          evaluation.abandon(failure.affectedNodes);
          continue;
        }
        return { ok: false, failure };
      }
      if (p !== PENDING) return { ok: true };
      // A resumable operator yielded while runnable: keep this future alive.
      if (evaluation.workQueue.runnable.length) return PENDING;
      // Only a chunk fetch, which may need this very turn, is worth detaching.
      if (detachOn === DetachOn.ChunkRequest && !evaluation.requests.hasPendingChunk())
        return PENDING;
      return { parked: true };
    }
  });
  const metrics = evaluation.metrics;
  const durableWrites = evaluation.durableWrites;
  if ("ok" in progress) {
    if (!progress.ok) throw progress.failure.error;
  } else {
    evaluation.workQueue.discardUnregisteredCompletions();
    evaluation.installInputFrontiers(rt);
    // Write finished state back now, or the next write would rerun its
    // transitions from pre-park state (#3815).
    evaluation.writeBackOrHold(rt, evaluation.workQueue.completeNodes());
    // A later evaluation restages this state, so it must begin at a later tick.
    rt.currentTick = Math.max(rt.currentTick, evaluation.currentTick);
    const pending = rt.pendingIncremental.state;
    const id = pending.nextId++;
    pending.register(id, [...evaluation.workQueue.incompleteNodes(), ...evaluation.heldBarriers]);
    pending.evaluations.set(id, evaluation);
    pending.order.push(id);
  }
  return [metrics, durableWrites];
}

export function assignResidentPublication(
  rt: IvmRuntime,
  tick: ResidentTick,
  publication: PublicationId,
): TickMetrics {
  if (tick.publication.publication.value !== undefined)
    throw new Error("assertion failed: publication already assigned");
  tick.publication.publication.value = publication;
  const notifications = tick.publication.notifications;
  tick.publication.notifications = [];
  for (const [, queued] of notifications) queued.publication = publication;
  if (tick.publication.deferNotificationsUntilDurable) {
    let list = rt.deferredNotifications.get(publication);
    if (!list) rt.deferredNotifications.set(publication, (list = []));
    list.push(...notifications);
    if (tick.publication.completed.value) rt.completedDeferredPublications.add(publication);
  } else sendDeferredNotifications(rt, notifications);
  return tick.metrics;
}

export function settleDeferredNotifications(rt: IvmRuntime, publication: PublicationId): void {
  if (!rt.completedDeferredPublications.delete(publication))
    rt.durableNotificationPublications.add(publication);
  const notifications = rt.deferredNotifications.get(publication);
  if (!notifications) return;
  rt.deferredNotifications.delete(publication);
  sendDeferredNotifications(rt, notifications);
}

function sendDeferredNotifications(
  rt: IvmRuntime,
  notifications: [SubscriptionId, QueuedMultisinkDeltas][],
): void {
  const dropped: SubscriptionId[] = [];
  for (const [id, queued] of notifications) {
    const s = rt.multisinkSubscriptions.get(id);
    if (s && !s.sender.send(queued)) dropped.push(id);
  }
  for (const id of dropped) unsubscribe(rt, id);
}

export function discardDeferredNotifications(rt: IvmRuntime, publication: PublicationId): void {
  rt.deferredNotifications.delete(publication);
  rt.completedDeferredPublications.delete(publication);
  rt.durableNotificationPublications.delete(publication);
}

export function pollPendingIncremental(rt: IvmRuntime, cx: Waker): Poll<void> {
  return pollIncremental(rt, cx, false, undefined);
}

/** Poll only evaluations with an in-memory continuation; cold ones stay untouched. */
export function pollResidentIncremental(rt: IvmRuntime, cx: Waker): Poll<void> {
  return pollIncremental(rt, cx, true, undefined);
}

function pollIncremental(
  rt: IvmRuntime,
  caller: Waker,
  residentOnly: boolean,
  selected: Set<number> | undefined,
): Poll<void> {
  if (rt.pendingIncrementalPolling) throw new Error("pending evaluation polling is not reentrant");
  rt.pendingIncrementalPolling = true;
  // Suspended futures keep only their latest waker; bridge to the owner too.
  const cx = ownerFanoutWakerFor(rt, caller) ?? caller;
  const slot = rt.pendingIncremental;
  const state = slot.state;
  slot.state = new PendingIncrementalState();
  const finish = (result: Poll<void>, error?: unknown): Poll<void> => {
    slot.state = state;
    rt.pendingIncrementalPolling = false;
    // Lifecycle operations may have released retainers during this turn.
    if (rt.ephemeralGraphGcPending) collectUnretainedEphemeralNodes(rt);
    if (error !== undefined) throw error;
    return result;
  };
  if (!state.order.length) return finish(undefined);
  const retained: number[] = [];
  while (state.order.length) {
    const id = state.order.shift()!;
    const evaluation = state.evaluations.get(id)!;
    state.evaluations.delete(id);
    if (
      (selected && !selected.has(id)) ||
      (residentOnly && !evaluation.hasResidentContinuation())
    ) {
      state.evaluations.set(id, evaluation);
      retained.push(id);
      continue;
    }
    let progress: Poll<void>;
    let failure: EvaluationFailure | undefined;
    try {
      if (evaluation instanceof IncrementalEvaluation) progress = evaluation.poll(rt, cx);
      else {
        try {
          progress = evaluation.session.poll(
            rt,
            evaluation.bindingSnapshots,
            evaluation.hydrateArrangements,
            evaluation.metrics,
            cx,
          );
        } catch (e) {
          throw new EvaluationFailure(
            EvaluationFailureKind.Scoped,
            new Set(evaluation.session.workQueue.incompleteNodes()),
            IvmRuntimeError.from(e),
          );
        }
      }
    } catch (e) {
      failure = EvaluationFailure.fatal(e);
      progress = undefined;
    }
    // A hydration session is one temporal barrier; incremental completions
    // are handed off once their state is live (#3815).
    if (evaluation instanceof IncrementalEvaluation) {
      const completed = evaluation.workQueue.drainCompletedEvents();
      if (failure) for (const n of completed) evaluation.heldBarriers.add(n);
      else if (progress === PENDING)
        state.releaseTemporalSuccessors(rt, id, evaluation.writeBackOrHold(rt, completed));
      else {
        const released = [...completed, ...evaluation.heldBarriers];
        evaluation.heldBarriers.clear();
        state.releaseTemporalSuccessors(rt, id, released);
      }
    }
    if (failure) {
      if (evaluation instanceof PendingSubscriptionHydration) {
        failSubscription(rt, evaluation.subscriptionId, failure.error);
        unsubscribe(rt, evaluation.subscriptionId);
      }
      if (failure.kind === EvaluationFailureKind.Fatal) {
        if (!(failure.error.kind === "Chunk"))
          throw new Error("trusted install failures originate from chunk requests");
        failAllSubscriptions(rt, failure.error);
        state.order = retained;
        return finish(undefined, failure.error);
      }
      // Reset held nodes too, so a later write rehydrates them.
      if (evaluation instanceof IncrementalEvaluation)
        for (const n of evaluation.heldBarriers) failure.affectedNodes.add(n);
      // A failed first-result session never published mutable state.
      if (
        !(
          evaluation instanceof PendingSubscriptionHydration &&
          evaluation.lifetime === SubscriptionLifetime.FirstResult
        )
      ) {
        failEvaluationNodes(rt, failure);
      }
      let releasedNodes: NodeId[];
      if (evaluation instanceof PendingSubscriptionHydration)
        releasedNodes = [...evaluation.session.workQueue.registeredNodes()];
      else {
        releasedNodes = [...evaluation.workQueue.incompleteNodes(), ...evaluation.heldBarriers];
        evaluation.heldBarriers.clear();
      }
      state.releaseWaiter(rt, id, releasedNodes, false);
    } else if (progress !== PENDING) {
      if (evaluation instanceof PendingSubscriptionHydration)
        completeHydration(rt, state, id, evaluation);
    } else {
      state.evaluations.set(id, evaluation);
      retained.push(id);
      // A resident slice keeps looking for unrelated resident work.
      if (residentOnly) continue;
      // One owner turn advances at most one suspended evaluation.
      state.order = [...retained, ...state.order];
      return finish(PENDING);
    }
  }
  state.order = retained;
  return finish(retained.length ? PENDING : undefined);
}

function completeHydration(
  rt: IvmRuntime,
  state: PendingIncrementalState,
  id: number,
  hydration: PendingSubscriptionHydration,
): void {
  let snapshot: MultisinkDeltas | undefined;
  let error: IvmRuntimeError | undefined;
  try {
    snapshot = subscriptionSnapshotFromHydrated(
      rt.graph,
      hydration.outputs,
      hydration.session.outputs,
      hydration.session.terminalDeltas,
    );
  } catch (e) {
    error = IvmRuntimeError.from(e);
  }
  const completed = hydration.session.workQueue.registeredNodes();
  // First-result state is not a maintenance proof; never install it.
  if (hydration.lifetime === SubscriptionLifetime.Retained) hydration.session.install(rt);
  state.releaseTemporalSuccessors(rt, id, completed);
  recordHydrationMemoMetrics(rt, hydration.metrics);
  evictEvalMemo(rt);
  if (snapshot) {
    hydration.initial.value = snapshot;
    if (hydration.lifetime === SubscriptionLifetime.FirstResult)
      unsubscribe(rt, hydration.subscriptionId);
  } else {
    failSubscription(rt, hydration.subscriptionId, error!);
    unsubscribe(rt, hydration.subscriptionId);
  }
}

/** Remember the runtime owner's wake bridge for cold work polled by later callers. */
export function retainOwnerProgressWaker(rt: IvmRuntime, owner: Waker): void {
  if (rt.ownerProgressWaker === owner) return;
  rt.ownerProgressWaker = owner;
  rt.ownerFanoutWaker = undefined;
}

function ownerFanoutWakerFor(rt: IvmRuntime, caller: Waker): Waker | undefined {
  const owner = rt.ownerProgressWaker;
  if (!owner || owner === caller) return undefined;
  if (rt.ownerFanoutWaker && rt.ownerFanoutWaker[0] === caller) return rt.ownerFanoutWaker[1];
  const fanout = () => {
    caller();
    owner();
  };
  rt.ownerFanoutWaker = [caller, fanout];
  return fanout;
}

/** Finish prepared durable writes in temporal order without advancing query roots. */
export function pollStorageExtraction(
  rt: IvmRuntime,
  storage: OwnedStorage,
  cx: Waker,
): Poll<void> {
  const pending = rt.pendingIncremental.state;
  for (const id of pending.order.slice()) {
    const e = pending.evaluations.get(id);
    if (
      e instanceof IncrementalEvaluation &&
      e.pollStorageFlush(rt.persistenceIndeterminate, storage.clone(), cx) === PENDING
    ) {
      return PENDING;
    }
  }
}

export function hasPendingStorageWrites(rt: IvmRuntime): boolean {
  for (const e of rt.pendingIncremental.state.evaluations.values()) {
    if (e instanceof IncrementalEvaluation && (e.persistFlush || !e.durableWrites.state.isEmpty()))
      return true;
  }
  return false;
}

export function hasPendingIncremental(rt: IvmRuntime): boolean {
  return rt.pendingIncremental.isPending();
}

/** Whether an admitted evaluation can still change this receiver's terminal. */
export function subscriptionHasPendingProgress(rt: IvmRuntime, id: SubscriptionId): boolean {
  const s = rt.multisinkSubscriptions.get(id);
  return !s || s.failed || subscriptionHasPendingEvaluation(rt, id);
}

/** Whether admitted work (including durable-deferred notifications) can still reach `id`. */
export function subscriptionHasPendingEvaluation(rt: IvmRuntime, id: SubscriptionId): boolean {
  if (rt.pendingIncrementalPolling) return true;
  for (const e of rt.pendingIncremental.state.evaluations.values()) {
    if (
      e instanceof PendingSubscriptionHydration
        ? e.subscriptionId === id
        : e.affectedSubscriptions.has(id)
    )
      return true;
  }
  for (const list of rt.deferredNotifications.values())
    if (list.some(([s]) => s === id)) return true;
  return false;
}

export function hasResidentContinuation(rt: IvmRuntime): boolean {
  return rt.pendingIncremental.hasResidentContinuation();
}

/** Drop an uninstalled hydration when its subscription is cancelled. */
export function cancelPendingSubscriptionHydration(
  rt: IvmRuntime,
  subscriptionId: SubscriptionId,
): void {
  const cancelled: number[] = [];
  for (const [id, e] of rt.pendingIncremental.state.evaluations) {
    if (e instanceof PendingSubscriptionHydration && e.subscriptionId === subscriptionId)
      cancelled.push(id);
  }
  removePendingHydrations(rt, cancelled);
}

/** Remove uninstalled hydrations, releasing successors without installing state. */
function removePendingHydrations(rt: IvmRuntime, ids: number[]): PendingSubscriptionHydration[] {
  const slot = rt.pendingIncremental;
  const state = slot.state;
  slot.state = new PendingIncrementalState();
  const removed: PendingSubscriptionHydration[] = [];
  for (const id of ids) {
    const evaluation = state.evaluations.get(id);
    if (!evaluation) continue;
    state.evaluations.delete(id);
    state.order = state.order.filter((c) => c !== id);
    state.releaseWaiter(rt, id, evaluation.workQueueRef().registeredNodes(), true);
    if (evaluation instanceof PendingSubscriptionHydration) removed.push(evaluation);
  }
  slot.state = state;
  return removed;
}

/**
 * Set aside hydrations parked on a large-value chunk fetch so an overlapping
 * local write can begin (#3901). Local writes never wait on chunk fetches.
 */
function abandonPendingHydrations(rt: IvmRuntime, ids: number[]): PendingSubscriptionHydration[] {
  const abandoned = removePendingHydrations(rt, ids);
  for (const h of abandoned) {
    if (h.lifetime === SubscriptionLifetime.Retained)
      unindexSubscriptionOutputs(rt, h.subscriptionId, h.outputs);
  }
  return abandoned;
}

/** Restart abandoned hydrations from the post-write snapshot, handing off chunk fetches. */
function restartAbandonedHydrations(
  rt: IvmRuntime,
  abandoned: PendingSubscriptionHydration[],
  storage: OwnedStorage,
): void {
  for (const h of abandoned) {
    if (!rt.multisinkSubscriptions.has(h.subscriptionId)) continue;
    if (h.lifetime === SubscriptionLifetime.Retained)
      indexSubscriptionOutputs(rt, h.subscriptionId, h.outputs);
    let id: number;
    try {
      id = enqueueSubscriptionHydration(
        rt,
        h.subscriptionId,
        h.outputs,
        storage.clone(),
        h.bindingSnapshots,
        h.bindingFrontierAdvance,
        h.initial,
        h.lifetime,
        h.session.rootIndirectValues,
        h.session.borrowed,
      );
    } catch (e) {
      failSubscription(rt, h.subscriptionId, IvmRuntimeError.from(e));
      unsubscribe(rt, h.subscriptionId);
      continue;
    }
    const restarted = rt.pendingIncremental.state.evaluations.get(id);
    if (!(restarted instanceof PendingSubscriptionHydration))
      throw new Error("a restarted hydration is queued under its id");
    h.session.requests.handOffChunks(restarted.session.requests);
    h.session.evaluationInputs.handOffChunks(restarted.session.evaluationInputs);
  }
}

export function drivePendingIncremental(rt: IvmRuntime): Promise<void> {
  return pollFn((cx) => pollPendingIncremental(rt, cx));
}

export async function tickWithParams(
  rt: IvmRuntime,
  tableDeltas: TableDelta[],
  bindingDeltas: BindingDelta[],
  storage: OwnedStorage,
  notificationPublication: PublicationId | undefined,
): Promise<TickMetrics> {
  if (rt.persistenceIndeterminate.value)
    throw IvmRuntimeError.of("PersistenceOutcomeIndeterminate");
  await drivePendingIncremental(rt);
  const evaluation = await beginTickWithParamsAndNotificationPolicy(
    rt,
    tableDeltas,
    bindingDeltas,
    storage,
    notificationPublication,
    false,
    undefined,
  );
  try {
    await pollFn((cx) => evaluation.poll(rt, cx));
  } catch (e) {
    throw e instanceof EvaluationFailure ? e.error : e;
  }
  return evaluation.metrics;
}

async function beginTickWithParamsAndNotificationPolicy(
  rt: IvmRuntime,
  tableDeltas: TableDelta[],
  bindingDeltas: BindingDelta[],
  storage: OwnedStorage,
  notificationPublication: PublicationId | undefined,
  deferNotificationsUntilDurable: boolean,
  pendingResidentPublication: PendingResidentPublication | undefined,
): Promise<IncrementalEvaluation> {
  const pendingBindingRetractions = rt.pendingBindingRetractions.length;
  if (pendingBindingRetractions)
    bindingDeltas = [...rt.pendingBindingRetractions, ...bindingDeltas];
  const activation = rt.graph.activationPlan(
    new Set(tableDeltas.map((d) => d.table)),
    bindingDeltas.map((d) => d.key),
  );
  if (activation.missing !== undefined)
    throw IvmRuntimeError.of("GraphNodeNotFound", activation.missing);
  const affectedNodes: Set<NodeId> = new Set(activation.affected);
  // Capture only the reached graph slice (plus ancestors), by graph key.
  const relevantNodes: Set<NodeId> = new Set(activation.relevant);
  const staged = stageStateForNodes(rt, relevantNodes);
  // Tick memo entries are disposable.
  const evalMemo = new EvaluationMemo();
  const nodeMeta = new Map<NodeId, NodeRuntimeMeta>();
  for (const node of relevantNodes) {
    const meta = rt.nodeMeta.get(node);
    if (meta) nodeMeta.set(node, cloneNodeMeta(meta));
  }
  const tableFrontiers = new Map<string, number>();
  const bindingFrontiers = new KeyedMap<BindingSourceKey, number>(BSK.key);
  for (const table of activation.tables as string[]) {
    const f = rt.tableFrontiers.get(table);
    if (f !== undefined) tableFrontiers.set(table, f);
  }
  for (const binding of activation.bindings as BindingSourceKey[]) {
    const f = rt.bindingFrontiers.get(binding);
    if (f !== undefined) bindingFrontiers.set(binding, f);
  }
  const currentTick = rt.currentTick + 1;
  const durableWrites: DurableWrites = { state: new StagedWriteState() };
  const bindingSnapshots = bindingSnapshotDeltas(rt);
  const scratch: EvaluatorScratch = {
    ...staged,
    evalMemo,
    evalMemoBytes: 0,
    memoUseClock: rt.memoUseClock,
    nodeMeta,
  };
  await tickDurableNodes(
    rt,
    tableDeltas,
    bindingSnapshots,
    activation.durable,
    currentTick,
    storage.asRef(),
    scratch,
    tableFrontiers,
    bindingFrontiers,
    durableWrites,
  );
  bumpInputFrontiersStaged(
    rt.graph,
    tableDeltas,
    bindingDeltas,
    tableFrontiers,
    bindingFrontiers,
    nodeMeta,
  );
  const affectedSubscriptions = new Set<SubscriptionId>();
  for (const node of affectedNodes)
    for (const s of rt.subscriptionsByOutputNode.get(node) ?? []) affectedSubscriptions.add(s);
  const metrics = new TickMetrics();
  metrics.tick = currentTick;
  metrics.tableDeltaRecords = tableDeltas.reduce((n, d) => n + d.deltas.length, 0);
  metrics.subscriptionsConsidered = affectedSubscriptions.size;
  // Only plain outputs carrying TopBy row identity consume root windows.
  const rootOrderingWindows = new Map<NodeId, RootOrderingWindows>();
  const affectedOutputs = () =>
    [...affectedSubscriptions]
      .flatMap((id) => rt.multisinkSubscriptions.get(id)?.outputs.valuesArray() ?? [])
      .filter((o) => affectedNodes.has(o.node));
  if (rt.plainOutputRootPositions) {
    for (const output of affectedOutputs()) {
      const ordering = output.rootOrderingNode;
      if (
        ordering !== undefined &&
        outputConsumesRootPositions(rt.graph, output.node, ordering) &&
        !rootOrderingWindows.has(ordering)
      ) {
        rootOrderingWindows.set(ordering, new RootOrderingWindows());
      }
    }
    // A routed TopBy must collect positions for any bound output it may reach.
    for (const terminal of activation.routed as NodeId[]) {
      for (const node of rt.graph.routes().table(terminal)?.rootOrderingNodes ?? []) {
        if (!rootOrderingWindows.has(node))
          rootOrderingWindows.set(node, new RootOrderingWindows());
      }
    }
  }
  const activeRoots: NodeId[] = [];
  for (const output of affectedOutputs()) {
    activeRoots.push(output.node);
    if (output.rootOrderingNode !== undefined) activeRoots.push(output.rootOrderingNode);
  }
  for (const node of activation.ephemeral as NodeId[]) {
    if (rt.nodeMeta.get(node)?.retainers.some((r: any) => r.kind !== "Hydration"))
      activeRoots.push(node);
  }
  const workQueue = EvaluationWorkQueue.discoverFrame(rt.graph, rt.nodeMeta, activeRoots, false);
  evalMemo.setLayout(workQueue.layout);
  return new IncrementalEvaluation(
    tableDeltas,
    bindingDeltas,
    bindingSnapshots,
    tableFrontiers,
    bindingFrontiers,
    currentTick,
    metrics,
    storage,
    workQueue,
    relevantNodes,
    affectedSubscriptions,
    affectedNodes,
    rootOrderingWindows,
    notificationPublication,
    deferNotificationsUntilDurable,
    pendingResidentPublication,
    scratch.operatorStates,
    scratch.arrangementStates,
    scratch.arrangementKeysByInput,
    scratch.evalMemo,
    scratch.evalMemoBytes,
    scratch.memoUseClock,
    scratch.nodeMeta,
    pendingBindingRetractions,
    durableWrites,
    (activation.routed as NodeId[]).slice(),
  );
}

export function bumpInputFrontiers(
  rt: IvmRuntime,
  tableDeltas: TableDelta[],
  bindingDeltas: BindingDelta[],
): void {
  bumpInputFrontiersStaged(
    rt.graph,
    tableDeltas,
    bindingDeltas,
    rt.tableFrontiers,
    rt.bindingFrontiers,
    rt.nodeMeta,
  );
}

export function evictEvalMemo(rt: IvmRuntime): void {
  const within = () =>
    rt.evalMemo.len() <= EVAL_MEMO_MAX_ENTRIES && rt.evalMemoBytes <= EVAL_MEMO_MAX_BYTES;
  if (within()) return;
  const entries: [any, number][] = [...rt.evalMemo.entries()].map(([k, e]: [any, any]) => [
    k,
    e.lastUsed,
  ]);
  entries.sort((a, b) => a[1] - b[1]);
  for (const [key] of entries) {
    if (within()) break;
    rt.removeRetainedEvalMemo(key);
  }
}

export function hydrationSnapshot(
  rt: IvmRuntime,
  outputNode: NodeId,
  storage: OrderedKvStorage,
  mode: HydrationMode,
): Promise<RecordDeltas> {
  return hydrationSnapshotWithRootValues(rt, outputNode, storage, mode, MATERIALIZE);
}

export async function hydrationSnapshotWithRootValues(
  rt: IvmRuntime,
  outputNode: NodeId,
  storage: OrderedKvStorage,
  mode: HydrationMode,
  rootIndirectValues: RootIndirectValues,
): Promise<RecordDeltas> {
  const out = await hydrationRootsOwned(
    rt,
    [outputNode],
    new OwnedStorage(storage),
    mode,
    undefined,
    undefined,
    rootIndirectValues,
  );
  const records = out.get(outputNode);
  if (!records) throw IvmRuntimeError.of("GraphNodeNotFound", outputNode);
  return records;
}

async function hydrationRootsOwned(
  rt: IvmRuntime,
  roots: NodeId[],
  storage: OwnedStorage,
  mode: HydrationMode,
  bindingSnapshots: BindingSnapshots | undefined,
  bindingFrontierAdvance: string | undefined,
  rootIndirectValues: RootIndirectValues,
): Promise<Map<NodeId, RecordDeltas>> {
  const snapshots = bindingSnapshots ?? bindingSnapshotDeltas(rt);
  const metrics = new TickMetrics();
  if (!roots.length) throw IvmRuntimeError.of("UnsupportedOperator");
  const hydrateArrangements =
    mode === HydrationMode.Subscription && roots.some((r) => outputDependsOnAggregate(rt, r));
  const session = EvaluationSession.hydration(rt, roots, storage);
  session.rootIndirectValues = rootIndirectValues;
  if (bindingFrontierAdvance !== undefined)
    session.advanceBindingInput(rt.graph, bindingFrontierAdvance);
  await pollFn((cx) => session.poll(rt, snapshots, hydrateArrangements, metrics, cx));
  const outputs = session.outputs;
  session.outputs = new Map();
  session.install(rt);
  recordHydrationMemoMetrics(rt, metrics);
  evictEvalMemo(rt);
  return outputs;
}

export function hydrationSnapshots(
  rt: IvmRuntime,
  outputs: SortedMap<string, CompiledNode>,
  storage: OrderedKvStorage,
  mode: HydrationMode,
): Promise<MultisinkDeltas> {
  return hydrationSnapshotsWithBindingSnapshots(rt, outputs, storage, mode, undefined, undefined);
}

async function hydrationSnapshotsWithBindingSnapshots(
  rt: IvmRuntime,
  outputs: SortedMap<string, CompiledNode>,
  storage: OrderedKvStorage,
  mode: HydrationMode,
  bindingSnapshots: BindingSnapshots | undefined,
  bindingFrontierAdvance: string | undefined,
): Promise<MultisinkDeltas> {
  const hydrated = await hydrationRootsOwned(
    rt,
    uniqueRoots(outputs),
    new OwnedStorage(storage),
    mode,
    bindingSnapshots,
    bindingFrontierAdvance,
    MATERIALIZE,
  );
  return subscriptionSnapshotFromHydrated(rt.graph, outputs, hydrated, new Map());
}

function outputDependsOnAggregate(rt: IvmRuntime, outputNode: NodeId): boolean {
  const ancestors = new Set<NodeId>();
  rt.graph.markAncestors(outputNode, ancestors);
  for (const ancestor of ancestors) {
    const node = rt.graph.node(ancestor);
    if (!node) throw IvmRuntimeError.of("GraphNodeNotFound", ancestor);
    const kind = node.descriptor.operator.kind;
    if (kind === "Aggregate" || kind === "ArgMinBy" || kind === "ArgMaxBy") return true;
  }
  return false;
}

async function tickDurableNodes(
  rt: IvmRuntime,
  tableDeltas: TableDelta[],
  bindingSnapshots: BindingSnapshots,
  durableNodes: NodeId[],
  currentTick: number,
  storage: OrderedKvStorage,
  scratch: EvaluatorScratch,
  tableFrontiers: Map<string, number>,
  bindingFrontiers: KeyedMap<BindingSourceKey, number>,
  durableWrites: DurableWrites,
): Promise<void> {
  const overlay = new StagedWriteOverlay(storage, durableWrites);
  const metrics = new TickMetrics();
  for (const node of durableNodes) {
    const graphNode = rt.graph.node(node);
    if (!graphNode) throw IvmRuntimeError.of("GraphNodeNotFound", node);
    const op = graphNode.descriptor.operator;
    if (op.kind !== "Persist") throw IvmRuntimeError.of("UnsupportedOperator");
    const inputs: NodeId[] = graphNode.descriptor.inputs;
    if (inputs.length !== 1) throw IvmRuntimeError.of("GraphInputArityMismatch", node);
    const evaluator = makeEvaluator(rt, scratch, {
      tableDeltas,
      bindingDeltas: [],
      bindingSnapshots,
      currentTick,
      tableFrontiers,
      bindingFrontiers,
      storage: overlay,
      evaluationInputs: undefined,
      context: EvalContext.root(),
      metrics,
      terminalDeltas: new Map(),
      rootOrderingWindows: new Map(),
    });
    let input: RecordDeltas;
    try {
      input = await evaluator.updateNode(inputs[0]);
    } finally {
      takeBack(scratch, evaluator);
    }
    await applyPersistDelta(overlay, op.op.storage, op.op.keyFields, op.op.unique, input);
  }
}

// ---- module helpers ----

function subscriptionSnapshotFromHydrated(
  graph: IvmGraph,
  outputs: SortedMap<string, CompiledNode>,
  hydrated: Map<NodeId, RecordDeltas>,
  terminalDeltas: Map<NodeId, TerminalDeltas>,
): MultisinkDeltas {
  const result = new MultisinkDeltas();
  for (const [sink, output] of outputs) {
    let ordering: RecordDeltas | undefined;
    if (output.rootOrderingNode !== undefined) {
      ordering = hydrated.get(output.rootOrderingNode);
      if (!ordering) throw IvmRuntimeError.of("GraphNodeNotFound", output.rootOrderingNode);
    }
    const found = hydrated.get(output.node);
    if (!found) throw IvmRuntimeError.of("GraphNodeNotFound", output.node);
    const records: RecordDeltas = { descriptor: found.descriptor, deltas: found.deltas.slice() };
    if (!records.descriptor.registryCompatibleWith(output.output))
      throw IvmRuntimeError.of("GraphOutputMismatch");
    if (ordering) orderTerminalSnapshot(records, ordering);
    const [hasPublicCollector, seeded] = terminalDeltaForHydratedOutput(
      graph,
      output.node,
      terminalDeltas,
    );
    // A hydrated root collector seeds its opening through the terminal representation.
    const terminal =
      seeded ?? (hasPublicCollector ? terminalDeltasFromRecordDeltas(records) : undefined);
    if (terminal && !terminal.isEmpty()) result.terminalSinks.set(sink, terminal);
    result.sinks.set(sink, records);
  }
  return result;
}

/** Locate the public collector of one output and return its hydration seed. */
function terminalDeltaForHydratedOutput(
  graph: IvmGraph,
  output: NodeId,
  terminalDeltas: Map<NodeId, TerminalDeltas>,
): [boolean, TerminalDeltas | undefined] {
  const pending = [output];
  const seen = new Set<NodeId>();
  let fallback: TerminalDeltas | undefined;
  let hasPublicCollector = false;
  while (pending.length) {
    const node = pending.pop()!;
    if (seen.has(node)) continue;
    seen.add(node);
    const g = graph.node(node);
    if (!g) throw IvmRuntimeError.of("GraphNodeNotFound", node);
    const op = g.descriptor.operator;
    const isPublic = op.kind === "CollectBy" && (op.op.mode === "Collect" || op.op.mode === "Root");
    hasPublicCollector ||= isPublic;
    const terminal = terminalDeltas.get(node);
    if (terminal) {
      if (isPublic) return [true, terminal];
      fallback ??= terminal;
    }
    pending.push(...g.descriptor.inputs);
  }
  return [hasPublicCollector, hasPublicCollector ? undefined : fallback];
}

/** Route barriers reached by this tick's shared-terminal deltas (#3288). */
function touchedRouteBarriers(
  evaluator: TickEvaluator,
  graph: IvmGraph,
  routed: NodeId[],
  cx: Waker,
): Set<NodeId> {
  const touched = new Set<NodeId>();
  const touchAll = (table: any) => {
    for (const b of table.barriers()) touched.add(b);
  };
  for (const terminal of routed) {
    const table = graph.routes().table(terminal);
    if (!table) continue;
    let records: RecordDeltas | undefined;
    try {
      const polled = evaluator.pollUpdateNode(terminal, cx);
      // Only route fields key a barrier; materialize just those.
      if (polled !== PENDING)
        records = evaluator.materializeIndirectFieldIndices(polled, table.fieldIndices);
    } catch {
      records = undefined;
    }
    if (!records || !graph.node(terminal)?.descriptor.output.records().equals(records.descriptor)) {
      touchAll(table);
      continue;
    }
    for (const delta of records.deltas) {
      const key = table.keyOfRecord(new BorrowedRecord(delta.record, records.descriptor));
      if (key === undefined) {
        touchAll(table);
        break;
      }
      for (const b of table.byKey.get(key) ?? []) touched.add(b);
    }
  }
  return touched;
}

function bumpInputFrontiersStaged(
  graph: IvmGraph,
  tableDeltas: TableDelta[],
  bindingDeltas: BindingDelta[],
  tableFrontiers: Map<string, number>,
  bindingFrontiers: KeyedMap<BindingSourceKey, number>,
  nodeMeta: Map<NodeId, NodeRuntimeMeta>,
): void {
  const changedTables: string[] = [];
  for (const d of tableDeltas) {
    if (!d.deltas.length) continue;
    tableFrontiers.set(d.table, (tableFrontiers.get(d.table) ?? 0) + 1);
    changedTables.push(d.table);
  }
  const changedBindings: BindingSourceKey[] = [];
  for (const d of bindingDeltas) {
    if (!d.deltas.length && !d.initializesSnapshot) continue;
    bindingFrontiers.set(d.key, (bindingFrontiers.get(d.key) ?? 0) + 1);
    changedBindings.push(d.key);
  }
  if (!changedTables.length && !changedBindings.length) return;
  for (const node of graph.affectedNodes(changedTables, changedBindings) as Set<NodeId>) {
    let meta = nodeMeta.get(node);
    if (!meta) nodeMeta.set(node, (meta = defaultNodeMeta()));
    meta.inputGeneration++;
  }
}

/** Fold an operator state's uncommitted overlays into its base. */
function commitOperatorState(state: OperatorState, recursive = true): void {
  switch (state.kind) {
    case "Recursive":
      if (recursive) state.value.value.commitStagedPositive();
      break;
    case "TopBy":
      state.value.value.commitOverlays();
      break;
    case "ArgBy":
      state.value.value.commitOverlay();
      break;
    case "SemiJoin":
    case "AntiJoin":
      state.value.commitPublishedOverlay();
      break;
    case "CollectBy":
      state.value.groups.commitOverlay();
      break;
  }
}

/** A written-back arrangement's component: its recursive owner, else its Arrange node. */
function arrangementOwner(key: ArrangementKey): NodeId {
  return key.scope.outermost() ?? key.input;
}

/**
 * Fold live lifecycle state (retainers, generations) into a snapshot before
 * it replaces the live entries; drop entries for nodes collected meanwhile.
 */
function carryLiveNodeLifecycle(
  snapshot: Map<NodeId, NodeRuntimeMeta>,
  rt: IvmRuntime,
  nodes: Iterable<NodeId>,
): void {
  for (const node of nodes) {
    const meta = snapshot.get(node);
    const live = rt.nodeMeta.get(node);
    if (meta && live) {
      meta.retainers = live.retainers.slice();
      meta.inputGeneration = Math.max(meta.inputGeneration, live.inputGeneration);
    } else if (live) snapshot.set(node, cloneNodeMeta(live));
    else if (meta && !rt.graph.node(node)) snapshot.delete(node);
  }
}
