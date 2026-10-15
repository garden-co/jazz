// Graph retention, runtime statistics, garbage collection, and node initialization.
// These are `impl IvmRuntime` methods; the runtime class lives in the tick partition.
import {
  ScopeId,
  type IvmRuntime,
  type NodeId,
  type Retainer,
  type RuntimeStats,
  type TickMetrics,
} from "../../../_extern/groove-ivm-ops.js";
import { operatorStateFor } from "./evaluator.js";
import { NodeState } from "./operator_updates.js";

/** Owns unretained graph additions until an operation promotes them by adding retainers. */
export class EphemeralGraphInstall {
  private committed = false;
  constructor(private readonly rt: IvmRuntime) {}

  runtime(): IvmRuntime {
    return this.rt;
  }

  commit(): void {
    this.committed = true;
  }

  /** Rust `Drop`: callers run this in a `finally`. */
  dispose(): void {
    if (!this.committed) collectUnretainedEphemeralNodes(this.rt);
  }
}

export function executionLayoutStats(rt: IvmRuntime): { builds: number; hits: number } {
  const [builds, hits] = rt.graph.executionLayoutCounters();
  return { builds, hits };
}

const isRetainedGcRoot = (rt: IvmRuntime, node: any): boolean =>
  node.isDurable() || (rt.nodeMeta.get(node.id)?.retainers.size ?? 0) > 0;

export function retainedNodeIds(rt: IvmRuntime): Set<NodeId> {
  const retained = new Set<NodeId>();
  const roots = [...rt.graph.nodes().values()]
    .filter((node: any) => isRetainedGcRoot(rt, node))
    .map((n: any) => n.id);
  for (const root of roots) rt.graph.markAncestors(root, retained);
  // A queued evaluation's graph slice stays live after its subscription closes.
  for (const node of rt.pendingIncremental.registeredNodes())
    rt.graph.markAncestors(node, retained);
  return retained;
}

export function stats(rt: IvmRuntime): RuntimeStats {
  const out = cheapStats(rt);
  for (const [, arrangement] of rt.arrangementStates) {
    out.arrangementRows += arrangement.value.rowCount();
    out.arrangementEncodedBytes += arrangement.value.encodedBytes();
  }
  for (const [, state] of rt.operatorStates) {
    if (state.kind !== "Recursive") continue;
    out.recursiveStateCount++;
    out.recursiveAccumulatedRows += state.state.value.accumulatedRowCount();
    out.recursiveAccumulatedEncodedBytes += state.state.value.accumulatedEncodedBytes();
  }
  return out;
}

export function cheapStats(rt: IvmRuntime): RuntimeStats {
  let shapeParams = 0;
  for (const source of rt.bindingSources.values()) shapeParams += source.refcounts.size;
  return {
    graphNodes: rt.graph.nodes().size,
    activeSubscriptions: rt.multisinkSubscriptions.size,
    activePreparedShapes: rt.preparedShapes.size,
    activeShapeParams: shapeParams,
    arrangementCount: rt.arrangementStates.size,
    evalMemoEntries: rt.evalMemo.size,
    hydrationMemoEntries: rt.evalMemo.size - rt.evalMemo.tickEntries(),
    evalMemoBytes: rt.evalMemoBytes,
    hydrationMemoHits: rt.hydrationMemoHits,
    hydrationMemoComputes: rt.hydrationMemoComputes,
    hydrationMemoDistinctComputedNodes: rt.hydrationMemoComputedNodes.size,
    arrangementRows: 0,
    arrangementEncodedBytes: 0,
    recursiveStateCount: 0,
    recursiveAccumulatedRows: 0,
    recursiveAccumulatedEncodedBytes: 0,
    logicalNodesRequested: rt.logicalNodesRequested,
    dedupedGraphNodes: rt.graph.nodes().size,
  };
}

export function recordHydrationMemoMetrics(rt: IvmRuntime, metrics: TickMetrics): void {
  rt.hydrationMemoHits += metrics.hydrationMemoHits;
  rt.hydrationMemoComputes += metrics.hydrationMemoComputes;
  for (const node of metrics.hydrationMemoComputedNodes) rt.hydrationMemoComputedNodes.add(node);
}

function meta(rt: IvmRuntime, id: NodeId) {
  let m = rt.nodeMeta.get(id);
  if (!m) rt.nodeMeta.set(id, (m = NodeState.defaultMeta()));
  return m;
}

export function addRetainer(rt: IvmRuntime, id: NodeId, retainer: Retainer): boolean {
  if (!rt.graph.node(id)) return false;
  const m = meta(rt, id);
  m.lastUsedTick = rt.currentTick;
  const key = JSON.stringify(retainer);
  if (m.retainers.has(key)) return false;
  m.retainers.set(key, retainer);
  return true;
}

export function removeMultisinkRetainers(
  rt: IvmRuntime,
  subscriptionId: any,
  outputs: Map<string, any>,
): boolean {
  let removed = false;
  for (const output of outputs.values()) {
    removed =
      removeRetainer(rt, output.node, {
        kind: "Subscription",
        key: subscriptionId.retainerKey(),
      }) || removed;
    removed =
      removeRetainer(rt, output.node, { kind: "Hydration", key: subscriptionId.retainerKey() }) ||
      removed;
  }
  collectUnretainedEphemeralNodes(rt);
  return removed;
}

export function removeRetainer(rt: IvmRuntime, id: NodeId, retainer: Retainer): boolean {
  // Conservatively a GC candidate even when nothing was removed.
  rt.gcCandidates.add(id);
  return rt.nodeMeta.get(id)?.retainers.delete(JSON.stringify(retainer)) ?? false;
}

const enum Retention {
  None,
  Queued,
  Retained,
}

/** Remove every node no durable, retained or queued node reaches through its inputs. */
function gcEphemeralNodes(rt: IvmRuntime): [NodeId[], boolean] {
  const candidates: NodeId[] = [...rt.gcCandidates, ...rt.graph.takeAddedNodes()];
  rt.gcCandidates = new Set();
  const closure = new Set<NodeId>();
  for (const candidate of candidates) rt.graph.markAncestors(candidate, closure);
  for (const id of [...closure]) if (!rt.graph.node(id)) closure.delete(id);
  if (closure.size === 0) return [[], false];
  const queued: Set<NodeId> = rt.pendingIncremental.isPending()
    ? rt.pendingIncremental.registeredNodes()
    : new Set();
  // Decide consumers before their inputs: iterative post-order over children.
  const kept = new Map<NodeId, Retention>();
  let blockedByQueue = false;
  for (const start of closure) {
    if (kept.has(start)) continue;
    const stack: [NodeId, boolean][] = [[start, false]];
    while (stack.length) {
      const [id, childrenDone] = stack.pop()!;
      if (kept.has(id)) continue;
      const node = rt.graph.node(id);
      if (!childrenDone) {
        stack.push([id, true]);
        for (const child of node.children)
          if (closure.has(child) && !kept.has(child)) stack.push([child, false]);
        continue;
      }
      const children: NodeId[] = node.children;
      const retention =
        isRetainedGcRoot(rt, node) ||
        children.some((c) => !closure.has(c) || kept.get(c) === Retention.Retained)
          ? Retention.Retained
          : queued.has(id) || children.some((c) => kept.get(c) === Retention.Queued)
            ? Retention.Queued
            : Retention.None;
      blockedByQueue ||= retention === Retention.Queued;
      kept.set(id, retention);
    }
  }
  const removable = [...kept].filter(([, r]) => r === Retention.None).map(([id]) => id);
  for (const id of removable) rt.graph.removeNode(id);
  if (blockedByQueue) {
    // Queued work releases its nodes without a lifecycle event: revisit.
    for (const id of candidates) if (rt.graph.node(id)) rt.gcCandidates.add(id);
  }
  return [removable, blockedByQueue];
}

/** Reclaim unretained nodes only while all pending evaluator queues are visible. */
export function collectUnretainedEphemeralNodes(rt: IvmRuntime): void {
  rt.ephemeralGraphGcPending = true;
  if (rt.pendingIncrementalPolling) return;
  const [removedList, blockedByQueue] = gcEphemeralNodes(rt);
  if (removedList.length) {
    const removed = new Set(removedList);
    for (const [key] of [...rt.operatorStates])
      if (removed.has(key.node)) rt.operatorStates.delete(key);
    for (const [key] of [...rt.arrangementStates])
      if (removed.has(key.input)) rt.arrangementStates.delete(key);
    rt.removeRetainedEvalMemosForNodes(removed);
    for (const node of removed) {
      rt.arrangementKeysByInput.delete(node);
      rt.nodeMeta.delete(node);
    }
  }
  if (!blockedByQueue) rt.ephemeralGraphGcPending = false;
}

export function affectedRecursiveNodesAreCurrent(
  rt: IvmRuntime,
  affected: Set<NodeId>,
  currentTick: number,
): boolean {
  for (const node of affected) {
    const state = rt.operatorStates.get({ scope: ScopeId.root(), node });
    if (state?.kind === "Recursive" && state.state.asOf !== currentTick) return false;
  }
  return true;
}

export function initializeNodeRuntime(rt: IvmRuntime, node: NodeId): void {
  meta(rt, node);
  const graphNode = rt.graph.node(node);
  if (!graphNode) return;
  const state = operatorStateFor(graphNode.descriptor.operator);
  const key = { scope: ScopeId.root(), node };
  if (state.kind !== "Stateless" && !rt.operatorStates.has(key)) rt.operatorStates.set(key, state);
}
