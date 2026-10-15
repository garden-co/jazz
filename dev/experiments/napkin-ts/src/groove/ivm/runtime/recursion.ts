// Recursive operator state and scoped fixpoint evaluation.
//
// Owns RecursiveState, recursive frontier handling, step arrangement hydration,
// and the bounded positive-recursion loop. It reuses the main graph evaluator
// under recursive scopes rather than defining separate operators.
import { type Bytes, compareBytes, toHex } from "../../../support/bytes.js";
import { KeyedMap, SortedMap } from "../../../support/collections.js";
import {
  EvalContext,
  IvmRuntimeError,
  ScanRequest,
  consolidateDeltas,
  emptyDeltas,
  projectBindingSourceDeltas,
  rawProjectionFields,
  recordStoreForTable,
  scanBounds,
  type BindingDelta,
  type BindingSnapshots,
  type DatabaseSchema,
  type IvmGraph,
  type NodeId,
  type OrderedKvStorage,
  type RecordDelta,
  type RecordDeltas,
  type RecordDescriptor,
  type ScopeId,
  type TableDelta,
} from "../../../_extern/groove-ivm-ops.js";
import type { EvaluationInputs } from "./evaluation_session.js";
import { type GraphRuntimeView, planExprNames } from "./evaluator.js";
import { createJoinRecord, joinKeys, thresholdSnapshot, touchedJoinKeys } from "./join.js";
import { NodeState } from "./operator_updates.js";
import { type ArgByDirection, argByCandidateReplaces, encodedRecordKeyPart } from "./windows.js";

/** Encoded record (hex) -> weight; the record bytes ride along. */
export type Multiset = Map<string, RecordDelta>;

const positive = (set: Multiset, nonZero = false): RecordDelta[] =>
  [...set.values()].filter((d) => (nonZero ? d.weight !== 0 : d.weight > 0)).map((d) => ({ ...d }));

const closureTrace = () => (globalThis as any).JAZZ_CLOSURE_TRACE !== undefined;

const MAX_HYDRATION_TRAVERSAL_NODES_PER_POLL = 32;

/** The compiled inputs owned by one recursive operator. */
export interface RecursiveNodes {
  seed: NodeId;
  step: NodeId;
  stepWitness: NodeId | undefined;
}

type PendingHydrationPhase =
  | { kind: "Seed" }
  | { kind: "Step"; frontier: RecordDeltas; subTick: number }
  | { kind: "StepWitness"; frontier: RecordDeltas; stepDelta: RecordDeltas; subTick: number }
  | { kind: "ReadyForArrangementHydration" };

interface PendingHydrationRecompute {
  inputGeneration: number;
  accumulated: Multiset;
  stepWitnessAccumulated: Multiset;
  phase: PendingHydrationPhase;
  traversal?: HydrationTraversal;
}

export type HydrationRecomputeProgress = "Yield" | "ReadyForArrangementHydration";
export type RecursiveDeltaProgress = "Yield" | { ready: RecordDelta[] };

export interface HydrationRecomputeContext {
  schema: DatabaseSchema;
  graph: IvmGraph;
  variantProjections: any;
  /** Set routes source reads through the session request registry. */
  inputs: EvaluationInputs | undefined;
  tableDeltas: TableDelta[] | undefined;
  storage: OrderedKvStorage;
  bindingSnapshots: BindingSnapshots;
  scope: ScopeId;
  inputGeneration: number;
}

export class RecursiveState {
  /** Current recursive result; set-style, each reachable record kept at weight 1. */
  private accumulated: Multiset = new Map();
  /** New positive facts stay in this per-tick overlay until the owner publishes. */
  private stagedPositive: Multiset = new Map();
  /** Side records emitted by actual recursive step evaluation. */
  private stepWitnessAccumulated: Multiset = new Map();
  private stepWitnessTickDeltas: RecordDelta[] = [];
  private stepArrangementsHydratedFlag = false;
  private hydratedInputGenerationValue: number | undefined;
  pendingHydration: PendingHydrationRecompute | undefined;

  isEmpty(): boolean {
    return this.accumulated.size === 0;
  }
  stepArrangementsHydrated(): boolean {
    return this.stepArrangementsHydratedFlag;
  }
  accumulatedRowCount(): number {
    return [...this.accumulated.values()].filter((d) => d.weight !== 0).length;
  }
  accumulatedEncodedBytes(): number {
    let n = 0;
    for (const d of this.accumulated.values()) n += d.record.length;
    return n;
  }
  markStepArrangementsHydrated(): void {
    this.stepArrangementsHydratedFlag = true;
  }
  hydratedInputGeneration(): number | undefined {
    return this.hydratedInputGenerationValue;
  }
  markHydratedInputGeneration(generation: number): void {
    this.hydratedInputGenerationValue = generation;
  }
  hasPendingHydration(): boolean {
    return this.pendingHydration !== undefined;
  }
  hasPendingHydrationFor(generation: number): boolean {
    return this.pendingHydration?.inputGeneration === generation;
  }
  beginHydrationRecompute(inputGeneration: number): void {
    this.pendingHydration = {
      inputGeneration,
      accumulated: new Map(),
      stepWitnessAccumulated: new Map(),
      phase: { kind: "Seed" },
    };
  }
  pendingHydrationMut(): PendingHydrationRecompute {
    if (!this.pendingHydration) throw new Error("hydration recompute was initialized");
    return this.pendingHydration;
  }
  pendingHydrationAccumulatedDeltas(descriptor: RecordDescriptor): RecordDeltas {
    return { descriptor, deltas: positive(this.pendingHydrationMut().accumulated) };
  }
  finishHydrationRecompute(): [Multiset, Multiset] {
    const pending = this.pendingHydrationMut();
    this.pendingHydration = undefined;
    return [pending.accumulated, pending.stepWitnessAccumulated];
  }
  accumulatedDeltas(): RecordDelta[] {
    return positive(this.accumulated);
  }
  stepWitnessDeltas(): RecordDelta[] {
    return this.stepWitnessTickDeltas.map((d) => ({ ...d }));
  }
  stepWitnessAccumulatedDeltas(): RecordDelta[] {
    return positive(this.stepWitnessAccumulated, true);
  }
  appendStepWitness(deltas: RecordDelta[]): void {
    deltas = consolidateDeltas(deltas);
    if (deltas.length === 0) return;
    // Copy-on-write: a cloned AsOf may still share the previous map.
    const accumulated = new Map(this.stepWitnessAccumulated);
    const tick = [...this.stepWitnessTickDeltas];
    for (const delta of deltas) {
      const key = toHex(delta.record);
      const weight = (accumulated.get(key)?.weight ?? 0) + delta.weight;
      if (weight === 0) accumulated.delete(key);
      else accumulated.set(key, { record: delta.record, weight });
      tick.push(delta);
    }
    this.stepWitnessAccumulated = accumulated;
    this.stepWitnessTickDeltas = consolidateDeltas(tick);
  }
  replaceStepWitnessWith(next: Multiset): void {
    this.stepWitnessTickDeltas = consolidateDeltas(
      diffMultisets(this.stepWitnessAccumulated, next),
    );
    this.stepWitnessAccumulated = next;
  }
  beginStepWitnessTick(): void {
    this.stepWitnessTickDeltas = [];
  }
  acceptPositiveStaged(deltas: RecordDelta[]): RecordDelta[] {
    rejectNonPositiveFrontierDeltas(deltas);
    const accepted: RecordDelta[] = [];
    const staged = new Map(this.stagedPositive);
    for (const delta of consolidateDeltas(deltas)) {
      if (delta.weight <= 0) throw new IvmRuntimeError("UnsupportedNonMonotoneRecursion");
      const key = toHex(delta.record);
      if (this.accumulated.has(key) || staged.has(key)) continue;
      staged.set(key, { record: delta.record, weight: 1 });
      accepted.push({ record: delta.record, weight: 1 });
    }
    this.stagedPositive = staged;
    return consolidateDeltas(accepted);
  }
  commitStagedPositive(): void {
    if (this.stagedPositive.size === 0) return;
    this.accumulated = new Map([...this.accumulated, ...this.stagedPositive]);
    this.stagedPositive = new Map();
  }
  replaceWith(next: Multiset): RecordDelta[] {
    const deltas = diffMultisets(this.accumulated, next);
    this.accumulated = next;
    this.stepArrangementsHydratedFlag = false;
    this.pendingHydration = undefined;
    this.stagedPositive = new Map();
    return consolidateDeltas(deltas);
  }
}

function diffMultisets(old: Multiset, next: Multiset): RecordDelta[] {
  const deltas: RecordDelta[] = [];
  for (const [key, { record, weight }] of old) {
    const delta = (next.get(key)?.weight ?? 0) - weight;
    if (delta !== 0) deltas.push({ record, weight: delta });
  }
  for (const [key, { record, weight }] of next) {
    if (!old.has(key) && weight !== 0) deltas.push({ record, weight });
  }
  return deltas;
}

type TraversalFrame = { visit: NodeId } | { evaluate: NodeId };
type TraversalProgress = "Yield" | { ready: RecordDeltas };

/** A bounded postorder walk over one immutable graph/context snapshot. */
class HydrationTraversal {
  private discovery: TraversalFrame[];
  private discovered = new Set<NodeId>();
  private visiting = new Set<NodeId>();
  private order: NodeId[] = [];
  private nextEvaluation = 0;
  private memo = new Map<NodeId, RecordDeltas>();

  constructor(
    private readonly root: NodeId,
    private readonly context: EvalContext,
  ) {
    this.discovery = [{ visit: root }];
  }

  async poll(context: HydrationRecomputeContext): Promise<TraversalProgress> {
    let remaining = MAX_HYDRATION_TRAVERSAL_NODES_PER_POLL;
    while (remaining > 0) {
      const frame = this.discovery.pop();
      if (frame) {
        remaining--;
        if ("visit" in frame) {
          const node = frame.visit;
          if (this.discovered.has(node)) {
            if (this.visiting.has(node)) throw new IvmRuntimeError("GraphCycle", node);
            continue;
          }
          this.discovered.add(node);
          this.visiting.add(node);
          const graphNode = context.graph.node(node);
          if (!graphNode) throw new IvmRuntimeError("GraphNodeNotFound", node);
          this.discovery.push({ evaluate: node });
          for (const input of [...graphNode.descriptor.inputs].reverse())
            this.discovery.push({ visit: input });
        } else {
          // Unmark outside any assertion: a node left visiting turns shared inputs into false cycles.
          this.visiting.delete(frame.evaluate);
          this.order.push(frame.evaluate);
        }
        continue;
      }
      const node = this.order[this.nextEvaluation];
      if (node === undefined) {
        const ready = this.memo.get(this.root);
        if (!ready) throw new IvmRuntimeError("GraphNodeNotFound", this.root);
        return { ready };
      }
      remaining--;
      const evaluator = new HydrationEvaluator(context, this.context, this.memo);
      this.memo.set(node, await evaluator.evalNode(node));
      this.nextEvaluation++;
    }
    return "Yield";
  }
}

/** Advance one bounded phase of an inputs-backed recursive snapshot. */
export async function resumeInputsHydrationRecompute(
  state: RecursiveState,
  context: HydrationRecomputeContext,
  node: NodeId,
  recursive: any,
  outputDesc: RecordDescriptor,
  nodes: RecursiveNodes,
): Promise<HydrationRecomputeProgress> {
  if (state.pendingHydration?.inputGeneration !== context.inputGeneration) {
    state.beginHydrationRecompute(context.inputGeneration);
  }
  const pending = state.pendingHydrationMut();
  const phase = pending.phase;
  if (phase.kind === "ReadyForArrangementHydration") return "ReadyForArrangementHydration";

  let traversalRoot: NodeId;
  let evalContext: EvalContext;
  if (phase.kind === "Seed") {
    traversalRoot = nodes.seed;
    evalContext = EvalContext.root();
  } else {
    if (phase.kind === "Step" && phase.subTick > recursive.maxIters) {
      if (recursive.truncateAtMaxIters) {
        pending.phase = { kind: "ReadyForArrangementHydration" };
        return "ReadyForArrangementHydration";
      }
      throw new IvmRuntimeError("RecursiveIterationLimit", { node, maxIters: recursive.maxIters });
    }
    if (phase.kind === "StepWitness" && nodes.stepWitness === undefined) {
      throw new Error("witness phase requires a witness graph");
    }
    traversalRoot = phase.kind === "Step" ? nodes.step : nodes.stepWitness!;
    evalContext = EvalContext.withBinding(
      context.scope,
      phase.subTick,
      recursive.frontier,
      structuredClone(phase.frontier),
    );
  }
  const traversal = pending.traversal ?? new HydrationTraversal(traversalRoot, evalContext);
  pending.traversal = undefined;
  let progress: TraversalProgress;
  try {
    progress = await traversal.poll(context);
  } catch (error) {
    // Keep the exact postorder stack/memo across a cold source.
    if ((error as any).kind === "EvaluationBlocked") pending.traversal = traversal;
    throw error;
  }
  if (progress === "Yield") {
    pending.traversal = traversal;
    return "Yield";
  }
  const result = progress.ready;
  switch (phase.kind) {
    case "Seed": {
      if (!result.descriptor.equals(outputDesc)) throw new IvmRuntimeError("GraphOutputMismatch");
      const accepted = acceptPositiveIntoSet(pending.accumulated, result.deltas);
      pending.phase =
        accepted.length === 0
          ? { kind: "ReadyForArrangementHydration" }
          : { kind: "Step", frontier: { descriptor: outputDesc, deltas: accepted }, subTick: 1 };
      return "Yield";
    }
    case "Step": {
      if (!result.descriptor.equals(outputDesc)) throw new IvmRuntimeError("GraphOutputMismatch");
      const accepted = acceptPositiveIntoSet(pending.accumulated, result.deltas);
      const stepDelta = { descriptor: outputDesc, deltas: accepted };
      pending.phase =
        nodes.stepWitness !== undefined
          ? { kind: "StepWitness", frontier: phase.frontier, stepDelta, subTick: phase.subTick }
          : accepted.length === 0
            ? { kind: "ReadyForArrangementHydration" }
            : { kind: "Step", frontier: stepDelta, subTick: phase.subTick + 1 };
      return "Yield";
    }
    case "StepWitness": {
      const witness = pending.stepWitnessAccumulated;
      for (const delta of consolidateDeltas(result.deltas)) {
        const key = toHex(delta.record);
        witness.set(key, {
          record: delta.record,
          weight: (witness.get(key)?.weight ?? 0) + delta.weight,
        });
      }
      for (const [key, d] of [...witness]) if (d.weight === 0) witness.delete(key);
      pending.phase =
        phase.stepDelta.deltas.length === 0
          ? { kind: "ReadyForArrangementHydration" }
          : { kind: "Step", frontier: phase.stepDelta, subTick: phase.subTick + 1 };
      return "Yield";
    }
  }
}

export async function recursiveDelta(
  state: RecursiveState,
  runtime: GraphRuntimeView,
  node: NodeId,
  recursive: any,
  outputDesc: RecordDescriptor,
  nodes: RecursiveNodes,
): Promise<RecursiveDeltaProgress> {
  const env = runtime.env;
  state.beginStepWitnessTick();
  const recomputeTable = hasRecomputeTableDelta(env.graph, env.tableDeltas, nodes);
  const hasTableDelta = env.tableDeltas.some((d: TableDelta) =>
    recursive.readTables.includes(d.table),
  );
  const recomputeBinding = hasRecomputeBindingDelta(env.graph, env.bindingDeltas, nodes);
  const hasBindingDeltas = env.bindingDeltas.length > 0;
  if (
    recomputeTable ||
    recomputeBinding ||
    (!hasBindingDeltas && state.isEmpty()) ||
    !state.stepArrangementsHydrated() ||
    (recursive.truncateAtMaxIters && (hasTableDelta || hasBindingDeltas))
  ) {
    // Retractions are handled by full recompute + diff until DRed/DBSP nested negatives exist.
    if (!state.hasPendingHydrationFor(env.currentTick)) env.metrics.recursiveRecomputes++;
    if (closureTrace()) {
      console.error(
        `CLOSURE_TRACE event=recursive_recompute node=${node} scope=${String(runtime.scope)} has_recompute_table_delta=${recomputeTable} has_table_delta=${hasTableDelta} has_recompute_binding_delta=${recomputeBinding} has_binding_deltas=${hasBindingDeltas} state_empty=${state.isEmpty()} step_hydrated=${state.stepArrangementsHydrated()} total_recomputes=${env.metrics.recursiveRecomputes}`,
      );
    }
    const progress = await resumeInputsHydrationRecompute(
      state,
      {
        schema: env.schema,
        graph: env.graph,
        variantProjections: env.variantProjections,
        inputs: env.evaluationInputs,
        tableDeltas: undefined,
        storage: env.storage,
        bindingSnapshots: env.bindingSnapshots,
        scope: runtime.scope,
        // A live tick's inputs are immutable while driven; a later tick restarts.
        inputGeneration: env.currentTick,
      },
      node,
      recursive,
      outputDesc,
      nodes,
    );
    if (progress === "Yield") return "Yield";
    const [next, nextStepWitness] = state.finishHydrationRecompute();
    const accumulated = { descriptor: outputDesc, deltas: positive(next) };
    const emitted = state.replaceWith(next);
    state.replaceStepWitnessWith(nextStepWitness);
    await hydrateRecursiveArrangements(runtime, recursive, nodes.step, accumulated);
    if (nodes.stepWitness !== undefined) {
      await hydrateRecursiveArrangements(runtime, recursive, nodes.stepWitness, {
        descriptor: outputDesc,
        deltas: state.accumulatedDeltas(),
      });
    }
    state.markStepArrangementsHydrated();
    return { ready: emitted };
  }

  const emitted: RecordDelta[] = [];
  let seedDelta: RecordDeltas;
  if (hasBindingDeltas) {
    // A new binding can expose existing seed rows: evaluate the seed over a current snapshot.
    const fullTableDeltas = env.evaluationInputs
      ? snapshotTableDeltasFromInputs(env.schema, env.graph, env.evaluationInputs, nodes.seed)
      : await snapshotTableDeltas(env.schema, env.graph, env.storage, nodes.seed);
    seedDelta = await runtime.evalWithBindingAndTableDeltas(
      fullTableDeltas,
      0,
      recursive.frontier,
      emptyDeltas(outputDesc),
      nodes.seed,
    );
  } else {
    seedDelta = await runtime.evalRoot(nodes.seed);
  }
  if (!seedDelta.descriptor.equals(outputDesc)) throw new IvmRuntimeError("GraphOutputMismatch");
  const seedDeltaCount = seedDelta.deltas.length;
  const seedFrontier = state.acceptPositiveStaged(seedDelta.deltas);
  if (closureTrace()) {
    console.error(
      `CLOSURE_TRACE event=recursive_positive node=${node} scope=${String(runtime.scope)} seed_delta=${seedDeltaCount} seed_frontier=${seedFrontier.length} has_table_delta=${hasTableDelta} has_binding_deltas=${hasBindingDeltas}`,
    );
  }
  emitted.push(...seedFrontier);

  // A changed table can extend only paths ending at the changed row's join key.
  let frontier: RecordDeltas = {
    descriptor: outputDesc,
    deltas: hasTableDelta
      ? consolidateDeltas([
          ...seedFrontier,
          ...affectedRecursiveFrontier(runtime, recursive, state.accumulatedDeltas(), nodes.step),
        ])
      : seedFrontier,
  };
  let subTick = 1;
  let mustRunStep = true;
  for (;;) {
    if (subTick > recursive.maxIters) {
      if (recursive.truncateAtMaxIters) break;
      throw new IvmRuntimeError("RecursiveIterationLimit", { node, maxIters: recursive.maxIters });
    }
    frontier.deltas = consolidateDeltas(frontier.deltas);
    if (frontier.deltas.length === 0 && !mustRunStep) break;
    mustRunStep = false;
    const stepDelta = await runtime.evalWithBinding(
      subTick,
      recursive.frontier,
      structuredClone(frontier),
      nodes.step,
    );
    if (!stepDelta.descriptor.equals(outputDesc)) throw new IvmRuntimeError("GraphOutputMismatch");
    if (nodes.stepWitness !== undefined) {
      const witnessDelta = await runtime.evalWithBinding(
        subTick,
        recursive.frontier,
        frontier,
        nodes.stepWitness,
      );
      const expected = recursive.stepWitnessOutput;
      if (expected === undefined || !witnessDelta.descriptor.equals(expected)) {
        throw new IvmRuntimeError("GraphOutputMismatch");
      }
      state.appendStepWitness(witnessDelta.deltas);
    }
    const accepted = state.acceptPositiveStaged(stepDelta.deltas);
    if (accepted.length === 0) break;
    emitted.push(...accepted);
    frontier = { descriptor: outputDesc, deltas: accepted };
    subTick++;
  }
  return { ready: consolidateDeltas(emitted) };
}

function affectedRecursiveFrontier(
  runtime: GraphRuntimeView,
  recursive: any,
  accumulated: RecordDelta[],
  step: NodeId,
): RecordDelta[] {
  const { graph, tableDeltas } = runtime.env;
  const selected = new Map<string, Bytes>();
  const pending = [step];
  const seen = new Set<NodeId>();
  while (pending.length) {
    const node = pending.pop()!;
    if (seen.has(node)) continue;
    seen.add(node);
    const graphNode = requireNode(graph, node);
    const operator = graphNode.descriptor.operator;
    const inputs: NodeId[] = graphNode.descriptor.inputs;
    if (operator.kind !== "Join") {
      pending.push(...inputs);
      continue;
    }
    const join = operator.value;
    if (inputs.length !== 2) throw new IvmRuntimeError("GraphInputArityMismatch", node);
    const [left, right] = inputs;
    const leftFrontier = containsFrontier(graph, left, recursive.frontier);
    const rightFrontier = containsFrontier(graph, right, recursive.frontier);
    pending.push(left, right);
    if (leftFrontier === rightFrontier) continue;
    let changedInput: NodeId,
      changedFields: string[],
      frontierFields: string[],
      frontierDesc: RecordDescriptor;
    if (leftFrontier && containsChangedTable(graph, right, tableDeltas)) {
      [changedInput, changedFields, frontierFields, frontierDesc] = [
        right,
        planExprNames(join.rightKey),
        planExprNames(join.leftKey),
        join.leftDescriptor,
      ];
    } else if (rightFrontier && containsChangedTable(graph, left, tableDeltas)) {
      [changedInput, changedFields, frontierFields, frontierDesc] = [
        left,
        planExprNames(join.leftKey),
        planExprNames(join.rightKey),
        join.rightDescriptor,
      ];
    } else continue;
    // Raw table keys are a selective frontier only while they keep the join descriptor's fields.
    let touched: Set<string>;
    try {
      touched = collectChangedJoinKeys(
        graph,
        changedInput,
        tableDeltas,
        changedFields,
        join.comparison,
      );
    } catch (error) {
      if ((error as any).kind === "GraphFieldNotFound") continue;
      throw error;
    }
    for (const delta of accumulated) {
      if (
        joinKeys(frontierDesc, delta.record, frontierFields, join.comparison).some((k) =>
          touched.has(toHex(k)),
        )
      ) {
        selected.set(toHex(delta.record), delta.record);
      }
    }
  }
  return [...selected.values()].map((record) => ({ record, weight: 1 }));
}

function requireNode(graph: IvmGraph, node: NodeId): any {
  const graphNode = graph.node(node);
  if (!graphNode) throw new IvmRuntimeError("GraphNodeNotFound", node);
  return graphNode;
}

/** Walk an IVM graph's inputs iteratively, visiting each node at most once. */
function walkInputGraph(
  graph: IvmGraph,
  root: NodeId,
  visit: (node: NodeId, graphNode: any) => boolean | void,
): boolean {
  const pending = [root];
  const seen = new Set<NodeId>();
  while (pending.length) {
    const node = pending.pop()!;
    if (seen.has(node)) continue;
    seen.add(node);
    const graphNode = requireNode(graph, node);
    if (visit(node, graphNode) === true) return true;
    // Reverse push retains the recursive walk's left-to-right input order.
    pending.push(...[...graphNode.descriptor.inputs].reverse());
  }
  return false;
}

function containsFrontier(graph: IvmGraph, root: NodeId, binding: any): boolean {
  return walkInputGraph(
    graph,
    root,
    (_, n) =>
      n.descriptor.operator.kind === "FrontierSource" &&
      n.descriptor.operator.value.binding === binding,
  );
}

function containsChangedTable(graph: IvmGraph, root: NodeId, tableDeltas: TableDelta[]): boolean {
  const changed = new Set(tableDeltas.filter((d) => d.deltas.length > 0).map((d) => d.table));
  return walkInputGraph(
    graph,
    root,
    (_, n) =>
      n.descriptor.operator.kind === "TableSource" &&
      changed.has(n.descriptor.operator.value.table),
  );
}

function collectChangedJoinKeys(
  graph: IvmGraph,
  root: NodeId,
  tableDeltas: TableDelta[],
  fields: string[],
  comparison: any,
): Set<string> {
  const keys = new Set<string>();
  walkInputGraph(graph, root, (_, n) => {
    if (n.descriptor.operator.kind !== "TableSource") return;
    const table = n.descriptor.operator.value.table;
    for (const delta of tableDeltas) {
      if (delta.table !== table || delta.deltas.length === 0) continue;
      for (const key of touchedJoinKeys(delta.descriptor, fields, delta.deltas, comparison))
        keys.add(toHex(key));
    }
  });
  return keys;
}

function sourceGraphs(nodes: RecursiveNodes): NodeId[] {
  return nodes.stepWitness === undefined
    ? [nodes.seed, nodes.step]
    : [nodes.seed, nodes.step, nodes.stepWitness];
}

function hasRecomputeTableDelta(
  graph: IvmGraph,
  tableDeltas: TableDelta[],
  nodes: RecursiveNodes,
): boolean {
  const tables = new Set<string>();
  const antiJoinRight = new Set<string>();
  for (const root of sourceGraphs(nodes)) collectTableSourceNames(graph, root, tables);
  for (const root of sourceGraphs(nodes))
    collectAntiJoinRightTableSources(graph, root, antiJoinRight);
  return tableDeltas
    .filter((d) => tables.has(d.table))
    .some(
      (d) => antiJoinRight.has(d.table) || d.deltas.some((delta: RecordDelta) => delta.weight <= 0),
    );
}

function hasRecomputeBindingDelta(
  graph: IvmGraph,
  bindingDeltas: BindingDelta[],
  nodes: RecursiveNodes,
): boolean {
  const shapes = new KeyedMap<any, true>(stableKey);
  for (const root of sourceGraphs(nodes)) {
    walkInputGraph(graph, root, (_, n) => {
      if (n.descriptor.operator.kind === "BindingSource")
        shapes.set(n.descriptor.operator.value.key, true);
    });
  }
  return bindingDeltas
    .filter((b) => shapes.has(b.key))
    .some((b) => b.deltas.some((delta: RecordDelta) => delta.weight <= 0));
}

export function snapshotRequirement(
  graph: IvmGraph,
  node: NodeId,
  nodes: RecursiveNodes,
  tableDeltas: TableDelta[],
  bindingDeltas: BindingDelta[],
  state: RecursiveState | undefined,
): NodeId | undefined {
  const hasBindings = bindingDeltas.length > 0;
  const requiresRecompute =
    hasRecomputeTableDelta(graph, tableDeltas, nodes) ||
    hasRecomputeBindingDelta(graph, bindingDeltas, nodes) ||
    (!hasBindings && (state === undefined || state.isEmpty())) ||
    state === undefined ||
    !state.stepArrangementsHydrated();
  // The owner's complete input closure includes the optional side graph.
  if (requiresRecompute) return node;
  return hasBindings ? nodes.seed : undefined;
}

export function requireSnapshotInputs(
  graph: IvmGraph,
  inputs: EvaluationInputs,
  root: NodeId,
): void {
  let blocked = false;
  for (const source of collectTableSources(graph, root)) {
    const request = NodeState.tableSourceRequest({
      table: source.table,
      scan: source.scan,
      variantProjection: undefined,
    });
    if (!request) continue;
    try {
      inputs.rows(request);
    } catch (error) {
      if ((error as any).kind !== "EvaluationBlocked") throw error;
      blocked = true;
    }
  }
  if (blocked) throw new IvmRuntimeError("EvaluationBlocked");
}

/** Evaluate the step once over a full snapshot to prepare shared arrangements; discard the result. */
export async function hydrateRecursiveArrangements(
  runtime: GraphRuntimeView,
  recursive: any,
  step: NodeId,
  accumulated: RecordDeltas,
): Promise<void> {
  const env = runtime.env;
  const fullTableDeltas = env.evaluationInputs
    ? []
    : await snapshotTableDeltas(env.schema, env.graph, env.storage, step);
  if (closureTrace()) {
    const records = fullTableDeltas.reduce((n, d) => n + d.deltas.length, 0);
    const tables = fullTableDeltas.map((d) => `${d.table}:${d.deltas.length}`).join(",");
    console.error(
      `CLOSURE_TRACE event=hydrate_recursive_arrangements step=${step} tables=${tables} records=${records} accumulated=${accumulated.deltas.length}`,
    );
  }
  await runtime.evalWithBindingAndTableDeltas(
    fullTableDeltas,
    0,
    recursive.frontier,
    accumulated,
    step,
  );
  runtime.clearOperatorStateForScope();
}

function tableSchema(schema: DatabaseSchema, table: string): any {
  const found = schema.table(table);
  if (!found) throw new IvmRuntimeError("TableNotFound", table);
  return found;
}

async function scanAllStored(store: any, bounds: any): Promise<Bytes[]> {
  const cursor = await store.scan(bounds);
  const out: Bytes[] = [];
  for (let batch = await cursor.nextBatch(); batch; batch = await cursor.nextBatch()) {
    for (const [, record] of batch) out.push(record);
  }
  return out;
}

export async function snapshotTableDeltas(
  schema: DatabaseSchema,
  graph: IvmGraph,
  storage: OrderedKvStorage,
  root: NodeId,
): Promise<TableDelta[]> {
  const output: TableDelta[] = [];
  for (const source of collectTableSources(graph, root)) {
    const table = tableSchema(schema, source.table);
    const store = recordStoreForTable(storage, table, table.recordSchema());
    let stored: Bytes[] = [];
    if (!source.scan)
      stored = await scanAllStored(store, ScanRequest.prefixBounds(new Uint8Array()));
    else {
      const bounds = scanBounds(source.scan);
      if (bounds.kind === "Prefix")
        stored = await scanAllStored(store, ScanRequest.prefixBounds(bounds.prefix));
      else if (compareBytes(bounds.start, bounds.end) < 0) {
        stored = await scanAllStored(store, ScanRequest.rangeBounds(bounds.start, bounds.end));
      }
    }
    output.push(...NodeState.groupSourceRows(table, stored));
  }
  return output;
}

function snapshotTableDeltasFromInputs(
  schema: DatabaseSchema,
  graph: IvmGraph,
  inputs: EvaluationInputs,
  root: NodeId,
): TableDelta[] {
  const tables = collectTableSources(graph, root);
  requireSnapshotInputs(graph, inputs, root);
  const output: TableDelta[] = [];
  for (const source of tables) {
    const request = NodeState.tableSourceRequest({
      table: source.table,
      scan: source.scan,
      variantProjection: undefined,
    });
    const stored = request ? inputs.rows(request).map(([, record]) => record) : [];
    output.push(...NodeState.groupSourceRows(tableSchema(schema, source.table), stored));
  }
  return output;
}

interface TableSnapshotSource {
  table: string;
  scan: any;
}

/** Hashable identity for plan structs (bytes rendered as hex). */
function stableKey(value: unknown): string {
  return JSON.stringify(value, (_, v) =>
    v instanceof Uint8Array ? toHex(v) : typeof v === "bigint" ? v.toString() : v,
  );
}

function collectTableSources(graph: IvmGraph, node: NodeId): TableSnapshotSource[] {
  const tables = new KeyedMap<TableSnapshotSource, TableSnapshotSource>(stableKey);
  walkInputGraph(graph, node, (_, n) => {
    if (n.descriptor.operator.kind !== "TableSource") return;
    const { table, scan } = n.descriptor.operator.value;
    const source = { table, scan };
    tables.set(source, source);
  });
  return [...tables].map(([source]) => source);
}

export function recursiveReadTables(
  graph: IvmGraph,
  seed: NodeId,
  step: NodeId,
  stepWitness: NodeId | undefined,
): string[] {
  const tables = new Set<string>();
  for (const root of sourceGraphs({ seed, step, stepWitness }))
    collectTableSourceNames(graph, root, tables);
  return [...tables].sort();
}

function collectTableSourceNames(graph: IvmGraph, node: NodeId, tables: Set<string>): void {
  walkInputGraph(graph, node, (_, n) => {
    const op = n.descriptor.operator;
    if (op.kind === "TableSource" || op.kind === "IndexSource") tables.add(op.value.table);
  });
}

function collectAntiJoinRightTableSources(
  graph: IvmGraph,
  node: NodeId,
  tables: Set<string>,
): void {
  walkInputGraph(graph, node, (id, n) => {
    if (n.descriptor.operator.kind !== "AntiJoin") return;
    const right = n.descriptor.inputs[1];
    if (right === undefined) throw new IvmRuntimeError("GraphInputMissing", id);
    collectTableSourceNames(graph, right, tables);
  });
}

/** Recompute matches the incremental regime: monotone set fixed points, not bag. */
function acceptPositiveIntoSet(multiset: Multiset, deltas: RecordDelta[]): RecordDelta[] {
  rejectNonPositiveFrontierDeltas(deltas);
  const accepted: RecordDelta[] = [];
  for (const delta of consolidateDeltas(deltas)) {
    if (delta.weight <= 0) throw new IvmRuntimeError("UnsupportedNonMonotoneRecursion");
    const key = toHex(delta.record);
    if (multiset.has(key)) continue;
    multiset.set(key, { record: delta.record, weight: 1 });
    accepted.push({ record: delta.record, weight: 1 });
  }
  return consolidateDeltas(accepted);
}

function rejectNonPositiveFrontierDeltas(deltas: RecordDelta[]): void {
  if (deltas.some((d) => d.weight <= 0))
    throw new IvmRuntimeError("UnsupportedNonMonotoneRecursion");
}

/** Snapshot ArgBy winners, sharing the maintained operator's comparator. */
export function hydratedArgByWinners(
  input: RecordDeltas,
  outputDesc: RecordDescriptor,
  groupFieldIndices: number[],
  comparisonFieldIndices: number[],
  direction: ArgByDirection,
): RecordDeltas {
  const winners = new SortedMap<Bytes, [Bytes, Bytes]>(compareBytes);
  for (const delta of input.deltas) {
    if (delta.weight <= 0) continue;
    const groupKey = encodedRecordKeyPart(outputDesc, delta.record, groupFieldIndices);
    const comparisonKey = encodedRecordKeyPart(outputDesc, delta.record, comparisonFieldIndices);
    const winner = winners.get(groupKey);
    if (
      !winner ||
      argByCandidateReplaces(comparisonKey, delta.record, winner[0], winner[1], direction)
    ) {
      winners.set(groupKey, [comparisonKey, delta.record]);
    }
  }
  return {
    descriptor: outputDesc,
    deltas: winners.valuesArray().map(([, record]) => ({ record, weight: 1 })),
  };
}

/** Full-snapshot evaluator used by recursive recompute fallback. */
class HydrationEvaluator {
  constructor(
    private readonly ctx: HydrationRecomputeContext,
    private readonly context: EvalContext,
    private readonly memo: Map<NodeId, RecordDeltas>,
  ) {}

  async evalNode(node: NodeId): Promise<RecordDeltas> {
    const memoized = this.memo.get(node);
    if (memoized) return structuredClone(memoized);
    const { schema, variantProjections, graph } = this.ctx;
    const graphNode = requireNode(graph, node);
    const outputDesc = graphNode.descriptor.output.records();
    const operator = graphNode.descriptor.operator;
    const op = operator.value;
    const unary = () => this.evalUnaryInput(graphNode, node);
    const binary = async (): Promise<[RecordDeltas, RecordDeltas]> => {
      const inputs: NodeId[] = graphNode.descriptor.inputs;
      if (inputs.length !== 2) throw new IvmRuntimeError("GraphInputArityMismatch", node);
      return [await this.evalNode(inputs[0]), await this.evalNode(inputs[1])];
    };
    switch (operator.kind) {
      case "TableSource":
        if (this.ctx.inputs) {
          return NodeState.updateTableSourceFromInputs(
            op,
            schema,
            variantProjections,
            outputDesc,
            this.ctx.inputs,
          );
        }
        if (this.ctx.tableDeltas) {
          return NodeState.updateTableSource(
            op,
            schema,
            variantProjections,
            outputDesc,
            this.ctx.tableDeltas,
          );
        }
        return this.evalTableSource(op, outputDesc);
      case "IndexSource":
        if (this.ctx.inputs) {
          return NodeState.updateIndexSourceFromInputs(
            op,
            schema,
            variantProjections,
            outputDesc,
            this.ctx.inputs,
          );
        }
        return NodeState.updateIndexSource(
          op,
          schema,
          variantProjections,
          outputDesc,
          [],
          this.ctx.storage,
          "Hydrate",
        );
      case "InlineRecords":
        return {
          descriptor: outputDesc,
          deltas: op.records.map((record: Bytes) => ({ record, weight: 1 })),
        };
      case "FrontierSource": {
        const deltas =
          structuredClone(this.context.bindings.get(op.binding)) ?? emptyDeltas(outputDesc);
        if (!deltas.descriptor.equals(outputDesc)) throw new IvmRuntimeError("GraphOutputMismatch");
        return deltas;
      }
      case "BindingSource":
        return projectBindingSourceDeltas(
          this.ctx.bindingSnapshots.get(op.key) ?? emptyDeltas(outputDesc),
          outputDesc,
        );
      case "Arrange":
        return unary();
      // A lookup's storage probes are driven by evaluation sessions, which this evaluator is not.
      case "Lookup":
        throw new IvmRuntimeError("UnsupportedLookup", "a lookup cannot run in a recursive step");
      case "Filter":
        return NodeState.updateFilter(op, outputDesc, await unary());
      case "MapProject": {
        const input = await unary();
        const fields = rawProjectionFields(op, input.descriptor, outputDesc);
        return NodeState.updateMapProject(op, outputDesc, input, fields, false);
      }
      case "UnwrapNullable":
        return NodeState.updateUnwrapNullable(op, outputDesc, await unary());
      case "Unnest":
        return NodeState.updateUnnest(op, outputDesc, await unary());
      case "VariantProject":
        return NodeState.updateVariantProject(op, outputDesc, await unary());
      case "ArgMaxBy":
      case "ArgMinBy":
        return hydratedArgByWinners(
          await unary(),
          outputDesc,
          op.groupFieldIndices,
          op.comparisonFieldIndices,
          operator.kind === "ArgMaxBy" ? "Max" : "Min",
        );
      case "Union": {
        const inputs: RecordDeltas[] = [];
        for (const input of graphNode.descriptor.inputs) inputs.push(await this.evalNode(input));
        return NodeState.updateUnion(outputDesc, inputs);
      }
      case "IndexBy":
        return NodeState.updateIndexBy(op, outputDesc, await unary());
      case "Join": {
        const [left, right] = await binary();
        const leftOn = planExprNames(op.leftKey);
        const rightOn = planExprNames(op.rightKey);
        const rightByKey = new Map<string, RecordDelta[]>();
        for (const delta of right.deltas) {
          for (const key of joinKeys(op.rightDescriptor, delta.record, rightOn, op.comparison)) {
            const k = toHex(key);
            const bucket = rightByKey.get(k);
            if (bucket) bucket.push(delta);
            else rightByKey.set(k, [delta]);
          }
        }
        const deltas: RecordDelta[] = [];
        for (const leftDelta of left.deltas) {
          for (const key of joinKeys(op.leftDescriptor, leftDelta.record, leftOn, op.comparison)) {
            for (const rightDelta of rightByKey.get(toHex(key)) ?? []) {
              deltas.push({
                record: createJoinRecord(
                  op.leftDescriptor,
                  leftDelta.record,
                  op.rightDescriptor,
                  rightDelta.record,
                  outputDesc,
                ),
                weight: leftDelta.weight * rightDelta.weight,
              });
            }
          }
        }
        return { descriptor: outputDesc, deltas };
      }
      case "SemiJoin":
      case "AntiJoin": {
        // A complete immutable snapshot: both threshold joins are safe with fresh replace-mode arrangements.
        const [left, right] = await binary();
        const deltas = thresholdSnapshot(
          left,
          right,
          planExprNames(op.leftKey),
          planExprNames(op.rightKey),
          op.comparison,
          operator.kind === "SemiJoin",
        );
        return { descriptor: outputDesc, deltas };
      }
      // A nested recursive graph needs its own persisted frontier state; v0 rejects it.
      case "Recursive":
        throw new IvmRuntimeError("UnsupportedNestedRecursion");
      default:
        // TopBy, CollectBy, Aggregate, RecursiveStepWitness, Persist, StreamingChecksum, Distinct, Negate.
        throw new IvmRuntimeError("UnsupportedOperator");
    }
  }

  private async evalTableSource(table: any, outputDesc: RecordDescriptor): Promise<RecordDeltas> {
    const { schema, storage, variantProjections } = this.ctx;
    const tableSchemaValue = tableSchema(schema, table.table);
    const store = recordStoreForTable(storage, tableSchemaValue, tableSchemaValue.recordSchema());
    const stored = await scanAllStored(store, ScanRequest.prefixBounds(new Uint8Array()));
    const tableDeltas = NodeState.groupSourceRows(tableSchemaValue, stored);
    return NodeState.updateTableSource(table, schema, variantProjections, outputDesc, tableDeltas);
  }

  private evalUnaryInput(graphNode: any, node: NodeId): Promise<RecordDeltas> {
    const input = graphNode.descriptor.inputs[0];
    if (input === undefined) throw new IvmRuntimeError("GraphInputMissing", node);
    return this.evalNode(input);
  }
}
