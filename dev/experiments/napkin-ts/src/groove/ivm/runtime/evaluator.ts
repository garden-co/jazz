// Per-tick evaluator state, memoization, arrangements, and recursive execution.
import { type Bytes, compareBytes, equalBytes, toHex } from "../../../support/bytes.js";
import { KeyedMap, SortedMap, SortedSet, cmpStr } from "../../../support/collections.js";
import { blake3 } from "../../../support/hash.js";
import { yieldNow } from "../../../support/runtime.js";
import {
  AsOf,
  EvalContext,
  EvalMemoEntry,
  IvmRuntimeError,
  NodeInputSignature,
  PreparedRecordCopy,
  ScopeId,
  applyChangedGroupOrdering,
  applyGroupWindowOrdering,
  consolidateDeltas,
  emptyDeltas,
  largeValues,
  outputIsStructuredCollectBy,
  projectWindowRecord,
  rawProjectionFields,
  recordDeltasEncodedBytes,
  recordsError,
  resolveFieldName,
  subTickCmp,
  topByIdentityFields,
  windowRecordReachesOutput,
  type ArrangementKey,
  type BindingDelta,
  type BindingSnapshots,
  type DatabaseSchema,
  type EvalMemoKey,
  type IvmGraph,
  type NodeId,
  type OperatorStateKey,
  type OrderedKvStorage,
  type PlanExpr,
  type RecordDelta,
  type RecordDeltas,
  type RecordDescriptor,
  type RootIdentity,
  type ScopeId as Scope,
  type SubTick,
  type TableDelta,
  type TableSchema,
  type TerminalDeltas,
  type TerminalOperation,
  type TickMetrics,
  type TopByOp,
} from "../../../_extern/groove-ivm-ops.js";
import { aggregateRowFromRecords, recordsBeforeFromDeltas } from "./aggregate.js";
import { CountedMap } from "./counted_map.js";
import { EvaluationMemo } from "./evaluation_memo.js";
import type { EvaluationInputs } from "./evaluation_session.js";
import { computeBatch } from "./evaluator/kernels.js";
import type { LookupState } from "./evaluator/lookup.js";
import {
  ArrangementState,
  ArrangementTransition,
  JoinState,
  ThresholdJoinState,
  joinOutputMapping,
} from "./join.js";
import { NodeState, type NodeRuntimeMeta } from "./operator_updates.js";
import { RankIndex } from "./rank_index.js";
import {
  RecursiveState,
  hydrateRecursiveArrangements,
  hydratedArgByWinners,
  recursiveDelta,
  requireSnapshotInputs,
  resumeInputsHydrationRecompute,
  snapshotRequirement,
  type RecursiveNodes,
} from "./recursion.js";
import {
  type ArgBySpec,
  type TopBySortPart,
  type WindowedRecord,
  argByOrderKey,
  argByOrderedWinner,
  cmpTopBySortParts,
  collectByExpandedWindow,
  collectByParentFromRecords,
  collectByRootFromRecords,
  collectByTreeParentFromRecords,
  diffRecordWindows,
  encodedArrangementKeyPart,
  encodedIdentityKeyPart,
  encodedRecordKeyPart,
  recordsBeforeDeltas,
  topBySortKey,
  topByWindowFromOrderedGroup,
  updateUnboundedCollectByTerminalState,
  updateUnboundedTopByGroupTouching,
} from "./windows.js";

export const PENDING: unique symbol = Symbol("pending");
export type Poll<T> = T | typeof PENDING;
/** Ready batches stay synchronous; only general evaluation allocates a promise. */
export type ReadyNodeEvaluation = RecordDeltas | Promise<RecordDeltas>;

export function planExprField(expr: PlanExpr): string | undefined {
  switch (expr.kind) {
    case "Field":
    case "Nullable":
    case "NullableFlat":
      return expr.value;
    case "EnumTagRemap":
    case "EnumRemap":
    case "RecursiveEnumRemap":
      return expr.field;
    default:
      return undefined;
  }
}

export function planExprFields(expressions: PlanExpr[]): SortedSet<string> {
  const out = new SortedSet<string>(cmpStr);
  for (const e of expressions) {
    const f = planExprField(e);
    if (f !== undefined) out.add(f);
  }
  return out;
}

export function planExprNames(expressions: PlanExpr[]): string[] {
  return expressions.map(planExprField).filter((f): f is string => f !== undefined);
}

export interface TerminalLineage {
  candidates: [NodeId, boolean][];
  hasPublicRoot: boolean;
}

export interface PendingUnaryBatch {
  lookup: NodeMemoLookup;
  input: RecordDeltas;
  next: number;
  output: RecordDeltas;
  projection: unknown;
}

const UNARY_ROWS_PER_POLL = 256;

export type OperatorState =
  | { kind: "Stateless" }
  | { kind: "Join"; state: JoinState }
  | { kind: "SemiJoin"; state: ThresholdJoinState }
  | { kind: "AntiJoin"; state: ThresholdJoinState }
  | { kind: "ArgBy"; state: any /* AsOf<SparseGroups, SubTick> */ }
  | { kind: "TopBy"; state: any /* AsOf<TopByIncrementalState, SubTick> */ }
  | { kind: "Recursive"; state: any /* AsOf<RecursiveState, Tick> */ }
  | { kind: "CollectBy"; state: CollectByIncrementalState }
  | { kind: "StreamingChecksum"; state: StreamingChecksumOperatorState }
  | { kind: "Lookup"; state: any /* AsOf<LookupState, SubTick> */ };

export interface StreamingChecksumOperatorState {
  pending?: PendingStreamingChecksum;
}

interface PendingStreamingChecksum {
  input: RecordDeltas;
  nextDelta: number;
  current?: any;
  output: RecordDelta[];
}

export type CollectByOrderKey = [TopBySortPart[], Bytes];
export const cmpOrderKey = (a: CollectByOrderKey, b: CollectByOrderKey): number =>
  cmpTopBySortParts(a[0], b[0]) || compareBytes(a[1], b[1]);

export class CollectByIncrementalState {
  groups = new SparseGroups();
  roots = new SortedMap<CollectByOrderKey, number>(cmpOrderKey);
  /** Ordered public root-collector occurrence index. */
  emittedRootOrder = new RankIndex<CollectByOrderKey, Bytes>(cmpOrderKey);
  /** Root terminal groups that have actually been emitted to a subscriber. */
  emittedRootKeys = new RankIndex<Bytes, null>(compareBytes);

  clone(): CollectByIncrementalState {
    const s = new CollectByIncrementalState();
    s.groups = this.groups.clone();
    s.roots = this.roots.clone();
    s.emittedRootOrder = this.emittedRootOrder.clone();
    s.emittedRootKeys = this.emittedRootKeys.clone();
    return s;
  }
}

/** Reuse a fresh root collector's Insert seeds only when every group has one representative. */
function singletonRootHydrationSnapshot(
  state: CollectByIncrementalState,
  input: RecordDelta[],
  operations: TerminalOperation[],
): RecordDelta[] | undefined {
  if (input.some((d) => d.weight <= 0)) return undefined;
  const seeds: [Bytes, Bytes][] = [];
  for (const operation of operations) {
    const edit = operation.edit;
    if (edit.kind !== "Insert") return undefined;
    if (operation.path.length !== 0 || !equalBytes(operation.rootKey, edit.key)) return undefined;
    const group = state.groups.get(edit.key);
    if (!group) return undefined;
    const it = group[Symbol.iterator]();
    const first = it.next();
    if (first.done || first.value[1] <= 0 || !it.next().done) return undefined;
    seeds.push([edit.key, edit.value]);
  }
  seeds.sort((a, b) => compareBytes(a[0], b[0]));
  return seeds.map(([, value]) => ({ record: value.slice(), weight: 1 }));
}

/** Copy-on-write ordered map for the outer window-group index. */
export class SparseGroups {
  constructor(
    private base = new SortedMap<Bytes, CollectByGroup>(compareBytes),
    private overlay = new SortedMap<Bytes, CollectByGroup | null>(compareBytes),
  ) {}

  clone(): SparseGroups {
    return new SparseGroups(this.base, this.overlay.clone());
  }

  get(key: Bytes): CollectByGroup | undefined {
    if (this.overlay.has(key)) return this.overlay.get(key) ?? undefined;
    return this.base.get(key);
  }

  getOrDefault(key: Bytes): CollectByGroup {
    const existing = this.overlay.get(key);
    if (existing) return existing;
    // A previous retraction can leave a tombstone; a later delta revives it.
    const group = this.base.get(key)?.clone() ?? new CollectByGroup();
    this.overlay.set(key, group);
    return group;
  }

  removeEmptyTouchedGroups(keys: Iterable<Bytes>): void {
    for (const key of keys) {
      if (this.get(key)?.isEmpty()) this.overlay.set(key, null);
    }
  }

  clear(): void {
    this.base = new SortedMap(compareBytes);
    this.overlay = new SortedMap(compareBytes);
  }

  /** Present group keys in ascending order, merging base with staged edits. */
  *presentKeys(): IterableIterator<Bytes> {
    const base = this.base.keysArray();
    const overlay = [...this.overlay];
    let i = 0;
    let j = 0;
    while (i < base.length || j < overlay.length) {
      if (j >= overlay.length) {
        yield base[i++];
        continue;
      }
      const [ok, group] = overlay[j];
      if (i < base.length) {
        const c = compareBytes(base[i], ok);
        if (c < 0) {
          yield base[i++];
          continue;
        }
        if (c === 0) i++;
      }
      j++;
      if (group) yield ok;
    }
  }

  /** Rank each of `keys` (ascending) among the present groups in one merged walk. */
  countBeforeEach(keys: Iterable<Bytes>): number[] {
    const present = this.presentKeys();
    let peek = present.next();
    let before = 0;
    const ranks: number[] = [];
    for (const key of keys) {
      while (!peek.done && compareBytes(peek.value, key) < 0) {
        before++;
        peek = present.next();
      }
      ranks.push(before);
    }
    return ranks;
  }

  /** Fold the staged overlay into the base after install. */
  commitOverlay(): void {
    if (this.overlay.size === 0) return;
    const base = this.base.clone();
    for (const [key, group] of this.overlay) {
      base.delete(key);
      if (group) {
        group.commitOverlay();
        base.set(key, group);
      }
    }
    this.base = base;
    this.overlay = new SortedMap(compareBytes);
  }

  *entries(): IterableIterator<[Bytes, CollectByGroup]> {
    for (const key of this.presentKeys()) yield [key, this.get(key)!];
  }
}

/** A collector group: shared counted base plus a sparse staged overlay. */
export class CollectByGroup {
  constructor(
    private base = new CountedMap<CollectByOrderKey>(cmpOrderKey),
    private overlay = new SortedMap<CollectByOrderKey, number | null>(cmpOrderKey),
  ) {}

  clone(): CollectByGroup {
    return new CollectByGroup(this.base, this.overlay.clone());
  }

  get(key: CollectByOrderKey): number | undefined {
    if (this.overlay.has(key)) return this.overlay.get(key) ?? undefined;
    return this.base.get(key);
  }

  set(key: CollectByOrderKey, weight: number): void {
    this.overlay.set(key, weight !== 0 ? weight : null);
  }

  countBefore(key: CollectByOrderKey): number {
    return this.countBeforeEach([key])[0];
  }

  /** Positive entries before each of `keys` (ascending), corrected by the overlay. */
  countBeforeEach(keys: Iterable<CollectByOrderKey>): number[] {
    const overlay = [...this.overlay];
    let j = 0;
    let adjustment = 0;
    const ranks: number[] = [];
    for (const key of keys) {
      while (j < overlay.length && cmpOrderKey(overlay[j][0], key) < 0) {
        const [candidate, weight] = overlay[j++];
        const staged = weight !== null && weight > 0;
        const base = (this.base.get(candidate) ?? 0) > 0;
        adjustment += Number(staged) - Number(base);
      }
      const rank = this.base.positiveBefore(key) + adjustment;
      if (rank < 0) throw new Error("rank counts present entries");
      ranks.push(rank);
    }
    return ranks;
  }

  isEmpty(): boolean {
    return this[Symbol.iterator]().next().done === true;
  }

  *[Symbol.iterator](): IterableIterator<[CollectByOrderKey, number]> {
    const base = this.base[Symbol.iterator]();
    const overlay = this.overlay[Symbol.iterator]();
    let b = base.next();
    let o = overlay.next();
    while (!b.done || !o.done) {
      if (!b.done && !o.done) {
        const c = cmpOrderKey(b.value[0], o.value[0]);
        if (c < 0) {
          yield b.value;
          b = base.next();
          continue;
        }
        if (c === 0) b = base.next();
        const [k, w] = o.value;
        o = overlay.next();
        if (w !== null) yield [k, w];
      } else if (!b.done) {
        yield b.value;
        b = base.next();
      } else {
        const [k, w] = o.value!;
        o = overlay.next();
        if (w !== null) yield [k, w];
      }
    }
  }

  commitOverlay(): void {
    if (this.overlay.size === 0) return;
    for (const [key, weight] of this.overlay) {
      if (weight !== null) this.base.insert(key, weight);
      else this.base.remove(key);
    }
    this.overlay = new SortedMap(cmpOrderKey);
  }
}

export class TopByIncrementalState {
  groups = new SparseGroups();

  clone(): TopByIncrementalState {
    const s = new TopByIncrementalState();
    s.groups = this.groups.clone();
    return s;
  }

  removeEmptyTouchedGroups(groups: Iterable<Bytes>): void {
    for (const group of groups) {
      if (this.groups.get(group)?.isEmpty()) this.groups.removeEmptyTouchedGroups([group]);
    }
  }

  commitOverlays(): void {
    this.groups.commitOverlay();
  }
}

export function operatorStateFor(operator: { kind: string }): OperatorState {
  switch (operator.kind) {
    case "Join":
      return { kind: "Join", state: new JoinState() };
    case "SemiJoin":
      return { kind: "SemiJoin", state: new ThresholdJoinState(true) };
    case "AntiJoin":
      return { kind: "AntiJoin", state: new ThresholdJoinState(false) };
    case "Recursive":
      return { kind: "Recursive", state: new AsOf(new RecursiveState()) };
    case "ArgMinBy":
    case "ArgMaxBy":
      return { kind: "ArgBy", state: new AsOf(new SparseGroups()) };
    case "TopBy":
      return { kind: "TopBy", state: new AsOf(new TopByIncrementalState()) };
    case "CollectBy":
      return { kind: "CollectBy", state: new CollectByIncrementalState() };
    case "StreamingChecksum":
      return { kind: "StreamingChecksum", state: {} };
    case "Lookup":
      return { kind: "Lookup", state: new AsOf(undefined) };
    default:
      return { kind: "Stateless" };
  }
}

/** 64-bit FNV-1a over the parts; the DefaultHasher stand-in. */
export function hashParts(parts: (string | number | Bytes)[]): bigint {
  let h = 0xcbf29ce484222325n;
  const mix = (b: number) => {
    h ^= BigInt(b & 0xff);
    h = (h * 0x100000001b3n) & 0xffffffffffffffffn;
  };
  for (const p of parts) {
    if (typeof p === "number") for (let i = 0; i < 8; i++) mix(Math.floor(p / 2 ** (8 * i)));
    else if (typeof p === "string") for (let i = 0; i < p.length; i++) mix(p.charCodeAt(i));
    else for (const b of p) mix(b);
  }
  return h;
}

export function recordDeltasDigest(deltas: RecordDeltas): bigint {
  const parts: (string | number | Bytes)[] = [String(deltas.descriptor.hashKey?.() ?? "")];
  for (const delta of deltas.deltas) parts.push(delta.weight, delta.record);
  return hashParts(parts);
}

export function builderContainsRecursive(graph: any): boolean {
  return graph.postorder().some((node: any) => node.kind === "Recursive");
}

export function validateArgByPrimaryKeyIndices(
  opName: string,
  table: TableSchema,
  groupFields: number[],
  orderFields: number[],
  primaryKeyFields: number[],
): void {
  const expected = [...groupFields, ...orderFields];
  if (
    primaryKeyFields.length !== expected.length ||
    primaryKeyFields.some((f, i) => f !== expected[i])
  ) {
    throw new IvmRuntimeError(
      "UnsupportedArgMaxBy",
      `${opName} v1 requires primary key for ${table.name} to equal group_cols + order_cols`,
    );
  }
}

interface GroupTransition {
  before: CollectByGroup;
  after: CollectByGroup;
  touched: SortedSet<CollectByOrderKey>;
  touchedComplete: boolean;
}

export interface ChangedWindowRows {
  removed: [number, Bytes][];
  added: [number, Bytes][];
}

/** Touched TopBy groups' first-touch and latest ordered state (#3505). */
export class RootOrderingWindows {
  groups = new Map<string, GroupTransition>();
  descriptor?: RecordDescriptor;
  identity: number[] = [];
  topBy?: TopByOp;

  record(
    descriptor: RecordDescriptor,
    topBy: TopByOp,
    group: Bytes,
    before: () => CollectByGroup,
    after: CollectByGroup,
    touched: Iterable<CollectByOrderKey>,
    touchedComplete: boolean,
  ): void {
    if (this.descriptor === undefined) {
      this.descriptor = descriptor;
      this.identity = topByIdentityFields(topBy, descriptor.fields().length);
      this.topBy = topBy;
    }
    const key = toHex(group);
    let transition = this.groups.get(key);
    if (!transition) {
      transition = {
        before: before(),
        after: new CollectByGroup(),
        touched: new SortedSet(cmpOrderKey),
        touchedComplete: true,
      };
      this.groups.set(key, transition);
    }
    transition.after = after.clone();
    for (const k of touched) transition.touched.add(k);
    transition.touchedComplete &&= touchedComplete;
  }

  groupWindow(group: Bytes): [WindowedRecord[], WindowedRecord[]] | undefined {
    const transition = this.groups.get(toHex(group));
    if (!transition || !this.topBy) return undefined;
    return [
      topByWindowFromOrderedGroup(transition.before, this.topBy),
      topByWindowFromOrderedGroup(transition.after, this.topBy),
    ];
  }

  /** Rows whose presence changed in a zero-offset unbounded window. */
  changedRows(group: Bytes): ChangedWindowRows | undefined {
    const transition = this.groups.get(toHex(group));
    const topBy = this.topBy;
    if (!transition || !topBy) return undefined;
    if (topBy.offset !== 0 || topBy.limit.kind !== "Unbounded" || !transition.touchedComplete) {
      return undefined;
    }
    const present = (g: CollectByGroup, key: CollectByOrderKey) => (g.get(key) ?? 0) > 0;
    const removed: CollectByOrderKey[] = [];
    const added: CollectByOrderKey[] = [];
    for (const key of transition.touched) {
      const b = present(transition.before, key);
      const a = present(transition.after, key);
      if (b && !a) removed.push(key);
      else if (!b && a) added.push(key);
    }
    const rows = (g: CollectByGroup, keys: CollectByOrderKey[]) =>
      g.countBeforeEach(keys).map((index, i): [number, Bytes] => [index, keys[i][1]]);
    return { removed: rows(transition.before, removed), added: rows(transition.after, added) };
  }
}

export interface NodeMemoLookup {
  key: EvalMemoKey;
  inputWatermark: number;
  dependsOnContext: boolean;
}

export interface FrameInputs {
  slots: number[];
}

/** Borrowed runtime pieces used by recursive evaluation to run child graphs. */
export interface EvaluatorEnv {
  schema: DatabaseSchema;
  graph: IvmGraph;
  variantProjections: Map<string, any>;
  tableDeltas: TableDelta[];
  bindingDeltas: BindingDelta[];
  bindingSnapshots: BindingSnapshots;
  currentTick: number;
  operatorStates: KeyedMap<OperatorStateKey, OperatorState>;
  arrangementStates: KeyedMap<ArrangementKey, any /* AsOf<ArrangementState, SubTick> */>;
  arrangementKeysByInput: Map<NodeId, KeyedMap<ArrangementKey, true>>;
  evalMemo: EvaluationMemo;
  evalMemoBytes: { value: number };
  tableFrontiers: Map<string, number>;
  bindingFrontiers: KeyedMap<any, number>;
  memoUseClock: { value: number };
  nodeMeta: Map<NodeId, NodeRuntimeMeta>;
  storage?: OrderedKvStorage;
  evaluationInputs?: EvaluationInputs;
  metrics: TickMetrics;
}

export class GraphRuntimeView {
  constructor(
    readonly env: EvaluatorEnv,
    readonly scope: Scope,
  ) {}

  async evalWithBinding(
    subTick: number,
    binding: any,
    deltas: RecordDeltas,
    node: NodeId,
  ): Promise<RecordDeltas> {
    const evaluator = new TickEvaluator(
      { ...this.env, evaluationInputs: undefined },
      EvalContext.withBinding(this.scope, subTick, binding, deltas),
    );
    return structuredClone(await evaluator.updateSubgraph(node));
  }

  async evalWithBindingAndTableDeltas(
    tableDeltas: TableDelta[],
    subTick: number,
    binding: any,
    deltas: RecordDeltas,
    node: NodeId,
  ): Promise<RecordDeltas> {
    const context = EvalContext.withBindingAndArrangementMode(
      this.scope,
      subTick,
      binding,
      deltas,
      "Replace",
    );
    if (this.env.evaluationInputs) context.evalMode = "Hydrate";
    const evaluator = new TickEvaluator(
      {
        ...this.env,
        tableDeltas,
        evalMemo: new EvaluationMemo(),
        evalMemoBytes: { value: 0 },
      },
      context,
    );
    return structuredClone(await evaluator.updateSubgraph(node));
  }

  clearOperatorStateForScope(): void {
    for (const [key] of [...this.env.operatorStates]) {
      if (key.scope === this.scope) this.env.operatorStates.delete(key);
    }
  }

  async evalRoot(node: NodeId): Promise<RecordDeltas> {
    const context = EvalContext.root();
    context.scope = this.scope;
    const evaluator = new TickEvaluator({ ...this.env, evaluationInputs: undefined }, context);
    return structuredClone(await evaluator.updateSubgraph(node));
  }
}

async function cooperativeOperatorYield(): Promise<never> {
  await yieldNow();
  throw new Error(
    "internal error: entered unreachable code: yielded operator futures resume through saved state",
  );
}

/** Operators whose hydration rebuilds retained state a downstream cache cannot vouch for. */
function holdsHydrationState(operator: { kind: string }): boolean {
  return ["Aggregate", "ArgMinBy", "ArgMaxBy", "Arrange"].includes(operator.kind);
}

const isRoot = (scope: Scope) => scope === ScopeId.root();
const groupBy = (deltas: RecordDelta[], key: (d: RecordDelta) => Bytes) => {
  const out = new SortedMap<Bytes, RecordDelta[]>(compareBytes);
  for (const d of deltas) out.getOrInsert(key(d), () => []).push(d);
  return out;
};

/** Single-tick evaluator over a deduplicated graph. */
export class TickEvaluator implements EvaluatorEnv {
  schema!: DatabaseSchema;
  graph!: IvmGraph;
  variantProjections!: Map<string, any>;
  tableDeltas!: TableDelta[];
  bindingDeltas!: BindingDelta[];
  bindingSnapshots!: BindingSnapshots;
  currentTick!: number;
  operatorStates!: KeyedMap<OperatorStateKey, OperatorState>;
  arrangementStates!: KeyedMap<ArrangementKey, any>;
  arrangementKeysByInput!: Map<NodeId, KeyedMap<ArrangementKey, true>>;
  evalMemo!: EvaluationMemo;
  evalMemoBytes!: { value: number };
  tableFrontiers!: Map<string, number>;
  bindingFrontiers!: KeyedMap<any, number>;
  memoUseClock!: { value: number };
  nodeMeta!: Map<NodeId, NodeRuntimeMeta>;
  storage?: OrderedKvStorage;
  evaluationInputs?: EvaluationInputs;
  metrics!: TickMetrics;
  terminalDeltas = new Map<NodeId, TerminalDeltas>();
  /** Exact pre/post windows captured by TopBy evaluation for terminal ordering. */
  rootOrderingWindows = new Map<NodeId, RootOrderingWindows>();

  constructor(
    env: EvaluatorEnv,
    public context: any,
  ) {
    Object.assign(this, env);
  }

  meta(node: NodeId): NodeRuntimeMeta {
    let meta = this.nodeMeta.get(node);
    if (!meta) this.nodeMeta.set(node, (meta = NodeState.defaultMeta()));
    return meta;
  }

  graphNode(node: NodeId): any {
    const graphNode = this.graph.node(node);
    if (!graphNode) throw new IvmRuntimeError("GraphNodeNotFound", node);
    return graphNode;
  }

  pollReadyNode(
    node: NodeId,
    pending: Map<NodeId, PendingUnaryBatch>,
    frameInputs: FrameInputs,
  ): Poll<ReadyNodeEvaluation> {
    if (!pending.has(node)) {
      const lookup = this.prepareMemoLookup(node);
      const cached = this.cachedNodeRecords(lookup);
      if (cached) return cached;
      const readyInputs = this.supportsResidentBatch(node)
        ? this.resolveRegisterInputs(frameInputs)
        : undefined;
      const op = this.graph.node(node)?.descriptor.operator.kind;
      const residentUnary =
        readyInputs && (op === "Filter" || op === "MapProject") ? readyInputs[0] : undefined;
      const input = residentUnary ?? this.readyUnaryInput(node, lookup);
      if (!input) {
        const result = this.computeReadyBatch(node, lookup, readyInputs);
        if (result) return result;
        return this.computeNode(node, lookup);
      }
      const batch = this.prepareUnaryBatch(node, lookup, input);
      if (!batch) return this.memoizeResult(lookup, this.computeUnaryInput(node, input));
      pending.set(node, batch);
    }
    const batch = pending.get(node)!;
    const graphNode = this.graph.node(node);
    const end = Math.min(batch.next + UNARY_ROWS_PER_POLL, batch.input.deltas.length);
    const input = batch.input.deltas.slice(batch.next, end);
    let result: RecordDeltas;
    try {
      const operator = graphNode.descriptor.operator;
      switch (operator.kind) {
        case "MapProject":
          result = NodeState.updateMapProjectSlice(
            operator.value,
            batch.output.descriptor,
            batch.input.descriptor,
            input,
            batch.projection,
            false,
          );
          break;
        case "Filter": {
          const filter = operator.value;
          const deltas: RecordDelta[] = [];
          for (const delta of input) {
            if (filter.predicate.matches(delta, batch.input.descriptor, filter.comparison))
              deltas.push(delta);
          }
          result = { descriptor: batch.output.descriptor, deltas };
          break;
        }
        default:
          throw new Error("internal error: entered unreachable code: prepared unary operator");
      }
    } catch (error) {
      pending.delete(node);
      throw error;
    }
    batch.output.deltas.push(...result.deltas);
    batch.next = end;
    if (end < batch.input.deltas.length) return PENDING;
    pending.delete(node);
    return this.memoizeResult(batch.lookup, batch.output);
  }

  private prepareUnaryBatch(
    node: NodeId,
    lookup: NodeMemoLookup,
    input: RecordDeltas,
  ): PendingUnaryBatch | undefined {
    if (input.deltas.length <= UNARY_ROWS_PER_POLL) return undefined;
    const graphNode = this.graphNode(node);
    const output = graphNode.descriptor.output.records();
    const operator = graphNode.descriptor.operator;
    let projection: unknown;
    if (operator.kind === "MapProject") {
      projection = this.rawProjectionFields(node, operator.value, input.descriptor, output);
    } else if (operator.kind === "Filter") {
      const referenced = new Set<string>();
      operator.value.predicate.referencedFields(referenced);
      // Indirect fields keep the existing kernel's request/error ordering.
      for (const field of referenced) {
        const index = resolveFieldName(input.descriptor, field);
        if (index === undefined) return undefined;
        if (input.descriptor.fields()[index].valueType.mayContainStoredScalar()) return undefined;
      }
    } else {
      throw new Error("internal error: entered unreachable code: checked unary operator");
    }
    this.noteHydrationCompute(node);
    return { lookup, input, next: 0, output: emptyDeltas(output), projection };
  }

  /** Evaluate one reachable graph slice in dependency order, iteratively. */
  async updateSubgraph(root: NodeId): Promise<RecordDeltas> {
    const pending: [NodeId, boolean][] = [[root, false]];
    const discovered = new Set<NodeId>();
    const order: NodeId[] = [];
    while (pending.length) {
      const [node, expanded] = pending.pop()!;
      if (expanded) {
        order.push(node);
        continue;
      }
      if (discovered.has(node)) continue;
      discovered.add(node);
      const graphNode = this.graphNode(node);
      pending.push([node, true]);
      // Recursive seed and step graphs run under a frontier-scoped evaluator.
      if (graphNode.descriptor.operator.kind !== "Recursive") {
        for (const input of [...graphNode.descriptor.inputs].reverse())
          pending.push([input, false]);
      }
    }
    let result: RecordDeltas | undefined;
    for (const node of order) {
      const records = await this.updateReadyNode(node);
      if (node === root) result = records;
    }
    if (!result) throw new IvmRuntimeError("GraphNodeNotFound", root);
    return result;
  }

  applyRootOrdering(
    orderingNode: NodeId,
    rootDescriptor: RecordDescriptor,
    identity: [RootIdentity, SortedSet<Bytes>] | undefined,
    terminal: TerminalDeltas,
  ): void {
    const windows = this.rootOrderingWindows.get(orderingNode);
    if (!windows || !identity) return;
    const [rootIdentity, groups] = identity;
    const descriptor = windows.descriptor;
    if (descriptor === undefined) return;
    const graph = this.graph;
    const keyOf = (record: Bytes): Bytes => {
      if (!rootIdentity.projected)
        return encodedIdentityKeyPart(descriptor, record, windows.identity);
      const projected = projectWindowRecord(graph, rootIdentity.chain, descriptor, record);
      if (!projected) return new Uint8Array();
      return encodedIdentityKeyPart(projected[0], projected[1], rootIdentity.fields);
    };
    let visited = 0;
    if (!rootIdentity.filtered) {
      for (const group of groups) {
        const changed = windows.changedRows(group);
        if (changed) {
          visited += changed.removed.length + changed.added.length;
          const keyed = (rows: [number, Bytes][]) =>
            rows.map(([index, record]) => [index, keyOf(record)]);
          const changes = { removed: keyed(changed.removed), added: keyed(changed.added) };
          if (applyChangedGroupOrdering(changes, rootDescriptor, terminal)) continue;
        }
        const window = windows.groupWindow(group);
        if (window) {
          visited += window[0].length + window[1].length;
          applyGroupWindowOrdering(window[0], window[1], keyOf, rootDescriptor, terminal);
        }
      }
      this.metrics.rootOrderingPositionRecords += visited;
      return;
    }
    // What this tick's output edits say about each root key.
    const evidence = new Map<string, [boolean, boolean]>();
    for (const operation of terminal.operations) {
      const k = toHex(operation.rootKey);
      let seen = evidence.get(k);
      if (!seen) evidence.set(k, (seen = [false, false]));
      const topLevel = operation.path.length === 0;
      if (operation.edit.kind === "Insert" && topLevel) seen[1] = true;
      else if (operation.edit.kind === "Remove" && topLevel) seen[0] = true;
      else seen[0] = seen[1] = true;
    }
    const reaching = (window: WindowedRecord[], other: WindowedRecord[], after: boolean) => {
      const kept: WindowedRecord[] = [];
      for (const entry of window) {
        let reaches: boolean;
        switch (windowRecordReachesOutput(graph, rootIdentity.chain, descriptor, entry[0])) {
          case "Yes":
            reaches = true;
            break;
          case "No":
            reaches = false;
            break;
          default: {
            const seen = evidence.get(toHex(keyOf(entry[0])));
            reaches = seen
              ? after
                ? seen[1]
                : seen[0]
              : other.some(([record]) => equalBytes(record, entry[0]));
          }
        }
        if (reaches) kept.push(entry);
      }
      return kept;
    };
    for (const group of groups) {
      const window = windows.groupWindow(group);
      if (!window) continue;
      const [before, after] = window;
      visited += before.length + after.length;
      applyGroupWindowOrdering(
        reaching(before, after, false),
        reaching(after, before, true),
        keyOf,
        rootDescriptor,
        terminal,
      );
    }
    this.metrics.rootOrderingPositionRecords += visited;
  }

  terminalDeltaNodeForOutput(node: NodeId): NodeId | undefined {
    const lineage = this.terminalLineage(node);
    let fallback: NodeId | undefined;
    for (const [candidate, isPublicRoot] of lineage.candidates) {
      if (this.terminalDeltas.has(candidate)) {
        if (isPublicRoot) return candidate;
        fallback ??= candidate;
      }
    }
    return lineage.hasPublicRoot ? undefined : fallback;
  }

  terminalDeltasForConsumer(node: NodeId, lastConsumer: boolean): TerminalDeltas | undefined {
    const deltas = this.terminalDeltas.get(node);
    if (lastConsumer) this.terminalDeltas.delete(node);
    return lastConsumer ? deltas : deltas && structuredClone(deltas);
  }

  outputIsStructuredCollectBy(node: NodeId): boolean {
    return outputIsStructuredCollectBy(this.graph, node);
  }

  outputHasPublicRoot(node: NodeId): boolean {
    return this.terminalLineage(node).hasPublicRoot;
  }

  private terminalLineage(root: NodeId): TerminalLineage {
    const cached = this.nodeMeta.get(root)?.terminalLineage;
    if (cached) return cached;
    const lineage: TerminalLineage = { candidates: [], hasPublicRoot: false };
    const pending = [root];
    const seen = new Set<NodeId>();
    while (pending.length) {
      const node = pending.pop()!;
      if (seen.has(node)) continue;
      seen.add(node);
      const graphNode = this.graphNode(node);
      // CollectBy is the only operator publishing into terminal_deltas.
      const operator = graphNode.descriptor.operator;
      if (operator.kind === "CollectBy") {
        const isPublic = operator.value.mode === "Root";
        lineage.hasPublicRoot ||= isPublic;
        lineage.candidates.push([node, isPublic]);
      }
      pending.push(...graphNode.descriptor.inputs);
    }
    this.meta(root).terminalLineage = lineage;
    return lineage;
  }

  private nodeDependsOnAggregate(node: NodeId): boolean {
    const cached = (n: NodeId) => this.nodeMeta.get(n)?.hasHydrationStateAncestor;
    const hit = cached(node);
    if (hit !== undefined) return hit;
    // Memoizing every visited node keeps a whole graph's classification linear.
    const pending: [NodeId, boolean][] = [[node, false]];
    while (pending.length) {
      const [current, expanded] = pending.pop()!;
      if (cached(current) !== undefined) continue;
      const graphNode = this.graphNode(current);
      let depends: boolean;
      if (holdsHydrationState(graphNode.descriptor.operator)) depends = true;
      else if (expanded)
        depends = graphNode.descriptor.inputs.some((input: NodeId) => cached(input) === true);
      else {
        pending.push([current, true]);
        for (const input of graphNode.descriptor.inputs) {
          if (cached(input) === undefined) pending.push([input, false]);
        }
        continue;
      }
      this.meta(current).hasHydrationStateAncestor = depends;
    }
    return cached(node)!;
  }

  private aggregateArrangementsAreCurrent(node: NodeId): boolean {
    return this.readinessFrontier(node).every((producer) => this.producerStateIsCurrent(producer));
  }

  private readinessFrontier(root: NodeId): NodeId[] {
    const cached = this.nodeMeta.get(root)?.readinessFrontier;
    if (cached) return cached;
    const frontier: NodeId[] = [];
    const pending = [root];
    const seen = new Set<NodeId>();
    while (pending.length) {
      const node = pending.pop()!;
      if (seen.has(node)) continue;
      seen.add(node);
      const graphNode = this.graphNode(node);
      const kind = graphNode.descriptor.operator.kind;
      if (kind === "Recursive") {
        // Child scopes are owned by this producer.
        frontier.push(node);
        continue;
      }
      if (
        kind === "Arrange" ||
        kind === "ArgMinBy" ||
        kind === "ArgMaxBy" ||
        kind === "Aggregate"
      ) {
        frontier.push(node);
      }
      pending.push(...[...graphNode.descriptor.inputs].reverse());
    }
    this.meta(root).readinessFrontier = frontier;
    return frontier;
  }

  private producerStateIsCurrent(node: NodeId): boolean {
    const graphNode = this.graphNode(node);
    const operator = graphNode.descriptor.operator;
    const inputs: NodeId[] = graphNode.descriptor.inputs;
    if (operator.kind === "Recursive") {
      const state = this.operatorStates.get(this.operatorKey(node));
      const generation = this.inputGeneration(node);
      return (
        state?.kind === "Recursive" &&
        state.state.asOf === this.currentTick &&
        !state.state.value.hasPendingHydration() &&
        state.state.value.stepArrangementsHydrated() &&
        state.state.value.hydratedInputGeneration() === generation
      );
    }
    if (operator.kind === "Arrange" && this.arrangementNeedsIndex(node)) {
      const key = { scope: this.context.scope, input: node };
      if (!this.asOfMatches(this.arrangementStates.get(key), this.arrangementSubTick(key)))
        return false;
    }
    if (operator.kind === "ArgMinBy" || operator.kind === "ArgMaxBy") {
      const key = this.operatorKey(node);
      const expected = this.scopedSubTick(key.scope);
      const state = this.operatorStates.get(key);
      if (state?.kind !== "ArgBy" || !this.asOfMatches(state.state, expected)) return false;
    }
    if (operator.kind === "Aggregate") {
      if (inputs.length !== 1) throw new IvmRuntimeError("GraphInputArityMismatch", node);
      const inputDesc = this.graphNode(inputs[0]).descriptor.output;
      const groupFields = this.aggregateGroupFields(node, operator.value);
      const key = this.arrangementKey(inputs[0], inputDesc.records(), groupFields, "Exact");
      if (!this.asOfMatches(this.arrangementStates.get(key), this.arrangementSubTick(key)))
        return false;
    }
    return true;
  }

  private asOfMatches(state: any, expected: SubTick): boolean {
    return state?.asOf !== undefined && subTickCmp(state.asOf, expected) === 0;
  }

  private scopedSubTick(scope: Scope): SubTick {
    return { tick: this.currentTick, subTick: isRoot(scope) ? 0 : this.context.subTick };
  }

  /** Memo hits stay synchronous so the driver never nests evaluator futures. */
  updateNode(node: NodeId): ReadyNodeEvaluation {
    const cached = this.cachedNodeRecords(this.prepareMemoLookup(node));
    return cached ?? this.updateSubgraph(node);
  }

  prepareMemoLookup(node: NodeId): NodeMemoLookup {
    const meta = this.nodeMeta.get(node);
    const signature = meta?.inputSignature ?? this.inputSignature(node);
    return {
      key: this.memoKey(node, signature),
      inputWatermark: meta?.inputSignature ? meta.inputGeneration : this.inputGeneration(node),
      dependsOnContext: signature.frontierBindings.length > 0,
    };
  }

  cachedNodeRecords(lookup: NodeMemoLookup): RecordDeltas | undefined {
    const node = lookup.key.node;
    const entry = this.evalMemo.get(lookup.key);
    if (!entry || entry.inputWatermark !== lookup.inputWatermark) return undefined;
    if (!this.cachedResultStateIsCurrent(node)) return undefined;
    entry.lastUsed = ++this.memoUseClock.value;
    if (this.context.evalMode === "Hydrate") this.metrics.hydrationMemoHits++;
    return entry.records;
  }

  private cachedResultStateIsCurrent(node: NodeId): boolean {
    // A cached batch is not proof that its producer-owned index exists in this scope.
    const kind = this.graph.node(node)?.descriptor.operator.kind;
    let requiresProducer = false;
    if (kind === "Arrange" && this.arrangementNeedsIndex(node)) {
      requiresProducer = !this.arrangementStates.has({ scope: this.context.scope, input: node });
    } else if (kind === "Lookup" && this.context.evalMode === "Hydrate") {
      const state = this.operatorStates.get(this.operatorKey(node));
      requiresProducer = !(state?.kind === "Lookup" && state.state.asOf !== undefined);
    }
    const requiresRebuild =
      requiresProducer ||
      (this.context.hydrateArrangements &&
        this.nodeDependsOnAggregate(node) &&
        !this.aggregateArrangementsAreCurrent(node)) ||
      (this.context.evalMode === "Tick" && this.context.arrangementUpdateMode === "Replace");
    return !requiresRebuild;
  }

  resolveRegisterInputs(inputs: FrameInputs): RecordDeltas[] | undefined {
    const resolved: RecordDeltas[] = [];
    for (const slot of inputs.slots) {
      const occupied = this.evalMemo.slot(slot);
      if (!occupied) return undefined;
      const node = occupied[0].node;
      const lookup = this.prepareMemoLookup(node);
      const current = this.evalMemo.slot(slot);
      if (
        !current ||
        !this.evalMemo.sameKey(current[0], lookup.key) ||
        current[1].inputWatermark !== lookup.inputWatermark ||
        !this.cachedResultStateIsCurrent(node)
      ) {
        return undefined;
      }
      const entry = current[1];
      entry.lastUsed = ++this.memoUseClock.value;
      if (this.context.evalMode === "Hydrate") this.metrics.hydrationMemoHits++;
      resolved.push(entry.records);
    }
    return resolved;
  }

  /** Execute a node whose ordinary inputs the caller's queue already drove. */
  updateReadyNode(node: NodeId): ReadyNodeEvaluation {
    const lookup = this.prepareMemoLookup(node);
    const cached = this.cachedNodeRecords(lookup);
    if (cached) return cached;
    return this.computeReadyBatch(node, lookup, undefined) ?? this.computeNode(node, lookup);
  }

  private readyUnaryInput(node: NodeId, lookup: NodeMemoLookup): RecordDeltas | undefined {
    if (this.context.subTick > 1 && !lookup.dependsOnContext) return undefined;
    const graphNode = this.graph.node(node);
    if (!graphNode) return undefined;
    const kind = graphNode.descriptor.operator.kind;
    if (kind !== "Filter" && kind !== "MapProject") return undefined;
    if (graphNode.descriptor.inputs.length !== 1)
      throw new IvmRuntimeError("GraphInputArityMismatch", node);
    return this.cachedNodeRecords(this.prepareMemoLookup(graphNode.descriptor.inputs[0]));
  }

  private computeUnaryInput(node: NodeId, input: RecordDeltas): RecordDeltas {
    const graphNode = this.graphNode(node);
    const output = graphNode.descriptor.output.records();
    this.noteHydrationCompute(node);
    const operator = graphNode.descriptor.operator;
    if (operator.kind === "Filter") return this.computeFilter(node, operator.value, output, input);
    if (operator.kind === "MapProject")
      return this.computeProjection(node, operator.value, output, input);
    throw new Error("internal error: entered unreachable code: checked unary kernel");
  }

  memoizeResult(lookup: NodeMemoLookup, result: RecordDeltas): RecordDeltas {
    this.metrics.recordsProcessed += result.deltas.length;
    this.metrics.nodesEvaluated++;
    const payloadBytes = recordDeltasEncodedBytes(result);
    const used = ++this.memoUseClock.value;
    const previous = this.evalMemo.insert(
      lookup.key,
      new EvalMemoEntry(result, lookup.inputWatermark, payloadBytes, used),
    );
    if (previous)
      this.evalMemoBytes.value = Math.max(0, this.evalMemoBytes.value - previous.payloadBytes);
    this.evalMemoBytes.value += payloadBytes;
    return result;
  }

  computeFilter(
    node: NodeId,
    filter: any,
    outputDesc: RecordDescriptor,
    input: RecordDeltas,
  ): RecordDeltas {
    const inputs = this.evaluationInputs;
    if (filter.predicate.supportsIndirectLiteralAttempt() && inputs) {
      const deltas: RecordDelta[] = [];
      for (const delta of input.deltas) {
        inputs.setChunkScope(node);
        let result: boolean | undefined;
        try {
          result = filter.predicate.matchesIndirectLiteralAttempt(delta, input.descriptor, inputs);
        } finally {
          inputs.setChunkScope(undefined);
        }
        const matches =
          result ?? filter.predicate.matches(delta, input.descriptor, filter.comparison);
        if (matches) deltas.push(delta);
      }
      return { descriptor: outputDesc, deltas };
    }
    const referenced = new Set<string>();
    filter.predicate.referencedFields(referenced);
    const materialized = this.materializeIndirectFields(input, referenced);
    if (materialized === input) return NodeState.updateFilter(filter, outputDesc, input);
    // Materialization only serves the predicate; emit rows as they arrived (#3309).
    const deltas = input.deltas.filter((_, i) =>
      filter.predicate.matches(materialized.deltas[i], materialized.descriptor, filter.comparison),
    );
    return { descriptor: outputDesc, deltas };
  }

  computeProjection(
    node: NodeId,
    project: any,
    outputDesc: RecordDescriptor,
    input: RecordDeltas,
  ): RecordDeltas {
    const rawProjection = this.rawProjectionFields(node, project, input.descriptor, outputDesc);
    return NodeState.updateMapProject(project, outputDesc, input, rawProjection, false);
  }

  /** The general (async) path: external requests and recursive scopes. */
  async computeNode(node: NodeId, lookup: NodeMemoLookup): Promise<RecordDeltas> {
    this.noteHydrationCompute(node);
    const graphNode = this.graphNode(node);
    const outputDesc = graphNode.descriptor.output.records();
    if (this.context.subTick > 1 && !lookup.dependsOnContext) {
      return this.memoizeResult(lookup, emptyDeltas(outputDesc));
    }
    const operator = graphNode.descriptor.operator;
    let result: RecordDeltas;
    if (
      operator.kind === "IndexSource" &&
      (this.context.evalMode !== "Hydrate" || !this.evaluationInputs)
    ) {
      result = await NodeState.updateIndexSource(
        operator.value,
        this.schema,
        this.variantProjections,
        outputDesc,
        this.tableDeltas,
        this.storage,
        this.context.evalMode,
      );
    } else if (operator.kind === "StreamingChecksum") {
      const input = await this.updateUnaryInput(graphNode, node);
      result = await this.updateStreamingChecksum(node, operator.value, outputDesc, input);
    } else if (operator.kind === "Recursive") {
      const inputs: NodeId[] = graphNode.descriptor.inputs;
      if (inputs.length !== 2 && inputs.length !== 3)
        throw new IvmRuntimeError("GraphInputArityMismatch", node);
      result = await this.updateRecursive(
        node,
        operator.value,
        outputDesc,
        inputs[0],
        inputs[1],
        inputs[2],
      );
    } else {
      const inputs: RecordDeltas[] = [];
      for (const input of graphNode.descriptor.inputs) inputs.push(await this.updateNode(input));
      result = computeBatch(this, node, inputs);
    }
    return this.memoizeResult(lookup, result);
  }

  private noteHydrationCompute(node: NodeId): void {
    if (this.context.evalMode === "Hydrate") {
      this.metrics.hydrationMemoComputes++;
      this.metrics.hydrationMemoComputedNodes.add(node);
    }
  }

  /** Resolve every input before mutating operator state. */
  private computeReadyBatch(
    node: NodeId,
    lookup: NodeMemoLookup,
    readyInputs: RecordDeltas[] | undefined,
  ): RecordDeltas | undefined {
    const graphNode = this.graph.node(node);
    if (!graphNode) return undefined;
    if (this.context.subTick > 1 && !lookup.dependsOnContext) {
      this.noteHydrationCompute(node);
      return this.memoizeResult(lookup, emptyDeltas(graphNode.descriptor.output.records()));
    }
    if (!this.supportsResidentBatch(node)) return undefined;
    let inputs = readyInputs;
    if (!inputs) {
      inputs = [];
      for (const input of graphNode.descriptor.inputs) {
        const records = this.cachedNodeRecords(this.prepareMemoLookup(input));
        if (!records) return undefined;
        inputs.push(records);
      }
    }
    this.noteHydrationCompute(node);
    return this.memoizeResult(lookup, computeBatch(this, node, inputs));
  }

  private supportsResidentBatch(node: NodeId): boolean {
    const kind = this.graph.node(node)?.descriptor.operator.kind;
    if (kind === undefined || kind === "Recursive" || kind === "StreamingChecksum") return false;
    if (kind === "IndexSource")
      return this.context.evalMode === "Hydrate" && !!this.evaluationInputs;
    return true;
  }

  memoKey(node: NodeId, signature: any): EvalMemoKey {
    return {
      scope: this.context.scope,
      node,
      inputSignatureHash: signature.hash,
      tickEpoch: this.context.evalMode === "Tick" ? this.currentTick : undefined,
      subTick: this.context.subTick,
      contextDigest: this.contextDigest(signature),
    };
  }

  inputGeneration(node: NodeId): number {
    return this.nodeMeta.get(node)?.inputGeneration ?? 0;
  }

  contextDigest(signature: any): bigint {
    if (signature.frontierBindings.length === 0) return 0n;
    const parts: (string | number)[] = [];
    for (const binding of signature.frontierBindings) {
      parts.push(String(binding), Number(this.context.bindingDigests.get(binding) ?? 0));
    }
    return hashParts(parts);
  }

  /** Recursive step state is isolated per recursive scope; root queries share. */
  operatorKey(node: NodeId): OperatorStateKey {
    return { scope: this.context.scope, node };
  }

  inputSignature(node: NodeId, seen = new Set<NodeId>()): any {
    const cached = this.nodeMeta.get(node)?.inputSignature;
    if (cached) return cached;
    if (seen.has(node)) return NodeInputSignature.empty();
    seen.add(node);
    const graphNode = this.graphNode(node);
    const operator = graphNode.descriptor.operator;
    const tables = new Set<string>();
    const bindings = new Set<any>();
    const frontierBindings = new Set<any>();
    switch (operator.kind) {
      case "TableSource":
      case "IndexSource":
        tables.add(operator.value.table);
        break;
      case "Lookup":
        tables.add(operator.value.source.table);
        break;
      case "BindingSource":
        bindings.add(operator.value.key);
        break;
      case "FrontierSource":
        frontierBindings.add(operator.value.binding);
        break;
    }
    for (const input of graphNode.descriptor.inputs) {
      const child = this.inputSignature(input, seen);
      for (const t of child.tables) tables.add(t);
      for (const b of child.bindings) bindings.add(b);
      for (const f of child.frontierBindings) frontierBindings.add(f);
    }
    seen.delete(node);
    const signature = NodeInputSignature.fromSets(tables, bindings, frontierBindings);
    const meta = this.meta(node);
    meta.dependsOnContext = signature.frontierBindings.length > 0;
    meta.inputSignature = signature;
    return signature;
  }

  rawProjectionFields(
    node: NodeId,
    project: any,
    inputDesc: RecordDescriptor,
    outputDesc: RecordDescriptor,
  ): any {
    const meta = this.meta(node);
    if (meta.rawProjectionFields !== undefined) return meta.rawProjectionFields;
    const resolved = rawProjectionFields(project, inputDesc, outputDesc) ?? null;
    meta.rawProjectionFields = resolved;
    return resolved;
  }

  frontierSource(frontierSource: any, output: RecordDescriptor): RecordDeltas {
    const deltas = this.context.bindings.get(frontierSource.binding) ?? emptyDeltas(output);
    if (!deltas.descriptor.registryCompatibleWith(output))
      throw new IvmRuntimeError("GraphOutputMismatch");
    return deltas;
  }

  updateJoin(
    node: NodeId,
    join: any,
    outputDesc: RecordDescriptor,
    leftInput: NodeId,
    rightInput: NodeId,
  ): RecordDeltas {
    const [leftOn, rightOn] = this.joinFieldNames(node, join);
    const projection = this.joinOutputProjection(
      node,
      join.leftDescriptor,
      join.rightDescriptor,
      outputDesc,
    );
    const leftKey = this.arrangementKey(leftInput, join.leftDescriptor, leftOn, join.comparison);
    const rightKey = this.arrangementKey(
      rightInput,
      join.rightDescriptor,
      rightOn,
      join.comparison,
    );
    const leftState = this.arrangementStates.get(leftKey);
    if (!leftState) throw new IvmRuntimeError("GraphNodeNotFound", leftInput);
    const rightState = this.arrangementStates.get(rightKey);
    if (!rightState) throw new IvmRuntimeError("GraphNodeNotFound", rightInput);
    const deltas = new JoinState().evaluatePrepared(
      ArrangementTransition.at(leftState, this.arrangementSubTick(leftKey)),
      ArrangementTransition.at(rightState, this.arrangementSubTick(rightKey)),
      projection,
      this.context.arrangementUpdateMode,
    );
    return { descriptor: outputDesc, deltas };
  }

  updateThresholdJoin(
    node: NodeId,
    join: any,
    outputDesc: RecordDescriptor,
    leftInput: NodeId,
    rightInput: NodeId,
    semi: boolean,
  ): RecordDeltas {
    const operatorKey = this.operatorKey(node);
    const [leftOn, rightOn] = this.joinFieldNames(node, join);
    const leftKey = this.arrangementKey(leftInput, join.leftDescriptor, leftOn, join.comparison);
    const rightKey = this.arrangementKey(
      rightInput,
      join.rightDescriptor,
      rightOn,
      join.comparison,
    );
    const left = this.arrangementStates.get(leftKey);
    if (!left) throw new IvmRuntimeError("GraphNodeNotFound", leftInput);
    const right = this.arrangementStates.get(rightKey);
    if (!right) throw new IvmRuntimeError("GraphNodeNotFound", rightInput);
    const operator = this.operatorStates.getOrInsert(operatorKey, () =>
      semi
        ? { kind: "SemiJoin", state: new ThresholdJoinState(true) }
        : { kind: "AntiJoin", state: new ThresholdJoinState(false) },
    );
    if (operator.kind !== (semi ? "SemiJoin" : "AntiJoin")) {
      throw new IvmRuntimeError("NodeStateOperatorMismatch", node);
    }
    const deltas = (operator as { state: ThresholdJoinState }).state.evaluate(
      ArrangementTransition.at(left, this.arrangementSubTick(leftKey)),
      ArrangementTransition.at(right, this.arrangementSubTick(rightKey)),
      this.context.arrangementUpdateMode,
    );
    return { descriptor: outputDesc, deltas };
  }

  private outOfOrder(current: SubTick | number, next: SubTick | number): Error {
    return new IvmRuntimeError("OutOfOrderRuntimeState", {
      current: JSON.stringify(current),
      next: JSON.stringify(next),
    });
  }

  private takeOperator(
    node: NodeId,
    key: OperatorStateKey,
    kind: OperatorState["kind"],
    make: () => OperatorState,
  ): any {
    const operator = this.operatorStates.get(key) ?? make();
    this.operatorStates.delete(key);
    if (operator.kind !== kind) throw new IvmRuntimeError("NodeStateOperatorMismatch", node);
    return operator;
  }

  updateArgBy(
    node: NodeId,
    spec: ArgBySpec,
    outputDesc: RecordDescriptor,
    input: RecordDeltas,
  ): RecordDeltas {
    if (this.context.evalMode === "Hydrate" && !this.context.hydrateArrangements) {
      // A probe needs only the winners, not an index for future deltas.
      return hydratedArgByWinners(
        input,
        outputDesc,
        spec.groupFieldIndices,
        spec.comparisonFieldIndices,
        spec.direction,
      );
    }
    const operatorKey = this.operatorKey(node);
    const operator = this.takeOperator(node, operatorKey, "ArgBy", () =>
      operatorStateFor({ kind: "ArgMaxBy" }),
    );
    const state = operator.state;
    const subTick = this.scopedSubTick(operatorKey.scope);
    if (state.asOf !== undefined && subTickCmp(state.asOf, subTick) > 0)
      throw this.outOfOrder(state.asOf, subTick);
    const replace = this.context.arrangementUpdateMode === "Replace";
    if (!replace && this.asOfMatches(state, subTick)) {
      this.operatorStates.set(operatorKey, operator);
      return emptyDeltas(outputDesc);
    }
    // Hydration supplies the complete input; clear so a shared node keeps no stale groups.
    const groups: SparseGroups = state.value;
    if (replace) groups.clear();
    const touched = groupBy(input.deltas, (d) =>
      encodedArrangementKeyPart(outputDesc, d.record, spec.groupFieldIndices),
    );
    const output: RecordDelta[] = [];
    for (const [groupPrefix, groupDeltas] of touched) {
      const group = groups.getOrDefault(groupPrefix);
      const before = argByOrderedWinner(group);
      for (const delta of groupDeltas) {
        const key = argByOrderKey(outputDesc, delta, spec);
        group.set(key, (group.get(key) ?? 0) + delta.weight);
      }
      const after = argByOrderedWinner(group);
      groups.removeEmptyTouchedGroups([groupPrefix]);
      if (before === after || (before && after && equalBytes(before, after))) continue;
      if (before) output.push({ record: before, weight: -1 });
      if (after) output.push({ record: after, weight: 1 });
    }
    state.markForwardAsOf(subTick);
    this.operatorStates.set(operatorKey, operator);
    return { descriptor: outputDesc, deltas: output };
  }

  updateTopBy(
    node: NodeId,
    topBy: TopByOp,
    outputDesc: RecordDescriptor,
    input: RecordDeltas,
  ): RecordDeltas {
    if (input.deltas.length === 0 || (topBy.limit.kind === "Finite" && topBy.limit.value === 0)) {
      return emptyDeltas(outputDesc);
    }
    const operatorKey = this.operatorKey(node);
    const operator = this.takeOperator(node, operatorKey, "TopBy", () =>
      operatorStateFor({ kind: "TopBy" }),
    );
    const state = operator.state;
    const subTick = this.scopedSubTick(operatorKey.scope);
    if (state.asOf !== undefined && subTickCmp(state.asOf, subTick) > 0)
      throw this.outOfOrder(state.asOf, subTick);
    if (this.context.arrangementUpdateMode === "Accumulate" && this.asOfMatches(state, subTick)) {
      this.operatorStates.set(operatorKey, operator);
      return emptyDeltas(outputDesc);
    }
    const value: TopByIncrementalState = state.value;
    const touchedGroups = groupBy(input.deltas, (d) =>
      encodedRecordKeyPart(outputDesc, d.record, topBy.groupFieldIndices),
    );
    const output: RecordDelta[] = [];
    const replace = this.context.arrangementUpdateMode === "Replace";
    const positions = this.rootOrderingWindows.has(node);
    if (!replace && topBy.offset === 0 && topBy.limit.kind === "Unbounded") {
      // Without a selection boundary, only touched records can change membership.
      for (const [groupPrefix, groupDeltas] of touchedGroups) {
        const before = positions
          ? (value.groups.get(groupPrefix)?.clone() ?? new CollectByGroup())
          : undefined;
        const group = value.groups.getOrDefault(groupPrefix);
        const touched: CollectByOrderKey[] = [];
        output.push(
          ...updateUnboundedTopByGroupTouching(
            outputDesc,
            topBy,
            group,
            groupDeltas,
            positions ? touched : undefined,
          ),
        );
        this.metrics.topByDeltaMembershipRecords += groupDeltas.length;
        if (before) {
          this.rootOrderingWindows
            .get(node)!
            .record(outputDesc, topBy, groupPrefix, () => before, group, touched, true);
        }
      }
      value.removeEmptyTouchedGroups(touchedGroups.keysArray());
      state.markForwardAsOf(subTick);
      this.operatorStates.set(operatorKey, operator);
      return { descriptor: outputDesc, deltas: output };
    }
    const beforeGroups = new SortedMap<Bytes, CollectByGroup>(compareBytes);
    for (const group of touchedGroups.keysArray()) {
      beforeGroups.set(
        group,
        replace ? new CollectByGroup() : (value.groups.get(group)?.clone() ?? new CollectByGroup()),
      );
    }
    if (replace) value.groups.clear();
    const touchedKeys = new Map<string, CollectByOrderKey[]>();
    for (const [groupPrefix, groupDeltas] of touchedGroups) {
      const group = value.groups.getOrDefault(groupPrefix);
      for (const delta of groupDeltas) {
        const orderKey: CollectByOrderKey = [
          topBySortKey(outputDesc, delta.record, topBy),
          delta.record,
        ];
        const weight = (group.get(orderKey) ?? 0) + delta.weight;
        if (positions) {
          const k = toHex(groupPrefix);
          touchedKeys.set(k, [...(touchedKeys.get(k) ?? []), orderKey]);
        }
        group.set(orderKey, weight);
      }
    }
    value.removeEmptyTouchedGroups(touchedGroups.keysArray());
    state.markForwardAsOf(subTick);
    for (const groupPrefix of touchedGroups.keysArray()) {
      const beforeGroup = beforeGroups.get(groupPrefix)!;
      const afterGroup = value.groups.get(groupPrefix)?.clone() ?? new CollectByGroup();
      const before = topByWindowFromOrderedGroup(beforeGroup, topBy);
      const after = topByWindowFromOrderedGroup(afterGroup, topBy);
      const windows = this.rootOrderingWindows.get(node);
      if (windows) {
        windows.record(
          outputDesc,
          topBy,
          groupPrefix,
          () => beforeGroup.clone(),
          afterGroup,
          touchedKeys.get(toHex(groupPrefix)) ?? [],
          !replace,
        );
      } else {
        this.metrics.rootOrderingPositionRecordsSkipped += before.length + after.length;
      }
      output.push(...diffRecordWindows(before, after));
    }
    this.operatorStates.set(operatorKey, operator);
    return { descriptor: outputDesc, deltas: output };
  }

  /** Render touched flat groups as complete parents. */
  updateCollectBy(
    node: NodeId,
    collectBy: any,
    outputDesc: RecordDescriptor,
    input: RecordDeltas,
    canonical: RecordDeltas,
  ): RecordDeltas {
    if (this.context.evalMode === "Hydrate") {
      // Hydration supplies a complete snapshot: rebuild instead of applying inserts.
      this.operatorStates.delete(this.operatorKey(node));
    }
    if (
      input.deltas.length === 0 ||
      (collectBy.limit.kind === "Finite" && collectBy.limit.value === 0)
    ) {
      return emptyDeltas(outputDesc);
    }
    let hydrationSnapshot: RecordDelta[] | undefined;
    const slots = collectBy.slots;
    let directTreeSlot: any;
    if (
      slots.length === 1 &&
      slots[0].slots.length === 0 &&
      slots[0].limit.kind === "Unbounded" &&
      slots[0].referenceArrayFieldIndex === undefined
    ) {
      directTreeSlot = slots[0];
    }
    if (
      collectBy.mode === "Root" ||
      (collectBy.mode === "Collect" &&
        (slots.length === 0 || directTreeSlot) &&
        (collectBy.limit.kind === "Unbounded" || directTreeSlot))
    ) {
      const operatorKey = this.operatorKey(node);
      const operator = this.takeOperator(node, operatorKey, "CollectBy", () =>
        operatorStateFor({ kind: "CollectBy" }),
      );
      const operations = updateUnboundedCollectByTerminalState(
        input.descriptor,
        outputDesc,
        collectBy,
        directTreeSlot,
        operator.state,
        input.deltas,
        this.context.evalMode === "Tick" || this.context.evalMode === "Hydrate",
      );
      if (this.context.evalMode === "Hydrate" && collectBy.mode === "Root") {
        hydrationSnapshot = singletonRootHydrationSnapshot(
          operator.state,
          input.deltas,
          operations,
        );
      }
      this.operatorStates.set(operatorKey, operator);
      // Retain hydration operations so the opening consumer can seed its terminal tree.
      if (operations.length) this.terminalDeltas.set(node, { operations });
      if (this.context.evalMode === "Tick") return emptyDeltas(outputDesc);
    }
    const inputs: NodeId[] = this.graphNode(node).descriptor.inputs;
    if (inputs.length !== 1) throw new IvmRuntimeError("GraphInputArityMismatch", node);
    const inputNode = inputs[0];
    if (hydrationSnapshot) return { descriptor: outputDesc, deltas: hydrationSnapshot };
    const inputDesc = input.descriptor;
    const arrangementKey = this.arrangementKey(
      inputNode,
      inputDesc,
      collectBy.groupFields,
      "Exact",
    );
    let arrangement: any;
    if (collectBy.mode === "Root") {
      // Only ambiguous hydration needs a temporary snapshot view.
      arrangement = new AsOf(new ArrangementState());
      arrangement.value.applyRecordDeltas(
        inputDesc,
        collectBy.groupFields,
        input.deltas,
        "Replace",
      );
    } else {
      arrangement = this.arrangementStates.get(arrangementKey);
      if (!arrangement) throw new IvmRuntimeError("GraphNodeNotFound", inputNode);
    }
    const touchedGroups = groupBy(canonical.deltas, (d) =>
      encodedRecordKeyPart(inputDesc, d.record, collectBy.groupFieldIndices),
    );
    const readFields = collectBy.readFieldIndices();
    const output: RecordDelta[] = [];
    for (const [groupPrefix, groupDeltas] of touchedGroups) {
      let afterRecords: [Bytes, number][] = arrangement.value.recordsForKey(groupPrefix);
      let beforeRecords =
        this.context.arrangementUpdateMode === "Replace"
          ? []
          : recordsBeforeDeltas(afterRecords.slice(), groupDeltas);
      // Only the fields the collector reads are rebuilt (#3830).
      afterRecords = this.materializeArrangedRecords(inputDesc, afterRecords, readFields);
      beforeRecords = this.materializeArrangedRecords(inputDesc, beforeRecords, readFields);
      if (collectBy.mode === "Expand") {
        const before = collectByExpandedWindow(inputDesc, outputDesc, collectBy, beforeRecords);
        const after = collectByExpandedWindow(inputDesc, outputDesc, collectBy, afterRecords);
        const occurrences = new SortedSet<Bytes>(compareBytes, [
          ...before.keysArray(),
          ...after.keysArray(),
        ]);
        for (const occurrence of occurrences) {
          const b = before.get(occurrence);
          const a = after.get(occurrence);
          if (b && a && equalBytes(b, a)) continue;
          if (b) output.push({ record: b, weight: -1 });
          if (a) output.push({ record: a, weight: 1 });
          if (!a && !b)
            throw new Error(
              "internal error: entered unreachable code: occurrence came from a selected window",
            );
        }
        continue;
      }
      const render = (records: [Bytes, number][]) =>
        collectBy.mode === "Root"
          ? collectByRootFromRecords(inputDesc, outputDesc, collectBy, records)
          : slots.length === 0
            ? collectByParentFromRecords(inputDesc, outputDesc, collectBy, records)
            : collectByTreeParentFromRecords(inputDesc, outputDesc, collectBy, records);
      const before = render(beforeRecords);
      const after = render(afterRecords);
      if (before === after || (before && after && equalBytes(before, after))) continue;
      if (before) output.push({ record: before, weight: -1 });
      if (after) output.push({ record: after, weight: 1 });
    }
    return { descriptor: outputDesc, deltas: output };
  }

  updateAggregate(
    node: NodeId,
    aggregate: any,
    outputDesc: RecordDescriptor,
    input: RecordDeltas,
    canonical: RecordDeltas,
  ): RecordDeltas {
    const inputs: NodeId[] = this.graphNode(node).descriptor.inputs;
    if (inputs.length !== 1) throw new IvmRuntimeError("GraphInputArityMismatch", node);
    const inputNode = inputs[0];
    const inputDesc = input.descriptor;
    const groupFields = this.aggregateGroupFields(node, aggregate);
    const arrangementKey = this.arrangementKey(inputNode, inputDesc, groupFields, "Exact");
    if (input.deltas.length === 0) {
      // An ungrouped aggregate has one logical empty group, seeded once.
      const seed =
        aggregate.groupKey.length === 0 &&
        (this.context.evalMode === "Hydrate" || !this.arrangementStates.has(arrangementKey));
      if (seed) {
        const record = aggregateRowFromRecords(inputDesc, outputDesc, aggregate, []);
        if (!record) throw new IvmRuntimeError("UnsupportedOperator");
        return { descriptor: outputDesc, deltas: [{ record, weight: 1 }] };
      }
      return emptyDeltas(outputDesc);
    }
    if (this.context.evalMode === "Hydrate") {
      const groups = new SortedMap<Bytes, [Bytes, number][]>(compareBytes);
      for (const delta of input.deltas) {
        const key = encodedRecordKeyPart(inputDesc, delta.record, aggregate.groupFieldIndices);
        groups.getOrInsert(key, () => []).push([delta.record, delta.weight]);
      }
      const output: RecordDelta[] = [];
      for (const records of groups.valuesArray()) {
        const record = aggregateRowFromRecords(inputDesc, outputDesc, aggregate, records);
        if (record) output.push({ record, weight: 1 });
      }
      return { descriptor: outputDesc, deltas: output };
    }
    const touchedGroups = groupBy(canonical.deltas, (d) =>
      encodedRecordKeyPart(inputDesc, d.record, aggregate.groupFieldIndices),
    );
    const arrangement = this.arrangementStates.get(arrangementKey);
    if (!arrangement) throw new IvmRuntimeError("GraphNodeNotFound", inputNode);
    const fields = [...aggregate.groupFieldIndices];
    const expressions = aggregate.aggregates
      .map((e: any) => e.expression)
      .filter((e: any) => e !== undefined);
    for (const field of planExprFields(expressions)) {
      const index = resolveFieldName(inputDesc, field);
      if (index === undefined) throw new IvmRuntimeError("GraphFieldNotFound", field);
      fields.push(index);
    }
    const sortedFields = [...new Set(fields)].sort((a, b) => a - b);
    const output: RecordDelta[] = [];
    for (const [groupPrefix, groupDeltas] of touchedGroups) {
      const afterRaw: [Bytes, number][] = arrangement.value.recordsForKey(groupPrefix);
      const beforeRaw = recordsBeforeFromDeltas(afterRaw.slice(), groupDeltas);
      const after = aggregateRowFromRecords(
        inputDesc,
        outputDesc,
        aggregate,
        this.materializeArrangedRecords(inputDesc, afterRaw, sortedFields),
      );
      const before = aggregateRowFromRecords(
        inputDesc,
        outputDesc,
        aggregate,
        this.materializeArrangedRecords(inputDesc, beforeRaw, sortedFields),
      );
      if (before === after || (before && after && equalBytes(before, after))) continue;
      if (before) output.push({ record: before, weight: -1 });
      if (after) output.push({ record: after, weight: 1 });
    }
    return { descriptor: outputDesc, deltas: consolidateDeltas(output) };
  }

  arrangementNeedsIndex(node: NodeId): boolean {
    const arrange = this.graph.node(node);
    if (!arrange) return false;
    return arrange.children.some((child: NodeId) => {
      const op = this.graph.node(child)?.descriptor.operator;
      switch (op?.kind) {
        case "Join":
        case "SemiJoin":
        case "AntiJoin":
        case "Aggregate":
          return true;
        case "CollectBy":
          return op.value.mode !== "Root";
        default:
          return false;
      }
    });
  }

  arrangementKey(
    input: NodeId,
    descriptor: RecordDescriptor,
    fields: string[],
    comparison: string,
  ): ArrangementKey {
    const arrangement = this.graphNode(input);
    const op = arrangement.descriptor.operator;
    if (op.kind !== "Arrange") throw new IvmRuntimeError("UnsupportedOperator");
    const spec = op.value;
    if (
      !arrangement.descriptor.output.records().equals(descriptor) ||
      spec.fields.length !== fields.length ||
      spec.fields.some((f: string, i: number) => f !== fields[i]) ||
      spec.comparison !== comparison
    ) {
      throw new IvmRuntimeError("GraphOutputMismatch");
    }
    return { scope: this.context.scope, input };
  }

  private joinFieldNames(node: NodeId, join: any): [string[], string[]] {
    const meta = this.meta(node);
    meta.joinLeftFields ??= planExprNames(join.leftKey);
    meta.joinRightFields ??= planExprNames(join.rightKey);
    return [meta.joinLeftFields, meta.joinRightFields];
  }

  joinOutputProjection(
    node: NodeId,
    leftDescriptor: RecordDescriptor,
    rightDescriptor: RecordDescriptor,
    outputDescriptor: RecordDescriptor,
  ): any {
    const meta = this.meta(node);
    if (meta.joinOutput) return meta.joinOutput;
    const mapping = joinOutputMapping(leftDescriptor, rightDescriptor, outputDescriptor);
    meta.joinOutput = new PreparedRecordCopy(
      [leftDescriptor, rightDescriptor],
      outputDescriptor,
      mapping,
    );
    return meta.joinOutput;
  }

  aggregateGroupFields(node: NodeId, aggregate: any): string[] {
    const meta = this.meta(node);
    return (meta.aggregateGroupFields ??= planExprNames(aggregate.groupKey));
  }

  /** Root-scope arrangements represent table time, not recursive evaluator time. */
  arrangementSubTick(key: ArrangementKey): SubTick {
    return this.scopedSubTick(key.scope);
  }

  insertArrangement(key: ArrangementKey, state: any): void {
    let keys = this.arrangementKeysByInput.get(key.input);
    if (!keys)
      this.arrangementKeysByInput.set(
        key.input,
        (keys = new KeyedMap((k) => `${String(k.scope)}:${k.input}`)),
      );
    keys.set(key, true);
    this.arrangementStates.set(key, state);
  }

  private view(scope: Scope): GraphRuntimeView {
    return new GraphRuntimeView(this, scope);
  }

  async updateRecursive(
    node: NodeId,
    recursive: any,
    outputDesc: RecordDescriptor,
    seed: NodeId,
    step: NodeId,
    stepWitness: NodeId | undefined,
  ): Promise<RecordDeltas> {
    const storage = this.storage;
    if (!storage) throw new IvmRuntimeError("StorageUnavailable");
    const operatorKey = this.operatorKey(node);
    const inputGeneration = this.inputGeneration(node);
    const nodes: RecursiveNodes = { seed, step, stepWitness };
    if (this.context.evalMode === "Tick") {
      const existing = this.operatorStates.get(operatorKey);
      const state = existing?.kind === "Recursive" ? existing.state.value : undefined;
      const root = snapshotRequirement(
        this.graph,
        node,
        nodes,
        this.tableDeltas,
        this.bindingDeltas,
        state,
      );
      if (root !== undefined && this.evaluationInputs)
        requireSnapshotInputs(this.graph, this.evaluationInputs, root);
    }
    // Remove only this recursive node's state; child operator state stays available.
    const operator = this.takeOperator(node, operatorKey, "Recursive", () =>
      operatorStateFor({ kind: "Recursive" }),
    );
    const asOf = operator.state;
    const value: RecursiveState = asOf.value;
    if (this.context.evalMode === "Hydrate") {
      if (
        !value.hasPendingHydration() &&
        value.stepArrangementsHydrated() &&
        asOf.asOf === this.currentTick &&
        value.hydratedInputGeneration() === inputGeneration
      ) {
        const deltas = asOf.valueAt(this.currentTick).accumulatedDeltas();
        this.operatorStates.set(operatorKey, operator);
        return { descriptor: outputDesc, deltas };
      }
      const scope = this.context.scope.child(node);
      let accumulated: RecordDeltas;
      try {
        const progress = await resumeInputsHydrationRecompute(
          value,
          {
            schema: this.schema,
            graph: this.graph,
            variantProjections: this.variantProjections,
            inputs: this.evaluationInputs,
            tableDeltas: this.tableDeltas,
            storage,
            bindingSnapshots: this.bindingSnapshots,
            scope,
            inputGeneration,
          },
          node,
          recursive,
          outputDesc,
          nodes,
        );
        if (progress === "Yield") {
          this.operatorStates.set(operatorKey, operator);
          return cooperativeOperatorYield();
        }
        accumulated = value.pendingHydrationAccumulatedDeltas(outputDesc);
      } catch (error) {
        this.operatorStates.set(operatorKey, operator);
        throw error;
      }
      const runtime = this.view(scope);
      await hydrateRecursiveArrangements(runtime, recursive, step, structuredClone(accumulated));
      if (stepWitness !== undefined) {
        await hydrateRecursiveArrangements(runtime, recursive, stepWitness, {
          descriptor: outputDesc,
          deltas: value.accumulatedDeltas(),
        });
      }
      if (value.hasPendingHydration()) {
        const [next, witness] = value.finishHydrationRecompute();
        value.replaceWith(next);
        value.replaceStepWitnessWith(witness);
      }
      value.markStepArrangementsHydrated();
      value.markHydratedInputGeneration(inputGeneration);
      asOf.markForwardAsOf(this.currentTick);
      this.operatorStates.set(operatorKey, operator);
      return accumulated;
    }
    let deltas: RecordDelta[];
    try {
      const progress = await recursiveDelta(
        value,
        this.view(this.context.scope.child(node)),
        node,
        recursive,
        outputDesc,
        nodes,
      );
      if (progress === "Yield") {
        this.operatorStates.set(operatorKey, operator);
        return cooperativeOperatorYield();
      }
      deltas = progress.ready;
    } catch (error) {
      this.operatorStates.set(operatorKey, operator);
      throw error;
    }
    asOf.markForwardAsOf(this.currentTick);
    value.markHydratedInputGeneration(inputGeneration);
    this.operatorStates.set(operatorKey, operator);
    return { descriptor: outputDesc, deltas };
  }

  updateRecursiveStepWitness(recursiveNode: NodeId, outputDesc: RecordDescriptor): RecordDeltas {
    const state = this.operatorStates.get(this.operatorKey(recursiveNode));
    if (state?.kind !== "Recursive")
      throw new IvmRuntimeError("NodeStateOperatorMismatch", recursiveNode);
    const value: RecursiveState = state.state.value;
    const deltas =
      this.context.evalMode === "Hydrate"
        ? value.stepWitnessAccumulatedDeltas()
        : value.stepWitnessDeltas();
    return { descriptor: outputDesc, deltas };
  }

  private async updateUnaryInput(graphNode: any, node: NodeId): Promise<RecordDeltas> {
    const input = graphNode.descriptor.inputs[0];
    if (input === undefined) throw new IvmRuntimeError("GraphInputMissing", node);
    return this.updateNode(input);
  }

  private async updateStreamingChecksum(
    node: NodeId,
    checksum: any,
    outputDesc: RecordDescriptor,
    input: RecordDeltas,
  ): Promise<RecordDeltas> {
    const operatorKey = this.operatorKey(node);
    const operator = this.takeOperator(node, operatorKey, "StreamingChecksum", () =>
      operatorStateFor({ kind: "StreamingChecksum" }),
    );
    const state: StreamingChecksumOperatorState = operator.state;
    if (!state.pending || state.pending.input !== input) {
      state.pending = { input, nextDelta: 0, current: undefined, output: [] };
    }
    const pending = state.pending;
    while (pending.nextDelta < pending.input.deltas.length) {
      const delta = pending.input.deltas[pending.nextDelta];
      const values = pending.input.descriptor.borrow(delta.record).toValues();
      const value = values[checksum.fieldIdx];
      if (value === undefined)
        throw new IvmRuntimeError("GraphFieldIndexOutOfBounds", checksum.fieldIdx);
      let digest: Bytes | undefined;
      switch (value.kind) {
        case "String":
          digest = blake3(new TextEncoder().encode(value.value));
          break;
        case "Bytes":
          digest = blake3(value.value);
          break;
        case "Large": {
          pending.current ??= new largeValues.StreamingChecksum(
            value.value,
            checksum.windowBytes,
            checksum.maxBytesPerTurn,
          );
          const streaming = pending.current;
          if (streaming.cursor().remainingBytes() !== 0) {
            const range = streaming.cursor().nextRange();
            const inputs = this.evaluationInputs;
            if (!inputs) throw new IvmRuntimeError("EvaluationBlocked");
            inputs.setChunkScope(node);
            let bytes: Bytes;
            try {
              bytes = largeValues.byteRangeAttempt(streaming.cursor().value(), range, inputs);
            } catch (error: any) {
              if (error?.kind === "EvaluationBlocked") {
                this.operatorStates.set(operatorKey, operator);
              }
              throw error;
            } finally {
              inputs.setChunkScope(undefined);
            }
            const shouldYield = streaming.consumeWindow(bytes);
            inputs.releaseChunksOwnedBy(node);
            if (shouldYield) {
              streaming.recordYield();
              this.operatorStates.set(operatorKey, operator);
              return cooperativeOperatorYield();
            }
          }
          if (streaming.cursor().remainingBytes() === 0) {
            const completed = pending.current;
            pending.current = undefined;
            digest = completed.finish()[0];
          }
          break;
        }
        default:
          throw new IvmRuntimeError("StreamingChecksumTypeMismatch");
      }
      if (!digest) continue;
      values[checksum.fieldIdx] = { kind: "Bytes", value: digest };
      pending.output.push({ record: outputDesc.create(values), weight: delta.weight });
      pending.nextDelta++;
    }
    this.operatorStates.set(operatorKey, { kind: "StreamingChecksum", state: {} });
    return { descriptor: outputDesc, deltas: pending.output };
  }

  /** Index identity stays in producer representation; decode after reconstruction. */
  materializeArrangedRecords(
    descriptor: RecordDescriptor,
    records: [Bytes, number][],
    fields: number[] | undefined,
  ): [Bytes, number][] {
    if (!this.evaluationInputs || fields?.length === 0) return records;
    const input: RecordDeltas = {
      descriptor,
      deltas: records.map(([record, weight]) => ({ record, weight })),
    };
    const output = fields
      ? this.materializeIndirectFieldIndices(input, fields)
      : this.materializeIndirectInput(input);
    return output.deltas.map((d) => [d.record, d.weight]);
  }

  materializeIndirectInput(input: RecordDeltas): RecordDeltas {
    const inputs = this.evaluationInputs;
    if (!inputs) return input;
    let changed = false;
    const deltas = input.deltas.map((delta) => {
      const raw = largeValues.materializeRecordAttempt(input.descriptor, delta.record, inputs);
      changed ||= !equalBytes(raw, delta.record);
      return { record: raw, weight: delta.weight };
    });
    return changed ? { descriptor: input.descriptor, deltas } : input;
  }

  materializeIndirectFields(input: RecordDeltas, fields: Iterable<string>): RecordDeltas {
    const indices: number[] = [];
    for (const field of fields) {
      const index = resolveFieldName(input.descriptor, field);
      if (index === undefined) throw new IvmRuntimeError("GraphFieldNotFound", field);
      indices.push(index);
    }
    return this.materializeIndirectFieldIndices(input, indices);
  }

  materializeIndirectFieldIndices(input: RecordDeltas, indices: number[]): RecordDeltas {
    const inputs = this.evaluationInputs;
    if (!inputs) return input;
    const fields = input.descriptor.fields();
    let required = false;
    for (const index of indices) {
      const field = fields[index];
      if (!field) throw recordsError("FieldIndexOutOfBounds", { index, len: fields.length });
      required ||= field.valueType.mayContainStoredScalar();
    }
    if (!required) return input;
    let changed = false;
    const deltas = input.deltas.map((delta) => {
      const raw = largeValues.materializeRecordFieldsAttempt(
        input.descriptor,
        delta.record,
        indices,
        inputs,
      );
      changed ||= !equalBytes(raw, delta.record);
      return { record: raw, weight: delta.weight };
    });
    return changed ? { descriptor: input.descriptor, deltas } : input;
  }
}
