import { IvmRuntimeError } from "./index.js";
import type { NodeId, BindingSourceKey, FrontierName } from "../../../_extern/groove-ivm-tick.js";
import { recordDeltasDigest, hashValues, BSK } from "../../../_extern/groove-ivm-tick.js";
import type { RecordDeltas } from "./subscriptions.js";

export type ExecutionLayoutStats = { builds: number; hits: number };

export class RuntimeStats {
  graphNodes = 0;
  activeSubscriptions = 0;
  activePreparedShapes = 0;
  activeShapeParams = 0;
  arrangementCount = 0;
  evalMemoEntries = 0;
  evalMemoBytes = 0;
  hydrationMemoEntries = 0;
  hydrationMemoHits = 0;
  hydrationMemoComputes = 0;
  hydrationMemoDistinctComputedNodes = 0;
  arrangementRows = 0;
  arrangementEncodedBytes = 0;
  recursiveStateCount = 0;
  recursiveAccumulatedRows = 0;
  recursiveAccumulatedEncodedBytes = 0;
  logicalNodesRequested = 0;
  dedupedGraphNodes = 0;

  dedupeRatio(): number {
    if (this.logicalNodesRequested === 0) return 1.0;
    return this.dedupedGraphNodes / this.logicalNodesRequested;
  }
}

export class TickMetrics {
  tick = 0;
  tableDeltaRecords = 0;
  recordsProcessed = 0;
  nodesEvaluated = 0;
  subscriptionsConsidered = 0;
  rootOrderingPositionRecords = 0;
  rootOrderingPositionRecordsSkipped = 0;
  topByDeltaMembershipRecords = 0;
  recursiveRecomputes = 0;
  hydrationMemoHits = 0;
  hydrationMemoComputes = 0;
  hydrationMemoComputedNodes = new Set<NodeId>();
  notificationsSent = 0;
  notificationRecords = 0;
  notificationEncodedBytes = 0;
  runtimeStats = new RuntimeStats();
}

/** Interned recursive scope path; identity equality is structural equality. */
export class ScopeId {
  private static readonly table = new Map<string, ScopeId>();
  private static readonly ROOT = new ScopeId([], "");
  private constructor(
    readonly path: readonly NodeId[],
    readonly key: string,
  ) {}
  static root(): ScopeId {
    return ScopeId.ROOT;
  }
  child(recursiveNode: NodeId): ScopeId {
    const path = [...this.path, recursiveNode];
    const key = path.join(",");
    let hit = ScopeId.table.get(key);
    if (!hit) ScopeId.table.set(key, (hit = new ScopeId(path, key)));
    return hit;
  }
  /** The outermost recursive node whose body this scope belongs to. */
  outermost(): NodeId | undefined {
    return this.path[0];
  }
}

export type OperatorStateKey = { scope: ScopeId; node: NodeId };
export const operatorStateKeyString = (k: OperatorStateKey) => `${k.scope.key}/${k.node}`;
export const rootStateKey = (node: NodeId): OperatorStateKey => ({ scope: ScopeId.root(), node });

export type ArrangementKey = { scope: ScopeId; input: NodeId };
export const arrangementKeyString = (k: ArrangementKey) => `${k.scope.key}/${k.input}`;

export type SubTick = { tick: number; subTick: number };
export type Tick = number;

function compareTime(a: SubTick | Tick, b: SubTick | Tick): number {
  if (typeof a === "number") return a - (b as number);
  const bb = b as SubTick;
  return a.tick - bb.tick || a.subTick - bb.subTick;
}
function formatTime(t: SubTick | Tick): string {
  return typeof t === "number"
    ? `Tick(${t})`
    : `SubTick { tick: ${t.tick}, sub_tick: ${t.subTick} }`;
}

/** Runtime state together with the logical time its contents reflect. */
export class AsOf<T, S extends SubTick | Tick> {
  asOf: S | undefined = undefined;
  constructor(public value: T) {}
  valueAt(expected: S): T {
    if (this.asOf !== undefined && compareTime(this.asOf, expected) === 0) return this.value;
    throw IvmRuntimeError.of("StaleRuntimeState", {
      expected: formatTime(expected),
      actual: this.asOf === undefined ? undefined : formatTime(this.asOf),
    });
  }
  markForwardAsOf(next: S): void {
    if (this.asOf !== undefined && compareTime(this.asOf, next) > 0) {
      throw IvmRuntimeError.of("OutOfOrderRuntimeState", {
        current: formatTime(this.asOf),
        next: formatTime(next),
      });
    }
    this.asOf = next;
  }
  replaceAsOfAtLeast(next: S): void {
    if (this.asOf === undefined || compareTime(this.asOf, next) <= 0) this.asOf = next;
  }
  clone(cloneValue: (v: T) => T): AsOf<T, S> {
    const out = new AsOf<T, S>(cloneValue(this.value));
    out.asOf = this.asOf;
    return out;
  }
}

export enum ArrangementUpdateMode {
  Accumulate,
  Replace,
}
export enum EvalMode {
  Tick,
  Hydrate,
}
export enum HydrationMode {
  Ordinary,
  Subscription,
}

export type EvalMemoKey = {
  scope: ScopeId;
  node: NodeId;
  inputSignatureHash: bigint;
  tickEpoch: number | undefined;
  subTick: number;
  contextDigest: bigint;
};
export const evalMemoKeyString = (k: EvalMemoKey) =>
  `${k.scope.key}/${k.node}/${k.inputSignatureHash}/${k.tickEpoch ?? "-"}/${k.subTick}/${k.contextDigest}`;

export class EvalMemoEntry {
  constructor(
    readonly records: RecordDeltas,
    readonly inputWatermark: number,
    readonly payloadBytes: number,
    public lastUsed: number,
  ) {}
}

export class NodeInputSignature {
  constructor(
    readonly tables: readonly string[],
    readonly bindings: readonly BindingSourceKey[],
    readonly frontierBindings: readonly FrontierName[],
    readonly hash: bigint,
  ) {}
  static fromSets(
    tables: Iterable<string>,
    bindings: Iterable<BindingSourceKey>,
    frontierBindings: Iterable<FrontierName>,
  ): NodeInputSignature {
    const t = [...tables].sort();
    const b = [...bindings].sort((x, y) => {
      const a = BSK.key(x);
      const c = BSK.key(y);
      return a < c ? -1 : a > c ? 1 : 0;
    });
    const f = [...frontierBindings].sort();
    return new NodeInputSignature(t, b, f, hashValues([t, b.map(BSK.key), f]));
  }
}

/** Current scoped inputs and logical time for node evaluation. */
export class EvalContext {
  scope = ScopeId.root();
  subTick = 0;
  bindings = new Map<FrontierName, RecordDeltas>();
  bindingDigests = new Map<FrontierName, bigint>();
  arrangementUpdateMode = ArrangementUpdateMode.Accumulate;
  evalMode = EvalMode.Tick;
  hydrateArrangements = false;

  static root(): EvalContext {
    return new EvalContext();
  }
  static rootSnapshot(): EvalContext {
    const ctx = new EvalContext();
    ctx.arrangementUpdateMode = ArrangementUpdateMode.Replace;
    ctx.evalMode = EvalMode.Hydrate;
    return ctx;
  }
  static rootSubscriptionSnapshot(): EvalContext {
    const ctx = EvalContext.rootSnapshot();
    ctx.hydrateArrangements = true;
    return ctx;
  }
  static withBinding(
    scope: ScopeId,
    subTick: number,
    binding: FrontierName,
    deltas: RecordDeltas,
  ): EvalContext {
    return EvalContext.withBindingAndArrangementMode(
      scope,
      subTick,
      binding,
      deltas,
      ArrangementUpdateMode.Accumulate,
    );
  }
  static withBindingAndArrangementMode(
    scope: ScopeId,
    subTick: number,
    binding: FrontierName,
    deltas: RecordDeltas,
    arrangementUpdateMode: ArrangementUpdateMode,
  ): EvalContext {
    const ctx = new EvalContext();
    ctx.scope = scope;
    ctx.subTick = subTick;
    ctx.bindingDigests.set(binding, recordDeltasDigest(deltas));
    ctx.bindings.set(binding, deltas);
    ctx.arrangementUpdateMode = arrangementUpdateMode;
    return ctx;
  }
}
