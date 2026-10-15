// Prepared shapes, routed bindings, and subscription delivery state.
import {
  type Bytes,
  KeyedMap,
  SortedMap,
  SortedSet,
  cmpStr,
  cmpBytes,
  toHex,
  equalBytes,
  compareBytes,
} from "../../../support/index.js";
import { IvmRuntimeError, DEFAULT_SINK, type IvmRuntime } from "./index.js";
import { TerminalDeltas } from "./terminal.js";
import { TickMetrics } from "./state.js";
import {
  type NodeId,
  type RecordDescriptor,
  type GraphBuilder,
  type Value,
  type LiteralValue,
  type PredicateExpr,
  type BindingSourceKey,
  type InputSourceId,
  type Retainer,
  type OrderedKvStorage,
  type ValueComparison,
  BorrowedRecord,
  OwnedStorage,
  RecordStore,
  BSK,
  FR,
  EphemeralGraphInstall,
  addDedupGraph,
  addRetainer,
  removeRetainer,
  resolveFieldRef,
  literalToValue,
  largeValues,
  compareRecordField,
  compareRecordFields,
  containsRecordField,
  containsRecordFieldValue,
  isSqlNullValue,
  recordValue,
  descriptorsEqual,
  isRoutableType,
  encodeRouteKey,
  outputConsumesRootPositions,
  removeMultisinkRetainers,
  collectUnretainedEphemeralNodes,
  addDedupSchemaIndex,
  builderContainsRecursive,
  literalValueType,
  newRecordDescriptor,
  builderPostorder,
  indexRecordDescriptor,
  collectByRootDescriptor,
  collectByDescriptor,
  collectByTreeDescriptor,
  collectByExpandDescriptor,
  aggregateDescriptor,
  unwrapNullableDescriptor,
  unnestDescriptor,
  variantProjectDescriptor,
  projectionPlan,
  joinDescriptor,
} from "../../../_extern/groove-ivm-tick.js";
import {
  autoDirectBindingField,
  boundRoutedMultisinkGraph,
  builderContainsBindingSource,
  countBuilderNodes,
  descriptorFieldNames,
  graphBuilderFingerprint,
  graphBuildersEqual,
  liftLiteralFilter,
  replaceBindingShape,
  validatePublicOutputFields,
} from "./subscriptions_builder.js";
import {
  PENDING,
  type Poll,
  type Waker,
  noopWaker,
  bumpInputFrontiers,
  drivePendingIncremental,
  enqueueSubscriptionHydration,
  flushPendingBindingRetractions,
  hasResidentContinuation,
  pollPendingIncremental,
  pollResidentIncremental,
  retainOwnerProgressWaker,
  tickBindingsDetachingCold,
  tickWithParams,
  cancelPendingSubscriptionHydration,
  hasPendingIncremental,
} from "./runtime_tick.js";

export enum SubscriptionLifetime {
  /** Publish one complete initial result, then release the consumer. */
  FirstResult,
  /** Hydrate the indexes needed to maintain subsequent committed changes. */
  Retained,
}

/** How indirect (large) scalar values appear in a subscription's root output. */
export type RootIndirectValues =
  | { kind: "Materialize" }
  | { kind: "Physical" }
  | { kind: "PhysicalFields"; fields: ReadonlySet<string> };

export const MATERIALIZE: RootIndirectValues = { kind: "Materialize" };

/** Top-level field indices that must be materialized, or undefined for all. */
export function materializedFieldIndices(
  r: RootIndirectValues,
  descriptor: RecordDescriptor,
): number[] | undefined {
  switch (r.kind) {
    case "Materialize":
      return undefined;
    case "Physical":
      return [];
    case "PhysicalFields": {
      const out: number[] = [];
      descriptor.fields().forEach((field: any, index: number) => {
        // Match the storage name only.
        if (!(field.name != null && r.fields.has(field.name))) out.push(index);
      });
      return out;
    }
  }
}

export function rootIndirectValuesEqual(a: RootIndirectValues, b: RootIndirectValues): boolean {
  if (a.kind !== b.kind) return false;
  if (a.kind !== "PhysicalFields") return true;
  const bf = (b as any).fields as ReadonlySet<string>;
  return a.fields.size === bf.size && [...a.fields].every((f) => bf.has(f));
}

/** Runtime-scoped subscription handle: "<namespace>:<sequence>". */
export type SubscriptionId = string;
export const subscriptionRetainerKey = (id: SubscriptionId) => id.slice(id.indexOf(":") + 1);
function lifetimeRetainer(lifetime: SubscriptionLifetime, id: SubscriptionId): Retainer {
  return lifetime === SubscriptionLifetime.FirstResult
    ? { kind: "Hydration", key: subscriptionRetainerKey(id) }
    : { kind: "Subscription", key: subscriptionRetainerKey(id) };
}

export type PublicationId = number;
export type PreparedShapeId = number;
export type PreparedShape = { id: PreparedShapeId };

export type PublicationUpdate<T> = { publication: PublicationId | undefined; deltas: T };
export type SubscriptionEvent<T> =
  | { kind: "Update"; update: PublicationUpdate<T> }
  | { kind: "Error"; error: SubscriptionError };

export class SubscriptionError extends Error {
  constructor(readonly source: IvmRuntimeError | undefined) {
    super(source ? `subscription evaluation failed: ${source.message}` : "subscription ended");
  }
  static ended(): SubscriptionError {
    return new SubscriptionError(undefined);
  }
  sourceError(): IvmRuntimeError | undefined {
    return this.source;
  }
}

/** One prepared multisink terminal. */
export class RoutedMultisinkTerminal {
  routeValueIndices: number[];
  constructor(
    public sink: string,
    public graph: GraphBuilder,
    public routeFields: string[],
    public publicFields: string[],
  ) {
    this.routeValueIndices = routeFields.map((_, i) => i);
  }
  withRouteValueIndices(indices: number[]): this {
    this.routeValueIndices = indices;
    return this;
  }
}

function routedTerminalsEqual(a: RoutedMultisinkTerminal, b: RoutedMultisinkTerminal): boolean {
  return (
    a.sink === b.sink &&
    graphBuildersEqual(a.graph, b.graph) &&
    a.routeFields.join("\0") === b.routeFields.join("\0") &&
    a.routeValueIndices.join(",") === b.routeValueIndices.join(",") &&
    a.publicFields.join("\0") === b.publicFields.join("\0")
  );
}

/** Deltas grouped by named output sink for one multisink graph subscription. */
export class MultisinkDeltas {
  constructor(
    public sinks = new SortedMap<string, RecordDeltas>(cmpStr),
    public terminalSinks = new SortedMap<string, TerminalDeltas>(cmpStr),
  ) {}
  isEmpty(): boolean {
    return (
      this.sinks.valuesArray().every((d) => d.deltas.length === 0) &&
      this.terminalSinks.valuesArray().every((t) => t.isEmpty())
    );
  }
  get(sink: string): RecordDeltas | undefined {
    return this.sinks.get(sink);
  }
}

export type QueuedMultisinkDeltas = {
  deltas: MultisinkDeltas;
  publication: PublicationId | undefined;
};
export const queuedDeltas = (deltas: MultisinkDeltas): QueuedMultisinkDeltas => ({
  deltas,
  publication: undefined,
});

type QueueItem =
  | { ok: true; queued: QueuedMultisinkDeltas }
  | { ok: false; error: SubscriptionError };

/** Shared channel state between a subscription's sender and receiver. */
class SubscriptionChannel {
  items: QueueItem[] = [];
  senderOpen = true;
  receiverAlive = true;
  waiter: Waker | undefined;
}

export class InitialSlot {
  value: MultisinkDeltas | undefined;
  take(): MultisinkDeltas | undefined {
    const v = this.value;
    this.value = undefined;
    return v;
  }
}

/** Receiving end of a live multisink graph subscription. */
export class MultisinkSubscription {
  constructor(
    readonly id: SubscriptionId,
    readonly initial: InitialSlot,
    private readonly channel: SubscriptionChannel,
  ) {}

  /** Dropping the receiver: later sends fail and the runtime prunes it. */
  close(): void {
    this.channel.receiverAlive = false;
  }
  takeInitial(): MultisinkDeltas | undefined {
    return this.initial.take();
  }
  tryRecvWithPublication(): PublicationUpdate<MultisinkDeltas> | undefined {
    const initial = this.takeInitial();
    if (initial) return { publication: undefined, deltas: initial };
    const item = this.channel.items.shift();
    if (!item) {
      if (!this.channel.senderOpen) throw SubscriptionError.ended();
      return undefined;
    }
    if (!item.ok) throw item.error;
    return { publication: item.queued.publication, deltas: item.queued.deltas };
  }
  tryRecv(): MultisinkDeltas | undefined {
    return this.tryRecvWithPublication()?.deltas;
  }
  pollNextEvent(waker: Waker): Poll<SubscriptionEvent<MultisinkDeltas>> {
    const initial = this.takeInitial();
    if (initial) return { kind: "Update", update: { publication: undefined, deltas: initial } };
    const next = (): SubscriptionEvent<MultisinkDeltas> | undefined => {
      const item = this.channel.items.shift();
      if (item) {
        return item.ok
          ? {
              kind: "Update",
              update: { publication: item.queued.publication, deltas: item.queued.deltas },
            }
          : { kind: "Error", error: item.error };
      }
      if (!this.channel.senderOpen) return { kind: "Error", error: SubscriptionError.ended() };
      return undefined;
    };
    const first = next();
    if (first) return first;
    this.channel.waiter = waker;
    return next() ?? PENDING;
  }
  pollNextWithPublication(waker: Waker): Poll<PublicationUpdate<MultisinkDeltas>> {
    const event = this.pollNextEvent(waker);
    if (event === PENDING) return PENDING;
    if (event.kind === "Error") throw event.error;
    return event.update;
  }
  /** Await the next update (the blocking `recv` of the Rust API). */
  recvWithPublication(): Promise<PublicationUpdate<MultisinkDeltas>> {
    return new Promise((resolve, reject) => {
      const attempt = () => {
        try {
          const r = this.pollNextWithPublication(attempt);
          if (r !== PENDING) resolve(r);
        } catch (e) {
          reject(e);
        }
      };
      attempt();
    });
  }
  async recv(): Promise<MultisinkDeltas> {
    return (await this.recvWithPublication()).deltas;
  }
}

/** One-sink convenience wrapper around MultisinkSubscription. */
export class Subscription {
  constructor(
    readonly inner: MultisinkSubscription,
    readonly sink: string,
    readonly output: RecordDescriptor,
  ) {}
  get id(): SubscriptionId {
    return this.inner.id;
  }
  private extract(deltas: MultisinkDeltas): RecordDeltas {
    return deltas.sinks.get(this.sink) ?? emptyRecordDeltas(this.output);
  }
  takeInitial(): RecordDeltas | undefined {
    const initial = this.inner.takeInitial();
    return initial && this.extract(initial);
  }
  tryRecv(): RecordDeltas | undefined {
    const update = this.inner.tryRecvWithPublication();
    return update && this.extract(update.deltas);
  }
  tryRecvWithPublication(): PublicationUpdate<RecordDeltas> | undefined {
    const update = this.inner.tryRecvWithPublication();
    return update && { publication: update.publication, deltas: this.extract(update.deltas) };
  }
  pollNextEvent(waker: Waker): Poll<SubscriptionEvent<RecordDeltas>> {
    const event = this.inner.pollNextEvent(waker);
    if (event === PENDING || event.kind === "Error") return event;
    return {
      kind: "Update",
      update: { publication: event.update.publication, deltas: this.extract(event.update.deltas) },
    };
  }
  pollNextWithPublication(waker: Waker): Poll<PublicationUpdate<RecordDeltas>> {
    const r = this.inner.pollNextWithPublication(waker);
    return r === PENDING ? r : { publication: r.publication, deltas: this.extract(r.deltas) };
  }
  async recvWithPublication(): Promise<PublicationUpdate<RecordDeltas>> {
    const u = await this.inner.recvWithPublication();
    return { publication: u.publication, deltas: this.extract(u.deltas) };
  }
  async recv(): Promise<RecordDeltas> {
    return (await this.recvWithPublication()).deltas;
  }
}

// ---- predicate evaluation (impl PredicateExpr) ----

const ORDERING_PREDICATES: Record<string, (ord: number) => boolean> = {
  Eq: (o) => o === 0,
  Neq: (o) => o !== 0,
  Gt: (o) => o > 0,
  GtEq: (o) => o >= 0,
  Lt: (o) => o < 0,
  LtEq: (o) => o <= 0,
};

export function supportsIndirectLiteralAttempt(p: PredicateExpr): boolean {
  return p.kind in ORDERING_PREDICATES;
}

const unwrapNullable = (v: Value): Value => (v.kind === "Nullable" && v.v !== null ? v.v : v);

export function matchesIndirectLiteralAttempt(
  p: PredicateExpr,
  record: any,
  inputs: any,
): boolean | undefined {
  const predicate = ORDERING_PREDICATES[p.kind];
  if (!predicate) return undefined;
  const actual = unwrapNullable(recordValue(record, (p as any).field));
  if (actual.kind !== "Large") return undefined;
  const large: any = actual.v;
  const literal = unwrapNullable(literalToValue((p as any).value));
  let inline: Bytes;
  if (large.kind === "Bytes" && literal.kind === "Bytes") inline = literal.v;
  else if ((large.kind === "String" || large.kind === "Json") && literal.kind === "String")
    inline = new TextEncoder().encode(literal.v);
  else return false;
  return predicate(largeValues.compareInlineAttempt(large, inline, inputs));
}

export function referencedFields(p: PredicateExpr, output: Set<string>): void {
  const q: any = p;
  switch (q.kind) {
    case "TemplateArgument":
      for (const f of q.fields) output.add(f);
      return;
    case "EqField":
    case "NeqField":
      output.add(q.field);
      output.add(q.valueField);
      return;
    case "ContainsField":
      output.add(q.field);
      output.add(q.needleField);
      return;
    case "And":
    case "Or":
      for (const child of q.predicates) referencedFields(child, output);
      return;
    default:
      output.add(q.field);
  }
}

export function predicateMatches(
  p: PredicateExpr,
  record: any,
  comparison: ValueComparison,
): boolean {
  const q: any = p;
  switch (q.kind) {
    case "TemplateArgument":
      throw IvmRuntimeError.of("UnsupportedOperator");
    case "Eq":
    case "Neq":
    case "Gt":
    case "GtEq":
    case "Lt":
    case "LtEq":
      return compareRecordField(record, q.field, q.value, ORDERING_PREDICATES[q.kind], comparison);
    case "Contains":
      return containsRecordField(record, q.field, q.value, comparison);
    case "EqField":
      return compareRecordFields(record, q.field, q.valueField, (o: number) => o === 0, comparison);
    case "NeqField":
      return compareRecordFields(record, q.field, q.valueField, (o: number) => o !== 0, comparison);
    case "ContainsField":
      return containsRecordFieldValue(record, q.field, q.needleField, comparison);
    case "IsNull":
      return isSqlNullValue(recordValue(record, q.field));
    case "IsNotNull":
      return !isSqlNullValue(recordValue(record, q.field));
    case "EnumMatch": {
      const value = unwrapNullable(recordValue(record, q.field));
      // Wrong arms and NULL never match (fail-closed).
      if (value.kind === "Enum" && value.v.tag() === q.caseTag) {
        return predicateMatches(q.payload, value.v.record().borrowed(), comparison);
      }
      return false;
    }
    case "And":
      return q.predicates.every((c: PredicateExpr) => predicateMatches(c, record, comparison));
    case "Or":
      return q.predicates.some((c: PredicateExpr) => predicateMatches(c, record, comparison));
  }
  throw IvmRuntimeError.of("UnsupportedOperator");
}

// ---- deltas ----

/** Deltas for one base table in a committed batch. */
export type TableDelta = {
  table: string;
  variantTag: number;
  descriptor: RecordDescriptor;
  deltas: RecordDelta[];
};
/** Weighted change to one encoded record. */
export type RecordDelta = { record: Bytes; weight: number };
/** Descriptor plus a batch of weighted encoded record changes. */
export type RecordDeltas = { descriptor: RecordDescriptor; deltas: RecordDelta[] };

export const emptyRecordDeltas = (descriptor: RecordDescriptor): RecordDeltas => ({
  descriptor,
  deltas: [],
});
export const borrowedDelta = (delta: RecordDelta, descriptor: RecordDescriptor) =>
  new BorrowedRecord(delta.record, descriptor);
export function recordDeltasToValues(deltas: RecordDeltas): [Value[], number][] {
  return deltas.deltas.map((d) => [borrowedDelta(d, deltas.descriptor).toValues(), d.weight]);
}
export const recordDeltasEncodedBytes = (deltas: RecordDeltas) =>
  deltas.deltas.reduce((n, d) => n + d.record.length, 0);
export const multisinkDeltasRecordCount = (deltas: MultisinkDeltas) =>
  deltas.sinks.valuesArray().reduce((n, r) => n + r.deltas.length, 0);
export const multisinkDeltasEncodedBytes = (deltas: MultisinkDeltas) =>
  deltas.sinks.valuesArray().reduce((n, r) => n + recordDeltasEncodedBytes(r), 0);

export function recordStoreForTable(
  storage: OrderedKvStorage,
  table: { name: string },
  descriptor: RecordDescriptor,
) {
  return new RecordStore(storage, table.name, descriptor);
}

// ---- runtime subscription state ----

export class SubscriptionSender {
  constructor(private readonly channel: SubscriptionChannel) {}
  /** Returns false when the receiver is gone. */
  send(queued: QueuedMultisinkDeltas): boolean {
    if (!this.channel.senderOpen || !this.channel.receiverAlive) return false;
    this.channel.items.push({ ok: true, queued });
    this.wake();
    return true;
  }
  fail(error: SubscriptionError): void {
    if (this.channel.senderOpen) {
      this.channel.senderOpen = false;
      this.channel.items.push({ ok: false, error });
    }
    this.wake();
  }
  receiverAlive(): boolean {
    return this.channel.receiverAlive;
  }
  private wake() {
    const waiter = this.channel.waiter;
    this.channel.waiter = undefined;
    waiter?.();
  }
}

export type CompiledNode = {
  output: RecordDescriptor;
  node: NodeId;
  rootOrderingNode: NodeId | undefined;
};

export type MultisinkSubscriptionTarget =
  | { kind: "Direct" }
  | {
      kind: "RoutedShape";
      shapeId: PreparedShapeId;
      bindingKey: BindingKey;
      routeBarriers: NodeId[];
    };

export type MultisinkSubscriptionState = {
  sender: SubscriptionSender;
  outputs: SortedMap<string, CompiledNode>;
  target: MultisinkSubscriptionTarget;
  failed: boolean;
  rootIndirectValues: RootIndirectValues;
};

export type RoutedMultisinkTerminalState = {
  terminal: RoutedMultisinkTerminal;
  output: CompiledNode;
};

export type SharedShapeKey = { shape: string; terminals: RoutedMultisinkTerminal[] };
const sharedShapeKeyString = (k: SharedShapeKey) =>
  `${k.shape}\0${k.terminals.map((t) => `${t.sink}:${graphBuilderFingerprint(t.graph)}:${t.routeFields}:${t.routeValueIndices}:${t.publicFields}`).join("|")}`;

export type RoutedMultisinkShapeState = {
  shape: string;
  bindingDescriptor: RecordDescriptor;
  terminals: SortedMap<string, RoutedMultisinkTerminalState>;
  autoFamilyKey: AutoDirectFamilyKey | undefined;
  sharedKey: SharedShapeKey | undefined;
  retireWhenUnbound: boolean;
};

/** Encoded binding record; maps key it by hex. */
export type BindingKey = Bytes;
const bk = (b: BindingKey) => toHex(b);

/** A new binding already admitted into a live prepared shape's shared graph. */
export type LiveAttach = {
  bindingSource: BindingSourceKey;
  bindingKey: BindingKey;
  borrowed: Set<NodeId>;
};

type PreparedPublicOutput =
  | { kind: "Fields"; fields: Map<string, string[]> }
  | { kind: "OneSink"; output: RecordDescriptor };

export type AutoDirectFamilyKey = {
  graph: GraphBuilder;
  bindingDescriptor: RecordDescriptor;
  bindingField: string;
  publicFields: string[];
};
/** KeyedMap key: fingerprint plus the scalar parts; equality is checked on hit. */
export function autoDirectFamilyKeyHash(k: AutoDirectFamilyKey): string {
  return `${graphBuilderFingerprint(k.graph)}\0${k.bindingDescriptor.key()}\0${k.bindingField}\0${k.publicFields.join("\0")}`;
}
function autoDirectFamilyKeysEqual(a: AutoDirectFamilyKey, b: AutoDirectFamilyKey): boolean {
  return (
    descriptorsEqual(a.bindingDescriptor, b.bindingDescriptor) &&
    a.bindingField === b.bindingField &&
    a.publicFields.join("\0") === b.publicFields.join("\0") &&
    graphBuildersEqual(a.graph, b.graph)
  );
}

type AutoDirectFamilyPlan = {
  key: AutoDirectFamilyKey;
  graph: GraphBuilder;
  shape: string;
  bindingDescriptor: RecordDescriptor;
  bindingField: string;
  bindingValue: Value;
  publicFields: string[];
};

/** Point-in-time binding multisets, one shared snapshot per binding source. */
export type BindingSnapshots = KeyedMap<BindingSourceKey, RecordDeltas>;

export type BindingSourceState = {
  descriptor: RecordDescriptor;
  refcounts: Map<string, [BindingKey, number]>;
  /** Runtime-owned inputs acknowledge their first replacement even when empty. */
  initialized: boolean;
};

export type BindingDelta = {
  key: BindingSourceKey;
  descriptor: RecordDescriptor;
  deltas: RecordDelta[];
  initializesSnapshot: boolean;
};

/** Binding source states plus a lazily maintained snapshot of their bindings. */
export class BindingSources {
  private states = new KeyedMap<BindingSourceKey, BindingSourceState>(BSK.key);
  private snapshotCache: BindingSnapshots = new KeyedMap(BSK.key);
  private dirty = new KeyedMap<BindingSourceKey, BindingSourceKey>(BSK.key);

  get(key: BindingSourceKey): BindingSourceState | undefined {
    return this.states.get(key);
  }
  containsKey(key: BindingSourceKey): boolean {
    return this.states.has(key);
  }
  values(): IterableIterator<BindingSourceState> {
    return this.states.values();
  }
  getMut(key: BindingSourceKey): BindingSourceState | undefined {
    const state = this.states.get(key);
    if (state) this.dirty.set(key, key);
    return state;
  }
  insert(key: BindingSourceKey, state: BindingSourceState): void {
    this.dirty.set(key, key);
    this.states.set(key, state);
  }
  remove(key: BindingSourceKey): BindingSourceState | undefined {
    const removed = this.states.get(key);
    if (!removed) return undefined;
    this.states.delete(key);
    this.dirty.set(key, key);
    return removed;
  }
  /** Current bindings of every source, as positive-weight record deltas. */
  snapshot(): BindingSnapshots {
    if (this.dirty.size) {
      const next = new KeyedMap<BindingSourceKey, RecordDeltas>(BSK.key);
      for (const [k, v] of this.snapshotCache) next.set(k, v);
      for (const [key] of this.dirty) {
        const source = this.states.get(key);
        if (source) next.set(key, BindingSources.sourceSnapshot(source));
        else next.delete(key);
      }
      this.dirty = new KeyedMap(BSK.key);
      this.snapshotCache = next;
    }
    return this.snapshotCache;
  }
  private static sourceSnapshot(source: BindingSourceState): RecordDeltas {
    return {
      descriptor: source.descriptor,
      deltas: [...source.refcounts.values()].map(([record]) => ({ record, weight: 1 })),
    };
  }
}

/** One exact replacement in a runtime-owned mutable input source. */
export type InputSourceReplacement = {
  id: InputSourceId;
  descriptor: RecordDescriptor;
  records: Bytes[];
};
/** One atomic, set-like change to a runtime-owned mutable input source. */
export type InputSourceDelta = {
  id: InputSourceId;
  descriptor: RecordDescriptor;
  adds: Bytes[];
  removes: Bytes[];
};

enum InputSourceTickMode {
  Complete,
  DetachCold,
}

// ---- runtime-owned input sources ----

function preflightInputSource(
  rt: IvmRuntime,
  id: InputSourceId,
  descriptor: RecordDescriptor,
): BindingSourceKey {
  if (!id.belongsTo(rt.inputSourceRuntimeNamespace)) throw IvmRuntimeError.of("ForeignInputSource");
  const key = id.bindingKey();
  const source = rt.bindingSources.get(key);
  if (!source) {
    throw IvmRuntimeError.of(
      id.wasAllocatedBy(rt.inputSourceRuntimeNamespace, rt.nextInputSourceId)
        ? "InputSourceRetired"
        : "ForeignInputSource",
    );
  }
  if (!descriptorsEqual(source.descriptor, descriptor)) {
    throw IvmRuntimeError.of("BindingSourceDescriptorMismatch", id.diagnosticName());
  }
  return key;
}

function validateRecords(records: Iterable<Bytes>, descriptor: RecordDescriptor): void {
  for (const record of records)
    IvmRuntimeError.wrapRecord(() => new BorrowedRecord(record, descriptor).toValues());
}

/** Atomically apply record-level set changes to runtime-owned inputs. */
export function applyInputSourceDeltas(
  rt: IvmRuntime,
  inputDeltas: Iterable<InputSourceDelta>,
  storage: OrderedKvStorage,
) {
  return applyInputSourceDeltasInMode(rt, inputDeltas, storage, InputSourceTickMode.Complete);
}
export function applyInputSourceDeltasDetachingCold(
  rt: IvmRuntime,
  inputDeltas: Iterable<InputSourceDelta>,
  storage: OrderedKvStorage,
) {
  return applyInputSourceDeltasInMode(rt, inputDeltas, storage, InputSourceTickMode.DetachCold);
}

type Canonical = {
  id: InputSourceId;
  descriptor: RecordDescriptor;
  adds: SortedSet<Bytes>;
  removes: SortedSet<Bytes>;
};

async function applyInputSourceDeltasInMode(
  rt: IvmRuntime,
  inputDeltas: Iterable<InputSourceDelta>,
  storage: OrderedKvStorage,
  mode: InputSourceTickMode,
): Promise<TickMetrics> {
  const canonical = new SortedMap<string, Canonical>(cmpStr);
  for (const delta of inputDeltas) {
    const adds = new SortedSet<Bytes>(cmpBytes, delta.adds);
    const removes = new SortedSet<Bytes>(cmpBytes, delta.removes);
    validateRecords([...adds, ...removes], delta.descriptor);
    const k = delta.id.sortKey();
    const existing = canonical.get(k);
    if (!existing) {
      canonical.set(k, { id: delta.id, descriptor: delta.descriptor, adds, removes });
      continue;
    }
    if (!descriptorsEqual(existing.descriptor, delta.descriptor)) {
      throw IvmRuntimeError.of("BindingSourceDescriptorMismatch", existing.id.diagnosticName());
    }
    for (const r of adds) existing.adds.add(r);
    for (const r of removes) existing.removes.add(r);
  }
  for (const [, c] of canonical) preflightInputSource(rt, c.id, c.descriptor);

  const deltas: BindingDelta[] = [];
  for (const [, { id, descriptor, adds, removes }] of canonical) {
    const key = id.bindingKey();
    const source = rt.bindingSources.getMut(key)!;
    const cancelled = new SortedSet<Bytes>(
      cmpBytes,
      [...adds].filter((r) => removes.has(r)),
    );
    const removals = [...removes].filter((r) => !cancelled.has(r) && source.refcounts.has(bk(r)));
    const additions = [...adds].filter((r) => !cancelled.has(r) && !source.refcounts.has(bk(r)));
    const records: RecordDelta[] = [
      ...removals.map((record) => ({ record, weight: -1 })),
      ...additions.map((record) => ({ record, weight: 1 })),
    ];
    if (!records.length) continue;
    for (const r of removals) source.refcounts.delete(bk(r));
    for (const r of additions) source.refcounts.set(bk(r), [r, 1]);
    deltas.push({ key, descriptor, deltas: records, initializesSnapshot: false });
  }
  if (!deltas.length) return new TickMetrics();
  return tickInputSources(rt, deltas, storage, mode);
}

/** Atomically replace the complete record multisets of runtime-owned inputs. */
export function replaceInputSources(
  rt: IvmRuntime,
  replacements: Iterable<InputSourceReplacement>,
  storage: OrderedKvStorage,
) {
  return replaceInputSourcesInMode(rt, replacements, storage, InputSourceTickMode.Complete);
}
export function replaceInputSourcesDetachingCold(
  rt: IvmRuntime,
  replacements: Iterable<InputSourceReplacement>,
  storage: OrderedKvStorage,
) {
  return replaceInputSourcesInMode(rt, replacements, storage, InputSourceTickMode.DetachCold);
}

async function replaceInputSourcesInMode(
  rt: IvmRuntime,
  replacements: Iterable<InputSourceReplacement>,
  storage: OrderedKvStorage,
  mode: InputSourceTickMode,
): Promise<TickMetrics> {
  const canonical = new SortedMap<
    string,
    { id: InputSourceId; descriptor: RecordDescriptor; records: SortedSet<Bytes> }
  >(cmpStr);
  for (const replacement of replacements) {
    const records = new SortedSet<Bytes>(cmpBytes, replacement.records);
    validateRecords(records, replacement.descriptor);
    const k = replacement.id.sortKey();
    const existing = canonical.get(k);
    if (!existing) {
      canonical.set(k, { id: replacement.id, descriptor: replacement.descriptor, records });
      continue;
    }
    if (!descriptorsEqual(existing.descriptor, replacement.descriptor)) {
      throw IvmRuntimeError.of("BindingSourceDescriptorMismatch", existing.id.diagnosticName());
    }
    for (const r of records) existing.records.add(r);
  }
  // Every failure-prone check runs before the first source changes.
  for (const [, c] of canonical) preflightInputSource(rt, c.id, c.descriptor);

  const deltas: BindingDelta[] = [];
  for (const [, { id, descriptor, records: replacement }] of canonical) {
    const key = id.bindingKey();
    const source = rt.bindingSources.getMut(key)!;
    const previous = new SortedSet<Bytes>(
      cmpBytes,
      [...source.refcounts.values()].filter(([, count]) => count > 0).map(([r]) => r),
    );
    const changed =
      previous.size !== replacement.size || [...previous].some((r) => !replacement.has(r));
    const initializesSnapshot = !source.initialized;
    if (!changed && !initializesSnapshot) continue;
    source.initialized = true;
    const records: RecordDelta[] = [
      ...[...previous].filter((r) => !replacement.has(r)).map((record) => ({ record, weight: -1 })),
      ...[...replacement].filter((r) => !previous.has(r)).map((record) => ({ record, weight: 1 })),
    ];
    source.refcounts = new Map([...replacement].map((r) => [bk(r), [r, 1] as [Bytes, number]]));
    deltas.push({ key, descriptor, deltas: records, initializesSnapshot });
  }
  if (!deltas.length) return new TickMetrics();
  return tickInputSources(rt, deltas, storage, mode);
}

function tickInputSources(
  rt: IvmRuntime,
  deltas: BindingDelta[],
  storage: OrderedKvStorage,
  mode: InputSourceTickMode,
) {
  const owned = new OwnedStorage(storage);
  return mode === InputSourceTickMode.Complete
    ? tickWithParams(rt, [], deltas, owned, undefined)
    : tickBindingsDetachingCold(rt, deltas, owned);
}

/** Retire runtime-owned input sources permanently, retracting live records first. */
export async function retireInputSources(
  rt: IvmRuntime,
  ids: Iterable<InputSourceId>,
  storage: OrderedKvStorage,
): Promise<TickMetrics> {
  const unique = new SortedMap<string, InputSourceId>(cmpStr);
  for (const id of ids) unique.set(id.sortKey(), id);
  for (const [, id] of unique) {
    if (!id.belongsTo(rt.inputSourceRuntimeNamespace))
      throw IvmRuntimeError.of("ForeignInputSource");
    if (!rt.bindingSources.containsKey(id.bindingKey()))
      throw IvmRuntimeError.of("InputSourceRetired");
  }
  const deltas: BindingDelta[] = [];
  for (const [, id] of unique) {
    const key = id.bindingKey();
    const source = rt.bindingSources.getMut(key)!;
    const records = [...source.refcounts.values()].map(([record]) => ({ record, weight: -1 }));
    if (records.length)
      deltas.push({
        key,
        descriptor: source.descriptor,
        deltas: records,
        initializesSnapshot: false,
      });
    source.refcounts.clear();
  }
  let metrics: TickMetrics;
  if (!deltas.length) {
    await drivePendingIncremental(rt);
    metrics = new TickMetrics();
  } else {
    metrics = await tickWithParams(rt, [], deltas, new OwnedStorage(storage), undefined);
  }
  for (const [, id] of unique) {
    const key = id.bindingKey();
    rt.bindingSources.remove(key);
    rt.bindingFrontiers.delete(key);
  }
  return metrics;
}

// ---- direct subscriptions ----

export function subscribeOneSink(
  rt: IvmRuntime,
  graph: GraphBuilder,
  storage: OrderedKvStorage,
): Promise<Subscription> {
  return subscribeOneSinkWithWaker(rt, graph, storage, undefined);
}

/** Direct one-sink opening with an optional continuation waker. */
export async function subscribeOneSinkWithWaker(
  rt: IvmRuntime,
  graph: GraphBuilder,
  storage: OrderedKvStorage,
  progressWaker: Waker | undefined,
): Promise<Subscription> {
  if (builderContainsBindingSource(graph)) throw IvmRuntimeError.of("BindingSourceRequiresPrepare");
  const plan = rt.autoDirectFamilyEnabled ? planAutoDirectFamily(rt, graph) : undefined;
  if (plan) {
    let shapeId = lookupAutoDirectFamily(rt, plan.key);
    if (shapeId === undefined) {
      const shape = await prepareOneSink(
        rt,
        plan.graph,
        plan.shape,
        plan.bindingDescriptor,
        [plan.bindingField],
        storage,
      );
      const state = rt.preparedShapes.get(shape.id);
      if (state) {
        state.autoFamilyKey = plan.key;
        const terminal = state.terminals.get(DEFAULT_SINK);
        if (terminal) terminal.terminal.publicFields = plan.publicFields.slice();
      }
      rt.autoDirectFamilies.set(plan.key, shape.id);
      shapeId = shape.id;
    }
    return bindShapeOneSinkWithWaker(
      rt,
      shapeId,
      [plan.bindingValue],
      storage,
      progressWaker,
      undefined,
    );
  }
  const multisink = subscribeStaged(
    rt,
    [[DEFAULT_SINK, graph]],
    storage,
    SubscriptionLifetime.Retained,
    MATERIALIZE,
  );
  const subscription = singleSinkSubscription(rt, multisink, DEFAULT_SINK);
  pollReadySubscriptionWorkNowWithWaker(rt, progressWaker);
  return subscription;
}

function lookupAutoDirectFamily(
  rt: IvmRuntime,
  key: AutoDirectFamilyKey,
): PreparedShapeId | undefined {
  for (const [candidate, id] of rt.autoDirectFamilies) {
    if (
      autoDirectFamilyKeyHash(candidate) === autoDirectFamilyKeyHash(key) &&
      autoDirectFamilyKeysEqual(candidate, key)
    )
      return id;
  }
  return undefined;
}

export function subscribe(
  rt: IvmRuntime,
  sinks: Iterable<[string, GraphBuilder]>,
  storage: OrderedKvStorage,
): MultisinkSubscription {
  return subscribeWithLifetime(
    rt,
    sinks,
    storage,
    SubscriptionLifetime.Retained,
    MATERIALIZE,
    undefined,
  );
}
export function subscribeWithWaker(
  rt: IvmRuntime,
  sinks: Iterable<[string, GraphBuilder]>,
  storage: OrderedKvStorage,
  progressWaker: Waker | undefined,
): MultisinkSubscription {
  return subscribeWithLifetime(
    rt,
    sinks,
    storage,
    SubscriptionLifetime.Retained,
    MATERIALIZE,
    progressWaker,
  );
}
export function subscribeWithLifetime(
  rt: IvmRuntime,
  sinks: Iterable<[string, GraphBuilder]>,
  storage: OrderedKvStorage,
  lifetime: SubscriptionLifetime,
  rootIndirectValues: RootIndirectValues,
  progressWaker: Waker | undefined,
): MultisinkSubscription {
  const subscription = subscribeStaged(rt, [...sinks], storage, lifetime, rootIndirectValues);
  pollReadySubscriptionWorkNowWithWaker(rt, progressWaker);
  return subscription;
}

/** Publish all subscription work that can complete from resident inputs now. */
function pollReadySubscriptionWorkNowWithWaker(
  rt: IvmRuntime,
  progressWaker: Waker | undefined,
): void {
  if (progressWaker) retainOwnerProgressWaker(rt, progressWaker);
  const cx = progressWaker ?? noopWaker;
  if (pollPendingIncremental(rt, cx) !== PENDING || progressWaker) return;
  // A direct opening owns every explicitly retained CPU slice; storage
  // requests stay untouched for a later owner.
  while (hasResidentContinuation(rt)) {
    if (pollResidentIncremental(rt, cx) !== PENDING) return;
  }
}

function newSubscriptionChannel(): [SubscriptionSender, SubscriptionChannel] {
  const channel = new SubscriptionChannel();
  return [new SubscriptionSender(channel), channel];
}

function subscribeStaged(
  rt: IvmRuntime,
  sinks: [string, GraphBuilder][],
  storage: OrderedKvStorage,
  lifetime: SubscriptionLifetime,
  rootIndirectValues: RootIndirectValues,
): MultisinkSubscription {
  if (!sinks.length) throw IvmRuntimeError.of("EmptyMultisinkSubscription");
  const names = new Set<string>();
  for (const [sink] of sinks) {
    if (names.has(sink)) throw IvmRuntimeError.of("DuplicateMultisinkSink", sink);
    names.add(sink);
  }
  const withBinding = sinks.find(([, graph]) => builderContainsBindingSource(graph));
  if (withBinding) throw IvmRuntimeError.of("MultisinkSinkRequiresPrepare", withBinding[0]);
  const subscriptionId = nextSubscriptionId(rt);
  const [sender, channel] = newSubscriptionChannel();
  // Compilation may add nodes to the shared graph; the ephemeral guard
  // collects unretained additions if hydration setup fails.
  const install = new EphemeralGraphInstall(rt);
  const outputs = new SortedMap<string, CompiledNode>(cmpStr);
  try {
    rt.logicalNodesRequested += sinks.reduce((n, [, g]) => n + countBuilderNodes(g), 0);
    for (const [sink, graph] of sinks) outputs.set(sink, addDedupGraph(rt, graph));
    for (const [, output] of outputs)
      addRetainer(rt, output.node, lifetimeRetainer(lifetime, subscriptionId));
    install.commit();
  } finally {
    install.finish();
  }
  rt.multisinkSubscriptions.set(subscriptionId, {
    sender,
    outputs: outputs.clone(),
    target: { kind: "Direct" },
    failed: false,
    rootIndirectValues,
  });
  if (lifetime === SubscriptionLifetime.Retained)
    indexSubscriptionOutputs(rt, subscriptionId, outputs);
  const initial = new InitialSlot();
  enqueueSubscriptionHydration(
    rt,
    subscriptionId,
    outputs,
    new OwnedStorage(storage),
    undefined,
    undefined,
    initial,
    lifetime,
    rootIndirectValues,
    new Set(),
  );
  return new MultisinkSubscription(subscriptionId, initial, channel);
}

// ---- prepared shapes ----

export async function prepare(
  rt: IvmRuntime,
  terminals: Iterable<RoutedMultisinkTerminal>,
  shape: string,
  bindingDescriptor: RecordDescriptor,
  storage: OrderedKvStorage,
): Promise<PreparedShape> {
  await flushPendingBindingRetractions(rt, storage);
  const list = [...terminals];
  if (!list.length) throw IvmRuntimeError.of("EmptyMultisinkSubscription");
  const names = new Set<string>();
  const arity = bindingDescriptor.fields().length;
  for (const terminal of list) {
    if (names.has(terminal.sink)) throw IvmRuntimeError.of("DuplicateMultisinkSink", terminal.sink);
    names.add(terminal.sink);
    if (terminal.routeFields.length > arity) {
      throw IvmRuntimeError.of("RoutedMultisinkRouteArityMismatch", {
        sink: terminal.sink,
        expected: arity,
        actual: terminal.routeFields.length,
      });
    }
    if (terminal.routeValueIndices.length !== terminal.routeFields.length) {
      throw IvmRuntimeError.of("RoutedMultisinkRouteArityMismatch", {
        sink: terminal.sink,
        expected: terminal.routeFields.length,
        actual: terminal.routeValueIndices.length,
      });
    }
    const outOfBounds = terminal.routeValueIndices.find((i) => i >= arity);
    if (outOfBounds !== undefined)
      throw IvmRuntimeError.of("GraphFieldIndexOutOfBounds", outOfBounds);
    const output = inferBuilderOutput(rt, terminal.graph);
    for (const field of terminal.routeFields) {
      if (output.fieldIndex(field) === undefined)
        throw IvmRuntimeError.of("GraphFieldNotFound", field);
    }
    for (const field of terminal.publicFields) resolveFieldRef(output, FR.storedName(field));
  }
  rt.logicalNodesRequested += list.reduce((n, t) => n + countBuilderNodes(t.graph), 0);
  const shapeId = nextShapeId(rt);
  const sourceKey = BSK.prepared(shape);
  const existing = rt.bindingSources.get(sourceKey);
  if (existing && !descriptorsEqual(existing.descriptor, bindingDescriptor)) {
    throw IvmRuntimeError.of("BindingSourceDescriptorMismatch", shape);
  }
  const insertedSource = !existing;
  if (insertedSource)
    rt.bindingSources.insert(sourceKey, {
      descriptor: bindingDescriptor,
      refcounts: new Map(),
      initialized: true,
    });
  const install = new EphemeralGraphInstall(rt);
  try {
    const terminalStates = new SortedMap<string, RoutedMultisinkTerminalState>(cmpStr);
    for (const terminal of list) {
      let output: CompiledNode;
      try {
        output = addDedupGraph(rt, terminal.graph);
      } catch (error) {
        if (insertedSource) rt.bindingSources.remove(BSK.prepared(shape));
        throw error;
      }
      terminalStates.set(terminal.sink, { terminal, output });
    }
    // Publish retainers only after every terminal compiles.
    for (const [, terminal] of terminalStates) {
      addRetainer(rt, terminal.output.node, { kind: "PreparedShape", key: String(shapeId) });
    }
    rt.preparedShapes.set(shapeId, {
      shape,
      bindingDescriptor,
      terminals: terminalStates,
      autoFamilyKey: undefined,
      sharedKey: undefined,
      retireWhenUnbound: false,
    });
    install.commit();
  } finally {
    install.finish();
  }
  return { id: shapeId };
}

/** Like prepare, but identical terminals over the same source share one shape. */
export async function prepareShared(
  rt: IvmRuntime,
  terminals: Iterable<RoutedMultisinkTerminal>,
  shape: string,
  bindingDescriptor: RecordDescriptor,
  storage: OrderedKvStorage,
): Promise<PreparedShape> {
  const sorted = [...terminals].sort((l, r) => cmpStr(l.sink, r.sink));
  const key: SharedShapeKey = { shape, terminals: sorted };
  // Prune first so a dropped shared shape retires now, not between this
  // caller's prepare and bind.
  await pruneDroppedSubscriptionsWithStorage(rt, storage);
  const shapeId = rt.sharedPreparedShapes.get(sharedShapeKeyString(key));
  const state = shapeId === undefined ? undefined : rt.preparedShapes.get(shapeId);
  if (
    shapeId !== undefined &&
    state &&
    descriptorsEqual(state.bindingDescriptor, bindingDescriptor) &&
    state.sharedKey &&
    state.sharedKey.terminals.length === sorted.length &&
    state.sharedKey.terminals.every((t, i) => routedTerminalsEqual(t, sorted[i]))
  ) {
    await flushPendingBindingRetractions(rt, storage);
    return { id: shapeId };
  }
  const prepared = await prepare(rt, sorted, shape, bindingDescriptor, storage);
  const preparedState = rt.preparedShapes.get(prepared.id);
  if (preparedState) {
    preparedState.sharedKey = key;
    preparedState.retireWhenUnbound = true;
  }
  rt.sharedPreparedShapes.set(sharedShapeKeyString(key), prepared.id);
  return prepared;
}

/** Retire a shared prepared shape that no retained binding holds. */
export function releaseSharedPreparedShape(rt: IvmRuntime, shapeId: PreparedShapeId): void {
  removeUnreferencedBindingOwnedShape(rt, shapeId);
}

/** Transfer a successful binding's shape to its retained bindings. */
export function transferPreparedShapeToBindings(rt: IvmRuntime, shapeId: PreparedShapeId): void {
  const shape = rt.preparedShapes.get(shapeId);
  if (shape) shape.retireWhenUnbound = true;
  removeUnreferencedBindingOwnedShape(rt, shapeId);
}

export function bindShape(
  rt: IvmRuntime,
  shapeId: PreparedShapeId,
  bindingValues: Value[],
  storage: OrderedKvStorage,
) {
  const subscription = bindShapeWithPublicFieldsStaged(
    rt,
    shapeId,
    bindingValues,
    { kind: "Fields", fields: new Map() },
    storage,
    SubscriptionLifetime.Retained,
    MATERIALIZE,
    undefined,
  );
  pollReadySubscriptionWorkNowWithWaker(rt, undefined);
  return subscription;
}

export function bindShapeWithLifetime(
  rt: IvmRuntime,
  shapeId: PreparedShapeId,
  bindingValues: Value[],
  storage: OrderedKvStorage,
  lifetime: SubscriptionLifetime,
  rootIndirectValues: RootIndirectValues,
  progressWaker: Waker | undefined,
  live: LiveAttach | undefined,
): MultisinkSubscription {
  const subscription = bindShapeWithPublicFieldsStaged(
    rt,
    shapeId,
    bindingValues,
    { kind: "Fields", fields: new Map() },
    storage,
    lifetime,
    rootIndirectValues,
    live,
  );
  pollReadySubscriptionWorkNowWithWaker(rt, progressWaker);
  return subscription;
}

function bindShapeWithPublicFieldsStaged(
  rt: IvmRuntime,
  shapeId: PreparedShapeId,
  bindingValues: Value[],
  publicOutput: PreparedPublicOutput,
  storage: OrderedKvStorage,
  lifetime: SubscriptionLifetime,
  rootIndirectValues: RootIndirectValues,
  live: LiveAttach | undefined,
): MultisinkSubscription {
  try {
    return bindShapeWithPublicFieldsStagedInner(
      rt,
      shapeId,
      bindingValues,
      publicOutput,
      storage,
      lifetime,
      rootIndirectValues,
      live,
    );
  } catch (error) {
    if (live) {
      // The attach tick already admitted this binding; retract it next tick.
      const delta = removeBindingRefForSource(rt, live.bindingSource, live.bindingKey);
      if (delta && delta.deltas.length) rt.pendingBindingRetractions.push(delta);
    }
    throw error;
  }
}

function bindShapeWithPublicFieldsStagedInner(
  rt: IvmRuntime,
  shapeId: PreparedShapeId,
  bindingValues: Value[],
  publicOutput: PreparedPublicOutput,
  storage: OrderedKvStorage,
  lifetime: SubscriptionLifetime,
  rootIndirectValues: RootIndirectValues,
  live: LiveAttach | undefined,
): MultisinkSubscription {
  const shape = rt.preparedShapes.get(shapeId);
  if (!shape) throw IvmRuntimeError.of("PreparedShapeNotFound", shapeId);
  let publicFields: Map<string, string[]>;
  if (publicOutput.kind === "Fields") publicFields = publicOutput.fields;
  else {
    const terminal = shape.terminals.get(DEFAULT_SINK);
    if (!terminal) throw IvmRuntimeError.of("DuplicateMultisinkSink", DEFAULT_SINK);
    validatePublicOutputFields(terminal.output.output, publicOutput.output);
    publicFields = new Map([[DEFAULT_SINK, descriptorFieldNames(publicOutput.output)]]);
  }
  const bindingKey: BindingKey = IvmRuntimeError.wrapRecord(() =>
    shape.bindingDescriptor.create(bindingValues),
  );
  const subscriptionId = nextSubscriptionId(rt);
  const install = new EphemeralGraphInstall(rt);
  const outputs = new SortedMap<string, CompiledNode>(cmpStr);
  const routeBarriers: NodeId[] = [];
  let bindingSnapshots: BindingSnapshots;
  let bindingAdded = false;
  try {
    rt.logicalNodesRequested += shape.terminals
      .valuesArray()
      .reduce((n, t) => n + countBuilderNodes(t.terminal.graph) + 2, 0);
    for (const [sink, prepared] of shape.terminals) {
      const terminal = new RoutedMultisinkTerminal(
        prepared.terminal.sink,
        prepared.terminal.graph,
        prepared.terminal.routeFields,
        publicFields.get(sink) ?? prepared.terminal.publicFields,
      ).withRouteValueIndices(prepared.terminal.routeValueIndices);
      const graph = boundRoutedMultisinkGraph(terminal, bindingValues, prepared.output.output);
      const output = addDedupGraph(rt, graph);
      if (lifetime === SubscriptionLifetime.Retained) {
        const barrier = registerRouteBarrier(rt, terminal, prepared.output, output, bindingValues);
        if (barrier !== undefined) routeBarriers.push(barrier);
      }
      outputs.set(sink, output);
    }
    const bindingShape = bindingSourceShapeName(rt, shapeId);
    const sourceKey = BSK.prepared(bindingShape);
    if (live) {
      // Shared nodes are already current: hydrate against just this binding.
      bindingSnapshots = cloneSnapshots(bindingSnapshotDeltas(rt));
      const current = bindingSnapshots.get(sourceKey);
      if (current) {
        bindingSnapshots.set(sourceKey, {
          descriptor: current.descriptor,
          deltas: current.deltas.filter((d) => equalBytes(d.record, bindingKey)),
        });
      }
    } else {
      const cancelledRetraction =
        lifetime === SubscriptionLifetime.Retained &&
        cancelPendingBindingRetraction(rt, bindingShape, bindingKey);
      const bindingDelta = provisionalBindingDelta(rt, shapeId, bindingKey);
      // A queued retraction still changes the source's binding set.
      bindingAdded =
        bindingDelta.deltas.length > 0 ||
        rt.pendingBindingRetractions.some((p) => BSK.key(p.key) === BSK.key(bindingDelta.key));
      bindingSnapshots = cloneSnapshots(bindingSnapshotDeltas(rt));
      const prior = bindingSnapshots.get(bindingDelta.key);
      const snapshot: RecordDeltas = {
        descriptor: prior?.descriptor ?? bindingDelta.descriptor,
        deltas: prior?.deltas.slice() ?? [],
      };
      for (const delta of bindingDelta.deltas) {
        if (delta.weight > 0 && !snapshot.deltas.some((e) => equalBytes(e.record, delta.record)))
          snapshot.deltas.push(delta);
      }
      bindingSnapshots.set(bindingDelta.key, snapshot);
      if (lifetime === SubscriptionLifetime.Retained) {
        const installed = addBindingRef(rt, shapeId, bindingKey);
        if (!cancelledRetraction) bumpInputFrontiers(rt, [], [installed]);
      }
    }
    for (const [, output] of outputs)
      addRetainer(rt, output.node, lifetimeRetainer(lifetime, subscriptionId));
    install.commit();
  } finally {
    install.finish();
  }
  const [sender, channel] = newSubscriptionChannel();
  rt.multisinkSubscriptions.set(subscriptionId, {
    sender,
    outputs: outputs.clone(),
    target:
      lifetime === SubscriptionLifetime.Retained
        ? { kind: "RoutedShape", shapeId, bindingKey, routeBarriers }
        : { kind: "Direct" },
    failed: false,
    rootIndirectValues,
  });
  if (lifetime === SubscriptionLifetime.Retained)
    indexSubscriptionOutputs(rt, subscriptionId, outputs);
  const initial = new InitialSlot();
  // Only a change to the source's binding set invalidates readers (#3797).
  const frontierAdvance = live ? undefined : bindingAdded ? shape.shape : undefined;
  const borrowed = live ? live.borrowed : new Set<NodeId>();
  if (live) live.borrowed = new Set();
  enqueueSubscriptionHydration(
    rt,
    subscriptionId,
    outputs,
    new OwnedStorage(storage),
    bindingSnapshots,
    frontierAdvance,
    initial,
    lifetime,
    rootIndirectValues,
    borrowed,
  );
  return new MultisinkSubscription(subscriptionId, initial, channel);
}

function cloneSnapshots(s: BindingSnapshots): BindingSnapshots {
  const out: BindingSnapshots = new KeyedMap(BSK.key);
  for (const [k, v] of s) out.set(k, v);
  return out;
}

/** One-sink sugar: a routed multisink shape with a single default terminal. */
export async function prepareOneSink(
  rt: IvmRuntime,
  graph: GraphBuilder,
  shape: string,
  bindingDescriptor: RecordDescriptor,
  outputKeyFields: string[],
  storage: OrderedKvStorage,
): Promise<PreparedShape> {
  const output = inferBuilderOutput(rt, graph);
  for (const field of outputKeyFields) {
    if (output.fieldIndex(field) === undefined)
      throw IvmRuntimeError.of("ShapeKeyFieldNotFound", field);
  }
  const publicFields = descriptorFieldNames(output);
  return prepare(
    rt,
    [new RoutedMultisinkTerminal(DEFAULT_SINK, graph, outputKeyFields, publicFields)],
    shape,
    bindingDescriptor,
    storage,
  );
}

export async function prepareOneSinkWithRouting(
  rt: IvmRuntime,
  outputGraph: GraphBuilder,
  routingGraph: GraphBuilder,
  shape: string,
  bindingDescriptor: RecordDescriptor,
  routingKeyFields: string[],
  storage: OrderedKvStorage,
): Promise<PreparedShape> {
  const output = inferBuilderOutput(rt, outputGraph);
  const routingOutput = inferBuilderOutput(rt, routingGraph);
  validatePublicOutputFields(routingOutput, output);
  for (const field of routingKeyFields) {
    if (routingOutput.fieldIndex(field) === undefined)
      throw IvmRuntimeError.of("ShapeKeyFieldNotFound", field);
  }
  const publicFields = descriptorFieldNames(output);
  return prepare(
    rt,
    [new RoutedMultisinkTerminal(DEFAULT_SINK, routingGraph, routingKeyFields, publicFields)],
    shape,
    bindingDescriptor,
    storage,
  );
}

export function bindShapeOneSink(
  rt: IvmRuntime,
  shapeId: PreparedShapeId,
  bindingValues: Value[],
  storage: OrderedKvStorage,
) {
  return bindShapeOneSinkWithWaker(rt, shapeId, bindingValues, storage, undefined, undefined);
}

export function bindShapeOneSinkWithWaker(
  rt: IvmRuntime,
  shapeId: PreparedShapeId,
  bindingValues: Value[],
  storage: OrderedKvStorage,
  progressWaker: Waker | undefined,
  live: LiveAttach | undefined,
): Subscription {
  const multisink = bindShapeWithPublicFieldsStaged(
    rt,
    shapeId,
    bindingValues,
    { kind: "Fields", fields: new Map() },
    storage,
    SubscriptionLifetime.Retained,
    MATERIALIZE,
    live,
  );
  const subscription = singleSinkSubscription(rt, multisink, DEFAULT_SINK);
  pollReadySubscriptionWorkNowWithWaker(rt, progressWaker);
  return subscription;
}

export function bindShapeOneSinkWithOutputAndWaker(
  rt: IvmRuntime,
  shapeId: PreparedShapeId,
  bindingValues: Value[],
  publicOutput: RecordDescriptor,
  storage: OrderedKvStorage,
  progressWaker: Waker | undefined,
  live: LiveAttach | undefined,
): Subscription {
  const multisink = bindShapeWithPublicFieldsStaged(
    rt,
    shapeId,
    bindingValues,
    { kind: "OneSink", output: publicOutput },
    storage,
    SubscriptionLifetime.Retained,
    MATERIALIZE,
    live,
  );
  const subscription = singleSinkSubscription(rt, multisink, DEFAULT_SINK);
  pollReadySubscriptionWorkNowWithWaker(rt, progressWaker);
  return subscription;
}

// ---- route barriers (#3288) ----

/** Mark a bound terminal's route filter as a route barrier; undefined keeps ordinary activation. */
function registerRouteBarrier(
  rt: IvmRuntime,
  terminal: RoutedMultisinkTerminal,
  shared: CompiledNode,
  bound: CompiledNode,
  bindingValues: Value[],
): NodeId | undefined {
  if (!terminal.routeFields.length) return undefined;
  let barrier: NodeId, sharedNode: NodeId, sharedOutput: RecordDescriptor;
  if (terminal.graph.kind === "CollectBy") {
    const found = collectorRouteFilter(rt, bound.node);
    if (!found) return undefined;
    [barrier, sharedNode, sharedOutput] = found;
  } else {
    const found = flatRouteFilter(rt, shared.node, bound.node);
    if (found === undefined) return undefined;
    [barrier, sharedNode, sharedOutput] = [found, shared.node, shared.output];
  }
  const fieldIndices: number[] = [];
  const fieldTypes: any[] = [];
  const values: Value[] = [];
  for (let i = 0; i < terminal.routeFields.length; i++) {
    const index = sharedOutput.fieldIndex(terminal.routeFields[i]);
    if (index === undefined) return undefined;
    const valueType = sharedOutput.fields()[index].valueType;
    if (!isRoutableType(valueType)) return undefined;
    const value = bindingValues[terminal.routeValueIndices[i]];
    if (value === undefined) return undefined;
    fieldIndices.push(index);
    fieldTypes.push(valueType);
    values.push(value);
  }
  const key = encodeRouteKey(values, fieldTypes);
  if (key === undefined) return undefined;
  // One table keys one shared node's delta by one field list.
  const table = rt.graph.routes().table(sharedNode);
  if (table && table.fieldIndices.join(",") !== fieldIndices.join(",")) return undefined;
  // A routed TopBy collects windows only for outputs that apply them.
  const rootOrderingNode =
    bound.rootOrderingNode !== undefined &&
    (outputConsumesRootPositions(rt.graph, bound.node, bound.rootOrderingNode) ?? true)
      ? bound.rootOrderingNode
      : undefined;
  rt.graph.addRouteBarrier(sharedNode, barrier, fieldIndices, fieldTypes, key, rootOrderingNode);
  return barrier;
}

/** The route filter of a flat binding: bound must be Project(Filter(shared)). */
function flatRouteFilter(rt: IvmRuntime, shared: NodeId, bound: NodeId): NodeId | undefined {
  const project = rt.graph.node(bound);
  if (
    !project ||
    project.descriptor.operator.kind !== "MapProject" ||
    project.descriptor.inputs.length !== 1
  )
    return undefined;
  const barrier = project.descriptor.inputs[0];
  const filter = rt.graph.node(barrier);
  if (!filter || filter.descriptor.operator.kind !== "Filter") return undefined;
  if (filter.descriptor.inputs.length !== 1 || filter.descriptor.inputs[0] !== shared)
    return undefined;
  return barrier;
}

/** The route filter of a collector binding: CollectBy(Arrange*(Filter(shared))). */
function collectorRouteFilter(
  rt: IvmRuntime,
  bound: NodeId,
): [NodeId, NodeId, RecordDescriptor] | undefined {
  const collector = rt.graph.node(bound);
  if (!collector || collector.descriptor.operator.kind !== "CollectBy") return undefined;
  let current = collector;
  for (;;) {
    if (current.descriptor.inputs.length !== 1) return undefined;
    current = rt.graph.node(current.descriptor.inputs[0]);
    if (!current) return undefined;
    const kind = current.descriptor.operator.kind;
    if (kind === "Arrange") continue;
    if (kind === "Filter") break;
    return undefined;
  }
  if (current.descriptor.inputs.length !== 1) return undefined;
  const shared = current.descriptor.inputs[0];
  const sharedNode = rt.graph.node(shared);
  if (!sharedNode) return undefined;
  return [current.id, shared, sharedNode.descriptor.output.records()];
}

export function indexSubscriptionOutputs(
  rt: IvmRuntime,
  id: SubscriptionId,
  outputs: SortedMap<string, CompiledNode>,
): void {
  for (const [, output] of outputs) {
    let set = rt.subscriptionsByOutputNode.get(output.node);
    if (!set) rt.subscriptionsByOutputNode.set(output.node, (set = new Set()));
    set.add(id);
  }
}

export function unindexSubscriptionOutputs(
  rt: IvmRuntime,
  id: SubscriptionId,
  outputs: SortedMap<string, CompiledNode>,
): void {
  for (const [, output] of outputs) {
    const set = rt.subscriptionsByOutputNode.get(output.node);
    if (!set) continue;
    set.delete(id);
    if (!set.size) rt.subscriptionsByOutputNode.delete(output.node);
  }
}

// ---- unsubscribe / retirement ----

function detachSubscription(
  rt: IvmRuntime,
  id: SubscriptionId,
): [MultisinkSubscriptionState, boolean] | undefined {
  const subscription = rt.multisinkSubscriptions.get(id);
  if (!subscription) return undefined;
  rt.multisinkSubscriptions.delete(id);
  unindexSubscriptionOutputs(rt, id, subscription.outputs);
  cancelPendingSubscriptionHydration(rt, id);
  const removed = removeMultisinkRetainers(rt, id, subscription.outputs);
  if (subscription.target.kind === "RoutedShape") {
    for (const barrier of subscription.target.routeBarriers) rt.graph.releaseRouteBarrier(barrier);
  }
  return [subscription, removed];
}

export function unsubscribe(rt: IvmRuntime, id: SubscriptionId): boolean {
  const detached = detachSubscription(rt, id);
  if (!detached) return false;
  const [subscription, removed] = detached;
  if (subscription.target.kind === "RoutedShape") {
    const { shapeId, bindingKey } = subscription.target;
    const delta = removeBindingRef(rt, shapeId, bindingKey);
    if (delta && delta.deltas.length) {
      rt.pendingBindingRetractions.push(delta);
      removeUnreferencedAutoFamily(rt, shapeId);
    }
    removeUnreferencedBindingOwnedShape(rt, shapeId);
  }
  return removed;
}

export async function unsubscribeWithStorage(
  rt: IvmRuntime,
  id: SubscriptionId,
  storage: OrderedKvStorage,
): Promise<boolean> {
  const detached = detachSubscription(rt, id);
  if (!detached) return false;
  const [subscription, removed] = detached;
  if (subscription.target.kind === "RoutedShape") {
    const { shapeId, bindingKey } = subscription.target;
    const delta = removeBindingRef(rt, shapeId, bindingKey);
    const hasRetraction = !!delta && delta.deltas.length > 0;
    if (hasRetraction) rt.pendingBindingRetractions.push(delta!);
    // No removed receiver remains to own this cleanup across a cold tick.
    removeUnreferencedBindingOwnedShape(rt, shapeId);
    if (hasRetraction) {
      await flushPendingBindingRetractions(rt, storage);
      removeUnreferencedAutoFamily(rt, shapeId);
    }
  }
  return removed;
}

const targetsShape = (s: MultisinkSubscriptionState, shapeId: PreparedShapeId) =>
  s.target.kind === "RoutedShape" && s.target.shapeId === shapeId;

/** Remove a caller-owned prepared shape after all of its bindings unsubscribed. */
export function retirePreparedShape(rt: IvmRuntime, shapeId: PreparedShapeId): void {
  for (const s of rt.multisinkSubscriptions.values()) {
    if (targetsShape(s, shapeId))
      throw IvmRuntimeError.of("PreparedShapeHasActiveBindings", shapeId);
  }
  const shape = rt.preparedShapes.get(shapeId);
  if (!shape) throw IvmRuntimeError.of("PreparedShapeNotFound", shapeId);
  rt.preparedShapes.delete(shapeId);
  if (shape.sharedKey) {
    const key = sharedShapeKeyString(shape.sharedKey);
    if (rt.sharedPreparedShapes.get(key) === shapeId) rt.sharedPreparedShapes.delete(key);
  }
  for (const [, terminal] of shape.terminals) {
    removeRetainer(rt, terminal.output.node, { kind: "PreparedShape", key: String(shapeId) });
  }
  collectUnretainedEphemeralNodes(rt);
  if (![...rt.preparedShapes.values()].some((other) => other.shape === shape.shape)) {
    const key = BSK.prepared(shape.shape);
    if (rt.bindingSources.get(key)?.refcounts.size === 0) rt.bindingSources.remove(key);
  }
}

export async function pruneDroppedSubscriptionsWithStorage(
  rt: IvmRuntime,
  storage: OrderedKvStorage,
): Promise<number> {
  const dropped = [...rt.multisinkSubscriptions]
    .filter(([, s]) => !s.sender.receiverAlive())
    .map(([id]) => id);
  for (const id of dropped) await unsubscribeWithStorage(rt, id, storage);
  return dropped.length;
}

export function addDedupSchemaIndices(rt: IvmRuntime): void {
  for (const table of rt.schema.tables.slice()) {
    for (const index of table.indices) addDedupSchemaIndex(rt, table, index);
  }
}

export function subscriptionOutputNode(rt: IvmRuntime, id: SubscriptionId): NodeId | undefined {
  const s = rt.multisinkSubscriptions.get(id);
  return s && s.outputs.size === 1 ? s.outputs.valuesArray()[0].node : undefined;
}

export function subscriptionOutput(
  rt: IvmRuntime,
  id: SubscriptionId,
): RecordDescriptor | undefined {
  const s = rt.multisinkSubscriptions.get(id);
  return s && s.outputs.size === 1 ? s.outputs.valuesArray()[0].output : undefined;
}

function singleSinkSubscription(
  rt: IvmRuntime,
  inner: MultisinkSubscription,
  sink: string,
): Subscription {
  const output = subscriptionOutput(rt, inner.id);
  if (!output) throw IvmRuntimeError.of("GraphOutputMismatch");
  return new Subscription(inner, sink, output);
}

function planAutoDirectFamily(
  rt: IvmRuntime,
  graph: GraphBuilder,
): AutoDirectFamilyPlan | undefined {
  if (builderContainsRecursive(graph)) return undefined;
  const originalOutput = inferBuilderOutput(rt, graph);
  const bindingField = autoDirectBindingField(graph, originalOutput, rt);
  const lifted = liftLiteralFilter(rt, graph, bindingField);
  if (!lifted) return undefined;
  const seeded = replaceBindingShape(lifted.graph, "__auto_direct_shape");
  const shapeOutput = inferBuilderOutput(rt, seeded);
  validatePublicOutputFields(shapeOutput, originalOutput);
  const publicFields = descriptorFieldNames(originalOutput);
  const shape = `auto_direct_${graphBuilderFingerprint(seeded).toString(16).padStart(16, "0")}`;
  const shaped = replaceBindingShape(seeded, shape);
  if (shapeOutput.fieldIndex(bindingField) === undefined) return undefined;
  const valueType = literalValueType(lifted.value);
  if (!valueType) throw IvmRuntimeError.of("UnsupportedOperator");
  const bindingDescriptor = newRecordDescriptor([[bindingField, valueType]]);
  return {
    key: { graph: shaped, bindingDescriptor, bindingField, publicFields },
    graph: shaped,
    shape,
    bindingDescriptor,
    bindingField,
    bindingValue: literalToValue(lifted.value),
    publicFields,
  };
}

// ---- output descriptor inference ----

export function inferBuilderOutput(rt: IvmRuntime, graph: GraphBuilder): RecordDescriptor {
  return inferBuilderOutputCached(rt, graph, new Map());
}

/** Infer every child before its parent explicitly (deep policy graphs). */
export function inferBuilderOutputCached(
  rt: IvmRuntime,
  graph: GraphBuilder,
  memo: Map<GraphBuilder, RecordDescriptor>,
): RecordDescriptor {
  const hit = memo.get(graph);
  if (hit) return hit;
  for (const builder of builderPostorder(graph)) {
    if (memo.has(builder)) continue;
    memo.set(builder, inferBuilderOutputUncached(rt, builder, memo));
  }
  const output = memo.get(graph);
  if (!output) throw IvmRuntimeError.of("UnsupportedOperator");
  return output;
}

export function inferBuilderOutputUncached(
  rt: IvmRuntime,
  graph: GraphBuilder,
  memo: Map<GraphBuilder, RecordDescriptor>,
): RecordDescriptor {
  const infer = (g: GraphBuilder) => inferBuilderOutputCached(rt, g, memo);
  const g: any = graph;
  switch (g.kind) {
    case "TypedTemplate": {
      if (g.predicates.length !== g.program.predicateMarkers.length)
        throw IvmRuntimeError.of("GraphOutputMismatch");
      if (g.inputs.length !== g.program.inputs.length)
        throw IvmRuntimeError.of("GraphOutputMismatch");
      g.inputs.forEach((input: GraphBuilder, i: number) => {
        if (!descriptorsEqual(infer(input), g.program.inputs[i]))
          throw IvmRuntimeError.of("GraphOutputMismatch");
      });
      return g.program.output;
    }
    case "TemplateInput": {
      if (!g.input) throw IvmRuntimeError.of("UnsupportedOperator");
      if (!descriptorsEqual(infer(g.input), g.output))
        throw IvmRuntimeError.of("GraphOutputMismatch");
      return g.output;
    }
    case "Table":
      return tableSourceOutput(rt, g.table, g.variantProjection);
    case "Lookup":
      infer(g.keys);
      return tableSourceOutput(rt, g.table, g.variantProjection);
    case "InlineRecords":
    case "InputSource":
    case "FrontierSource":
    case "BindingSource":
      return g.output;
    case "Index":
      if (g.rowProjection != null)
        return namedVariantProjectionOutput(rt, g.table, g.rowProjection);
      return indexRecordDescriptor();
    case "Filter":
    case "ArgMaxBy":
    case "ArgMinBy":
    case "TopBy":
      return infer(g.input);
    case "CollectBy": {
      const input = infer(g.input);
      const c = g.collect;
      switch (c.mode) {
        case "Root":
          return collectByRootDescriptor(input, c.parentFields);
        case "Collect":
          return c.slots.length === 0
            ? collectByDescriptor(input, c.parentFields, c.childFields, c.collectionField)
            : collectByTreeDescriptor(input, c.parentFields, c.slots);
        case "Expand":
          if (c.slots.length === 0) return collectByExpandDescriptor(input, c.tupleFields);
          throw IvmRuntimeError.of(
            "InvalidCollectBy",
            "expand mode does not accept recursive collection slots",
          );
      }
      throw IvmRuntimeError.of("UnsupportedOperator");
    }
    case "Aggregate":
      return aggregateDescriptor(infer(g.input), g.groupCols, g.aggregates);
    case "UnwrapNullable": {
      const input = infer(g.input);
      return unwrapNullableDescriptor(input, resolveFieldRef(input, g.field));
    }
    case "Unnest": {
      const input = infer(g.input);
      return unnestDescriptor(input, resolveFieldRef(input, g.arrayField), g.elementField);
    }
    case "VariantProject":
      return variantProjectDescriptor(infer(g.input), g.field, g.case);
    case "Project":
      return projectionPlan(rt, infer(g.input), g.fields).output;
    case "StreamingChecksum": {
      if (g.windowBytes === 0 || g.maxBytesPerTurn === 0)
        throw IvmRuntimeError.of("InvalidStreamingChecksumBudget");
      const input = infer(g.input);
      const fieldIdx = resolveFieldRef(input, g.field);
      const kind = input.fields()[fieldIdx].valueType.kind;
      if (kind !== "String" && kind !== "Bytes")
        throw IvmRuntimeError.of("StreamingChecksumTypeMismatch");
      return newRecordDescriptor(
        input
          .fields()
          .map((f: any, index: number) =>
            index === fieldIdx ? [g.outputField, { kind: "Bytes" }] : [f.name ?? "", f.valueType],
          ),
      );
    }
    case "Union": {
      let output: RecordDescriptor | undefined;
      for (const input of g.inputs) {
        const next = infer(input);
        if (!output) output = next;
        else if (!output.registryCompatibleWith(next))
          throw IvmRuntimeError.of("GraphOutputMismatch");
      }
      return output ?? newRecordDescriptor([]);
    }
    case "Join":
      return joinDescriptor(infer(g.left), infer(g.right));
    case "SemiJoin":
    case "AntiJoin":
      return infer(g.left);
    case "Recursive": {
      const seed = infer(g.seed);
      if (!seed.registryCompatibleWith(infer(g.step)))
        throw IvmRuntimeError.of("GraphOutputMismatch");
      return seed;
    }
    case "RecursiveStepWitness": {
      if (g.recursive.kind !== "Recursive" || !g.recursive.stepWitness)
        throw IvmRuntimeError.of("UnsupportedOperator");
      return infer(g.recursive.stepWitness);
    }
  }
  throw IvmRuntimeError.of("UnsupportedOperator");
}

function namedVariantProjectionOutput(
  rt: IvmRuntime,
  table: string,
  target: string,
): RecordDescriptor {
  const projection = rt.variantProjections.get({ table, target: { kind: "Named", name: target } });
  if (!projection) throw IvmRuntimeError.of("VariantProjectionNotFound", { table, target });
  return projection.output;
}

/** The output of a table source, plain or through a named variant projection. */
function tableSourceOutput(
  rt: IvmRuntime,
  table: string,
  variantProjection: string | undefined,
): RecordDescriptor {
  if (variantProjection != null) return namedVariantProjectionOutput(rt, table, variantProjection);
  const schema = rt.schema.table(table);
  if (!schema) throw IvmRuntimeError.of("TableNotFound", table);
  if (schema.hasVariants()) throw IvmRuntimeError.of("VariantProjectionRequired", table);
  return rt.tableDescriptors.get(table) ?? schema.recordSchema();
}

export function nextSubscriptionId(rt: IvmRuntime): SubscriptionId {
  return `${rt.inputSourceRuntimeNamespace}:${rt.nextSubscriptionId++}`;
}

export function nextShapeId(rt: IvmRuntime): PreparedShapeId {
  return rt.nextShapeId++;
}

// ---- binding references ----

function addBindingRef(
  rt: IvmRuntime,
  shapeId: PreparedShapeId,
  binding: BindingKey,
): BindingDelta {
  return addBindingRefForShape(rt, bindingSourceShapeName(rt, shapeId), binding);
}

/** Reacquiring the last reference cancels its queued retraction. */
function cancelPendingBindingRetraction(
  rt: IvmRuntime,
  shape: string,
  binding: BindingKey,
): boolean {
  let cancelled = false;
  const key = BSK.key(BSK.prepared(shape));
  for (const pending of rt.pendingBindingRetractions) {
    if (BSK.key(pending.key) !== key) continue;
    pending.deltas = pending.deltas.filter((delta) => {
      const matches = delta.weight < 0 && equalBytes(delta.record, binding);
      cancelled ||= matches;
      return !matches;
    });
  }
  rt.pendingBindingRetractions = rt.pendingBindingRetractions.filter((p) => p.deltas.length > 0);
  return cancelled;
}

function provisionalBindingDelta(
  rt: IvmRuntime,
  shapeId: PreparedShapeId,
  binding: BindingKey,
): BindingDelta {
  const shape = bindingSourceShapeName(rt, shapeId);
  const source = rt.bindingSources.get(BSK.prepared(shape));
  if (!source) throw IvmRuntimeError.of("BindingSourceNotFound", shape);
  return {
    key: BSK.prepared(shape),
    descriptor: source.descriptor,
    deltas: source.refcounts.has(bk(binding)) ? [] : [{ record: binding, weight: 1 }],
    initializesSnapshot: false,
  };
}

/**
 * Attach a new binding to a prepared shape whose shared graph is already
 * maintained for sibling bindings, via one ordinary binding tick. Returns
 * undefined, having changed nothing, when that precondition is not certain.
 */
export async function prepareLiveAttach(
  rt: IvmRuntime,
  shapeId: PreparedShapeId,
  bindingValues: Value[],
  storage: OrderedKvStorage,
): Promise<LiveAttach | undefined> {
  await pruneDroppedSubscriptionsWithStorage(rt, storage);
  absorbUnobservedBindingRetractions(rt, shapeId);
  await flushPendingBindingRetractions(rt, storage);
  const borrowed = liveAttachBorrowedNodes(rt, shapeId, bindingValues);
  if (!borrowed) return undefined;
  const shape = rt.preparedShapes.get(shapeId);
  if (!shape) throw IvmRuntimeError.of("PreparedShapeNotFound", shapeId);
  const bindingKey: BindingKey = IvmRuntimeError.wrapRecord(() =>
    shape.bindingDescriptor.create(bindingValues),
  );
  const delta = addBindingRef(rt, shapeId, bindingKey);
  // Admission owns the binding reference until handoff: an unpublished tick
  // owes metadata rollback, a published one a compensating retraction.
  let published = false;
  try {
    await tickWithParams(rt, [], [delta], new OwnedStorage(storage), undefined);
    published = true;
    // A receiver dropped during the attach tick queued a retraction.
    while (rt.pendingBindingRetractions.length) await flushPendingBindingRetractions(rt, storage);
  } catch (error) {
    if (published) {
      const retraction = removeBindingRefForSource(rt, delta.key, bindingKey);
      if (retraction && retraction.deltas.length) rt.pendingBindingRetractions.push(retraction);
    } else removeBindingMetadata(rt, delta.key, bindingKey);
    throw error;
  }
  rt.liveAttaches++;
  return { bindingSource: delta.key, bindingKey, borrowed };
}

/** Drop queued retractions that the coming full hydration makes moot. */
function absorbUnobservedBindingRetractions(rt: IvmRuntime, shapeId: PreparedShapeId): void {
  const shape = rt.preparedShapes.get(shapeId);
  if (!shape) throw IvmRuntimeError.of("PreparedShapeNotFound", shapeId);
  const sourceKey = BSK.prepared(shape.shape);
  const sk = BSK.key(sourceKey);
  if (
    !rt.pendingBindingRetractions.some((p) => BSK.key(p.key) === sk) ||
    hasPendingIncremental(rt) ||
    (rt.bindingSources.get(sourceKey)?.refcounts.size ?? 0) > 0 ||
    [...rt.multisinkSubscriptions.values()].some((s) => {
      if (s.target.kind !== "RoutedShape") return false;
      const target = rt.preparedShapes.get(s.target.shapeId);
      return !target || target.shape === shape.shape;
    })
  ) {
    return;
  }
  const closure = upstreamClosure(
    rt,
    shape.terminals.valuesArray().map((t) => t.output.node),
  );
  const affected = rt.graph.affectedNodesThroughRoutes([], [sourceKey]);
  for (const node of affected) if (!closure.has(node)) return;
  rt.pendingBindingRetractions = rt.pendingBindingRetractions.filter((p) => BSK.key(p.key) !== sk);
}

function upstreamClosure(rt: IvmRuntime, roots: NodeId[]): Set<NodeId> {
  const closure = new Set<NodeId>();
  const pending = roots.slice();
  while (pending.length) {
    const node = pending.pop()!;
    if (closure.has(node)) continue;
    closure.add(node);
    const graphNode = rt.graph.node(node);
    if (graphNode) pending.push(...graphNode.descriptor.inputs);
  }
  return closure;
}

/** Whether any binding currently holds the prepared binding source `shape`. */
export function preparedBindingSourceIsBound(rt: IvmRuntime, shape: string): boolean {
  return (rt.bindingSources.get(BSK.prepared(shape))?.refcounts.size ?? 0) > 0;
}

/** The shared nodes a live attach may borrow, or undefined when uncertain. */
function liveAttachBorrowedNodes(
  rt: IvmRuntime,
  shapeId: PreparedShapeId,
  bindingValues: Value[],
): Set<NodeId> | undefined {
  const shape = rt.preparedShapes.get(shapeId);
  if (!shape) throw IvmRuntimeError.of("PreparedShapeNotFound", shapeId);
  const bindingKey: BindingKey = IvmRuntimeError.wrapRecord(() =>
    shape.bindingDescriptor.create(bindingValues),
  );
  const sourceKey = BSK.prepared(shape.shape);
  const source = rt.bindingSources.get(sourceKey);
  if (!source) return undefined;
  if (
    source.refcounts.size === 0 ||
    source.refcounts.has(bk(bindingKey)) ||
    rt.pendingBindingRetractions.some((p) => BSK.key(p.key) === BSK.key(sourceKey)) ||
    hasPendingIncremental(rt) ||
    [...rt.multisinkSubscriptions.values()].some((s) => !s.sender.receiverAlive())
  ) {
    return undefined;
  }
  // A retained, completed sibling of the same terminals keeps shared nodes current.
  const sharedOutputs = [...shape.terminals].map(([sink, t]) => [sink, t.output.node] as const);
  const maintained = [...rt.multisinkSubscriptions.values()].some((s) => {
    if (s.target.kind !== "RoutedShape" || s.failed) return false;
    const sibling = rt.preparedShapes.get(s.target.shapeId);
    return (
      !!sibling &&
      sibling.shape === shape.shape &&
      sibling.terminals.size === sharedOutputs.length &&
      sharedOutputs.every(([sink, node]) => sibling.terminals.get(sink)?.output.node === node)
    );
  });
  if (!maintained) return undefined;
  return upstreamClosure(
    rt,
    sharedOutputs.map(([, node]) => node),
  );
}

function addBindingRefForShape(rt: IvmRuntime, shape: string, binding: BindingKey): BindingDelta {
  const source = rt.bindingSources.getMut(BSK.prepared(shape));
  if (!source) throw IvmRuntimeError.of("BindingSourceNotFound", shape);
  const entry = source.refcounts.get(bk(binding)) ?? [binding, 0];
  entry[1]++;
  source.refcounts.set(bk(binding), entry);
  return {
    key: BSK.prepared(shape),
    descriptor: source.descriptor,
    deltas: entry[1] === 1 ? [{ record: binding, weight: 1 }] : [],
    initializesSnapshot: false,
  };
}

function removeBindingRef(
  rt: IvmRuntime,
  shapeId: PreparedShapeId,
  binding: BindingKey,
): BindingDelta | undefined {
  const shape = rt.preparedShapes.get(shapeId)?.shape;
  return shape === undefined
    ? undefined
    : removeBindingRefForSource(rt, BSK.prepared(shape), binding);
}

function removeBindingMetadata(
  rt: IvmRuntime,
  sourceKey: BindingSourceKey,
  binding: BindingKey,
): [RecordDescriptor, boolean] | undefined {
  const source = rt.bindingSources.getMut(sourceKey);
  const entry = source?.refcounts.get(bk(binding));
  if (!source || !entry) return undefined;
  entry[1]--;
  const last = entry[1] === 0;
  if (last) source.refcounts.delete(bk(binding));
  return [source.descriptor, last];
}

export function removeBindingRefForSource(
  rt: IvmRuntime,
  sourceKey: BindingSourceKey,
  binding: BindingKey,
): BindingDelta | undefined {
  const removed = removeBindingMetadata(rt, sourceKey, binding);
  if (!removed) return undefined;
  const [descriptor, last] = removed;
  return {
    key: sourceKey,
    descriptor,
    deltas: last ? [{ record: binding, weight: -1 }] : [],
    initializesSnapshot: false,
  };
}

function bindingSourceShapeName(rt: IvmRuntime, shapeId: PreparedShapeId): string {
  const shape = rt.preparedShapes.get(shapeId);
  if (!shape) throw IvmRuntimeError.of("PreparedShapeNotFound", shapeId);
  return shape.shape;
}

export function bindingSnapshotDeltas(rt: IvmRuntime): BindingSnapshots {
  return rt.bindingSources.snapshot();
}

/** Retire a binding-owned shape once no retained binding targets it. */
function removeUnreferencedBindingOwnedShape(rt: IvmRuntime, shapeId: PreparedShapeId): void {
  if (!rt.preparedShapes.get(shapeId)?.retireWhenUnbound) return;
  for (const s of rt.multisinkSubscriptions.values()) if (targetsShape(s, shapeId)) return;
  try {
    retirePreparedShape(rt, shapeId);
  } catch {
    // Already gone or still bound: nothing to retire.
  }
}

function removeUnreferencedAutoFamily(rt: IvmRuntime, shapeId: PreparedShapeId): void {
  const shape = rt.preparedShapes.get(shapeId);
  const key = shape?.autoFamilyKey;
  if (!shape || !key) return;
  for (const s of rt.multisinkSubscriptions.values()) if (targetsShape(s, shapeId)) return;
  rt.preparedShapes.delete(shapeId);
  rt.bindingSources.remove(BSK.prepared(shape.shape));
  rt.autoDirectFamilies.delete(key);
  for (const [, terminal] of shape.terminals) {
    removeRetainer(rt, terminal.output.node, { kind: "PreparedShape", key: String(shapeId) });
  }
  collectUnretainedEphemeralNodes(rt);
}
