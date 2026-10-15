// Synchronous IVM graph runtime: executable state for hash-consed graphs.
import { JazzError, KeyedMap, assert } from "../../../support/index.js";
import {
  type NodeId,
  type RecordDescriptor,
  type DatabaseSchema,
  type IndexSchema,
  type MapProjectOp,
  type PreparedProjection,
  type VariantProjectionTarget,
  type BindingSourceKey,
  type FrontierName,
  type ArrangementState,
  type OperatorState,
  type NodeRuntimeMeta,
  IvmGraph,
  OwnedChunkProvider,
  ProjectionPlans,
  CompilationCache,
  EvaluationMemo,
  SharedIndexIds,
  InputSourceId,
  RecordsError,
  StorageError,
  ChunkError,
  LargeValueError,
  defineSchemaIndexVariantProjections,
  descriptorsEqual,
  mapProjectOpsEqual,
  variantProjectionTargetKey,
  BSK,
} from "../../../_extern/groove-ivm-tick.js";
import { PendingIncrementalEvaluation } from "./runtime_tick.js";
import {
  BindingSources,
  addDedupSchemaIndices,
  type AutoDirectFamilyKey,
  type BindingDelta,
  type MultisinkSubscriptionState,
  type PreparedShapeId,
  type PublicationId,
  type QueuedMultisinkDeltas,
  type RoutedMultisinkShapeState,
  type SubscriptionId,
  autoDirectFamilyKeyHash,
} from "./subscriptions.js";
import {
  type ArrangementKey,
  type AsOf,
  type EvalMemoEntry,
  type EvalMemoKey,
  type OperatorStateKey,
  type SubTick,
  arrangementKeyString,
  evalMemoKeyString,
  operatorStateKeyString,
} from "./state.js";

export * from "./state.js";
export * from "./terminal.js";
export * from "./subscriptions.js";

const DEFAULT_SINK = "__default";
export const EVAL_MEMO_MAX_ENTRIES = 8192;
export const EVAL_MEMO_MAX_BYTES = 128 * 1024 * 1024;
export { DEFAULT_SINK };

let nextInputSourceRuntimeNamespace = 1;

export type VariantProjectionKey = { table: string; target: VariantProjectionTarget };
export const variantProjectionKeyString = (k: VariantProjectionKey) =>
  `${k.table}\0${variantProjectionTargetKey(k.target)}`;

export type VariantProjection = {
  output: RecordDescriptor;
  cases: Map<number, VariantProjectionCase>;
};

export type VariantProjectionCase =
  | {
      kind: "Project";
      source: RecordDescriptor;
      project: MapProjectOp;
      rawProjection?: PreparedProjection;
      omitUnrepresentableEnumRows: boolean;
    }
  | { kind: "Ignore"; source: RecordDescriptor }
  | {
      kind: "Enum";
      source: RecordDescriptor;
      tag: number;
      payload: RecordDescriptor;
      project: MapProjectOp;
      rawProjection?: PreparedProjection;
    };

export function variantProjectionCaseSource(c: VariantProjectionCase): RecordDescriptor {
  return c.source;
}

/** A raw source case may refresh only when its program is unchanged and its input evolves append-only. */
export function canRefreshRegistriesTo(
  current: VariantProjectionCase,
  next: VariantProjectionCase,
): boolean {
  if (current.kind === "Project" && next.kind === "Project") {
    return (
      current.omitUnrepresentableEnumRows === next.omitUnrepresentableEnumRows &&
      mapProjectOpsEqual(current.project, next.project) &&
      current.source.canEvolveRegistryTo(next.source)
    );
  }
  if (current.kind === "Ignore" && next.kind === "Ignore")
    return current.source.canEvolveRegistryTo(next.source);
  return false;
}

const MESSAGES: Record<string, (d: any) => string> = {
  PersistenceOutcomeIndeterminate: () =>
    "runtime persistence outcome is indeterminate; reopen before continuing",
  AggregateOverflow: () => "aggregate result exceeds its declared numeric width",
  GraphFieldNotFound: (d) => `graph field not found: ${d}`,
  AmbiguousGraphFieldReference: (d) =>
    `exact graph field reference cannot be represented unambiguously by name: ${d}`,
  GraphFieldIndexOutOfBounds: (d) => `graph field index out of bounds: ${d}`,
  GraphInputArityMismatch: (d) => `graph node has unexpected input arity: ${d}`,
  GraphInputMissing: (d) => `graph node is missing input: ${d}`,
  GraphNodeNotFound: (d) => `graph node not found: ${d}`,
  GraphCycle: (d) => `graph contains a dependency cycle at node: ${d}`,
  GraphOutputMismatch: () => "graph output descriptors do not match",
  EnumTagProjectionAbsent: (d) => `enum tag ${d.tag} is absent from this projection target`,
  EnumTagProjectionNonEnum: () => "enum tag projection requires an enum value",
  EnumProjectionAbsent: (d) => `payload enum tag ${d.tag} is absent from this projection target`,
  EnumProjectionNonEnum: () => "payload enum projection requires an enum value",
  IndexNotFound: (d) => `index not found: ${d}`,
  InvalidPersistedIndex: (d) => `invalid persisted index entry: ${d}`,
  UnsupportedIndexIntersectionScan: () =>
    "intersected index sources currently require prefix scans",
  UnsupportedIndexCandidateFilter: () =>
    "candidate-filtered index sources require snapshot row projection and prefix scans",
  UnsupportedLookup: (d) => `unsupported keyed lookup: ${d}`,
  JoinKeyArityMismatch: (d) => `join key arity mismatch: left=${d.left}, right=${d.right}`,
  ShapeKeyFieldNotFound: (d) => `shape key field not found: ${d}`,
  MissingPrimaryKey: (d) => `table has no primary key: ${d}`,
  NodeStateMissing: (d) => `runtime node state missing: ${d}`,
  NodeStateOperatorMismatch: (d) => `runtime node state operator mismatch: ${d}`,
  OutOfOrderRuntimeState: (d) =>
    `runtime state advanced out of order: current=${d.current}, next=${d.next}`,
  PersistRecordMismatch: () => "persist node expected key/value bytes",
  BindingSourceRequiresPrepare: () =>
    "binding sources can only be evaluated through prepared shapes",
  EmptyMultisinkSubscription: () => "multisink subscription must have at least one sink",
  DuplicateMultisinkSink: (d) => `multisink sink already exists: ${d}`,
  MultisinkSinkRequiresPrepare: (d) =>
    `multisink sink requires prepare because it contains a binding source: ${d}`,
  RoutedMultisinkRouteArityMismatch: (d) =>
    `routed multisink sink ${d.sink} has ${d.actual} route fields, expected ${d.expected}`,
  BindingSourceNotFound: (d) => `binding source not found: ${d}`,
  BindingSourceDescriptorMismatch: (d) => `binding source descriptor mismatch: ${d}`,
  ForeignInputSource: () => "input source belongs to a different IVM runtime",
  InputSourceRetired: () => "input source has been retired",
  DuplicateTableVariant: (d) => `duplicate schema version ${d.version} for table ${d.table}`,
  RecordEncoding: (d) => String(d?.message ?? d),
  RecursiveIterationLimit: (d) => `recursive node ${d.node} exceeded iteration limit ${d.maxIters}`,
  Storage: (d) => String(d?.message ?? d),
  Chunk: (d) => String(d?.message ?? d),
  LargeValue: (d) => String(d?.message ?? d),
  StorageUnavailable: () => "storage unavailable for durable node",
  EvaluationBlocked: () => "evaluation blocked on a non-resident storage input",
  InvalidStreamingChecksumBudget: () =>
    "streaming checksum window and work budgets must be non-zero",
  StreamingChecksumTypeMismatch: () => "streaming checksum requires a String or Bytes field",
  PreparedShapeNotFound: (d) => `subscription shape not found: PreparedShapeId(${d})`,
  PreparedShapeHasActiveBindings: (d) =>
    `cannot retire prepared shape PreparedShapeId(${d}) while it has active bindings`,
  StaleRuntimeState: (d) =>
    `runtime state is stale: expected=${d.expected}, actual=${d.actual === undefined ? "None" : `Some(${JSON.stringify(d.actual)})`}`,
  TableNotFound: (d) => `table not found: ${d}`,
  TableAlreadyExists: (d) => `table already exists: ${d}`,
  TableFieldAlreadyExists: (d) =>
    `field already exists in the live catalogue: ${d.table}.${d.field}`,
  UnknownTableVariant: (d) => `unknown schema version ${d.version} for table ${d.table}`,
  VariantProjectionNotFound: (d) => `variant projection not found: ${d.table}.${d.target}`,
  VariantProjectionRequired: (d) => `schema-variant table requires a fixed-output projection: ${d}`,
  VariantProjectionOutputMismatch: (d) =>
    `variant projection output descriptor mismatch: ${d.table}.${d.target}`,
  VariantProjectionCaseAlreadyRegistered: (d) =>
    `variant projection case already registered with different semantics: ${d.table}.${d.target} version ${d.version}`,
  VariantProjectionCaseNotFound: (d) =>
    `variant projection case not found: ${d.table}.${d.target} version ${d.version}`,
  VariantProjectionSourceMismatch: (d) =>
    `variant projection source descriptor mismatch: ${d.table}.${d.target} version ${d.version}`,
  VariantProjectionEnumFieldNotFound: (d) =>
    `variant projection enum field not found: ${d.table}.${d.target}.${d.field}`,
  VariantProjectionEnumOutputMustBeSingleField: (d) =>
    `variant projection enum output must contain exactly one field: ${d.table}.${d.target}`,
  VariantProjectionEnumFieldTypeMismatch: (d) =>
    `variant projection field is not an enum: ${d.table}.${d.target}.${d.field}`,
  VariantProjectionEnumSchemaMismatch: (d) =>
    `variant projection enum schema does not match fixed output: ${d.table}.${d.target}.${d.field}`,
  VariantProjectionEnumPayloadMismatch: (d) =>
    `variant projection payload does not match enum case: ${d.table}.${d.target}.${d.case}`,
  RecursiveEnumProjectionDescriptorMismatch: (d) =>
    `recursive enum projection descriptor mismatch at ${d.path}`,
  VariantProjectFieldTypeMismatch: (d) => `enum match field is not an enum: ${d.field}`,
  VariantProjectPayloadMismatch: (d) =>
    `enum match payload descriptor does not match its declared case: ${d.field}`,
  UniqueIndexViolation: (d) => `unique index violation: ${d.index}`,
  UnsupportedJoinKey: () => "unsupported join key",
  UnsupportedNonMonotoneRecursion: () =>
    "non-monotone recursive delta reached positive-only incremental recursion",
  UnsupportedNestedRecursion: () => "nested recursive graphs are not supported in v0",
  UnsupportedArgMaxBy: (d) => `unsupported arg_by graph: ${d}`,
  CollectByMustBeTerminal: () => "collect_by is terminal-only and cannot feed another graph node",
  InvalidCollectBy: (d) => `invalid collect_by descriptor: ${d}`,
  InvalidTopBy: (d) => `invalid top_by descriptor: ${d}`,
  DuplicateCollectByOccurrenceId: () =>
    "collect_by expand encountered duplicate output occurrence source ids",
  UnsupportedOperator: () => "unsupported operator",
};

export type IvmRuntimeErrorKind = keyof typeof MESSAGES;

export class IvmRuntimeError extends JazzError {
  constructor(
    kind: string,
    readonly data?: any,
  ) {
    super(kind, MESSAGES[kind](data), data instanceof Error ? data : undefined);
  }
  static of(kind: string, data?: any): IvmRuntimeError {
    return new IvmRuntimeError(kind, data);
  }
  static is(e: unknown, kind: string): e is IvmRuntimeError {
    return e instanceof IvmRuntimeError && e.kind === kind;
  }
  /** `#[from]` conversions for transparent foreign errors. */
  static from(e: unknown): IvmRuntimeError {
    if (e instanceof IvmRuntimeError) return e;
    if (e instanceof RecordsError) return new IvmRuntimeError("RecordEncoding", e);
    if (e instanceof ChunkError) return new IvmRuntimeError("Chunk", e);
    if (e instanceof LargeValueError) return new IvmRuntimeError("LargeValue", e);
    if (e instanceof StorageError) return new IvmRuntimeError("Storage", e);
    throw e;
  }
  static wrapRecord<T>(f: () => T): T {
    try {
      return f();
    } catch (e) {
      throw IvmRuntimeError.from(e);
    }
  }
  static async wrapStorageAsync<T>(f: () => Promise<T>): Promise<T> {
    try {
      return await f();
    } catch (e) {
      throw IvmRuntimeError.from(e);
    }
  }
}

const opKey = operatorStateKeyString;

/** Stateful executor for deduplicated IVM graphs and subscriptions. */
export class IvmRuntime {
  schema: DatabaseSchema;
  chunkProvider = new OwnedChunkProvider();
  tableStorageDescriptors = new Map<string, RecordDescriptor>();
  tableDescriptors = new Map<string, RecordDescriptor>();
  variantDescriptors = new Map<string, Map<number, RecordDescriptor>>();
  variantProjections = new KeyedMap<VariantProjectionKey, VariantProjection>(
    variantProjectionKeyString,
  );
  graph = new IvmGraph();
  projectionPlans = new ProjectionPlans();
  compilationCache = new CompilationCache();
  multisinkSubscriptions = new Map<SubscriptionId, MultisinkSubscriptionState>();
  subscriptionsByOutputNode = new Map<NodeId, Set<SubscriptionId>>();
  pendingIncremental = new PendingIncrementalEvaluation();
  pendingIncrementalPolling = false;
  ownerProgressWaker: (() => void) | undefined;
  ownerFanoutWaker: [() => void, () => void] | undefined;
  ephemeralGraphGcPending = false;
  gcCandidates = new Set<NodeId>();
  preparedShapes = new Map<PreparedShapeId, RoutedMultisinkShapeState>();
  autoDirectFamilies = new KeyedMap<AutoDirectFamilyKey, PreparedShapeId>(autoDirectFamilyKeyHash);
  sharedPreparedShapes = new Map<string, PreparedShapeId>();
  bindingSources = new BindingSources();
  inputSourceRuntimeNamespace: number;
  nextInputSourceId = 1;
  pendingBindingRetractions: BindingDelta[] = [];
  liveAttaches = 0;
  deferredNotifications = new Map<PublicationId, [SubscriptionId, QueuedMultisinkDeltas][]>();
  durableNotificationPublications = new Set<PublicationId>();
  completedDeferredPublications = new Set<PublicationId>();
  persistenceIndeterminate = { value: false };
  indexIds: SharedIndexIds;
  operatorStates = new KeyedMap<OperatorStateKey, OperatorState>(opKey);
  arrangementStates = new KeyedMap<ArrangementKey, AsOf<ArrangementState, SubTick>>(
    arrangementKeyString,
  );
  arrangementKeysByInput = new Map<NodeId, KeyedMap<ArrangementKey, ArrangementKey>>();
  evalMemoKeysByNode = new Map<NodeId, KeyedMap<EvalMemoKey, EvalMemoKey>>();
  evalMemo = new EvaluationMemo();
  tableFrontiers = new Map<string, number>();
  bindingFrontiers = new KeyedMap<BindingSourceKey, number>(BSK.key);
  memoUseClock = 0;
  evalMemoBytes = 0;
  hydrationMemoHits = 0;
  hydrationMemoComputes = 0;
  hydrationMemoComputedNodes = new Set<NodeId>();
  nodeMeta = new Map<NodeId, NodeRuntimeMeta>();
  currentTick = 0;
  nextSubscriptionId = 1;
  nextShapeId = 1;
  logicalNodesRequested = 0;
  autoDirectFamilyEnabled = true;
  plainOutputRootPositions = true;
  collectTickRuntimeStats = false;

  constructor(schema: DatabaseSchema, indexIds: SharedIndexIds = new SharedIndexIds()) {
    this.schema = schema;
    this.indexIds = indexIds;
    for (const table of schema.tables) {
      this.tableStorageDescriptors.set(table.name, table.recordSchema());
      if (!table.hasVariants()) {
        this.tableDescriptors.set(table.name, table.recordSchema());
        continue;
      }
      const descriptors = new Map<number, RecordDescriptor>();
      for (const variant of table.variants) {
        const descriptor = table.recordSchemaForVariant(variant.tag);
        if (descriptor) descriptors.set(variant.tag, descriptor);
      }
      this.variantDescriptors.set(table.name, descriptors);
    }
    this.inputSourceRuntimeNamespace = nextInputSourceRuntimeNamespace++;
    defineSchemaIndexVariantProjections(this);
    addDedupSchemaIndices(this);
  }

  static new(schema: DatabaseSchema): IvmRuntime {
    return new IvmRuntime(schema);
  }
  static newWithIndexIds(schema: DatabaseSchema, indexIds: SharedIndexIds): IvmRuntime {
    return new IvmRuntime(schema, indexIds);
  }

  insertRetainedEvalMemo(key: EvalMemoKey, entry: EvalMemoEntry): void {
    assert(key.tickEpoch === undefined);
    let keys = this.evalMemoKeysByNode.get(key.node);
    if (!keys) this.evalMemoKeysByNode.set(key.node, (keys = new KeyedMap(evalMemoKeyString)));
    keys.set(key, key);
    const previous = this.evalMemo.insert(key, entry);
    if (previous) this.evalMemoBytes = Math.max(0, this.evalMemoBytes - previous.payloadBytes);
    this.evalMemoBytes += entry.payloadBytes;
  }

  removeRetainedEvalMemo(key: EvalMemoKey): void {
    const entry = this.evalMemo.remove(key);
    if (entry) this.evalMemoBytes = Math.max(0, this.evalMemoBytes - entry.payloadBytes);
    const keys = this.evalMemoKeysByNode.get(key.node);
    if (keys) {
      keys.delete(key);
      if (keys.size === 0) this.evalMemoKeysByNode.delete(key.node);
    }
  }

  removeRetainedEvalMemosForNodes(nodes: Iterable<NodeId>): void {
    for (const node of nodes) {
      const keys = this.evalMemoKeysByNode.get(node);
      if (!keys) continue;
      this.evalMemoKeysByNode.delete(node);
      for (const [key] of keys) {
        const entry = this.evalMemo.remove(key);
        if (entry) this.evalMemoBytes = Math.max(0, this.evalMemoBytes - entry.payloadBytes);
      }
    }
  }

  /** Allocate one opaque, runtime-local mutable input identity. */
  allocateInputSource(descriptor: RecordDescriptor): InputSourceId {
    const id = new InputSourceId(this.inputSourceRuntimeNamespace, this.nextInputSourceId);
    if (this.nextInputSourceId >= Number.MAX_SAFE_INTEGER)
      throw new Error("input source IDs must not wrap within one runtime");
    this.nextInputSourceId++;
    const key = id.bindingKey();
    if (this.bindingSources.containsKey(key))
      throw new Error("fresh input source identity already has state");
    this.bindingSources.insert(key, { descriptor, refcounts: new Map(), initialized: false });
    return id;
  }

  setChunkProvider(provider: any): void {
    this.chunkProvider = new OwnedChunkProvider(provider);
  }
  setOwnedChunkProvider(provider: OwnedChunkProvider): void {
    this.chunkProvider = provider;
  }
  setTickRuntimeStatsEnabled(enabled: boolean): void {
    this.collectTickRuntimeStats = enabled;
  }
  setAutoDirectFamilyEnabled(enabled: boolean): void {
    this.autoDirectFamilyEnabled = enabled;
  }
  setPlainOutputRootPositionsEnabled(enabled: boolean): void {
    this.plainOutputRootPositions = enabled;
  }
  table(table: string) {
    return this.schema.table(table);
  }
  tableDescriptor(table: string): RecordDescriptor | undefined {
    return this.tableDescriptors.get(table);
  }
  indexStorageId(table: string, index: IndexSchema): number {
    return this.indexIds.resolve(table, index);
  }
  tableStorageDescriptor(table: string): RecordDescriptor | undefined {
    return this.tableStorageDescriptors.get(table);
  }
  recordDescriptor(table: string, variantTag: number): RecordDescriptor | undefined {
    const plain = variantTag === 0 ? this.tableDescriptors.get(table) : undefined;
    return plain ?? this.variantDescriptors.get(table)?.get(variantTag);
  }
}

/** Freezes a batch output buffer, copying it down when mostly unused. */
export function freezeBatchBuffer(buffer: Uint8Array, len: number): Uint8Array {
  return buffer.length - len > len / 2 ? buffer.slice(0, len) : buffer.subarray(0, len);
}
