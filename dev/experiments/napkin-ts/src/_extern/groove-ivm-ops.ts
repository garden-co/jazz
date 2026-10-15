import { extern } from "../support/runtime.js";

// groove records (groove-records)
export type RecordDescriptor = any;
export type Value = any;
export type ValueType = any;
export type BorrowedRecord = any;
export type PreparedProjection = any;
export type PreparedRecordCopy = any;
export const BorrowedRecord = extern<{ new (raw: Uint8Array, desc: RecordDescriptor): any }>();
export const PreparedRecordCopy = extern<{ new (inputs: any[], output: any, mapping: any): any }>();
export const OwnedRecord = extern<any>();
export const recordsError = extern<(kind: string, detail?: any) => Error>();
export const collectByOrderedScalar = extern<(value: Value) => any>();
export const largeValues = extern<{
  materializeRecordAttempt(desc: any, raw: Uint8Array, inputs: any): Uint8Array;
  materializeRecordFieldsAttempt(
    desc: any,
    raw: Uint8Array,
    fields: number[],
    inputs: any,
  ): Uint8Array;
  byteRangeAttempt(value: any, range: any, inputs: any): Uint8Array;
  StreamingChecksum: { new (value: any, windowBytes: number, maxBytesPerTurn: number): any };
  verifyChunkHash(bytes: Uint8Array, expected: Uint8Array): boolean;
}>();
export type ChunkRequest = any;
export type ChunkLease = any;
export type DatabaseSchema = any;
export type TableSchema = any;
export type IndexSchema = any;

// groove storage (groove-db-storage)
export type OrderedKvStorage = any;
export type ScanRequest = any;
export const ScanRequest = extern<any>();
export const storageKeyCodec = extern<any>();

// ivm plan (groove-ivm-plan)
export type NodeId = number;
export type IvmGraph = any;
export type GraphNode = any;
export type OpType = any;
export type PlanExpr = any;
export type AggregateExpr = any;
export type AggregateOp = any;
export type TopByOp = any;
export type CollectByOp = any;
export type CollectBySlot = any;
export type JoinOp = any;
export type LookupOp = any;
export type MapProjectOp = any;
export type FilterOp = any;
export type RecursiveOp = any;
export type StreamingChecksumOp = any;
export type FrontierSourceOp = any;
export type TableSourceOp = any;
export type IndexSourceOp = any;
export type IndexByOp = any;
export type PersistOp = any;
export type ExecutionLayout = any;
export type Retainer = any;
export type InputSourceId = any;
export const GraphBuilder = extern<any>();
export const resolveFieldName =
  extern<(desc: RecordDescriptor, name: string) => number | undefined>();
export const rawProjectionFields = extern<(project: any, input: any, output: any) => any>();
export const remapEnumTag = extern<(value: Value, tags: any) => Value>();
export const remapEnum = extern<(value: Value, tags: any) => Value>();
export const encodeKeyPart = extern<(out: number[], value: Value) => void>();
export const encodeRecordFieldKeyPart =
  extern<(out: number[], desc: any, raw: Uint8Array, field: number) => void>();
export const primaryKeyFieldIndices = extern<(table: TableSchema, output: any) => number[]>();
export const scanBounds = extern<(scan: any) => any>();
export const projectRecord =
  extern<(project: any, input: BorrowedRecord, output: any, raw?: any) => Uint8Array>();
export const compilation = extern<any>();
export const keyEncoding = extern<any>();
export const indexIds = extern<any>();
export const typedTemplate = extern<any>();

// ivm tick (groove-ivm-tick)
export type RecordDelta = { record: Uint8Array; weight: number };
export type RecordDeltas = { descriptor: RecordDescriptor; deltas: RecordDelta[] };
export type TableDelta = any;
export type BindingDelta = any;
export type BindingSnapshots = any;
export type TerminalDeltas = { operations: any[] };
export type TerminalOperation = any;
export type IvmRuntime = any;
export type TickMetrics = any;
export type RuntimeStats = any;
export type ScopeId = any;
export type EvalContext = any;
export type SubTick = { tick: number; subTick: number };
export type EvalMemoKey = any;
export type EvalMemoEntry = any;
export type NodeInputSignature = any;
export type OperatorStateKey = { scope: ScopeId; node: NodeId };
export type ArrangementKey = { scope: ScopeId; input: NodeId };
export type RootIdentity = any;
export const IvmRuntimeError = extern<{
  new (kind: string, detail?: any): Error & { kind: string };
}>();
export const emptyDeltas = extern<(desc: RecordDescriptor) => RecordDeltas>();
export const AsOf = extern<{ new <T>(value: T): any }>();
export const ScopeId = extern<{ root(): ScopeId; eq(a: ScopeId, b: ScopeId): boolean }>();
export const EvalContext = extern<{
  root(): EvalContext;
  withBinding(scope: ScopeId, subTick: number, binding: any, deltas: RecordDeltas): EvalContext;
  withBindingAndArrangementMode(
    scope: ScopeId,
    subTick: number,
    binding: any,
    deltas: RecordDeltas,
    mode: string,
  ): EvalContext;
}>();
export const NodeInputSignature = extern<{
  fromSets(tables: Set<string>, bindings: any, frontier: any): any;
  empty(): any;
}>();
export const EvalMemoEntry = extern<{
  new (records: RecordDeltas, watermark: number, bytes: number, used: number): any;
}>();
export const evalMemoKeyHash = extern<(key: EvalMemoKey) => string>();
export const subTickCmp = extern<(a: SubTick, b: SubTick) => number>();
export const consolidateDeltas = extern<(deltas: RecordDelta[]) => RecordDelta[]>();
export const recordDeltasEncodedBytes = extern<(deltas: RecordDeltas) => number>();
export const applyChangedGroupOrdering =
  extern<(changes: any, desc: any, terminal: TerminalDeltas) => boolean>();
export const applyGroupWindowOrdering =
  extern<
    (
      before: any[],
      after: any[],
      keyOf: (r: Uint8Array) => Uint8Array,
      desc: any,
      terminal: TerminalDeltas,
    ) => void
  >();
export const projectWindowRecord =
  extern<
    (graph: any, chain: any, desc: any, record: Uint8Array) => [any, Uint8Array] | undefined
  >();
export const windowRecordReachesOutput =
  extern<(graph: any, chain: any, desc: any, record: Uint8Array) => "Yes" | "No" | "Unknown">();
export const topByIdentityFields = extern<(topBy: TopByOp, len: number) => number[]>();
export const outputIsStructuredCollectBy = extern<(graph: any, node: NodeId) => boolean>();
export const terminalOccurrenceKey = extern<(...args: any[]) => Uint8Array>();
export const terminalOps = extern<any>();
export const terminalDeltasFromRecordDeltas = extern<(...args: any[]) => any>();
export const SubscriptionError = extern<any>();
export const PendingIncrementalEvaluation = extern<any>();

// groove db (groove-db-storage)
export const persistedIndexPrimaryKey =
  extern<
    (table: any, index: string, schema: any, key: Uint8Array, value: Uint8Array) => Uint8Array
  >();
export const persistedIndexColumnValue =
  extern<
    (
      table: any,
      index: string,
      schema: any,
      key: Uint8Array,
      value: Uint8Array,
      column: string,
    ) => Value
  >();
export type KeyValue = [Uint8Array, Uint8Array];
export type OwnedStorage = any;
export type OwnedChunkProvider = any;

// misc plan/runtime helpers used by operator updates (groove-ivm-plan / groove-ivm-tick / groove-records)
type Fn = (...args: any[]) => any;
export const scanMaxItems = extern<Fn>();
export const scanReversed = extern<Fn>();
export const persistedIndexScanBounds = extern<Fn>();
export const durableIndexKeyPrefix = extern<(index: any) => Uint8Array>();
export const indexRecordFromStorage =
  extern<(prefixLen: number, key: Uint8Array, value: Uint8Array) => Uint8Array>();
export const primaryKeyValueBytes = extern<Fn>();
export const keyMatchesStaticScan = extern<Fn>();
export const applyIndexBy = extern<Fn>();
export const projectBindingSourceDeltas = extern<Fn>();
export const projectFieldValue = extern<Fn>();
export const encodeProjectionFieldValue =
  extern<(desc: any, index: number, value: Value) => Uint8Array>();
export const projectRecordRaw =
  extern<(exprs: any, mapping: any, output: any, input: any, raw: Uint8Array) => Uint8Array>();
export const variantProjectionTargetName = extern<(target: any) => string>();
export const splitVariantRecord = extern<(stored: Uint8Array) => [number, Uint8Array]>();
export const EnumValue = extern<{ new (tag: number, record: any): any }>();
export const freezeBatchBuffer = extern<(parts: Uint8Array[]) => Uint8Array>();
export const PreparedProjection = extern<{
  new (source: any, output: any, fields: any[]): { reusesInput: boolean };
}>();
export const decodeSingleFieldValue = extern<(bytes: Uint8Array, ty: ValueType) => Value>();
export const recordFieldLiteralOrdering = extern<(record: any, index: number, value: any) => any>();
export const compositeKeys =
  extern<
    (
      prefix: number[],
      values: Iterable<Value>,
      encode: (key: number[], value: Value) => void,
    ) => number[][]
  >();
export const expandsToKeyParts = extern<(ty: ValueType) => boolean>();
export const integerValue = extern<(value: Value) => bigint | undefined>();
export const resolveFieldRef = extern<(desc: RecordDescriptor, field: any) => number>();
export const FieldRef = extern<{ storedName(name: string): any }>();
export const encodeScalarKeyPart = extern<(key: number[], value: Value) => void>();
export const encodeOrderedBytes = extern<(key: number[], bytes: Uint8Array) => void>();
export const encodeRecordDescriptor = extern<(desc: RecordDescriptor) => Uint8Array>();
export const isSqlNullValue = extern<(value: Value) => boolean>();
export const RecordDescriptor = extern<{ new (fields: [string, ValueType][]): any }>();
export const recordStoreForTable =
  extern<(storage: OrderedKvStorage, table: any, descriptor: RecordDescriptor) => any>();
