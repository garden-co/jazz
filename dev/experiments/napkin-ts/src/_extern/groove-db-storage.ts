// Symbols owned by other partitions, used by groove-db-storage.
import { extern } from "../support/runtime.js";

// --- idb_tree crate ---
export type PageStore = any;
export interface IdbTree {
  readCommitted(): IdbTree;
  reload(): Promise<void>;
  get(key: Uint8Array): Promise<Uint8Array | undefined> | Uint8Array | undefined;
  valueEquals(
    key: Uint8Array,
    expected: Uint8Array,
  ): Promise<boolean | undefined> | boolean | undefined;
  rangeLimit(
    start: Uint8Array,
    end: Uint8Array,
    limit: number,
  ): Promise<[Uint8Array, Uint8Array][]> | [Uint8Array, Uint8Array][];
  rangeReverse(
    start: Uint8Array,
    end: Uint8Array,
    limit: number,
  ): Promise<[Uint8Array, Uint8Array][]> | [Uint8Array, Uint8Array][];
  writeMany(writes: any[]): Promise<void>;
  flush(): Promise<void>;
}
export const IdbTree = extern<{ open(store: PageStore, options: any): Promise<IdbTree> }>();
export const isIdbTreeGenerationConflict = extern<(error: any) => boolean>();

// --- groove records ---
export type Record = any;

// Values are `{ kind: "U64", v: bigint }`-shaped (see groove-records); loosely typed here.
export type Value = any;
export type ValueType = any;
export type RecordDescriptor = any;
export const RecordDescriptor: any = extern<any>();
export type OwnedRecord = any;
export const OwnedRecord: any = extern<any>();
export type VariantRecord = any;
export const VariantRecord: any = extern<any>();
export const ensureValueType = extern<(value: Value, valueType: ValueType) => void>();
export const recordsArityMismatch = extern<(expected: number, actual: number) => unknown>();
export const recordsFieldNotFound = extern<(field: string) => unknown>();
export const splitVariantRecord = extern<(bytes: Uint8Array) => [number, Uint8Array]>();
export const encodeVariantRecord = extern<(tag: number, payload: Uint8Array) => Uint8Array>();
export const u48BeBytes = extern<(n: number) => Uint8Array>();
export const u48FromBeBytes = extern<(bytes: Uint8Array) => number>();
export const columnTypeOfKey = extern<(key: any) => ValueType>();

// --- schema ---
export type DatabaseSchema = any;
export type TableSchema = any;
export type IndexSchema = any;
export type PrimaryKeyColumn = any;
export type ColumnSchema = any;
export type EnumSchema = any;
export type ProjectField = any;
export type TableVariant = any;

// --- ivm runtime / query ---
export type Waker = () => void;
export const PENDING = extern<symbol>();
export type Poll<T> = T | symbol;
export type IvmRuntime = any;
export const IvmRuntime: any = extern<any>();
export type IndexIdRegistry = any;
export const IndexIdRegistry: any = extern<any>();
export type PublicationId = number;
export type TickMetrics = any;
export type PreparedShapeId = any;
export type SubscriptionId = any;
export type InputSourceId = any;
export type InputSourceDelta = any;
export type InputSourceReplacement = any;
export type QueryParameter = any;
export type Query = any;
export type GraphBuilder = any;
export type MultisinkSubscription = any;
export type Subscription = any;
export type RoutedMultisinkTerminal = any;
export type RecordDeltas = any;
export type TemplateGraphInput = any;
export const TemplateGraphInput: any = extern<any>();
export const planQuery = extern<(...args: any[]) => any>();
export const planPreparedShape = extern<(...args: any[]) => any>();
export const ivmErrorFrom = extern<(error: unknown) => any>();
export type EvaluationInputs = any;
export const EvaluationInputs: any = extern<any>();
export const splitDurableIndexKey = extern<(key: Uint8Array) => any>();
export const durableIndexKeyPrefix = extern<(...args: any[]) => Uint8Array>();
export const encodeKeyPart = extern<(out: number[], value: Value) => void>();

// --- large values ---
export type NodeRef = any;
export type LargeValueRef = any;
export const cmpNodeRef = extern<(a: NodeRef, b: NodeRef) => number>();
export const encodeNodeRef = extern<(n: NodeRef) => Uint8Array>();
export const decodeNodeRef = extern<(b: Uint8Array) => NodeRef>();
export const encodeLargeValueRef = extern<(v: LargeValueRef) => Uint8Array>();
export const decodeLargeValueRef = extern<(b: Uint8Array) => LargeValueRef>();
export const nodeRefDescriptor = extern<() => RecordDescriptor>();
export const nodeRefValue = extern<(n: NodeRef) => Value>();
export const nodeRefFromValue = extern<(v: Value) => NodeRef>();
export const decodeNodeUntypedAuthenticated = extern<(...args: any[]) => any>();
export type PhysicalTraversalNodeBudget = any;
export const PhysicalTraversalNodeBudget: any = extern<any>();
/** `crate::large_values` module surface (prepare, validators, edit attempts, cursors). */
export const largeValues = extern<any>();
/** blake3::derive_key */
export const deriveKey = extern<(context: string, material: Uint8Array) => Uint8Array>();

// --- chunks ---
export type ChunkStorage = any;
export type MissingChunkResolver = any;
export type PublicationInstallFailures = any;
export const PublicationInstallFailures: any = extern<any>();
export const ChunkError: any = extern<any>();
/** `crate::chunks` module surface (providers, managed/ordered storage, readers). */
export const chunks = extern<any>();
