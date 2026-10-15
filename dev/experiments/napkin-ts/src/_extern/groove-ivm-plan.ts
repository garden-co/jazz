import { extern } from "../support/runtime.js";

// groove records (groove-records)
export type RecordDescriptor = any;
export const RecordDescriptor = extern<any>();
export type DescriptorField = any;
export const DescriptorField = extern<any>();
export type BorrowedRecord = any;
export const BorrowedRecord = extern<any>();
export type OwnedRecord = any;
export const OwnedRecord = extern<any>();
export type EnumValue = any;
export const EnumValue = extern<any>();
export type PreparedProjection = any;
export const PreparedProjection = extern<any>();
export type FieldIdentity = any;
export type Value = any;
export type ValueType = any;
export type EnumSchema = any;
export type VariantRecord = any;
export const RecordsError = extern<any>();
export const encodeRecordDescriptor = extern<(d: RecordDescriptor) => Uint8Array>();
export const decodeRecordDescriptor = extern<(b: Uint8Array) => RecordDescriptor>();
export const encodeSingleFieldValue = extern<(value: Value, t: ValueType) => Uint8Array>();
export const collectByOrderedScalar = extern<(t: ValueType) => boolean>();
export const valueTypeContainsRecord = extern<(t: ValueType) => boolean>();
export const u48BeBytes = extern<(n: number) => Uint8Array>();

// schema / query AST (model-core, model-query)
export type DatabaseSchema = any;
export const DatabaseSchema = extern<any>();
export type TableSchema = any;
export type IndexSchema = any;
export type ColumnSchema = any;
export type TableVariant = any;
export type Query = any;
export type Expr = any;
export type Select = any;
export type SelectItem = any;
export type TableRef = any;
export const enumSchemaDiscriminant = extern<(schema: any, name: string) => number | undefined>();

// storage (groove-db-storage)
export type OrderedKvStorage = any;
export type OwnedWriteOperation = any;
export const OwnedWriteOperation = extern<any>();
export const ScanRequest = extern<any>();
export const StorageError = extern<any>();
export const collectScan = extern<(stream: any) => Promise<any[]>>();

// ivm runtime (groove-ivm-tick, groove-ivm-ops)
export type IvmRuntime = any;
export const IvmRuntime = extern<any>();
export const IvmRuntimeError = extern<any>();
export type CompiledNode = any;
export type RecordDelta = any;
export type RecordDeltas = any;
export type TerminalDeltas = any;
export type WindowedRecord = any;
export const NodeState = extern<any>();
export const HydrationMode = extern<any>();
export const RootIndirectValues = extern<any>();
export const EphemeralGraphInstall = extern<any>();
export const applyPersistDelta = extern<(...args: any[]) => Promise<void>>();
export const applyPersistDeltaWith = extern<(...args: any[]) => Promise<void>>();
export const pipelineSupportsNode = extern<(...args: any[]) => boolean>();
export const predicateMatches = extern<(...args: any[]) => boolean>();
export const predicateReferencedFields = extern<(predicate: any, out: Set<string>) => void>();
export const graphBuilderFingerprint = extern<(g: any) => bigint>();
export const graphBuildersEqual = extern<(a: any, b: any) => boolean>();
export const graphBuildersEqualWith =
  extern<(a: any, b: any, hook: (a: any, b: any) => boolean | undefined) => boolean>();
export const builderContainsBindingSource = extern<(g: any) => boolean>();
export const builderContainsRecursive = extern<(g: any) => boolean>();
export const countBuilderNodes = extern<(g: any) => number>();
export const joinOutputMapping = extern<(...args: any[]) => any>();
export const planExprNames = extern<(...args: any[]) => any>();
export const recursiveReadTables = extern<(...args: any[]) => any>();
export const resolveAggregateExpr = extern<(...args: any[]) => any>();
export const validateArgByPrimaryKeyIndices = extern<(...args: any[]) => any>();
export const variantCaseCanRefreshRegistriesTo = extern<(from: any, to: any) => boolean>();
