// Symbols owned by other partitions (groove, model, query, schema, tx).
import { extern } from "../support/runtime.js";
import type { Reader, Writer } from "../support/postcard.js";

type W<T = any> = (w: Writer, v: T) => void;
type R<T = any> = (r: Reader) => T;

// groove::records
export const RecordDescriptor = extern<any>();
export const ValueType = extern<any>();
export const Value = extern<any>();
export const OwnedRecord = extern<any>();
export const EnumSchema = extern<any>();
export const EnumCase = extern<any>();
export const EnumValue = extern<any>();
export const GrooveRecordError = extern<{
  NonCanonicalRecord(): Error;
  TypeMismatch(expected: any): Error;
  LengthOverflow(): Error;
}>();
export const encodePersistedRecordDescriptor = extern<(d: any) => Uint8Array>();
export const decodePersistedRecordDescriptor = extern<(b: Uint8Array) => any>();
export const encodeRecordDescriptor = extern<(d: any) => Uint8Array>();
export const descriptorKey = extern<(d: any) => string>();
export const writeValue = extern<W>();
export const readValue = extern<R>();
export const compareValue = extern<(a: any, b: any) => number>();

// groove::storage
export type OrderedKvStorage = any;
export const RecordStore = extern<any>();
export const ScanBounds = extern<any>();
export const StorageCodecProfile = extern<any>();
export const GROOVE_DURABLE_INDEX_V2_CODEC = extern<string>();

// groove::large_values
export const writeLocator = extern<W>();
export const readLocator = extern<R>();
export const writeLargeValueRef = extern<W>();
export const readLargeValueRef = extern<R>();
export const writeStagedChunk = extern<W>();
export const readStagedChunk = extern<R>();
export const writeNodeRef = extern<W>();
export const readNodeRef = extern<R>();
export const encodeStoredScalar = extern<(kind: any, scalar: any) => Uint8Array>();

// jazz-model: tx
export type TxId = { time: bigint; node: string };
export type Transaction = any;
export type Fate = any;
export type DurabilityTier = "None" | "Local" | "Global";
export type DeletionEvent = "Deleted" | "Restored";
export const writeTxId = extern<W<TxId>>();
export const readTxId = extern<R<TxId>>();
export const compareTxId = extern<(a: TxId, b: TxId) => number>();
export const writeTransaction = extern<W>();
export const readTransaction = extern<R>();
export const transactionEquals = extern<(a: any, b: any) => boolean>();
export const writeFate = extern<W>();
export const readFate = extern<R>();
export const fateEquals = extern<(a: any, b: any) => boolean>();
export const writeDurabilityTier = extern<W<DurabilityTier>>();
export const readDurabilityTier = extern<R<DurabilityTier>>();

// jazz-model: query
export type ShapeId = any;
export type BindingId = any;
export const writeShapeId = extern<W>();
export const readShapeId = extern<R>();
export const compareShapeId = extern<(a: any, b: any) => number>();
export const writeBindingId = extern<W>();
export const readBindingId = extern<R>();
export const compareBindingId = extern<(a: any, b: any) => number>();
export const writeQuery = extern<W>();
export const readQuery = extern<R>();
export const writeRelationQueryRequired = extern<W>();
export const readRelationQueryRequired = extern<R>();

// jazz-model: schema
export type JazzSchema = any;
export type TableSchema = any;
export const APP_COLUMN_PREFIX = extern<string>();
export const physicalColumnEpochIsCompatible =
  extern<(sourceTable: any, sourceName: string, targetTable: any, targetName: string) => boolean>();
export const decodePublicSchemaJson = extern<(bytes: Uint8Array) => JazzSchema>();
export const publicSchemaJsonBytes = extern<(schema: JazzSchema) => Uint8Array>();
export const schemaVersionIdOf = extern<(schema: JazzSchema) => string>();
export const wireRecordDescriptor = extern<(table: TableSchema) => any>();

// jazz-model: branch
export type BranchKey = any;
export type BranchSelector = { values: [string, { 0: Uint8Array }][] } | any;
export type BranchViewBase = any;
export type SnapshotRef = any;
export const defaultBranchKey = extern<() => BranchKey>();
export const compareBranchKey = extern<(a: any, b: any) => number>();
export const writeBranchKey = extern<W>();
export const readBranchKey = extern<R>();
export const writeBranchSelector = extern<W>();
export const readBranchSelector = extern<R>();
export const writeBranchViewBase = extern<W>();
export const readBranchViewBase = extern<R>();
export const writeSnapshotRef = extern<W>();
export const readSnapshotRef = extern<R>();

// jazz-model: policy claims / directory
export const canonicalPolicyBindingClaims =
  extern<(identity: any, claims: Map<string, any>) => [string, any][]>();
export const policyBindingDirectoryClaimsFromValue = extern<(v: any) => Map<string, any>>();
export const policyBindingDirectoryClaimsValue = extern<(claims: Map<string, any>) => any>();
export const policyDirectoryDescriptor = extern<() => any>();
export const policyDirectoryPayload = extern<(presence: number, claims: any) => any>();
export const MAX_POLICY_EXPRESSION_DEPTH = extern<number>();
export const MAX_POLICY_EXPRESSION_NODES = extern<number>();
