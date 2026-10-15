// Symbols the model-core partition uses but does not own.
import { extern } from "../support/runtime.js";
import type { Bytes } from "../support/bytes.js";

// ---- jazz-types (ids, object, time) ----
export type ObjectId = any;
export type ResultKey = any;
export type AccountId = any;
export type NodeUuid = any;
export type RowUuid = any;
export type PhysicalTableId = any;
export type GlobalTime = any;
export type TxTime = any;
export const objectIdFromUuid = extern<(uuid: Bytes) => ObjectId>();
export const objectIdUuidBytes = extern<(id: ObjectId) => Bytes>();
export const nodeUuidBytes = extern<(n: NodeUuid) => Bytes>();
export const rowUuidBytes = extern<(r: RowUuid) => Bytes>();
export const txTimePhysicalMs = extern<(t: TxTime) => number>();
export const accountIsSystem = extern<(a: AccountId) => boolean>();
export const AuthorSubject = extern<{
  LOCAL_FIRST_ISSUER: string;
  ANONYMOUS_ISSUER: string;
  authenticated(issuer: string, subject: string): any;
  reserved(issuer: string, subject: string): any;
  valueType(): GValueType;
}>();
export const AuthorSubjectError = extern<{
  reservedIssuer(issuer: string): Error;
  reservedAccount(): Error;
}>();
export const RowAuthor = extern<{
  fromPersistedSubject(author: any): any;
  fromRecord(record: any): any;
  valueType(): GValueType;
}>();
export const SchemaVersionId = extern<new (uuid: Bytes) => any>();
export type SchemaVersionId = any;
export const parseUuid = extern<(s: string) => Bytes | undefined>();
export const uuidString = extern<(u: Bytes) => string>();

// ---- groove::records / groove::schema / groove::storage ----
// Values `{ kind, value }`, types `{ kind, inner?, members?, schema?, descriptor? }`.
export type GValue = any;
export type GValueType = any;
export const RecordDescriptor = extern<new (fields: [string, GValueType][]) => any>();
export const OwnedRecord = extern<new (raw: Bytes, descriptor: any) => any>();
export const EnumSchema = extern<new (name: string, cases: any[]) => any>();
export const EnumCase = extern<new (name: string, payload: any) => any>();
export const EnumValue = extern<{ create(tag: number, payload: any, values: GValue[]): any }>();
export const ScalarEnumSchema = extern<new (name: string, variants: string[]) => any>();
export const SystemVariantRegistry = extern<{ deletionState(): any }>();
export const valueTypeEquals = extern<(a: GValueType, b: GValueType) => boolean>();
export const gvalueEquals = extern<(a: GValue, b: GValue) => boolean>();
export const isInternalStorageType = extern<(t: GValueType) => boolean>();
export const physicalStorageValueType = extern<(kind: "Json" | "String" | "Bytes") => GValueType>();
export const GrooveDatabaseSchema = extern<new (tables: any[]) => any>();
export const DirectRecordStoreSchema = extern<new (name: string, key: any, value: any) => any>();
export const GrooveTableSchema = extern<new (name: string, columns: any[]) => any>();
export const GrooveColumnSchema = extern<new (name: string, columnType: GValueType) => any>();
export const GrooveIndexSchema = extern<new (name: string, columns: string[]) => any>();
export const PrimaryKey = extern<{
  new (column: string, type: string): any;
  composite(columns: any[]): any;
}>();
export const PrimaryKeyColumn = extern<{
  integer(name: string, type: string): any;
  uuid(name: string): any;
  bytes(name: string): any;
}>();
export const StorageLayout = extern<{
  jazzClassV2(): { physicalColumnFamilies(cfs: string[]): string[] };
}>();

// ---- model-query partition (query AST, builders, claim keys) ----
export type Query = any;
export type Predicate = any;
export type Operand = any;
export type JoinVia = any;
export type JoinCorrelation = { join_column: string; source_column: string };
export type PolicyBranch = any;
export type InheritsOperation = any;
export type ShapeId = any;
export type BindingId = any;
export const queryFrom = extern<(table: string) => Query>();
export const qFilter = extern<(q: Query, p: Predicate) => Query>();
export const qPolicyBranch = extern<(q: Query, b: PolicyBranch) => Query>();
export const qJoinVia =
  extern<
    (
      q: Query,
      table: string,
      onColumn: string,
      filters: Iterable<Predicate>,
      opts?: {
        target?: string;
        sourceColumn?: string;
        correlatedFilters?: Iterable<JoinCorrelation>;
        nestedJoins?: Iterable<JoinVia>;
      },
    ) => Query
  >();
export const qInherits =
  extern<(q: Query, parentColumn: string, op: InheritsOperation, maxDepth?: number) => Query>();
export const policyAlternativesFromQuery = extern<(q: Query) => PolicyBranch[]>();
export const validateQueryRuntime = extern<(q: Query, schema: any) => unknown>();
export const providerClaimKey = extern<(name: string) => string>();
export const providerClaimPathOperandKey = extern<(segments: string[]) => string>();
export const authorClaimPathKey = extern<(path: string[]) => string | undefined>();
export const authorSubjectMetadataType = extern<(column: string) => GValueType | undefined>();
