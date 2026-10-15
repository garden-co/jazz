import { extern } from "../support/runtime.js";
import type { Bytes } from "../support/bytes.js";

// ---- node mod / state (node-state partition) ----
export type NodeState = any;
export type DatabaseBatch = any;
/** Rust `node::Error`: one variant per `kind`. */
export const nodeError =
  extern<(kind: string, detail?: unknown) => Error & { kind: string; detail?: unknown }>();
export const isNodeError = extern<(e: unknown, kind?: string) => boolean>();
export const hydrateNestedScalarEnumCases =
  extern<
    (
      valueType: any,
      enumVariants: any,
      schemaVersion: Bytes,
      path: string,
      identityPath: string,
      out: any,
    ) => void
  >();
export const hydrateNestedPayloadEnumCases =
  extern<
    (
      valueType: any,
      enumVariants: any,
      schemaVersion: Bytes,
      path: string,
      identityPath: string,
      out: any,
    ) => void
  >();
export const globalCasePath = extern<(path: string, c: { id: Bytes }) => string>();
export const isCatalogueMutation = extern<(m: SyncMessage) => boolean>();
export const nextGrooveRuntimeToken = extern<() => any>();
export const SKEW_TOLERANCE_MS = extern<number>();
export const LARGE_VALUE_UPLOAD_START_INGRESS_CHARGE_BYTES = extern<number>();
export const PublicationOutcome = extern<{
  settled<T>(v: T): { value: T; extend(o: any): void };
}>();
export type PublicationOutcome<T> = { value: T; extend(o: any): void };
export const CommitUnitTrust = extern<{ isTrusted(t: string): boolean }>();
export type CommitUnitIngestContext = {
  identity: any;
  trust: "Session" | "Relay" | "TrustedBackend" | "TrustedAuthority" | "TrustedAdmin";
  admittedWriteAuthorization: boolean;
  versionReceiptsValidated: boolean;
};
export const layerStack = extern<{
  refLayers(
    settled: string,
    f: (r: { kind: "Local" } | { kind: "Remote"; remote: "Origin" }) => any,
  ): any;
  compose(layers: any): any;
  Layer: { new_(g: any): any };
}>();
export const mergeOps = extern<{
  validateCounterSigns(table: any, signs: Bytes): void;
  clearCounterSigns(values: Value[], descriptor: any): void;
  counterSign(table: any, signs: Bytes, index: number): boolean;
  counterAfterOp(t: ValueType, previous: Value, op: Value, negative: boolean): Value | undefined;
  applyMergeOp(
    strategy: string,
    t: ValueType,
    previous: Value,
    op: Value,
    negative: boolean,
  ): Value;
  applyPendingMergeOp(
    strategy: string,
    t: ValueType,
    previous: Value,
    op: Value,
    negative: boolean,
  ): Value;
  counterToI128(v: Value): bigint;
  counterOpDelta(t: ValueType, op: Value, negative: boolean): bigint;
}>();
export const exclusiveReadEvidence = extern<{
  evidenceSlotValues(tx: Transaction, pending: boolean): [Value, Value, Value, Value];
}>();
export const migrationValidation = extern<{
  validateMigrationLensBetween(lens: any, source: any, target: any): void;
  validateLineageTablePartition(
    source: any,
    target: any,
    lens: any,
    newTables: string[],
    droppedTables: string[],
  ): void;
}>();
export const userColumnField = extern<(name: string) => string>();

// ---- groove records ----
export type Value = any;
export type ValueType = any;
export type RecordDescriptor = any;
export type OwnedRecord = any;
export type BorrowedRecord = any;
export type EnumSchema = any;
export type GrooveTableSchema = any;
/** groove `Value` constructors; values are `{ kind, value }`. */
export const V = extern<{
  u64(n: number | bigint): Value;
  u32(n: number): Value;
  bytes(b: Bytes): Value;
  uuid(u: Bytes): Value;
  str(s: string): Value;
  bool(b: boolean): Value;
  nullable(v?: Value): Value;
  array(vs: Value[]): Value;
  tuple(vs: Value[]): Value;
  record(r: OwnedRecord): Value;
  enumTag(tag: number): Value;
  enumValue(ev: any): Value;
  nullableOf(r: OwnedRecord | undefined): Value;
}>();
export const valueEquals = extern<(a: Value, b: Value) => boolean>();
/** groove `ValueType` constructors; types are `{ kind, inner?, items?, descriptor?, schema? }`. */
export const VT = extern<{
  U64: ValueType;
  Uuid: ValueType;
  Bytes: ValueType;
  nullable(t: ValueType): ValueType;
  array(t: ValueType): ValueType;
  tuple(ts: ValueType[]): ValueType;
  record(d: RecordDescriptor): ValueType;
  enumTag(s: any): ValueType;
  enum_(s: EnumSchema): ValueType;
}>();
export const valueTypeEquals = extern<(a: ValueType, b: ValueType) => boolean>();
export const RecordDescriptor = extern<{
  new_(fields: Iterable<[string, ValueType]>): RecordDescriptor;
  newWithFields(fields: Iterable<any>): RecordDescriptor;
  field(name: string, t: ValueType): any;
}>();
export const descriptorEquals = extern<(a: RecordDescriptor, b: RecordDescriptor) => boolean>();
export const OwnedRecord = extern<{ new_(raw: Bytes, d: RecordDescriptor): OwnedRecord }>();
export const EnumValue = extern<{ new_(tag: number, payload: OwnedRecord): any }>();
export const EnumCase = extern<{ new_(name: string, payload: RecordDescriptor): any }>();
export const EnumSchema = extern<{ new_(name: string, cases: any[]): EnumSchema }>();
export const ScalarEnumSchema = extern<{ new_(name: string, variants: string[]): any }>();
export const variantRegistryIdForPath = extern<(path: string) => any>();
export const encodePersistedRecordDescriptor = extern<(d: RecordDescriptor) => Bytes>();
export const FieldIdentity = extern<{
  name(n: string): any;
  slot(id: number): any;
  namedSlot(name: string, slot: number): any;
}>();
export const ValidatedVariantRecord = extern<{
  createWithEncodedFields(
    tag: number,
    d: RecordDescriptor,
    cap: number,
    f: (index: number, out: number[]) => void,
  ): any;
}>();
export const VariantRecord = extern<{ new_(tag: number, r: OwnedRecord): any }>();
export const largeValuesPhysicalStorageValueType = extern<(kind: "Json") => ValueType>();
export const RecursiveEnumRemaps = extern<{ default_(): EnumOccurrenceRemaps }>();
export type EnumOccurrenceRemaps = {
  scalar: Map<string, (number | undefined)[]>;
  payload: Map<string, (number | undefined)[]>;
  payloadChildren: Map<string, (string | undefined)[]>;
};

// ---- groove schema / ivm / db ----
export const GrooveColumnSchema = extern<{ new_(name: string, t: ValueType): any }>();
export const GrooveIndexSchema = extern<{ new_(name: string, columns: Iterable<string>): any }>();
export const GrooveTableSchema = extern<{
  newWithBoundRegistries(name: string, columns: Iterable<any>): GrooveTableSchema;
}>();
export const GrooveTableVariantField = extern<{
  shared(name: string, t: ValueType, column: string): any;
}>();
export type GraphBuilder = any;
export const GraphBuilder = extern<{
  table(name: string): GraphBuilder;
  variantSource(table: string, target: string): GraphBuilder;
  variantSourceScan(table: string, target: string, scan: any): GraphBuilder;
  variantIndexScan(table: string, index: string, target: string, scan: any): GraphBuilder;
}>();
export type ProjectField = any;
export const ProjectField = extern<{
  named(n: string): ProjectField;
  renamed(from: string, to: string): ProjectField;
  literalTyped(name: string, v: Value, t: ValueType): ProjectField;
  recursiveEnumRemap(
    source: string,
    out: string,
    t: ValueType,
    r: EnumOccurrenceRemaps,
  ): ProjectField;
  recursiveEnumRemapOmittingUnrepresentable(
    source: string,
    out: string,
    t: ValueType,
    r: EnumOccurrenceRemaps,
  ): ProjectField;
}>();
export type StaticScanSpec =
  | { kind: "Point"; values: any[] }
  | { kind: "Prefix"; values: any[] }
  | { kind: "PrefixLimit"; prefix: any[]; maxItems: number }
  | { kind: "ReversePrefixLimit"; prefix: any[]; maxItems: number }
  | { kind: "Range"; start: any[]; end: any[] };
export const literalValue = extern<(v: Value) => any>();
export type PrimaryKeyValue = any;
export const PK = extern<{
  composite(parts: PrimaryKeyValue[]): PrimaryKeyValue;
  bytes(b: Bytes): PrimaryKeyValue;
  uuid(u: Bytes): PrimaryKeyValue;
  u64(n: number | bigint): PrimaryKeyValue;
  intoBytes(pk: PrimaryKeyValue): Bytes;
}>();
export type PredicateExpr = any;
export const PredicateExpr = extern<{
  or(ps: PredicateExpr[]): PredicateExpr;
  isNull(f: string): PredicateExpr;
  neq(f: string, v: Value): PredicateExpr;
}>();
export const isGrooveDbError = extern<(e: unknown, kind: string) => boolean>();
export const isLargeValueUploadRejected = extern<(e: unknown) => boolean>();

// ---- types / protocol ----
export type Uuid = Bytes;
export type TxId = { time: bigint; node: Uuid };
export const txIdKey = extern<(t: TxId) => string>();
export const cmpTxId = extern<(a: TxId, b: TxId) => number>();
export const txIdEquals = extern<(a: TxId, b: TxId) => boolean>();
export const txTimeFromPhysicalMs = extern<(ms: number | bigint) => bigint>();
export const txTimePhysicalMs = extern<(t: bigint) => number>();
export const txTimeSortKey = extern<(t: bigint, node: Uuid) => any>();
export const authorityNowMs = extern<(nowMs: number, fallbackMs: number) => number>();
export type Transaction = any;
export type VersionRecord = any;
export type SyncMessage = any;
export type Fate =
  | { kind: "Pending" }
  | { kind: "Accepted" }
  | { kind: "Rejected"; reason: RejectionReason };
export type RejectionReason = any;
export const RejectionReason = extern<{
  toStorageValue(r: RejectionReason): Value;
  fromStorageValue(v: Value): RejectionReason;
  equals(a: RejectionReason, b: RejectionReason): boolean;
}>();
export type DurabilityTier = "None" | "Local" | "Global";
export const durabilityRank = extern<(d: DurabilityTier) => number>();
export const durabilityFromDiscriminant = extern<(n: number) => DurabilityTier>();
export const transactionEquals = extern<(a: Transaction, b: Transaction) => boolean>();
export const cmpVersionRecord = extern<(a: VersionRecord, b: VersionRecord) => number>();
export const versionRecordEquals = extern<(a: VersionRecord, b: VersionRecord) => boolean>();
export const VersionRecord = extern<{
  new_(table: string, schema: Uuid, record: OwnedRecord): VersionRecord;
  encode(
    table: any,
    schema: Uuid,
    row: Uuid,
    createdBy: any,
    createdAt: number,
    updatedBy: any,
    updatedAt: number,
    cells: (Value | undefined)[],
    deletion: any,
  ): VersionRecord;
}>();
export const BranchKey = extern<{
  default_(): any;
  fromCanonicalBytes(b: Bytes): any;
  equals(a: any, b: any): boolean;
  key(b: any): string;
}>();
export const RowAuthor = extern<{
  fromPersistedSubject(a: any): any;
  fromRecord(r: any): any;
  valueType(): ValueType;
  readOptional(r: BorrowedRecord, idx: number): any;
}>();
export const AuthorSubject = extern<{ SYSTEM: any; equals(a: any, b: any): boolean }>();
export const canonicalCatalogueSchemaBytes = extern<(s: any) => Bytes>();
export const decodePublicSchemaJson = extern<(b: Bytes) => any>();
export const canonicalLensBytes = extern<(l: any) => Bytes>();
export const decodeCanonicalLensBytes = extern<(b: Bytes) => any>();
export const SchemaVersion = extern<{ new_(schema: any): any }>();
export const ActiveSchema = extern<{
  new_(write: { revision: number; schema: Uuid }, compiled: any): any;
}>();
export const publicationContentId = extern<(p: any) => Uuid>();
export const publicationEquals = extern<(a: any, b: any) => boolean>();
export const lensContentId = extern<(l: any) => Uuid>();
export const validateIdentitiesForSchema = extern<(m: any, schema: any) => string | undefined>();
export const validateVersionRecords = extern<(v: VersionRecord[]) => boolean>();
export const commitUnitLimitViolation = extern<(v: VersionRecord[]) => string | undefined>();
export const validateShapeRegistrationSize = extern<(ast: any, opts: any) => boolean>();
export const contributionMergeStorageType = extern<() => ValueType>();
export const validateContributionMergeProvenance = extern<(p: any) => boolean>();
export const storageColumnType = extern<(c: any) => ValueType>();
export const columnStorageValue = extern<(c: any, v: Value) => Value>();
export const CurrentRow = extern<{
  new_(table: string, r: OwnedRecord): any;
  newWithBindingFields(table: string, r: OwnedRecord, role: "LogicalField"): any;
}>();
export const HistoryEntry = extern<{
  new_(table: string, r: OwnedRecord, tx: any, local: boolean, global: boolean): any;
}>();
export const SCHEMA_FIELDS = extern<{
  COUNTER_SIGNS_FIELD: string;
  SEQ_FIELD: string;
  BASE_SEQ_FIELD: string;
  BASE_PENDING_FIELD: string;
  GLOBAL_CURRENT_BY_SEQ_INDEX: string;
}>();
export const CATALOGUE_SCHEMA_VERSION = extern<number>();
export const PARKED_PREDECESSOR_WAIT_MS_EXTERN = extern<number>();

// ---- ingest additions ----
export const grooveDbError = extern<(kind: string, detail?: unknown) => Error>();
export const versionIndirectDescriptors = extern<(versions: VersionRecord[]) => any[]>();
export const applyCompiledLensPath = extern<(path: any, cells: Map<string, Value>) => void>();
export const SUPERSEDED_WRITES_STORE = extern<string>();
