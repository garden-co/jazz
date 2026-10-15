// Schema-aware database facade over records, storage, and the IVM runtime.
import { type Bytes, concatBytes, equalBytes, utf8 } from "../../support/bytes.js";
import { SortedMap, SortedSet, cmpNum } from "../../support/collections.js";
import { AsyncLock, type LockRelease } from "./storage_helpers.js";
import {
  type IvmRuntime,
  type NodeRef,
  type LargeValueRef,
  type PublicationId,
  type TickMetrics,
  type Value,
  RecordDescriptor,
  cmpNodeRef,
  encodeNodeRef,
  decodeNodeRef,
  encodeLargeValueRef,
  decodeLargeValueRef,
  nodeRefDescriptor,
  nodeRefValue,
  nodeRefFromValue,
  decodeNodeUntypedAuthenticated,
  PhysicalTraversalNodeBudget,
  ChunkError,
  type PublicationInstallFailures,
  type ChunkStorage,
  type MissingChunkResolver,
  ivmErrorFrom,
} from "../../_extern/groove-db-storage.js";
import {
  type LayoutStorage,
  type OrderedKvStorage,
  type WriteOperation,
  OwnedStorage,
  StagedWriteState,
  StorageError,
} from "../storage/index.js";
import { CommitMethods } from "./commit.js";
import { FacadeMethods } from "./facade.js";
import { PrimaryStorageMethods } from "./primary_storage.js";
import { QueryMethods } from "./query.js";
import { SchemaAdmissionMethods } from "./schema_admission.js";
import { EncodingMethods } from "./encoding.js";
import {
  type CommitMetrics,
  type ReadMetricsSink,
  StorageWriteMetrics,
} from "./storage_helpers.js";

export * from "./batch.js";
export {
  type CommitMetrics,
  DirectRecordStore,
  type DirectRecordStoreEntry,
  type DirectRecordStoreWrite,
  EncodedKeyValue,
  PreparedShape,
  type StorageReadBucket,
  type StorageReadMetrics,
  type StorageWriteBucket,
  StorageWriteMetrics,
  MeteredStorage,
} from "./storage_helpers.js";
export {
  indexRecordDescriptor,
  persistedIndexColumnValue,
  persistedIndexPrimaryKey,
} from "./encoding.js";

/** Reserved Groove-owned metadata plane for staged roots and reference accounting. */
export const LARGE_VALUE_METADATA_CF = "__groove_large_values";

// ---- errors ----

const DB_MESSAGES = {
  ImmutableBatchConflict: () => "immutable record conflict invalidated the batch",
  StaleImmutableBatch: () => "immutable batch must be rebuilt against the current database state",
  DatabasePoisoned: () => "database instance is poisoned after failure or storage extraction",
  UnsettledPublications: () =>
    "database storage extraction requires all publications to be settled",
  PublicationNotFound: (d: any) =>
    `publication does not belong to this database: PublicationId(${d})`,
  SubscriptionEnded: () => "subscription ended",
  SubscriptionFailed: (d: any) => String(d?.message ?? d),
  DuplicatePrimaryKey: (d: any) => `duplicate primary key for table ${d.table}: [${d.key}]`,
  DuplicateTableVariant: (d: any) => `duplicate schema version ${d.version} for table ${d.table}`,
  TableVariantTagOutOfRange: (d: any) =>
    `table ${d.table} variant tag ${d.tag} exceeds the bounded u32 tag space`,
  DuplicateParameter: (d: any) => `duplicate query parameter binding: ${d}`,
  IvmRuntime: (d: any) => String(d?.message ?? d),
  InvalidPersistedIndex: (d: any) => `invalid persisted index contents: ${d}`,
  InvalidLargeValueMetadata: (d: any) => `invalid persisted large-value metadata: ${d}`,
  PendingLargeValueUploadLimitExceeded: (d: any) =>
    `pending large-value upload limit reached: ${d.limit}`,
  IndexKeyArity: (d: any) =>
    `index key arity mismatch for ${d.index}: expected at most ${d.expected}, got ${d.actual}`,
  IndexNotFound: (d: any) => `index not found: ${d.table}.${d.index}`,
  MissingParameter: (d: any) => `missing query parameter binding: ${d}`,
  MissingPrimaryKey: (d: any) => `table has no primary key: ${d}`,
  InvalidTableVariantField: (d: any) =>
    `invalid field ${d.field} in schema version ${d.version} for table ${d.table}`,
  PrimaryKeyArity: (d: any) =>
    `primary key arity mismatch for ${d.table}: expected at most ${d.expected}, got ${d.actual}`,
  PrimaryKeyTypeMismatch: (d: any) => `primary key type mismatch for ${d.table}.${d.column}`,
  QueryPlanning: (d: any) => String(d?.message ?? d),
  RecordEncoding: (d: any) => String(d?.message ?? d),
  DirectRecordStoreNotFound: (d: any) => `direct record store not found: ${d}`,
  InvalidDirectRecordStoreKey: (d: any) => `invalid direct record store key: ${d}`,
  InvalidApplicationStorageName: (d: any) =>
    `invalid application storage name ${JSON.stringify(d.name)}: ${d.reason}`,
  DuplicateApplicationStorageName: (d: any) =>
    `application table/direct-record-store storage name is duplicated: ${d}`,
  Storage: (d: any) => String(d?.message ?? d),
  TableNotFound: (d: any) => `table not found: ${d}`,
  TableAlreadyExists: (d: any) => `table already exists: ${d}`,
  TableFieldDefinitionMismatch: (d: any) =>
    `field definition does not match the live catalogue: ${d.table}.${d.field}`,
  TableIndexDefinitionMismatch: (d: any) =>
    `index definition does not match the live catalogue: ${d.table}.${d.index}`,
  TableIndexRegistrationWhilePublicationsResident: (d: any) =>
    `cannot register index ${d.table}.${d.index} while database publications remain resident`,
  TableIndexFieldNotFound: (d: any) =>
    `index ${d.table}.${d.index} references unknown field ${d.field}`,
  SchemaVersionMissingPrimaryKey: (d: any) =>
    `schema version ${d.version} for table ${d.table} omits primary-key column ${d.column}`,
  UnsupportedSchemaVariantTableFeature: (d: any) =>
    `schema-variant table uses foreign keys, which are not supported yet: ${d}`,
  SchemaVersionDescriptorMismatch: (d: any) =>
    `record descriptor does not match schema version ${d.version} for table ${d.table}`,
  ReservedTableVariant: (d: any) =>
    `schema version 0 is reserved for Groove's implicit table layout: ${d}`,
  CannotPromoteLiveTableToSchemaVariants: (d: any) =>
    `cannot add the first explicit schema version to a live homogeneous table: ${d}`,
  UnknownTableVariant: (d: any) => `unknown schema version ${d.version} for table ${d.table}`,
  UnknownParameter: (d: any) => `unknown query parameter binding: ${d}`,
};
export type DbErrorKind = keyof typeof DB_MESSAGES;

export class DbError extends Error {
  constructor(
    readonly kind: DbErrorKind,
    readonly data: any = undefined,
  ) {
    super(DB_MESSAGES[kind](data));
  }
  /** `From<storage::Error>` / `From<records::Error>` / `From<IvmRuntimeError>`. */
  static from(error: unknown): DbError {
    if (error instanceof DbError) return error;
    if (error instanceof StorageError) return new DbError("Storage", error);
    if ((error as any)?.isIvmRuntimeError) return new DbError("IvmRuntime", error);
    if ((error as any)?.isPlannerError) return new DbError("QueryPlanning", error);
    return new DbError("RecordEncoding", error);
  }
}
export const lvMeta = (message: string) => new DbError("InvalidLargeValueMetadata", message);

// ---- large-value metadata keys ----

const keyWith = (prefix: string, tail: Bytes) => concatBytes(utf8(prefix), tail);
export const stagedLargeValueKey = (id: Bytes) => keyWith("staged/", id);
export const pendingLargeValueUploadKey = (id: Bytes) => keyWith("upload/", id);
export const completedLargeValueUploadKey = (id: Bytes) => keyWith("completed-upload/", id);
export const completedLargeValueReceiptKey = (id: Bytes) => keyWith("completed-receipt/", id);

/** Bind a lifecycle record to the fixed-width identity in its metadata key. */
export function stagedLargeValueIdFromMetadataKey(
  key: Bytes,
  prefix: string,
  recordName: string,
): Bytes {
  const p = utf8(prefix);
  if (key.length < p.length || !equalBytes(key.subarray(0, p.length), p))
    throw lvMeta(`${recordName} has an invalid metadata key prefix`);
  const id = key.subarray(p.length);
  if (id.length !== 16)
    throw lvMeta(`${recordName} metadata key must contain exactly 16 staging-id bytes`);
  return id.slice();
}

function nodeRefKey(prefix: string, nodeRef: NodeRef, what: string): Bytes {
  try {
    return keyWith(prefix, encodeNodeRef(nodeRef));
  } catch (error) {
    throw lvMeta(`cannot encode ${what} identity: ${error}`);
  }
}
export const largeValueRootKey = (n: NodeRef) => nodeRefKey("root/", n, "root");
export const largeValueNodeKey = (n: NodeRef) => nodeRefKey("node/", n, "node");
export const largeValueReclaimKey = (n: NodeRef) => nodeRefKey("reclaim/", n, "reclaim");
export const largeValuePendingInstallKey = (n: NodeRef) =>
  nodeRefKey("install/", n, "pending install");

export function largeValueReclaimNodeRefFromKey(key: Bytes): NodeRef {
  const p = utf8("reclaim/");
  if (key.length < p.length || !equalBytes(key.subarray(0, p.length), p))
    throw lvMeta("reclaim entry has an invalid key prefix");
  try {
    return decodeNodeRef(key.subarray(p.length));
  } catch (error) {
    throw lvMeta(`cannot decode reclaim key identity: ${error}`);
  }
}

export interface LargeValueRootReferences {
  durable: number;
  staged: number;
  nodeActive: boolean;
}
export const emptyRootReferences = (): LargeValueRootReferences => ({
  durable: 0,
  staged: 0,
  nodeActive: false,
});

export interface LargeValueNodeReferences {
  references: number;
  uploadReferences: number;
  children: NodeRef[];
}
export const emptyNodeReferences = (): LargeValueNodeReferences => ({
  references: 0,
  uploadReferences: 0,
  children: [],
});

export type LargeValueReclaimDecision =
  | "ClearStaleQueue"
  | "WaitForDurableZero"
  | "ResidentVeto"
  | "Delete";

/** Durable zero is the sole authorization; resident references can only veto it. */
export function largeValueReclaimDecision(
  durable: LargeValueNodeReferences,
  resident: LargeValueNodeReferences,
): LargeValueReclaimDecision {
  const d = durable.references !== 0 || durable.uploadReferences !== 0;
  const r = resident.references !== 0 || resident.uploadReferences !== 0;
  return d ? (r ? "ClearStaleQueue" : "WaitForDurableZero") : r ? "ResidentVeto" : "Delete";
}

export function uniqueLargeValueChildren(node: any): NodeRef[] {
  return node.kind === "Leaf"
    ? []
    : canonicalLargeValueChildren(node.children.map((c: any) => c.nodeRef));
}

/** Normalize child edges to one edge per physical child, in canonical order. */
export function canonicalLargeValueChildren(children: Iterable<NodeRef>): NodeRef[] {
  return new SortedSet(cmpNodeRef, children).toArray();
}

// ---- durable metadata records ----
// Field IDs are durable one-based record slots, contiguous from one.

interface DurableMetadataRecordSchema {
  fieldIds: number[];
  descriptor: RecordDescriptor;
}

function durableMetadataRecordSchema(fields: [number, string, any][]): DurableMetadataRecordSchema {
  fields.sort((a, b) => a[0] - b[0]);
  if (!fields.length) throw new Error("durable metadata record must have at least one field");
  if (!fields.every(([id], i) => id === i + 1))
    throw new Error("durable metadata record field IDs must be contiguous starting at one");
  return {
    fieldIds: fields.map(([id]) => id),
    descriptor: new RecordDescriptor(
      fields.map(([id, name, vt]) => [`f${String(id).padStart(4, "0")}_${name}`, vt]),
    ),
  };
}

function lazy<T>(f: () => T): () => T {
  let v: T | undefined;
  return () => (v ??= f());
}
const RAW_BYTES = { kind: "RawBytes" };
const nodeRefArray = () => ({
  kind: "Array",
  inner: { kind: "Record", descriptor: nodeRefDescriptor() },
});

const rootReferencesSchema = lazy(() =>
  durableMetadataRecordSchema([
    [1, "durable", "U64"],
    [2, "staged", "U64"],
    [3, "node_active", "Bool"],
  ]),
);
const nodeReferencesSchema = lazy(() =>
  durableMetadataRecordSchema([
    [1, "references", "U64"],
    [2, "upload_references", "U64"],
    [3, "children", nodeRefArray()],
  ]),
);
const stagedLargeValueSchema = lazy(() =>
  durableMetadataRecordSchema([
    [1, "id", RAW_BYTES],
    [2, "value_ref", RAW_BYTES],
    [3, "encoded_bytes", "U64"],
    [4, "node_count", "U64"],
    [5, "created_at_ms", "U64"],
  ]),
);
const pendingUploadSchema = lazy(() =>
  durableMetadataRecordSchema([
    [1, "id", RAW_BYTES],
    [2, "descriptor", { kind: "Nullable", inner: RAW_BYTES }],
    [3, "receipt_id", { kind: "Nullable", inner: RAW_BYTES }],
    [4, "encoded_bytes", "U64"],
    [5, "node_count", "U64"],
    [6, "created_at_ms", "U64"],
    [7, "chunks", nodeRefArray()],
  ]),
);
const completedUploadSchema = lazy(() =>
  durableMetadataRecordSchema([
    [1, "id", RAW_BYTES],
    [2, "receipt_id", RAW_BYTES],
    [3, "descriptor", RAW_BYTES],
    [4, "encoded_bytes", "U64"],
    [5, "node_count", "U64"],
  ]),
);

function encodeMetadataRecord(
  schema: DurableMetadataRecordSchema,
  input: [number, Value][],
  name: string,
): Bytes {
  const values = new Map<number, Value>();
  for (const [id, value] of input) {
    if (values.has(id)) throw lvMeta(`cannot encode ${name}: duplicate field id ${id}`);
    values.set(id, value);
  }
  const ordered: Value[] = [];
  for (const id of schema.fieldIds) {
    if (!values.has(id)) throw lvMeta(`cannot encode ${name}: missing field id ${id}`);
    ordered.push(values.get(id)!);
    values.delete(id);
  }
  for (const id of values.keys()) throw lvMeta(`cannot encode ${name}: unknown field id ${id}`);
  try {
    return schema.descriptor.create(ordered);
  } catch (error) {
    throw lvMeta(`cannot encode ${name}: ${error}`);
  }
}

function decodeMetadataRecord(
  encoded: Bytes,
  schema: DurableMetadataRecordSchema,
  name: string,
): Map<number, Value> {
  let values: Value[];
  let canonical: Bytes;
  try {
    values = schema.descriptor.bind(encoded).toValues();
    canonical = schema.descriptor.create(values);
  } catch (error) {
    throw lvMeta(`cannot decode ${name}: ${error}`);
  }
  if (!equalBytes(canonical, encoded)) throw lvMeta(`cannot decode ${name}: non-canonical record`);
  return new Map(schema.fieldIds.map((id, i) => [id, values[i]]));
}

function takeField(values: Map<number, Value>, id: number, name: string): Value {
  const v = values.get(id);
  if (v === undefined) throw lvMeta(`cannot decode ${name}: missing field id ${id}`);
  values.delete(id);
  return v;
}
function takeKind(
  values: Map<number, Value>,
  id: number,
  name: string,
  kind: string,
  error: string,
): any {
  const v = takeField(values, id, name);
  if (v.kind !== kind) throw lvMeta(error);
  return v.v;
}

const u64 = (n: number): Value => ({ kind: "U64", v: BigInt(n) });
const idValue = (id: Bytes): Value => ({ kind: "Bytes", v: id });
export function stagedLargeValueIdFromValue(value: Value): Bytes {
  if (value.kind !== "Bytes") throw lvMeta("large-value staging id must be bytes");
  if (value.v.length !== 16) throw lvMeta("large-value staging id must be 16 bytes");
  return value.v;
}

const canonicalNodeRefValues = (refs: Iterable<NodeRef>) =>
  canonicalLargeValueChildren(refs).map(nodeRefValue);

function canonicalNodeRefsFromValue(value: Value, name: string): NodeRef[] {
  if (value.kind !== "Array") throw lvMeta(`${name} must be an array`);
  const refs: NodeRef[] = [];
  for (const v of value.v) {
    let nodeRef: NodeRef;
    try {
      nodeRef = nodeRefFromValue(v);
    } catch (error) {
      throw lvMeta(`cannot decode ${name}: ${error}`);
    }
    if (refs.length && cmpNodeRef(refs[refs.length - 1], nodeRef) >= 0)
      throw lvMeta(`${name} must be strictly increasing`);
    refs.push(nodeRef);
  }
  return refs;
}

function encodeRef(ref: LargeValueRef, what: string): Bytes {
  try {
    return encodeLargeValueRef(ref);
  } catch (error) {
    throw lvMeta(`cannot encode ${what}: ${error}`);
  }
}
function decodeRef(bytes: Bytes, what: string): LargeValueRef {
  try {
    return decodeLargeValueRef(bytes);
  } catch (error) {
    throw lvMeta(`cannot decode ${what}: ${error}`);
  }
}

export interface StagedLargeValueAccounting {
  encodedBytes: number;
  nodeCount: number;
}
export interface StagedLargeValue {
  id: Bytes;
  valueRef: LargeValueRef;
  accounting: StagedLargeValueAccounting;
  createdAtMs: number;
}
export interface PendingLargeValueUpload {
  id: Bytes;
  descriptor?: LargeValueRef;
  receiptId?: Bytes;
  accounting: StagedLargeValueAccounting;
  createdAtMs: number;
  chunks: NodeRef[];
}
/** Durable completion evidence retained until its staged receipt is closed. */
export interface CompletedLargeValueUpload {
  id: Bytes;
  receiptId: Bytes;
  descriptor: LargeValueRef;
  accounting: StagedLargeValueAccounting;
}

export function encodeCompletedLargeValueUpload(u: CompletedLargeValueUpload): Bytes {
  const descriptor = encodeRef(u.descriptor, "completed upload descriptor");
  return encodeMetadataRecord(
    completedUploadSchema(),
    [
      [1, idValue(u.id)],
      [2, idValue(u.receiptId)],
      [3, { kind: "Bytes", v: descriptor }],
      [4, u64(u.accounting.encodedBytes)],
      [5, u64(u.accounting.nodeCount)],
    ],
    "completed upload",
  );
}

export function decodeCompletedLargeValueUpload(encoded: Bytes): CompletedLargeValueUpload {
  const name = "completed upload";
  const f = decodeMetadataRecord(encoded, completedUploadSchema(), name);
  const id = takeField(f, 1, name);
  const receiptId = takeField(f, 2, name);
  const descriptor = takeKind(f, 3, name, "Bytes", "invalid completed upload descriptor");
  const encodedBytes = takeKind(f, 4, name, "U64", "invalid completed upload accounting");
  const nodeCount = takeKind(f, 5, name, "U64", "invalid completed upload accounting");
  return {
    id: stagedLargeValueIdFromValue(id),
    receiptId: stagedLargeValueIdFromValue(receiptId),
    descriptor: decodeRef(descriptor, "completed upload descriptor"),
    accounting: { encodedBytes: Number(encodedBytes), nodeCount: Number(nodeCount) },
  };
}

export function encodeLargeValueRootReferences(r: LargeValueRootReferences): Bytes {
  return encodeMetadataRecord(
    rootReferencesSchema(),
    [
      [1, u64(r.durable)],
      [2, u64(r.staged)],
      [3, { kind: "Bool", v: r.nodeActive }],
    ],
    "root references",
  );
}

export function decodeLargeValueRootReferences(encoded: Bytes): LargeValueRootReferences {
  const name = "root references";
  const err = "cannot decode root references: invalid fields";
  const f = decodeMetadataRecord(encoded, rootReferencesSchema(), name);
  return {
    durable: Number(takeKind(f, 1, name, "U64", err)),
    staged: Number(takeKind(f, 2, name, "U64", err)),
    nodeActive: takeKind(f, 3, name, "Bool", err),
  };
}

export function encodeLargeValueNodeReferences(r: LargeValueNodeReferences): Bytes {
  return encodeMetadataRecord(
    nodeReferencesSchema(),
    [
      [1, u64(r.references)],
      [2, u64(r.uploadReferences)],
      [3, { kind: "Array", v: canonicalNodeRefValues(r.children) }],
    ],
    "large-value node references",
  );
}

export function decodeLargeValueNodeReferences(encoded: Bytes): LargeValueNodeReferences {
  const name = "large-value node references";
  const err = "cannot decode large-value node references: invalid fields";
  const f = decodeMetadataRecord(encoded, nodeReferencesSchema(), name);
  const references = Number(takeKind(f, 1, name, "U64", err));
  const uploadReferences = Number(takeKind(f, 2, name, "U64", err));
  const children = takeField(f, 3, name);
  return {
    references,
    uploadReferences,
    children: canonicalNodeRefsFromValue(children, "large-value node children"),
  };
}

export function encodeStagedLargeValue(s: StagedLargeValue): Bytes {
  return encodeMetadataRecord(
    stagedLargeValueSchema(),
    [
      [1, idValue(s.id)],
      [2, { kind: "Bytes", v: encodeRef(s.valueRef, "staged large value") }],
      [3, u64(s.accounting.encodedBytes)],
      [4, u64(s.accounting.nodeCount)],
      [5, u64(s.createdAtMs)],
    ],
    "staged large value",
  );
}

export function decodeStagedLargeValue(encoded: Bytes): StagedLargeValue {
  const name = "staged large value";
  const err = "cannot decode staged large value: invalid fields";
  const f = decodeMetadataRecord(encoded, stagedLargeValueSchema(), name);
  const id = takeField(f, 1, name);
  const valueRef = takeKind(
    f,
    2,
    name,
    "Bytes",
    "cannot decode staged large value: invalid descriptor bytes",
  );
  const encodedBytes = takeKind(f, 3, name, "U64", err);
  const nodeCount = takeKind(f, 4, name, "U64", err);
  const createdAtMs = takeKind(f, 5, name, "U64", err);
  return {
    id: stagedLargeValueIdFromValue(id),
    valueRef: decodeRef(valueRef, "staged large value descriptor"),
    accounting: { encodedBytes: Number(encodedBytes), nodeCount: Number(nodeCount) },
    createdAtMs: Number(createdAtMs),
  };
}

export function decodeStagedLargeValueAtKey(key: Bytes, encoded: Bytes): StagedLargeValue {
  const keyId = stagedLargeValueIdFromMetadataKey(key, "staged/", "staged large value");
  const staged = decodeStagedLargeValue(encoded);
  if (!equalBytes(staged.id, keyId)) throw lvMeta("staged large value key and receipt id differ");
  return staged;
}

export function encodePendingLargeValueUpload(u: PendingLargeValueUpload): Bytes {
  const descriptor =
    u.descriptor && encodeRef(u.descriptor, "pending large-value upload descriptor");
  return encodeMetadataRecord(
    pendingUploadSchema(),
    [
      [1, idValue(u.id)],
      [2, { kind: "Nullable", v: descriptor ? { kind: "Bytes", v: descriptor } : null }],
      [3, { kind: "Nullable", v: u.receiptId ? idValue(u.receiptId) : null }],
      [4, u64(u.accounting.encodedBytes)],
      [5, u64(u.accounting.nodeCount)],
      [6, u64(u.createdAtMs)],
      [7, { kind: "Array", v: canonicalNodeRefValues(u.chunks) }],
    ],
    "pending large-value upload",
  );
}

export function decodePendingLargeValueUpload(encoded: Bytes): PendingLargeValueUpload {
  const name = "pending large-value upload";
  const err = "cannot decode pending large-value upload: invalid fields";
  const f = decodeMetadataRecord(encoded, pendingUploadSchema(), name);
  const id = takeField(f, 1, name);
  const descriptor: Value | null = takeKind(f, 2, name, "Nullable", err);
  const receiptId: Value | null = takeKind(f, 3, name, "Nullable", err);
  const encodedBytes = takeKind(f, 4, name, "U64", err);
  const nodeCount = takeKind(f, 5, name, "U64", err);
  const createdAtMs = takeKind(f, 6, name, "U64", err);
  const chunks = takeField(f, 7, name);
  let decodedDescriptor: LargeValueRef | undefined;
  if (descriptor) {
    if (descriptor.kind !== "Bytes")
      throw lvMeta(
        "cannot decode pending large-value upload descriptor: malformed large-value scalar",
      );
    decodedDescriptor = decodeRef(descriptor.v, "pending large-value upload descriptor");
  }
  return {
    id: stagedLargeValueIdFromValue(id),
    descriptor: decodedDescriptor,
    receiptId: receiptId ? stagedLargeValueIdFromValue(receiptId) : undefined,
    accounting: { encodedBytes: Number(encodedBytes), nodeCount: Number(nodeCount) },
    createdAtMs: Number(createdAtMs),
    chunks: canonicalNodeRefsFromValue(chunks, "pending large-value upload chunks"),
  };
}

export function decodePendingLargeValueUploadAtKey(
  key: Bytes,
  encoded: Bytes,
): PendingLargeValueUpload {
  const keyId = stagedLargeValueIdFromMetadataKey(key, "upload/", "pending upload");
  const upload = decodePendingLargeValueUpload(encoded);
  if (!equalBytes(upload.id, keyId)) throw lvMeta("pending upload key and journal id differ");
  return upload;
}

export function decodeCompletedLargeValueUploadAtKey(
  key: Bytes,
  encoded: Bytes,
): CompletedLargeValueUpload {
  const keyId = stagedLargeValueIdFromMetadataKey(key, "completed-upload/", "completed upload");
  const completed = decodeCompletedLargeValueUpload(encoded);
  if (!equalBytes(completed.id, keyId)) throw lvMeta("completed upload key and journal id differ");
  return completed;
}

export function decodeCompletedLargeValueReceiptAtKey(key: Bytes, encoded: Bytes): Bytes {
  stagedLargeValueIdFromMetadataKey(key, "completed-receipt/", "completed receipt");
  if (encoded.length !== 16)
    throw lvMeta("completed receipt binding must contain exactly 16 upload-id bytes");
  return encoded.slice();
}

/**
 * Apply physical-node ownership transitions against one read-your-own-write
 * overlay. Each active parent contributes one reference to each distinct child.
 */
export async function largeValueNodeTransitionOperations(
  storage: OrderedKvStorage,
  nodeUpdates: SortedMap<NodeRef, LargeValueNodeReferences>,
  pending: [NodeRef, number][],
  allowMissingPositiveMetadata: boolean,
): Promise<WriteOperation[]> {
  const budget = new PhysicalTraversalNodeBudget();
  try {
    budget.consumeMany(nodeUpdates.size);
  } catch (error) {
    throw new DbError("IvmRuntime", ivmErrorFrom(error));
  }
  const reclaimCandidates = new SortedSet<NodeRef>(cmpNodeRef);
  for (let next; (next = pending.pop()); ) {
    const [nodeRef, delta] = next;
    let metadata = nodeUpdates.delete(nodeRef);
    if (!metadata) {
      try {
        budget.consume();
      } catch (error) {
        throw new DbError("IvmRuntime", ivmErrorFrom(error));
      }
      const encoded = await storage.get(LARGE_VALUE_METADATA_CF, largeValueNodeKey(nodeRef));
      if (encoded) metadata = decodeLargeValueNodeReferences(encoded);
      else if (delta > 0 && allowMissingPositiveMetadata) metadata = emptyNodeReferences();
      else throw lvMeta("active node reference metadata is missing");
    }
    metadata.children = canonicalLargeValueChildren(metadata.children);
    let crossedZero: boolean;
    if (delta > 0) {
      crossedZero = metadata.references === 0;
      if (metadata.references >= Number.MAX_SAFE_INTEGER)
        throw lvMeta("node reference count overflow");
      metadata.references++;
      reclaimCandidates.delete(nodeRef);
    } else {
      if (metadata.references === 0) throw lvMeta("node reference count underflow");
      metadata.references--;
      crossedZero = metadata.references === 0;
      if (crossedZero) reclaimCandidates.add(nodeRef);
    }
    if (crossedZero) for (const child of metadata.children) pending.push([child, delta]);
    nodeUpdates.set(nodeRef, metadata);
  }
  const operations: WriteOperation[] = [];
  for (const [nodeRef, metadata] of nodeUpdates) {
    operations.push({
      kind: "Set",
      cf: LARGE_VALUE_METADATA_CF,
      key: largeValueNodeKey(nodeRef),
      value: encodeLargeValueNodeReferences(metadata),
    });
    if (metadata.references === 0 && reclaimCandidates.has(nodeRef)) {
      let value: Bytes;
      try {
        value = encodeNodeRef(nodeRef);
      } catch (error) {
        throw lvMeta(`cannot encode reclaim entry: ${error}`);
      }
      operations.push({
        kind: "Set",
        cf: LARGE_VALUE_METADATA_CF,
        key: largeValueReclaimKey(nodeRef),
        value,
      });
    }
  }
  return operations;
}

// ---- chunk install journal / observer ----

const backend = (message: string) => ChunkError.backend(message);
const asBackend = <T>(f: () => T): T => {
  try {
    return f();
  } catch (error) {
    throw backend(String((error as Error)?.message ?? error));
  }
};
async function asBackendAsync<T>(p: Promise<T>): Promise<T> {
  try {
    return await p;
  } catch (error) {
    throw backend(String((error as Error)?.message ?? error));
  }
}

/** Answers whether a particular exact chunk needs install reconciliation. */
export class MetadataChunkInstallJournal {
  constructor(private readonly storage: WeakRef<LayoutStorage>) {}
  private upgrade(what: string): LayoutStorage {
    const storage = this.storage.deref();
    if (!storage) throw backend(`database storage closed while ${what}`);
    return storage;
  }
  async markPending(nodeRef: NodeRef): Promise<void> {
    const storage = this.upgrade("recording chunk installation");
    const key = asBackend(() => largeValuePendingInstallKey(nodeRef));
    const existing = await asBackendAsync(
      storage.putIfAbsent(LARGE_VALUE_METADATA_CF, key, new Uint8Array()),
    );
    if (existing && existing.length) throw ChunkError.integrity();
  }
  async isPending(nodeRef: NodeRef): Promise<boolean> {
    const storage = this.upgrade("reading chunk installation journal");
    const key = asBackend(() => largeValuePendingInstallKey(nodeRef));
    const value = await asBackendAsync(storage.get(LARGE_VALUE_METADATA_CF, key));
    if (!value) return false;
    if (!value.length) return true;
    throw ChunkError.integrity();
  }
  async complete(nodeRef: NodeRef): Promise<void> {
    const storage = this.upgrade("completing chunk installation");
    const key = asBackend(() => largeValuePendingInstallKey(nodeRef));
    await asBackendAsync(storage.compareAndDelete(LARGE_VALUE_METADATA_CF, key, new Uint8Array()));
  }
}

export interface ResidentLifecycleInstall {
  storage: OwnedStorage;
  staged: StagedWriteState;
  /** Whether the database currently owns `lifecycle` for resident publications. */
  lifecycleHeld: { value: boolean };
  /** Before durability, install metadata belongs in the publication snapshot. */
  durable: { value: boolean };
  installFailures: PublicationInstallFailures;
}

export class MetadataChunkInstallObserver {
  constructor(
    private readonly storage: WeakRef<LayoutStorage>,
    private readonly lifecycle: WeakRef<AsyncLock>,
    private readonly residentInstall?: ResidentLifecycleInstall,
  ) {}

  completesInstallJournal() {
    return true;
  }

  async installed(nodeRef: NodeRef, encoded: Bytes): Promise<void> {
    const storage = this.storage.deref();
    if (!storage) throw backend("database storage closed during chunk installation");
    const install = this.residentInstall;
    let release: LockRelease | undefined;
    if (!install || !install.lifecycleHeld.value) {
      const lifecycle = this.lifecycle.deref();
      if (!lifecycle) throw backend("database lifecycle closed during chunk installation");
      release = await lifecycle.lock();
    }
    try {
      const readStorage: OrderedKvStorage =
        install && !install.durable.value ? install.storage.storage : storage;
      let node: any;
      try {
        node = decodeNodeUntypedAuthenticated(nodeRef.objectHash, encoded);
      } catch {
        throw ChunkError.integrity();
      }
      const children = uniqueLargeValueChildren(node);
      const nodeKey = asBackend(() => largeValueNodeKey(nodeRef));
      const existing = await asBackendAsync(readStorage.get(LARGE_VALUE_METADATA_CF, nodeKey));
      const metadata = existing
        ? asBackend(() => decodeLargeValueNodeReferences(existing))
        : emptyNodeReferences();
      const existingChildren = canonicalLargeValueChildren(metadata.children);
      if (
        existingChildren.length &&
        (existingChildren.length !== children.length ||
          existingChildren.some((c, i) => cmpNodeRef(c, children[i]) !== 0))
      )
        throw ChunkError.integrity();
      const newlyDiscoveredActiveChildren =
        metadata.references > 0 && !existingChildren.length && children.length > 0;
      metadata.children = children;

      const rootKey = asBackend(() => largeValueRootKey(nodeRef));
      const rootEncoded = await asBackendAsync(readStorage.get(LARGE_VALUE_METADATA_CF, rootKey));
      const rootReferences = rootEncoded
        ? asBackend(() => decodeLargeValueRootReferences(rootEncoded))
        : emptyRootReferences();
      const activateRoot =
        rootReferences.durable + rootReferences.staged > 0 && !rootReferences.nodeActive;
      if (activateRoot) rootReferences.nodeActive = true;
      const transitions: [NodeRef, number][] = [];
      if (activateRoot) transitions.push([nodeRef, 1]);
      if (newlyDiscoveredActiveChildren) for (const child of children) transitions.push([child, 1]);
      const operations = await asBackendAsync(
        largeValueNodeTransitionOperations(
          readStorage,
          new SortedMap(cmpNodeRef, [[nodeRef, metadata]]),
          transitions,
          true,
        ),
      );
      if (activateRoot)
        operations.push({
          kind: "Set",
          cf: LARGE_VALUE_METADATA_CF,
          key: rootKey,
          value: asBackend(() => encodeLargeValueRootReferences(rootReferences)),
        });
      // The recovery marker and node/root metadata share one durability boundary.
      operations.push({
        kind: "Delete",
        cf: LARGE_VALUE_METADATA_CF,
        key: asBackend(() => largeValuePendingInstallKey(nodeRef)),
      });
      if (!install) return await asBackendAsync(storage.writeMany(operations));
      if (!install.durable.value) {
        install.staged.extend(operations);
        return;
      }
      try {
        await storage.writeMany(operations);
      } catch (error) {
        const chunkError = backend(String((error as Error)?.message ?? error));
        install.installFailures.record(nodeRef, chunkError);
        throw chunkError;
      }
    } finally {
      release?.();
    }
  }
}

// ---- publications ----

export type AppliedBatchLifecycle =
  | "Applied"
  | "Persisting"
  | "PersistenceComplete"
  | "Finished"
  | "Abandoned";

export interface PersistenceOrder {
  ownerWaiter?: () => void;
  next: number;
  waiters: Map<number, () => void>;
  failure?: string;
}

export function failPublicationOrder(order: PersistenceOrder, message: string) {
  order.failure = message;
  const waiters = [...order.waiters.values()];
  order.waiters.clear();
  for (const w of waiters) w();
}

export function wakePublicationOwner(order: PersistenceOrder) {
  const w = order.ownerWaiter;
  order.ownerWaiter = undefined;
  w?.();
}

type Flag = { value: boolean };
type Cell<T> = { value: T };

/** One resident publication whose ordered storage write can progress independently. */
export class AppliedBatch {
  constructor(
    readonly publication: PublicationId,
    readonly storage: Cell<LayoutStorage | undefined>,
    readonly operations: StagedWriteState,
    readonly residentInstallDurable: Flag | undefined,
    readonly order: PersistenceOrder,
    readonly ivmTickTime: number,
    readonly tick: TickMetrics,
    readonly changedTables: string[],
    readonly notificationsDeferred: boolean,
    readonly lifecycle: Cell<AppliedBatchLifecycle>,
    readonly abandonedApplication: Flag,
    readonly indexIds: { value: any },
  ) {}

  async persist(): Promise<PersistedBatch> {
    const previous = this.lifecycle.value;
    this.lifecycle.value = "Persisting";
    if (previous !== "Applied")
      throw new Error("an applied batch may have only one persistence attempt at a time");
    const storage = this.storage.value;
    if (!storage) throw new Error("an applied publication retains storage until settlement");
    let turn: StorageError | undefined;
    for (;;) {
      if (this.order.failure !== undefined) {
        turn = StorageError.backend("publication order", this.order.failure);
        break;
      }
      if (this.order.next === this.publication) break;
      await new Promise<void>((r) => this.order.waiters.set(this.publication, r));
    }
    const operations = this.operations.snapshot().flat();
    const storageWrites = StorageWriteMetrics.fromOperations(operations, this.indexIds.value);
    const storageStart = performance.now();
    const outcome = turn
      ? ({ kind: "Uncommitted", error: turn } as const)
      : await storage.writeManyBorrowedOutcome(operations);
    let result: StorageError | undefined;
    if (outcome.kind !== "Committed") {
      result = outcome.error;
      // Runtime state already reflects this publication; without a definite
      // non-commit receipt every entry point must fail closed.
      if (outcome.kind === "PossiblyCommitted") this.abandonedApplication.value = true;
    }
    const storageWriteTime = performance.now() - storageStart;
    if (!result && this.residentInstallDurable) this.residentInstallDurable.value = true;
    this.lifecycle.value = "PersistenceComplete";
    if (!result) {
      this.order.next++;
      const waiter = this.order.waiters.get(this.order.next);
      this.order.waiters.delete(this.order.next);
      waiter?.();
    } else failPublicationOrder(this.order, result.message);
    if (this.abandonedApplication.value) wakePublicationOwner(this.order);
    return new PersistedBatch(
      this.publication,
      result,
      this.notificationsDeferred,
      {
        storageWriteTime,
        ivmTickTime: this.ivmTickTime,
        storageWriteCount: storageWrites.total.count,
        storageWriteBytes: storageWrites.total.bytes,
        storageWrites,
        tick: this.tick,
      },
      new PersistenceReceipt(this.storage, this.lifecycle, this.order, this.abandonedApplication),
    );
  }

  /** Rust `Drop`: an application abandoned before persistence fails closed. */
  dispose() {
    if (this.lifecycle.value !== "Applied") return;
    this.lifecycle.value = "Abandoned";
    this.abandonedApplication.value = true;
    failPublicationOrder(
      this.order,
      `publication PublicationId(${this.publication}) application was abandoned before persistence`,
    );
    wakePublicationOwner(this.order);
  }
}

/** Fail closed if a host-owned durable/runtime application is abandoned. */
export class HostApplicationGuard {
  private completed = false;
  constructor(
    private readonly order: PersistenceOrder,
    private readonly abandonedApplication: Flag,
  ) {}
  complete() {
    this.completed = true;
  }
  dispose() {
    if (this.completed) return;
    this.abandonedApplication.value = true;
    wakePublicationOwner(this.order);
  }
}

/** Completion of one owned publication persistence operation. */
export class PersistedBatch {
  constructor(
    readonly publication: PublicationId,
    readonly result: StorageError | undefined,
    readonly notificationsDeferred: boolean,
    readonly metrics: CommitMetrics,
    readonly receipt: PersistenceReceipt,
  ) {}
}

export class PersistenceReceipt {
  constructor(
    readonly storage: Cell<LayoutStorage | undefined>,
    readonly lifecycle: Cell<AppliedBatchLifecycle>,
    readonly order: PersistenceOrder,
    readonly abandonedApplication: Flag,
  ) {}
  finish() {
    this.lifecycle.value = "Finished";
    // A settled handle must not pin a replaced database runtime.
    this.storage.value = undefined;
  }
  dispose() {
    if (this.lifecycle.value !== "PersistenceComplete") return;
    this.lifecycle.value = "Abandoned";
    this.abandonedApplication.value = true;
    wakePublicationOwner(this.order);
  }
}

/** An in-memory checkpoint of the schema-derived IVM registry. */
export interface RuntimeRegistryCheckpoint {
  ivmRuntime: IvmRuntime;
  storedRecordDescriptors: Map<string, Map<number, RecordDescriptor>>;
}

// ---- database ----

/** Schema-aware database facade over storage and IVM subscriptions. */
export class Database {
  chunkStorage!: ChunkStorage;
  chunkResolver!: MissingChunkResolver;
  lastCommitMetrics?: CommitMetrics;
  lastTickMetrics?: TickMetrics;
  /** Dense record descriptors per table variant, interned beside the schema. */
  storedRecordDescriptors = new Map<string, Map<number, RecordDescriptor>>();
  nextPublicationId = 1;
  immutableBatchOwner: object = {};
  /** Rotated whenever physical record admission can change. */
  batchPreparationOwner: object = {};
  durablePublicationFrontier?: PublicationId;
  residentPublications = new SortedMap<PublicationId, StagedWriteState>(cmpNum);
  persistedPublications = new Set<PublicationId>();
  residentWrites = new StagedWriteState();
  publicationPersistence: PersistenceOrder = { next: 1, waiters: new Map() };
  /** Serializes the durable upload journal, blob staging, expiry, promotion and reclamation. */
  largeValueLifecycle = new AsyncLock();
  /** Held while resident publications carry a not-yet-durable root/node transition. */
  largeValuePublicationLifecycleGuard?: LockRelease;
  largeValueLifecycleHeld: Flag = { value: false };
  largeValueLifecyclePublications = new Set<PublicationId>();
  abandonedApplication: Flag = { value: false };
  poisoned = false;
  storageExtractionPrepared = false;

  constructor(
    public storage: LayoutStorage,
    public ivmRuntime: IvmRuntime,
    public storageReadMetrics: ReadMetricsSink,
  ) {}
}

export interface Database
  extends
    CommitMethods,
    FacadeMethods,
    PrimaryStorageMethods,
    QueryMethods,
    SchemaAdmissionMethods,
    EncodingMethods {}

for (const mixin of [
  CommitMethods,
  FacadeMethods,
  PrimaryStorageMethods,
  QueryMethods,
  SchemaAdmissionMethods,
  EncodingMethods,
])
  for (const name of Object.getOwnPropertyNames(mixin.prototype))
    if (name !== "constructor")
      Object.defineProperty(
        Database.prototype,
        name,
        Object.getOwnPropertyDescriptor(mixin.prototype, name)!,
      );
