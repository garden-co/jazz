// Storage and wire codec helpers for node-owned records: groove record layouts,
// typed row wrappers, storage keys, catalogue payloads and contribution provenance.
import {
  type Bytes,
  equalBytes,
  compareBytes,
  utf8,
  fromUtf8,
  concatBytes,
} from "../../support/bytes.js";
import {
  SortedMap,
  SortedSet,
  cmpNum,
  cmpStr,
  cmpBytes,
  cmpTuple,
} from "../../support/collections.js";
import { uuidV5 } from "../../support/hash.js";
import { unreachable } from "../../support/runtime.js";
import {
  nodeError,
  V,
  VT,
  RecordDescriptor,
  OwnedRecord,
  EnumValue,
  PK,
  PredicateExpr,
  ProjectField,
  GraphBuilder,
  RowAuthor,
  BranchKey,
  VersionRecord,
  CurrentRow,
  HistoryEntry,
  RejectionReason,
  SCHEMA_FIELDS,
  CATALOGUE_SCHEMA_VERSION,
  canonicalCatalogueSchemaBytes,
  decodePublicSchemaJson,
  canonicalLensBytes,
  decodeCanonicalLensBytes,
  publicationContentId,
  SchemaVersion,
  ActiveSchema,
  contributionMergeStorageType,
  validateContributionMergeProvenance,
  encodePersistedRecordDescriptor,
  storageColumnType,
  columnStorageValue,
  largeValuesPhysicalStorageValueType,
  descriptorEquals,
  valueEquals,
  valueTypeEquals,
  transactionEquals,
  cmpVersionRecord,
  txTimeFromPhysicalMs,
  txTimePhysicalMs,
  txTimeSortKey,
  durabilityRank,
  durabilityFromDiscriminant,
  layerStack,
  mergeOps,
  exclusiveReadEvidence,
  userColumnField,
  FieldIdentity,
  type Value,
  type ValueType,
  type Uuid,
  type TxId,
  type Transaction,
  type Fate,
  type DurabilityTier,
  type GrooveTableSchema,
  type PrimaryKeyValue,
  type CommitUnitIngestContext,
} from "../../_extern/node-physical-ingest.js";
import type {
  SchemaPhysicalMapping,
  TablePhysicalMapping,
  GlobalScalarEnumCaseId,
  GlobalEnumCaseId,
  PhysicalVariantCase,
} from "./physical/catalogue.js";

const invalid = (m: string) => nodeError("InvalidStoredValue", m);

// ---- groove::define_record! layouts ----
const HISTORY_PREFIX = [
  "branch_key",
  "row_uuid",
  "tx_time",
  "tx_node_id",
  "schema_version",
  "created_by",
  "created_at",
  "updated_by",
  "updated_at",
  "_deletion",
];
export const HistoryRowRecord = {
  FIELD_BRANCH_KEY_IDX: 0,
  FIELD_ROW_UUID_IDX: 1,
  FIELD_TX_TIME_IDX: 2,
  FIELD_TX_NODE_ID_IDX: 3,
  FIELD_SCHEMA_VERSION_IDX: 4,
  FIELD_CREATED_BY_IDX: 5,
  FIELD_CREATED_AT_IDX: 6,
  FIELD_UPDATED_BY_IDX: 7,
  FIELD_UPDATED_AT_IDX: 8,
  FIELD__DELETION_IDX: 9,
  USER_CELLS: 10,
  PREFIX_FIELD_NAMES: HISTORY_PREFIX,
};
export const GlobalCurrentRowRecord = {
  FIELD_TX_TIME_IDX: 2,
  FIELD_TX_NODE_ID_IDX: 3,
  FIELD_CREATED_AT_IDX: 6,
  FIELD_UPDATED_BY_IDX: 7,
  FIELD_UPDATED_AT_IDX: 8,
  FIELD_GLOBAL_TIME_IDX: 9,
  FIELD__DELETION_IDX: 10,
  USER_CELLS: 11,
  PREFIX_FIELD_NAMES: [...HISTORY_PREFIX.slice(0, 9), "global_time", "_deletion"],
};
export const CurrentRowRecord = { FIELD_ROW_UUID_IDX: 0, USER_CELLS: 1 };
export const WireRowRecord = { USER_CELLS: 6 };
export const TransactionRowRecord = {
  FIELD_TIME_IDX: 0,
  FIELD_NODE_ID_IDX: 1,
  FIELD_FATE_IDX: 13,
  FIELD_REJECTION_REASON_IDX: 15,
  FIELD_DURABILITY_IDX: 16,
};
export const RejectedTransactionRowRecord = { FIELD_TIME_IDX: 0, FIELD_NODE_ID_IDX: 1 };
export const RejectedVersionRowRecord = {
  FIELD_TX_TIME_IDX: 0,
  FIELD_TX_NODE_ID_IDX: 1,
  FIELD_ROW_UUID_IDX: 2,
  USER_CELLS: 4,
  PREFIX_FIELD_NAMES: ["tx_time", "tx_node_id", "row_uuid", "_deletion"],
};
export const NodeAliasRowRecord = { FIELD_ID_IDX: 0, FIELD_UUID_IDX: 1 };
export const SchemaVersionAliasRowRecord = {
  FIELD_ID_IDX: 0,
  FIELD_UUID_IDX: 1,
  FIELD_PHYSICAL_MAPPING_IDX: 2,
};
export const CatalogueRowRecord = { FIELD_KIND_IDX: 0, FIELD_ID_IDX: 1, FIELD_PAYLOAD_IDX: 2 };

export type FateTag = "Pending" | "Accepted" | "Rejected";
export const FATE_TAGS: FateTag[] = ["Pending", "Accepted", "Rejected"];

/** Node-local touched-row lists, one record per transaction. */
export const TX_TOUCHED_ROWS_TABLE = "jazz_tx_touched_rows";
const TX_TOUCHED_ROWS_LIST_IDX = 2;

type TouchedKey = [number, Bytes];
const cmpTouchedKey = cmpTuple(cmpNum, cmpBytes) as (a: TouchedKey, b: TouchedKey) => number;

/** The history rows one transaction wrote on this node, keyed exactly as history is. */
export class TouchedRows {
  readonly groups = new SortedMap<TouchedKey, SortedSet<Uuid>>(cmpTouchedKey);
  insert(table: number, branchKey: Bytes, row: Uuid): void {
    this.groups.getOrInsert([table, branchKey], () => new SortedSet(cmpBytes)).add(row);
  }
  extend(other: TouchedRows): void {
    for (const [[table, branch], rows] of other.groups)
      for (const row of rows) this.insert(table, branch, row);
  }
  *iter(): IterableIterator<[number, Bytes, Uuid]> {
    for (const [[table, branch], rows] of this.groups)
      for (const row of rows) yield [table, branch, row];
  }
  private static groupDescriptor() {
    return RecordDescriptor.new_([
      ["physical_table_id", VT.U64],
      ["branch_key", VT.Bytes],
      ["row_uuids", VT.array(VT.Uuid)],
    ]);
  }
  toValue(): Value {
    const descriptor = TouchedRows.groupDescriptor();
    const groups: Value[] = [];
    for (const [[table, branch], rows] of this.groups) {
      const values = [V.u64(table), V.bytes(branch), V.array([...rows].map((r) => V.uuid(r)))];
      groups.push(V.record(OwnedRecord.new_(descriptor.create(values), descriptor)));
    }
    return V.array(groups);
  }
  static fromListRecord(record: any): TouchedRows {
    return TouchedRows.fromValue(record.getIdx(TX_TOUCHED_ROWS_LIST_IDX));
  }
  listRecordValues(txTime: bigint, txNodeAlias: number): Value[] {
    return [V.u64(txTime), V.u64(txNodeAlias), this.toValue()];
  }
  private static fromValue(value: Value): TouchedRows {
    if (value.kind !== "Array") throw invalid("transaction touched_rows must be an array");
    const touched = new TouchedRows();
    let previous: TouchedKey | undefined;
    for (const group of value.value as Value[]) {
      if (group.kind !== "Record") throw invalid("transaction touched_rows group must be a record");
      const g = group.value;
      const key: TouchedKey = [g.getU64(0), g.getBytes(1).slice()];
      const rows = g.getIdx(2);
      if (rows.kind !== "Array")
        throw invalid("transaction touched_rows group rows must be an array");
      const uuids = new SortedSet<Uuid>(cmpBytes);
      let last: Uuid | undefined;
      for (const row of rows.value as Value[]) {
        if (row.kind !== "Uuid") throw invalid("transaction touched row must be a UUID");
        if (last && compareBytes(last, row.value) >= 0)
          throw invalid("transaction touched rows are not strictly increasing");
        last = row.value;
        uuids.add(row.value);
      }
      if (uuids.size === 0 || (previous && cmpTouchedKey(previous, key) >= 0))
        throw invalid("transaction touched_rows groups are empty or not strictly increasing");
      previous = key;
      touched.groups.set(key, uuids);
    }
    return touched;
  }
}

/** Permanent discriminators for the hard-coded catalogue kernel. */
export const CATALOGUE_RECORD_KINDS = [
  "Genesis",
  "Schema",
  "Lens",
  "SchemaLineageStaged",
  "SchemaLineagePending",
  "SchemaLineageActive",
  "WritePointerPending",
  "BootstrapReady",
  "ActiveSchema",
] as const;
export type CatalogueRecordKind = (typeof CATALOGUE_RECORD_KINDS)[number];
export const catalogueRecordKindKey = (k: CatalogueRecordKind): number =>
  CATALOGUE_RECORD_KINDS.indexOf(k);
export function catalogueRecordKindFromKey(key: number): CatalogueRecordKind {
  const k = CATALOGUE_RECORD_KINDS[key];
  if (k === undefined) throw nodeError("Records", "NonCanonicalRecord");
  return k;
}

const CATALOGUE_BOOTSTRAP_READY_VERSION = 1;
const CATALOGUE_WRITE_POINTER_VERSION = 1;
const CATALOGUE_LINEAGE_ACTIVATION_VERSION = 1;
const CATALOGUE_PUBLICATION_VERSION = 2;
const CATALOGUE_STAGED_LINEAGE_VERSION = 1;
const CATALOGUE_PENDING_LINEAGE_VERSION = 1;
const PHYSICAL_MAPPING_VERSION = 1;
const MAPPING_CONTEXT = "invalid physical mapping payload";

/** Little-endian byte sink for the fixed catalogue kernel payloads. */
class Payload {
  private parts: number[] = [];
  constructor(version?: number) {
    if (version !== undefined) this.parts.push(version);
  }
  u8(v: number) {
    this.parts.push(v & 0xff);
  }
  u32(v: number) {
    for (let i = 0; i < 4; i++) this.parts.push((v >>> (8 * i)) & 0xff);
  }
  u64(v: number | bigint) {
    let b = BigInt(v);
    for (let i = 0; i < 8; i++) {
      this.parts.push(Number(b & 0xffn));
      b >>= 8n;
    }
  }
  raw(b: Bytes) {
    for (const x of b) this.parts.push(x);
  }
  len(length: number, context: string) {
    if (length > 0xffffffff) throw invalid(context);
    this.u32(length);
  }
  str(value: string, context: string) {
    const b = utf8(value);
    this.len(b.length, context);
    this.raw(b);
  }
  sized(b: Bytes, context: string) {
    this.len(b.length, context);
    this.raw(b);
  }
  finish(): Bytes {
    return Uint8Array.from(this.parts);
  }
}

class CataloguePayloadCursor {
  private offset = 1;
  constructor(
    private readonly payload: Bytes,
    version: number,
    private readonly context: string,
  ) {
    if (payload[0] !== version) throw invalid(context);
  }
  bytes(length: number): Bytes {
    const end = this.offset + length;
    if (end > this.payload.length) throw invalid(this.context);
    const out = this.payload.subarray(this.offset, end);
    this.offset = end;
    return out;
  }
  uuid(): Uuid {
    return this.bytes(16).slice();
  }
  u64(): number {
    const b = this.bytes(8);
    return Number(new DataView(b.buffer, b.byteOffset, 8).getBigUint64(0, true));
  }
  u32(): number {
    const b = this.bytes(4);
    return new DataView(b.buffer, b.byteOffset, 4).getUint32(0, true);
  }
  string(): string {
    try {
      return fromUtf8(this.sizedBytes());
    } catch {
      throw invalid(this.context);
    }
  }
  sizedBytes(): Bytes {
    return this.bytes(this.u32());
  }
  finish(): void {
    if (this.offset !== this.payload.length) throw invalid(this.context);
  }
}

function requireStrictlyIncreasing(previous: string | undefined, value: string, context: string) {
  if (previous !== undefined && previous >= value) throw invalid(context);
}

/** Encode node-local physical storage metadata as one typed, canonical value. */
export function encodePhysicalMapping(mapping: SchemaPhysicalMapping): Bytes {
  const p = new Payload(PHYSICAL_MAPPING_VERSION);
  encodePhysicalIdentityManifest(p, mapping.identities);
  p.len(mapping.tables.size, "physical mapping table count");
  for (const [tableName, table] of mapping.tables) {
    p.str(tableName, "physical mapping table name");
    p.u64(table.tableId);
    p.len(table.columns.size, "physical mapping column count");
    for (const [columnName, columnId] of table.columns) {
      p.str(columnName, "physical mapping column name");
      p.u64(columnId);
    }
    p.len(table.variantCases.length, "physical mapping variant case count");
    for (const c of table.variantCases) {
      p.u32(c.tag);
      p.len(c.fields.size, "physical mapping variant field count");
      for (const f of c.fields) p.str(f, "physical mapping variant field");
    }
    encodeDirectRegistries(p, table.scalarEnumCases, encodeScalarCaseId);
    encodeDirectRegistries(p, table.payloadEnumCases, encodePayloadCaseId);
    encodeNestedRegistries(p, table.nestedScalarEnumCases, encodeScalarCaseId);
    encodeNestedRegistries(p, table.nestedPayloadEnumCases, encodePayloadCaseId);
  }
  return p.finish();
}

export function decodePhysicalMapping(payload: Bytes): SchemaPhysicalMapping {
  if (payload[0] !== PHYSICAL_MAPPING_VERSION) throw invalid(MAPPING_CONTEXT);
  const cursor = new CataloguePayloadCursor(payload, PHYSICAL_MAPPING_VERSION, MAPPING_CONTEXT);
  const identities = decodePhysicalIdentityManifest(cursor);
  const tableCount = cursor.u32();
  const tables = new SortedMap<string, TablePhysicalMapping>(cmpStr);
  let previousTable: string | undefined;
  for (let t = 0; t < tableCount; t++) {
    const tableName = cursor.string();
    requireStrictlyIncreasing(previousTable, tableName, MAPPING_CONTEXT);
    previousTable = tableName;
    const tableId = cursor.u64();
    if (tableId === 0) throw invalid(MAPPING_CONTEXT);
    const columnCount = cursor.u32();
    const columns = new SortedMap<string, number>(cmpStr);
    let previousColumn: string | undefined;
    for (let c = 0; c < columnCount; c++) {
      const name = cursor.string();
      requireStrictlyIncreasing(previousColumn, name, MAPPING_CONTEXT);
      previousColumn = name;
      const id = cursor.u64();
      if (id === 0 || columns.set(name, id) !== undefined) throw invalid(MAPPING_CONTEXT);
    }
    const variantCount = cursor.u32();
    const variantCases: PhysicalVariantCase[] = [];
    for (let v = 0; v < variantCount; v++) {
      const tag = cursor.u32();
      const fieldCount = cursor.u32();
      const fields = new SortedSet<string>(cmpStr);
      let previousField: string | undefined;
      for (let f = 0; f < fieldCount; f++) {
        const field = cursor.string();
        requireStrictlyIncreasing(previousField, field, MAPPING_CONTEXT);
        previousField = field;
        if (!fields.add(field)) throw invalid(MAPPING_CONTEXT);
      }
      variantCases.push({ tag, fields });
    }
    const scalarEnumCases = decodeDirectRegistries(cursor, decodeScalarCaseId);
    const payloadEnumCases = decodeDirectRegistries(cursor, decodePayloadCaseId);
    const nestedScalarEnumCases = decodeNestedRegistries(cursor, decodeScalarCaseId);
    const nestedPayloadEnumCases = decodeNestedRegistries(cursor, decodePayloadCaseId);
    if (
      tables.set(tableName, {
        tableId,
        columns,
        variantCases,
        scalarEnumCases,
        payloadEnumCases,
        nestedScalarEnumCases,
        nestedPayloadEnumCases,
      }) !== undefined
    )
      throw invalid(MAPPING_CONTEXT);
  }
  cursor.finish();
  return { identities, tables };
}

function encodePhysicalIdentityManifest(p: Payload, manifest: any): void {
  p.len(manifest.tables.size, "physical identity table count");
  for (const [tableName, table] of manifest.tables) {
    p.str(tableName, "physical identity table name");
    p.raw(table.id);
    p.len(table.columns.size, "physical identity column count");
    for (const [columnName, column] of table.columns) {
      p.str(columnName, "physical identity column name");
      p.raw(column.id);
      p.len(column.enumVariants.size, "physical identity enum occurrence count");
      for (const [path, variants] of column.enumVariants) {
        p.str(path, "physical identity enum path");
        p.len(variants.length, "physical identity enum case count");
        for (const v of variants) p.raw(v);
      }
    }
  }
}

function decodePhysicalIdentityManifest(cursor: CataloguePayloadCursor): any {
  const tables = new SortedMap<string, any>(cmpStr);
  let previousTable: string | undefined;
  const tableCount = cursor.u32();
  for (let t = 0; t < tableCount; t++) {
    const tableName = cursor.string();
    requireStrictlyIncreasing(previousTable, tableName, MAPPING_CONTEXT);
    previousTable = tableName;
    const id = cursor.uuid();
    const columns = new SortedMap<string, any>(cmpStr);
    let previousColumn: string | undefined;
    const columnCount = cursor.u32();
    for (let c = 0; c < columnCount; c++) {
      const columnName = cursor.string();
      requireStrictlyIncreasing(previousColumn, columnName, MAPPING_CONTEXT);
      previousColumn = columnName;
      const columnId = cursor.uuid();
      const enumVariants = new SortedMap<string, Uuid[]>(cmpStr);
      let previousPath: string | undefined;
      const occurrenceCount = cursor.u32();
      for (let o = 0; o < occurrenceCount; o++) {
        const path = cursor.string();
        requireStrictlyIncreasing(previousPath, path, MAPPING_CONTEXT);
        previousPath = path;
        const caseCount = cursor.u32();
        const variants: Uuid[] = [];
        for (let i = 0; i < caseCount; i++) variants.push(cursor.uuid());
        enumVariants.set(path, variants);
      }
      columns.set(columnName, { id: columnId, enumVariants });
    }
    tables.set(tableName, { id, columns });
  }
  return { tables };
}

function encodeScalarCaseId(p: Payload, c: GlobalScalarEnumCaseId) {
  p.raw(c.id);
  p.raw(c.introducingSchema);
  p.u8(c.introducingOrdinal);
}
function encodePayloadCaseId(p: Payload, c: GlobalEnumCaseId) {
  p.raw(c.id);
  p.raw(c.introducingSchema);
  p.u32(c.introducingOrdinal);
}
function decodeScalarCaseId(cursor: CataloguePayloadCursor): GlobalScalarEnumCaseId {
  return {
    id: cursor.uuid(),
    introducingSchema: cursor.uuid(),
    introducingOrdinal: cursor.bytes(1)[0],
  };
}
function decodePayloadCaseId(cursor: CataloguePayloadCursor): GlobalEnumCaseId {
  return { id: cursor.uuid(), introducingSchema: cursor.uuid(), introducingOrdinal: cursor.u32() };
}

function encodeCases<C>(p: Payload, cases: C[], enc: (p: Payload, c: C) => void) {
  p.len(cases.length, "physical mapping enum case count");
  for (const c of cases) enc(p, c);
}
function encodeDirectRegistries<C>(
  p: Payload,
  registries: SortedMap<number, C[]>,
  enc: (p: Payload, c: C) => void,
) {
  p.len(registries.size, "physical mapping direct registry count");
  for (const [columnId, cases] of registries) {
    p.u64(columnId);
    encodeCases(p, cases, enc);
  }
}
function encodeNestedRegistries<C>(
  p: Payload,
  registries: SortedMap<number, SortedMap<string, C[]>>,
  enc: (p: Payload, c: C) => void,
) {
  p.len(registries.size, "physical mapping nested registry count");
  for (const [columnId, paths] of registries) {
    p.u64(columnId);
    p.len(paths.size, "physical mapping nested registry path count");
    for (const [path, cases] of paths) {
      p.str(path, "physical mapping nested registry path");
      encodeCases(p, cases, enc);
    }
  }
}
function decodeCases<C extends { id: Uuid }>(
  cursor: CataloguePayloadCursor,
  dec: (c: CataloguePayloadCursor) => C,
): C[] {
  const count = cursor.u32();
  const cases: C[] = [];
  const unique = new SortedSet<Uuid>(cmpBytes);
  for (let i = 0; i < count; i++) {
    const c = dec(cursor);
    if (!unique.add(c.id)) throw invalid(MAPPING_CONTEXT);
    cases.push(c);
  }
  return cases;
}
function decodeRegistryColumn(
  cursor: CataloguePayloadCursor,
  previous: number | undefined,
): number {
  const id = cursor.u64();
  if (id === 0 || (previous !== undefined && id <= previous)) throw invalid(MAPPING_CONTEXT);
  return id;
}
function decodeDirectRegistries<C extends { id: Uuid }>(
  cursor: CataloguePayloadCursor,
  dec: (c: CataloguePayloadCursor) => C,
): SortedMap<number, C[]> {
  const count = cursor.u32();
  const registries = new SortedMap<number, C[]>(cmpNum);
  let previous: number | undefined;
  for (let i = 0; i < count; i++) {
    const id = decodeRegistryColumn(cursor, previous);
    previous = id;
    if (registries.set(id, decodeCases(cursor, dec)) !== undefined) throw invalid(MAPPING_CONTEXT);
  }
  return registries;
}
function decodeNestedRegistries<C extends { id: Uuid }>(
  cursor: CataloguePayloadCursor,
  dec: (c: CataloguePayloadCursor) => C,
): SortedMap<number, SortedMap<string, C[]>> {
  const count = cursor.u32();
  const registries = new SortedMap<number, SortedMap<string, C[]>>(cmpNum);
  let previousColumn: number | undefined;
  for (let i = 0; i < count; i++) {
    const id = decodeRegistryColumn(cursor, previousColumn);
    previousColumn = id;
    const pathCount = cursor.u32();
    const paths = new SortedMap<string, C[]>(cmpStr);
    let previousPath: string | undefined;
    for (let j = 0; j < pathCount; j++) {
      const path = cursor.string();
      requireStrictlyIncreasing(previousPath, path, MAPPING_CONTEXT);
      previousPath = path;
      if (paths.set(path, decodeCases(cursor, dec)) !== undefined) throw invalid(MAPPING_CONTEXT);
    }
    if (registries.set(id, paths) !== undefined) throw invalid(MAPPING_CONTEXT);
  }
  return registries;
}

export function encodeCatalogueSchema(schema: any): Bytes {
  try {
    return canonicalCatalogueSchemaBytes(schema);
  } catch {
    throw invalid("encode catalogue public schema");
  }
}

export function decodeCatalogueSchema(payload: Bytes): { id: Uuid; schema: any } {
  const cursor = new CataloguePayloadCursor(
    payload,
    CATALOGUE_SCHEMA_VERSION,
    "invalid catalogue schema payload",
  );
  const id = cursor.uuid();
  const publicSchema = cursor.sizedBytes();
  cursor.finish();
  let schema: any;
  try {
    schema = decodePublicSchemaJson(publicSchema);
  } catch {
    throw invalid("invalid catalogue schema public schema");
  }
  const canonical = utf8(JSON.stringify(schema.publicSchema()));
  if (!equalBytes(canonical, publicSchema))
    throw invalid("non-canonical catalogue schema public schema");
  if (!equalBytes(schema.versionId(), id)) throw invalid("catalogue schema content id mismatch");
  return { id, schema };
}

export type CurrentWriteSchema = { revision: number; schema: Uuid };
export type CatalogueBootstrapReady = {
  genesis: Uuid;
  currentWriteSchema: CurrentWriteSchema;
  activeCatalogueSeq: number;
};

export function encodeCatalogueBootstrapReady(ready: CatalogueBootstrapReady): Bytes {
  const p = new Payload(CATALOGUE_BOOTSTRAP_READY_VERSION);
  p.raw(ready.genesis);
  p.u64(ready.currentWriteSchema.revision);
  p.raw(ready.currentWriteSchema.schema);
  p.u64(ready.activeCatalogueSeq);
  return p.finish();
}

export function decodeCatalogueBootstrapReady(payload: Bytes): CatalogueBootstrapReady {
  const cursor = new CataloguePayloadCursor(
    payload,
    CATALOGUE_BOOTSTRAP_READY_VERSION,
    "invalid catalogue bootstrap receipt payload",
  );
  const genesis = cursor.uuid();
  const revision = cursor.u64();
  const schema = cursor.uuid();
  const activeCatalogueSeq = cursor.u64();
  cursor.finish();
  return { genesis, currentWriteSchema: { revision, schema }, activeCatalogueSeq };
}

export function catalogueWritePointerId(pointer: CurrentWriteSchema): Uuid {
  const p = new Payload();
  p.u64(pointer.revision);
  return uuidV5(pointer.schema, p.finish());
}

export function decodeCatalogueWritePointer(payload: Bytes): CurrentWriteSchema {
  const cursor = new CataloguePayloadCursor(
    payload,
    CATALOGUE_WRITE_POINTER_VERSION,
    "invalid catalogue write-pointer payload",
  );
  const revision = cursor.u64();
  const schema = cursor.uuid();
  cursor.finish();
  return { revision, schema };
}

export type SchemaLineageActivation = { id: Uuid; catalogueSeq: number };
export function encodeCatalogueLineageActivation(a: SchemaLineageActivation): Bytes {
  const p = new Payload(CATALOGUE_LINEAGE_ACTIVATION_VERSION);
  p.raw(a.id);
  p.u64(a.catalogueSeq);
  return p.finish();
}
export function decodeCatalogueLineageActivation(payload: Bytes): SchemaLineageActivation {
  const cursor = new CataloguePayloadCursor(
    payload,
    CATALOGUE_LINEAGE_ACTIVATION_VERSION,
    "invalid catalogue lineage activation payload",
  );
  const id = cursor.uuid();
  const catalogueSeq = cursor.u64();
  cursor.finish();
  return { id, catalogueSeq };
}

export function encodeCatalogueLens(lens: any): Bytes {
  return concatBytes(Uint8Array.of(1), canonicalLensBytes(lens));
}
export function decodeCatalogueLens(payload: Bytes): any {
  if (payload[0] !== 1) throw invalid("invalid catalogue lens payload");
  try {
    return decodeCanonicalLensBytes(payload.subarray(1));
  } catch {
    throw invalid("invalid catalogue lens payload");
  }
}

function sortedUniqueStrings(values: string[], duplicate: string): string[] {
  const sorted = [...values].sort(cmpStr);
  for (let i = 1; i < sorted.length; i++) if (sorted[i - 1] === sorted[i]) throw invalid(duplicate);
  return sorted;
}

function encodeCataloguePublication(publication: any): Bytes {
  const schema = encodeCatalogueSchema(publication.schema);
  const p = new Payload(CATALOGUE_PUBLICATION_VERSION);
  p.raw(publication.id);
  p.sized(schema, "catalogue publication schema length");
  const predecessors = [...publication.predecessors].sort((a: any, b: any) =>
    compareBytes(a.lens.source, b.lens.source),
  );
  if (
    predecessors.length === 0 ||
    predecessors.some((x, i) => i > 0 && equalBytes(predecessors[i - 1].lens.source, x.lens.source))
  )
    throw invalid("invalid schema predecessors");
  p.len(predecessors.length, "catalogue predecessor count");
  for (const predecessor of predecessors) {
    p.sized(encodeCatalogueLens(predecessor.lens), "catalogue publication lens length");
    const newTables = sortedUniqueStrings(
      predecessor.newTables,
      "catalogue publication duplicate new table",
    );
    p.len(newTables.length, "catalogue publication new table count");
    for (const t of newTables) p.str(t, "catalogue publication table");
    const dropped = sortedUniqueStrings(
      predecessor.droppedTables,
      "catalogue publication duplicate dropped table",
    );
    p.len(dropped.length, "catalogue publication dropped table count");
    for (const t of dropped) p.str(t, "catalogue publication table");
  }
  encodePhysicalIdentityManifest(p, publication.physicalIdentities);
  return p.finish();
}

function decodeCataloguePublication(payload: Bytes): any {
  const CONTEXT = "invalid catalogue protocol publication";
  const cursor = new CataloguePayloadCursor(payload, CATALOGUE_PUBLICATION_VERSION, CONTEXT);
  const id = cursor.uuid();
  const schema = decodeCatalogueSchema(cursor.sizedBytes());
  const count = cursor.u32();
  if (count === 0) throw invalid(CONTEXT);
  const predecessors: any[] = [];
  for (let i = 0; i < count; i++)
    predecessors.push({
      lens: decodeCatalogueLens(cursor.sizedBytes()),
      newTables: decodeSortedStrings(cursor, CONTEXT),
      droppedTables: decodeSortedStrings(cursor, CONTEXT),
    });
  const physicalIdentities = decodePhysicalIdentityManifest(cursor);
  cursor.finish();
  const publication = { id, schema, predecessors, physicalIdentities };
  if (
    !equalBytes(publication.id, publicationContentId(publication)) ||
    !equalBytes(encodeCataloguePublication(publication), payload)
  )
    throw invalid(CONTEXT);
  return publication;
}

function decodeSortedStrings(cursor: CataloguePayloadCursor, context: string): string[] {
  const values: string[] = [];
  let previous: string | undefined;
  const n = cursor.u32();
  for (let i = 0; i < n; i++) {
    const value = cursor.string();
    requireStrictlyIncreasing(previous, value, context);
    previous = value;
    values.push(value);
  }
  return values;
}

export type StagedSchemaLineage = {
  catalogueSeq: number;
  publication: any;
  alias: number;
  mapping: SchemaPhysicalMapping;
};
export type PendingSchemaLineage = { catalogueSeq: number; publication: any };

export function encodeCatalogueStagedLineage(staged: StagedSchemaLineage): Bytes {
  return encodeStagedWithMapping(staged, encodePhysicalMapping(staged.mapping));
}
function encodeStagedWithMapping(staged: StagedSchemaLineage, mapping: Bytes): Bytes {
  const publication = encodeCataloguePublication(staged.publication);
  const p = new Payload(CATALOGUE_STAGED_LINEAGE_VERSION);
  p.u64(staged.catalogueSeq);
  p.sized(publication, "staged catalogue publication length");
  p.u64(staged.alias);
  p.sized(mapping, "staged catalogue mapping length");
  return p.finish();
}
export function decodeCatalogueStagedLineage(payload: Bytes): StagedSchemaLineage {
  const CONTEXT = "invalid staged catalogue lineage";
  const cursor = new CataloguePayloadCursor(payload, CATALOGUE_STAGED_LINEAGE_VERSION, CONTEXT);
  const catalogueSeq = cursor.u64();
  const publication = decodeCataloguePublication(cursor.sizedBytes());
  const alias = cursor.u64();
  const mappingPayload = cursor.sizedBytes();
  const mapping = decodePhysicalMapping(mappingPayload);
  cursor.finish();
  const staged = { catalogueSeq, publication, alias, mapping };
  if (
    !equalBytes(encodePhysicalMapping(mapping), mappingPayload) ||
    !equalBytes(encodeStagedWithMapping(staged, mappingPayload), payload)
  )
    throw invalid(CONTEXT);
  return staged;
}

export function encodeCataloguePendingLineage(pending: PendingSchemaLineage): Bytes {
  const p = new Payload(CATALOGUE_PENDING_LINEAGE_VERSION);
  p.u64(pending.catalogueSeq);
  p.sized(encodeCataloguePublication(pending.publication), "pending catalogue publication length");
  return p.finish();
}
export function decodeCataloguePendingLineage(payload: Bytes): PendingSchemaLineage {
  const CONTEXT = "invalid pending catalogue lineage";
  const cursor = new CataloguePayloadCursor(payload, CATALOGUE_PENDING_LINEAGE_VERSION, CONTEXT);
  const catalogueSeq = cursor.u64();
  const publication = decodeCataloguePublication(cursor.sizedBytes());
  cursor.finish();
  const pending = { catalogueSeq, publication };
  if (!equalBytes(encodeCataloguePendingLineage(pending), payload)) throw invalid(CONTEXT);
  return pending;
}

/** Active-schema record v1: version, revision u64 LE, schema payload length u32 LE, schema payload. */
export function encodeActiveSchema(active: any): Bytes {
  const schema = encodeCatalogueSchema(SchemaVersion.new_(active.compiled));
  if (schema.length > 0xffffffff) throw invalid("active schema too large");
  const p = new Payload(1);
  p.u64(active.revision);
  p.sized(schema, "active schema too large");
  return p.finish();
}
export function decodeActiveSchema(bytes: Bytes): any {
  const cursor = new CataloguePayloadCursor(bytes, 1, "invalid active schema payload");
  const revision = cursor.u64();
  const length = cursor.u32();
  const schema = decodeCatalogueSchema(cursor.bytes(length));
  cursor.finish();
  return ActiveSchema.new_({ revision, schema: schema.id }, schema.schema);
}

// ---- node-side version record construction ----
export function versionRecordFromCommit(commit: any, table: any, schemaVersion: Uuid): any {
  try {
    txTimeFromPhysicalMs(commit.nowMs);
  } catch {
    throw nodeError(
      "InvalidMergeableCommit",
      "commit now_ms exceeds packed HLC physical-millisecond range",
    );
  }
  const positional = positionalCellsFromMap(table, commit.cells);
  return VersionRecord.encode(
    table,
    schemaVersion,
    commit.rowUuid,
    commit.madeBy,
    commit.nowMs,
    commit.madeBy,
    commit.nowMs,
    positional,
    commit.deletion,
  ).withAuthoredColumns(commit.authoredColumns);
}

export function versionRecordFromStored(
  stored: VersionRow,
  table: any,
  schemaVersion: Uuid,
  authoredColumns: Set<string> | undefined,
): any {
  const descriptor = versionRecordDescriptors(table)[1];
  const input = stored.record;
  const raw = descriptor.createWithEncodedFields(
    input.raw.length,
    (index: number, output: number[]) => {
      // Both stored layouts share the provenance prefix. Timestamps are packed HLC values in storage.
      const source =
        index === 0
          ? HistoryRowRecord.FIELD_ROW_UUID_IDX
          : index === 1
            ? HistoryRowRecord.FIELD_CREATED_BY_IDX
            : index === 5
              ? HistoryRowRecord.FIELD__DELETION_IDX
              : index >= WireRowRecord.USER_CELLS
                ? HistoryRowRecord.USER_CELLS + index - WireRowRecord.USER_CELLS
                : undefined;
      if (source !== undefined) {
        const [s, e] = input.descriptor.fieldSpan(input.raw, source);
        output.push(...input.raw.subarray(s, e));
        return;
      }
      const value =
        index === 2
          ? V.u64(txTimePhysicalMs(stored.createdAt()))
          : index === 3
            ? rowAuthorValue(stored.updatedBy())
            : index === 4
              ? V.u64(txTimePhysicalMs(stored.updatedAt()))
              : V.nullable();
      descriptor.encodeFieldInto(index, value, output);
    },
  );
  return VersionRecord.new_(table.name, schemaVersion, OwnedRecord.new_(raw, descriptor))
    .withBranchKey(stored.branchKey)
    .withAuthoredColumns(authoredColumns)
    .withBase(stored.base())
    .withCounterSigns(stored.counterSigns());
}

/** Debug-only layout assertion; compiled out of release builds. */
export function debugAssertLoweredLayouts(_schema: any): void {}

export type StoredTransaction = {
  tx: Transaction;
  nodeAlias: number;
  fate: Fate;
  globalTime: bigint | undefined;
  durability: DurabilityTier;
  isPartial: boolean;
};

/** Merge incoming status without undoing an already known outcome, global time, or durability. */
export function reconcileFate(
  stored: StoredTransaction,
  fate: Fate,
  globalTime?: bigint,
  durability?: DurabilityTier,
): void {
  if (stored.globalTime !== undefined && globalTime !== undefined && globalTime < stored.globalTime)
    throw nodeError("NonMonotoneState", "global seq cannot move backwards");
  stored.fate = nextFate(stored.fate, fate);
  stored.globalTime = globalTime ?? stored.globalTime;
  if (durability !== undefined && durabilityRank(durability) > durabilityRank(stored.durability))
    stored.durability = durability;
}

export function storedTransactionToRecord(s: StoredTransaction): any {
  return {
    txId: s.tx.txId,
    madeBy: s.tx.madeBy,
    kind: s.tx.kind,
    nTotalWrites: s.tx.nTotalWrites,
    fate: s.fate,
    globalTime: s.globalTime,
    durability: s.durability,
    userMetadataJson: s.tx.userMetadataJson,
    contributionMerge: s.tx.contributionMerge,
  };
}

export type VersionRowParts = {
  table: string;
  branchKey: any;
  rowUuid: Uuid;
  txNodeAlias: number;
  schemaVersionAlias: number;
  txTime: bigint;
  createdBy: any;
  createdAt: bigint;
  updatedBy: any;
  updatedAt: bigint;
  cells: Map<string, Value>;
  authoredColumns: SortedSet<number> | undefined;
  deletion: "Deleted" | "Restored" | undefined;
  seq: bigint;
  base: RowBase;
  counterSigns: Bytes;
};
export type RowBase = { seq: bigint | undefined; pending: TxId | undefined };

// Record layout depends on the table name, ordered column shape and merge strategies.
type HistoryDescriptorCacheEntry = {
  tableName: string;
  columns: [string, any, any, any][];
  descriptor: any;
  wireDescriptor: any;
};
const historyDescriptorCache: HistoryDescriptorCacheEntry[] = [];

export const preparedWireRecordDescriptor = (table: any) => versionRecordDescriptors(table)[1];
export const historyRecordDescriptor = (table: any) => versionRecordDescriptors(table)[0];

function versionRecordDescriptors(table: any): [any, any] {
  const hit = historyDescriptorCache.find(
    (e) =>
      e.tableName === table.name &&
      e.columns.length === table.columns.length &&
      e.columns.every(
        ([name, ty, kind, strategy], i) =>
          name === table.columns[i].name &&
          valueTypeEquals(ty, table.columns[i].columnType) &&
          kind === table.columns[i].largeValueKind &&
          strategy === table.mergeStrategy(table.columns[i].name),
      ),
  );
  if (hit) return [hit.descriptor, hit.wireDescriptor];
  const descriptor = table.authoredHistoryStorageTable().recordSchema();
  const wireDescriptor = table.wireRecordDescriptor();
  // Bound retained preparation data; eviction affects preparation cost only.
  if (historyDescriptorCache.length === 128) historyDescriptorCache.shift();
  historyDescriptorCache.push({
    tableName: table.name,
    columns: table.columns.map((c: any) => [
      c.name,
      c.columnType,
      c.largeValueKind,
      table.mergeStrategy(c.name),
    ]),
    descriptor,
    wireDescriptor,
  });
  return [descriptor, wireDescriptor];
}

export class VersionRow {
  constructor(
    readonly table: string,
    readonly branchKey: any,
    readonly record: any,
  ) {}

  /** The same row image with its record fields replaced, keeping the layout. */
  withRecordValues(values: Value[]): VersionRow {
    return new VersionRow(
      this.table,
      this.branchKey,
      ownedRecordFromStorageValuesWithDescriptor(this.record.descriptor, values),
    );
  }

  static fromParts(table: any, parts: VersionRowParts, historyDescriptor?: any): VersionRow {
    const values = historyValuesFromParts(table, parts);
    const record = ownedRecordFromStorageValuesWithDescriptor(
      historyDescriptor ?? historyRecordDescriptor(table),
      values,
    );
    return new VersionRow(parts.table, parts.branchKey, record);
  }

  static fromWire(
    table: any,
    version: any,
    authoredColumns: SortedSet<number> | undefined,
    txNodeAlias: number,
    schemaVersionAlias: number,
    txTime: bigint,
    seq: bigint,
  ): VersionRow {
    if (!version.branchKey().isCanonical())
      throw nodeError("InvalidMergeableCommit", "row version branch key is not canonical");
    const deletion = version.deletion();
    mergeOps.validateCounterSigns(table, version.counterSigns());
    const ncols = table.columns.length;
    const firstMergeField = HistoryRowRecord.USER_CELLS + ncols + 2;
    const descriptor = historyRecordDescriptor(table);
    const source = version.record();
    const sourceDescriptor = source.descriptor;
    for (const author of [version.createdBy(), version.updatedBy()]) {
      try {
        RowAuthor.fromPersistedSubject(author);
      } catch {
        throw nodeError("UnadmittedWriteAuthor");
      }
    }
    const createdAt = wireMs(version.createdAtMs(), "wire created_at_ms exceeds packed HLC range");
    const updatedAt = wireMs(version.updatedAtMs(), "wire updated_at_ms exceeds packed HLC range");
    const raw = descriptor.createWithEncodedFields(
      source.raw.length + 128,
      (index: number, output: number[]) => {
        const sourceIndex =
          index === 1
            ? 0
            : index === 5
              ? 1
              : index === 7
                ? 3
                : index === 9
                  ? 5
                  : index >= HistoryRowRecord.USER_CELLS &&
                      index < HistoryRowRecord.USER_CELLS + ncols
                    ? index - HistoryRowRecord.USER_CELLS + 6
                    : undefined;
        if (sourceIndex !== undefined) {
          const f = sourceDescriptor.fields()[sourceIndex];
          if (f && valueTypeEquals(f.valueType, descriptor.fields()[index].valueType)) {
            const [s, e] = sourceDescriptor.fieldSpan(source.raw, sourceIndex);
            output.push(...source.raw.subarray(s, e));
            return;
          }
        }
        let value: Value;
        switch (index) {
          case 0:
            value = V.bytes(version.branchKey().canonicalBytes());
            break;
          case 1:
            value = V.uuid(version.rowUuid());
            break;
          case 2:
            value = V.u64(txTime);
            break;
          case 3:
            value = V.u64(txNodeAlias);
            break;
          case 4:
            value = V.u64(schemaVersionAlias);
            break;
          case 5:
            value = rowAuthorValue(version.createdBy());
            break;
          case 6:
            value = V.u64(createdAt);
            break;
          case 7:
            value = historyUpdatedByValue(version.updatedBy());
            break;
          case 8:
            value = V.u64(updatedAt);
            break;
          case 9:
            value = nullableDeletionValue(deletion);
            break;
          default:
            if (index < HistoryRowRecord.USER_CELLS + ncols) {
              const column = index - HistoryRowRecord.USER_CELLS;
              const cell = version.optionalCellAt(column);
              if (cell !== undefined) validateCellValue(table.columns[column], cell);
              value = V.nullable(cell);
            } else if (index === HistoryRowRecord.USER_CELLS + ncols)
              value = authoredColumnIdsValue(authoredColumns);
            else if (index === HistoryRowRecord.USER_CELLS + ncols + 1)
              value = V.bytes(version.counterSigns());
            else
              switch (index - firstMergeField) {
                case 0:
                  value = V.u64(seq);
                  break;
                case 1:
                  value = baseSeqValue(version.base());
                  break;
                case 2:
                  value = basePendingValue(version.base());
                  break;
                default:
                  throw invalid("history layout does not end with the merge fields");
              }
        }
        descriptor.encodeFieldInto(index, value, output);
      },
    );
    return new VersionRow(version.table(), version.branchKey(), OwnedRecord.new_(raw, descriptor));
  }

  rowUuid(): Uuid {
    return this.record.getUuid(HistoryRowRecord.FIELD_ROW_UUID_IDX);
  }
  txNodeAlias(): number {
    return this.record.getU64(HistoryRowRecord.FIELD_TX_NODE_ID_IDX);
  }
  txTime(): bigint {
    return BigInt(this.record.getU64(HistoryRowRecord.FIELD_TX_TIME_IDX));
  }
  validateCanonical(): void {
    validateCanonicalVersionParts(this.branchKey);
  }
  createdBy(): any {
    return RowAuthor.fromRecord(
      this.record.getRecord(HistoryRowRecord.FIELD_CREATED_BY_IDX),
    ).asAuthorSubject();
  }
  createdAt(): bigint {
    return BigInt(this.record.getU64(HistoryRowRecord.FIELD_CREATED_AT_IDX));
  }
  /** History storage omits `updated_by` when it is the transaction's author; reads fill it back in. */
  updatedBy(): any {
    const author = RowAuthor.readOptional(this.record, HistoryRowRecord.FIELD_UPDATED_BY_IDX);
    if (author === undefined)
      throw new Error("history updated_by is resolved when the image is read");
    return author.asAuthorSubject();
  }
  updatedByIsImplicit(): boolean {
    const v = this.record.getIdx(HistoryRowRecord.FIELD_UPDATED_BY_IDX);
    return v.kind === "Nullable" && v.value === undefined;
  }
  updatedAt(): bigint {
    return BigInt(this.record.getU64(HistoryRowRecord.FIELD_UPDATED_AT_IDX));
  }
  schemaVersionAlias(): number {
    return this.record.getU64(HistoryRowRecord.FIELD_SCHEMA_VERSION_IDX);
  }
  deletion(): "Deleted" | "Restored" | undefined {
    const v = nullableValue(this.record.getIdx(HistoryRowRecord.FIELD__DELETION_IDX));
    return v === undefined ? undefined : deletionEventFromValue(v);
  }
  isDeleted(): boolean {
    return this.deletion() === "Deleted";
  }
  cells(table: any): Map<string, Value> {
    const cells = new Map<string, Value>();
    table.columns.forEach((column: any, idx: number) => {
      const v = nullableValue(this.record.getIdx(HistoryRowRecord.USER_CELLS + idx));
      if (v !== undefined) cells.set(column.name, v);
    });
    return cells;
  }
  cell(table: any, column: string): Value | undefined {
    const idx = table.columns.findIndex((c: any) => c.name === column);
    if (idx < 0) throw invalid("missing user column field");
    return nullableValue(this.record.getIdx(HistoryRowRecord.USER_CELLS + idx));
  }
  /** `undefined` is the legacy/lens fallback: every present cell counts as authored. */
  authoredColumnIds(): SortedSet<number> | undefined {
    const field = this.record.descriptor.fieldIndex("authored_columns");
    if (field === undefined) return undefined;
    const v = nullableValue(this.record.getIdx(field));
    return v === undefined ? undefined : authoredColumnIdsFromValue(v);
  }
  counterSigns(): Bytes {
    const v = this.trailingField(SCHEMA_FIELDS.COUNTER_SIGNS_FIELD);
    if (v.kind !== "Bytes") throw invalid("counter_signs field must be bytes");
    return v.value;
  }
  private trailingFieldIndex(name: string): number {
    const i = this.record.descriptor.fieldIndex(name);
    if (i === undefined) throw invalid("history merge field missing");
    return i;
  }
  private trailingField(name: string): Value {
    return this.record.getIdx(this.trailingFieldIndex(name));
  }
  seq(): bigint {
    const v = this.trailingField(SCHEMA_FIELDS.SEQ_FIELD);
    if (v.kind !== "U64") throw invalid("history seq must be u64");
    return BigInt(v.value);
  }
  base(): RowBase {
    const s = nullableValue(this.trailingField(SCHEMA_FIELDS.BASE_SEQ_FIELD));
    if (s !== undefined && s.kind !== "U64") throw invalid("history base_seq must be u64");
    const pending = nullableValue(this.trailingField(SCHEMA_FIELDS.BASE_PENDING_FIELD));
    return {
      seq: s === undefined ? undefined : BigInt(s.value),
      pending: pending === undefined ? undefined : txIdFromValue(pending),
    };
  }
  withMergeFields(seq: bigint, base: RowBase): VersionRow {
    const seqIdx = this.trailingFieldIndex(SCHEMA_FIELDS.SEQ_FIELD);
    const baseSeqIdx = this.trailingFieldIndex(SCHEMA_FIELDS.BASE_SEQ_FIELD);
    const basePendingIdx = this.trailingFieldIndex(SCHEMA_FIELDS.BASE_PENDING_FIELD);
    const values = this.record.toValues();
    values[seqIdx] = V.u64(seq);
    values[baseSeqIdx] = baseSeqValue(base);
    values[basePendingIdx] = basePendingValue(base);
    return this.withRecordValues(values);
  }
  withSeq(seq: bigint): VersionRow {
    return this.seq() === seq ? this : this.withMergeFields(seq, this.base());
  }
  toHistoryEntry(
    tx: StoredTransaction,
    isLocallyCurrent: boolean,
    isGloballyCurrent: boolean,
  ): any {
    return HistoryEntry.new_(
      this.table,
      this.record,
      storedTransactionToRecord(tx),
      isLocallyCurrent,
      isGloballyCurrent,
    );
  }
}

function wireMs(ms: number, message: string): bigint {
  try {
    return txTimeFromPhysicalMs(ms);
  } catch {
    throw invalid(message);
  }
}

export function ownedRecordFromStorageValues(
  storageTable: GrooveTableSchema,
  values: Value[],
): any {
  return ownedRecordFromStorageValuesWithDescriptor(storageTable.recordSchema(), values);
}
export function ownedRecordFromStorageValuesWithDescriptor(descriptor: any, values: Value[]): any {
  return OwnedRecord.new_(descriptor.create(values), descriptor);
}

export type ParkedIngressRole = "Relay" | "Authority";
export const strongestIngressRole = (
  a: ParkedIngressRole,
  b: ParkedIngressRole,
): ParkedIngressRole => (a === "Authority" || b === "Authority" ? "Authority" : "Relay");

export type ParkedCommitUnit = {
  tx: Transaction;
  versions: any[];
  nowMs: number;
  ingestContext: CommitUnitIngestContext | undefined;
  ingressRole: ParkedIngressRole;
};

export function currentVersionIndex(
  versions: VersionRow[],
  candidates: number[],
  nodeAliases: any,
): number | undefined {
  let best: number | undefined;
  let bestKey: any;
  for (const idx of candidates) {
    const txId = versionTxIdFromAliases(versions[idx], nodeAliases);
    if (!txId) throw new Error("valid version tx id");
    const key = txTimeSortKey(versions[idx].txTime(), txId.node);
    if (best === undefined || key.compare(bestKey) >= 0) {
      best = idx;
      bestKey = key;
    }
  }
  return best;
}

export function versionTxIdFromAliases(version: VersionRow, nodeAliases: any): TxId | undefined {
  const node = nodeAliases.nodeForAlias(version.txNodeAlias());
  return node === undefined ? undefined : { time: version.txTime(), node };
}

// ---- contribution provenance storage ----
let contributionMergeDescriptorCache: any;
function contributionMergeDescriptor(): any {
  if (!contributionMergeDescriptorCache) {
    const t = contributionMergeStorageType();
    if (t.kind !== "Nullable") unreachable("contribution merge storage column is nullable");
    if (t.inner.kind !== "Record")
      unreachable("contribution merge storage column contains a record");
    contributionMergeDescriptorCache = t.inner.descriptor;
  }
  return contributionMergeDescriptorCache;
}
function nestedArrayRecord(descriptor: any, index: number, what: string): any {
  const t = recordFieldType(descriptor, index);
  if (t.kind !== "Array") unreachable(`${what} is an array`);
  if (t.inner.kind !== "Record") unreachable(`${what} is a record`);
  return t.inner.descriptor;
}
const branchViewCopyDescriptor = () =>
  nestedArrayRecord(contributionMergeDescriptor(), 3, "branch-view copy");
const branchWriteIntentDescriptor = () =>
  nestedArrayRecord(contributionMergeDescriptor(), 4, "branch write intent");
function enumField(descriptor: any, index: number, what: string): any {
  const t = recordFieldType(descriptor, index);
  if (t.kind !== "Enum") unreachable(`${what} is an enum`);
  return t.schema;
}
const branchWriteOperationSchema = () =>
  enumField(branchWriteIntentDescriptor(), 5, "branch write operation");
const branchViewCopyBaseSchema = () =>
  enumField(branchViewCopyDescriptor(), 2, "branch-view copy base");

function enumCaseValue(schema: any, name: string, values: Value[]): any {
  const tag = schema.tag(name);
  const payload = schema.case(tag).payload;
  return EnumValue.new_(tag, OwnedRecord.new_(payload.create(values), payload));
}
function canonicalOr(branch: any, message: string): Bytes {
  try {
    return branch.tryCanonicalBytes();
  } catch {
    throw invalid(message);
  }
}
function branchFromCanonical(bytes: Bytes, message: string): any {
  try {
    return BranchKey.fromCanonicalBytes(bytes);
  } catch {
    throw invalid(message);
  }
}
function recordOf(descriptor: any, values: Value[]): any {
  return OwnedRecord.new_(descriptor.create(values), descriptor);
}

function branchWriteIntentStorageRecord(intent: any, evidenceIndex: number | undefined): any {
  const schema = branchWriteOperationSchema();
  const op = intent.operation;
  let operation: any;
  if (op.kind === "ExactHeadInsert" && evidenceIndex === undefined)
    operation = enumCaseValue(schema, "exact_head_insert", []);
  else if (op.kind === "ExactHeadUpdate" && evidenceIndex === undefined)
    operation = enumCaseValue(schema, "exact_head_update", []);
  else if (op.kind === "ViewUpdateCopy" && evidenceIndex !== undefined)
    operation = enumCaseValue(schema, "view_update_copy", [V.u32(evidenceIndex)]);
  else throw invalid("branch write intent evidence binding is invalid");
  return recordOf(branchWriteIntentDescriptor(), [
    V.u32(intent.version),
    V.u64(intent.physicalTableId),
    V.uuid(intent.authoredSchema),
    V.uuid(intent.rowUuid),
    V.bytes(canonicalOr(intent.head, "branch write intent head is invalid")),
    V.enumValue(operation),
  ]);
}

function branchWriteIntentFromStorageRecord(record: any, copies: any[]): any {
  if (!descriptorEquals(record.descriptor, branchWriteIntentDescriptor()))
    throw invalid("branch write intent must be a v1 record");
  const operationValue = record.getIdx(5).value;
  let c: any;
  try {
    c = branchWriteOperationSchema().case(operationValue.tag());
  } catch {
    throw invalid("branch write operation tag is invalid");
  }
  let operation: any;
  switch (c.name) {
    case "exact_head_insert":
      operation = { kind: "ExactHeadInsert" };
      break;
    case "exact_head_update":
      operation = { kind: "ExactHeadUpdate" };
      break;
    case "view_update_copy": {
      let index: Value;
      try {
        index = operationValue.intoRecord().getIdx(0);
      } catch {
        throw invalid("branch write copy index is invalid");
      }
      if (index.kind !== "U32") throw invalid("branch write copy index is invalid");
      const evidence = copies[index.value];
      if (evidence === undefined) throw invalid("branch write copy evidence is missing");
      operation = { kind: "ViewUpdateCopy", evidence };
      break;
    }
    default:
      throw invalid("branch write operation tag is invalid");
  }
  return {
    version: record.getIdx(0).value,
    physicalTableId: record.getU64(1),
    authoredSchema: record.getUuid(2),
    rowUuid: record.getUuid(3),
    head: branchFromCanonical(record.getBytes(4), "branch write intent head is invalid"),
    operation,
  };
}

function branchViewCopyBaseStorageValue(base: any): any {
  const schema = branchViewCopyBaseSchema();
  if (base.kind === "Current")
    return enumCaseValue(schema, "current", [
      V.bytes(canonicalOr(base.branch, "branch-view copy base is invalid")),
    ]);
  const tag = schema.tag("snapshot");
  const payload = schema.case(tag).payload;
  const dotDescriptor = nestedArrayRecord(payload, 4, "branch-view snapshot dots");
  const dots = base.snapshot.dots.map((dot: TxId) =>
    V.record(recordOf(dotDescriptor, [V.u64(dot.time), V.uuid(dot.node)])),
  );
  return EnumValue.new_(
    tag,
    recordOf(payload, [
      V.bytes(canonicalOr(base.branch, "branch-view copy base is invalid")),
      V.uuid(base.snapshot.owner),
      V.u64(base.snapshot.globalBase),
      V.u64(base.snapshot.localBase),
      V.array(dots),
    ]),
  );
}

function branchViewCopyStorageRecord(evidence: any): any {
  return recordOf(branchViewCopyDescriptor(), [
    V.u32(evidence.version),
    V.bytes(canonicalOr(evidence.head, "branch-view copy head is invalid")),
    V.enumValue(branchViewCopyBaseStorageValue(evidence.base)),
    V.str(evidence.table),
    V.uuid(evidence.rowUuid),
    V.u64(evidence.sourceVersion.time),
    V.uuid(evidence.sourceVersion.node),
  ]);
}

function branchViewCopyFromStorageRecord(record: any): any {
  if (!descriptorEquals(record.descriptor, branchViewCopyDescriptor()))
    throw invalid("branch-view copy evidence must be a v1 record");
  const baseValue = record.getIdx(2).value;
  let baseCase: any;
  try {
    baseCase = branchViewCopyBaseSchema().case(baseValue.tag());
  } catch {
    throw invalid("branch-view copy base tag is invalid");
  }
  let base: any;
  if (baseCase.name === "current") {
    const payload = baseValue.intoRecord();
    base = {
      kind: "Current",
      branch: branchFromCanonical(
        payload.getBytes(0),
        "branch-view copy current branch is invalid",
      ),
    };
  } else if (baseCase.name === "snapshot") {
    const payload = baseValue.intoRecord();
    const dots = (payload.getIdx(4).value as Value[]).map((v) => {
      if (v.kind !== "Record") throw invalid("branch-view copy snapshot dot must be a record");
      return { time: BigInt(v.value.getU64(0)), node: v.value.getUuid(1) };
    });
    base = {
      kind: "Snapshot",
      branch: branchFromCanonical(
        payload.getBytes(0),
        "branch-view copy snapshot branch is invalid",
      ),
      snapshot: {
        owner: payload.getUuid(1),
        globalBase: BigInt(payload.getU64(2)),
        localBase: BigInt(payload.getU64(3)),
        dots,
      },
    };
  } else throw invalid("branch-view copy base tag is invalid");
  return {
    version: record.getIdx(0).value,
    head: branchFromCanonical(record.getBytes(1), "branch-view copy head is invalid"),
    base,
    table: record.getIdx(3).value,
    rowUuid: record.getUuid(4),
    sourceVersion: { time: BigInt(record.getU64(5)), node: record.getUuid(6) },
  };
}

export function recordFieldType(descriptor: any, index: number): ValueType {
  const f = descriptor.fields()[index];
  if (!f) throw new Error("contribution storage descriptor field");
  return f.valueType;
}

type ResolveTableId = (table: string) => number;
type ResolveColumnId = (table: string, column: string) => number;
type ResolveTableName = (id: number) => string;
type ResolveColumnName = (table: string, id: number) => string;

function contributionComponentStorageValue(
  coordinate: any,
  valueType: ValueType,
  resolveColumnId: ResolveColumnId,
): any {
  if (valueType.kind !== "Enum") unreachable("contribution component uses a Groove payload enum");
  const schema = valueType.schema;
  const component = coordinate.component;
  const columnId = (name: string) => {
    const id = resolveColumnId(coordinate.table, name);
    if (id === 0) throw invalid("contribution physical column id must be nonzero");
    return id;
  };
  switch (component.kind) {
    case "Column":
      return enumCaseValue(schema, "column", [V.u64(columnId(component.column))]);
    case "Operation":
      return enumCaseValue(schema, "operation", [
        V.u64(columnId(component.column)),
        V.bytes(component.identity),
      ]);
    default:
      return enumCaseValue(schema, "register", []);
  }
}

function contributionCoordinateStorageRecord(
  coordinate: any,
  descriptor: any,
  rt: ResolveTableId,
  rc: ResolveColumnId,
): any {
  const tableId = rt(coordinate.table);
  if (tableId === 0) throw invalid("contribution physical table id must be nonzero");
  return recordOf(descriptor, [
    V.bytes(coordinate.branchKey.canonicalBytes()),
    V.u64(tableId),
    V.uuid(coordinate.rowUuid),
    coordinate.layerValue(),
    V.enumValue(contributionComponentStorageValue(coordinate, recordFieldType(descriptor, 4), rc)),
  ]);
}

function contributionDotStorageRecord(
  dot: any,
  descriptor: any,
  rt: ResolveTableId,
  rc: ResolveColumnId,
): any {
  const coordinate = (recordFieldType(descriptor, 2) as any).descriptor;
  return recordOf(descriptor, [
    V.u64(dot.txId.time),
    V.uuid(dot.txId.node),
    V.record(contributionCoordinateStorageRecord(dot.coordinate, coordinate, rt, rc)),
  ]);
}

function contributionSubstitutionStorageRecord(
  s: any,
  descriptor: any,
  rt: ResolveTableId,
  rc: ResolveColumnId,
): any {
  const target = (recordFieldType(descriptor, 0) as any).descriptor;
  const source = nestedArrayRecord(descriptor, 1, "contribution substitution sources");
  return recordOf(descriptor, [
    V.record(contributionCoordinateStorageRecord(s.target, target, rt, rc)),
    V.array(
      s.sources.map((dot: any) => V.record(contributionDotStorageRecord(dot, source, rt, rc))),
    ),
  ]);
}

export function contributionMergeStorageValue(
  provenance: any | undefined,
  rt: ResolveTableId,
  rc: ResolveColumnId,
): Value {
  if (!provenance) return V.nullable();
  const descriptor = contributionMergeDescriptor();
  const substitution = nestedArrayRecord(descriptor, 2, "contribution substitution");
  const record = recordOf(descriptor, [
    V.bytes(provenance.source.canonicalBytes()),
    V.bytes(provenance.target.canonicalBytes()),
    V.array(
      provenance.substitutions.map((s: any) =>
        V.record(contributionSubstitutionStorageRecord(s, substitution, rt, rc)),
      ),
    ),
    V.array(provenance.branchViewCopies.map((e: any) => V.record(branchViewCopyStorageRecord(e)))),
    V.array(
      provenance.branchWriteIntents.map((intent: any) => {
        const index =
          intent.operation.kind === "ViewUpdateCopy"
            ? provenance.branchViewCopies.findIndex((c: any) => c === intent.operation.evidence)
            : -1;
        return V.record(branchWriteIntentStorageRecord(intent, index < 0 ? undefined : index));
      }),
    ),
  ]);
  return V.nullable(V.record(record));
}

export function contributionComponentFromStorage(
  value: any,
  schema: any,
  table: string,
  resolveColumnName: ResolveColumnName,
): any {
  let c: any;
  try {
    c = schema.case(value.tag());
  } catch {
    throw invalid("transaction contribution component tag is invalid");
  }
  const payload = value.intoRecord();
  const column = () => {
    const id = payload.getU64(0);
    if (id === 0) throw invalid("stored contribution physical column id must be nonzero");
    return resolveColumnName(table, id);
  };
  if (c.name === "column") return { kind: "Column", column: column() };
  if (c.name === "operation")
    return { kind: "Operation", column: column(), identity: payload.getBytes(1) };
  if (c.name === "register" && payload.descriptor.fields().length === 0) {
    payload.toValues();
    return { kind: "Register" };
  }
  throw invalid("transaction contribution component payload is invalid");
}

function contributionCoordinateFromStorage(
  record: any,
  rt: ResolveTableName,
  rc: ResolveColumnName,
): any {
  const componentType = recordFieldType(record.descriptor, 4);
  if (componentType.kind !== "Enum")
    throw invalid("transaction contribution component descriptor is invalid");
  const tableId = record.getU64(1);
  if (tableId === 0) throw invalid("stored contribution physical table id must be nonzero");
  const table = rt(tableId);
  return {
    branchKey: branchFromCanonical(
      record.getBytes(0),
      "transaction contribution branch key is invalid",
    ),
    table,
    rowUuid: record.getUuid(2),
    layer: record.getIdx(3),
    component: contributionComponentFromStorage(
      record.getIdx(4).value,
      componentType.schema,
      table,
      rc,
    ),
  };
}

function recordValues(values: Value[], message: string): any[] {
  return values.map((v) => {
    if (v.kind !== "Record") throw invalid(message);
    return v.value;
  });
}

export function contributionMergeFromStorageRecord(
  record: any,
  rt: ResolveTableName,
  rc: ResolveColumnName,
): any {
  if (!descriptorEquals(record.descriptor, contributionMergeDescriptor()))
    throw invalid("transaction contribution provenance must be a record");
  const branchViewCopies = recordValues(
    record.getIdx(3).value,
    "branch-view copy evidence must be a record",
  ).map(branchViewCopyFromStorageRecord);
  const provenance = {
    source: branchFromCanonical(
      record.getBytes(0),
      "transaction contribution source branch key is invalid",
    ),
    target: branchFromCanonical(
      record.getBytes(1),
      "transaction contribution target branch key is invalid",
    ),
    substitutions: recordValues(
      record.getIdx(2).value,
      "transaction contribution substitution must be a record",
    ).map((s) => ({
      target: contributionCoordinateFromStorage(s.getIdx(0).value, rt, rc),
      sources: recordValues(s.getIdx(1).value, "transaction contribution dot must be a record").map(
        (d) => ({
          txId: { time: BigInt(d.getU64(0)), node: d.getUuid(1) },
          coordinate: contributionCoordinateFromStorage(d.getIdx(2).value, rt, rc),
        }),
      ),
    })),
    branchViewCopies,
    branchWriteIntents: recordValues(
      record.getIdx(4).value,
      "branch write intent must be a record",
    ).map((r) => branchWriteIntentFromStorageRecord(r, branchViewCopies)),
  };
  if (!validateContributionMergeProvenance(provenance))
    throw invalid("transaction contribution provenance must be canonical");
  return provenance;
}

const MAX_RUNTIME_RESULT_IDENTITY_BYTES = 1024 * 1024;

/** Encode a runtime synthetic aggregate row/replacement identity. */
export function runtimeResultIdentityBytes(value: Value, valueType: ValueType): Bytes {
  const valueDescriptor = RecordDescriptor.new_([["value", valueType]]);
  const descriptor = encodePersistedRecordDescriptor(valueDescriptor);
  const encodedValue = valueDescriptor.create([value]);
  const encoded = RecordDescriptor.new_([
    ["descriptor", VT.Bytes],
    ["value", VT.Bytes],
  ]).create([V.bytes(descriptor), V.bytes(encodedValue)]);
  if (encoded.length > MAX_RUNTIME_RESULT_IDENTITY_BYTES)
    throw invalid("settled result value encoding is too large");
  return encoded;
}

const txKindString = (tx: Transaction) => (tx.kind === "Mergeable" ? "mergeable" : "exclusive");
function txAuthorValue(tx: Transaction): Value {
  return rowAuthorValue(tx.madeBy);
}

/** Values of a `jazz_transactions` record. */
export function transactionValues(
  nodeAlias: number,
  tx: Transaction,
  fate: Fate,
  globalTime: bigint | undefined,
  durability: DurabilityTier,
  contributionMerge: Value,
  isPartial = false,
): Value[] {
  // Slots 5-8: exclusive read evidence while an exclusive fate is pending.
  const [baseSnapshot, rowReadSet, absentReadSet, predicateReadSet] =
    exclusiveReadEvidence.evidenceSlotValues(tx, fate.kind === "Pending");
  return [
    V.u64(tx.txId.time),
    V.u64(nodeAlias),
    V.str(txKindString(tx)),
    V.u32(tx.nTotalWrites),
    txAuthorValue(tx),
    baseSnapshot,
    rowReadSet,
    absentReadSet,
    predicateReadSet,
    V.nullable(tx.userMetadataJson === undefined ? undefined : V.str(tx.userMetadataJson)),
    contributionMerge,
    V.nullable(tx.permissionSubject === undefined ? undefined : tx.permissionSubject.toValue()),
    V.bool(isPartial),
    V.str(fateString(fate)),
    V.nullable(globalTime === undefined ? undefined : V.u64(globalTime)),
    V.nullable(fate.kind === "Rejected" ? RejectionReason.toStorageValue(fate.reason) : undefined),
    V.str(durabilityString(durability)),
  ];
}
export const transactionValuesWithPartialFlag = (
  nodeAlias: number,
  tx: Transaction,
  fate: Fate,
  globalTime: bigint | undefined,
  durability: DurabilityTier,
  isPartial: boolean,
  contributionMerge: Value,
) => transactionValues(nodeAlias, tx, fate, globalTime, durability, contributionMerge, isPartial);

export function rejectedTransactionValues(
  nodeAlias: number,
  tx: Transaction,
  reason: any,
): Value[] {
  return [
    V.u64(tx.txId.time),
    V.u64(nodeAlias),
    V.str(txKindString(tx)),
    txAuthorValue(tx),
    RejectionReason.toStorageValue(reason),
    V.nullable(tx.userMetadataJson === undefined ? undefined : V.str(tx.userMetadataJson)),
  ];
}

export function rejectedVersionValues(tableSchema: any, version: VersionRow): Value[] {
  const cells = version.cells(tableSchema);
  return [
    V.u64(version.txTime()),
    V.u64(version.txNodeAlias()),
    V.uuid(version.rowUuid()),
    nullableDeletionValue(version.deletion()),
    ...tableSchema.columns.map((c: any) => V.nullable(cells.get(c.name))),
  ];
}

export const fateString = (fate: Fate): string => fate.kind.toLowerCase();
export const durabilityString = (d: DurabilityTier): string => d.toLowerCase();

export function nextFate(current: Fate, incoming: Fate): Fate {
  if (current.kind === "Pending") return incoming;
  if (current.kind === "Accepted") {
    if (incoming.kind === "Rejected") throw nodeError("ConflictingFate");
    return current;
  }
  if (incoming.kind === "Accepted") throw nodeError("ConflictingFate");
  return current;
}

const READ_EVIDENCE = ["baseSnapshot", "rowReadSet", "absentReadSet", "predicateReadSet"];
const redactEvidence = (tx: Transaction) => {
  const out = { ...tx };
  for (const k of READ_EVIDENCE) out[k] = undefined;
  return out;
};
export function knownTransactionPayloadMatches(
  existing: Transaction,
  incoming: Transaction,
): boolean {
  const re = redactEvidence(existing);
  const ri = redactEvidence(incoming);
  return (
    transactionEquals(existing, incoming) ||
    transactionEquals(re, incoming) ||
    transactionEquals(existing, ri) ||
    transactionEquals(re, ri)
  );
}
/** Copy a transaction without its local-only policy capability. */
export const transactionWithoutPermissionSubject = (tx: Transaction): Transaction => ({
  ...tx,
  permissionSubject: undefined,
});
export const knownTransactionPayloadMatchesRedactedPermissionSubject = (
  a: Transaction,
  b: Transaction,
) =>
  knownTransactionPayloadMatches(
    transactionWithoutPermissionSubject(a),
    transactionWithoutPermissionSubject(b),
  );
export const knownTransactionPayloadMatchesRedactedCardinality = (a: Transaction, b: Transaction) =>
  knownTransactionPayloadMatches({ ...a, nTotalWrites: 0 }, { ...b, nTotalWrites: 0 });
export const canonicalVersions = (versions: any[]): any[] => [...versions].sort(cmpVersionRecord);

/** Reserved authored-column marker for the row image's `_deletion` cell (sorts last). */
export const DELETION_COLUMN_ID = Number.MAX_SAFE_INTEGER;
export const DELETION_COLUMN_NAME = "_deletion";

export function authoredColumnIdsFromValue(value: Value): SortedSet<number> {
  if (value.kind !== "Array")
    throw invalid("authored columns must be an array of physical column ids");
  const ids = new SortedSet<number>(cmpNum);
  let previous: number | undefined;
  for (const v of value.value as Value[]) {
    if (v.kind !== "U64") throw invalid("authored columns must contain physical column ids");
    const id = Number(v.value);
    if (id === 0) throw invalid("authored physical column ids must be nonzero");
    if (previous !== undefined && previous >= id)
      throw invalid("authored physical column ids must be strictly increasing");
    previous = id;
    ids.add(id);
  }
  return ids;
}
const authoredColumnIdsValue = (columns: SortedSet<number> | undefined): Value =>
  V.nullable(columns ? V.array([...columns].map((c) => V.u64(c))) : undefined);

export function rowAuthorValue(author: any): Value {
  try {
    return RowAuthor.fromPersistedSubject(author).toValue();
  } catch {
    throw nodeError("UnadmittedWriteAuthor");
  }
}
export const historyUpdatedByValue = (author: any): Value => V.nullable(rowAuthorValue(author));

export function historyValuesFromParts(table: any, v: VersionRowParts): Value[] {
  return [
    V.bytes(v.branchKey.canonicalBytes()),
    V.uuid(v.rowUuid),
    V.u64(v.txTime),
    V.u64(v.txNodeAlias),
    V.u64(v.schemaVersionAlias),
    rowAuthorValue(v.createdBy),
    V.u64(v.createdAt),
    historyUpdatedByValue(v.updatedBy),
    V.u64(v.updatedAt),
    nullableDeletionValue(v.deletion),
    ...table.columns.map((c: any) => V.nullable(v.cells.get(c.name))),
    authoredColumnIdsValue(v.authoredColumns),
    V.bytes(v.counterSigns),
    V.u64(v.seq),
    baseSeqValue(v.base),
    basePendingValue(v.base),
  ];
}
export const baseSeqValue = (base: RowBase): Value =>
  V.nullable(base.seq === undefined ? undefined : V.u64(base.seq));
export const basePendingValue = (base: RowBase): Value =>
  V.nullable(base.pending === undefined ? undefined : txIdValue(base.pending));
export const nullableDeletionValue = (d: "Deleted" | "Restored" | undefined): Value =>
  V.nullable(d === undefined ? undefined : deletionEventValue(d));
export const deletionEventValue = (d: "Deleted" | "Restored"): Value =>
  V.enumTag(d === "Deleted" ? 0 : 1);

/** Accepted writes are keyed by seq, pending ones (seq 0) by their transaction. */
export const historyPrimaryKey = (version: VersionRow): PrimaryKeyValue =>
  historyPrimaryKeyAt(
    version.branchKey,
    version.rowUuid(),
    version.seq(),
    version.txTime(),
    version.txNodeAlias(),
  );
export const historyPrimaryKeyAt = (
  branchKey: any,
  rowUuid: Uuid,
  seq: bigint,
  txTime: bigint,
  txNodeAlias: number,
): PrimaryKeyValue =>
  PK.composite([
    PK.bytes(branchKey.canonicalBytes()),
    PK.uuid(rowUuid),
    PK.u64(seq),
    PK.u64(txTime),
    PK.u64(txNodeAlias),
  ]);
export const historySeqCandidates = (globalTime: bigint | undefined): bigint[] =>
  globalTime !== undefined && globalTime !== 0n ? [globalTime, 0n] : [0n];
export const historyKeyValues = (
  branchKey: Bytes,
  rowUuid: Uuid,
  seq: bigint,
  txTime: bigint,
  txNodeAlias: number,
): Value[] => [V.bytes(branchKey), V.uuid(rowUuid), V.u64(seq), V.u64(txTime), V.u64(txNodeAlias)];
export const globalCurrentPrimaryKey = (branchKey: any, rowUuid: Uuid): PrimaryKeyValue =>
  PK.composite([PK.bytes(branchKey.canonicalBytes()), PK.uuid(rowUuid)]);
export const globalCurrentBranchKeyPrefix = (branchKey: any): Bytes =>
  PK.intoBytes(PK.bytes(branchKey.canonicalBytes()));
const GLOBAL_CURRENT_KEY_ROW_UUID_LEN = 1 + 16;
export const globalCurrentKeyBranchPrefix = (key: Bytes): Bytes =>
  key.subarray(0, Math.max(0, key.length - GLOBAL_CURRENT_KEY_ROW_UUID_LEN));

export const rejectedTransactionPrimaryKey = (alias: number, txId: TxId): PrimaryKeyValue =>
  PK.composite([PK.u64(txId.time), PK.u64(alias)]);
export function rejectedVersionPrimaryKeyFromRecord(record: any): PrimaryKeyValue {
  return PK.composite([
    PK.u64(expectU64(record.getIdx(RejectedVersionRowRecord.FIELD_TX_TIME_IDX), "tx_time")),
    PK.u64(expectU64(record.getIdx(RejectedVersionRowRecord.FIELD_TX_NODE_ID_IDX), "tx_node_id")),
    PK.uuid(expectUuid(record.getIdx(RejectedVersionRowRecord.FIELD_ROW_UUID_IDX), "row_uuid")),
  ]);
}

/** Row images that are not deleted; null `_deletion` is matched explicitly. */
export const notDeletedPredicate = () =>
  PredicateExpr.or([
    PredicateExpr.isNull("_deletion"),
    PredicateExpr.neq("_deletion", V.enumTag(0)),
  ]);

export function visibleCurrentGraph(table: any, settled: DurabilityTier): any {
  const userFields: string[] = table.columns.map((c: any) => userColumnField(c.name));
  const contentFields = [
    "row_uuid",
    ...userFields,
    "created_by",
    "created_at",
    "updated_by",
    "updated_at",
    "tx_time",
    "tx_node_id",
    "_deletion",
  ];
  const layers = layerStack.refLayers(settled, (r) =>
    layerStack.Layer.new_(
      GraphBuilder.table(
        r.kind === "Local" ? aheadCurrentTableName(table.name) : globalCurrentTableName(table.name),
      ).project(contentFields),
    ),
  );
  const current =
    settled === "Global"
      ? layerStack.compose(layers)
      : layerStack.compose(layers).project(contentFields);
  return current
    .filter(notDeletedPredicate())
    .projectFields([
      ProjectField.named("row_uuid"),
      ...userFields.map((f) => ProjectField.named(f)),
      ProjectField.renamed("created_by", "$createdBy"),
      ProjectField.renamed("created_at", "$createdAt"),
      ProjectField.renamed("updated_by", "$updatedBy"),
      ProjectField.renamed("updated_at", "$updatedAt"),
      ProjectField.named("tx_time"),
      ProjectField.named("tx_node_id"),
    ]);
}

export const decodeCurrentRow = (table: any, record: any) =>
  CurrentRow.new_(table.name, OwnedRecord.new_(record.raw.slice(), record.descriptor));

export function sortCurrentRows(rows: any[]): void {
  rows.sort(
    (l, r) => compareBytes(l.rowUuid(), r.rowUuid()) || compareBytes(l.record.raw, r.record.raw),
  );
}

export function currentRowFromVersionProjection(table: any, version: VersionRow): any {
  const values = currentRowPrefixAndCellsFromVersion(table, version);
  appendCurrentRowProvenance(values, version);
  return buildCurrentRow(table, values);
}
export const currentRowFromMaterializedCells = (
  table: any,
  version: VersionRow,
  cells: Map<string, Value>,
) => currentRowFromMaterializedCellsWithLayerProvenance(table, version, version, version, cells);
export const currentRowFromMaterializedCellsWithProvenance = (
  table: any,
  content: VersionRow,
  provenance: VersionRow,
  cells: Map<string, Value>,
) =>
  currentRowFromMaterializedCellsWithLayerProvenance(table, content, provenance, provenance, cells);

/** Cells and creation provenance from the content winner, update provenance from the latest layer. */
export function currentRowFromMaterializedCellsWithLayerProvenance(
  table: any,
  content: VersionRow,
  created: VersionRow,
  updated: VersionRow,
  cells: Map<string, Value>,
): any {
  return buildCurrentRow(table, [
    V.uuid(content.rowUuid()),
    ...table.columns.map((c: any) => V.nullable(cells.get(c.name))),
    rowAuthorValue(created.createdBy()),
    V.u64(txTimePhysicalMs(created.createdAt())),
    rowAuthorValue(updated.updatedBy()),
    V.u64(txTimePhysicalMs(updated.updatedAt())),
    V.u64(updated.txTime()),
    V.u64(updated.txNodeAlias()),
  ]);
}

export function currentRowFromCellsWithExplicitProvenance(
  table: any,
  rowUuid: Uuid,
  cells: Map<string, Value>,
  provenance: any,
  projectedTx?: [bigint, number],
): any {
  const [txTime, txNodeAlias] = projectedTx ?? [0n, 0];
  return buildCurrentRow(table, [
    V.uuid(rowUuid),
    ...table.columns.map((c: any) => V.nullable(cells.get(c.name))),
    rowAuthorValue(provenance.createdBy),
    V.u64(provenance.createdAt),
    rowAuthorValue(provenance.updatedBy),
    V.u64(provenance.updatedAt),
    V.u64(txTime),
    V.u64(txNodeAlias),
  ]);
}

function buildCurrentRow(table: any, values: Value[]): any {
  const descriptor = currentRowDescriptor(table);
  return CurrentRow.new_(table.name, OwnedRecord.new_(descriptor.create(values), descriptor));
}

function currentRowPrefixAndCellsFromVersion(table: any, version: VersionRow): Value[] {
  const values = [V.uuid(version.rowUuid())];
  table.columns.forEach((_: any, idx: number) =>
    values.push(
      V.nullable(nullableValue(version.record.getIdx(HistoryRowRecord.USER_CELLS + idx))),
    ),
  );
  return values;
}

function appendCurrentRowProvenance(values: Value[], p: VersionRow): void {
  values.push(
    RowAuthor.fromPersistedSubject(p.createdBy()).toValue(),
    V.u64(txTimePhysicalMs(p.createdAt())),
    RowAuthor.fromPersistedSubject(p.updatedBy()).toValue(),
    V.u64(txTimePhysicalMs(p.updatedAt())),
    V.u64(p.txTime()),
    V.u64(p.txNodeAlias()),
  );
}

/** Runtime carriers keep logical nullable wrappers while retaining indirect JSON's kind. */
export function currentRowColumnType(column: any): ValueType {
  const jsonType = (t: ValueType): ValueType =>
    t.kind === "Nullable"
      ? VT.nullable(jsonType(t.inner))
      : largeValuesPhysicalStorageValueType("Json");
  return column.largeValueKind === "Json" ? jsonType(column.columnType) : column.columnType;
}

type CurrentRowDescriptorCacheEntry = {
  tableName: string;
  columns: [string, ValueType][];
  descriptor: any;
};
const currentRowDescriptorCache: CurrentRowDescriptorCacheEntry[] = [];
function currentRowDescriptor(table: any): any {
  const hit = currentRowDescriptorCache.find(
    (e) =>
      e.tableName === table.name &&
      e.columns.length === table.columns.length &&
      e.columns.every(
        ([name, t], i) =>
          name === table.columns[i].name &&
          valueTypeEquals(t, currentRowColumnType(table.columns[i])),
      ),
  );
  if (hit) return hit.descriptor;
  const descriptor = buildCurrentRowDescriptor(table);
  currentRowDescriptorCache.push({
    tableName: table.name,
    columns: table.columns.map((c: any) => [c.name, currentRowColumnType(c)]),
    descriptor,
  });
  return descriptor;
}

function buildCurrentRowDescriptor(table: any): any {
  return RecordDescriptor.newWithFields([
    RecordDescriptor.field("row_uuid", VT.Uuid),
    ...table.columns.map((c: any) =>
      RecordDescriptor.field(
        userColumnField(c.name),
        VT.nullable(currentRowColumnType(c)),
      ).withIdentity(FieldIdentity.name(c.name)),
    ),
    RecordDescriptor.field("$createdBy", RowAuthor.valueType()),
    RecordDescriptor.field("$createdAt", VT.U64),
    RecordDescriptor.field("$updatedBy", RowAuthor.valueType()),
    RecordDescriptor.field("$updatedAt", VT.U64),
    RecordDescriptor.field("tx_time", VT.U64),
    RecordDescriptor.field("tx_node_id", VT.U64),
  ]);
}

export function currentRowFromPositionalCells(
  table: any,
  rowUuid: Uuid,
  cells: (Value | undefined)[],
): any {
  const descriptor = RecordDescriptor.new_([
    ["row_uuid", VT.Uuid],
    ...table.columns.map((c: any): [string, ValueType] => [c.name, VT.nullable(c.columnType)]),
  ]);
  const values = [
    V.uuid(rowUuid),
    ...table.columns.map((_: any, i: number) => V.nullable(cells[i])),
  ];
  // App-facing positional projection uses logical schema names, not storage carriers.
  return CurrentRow.newWithBindingFields(
    table.name,
    OwnedRecord.new_(descriptor.create(values), descriptor),
    "LogicalField",
  );
}

export function positionalCellsFromMap(
  table: any,
  cells: Map<string, Value>,
): (Value | undefined)[] {
  for (const column of cells.keys())
    if (!table.columns.some((c: any) => c.name === column))
      throw nodeError("InvalidMergeableCommit", "unknown user cell column");
  return table.columns.map((column: any) => {
    const v = cells.get(column.name);
    if (v === undefined) return undefined;
    const value = columnStorageValue(column, v);
    validateCellValue(column, value);
    return value;
  });
}

export function cellsFromPositional(table: any, cells: (Value | undefined)[]): Map<string, Value> {
  const out = new Map<string, Value>();
  table.columns.forEach((c: any, i: number) => {
    if (cells[i] !== undefined) out.set(c.name, cells[i]);
  });
  return out;
}

export function nullableValue(value: Value): Value | undefined {
  if (value.kind !== "Nullable") throw invalid("nullable value expected");
  return value.value;
}
export function validateCellValue(column: any, value: Value): void {
  RecordDescriptor.new_([["cell", storageColumnType(column)]]).create([value]);
}
export function expectU64(value: Value, field: string): number {
  if (value.kind !== "U64") throw invalid(field);
  return value.value;
}
export function expectUuid(value: Value, field: string): Uuid {
  if (value.kind !== "Uuid") throw invalid(field);
  return value.value;
}
export function validateCanonicalVersionParts(branchKey: any): void {
  try {
    branchKey.tryCanonicalBytes();
  } catch {
    throw nodeError("InvalidMergeableCommit", "row version branch key is not canonical");
  }
}
export function txIdFromValue(value: Value): TxId {
  if (value.kind !== "Tuple" || value.value.length !== 2)
    throw invalid("tx id must be tuple(u64, uuid)");
  const [time, node] = value.value;
  if (time.kind !== "U64") throw invalid("tx id time must be u64");
  if (node.kind !== "Uuid") throw invalid("tx id node must be uuid");
  return { time: BigInt(time.value), node: node.value };
}
export function txKindFromDiscriminant(value: number): "Mergeable" | "Exclusive" {
  if (value === 0) return "Mergeable";
  if (value === 1) return "Exclusive";
  throw invalid("unknown tx kind");
}
export function fateFromEncodedFields(record: any): Fate {
  switch (record.getEnum(TransactionRowRecord.FIELD_FATE_IDX)) {
    case 0:
      return { kind: "Pending" };
    case 1:
      return { kind: "Accepted" };
    case 2:
      return { kind: "Rejected", reason: rejectionReasonFromEncodedFields(record) };
    default:
      throw invalid("unknown fate");
  }
}
export function rejectionReasonFromEncodedFields(record: any): any {
  const v = record.getIdx(TransactionRowRecord.FIELD_REJECTION_REASON_IDX);
  if (v.kind !== "Nullable" || v.value === undefined)
    throw invalid("rejected transaction missing reason");
  try {
    return RejectionReason.fromStorageValue(v.value);
  } catch {
    throw invalid("invalid rejection reason");
  }
}
export function durabilityFromDiscriminantChecked(value: number): DurabilityTier {
  try {
    return durabilityFromDiscriminant(value);
  } catch {
    throw invalid("unknown durability");
  }
}
export function deletionEventFromValue(value: Value): "Deleted" | "Restored" {
  if (value.kind === "Nullable" && value.value !== undefined)
    return deletionEventFromValue(value.value);
  if (value.kind === "EnumTag" && value.value === 0) return "Deleted";
  if (value.kind === "EnumTag" && value.value === 1) return "Restored";
  throw invalid("unknown deletion event");
}
export const txIdValue = (txId: TxId): Value => V.tuple([V.u64(txId.time), V.uuid(txId.node)]);
export const globalCurrentTableName = (table: string) => `jazz_${table}_global_current`;
export const aheadCurrentTableName = (table: string) => `jazz_${table}_ahead_current`;
export { valueEquals };
