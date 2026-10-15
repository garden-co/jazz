import { type Bytes, equalBytes, utf8, fromUtf8 } from "../../support/bytes.js";
import {
  type Value,
  type TableSchema,
  type IndexSchema,
  type PrimaryKeyColumn,
  RecordDescriptor,
  OwnedRecord,
  VariantRecord,
  recordsArityMismatch,
  recordsFieldNotFound,
  splitVariantRecord,
  splitDurableIndexKey,
  encodeKeyPart,
  u48BeBytes,
  u48FromBeBytes,
  columnTypeOfKey,
} from "../../_extern/groove-db-storage.js";
import { type Database, DbError } from "./index.js";
import type { RawRecordInput, RecordInput } from "./batch.js";
import { EncodedKeyValue } from "./storage_helpers.js";

type Resolved = [variantTag: number, descriptor: RecordDescriptor, record: Bytes];
const kindOf = (t: any): string => (typeof t === "string" ? t : t.kind);

function wrap<T>(f: () => T): T {
  try {
    return f();
  } catch (error) {
    throw DbError.from(error);
  }
}

export function resolveRecordInput(
  table: TableSchema,
  input: RecordInput,
  descriptor: RecordDescriptor,
): Resolved {
  if (input.kind === "Values")
    return [0, descriptor, encodeRecord(table, descriptor, input.values)];
  return resolveVariantRecord(table, input.record, descriptor);
}

export function resolveRawRecordInput(
  table: TableSchema,
  input: RawRecordInput,
  descriptor: RecordDescriptor,
): Resolved {
  switch (input.kind) {
    case "Payload":
      return [0, descriptor, input.payload];
    case "Record":
      return resolveVariantRecord(table, input.record, descriptor);
    case "ValidatedRecord": {
      const tag = input.record.variantTag();
      if (!input.record.descriptor().registryCompatibleWith(descriptor))
        throw new DbError("SchemaVersionDescriptorMismatch", { table: table.name, version: tag });
      return [tag, descriptor, input.record.record().raw()];
    }
  }
}

export function resolveVariantRecord(
  table: TableSchema,
  record: VariantRecord,
  descriptor: RecordDescriptor,
): Resolved {
  const tag = record.variantTag();
  if (!record.record().descriptor().registryCompatibleWith(descriptor))
    throw new DbError("SchemaVersionDescriptorMismatch", { table: table.name, version: tag });
  wrap(() => record.record().validate());
  return [tag, descriptor, record.record().raw()];
}

export function encodeRecord(
  table: TableSchema,
  descriptor: RecordDescriptor,
  values: Value[],
): Bytes {
  const columns = table.columns;
  if (columns.length !== values.length)
    throw DbError.from(recordsArityMismatch(columns.length, values.length));
  const fields = descriptor.fields();
  // Ordinary table writes already have declaration order.
  if (
    fields.length === columns.length &&
    fields.every((f: any, i: number) => f.name === columns[i].name)
  )
    return wrap(() => descriptor.create(values));
  // Variant descriptors can select or reorder declaration fields.
  const ordered = fields.map((field: any) => {
    const name = field.name;
    if (name === undefined) throw DbError.from(recordsFieldNotFound("<unnamed>"));
    const idx = columns.findIndex((c: any) => c.name === name);
    if (idx < 0) throw DbError.from(recordsFieldNotFound(name));
    if (idx >= values.length)
      throw DbError.from(recordsArityMismatch(columns.length, values.length));
    return values[idx];
  });
  return wrap(() => descriptor.create(ordered));
}

export function primaryKeyBytes(
  table: TableSchema,
  variantTag: number,
  schema: RecordDescriptor,
  record: Bytes,
): Bytes {
  const primaryKey = table.primaryKey;
  if (!primaryKey) throw new DbError("MissingPrimaryKey", table.name);
  const out: number[] = [];
  for (const column of primaryKey.columns) {
    const localName = !table.variants.length
      ? column.column
      : table.variant(variantTag)?.payloadNameForShared(column.column);
    if (localName === undefined)
      throw new DbError("SchemaVersionMissingPrimaryKey", {
        table: table.name,
        version: variantTag,
        column: column.column,
      });
    const value = wrap(() => schema.get(record, localName));
    ensurePrimaryKeyValueType(table, column, value);
    encodePrimaryKeyPart(out, value);
  }
  return Uint8Array.from(out);
}

export function validateRawPrimaryKey(
  table: TableSchema,
  tag: number,
  schema: RecordDescriptor,
  record: Bytes,
  key: Bytes,
) {
  if (!equalBytes(primaryKeyBytes(table, tag, schema, record), key))
    throw new DbError(
      "InvalidDirectRecordStoreKey",
      `raw record primary key mismatch for table ${table.name}`,
    );
}

export class EncodingMethods {
  decodeStoredKeyValue(
    this: Database,
    table: TableSchema,
    key: Bytes,
    stored: Bytes,
  ): EncodedKeyValue {
    const [variantTag, payload] = wrap(() => splitVariantRecord(stored));
    let variants = this.storedRecordDescriptors.get(table.name);
    let descriptor = variants?.get(variantTag);
    if (!descriptor) {
      descriptor = table.recordSchemaForVariant(variantTag);
      if (!descriptor)
        throw new DbError("UnknownTableVariant", { table: table.name, version: variantTag });
      if (!variants) this.storedRecordDescriptors.set(table.name, (variants = new Map()));
      variants.set(variantTag, descriptor!);
    }
    return new EncodedKeyValue(
      key,
      new VariantRecord(variantTag, new OwnedRecord(payload.slice(), descriptor)),
    );
  }
}

export type KeyCursor = { bytes: Bytes; pos: number };

/** Rebuild the full encoded primary key of one durable index entry. */
export function persistedIndexPrimaryKey(
  table: TableSchema,
  indexName: string,
  index: IndexSchema,
  storageKey: Bytes,
  storedValue: Bytes,
): Bytes {
  const invalid = () => new DbError("InvalidPersistedIndex", indexName);
  const logical = persistedIndexLogicalKey(indexName, storageKey);
  const primaryKey = table.primaryKey;
  if (!primaryKey) throw new DbError("MissingPrimaryKey", table.name);
  const cursor: KeyCursor = { bytes: logical, pos: 0 };
  const indexValues = decodeIndexColumns(table, indexName, index, cursor);
  const remaining = logical.subarray(cursor.pos);
  const coversPrimaryKey = primaryKey.columns.every((c: any) => index.columns.includes(c.column));
  let suffix: Bytes;
  if (coversPrimaryKey) {
    if (remaining.length || storedValue.length) throw invalid();
    suffix = new Uint8Array();
  } else if (index.unique) {
    if (remaining.length) throw invalid();
    suffix = storedValue;
  } else {
    if (remaining[0] !== 0xff || storedValue.length) throw invalid();
    suffix = remaining.subarray(1);
  }
  const s: KeyCursor = { bytes: suffix, pos: 0 };
  const out: number[] = [];
  for (const column of primaryKey.columns) {
    const position = index.columns.indexOf(column.column);
    if (position >= 0) {
      const value = indexValues[position];
      if (value === undefined) throw invalid();
      ensurePrimaryKeyValueType(table, column, value);
      encodePrimaryKeyPart(out, value);
    } else {
      const before = s.pos;
      try {
        decodePrimaryKeyPart(s, columnTypeOfKey(column.keyType));
      } catch {
        throw invalid();
      }
      out.push(...suffix.subarray(before, s.pos));
    }
  }
  if (s.pos !== suffix.length) throw invalid();
  return Uint8Array.from(out);
}

function decodeIndexColumns(
  table: TableSchema,
  indexName: string,
  index: IndexSchema,
  cursor: KeyCursor,
): Value[] {
  return index.columns.map((name: string) => {
    const column = table.columns.find((c: any) => c.name === name);
    if (!column) throw new DbError("InvalidPersistedIndex", indexName);
    return decodeIndexKeyPart(cursor, column.columnType, indexName);
  });
}

/** Decode one covered index or primary-key column without loading its row. */
export function persistedIndexColumnValue(
  table: TableSchema,
  indexName: string,
  index: IndexSchema,
  storageKey: Bytes,
  storedValue: Bytes,
  columnName: string,
): Value {
  const cursor: KeyCursor = { bytes: persistedIndexLogicalKey(indexName, storageKey), pos: 0 };
  for (const indexed of index.columns) {
    const column = table.columns.find((c: any) => c.name === indexed);
    if (!column) throw new DbError("InvalidPersistedIndex", indexName);
    const value = decodeIndexKeyPart(cursor, column.columnType, indexName);
    if (indexed === columnName) return value;
  }
  const primaryKey = table.primaryKey;
  if (!primaryKey) throw new DbError("MissingPrimaryKey", table.name);
  const primary: KeyCursor = {
    bytes: persistedIndexPrimaryKey(table, indexName, index, storageKey, storedValue),
    pos: 0,
  };
  for (const column of primaryKey.columns) {
    const value = decodePrimaryKeyPart(primary, columnTypeOfKey(column.keyType));
    if (column.column === columnName) return value;
  }
  throw new DbError("InvalidPersistedIndex", indexName);
}

/** Strip the numeric index-id prefix from a durable index storage key. */
export function persistedIndexLogicalKey(indexName: string, storageKey: Bytes): Bytes {
  const split = splitDurableIndexKey(storageKey);
  if (!split) throw new DbError("InvalidPersistedIndex", indexName);
  return split[1];
}

const PK_KIND_FOR_VALUE: Record<string, string> = {
  U8: "U8",
  U16: "U16",
  U32: "U32",
  U64: "U64",
  Bool: "Bool",
  String: "String",
  Bytes: "Bytes",
  Uuid: "Uuid",
};

export function ensurePrimaryKeyValueType(
  table: TableSchema,
  column: PrimaryKeyColumn,
  value: Value,
) {
  const kt = column.keyType;
  const keyKind = kindOf(kt) === "Integer" ? kt.width : kindOf(kt);
  if (PK_KIND_FOR_VALUE[value.kind] !== keyKind)
    throw new DbError("PrimaryKeyTypeMismatch", { table: table.name, column: column.column });
}

const be = (out: number[], width: number, v: number | bigint) => {
  let x = BigInt(v);
  const bytes: number[] = [];
  for (let i = 0; i < width; i++) {
    bytes.unshift(Number(x & 0xffn));
    x >>= 8n;
  }
  out.push(...bytes);
};
export const orderPreservingI64Bits = (v: bigint) => BigInt.asUintN(64, v) ^ (1n << 63n);
export const orderPreservingI32Bits = (v: number) => ((v >>> 0) ^ 0x80000000) >>> 0;

export function encodePrimaryKeyPart(key: number[], value: Value): void {
  switch (value.kind) {
    case "U8":
      key.push(0, value.v);
      break;
    case "U16":
      key.push(1);
      be(key, 2, value.v);
      break;
    case "U32":
      key.push(2);
      be(key, 4, value.v);
      break;
    case "U64":
      key.push(3);
      be(key, 8, value.v);
      break;
    case "U48":
      key.push(16, ...wrap(() => u48BeBytes(value.v)));
      break;
    case "I32":
      key.push(14);
      be(key, 4, orderPreservingI32Bits(value.v));
      break;
    case "I64":
      key.push(13);
      be(key, 8, orderPreservingI64Bits(value.v));
      break;
    case "Bool":
      key.push(5, value.v ? 1 : 0);
      break;
    case "String":
      key.push(6);
      encodeOrderedBytes(key, utf8(value.v));
      break;
    case "EnumTag":
      key.push(0, value.v);
      break;
    case "Bytes":
      key.push(7);
      encodeOrderedBytes(key, value.v);
      break;
    case "Uuid":
      key.push(10, ...value.v);
      break;
    case "Tuple":
      key.push(11);
      for (const v of value.v) encodePrimaryKeyPart(key, v);
      break;
    default:
      // F64/Array/Nullable/Record/Large/Enum have no declared total order.
      throw new DbError("InvalidDirectRecordStoreKey", "unsupported direct record store key type");
  }
}

export function encodeOrderedBytes(key: number[], value: Bytes) {
  for (const b of value) {
    if (b === 0) key.push(0, 0xff);
    else key.push(b);
  }
  key.push(0, 0);
}

// Direct-store and persisted-index decoders differ only in their error.
const directErr = (what: string) => () => new DbError("InvalidDirectRecordStoreKey", what);

function take(c: KeyCursor, len: number, err: () => DbError): Bytes {
  if (c.bytes.length - c.pos < len) throw err();
  return c.bytes.subarray(c.pos, (c.pos += len));
}
function expectTag(c: KeyCursor, expected: number, err: () => DbError, tagErr = err) {
  if (take(c, 1, err)[0] !== expected) throw tagErr();
}
function readBe(bytes: Bytes): bigint {
  let v = 0n;
  for (const b of bytes) v = (v << 8n) | BigInt(b);
  return v;
}
function decodeOrdered(c: KeyCursor, err: () => DbError, escErr = err): Bytes {
  const out: number[] = [];
  for (;;) {
    const b = take(c, 1, err)[0];
    if (b !== 0) {
      out.push(b);
      continue;
    }
    const next = take(c, 1, err)[0];
    if (next === 0) return Uint8Array.from(out);
    if (next === 0xff) out.push(0);
    else throw escErr();
  }
}

/** Shared scalar decoder for primary-key and index-key parts. */
function decodeScalar(
  c: KeyCursor,
  kind: string,
  err: () => DbError,
  tagErr: () => DbError,
  strict: boolean,
): Value | undefined {
  const tagged = (tag: number, len: number) => {
    expectTag(c, tag, err, tagErr);
    return take(c, len, err);
  };
  switch (kind) {
    case "U8":
      return { kind: "U8", v: tagged(0, 1)[0] };
    case "U16":
      return { kind: "U16", v: Number(readBe(tagged(1, 2))) };
    case "U32":
      return { kind: "U32", v: Number(readBe(tagged(2, 4))) };
    case "U64":
      return { kind: "U64", v: readBe(tagged(3, 8)) };
    case "U48":
      return { kind: "U48", v: u48FromBeBytes(tagged(16, 6)) };
    case "I32":
      return { kind: "I32", v: (Number(readBe(tagged(14, 4))) ^ 0x80000000) | 0 };
    case "I64":
      return { kind: "I64", v: BigInt.asIntN(64, readBe(tagged(13, 8)) ^ (1n << 63n)) };
    case "Bool": {
      const b = tagged(5, 1)[0];
      if (b > 1) throw strict ? directErr("bool")() : err();
      return { kind: "Bool", v: b === 1 };
    }
    case "String": {
      expectTag(c, 6, err, tagErr);
      const bytes = decodeOrdered(c, err, strict ? directErr("string") : err);
      try {
        return { kind: "String", v: fromUtf8(bytes) };
      } catch {
        throw strict ? directErr("string")() : err();
      }
    }
    case "Bytes":
      expectTag(c, 7, err, tagErr);
      return { kind: "Bytes", v: decodeOrdered(c, err, strict ? directErr("bytes") : err) };
    case "Uuid":
      return { kind: "Uuid", v: tagged(10, 16).slice() };
  }
  return undefined;
}

export function decodePrimaryKeyPart(c: KeyCursor, valueType: any): Value {
  const kind = kindOf(valueType);
  const scalar = decodeScalar(c, kind, directErr("truncated"), directErr("tag"), true);
  if (scalar) return scalar;
  if (kind === "EnumTag") {
    expectTag(c, 0, directErr("truncated"), directErr("tag"));
    return { kind: "EnumTag", v: take(c, 1, directErr("truncated"))[0] };
  }
  if (kind === "Tuple") {
    expectTag(c, 11, directErr("truncated"), directErr("tag"));
    return { kind: "Tuple", v: valueType.members.map((m: any) => decodePrimaryKeyPart(c, m)) };
  }
  throw new DbError("InvalidDirectRecordStoreKey", "unsupported direct record store key type");
}

export function decodeIndexKeyPart(c: KeyCursor, columnType: any, indexName: string): Value {
  const err = () => new DbError("InvalidPersistedIndex", indexName);
  const kind = kindOf(columnType);
  const scalar = decodeScalar(c, kind, err, err, false);
  if (scalar) return scalar;
  switch (kind) {
    case "F64": {
      expectTag(c, 4, err);
      const ordered = readBe(take(c, 8, err));
      const bits = ordered & (1n << 63n) ? ordered ^ (1n << 63n) : ~ordered & 0xffffffffffffffffn;
      const view = new DataView(new ArrayBuffer(8));
      view.setBigUint64(0, bits);
      const value = view.getFloat64(0);
      if (Number.isNaN(value)) throw err();
      return { kind: "F64", v: value };
    }
    case "EnumTag": {
      expectTag(c, 0, err);
      const discriminant = take(c, 1, err)[0];
      try {
        columnType.schema.variant(discriminant);
      } catch {
        throw err();
      }
      return { kind: "EnumTag", v: discriminant };
    }
    case "Tuple":
      expectTag(c, 11, err);
      return {
        kind: "Tuple",
        v: columnType.members.map((m: any) => decodeIndexKeyPart(c, m, indexName)),
      };
    case "Nullable": {
      const marker = take(c, 1, err)[0];
      if (marker === 8) return { kind: "Nullable", v: null };
      if (marker === 9)
        return { kind: "Nullable", v: decodeIndexKeyPart(c, columnType.inner, indexName) };
      throw err();
    }
  }
  throw err();
}

let indexDescriptor: RecordDescriptor | undefined;
export function indexRecordDescriptor(): RecordDescriptor {
  return (indexDescriptor ??= new RecordDescriptor([
    ["key", "Bytes"],
    ["value", "Bytes"],
  ]));
}

export function encodeIndexPrefixPart(key: number[], value: Value, columnType: any): void {
  const ct = kindOf(columnType);
  const ivm = (v: Value) => {
    try {
      encodeKeyPart(key, v);
    } catch (error) {
      throw new DbError("IvmRuntime", error);
    }
  };
  if (ct === "EnumTag" && value.kind === "String") {
    const d = wrap(() => columnType.schema.discriminant(value.v));
    return ivm({ kind: "U8", v: d });
  }
  if (ct === "EnumTag" && value.kind === "EnumTag") {
    try {
      columnType.schema.variant(value.v);
    } catch (error) {
      throw new DbError("RecordEncoding", error);
    }
    return ivm({ kind: "U8", v: value.v });
  }
  if (ct === "Nullable" && value.kind === "Nullable") {
    if (value.v === null) return ivm(value);
    key.push(9);
    return encodeIndexPrefixPart(key, value.v, columnType.inner);
  }
  ivm(value);
}
