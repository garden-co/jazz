// Index scan bounds, comparison semantics, and order-preserving key encoding.
import {
  type BorrowedRecord,
  type RecordDelta,
  type RecordDeltas,
  type TableSchema,
  type Value,
  type ValueType,
  IvmRuntimeError,
  RecordDescriptor,
  RecordsError,
  decodeRecordDescriptor,
  u48BeBytes,
} from "../../../_extern/groove-ivm-plan.js";
import { compareBytes, equalBytes, toHex, utf8 } from "../../../support/bytes.js";
import { FieldRef } from "../graph.js";
import { type LiteralValue, type ValueComparison, literalToValue } from "../op_types.js";
import { resolveFieldName, resolveFieldRef } from "./record_projection.js";

/** Ordering result: -1/0/1, a SQL null, or no typed fast path. */
export type FieldLiteralOrdering = number | "SqlNull" | "Unsupported";

/** A predicate reads fields, not necessarily a materialized intermediate row. */
export interface PredicateRecord {
  value(field: string): Value;
  literalOrdering(field: string, value: LiteralValue): FieldLiteralOrdering;
}

export function borrowedPredicateRecord(record: BorrowedRecord): PredicateRecord {
  return {
    value(field) {
      const index = resolveFieldName(record.descriptor(), field);
      if (index === undefined)
        throw IvmRuntimeError.RecordEncoding(RecordsError.FieldNotFound(field));
      return record.getIdx(index);
    },
    literalOrdering(field, value) {
      const index = resolveFieldName(record.descriptor(), field);
      if (index === undefined) throw IvmRuntimeError.GraphFieldNotFound(field);
      return recordFieldLiteralOrdering(record, index, value);
    },
  };
}

export function primaryKeyValueBytes(
  descriptor: RecordDescriptor,
  record: Uint8Array,
  indices: number[],
): Uint8Array {
  const bytes: number[] = [];
  for (const idx of indices) encodeRecordFieldKeyPart(bytes, descriptor, record, idx);
  return Uint8Array.from(bytes);
}

export function primaryKeyFieldIndices(table: TableSchema, descriptor: RecordDescriptor): number[] {
  const primaryKey = table.primaryKey;
  if (!primaryKey) throw IvmRuntimeError.MissingPrimaryKey(table.name);
  return primaryKey.columns.map((c: any) => {
    const idx = descriptor.fieldIndex(c.column);
    if (idx === undefined) throw IvmRuntimeError.GraphFieldNotFound(c.column);
    return idx;
  });
}

function fieldAt(descriptor: RecordDescriptor, idx: number) {
  const field = descriptor.fields()[idx];
  if (!field) throw IvmRuntimeError.GraphFieldIndexOutOfBounds(idx);
  return field;
}

export function encodeRecordFieldKeyPart(
  key: number[],
  descriptor: RecordDescriptor,
  record: Uint8Array,
  i: number,
): void {
  const field = fieldAt(descriptor, i);
  const b = descriptor.bind(record);
  // Typed reads skip materializing a Value; bytes match encodeKeyPart.
  const t = field.valueType;
  switch (t.kind) {
    case "U8":
      return putU8(key, b.getU8(i));
    case "U32":
      return putU32(key, b.getU32(i));
    case "U64":
      return putU64(key, b.getU64(i));
    case "U48":
      return putU48(key, b.getU48(i));
    case "I32":
      return putI32(key, b.getI32(i));
    case "I64":
      return putI64(key, b.getI64(i));
    case "F64":
      return putF64(key, nonNan(b.getF64(i)));
    case "Bool":
      return putBool(key, b.getBool(i));
    case "String":
      return putStr(key, b.getStr(i));
    case "Bytes":
      return putBytes(key, b.getBytes(i));
    case "Uuid":
      return putUuid(key, b.getUuid(i));
    case "EnumTag":
      return putU8(key, b.getEnum(i));
    case "Nullable":
      switch (t.inner.kind) {
        case "U64":
          return putNullable(key, b.getNullableU64(i), putU64);
        case "I64":
          return putNullable(key, b.getNullableI64(i), putI64);
        case "I32":
          return putNullable(key, b.getNullableI32(i), putI32);
        case "F64": {
          const v = b.getNullableF64(i);
          return putNullable(key, v === undefined ? undefined : nonNan(v), putF64);
        }
        case "String":
          return putNullable(key, b.getNullableString(i), putStr);
        case "Bytes":
          return putNullable(key, b.getNullableBytes(i), putBytes);
        case "Uuid":
          return putNullable(key, b.getNullableUuid(i), putUuid);
        case "EnumTag":
          return putNullable(key, b.getNullableEnum(i), putU8);
      }
  }
  encodeKeyPart(key, namedFieldValue(descriptor, record, i));
}

export function recordFieldKeyParts(
  descriptor: RecordDescriptor,
  record: Uint8Array,
  fieldIdx: number,
): Uint8Array[] {
  const field = fieldAt(descriptor, fieldIdx);
  if (!expandsToKeyParts(field.valueType)) {
    const key: number[] = [];
    encodeRecordFieldKeyPart(key, descriptor, record, fieldIdx);
    return [Uint8Array.from(key)];
  }
  const value = namedFieldValue(descriptor, record, fieldIdx);
  if (field.valueType.kind === "Array" && value.kind !== "Array")
    throw IvmRuntimeError.GraphFieldNotFound(field.name ?? "<unnamed>");
  return keyParts(value).map((part) => {
    const key: number[] = [];
    encodeKeyPart(key, part);
    return Uint8Array.from(key);
  });
}

/** Whether a field of this type is indexed once per array element. */
export function expandsToKeyParts(t: ValueType): boolean {
  return t.kind === "Array" || (t.kind === "Nullable" && t.inner.kind === "Array");
}

/** The values a record is keyed under for one field. */
export function keyParts(value: Value): Value[] {
  if (value.kind === "Array") return value.values;
  if (value.kind === "Nullable" && value.value !== undefined) {
    const inner = value.value;
    if (inner.kind === "Array")
      return inner.values.map((v: Value) => ({ kind: "Nullable", value: v }));
    return [{ kind: "Nullable", value: inner }];
  }
  return [value];
}

/** Read field `fieldIdx` through its name, so aliases resolve as predicates do. */
export function namedFieldValue(
  descriptor: RecordDescriptor,
  record: Uint8Array,
  fieldIdx: number,
): Value {
  const field = fieldAt(descriptor, fieldIdx);
  if (field.name === undefined) throw IvmRuntimeError.GraphFieldNotFound("<unnamed>");
  const idx = resolveFieldName(descriptor, field.name);
  if (idx === undefined) throw IvmRuntimeError.GraphFieldNotFound(field.name);
  return descriptor.bind(record).getIdx(idx);
}

/** Every composite key of a record: prefix plus one key part per field, deduplicated. */
export function compositeKeys(
  prefix: number[],
  fieldValues: Iterable<Value>,
  encode: (key: number[], value: Value) => void,
): number[][] {
  let keys = [prefix];
  for (const value of fieldValues) {
    const parts = keyParts(value);
    if (keys.length === 1 && parts.length === 1) {
      // One key and one part cannot produce a duplicate.
      encode(keys[0], parts[0]);
      continue;
    }
    if (!parts.length) return [];
    const seen = new Set<string>();
    const next: number[][] = [];
    for (const key of keys)
      for (const part of parts) {
        const k = key.slice();
        encode(k, part);
        const s = k.join(",");
        if (!seen.has(s)) {
          seen.add(s);
          next.push(k);
        }
      }
    keys = next;
  }
  return keys;
}

export function compareRecordField(
  record: PredicateRecord,
  field: string,
  value: LiteralValue,
  predicate: (ord: number) => boolean,
  comparison: ValueComparison,
): boolean {
  const ord = record.literalOrdering(field, value);
  if (typeof ord === "number") return predicate(ord);
  if (ord === "SqlNull") return false;
  const result = compareValuesSql(record.value(field), literalToValue(value), comparison);
  return result !== undefined && predicate(result);
}

const cmp = <T>(a: T, b: T): FieldLiteralOrdering =>
  a < b ? -1 : a > b ? 1 : a === b ? 0 : "SqlNull";
const cmpB = (a: Uint8Array, b: Uint8Array): FieldLiteralOrdering => Math.sign(compareBytes(a, b));

export function recordFieldLiteralOrdering(
  record: BorrowedRecord,
  i: number,
  value: LiteralValue,
): FieldLiteralOrdering {
  const t = record.field(i).valueType;
  if (t.kind === "Record" && value.kind === "NativeRecord") {
    let expected: RecordDescriptor;
    try {
      expected = decodeRecordDescriptor(value.descriptor);
    } catch {
      throw IvmRuntimeError.UnsupportedOperator();
    }
    if (t.descriptor !== expected) return "Unsupported";
    const actual = record.getIdx(i);
    if (actual.kind !== "Record") return "Unsupported";
    return cmpB(actual.record.raw(), value.raw);
  }
  if (t.kind === "Nullable" && value.kind === "Nullable")
    return value.value === undefined
      ? "SqlNull"
      : nullableRecordFieldLiteralOrdering(record, i, t.inner, value.value);
  const tk = t.kind === "EnumTag" ? "EnumTag" : t.kind;
  if (tk !== value.kind) return "Unsupported";
  switch (value.kind) {
    case "U8":
      return cmp(record.getU8(i), value.value);
    case "U32":
      return cmp(record.getU32(i), value.value);
    case "U64":
      return cmp(record.getU64(i), value.value);
    case "U48":
      return cmp(record.getU48(i), value.value);
    case "I32":
      return cmp(record.getI32(i), value.value);
    case "I64":
      return cmp(record.getI64(i), value.value);
    case "F64":
      return cmp(record.getF64(i), value.value);
    case "Bool":
      return cmp(record.getBool(i), value.value);
    case "String":
      return cmp(record.getStr(i), value.value);
    case "Bytes":
      return cmpB(record.getBytes(i), value.value);
    case "Uuid":
      return cmpB(record.getUuid(i), value.value);
    case "EnumTag":
      return cmp(record.getEnum(i), value.value);
    default:
      return "Unsupported";
  }
}

function nullableRecordFieldLiteralOrdering(
  r: BorrowedRecord,
  i: number,
  inner: ValueType,
  e: LiteralValue,
): FieldLiteralOrdering {
  if (inner.kind !== e.kind) return "Unsupported";
  const nullable = <T>(
    actual: T | undefined,
    expected: T,
    c: (a: T, b: T) => FieldLiteralOrdering = cmp,
  ) => (actual === undefined ? "SqlNull" : c(actual, expected));
  switch (e.kind) {
    case "U64":
      return nullable(r.getNullableU64(i), e.value);
    case "U48":
      return nullable(r.getNullableU48(i), e.value);
    case "I64":
      return nullable(r.getNullableI64(i), e.value);
    case "I32":
      return nullable(r.getNullableI32(i), e.value);
    case "F64":
      return nullable(r.getNullableF64(i), e.value);
    case "String":
      return nullable(r.getNullableString(i), e.value);
    case "Bytes":
      return nullable(r.getNullableBytes(i), e.value, cmpB);
    case "Uuid":
      return nullable(r.getNullableUuid(i), e.value, cmpB);
    case "EnumTag":
      return nullable(r.getNullableEnum(i), e.value);
    default:
      return "Unsupported";
  }
}

export function compareRecordFields(
  record: PredicateRecord,
  field: string,
  valueField: string,
  predicate: (ord: number) => boolean,
  comparison: ValueComparison,
): boolean {
  const ord = compareValuesSql(record.value(field), record.value(valueField), comparison);
  return ord !== undefined && predicate(ord);
}

export function containsRecordField(
  record: PredicateRecord,
  field: string,
  value: LiteralValue,
  comparison: ValueComparison,
): boolean {
  return valueContainsSql(record.value(field), literalToValue(value), comparison);
}

export function containsRecordFieldValue(
  record: PredicateRecord,
  field: string,
  needleField: string,
  comparison: ValueComparison,
): boolean {
  return valueContainsSql(record.value(field), record.value(needleField), comparison);
}

function valueContainsSql(left: Value, right: Value, comparison: ValueComparison): boolean {
  if (left.kind === "Nullable")
    return left.value !== undefined && valueContainsSql(left.value, right, comparison);
  if (right.kind === "Nullable")
    return right.value !== undefined && valueContainsSql(left, right.value, comparison);
  if (left.kind === "String" && right.kind === "String") return left.value.includes(right.value);
  if (left.kind === "Array")
    return left.values.some((v: Value) => compareValuesSql(v, right, comparison) === 0);
  return false;
}

export function compareValuesSql(
  left: Value,
  right: Value,
  comparison: ValueComparison,
): number | undefined {
  if (left.kind === "Nullable")
    return left.value === undefined ? undefined : compareValuesSql(left.value, right, comparison);
  if (right.kind === "Nullable")
    return right.value === undefined ? undefined : compareValuesSql(left, right.value, comparison);
  return compareValues(left, right, comparison);
}

export function isSqlNullValue(value: Value): boolean {
  return value.kind === "Nullable" && (value.value === undefined || isSqlNullValue(value.value));
}

function compareValues(left: Value, right: Value, comparison: ValueComparison): number | undefined {
  if (comparison === "Policy") {
    const l = integerValue(left);
    const r = integerValue(right);
    // Floats deliberately do not participate.
    if (l !== undefined && r !== undefined) return l < r ? -1 : l > r ? 1 : 0;
  }
  if (left.kind !== right.kind) return undefined;
  switch (left.kind) {
    case "U8":
    case "U16":
    case "U32":
    case "U64":
    case "U48":
    case "I32":
    case "I64":
    case "F64":
    case "Bool":
    case "EnumTag":
    case "String": {
      const o = cmp(left.value, right.value);
      return typeof o === "number" ? o : undefined;
    }
    case "Bytes":
    case "Uuid":
      return Math.sign(compareBytes(left.value, right.value));
    case "Tuple":
    case "Array": {
      const n = Math.min(left.values.length, right.values.length);
      for (let i = 0; i < n; i++) {
        const o = compareValues(left.values[i], right.values[i], comparison);
        if (o !== 0) return o;
      }
      return Math.sign(left.values.length - right.values.length);
    }
    default:
      return undefined;
  }
}

export function integerValue(value: Value): bigint | undefined {
  switch (value.kind) {
    case "U8":
    case "U16":
    case "U32":
    case "U64":
    case "U48":
    case "I32":
    case "I64":
      return BigInt(value.value);
    default:
      return undefined;
  }
}

export function joinDescriptor(left: RecordDescriptor, right: RecordDescriptor): RecordDescriptor {
  const fields: [string, ValueType][] = [];
  for (const f of left.fields())
    if (f.name !== undefined) fields.push([`left.${f.name}`, f.valueType]);
  for (const f of right.fields())
    if (f.name !== undefined) fields.push([`right.${f.name}`, f.valueType]);
  return RecordDescriptor.new(fields);
}

export function encodeKeyPart(key: number[], value: Value): void {
  // Type tags make composite keys unambiguous; payloads preserve natural order.
  switch (value.kind) {
    case "F64":
      if (Number.isNaN(value.value))
        throw IvmRuntimeError.RecordEncoding(RecordsError.InvalidF64NaN());
      return encodeScalarKeyPart(key, value);
    case "Tuple":
      key.push(11);
      for (const v of value.values) encodeKeyPart(key, v);
      return;
    case "Nullable":
      if (value.value === undefined) key.push(8);
      else {
        key.push(9);
        encodeKeyPart(key, value.value);
      }
      return;
    default:
      return encodeScalarKeyPart(key, value);
  }
}

/** Type-tagged, order-preserving bytes of one scalar key value. */
export function encodeScalarKeyPart(key: number[], value: Value): void {
  switch (value.kind) {
    case "U8":
    case "EnumTag":
      return putU8(key, value.value);
    case "U16":
      key.push(1, value.value >> 8, value.value & 0xff);
      return;
    case "U32":
      return putU32(key, value.value);
    case "U64":
      return putU64(key, value.value);
    case "U48":
      return putU48(key, value.value);
    case "I32":
      return putI32(key, value.value);
    case "I64":
      return putI64(key, value.value);
    case "F64":
      return putF64(key, value.value);
    case "Bool":
      return putBool(key, value.value);
    case "String":
      return putStr(key, value.value);
    case "Bytes":
      return putBytes(key, value.value);
    case "Uuid":
      return putUuid(key, value.value);
    default:
      throw IvmRuntimeError.UnsupportedJoinKey();
  }
}

const be = (bits: bigint, bytes: number): number[] => {
  const out = new Array<number>(bytes);
  for (let i = bytes - 1; i >= 0; i--) {
    out[i] = Number(bits & 0xffn);
    bits >>= 8n;
  }
  return out;
};

function putU8(key: number[], v: number) {
  key.push(0, v);
}
function putU32(key: number[], v: number) {
  key.push(2, ...be(BigInt(v), 4));
}
function putU64(key: number[], v: number) {
  key.push(3, ...be(BigInt(v), 8));
}
function putU48(key: number[], v: number) {
  key.push(16, ...u48BeBytes(v));
}
function putI32(key: number[], v: number) {
  key.push(14, ...be(BigInt(orderPreservingI32Bits(v)), 4));
}
function putI64(key: number[], v: number) {
  key.push(13, ...be(orderPreservingI64Bits(v), 8));
}
/** Callers that may see NaN reject it first with nonNan. */
function putF64(key: number[], v: number) {
  key.push(4, ...be(orderPreservingF64Bits(v), 8));
}
function putBool(key: number[], v: boolean) {
  key.push(5, v ? 1 : 0);
}
function putStr(key: number[], v: string) {
  key.push(6);
  encodeOrderedBytes(key, utf8(v));
}
function putBytes(key: number[], v: Uint8Array) {
  key.push(7);
  encodeOrderedBytes(key, v);
}
function putUuid(key: number[], v: Uint8Array) {
  key.push(10, ...v);
}
/** Nullable framing: 8 for null, else 9 followed by the value. */
function putNullable<T>(key: number[], value: T | undefined, put: (key: number[], v: T) => void) {
  if (value === undefined) key.push(8);
  else {
    key.push(9);
    put(key, value);
  }
}

function nonNan(v: number): number {
  if (Number.isNaN(v)) throw IvmRuntimeError.RecordEncoding(RecordsError.InvalidF64NaN());
  return v;
}

const SIGN64 = 1n << 63n;
const MASK64 = (1n << 64n) - 1n;

function orderPreservingF64Bits(v: number): bigint {
  const view = new DataView(new ArrayBuffer(8));
  view.setFloat64(0, v);
  const bits = view.getBigUint64(0);
  // Flip positive signs and invert negatives.
  return (bits & SIGN64) === 0n ? bits ^ SIGN64 : ~bits & MASK64;
}

export function orderPreservingI64Bits(v: number): bigint {
  return (BigInt.asUintN(64, BigInt(v)) ^ SIGN64) & MASK64;
}

export function orderPreservingI32Bits(v: number): number {
  return ((v >>> 0) ^ 0x80000000) >>> 0;
}

/** 0x00 terminates the byte string; embedded NULs are escaped as 00 ff. */
export function encodeOrderedBytes(key: number[], value: Uint8Array): void {
  for (const byte of value) {
    if (byte === 0) key.push(0, 0xff);
    else key.push(byte);
  }
  key.push(0, 0);
}

export function projectBindingSourceDeltas(
  input: RecordDeltas,
  outputDesc: RecordDescriptor,
): RecordDeltas {
  if (input.descriptor === outputDesc) return input;
  const mapping = outputDesc.fields().map((field: any): [number, number] => {
    if (field.name === undefined) throw IvmRuntimeError.GraphFieldNotFound("<unnamed>");
    return [0, resolveFieldRef(input.descriptor, FieldRef.storedName(field.name))];
  });
  const deltas = input.deltas.map((delta: RecordDelta) => ({
    record: outputDesc.projectRecordRaw([input.descriptor], [delta.raw()], mapping),
    weight: delta.weight,
  }));
  return { descriptor: outputDesc, deltas };
}

export function consolidateDeltas(deltas: RecordDelta[]): RecordDelta[] {
  if (deltas.length <= 1) return deltas;
  const sorted = deltas.slice().sort((a, b) => compareBytes(a.record, b.record));
  const out: RecordDelta[] = [];
  for (const delta of sorted) {
    const last = out[out.length - 1];
    if (last && equalBytes(last.record, delta.record)) {
      last.weight += delta.weight;
      continue;
    }
    out.push({ ...delta });
  }
  return out.filter((d) => d.weight !== 0);
}

export { toHex };
