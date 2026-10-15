// Typed record-field codecs and the typed-row wrapper factory (Rust: RecordField
// trait impls + `define_record!` / `impl_record_field_*!` macros).
import type { Bytes } from "../../support/bytes.js";
import { fromUtf8 } from "../../support/bytes.js";
import {
  type BorrowedRecord,
  type EnumValue,
  OwnedRecord,
  type RecordDescriptor,
  RecordError,
  type Value,
  type ValueType,
  U48_MAX,
  decodeValue,
  fixedSize,
  formatValueType,
  u48FromBeBytes,
  u48FromLeBytes,
  valueTypeEq,
} from "./index.js";
import { inlineScalarBytes } from "../large_values.js";

export type FieldKind =
  | "U64"
  | "U48"
  | "I64"
  | "U32"
  | "I32"
  | "F64"
  | "U8"
  | "Bool"
  | "Bytes"
  | "Uuid"
  | "String"
  | "Enum"
  | "PayloadEnum"
  | "Tuple"
  | "Array"
  | "Nullable"
  | "Record";

function kindMatches(kind: FieldKind, vt: ValueType): boolean {
  if (typeof vt === "string") return kind === vt;
  const map: Record<string, FieldKind> = {
    EnumTag: "Enum",
    Enum: "PayloadEnum",
    Tuple: "Tuple",
    Array: "Array",
    Nullable: "Nullable",
    Record: "Record",
  };
  return map[vt.kind] === kind;
}

export interface RecordField<T> {
  readonly kind: FieldKind;
  read(record: BorrowedRecord, idx: number): T;
  toValue(v: T): Value;
  readRaw(bytes: Bytes, vt: ValueType): T;
  readTupleRaw(bytes: Bytes, vt: ValueType): T;
}

function readExact(bytes: Bytes, n: number): Bytes {
  if (bytes.length !== n) throw RecordError.eof();
  return bytes;
}
const dv = (b: Bytes) => new DataView(b.buffer, b.byteOffset, b.byteLength);
function expect(vt: ValueType, expected: ValueType) {
  if (!valueTypeEq(vt, expected)) throw RecordError.typeMismatch(expected);
}
const notTupleMember = (_: Bytes, vt: ValueType): never => {
  throw RecordError.invalidTupleMember(vt);
};

function scalarField<T>(
  kind: FieldKind & ValueType,
  read: (r: BorrowedRecord, idx: number) => T,
  toValue: (v: T) => Value,
  le: (b: Bytes) => T,
  be: ((b: Bytes) => T) | null,
): RecordField<T> {
  const raw = (decode: (b: Bytes) => T) => (bytes: Bytes, vt: ValueType) => {
    expect(vt, kind);
    return decode(bytes);
  };
  return {
    kind,
    read,
    toValue,
    readRaw: raw(le),
    readTupleRaw: be ? raw(be) : (b, vt) => notTupleMember(b, vt),
  };
}

export const u64Field = scalarField<bigint>(
  "U64",
  (r, i) => r.getU64(i),
  (v) => ({ kind: "U64", v }),
  (b) => dv(readExact(b, 8)).getBigUint64(0, true),
  (b) => dv(readExact(b, 8)).getBigUint64(0, false),
);
export const u48Field = scalarField<number>(
  "U48",
  (r, i) => r.getU48(i),
  (v) => ({ kind: "U48", v }),
  (b) => u48FromLeBytes(readExact(b, 6)),
  (b) => u48FromBeBytes(readExact(b, 6)),
);
export const i64Field = scalarField<bigint>(
  "I64",
  (r, i) => r.getI64(i),
  (v) => ({ kind: "I64", v }),
  (b) => dv(readExact(b, 8)).getBigInt64(0, true),
  (b) => BigInt.asIntN(64, dv(readExact(b, 8)).getBigUint64(0, false) ^ (1n << 63n)),
);
export const i32Field = scalarField<number>(
  "I32",
  (r, i) => r.getI32(i),
  (v) => ({ kind: "I32", v }),
  (b) => dv(readExact(b, 4)).getInt32(0, true),
  (b) => (dv(readExact(b, 4)).getUint32(0, false) ^ 0x80000000) | 0,
);
export const u32Field = scalarField<number>(
  "U32",
  (r, i) => r.getU32(i),
  (v) => ({ kind: "U32", v }),
  (b) => dv(readExact(b, 4)).getUint32(0, true),
  (b) => dv(readExact(b, 4)).getUint32(0, false),
);
export const f64Field = scalarField<number>(
  "F64",
  (r, i) => r.getF64(i),
  (v) => ({ kind: "F64", v }),
  (b) => {
    const v = dv(readExact(b, 8)).getFloat64(0, true);
    if (Number.isNaN(v)) throw RecordError.nan();
    return v;
  },
  null,
);
const u8Raw = (b: Bytes) => readExact(b, 1)[0];
export const u8Field = scalarField<number>(
  "U8",
  (r, i) => r.getU8(i),
  (v) => ({ kind: "U8", v }),
  u8Raw,
  u8Raw,
);
const boolRaw = (b: Bytes) => {
  const v = readExact(b, 1)[0];
  if (v > 1) throw RecordError.invalidBool(v);
  return v === 1;
};
export const boolField = scalarField<boolean>(
  "Bool",
  (r, i) => r.getBool(i),
  (v) => ({ kind: "Bool", v }),
  boolRaw,
  boolRaw,
);
export const bytesField = scalarField<Bytes>(
  "Bytes",
  (r, i) => r.getBytes(i).slice(),
  (v) => ({ kind: "Bytes", v }),
  (b) => inlineScalarBytes("Bytes", b).slice(),
  null,
);
const uuidRaw = (b: Bytes) => readExact(b, 16).slice();
export const uuidField = scalarField<Bytes>(
  "Uuid",
  (r, i) => r.getUuid(i),
  (v) => ({ kind: "Uuid", v }),
  uuidRaw,
  uuidRaw,
);
export const stringField = scalarField<string>(
  "String",
  (r, i) => r.getStr(i),
  (v) => ({ kind: "String", v }),
  (b) => {
    try {
      return fromUtf8(inlineScalarBytes("String", b));
    } catch (e) {
      if (e instanceof TypeError) throw RecordError.utf8();
      throw e;
    }
  },
  null,
);

export const valueField: RecordField<Value> = {
  kind: "Nullable",
  read: (r, i) => decodeValue(r.fieldBytesUnchecked(i), r.field(i).valueType),
  toValue: (v) => v,
  readRaw: decodeValue,
  readTupleRaw: decodeValue,
};

function valueVariantField<T>(
  variant: "Array" | "Record" | "Enum",
  kind: FieldKind,
): RecordField<T> {
  const readRaw = (bytes: Bytes, vt: ValueType): T => {
    const v = decodeValue(bytes, vt);
    if (v.kind !== variant) throw RecordError.typeMismatch(vt);
    return v.v as T;
  };
  return {
    kind,
    read: (r, i) => readRaw(r.fieldBytesUnchecked(i), r.field(i).valueType),
    toValue: (v) => ({ kind: variant, v }) as Value,
    readRaw,
    readTupleRaw: readRaw,
  };
}
export const arrayField = valueVariantField<Value[]>("Array", "Array");
export const recordField = valueVariantField<OwnedRecord>("Record", "Record");
export const enumValueField = valueVariantField<EnumValue>("Enum", "PayloadEnum");

function readNullableRaw<T>(
  bytes: Bytes,
  inner: ValueType,
  present: (b: Bytes, vt: ValueType) => T,
): T | null {
  if (!bytes.length) throw RecordError.eof();
  const [flag, payload] = [bytes[0], bytes.subarray(1)];
  if (flag === 0) {
    if (fixedSize(inner) !== undefined) {
      if (payload.some((b) => b !== 0)) throw RecordError.invalidOffset();
    } else if (payload.length) throw RecordError.invalidOffset();
    return null;
  }
  if (flag === 1) return present(payload, inner);
  throw RecordError.invalidNullFlag(flag);
}

export function optionField<T>(inner: RecordField<T>): RecordField<T | null> {
  const unwrap = (vt: ValueType): ValueType => {
    if (typeof vt === "string" || vt.kind !== "Nullable")
      throw RecordError.typeMismatch({ kind: "Nullable", inner: "Bytes" });
    return vt.inner;
  };
  return {
    kind: "Nullable",
    read: (r, i) =>
      readNullableRaw(r.fieldBytesUnchecked(i), unwrap(r.field(i).valueType), inner.readRaw),
    toValue: (v) => ({ kind: "Nullable", v: v === null ? null : inner.toValue(v) }),
    readRaw: (b, vt) => readNullableRaw(b, unwrap(vt), inner.readRaw),
    readTupleRaw: (b, vt) => readNullableRaw(b, unwrap(vt), inner.readTupleRaw),
  };
}

export function pairField<A, B>(a: RecordField<A>, b: RecordField<B>): RecordField<[A, B]> {
  const readRaw = (bytes: Bytes, vt: ValueType): [A, B] => {
    if (typeof vt === "string" || vt.kind !== "Tuple")
      throw RecordError.typeMismatch({ kind: "Tuple", members: ["Bytes", "Bytes"] });
    const m = vt.members;
    if (m.length !== 2) throw RecordError.arity(2, m.length);
    const lw = fixedSize(m[0]);
    if (lw === undefined) throw RecordError.invalidTupleMember(m[0]);
    const rw = fixedSize(m[1]);
    if (rw === undefined) throw RecordError.invalidTupleMember(m[1]);
    if (bytes.length !== lw + rw) throw RecordError.eof();
    return [a.readTupleRaw(bytes.subarray(0, lw), m[0]), b.readTupleRaw(bytes.subarray(lw), m[1])];
  };
  return {
    kind: "Tuple",
    read: (r, i) => readRaw(r.fieldBytesUnchecked(i), r.field(i).valueType),
    toValue: ([x, y]) => ({ kind: "Tuple", v: [a.toValue(x), b.toValue(y)] }),
    readRaw,
    readTupleRaw: readRaw,
  };
}

/** `impl_record_field_{u64,u48,uuid,bytes16}!`: a newtype codec mapped over a base codec. */
export function newtypeField<T, U>(
  base: RecordField<T>,
  wrap: (t: T) => U,
  unwrap: (u: U) => T,
): RecordField<U> {
  return {
    kind: base.kind,
    read: (r, i) => wrap(base.read(r, i)),
    toValue: (u) => base.toValue(unwrap(u)),
    readRaw: (b, vt) => wrap(base.readRaw(b, vt)),
    readTupleRaw: (b, vt) => wrap(base.readTupleRaw(b, vt)),
  };
}
export const bytes16Field = newtypeField(
  bytesField,
  (b) => {
    if (b.length !== 16) throw RecordError.invalidOffset();
    return b;
  },
  (b: Bytes) => b,
);

/** `impl_record_field_enum!`: a discriminant-tagged scalar enum codec. */
export function enumTagField<E>(enumName: string, variants: readonly E[]): RecordField<E> {
  const fromDiscriminant = (d: number): E => {
    if (d >= variants.length)
      throw new RecordError(
        "InvalidEnumDiscriminant",
        `invalid enum discriminant ${d} for enum ${enumName}`,
      );
    return variants[d];
  };
  const readRaw = (bytes: Bytes, vt: ValueType): E => {
    if (typeof vt === "string" || vt.kind !== "EnumTag") throw RecordError.typeMismatch("U8");
    const d = u8Field.readRaw(bytes, "U8");
    vt.schema.variant(d);
    return fromDiscriminant(d);
  };
  return {
    kind: "Enum",
    read: (r, i) => fromDiscriminant(r.getEnum(i)),
    toValue: (e) => ({ kind: "EnumTag", v: variants.indexOf(e) }),
    readRaw,
    readTupleRaw: readRaw,
  };
}

export function assertRecordFieldLayout(
  descriptor: RecordDescriptor,
  idx: number,
  name: string,
  kind: FieldKind,
): void {
  const field = descriptor.fields[idx];
  if (!field) throw new Error(`record field index ${idx} missing for ${name}`);
  if (field.name !== name) throw new Error(`record field index drifted for ${name}`);
  if (!kindMatches(kind, field.valueType))
    throw new Error(
      `record field ${name} has kind ${formatValueType(field.valueType)}, expected ${kind}`,
    );
}

export class U48 {
  static readonly MAX = U48_MAX;
  private constructor(readonly value: number) {}
  static new(value: number): U48 {
    if (value > U48.MAX)
      throw new RecordError("U48OutOfRange", `u48 value ${value} exceeds 2^48 - 1`);
    return new U48(value);
  }
}

type FieldSpecs = Record<string, RecordField<any>>;
type FieldValues<S extends FieldSpecs> = {
  [K in keyof S]: S[K] extends RecordField<infer T> ? T : never;
};

/** `define_record!`: a typed wrapper whose fields are declared in order (index = position). */
export function defineRecord<S extends FieldSpecs>(specs: S, opts: { tail?: boolean } = {}) {
  const names = Object.keys(specs);
  const userBase = names.length;
  return class TypedRecord {
    static readonly FIELD_NAMES = names;
    static readonly USER_BASE = userBase;
    constructor(readonly record: OwnedRecord) {}
    get<K extends keyof S & string>(field: K): FieldValues<S>[K] {
      return specs[field].read(this.record.borrowed(), names.indexOf(field));
    }
    cell(i: number): Value | null {
      return optionField(valueField).read(this.record.borrowed(), userBase + i);
    }
    cells(): (Value | null)[] {
      const n = Math.max(this.record.descriptor.fields.length - userBase, 0);
      return Array.from({ length: n }, (_, i) => this.cell(i));
    }
    static encode(
      descriptor: RecordDescriptor,
      fields: FieldValues<S>,
      tail: (Value | null)[] = [],
    ) {
      const values = names.map((n) => specs[n].toValue(fields[n]));
      if (opts.tail) for (const v of tail) values.push({ kind: "Nullable", v });
      return new TypedRecord(new OwnedRecord(descriptor.create(values), descriptor));
    }
    static assertLayout(descriptor: RecordDescriptor): void {
      names.forEach((n, i) => assertRecordFieldLayout(descriptor, i, n, specs[n].kind));
      if (opts.tail ? descriptor.fields.length < userBase : descriptor.fields.length !== userBase)
        throw new Error("record descriptor arity drifted");
    }
  };
}
