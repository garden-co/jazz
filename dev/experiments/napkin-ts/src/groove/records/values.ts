// Logical record values, value types, and primitive encoders.
import { type Bytes, equalBytes, utf8, fromUtf8 } from "../../support/bytes.js";
import {
  type DescriptorField,
  type FieldIdentity,
  OwnedRecord,
  RecordDescriptor,
  RecordError,
  appendVariantRecord,
  splitVariantRecord,
  type BorrowedRecord,
} from "./index.js";
import {
  type LargeValueKind,
  type LargeValueRef,
  LargeValueError,
  decodeStoredScalar,
  encodePrimitiveStoredScalarInto,
  encodeStoredScalar,
  inlineScalarBytes,
} from "../large_values.js";

const SYSTEM_REGISTRY_MARKER = 1n << 63n;
const U64_MASK = (1n << 64n) - 1n;

export function variantRegistryIdForPath(path: string): bigint {
  let hash = 0xcbf29ce484222325n;
  for (const byte of utf8(path)) {
    hash ^= BigInt(byte);
    hash = (hash * 0x100000001b3n) & U64_MASK;
  }
  hash &= ~SYSTEM_REGISTRY_MARKER & U64_MASK;
  return hash === 0n ? 1n : hash;
}

// ---- Values and types ----

export type Value =
  | { kind: "U8"; v: number }
  | { kind: "U16"; v: number }
  | { kind: "U32"; v: number }
  | { kind: "U64"; v: bigint }
  | { kind: "F64"; v: number }
  | { kind: "Bool"; v: boolean }
  | { kind: "String"; v: string }
  | { kind: "Bytes"; v: Bytes }
  | { kind: "Large"; v: LargeValueRef }
  | { kind: "Uuid"; v: Bytes }
  | { kind: "EnumTag"; v: number }
  | { kind: "Tuple"; v: Value[] }
  | { kind: "Array"; v: Value[] }
  | { kind: "Nullable"; v: Value | null }
  | { kind: "I64"; v: bigint }
  | { kind: "I32"; v: number }
  | { kind: "Record"; v: OwnedRecord }
  | { kind: "Enum"; v: EnumValue }
  | { kind: "U48"; v: number };

export const V = {
  u8: (v: number): Value => ({ kind: "U8", v }),
  u32: (v: number): Value => ({ kind: "U32", v }),
  u64: (v: bigint): Value => ({ kind: "U64", v }),
  str: (v: string): Value => ({ kind: "String", v }),
  bytes: (v: Bytes): Value => ({ kind: "Bytes", v }),
  arr: (v: Value[]): Value => ({ kind: "Array", v }),
  nullable: (v: Value | null): Value => ({ kind: "Nullable", v }),
};

export class EnumValue {
  constructor(
    readonly tag: number,
    readonly record: OwnedRecord,
  ) {}
  static create(tag: number, descriptor: RecordDescriptor, values: Value[]): EnumValue {
    return new EnumValue(tag, new OwnedRecord(descriptor.create(values), descriptor));
  }
}

export type ScalarType =
  | "U8"
  | "U16"
  | "U32"
  | "U64"
  | "I32"
  | "I64"
  | "F64"
  | "Bool"
  | "String"
  | "Bytes"
  | "Uuid"
  | "U48";

export type ValueType =
  | ScalarType
  | { kind: "RawString" }
  | { kind: "RawBytes" }
  | { kind: "Stored"; stored: LargeValueKind }
  | { kind: "EnumTag"; schema: ScalarEnumSchema }
  | { kind: "Tuple"; members: ValueType[] }
  | { kind: "Array"; inner: ValueType }
  | { kind: "Nullable"; inner: ValueType }
  | { kind: "Record"; descriptor: RecordDescriptor }
  | { kind: "Enum"; schema: EnumSchema };

export const rawString = (): ValueType => ({ kind: "RawString" });
export const rawBytes = (): ValueType => ({ kind: "RawBytes" });
export const storedScalar = (stored: LargeValueKind): ValueType => ({ kind: "Stored", stored });
export const nullable = (inner: ValueType): ValueType => ({ kind: "Nullable", inner });
export const arrayOf = (inner: ValueType): ValueType => ({ kind: "Array", inner });

export class ScalarEnumSchema {
  private constructor(
    public registryId: bigint,
    public name: string,
    public variants: string[],
  ) {}
  static new(name: string, variants: Iterable<string>): ScalarEnumSchema {
    return ScalarEnumSchema.fromParts(0n, name, [...variants]);
  }
  static fromParts(registryId: bigint, name: string, variants: string[]): ScalarEnumSchema {
    ScalarEnumSchema.validateVariants(name, variants);
    return new ScalarEnumSchema(registryId, name, variants);
  }
  private static validateVariants(name: string, variants: string[]): void {
    if (variants.length > 256) throw RecordError.enumTooManyVariants(name, variants.length);
    variants.forEach((variant, index) => {
      if (variants.slice(0, index).includes(variant))
        throw new RecordError(
          "DuplicateEnumVariantName",
          `duplicate variant ${variant} in enum ${name}`,
        );
    });
  }
  validate(): void {
    ScalarEnumSchema.validateVariants(this.name, this.variants);
  }
  withRegistryId(registryId: bigint): ScalarEnumSchema {
    return new ScalarEnumSchema(registryId & ~SYSTEM_REGISTRY_MARKER, this.name, this.variants);
  }
  withSystemRegistry(registry: SystemVariantRegistry): ScalarEnumSchema {
    return new ScalarEnumSchema(registry.id, this.name, this.variants);
  }
  discriminant(variant: string): number {
    const idx = this.variants.indexOf(variant);
    if (idx < 0 || idx > 255)
      throw new RecordError(
        "UnknownEnumVariant",
        `unknown enum variant ${variant} for enum ${this.name}`,
      );
    return idx;
  }
  variant(discriminant: number): string {
    const v = this.variants[discriminant];
    if (v === undefined)
      throw new RecordError(
        "InvalidEnumDiscriminant",
        `invalid enum discriminant ${discriminant} for enum ${this.name}`,
      );
    return v;
  }
}

export class SystemVariantRegistry {
  private constructor(readonly id: bigint) {}
  static deletionState(): SystemVariantRegistry {
    return new SystemVariantRegistry(
      variantRegistryIdForPath("jazz/internal/deletion") | SYSTEM_REGISTRY_MARKER,
    );
  }
}

export type EnumCase = { name: string; payload: RecordDescriptor };
export const enumCase = (name: string, payload: RecordDescriptor): EnumCase => ({ name, payload });

export class EnumSchema {
  constructor(
    public registryId: bigint,
    public name: string,
    public cases: EnumCase[],
  ) {}
  static new(name: string, cases: Iterable<EnumCase>): EnumSchema {
    const list = [...cases];
    EnumSchema.validateCases(name, list);
    return new EnumSchema(0n, name, list);
  }
  withRegistryId(registryId: bigint): EnumSchema {
    return new EnumSchema(registryId, this.name, this.cases);
  }
  private static validateCases(name: string, cases: EnumCase[]): void {
    if (cases.length - 1 > 0xffffffff)
      throw new RecordError(
        "EnumTooManyCases",
        `enum ${name} has ${cases.length} cases; maximum is u32::MAX + 1`,
      );
    cases.forEach((c, index) => {
      if (cases.slice(0, index).some((other) => other.name === c.name))
        throw new RecordError("DuplicateEnumCaseName", `duplicate case ${c.name} in enum ${name}`);
    });
  }
  validate(): void {
    EnumSchema.validateCases(this.name, this.cases);
  }
  case(tag: number): EnumCase {
    const c = this.cases[tag];
    if (!c) throw new RecordError("UnknownEnumTag", `unknown tag ${tag} in enum ${this.name}`);
    return c;
  }
  tag(name: string): number {
    const idx = this.cases.findIndex((c) => c.name === name);
    if (idx < 0)
      throw new RecordError("UnknownEnumCase", `unknown case ${name} in enum ${this.name}`);
    return idx;
  }
}

export type VariantRegistry =
  | { kind: "EnumTag"; variants: string[] }
  | { kind: "Enum"; cases: string[] };

export function variantRegistryEq(a: VariantRegistry, b: VariantRegistry): boolean {
  const la = a.kind === "EnumTag" ? a.variants : a.cases;
  const lb = b.kind === "EnumTag" ? b.variants : b.cases;
  return a.kind === b.kind && la.length === lb.length && la.every((x, i) => x === lb[i]);
}

// ---- ValueType structural helpers ----

const strArrEq = (a: string[], b: string[]) =>
  a.length === b.length && a.every((x, i) => x === b[i]);
const startsWith = (a: string[], prefix: string[]) =>
  prefix.length <= a.length && prefix.every((x, i) => a[i] === x);

export function valueTypeEq(a: ValueType, b: ValueType): boolean {
  if (typeof a === "string" || typeof b === "string") return a === b;
  if (a.kind !== b.kind) return false;
  switch (a.kind) {
    case "RawString":
    case "RawBytes":
      return true;
    case "Stored":
      return a.stored === (b as typeof a).stored;
    case "EnumTag": {
      const s = (b as typeof a).schema;
      return (
        a.schema.registryId === s.registryId &&
        a.schema.name === s.name &&
        strArrEq(a.schema.variants, s.variants)
      );
    }
    case "Tuple": {
      const m = (b as typeof a).members;
      return a.members.length === m.length && a.members.every((x, i) => valueTypeEq(x, m[i]));
    }
    case "Array":
    case "Nullable":
      return valueTypeEq(a.inner, (b as typeof a).inner);
    case "Record":
      return a.descriptor === (b as typeof a).descriptor;
    case "Enum": {
      const s = (b as typeof a).schema;
      return (
        a.schema.registryId === s.registryId &&
        a.schema.name === s.name &&
        a.schema.cases.length === s.cases.length &&
        a.schema.cases.every(
          (c, i) => c.name === s.cases[i].name && c.payload === s.cases[i].payload,
        )
      );
    }
  }
}

/** Structural intern key (descriptors are interned, so nested records use their id). */
export function valueTypeKey(vt: ValueType): string {
  if (typeof vt === "string") return vt;
  switch (vt.kind) {
    case "RawString":
    case "RawBytes":
      return vt.kind;
    case "Stored":
      return `Stored(${vt.stored})`;
    case "EnumTag":
      return `EnumTag(${vt.schema.registryId},${JSON.stringify(vt.schema.name)},${JSON.stringify(vt.schema.variants)})`;
    case "Tuple":
      return `Tuple(${vt.members.map(valueTypeKey).join(",")})`;
    case "Array":
    case "Nullable":
      return `${vt.kind}(${valueTypeKey(vt.inner)})`;
    case "Record":
      return `Record(#${vt.descriptor.id})`;
    case "Enum":
      return `Enum(${vt.schema.registryId},${JSON.stringify(vt.schema.name)},${vt.schema.cases
        .map((c) => `${JSON.stringify(c.name)}:#${c.payload.id}`)
        .join(",")})`;
  }
}

/** Approximates the Rust `{:?}` rendering used in error messages. */
export function formatValueType(vt: ValueType): string {
  if (typeof vt === "string") return vt;
  switch (vt.kind) {
    case "RawString":
      return "Internal(InternalValueType(RawString))";
    case "RawBytes":
      return "Internal(InternalValueType(RawBytes))";
    case "Stored":
      return `Internal(InternalValueType(StoredScalar(${vt.stored})))`;
    case "EnumTag":
      return `EnumTag(ScalarEnumSchema { registry_id: ${vt.schema.registryId}, name: ${JSON.stringify(vt.schema.name)}, variants: ${JSON.stringify(vt.schema.variants)} })`;
    case "Tuple":
      return `Tuple([${vt.members.map(formatValueType).join(", ")}])`;
    case "Array":
    case "Nullable":
      return `${vt.kind}(${formatValueType(vt.inner)})`;
    case "Record":
      return `Record(RecordDescriptor(${vt.descriptor.fields
        .map((f) => `${f.name}: ${formatValueType(f.valueType)}`)
        .join(", ")}))`;
    case "Enum":
      return `Enum(EnumSchema { registry_id: ${vt.schema.registryId}, name: ${JSON.stringify(vt.schema.name)}, cases: [${vt.schema.cases.map((c) => c.name).join(", ")}] })`;
  }
}

export function nonNullable(vt: ValueType): ValueType {
  while (typeof vt !== "string" && vt.kind === "Nullable") vt = vt.inner;
  return vt;
}
export const isArrayFk = (vt: ValueType) =>
  typeof vt !== "string" && vt.kind === "Array" && vt.inner === "Uuid";
export const isInternalStorageType = (vt: ValueType) =>
  typeof vt !== "string" &&
  (vt.kind === "RawString" || vt.kind === "RawBytes" || vt.kind === "Stored");

export function mayContainStoredScalar(vt: ValueType): boolean {
  if (typeof vt === "string") return vt === "String" || vt === "Bytes";
  switch (vt.kind) {
    case "Stored":
      return true;
    case "Tuple":
      return vt.members.some(mayContainStoredScalar);
    case "Array":
    case "Nullable":
      return mayContainStoredScalar(vt.inner);
    case "Record":
      return vt.descriptor.fields.some((f) => mayContainStoredScalar(f.valueType));
    case "Enum":
      return vt.schema.cases.some((c) =>
        c.payload.fields.some((f) => mayContainStoredScalar(f.valueType)),
      );
    default:
      return false;
  }
}

export function variantRegistryOccurrenceCount(vt: ValueType): number {
  if (typeof vt === "string") return 0;
  switch (vt.kind) {
    case "EnumTag":
      return 1;
    case "Enum":
      return (
        1 +
        vt.schema.cases
          .flatMap((c) => c.payload.fields)
          .reduce((n, f) => n + variantRegistryOccurrenceCount(f.valueType), 0)
      );
    case "Tuple":
      return vt.members.reduce((n, m) => n + variantRegistryOccurrenceCount(m), 0);
    case "Array":
    case "Nullable":
      return variantRegistryOccurrenceCount(vt.inner);
    case "Record":
      return vt.descriptor.fields.reduce(
        (n, f) => n + variantRegistryOccurrenceCount(f.valueType),
        0,
      );
    default:
      return 0;
  }
}

type FieldsPred = (a: DescriptorField, b: DescriptorField) => boolean;
function fieldsAll(a: readonly DescriptorField[], b: readonly DescriptorField[], f: FieldsPred) {
  return a.length === b.length && a.every((x, i) => f(x, b[i]));
}

/** Shared walker for the three registry-compatibility relations. */
function registryRelation(
  a: ValueType,
  b: ValueType,
  rel: (a: ValueType, b: ValueType) => boolean,
  enumTag: (a: ScalarEnumSchema, b: ScalarEnumSchema) => boolean | undefined,
  enumCases: (a: EnumSchema, b: EnumSchema) => [number, boolean] | undefined,
): boolean {
  if (typeof a !== "string" && typeof b !== "string" && a.kind === b.kind) {
    switch (a.kind) {
      case "EnumTag": {
        const r = enumTag(a.schema, (b as typeof a).schema);
        if (r !== undefined) return r;
        break;
      }
      case "Enum": {
        const bs = (b as typeof a).schema;
        const r = enumCases(a.schema, bs);
        if (r === undefined) break;
        const [shared, ok] = r;
        return (
          ok &&
          a.schema.cases
            .slice(0, shared)
            .every(
              (c, i) =>
                c.name === bs.cases[i].name &&
                fieldsAll(
                  c.payload.fields,
                  bs.cases[i].payload.fields,
                  (x, y) => x.name === y.name && rel(x.valueType, y.valueType),
                ),
            )
        );
      }
      case "Tuple": {
        const m = (b as typeof a).members;
        return a.members.length === m.length && a.members.every((x, i) => rel(x, m[i]));
      }
      case "Array":
      case "Nullable":
        return rel(a.inner, (b as typeof a).inner);
      case "Record":
        return fieldsAll(
          a.descriptor.fields,
          (b as typeof a).descriptor.fields,
          (x, y) => x.name === y.name && rel(x.valueType, y.valueType),
        );
    }
  }
  return valueTypeEq(a, b);
}

export function registryCompatibleWith(a: ValueType, b: ValueType): boolean {
  return registryRelation(
    a,
    b,
    registryCompatibleWith,
    (l, r) =>
      l.registryId === r.registryId
        ? startsWith(l.variants, r.variants) || startsWith(r.variants, l.variants)
        : undefined,
    (l, r) =>
      l.registryId === r.registryId ? [Math.min(l.cases.length, r.cases.length), true] : undefined,
  );
}

export function registryReboundLayoutCompatibleWith(a: ValueType, b: ValueType): boolean {
  return registryRelation(
    a,
    b,
    registryReboundLayoutCompatibleWith,
    (l, r) => strArrEq(l.variants, r.variants),
    (l, r) => [l.cases.length, l.cases.length === r.cases.length],
  );
}

export function canEvolveRegistryTo(current: ValueType, next: ValueType): boolean {
  return registryRelation(
    current,
    next,
    canEvolveRegistryTo,
    (c, n) => (c.registryId === n.registryId ? startsWith(n.variants, c.variants) : undefined),
    (c, n) =>
      c.registryId === n.registryId
        ? [c.cases.length, n.cases.length >= c.cases.length]
        : undefined,
  );
}

export function collectVariantRegistries(
  vt: ValueType,
  output: Map<bigint, VariantRegistry>,
): void {
  if (typeof vt === "string") return;
  switch (vt.kind) {
    case "EnumTag":
      output.set(vt.schema.registryId, { kind: "EnumTag", variants: [...vt.schema.variants] });
      break;
    case "Tuple":
      vt.members.forEach((m) => collectVariantRegistries(m, output));
      break;
    case "Array":
    case "Nullable":
      collectVariantRegistries(vt.inner, output);
      break;
    case "Record":
      for (const f of vt.descriptor.fields) collectVariantRegistries(f.valueType, output);
      break;
    case "Enum":
      output.set(vt.schema.registryId, { kind: "Enum", cases: vt.schema.cases.map((c) => c.name) });
      for (const c of vt.schema.cases)
        for (const f of c.payload.fields) collectVariantRegistries(f.valueType, output);
      break;
  }
}

export const stampVariantRegistries = (vt: ValueType, path: string) =>
  assignVariantRegistries(vt, path, false);
export const rebindVariantRegistries = (vt: ValueType, path: string) =>
  assignVariantRegistries(vt, path, true);

function assignRecordVariantRegistries(
  d: RecordDescriptor,
  path: string,
  replace: boolean,
): RecordDescriptor {
  return RecordDescriptor.fromLogicalFields(
    d.fields.map((f, index) => ({
      name: f.name,
      identity: f.identity,
      valueType: assignVariantRegistries(f.valueType, `${path}/field/${index}`, replace),
    })),
  );
}

function assignVariantRegistries(vt: ValueType, path: string, replace: boolean): ValueType {
  if (typeof vt === "string") return vt;
  switch (vt.kind) {
    case "EnumTag": {
      const s = vt.schema;
      if ((s.registryId & SYSTEM_REGISTRY_MARKER) === 0n && (replace || s.registryId === 0n))
        return {
          kind: "EnumTag",
          schema: ScalarEnumSchema.fromParts(variantRegistryIdForPath(path), s.name, s.variants),
        };
      return vt;
    }
    case "Tuple":
      return {
        kind: "Tuple",
        members: vt.members.map((m, i) =>
          assignVariantRegistries(m, `${path}/tuple/${i}`, replace),
        ),
      };
    case "Array":
      return { kind: "Array", inner: assignVariantRegistries(vt.inner, `${path}/array`, replace) };
    case "Nullable":
      return {
        kind: "Nullable",
        inner: assignVariantRegistries(vt.inner, `${path}/nullable`, replace),
      };
    case "Record":
      return {
        kind: "Record",
        descriptor: assignRecordVariantRegistries(vt.descriptor, `${path}/record`, replace),
      };
    case "Enum": {
      const id =
        replace || vt.schema.registryId === 0n
          ? variantRegistryIdForPath(path)
          : vt.schema.registryId;
      return {
        kind: "Enum",
        schema: new EnumSchema(
          id,
          vt.schema.name,
          vt.schema.cases.map((c, i) => ({
            name: c.name,
            payload: assignRecordVariantRegistries(c.payload, `${path}/case/${i}`, replace),
          })),
        ),
      };
    }
    default:
      return vt;
  }
}

export function containsRecord(vt: ValueType): boolean {
  if (typeof vt === "string") return false;
  switch (vt.kind) {
    case "Tuple":
      return vt.members.some(containsRecord);
    case "Array":
    case "Nullable":
      return containsRecord(vt.inner);
    case "Record":
    case "Enum":
      return true;
    default:
      return false;
  }
}

export function fixedSize(vt: ValueType): number | undefined {
  if (typeof vt === "string") {
    switch (vt) {
      case "U8":
      case "Bool":
        return 1;
      case "U16":
        return 2;
      case "U32":
      case "I32":
        return 4;
      case "U48":
        return 6;
      case "U64":
      case "I64":
      case "F64":
        return 8;
      case "Uuid":
        return 16;
      default:
        return undefined;
    }
  }
  switch (vt.kind) {
    case "EnumTag":
      return 1;
    case "Tuple": {
      let total = 0;
      for (const m of vt.members) {
        const w = fixedSize(m);
        if (w === undefined) return undefined;
        total += w;
      }
      return total;
    }
    case "Nullable": {
      const w = fixedSize(vt.inner);
      return w === undefined ? undefined : w + 1;
    }
    default:
      return undefined;
  }
}
export const isFixedSize = (vt: ValueType) => fixedSize(vt) !== undefined;

export function collectByOrderedScalar(vt: ValueType): boolean {
  if (typeof vt === "string") return true;
  if (vt.kind === "Nullable") return collectByOrderedScalar(vt.inner);
  return vt.kind === "EnumTag";
}

// ---- growable output buffer (Rust Vec<u8>/BytesMut) ----

export class ByteBuf {
  buf = new Uint8Array(64);
  len = 0;
  private reserve(n: number) {
    if (this.len + n <= this.buf.length) return;
    let cap = this.buf.length * 2;
    while (cap < this.len + n) cap *= 2;
    const next = new Uint8Array(cap);
    next.set(this.buf.subarray(0, this.len));
    this.buf = next;
  }
  push(b: number): void {
    this.reserve(1);
    this.buf[this.len++] = b;
  }
  extend(bytes: Bytes): void {
    this.reserve(bytes.length);
    this.buf.set(bytes, this.len);
    this.len += bytes.length;
  }
  resize(n: number): void {
    if (n > this.len) {
      this.reserve(n - this.len);
      this.buf.fill(0, this.len, n);
    }
    this.len = n;
  }
  truncate(n: number): void {
    if (n < this.len) this.len = n;
  }
  setU32(offset: number, v: number): void {
    new DataView(this.buf.buffer).setUint32(offset, v, true);
  }
  view(start = 0, end = this.len): Bytes {
    return this.buf.subarray(start, end);
  }
  finish(): Bytes {
    return this.buf.slice(0, this.len);
  }
}

const dv = (b: Bytes) => new DataView(b.buffer, b.byteOffset, b.byteLength);
export function le(width: number, v: number | bigint, signed = false): Bytes {
  const out = new Uint8Array(width);
  const d = dv(out);
  if (width === 8) signed ? d.setBigInt64(0, BigInt(v), true) : d.setBigUint64(0, BigInt(v), true);
  else if (width === 4) signed ? d.setInt32(0, Number(v), true) : d.setUint32(0, Number(v), true);
  else if (width === 2) d.setUint16(0, Number(v), true);
  else out[0] = Number(v);
  return out;
}
export const writeU32 = (out: ByteBuf, v: number) => out.extend(le(4, v));
export function readU32At(bytes: Bytes, start: number): number {
  if (start + 4 > bytes.length) throw RecordError.eof();
  return dv(bytes).getUint32(start, true);
}
export function usizeToU32(v: number): number {
  if (v > 0xffffffff) throw RecordError.lengthOverflow();
  return v;
}
export const checkedAdd = (a: number, b: number) => {
  const s = a + b;
  if (!Number.isSafeInteger(s)) throw RecordError.lengthOverflow();
  return s;
};
const checkedArrayTableEnd = (prefix: number, count: number) =>
  checkedAdd(prefix, Math.max(count - 1, 0) * 4);
const checkedOffsetSlot = (start: number, index: number) => checkedAdd(start, index * 4);

export const U48_MAX = 2 ** 48 - 1;
function checkedU48(v: number): number {
  if (v > U48_MAX) throw new RecordError("U48OutOfRange", `u48 value ${v} exceeds 2^48 - 1`);
  return v;
}
const u48LeBytes = (v: number) => le(8, BigInt(checkedU48(v))).slice(0, 6);
export const u48BeBytes = (v: number) =>
  le(8, BigInt(checkedU48(v)))
    .reverse()
    .slice(2);
export function u48FromLeBytes(b: Bytes): number {
  let v = 0;
  for (let i = 5; i >= 0; i--) v = v * 256 + b[i];
  return v;
}
export function u48FromBeBytes(b: Bytes): number {
  let v = 0;
  for (let i = 0; i < 6; i++) v = v * 256 + b[i];
  return v;
}
function readExact(bytes: Bytes, n: number): Bytes {
  if (bytes.length !== n) throw RecordError.eof();
  return bytes;
}
const beBig = (b: Bytes) => dv(b).getBigUint64(0, false);
const orderPreservingI64 = (v: bigint) => {
  const out = new Uint8Array(8);
  dv(out).setBigUint64(0, BigInt.asUintN(64, v) ^ SYSTEM_REGISTRY_MARKER, false);
  return out;
};
const i64FromOrderPreserving = (b: Bytes) => BigInt.asIntN(64, beBig(b) ^ SYSTEM_REGISTRY_MARKER);
const orderPreservingI32 = (v: number) => {
  const out = new Uint8Array(4);
  dv(out).setUint32(0, (v ^ 0x80000000) >>> 0, false);
  return out;
};
const i32FromOrderPreserving = (b: Bytes) => (dv(b).getUint32(0, false) ^ 0x80000000) | 0;

// ---- descriptor codec ----

const DESCRIPTOR_CODEC_MAX_NODES = 1024;
const N = {
  DESCRIPTOR: 0,
  FIELD: 1,
  U8: 2,
  U16: 3,
  U32: 4,
  U64: 5,
  I32: 6,
  I64: 7,
  F64: 8,
  BOOL: 9,
  STRING: 10,
  BYTES: 11,
  RAW_STRING: 12,
  RAW_BYTES: 13,
  STORED_BYTES: 14,
  STORED_STRING: 15,
  STORED_JSON: 16,
  UUID: 17,
  ENUM_TAG: 18,
  TUPLE: 19,
  ARRAY: 20,
  NULLABLE: 21,
  RECORD: 22,
  ENUM: 23,
  ENUM_CASE: 24,
  U48: 25,
} as const;
const SCALAR_NODES: [ValueType, number][] = [
  ["U8", N.U8],
  ["U16", N.U16],
  ["U32", N.U32],
  ["U64", N.U64],
  ["U48", N.U48],
  ["I32", N.I32],
  ["I64", N.I64],
  ["F64", N.F64],
  ["Bool", N.BOOL],
  ["String", N.STRING],
  ["Bytes", N.BYTES],
  [rawString(), N.RAW_STRING],
  [rawBytes(), N.RAW_BYTES],
  [storedScalar("Bytes"), N.STORED_BYTES],
  [storedScalar("String"), N.STORED_STRING],
  [storedScalar("Json"), N.STORED_JSON],
  ["Uuid", N.UUID],
];

type CodecNode = {
  tag: number;
  name: string | null;
  identityName: string | null;
  identitySlot: bigint | null;
  registryId: bigint;
  children: number;
  strings: string[];
};
const node = (tag: number, over: Partial<CodecNode> = {}): CodecNode => ({
  tag,
  name: null,
  identityName: null,
  identitySlot: null,
  registryId: 0n,
  children: 0,
  strings: [],
  ...over,
});

let nodeDescriptorCache: RecordDescriptor | undefined;
function codecNodeDescriptor(): RecordDescriptor {
  return (nodeDescriptorCache ??= RecordDescriptor.new([
    ["tag", "U8"],
    ["name", nullable(rawString())],
    ["identity_name", nullable(rawString())],
    ["identity_slot", nullable("U64")],
    ["registry_id", "U64"],
    ["children", "U32"],
    ["strings", arrayOf(rawString())],
  ]));
}
let envelopeCache: RecordDescriptor | undefined;
function codecEnvelope(): RecordDescriptor {
  return (envelopeCache ??= RecordDescriptor.new([
    ["nodes", arrayOf({ kind: "Record", descriptor: codecNodeDescriptor() })],
  ]));
}

const nonCanonical = () =>
  new RecordError("NonCanonicalRecord", "nested record bytes are not canonical");
const optStr = (v: string | null): Value => V.nullable(v === null ? null : V.str(v));
function takeOptStr(v: Value): string | null {
  if (v.kind !== "Nullable") throw nonCanonical();
  if (v.v === null) return null;
  if (v.v.kind !== "String") throw nonCanonical();
  return v.v.v;
}
function takeStrings(v: Value): string[] {
  if (v.kind !== "Array") throw nonCanonical();
  return v.v.map((s) => {
    if (s.kind !== "String") throw nonCanonical();
    return s.v;
  });
}

function codecNodeValue(n: CodecNode): Value {
  const d = codecNodeDescriptor();
  const raw = d.create([
    V.u8(n.tag),
    optStr(n.name),
    optStr(n.identityName),
    V.nullable(n.identitySlot === null ? null : V.u64(n.identitySlot)),
    V.u64(n.registryId),
    V.u32(n.children),
    V.arr(n.strings.map(V.str)),
  ]);
  return { kind: "Record", v: new OwnedRecord(raw, d) };
}

function codecNodeFromValue(value: Value): CodecNode {
  if (value.kind !== "Record" || value.v.descriptor !== codecNodeDescriptor()) throw nonCanonical();
  const [tag, name, identityName, identitySlot, registryId, children, strings] = value.v.toValues();
  if (
    tag?.kind !== "U8" ||
    registryId?.kind !== "U64" ||
    children?.kind !== "U32" ||
    identitySlot?.kind !== "Nullable"
  )
    throw nonCanonical();
  let slot: bigint | null = null;
  if (identitySlot.v !== null) {
    if (identitySlot.v.kind !== "U64") throw nonCanonical();
    slot = identitySlot.v.v;
  }
  return {
    tag: tag.v,
    name: takeOptStr(name),
    identityName: takeOptStr(identityName),
    identitySlot: slot,
    registryId: registryId.v,
    children: children.v,
    strings: takeStrings(strings),
  };
}

function push(nodes: CodecNode[], n: CodecNode) {
  if (nodes.length === DESCRIPTOR_CODEC_MAX_NODES) throw RecordError.lengthOverflow();
  nodes.push(n);
}

function pushDescriptor(nodes: CodecNode[], d: RecordDescriptor) {
  push(nodes, node(N.DESCRIPTOR, { children: d.fields.length }));
  for (const f of d.fields) {
    const id = f.identity;
    push(
      nodes,
      node(N.FIELD, {
        name: f.name,
        identityName: id?.kind === "Name" || id?.kind === "NamedSlot" ? id.name : null,
        identitySlot: id?.kind === "Slot" || id?.kind === "NamedSlot" ? id.slot : null,
        children: 1,
      }),
    );
    pushValueType(nodes, f.valueType);
  }
}

function pushValueType(nodes: CodecNode[], vt: ValueType) {
  for (const [t, tag] of SCALAR_NODES) if (valueTypeEq(t, vt)) return push(nodes, node(tag));
  if (typeof vt === "string") throw unreachableType(vt);
  switch (vt.kind) {
    case "EnumTag":
      return push(
        nodes,
        node(N.ENUM_TAG, {
          name: vt.schema.name,
          registryId: vt.schema.registryId,
          strings: [...vt.schema.variants],
        }),
      );
    case "Tuple":
      push(nodes, node(N.TUPLE, { children: vt.members.length }));
      vt.members.forEach((m) => pushValueType(nodes, m));
      return;
    case "Array":
    case "Nullable":
      push(nodes, node(vt.kind === "Array" ? N.ARRAY : N.NULLABLE, { children: 1 }));
      return pushValueType(nodes, vt.inner);
    case "Record":
      push(nodes, node(N.RECORD, { children: 1 }));
      return pushDescriptor(nodes, vt.descriptor);
    case "Enum":
      push(
        nodes,
        node(N.ENUM, {
          name: vt.schema.name,
          registryId: vt.schema.registryId,
          children: vt.schema.cases.length,
        }),
      );
      for (const c of vt.schema.cases) {
        push(nodes, node(N.ENUM_CASE, { name: c.name, children: 1 }));
        pushDescriptor(nodes, c.payload);
      }
      return;
    default:
      throw unreachableType(vt);
  }
}
const unreachableType = (vt: ValueType) =>
  new Error(`unexpected value type ${formatValueType(vt)}`);

type Cursor = { pos: number };
function take(nodes: CodecNode[], cursor: Cursor): CodecNode {
  const n = nodes[cursor.pos];
  if (!n) throw RecordError.eof();
  cursor.pos++;
  return n;
}
const metadataEmpty = (n: CodecNode) =>
  n.name === null &&
  n.identityName === null &&
  n.identitySlot === null &&
  n.registryId === 0n &&
  n.children === 0 &&
  n.strings.length === 0;
const noIdentity = (n: CodecNode) => n.identityName === null && n.identitySlot === null;

function fieldIdentity(n: CodecNode): FieldIdentity | null {
  if (n.identityName !== null && n.identitySlot !== null)
    return { kind: "NamedSlot", name: n.identityName, slot: n.identitySlot };
  if (n.identityName !== null) return { kind: "Name", name: n.identityName };
  if (n.identitySlot !== null) return { kind: "Slot", slot: n.identitySlot };
  return null;
}

function decodeDescriptorNodes(nodes: CodecNode[], cursor: Cursor): RecordDescriptor {
  const n = take(nodes, cursor);
  if (
    n.tag !== N.DESCRIPTOR ||
    n.name !== null ||
    !noIdentity(n) ||
    n.registryId !== 0n ||
    n.strings.length
  )
    throw nonCanonical();
  const fields: DescriptorField[] = [];
  for (let i = 0; i < n.children; i++) {
    const f = take(nodes, cursor);
    if (f.tag !== N.FIELD || f.registryId !== 0n || f.children !== 1 || f.strings.length)
      throw nonCanonical();
    fields.push({
      name: f.name,
      identity: fieldIdentity(f),
      valueType: decodeValueTypeNodes(nodes, cursor),
    });
  }
  for (const f of fields) validateSchemaValueType(f.valueType);
  return RecordDescriptor.fromLogicalFields(fields);
}

function decodeValueTypeNodes(nodes: CodecNode[], cursor: Cursor): ValueType {
  const n = take(nodes, cursor);
  const scalar = SCALAR_NODES.find(([, tag]) => tag === n.tag);
  if (scalar) {
    if (!metadataEmpty(n)) throw nonCanonical();
    return scalar[0];
  }
  switch (n.tag) {
    case N.ENUM_TAG:
      if (n.name === null || !noIdentity(n) || n.children !== 0) throw nonCanonical();
      return {
        kind: "EnumTag",
        schema: ScalarEnumSchema.fromParts(n.registryId, n.name, n.strings),
      };
    case N.TUPLE: {
      if (n.name !== null || !noIdentity(n) || n.registryId !== 0n || n.strings.length)
        throw nonCanonical();
      const members: ValueType[] = [];
      for (let i = 0; i < n.children; i++) members.push(decodeValueTypeNodes(nodes, cursor));
      const vt: ValueType = { kind: "Tuple", members };
      if (fixedSize(vt) === undefined) throw RecordError.invalidTupleMember(vt);
      return vt;
    }
    case N.ARRAY:
    case N.NULLABLE:
    case N.RECORD:
      if (
        n.name !== null ||
        !noIdentity(n) ||
        n.registryId !== 0n ||
        n.children !== 1 ||
        n.strings.length
      )
        throw nonCanonical();
      if (n.tag === N.RECORD)
        return { kind: "Record", descriptor: decodeDescriptorNodes(nodes, cursor) };
      return {
        kind: n.tag === N.ARRAY ? "Array" : "Nullable",
        inner: decodeValueTypeNodes(nodes, cursor),
      };
    case N.ENUM: {
      if (n.name === null || !noIdentity(n) || n.strings.length) throw nonCanonical();
      const cases: EnumCase[] = [];
      for (let i = 0; i < n.children; i++) {
        const c = take(nodes, cursor);
        if (
          c.tag !== N.ENUM_CASE ||
          c.name === null ||
          !noIdentity(c) ||
          c.registryId !== 0n ||
          c.children !== 1 ||
          c.strings.length
        )
          throw nonCanonical();
        cases.push(enumCase(c.name, decodeDescriptorNodes(nodes, cursor)));
      }
      return { kind: "Enum", schema: EnumSchema.new(n.name, cases).withRegistryId(n.registryId) };
    }
    default:
      throw nonCanonical();
  }
}

export function encodeRecordDescriptor(d: RecordDescriptor): Bytes {
  const nodes: CodecNode[] = [];
  pushDescriptor(nodes, d);
  return codecEnvelope().create([V.arr(nodes.map(codecNodeValue))]);
}

export function decodeRecordDescriptor(encoded: Bytes): RecordDescriptor {
  const envelope = codecEnvelope();
  const values = envelope.bind(encoded).toValues();
  if (!equalBytes(envelope.create(values), encoded)) throw nonCanonical();
  const [nodesValue] = values;
  if (values.length !== 1 || nodesValue.kind !== "Array") throw nonCanonical();
  if (nodesValue.v.length > DESCRIPTOR_CODEC_MAX_NODES) throw RecordError.lengthOverflow();
  const nodes = nodesValue.v.map(codecNodeFromValue);
  const cursor = { pos: 0 };
  const d = decodeDescriptorNodes(nodes, cursor);
  if (cursor.pos !== nodes.length || !equalBytes(encodeRecordDescriptor(d), encoded))
    throw nonCanonical();
  return d;
}

const PERSISTED_KINDS = [
  "Descriptor",
  "Field",
  "U8",
  "U16",
  "U32",
  "U64",
  "I32",
  "I64",
  "F64",
  "Bool",
  "String",
  "Bytes",
  "RawString",
  "RawBytes",
  "StoredBytes",
  "StoredString",
  "StoredJson",
  "Uuid",
  "EnumTag",
  "Tuple",
  "Array",
  "Nullable",
  "Record",
  "Enum",
  "EnumCase",
  "U48",
];
let persistedSchemaCache: EnumSchema | undefined;
function persistedNodeSchema(): EnumSchema {
  return (persistedSchemaCache ??= EnumSchema.new(
    "groove.persisted_descriptor.v2",
    PERSISTED_KINDS.map((name, tag) => {
      let fields: [string, ValueType][] = [];
      if (tag === N.DESCRIPTOR || tag === N.TUPLE) fields = [["children", "U32"]];
      else if (tag === N.FIELD) fields = [["name", nullable(rawString())]];
      else if (tag === N.ENUM_TAG)
        fields = [
          ["name", rawString()],
          ["registry_id", "U64"],
          ["strings", arrayOf(rawString())],
        ];
      else if (tag === N.ENUM)
        fields = [
          ["name", rawString()],
          ["registry_id", "U64"],
          ["children", "U32"],
        ];
      else if (tag === N.ENUM_CASE) fields = [["name", rawString()]];
      return enumCase(name, RecordDescriptor.new(fields));
    }),
  ));
}
let persistedEnvelopeCache: RecordDescriptor | undefined;
function persistedEnvelope(): RecordDescriptor {
  return (persistedEnvelopeCache ??= RecordDescriptor.new([
    ["nodes", arrayOf({ kind: "Enum", schema: persistedNodeSchema() })],
  ]));
}

function persistedNodeValue(n: CodecNode): Value {
  const req = () => {
    if (n.name === null) throw nonCanonical();
    return V.str(n.name);
  };
  let fields: Value[] = [];
  if (n.tag === N.DESCRIPTOR || n.tag === N.TUPLE) fields = [V.u32(n.children)];
  else if (n.tag === N.FIELD) fields = [optStr(n.name)];
  else if (n.tag === N.ENUM_TAG) fields = [req(), V.u64(n.registryId), V.arr(n.strings.map(V.str))];
  else if (n.tag === N.ENUM) fields = [req(), V.u64(n.registryId), V.u32(n.children)];
  else if (n.tag === N.ENUM_CASE) fields = [req()];
  const payload = persistedNodeSchema().case(n.tag).payload;
  return { kind: "Enum", v: EnumValue.create(n.tag, payload, fields) };
}

function persistedNodeFromValue(value: Value): CodecNode {
  if (value.kind !== "Enum" || value.v.tag > 255) throw nonCanonical();
  const tag = value.v.tag;
  const f = value.v.record.toValues();
  const n = node(tag);
  if ((tag === N.DESCRIPTOR || tag === N.TUPLE) && f.length === 1 && f[0].kind === "U32") {
    n.children = f[0].v;
  } else if (tag === N.FIELD && f.length === 1) {
    n.name = takeOptStr(f[0]);
    n.children = 1;
  } else if (
    tag === N.ENUM_TAG &&
    f.length === 3 &&
    f[0].kind === "String" &&
    f[1].kind === "U64"
  ) {
    n.name = f[0].v;
    n.registryId = f[1].v;
    n.strings = takeStrings(f[2]);
  } else if (
    tag === N.ENUM &&
    f.length === 3 &&
    f[0].kind === "String" &&
    f[1].kind === "U64" &&
    f[2].kind === "U32"
  ) {
    n.name = f[0].v;
    n.registryId = f[1].v;
    n.children = f[2].v;
  } else if (tag === N.ENUM_CASE && f.length === 1 && f[0].kind === "String") {
    n.name = f[0].v;
    n.children = 1;
  } else if ((tag === N.ARRAY || tag === N.NULLABLE || tag === N.RECORD) && f.length === 0) {
    n.children = 1;
  } else if (!(((tag >= N.U8 && tag <= N.UUID) || tag === N.U48) && f.length === 0)) {
    throw nonCanonical();
  }
  return n;
}

export function encodePersistedRecordDescriptor(d: RecordDescriptor): Bytes {
  const nodes: CodecNode[] = [];
  pushDescriptor(nodes, d);
  return persistedEnvelope().create([V.arr(nodes.map(persistedNodeValue))]);
}

export function decodePersistedRecordDescriptor(encoded: Bytes): RecordDescriptor {
  const envelope = persistedEnvelope();
  const values = envelope.bind(encoded).toValues();
  if (!equalBytes(envelope.create(values), encoded)) throw nonCanonical();
  const [records] = values;
  if (values.length !== 1 || records.kind !== "Array") throw nonCanonical();
  if (records.v.length > DESCRIPTOR_CODEC_MAX_NODES) throw RecordError.lengthOverflow();
  const nodes = records.v.map(persistedNodeFromValue);
  const cursor = { pos: 0 };
  const d = decodeDescriptorNodes(nodes, cursor);
  if (cursor.pos !== nodes.length || !equalBytes(encodePersistedRecordDescriptor(d), encoded))
    throw nonCanonical();
  return d;
}

// ---- value encoding ----

const mismatch = (vt: ValueType) => RecordError.typeMismatch(vt);
const isKind = <K extends string>(vt: ValueType, kind: K): vt is Extract<ValueType, { kind: K }> =>
  typeof vt !== "string" && vt.kind === kind;

export function encodeValue(value: Value, vt: ValueType): Bytes {
  const out = new ByteBuf();
  encodeValueInto(out, value, vt);
  return out.finish();
}

const largeErr = (e: unknown): never => {
  if (e instanceof LargeValueError) throw RecordError.largeValue(e);
  throw e;
};

export function encodeValueInto(out: ByteBuf, value: Value, vt: ValueType): void {
  try {
    if (
      value.kind === "String" &&
      (vt === "String" || (isKind(vt, "Stored") && vt.stored !== "Bytes"))
    )
      return encodePrimitiveStoredScalarInto(
        vt === "String" ? "String" : (vt as { stored: LargeValueKind }).stored,
        utf8(value.v),
        out,
      );
    if (
      value.kind === "Bytes" &&
      (vt === "Bytes" || (isKind(vt, "Stored") && vt.stored === "Bytes"))
    )
      return encodePrimitiveStoredScalarInto("Bytes", value.v, out);
    if (value.kind === "Large") {
      const kind =
        vt === "String"
          ? "String"
          : vt === "Bytes"
            ? "Bytes"
            : isKind(vt, "Stored")
              ? vt.stored
              : null;
      if (kind !== null && value.v.kind === kind)
        return out.extend(encodeStoredScalar(kind, { kind: "Chunked", value: value.v }));
    }
  } catch (e) {
    largeErr(e);
  }
  if (value.kind === "String" && isKind(vt, "RawString")) return out.extend(utf8(value.v));
  if (value.kind === "Bytes" && isKind(vt, "RawBytes")) return out.extend(value.v);
  if (value.kind === "Array" && isKind(vt, "Array")) return encodeArray(out, value.v, vt.inner);
  if (value.kind === "Record" && isKind(vt, "Record")) {
    ensureValueType(value, vt);
    return out.extend(value.v.raw);
  }
  if (value.kind === "Enum" && isKind(vt, "Enum")) {
    ensureEnumValue(value.v, vt.schema);
    return appendVariantRecord(out, value.v.tag, value.v.record.raw);
  }
  if (isFixedSize(vt)) return encodeFixedValue(out, value, vt);
  throw mismatch(vt);
}

export function encodeFixedValue(out: ByteBuf, value: Value, vt: ValueType): void {
  switch (value.kind) {
    case "U8":
    case "U16":
    case "U32":
    case "I32":
      if (vt !== value.kind) break;
      return out.extend(
        le(value.kind === "U8" ? 1 : value.kind === "U16" ? 2 : 4, value.v, value.kind === "I32"),
      );
    case "U64":
    case "I64":
      if (vt !== value.kind) break;
      return out.extend(le(8, value.v, value.kind === "I64"));
    case "U48":
      if (vt !== "U48") break;
      return out.extend(u48LeBytes(value.v));
    case "F64":
      if (vt !== "F64") break;
      if (Number.isNaN(value.v)) throw RecordError.nan();
      {
        const b = new Uint8Array(8);
        dv(b).setFloat64(0, value.v, true);
        return out.extend(b);
      }
    case "Bool":
      if (vt !== "Bool") break;
      return out.push(value.v ? 1 : 0);
    case "Uuid":
      if (vt !== "Uuid") break;
      return out.extend(value.v);
    case "String":
      if (!isKind(vt, "EnumTag")) break;
      return out.push(vt.schema.discriminant(value.v));
    case "EnumTag":
      if (!isKind(vt, "EnumTag")) break;
      return out.push(value.v);
    case "Tuple":
      if (!isKind(vt, "Tuple")) break;
      return encodeTuple(out, value.v, vt.members);
    case "Nullable":
      if (!isKind(vt, "Nullable")) break;
      return encodeNullable(out, value.v, vt.inner);
  }
  throw mismatch(vt);
}

function decodeScalar(kind: LargeValueKind, bytes: Bytes, vt: ValueType, mapUtf8: boolean): Value {
  let scalar;
  try {
    scalar = decodeStoredScalar(kind, bytes);
  } catch (e) {
    if (mapUtf8 && e instanceof LargeValueError && e.kind === "InvalidUtf8")
      throw RecordError.utf8();
    return largeErr(e);
  }
  if (scalar.kind === "Primitive") {
    if (kind === "Bytes") return V.bytes(scalar.bytes);
    try {
      return V.str(fromUtf8(scalar.bytes));
    } catch {
      throw RecordError.utf8();
    }
  }
  if (scalar.value.kind === kind) return { kind: "Large", v: scalar.value };
  throw mismatch(vt);
}

export function decodeValue(bytes: Bytes, vt: ValueType): Value {
  if (typeof vt === "string") {
    switch (vt) {
      case "U8":
        return V.u8(readExact(bytes, 1)[0]);
      case "U16":
        return { kind: "U16", v: dv(readExact(bytes, 2)).getUint16(0, true) };
      case "U32":
        return V.u32(dv(readExact(bytes, 4)).getUint32(0, true));
      case "U64":
        return V.u64(dv(readExact(bytes, 8)).getBigUint64(0, true));
      case "U48":
        return { kind: "U48", v: u48FromLeBytes(readExact(bytes, 6)) };
      case "I32":
        return { kind: "I32", v: dv(readExact(bytes, 4)).getInt32(0, true) };
      case "I64":
        return { kind: "I64", v: dv(readExact(bytes, 8)).getBigInt64(0, true) };
      case "F64": {
        const v = dv(readExact(bytes, 8)).getFloat64(0, true);
        if (Number.isNaN(v)) throw RecordError.nan();
        return { kind: "F64", v };
      }
      case "Bool": {
        const b = readExact(bytes, 1)[0];
        if (b > 1) throw RecordError.invalidBool(b);
        return { kind: "Bool", v: b === 1 };
      }
      case "String":
        return decodeScalar("String", bytes, vt, true);
      case "Bytes":
        return decodeScalar("Bytes", bytes, vt, false);
      case "Uuid":
        return { kind: "Uuid", v: readExact(bytes, 16).slice() };
    }
  }
  switch (vt.kind) {
    case "RawString":
      try {
        return V.str(fromUtf8(bytes));
      } catch {
        throw RecordError.utf8();
      }
    case "RawBytes":
      return V.bytes(bytes.slice());
    case "Stored":
      return decodeScalar(vt.stored, bytes, vt, false);
    case "EnumTag": {
      const d = readExact(bytes, 1)[0];
      vt.schema.variant(d);
      return { kind: "EnumTag", v: d };
    }
    case "Tuple":
      return decodeTuple(bytes, vt.members);
    case "Array":
      return decodeArray(bytes, vt.inner);
    case "Nullable":
      return decodeNullable(bytes, vt.inner);
    case "Record":
      return { kind: "Record", v: new OwnedRecord(bytes.slice(), vt.descriptor) };
    case "Enum": {
      const [tag, payload] = splitEnumHeader(bytes);
      const c = vt.schema.case(tag);
      return { kind: "Enum", v: new EnumValue(tag, new OwnedRecord(payload.slice(), c.payload)) };
    }
  }
}

function splitEnumHeader(bytes: Bytes): [number, Bytes] {
  try {
    return splitVariantRecord(bytes);
  } catch (e) {
    if (e instanceof RecordError && e.kind === "InvalidSchemaVersionHeader")
      throw new RecordError("InvalidEnumHeader", "invalid enum value header");
    throw e;
  }
}

export const validateValue = (bytes: Bytes, vt: ValueType) => validateValueInner(bytes, vt, false);
export const validateCanonicalValue = (bytes: Bytes, vt: ValueType) =>
  validateValueInner(bytes, vt, true);

function validateInlineScalar(kind: LargeValueKind, bytes: Bytes) {
  try {
    inlineScalarBytes(kind, bytes);
  } catch (e) {
    if (e instanceof LargeValueError && e.kind === "RequiresEvaluation") return;
    largeErr(e);
  }
}

function validateValueInner(bytes: Bytes, vt: ValueType, strict: boolean): void {
  if (typeof vt === "string") {
    switch (vt) {
      case "F64":
        decodeValue(bytes, vt);
        return;
      case "Bool": {
        const b = readExact(bytes, 1)[0];
        if (b > 1) throw RecordError.invalidBool(b);
        return;
      }
      case "String":
      case "Bytes":
        return validateInlineScalar(vt, bytes);
      default:
        readExact(bytes, fixedSize(vt)!);
        return;
    }
  }
  switch (vt.kind) {
    case "RawString":
      try {
        fromUtf8(bytes);
      } catch {
        throw RecordError.utf8();
      }
      return;
    case "RawBytes":
      return;
    case "Stored":
      return validateInlineScalar(vt.stored, bytes);
    case "EnumTag":
      vt.schema.variant(readExact(bytes, 1)[0]);
      return;
    case "Tuple": {
      let offset = 0;
      for (const m of vt.members) {
        const w = fixedSize(m);
        if (w === undefined) throw RecordError.invalidTupleMember(m);
        const end = checkedAdd(offset, w);
        if (end > bytes.length) throw RecordError.eof();
        validateValueInner(bytes.subarray(offset, end), m, strict);
        offset = end;
      }
      if (offset !== bytes.length) throw RecordError.invalidOffset();
      return;
    }
    case "Array":
      return validateArray(bytes, vt.inner, strict);
    case "Nullable": {
      if (!bytes.length) throw RecordError.eof();
      const flag = bytes[0];
      if (flag === 0) return checkNullPayload(bytes.subarray(1), vt.inner);
      if (flag === 1) return validateValueInner(bytes.subarray(1), vt.inner, strict);
      throw RecordError.invalidNullFlag(flag);
    }
    case "Record":
      return vt.descriptor.bind(bytes).validateCanonical();
    case "Enum": {
      const [tag, payload] = splitEnumHeader(bytes);
      return vt.schema.case(tag).payload.bind(payload).validateCanonical();
    }
  }
}

export function checkNullPayload(payload: Bytes, inner: ValueType): void {
  if (fixedSize(inner) !== undefined) {
    if (payload.some((b) => b !== 0)) throw RecordError.invalidOffset();
  } else if (payload.length) throw RecordError.invalidOffset();
}

/** Iterates variable-width array element spans after bounds-checking the table. */
function* variableArraySpans(bytes: Bytes): Generator<[number, number]> {
  const count = readU32At(bytes, 0);
  let start = checkedArrayTableEnd(4, count);
  if (start > bytes.length) throw RecordError.eof();
  for (let index = 0; index < count; index++) {
    const end = index + 1 === count ? bytes.length : readU32At(bytes, checkedOffsetSlot(4, index));
    if (end < start || end > bytes.length) throw RecordError.invalidOffset();
    yield [start, end];
    start = end;
  }
}

function fixedChunks(bytes: Bytes, size: number): Bytes[] {
  if (size === 0 || bytes.length % size !== 0) throw RecordError.invalidOffset();
  const out: Bytes[] = [];
  for (let i = 0; i < bytes.length; i += size) out.push(bytes.subarray(i, i + size));
  return out;
}

function validateArray(bytes: Bytes, elem: ValueType, strict: boolean): void {
  const size = fixedSize(elem);
  if (size !== undefined) {
    for (const c of fixedChunks(bytes, size)) validateValueInner(c, elem, strict);
    return;
  }
  if (readU32At(bytes, 0) === 0) {
    if (bytes.length !== 4) throw RecordError.invalidOffset();
    return;
  }
  for (const [s, e] of variableArraySpans(bytes))
    validateValueInner(bytes.subarray(s, e), elem, strict);
}

function encodeNullable(out: ByteBuf, value: Value | null, inner: ValueType): void {
  if (value !== null) {
    out.push(1);
    if (isFixedSize(inner)) encodeFixedValue(out, value, inner);
    else encodeValueInto(out, value, inner);
  } else {
    out.push(0);
    const size = fixedSize(inner);
    if (size !== undefined) out.resize(out.len + size);
  }
}

export function visitEncodedIndirectValues(
  bytes: Bytes,
  vt: ValueType,
  visitor: (bytes: Bytes, vt: ValueType) => boolean,
): boolean {
  if (vt === "String" || vt === "Bytes" || isKind(vt, "Stored")) {
    const [tag] = splitVariantRecord(bytes);
    if (tag === 0) return false;
    if (tag === 1) return visitor(bytes, vt);
    throw RecordError.largeValue(new LargeValueError("MalformedScalar"));
  }
  if (isKind(vt, "Nullable")) {
    if (!bytes.length) throw RecordError.eof();
    if (bytes[0] === 0) return false;
    if (bytes[0] === 1) return visitEncodedIndirectValues(bytes.subarray(1), vt.inner, visitor);
    throw RecordError.invalidNullFlag(bytes[0]);
  }
  if (isKind(vt, "Record")) return vt.descriptor.visitEncodedIndirectFields(bytes, null, visitor);
  if (isKind(vt, "Enum")) {
    const [tag, payload] = splitVariantRecord(bytes);
    return vt.schema.case(tag).payload.visitEncodedIndirectFields(payload, null, visitor);
  }
  if (isKind(vt, "Array") && mayContainStoredScalar(vt.inner)) {
    for (const [s, e] of variableArraySpans(bytes))
      if (visitEncodedIndirectValues(bytes.subarray(s, e), vt.inner, visitor)) return true;
    return false;
  }
  return false;
}

function decodeNullable(bytes: Bytes, inner: ValueType): Value {
  if (!bytes.length) throw RecordError.eof();
  const flag = bytes[0];
  if (flag === 0) {
    checkNullPayload(bytes.subarray(1), inner);
    return V.nullable(null);
  }
  if (flag === 1) return V.nullable(decodeValue(bytes.subarray(1), inner));
  throw RecordError.invalidNullFlag(flag);
}

function encodeArray(out: ByteBuf, values: Value[], elem: ValueType): void {
  for (const v of values) ensureValueType(v, elem);
  if (isFixedSize(elem)) {
    for (const v of values) encodeFixedValue(out, v, elem);
    return;
  }
  const base = out.len;
  writeU32(out, usizeToU32(values.length));
  const offsetCount = Math.max(values.length - 1, 0);
  out.resize(checkedArrayTableEnd(out.len, values.length));
  values.forEach((v, index) => {
    encodeValueInto(out, v, elem);
    if (index < offsetCount)
      out.setU32(checkedOffsetSlot(base + 4, index), usizeToU32(out.len - base));
  });
}

function decodeArray(bytes: Bytes, elem: ValueType): Value {
  const size = fixedSize(elem);
  if (size !== undefined) return V.arr(fixedChunks(bytes, size).map((c) => decodeValue(c, elem)));
  if (readU32At(bytes, 0) === 0) {
    if (bytes.length !== 4) throw RecordError.invalidOffset();
    return V.arr([]);
  }
  const values: Value[] = [];
  for (const [s, e] of variableArraySpans(bytes))
    values.push(decodeValue(bytes.subarray(s, e), elem));
  return V.arr(values);
}

export function ensureValueType(value: Value, vt: ValueType): void {
  switch (value.kind) {
    case "U48":
      if (vt !== "U48") break;
      checkedU48(value.v);
      return;
    case "U8":
    case "U16":
    case "U32":
    case "U64":
    case "I32":
    case "I64":
    case "Bool":
    case "Uuid":
      if (vt === value.kind) return;
      break;
    case "String":
      if (
        vt === "String" ||
        isKind(vt, "RawString") ||
        (isKind(vt, "Stored") && vt.stored !== "Bytes")
      )
        return;
      if (isKind(vt, "EnumTag")) {
        vt.schema.discriminant(value.v);
        return;
      }
      break;
    case "Bytes":
      if (
        vt === "Bytes" ||
        isKind(vt, "RawBytes") ||
        (isKind(vt, "Stored") && vt.stored === "Bytes")
      )
        return;
      break;
    case "Large":
      if (
        (isKind(vt, "Stored") && vt.stored === value.v.kind) ||
        (vt === "String" && value.v.kind === "String") ||
        (vt === "Bytes" && value.v.kind === "Bytes")
      )
        return;
      break;
    case "F64":
      if (vt !== "F64") break;
      if (Number.isNaN(value.v)) throw RecordError.nan();
      return;
    case "EnumTag":
      if (!isKind(vt, "EnumTag")) break;
      vt.schema.variant(value.v);
      return;
    case "Tuple":
      if (!isKind(vt, "Tuple")) break;
      if (value.v.length !== vt.members.length)
        throw RecordError.arity(vt.members.length, value.v.length);
      value.v.forEach((v, i) => {
        const m = vt.members[i];
        if (fixedSize(m) === undefined) throw RecordError.invalidTupleMember(m);
        ensureValueType(v, m);
      });
      return;
    case "Array":
      if (!isKind(vt, "Array")) break;
      for (const v of value.v) ensureValueType(v, vt.inner);
      return;
    case "Nullable":
      if (!isKind(vt, "Nullable")) break;
      if (value.v !== null) ensureValueType(value.v, vt.inner);
      return;
    case "Record":
      if (!isKind(vt, "Record")) break;
      if (value.v.descriptor !== vt.descriptor) throw mismatch(vt);
      return validateEmbeddedRecord(value.v.borrowed());
    case "Enum":
      if (!isKind(vt, "Enum")) break;
      return ensureEnumValue(value.v, vt.schema);
  }
  throw mismatch(vt);
}

function ensureEnumValue(value: EnumValue, schema: EnumSchema): void {
  const c = schema.case(value.tag);
  if (value.record.descriptor !== c.payload) throw mismatch({ kind: "Enum", schema });
  validateEmbeddedRecord(value.record.borrowed());
}

function containsTuple(vt: ValueType): boolean {
  if (typeof vt === "string") return false;
  switch (vt.kind) {
    case "Tuple":
      return true;
    case "Array":
    case "Nullable":
      return containsTuple(vt.inner);
    case "Record":
      return vt.descriptor.fields.some((f) => containsTuple(f.valueType));
    case "Enum":
      return vt.schema.cases.some((c) => c.payload.fields.some((f) => containsTuple(f.valueType)));
    default:
      return false;
  }
}

function validateEmbeddedRecord(record: BorrowedRecord): void {
  const d = record.descriptor;
  if (!d.fields.some((f) => containsTuple(f.valueType))) {
    try {
      record.validateCanonical();
      return;
    } catch {
      // fall through to the legacy round-trip check for its error ordering
    }
  }
  const values = record.toValues();
  if (!equalBytes(d.create(values), record.raw)) throw nonCanonical();
}

export function validateSchemaValueType(vt: ValueType): void {
  if (typeof vt === "string") return;
  switch (vt.kind) {
    case "Tuple":
      for (const m of vt.members) {
        validateSchemaValueType(m);
        if (fixedSize(m) === undefined) throw RecordError.invalidTupleMember(m);
      }
      return;
    case "Array":
    case "Nullable":
      return validateSchemaValueType(vt.inner);
    case "Record":
      for (const f of vt.descriptor.fields) validateSchemaValueType(f.valueType);
      return;
    case "EnumTag":
      return vt.schema.validate();
    case "Enum":
      vt.schema.validate();
      for (const c of vt.schema.cases)
        for (const f of c.payload.fields) validateSchemaValueType(f.valueType);
      return;
  }
}

function encodeTuple(out: ByteBuf, values: Value[], members: ValueType[]): void {
  if (values.length !== members.length) throw RecordError.arity(members.length, values.length);
  values.forEach((v, i) => encodeTupleMember(out, v, members[i]));
}

function encodeTupleMember(out: ByteBuf, value: Value, vt: ValueType): void {
  const be = (width: number, v: number | bigint) => out.extend(le(width, v).reverse());
  switch (value.kind) {
    case "U8":
      if (vt === "U8") return out.push(value.v);
      break;
    case "U16":
    case "U32":
      if (vt === value.kind) return be(value.kind === "U16" ? 2 : 4, value.v);
      break;
    case "U64":
      if (vt === "U64") return be(8, value.v);
      break;
    case "U48":
      if (vt === "U48") return out.extend(u48BeBytes(value.v));
      break;
    case "I32":
      if (vt === "I32") return out.extend(orderPreservingI32(value.v));
      break;
    case "I64":
      if (vt === "I64") return out.extend(orderPreservingI64(value.v));
      break;
    case "Bool":
      if (vt === "Bool") return out.push(value.v ? 1 : 0);
      break;
    case "Uuid":
      if (vt === "Uuid") return out.extend(value.v);
      break;
    case "String":
      if (isKind(vt, "EnumTag")) return out.push(vt.schema.discriminant(value.v));
      break;
    case "EnumTag":
      if (isKind(vt, "EnumTag")) return out.push(value.v);
      break;
    case "Tuple":
      if (isKind(vt, "Tuple")) return encodeTuple(out, value.v, vt.members);
      break;
    case "Nullable":
      if (!isKind(vt, "Nullable")) break;
      out.push(value.v !== null ? 1 : 0);
      if (value.v !== null) return encodeTupleMember(out, value.v, vt.inner);
      {
        const size = fixedSize(vt.inner);
        if (size === undefined) throw RecordError.invalidTupleMember(vt.inner);
        out.resize(out.len + size);
      }
      return;
  }
  throw mismatch(vt);
}

function decodeTuple(bytes: Bytes, members: ValueType[]): Value {
  const values: Value[] = [];
  let offset = 0;
  for (const m of members) {
    const w = fixedSize(m);
    if (w === undefined) throw RecordError.invalidTupleMember(m);
    const end = checkedAdd(offset, w);
    if (end > bytes.length) throw RecordError.eof();
    values.push(decodeTupleMember(bytes.subarray(offset, end), m));
    offset = end;
  }
  if (offset !== bytes.length) throw RecordError.invalidOffset();
  return { kind: "Tuple", v: values };
}

function decodeTupleMember(bytes: Bytes, vt: ValueType): Value {
  if (typeof vt === "string") {
    switch (vt) {
      case "U8":
      case "Bool":
      case "Uuid":
        return decodeValue(bytes, vt);
      case "U16":
        return { kind: "U16", v: dv(readExact(bytes, 2)).getUint16(0, false) };
      case "U32":
        return V.u32(dv(readExact(bytes, 4)).getUint32(0, false));
      case "U64":
        return V.u64(beBig(readExact(bytes, 8)));
      case "U48":
        return { kind: "U48", v: u48FromBeBytes(readExact(bytes, 6)) };
      case "I32":
        return { kind: "I32", v: i32FromOrderPreserving(readExact(bytes, 4)) };
      case "I64":
        return { kind: "I64", v: i64FromOrderPreserving(readExact(bytes, 8)) };
    }
  } else {
    switch (vt.kind) {
      case "EnumTag":
        return decodeValue(bytes, vt);
      case "Tuple":
        return decodeTuple(bytes, vt.members);
      case "Nullable":
        return decodeNullable(bytes, vt.inner);
    }
  }
  throw RecordError.invalidTupleMember(vt);
}
