// Compact binary record descriptors, layout, and encoded row access.
// Fixed-width fields first, then a u32 end-offset table for all but the last
// variable field, then variable payloads. Multi-byte scalars are little-endian.
import { type Bytes, fromUtf8 } from "../../support/bytes.js";
import { Interner } from "../../support/runtime.js";
import { LargeValueError, inlineScalarBytes, type LargeValueRef } from "../large_values.js";
import {
  ByteBuf,
  type Value,
  type ValueType,
  checkedAdd,
  checkNullPayload,
  decodeValue,
  encodeFixedValue,
  encodeValue,
  encodeValueInto,
  ensureValueType,
  fixedSize,
  formatValueType,
  isFixedSize,
  mayContainStoredScalar,
  readU32At,
  registryCompatibleWith,
  registryReboundLayoutCompatibleWith,
  canEvolveRegistryTo,
  u48FromLeBytes,
  usizeToU32,
  validateCanonicalValue,
  validateSchemaValueType,
  validateValue,
  valueTypeEq,
  valueTypeKey,
  visitEncodedIndirectValues,
  writeU32,
  le,
} from "./values.js";

export * from "./values.js";
export * from "./macros.js";

export const MAX_VARIANT_TAG_LEN = 5;

export class RecordError extends Error {
  constructor(
    readonly kind: string,
    message: string,
    readonly cause?: unknown,
  ) {
    super(message);
  }
  static largeValue(e: LargeValueError) {
    return new RecordError("LargeValue", e.message, e);
  }
  static arity(expected: number, actual: number) {
    return new RecordError("ArityMismatch", `expected ${expected} values, got ${actual}`);
  }
  static fieldNotFound(name: string) {
    return new RecordError("FieldNotFound", `field not found: ${name}`);
  }
  static oob(index: number, len: number) {
    return new RecordError(
      "FieldIndexOutOfBounds",
      `field index ${index} out of bounds for descriptor length ${len}`,
    );
  }
  static invalidBool(b: number) {
    return new RecordError("InvalidBool", `invalid boolean byte: ${b}`);
  }
  static invalidNullFlag(b: number) {
    return new RecordError("InvalidNullFlag", `invalid nullable flag byte: ${b}`);
  }
  static enumTooManyVariants(name: string, variants: number) {
    return new RecordError(
      "EnumTooManyVariants",
      `enum ${name} has ${variants} variants; maximum is 256`,
    );
  }
  static invalidOffset() {
    return new RecordError("InvalidOffset", "invalid offset");
  }
  static schemaHeader() {
    return new RecordError("InvalidSchemaVersionHeader", "invalid schema-version header");
  }
  static variantTagOutOfRange(tag: number | bigint) {
    return new RecordError(
      "VariantTagOutOfRange",
      `table variant tag ${tag} exceeds the bounded u32 tag space`,
    );
  }
  static invalidTupleMember(vt: ValueType) {
    return new RecordError(
      "InvalidTupleMember",
      `tuple members must be fixed-width, got ${formatValueType(vt)}`,
    );
  }
  static utf8() {
    return new RecordError("InvalidUtf8", "invalid utf-8 string");
  }
  static nan() {
    return new RecordError("InvalidF64NaN", "NaN is not a valid f64 record value");
  }
  static lengthOverflow() {
    return new RecordError("LengthOverflow", "encoded length exceeds u32::MAX");
  }
  static typeMismatch(expected: ValueType) {
    return new RecordError(
      "TypeMismatch",
      `value does not match type ${formatValueType(expected)}`,
    );
  }
  static eof() {
    return new RecordError("UnexpectedEof", "unexpected end of record");
  }
}

export const encodeSingleFieldValue = encodeValue;
export const decodeSingleFieldValue = decodeValue;

export type FieldIdentity =
  | { kind: "Name"; name: string }
  | { kind: "Slot"; slot: bigint }
  | { kind: "NamedSlot"; name: string; slot: bigint };

export type DescriptorField = {
  name: string | null;
  identity: FieldIdentity | null;
  valueType: ValueType;
};

export const descriptorField = (name: string, valueType: ValueType): DescriptorField => ({
  name,
  identity: null,
  valueType,
});

export function logicalName(f: DescriptorField): string | null {
  if (f.identity?.kind === "Name" || f.identity?.kind === "NamedSlot") return f.identity.name;
  return f.name;
}

function identityEq(a: FieldIdentity | null, b: FieldIdentity | null): boolean {
  if (a === null || b === null) return a === b;
  return (
    a.kind === b.kind &&
    (a as { name?: string }).name === (b as { name?: string }).name &&
    (a as { slot?: bigint }).slot === (b as { slot?: bigint }).slot
  );
}

type FieldLayout =
  | { static: true; offset: number; width: number }
  | { static: false; variableIdx: number };
const layoutEq = (a: FieldLayout, b: FieldLayout) =>
  a.static
    ? b.static && a.offset === b.offset && a.width === b.width
    : !b.static && a.variableIdx === b.variableIdx;

type Span = { start: number; end: number };

let nextDescriptorId = 0;
const descriptorInterner = new Interner<RecordDescriptor>((d) => d.key);

/** Interned descriptor: equal field lists share one instance, so `===` is equality. */
export class RecordDescriptor {
  readonly id = nextDescriptorId++;
  readonly key: string;
  readonly layoutFields: FieldLayout[] = [];
  readonly logicalByPhysical: number[] = [];
  readonly fixedSize: number = 0;
  readonly variableCount: number = 0;

  private constructor(readonly fields: readonly DescriptorField[]) {
    this.key = fields
      .map(
        (f) =>
          `${JSON.stringify(f.name)}|${f.identity ? JSON.stringify(f.identity, (_, v) => (typeof v === "bigint" ? `${v}` : v)) : ""}|${valueTypeKey(f.valueType)}`,
      )
      .join(";");
    const fixed: number[] = [];
    const variable: number[] = [];
    fields.forEach((f, i) => (isFixedSize(f.valueType) ? fixed : variable).push(i));
    let fixedSize = 0;
    let variableCount = 0;
    for (const logical of [...fixed, ...variable]) {
      this.logicalByPhysical.push(logical);
      const width = fixedSizeOf(fields[logical].valueType);
      if (width !== undefined) {
        this.layoutFields[logical] = { static: true, offset: fixedSize, width };
        fixedSize += width;
      } else {
        this.layoutFields[logical] = { static: false, variableIdx: variableCount++ };
      }
    }
    this.fixedSize = fixedSize;
    this.variableCount = variableCount;
  }

  static new(fields: Iterable<[string, ValueType]>): RecordDescriptor {
    return RecordDescriptor.fromLogicalFields(
      [...fields].map(([name, valueType]) => descriptorField(name, valueType)),
    );
  }
  static newWithFields(fields: Iterable<DescriptorField>): RecordDescriptor {
    return RecordDescriptor.fromLogicalFields([...fields]);
  }
  static empty(): RecordDescriptor {
    return RecordDescriptor.fromLogicalFields([]);
  }
  static fromLogicalFields(fields: DescriptorField[]): RecordDescriptor {
    const withIdentity = fields.map((f) =>
      f.identity === null && f.name !== null
        ? { ...f, identity: { kind: "Name" as const, name: f.name } }
        : f,
    );
    for (const f of withIdentity) {
      try {
        validateSchemaValueType(f.valueType);
      } catch (err) {
        throw new Error(
          `invalid record field ${JSON.stringify(f.name)}: ${(err as Error).message}`,
        );
      }
    }
    return descriptorInterner.intern(new RecordDescriptor(withIdentity));
  }

  private get offsetTableSize() {
    return Math.max(this.variableCount - 1, 0) * 4;
  }

  registryCompatibleWith(other: RecordDescriptor): boolean {
    return (
      this.fields.length === other.fields.length &&
      this.fields.every(
        (a, i) =>
          a.name === other.fields[i].name &&
          registryCompatibleWith(a.valueType, other.fields[i].valueType),
      )
    );
  }
  canEvolveRegistryTo(next: RecordDescriptor): boolean {
    return (
      this.fields.length === next.fields.length &&
      this.fields.every(
        (a, i) =>
          a.name === next.fields[i].name &&
          canEvolveRegistryTo(a.valueType, next.fields[i].valueType),
      )
    );
  }
  fieldIndex(name: string): number | undefined {
    const i = this.fields.findIndex((f) => logicalName(f) === name);
    return i < 0 ? undefined : i;
  }
  fieldIndexByIdentity(identity: FieldIdentity): number | undefined {
    const i = this.fields.findIndex((f) => identityEq(f.identity, identity));
    return i < 0 ? undefined : i;
  }
  field(index: number): DescriptorField {
    const f = this.fields[index];
    if (!f) throw RecordError.oob(index, this.fields.length);
    return f;
  }

  create(values: Value[]): Bytes {
    if (this.fields.length !== values.length)
      throw RecordError.arity(this.fields.length, values.length);
    return this.createWithEncodedFields((index, out) =>
      this.encodeFieldInto(index, values[index], out),
    );
  }

  static project(
    sources: RecordDescriptor[],
    records: Bytes[],
    mapping: [number, number][],
  ): [RecordDescriptor, Bytes] {
    const [fields, values] = projectFieldsAndValues(sources, records, mapping);
    const d = RecordDescriptor.fromLogicalFields(fields);
    return [d, d.create(values)];
  }
  projectRecord(sources: RecordDescriptor[], records: Bytes[], mapping: [number, number][]): Bytes {
    return this.create(projectFieldsAndValues(sources, records, mapping)[1]);
  }

  projectRecordRaw(
    sources: RecordDescriptor[],
    records: Bytes[],
    mapping: [number, number][],
  ): Bytes {
    if (this.fields.length !== mapping.length)
      throw RecordError.arity(this.fields.length, mapping.length);
    if (sources.length !== records.length) throw RecordError.arity(sources.length, records.length);
    const out = new ByteBuf();
    const variable: Bytes[] = [];
    for (const logical of this.logicalByPhysical) {
      const [di, fi] = mapping[logical];
      const sd = sources[di];
      if (!sd) throw RecordError.oob(di, sources.length);
      const rec = records[di];
      const sf = sd.field(fi);
      const of = this.fields[logical];
      if (!valueTypeEq(sf.valueType, of.valueType)) throw RecordError.typeMismatch(of.valueType);
      const span = sd.fieldSpan(rec, fi);
      const encoded = rec.subarray(span.start, span.end);
      const layout = this.layoutFields[logical];
      if (layout.static) {
        if (encoded.length !== layout.width) throw RecordError.invalidOffset();
        out.extend(encoded);
      } else variable.push(encoded);
    }
    let next = this.fixedSize + this.offsetTableSize;
    for (const enc of variable.slice(0, Math.max(variable.length - 1, 0))) {
      next = checkedAdd(next, enc.length);
      writeU32(out, usizeToU32(next));
    }
    for (const enc of variable) out.extend(enc);
    return out.finish();
  }

  bind(raw: Bytes): BorrowedRecord {
    return new BorrowedRecord(raw, this);
  }
  bindOwned(raw: Bytes): OwnedRecord {
    return new OwnedRecord(raw, this);
  }
  get(record: Bytes, name: string): Value {
    const idx = this.fieldIndex(name);
    if (idx === undefined) throw RecordError.fieldNotFound(name);
    return this.getIdx(record, idx);
  }
  getIdx(record: Bytes, idx: number): Value {
    const f = this.field(idx);
    const span = this.fieldSpan(record, idx);
    return decodeValue(record.subarray(span.start, span.end), f.valueType);
  }
  fieldSpan(record: Bytes, idx: number): Span {
    const layout = this.layoutFields[idx];
    if (!layout) throw RecordError.oob(idx, this.fields.length);
    validateRecordHeader(record, this);
    return spanForLayout(record, this, layout);
  }

  fieldsContainIndirectValues(record: Bytes, indices: Iterable<number>): boolean {
    return this.visitEncodedIndirectFields(record, indices, () => true);
  }
  visitLargeValueRefs(record: Bytes, visitor: (r: LargeValueRef) => boolean): boolean {
    return this.visitEncodedIndirectFields(record, null, (bytes, vt) => {
      const v = decodeValue(bytes, vt);
      if (v.kind !== "Large")
        throw new Error("indirect traversal visits only the indirect scalar arm");
      return visitor(v.v);
    });
  }
  visitEncodedIndirectFields(
    record: Bytes,
    indices: Iterable<number> | null,
    visitor: (bytes: Bytes, vt: ValueType) => boolean,
  ): boolean {
    for (const index of indices ?? this.fields.keys()) {
      const f = this.field(index);
      if (mayContainStoredScalar(f.valueType)) {
        const span = this.fieldSpan(record, index);
        if (visitEncodedIndirectValues(record.subarray(span.start, span.end), f.valueType, visitor))
          return true;
      }
    }
    return false;
  }

  /** Assemble a record; `append` emits each field's encoding, offsets are owned here. */
  createWithEncodedFields(append: (index: number, out: ByteBuf) => void): Bytes {
    const out = new ByteBuf();
    this.writeProjectedFieldsInto(out, append);
    return out.finish();
  }

  encodeFieldInto(index: number, value: Value, out: ByteBuf): void {
    const f = this.field(index);
    ensureValueType(value, f.valueType);
    if (isFixedSize(f.valueType)) return encodeFixedValue(out, value, f.valueType);
    const start = out.len;
    try {
      encodeValueInto(out, value, f.valueType);
    } catch (e) {
      out.truncate(start);
      throw e;
    }
  }

  patchField(record: Bytes, idx: number, value: Value): Bytes {
    const f = this.field(idx);
    ensureValueType(value, f.valueType);
    const span = this.fieldSpan(record, idx);
    const layout = this.layoutFields[idx];
    if (layout.static && span.end - span.start === layout.width) {
      const patched = record.slice();
      const enc = new ByteBuf();
      encodeFixedValue(enc, value, f.valueType);
      patched.set(enc.view(), span.start);
      return patched;
    }
    const values = this.bind(record).toValues();
    values[idx] = value;
    return this.create(values);
  }

  /** Shared framing for ordinary and composed projections. Returns the output span. */
  writeProjectedFieldsInto(out: ByteBuf, append: (targetIdx: number, out: ByteBuf) => void): Span {
    const start = out.len;
    for (const t of this.logicalByPhysical) if (this.layoutFields[t].static) append(t, out);
    out.resize(out.len + this.offsetTableSize);
    for (const t of this.logicalByPhysical) {
      const layout = this.layoutFields[t];
      if (layout.static) continue;
      append(t, out);
      if (layout.variableIdx + 1 < this.variableCount)
        out.setU32(start + this.fixedSize + layout.variableIdx * 4, usizeToU32(out.len - start));
    }
    return { start, end: out.len };
  }

  projectRawFieldsInto(
    source: RecordDescriptor,
    sourceRecord: Bytes,
    fields: RawProjectionField[],
    out: ByteBuf,
    evaluate: (targetIdx: number, out: ByteBuf) => void,
  ): Span {
    return this.writeProjectedFieldsInto(out, (t, o) =>
      appendProjectedField(source, sourceRecord, fields[t], t, o, evaluate),
    );
  }

  /** Copy `source` unwrapping one nullable field; returns null when it is null and the target is not nullable. */
  unwrapNullableFieldInto(
    source: RecordDescriptor,
    sourceRecord: Bytes,
    fieldIdx: number,
    out: ByteBuf,
  ): Span | null {
    if (this.fields.length !== source.fields.length)
      throw RecordError.arity(this.fields.length, source.fields.length);
    const sf = source.field(fieldIdx);
    const tf = this.field(fieldIdx);
    let unwrapInner: ValueType | null = null;
    if (typeof sf.valueType !== "string" && sf.valueType.kind === "Nullable") {
      if (!valueTypeEq(sf.valueType.inner, tf.valueType))
        throw RecordError.typeMismatch(tf.valueType);
      unwrapInner = sf.valueType.inner;
    } else if (!valueTypeEq(sf.valueType, tf.valueType))
      throw RecordError.typeMismatch(tf.valueType);

    const start = out.len;
    const variable: Bytes[] = [];
    for (const t of this.logicalByPhysical) {
      let bytes: Bytes;
      if (t === fieldIdx) {
        const span = source.fieldSpan(sourceRecord, fieldIdx);
        if (unwrapInner !== null) {
          const payload = nullablePresentPayload(sourceRecord, span, unwrapInner);
          if (payload) bytes = sourceRecord.subarray(payload.start, payload.end);
          else if (typeof tf.valueType !== "string" && tf.valueType.kind === "Nullable")
            bytes = encodeValue({ kind: "Nullable", v: null }, tf.valueType);
          else {
            out.truncate(start);
            return null;
          }
        } else bytes = sourceRecord.subarray(span.start, span.end);
      } else {
        const s = source.field(t);
        const tt = this.field(t);
        if (!valueTypeEq(s.valueType, tt.valueType)) throw RecordError.typeMismatch(tt.valueType);
        const span = source.fieldSpan(sourceRecord, t);
        bytes = sourceRecord.subarray(span.start, span.end);
      }
      if (this.layoutFields[t].static) out.extend(bytes);
      else variable.push(bytes);
    }
    let next = this.fixedSize + this.offsetTableSize;
    for (const b of variable.slice(0, Math.max(variable.length - 1, 0))) {
      next = checkedAdd(next, b.length);
      out.extend(le(4, usizeToU32(next)));
    }
    for (const b of variable) out.extend(b);
    return { start, end: out.len };
  }
}

function fixedSizeOf(vt: ValueType): number | undefined {
  return fixedSize(vt);
}

export type RawProjectionField =
  | { kind: "Copy"; sourceIdx: number }
  | { kind: "WrapNullable"; sourceIdx: number }
  | { kind: "Nested"; path: [RecordDescriptor, number][] }
  | { kind: "Error"; error: RecordError }
  | { kind: "Evaluate" }
  | { kind: "Encoded"; bytes: Bytes };

function appendProjectedField(
  source: RecordDescriptor,
  record: Bytes,
  field: RawProjectionField,
  targetIdx: number,
  out: ByteBuf,
  evaluate: (targetIdx: number, out: ByteBuf) => void,
): void {
  const copy = (idx: number) => {
    const s = source.fieldSpan(record, idx);
    out.extend(record.subarray(s.start, s.end));
  };
  switch (field.kind) {
    case "Copy":
      return copy(field.sourceIdx);
    case "WrapNullable":
      out.push(1);
      return copy(field.sourceIdx);
    case "Nested": {
      let bytes = record;
      for (const [d, index] of field.path) {
        const s = d.fieldSpan(bytes, index);
        bytes = bytes.subarray(s.start, s.end);
      }
      return out.extend(bytes);
    }
    case "Encoded":
      return out.extend(field.bytes);
    case "Evaluate":
      return evaluate(targetIdx, out);
    case "Error":
      throw field.error;
  }
}

export class PreparedProjection {
  readonly reusesInput: boolean;
  constructor(
    source: RecordDescriptor,
    target: RecordDescriptor,
    readonly fields: RawProjectionField[],
  ) {
    this.reusesInput =
      source.fields.length === target.fields.length &&
      fields.length === target.fields.length &&
      source.fixedSize === target.fixedSize &&
      source.variableCount === target.variableCount &&
      fields.every((f, t) => {
        if (f.kind !== "Copy") return false;
        const sf = source.fields[f.sourceIdx];
        return (
          sf !== undefined &&
          valueTypeEq(sf.valueType, target.fields[t].valueType) &&
          layoutEq(source.layoutFields[f.sourceIdx], target.layoutFields[t])
        );
      });
  }
}

/** Multi-input byte-copy kernel with descriptor/type work compiled once. */
export class PreparedRecordCopy {
  private constructor(
    private readonly sources: [RecordDescriptor, boolean][],
    private readonly target: RecordDescriptor,
    private readonly fields: [number, FieldLayout][],
  ) {}
  static new(sources: RecordDescriptor[], target: RecordDescriptor, mapping: [number, number][]) {
    if (target.fields.length !== mapping.length)
      throw RecordError.arity(target.fields.length, mapping.length);
    const srcs: [RecordDescriptor, boolean][] = sources.map((s) => [s, false]);
    const fields: [number, FieldLayout][] = mapping.map(([si, fi], t) => {
      const entry = srcs[si];
      if (!entry) throw RecordError.oob(si, srcs.length);
      const f = entry[0].field(fi);
      if (!valueTypeEq(f.valueType, target.fields[t].valueType))
        throw RecordError.typeMismatch(target.fields[t].valueType);
      entry[1] = true;
      return [si, entry[0].layoutFields[fi]];
    });
    return new PreparedRecordCopy(srcs, target, fields);
  }
  projectInto(records: Bytes[], out: ByteBuf): Span {
    if (records.length !== this.sources.length)
      throw RecordError.arity(this.sources.length, records.length);
    this.sources.forEach(([d, used], i) => used && validateRecordHeader(records[i], d));
    const start = out.len;
    try {
      return this.target.writeProjectedFieldsInto(out, (t, o) => {
        const [si, layout] = this.fields[t];
        const rec = records[si];
        const span = spanForLayout(rec, this.sources[si][0], layout);
        o.extend(rec.subarray(span.start, span.end));
      });
    } catch (e) {
      out.truncate(start);
      throw e;
    }
  }
}

/** Descriptor-validated encoded projection: copies spans, never decodes values. */
export class RecordProjector {
  private constructor(
    readonly source: RecordDescriptor,
    readonly target: RecordDescriptor,
    private readonly targetToSource: number[],
  ) {}
  static new(
    source: RecordDescriptor,
    target: RecordDescriptor,
    mapping: Iterable<[number, number]>,
  ) {
    return RecordProjector.newInner(source, target, mapping, false);
  }
  static newRegistryRebound(
    source: RecordDescriptor,
    target: RecordDescriptor,
    mapping: Iterable<[number, number]>,
  ) {
    return RecordProjector.newInner(source, target, mapping, true);
  }
  private static newInner(
    source: RecordDescriptor,
    target: RecordDescriptor,
    mapping: Iterable<[number, number]>,
    allowRebinding: boolean,
  ): RecordProjector {
    const t2s: (number | undefined)[] = new Array(target.fields.length).fill(undefined);
    for (const [si, ti] of mapping) {
      const sf = source.field(si);
      const tf = target.field(ti);
      if (t2s[ti] !== undefined)
        throw new RecordError(
          "ProjectDuplicateTarget",
          `projection target field ${ti} was mapped more than once`,
        );
      if (
        !valueTypeEq(sf.valueType, tf.valueType) &&
        !(allowRebinding && registryReboundLayoutCompatibleWith(sf.valueType, tf.valueType))
      )
        throw new RecordError(
          "ProjectTypeMismatch",
          `projection source field ${si} type ${formatValueType(sf.valueType)} does not match target field ${ti} type ${formatValueType(tf.valueType)}`,
        );
      t2s[ti] = si;
    }
    const resolved = t2s.map((s, ti) => {
      if (s === undefined)
        throw new RecordError(
          "ProjectMissingTarget",
          `projection target field ${ti} has no source field`,
        );
      return s;
    });
    return new RecordProjector(source, target, resolved);
  }

  project(record: BorrowedRecord): OwnedRecord {
    if (record.descriptor !== this.source)
      throw new RecordError(
        "ProjectSourceDescriptorMismatch",
        "projection source record descriptor does not match projector source descriptor",
      );
    const raw = this.target.createWithEncodedFields((t, out) => {
      const span = this.source.fieldSpan(record.raw, this.targetToSource[t]);
      out.extend(record.raw.subarray(span.start, span.end));
    });
    return new OwnedRecord(raw, this.target);
  }
}

function nullablePresentPayload(record: Bytes, span: Span, inner: ValueType): Span | null {
  const bytes = record.subarray(span.start, span.end);
  if (!bytes.length) throw RecordError.eof();
  if (bytes[0] === 0) {
    checkNullPayload(bytes.subarray(1), inner);
    return null;
  }
  if (bytes[0] === 1) return { start: span.start + 1, end: span.end };
  throw RecordError.invalidNullFlag(bytes[0]);
}

function projectFieldsAndValues(
  sources: RecordDescriptor[],
  records: Bytes[],
  mapping: [number, number][],
): [DescriptorField[], Value[]] {
  if (sources.length !== records.length) throw RecordError.arity(sources.length, records.length);
  const fields: DescriptorField[] = [];
  const values: Value[] = [];
  for (const [di, fi] of mapping) {
    const d = sources[di];
    if (!d) throw RecordError.oob(di, sources.length);
    fields.push(d.field(fi));
    values.push(d.getIdx(records[di], fi));
  }
  return [[...RecordDescriptor.fromLogicalFields(fields).fields], values];
}

function validateRecordHeader(record: Bytes, d: RecordDescriptor): void {
  const variableStart = checkedAdd(d.fixedSize, Math.max(d.variableCount - 1, 0) * 4);
  if (record.length < variableStart) throw RecordError.eof();
  if (d.variableCount === 0 && record.length !== d.fixedSize) throw RecordError.invalidOffset();
}

function spanForLayout(record: Bytes, d: RecordDescriptor, layout: FieldLayout): Span {
  if (layout.static) {
    const end = checkedAdd(layout.offset, layout.width);
    if (record.length < end) throw RecordError.eof();
    return { start: layout.offset, end };
  }
  const variableStart = checkedAdd(d.fixedSize, Math.max(d.variableCount - 1, 0) * 4);
  const vi = layout.variableIdx;
  const start = vi === 0 ? variableStart : readU32At(record, d.fixedSize + (vi - 1) * 4);
  const end = vi + 1 === d.variableCount ? record.length : readU32At(record, d.fixedSize + vi * 4);
  if (start < variableStart || end < start || end > record.length)
    throw RecordError.invalidOffset();
  return { start, end };
}

function readExactArray(bytes: Bytes, n: number): Bytes {
  if (bytes.length < n) throw RecordError.eof();
  if (bytes.length > n) throw RecordError.invalidOffset();
  return bytes;
}
const dv = (b: Bytes) => new DataView(b.buffer, b.byteOffset, b.byteLength);
function inlineScalar(kind: "Bytes" | "String", bytes: Bytes): Bytes {
  try {
    return inlineScalarBytes(kind, bytes);
  } catch (e) {
    if (e instanceof LargeValueError) throw RecordError.largeValue(e);
    throw e;
  }
}
function utf8Str(b: Bytes): string {
  try {
    return fromUtf8(b);
  } catch {
    throw RecordError.utf8();
  }
}

/** Zero-copy view over an encoded record; accessors decode one field. */
export class BorrowedRecord {
  constructor(
    readonly raw: Bytes,
    readonly descriptor: RecordDescriptor,
  ) {}
  get bytes(): Bytes {
    return this.raw;
  }
  get(name: string): Value {
    return this.descriptor.get(this.raw, name);
  }
  getIdx(idx: number): Value {
    return this.descriptor.getIdx(this.raw, idx);
  }
  getRecord(idx: number): BorrowedRecord {
    const f = this.descriptor.field(idx);
    if (typeof f.valueType === "string" || f.valueType.kind !== "Record")
      throw RecordError.typeMismatch(f.valueType);
    const span = this.descriptor.fieldSpan(this.raw, idx);
    return new BorrowedRecord(this.raw.subarray(span.start, span.end), f.valueType.descriptor);
  }
  toValues(): Value[] {
    validateRecordHeader(this.raw, this.descriptor);
    return this.descriptor.fields.map((f, i) => {
      const s = spanForLayout(this.raw, this.descriptor, this.descriptor.layoutFields[i]);
      return decodeValue(this.raw.subarray(s.start, s.end), f.valueType);
    });
  }
  validate(): void {
    this.validateInner(false);
  }
  validateCanonical(): void {
    this.validateInner(true);
  }
  private validateInner(canonical: boolean): void {
    validateRecordHeader(this.raw, this.descriptor);
    this.descriptor.fields.forEach((f, i) => {
      const s = spanForLayout(this.raw, this.descriptor, this.descriptor.layoutFields[i]);
      const bytes = this.raw.subarray(s.start, s.end);
      (canonical ? validateCanonicalValue : validateValue)(bytes, f.valueType);
    });
  }

  private fieldBytes(idx: number, expected: ValueType): Bytes {
    const f = this.descriptor.field(idx);
    if (!valueTypeEq(f.valueType, expected)) throw RecordError.typeMismatch(expected);
    return this.fieldBytesUnchecked(idx);
  }
  fieldBytesUnchecked(idx: number): Bytes {
    this.descriptor.field(idx);
    const s = this.descriptor.fieldSpan(this.raw, idx);
    return this.raw.subarray(s.start, s.end);
  }
  field(idx: number): DescriptorField {
    return this.descriptor.field(idx);
  }
  private nullableField<T>(idx: number, inner: ValueType, decode: (p: Bytes) => T): T | null {
    const f = this.descriptor.field(idx);
    if (
      typeof f.valueType === "string" ||
      f.valueType.kind !== "Nullable" ||
      !valueTypeEq(f.valueType.inner, inner)
    )
      throw RecordError.typeMismatch({ kind: "Nullable", inner });
    const bytes = this.fieldBytesUnchecked(idx);
    if (!bytes.length) throw RecordError.eof();
    const [flag, payload] = [bytes[0], bytes.subarray(1)];
    if (flag === 0) {
      if (this.descriptor.layoutFields[idx].static) {
        if (payload.some((b) => b !== 0)) throw RecordError.invalidOffset();
      } else if (payload.length) throw RecordError.invalidOffset();
      return null;
    }
    if (flag === 1) return decode(payload);
    throw RecordError.invalidNullFlag(flag);
  }

  getU64(idx: number): bigint {
    return dv(readExactArray(this.fieldBytes(idx, "U64"), 8)).getBigUint64(0, true);
  }
  getI64(idx: number): bigint {
    return dv(readExactArray(this.fieldBytes(idx, "I64"), 8)).getBigInt64(0, true);
  }
  getI32(idx: number): number {
    return dv(readExactArray(this.fieldBytes(idx, "I32"), 4)).getInt32(0, true);
  }
  getU32(idx: number): number {
    return dv(readExactArray(this.fieldBytes(idx, "U32"), 4)).getUint32(0, true);
  }
  getF64(idx: number): number {
    return readF64(this.fieldBytes(idx, "F64"));
  }
  getU48(idx: number): number {
    return u48FromLeBytes(readExactArray(this.fieldBytes(idx, "U48"), 6));
  }
  getU8(idx: number): number {
    const b = this.fieldBytes(idx, "U8");
    if (!b.length) throw RecordError.eof();
    return b[0];
  }
  getBool(idx: number): boolean {
    const b = this.fieldBytes(idx, "Bool");
    if (!b.length) throw RecordError.eof();
    if (b[0] > 1) throw RecordError.invalidBool(b[0]);
    return b[0] === 1;
  }
  getEnum(idx: number): number {
    const f = this.descriptor.field(idx);
    if (typeof f.valueType === "string" || f.valueType.kind !== "EnumTag")
      throw RecordError.typeMismatch("U8");
    return readExactArray(this.fieldBytesUnchecked(idx), 1)[0];
  }
  getEnumName(idx: number): string {
    const f = this.descriptor.field(idx);
    if (typeof f.valueType === "string" || f.valueType.kind !== "EnumTag")
      throw RecordError.typeMismatch("U8");
    return f.valueType.schema.variant(this.getEnum(idx));
  }
  getBytes(idx: number): Bytes {
    return inlineScalar("Bytes", this.fieldBytes(idx, "Bytes"));
  }
  getUuid(idx: number): Bytes {
    return readExactArray(this.fieldBytes(idx, "Uuid"), 16);
  }
  getStr(idx: number): string {
    return utf8Str(inlineScalar("String", this.fieldBytes(idx, "String")));
  }
  getNullableU48(idx: number) {
    return this.nullableField(idx, "U48", (p) => u48FromLeBytes(readExactArray(p, 6)));
  }
  getNullableU64(idx: number) {
    return this.nullableField(idx, "U64", (p) => dv(readExactArray(p, 8)).getBigUint64(0, true));
  }
  getNullableI64(idx: number) {
    return this.nullableField(idx, "I64", (p) => dv(readExactArray(p, 8)).getBigInt64(0, true));
  }
  getNullableI32(idx: number) {
    return this.nullableField(idx, "I32", (p) => dv(readExactArray(p, 4)).getInt32(0, true));
  }
  getNullableF64(idx: number) {
    return this.nullableField(idx, "F64", readF64);
  }
  getNullableString(idx: number) {
    return this.nullableField(idx, "String", (p) => utf8Str(inlineScalar("String", p)));
  }
  getNullableBytes(idx: number) {
    return this.nullableField(idx, "Bytes", (p) => inlineScalar("Bytes", p));
  }
  getNullableUuid(idx: number) {
    return this.nullableField(idx, "Uuid", (p) => readExactArray(p, 16));
  }
  getNullableEnum(idx: number): number | null {
    const f = this.descriptor.field(idx);
    const vt = f.valueType;
    if (
      typeof vt === "string" ||
      vt.kind !== "Nullable" ||
      typeof vt.inner === "string" ||
      vt.inner.kind !== "EnumTag"
    )
      throw RecordError.typeMismatch({ kind: "Nullable", inner: "U8" });
    const bytes = this.fieldBytesUnchecked(idx);
    if (!bytes.length) throw RecordError.eof();
    if (bytes[0] === 0) {
      if (bytes.subarray(1).some((b) => b !== 0)) throw RecordError.invalidOffset();
      return null;
    }
    if (bytes[0] === 1) return readExactArray(bytes.subarray(1), 1)[0];
    throw RecordError.invalidNullFlag(bytes[0]);
  }
  getArrayElement(idx: number, elementIdx: number): Value {
    const f = this.descriptor.field(idx);
    if (typeof f.valueType === "string" || f.valueType.kind !== "Array")
      throw RecordError.typeMismatch({ kind: "Array", inner: "Bytes" });
    const elem = f.valueType.inner;
    const size = fixedSize(elem);
    if (size === undefined) throw RecordError.invalidTupleMember(elem);
    const array = this.fieldBytesUnchecked(idx);
    if (array.length % size !== 0) throw RecordError.eof();
    const len = array.length / size;
    if (elementIdx >= len) throw RecordError.oob(elementIdx, len);
    const start = elementIdx * size;
    return decodeValue(array.subarray(start, start + size), elem);
  }
}

function readF64(b: Bytes): number {
  const v = dv(readExactArray(b, 8)).getFloat64(0, true);
  if (Number.isNaN(v)) throw RecordError.nan();
  return v;
}

/** Owned encoded record tied to its descriptor (Rust Record<'a> and OwnedRecord). */
export class OwnedRecord {
  constructor(
    readonly raw: Bytes,
    readonly descriptor: RecordDescriptor,
  ) {}
  borrowed(): BorrowedRecord {
    return new BorrowedRecord(this.raw, this.descriptor);
  }
  get(name: string): Value {
    return this.borrowed().get(name);
  }
  getIdx(idx: number): Value {
    return this.borrowed().getIdx(idx);
  }
  toValues(): Value[] {
    return this.borrowed().toValues();
  }
  validate(): void {
    this.borrowed().validate();
  }
}

/** An encoded row bound to its table-local variant case. */
export class VariantRecord {
  constructor(
    readonly variantTag: number,
    readonly record: OwnedRecord,
  ) {}
  static create(tag: number, descriptor: RecordDescriptor, values: Value[]): VariantRecord {
    return new VariantRecord(tag, new OwnedRecord(descriptor.create(values), descriptor));
  }
  get descriptor(): RecordDescriptor {
    return this.record.descriptor;
  }
  get raw(): Bytes {
    return this.record.raw;
  }
  borrowed(): BorrowedRecord {
    return this.record.borrowed();
  }
  get(name: string): Value {
    return this.record.get(name);
  }
  getIdx(idx: number): Value {
    return this.record.getIdx(idx);
  }
  toValues(): Value[] {
    return this.record.toValues();
  }
  intoStoredBytes(): Bytes {
    return encodeVariantRecord(this.variantTag, this.record.raw);
  }
}

/** A variant record whose bytes were produced by its descriptor (no re-validation needed). */
export class ValidatedVariantRecord extends VariantRecord {
  static createWithEncodedFields(
    tag: number,
    descriptor: RecordDescriptor,
    append: (index: number, out: ByteBuf) => void,
  ): ValidatedVariantRecord {
    const raw = descriptor.createWithEncodedFields(append);
    return new ValidatedVariantRecord(tag, new OwnedRecord(raw, descriptor));
  }
  static override create(tag: number, descriptor: RecordDescriptor, values: Value[]) {
    return new ValidatedVariantRecord(tag, new OwnedRecord(descriptor.create(values), descriptor));
  }
}

export function encodeVariantRecord(tag: number, payload: Bytes): Bytes {
  const out = new ByteBuf();
  appendVariantRecord(out, tag, payload);
  return out.finish();
}
export function appendVariantRecord(out: ByteBuf, tag: number, payload: Bytes): void {
  let value = tag >>> 0;
  while (value >= 0x80) {
    out.push((value & 0x7f) | 0x80);
    value >>>= 7;
  }
  out.push(value);
  out.extend(payload);
}
export function splitVariantRecord(stored: Bytes): [number, Bytes] {
  let value = 0;
  for (let index = 0; index < MAX_VARIANT_TAG_LEN; index++) {
    const byte = stored[index];
    if (byte === undefined) throw RecordError.schemaHeader();
    const payload = byte & 0x7f;
    if (index === MAX_VARIANT_TAG_LEN - 1 && payload > 0x0f) throw RecordError.schemaHeader();
    value += payload * 2 ** (index * 7);
    if ((byte & 0x80) === 0) {
      if (index > 0 && payload === 0) throw RecordError.schemaHeader();
      return [value, stored.subarray(index + 1)];
    }
  }
  throw RecordError.schemaHeader();
}
