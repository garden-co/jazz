import type { ColumnDescriptor, ColumnType, Value, WasmRow } from "../../drivers/types.js";
import { isProvenanceMagicTimestampColumn } from "../../magic-columns.js";

const textDecoder = new TextDecoder();

export type ValueType = { tag: number; inner?: ValueType; members?: ValueType[] };
export type DescriptorField = { name?: string; valueType: ValueType };
export type NativeRow = { rowId: Uint8Array; deleted: boolean; raw: Uint8Array };
export type NativeRowBatch = { table: string; descriptor: DescriptorField[]; rows: NativeRow[] };
export type NativeRemovedRow = { table: string; rowId: Uint8Array };
export type NativeSubscriptionDelta = {
  added: NativeRowBatch[];
  updated: NativeRowBatch[];
  removed: NativeRemovedRow[];
};
export type NativeRelationSubscriptionEdge = {
  sourceTable: string;
  sourceRowId: Uint8Array;
  relation: string;
  targetTable: string;
  targetRowId: Uint8Array;
};
export type NativeRelationSubscriptionSnapshot = {
  cursor: number;
  rootCount: number;
  rows: NativeRowBatch[];
  edges: NativeRelationSubscriptionEdge[];
};
export type NativeRelationSubscriptionDelta = {
  baseCursor?: number;
  cursor: number;
  added: NativeRowBatch[];
  updated: NativeRowBatch[];
  removed: NativeRemovedRow[];
  addedEdges: NativeRelationSubscriptionEdge[];
  removedEdges: NativeRelationSubscriptionEdge[];
};

type PostcardReaderLike = {
  string(): string;
  u64(): number;
  option<T>(readValue: (reader: PostcardReaderLike) => T): T | undefined;
  bytes(): Uint8Array;
  bool(): boolean;
  readVec<T>(readItem: (reader: PostcardReaderLike) => T): T[];
};

type PostcardWriterLike = {
  vec(writeItem: (writer: PostcardWriterLike, index: number) => void, length: number): void;
  some(writeValue: (writer: PostcardWriterLike) => void): void;
  string(value: string): void;
  enumUnit(tag: number): void;
  bytes(value: Uint8Array): void;
  u32Le(value: number): void;
  finish(): Uint8Array;
};

export function readNativeRowBatch(reader: PostcardReaderLike): NativeRowBatch {
  return {
    table: reader.string(),
    descriptor: readDescriptor(reader),
    rows: reader.readVec((rowReader) => ({
      rowId: rowReader.bytes(),
      deleted: rowReader.bool(),
      raw: rowReader.bytes(),
    })),
  };
}

export function readNativeSubscriptionDelta(reader: PostcardReaderLike): NativeSubscriptionDelta {
  return {
    added: reader.readVec(readNativeRowBatch),
    updated: reader.readVec(readNativeRowBatch),
    removed: reader.readVec(readNativeRemovedRow),
  };
}

export function readNativeRelationSubscriptionSnapshot(
  reader: PostcardReaderLike,
): NativeRelationSubscriptionSnapshot {
  return {
    cursor: reader.u64(),
    rootCount: reader.u64(),
    rows: reader.readVec(readNativeRowBatch),
    edges: reader.readVec(readNativeRelationSubscriptionEdge),
  };
}

export function readNativeRelationSubscriptionDelta(
  reader: PostcardReaderLike,
): NativeRelationSubscriptionDelta {
  return {
    baseCursor: reader.option((value) => value.u64()),
    cursor: reader.u64(),
    added: reader.readVec(readNativeRowBatch),
    updated: reader.readVec(readNativeRowBatch),
    removed: reader.readVec(readNativeRemovedRow),
    addedEdges: reader.readVec(readNativeRelationSubscriptionEdge),
    removedEdges: reader.readVec(readNativeRelationSubscriptionEdge),
  };
}

export function readNativeRemovedRow(reader: PostcardReaderLike): NativeRemovedRow {
  return {
    table: reader.string(),
    rowId: reader.bytes(),
  };
}

export function readNativeRelationSubscriptionEdge(
  reader: PostcardReaderLike,
): NativeRelationSubscriptionEdge {
  return {
    sourceTable: reader.string(),
    sourceRowId: reader.bytes(),
    relation: reader.string(),
    targetTable: reader.string(),
    targetRowId: reader.bytes(),
  };
}

export function writeDescriptor(writer: PostcardWriterLike, descriptor: DescriptorField[]): void {
  writer.vec((field, index) => {
    field.some((nameWriter) => nameWriter.string(descriptor[index].name ?? ""));
    writeValueType(field, descriptor[index].valueType);
  }, descriptor.length);
}

export function readDescriptor(reader: PostcardReaderLike): DescriptorField[] {
  return reader.readVec((fieldReader) => ({
    name: fieldReader.option((nameReader) => nameReader.string()),
    valueType: readValueType(fieldReader),
  }));
}

export function writeValueType(writer: PostcardWriterLike, valueType: ValueType): void {
  writer.enumUnit(valueType.tag);
  if (valueType.tag === 10) {
    const members = valueType.members ?? (valueType.inner ? [valueType.inner] : []);
    writer.vec(
      (memberWriter, index) => writeValueType(memberWriter, members[index]),
      members.length,
    );
    return;
  }
  if (valueType.tag === 11 || valueType.tag === 12) {
    if (!valueType.inner) throw new Error(`missing inner value type for tag ${valueType.tag}`);
    writeValueType(writer, valueType.inner);
  }
}

export function readValueType(reader: PostcardReaderLike): ValueType {
  const tag = reader.u64();
  if (tag === 11 || tag === 12) {
    return { tag, inner: readValueType(reader) };
  }
  if (tag === 10) {
    const members = reader.readVec(readValueType);
    return { tag, members, inner: members[0] };
  }
  return { tag };
}

export function createRecord(descriptor: DescriptorField[], values: Uint8Array[]): Uint8Array {
  const layout = recordLayout(descriptor);
  const staticChunks = layout.fixed.map((field) => values[field.logicalIndex]);
  const variableChunks = layout.variable.map((field) => values[field.logicalIndex]);
  const fixed = concatBytes(staticChunks);
  const offsets = new OffsetWriter();
  let nextOffset = fixed.length + Math.max(0, variableChunks.length - 1) * 4;
  for (const chunk of variableChunks.slice(0, -1)) {
    nextOffset += chunk.length;
    offsets.u32Le(nextOffset);
  }
  return concatBytes([fixed, offsets.finish(), ...variableChunks]);
}

export function fieldIndex(descriptor: DescriptorField[], name: string): number {
  const index = descriptor.findIndex(
    (field) => field.name === name || field.name === `user_${name}`,
  );
  if (index < 0) {
    throw new Error(
      `missing ${name} field in [${descriptor.map((field) => field.name ?? "<anonymous>").join(", ")}]`,
    );
  }
  return index;
}

export function decodeRecordBool(
  descriptor: DescriptorField[],
  raw: Uint8Array,
  logicalIndex: number,
): boolean {
  const bytes = decodeRecordBytes(descriptor, raw, logicalIndex);
  if (bytes.length !== 1) throw new Error(`invalid bool size ${bytes.length}`);
  return bytes[0] !== 0;
}

export function decodeRecordString(
  descriptor: DescriptorField[],
  raw: Uint8Array,
  logicalIndex: number,
): string {
  return new TextDecoder().decode(decodeRecordBytes(descriptor, raw, logicalIndex));
}

export function decodeRecordBytes(
  descriptor: DescriptorField[],
  raw: Uint8Array,
  logicalIndex: number,
): Uint8Array {
  const value = decodeRecordValue(descriptor, raw, logicalIndex);
  if (value == null) return new Uint8Array();
  return value;
}

export function decodeNativeRowValues(
  columns: readonly ColumnDescriptor[],
  raw: Uint8Array,
): Value[] {
  const descriptor = descriptorFromColumns(columns);
  return columns.map((column, index) => {
    const bytes = decodeRecordValue(descriptor, raw, index);
    if (bytes == null) return { type: "Null" };
    return decodeBytes(column.column_type, bytes);
  });
}

export function decodeNativeRowValuesByColumn(
  columns: readonly ColumnDescriptor[],
  raw: Uint8Array,
): Map<string, Value> {
  const descriptor = descriptorFromColumns(columns);
  const valuesByColumn = new Map<string, Value>();

  for (let i = 0; i < columns.length; i++) {
    const column = columns[i];
    if (!column) continue;
    const bytes = decodeRecordValue(descriptor, raw, i);
    valuesByColumn.set(
      column.name,
      bytes == null ? { type: "Null" } : decodeBytes(column.column_type, bytes),
    );
  }

  return valuesByColumn;
}

export function decodeNativeRow(
  id: string,
  columns: readonly ColumnDescriptor[],
  raw: Uint8Array,
): WasmRow {
  const row = {
    id,
    values: decodeNativeRowValues(columns, raw),
  };
  Object.defineProperty(row, "valuesByColumn", {
    value: decodeNativeRowValuesByColumn(columns, raw),
    enumerable: false,
    configurable: true,
  });
  return row;
}

export function decodeNativeRowObject(
  id: string | undefined,
  columns: readonly ColumnDescriptor[],
  raw: Uint8Array,
): Record<string, unknown> {
  const descriptor = descriptorFromColumns(columns);
  const obj: Record<string, unknown> = {};
  if (id !== undefined) {
    obj.id = id;
  }

  for (let i = 0; i < columns.length; i++) {
    const column = columns[i];
    if (!column) continue;
    const bytes = decodeRecordValue(descriptor, raw, i);
    obj[column.name] =
      bytes == null ? null : decodePlainValue(column.column_type, bytes, column.name);
  }

  return obj;
}

export function decodeRecordValue(
  descriptor: DescriptorField[],
  raw: Uint8Array,
  logicalIndex: number,
): Uint8Array | null {
  const layout = recordLayout(descriptor);
  const target = layout.fields[logicalIndex];
  if (!target) throw new Error("field is not present");
  const valueType = descriptor[logicalIndex].valueType;
  if (target.kind === "fixed") {
    const end = target.offset + target.size;
    if (end > raw.length) throw new Error("unexpected end of record");
    const value = raw.subarray(target.offset, target.offset + target.size);
    return unwrapValue(value, valueType);
  }
  const offsetTableStart = layout.fixedSize;
  const variableStart = layout.fixedSize + Math.max(0, layout.variable.length - 1) * 4;
  const start =
    target.variableIndex === 0
      ? variableStart
      : readU32Le(raw, offsetTableStart + (target.variableIndex - 1) * 4);
  const end =
    target.variableIndex === layout.variable.length - 1
      ? raw.length
      : readU32Le(raw, offsetTableStart + target.variableIndex * 4);
  if (start > end || end > raw.length) throw new Error("invalid offset");
  const value = raw.subarray(start, end);
  return unwrapValue(value, valueType);
}

function unwrapValue(value: Uint8Array, valueType: ValueType): Uint8Array | null {
  if (valueType.tag !== 12) return value;
  const unwrapped = unwrapNullable(value);
  if (unwrapped == null) return null;
  return valueType.inner ? unwrapValue(unwrapped, valueType.inner) : unwrapped;
}

function unwrapNullable(value: Uint8Array): Uint8Array | null {
  if (value[0] === 0) return null;
  if (value[0] !== 1) return value;
  return value.subarray(1);
}

function descriptorFromColumns(columns: readonly ColumnDescriptor[]): DescriptorField[] {
  return columns.map((column) => ({
    name: column.name,
    valueType: columnValueType(column),
  }));
}

function columnValueType(column: ColumnDescriptor): ValueType {
  const valueType = columnTypeToValueType(column.column_type);
  return column.nullable ? { tag: 12, inner: valueType } : valueType;
}

function columnTypeToValueType(type: ColumnType): ValueType {
  switch (type.type) {
    case "Boolean":
      return { tag: 5 };
    case "Integer":
      return { tag: 2 };
    case "BigInt":
    case "Timestamp":
      return { tag: 3 };
    case "Double":
      return { tag: 4 };
    case "Text":
    case "Json":
    case "Enum":
      return { tag: 6 };
    case "Bytea":
      return { tag: 7 };
    case "Uuid":
      return { tag: 8 };
    case "Array":
      return { tag: 11, inner: columnTypeToValueType(type.element) };
    case "Row":
      throw new Error("Core runtime does not encode nested row columns yet");
  }
}

function decodeBytes(type: ColumnType, bytes: Uint8Array): Value {
  const view = new DataView(bytes.buffer, bytes.byteOffset, bytes.byteLength);
  switch (type.type) {
    case "Boolean":
      return { type: "Boolean", value: bytes[0] !== 0 };
    case "Integer":
      return { type: "Integer", value: decodeSignedI32FromCore(view.getUint32(0, true)) };
    case "BigInt":
      return { type: "BigInt", value: Number(view.getBigUint64(0, true)) };
    case "Double":
      return { type: "Double", value: view.getFloat64(0, true) };
    case "Timestamp":
      return { type: "Timestamp", value: Number(view.getBigUint64(0, true)) };
    case "Text":
    case "Json":
    case "Enum":
      return { type: "Text", value: textDecoder.decode(bytes) };
    case "Uuid":
      return { type: "Uuid", value: formatUuid(bytes) };
    case "Bytea":
      return { type: "Bytea", value: bytes.slice() };
    case "Array":
      return { type: "Array", value: decodeArray(type.element, bytes) };
    case "Row":
      return { type: "Bytea", value: bytes.slice() };
  }
}

function decodePlainValue(type: ColumnType, bytes: Uint8Array, columnName?: string): unknown {
  const value = decodeBytes(type, bytes);
  switch (type.type) {
    case "Timestamp":
      return value.type === "Timestamp" ? timestampToDate(value.value, columnName) : null;
    case "Json":
      return value.type === "Text" ? JSON.parse(value.value) : null;
    case "Array":
      return decodePlainArray(type.element, bytes);
    case "Text":
    case "Enum":
    case "Bytea":
    case "Uuid":
    case "Boolean":
    case "Integer":
    case "BigInt":
    case "Double":
      return "value" in value ? value.value : null;
    case "Row":
      return "value" in value ? value.value : null;
  }
}

function decodePlainArray(elementType: ColumnType, bytes: Uint8Array): unknown[] {
  return decodeArrayElements(elementType, bytes, (element) =>
    decodePlainValue(elementType, element),
  );
}

function decodeArray(elementType: ColumnType, bytes: Uint8Array): Value[] {
  return decodeArrayElements(elementType, bytes, (element) => decodeBytes(elementType, element));
}

function decodeArrayElements<T>(
  elementType: ColumnType,
  bytes: Uint8Array,
  decodeElement: (bytes: Uint8Array) => T,
): T[] {
  const elementWidth = fixedSize(columnTypeToValueType(elementType));
  if (elementWidth != null) {
    if (elementWidth === 0) return [];
    if (bytes.length % elementWidth !== 0) {
      throw new Error(`invalid fixed-width array byte length ${bytes.length}`);
    }
    const values: T[] = [];
    for (let offset = 0; offset < bytes.length; offset += elementWidth) {
      values.push(decodeElement(bytes.subarray(offset, offset + elementWidth)));
    }
    return values;
  }

  if (bytes.length < 4) {
    throw new Error("invalid variable-width array byte length");
  }

  const length = readU32Le(bytes, 0);
  const offsetTableEnd = 4 + Math.max(0, length - 1) * 4;
  if (offsetTableEnd > bytes.length) {
    throw new Error("invalid variable-width array offset table");
  }

  const values: T[] = [];
  for (let index = 0; index < length; index += 1) {
    const start = index === 0 ? offsetTableEnd : readU32Le(bytes, 4 + (index - 1) * 4);
    const end = index === length - 1 ? bytes.length : readU32Le(bytes, 4 + index * 4);
    if (start > end || end > bytes.length) {
      throw new Error("invalid variable-width array element offset");
    }
    values.push(decodeElement(bytes.subarray(start, end)));
  }
  return values;
}

function timestampToDate(value: number, columnName?: string): Date {
  if (columnName && isProvenanceMagicTimestampColumn(columnName)) {
    return new Date(Math.trunc(value / 1_000));
  }
  return new Date(value);
}

function decodeSignedI32FromCore(value: number): number {
  return (value ^ 0x80000000) | 0;
}

function formatUuid(bytes: Uint8Array): string {
  const hex = Array.from(bytes.subarray(0, 16), (byte) => byte.toString(16).padStart(2, "0")).join(
    "",
  );
  return `${hex.slice(0, 8)}-${hex.slice(8, 12)}-${hex.slice(12, 16)}-${hex.slice(
    16,
    20,
  )}-${hex.slice(20)}`;
}

function fixedSize(valueType: ValueType): number | undefined {
  switch (valueType.tag) {
    case 0:
    case 5:
    case 9:
      return 1;
    case 1:
      return 2;
    case 2:
      return 4;
    case 3:
    case 4:
      return 8;
    case 8:
      return 16;
    case 10: {
      const members = valueType.members ?? (valueType.inner ? [valueType.inner] : []);
      return members.reduce<number | undefined>((total, member) => {
        if (total == null) return undefined;
        const memberSize = fixedSize(member);
        return memberSize == null ? undefined : total + memberSize;
      }, 0);
    }
    case 11:
      return undefined;
    case 12: {
      const innerSize = valueType.inner ? fixedSize(valueType.inner) : undefined;
      return innerSize == null ? undefined : innerSize + 1;
    }
    default:
      return undefined;
  }
}

type FieldLayout =
  | {
      kind: "fixed";
      logicalIndex: number;
      offset: number;
      size: number;
    }
  | {
      kind: "variable";
      logicalIndex: number;
      variableIndex: number;
    };

function recordLayout(descriptor: DescriptorField[]): {
  fields: FieldLayout[];
  fixed: Extract<FieldLayout, { kind: "fixed" }>[];
  variable: Extract<FieldLayout, { kind: "variable" }>[];
  fixedSize: number;
} {
  const fields: FieldLayout[] = [];
  fields.length = descriptor.length;
  const fixed: Extract<FieldLayout, { kind: "fixed" }>[] = [];
  const variable: Extract<FieldLayout, { kind: "variable" }>[] = [];
  let fixedOffset = 0;

  for (let logicalIndex = 0; logicalIndex < descriptor.length; logicalIndex += 1) {
    const size = fixedSize(descriptor[logicalIndex].valueType);
    if (size == null) continue;
    const layout = { kind: "fixed" as const, logicalIndex, offset: fixedOffset, size };
    fields[logicalIndex] = layout;
    fixed.push(layout);
    fixedOffset += size;
  }

  for (let logicalIndex = 0; logicalIndex < descriptor.length; logicalIndex += 1) {
    if (fixedSize(descriptor[logicalIndex].valueType) != null) continue;
    const layout = {
      kind: "variable" as const,
      logicalIndex,
      variableIndex: variable.length,
    };
    fields[logicalIndex] = layout;
    variable.push(layout);
  }

  return { fields, fixed, variable, fixedSize: fixedOffset };
}

function readU32Le(bytes: Uint8Array, offset: number): number {
  return (
    bytes[offset] | (bytes[offset + 1] << 8) | (bytes[offset + 2] << 16) | (bytes[offset + 3] << 24)
  );
}

function concatBytes(chunks: Uint8Array[]): Uint8Array {
  const length = chunks.reduce((sum, chunk) => sum + chunk.length, 0);
  const out = new Uint8Array(length);
  let offset = 0;
  for (const chunk of chunks) {
    out.set(chunk, offset);
    offset += chunk.length;
  }
  return out;
}

class OffsetWriter {
  readonly #bytes: number[] = [];

  u32Le(value: number): void {
    this.#bytes.push(
      value & 0xff,
      (value >>> 8) & 0xff,
      (value >>> 16) & 0xff,
      (value >>> 24) & 0xff,
    );
  }

  finish(): Uint8Array {
    return new Uint8Array(this.#bytes);
  }
}
