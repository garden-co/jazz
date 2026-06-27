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
