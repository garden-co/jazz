export type ValueType = { tag: number; inner?: ValueType };
export type DescriptorField = { name?: string; valueType: ValueType };
export type AbiRow = { rowId: Uint8Array; deleted: boolean; raw: Uint8Array };
export type AbiRowBatch = { table: string; descriptor: DescriptorField[]; rows: AbiRow[] };
export type AbiRemovedRow = { table: string; rowId: Uint8Array };
export type AbiSubscriptionDelta = {
  added: AbiRowBatch[];
  updated: AbiRowBatch[];
  removed: AbiRemovedRow[];
};
export type AbiRelationSubscriptionEdge = {
  sourceTable: string;
  sourceRowId: Uint8Array;
  relation: string;
  targetTable: string;
  targetRowId: Uint8Array;
};
export type AbiRelationSubscriptionSnapshot = {
  cursor: number;
  rows: AbiRowBatch[];
  edges: AbiRelationSubscriptionEdge[];
};
export type AbiRelationSubscriptionDelta = {
  baseCursor?: number;
  cursor: number;
  added: AbiRowBatch[];
  updated: AbiRowBatch[];
  removed: AbiRemovedRow[];
  addedEdges: AbiRelationSubscriptionEdge[];
  removedEdges: AbiRelationSubscriptionEdge[];
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

export function readAbiRowBatch(reader: PostcardReaderLike): AbiRowBatch {
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

export function readAbiSubscriptionDelta(reader: PostcardReaderLike): AbiSubscriptionDelta {
  return {
    added: reader.readVec(readAbiRowBatch),
    updated: reader.readVec(readAbiRowBatch),
    removed: reader.readVec(readAbiRemovedRow),
  };
}


export function readAbiRelationSubscriptionSnapshot(reader: PostcardReaderLike): AbiRelationSubscriptionSnapshot {
  return {
    cursor: reader.u64(),
    rows: reader.readVec(readAbiRowBatch),
    edges: reader.readVec(readAbiRelationSubscriptionEdge),
  };
}

export function readAbiRelationSubscriptionDelta(reader: PostcardReaderLike): AbiRelationSubscriptionDelta {
  return {
    baseCursor: reader.option((value) => value.u64()),
    cursor: reader.u64(),
    added: reader.readVec(readAbiRowBatch),
    updated: reader.readVec(readAbiRowBatch),
    removed: reader.readVec(readAbiRemovedRow),
    addedEdges: reader.readVec(readAbiRelationSubscriptionEdge),
    removedEdges: reader.readVec(readAbiRelationSubscriptionEdge),
  };
}

export function readAbiRemovedRow(reader: PostcardReaderLike): AbiRemovedRow {
  return {
    table: reader.string(),
    rowId: reader.bytes(),
  };
}

export function readAbiRelationSubscriptionEdge(reader: PostcardReaderLike): AbiRelationSubscriptionEdge {
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
}

export function readValueType(reader: PostcardReaderLike): ValueType {
  const tag = reader.u64();
  if (tag === 12 || tag === 13) {
    return { tag, inner: readValueType(reader) };
  }
  if (tag === 10) {
    const members = reader.readVec(readValueType);
    return { tag, inner: members[0] };
  }
  return { tag };
}

export function createRecord(descriptor: DescriptorField[], values: Uint8Array[]): Uint8Array {
  const staticChunks: Uint8Array[] = [];
  const variableChunks: Uint8Array[] = [];
  for (let index = 0; index < descriptor.length; index += 1) {
    const valueType = descriptor[index].valueType;
    if (fixedSize(valueType) == null) {
      variableChunks.push(values[index]);
    } else {
      staticChunks.push(values[index]);
    }
  }
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
  const index = descriptor.findIndex((field) => field.name === name || field.name === `user_${name}`);
  if (index < 0) {
    throw new Error(`missing ${name} field in [${descriptor.map((field) => field.name ?? "<anonymous>").join(", ")}]`);
  }
  return index;
}

export function decodeRecordBool(descriptor: DescriptorField[], raw: Uint8Array, logicalIndex: number): boolean {
  const bytes = decodeRecordBytes(descriptor, raw, logicalIndex);
  if (bytes.length !== 1) throw new Error(`invalid bool size ${bytes.length}`);
  return bytes[0] !== 0;
}

export function decodeRecordString(descriptor: DescriptorField[], raw: Uint8Array, logicalIndex: number): string {
  return new TextDecoder().decode(decodeRecordBytes(descriptor, raw, logicalIndex));
}

export function decodeRecordBytes(descriptor: DescriptorField[], raw: Uint8Array, logicalIndex: number): Uint8Array {
  const valueType = descriptor[logicalIndex].valueType;
  let fixedOffset = 0;
  const variables: { index: number; offsetIndex: number }[] = [];
  for (let index = 0; index < descriptor.length; index += 1) {
    const size = fixedSize(descriptor[index].valueType);
    if (size == null) {
      variables.push({ index, offsetIndex: variables.length });
    } else if (index === logicalIndex) {
      let value = raw.subarray(fixedOffset, fixedOffset + size);
      if (valueType.tag === 12) value = unwrapNullable(value);
      return value;
    } else {
      fixedOffset += size;
    }
  }
  const target = variables.find((variable) => variable.index === logicalIndex);
  if (!target) throw new Error("field is not present");
  const offsetTableStart = fixedOffset;
  const variableStart = fixedOffset + Math.max(0, variables.length - 1) * 4;
  const start = target.offsetIndex === 0 ? variableStart : readU32Le(raw, offsetTableStart + (target.offsetIndex - 1) * 4);
  const end = target.offsetIndex === variables.length - 1 ? raw.length : readU32Le(raw, offsetTableStart + target.offsetIndex * 4);
  let value = raw.subarray(start, end);
  if (valueType.tag === 12) value = unwrapNullable(value);
  return value;
}

function unwrapNullable(value: Uint8Array): Uint8Array {
  if (value[0] === 0) return new Uint8Array();
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
    case 12: {
      const innerSize = valueType.inner ? fixedSize(valueType.inner) : undefined;
      return innerSize == null ? undefined : innerSize + 1;
    }
    case 13:
      return valueType.inner ? fixedSize(valueType.inner) : undefined;
    default:
      return undefined;
  }
}

function readU32Le(bytes: Uint8Array, offset: number): number {
  return bytes[offset] | (bytes[offset + 1] << 8) | (bytes[offset + 2] << 16) | (bytes[offset + 3] << 24);
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
    this.#bytes.push(value & 0xff, (value >>> 8) & 0xff, (value >>> 16) & 0xff, (value >>> 24) & 0xff);
  }

  finish(): Uint8Array {
    return new Uint8Array(this.#bytes);
  }
}
