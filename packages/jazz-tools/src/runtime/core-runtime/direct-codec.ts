import {
  type AbiRowBatch,
  type AbiRelationSubscriptionDelta,
  type AbiRelationSubscriptionEdge,
  type AbiRelationSubscriptionSnapshot,
  type AbiRemovedRow,
  type AbiSubscriptionDelta,
  type DescriptorField,
  createRecord,
  readAbiRowBatch,
  readAbiRelationSubscriptionDelta,
  readAbiRelationSubscriptionEdge,
  readAbiRelationSubscriptionSnapshot,
  readAbiRemovedRow,
  readAbiSubscriptionDelta,
  writeDescriptor,
  writeValueType,
} from "./direct-row-codec.js";

export {
  createRecord,
  decodeRecordBool,
  decodeRecordBytes,
  decodeRecordString,
  fieldIndex,
  readAbiRowBatch,
  readAbiRelationSubscriptionDelta,
  readAbiRelationSubscriptionEdge,
  readAbiRelationSubscriptionSnapshot,
  readAbiRemovedRow,
  readAbiSubscriptionDelta,
  readDescriptor,
  readValueType,
  writeDescriptor,
  writeValueType,
} from "./direct-row-codec.js";
export type {
  AbiRelationSubscriptionDelta,
  AbiRelationSubscriptionEdge,
  AbiRelationSubscriptionSnapshot,
  AbiRemovedRow,
  AbiRow,
  AbiRowBatch,
  AbiSubscriptionDelta,
  DescriptorField,
  ValueType,
} from "./direct-row-codec.js";

export type SubscriptionSnapshotChunk = {
  type: "snapshot";
  rows: AbiRowBatch[];
  settled?: boolean;
  tier?: string;
};
export type SubscriptionDeltaChunk = {
  type: "delta";
  delta: AbiSubscriptionDelta;
  settled?: boolean;
  tier?: string;
};
export type SubscriptionStreamChunk = SubscriptionSnapshotChunk | SubscriptionDeltaChunk;

export async function readSubscriptionSnapshot(
  reader: ReadableStreamDefaultReader<SubscriptionStreamChunk>,
): Promise<SubscriptionSnapshotChunk> {
  const next = await reader.read();
  if (next.done || next.value.type !== "snapshot") {
    throw new Error("expected subscription snapshot chunk");
  }
  return next.value;
}

export async function readSubscriptionDelta(
  reader: ReadableStreamDefaultReader<SubscriptionStreamChunk>,
): Promise<SubscriptionDeltaChunk> {
  const next = await reader.read();
  if (next.done || next.value.type !== "delta") {
    throw new Error("expected subscription delta chunk");
  }
  return next.value;
}

export function tableSchema(tableName: string, descriptor: DescriptorField[]): Uint8Array {
  const writer = new PostcardWriter();
  writer.vec((table) => {
    table.string(tableName);
    table.vec((column, index) => {
      const columnSpec = descriptor[index];
      column.string(columnSpec.name ?? "");
      writeValueType(column, columnSpec.valueType);
      column.none();
    }, descriptor.length);
    table.map(0);
    table.none();
    table.none();
    table.set(0);
    table.map(0);
  }, 1);
  writer.none();
  writer.none();
  return writer.finish();
}

export function openConfig(
  node: Uint8Array,
  author: Uint8Array,
  sourceId?: number,
  historyComplete = false,
): Uint8Array {
  const writer = new PostcardWriter();
  writer.bytes(node);
  writer.bytes(author);
  if (sourceId == null) {
    writer.none();
  } else {
    writer.some((value) => value.u64(sourceId));
  }
  writer.bool(historyComplete);
  return writer.finish();
}

export function queryFromTable(table: string): Uint8Array {
  const writer = new PostcardWriter();
  writer.string(table);
  writer.vec(() => undefined, 0);
  writer.vec(() => undefined, 0);
  writer.vec(() => undefined, 0);
  writer.vec(() => undefined, 0);
  writer.none();
  writer.vec(() => undefined, 0);
  writer.none();
  writer.none();
  writer.u64(0);
  return writer.finish();
}

export function queryWhereBool(table: string, column: string, value: boolean): Uint8Array {
  return queryWithEqFilters(table, [{ column, value: { type: "Boolean", value } }]);
}

export function queryWhereStringContains(
  table: string,
  column: string,
  value: string,
  limit?: number,
): Uint8Array {
  if (limit != null && (!Number.isSafeInteger(limit) || limit < 0)) {
    throw new Error("query limit must be a non-negative safe integer");
  }
  const writer = new PostcardWriter();
  writer.string(table);
  writer.vec((filter) => {
    writePredicateContainsString(filter, column, value);
  }, 1);
  writer.vec(() => undefined, 0);
  writer.vec(() => undefined, 0);
  writer.vec(() => undefined, 0);
  writer.none();
  writer.vec(() => undefined, 0);
  writer.none();
  if (limit == null) {
    writer.none();
  } else {
    writer.some((valueWriter) => valueWriter.u64(limit));
  }
  writer.u64(0);
  return writer.finish();
}

export type DirectQueryLiteral =
  | { type: "Boolean"; value: boolean }
  | { type: "Text"; value: string }
  | { type: "Uuid"; value: string }
  | { type: "Nullable"; value: DirectQueryLiteral | null };

export type DirectQueryPredicate = {
  column: string;
  op: DirectQueryPredicateOp;
  value: DirectQueryLiteral;
};

export type DirectQueryPredicateOp = "Eq" | "Ne" | "Gt" | "Gte" | "Lt" | "Lte";
export type DirectQueryOrder = {
  column: string;
  direction: "Asc" | "Desc";
};

export function queryWithEqFilters(
  table: string,
  filters: Array<{ column: string; value: DirectQueryLiteral }>,
  limit?: number,
): Uint8Array {
  return queryWithPredicates(
    table,
    filters.map((filter) => ({ ...filter, op: "Eq" })),
    limit,
  );
}

export function queryWithPredicates(
  table: string,
  predicates: DirectQueryPredicate[],
  options: number | { limit?: number; offset?: number; orderBy?: DirectQueryOrder[] } = {},
): Uint8Array {
  const queryOptions = typeof options === "number" ? { limit: options } : options;
  const { limit, offset = 0, orderBy = [] } = queryOptions;
  if (limit != null && (!Number.isSafeInteger(limit) || limit < 0)) {
    throw new Error("query limit must be a non-negative safe integer");
  }
  if (!Number.isSafeInteger(offset) || offset < 0) {
    throw new Error("query offset must be a non-negative safe integer");
  }
  const writer = new PostcardWriter();
  writer.string(table);
  writer.vec((filter, index) => {
    const predicate = predicates[index]!;
    writePredicateCmpLiteral(filter, predicate.column, predicate.op, predicate.value);
  }, predicates.length);
  writer.vec(() => undefined, 0);
  writer.vec(() => undefined, 0);
  writer.vec(() => undefined, 0);
  writer.none();
  writer.vec((order, index) => {
    const term = orderBy[index]!;
    order.string(term.column);
    order.u64(term.direction === "Asc" ? 0 : 1);
  }, orderBy.length);
  writer.none();
  if (limit == null) {
    writer.none();
  } else {
    writer.some((valueWriter) => valueWriter.u64(limit));
  }
  writer.u64(offset);
  return writer.finish();
}

function writePredicateCmpLiteral(
  writer: PostcardWriter,
  column: string,
  op: DirectQueryPredicateOp,
  value: DirectQueryLiteral,
): void {
  writer.u64(predicateOpTag(op));
  writer.u64(0); // Operand::Column
  writer.string(column);
  writer.u64(3); // Operand::Literal
  writeGrooveValue(writer, value);
}

function predicateOpTag(op: DirectQueryPredicateOp): number {
  switch (op) {
    case "Eq":
      return 3; // Predicate::Eq
    case "Ne":
      return 4; // Predicate::Ne
    case "Gt":
      return 6; // Predicate::Gt
    case "Gte":
      return 7; // Predicate::Gte
    case "Lt":
      return 8; // Predicate::Lt
    case "Lte":
      return 9; // Predicate::Lte
  }
}

function writeGrooveValue(writer: PostcardWriter, value: DirectQueryLiteral): void {
  if (value.type === "Nullable") {
    writer.u64(12); // groove::records::Value::Nullable
    if (value.value == null) {
      writer.none();
    } else {
      writer.some((inner) => writeGrooveValue(inner, value.value!));
    }
    return;
  }
  if (value.type === "Boolean") {
    writer.u64(5); // groove::records::Value::Bool
    writer.bool(value.value);
    return;
  }
  if (value.type === "Uuid") {
    writer.u64(8); // groove::records::Value::Uuid
    writer.bytes(parseUuidBytes(value.value));
    return;
  }
  writer.u64(6); // groove::records::Value::String
  writer.string(value.value);
}

function writePredicateContainsString(writer: PostcardWriter, column: string, value: string): void {
  writer.u64(10); // Predicate::Contains
  writer.u64(0); // Operand::Column
  writer.string(column);
  writer.u64(3); // Operand::Literal
  writer.u64(6); // groove::records::Value::String
  writer.string(value);
}

function parseUuidBytes(value: string): Uint8Array {
  const hex = value.replaceAll("-", "");
  if (!/^[0-9a-fA-F]{32}$/.test(hex)) throw new Error(`invalid uuid ${value}`);
  const bytes = new Uint8Array(16);
  for (let i = 0; i < 16; i += 1) {
    bytes[i] = Number.parseInt(hex.slice(i * 2, i * 2 + 2), 16);
  }
  return bytes;
}

export function encodedCells(descriptor: DescriptorField[], values: Uint8Array[]): Uint8Array {
  const writer = new PostcardWriter();
  writeDescriptor(writer, descriptor);
  writer.bytes(createRecord(descriptor, values));
  return writer.finish();
}

export function rowCount(batches: AbiRowBatch[]): number {
  return batches.reduce((sum, batch) => sum + batch.rows.length, 0);
}

export class PostcardWriter {
  private chunks: number[] = [];

  finish(): Uint8Array {
    return Uint8Array.from(this.chunks);
  }

  u64(value: number): void {
    let remaining = value;
    do {
      let byte = remaining & 0x7f;
      remaining = Math.floor(remaining / 128);
      if (remaining !== 0) byte |= 0x80;
      this.chunks.push(byte);
    } while (remaining !== 0);
  }

  u32Le(value: number): void {
    this.chunks.push(
      value & 0xff,
      (value >>> 8) & 0xff,
      (value >>> 16) & 0xff,
      (value >>> 24) & 0xff,
    );
  }

  bool(value: boolean): void {
    this.chunks.push(value ? 1 : 0);
  }

  string(value: string): void {
    this.bytes(utf8(value));
  }

  bytes(value: Uint8Array, withLength = true): void {
    if (withLength) this.u64(value.length);
    for (let offset = 0; offset < value.length; offset += 16_384) {
      this.chunks.push(...value.subarray(offset, offset + 16_384));
    }
  }

  vec(writeItem: (writer: PostcardWriter, index: number) => void, length: number): void {
    this.u64(length);
    for (let index = 0; index < length; index += 1) {
      writeItem(this, index);
    }
  }

  map(length: number): void {
    this.u64(length);
  }

  set(length: number): void {
    this.u64(length);
  }

  none(): void {
    this.chunks.push(0);
  }

  some(writeValue: (writer: PostcardWriter) => void): void {
    this.chunks.push(1);
    writeValue(this);
  }

  enumUnit(index: number): void {
    this.u64(index);
  }
}

export class PostcardReader {
  private offset = 0;

  constructor(private readonly bytesValue: Uint8Array) {}

  u64(): number {
    let result = 0;
    let shift = 0;
    while (true) {
      const byte = this.readByte();
      result += (byte & 0x7f) * 2 ** shift;
      if ((byte & 0x80) === 0) return result;
      shift += 7;
    }
  }

  string(): string {
    return new TextDecoder().decode(this.bytes());
  }

  bool(): boolean {
    const tag = this.readByte();
    if (tag === 0) return false;
    if (tag === 1) return true;
    throw new Error(`invalid bool tag ${tag}`);
  }

  bytes(withLength = true): Uint8Array {
    const length = withLength ? this.u64() : 16;
    const end = this.offset + length;
    if (end > this.bytesValue.length) throw new Error("postcard bytes overflow");
    const value = this.bytesValue.subarray(this.offset, end);
    this.offset = end;
    return value;
  }

  option<T>(readValue: (reader: PostcardReader) => T): T | undefined {
    const tag = this.readByte();
    if (tag === 0) return undefined;
    if (tag !== 1) throw new Error(`invalid option tag ${tag}`);
    return readValue(this);
  }

  readVec<T>(readItem: (reader: PostcardReader) => T): T[] {
    const length = this.u64();
    return Array.from({ length }, () => readItem(this));
  }

  private readByte(): number {
    if (this.offset >= this.bytesValue.length) throw new Error("postcard eof");
    return this.bytesValue[this.offset++];
  }
}

export function assertBytes(value: unknown, label: string): Uint8Array {
  if (value instanceof Uint8Array) {
    return value;
  }
  if (value instanceof ArrayBuffer) {
    return new Uint8Array(value);
  }
  if (ArrayBuffer.isView(value)) {
    return new Uint8Array(
      value.buffer.slice(value.byteOffset, value.byteOffset + value.byteLength),
    );
  }
  if (
    Array.isArray(value) &&
    value.every((byte) => Number.isInteger(byte) && byte >= 0 && byte <= 255)
  ) {
    return Uint8Array.from(value);
  }
  throw new Error(`expected ${label} to be bytes`);
}

export function normalizePayloadBytes(value: unknown, label = "payload"): Uint8Array {
  return assertBytes(value, label);
}

export function optionalNumber(value: unknown): number | undefined {
  return typeof value === "number" ? value : undefined;
}

export function utf8(value: string): Uint8Array {
  return new TextEncoder().encode(value);
}
