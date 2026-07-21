import {
  type NativeRowBatch,
  type NativeRelationSubscriptionDelta,
  type NativeRelationSubscriptionEdge,
  type NativeRelationSubscriptionSnapshot,
  type NativeRemovedRow,
  type NativeSubscriptionDelta,
  type DescriptorField,
  createRecord,
  readNativeRowBatch,
  readNativeRelationSubscriptionDelta,
  readNativeRelationSubscriptionEdge,
  readNativeRelationSubscriptionSnapshot,
  readNativeRemovedRow,
  readNativeSubscriptionDelta,
  writeDescriptor,
  writeValueType,
} from "./native-row-codec.js";

export {
  createRecord,
  decodeRecordBool,
  decodeRecordBytes,
  decodeRecordString,
  fieldIndex,
  readNativeRowBatch,
  readNativeRelationSubscriptionDelta,
  readNativeRelationSubscriptionEdge,
  readNativeRelationSubscriptionSnapshot,
  readNativeRemovedRow,
  readNativeSubscriptionDelta,
  readDescriptor,
  readValueType,
  writeDescriptor,
  writeValueType,
} from "./native-row-codec.js";
export type {
  NativeRelationSubscriptionDelta,
  NativeRelationSubscriptionEdge,
  NativeRelationSubscriptionSnapshot,
  NativeRemovedRow,
  NativeRow,
  NativeRowBatch,
  NativeSubscriptionDelta,
  DescriptorField,
  ValueType,
} from "./native-row-codec.js";

export type SubscriptionSnapshotChunk = {
  type: "snapshot";
  rows: NativeRowBatch[];
  settled?: boolean;
  tier?: string;
};
export type SubscriptionDeltaChunk = {
  type: "delta";
  delta: NativeSubscriptionDelta;
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
      column.none();
    }, descriptor.length);
    table.map(0);
    table.none();
    table.none();
    table.none();
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
  return queryWithPredicates(table, []);
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
  return queryWithPredicates(
    table,
    [{ column, op: "Contains", value: { type: "Text", value } }],
    limit,
  );
}

export type QueryLiteral =
  | { type: "Boolean"; value: boolean }
  | { type: "Integer"; value: number }
  | { type: "BigInt"; value: bigint }
  | { type: "Double"; value: number }
  | { type: "Timestamp"; value: number }
  | { type: "Text"; value: string }
  | { type: "Uuid"; value: string }
  | { type: "Bytea"; value: Uint8Array }
  | { type: "Array"; value: QueryLiteral[] }
  | { type: "Nullable"; value: QueryLiteral | null };

export type QueryPredicate =
  | {
      op: "All" | "Any";
      predicates: QueryPredicate[];
    }
  | {
      column: string;
      op: QueryPredicateOp;
      value: QueryLiteral;
    }
  | {
      column: string;
      op: "In";
      values: QueryLiteral[];
    }
  | {
      column: string;
      op: "Contains";
      value: QueryLiteral;
    }
  | {
      column: string;
      op: "IsNull";
    }
  | {
      column: string;
      op: "IsNotNull";
    };

export type QueryPredicateOp = "Eq" | "Ne" | "Gt" | "Gte" | "Lt" | "Lte";
export type QueryOrder = {
  column: string;
  direction: "Asc" | "Desc";
};
export type QueryArraySubqueryRequirement =
  | "Optional"
  | "AtLeastOne"
  | "MatchCorrelationCardinality";
export type QueryArraySubquery = {
  columnName: string;
  table: string;
  innerColumn: string;
  outerColumn: string;
  filters?: QueryPredicate[];
  select?: string[];
  orderBy?: QueryOrder[];
  limit?: number | null;
  requirement?: QueryArraySubqueryRequirement;
  nestedArrays?: QueryArraySubquery[];
};

export type QueryAggregateFunction = "count" | "sum" | "avg" | "min" | "max";

export type QueryAggregate = {
  function: QueryAggregateFunction;
  column?: string;
  alias: string;
};

export type QueryAggregateSpec = {
  aggregates: QueryAggregate[];
  groupBy?: string;
};

// Postcard enum discriminants for jazz::query::AggregateFunction, in variant
// declaration order.
const AGGREGATE_FUNCTION_DISCRIMINANTS: Record<QueryAggregateFunction, number> = {
  count: 0,
  sum: 1,
  avg: 2,
  min: 3,
  max: 4,
};

export type QueryOptions = {
  limit?: number;
  offset?: number;
  orderBy?: QueryOrder[];
  select?: string[];
  arraySubqueries?: QueryArraySubquery[];
  aggregate?: QueryAggregateSpec;
};

export function queryWithEqFilters(
  table: string,
  filters: Array<{ column: string; value: QueryLiteral }>,
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
  predicates: QueryPredicate[],
  options: number | QueryOptions = {},
): Uint8Array {
  const queryOptions = typeof options === "number" ? { limit: options } : options;
  const { limit, offset = 0, orderBy = [], select, arraySubqueries = [], aggregate } = queryOptions;
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
    writePredicate(filter, predicate);
  }, predicates.length);
  writer.vec(() => undefined, 0);
  writer.vec(() => undefined, 0);
  writer.vec(() => undefined, 0);
  writer.vec(() => undefined, 0);
  writer.vec(() => undefined, 0);
  writer.vec((subquery, index) => {
    writeArraySubquery(subquery, arraySubqueries[index]!);
  }, arraySubqueries.length);
  if (select == null) {
    writer.none();
  } else {
    writer.some((selectWriter) => {
      selectWriter.vec((columnWriter, index) => {
        columnWriter.string(select[index]!);
      }, select.length);
    });
  }
  writer.vec((order, index) => {
    const term = orderBy[index]!;
    order.string(term.column);
    order.u64(term.direction === "Asc" ? 0 : 1);
  }, orderBy.length);
  // jazz::query::Query.aggregate — Option<AggregateQuery { aggregates, group_by }>.
  if (aggregate == null) {
    writer.none();
  } else {
    writer.some((aggregateWriter) => {
      aggregateWriter.vec((item, index) => {
        const spec = aggregate.aggregates[index]!;
        item.u64(AGGREGATE_FUNCTION_DISCRIMINANTS[spec.function]);
        if (spec.column == null) {
          item.none();
        } else {
          item.some((columnWriter) => columnWriter.string(spec.column!));
        }
        item.string(spec.alias);
      }, aggregate.aggregates.length);
      if (aggregate.groupBy == null) {
        aggregateWriter.none();
      } else {
        aggregateWriter.some((groupWriter) => groupWriter.string(aggregate.groupBy!));
      }
    });
  }
  if (limit == null) {
    writer.none();
  } else {
    writer.some((valueWriter) => valueWriter.u64(limit));
  }
  writer.u64(offset);
  return writer.finish();
}

function writeArraySubquery(writer: PostcardWriter, subquery: QueryArraySubquery): void {
  const {
    filters = [],
    select,
    orderBy = [],
    limit = null,
    requirement = "Optional",
    nestedArrays = [],
  } = subquery;
  writer.string(subquery.columnName);
  writer.string(subquery.table);
  writer.string(subquery.innerColumn);
  writer.string(subquery.outerColumn);
  writer.vec((filter, index) => {
    writePredicate(filter, filters[index]!);
  }, filters.length);
  if (select == null) {
    writer.none();
  } else {
    writer.some((selectWriter) => {
      selectWriter.vec((columnWriter, index) => {
        columnWriter.string(select[index]!);
      }, select.length);
    });
  }
  writer.vec((order, index) => {
    const term = orderBy[index]!;
    order.string(term.column);
    order.u64(term.direction === "Asc" ? 0 : 1);
  }, orderBy.length);
  if (limit == null) {
    writer.none();
  } else {
    writer.some((valueWriter) => valueWriter.u64(limit));
  }
  writer.u64(arraySubqueryRequirementTag(requirement));
  writer.vec((nested, index) => {
    writeArraySubquery(nested, nestedArrays[index]!);
  }, nestedArrays.length);
}

function arraySubqueryRequirementTag(requirement: QueryArraySubqueryRequirement): number {
  switch (requirement) {
    case "Optional":
      return 0;
    case "AtLeastOne":
      return 1;
    case "MatchCorrelationCardinality":
      return 2;
  }
}

function writePredicate(writer: PostcardWriter, predicate: QueryPredicate): void {
  if (predicate.op === "All" || predicate.op === "Any") {
    writer.u64(predicate.op === "All" ? 0 : 1);
    writer.vec(
      (predicateWriter, index) => writePredicate(predicateWriter, predicate.predicates[index]!),
      predicate.predicates.length,
    );
    return;
  }
  if (predicate.op === "In") {
    writer.u64(5); // Predicate::In
    writeColumnOperand(writer, predicate.column);
    writer.vec((valueWriter, index) => {
      valueWriter.u64(3); // Operand::Literal
      writeGrooveValue(valueWriter, predicate.values[index]!);
    }, predicate.values.length);
    return;
  }
  if (predicate.op === "Contains") {
    writer.u64(10); // Predicate::Contains
    writeColumnOperand(writer, predicate.column);
    writeLiteralOperand(writer, predicate.value);
    return;
  }
  if (predicate.op === "IsNull" || predicate.op === "IsNotNull") {
    if (predicate.op === "IsNotNull") {
      writer.u64(2); // Predicate::Not
    }
    writer.u64(11); // Predicate::IsNull
    writeColumnOperand(writer, predicate.column);
    return;
  }
  if (isQueryPredicateCmp(predicate)) {
    writePredicateCmpLiteral(writer, predicate.column, predicate.op, predicate.value);
    return;
  }
  throw new Error(`unsupported query predicate ${JSON.stringify(predicate)}`);
}

function isQueryPredicateCmp(
  predicate: QueryPredicate,
): predicate is Extract<QueryPredicate, { op: QueryPredicateOp }> {
  return (
    predicate.op === "Eq" ||
    predicate.op === "Ne" ||
    predicate.op === "Gt" ||
    predicate.op === "Gte" ||
    predicate.op === "Lt" ||
    predicate.op === "Lte"
  );
}

function writePredicateCmpLiteral(
  writer: PostcardWriter,
  column: string,
  op: QueryPredicateOp,
  value: QueryLiteral,
): void {
  writer.u64(predicateOpTag(op));
  writeColumnOperand(writer, column);
  writeLiteralOperand(writer, value);
}

function writeColumnOperand(writer: PostcardWriter, column: string): void {
  writer.u64(0); // Operand::Column
  writer.string(column);
}

function writeLiteralOperand(writer: PostcardWriter, value: QueryLiteral): void {
  writer.u64(3); // Operand::Literal
  writeGrooveValue(writer, value);
}

function predicateOpTag(op: QueryPredicateOp): number {
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

function writeGrooveValue(writer: PostcardWriter, value: QueryLiteral): void {
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
  if (value.type === "Integer") {
    if (
      !Number.isSafeInteger(value.value) ||
      value.value < -0x80000000 ||
      value.value > 0x7fffffff
    ) {
      throw new Error("Integer value must be a signed 32-bit integer");
    }
    writer.u64(2); // groove::records::Value::U32
    writer.u64((value.value ^ 0x80000000) >>> 0);
    return;
  }
  if (value.type === "BigInt") {
    writer.u64(13); // groove::records::Value::I64
    writer.i64(value.value);
    return;
  }
  if (value.type === "Timestamp") {
    if (!Number.isSafeInteger(value.value) || value.value < 0) {
      throw new Error(`${value.type} value must be a non-negative safe integer`);
    }
    writer.u64(3); // groove::records::Value::U64
    writer.u64(value.value);
    return;
  }
  if (value.type === "Double") {
    if (!Number.isFinite(value.value)) {
      throw new Error("Double value must be finite");
    }
    writer.u64(4); // groove::records::Value::F64
    writer.f64Le(value.value);
    return;
  }
  if (value.type === "Uuid") {
    writer.u64(8); // groove::records::Value::Uuid
    writer.bytes(parseUuidBytes(value.value));
    return;
  }
  if (value.type === "Bytea") {
    writer.u64(7); // groove::records::Value::Bytes
    writer.bytes(value.value);
    return;
  }
  if (value.type === "Array") {
    writer.u64(11); // groove::records::Value::Array
    writer.vec((item, index) => writeGrooveValue(item, value.value[index]!), value.value.length);
    return;
  }
  writer.u64(6); // groove::records::Value::String
  writer.string(value.value);
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

export function rowCount(batches: NativeRowBatch[]): number {
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

  i64(value: bigint | number): void {
    const bigintValue = BigInt(value);
    const encoded = bigintValue < 0n ? (-bigintValue << 1n) - 1n : bigintValue << 1n;
    this.u64Big(encoded);
  }

  private u64Big(value: bigint): void {
    let remaining = value;
    do {
      let byte = Number(remaining & 0x7fn);
      remaining >>= 7n;
      if (remaining !== 0n) byte |= 0x80;
      this.chunks.push(byte);
    } while (remaining !== 0n);
  }

  u32Le(value: number): void {
    this.chunks.push(
      value & 0xff,
      (value >>> 8) & 0xff,
      (value >>> 16) & 0xff,
      (value >>> 24) & 0xff,
    );
  }

  f64Le(value: number): void {
    const bytes = new Uint8Array(8);
    new DataView(bytes.buffer).setFloat64(0, value, true);
    this.bytes(bytes, false);
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

  i64(): bigint {
    let result = 0n;
    let shift = 0n;
    while (true) {
      const byte = this.readByte();
      result += BigInt(byte & 0x7f) << shift;
      if ((byte & 0x80) === 0) {
        return (result & 1n) === 0n ? result >> 1n : -((result + 1n) >> 1n);
      }
      shift += 7n;
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

  f64Le(): number {
    const bytes = this.bytesOfLength(8);
    return new DataView(bytes.buffer, bytes.byteOffset, bytes.byteLength).getFloat64(0, true);
  }

  bytes(withLength = true): Uint8Array {
    const length = withLength ? this.u64() : 16;
    return this.bytesOfLength(length);
  }

  private bytesOfLength(length: number): Uint8Array {
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
