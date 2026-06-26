import type {
  ColumnDescriptor,
  ColumnType,
  InsertValues,
  SubscriptionWireDelta,
  Value,
  WasmSchema,
} from "../../drivers/types.js";
import { serializeRuntimeSchema } from "../../drivers/schema-wire.js";
import type {
  DirectInsertResult,
  DirectMutationResult,
  MutationErrorEvent,
  Runtime,
  TransactionKind,
} from "../client.js";
import {
  PostcardReader,
  PostcardWriter,
  openConfig,
  queryFromTable,
  readAbiRowBatch,
  readAbiSubscriptionDelta,
  type AbiRowBatch,
  type DescriptorField,
  type ValueType,
} from "./direct-codec.js";
import { createRecord, decodeRecordBytes } from "./direct-row-codec.js";

type WasmDbConstructor = {
  openMemory(schema: Uint8Array, config: Uint8Array): DirectWasmDb;
  openBrowser(namespace: string, schema: Uint8Array, config: Uint8Array): Promise<DirectWasmDb>;
};

type DirectWasmDb = {
  all(query: DirectPreparedQuery, opts: unknown): Uint8Array;
  one(query: DirectPreparedQuery, opts: unknown): Uint8Array;
  prepareQuery(query: Uint8Array): DirectPreparedQuery;
  subscribe(query: DirectPreparedQuery, opts: unknown): ReadableStream<unknown>;
  insertWithIdEncoded(table: string, rowId: Uint8Array, cells: Uint8Array): DirectWrite;
  restoreEncoded(table: string, rowId: Uint8Array, cells: Uint8Array): DirectWrite;
  updateEncoded(table: string, rowId: Uint8Array, patch: Uint8Array): DirectWrite;
  upsertEncoded(table: string, rowId: Uint8Array, cells: Uint8Array): DirectWrite;
  delete(table: string, rowId: Uint8Array): DirectWrite;
  mergeableTx(): DirectTx;
  connectUpstream(): DirectTransport;
  tick(): void;
};

type DirectPreparedQuery = object;

type DirectWrite = {
  payload: Uint8Array;
  wait(tier: string): void;
  writeState(): unknown;
};

type DirectTx = {
  commit(): DirectWrite;
  rollback(): void;
  insertWithIdEncoded(table: string, rowId: Uint8Array, cells: Uint8Array): void;
  restoreEncoded(table: string, rowId: Uint8Array, cells: Uint8Array): void;
  updateEncoded(table: string, rowId: Uint8Array, patch: Uint8Array): void;
  upsertEncoded(table: string, rowId: Uint8Array, cells: Uint8Array): void;
  delete(table: string, rowId: Uint8Array): void;
};

export type DirectTransport = {
  close(): boolean;
  recvWireFrames(): unknown[];
  sendWireFrame(frame: Uint8Array): void;
  tick(): number;
};

export type DirectOpenPayload = {
  schema: Uint8Array;
  config: Uint8Array;
};

type PendingTx = {
  kind: TransactionKind;
  tx: DirectTx;
  writes: Array<{ table: string; rowId: Uint8Array }>;
};

type SubscriptionState = {
  reader: ReadableStreamDefaultReader<unknown>;
  rows: RowState[];
  callback?: Function;
  cancelled: boolean;
};

type RowState = {
  table: string;
  id: string;
  values: Value[];
};

const textEncoder = new TextEncoder();
const textDecoder = new TextDecoder();

export class DirectWasmRuntime implements Runtime {
  private readonly db: DirectWasmDb;
  private readonly schemaBytes: Uint8Array;
  private readonly configBytes: Uint8Array;
  private readonly schemaHash: string;
  private readonly preparedQueries = new Map<string, DirectPreparedQuery>();
  private readonly pendingTxs = new Map<string, PendingTx>();
  private readonly writes = new Map<string, DirectWrite>();
  private readonly subscriptions = new Map<number, SubscriptionState>();
  private mutationErrorCallback: ((event: MutationErrorEvent) => void) | null = null;
  private authFailureCallback: ((reason: string) => void) | null = null;
  private nextTransactionId = 1;
  private nextSubscriptionId = 1;

  constructor(
    Runtime: WasmDbConstructor,
    private readonly schema: WasmSchema,
    node: Uint8Array,
    author: Uint8Array,
    sourceId: number,
    historyComplete: boolean,
  ) {
    this.schemaBytes = encodeSchema(schema);
    this.configBytes = openConfig(node, author, sourceId, historyComplete);
    this.schemaHash = serializeRuntimeSchema(schema);
    this.db = Runtime.openMemory(this.schemaBytes, this.configBytes);
  }

  getDirectOpenPayload(): DirectOpenPayload {
    return { schema: this.schemaBytes, config: this.configBytes };
  }

  connectUpstreamPeer(): DirectTransport {
    return this.db.connectUpstream();
  }

  insert(
    table: string,
    values: InsertValues,
    _writeContext?: string | null,
    objectId?: string | null,
  ): DirectInsertResult {
    const rowId = objectId ? parseUuid(objectId) : crypto.getRandomValues(new Uint8Array(16));
    const cells = encodeCellsForRow(this.table(table), values);
    const tx = this.currentTx(_writeContext);
    if (tx) {
      tx.tx.insertWithIdEncoded(table, rowId, cells);
      tx.writes.push({ table, rowId });
      return this.resultForRow(table, rowId, txIdFromContext(_writeContext) ?? "");
    }
    const write = this.db.insertWithIdEncoded(table, rowId, cells);
    return this.finishInsert(table, rowId, write);
  }

  restore(table: string, objectId: string, values: InsertValues, writeContext?: string | null): DirectInsertResult {
    const rowId = parseUuid(objectId);
    const cells = encodeCellsForRow(this.table(table), values);
    const tx = this.currentTx(writeContext);
    if (tx) {
      tx.tx.restoreEncoded(table, rowId, cells);
      tx.writes.push({ table, rowId });
      return this.resultForRow(table, rowId, txIdFromContext(writeContext) ?? "");
    }
    return this.finishInsert(table, rowId, this.db.restoreEncoded(table, rowId, cells));
  }

  update(objectId: string, values: Record<string, Value>, writeContext?: string | null): DirectMutationResult {
    const { table, rowId } = this.resolveRow(objectId);
    const patch = encodeCellsForPatch(this.table(table), values);
    const tx = this.currentTx(writeContext);
    if (tx) {
      tx.tx.updateEncoded(table, rowId, patch);
      tx.writes.push({ table, rowId });
      return { transactionId: txIdFromContext(writeContext) ?? "" };
    }
    return this.finishMutation(this.db.updateEncoded(table, rowId, patch));
  }

  upsert(table: string, objectId: string, values: InsertValues, writeContext?: string | null): DirectMutationResult {
    const rowId = parseUuid(objectId);
    const cells = encodeCellsForRow(this.table(table), values);
    const tx = this.currentTx(writeContext);
    if (tx) {
      tx.tx.upsertEncoded(table, rowId, cells);
      tx.writes.push({ table, rowId });
      return { transactionId: txIdFromContext(writeContext) ?? "" };
    }
    return this.finishMutation(this.db.upsertEncoded(table, rowId, cells));
  }

  delete(objectId: string, writeContext?: string | null): DirectMutationResult {
    const { table, rowId } = this.resolveRow(objectId);
    const tx = this.currentTx(writeContext);
    if (tx) {
      tx.tx.delete(table, rowId);
      tx.writes.push({ table, rowId });
      return { transactionId: txIdFromContext(writeContext) ?? "" };
    }
    return this.finishMutation(this.db.delete(table, rowId));
  }

  onMutationError(callback: (event: MutationErrorEvent) => void): void {
    this.mutationErrorCallback = callback;
  }

  beginTransaction(kind: TransactionKind): string {
    if (kind !== "mergeable") {
      throw new Error("Direct WasmDb runtime does not support exclusive transactions yet");
    }
    const id = `tx-${this.nextTransactionId++}`;
    this.pendingTxs.set(id, { kind, tx: this.db.mergeableTx(), writes: [] });
    return id;
  }

  commitTransaction(transactionId: string): void {
    const pending = this.pendingTxs.get(transactionId);
    if (!pending) throw new Error(`unknown transaction ${transactionId}`);
    const write = pending.tx.commit();
    this.writes.set(transactionId, write);
    this.pendingTxs.delete(transactionId);
    this.pumpSubscriptions();
  }

  async waitForTransaction(transactionId: string, tier: string): Promise<void> {
    const write = this.writes.get(transactionId);
    write?.wait(tier);
  }

  rollbackTransaction(transactionId: string): boolean {
    const pending = this.pendingTxs.get(transactionId);
    if (!pending) return false;
    pending.tx.rollback();
    this.pendingTxs.delete(transactionId);
    return true;
  }

  async query(queryJson: string, _sessionJson?: string | null, _tier?: string | null, _optionsJson?: string | null): Promise<unknown> {
    const query = this.prepareQuery(queryJson);
    return rowsFromBatches(readRowBatches(this.db.all(query, readOptions())), this.schema);
  }

  createSubscription(queryJson: string, _sessionJson?: string | null, _tier?: string | null, _optionsJson?: string | null): number {
    const handle = this.nextSubscriptionId++;
    const query = this.prepareQuery(queryJson);
    const reader = this.db.subscribe(query, readOptions()).getReader();
    this.subscriptions.set(handle, { reader, rows: [], cancelled: false });
    return handle;
  }

  executeSubscription(handle: number, onUpdate: Function): void {
    const subscription = this.subscriptions.get(handle);
    if (!subscription) return;
    subscription.callback = onUpdate;
    void this.readSubscription(handle, subscription);
  }

  unsubscribe(handle: number): void {
    const subscription = this.subscriptions.get(handle);
    if (!subscription) return;
    subscription.cancelled = true;
    void subscription.reader.cancel();
    this.subscriptions.delete(handle);
  }

  connect(_url: string, _authJson: string): void {
    throw new Error("Server websocket transport is not wired to DirectWasmRuntime yet");
  }

  disconnect(): void {}

  updateAuth(_authJson: string): void {}

  onAuthFailure(callback: (reason: string) => void): void {
    this.authFailureCallback = callback;
  }

  getSchema(): unknown {
    return this.schema;
  }

  getSchemaHash(): string {
    return this.schemaHash;
  }

  private finishInsert(table: string, rowId: Uint8Array, write: DirectWrite): DirectInsertResult {
    const transactionId = writeId(write, this.writes);
    this.pumpSubscriptions();
    return this.resultForRow(table, rowId, transactionId);
  }

  private finishMutation(write: DirectWrite): DirectMutationResult {
    const transactionId = writeId(write, this.writes);
    this.pumpSubscriptions();
    return { transactionId };
  }

  private resultForRow(table: string, rowId: Uint8Array, transactionId: string): DirectInsertResult {
    const row = this.readRow(table, rowId);
    return { id: formatUuid(rowId), values: row?.values ?? [], transactionId };
  }

  private readRow(table: string, rowId: Uint8Array): RowState | undefined {
    const query = this.prepareQuery(JSON.stringify({ table }));
    return rowsFromBatches(readRowBatches(this.db.all(query, readOptions())), this.schema)
      .find((row) => row.table === table && row.id === formatUuid(rowId));
  }

  private resolveRow(objectId: string): { table: string; rowId: Uint8Array } {
    const rowId = parseUuid(objectId);
    for (const table of Object.keys(this.schema)) {
      if (this.readRow(table, rowId)) return { table, rowId };
    }
    const firstTable = Object.keys(this.schema)[0];
    if (!firstTable) throw new Error("cannot resolve row without schema tables");
    return { table: firstTable, rowId };
  }

  private prepareQuery(queryJson: string): DirectPreparedQuery {
    const queryBytes = encodeQueryJson(queryJson);
    const key = bytesKey(queryBytes);
    let query = this.preparedQueries.get(key);
    if (!query) {
      query = this.db.prepareQuery(queryBytes);
      this.preparedQueries.set(key, query);
    }
    return query;
  }

  private table(table: string): { columns: ColumnDescriptor[] } {
    const definition = this.schema[table];
    if (!definition) throw new Error(`unknown table ${table}`);
    return definition;
  }

  private currentTx(writeContext?: string | null): PendingTx | undefined {
    const id = txIdFromContext(writeContext);
    return id ? this.pendingTxs.get(id) : undefined;
  }

  private pumpSubscriptions(): void {
    this.db.tick();
    for (const [handle, subscription] of this.subscriptions) {
      void this.readSubscription(handle, subscription);
    }
  }

  private async readSubscription(handle: number, subscription: SubscriptionState): Promise<void> {
    if (subscription.cancelled) return;
    const next = await subscription.reader.read();
    if (next.done || subscription.cancelled) return;
    const chunk = normalizeSubscriptionChunk(next.value);
    if (chunk.type === "snapshot") {
      subscription.rows = rowsFromBatches(chunk.rows, this.schema);
      subscription.callback?.(nativeDeltaFromRows(subscription.rows));
    } else {
      subscription.rows = rowsFromBatches(chunk.delta.added, this.schema)
        .concat(rowsFromBatches(chunk.delta.updated, this.schema));
      subscription.callback?.(nativeDeltaFromRows(subscription.rows));
    }
  }
}

function writeId(write: DirectWrite, writes: Map<string, DirectWrite>): string {
  const id = `tx-${writes.size + 1}`;
  writes.set(id, write);
  return id;
}

function txIdFromContext(writeContext?: string | null): string | undefined {
  if (!writeContext) return undefined;
  try {
    const parsed = JSON.parse(writeContext) as { batch_id?: unknown };
    return typeof parsed.batch_id === "string" ? parsed.batch_id : undefined;
  } catch {
    return undefined;
  }
}

function readOptions(): unknown {
  return { tier: "local" };
}

function encodeQueryJson(queryJson: string): Uint8Array {
  const parsed = JSON.parse(queryJson) as { table?: unknown; limit?: unknown };
  if (typeof parsed.table !== "string") {
    throw new Error("Direct WasmDb runtime only supports table queries in this slice");
  }
  if (parsed.limit != null) {
    throw new Error("Direct WasmDb runtime query limit encoding is not wired yet");
  }
  return queryFromTable(parsed.table);
}

function encodeSchema(schema: WasmSchema): Uint8Array {
  const tables = Object.entries(schema);
  const writer = new PostcardWriter();
  writer.vec((table, index) => {
    const [tableName, definition] = tables[index]!;
    table.string(tableName);
    table.vec((column, columnIndex) => {
      const columnSpec = definition.columns[columnIndex]!;
      column.string(columnSpec.name);
      writeValueType(column, columnValueType(columnSpec));
      column.none();
    }, definition.columns.length);
    table.map(definition.columns.filter((column) => column.references).length);
    for (const column of definition.columns) {
      if (column.references) {
        table.string(column.name);
        table.string(column.references);
      }
    }
    table.none();
    table.none();
    table.set(0);
    table.map(0);
  }, tables.length);
  writer.none();
  writer.none();
  return writer.finish();
}

function encodeCellsForRow(definition: { columns: ColumnDescriptor[] }, row: InsertValues): Uint8Array {
  return encodeCells(definition.columns, (column) => row[column.name], true);
}

function encodeCellsForPatch(definition: { columns: ColumnDescriptor[] }, patch: Record<string, Value>): Uint8Array {
  const columns = definition.columns.filter((column) => Object.hasOwn(patch, column.name));
  return encodeCells(columns, (column) => patch[column.name], false);
}

function encodeCells(
  columns: ColumnDescriptor[],
  valueFor: (column: ColumnDescriptor) => Value | undefined,
  requireMissingDefaults: boolean,
): Uint8Array {
  const descriptor = [...columns]
    .sort((left, right) => left.name.localeCompare(right.name))
    .map((column) => ({ name: column.name, valueType: columnValueType(column), column }));
  const values = descriptor.map(({ column }) => encodeValue(column, valueFor(column), requireMissingDefaults));
  const writer = new PostcardWriter();
  writer.vec((field, index) => {
    field.some((name) => name.string(descriptor[index]!.name));
    writeValueType(field, descriptor[index]!.valueType);
  }, descriptor.length);
  writer.bytes(createRecord(descriptor, values));
  return writer.finish();
}

function encodeValue(column: ColumnDescriptor, value: Value | undefined, requireMissingDefaults: boolean): Uint8Array {
  const resolved = value ?? column.default;
  if (!resolved || resolved.type === "Null") {
    if (column.nullable) return Uint8Array.of(0);
    if (requireMissingDefaults) throw new Error(`missing required column ${column.name}`);
    return new Uint8Array();
  }
  const bytes = encodeNonNullValue(column.column_type, resolved);
  return column.nullable ? concatBytes([Uint8Array.of(1), bytes]) : bytes;
}

function encodeNonNullValue(type: ColumnType, value: Value): Uint8Array {
  const view = new DataView(new ArrayBuffer(8));
  switch (type.type) {
    case "Boolean":
      return Uint8Array.of(value.type === "Boolean" && value.value ? 1 : 0);
    case "Integer":
      view.setUint32(0, expectNumber(value, "Integer"), true);
      return new Uint8Array(view.buffer, 0, 8);
    case "BigInt":
    case "Timestamp":
      view.setBigUint64(0, BigInt(expectNumber(value, type.type)), true);
      return new Uint8Array(view.buffer);
    case "Double":
      view.setFloat64(0, expectNumber(value, "Double"), true);
      return new Uint8Array(view.buffer);
    case "Text":
    case "Json":
    case "Enum":
      return textEncoder.encode(expectString(value, type.type));
    case "Uuid":
      return parseUuid(expectString(value, "Uuid"));
    case "Bytea":
      if (value.type !== "Bytea") throw new Error("expected Bytea value");
      return value.value;
    case "Array":
    case "Row":
      throw new Error(`Direct WasmDb runtime does not encode ${type.type} values yet`);
  }
}

function expectNumber(value: Value, type: string): number {
  if (
    (value.type === "Integer" || value.type === "BigInt" || value.type === "Double" || value.type === "Timestamp") &&
    typeof value.value === "number"
  ) {
    return value.value;
  }
  throw new Error(`expected ${type} value`);
}

function expectString(value: Value, type: string): string {
  if ((value.type === "Text" || value.type === "Uuid") && typeof value.value === "string") {
    return value.value;
  }
  throw new Error(`expected ${type} value`);
}

function readRowBatches(payload: Uint8Array): AbiRowBatch[] {
  return new PostcardReader(payload).readVec(readAbiRowBatch);
}

function rowsFromBatches(batches: AbiRowBatch[], schema: WasmSchema): RowState[] {
  return batches.flatMap((batch) => batch.rows.map((row) => ({
    table: batch.table,
    id: formatUuid(row.rowId),
    values: batch.descriptor
      .filter((field) => field.name && !isInternalField(field.name))
      .map((field, index) => decodeField(batch.table, field, batch.descriptor, row.raw, index, schema)),
  })));
}

function decodeField(
  table: string,
  field: DescriptorField,
  descriptor: DescriptorField[],
  raw: Uint8Array,
  index: number,
  schema: WasmSchema,
): Value {
  const column = schema[table]?.columns.find((candidate) => candidate.name === publicFieldName(field.name ?? ""));
  const type = column?.column_type;
  const bytes = decodeRecordBytes(descriptor, raw, index);
  if (!type) return { type: "Bytea", value: bytes };
  return decodeBytes(type, bytes);
}

function decodeBytes(type: ColumnType, bytes: Uint8Array): Value {
  const view = new DataView(bytes.buffer, bytes.byteOffset, bytes.byteLength);
  switch (type.type) {
    case "Boolean":
      return { type: "Boolean", value: bytes[0] !== 0 };
    case "Integer":
      return { type: "Integer", value: view.getUint32(0, true) };
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
    case "Row":
      return { type: "Bytea", value: bytes.slice() };
  }
}

function normalizeSubscriptionChunk(chunk: unknown):
  | { type: "snapshot"; rows: AbiRowBatch[] }
  | { type: "delta"; delta: { added: AbiRowBatch[]; updated: AbiRowBatch[]; removed: unknown[] } } {
  if (!chunk || typeof chunk !== "object") throw new Error("expected subscription chunk");
  const record = chunk as { type?: unknown; rows?: unknown; delta?: unknown };
  if (record.type === "snapshot" || record.type === "Snapshot") {
    return { type: "snapshot", rows: readRowBatches(assertBytes(record.rows, "subscription rows")) };
  }
  if (record.type === "delta" || record.type === "Delta") {
    return { type: "delta", delta: readAbiSubscriptionDelta(new PostcardReader(assertBytes(record.delta, "subscription delta"))) };
  }
  throw new Error("unknown subscription chunk");
}

function nativeDeltaFromRows(rows: RowState[]): SubscriptionWireDelta {
  return rows.map((row, index) => ({
    kind: 0,
    id: row.id,
    index,
    row: { id: row.id, values: row.values },
  }));
}

function readValueType(type: ColumnType): ValueType {
  return columnTypeToValueType(type);
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
      return { tag: 11, inner: readValueType(type.element) };
    case "Row":
      throw new Error("Direct WasmDb runtime does not encode nested row columns yet");
  }
}

function writeValueType(writer: PostcardWriter, valueType: ValueType): void {
  writer.enumUnit(valueType.tag);
  if ((valueType.tag === 10 || valueType.tag === 11 || valueType.tag === 12) && valueType.inner) {
    writeValueType(writer, valueType.inner);
  }
}

function parseUuid(value: string): Uint8Array {
  const hex = value.replaceAll("-", "");
  if (!/^[0-9a-fA-F]{32}$/.test(hex)) throw new Error(`invalid uuid ${value}`);
  const bytes = new Uint8Array(16);
  for (let i = 0; i < 16; i += 1) {
    bytes[i] = Number.parseInt(hex.slice(i * 2, i * 2 + 2), 16);
  }
  return bytes;
}

function formatUuid(bytes: Uint8Array): string {
  const hex = Array.from(bytes.subarray(0, 16), (byte) => byte.toString(16).padStart(2, "0")).join("");
  return `${hex.slice(0, 8)}-${hex.slice(8, 12)}-${hex.slice(12, 16)}-${hex.slice(16, 20)}-${hex.slice(20)}`;
}

function bytesKey(bytes: Uint8Array): string {
  return Array.from(bytes, (byte) => String.fromCharCode(byte)).join("");
}

function concatBytes(chunks: Uint8Array[]): Uint8Array {
  const out = new Uint8Array(chunks.reduce((sum, chunk) => sum + chunk.length, 0));
  let offset = 0;
  for (const chunk of chunks) {
    out.set(chunk, offset);
    offset += chunk.length;
  }
  return out;
}

function publicFieldName(name: string): string {
  return name.startsWith("user_") ? name.slice("user_".length) : name;
}

function isInternalField(name?: string): boolean {
  return name === "row_uuid" || name === "tx_node_id" || name === "tx_time";
}

function assertBytes(value: unknown, label: string): Uint8Array {
  if (value instanceof Uint8Array) return value;
  if (Array.isArray(value)) return Uint8Array.from(value);
  throw new Error(`expected ${label} bytes`);
}
