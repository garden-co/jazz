import { type Bytes, toHex } from "../../support/bytes.js";
import {
  type RecordDescriptor,
  type Value,
  type IndexIdRegistry,
  type PreparedShapeId,
  type QueryParameter,
  type TickMetrics,
  type DatabaseSchema,
  OwnedRecord,
  VariantRecord,
  ensureValueType,
  recordsArityMismatch,
  splitVariantRecord,
  encodeVariantRecord,
  splitDurableIndexKey,
} from "../../_extern/groove-db-storage.js";
import {
  type KeyValue,
  type ScanRequest,
  type StorageCursor,
  type WriteOperation,
  type LayoutStorage,
  OrderedKvStorage,
  RecordStore,
} from "../storage/index.js";
import { DbError } from "./index.js";
import { decodePrimaryKeyPart, encodePrimaryKeyPart } from "./encoding.js";

export type LockRelease = () => void;
/** futures::lock::Mutex<()> stand-in with owned guards. */
export class AsyncLock {
  private locked = false;
  private waiters: (() => void)[] = [];
  isLocked() {
    return this.locked;
  }
  tryLock(): LockRelease | undefined {
    if (this.locked) return undefined;
    this.locked = true;
    return this.releaser();
  }
  async lock(): Promise<LockRelease> {
    while (this.locked) await new Promise<void>((r) => this.waiters.push(r));
    this.locked = true;
    return this.releaser();
  }
  private releaser(): LockRelease {
    let released = false;
    return () => {
      if (released) return;
      released = true;
      this.locked = false;
      this.waiters.shift()?.();
    };
  }
}

/** Typed facade over one schema-declared direct record store. */
export class DirectRecordStore {
  constructor(
    readonly storage: LayoutStorage,
    readonly name: string,
    readonly keyDescriptor: RecordDescriptor,
    readonly valueDescriptor: RecordDescriptor,
  ) {}

  async set(key: Value[], value: Value[]): Promise<void> {
    const op: WriteOperation = {
      kind: "Set",
      cf: this.name,
      key: this.keyBytes(key),
      value: this.create(value),
    };
    await this.write([op]);
  }
  async get(key: Value[]) {
    const raw = await this.recordStore().getRaw(this.keyBytes(key));
    return raw && this.valueDescriptor.bindOwned(raw);
  }
  async delete(key: Value[]): Promise<void> {
    await this.write([{ kind: "Delete", cf: this.name, key: this.keyBytes(key) }]);
  }
  async range(start: Value[], end: Value[]) {
    const rows = await this.recordStore().range(
      this.keyPrefixBytes(start),
      this.keyPrefixBytes(end),
    );
    return rows.map(([, v]) => this.valueDescriptor.bindOwned(v));
  }
  async rangeEntries(start: Value[], end: Value[]): Promise<DirectRecordStoreEntry[]> {
    const rows = await this.recordStore().range(
      this.keyPrefixBytes(start),
      this.keyPrefixBytes(end),
    );
    return rows.map(([k, v]) => ({
      key: this.decodeKey(k),
      value: this.valueDescriptor.bindOwned(v),
    }));
  }
  async prefix(prefix: Value[]) {
    const rows = await this.recordStore().prefix(this.keyPrefixBytes(prefix));
    return rows.map(([, v]) => this.valueDescriptor.bindOwned(v));
  }
  async prefixEntries(prefix: Value[]): Promise<DirectRecordStoreEntry[]> {
    const rows = await this.recordStore().prefix(this.keyPrefixBytes(prefix));
    return rows.map(([k, v]) => ({
      key: this.decodeKey(k),
      value: this.valueDescriptor.bindOwned(v),
    }));
  }
  writeMany(operations: DirectRecordStoreWrite[]) {
    return this.write(this.encodeWrites(operations));
  }
  private async write(ops: WriteOperation[]) {
    try {
      await this.storage.writeMany(ops);
    } catch (error) {
      throw DbError.from(error);
    }
  }
  private create(values: Value[]): Bytes {
    try {
      return this.valueDescriptor.create(values);
    } catch (error) {
      throw DbError.from(error);
    }
  }
  encodeWrites(operations: DirectRecordStoreWrite[]): WriteOperation[] {
    return operations.map((op) =>
      op.kind === "Set"
        ? { kind: "Set", cf: this.name, key: this.keyBytes(op.key), value: this.create(op.value) }
        : { kind: "Delete", cf: this.name, key: this.keyBytes(op.key) },
    );
  }
  keyBytes(values: Value[]): Bytes {
    const expected = this.keyDescriptor.fields().length;
    if (values.length !== expected)
      throw DbError.from(recordsArityMismatch(expected, values.length));
    return this.keyPrefixBytes(values);
  }
  keyPrefixBytes(values: Value[]): Bytes {
    const fields = this.keyDescriptor.fields();
    if (values.length > fields.length)
      throw DbError.from(recordsArityMismatch(fields.length, values.length));
    const out: number[] = [];
    values.forEach((value, i) => {
      try {
        ensureValueType(value, fields[i].valueType);
      } catch (error) {
        throw DbError.from(error);
      }
      encodePrimaryKeyPart(out, value);
    });
    return Uint8Array.from(out);
  }
  recordStore() {
    return new RecordStore(this.storage, this.name, this.valueDescriptor);
  }
  decodeKey(key: Bytes): Value[] {
    const cursor = { bytes: key, pos: 0 };
    const values = this.keyDescriptor
      .fields()
      .map((f: any) => decodePrimaryKeyPart(cursor, f.valueType));
    if (cursor.pos !== key.length) throw new DbError("InvalidDirectRecordStoreKey", this.name);
    return values;
  }
}

export interface DirectRecordStoreEntry {
  key: Value[];
  value: any;
}

export type DirectRecordStoreWrite =
  | { kind: "Set"; key: Value[]; value: Value[] }
  | { kind: "Delete"; key: Value[] };

/** Prepared parameterized subscription shape produced from a SQL-ish query. */
export class PreparedShape {
  constructor(
    readonly id: PreparedShapeId,
    readonly parameters: QueryParameter[],
    readonly output: RecordDescriptor,
  ) {}
}

/** Timing and write-size split for the most recent committed batch. */
export interface CommitMetrics {
  storageWriteTime: number;
  ivmTickTime: number;
  storageWriteCount: number;
  storageWriteBytes: number;
  storageWrites: StorageWriteMetrics;
  tick: TickMetrics;
}

export interface StorageWriteBucket {
  count: number;
  bytes: number;
}
export interface StorageReadBucket {
  reads: number;
  ranges: number;
}

type WriteDestination =
  | "historyRows"
  | "historyIndexes"
  | "globalCurrentRows"
  | "globalCurrentIndexes"
  | "registerGlobalCurrentRows"
  | "globalChangesRows"
  | "globalChangesIndexes"
  | "transactionsRows"
  | "transactionsIndexes"
  | "other";
type ReadDestination = WriteDestination | "aheadCurrentRows";

const WRITE_BUCKETS: WriteDestination[] = [
  "historyRows",
  "historyIndexes",
  "globalCurrentRows",
  "globalCurrentIndexes",
  "registerGlobalCurrentRows",
  "globalChangesRows",
  "globalChangesIndexes",
  "transactionsRows",
  "transactionsIndexes",
  "other",
];

/** Durable storage-write counts split by stable Jazz logical destinations. */
export class StorageWriteMetrics {
  total: StorageWriteBucket = { count: 0, bytes: 0 };
  [bucket: string]: any;
  constructor() {
    for (const b of WRITE_BUCKETS) this[b] = { count: 0, bytes: 0 };
  }
  static fromOperations(
    operations: WriteOperation[],
    indexIds: IndexIdRegistry,
  ): StorageWriteMetrics {
    const metrics = new StorageWriteMetrics();
    for (const op of operations) metrics.record(op, indexIds);
    return metrics;
  }
  record(op: WriteOperation, indexIds: IndexIdRegistry) {
    const bytes = writeOperationBytes(op);
    for (const bucket of [this.total, this[storageWriteDestination(op, indexIds)]]) {
      bucket.count++;
      bucket.bytes += bytes;
    }
  }
}

/** Durable storage-read counts split by stable Jazz logical destinations. */
export type StorageReadMetrics = Record<"total" | ReadDestination, StorageReadBucket>;
export function emptyReadMetrics(): StorageReadMetrics {
  const m: any = { total: { reads: 0, ranges: 0 }, aheadCurrentRows: { reads: 0, ranges: 0 } };
  for (const b of WRITE_BUCKETS) m[b] = { reads: 0, ranges: 0 };
  return m;
}

/** A database's read counters plus the index-id registry naming durable indexes. */
export class ReadMetricsSink {
  metrics = emptyReadMetrics();
  constructor(readonly indexIds: { value: IndexIdRegistry }) {}
  private record(cf: string, key: Bytes, reads: number, ranges: number) {
    const destination = storageReadDestination(cf, key, this.indexIds.value);
    for (const bucket of [this.metrics.total, this.metrics[destination]]) {
      bucket.reads += reads;
      bucket.ranges += ranges;
    }
  }
  recordPoint(cf: string, key: Bytes) {
    this.record(cf, key, 1, 1);
  }
  recordRange(cf: string, key: Bytes) {
    this.record(cf, key, 0, 1);
  }
  recordRangeRow(cf: string, key: Bytes) {
    this.record(cf, key, 1, 0);
  }
}

class MeteredStorageCursor implements StorageCursor {
  constructor(
    private readonly inner: StorageCursor,
    private readonly cf: string,
    private readonly metrics: ReadMetricsSink,
  ) {}
  async nextBatch() {
    const batch = await this.inner.nextBatch();
    if (batch) for (const [key] of batch) this.metrics.recordRangeRow(this.cf, key);
    return batch;
  }
}

/** Storage view counting logical reads by destination. */
export class MeteredStorage extends OrderedKvStorage {
  constructor(
    readonly storage: OrderedKvStorage,
    readonly metrics: ReadMetricsSink,
  ) {
    super();
  }
  override permitsEagerReadRetry() {
    return this.storage.permitsEagerReadRetry();
  }
  async scan(request: ScanRequest) {
    const metricKey =
      request.bounds.kind === "Prefix" ? request.bounds.prefix : request.bounds.start;
    this.metrics.recordRange(request.cf, metricKey);
    return new MeteredStorageCursor(await this.storage.scan(request), request.cf, this.metrics);
  }
  override close() {
    return this.storage.close();
  }
  override setWriteFlushCadence(every: number) {
    return this.storage.setWriteFlushCadence(every);
  }
  override flushWriteBoundary() {
    return this.storage.flushWriteBoundary();
  }
  override approximateClassBytes(cf: string) {
    return this.storage.approximateClassBytes(cf);
  }
  override columnFamilyNames() {
    return this.storage.columnFamilyNames();
  }
  override compareValue(cf: string, key: Bytes, expected: Bytes) {
    this.metrics.recordPoint(cf, key);
    return this.storage.compareValue(cf, key, expected);
  }
  get(cf: string, key: Bytes) {
    this.metrics.recordPoint(cf, key);
    return this.storage.get(cf, key);
  }
  putIfAbsent(cf: string, key: Bytes, value: Bytes) {
    this.metrics.recordPoint(cf, key);
    return this.storage.putIfAbsent(cf, key, value);
  }
  compareAndDelete(cf: string, key: Bytes, expected: Bytes) {
    this.metrics.recordPoint(cf, key);
    return this.storage.compareAndDelete(cf, key, expected);
  }
  set(cf: string, key: Bytes, value: Bytes) {
    return this.storage.set(cf, key, value);
  }
  delete(cf: string, key: Bytes) {
    return this.storage.delete(cf, key);
  }
  override async lastWithPrefix(cf: string, prefix: Bytes) {
    this.metrics.recordRange(cf, prefix);
    const value = await this.storage.lastWithPrefix(cf, prefix);
    if (value) this.metrics.recordRangeRow(cf, value[0]);
    return value;
  }
  override async lastWithPrefixBeforeOrAt(cf: string, prefix: Bytes, upper: Bytes) {
    this.metrics.recordRange(cf, prefix);
    const value = await this.storage.lastWithPrefixBeforeOrAt(cf, prefix, upper);
    if (value) this.metrics.recordRangeRow(cf, value[0]);
    return value;
  }
  writeMany(operations: WriteOperation[]) {
    return this.storage.writeMany(operations);
  }
}

/** Owned raw database entry with a lazy encoded-record view over the value. */
export class EncodedKeyValue {
  constructor(
    readonly key: Bytes,
    readonly variantRecord: VariantRecord,
  ) {}
  static create(key: Bytes, value: Bytes, descriptor: RecordDescriptor) {
    return new EncodedKeyValue(key, new VariantRecord(0, new OwnedRecord(value, descriptor)));
  }
  raw(): Bytes {
    return this.variantRecord.record().raw();
  }
  variantTag(): number {
    return this.variantRecord.variantTag();
  }
  intoParts(): KeyValue {
    return [this.key, this.raw()];
  }
  record() {
    return this.variantRecord.record().borrowed();
  }
  ownedRecord() {
    return this.variantRecord.record();
  }
}

export const writeOperationBytes = (op: WriteOperation) =>
  op.kind === "Set" ? op.key.length + op.value.length : op.key.length;

export function storageWriteDestination(
  op: WriteOperation,
  indexIds: IndexIdRegistry,
): WriteDestination {
  return op.cf === "indices"
    ? storageIndexWriteDestination(op.key, indexIds)
    : storageTableWriteDestination(op.cf);
}

export function storageTableWriteDestination(table: string): WriteDestination {
  if (table === "jazz_global_changes") return "globalChangesRows";
  if (table === "jazz_transactions") return "transactionsRows";
  const jazz = table.startsWith("jazz_");
  if (jazz && table.endsWith("_register_global_current") && !table.includes("_ahead_current"))
    return "registerGlobalCurrentRows";
  if (
    jazz &&
    table.endsWith("_global_current") &&
    !table.includes("_register_global_current") &&
    !table.includes("_ahead_current")
  )
    return "globalCurrentRows";
  if (jazz && table.endsWith("_history")) return "historyRows";
  return "other";
}

export function storageIndexWriteDestination(
  key: Bytes,
  indexIds: IndexIdRegistry,
): WriteDestination {
  const names = durableIndexTableAndName(key, indexIds);
  if (!names) return "other";
  const [table, index] = names;
  if (
    table === "jazz_global_changes" &&
    (index === "by_global_time" || index === "by_table_global_time")
  )
    return "globalChangesIndexes";
  if (table === "jazz_transactions" && index === "by_global_time") return "transactionsIndexes";
  if (
    table.startsWith("jazz_") &&
    table.endsWith("_global_current") &&
    !table.includes("_register_global_current") &&
    (index.startsWith("by_app_") ||
      index.startsWith("by_physical_app_") ||
      index.startsWith("by_physical_composite_"))
  )
    return "globalCurrentIndexes";
  if (table.startsWith("jazz_") && table.endsWith("_history") && index === "by_tx")
    return "historyIndexes";
  return "other";
}

export function storageReadDestination(
  cf: string,
  key: Bytes,
  indexIds: IndexIdRegistry,
): ReadDestination {
  if (cf === "indices") {
    const d = storageIndexWriteDestination(key, indexIds);
    return d.endsWith("Indexes") ? d : "other";
  }
  if (
    cf.startsWith("jazz_") &&
    (cf.endsWith("_ahead_current") || cf.endsWith("_register_ahead_current"))
  )
    return "aheadCurrentRows";
  const d = storageTableWriteDestination(cf);
  return d === "globalCurrentIndexes" ? "other" : d;
}

/** The table and index a durable index key belongs to, via its numeric id. */
export function durableIndexTableAndName(
  key: Bytes,
  indexIds: IndexIdRegistry,
): [string, string] | undefined {
  const split = splitDurableIndexKey(key);
  return split && indexIds.names(split[0]);
}

export type WriteMode = "Insert" | "InsertFresh" | "Update";

/** Insert and update share one storage operation; deltas decide retraction. */
export type PendingTableWrite =
  | {
      kind: "Set";
      mode: WriteMode;
      table: string;
      key: Bytes;
      variantTag: number;
      descriptor: RecordDescriptor;
      record: Bytes;
    }
  | { kind: "Delete"; table: string; key: Bytes; descriptor: RecordDescriptor };

export const storedRecord = (w: PendingTableWrite): Bytes | undefined =>
  w.kind === "Set" ? encodeVariantRecord(w.variantTag, w.record) : undefined;

/** Compute homogeneous per-(table, variant) deltas, observing earlier writes in the batch. */
export async function computeTableDeltas(
  pendingWrites: PendingTableWrite[],
  stores: RecordStore[],
  schema: DatabaseSchema,
): Promise<any[]> {
  const overlay = new Map<string, [number, Bytes] | undefined>();
  const byTable = new Map<
    string,
    {
      table: string;
      variantTag: number;
      descriptor: RecordDescriptor;
      records: Map<string, [Bytes, number]>;
    }
  >();
  const oldDescriptors = new Map<string, RecordDescriptor>();
  const bump = (
    table: string,
    variantTag: number,
    descriptor: RecordDescriptor,
    record: Bytes,
    weight: number,
  ) => {
    const groupKey = `${table}\0${variantTag}\0${descriptor.id ?? ""}`;
    let group = byTable.get(groupKey);
    if (!group)
      byTable.set(groupKey, (group = { table, variantTag, descriptor, records: new Map() }));
    const rk = toHex(record);
    const entry = group.records.get(rk);
    if (entry) entry[1] += weight;
    else group.records.set(rk, [record, weight]);
  };

  for (let i = 0; i < pendingWrites.length; i++) {
    const write = pendingWrites[i];
    const overlayKey = `${write.table}\0${toHex(write.key)}`;
    let current: [number, Bytes] | undefined;
    if (overlay.has(overlayKey)) current = overlay.get(overlayKey);
    else if (!(write.kind === "Set" && write.mode === "InsertFresh")) {
      const stored = await stores[i].getRaw(write.key);
      try {
        current = stored && splitVariantRecord(stored);
      } catch (error) {
        throw DbError.from(error);
      }
    }
    if (write.kind === "Set" && write.mode === "Insert" && current)
      throw new DbError("DuplicatePrimaryKey", { table: write.table, key: write.key });
    const tableSchema = schema.table(write.table);
    if (!tableSchema) throw new DbError("TableNotFound", write.table);
    if (current) {
      const [variantTag, payload] = current;
      const dk = `${write.table}\0${variantTag}`;
      let descriptor = oldDescriptors.get(dk);
      if (!descriptor) {
        descriptor = tableSchema.recordSchemaForVariant(variantTag);
        if (!descriptor)
          throw new DbError("UnknownTableVariant", {
            table: tableSchema.name,
            version: variantTag,
          });
        oldDescriptors.set(dk, descriptor);
      }
      bump(write.table, variantTag, descriptor!, payload, -1);
    }
    if (write.kind === "Set")
      bump(write.table, write.variantTag, write.descriptor, write.record, 1);
    overlay.set(overlayKey, write.kind === "Set" ? [write.variantTag, write.record] : undefined);
  }

  const out: any[] = [];
  for (const { table, variantTag, descriptor, records } of byTable.values()) {
    const deltas = [...records.values()]
      .filter(([, w]) => w !== 0)
      .map(([record, weight]) => ({ record, weight }));
    if (deltas.length) out.push({ table, variantTag, descriptor, deltas });
  }
  return out;
}
