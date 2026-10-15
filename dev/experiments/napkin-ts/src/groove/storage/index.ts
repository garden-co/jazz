// Ordered key/value storage seam used behind record stores.
import { type Bytes, compareBytes, equalBytes, utf8, concatBytes } from "../../support/bytes.js";
import { SortedMap, cmpBytes, cmpStr } from "../../support/collections.js";
import type { RecordDescriptor, Record } from "../../_extern/groove-db-storage.js";
import { incrementBytes } from "./key_codec.js";

export { IdbStorage } from "./idb.js";
export {
  type AdapterFormat,
  GROOVE_DURABLE_INDEX_V2_CODEC,
  type ManifestOpenReceipt,
  type MigrationJournal,
  MigrationRegistry,
  STORAGE_EPOCH_1,
  StorageCodecProfile,
  StorageEpochManifest,
  type StorageMigration,
} from "./manifest.js";
export { MemoryStorage } from "./memory.js";

export type KeyValue = [Bytes, Bytes];

// ---- errors ----

export type StorageErrorKind =
  | "ColumnFamilyNotFound"
  | "InvalidStorageLayout"
  | "UnsupportedStorageCodecs"
  | "InvalidStorageKey"
  | "ConditionalMutationInTransaction"
  | "IdbGenerationContention"
  | "Record"
  | "Backend"
  | "IdbTree";

export class StorageError extends Error {
  constructor(
    readonly kind: StorageErrorKind,
    message: string,
    readonly data: any = undefined,
  ) {
    super(message);
  }
  static columnFamilyNotFound(cf: string) {
    return new StorageError("ColumnFamilyNotFound", `column family not found: ${cf}`, cf);
  }
  static invalidLayout(msg: string) {
    return new StorageError("InvalidStorageLayout", `invalid storage layout: ${msg}`, msg);
  }
  static unsupportedCodecs(epoch: number, missing: string[], unknown: string[]) {
    return new StorageError(
      "UnsupportedStorageCodecs",
      `unsupported storage format: this epoch-${epoch} root lacks codec families ${JSON.stringify(missing)} required by this build and declares ${JSON.stringify(unknown)} that this build does not read`,
      { epoch, missing, unknown },
    );
  }
  static invalidKey(msg: string) {
    return new StorageError("InvalidStorageKey", `invalid storage key: ${msg}`, msg);
  }
  static conditionalInTxn() {
    return new StorageError(
      "ConditionalMutationInTransaction",
      "conditional mutations require a direct storage boundary",
    );
  }
  static idbContention(retries: number) {
    return new StorageError(
      "IdbGenerationContention",
      `IndexedDB storage remained contended after ${retries} generation-conflict retries`,
      { retries },
    );
  }
  static record(error: any) {
    return new StorageError("Record", `record error: ${error}`, error);
  }
  static backend(backend: string, message: string) {
    return new StorageError("Backend", `${backend} storage error: ${message}`, {
      backend,
      message,
    });
  }
  static idbTree(error: any) {
    return new StorageError("IdbTree", String(error?.message ?? error), error);
  }
}

export function prefixSuccessor(prefix: Bytes): Bytes | undefined {
  return incrementBytes(prefix);
}

export const MAX_APPLICATION_STORAGE_NAME_BYTES = 0xffff;

export function validatePhysicalStorageName(name: string): void {
  const len = utf8(name).length;
  if (len > MAX_APPLICATION_STORAGE_NAME_BYTES)
    throw StorageError.invalidLayout(
      `storage name exceeds the ${MAX_APPLICATION_STORAGE_NAME_BYTES}-byte limit: ${len} bytes`,
    );
  if (name.includes("\0"))
    throw StorageError.invalidLayout("storage name contains an embedded NUL");
}

export function validatePhysicalStorageNames(names: Iterable<string>): void {
  for (const name of names) validatePhysicalStorageName(name);
}

export function validateApplicationStorageName(name: string): void {
  validatePhysicalStorageName(name);
  if (name.startsWith("__groove_") || name === "indices" || name === "default")
    throw StorageError.invalidLayout(
      `application storage name is reserved by Groove: ${JSON.stringify(name)}`,
    );
}

// ---- scans ----

export type ScanBounds =
  | { kind: "Prefix"; prefix: Bytes }
  | { kind: "Range"; start: Bytes; end: Bytes };
export const isEmptyRange = (b: ScanBounds) =>
  b.kind === "Range" && compareBytes(b.start, b.end) >= 0;

export type ScanDirection = "Forward" | "Reverse";

export interface ScanRequest {
  cf: string;
  bounds: ScanBounds;
  direction: ScanDirection;
  maxItems?: number;
}
export const scanPrefix = (cf: string, prefix: Bytes): ScanRequest => ({
  cf,
  bounds: { kind: "Prefix", prefix },
  direction: "Forward",
});
export const scanRange = (cf: string, start: Bytes, end: Bytes): ScanRequest => ({
  cf,
  bounds: { kind: "Range", start, end },
  direction: "Forward",
});

export interface StorageCursor {
  /** An empty batch is not an end marker; only `undefined` completes the scan. */
  nextBatch(): Promise<KeyValue[] | undefined>;
}

export class ReadyStorageCursor implements StorageCursor {
  private pos = 0;
  constructor(private readonly values: KeyValue[]) {}
  async nextBatch() {
    const batch = this.values.slice(this.pos, this.pos + 256);
    this.pos += batch.length;
    return batch.length ? batch : undefined;
  }
}

export async function collectScan(scan: StorageCursor): Promise<KeyValue[]> {
  const values: KeyValue[] = [];
  for (let batch; (batch = await scan.nextBatch()); ) values.push(...batch);
  return values;
}

const STAGED_POINT_READS_BEFORE_INDEX = 16;
const STAGED_OPS_BEFORE_POINT_INDEX = 64;

export type ValueComparison = "Absent" | "Identical" | "Different";

export type WriteOperation =
  | { kind: "Set"; cf: string; key: Bytes; value: Bytes }
  | { kind: "Delete"; cf: string; key: Bytes };

/** Settled acknowledgement of one atomic `writeMany` attempt. */
export type WriteManyOutcome =
  | { kind: "Committed" }
  | { kind: "Uncommitted"; error: StorageError }
  | { kind: "PossiblyCommitted"; error: StorageError };
export const COMMITTED: WriteManyOutcome = { kind: "Committed" };
export const outcomeError = (o: WriteManyOutcome) => (o.kind === "Committed" ? undefined : o.error);
export const mayHaveCommitted = (o: WriteManyOutcome) => o.kind !== "Uncommitted";

let nextCacheToken = 1;

/** Backing-implementation interface for ordered key/value storage. */
export abstract class OrderedKvStorage {
  private token = 0;
  permitsEagerReadRetry(): boolean {
    return false;
  }
  beginTxn(): StorageTransaction {
    return new StorageTransaction(this);
  }
  abstract get(cf: string, key: Bytes): Promise<Bytes | undefined>;
  async compareValue(cf: string, key: Bytes, expected: Bytes): Promise<ValueComparison> {
    const value = await this.get(cf, key);
    if (!value) return "Absent";
    return equalBytes(value, expected) ? "Identical" : "Different";
  }
  abstract putIfAbsent(cf: string, key: Bytes, value: Bytes): Promise<Bytes | undefined>;
  abstract compareAndDelete(cf: string, key: Bytes, expected: Bytes): Promise<boolean>;
  abstract set(cf: string, key: Bytes, value: Bytes): Promise<void>;
  abstract delete(cf: string, key: Bytes): Promise<void>;
  async close(): Promise<void> {}
  async setWriteFlushCadence(_every: number): Promise<void> {}
  async flushWriteBoundary(): Promise<void> {}
  cacheToken(): number {
    return (this.token ||= nextCacheToken++);
  }
  async approximateClassBytes(_cf: string): Promise<number | undefined> {
    return undefined;
  }
  abstract scan(request: ScanRequest): Promise<StorageCursor>;
  async lastWithPrefix(cf: string, prefix: Bytes): Promise<KeyValue | undefined> {
    const req = scanPrefix(cf, prefix);
    req.direction = "Reverse";
    req.maxItems = 1;
    return (await collectScan(await this.scan(req)))[0];
  }
  async lastWithPrefixBeforeOrAt(
    cf: string,
    prefix: Bytes,
    upper: Bytes,
  ): Promise<KeyValue | undefined> {
    let last: KeyValue | undefined;
    for (const kv of await collectScan(await this.scan(scanPrefix(cf, prefix)))) {
      if (compareBytes(kv[0], upper) > 0) break;
      last = kv;
    }
    return last;
  }
  abstract writeMany(operations: WriteOperation[]): Promise<void>;
  async writeManyOutcome(operations: WriteOperation[]): Promise<WriteManyOutcome> {
    try {
      await this.writeMany(operations);
      return COMMITTED;
    } catch (error) {
      return { kind: "PossiblyCommitted", error: error as StorageError };
    }
  }
  writeManyBorrowedOutcome(operations: WriteOperation[]): Promise<WriteManyOutcome> {
    return this.writeManyOutcome(operations);
  }
  columnFamilyNames(): string[] | undefined {
    return undefined;
  }
  async range(cf: string, start: Bytes, end: Bytes): Promise<KeyValue[]> {
    return collectScan(await this.scan(scanRange(cf, start, end)));
  }
  async prefix(cf: string, prefix: Bytes): Promise<KeyValue[]> {
    return collectScan(await this.scan(scanPrefix(cf, prefix)));
  }
}

/** Storage that can be reconstructed with an expanded table/column-family set. */
export interface ReopenableStorage extends OrderedKvStorage {
  reopen(columnFamilies: string[]): Promise<ReopenableStorage>;
}
export type BoxedStorage = ReopenableStorage;

/** Opens a persistent ordered-KV backend at an exact target-owned path. */
export interface StorageFactory {
  open(path: string, columnFamilies: string[], codecProfile: any): Promise<BoxedStorage>;
}

/** Executor-local owned storage used by interruptible engine work. */
export class OwnedStorage {
  constructor(readonly storage: OrderedKvStorage) {}
  get(cf: string, key: Bytes) {
    return this.storage.get(cf, key);
  }
  async scan(request: ScanRequest) {
    return collectScan(await this.storage.scan(request));
  }
}

// ---- class-CF layout ----

const CLASS_HISTORY_CF = "__groove_class_history";
const CLASS_REGISTER_CF = "__groove_class_register";
const CLASS_GLOBAL_CURRENT_CF = "__groove_class_global_current";
const CLASS_AHEAD_CURRENT_CF = "__groove_class_ahead_current";
const CLASS_CHANGES_CF = "__groove_class_changes";
const CLASS_INDICES_CF = "__groove_class_indices";
const CLASS_META_CF = "__groove_class_meta";
const CLASS_LAYOUT_MARKER_KEY = utf8("groove-storage-layout");
const CLASS_LAYOUT_MARKER_VALUE = utf8("class-cf-v2");

export type StorageLayout = "Identity" | "JazzClassV2";

export function physicalColumnFamilies(layout: StorageLayout, logical: Iterable<string>): string[] {
  const names = new Set<string>();
  if (layout === "JazzClassV2") names.add(CLASS_META_CF);
  for (const l of logical) names.add(layout === "Identity" ? l : (jazzPhysicalClass(l) ?? l));
  return [...names].sort(cmpStr);
}

function mapCf(
  layout: StorageLayout,
  logicalCf: string,
): { physicalCf: string; logicalPrefix?: string } {
  validatePhysicalStorageName(logicalCf);
  if (layout === "Identity") return { physicalCf: logicalCf };
  const physicalCf = jazzPhysicalClass(logicalCf);
  if (!physicalCf) return { physicalCf: logicalCf };
  return { physicalCf, logicalPrefix: physicalCf !== CLASS_INDICES_CF ? logicalCf : undefined };
}

class MappedStorageCursor implements StorageCursor {
  constructor(
    private readonly inner: StorageCursor,
    private readonly stripLen: number,
  ) {}
  async nextBatch() {
    const batch = await this.inner.nextBatch();
    if (!batch) return undefined;
    return batch.map(([key, value]): KeyValue => {
      if (key.length < this.stripLen)
        throw StorageError.invalidKey("physical layout key shorter than logical prefix");
      return [key.slice(this.stripLen), value];
    });
  }
}

const isJazzHistoryTable = (n: string) => n.startsWith("jazz_") && n.endsWith("_history");
const isJazzRegisterTable = (n: string) =>
  n.startsWith("jazz_") &&
  n.endsWith("_register") &&
  !n.endsWith("_register_global_current") &&
  !n.endsWith("_register_ahead_current");
const isJazzGlobalCurrentTable = (n: string) =>
  n.startsWith("jazz_") &&
  (n.endsWith("_global_current") || n.endsWith("_register_global_current")) &&
  !n.includes("_ahead_current");
const isJazzAheadCurrentTable = (n: string) =>
  n.startsWith("jazz_") && (n.endsWith("_ahead_current") || n.endsWith("_register_ahead_current"));

function jazzPhysicalClass(cf: string): string | undefined {
  if (isJazzHistoryTable(cf)) return CLASS_HISTORY_CF;
  if (isJazzRegisterTable(cf)) return CLASS_REGISTER_CF;
  if (isJazzGlobalCurrentTable(cf)) return CLASS_GLOBAL_CURRENT_CF;
  if (isJazzAheadCurrentTable(cf)) return CLASS_AHEAD_CURRENT_CF;
  if (cf === "jazz_global_changes") return CLASS_CHANGES_CF;
  if (cf === "indices") return CLASS_INDICES_CF;
  if (cf.startsWith("jazz_")) return CLASS_META_CF;
  return undefined;
}

function framePrefix(logical: string): Bytes {
  const name = utf8(logical);
  const out = new Uint8Array(4 + name.length);
  new DataView(out.buffer).setUint32(0, name.length);
  out.set(name, 4);
  return out;
}

/** Keeps logical CF names at the database boundary over a physical class-CF layout. */
export class LayoutStorage extends OrderedKvStorage {
  private constructor(
    readonly inner: BoxedStorage,
    private readonly layout: StorageLayout,
  ) {
    super();
  }

  static async create(inner: BoxedStorage, layout: StorageLayout): Promise<LayoutStorage> {
    const storage = new LayoutStorage(inner, layout);
    await storage.ensureLayoutMarker();
    return storage;
  }

  async approximateClassBytesForLogicalFamilies(
    logical: Iterable<string>,
  ): Promise<number | undefined> {
    const physical = new Set<string>();
    for (const cf of logical) physical.add(mapCf(this.layout, cf).physicalCf);
    let bytes = 0;
    for (const cf of [...physical].sort(cmpStr)) {
      const classBytes = await this.inner.approximateClassBytes(cf);
      if (classBytes === undefined) return undefined;
      bytes += classBytes;
    }
    return bytes;
  }

  intoInner(): BoxedStorage {
    return this.inner;
  }

  private async ensureLayoutMarker(): Promise<void> {
    if (this.layout !== "JazzClassV2") return;
    const value = await this.inner.get(CLASS_META_CF, CLASS_LAYOUT_MARKER_KEY);
    if (value) {
      if (equalBytes(value, CLASS_LAYOUT_MARKER_VALUE)) return;
      throw StorageError.invalidLayout("unsupported class-CF storage layout marker");
    }
    if (await this.hasClassDataOrLegacyLayout())
      throw StorageError.invalidLayout("missing class-CF storage layout marker in non-empty store");
    await this.inner.set(CLASS_META_CF, CLASS_LAYOUT_MARKER_KEY, CLASS_LAYOUT_MARKER_VALUE);
  }

  private async hasClassDataOrLegacyLayout(): Promise<boolean> {
    if (this.layout === "JazzClassV2") {
      const names = this.inner.columnFamilyNames();
      if (!names)
        throw StorageError.invalidLayout(
          "class-CF v2 requires enumerable physical column families",
        );
      if (names.some((n) => jazzPhysicalClass(n) !== undefined)) return true;
    }
    for (const cf of [
      CLASS_HISTORY_CF,
      CLASS_REGISTER_CF,
      CLASS_GLOBAL_CURRENT_CF,
      CLASS_AHEAD_CURRENT_CF,
      CLASS_CHANGES_CF,
      CLASS_INDICES_CF,
      CLASS_META_CF,
    ]) {
      try {
        if (await this.inner.lastWithPrefix(cf, new Uint8Array())) return true;
      } catch (error) {
        if (!(error instanceof StorageError && error.kind === "ColumnFamilyNotFound")) throw error;
      }
    }
    return false;
  }

  private physicalKey(cf: string, key: Bytes): [string, Bytes] {
    const m = mapCf(this.layout, cf);
    if (m.logicalPrefix === undefined) return [m.physicalCf, key];
    return [m.physicalCf, concatBytes(framePrefix(m.logicalPrefix), key)];
  }

  private physicalPrefix(cf: string, prefix: Bytes): [string, Bytes, number] {
    const m = mapCf(this.layout, cf);
    if (m.logicalPrefix === undefined) return [m.physicalCf, prefix, 0];
    const frame = framePrefix(m.logicalPrefix);
    return [m.physicalCf, concatBytes(frame, prefix), frame.length];
  }

  private physicalOperations(ops: WriteOperation[]): WriteOperation[] {
    return ops.map((op) => {
      const [cf, key] = this.physicalKey(op.cf, op.key);
      return { ...op, cf, key };
    });
  }

  override permitsEagerReadRetry() {
    return this.inner.permitsEagerReadRetry();
  }
  override async compareValue(cf: string, key: Bytes, expected: Bytes) {
    const [pcf, pkey] = this.physicalKey(cf, key);
    return this.inner.compareValue(pcf, pkey, expected);
  }
  async get(cf: string, key: Bytes) {
    const [pcf, pkey] = this.physicalKey(cf, key);
    return this.inner.get(pcf, pkey);
  }
  async putIfAbsent(cf: string, key: Bytes, value: Bytes) {
    const [pcf, pkey] = this.physicalKey(cf, key);
    return this.inner.putIfAbsent(pcf, pkey, value);
  }
  async compareAndDelete(cf: string, key: Bytes, expected: Bytes) {
    const [pcf, pkey] = this.physicalKey(cf, key);
    return this.inner.compareAndDelete(pcf, pkey, expected);
  }
  async set(cf: string, key: Bytes, value: Bytes) {
    const [pcf, pkey] = this.physicalKey(cf, key);
    return this.inner.set(pcf, pkey, value);
  }
  async delete(cf: string, key: Bytes) {
    const [pcf, pkey] = this.physicalKey(cf, key);
    return this.inner.delete(pcf, pkey);
  }
  override close() {
    return this.inner.close();
  }
  override setWriteFlushCadence(every: number) {
    return this.inner.setWriteFlushCadence(every);
  }
  override flushWriteBoundary() {
    return this.inner.flushWriteBoundary();
  }
  async scan(request: ScanRequest): Promise<StorageCursor> {
    const { cf, bounds } = request;
    let physicalCf: string;
    let physicalBounds: ScanBounds;
    let stripLen: number;
    if (bounds.kind === "Prefix") {
      const [pcf, prefix, strip] = this.physicalPrefix(cf, bounds.prefix);
      [physicalCf, physicalBounds, stripLen] = [pcf, { kind: "Prefix", prefix }, strip];
    } else {
      const [pcf, start, strip] = this.physicalPrefix(cf, bounds.start);
      const [, end] = this.physicalPrefix(cf, bounds.end);
      [physicalCf, physicalBounds, stripLen] = [pcf, { kind: "Range", start, end }, strip];
    }
    const inner = await this.inner.scan({ ...request, cf: physicalCf, bounds: physicalBounds });
    return new MappedStorageCursor(inner, stripLen);
  }
  override async lastWithPrefix(cf: string, prefix: Bytes) {
    const [pcf, pprefix, strip] = this.physicalPrefix(cf, prefix);
    const req = scanPrefix(pcf, pprefix);
    req.direction = "Reverse";
    req.maxItems = 1;
    const kv = (await collectScan(await this.inner.scan(req)))[0];
    return kv && ([kv[0].slice(strip), kv[1]] as KeyValue);
  }
  override async lastWithPrefixBeforeOrAt(cf: string, prefix: Bytes, upper: Bytes) {
    const [pcf, pprefix, strip] = this.physicalPrefix(cf, prefix);
    const [, pupper] = this.physicalPrefix(cf, upper);
    const rows = await collectScan(await this.inner.scan(scanPrefix(pcf, pprefix)));
    for (let i = rows.length - 1; i >= 0; i--)
      if (compareBytes(rows[i][0], pupper) <= 0)
        return [rows[i][0].slice(strip), rows[i][1]] as KeyValue;
    return undefined;
  }
  async writeMany(operations: WriteOperation[]) {
    return this.inner.writeMany(this.physicalOperations(operations));
  }
  override async writeManyOutcome(operations: WriteOperation[]): Promise<WriteManyOutcome> {
    let mapped: WriteOperation[];
    try {
      mapped = this.physicalOperations(operations);
    } catch (error) {
      return { kind: "Uncommitted", error: error as StorageError };
    }
    return this.inner.writeManyOutcome(mapped);
  }
  override writeManyBorrowedOutcome(operations: WriteOperation[]) {
    return this.writeManyOutcome(operations);
  }
  override columnFamilyNames() {
    return this.inner.columnFamilyNames();
  }
  override async approximateClassBytes(cf: string) {
    return this.inner.approximateClassBytes(mapCf(this.layout, cf).physicalCf);
  }
}

// ---- typed record-store view ----

export class RecordStore {
  constructor(
    readonly storage: OrderedKvStorage,
    readonly columnFamily: string,
    readonly descriptor: RecordDescriptor,
  ) {}
  getRaw(key: Bytes) {
    return this.storage.get(this.columnFamily, key);
  }
  async get(key: Bytes): Promise<Record | undefined> {
    const raw = await this.getRaw(key);
    return raw && this.descriptor.bindOwned(raw);
  }
  range(start: Bytes, end: Bytes) {
    return this.storage.range(this.columnFamily, start, end);
  }
  prefix(prefix: Bytes) {
    return this.storage.prefix(this.columnFamily, prefix);
  }
  async rangeReverse(start: Bytes, end: Bytes) {
    return (await this.range(start, end)).reverse();
  }
  scan(bounds: ScanBounds) {
    return this.storage.scan({ cf: this.columnFamily, bounds, direction: "Forward" });
  }
  lastWithPrefix(prefix: Bytes) {
    return this.storage.lastWithPrefix(this.columnFamily, prefix);
  }
  lastWithPrefixBeforeOrAt(prefix: Bytes, upper: Bytes) {
    return this.storage.lastWithPrefixBeforeOrAt(this.columnFamily, prefix, upper);
  }
  async putIfAbsent(key: Bytes, record: Bytes): Promise<Record | undefined> {
    const existing = await this.storage.putIfAbsent(this.columnFamily, key, record);
    return existing && this.descriptor.bindOwned(existing);
  }
  set(key: Bytes, value: Bytes): WriteOperation {
    return { kind: "Set", cf: this.columnFamily, key, value };
  }
  delete(key: Bytes): WriteOperation {
    return { kind: "Delete", cf: this.columnFamily, key };
  }
  writeMany(ops: WriteOperation[]) {
    return this.storage.writeMany(ops);
  }
}

// ---- staged writes ----

export type StagedPointValue =
  | { kind: "Miss" }
  | { kind: "Set"; value: Bytes }
  | { kind: "Delete" };

/** Append-only staged operations with a lazily built latest-by-key index. */
export class StagedWriteState {
  // Blocks are immutable once shared via snapshot().
  private blocks: WriteOperation[][] = [];
  private ends: number[] = [];
  private sharedFrom = 0;
  private latest?: Map<string, SortedMap<Bytes, number>>;
  private pointReadsWithoutIndex = 0;

  static from(ops: Iterable<WriteOperation>): StagedWriteState {
    const s = new StagedWriteState();
    s.extend(ops);
    return s;
  }
  isEmpty() {
    return this.blocks.length === 0;
  }
  private count() {
    return this.ends.length ? this.ends[this.ends.length - 1] : 0;
  }
  private operation(index: number): WriteOperation {
    let lo = 0;
    let hi = this.ends.length;
    while (lo < hi) {
      const mid = (lo + hi) >>> 1;
      if (this.ends[mid] <= index) lo = mid + 1;
      else hi = mid;
    }
    const start = lo === 0 ? 0 : this.ends[lo - 1];
    return this.blocks[lo][index - start];
  }
  private indexInsert(op: WriteOperation, index: number) {
    let byKey = this.latest!.get(op.cf);
    if (!byKey) this.latest!.set(op.cf, (byKey = new SortedMap(cmpBytes)));
    byKey.set(op.key, index);
  }
  stage(op: WriteOperation) {
    const index = this.count();
    if (this.latest) this.indexInsert(op, index);
    if (this.blocks.length > this.sharedFrom) {
      this.blocks[this.blocks.length - 1].push(op);
      this.ends[this.ends.length - 1] = index + 1;
    } else {
      this.blocks.push([op]);
      this.ends.push(index + 1);
    }
  }
  extend(ops: Iterable<WriteOperation>) {
    for (const op of ops) this.stage(op);
  }
  *operations(): IterableIterator<WriteOperation> {
    for (const block of this.blocks) yield* block;
  }
  snapshot(): WriteOperation[][] {
    this.sharedFrom = this.blocks.length;
    return this.blocks.slice();
  }
  extendShared(snapshot: WriteOperation[][]) {
    for (const block of snapshot) {
      if (!block.length) continue;
      const start = this.count();
      if (this.latest) block.forEach((op, i) => this.indexInsert(op, start + i));
      this.ends.push(start + block.length);
      this.blocks.push(block);
    }
    this.sharedFrom = this.blocks.length;
  }
  intoOperations(): WriteOperation[] {
    return this.blocks.flat();
  }
  takeOperations(): WriteOperation[] {
    const ops = this.intoOperations();
    Object.assign(this, new StagedWriteState());
    return ops;
  }
  latestIndex(cf: string, key: Bytes): number | undefined {
    if (!this.latest) {
      if (
        this.count() < STAGED_OPS_BEFORE_POINT_INDEX &&
        this.pointReadsWithoutIndex < STAGED_POINT_READS_BEFORE_INDEX
      ) {
        this.pointReadsWithoutIndex++;
        for (let i = this.count() - 1; i >= 0; i--) {
          const op = this.operation(i);
          if (op.cf === cf && equalBytes(op.key, key)) return i;
        }
        return undefined;
      }
      this.ensureKeyIndex();
    }
    return this.latest!.get(cf)?.get(key);
  }
  private ensureKeyIndex() {
    if (this.latest) return;
    this.latest = new Map();
    let i = 0;
    for (const op of this.operations()) this.indexInsert(op, i++);
  }
  scanSnapshot(cf: string, bounds: ScanBounds): [Bytes, Bytes | undefined][] {
    if (this.isEmpty() || isEmptyRange(bounds)) return [];
    this.ensureKeyIndex();
    const byKey = this.latest!.get(cf);
    if (!byKey) return [];
    const [start, end] =
      bounds.kind === "Prefix"
        ? [bounds.prefix, prefixSuccessor(bounds.prefix)]
        : [bounds.start, bounds.end];
    const out: [Bytes, Bytes | undefined][] = [];
    for (const [key, index] of byKey.range(start, end)) {
      const op = this.operation(index);
      out.push([key, op.kind === "Set" ? op.value : undefined]);
    }
    return out;
  }
  pointValue(cf: string, key: Bytes): StagedPointValue {
    if (this.isEmpty()) return { kind: "Miss" };
    const index = this.latestIndex(cf, key);
    if (index === undefined) return { kind: "Miss" };
    const op = this.operation(index);
    return op.kind === "Set" ? { kind: "Set", value: op.value } : { kind: "Delete" };
  }
  containsKey(cf: string, key: Bytes) {
    return this.latestIndex(cf, key) !== undefined;
  }
}

/** Ordered cursor merging the durable base with staged transaction writes. */
class OverlayScanCursor implements StorageCursor {
  private baseEntries: KeyValue[] = [];
  private baseDone = false;
  constructor(
    private readonly base: StorageCursor,
    private readonly staged: [Bytes, Bytes | undefined][],
    private readonly direction: ScanDirection,
    private remaining: number | undefined,
  ) {}
  private async nextEntry(): Promise<KeyValue | undefined> {
    for (;;) {
      while (!this.baseEntries.length && !this.baseDone) {
        const batch = await this.base.nextBatch();
        if (batch) this.baseEntries.push(...batch);
        else this.baseDone = true;
      }
      const b = this.baseEntries[0];
      const s = this.staged[0];
      if (!b && !s) return undefined;
      let takeBase: boolean;
      let takeStaged: boolean;
      if (!s) [takeBase, takeStaged] = [true, false];
      else if (!b) [takeBase, takeStaged] = [false, true];
      else {
        const c = compareBytes(s[0], b[0]);
        if (c === 0) [takeBase, takeStaged] = [true, true];
        else {
          const stagedFirst = this.direction === "Forward" ? c < 0 : c > 0;
          [takeBase, takeStaged] = [!stagedFirst, stagedFirst];
        }
      }
      const baseEntry = takeBase ? this.baseEntries.shift() : undefined;
      const stagedEntry = takeStaged ? this.staged.shift() : undefined;
      if (stagedEntry) {
        if (stagedEntry[1]) return [stagedEntry[0], stagedEntry[1]];
      } else if (baseEntry) return baseEntry;
    }
  }
  async nextBatch() {
    const batchLen =
      this.remaining === undefined ? 256 : this.remaining > 0 ? Math.min(this.remaining, 256) : 0;
    if (!batchLen) return undefined;
    const values: KeyValue[] = [];
    while (values.length < batchLen) {
      const entry = await this.nextEntry();
      if (!entry) break;
      values.push(entry);
      if (this.remaining !== undefined) this.remaining--;
    }
    return values.length ? values : undefined;
  }
}

function overlayScan(
  base: OrderedKvStorage,
  staged: StagedWriteState,
  request: ScanRequest,
): Promise<StorageCursor> {
  // Snapshot only the final value per key at scan creation.
  const snapshot = staged.scanSnapshot(request.cf, request.bounds);
  return (async () => {
    // `limit + final removals` is a hard physical ceiling that still fills the logical result.
    const physicalMax =
      request.maxItems === undefined
        ? undefined
        : request.maxItems + snapshot.filter(([, v]) => v === undefined).length;
    const cursor = await base.scan({ ...request, maxItems: physicalMax });
    if (request.direction === "Reverse") snapshot.reverse();
    return new OverlayScanCursor(cursor, snapshot, request.direction, request.maxItems);
  })();
}

/** Read-your-writes view over a base storage and a staged write state. */
export class StagedWriteOverlay extends OrderedKvStorage {
  constructor(
    readonly base: OrderedKvStorage,
    readonly staged: StagedWriteState,
  ) {
    super();
  }
  stagedPointValue(cf: string, key: Bytes): StagedPointValue {
    return this.staged.pointValue(cf, key);
  }
  stage(op: WriteOperation) {
    this.staged.stage(op);
  }
  drainInto(target: WriteOperation[]) {
    target.push(...this.staged.takeOperations());
  }
  override async compareValue(cf: string, key: Bytes, expected: Bytes): Promise<ValueComparison> {
    const p = this.staged.pointValue(cf, key);
    if (p.kind === "Delete") return "Absent";
    if (p.kind === "Set") return equalBytes(p.value, expected) ? "Identical" : "Different";
    return this.base.compareValue(cf, key, expected);
  }
  override permitsEagerReadRetry() {
    return this.base.permitsEagerReadRetry();
  }
  async putIfAbsent(): Promise<Bytes | undefined> {
    throw StorageError.conditionalInTxn();
  }
  async compareAndDelete(): Promise<boolean> {
    throw StorageError.conditionalInTxn();
  }
  scan(request: ScanRequest) {
    return overlayScan(this.base, this.staged, request);
  }
  get(cf: string, key: Bytes) {
    const p = this.staged.pointValue(cf, key);
    if (p.kind === "Miss") return this.base.get(cf, key);
    return Promise.resolve(p.kind === "Set" ? p.value : undefined);
  }
  async set(cf: string, key: Bytes, value: Bytes) {
    this.stage({ kind: "Set", cf, key, value });
  }
  async delete(cf: string, key: Bytes) {
    this.stage({ kind: "Delete", cf, key });
  }
  async writeMany(ops: WriteOperation[]) {
    this.staged.extend(ops);
  }
  override approximateClassBytes(cf: string) {
    return this.base.approximateClassBytes(cf);
  }
  override columnFamilyNames() {
    return this.base.columnFamilyNames();
  }
}

/** Encoded storage transaction with read-your-own-writes semantics. */
export class StorageTransaction extends StagedWriteOverlay {
  constructor(base: OrderedKvStorage) {
    super(base, new StagedWriteState());
  }
  async commit(): Promise<void> {
    await this.base.writeMany(this.staged.intoOperations());
  }
  isEmpty() {
    return this.staged.isEmpty();
  }
  stageOwnedOperations(ops: Iterable<WriteOperation>) {
    this.staged.extend(ops);
  }
}
