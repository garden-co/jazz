// Async IDBTree adapter for Groove's ordered storage contract.
import { type Bytes, equalBytes, compareBytes } from "../../support/bytes.js";
import { yieldNow } from "../../support/runtime.js";
import {
  IdbTree,
  type PageStore,
  isIdbTreeGenerationConflict,
} from "../../_extern/groove-db-storage.js";
import {
  type KeyValue,
  type ReopenableStorage,
  type ScanRequest,
  type StorageCursor,
  type ValueComparison,
  type WriteManyOutcome,
  type WriteOperation,
  COMMITTED,
  OrderedKvStorage,
  ReadyStorageCursor,
  StorageError,
  isEmptyRange,
  prefixSuccessor,
  validatePhysicalStorageNames,
} from "./index.js";
import { decodeColumnFamilyKey, encodeColumnFamilyKey } from "./key_codec.js";

const MAX_GENERATION_CONFLICT_RETRIES = 8;
const MAX_CONFLICT_BACKOFF_YIELDS = 16;

type MaybePromise<T> = T | Promise<T>;
const isPromise = (v: unknown): v is Promise<unknown> => typeof (v as any)?.then === "function";

/** futures::lock::Mutex stand-in. */
class AsyncMutex {
  private locked = false;
  private waiters: (() => void)[] = [];
  isLocked() {
    return this.locked;
  }
  async lock(): Promise<() => void> {
    while (this.locked) await new Promise<void>((r) => this.waiters.push(r));
    this.locked = true;
    return () => {
      this.locked = false;
      this.waiters.shift()?.();
    };
  }
}

const toStorageError = (e: unknown) => (e instanceof StorageError ? e : StorageError.idbTree(e));

export class IdbStorage extends OrderedKvStorage implements ReopenableStorage {
  private readonly mutationGate = new AsyncMutex();
  private needsReset = false;
  private treeEpoch = 0;

  private constructor(
    private tree: IdbTree,
    private readonly columnFamilies: Set<string>,
  ) {
    super();
  }

  static async open(store: PageStore, columnFamilies: string[]): Promise<IdbStorage> {
    validatePhysicalStorageNames(columnFamilies);
    try {
      return new IdbStorage(await IdbTree.open(store, {}), new Set(columnFamilies));
    } catch (e) {
      throw toStorageError(e);
    }
  }

  private ensureCf(cf: string) {
    if (!this.columnFamilies.has(cf)) throw StorageError.columnFamilyNotFound(cf);
  }
  private encodedKey(cf: string, key: Bytes): Bytes {
    this.ensureCf(cf);
    return encodeColumnFamilyKey(cf, key);
  }
  private static decodeRows(rows: KeyValue[]): KeyValue[] {
    return rows.map(([key, value]) => [decodeColumnFamilyKey(key)[1].slice(), value]);
  }
  private prevalidateWriteMany(ops: WriteOperation[]) {
    for (const op of ops) this.ensureCf(op.cf);
  }

  // Answer read-committed from a resident generation without the gate; otherwise
  // hydrate without holding the gate, then retry under it. Only a resident,
  // serialized attempt may return a value.
  private async readResident<T>(read: (tree: IdbTree) => MaybePromise<T>): Promise<T> {
    for (;;) {
      if (this.mutationGate.isLocked() && !this.needsReset) {
        try {
          const v = read(this.tree.readCommitted());
          if (!isPromise(v)) return v;
          v.catch(() => {});
        } catch {
          // fall through to the serialized path
        }
      }
      let release = await this.mutationGate.lock();
      try {
        await this.ensureReady();
        const epoch = this.treeEpoch;
        const pending = read(this.tree);
        if (!isPromise(pending)) return pending;
        release();
        release = () => {};
        try {
          await pending;
        } catch (error) {
          release = await this.mutationGate.lock();
          await this.ensureReady();
          if (this.treeEpoch === epoch) throw toStorageError(error);
        }
      } catch (e) {
        throw toStorageError(e);
      } finally {
        release();
      }
    }
  }

  private async reopenAfterGenerationConflict() {
    // Discard the stale cache and recompute from the newly durable tree.
    await this.tree.reload();
    this.treeEpoch++;
    this.needsReset = false;
  }
  private async ensureReady() {
    if (this.needsReset) await this.reopenAfterGenerationConflict();
  }
  private async discardFailedTree(error: StorageError): Promise<StorageError> {
    this.needsReset = true;
    try {
      await this.reopenAfterGenerationConflict();
      return error;
    } catch (resetError) {
      return toStorageError(resetError);
    }
  }
  private static isGenerationConflict(error: StorageError) {
    return error.kind === "IdbTree" && isIdbTreeGenerationConflict(error.data);
  }
  private static async backOffAfterGenerationConflict(retry: number) {
    const yields = Math.min(1 << Math.min(retry, 4), MAX_CONFLICT_BACKOFF_YIELDS);
    for (let i = 0; i < yields; i++) await yieldNow();
  }

  private async writeManyOnce(tree: IdbTree, ops: WriteOperation[]) {
    const writes = ops.map((op) =>
      op.kind === "Set"
        ? { kind: "Set", key: this.encodedKey(op.cf, op.key), value: op.value }
        : { kind: "Delete", key: this.encodedKey(op.cf, op.key) },
    );
    // A cancelled or failed write leaves staged state; force a reload first.
    this.needsReset = true;
    try {
      await tree.writeMany(writes);
      await tree.flush();
    } catch (e) {
      throw toStorageError(e);
    }
    this.needsReset = false;
  }

  private async flushTree() {
    this.needsReset = true;
    try {
      await this.tree.flush();
      this.needsReset = false;
    } catch (e) {
      throw await this.discardFailedTree(toStorageError(e));
    }
  }

  private async writeUnless<T>(
    op: WriteOperation,
    settled: (v: Bytes | undefined) => T | undefined,
    written: T,
  ): Promise<T> {
    this.ensureCf(op.cf);
    const release = await this.mutationGate.lock();
    try {
      await this.ensureReady();
      const encodedKey = this.encodedKey(op.cf, op.key);
      for (let retry = 0; retry <= MAX_GENERATION_CONFLICT_RETRIES; retry++) {
        const tree = this.tree;
        const outcome = settled(await tree.get(encodedKey));
        if (outcome !== undefined) return outcome;
        try {
          await this.writeManyOnce(tree, [op]);
          return written;
        } catch (e) {
          const error = toStorageError(e);
          if (!IdbStorage.isGenerationConflict(error)) throw await this.discardFailedTree(error);
          await this.reopenAfterGenerationConflict();
          if (retry === MAX_GENERATION_CONFLICT_RETRIES)
            throw StorageError.idbContention(MAX_GENERATION_CONFLICT_RETRIES);
          await IdbStorage.backOffAfterGenerationConflict(retry);
        }
      }
      throw new Error("bounded retry loop returns");
    } finally {
      release();
    }
  }

  private async writeManyReplayingGenerationConflicts(ops: WriteOperation[]) {
    let retries = 0;
    for (;;) {
      try {
        await this.writeManyOnce(this.tree, ops);
        return;
      } catch (e) {
        const error = toStorageError(e);
        if (!IdbStorage.isGenerationConflict(error)) throw await this.discardFailedTree(error);
        if (retries === MAX_GENERATION_CONFLICT_RETRIES) {
          await this.reopenAfterGenerationConflict();
          throw StorageError.idbContention(retries);
        }
        retries++;
        await this.reopenAfterGenerationConflict();
        await IdbStorage.backOffAfterGenerationConflict(retries);
      }
    }
  }

  private async gated(ops: WriteOperation[]) {
    this.prevalidateWriteMany(ops);
    const release = await this.mutationGate.lock();
    try {
      await this.ensureReady();
      await this.writeManyReplayingGenerationConflicts(ops);
    } finally {
      release();
    }
  }

  override async compareValue(cf: string, key: Bytes, expected: Bytes): Promise<ValueComparison> {
    const k = this.encodedKey(cf, key);
    const result = await this.readResident((tree) => tree.valueEquals(k, expected));
    return result === undefined ? "Absent" : result ? "Identical" : "Different";
  }
  async get(cf: string, key: Bytes): Promise<Bytes | undefined> {
    const k = this.encodedKey(cf, key);
    return this.readResident((tree) => tree.get(k));
  }
  putIfAbsent(cf: string, key: Bytes, value: Bytes) {
    return this.writeUnless<Bytes | null>(
      { kind: "Set", cf, key, value },
      (existing) => existing,
      null,
    ).then((v) => v ?? undefined);
  }
  compareAndDelete(cf: string, key: Bytes, expected: Bytes) {
    return this.writeUnless(
      { kind: "Delete", cf, key },
      (current) => (current && equalBytes(current, expected) ? undefined : false),
      true,
    );
  }
  set(cf: string, key: Bytes, value: Bytes) {
    return this.gated([{ kind: "Set", cf, key, value }]);
  }
  delete(cf: string, key: Bytes) {
    return this.gated([{ kind: "Delete", cf, key }]);
  }
  override async close() {
    return this.flushWriteBoundary();
  }
  override async flushWriteBoundary() {
    const release = await this.mutationGate.lock();
    try {
      await this.ensureReady();
      await this.flushTree();
    } finally {
      release();
    }
  }
  async scan(request: ScanRequest): Promise<StorageCursor> {
    const { cf, bounds, direction, maxItems } = request;
    if (maxItems === 0 || isEmptyRange(bounds)) {
      this.encodedKey(cf, new Uint8Array());
      return new ReadyStorageCursor([]);
    }
    let start: Bytes;
    let end: Bytes;
    if (bounds.kind === "Range")
      [start, end] = [this.encodedKey(cf, bounds.start), this.encodedKey(cf, bounds.end)];
    else {
      start = this.encodedKey(cf, bounds.prefix);
      end = prefixSuccessor(start) ?? Uint8Array.of(0xff);
    }
    const limit = maxItems ?? Number.MAX_SAFE_INTEGER;
    const rows = await this.readResident((tree) =>
      direction === "Forward"
        ? tree.rangeLimit(start, end, limit)
        : tree.rangeReverse(start, end, limit),
    );
    return new ReadyStorageCursor(IdbStorage.decodeRows(rows));
  }
  override async lastWithPrefix(cf: string, prefix: Bytes) {
    const start = this.encodedKey(cf, prefix);
    const end = prefixSuccessor(start) ?? Uint8Array.of(0xff);
    const rows = await this.readResident((tree) => tree.rangeReverse(start, end, 1));
    return IdbStorage.decodeRows(rows.slice(0, 1)).pop();
  }
  override async lastWithPrefixBeforeOrAt(cf: string, prefix: Bytes, upper: Bytes) {
    const start = this.encodedKey(cf, prefix);
    const encodedUpper = this.encodedKey(cf, upper);
    const end = new Uint8Array(encodedUpper.length + 1);
    end.set(encodedUpper);
    const rows = await this.readResident((tree) => tree.rangeReverse(start, end, 1));
    if (!rows.length) return undefined;
    const decoded = IdbStorage.decodeRows([rows[0]])[0];
    const key = decoded[0];
    return key.length >= prefix.length &&
      equalBytes(key.subarray(0, prefix.length), prefix) &&
      compareBytes(key, upper) <= 0
      ? decoded
      : undefined;
  }
  writeMany(operations: WriteOperation[]) {
    return this.gated(operations);
  }
  override async writeManyOutcome(operations: WriteOperation[]): Promise<WriteManyOutcome> {
    try {
      this.prevalidateWriteMany(operations);
    } catch (error) {
      return { kind: "Uncommitted", error: error as StorageError };
    }
    try {
      await this.writeMany(operations);
      return COMMITTED;
    } catch (error) {
      return { kind: "PossiblyCommitted", error: toStorageError(error) };
    }
  }
  override columnFamilyNames() {
    return [...this.columnFamilies].sort();
  }
  async reopen(columnFamilies: string[]) {
    validatePhysicalStorageNames(columnFamilies);
    for (const cf of columnFamilies) this.columnFamilies.add(cf);
    return this;
  }
}
