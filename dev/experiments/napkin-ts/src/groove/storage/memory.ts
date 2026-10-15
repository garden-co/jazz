// In-memory implementation of the ordered key/value storage trait.
import { type Bytes, compareBytes, equalBytes, utf8, fromUtf8 } from "../../support/bytes.js";
import { SortedMap, cmpBytes, cmpStr } from "../../support/collections.js";
import {
  type KeyValue,
  type ReopenableStorage,
  type ScanBounds,
  type ScanDirection,
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

const MEMORY_STORAGE_SNAPSHOT_VERSION = 1;
const MEMORY_STORAGE_SNAPSHOT_MAGIC = utf8("GMS1");

type Family = SortedMap<Bytes, Bytes>;
type ColumnFamilies = SortedMap<string, Family>;

export class MemoryStorageSnapshotError extends Error {
  static invalid(what: string) {
    return new MemoryStorageSnapshotError(`memory storage snapshot is invalid: ${what}`);
  }
  static tooLarge(field: string) {
    return new MemoryStorageSnapshotError(`memory storage snapshot ${field} exceeds v1 bounds`);
  }
  static unsupportedVersion(found: number, expected: number) {
    return new MemoryStorageSnapshotError(
      `unsupported memory storage snapshot version ${found}; expected ${expected}`,
    );
  }
  static invalidColumnFamily(e: unknown) {
    return new MemoryStorageSnapshotError(
      `memory storage snapshot has an invalid physical column-family name: ${(e as Error).message}`,
    );
  }
}

/** Keeps only one cursor batch cloned at a time (bounded traversal). */
class MemoryStorageCursor implements StorageCursor {
  private lastKey?: Bytes;
  private done = false;
  constructor(
    private readonly storage: MemoryStorage,
    private readonly cf: string,
    private readonly bounds: ScanBounds,
    private readonly direction: ScanDirection,
    private remaining: number | undefined,
  ) {}

  private nextValues(limit: number): KeyValue[] {
    const values = this.storage.withCf(this.cf, (family) => {
      let lower: Bytes;
      let upper: Bytes | undefined;
      if (this.bounds.kind === "Prefix")
        [lower, upper] = [this.bounds.prefix, prefixSuccessor(this.bounds.prefix)];
      else [lower, upper] = [this.bounds.start, this.bounds.end];
      const last = this.lastKey;
      const out: KeyValue[] = [];
      if (this.direction === "Forward") {
        for (const [k, v] of family.range(last ?? lower, upper)) {
          if (last && compareBytes(k, last) <= 0) continue;
          if (out.length >= limit) break;
          out.push([k, v]);
        }
      } else {
        const all = [...family.range(lower, last ?? upper)];
        for (let i = all.length - 1; i >= 0 && out.length < limit; i--) out.push(all[i]);
      }
      return out;
    });
    if (values.length) this.lastKey = values[values.length - 1][0];
    else this.done = true;
    return values;
  }

  async nextBatch() {
    if (this.done) return undefined;
    const limit =
      this.remaining === undefined ? 256 : this.remaining > 0 ? Math.min(this.remaining, 256) : 0;
    if (!limit) {
      this.done = true;
      return undefined;
    }
    const values = this.nextValues(limit);
    if (!values.length) return undefined;
    if (this.remaining !== undefined) this.remaining -= values.length;
    return values;
  }
}

// `GMS1 | version:u16-be | family_count:u32-be | families...`, each family
// `name_len:u16-be | name | entry_count:u32-be | (key_len:u32 key value_len:u32 value)*`.
function encodeSnapshot(version: number, families: ColumnFamilies): Bytes {
  const out: number[] = [...MEMORY_STORAGE_SNAPSHOT_MAGIC];
  const u16 = (v: number, field: string) => {
    if (v > 0xffff) throw MemoryStorageSnapshotError.tooLarge(field);
    out.push(v >> 8, v & 0xff);
  };
  const u32 = (v: number, field: string) => {
    if (v > 0xffffffff) throw MemoryStorageSnapshotError.tooLarge(field);
    out.push((v >>> 24) & 0xff, (v >>> 16) & 0xff, (v >>> 8) & 0xff, v & 0xff);
  };
  out.push(version >> 8, version & 0xff);
  u32(families.size, "family count");
  for (const [name, entries] of families) {
    const nameBytes = utf8(name);
    u16(nameBytes.length, "family name");
    out.push(...nameBytes);
    u32(entries.size, "entry count");
    for (const [key, value] of entries) {
      u32(key.length, "key");
      for (const b of key) out.push(b);
      u32(value.length, "value");
      for (const b of value) out.push(b);
    }
  }
  return Uint8Array.from(out);
}

function decodeSnapshot(bytes: Bytes): { version: number; families: ColumnFamilies } {
  let pos = 0;
  const take = (n: number) => {
    if (bytes.length - pos < n) throw MemoryStorageSnapshotError.invalid("truncated bytes");
    return bytes.subarray(pos, (pos += n));
  };
  const u16 = () => {
    const b = take(2);
    return (b[0] << 8) | b[1];
  };
  const u32 = () => new DataView(take(4).slice().buffer).getUint32(0);
  if (!equalBytes(take(4), MEMORY_STORAGE_SNAPSHOT_MAGIC))
    throw MemoryStorageSnapshotError.invalid("magic is invalid");
  const version = u16();
  const families: ColumnFamilies = new SortedMap(cmpStr);
  for (let f = u32(); f > 0; f--) {
    let name: string;
    try {
      name = fromUtf8(take(u16()));
    } catch {
      throw MemoryStorageSnapshotError.invalid("family name is not UTF-8");
    }
    const entries: Family = new SortedMap(cmpBytes);
    for (let e = u32(); e > 0; e--) {
      const key = take(u32()).slice();
      entries.set(key, take(u32()).slice());
    }
    families.set(name, entries);
  }
  if (pos !== bytes.length) throw MemoryStorageSnapshotError.invalid("trailing bytes");
  if (!equalBytes(encodeSnapshot(version, families), bytes))
    throw MemoryStorageSnapshotError.invalid("noncanonical bytes");
  return { version, families };
}

/** Ordered in-memory storage for tests, examples, benches, and wasm probes. */
export class MemoryStorage extends OrderedKvStorage implements ReopenableStorage {
  private inner: ColumnFamilies = new SortedMap(cmpStr);

  static create(columnFamilies: string[]): MemoryStorage {
    validatePhysicalStorageNames(columnFamilies);
    const storage = new MemoryStorage();
    storage.ensureColumnFamilies(columnFamilies);
    return storage;
  }

  private ensureColumnFamilies(cfs: string[]) {
    for (const cf of cfs) this.inner.getOrInsert(cf, () => new SortedMap(cmpBytes));
  }

  private family(cf: string): Family {
    const family = this.inner.get(cf);
    if (!family) throw StorageError.columnFamilyNotFound(cf);
    return family;
  }

  withCf<T>(cf: string, f: (family: Family) => T): T {
    return f(this.family(cf));
  }

  exportSnapshot(): Bytes {
    return encodeSnapshot(MEMORY_STORAGE_SNAPSHOT_VERSION, this.inner);
  }

  importSnapshot(bytes: Bytes): void {
    const snapshot = decodeSnapshot(bytes);
    if (snapshot.version !== MEMORY_STORAGE_SNAPSHOT_VERSION)
      throw MemoryStorageSnapshotError.unsupportedVersion(
        snapshot.version,
        MEMORY_STORAGE_SNAPSHOT_VERSION,
      );
    try {
      validatePhysicalStorageNames(snapshot.families.keysArray());
    } catch (e) {
      throw MemoryStorageSnapshotError.invalidColumnFamily(e);
    }
    this.inner = snapshot.families;
  }

  override async compareValue(cf: string, key: Bytes, expected: Bytes): Promise<ValueComparison> {
    const value = this.family(cf).get(key);
    return !value ? "Absent" : equalBytes(value, expected) ? "Identical" : "Different";
  }
  override permitsEagerReadRetry() {
    return true;
  }
  async get(cf: string, key: Bytes) {
    return this.family(cf).get(key);
  }
  async putIfAbsent(cf: string, key: Bytes, value: Bytes) {
    const family = this.family(cf);
    const existing = family.get(key);
    if (existing) return existing;
    family.set(key, value);
    return undefined;
  }
  async compareAndDelete(cf: string, key: Bytes, expected: Bytes) {
    const family = this.family(cf);
    const current = family.get(key);
    if (!current || !equalBytes(current, expected)) return false;
    family.delete(key);
    return true;
  }
  override async approximateClassBytes(cf: string) {
    let total = 0;
    for (const [k, v] of this.family(cf)) total += k.length + v.length;
    return total;
  }
  async set(cf: string, key: Bytes, value: Bytes) {
    this.family(cf).set(key, value);
  }
  async delete(cf: string, key: Bytes) {
    this.family(cf).delete(key);
  }
  async scan(request: ScanRequest): Promise<StorageCursor> {
    this.family(request.cf);
    if (isEmptyRange(request.bounds)) return new ReadyStorageCursor([]);
    return new MemoryStorageCursor(
      this,
      request.cf,
      request.bounds,
      request.direction,
      request.maxItems,
    );
  }
  override async lastWithPrefix(cf: string, prefix: Bytes) {
    const all = [...this.family(cf).range(prefix, prefixSuccessor(prefix))];
    return all[all.length - 1];
  }
  override async lastWithPrefixBeforeOrAt(cf: string, prefix: Bytes, upper: Bytes) {
    const all = [...this.family(cf).range(prefix, upper, true)];
    for (let i = all.length - 1; i >= 0; i--) {
      const key = all[i][0];
      if (key.length >= prefix.length && equalBytes(key.subarray(0, prefix.length), prefix))
        return all[i];
    }
    return undefined;
  }
  async writeMany(operations: WriteOperation[]) {
    // Validate every operation before changing the store (batch atomicity).
    for (const op of operations)
      if (!this.inner.has(op.cf)) throw StorageError.columnFamilyNotFound(op.cf);
    for (const op of operations) {
      const family = this.inner.get(op.cf)!;
      if (op.kind === "Set") family.set(op.key, op.value);
      else family.delete(op.key);
    }
  }
  override async writeManyOutcome(operations: WriteOperation[]): Promise<WriteManyOutcome> {
    try {
      await this.writeMany(operations);
      return COMMITTED;
    } catch (error) {
      return { kind: "Uncommitted", error: error as StorageError };
    }
  }
  override columnFamilyNames() {
    return [...this.inner.keysArray()];
  }
  async reopen(columnFamilies: string[]) {
    validatePhysicalStorageNames(columnFamilies);
    this.ensureColumnFamilies(columnFamilies);
    return this;
  }
}
