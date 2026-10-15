// Durable numeric identities for schema indexes.
import {
  type IndexSchema,
  type OrderedKvStorage,
  type OwnedWriteOperation,
  ScanRequest,
  StorageError,
  collectScan,
} from "../../../_extern/groove-ivm-plan.js";
import { cmpStr, cmpTuple, SortedMap, SortedSet } from "../../../support/collections.js";
import { concatBytes, equalBytes, fromUtf8, utf8 } from "../../../support/bytes.js";

/** The logical storage family that holds every durable schema index. */
export const INDICES_CF = "indices";
export const INDEX_ID_REGISTRY_PREFIX = utf8("\0groove-index-id\0");
export const INDEX_LAYOUT_MARKER_KEY = utf8("\0groove-index-layout");
export const INDEX_LAYOUT_MARKER_VALUE = utf8("groove-durable-index-v2");

type Key = [string, string];
const cmpKey = cmpTuple(cmpStr, cmpStr);

interface IndexIdEntry {
  id: number;
  definition: Uint8Array;
}

/** The registry writes one flush must make durable, and what to mark once it has. */
export class PendingIndexIds {
  operations: OwnedWriteOperation[] = [];
  marker = false;
  entries: [Key, number][] = [];
  isEmpty(): boolean {
    return !this.operations.length;
  }
}

const layoutError = (m: string) => StorageError.InvalidStorageLayout(m);

/** Durable `(table, index) -> id` assignments plus their persistence state. */
export class IndexIdRegistry {
  private entries = new SortedMap<Key, IndexIdEntry>(cmpKey as any);
  private namesById = new Map<number, Key>();
  private maxId = 0;
  private unpersisted = new SortedSet<Key>(cmpKey as any);
  private markerPersisted = false;

  /** Load the registry from the `indices` family and validate its layout marker. */
  static async load(storage: OrderedKvStorage): Promise<IndexIdRegistry> {
    let marker: Uint8Array | undefined;
    try {
      marker = await storage.get(INDICES_CF, INDEX_LAYOUT_MARKER_KEY);
    } catch (error: any) {
      if (error?.kind === "ColumnFamilyNotFound") return new IndexIdRegistry();
      throw error;
    }
    const registry = new IndexIdRegistry();
    if (marker === undefined) {
      // Metadata keys start with 0x00 and sort first, so the last key is an index entry whenever any exists.
      const last = await collectScan(
        await storage.scan(
          ScanRequest.prefix(INDICES_CF, new Uint8Array()).withMaxItems(1).reversed(),
        ),
      );
      if (last.some(([key]: [Uint8Array]) => key[0] !== 0))
        throw layoutError(
          "durable-index entries without the groove-durable-index-v2 layout marker",
        );
      return registry;
    }
    if (!equalBytes(marker, INDEX_LAYOUT_MARKER_VALUE))
      throw layoutError("unsupported durable-index layout marker");
    registry.markerPersisted = true;
    const entries = await collectScan(
      await storage.scan(ScanRequest.prefix(INDICES_CF, INDEX_ID_REGISTRY_PREFIX)),
    );
    for (const [key, value] of entries as [Uint8Array, Uint8Array][]) {
      const names = decodeRegistryKey(key);
      const [id, definition] = decodeRegistryValue(value);
      if (registry.namesById.has(id)) throw layoutError(`durable index id ${id} is assigned twice`);
      registry.namesById.set(id, names);
      registry.maxId = Math.max(registry.maxId, id);
      registry.entries.set(names, { id, definition });
    }
    return registry;
  }

  /** Return the id for `index` on `table`, allocating a fresh one when unknown or redefined. */
  resolve(table: string, index: IndexSchema): number {
    const definition = indexDefinitionBytes(index);
    const key: Key = [table, index.name];
    const existing = this.entries.get(key);
    if (existing && equalBytes(existing.definition, definition)) return existing.id;
    if (this.maxId >= 0xffffffff) throw new Error("durable index ids must not wrap");
    const id = ++this.maxId;
    const previous = this.entries.set(key, { id, definition });
    // The superseded id stays retired: maxId never decreases.
    if (previous) this.namesById.delete(previous.id);
    this.namesById.set(id, key);
    this.unpersisted.add(key);
    return id;
  }

  /** Retire the id currently assigned to `index` on `table`. */
  retire(table: string, index: string): void {
    const key: Key = [table, index];
    const previous = this.entries.delete(key);
    if (previous) {
      this.namesById.delete(previous.id);
      this.unpersisted.delete(key);
    }
  }

  names(id: number): Key | undefined {
    return this.namesById.get(id);
  }

  /** Registry writes not yet known to be durable. */
  pending(): PendingIndexIds {
    const pending = new PendingIndexIds();
    if (!this.unpersisted.size) return pending;
    if (!this.markerPersisted) {
      pending.marker = true;
      pending.operations.push({
        kind: "Set",
        cf: INDICES_CF,
        key: INDEX_LAYOUT_MARKER_KEY,
        value: INDEX_LAYOUT_MARKER_VALUE,
      });
    }
    for (const key of this.unpersisted) {
      const entry = this.entries.get(key)!;
      pending.operations.push({
        kind: "Set",
        cf: INDICES_CF,
        key: encodeRegistryKey(key[0], key[1]),
        value: encodeRegistryValue(entry.id, entry.definition),
      });
      pending.entries.push([key, entry.id]);
    }
    return pending;
  }

  markPersisted(pending: PendingIndexIds): void {
    if (pending.marker) this.markerPersisted = true;
    for (const [key, id] of pending.entries) {
      if (this.entries.get(key)?.id === id) this.unpersisted.delete(key);
    }
  }
}

const u16be = (n: number) => {
  if (n > 0xffff) throw new Error("index column count fits u16");
  return Uint8Array.of(n >> 8, n & 0xff);
};

/** Canonical bytes of an index definition. */
function indexDefinitionBytes(index: IndexSchema): Uint8Array {
  const parts = [Uint8Array.of(index.unique ? 1 : 0), u16be(index.columns.length)];
  for (const column of index.columns as string[]) {
    const bytes = utf8(column);
    parts.push(u16be(bytes.length), bytes);
  }
  return concatBytes(...parts);
}

function encodeRegistryKey(table: string, index: string): Uint8Array {
  const t = utf8(table);
  return concatBytes(INDEX_ID_REGISTRY_PREFIX, u16be(t.length), t, utf8(index));
}

function decodeRegistryKey(key: Uint8Array): Key {
  const malformed = () => layoutError("malformed durable index id key");
  const p = INDEX_ID_REGISTRY_PREFIX;
  if (key.length < p.length || !equalBytes(key.subarray(0, p.length), p)) throw malformed();
  const rest = key.subarray(p.length);
  if (rest.length < 2) throw malformed();
  const len = (rest[0] << 8) | rest[1];
  if (rest.length - 2 < len) throw malformed();
  try {
    const dec = new TextDecoder("utf-8", { fatal: true });
    return [dec.decode(rest.subarray(2, 2 + len)), dec.decode(rest.subarray(2 + len))];
  } catch {
    throw malformed();
  }
}

function encodeRegistryValue(id: number, definition: Uint8Array): Uint8Array {
  const v = new Uint8Array(4 + definition.length);
  new DataView(v.buffer).setUint32(0, id);
  v.set(definition, 4);
  return v;
}

function decodeRegistryValue(value: Uint8Array): [number, Uint8Array] {
  if (value.length < 4) throw layoutError("malformed durable index id value");
  const id = new DataView(value.buffer, value.byteOffset, 4).getUint32(0);
  if (id === 0) throw layoutError("durable index id 0 is reserved");
  return [id, value.slice(4)];
}

/** The storage-key prefix of one durable index: minimal unsigned LEB128 of its id. */
export function durableIndexKeyPrefix(id: number): Uint8Array {
  const prefix: number[] = [];
  let rest = id >>> 0;
  for (;;) {
    const byte = rest & 0x7f;
    rest >>>= 7;
    if (rest === 0) {
      prefix.push(byte);
      return Uint8Array.from(prefix);
    }
    prefix.push(byte | 0x80);
  }
}

/** Split a durable index storage key into its id and logical key. */
export function splitDurableIndexKey(key: Uint8Array): [number, Uint8Array] | undefined {
  let id = 0;
  for (let position = 0; position < Math.min(5, key.length); position++) {
    const byte = key[position];
    const bits = byte & 0x7f;
    if (position === 4 && bits > 0x0f) return undefined;
    id = (id | (bits << (7 * position))) >>> 0;
    if ((byte & 0x80) === 0) {
      // Minimal: a final zero byte is only valid alone, and id 0 is metadata.
      if ((position > 0 && byte === 0) || id === 0) return undefined;
      return [id, key.subarray(position + 1)];
    }
  }
  return undefined;
}

export { fromUtf8 };
