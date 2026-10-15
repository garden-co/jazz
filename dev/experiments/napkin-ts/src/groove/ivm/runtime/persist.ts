// Durable persist operator writes.
import {
  type Bytes,
  SortedMap,
  cmpBytes,
  concatBytes,
  toHex,
  equalBytes,
} from "../../../support/index.js";
import { IvmRuntimeError } from "./index.js";
import type { RecordDeltas } from "./subscriptions.js";
import {
  type DurableStorage,
  type OrderedKvStorage,
  type OwnedWriteOperation,
  type RecordDescriptor,
  RecordStore,
  compositeKeys,
  encodeKeyPart,
  indexRecordDescriptor,
  namedFieldValue,
  descriptorsEqual,
} from "../../../_extern/groove-ivm-tick.js";

class PendingPersistKey {
  weight = 0;
  positiveRecord: Bytes | undefined;
  uniqueRecordDeltas = new Map<string, [Bytes, number]>();
}

export function applyPersistDelta(
  storage: OrderedKvStorage,
  durableStorage: DurableStorage,
  keyFields: number[],
  unique: boolean,
  delta: RecordDeltas,
): Promise<void> {
  return applyPersistDeltaWith(storage, durableStorage, keyFields, unique, delta, []);
}

/** Like applyPersistDelta, submitting `leading` in the same atomic write. */
export async function applyPersistDeltaWith(
  storage: OrderedKvStorage,
  durableStorage: DurableStorage,
  keyFields: number[],
  unique: boolean,
  delta: RecordDeltas,
  leading: OwnedWriteOperation[],
): Promise<void> {
  if (
    keyFields.length === 1 &&
    keyFields[0] === 0 &&
    descriptorsEqual(delta.descriptor, indexRecordDescriptor())
  ) {
    return applyIndexPersistDelta(storage, durableStorage, unique, delta, leading);
  }
  const store = new RecordStore(storage, durableStorage.columnFamily, delta.descriptor);
  const pending = new SortedMap<Bytes, PendingPersistKey>(cmpBytes);
  for (const recordDelta of delta.deltas) {
    const keys = persistRecordKeys(delta.descriptor, recordDelta.record, keyFields, durableStorage);
    for (const key of keys) {
      if (recordDelta.weight === 0) continue;
      addPendingDelta(
        pending.getOrInsert(key, () => new PendingPersistKey()),
        recordDelta.record,
        recordDelta.weight,
        unique,
      );
    }
  }
  return writePending(store, durableStorage, unique, pending, leading);
}

/** Persist IndexBy records as compact durable index entries. */
async function applyIndexPersistDelta(
  storage: OrderedKvStorage,
  durableStorage: DurableStorage,
  unique: boolean,
  delta: RecordDeltas,
  leading: OwnedWriteOperation[],
): Promise<void> {
  const store = new RecordStore(storage, durableStorage.columnFamily, delta.descriptor);
  const pending = new SortedMap<Bytes, PendingPersistKey>(cmpBytes);
  for (const recordDelta of delta.deltas) {
    if (recordDelta.weight === 0) continue;
    const record = delta.descriptor.borrow(recordDelta.record);
    const logicalKey: Bytes = IvmRuntimeError.wrapRecord(() => record.getBytes(0));
    const value: Bytes = IvmRuntimeError.wrapRecord(() => record.getBytes(1));
    const key = concatBytes(durableStorage.keyPrefix, logicalKey);
    addPendingDelta(
      pending.getOrInsert(key, () => new PendingPersistKey()),
      value,
      recordDelta.weight,
      unique,
    );
  }
  return writePending(store, durableStorage, unique, pending, leading);
}

async function writePending(
  store: RecordStore,
  durableStorage: DurableStorage,
  unique: boolean,
  pending: SortedMap<Bytes, PendingPersistKey>,
  leading: OwnedWriteOperation[],
): Promise<void> {
  const operations = leading.slice();
  const cf = durableStorage.columnFamily;
  if (unique) {
    operations.push(...(await uniquePendingOperations(store, durableStorage, pending)));
    return IvmRuntimeError.wrapStorageAsync(() => store.writeMany(operations));
  }
  for (const [key, entry] of pending) {
    if (entry.weight > 0) {
      if (!entry.positiveRecord) throw IvmRuntimeError.of("PersistRecordMismatch");
      operations.push({ kind: "Set", cf, key, value: entry.positiveRecord });
    } else if (entry.weight < 0) {
      operations.push({ kind: "Delete", cf, key });
    } else if (entry.positiveRecord && (await store.getRaw(key)) !== undefined) {
      operations.push({ kind: "Set", cf, key, value: entry.positiveRecord });
    }
  }
  return IvmRuntimeError.wrapStorageAsync(() => store.writeMany(operations));
}

/** Rebuild the in-memory IndexBy record of one durable index entry. */
export function indexRecordFromStorage(prefixLen: number, key: Bytes, value: Bytes): Bytes {
  if (key.length < prefixLen) {
    throw IvmRuntimeError.of("InvalidPersistedIndex", "index key shorter than its id prefix");
  }
  return IvmRuntimeError.wrapRecord(() =>
    indexRecordDescriptor().create([
      { kind: "Bytes", v: key.slice(prefixLen) },
      { kind: "Bytes", v: value },
    ]),
  );
}

function addPendingDelta(
  entry: PendingPersistKey,
  record: Bytes,
  weight: number,
  unique: boolean,
): void {
  if (weight === 0) return;
  entry.weight += weight;
  if (unique) {
    const k = toHex(record);
    const current = entry.uniqueRecordDeltas.get(k);
    if (current) current[1] += weight;
    else entry.uniqueRecordDeltas.set(k, [record, weight]);
  } else if (weight > 0) {
    entry.positiveRecord = record;
  }
}

async function uniquePendingOperations(
  store: RecordStore,
  durableStorage: DurableStorage,
  pending: SortedMap<Bytes, PendingPersistKey>,
): Promise<OwnedWriteOperation[]> {
  const operations: OwnedWriteOperation[] = [];
  const cf = durableStorage.columnFamily;
  for (const [key, entry] of pending) {
    const record = await resolveUniqueOwner(store, durableStorage, key, entry);
    operations.push(record ? { kind: "Set", cf, key, value: record } : { kind: "Delete", cf, key });
  }
  return operations;
}

async function resolveUniqueOwner(
  store: RecordStore,
  durableStorage: DurableStorage,
  key: Bytes,
  entry: PendingPersistKey,
): Promise<Bytes | undefined> {
  const durableOwner: Bytes | undefined = await IvmRuntimeError.wrapStorageAsync(() =>
    store.getRaw(key),
  );
  const recordDeltas = entry.uniqueRecordDeltas;
  let survives = false;
  if (durableOwner) {
    const k = toHex(durableOwner);
    const d = recordDeltas.get(k)?.[1] ?? 0;
    recordDeltas.delete(k);
    survives = d >= 0;
  }
  let owner = survives ? durableOwner : undefined;
  for (const [record, delta] of recordDeltas.values()) {
    if (delta <= 0) continue;
    if (owner) throw IvmRuntimeError.of("UniqueIndexViolation", { index: durableStorage.name });
    owner = record;
  }
  return owner;
}

function persistRecordKeys(
  descriptor: RecordDescriptor,
  record: Bytes,
  keyFields: number[],
  durableStorage: DurableStorage,
): Bytes[] {
  return compositeKeys(
    durableStorage.keyPrefix,
    keyFields.map((fieldIdx) => namedFieldValue(descriptor, record, fieldIdx)),
    encodeKeyPart,
  );
}
