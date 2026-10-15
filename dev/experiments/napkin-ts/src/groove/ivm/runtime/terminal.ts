// Structured terminal snapshot ordering and incremental-tree differencing.
import {
  compareBytes,
  concatBytes,
  type Bytes,
  SortedMap,
  SortedSet,
  cmpBytes,
  toHex,
} from "../../../support/index.js";
import { IvmRuntimeError } from "./index.js";
import type { RecordDeltas } from "./subscriptions.js";
import {
  type RecordDescriptor,
  OwnedRecord,
  encodedIdentityKeyPart,
  encodedRecordKeyPart,
  valuesEqual,
} from "../../../_extern/groove-ivm-tick.js";

/** Terminal child occurrence key v1: repeats append 0xff and a u64 BE ordinal. */
export function terminalOccurrenceKey(rowKey: Bytes, occurrence: number): Bytes {
  if (occurrence === 0) return rowKey;
  const suffix = new Uint8Array(9);
  suffix[0] = 0xff;
  new DataView(suffix.buffer).setBigUint64(1, BigInt(occurrence));
  return concatBytes(rowKey, suffix);
}

export type TerminalPathSegment =
  | { kind: "Collection"; field: string }
  | { kind: "Key"; key: Bytes };

export type TerminalEdit =
  | { kind: "Insert"; index: number; key: Bytes; value: Bytes }
  | { kind: "Update"; key: Bytes; value: Bytes }
  | { kind: "Remove"; key: Bytes }
  | { kind: "Move"; key: Bytes; index: number };

export type TerminalOperation = {
  rootDescriptor: RecordDescriptor;
  rootKey: Bytes;
  path: TerminalPathSegment[];
  edit: TerminalEdit;
};

export class TerminalDeltas {
  constructor(public operations: TerminalOperation[] = []) {}
  isEmpty(): boolean {
    return this.operations.length === 0;
  }
}

export function terminalDeltasFromRecordDeltas(deltas: RecordDeltas): TerminalDeltas {
  const keys = deltas.deltas.map((d) => encodedRecordKeyPart(deltas.descriptor, d.record, [0]));
  return terminalDeltasWithKeys(deltas, keys);
}

/** Terminal edits keyed by the identity `keyFields` of the physical deltas. */
export function terminalDeltasKeyedByIdentity(
  deltas: RecordDeltas,
  physical: RecordDeltas,
  keyFields: number[],
): TerminalDeltas {
  if (physical.deltas.length !== deltas.deltas.length)
    throw IvmRuntimeError.of("GraphOutputMismatch");
  const keys = physical.deltas.map((d) =>
    encodedIdentityKeyPart(physical.descriptor, d.record, keyFields),
  );
  return terminalDeltasWithKeys(deltas, keys);
}

function terminalDeltasWithKeys(deltas: RecordDeltas, keys: Bytes[]): TerminalDeltas {
  const before = new SortedMap<Bytes, any>(cmpBytes);
  const after = new SortedMap<Bytes, any>(cmpBytes);
  deltas.deltas.forEach((delta, i) => {
    const record = new OwnedRecord(delta.record, deltas.descriptor);
    if (delta.weight < 0) before.set(keys[i], record);
    else if (delta.weight > 0) after.set(keys[i], record);
  });
  const allKeys = new SortedSet<Bytes>(cmpBytes, [...before.keysArray(), ...after.keysArray()]);
  const operations: TerminalOperation[] = [];
  for (const key of allKeys) {
    const b = before.get(key);
    const a = after.get(key);
    const op = (edit: TerminalEdit): TerminalOperation => ({
      rootDescriptor: deltas.descriptor,
      rootKey: key,
      path: [],
      edit,
    });
    if (!b && a) operations.push(op({ kind: "Insert", index: 0, key, value: a.raw() }));
    else if (b && !a) operations.push(op({ kind: "Remove", key }));
    else if (b && a) diffTerminalRecord(key, deltas.descriptor, [], b, a, operations);
    else throw new Error("terminal key came from before or after");
  }
  return new TerminalDeltas(operations);
}

export function orderTerminalSnapshot(terminal: RecordDeltas, ordering: RecordDeltas): void {
  const positions = new Map<string, number>();
  let index = 0;
  for (const delta of ordering.deltas) {
    if (delta.weight <= 0) continue;
    const key = toHex(encodedRecordKeyPart(ordering.descriptor, delta.record, [0]));
    if (!positions.has(key)) positions.set(key, index);
    index++;
  }
  const keyed = terminal.deltas.map((delta) => {
    const key = encodedRecordKeyPart(terminal.descriptor, delta.record, [0]);
    return { position: positions.get(toHex(key)) ?? Number.MAX_SAFE_INTEGER, key, delta };
  });
  keyed.sort((l, r) => l.position - r.position || compareBytes(l.key, r.key));
  terminal.deltas = keyed.map((k) => k.delta);
}

function diffTerminalRecord(
  rootKey: Bytes,
  rootDescriptor: RecordDescriptor,
  path: TerminalPathSegment[],
  before: any,
  after: any,
  operations: TerminalOperation[],
): void {
  const beforeValues = IvmRuntimeError.wrapRecord(() => before.toValues());
  const afterValues = IvmRuntimeError.wrapRecord(() => after.toValues());
  let scalarChanged = false;
  const n = Math.min(beforeValues.length, afterValues.length);
  for (let index = 0; index < n; index++) {
    const bv = beforeValues[index];
    const av = afterValues[index];
    const field = after.descriptor().fields()[index];
    if (!field) throw IvmRuntimeError.of("GraphFieldIndexOutOfBounds", index);
    if (
      bv.kind === "Array" &&
      av.kind === "Array" &&
      field.valueType.kind === "Array" &&
      field.valueType.inner.kind === "Record"
    ) {
      if (field.name == null) throw IvmRuntimeError.of("GraphFieldNotFound", "<unnamed>");
      diffTerminalCollection(
        rootKey,
        rootDescriptor,
        [...path, { kind: "Collection", field: field.name }],
        bv.v,
        av.v,
        operations,
      );
    } else if (!valuesEqual(bv, av)) scalarChanged = true;
  }
  if (scalarChanged) {
    const last = path[path.length - 1];
    const key =
      last?.kind === "Key"
        ? last.key
        : last === undefined
          ? rootKey
          : encodedRecordKeyPart(after.descriptor(), after.raw(), [0]);
    operations.push({
      rootDescriptor,
      rootKey,
      path,
      edit: { kind: "Update", key, value: after.raw() },
    });
  }
}

function diffTerminalCollection(
  rootKey: Bytes,
  rootDescriptor: RecordDescriptor,
  path: TerminalPathSegment[],
  beforeValues: any[],
  afterValues: any[],
  operations: TerminalOperation[],
): void {
  const children = (values: any[]) => {
    const out = new SortedMap<Bytes, [number, any]>(cmpBytes);
    const occurrences = new Map<string, number>();
    values.forEach((value, index) => {
      if (value.kind !== "Record") {
        throw IvmRuntimeError.of(
          "InvalidCollectBy",
          "structured terminal arrays must contain records",
        );
      }
      const record = value.v;
      const rowKey = encodedRecordKeyPart(record.descriptor(), record.raw(), [0]);
      const hex = toHex(rowKey);
      const occurrence = occurrences.get(hex) ?? 0;
      occurrences.set(hex, occurrence + 1);
      if (out.set(terminalOccurrenceKey(rowKey, occurrence), [index, record]) !== undefined) {
        throw IvmRuntimeError.of("DuplicateCollectByOccurrenceId");
      }
    });
    return out;
  };
  const before = children(beforeValues);
  const after = children(afterValues);
  const keys = new SortedSet<Bytes>(cmpBytes, [...before.keysArray(), ...after.keysArray()]);
  for (const key of keys) {
    const b = before.get(key);
    const a = after.get(key);
    const op = (edit: TerminalEdit) =>
      operations.push({ rootDescriptor, rootKey, path: path.slice(), edit });
    if (!b && a) op({ kind: "Insert", index: a[0], key, value: a[1].raw() });
    else if (b && !a) op({ kind: "Remove", key });
    else if (b && a) {
      if (b[0] !== a[0]) op({ kind: "Move", key, index: a[0] });
      diffTerminalRecord(
        rootKey,
        rootDescriptor,
        [...path, { kind: "Key", key }],
        b[1],
        a[1],
        operations,
      );
    } else throw new Error("child key came from before or after");
  }
}
