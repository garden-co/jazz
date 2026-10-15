// `jazz.exclusive-read-evidence.v1`: an exclusive transaction's snapshot and
// read sets, kept in `jazz_transactions` slots 5-8 while its fate is pending.
import { equalBytes } from "../../support/bytes.js";
import { uuidV5 } from "../../support/hash.js";
import {
  QUERY_NAMESPACE,
  Record_,
  RecordDescriptor,
  Snapshot,
  V,
  ValueType,
  absentReadsEqual,
  bindingValuesFromCanonicalBytes,
  canonicalBindingBytesForValues,
  encodeQueryPostcard,
  nodeInvalidStoredValue,
  predicateReadsEqual,
  rowReadsEqual,
  snapshotEquals,
  takeQueryPostcard,
  txIdNew,
} from "../../_extern/node-policy-peer.js";

const FORMAT_V1 = 1;
type Value = any;

const dotDescriptor = () =>
  RecordDescriptor.new([
    ["time", ValueType.U64],
    ["node", ValueType.Uuid],
  ]);
const rowReadDescriptor = () =>
  RecordDescriptor.new([
    ["table", ValueType.String],
    ["row_uuid", ValueType.Uuid],
    ["version_time", ValueType.U64],
    ["version_node", ValueType.Uuid],
  ]);
const absentReadDescriptor = () =>
  RecordDescriptor.new([
    ["table", ValueType.String],
    ["row_uuid", ValueType.Uuid],
  ]);
const predicateReadDescriptor = () =>
  RecordDescriptor.new([
    ["table", ValueType.String],
    ["shape_id", ValueType.Uuid],
    ["query", ValueType.Bytes],
    ["binding_id", ValueType.Uuid],
    ["bindings", ValueType.Bytes],
  ]);

/** `jazz_exclusive_base_snapshot_v1` */
export const baseSnapshotDescriptor = () =>
  RecordDescriptor.new([
    ["format_v1", ValueType.U8],
    ["owner", ValueType.Uuid],
    ["global_base", ValueType.U64],
    ["local_base", ValueType.U64],
    ["dots", ValueType.arrayOf(ValueType.record(dotDescriptor()))],
  ]);
const readsDescriptor = (item: any) =>
  RecordDescriptor.new([
    ["format_v1", ValueType.U8],
    ["reads", ValueType.arrayOf(ValueType.record(item))],
  ]);
export const rowReadsDescriptor = () => readsDescriptor(rowReadDescriptor());
export const absentReadsDescriptor = () => readsDescriptor(absentReadDescriptor());
export const predicateReadsDescriptor = () => readsDescriptor(predicateReadDescriptor());

const nested = (descriptor: any, values: Value[]): Value =>
  V.record(descriptor.create(values), descriptor);
const slot = (bytes: Uint8Array | undefined): Value =>
  V.nullable(bytes === undefined ? undefined : V.bytes(bytes));

function encodeBaseSnapshot(snapshot: any): Uint8Array {
  const dot = dotDescriptor();
  const dots = snapshot.dots.map((d: any) => nested(dot, [V.u64(d.time), V.uuid(d.node)]));
  return baseSnapshotDescriptor().create([
    V.u8(FORMAT_V1),
    V.uuid(snapshot.owner),
    V.u64(snapshot.globalBase),
    V.u64(snapshot.localBase),
    V.array(dots),
  ]);
}

const encodeReads = (descriptor: any, item: any, reads: Value[][]): Uint8Array =>
  descriptor.create([V.u8(FORMAT_V1), V.array(reads.map((values) => nested(item, values)))]);

const encodeRowReads = (reads: any[]) =>
  encodeReads(
    rowReadsDescriptor(),
    rowReadDescriptor(),
    reads.map((r) => [
      V.string(r.table),
      V.uuid(r.rowUuid),
      V.u64(r.version.time),
      V.uuid(r.version.node),
    ]),
  );

const encodeAbsentReads = (reads: any[]) =>
  encodeReads(
    absentReadsDescriptor(),
    absentReadDescriptor(),
    reads.map((r) => [V.string(r.table), V.uuid(r.rowUuid)]),
  );

/** `undefined` when a predicate read cannot be represented in v1. */
function encodePredicateReads(reads: any[]): Uint8Array | undefined {
  const items: Value[][] = [];
  for (const read of reads) {
    let query: Uint8Array;
    let bindings: Uint8Array;
    try {
      query = encodeQueryPostcard(read.shape);
      bindings = canonicalBindingBytesForValues(read.bindingValues);
      bindingValuesFromCanonicalBytes(bindings);
      decodeQuery(query);
    } catch {
      return undefined;
    }
    items.push([
      V.string(read.table),
      V.uuid(read.shapeId),
      V.bytes(query),
      V.uuid(read.bindingId),
      V.bytes(bindings),
    ]);
  }
  return encodeReads(predicateReadsDescriptor(), predicateReadDescriptor(), items);
}

const optEq = <T>(a: T | undefined, b: T | undefined, eq: (a: T, b: T) => boolean) =>
  a === undefined ? b === undefined : b !== undefined && eq(a, b);

/** Storage values for slots 5-8 of a `jazz_transactions` row. */
export function evidenceSlotValues(tx: any, pending: boolean): Value[] {
  const none = () => [slot(undefined), slot(undefined), slot(undefined), slot(undefined)];
  if (!pending || tx.kind !== "Exclusive") return none();
  const snapshot = tx.baseSnapshot;
  if (!snapshot) return none();
  let predicate: Uint8Array | undefined;
  if (tx.predicateReadSet) {
    predicate = encodePredicateReads(tx.predicateReadSet);
    if (!predicate) return none();
  }
  const base = encodeBaseSnapshot(snapshot);
  const rows = tx.rowReadSet ? encodeRowReads(tx.rowReadSet) : undefined;
  const absent = tx.absentReadSet ? encodeAbsentReads(tx.absentReadSet) : undefined;
  let roundTrips = false;
  try {
    const d = decodeEvidenceSlots(base, rows, absent, predicate);
    roundTrips =
      optEq(d.baseSnapshot, snapshot, snapshotEquals) &&
      optEq(d.rowReadSet, tx.rowReadSet, rowReadsEqual) &&
      optEq(d.absentReadSet, tx.absentReadSet, absentReadsEqual) &&
      optEq(d.predicateReadSet, tx.predicateReadSet, predicateReadsEqual);
  } catch {
    roundTrips = false;
  }
  if (!roundTrips) return none();
  return [slot(base), slot(rows), slot(absent), slot(predicate)];
}

const invalid = (message: string) => nodeInvalidStoredValue(message);

function canonicalValues(descriptor: any, bytes: Uint8Array, what: string): Value[] {
  let values: Value[];
  let recreated: Uint8Array;
  try {
    values = Record_.new(bytes.slice(), descriptor).toValues();
    recreated = descriptor.create(values);
  } catch {
    throw invalid(what);
  }
  if (!equalBytes(recreated, bytes)) throw invalid(what);
  return values;
}

function versionedReads(descriptor: any, bytes: Uint8Array, what: string): Value[][] {
  const values = canonicalValues(descriptor, bytes, what);
  if (
    values.length !== 2 ||
    values[0].kind !== "U8" ||
    values[0].value !== FORMAT_V1 ||
    values[1].kind !== "Array"
  )
    throw invalid(what);
  return values[1].value.map((read: Value) => {
    if (read.kind !== "Record") throw invalid(what);
    try {
      return read.value.toValues();
    } catch {
      throw invalid(what);
    }
  });
}

function decodeQuery(bytes: Uint8Array): any {
  const what = "noncanonical exclusive predicate query";
  let query: any;
  let rest: Uint8Array;
  try {
    [query, rest] = takeQueryPostcard(bytes);
  } catch {
    throw invalid(what);
  }
  if (rest.length !== 0 || !equalBytes(encodeQueryPostcard(query), bytes)) throw invalid(what);
  return query;
}

const is = (v: Value, kind: string) => v?.kind === kind;
const shapeMatches = (values: Value[], kinds: string[]) =>
  values.length === kinds.length && kinds.every((k, i) => is(values[i], k));

function decodeBaseSnapshot(bytes: Uint8Array): any {
  const what = "invalid exclusive base snapshot v1";
  const values = canonicalValues(baseSnapshotDescriptor(), bytes, what);
  if (!shapeMatches(values, ["U8", "Uuid", "U64", "U64", "Array"]) || values[0].value !== FORMAT_V1)
    throw invalid(what);
  const dots = values[4].value.map((dot: Value) => {
    if (!is(dot, "Record")) throw invalid(what);
    let fields: Value[];
    try {
      fields = dot.value.toValues();
    } catch {
      throw invalid(what);
    }
    if (!shapeMatches(fields, ["U64", "Uuid"])) throw invalid(what);
    return txIdNew(fields[0].value, fields[1].value);
  });
  try {
    return Snapshot.exclusiveBase(values[1].value, values[2].value, values[3].value, dots);
  } catch {
    throw invalid(what);
  }
}

function decodeRowReads(bytes: Uint8Array): any[] {
  const what = "invalid exclusive row reads v1";
  return versionedReads(rowReadsDescriptor(), bytes, what).map((r) => {
    if (!shapeMatches(r, ["String", "Uuid", "U64", "Uuid"])) throw invalid(what);
    return { table: r[0].value, rowUuid: r[1].value, version: txIdNew(r[2].value, r[3].value) };
  });
}

function decodeAbsentReads(bytes: Uint8Array): any[] {
  const what = "invalid exclusive absent reads v1";
  return versionedReads(absentReadsDescriptor(), bytes, what).map((r) => {
    if (!shapeMatches(r, ["String", "Uuid"])) throw invalid(what);
    return { table: r[0].value, rowUuid: r[1].value };
  });
}

function decodePredicateReads(bytes: Uint8Array): any[] {
  const what = "invalid exclusive predicate reads v1";
  return versionedReads(predicateReadsDescriptor(), bytes, what).map((r) => {
    if (!shapeMatches(r, ["String", "Uuid", "Bytes", "Uuid", "Bytes"])) throw invalid(what);
    const bindings: Uint8Array = r[4].value;
    let bindingValues: any;
    try {
      bindingValues = bindingValuesFromCanonicalBytes(bindings);
    } catch {
      throw invalid(what);
    }
    if (!equalBytes(uuidV5(QUERY_NAMESPACE, bindings), r[3].value))
      throw invalid("exclusive predicate binding id mismatch");
    return {
      table: r[0].value,
      shapeId: r[1].value,
      shape: decodeQuery(r[2].value),
      bindingId: r[3].value,
      bindingValues,
    };
  });
}

export type StoredEvidence = {
  baseSnapshot?: any;
  rowReadSet?: any[];
  absentReadSet?: any[];
  predicateReadSet?: any[];
};

export function decodeEvidenceSlots(
  baseSnapshot?: Uint8Array,
  rowReadSet?: Uint8Array,
  absentReadSet?: Uint8Array,
  predicateReadSet?: Uint8Array,
): StoredEvidence {
  if (!baseSnapshot && (rowReadSet || absentReadSet || predicateReadSet))
    throw invalid("exclusive read sets stored without a base snapshot");
  return {
    baseSnapshot: baseSnapshot && decodeBaseSnapshot(baseSnapshot),
    rowReadSet: rowReadSet && decodeRowReads(rowReadSet),
    absentReadSet: absentReadSet && decodeAbsentReads(absentReadSet),
    predicateReadSet: predicateReadSet && decodePredicateReads(predicateReadSet),
  };
}
