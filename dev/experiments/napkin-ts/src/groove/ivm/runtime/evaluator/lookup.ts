// Keyed lookup: a table source probed by the keys of its only input. Nothing
// here is proportional to the table's size.
import { type Bytes, compareBytes, equalBytes, toHex } from "../../../../support/bytes.js";
import { SortedMap } from "../../../../support/collections.js";
import {
  AsOf,
  ScopeId,
  IvmRuntimeError,
  emptyDeltas,
  encodeRecordFieldKeyPart,
  primaryKeyFieldIndices,
  scanBounds,
  subTickCmp,
  type LookupOp,
  type NodeId,
  type RecordDelta,
  type RecordDeltas,
  type RecordDescriptor,
} from "../../../../_extern/groove-ivm-ops.js";
import type { TickEvaluator } from "../evaluator.js";
import { NodeState } from "../operator_updates.js";

type Rows = SortedMap<Bytes, Bytes>;

/** Maintained state of one lookup node; copied only when a tick changes it. */
export class LookupState {
  /** Multiplicity of each live key (hex); zero counts are not kept. */
  keys = new Map<string, number>();
  /** The matched rows of each live key, by encoded primary key. */
  rows = new Map<string, Rows>();

  live(key: string): boolean {
    return (this.keys.get(key) ?? 0) > 0;
  }

  clone(): LookupState {
    const s = new LookupState();
    s.keys = new Map(this.keys);
    s.rows = new Map(this.rows);
    return s;
  }
}

function encodedKey(descriptor: RecordDescriptor, record: Bytes, fields: number[]): Bytes {
  const key: number[] = [];
  for (const field of fields) encodeRecordFieldKeyPart(key, descriptor, record, field);
  return Uint8Array.from(key);
}

interface LookupLayout {
  prefix: Bytes;
  primaryKey: number[];
  keyColumns: number[];
}

function lookupLayout(ev: TickEvaluator, lookup: LookupOp, output: RecordDescriptor): LookupLayout {
  const table = ev.schema.table(lookup.source.table);
  if (!table) throw new IvmRuntimeError("TableNotFound", lookup.source.table);
  const primaryKey = primaryKeyFieldIndices(table, output);
  const scan = lookup.source.scan;
  let prefixLen = 0;
  if (scan) {
    if (scan.kind !== "Prefix" && scan.kind !== "Point") {
      throw new IvmRuntimeError(
        "UnsupportedLookup",
        "a lookup's static scan must be a point or prefix",
      );
    }
    prefixLen = scan.values.length;
  }
  let prefix: Bytes = new Uint8Array();
  if (scan) {
    const bounds = scanBounds(scan);
    if (bounds.kind !== "Prefix") {
      throw new Error(
        "internal error: entered unreachable code: point and prefix scans have prefix bounds",
      );
    }
    prefix = bounds.prefix;
  }
  if (prefixLen + lookup.keyFields.length > primaryKey.length) {
    throw new IvmRuntimeError(
      "UnsupportedLookup",
      "a lookup's prefix and keys exceed the table's primary key",
    );
  }
  return {
    prefix,
    primaryKey,
    keyColumns: primaryKey.slice(prefixLen, prefixLen + lookup.keyFields.length),
  };
}

/** Read every row of `keys`; registers all missing probes before blocking. */
function probeLookupKeys(
  ev: TickEvaluator,
  lookup: LookupOp,
  output: RecordDescriptor,
  layout: LookupLayout,
  keys: Bytes[],
): Map<string, Rows> {
  const inputs = ev.evaluationInputs;
  if (!inputs) {
    throw new IvmRuntimeError(
      "UnsupportedLookup",
      "a lookup reads storage and cannot run in a recursive step or durable node",
    );
  }
  const request = (key: Bytes) => {
    const prefix = new Uint8Array(layout.prefix.length + key.length);
    prefix.set(layout.prefix);
    prefix.set(key, layout.prefix.length);
    return { kind: "ScanPrefix" as const, family: lookup.source.table, prefix };
  };
  let blocked = false;
  for (const key of keys) {
    try {
      inputs.rows(request(key));
    } catch (error: any) {
      if (error?.kind !== "EvaluationBlocked") throw error;
      blocked = true;
    }
  }
  if (blocked) throw new IvmRuntimeError("EvaluationBlocked");
  const table = ev.schema.table(lookup.source.table);
  if (!table) throw new IvmRuntimeError("TableNotFound", lookup.source.table);
  // The probe prefix already bounds the rows; project them like the table source would.
  const source = { ...lookup.source, scan: undefined };
  const probed = new Map<string, Rows>();
  for (const key of keys) {
    const stored = inputs.rows(request(key));
    const grouped = NodeState.groupSourceRows(
      table,
      stored.map(([, row]) => row),
    );
    const projected = NodeState.updateTableSource(
      source,
      ev.schema,
      ev.variantProjections,
      output,
      grouped,
    );
    const k = toHex(key);
    let rows = probed.get(k);
    if (!rows) probed.set(k, (rows = new SortedMap(compareBytes)));
    for (const delta of projected.deltas)
      rows.set(encodedKey(output, delta.record, layout.primaryKey), delta.record);
  }
  return probed;
}

export function updateLookup(
  ev: TickEvaluator,
  node: NodeId,
  lookup: LookupOp,
  outputDesc: RecordDescriptor,
  rawInput: RecordDeltas,
): RecordDeltas {
  if (ev.context.scope !== ScopeId.root()) {
    throw new IvmRuntimeError("UnsupportedLookup", "a lookup cannot run in a recursive step");
  }
  const layout = lookupLayout(ev, lookup, outputDesc);
  const input = ev.materializeIndirectFieldIndices(rawInput, lookup.keyFields);
  const keyDeltas = new SortedMap<Bytes, number>(compareBytes);
  for (const delta of input.deltas) {
    const key = encodedKey(input.descriptor, delta.record, lookup.keyFields);
    keyDeltas.set(key, (keyDeltas.get(key) ?? 0) + delta.weight);
  }
  const operatorKey = ev.operatorKey(node);
  const subTick = { tick: ev.currentTick, subTick: 0 };
  const replace = ev.context.arrangementUpdateMode === "Replace";
  const existing = ev.operatorStates.get(operatorKey);
  if (existing && existing.kind !== "Lookup")
    throw new IvmRuntimeError("NodeStateOperatorMismatch", node);
  const state = existing?.state;
  if (state?.asOf !== undefined) {
    if (subTickCmp(state.asOf, subTick) > 0) {
      throw new IvmRuntimeError("OutOfOrderRuntimeState", {
        current: JSON.stringify(state.asOf),
        next: JSON.stringify(subTick),
      });
    }
    if (!replace && subTickCmp(state.asOf, subTick) === 0) return emptyDeltas(outputDesc);
  }
  // Hydration supplies the complete key relation; a rebuild starts from nothing.
  const before: LookupState =
    state && !replace ? (state.value ?? new LookupState()) : new LookupState();
  const entering: Bytes[] = [];
  const leaving = new Set<string>();
  for (const [key, delta] of keyDeltas) {
    const old = before.keys.get(toHex(key)) ?? 0;
    const now = old + delta;
    if (old <= 0 && now > 0) entering.push(key);
    else if (old > 0 && now <= 0) leaving.add(toHex(key));
  }
  // Read every entering key before touching state: a blocked probe re-polls.
  const probed = probeLookupKeys(ev, lookup, outputDesc, layout, entering);
  const tableRows = replace
    ? emptyDeltas(outputDesc)
    : NodeState.updateTableSource(
        lookup.source,
        ev.schema,
        ev.variantProjections,
        outputDesc,
        ev.tableDeltas,
      );

  let operator = ev.operatorStates.get(operatorKey);
  ev.operatorStates.delete(operatorKey);
  if (!operator || replace) operator = { kind: "Lookup", state: new AsOf(new LookupState()) };
  if (operator.kind !== "Lookup") throw new IvmRuntimeError("NodeStateOperatorMismatch", node);
  const asOf = operator.state;
  const current: LookupState = asOf.value ?? new LookupState();
  const changes: [number, string, Bytes, Bytes][] = [];
  for (const delta of tableRows.deltas) {
    const key = toHex(encodedKey(outputDesc, delta.record, layout.keyColumns));
    if (!current.live(key) || leaving.has(key)) continue;
    changes.push([
      delta.weight,
      key,
      encodedKey(outputDesc, delta.record, layout.primaryKey),
      delta.record,
    ]);
  }
  const output: RecordDelta[] = [];
  if (leaving.size || changes.length || probed.size || keyDeltas.size) {
    const contents = current.clone();
    for (const key of leaving) {
      const rows = contents.rows.get(key);
      contents.rows.delete(key);
      if (rows) for (const record of rows.valuesArray()) output.push({ record, weight: -1 });
    }
    // Retractions first; converge with probes that already saw a later write.
    changes.sort((a, b) => Number(a[0] > 0) - Number(b[0] > 0));
    for (const [weight, key, primaryKey, record] of changes) {
      let rows = contents.rows.get(key);
      if (!rows) contents.rows.set(key, (rows = new SortedMap(compareBytes)));
      if (weight < 0) {
        const stored = rows.get(primaryKey);
        if (stored && equalBytes(stored, record)) {
          rows.delete(primaryKey);
          output.push({ record, weight: -1 });
        }
      } else if (weight > 0 && !rows.has(primaryKey)) {
        rows.set(primaryKey, record);
        output.push({ record, weight: 1 });
      }
    }
    for (const [key, rows] of probed) {
      for (const record of rows.valuesArray()) output.push({ record, weight: 1 });
      contents.rows.set(key, rows);
    }
    for (const [keyBytes, delta] of keyDeltas) {
      const key = toHex(keyBytes);
      const count = (contents.keys.get(key) ?? 0) + delta;
      if (count === 0) contents.keys.delete(key);
      else contents.keys.set(key, count);
      if (!contents.live(key)) contents.rows.delete(key);
    }
    for (const [key, rows] of [...contents.rows]) if (rows.size === 0) contents.rows.delete(key);
    asOf.value = contents;
  }
  asOf.markForwardAsOf(subTick);
  ev.operatorStates.set(operatorKey, operator);
  return { descriptor: outputDesc, deltas: output };
}
