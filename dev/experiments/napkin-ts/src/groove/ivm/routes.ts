// Route index for prepared-shape bindings (#3288).
import type { NodeId } from "./graph.js";
import {
  type BorrowedRecord,
  type Value,
  type ValueType,
  encodeSingleFieldValue,
} from "../../_extern/groove-ivm-plan.js";
import { toHex } from "../../support/bytes.js";

/** One shared terminal's barriers, grouped by encoded route key (hex). */
export class RouteTable {
  fieldIndices: number[] = [];
  fieldTypes: ValueType[] = [];
  byKey = new Map<string, Set<NodeId>>();
  rootOrderingNodes = new Set<NodeId>();

  keyOfRecord(record: BorrowedRecord): Uint8Array | undefined {
    const values: Value[] = [];
    for (const index of this.fieldIndices) {
      try {
        values.push(record.getIdx(index));
      } catch {
        return undefined;
      }
    }
    return encodeRouteKey(values, this.fieldTypes);
  }

  *barriers(): IterableIterator<NodeId> {
    for (const nodes of this.byKey.values()) yield* nodes;
  }
}

interface Barrier {
  terminal: NodeId;
  key: string;
  refs: number;
}

export class RouteIndex {
  private tables = new Map<NodeId, RouteTable>();
  private barriers = new Map<NodeId, Barrier>();

  isBarrier(node: NodeId): boolean {
    return this.barriers.has(node);
  }
  table(terminal: NodeId): RouteTable | undefined {
    return this.tables.get(terminal);
  }
  hasTable(terminal: NodeId): boolean {
    return this.tables.has(terminal);
  }

  /** Returns whether the topology changed (a new barrier). */
  add(
    terminal: NodeId,
    barrier: NodeId,
    fieldIndices: number[],
    fieldTypes: ValueType[],
    keyBytes: Uint8Array,
    rootOrderingNode?: NodeId,
  ): boolean {
    const key = toHex(keyBytes);
    const existing = this.barriers.get(barrier);
    if (existing) {
      existing.refs++;
      return false;
    }
    let table = this.tables.get(terminal);
    if (!table) this.tables.set(terminal, (table = new RouteTable()));
    if (!table.fieldIndices.length) {
      table.fieldIndices = fieldIndices;
      table.fieldTypes = fieldTypes;
    }
    let nodes = table.byKey.get(key);
    if (!nodes) table.byKey.set(key, (nodes = new Set()));
    nodes.add(barrier);
    if (rootOrderingNode !== undefined) table.rootOrderingNodes.add(rootOrderingNode);
    this.barriers.set(barrier, { terminal, key, refs: 1 });
    return true;
  }

  /** Release one reference. Returns whether the barrier was removed. */
  release(barrier: NodeId): boolean {
    const entry = this.barriers.get(barrier);
    if (!entry) return false;
    if (--entry.refs > 0) return false;
    return this.remove(barrier);
  }

  /** Forget a barrier regardless of references (its node left the graph). */
  remove(barrier: NodeId): boolean {
    const entry = this.barriers.get(barrier);
    if (!entry) return false;
    this.barriers.delete(barrier);
    const table = this.tables.get(entry.terminal);
    if (table) {
      const nodes = table.byKey.get(entry.key);
      if (nodes) {
        nodes.delete(barrier);
        if (!nodes.size) table.byKey.delete(entry.key);
      }
      if (!table.byKey.size) this.tables.delete(entry.terminal);
    }
    return true;
  }

  clone(): RouteIndex {
    const r = new RouteIndex();
    for (const [k, t] of this.tables) {
      const c = new RouteTable();
      c.fieldIndices = t.fieldIndices;
      c.fieldTypes = t.fieldTypes;
      for (const [key, nodes] of t.byKey) c.byKey.set(key, new Set(nodes));
      c.rootOrderingNodes = new Set(t.rootOrderingNodes);
      r.tables.set(k, c);
    }
    for (const [k, b] of this.barriers) r.barriers.set(k, { ...b });
    return r;
  }
}

/** Fixed-width field types whose route equality is byte equality. */
export function isRoutableType(t: ValueType): boolean {
  switch (t.kind) {
    case "U8":
    case "U16":
    case "U32":
    case "U64":
    case "U48":
    case "I32":
    case "I64":
    case "Bool":
    case "Uuid":
      return true;
    case "Nullable":
      return isRoutableType(t.inner);
    default:
      return false;
  }
}

/** Length-delimited concatenation of each route value's canonical encoding. */
export function encodeRouteKey(values: Value[], types: ValueType[]): Uint8Array | undefined {
  if (values.length !== types.length) return undefined;
  const parts: number[] = [];
  for (let i = 0; i < values.length; i++) {
    let encoded: Uint8Array;
    try {
      encoded = encodeSingleFieldValue(values[i], types[i]);
    } catch {
      return undefined;
    }
    const n = encoded.length;
    parts.push(n & 0xff, (n >>> 8) & 0xff, (n >>> 16) & 0xff, (n >>> 24) & 0xff, ...encoded);
  }
  return Uint8Array.from(parts);
}
