// Merge columns as operations: counter deltas and g-set additions relative to the write's base image.
import { toHex } from "../../support/bytes.js";
import {
  type Value,
  type ValueType,
  COUNTER_SIGNS_FIELD,
  RecordDescriptor,
} from "../../_extern/node-state.js";
import { invalidCommit, invalidStored } from "./index.js";

export type MergeStrategy = "Lww" | "Counter" | "GSet";

export function counterToBig(value: Value): bigint {
  switch (value.kind) {
    case "U8":
    case "U16":
    case "U32":
    case "U64":
    case "I32":
    case "I64":
      return BigInt(value.value);
    case "Nullable":
      return value.value === undefined ? 0n : counterToBig(value.value);
    default:
      throw invalidStored("counter value must be integer");
  }
}

const RANGES: Record<string, [number, bigint, bigint]> = {
  U8: [8, 0n, 255n],
  U16: [16, 0n, 65535n],
  U32: [32, 0n, 0xffffffffn],
  U64: [64, 0n, 0xffffffffffffffffn],
  I32: [32, -(1n << 31n), (1n << 31n) - 1n],
  I64: [64, -(1n << 63n), (1n << 63n) - 1n],
};

/** The bit width and value range of a counter column's integer type. */
function counterRange(columnType: ValueType): [number, bigint, bigint] {
  if (columnType.kind === "Nullable") return counterRange(columnType.inner);
  const r = RANGES[columnType.kind];
  if (!r) throw invalidStored("counter strategy requires integer column");
  return r;
}

/** `value`'s low bits in the column's integer type (two's-complement residue). */
function counterBits(columnType: ValueType, value: bigint): Value {
  if (columnType.kind === "Nullable") return counterBits(columnType.inner, value);
  const [width] = counterRange(columnType);
  const signed = columnType.kind === "I32" || columnType.kind === "I64";
  const v = signed ? BigInt.asIntN(width, value) : BigInt.asUintN(width, value);
  return { kind: columnType.kind, value: width === 64 ? v : Number(v) };
}

/** A counter op: the exact delta of one write, one bit wider than the column's type. */
export function counterOp(columnType: ValueType, delta: bigint): [Value, boolean] {
  const [width] = counterRange(columnType);
  const span = 1n << BigInt(width);
  if (delta < -span || delta >= span)
    throw invalidCommit("counter delta exceeds the span of the column's type");
  return [counterBits(columnType, delta), delta < 0n];
}

export function counterOpDelta(columnType: ValueType, op: Value, negative: boolean): bigint {
  const [width] = counterRange(columnType);
  const span = 1n << BigInt(width);
  const low = ((counterToBig(op) % span) + span) % span;
  return negative ? low - span : low;
}

/** A counter's value after one op, or undefined when the sum leaves the column's range. */
export function counterAfterOp(
  columnType: ValueType,
  previous: Value,
  op: Value,
  negative: boolean,
): Value | undefined {
  const [, min, max] = counterRange(columnType);
  const sum = counterToBig(previous) + counterOpDelta(columnType, op, negative);
  if (sum < min || sum > max) return undefined;
  return counterBits(columnType, sum);
}

/** Ordinal of each counter column among the table's counter columns, in schema order. */
function counterOrdinals(table: any): (number | undefined)[] {
  let next = 0;
  return table.columns.map((c: any) =>
    table.mergeStrategy(c.name) === "Counter" ? next++ : undefined,
  );
}

export function counterSign(table: any, signs: Uint8Array, index: number): boolean {
  const bit = counterOrdinals(table)[index];
  return bit !== undefined && ((signs[bit >> 3] ?? 0) & (1 << (bit % 8))) !== 0;
}

export function clearCounterSigns(values: Value[], descriptor: any): void {
  const field = descriptor.fieldIndex(COUNTER_SIGNS_FIELD);
  if (field !== undefined) values[field] = { kind: "Bytes", value: new Uint8Array() };
}

/** Counter signs are canonical: only counter columns, no trailing zero byte. */
export function validateCounterSigns(table: any, signs: Uint8Array): void {
  const counters = counterOrdinals(table).filter((o) => o !== undefined).length;
  const canonical =
    (signs.length === 0 || signs[signs.length - 1] !== 0) &&
    signs.length <= Math.ceil(counters / 8) &&
    [...signs].every((byte, i) => {
      for (let bit = 0; bit < 8; bit++)
        if (byte & (1 << bit) && i * 8 + bit >= counters) return false;
      return true;
    });
  if (!canonical)
    throw invalidCommit(
      "counter signs must name only counter columns, without trailing zero bytes",
    );
}

function gsetElements(
  columnType: ValueType,
  values: Value[],
  elements: Map<string, [Uint8Array, Value]>,
): void {
  if (columnType.kind !== "Array")
    throw invalidStored("g-set merge strategy requires an array column");
  const descriptor = RecordDescriptor.new([["element", columnType.inner]]);
  for (const value of values) {
    const key = descriptor.create([value]);
    const k = toHex(key);
    if (!elements.has(k)) elements.set(k, [key, value]);
  }
}

function gsetValues(value: Value | undefined): Value[] {
  if (!value) return [];
  if (value.kind === "Array") return [...value.value];
  if (value.kind === "Nullable") return gsetValues(value.value);
  return [];
}

const sortedElements = (m: Map<string, [Uint8Array, Value]>) =>
  [...m.entries()].sort(([a], [b]) => (a < b ? -1 : a > b ? 1 : 0));

/** The canonical union of two g-set values. */
export function gsetUnion(columnType: ValueType, left?: Value, right?: Value): Value {
  const elements = new Map<string, [Uint8Array, Value]>();
  gsetElements(columnType, gsetValues(left), elements);
  gsetElements(columnType, gsetValues(right), elements);
  return { kind: "Array", value: sortedElements(elements).map(([, [, v]]) => v) };
}

/** The elements of `written` missing from `base`: the add op of a g-set write. */
export function gsetAdded(columnType: ValueType, written?: Value, base?: Value): Value {
  const known = new Map<string, [Uint8Array, Value]>();
  gsetElements(columnType, gsetValues(base), known);
  const added = new Map<string, [Uint8Array, Value]>();
  gsetElements(columnType, gsetValues(written), added);
  return {
    kind: "Array",
    value: sortedElements(added)
      .filter(([k]) => !known.has(k))
      .map(([, [, v]]) => v),
  };
}

/** Split an authored write into the patch's op cells and the local image; returns image and signs. */
export function splitMergeOps(
  tableSchema: any,
  authored: Set<string>,
  base: Map<string, Value>,
  cells: Map<string, Value>,
): [Map<string, Value> | undefined, Uint8Array] {
  let image: Map<string, Value> | undefined;
  const signs: number[] = [];
  const ordinals = counterOrdinals(tableSchema);
  tableSchema.columns.forEach((column: any, i: number) => {
    const strategy: MergeStrategy = tableSchema.mergeStrategy(column.name);
    if (strategy === "Lww" || !authored.has(column.name)) return;
    const written = cells.get(column.name);
    if (written === undefined) return;
    image ??= new Map(cells);
    const baseValue = base.get(column.name);
    if (strategy === "Counter") {
      const delta = counterToBig(written) - (baseValue ? counterToBig(baseValue) : 0n);
      const [op, negative] = counterOp(column.columnType, delta);
      if (negative) {
        const bit = ordinals[i];
        if (bit === undefined) throw invalidStored("counter column must have a sign ordinal");
        while (signs.length <= bit >> 3) signs.push(0);
        signs[bit >> 3] |= 1 << (bit % 8);
      }
      cells.set(column.name, op);
    } else {
      image.set(column.name, gsetUnion(column.columnType, baseValue, written));
      cells.set(column.name, gsetAdded(column.columnType, written, baseValue));
    }
  });
  return [image, Uint8Array.from(signs)];
}

/** Apply an accepted patch's merge-column op onto the previous image's value. */
export function applyMergeOp(
  strategy: MergeStrategy,
  columnType: ValueType,
  previous: Value,
  op: Value,
  negative: boolean,
): Value {
  switch (strategy) {
    case "Counter": {
      const v = counterAfterOp(columnType, previous, op, negative);
      if (v === undefined) throw invalidStored("accepted counter op leaves the column's range");
      return v;
    }
    case "GSet":
      return gsetUnion(columnType, previous, op);
    case "Lww":
      return op;
  }
}

/** Apply a still-pending patch's op onto a local base; an out-of-range counter keeps the base. */
export function applyPendingMergeOp(
  strategy: MergeStrategy,
  columnType: ValueType,
  previous: Value,
  op: Value,
  negative: boolean,
): Value {
  if (strategy === "Counter")
    return counterAfterOp(columnType, previous, op, negative) ?? counterValueWithoutNull(previous);
  return applyMergeOp(strategy, columnType, previous, op, negative);
}

function counterValueWithoutNull(value: Value): Value {
  return value.kind === "Nullable" && value.value !== undefined
    ? counterValueWithoutNull(value.value)
    : value;
}
