import type { InsertValues, Value, WasmRow } from "../drivers/types.js";

/** Compare schema values without losing bigint, byte, or floating-point identity. */
export function structuralValuesEqual(left: Value | undefined, right: Value | undefined): boolean {
  if (left === undefined || right === undefined) return left === right;
  if (left.type !== right.type) return false;
  switch (left.type) {
    case "Null":
      return true;
    case "Integer":
    case "Double":
    case "Timestamp":
    case "Boolean":
    case "Text":
      return Object.is(left.value, (right as typeof left).value);
    case "BigInt":
      return structuralBigInt(left.value) === structuralBigInt((right as typeof left).value);
    case "Uuid":
      return (
        left.value.replace(/-/g, "").toLowerCase() ===
        (right as typeof left).value.replace(/-/g, "").toLowerCase()
      );
    case "Bytea": {
      const other = (right as typeof left).value;
      return (
        left.value.length === other.length &&
        left.value.every((byte, index) => byte === other[index])
      );
    }
    case "Array":
      return valuesEqual(left.value, (right as typeof left).value);
    case "Row":
      return valuesEqual(left.value.values, (right as typeof left).value.values);
  }
  const exhaustive: never = left;
  throw new Error(`Unsupported structural value: ${String(exhaustive)}`);
}

function valuesEqual(left: Value[], right: Value[]): boolean {
  return (
    left.length === right.length &&
    left.every((value, index) => structuralValuesEqual(value, right[index]))
  );
}

/** Human JSON schemas carry i64 values as decimal strings; legacy safe numbers also occur. */
export function structuralBigInt(value: unknown): bigint {
  if (
    (typeof value !== "bigint" && typeof value !== "number" && typeof value !== "string") ||
    (typeof value === "number" && !Number.isSafeInteger(value)) ||
    (typeof value === "string" && !/^-?[0-9]+$/.test(value))
  ) {
    throw new Error("Invalid structural BigInt default: expected a signed 64-bit integer.");
  }
  const result = BigInt(value);
  if (result < -(1n << 63n) || result > (1n << 63n) - 1n) {
    throw new Error("Invalid structural BigInt default: expected a signed 64-bit integer.");
  }
  return result;
}

/** Snapshot row data, including nested values, named-value maps and byte arrays. */
export function cloneRowData<T extends WasmRow | InsertValues>(value: T): T {
  return cloneRowGraph(value, new WeakMap<object, unknown>());
}

function cloneRowGraph<T>(value: T, seen: WeakMap<object, unknown>): T {
  if (typeof value !== "object" || value === null) return value;
  const existing = seen.get(value);
  if (existing !== undefined) return existing as T;
  if (value instanceof Uint8Array) {
    const clone = new Uint8Array(value);
    seen.set(value, clone);
    return clone as T;
  }
  if (value instanceof Map) {
    const clone = new Map<unknown, unknown>();
    seen.set(value, clone);
    for (const [key, entry] of value) {
      clone.set(cloneRowGraph(key, seen), cloneRowGraph(entry, seen));
    }
    return clone as T;
  }
  if (Array.isArray(value)) {
    const clone: unknown[] = [];
    seen.set(value, clone);
    for (const entry of value) clone.push(cloneRowGraph(entry, seen));
    return clone as T;
  }

  const clone = Object.create(Object.getPrototypeOf(value)) as object;
  seen.set(value, clone);
  for (const key of Reflect.ownKeys(value)) {
    const descriptor = Object.getOwnPropertyDescriptor(value, key);
    if (!descriptor) continue;
    if ("value" in descriptor) {
      descriptor.value = cloneRowGraph(descriptor.value, seen);
    }
    Object.defineProperty(clone, key, descriptor);
  }
  return clone as T;
}
