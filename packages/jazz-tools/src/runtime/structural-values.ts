import type { Value } from "../drivers/types.js";

/** Compare schema values without losing bigint, byte, or floating-point identity. */
export function structuralValuesEqual(left: Value | undefined, right: Value | undefined): boolean {
  if (left === undefined || right === undefined) return left === right;
  if (left.type !== right.type) return false;
  switch (left.type) {
    case "Null":
      return true;
    case "Integer":
    case "BigInt":
    case "Double":
    case "Timestamp":
    case "Boolean":
    case "Text":
      return Object.is(left.value, (right as typeof left).value);
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
    case "Enum": {
      const other = (right as typeof left).value;
      return left.value.case === other.case && valuesEqual(left.value.values, other.values);
    }
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
