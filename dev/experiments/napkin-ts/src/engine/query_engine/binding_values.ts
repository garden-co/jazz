import { uuidFromString, uuidToString } from "../../support/hash.js";
import type { ColumnType, Value } from "../../_extern/engine.js";

const INT_RANGES: Record<string, [bigint, bigint]> = {
  U8: [0n, 255n],
  U16: [0n, 65535n],
  U32: [0n, 4294967295n],
  U64: [0n, 18446744073709551615n],
  I32: [-2147483648n, 2147483647n],
  I64: [-9223372036854775808n, 9223372036854775807n],
};

/** Canonicalizes a bound value to the descriptor representation used by a prepared binding source. */
export function coercePreparedBindingValue(value: Value, columnType: ColumnType): Value {
  const integer = coercePreparedIntegerValue(value, columnType);
  if (integer !== undefined) return integer;
  if (value.kind === "Uuid" && columnType.kind === "String") {
    return { kind: "String", value: uuidToString(value.value) };
  }
  if (value.kind === "String" && columnType.kind === "Uuid") {
    try {
      return { kind: "Uuid", value: uuidFromString(value.value) };
    } catch {
      return value;
    }
  }
  if (value.kind === "Nullable" && columnType.kind === "Nullable") {
    return {
      kind: "Nullable",
      value:
        value.value === undefined
          ? undefined
          : coercePreparedBindingValue(value.value, columnType.inner),
    };
  }
  if (value.kind === "Nullable" && value.value !== undefined) {
    return { kind: "Nullable", value: coercePreparedBindingValue(value.value, columnType) };
  }
  if (value.kind === "Nullable") return value;
  if (value.kind === "Array" && columnType.kind === "Array") {
    return {
      kind: "Array",
      value: value.value.map((v: Value) => coercePreparedBindingValue(v, columnType.inner)),
    };
  }
  if (
    value.kind === "Tuple" &&
    columnType.kind === "Tuple" &&
    value.value.length === columnType.types.length
  ) {
    return {
      kind: "Tuple",
      value: value.value.map((v: Value, i: number) =>
        coercePreparedBindingValue(v, columnType.types[i]),
      ),
    };
  }
  if (columnType.kind === "Nullable") {
    return { kind: "Nullable", value: coercePreparedBindingValue(value, columnType.inner) };
  }
  return value;
}

/** Failed conversions return undefined so the original typed value stays in the binding. */
function coercePreparedIntegerValue(value: Value, columnType: ColumnType): Value | undefined {
  if (!(value.kind in INT_RANGES)) return undefined;
  const range = INT_RANGES[columnType.kind];
  if (!range) return undefined;
  const n = BigInt(value.value);
  if (n < range[0] || n > range[1]) return undefined;
  const big = columnType.kind === "U64" || columnType.kind === "I64";
  return { kind: columnType.kind, value: big ? n : Number(n) };
}
