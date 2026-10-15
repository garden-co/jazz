import { toHex, equalBytes, type Bytes } from "../../../../support/bytes.js";
import type { ObjectId } from "../../../../_extern/model-core.js";
import type { ColumnType } from "./schema.js";

/** Value type for API boundary (insert input, query output). */
export type Value =
  | { type: "Integer"; value: number }
  | { type: "BigInt"; value: bigint }
  | { type: "Double"; value: number }
  | { type: "Boolean"; value: boolean }
  | { type: "Text"; value: string }
  | { type: "Timestamp"; value: number }
  | { type: "Uuid"; value: ObjectId }
  | { type: "TransactionId"; value: Bytes }
  | { type: "Bytea"; value: Bytes }
  | { type: "Array"; value: Value[] }
  | { type: "Row"; id?: ObjectId; values: Value[] }
  | { type: "Enum"; case: string; values: Value[] }
  | { type: "Null" };

export const NULL: Value = { type: "Null" };

export function debugValue(v: Value): string {
  switch (v.type) {
    case "TransactionId":
      return `TransactionId(${toHex(v.value)})`;
    case "Bytea":
      return `Bytea { len: ${v.value.length} }`;
    case "Array":
      return `Array { len: ${v.value.length} }`;
    case "Row":
      return `Row { id: ${v.id ?? "None"}, values_len: ${v.values.length} }`;
    case "Enum":
      return `Enum { case: ${JSON.stringify(v.case)}, values_len: ${v.values.length} }`;
    case "Null":
      return "Null";
    default:
      return `${v.type}(${String(v.value)})`;
  }
}

const NON_HUMAN_VALUE_CODEC_ERROR =
  "public Value does not support non-human serde codecs; use the descriptor/raw row codec";
export { NON_HUMAN_VALUE_CODEC_ERROR };

const I64_MIN = -(2n ** 63n);
const I64_MAX = 2n ** 63n - 1n;

function timestampFromJson(raw: unknown): number {
  if (typeof raw !== "number")
    throw new Error("invalid type: expected a non-negative integer timestamp");
  if (!Number.isFinite(raw)) throw new Error("timestamp must be finite");
  if (raw < 0) throw new Error("timestamp must be non-negative");
  if (raw % 1 !== 0) throw new Error("timestamp must be an integer");
  if (raw > 18446744073709551615) throw new Error("timestamp is out of range");
  return raw;
}

function bigintFromJson(raw: unknown): bigint {
  if (typeof raw === "string") {
    if (!/^[+-]?\d+$/.test(raw)) throw new Error("bigint string must be a signed 64-bit integer");
    const v = BigInt(raw);
    if (v < I64_MIN || v > I64_MAX)
      throw new Error("bigint string must be a signed 64-bit integer");
    return v;
  }
  if (typeof raw === "number" && Number.isInteger(raw)) {
    const v = BigInt(raw);
    if (v > I64_MAX) throw new Error("bigint is out of signed 64-bit range");
    return v;
  }
  if (typeof raw === "bigint") return raw;
  throw new Error("invalid type: expected a signed 64-bit integer or decimal string");
}

/** Decode the adjacently tagged JSON shape `{type, value}`. */
export function valueFromJson(json: any): Value {
  const t = json?.type;
  const p = json?.value;
  switch (t) {
    case "Integer":
    case "Double":
      return { type: t, value: Number(p) };
    case "BigInt":
      return { type: t, value: bigintFromJson(p) };
    case "Boolean":
      return { type: t, value: Boolean(p) };
    case "Text":
      return { type: t, value: String(p) };
    case "Timestamp":
      return { type: t, value: timestampFromJson(p) };
    case "Uuid":
      return { type: t, value: p };
    case "TransactionId":
    case "Bytea":
      return { type: t, value: Uint8Array.from(p) };
    case "Array":
      return { type: t, value: (p as any[]).map(valueFromJson) };
    case "Row": {
      const values = Array.isArray(p) ? p[1] : p.values;
      const id = Array.isArray(p) ? p[0] : p.id;
      return { type: t, id: id ?? undefined, values: (values as any[]).map(valueFromJson) };
    }
    case "Enum": {
      const kase = Array.isArray(p) ? p[0] : p.case;
      const values = Array.isArray(p) ? p[1] : p.values;
      return { type: t, case: kase, values: (values as any[]).map(valueFromJson) };
    }
    case "Null":
      return NULL;
    default:
      throw new Error(`unknown variant \`${t}\``);
  }
}

export function valueToJson(v: Value): any {
  switch (v.type) {
    case "BigInt":
      return { type: v.type, value: v.value.toString() };
    case "TransactionId":
    case "Bytea":
      return { type: v.type, value: [...v.value] };
    case "Array":
      return { type: v.type, value: v.value.map(valueToJson) };
    case "Row":
      return {
        type: v.type,
        value:
          v.id === undefined
            ? { values: v.values.map(valueToJson) }
            : { id: v.id, values: v.values.map(valueToJson) },
      };
    case "Enum":
      return { type: v.type, value: { case: v.case, values: v.values.map(valueToJson) } };
    case "Null":
      return { type: "Null" };
    default:
      return { type: v.type, value: v.value };
  }
}

const f64Bits = (n: number) => {
  const dv = new DataView(new ArrayBuffer(8));
  dv.setFloat64(0, n);
  return dv.getBigUint64(0);
};

/** Bitwise double equality: NaN == NaN and -0.0 != 0.0. */
export function valueEquals(a: Value, b: Value): boolean {
  if (a.type !== b.type) return false;
  switch (a.type) {
    case "Double":
      return f64Bits(a.value) === f64Bits((b as typeof a).value);
    case "TransactionId":
    case "Bytea":
      return equalBytes(a.value, (b as typeof a).value);
    case "Array":
      return valuesEqual(a.value, (b as typeof a).value);
    case "Row": {
      const o = b as typeof a;
      return a.id === o.id && valuesEqual(a.values, o.values);
    }
    case "Enum": {
      const o = b as typeof a;
      return a.case === o.case && valuesEqual(a.values, o.values);
    }
    case "Null":
      return true;
    default:
      return a.value === (b as any).value;
  }
}
export function valuesEqual(a: readonly Value[], b: readonly Value[]): boolean {
  return a.length === b.length && a.every((v, i) => valueEquals(v, b[i]));
}

/** Returns the column type this value represents, or undefined for Null/Row/Enum. */
export function valueColumnType(v: Value): ColumnType | undefined {
  switch (v.type) {
    case "Integer":
    case "BigInt":
    case "Double":
    case "Boolean":
    case "Text":
    case "Timestamp":
    case "Uuid":
    case "TransactionId":
    case "Bytea":
      return { type: v.type };
    case "Array":
      for (const e of v.value) {
        const t = valueColumnType(e);
        if (t) return { type: "Array", element: t };
      }
      return undefined;
    default:
      return undefined;
  }
}

export const isNull = (v: Value) => v.type === "Null";
export const asArray = (v: Value) => (v.type === "Array" ? v.value : undefined);
export const asRow = (v: Value) => (v.type === "Row" ? v.values : undefined);
export const rowIdOf = (v: Value) => (v.type === "Row" ? v.id : undefined);

/** Rust `From<T> for Value`, used by the builder DSLs. */
export type ValueInput = Value | boolean | number | bigint | string | Bytes | null | undefined;
export function toValue(v: ValueInput): Value {
  if (v === null || v === undefined) return NULL;
  if (typeof v === "boolean") return { type: "Boolean", value: v };
  if (typeof v === "number")
    return Number.isInteger(v) && Math.abs(v) < 2 ** 31
      ? { type: "Integer", value: v }
      : { type: "Double", value: v };
  if (typeof v === "bigint") return { type: "BigInt", value: v };
  if (typeof v === "string") return { type: "Text", value: v };
  if (v instanceof Uint8Array) return { type: "Bytea", value: v };
  return v;
}
