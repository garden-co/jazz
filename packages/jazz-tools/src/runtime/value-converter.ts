/**
 * Convert JS values to WasmValue types for mutations.
 *
 * Used by Db.insert() and Db.update() to convert typed Init objects
 * into the Value[] format expected by JazzClient.
 */

import type { WasmSchema, ColumnType, Value as WasmValue } from "../drivers/types.js";
import { toJsonText } from "./json-text.js";

function toTimestampMs(value: unknown): number {
  const numeric = value instanceof Date ? value.getTime() : Number(value);
  if (!Number.isFinite(numeric)) {
    throw new Error("Invalid timestamp value. Expected Date or finite number.");
  }
  return numeric;
}

/**
 * Convert a JS value to WasmValue based on column type.
 */
export function toValue(value: unknown, columnType: ColumnType): WasmValue {
  if (value === null || value === undefined) {
    return { type: "Null" };
  }

  switch (columnType.type) {
    case "Text":
      return { type: "Text", value: String(value) };
    case "Boolean":
      return { type: "Boolean", value: Boolean(value) };
    case "Integer":
      return { type: "Integer", value: Number(value) };
    case "BigInt":
      return { type: "BigInt", value: Number(value) };
    case "Double":
      return { type: "Double", value: Number(value) };
    case "Timestamp":
      return { type: "Timestamp", value: toTimestampMs(value) };
    case "Uuid":
      return { type: "Uuid", value: String(value) };
    case "Bytea": {
      if (value instanceof Uint8Array) {
        return { type: "Bytea", value };
      }
      if (Array.isArray(value)) {
        const bytes = value.map((entry) => {
          const n = Number(entry);
          if (!Number.isInteger(n) || n < 0 || n > 255) {
            throw new Error("Bytea arrays must contain integers in range 0..255");
          }
          return n;
        });
        return { type: "Bytea", value: new Uint8Array(bytes) };
      }
      throw new Error("Expected Uint8Array or byte array for Bytea column type");
    }
    case "Json":
      return { type: "Text", value: toJsonText(value) };
    case "Enum": {
      const enumValue = String(value);
      if (!columnType.variants.includes(enumValue)) {
        throw new Error(
          `Invalid enum value "${enumValue}". Expected one of: ${columnType.variants.join(", ")}`,
        );
      }
      return { type: "Text", value: enumValue };
    }
    case "Array": {
      if (!Array.isArray(value)) {
        throw new Error(`Expected array for Array column type, got ${typeof value}`);
      }
      const elementType = columnType.element;
      return {
        type: "Array",
        value: value.map((v) => toValue(v, elementType)),
      };
    }
    case "Row": {
      // Row type expects an object with named fields
      if (typeof value !== "object" || value === null) {
        throw new Error(`Expected object for Row column type, got ${typeof value}`);
      }
      const rowValue = value as Record<string, unknown>;
      const columns = columnType.columns;
      return {
        type: "Row",
        value: columns.map((col) => toValue(rowValue[col.name], col.column_type)),
      };
    }
    default:
      throw new Error(`Unsupported column type: ${(columnType as { type: string }).type}`);
  }
}

/**
 * Convert Init object to Value[] in schema column order.
 *
 * @param data The Init object with field values
 * @param schema WasmSchema containing table definitions
 * @param tableName Name of the table to insert into
 * @returns Array of WasmValue in column order
 */
export function toValueArray(
  data: Record<string, unknown>,
  schema: WasmSchema,
  tableName: string,
): WasmValue[] {
  const table = schema.tables[tableName];
  if (!table) {
    throw new Error(`Unknown table "${tableName}"`);
  }

  return table.columns.map((col) => {
    const value = data[col.name];
    return toValue(value, col.column_type);
  });
}

/**
 * Convert partial update object to Record<string, WasmValue>.
 *
 * Only includes fields that are present in the data object.
 * Undefined values are skipped.
 *
 * @param data Partial object with fields to update
 * @param schema WasmSchema containing table definitions
 * @param tableName Name of the table being updated
 * @returns Record mapping column names to WasmValues
 */
export function toUpdateRecord(
  data: Record<string, unknown>,
  schema: WasmSchema,
  tableName: string,
): Record<string, WasmValue> {
  const table = schema.tables[tableName];
  if (!table) {
    throw new Error(`Unknown table "${tableName}"`);
  }

  const result: Record<string, WasmValue> = {};
  for (const [key, value] of Object.entries(data)) {
    if (value === undefined) continue;
    const col = table.columns.find((c) => c.name === key);
    if (!col) {
      throw new Error(`Unknown column "${key}" on table "${tableName}"`);
    }
    result[key] = toValue(value, col.column_type);
  }
  return result;
}
