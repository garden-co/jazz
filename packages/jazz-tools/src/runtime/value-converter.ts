/**
 * Convert JS values to WasmValue types for mutations.
 *
 * Used by Db insert/update paths to convert typed Init objects into
 * the runtime value format expected by JazzClient.
 */

import type { WasmSchema, ColumnType, Value as WasmValue, InsertValues } from "../drivers/types.js";
import { toJsonText } from "./json-text.js";

function toTimestampMs(value: unknown): number {
  const numeric = value instanceof Date ? value.getTime() : Number(value);
  if (!Number.isFinite(numeric)) {
    throw new Error("Invalid timestamp value. Expected Date or finite number.");
  }
  return numeric;
}

function normalizeByteaValue(value: unknown): Uint8Array {
  if (value instanceof Uint8Array) {
    return value;
  }

  if (Array.isArray(value)) {
    const bytes = value.map((entry) => {
      const n = Number(entry);
      if (!Number.isInteger(n) || n < 0 || n > 255) {
        throw new Error("Bytea arrays must contain integers in range 0..255");
      }
      return n;
    });
    return new Uint8Array(bytes);
  }

  throw new Error("Expected Uint8Array or byte array for Bytea column type");
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
      return { type: "Bytea", value: normalizeByteaValue(value) };
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
        value: { values: columns.map((col) => toValue(rowValue[col.name], col.column_type)) },
      };
    }
    default:
      throw new Error(`Unsupported column type: ${(columnType as { type: string }).type}`);
  }
}

/**
 * Convert a mutation object to a named WasmValue record.
 *
 * Only includes fields that are present in the data object.
 * Undefined values are skipped so Rust can apply schema defaults or leave
 * fields unchanged, depending on the write operation.
 *
 * @param data Object with fields to write
 * @param schema WasmSchema containing table definitions
 * @param tableName Name of the table being written
 * @returns Record mapping column names to WasmValues
 */
export function toWriteRecord(
  data: Record<string, unknown>,
  schema: WasmSchema,
  tableName: string,
): InsertValues {
  const table = schema[tableName];
  if (!table) {
    throw new Error(`Unknown table "${tableName}"`);
  }

  const result: InsertValues = {};
  for (const [key, value] of Object.entries(data)) {
    if (value === undefined) continue;
    const col = table.columns.find((c) => c.name === key);
    if (!col) {
      throw new Error(`Unknown column "${key}" on table "${tableName}"`);
    }
    if (value === null && !col.nullable) {
      throw new Error(`Cannot set required field '${key}' to null`);
    }
    if (col.column_type.type === "Json") {
      validateJsonSchemaValue(key, value, col.column_type.schema);
    }
    result[key] = toValue(value, col.column_type);
  }
  return result;
}

type JsonSchemaObject = Record<string, unknown>;

function validateJsonSchemaValue(
  column: string,
  value: unknown,
  schema: JsonSchemaObject | undefined,
): void {
  if (!schema) return;
  const error = validateJsonValue(value, schema);
  if (!error) return;
  throw new Error(
    `encoding error: JSON schema validation failed for column \`${column}\`: ${error}`,
  );
}

function validateJsonValue(value: unknown, schema: JsonSchemaObject): string | null {
  const type = schema.type;
  if (type === "object") {
    if (typeof value !== "object" || value === null || Array.isArray(value)) {
      return `${JSON.stringify(value)} is not an object`;
    }
    const object = value as Record<string, unknown>;
    const properties = objectSchemaProperties(schema);
    const required = new Set(stringArray(schema.required));
    for (const key of required) {
      if (!Object.hasOwn(object, key)) {
        return `missing required property ${JSON.stringify(key)}`;
      }
    }
    if (schema.additionalProperties === false) {
      for (const key of Object.keys(object)) {
        if (!Object.hasOwn(properties, key)) {
          return `unexpected property ${JSON.stringify(key)}`;
        }
      }
    }
    for (const [key, propertySchema] of Object.entries(properties)) {
      if (Object.hasOwn(object, key)) {
        const error = validateJsonValue(object[key], propertySchema);
        if (error) return error;
      }
    }
    return null;
  }
  if (type === "string") {
    if (typeof value !== "string") {
      return `${JSON.stringify(value)} is not a string`;
    }
    const minLength = numberKeyword(schema.minLength);
    if (minLength != null && value.length < minLength) {
      return `${JSON.stringify(value)} is shorter than ${minLength} character${minLength === 1 ? "" : "s"}`;
    }
    const maxLength = numberKeyword(schema.maxLength);
    if (maxLength != null && value.length > maxLength) {
      return `${JSON.stringify(value)} is longer than ${maxLength} characters`;
    }
    const pattern = stringKeyword(schema.pattern);
    if (pattern && !new RegExp(pattern).test(value)) {
      return `${JSON.stringify(value)} does not match ${JSON.stringify(displayPattern(pattern))}`;
    }
    return null;
  }
  if (type === "integer") {
    if (typeof value !== "number" || !Number.isInteger(value)) {
      return `${JSON.stringify(value)} is not an integer`;
    }
    const minimum = numberKeyword(schema.minimum);
    if (minimum != null && value < minimum) {
      return `${value} is less than the minimum of ${minimum}`;
    }
    const maximum = numberKeyword(schema.maximum);
    if (maximum != null && value > maximum) {
      return `${value} is greater than the maximum of ${maximum}`;
    }
    return null;
  }
  return null;
}

function objectSchemaProperties(schema: JsonSchemaObject): Record<string, JsonSchemaObject> {
  if (
    typeof schema.properties !== "object" ||
    schema.properties === null ||
    Array.isArray(schema.properties)
  ) {
    return {};
  }
  return Object.fromEntries(
    Object.entries(schema.properties).filter((entry): entry is [string, JsonSchemaObject] => {
      const [, value] = entry;
      return typeof value === "object" && value !== null && !Array.isArray(value);
    }),
  );
}

function stringArray(value: unknown): string[] {
  return Array.isArray(value)
    ? value.filter((entry): entry is string => typeof entry === "string")
    : [];
}

function numberKeyword(value: unknown): number | null {
  return typeof value === "number" && Number.isFinite(value) ? value : null;
}

function stringKeyword(value: unknown): string | null {
  return typeof value === "string" ? value : null;
}

function displayPattern(pattern: string): string {
  return pattern.replaceAll("\\d", "[0-9]");
}
