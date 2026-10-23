import type { ColumnDescriptor, ColumnType } from "../drivers/types.js";

// Durable cell-record v1 encoding. Changes require fixture and format review.
export function encodeCellType(column: ColumnDescriptor): Uint8Array {
  return new TextEncoder().encode(canonicalJson(logicalColumn(column)));
}

function logicalColumn(column: ColumnDescriptor): Record<string, unknown> {
  return { column_type: logicalType(column.column_type), nullable: column.nullable };
}

function namedColumn(column: ColumnDescriptor): Record<string, unknown> {
  return { name: column.name, ...logicalColumn(column) };
}

function logicalType(type: ColumnType): Record<string, unknown> {
  switch (type.type) {
    case "Array":
      return { type: type.type, element: logicalType(type.element) };
    case "Row":
      return { type: type.type, columns: type.columns.map(namedColumn) };
    case "EnumPayload":
      return {
        type: type.type,
        cases: type.cases.map((entry) => ({
          name: entry.name,
          fields: entry.fields.map(namedColumn),
        })),
      };
    case "Enum":
      return { type: type.type, variants: type.variants };
    case "Json":
      return { type: type.type, ...(type.schema === undefined ? {} : { schema: type.schema }) };
    case "Integer":
    case "BigInt":
    case "Double":
    case "Boolean":
    case "Text":
    case "Timestamp":
    case "Uuid":
    case "Bytea":
      return { type: type.type };
    default:
      throw new Error("Unsupported encrypted logical type");
  }
}

function canonicalJson(value: unknown): string {
  if (value === null || typeof value === "string" || typeof value === "boolean")
    return JSON.stringify(value);
  if (typeof value === "number" && Number.isFinite(value)) return JSON.stringify(value);
  if (Array.isArray(value)) return `[${Array.from(value, canonicalJson).join(",")}]`;
  if (
    value &&
    typeof value === "object" &&
    (Object.getPrototypeOf(value) === Object.prototype || Object.getPrototypeOf(value) === null)
  ) {
    const record = value as Record<string, unknown>;
    return `{${Object.keys(record)
      .sort()
      .map((key) => `${JSON.stringify(key)}:${canonicalJson(record[key])}`)
      .join(",")}}`;
  }
  throw new Error("Encrypted logical types require finite JSON metadata");
}
