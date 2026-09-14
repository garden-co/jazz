import { createHash, randomUUID } from "node:crypto";
import type { ColumnDescriptor, ColumnType, TableSchema, WasmSchema } from "../drivers/types.js";
import { unwrapValue } from "../runtime/row-transformer.js";
import { DataError, nearestName } from "./errors.js";

export type DescribeMeta = {
  hasDefault: boolean;
  generated: boolean;
  sparse: boolean;
  mergeStrategy: string;
  typeDefinition: ColumnType;
};

export type DescribeRow = {
  column: string;
  type: string;
  nullable: boolean;
  default: unknown;
  references: string | null;
  indexed: boolean;
  branchKey: boolean;
  meta: DescribeMeta;
};

export type SchemaResult = {
  kind: "rows";
  columns: string[];
  rows: Record<string, unknown>[];
  schemaSource: string;
  schemaHash: string;
  tables: string[];
};

function typeName(type: ColumnType): string {
  switch (type.type) {
    case "Array":
      return `${typeName(type.element)}[]`;
    case "Enum":
      return `ENUM(${type.variants.map((value) => JSON.stringify(value)).join(", ")})`;
    case "EnumPayload":
      return `ENUM(${type.cases.map((entry) => entry.name).join(", ")})`;
    case "Row":
      return "ROW";
    default:
      return type.type.toUpperCase();
  }
}

/** Stable content identity of a schema, so callers can tell two views apart. */
export function schemaHashOf(schema: WasmSchema): string {
  return createHash("sha256").update(stableStringify(schema)).digest("hex");
}

function stableStringify(value: unknown): string {
  if (value === null || typeof value !== "object") return JSON.stringify(value) ?? "null";
  if (Array.isArray(value)) return `[${value.map(stableStringify).join(",")}]`;
  const entries = Object.entries(value as Record<string, unknown>)
    .filter(([, item]) => item !== undefined)
    .sort(([a], [b]) => (a < b ? -1 : a > b ? 1 : 0))
    .map(([key, item]) => `${JSON.stringify(key)}:${stableStringify(item)}`);
  return `{${entries.join(",")}}`;
}

export function toDescribeRows(table: TableSchema): DescribeRow[] {
  const columns: ColumnDescriptor[] = [
    { name: "id", column_type: { type: "Uuid" }, nullable: false },
    ...table.columns,
  ];
  return columns.map((column) => ({
    column: column.name,
    type: typeName(column.column_type),
    nullable: column.nullable,
    default:
      column.name === "id"
        ? "<generated>"
        : column.default === undefined
          ? null
          : unwrapValue(column.default, column.column_type),
    references: column.references ?? null,
    indexed: column.name === "id" || (table.indexed_columns?.includes(column.name) ?? false),
    branchKey: table.branchBy?.includes(column.name) ?? false,
    // Machine-only detail lives in one declared column, so the table view never
    // silently hides a field: hasDefault distinguishes an absent default from an
    // explicit NULL default, and typeDefinition retains the structured type.
    meta: {
      hasDefault: column.default !== undefined,
      generated: column.name === "id",
      sparse: column.sparse ?? false,
      mergeStrategy: column.merge_strategy ?? "LWW",
      typeDefinition: column.column_type,
    },
  }));
}

export function listTables(schema: WasmSchema): string[] {
  return Object.keys(schema).sort();
}

export function schemaListResult(schema: WasmSchema, schemaSource: string): SchemaResult {
  const tables = listTables(schema);
  return {
    kind: "rows",
    schemaSource,
    schemaHash: schemaHashOf(schema),
    tables,
    columns: ["table"],
    rows: tables.map((table) => ({ table })),
  };
}

export function schemaDescribeResult(
  schema: WasmSchema,
  tableName: string,
  schemaSource: string,
): SchemaResult & { value: DescribeRow[] } {
  if (!Object.hasOwn(schema, tableName)) {
    const suggestion = nearestName(tableName, Object.keys(schema));
    throw new DataError("UNKNOWN_TABLE", `Unknown table ${JSON.stringify(tableName)}`, {
      hint: suggestion
        ? `Did you mean ${JSON.stringify(suggestion)}? Run \`jazz-tools sql 'SHOW TABLES'\`.`
        : "Run `jazz-tools sql 'SHOW TABLES'` to list tables.",
    });
  }
  const rows = toDescribeRows(schema[tableName]!);
  return {
    kind: "rows",
    schemaSource,
    schemaHash: schemaHashOf(schema),
    tables: listTables(schema),
    columns: [
      "column",
      "type",
      "nullable",
      "default",
      "references",
      "indexed",
      "branchKey",
      "meta",
    ],
    rows: rows as unknown as Record<string, unknown>[],
    value: rows,
  };
}

/** Locally generated ids use this namespace so they cannot masquerade as server ids. */
export const CLIENT_GENERATED_ID_NAMESPACE = createHash("sha256")
  .update("jazz-tools:data-cli:client-generated-id:v1")
  .digest();

/**
 * Derive a deterministic v5-style UUID from a caller-supplied seed.
 *
 * The id is not random and does not prove prior existence, but it makes retrying
 * a failed INSERT idempotent: the same seed maps to the same row id.
 */
export function idFromSeed(seed: string): string {
  const digest = createHash("sha256")
    .update(CLIENT_GENERATED_ID_NAMESPACE)
    .update(seed)
    .digest()
    .subarray(0, 16);
  digest[6] = ((digest[6] ?? 0) & 0x0f) | 0x50;
  digest[8] = ((digest[8] ?? 0) & 0x3f) | 0x80;
  const hex = digest.toString("hex");
  return [
    hex.slice(0, 8),
    hex.slice(8, 12),
    hex.slice(12, 16),
    hex.slice(16, 20),
    hex.slice(20, 32),
  ].join("-");
}

export function randomClientId(): string {
  return randomUUID();
}
