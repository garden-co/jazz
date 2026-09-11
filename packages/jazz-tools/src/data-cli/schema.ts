import type { ColumnDescriptor, ColumnType, TableSchema, WasmSchema } from "../drivers/types.js";
import { unwrapValue } from "../runtime/row-transformer.js";
import type { DataResult } from "./sql.js";

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

export function listTables(schema: WasmSchema): DataResult {
  return {
    columns: ["table"],
    rows: Object.keys(schema)
      .sort()
      .map((table) => ({ table })),
  };
}

export function describeTable(table: TableSchema): DataResult {
  const columns: ColumnDescriptor[] = [
    { name: "id", column_type: { type: "Uuid" }, nullable: false },
    ...table.columns,
  ];
  return {
    columns: ["column", "type", "nullable", "default", "references", "indexed", "branchKey"],
    rows: columns.map((column) => ({
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
      // Machine-readable formats retain full structured type definitions and
      // distinguish an absent default from an explicit NULL default.
      hasDefault: column.default !== undefined,
      generated: column.name === "id",
      sparse: column.sparse ?? false,
      mergeStrategy: column.merge_strategy ?? "LWW",
      typeDefinition: column.column_type,
    })),
  };
}
