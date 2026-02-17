/**
 * Convert TS DSL Schema to WasmSchema JSON format.
 */

import type { Schema, SqlType } from "../schema.js";
import type { WasmSchema, ColumnType, ColumnDescriptor, TableSchema } from "../drivers/types.js";

/**
 * Convert a DSL SqlType to WasmColumnType format.
 */
function sqlTypeToWasm(sqlType: SqlType): ColumnType {
  const map: Record<SqlType, ColumnType> = {
    TEXT: { type: "Text" },
    BOOLEAN: { type: "Boolean" },
    INTEGER: { type: "Integer" },
    REAL: { type: "Integer" }, // REAL maps to Integer in WASM (no Float type)
    UUID: { type: "Uuid" },
  };
  return map[sqlType];
}

/**
 * Convert a TS DSL Schema to WasmSchema format.
 *
 * This produces a JSON-serializable structure that can be passed to the WASM runtime.
 */
export function schemaToWasm(schema: Schema): WasmSchema {
  const tables: Record<string, TableSchema> = {};

  for (const table of schema.tables) {
    const columns: ColumnDescriptor[] = table.columns.map((col) => {
      const descriptor: ColumnDescriptor = {
        name: col.name,
        column_type: sqlTypeToWasm(col.sqlType),
        nullable: col.nullable,
      };
      if (col.references) {
        descriptor.references = col.references;
      }
      return descriptor;
    });

    tables[table.name] = { columns };
  }

  return { tables };
}
