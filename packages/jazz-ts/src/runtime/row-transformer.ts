/**
 * Transform WASM row results to typed TypeScript objects.
 */

import type { WasmRow, WasmSchema } from "../drivers/types.js";

/**
 * WasmValue union type - matches the tsify output from Rust.
 * Each variant has a type discriminator and optional value.
 */
export type WasmValue =
  | { type: "Text"; value: string }
  | { type: "Uuid"; value: string }
  | { type: "Boolean"; value: boolean }
  | { type: "Integer"; value: number }
  | { type: "BigInt"; value: number }
  | { type: "Timestamp"; value: number }
  | { type: "Null" }
  | { type: "Array"; value: WasmValue[] }
  | { type: "Row"; value: WasmValue[] };

/**
 * Unwrap a WasmValue to its JavaScript equivalent.
 */
export function unwrapValue(v: WasmValue): unknown {
  switch (v.type) {
    case "Text":
    case "Uuid":
      return v.value;
    case "Boolean":
      return v.value;
    case "Integer":
    case "BigInt":
    case "Timestamp":
      return v.value;
    case "Null":
      return undefined;
    case "Array":
      return v.value.map(unwrapValue);
    case "Row":
      return v.value.map(unwrapValue);
  }
}

/**
 * Transform WasmRow[] to typed objects using schema column order.
 *
 * @param rows Array of WasmRow results from query
 * @param schema WasmSchema containing table definitions
 * @param tableName Name of the table being queried
 * @returns Array of typed objects with named properties
 */
export function transformRows<T>(rows: WasmRow[], schema: WasmSchema, tableName: string): T[] {
  const table = schema.tables[tableName];
  if (!table) {
    throw new Error(`Unknown table "${tableName}" in schema`);
  }

  return rows.map((row) => {
    const obj: Record<string, unknown> = { id: row.id };

    for (let i = 0; i < table.columns.length; i++) {
      const col = table.columns[i];
      const value = row.values[i];
      if (value !== undefined) {
        obj[col.name] = unwrapValue(value as WasmValue);
      }
    }

    // Handle nested arrays from array_subqueries (includes)
    // These come after the regular column values
    if (row.values.length > table.columns.length) {
      // Nested arrays are appended after column values
      // We need relation metadata to name them properly
      // For now, preserve any extra values as-is
      // TODO: Map nested arrays to relation names once we have that metadata
    }

    return obj as T;
  });
}
