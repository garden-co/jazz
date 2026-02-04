/**
 * Generate TypeScript interfaces from WasmSchema.
 */

import type { WasmSchema, ColumnType } from "../drivers/types.js";

/**
 * Convert a WasmColumnType to TypeScript type string.
 */
function wasmTypeToTs(colType: ColumnType): string {
  switch (colType.type) {
    case "Text":
      return "string";
    case "Boolean":
      return "boolean";
    case "Integer":
    case "BigInt":
    case "Timestamp":
      return "number";
    case "Uuid":
      return "string";
    case "Array":
      return `${wasmTypeToTs(colType.element)}[]`;
    case "Row":
      // Nested row - generate inline type
      const fields = colType.columns
        .map((c) => {
          const opt = c.nullable ? "?" : "";
          return `${c.name}${opt}: ${wasmTypeToTs(c.column_type)}`;
        })
        .join("; ");
      return `{ ${fields} }`;
    default:
      return "unknown";
  }
}

/**
 * Singularize a word using simple heuristics.
 *
 * Examples:
 *   todos -> todo
 *   categories -> category
 *   users -> user
 *   data -> data (unchanged)
 */
function singularize(word: string): string {
  if (word.endsWith("ies")) {
    // categories -> category
    return word.slice(0, -3) + "y";
  }
  if (word.endsWith("es") && word.length > 3) {
    // Some words ending in 'es' - be conservative
    const stem = word.slice(0, -2);
    // Only apply if it ends with common patterns like 'sses', 'xes', 'ches', 'shes'
    if (
      word.endsWith("sses") ||
      word.endsWith("xes") ||
      word.endsWith("ches") ||
      word.endsWith("shes")
    ) {
      return stem;
    }
    // Otherwise just remove 's'
    return word.slice(0, -1);
  }
  if (word.endsWith("s") && !word.endsWith("ss")) {
    // todos -> todo, users -> user
    return word.slice(0, -1);
  }
  return word;
}

/**
 * Convert a table name to a TypeScript interface name.
 *
 * Examples:
 *   todos -> Todo
 *   user_profiles -> UserProfile
 *   categories -> Category
 */
function tableNameToInterface(name: string): string {
  // Convert snake_case to words, singularize the last word, then PascalCase
  const parts = name.split("_");
  // Singularize only the last part (table names are typically plural)
  parts[parts.length - 1] = singularize(parts[parts.length - 1]);

  return parts.map((word) => word.charAt(0).toUpperCase() + word.slice(1)).join("");
}

/**
 * Generate TypeScript code from a WasmSchema.
 *
 * Produces:
 * 1. Base interfaces with id field (e.g., Todo)
 * 2. Init interfaces without id (e.g., TodoInit)
 * 3. Exported wasmSchema constant
 */
export function generateTypes(schema: WasmSchema): string {
  const lines: string[] = [
    "// AUTO-GENERATED FILE - DO NOT EDIT",
    'import type { WasmSchema } from "jazz-ts";',
    "",
  ];

  // Base types (with id)
  for (const [tableName, table] of Object.entries(schema.tables)) {
    const interfaceName = tableNameToInterface(tableName);
    lines.push(`export interface ${interfaceName} {`);
    lines.push("  id: string;");
    for (const col of table.columns) {
      const opt = col.nullable ? "?" : "";
      lines.push(`  ${col.name}${opt}: ${wasmTypeToTs(col.column_type)};`);
    }
    lines.push("}");
    lines.push("");
  }

  // Init types (without id, for inserts)
  for (const [tableName, table] of Object.entries(schema.tables)) {
    const interfaceName = tableNameToInterface(tableName) + "Init";
    lines.push(`export interface ${interfaceName} {`);
    for (const col of table.columns) {
      const opt = col.nullable ? "?" : "";
      lines.push(`  ${col.name}${opt}: ${wasmTypeToTs(col.column_type)};`);
    }
    lines.push("}");
    lines.push("");
  }

  // Export WasmSchema JSON
  lines.push(`export const wasmSchema: WasmSchema = ${JSON.stringify(schema, null, 2)};`);
  lines.push("");

  return lines.join("\n");
}
