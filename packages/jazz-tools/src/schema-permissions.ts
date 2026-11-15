/**
 * An app's permissions in the engine's form: what Jazz's Rust loader makes
 * of `jazz.permissions.json` (jazz-napi `loadBackendApp`), per table. The
 * TS runtime never reads the readable file itself; it merges these into a
 * schema for a backend's in-process sessions.
 */
import type { TablePolicies as WasmTablePolicies, WasmSchema } from "./drivers/types.js";

/** The engine's policies of each table, as the Rust loader returns them. */
export type CompiledPermissionsMap = Record<string, WasmTablePolicies>;

function validatePermissionTables(
  schemaTableNames: readonly string[],
  permissions: CompiledPermissionsMap,
): void {
  const knownTables = new Set(schemaTableNames);
  const unknownTables = Object.keys(permissions).filter((table) => !knownTables.has(table));
  if (unknownTables.length > 0) {
    throw new Error(
      `jazz.permissions.json defines permissions for unknown table(s): ${unknownTables.join(", ")}.`,
    );
  }
}

/** Internal native representation. The explicit bundle replaces all embedded policies. */
export function mergePermissionsIntoWasmSchema(
  schema: WasmSchema,
  permissions: CompiledPermissionsMap,
): WasmSchema {
  validatePermissionTables(Object.keys(schema), permissions);
  const merged: WasmSchema = {};
  for (const [tableName, table] of Object.entries(schema)) {
    merged[tableName] = { ...table, policies: permissions[tableName] };
  }
  return merged;
}
