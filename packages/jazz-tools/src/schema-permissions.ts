import type { TablePolicies as WasmTablePolicies, WasmSchema } from "./drivers/types.js";
import type { Schema, TablePolicies } from "./schema.js";

export type CompiledPermissionsMap = Record<string, TablePolicies>;

function validatePermissionTables(
  schemaTableNames: readonly string[],
  compiledPermissions: CompiledPermissionsMap,
): void {
  const knownTables = new Set(schemaTableNames);
  const unknownTables = Object.keys(compiledPermissions).filter(
    (tableName) => !knownTables.has(tableName),
  );

  if (unknownTables.length > 0) {
    throw new Error(
      `permissions.ts defines permissions for unknown table(s): ${unknownTables.join(", ")}.`,
    );
  }
}

export function mergePermissionsIntoSchema(
  schema: Schema,
  compiledPermissions: CompiledPermissionsMap,
): Schema {
  validatePermissionTables(
    schema.tables.map((table) => table.name),
    compiledPermissions,
  );

  return {
    tables: schema.tables.map((table) => {
      const external = compiledPermissions[table.name];
      if (!external) {
        return table;
      }

      return {
        ...table,
        policies: external,
      };
    }),
  };
}

export function mergePermissionsIntoWasmSchema(
  schema: WasmSchema,
  compiledPermissions: CompiledPermissionsMap,
): WasmSchema {
  validatePermissionTables(Object.keys(schema), compiledPermissions);

  const merged: WasmSchema = {};
  for (const [tableName, table] of Object.entries(schema)) {
    merged[tableName] = {
      ...table,
      policies:
        (compiledPermissions[tableName] as unknown as WasmTablePolicies | undefined) ??
        table.policies,
    };
  }
  return merged;
}
