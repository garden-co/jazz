import type { WasmSchema } from "../drivers/types.js";

export const HIDDEN_INCLUDE_COLUMN_PREFIX = "__jazz_include_";

export function resolveIncludeColumnNames(
  tableName: string,
  relationNames: readonly string[],
  schema: WasmSchema,
): ReadonlyMap<string, string> {
  const columnNames = new Set(schema[tableName]?.columns.map((column) => column.name) ?? []);
  const occupiedNames = new Set([...columnNames, ...relationNames]);
  const outputNames = new Map<string, string>();

  for (const relationName of relationNames) {
    if (!columnNames.has(relationName)) {
      outputNames.set(relationName, relationName);
      continue;
    }

    let outputName = `${HIDDEN_INCLUDE_COLUMN_PREFIX}${relationName}`;
    while (occupiedNames.has(outputName)) {
      outputName = `${HIDDEN_INCLUDE_COLUMN_PREFIX}${outputName}`;
    }
    occupiedNames.add(outputName);
    outputNames.set(relationName, outputName);
  }

  return outputNames;
}

export function resolveSelectedColumns(
  tableName: string,
  schema: WasmSchema,
  projection: readonly string[] | undefined,
): string[] {
  const table = schema[tableName];
  if (!table) {
    throw new Error(`Unknown table "${tableName}" in schema`);
  }

  if (!projection || projection.length === 0) {
    return table.columns.map((column) => column.name);
  }

  const schemaColumnNames = new Set(table.columns.map((column) => column.name));
  const selection = {
    explicitColumnsInSchema: new Set<string>(),
    explicitColumnsNotInSchema: new Set<string>(),
    hasWildcard: false,
  };

  for (const column of projection) {
    if (column === "*") {
      selection.hasWildcard = true;
      continue;
    }
    if (column === "id") {
      continue;
    }
    if (schemaColumnNames.has(column)) {
      selection.explicitColumnsInSchema.add(column);
    } else {
      selection.explicitColumnsNotInSchema.add(column);
    }
  }

  // if no wildcard, return all explicitly selected columns
  if (!selection.hasWildcard) {
    return [...selection.explicitColumnsInSchema, ...selection.explicitColumnsNotInSchema];
  }

  if (selection.explicitColumnsNotInSchema.size === 0) {
    return [...schemaColumnNames];
  }

  // If wildcard is present, return all schema columns plus explicit non-schema columns like
  // permission introspection fields.
  return [...schemaColumnNames, ...selection.explicitColumnsNotInSchema];
}
