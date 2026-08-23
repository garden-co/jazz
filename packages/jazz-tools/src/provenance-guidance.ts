import type { Schema } from "./schema.js";

const CONVENTIONAL_PROVENANCE = new Map([
  ["createdAt", "$createdAt"],
  ["createdBy", "$createdBy"],
  ["updatedAt", "$updatedAt"],
  ["updatedBy", "$updatedBy"],
] as const);

export function collectConventionalProvenanceDiagnostics(schema: Schema): string[] {
  const diagnostics: string[] = [];
  for (const table of schema.tables) {
    for (const column of table.columns) {
      const magic = CONVENTIONAL_PROVENANCE.get(
        column.name as "createdAt" | "createdBy" | "updatedAt" | "updatedBy",
      );
      if (!magic || column.allowExternalProvenanceName) continue;
      diagnostics.push(
        `Warning: table "${table.name}" declares "${column.name}", which commonly duplicates Jazz's built-in ${magic} provenance column. Remove it from the schema and writes; use ${magic} in queries, ordering, and permissions. If this is truly imported external-domain provenance, wrap the column in s.allowExternalProvenanceName(...).`,
      );
    }
  }
  return diagnostics;
}
