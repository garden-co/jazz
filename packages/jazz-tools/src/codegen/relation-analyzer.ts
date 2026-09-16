import { assertRelationshipDeclaration } from "../relationships.js";
/**
 * Analyze schema to derive forward and reverse relations.
 */

import type { WasmSchema } from "../drivers/types.js";

/**
 * A relation between two tables (forward or reverse).
 */
export interface Relation {
  /** Relation name, e.g., "parent" or "todosViaOwner" */
  name: string;
  /** Whether this is a forward (FK holder) or reverse relation */
  type: "forward" | "reverse";
  /** Table that holds this relation */
  fromTable: string;
  /** Table being referenced */
  toTable: string;
  /** Column name on the "from" side */
  fromColumn: string;
  /** Column name on the "to" side (always "id" for reverse) */
  toColumn: string;
  /** True for reverse relations (always arrays) */
  isArray: boolean;
  /** Whether the FK column is nullable */
  nullable: boolean;
}

export class AmbiguousRelationNameError extends Error {}
export class DuplicateColumnNameError extends Error {}

/** Resolve only declared relationships. Reference columns never imply aliases or inverses. */
export function analyzeRelations(schema: WasmSchema): Map<string, Relation[]> {
  const result = new Map<string, Relation[]>();
  for (const [tableName, table] of Object.entries(schema)) {
    const columns = new Set<string>();
    for (const column of table.columns) {
      if (columns.has(column.name))
        throw new DuplicateColumnNameError(
          `Table "${tableName}" has duplicate column descriptor "${column.name}".`,
        );
      columns.add(column.name);
    }
    result.set(tableName, []);
    for (const name of Object.keys(table.relations ?? {})) {
      if (
        !name ||
        ["__proto__", "constructor", "prototype"].includes(name) ||
        name.startsWith("$") ||
        name === "id" ||
        columns.has(name)
      )
        throw new AmbiguousRelationNameError(
          `Relationship "${tableName}.${name}" collides with a stored/public output column.`,
        );
    }
  }
  for (const [tableName, table] of Object.entries(schema)) {
    for (const [name, declaration] of Object.entries(table.relations ?? {})) {
      assertRelationshipDeclaration(declaration, `${tableName}.${name}`);
      if (!Object.hasOwn(schema, declaration.table))
        throw new Error(
          `Relationship "${tableName}.${name}" references unknown table "${declaration.table}".`,
        );
      const forward =
        declaration.kind === "forward"
          ? declaration
          : schema[declaration.table]!.relations?.[declaration.relation];
      if (!forward || forward.kind !== "forward")
        throw new Error(
          `Reverse relationship "${tableName}.${name}" must reference a named forward relation on "${declaration.table}".`,
        );
      if (declaration.kind === "reverse" && forward.table !== tableName)
        throw new Error(
          `Reverse relationship "${tableName}.${name}" references a forward relation targeting "${forward.table}", not "${tableName}".`,
        );
      const source = declaration.kind === "forward" ? table : schema[declaration.table]!;
      const column = source.columns.find((c) => c.name === forward.column);
      if (!column)
        throw new Error(
          `Relationship "${tableName}.${name}" references unknown column "${forward.column}".`,
        );
      const array =
        column.column_type.type === "Array" && column.column_type.element.type === "Uuid";
      if (column.column_type.type !== "Uuid" && !array)
        throw new Error(
          `Relationship "${tableName}.${name}" requires UUID or UUID[] column "${forward.column}".`,
        );
      if (column.references && column.references !== forward.table)
        throw new Error(
          `Relationship "${tableName}.${name}" conflicts with column reference target "${column.references}".`,
        );
      result.get(tableName)!.push({
        name,
        type: declaration.kind,
        fromTable: tableName,
        toTable: declaration.table,
        fromColumn: declaration.kind === "forward" ? forward.column : "id",
        toColumn: declaration.kind === "forward" ? "id" : forward.column,
        isArray: declaration.kind === "reverse" || array,
        nullable: declaration.kind === "forward" && column.nullable,
      });
    }
  }
  return result;
}
