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

/**
 * Capitalize the first letter of a string.
 */
function capitalize(s: string): string {
  return s.charAt(0).toUpperCase() + s.slice(1);
}

/**
 * Analyze a WasmSchema and derive all forward and reverse relations.
 *
 * Forward relations: Created from FK columns, stripping _id suffix.
 *   e.g., parent_id -> parent
 *
 * Reverse relations: Created on the target table of each FK.
 *   e.g., todos.owner_id -> users gets a todosViaOwner reverse relation
 *
 * @param schema The WasmSchema to analyze
 * @returns Map from table name to array of relations on that table
 */
export function analyzeRelations(schema: WasmSchema): Map<string, Relation[]> {
  const relations = new Map<string, Relation[]>();

  // Initialize empty arrays for all tables
  for (const tableName of Object.keys(schema)) {
    relations.set(tableName, []);
  }

  for (const [tableName, table] of Object.entries(schema)) {
    for (const col of table.columns) {
      if (col.references) {
        const isUuidRef =
          col.column_type.type === "Uuid" ||
          (col.column_type.type === "Array" && col.column_type.element.type === "Uuid");
        if (!isUuidRef) {
          throw new Error(
            `Column "${tableName}.${col.name}" uses references but is not UUID or UUID[]`,
          );
        }
        const isForwardArray =
          col.column_type.type === "Array" && col.column_type.element.type === "Uuid";

        // Forward relation: parent_id -> parent
        const forwardName = col.name.replace(/_id$/, "");
        const forwardRelation: Relation = {
          name: forwardName,
          type: "forward",
          fromTable: tableName,
          toTable: col.references,
          fromColumn: col.name,
          toColumn: "id",
          isArray: isForwardArray,
          nullable: col.nullable,
        };
        relations.get(tableName)!.push(forwardRelation);

        // Verify the referenced table exists
        if (!relations.has(col.references)) {
          throw new Error(
            `Table "${tableName}" references unknown table "${col.references}" via column "${col.name}"`,
          );
        }

        // Reverse relation on target table: todosViaParent
        const reverseName = `${tableName}Via${capitalize(forwardName)}`;
        const reverseRelation: Relation = {
          name: reverseName,
          type: "reverse",
          fromTable: col.references,
          toTable: tableName,
          fromColumn: "id",
          toColumn: col.name,
          isArray: true,
          nullable: false, // Arrays are not nullable, just empty
        };
        relations.get(col.references)!.push(reverseRelation);
      }
    }
  }

  return relations;
}
