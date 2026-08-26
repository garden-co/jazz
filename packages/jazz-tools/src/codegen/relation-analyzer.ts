/**
 * Analyze schema to derive forward and reverse relations.
 */

import type { WasmSchema } from "../drivers/types.js";
import pluralize from "pluralize-esm";

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

function columnDescriptorProvenance(
  index: number,
  column: WasmSchema[string]["columns"][number],
): string {
  const reference = column.references ? ` referencing "${column.references}"` : "";
  return `descriptor #${index + 1} (${column.column_type.type}${reference})`;
}

function validateUniqueColumnDescriptors(schema: WasmSchema): void {
  for (const [tableName, table] of Object.entries(schema)) {
    const seen = new Map<string, number>();
    for (const [index, column] of table.columns.entries()) {
      const previousIndex = seen.get(column.name);
      if (previousIndex !== undefined) {
        const previous = table.columns[previousIndex]!;
        throw new DuplicateColumnNameError(
          `Table "${tableName}" has duplicate column descriptor "${column.name}": ${columnDescriptorProvenance(previousIndex, previous)} conflicts with ${columnDescriptorProvenance(index, column)}. Column names must be unique before relation names are derived.`,
        );
      }
      seen.set(column.name, index);
    }
  }
}

function relationProvenance(relation: Relation): string {
  const referenceColumn = relation.type === "forward" ? relation.fromColumn : relation.toColumn;
  const referenceTable = relation.type === "forward" ? relation.fromTable : relation.toTable;
  const referencedTable = relation.type === "forward" ? relation.toTable : relation.fromTable;

  return `${relation.type} relation generated from reference column "${referenceTable}.${referenceColumn}" to "${referencedTable}.id"`;
}

function addRelation(
  relations: Map<string, Relation[]>,
  outputColumnsByTable: Map<string, Set<string>>,
  relation: Relation,
): void {
  const tableRelations = relations.get(relation.fromTable);
  if (!tableRelations) {
    throw new Error(`Unknown relation source table "${relation.fromTable}"`);
  }

  // A scalar reference may intentionally use its own relation name (for
  // example `team: ref("teams")`). The typed include API replaces that one
  // reference value with the joined row. Every other output-column collision
  // would instead make two independently-addressable public values share a
  // key, so reject it.
  const isOwnReferenceColumn = relation.type === "forward" && relation.fromColumn === relation.name;
  if (!isOwnReferenceColumn && outputColumnsByTable.get(relation.fromTable)?.has(relation.name)) {
    throw new AmbiguousRelationNameError(
      `Generated relation name "${relation.name}" on table "${relation.fromTable}" (${relationProvenance(relation)}) collides with the stored/public output column "${relation.fromTable}.${relation.name}". Rename the reference column or the output column.`,
    );
  }

  const existing = tableRelations.find((candidate) => candidate.name === relation.name);
  if (existing) {
    throw new AmbiguousRelationNameError(
      `Generated relation name "${relation.name}" is ambiguous on table "${relation.fromTable}" between ${relationProvenance(existing)} and ${relationProvenance(relation)}. Rename one of the reference columns.`,
    );
  }
  tableRelations.push(relation);
}

/**
 * Capitalize the first letter of a string.
 */
function capitalize(s: string): string {
  return s.charAt(0).toUpperCase() + s.slice(1);
}

function forwardRefNameFromFK(columnName: string): string {
  const withoutIdSuffix = columnName.replace(/(?:_ids|Ids|_id|Id)$/, "");
  const requiresPluralization = columnName.endsWith("s");
  return requiresPluralization ? pluralize.plural(withoutIdSuffix) : withoutIdSuffix;
}

/**
 * Analyze a WasmSchema and derive all forward and reverse relations.
 *
 * Forward relations: Created from FK columns, stripping Id/_id/Ids/_ids suffixes.
 *   e.g., parent_id -> parent, assignees_ids -> assignees
 *
 * Reverse relations: Created on the target table of each FK.
 *   e.g., todos.owner_id -> users gets a todosViaOwner reverse relation
 *
 * @param schema The WasmSchema to analyze
 * @returns Map from table name to array of relations on that table
 */
export function analyzeRelations(schema: WasmSchema): Map<string, Relation[]> {
  validateUniqueColumnDescriptors(schema);

  const relations = new Map<string, Relation[]>();
  const outputColumnsByTable = new Map<string, Set<string>>();

  // Initialize empty arrays for all tables
  for (const [tableName, table] of Object.entries(schema)) {
    relations.set(tableName, []);
    // `id` is implicit in every public row even though it is not a stored
    // descriptor. Includes are materialized onto the same public row object,
    // so relation names must not shadow either it or a stored column.
    outputColumnsByTable.set(tableName, new Set(["id", ...table.columns.map((col) => col.name)]));
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

        const forwardName = forwardRefNameFromFK(col.name);
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
        addRelation(relations, outputColumnsByTable, forwardRelation);

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
        addRelation(relations, outputColumnsByTable, reverseRelation);
      }
    }
  }

  return relations;
}
