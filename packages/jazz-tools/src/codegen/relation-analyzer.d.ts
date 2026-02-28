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
export declare function analyzeRelations(schema: WasmSchema): Map<string, Relation[]>;
//# sourceMappingURL=relation-analyzer.d.ts.map
