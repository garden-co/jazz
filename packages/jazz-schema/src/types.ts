import type { ZodType, ZodOptional } from "zod";

/**
 * Symbol to identify table descriptors
 */
export const TABLE_SYMBOL = Symbol.for("jazz:table");

/**
 * SQL column types matching Groove's ColumnType enum
 */
export type SqlColumnType =
  | { kind: "bool" }
  | { kind: "i64" }
  | { kind: "f64" }
  | { kind: "string" }
  | { kind: "bytes" }
  | { kind: "ref"; table: string };

/**
 * A column definition after analysis
 */
export interface ColumnDef {
  name: string;
  sqlType: SqlColumnType;
  nullable: boolean;
  /** If this is a reference, the table descriptor it points to */
  refTable?: TableDescriptor;
}

/**
 * A reverse reference (one-to-many relationship).
 *
 * When table B has a column that references table A, table A gets a
 * reverse reference to B. This enables ORM-style eager loading:
 *
 *   Folder.notes -> ARRAY(SELECT n FROM notes n WHERE n.folder = folder.id)
 */
export interface ReverseRef {
  /** Name of the reverse collection (pluralized table name by default) */
  name: string;
  /** The table that has the forward reference to this table */
  sourceTable: string;
  /** The column in the source table that references this table */
  sourceColumn: string;
  /** Whether the forward reference is nullable */
  nullable: boolean;
}

/**
 * A table descriptor created by table()
 */
export interface TableDescriptor {
  [TABLE_SYMBOL]: true;
  /** Table name (derived from schema export name) */
  name: string;
  /** Raw column definitions from user (getters are used for self-references) */
  columns: Record<string, ZodType | TableDescriptor | ZodOptional<ZodType>>;
}

/**
 * Options for generateSchema()
 */
export interface GenerateOptions {
  /** Output directory for generated files (default: same as schema file) */
  output?: string;
}

/**
 * Check if a value is a TableDescriptor
 */
export function isTableDescriptor(value: unknown): value is TableDescriptor {
  return (
    typeof value === "object" &&
    value !== null &&
    TABLE_SYMBOL in value &&
    (value as TableDescriptor)[TABLE_SYMBOL] === true
  );
}
