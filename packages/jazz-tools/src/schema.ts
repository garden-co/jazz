// Schema type definitions

export type ScalarSqlType = "TEXT" | "BOOLEAN" | "INTEGER" | "REAL" | "UUID";
export interface ArraySqlType {
  kind: "ARRAY";
  element: SqlType;
}
export type SqlType = ScalarSqlType | ArraySqlType;

export function sqlTypeToString(sqlType: SqlType): string {
  if (typeof sqlType === "string") {
    return sqlType;
  }
  return `${sqlTypeToString(sqlType.element)}[]`;
}

export interface Column {
  name: string;
  sqlType: SqlType;
  nullable: boolean;
  references?: string; // Target table name for foreign key
}

export interface Table {
  name: string;
  columns: Column[];
}

export interface Schema {
  tables: Table[];
}

// Migration operation types
export interface AddOp {
  _type: "add";
  sqlType: SqlType;
  default: unknown;
}

export interface DropOp {
  _type: "drop";
  sqlType: SqlType;
  backwardsDefault: unknown;
}

export interface RenameOp {
  _type: "rename";
  oldName: string;
}

export type MigrationOp = AddOp | DropOp | RenameOp;

// Internal representation for a single-table migration
export interface TableMigration {
  table: string;
  operations: MigrationOpEntry[];
}

export interface MigrationOpEntry {
  column: string;
  op: MigrationOp;
}

// Lens format for SQL generation
export interface IntroduceLensOp {
  type: "introduce";
  column: string;
  sqlType: SqlType;
  value: unknown;
}

export interface DropLensOp {
  type: "drop";
  column: string;
  sqlType: SqlType;
  value: unknown;
}

export interface RenameLensOp {
  type: "rename";
  column: string;
  value: string;
}

export type LensOp = IntroduceLensOp | DropLensOp | RenameLensOp;

export type LensOpType = LensOp["type"];

export interface Lens {
  table: string;
  operations: LensOp[];
}
