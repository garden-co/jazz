// Schema type definitions
import type { RelExpr } from "./ir.js";

export type ScalarSqlType = "TEXT" | "BOOLEAN" | "INTEGER" | "REAL" | "TIMESTAMP" | "UUID";
export interface EnumSqlType {
  kind: "ENUM";
  variants: string[];
}
export interface ArraySqlType {
  kind: "ARRAY";
  element: SqlType;
}
export type SqlType = ScalarSqlType | ArraySqlType | EnumSqlType;

export function sqlTypeToString(sqlType: SqlType): string {
  if (typeof sqlType === "string") {
    return sqlType;
  }
  if (sqlType.kind === "ENUM") {
    const variants = sqlType.variants.map((variant) => `'${variant.replace(/'/g, "''")}'`);
    return `ENUM(${variants.join(",")})`;
  }
  return `${sqlTypeToString(sqlType.element)}[]`;
}

type TSTypeFromScalarSqlType<T extends ScalarSqlType> = T extends "TEXT"
  ? string
  : T extends "BOOLEAN"
    ? boolean
    : T extends "INTEGER"
      ? number
      : T extends "REAL"
        ? number
        : T extends "TIMESTAMP"
          ? number
        : T extends "UUID"
          ? string
          : never;

export type TSTypeFromSqlType<T extends SqlType> = T extends ScalarSqlType
  ? TSTypeFromScalarSqlType<T>
  : T extends ArraySqlType
    ? TSTypeFromSqlType<T["element"]>[]
    : T extends EnumSqlType
      ? T["variants"][number]
      : never;

export interface Column {
  name: string;
  sqlType: SqlType;
  nullable: boolean;
  references?: string; // Target table name for foreign key
}

export type PolicyOperation = "Select" | "Insert" | "Update" | "Delete";
export type PolicyCmpOp = "Eq" | "Ne" | "Lt" | "Le" | "Gt" | "Ge";

export type PolicyValue =
  | {
      type: "Literal";
      value: unknown;
    }
  | {
      type: "SessionRef";
      path: string[];
    };

export type PolicyExpr =
  | {
      type: "Cmp";
      column: string;
      op: PolicyCmpOp;
      value: PolicyValue;
    }
  | {
      type: "IsNull";
      column: string;
    }
  | {
      type: "IsNotNull";
      column: string;
    }
  | {
      type: "In";
      column: string;
      session_path: string[];
    }
  | {
      type: "Exists";
      table: string;
      condition: PolicyExpr;
    }
  | {
      type: "ExistsRel";
      rel: RelExpr;
    }
  | {
      type: "Inherits";
      operation: PolicyOperation;
      via_column: string;
      max_depth?: number;
    }
  | {
      type: "And";
      exprs: PolicyExpr[];
    }
  | {
      type: "Or";
      exprs: PolicyExpr[];
    }
  | {
      type: "Not";
      expr: PolicyExpr;
    }
  | {
      type: "True";
    }
  | {
      type: "False";
    };

export interface OperationPolicy {
  using?: PolicyExpr;
  with_check?: PolicyExpr;
}

export interface TablePolicies {
  select?: OperationPolicy;
  insert?: OperationPolicy;
  update?: OperationPolicy;
  delete?: OperationPolicy;
}

export interface Table {
  name: string;
  columns: Column[];
  policies?: TablePolicies;
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
