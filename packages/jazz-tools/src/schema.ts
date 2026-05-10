// Schema type definitions
import type { RelExpr } from "./ir.js";
import type { FromSchema, JSONSchema } from "json-schema-to-ts";

export type ScalarSqlType =
  | "TEXT"
  | "BOOLEAN"
  | "INTEGER"
  | "REAL"
  | "TIMESTAMP"
  | "UUID"
  | "BYTEA";

export type JsonPrimitive = string | number | boolean | null;
export type JsonValue = JsonPrimitive | { [key: string]: JsonValue } | JsonValue[];
export type JsonSchema = Exclude<JSONSchema, boolean>;
export type JsonSchemaToTs<Schema extends JsonSchema> = FromSchema<Schema>;

export interface EnumSqlType {
  kind: "ENUM";
  variants: string[];
}
export interface ArraySqlType {
  kind: "ARRAY";
  element: SqlType;
}
export interface JsonSqlType<Output = JsonValue> {
  kind: "JSON";
  schema?: JsonSchema;
  /**
   * Phantom field for compile-time output inference.
   * This property is never populated at runtime.
   */
  __output?: Output;
}
export type SqlType = ScalarSqlType | ArraySqlType | EnumSqlType | JsonSqlType<unknown>;
export type ColumnMergeStrategy = "counter";
export type ColumnMergeStrategyName = ColumnMergeStrategy | "lww";

export function sqlTypeToString(sqlType: SqlType): string {
  if (typeof sqlType === "string") {
    return sqlType;
  }
  if (sqlType.kind === "ENUM") {
    const variants = sqlType.variants.map((variant) => `'${variant.replace(/'/g, "''")}'`);
    return `ENUM(${variants.join(",")})`;
  }
  if (sqlType.kind === "JSON") {
    if (!sqlType.schema) {
      return "JSON";
    }
    return `JSON('${JSON.stringify(sqlType.schema).replace(/'/g, "''")}')`;
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
          ? Date | number
          : T extends "UUID"
            ? string
            : T extends "BYTEA"
              ? Uint8Array
              : never;

export type TSTypeFromSqlType<T extends SqlType> = T extends ScalarSqlType
  ? TSTypeFromScalarSqlType<T>
  : T extends ArraySqlType
    ? TSTypeFromSqlType<T["element"]>[]
    : T extends EnumSqlType
      ? T["variants"][number]
      : T extends JsonSqlType<infer Output>
        ? Output
        : never;

export interface Column {
  name: string;
  sqlType: SqlType;
  nullable: boolean;
  default?: unknown;
  references?: string; // Target table name for foreign key
  mergeStrategy?: ColumnMergeStrategy;
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

export type PolicyLiteralValue = Extract<PolicyValue, { type: "Literal" }>;

export type PolicyExpr =
  | {
      type: "Cmp";
      column: string;
      op: PolicyCmpOp;
      value: PolicyValue;
    }
  | {
      type: "SessionCmp";
      path: string[];
      op: PolicyCmpOp;
      value: PolicyLiteralValue;
    }
  | {
      type: "IsNull";
      column: string;
    }
  | {
      type: "SessionIsNull";
      path: string[];
    }
  | {
      type: "IsNotNull";
      column: string;
    }
  | {
      type: "SessionIsNotNull";
      path: string[];
    }
  | {
      type: "Contains";
      column: string;
      value: PolicyValue;
    }
  | {
      type: "SessionContains";
      path: string[];
      value: PolicyLiteralValue;
    }
  | {
      type: "In";
      column: string;
      session_path: string[];
    }
  | {
      type: "InList";
      column: string;
      values: PolicyValue[];
    }
  | {
      type: "SessionInList";
      path: string[];
      values: PolicyLiteralValue[];
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
      type: "InheritsReferencing";
      operation: PolicyOperation;
      source_table: string;
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
  indexedColumns?: string[];
  policies?: TablePolicies;
}

export interface Schema {
  tables: Table[];
}

// Migration operation types
export interface AddOp<TSqlType extends SqlType = SqlType, TDefault = unknown> {
  _type: "add";
  sqlType: TSqlType;
  default: TDefault;
}

export interface DropOp<TSqlType extends SqlType = SqlType, TBackwardsDefault = unknown> {
  _type: "drop";
  sqlType: TSqlType;
  backwardsDefault: TBackwardsDefault;
}

export interface RenameOp<TOldName extends string = string> {
  _type: "rename";
  oldName: TOldName;
}

export interface RenameTableFromOp<TOldName extends string = string> {
  _type: "renameTable";
  oldName: TOldName;
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

export interface TableLens {
  table: string;
  added?: boolean;
  removed?: boolean;
  renamedFrom?: string;
  operations: LensOp[];
}

export type Lens = TableLens;
