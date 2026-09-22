// Schema type definitions
import type { RelExpr } from "./ir.js";
import type { FromSchema, JSONSchema } from "json-schema-to-ts";

export type ScalarSqlType =
  | "TEXT"
  | "BOOLEAN"
  | "INTEGER"
  | "BIGINT"
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
  /** The established zero-payload scalar enum form. */
  variants?: readonly string[];
  /** Payload-bearing cases. This form is deliberately distinct from variants. */
  cases?: readonly EnumCaseSqlType[];
}
export interface EnumCaseSqlType {
  name: string;
  fields: Column[];
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
export type ColumnMergeStrategy = "counter" | "g-set";
export type ColumnMergeStrategyName = ColumnMergeStrategy | "lww";

export function sqlTypeToString(sqlType: SqlType): string {
  if (typeof sqlType === "string") {
    return sqlType;
  }
  if (sqlType.kind === "ENUM") {
    if (sqlType.variants) {
      const variants = sqlType.variants.map((variant) => `'${variant.replace(/'/g, "''")}'`);
      return `ENUM(${variants.join(",")})`;
    }
    return `ENUM(${(sqlType.cases ?? []).map((entry) => entry.name).join(",")})`;
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
      : T extends "BIGINT"
        ? bigint
        : T extends "REAL"
          ? number
          : T extends "TIMESTAMP"
            ? Date
            : T extends "UUID"
              ? string
              : T extends "BYTEA"
                ? Uint8Array
                : never;

export type TSTypeFromSqlType<T extends SqlType> = SqlTypeValue<T, false>;

/** Write inputs may omit nullable/defaulted payload fields; reads are materialized. */
export type TSInitFromSqlType<T extends SqlType> = SqlTypeValue<T, true>;

type SqlTypeValue<T extends SqlType, Init extends boolean> = T extends ScalarSqlType
  ? TSTypeFromScalarSqlType<T>
  : T extends ArraySqlType
    ? SqlTypeValue<T["element"], Init>[]
    : T extends EnumSqlType
      ? T["variants"] extends readonly string[]
        ? T["variants"][number]
        : T["cases"] extends readonly EnumCaseSqlType[]
          ? EnumValueFromCases<T["cases"], Init>
          : never
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
  /** Local authoring metadata; never emitted into the structural schema. */
  allowExternalProvenanceName?: true;
}

type EnumCaseFieldValue<Field extends Column, Init extends boolean> =
  | SqlTypeValue<Field["sqlType"], Init>
  | (Field["nullable"] extends true ? null : never);

type EnumCaseFieldIsOptional<Field extends Column> = Field["nullable"] extends true
  ? true
  : Field extends { __jazzHasDefault: true }
    ? true
    : false;

type EnumCasePayload<Fields extends readonly Column[], Init extends boolean> = Init extends false
  ? { [Field in Fields[number] as Field["name"]]: EnumCaseFieldValue<Field, false> }
  : {
      [Field in Fields[number] as EnumCaseFieldIsOptional<Field> extends true
        ? never
        : Field["name"]]: EnumCaseFieldValue<Field, true>;
    } & {
      [Field in Fields[number] as EnumCaseFieldIsOptional<Field> extends true
        ? Field["name"]
        : never]?: EnumCaseFieldValue<Field, true>;
    };

export type EnumValueFromCases<
  Cases extends readonly EnumCaseSqlType[],
  Init extends boolean = false,
> = {
  [Case in Cases[number] as Case["name"]]: { type: Case["name"] } & EnumCasePayload<
    Case["fields"],
    Init
  >;
}[Cases[number]["name"]];

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
  /** Authoring metadata; excluded from storage schema identity. */
  relations?: import("./relationships.js").Relationships;
  name: string;
  columns: Column[];
  indexedColumns?: string[];
  branchBy?: string[];
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
