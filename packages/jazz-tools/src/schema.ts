// Schema type definitions

export type SqlType = "TEXT" | "BOOLEAN" | "INTEGER" | "REAL" | "UUID";

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
      type: "Inherits";
      operation: PolicyOperation;
      via_column: string;
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
export type LensOpType = "introduce" | "drop" | "rename";

export interface LensOp {
  type: LensOpType;
  column: string;
  value: unknown;
}

export interface Lens {
  table: string;
  operations: LensOp[];
}
