// DSL for defining schemas and migrations

import type {
  Column,
  Schema,
  Table,
  SqlType,
  PolicyExpr,
  PolicyOperation,
  Lens,
  LensOp,
  AddOp,
  DropOp,
  RenameOp,
  MigrationOp,
  TableMigration,
} from "./schema.js";

// ============================================================================
// Column Builder (for schema context)
// ============================================================================

class ColumnBuilder {
  private _nullable = false;

  constructor(private _sqlType: SqlType) {}

  optional(): this {
    this._nullable = true;
    return this;
  }

  _build(name: string): Column {
    return {
      name,
      sqlType: this._sqlType,
      nullable: this._nullable,
    };
  }
}

// ============================================================================
// Ref Builder (for foreign key references in schema context)
// ============================================================================

class RefBuilder {
  private _nullable = false;

  constructor(private _targetTable: string) {}

  optional(): this {
    this._nullable = true;
    return this;
  }

  _build(name: string): Column {
    return {
      name,
      sqlType: "UUID",
      nullable: this._nullable,
      references: this._targetTable,
    };
  }
}

// ============================================================================
// Add Builder (for migration context)
// ============================================================================

type MaybeOptional<T, Optional extends boolean> = Optional extends true ? T | null : T;
type SessionPathInput = string | string[];
type PolicyOperationInput = "select" | "insert" | "update" | "delete";

type SelectPolicyInput = PolicyExpr | { using: PolicyExpr };
type InsertPolicyInput = PolicyExpr | { withCheck: PolicyExpr };
type UpdatePolicyInput = PolicyExpr | { using?: PolicyExpr; withCheck?: PolicyExpr };
type DeletePolicyInput = PolicyExpr | { using: PolicyExpr };

interface TablePermissionsInput {
  select?: SelectPolicyInput;
  insert?: InsertPolicyInput;
  update?: UpdatePolicyInput;
  delete?: DeletePolicyInput;
}

interface TableOptions {
  permissions?: TablePermissionsInput;
}

class AddBuilder<Optional extends boolean = false> {
  string(opts: { default: MaybeOptional<string, Optional> }): AddOp {
    return { _type: "add", sqlType: "TEXT", default: opts.default };
  }

  int(opts: { default: MaybeOptional<number, Optional> }): AddOp {
    return { _type: "add", sqlType: "INTEGER", default: opts.default };
  }

  boolean(opts: { default: MaybeOptional<boolean, Optional> }): AddOp {
    return { _type: "add", sqlType: "BOOLEAN", default: opts.default };
  }

  float(opts: { default: MaybeOptional<number, Optional> }): AddOp {
    return { _type: "add", sqlType: "REAL", default: opts.default };
  }

  optional(): AddBuilder<true> {
    return this as AddBuilder<true>;
  }
}

// ============================================================================
// Drop Builder (for migration context)
// ============================================================================

class DropBuilder {
  string(opts: { backwardsDefault: string }): DropOp {
    return { _type: "drop", sqlType: "TEXT", backwardsDefault: opts.backwardsDefault };
  }

  int(opts: { backwardsDefault: number }): DropOp {
    return { _type: "drop", sqlType: "INTEGER", backwardsDefault: opts.backwardsDefault };
  }

  boolean(opts: { backwardsDefault: boolean }): DropOp {
    return { _type: "drop", sqlType: "BOOLEAN", backwardsDefault: opts.backwardsDefault };
  }

  float(opts: { backwardsDefault: number }): DropOp {
    return { _type: "drop", sqlType: "REAL", backwardsDefault: opts.backwardsDefault };
  }
}

// ============================================================================
// col namespace
// ============================================================================

export const col = {
  // Schema context
  string: () => new ColumnBuilder("TEXT"),
  boolean: () => new ColumnBuilder("BOOLEAN"),
  int: () => new ColumnBuilder("INTEGER"),
  float: () => new ColumnBuilder("REAL"),
  ref: (targetTable: string) => new RefBuilder(targetTable),

  // Migration context
  add: () => new AddBuilder(),
  drop: () => new DropBuilder(),
  rename: (oldName: string): RenameOp => ({ _type: "rename", oldName }),
};

const POLICY_OPERATION_MAP: Record<PolicyOperationInput, PolicyOperation> = {
  select: "Select",
  insert: "Insert",
  update: "Update",
  delete: "Delete",
};

function normalizeSessionPath(path: SessionPathInput): string[] {
  const parts = Array.isArray(path) ? path : path.split(".");
  return parts.map((part) => part.trim()).filter((part) => part.length > 0);
}

function isPolicyExpr(input: unknown): input is PolicyExpr {
  return typeof input === "object" && input !== null && "type" in input;
}

export const policy = {
  allow(): PolicyExpr {
    return { type: "True" };
  },

  deny(): PolicyExpr {
    return { type: "False" };
  },

  eq(column: string, value: unknown): PolicyExpr {
    return {
      type: "Cmp",
      column,
      op: "Eq",
      value: { type: "Literal", value },
    };
  },

  eqSession(column: string, path: SessionPathInput): PolicyExpr {
    return {
      type: "Cmp",
      column,
      op: "Eq",
      value: { type: "SessionRef", path: normalizeSessionPath(path) },
    };
  },

  inSession(column: string, path: SessionPathInput): PolicyExpr {
    return {
      type: "In",
      column,
      session_path: normalizeSessionPath(path),
    };
  },

  exists(table: string, condition: PolicyExpr): PolicyExpr {
    return {
      type: "Exists",
      table,
      condition,
    };
  },

  isNull(column: string): PolicyExpr {
    return { type: "IsNull", column };
  },

  isNotNull(column: string): PolicyExpr {
    return { type: "IsNotNull", column };
  },

  inherits(operation: PolicyOperationInput, viaColumn: string): PolicyExpr {
    return {
      type: "Inherits",
      operation: POLICY_OPERATION_MAP[operation],
      via_column: viaColumn,
    };
  },

  and(exprs: PolicyExpr[]): PolicyExpr {
    if (exprs.length === 0) {
      return { type: "True" };
    }
    if (exprs.length === 1) {
      return exprs[0];
    }
    return { type: "And", exprs };
  },

  or(exprs: PolicyExpr[]): PolicyExpr {
    if (exprs.length === 0) {
      return { type: "False" };
    }
    if (exprs.length === 1) {
      return exprs[0];
    }
    return { type: "Or", exprs };
  },

  not(expr: PolicyExpr): PolicyExpr {
    return { type: "Not", expr };
  },
};

function normalizePermissions(
  permissions: TablePermissionsInput | undefined,
): Table["policies"] | undefined {
  if (!permissions) {
    return undefined;
  }

  const policies: NonNullable<Table["policies"]> = {};

  if (permissions.select) {
    if (isPolicyExpr(permissions.select)) {
      policies.select = { using: permissions.select };
    } else {
      policies.select = { using: permissions.select.using };
    }
  }

  if (permissions.insert) {
    if (isPolicyExpr(permissions.insert)) {
      policies.insert = { with_check: permissions.insert };
    } else {
      policies.insert = { with_check: permissions.insert.withCheck };
    }
  }

  if (permissions.update) {
    if (isPolicyExpr(permissions.update)) {
      policies.update = { using: permissions.update, with_check: permissions.update };
    } else {
      policies.update = {
        using: permissions.update.using,
        with_check: permissions.update.withCheck,
      };
    }
  }

  if (permissions.delete) {
    if (isPolicyExpr(permissions.delete)) {
      policies.delete = { using: permissions.delete };
    } else {
      policies.delete = { using: permissions.delete.using };
    }
  }

  if (!policies.select && !policies.insert && !policies.update && !policies.delete) {
    return undefined;
  }

  return policies;
}

// ============================================================================
// Side-effect collection
// ============================================================================

let collectedTables: Table[] = [];
let collectedMigrations: TableMigration[] = [];

export function table(
  name: string,
  columns: Record<string, ColumnBuilder | RefBuilder>,
  options?: TableOptions,
): void {
  const cols: Column[] = [];
  for (const [colName, builder] of Object.entries(columns)) {
    cols.push(builder._build(colName));
  }
  collectedTables.push({
    name,
    columns: cols,
    policies: normalizePermissions(options?.permissions),
  });
}

export function migrate(tableName: string, ops: Record<string, MigrationOp>): void {
  const operations = Object.entries(ops).map(([column, op]) => ({ column, op }));
  collectedMigrations.push({ table: tableName, operations });
}

export function getCollectedSchema(): Schema {
  const schema = { tables: [...collectedTables] };
  collectedTables = [];
  return schema;
}

export function getCollectedMigration(): Lens | null {
  if (collectedMigrations.length === 0) {
    return null;
  }

  const migration = collectedMigrations[0];
  collectedMigrations = [];

  const operations: LensOp[] = migration.operations.map(({ column, op }) => {
    switch (op._type) {
      case "add":
        return { type: "introduce" as const, column, value: op.default };
      case "drop":
        return { type: "drop" as const, column, value: op.backwardsDefault };
      case "rename":
        return { type: "rename" as const, column, value: op.oldName };
    }
  });

  return { table: migration.table, operations };
}

export function resetCollectedState(): void {
  collectedTables = [];
  collectedMigrations = [];
}
