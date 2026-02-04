// DSL for defining schemas and migrations

import type {
  Column,
  Schema,
  Table,
  SqlType,
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

class AddBuilder {
  string(opts: { default: string }): AddOp {
    return { _type: "add", sqlType: "TEXT", default: opts.default };
  }

  int(opts: { default: number }): AddOp {
    return { _type: "add", sqlType: "INTEGER", default: opts.default };
  }

  boolean(opts: { default: boolean }): AddOp {
    return { _type: "add", sqlType: "BOOLEAN", default: opts.default };
  }

  float(opts: { default: number }): AddOp {
    return { _type: "add", sqlType: "REAL", default: opts.default };
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

// ============================================================================
// Side-effect collection
// ============================================================================

let collectedTables: Table[] = [];
let collectedMigrations: TableMigration[] = [];

export function table(name: string, columns: Record<string, ColumnBuilder | RefBuilder>): void {
  const cols: Column[] = [];
  for (const [colName, builder] of Object.entries(columns)) {
    cols.push(builder._build(colName));
  }
  collectedTables.push({ name, columns: cols });
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
