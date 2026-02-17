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
  ScalarSqlType,
  TSTypeFromSqlType,
  ArraySqlType,
} from "./schema.js";

// ============================================================================
// Column Builder (for schema context)
// ============================================================================

interface ColumnBuilder {
  optional(): this;
  _build(name: string): Column;
  _sqlType: SqlType;
  _references: string | undefined;
}

class ScalarBuilder implements ColumnBuilder {
  private _nullable = false;

  constructor(public _sqlType: ScalarSqlType) {}

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

  get _references(): string | undefined {
    return undefined;
  }
}

// ============================================================================
// Ref Builder (for foreign key references in schema context)
// ============================================================================

class RefBuilder implements ColumnBuilder {
  private _nullable = false;

  constructor(private _targetTable: string) {}

  optional(): this {
    this._nullable = true;
    return this;
  }

  _build(name: string): Column {
    return {
      name,
      sqlType: this._sqlType,
      nullable: this._nullable,
      references: this._references,
    };
  }

  get _sqlType(): SqlType {
    return "UUID";
  }

  get _references(): string | undefined {
    return this._targetTable;
  }
}

class ArrayBuilder implements ColumnBuilder {
  private _nullable = false;

  constructor(private _element: ColumnBuilder) {}

  optional(): this {
    this._nullable = true;
    return this;
  }

  _build(name: string): Column {
    return {
      name,
      sqlType: this._sqlType,
      nullable: this._nullable,
      references: this._references,
    };
  }

  get _sqlType(): SqlType {
    return { kind: "ARRAY" as const, element: this._element._sqlType };
  }

  get _references(): string | undefined {
    return this._element._references;
  }
}

// ============================================================================
// Add Builder (for migration context)
// ============================================================================

type MaybeOptional<T, Optional extends boolean> = Optional extends true ? T | null : T;

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

  array<T extends SqlType>(opts: {
    of: T;
    default: MaybeOptional<TSTypeFromSqlType<T>[], Optional>;
  }): AddOp {
    return {
      _type: "add",
      sqlType: { kind: "ARRAY", element: opts.of },
      default: opts.default,
    };
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

  array<T extends SqlType>(opts: { of: T; backwardsDefault: TSTypeFromSqlType<T>[] }): DropOp {
    return {
      _type: "drop",
      sqlType: { kind: "ARRAY", element: opts.of },
      backwardsDefault: opts.backwardsDefault,
    };
  }
}

// ============================================================================
// col namespace
// ============================================================================

export const col = {
  // Schema context
  string: () => new ScalarBuilder("TEXT"),
  boolean: () => new ScalarBuilder("BOOLEAN"),
  int: () => new ScalarBuilder("INTEGER"),
  float: () => new ScalarBuilder("REAL"),
  ref: (targetTable: string) => new RefBuilder(targetTable),
  array: (element: ColumnBuilder) => new ArrayBuilder(element),

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

export function table(name: string, columns: Record<string, ColumnBuilder>): void {
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
        return {
          type: "introduce" as const,
          column,
          sqlType: op.sqlType,
          value: op.default,
        };
      case "drop":
        return {
          type: "drop" as const,
          column,
          sqlType: op.sqlType,
          value: op.backwardsDefault,
        };
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
