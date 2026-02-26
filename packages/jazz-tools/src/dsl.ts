// DSL for defining schemas and migrations

import type {
  Column,
  Schema,
  Table,
  SqlType,
  EnumSqlType,
  Lens,
  LensOp,
  AddOp,
  DropOp,
  RenameOp,
  MigrationOp,
  TableMigration,
  ScalarSqlType,
  TSTypeFromSqlType,
} from "./schema.js";

function normalizeEnumVariants(variants: readonly string[]): string[] {
  if (variants.length === 0) {
    throw new Error("Enum columns require at least one variant.");
  }
  for (const variant of variants) {
    if (variant.length === 0) {
      throw new Error("Enum variants cannot be empty strings.");
    }
  }
  const unique = new Set(variants);
  if (unique.size !== variants.length) {
    throw new Error("Enum variants must be unique.");
  }
  return [...unique].sort((a, b) => a.localeCompare(b));
}

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

class EnumBuilder implements ColumnBuilder {
  private _nullable = false;
  public _sqlType: EnumSqlType;

  constructor(...variants: string[]) {
    this._sqlType = { kind: "ENUM", variants: normalizeEnumVariants(variants) };
  }

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

  timestamp(opts: { default: MaybeOptional<Date | number, Optional> }): AddOp {
    return { _type: "add", sqlType: "TIMESTAMP", default: opts.default };
  }

  boolean(opts: { default: MaybeOptional<boolean, Optional> }): AddOp {
    return { _type: "add", sqlType: "BOOLEAN", default: opts.default };
  }

  float(opts: { default: MaybeOptional<number, Optional> }): AddOp {
    return { _type: "add", sqlType: "REAL", default: opts.default };
  }

  bytes(opts: { default: MaybeOptional<Uint8Array, Optional> }): AddOp {
    return { _type: "add", sqlType: "BYTEA", default: opts.default };
  }

  enum<const Variants extends readonly [string, ...string[]]>(
    ...args: [...variants: Variants, opts: { default: MaybeOptional<Variants[number], Optional> }]
  ): AddOp {
    const opts = args[args.length - 1] as { default: MaybeOptional<Variants[number], Optional> };
    const variants = normalizeEnumVariants(args.slice(0, -1) as string[]);
    return {
      _type: "add",
      sqlType: { kind: "ENUM", variants },
      default: opts.default,
    };
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

  timestamp(opts: { backwardsDefault: Date | number }): DropOp {
    return { _type: "drop", sqlType: "TIMESTAMP", backwardsDefault: opts.backwardsDefault };
  }

  boolean(opts: { backwardsDefault: boolean }): DropOp {
    return { _type: "drop", sqlType: "BOOLEAN", backwardsDefault: opts.backwardsDefault };
  }

  float(opts: { backwardsDefault: number }): DropOp {
    return { _type: "drop", sqlType: "REAL", backwardsDefault: opts.backwardsDefault };
  }

  bytes(opts: { backwardsDefault: Uint8Array }): DropOp {
    return { _type: "drop", sqlType: "BYTEA", backwardsDefault: opts.backwardsDefault };
  }

  enum<const Variants extends readonly [string, ...string[]]>(
    ...args: [...variants: Variants, opts: { backwardsDefault: Variants[number] }]
  ): DropOp {
    const opts = args[args.length - 1] as { backwardsDefault: Variants[number] };
    const variants = normalizeEnumVariants(args.slice(0, -1) as string[]);
    return {
      _type: "drop",
      sqlType: { kind: "ENUM", variants },
      backwardsDefault: opts.backwardsDefault,
    };
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
  timestamp: () => new ScalarBuilder("TIMESTAMP"),
  float: () => new ScalarBuilder("REAL"),
  bytes: () => new ScalarBuilder("BYTEA"),
  enum: (...variants: string[]) => new EnumBuilder(...variants),
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
  if (arguments.length > 2) {
    throw new Error(
      "Inline table permissions are no longer supported in current.ts. " +
        "Define policies in schema/permissions.ts with definePermissions(...).",
    );
  }

  const cols: Column[] = [];
  for (const [colName, builder] of Object.entries(columns)) {
    cols.push(builder._build(colName));
  }
  collectedTables.push({
    name,
    columns: cols,
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
