// DSL for defining schemas and migrations

import type { StandardJSONSchemaV1 } from "@standard-schema/spec";
import type {
  Column,
  Schema,
  Table,
  SqlType,
  EnumSqlType,
  JsonSqlType,
  JsonSchema,
  JsonValue,
  Lens,
  LensOp,
  AddOp,
  DropOp,
  RenameOp,
  MigrationOp,
  TableMigration,
  ScalarSqlType,
  ArraySqlType,
  TSTypeFromSqlType,
} from "./schema.js";
import { assertUserColumnNameAllowed } from "./magic-columns.js";

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

type JsonSchemaSource<Output = JsonValue> = StandardJSONSchemaV1<unknown, Output> | JsonSchema;

function isJsonObject(value: unknown): value is JsonSchema {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}

function normalizeJsonSchema<Output>(schema: JsonSchemaSource<Output>): JsonSchema {
  const maybeStandard = (
    schema as {
      "~standard"?: {
        jsonSchema?: {
          input?: (options: { target: string }) => unknown;
        };
      };
    }
  )["~standard"];
  const converter = maybeStandard?.jsonSchema?.input;
  if (typeof converter === "function") {
    const converted = converter({ target: "draft-07" });
    if (!isJsonObject(converted)) {
      throw new Error(
        "JSON schema conversion failed: expected an object from ~standard.jsonSchema.input(...).",
      );
    }
    return converted;
  }

  if (!isJsonObject(schema)) {
    throw new Error("JSON schema must be an object or implement ~standard.jsonSchema.");
  }
  return schema;
}

function jsonColumn(): JsonBuilder<JsonValue>;
function jsonColumn<Schema extends StandardJSONSchemaV1<unknown, unknown>>(
  schema: Schema,
): JsonBuilder<StandardJSONSchemaV1.InferOutput<Schema>>;
function jsonColumn(schema: JsonSchema): JsonBuilder<JsonValue>;
function jsonColumn(schema?: JsonSchemaSource): JsonBuilder {
  return new JsonBuilder(schema);
}

// ============================================================================
// Column Builder (for schema context)
// ============================================================================

type MaybeOptional<T, Optional extends boolean> = Optional extends true ? T | null : T;

interface ColumnBuilder<
  TSqlType extends SqlType = SqlType,
  TValue = TSTypeFromSqlType<TSqlType>,
  Optional extends boolean = boolean,
> {
  optional(): ColumnBuilder<TSqlType, TValue, true>;
  default(value: MaybeOptional<TValue, Optional>): this;
  _build(name: string): Column;
  _sqlType: TSqlType;
  _references: string | undefined;
}

type RefColumnKey = `${string}Id` | `${string}_id`;
type RefArrayColumnKey = `${string}Ids` | `${string}_ids`;

function isValidRefColumnKey(name: string): name is RefColumnKey {
  return name.endsWith("Id") || name.endsWith("_id");
}

function isValidRefArrayColumnKey(name: string): name is RefArrayColumnKey {
  return name.endsWith("Ids") || name.endsWith("_ids");
}

function validateReferenceColumnName(name: string, builder: ColumnBuilder): void {
  if (!builder._references) {
    return;
  }

  if (builder instanceof ArrayBuilder) {
    if (!isValidRefArrayColumnKey(name)) {
      throw new Error(
        `Invalid array reference key '${name}'. Rename it to '${name}_ids' or '${name}Ids'.`,
      );
    }
    return;
  }

  if (!isValidRefColumnKey(name)) {
    throw new Error(`Invalid reference key '${name}'. Rename it to '${name}_id' or '${name}Id'.`);
  }
}

class ScalarBuilder<
  T extends ScalarSqlType,
  Optional extends boolean = false,
> implements ColumnBuilder<T, TSTypeFromSqlType<T>, Optional> {
  private _nullable = false;
  private _default: MaybeOptional<TSTypeFromSqlType<T>, Optional> | undefined;

  constructor(public _sqlType: T) {}

  optional(): ScalarBuilder<T, true> {
    this._nullable = true;
    return this as unknown as ScalarBuilder<T, true>;
  }

  default(value: MaybeOptional<TSTypeFromSqlType<T>, Optional>): this {
    this._default = value;
    return this;
  }

  _build(name: string): Column {
    return {
      name,
      sqlType: this._sqlType,
      nullable: this._nullable,
      ...(this._default === undefined ? {} : { default: this._default }),
    };
  }

  get _references(): string | undefined {
    return undefined;
  }
}

class EnumBuilder<
  Variant extends string = string,
  Optional extends boolean = false,
> implements ColumnBuilder<EnumSqlType, Variant, Optional> {
  private _nullable = false;
  private _default: MaybeOptional<Variant, Optional> | undefined;
  public _sqlType: EnumSqlType;

  constructor(...variants: string[]) {
    this._sqlType = { kind: "ENUM", variants: normalizeEnumVariants(variants) };
  }

  optional(): EnumBuilder<Variant, true> {
    this._nullable = true;
    return this as unknown as EnumBuilder<Variant, true>;
  }

  default(value: MaybeOptional<Variant, Optional>): this {
    this._default = value;
    return this;
  }

  _build(name: string): Column {
    return {
      name,
      sqlType: this._sqlType,
      nullable: this._nullable,
      ...(this._default === undefined ? {} : { default: this._default }),
    };
  }

  get _references(): string | undefined {
    return undefined;
  }
}

class JsonBuilder<Output = JsonValue, Optional extends boolean = false> implements ColumnBuilder<
  JsonSqlType<Output>,
  Output,
  Optional
> {
  private _nullable = false;
  private _default: MaybeOptional<Output, Optional> | undefined;
  public _sqlType: JsonSqlType<Output>;

  constructor(schema?: JsonSchemaSource<Output>) {
    this._sqlType = schema
      ? { kind: "JSON", schema: normalizeJsonSchema(schema) }
      : { kind: "JSON" };
  }

  optional(): JsonBuilder<Output, true> {
    this._nullable = true;
    return this as unknown as JsonBuilder<Output, true>;
  }

  default(value: MaybeOptional<Output, Optional>): this {
    this._default = value;
    return this;
  }

  _build(name: string): Column {
    return {
      name,
      sqlType: this._sqlType,
      nullable: this._nullable,
      ...(this._default === undefined ? {} : { default: this._default }),
    };
  }

  get _references(): string | undefined {
    return undefined;
  }
}

// ============================================================================
// Ref Builder (for foreign key references in schema context)
// ============================================================================

class RefBuilder<Optional extends boolean = false> implements ColumnBuilder<
  "UUID",
  string,
  Optional
> {
  private _nullable = false;
  private _default: MaybeOptional<string, Optional> | undefined;

  constructor(private _targetTable: string) {}

  optional(): RefBuilder<true> {
    this._nullable = true;
    return this as unknown as RefBuilder<true>;
  }

  default(value: MaybeOptional<string, Optional>): this {
    this._default = value;
    return this;
  }

  _build(name: string): Column {
    return {
      name,
      sqlType: this._sqlType,
      nullable: this._nullable,
      ...(this._default === undefined ? {} : { default: this._default }),
      references: this._references,
    };
  }

  get _sqlType(): "UUID" {
    return "UUID";
  }

  get _references(): string | undefined {
    return this._targetTable;
  }
}

type BuilderValue<T extends ColumnBuilder> =
  T extends ColumnBuilder<SqlType, infer TValue, boolean> ? TValue : never;

class ArrayBuilder<
  T extends ColumnBuilder,
  Optional extends boolean = false,
> implements ColumnBuilder<ArraySqlType, BuilderValue<T>[], Optional> {
  private _nullable = false;
  private _default: MaybeOptional<BuilderValue<T>[], Optional> | undefined;

  constructor(public _element: T) {}

  optional(): ArrayBuilder<T, true> {
    this._nullable = true;
    return this as unknown as ArrayBuilder<T, true>;
  }

  default(value: MaybeOptional<BuilderValue<T>[], Optional>): this {
    this._default = value;
    return this;
  }

  _build(name: string): Column {
    return {
      name,
      sqlType: this._sqlType,
      nullable: this._nullable,
      ...(this._default === undefined ? {} : { default: this._default }),
      references: this._references,
    };
  }

  get _sqlType(): ArraySqlType {
    return { kind: "ARRAY" as const, element: this._element._sqlType };
  }

  get _references(): string | undefined {
    return this._element._references;
  }
}

// ============================================================================
// Add Builder (for migration context)
// ============================================================================

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

  json(opts: { default: MaybeOptional<string, Optional>; schema?: JsonSchemaSource }): AddOp {
    return {
      _type: "add",
      sqlType: opts.schema
        ? { kind: "JSON", schema: normalizeJsonSchema(opts.schema) }
        : { kind: "JSON" },
      default: opts.default,
    };
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

  json(opts: { backwardsDefault: string; schema?: JsonSchemaSource }): DropOp {
    return {
      _type: "drop",
      sqlType: opts.schema
        ? { kind: "JSON", schema: normalizeJsonSchema(opts.schema) }
        : { kind: "JSON" },
      backwardsDefault: opts.backwardsDefault,
    };
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
  json: jsonColumn,
  enum: <const Variants extends readonly [string, ...string[]]>(...variants: Variants) =>
    new EnumBuilder<Variants[number]>(...variants),
  ref: (targetTable: string) => new RefBuilder(targetTable),
  array: <T extends ColumnBuilder>(element: T) => new ArrayBuilder(element),

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

type ScalarIdColumnError<K extends string> =
  `Invalid reference key '${K}'. Rename it to '${K}_id' or '${K}Id'`;

type ArrayIdColumnError<K extends string> =
  `Invalid array reference key '${K}'. Rename it to '${K}_ids' or '${K}Ids'`;

type EnforceReferenceColumnNames<T extends Record<string, ColumnBuilder>> = {
  [K in keyof T & string]: T[K] extends RefBuilder
    ? K extends RefColumnKey
      ? T[K]
      : ScalarIdColumnError<K>
    : T[K] extends ArrayBuilder<RefBuilder>
      ? K extends RefArrayColumnKey
        ? T[K]
        : ArrayIdColumnError<K>
      : T[K];
};

export function table<const T extends Record<string, ColumnBuilder>>(
  name: string,
  columns: EnforceReferenceColumnNames<T>,
): void {
  if (arguments.length > 2) {
    throw new Error(
      "Inline table permissions are no longer supported in current.ts. " +
        "Define policies in schema/permissions.ts with definePermissions(...).",
    );
  }

  const cols: Column[] = [];
  for (const [colName, builder] of Object.entries(columns as Record<string, ColumnBuilder>)) {
    validateReferenceColumnName(colName, builder);
    assertUserColumnNameAllowed(colName);
    cols.push(builder._build(colName));
  }
  collectedTables.push({
    name,
    columns: cols,
  });
}

export function migrate(tableName: string, ops: Record<string, MigrationOp>): void {
  const operations = Object.entries(ops).map(([column, op]) => {
    if (op._type !== "drop") {
      assertUserColumnNameAllowed(column);
    }
    return { column, op };
  });
  collectedMigrations.push({ table: tableName, operations });
}

export function getCollectedSchema(): Schema {
  const schema = { tables: [...collectedTables] };
  collectedTables = [];
  return schema;
}

export function getCollectedMigration(): Lens | null {
  const migration = collectedMigrations.shift();
  if (!migration) {
    return null;
  }

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

export function getCollectedMigrations(): Lens[] {
  const migrations = [...collectedMigrations];
  collectedMigrations = [];
  return migrations.map((migration) => {
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
  });
}

export function resetCollectedState(): void {
  collectedTables = [];
  collectedMigrations = [];
}
