import { rel, reverse } from "./relationships.js";
import { ENUM_CASES_NOT_SUPPORTED } from "./schema-errors.js";
// DSL for defining schemas and migrations

import type { StandardJSONSchemaV1 } from "@standard-schema/spec";
import type {
  Column,
  ColumnMergeStrategy,
  ColumnMergeStrategyName,
  Schema,
  Table,
  SqlType,
  EnumCaseSqlType,
  EnumSqlType,
  JsonSqlType,
  JsonSchema,
  JsonValue,
  AddOp,
  DropOp,
  RenameOp,
  ScalarSqlType,
  TSTypeFromSqlType,
  TSInitFromSqlType,
  VectorSqlType,
} from "./schema.js";
import {
  assertUserColumnNameAllowed,
  assertUserTableColumnNameAllowed,
  type NoExplicitIdColumn,
} from "./magic-columns.js";

const MAX_ENUM_VARIANTS = 256;

function normalizeEnumVariants(variants: readonly string[]): string[] {
  if (variants.length === 0) {
    throw new Error("Enum columns require at least one variant.");
  }
  if (variants.length > MAX_ENUM_VARIANTS) {
    throw new Error(`Enum columns support at most ${MAX_ENUM_VARIANTS} variants.`);
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
  // Scalar-enum discriminants are declaration-order-sensitive. Keeping this
  // order lets later schemas append a case without retagging existing values.
  return [...variants];
}

type JsonSchemaSource<Output = JsonValue> = StandardJSONSchemaV1<unknown, Output> | JsonSchema;

function isJsonObject(value: unknown): value is JsonSchema {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}

function normalizeJsonSchema<Output>(schema: JsonSchemaSource<Output>): JsonSchema {
  const maybeStandard =
    "~standard" in schema
      ? (schema as StandardJSONSchemaV1<unknown, Output>)["~standard"]
      : undefined;
  if (maybeStandard) {
    const converted = maybeStandard.jsonSchema.output({ target: "draft-07" });
    if (!isJsonObject(converted)) {
      throw new Error(
        "JSON schema conversion failed: expected an object from ~standard.jsonSchema.output(...).",
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

interface ColumnBuilder {
  optional(): this;
  default(value: unknown): this;
  merge(strategy: ColumnMergeStrategyName): this;
  transform(name: string, view: ColumnBuilder): ColumnBuilder;
  _build(name: string): Column;
  _sqlType: SqlType;
  _references: string | undefined;
  _transform?: ColumnTransform;
  _allowExternalProvenanceName?: true;
}

type MergeStrategyColumnType = {
  counter: "INTEGER" | "BIGINT";
  "g-set": { kind: "ARRAY" };
  text: "TEXT";
  markdown: "TEXT";
  html: "TEXT";
};

type TextMergeStrategy = "text" | "markdown" | "html";

// Text merges also run on optional columns; counter and g-set need a value.
type AllowedColumnMergeStrategy<
  Sql extends SqlType,
  Optional extends boolean,
> = Optional extends true
  ? "lww" | (Sql extends "TEXT" ? TextMergeStrategy : never)
  : | "lww"
    | {
        [Strategy in keyof MergeStrategyColumnType]: Sql extends MergeStrategyColumnType[Strategy]
          ? Strategy
          : never;
      }[keyof MergeStrategyColumnType];
type ColumnDefaultValue<Sql extends SqlType> = Sql extends "TIMESTAMP"
  ? Date | number
  : TSInitFromSqlType<Sql>;

export type TypedColumnBuilder<
  Sql extends SqlType = SqlType,
  Optional extends boolean = boolean,
  Ref extends string | undefined = string | undefined,
  HasDefault extends boolean = boolean,
  Value = TSTypeFromSqlType<Sql>,
  Init = TSInitFromSqlType<Sql>,
> = Omit<ColumnBuilder, "optional" | "default" | "merge" | "transform"> & {
  readonly __jazzSqlType: Sql;
  readonly __jazzOptional: Optional;
  readonly __jazzReferences: Ref;
  readonly __jazzHasDefault: HasDefault;
  readonly __jazzValue: Value;
  readonly __jazzInitValue: Init;
  /**
   * Set the default value for the column.
   *
   * `db.insert(...)` and `db.restore(...)` apply this literal when the column is omitted before
   * submitting the write to the runtime. Explicit `null` on nullable columns is preserved.
   */
  default(
    value: MaybeOptional<ColumnDefaultValue<Sql>, Optional>,
  ): ColumnAlias<Sql, Optional, Ref, true, Value, Init>;
  /**
   * Set the merge strategy for the column (defaults to LWW)
   */
  merge(
    strategy: AllowedColumnMergeStrategy<Sql, Optional>,
  ): ColumnAlias<Sql, Optional, Ref, HasDefault, Value, Init>;
  /**
   * Store the column through the app-wide transform `name`: the engine keeps
   * this column's type, code reads and writes `view`'s. Every language
   * implements the transform (in TS, `jazz.computed.ts`). Permissions see
   * the stored value.
   */
  transform<const Name extends string>(
    name: Name,
    view: AnyTypedColumnBuilder,
  ): ColumnAlias<Sql, Optional, Ref, HasDefault, Value, Init>;
  /**
   * Make the column nullable
   */
  optional(): ColumnAlias<Sql, true, Ref, HasDefault, Value, Init>;
  /**
   * The column holds ids of `table`'s rows, for a reference no forward
   * relation names (a forward relation's column references its table
   * without it).
   */
  references<const Target extends string>(
    table: Target,
  ): ColumnAlias<Sql, Optional, Target, HasDefault, Value, Init>;
};

// This is a constraint for builder input positions, not a public builder
// surface. Omitting its generic mutator methods avoids contravariant method
// parameter collapse when a payload enum's discriminated SQL type is widened.
export type AnyTypedColumnBuilder = Omit<
  ColumnBuilder,
  "optional" | "default" | "merge" | "transform"
> & {
  readonly __jazzSqlType: SqlType;
  readonly __jazzOptional: boolean;
  readonly __jazzReferences: string | undefined;
  readonly __jazzHasDefault: boolean;
  readonly __jazzValue: unknown;
  readonly __jazzInitValue: unknown;
};

/**
 * Acknowledge that a conventional provenance name is imported domain data,
 * not Jazz's authenticated `$createdAt`/`$createdBy` metadata. Use sparingly.
 */
export function allowExternalProvenanceName<T extends AnyTypedColumnBuilder>(column: T): T {
  (column as unknown as { _allowExternalProvenanceName?: true })._allowExternalProvenanceName =
    true;
  return column;
}

export function hasExternalProvenanceNameAllowance(column: {
  _allowExternalProvenanceName?: true;
}): boolean {
  return column._allowExternalProvenanceName === true;
}
/** A client-side, read-only field with no stored column. */
/**
 * A field computed in code from stored columns of the same row (`s.computed`):
 * not stored, so only its type and inputs are declared here; the
 * implementation lives in `jazz.computed.ts`.
 */
export interface ComputedColumn<Value = unknown, From extends string = string> {
  readonly _computed: {
    readonly type: ColumnBuilder;
    readonly from: readonly From[];
  };
  readonly _transform?: never;
  readonly _allowExternalProvenanceName?: never;
  readonly __jazzSqlType: never;
  readonly __jazzOptional: false;
  readonly __jazzReferences: undefined;
  readonly __jazzHasDefault: false;
  readonly __jazzValue: Readonly<Value>;
  readonly __jazzInitValue: never;
}

/** `s.computed(type, from)`: a field of the row computed in code from `from`'s stored columns. */
export function computed<const T extends AnyTypedColumnBuilder, const From extends string>(
  type: T,
  from: readonly From[],
): ComputedColumn<
  ColumnBuilderOptional<T> extends true ? ColumnBuilderValue<T> | null : ColumnBuilderValue<T>,
  From
> {
  const builder = type as unknown as ColumnBuilder;
  if (!builder || typeof builder !== "object" || !("_sqlType" in builder))
    throw new TypeError("s.computed(type, from) requires a column type such as s.int().");
  if (!Array.isArray(from) || from.some((name) => typeof name !== "string"))
    throw new TypeError("s.computed(type, from) requires the array of columns it reads.");
  return Object.freeze({
    _computed: Object.freeze({ type: builder, from: Object.freeze([...from]) }),
  }) as unknown as ComputedColumn<never, From>;
}

export type AnyColumnDefinition = AnyTypedColumnBuilder | ComputedColumn;

export function isComputedColumn(column: AnyColumnDefinition): column is ComputedColumn {
  return "_computed" in column;
}

export type ColumnBuilderSqlType<TBuilder extends AnyColumnDefinition> = TBuilder["__jazzSqlType"];
export type ColumnBuilderOptional<TBuilder extends AnyColumnDefinition> =
  TBuilder["__jazzOptional"];
export type ColumnBuilderReferences<TBuilder extends AnyColumnDefinition> =
  TBuilder["__jazzReferences"];
export type ColumnBuilderHasDefault<TBuilder extends AnyColumnDefinition> =
  TBuilder["__jazzHasDefault"];
export type ColumnBuilderValue<TBuilder extends AnyColumnDefinition> = TBuilder["__jazzValue"];

/** Input values are explicit so even same-shape transforms retain their contract. */
export type ColumnBuilderInitValue<TBuilder extends AnyColumnDefinition> =
  TBuilder["__jazzInitValue"];

/** A column's transform: its app-wide name and the type code sees. */
export interface ColumnTransform {
  readonly name: string;
  readonly view: ColumnBuilder;
}

export type StringColumn<
  Optional extends boolean = false,
  HasDefault extends boolean = false,
  Value = string,
  Init = Value,
> = TypedColumnBuilder<"TEXT", Optional, undefined, HasDefault, Value, Init>;
/** UUID value without an application-table foreign key, e.g. an account ID. */
export type UuidColumn<
  Optional extends boolean = false,
  HasDefault extends boolean = false,
  Value = string,
  Init = Value,
> = TypedColumnBuilder<"UUID", Optional, undefined, HasDefault, Value, Init>;
export type BooleanColumn<
  Optional extends boolean = false,
  HasDefault extends boolean = false,
  Value = boolean,
  Init = Value,
> = TypedColumnBuilder<"BOOLEAN", Optional, undefined, HasDefault, Value, Init>;
export type IntColumn<
  Optional extends boolean = false,
  HasDefault extends boolean = false,
  Value = number,
  Init = Value,
> = TypedColumnBuilder<"INTEGER", Optional, undefined, HasDefault, Value, Init>;
export type BigIntColumn<
  Optional extends boolean = false,
  HasDefault extends boolean = false,
  Value = bigint,
  Init = Value,
> = TypedColumnBuilder<"BIGINT", Optional, undefined, HasDefault, Value, Init>;
export type TimestampColumn<
  Optional extends boolean = false,
  HasDefault extends boolean = false,
  Value = Date,
  Init = Value,
> = TypedColumnBuilder<"TIMESTAMP", Optional, undefined, HasDefault, Value, Init>;
export type FloatColumn<
  Optional extends boolean = false,
  HasDefault extends boolean = false,
  Value = number,
  Init = Value,
> = TypedColumnBuilder<"REAL", Optional, undefined, HasDefault, Value, Init>;
export type BytesColumn<
  Optional extends boolean = false,
  HasDefault extends boolean = false,
  Value = Uint8Array,
  Init = Value,
> = TypedColumnBuilder<"BYTEA", Optional, undefined, HasDefault, Value, Init>;
export type PointColumn<
  Optional extends boolean = false,
  HasDefault extends boolean = false,
  Value = readonly [lng: number, lat: number],
  Init = Value,
> = TypedColumnBuilder<"POINT", Optional, undefined, HasDefault, Value, Init>;
export type VectorColumn<
  Optional extends boolean = false,
  HasDefault extends boolean = false,
  Value = Float32Array,
  Init = Float32Array | readonly number[],
> = TypedColumnBuilder<VectorSqlType, Optional, undefined, HasDefault, Value, Init>;
export type JsonColumn<
  Output = JsonValue,
  Optional extends boolean = false,
  HasDefault extends boolean = false,
  Value = Output,
  Init = Value,
> = TypedColumnBuilder<JsonSqlType<Output>, Optional, undefined, HasDefault, Value, Init>;
export type EnumColumn<
  Variants extends readonly string[] = readonly string[],
  Optional extends boolean = false,
  HasDefault extends boolean = false,
  Value = Variants[number],
  Init = Value,
> = TypedColumnBuilder<
  {
    kind: "ENUM";
    variants: [...Variants];
  },
  Optional,
  undefined,
  HasDefault,
  Value,
  Init
>;
export type EnumCasesColumn<
  Cases extends readonly EnumCaseSqlType[] = readonly EnumCaseSqlType[],
  Optional extends boolean = false,
  HasDefault extends boolean = false,
  Value = TSTypeFromSqlType<{ kind: "ENUM"; cases: Cases }>,
  Init = TSInitFromSqlType<{ kind: "ENUM"; cases: Cases }>,
> = TypedColumnBuilder<
  { kind: "ENUM"; cases: Cases },
  Optional,
  undefined,
  HasDefault,
  Value,
  Init
>;
export type RefColumn<
  TargetTable extends string,
  Optional extends boolean = false,
  HasDefault extends boolean = false,
  Value = string,
  Init = Value,
> = TypedColumnBuilder<"UUID", Optional, TargetTable, HasDefault, Value, Init>;
export type ArrayColumn<
  ElementSql extends SqlType = SqlType,
  Optional extends boolean = false,
  Ref extends string | undefined = undefined,
  HasDefault extends boolean = false,
  Value = TSTypeFromSqlType<{ kind: "ARRAY"; element: ElementSql }>,
  Init = TSInitFromSqlType<{ kind: "ARRAY"; element: ElementSql }>,
> = TypedColumnBuilder<
  {
    kind: "ARRAY";
    element: ElementSql;
  },
  Optional,
  Ref,
  HasDefault,
  Value,
  Init
>;
export type ColumnAlias<
  Sql extends SqlType = SqlType,
  Optional extends boolean = boolean,
  Ref extends string | undefined = string | undefined,
  HasDefault extends boolean = boolean,
  Value = TSTypeFromSqlType<Sql>,
  Init = TSInitFromSqlType<Sql>,
> = Sql extends {
  kind: "ARRAY";
  element: infer ElementSql extends SqlType;
}
  ? ArrayColumn<ElementSql, Optional, Ref, HasDefault, Value, Init>
  : Ref extends string
    ? RefColumn<Ref, Optional, HasDefault, Value, Init>
    : Sql extends "TEXT"
      ? StringColumn<Optional, HasDefault, Value, Init>
      : Sql extends "BOOLEAN"
        ? BooleanColumn<Optional, HasDefault, Value, Init>
        : Sql extends "INTEGER"
          ? IntColumn<Optional, HasDefault, Value, Init>
          : Sql extends "BIGINT"
            ? BigIntColumn<Optional, HasDefault, Value, Init>
            : Sql extends "TIMESTAMP"
              ? TimestampColumn<Optional, HasDefault, Value, Init>
              : Sql extends "REAL"
                ? FloatColumn<Optional, HasDefault, Value, Init>
                : Sql extends "BYTEA"
                  ? BytesColumn<Optional, HasDefault, Value, Init>
                  : Sql extends "POINT"
                    ? PointColumn<Optional, HasDefault, Value, Init>
                    : Sql extends VectorSqlType
                      ? VectorColumn<Optional, HasDefault, Value, Init>
                      : Sql extends JsonSqlType<infer Output>
                        ? JsonColumn<Output, Optional, HasDefault, Value, Init>
                        : Sql extends {
                              kind: "ENUM";
                              variants: infer Variants extends readonly string[];
                            }
                          ? EnumColumn<Variants, Optional, HasDefault, Value, Init>
                          : Sql extends {
                                kind: "ENUM";
                                cases: infer Cases extends readonly EnumCaseSqlType[];
                              }
                            ? EnumCasesColumn<Cases, Optional, HasDefault, Value, Init>
                            : TypedColumnBuilder<Sql, Optional, Ref, HasDefault, Value, Init>;

function normalizeColumnMergeStrategy(
  strategy: ColumnMergeStrategyName,
  sqlType: SqlType,
  nullable: boolean,
): ColumnMergeStrategy | undefined {
  if (strategy === "lww") {
    return undefined;
  }
  if (strategy === "text" || strategy === "markdown" || strategy === "html") {
    if (sqlType !== "TEXT") {
      throw new Error(`${strategy} merge strategy is only supported on TEXT columns.`);
    }
    return strategy;
  }
  if (strategy === "g-set") {
    if (nullable || typeof sqlType !== "object" || sqlType.kind !== "ARRAY") {
      throw new Error("g-set merge strategy is only supported on non-nullable ARRAY columns.");
    }
    return "g-set";
  }
  if ((sqlType !== "INTEGER" && sqlType !== "BIGINT") || nullable) {
    throw new Error(
      "Counter merge strategy is only supported on non-nullable INTEGER or BIGINT columns.",
    );
  }
  return "counter";
}

abstract class BaseColumnBuilder implements ColumnBuilder {
  private _nullable = false;
  private _default: unknown = undefined;
  private _mergeStrategy: ColumnMergeStrategy | undefined;
  _transform?: ColumnTransform;
  abstract readonly _sqlType: SqlType;

  optional(): this {
    if (this._mergeStrategy !== undefined) {
      // Counter and g-set columns must be non-nullable; re-validate as
      // nullable so the strategy's own error is thrown.
      normalizeColumnMergeStrategy(this._mergeStrategy, this._sqlType, true);
    }
    this._nullable = true;
    return this;
  }

  default(value: unknown): this {
    this._default = value;
    return this;
  }

  merge(strategy: ColumnMergeStrategyName): this {
    this._mergeStrategy = normalizeColumnMergeStrategy(strategy, this._sqlType, this._nullable);
    return this;
  }

  transform(name: string, view: ColumnBuilder): this {
    if (!/^[A-Za-z_][A-Za-z0-9_]*$/.test(name))
      throw new Error(`.transform(name, view): "${name}" is not an identifier.`);
    if (view._transform || view._sqlType === undefined)
      throw new Error(`.transform("${name}", view): view must be a plain column type.`);
    this._transform = { name, view };
    return this;
  }

  private _referencesTable: string | undefined;

  references(table: string): this {
    const sqlType = this._sqlType;
    const uuid =
      sqlType === "UUID" ||
      (typeof sqlType === "object" && sqlType.kind === "ARRAY" && sqlType.element === "UUID");
    if (!uuid) throw new Error(".references(table) is only for uuid and uuid-array columns.");
    if (typeof table !== "string" || !table)
      throw new Error(".references(table) requires a table name.");
    this._referencesTable = table;
    return this;
  }

  _build(name: string): Column {
    return {
      name,
      sqlType: this._sqlType,
      nullable: this._nullable,
      ...(this._referencesTable === undefined ? {} : { references: this._referencesTable }),
      ...(this._default === undefined ? {} : { default: this._default }),
      ...(this._mergeStrategy === undefined ? {} : { mergeStrategy: this._mergeStrategy }),
    };
  }

  get _references(): string | undefined {
    return this._referencesTable;
  }
}

class ScalarBuilder extends BaseColumnBuilder {
  constructor(public _sqlType: ScalarSqlType) {
    super();
  }
}

class VectorBuilder extends BaseColumnBuilder {
  public _sqlType: VectorSqlType;

  constructor(dimensions: number) {
    super();
    this._sqlType = vectorSqlType(dimensions);
  }
}

function vectorSqlType(dimensions: number): VectorSqlType {
  // How many a vector may have is the schema compiler's to check.
  if (!Number.isInteger(dimensions))
    throw new Error("s.vector(dimensions) requires a whole number of dimensions.");
  return { kind: "VECTOR", dimensions };
}

class EnumBuilder extends BaseColumnBuilder {
  public _sqlType: EnumSqlType;

  constructor(variants: string[]) {
    super();
    this._sqlType = { kind: "ENUM", variants: normalizeEnumVariants(variants) };
  }
}

function containsReference(sqlType: SqlType): boolean {
  return (
    typeof sqlType === "object" && sqlType.kind === "ARRAY" && containsReference(sqlType.element)
  );
}

function isUuidArrayReference(builder: ColumnBuilder): boolean {
  const sqlType = builder._sqlType;
  return (
    typeof sqlType === "object" &&
    sqlType.kind === "ARRAY" &&
    sqlType.element === "UUID" &&
    builder._references !== undefined
  );
}

class JsonBuilder<Output = JsonValue> extends BaseColumnBuilder {
  public _sqlType: JsonSqlType<Output>;

  constructor(schema?: JsonSchemaSource<Output>) {
    super();
    this._sqlType = schema
      ? { kind: "JSON", schema: normalizeJsonSchema(schema) }
      : { kind: "JSON" };
  }
}

class ArrayBuilder<T extends ColumnBuilder> extends BaseColumnBuilder {
  constructor(public _element: T) {
    super();
    if (isUuidArrayReference(_element)) {
      throw new Error(
        "Nested reference arrays are not supported; use a direct array(ref(...)) column.",
      );
    }
  }

  _build(name: string): Column {
    const references = this._references;
    return { ...super._build(name), ...(references === undefined ? {} : { references }) };
  }

  get _sqlType(): SqlType {
    return { kind: "ARRAY" as const, element: this._element._sqlType };
  }

  get _references(): string | undefined {
    return super._references ?? this._element._references;
  }
}

// ============================================================================
// Add Builder (for migration context)
// ============================================================================

type MaybeOptional<T, Optional extends boolean> = Optional extends true ? T | null : T;
type ArrayElementSource = SqlType | AnyTypedColumnBuilder;
type ArrayElementSqlType<TElement extends ArrayElementSource> =
  TElement extends AnyTypedColumnBuilder ? ColumnBuilderSqlType<TElement> : TElement;
type ArrayElementValue<TElement extends ArrayElementSource> = TSTypeFromSqlType<
  ArrayElementSqlType<TElement>
>;

function isTypedColumnBuilder(value: ArrayElementSource): value is AnyTypedColumnBuilder {
  return typeof value === "object" && value !== null && "_build" in value && "_sqlType" in value;
}

class AddBuilder {
  string<const TDefault extends string | null>(opts: {
    default: TDefault;
  }): AddOp<"TEXT", TDefault> {
    return { _type: "add", sqlType: "TEXT", default: opts.default };
  }

  int<const TDefault extends number | null>(opts: {
    default: TDefault;
  }): AddOp<"INTEGER", TDefault> {
    return { _type: "add", sqlType: "INTEGER", default: opts.default };
  }

  bigint<const TDefault extends bigint | null>(opts: {
    default: TDefault;
  }): AddOp<"BIGINT", TDefault> {
    return { _type: "add", sqlType: "BIGINT", default: opts.default };
  }

  timestamp<const TDefault extends Date | number | null>(opts: {
    default: TDefault;
  }): AddOp<"TIMESTAMP", TDefault> {
    return { _type: "add", sqlType: "TIMESTAMP", default: opts.default };
  }

  boolean<const TDefault extends boolean | null>(opts: {
    default: TDefault;
  }): AddOp<"BOOLEAN", TDefault> {
    return { _type: "add", sqlType: "BOOLEAN", default: opts.default };
  }

  float<const TDefault extends number | null>(opts: {
    default: TDefault;
  }): AddOp<"REAL", TDefault> {
    return { _type: "add", sqlType: "REAL", default: opts.default };
  }

  bytes<const TDefault extends Uint8Array | null>(opts: {
    default: TDefault;
  }): AddOp<"BYTEA", TDefault> {
    return { _type: "add", sqlType: "BYTEA", default: opts.default };
  }

  point<const TDefault extends readonly [lng: number, lat: number] | null>(opts: {
    default: TDefault;
  }): AddOp<"POINT", TDefault> {
    return { _type: "add", sqlType: "POINT", default: opts.default };
  }

  vector<const TDefault extends Float32Array | readonly number[] | null>(
    dimensions: number,
    opts: { default: TDefault },
  ): AddOp<VectorSqlType, TDefault> {
    return { _type: "add", sqlType: vectorSqlType(dimensions), default: opts.default };
  }

  /** A plain `s.uuid()` column; for a reference to a table, use {@link AddBuilder.ref}. */
  uuid<const TDefault extends string | null>(opts: { default: TDefault }): AddOp<"UUID", TDefault> {
    return { _type: "add", sqlType: "UUID", default: opts.default };
  }

  ref<const TTargetTable extends string, const TDefault extends string | null>(
    _targetTable: TTargetTable,
    opts: { default: TDefault },
  ): AddOp<"UUID", TDefault> {
    return { _type: "add", sqlType: "UUID", default: opts.default };
  }

  json<const TDefault extends JsonValue | null>(opts: {
    default: TDefault;
    schema?: JsonSchemaSource;
  }): AddOp<JsonSqlType, TDefault> {
    return {
      _type: "add",
      sqlType: opts.schema
        ? { kind: "JSON", schema: normalizeJsonSchema(opts.schema) }
        : { kind: "JSON" },
      default: opts.default,
    };
  }

  enum<
    const Variants extends readonly [string, ...string[]],
    const TDefault extends Variants[number] | null,
  >(
    ...args: [...variants: Variants, opts: { default: TDefault }]
  ): AddOp<{ kind: "ENUM"; variants: [...Variants] }, TDefault> {
    const opts = args[args.length - 1] as { default: TDefault };
    const variants = normalizeEnumVariants(args.slice(0, -1) as string[]);
    return {
      _type: "add",
      sqlType: { kind: "ENUM", variants: variants as [...Variants] },
      default: opts.default,
    };
  }

  array<
    TElement extends ArrayElementSource,
    const TDefault extends ArrayElementValue<TElement>[] | null,
  >(opts: {
    of: TElement;
    default: TDefault;
  }): AddOp<{ kind: "ARRAY"; element: ArrayElementSqlType<TElement> }, TDefault> {
    return {
      _type: "add",
      sqlType: {
        kind: "ARRAY",
        element: (isTypedColumnBuilder(opts.of)
          ? opts.of._sqlType
          : opts.of) as ArrayElementSqlType<TElement>,
      },
      default: opts.default,
    };
  }
}

// ============================================================================
// Drop Builder (for migration context)
// ============================================================================

class DropBuilder {
  string<const TBackwardsDefault extends string | null>(opts: {
    backwardsDefault: TBackwardsDefault;
  }): DropOp<"TEXT", TBackwardsDefault> {
    return { _type: "drop", sqlType: "TEXT", backwardsDefault: opts.backwardsDefault };
  }

  int<const TBackwardsDefault extends number | null>(opts: {
    backwardsDefault: TBackwardsDefault;
  }): DropOp<"INTEGER", TBackwardsDefault> {
    return { _type: "drop", sqlType: "INTEGER", backwardsDefault: opts.backwardsDefault };
  }

  bigint<const TBackwardsDefault extends bigint | null>(opts: {
    backwardsDefault: TBackwardsDefault;
  }): DropOp<"BIGINT", TBackwardsDefault> {
    return { _type: "drop", sqlType: "BIGINT", backwardsDefault: opts.backwardsDefault };
  }

  timestamp<const TBackwardsDefault extends Date | number | null>(opts: {
    backwardsDefault: TBackwardsDefault;
  }): DropOp<"TIMESTAMP", TBackwardsDefault> {
    return { _type: "drop", sqlType: "TIMESTAMP", backwardsDefault: opts.backwardsDefault };
  }

  boolean<const TBackwardsDefault extends boolean | null>(opts: {
    backwardsDefault: TBackwardsDefault;
  }): DropOp<"BOOLEAN", TBackwardsDefault> {
    return { _type: "drop", sqlType: "BOOLEAN", backwardsDefault: opts.backwardsDefault };
  }

  float<const TBackwardsDefault extends number | null>(opts: {
    backwardsDefault: TBackwardsDefault;
  }): DropOp<"REAL", TBackwardsDefault> {
    return { _type: "drop", sqlType: "REAL", backwardsDefault: opts.backwardsDefault };
  }

  bytes<const TBackwardsDefault extends Uint8Array | null>(opts: {
    backwardsDefault: TBackwardsDefault;
  }): DropOp<"BYTEA", TBackwardsDefault> {
    return { _type: "drop", sqlType: "BYTEA", backwardsDefault: opts.backwardsDefault };
  }

  point<const TBackwardsDefault extends readonly [lng: number, lat: number] | null>(opts: {
    backwardsDefault: TBackwardsDefault;
  }): DropOp<"POINT", TBackwardsDefault> {
    return { _type: "drop", sqlType: "POINT", backwardsDefault: opts.backwardsDefault };
  }

  vector<const TBackwardsDefault extends Float32Array | readonly number[] | null>(
    dimensions: number,
    opts: { backwardsDefault: TBackwardsDefault },
  ): DropOp<VectorSqlType, TBackwardsDefault> {
    return {
      _type: "drop",
      sqlType: vectorSqlType(dimensions),
      backwardsDefault: opts.backwardsDefault,
    };
  }

  /** A plain `s.uuid()` column; for a reference to a table, use {@link DropBuilder.ref}. */
  uuid<const TBackwardsDefault extends string | null>(opts: {
    backwardsDefault: TBackwardsDefault;
  }): DropOp<"UUID", TBackwardsDefault> {
    return { _type: "drop", sqlType: "UUID", backwardsDefault: opts.backwardsDefault };
  }

  ref<const TTargetTable extends string, const TBackwardsDefault extends string | null>(
    _targetTable: TTargetTable,
    opts: { backwardsDefault: TBackwardsDefault },
  ): DropOp<"UUID", TBackwardsDefault> {
    return { _type: "drop", sqlType: "UUID", backwardsDefault: opts.backwardsDefault };
  }

  json<const TBackwardsDefault extends JsonValue | null>(opts: {
    backwardsDefault: TBackwardsDefault;
    schema?: JsonSchemaSource;
  }): DropOp<JsonSqlType, TBackwardsDefault> {
    return {
      _type: "drop",
      sqlType: opts.schema
        ? { kind: "JSON", schema: normalizeJsonSchema(opts.schema) }
        : { kind: "JSON" },
      backwardsDefault: opts.backwardsDefault,
    };
  }

  enum<
    const Variants extends readonly [string, ...string[]],
    const TBackwardsDefault extends Variants[number] | null,
  >(
    ...args: [...variants: Variants, opts: { backwardsDefault: TBackwardsDefault }]
  ): DropOp<{ kind: "ENUM"; variants: [...Variants] }, TBackwardsDefault> {
    const opts = args[args.length - 1] as { backwardsDefault: TBackwardsDefault };
    const variants = normalizeEnumVariants(args.slice(0, -1) as string[]);
    return {
      _type: "drop",
      sqlType: { kind: "ENUM", variants: variants as [...Variants] },
      backwardsDefault: opts.backwardsDefault,
    };
  }

  array<
    TElement extends ArrayElementSource,
    const TBackwardsDefault extends ArrayElementValue<TElement>[] | null,
  >(opts: {
    of: TElement;
    backwardsDefault: TBackwardsDefault;
  }): DropOp<{ kind: "ARRAY"; element: ArrayElementSqlType<TElement> }, TBackwardsDefault> {
    return {
      _type: "drop",
      sqlType: {
        kind: "ARRAY",
        element: (isTypedColumnBuilder(opts.of)
          ? opts.of._sqlType
          : opts.of) as ArrayElementSqlType<TElement>,
      },
      backwardsDefault: opts.backwardsDefault,
    };
  }
}

// ============================================================================
// Schema column and migration operation builders
// ============================================================================

export const schemaColumns = {
  string: () => new ScalarBuilder("TEXT") as unknown as StringColumn,
  uuid: () => new ScalarBuilder("UUID") as unknown as UuidColumn,
  boolean: () => new ScalarBuilder("BOOLEAN") as unknown as BooleanColumn,
  int: () => new ScalarBuilder("INTEGER") as unknown as IntColumn,
  bigint: () => new ScalarBuilder("BIGINT") as unknown as BigIntColumn,
  timestamp: () => new ScalarBuilder("TIMESTAMP") as unknown as TimestampColumn,
  float: () => new ScalarBuilder("REAL") as unknown as FloatColumn,
  bytes: () => new ScalarBuilder("BYTEA") as unknown as BytesColumn,
  /** A WGS84 point, `[lng, lat]` in degrees (GeoJSON's order); searched through a `geoIndex`. */
  point: () => new ScalarBuilder("POINT") as unknown as PointColumn,
  /** `dimensions` 32-bit floats (an embedding); searched through a `vectorIndex`. */
  vector: (dimensions: number) => new VectorBuilder(dimensions) as unknown as VectorColumn,
  json: jsonColumn as unknown as {
    (): JsonColumn;
    <Schema extends StandardJSONSchemaV1<unknown, unknown>>(
      schema: Schema,
    ): JsonColumn<StandardJSONSchemaV1.InferOutput<Schema>>;
    (schema: JsonSchema): JsonColumn;
  },
  enum: ((...input: unknown[]) => {
    if (input.length === 1 && typeof input[0] === "object" && input[0] !== null) {
      throw new Error(ENUM_CASES_NOT_SUPPORTED);
    }
    return new EnumBuilder(input as string[]);
  }) as unknown as <const Variants extends readonly [string, ...string[]]>(
    ...variants: Variants
  ) => EnumColumn<Variants>,
  rel,
  reverse,
  array: <Builder extends AnyTypedColumnBuilder>(element: Builder) =>
    new ArrayBuilder(element as unknown as ColumnBuilder) as unknown as ArrayColumn<
      ColumnBuilderSqlType<Builder>,
      false,
      ColumnBuilderReferences<Builder>
    >,
};

export const migrationOperations = {
  /**
   * Add a new column to the table
   */
  add: /*#__PURE__*/ new AddBuilder(),
  /**
   * Drop a column from the table
   */
  drop: /*#__PURE__*/ new DropBuilder(),
  /**
   * Rename a column in the table
   */
  renameFrom: <const TOldName extends string>(oldName: TOldName): RenameOp<TOldName> => ({
    _type: "rename",
    oldName,
  }),
};
