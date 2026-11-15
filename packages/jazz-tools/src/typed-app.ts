import type { TxId } from "./runtime/client.js";
import { historyTableName, resolveQueryTable } from "./runtime/history-relation.js";
import { resolveQueryOutputTable } from "./runtime/query-output-table.js";
import { sha256 } from "@noble/hashes/sha2.js";
import { bytesToHex } from "@noble/hashes/utils.js";
import type { ColumnType, WasmSchema } from "./drivers/types.js";
import { magicColumnType, type ProvenanceMagicColumn, type RowAuthor } from "./magic-columns.js";
import { assertSchemaNameAllowed } from "./schema-name.js";
import { WHERE_OPERATORS, type WhereOperator } from "./where-operators.js";
import type { ColumnTransformMap, ColumnTransformRegistry, QueryBuilder } from "./runtime/db.js";
import type { StreamingValueSource } from "./runtime/client.js";
import { vectorLiteralJson } from "./runtime/vector.js";

// ============================================================================
// The generated type map
// ============================================================================

/**
 * How a stored column is kept, as `jazz.app.json` names it. It decides which
 * `where` operators, streaming writes and partial reads the column takes.
 */
export type ColumnKind =
  | "string"
  | "uuid"
  | "boolean"
  | "int"
  | "float"
  | "bigint"
  | "timestamp"
  | "bytes"
  | "json"
  | "enum"
  | "array";

/** One stored column in the generated type map. */
export interface ColumnTypes {
  readonly kind: ColumnKind;
  readonly nullable: boolean;
  /** The stored value's TS type (for a transformed column, before its transform). */
  readonly value: unknown;
}

/**
 * One relation in the generated type map: `one` for a reference column,
 * `many` for a reverse relation or a reference array.
 */
export type RelationTypes =
  | { readonly one: string; readonly nullable: boolean }
  | { readonly many: string; readonly nullable?: boolean };

/** One table in the generated type map. */
export interface TableTypes {
  /** The row as queries return it, `id` included. */
  readonly row: { readonly id: string };
  /** What `db.insert` takes. */
  readonly insert: object;
  /** Every stored column, computed fields excluded. */
  readonly columns: { readonly [column: string]: ColumnTypes };
  readonly relations: { readonly [relation: string]: RelationTypes };
  /** Fields the app computes from other columns of the row (`jazz.computed.ts`). */
  readonly computed?: {
    readonly [field: string]: { readonly from: object; readonly value: unknown };
  };
  /** Columns stored through a named transform: column → transform name. */
  readonly transforms?: { readonly [column: string]: string };
}

/**
 * The type map `jazz-tools generate` generates from `jazz.app.json` into
 * `jazz.generated.ts` (`interface Types extends AppTypes { … }`): one
 * {@link TableTypes} per table. The typed query builder, the hooks and
 * `db.insert` read an app's types from it.
 */
// eslint-disable-next-line @typescript-eslint/no-empty-object-type
export interface AppTypes {}

/** The transforms an app declares (`interface Transforms` in `jazz.generated.ts`). */
// An object type, not an index signature, so the generated interface satisfies it.
export type TransformTypes = object;

/** Kept for code that is generic over an app's types. */
export type SchemaLike = AppTypes;

export type Simplify<T> = { [K in keyof T]: T[K] } & {};

type TableName<TSchema extends SchemaLike> = Extract<keyof TSchema, string>;
type TableTypesOf<TSchema extends SchemaLike, TTable extends string> = TTable extends keyof TSchema
  ? Extract<TSchema[TTable], TableTypes>
  : never;
type ColumnsOf<TSchema extends SchemaLike, TTable extends string> = TableTypesOf<
  TSchema,
  TTable
>["columns"];
type StoredColumnName<TSchema extends SchemaLike, TTable extends string> = Extract<
  keyof ColumnsOf<TSchema, TTable>,
  string
>;
type ColumnOf<
  TSchema extends SchemaLike,
  TTable extends string,
  TColumn extends StoredColumnName<TSchema, TTable>,
> = Extract<ColumnsOf<TSchema, TTable>[TColumn], ColumnTypes>;

// Simplified, so the generated interfaces read as plain object types.
export type TableRow<
  TSchema extends SchemaLike,
  TTable extends TableName<TSchema>,
> = string extends keyof TableTypesOf<TSchema, TTable>["row"]
  ? // An index-signature row (a test's loose app): simplifying would drop `id`.
    TableTypesOf<TSchema, TTable>["row"] & { id: string }
  : Simplify<TableTypesOf<TSchema, TTable>["row"] & { id: string }>;

export type TableInit<TSchema extends SchemaLike, TTable extends TableName<TSchema>> = Simplify<
  TableTypesOf<TSchema, TTable>["insert"]
>;

type StreamingColumnName<TSchema extends SchemaLike, TTable extends string> = {
  [TColumn in StoredColumnName<TSchema, TTable>]-?: ColumnOf<
    TSchema,
    TTable,
    TColumn
  >["kind"] extends "string" | "bytes" | "json"
    ? TColumn
    : never;
}[StoredColumnName<TSchema, TTable>];

/**
 * Input for a streaming insert. Exactly one Text, JSON, or Bytea column is
 * replaced by an asynchronous source; every other column retains its ordinary
 * insert type and required/defaulted status.
 */
export type TableStreamingInit<TSchema extends SchemaLike, TTable extends TableName<TSchema>> =
  // A test's loose app: any column may be the streamed one.
  string extends StreamingColumnName<TSchema, TTable>
    ? TableInit<TSchema, TTable>
    : StrictTableStreamingInit<TSchema, TTable>;

type StrictTableStreamingInit<TSchema extends SchemaLike, TTable extends TableName<TSchema>> = {
  [TColumn in StreamingColumnName<TSchema, TTable>]: Simplify<
    Omit<TableInit<TSchema, TTable>, TColumn> & {
      [TStreamed in TColumn]-?: StreamingValueSource;
    }
  >;
}[StreamingColumnName<TSchema, TTable>];

/** Streaming update/upsert input with one required streamed scalar. */
export type TableStreamingUpdate<TSchema extends SchemaLike, TTable extends TableName<TSchema>> =
  string extends StreamingColumnName<TSchema, TTable>
    ? Partial<TableInit<TSchema, TTable>>
    : StrictTableStreamingUpdate<TSchema, TTable>;

type StrictTableStreamingUpdate<TSchema extends SchemaLike, TTable extends TableName<TSchema>> = {
  [TColumn in StreamingColumnName<TSchema, TTable>]: Simplify<
    Partial<Omit<TableInit<TSchema, TTable>, TColumn>> & {
      [TStreamed in TColumn]-?: StreamingValueSource;
    }
  >;
}[StreamingColumnName<TSchema, TTable>];

type MaybeNullableWhere<T, TOptional extends boolean> = TOptional extends true ? T | null : T;
type NullableWhere<TOptional extends boolean> = TOptional extends true ? { isNull?: boolean } : {};
type WhereEqNe<T, TOptional extends boolean, TExtra extends object = {}> =
  | MaybeNullableWhere<T, TOptional>
  | ({
      eq?: MaybeNullableWhere<T, TOptional>;
      ne?: MaybeNullableWhere<T, TOptional>;
    } & TExtra &
      NullableWhere<TOptional>);
type Membership<T> = { in?: T[]; notIn?: T[] };
type NumberWhere<T extends number | bigint, TOptional extends boolean> = WhereEqNe<
  T,
  TOptional,
  { gt?: T; gte?: T; lt?: T; lte?: T } & Membership<T>
>;
type TimestampWhere<TOptional extends boolean> = WhereEqNe<
  Date | number,
  TOptional,
  {
    gt?: Date | number;
    gte?: Date | number;
    lt?: Date | number;
    lte?: Date | number;
  } & Membership<Date | number>
>;
type UuidWhere<TOptional extends boolean> = WhereEqNe<string, TOptional, Membership<string>>;
type PayloadEnumMatch<T> = T extends { type: infer Case extends string }
  ? { type: Case; where?: Partial<Omit<T, "type">> }
  : never;
type ArrayElement<T> = T extends readonly (infer E)[] ? E : never;

type WhereInputForColumn<C extends ColumnTypes> = C["kind"] extends "string"
  ? WhereEqNe<string, C["nullable"], { contains?: string } & Membership<string>>
  : C["kind"] extends "boolean"
    ? WhereEqNe<boolean, C["nullable"], Membership<boolean>>
    : C["kind"] extends "int" | "float"
      ? NumberWhere<number, C["nullable"]>
      : C["kind"] extends "bigint"
        ? NumberWhere<bigint, C["nullable"]>
        : C["kind"] extends "timestamp"
          ? TimestampWhere<C["nullable"]>
          : C["kind"] extends "uuid"
            ? UuidWhere<C["nullable"]>
            : C["kind"] extends "bytes"
              ? WhereEqNe<Uint8Array, C["nullable"], Membership<Uint8Array | number[]>>
              : C["kind"] extends "json"
                ? WhereEqNe<C["value"], C["nullable"], Membership<C["value"]>>
                : C["kind"] extends "enum"
                  ? [C["value"]] extends [string]
                    ? WhereEqNe<C["value"], C["nullable"], Membership<C["value"]>>
                    : { match?: PayloadEnumMatch<C["value"]> }
                  : C["kind"] extends "array"
                    ? WhereEqNe<
                        C["value"],
                        C["nullable"],
                        { contains?: ArrayElement<C["value"]> } & Membership<C["value"]>
                      >
                    : never;

export type TableWhereInput<
  TSchema extends SchemaLike,
  TTable extends TableName<TSchema>,
> = Simplify<
  {
    id?: string | ({ eq?: string; ne?: string } & Membership<string>);
  } & {
    [TColumn in StoredColumnName<TSchema, TTable>]?: WhereInputForColumn<
      ColumnOf<TSchema, TTable, TColumn>
    >;
  } & {
    [TColumn in ProvenanceMagicColumn]?: ProvenanceMagicColumns[TColumn] extends Date
      ?
          | Date
          | number
          | {
              eq?: Date | number;
              ne?: Date | number;
              gt?: Date | number;
              gte?: Date | number;
              lt?: Date | number;
              lte?: Date | number;
            }
      :
          | ProvenanceMagicColumns[TColumn]
          | { eq?: ProvenanceMagicColumns[TColumn]; ne?: ProvenanceMagicColumns[TColumn] };
  }
>;

type BaseColumnName<TSchema extends SchemaLike, TTable extends TableName<TSchema>> = Extract<
  keyof TableRow<TSchema, TTable>,
  string
>;

type DefaultSelection<
  TSchema extends SchemaLike,
  TTable extends TableName<TSchema>,
> = BaseColumnName<TSchema, TTable>;

type RelationsOf<TSchema extends SchemaLike, TTable extends string> = TableTypesOf<
  TSchema,
  TTable
>["relations"];
type RelationName<TSchema extends SchemaLike, TTable extends string> = Extract<
  keyof RelationsOf<TSchema, TTable>,
  string
>;
type RelationOf<
  TSchema extends SchemaLike,
  TTable extends string,
  TRelation extends RelationName<TSchema, TTable>,
> = Extract<RelationsOf<TSchema, TTable>[TRelation], RelationTypes>;
type RelationTargetTable<
  TSchema extends SchemaLike,
  TTable extends string,
  TRelation extends RelationName<TSchema, TTable>,
> =
  RelationOf<TSchema, TTable, TRelation> extends { readonly one: infer T extends string }
    ? Extract<T, TableName<TSchema>>
    : RelationOf<TSchema, TTable, TRelation> extends { readonly many: infer T extends string }
      ? Extract<T, TableName<TSchema>>
      : never;
type RelationIsArray<
  TSchema extends SchemaLike,
  TTable extends string,
  TRelation extends RelationName<TSchema, TTable>,
> = RelationOf<TSchema, TTable, TRelation> extends { readonly many: string } ? true : false;
type RelationNullable<
  TSchema extends SchemaLike,
  TTable extends string,
  TRelation extends RelationName<TSchema, TTable>,
> = RelationOf<TSchema, TTable, TRelation> extends { readonly nullable: true } ? true : false;

type QueryBuilderShape<
  TSchema extends SchemaLike,
  TTable extends TableName<TSchema>,
  TRow = unknown,
> = QueryBuilder<TRow> & {
  readonly _table: TTable;
  readonly _initType: TableInit<TSchema, TTable>;
  readonly _streamingInitType: TableStreamingInit<TSchema, TTable>;
  readonly _streamingUpdateType: TableStreamingUpdate<TSchema, TTable>;
};

type RelationSeedQuery<TTable extends string = string> = QueryBuilder<unknown> & {
  readonly _table: TTable;
  _serializeRelation(): unknown;
};

type AuthorFields = {
  "": RowAuthor;
  ".account": RowAuthor["account"];
  ".identity": RowAuthor["identity"];
  ".identity.issuer": RowAuthor["identity"]["issuer"];
  ".identity.subject": RowAuthor["identity"]["subject"];
};

type AuthorColumns<Prefix extends string> = {
  [K in keyof AuthorFields as `${Prefix}${K}`]: AuthorFields[K];
};

type NullableColumns<T> = {
  [K in keyof T]: T[K] | null;
};

type ProvenanceMagicColumns = {
  $createdAt: Date;
  $updatedAt: Date;
} & AuthorColumns<"$createdBy"> &
  AuthorColumns<"$updatedBy">;

export type TableSelectableColumn<TSchema extends SchemaLike, TTable extends TableName<TSchema>> =
  | BaseColumnName<TSchema, TTable>
  | ProvenanceMagicColumn
  | "*";

export type TableOrderableColumn<TSchema extends SchemaLike, TTable extends TableName<TSchema>> =
  | BaseColumnName<TSchema, TTable>
  | ProvenanceMagicColumn;

export type TableSelected<
  TSchema extends SchemaLike,
  TTable extends TableName<TSchema>,
  TSelection extends TableSelectableColumn<TSchema, TTable> = BaseColumnName<TSchema, TTable>,
> = SelectedRow<TableRow<TSchema, TTable>, TSelection>;

type DeletionMagicColumns = {
  $deletedAt: Date | null;
} & NullableColumns<AuthorColumns<"$deletedBy">>;
type DeletedColumn<TDeleted extends boolean> = TDeleted extends true
  ? keyof DeletionMagicColumns
  : never;
type DeletedWhere<TDeleted extends boolean> = TDeleted extends true
  ? {
      [K in keyof DeletionMagicColumns]?: K extends "$deletedAt"
        ? TimestampWhere<true>
        : WhereEqNe<NonNullable<DeletionMagicColumns[K]>, true>;
    }
  : {};

/** Shared projection typing for ordinary and history rows. */
type SelectedRow<TRow, TSelection> = string extends keyof TRow
  ? // An index-signature row (a test's loose app): every column, `id` kept.
    TRow
  : Simplify<
      ("*" extends TSelection ? TRow : Pick<TRow, Extract<TSelection | "id", keyof TRow>>) &
        Pick<
          ProvenanceMagicColumns & DeletionMagicColumns,
          Extract<TSelection, ProvenanceMagicColumn | keyof DeletionMagicColumns>
        >
    >;

/** A selected row with its included relations in place of same-named columns. */
type WithIncluded<TSelected, TIncluded> = string extends keyof TSelected
  ? TSelected & TIncluded
  : Simplify<Omit<TSelected, Extract<keyof TIncluded, keyof TSelected>> & TIncluded>;

type ApplyRelationCardinality<
  TValue,
  TIsArray extends boolean,
  TNullable extends boolean,
  TRequired extends boolean,
> = TIsArray extends true
  ? TNullable extends true
    ? TValue[] | null
    : TValue[]
  : TRequired extends true
    ? TNullable extends true
      ? TValue | null
      : TValue
    : TValue | null;

export type TableInclude<TSchema extends SchemaLike, TTable extends TableName<TSchema>> = {
  [TRelation in RelationName<TSchema, TTable>]?:
    | true
    | TableInclude<TSchema, RelationTargetTable<TSchema, TTable, TRelation>>
    | QueryBuilderShape<TSchema, RelationTargetTable<TSchema, TTable, TRelation>, any>;
};

type IncludedRelationValue<
  TSchema extends SchemaLike,
  TTable extends TableName<TSchema>,
  TRelation extends RelationName<TSchema, TTable>,
  TRequired extends boolean,
  TSpec,
> = TSpec extends true
  ? ApplyRelationCardinality<
      TableRow<TSchema, RelationTargetTable<TSchema, TTable, TRelation>>,
      RelationIsArray<TSchema, TTable, TRelation>,
      RelationNullable<TSchema, TTable, TRelation>,
      TRequired
    >
  : TSpec extends QueryBuilderShape<
        TSchema,
        RelationTargetTable<TSchema, TTable, TRelation>,
        infer TQueryRow
      >
    ? ApplyRelationCardinality<
        TQueryRow,
        RelationIsArray<TSchema, TTable, TRelation>,
        RelationNullable<TSchema, TTable, TRelation>,
        TRequired
      >
    : TSpec extends TableInclude<TSchema, RelationTargetTable<TSchema, TTable, TRelation>>
      ? ApplyRelationCardinality<
          TableSelectedWithIncludes<
            TSchema,
            RelationTargetTable<TSchema, TTable, TRelation>,
            TSpec,
            DefaultSelection<TSchema, RelationTargetTable<TSchema, TTable, TRelation>>,
            TRequired
          >,
          RelationIsArray<TSchema, TTable, TRelation>,
          RelationNullable<TSchema, TTable, TRelation>,
          TRequired
        >
      : never;

type IncludedRelations<
  TSchema extends SchemaLike,
  TTable extends TableName<TSchema>,
  TRequired extends boolean,
  TInclude extends TableInclude<TSchema, TTable>,
> = {
  [TRelation in keyof TInclude & RelationName<TSchema, TTable>]-?: IncludedRelationValue<
    TSchema,
    TTable,
    TRelation,
    TRequired,
    NonNullable<TInclude[TRelation]>
  >;
};

export type TableSelectedWithIncludes<
  TSchema extends SchemaLike,
  TTable extends TableName<TSchema>,
  TInclude extends TableInclude<TSchema, TTable> = {},
  TSelection extends TableSelectableColumn<TSchema, TTable> = DefaultSelection<TSchema, TTable>,
  TRequired extends boolean = false,
> = WithIncluded<
  TableSelected<TSchema, TTable, TSelection>,
  IncludedRelations<TSchema, TTable, TRequired, TInclude>
>;

export interface TableRelation<
  TTarget extends AnyTableMeta = AnyTableMeta,
  TIsArray extends boolean = boolean,
  TNullable extends boolean = boolean,
> {
  readonly target: TTarget;
  readonly isArray: TIsArray;
  readonly nullable: TNullable;
}

export type TableRelationMap = Record<string, TableRelation>;

export interface TableMeta<
  TName extends string = string,
  TRow extends { id: string } = { id: string },
  TInit extends object = Record<string, never>,
  TWhere extends object = Record<string, never>,
  TRelations extends TableRelationMap = {},
  TStreamingInit = never,
  TStreamingUpdate = never,
  TLargeValueSelection = never,
> {
  readonly name: TName;
  readonly row: TRow;
  readonly init: TInit;
  readonly streamingInit: TStreamingInit;
  readonly streamingUpdate: TStreamingUpdate;
  /** Typed object-form large-value projection descriptors. */
  readonly largeValueSelection: TLargeValueSelection;
  readonly where: TWhere;
  readonly relations: TRelations;
}

export type AnyTableMeta = TableMeta<
  string,
  { id: string },
  Record<string, unknown>,
  Record<string, unknown>,
  TableRelationMap,
  unknown,
  unknown,
  Record<string, LargeValuePage | undefined>
>;

type TableNameFromMeta<TMeta extends AnyTableMeta> = TMeta["name"];
type TableRowFromMeta<TMeta extends AnyTableMeta> = TMeta["row"];
type TableInitFromMeta<TMeta extends AnyTableMeta> = TMeta["init"];
type TableStreamingInitFromMeta<TMeta extends AnyTableMeta> = TMeta["streamingInit"];
type TableStreamingUpdateFromMeta<TMeta extends AnyTableMeta> = TMeta["streamingUpdate"];
type TableLargeValueSelectionFromMeta<TMeta extends AnyTableMeta> = TMeta["largeValueSelection"];
type TableWhereFromMeta<TMeta extends AnyTableMeta> = TMeta["where"];
type TableRelationsFromMeta<TMeta extends AnyTableMeta> = TMeta["relations"];
type RelationNameFromMeta<TMeta extends AnyTableMeta> = Extract<
  keyof TableRelationsFromMeta<TMeta>,
  string
>;
type RelationFromMeta<
  TMeta extends AnyTableMeta,
  TRelation extends RelationNameFromMeta<TMeta>,
> = TableRelationsFromMeta<TMeta>[TRelation];
type RelationTargetFromMeta<
  TMeta extends AnyTableMeta,
  TRelation extends RelationNameFromMeta<TMeta>,
> = RelationFromMeta<TMeta, TRelation>["target"];
type RelationIsArrayFromMeta<
  TMeta extends AnyTableMeta,
  TRelation extends RelationNameFromMeta<TMeta>,
> = RelationFromMeta<TMeta, TRelation>["isArray"];
type RelationNullableFromMeta<
  TMeta extends AnyTableMeta,
  TRelation extends RelationNameFromMeta<TMeta>,
> = RelationFromMeta<TMeta, TRelation>["nullable"];
type BaseColumnNameFromMeta<TMeta extends AnyTableMeta> = Extract<
  keyof TableRowFromMeta<TMeta>,
  string
>;
type TableSelectableFromMeta<TMeta extends AnyTableMeta> =
  | BaseColumnNameFromMeta<TMeta>
  | ProvenanceMagicColumn
  | "*";

/** A half-open byte range `[from, to)` for a bytes column. */
export interface BytePage {
  from: number;
  to: number;
}

/** A half-open UTF-16 code-unit range `[from, to)` for a text column. */
export interface TextPage {
  from: number;
  to: number;
}

/** A half-open UTF-8 byte range `[fromUtf8, toUtf8)` for a text column. */
export interface Utf8TextPage {
  fromUtf8: number;
  toUtf8: number;
}

/** An RFC 6901 pointer selecting a JSON subtree. */
export interface JsonPointer {
  at: string;
}

export type LargeValuePage = BytePage | TextPage | Utf8TextPage | JsonPointer;

/** A sequential, page-relative byte splice for a BYTEA field. */
export interface ByteValueSplice {
  at: number;
  delete: number;
  insert: Uint8Array;
}

/** A sequential, page-relative UTF-16 splice for a TEXT field. */
export interface TextValueSplice {
  at: number;
  delete: number;
  insert: string;
}

/** A sequential, page-relative UTF-8 splice for a TEXT field. */
export interface Utf8TextValueSplice {
  atUtf8: number;
  deleteUtf8: number;
  insert: string;
}

export interface ByteValueUpdate {
  within: BytePage;
  splices: readonly ByteValueSplice[];
}

export interface TextValueUpdate {
  within: TextPage;
  splices: readonly TextValueSplice[];
}

export interface Utf8TextValueUpdate {
  within: Utf8TextPage;
  splices: readonly Utf8TextValueSplice[];
}

export interface JsonSetValueUpdate {
  edits: readonly { op: "set"; at: string; value: unknown }[];
}

type LargeValueUpdateForColumn<C extends ColumnTypes> = C["kind"] extends "bytes"
  ? ByteValueUpdate
  : C["kind"] extends "string"
    ? TextValueUpdate | Utf8TextValueUpdate
    : C["kind"] extends "json"
      ? JsonSetValueUpdate
      : never;

/** Typed diff map accepted by the `applyDiffs` option of `Db.update`. */
export type TableLargeValueUpdate<
  TSchema extends SchemaLike,
  TTable extends TableName<TSchema>,
> = Simplify<{
  [TColumn in StoredColumnName<TSchema, TTable>]?: LargeValueUpdateForColumn<
    ColumnOf<TSchema, TTable, TColumn>
  >;
}>;

type PageForColumn<C extends ColumnTypes> = C["kind"] extends "bytes"
  ? BytePage
  : C["kind"] extends "string"
    ? TextPage | Utf8TextPage
    : C["kind"] extends "json"
      ? JsonPointer
      : never;

type TableLargeValueSelection<
  TSchema extends SchemaLike,
  TTable extends TableName<TSchema>,
> = Simplify<{
  [TColumn in StoredColumnName<TSchema, TTable>]?: PageForColumn<
    ColumnOf<TSchema, TTable, TColumn>
  >;
}>;

/** Object-form partial projection accepted by `select`. */
export type LargeValueSelection<TMeta extends AnyTableMeta> =
  TableLargeValueSelectionFromMeta<TMeta>;

/**
 * One argument of `select(...)`: a column name, or an object-form partial
 * projection. Names and partial projections may be mixed in any order.
 */
type TableSelectionArg<TMeta extends AnyTableMeta, TDeleted extends boolean> =
  | TableSelectableFromMeta<TMeta>
  | DeletedColumn<TDeleted>
  | LargeValueSelection<TMeta>;

/**
 * Split `select(...)` arguments into whole-column names and per-column partial
 * projections. A column may be selected once: whole (by name, or through
 * `"*"`), or in part. Selecting it both ways, or with two ranges, is ambiguous
 * and throws instead of letting one silently win.
 */
function splitSelection(selection: readonly unknown[]): {
  /** Every selected column, in argument order. */
  columns: string[];
  names: string[];
  partial: Record<string, LargeValuePage>;
} {
  const columns: string[] = [];
  const names: string[] = [];
  const partial: Record<string, LargeValuePage> = {};
  for (const arg of selection) {
    if (typeof arg === "string") {
      if (Object.hasOwn(partial, arg))
        throw new TypeError(
          `select(...): column "${arg}" is selected both whole and in part; selecting a column both ways is not supported. Select it either by name or with a range.`,
        );
      names.push(arg);
      columns.push(arg);
    } else if (typeof arg === "object" && arg !== null && !Array.isArray(arg)) {
      for (const [column, page] of Object.entries(arg as Record<string, LargeValuePage>)) {
        if (Object.hasOwn(partial, column))
          throw new TypeError(
            `select(...): column "${column}" has more than one partial selection; selecting several parts of one column is not supported yet.`,
          );
        if (names.includes(column))
          throw new TypeError(
            `select(...): column "${column}" is selected both whole and in part; selecting a column both ways is not supported. Select it either by name or with a range.`,
          );
        partial[column] = page;
        columns.push(column);
      }
    } else {
      throw new TypeError(
        `select(...) takes column names and partial-selection objects, got ${arg === null ? "null" : Array.isArray(arg) ? "an array" : typeof arg}.`,
      );
    }
  }
  return { columns, names, partial };
}

type TableOrderableFromMeta<TMeta extends AnyTableMeta> =
  | Extract<keyof TMeta["where"], string>
  | ProvenanceMagicColumn;
type DefaultTableSelection<TMeta extends AnyTableMeta> = BaseColumnNameFromMeta<TMeta>;

export type SchemaRelations<TTable extends string, TSchema extends SchemaLike> =
  TTable extends TableName<TSchema>
    ? {
        [TRelation in RelationName<TSchema, TTable>]: TableRelation<
          SchemaTable<RelationTargetTable<TSchema, TTable, TRelation>, TSchema>,
          RelationIsArray<TSchema, TTable, TRelation>,
          RelationNullable<TSchema, TTable, TRelation>
        >;
      }
    : never;

export type SchemaTable<TTable extends string, TSchema extends SchemaLike> =
  TTable extends TableName<TSchema>
    ? TableMeta<
        TTable,
        TableRow<TSchema, TTable>,
        TableInit<TSchema, TTable>,
        TableWhereInput<TSchema, TTable>,
        SchemaRelations<TTable, TSchema>,
        TableStreamingInit<TSchema, TTable>,
        TableStreamingUpdate<TSchema, TTable>,
        TableLargeValueSelection<TSchema, TTable>
      >
    : never;

type SchemaMeta<TTable extends string, TSchema extends SchemaLike> = SchemaTable<TTable, TSchema>;

type MetaQueryBuilderShape<TMeta extends AnyTableMeta, TRow = unknown> = QueryBuilder<TRow> & {
  readonly _table: TableNameFromMeta<TMeta>;
  readonly _initType: TableInitFromMeta<TMeta>;
  readonly _streamingInitType: TableStreamingInitFromMeta<TMeta>;
  readonly _streamingUpdateType: TableStreamingUpdateFromMeta<TMeta>;
};

type BuilderInclude<TMeta extends AnyTableMeta> = {
  [TRelation in RelationNameFromMeta<TMeta>]?:
    | true
    | BuilderInclude<RelationTargetFromMeta<TMeta, TRelation>>
    | MetaQueryBuilderShape<RelationTargetFromMeta<TMeta, TRelation>, any>;
};

type SelectedFromMeta<
  TMeta extends AnyTableMeta,
  TSelection = DefaultTableSelection<TMeta>,
> = SelectedRow<TableRowFromMeta<TMeta>, SelectedColumnNames<TSelection>>;

type SelectedColumnNames<TSelection> =
  TSelection extends Record<string, LargeValuePage> ? keyof TSelection : TSelection;

type IncludedRelationValueFromMeta<
  TMeta extends AnyTableMeta,
  TRelation extends RelationNameFromMeta<TMeta>,
  TRequired extends boolean,
  TSpec,
> = TSpec extends true
  ? ApplyRelationCardinality<
      TableRowFromMeta<RelationTargetFromMeta<TMeta, TRelation>>,
      RelationIsArrayFromMeta<TMeta, TRelation>,
      RelationNullableFromMeta<TMeta, TRelation>,
      TRequired
    >
  : TSpec extends MetaQueryBuilderShape<RelationTargetFromMeta<TMeta, TRelation>, infer TQueryRow>
    ? ApplyRelationCardinality<
        TQueryRow,
        RelationIsArrayFromMeta<TMeta, TRelation>,
        RelationNullableFromMeta<TMeta, TRelation>,
        TRequired
      >
    : TSpec extends BuilderInclude<RelationTargetFromMeta<TMeta, TRelation>>
      ? ApplyRelationCardinality<
          SelectedWithIncludesFromMeta<
            RelationTargetFromMeta<TMeta, TRelation>,
            TSpec,
            DefaultTableSelection<RelationTargetFromMeta<TMeta, TRelation>>,
            TRequired
          >,
          RelationIsArrayFromMeta<TMeta, TRelation>,
          RelationNullableFromMeta<TMeta, TRelation>,
          TRequired
        >
      : never;

type IncludedRelationsFromMeta<
  TMeta extends AnyTableMeta,
  TRequired extends boolean,
  TInclude extends BuilderInclude<TMeta>,
> = {
  [TRelation in keyof TInclude & RelationNameFromMeta<TMeta>]-?: IncludedRelationValueFromMeta<
    TMeta,
    TRelation,
    TRequired,
    NonNullable<TInclude[TRelation]>
  >;
};

type SelectedWithIncludesFromMeta<
  TMeta extends AnyTableMeta,
  TInclude extends BuilderInclude<TMeta> = {},
  TSelection = DefaultTableSelection<TMeta>,
  TRequired extends boolean = false,
> = WithIncluded<
  SelectedFromMeta<TMeta, TSelection>,
  IncludedRelationsFromMeta<TMeta, TRequired, TInclude>
>;

type BuiltCondition = { column: string; op: string; value: unknown };
type BuiltRelation = {
  table?: string;
  conditions?: BuiltCondition[];
  hops?: string[];
  orderBy?: Array<[string, "asc" | "desc"]>;
  limit?: number;
  offset?: number;
  gather?: BuiltGather;
  union?: {
    inputs: BuiltUnionArm[];
  };
};
type BuiltUnionArm = { label: string; input: BuiltRelation };
type BuiltGather = {
  seed?: BuiltRelation;
  max_depth: number;
  step_table: string;
  step_current_column: string;
  step_conditions: BuiltCondition[];
  step_hops: string[];
};

function cloneBuiltCondition(condition: BuiltCondition): BuiltCondition {
  return { ...condition };
}

function queryBuilderJsonReplacer(_key: string, value: unknown): unknown {
  if (value instanceof Uint8Array) return [...value];
  // Hex of the f32le bytes, so a filter keeps every bit (JSON numbers lose -0).
  if (value instanceof Float32Array) return vectorLiteralJson(value);
  return typeof value === "bigint" ? value.toString() : value;
}

function cloneBuiltRelation(relation: BuiltRelation): BuiltRelation {
  return {
    ...(relation.table ? { table: relation.table } : {}),
    ...(relation.conditions ? { conditions: relation.conditions.map(cloneBuiltCondition) } : {}),
    ...(relation.hops ? { hops: [...relation.hops] } : {}),
    ...(relation.orderBy
      ? {
          orderBy: relation.orderBy.map(
            ([key, direction]) => [key, direction] as [string, "asc" | "desc"],
          ),
        }
      : {}),
    ...(relation.limit !== undefined ? { limit: relation.limit } : {}),
    ...(relation.offset !== undefined ? { offset: relation.offset } : {}),
    ...(relation.gather ? { gather: cloneBuiltGather(relation.gather) } : {}),
    ...(relation.union
      ? {
          union: {
            inputs: relation.union.inputs.map((arm) => ({
              label: arm.label,
              input: cloneBuiltRelation(arm.input),
            })),
          },
        }
      : {}),
  };
}

function cloneBuiltGather(gather: BuiltGather): BuiltGather {
  return {
    ...(gather.seed ? { seed: cloneBuiltRelation(gather.seed) } : {}),
    max_depth: gather.max_depth,
    step_table: gather.step_table,
    step_current_column: gather.step_current_column,
    step_conditions: gather.step_conditions.map(cloneBuiltCondition),
    step_hops: [...gather.step_hops],
  };
}

export class TypedTableQueryBuilder<
  TMeta extends AnyTableMeta,
  TInclude extends BuilderInclude<TMeta> = {},
  TSelection = DefaultTableSelection<TMeta>,
  TRequired extends boolean = false,
  TDeleted extends boolean = false,
> implements QueryBuilder<SelectedWithIncludesFromMeta<TMeta, TInclude, TSelection, TRequired>> {
  readonly _table: TableNameFromMeta<TMeta>;
  readonly _schema: WasmSchema;
  declare readonly _rowType: SelectedWithIncludesFromMeta<TMeta, TInclude, TSelection, TRequired>;
  declare readonly _initType: TableInitFromMeta<TMeta>;
  declare readonly _streamingInitType: TableStreamingInitFromMeta<TMeta>;
  declare readonly _streamingUpdateType: TableStreamingUpdateFromMeta<TMeta>;
  private _conditions: BuiltCondition[] = [];
  private _includes: Partial<BuilderInclude<TMeta>> = {};
  private _requireIncludes = false;
  private _select?: string[] | Record<string, LargeValuePage>;
  private _partialSelect?: Record<string, LargeValuePage>;
  private _orderBys: Array<[string, "asc" | "desc"]> = [];
  private _limitVal?: number;
  private _offsetVal?: number;
  private _includeDeleted = false;
  private _hops: string[] = [];
  private _gatherVal?: BuiltGather;
  private _unionVal?: BuiltRelation;
  _columnTransforms?: ColumnTransformMap;
  readonly _columnTransformsByTable?: ColumnTransformRegistry;

  constructor(
    table: TableNameFromMeta<TMeta>,
    schema: WasmSchema,
    columnTransforms?: ColumnTransformMap,
    columnTransformsByTable?: ColumnTransformRegistry,
  ) {
    this._table = table;
    this._schema = schema;
    this._columnTransforms = columnTransforms;
    this._columnTransformsByTable =
      columnTransformsByTable ?? (columnTransforms ? { [table]: columnTransforms } : undefined);
  }

  where(
    conditions: TableWhereFromMeta<TMeta> & DeletedWhere<TDeleted>,
  ): MetaQueryHandle<TMeta, TInclude, TSelection, TRequired, TDeleted> {
    if (this._unionVal) {
      throw new Error("union(...) currently only supports gather(...) in MVP.");
    }
    const clone = this._clone<TInclude, TSelection, TRequired, TDeleted>();
    clone._conditions.push(...this._whereConditions(conditions as Record<string, unknown>));
    return clone;
  }

  select<
    FirstSelection extends TableSelectionArg<TMeta, TDeleted>,
    RestSelection extends readonly TableSelectionArg<TMeta, TDeleted>[] = [],
  >(
    first: FirstSelection,
    ...rest: RestSelection
  ): MetaQueryHandle<TMeta, TInclude, FirstSelection | RestSelection[number], TRequired, TDeleted> {
    this._rejectUnsupportedUnionModifier("select(...)");
    const clone = this._clone<
      TInclude,
      FirstSelection | RestSelection[number],
      TRequired,
      TDeleted
    >();
    const { columns, names, partial } = splitSelection([first, ...rest]);
    const transforms = this._outputColumnTransforms();
    for (const column of Object.keys(partial))
      if (transforms?.[column]?.computed)
        throw new TypeError(`Computed field "${column}" cannot have a stored partial selection.`);
    const hasPartial = Object.keys(partial).length > 0;
    // A range-only selection keeps the object form; a mix lists every
    // selected column by name and carries the ranges in `partialSelect`.
    clone._select = hasPartial && names.length === 0 ? partial : columns;
    clone._partialSelect = hasPartial && names.length > 0 ? partial : undefined;
    return clone;
  }

  include<NewInclude extends BuilderInclude<TMeta>>(
    relations: NewInclude,
  ): MetaQueryHandle<TMeta, TInclude & NewInclude, TSelection, TRequired, TDeleted> {
    this._rejectUnsupportedUnionModifier("include(...)");
    const clone = this._clone<TInclude & NewInclude, TSelection, TRequired, TDeleted>();
    clone._includes = { ...this._includes, ...relations };
    return clone;
  }

  requireIncludes(): MetaQueryHandle<TMeta, TInclude, TSelection, true, TDeleted> {
    this._rejectUnsupportedUnionModifier("requireIncludes()");
    const clone = this._clone<TInclude, TSelection, true, TDeleted>();
    clone._requireIncludes = true;
    return clone;
  }

  orderBy(
    column: TableOrderableFromMeta<TMeta> | DeletedColumn<TDeleted>,
    direction: "asc" | "desc" = "asc",
  ): MetaQueryHandle<TMeta, TInclude, TSelection, TRequired, TDeleted> {
    if (this._outputColumnTransforms()?.[column as string]?.computed)
      throw new TypeError(
        `Computed field "${String(column)}" cannot be ordered in a stored query.`,
      );
    const clone = this._clone<TInclude, TSelection, TRequired, TDeleted>();
    clone._orderBys.push([column as string, direction]);
    return clone;
  }

  limit(n: number): MetaQueryHandle<TMeta, TInclude, TSelection, TRequired, TDeleted> {
    const clone = this._clone<TInclude, TSelection, TRequired, TDeleted>();
    clone._limitVal = n;
    return clone;
  }

  offset(n: number): MetaQueryHandle<TMeta, TInclude, TSelection, TRequired, TDeleted> {
    const clone = this._clone<TInclude, TSelection, TRequired, TDeleted>();
    clone._offsetVal = n;
    return clone;
  }

  includeDeleted(): MetaQueryHandle<TMeta, TInclude, TSelection, TRequired, true> {
    this._rejectUnsupportedUnionModifier("includeDeleted()");
    const clone = this._clone<TInclude, TSelection, TRequired, true>();
    clone._includeDeleted = true;
    return clone;
  }

  hopTo<TRelation extends RelationNameFromMeta<TMeta>>(
    relation: TRelation,
  ): MetaQueryHandle<
    RelationTargetFromMeta<TMeta, TRelation>,
    {},
    DefaultTableSelection<RelationTargetFromMeta<TMeta, TRelation>>,
    TRequired,
    TDeleted
  > {
    if (this._unionVal) {
      throw new Error("union(...) currently only supports gather(...) in MVP.");
    }
    const clone = this._clone<TInclude, TSelection, TRequired, TDeleted>();
    clone._hops.push(relation as string);
    // The type now represents the destination table while `_table` at runtime
    // is still the source — the destination is encoded in `_hops` and resolved
    // by the query adapter. Aligning runtime `_table` with the type is a
    // separate change (it touches `_build()` and hop lowering) and is out of
    // scope here; `_table` is internal and not part of the public surface.
    return clone as unknown as MetaQueryHandle<
      RelationTargetFromMeta<TMeta, TRelation>,
      {},
      DefaultTableSelection<RelationTargetFromMeta<TMeta, TRelation>>,
      TRequired,
      TDeleted
    >;
  }

  gather(options: {
    start?: TableWhereFromMeta<TMeta>;
    step: (ctx: { current: string }) => QueryBuilder<unknown>;
    /** Maximum recursive hops. Zero evaluates the seed only. */
    maxDepth?: number;
  }): MetaQueryHandle<TMeta, TInclude, TSelection, TRequired, TDeleted> {
    if (typeof options.step !== "function") {
      throw new Error("gather(...) requires step callback.");
    }

    const maxDepth = options.maxDepth ?? 10;
    if (!Number.isInteger(maxDepth) || maxDepth < 0) {
      throw new Error("gather(...) maxDepth must be a non-negative integer.");
    }
    if (Object.keys(this._includes).length > 0) {
      throw new Error("gather(...) does not support include(...) in MVP.");
    }
    if (options.start && this._unionVal) {
      throw new Error("gather(...) start does not support union(...) seeds in MVP.");
    }

    const currentToken = "__jazz_gather_current__";
    const stepOutput = options.step({ current: currentToken });
    if (
      !stepOutput ||
      typeof stepOutput !== "object" ||
      typeof (stepOutput as { _build?: unknown })._build !== "function"
    ) {
      throw new Error("gather(...) step must return a query expression built from app.<table>.");
    }

    const stepBuilt = JSON.parse(stepOutput._build()) as {
      table?: unknown;
      conditions?: Array<{ column: string; op: string; value: unknown }>;
      hops?: unknown;
    };

    if (typeof stepBuilt.table !== "string" || !stepBuilt.table) {
      throw new Error("gather(...) step query is missing table metadata.");
    }
    if (!Array.isArray(stepBuilt.conditions)) {
      throw new Error("gather(...) step query is missing condition metadata.");
    }

    const stepHops = Array.isArray(stepBuilt.hops)
      ? stepBuilt.hops.filter((hop): hop is string => typeof hop === "string")
      : [];
    if (stepHops.length !== 1) {
      throw new Error("gather(...) step must include exactly one hopTo(...).");
    }

    const currentConditions = stepBuilt.conditions.filter(
      (condition) => condition.op === "eq" && condition.value === currentToken,
    );
    if (currentConditions.length !== 1) {
      throw new Error(
        "gather(...) step must include exactly one where condition bound to current.",
      );
    }

    const currentCondition = currentConditions[0]!;
    const stepConditions = stepBuilt.conditions.filter(
      (condition) => !(condition.op === "eq" && condition.value === currentToken),
    );

    const seedSource = options.start === undefined ? this : this.where(options.start);
    // The seed's own filters travel in an explicit seed: the gather's
    // top-level conditions filter its output.
    const needsExplicitSeed =
      this._unionVal !== undefined ||
      this._hops.length > 0 ||
      this._gatherVal !== undefined ||
      seedSource._conditions.length > 0;
    const clone = needsExplicitSeed
      ? this._clone<TInclude, TSelection, TRequired, TDeleted>()
      : seedSource._clone<TInclude, TSelection, TRequired, TDeleted>();
    clone._conditions = [];
    clone._hops = [];
    const seed = needsExplicitSeed ? seedSource._serializeRelation() : undefined;
    if (seed) {
      // These modifiers remain on the gathered result. Keep only windows
      // nested inside the seed (such as individual UNION arms).
      delete seed.orderBy;
      delete seed.limit;
      delete seed.offset;
    }
    clone._gatherVal = {
      ...(seed ? { seed } : {}),
      max_depth: maxDepth,
      step_table: stepBuilt.table,
      step_current_column: currentCondition.column,
      step_conditions: stepConditions,
      step_hops: stepHops,
    };
    clone._unionVal = undefined;

    return clone;
  }

  _build(): string {
    return JSON.stringify(
      {
        table: this._table,
        conditions: this._conditions,
        includes: this._includes,
        __jazz_requireIncludes: this._requireIncludes || undefined,
        select: this._select,
        ...(this._partialSelect ? { partialSelect: this._partialSelect } : {}),
        orderBy: this._orderBys,
        limit: this._limitVal,
        offset: this._offsetVal,
        includeDeleted: this._includeDeleted || undefined,
        hops: this._hops,
        gather: this._gatherVal,
        ...(this._unionVal ? { union: cloneBuiltRelation(this._unionVal).union } : {}),
      },
      queryBuilderJsonReplacer,
    );
  }

  toJSON(): unknown {
    return JSON.parse(this._build());
  }

  private _clone<
    TNewInclude extends BuilderInclude<TMeta>,
    TNewSelection,
    TNewRequired extends boolean,
    TNewDeleted extends boolean = TDeleted,
  >(): TypedTableQueryBuilder<TMeta, TNewInclude, TNewSelection, TNewRequired, TNewDeleted> {
    const clone = new TypedTableQueryBuilder<
      TMeta,
      TNewInclude,
      TNewSelection,
      TNewRequired,
      TNewDeleted
    >(this._table, this._schema, this._columnTransforms, this._columnTransformsByTable);
    clone._conditions = [...this._conditions];
    clone._includes = { ...this._includes } as Partial<BuilderInclude<TMeta>>;
    clone._requireIncludes = this._requireIncludes;
    clone._select = Array.isArray(this._select)
      ? [...this._select]
      : this._select
        ? { ...this._select }
        : undefined;
    clone._orderBys = [...this._orderBys];
    clone._partialSelect = this._partialSelect ? { ...this._partialSelect } : undefined;
    clone._limitVal = this._limitVal;
    clone._offsetVal = this._offsetVal;
    clone._includeDeleted = this._includeDeleted;
    clone._hops = [...this._hops];
    clone._gatherVal = this._gatherVal ? cloneBuiltGather(this._gatherVal) : undefined;
    clone._unionVal = this._unionVal ? cloneBuiltRelation(this._unionVal) : undefined;
    return clone;
  }

  private _outputColumnTransforms(): ColumnTransformMap | undefined {
    if (!this._columnTransformsByTable) return this._columnTransforms;
    if (!this._hops.length && !this._gatherVal?.seed && !this._unionVal)
      return this._columnTransforms;
    return this._columnTransformsByTable[
      resolveQueryOutputTable(this._schema, this._serializeRelation())
    ];
  }

  private _whereConditions(conditions: Record<string, unknown>): BuiltCondition[] {
    const built: BuiltCondition[] = [];
    const tableSchema = resolveQueryTable(this._schema, this._table);
    const transforms = this._outputColumnTransforms();
    for (const [key, value] of Object.entries(conditions)) {
      if (transforms?.[key]?.computed)
        throw new TypeError(`Computed field "${key}" cannot be filtered in a stored query.`);
      if (value === undefined) continue;
      const declaredColumn = tableSchema?.columns.find((column) => column.name === key);
      const columnType: ColumnType | undefined =
        key === "id" ? { type: "Uuid" } : (magicColumnType(key) ?? declaredColumn?.column_type);
      const isObjectValue = typeof value === "object" && value !== null && !Array.isArray(value);
      const hasRecognizedOperator = isObjectValue
        ? Object.keys(value).some((op) => WHERE_OPERATORS.includes(op as WhereOperator))
        : false;
      const isDirectObjectValue =
        isObjectValue &&
        ((columnType?.type === "Json" && !hasRecognizedOperator) ||
          (columnType?.type === "Timestamp" && value instanceof Date) ||
          (columnType?.type === "Bytea" && value instanceof Uint8Array) ||
          (columnType?.type === "Vector" && value instanceof Float32Array) ||
          (columnType?.type === "Row" &&
            columnType.columns.some((column) => Object.hasOwn(value, column.name))));

      if (isObjectValue && !isDirectObjectValue) {
        for (const [op, opValue] of Object.entries(value)) {
          if (opValue !== undefined) {
            built.push({ column: key, op, value: opValue });
          }
        }
      } else {
        built.push({ column: key, op: "eq", value });
      }
    }
    return built;
  }

  private _rejectUnsupportedUnionModifier(modifier: string): void {
    if (this._unionVal) {
      throw new Error(
        `union(...) currently supports only orderBy(...), offset(...), limit(...), and gather(...); ${modifier} is not supported.`,
      );
    }
  }

  _serializeRelation(): BuiltRelation {
    const output = {
      ...(this._orderBys.length
        ? {
            orderBy: this._orderBys.map(
              ([key, direction]) => [key, direction] as [string, "asc" | "desc"],
            ),
          }
        : {}),
      ...(this._limitVal !== undefined ? { limit: this._limitVal } : {}),
      ...(this._offsetVal !== undefined ? { offset: this._offsetVal } : {}),
    };
    if (this._unionVal) {
      return { ...cloneBuiltRelation(this._unionVal), ...output };
    }
    return {
      table: this._table,
      conditions: this._conditions.map(cloneBuiltCondition),
      hops: [...this._hops],
      ...(this._gatherVal ? { gather: cloneBuiltGather(this._gatherVal) } : {}),
      ...output,
    };
  }
}

interface MetaQueryHandle<
  TMeta extends AnyTableMeta,
  TInclude extends BuilderInclude<TMeta> = {},
  TSelection = DefaultTableSelection<TMeta>,
  TRequired extends boolean = false,
  TDeleted extends boolean = false,
> extends TypedTableQueryBuilder<TMeta, TInclude, TSelection, TRequired, TDeleted> {}

export interface Query<
  TTable extends string,
  TInclude extends BuilderInclude<SchemaMeta<TTable, TSchema>> = {},
  TSelection = any,
  TSchema extends SchemaLike = SchemaLike,
  TRequired extends boolean = false,
  TDeleted extends boolean = false,
> extends TypedTableQueryBuilder<
  SchemaMeta<TTable, TSchema>,
  TInclude,
  TSelection,
  TRequired,
  TDeleted
> {
  /** @internal Phantom used by `Db.update` to retain column-specific diff shapes. */
  readonly _largeValueUpdateType: TableLargeValueUpdate<
    TSchema,
    Extract<TTable, TableName<TSchema>>
  >;
  where(
    conditions: TableWhereInput<TSchema, Extract<TTable, TableName<TSchema>>> &
      DeletedWhere<TDeleted>,
  ): Query<TTable, TInclude, TSelection, TSchema, TRequired, TDeleted>;
  select<
    FirstSelection extends TableSelectionArg<SchemaMeta<TTable, TSchema>, TDeleted>,
    RestSelection extends readonly TableSelectionArg<SchemaMeta<TTable, TSchema>, TDeleted>[] = [],
  >(
    first: FirstSelection,
    ...rest: RestSelection
  ): Query<TTable, TInclude, FirstSelection | RestSelection[number], TSchema, TRequired, TDeleted>;
  include<NewInclude extends BuilderInclude<SchemaMeta<TTable, TSchema>>>(
    relations: NewInclude,
  ): Query<TTable, TInclude & NewInclude, TSelection, TSchema, TRequired, TDeleted>;
  requireIncludes(): Query<TTable, TInclude, TSelection, TSchema, true, TDeleted>;
  orderBy(
    column: TableOrderableFromMeta<SchemaMeta<TTable, TSchema>> | DeletedColumn<TDeleted>,
    direction?: "asc" | "desc",
  ): Query<TTable, TInclude, TSelection, TSchema, TRequired, TDeleted>;
  limit(n: number): Query<TTable, TInclude, TSelection, TSchema, TRequired, TDeleted>;
  offset(n: number): Query<TTable, TInclude, TSelection, TSchema, TRequired, TDeleted>;
  includeDeleted(): Query<TTable, TInclude, TSelection, TSchema, TRequired, true>;
  hopTo<TRelation extends RelationNameFromMeta<SchemaMeta<TTable, TSchema>>>(
    relation: TRelation,
  ): Query<
    RelationTargetFromMeta<SchemaMeta<TTable, TSchema>, TRelation>["name"],
    {},
    DefaultTableSelection<RelationTargetFromMeta<SchemaMeta<TTable, TSchema>, TRelation>>,
    TSchema,
    TRequired,
    TDeleted
  >;
  gather(options: {
    start?: TableWhereInput<TSchema, Extract<TTable, TableName<TSchema>>>;
    step: (ctx: { current: string }) => QueryBuilder<unknown>;
    /** Maximum recursive hops. Zero evaluates the seed only. */
    maxDepth?: number;
  }): Query<TTable, TInclude, TSelection, TSchema, TRequired, TDeleted>;
}

export type HistoryRow<TRow extends { id: string }> = TRow & {
  $transactionId: TxId;
  $acceptedAt: Date;
};

/**
 * Query previous values of a table's rows.
 * Only includes authority-confirmed versions.
 * Bound the query with an exact row ID or an explicit `id.in` list;
 * joins and transaction-ID predicates are not supported.
 */
export interface HistoryQuery<
  TRow extends { id: string },
  TWhere,
  TSelection extends
    | keyof HistoryRow<TRow>
    | ProvenanceMagicColumn
    | keyof DeletionMagicColumns
    | "*" = keyof HistoryRow<TRow>,
  TDeleted extends boolean = false,
> extends QueryBuilder<SelectedRow<HistoryRow<TRow>, TSelection>> {
  where(
    input: TWhere & { $acceptedAt?: TimestampWhere<false> } & DeletedWhere<TDeleted>,
  ): HistoryQuery<TRow, TWhere, TSelection, TDeleted>;
  select<
    TColumn extends keyof HistoryRow<TRow> | ProvenanceMagicColumn | DeletedColumn<TDeleted> | "*",
  >(
    ...columns: [TColumn, ...TColumn[]]
  ): HistoryQuery<TRow, TWhere, TColumn, TDeleted>;
  orderBy(
    column: keyof TRow | "$acceptedAt" | ProvenanceMagicColumn | DeletedColumn<TDeleted>,
    direction?: "asc" | "desc",
  ): HistoryQuery<TRow, TWhere, TSelection, TDeleted>;
  limit(count: number): HistoryQuery<TRow, TWhere, TSelection, TDeleted>;
  offset(count: number): HistoryQuery<TRow, TWhere, TSelection, TDeleted>;
  includeDeleted(): HistoryQuery<TRow, TWhere, TSelection, true>;
}

export interface Table<TTable extends string, TSchema extends SchemaLike> extends Query<
  TTable,
  {},
  DefaultTableSelection<SchemaMeta<TTable, TSchema>>,
  TSchema
> {}

export type QueryHandle<
  TTable extends string,
  TSchema extends SchemaLike,
  TInclude extends BuilderInclude<SchemaMeta<TTable, TSchema>> = {},
  TSelection = DefaultTableSelection<SchemaMeta<TTable, TSchema>>,
> = Query<TTable, TInclude, TSelection, TSchema>;

export type TableHandle<TTable extends string, TSchema extends SchemaLike> = Table<TTable, TSchema>;

export type App<TSchema extends SchemaLike> = Simplify<
  {
    [TTable in TableName<TSchema>]: Table<TTable, TSchema>;
  } & {
    /** Read-only queries over accepted content versions of each table. */
    readonly $history: {
      readonly [TTable in TableName<TSchema>]: HistoryQuery<
        TableRowFromMeta<SchemaMeta<TTable, TSchema>>,
        TableWhereFromMeta<SchemaMeta<TTable, TSchema>>
      >;
    };
    union<TTable extends TableName<TSchema>>(
      relations: readonly RelationSeedQuery<TTable>[],
    ): TypedTableQueryBuilder<
      SchemaMeta<TTable, TSchema>,
      {},
      DefaultTableSelection<SchemaMeta<TTable, TSchema>>,
      false
    >;
    union<TTable extends TableName<TSchema>>(
      relations: Readonly<Record<string, RelationSeedQuery<TTable>>>,
    ): TypedTableQueryBuilder<
      SchemaMeta<TTable, TSchema>,
      {},
      DefaultTableSelection<SchemaMeta<TTable, TSchema>>,
      false
    >;
    wasmSchema: WasmSchema;
  }
>;

export type TypedApp<TSchema extends SchemaLike> = App<TSchema>;

export type RowOf<TTable> = TTable extends { readonly _rowType: infer TRow } ? TRow : never;
export type InsertOf<TTable> = TTable extends { readonly _initType: infer TInit } ? TInit : never;
export type LargeValueUpdateOf<TTable> = TTable extends {
  readonly _largeValueUpdateType: infer TUpdate;
}
  ? TUpdate
  : never;
export type StreamingInsertOf<TTable> = TTable extends {
  readonly _streamingInitType: infer TStreamingInit;
}
  ? TStreamingInit
  : never;
export type StreamingUpdateOf<TTable> = TTable extends {
  readonly _streamingUpdateType: infer TStreamingUpdate;
}
  ? TStreamingUpdate
  : never;
export type StreamingUpsertOf<TTable> = StreamingUpdateOf<TTable>;
export type TableMetaOf<TTable> =
  TTable extends Table<infer TTableName, infer TSchema>
    ? SchemaMeta<Extract<TTableName, string>, Extract<TSchema, SchemaLike>>
    : TTable extends Query<infer TTableName, any, any, infer TSchema, any>
      ? SchemaMeta<Extract<TTableName, string>, Extract<TSchema, SchemaLike>>
      : TTable extends TypedTableQueryBuilder<infer TMeta, any, any, any>
        ? TMeta
        : never;
export type WhereOf<TQuery> = TQuery extends { where(input: infer TWhere): unknown }
  ? TWhere
  : never;
import { shipMigrations, type ClientMigrations } from "./runtime/client-migrations.js";

// ============================================================================
// bindApp: the generated module's entry point
// ============================================================================

/**
 * The compiled schema `jazz-tools generate` embeds in `jazz.generated.ts`:
 * what Jazz's Rust loader makes of `jazz.app.json`, so the runtime never
 * reads the file itself.
 */
export interface CompiledAppSchema {
  /** The current schema's hash. */
  readonly current: string;
  /** The current schema, relations included. */
  readonly tables?: WasmSchema;
  /** Every version the app ships, oldest first, the current one last. */
  readonly schemas: ClientMigrations["schemas"];
  readonly migrations: ClientMigrations["migrations"];
  /** Computed fields: table → field → the columns it is computed from. */
  readonly computed?: Readonly<Record<string, Readonly<Record<string, readonly string[]>>>>;
  /** Transformed columns: table → column → transform name. */
  readonly transforms?: Readonly<Record<string, Readonly<Record<string, string>>>>;
}

/** Relations as the generator may emit them beside `schema`. */
type CompiledRelations = Readonly<
  Record<
    string,
    Readonly<
      Record<
        string,
        | { readonly one: string; readonly column: string }
        | { readonly many: string; readonly relation?: string; readonly column?: string }
      >
    >
  >
>;

type TablesWithComputed<TTypes extends SchemaLike> = {
  [TTable in TableName<TTypes>]: TableTypesOf<TTypes, TTable> extends {
    readonly computed: infer C;
  }
    ? keyof C extends never
      ? never
      : TTable
    : never;
}[TableName<TTypes>];

/**
 * The computed fields an app implements (in `jazz.computed.ts`): per table,
 * one function per field from the columns `jazz.app.json` names to the value.
 */
export type ComputedImpl<TTypes extends SchemaLike> = {
  readonly [TTable in TablesWithComputed<TTypes>]: TableTypesOf<TTypes, TTable> extends {
    readonly computed: infer C extends Record<string, { from: object; value: unknown }>;
  }
    ? { readonly [TField in keyof C]: (row: Readonly<C[TField]["from"]>) => C[TField]["value"] }
    : never;
};

/**
 * The transforms an app implements (in `jazz.computed.ts`): per name, `from`
 * turns a stored value into the value the app sees, `to` turns it back.
 *
 * Null passes through: Jazz never calls a transform with null, so a null
 * branch is never reached. Either may still return null, so an alpha.64
 * transform on an optional column, which handled null itself and which
 * `jazz-tools upgrade` moves here unchanged, keeps typechecking.
 */
/**
 * Whether every column stored through the transform `TName` is nullable: only
 * then may its callbacks return `null` (an alpha.64 transform on an optional
 * column that handled null itself). Jazz never calls a transform with null.
 */
type TransformColumnsNullable<TTypes extends SchemaLike, TName> = false extends {
  [TTable in TableName<TTypes>]: TableTypesOf<TTypes, TTable> extends {
    readonly transforms: infer TColumns extends Record<string, string>;
  }
    ? {
        [TColumn in keyof TColumns]: TColumns[TColumn] extends TName
          ? TColumn extends StoredColumnName<TTypes, TTable>
            ? ColumnOf<TTypes, TTable, TColumn>["nullable"]
            : never
          : never;
      }[keyof TColumns]
    : never;
}[TableName<TTypes>]
  ? false
  : true;

/**
 * The transforms an app implements (in `jazz.computed.ts`): per name, `from`
 * turns a stored value into the value the app sees, `to` turns it back.
 * Jazz passes `null` through without calling either. With the app's types
 * (`TTypes`), a callback may return `null` only when every column stored
 * through it is nullable; without them, `null` is allowed and `bindApp`, which
 * has the types, checks it.
 */
export type TransformImpl<TTransforms extends TransformTypes, TTypes extends SchemaLike = never> = {
  readonly [TName in keyof TTransforms]: TTransforms[TName] extends {
    readonly stored: infer TStored;
    readonly view: infer TView;
  }
    ? [TTypes] extends [never]
      ? { from(stored: TStored): TView | null; to(view: TView): TStored | null }
      : TransformColumnsNullable<TTypes, TName> extends true
        ? { from(stored: TStored): TView | null; to(view: TView): TStored | null }
        : { from(stored: TStored): TView; to(view: TView): TStored }
    : never;
};

type ComputedOption<TTypes extends SchemaLike> = [TablesWithComputed<TTypes>] extends [never]
  ? { readonly computed?: undefined }
  : { readonly computed: ComputedImpl<TTypes> };
type TransformsOption<TTypes extends SchemaLike, TTransforms extends TransformTypes> = [
  keyof TTransforms,
] extends [never]
  ? { readonly transforms?: undefined }
  : { readonly transforms: TransformImpl<TTransforms, TTypes> };

/** What `jazz.generated.ts` passes to {@link bindApp}. */
export type BindAppOptions<TTypes extends SchemaLike, TTransforms extends TransformTypes = {}> = {
  /** `"sha256:<hex>"` of the `jazz.app.json` the bindings were generated from. */
  readonly source: string;
  readonly schema: CompiledAppSchema;
  readonly relations?: CompiledRelations;
} & ComputedOption<TTypes> &
  TransformsOption<TTypes, TTransforms>;

/**
 * Where `bindApp` records what the bindings passed it on the app's schema,
 * read across copies of jazz-tools (a bundler may load the generated module
 * and the backend's as separate copies). Not enumerable: the schema's JSON,
 * and so its hash, stay as they are.
 */
const BOUND = Symbol.for("jazz-tools.boundApp");

/** What the bindings that made an app's schema passed to `bindApp`. */
export interface BoundApp {
  /** `"sha256:<hex>"` of the `jazz.app.json` they were generated from. */
  readonly source: string;
  readonly schema: CompiledAppSchema;
  readonly relations?: CompiledRelations;
}

/** What the bindings that made `wasmSchema` passed to `bindApp`, if `bindApp` made it. */
export function boundApp(wasmSchema: WasmSchema): BoundApp | undefined {
  return (wasmSchema as { [BOUND]?: BoundApp })[BOUND];
}

const REGENERATE = "run `npx jazz-tools generate` to regenerate jazz.generated.ts";

/**
 * Bind an app's generated types to its compiled schema and the computed
 * fields and transforms it implements. Called by `jazz.generated.ts`, which
 * `jazz-tools generate` writes from `jazz.app.json`; apps import `app` from
 * there instead of calling this.
 */
export function bindApp<TTypes extends SchemaLike, TTransforms extends TransformTypes = {}>(
  options: BindAppOptions<TTypes, TTransforms>,
): App<TTypes> {
  if (!options || typeof options !== "object" || !options.schema) {
    throw new TypeError(`bindApp: expected the options jazz.generated.ts passes; ${REGENERATE}.`);
  }
  const { source, schema } = options;
  if (typeof source !== "string" || !/^sha256:[0-9a-f]{64}$/.test(source)) {
    throw new TypeError(`bindApp: \`source\` must be "sha256:<hex>"; ${REGENERATE}.`);
  }
  const wasmSchema = currentTables(schema, options.relations);
  shipMigrations(wasmSchema, { schemas: schema.schemas, migrations: schema.migrations });
  const bound: BoundApp = Object.freeze({ source, schema, relations: options.relations });
  Object.defineProperty(wasmSchema, BOUND, { value: bound, enumerable: false });
  const transforms = columnTransforms(
    wasmSchema,
    schema,
    options.computed as Record<string, Record<string, (row: unknown) => unknown>> | undefined,
    options.transforms as
      | Record<string, { from(value: unknown): unknown; to(value: unknown): unknown }>
      | undefined,
  );
  return createAppForTables(
    Object.keys(wasmSchema),
    wasmSchema,
    transforms,
  ) as unknown as App<TTypes>;
}

/** The current schema, with its relations, checked against the versions the bindings ship. */
function currentTables(
  schema: CompiledAppSchema,
  relations: CompiledRelations | undefined,
): WasmSchema {
  if (typeof schema.current !== "string" || !Array.isArray(schema.schemas)) {
    throw new TypeError(`bindApp: the compiled schema is malformed; ${REGENERATE}.`);
  }
  const current = schema.schemas.find((version) => version.hash === schema.current);
  if (!current) {
    throw new Error(
      `bindApp: jazz.generated.ts ships no schema for its current version ${schema.current}; ${REGENERATE}.`,
    );
  }
  // A version's schema is the engine's serde form, `{ tables }`.
  const engine = current.schema as { tables?: WasmSchema };
  const tables: WasmSchema = structuredClone(
    schema.tables ?? engine.tables ?? (engine as WasmSchema),
  );
  for (const [table, entries] of Object.entries(relations ?? {})) {
    const tableSchema = tables[table];
    if (!tableSchema) {
      throw new Error(`bindApp: relations name unknown table "${table}"; ${REGENERATE}.`);
    }
    const declared: NonNullable<WasmSchema[string]["relations"]> = {};
    for (const [name, relation] of Object.entries(entries)) {
      if ("one" in relation) {
        declared[name] = { kind: "forward", table: relation.one, column: relation.column };
      } else if (relation.relation !== undefined) {
        declared[name] = { kind: "reverse", table: relation.many, relation: relation.relation };
      } else if (relation.column !== undefined) {
        declared[name] = { kind: "forward", table: relation.many, column: relation.column };
      } else {
        throw new Error(`bindApp: relation ${table}.${name} is malformed; ${REGENERATE}.`);
      }
    }
    // Stored schemas carry no relations; only a table with some gets the key.
    if (Object.keys(declared).length > 0) tableSchema.relations = declared;
  }
  for (const table of Object.keys(tables)) assertSchemaNameAllowed(table);
  return tables;
}

/** The runtime transforms for the computed fields and transformed columns the schema declares. */
function columnTransforms(
  wasmSchema: WasmSchema,
  schema: CompiledAppSchema,
  computed: Record<string, Record<string, (row: unknown) => unknown>> | undefined,
  transforms:
    | Record<string, { from(value: unknown): unknown; to(value: unknown): unknown }>
    | undefined,
): ColumnTransformRegistry | undefined {
  const registry: ColumnTransformRegistry = Object.create(null);
  const tableTransforms = (table: string): ColumnTransformMap =>
    (registry[table] ??= Object.create(null) as ColumnTransformMap);

  const declaredComputed = schema.computed ?? {};
  for (const [table, fields] of Object.entries(declaredComputed)) {
    if (!wasmSchema[table]) {
      throw new Error(`bindApp: computed fields name unknown table "${table}"; ${REGENERATE}.`);
    }
    for (const [field, from] of Object.entries(fields)) {
      const compute = computed?.[table]?.[field];
      if (typeof compute !== "function") {
        throw new Error(
          `jazz.app.json declares the computed field ${table}.${field}, but jazz.computed.ts doesn't implement it: export it from \`computed.${table}.${field}\`.`,
        );
      }
      tableTransforms(table)[field] = {
        computed: true,
        rowColumns: Object.freeze([...from]),
        from: (_value, row) => compute(row),
        to: () => {
          throw new TypeError(`Computed field "${field}" is read-only.`);
        },
      };
    }
  }
  for (const [table, fields] of Object.entries(computed ?? {})) {
    for (const field of Object.keys(fields ?? {})) {
      if (!declaredComputed[table]?.[field]) {
        throw new Error(
          `jazz.computed.ts implements the computed field ${table}.${field}, which jazz.app.json doesn't declare: declare it under tables.${table}.computed, or remove it.`,
        );
      }
    }
  }

  const used = new Set<string>();
  for (const [table, columns] of Object.entries(schema.transforms ?? {})) {
    if (!wasmSchema[table]) {
      throw new Error(`bindApp: transformed columns name unknown table "${table}"; ${REGENERATE}.`);
    }
    for (const [column, name] of Object.entries(columns)) {
      const transform = transforms?.[name];
      if (
        !transform ||
        typeof transform.from !== "function" ||
        typeof transform.to !== "function"
      ) {
        throw new Error(
          `jazz.app.json stores ${table}.${column} through the transform "${name}", but jazz.computed.ts doesn't implement it: export \`transforms.${name}\` with \`from\` and \`to\`.`,
        );
      }
      used.add(name);
      // Null passes through uncalled. A callback that returns null for a
      // required column is refused, as the types refuse it.
      const nullable =
        wasmSchema[table]!.columns.find((descriptor) => descriptor.name === column)?.nullable ??
        true;
      const checked = (direction: "from" | "to", result: unknown): unknown => {
        if (!nullable && (result === null || result === undefined)) {
          throw new TypeError(
            `The transform "${name}" returned ${String(result)} from \`${direction}\` for ${table}.${column}, which is required: return a value, or make the column nullable in jazz.app.json.`,
          );
        }
        return result;
      };
      tableTransforms(table)[column] = {
        from: (value) =>
          value === null || value === undefined ? value : checked("from", transform.from(value)),
        to: (value) =>
          value === null || value === undefined ? value : checked("to", transform.to(value)),
      };
    }
  }
  for (const name of Object.keys(transforms ?? {})) {
    if (!used.has(name)) {
      throw new Error(
        `jazz.computed.ts implements the transform "${name}", which no column of jazz.app.json uses: declare it under "transforms" and name it on a column, or remove it.`,
      );
    }
  }
  return Object.keys(registry).length ? registry : undefined;
}

// The statically-registered app schema: the inspector host handle falls back to
// it before any client exists (e.g. a write-only page with no query yet). Set
// at bindApp time, so it does not depend on a connection being established.
// Last-bindApp-wins by design: a dev-only fallback, not a multi-app registry.
let registeredWasmSchema: WasmSchema | undefined;

/** The most recently bound app's WasmSchema, if any (used by the inspector). */
export function getRegisteredWasmSchema(): WasmSchema | undefined {
  return registeredWasmSchema;
}

function createAppForTables(
  tableNames: readonly string[],
  wasmSchema: WasmSchema,
  columnTransformsByTable: ColumnTransformRegistry | undefined,
): App<SchemaLike> {
  registeredWasmSchema = wasmSchema;
  const tables = {} as Record<string, TypedTableQueryBuilder<any>>;
  const history = {} as Record<string, unknown>;

  for (const tableName of tableNames) {
    assertSchemaNameAllowed(tableName);
    tables[tableName] = new TypedTableQueryBuilder(
      tableName,
      wasmSchema,
      columnTransformsByTable?.[tableName],
      columnTransformsByTable,
    );
    Object.defineProperty(history, tableName, {
      enumerable: true,
      get: () =>
        new TypedTableQueryBuilder(
          historyTableName(tableName),
          wasmSchema,
          columnTransformsByTable?.[tableName],
          columnTransformsByTable,
        ),
    });
  }

  return {
    ...tables,
    $history: history,
    union<TTable extends string>(
      relations:
        | readonly RelationSeedQuery<TTable>[]
        | Readonly<Record<string, RelationSeedQuery<TTable>>>,
    ) {
      const arms = Array.isArray(relations)
        ? relations.map((relation) => ({
            relation,
            label: derivedUnionArmLabel(relation._serializeRelation()),
          }))
        : Object.entries(relations).map(([label, relation]) => ({ label, relation }));
      if (arms.length === 0) {
        throw new Error("union(...) requires at least one relation.");
      }
      const labels = new Set<string>();
      for (const { label } of arms) {
        if (!label || utf8ByteLength(label) > 4096 || label.includes("\0") || labels.has(label)) {
          throw new Error(
            "union(...) arm labels must be 1..=4096 bytes, NUL-free, and unique; use named arms for duplicate relations.",
          );
        }
        labels.add(label);
      }
      const first = arms[0]!.relation;
      const builder = new TypedTableQueryBuilder(
        first._table,
        wasmSchema,
        first._columnTransforms,
        first._columnTransformsByTable,
      );
      (builder as any)._unionVal = {
        union: {
          inputs: arms.map(({ label, relation }) => ({
            label,
            input: relation._serializeRelation() as BuiltRelation,
          })),
        },
      };
      return builder;
    },
    wasmSchema,
  } as unknown as App<SchemaLike>;
}

const utf8 = new TextEncoder();

function utf8ByteLength(value: string): number {
  return utf8.encode(value).byteLength;
}

function compareUtf8(left: string, right: string): number {
  const leftBytes = utf8.encode(left);
  const rightBytes = utf8.encode(right);
  const sharedLength = Math.min(leftBytes.length, rightBytes.length);
  for (let index = 0; index < sharedLength; index += 1) {
    const difference = leftBytes[index]! - rightBytes[index]!;
    if (difference !== 0) return difference;
  }
  return leftBytes.length - rightBytes.length;
}

function derivedUnionArmLabel(relation: BuiltRelation): string {
  // Labels are public durable input and therefore have a fixed small bound.
  // Hash the typed canonical relation instead of embedding JSON: Date, bigint
  // and byte values have non-JSON semantics, and a large literal must not turn
  // a valid array union into an overlong inferred label.
  return `derived:${bytesToHex(sha256(utf8.encode(canonicalUnionRelation(relation))))}`;
}

function canonicalUnionRelation(relation: BuiltRelation): string {
  // jazz.sdk.union-arm-output.v1 extends only seeds with output modifiers.
  // Existing unmodified seeds retain their exact canonical bytes and labels.
  const output = {
    ...(relation.orderBy ? { orderBy: relation.orderBy } : {}),
    ...(relation.limit !== undefined ? { limit: relation.limit } : {}),
    ...(relation.offset !== undefined ? { offset: relation.offset } : {}),
  };
  const normalized = {
    ...(relation.table ? { table: relation.table } : {}),
    conditions: [...(relation.conditions ?? [])]
      .map((condition) => canonicalJson(condition))
      .sort(),
    hops: relation.hops ?? [],
    ...(Object.keys(output).length ? { output_v1: output } : {}),
    ...(relation.gather ? { gather: relation.gather } : {}),
    ...(relation.union
      ? {
          union: relation.union.inputs
            .map((arm) => ({ label: arm.label, input: canonicalUnionRelation(arm.input) }))
            .sort((left, right) => compareUtf8(left.label, right.label)),
        }
      : {}),
  };
  return canonicalJson(normalized);
}

function canonicalJson(value: unknown): string {
  if (value === null) return "null";
  if (value === undefined) return "undefined";
  if (typeof value === "string") return `string:${JSON.stringify(value)}`;
  if (typeof value === "boolean") return `bool:${value}`;
  if (typeof value === "bigint") return `bigint:${value}`;
  if (typeof value === "number") {
    return Number.isFinite(value) ? `number:${value}` : `number:${String(value)}`;
  }
  if (value instanceof Date) {
    return Number.isNaN(value.getTime()) ? "date:invalid" : `date:${value.toISOString()}`;
  }
  if (value instanceof Uint8Array) return `bytes:${bytesToHex(value)}`;
  if (value instanceof ArrayBuffer) return `bytes:${bytesToHex(new Uint8Array(value))}`;
  if (ArrayBuffer.isView(value)) {
    return `typed:${value.constructor.name}:${bytesToHex(new Uint8Array(value.buffer, value.byteOffset, value.byteLength))}`;
  }
  if (Array.isArray(value)) return `[${value.map(canonicalJson).join(",")}]`;
  if (value && typeof value === "object") {
    const record = value as Record<string, unknown>;
    return `{${Object.keys(record)
      .sort()
      .map((key) => `${JSON.stringify(key)}:${canonicalJson(record[key])}`)
      .join(",")}}`;
  }
  return `unknown:${String(value)}`;
}
