import type {
  AnyTypedColumnBuilder,
  ColumnBuilderOptional,
  ColumnBuilderReferences,
  ColumnBuilderSqlType,
} from "./dsl.js";
import { schemaToWasm } from "./codegen/schema-reader.js";
import type { WasmSchema } from "./drivers/types.js";
import {
  PERMISSION_INTROSPECTION_COLUMNS,
  type PermissionIntrospectionColumn,
  assertUserColumnNameAllowed,
} from "./magic-columns.js";
import type { QueryBuilder } from "./runtime/db.js";
import type { Column, Schema, SqlType, TSTypeFromSqlType } from "./schema.js";

export type TableDefinition = Record<string, AnyTypedColumnBuilder>;
export type SchemaDefinition = Record<string, TableDefinition>;
export type Simplify<T> = { [K in keyof T]: T[K] } & {};
export type CompactSchema<TSchema extends SchemaDefinition> = Simplify<{
  [TTable in keyof TSchema]: Simplify<TSchema[TTable]>;
}>;

declare const definedSchemaBrand: unique symbol;
export interface DefinedSchema<TSchema extends SchemaDefinition = SchemaDefinition> {
  readonly [definedSchemaBrand]: CompactSchema<TSchema>;
}

type SchemaLike = SchemaDefinition | DefinedSchema<any>;
type NormalizedSchema<TSchema extends SchemaLike> =
  TSchema extends DefinedSchema<infer TDefinition>
    ? CompactSchema<TDefinition>
    : TSchema extends SchemaDefinition
      ? CompactSchema<TSchema>
      : never;

type TableName<TSchema extends SchemaLike> = Extract<keyof NormalizedSchema<TSchema>, string>;
type ColumnName<TSchema extends SchemaLike, TTable extends TableName<TSchema>> = Extract<
  keyof NormalizedSchema<TSchema>[TTable],
  string
>;

type BuilderForColumn<
  TSchema extends SchemaLike,
  TTable extends TableName<TSchema>,
  TColumn extends ColumnName<TSchema, TTable>,
> = NormalizedSchema<TSchema>[TTable][TColumn];

type ColumnValue<TBuilder extends AnyTypedColumnBuilder> = TSTypeFromSqlType<
  ColumnBuilderSqlType<TBuilder>
>;

type OptionalColumnName<TSchema extends SchemaLike, TTable extends TableName<TSchema>> = {
  [TColumn in ColumnName<TSchema, TTable>]-?: ColumnBuilderOptional<
    BuilderForColumn<TSchema, TTable, TColumn>
  > extends true
    ? TColumn
    : never;
}[ColumnName<TSchema, TTable>];

type RequiredColumnName<TSchema extends SchemaLike, TTable extends TableName<TSchema>> = Exclude<
  ColumnName<TSchema, TTable>,
  OptionalColumnName<TSchema, TTable>
>;

export type TableRow<TSchema extends SchemaLike, TTable extends TableName<TSchema>> = Simplify<
  {
    id: string;
  } & {
    [TColumn in RequiredColumnName<TSchema, TTable>]: ColumnValue<
      BuilderForColumn<TSchema, TTable, TColumn>
    >;
  } & {
    [TColumn in OptionalColumnName<TSchema, TTable>]?: ColumnValue<
      BuilderForColumn<TSchema, TTable, TColumn>
    >;
  }
>;

export type TableInit<TSchema extends SchemaLike, TTable extends TableName<TSchema>> = Simplify<
  {
    [TColumn in RequiredColumnName<TSchema, TTable>]: ColumnValue<
      BuilderForColumn<TSchema, TTable, TColumn>
    >;
  } & {
    [TColumn in OptionalColumnName<TSchema, TTable>]?: ColumnValue<
      BuilderForColumn<TSchema, TTable, TColumn>
    >;
  }
>;

type PrimitiveWhere<T> = T | { eq?: T; ne?: T };
type NumberWhere<T extends number> = T | { eq?: T; ne?: T; gt?: T; gte?: T; lt?: T; lte?: T };
type TimestampWhere =
  | Date
  | number
  | {
      eq?: Date | number;
      gt?: Date | number;
      gte?: Date | number;
      lt?: Date | number;
      lte?: Date | number;
    };
type UuidWhere<TOptional extends boolean, TRef extends string | undefined> = TRef extends string
  ? TOptional extends true
    ? string | { eq?: string; ne?: string; isNull?: boolean }
    : string | { eq?: string; ne?: string }
  : string | { eq?: string; ne?: string; in?: string[] };

type WhereInputForBuilder<TBuilder extends AnyTypedColumnBuilder> =
  ColumnBuilderSqlType<TBuilder> extends "TEXT"
    ? string | { eq?: string; ne?: string; contains?: string }
    : ColumnBuilderSqlType<TBuilder> extends "BOOLEAN"
      ? boolean
      : ColumnBuilderSqlType<TBuilder> extends "INTEGER" | "REAL"
        ? NumberWhere<number>
        : ColumnBuilderSqlType<TBuilder> extends "TIMESTAMP"
          ? TimestampWhere
          : ColumnBuilderSqlType<TBuilder> extends "UUID"
            ? UuidWhere<ColumnBuilderOptional<TBuilder>, ColumnBuilderReferences<TBuilder>>
            : ColumnBuilderSqlType<TBuilder> extends "BYTEA"
              ? PrimitiveWhere<Uint8Array>
              : ColumnBuilderSqlType<TBuilder> extends { kind: "JSON" }
                ?
                    | ColumnValue<TBuilder>
                    | {
                        eq?: ColumnValue<TBuilder>;
                        ne?: ColumnValue<TBuilder>;
                        in?: ColumnValue<TBuilder>[];
                      }
                : ColumnBuilderSqlType<TBuilder> extends {
                      kind: "ENUM";
                      variants: readonly (infer TVariant extends string)[];
                    }
                  ? TVariant | { eq?: TVariant; ne?: TVariant; in?: TVariant[] }
                  : ColumnBuilderSqlType<TBuilder> extends {
                        kind: "ARRAY";
                        element: infer TElementSql extends SqlType;
                      }
                    ?
                        | ColumnValue<TBuilder>
                        | {
                            eq?: ColumnValue<TBuilder>;
                            contains?: TSTypeFromSqlType<TElementSql>;
                          }
                    : never;

export type TableWhereInput<
  TSchema extends SchemaLike,
  TTable extends TableName<TSchema>,
> = Simplify<
  {
    id?: string | { eq?: string; ne?: string; in?: string[] };
  } & {
    [TColumn in ColumnName<TSchema, TTable>]?: WhereInputForBuilder<
      BuilderForColumn<TSchema, TTable, TColumn>
    >;
  } & {
    [TColumn in PermissionIntrospectionColumn]?: boolean;
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

type ForwardRelationName<TColumn extends string> = TColumn extends `${infer TPrefix}_id`
  ? TPrefix
  : TColumn;

type IsArrayRelation<TBuilder extends AnyTypedColumnBuilder> =
  ColumnBuilderSqlType<TBuilder> extends {
    kind: "ARRAY";
    element: "UUID";
  }
    ? true
    : false;

type ForwardRelationEntry<
  TSchema extends SchemaLike,
  TTable extends TableName<TSchema>,
  TColumn extends ColumnName<TSchema, TTable>,
> =
  ColumnBuilderReferences<BuilderForColumn<TSchema, TTable, TColumn>> extends infer TRef
    ? TRef extends TableName<TSchema>
      ? {
          name: ForwardRelationName<TColumn>;
          toTable: TRef;
          isArray: IsArrayRelation<BuilderForColumn<TSchema, TTable, TColumn>>;
          nullable: ColumnBuilderOptional<BuilderForColumn<TSchema, TTable, TColumn>>;
        }
      : never
    : never;

type ForwardRelationEntries<TSchema extends SchemaLike, TTable extends TableName<TSchema>> = {
  [TColumn in ColumnName<TSchema, TTable>]: ForwardRelationEntry<TSchema, TTable, TColumn>;
}[ColumnName<TSchema, TTable>];

type ReverseRelationEntryUnion<TSchema extends SchemaLike, TTable extends TableName<TSchema>> = {
  [TSourceTable in TableName<TSchema>]: {
    [TColumn in ColumnName<TSchema, TSourceTable>]: ColumnBuilderReferences<
      BuilderForColumn<TSchema, TSourceTable, TColumn>
    > extends TTable
      ? {
          name: `${TSourceTable}Via${Capitalize<ForwardRelationName<TColumn>>}`;
          toTable: TSourceTable;
          isArray: true;
          nullable: false;
        }
      : never;
  }[ColumnName<TSchema, TSourceTable>];
}[TableName<TSchema>];

type ForwardRelationMap<TSchema extends SchemaLike, TTable extends TableName<TSchema>> = {
  [TRelation in ForwardRelationEntries<TSchema, TTable> as TRelation["name"]]: Omit<
    TRelation,
    "name"
  >;
};

type ReverseRelationMap<TSchema extends SchemaLike, TTable extends TableName<TSchema>> = {
  [TRelation in ReverseRelationEntryUnion<TSchema, TTable> as TRelation["name"]]: Omit<
    TRelation,
    "name"
  >;
};

type RelationMap<
  TSchema extends SchemaLike,
  TTable extends TableName<TSchema>,
> = ForwardRelationMap<TSchema, TTable> & ReverseRelationMap<TSchema, TTable>;

type RelationName<TSchema extends SchemaLike, TTable extends TableName<TSchema>> = Extract<
  keyof RelationMap<TSchema, TTable>,
  string
>;

type RelationInfo<
  TSchema extends SchemaLike,
  TTable extends TableName<TSchema>,
  TRelation extends RelationName<TSchema, TTable>,
> = Extract<
  ForwardRelationEntries<TSchema, TTable> | ReverseRelationEntryUnion<TSchema, TTable>,
  { name: TRelation }
>;

type RelationTargetTable<
  TSchema extends SchemaLike,
  TTable extends TableName<TSchema>,
  TRelation extends RelationName<TSchema, TTable>,
> = RelationInfo<TSchema, TTable, TRelation>["toTable"];

type RelationIsArray<
  TSchema extends SchemaLike,
  TTable extends TableName<TSchema>,
  TRelation extends RelationName<TSchema, TTable>,
> = RelationInfo<TSchema, TTable, TRelation>["isArray"];

type RelationNullable<
  TSchema extends SchemaLike,
  TTable extends TableName<TSchema>,
  TRelation extends RelationName<TSchema, TTable>,
> = RelationInfo<TSchema, TTable, TRelation>["nullable"];

type QueryBuilderShape<
  TSchema extends SchemaLike,
  TTable extends TableName<TSchema>,
  TRow = unknown,
> = QueryBuilder<TRow> & {
  readonly _table: TTable;
  readonly _initType: TableInit<TSchema, TTable>;
};

type PermissionIntrospectionColumns = {
  $canRead: boolean | null;
  $canEdit: boolean | null;
  $canDelete: boolean | null;
};

export type TableSelectableColumn<TSchema extends SchemaLike, TTable extends TableName<TSchema>> =
  | BaseColumnName<TSchema, TTable>
  | PermissionIntrospectionColumn
  | "*";

export type TableOrderableColumn<TSchema extends SchemaLike, TTable extends TableName<TSchema>> =
  | BaseColumnName<TSchema, TTable>
  | PermissionIntrospectionColumn;

export type TableSelected<
  TSchema extends SchemaLike,
  TTable extends TableName<TSchema>,
  TSelection extends TableSelectableColumn<TSchema, TTable> = BaseColumnName<TSchema, TTable>,
> = Simplify<
  "*" extends TSelection
    ? TableRow<TSchema, TTable>
    : Pick<TableRow<TSchema, TTable>, Extract<TSelection | "id", keyof TableRow<TSchema, TTable>>> &
        Pick<PermissionIntrospectionColumns, Extract<TSelection, PermissionIntrospectionColumn>>
>;

type ApplyRelationCardinality<
  TValue,
  TIsArray extends boolean,
  TNullable extends boolean,
> = TIsArray extends true
  ? TNullable extends true
    ? TValue[] | undefined
    : TValue[]
  : TNullable extends true
    ? TValue | undefined
    : TValue;

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
  TSpec,
> = TSpec extends true
  ? ApplyRelationCardinality<
      TableRow<TSchema, RelationTargetTable<TSchema, TTable, TRelation>>,
      RelationIsArray<TSchema, TTable, TRelation>,
      RelationNullable<TSchema, TTable, TRelation>
    >
  : TSpec extends QueryBuilderShape<
        TSchema,
        RelationTargetTable<TSchema, TTable, TRelation>,
        infer TQueryRow
      >
    ? ApplyRelationCardinality<
        TQueryRow,
        RelationIsArray<TSchema, TTable, TRelation>,
        RelationNullable<TSchema, TTable, TRelation>
      >
    : TSpec extends TableInclude<TSchema, RelationTargetTable<TSchema, TTable, TRelation>>
      ? ApplyRelationCardinality<
          TableSelectedWithIncludes<
            TSchema,
            RelationTargetTable<TSchema, TTable, TRelation>,
            TSpec
          >,
          RelationIsArray<TSchema, TTable, TRelation>,
          RelationNullable<TSchema, TTable, TRelation>
        >
      : never;

type IncludedRelations<
  TSchema extends SchemaLike,
  TTable extends TableName<TSchema>,
  TInclude extends TableInclude<TSchema, TTable>,
> = {
  [TRelation in keyof TInclude & RelationName<TSchema, TTable>]-?: IncludedRelationValue<
    TSchema,
    TTable,
    TRelation,
    NonNullable<TInclude[TRelation]>
  >;
};

export type TableSelectedWithIncludes<
  TSchema extends SchemaLike,
  TTable extends TableName<TSchema>,
  TInclude extends TableInclude<TSchema, TTable> = {},
  TSelection extends TableSelectableColumn<TSchema, TTable> = DefaultSelection<TSchema, TTable>,
> = Simplify<
  Omit<
    TableSelected<TSchema, TTable, TSelection>,
    Extract<keyof TInclude, keyof TableSelected<TSchema, TTable, TSelection>>
  > &
    IncludedRelations<TSchema, TTable, TInclude>
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
  TWhere extends object = Record<string, never>,
  TRelations extends TableRelationMap = {},
> {
  readonly name: TName;
  readonly row: TRow;
  readonly where: TWhere;
  readonly relations: TRelations;
}

export type AnyTableMeta = TableMeta<
  string,
  { id: string },
  Record<string, unknown>,
  TableRelationMap
>;

type TableNameFromMeta<TMeta extends AnyTableMeta> = TMeta["name"];
type TableRowFromMeta<TMeta extends AnyTableMeta> = TMeta["row"];
type TableInitFromMeta<TMeta extends AnyTableMeta> = Simplify<Omit<TableRowFromMeta<TMeta>, "id">>;
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
  | PermissionIntrospectionColumn
  | "*";
type TableOrderableFromMeta<TMeta extends AnyTableMeta> =
  | BaseColumnNameFromMeta<TMeta>
  | PermissionIntrospectionColumn;
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
        TableWhereInput<TSchema, TTable>,
        SchemaRelations<TTable, TSchema>
      >
    : never;

type SchemaMeta<TTable extends string, TSchema extends SchemaLike> = SchemaTable<TTable, TSchema>;

type MetaQueryBuilderShape<TMeta extends AnyTableMeta, TRow = unknown> = QueryBuilder<TRow> & {
  readonly _table: TableNameFromMeta<TMeta>;
  readonly _initType: TableInitFromMeta<TMeta>;
};

type BuilderInclude<TMeta extends AnyTableMeta> = {
  [TRelation in RelationNameFromMeta<TMeta>]?:
    | true
    | BuilderInclude<RelationTargetFromMeta<TMeta, TRelation>>
    | MetaQueryBuilderShape<RelationTargetFromMeta<TMeta, TRelation>, any>;
};

type SelectedFromMeta<
  TMeta extends AnyTableMeta,
  TSelection extends TableSelectableFromMeta<TMeta> = DefaultTableSelection<TMeta>,
> = Simplify<
  "*" extends TSelection
    ? TableRowFromMeta<TMeta>
    : Pick<TableRowFromMeta<TMeta>, Extract<TSelection | "id", keyof TableRowFromMeta<TMeta>>> &
        Pick<PermissionIntrospectionColumns, Extract<TSelection, PermissionIntrospectionColumn>>
>;

type IncludedRelationValueFromMeta<
  TMeta extends AnyTableMeta,
  TRelation extends RelationNameFromMeta<TMeta>,
  TSpec,
> = TSpec extends true
  ? ApplyRelationCardinality<
      TableRowFromMeta<RelationTargetFromMeta<TMeta, TRelation>>,
      RelationIsArrayFromMeta<TMeta, TRelation>,
      RelationNullableFromMeta<TMeta, TRelation>
    >
  : TSpec extends MetaQueryBuilderShape<RelationTargetFromMeta<TMeta, TRelation>, infer TQueryRow>
    ? ApplyRelationCardinality<
        TQueryRow,
        RelationIsArrayFromMeta<TMeta, TRelation>,
        RelationNullableFromMeta<TMeta, TRelation>
      >
    : TSpec extends BuilderInclude<RelationTargetFromMeta<TMeta, TRelation>>
      ? ApplyRelationCardinality<
          SelectedWithIncludesFromMeta<RelationTargetFromMeta<TMeta, TRelation>, TSpec>,
          RelationIsArrayFromMeta<TMeta, TRelation>,
          RelationNullableFromMeta<TMeta, TRelation>
        >
      : never;

type IncludedRelationsFromMeta<
  TMeta extends AnyTableMeta,
  TInclude extends BuilderInclude<TMeta>,
> = {
  [TRelation in keyof TInclude & RelationNameFromMeta<TMeta>]-?: IncludedRelationValueFromMeta<
    TMeta,
    TRelation,
    NonNullable<TInclude[TRelation]>
  >;
};

type SelectedWithIncludesFromMeta<
  TMeta extends AnyTableMeta,
  TInclude extends BuilderInclude<TMeta> = {},
  TSelection extends TableSelectableFromMeta<TMeta> = DefaultTableSelection<TMeta>,
> = Simplify<
  Omit<
    SelectedFromMeta<TMeta, TSelection>,
    Extract<keyof TInclude, keyof SelectedFromMeta<TMeta, TSelection>>
  > &
    IncludedRelationsFromMeta<TMeta, TInclude>
>;

type BuiltCondition = { column: string; op: string; value: unknown };
type BuiltGather = {
  max_depth: number;
  step_table: string;
  step_current_column: string;
  step_conditions: BuiltCondition[];
  step_hops: string[];
};

export class TypedTableQueryBuilder<
  TMeta extends AnyTableMeta,
  TInclude extends BuilderInclude<TMeta> = {},
  TSelection extends TableSelectableFromMeta<TMeta> = DefaultTableSelection<TMeta>,
> implements QueryBuilder<SelectedWithIncludesFromMeta<TMeta, TInclude, TSelection>> {
  readonly _table: TableNameFromMeta<TMeta>;
  readonly _schema: WasmSchema;
  declare readonly _rowType: SelectedWithIncludesFromMeta<TMeta, TInclude, TSelection>;
  declare readonly _initType: TableInitFromMeta<TMeta>;
  private _conditions: BuiltCondition[] = [];
  private _includes: Partial<BuilderInclude<TMeta>> = {};
  private _selectColumns?: string[];
  private _orderBys: Array<[string, "asc" | "desc"]> = [];
  private _limitVal?: number;
  private _offsetVal?: number;
  private _hops: string[] = [];
  private _gatherVal?: BuiltGather;

  constructor(table: TableNameFromMeta<TMeta>, schema: WasmSchema) {
    this._table = table;
    this._schema = schema;
  }

  where(conditions: TableWhereFromMeta<TMeta>): MetaQueryHandle<TMeta, TInclude, TSelection> {
    const clone = this._clone<TInclude, TSelection>();
    for (const [key, value] of Object.entries(conditions as Record<string, unknown>)) {
      if (value === undefined) continue;
      if (typeof value === "object" && value !== null && !Array.isArray(value)) {
        for (const [op, opValue] of Object.entries(value)) {
          if (opValue !== undefined) {
            clone._conditions.push({ column: key, op, value: opValue });
          }
        }
      } else {
        clone._conditions.push({ column: key, op: "eq", value });
      }
    }
    return clone;
  }

  select<NewSelection extends TableSelectableFromMeta<TMeta>>(
    ...columns: [NewSelection, ...NewSelection[]]
  ): MetaQueryHandle<TMeta, TInclude, NewSelection> {
    const clone = this._clone<TInclude, NewSelection>();
    clone._selectColumns = [...columns] as string[];
    return clone;
  }

  include<NewInclude extends BuilderInclude<TMeta>>(
    relations: NewInclude,
  ): MetaQueryHandle<TMeta, TInclude & NewInclude, TSelection> {
    const clone = this._clone<TInclude & NewInclude, TSelection>();
    clone._includes = { ...this._includes, ...relations };
    return clone;
  }

  orderBy(
    column: TableOrderableFromMeta<TMeta>,
    direction: "asc" | "desc" = "asc",
  ): MetaQueryHandle<TMeta, TInclude, TSelection> {
    const clone = this._clone<TInclude, TSelection>();
    clone._orderBys.push([column as string, direction]);
    return clone;
  }

  limit(n: number): MetaQueryHandle<TMeta, TInclude, TSelection> {
    const clone = this._clone<TInclude, TSelection>();
    clone._limitVal = n;
    return clone;
  }

  offset(n: number): MetaQueryHandle<TMeta, TInclude, TSelection> {
    const clone = this._clone<TInclude, TSelection>();
    clone._offsetVal = n;
    return clone;
  }

  hopTo(relation: RelationNameFromMeta<TMeta>): MetaQueryHandle<TMeta, TInclude, TSelection> {
    const clone = this._clone<TInclude, TSelection>();
    clone._hops.push(relation as string);
    return clone;
  }

  gather(options: {
    start: TableWhereFromMeta<TMeta>;
    step: (ctx: { current: string }) => QueryBuilder<unknown>;
    maxDepth?: number;
  }): MetaQueryHandle<TMeta, TInclude, TSelection> {
    if (options.start === undefined) {
      throw new Error("gather(...) requires start where conditions.");
    }
    if (typeof options.step !== "function") {
      throw new Error("gather(...) requires step callback.");
    }

    const maxDepth = options.maxDepth ?? 10;
    if (!Number.isInteger(maxDepth) || maxDepth <= 0) {
      throw new Error("gather(...) maxDepth must be a positive integer.");
    }
    if (Object.keys(this._includes).length > 0) {
      throw new Error("gather(...) does not support include(...) in MVP.");
    }
    if (this._hops.length > 0) {
      throw new Error("gather(...) must be called before hopTo(...).");
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

    const currentCondition = currentConditions[0];
    const stepConditions = stepBuilt.conditions.filter(
      (condition) => !(condition.op === "eq" && condition.value === currentToken),
    );

    const withStart = this.where(options.start);
    const clone = withStart._clone<TInclude, TSelection>();
    clone._hops = [];
    clone._gatherVal = {
      max_depth: maxDepth,
      step_table: stepBuilt.table,
      step_current_column: currentCondition.column,
      step_conditions: stepConditions,
      step_hops: stepHops,
    };

    return clone;
  }

  _build(): string {
    return JSON.stringify({
      table: this._table,
      conditions: this._conditions,
      includes: this._includes,
      select: this._selectColumns,
      orderBy: this._orderBys,
      limit: this._limitVal,
      offset: this._offsetVal,
      hops: this._hops,
      gather: this._gatherVal,
    });
  }

  toJSON(): unknown {
    return JSON.parse(this._build());
  }

  private _clone<
    TNewInclude extends BuilderInclude<TMeta>,
    TNewSelection extends TableSelectableFromMeta<TMeta>,
  >(): TypedTableQueryBuilder<TMeta, TNewInclude, TNewSelection> {
    const clone = new TypedTableQueryBuilder<TMeta, TNewInclude, TNewSelection>(
      this._table,
      this._schema,
    );
    clone._conditions = [...this._conditions];
    clone._includes = { ...this._includes } as Partial<BuilderInclude<TMeta>>;
    clone._selectColumns = this._selectColumns ? [...this._selectColumns] : undefined;
    clone._orderBys = [...this._orderBys];
    clone._limitVal = this._limitVal;
    clone._offsetVal = this._offsetVal;
    clone._hops = [...this._hops];
    clone._gatherVal = this._gatherVal
      ? {
          ...this._gatherVal,
          step_conditions: this._gatherVal.step_conditions.map((condition) => ({ ...condition })),
          step_hops: [...this._gatherVal.step_hops],
        }
      : undefined;
    return clone;
  }
}

interface MetaQueryHandle<
  TMeta extends AnyTableMeta,
  TInclude extends BuilderInclude<TMeta> = {},
  TSelection extends TableSelectableFromMeta<TMeta> = DefaultTableSelection<TMeta>,
> extends TypedTableQueryBuilder<TMeta, TInclude, TSelection> {}

export interface Query<
  TTable extends string,
  TInclude extends BuilderInclude<SchemaMeta<TTable, TSchema>> = {},
  TSelection extends TableSelectableFromMeta<SchemaMeta<TTable, TSchema>> = any,
  TSchema extends SchemaLike = SchemaLike,
> extends TypedTableQueryBuilder<SchemaMeta<TTable, TSchema>, TInclude, TSelection> {
  where(
    conditions: TableWhereInput<TSchema, Extract<TTable, TableName<TSchema>>>,
  ): Query<TTable, TInclude, TSelection, TSchema>;
  select<NewSelection extends TableSelectableFromMeta<SchemaMeta<TTable, TSchema>>>(
    ...columns: [NewSelection, ...NewSelection[]]
  ): Query<TTable, TInclude, NewSelection, TSchema>;
  include<NewInclude extends BuilderInclude<SchemaMeta<TTable, TSchema>>>(
    relations: NewInclude,
  ): Query<TTable, TInclude & NewInclude, TSelection, TSchema>;
  orderBy(
    column: TableOrderableFromMeta<SchemaMeta<TTable, TSchema>>,
    direction?: "asc" | "desc",
  ): Query<TTable, TInclude, TSelection, TSchema>;
  limit(n: number): Query<TTable, TInclude, TSelection, TSchema>;
  offset(n: number): Query<TTable, TInclude, TSelection, TSchema>;
  hopTo(
    relation: RelationNameFromMeta<SchemaMeta<TTable, TSchema>>,
  ): Query<TTable, TInclude, TSelection, TSchema>;
  gather(options: {
    start: TableWhereInput<TSchema, Extract<TTable, TableName<TSchema>>>;
    step: (ctx: { current: string }) => QueryBuilder<unknown>;
    maxDepth?: number;
  }): Query<TTable, TInclude, TSelection, TSchema>;
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
  TSelection extends TableSelectableFromMeta<SchemaMeta<TTable, TSchema>> = DefaultTableSelection<
    SchemaMeta<TTable, TSchema>
  >,
> = Query<TTable, TInclude, TSelection, TSchema>;

export type TableHandle<TTable extends string, TSchema extends SchemaLike> = Table<TTable, TSchema>;

export type TypedApp<TSchema extends SchemaLike> = Simplify<
  {
    [TTable in TableName<TSchema>]: Table<TTable, TSchema>;
  } & {
    wasmSchema: WasmSchema;
  }
>;

export type RowOf<TTable> = TTable extends { readonly _rowType: infer TRow } ? TRow : never;
export type InsertOf<TTable> = TTable extends { readonly _initType: infer TInit } ? TInit : never;
export type TableMetaOf<TTable> =
  TTable extends Table<infer TTableName, infer TSchema>
    ? SchemaMeta<Extract<TTableName, string>, Extract<TSchema, SchemaLike>>
    : TTable extends Query<infer TTableName, any, any, infer TSchema>
      ? SchemaMeta<Extract<TTableName, string>, Extract<TSchema, SchemaLike>>
      : TTable extends TypedTableQueryBuilder<infer TMeta, any, any>
        ? TMeta
        : never;
export type WhereOf<TQuery> = TQuery extends { where(input: infer TWhere): unknown }
  ? TWhere
  : never;

function definitionToColumns(definition: TableDefinition): Column[] {
  const columns: Column[] = [];
  for (const [columnName, builder] of Object.entries(definition)) {
    assertUserColumnNameAllowed(columnName);
    columns.push(builder._build(columnName));
  }
  return columns;
}

function definitionToSchema<TSchema extends SchemaDefinition>(definition: TSchema): Schema {
  return {
    tables: Object.entries(definition).map(([tableName, tableDefinition]) => ({
      name: tableName,
      columns: definitionToColumns(tableDefinition),
    })),
  };
}

export function defineSchema<const TSchema extends SchemaDefinition>(
  definition: TSchema,
): DefinedSchema<TSchema> {
  return definition as unknown as DefinedSchema<TSchema>;
}

export function defineApp<const TSchema extends DefinedSchema<any>>(
  definition: TSchema,
): TypedApp<TSchema>;
export function defineApp<const TSchema extends SchemaDefinition>(
  definition: TSchema,
): TypedApp<DefinedSchema<TSchema>>;
export function defineApp(
  definition: SchemaDefinition | DefinedSchema<any>,
): TypedApp<DefinedSchema<SchemaDefinition>> {
  const normalizedDefinition = definition as unknown as SchemaDefinition;
  const schema = definitionToSchema(normalizedDefinition);
  const wasmSchema = schemaToWasm(schema);
  const tables = {} as Record<string, TypedTableQueryBuilder<any>>;

  for (const tableName of Object.keys(normalizedDefinition)) {
    tables[tableName] = new TypedTableQueryBuilder(tableName, wasmSchema);
  }

  return {
    ...tables,
    wasmSchema,
  } as TypedApp<DefinedSchema<SchemaDefinition>>;
}

export const permissionIntrospectionColumns = [...PERMISSION_INTROSPECTION_COLUMNS];
