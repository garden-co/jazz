import type {
  AnyTypedColumnBuilder,
  ColumnBuilderHasDefault,
  ColumnBuilderOptional,
  ColumnBuilderReferences,
  ColumnBuilderSqlType,
  ColumnBuilderValue,
  ColumnTransform,
} from "./dsl.js";
import { schemaToWasm } from "./codegen/schema-reader.js";
import type { WasmSchema } from "./drivers/types.js";
import {
  PERMISSION_INTROSPECTION_COLUMNS,
  PROVENANCE_MAGIC_COLUMNS,
  type PermissionIntrospectionColumn,
  type ProvenanceMagicColumn,
  assertUserColumnNameAllowed,
} from "./magic-columns.js";
import type { ColumnTransformMap, QueryBuilder } from "./runtime/db.js";
import type { Column, Schema as SchemaAst, SqlType, TSTypeFromSqlType } from "./schema.js";

export type TableDefinition = Record<string, AnyTypedColumnBuilder>;

// Wrap table columns so we can hang chained modifiers like .indexOnly(...) off tables
// without changing the column-level schema representation the runtime uses today.
export class DefinedTable<TColumns extends TableDefinition = TableDefinition> {
  public readonly __jazzTableDefinition = true as const;

  constructor(
    public readonly columns: TColumns,
    public readonly indexedColumns?: readonly Extract<keyof TColumns, string>[],
  ) {}

  indexOnly<
    const TColumnsForIndex extends readonly [
      Extract<keyof TColumns, string>,
      ...Extract<keyof TColumns, string>[],
    ],
  >(columns: TColumnsForIndex): DefinedTable<TColumns> {
    const normalizedColumns = [...columns] as Extract<keyof TColumns, string>[];
    for (const column of normalizedColumns) {
      if (!(column in this.columns)) {
        throw new Error(`table.indexOnly(...) references unknown column "${column}".`);
      }
    }

    return new DefinedTable(this.columns, normalizedColumns);
  }
}

/**
 * Define a table with the given columns.
 *
 * @example
 * ```typescript
 * const schema = {
 *   todos: s.table({
 *     title: s.string(),
 *     done: s.boolean(),
 *   }),
 * });
 * type AppSchema = s.Schema<typeof schema>;
 * export const app: s.App<AppSchema> = s.defineApp(schema);
 * ```
 */
export function defineTable<const TColumns extends TableDefinition>(
  columns: TColumns,
): DefinedTable<TColumns> {
  return new DefinedTable(columns);
}

type TableSource<TColumns extends TableDefinition = any> = TColumns | DefinedTable<TColumns>;

type NormalizeTableDefinition<TTable extends TableSource> =
  TTable extends DefinedTable<infer TColumns>
    ? Simplify<TColumns>
    : TTable extends TableDefinition
      ? Simplify<TTable>
      : never;

export type SchemaDefinition = Record<string, TableSource>;
export type Simplify<T> = { [K in keyof T]: T[K] } & {};
export type CompactSchema<TSchema extends SchemaDefinition> = Simplify<{
  [TTable in keyof TSchema]: NormalizeTableDefinition<TSchema[TTable]>;
}>;

declare const definedSchemaBrand: unique symbol;
export interface Schema<TSchema extends SchemaDefinition = SchemaDefinition> {
  readonly [definedSchemaBrand]: CompactSchema<TSchema>;
}

export type DefinedSchema<TSchema extends SchemaDefinition = SchemaDefinition> = Schema<TSchema>;

type SchemaLike = SchemaDefinition | Schema<any>;
type SchemaColumns<TSchema extends SchemaDefinition> = CompactSchema<TSchema>;
type InvalidRefTargetEntries<TSchema extends SchemaDefinition> = {
  [TTable in Extract<keyof SchemaColumns<TSchema>, string>]: {
    [TColumn in Extract<
      keyof SchemaColumns<TSchema>[TTable],
      string
    >]: SchemaColumns<TSchema>[TTable][TColumn] extends infer TBuilder extends AnyTypedColumnBuilder
      ? ColumnBuilderSqlType<TBuilder> extends
          | "UUID"
          | {
              kind: "ARRAY";
              element: "UUID";
            }
        ? ColumnBuilderReferences<TBuilder> extends infer TRef
          ? TRef extends string
            ? TRef extends Extract<keyof SchemaColumns<TSchema>, string>
              ? never
              : {
                  table: TTable;
                  column: TColumn;
                  ref: TRef;
                }
            : never
          : never
        : never
      : never;
  }[Extract<keyof SchemaColumns<TSchema>[TTable], string>];
}[Extract<keyof SchemaColumns<TSchema>, string>];

type ValidateSchemaRefs<TSchema extends SchemaDefinition> = [
  InvalidRefTargetEntries<TSchema>,
] extends [never]
  ? unknown
  : {
      readonly __schemaRefValidationError__: "Schema refs must point at declared table names";
      readonly __invalidRefTargets__: InvalidRefTargetEntries<TSchema>;
    };

type NormalizedSchema<TSchema extends SchemaLike> =
  TSchema extends Schema<infer TDefinition>
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

type ColumnValue<TBuilder extends AnyTypedColumnBuilder> = ColumnBuilderValue<TBuilder>;
type StoredColumnValue<TBuilder extends AnyTypedColumnBuilder> = TSTypeFromSqlType<
  ColumnBuilderSqlType<TBuilder>
>;
type ReturnedColumnValue<TBuilder extends AnyTypedColumnBuilder> =
  ColumnBuilderOptional<TBuilder> extends true
    ? ColumnValue<TBuilder> | null
    : ColumnValue<TBuilder>;
type InsertColumnValue<TBuilder extends AnyTypedColumnBuilder> =
  ColumnBuilderOptional<TBuilder> extends true
    ? ColumnValue<TBuilder> | null
    : ColumnValue<TBuilder>;

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
type DefaultedColumnName<TSchema extends SchemaLike, TTable extends TableName<TSchema>> = {
  [TColumn in ColumnName<TSchema, TTable>]-?: ColumnBuilderHasDefault<
    BuilderForColumn<TSchema, TTable, TColumn>
  > extends true
    ? TColumn
    : never;
}[ColumnName<TSchema, TTable>];
type OptionalInsertColumnName<TSchema extends SchemaLike, TTable extends TableName<TSchema>> =
  | OptionalColumnName<TSchema, TTable>
  | DefaultedColumnName<TSchema, TTable>;
type RequiredInsertColumnName<
  TSchema extends SchemaLike,
  TTable extends TableName<TSchema>,
> = Exclude<ColumnName<TSchema, TTable>, OptionalInsertColumnName<TSchema, TTable>>;

export type TableRow<TSchema extends SchemaLike, TTable extends TableName<TSchema>> = Simplify<
  {
    id: string;
  } & {
    [TColumn in RequiredColumnName<TSchema, TTable>]: ColumnValue<
      BuilderForColumn<TSchema, TTable, TColumn>
    >;
  } & {
    [TColumn in OptionalColumnName<TSchema, TTable>]: ReturnedColumnValue<
      BuilderForColumn<TSchema, TTable, TColumn>
    >;
  }
>;

export type TableInit<TSchema extends SchemaLike, TTable extends TableName<TSchema>> = Simplify<
  {
    [TColumn in RequiredInsertColumnName<TSchema, TTable>]: InsertColumnValue<
      BuilderForColumn<TSchema, TTable, TColumn>
    >;
  } & {
    [TColumn in OptionalInsertColumnName<TSchema, TTable>]?: InsertColumnValue<
      BuilderForColumn<TSchema, TTable, TColumn>
    >;
  }
>;

type MaybeNullableWhere<T, TOptional extends boolean> = TOptional extends true ? T | null : T;
type WhereEqNe<T, TOptional extends boolean, TExtra extends object = {}> =
  | MaybeNullableWhere<T, TOptional>
  | ({
      eq?: MaybeNullableWhere<T, TOptional>;
      ne?: MaybeNullableWhere<T, TOptional>;
    } & TExtra);
type NumberWhere<T extends number, TOptional extends boolean> = WhereEqNe<
  T,
  TOptional,
  { gt?: T; gte?: T; lt?: T; lte?: T }
>;
type TimestampWhere<TOptional extends boolean> = WhereEqNe<
  Date | number,
  TOptional,
  {
    gt?: Date | number;
    gte?: Date | number;
    lt?: Date | number;
    lte?: Date | number;
  }
>;
type UuidWhere<TOptional extends boolean, TRef extends string | undefined> = TRef extends string
  ? WhereEqNe<string, TOptional, TOptional extends true ? { isNull?: boolean } : {}>
  : WhereEqNe<
      string,
      TOptional,
      TOptional extends true ? { in?: string[]; isNull?: boolean } : { in?: string[] }
    >;

type WhereInputForBuilder<TBuilder extends AnyTypedColumnBuilder> =
  ColumnBuilderSqlType<TBuilder> extends "TEXT"
    ? WhereEqNe<string, ColumnBuilderOptional<TBuilder>, { contains?: string }>
    : ColumnBuilderSqlType<TBuilder> extends "BOOLEAN"
      ? boolean
      : ColumnBuilderSqlType<TBuilder> extends "INTEGER" | "REAL"
        ? NumberWhere<number, ColumnBuilderOptional<TBuilder>>
        : ColumnBuilderSqlType<TBuilder> extends "TIMESTAMP"
          ? TimestampWhere<ColumnBuilderOptional<TBuilder>>
          : ColumnBuilderSqlType<TBuilder> extends "UUID"
            ? UuidWhere<ColumnBuilderOptional<TBuilder>, ColumnBuilderReferences<TBuilder>>
            : ColumnBuilderSqlType<TBuilder> extends "BYTEA"
              ? WhereEqNe<Uint8Array, ColumnBuilderOptional<TBuilder>>
              : ColumnBuilderSqlType<TBuilder> extends { kind: "JSON" }
                ? WhereEqNe<
                    StoredColumnValue<TBuilder>,
                    ColumnBuilderOptional<TBuilder>,
                    { in?: StoredColumnValue<TBuilder>[] }
                  >
                : ColumnBuilderSqlType<TBuilder> extends {
                      kind: "ENUM";
                      variants: readonly (infer TVariant extends string)[];
                    }
                  ? WhereEqNe<TVariant, ColumnBuilderOptional<TBuilder>, { in?: TVariant[] }>
                  : ColumnBuilderSqlType<TBuilder> extends {
                        kind: "ARRAY";
                        element: infer TElementSql extends SqlType;
                      }
                    ? WhereEqNe<
                        StoredColumnValue<TBuilder>,
                        ColumnBuilderOptional<TBuilder>,
                        { contains?: TSTypeFromSqlType<TElementSql> }
                      >
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
  } & {
    [TColumn in ProvenanceMagicColumn]?:
      | string
      | Date
      | number
      | {
          eq?: string | Date | number;
          ne?: string | Date | number;
          gt?: Date | number;
          gte?: Date | number;
          lt?: Date | number;
          lte?: Date | number;
        };
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

type StripRefSuffix<TColumn extends string> = TColumn extends `${infer TPrefix}_ids`
  ? TPrefix
  : TColumn extends `${infer TPrefix}Ids`
    ? TPrefix
    : TColumn extends `${infer TPrefix}_id`
      ? TPrefix
      : TColumn extends `${infer TPrefix}Id`
        ? TPrefix
        : TColumn;

type MaybePluralize<TName extends string> = TName extends `${string}s` ? TName : `${TName}s`;

type ForwardRelationName<TColumn extends string> = TColumn extends `${string}_ids` | `${string}Ids`
  ? MaybePluralize<StripRefSuffix<TColumn>>
  : StripRefSuffix<TColumn>;

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

type RelationSeedQuery<TTable extends string = string> = QueryBuilder<unknown> & {
  readonly _table: TTable;
  _serializeRelation(): unknown;
};

type PermissionIntrospectionColumns = {
  $canRead: boolean | null;
  $canEdit: boolean | null;
  $canDelete: boolean | null;
};

type ProvenanceMagicColumns = {
  $createdBy: string;
  $createdAt: Date;
  $updatedBy: string;
  $updatedAt: Date;
};

export type TableSelectableColumn<TSchema extends SchemaLike, TTable extends TableName<TSchema>> =
  | BaseColumnName<TSchema, TTable>
  | PermissionIntrospectionColumn
  | ProvenanceMagicColumn
  | "*";

export type TableOrderableColumn<TSchema extends SchemaLike, TTable extends TableName<TSchema>> =
  | BaseColumnName<TSchema, TTable>
  | PermissionIntrospectionColumn
  | ProvenanceMagicColumn;

export type TableSelected<
  TSchema extends SchemaLike,
  TTable extends TableName<TSchema>,
  TSelection extends TableSelectableColumn<TSchema, TTable> = BaseColumnName<TSchema, TTable>,
> = Simplify<
  ("*" extends TSelection
    ? TableRow<TSchema, TTable>
    : Pick<
        TableRow<TSchema, TTable>,
        Extract<TSelection | "id", keyof TableRow<TSchema, TTable>>
      >) &
    Pick<PermissionIntrospectionColumns, Extract<TSelection, PermissionIntrospectionColumn>> &
    Pick<ProvenanceMagicColumns, Extract<TSelection, ProvenanceMagicColumn>>
>;

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
> = Simplify<
  Omit<
    TableSelected<TSchema, TTable, TSelection>,
    Extract<keyof TInclude, keyof TableSelected<TSchema, TTable, TSelection>>
  > &
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
> {
  readonly name: TName;
  readonly row: TRow;
  readonly init: TInit;
  readonly where: TWhere;
  readonly relations: TRelations;
}

export type AnyTableMeta = TableMeta<
  string,
  { id: string },
  Record<string, unknown>,
  Record<string, unknown>,
  TableRelationMap
>;

type TableNameFromMeta<TMeta extends AnyTableMeta> = TMeta["name"];
type TableRowFromMeta<TMeta extends AnyTableMeta> = TMeta["row"];
type TableInitFromMeta<TMeta extends AnyTableMeta> = TMeta["init"];
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
  | ProvenanceMagicColumn
  | "*";
type TableOrderableFromMeta<TMeta extends AnyTableMeta> =
  | BaseColumnNameFromMeta<TMeta>
  | PermissionIntrospectionColumn
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
  ("*" extends TSelection
    ? TableRowFromMeta<TMeta>
    : Pick<TableRowFromMeta<TMeta>, Extract<TSelection | "id", keyof TableRowFromMeta<TMeta>>>) &
    Pick<PermissionIntrospectionColumns, Extract<TSelection, PermissionIntrospectionColumn>> &
    Pick<ProvenanceMagicColumns, Extract<TSelection, ProvenanceMagicColumn>>
>;

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
  TSelection extends TableSelectableFromMeta<TMeta> = DefaultTableSelection<TMeta>,
  TRequired extends boolean = false,
> = Simplify<
  Omit<
    SelectedFromMeta<TMeta, TSelection>,
    Extract<keyof TInclude, keyof SelectedFromMeta<TMeta, TSelection>>
  > &
    IncludedRelationsFromMeta<TMeta, TRequired, TInclude>
>;

type BuiltCondition = { column: string; op: string; value: unknown };
type BuiltRelation = {
  table?: string;
  conditions?: BuiltCondition[];
  hops?: string[];
  gather?: BuiltGather;
  union?: {
    inputs: BuiltRelation[];
  };
};
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

function cloneBuiltRelation(relation: BuiltRelation): BuiltRelation {
  return {
    ...(relation.table ? { table: relation.table } : {}),
    ...(relation.conditions ? { conditions: relation.conditions.map(cloneBuiltCondition) } : {}),
    ...(relation.hops ? { hops: [...relation.hops] } : {}),
    ...(relation.gather ? { gather: cloneBuiltGather(relation.gather) } : {}),
    ...(relation.union
      ? {
          union: {
            inputs: relation.union.inputs.map(cloneBuiltRelation),
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
  TSelection extends TableSelectableFromMeta<TMeta> = DefaultTableSelection<TMeta>,
  TRequired extends boolean = false,
> implements QueryBuilder<SelectedWithIncludesFromMeta<TMeta, TInclude, TSelection, TRequired>> {
  readonly _table: TableNameFromMeta<TMeta>;
  readonly _schema: WasmSchema;
  declare readonly _rowType: SelectedWithIncludesFromMeta<TMeta, TInclude, TSelection, TRequired>;
  declare readonly _initType: TableInitFromMeta<TMeta>;
  private _conditions: BuiltCondition[] = [];
  private _includes: Partial<BuilderInclude<TMeta>> = {};
  private _requireIncludes = false;
  private _selectColumns?: string[];
  private _orderBys: Array<[string, "asc" | "desc"]> = [];
  private _limitVal?: number;
  private _offsetVal?: number;
  private _hops: string[] = [];
  private _gatherVal?: BuiltGather;
  private _unionVal?: BuiltRelation;
  _columnTransforms?: ColumnTransformMap;

  constructor(
    table: TableNameFromMeta<TMeta>,
    schema: WasmSchema,
    columnTransforms?: ColumnTransformMap,
  ) {
    this._table = table;
    this._schema = schema;
    this._columnTransforms = columnTransforms;
  }

  where(
    conditions: TableWhereFromMeta<TMeta>,
  ): MetaQueryHandle<TMeta, TInclude, TSelection, TRequired> {
    if (this._unionVal) {
      throw new Error("union(...) currently only supports gather(...) in MVP.");
    }
    const clone = this._clone<TInclude, TSelection, TRequired>();
    clone._conditions.push(...this._whereConditions(conditions as Record<string, unknown>));
    return clone;
  }

  select<NewSelection extends TableSelectableFromMeta<TMeta>>(
    ...columns: [NewSelection, ...NewSelection[]]
  ): MetaQueryHandle<TMeta, TInclude, NewSelection, TRequired> {
    const clone = this._clone<TInclude, NewSelection, TRequired>();
    clone._selectColumns = [...columns] as string[];
    return clone;
  }

  include<NewInclude extends BuilderInclude<TMeta>>(
    relations: NewInclude,
  ): MetaQueryHandle<TMeta, TInclude & NewInclude, TSelection, TRequired> {
    const clone = this._clone<TInclude & NewInclude, TSelection, TRequired>();
    clone._includes = { ...this._includes, ...relations };
    return clone;
  }

  requireIncludes(): MetaQueryHandle<TMeta, TInclude, TSelection, true> {
    const clone = this._clone<TInclude, TSelection, true>();
    clone._requireIncludes = true;
    return clone;
  }

  orderBy(
    column: TableOrderableFromMeta<TMeta>,
    direction: "asc" | "desc" = "asc",
  ): MetaQueryHandle<TMeta, TInclude, TSelection, TRequired> {
    const clone = this._clone<TInclude, TSelection, TRequired>();
    clone._orderBys.push([column as string, direction]);
    return clone;
  }

  limit(n: number): MetaQueryHandle<TMeta, TInclude, TSelection, TRequired> {
    const clone = this._clone<TInclude, TSelection, TRequired>();
    clone._limitVal = n;
    return clone;
  }

  offset(n: number): MetaQueryHandle<TMeta, TInclude, TSelection, TRequired> {
    const clone = this._clone<TInclude, TSelection, TRequired>();
    clone._offsetVal = n;
    return clone;
  }

  hopTo(
    relation: RelationNameFromMeta<TMeta>,
  ): MetaQueryHandle<TMeta, TInclude, TSelection, TRequired> {
    if (this._unionVal) {
      throw new Error("union(...) currently only supports gather(...) in MVP.");
    }
    const clone = this._clone<TInclude, TSelection, TRequired>();
    clone._hops.push(relation as string);
    return clone;
  }

  gather(options: {
    start?: TableWhereFromMeta<TMeta>;
    step: (ctx: { current: string }) => QueryBuilder<unknown>;
    maxDepth?: number;
  }): MetaQueryHandle<TMeta, TInclude, TSelection, TRequired> {
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

    const needsExplicitSeed =
      this._unionVal !== undefined || this._hops.length > 0 || this._gatherVal !== undefined;
    const seedSource = options.start === undefined ? this : this.where(options.start);
    const clone = needsExplicitSeed
      ? this._clone<TInclude, TSelection, TRequired>()
      : seedSource._clone<TInclude, TSelection, TRequired>();
    clone._conditions = [];
    clone._hops = [];
    clone._gatherVal = {
      ...(needsExplicitSeed ? { seed: seedSource._serializeRelation() } : {}),
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
    return JSON.stringify({
      table: this._table,
      conditions: this._conditions,
      includes: this._includes,
      __jazz_requireIncludes: this._requireIncludes || undefined,
      select: this._selectColumns,
      orderBy: this._orderBys,
      limit: this._limitVal,
      offset: this._offsetVal,
      hops: this._hops,
      gather: this._gatherVal,
      ...(this._unionVal ? { union: cloneBuiltRelation(this._unionVal).union } : {}),
    });
  }

  toJSON(): unknown {
    return JSON.parse(this._build());
  }

  private _clone<
    TNewInclude extends BuilderInclude<TMeta>,
    TNewSelection extends TableSelectableFromMeta<TMeta>,
    TNewRequired extends boolean,
  >(): TypedTableQueryBuilder<TMeta, TNewInclude, TNewSelection, TNewRequired> {
    const clone = new TypedTableQueryBuilder<TMeta, TNewInclude, TNewSelection, TNewRequired>(
      this._table,
      this._schema,
      this._columnTransforms,
    );
    clone._conditions = [...this._conditions];
    clone._includes = { ...this._includes } as Partial<BuilderInclude<TMeta>>;
    clone._requireIncludes = this._requireIncludes;
    clone._selectColumns = this._selectColumns ? [...this._selectColumns] : undefined;
    clone._orderBys = [...this._orderBys];
    clone._limitVal = this._limitVal;
    clone._offsetVal = this._offsetVal;
    clone._hops = [...this._hops];
    clone._gatherVal = this._gatherVal ? cloneBuiltGather(this._gatherVal) : undefined;
    clone._unionVal = this._unionVal ? cloneBuiltRelation(this._unionVal) : undefined;
    return clone;
  }

  private _whereConditions(conditions: Record<string, unknown>): BuiltCondition[] {
    const built: BuiltCondition[] = [];
    for (const [key, value] of Object.entries(conditions)) {
      if (value === undefined) continue;
      if (typeof value === "object" && value !== null && !Array.isArray(value)) {
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

  _serializeRelation(): BuiltRelation {
    if (this._unionVal) {
      return cloneBuiltRelation(this._unionVal);
    }
    return {
      table: this._table,
      conditions: this._conditions.map(cloneBuiltCondition),
      hops: [...this._hops],
      ...(this._gatherVal ? { gather: cloneBuiltGather(this._gatherVal) } : {}),
    };
  }
}

interface MetaQueryHandle<
  TMeta extends AnyTableMeta,
  TInclude extends BuilderInclude<TMeta> = {},
  TSelection extends TableSelectableFromMeta<TMeta> = DefaultTableSelection<TMeta>,
  TRequired extends boolean = false,
> extends TypedTableQueryBuilder<TMeta, TInclude, TSelection, TRequired> {}

export interface Query<
  TTable extends string,
  TInclude extends BuilderInclude<SchemaMeta<TTable, TSchema>> = {},
  TSelection extends TableSelectableFromMeta<SchemaMeta<TTable, TSchema>> = any,
  TSchema extends SchemaLike = SchemaLike,
> extends TypedTableQueryBuilder<SchemaMeta<TTable, TSchema>, TInclude, TSelection, false> {
  where(
    conditions: TableWhereInput<TSchema, Extract<TTable, TableName<TSchema>>>,
  ): Query<TTable, TInclude, TSelection, TSchema>;
  select<NewSelection extends TableSelectableFromMeta<SchemaMeta<TTable, TSchema>>>(
    ...columns: [NewSelection, ...NewSelection[]]
  ): Query<TTable, TInclude, NewSelection, TSchema>;
  include<NewInclude extends BuilderInclude<SchemaMeta<TTable, TSchema>>>(
    relations: NewInclude,
  ): Query<TTable, TInclude & NewInclude, TSelection, TSchema>;
  requireIncludes(): RequiredQuery<TTable, TInclude, TSelection, TSchema>;
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
    start?: TableWhereInput<TSchema, Extract<TTable, TableName<TSchema>>>;
    step: (ctx: { current: string }) => QueryBuilder<unknown>;
    maxDepth?: number;
  }): Query<TTable, TInclude, TSelection, TSchema>;
}

export interface RequiredQuery<
  TTable extends string,
  TInclude extends BuilderInclude<SchemaMeta<TTable, TSchema>> = {},
  TSelection extends TableSelectableFromMeta<SchemaMeta<TTable, TSchema>> = any,
  TSchema extends SchemaLike = SchemaLike,
> extends TypedTableQueryBuilder<SchemaMeta<TTable, TSchema>, TInclude, TSelection, true> {
  where(
    conditions: TableWhereInput<TSchema, Extract<TTable, TableName<TSchema>>>,
  ): RequiredQuery<TTable, TInclude, TSelection, TSchema>;
  select<NewSelection extends TableSelectableFromMeta<SchemaMeta<TTable, TSchema>>>(
    ...columns: [NewSelection, ...NewSelection[]]
  ): RequiredQuery<TTable, TInclude, NewSelection, TSchema>;
  include<NewInclude extends BuilderInclude<SchemaMeta<TTable, TSchema>>>(
    relations: NewInclude,
  ): RequiredQuery<TTable, TInclude & NewInclude, TSelection, TSchema>;
  requireIncludes(): RequiredQuery<TTable, TInclude, TSelection, TSchema>;
  orderBy(
    column: TableOrderableFromMeta<SchemaMeta<TTable, TSchema>>,
    direction?: "asc" | "desc",
  ): RequiredQuery<TTable, TInclude, TSelection, TSchema>;
  limit(n: number): RequiredQuery<TTable, TInclude, TSelection, TSchema>;
  offset(n: number): RequiredQuery<TTable, TInclude, TSelection, TSchema>;
  hopTo(
    relation: RelationNameFromMeta<SchemaMeta<TTable, TSchema>>,
  ): RequiredQuery<TTable, TInclude, TSelection, TSchema>;
  gather(options: {
    start?: TableWhereInput<TSchema, Extract<TTable, TableName<TSchema>>>;
    step: (ctx: { current: string }) => QueryBuilder<unknown>;
    maxDepth?: number;
  }): RequiredQuery<TTable, TInclude, TSelection, TSchema>;
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

export type App<TSchema extends SchemaLike> = Simplify<
  {
    [TTable in TableName<TSchema>]: Table<TTable, TSchema>;
  } & {
    union<TTable extends string>(
      relations: readonly RelationSeedQuery<TTable>[],
    ): TypedTableQueryBuilder<any, any, any, any>;
    wasmSchema: WasmSchema;
  }
>;

export type TypedApp<TSchema extends SchemaLike> = App<TSchema>;

type SchemaSlice<
  TSchema extends SchemaLike,
  TTables extends readonly TableName<TSchema>[],
> = Schema<Pick<NormalizedSchema<TSchema>, TTables[number]>>;

export interface SliceableApp<TSchema extends SchemaLike> {
  readonly wasmSchema: WasmSchema;
  slice<const TTables extends readonly [TableName<TSchema>, ...TableName<TSchema>[]]>(
    ...tables: TTables
  ): App<SchemaSlice<TSchema, TTables>>;
}

export type RowOf<TTable> = TTable extends { readonly _rowType: infer TRow } ? TRow : never;
export type InsertOf<TTable> = TTable extends { readonly _initType: infer TInit } ? TInit : never;
export type TableMetaOf<TTable> =
  TTable extends Table<infer TTableName, infer TSchema>
    ? SchemaMeta<Extract<TTableName, string>, Extract<TSchema, SchemaLike>>
    : TTable extends Query<infer TTableName, any, any, infer TSchema>
      ? SchemaMeta<Extract<TTableName, string>, Extract<TSchema, SchemaLike>>
      : TTable extends RequiredQuery<infer TTableName, any, any, infer TSchema>
        ? SchemaMeta<Extract<TTableName, string>, Extract<TSchema, SchemaLike>>
        : TTable extends TypedTableQueryBuilder<infer TMeta, any, any, any>
          ? TMeta
          : never;
export type WhereOf<TQuery> = TQuery extends { where(input: infer TWhere): unknown }
  ? TWhere
  : never;

export function unwrapTableDefinition<const TColumns extends TableDefinition>(
  definition: TColumns | DefinedTable<TColumns>,
): TColumns {
  if (definition instanceof DefinedTable) {
    return definition.columns;
  }

  if (typeof definition === "object" && definition !== null) {
    const maybeDefinedTable = definition as {
      __jazzTableDefinition?: unknown;
      columns?: TColumns;
    };
    if (maybeDefinedTable.__jazzTableDefinition === true && maybeDefinedTable.columns) {
      return maybeDefinedTable.columns;
    }
  }

  return definition;
}

function tableIndexedColumns(
  definition: TableDefinition | DefinedTable<TableDefinition>,
): string[] | undefined {
  if (definition instanceof DefinedTable) {
    return definition.indexedColumns ? [...definition.indexedColumns] : undefined;
  }

  if (typeof definition === "object" && definition !== null) {
    const maybeDefinedTable = definition as {
      __jazzTableDefinition?: unknown;
      indexedColumns?: readonly string[];
    };
    if (maybeDefinedTable.__jazzTableDefinition === true) {
      return maybeDefinedTable.indexedColumns ? [...maybeDefinedTable.indexedColumns] : undefined;
    }
  }

  return undefined;
}

function definitionToColumns(
  definition: TableDefinition | DefinedTable<TableDefinition>,
): Column[] {
  const columnsDefinition = unwrapTableDefinition(definition);
  const columns: Column[] = [];
  for (const [columnName, builder] of Object.entries(columnsDefinition)) {
    assertUserColumnNameAllowed(columnName);
    columns.push(builder._build(columnName));
  }
  return columns;
}

function columnTransformsForTable(
  definition: TableDefinition | DefinedTable<TableDefinition> | undefined,
): ColumnTransformMap | undefined {
  if (!definition) {
    return undefined;
  }

  const columnsDefinition = unwrapTableDefinition(definition);
  const transforms: ColumnTransformMap = {};
  for (const [columnName, builder] of Object.entries(columnsDefinition)) {
    if (builder._transform) {
      transforms[columnName] = builder._transform as ColumnTransform;
    }
  }
  return Object.keys(transforms).length > 0 ? transforms : undefined;
}

function definitionToSchema<TSchema extends SchemaDefinition>(definition: TSchema): SchemaAst {
  return {
    tables: Object.entries(definition).map(([tableName, tableDefinition]) => {
      const indexedColumns = tableIndexedColumns(tableDefinition);
      return {
        name: tableName,
        columns: definitionToColumns(tableDefinition),
        ...(indexedColumns ? { indexedColumns } : {}),
      };
    }),
  };
}

export function defineSchema<const TSchema extends SchemaDefinition>(
  definition: TSchema & ValidateSchemaRefs<TSchema>,
): Schema<TSchema> {
  return definition as unknown as Schema<TSchema>;
}

/**
 * Create an app from a schema definition.
 *
 * @example
 * ```typescript
 * const schema = {
 *   todos: s.table({
 *     title: s.string(),
 *     done: s.boolean(),
 *   }),
 * });
 * type AppSchema = s.Schema<typeof schema>;
 * export const app: s.App<AppSchema> = s.defineApp(schema);
 * ```
 */
export function defineApp<const TSchema extends Schema<any>>(definition: TSchema): App<TSchema>;
export function defineApp<const TSchema extends SchemaDefinition>(
  definition: TSchema & ValidateSchemaRefs<TSchema>,
): App<Schema<TSchema>>;
export function defineApp(
  definition: SchemaDefinition | Schema<any>,
): App<Schema<SchemaDefinition>> {
  const normalizedDefinition = definition as unknown as SchemaDefinition;
  const schema = definitionToSchema(normalizedDefinition);
  const wasmSchema = schemaToWasm(schema);
  return createAppForTables(Object.keys(normalizedDefinition), wasmSchema, normalizedDefinition);
}

/**
 * Create a sliceable app from a full schema definition.
 *
 * The full schema is compiled for runtime hashing, migrations, validation, and query planning,
 * while each `.slice(...)` call exposes a smaller typed app surface backed by that same runtime
 * schema.
 *
 * @example
 * ```typescript
 * const app = s.defineSliceableApp(schema);
 * export const orgApp = app.slice("teams", "projects", "members");
 * ```
 */
export function defineSliceableApp<const TSchema extends Schema<any>>(
  definition: TSchema,
): SliceableApp<TSchema>;
export function defineSliceableApp<const TSchema extends SchemaDefinition>(
  definition: TSchema,
): SliceableApp<Schema<TSchema>>;
export function defineSliceableApp(
  definition: SchemaDefinition | Schema<any>,
): SliceableApp<Schema<SchemaDefinition>> {
  const normalizedDefinition = definition as unknown as SchemaDefinition;
  const schema = definitionToSchema(normalizedDefinition);
  const wasmSchema = schemaToWasm(schema);

  return {
    wasmSchema,
    slice(...tableNames: string[]) {
      if (tableNames.length === 0) {
        throw new Error("slice(...) requires at least one table name.");
      }

      for (const tableName of tableNames) {
        if (!(tableName in normalizedDefinition)) {
          throw new Error(`slice(...) references unknown table "${tableName}".`);
        }
      }

      return createAppForTables(tableNames, wasmSchema, normalizedDefinition);
    },
  } as SliceableApp<Schema<SchemaDefinition>>;
}

function createAppForTables(
  tableNames: readonly string[],
  wasmSchema: WasmSchema,
  definition?: SchemaDefinition,
): App<Schema<SchemaDefinition>> {
  const tables = {} as Record<string, TypedTableQueryBuilder<any>>;

  for (const tableName of tableNames) {
    tables[tableName] = new TypedTableQueryBuilder(
      tableName,
      wasmSchema,
      definition ? columnTransformsForTable(definition[tableName]) : undefined,
    );
  }

  return {
    ...tables,
    union<TTable extends string>(relations: readonly RelationSeedQuery<TTable>[]) {
      if (relations.length === 0) {
        throw new Error("union(...) requires at least one relation.");
      }

      const first = relations[0]!;
      const builder = new TypedTableQueryBuilder(first._table, wasmSchema);
      (builder as any)._unionVal = {
        union: {
          inputs: relations.map((relation) => relation._serializeRelation() as BuiltRelation),
        },
      };
      return builder;
    },
    wasmSchema,
  } as App<Schema<SchemaDefinition>>;
}

export const permissionIntrospectionColumns = [...PERMISSION_INTROSPECTION_COLUMNS];
export const provenanceMagicColumns = [...PROVENANCE_MAGIC_COLUMNS];
