/**
 * `m.defineMigration(...)`: a migration between two schema versions, as
 * `jazz-tools migrations create` writes it in `migrations/*.ts` (.64's stub
 * shape). `jazz-tools generate` turns each into a `migrations` entry of
 * `jazz.app.json` (SPEC §10.10's readable Migration); the engine derives
 * the lens both ways from it.
 *
 * `from` and `to` are witnesses: the tables the migration touches, at both
 * versions. `fromHash` / `toHash` name the versions (short schema hashes).
 */
import type {
  AnyColumnDefinition,
  AnyTypedColumnBuilder,
  ColumnBuilderOptional,
  ColumnBuilderReferences,
  ColumnBuilderSqlType,
} from "./dsl.js";
import { isComputedColumn } from "./dsl.js";
import { assertUserTableColumnNameAllowed } from "./magic-columns.js";
import type { ForwardRelationship } from "./relationships.js";
import type {
  AddOp,
  Column,
  DropOp,
  RenameOp,
  RenameTableFromOp,
  TSTypeFromSqlType,
} from "./schema.js";
import { readableType, readableValue, type ReadableJson } from "./schema-dsl/readable-app.js";
import { isDefinedTable, type DefinedTable, type TableDefinition } from "./schema-dsl/table.js";

/** A migration's witness tables: `s.table(...)` definitions (or plain column records), by name. */
export type SchemaLike = Record<string, TableSource>;
type TableSource = DefinedTable<any, any> | TableDefinition;
type Simplify<T> = { [K in keyof T]: T[K] } & {};

type ExplicitColumnTarget<R, C extends string> = {
  [N in keyof R]: R[N] extends ForwardRelationship<infer T, C> ? T : never;
}[keyof R];
/** A witness table's columns, each forward relation's column marked with its target. */
type ColumnsWithReferences<C, R> = {
  [K in keyof C]: K extends string
    ? [ExplicitColumnTarget<R, K>] extends [never]
      ? C[K]
      : Omit<C[K], "__jazzReferences"> & { readonly __jazzReferences: ExplicitColumnTarget<R, K> }
    : C[K];
};
type NormalizeTable<T> =
  T extends DefinedTable<infer C, infer R> ? ColumnsWithReferences<C, R> : T;

type StoredSchema<S> = {
  [T in keyof S]: {
    [K in keyof S[T] as S[T][K] extends AnyTypedColumnBuilder ? K : never]: S[T][K];
  };
};
type NormalizedSchema<TSchema extends SchemaLike> = StoredSchema<{
  [T in keyof TSchema]: NormalizeTable<TSchema[T]>;
}>;

type TableName<TSchema extends SchemaLike> = Extract<keyof NormalizedSchema<TSchema>, string>;
type AddedTableName<TFrom extends SchemaLike, TTo extends SchemaLike> = Exclude<
  TableName<TTo>,
  TableName<TFrom>
>;
type RemovedTableName<TFrom extends SchemaLike, TTo extends SchemaLike> = Exclude<
  TableName<TFrom>,
  TableName<TTo>
>;
type SharedTableName<TFrom extends SchemaLike, TTo extends SchemaLike> = Extract<
  TableName<TFrom>,
  TableName<TTo>
>;

export type RenameTableShape<TFrom extends SchemaLike, TTo extends SchemaLike> = Simplify<{
  [TTable in AddedTableName<TFrom, TTo>]?: RenameTableFromOp<RemovedTableName<TFrom, TTo>>;
}>;

export type AddedTableShape<TFrom extends SchemaLike, TTo extends SchemaLike> = Simplify<{
  [TTable in AddedTableName<TFrom, TTo>]?: true;
}>;

export type RemovedTableShape<TFrom extends SchemaLike, TTo extends SchemaLike> = Simplify<{
  [TTable in RemovedTableName<TFrom, TTo>]?: true;
}>;

type RenameTables<TRenameTables> = [TRenameTables] extends [Record<string, unknown>]
  ? TRenameTables
  : {};

type CreateTables<TCreateTables> = [TCreateTables] extends [Record<string, unknown>]
  ? TCreateTables
  : {};

type DropTables<TDropTables> = [TDropTables] extends [Record<string, unknown>] ? TDropTables : {};

type RenamedTargetTableName<TRenameTables> = Extract<keyof RenameTables<TRenameTables>, string>;
type DeclaredAddedTableName<TCreateTables> = Extract<keyof CreateTables<TCreateTables>, string>;
type DeclaredRemovedTableName<TDropTables> = Extract<keyof DropTables<TDropTables>, string>;

type RenamedSourceTableName<TRenameTables> = {
  [TTable in RenamedTargetTableName<TRenameTables>]: RenameTables<TRenameTables>[TTable] extends RenameTableFromOp<
    infer TOldName extends string
  >
    ? TOldName
    : never;
}[RenamedTargetTableName<TRenameTables>];

type MigratedTableName<TFrom extends SchemaLike, TTo extends SchemaLike, TRenameTables> =
  | SharedTableName<TFrom, TTo>
  | RenamedTargetTableName<TRenameTables>;

type SourceTableNameFor<
  TFrom extends SchemaLike,
  TTo extends SchemaLike,
  TRenameTables,
  TTable extends MigratedTableName<TFrom, TTo, TRenameTables>,
> =
  TTable extends SharedTableName<TFrom, TTo>
    ? TTable
    : TTable extends RenamedTargetTableName<TRenameTables>
      ? RenameTables<TRenameTables>[TTable] extends RenameTableFromOp<infer TOldName extends string>
        ? Extract<TOldName, TableName<TFrom>>
        : never
      : never;

type ColumnName<TSchema extends SchemaLike, TTable extends TableName<TSchema>> = Extract<
  keyof NormalizedSchema<TSchema>[TTable],
  string
>;

type SourceColumnName<
  TFrom extends SchemaLike,
  TTo extends SchemaLike,
  TRenameTables,
  TTable extends MigratedTableName<TFrom, TTo, TRenameTables>,
> = Extract<
  keyof NormalizedSchema<TFrom>[SourceTableNameFor<TFrom, TTo, TRenameTables, TTable>],
  string
>;

type CommonColumnName<
  TSchema extends SchemaLike,
  TTable extends TableName<TSchema>,
  TOtherColumn extends string,
> = Extract<ColumnName<TSchema, TTable>, TOtherColumn>;

type BuilderForTargetColumn<
  TTo extends SchemaLike,
  TTable extends TableName<TTo>,
  TColumn extends ColumnName<TTo, TTable>,
> = Extract<NormalizedSchema<TTo>[TTable][TColumn], AnyTypedColumnBuilder>;

type BuilderForSourceColumn<
  TFrom extends SchemaLike,
  TTo extends SchemaLike,
  TRenameTables,
  TTable extends MigratedTableName<TFrom, TTo, TRenameTables>,
  TColumn extends SourceColumnName<TFrom, TTo, TRenameTables, TTable>,
> = Extract<
  NormalizedSchema<TFrom>[SourceTableNameFor<TFrom, TTo, TRenameTables, TTable>][TColumn],
  AnyTypedColumnBuilder
>;

type ColumnValue<TBuilder extends AnyTypedColumnBuilder> = TSTypeFromSqlType<
  ColumnBuilderSqlType<TBuilder>
>;

type DefaultValueForBuilder<TBuilder extends AnyTypedColumnBuilder> =
  ColumnBuilderOptional<TBuilder> extends true
    ? ColumnValue<TBuilder> | null
    : ColumnValue<TBuilder>;

type AddedColumnName<
  TFrom extends SchemaLike,
  TTo extends SchemaLike,
  TRenameTables,
  TTable extends MigratedTableName<TFrom, TTo, TRenameTables> & TableName<TTo>,
> = Exclude<ColumnName<TTo, TTable>, SourceColumnName<TFrom, TTo, TRenameTables, TTable>>;

type RemovedColumnName<
  TFrom extends SchemaLike,
  TTo extends SchemaLike,
  TRenameTables,
  TTable extends MigratedTableName<TFrom, TTo, TRenameTables> & TableName<TTo>,
> = Exclude<SourceColumnName<TFrom, TTo, TRenameTables, TTable>, ColumnName<TTo, TTable>>;

type SourceReferenceFor<TReference, TRenameTables> =
  TReference extends keyof RenameTables<TRenameTables>
    ? RenameTables<TRenameTables>[TReference] extends RenameTableFromOp<infer TOldName>
      ? TOldName
      : TReference
    : TReference;

type BuilderIdentity<TBuilder extends AnyTypedColumnBuilder, TRenameTables = undefined> = readonly [
  ColumnBuilderSqlType<TBuilder>,
  ColumnBuilderOptional<TBuilder>,
  SourceReferenceFor<ColumnBuilderReferences<TBuilder>, TRenameTables>,
];

type BuildersEqual<
  TLeft extends AnyTypedColumnBuilder,
  TRight extends AnyTypedColumnBuilder,
  TRenameTables,
> = [BuilderIdentity<TLeft>] extends [BuilderIdentity<TRight, TRenameTables>]
  ? [BuilderIdentity<TRight, TRenameTables>] extends [BuilderIdentity<TLeft>]
    ? true
    : false
  : false;

/**
 * Whether `TRight` is the enum `TLeft` (or an array of it) with variants
 * appended to its list: rows of `TLeft` pass unchanged and sort as before.
 */
type SqlTypeAddsEnumVariants<TLeft, TRight> = TLeft extends {
  kind: "ENUM";
  variants: infer TLeftVariants extends readonly string[];
}
  ? TRight extends { kind: "ENUM"; variants: readonly [...TLeftVariants, ...string[]] }
    ? true
    : false
  : TLeft extends { kind: "ARRAY"; element: infer TLeftElement }
    ? TRight extends { kind: "ARRAY"; element: infer TRightElement }
      ? SqlTypeAddsEnumVariants<TLeftElement, TRightElement>
      : false
    : false;

type SharedBuildersCompatible<
  TLeft extends AnyTypedColumnBuilder,
  TRight extends AnyTypedColumnBuilder,
  TRenameTables,
> =
  BuildersEqual<TLeft, TRight, TRenameTables> extends true
    ? true
    : [
          SqlTypeAddsEnumVariants<ColumnBuilderSqlType<TLeft>, ColumnBuilderSqlType<TRight>>,
          ColumnBuilderReferences<TLeft>,
        ] extends [true, undefined]
      ? [ColumnBuilderOptional<TLeft>] extends [ColumnBuilderOptional<TRight>]
        ? [ColumnBuilderOptional<TRight>] extends [ColumnBuilderOptional<TLeft>]
          ? true
          : false
        : false
      : ColumnBuilderReferences<TLeft> extends undefined
        ? [ColumnBuilderSqlType<TLeft>, ColumnBuilderOptional<TLeft>] extends [
            ColumnBuilderSqlType<TRight>,
            ColumnBuilderOptional<TRight>,
          ]
          ? [ColumnBuilderSqlType<TRight>, ColumnBuilderOptional<TRight>] extends [
              ColumnBuilderSqlType<TLeft>,
              ColumnBuilderOptional<TLeft>,
            ]
            ? true
            : false
          : false
        : false;

type AddOperationForBuilder<TBuilder extends AnyTypedColumnBuilder> = AddOp<
  ColumnBuilderSqlType<TBuilder>,
  DefaultValueForBuilder<TBuilder>
>;

type DropOperationForBuilder<TBuilder extends AnyTypedColumnBuilder> = DropOp<
  ColumnBuilderSqlType<TBuilder>,
  DefaultValueForBuilder<TBuilder>
>;

export type MigrationTableShape<
  TFrom extends SchemaLike,
  TTo extends SchemaLike,
  TRenameTables,
  TTable extends MigratedTableName<TFrom, TTo, TRenameTables> & TableName<TTo>,
> = Simplify<
  {
    [TColumn in AddedColumnName<TFrom, TTo, TRenameTables, TTable>]?:
      | AddOperationForBuilder<BuilderForTargetColumn<TTo, TTable, TColumn>>
      | RenameOp<RemovedColumnName<TFrom, TTo, TRenameTables, TTable>>;
  } & {
    [TColumn in RemovedColumnName<TFrom, TTo, TRenameTables, TTable>]?: DropOperationForBuilder<
      BuilderForSourceColumn<TFrom, TTo, TRenameTables, TTable, TColumn>
    >;
  }
>;

export type MigrationShape<
  TFrom extends SchemaLike,
  TTo extends SchemaLike,
  TRenameTables = undefined,
> = Simplify<{
  [TTable in MigratedTableName<TFrom, TTo, TRenameTables> & TableName<TTo>]?: MigrationTableShape<
    TFrom,
    TTo,
    TRenameTables,
    TTable
  >;
}>;

type MigrationTables<TMigrate> = [TMigrate] extends [Record<string, unknown>] ? TMigrate : {};

type TableOpsFor<TMigrate, TTable extends string> = TTable extends keyof MigrationTables<TMigrate>
  ? MigrationTables<TMigrate>[TTable] extends Record<string, unknown>
    ? MigrationTables<TMigrate>[TTable]
    : {}
  : {};

type TableOpFor<
  TMigrate,
  TTable extends string,
  TColumn extends string,
> = TColumn extends keyof TableOpsFor<TMigrate, TTable>
  ? TableOpsFor<TMigrate, TTable>[TColumn]
  : never;

type RenameSourcesForTable<TTableOps> =
  TTableOps extends Record<string, unknown>
    ? {
        [TColumn in Extract<keyof TTableOps, string>]: TTableOps[TColumn] extends RenameOp<
          infer TOldName extends string
        >
          ? TOldName
          : never;
      }[Extract<keyof TTableOps, string>]
    : never;

type UnknownMigrationTables<
  TFrom extends SchemaLike,
  TTo extends SchemaLike,
  TRenameTables,
  TMigrate,
> = Exclude<
  Extract<keyof MigrationTables<TMigrate>, string>,
  MigratedTableName<TFrom, TTo, TRenameTables>
>;

type UnknownCreateTables<TFrom extends SchemaLike, TTo extends SchemaLike, TCreateTables> = Exclude<
  DeclaredAddedTableName<TCreateTables>,
  AddedTableName<TFrom, TTo>
>;

type UnknownDropTables<TFrom extends SchemaLike, TTo extends SchemaLike, TDropTables> = Exclude<
  DeclaredRemovedTableName<TDropTables>,
  RemovedTableName<TFrom, TTo>
>;

type MissingCreateTables<
  TFrom extends SchemaLike,
  TTo extends SchemaLike,
  TRenameTables,
  TCreateTables,
> = Exclude<
  AddedTableName<TFrom, TTo>,
  RenamedTargetTableName<TRenameTables> | DeclaredAddedTableName<TCreateTables>
>;

type MissingDropTables<
  TFrom extends SchemaLike,
  TTo extends SchemaLike,
  TRenameTables,
  TDropTables,
> = Exclude<
  RemovedTableName<TFrom, TTo>,
  RenamedSourceTableName<TRenameTables> | DeclaredRemovedTableName<TDropTables>
>;

type UnknownMigrationColumns<
  TFrom extends SchemaLike,
  TTo extends SchemaLike,
  TRenameTables,
  TMigrate,
> = {
  [TTable in MigratedTableName<TFrom, TTo, TRenameTables> & TableName<TTo>]: Exclude<
    Extract<keyof TableOpsFor<TMigrate, TTable>, string>,
    | AddedColumnName<TFrom, TTo, TRenameTables, TTable>
    | RemovedColumnName<TFrom, TTo, TRenameTables, TTable>
  > extends infer TUnknownColumn
    ? [TUnknownColumn] extends [never]
      ? never
      : {
          readonly table: TTable;
          readonly column: Extract<TUnknownColumn, string>;
          readonly problem: "Migration tables may only mention added or removed columns";
        }
    : never;
}[MigratedTableName<TFrom, TTo, TRenameTables> & TableName<TTo>];

type ValidateAddedColumnOperation<
  TFrom extends SchemaLike,
  TTo extends SchemaLike,
  TRenameTables,
  TTable extends MigratedTableName<TFrom, TTo, TRenameTables> & TableName<TTo>,
  TColumn extends AddedColumnName<TFrom, TTo, TRenameTables, TTable>,
  TMigrate,
> =
  TableOpFor<TMigrate, TTable, TColumn> extends infer TOperation
    ? [TOperation] extends [never]
      ? {
          readonly table: TTable;
          readonly column: TColumn;
          readonly problem: "Added columns must use m.add.*(...) or m.renameFrom(...)";
        }
      : TOperation extends AddOperationForBuilder<BuilderForTargetColumn<TTo, TTable, TColumn>>
        ? never
        : TOperation extends RenameOp<infer TOldName extends string>
          ? TOldName extends RemovedColumnName<TFrom, TTo, TRenameTables, TTable>
            ? BuildersEqual<
                BuilderForSourceColumn<TFrom, TTo, TRenameTables, TTable, TOldName>,
                BuilderForTargetColumn<TTo, TTable, TColumn>,
                TRenameTables
              > extends true
              ? never
              : {
                  readonly table: TTable;
                  readonly column: TColumn;
                  readonly renameFrom: TOldName;
                  readonly problem: "m.renameFrom(...) must point at a removed column with the same type";
                }
            : {
                readonly table: TTable;
                readonly column: TColumn;
                readonly renameFrom: TOldName;
                readonly problem: "m.renameFrom(...) must point at a removed column in the same table";
              }
          : {
              readonly table: TTable;
              readonly column: TColumn;
              readonly problem: "Added columns must use m.add.*(...) or m.renameFrom(...)";
            }
    : never;

type AddedColumnOperationErrors<
  TFrom extends SchemaLike,
  TTo extends SchemaLike,
  TRenameTables,
  TMigrate,
> = {
  [TTable in MigratedTableName<TFrom, TTo, TRenameTables> & TableName<TTo>]: {
    [TColumn in AddedColumnName<TFrom, TTo, TRenameTables, TTable>]: ValidateAddedColumnOperation<
      TFrom,
      TTo,
      TRenameTables,
      TTable,
      TColumn,
      TMigrate
    >;
  }[AddedColumnName<TFrom, TTo, TRenameTables, TTable>];
}[MigratedTableName<TFrom, TTo, TRenameTables> & TableName<TTo>];

type ValidateRemovedColumnOperation<
  TFrom extends SchemaLike,
  TTo extends SchemaLike,
  TRenameTables,
  TTable extends MigratedTableName<TFrom, TTo, TRenameTables> & TableName<TTo>,
  TColumn extends RemovedColumnName<TFrom, TTo, TRenameTables, TTable>,
  TMigrate,
> =
  TColumn extends RenameSourcesForTable<TableOpsFor<TMigrate, TTable>>
    ? TableOpFor<TMigrate, TTable, TColumn> extends never
      ? never
      : {
          readonly table: TTable;
          readonly column: TColumn;
          readonly problem: "Removed columns cannot be both dropped and renamed from";
        }
    : [TableOpFor<TMigrate, TTable, TColumn>] extends [never]
      ? {
          readonly table: TTable;
          readonly column: TColumn;
          readonly problem: "Removed columns must use m.drop.*(...) or be referenced by m.renameFrom(...)";
        }
      : [TableOpFor<TMigrate, TTable, TColumn>] extends [
            DropOperationForBuilder<
              BuilderForSourceColumn<TFrom, TTo, TRenameTables, TTable, TColumn>
            >,
          ]
        ? never
        : {
            readonly table: TTable;
            readonly column: TColumn;
            readonly problem: "Removed columns must use m.drop.*(...) or be referenced by m.renameFrom(...)";
          };

type RemovedColumnOperationErrors<
  TFrom extends SchemaLike,
  TTo extends SchemaLike,
  TRenameTables,
  TMigrate,
> = {
  [TTable in MigratedTableName<TFrom, TTo, TRenameTables> & TableName<TTo>]: {
    [TColumn in RemovedColumnName<
      TFrom,
      TTo,
      TRenameTables,
      TTable
    >]: ValidateRemovedColumnOperation<TFrom, TTo, TRenameTables, TTable, TColumn, TMigrate>;
  }[RemovedColumnName<TFrom, TTo, TRenameTables, TTable>];
}[MigratedTableName<TFrom, TTo, TRenameTables> & TableName<TTo>];

type UnsupportedSharedColumnChanges<
  TFrom extends SchemaLike,
  TTo extends SchemaLike,
  TRenameTables,
> = {
  [TTable in MigratedTableName<TFrom, TTo, TRenameTables> & TableName<TTo>]: {
    [TColumn in CommonColumnName<
      TTo,
      TTable,
      SourceColumnName<TFrom, TTo, TRenameTables, TTable>
    >]: SharedBuildersCompatible<
      BuilderForSourceColumn<TFrom, TTo, TRenameTables, TTable, TColumn>,
      BuilderForTargetColumn<TTo, TTable, TColumn>,
      TRenameTables
    > extends true
      ? never
      : {
          readonly table: TTable;
          readonly column: TColumn;
          readonly problem: "Columns with the same name must keep the same type, optionality, and ref target (an enum may only gain variants at the end)";
        };
  }[CommonColumnName<TTo, TTable, SourceColumnName<TFrom, TTo, TRenameTables, TTable>>];
}[MigratedTableName<TFrom, TTo, TRenameTables> & TableName<TTo>];

type MigrationValidationErrors<
  TFrom extends SchemaLike,
  TTo extends SchemaLike,
  TRenameTables,
  TCreateTables,
  TDropTables,
  TMigrate,
> =
  | (UnknownCreateTables<TFrom, TTo, TCreateTables> extends infer TUnknownAddedTable
      ? [TUnknownAddedTable] extends [never]
        ? never
        : {
            readonly table: Extract<TUnknownAddedTable, string>;
            readonly problem: "createTables may only mention target-only tables";
          }
      : never)
  | (UnknownDropTables<TFrom, TTo, TDropTables> extends infer TUnknownRemovedTable
      ? [TUnknownRemovedTable] extends [never]
        ? never
        : {
            readonly table: Extract<TUnknownRemovedTable, string>;
            readonly problem: "dropTables may only mention source-only tables";
          }
      : never)
  | (MissingCreateTables<TFrom, TTo, TRenameTables, TCreateTables> extends infer TMissingAddedTable
      ? [TMissingAddedTable] extends [never]
        ? never
        : {
            readonly table: Extract<TMissingAddedTable, string>;
            readonly problem: "Target-only tables must be covered by createTables or renameTables";
          }
      : never)
  | (MissingDropTables<TFrom, TTo, TRenameTables, TDropTables> extends infer TMissingRemovedTable
      ? [TMissingRemovedTable] extends [never]
        ? never
        : {
            readonly table: Extract<TMissingRemovedTable, string>;
            readonly problem: "Source-only tables must be covered by dropTables or renameTables";
          }
      : never)
  | (UnknownMigrationTables<TFrom, TTo, TRenameTables, TMigrate> extends infer TUnknownTable
      ? [TUnknownTable] extends [never]
        ? never
        : {
            readonly table: Extract<TUnknownTable, string>;
            readonly problem: "Migration only supports shared tables or target tables declared in renameTables";
          }
      : never)
  | UnknownMigrationColumns<TFrom, TTo, TRenameTables, TMigrate>
  | AddedColumnOperationErrors<TFrom, TTo, TRenameTables, TMigrate>
  | RemovedColumnOperationErrors<TFrom, TTo, TRenameTables, TMigrate>
  | UnsupportedSharedColumnChanges<TFrom, TTo, TRenameTables>;

type ValidateMigrationConfig<
  TFrom extends SchemaLike,
  TTo extends SchemaLike,
  TRenameTables,
  TCreateTables,
  TDropTables,
  TMigrate,
> = [
  MigrationValidationErrors<TFrom, TTo, TRenameTables, TCreateTables, TDropTables, TMigrate>,
] extends [never]
  ? unknown
  : {
      readonly __migrationValidationError__: "Migration definitions must cover every added or removed table and column";
      readonly __migrationErrors__: MigrationValidationErrors<
        TFrom,
        TTo,
        TRenameTables,
        TCreateTables,
        TDropTables,
        TMigrate
      >;
    };

/** A column step of a readable migration (SPEC §10.10). */
export type ReadableColumnStep =
  | { add: ReadableJson; default: ReadableJson }
  | { drop: ReadableJson; backwardsDefault: ReadableJson }
  | { renameFrom: string };

/** What a migration does, in `jazz.app.json`'s readable form (without `from` / `to`). */
export interface ReadableMigrationSteps {
  createTables?: string[];
  dropTables?: string[];
  renameTables?: Record<string, string>;
  migrate?: Record<string, Record<string, ReadableColumnStep>>;
}

export interface DefinedMigration<
  TFrom extends SchemaLike = SchemaLike,
  TTo extends SchemaLike = SchemaLike,
> {
  readonly __jazzMigration: true;
  readonly from: TFrom;
  readonly to: TTo;
  readonly fromHash?: string;
  readonly toHash?: string;
  /** The migration's steps, in readable form. */
  readonly steps: ReadableMigrationSteps;
}

/** `renameTables: { newName: m.renameTableFrom("oldName") }`. */
export function renameTableFrom<const TOldName extends string>(
  oldName: TOldName,
): RenameTableFromOp<TOldName> {
  return { _type: "renameTable", oldName };
}

type BuiltColumns = Record<string, Column>;

/** A witness table's stored columns, built, with forward relations' targets as `references`. */
export function witnessColumns(table: TableSource): BuiltColumns {
  const columns = (isDefinedTable(table) ? table.columns : table) as Record<
    string,
    AnyColumnDefinition
  >;
  const relations = isDefinedTable(table) ? table.relations : {};
  const out: BuiltColumns = {};
  for (const [name, builder] of Object.entries(columns)) {
    if (isComputedColumn(builder)) continue;
    const column = (builder as unknown as { _build(name: string): Column })._build(name);
    const forward = Object.values(relations).find(
      (relation) => relation.kind === "forward" && relation.column === name,
    );
    if (forward && !column.references) column.references = forward.table;
    out[name] = column;
  }
  return out;
}

function tableMarkers(
  kind: "createTables" | "dropTables",
  markers: unknown,
  present: Record<string, unknown>,
  absent: Record<string, unknown>,
  renamed: { has(table: string): boolean },
): string[] {
  if (markers === undefined) return [];
  // .64 wrote `{ table: true }`; an array of names is accepted too.
  const names = Array.isArray(markers)
    ? (markers as string[])
    : Object.entries(markers as Record<string, unknown>).map(([name, marker]) => {
        if (marker !== true) throw new Error(`${kind}.${name} must be true.`);
        return name;
      });
  const create = kind === "createTables";
  for (const name of names) {
    if (!(name in present))
      throw new Error(`${kind} references unknown ${create ? "target" : "source"} table ${name}.`);
    if (name in absent)
      throw new Error(
        create
          ? `createTables only supports target-only tables; ${name} already exists in the source schema.`
          : `dropTables only supports source-only tables; ${name} still exists in the target schema.`,
      );
    if (renamed.has(name))
      throw new Error(`Table ${name} cannot be both ${create ? "added" : "removed"} and renamed.`);
  }
  return names;
}

/** A type for a migration step: no `?` and no options (a lens carries no nullability). */
function stepType(sqlType: Column["sqlType"]): ReadableJson {
  return readableType(sqlType);
}

function buildSteps(config: {
  from: SchemaLike;
  to: SchemaLike;
  migrate?: unknown;
  renameTables?: unknown;
  createTables?: unknown;
  dropTables?: unknown;
}): ReadableMigrationSteps {
  const from = config.from as Record<string, TableSource>;
  const to = config.to as Record<string, TableSource>;
  if (!from || typeof from !== "object") throw new Error("defineMigration: `from` must be an object of tables.");
  if (!to || typeof to !== "object") throw new Error("defineMigration: `to` must be an object of tables.");

  const renames = new Map<string, string>();
  const usedSources = new Set<string>();
  for (const [target, op] of Object.entries(
    (config.renameTables ?? {}) as Record<string, RenameTableFromOp>,
  )) {
    if (!op || op._type !== "renameTable")
      throw new Error(`renameTables.${target} must be m.renameTableFrom("<old name>").`);
    if (!(target in to)) throw new Error(`Table rename references unknown target table ${target}.`);
    if (target in from)
      throw new Error(
        `Table rename target ${target} already exists in the source schema; renameTables only supports target-only tables.`,
      );
    if (!(op.oldName in from))
      throw new Error(`Table rename references unknown source table ${op.oldName}.`);
    if (op.oldName in to)
      throw new Error(
        `Table rename source ${op.oldName} still exists in the target schema; renameTables only supports source-only tables.`,
      );
    if (usedSources.has(op.oldName))
      throw new Error(`Table rename source ${op.oldName} is used more than once.`);
    usedSources.add(op.oldName);
    renames.set(target, op.oldName);
  }
  const created = tableMarkers("createTables", config.createTables, to, from, renames);
  const dropped = tableMarkers("dropTables", config.dropTables, from, to, usedSources);

  const migrate: Record<string, Record<string, ReadableColumnStep>> = {};
  for (const [table, rawOps] of Object.entries((config.migrate ?? {}) as Record<string, unknown>)) {
    if (!rawOps || typeof rawOps !== "object") continue;
    if (created.includes(table) || dropped.includes(table))
      throw new Error(
        `Table ${table} cannot have column operations when declared in createTables or dropTables.`,
      );
    const targetTable = to[table];
    const sourceName = renames.get(table) ?? table;
    const sourceTable = from[sourceName];
    const targetColumns = targetTable ? witnessColumns(targetTable) : {};
    const sourceColumns = sourceTable ? witnessColumns(sourceTable) : {};
    const steps: Record<string, ReadableColumnStep> = {};
    const renamedSources = new Set<string>();
    const droppedColumns = new Set<string>();
    for (const [column, op] of Object.entries(rawOps as Record<string, AddOp | DropOp | RenameOp>)) {
      switch (op?._type) {
        case "rename": {
          assertUserTableColumnNameAllowed(column);
          if (renamedSources.has(op.oldName))
            throw new Error(`Migration for ${table} renames ${op.oldName} more than once.`);
          if (droppedColumns.has(op.oldName))
            throw new Error(`Migration for ${table} cannot both drop and rename ${op.oldName}.`);
          renamedSources.add(op.oldName);
          steps[column] = { renameFrom: op.oldName };
          break;
        }
        case "add": {
          assertUserTableColumnNameAllowed(column);
          const target = targetColumns[column];
          if (!target)
            throw new Error(`Migration references unknown target column ${table}.${column}.`);
          steps[column] = {
            add: stepType(target.sqlType),
            default: readableValue(op.default, target.sqlType),
          };
          break;
        }
        case "drop": {
          if (renamedSources.has(column))
            throw new Error(`Migration for ${table} cannot both drop and rename ${column}.`);
          droppedColumns.add(column);
          const source = sourceColumns[column];
          if (!source)
            throw new Error(`Migration references unknown source column ${sourceName}.${column}.`);
          steps[column] = {
            drop: stepType(source.sqlType),
            backwardsDefault: readableValue(op.backwardsDefault, source.sqlType),
          };
          break;
        }
        default:
          throw new Error(
            `migrate.${table}.${column} must be m.add.<type>(...), m.drop.<type>(...) or m.renameFrom(...).`,
          );
      }
    }
    if (Object.keys(steps).length > 0) migrate[table] = steps;
  }
  return {
    ...(created.length ? { createTables: created } : {}),
    ...(dropped.length ? { dropTables: dropped } : {}),
    ...(renames.size ? { renameTables: Object.fromEntries(renames) } : {}),
    ...(Object.keys(migrate).length ? { migrate } : {}),
  };
}

export function isDefinedMigration(value: unknown): value is DefinedMigration {
  return (
    typeof value === "object" &&
    value !== null &&
    (value as { __jazzMigration?: unknown }).__jazzMigration === true
  );
}

/**
 * Define a migration between two schema versions: a bidirectional lens.
 * The backward direction is derived, so clients on the older schema still
 * read rows written under the new one.
 *
 * `jazz-tools migrations create` writes the stub; `jazz-tools generate`
 * puts it in `jazz.app.json`.
 *
 * @example
 * ```typescript
 * import { schema as s, migration as m } from "jazz-tools";
 *
 * export default m.defineMigration({
 *   migrate: {
 *     todos: {
 *       priority: m.add.enum("low", "medium", "high", { default: "medium" }),
 *     },
 *   },
 *   fromHash: "aaaaaaaaaaaa",
 *   toHash: "bbbbbbbbbbbb",
 *   from: {
 *     todos: s.table({ title: s.string(), done: s.boolean() }, {}),
 *   },
 *   to: {
 *     todos: s.table({ title: s.string(), done: s.boolean(), priority: s.enum("low", "medium", "high") }, {}),
 *   },
 * });
 * ```
 */
export function defineMigration<
  const TFrom extends SchemaLike,
  const TTo extends SchemaLike,
  const TRenameTables extends RenameTableShape<TFrom, TTo> | undefined = undefined,
  const TCreateTables extends AddedTableShape<TFrom, TTo> | undefined = undefined,
  const TDropTables extends RemovedTableShape<TFrom, TTo> | undefined = undefined,
  const TMigrate extends MigrationShape<TFrom, TTo, TRenameTables> | undefined = undefined,
>(
  config: {
    /** The source version's short schema hash. */
    fromHash?: string;
    /** The destination version's short schema hash. */
    toHash?: string;
    /** Tables from the source schema that are affected by this migration */
    from: TFrom;
    /** Tables from the destination schema that are affected by this migration */
    to: TTo;
    renameTables?: TRenameTables;
    createTables?: TCreateTables;
    dropTables?: TDropTables;
    migrate?: TMigrate;
  } & ValidateMigrationConfig<TFrom, TTo, TRenameTables, TCreateTables, TDropTables, TMigrate>,
): DefinedMigration<TFrom, TTo> {
  return Object.freeze({
    __jazzMigration: true as const,
    from: config.from,
    to: config.to,
    ...(config.fromHash === undefined ? {} : { fromHash: config.fromHash }),
    ...(config.toHash === undefined ? {} : { toHash: config.toHash }),
    steps: buildSteps(config),
  });
}
