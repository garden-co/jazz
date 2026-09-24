import { columnTypeSignature } from "./runtime/schema-metadata.js";
import { structuralValuesEqual } from "./runtime/structural-values.js";
import { sqlTypeToWasm } from "./codegen/schema-reader.js";
import { toValue } from "./runtime/value-converter.js";
import type {
  AnyTypedColumnBuilder,
  ColumnBuilderOptional,
  ColumnBuilderReferences,
  ColumnBuilderSqlType,
} from "./dsl.js";
import { assertUserTableColumnNameAllowed } from "./magic-columns.js";
import type {
  AddOp,
  DropOp,
  Lens,
  RenameOp,
  RenameTableFromOp,
  Schema as SchemaAst,
  TSTypeFromSqlType,
  TableLens,
} from "./schema.js";
import type {
  CompactSchema,
  Schema as AppSchema,
  SchemaDefinition,
  Simplify,
  TableDefinition,
} from "./typed-app.js";
import {
  DefinedTable,
  unwrapTableDefinition,
  withManagedEncryptionTables,
  definitionToSchema as compileSchemaDefinition,
} from "./typed-app.js";
import { encryptedTableToPhysical, equalityIndexColumn } from "./e2ee/encrypted-schema.js";

type SchemaLike = SchemaDefinition | AppSchema<any>;

type NormalizedSchema<TSchema extends SchemaLike> =
  TSchema extends AppSchema<infer TDefinition>
    ? CompactSchema<TDefinition>
    : TSchema extends SchemaDefinition
      ? CompactSchema<TSchema>
      : never;

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
> = NormalizedSchema<TTo>[TTable][TColumn];

type BuilderForSourceColumn<
  TFrom extends SchemaLike,
  TTo extends SchemaLike,
  TRenameTables,
  TTable extends MigratedTableName<TFrom, TTo, TRenameTables>,
  TColumn extends SourceColumnName<TFrom, TTo, TRenameTables, TTable>,
> = NormalizedSchema<TFrom>[SourceTableNameFor<TFrom, TTo, TRenameTables, TTable>][TColumn];

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

type BuilderIdentity<TBuilder extends AnyTypedColumnBuilder> = readonly [
  ColumnBuilderSqlType<TBuilder>,
  ColumnBuilderOptional<TBuilder>,
  ColumnBuilderReferences<TBuilder>,
];

type BuildersEqual<TLeft extends AnyTypedColumnBuilder, TRight extends AnyTypedColumnBuilder> = [
  BuilderIdentity<TLeft>,
] extends [BuilderIdentity<TRight>]
  ? [BuilderIdentity<TRight>] extends [BuilderIdentity<TLeft>]
    ? true
    : false
  : false;

type SharedBuildersCompatible<
  TLeft extends AnyTypedColumnBuilder,
  TRight extends AnyTypedColumnBuilder,
> =
  BuildersEqual<TLeft, TRight> extends true
    ? true
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
          readonly problem: "Added columns must use col.add.*(...) or col.renameFrom(...)";
        }
      : TOperation extends AddOperationForBuilder<BuilderForTargetColumn<TTo, TTable, TColumn>>
        ? never
        : TOperation extends RenameOp<infer TOldName extends string>
          ? TOldName extends RemovedColumnName<TFrom, TTo, TRenameTables, TTable>
            ? BuildersEqual<
                BuilderForSourceColumn<TFrom, TTo, TRenameTables, TTable, TOldName>,
                BuilderForTargetColumn<TTo, TTable, TColumn>
              > extends true
              ? never
              : {
                  readonly table: TTable;
                  readonly column: TColumn;
                  readonly renameFrom: TOldName;
                  readonly problem: "col.renameFrom(...) must point at a removed column with the same type";
                }
            : {
                readonly table: TTable;
                readonly column: TColumn;
                readonly renameFrom: TOldName;
                readonly problem: "col.renameFrom(...) must point at a removed column in the same table";
              }
          : {
              readonly table: TTable;
              readonly column: TColumn;
              readonly problem: "Added columns must use col.add.*(...) or col.renameFrom(...)";
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
          readonly problem: "Removed columns must use col.drop.*(...) or be referenced by col.renameFrom(...)";
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
            readonly problem: "Removed columns must use col.drop.*(...) or be referenced by col.renameFrom(...)";
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
      BuilderForTargetColumn<TTo, TTable, TColumn>
    > extends true
      ? never
      : {
          readonly table: TTable;
          readonly column: TColumn;
          readonly problem: "Columns with the same name must keep the same type, optionality, and ref target";
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

export interface DefinedMigration<
  TFrom extends SchemaLike = SchemaLike,
  TTo extends SchemaLike = SchemaLike,
> {
  readonly from: TFrom;
  readonly to: TTo;
  readonly fromHash?: string;
  readonly toHash?: string;
  readonly forward: Lens[];
}

function normalizeSchemaDefinition(
  definition: SchemaDefinition | AppSchema<any>,
): Record<string, TableDefinition | DefinedTable<TableDefinition>> {
  return Object.fromEntries(
    Object.entries(definition as SchemaDefinition).map(([tableName, tableDefinition]) => [
      tableName,
      tableDefinition as TableDefinition | DefinedTable<TableDefinition>,
    ]),
  );
}

function definitionToSchema(definition: SchemaDefinition): SchemaAst {
  const normalizedDefinition = withManagedEncryptionTables(normalizeSchemaDefinition(definition));
  const logical = compileSchemaDefinition(normalizedDefinition);
  return {
    tables: logical.tables.map((table) => {
      const tableDefinition = normalizedDefinition[table.name];
      return encryptedTableToPhysical(
        table,
        tableDefinition instanceof DefinedTable ? tableDefinition.encryption : undefined,
      );
    }),
  };
}

export function renameTableFrom<const TOldName extends string>(
  oldName: TOldName,
): RenameTableFromOp<TOldName> {
  return {
    _type: "renameTable",
    oldName,
  };
}

function buildRenameTableMap(
  renameTables: Record<string, RenameTableFromOp<string>> | undefined,
  fromDefinition: Record<string, TableDefinition>,
  toDefinition: Record<string, TableDefinition>,
): Map<string, string> {
  const map = new Map<string, string>();
  const usedSources = new Set<string>();

  if (!renameTables) {
    return map;
  }

  for (const [tableName, operation] of Object.entries(renameTables)) {
    if (!(tableName in toDefinition)) {
      throw new Error(`Table rename references unknown target table ${tableName}.`);
    }
    if (tableName in fromDefinition) {
      throw new Error(
        `Table rename target ${tableName} already exists in the source schema; renameTables only supports target-only tables.`,
      );
    }
    if (!(operation.oldName in fromDefinition)) {
      throw new Error(`Table rename references unknown source table ${operation.oldName}.`);
    }
    if (operation.oldName in toDefinition) {
      throw new Error(
        `Table rename source ${operation.oldName} still exists in the target schema; renameTables only supports source-only tables.`,
      );
    }
    if (usedSources.has(operation.oldName)) {
      throw new Error(`Table rename source ${operation.oldName} is used more than once.`);
    }

    usedSources.add(operation.oldName);
    map.set(tableName, operation.oldName);
  }

  return map;
}

function buildAddedTableSet(
  createTables: Record<string, true> | undefined,
  fromDefinition: Record<string, TableDefinition>,
  toDefinition: Record<string, TableDefinition>,
  renameTableMap: ReadonlyMap<string, string>,
): Set<string> {
  const set = new Set<string>();

  if (!createTables) {
    return set;
  }

  for (const [tableName, marker] of Object.entries(createTables)) {
    if (marker !== true) {
      throw new Error(`createTables.${tableName} must be true.`);
    }
    if (!(tableName in toDefinition)) {
      throw new Error(`createTables references unknown target table ${tableName}.`);
    }
    if (tableName in fromDefinition) {
      throw new Error(
        `createTables only supports target-only tables; ${tableName} already exists in the source schema.`,
      );
    }
    if (renameTableMap.has(tableName)) {
      throw new Error(`Table ${tableName} cannot be both added and renamed.`);
    }

    set.add(tableName);
  }

  return set;
}

function buildRemovedTableSet(
  dropTables: Record<string, true> | undefined,
  fromDefinition: Record<string, TableDefinition>,
  toDefinition: Record<string, TableDefinition>,
  renamedSources: ReadonlySet<string>,
): Set<string> {
  const set = new Set<string>();

  if (!dropTables) {
    return set;
  }

  for (const [tableName, marker] of Object.entries(dropTables)) {
    if (marker !== true) {
      throw new Error(`dropTables.${tableName} must be true.`);
    }
    if (!(tableName in fromDefinition)) {
      throw new Error(`dropTables references unknown source table ${tableName}.`);
    }
    if (tableName in toDefinition) {
      throw new Error(
        `dropTables only supports source-only tables; ${tableName} still exists in the target schema.`,
      );
    }
    if (renamedSources.has(tableName)) {
      throw new Error(`Table ${tableName} cannot be both removed and renamed.`);
    }

    set.add(tableName);
  }

  return set;
}

function columnShapeSignature(builder: AnyTypedColumnBuilder, omitReference = false): string {
  const column = builder._build("__migration_shape__");
  return JSON.stringify({
    sqlType: columnTypeSignature(sqlTypeToWasm(column.sqlType)),
    nullable: column.nullable,
    references: omitReference ? null : (column.references ?? null),
  });
}

function columnDefaultsEqual(left: AnyTypedColumnBuilder, right: AnyTypedColumnBuilder): boolean {
  const source = left._build("__migration_shape__"),
    target = right._build("__migration_shape__");
  if (source.default === undefined || target.default === undefined)
    return source.default === target.default;
  return structuralValuesEqual(
    toValue(source.default, sqlTypeToWasm(source.sqlType)),
    toValue(target.default, sqlTypeToWasm(target.sqlType)),
  );
}

function columnMetadataEqual(left: AnyTypedColumnBuilder, right: AnyTypedColumnBuilder): boolean {
  const source = left._build("__migration_shape__"),
    target = right._build("__migration_shape__");
  return source.mergeStrategy === target.mergeStrategy && columnDefaultsEqual(left, right);
}

function encryptionShapeSignature(
  definition: TableDefinition | DefinedTable<TableDefinition>,
  columnRenames?: ReadonlyMap<string, string>,
  tableRenames?: ReadonlyMap<string, string>,
): string | undefined {
  if (!(definition instanceof DefinedTable) || !definition.encryption) return undefined;
  const { space, columns, indexes } = definition.encryption;
  const scope = Object.values(definition.relations).find(
    (relation) => relation.kind === "forward" && relation.column === space,
  )!.table;
  const originalName = (name: string) => columnRenames?.get(name) ?? name;
  return JSON.stringify({
    space: [originalName(space), tableRenames?.get(scope) ?? scope],
    columns: columns
      .map((name) => [originalName(name), columnShapeSignature(definition.columns[name]!)])
      .sort(([left], [right]) => left!.localeCompare(right!)),
    indexes: Object.keys(indexes ?? {})
      .map((name) => [originalName(name), indexes![name]])
      .sort(([left], [right]) => left!.localeCompare(right!)),
  });
}

function tableMatchesAfterApplyingColumnOperations(
  sourceTable: Record<string, AnyTypedColumnBuilder>,
  targetTable: Record<string, AnyTypedColumnBuilder>,
  tableOps: Record<string, AddOp | DropOp | RenameOp>,
  allowReferenceAdditions = false,
): boolean {
  const transformed = new Map<string, AnyTypedColumnBuilder>(Object.entries(sourceTable));

  for (const [columnName, operation] of Object.entries(tableOps)) {
    switch (operation._type) {
      case "rename": {
        const builder = transformed.get(operation.oldName);
        if (!builder) {
          return false;
        }
        if (columnName !== operation.oldName && transformed.has(columnName)) {
          return false;
        }
        transformed.delete(operation.oldName);
        transformed.set(columnName, builder);
        break;
      }
      case "add": {
        const builder = targetTable[columnName];
        if (!builder || transformed.has(columnName)) {
          return false;
        }
        transformed.set(columnName, builder);
        break;
      }
      case "drop": {
        if (!transformed.delete(columnName)) {
          return false;
        }
        break;
      }
    }
  }

  const targetEntries = Object.entries(targetTable);
  if (transformed.size !== targetEntries.length) {
    return false;
  }

  for (const [columnName, targetBuilder] of targetEntries) {
    const sourceBuilder = transformed.get(columnName);
    if (!sourceBuilder) {
      return false;
    }
    if (allowReferenceAdditions && !columnMetadataEqual(sourceBuilder, targetBuilder)) return false;
    const omitReference = allowReferenceAdditions && !sourceBuilder._build(columnName).references;
    if (
      columnShapeSignature(sourceBuilder, omitReference) !==
      columnShapeSignature(targetBuilder, omitReference)
    ) {
      return false;
    }
  }

  return true;
}

function unwrapSchemaTables<TSchema extends SchemaLike>(
  definition: NormalizedSchema<TSchema>,
): Record<string, Record<string, AnyTypedColumnBuilder>> {
  const ast = compileSchemaDefinition(definition as SchemaDefinition);
  return Object.fromEntries(
    Object.entries(definition).map(([tableName, tableDefinition]) => {
      const columns = unwrapTableDefinition(
        tableDefinition as TableDefinition | DefinedTable<TableDefinition>,
      );
      const lowered = ast.tables.find((table) => table.name === tableName)!;
      return [
        tableName,
        Object.fromEntries(
          Object.entries(columns).map(([name, builder]) => {
            const references = lowered.columns.find((column) => column.name === name)?.references;
            // Preserve the caller's builder and methods while presenting its effective stored reference
            // metadata to the existing column-shape comparison (including renamed columns).
            const effective = Object.create(builder) as AnyTypedColumnBuilder;
            effective._build = (columnName: string) => ({
              ...builder._build(columnName),
              references,
            });
            return [name, effective];
          }),
        ),
      ];
    }),
  );
}

function buildForwardLenses<
  TFrom extends SchemaLike,
  TTo extends SchemaLike,
  TRenameTables extends RenameTableShape<TFrom, TTo> | undefined,
  TCreateTables extends AddedTableShape<TFrom, TTo> | undefined,
  TDropTables extends RemovedTableShape<TFrom, TTo> | undefined,
>(
  migrate: MigrationShape<TFrom, TTo, TRenameTables> | undefined,
  renameTables: TRenameTables | undefined,
  createTables: TCreateTables | undefined,
  dropTables: TDropTables | undefined,
  fromDefinition: NormalizedSchema<TFrom>,
  toDefinition: NormalizedSchema<TTo>,
): Lens[] {
  const renameTableMap = buildRenameTableMap(
    renameTables as Record<string, RenameTableFromOp<string>> | undefined,
    fromDefinition as Record<string, TableDefinition>,
    toDefinition as Record<string, TableDefinition>,
  );
  const renamedSources = new Set(renameTableMap.values());
  const addedTableSet = buildAddedTableSet(
    createTables as Record<string, true> | undefined,
    fromDefinition as Record<string, TableDefinition>,
    toDefinition as Record<string, TableDefinition>,
    renameTableMap,
  );
  const removedTableSet = buildRemovedTableSet(
    dropTables as Record<string, true> | undefined,
    fromDefinition as Record<string, TableDefinition>,
    toDefinition as Record<string, TableDefinition>,
    renamedSources,
  );

  // Check before the empty-lens shortcut: encryption is client-side metadata,
  // not a server lens capable of converting existing values.
  for (const [tableName, target] of Object.entries(toDefinition)) {
    const source = (fromDefinition as SchemaDefinition)[renameTableMap.get(tableName) ?? tableName];
    if (!source) continue;
    const columnRenames = new Map<string, string>();
    const operations = (
      migrate as Record<string, Record<string, AddOp | DropOp | RenameOp>> | undefined
    )?.[tableName];
    for (const [name, operation] of Object.entries(operations ?? {})) {
      if (operation._type === "rename") columnRenames.set(name, operation.oldName);
    }
    if (
      encryptionShapeSignature(source) !==
      encryptionShapeSignature(
        target as TableDefinition | DefinedTable<TableDefinition>,
        columnRenames,
        renameTableMap,
      )
    ) {
      throw new Error(
        `Migration for ${tableName} changes encryption; convert existing data through an authorised client instead.`,
      );
    }
  }
  const sourceTables = unwrapSchemaTables(fromDefinition);
  const targetTables = unwrapSchemaTables(toDefinition);
  const referenceAdditionTables = new Set<string>();
  for (const [targetName, targetColumns] of Object.entries(targetTables)) {
    const sourceName = renameTableMap.get(targetName) ?? targetName;
    const sourceColumns = sourceTables[sourceName];
    if (!sourceColumns || addedTableSet.has(targetName)) continue;
    const operations =
      (migrate as Record<string, Record<string, AddOp | DropOp | RenameOp>> | undefined)?.[
        targetName
      ] ?? {};
    for (const [columnName, targetBuilder] of Object.entries(targetColumns)) {
      const operation = operations[columnName];
      if (operation?._type === "add") continue;
      const sourceColumn = operation?._type === "rename" ? operation.oldName : columnName;
      const sourceBuilder = sourceColumns[sourceColumn];
      if (!sourceBuilder) continue;
      const source = sourceBuilder._build(sourceColumn);
      const target = targetBuilder._build(columnName);
      if (source.references === target.references) continue;
      if (
        !source.references &&
        target.references &&
        !operation &&
        !renameTableMap.has(targetName) &&
        columnShapeSignature(sourceBuilder, true) === columnShapeSignature(targetBuilder, true) &&
        columnMetadataEqual(sourceBuilder, targetBuilder)
      ) {
        if (!targetTables[target.references]) {
          throw new Error(
            `Reference addition for "${targetName}.${columnName}" requires target table "${target.references}" in the destination schema witness.`,
          );
        }
        referenceAdditionTables.add(targetName);
      } else {
        throw new Error(
          `Column "${targetName}.${columnName}" must keep the same reference target in a migration unless adding a reference to an unchanged column.`,
        );
      }
    }
  }

  if (
    !migrate &&
    referenceAdditionTables.size === 0 &&
    renameTableMap.size === 0 &&
    addedTableSet.size === 0 &&
    removedTableSet.size === 0
  ) {
    return [];
  }

  const forward: Lens[] = [];
  const orderedTableNames = [
    ...new Set([
      ...Object.keys(createTables ?? {}),
      ...Object.keys(dropTables ?? {}),
      ...Object.keys(renameTables ?? {}),
      ...Object.keys(migrate ?? {}),
      ...referenceAdditionTables,
    ]),
  ];
  for (const tableName of orderedTableNames) {
    const added = addedTableSet.has(tableName) ? true : undefined;
    const removed = removedTableSet.has(tableName) ? true : undefined;
    const renamedFrom = renameTableMap.get(tableName);
    const rawTableOps = (migrate as Record<string, unknown> | undefined)?.[tableName];
    const tableOps =
      rawTableOps && typeof rawTableOps === "object"
        ? (rawTableOps as Record<string, AddOp | DropOp | RenameOp>)
        : {};
    const operationEntries = Object.entries(tableOps);

    if (added && removed) {
      throw new Error(`Table ${tableName} cannot be both added and removed.`);
    }
    if ((added || removed) && renamedFrom) {
      throw new Error(
        `Table ${tableName} cannot be combined with both table markers and renameTables.`,
      );
    }
    if ((added || removed) && operationEntries.length > 0) {
      throw new Error(
        `Table ${tableName} cannot have column operations when declared in createTables or dropTables.`,
      );
    }

    const operations: TableLens["operations"] = [];
    const renamedSources = new Set<string>();
    const droppedColumns = new Set<string>();
    const sourceTableName = renamedFrom ?? tableName;
    const sourceDefinition = (fromDefinition as SchemaDefinition)[sourceTableName];
    const sourceEncryption =
      sourceDefinition instanceof DefinedTable ? sourceDefinition.encryption : undefined;

    for (const [columnName, operation] of operationEntries) {
      switch (operation._type) {
        case "rename": {
          assertUserTableColumnNameAllowed(columnName);
          if (renamedSources.has(operation.oldName)) {
            throw new Error(
              `Migration for ${tableName} renames ${operation.oldName} more than once.`,
            );
          }
          if (droppedColumns.has(operation.oldName)) {
            throw new Error(
              `Migration for ${tableName} cannot both drop and rename ${operation.oldName}.`,
            );
          }
          renamedSources.add(operation.oldName);
          operations.push({
            type: "rename",
            column: operation.oldName,
            value: columnName,
          });
          if (sourceEncryption?.indexes?.[operation.oldName]) {
            operations.push({
              type: "rename",
              column: equalityIndexColumn(operation.oldName),
              value: equalityIndexColumn(columnName),
            });
          }
          break;
        }
        case "add": {
          assertUserTableColumnNameAllowed(columnName);
          const builder = targetTables[tableName]?.[columnName];
          if (!builder) {
            throw new Error(
              `Migration references unknown target column ${tableName}.${columnName}.`,
            );
          }
          operations.push({
            type: "introduce",
            column: columnName,
            sqlType: builder._sqlType,
            value: operation.default,
          });
          break;
        }
        case "drop": {
          if (renamedSources.has(columnName)) {
            throw new Error(
              `Migration for ${tableName} cannot both drop and rename ${columnName}.`,
            );
          }
          droppedColumns.add(columnName);
          const builder = sourceTables[sourceTableName]?.[columnName];
          if (!builder) {
            throw new Error(
              `Migration references unknown source column ${sourceTableName}.${columnName}.`,
            );
          }
          operations.push({
            type: "drop",
            column: columnName,
            sqlType: builder._sqlType,
            value: operation.backwardsDefault,
          });
          break;
        }
      }
    }

    if (
      referenceAdditionTables.has(tableName) &&
      !tableMatchesAfterApplyingColumnOperations(
        sourceTables[sourceTableName]!,
        targetTables[tableName]!,
        tableOps,
        true,
      )
    ) {
      throw new Error(
        `Reference additions for table "${tableName}" require unchanged column shapes after applying column migrations.`,
      );
    }

    if (renamedFrom) {
      const sourceTable = sourceTables[sourceTableName];
      const targetTable = targetTables[tableName];

      if (!sourceTable || !targetTable) {
        throw new Error(
          `Table rename ${sourceTableName} -> ${tableName} references a missing source or target table.`,
        );
      }

      if (!tableMatchesAfterApplyingColumnOperations(sourceTable, targetTable, tableOps)) {
        throw new Error(
          `Table rename ${sourceTableName} -> ${tableName} does not match the target table after applying its column migrations.`,
        );
      }
    }

    if (
      added ||
      removed ||
      renamedFrom ||
      operations.length > 0 ||
      referenceAdditionTables.has(tableName)
    ) {
      forward.push({
        table: tableName,
        added,
        removed,
        renamedFrom,
        operations,
      });
    }
  }

  return forward;
}

export function schemaDefinitionToAst(definition: SchemaDefinition | AppSchema<any>): SchemaAst {
  return definitionToSchema(definition as SchemaDefinition);
}

/**
 * Create a new migration lens: a bidirectional transformation between two schema versions.
 * The forward direction applies the migration; the backward direction is generated automatically
 * so older clients can still read data written under the new schema.
 *
 * Migration stubs can be generated with the `jazz-tools@alpha migrations create` command
 * and published with the `jazz-tools@alpha deploy` command.
 *
 * @example
 * ```typescript
 * export default s.defineMigration({
 *   migrate: {
 *     todos: {
 *       priority: s.add.enum("low", "medium", "high", { default: "medium" }),
 *     },
 *   },
 *   fromHash: "aaaaaaaaaaaa",
 *   toHash: "bbbbbbbbbbbb",
 *   from: {
 *     todos: s.table({
 *       title: s.string(),
 *       done: s.boolean(),
 *     }, {}),
 *   },
 *   to: {
 *     todos: s.table({
 *       title: s.string(),
 *       done: s.boolean(),
 *       priority: s.enum("low", "medium", "high"),
 *     }, {}),
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
    /**
     * Optional hash witness for the source schema.
     */
    fromHash?: string;
    /**
     * Optional hash witness for the destination schema.
     */
    toHash?: string;
    /**
     * Tables from the source schema that are affected by this migration
     */
    from: TFrom;
    /**
     * Tables from the destination schema that are affected by this migration
     */
    to: TTo;
    renameTables?: TRenameTables;
    createTables?: TCreateTables;
    dropTables?: TDropTables;
    migrate?: TMigrate;
  } & ValidateMigrationConfig<TFrom, TTo, TRenameTables, TCreateTables, TDropTables, TMigrate>,
): DefinedMigration<TFrom, TTo> {
  const fromDefinition = normalizeSchemaDefinition(
    config.from as SchemaDefinition,
  ) as NormalizedSchema<TFrom>;
  const toDefinition = normalizeSchemaDefinition(
    config.to as SchemaDefinition,
  ) as NormalizedSchema<TTo>;

  const forward = buildForwardLenses(
    config.migrate,
    config.renameTables,
    config.createTables,
    config.dropTables,
    fromDefinition,
    toDefinition,
  );

  return {
    from: config.from,
    to: config.to,
    fromHash: config.fromHash,
    toHash: config.toHash,
    forward,
  };
}
