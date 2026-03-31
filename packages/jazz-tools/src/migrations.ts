import type {
  AnyTypedColumnBuilder,
  ColumnBuilderOptional,
  ColumnBuilderReferences,
  ColumnBuilderSqlType,
} from "./dsl.js";
import { assertUserColumnNameAllowed } from "./magic-columns.js";
import type {
  AddOp,
  DropOp,
  Lens,
  RenameOp,
  RenameTableFromOp,
  Schema as SchemaAst,
  Table as SchemaAstTable,
  TSTypeFromSqlType,
  TableLens,
} from "./schema.js";
import type {
  CompactSchema,
  DefinedTable,
  Schema as AppSchema,
  SchemaDefinition,
  Simplify,
  TableDefinition,
} from "./typed-app.js";
import { unwrapTableDefinition } from "./typed-app.js";

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

type RenameTables<TRenameTables> =
  NonNullable<TRenameTables> extends Record<string, unknown> ? NonNullable<TRenameTables> : {};

type RenamedTargetTableName<TRenameTables> = Extract<keyof RenameTables<TRenameTables>, string>;

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

type MigrationTables<TMigrate> =
  NonNullable<TMigrate> extends Record<string, unknown> ? NonNullable<TMigrate> : {};

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
    >]: BuildersEqual<
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
  TMigrate,
> =
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
  TMigrate,
> = [MigrationValidationErrors<TFrom, TTo, TRenameTables, TMigrate>] extends [never]
  ? unknown
  : {
      readonly __migrationValidationError__: "Migration definitions must cover every added or removed column";
      readonly __migrationErrors__: MigrationValidationErrors<TFrom, TTo, TRenameTables, TMigrate>;
    };

export interface DefinedMigration<
  TFrom extends SchemaLike = SchemaLike,
  TTo extends SchemaLike = SchemaLike,
> {
  readonly fromHash: string;
  readonly toHash: string;
  readonly from: TFrom;
  readonly to: TTo;
  readonly forward: Lens[];
}

function tableDefinitionToAst(
  tableName: string,
  definition: TableDefinition | DefinedTable<TableDefinition>,
): SchemaAstTable {
  const columnsDefinition = unwrapTableDefinition(definition);
  return {
    name: tableName,
    columns: Object.entries(columnsDefinition).map(([columnName, builder]) => {
      assertUserColumnNameAllowed(columnName);
      return builder._build(columnName);
    }),
  };
}

function normalizeSchemaDefinition(
  definition: SchemaDefinition | AppSchema<any>,
): Record<string, TableDefinition> {
  return Object.fromEntries(
    Object.entries(definition as SchemaDefinition).map(([tableName, tableDefinition]) => [
      tableName,
      unwrapTableDefinition(tableDefinition as TableDefinition | DefinedTable<TableDefinition>),
    ]),
  );
}

function definitionToSchema(definition: SchemaDefinition): SchemaAst {
  const normalizedDefinition = normalizeSchemaDefinition(definition);
  return {
    tables: Object.entries(normalizedDefinition).map(([tableName, tableDefinition]) =>
      tableDefinitionToAst(tableName, tableDefinition),
    ),
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

function buildForwardLenses<
  TFrom extends SchemaLike,
  TTo extends SchemaLike,
  TRenameTables extends RenameTableShape<TFrom, TTo> | undefined,
>(
  migrate: MigrationShape<TFrom, TTo, TRenameTables> | undefined,
  renameTables: TRenameTables | undefined,
  fromDefinition: NormalizedSchema<TFrom>,
  toDefinition: NormalizedSchema<TTo>,
): Lens[] {
  const renameTableMap = buildRenameTableMap(
    renameTables as Record<string, RenameTableFromOp<string>> | undefined,
    fromDefinition as Record<string, TableDefinition>,
    toDefinition as Record<string, TableDefinition>,
  );
  if (!migrate && renameTableMap.size === 0) {
    return [];
  }

  const forward: Lens[] = [];
  const orderedTableNames = [
    ...new Set([
      ...Object.keys((renameTables ?? {}) as Record<string, unknown>),
      ...Object.keys((migrate ?? {}) as Record<string, unknown>),
    ]),
  ];
  const sourceTables = fromDefinition as Record<string, Record<string, AnyTypedColumnBuilder>>;
  const targetTables = toDefinition as Record<string, Record<string, AnyTypedColumnBuilder>>;

  for (const tableName of orderedTableNames) {
    const renamedFrom = renameTableMap.get(tableName);
    const rawTableOps = (migrate as Record<string, unknown> | undefined)?.[tableName];
    const tableOps =
      rawTableOps && typeof rawTableOps === "object"
        ? (rawTableOps as Record<string, AddOp | DropOp | RenameOp>)
        : {};
    const operations: TableLens["operations"] = [];
    const renamedSources = new Set<string>();
    const droppedColumns = new Set<string>();
    const sourceTableName = renamedFrom ?? tableName;

    for (const [columnName, operation] of Object.entries(tableOps)) {
      switch (operation._type) {
        case "rename": {
          assertUserColumnNameAllowed(columnName);
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
          break;
        }
        case "add": {
          assertUserColumnNameAllowed(columnName);
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

    if (renamedFrom || operations.length > 0) {
      forward.push({
        table: tableName,
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

export function defineMigration<
  const TFrom extends SchemaLike,
  const TTo extends SchemaLike,
  const TRenameTables extends RenameTableShape<TFrom, TTo> | undefined = undefined,
  const TMigrate extends MigrationShape<TFrom, TTo, TRenameTables> | undefined = undefined,
>(
  config: {
    fromHash: string;
    toHash: string;
    from: TFrom;
    to: TTo;
    renameTables?: TRenameTables;
    migrate?: TMigrate;
  } & ValidateMigrationConfig<TFrom, TTo, TRenameTables, TMigrate>,
): DefinedMigration<TFrom, TTo> {
  const fromDefinition = normalizeSchemaDefinition(
    config.from as SchemaDefinition,
  ) as NormalizedSchema<TFrom>;
  const toDefinition = normalizeSchemaDefinition(
    config.to as SchemaDefinition,
  ) as NormalizedSchema<TTo>;

  return {
    fromHash: config.fromHash,
    toHash: config.toHash,
    from: config.from,
    to: config.to,
    forward: buildForwardLenses(config.migrate, config.renameTables, fromDefinition, toDefinition),
  };
}
