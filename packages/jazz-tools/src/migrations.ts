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
  Schema as SchemaAst,
  Table as SchemaAstTable,
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

type SchemaLike = SchemaDefinition | AppSchema<any>;

type NormalizedSchema<TSchema extends SchemaLike> =
  TSchema extends AppSchema<infer TDefinition>
    ? CompactSchema<TDefinition>
    : TSchema extends SchemaDefinition
      ? CompactSchema<TSchema>
      : never;

type TableName<TSchema extends SchemaLike> = Extract<keyof NormalizedSchema<TSchema>, string>;
type SharedTableName<TFrom extends SchemaLike, TTo extends SchemaLike> = Extract<
  TableName<TFrom>,
  TableName<TTo>
>;
type ColumnName<TSchema extends SchemaLike, TTable extends TableName<TSchema>> = Extract<
  keyof NormalizedSchema<TSchema>[TTable],
  string
>;
type CommonColumnName<
  TFrom extends SchemaLike,
  TTo extends SchemaLike,
  TTable extends SharedTableName<TFrom, TTo>,
> = Extract<ColumnName<TFrom, TTable>, ColumnName<TTo, TTable>>;

type BuilderForColumn<
  TSchema extends SchemaLike,
  TTable extends TableName<TSchema>,
  TColumn extends ColumnName<TSchema, TTable>,
> = NormalizedSchema<TSchema>[TTable][TColumn];

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
  TTable extends SharedTableName<TFrom, TTo>,
> = Exclude<ColumnName<TTo, TTable>, ColumnName<TFrom, TTable>>;

type RemovedColumnName<
  TFrom extends SchemaLike,
  TTo extends SchemaLike,
  TTable extends SharedTableName<TFrom, TTo>,
> = Exclude<ColumnName<TFrom, TTable>, ColumnName<TTo, TTable>>;

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
  TTable extends SharedTableName<TFrom, TTo>,
> = Simplify<
  {
    [TColumn in AddedColumnName<TFrom, TTo, TTable>]?:
      | AddOperationForBuilder<BuilderForColumn<TTo, TTable, TColumn>>
      | RenameOp<RemovedColumnName<TFrom, TTo, TTable>>;
  } & {
    [TColumn in RemovedColumnName<TFrom, TTo, TTable>]?: DropOperationForBuilder<
      BuilderForColumn<TFrom, TTable, TColumn>
    >;
  }
>;

export type MigrationShape<TFrom extends SchemaLike, TTo extends SchemaLike> = Simplify<{
  [TTable in SharedTableName<TFrom, TTo>]?: MigrationTableShape<TFrom, TTo, TTable>;
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

type UnknownMigrationTables<TFrom extends SchemaLike, TTo extends SchemaLike, TMigrate> = Exclude<
  Extract<keyof MigrationTables<TMigrate>, string>,
  SharedTableName<TFrom, TTo>
>;

type UnknownMigrationColumns<TFrom extends SchemaLike, TTo extends SchemaLike, TMigrate> = {
  [TTable in SharedTableName<TFrom, TTo>]: Exclude<
    Extract<keyof TableOpsFor<TMigrate, TTable>, string>,
    AddedColumnName<TFrom, TTo, TTable> | RemovedColumnName<TFrom, TTo, TTable>
  > extends infer TUnknownColumn
    ? [TUnknownColumn] extends [never]
      ? never
      : {
          readonly table: TTable;
          readonly column: Extract<TUnknownColumn, string>;
          readonly problem: "Migration tables may only mention added or removed columns";
        }
    : never;
}[SharedTableName<TFrom, TTo>];

type ValidateAddedColumnOperation<
  TFrom extends SchemaLike,
  TTo extends SchemaLike,
  TTable extends SharedTableName<TFrom, TTo>,
  TColumn extends AddedColumnName<TFrom, TTo, TTable>,
  TMigrate,
> =
  TableOpFor<TMigrate, TTable, TColumn> extends infer TOperation
    ? [TOperation] extends [never]
      ? {
          readonly table: TTable;
          readonly column: TColumn;
          readonly problem: "Added columns must use col.add.*(...) or col.renameFrom(...)";
        }
      : TOperation extends AddOperationForBuilder<BuilderForColumn<TTo, TTable, TColumn>>
        ? never
        : TOperation extends RenameOp<infer TOldName extends string>
          ? TOldName extends RemovedColumnName<TFrom, TTo, TTable>
            ? BuildersEqual<
                BuilderForColumn<TFrom, TTable, TOldName>,
                BuilderForColumn<TTo, TTable, TColumn>
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

type AddedColumnOperationErrors<TFrom extends SchemaLike, TTo extends SchemaLike, TMigrate> = {
  [TTable in SharedTableName<TFrom, TTo>]: {
    [TColumn in AddedColumnName<TFrom, TTo, TTable>]: ValidateAddedColumnOperation<
      TFrom,
      TTo,
      TTable,
      TColumn,
      TMigrate
    >;
  }[AddedColumnName<TFrom, TTo, TTable>];
}[SharedTableName<TFrom, TTo>];

type ValidateRemovedColumnOperation<
  TFrom extends SchemaLike,
  TTo extends SchemaLike,
  TTable extends SharedTableName<TFrom, TTo>,
  TColumn extends RemovedColumnName<TFrom, TTo, TTable>,
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
            DropOperationForBuilder<BuilderForColumn<TFrom, TTable, TColumn>>,
          ]
        ? never
        : {
            readonly table: TTable;
            readonly column: TColumn;
            readonly problem: "Removed columns must use col.drop.*(...) or be referenced by col.renameFrom(...)";
          };

type RemovedColumnOperationErrors<TFrom extends SchemaLike, TTo extends SchemaLike, TMigrate> = {
  [TTable in SharedTableName<TFrom, TTo>]: {
    [TColumn in RemovedColumnName<TFrom, TTo, TTable>]: ValidateRemovedColumnOperation<
      TFrom,
      TTo,
      TTable,
      TColumn,
      TMigrate
    >;
  }[RemovedColumnName<TFrom, TTo, TTable>];
}[SharedTableName<TFrom, TTo>];

type UnsupportedSharedColumnChanges<TFrom extends SchemaLike, TTo extends SchemaLike> = {
  [TTable in SharedTableName<TFrom, TTo>]: {
    [TColumn in CommonColumnName<TFrom, TTo, TTable>]: BuildersEqual<
      BuilderForColumn<TFrom, TTable, TColumn>,
      BuilderForColumn<TTo, TTable, TColumn>
    > extends true
      ? never
      : {
          readonly table: TTable;
          readonly column: TColumn;
          readonly problem: "Columns with the same name must keep the same type, optionality, and ref target";
        };
  }[CommonColumnName<TFrom, TTo, TTable>];
}[SharedTableName<TFrom, TTo>];

type MigrationValidationErrors<TFrom extends SchemaLike, TTo extends SchemaLike, TMigrate> =
  | (UnknownMigrationTables<TFrom, TTo, TMigrate> extends infer TUnknownTable
      ? [TUnknownTable] extends [never]
        ? never
        : {
            readonly table: Extract<TUnknownTable, string>;
            readonly problem: "Migration only supports tables present in both from and to";
          }
      : never)
  | UnknownMigrationColumns<TFrom, TTo, TMigrate>
  | AddedColumnOperationErrors<TFrom, TTo, TMigrate>
  | RemovedColumnOperationErrors<TFrom, TTo, TMigrate>
  | UnsupportedSharedColumnChanges<TFrom, TTo>;

type ValidateMigrationConfig<TFrom extends SchemaLike, TTo extends SchemaLike, TMigrate> = [
  MigrationValidationErrors<TFrom, TTo, TMigrate>,
] extends [never]
  ? unknown
  : {
      readonly __migrationValidationError__: "Migration definitions must cover every added or removed column";
      readonly __migrationErrors__: MigrationValidationErrors<TFrom, TTo, TMigrate>;
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

function tableDefinitionToAst(tableName: string, definition: TableDefinition): SchemaAstTable {
  return {
    name: tableName,
    columns: Object.entries(definition).map(([columnName, builder]) => {
      assertUserColumnNameAllowed(columnName);
      return builder._build(columnName);
    }),
  };
}

function definitionToSchema(definition: SchemaDefinition): SchemaAst {
  return {
    tables: Object.entries(definition).map(([tableName, tableDefinition]) =>
      tableDefinitionToAst(tableName, tableDefinition),
    ),
  };
}

function buildForwardLenses<TFrom extends SchemaLike, TTo extends SchemaLike>(
  migrate: MigrationShape<TFrom, TTo> | undefined,
  fromDefinition: NormalizedSchema<TFrom>,
  toDefinition: NormalizedSchema<TTo>,
): Lens[] {
  if (!migrate) {
    return [];
  }

  const forward: Lens[] = [];

  for (const [tableName, rawTableOps] of Object.entries(migrate)) {
    if (!rawTableOps || typeof rawTableOps !== "object") {
      continue;
    }

    const tableOps = rawTableOps as Record<string, AddOp | DropOp | RenameOp>;
    const operations: TableLens["operations"] = [];
    const renamedSources = new Set<string>();
    const droppedColumns = new Set<string>();

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
          const builder = (toDefinition as Record<string, Record<string, AnyTypedColumnBuilder>>)[
            tableName
          ]?.[columnName];
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
          const builder = (fromDefinition as Record<string, Record<string, AnyTypedColumnBuilder>>)[
            tableName
          ]?.[columnName];
          if (!builder) {
            throw new Error(
              `Migration references unknown source column ${tableName}.${columnName}.`,
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

    if (operations.length > 0) {
      forward.push({
        table: tableName,
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
  const TMigrate extends MigrationShape<TFrom, TTo> | undefined = undefined,
>(
  config: {
    fromHash: string;
    toHash: string;
    from: TFrom;
    to: TTo;
    migrate?: TMigrate;
  } & ValidateMigrationConfig<TFrom, TTo, TMigrate>,
): DefinedMigration<TFrom, TTo> {
  const fromDefinition = config.from as unknown as NormalizedSchema<TFrom>;
  const toDefinition = config.to as unknown as NormalizedSchema<TTo>;

  return {
    fromHash: config.fromHash,
    toHash: config.toHash,
    from: config.from,
    to: config.to,
    forward: buildForwardLenses(config.migrate, fromDefinition, toDefinition),
  };
}
