import type { AnyTypedColumnBuilder, ColumnBuilderOptional, ColumnBuilderSqlType } from "./dsl.js";
import { assertUserColumnNameAllowed } from "./magic-columns.js";
import type { Lens, Schema, TSTypeFromSqlType } from "./schema.js";
import type { CompactSchema, DefinedSchema, SchemaDefinition } from "./typed-app.js";

type SchemaLike = SchemaDefinition | DefinedSchema<any>;

type NormalizedSchema<TSchema extends SchemaLike> =
  TSchema extends DefinedSchema<infer TDefinition>
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

export interface MigrationTableEditor<
  TFrom extends SchemaLike,
  TTo extends SchemaLike,
  TTable extends SharedTableName<TFrom, TTo>,
> {
  rename<TOldName extends ColumnName<TFrom, TTable>, TNewName extends ColumnName<TTo, TTable>>(
    oldName: TOldName,
    newName: TNewName,
  ): this;
  add<TColumn extends AddedColumnName<TFrom, TTo, TTable>>(
    column: TColumn,
    opts: {
      default: DefaultValueForBuilder<BuilderForColumn<TTo, TTable, TColumn>>;
    },
  ): this;
  drop<TColumn extends RemovedColumnName<TFrom, TTo, TTable>>(
    column: TColumn,
    opts: {
      backwardsDefault: DefaultValueForBuilder<BuilderForColumn<TFrom, TTable, TColumn>>;
    },
  ): this;
}

export interface MigrationBuilder<TFrom extends SchemaLike, TTo extends SchemaLike> {
  table<TTable extends SharedTableName<TFrom, TTo>>(
    table: TTable,
    build: (table: MigrationTableEditor<TFrom, TTo, TTable>) => unknown,
  ): void;
}

class TableMigrationBuilder<
  TFrom extends SchemaLike,
  TTo extends SchemaLike,
  TTable extends SharedTableName<TFrom, TTo>,
> implements MigrationTableEditor<TFrom, TTo, TTable> {
  private readonly operations: Lens["operations"] = [];

  constructor(
    private readonly tableName: TTable,
    private readonly fromTable: NormalizedSchema<TFrom>[TTable],
    private readonly toTable: NormalizedSchema<TTo>[TTable],
  ) {}

  rename<TOldName extends ColumnName<TFrom, TTable>, TNewName extends ColumnName<TTo, TTable>>(
    oldName: TOldName,
    newName: TNewName,
  ): this {
    assertUserColumnNameAllowed(oldName);
    assertUserColumnNameAllowed(newName);
    this.operations.push({
      type: "rename",
      column: oldName,
      value: newName,
    });
    return this;
  }

  add<TColumn extends AddedColumnName<TFrom, TTo, TTable>>(
    column: TColumn,
    opts: {
      default: DefaultValueForBuilder<BuilderForColumn<TTo, TTable, TColumn>>;
    },
  ): this {
    assertUserColumnNameAllowed(column);
    const builder = this.toTable[column] as BuilderForColumn<TTo, TTable, TColumn>;
    this.operations.push({
      type: "introduce",
      column,
      sqlType: builder._sqlType,
      value: opts.default,
    });
    return this;
  }

  drop<TColumn extends RemovedColumnName<TFrom, TTo, TTable>>(
    column: TColumn,
    opts: {
      backwardsDefault: DefaultValueForBuilder<BuilderForColumn<TFrom, TTable, TColumn>>;
    },
  ): this {
    assertUserColumnNameAllowed(column);
    const builder = this.fromTable[column] as BuilderForColumn<TFrom, TTable, TColumn>;
    this.operations.push({
      type: "drop",
      column,
      sqlType: builder._sqlType,
      value: opts.backwardsDefault,
    });
    return this;
  }

  build(): Lens | null {
    if (this.operations.length === 0) {
      return null;
    }

    return {
      table: this.tableName,
      operations: [...this.operations],
    };
  }
}

class MigrationCollector<
  TFrom extends SchemaLike,
  TTo extends SchemaLike,
> implements MigrationBuilder<TFrom, TTo> {
  readonly forward: Lens[] = [];

  constructor(
    private readonly fromDefinition: NormalizedSchema<TFrom>,
    private readonly toDefinition: NormalizedSchema<TTo>,
  ) {}

  table<TTable extends SharedTableName<TFrom, TTo>>(
    table: TTable,
    build: (table: MigrationTableEditor<TFrom, TTo, TTable>) => unknown,
  ): void {
    const tableBuilder = new TableMigrationBuilder<TFrom, TTo, TTable>(
      table,
      this.fromDefinition[table],
      this.toDefinition[table],
    );
    build(tableBuilder);
    const lens = tableBuilder.build();
    if (lens) {
      this.forward.push(lens);
    }
  }
}

function definitionToSchema(definition: SchemaDefinition): Schema {
  return {
    tables: Object.entries(definition).map(([tableName, tableDefinition]) => ({
      name: tableName,
      columns: Object.entries(tableDefinition).map(([columnName, builder]) => {
        assertUserColumnNameAllowed(columnName);
        return builder._build(columnName);
      }),
    })),
  };
}

export function schemaDefinitionToAst(definition: SchemaDefinition | DefinedSchema<any>): Schema {
  return definitionToSchema(definition as SchemaDefinition);
}

export function defineMigration<
  const TFrom extends SchemaLike,
  const TTo extends SchemaLike,
>(config: {
  fromHash: string;
  toHash: string;
  from: TFrom;
  to: TTo;
  migrate?: (migration: MigrationBuilder<TFrom, TTo>) => void;
}): DefinedMigration<TFrom, TTo> {
  const collector = new MigrationCollector<TFrom, TTo>(
    config.from as unknown as NormalizedSchema<TFrom>,
    config.to as unknown as NormalizedSchema<TTo>,
  );
  config.migrate?.(collector);

  return {
    fromHash: config.fromHash,
    toHash: config.toHash,
    from: config.from,
    to: config.to,
    forward: collector.forward,
  };
}
