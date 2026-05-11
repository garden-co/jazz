// Public exports

import {
  col,
  getCollectedMigration,
  getCollectedSchema,
  migrate,
  resetCollectedState,
  table,
} from "./dsl.js";
import { defineMigration, renameTableFrom } from "./migrations.js";
import { definePermissions } from "./permissions/index.js";
import {
  defineApp,
  defineSchema,
  defineSliceableApp,
  defineTable,
  TypedTableQueryBuilder,
  permissionIntrospectionColumns,
} from "./typed-app.js";
import type {
  App as TypedApp,
  InsertOf as TypedInsertOf,
  RowOf as TypedRowOf,
  Schema as TypedSchema,
  SchemaDefinition as TypedSchemaDefinition,
  SliceableApp as TypedSliceableApp,
  TableDefinition as TypedTableDefinition,
  TableMetaOf as TypedTableMetaOf,
  WhereOf as TypedWhereOf,
} from "./typed-app.js";

// DSL for schema definitions
export {
  table,
  col,
  migrate,
  getCollectedSchema,
  getCollectedMigration,
  resetCollectedState,
} from "./dsl.js";
export type {
  Schema as SchemaAst,
  Table as SchemaAstTable,
  Column,
  ColumnMergeStrategy,
  ColumnMergeStrategyName,
  JsonSqlType,
  PolicyExpr,
  PolicyOperation,
  PolicyCmpOp,
  PolicyValue,
  OperationPolicy,
  TablePolicies,
  TableLens,
  Lens,
  LensOp,
  SqlType,
  LensOpType,
  MigrationOp,
  AddOp,
  DropOp,
  RenameOp,
  RenameTableFromOp,
  JsonValue,
  JsonSchema,
  JsonSchemaToTs,
} from "./schema.js";
export type {
  TypedColumnBuilder,
  AnyTypedColumnBuilder,
  ColumnAlias,
  ColumnBuilderSqlType,
  ColumnBuilderOptional,
  ColumnBuilderReferences,
  ColumnBuilderValue,
  ColumnTransform,
  StringColumn,
  BooleanColumn,
  IntColumn,
  TimestampColumn,
  FloatColumn,
  BytesColumn,
  JsonColumn,
  EnumColumn,
  RefColumn,
  ArrayColumn,
} from "./dsl.js";
export type {
  RelColumnRef,
  RelRowIdRef,
  RelValueRef,
  RelPredicateCmpOp,
  RelPredicateExpr,
  RelJoinKind,
  RelJoinCondition,
  RelKeyRef,
  RelProjectExpr,
  RelProjectColumn,
  RelOrderDirection,
  RelOrderByExpr,
  RelExpr,
  PolicyOperationV2,
  PolicyExprV2,
} from "./ir.js";

// Typed schema app
export { schemaToWasm } from "./codegen/schema-reader.js";
export {
  defineSchema,
  defineApp,
  defineSliceableApp,
  TypedTableQueryBuilder,
  permissionIntrospectionColumns,
} from "./typed-app.js";
export { defineMigration, renameTableFrom } from "./migrations.js";
export type {
  Schema,
  TableDefinition,
  SchemaDefinition,
  Simplify,
  CompactSchema,
  DefinedSchema,
  DefinedTable,
  TableRow,
  TableInit,
  TableWhereInput,
  TableSelectableColumn,
  TableOrderableColumn,
  TableSelected,
  TableInclude,
  TableSelectedWithIncludes,
  TableRelation,
  TableRelationMap,
  TableMeta,
  SchemaRelations,
  SchemaTable,
  AnyTableMeta,
  Table,
  Query,
  TableHandle,
  QueryHandle,
  App,
  SliceableApp,
  TypedApp,
  RowOf,
  InsertOf,
  TableMetaOf,
  WhereOf,
} from "./typed-app.js";
export type {
  DefinedMigration,
  AddedTableShape,
  MigrationShape,
  MigrationTableShape,
  RenameTableShape,
  RemovedTableShape,
} from "./migrations.js";

type RuntimeSchemaNamespace = typeof col & {
  table: typeof defineTable;
  defineSchema: typeof defineSchema;
  defineApp: typeof defineApp;
  defineSliceableApp: typeof defineSliceableApp;
  defineMigration: typeof defineMigration;
  renameTableFrom: typeof renameTableFrom;
  definePermissions: typeof definePermissions;
  permissionIntrospectionColumns: typeof permissionIntrospectionColumns;
};

export const schema: RuntimeSchemaNamespace = Object.assign({}, col, {
  table: defineTable,
  defineSchema,
  defineApp,
  defineSliceableApp,
  defineMigration,
  renameTableFrom,
  definePermissions,
  permissionIntrospectionColumns,
} as const);

export namespace schema {
  export type TableDefinition = TypedTableDefinition;
  export type SchemaDefinition = TypedSchemaDefinition;
  /**
   * Normalized type for a schema definition.
   */
  export type Schema<TSchema extends TypedSchemaDefinition = TypedSchemaDefinition> =
    TypedSchema<TSchema>;
  /**
   * App for a given schema.
   */
  export type App<TSchema extends TypedSchema<any> | TypedSchemaDefinition> = TypedApp<TSchema>;
  /**
   * App factory for deriving typed slices over one full runtime schema.
   */
  export type SliceableApp<TSchema extends TypedSchema<any> | TypedSchemaDefinition> =
    TypedSliceableApp<TSchema>;
  /**
   * Row type for a given table (all columns, `id` included)
   */
  export type RowOf<TTable> = TypedRowOf<TTable>;
  /**
   * Input type for new rows inserted into a table (no `id`, respects optionals and defaults)
   */
  export type InsertOf<TTable> = TypedInsertOf<TTable>;
  /**
   * Metadata for a given table.
   */
  export type TableMetaOf<TTable> = TypedTableMetaOf<TTable>;
  /**
   * The `where(...)` input shape for that table
   */
  export type WhereOf<TQuery> = TypedWhereOf<TQuery>;
}

// Storage drivers
export * from "./drivers/index.js";

// Runtime client
export * from "./runtime/index.js";

// Permissions DSL
export * from "./permissions/index.js";
export * from "./dev-tools/index.js";
