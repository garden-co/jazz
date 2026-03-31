// Public exports

import {
  col,
  getCollectedMigration,
  getCollectedSchema,
  migrate,
  resetCollectedState,
  table,
} from "./dsl.js";
import { defineMigration } from "./migrations.js";
import { definePermissions } from "./permissions/index.js";
import {
  defineApp,
  defineSchema,
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
  TableDefinition as TypedTableDefinition,
  TableIndex as TypedTableIndex,
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
  TypedTableQueryBuilder,
  permissionIntrospectionColumns,
} from "./typed-app.js";
export { defineMigration } from "./migrations.js";
export type {
  Schema,
  TableDefinition,
  SchemaDefinition,
  Simplify,
  CompactSchema,
  DefinedSchema,
  TableIndex,
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
  TypedApp,
  RowOf,
  InsertOf,
  TableMetaOf,
  WhereOf,
} from "./typed-app.js";
export type { DefinedMigration, MigrationShape, MigrationTableShape } from "./migrations.js";

type RuntimeSchemaNamespace = typeof col & {
  table: typeof defineTable;
  defineSchema: typeof defineSchema;
  defineApp: typeof defineApp;
  defineMigration: typeof defineMigration;
  definePermissions: typeof definePermissions;
  permissionIntrospectionColumns: typeof permissionIntrospectionColumns;
};

export const schema: RuntimeSchemaNamespace = Object.assign({}, col, {
  table: defineTable,
  defineSchema,
  defineApp,
  defineMigration,
  definePermissions,
  permissionIntrospectionColumns,
} as const);

export namespace schema {
  export type TableDefinition = TypedTableDefinition;
  export type SchemaDefinition = TypedSchemaDefinition;
  export type TableIndex<TColumns extends TypedTableDefinition = TypedTableDefinition> =
    TypedTableIndex<TColumns>;
  export type Schema<TSchema extends TypedSchemaDefinition = TypedSchemaDefinition> =
    TypedSchema<TSchema>;
  export type App<TSchema extends TypedSchema<any> | TypedSchemaDefinition> = TypedApp<TSchema>;
  export type RowOf<TTable> = TypedRowOf<TTable>;
  export type InsertOf<TTable> = TypedInsertOf<TTable>;
  export type TableMetaOf<TTable> = TypedTableMetaOf<TTable>;
  export type WhereOf<TQuery> = TypedWhereOf<TQuery>;
}

// Storage drivers
export * from "./drivers/index.js";

// Runtime client
export * from "./runtime/index.js";

// Permissions DSL
export * from "./permissions/index.js";
export * from "./dev-tools/index.js";

// Local synthetic users and vanilla switcher UI
export {
  createSyntheticUserProfile,
  getActiveSyntheticAuth,
  loadSyntheticUserStore,
  saveSyntheticUserStore,
  setActiveSyntheticProfile,
  syntheticUserStorageKey,
  type ActiveSyntheticAuth,
  type StorageLike,
  type SyntheticUserProfile,
  type SyntheticUserStorageOptions,
  type SyntheticUserStore,
} from "./synthetic-users.js";
export {
  createSyntheticUserSwitcher,
  type SyntheticUserSwitcherHandle,
  type SyntheticUserSwitcherOptions,
} from "./synthetic-user-switcher.js";
