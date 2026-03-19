// Public exports

// DSL for schema definitions
export {
  table,
  col,
  migrate,
  getCollectedSchema,
  getCollectedMigration,
  resetCollectedState,
} from "./dsl.js";
export { schemaToSql, lensToSql } from "./sql-gen.js";
export type {
  Schema,
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

// Codegen
export { generateClient, schemaToWasm, generateTypes } from "./codegen/index.js";
export {
  defineSchema,
  defineApp,
  TypedTableQueryBuilder,
  permissionIntrospectionColumns,
} from "./typed-app.js";
export { defineMigration } from "./migrations.js";
export type {
  TableDefinition,
  SchemaDefinition,
  Simplify,
  CompactSchema,
  DefinedSchema,
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
  TypedApp,
  RowOf,
  InsertOf,
  TableMetaOf,
  WhereOf,
} from "./typed-app.js";
export type { DefinedMigration, MigrationBuilder, MigrationTableEditor } from "./migrations.js";

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
