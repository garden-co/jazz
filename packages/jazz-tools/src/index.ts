// Public exports
export type { RowAuthor } from "./magic-columns.js";
export { SYSTEM_ACCOUNT_ID, SYSTEM_ISSUER } from "./magic-columns.js";

// DSL for schema definitions
export {
  table,
  col,
  getCollectedSchema,
  resetCollectedState,
  allowExternalProvenanceName,
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
  UuidColumn,
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
  PolicyIRExpr,
} from "./ir.js";

// Typed schema app
export { schemaToWasm } from "./codegen/schema-reader.js";
export {
  defineSchema,
  defineApp,
  defineSliceableApp,
  TypedTableQueryBuilder,
} from "./typed-app.js";
export { defineMigration, renameTableFrom } from "./migrations.js";
export {
  getSupportedWhereOperatorsForColumn,
  getSupportedWhereOperatorsForSchemaColumn,
} from "./where-operators.js";
export type { WhereOperator, WhereOperatorColumn } from "./where-operators.js";
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
  TableStreamingInit,
  TableStreamingUpdate,
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
  LargeValueUpdateOf,
  StreamingInsertOf,
  StreamingUpdateOf,
  StreamingUpsertOf,
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

export { schema } from "./schema-namespace.js";

// Storage drivers
export * from "./drivers/index.js";

// Runtime client
export * from "./runtime/index.js";

// Permissions DSL
export * from "./permissions/index.js";
// Inspector overlay host contract (types + global) and the host bridge that
// publishes the handle for the same-origin overlay iframe.
export * from "./dev/inspector-overlay/inspector-host-types.js";
export { installInspectorHost, type InspectorHostDb } from "./dev/inspector-overlay/host-bridge.js";

export {
  createAccountManager,
  type AccountManagerConfig,
} from "./accounts/create-account-manager.js";
export type {
  AccountHandle,
  AccountIdentity,
  AccountSnapshot,
  AccountManager,
} from "./accounts/state.js";
export {
  AccountAuthError,
  exportLocalFirstSecret,
  type BackendAuth,
  type JWTAuth,
} from "./accounts/enrollment.js";
export type { AccountStore } from "./accounts/persistence.js";

export { accountRegistryUrl } from "./accounts/context.js";

export { createJazzSession, type JazzSessionConfig } from "./session/create-jazz-session.js";
export type {
  JazzSession,
  JazzSessionActions,
  JazzSessionSnapshot,
  JazzSessionOperation,
} from "./session/state.js";
