// Public exports

import {
  col,
  getCollectedSchema,
  resetCollectedState,
  table,
  allowExternalProvenanceName,
} from "./dsl.js";
import { defineMigration, renameTableFrom } from "./migrations.js";
import { definePermissions } from "./permissions/index.js";
import {
  defineApp,
  defineSchema,
  defineSliceableApp,
  defineTable,
  TypedTableQueryBuilder,
} from "./typed-app.js";
import type {
  App as TypedApp,
  InsertOf as TypedInsertOf,
  LargeValueUpdateOf as TypedLargeValueUpdateOf,
  StreamingInsertOf as TypedStreamingInsertOf,
  StreamingUpdateOf as TypedStreamingUpdateOf,
  StreamingUpsertOf as TypedStreamingUpsertOf,
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

type RuntimeSchemaNamespace = typeof col & {
  table: typeof defineTable;
  defineSchema: typeof defineSchema;
  defineApp: typeof defineApp;
  defineSliceableApp: typeof defineSliceableApp;
  defineMigration: typeof defineMigration;
  renameTableFrom: typeof renameTableFrom;
  definePermissions: typeof definePermissions;
  allowExternalProvenanceName: typeof allowExternalProvenanceName;
};

export const schema: RuntimeSchemaNamespace = Object.assign({}, col, {
  table: defineTable,
  defineSchema,
  defineApp,
  defineSliceableApp,
  defineMigration,
  renameTableFrom,
  definePermissions,
  allowExternalProvenanceName,
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
  /** Input type for updating a row, including typed partial large-value descriptors. */
  export type LargeValueUpdateOf<TTable> = TypedLargeValueUpdateOf<TTable>;
  /** Input type for inserting a row with one streamed Text, JSON, or Bytea column. */
  export type StreamingInsertOf<TTable> = TypedStreamingInsertOf<TTable>;
  export type StreamingUpdateOf<TTable> = TypedStreamingUpdateOf<TTable>;
  export type StreamingUpsertOf<TTable> = TypedStreamingUpsertOf<TTable>;
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
// Inspector overlay host contract (types + global) and the host bridge that
// publishes the handle for the same-origin overlay iframe.
export * from "./dev/inspector-overlay/inspector-host-types.js";
export { installInspectorHost, type InspectorHostDb } from "./dev/inspector-overlay/host-bridge.js";
