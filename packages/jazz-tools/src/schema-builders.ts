/**
 * The members of the `schema` namespace (`schema-namespace.ts`): the TS
 * authoring DSL `jazz-tools generate` reads. Apps write `schema.ts` with
 * these; runtime code imports `./jazz.generated` instead.
 *
 * Each builder is its own export so bundlers can drop what an app never
 * names.
 */
import { schemaColumns } from "./dsl.js";
import { removedSchemaApi } from "./removed-schema-api.js";
import type { RemovedApi } from "./removed-api.js";
import type { Schema as DefinedSchema } from "./schema-dsl/define-schema.js";
import type {
  Row as RowOf,
  OldRow as OldRowOf,
  SchemaTables,
  Conditions as ConditionsOf,
} from "./permissions/types.js";

export { rel, reverse } from "./relationships.js";
export { allowExternalProvenanceName, computed } from "./dsl.js";
export { defineTable as table } from "./schema-dsl/table.js";
export { defineSchema, claims } from "./schema-dsl/define-schema.js";

/** @deprecated Removed: define the schema with `s.defineSchema({ … })`; throws, pointing at `npx jazz-tools upgrade`. */
export const defineApp: RemovedApi = /*#__PURE__*/ removedSchemaApi("schema.defineApp");
/** @deprecated Removed: schema slicing is gone; throws, pointing at `npx jazz-tools upgrade`. */
export const defineSliceableApp: RemovedApi = /*#__PURE__*/ removedSchemaApi(
  "schema.defineSliceableApp",
);
/** @deprecated Removed: use `schema.definePermissions({ … })`; throws, pointing at `npx jazz-tools upgrade`. */
export const definePermissions: RemovedApi = /*#__PURE__*/ removedSchemaApi(
  "schema.definePermissions",
);

export const string = schemaColumns.string;
export const uuid = schemaColumns.uuid;
export const boolean = schemaColumns.boolean;
export const int = schemaColumns.int;
export const bigint = schemaColumns.bigint;
export const timestamp = schemaColumns.timestamp;
export const float = schemaColumns.float;
export const bytes = schemaColumns.bytes;
export const json = schemaColumns.json;
const enumColumn = schemaColumns.enum;
export { enumColumn as enum };
export const array = schemaColumns.array;

export type { DefinedTable as Table, TableDefinition } from "./schema-dsl/table.js";
export type {
  Uuid,
  Value,
  ValueKind,
  ValueHandle,
  Policy,
  NestedPolicy,
  Inherits,
  Rel,
  Permissions,
  ColumnHandle,
} from "./permissions/types.js";
export type { DefineSchemaOptions } from "./schema-dsl/define-schema.js";

type TablesOf<TSchema> = TSchema extends { readonly __tables?: infer S } ? NonNullable<S> : never;
type AnySchema = { readonly __tables?: SchemaTables };

/** What `s.defineSchema` returns, for the tables `S`. */
export type Schema<S extends SchemaTables = SchemaTables> = DefinedSchema<S, any>;

/** The row handle of a table: `s.Row<typeof schema, "releases">`. */
export type Row<TSchema extends AnySchema, TTable extends keyof TablesOf<TSchema> & string> = RowOf<
  TablesOf<TSchema>,
  TTable
>;

/** The row before an update (`updateTo`'s `old`): `s.OldRow<typeof schema, "chats">`. */
export type OldRow<
  TSchema extends AnySchema,
  TTable extends keyof TablesOf<TSchema> & string,
> = OldRowOf<TablesOf<TSchema>, TTable>;

/** A table's condition object: `s.Conditions<typeof schema, "chats">`. */
export type Conditions<
  TSchema extends AnySchema,
  TTable extends keyof TablesOf<TSchema> & string,
> = ConditionsOf<TablesOf<TSchema>, TTable>;

// The types inferred `schema` and `permissions` exports reach, so a
// `declaration: true` build can name them (`import("jazz-tools").schema.…`).
// Names `s.Row`, `s.OldRow`, `s.Conditions` and `s.Schema` already take are
// re-exported under another one.
export type {
  TypedColumnBuilder,
  AnyTypedColumnBuilder,
  ComputedColumn,
  AnyColumnDefinition,
  ColumnTransform,
  StringColumn,
  UuidColumn,
  BooleanColumn,
  IntColumn,
  BigIntColumn,
  TimestampColumn,
  FloatColumn,
  BytesColumn,
  JsonColumn,
  EnumColumn,
  EnumCasesColumn,
  RefColumn,
  ArrayColumn,
  ColumnAlias,
} from "./dsl.js";
export type {
  OldValue,
  CompatibleKind,
  CompareOp,
  ContainsOp,
  InOp,
  SchemaTables,
  RelationTarget,
  KindOf,
  LiteralFor,
  Operand,
  ConditionValue,
  ColumnInfo,
  Conditions as TableConditions,
  Row as RowHandle,
  OldRow as OldRowHandle,
  TableHandles,
  PolicyInput,
  ConditionsIn,
  PermissionError,
  CheckPermissions,
  RuleFor,
  PermissionsInput,
  AllTables,
  ReadablePermissionsFile,
  AnyClaims,
  TypedClaims,
  ClaimsMarker,
  Session,
} from "./permissions/types.js";
export type {
  CompareHelper,
  EqHelper,
  ExistsPolicy,
  Exists,
  AllowedTarget,
  AllowedTo,
  AllowedPolicy,
  DefinePermissions,
  PermissionTools,
} from "./permissions/dsl.js";
export type {
  RelationKey,
  RowIdRef,
  RelationSource,
  RelationQuery,
  From,
} from "./permissions/relation.js";
export type { Schema as DefinedSchema } from "./schema-dsl/define-schema.js";
export type { StoredColumnKeys } from "./schema-dsl/table.js";
export type {
  SqlType,
  ScalarSqlType,
  EnumSqlType,
  ArraySqlType,
  JsonSqlType,
  JsonSchema,
  TSTypeFromSqlType,
  TSInitFromSqlType,
} from "./schema.js";
export type {
  ForwardRelationship,
  ReverseRelationship,
  Relationship,
  Relationships,
} from "./relationships.js";
