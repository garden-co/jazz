// Public exports
export type { RowAuthor } from "./magic-columns.js";
export { SYSTEM_ACCOUNT_ID, SYSTEM_ISSUER } from "./magic-columns.js";

// Diagnostic-only Classic exports; these never implement Classic behaviour.
export {
  co,
  z,
  CoMap,
  CoList,
  CoFeed,
  CoPlainText,
  CoRichText,
  FileStream,
  Account,
  Group,
  Profile,
} from "./classic-api.js";

// The TS authoring DSL (`schema.ts`, `migrations/*.ts`), read by `jazz-tools generate`.
export { schema } from "./schema-namespace.js";
export { migration } from "./migration-namespace.js";

// Removed in alpha.65: stubs that throw, pointing at `jazz-tools upgrade`.
export {
  defineApp,
  defineSchema,
  defineSliceableApp,
  definePermissions,
  defineMigration,
  renameTableFrom,
  schemaToWasm,
  table,
  anyOf,
  allOf,
} from "./removed-schema-api.js";
export type { JsonValue, JsonPrimitive } from "./json-value.js";

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

// Typed app bindings (jazz.generated.ts calls bindApp)
export { bindApp, TypedTableQueryBuilder } from "./typed-app.js";
export {
  getSupportedWhereOperatorsForColumn,
  getSupportedWhereOperatorsForSchemaColumn,
} from "./where-operators.js";
export type { WhereOperator, WhereOperatorColumn } from "./where-operators.js";
export type {
  AppTypes,
  TableTypes,
  ColumnTypes,
  ColumnKind,
  RelationTypes,
  TransformTypes,
  ComputedImpl,
  TransformImpl,
  BindAppOptions,
  CompiledAppSchema,
  HistoryQuery,
  HistoryRow,
  Simplify,
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

// Storage drivers
export * from "./drivers/index.js";

// Runtime client
export * from "./runtime/index.js";

// Inspector overlay host contract (types + global) and the host bridge that
// publishes the handle for the same-origin overlay iframe.
export * from "./dev/inspector-overlay/inspector-host-types.js";
export { installInspectorHost, type InspectorHostDb } from "./dev/inspector-overlay/host-bridge.js";

export * from "./accounts/exports.js";

export { accountRegistryUrl } from "./accounts/context.js";

export {
  createJazzClient,
  startJazzClient,
  type JazzClientConfig,
} from "./session/create-jazz-session.js";
export type {
  JazzSession,
  JazzSessionSnapshot,
  JazzSessionOperation,
  JazzUser,
} from "./session/jazz-session.js";
export { NoKeptDeviceAccountError } from "./session/no-kept-device-account-error.js";
export { DeviceAccountNeedsStoreError } from "./accounts/process-store.js";

export type {
  ForwardRelationship,
  ReverseRelationship,
  Relationship,
  Relationships,
} from "./relationships.js";

// Removed in alpha.61: stubs that throw, naming the replacement (one release).
export {
  AccountManager,
  BrowserAuthSecretStore,
  createAccountManager,
  createDb,
  exportLocalFirstSecret,
} from "./removed-api.js";
