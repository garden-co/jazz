export {
  DirectBatch,
  type BatchMode,
  type BatchSettlement,
  JazzClient,
  type LocalBatchRecord,
  type LinkExternalIdentityOptions,
  type LinkExternalIdentityResult,
  type LocalUpdatesMode,
  PersistedWrite,
  PersistedWriteRejectedError,
  SessionClient,
  Transaction,
  loadWasmModule,
  type DurabilityTier,
  type QueryExecutionOptions,
  type QueryInput,
  type QueryPropagation,
  type QueryVisibility,
  type RequestLike,
  type Row,
  type Runtime,
  type SubscriptionCallback,
  type VisibleBatchMember,
  type WriteDurabilityOptions,
  type WasmModule,
} from "./client.js";
export type { AppContext, LocalAuthMode, RuntimeSourcesConfig, Session } from "./context.js";
export { linkExternalIdentity, type LinkExternalResponse } from "./sync-transport.js";
export {
  createDb,
  Db,
  DbDirectBatch,
  DbPersistedWrite,
  DbTransaction,
  type ActiveQuerySubscriptionTrace,
  type DbConfig,
  type QueryBuilder,
  type QueryOptions,
  type TableProxy,
} from "./db.js";
export { allRowsInTableQuery, type DynamicTableRow } from "./dynamic-query.js";
export {
  deriveLocalPrincipalId,
  deriveLocalPrincipalIdSync,
  resolveClientSessionSync,
  resolveClientSessionStateSync,
} from "./client-session.js";
export type { AuthFailureReason, AuthState } from "./auth-state.js";
export {
  fetchSchemaHashes,
  fetchStoredWasmSchema,
  type FetchStoredWasmSchemaOptions,
} from "./schema-fetch.js";
export {
  fetchServerSubscriptions,
  type FetchServerSubscriptionsOptions,
  type IntrospectionSubscriptionGroup,
  type IntrospectionSubscriptionResponse,
} from "./introspection-fetch.js";
export { resolveLocalAuthDefaults } from "./local-auth.js";
export { translateQuery } from "./query-adapter.js";
export { transformRows, unwrapValue, type WasmValue } from "./row-transformer.js";
export { toInsertRecord, toValue, toUpdateRecord } from "./value-converter.js";
export {
  DEFAULT_FILE_CHUNK_SIZE_BYTES,
  MAX_FILE_PART_BYTES,
  FileNotFoundError,
  IncompleteFileDataError,
  type ConventionalFileApp,
  type ConventionalFileRow,
  type FileReadOptions,
  type FileWriteOptions,
} from "./file-storage.js";
export {
  SubscriptionManager,
  type RowChangeKind,
  type RowDelta,
  type SubscriptionDelta,
} from "./subscription-manager.js";
export { WorkerBridge, type WorkerBridgeOptions } from "./worker-bridge.js";
