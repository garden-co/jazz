export {
  JazzClient,
  type LinkExternalIdentityOptions,
  type LinkExternalIdentityResult,
  SessionClient,
  loadWasmModule,
  type PersistenceTier,
  type QueryExecutionOptions,
  type QueryInput,
  type QueryPropagation,
  type RequestLike,
  type Row,
  type Runtime,
  type SubscriptionCallback,
  type WasmModule,
} from "./client.js";
export type { AppContext, LocalAuthMode, Session } from "./context.js";
export { linkExternalIdentity, type LinkExternalResponse } from "./sync-transport.js";
export {
  createDb,
  Db,
  type DbConfig,
  type QueryBuilder,
  type QueryOptions,
  type TableProxy,
} from "./db.js";
export { allRowsInTableQuery, type DynamicTableRow } from "./dynamic-query.js";
export { deriveLocalPrincipalId, resolveClientSession } from "./client-session.js";
export {
  fetchSchemaHashes,
  fetchStoredWasmSchema,
  type FetchStoredWasmSchemaOptions,
} from "./schema-fetch.js";
export { resolveLocalAuthDefaults } from "./local-auth.js";
export { translateQuery } from "./query-adapter.js";
export { transformRows, unwrapValue, type WasmValue } from "./row-transformer.js";
export { toValue, toValueArray, toUpdateRecord } from "./value-converter.js";
export {
  SubscriptionManager,
  type RowChangeKind,
  type RowDelta,
  type SubscriptionDelta,
} from "./subscription-manager.js";
export { WorkerBridge, type WorkerBridgeOptions } from "./worker-bridge.js";
