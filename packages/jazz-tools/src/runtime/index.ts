export {
  type AuthConfig,
  type LocalTransactionRecord,
  type MutationErrorEvent,
  type TransactionFate,
  type PermissionAdvice,
  PersistedWriteRejectedError,
  loadWasmModule,
  type DurabilityTier,
  ReadTier,
  type QueryVisibility,
  type Row,
  type SubscriptionCallback,
  type StreamingValueChunk,
  type StreamingValueSource,
  type WasmModule,
  WriteHandle,
  WriteResult,
  ExclusiveWriteHandle,
  ExclusiveWriteResult,
} from "./client.js";
export type { PublicSession, RuntimeSourcesConfig } from "./context.js";
/** Encode the opaque logical identity exposed as `session.user` by bindings. */
export { canonicalAuthorSubject as userIdentity } from "./author-id.js";
export {
  createDb,
  Db,
  type ActiveQuerySubscriptionTrace,
  type DbConfig,
  type LogoutOptions,
  Transaction,
  type TransactionKind,
  type TransactionScope,
  type QueryBuilder,
  type QueryOptions,
  type QueryOptions as QueryExecutionOptions,
  type Branch,
  type BranchBase,
  type BranchValue,
  type QualifiedBranch,
  type InsertOptions,
  type StreamingInsertOptions,
  type RestoreOptions,
  type UpdateOptions,
  type DeleteOptions,
  type TableProxy,
} from "./db.js";
export type { AuthFailureReason, AuthState } from "./auth-state.js";
export {
  fetchStoredPermissions,
  fetchSchemaHashes,
  fetchStoredWasmSchema,
  publishStoredPermissions,
  publishStoredSchema,
  type PublishStoredSchemaOptions,
  type PublishStoredPermissionsOptions,
  type FetchStoredPermissionsOptions,
  type FetchStoredWasmSchemaOptions,
  type StoredSchemaHash,
  type StoredPermissionsResponse,
} from "./schema-fetch.js";
export {
  fetchServerSubscriptions,
  type FetchServerSubscriptionsOptions,
  type IntrospectionSubscriptionGroup,
  type IntrospectionSubscriptionResponse,
} from "./introspection-fetch.js";
export {
  generateAuthSecret,
  BrowserAuthSecretStore,
  authSecretStorageKey,
  formatAuthSecret,
  parseAuthSecret,
  AuthSecretFormatError,
} from "./auth-secret-store.js";
export type { AuthSecretStore, BrowserAuthSecretStoreOptions } from "./auth-secret-store.js";
