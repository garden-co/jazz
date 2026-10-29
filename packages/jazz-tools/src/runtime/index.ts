export {
  type AuthConfig,
  type LocalTransactionRecord,
  type MutationErrorEvent,
  type TransactionFate,
  type PermissionAdvice,
  PersistedWriteRejectedError,
  WriteWaitTimeoutError,
  type WriteWaitOptions,
  type DurabilityTier,
  ReadTier,
  type Row,
  type SubscriptionCallback,
  type SubscriptionCallbacks,
  type StreamingValueChunk,
  type StreamingValueSource,
  WriteHandle,
  WriteResult,
  SerializableWriteHandle,
  SerializableWriteResult,
} from "./client.js";
export { loadWasmModule, type WasmModule } from "./wasm-loader.js";
export {
  UnsupportedStorageCodecsError,
  isUnsupportedStorageCodecsError,
} from "./unsupported-storage-codecs-error.js";
export { createDb } from "./default-create-db.js";
export {
  type SnapshotRef,
  parseSnapshotRef,
  serializeSnapshotRef,
  SnapshotRefFormatError,
  UnsupportedSnapshotError,
} from "./snapshot-ref.js";
export type { PublicSession, RuntimeSourcesConfig } from "./context.js";
export type { ValueSplices } from "./subscription-manager.js";
/** Encode the opaque logical identity exposed as `session.user` by bindings. */
export { canonicalAuthorSubject as userIdentity } from "./author-id.js";
export {
  Db,
  type ActiveQuerySubscriptionTrace,
  type DbSubscriptionCallbacks,
  type DbSubscriptionUpdate,
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
export type { AccountDbConfig as DbConfig } from "../accounts/context.js";
export type { AuthFailureReason, AuthState } from "./auth-state.js";
export type { LinkStatus, SyncStatus } from "./sync-status.js";
export {
  fetchStoredPermissions,
  fetchSchemaHashes,
  fetchStoredWasmSchema,
  type FetchStoredPermissionsOptions,
  type FetchStoredWasmSchemaOptions,
  type StoredSchemaHash,
  type StoredPermissionsResponse,
} from "../dev/catalogue-api.js";
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
