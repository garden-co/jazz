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
export { AuthorityChangedError, isAuthorityChangedError } from "./authority-changed-error.js";
export { CoreTickFailedError, isCoreTickFailedError } from "./core-tick-failed-error.js";
export {
  StoreHeldByAnotherTabError,
  isStoreHeldByAnotherTabError,
} from "./store-held-by-another-tab-error.js";
export { DbReplacedError, type DbReplacedReason } from "./db-replaced-error.js";
export {
  type SnapshotRef,
  parseSnapshotRef,
  serializeSnapshotRef,
  SnapshotRefFormatError,
  UnsupportedSnapshotError,
} from "./snapshot-ref.js";
export type { PublicSession, RuntimeSourcesConfig } from "./context.js";
export type { ValueSplices } from "./subscription-manager.js";
/** Encode a login (`session.identity`) as its canonical opaque `[issuer, subject]` string. */
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
  type UpdateEditOptions,
  type DeleteOptions,
  type TableProxy,
} from "./db.js";
export type { AccountDbConfig as DbConfig } from "../accounts/context.js";
export type { AppArtifact, AppPermissions } from "./app-artifact.js";
export type { AuthFailureReason, AuthState } from "./auth-state.js";
export type {
  LinkFailure,
  LinkStatus,
  OfflineReason,
  SyncError,
  SyncStatus,
} from "./sync-status.js";
export type { JazzPageHandle, PageDbInfo } from "./page-handle.js";
export {
  fetchPublishedBundles,
  fetchSchemaHashes,
  fetchStoredWasmSchema,
  type FetchPublishedBundlesOptions,
  type FetchStoredWasmSchemaOptions,
  type PublishedBundle,
  type StoredSchemaHash,
} from "../dev/catalogue-api.js";
export {
  fetchServerSubscriptions,
  type FetchServerSubscriptionsOptions,
  type IntrospectionSubscriptionGroup,
  type IntrospectionSubscriptionResponse,
} from "./introspection-fetch.js";
export {
  generateAuthSecret,
  authSecretStorageKey,
  formatAuthSecret,
  parseAuthSecret,
  AuthSecretFormatError,
} from "./auth-secret-store.js";
export type {
  ResolvedTextAnchor,
  ResolvedTextRange,
  TextAnchor,
  TextAnchors,
  TextRange,
  TextSide,
} from "./text-anchors.js";
