export {
  createJazzClient,
  type BackendClientCloseOptions,
  type BackendClientSession,
  type JazzClientConfig,
  type JazzClient,
} from "./create-jazz-session.js";
export {
  type BackendQuerySchemaSource,
  type BackendSchemaInput,
  type BackendSchemaSource,
} from "./create-jazz-context.js";
export { isRequestAuthenticationError, RequestAuthenticationError } from "./request-auth.js";
export { isNoJazzAccountError, NoJazzAccountError } from "./no-account-error.js";
export type { WasmSchema } from "../drivers/types.js";
export type { PublicSession } from "../runtime/context.js";
export { Db, type QueryBuilder, type QueryOptions, type TableProxy } from "../runtime/db.js";
export {
  type SnapshotRef,
  parseSnapshotRef,
  serializeSnapshotRef,
  SnapshotRefFormatError,
  UnsupportedSnapshotError,
} from "../runtime/snapshot-ref.js";
export { trustWorkload, type BackendClientTrust } from "./backend-clients.js";
export { fromJWT, type FromJWTOptions } from "./from-jwt.js";
export {
  createJazz,
  files,
  memory,
  remote,
  s3,
  sqlite,
  vercel,
  vercelBlob,
  type BlobStore,
  type S3BlobStore,
  type VercelBlobStore,
  type CreateJazzOptions,
  type Jazz,
  type JazzStore,
  type BackendCredential,
  type JazzUser,
  type LocalFirstGuest,
  acceptDeviceAccounts,
  perBranch,
  type VercelNamespace,
  type VercelNamespaceContext,
  type VercelStoreOptions,
  type WsLikeSocket,
  type InspectEvent,
  type InspectGrant,
  type InspectHook,
  type InspectorOptions,
} from "./create-jazz.js";

// Removed in alpha.61: stubs that throw, naming the replacement (one release).
export { acceptLocalFirstAuth } from "../removed-api.js";
