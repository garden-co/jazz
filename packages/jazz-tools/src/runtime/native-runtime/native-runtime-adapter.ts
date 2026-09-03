import type {
  ColumnDescriptor,
  ColumnType,
  InsertValues,
  NativeTerminalOperation,
  RuntimeSubscriptionDelta,
  RuntimeTerminalOperation,
  TablePolicies,
  Value,
  WasmRow,
  WasmSchema,
} from "../../drivers/types.js";
import { serializeRuntimeSchema } from "../../drivers/schema-wire.js";
import type {
  TxId,
  InsertResult,
  MutationErrorEvent,
  MutationResult,
  OpenTransactionId,
  PermissionAdvice,
  Runtime,
  StreamingInsertResult,
  StreamingMutationKind,
  StreamingValueSource,
  TransactionKind,
} from "../client.js";
import type { Session } from "../context.js";
import { SYSTEM_AUTHOR_ID } from "../system-identity.js";
import {
  SYSTEM_SESSION_ISSUER,
  TRUSTED_RESERVED_SESSION_TOKEN_FIELD,
  isReservedJazzIssuer,
  isTrustedReservedSession,
} from "../client-session.js";
import {
  authorBytesForSession,
  decodeCanonicalAuthorSubjectBytes,
  isUsableSubject,
  parseCanonicalAuthorSubject,
} from "../author-id.js";
import {
  PostcardReader,
  PostcardWriter,
  openConfig,
  queryWithPredicates,
  readNativeRowBatch,
  readNativeRelationSubscriptionSnapshot,
  readNativeSubscriptionDelta,
  type NativeRelationSubscriptionSnapshot,
  type NativeRowBatch,
  type NativeSubscriptionDelta,
  type NativeSelfSignedClientProof,
  type QueryArraySubquery,
  type DescriptorField,
  type QueryLiteral,
  type QueryOrder,
  type QueryPredicate,
  type QueryPredicateOp,
  type ValueType,
} from "./native-codec.js";
import { exactSignedI64 } from "./exact-integer.js";
import { encodeSchema } from "./schema-codec.js";
import { nativeRowFieldPlanCacheKey } from "./native-row-descriptor-key.js";
import {
  WebSocketCarrier,
  isRetryablePreHelloWireError,
  normalizeBackendWebSocketAuth,
  peerIdentityForWebSocketAuth,
  type WebSocketNegotiation,
  wireAuthFailureReason,
} from "./websocket.js";
import {
  createRecord,
  createRecordValueDecoder,
  decodeNativeTerminalRow,
  decodeNativeRowValues,
  encodeNativeColumnValue,
  encodeNativeNullValue,
  logicalStorageColumns,
  nativeFixedValueSize,
  storageColumnTypeToValueType,
  storageColumnValueType,
  writeDescriptor,
} from "./native-row-codec.js";
import { HIDDEN_INCLUDE_COLUMN_PREFIX } from "../select-projection.js";
import {
  isPermissionIntrospectionColumn,
  isProvenanceMagicColumn,
  magicColumnType,
} from "../../magic-columns.js";

export { encodeSchema } from "./schema-codec.js";

const SERVER_PUMP_DEBOUNCE_MS = 16;
const PRE_HELLO_RETRY_INITIAL_DELAY_MS = 25;
const PRE_HELLO_RETRY_MAX_DELAY_MS = 1_000;
// Amortize scheduler overhead without allowing a ready evaluator to monopolize
// the browser task queue. Transport pumps never add a second inner tick loop.
const MAX_CORE_TICKS_PER_TURN = 4;

type ReadAuthorizationHost = "client-local" | "trusted-serving";
/**
 * The native ABI has three deliberately non-interchangeable read entry
 * points.  This is an adapter-private capability choice, never wire/session
 * data: callers can supply a public session, but cannot name backend
 * authority or turn a client-local read into an authority read.
 */
type NativeReadContext =
  | { readonly kind: "client-local" }
  | { readonly kind: "session-authority"; readonly identity: Uint8Array }
  | { readonly kind: "backend-authority" };
type CoreTickWake = "immediate" | "deferred" | "after-current-turn" | `after:${number}`;

type NativeDbConstructor = {
  openMemory(schema: Uint8Array, config: Uint8Array): NativeDb;
  openMemoryAsBackend?(schema: Uint8Array, config: Uint8Array): NativeDb;
  openPersistent?(dataPath: string, schema: Uint8Array, config: Uint8Array): NativeDb;
  openPersistentAsBackend?(dataPath: string, schema: Uint8Array, config: Uint8Array): NativeDb;
  openMemoryWithSelfSignedProof?(
    schema: Uint8Array,
    config: Uint8Array,
    token: string,
    appId: string,
    claimedAuthor: string,
  ): NativeDb;
  openPersistentWithSelfSignedProof?(
    dataPath: string,
    schema: Uint8Array,
    config: Uint8Array,
    token: string,
    appId: string,
    claimedAuthor: string,
  ): NativeDb;
};

type NativeWriteOptions = {
  author?: Uint8Array;
  updatedAtMs?: number;
};

type PendingNativeRead = { poll(): Uint8Array | null };
type NativeReadResult = Uint8Array | PendingNativeRead;
type PendingNativeSubscriptionBatch = { retryAfterMs?(): number | null };
type PendingNativeWrite = { poll(): Write | null };
type PendingNativePermissionAdvice = {
  poll(): string | null;
  cancel(): void;
};

function isPendingNativeRead(value: unknown): value is PendingNativeRead {
  return typeof (value as PendingNativeRead | null)?.poll === "function";
}

function isPendingNativeWrite(value: unknown): value is PendingNativeWrite {
  return typeof (value as PendingNativeWrite | null)?.poll === "function";
}

function isPendingNativePermissionAdvice(value: unknown): value is PendingNativePermissionAdvice {
  const candidate = value as PendingNativePermissionAdvice | null;
  return typeof candidate?.poll === "function" && typeof candidate.cancel === "function";
}

type NativeInsertOptions = NativeWriteOptions & {
  rowId?: Uint8Array;
  branch?: unknown;
};

type NativeUpdateOptions = NativeWriteOptions & {
  head?: unknown;
  base?: unknown;
};

type NativeUpsertOptions = NativeUpdateOptions;

type NativeDeleteOptions = NativeUpdateOptions;
type NativeRestoreOptions = NativeWriteOptions & {
  branch?: unknown;
};

type NativeDb = {
  // Native runtime adapters may close synchronously or asynchronously and may
  // report whether they transitioned state. The adapter awaits either form and
  // owns idempotence, so callers never observe that implementation detail.
  close?(): void | boolean | Promise<void | boolean>;
  registerSchema(schema: Uint8Array): NativeDb;
  beginTransaction(openTransactionId: string, kind: TransactionKind, author?: Uint8Array): void;
  beginTransactionAttributed?(openTransactionId: string, attribution: Uint8Array): void;
  commitTransaction(openTransactionId: string, kind?: TransactionKind): Write;
  rollbackTransaction(openTransactionId: string): void;
  attachMergeableTx(openTransactionId: string): Tx;
  attachExclusiveTx?(openTransactionId: string): Tx;
  all(query: PreparedQuery, opts: unknown): NativeReadResult;
  allForIdentity(query: PreparedQuery, author: Uint8Array, opts: unknown): NativeReadResult;
  allForBackend?(query: PreparedQuery, opts: unknown): NativeReadResult | Promise<NativeReadResult>;
  allAsync?(query: PreparedQuery, opts: unknown): NativeReadResult | Promise<NativeReadResult>;
  allForIdentityAsync?(
    query: PreparedQuery,
    author: Uint8Array,
    opts: unknown,
  ): NativeReadResult | Promise<NativeReadResult>;
  allRelationQuery?(queryJson: string, opts: unknown): NativeReadResult | Promise<NativeReadResult>;
  allRelationQueryForIdentity?(
    queryJson: string,
    author: Uint8Array,
    opts: unknown,
  ): NativeReadResult | Promise<NativeReadResult>;
  /** Authority-serving relation read owned by an explicit backend open. */
  allRelationQueryForBackend?(
    queryJson: string,
    opts: unknown,
  ): NativeReadResult | Promise<NativeReadResult>;
  allRelationSnapshot?(
    query: PreparedQuery,
    opts: unknown,
  ): NativeReadResult | Promise<NativeReadResult>;
  allRelationSnapshotForIdentity?(
    query: PreparedQuery,
    author: Uint8Array,
    opts: unknown,
  ): NativeReadResult | Promise<NativeReadResult>;
  /** Authority-serving nested/array snapshot owned by an explicit backend open. */
  allRelationSnapshotForBackend?(
    query: PreparedQuery,
    opts: unknown,
  ): NativeReadResult | Promise<NativeReadResult>;
  setIdentityClaims?(author: Uint8Array, claims: Record<string, unknown> | undefined | null): void;
  foregroundTxTimeHighWater?(): bigint;
  seedForegroundTxTimeHighWater?(highWater: bigint): void;
  attachQuery?(query: PreparedQuery, opts: unknown): unknown;
  attachQueryForIdentity?(query: PreparedQuery, author: Uint8Array, opts: unknown): unknown;
  attachQueryForBackend?(query: PreparedQuery, opts: unknown): unknown;
  queryAttachmentIsCovered?(attachment: unknown): boolean;
  detachQuery?(attachment: unknown): void;
  prepareQuery(query: Uint8Array): PreparedQuery;
  subscribe?(query: PreparedQuery, opts: unknown): ReadableStream<unknown> | Subscription;
  subscribeForIdentity?(
    query: PreparedQuery,
    author: Uint8Array,
    opts: unknown,
  ): ReadableStream<unknown> | Subscription;
  /** Authority-serving subscription owned by an explicit backend open. */
  subscribeForBackend?(query: PreparedQuery, opts: unknown): ReadableStream<unknown> | Subscription;
  subscribeRelationQuery?(queryJson: string, opts: unknown): ReadableStream<unknown> | Subscription;
  subscribeRelationQueryForIdentity?(
    queryJson: string,
    author: Uint8Array,
    opts: unknown,
  ): ReadableStream<unknown> | Subscription;
  /** Authority-serving relation subscription owned by an explicit backend open. */
  subscribeRelationQueryForBackend?(
    queryJson: string,
    opts: unknown,
  ): ReadableStream<unknown> | Subscription;
  insertEncoded(table: string, cells: Uint8Array, options?: NativeInsertOptions): Write;
  insertWithIdEncodedAttributed?(
    table: string,
    rowId: Uint8Array,
    cells: Uint8Array,
    author: Uint8Array,
  ): Write;
  updateEncoded(
    table: string,
    rowId: Uint8Array,
    patch: Uint8Array,
    options?: NativeUpdateOptions,
  ): Write;
  updateEncodedAttributed?(
    table: string,
    rowId: Uint8Array,
    patch: Uint8Array,
    author: Uint8Array,
  ): Write;
  upsertEncoded(
    table: string,
    rowId: Uint8Array,
    cells: Uint8Array,
    options?: NativeUpsertOptions,
  ): Write;
  upsertEncodedAttributed?(
    table: string,
    rowId: Uint8Array,
    cells: Uint8Array,
    author: Uint8Array,
  ): Write;
  deleteEncoded(table: string, rowId: Uint8Array, options?: NativeDeleteOptions): Write;
  deleteAttributed?(table: string, rowId: Uint8Array, author: Uint8Array): Write;
  restoreEncoded(
    table: string,
    rowId: Uint8Array,
    cells: Uint8Array,
    options?: NativeRestoreOptions,
  ): Write;
  restoreEncodedAttributed?(
    table: string,
    rowId: Uint8Array,
    cells: Uint8Array,
    author: Uint8Array,
  ): Write;
  beginStreamingMutationEncoded?(
    table: string,
    rowId: Uint8Array,
    cells: Uint8Array,
    column: string,
    mutation?: StreamingMutationKind,
    author?: Uint8Array,
    updatedAtMs?: number,
    head?: unknown,
    base?: unknown,
  ): NativeStreamingMutation;
  beginStreamingMutationAttributedEncoded?(
    table: string,
    rowId: Uint8Array,
    cells: Uint8Array,
    column: string,
    mutation: StreamingMutationKind | undefined,
    author: Uint8Array | undefined,
    attribution: Uint8Array,
    updatedAtMs?: number,
  ): NativeStreamingMutation;
  requestInsertPermissionAdviceEncoded?(
    table: string,
    cells: Uint8Array,
  ): NativePermissionAdviceResult;
  requestReadPermissionAdvice?(table: string, rowId: Uint8Array): NativePermissionAdviceResult;
  requestUpdatePermissionAdviceEncoded?(
    table: string,
    rowId: Uint8Array,
    patch: Uint8Array,
  ): NativePermissionAdviceResult;
  requestDeletePermissionAdvice?(table: string, rowId: Uint8Array): NativePermissionAdviceResult;
  mergeableTx(openTransactionId: OpenTransactionId): Tx;
  mergeableTxForIdentity?(openTransactionId: OpenTransactionId, author: Uint8Array): Tx;
  exclusiveTx?(openTransactionId: OpenTransactionId): Tx;
  /**
   * Transaction-local reads may wait for the owning runtime to finish queued
   * staging work or cold storage. Callers must preserve FIFO visibility by
   * awaiting this result before decoding it.
   */
  allInTransaction?(
    query: PreparedQuery,
    tx: Tx,
    opts: unknown,
  ): NativeReadResult | Promise<NativeReadResult>;
  /** Trusted-serving transaction reads must retain the identity fixed at begin. */
  allInTransactionForIdentity?(
    query: PreparedQuery,
    tx: Tx,
    author: Uint8Array,
    opts: unknown,
  ): NativeReadResult | Promise<NativeReadResult>;
  /** Backend authority transaction read; no public author may select it. */
  allInTransactionForBackend?(
    query: PreparedQuery,
    tx: Tx,
    opts: unknown,
  ): NativeReadResult | Promise<NativeReadResult>;
  /** Relation snapshots must use the transaction's snapshot and staged overlay. */
  allRelationSnapshotInTransaction?(
    query: PreparedQuery,
    tx: Tx,
    opts: unknown,
  ): NativeReadResult | Promise<NativeReadResult>;
  /** Trusted-serving relation snapshots retain the identity fixed at begin. */
  allRelationSnapshotInTransactionForIdentity?(
    query: PreparedQuery,
    tx: Tx,
    author: Uint8Array,
    opts: unknown,
  ): NativeReadResult | Promise<NativeReadResult>;
  /** Backend authority transaction relation snapshot; no public author may select it. */
  allRelationSnapshotInTransactionForBackend?(
    query: PreparedQuery,
    tx: Tx,
    opts: unknown,
  ): NativeReadResult | Promise<NativeReadResult>;
  setTickScheduler(
    callback:
      | ((urgency: "immediate" | "deferred") => void)
      | ((error: Error | null, urgency: string) => void),
  ): void;
  onMutationError(callback: (event: MutationErrorEvent) => void): void;
  setNonDurableClient?(): void;
  /** Exact wire features compiled into the native artifact backing this DB. */
  wireFeatures?(): number;
  setLargeValueStagingPolicy?(
    incomingBytesPerWindow: number,
    windowMs: number,
    maxAgeMs?: number | null,
  ): void;
  evictExpiredStagedLargeValues?(): number | Promise<number>;
  readValueRange?(
    table: string,
    rowId: Uint8Array,
    column: string,
    start: number,
    end: number,
  ): NativeReadResult | Promise<NativeReadResult>;
  readTextUtf16Range?(
    table: string,
    rowId: Uint8Array,
    column: string,
    start: number,
    end: number,
  ): string | PendingNativeRead | Promise<string | PendingNativeRead>;
  readJsonPointer?(
    table: string,
    rowId: Uint8Array,
    column: string,
    pointer: string,
  ): unknown | PendingNativeRead | Promise<unknown | PendingNativeRead>;
  appendValue?(
    table: string,
    rowId: Uint8Array,
    column: string,
    bytes: Uint8Array,
  ): Write | PendingNativeWrite | Promise<Write | PendingNativeWrite>;
  spliceValue?(
    table: string,
    rowId: Uint8Array,
    column: string,
    offset: number,
    deleteLength: number,
    insert: Uint8Array,
  ): Write | PendingNativeWrite | Promise<Write | PendingNativeWrite>;
  updateLargeValuesEncoded?(
    table: string,
    rowId: Uint8Array,
    patch: Uint8Array,
    descriptors: unknown,
    updatedAtMs?: number | null,
  ): Write;
  connectUpstream(): Transport;
  connectUpstreamWithSession?(
    protocolVersion: number,
    features: number,
    remoteNode: Uint8Array,
    remoteEpoch: bigint,
    localNode: Uint8Array,
    localEpoch: bigint,
  ): Transport | Promise<Transport>;
  acceptSubscriber?(author: Uint8Array, claims: Record<string, unknown>): Transport;
  acceptSubscriberWithSelfSignedProof?(
    claims: Record<string, unknown>,
    token: string,
    appId: string,
    claimedAuthor: string,
  ): Transport;
  tick(): void | Promise<void>;
  close?(): void;
  free?(): void;
};

type NativeStreamingMutation = {
  push(chunk: Uint8Array): void | Promise<void>;
  finish(): Write | Promise<Write>;
  abort(): boolean | Promise<boolean>;
};

type NativePermissionAdviceRequest = {
  readonly promise: Promise<PermissionAdvice>;
  cancel(): void;
};

type NativePermissionAdviceResult =
  | NativePermissionAdviceRequest
  | string
  | PendingNativePermissionAdvice;

type PreparedQuery = object;

type Subscription = {
  readAll(): unknown[] | PendingNativeSubscriptionBatch;
  drain?(): unknown[] | PendingNativeSubscriptionBatch;
  close?(): boolean;
};

type Write = {
  readonly txId: string;
  readonly payload: Uint8Array;
  readonly rowId: Uint8Array;
  wait(tier: string): Promise<void>;
  writeState(): unknown;
  close?(): boolean;
};

type Tx = {
  commit(): Write;
  rollback(): void;
  /** Release the native transaction view after its owner batch has completed. */
  close?(): boolean;
  insertEncoded(table: string, cells: Uint8Array, options?: NativeInsertOptions): Uint8Array;
  updateEncoded(
    table: string,
    rowId: Uint8Array,
    patch: Uint8Array,
    options?: NativeUpdateOptions,
  ): void;
  upsertEncoded(
    table: string,
    rowId: Uint8Array,
    cells: Uint8Array,
    options?: NativeUpsertOptions,
  ): void;
  deleteEncoded(table: string, rowId: Uint8Array, options?: NativeDeleteOptions): void;
  restoreEncoded(
    table: string,
    rowId: Uint8Array,
    cells: Uint8Array,
    options?: NativeRestoreOptions,
  ): void;
};

export type Transport = {
  close(): boolean;
  recvWireFrames(): unknown[];
  sendWireFrame(frame: Uint8Array): void;
  sendWireFrames?(frames: readonly Uint8Array[]): void;
  setOutboundScheduler?(callback: () => void): void;
  clearOutboundScheduler?(): void;
  routeAuxiliaryWireFrame?(frame: Uint8Array): unknown | Promise<unknown>;
  recvAuxiliaryWireFrames?(maxFrames?: number, maxBytes?: number): unknown[];
  auxiliaryOutboundReady?(): boolean | Promise<void>;
  /** Bounded redacted diagnostics emitted by the auxiliary chunk relay. */
  takeAuxiliaryTrace?(): unknown[];
  setAuxiliaryTraceEnabled?(enabled: boolean): void;
  tick(): number | Promise<number>;
  updateAuthenticatedClaims?(claims: Record<string, unknown>): void | Promise<void>;
  free?(): void;
};

type PendingTx = {
  id: OpenTransactionId;
  kind: TransactionKind;
  txByView: Map<NativeRuntimeAdapter, Tx>;
  identity?: Uint8Array;
  /** External provenance fixed at begin, never supplied per staged operation. */
  attribution?: Uint8Array;
  writes: PendingTxWrite[];
};

type PendingTxWrite = {
  table: string;
  rowId: Uint8Array;
  row?: RowState;
  deleted?: boolean;
};

type CompletedTx = {
  kind: TransactionKind;
  state: "committed" | "rolled_back";
};

type TransactionOwnerState = {
  pendingTxs: Map<string, PendingTx>;
  completedTxs: Map<string, CompletedTx>;
  writes: Map<string, Write>;
};

type ServerTransportErrorWaiter = {
  active: boolean;
  reject: (error: Error) => void;
};

type ServerTransportWorkWaiter = {
  active: boolean;
  resolve: () => void;
};

type ServerConnectionAttempt = {
  generation: number;
  carrier: WebSocketCarrier;
  terminal: Promise<Error>;
  resolveTerminal: (error: Error) => void;
  finished: boolean;
  outcome: Error | null;
  transport: Transport | null;
  retirement: Promise<void> | null;
};

type AuxiliaryRelayTrace = {
  event: string;
  role: "upstream" | "subscriber";
  connection: string;
  requestId: string;
  remainingHops: number;
  objectHash: string;
  locatorFingerprint: string;
  response?: "found" | "unavailable" | "retryable";
  storageError?: "unavailable" | "locator-conflict" | "integrity" | "backend";
};

type RuntimeSession = {
  issuer: string;
  user_id: string;
  authMode?: string;
  claims: Record<string, unknown>;
  identity: Uint8Array;
  /** Module-owned marker selecting backend authority, never a public subject. */
  backendAuthority: boolean;
};

type SubscriptionState = {
  sources: SubscriptionSourceState[];
  queryJson: string;
  query: PreparedQuery | null;
  identity?: Uint8Array;
  rows: RowState[];
  rowIndexByKey: Map<string, number>;
  visibleRows: RowState[];
  outputColumns: SubscriptionOutputColumns | null;
  session: RuntimeSession | null;
  opts: unknown;
  opened: boolean;
  visibleOpened: boolean;
  deferredVisiblePublication: boolean;
  deferredVisibleReset: boolean;
  deferredTerminalOperations: RuntimeTerminalOperation[];
  deferredPlaceholderChunks: number;
  deferredPlaceholderRows: number;
  deferredPlaceholderBytes: number;
  callback?: (result: RuntimeSubscriptionDelta | Error) => void;
  cancelled: boolean;
};

type SubscriptionOutputColumns = {
  rootTable: string;
  rootColumns: readonly ColumnDescriptor[];
};

type SubscriptionSourceState = {
  source: ReadableStreamDefaultReader<unknown> | Subscription;
  reading: boolean;
};

export type RowState = {
  table: string;
  id: string;
  values: Value[];
  valuesByColumn?: Map<string, Value>;
  resultKey?: string;
  resultKeyBytes?: Uint8Array;
};

type NativeRowFieldPlan = {
  name: string;
  index: number;
  type?: ColumnType;
  storageType: ValueType;
  includeInValues: boolean;
};

const textDecoder = new TextDecoder();
const byteHex = Array.from({ length: 256 }, (_, byte) => byte.toString(16).padStart(2, "0"));
const nativeRowFieldPlanCache = new WeakMap<WasmSchema, Map<string, NativeRowFieldPlan[]>>();
const MAX_DEFERRED_PLACEHOLDER_CHUNKS = 16;
const MAX_DEFERRED_PLACEHOLDER_ROWS = 4_096;
const MAX_DEFERRED_PLACEHOLDER_BYTES = 4 * 1024 * 1024;

function openPersistentDb(
  Runtime: NativeDbConstructor,
  dataPath: string,
  schema: Uint8Array,
  config: Uint8Array,
  selfSignedClientProof?: NativeSelfSignedClientProof,
  backendMode = false,
): NativeDb {
  if (selfSignedClientProof && backendMode) {
    throw new Error("A native runtime cannot be both self-signed and backend-scoped");
  }
  if (backendMode) {
    if (!Runtime.openPersistentAsBackend) {
      throw new Error(
        "Native runtime does not support explicit backend opens; rebuild the matching Jazz native artifact",
      );
    }
    return Runtime.openPersistentAsBackend(dataPath, schema, config);
  }
  if (selfSignedClientProof) {
    if (!Runtime.openPersistentWithSelfSignedProof) {
      throw new Error(
        "Native runtime does not support self-signed client opens; rebuild the matching Jazz native artifact",
      );
    }
    return Runtime.openPersistentWithSelfSignedProof(
      dataPath,
      schema,
      config,
      selfSignedClientProof.token,
      selfSignedClientProof.appId,
      selfSignedClientProof.claimedAuthor,
    );
  }
  if (!Runtime.openPersistent) {
    throw new Error("Native runtime does not expose persistent storage");
  }
  return Runtime.openPersistent(dataPath, schema, config);
}

function openMemoryDb(
  Runtime: NativeDbConstructor,
  schema: Uint8Array,
  config: Uint8Array,
  selfSignedClientProof?: NativeSelfSignedClientProof,
  backendMode = false,
): NativeDb {
  if (selfSignedClientProof && backendMode) {
    throw new Error("A native runtime cannot be both self-signed and backend-scoped");
  }
  if (backendMode) {
    if (!Runtime.openMemoryAsBackend) {
      throw new Error(
        "Native runtime does not support explicit backend opens; rebuild the matching Jazz native artifact",
      );
    }
    return Runtime.openMemoryAsBackend(schema, config);
  }
  if (!selfSignedClientProof) return Runtime.openMemory(schema, config);
  if (!Runtime.openMemoryWithSelfSignedProof) {
    throw new Error(
      "Native runtime does not support self-signed client opens; rebuild the matching Jazz native artifact",
    );
  }
  return Runtime.openMemoryWithSelfSignedProof(
    schema,
    config,
    selfSignedClientProof.token,
    selfSignedClientProof.appId,
    selfSignedClientProof.claimedAuthor,
  );
}

export class NativeRuntimeAdapter implements Runtime {
  private readonly db: NativeDb;
  private readonly schemaBytes: Uint8Array;
  private readonly configBytes: Uint8Array;
  private readonly peerIdentity: Uint8Array;
  private readonly selfSignedClientProof: NativeSelfSignedClientProof | undefined;
  private readonly scopeIsolatedRelay: boolean;
  private readonly schemaHash: string;
  private readonly trustedBackend: boolean;
  private readonly preparedQueries = new Map<string, PreparedQuery>();
  private readonly transactionOwner: TransactionOwnerState;
  private readonly pendingTxs: Map<string, PendingTx>;
  private readonly completedTxs: Map<string, CompletedTx>;
  private readonly writes: Map<string, Write>;
  private readonly pendingLocalSettlements = new Set<Promise<void>>();
  // A streaming mutation can wait on its source while owning a native upload
  // handle. A foreground lease cannot return until every admitted handle has
  // published its write (and advanced the HLC) or has been aborted. Local
  // settlement starts only after a write has already been minted.
  private readonly pendingStreamingMutations = new Set<Promise<void>>();
  // Streaming sources can be consumed concurrently, but native `finish()`
  // mutates the owner runtime. WASM evaluates that runtime behind one async
  // borrow, so overlapping finalization can deadlock. Keep only this atomic
  // publication boundary FIFO; ordinary writes and source reads stay parallel.
  private streamingFinalization: Promise<void> = Promise.resolve();
  private readonly ownerRuntime: NativeRuntimeAdapter;
  private readonly readAuthorizationHost: ReadAuthorizationHost;
  private readonly subscriptions = new Map<number, SubscriptionState>();
  private authFailureCallback: ((reason: string) => void) | null = null;
  private mutationErrorCallback: ((event: MutationErrorEvent) => void) | null = null;
  private serverTransportErrorCallback: ((error: Error) => void) | null = null;
  private readonly deliveredMutationErrors = new Set<string>();
  private serverTransport: Transport | null = null;
  private peerUpstreamAttached = false;
  private nonDurableClient = false;
  private serverCarrier: WebSocketCarrier | null = null;
  private serverCarrierPromise: Promise<WebSocketCarrier> | null = null;
  private serverConnectionAttempt: ServerConnectionAttempt | null = null;
  private serverTransportError: Error | null = null;
  private serverTransportErrorWaiters: ServerTransportErrorWaiter[] = [];
  private serverTransportWorkEpoch = 0;
  private serverTransportWorkWaiters: ServerTransportWorkWaiter[] = [];
  private nextServerConnectionEpoch = 1n;
  private serverEndpointUrl: string | null = null;
  private serverAuthJson: string | null = null;
  private serverReconnectTimer: ReturnType<typeof setTimeout> | null = null;
  private serverReconnectReject: ((error: Error) => void) | null = null;
  private preHelloRetryCount = 0;
  private readonly queuedServerFrames: Uint8Array[] = [];
  private readonly pendingInboundServerFrames: Uint8Array[] = [];
  private serverInboundRouting: Promise<void> = Promise.resolve();
  private serverInboundProcessed = false;
  private readonly peerTransportWorkListeners = new Set<(requiresDistinctPass?: boolean) => void>();
  private readonly auxiliaryTraceListeners = new Set<(entries: AuxiliaryRelayTrace[]) => void>();
  private readonly queryCoverageTraceListeners = new Set<
    (entry: {
      stage: "attach" | "covered";
      peerActivityEpoch: number;
      peerProcessedActivityEpoch: number;
    }) => void
  >();
  private peerTransportActivityEpoch = 0;
  private peerTransportProcessedActivityEpoch = 0;
  // A non-durable follower needs a worker response before trusting native
  // coverage. Full-propagation one-shot reads detach their query, so a later
  // attachment must not reuse the prior attachment's confirmation: the
  // upstream can have changed while no transport was attached. The recorded
  // epoch distinguishes that reattachment from its initial attachment.
  // Local-only reattachments may reuse their own confirmation when no newer
  // worker frame has arrived, scoped to the serving authorization context.
  private readonly peerCoveredQueries = new Map<PreparedQuery, Map<string, number>>();
  private coreTickScheduled = false;
  private coreTickRunning = false;
  private coreTickAgain = false;
  private coreTickCompletion: Promise<void> | null = null;
  private readonly pendingTransportRetirements = new Map<
    Transport,
    Array<{ resolve: () => void; reject: (error: unknown) => void }>
  >();
  private serverPumpScheduled = false;
  private serverPumpRunning = false;
  private serverPumpAgain = false;
  private serverConnectionGeneration = 0;
  private closed = false;
  // A foreground lease handoff closes mutation admission before it snapshots
  // the native HLC, but physical native disposal still happens through the
  // ordinary client shutdown path.
  private foregroundLeaseQuiesced = false;
  // A concurrent ordinary close must not dispose native state between a lease
  // handoff's mutation drain and its HLC readout.
  private foregroundLeaseCapture: Promise<bigint> | null = null;
  private physicalCloseStarted = false;
  private nextSubscriptionId = 1;

  static fromDb(
    db: NativeDb,
    schema: WasmSchema,
    node: Uint8Array,
    author: Uint8Array,
    sourceId: number,
    historyComplete: boolean,
    opts?: Pick<
      NonNullable<ConstructorParameters<typeof NativeRuntimeAdapter>[6]>,
      "selfSignedClientProof" | "scopeIsolatedRelay"
    >,
  ): NativeRuntimeAdapter {
    return new NativeRuntimeAdapter(null, schema, node, author, sourceId, historyComplete, {
      db,
      selfSignedClientProof: opts?.selfSignedClientProof,
      scopeIsolatedRelay: opts?.scopeIsolatedRelay,
    });
  }

  registerSchemaView(schema: WasmSchema): NativeRuntimeAdapter {
    return new NativeRuntimeAdapter(null, schema, this.node, this.peerIdentity, 1, true, {
      db: this.db.registerSchema(encodeSchema(schema)),
      // Registered schema views are façades over this same native open. The
      // capability is private to the owner and must survive view creation so
      // backend reads keep selecting the backend ABI rather than identity ABI.
      backendMode: this.trustedBackend,
      owner: this,
    });
  }

  constructor(
    Runtime: NativeDbConstructor | null,
    private readonly schema: WasmSchema,
    private readonly node: Uint8Array,
    author: Uint8Array,
    // Retained for constructor compatibility; production row IDs must use the core clock source.
    _sourceId: number,
    historyComplete: boolean,
    opts?: {
      persistentPath?: string;
      db?: NativeDb;
      initialSyncFlushEvery?: number;
      selfSignedClientProof?: NativeSelfSignedClientProof;
      scopeIsolatedRelay?: boolean;
      readAuthorizationHost?: ReadAuthorizationHost;
      backendMode?: boolean;
      owner?: NativeRuntimeAdapter;
    },
  ) {
    this.ownerRuntime = opts?.owner?.ownerRuntime ?? this;
    this.readAuthorizationHost =
      opts?.owner?.readAuthorizationHost ?? opts?.readAuthorizationHost ?? "client-local";
    this.transactionOwner = opts?.owner?.transactionOwner ?? {
      pendingTxs: new Map(),
      completedTxs: new Map(),
      writes: new Map(),
    };
    this.pendingTxs = this.transactionOwner.pendingTxs;
    this.completedTxs = this.transactionOwner.completedTxs;
    this.writes = this.transactionOwner.writes;
    this.schemaBytes = encodeSchema(schema);
    this.trustedBackend = (opts?.owner?.trustedBackend ?? opts?.backendMode) === true;
    this.selfSignedClientProof = opts?.selfSignedClientProof;
    this.scopeIsolatedRelay = opts?.scopeIsolatedRelay === true;
    this.configBytes = openConfig(
      node,
      author,
      undefined,
      historyComplete,
      opts?.initialSyncFlushEvery,
      opts?.selfSignedClientProof,
    );
    this.peerIdentity = author;
    this.schemaHash = serializeRuntimeSchema(schema);
    if (opts?.db) {
      this.db = opts.db;
    } else if (opts?.persistentPath) {
      if (!Runtime) {
        throw new Error("Native runtime constructor required for persistent storage");
      }
      this.db = openPersistentDb(
        Runtime,
        opts.persistentPath,
        this.schemaBytes,
        this.configBytes,
        opts?.selfSignedClientProof,
        opts?.backendMode,
      );
    } else {
      if (!Runtime) {
        throw new Error("Native runtime constructor required for memory storage");
      }
      this.db = openMemoryDb(
        Runtime,
        this.schemaBytes,
        this.configBytes,
        opts?.selfSignedClientProof,
        opts?.backendMode,
      );
    }
    if (opts?.owner) return;
    if (typeof this.db.setTickScheduler !== "function") {
      throw new Error("Native runtime requires db.setTickScheduler");
    }
    this.db.setTickScheduler(((first: Error | string | null, second?: string) => {
      const urgency = typeof first === "string" ? first : second;
      if (
        urgency === "immediate" ||
        urgency === "deferred" ||
        urgency === "after-current-turn" ||
        (typeof urgency === "string" && urgency.startsWith("after:"))
      ) {
        this.scheduleCoreWake(urgency as CoreTickWake);
      }
    }) as (error: Error | null, urgency: string) => void);
  }

  connectUpstreamPeer(): Transport {
    if (this !== this.ownerRuntime) return this.ownerRuntime.connectUpstreamPeer();
    this.peerUpstreamAttached = true;
    return this.db.connectUpstream();
  }

  onPeerTransportWork(listener: (requiresDistinctPass?: boolean) => void): () => void {
    if (this !== this.ownerRuntime) return this.ownerRuntime.onPeerTransportWork(listener);
    this.peerTransportWorkListeners.add(listener);
    return () => {
      this.peerTransportWorkListeners.delete(listener);
    };
  }

  /** Subscribe to bounded, redacted auxiliary-relay diagnostics. */
  onAuxiliaryTrace(listener: (entries: AuxiliaryRelayTrace[]) => void): () => void {
    if (this !== this.ownerRuntime) return this.ownerRuntime.onAuxiliaryTrace(listener);
    this.auxiliaryTraceListeners.add(listener);
    this.serverTransport?.setAuxiliaryTraceEnabled?.(true);
    return () => {
      this.auxiliaryTraceListeners.delete(listener);
      if (this.auxiliaryTraceListeners.size === 0) {
        this.serverTransport?.setAuxiliaryTraceEnabled?.(false);
      }
    };
  }

  /** @internal Redacted browser-worker lifecycle diagnostics. */
  onQueryCoverageTrace(
    listener: (entry: {
      stage: "attach" | "covered";
      peerActivityEpoch: number;
      peerProcessedActivityEpoch: number;
    }) => void,
  ): () => void {
    if (this !== this.ownerRuntime) return this.ownerRuntime.onQueryCoverageTrace(listener);
    this.queryCoverageTraceListeners.add(listener);
    return () => this.queryCoverageTraceListeners.delete(listener);
  }

  private emitQueryCoverageTrace(stage: "attach" | "covered"): void {
    if (this !== this.ownerRuntime) {
      this.ownerRuntime.emitQueryCoverageTrace(stage);
      return;
    }
    const entry = {
      stage,
      peerActivityEpoch: this.peerTransportActivityEpoch,
      peerProcessedActivityEpoch: this.peerTransportProcessedActivityEpoch,
    };
    for (const listener of this.queryCoverageTraceListeners) listener(entry);
  }

  notifyPeerTransportActivity(): void {
    if (this !== this.ownerRuntime) return this.ownerRuntime.notifyPeerTransportActivity();
    this.peerTransportActivityEpoch += 1;
    // External peer ingress can alter the result visible through every peer,
    // including one whose evaluator pass began before this frame arrived.
    // Require a distinct post-admission pass; routine end-of-pass work remains
    // coalescible and therefore cannot create a self-sustaining pump loop.
    this.notifyPeerTransportWork(true);
  }

  async progressPeerTransport(): Promise<void> {
    if (this !== this.ownerRuntime) return this.ownerRuntime.progressPeerTransport();
    const activityReadyForProcessing = this.peerTransportActivityEpoch;
    await this.runCoreTick();
    this.peerTransportProcessedActivityEpoch = Math.max(
      this.peerTransportProcessedActivityEpoch,
      activityReadyForProcessing,
    );
  }

  retirePeerTransport(transport: Transport): Promise<void> {
    if (this !== this.ownerRuntime) return this.ownerRuntime.retirePeerTransport(transport);
    if (!this.coreTickRunning) {
      transport.close();
      return Promise.resolve();
    }
    return new Promise<void>((resolve, reject) => {
      const waiters = this.pendingTransportRetirements.get(transport) ?? [];
      waiters.push({ resolve, reject });
      this.pendingTransportRetirements.set(transport, waiters);
    });
  }

  setNonDurableClient(): void {
    if (this !== this.ownerRuntime) return this.ownerRuntime.setNonDurableClient();
    if (!this.db.setNonDurableClient) {
      throw new Error("Native runtime does not expose non-durable client mode");
    }
    this.db.setNonDurableClient();
    this.nonDurableClient = true;
  }

  /** @internal Return the native HLC high-water for a foreground lease handoff. */
  foregroundTxTimeHighWater(): bigint {
    if (this !== this.ownerRuntime) return this.ownerRuntime.foregroundTxTimeHighWater();
    if (!this.db.foregroundTxTimeHighWater) {
      throw new Error("Native runtime does not expose foreground transaction high-water");
    }
    return this.db.foregroundTxTimeHighWater();
  }

  /**
   * Close mutation admission, drain already-started native work, then return
   * the final HLC high-water. No synchronous or async write can mint after
   * this resolves. The ordinary `close()` call remains responsible for final
   * native disposal after the lease owner has durably recorded this value.
   * @internal
   */
  quiesceForegroundTxTimeHighWater(): Promise<bigint> {
    if (this !== this.ownerRuntime) {
      return this.ownerRuntime.quiesceForegroundTxTimeHighWater();
    }
    if (this.foregroundLeaseCapture) return this.foregroundLeaseCapture;
    if (this.closed) throw new Error("Native runtime is already closed");
    this.closed = true;
    this.foregroundLeaseQuiesced = true;
    const capture = this.captureForegroundTxTimeHighWater();
    this.foregroundLeaseCapture = capture;
    return capture;
  }

  private async captureForegroundTxTimeHighWater(): Promise<bigint> {
    await Promise.all(this.pendingStreamingMutations);
    await Promise.all(this.pendingLocalSettlements);
    await this.coreTickCompletion?.catch(() => undefined);
    return this.foregroundTxTimeHighWater();
  }

  /** @internal Seed a foreground lease high-water before the first local write. */
  seedForegroundTxTimeHighWater(highWater: bigint): void {
    if (this !== this.ownerRuntime) {
      return this.ownerRuntime.seedForegroundTxTimeHighWater(highWater);
    }
    if (!this.db.seedForegroundTxTimeHighWater) {
      throw new Error("Native runtime does not expose foreground transaction high-water seeding");
    }
    this.db.seedForegroundTxTimeHighWater(highWater);
  }

  /** Configure Jazz-owned upload rate and unpublished-tree expiry policy. */
  setLargeValueStagingPolicy(
    incomingBytesPerWindow: number,
    windowMs: number,
    maxAgeMs?: number | null,
  ): void {
    if (this !== this.ownerRuntime) {
      return this.ownerRuntime.setLargeValueStagingPolicy(
        incomingBytesPerWindow,
        windowMs,
        maxAgeMs,
      );
    }
    if (!this.db.setLargeValueStagingPolicy) {
      throw new Error("Native runtime does not expose large-value staging policy");
    }
    this.db.setLargeValueStagingPolicy(incomingBytesPerWindow, windowMs, maxAgeMs);
  }

  /** Run one idempotent expiry pass from an environment-owned timer. */
  async evictExpiredStagedLargeValues(): Promise<number> {
    if (this !== this.ownerRuntime) {
      return await this.ownerRuntime.evictExpiredStagedLargeValues();
    }
    if (!this.db.evictExpiredStagedLargeValues) {
      throw new Error("Native runtime does not expose large-value staging maintenance");
    }
    return await this.db.evictExpiredStagedLargeValues();
  }

  async readValueRange(
    table: string,
    objectId: string,
    column: string,
    start: number,
    end: number,
  ): Promise<Uint8Array> {
    if (this !== this.ownerRuntime) {
      return await this.ownerRuntime.readValueRange(table, objectId, column, start, end);
    }
    if (!this.db.readValueRange) throw new Error("Native runtime does not expose value ranges");
    return this.awaitNativeRead(
      this.db.readValueRange(table, parseUuid(objectId), column, start, end),
    );
  }

  async readTextUtf16Range(
    table: string,
    objectId: string,
    column: string,
    start: number,
    end: number,
  ): Promise<string> {
    if (this !== this.ownerRuntime) {
      return await this.ownerRuntime.readTextUtf16Range(table, objectId, column, start, end);
    }
    if (!this.db.readTextUtf16Range) {
      throw new Error("Native runtime does not expose UTF-16 value ranges");
    }
    const result = await this.db.readTextUtf16Range(table, parseUuid(objectId), column, start, end);
    return isPendingNativeRead(result)
      ? new TextDecoder().decode(await this.awaitNativeRead(result))
      : result;
  }

  async readJsonPointer(
    table: string,
    objectId: string,
    column: string,
    pointer: string,
  ): Promise<unknown> {
    if (this !== this.ownerRuntime) {
      return await this.ownerRuntime.readJsonPointer(table, objectId, column, pointer);
    }
    if (!this.db.readJsonPointer) throw new Error("Native runtime does not expose JSON pointers");
    let value = await this.db.readJsonPointer(table, parseUuid(objectId), column, pointer);
    if (isPendingNativeRead(value)) {
      value = new TextDecoder().decode(await this.awaitNativeRead(value));
    }
    return typeof value === "string" ? JSON.parse(value) : value;
  }

  async appendValue(
    table: string,
    objectId: string,
    column: string,
    bytes: Uint8Array,
  ): Promise<MutationResult> {
    if (this !== this.ownerRuntime) {
      return await this.ownerRuntime.appendValue(table, objectId, column, bytes);
    }
    if (!this.db.appendValue) throw new Error("Native runtime does not expose value append");
    const write = await this.db.appendValue(table, parseUuid(objectId), column, bytes);
    return this.finishMutation(
      isPendingNativeWrite(write) ? await this.awaitNativeWrite(write) : write,
    );
  }

  async spliceValue(
    table: string,
    objectId: string,
    column: string,
    offset: number,
    deleteLength: number,
    insert: Uint8Array,
  ): Promise<MutationResult> {
    if (this !== this.ownerRuntime) {
      return await this.ownerRuntime.spliceValue(
        table,
        objectId,
        column,
        offset,
        deleteLength,
        insert,
      );
    }
    if (!this.db.spliceValue) throw new Error("Native runtime does not expose value splice");
    const write = await this.db.spliceValue(
      table,
      parseUuid(objectId),
      column,
      offset,
      deleteLength,
      insert,
    );
    return this.finishMutation(
      isPendingNativeWrite(write) ? await this.awaitNativeWrite(write) : write,
    );
  }

  acceptPeer(claims: Record<string, unknown> = {}): Transport {
    if (this !== this.ownerRuntime) return this.ownerRuntime.acceptPeer(claims);
    const proof = this.selfSignedClientProof;
    if (proof) {
      if (!this.db.acceptSubscriberWithSelfSignedProof) {
        throw new Error(
          "Native runtime does not support self-signed subscriber admission; rebuild the matching Jazz WASM artifact",
        );
      }
      return this.db.acceptSubscriberWithSelfSignedProof(
        claims,
        proof.token,
        proof.appId,
        proof.claimedAuthor,
      );
    }
    if (!this.db.acceptSubscriber) {
      throw new Error("Native runtime does not expose subscriber links");
    }
    return this.db.acceptSubscriber(this.peerIdentity, claims);
  }

  async acceptPeerWhenIdle(claims: Record<string, unknown> = {}): Promise<Transport> {
    if (this !== this.ownerRuntime) return this.ownerRuntime.acceptPeerWhenIdle(claims);
    await this.waitForCoreIdle();
    // No await separates the idle check from admission, so another browser
    // task cannot enter the evaluator and borrow the connection registry here.
    return this.acceptPeer(claims);
  }

  private async waitForCoreIdle(): Promise<void> {
    const owner = this.ownerRuntime;
    while (owner.coreTickRunning) {
      await owner.coreTickCompletion;
    }
  }

  async waitForUpstreamServerConnection(): Promise<void> {
    if (this !== this.ownerRuntime) {
      return await this.ownerRuntime.waitForUpstreamServerConnection();
    }
    if (!this.serverCarrierPromise) {
      if (this.serverTransportError) throw this.serverTransportError;
      return;
    }
    await this.serverCarrierPromise;
  }

  /** @internal */
  isClosed(): boolean {
    return this.ownerRuntime.closed;
  }

  async close(): Promise<void> {
    if (this !== this.ownerRuntime) {
      this.closeRuntimeState();
      return;
    }
    if (this.physicalCloseStarted) return;
    if (this.closed && !this.foregroundLeaseQuiesced) return;
    this.physicalCloseStarted = true;
    // Stop admitting/scheduling work first, but keep every WASM receiver alive
    // until the evaluator future that may currently borrow it has unwound.
    this.closed = true;
    await this.foregroundLeaseCapture?.catch(() => undefined);
    if (this.pendingStreamingMutations.size > 0) {
      await Promise.all(this.pendingStreamingMutations);
    }
    if (this.pendingLocalSettlements.size > 0) {
      await Promise.all(this.pendingLocalSettlements);
    }
    await this.coreTickCompletion?.catch(() => undefined);
    this.closeRuntimeState(true);
    await this.db.close?.();
    // wasm-bindgen futures may retain this receiver after logical closure.
    // Its registered finalizer owns physical release; explicit `free()` here
    // creates a second, unsynchronised lifetime and can corrupt the WASM heap.
  }

  /** Discard a runtime whose persistence epoch is no longer usable. */
  discard(): void {
    // Do not explicitly free the WASM wrapper here. A forced IndexedDB close
    // can reject a storage Promise while a WASM future is still unwinding;
    // freeing its receiver in that window is unsafe. Sever all runtime work
    // and let the now-unreferenced wrapper be garbage-collected instead.
    this.closeRuntimeState();
  }

  private closeRuntimeState(alreadyMarkedClosed = false): boolean {
    if (this.closed && !alreadyMarkedClosed) return false;
    this.closed = true;
    for (const subscription of this.subscriptions.values()) {
      for (const source of subscription.sources) {
        closeSubscriptionSource(source.source);
      }
    }
    // Prepared plans and coverage receipts are valid only while this runtime
    // is live. Release them before closing the native owner so long-lived JS
    // Db wrappers cannot retain stale native graph/storage state through a
    // cache after their context has shut down.
    this.preparedQueries.clear();
    this.peerCoveredQueries.clear();
    if (this !== this.ownerRuntime) {
      this.subscriptions.clear();
      // A schema view can own an attached transaction handle while its parent
      // owns the batch. Closing this view must release only that handle; the
      // parent still needs its batch and sibling views to complete it.
      for (const pending of this.pendingTxs.values()) {
        const tx = pending.txByView.get(this);
        if (!tx) continue;
        tx.close?.();
        pending.txByView.delete(this);
      }
      // Query and subscription futures may still be unwinding through this
      // schema-view wrapper. Eagerly freeing a wasm-bindgen receiver here is a
      // use-after-free; let GC release the Rc-backed view after those promises
      // drop their references.
      return false;
    }
    for (const write of this.writes.values()) {
      write.close?.();
    }
    this.subscriptions.clear();
    for (const pending of this.pendingTxs.values()) {
      this.releaseTransactionViews(pending);
    }
    this.pendingTxs.clear();
    this.completedTxs.clear();
    this.writes.clear();
    this.serverConnectionGeneration += 1;
    this.clearServerReconnectTimer();
    const connectionAttempt = this.serverConnectionAttempt;
    this.serverConnectionAttempt = null;
    if (connectionAttempt) {
      this.finishServerConnectionAttempt(connectionAttempt, new Error("runtime closed"));
    }
    this.serverCarrierPromise = null;
    this.clearServerTransportErrorWaiters();
    this.resolveServerTransportWorkWaiters();
    this.peerTransportWorkListeners.clear();
    this.queuedServerFrames.length = 0;
    this.pendingInboundServerFrames.length = 0;
    const serverTransport = this.serverTransport;
    this.serverTransport = null;
    if (serverTransport) {
      void this.retirePeerTransport(serverTransport).catch(reportAsyncRuntimeError);
    }
    this.peerUpstreamAttached = false;
    this.serverCarrier?.close();
    this.serverCarrier = null;
    return true;
  }

  insert(
    table: string,
    values: InsertValues,
    _writeContext?: string | null,
    objectId?: string | null,
  ): InsertResult {
    this.assertMutationAdmission("Insert");
    const suppliedRowId = objectId ? parseUuid(objectId) : undefined;
    const writeSession = sessionFromWriteContext(_writeContext);
    this.applySessionClaims(writeSession);
    const writeIdentity = this.trustedWriteIdentity(writeSession);
    const attribution = this.backendAttribution(_writeContext);
    const updatedAtMs = effectiveUpdatedAtMs(_writeContext);
    const branchView = branchViewFromWriteContext(_writeContext);
    rejectAttributedBranchWrite(attribution, branchView);
    const tx = this.currentTx(_writeContext, "Insert");
    if (tx) this.assertTransactionAttribution(tx, attribution);
    const attributedInsert =
      attribution && !tx
        ? requireBackendAttributionAbi(this.db.insertWithIdEncodedAttributed, "insert")
        : undefined;
    const cells = encodeCellsForRow(this.table(table), values);
    if (tx) {
      const nativeTx = this.txForWrite(tx, attribution ? undefined : writeIdentity);
      const rowId = nativeTx.insertEncoded(table, cells, {
        rowId: suppliedRowId,
        branch: branchView?.head,
        updatedAtMs: updatedAtMs ?? undefined,
      });
      const row = this.rowStateFromValues(table, rowId, values);
      tx.writes.push({ table, rowId, row });
      return {
        id: row.id,
        values: row.values,
        kind: "staged",
        openTransactionId: txIdFromContext(_writeContext)!,
      };
    }
    const write = writeOrNormalizeRejection("Insert", () => {
      if (attribution) {
        return attributedInsert!.call(
          this.db,
          table,
          suppliedRowId ?? crypto.getRandomValues(new Uint8Array(16)),
          cells,
          attribution,
        );
      }
      return this.db.insertEncoded(table, cells, {
        rowId: suppliedRowId,
        author: writeIdentity,
        branch: branchView?.head,
        updatedAtMs: updatedAtMs ?? undefined,
      });
    });
    return this.finishInsert(table, suppliedRowId ?? write.rowId, values, write);
  }

  async streamingMutation(
    mutation: StreamingMutationKind,
    table: string,
    values: InsertValues,
    column: string,
    source: StreamingValueSource,
    writeContext?: string | null,
    objectId?: string | null,
  ): Promise<StreamingInsertResult> {
    const begin = this.db.beginStreamingMutationEncoded;
    const operation =
      mutation === "insert" ? "Insert" : mutation === "update" ? "Update" : "Upsert";
    this.assertMutationAdmission(operation);
    if (this.currentTx(writeContext, operation)) {
      throw new Error("Streaming mutations are not supported inside a transaction");
    }
    const writeSession = sessionFromWriteContext(writeContext);
    this.applySessionClaims(writeSession);
    const writeIdentity = this.trustedWriteIdentity(writeSession);
    const attribution = this.backendAttribution(writeContext);
    const branchView = branchViewFromWriteContext(writeContext);
    const updatedAtMs = effectiveUpdatedAtMs(writeContext);
    rejectAttributedBranchWrite(attribution, branchView);
    const attributedBegin = attribution
      ? requireBackendAttributionAbi(
          this.db.beginStreamingMutationAttributedEncoded,
          "streaming mutations",
        )
      : undefined;
    const ordinaryBegin = attribution
      ? undefined
      : requireBackendAttributionAbi(begin, "streaming mutations");

    const definition = this.table(table);
    const descriptor = definition.columns.find((candidate) => candidate.name === column);
    const kind = descriptor?.column_type.type;
    if (kind !== "Text" && kind !== "Json" && kind !== "Bytea") {
      throw new Error(
        `Streaming insert requires a Text, Json, or Bytea column: ${table}.${column}`,
      );
    }
    const rowId = objectId ? parseUuid(objectId) : crypto.getRandomValues(new Uint8Array(16));
    const cells =
      mutation === "insert"
        ? encodeCellsForStreamingRow(definition, values, column, table)
        : encodeCellsForStreamingPatch(definition, values, column);
    const upload = attribution
      ? attributedBegin!.call(
          this.db,
          table,
          rowId,
          cells,
          column,
          mutation,
          undefined,
          attribution,
          updatedAtMs ?? undefined,
        )
      : ordinaryBegin!.call(
          this.db,
          table,
          rowId,
          cells,
          column,
          mutation,
          writeIdentity,
          updatedAtMs ?? undefined,
          branchView?.head,
          branchView?.base,
        );
    const owner = this.ownerRuntime;
    let releaseAdmission!: () => void;
    const admission = new Promise<void>((resolve) => {
      releaseAdmission = resolve;
    });
    owner.pendingStreamingMutations.add(admission);
    const encoder = new TextEncoder();
    const pushBounded = async (bytes: Uint8Array): Promise<void> => {
      const hostWindowBytes = 64 * 1024;
      for (let offset = 0; offset < bytes.byteLength; offset += hostWindowBytes) {
        await upload.push(bytes.subarray(offset, offset + hostWindowBytes));
      }
    };
    let pendingHighSurrogate = "";
    try {
      for await (const chunk of streamingChunks(source)) {
        if (typeof chunk === "string") {
          if (kind === "Bytea") throw new Error("Bytea streams require Uint8Array chunks");
          let text = pendingHighSurrogate + chunk;
          pendingHighSurrogate = "";
          const trailing = text.charCodeAt(text.length - 1);
          if (trailing >= 0xd800 && trailing <= 0xdbff) {
            pendingHighSurrogate = text.slice(-1);
            text = text.slice(0, -1);
          }
          if (text.length > 0) await pushBounded(encoder.encode(text));
        } else if (chunk instanceof Uint8Array) {
          if (pendingHighSurrogate) {
            await pushBounded(encoder.encode(pendingHighSurrogate));
            pendingHighSurrogate = "";
          }
          await pushBounded(chunk);
        } else {
          throw new Error("Streaming insert chunks must be strings or Uint8Array values");
        }
      }
      if (pendingHighSurrogate) await pushBounded(encoder.encode(pendingHighSurrogate));
      const previousFinalization = owner.streamingFinalization;
      let releaseFinalization!: () => void;
      const finalization = new Promise<void>((resolve) => {
        releaseFinalization = resolve;
      });
      owner.streamingFinalization = previousFinalization.then(() => finalization);
      await previousFinalization;
      let receipt: MutationResult;
      try {
        receipt = this.finishMutation(await upload.finish());
      } finally {
        releaseFinalization();
      }
      return { id: formatUuid(rowId), ...receipt };
    } catch (error) {
      await upload.abort();
      throw error;
    } finally {
      releaseAdmission();
      owner.pendingStreamingMutations.delete(admission);
    }
  }

  restore(
    table: string,
    objectId: string,
    values: InsertValues,
    writeContext?: string | null,
  ): InsertResult {
    this.assertMutationAdmission("Restore");
    const rowId = parseUuid(objectId);
    const writeSession = sessionFromWriteContext(writeContext);
    this.applySessionClaims(writeSession);
    const writeIdentity = this.trustedWriteIdentity(writeSession);
    const attribution = this.backendAttribution(writeContext);
    const updatedAtMs = effectiveUpdatedAtMs(writeContext);
    const branchView = branchViewFromWriteContext(writeContext);
    rejectAttributedBranchWrite(attribution, branchView);
    const tx = this.currentTx(writeContext, "Restore");
    if (tx) this.assertTransactionAttribution(tx, attribution);
    const attributedRestore =
      attribution && !tx
        ? requireBackendAttributionAbi(this.db.restoreEncodedAttributed, "restore")
        : undefined;
    const cells = encodeCellsForRow(this.table(table), values);
    if (tx) {
      const nativeTx = this.txForWrite(tx, attribution ? undefined : writeIdentity);
      nativeTx.restoreEncoded(table, rowId, cells, {
        branch: branchView?.head,
        updatedAtMs: updatedAtMs ?? undefined,
      });
      const row = this.rowStateFromValues(table, rowId, values);
      tx.writes.push({ table, rowId, row });
      return {
        id: row.id,
        values: row.values,
        kind: "staged",
        openTransactionId: txIdFromContext(writeContext)!,
      };
    }
    const write = writeOrNormalizeRejection("Restore", () => {
      if (attribution) {
        return attributedRestore!.call(this.db, table, rowId, cells, attribution);
      }
      return this.db.restoreEncoded(table, rowId, cells, {
        author: writeIdentity,
        branch: branchView?.head,
        updatedAtMs: updatedAtMs ?? undefined,
      });
    });
    return this.finishInsert(table, rowId, values, write);
  }

  update(
    table: string,
    objectId: string,
    values: Record<string, Value>,
    writeContext?: string | null,
  ): MutationResult {
    this.assertMutationAdmission("Update");
    const rowId = parseUuid(objectId);
    const writeSession = sessionFromWriteContext(writeContext);
    this.applySessionClaims(writeSession);
    const writeIdentity = this.trustedWriteIdentity(writeSession);
    const attribution = this.backendAttribution(writeContext);
    const updatedAtMs = effectiveUpdatedAtMs(writeContext);
    const branchView = branchViewFromWriteContext(writeContext);
    rejectAttributedBranchWrite(attribution, branchView);
    const tx = this.currentTx(writeContext, "Update");
    if (tx) this.assertTransactionAttribution(tx, attribution);
    const attributedUpdate =
      attribution && !tx
        ? requireBackendAttributionAbi(this.db.updateEncodedAttributed, "update")
        : undefined;
    const patch = encodeCellsForPatch(this.table(table), values);
    if (tx) {
      const nativeTx = this.txForWrite(tx, attribution ? undefined : writeIdentity);
      nativeTx.updateEncoded(table, rowId, patch, {
        head: branchView?.head,
        base: branchView?.base,
        updatedAtMs: updatedAtMs ?? undefined,
      });
      tx.writes.push({
        table,
        rowId,
        row: this.mergeRowState(table, rowId, values, tx, writeIdentity),
      });
      return { kind: "staged", openTransactionId: txIdFromContext(writeContext)! };
    }
    const write = writeOrNormalizeRejection("Update", () => {
      if (attribution) {
        return attributedUpdate!.call(this.db, table, rowId, patch, attribution);
      }
      return this.db.updateEncoded(table, rowId, patch, {
        author: writeIdentity,
        head: branchView?.head,
        base: branchView?.base,
        updatedAtMs: updatedAtMs ?? undefined,
      });
    });
    return this.finishMutation(write);
  }

  updateLargeValues(
    table: string,
    objectId: string,
    values: Record<string, Value>,
    descriptors: readonly unknown[],
    writeContext?: string | null,
  ): MutationResult {
    this.assertMutationAdmission("Update");
    const rowId = parseUuid(objectId);
    const updatedAtMs = effectiveUpdatedAtMs(writeContext);
    const branchView = branchViewFromWriteContext(writeContext);
    const tx = this.currentTx(writeContext, "Update");

    // The first partial-value API is intentionally root-context only (#2087).
    // Do not silently substitute the adapter's root author for a session or
    // attributed write, nor read/stage through a transaction or branch view.
    if (branchView) {
      throw new Error("Typed large-value updates are not supported in branch views.");
    }
    if (tx) {
      throw writeError("Update", "typed partial-value updates are not supported in transactions");
    }
    if (largeValueWriteHasIncompatibleIdentity(writeContext, this.peerIdentity)) {
      throw new Error("Typed large-value updates do not yet support an attributed identity.");
    }
    const updateLargeValues = this.db.updateLargeValuesEncoded;
    if (!updateLargeValues) {
      throw new Error("Native runtime does not support typed partial-value updates.");
    }
    const patch = encodeCellsForPatch(this.table(table), values);
    const write = writeOrNormalizeRejection("Update", () =>
      updateLargeValues.call(this.db, table, rowId, patch, descriptors, updatedAtMs ?? undefined),
    );
    return this.finishMutation(write);
  }

  upsert(
    table: string,
    objectId: string,
    values: InsertValues,
    writeContext?: string | null,
  ): MutationResult {
    this.assertMutationAdmission("Upsert");
    const rowId = parseUuid(objectId);
    const definition = this.table(table);
    const writeSession = sessionFromWriteContext(writeContext);
    this.applySessionClaims(writeSession);
    const writeIdentity = this.trustedWriteIdentity(writeSession);
    const attribution = this.backendAttribution(writeContext);
    const updatedAtMs = effectiveUpdatedAtMs(writeContext);
    const branchView = branchViewFromWriteContext(writeContext);
    rejectAttributedBranchWrite(attribution, branchView);
    const tx = this.currentTx(writeContext, "Upsert");
    if (tx) this.assertTransactionAttribution(tx, attribution);
    const attributedUpsert =
      attribution && !tx
        ? requireBackendAttributionAbi(this.db.upsertEncodedAttributed, "upsert")
        : undefined;
    const existing = branchView
      ? true
      : tx
        ? (this.stagedRowForWriteMerge(tx, table, rowId) ?? this.readRowForWriteMerge(table, rowId))
        : this.readRow(table, rowId, writeIdentity);
    let cells: Uint8Array;
    try {
      cells = existing
        ? encodeCellsForPatch(definition, values)
        : encodeCellsForRow(definition, values, table);
    } catch (error) {
      throw writeError("Upsert", normalizeWriteSetupMessage(errorMessage(error)));
    }
    if (tx) {
      this.txForWrite(tx, attribution ? undefined : writeIdentity).upsertEncoded(
        table,
        rowId,
        cells,
        {
          head: branchView?.head,
          base: branchView?.base,
          updatedAtMs: updatedAtMs ?? undefined,
        },
      );
      tx.writes.push({
        table,
        rowId,
        row: existing
          ? this.mergeRowState(table, rowId, values, tx, writeIdentity)
          : this.rowStateFromValues(table, rowId, values),
      });
      return { kind: "staged", openTransactionId: txIdFromContext(writeContext)! };
    }
    const write = writeOrNormalizeRejection("Upsert", () => {
      if (attribution) {
        return attributedUpsert!.call(this.db, table, rowId, cells, attribution);
      }
      return this.db.upsertEncoded(table, rowId, cells, {
        author: writeIdentity,
        head: branchView?.head,
        base: branchView?.base,
        updatedAtMs: updatedAtMs ?? undefined,
      });
    });
    return this.finishMutation(write);
  }

  delete(table: string, objectId: string, writeContext?: string | null): MutationResult {
    this.assertMutationAdmission("Delete");
    this.table(table);
    const rowId = parseUuid(objectId);
    const writeSession = sessionFromWriteContext(writeContext);
    this.applySessionClaims(writeSession);
    const writeIdentity = this.trustedWriteIdentity(writeSession);
    const attribution = this.backendAttribution(writeContext);
    const updatedAtMs = effectiveUpdatedAtMs(writeContext);
    const branchView = branchViewFromWriteContext(writeContext);
    rejectAttributedBranchWrite(attribution, branchView);
    const tx = this.currentTx(writeContext, "Delete");
    if (tx) this.assertTransactionAttribution(tx, attribution);
    const attributedDelete =
      attribution && !tx
        ? requireBackendAttributionAbi(this.db.deleteAttributed, "delete")
        : undefined;
    if (tx) {
      const nativeTx = this.txForWrite(tx, attribution ? undefined : writeIdentity);
      nativeTx.deleteEncoded(table, rowId, {
        head: branchView?.head,
        base: branchView?.base,
        updatedAtMs: updatedAtMs ?? undefined,
      });
      tx.writes.push({ table, rowId, deleted: true });
      return { kind: "staged", openTransactionId: txIdFromContext(writeContext)! };
    }
    const write = writeOrNormalizeRejection("Delete", () => {
      if (attribution) {
        return attributedDelete!.call(this.db, table, rowId, attribution);
      }
      return this.db.deleteEncoded(table, rowId, {
        author: writeIdentity,
        head: branchView?.head,
        base: branchView?.base,
        updatedAtMs: updatedAtMs ?? undefined,
      });
    });
    return this.finishMutation(write);
  }

  canInsertLocally(table: string, values: InsertValues, session?: Session): PermissionAdvice {
    void table;
    void values;
    void session;
    return "unknown";
  }

  canReadLocally(table: string, objectId: string, session?: Session): PermissionAdvice {
    void table;
    void objectId;
    void session;
    return "unknown";
  }

  canUpdateLocally(
    table: string,
    objectId: string,
    values: Record<string, Value>,
    session?: Session,
  ): PermissionAdvice {
    void table;
    void objectId;
    void values;
    void session;
    return "unknown";
  }

  canDeleteLocally(table: string, objectId: string, session?: Session): PermissionAdvice {
    void table;
    void objectId;
    void session;
    return "unknown";
  }

  requestInsertPermissionAdvice(
    table: string,
    values: InsertValues,
    _session?: Session,
  ): Promise<PermissionAdvice> {
    const request = this.db.requestInsertPermissionAdviceEncoded;
    if (!request) return Promise.resolve("unknown");
    const cells = encodeCellsForRow(this.table(table), values, table);
    return this.withPermissionAdviceTimeout(() => request.call(this.db, table, cells));
  }

  requestReadPermissionAdvice(
    table: string,
    objectId: string,
    _session?: Session,
  ): Promise<PermissionAdvice> {
    const request = this.db.requestReadPermissionAdvice;
    if (!request) return Promise.resolve("unknown");
    return this.withPermissionAdviceTimeout(() =>
      request.call(this.db, table, parseUuid(objectId)),
    );
  }

  requestUpdatePermissionAdvice(
    table: string,
    objectId: string,
    values: Record<string, Value>,
    _session?: Session,
  ): Promise<PermissionAdvice> {
    const request = this.db.requestUpdatePermissionAdviceEncoded;
    if (!request) return Promise.resolve("unknown");
    const patch = encodeCellsForPatch(this.table(table), values);
    return this.withPermissionAdviceTimeout(() =>
      request.call(this.db, table, parseUuid(objectId), patch),
    );
  }

  requestDeletePermissionAdvice(
    table: string,
    objectId: string,
    _session?: Session,
  ): Promise<PermissionAdvice> {
    const request = this.db.requestDeletePermissionAdvice;
    if (!request) return Promise.resolve("unknown");
    return this.withPermissionAdviceTimeout(() =>
      request.call(this.db, table, parseUuid(objectId)),
    );
  }

  beginTransaction(
    kind: TransactionKind,
    id: OpenTransactionId,
    sessionJson?: string | null,
  ): OpenTransactionId {
    if (this !== this.ownerRuntime) {
      return this.ownerRuntime.beginTransaction(kind, id, sessionJson);
    }
    if (this.closed) {
      throw new Error("Begin transaction failed: native runtime is closed");
    }
    if (this.pendingTxs.has(id) || this.completedTxs.has(id)) {
      throw new Error(`Begin transaction failed: transaction ${id} has already been opened`);
    }
    const session = sessionFromWriteContext(sessionJson);
    this.applySessionClaims(session);
    // The native core binds an exclusive transaction's identity at begin. Keep
    // the trusted-serving subject here so every staged operation and its
    // commit are authorized as that one subject.
    const identity = this.trustedWriteIdentity(session);
    const attribution = this.backendAttribution(sessionJson);
    const admission = attribution ? undefined : identity;
    if (attribution) {
      if (kind !== "mergeable") {
        throw new Error("Backend-attributed transactions require mergeable kind");
      }
      requireBackendAttributionAbi(
        this.db.beginTransactionAttributed,
        "mergeable transactions",
      ).call(this.db, id, attribution);
    } else {
      this.db.beginTransaction(id, kind, admission);
    }
    this.pendingTxs.set(id, {
      id,
      kind,
      identity: admission,
      attribution,
      writes: [],
      txByView: new Map(),
    });
    return id;
  }

  commitTransaction(openTransactionId: OpenTransactionId): TxId {
    if (this !== this.ownerRuntime) return this.ownerRuntime.commitTransaction(openTransactionId);
    if (this.closed) {
      throw new Error("Commit transaction failed: native runtime is closed");
    }
    const pending = this.pendingTxs.get(openTransactionId);
    if (!pending) {
      throw new Error(commitTransactionMessage(openTransactionId, this.completedTxs));
    }
    if (pending.writes.length === 0 && pending.kind === "mergeable") {
      throw new Error(
        "Commit transaction failed: empty mergeable transaction has no committed unit; roll it back instead",
      );
    }
    const write = this.db.commitTransaction(openTransactionId, pending.kind);
    this.releaseTransactionViews(pending);
    this.pendingTxs.delete(openTransactionId);
    this.completedTxs.set(openTransactionId, { kind: pending.kind, state: "committed" });
    this.pumpSubscriptions();
    this.scheduleServerPump();
    this.notifyPeerTransportWork();
    const txId = recordWrite(write, this.writes);
    if (this.nonDurableClient) this.trackLocalSettlement(txId);
    return txId;
  }

  async waitForTransaction(txId: TxId | Promise<TxId>, tier: string): Promise<void> {
    if (this !== this.ownerRuntime) {
      return this.ownerRuntime.waitForTransaction(txId, tier);
    }
    txId = await txId;
    const write = this.writes.get(txId);
    if (!write) {
      throw new Error(`Wait for transaction failed: unknown transaction ${txId}`);
    }
    for (;;) {
      this.throwServerTransportErrorForTier(tier);
      const observedServerWorkEpoch = this.serverTransportWorkEpoch;
      void this.pumpServerTransport();
      this.throwServerTransportErrorForTier(tier);
      const settlement = write.wait(tier);
      const transportError = this.waitForServerTransportError(tier);
      const transportWork = this.waitForServerTransportWork(tier, observedServerWorkEpoch);
      try {
        const wakes: Array<Promise<"settled" | "work"> | Promise<never>> = [
          settlement.then(() => "settled" as const),
        ];
        if (transportError) wakes.push(transportError.promise);
        if (transportWork) wakes.push(transportWork.promise.then(() => "work" as const));
        if ((await Promise.race(wakes)) === "settled") {
          this.pumpSubscriptions();
          return;
        }
      } catch (error) {
        const rejected = rejectedWaitError(txId, error);
        if (rejected) {
          throw rejected;
        }
        throw error;
      } finally {
        transportError?.cancel();
        transportWork?.cancel();
      }
    }
  }

  rollbackTransaction(openTransactionId: OpenTransactionId): Promise<boolean> {
    if (this !== this.ownerRuntime) return this.ownerRuntime.rollbackTransaction(openTransactionId);
    const pending = this.pendingTxs.get(openTransactionId);
    if (!pending) {
      throw new Error(rollbackTransactionMessage(openTransactionId, this.completedTxs));
    }
    try {
      this.db.rollbackTransaction(openTransactionId);
    } finally {
      this.releaseTransactionViews(pending);
      this.pendingTxs.delete(openTransactionId);
    }
    this.completedTxs.set(openTransactionId, { kind: pending.kind, state: "rolled_back" });
    return Promise.resolve(true);
  }

  async query(
    queryJson: string,
    sessionJson?: string | null,
    tier?: string | null,
    optionsJson?: string | null,
  ): Promise<unknown> {
    assertSupportedReadOptions(tier, optionsJson);
    assertTransactionReadOpen(optionsJson, this.pendingTxs, this.completedTxs);
    const session = readSession(sessionJson);
    this.applySessionClaims(session);
    assertNoUnsupportedPermissionIntrospection(queryJson);
    const coreQueryJson = addNestedOuterColumns(queryJson);
    const pendingTx = pendingTxFromOptions(optionsJson, this.pendingTxs);
    // Relation-IR lowering still has a JSON-only binding API.  Array includes
    // below use the transaction-aware snapshot ABI instead, so they preserve
    // staged writes and do not fall back to the owner's ordinary view.
    if (pendingTx && queryUsesNativeRelationApi(coreQueryJson)) {
      throw new Error("Native runtime does not support relation reads inside a transaction");
    }
    // Browser runtimes still materialize row bodies from their in-memory
    // cache, but an Edge/Global read must keep its requested tier while doing
    // so. The settled membership from the worker is the authorization
    // boundary; lowering it to Local here would re-scan cached rows that a
    // fresh remote receipt had just removed.
    const opts = readOptions(tier, queryIncludesDeleted(coreQueryJson), optionsJson);
    const readContext = this.nativeReadContext(session, pendingTx);
    if (queryUsesNativeRelationApi(coreQueryJson)) {
      await this.waitForStrictRemoteQueryTransport(tier);
      if (this.closed) return [];
      const payload = await this.readRelationQueryForContext(coreQueryJson, opts, readContext);
      return rowsFromBatches(readRowBatches(payload), this.schema);
    }
    const query = this.prepareQuery(coreQueryJson);
    const attachment = await this.attachQueryIfNeeded(tier, optionsJson, query, session);
    if (this.closed) return [];
    this.attachLocalReadCoverageInBackground(tier, optionsJson, query, session);
    try {
      if (queryHasArraySubqueries(coreQueryJson)) {
        if (pendingTx) {
          const payload = await this.readRelationSnapshotInTransactionForContext(
            query,
            this.txForRead(pendingTx),
            opts,
            readContext,
          );
          return rowsFromRelationSnapshot(
            readRelationSnapshot(payload),
            this.schema,
            subscriptionOutputColumns(coreQueryJson, this.schema).rootColumns,
          );
        }
        const payload = await this.readRelationSnapshotForContext(query, opts, readContext);
        return rowsFromRelationSnapshot(
          readRelationSnapshot(payload),
          this.schema,
          subscriptionOutputColumns(coreQueryJson, this.schema).rootColumns,
        );
      }
      const projectedColumns = subscriptionOutputColumns(coreQueryJson, this.schema).rootColumns;
      let rows = await this.readPlainRows(query, opts, session ?? undefined, pendingTx);
      let rowStates = rowsFromBatches(readRowBatches(rows), this.schema, projectedColumns);
      if (!pendingTx && (tier === "edge" || tier === "global") && rowStates.length > 0) {
        await this.refreshRowsFromEdge(session, query, opts);
        if (this.closed) return [];
        rows = await this.readPlainRows(query, opts, session ?? undefined, pendingTx);
        rowStates = rowsFromBatches(readRowBatches(rows), this.schema, projectedColumns);
      }
      return rowStates;
    } finally {
      if (attachment !== undefined && !this.closed) this.db.detachQuery?.(attachment);
    }
  }

  createSubscription(
    queryJson: string,
    sessionJson?: string | null,
    tier?: string | null,
    optionsJson?: string | null,
  ): number {
    assertSupportedReadOptions(tier, optionsJson);
    if (queryIncludesDeleted(queryJson)) {
      throw new Error("Native runtime does not support include_deleted subscriptions yet");
    }
    const session = readSession(sessionJson);
    this.applySessionClaims(session);
    const readContext = this.nativeReadContext(session);
    assertNoUnsupportedPermissionIntrospection(queryJson);
    const usesNativeRelationApi = queryUsesNativeRelationApi(queryJson);
    const handle = this.nextSubscriptionId++;
    const opts = readOptions(tier, false, optionsJson);
    const identity = session?.identity;
    let nativeSubscription: ReadableStream<unknown> | Subscription;
    let preparedQuery: PreparedQuery | null = null;
    try {
      if (usesNativeRelationApi) {
        nativeSubscription = this.subscribeRelationForContext(queryJson, opts, readContext);
      } else {
        const query = this.prepareQuery(queryJson);
        preparedQuery = query;
        nativeSubscription = this.subscribeForContext(query, opts, readContext);
      }
    } catch (error) {
      const nativeStack = error instanceof Error ? error.stack : undefined;
      throw new Error(
        `Core subscribe failed for ${queryJson}: ${errorMessage(error)}${nativeStack ? `\n${nativeStack}` : ""}`,
      );
    }
    this.subscriptions.set(handle, {
      sources: [{ source: subscriptionSource(nativeSubscription), reading: false }],
      queryJson,
      query: preparedQuery,
      identity,
      rows: [],
      rowIndexByKey: new Map(),
      visibleRows: [],
      outputColumns: usesNativeRelationApi
        ? null
        : subscriptionOutputColumns(queryJson, this.schema),
      session,
      opts,
      opened: false,
      visibleOpened: false,
      deferredVisiblePublication: false,
      deferredVisibleReset: false,
      deferredTerminalOperations: [],
      deferredPlaceholderChunks: 0,
      deferredPlaceholderRows: 0,
      deferredPlaceholderBytes: 0,
      cancelled: false,
    });
    return handle;
  }

  executeSubscription(handle: number, onUpdate: (delta: RuntimeSubscriptionDelta) => void): void;
  executeSubscription(
    handle: number,
    onUpdate: (result: RuntimeSubscriptionDelta | Error) => void,
  ): void;
  executeSubscription(handle: number, onUpdate: Function): void {
    const subscription = this.subscriptions.get(handle);
    if (!subscription) return;
    subscription.callback = onUpdate as (result: RuntimeSubscriptionDelta | Error) => void;
    if (subscription.visibleOpened) {
      subscription.callback(
        runtimeResetDeltaFromRows(
          subscription.visibleRows,
          this.schema,
          subscription.outputColumns,
        ),
      );
    }
    this.startSubscriptionReader(handle, subscription);
  }

  unsubscribe(handle: number): void {
    const subscription = this.subscriptions.get(handle);
    if (!subscription) return;
    subscription.cancelled = true;
    clearDeferredPlaceholderBuffer(subscription);
    closeSubscriptionSourceState(subscription);
    this.subscriptions.delete(handle);
  }

  connect(url: string, authJson: string): void {
    if (this !== this.ownerRuntime) return this.ownerRuntime.connect(url, authJson);
    const normalizedAuthJson = normalizeBackendWebSocketAuth(authJson);
    // A new transport replaces the old one during a temporary reconnect. Server-tier
    // waits are still meaningful across that transition, so only an explicit runtime
    // shutdown is allowed to reject them.
    void this.disconnect({ rejectWaiters: false, preservePreHelloRetry: true });
    const generation = ++this.serverConnectionGeneration;
    const transportIdentity = peerIdentityForWebSocketAuth(normalizedAuthJson, this.peerIdentity);
    this.serverTransportError = null;
    this.serverEndpointUrl = url;
    this.serverAuthJson = normalizedAuthJson;
    let resolveTerminal!: (error: Error) => void;
    const terminal = new Promise<Error>((resolve) => {
      resolveTerminal = resolve;
    });
    let attempt: ServerConnectionAttempt | null = null;
    const carrier = new WebSocketCarrier({
      endpointUrl: url,
      peerIdentity: transportIdentity,
      features: this.nativeWireFeatures(),
      authJson: normalizedAuthJson,
      requestedLink: this.scopeIsolatedRelay ? "scope_isolated_client_relay" : undefined,
      onFrame: (frame) => {
        if (generation !== this.serverConnectionGeneration) return;
        this.pendingInboundServerFrames.push(frame);
        this.notifyServerTransportWork();
        this.scheduleServerPump();
        // A normal idle connection lets the debounced pump coalesce frames.
        // If the pump is already suspended in a core tick waiting for a chunk,
        // route the frame independently so the response can wake that tick.
        if (this.serverPumpRunning) {
          queueMicrotask(() => {
            if (!this.serverPumpRunning || this.pendingInboundServerFrames.length === 0) return;
            void this.routePendingInboundServerFrames().catch((error) =>
              this.handleServerTransportError(error, generation),
            );
          });
        }
      },
      onError: (error) => {
        if (error.code === "not_ready" && error.retry === "later") return;
        this.handleServerTransportError(error, generation);
        const reason = wireAuthFailureReason(error);
        if (reason) this.authFailureCallback?.(reason);
      },
      onTerminal: (error) => {
        if (!attempt) return;
        if (error.code === "not_ready" && error.retry === "later") return;
        this.finishServerConnectionAttempt(attempt, new Error(error.message));
      },
    });
    attempt = {
      generation,
      carrier,
      terminal,
      resolveTerminal,
      finished: false,
      outcome: null,
      transport: null,
      retirement: null,
    };
    this.serverConnectionAttempt = attempt;
    this.serverCarrier = carrier;
    this.serverCarrierPromise = carrier
      .ready()
      .then(async (negotiation) => {
        if (
          generation !== this.serverConnectionGeneration ||
          carrier !== this.serverCarrier ||
          attempt !== this.serverConnectionAttempt
        ) {
          carrier.close();
          return carrier;
        }
        this.preHelloRetryCount = 0;
        const admission = this.connectNegotiatedUpstream(negotiation).catch((error) => {
          throw contextualError("connecting the negotiated upstream transport", error);
        });
        const outcome = await Promise.race([
          admission.then((transport) => ({ type: "admitted" as const, transport })),
          attempt.terminal.then((error) => ({ type: "terminal" as const, error })),
        ]);
        if (outcome.type === "terminal") {
          void admission.then(
            (transport) => this.retirePeerTransport(transport).catch(reportAsyncRuntimeError),
            () => undefined,
          );
          throw outcome.error;
        }
        const transport = outcome.transport;
        if (
          this.closed ||
          generation !== this.serverConnectionGeneration ||
          carrier !== this.serverCarrier ||
          attempt !== this.serverConnectionAttempt
        ) {
          carrier.close();
          await this.retirePeerTransport(transport);
          return carrier;
        }
        attempt.transport = transport;
        this.serverTransport = transport;
        transport.setAuxiliaryTraceEnabled?.(this.auxiliaryTraceListeners.size > 0);
        void this.watchAuxiliaryOutbound(transport, carrier, generation);
        this.flushQueuedServerFrames(carrier);
        await this.pumpServerTransport();
        this.pumpSubscriptions();
        return carrier;
      })
      .catch((error) => {
        if (isRetryablePreHelloWireError(error)) {
          const retry = this.retryPreHelloConnection(attempt);
          if (retry) return retry;
        }
        const failure = error instanceof Error ? error : new Error(errorMessage(error));
        this.finishServerConnectionAttempt(attempt, failure);
        throw attempt.outcome ?? failure;
      });
    this.serverCarrierPromise.catch((error) => {
      if (isRetryablePreHelloWireError(error)) return;
      this.handleServerTransportError(error, generation);
    });
  }

  private async connectNegotiatedUpstream(negotiation: WebSocketNegotiation): Promise<Transport> {
    const authority = negotiation.authority;
    const connectWithSession = this.db.connectUpstreamWithSession;
    if (!authority || !connectWithSession) return this.db.connectUpstream();
    const localEpoch = this.nextServerConnectionEpoch++;
    return await connectWithSession.call(
      this.db,
      negotiation.protocolVersion,
      negotiation.features,
      authority.node,
      authority.epoch,
      this.node,
      localEpoch,
    );
  }

  private nativeWireFeatures(): number {
    if (!this.db.wireFeatures) {
      throw new Error(
        "native runtime binding does not expose its wire feature mask; install the matching Jazz native runtime package",
      );
    }
    const features = this.db.wireFeatures();
    if (!Number.isSafeInteger(features) || features < 0 || features > 0xffff_ffff) {
      throw new Error(`native binding returned invalid wire feature mask ${features}`);
    }
    return features;
  }

  async disconnect(
    options: { rejectWaiters?: boolean; preservePreHelloRetry?: boolean } = {},
  ): Promise<void> {
    if (this !== this.ownerRuntime) return this.ownerRuntime.disconnect(options);
    this.serverConnectionGeneration += 1;
    this.clearServerReconnectTimer();
    if (!options.preservePreHelloRetry) this.preHelloRetryCount = 0;
    const attempt = this.serverConnectionAttempt;
    this.serverConnectionAttempt = null;
    if (attempt) {
      this.finishServerConnectionAttempt(attempt, new Error("server transport disconnected"));
    }
    this.serverCarrier?.close();
    this.serverCarrier = null;
    this.serverCarrierPromise = null;
    this.serverTransportError = null;
    if (options.rejectWaiters ?? true) {
      this.resolveServerTransportErrorWaiters(new Error("server transport disconnected"));
    } else {
      this.clearServerTransportErrorWaiters();
    }
    this.resolveServerTransportWorkWaiters();
    const transport = this.serverTransport;
    this.serverTransport = null;
    this.serverEndpointUrl = null;
    this.serverAuthJson = null;
    this.queuedServerFrames.length = 0;
    this.pendingInboundServerFrames.length = 0;
    this.serverPumpScheduled = false;
    this.serverPumpAgain = false;
    // Db::tick borrows peer connections across its async evaluator pass.
    // Detaching the transport while that borrow is live panics on WASM's
    // single-threaded RefCell mutex. Remove it from new work immediately, then
    // perform the physical detach once the owning tick has released the borrow.
    if (transport) await this.retirePeerTransport(transport);
    if (attempt?.retirement) await attempt.retirement;
  }

  updateAuth(authJson: string): Promise<void> | void {
    if (this !== this.ownerRuntime) return this.ownerRuntime.updateAuth(authJson);
    if (!this.serverEndpointUrl) return;
    return this.connect(this.serverEndpointUrl, authJson);
  }

  onAuthFailure(callback: (reason: string) => void): void {
    if (this !== this.ownerRuntime) return this.ownerRuntime.onAuthFailure(callback);
    this.authFailureCallback = callback;
  }

  onMutationError(callback: (event: MutationErrorEvent) => void): void {
    if (this !== this.ownerRuntime) return this.ownerRuntime.onMutationError(callback);
    this.mutationErrorCallback = callback;
    this.db.onMutationError((event) => this.deliverMutationError(event));
  }

  /**
   * Observe a terminal upstream transport/protocol failure. This has no fate
   * semantics: it only wakes remote waits and subscriptions that are active
   * in this runtime.
   */
  onServerTransportError(callback: (error: Error) => void): void {
    if (this !== this.ownerRuntime) return this.ownerRuntime.onServerTransportError(callback);
    this.serverTransportErrorCallback = callback;
  }

  /** Record a terminal error relayed from a durable browser-worker upstream. */
  reportRemoteServerTransportError(error: Error): void {
    if (this !== this.ownerRuntime)
      return this.ownerRuntime.reportRemoteServerTransportError(error);
    this.handleServerTransportError(error);
  }

  /** Clear a terminal remote-peer error after its replacement transport is ready. */
  clearRemoteServerTransportError(): void {
    if (this !== this.ownerRuntime) return this.ownerRuntime.clearRemoteServerTransportError();
    this.serverTransportError = null;
    this.clearServerTransportErrorWaiters();
  }

  reportRemoteMutationError(event: MutationErrorEvent): void {
    if (this !== this.ownerRuntime) return this.ownerRuntime.reportRemoteMutationError(event);
    this.deliverMutationError(event);
  }

  async flushLocalSettlements(): Promise<void> {
    if (this !== this.ownerRuntime) return this.ownerRuntime.flushLocalSettlements();
    if (this.pendingLocalSettlements.size > 0) {
      await Promise.all(this.pendingLocalSettlements);
    }
  }

  private deliverMutationError(event: MutationErrorEvent): void {
    const transactionId = event.transaction.transactionId;
    if (this.deliveredMutationErrors.has(transactionId)) return;
    this.deliveredMutationErrors.add(transactionId);
    this.mutationErrorCallback?.(event);
  }

  private trackLocalSettlement(txId: TxId): void {
    if (this !== this.ownerRuntime) return this.ownerRuntime.trackLocalSettlement(txId);
    let settlement!: Promise<void>;
    // The follower itself is non-durable. Its `local` receipt is the durable
    // worker's acknowledgement, emitted after inbound persistence but before
    // separately scheduled cold downstream view assembly.
    settlement = this.waitForTransaction(txId, "local")
      .catch(() => undefined)
      .finally(() => this.pendingLocalSettlements.delete(settlement));
    this.pendingLocalSettlements.add(settlement);
  }

  private finishInsert(
    table: string,
    rowId: Uint8Array,
    values: InsertValues,
    write: Write,
  ): InsertResult {
    const txId = recordWrite(write, this.writes);
    if (this.nonDurableClient) this.trackLocalSettlement(txId);
    this.pumpSubscriptions();
    this.scheduleServerPump();
    this.notifyPeerTransportWork();
    const row = this.rowStateFromValues(table, rowId, values);
    return { id: row.id, values: row.values, kind: "committed", txId };
  }

  private finishMutation(write: Write): MutationResult {
    const txId = recordWrite(write, this.writes);
    if (this.nonDurableClient) this.trackLocalSettlement(txId);
    this.pumpSubscriptions();
    this.scheduleServerPump();
    this.notifyPeerTransportWork();
    return { kind: "committed", txId };
  }

  /** Refuse every mutation once a foreground lease begins clean handoff. */
  private assertMutationAdmission(operation: string): void {
    if (this.ownerRuntime.closed) {
      throw new Error(`${operation} failed: native runtime is closed`);
    }
  }

  private resultForRow(
    table: string,
    rowId: Uint8Array,
    receipt:
      | { kind: "committed"; txId: TxId }
      | { kind: "staged"; openTransactionId: OpenTransactionId },
    identity?: Uint8Array,
  ): InsertResult {
    const row = this.readRow(table, rowId, identity);
    return { id: formatUuid(rowId), values: row?.values ?? [], ...receipt };
  }

  private readRow(table: string, rowId: Uint8Array, identity?: Uint8Array): RowState | undefined {
    if (!identity) return this.readRowForWriteMerge(table, rowId);
    const query = this.prepareQuery(JSON.stringify({ table }));
    const rows = this.readRowsForContext(
      query,
      readOptions(),
      this.nativeReadContext({ identity } as RuntimeSession),
    );
    return rowsFromBatches(readRowBatches(rows), this.schema).find(
      (row) => row.table === table && row.id === formatUuid(rowId),
    );
  }

  private readRowForWriteMerge(table: string, rowId: Uint8Array): RowState | undefined {
    const exactReader = (
      this.db as { localCurrentRow?: (table: string, rowId: Uint8Array) => Uint8Array }
    ).localCurrentRow;
    if (exactReader) {
      const rows = rowsFromBatches(
        readRowBatches(exactReader.call(this.db, table, rowId)),
        this.schema,
      );
      return rows[0];
    }
    const query = this.prepareQuery(JSON.stringify({ table }));
    const rows = this.db.all(query, readOptions());
    if (isPendingNativeRead(rows)) {
      throw new Error(
        "write merge cannot synchronously hydrate a large value; use the exact local row reader",
      );
    }
    return rowsFromBatches(readRowBatches(rows), this.schema).find(
      (row) => row.table === table && row.id === formatUuid(rowId),
    );
  }

  private rowStateFromValues(
    table: string,
    rowId: Uint8Array,
    values: Record<string, Value>,
  ): RowState {
    const visibleColumns = this.table(table).columns.filter(
      (column) => !isHiddenIncludeColumn(column.name),
    );
    const valuesByColumn = new Map<string, Value>();
    for (const column of visibleColumns) {
      valuesByColumn.set(column.name, values[column.name] ?? column.default ?? { type: "Null" });
    }
    return withValuesByColumn(
      {
        table,
        id: formatUuid(rowId),
        values: visibleColumns
          .filter((column) => !isProvenanceMagicColumn(column.name))
          .map((column) => valuesByColumn.get(column.name)!),
      },
      valuesByColumn,
    );
  }

  private mergeRowState(
    table: string,
    rowId: Uint8Array,
    patch: Record<string, Value>,
    tx: PendingTx,
    _identity?: Uint8Array,
  ): RowState {
    const current =
      this.stagedRowForWriteMerge(tx, table, rowId) ?? this.readRowForWriteMerge(table, rowId);
    const merged: Record<string, Value> = {};
    for (const column of this.table(table).columns) {
      const existing = current?.valuesByColumn?.get(column.name);
      if (existing !== undefined) merged[column.name] = existing;
    }
    Object.assign(merged, patch);
    return this.rowStateFromValues(table, rowId, merged);
  }

  private async readPlainRows(
    query: PreparedQuery,
    opts: unknown,
    session: RuntimeSession | undefined,
    pendingTx: PendingTx | undefined,
  ): Promise<Uint8Array> {
    const context = this.nativeReadContext(session, pendingTx);
    if (!pendingTx) return this.readRowsForContextAsync(query, opts, context);
    const tx = this.txForRead(pendingTx);
    switch (context.kind) {
      case "backend-authority":
        if (!this.db.allInTransactionForBackend) {
          throw new Error("Native runtime does not support backend authority transaction reads");
        }
        return this.awaitNativeRead(this.db.allInTransactionForBackend(query, tx, opts));
      case "session-authority":
        if (!this.db.allInTransactionForIdentity) {
          throw new Error("Native runtime does not support session-authority transaction reads");
        }
        return this.awaitNativeRead(
          this.db.allInTransactionForIdentity(query, tx, context.identity, opts),
        );
      case "client-local":
        if (!this.db.allInTransaction) {
          throw new Error("Native runtime does not support transaction reads");
        }
        return this.awaitNativeRead(this.db.allInTransaction(query, tx, opts));
    }
  }

  /**
   * Client runtimes consume server-scoped settled data locally. Only an
   * explicitly configured serving host may select the policy-enforcing entry
   * point, with a request session supplying its subject when present.
   */
  private readRowsForContext(
    query: PreparedQuery,
    opts: unknown,
    context: NativeReadContext,
  ): Uint8Array {
    const result = this.startRowsForContext(query, opts, context);
    if (typeof (result as Promise<unknown>).then === "function") {
      throw new Error("native read is asynchronous; use the asynchronous read boundary");
    }
    if (isPendingNativeRead(result)) {
      throw new Error("large-value hydration is pending; use the asynchronous read boundary");
    }
    return result as Uint8Array;
  }

  private async readRowsForContextAsync(
    query: PreparedQuery,
    opts: unknown,
    context: NativeReadContext,
  ): Promise<Uint8Array> {
    return this.awaitNativeRead(this.startRowsForContext(query, opts, context));
  }

  private startRowsForContext(
    query: PreparedQuery,
    opts: unknown,
    context: NativeReadContext,
  ): NativeReadResult | Promise<NativeReadResult> {
    switch (context.kind) {
      case "backend-authority":
        return (
          this.db.allForBackend?.(query, opts) ??
          (() => {
            throw new Error("Native runtime does not support backend authority reads");
          })()
        );
      case "session-authority":
        return (
          this.db.allForIdentityAsync?.(query, context.identity, opts) ??
          this.db.allForIdentity(query, context.identity, opts)
        );
      case "client-local":
        return this.db.allAsync?.(query, opts) ?? this.db.all(query, opts);
    }
  }

  /**
   * Select the one native read context once, before lowering any surface.
   * `SYSTEM_READ_SESSION` only becomes backend authority after the adapter has
   * opened a trusted backend; its marker is never serialized to Rust. A
   * transaction keeps its begin-bound public session when the caller omits a
   * session on a later read.
   */
  private nativeReadContext(
    session: RuntimeSession | null | undefined,
    pendingTx?: PendingTx,
  ): NativeReadContext {
    if (this.trustedBackend && (session?.backendAuthority || (!session && !pendingTx?.identity))) {
      return { kind: "backend-authority" };
    }
    if (this.readAuthorizationHost === "trusted-serving") {
      return {
        kind: "session-authority",
        identity: pendingTx?.identity ?? session?.identity ?? this.peerIdentity,
      };
    }
    return { kind: "client-local" };
  }

  private readRelationQueryForContext(
    queryJson: string,
    opts: unknown,
    context: NativeReadContext,
  ): Promise<Uint8Array> {
    switch (context.kind) {
      case "backend-authority":
        if (!this.db.allRelationQueryForBackend) {
          throw new Error("Native runtime does not support backend authority relation queries");
        }
        return this.awaitNativeRead(this.db.allRelationQueryForBackend(queryJson, opts));
      case "session-authority":
        if (!this.db.allRelationQueryForIdentity) {
          throw new Error("Native runtime does not support session-authority relation queries");
        }
        return this.awaitNativeRead(
          this.db.allRelationQueryForIdentity(queryJson, context.identity, opts),
        );
      case "client-local":
        if (!this.db.allRelationQuery) {
          throw new Error("Native runtime does not support relation queries");
        }
        return this.awaitNativeRead(this.db.allRelationQuery(queryJson, opts));
    }
  }

  private readRelationSnapshotForContext(
    query: PreparedQuery,
    opts: unknown,
    context: NativeReadContext,
  ): Promise<Uint8Array> {
    switch (context.kind) {
      case "backend-authority":
        if (!this.db.allRelationSnapshotForBackend) {
          throw new Error("Native runtime does not support backend authority relation snapshots");
        }
        return this.awaitNativeRead(this.db.allRelationSnapshotForBackend(query, opts));
      case "session-authority":
        if (!this.db.allRelationSnapshotForIdentity) {
          throw new Error("Native runtime does not support session-authority relation snapshots");
        }
        return this.awaitNativeRead(
          this.db.allRelationSnapshotForIdentity(query, context.identity, opts),
        );
      case "client-local":
        if (!this.db.allRelationSnapshot) {
          throw new Error("Native runtime does not support relation snapshots");
        }
        return this.awaitNativeRead(this.db.allRelationSnapshot(query, opts));
    }
  }

  private readRelationSnapshotInTransactionForContext(
    query: PreparedQuery,
    tx: Tx,
    opts: unknown,
    context: NativeReadContext,
  ): Promise<Uint8Array> {
    switch (context.kind) {
      case "backend-authority":
        if (!this.db.allRelationSnapshotInTransactionForBackend) {
          throw new Error(
            "Native runtime does not support backend authority transaction relation reads",
          );
        }
        return this.awaitNativeRead(
          this.db.allRelationSnapshotInTransactionForBackend(query, tx, opts),
        );
      case "session-authority":
        if (!this.db.allRelationSnapshotInTransactionForIdentity) {
          throw new Error(
            "Native runtime does not support session-authority transaction relation reads",
          );
        }
        return this.awaitNativeRead(
          this.db.allRelationSnapshotInTransactionForIdentity(query, tx, context.identity, opts),
        );
      case "client-local":
        if (!this.db.allRelationSnapshotInTransaction) {
          throw new Error("Native runtime does not support transaction relation reads");
        }
        return this.awaitNativeRead(this.db.allRelationSnapshotInTransaction(query, tx, opts));
    }
  }

  private subscribeForContext(
    query: PreparedQuery,
    opts: unknown,
    context: NativeReadContext,
  ): ReadableStream<unknown> | Subscription {
    switch (context.kind) {
      case "backend-authority":
        if (!this.db.subscribeForBackend) {
          throw new Error("Native runtime does not support backend authority subscriptions");
        }
        return this.db.subscribeForBackend(query, opts);
      case "session-authority":
        if (!this.db.subscribeForIdentity) {
          throw new Error("Native runtime does not support session-authority subscriptions");
        }
        return this.db.subscribeForIdentity(query, context.identity, opts);
      case "client-local":
        if (!this.db.subscribe) throw new Error("Native runtime does not support subscriptions");
        return this.db.subscribe(query, opts);
    }
  }

  private subscribeRelationForContext(
    queryJson: string,
    opts: unknown,
    context: NativeReadContext,
  ): ReadableStream<unknown> | Subscription {
    switch (context.kind) {
      case "backend-authority":
        if (!this.db.subscribeRelationQueryForBackend) {
          throw new Error(
            "Native runtime does not support backend authority relation subscriptions",
          );
        }
        return this.db.subscribeRelationQueryForBackend(queryJson, opts);
      case "session-authority":
        if (!this.db.subscribeRelationQueryForIdentity) {
          throw new Error(
            "Native runtime does not support session-authority relation subscriptions",
          );
        }
        return this.db.subscribeRelationQueryForIdentity(queryJson, context.identity, opts);
      case "client-local":
        if (!this.db.subscribeRelationQuery) {
          throw new Error("Native runtime does not support relation query subscriptions");
        }
        return this.db.subscribeRelationQuery(queryJson, opts);
    }
  }

  private attachQueryForContext(
    query: PreparedQuery,
    opts: unknown,
    context: NativeReadContext,
  ): unknown {
    switch (context.kind) {
      case "backend-authority":
        if (!this.db.attachQueryForBackend) {
          throw new Error("Native runtime does not support backend authority query coverage");
        }
        return this.db.attachQueryForBackend(query, opts);
      case "session-authority":
        if (!this.db.attachQueryForIdentity) {
          throw new Error("Native runtime does not support session-authority query coverage");
        }
        return this.db.attachQueryForIdentity(query, context.identity, opts);
      case "client-local":
        if (!this.db.attachQuery) {
          throw new Error("Native runtime does not support query coverage");
        }
        return this.db.attachQuery(query, opts);
    }
  }

  /**
   * Native NAPI reads may suspend on a routed large-value chunk.  Keep the
   * thread-affine Rust future in the binding and let the existing peer pump
   * deliver the missing chunk between polls; never block the JS event loop.
   */
  private async awaitNativeRead(
    started: NativeReadResult | Promise<NativeReadResult>,
  ): Promise<Uint8Array> {
    const result = await started;
    if (!isPendingNativeRead(result)) return result;
    for (;;) {
      if (this.closed) throw new Error("large-value hydration was cancelled by runtime shutdown");
      const bytes = result.poll();
      if (bytes !== null) return bytes;
      await this.pumpServerTransport();
      await sleep(0);
    }
  }

  private async awaitNativeWrite(pending: PendingNativeWrite): Promise<Write> {
    for (;;) {
      if (this.closed) throw new Error("large-value mutation was cancelled by runtime shutdown");
      const write = pending.poll();
      if (write !== null) return write;
      await this.pumpServerTransport();
      await sleep(0);
    }
  }

  /**
   * A session supplies the subject for a trusted-serving host; it never turns
   * an ordinary client mutation into a policy-enforcing local admission.
   */
  private trustedWriteIdentity(session: RuntimeSession | null | undefined): Uint8Array | undefined {
    if (this.trustedBackend && !session) return undefined;
    return this.readAuthorizationHost === "trusted-serving"
      ? (session?.identity ?? this.peerIdentity)
      : undefined;
  }

  /** External provenance is accepted only by the explicit backend runtime;
   * ordinary/session callers continue through their normal identity path. */
  private backendAttribution(writeContext?: string | null): Uint8Array | undefined {
    if (!this.trustedBackend || !writeContext) return undefined;
    let attribution: unknown;
    try {
      attribution = (JSON.parse(writeContext) as { attribution?: unknown }).attribution;
    } catch {
      return undefined;
    }
    if (attribution === undefined) return undefined;
    if (typeof attribution !== "string") {
      throw new Error("backend attribution must be a canonical author subject string");
    }
    const author = parsePublicCanonicalAuthor(attribution);
    if (!author) {
      throw new Error("backend attribution must be a canonical author subject string");
    }
    return authorBytesForSession({ issuer: author.issuer, user_id: author.user_id });
  }

  private stagedRowForWriteMerge(
    tx: PendingTx,
    table: string,
    rowId: Uint8Array,
  ): RowState | undefined {
    const id = formatUuid(rowId);
    for (let index = tx.writes.length - 1; index >= 0; index -= 1) {
      const write = tx.writes[index]!;
      if (write.table !== table || formatUuid(write.rowId) !== id) continue;
      return write.deleted ? undefined : write.row;
    }
    return undefined;
  }

  private warnedOnce = new Set<string>();
  private warnOnce(key: string, message: string): void {
    if (this.warnedOnce.has(key)) return;
    this.warnedOnce.add(key);
    console.warn(`[jazz native-runtime] ${message}`);
  }

  private async refreshRowsFromEdge(
    session: RuntimeSession | null,
    query: PreparedQuery,
    opts: unknown,
  ): Promise<void> {
    if (!this.hasUpstream()) return;
    // The outer Edge attachment owns both membership and concrete
    // occurrences. Reusing that prepared query materializes only its
    // delivered versions; a nested exact-id request creates a second scope
    // and can circularly await its own authority receipt.
    await this.readRowsForContextAsync(query, opts, this.nativeReadContext(session));
  }

  private prepareQuery(queryJson: string): PreparedQuery {
    const queryBytes = encodeQueryJson(queryJson, this.schema);
    const key = bytesKey(queryBytes);
    let query = this.preparedQueries.get(key);
    if (!query) {
      try {
        query = this.db.prepareQuery(queryBytes);
      } catch (error) {
        throw new Error(`Core prepareQuery failed for ${queryJson}: ${errorMessage(error)}`);
      }
      this.preparedQueries.set(key, query);
    }
    return query;
  }

  private async attachQueryIfNeeded(
    tier: string | null | undefined,
    optionsJson: string | null | undefined,
    query: PreparedQuery,
    session: RuntimeSession | null,
  ): Promise<unknown | undefined> {
    if (this.closed) return;
    if (tier == null || (tier === "local" && !this.nonDurableClient)) return;
    if (!readPropagationIsFull(optionsJson) && !this.nonDurableClient) return;
    await this.waitForStrictRemoteQueryTransport(tier);
    if (!this.db.attachQuery) return;
    // Coverage registration and probes are synchronous node operations. A
    // storage-backed evaluator may hold that node across suspension, so enter
    // the same owner-wide idle boundary used for peer admission first.
    await this.waitForCoreIdle();
    if (this.closed) return;
    const opts = readOptions(tier, false, optionsJson);
    const readContext = this.nativeReadContext(session);
    const attachment = this.attachQueryForContext(query, opts, readContext);
    this.emitQueryCoverageTrace("attach");
    // Local+Full has two independent axes: registering the attachment starts
    // normal upstream propagation, but the public Local read is complete from
    // current local knowledge and must not wait for that remote coverage.
    if (tier === "local") return attachment;
    if (!this.db.queryAttachmentIsCovered) return attachment;
    const coverageKey = this.coverageKey(readContext, session);
    const confirmedPeerActivityEpoch = this.peerCoveredQueries.get(query)?.get(coverageKey);
    const mayReusePeerConfirmation = this.nonDurableClient && !readPropagationIsFull(optionsJson);
    const requiresFreshPeerConfirmation =
      this.nonDurableClient &&
      readPropagationIsFull(optionsJson) &&
      confirmedPeerActivityEpoch != null;
    // A prior confirmation can recover a reattachment only if no newer worker
    // frame has arrived. Otherwise the old coverage state could be exposed to
    // a query whose authorization (for example, an authorship-scoped policy)
    // is about to change.
    if (
      mayReusePeerConfirmation &&
      confirmedPeerActivityEpoch != null &&
      this.peerTransportActivityEpoch <= confirmedPeerActivityEpoch &&
      this.db.queryAttachmentIsCovered(attachment)
    ) {
      return attachment;
    }
    const minimumPeerActivityEpoch = this.nonDurableClient
      ? this.peerTransportActivityEpoch
      : undefined;
    const pendingPeerActivityEpoch =
      this.nonDurableClient &&
      !requiresFreshPeerConfirmation &&
      this.peerTransportActivityEpoch > this.peerTransportProcessedActivityEpoch
        ? this.peerTransportActivityEpoch
        : undefined;
    await this.waitForQueryCoverage(
      attachment,
      query,
      readOptions(tier, false, optionsJson),
      readContext,
      minimumPeerActivityEpoch,
      pendingPeerActivityEpoch,
      mayReusePeerConfirmation && confirmedPeerActivityEpoch != null,
    );
    if (this.nonDurableClient && this.db.queryAttachmentIsCovered(attachment)) {
      const confirmations = this.peerCoveredQueries.get(query) ?? new Map<string, number>();
      confirmations.set(coverageKey, this.peerTransportProcessedActivityEpoch);
      this.peerCoveredQueries.set(query, confirmations);
    }
    this.emitQueryCoverageTrace("covered");
    return attachment;
  }

  /**
   * A strict remote query cannot materialize its local snapshot before an
   * in-flight server handshake has either admitted its authority transport or
   * failed. Relation-IR reads bypass query attachment, so they share this
   * gate with attached reads instead of acquiring a second coverage path.
   */
  private async waitForStrictRemoteQueryTransport(tier: string | null | undefined): Promise<void> {
    if (tier !== "edge" && tier !== "global") return;
    // `connect()` starts its WebSocket handshake before it can admit the
    // native transport. A strict remote read begun in that interval must
    // await the in-flight connection instead of falling through to a local
    // materialization merely because the transport has not been installed yet.
    while (!this.hasUpstream()) {
      const pendingConnection = this.serverCarrierPromise;
      if (!pendingConnection) return;
      const attempt = this.serverConnectionAttempt;
      const terminal =
        attempt?.carrier === this.serverCarrier
          ? await Promise.race([pendingConnection.then(() => null), attempt.terminal])
          : null;
      if (this.closed) return;
      // Reauthentication/reconnect can retire a stalled carrier while this
      // query is waiting. Follow the replacement attempt; only surface a
      // terminal error when this was still the current connection.
      if (terminal) {
        if (
          this.serverCarrierPromise !== null &&
          this.serverCarrierPromise !== pendingConnection &&
          this.serverConnectionAttempt?.carrier === this.serverCarrier
        ) {
          continue;
        }
        throw terminal;
      }
    }
  }

  private attachLocalReadCoverageInBackground(
    tier: string | null | undefined,
    optionsJson: string | null | undefined,
    query: PreparedQuery,
    session: RuntimeSession | null,
  ): void {
    if (tier != null && tier !== "local") return;
    if (!readPropagationIsFull(optionsJson)) return;
    if (this.nonDurableClient || !this.serverTransport || !this.db.attachQuery) return;

    const refresh = async () => {
      await this.serverCarrierPromise;
      if (this.closed) return;
      const edgeOptionsJson = JSON.stringify({ propagation: "full" });
      const attachment = await this.attachQueryIfNeeded("edge", edgeOptionsJson, query, session);
      if (attachment !== undefined && !this.closed) this.db.detachQuery?.(attachment);
    };

    void refresh().catch((error: unknown) => {
      if (this.closed) return;
      if (error instanceof Error && error.message === "Timed out waiting for edge query coverage") {
        return;
      }
      this.handleServerTransportError(error);
    });
  }

  /** Coverage is partitioned by the same native read context that owns it. */
  private coverageKey(context: NativeReadContext, session: RuntimeSession | null): string {
    switch (context.kind) {
      case "client-local":
        return "client-local";
      case "backend-authority":
        return "backend-authority";
      case "session-authority":
        return JSON.stringify([bytesKey(context.identity), canonicalJson(session?.claims ?? {})]);
    }
  }

  private applySessionClaims(session: RuntimeSession | null | undefined): void {
    // Client runtimes only evaluate their already-settled local replica. They
    // never select the policy-enforcing native entry points, so there is no
    // reason to serialize a session subject into the public native ABI here.
    // In particular, local-first and anonymous subjects are admitted by the
    // first-party TypeScript flow, while raw native identity ingress must keep
    // rejecting their reserved issuers. Claims are required only by the
    // explicitly selected trusted-serving host, where every identity call is
    // part of that serving boundary.
    if (
      !session ||
      session.backendAuthority ||
      this.readAuthorizationHost !== "trusted-serving" ||
      !this.db.setIdentityClaims
    ) {
      return;
    }
    this.db.setIdentityClaims(session.identity, session.claims);
  }

  private async waitForQueryCoverage(
    attachment: unknown,
    query: PreparedQuery,
    opts: unknown,
    context: NativeReadContext,
    minimumPeerActivityEpoch?: number,
    pendingPeerActivityEpoch?: number,
    exactContextWasConfirmed = false,
  ): Promise<void> {
    const deadline = Date.now() + 15_000;
    const tier = (opts as { tier?: string }).tier ?? "";
    while (Date.now() < deadline) {
      // A query can still be waiting for an upstream coverage response while
      // its owning browser runtime is being torn down. `close()` frees the
      // WASM Db, so this background wait must not touch its attachment after
      // that boundary.
      if (this.closed) return;
      this.throwServerTransportErrorForTier(tier);
      await this.pumpServerTransport();
      this.throwServerTransportErrorForTier(tier);
      await this.waitForCoreIdle();
      if (this.closed) return;
      if (this.db.queryAttachmentIsCovered) {
        const peerActivityWasProcessed =
          minimumPeerActivityEpoch == null ||
          this.peerTransportProcessedActivityEpoch > minimumPeerActivityEpoch ||
          (exactContextWasConfirmed &&
            minimumPeerActivityEpoch > 0 &&
            this.peerTransportProcessedActivityEpoch >= minimumPeerActivityEpoch) ||
          (pendingPeerActivityEpoch != null &&
            this.peerTransportProcessedActivityEpoch >= pendingPeerActivityEpoch);
        if (peerActivityWasProcessed && this.db.queryAttachmentIsCovered(attachment)) return;
      }
      try {
        await this.readRowsForContextAsync(query, opts, context);
        if (!this.db.queryAttachmentIsCovered) return;
      } catch (error) {
        if (!isPendingCoverageError(error)) throw error;
      }
      const transportError = this.waitForServerTransportError(tier);
      try {
        await (transportError ? Promise.race([sleep(10), transportError.promise]) : sleep(10));
      } finally {
        transportError?.cancel();
      }
    }
    this.scheduleServerPump();
    throw new Error("Timed out waiting for edge query coverage");
  }

  private table(table: string): { columns: ColumnDescriptor[]; policies?: TablePolicies } {
    const definition = this.schema[table];
    if (!definition) throw new Error(`unknown table ${table}`);
    return definition;
  }

  private currentTx(
    writeContext: string | null | undefined,
    operation: "Insert" | "Restore" | "Update" | "Upsert" | "Delete",
  ): PendingTx | undefined {
    const id = txIdFromContext(writeContext);
    if (!id) return undefined;
    const pending = this.pendingTxs.get(id);
    if (pending) return pending;
    throw new Error(`${operation} failed: WriteError("${txStateMessage(id, this.completedTxs)}")`);
  }

  private txForWrite(pending: PendingTx, identity: Uint8Array | undefined): Tx {
    if (pending.kind === "exclusive") {
      if (pending.identity && (!identity || !sameBytes(pending.identity, identity))) {
        throw new Error("Native runtime exclusive transaction cannot mix write identities");
      }
      return this.txForRead(pending);
    }
    if (pending.identity && (!identity || !sameBytes(pending.identity, identity))) {
      throw new Error("Native runtime mergeable transaction cannot mix write identities");
    }
    if (identity && !pending.identity) {
      throw new Error("Native runtime mergeable transaction cannot mix write identities");
    }
    return this.txForRead(pending);
  }

  private assertTransactionAttribution(
    pending: PendingTx,
    attribution: Uint8Array | undefined,
  ): void {
    if (!pending.attribution && !attribution) return;
    if (!pending.attribution) {
      throw new Error("Native runtime transaction was opened without backend attribution");
    }
    if (!attribution || !sameBytes(pending.attribution, attribution)) {
      throw new Error(
        "Native runtime backend-attributed transaction requires its original provenance subject",
      );
    }
  }

  private txForRead(pending: PendingTx): Tx {
    const existing = pending.txByView.get(this);
    if (existing) return existing;
    const tx =
      pending.kind === "mergeable"
        ? this.db.attachMergeableTx(pending.id)
        : this.db.attachExclusiveTx?.(pending.id);
    if (!tx) throw new Error("Native runtime does not support attached exclusive transactions");
    pending.txByView.set(this, tx);
    return tx;
  }

  private releaseTransactionViews(pending: PendingTx): void {
    for (const tx of pending.txByView.values()) {
      tx.close?.();
    }
    pending.txByView.clear();
  }

  private exclusiveTx(id: OpenTransactionId): Tx {
    if (!this.db.exclusiveTx) {
      throw new Error(
        "Native runtime cannot perform exclusive transaction writes: " +
          "the native runtime exclusive transaction API is unavailable.",
      );
    }
    return this.db.exclusiveTx(id);
  }

  private mergeableTxForIdentity(id: OpenTransactionId, identity: Uint8Array): Tx {
    if (!this.db.mergeableTxForIdentity) {
      throw new Error(
        "Native runtime cannot perform session-scoped transaction writes: " +
          "the native runtime mergeable transaction API has no identity-aware staging methods.",
      );
    }
    return this.db.mergeableTxForIdentity(id, identity);
  }

  private pumpSubscriptions(): void {
    for (const [handle, subscription] of this.subscriptions) {
      this.startSubscriptionReader(handle, subscription);
    }
  }

  private withPermissionAdviceTimeout(
    start: () => NativePermissionAdviceResult,
  ): Promise<PermissionAdvice> {
    if (this !== this.ownerRuntime) return this.ownerRuntime.withPermissionAdviceTimeout(start);
    if (this.closed || !this.serverTransport || !this.serverCarrier) {
      return Promise.resolve("unknown");
    }
    const started = start();
    const request: NativePermissionAdviceRequest = isPendingNativePermissionAdvice(started)
      ? {
          promise: this.awaitNativePermissionAdvice(started),
          cancel: () => started.cancel(),
        }
      : typeof started === "string"
        ? {
            promise: Promise.resolve(
              started === "allowed" || started === "denied" || started === "unknown"
                ? started
                : "unknown",
            ),
            cancel: () => {},
          }
        : started;
    return new Promise((resolve) => {
      let settled = false;
      const finish = (advice: PermissionAdvice) => {
        if (settled) return;
        settled = true;
        clearTimeout(timeout);
        resolve(advice);
      };
      const timeout = setTimeout(() => {
        request.cancel();
        finish("unknown");
      }, 2_000);
      request.promise.then(finish, () => finish("unknown"));
    });
  }

  /**
   * Keep a permission request's cancel-safe core future on its owning JS
   * thread, allowing the normal peer pump to deliver its authority receipt
   * between polls.  A missing or malformed native result fails closed.
   */
  private async awaitNativePermissionAdvice(
    pending: PendingNativePermissionAdvice,
  ): Promise<PermissionAdvice> {
    for (;;) {
      if (this.closed) {
        pending.cancel();
        return "unknown";
      }
      const advice = pending.poll();
      if (advice !== null) {
        return advice === "allowed" || advice === "denied" || advice === "unknown"
          ? advice
          : "unknown";
      }
      await this.pumpServerTransport();
      await sleep(0);
    }
  }

  private scheduleCoreWake(urgency: CoreTickWake): void {
    if (this.closed) return;
    if (urgency === "after-current-turn") {
      // A microtask would set `coreTickAgain` while a core tick is still
      // running, recursively entering the next tick before the browser can
      // deliver newly-produced frames. Cold subscriber hydration must yield
      // to the host task queue so inbound commits/fates get a fair turn.
      setTimeout(() => this.scheduleCoreWake("immediate"), 0);
      return;
    }
    if (urgency.startsWith("after:")) {
      const delayMs = Number(urgency.slice("after:".length));
      if (!Number.isSafeInteger(delayMs) || delayMs < 0) return;
      // A protocol admission deadline is not a deferred microtask. Keep the
      // host event loop live and only wake the thread-affine core after the
      // promised window has elapsed.
      setTimeout(() => this.scheduleCoreWake("immediate"), delayMs);
      return;
    }
    this.notifyPeerTransportWork();
    if (urgency === "immediate") {
      this.scheduleCoreTick();
      return;
    }
    queueMicrotask(() => {
      this.scheduleCoreTick();
    });
  }

  private scheduleCoreTick(): void {
    if (this.closed) return;
    if (this.coreTickRunning) {
      this.coreTickAgain = true;
      return;
    }
    if (this.coreTickScheduled) return;
    this.coreTickScheduled = true;
    queueMicrotask(() => {
      this.coreTickScheduled = false;
      void this.runCoreTick().catch(reportAsyncRuntimeError);
    });
  }

  private runCoreTick(): Promise<void> {
    if (this.closed) return Promise.resolve();
    if (this.coreTickRunning) {
      this.coreTickAgain = true;
      return this.coreTickCompletion ?? Promise.resolve();
    }
    this.coreTickRunning = true;
    let resolve!: () => void;
    let reject!: (error: unknown) => void;
    const completion = new Promise<void>((onResolve, onReject) => {
      resolve = onResolve;
      reject = onReject;
    });
    this.coreTickCompletion = completion;
    void this.driveCoreTicks().then(resolve, reject);
    return completion;
  }

  private async driveCoreTicks(): Promise<void> {
    let yielded = false;
    try {
      for (let round = 0; ; round += 1) {
        this.coreTickAgain = false;
        await this.db.tick();
        this.pumpSubscriptions();
        this.scheduleServerPump();
        this.notifyPeerTransportWork();
        if (this.closed || !this.coreTickAgain) break;
        if (round + 1 >= MAX_CORE_TICKS_PER_TURN) {
          yielded = true;
          break;
        }
      }
    } finally {
      this.drainTransportRetirements();
      this.coreTickRunning = false;
      this.coreTickCompletion = null;
    }
    if (yielded && !this.closed) {
      setTimeout(() => this.scheduleCoreTick(), 0);
    }
  }

  private drainTransportRetirements(): void {
    const retirements = [...this.pendingTransportRetirements];
    this.pendingTransportRetirements.clear();
    for (const [transport, waiters] of retirements) {
      try {
        transport.close();
        for (const waiter of waiters) waiter.resolve();
      } catch (error) {
        for (const waiter of waiters) waiter.reject(error);
      }
    }
  }

  private startSubscriptionReader(handle: number, subscription: SubscriptionState): void {
    if (subscription.cancelled) return;
    for (const source of subscription.sources) {
      if (!isReadableSubscriptionReader(source.source)) {
        if (source.reading) continue;
        source.reading = true;
        void this.drainNativeSubscription(handle, subscription, source);
        continue;
      }
      if (source.reading) continue;
      source.reading = true;
      void this.readSubscription(handle, subscription, source);
    }
  }

  private async readSubscription(
    handle: number,
    subscription: SubscriptionState,
    source: SubscriptionSourceState,
  ): Promise<void> {
    if (!isReadableSubscriptionReader(source.source)) return;
    try {
      while (!subscription.cancelled && this.subscriptions.get(handle) === subscription) {
        const next = await source.source.read();
        if (next.done || subscription.cancelled) return;
        try {
          this.applySubscriptionChunk(subscription, next.value);
        } catch (error) {
          this.failSubscription(
            subscription,
            error instanceof Error ? error : new Error(String(error)),
          );
        }
      }
    } finally {
      source.reading = false;
    }
  }

  private async drainNativeSubscription(
    handle: number,
    subscription: SubscriptionState,
    source: SubscriptionSourceState,
  ): Promise<void> {
    if (isReadableSubscriptionReader(source.source)) return;
    try {
      while (!subscription.cancelled && this.subscriptions.get(handle) === subscription) {
        const batch = source.source.readAll();
        if (!Array.isArray(batch)) {
          await this.pumpServerTransport();
          const retryAfterMs = batch.retryAfterMs?.() ?? 0;
          await sleep(Math.max(0, retryAfterMs));
          continue;
        }
        for (const event of batch) {
          if (subscription.cancelled || this.subscriptions.get(handle) !== subscription) return;
          try {
            this.applySubscriptionChunk(subscription, event);
          } catch (error) {
            this.failSubscription(
              subscription,
              error instanceof Error ? error : new Error(String(error)),
            );
          }
        }
        if (batch.length === 0) return;
      }
    } finally {
      source.reading = false;
    }
  }

  private applySubscriptionChunk(subscription: SubscriptionState, value: unknown): void {
    const chunk = normalizeSubscriptionChunk(value);
    if (chunk.type === "closed") {
      clearDeferredPlaceholderBuffer(subscription);
      closeSubscriptionSourceState(subscription);
      subscription.cancelled = true;
      return;
    }
    if (chunk.type === "rejected") {
      if (chunk.reason.type === "ShapeRegistrationPendingCatalogueAdmission") {
        return;
      }
      this.failSubscription(subscription, subscriptionRejectionError(chunk.reason));
      return;
    }
    if (chunk.type === "delta" && chunk.publishable === false) return;
    if (chunk.type === "snapshot") {
      const previousRows = subscription.rows;
      const wasOpened = subscription.opened;
      subscription.rows = rowsFromRelationSnapshot(
        chunk.snapshot,
        this.schema,
        subscription.outputColumns?.rootColumns,
        "full-record",
      );
      subscription.rowIndexByKey = indexRowsByKey(subscription.rows);
      subscription.opened = true;
      this.publishSubscriptionRows(
        subscription,
        wasOpened
          ? runtimeDeltaFromRows(
              subscription.rows,
              previousRows,
              this.schema,
              subscription.outputColumns,
            )
          : runtimeResetDeltaFromRows(subscription.rows, this.schema, subscription.outputColumns),
        chunk.settled,
        !wasOpened,
      );
    } else {
      if (chunk.reset) {
        subscription.rows = [];
        subscription.rowIndexByKey = new Map();
        clearDeferredPlaceholderBuffer(subscription);
      }
      const applied = applySubscriptionDeltaWithRootDelta(
        subscription.rows,
        chunk.delta,
        this.schema,
        chunk.reset === true,
        subscription.outputColumns,
      );
      subscription.rows = applied.rows;
      subscription.rowIndexByKey = applied.rowIndexByKey;
      subscription.opened = true;
      const terminalOperations = decodeRuntimeTerminalOperations(
        chunk.terminalOperations,
        subscription.outputColumns?.rootColumns,
      );
      if (
        subscriptionRowsRequireBufferedPublication(
          subscription.rows,
          this.schema,
          subscription.outputColumns,
        )
      ) {
        if (chunk.settled === true) {
          throw new Error(
            "settled relation subscription chunk retained unresolved placeholder rows",
          );
        }
        this.deferSubscriptionRows(
          subscription,
          terminalOperations,
          chunk.terminalOperations,
          chunk.reset === true,
          chunk.delta,
        );
        return;
      }
      applied.rootDelta.terminalOperations = terminalOperations;
      this.publishSubscriptionRows(
        subscription,
        applied.rootDelta,
        chunk.settled,
        chunk.reset === true,
      );
    }
  }

  private publishSubscriptionRows(
    subscription: SubscriptionState,
    rootDelta: RuntimeSubscriptionDelta,
    settled: boolean | undefined,
    reset: boolean,
  ): void {
    if (this.subscriptionCallbacksAreSettledGated(subscription) && settled === false) {
      subscription.deferredVisiblePublication = true;
      subscription.deferredVisibleReset ||= reset;
      subscription.deferredTerminalOperations.push(...(rootDelta.terminalOperations ?? []));
      return;
    }

    let visibleDelta = rootDelta;
    if (
      subscription.deferredVisiblePublication ||
      subscription.deferredVisibleReset ||
      !subscription.visibleOpened
    ) {
      const publishReset = subscription.deferredVisibleReset || !subscription.visibleOpened;
      if (publishReset) {
        visibleDelta = runtimeResetDeltaFromRows(
          subscription.rows,
          this.schema,
          subscription.outputColumns,
        );
      } else {
        visibleDelta = runtimeDeltaFromRows(
          subscription.rows,
          subscription.visibleRows,
          this.schema,
          subscription.outputColumns,
        );
      }
    }

    // A canonical delta rebuilt from `subscription.rows` already contains the
    // full present-state terminal values. Replaying producer operations that
    // led to that state on top of it can address occurrence lifecycles that no
    // longer exist (for example, a deferred Move after a synthesized reset).
    // Producer terminal history belongs only to a forwarded producer delta.
    if (visibleDelta === rootDelta) {
      const terminalOperations = [
        ...subscription.deferredTerminalOperations,
        ...(rootDelta.terminalOperations ?? []),
      ];
      if (terminalOperations.length > 0) {
        visibleDelta.terminalOperations = terminalOperations;
      }
    }

    subscription.callback?.(visibleDelta);
    subscription.visibleRows = [...subscription.rows];
    subscription.visibleOpened = true;
    clearDeferredPlaceholderBuffer(subscription);
  }

  private subscriptionCallbacksAreSettledGated(subscription: SubscriptionState): boolean {
    const tier = (subscription.opts as { tier?: unknown }).tier;
    return tier === "global" || (this.nonDurableClient && tier === "edge");
  }

  private deferSubscriptionRows(
    subscription: SubscriptionState,
    terminalOperations: RuntimeTerminalOperation[] | undefined,
    nativeTerminalOperations: NativeTerminalOperation[] | undefined,
    reset: boolean,
    delta: NativeSubscriptionDelta,
  ): void {
    subscription.deferredVisiblePublication = true;
    subscription.deferredVisibleReset ||= reset;
    subscription.deferredTerminalOperations.push(...(terminalOperations ?? []));
    subscription.deferredPlaceholderChunks = reset ? 1 : subscription.deferredPlaceholderChunks + 1;
    subscription.deferredPlaceholderRows = subscription.rows.length;
    subscription.deferredPlaceholderBytes = reset
      ? subscriptionDeltaPayloadBytes(delta, nativeTerminalOperations)
      : subscription.deferredPlaceholderBytes +
        subscriptionDeltaPayloadBytes(delta, nativeTerminalOperations);
    if (
      subscription.deferredPlaceholderChunks > MAX_DEFERRED_PLACEHOLDER_CHUNKS ||
      subscription.deferredPlaceholderRows > MAX_DEFERRED_PLACEHOLDER_ROWS ||
      subscription.deferredPlaceholderBytes > MAX_DEFERRED_PLACEHOLDER_BYTES
    ) {
      throw new Error(
        "relation subscription buffered unresolved placeholder rows beyond bounded limits",
      );
    }
  }

  private scheduleServerPump(): void {
    if (this.closed || !this.serverTransport) return;
    if (this.serverPumpRunning) {
      this.serverPumpAgain = true;
      return;
    }
    if (this.serverPumpScheduled) return;
    this.serverPumpScheduled = true;
    setTimeout(() => {
      this.serverPumpScheduled = false;
      if (this.closed) return;
      void this.pumpServerTransport().catch((error) => this.handleServerTransportError(error));
    }, SERVER_PUMP_DEBOUNCE_MS);
  }

  private notifyPeerTransportWork(requiresDistinctPass = false): void {
    for (const listener of this.peerTransportWorkListeners) {
      listener(requiresDistinctPass);
    }
  }

  private hasUpstream(): boolean {
    return this.serverTransport != null || this.peerUpstreamAttached;
  }

  private async pumpServerTransport(): Promise<void> {
    const transport = this.serverTransport;
    const carrier = this.serverCarrier;
    const generation = this.serverConnectionGeneration;
    if (this.closed || !transport || !carrier) return;
    if (this.serverPumpRunning) {
      this.serverPumpAgain = true;
      return;
    }
    this.serverPumpRunning = true;
    try {
      let processedInbound =
        this.pendingInboundServerFrames.length > 0
          ? await this.routePendingInboundServerFrames()
          : false;
      processedInbound ||= this.serverInboundProcessed;
      this.serverInboundProcessed = false;
      for (let round = 0; round < 32; round += 1) {
        await this.runCoreTick();
        this.flushAuxiliaryOutbound(transport, carrier, generation);
        if (processedInbound || this.serverInboundProcessed) {
          // Frame arrival wakes waiters promptly, but the observable write or
          // coverage state changes only after the evaluator consumes it.
          // Publish that second edge so waiters re-read settled state.
          processedInbound = false;
          this.serverInboundProcessed = false;
          this.notifyServerTransportWork();
          // A frame can be routed by the auxiliary path while the waiter is
          // still consuming the arrival edge. Publish once more after those
          // promise continuations have had a chance to re-arm.
          queueMicrotask(() => this.notifyServerTransportWork());
        }
        if (
          this.closed ||
          generation !== this.serverConnectionGeneration ||
          transport !== this.serverTransport ||
          carrier !== this.serverCarrier
        ) {
          return;
        }
        const frames = normalizeTransportFrames(transport.recvWireFrames());
        if (frames.length > 0) {
          this.sendServerFrames(frames, carrier, generation);
        }
        this.pumpSubscriptions();
        if (frames.length === 0) {
          return;
        }
      }
      this.serverPumpAgain = true;
    } finally {
      this.serverPumpRunning = false;
      if (this.serverPumpAgain) {
        this.serverPumpAgain = false;
        this.scheduleServerPump();
      }
    }
  }

  private async routePendingInboundServerFrames(): Promise<boolean> {
    let processedInbound = false;
    const operation = this.serverInboundRouting.then(async () => {
      const transport = this.serverTransport;
      if (!transport || this.pendingInboundServerFrames.length === 0) return;
      const frames = this.pendingInboundServerFrames.splice(0);
      processedInbound = true;
      this.serverInboundProcessed = true;
      const canonical: Uint8Array[] = [];
      for (const frame of frames) {
        const routed = transport.routeAuxiliaryWireFrame
          ? await transport.routeAuxiliaryWireFrame(frame)
          : frame;
        if (routed != null) canonical.push(normalizeTransportFrame(routed));
      }
      this.publishAuxiliaryTrace(transport);
      if (canonical.length > 0) {
        if (transport.sendWireFrames) transport.sendWireFrames(canonical);
        else for (const frame of canonical) transport.sendWireFrame(frame);
      }
      // Carrier ingress can change the result visible through every local peer,
      // including a subscriber whose evaluator pass began before this batch was
      // admitted. Require one distinct post-admission pass. This is deliberately
      // narrower than the general native tick scheduler, whose routine wakes
      // must remain coalescible to avoid self-sustaining peer-pump loops.
      this.notifyPeerTransportWork(true);
      const carrier = this.serverCarrier;
      if (carrier) {
        this.flushAuxiliaryOutbound(transport, carrier, this.serverConnectionGeneration);
      }
    });
    this.serverInboundRouting = operation.then(
      () => undefined,
      () => undefined,
    );
    await operation;
    return processedInbound;
  }

  private flushAuxiliaryOutbound(
    transport: Transport,
    carrier: WebSocketCarrier,
    generation: number,
  ): void {
    const receive = transport.recvAuxiliaryWireFrames;
    if (!receive) return;
    const frames = normalizeTransportFrames(receive.call(transport));
    if (frames.length > 0) this.sendServerFrames(frames, carrier, generation);
    this.publishAuxiliaryTrace(transport);
  }

  private publishAuxiliaryTrace(transport: Transport): void {
    const entries = transport.takeAuxiliaryTrace?.();
    if (!entries || entries.length === 0) return;
    for (const listener of this.auxiliaryTraceListeners) {
      listener(entries as AuxiliaryRelayTrace[]);
    }
  }

  private async watchAuxiliaryOutbound(
    transport: Transport,
    carrier: WebSocketCarrier,
    generation: number,
  ): Promise<void> {
    while (
      !this.closed &&
      transport === this.serverTransport &&
      carrier === this.serverCarrier &&
      generation === this.serverConnectionGeneration
    ) {
      const readiness = transport.auxiliaryOutboundReady?.();
      if (!readiness || typeof readiness === "boolean") return;
      await readiness;
      if (transport !== this.serverTransport || carrier !== this.serverCarrier) return;
      this.flushAuxiliaryOutbound(transport, carrier, generation);
    }
  }

  private sendServerFrames(
    frames: Uint8Array[],
    carrier = this.serverCarrier,
    generation = this.serverConnectionGeneration,
  ): void {
    if (
      !carrier ||
      generation !== this.serverConnectionGeneration ||
      carrier !== this.serverCarrier
    ) {
      this.queuedServerFrames.push(...frames);
      return;
    }
    void carrier.sendBatch(frames).catch((error) => {
      this.handleServerTransportError(error, generation);
    });
  }

  private flushQueuedServerFrames(carrier: WebSocketCarrier): void {
    if (this.queuedServerFrames.length === 0 || carrier !== this.serverCarrier) return;
    const frames = this.queuedServerFrames.splice(0);
    void carrier.sendBatch(frames).catch((error) => {
      this.handleServerTransportError(error);
    });
  }

  private handleServerTransportError(
    error: unknown,
    generation = this.serverConnectionGeneration,
  ): void {
    if (generation !== this.serverConnectionGeneration) return;
    const message = errorMessage(error);
    if (this.serverTransportError && message === "websocket closed") return;
    const isFirstTerminalError = this.serverTransportError === null;
    this.serverTransportError = error instanceof Error ? error : new Error(message);
    this.failRemoteSubscriptions(this.serverTransportError);
    this.resolveServerTransportErrorWaiters(this.serverTransportError);
    if (isFirstTerminalError) this.serverTransportErrorCallback?.(this.serverTransportError);
  }

  /**
   * A server may reject the first handshake while its authoritative catalogue
   * is still bootstrapping. This is a protocol-level retry, not a failed
   * upstream: keep remote waits pending and reconnect with bounded backoff.
   */
  private retryPreHelloConnection(
    attempt: ServerConnectionAttempt,
  ): Promise<WebSocketCarrier> | null {
    if (
      this.closed ||
      attempt !== this.serverConnectionAttempt ||
      attempt.generation !== this.serverConnectionGeneration ||
      attempt.carrier !== this.serverCarrier ||
      !this.serverEndpointUrl ||
      !this.serverAuthJson
    ) {
      return null;
    }
    const url = this.serverEndpointUrl;
    const authJson = this.serverAuthJson;
    const delay = Math.min(
      PRE_HELLO_RETRY_INITIAL_DELAY_MS * 2 ** this.preHelloRetryCount,
      PRE_HELLO_RETRY_MAX_DELAY_MS,
    );
    this.preHelloRetryCount = Math.min(this.preHelloRetryCount + 1, 30);
    this.serverConnectionGeneration += 1;
    this.serverConnectionAttempt = null;
    this.serverCarrier = null;
    this.serverCarrierPromise = null;
    this.resolveServerTransportWorkWaiters();
    attempt.carrier.close();
    return new Promise((resolve, reject) => {
      this.serverReconnectReject = reject;
      this.serverReconnectTimer = setTimeout(() => {
        this.serverReconnectTimer = null;
        this.serverReconnectReject = null;
        if (this.closed || this.serverEndpointUrl !== url || this.serverAuthJson !== authJson) {
          reject(new Error("server transport disconnected"));
          return;
        }
        this.connect(url, authJson);
        const reconnect = this.serverCarrierPromise;
        if (!reconnect) {
          reject(new Error("server transport reconnect was not started"));
          return;
        }
        void reconnect.then(resolve, reject);
      }, delay);
    });
  }

  private clearServerReconnectTimer(): void {
    if (this.serverReconnectTimer) {
      clearTimeout(this.serverReconnectTimer);
      this.serverReconnectTimer = null;
    }
    this.serverReconnectReject?.(new Error("server transport disconnected"));
    this.serverReconnectReject = null;
  }

  private finishServerConnectionAttempt(attempt: ServerConnectionAttempt, error: Error): void {
    if (attempt.finished) return;
    attempt.finished = true;
    attempt.outcome = error;
    attempt.resolveTerminal(error);
    const isCurrent =
      attempt === this.serverConnectionAttempt &&
      attempt.generation === this.serverConnectionGeneration &&
      attempt.carrier === this.serverCarrier;
    if (isCurrent) {
      this.serverConnectionGeneration += 1;
      this.serverConnectionAttempt = null;
      this.serverCarrier = null;
      this.serverCarrierPromise = null;
    }
    attempt.carrier.close();
    const transport = attempt.transport;
    attempt.transport = null;
    if (transport) {
      if (transport === this.serverTransport) this.serverTransport = null;
      attempt.retirement = this.retirePeerTransport(transport).catch(reportAsyncRuntimeError);
    }
    if (isCurrent) {
      this.handleServerTransportError(error);
      this.resolveServerTransportWorkWaiters();
    }
  }

  private failRemoteSubscriptions(error: Error): void {
    for (const subscription of this.subscriptions.values()) {
      if (subscription.cancelled) continue;
      const tier = (subscription.opts as { tier?: unknown }).tier ?? "local";
      if (tier !== "edge" && tier !== "global") continue;
      this.failSubscription(subscription, error);
    }
  }

  private failSubscription(subscription: SubscriptionState, error: Error): void {
    if (subscription.cancelled) return;
    subscription.cancelled = true;
    clearDeferredPlaceholderBuffer(subscription);
    closeSubscriptionSourceState(subscription);
    try {
      subscription.callback?.(error);
    } catch (callbackError) {
      setTimeout(() => {
        throw callbackError;
      }, 0);
    }
  }

  private throwServerTransportErrorForTier(tier: string): void {
    if ((tier === "edge" || tier === "global") && this.serverTransportError) {
      throw this.serverTransportError;
    }
  }

  private waitForServerTransportError(
    tier: string,
  ): { promise: Promise<never>; cancel: () => void } | null {
    if (tier !== "edge" && tier !== "global") return null;
    if (this.serverTransportError) {
      return {
        promise: Promise.reject(this.serverTransportError),
        cancel: () => {},
      };
    }
    const waiter: ServerTransportErrorWaiter = {
      active: true,
      reject: () => {},
    };
    const promise = new Promise<never>((_, reject) => {
      waiter.reject = reject;
    });
    this.serverTransportErrorWaiters.push(waiter);
    return {
      promise,
      cancel: () => {
        if (!waiter.active) return;
        waiter.active = false;
        const index = this.serverTransportErrorWaiters.indexOf(waiter);
        if (index >= 0) this.serverTransportErrorWaiters.splice(index, 1);
      },
    };
  }

  private resolveServerTransportErrorWaiters(error: Error): void {
    const waiters = this.serverTransportErrorWaiters.splice(0);
    for (const waiter of waiters) {
      if (waiter.active) waiter.reject(error);
    }
  }

  private clearServerTransportErrorWaiters(): void {
    for (const waiter of this.serverTransportErrorWaiters) {
      waiter.active = false;
    }
    this.serverTransportErrorWaiters.length = 0;
  }

  private waitForServerTransportWork(
    tier: string,
    observedEpoch: number,
  ): { promise: Promise<void>; cancel: () => void } | null {
    if (tier !== "edge" && tier !== "global") return null;
    if (
      this.serverTransportWorkEpoch !== observedEpoch ||
      this.pendingInboundServerFrames.length > 0
    ) {
      return { promise: Promise.resolve(), cancel: () => {} };
    }
    const waiter: ServerTransportWorkWaiter = {
      active: true,
      resolve: () => {},
    };
    const promise = new Promise<void>((resolve) => {
      waiter.resolve = resolve;
    });
    this.serverTransportWorkWaiters.push(waiter);
    return {
      promise,
      cancel: () => {
        if (!waiter.active) return;
        waiter.active = false;
        const index = this.serverTransportWorkWaiters.indexOf(waiter);
        if (index >= 0) this.serverTransportWorkWaiters.splice(index, 1);
      },
    };
  }

  private notifyServerTransportWork(): void {
    this.serverTransportWorkEpoch += 1;
    this.resolveServerTransportWorkWaiters();
  }

  private resolveServerTransportWorkWaiters(): void {
    const waiters = this.serverTransportWorkWaiters.splice(0);
    for (const waiter of waiters) {
      if (waiter.active) waiter.resolve();
    }
  }
}

function closeSubscriptionSourceState(subscription: SubscriptionState): void {
  for (const source of subscription.sources) {
    closeSubscriptionSource(source.source);
  }
}

function clearDeferredPlaceholderBuffer(subscription: SubscriptionState): void {
  subscription.deferredVisiblePublication = false;
  subscription.deferredVisibleReset = false;
  subscription.deferredTerminalOperations = [];
  subscription.deferredPlaceholderChunks = 0;
  subscription.deferredPlaceholderRows = 0;
  subscription.deferredPlaceholderBytes = 0;
}

function normalizeTransportFrames(frames: unknown[]): Uint8Array[] {
  return frames.filter(
    (frame): frame is Uint8Array =>
      ArrayBuffer.isView(frame) && frame.constructor.name === "Uint8Array",
  );
}

function normalizeTransportFrame(frame: unknown): Uint8Array {
  const normalized = normalizeTransportFrames([frame])[0];
  if (!normalized) throw new Error("native transport returned a non-byte wire frame");
  return normalized;
}

function recordWrite(write: Write, writes: Map<string, Write>): TxId {
  const id = write.txId as TxId;
  writes.set(id, write);
  return id as TxId;
}

function txIdFromContext(writeContext?: string | null): OpenTransactionId | undefined {
  if (!writeContext) return undefined;
  try {
    const parsed = JSON.parse(writeContext) as { transaction_id?: unknown };
    return typeof parsed.transaction_id === "string"
      ? (parsed.transaction_id as OpenTransactionId)
      : undefined;
  } catch {
    return undefined;
  }
}

type EncodedBranchView = { head: unknown; base?: unknown };

function branchViewFromWriteContext(writeContext?: string | null): EncodedBranchView | undefined {
  if (!writeContext) return undefined;
  try {
    const parsed = JSON.parse(writeContext) as { branch_view?: unknown };
    if (!parsed.branch_view || typeof parsed.branch_view !== "object") return undefined;
    const view = parsed.branch_view as { head?: unknown; base?: unknown };
    return view.head == null ? undefined : { head: view.head, base: view.base };
  } catch {
    return undefined;
  }
}

function largeValueWriteHasIncompatibleIdentity(
  writeContext: string | null | undefined,
  runtimeAuthor: Uint8Array,
): boolean {
  if (!writeContext) return false;
  let parsed: {
    issuer?: unknown;
    user_id?: unknown;
    session?: unknown;
    attribution?: unknown;
  };
  try {
    parsed = JSON.parse(writeContext) as {
      issuer?: unknown;
      user_id?: unknown;
      session?: unknown;
      attribution?: unknown;
    };
    if (parsed.attribution !== undefined) return true;
    const hasSessionIdentity =
      parsed.issuer !== undefined || parsed.user_id !== undefined || parsed.session !== undefined;
    if (!hasSessionIdentity) return false;
  } catch {
    return false;
  }
  try {
    const session = sessionFromWriteContext(writeContext);
    return session === null || !sameBytes(session.identity, runtimeAuthor);
  } catch {
    return true;
  }
}

function rejectAttributedBranchWrite(
  attribution: Uint8Array | undefined,
  branchView: EncodedBranchView | undefined,
): void {
  if (attribution && branchView) {
    throw new Error("Backend-attributed writes do not support branch views");
  }
}

function requireBackendAttributionAbi<T>(method: T | undefined, operation: string): T {
  if (!method) {
    throw new Error(
      `Native runtime does not support backend-attributed ${operation}; rebuild the matching Jazz native artifact`,
    );
  }
  return method;
}

function sessionFromWriteContext(writeContext?: string | null): RuntimeSession | null {
  if (!writeContext) return null;
  try {
    const parsed = JSON.parse(writeContext) as {
      issuer?: unknown;
      user_id?: unknown;
      claims?: unknown;
      authMode?: unknown;
      attribution?: unknown;
      [TRUSTED_RESERVED_SESSION_TOKEN_FIELD]?: unknown;
      session?: {
        issuer?: unknown;
        user_id?: unknown;
        claims?: unknown;
        authMode?: unknown;
        [TRUSTED_RESERVED_SESSION_TOKEN_FIELD]?: unknown;
      };
    };
    if (parsed.attribution === SYSTEM_AUTHOR_ID) {
      throw new Error("Native runtime public session uses reserved issuer");
    }
    const attributedAuthor =
      typeof parsed.attribution === "string"
        ? parsePublicCanonicalAuthor(parsed.attribution)
        : null;
    const userId =
      attributedAuthor?.user_id ??
      (typeof parsed.user_id === "string"
        ? parsed.user_id
        : typeof parsed.session?.user_id === "string"
          ? parsed.session.user_id
          : undefined);
    if (!userId || !isUsableSubject(userId)) return null;
    const issuer = attributedAuthor?.issuer ?? parsed.session?.issuer ?? parsed.issuer;
    if (typeof issuer !== "string" || !isUsableSubject(issuer)) {
      throw new Error("session is missing issuer");
    }
    const session: Pick<Session, "issuer" | "user_id" | "authMode"> = {
      issuer,
      user_id: userId,
      authMode: (typeof parsed.session?.authMode === "string"
        ? parsed.session.authMode
        : typeof parsed.authMode === "string"
          ? parsed.authMode
          : "external") as Session["authMode"],
    };
    assertPublicSessionIssuer(
      session.issuer,
      session.user_id,
      session.authMode,
      parsed.session?.[TRUSTED_RESERVED_SESSION_TOKEN_FIELD] ??
        parsed[TRUSTED_RESERVED_SESSION_TOKEN_FIELD],
    );
    const reservedToken =
      parsed.session?.[TRUSTED_RESERVED_SESSION_TOKEN_FIELD] ??
      parsed[TRUSTED_RESERVED_SESSION_TOKEN_FIELD];
    const claims = sessionClaims(parsed.session?.claims ?? parsed.claims, session);
    return {
      ...session,
      claims,
      identity: authorBytesForSession(session),
      backendAuthority:
        session.issuer === SYSTEM_SESSION_ISSUER &&
        session.user_id === SYSTEM_AUTHOR_ID &&
        isTrustedReservedSession(session, reservedToken),
    };
  } catch (error) {
    if (
      error instanceof Error &&
      (error.message === "session is missing issuer" ||
        error.message === "Native runtime public session uses reserved issuer")
    ) {
      throw error;
    }
    return null;
  }
}

function parsePublicCanonicalAuthor(value: string): { issuer: string; user_id: string } | null {
  const parsed = parseCanonicalAuthorSubject(value);
  if (parsed && isReservedJazzIssuer(parsed.issuer)) {
    throw new Error("Native runtime public session uses reserved issuer");
  }
  return parsed ? { issuer: parsed.issuer, user_id: parsed.user_id } : null;
}

function assertPublicSessionIssuer(
  issuer: string,
  userId: string,
  authMode: string | undefined,
  trustedToken?: unknown,
): void {
  if (
    isReservedJazzIssuer(issuer) &&
    !(
      (authMode === "local-first" || authMode === "anonymous" || authMode === "external") &&
      isTrustedReservedSession({ issuer, user_id: userId, authMode }, trustedToken)
    )
  ) {
    throw new Error("Native runtime public session uses reserved issuer");
  }
}

function updatedAtMsFromWriteContext(writeContext?: string | null): number | undefined {
  if (!writeContext) return undefined;
  let parsed: { updated_at?: unknown };
  try {
    parsed = JSON.parse(writeContext) as { updated_at?: unknown };
  } catch {
    return undefined;
  }
  if (typeof parsed.updated_at !== "number") return undefined;
  if (!Number.isSafeInteger(parsed.updated_at) || parsed.updated_at < 0) {
    throw new Error("updatedAt must be a nonnegative safe integer");
  }
  return parsed.updated_at;
}

function effectiveUpdatedAtMs(writeContext?: string | null): number | null {
  return updatedAtMsFromWriteContext(writeContext) ?? Date.now();
}

function txStateMessage(
  openTransactionId: string,
  completedBatches: Map<string, CompletedTx>,
): string {
  const completed = completedBatches.get(openTransactionId);
  if (completed?.state === "committed") {
    return `open transaction ${openTransactionId} is already committed`;
  }
  return `open transaction ${openTransactionId} has already been completed or was never opened`;
}

function commitTransactionMessage(
  openTransactionId: string,
  completedBatches: Map<string, CompletedTx>,
): string {
  const message = txStateMessage(openTransactionId, completedBatches);
  return completedBatches.get(openTransactionId)?.state === "committed"
    ? `Write error: ${message}`
    : `Commit transaction failed: Write error: ${message}`;
}

function rollbackTransactionMessage(
  openTransactionId: string,
  completedBatches: Map<string, CompletedTx>,
): string {
  const message = txStateMessage(openTransactionId, completedBatches);
  return completedBatches.get(openTransactionId)?.state === "committed"
    ? `Write error: ${message}`
    : `Rollback transaction failed: Write error: ${message}`;
}

function assertTransactionReadOpen(
  optionsJson: string | null | undefined,
  pendingTxs: Map<string, PendingTx>,
  completedTxs: Map<string, CompletedTx>,
): void {
  const openTransactionId = openTransactionIdFromOptions(optionsJson);
  if (!openTransactionId || pendingTxs.has(openTransactionId)) return;
  throw new Error(
    `Query setup failed: Write error: ${txStateMessage(openTransactionId, completedTxs)}`,
  );
}

function pendingTxFromOptions(
  optionsJson: string | null | undefined,
  pendingTxs: Map<string, PendingTx>,
): PendingTx | undefined {
  const openTransactionId = openTransactionIdFromOptions(optionsJson);
  return openTransactionId ? pendingTxs.get(openTransactionId) : undefined;
}

function openTransactionIdFromOptions(optionsJson?: string | null): OpenTransactionId | undefined {
  if (!optionsJson) return undefined;
  try {
    const parsed = JSON.parse(optionsJson) as { transaction_id?: unknown };
    return typeof parsed.transaction_id === "string"
      ? (parsed.transaction_id as OpenTransactionId)
      : undefined;
  } catch {
    return undefined;
  }
}

function readOptions(
  tier?: string | null,
  includeDeleted = false,
  optionsJson?: string | null,
): unknown {
  const options = optionsJson == null ? ({} as Record<string, unknown>) : JSON.parse(optionsJson);
  const readOptions: Record<string, unknown> = { tier: tier ?? "local" };
  if (includeDeleted) readOptions.include_deleted = true;
  if (options.propagation === "local-only") readOptions.propagation = "local_only";
  if (options.propagation === "full") readOptions.propagation = "full";
  const readView = options.read_view ?? options.readView;
  if (readView != null) readOptions.read_view = readView;
  return readOptions;
}

function readPropagationIsFull(optionsJson?: string | null): boolean {
  if (optionsJson == null) return true;
  try {
    const options = JSON.parse(optionsJson) as { propagation?: unknown };
    return options.propagation == null || options.propagation === "full";
  } catch {
    return true;
  }
}

function assertSupportedReadOptions(tier?: string | null, optionsJson?: string | null): void {
  if (tier != null && !["local", "edge", "global"].includes(tier)) {
    throw new Error(`Native runtime received unsupported read tier '${tier}'`);
  }
  if (optionsJson != null) readSupportedReadOptions(optionsJson);
}

function readSession(sessionJson?: string | null): RuntimeSession | null {
  if (sessionJson == null) return null;
  const parsed = JSON.parse(sessionJson) as {
    issuer?: unknown;
    user_id?: unknown;
    claims?: unknown;
    authMode?: unknown;
    [TRUSTED_RESERVED_SESSION_TOKEN_FIELD]?: unknown;
  };
  if (typeof parsed.user_id !== "string" || !isUsableSubject(parsed.user_id)) {
    throw new Error("Native runtime session is missing user_id");
  }
  if (typeof parsed.issuer !== "string" || !isUsableSubject(parsed.issuer)) {
    throw new Error("Native runtime session is missing issuer");
  }
  const session = {
    issuer: parsed.issuer,
    user_id: parsed.user_id,
    authMode: (typeof parsed.authMode === "string"
      ? parsed.authMode
      : "external") as Session["authMode"],
  };
  assertPublicSessionIssuer(
    session.issuer,
    session.user_id,
    session.authMode,
    parsed[TRUSTED_RESERVED_SESSION_TOKEN_FIELD],
  );
  const backendAuthority =
    session.issuer === SYSTEM_SESSION_ISSUER &&
    session.user_id === SYSTEM_AUTHOR_ID &&
    isTrustedReservedSession(session, parsed[TRUSTED_RESERVED_SESSION_TOKEN_FIELD]);
  return {
    ...session,
    claims: sessionClaims(parsed.claims, session),
    identity: authorBytesForSession(session),
    backendAuthority,
  };
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}

function sessionClaims(
  rawClaims: unknown,
  session: { issuer: string; user_id: string; authMode?: string },
): Record<string, unknown> {
  return {
    ...(isRecord(rawClaims) ? rawClaims : {}),
    iss: session.issuer,
    sub: session.user_id,
    authMode: session.authMode ?? "external",
  };
}

function closeSubscriptionSource(source: SubscriptionSourceState["source"]): void {
  if ("close" in source && typeof source.close === "function") {
    source.close();
    return;
  }
  if ("cancel" in source && typeof source.cancel === "function") {
    void source.cancel().catch(() => {});
  }
}

function readSupportedReadOptions(optionsJson: string): void {
  const parsed = JSON.parse(optionsJson) as Record<string, unknown>;
  const propagation = parsed.propagation;
  if (propagation != null && propagation !== "full" && propagation !== "local-only") {
    throw new Error(
      `Native runtime does not support read propagation '${String(propagation)}' yet`,
    );
  }
}

function queryIncludesDeleted(queryJson: string): boolean {
  try {
    return (JSON.parse(queryJson) as { include_deleted?: unknown }).include_deleted === true;
  } catch {
    return false;
  }
}

function queryHasArraySubqueries(queryJson: string): boolean {
  try {
    const value = (JSON.parse(queryJson) as { array_subqueries?: unknown }).array_subqueries;
    return Array.isArray(value) && value.length > 0;
  } catch {
    return false;
  }
}

function queryUsesNativeRelationApi(queryJson: string): boolean {
  try {
    const relationIr = (JSON.parse(queryJson) as { relation_ir?: unknown }).relation_ir;
    return relationIrContainsNativeOperator(relationIr);
  } catch {
    return false;
  }
}

function relationIrContainsNativeOperator(value: unknown): boolean {
  if (!value || typeof value !== "object") return false;
  if (Array.isArray(value)) return value.some(relationIrContainsNativeOperator);
  const record = value as Record<string, unknown>;
  if ("Join" in record || "Gather" in record || "Union" in record) return true;
  return Object.values(record).some(relationIrContainsNativeOperator);
}

function assertNoUnsupportedPermissionIntrospection(queryJson: string): void {
  if (!queryContainsPermissionIntrospection(queryJson)) return;
  throw new Error(
    "Native runtime does not support permission-introspection query columns or predicates " +
      "($canRead) until unified policy lowering exists.",
  );
}

function queryContainsPermissionIntrospection(queryJson: string): boolean {
  const parsed = JSON.parse(queryJson) as {
    conditions?: unknown;
    relation_ir?: unknown;
    select?: unknown;
    select_columns?: unknown;
    array_subqueries?: unknown;
  };
  return (
    selectedColumnsContainPermissionIntrospection(parsed.select_columns ?? parsed.select) ||
    flatConditionsContainPermissionIntrospection(parsed.conditions) ||
    relationIrContainsPermissionPredicate(parsed.relation_ir) ||
    relationIrContainsPermissionProjection(parsed.relation_ir) ||
    arraySubqueriesContainPermissionIntrospection(parsed.array_subqueries)
  );
}

function relationIrContainsPermissionPredicate(value: unknown): boolean {
  return predicateIrContainsPermissionIntrospection(value);
}

function relationIrContainsPermissionProjection(value: unknown): boolean {
  if (!value || typeof value !== "object") return false;
  if (Array.isArray(value)) return value.some(relationIrContainsPermissionProjection);
  const record = value as Record<string, unknown>;
  const project = record.Project;
  if (project && typeof project === "object") {
    const columns = (project as { columns?: unknown }).columns;
    if (Array.isArray(columns)) {
      for (const entry of columns) {
        if (!entry || typeof entry !== "object") continue;
        const projection = entry as { expr?: unknown; source?: unknown };
        const column = readProjectedColumnRef(projection.expr ?? projection.source);
        if (column && isPermissionIntrospectionColumn(column)) return true;
      }
    }
  }
  return Object.values(record).some(relationIrContainsPermissionProjection);
}

function readMagicPredicateColumn(value: unknown): string | null {
  if (!value || typeof value !== "object") return null;
  const record = value as { left?: unknown; column?: unknown };
  return readColumnRef(record.left ?? record.column);
}

function readProjectedColumnRef(value: unknown): string | null {
  if (!value || typeof value !== "object") return null;
  if ("Column" in value) {
    return readColumnRef((value as { Column?: unknown }).Column);
  }
  return readColumnRef(value);
}

function selectedColumnsContainPermissionIntrospection(value: unknown): boolean {
  if (!Array.isArray(value)) return false;
  return value.some(
    (column) =>
      typeof column === "string" && isPermissionIntrospectionColumn(unqualifiedColumn(column)),
  );
}

function flatConditionsContainPermissionIntrospection(value: unknown): boolean {
  if (!Array.isArray(value)) return false;
  return value.some(predicateIrContainsPermissionIntrospection);
}

function arraySubqueriesContainPermissionIntrospection(value: unknown): boolean {
  if (!Array.isArray(value)) return false;
  return value.some((entry) => {
    if (!entry || typeof entry !== "object") return false;
    const record = entry as {
      filters?: unknown;
      nested_arrays?: unknown;
      select?: unknown;
      select_columns?: unknown;
    };
    return (
      selectedColumnsContainPermissionIntrospection(record.select_columns ?? record.select) ||
      arrayFiltersContainPermissionIntrospection(record.filters) ||
      arraySubqueriesContainPermissionIntrospection(record.nested_arrays)
    );
  });
}

function arrayFiltersContainPermissionIntrospection(value: unknown): boolean {
  if (!Array.isArray(value)) return false;
  return value.some(predicateIrContainsPermissionIntrospection);
}

function predicateIrContainsPermissionIntrospection(value: unknown): boolean {
  if (!value || typeof value !== "object") return false;
  if (Array.isArray(value)) return value.some(predicateIrContainsPermissionIntrospection);
  const record = value as Record<string, unknown>;

  // Preserve support for the legacy flat/array predicate envelopes while the
  // query adapter emits canonical predicate IR for all new queries.
  if (
    typeof record.op === "string" &&
    typeof record.column === "string" &&
    isPermissionIntrospectionColumn(unqualifiedColumn(record.column))
  ) {
    return true;
  }

  for (const key of ["Eq", "Ne", "Gt", "Ge", "Lt", "Le", "IsNull", "IsNotNull", "Contains"]) {
    const legacyPredicate = record[key];
    if (!legacyPredicate || typeof legacyPredicate !== "object") continue;
    const column = (legacyPredicate as { column?: unknown }).column;
    if (typeof column === "string" && isPermissionIntrospectionColumn(unqualifiedColumn(column))) {
      return true;
    }
  }

  const canonicalColumn =
    readMagicPredicateColumn(record.Cmp) ??
    readMagicPredicateColumn(record.In) ??
    readMagicPredicateColumn(record.IsNull) ??
    readMagicPredicateColumn(record.IsNotNull) ??
    readMagicPredicateColumn(record.Contains);
  if (canonicalColumn && isPermissionIntrospectionColumn(unqualifiedColumn(canonicalColumn))) {
    return true;
  }

  // This recursive walk deliberately includes And/Or/Not and nested array
  // filters. A forbidden column must be rejected regardless of predicate shape.
  return Object.entries(record).some(
    ([key, child]) => key !== "Literal" && predicateIrContainsPermissionIntrospection(child),
  );
}

function unqualifiedColumn(column: string): string {
  return column.split(".").at(-1) ?? column;
}

function subscriptionOutputColumns(
  queryJson: string,
  schema: WasmSchema,
): SubscriptionOutputColumns {
  const parsed = JSON.parse(queryJson) as {
    table?: unknown;
    select?: unknown;
    select_columns?: unknown;
    array_subqueries?: unknown;
    relation_ir?: unknown;
  };
  if (typeof parsed.table !== "string") {
    throw new Error("Native runtime only supports table queries in this slice");
  }
  return {
    rootTable: parsed.table,
    rootColumns: outputColumnsForTable(
      parsed.table,
      schema,
      readSelectColumns(parsed.select_columns ?? parsed.select),
      readQueryArraySubqueries(parsed.array_subqueries, parsed.table, schema) ?? [],
    ),
  };
}

/**
 * Compile the native producer's named terminal path and packed row payloads
 * into the logical edit shape consumed by the TypeScript materializer.
 */
function decodeRuntimeTerminalOperations(
  operations: readonly NativeTerminalOperation[] | undefined,
  rootColumns: readonly ColumnDescriptor[] | undefined,
): RuntimeTerminalOperation[] | undefined {
  if (!operations || operations.length === 0) return undefined;

  return operations.map((operation) => {
    let columns = rootColumns;
    let targetColumns: readonly ColumnDescriptor[] | undefined;
    const path: RuntimeTerminalOperation["path"] = operation.path.map((segment) => {
      if ("Key" in segment) return { Key: segment.Key };

      if (!columns) {
        throw new Error("native terminal collection path requires subscription output columns");
      }

      const collectionName = segment.Collection.startsWith(HIDDEN_INCLUDE_COLUMN_PREFIX)
        ? segment.Collection.slice(HIDDEN_INCLUDE_COLUMN_PREFIX.length)
        : segment.Collection;
      const collectionIndex = columns.findIndex((column) => column.name === collectionName);
      const collectionType = columns[collectionIndex]?.column_type;
      if (collectionType?.type !== "Array" || collectionType.element.type !== "Row") {
        throw new Error(`native terminal operation addressed unknown collection ${collectionName}`);
      }
      columns = collectionType.element.columns;
      targetColumns = columns;
      return { Collection: collectionIndex };
    });

    const edit = operation.edit;
    if ("Insert" in edit) {
      if (!targetColumns) throw new Error("native terminal insert has no collection target");
      const id = terminalPayloadRowId(edit.Insert.key);
      return {
        root_key: operation.root_key,
        path,
        edit: {
          Insert: {
            index: edit.Insert.index,
            key: edit.Insert.key,
            row: decodeNativeTerminalRow(id, targetColumns, Uint8Array.from(edit.Insert.value)),
          },
        },
      };
    }
    if ("Update" in edit) {
      if (!targetColumns) throw new Error("native terminal update has no collection target");
      const id = terminalPayloadRowId(edit.Update.key);
      return {
        root_key: operation.root_key,
        path,
        edit: {
          Update: {
            key: edit.Update.key,
            row: decodeNativeTerminalRow(id, targetColumns, Uint8Array.from(edit.Update.value)),
          },
        },
      };
    }
    if ("Remove" in edit) {
      return {
        root_key: operation.root_key,
        path,
        edit: { Remove: edit.Remove },
      };
    }
    return {
      root_key: operation.root_key,
      path,
      edit: { Move: edit.Move },
    };
  });
}

/** Decode the leading UUID key field from Groove's ordered record-key carrier. */
function terminalPayloadRowId(encoded: readonly number[]): string {
  const bytes = Uint8Array.from(encoded);
  if (bytes.length < 17 || bytes[0] !== 10) {
    throw new Error("terminal key must begin with a UUID row key");
  }
  return formatUuid(bytes.subarray(1, 17));
}

function outputColumnsForTable(
  table: string,
  schema: WasmSchema,
  select: string[] | undefined,
  arraySubqueries: readonly QueryArraySubquery[],
  rootTerminal = true,
): ColumnDescriptor[] {
  const tableSchema = schema[table];
  if (!tableSchema) throw new Error(`missing schema for subscription table ${table}`);
  const wildcard = select === undefined;
  const selected = select ?? tableSchema.columns.map((column) => column.name);
  const columns = selected
    .map((columnName) => {
      const declared = tableSchema.columns.find((column) => column.name === columnName);
      if (declared) return wildcard && rootTerminal ? { ...declared, sparse: true } : declared;
      const magicType = magicColumnType(columnName);
      return magicType
        ? ({ name: columnName, column_type: magicType, nullable: false } satisfies ColumnDescriptor)
        : undefined;
    })
    .filter((column): column is ColumnDescriptor => column !== undefined);

  for (const subquery of arraySubqueries) {
    const childColumns = outputColumnsForTable(
      subquery.table,
      schema,
      subquery.select,
      subquery.nestedArrays ?? [],
      false,
    );
    columns.push({
      name: subquery.columnName,
      column_type: {
        type: "Array",
        element: { type: "Row", columns: childColumns },
      },
      nullable: false,
    });
  }

  return columns;
}

function addNestedOuterColumns(queryJson: string): string {
  const parsed = JSON.parse(queryJson) as { array_subqueries?: unknown };
  addNestedOuterColumnsToSubqueries(parsed.array_subqueries);
  return JSON.stringify(parsed);
}

function addNestedOuterColumnsToSubqueries(subqueries: unknown): void {
  if (!Array.isArray(subqueries)) return;
  for (const entry of subqueries) {
    if (!entry || typeof entry !== "object") continue;
    const record = entry as {
      select_columns?: unknown;
      nested_arrays?: unknown;
    };
    if (Array.isArray(record.select_columns) && Array.isArray(record.nested_arrays)) {
      for (const nested of record.nested_arrays) {
        if (!nested || typeof nested !== "object") continue;
        const outerColumn = (nested as { outer_column?: unknown }).outer_column;
        if (typeof outerColumn !== "string") continue;
        const column = outerColumn.split(".").at(-1) ?? outerColumn;
        if (!(readSelectColumns(record.select_columns) ?? []).includes(column)) {
          record.select_columns.push(column);
        }
      }
    }
    addNestedOuterColumnsToSubqueries(record.nested_arrays);
  }
}

function sleep(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

function isPendingCoverageError(error: unknown): boolean {
  const message = errorMessage(error);
  return (
    message.includes("NotCovered") ||
    message.includes("not covered") ||
    message.includes("has not reached requested tier")
  );
}

function rejectedWaitError(
  transactionId: TxId,
  error: unknown,
): {
  kind: "rejected";
  transactionId: TxId;
  code: string;
  reason: string;
  /** An Error-compatible diagnostic for direct native callers. */
  message: string;
} | null {
  const message = errorMessage(error);
  if (extractWriteRejectedReason(message) === null) return null;
  return queuedWriteRejection(transactionId, error);
}

function queuedWriteRejection(
  transactionId: TxId,
  error: unknown,
): {
  kind: "rejected";
  transactionId: TxId;
  code: string;
  reason: string;
  /** An Error-compatible diagnostic for direct native callers. */
  message: string;
} {
  const message = errorMessage(error);
  const rejection: {
    kind: "rejected";
    transactionId: TxId;
    code: string;
    reason: string;
    message: string;
  } = {
    kind: "rejected",
    transactionId,
    code: rejectionCode(message),
    reason: rejectionReason(message),
    message,
  };
  // Worker transport intentionally carries only the enumerable structured
  // fields. Native callers also inspect rejected promises like Errors, so
  // retain Rust's diagnostic without widening that transport payload.
  Object.defineProperty(rejection, "message", { enumerable: false });
  return rejection;
}

function writeOrNormalizeRejection<T>(
  operation: "Insert" | "Restore" | "Update" | "Upsert" | "Delete",
  write: () => T,
): T {
  try {
    return write();
  } catch (error) {
    const message = errorMessage(error);
    const reason = extractWriteRejectedReason(message);
    if (reason !== null) {
      throw new Error(`${operation} failed: WriteError("${reason}")`);
    }
    throw error;
  }
}

function writeError(
  operation: "Insert" | "Restore" | "Update" | "Upsert" | "Delete",
  reason: string,
): Error {
  return new Error(`${operation} failed: WriteError("${reason.replaceAll('"', '\\"')}")`);
}

function normalizeWriteSetupMessage(message: string): string {
  const missingRequiredColumn = /^missing required column ([A-Za-z_$][\w$]*)$/.exec(message);
  if (missingRequiredColumn) {
    return `missing required field \`${missingRequiredColumn[1]}\``;
  }
  return message;
}

function errorMessage(error: unknown): string {
  if (error instanceof Error) return error.message;
  if (error && typeof error === "object") {
    const message = (error as { message?: unknown }).message;
    if (typeof message === "string" && message.trim()) return message;
    try {
      return JSON.stringify(error);
    } catch {
      return Object.prototype.toString.call(error);
    }
  }
  return String(error);
}

function contextualError(context: string, error: unknown): Error {
  const cause = error instanceof Error ? error : new Error(errorMessage(error));
  return new Error(`${context}: ${cause.message}`, { cause });
}

function rejectionCode(message: string): string {
  if (message.includes("AuthorizationDenied")) return "permission_denied";
  if (message.includes("ExclusiveConflict")) return "exclusive_conflict";
  if (message.includes("CausalityViolation")) return "causality_violation";
  if (message.includes("ClientClockTooFarAhead")) return "client_clock_too_far_ahead";
  if (message.includes("Cascade")) return "cascade_rejected";
  return "write_rejected";
}

function rejectionReason(message: string): string {
  const reason = extractWriteRejectedReason(message);
  if (reason === null) return message;
  if (reason.includes("AuthorizationDenied")) return "Write rejected by server authorization";
  return reason || "Write rejected";
}

/** Parse the exact stable Rust `Error` display prefix without matching quoted diagnostics. */
function extractWriteRejectedReason(message: string): string | null {
  const match = /^WriteRejected:\s*(.*)$/s.exec(message);
  return match ? match[1]! : null;
}

function encodeQueryJson(queryJson: string, schema: WasmSchema): Uint8Array {
  const parsed = JSON.parse(queryJson) as {
    array_subqueries?: unknown;
    conditions?: unknown;
    table?: unknown;
    limit?: unknown;
    relation_ir?: unknown;
    offset?: unknown;
    order_by?: unknown;
    orderBy?: unknown;
    select?: unknown;
    select_columns?: unknown;
  };
  if (typeof parsed.table !== "string") {
    throw new Error("Native runtime only supports table queries in this slice");
  }
  const encoded = encodeSimpleRelationQuery(parsed.table, parsed, schema);
  return queryWithPredicates(parsed.table, encoded.predicates, {
    limit: readLimitIfPresent(parsed.limit ?? encoded.limit),
    offset: readOffset(parsed.offset ?? encoded.offset),
    orderBy: encoded.orderBy.concat(readRootOrderBy(parsed.order_by ?? parsed.orderBy)),
    select: readSelectColumns(parsed.select_columns ?? parsed.select ?? encoded.select),
    arraySubqueries: readQueryArraySubqueries(parsed.array_subqueries, parsed.table, schema),
  });
}

function unsupportedQueryEncodingError(context?: string): Error {
  const suffix = context ? ` (${context})` : "";
  return new Error(`Native runtime cannot encode this query shape${suffix}.`);
}

function unsupportedRelationQueryError(operator?: string): Error {
  const detail = operator
    ? ` Relation IR operator "${operator}" requires a relation-tree lowerer or native relation query API; the TS native runtime can currently lower only TableScan plus Filter/Project/OrderBy/Offset/Limit into flat native predicates.`
    : " The TS native runtime can currently lower only TableScan plus Filter/Project/OrderBy/Offset/Limit into flat native predicates.";
  return new Error(`Native runtime cannot lower this relation IR.${detail}`);
}

function encodeSimpleRelationQuery(
  table: string,
  query: {
    conditions?: unknown;
    relation_ir?: unknown;
    limit?: unknown;
    offset?: unknown;
  },
  schema: WasmSchema,
): {
  predicates: QueryPredicate[];
  limit?: number;
  offset: number;
  orderBy: QueryOrder[];
  select?: string[];
} {
  const unwrapped = unwrapSimpleQuery(table, query);
  if (!unwrapped) throw unsupportedRelationQueryError(relationOperator(query.relation_ir));
  const rootPredicates = readFlatConditions(query.conditions);
  if (!rootPredicates) throw unsupportedQueryEncodingError();
  return {
    limit: unwrapped.limit,
    offset: unwrapped.offset,
    orderBy: unwrapped.orderBy,
    select: unwrapped.select,
    predicates: unwrapped.predicates
      .concat(rootPredicates)
      .map((filter) => coerceQueryPredicate(table, filter, schema)),
  };
}

function relationOperator(value: unknown): string | undefined {
  if (!value || typeof value !== "object") return undefined;
  const record = value as Record<string, unknown>;
  for (const operator of ["Join", "Project", "Gather", "Union"]) {
    if (operator in record) return operator;
  }
  for (const operator of ["Limit", "Offset", "OrderBy", "Filter"]) {
    const child = record[operator];
    if (child && typeof child === "object") {
      const input = (child as { input?: unknown }).input;
      const nested = relationOperator(input);
      if (nested) return nested;
    }
  }
  return undefined;
}

function unwrapSimpleQuery(
  table: string,
  query: {
    relation_ir?: unknown;
  },
): {
  predicates: QueryPredicate[];
  limit?: number;
  offset: number;
  orderBy: QueryOrder[];
  select?: string[];
} | null {
  if (query.relation_ir == null) return { predicates: [], offset: 0, orderBy: [] };
  return unwrapSimpleRelation(table, query.relation_ir);
}

function unwrapSimpleRelation(
  table: string,
  relationIr: unknown,
): {
  predicates: QueryPredicate[];
  limit?: number;
  offset: number;
  orderBy: QueryOrder[];
  select?: string[];
} | null {
  if (relationIr == null) return { predicates: [], offset: 0, orderBy: [] };
  if (typeof relationIr !== "object") return null;
  const relation = relationIr as Record<string, unknown>;
  const tableScan = relation.TableScan;
  if (
    tableScan &&
    typeof tableScan === "object" &&
    (tableScan as { table?: unknown }).table === table
  ) {
    return { predicates: [], offset: 0, orderBy: [] };
  }
  const limit = relation.Limit;
  if (limit && typeof limit === "object") {
    const limitRecord = limit as { input?: unknown; limit?: unknown };
    const input = unwrapSimpleRelation(table, limitRecord.input);
    if (!input) return null;
    return { ...input, limit: readLimit(limitRecord.limit) };
  }
  const offset = relation.Offset;
  if (offset && typeof offset === "object") {
    const offsetRecord = offset as { input?: unknown; offset?: unknown };
    const input = unwrapSimpleRelation(table, offsetRecord.input);
    if (!input) return null;
    return { ...input, offset: readOffset(offsetRecord.offset) };
  }
  const orderBy = relation.OrderBy;
  if (orderBy && typeof orderBy === "object") {
    const orderByRecord = orderBy as { input?: unknown; terms?: unknown };
    const input = unwrapSimpleRelation(table, orderByRecord.input);
    const terms = readOrderByTerms(orderByRecord.terms);
    if (!input || !terms) return null;
    return { ...input, orderBy: input.orderBy.concat(terms) };
  }
  const project = relation.Project;
  if (project && typeof project === "object") {
    const projectRecord = project as { input?: unknown; columns?: unknown };
    const input = unwrapSimpleRelation(table, projectRecord.input);
    const columns = readProjectColumns(projectRecord.columns);
    if (!input || !columns) return null;
    return { ...input, select: columns };
  }
  const filter = relation.Filter;
  if (!filter || typeof filter !== "object") return null;
  const filterRecord = filter as { input?: unknown; predicate?: unknown };
  const input = unwrapSimpleRelation(table, filterRecord.input);
  if (!input) return null;
  const predicates = predicateToFilters(filterRecord.predicate);
  return predicates ? { ...input, predicates: input.predicates.concat(predicates) } : null;
}

function readProjectColumns(value: unknown): string[] | null {
  if (!Array.isArray(value)) return null;
  const columns: string[] = [];
  for (const entry of value) {
    if (!entry || typeof entry !== "object") return null;
    const record = entry as { alias?: unknown; expr?: unknown; source?: unknown };
    const expr = record.expr ?? record.source;
    if (!expr || typeof expr !== "object") return null;
    const column = readColumnProjectExpr(expr);
    if (!column) return null;
    if (record.alias != null && record.alias !== column) return null;
    columns.push(column);
  }
  return columns;
}

function readColumnProjectExpr(value: unknown): string | null {
  if (!value || typeof value !== "object") return null;
  const record = value as { Column?: unknown; column?: unknown };
  if (record.Column != null) return readColumnRef(record.Column);
  if (record.column != null) return readColumnRef(record);
  return null;
}

function coerceQueryPredicate(
  table: string,
  filter: QueryPredicate,
  schema: WasmSchema,
): QueryPredicate {
  if (filter.op === "All" || filter.op === "Any") {
    return {
      op: filter.op,
      predicates: filter.predicates.map((predicate) =>
        coerceQueryPredicate(table, predicate, schema),
      ),
    };
  }
  if (filter.op === "Not") {
    return { op: "Not", predicate: coerceQueryPredicate(table, filter.predicate, schema) };
  }
  if (filter.op === "In") {
    const columnType =
      filter.column === "id"
        ? ({ type: "Uuid" } as const)
        : schema[table]?.columns.find((entry) => entry.name === filter.column)?.column_type;
    if (columnType?.type === "Array") {
      return {
        op: "Any",
        predicates: filter.values.map((value) =>
          value.type === "Array"
            ? {
                column: filter.column,
                op: "Eq",
                value: coerceQueryLiteral(table, filter.column, value, schema),
              }
            : {
                column: filter.column,
                op: "Contains",
                value: coerceLiteralForColumnType(value, columnType.element, false),
              },
        ),
      };
    }
    return {
      ...filter,
      values: filter.values.map((value) => coerceQueryLiteral(table, filter.column, value, schema)),
    };
  }
  if (filter.op === "Contains") {
    const columnType =
      filter.column === "id"
        ? ({ type: "Uuid" } as const)
        : schema[table]?.columns.find((entry) => entry.name === filter.column)?.column_type;
    return {
      ...filter,
      value:
        columnType?.type === "Array"
          ? coerceLiteralForColumnType(filter.value, columnType.element, false)
          : coerceQueryLiteral(table, filter.column, filter.value, schema),
    };
  }
  if (filter.op === "EnumMatch") {
    const columnType = schema[table]?.columns.find(
      (entry) => entry.name === filter.column,
    )?.column_type;
    if (columnType?.type !== "EnumPayload") {
      throw new Error(`enum match requires payload enum column "${filter.column}"`);
    }
    const entry = columnType.cases.find((candidate) => candidate.name === filter.case);
    if (!entry) {
      throw new Error(`unknown payload enum case "${filter.case}"`);
    }
    return {
      ...filter,
      payload: coerceEnumPayloadPredicate(filter.payload, entry.fields),
    };
  }
  if (filter.op === "IsNull" || filter.op === "IsNotNull") return filter;
  if (isQueryPredicateCmp(filter)) {
    return {
      ...filter,
      value: coerceQueryLiteral(table, filter.column, filter.value, schema),
    };
  }
  throw new Error(`unsupported query predicate ${JSON.stringify(filter)}`);
}

function coerceEnumPayloadPredicate(
  predicate: QueryPredicate,
  fields: ColumnDescriptor[],
): QueryPredicate {
  if (predicate.op === "All" || predicate.op === "Any") {
    return {
      ...predicate,
      predicates: predicate.predicates.map((child) => coerceEnumPayloadPredicate(child, fields)),
    };
  }
  if (predicate.op === "Not") {
    return { ...predicate, predicate: coerceEnumPayloadPredicate(predicate.predicate, fields) };
  }
  if (predicate.op === "EnumMatch") {
    throw new Error("payload enum matches cannot be nested");
  }
  if (!("column" in predicate)) {
    throw new Error("payload enum predicate must target a payload field");
  }
  const field = fields.find((candidate) => candidate.name === predicate.column);
  if (!field) {
    throw new Error(`unknown payload enum field "${predicate.column}"`);
  }
  if (predicate.op === "In") {
    return {
      ...predicate,
      values: predicate.values.map((value) =>
        coerceLiteralForColumnType(value, field.column_type, field.nullable),
      ),
    };
  }
  if (predicate.op === "Contains" || isQueryPredicateCmp(predicate)) {
    return {
      ...predicate,
      value: coerceLiteralForColumnType(predicate.value, field.column_type, field.nullable),
    };
  }
  return predicate;
}

function isQueryPredicateCmp(
  predicate: QueryPredicate,
): predicate is Extract<QueryPredicate, { op: QueryPredicateOp }> {
  return (
    predicate.op === "Eq" ||
    predicate.op === "Ne" ||
    predicate.op === "Gt" ||
    predicate.op === "Gte" ||
    predicate.op === "Lt" ||
    predicate.op === "Lte"
  );
}

function readSelectColumns(value: unknown): string[] | undefined {
  if (value == null) return undefined;
  if (!Array.isArray(value)) throw unsupportedQueryEncodingError();
  const columns: string[] = [];
  for (const entry of value) {
    if (typeof entry === "string") {
      columns.push(entry);
      continue;
    }
    if (
      !entry ||
      typeof entry !== "object" ||
      typeof (entry as { column?: unknown }).column !== "string"
    ) {
      throw unsupportedQueryEncodingError();
    }
    const projection = entry as { kind?: unknown; column: string };
    if (
      projection.kind !== "full" &&
      projection.kind !== "bytes" &&
      projection.kind !== "text_utf16" &&
      projection.kind !== "text_utf8" &&
      projection.kind !== "json_pointer"
    ) {
      throw unsupportedQueryEncodingError();
    }
    columns.push(projection.column);
  }
  return columns;
}

function readRootOrderBy(value: unknown): QueryOrder[] {
  if (value == null) return [];
  if (!Array.isArray(value)) throw unsupportedQueryEncodingError("order_by");
  return value.map((entry) => {
    if (Array.isArray(entry) && entry.length === 2 && typeof entry[0] === "string") {
      if (entry[1] !== "asc" && entry[1] !== "desc") {
        throw unsupportedQueryEncodingError("order_by");
      }
      return { column: entry[0], direction: entry[1] === "asc" ? "Asc" : "Desc" };
    }
    if (!entry || typeof entry !== "object") {
      throw unsupportedQueryEncodingError("order_by");
    }
    const record = entry as { column?: unknown; direction?: unknown };
    if (typeof record.column !== "string") {
      throw unsupportedQueryEncodingError("order_by");
    }
    if (record.direction !== "Asc" && record.direction !== "Desc") {
      throw unsupportedQueryEncodingError("order_by");
    }
    return { column: record.column, direction: record.direction };
  });
}

function readQueryArraySubqueries(
  value: unknown,
  parentTable: string,
  schema: WasmSchema,
): QueryArraySubquery[] | undefined {
  if (value == null) return undefined;
  if (!Array.isArray(value)) throw unsupportedQueryEncodingError("array_subqueries");
  return value.map((entry) => readQueryArraySubquery(entry, parentTable, schema));
}

function readQueryArraySubquery(
  value: unknown,
  parentTable: string,
  schema: WasmSchema,
): QueryArraySubquery {
  if (!value || typeof value !== "object") throw unsupportedQueryEncodingError("array_subqueries");
  const record = value as {
    column_name?: unknown;
    table?: unknown;
    inner_column?: unknown;
    outer_column?: unknown;
    filters?: unknown;
    joins?: unknown;
    select_columns?: unknown;
    order_by?: unknown;
    limit?: unknown;
    offset?: unknown;
    requirement?: unknown;
    nested_arrays?: unknown;
  };
  if (
    typeof record.column_name !== "string" ||
    typeof record.table !== "string" ||
    typeof record.inner_column !== "string" ||
    typeof record.outer_column !== "string"
  ) {
    throw unsupportedQueryEncodingError("array_subqueries");
  }
  if (Array.isArray(record.joins) && record.joins.length > 0) {
    throw unsupportedQueryEncodingError("array_subqueries.joins");
  }
  const filters = readArraySubqueryFilters(record.filters, record.table, schema);
  const select = readSelectColumns(record.select_columns);
  const orderBy = readArraySubqueryOrder(record.order_by);
  const nestedArrays = readQueryArraySubqueries(record.nested_arrays, record.table, schema) ?? [];
  return {
    columnName: record.column_name,
    table: record.table,
    innerColumn: record.inner_column,
    outerColumn: stripParentQualifier(record.outer_column, parentTable),
    filters,
    select,
    orderBy,
    limit: record.limit == null ? null : readLimit(record.limit),
    offset: readOffset(record.offset),
    requirement: readArraySubqueryRequirement(record.requirement),
    nestedArrays,
  };
}

function readArraySubqueryFilters(
  value: unknown,
  table: string,
  schema: WasmSchema,
): QueryPredicate[] {
  if (value == null) return [];
  if (!Array.isArray(value)) throw unsupportedQueryEncodingError("array_subqueries.filters");
  const filters: QueryPredicate[] = [];
  for (const entry of value) {
    const next = arraySubqueryFilterToPredicates(entry);
    if (!next) throw unsupportedQueryEncodingError("array_subqueries.filters");
    filters.push(...next.map((filter) => coerceQueryPredicate(table, filter, schema)));
  }
  return filters;
}

function arraySubqueryFilterToPredicates(value: unknown): QueryPredicate[] | null {
  const canonical = predicateToFilterTree(value);
  if (canonical) return [canonical];
  if (!value || typeof value !== "object") return null;
  const record = value as Record<string, unknown>;
  for (const [key, op] of [
    ["Eq", "Eq"],
    ["Ne", "Ne"],
    ["Gt", "Gt"],
    ["Ge", "Gte"],
    ["Lt", "Lt"],
    ["Le", "Lte"],
  ] as const) {
    const entry = record[key];
    if (entry && typeof entry === "object") {
      const { column, value: literal } = entry as { column?: unknown; value?: unknown };
      return typeof column === "string"
        ? [{ column, op, value: valueToQueryLiteral(literal) }]
        : null;
    }
  }
  const isNull = record.IsNull;
  if (isNull && typeof isNull === "object") {
    const column = (isNull as { column?: unknown }).column;
    return typeof column === "string" ? [{ column, op: "IsNull" }] : null;
  }
  const isNotNull = record.IsNotNull;
  if (isNotNull && typeof isNotNull === "object") {
    const column = (isNotNull as { column?: unknown }).column;
    return typeof column === "string" ? [{ column, op: "IsNotNull" }] : null;
  }
  const contains = record.Contains;
  if (contains && typeof contains === "object") {
    const { column, value: literal } = contains as { column?: unknown; value?: unknown };
    return typeof column === "string"
      ? [{ column, op: "Contains", value: valueToQueryLiteral(literal) }]
      : null;
  }
  return null;
}

function readArraySubqueryOrder(value: unknown): QueryOrder[] {
  if (value == null) return [];
  if (!Array.isArray(value)) throw unsupportedQueryEncodingError("array_subqueries.order_by");
  return value.map((entry) => {
    if (!Array.isArray(entry) || entry.length !== 2 || typeof entry[0] !== "string") {
      throw unsupportedQueryEncodingError("array_subqueries.order_by");
    }
    if (entry[1] !== "Ascending" && entry[1] !== "Descending") {
      throw unsupportedQueryEncodingError("array_subqueries.order_by");
    }
    return { column: entry[0], direction: entry[1] === "Ascending" ? "Asc" : "Desc" };
  });
}

function readArraySubqueryRequirement(value: unknown): QueryArraySubquery["requirement"] {
  if (value == null || value === "Optional") return "Optional";
  if (value === "AtLeastOne" || value === "MatchCorrelationCardinality") return value;
  throw unsupportedQueryEncodingError("array_subqueries.requirement");
}

function stripParentQualifier(column: string, parentTable: string): string {
  const prefix = `${parentTable}.`;
  return column.startsWith(prefix) ? column.slice(prefix.length) : column;
}

function readOrderByTerms(value: unknown): QueryOrder[] | null {
  if (!Array.isArray(value)) return null;
  const terms: QueryOrder[] = [];
  for (const term of value) {
    if (!term || typeof term !== "object") return null;
    const record = term as { column?: unknown; direction?: unknown };
    const column = readColumnRef(record.column);
    if (!column || (record.direction !== "Asc" && record.direction !== "Desc")) return null;
    terms.push({ column, direction: record.direction });
  }
  return terms;
}

function coerceQueryLiteral(
  table: string,
  column: string,
  value: QueryLiteral,
  schema: WasmSchema,
): QueryLiteral {
  const columnType =
    column === "id"
      ? ({ type: "Uuid" } as const)
      : (magicColumnType(column) ??
        schema[table]?.columns.find((entry) => entry.name === column)?.column_type);
  if (columnType?.type === "Bytea" && value.type === "Array") {
    return { type: "Bytea", value: Uint8Array.from(value.value.map(readByteLiteral)) };
  }
  if (value.type === "Array") {
    const elementType =
      column === "id"
        ? { type: "Uuid" as const }
        : schema[table]?.columns.find((entry) => entry.name === column)?.column_type;
    const elementColumnType = elementType?.type === "Array" ? elementType.element : elementType;
    return {
      type: "Array",
      value: value.value.map((entry) =>
        coerceLiteralForColumnType(entry, elementColumnType, false),
      ),
    };
  }
  const coerced = coerceLiteralForColumnType(value, columnType, true);
  return coerced;
}

function coerceLiteralForColumnType(
  value: QueryLiteral,
  columnType: ColumnType | undefined,
  allowNullable: boolean,
): QueryLiteral {
  if (value.type === "Nullable") {
    return allowNullable && value.value
      ? { type: "Nullable", value: coerceLiteralForColumnType(value.value, columnType, false) }
      : value;
  }
  if (columnType?.type === "Uuid" && value.type === "Text" && isUuidString(value.value)) {
    return { type: "Uuid", value: value.value };
  }
  if (columnType?.type === "Text" && value.type === "Uuid") {
    return { type: "Text", value: value.value };
  }
  if (columnType?.type === "Double" && value.type === "Integer") {
    return { type: "Double", value: value.value };
  }
  if (columnType?.type === "BigInt" && value.type === "Integer") {
    return { type: "BigInt", value: exactSignedI64(value.value, "BigInt value") };
  }
  if (columnType?.type === "Timestamp" && value.type === "Integer") {
    return { type: "Timestamp", value: value.value };
  }
  if (columnType?.type === "Timestamp" && value.type === "Text") {
    const time = Date.parse(value.value);
    if (Number.isFinite(time)) {
      return { type: "Timestamp", value: time };
    }
  }
  if (columnType?.type === "Bytea" && value.type === "Array") {
    return { type: "Bytea", value: Uint8Array.from(value.value.map(readByteLiteral)) };
  }
  if (columnType?.type === "Array" && value.type === "Array") {
    return {
      type: "Array",
      value: value.value.map((entry) =>
        coerceLiteralForColumnType(entry, columnType.element, false),
      ),
    };
  }
  return value;
}

function readByteLiteral(value: QueryLiteral): number {
  if (value.type !== "Integer" || value.value < 0 || value.value > 255) {
    throw new Error("Bytea values must contain integers in range 0..255");
  }
  return value.value;
}

function readFlatConditions(conditions: unknown): QueryPredicate[] | null {
  if (conditions == null) return [];
  if (!Array.isArray(conditions)) return null;
  const predicates: QueryPredicate[] = [];
  for (const condition of conditions) {
    if (!condition || typeof condition !== "object") return null;
    const canonical = predicateToFilterTree(condition);
    if (canonical) {
      predicates.push(canonical);
      continue;
    }
    const record = condition as { column?: unknown; op?: unknown; value?: unknown };
    if (typeof record.column !== "string" || typeof record.op !== "string") return null;
    const column = record.column.split(".").at(-1) ?? record.column;
    switch (record.op) {
      case "eq":
        if (record.value === null) {
          predicates.push({ column, op: "IsNull" });
        } else {
          predicates.push({ column, op: "Eq", value: valueToQueryLiteral(record.value) });
        }
        break;
      case "ne":
        if (record.value === null) {
          predicates.push({ column, op: "IsNotNull" });
        } else {
          predicates.push({ column, op: "Ne", value: valueToQueryLiteral(record.value) });
        }
        break;
      case "gt":
        predicates.push({ column, op: "Gt", value: valueToQueryLiteral(record.value) });
        break;
      case "gte":
        predicates.push({ column, op: "Gte", value: valueToQueryLiteral(record.value) });
        break;
      case "lt":
        predicates.push({ column, op: "Lt", value: valueToQueryLiteral(record.value) });
        break;
      case "lte":
        predicates.push({ column, op: "Lte", value: valueToQueryLiteral(record.value) });
        break;
      case "contains":
        predicates.push({ column, op: "Contains", value: valueToQueryLiteral(record.value) });
        break;
      case "isNull":
        if (typeof record.value !== "boolean") return null;
        predicates.push({ column, op: record.value ? "IsNull" : "IsNotNull" });
        break;
      case "in":
        if (!Array.isArray(record.value)) return null;
        predicates.push({
          column,
          op: "In",
          values: record.value.map(valueToQueryLiteral),
        });
        break;
      default:
        return null;
    }
  }
  return predicates;
}

function predicateToFilters(predicate: unknown): QueryPredicate[] | null {
  if (predicate === "True") return [];
  if (predicate === "False") return [{ column: "id", op: "In", values: [] }];
  if (!predicate || typeof predicate !== "object") return null;
  const record = predicate as Record<string, unknown>;
  if (Array.isArray(record.And)) {
    const filters: QueryPredicate[] = [];
    for (const child of record.And) {
      const childFilters = predicateToFilters(child);
      if (!childFilters) return null;
      filters.push(...childFilters);
    }
    return filters;
  }
  if (Array.isArray(record.Or)) return null;
  if (record.Not) {
    const predicate = predicateToFilterTree(record.Not);
    return predicate ? [{ op: "Not", predicate }] : null;
  }
  const enumMatch = record.EnumMatch;
  if (enumMatch && typeof enumMatch === "object") {
    const match = enumMatch as { column?: unknown; case?: unknown; payload?: unknown };
    const column = readColumnRef(match.column);
    const payload = predicateToFilterTree(match.payload);
    return column && typeof match.case === "string" && payload
      ? [{ column, op: "EnumMatch", case: match.case, payload }]
      : null;
  }
  const isNull = record.IsNull;
  if (isNull && typeof isNull === "object") {
    const column = readColumnRef((isNull as { column?: unknown }).column);
    return column ? [{ column, op: "IsNull" }] : null;
  }
  const isNotNull = record.IsNotNull;
  if (isNotNull && typeof isNotNull === "object") {
    const column = readColumnRef((isNotNull as { column?: unknown }).column);
    return column ? [{ column, op: "IsNotNull" }] : null;
  }
  const contains = record.Contains;
  if (contains && typeof contains === "object") {
    const containsRecord = contains as { left?: unknown; right?: unknown };
    const column = readColumnRef(containsRecord.left);
    const value = readLiteral(containsRecord.right);
    return column && value ? [{ column, op: "Contains", value }] : null;
  }
  const inPredicate = record.In;
  if (inPredicate && typeof inPredicate === "object") {
    const inRecord = inPredicate as { left?: unknown; values?: unknown };
    const column = readColumnRef(inRecord.left);
    if (!column || !Array.isArray(inRecord.values)) return null;
    const values = inRecord.values.map(readLiteral);
    return values.every((value): value is QueryLiteral => value != null)
      ? [{ column, op: "In", values }]
      : null;
  }
  const cmp = record.Cmp;
  if (!cmp || typeof cmp !== "object") return null;
  const cmpRecord = cmp as { left?: unknown; op?: unknown; right?: unknown };
  const op = readPredicateOp(cmpRecord.op);
  if (!op) return null;
  const column = readColumnRef(cmpRecord.left);
  const value = readLiteral(cmpRecord.right);
  return column && value ? [{ column, op, value }] : null;
}

function predicateToFilterTree(predicate: unknown): QueryPredicate | null {
  if (predicate === "True") return { op: "All", predicates: [] };
  if (predicate === "False") return { op: "Any", predicates: [] };
  if (!predicate || typeof predicate !== "object") return null;
  const record = predicate as Record<string, unknown>;
  if (Array.isArray(record.And) || Array.isArray(record.Or)) {
    const op = Array.isArray(record.And) ? "All" : "Any";
    const children = (record.And ?? record.Or) as unknown[];
    const predicates = children.map(predicateToFilterTree);
    return predicates.every((child): child is QueryPredicate => child !== null)
      ? { op, predicates }
      : null;
  }
  if (record.Not) {
    const predicate = predicateToFilterTree(record.Not);
    return predicate ? { op: "Not", predicate } : null;
  }
  const filters = predicateToFilters(predicate);
  return filters?.length === 1 ? filters[0]! : null;
}

function valueToQueryLiteral(value: unknown): QueryLiteral {
  if (value === null || value === undefined) return { type: "Nullable", value: null };
  if (typeof value === "boolean") return { type: "Boolean", value };
  if (typeof value === "number" && Number.isSafeInteger(value)) return { type: "Integer", value };
  if (typeof value === "number" && Number.isFinite(value)) return { type: "Double", value };
  if (typeof value === "bigint") return { type: "BigInt", value };
  if (typeof value === "string")
    return isUuidString(value) ? { type: "Uuid", value } : { type: "Text", value };
  if (value instanceof Uint8Array) return { type: "Bytea", value };
  if (Array.isArray(value)) return { type: "Array", value: value.map(valueToQueryLiteral) };
  throw unsupportedQueryEncodingError();
}

function readPredicateOp(value: unknown): QueryPredicateOp | null {
  switch (value) {
    case "Eq":
    case "Ne":
    case "Gt":
    case "Gte":
    case "Lt":
    case "Lte":
      return value;
    case "Ge":
      return "Gte";
    case "Le":
      return "Lte";
    default:
      return null;
  }
}

function readColumnRef(value: unknown): string | null {
  if (!value || typeof value !== "object") return null;
  const column = (value as { column?: unknown }).column;
  if (typeof column !== "string") return null;
  return column.split(".").at(-1) ?? column;
}

function readLiteral(value: unknown): QueryLiteral | null {
  if (!value || typeof value !== "object" || !("Literal" in value)) return null;
  const literal = (value as { Literal?: unknown }).Literal;
  if (!literal || typeof literal !== "object") return null;
  const record = literal as { type?: unknown; value?: unknown };
  if (record.type === "Boolean" && typeof record.value === "boolean") {
    return { type: "Boolean", value: record.value };
  }
  if (
    record.type === "Integer" &&
    typeof record.value === "number" &&
    Number.isSafeInteger(record.value)
  ) {
    return { type: "Integer", value: record.value };
  }
  if (
    record.type === "BigInt" &&
    (typeof record.value === "bigint" ||
      (typeof record.value === "number" && Number.isSafeInteger(record.value)))
  ) {
    return { type: "BigInt", value: BigInt(record.value) };
  }
  if (
    record.type === "Timestamp" &&
    typeof record.value === "number" &&
    Number.isSafeInteger(record.value)
  ) {
    return { type: "Timestamp", value: record.value };
  }
  if (
    record.type === "Double" &&
    typeof record.value === "number" &&
    Number.isFinite(record.value)
  ) {
    return { type: "Double", value: record.value };
  }
  if (record.type === "Bytea" && Array.isArray(record.value)) {
    return { type: "Bytea", value: Uint8Array.from(record.value.map(Number)) };
  }
  if (record.type === "Null") {
    return { type: "Nullable", value: null };
  }
  if (record.type === "Array" && Array.isArray(record.value)) {
    const values = record.value.map((entry) => readLiteral({ Literal: entry }));
    if (values.every((entry): entry is QueryLiteral => entry != null)) {
      return { type: "Array", value: values };
    }
  }
  if (record.type === "Uuid" && typeof record.value === "string") {
    return { type: "Uuid", value: record.value };
  }
  if ((record.type === "Text" || record.type === "Enum") && typeof record.value === "string") {
    return { type: "Text", value: record.value };
  }
  return null;
}

function readLimit(value: unknown): number {
  if (typeof value !== "number" || !Number.isSafeInteger(value) || value < 0) {
    throw new Error("query limit must be a non-negative safe integer");
  }
  return value;
}

function readLimitIfPresent(value: unknown): number | undefined {
  return value == null ? undefined : readLimit(value);
}

function readOffset(value: unknown): number {
  if (value == null) return 0;
  if (typeof value !== "number" || !Number.isSafeInteger(value) || value < 0) {
    throw new Error("query offset must be a non-negative safe integer");
  }
  return value;
}

function isUuidString(value: string): boolean {
  return /^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$/.test(
    value,
  );
}

export function encodeCellsForRow(
  definition: { columns: ColumnDescriptor[]; policies?: TablePolicies },
  row: InsertValues,
  table?: string,
): Uint8Array {
  assertRequiredRowColumnsPresent(definition.columns, row, table);
  const columns = definition.columns.filter(
    (column) =>
      Object.hasOwn(row, column.name) ||
      (column.column_type.type === "Array" && column.default == null),
  );
  return encodeCells(columns, (column) => row[column.name], true);
}

function encodeCellsForStreamingRow(
  definition: { columns: ColumnDescriptor[]; policies?: TablePolicies },
  row: InsertValues,
  streamedColumn: string,
  table?: string,
): Uint8Array {
  assertRequiredRowColumnsPresent(
    definition.columns.filter((column) => column.name !== streamedColumn),
    row,
    table,
  );
  const columns = definition.columns.filter(
    (column) =>
      column.name !== streamedColumn &&
      (Object.hasOwn(row, column.name) ||
        (column.column_type.type === "Array" && column.default == null)),
  );
  return encodeCells(columns, (column) => row[column.name], true);
}

function encodeCellsForStreamingPatch(
  definition: { columns: ColumnDescriptor[]; policies?: TablePolicies },
  row: InsertValues,
  streamedColumn: string,
): Uint8Array {
  const columns = definition.columns.filter(
    (column) => column.name !== streamedColumn && Object.hasOwn(row, column.name),
  );
  return encodeCells(columns, (column) => row[column.name], false);
}

async function* streamingChunks(source: StreamingValueSource): AsyncGenerator<Uint8Array | string> {
  const readable = source as ReadableStream<Uint8Array | string>;
  if (typeof readable.getReader !== "function") {
    yield* source as AsyncIterable<Uint8Array | string>;
    return;
  }
  const reader = readable.getReader();
  let completed = false;
  try {
    while (true) {
      const result = await reader.read();
      if (result.done) {
        completed = true;
        return;
      }
      yield result.value;
    }
  } finally {
    if (!completed) await reader.cancel().catch(() => undefined);
    reader.releaseLock();
  }
}

export function encodeCellsForPatch(
  definition: { columns: ColumnDescriptor[]; policies?: TablePolicies },
  patch: Record<string, Value>,
): Uint8Array {
  const columns = definition.columns.filter((column) => Object.hasOwn(patch, column.name));
  return encodeCells(columns, (column) => patch[column.name], false);
}

function encodeCells(
  columns: ColumnDescriptor[],
  valueFor: (column: ColumnDescriptor) => Value | undefined,
  requireMissingDefaults: boolean,
): Uint8Array {
  const descriptor = [...columns]
    .sort((left, right) => left.name.localeCompare(right.name))
    .map((column) => ({ name: column.name, valueType: storageColumnValueType(column), column }));
  const values = descriptor.map(({ column }) =>
    encodeCellValue(column, valueFor(column), requireMissingDefaults),
  );
  const writer = new PostcardWriter();
  writeDescriptor(writer, descriptor);
  writer.bytes(createRecord(descriptor, values));
  return writer.finish();
}

function assertRequiredRowColumnsPresent(
  columns: ColumnDescriptor[],
  row: InsertValues,
  table?: string,
): void {
  for (const column of columns) {
    const value = row[column.name] ?? column.default;
    if (value && value.type !== "Null") continue;
    if (column.nullable || column.column_type.type === "Array") continue;
    throw new Error(
      table
        ? `encoding error: missing required field \`${column.name}\` on table \`${table}\``
        : `missing required column ${column.name}`,
    );
  }
}

/**
 * Mutation cells deliberately differ from packed rows only when a field is
 * omitted: inserts can leave a server default unresolved, whereas patches
 * never synthesize a missing field. Every present value is encoded by the row
 * codec so scalar tags, arrays, nested rows, nullable values, and sparse
 * carriers have one binary authority.
 */
function encodeCellValue(
  column: ColumnDescriptor,
  value: Value | undefined,
  requireMissingDefaults: boolean,
): Uint8Array {
  const resolved = value;
  if (!resolved) {
    if (column.nullable) return encodeNativeNullValue(storageColumnValueType(column));
    if (column.column_type.type === "Array") {
      return encodeNativeColumnValue(column, { type: "Array", value: [] });
    }
    if (requireMissingDefaults && column.default == null) {
      throw new Error(`missing required column ${column.name}`);
    }
    return new Uint8Array();
  }
  return encodeNativeColumnValue(column, resolved);
}

function readRowBatches(payload: Uint8Array): NativeRowBatch[] {
  return new PostcardReader(payload).readVec(readNativeRowBatch);
}

function readRelationSnapshot(payload: Uint8Array): NativeRelationSubscriptionSnapshot {
  return readNativeRelationSubscriptionSnapshot(new PostcardReader(payload));
}

export function rowsFromBatches(
  batches: NativeRowBatch[],
  schema: WasmSchema,
  projectedColumns?: readonly ColumnDescriptor[],
  nestedRowCarrier: NestedRowCarrier = "full-record",
): RowState[] {
  const rows: RowState[] = [];
  for (const batch of batches) {
    const fieldPlans = nativeRowFieldPlans(batch, schema, projectedColumns);
    const decodeRecord = createRecordValueDecoder(batch.descriptor);
    for (const row of batch.rows) {
      const values: Value[] = [];
      const valuesByColumn = new Map<string, Value>();

      for (const field of fieldPlans) {
        const value = decodePlannedField(field, decodeRecord, row.raw, nestedRowCarrier);
        valuesByColumn.set(field.name, value);
        if (field.includeInValues) {
          values.push(value);
        }
      }

      rows.push(
        withValuesByColumn(
          {
            table: batch.table,
            id: formatUuid(row.rowId),
            values,
          },
          valuesByColumn,
        ),
      );
    }
  }
  return rows;
}

function nativeRowFieldPlans(
  batch: NativeRowBatch,
  schema: WasmSchema,
  projectedColumns?: readonly ColumnDescriptor[],
): NativeRowFieldPlan[] {
  let cache = nativeRowFieldPlanCache.get(schema);
  if (!cache) {
    cache = new Map();
    nativeRowFieldPlanCache.set(schema, cache);
  }
  const cacheKey = nativeRowFieldPlanCacheKey(batch);
  const cached = projectedColumns ? undefined : cache.get(cacheKey);
  if (cached) return cached;

  const columns = projectedColumns ?? schema[batch.table]?.columns ?? [];
  const columnsByName = new Map(columns.map((column) => [column.name, column]));
  const projectedNames = projectedColumns
    ? new Set(projectedColumns.map((column) => column.name))
    : null;
  const plans: NativeRowFieldPlan[] = [];

  for (let index = 0; index < batch.descriptor.length; index += 1) {
    const fieldName = batch.descriptor[index]?.name;
    if (!fieldName || isInternalField(fieldName) || isCurrentRowPhysicalField(fieldName)) continue;

    const name = publicFieldName(fieldName);
    const type = magicColumnType(name) ?? columnsByName.get(name)?.column_type;
    plans.push({
      name,
      index,
      type,
      storageType: batch.descriptor[index]!.valueType,
      includeInValues:
        !isHiddenIncludeColumn(name) &&
        (!isProvenanceMagicColumn(name) || projectedNames?.has(name) === true),
    });
  }

  if (!projectedColumns) cache.set(cacheKey, plans);
  return plans;
}

// These fields are provenance retained by settled/materializer read paths.
// They are never Jazz application columns (user columns use the `user_`
// descriptor namespace) and must not cross the public native row boundary.
function isCurrentRowPhysicalField(fieldName: string): boolean {
  return (
    fieldName === "schema_version" || fieldName === "parents" || fieldName === "authored_columns"
  );
}

function rowsFromRelationSnapshot(
  snapshot: NativeRelationSubscriptionSnapshot,
  schema: WasmSchema,
  projectedColumns?: readonly ColumnDescriptor[],
  nestedRowCarrier: NestedRowCarrier = "full-record",
): RowState[] {
  const rows = stripRelationSnapshotMetadata(
    rowsFromBatches(snapshot.rows, schema, projectedColumns, nestedRowCarrier),
    schema,
  );
  return rows.slice(0, snapshot.rootCount);
}

const RELATION_SNAPSHOT_METADATA_FIELDS = new Set([
  "table",
  "layer",
  "schema_version",
  "parents",
  "created_by",
  "created_at",
  "updated_by",
  "updated_at",
]);

function stripRelationSnapshotMetadata(rows: RowState[], schema: WasmSchema): RowState[] {
  return rows.map((row) => {
    if (!row.valuesByColumn) return row;
    const schemaColumns = new Set((schema[row.table]?.columns ?? []).map((column) => column.name));
    const valuesByColumn = new Map(row.valuesByColumn);
    const metadataValues = new Set<Value>();
    for (const field of RELATION_SNAPSHOT_METADATA_FIELDS) {
      if (schemaColumns.has(field)) continue;
      const value = valuesByColumn.get(field);
      if (!value) continue;
      metadataValues.add(value);
      valuesByColumn.delete(field);
    }
    if (metadataValues.size === 0) return row;
    return withValuesByColumn(
      {
        ...row,
        values: row.values.filter((value) => !metadataValues.has(value)),
      },
      valuesByColumn,
    );
  });
}

function withValuesByColumn(row: RowState, valuesByColumn: Map<string, Value>): RowState {
  Object.defineProperty(row, "valuesByColumn", {
    value: valuesByColumn,
    enumerable: false,
    configurable: true,
  });
  return row;
}

export function applySubscriptionDeltaWithRootDelta(
  currentRows: RowState[],
  delta: NativeSubscriptionDelta,
  schema: WasmSchema,
  reset = false,
  outputColumns: SubscriptionOutputColumns | null = null,
): {
  rows: RowState[];
  rowIndexByKey: Map<string, number>;
  rootDelta: RuntimeSubscriptionDelta;
} {
  const { addedRows, updatedRows, removedEntries, rows, rowIndexByKey } =
    applySubscriptionDeltaToState(currentRows, delta, schema, reset, outputColumns);
  const rootIndexByKey = new Map<string, number>();
  addedRows.forEach((row, index) =>
    rootIndexByKey.set(rowStateKey(row), delta.addedIndices[index]!),
  );
  updatedRows.forEach((row, index) =>
    rootIndexByKey.set(rowStateKey(row), delta.updatedIndices[index]!),
  );
  return {
    rows,
    rowIndexByKey,
    rootDelta: {
      ...runtimeDeltaFromChanges(
        subscriptionOutputRows(addedRows, outputColumns),
        subscriptionOutputRows(updatedRows, outputColumns),
        subscriptionOutputRemovals(removedEntries, outputColumns),
        rootIndexByKey,
        schema,
        outputColumns,
      ),
      ...(reset ? { reset: true } : {}),
    },
  };
}

function subscriptionOutputRows(
  rows: RowState[],
  outputColumns: SubscriptionOutputColumns | null,
): RowState[] {
  return outputColumns ? rows.filter((row) => row.table === outputColumns.rootTable) : rows;
}

function subscriptionOutputRemovals(
  removed: Array<{ table: string; id: string; index: number; resultKeyBytes?: Uint8Array }>,
  outputColumns: SubscriptionOutputColumns | null,
): Array<{ id: string; index: number; resultKeyBytes?: Uint8Array }> {
  return outputColumns ? removed.filter((row) => row.table === outputColumns.rootTable) : removed;
}

function applySubscriptionDeltaToState(
  currentRows: RowState[],
  delta: NativeSubscriptionDelta,
  schema: WasmSchema,
  reset = false,
  outputColumns: SubscriptionOutputColumns | null = null,
): {
  addedRows: RowState[];
  updatedRows: RowState[];
  removedEntries: Array<{ table: string; id: string; index: number; resultKeyBytes?: Uint8Array }>;
  rows: RowState[];
  rowIndexByKey: Map<string, number>;
} {
  const rowsByKey = reset
    ? new Map<string, RowState>()
    : new Map(currentRows.map((row) => [rowStateKey(row), row]));
  const removedEntries: Array<{
    table: string;
    id: string;
    index: number;
    resultKeyBytes?: Uint8Array;
  }> = [];

  const addedRows = rowsFromSubscriptionBatches(delta.added, schema, outputColumns, "full-record");
  const updatedRows = rowsFromSubscriptionBatches(
    delta.updated,
    schema,
    outputColumns,
    "full-record",
  );
  attachOccurrenceKeys(addedRows, delta.addedOccurrenceKeys);
  attachOccurrenceKeys(updatedRows, delta.updatedOccurrenceKeys);

  for (const [removedIndex, removed] of delta.removed.entries()) {
    const id = formatUuid(removed.rowId);
    const resultKeyBytes = delta.removedOccurrenceKeys[removedIndex];
    const key = resultKeyBytes
      ? occurrenceStateKey(resultKeyBytes, removed.table, id)
      : rowKey(removed.table, id);
    removedEntries.push({
      table: removed.table,
      id,
      index: delta.removedIndices[removedIndex]!,
      resultKeyBytes,
    });
    rowsByKey.delete(key);
  }

  const changedRows = addedRows.concat(updatedRows);
  for (const row of changedRows) {
    rowsByKey.set(rowStateKey(row), row);
  }

  const changedKeys = new Set(changedRows.map((row) => rowStateKey(row)));
  const rows = (reset ? [] : currentRows).filter((row) => {
    const key = rowStateKey(row);
    return rowsByKey.has(key) && !changedKeys.has(key);
  });
  const placements = [
    ...addedRows.map((row, index) => ({ row, index: delta.addedIndices[index]! })),
    ...updatedRows.map((row, index) => ({ row, index: delta.updatedIndices[index]! })),
  ].sort((left, right) => left.index - right.index);
  for (const placement of placements) {
    rows.splice(Math.max(0, Math.min(placement.index, rows.length)), 0, placement.row);
  }
  const rowIndexByKey = indexRowsByKey(rows);
  return {
    addedRows,
    updatedRows,
    removedEntries,
    rows,
    rowIndexByKey,
  };
}

function rowsFromSubscriptionBatches(
  batches: NativeRowBatch[],
  schema: WasmSchema,
  outputColumns: SubscriptionOutputColumns | null,
  nestedRowCarrier: NestedRowCarrier,
): RowState[] {
  return batches.flatMap((batch) =>
    rowsFromBatches(
      [batch],
      schema,
      batch.table === outputColumns?.rootTable ? outputColumns.rootColumns : undefined,
      nestedRowCarrier,
    ),
  );
}

function indexRowsByKey(rows: RowState[]): Map<string, number> {
  const index = new Map<string, number>();
  rows.forEach((row, rowIndex) => {
    index.set(rowStateKey(row), rowIndex);
  });
  return index;
}

function attachOccurrenceKeys(rows: RowState[], keys: Uint8Array[]): void {
  if (rows.length !== keys.length)
    throw new Error("subscription occurrence sidecar length mismatch");
  rows.forEach((row, index) => {
    const bytes = keys[index]!;
    row.resultKeyBytes = bytes;
    row.resultKey = publicResultKey(bytes);
  });
}

function occurrenceStateKey(bytes: Uint8Array, table?: string, sourceId?: string): string {
  if (bytes.length === 17 && bytes[0] === 1 && table && sourceId) return rowKey(table, sourceId);
  return `result\0${Array.from(bytes, (byte) => byteHex[byte]).join("")}`;
}

function publicResultKey(bytes: Uint8Array): string {
  if (bytes.length === 17 && bytes[0] === 1) return formatUuid(bytes.subarray(1));
  return `result:${Array.from(bytes, (byte) => byteHex[byte]).join("")}`;
}

function rowStateKey(row: RowState): string {
  return row.resultKeyBytes
    ? occurrenceStateKey(row.resultKeyBytes, row.table, row.id)
    : rowKey(row.table, row.id);
}

function rowKey(table: string, id: string): string {
  return `${table}\0${id}`;
}

function decodePlannedField(
  field: NativeRowFieldPlan,
  decodeRecord: (raw: Uint8Array, logicalIndex: number) => Uint8Array | null,
  raw: Uint8Array,
  nestedRowCarrier: NestedRowCarrier,
): Value {
  const bytes = decodeRecord(raw, field.index);
  if (bytes == null) return { type: "Null" };
  if (!field.type) return { type: "Bytea", value: bytes };
  try {
    return decodeBytes(field.type, bytes, field.name, field.storageType, nestedRowCarrier);
  } catch (error) {
    throw new Error(
      `${String(error)} while decoding ${field.name} as ${field.type.type} from storage tag ${field.storageType.tag} (${bytes.byteLength} bytes)`,
    );
  }
}

function decodeBytes(
  type: ColumnType,
  bytes: Uint8Array,
  fieldName?: string,
  storageType?: ValueType,
  nestedRowCarrier: NestedRowCarrier = "full-record",
): Value {
  const view = new DataView(bytes.buffer, bytes.byteOffset, bytes.byteLength);
  switch (type.type) {
    case "Boolean":
      return { type: "Boolean", value: bytes[0] !== 0 };
    case "Integer":
      return { type: "Integer", value: view.getInt32(0, true) };
    case "BigInt":
      return { type: "BigInt", value: view.getBigInt64(0, true) };
    case "Double":
      return { type: "Double", value: view.getFloat64(0, true) };
    case "Timestamp":
      return {
        type: "Timestamp",
        // Current-row provenance and ordinary timestamp columns both cross the
        // public binding boundary as Unix milliseconds.
        value: Number(view.getBigUint64(0, true)),
      };
    case "Text":
    case "Json":
    case "Enum":
      if (
        fieldName !== undefined &&
        isProvenanceMagicColumn(fieldName) &&
        type.type === "Text" &&
        storageType?.tag === 8
      ) {
        return { type: "Text", value: decodeProvenanceText(bytes) };
      }
      if (bytes[0] !== 2) throw new Error("indirect scalar crossed a logical binding boundary");
      return { type: "Text", value: textDecoder.decode(bytes.subarray(1)) };
    case "EnumPayload":
      return decodePayloadEnumBytes(type, bytes, storageType, nestedRowCarrier);
    case "Uuid":
      return { type: "Uuid", value: formatUuid(bytes) };
    case "Bytea":
      if (bytes[0] !== 2) throw new Error("indirect scalar crossed a logical binding boundary");
      return { type: "Bytea", value: bytes.subarray(1).slice() };
    case "Array":
      return {
        type: "Array",
        value: decodeArrayBytes(
          type.element,
          bytes,
          arrayElementStorageType(storageType),
          nestedRowCarrier,
        ),
      };
    case "Row":
      return {
        type: "Row",
        value: decodeNestedRowBytes(
          type.columns,
          bytes,
          recordStorageDescriptor(storageType),
          nestedRowCarrier,
        ),
      };
  }
}

function decodeProvenanceText(bytes: Uint8Array): string {
  return decodeCanonicalAuthorSubjectBytes(bytes);
}

function decodePayloadEnumBytes(
  type: Extract<ColumnType, { type: "EnumPayload" }>,
  bytes: Uint8Array,
  storageType: ValueType | undefined,
  nestedRowCarrier: NestedRowCarrier,
): Value {
  if (bytes.byteLength < 4) throw new Error("invalid Enum payload value");
  const view = new DataView(bytes.buffer, bytes.byteOffset, bytes.byteLength);
  const nameLength = view.getUint32(0, true);
  if (bytes.byteLength < 4 + nameLength) throw new Error("invalid Enum payload case");
  const caseName = textDecoder.decode(bytes.subarray(4, 4 + nameLength));
  const entry = type.cases.find((candidate) => candidate.name === caseName);
  if (!entry) throw new Error("unknown Enum payload case");
  const enumStorage = nonNullableStorageType(storageType);
  const payloadDescriptor =
    enumStorage?.tag === 17
      ? enumStorage.enumSchema?.cases?.find((candidate) => candidate.name === caseName)?.payload
      : undefined;
  if (!payloadDescriptor || payloadDescriptor.length !== entry.fields.length) {
    throw new Error("Enum payload descriptor mismatch");
  }
  const raw = bytes.subarray(4 + nameLength);
  const decodeRecord = createRecordValueDecoder(payloadDescriptor);
  return {
    type: "Enum",
    value: {
      case: caseName,
      values: entry.fields.map((field, index) => {
        const fieldBytes = decodeRecord(raw, index);
        return fieldBytes == null
          ? { type: "Null" }
          : decodeBytes(
              field.column_type,
              fieldBytes,
              field.name,
              payloadDescriptor[index]?.valueType,
              nestedRowCarrier,
            );
      }),
    },
  };
}

function nonNullableStorageType(storageType?: ValueType): ValueType | undefined {
  let current = storageType;
  while (current?.tag === 15) current = current.inner;
  return current;
}

function arrayElementStorageType(storageType?: ValueType): ValueType | undefined {
  const array = nonNullableStorageType(storageType);
  return array?.tag === 14 ? array.inner : undefined;
}

function recordStorageDescriptor(storageType?: ValueType): DescriptorField[] | undefined {
  const record = nonNullableStorageType(storageType);
  return record?.tag === 16 ? record.record : undefined;
}

export type NestedRowCarrier = "full-record" | "keyed-terminal";

export function decodeNestedRowBytes(
  columns: readonly ColumnDescriptor[],
  bytes: Uint8Array,
  descriptor?: DescriptorField[],
  carrier: NestedRowCarrier = "full-record",
): { id?: string; values: Value[]; valuesByColumn?: Map<string, Value> } {
  if (descriptor) {
    const keyed = carrier === "keyed-terminal";
    if (keyed && descriptor[0]?.name !== "row_uuid") {
      throw new Error("keyed terminal nested row descriptor must start with row_uuid");
    }
    if (keyed && bytes.byteLength < 16) throw new Error("terminal nested row is missing its key");
    const payloadDescriptor = keyed ? descriptor.slice(1) : descriptor;
    const payload = keyed ? bytes.subarray(16) : bytes;
    const decodeRecord = createRecordValueDecoder(payloadDescriptor);
    const columnsByName = new Map(columns.map((column) => [column.name, column]));
    const valuesByColumn = new Map<string, Value>();
    let id = keyed ? formatUuid(bytes.subarray(0, 16)) : undefined;
    for (let index = 0; index < payloadDescriptor.length; index += 1) {
      const name = payloadDescriptor[index]?.name;
      if (!name) continue;
      const valueBytes = decodeRecord(payload, index);
      if (!keyed && name === "row_uuid" && valueBytes) {
        id = formatUuid(valueBytes);
        continue;
      }
      const column = columnsByName.get(name);
      if (!column) continue;
      valuesByColumn.set(
        name,
        valueBytes == null
          ? { type: "Null" }
          : decodeBytes(
              column.column_type,
              valueBytes,
              name,
              payloadDescriptor[index]?.valueType,
              carrier,
            ),
      );
    }
    const row: { id?: string; values: Value[]; valuesByColumn?: Map<string, Value> } = {
      id,
      values: columns.map(
        (column) =>
          valuesByColumn.get(column.name) ??
          (column.column_type.type === "Array"
            ? ({ type: "Array", value: [] } satisfies Value)
            : ({ type: "Null" } satisfies Value)),
      ),
    };
    Object.defineProperty(row, "valuesByColumn", {
      value: valuesByColumn,
      enumerable: false,
      configurable: true,
    });
    return row;
  }
  if (bytes.byteLength < 5) throw new Error("invalid nested row value");
  const hasId = bytes[0] === 1;
  let offset = 1;
  let id: string | undefined;
  if (hasId) {
    if (bytes.byteLength < 21) throw new Error("invalid nested row id");
    id = formatUuid(bytes.subarray(offset, offset + 16));
    offset += 16;
  }
  const length = readU32Le(bytes, offset);
  offset += 4;
  const raw = bytes.subarray(offset, offset + length);
  if (raw.byteLength !== length) throw new Error("invalid nested row value length");
  return { id, values: decodeNativeRowValues(columns, raw) };
}

function decodeArrayBytes(
  elementType: ColumnType,
  bytes: Uint8Array,
  storageElementType?: ValueType,
  nestedRowCarrier: NestedRowCarrier = "full-record",
): Value[] {
  const elementWidth = nativeFixedValueSize(
    storageElementType ?? storageColumnTypeToValueType(elementType),
  );
  if (elementWidth != null) {
    if (elementWidth === 0) return [];
    if (bytes.length % elementWidth !== 0) {
      throw new Error(`invalid fixed-width array byte length ${bytes.length}`);
    }
    const values: Value[] = [];
    for (let offset = 0; offset < bytes.length; offset += elementWidth) {
      values.push(
        decodeBytes(
          elementType,
          bytes.subarray(offset, offset + elementWidth),
          undefined,
          storageElementType,
          nestedRowCarrier,
        ),
      );
    }
    return values;
  }

  if (bytes.length < 4) {
    throw new Error("invalid variable-width array byte length");
  }

  const length = readU32Le(bytes, 0);
  const offsetTableEnd = 4 + Math.max(0, length - 1) * 4;
  if (offsetTableEnd > bytes.length) {
    throw new Error("invalid variable-width array offset table");
  }

  const values: Value[] = [];
  for (let index = 0; index < length; index += 1) {
    const start = index === 0 ? offsetTableEnd : readU32Le(bytes, 4 + (index - 1) * 4);
    const end = index === length - 1 ? bytes.length : readU32Le(bytes, 4 + index * 4);
    if (start > end || end > bytes.length) {
      throw new Error("invalid variable-width array element offset");
    }
    values.push(
      decodeBytes(
        elementType,
        bytes.subarray(start, end),
        undefined,
        storageElementType,
        nestedRowCarrier,
      ),
    );
  }
  return values;
}

function normalizeSubscriptionChunk(chunk: unknown):
  | { type: "snapshot"; snapshot: NativeRelationSubscriptionSnapshot; settled?: boolean }
  | {
      type: "delta";
      reset?: boolean;
      delta: NativeSubscriptionDelta;
      terminalOperations?: NativeTerminalOperation[];
      settled?: boolean;
      publishable?: boolean;
    }
  | {
      type: "rejected";
      reason:
        | { type: "UnsupportedShapeCapability"; detail: string }
        | { type: "ServerFailure"; code: string }
        | { type: "InvalidAuthoritySourceClosure"; transition: string }
        | { type: "ShapeRegistrationPendingCatalogueAdmission" };
    }
  | { type: "closed" } {
  if (!chunk || typeof chunk !== "object") throw new Error("expected subscription chunk");
  const record = chunk as {
    type?: unknown;
    rows?: unknown;
    delta?: unknown;
    reason?: unknown;
    reset?: unknown;
    settled?: unknown;
    publishable?: unknown;
    terminalOperations?: unknown;
  };
  if (record.type === "closed" || record.type === "Closed") {
    return { type: "closed" };
  }
  if (record.type === "snapshot" || record.type === "Snapshot") {
    return {
      type: "snapshot",
      snapshot: readRelationSnapshot(assertBytes(record.rows, "subscription rows")),
      settled: typeof record.settled === "boolean" ? record.settled : undefined,
    };
  }
  if (record.type === "delta" || record.type === "Delta") {
    return {
      type: "delta",
      reset: record.reset === true,
      delta: readNativeSubscriptionDelta(
        new PostcardReader(assertBytes(record.delta, "subscription delta")),
      ),
      terminalOperations: Array.isArray(record.terminalOperations)
        ? (record.terminalOperations as NativeTerminalOperation[])
        : undefined,
      settled: typeof record.settled === "boolean" ? record.settled : undefined,
      publishable: typeof record.publishable === "boolean" ? record.publishable : undefined,
    };
  }
  if (record.type === "rejected" || record.type === "Rejected") {
    return {
      type: "rejected",
      reason: normalizeSubscriptionRejectionReason(record.reason),
    };
  }
  throw new Error("unknown subscription chunk");
}

function normalizeSubscriptionRejectionReason(
  reason: unknown,
):
  | { type: "UnsupportedShapeCapability"; detail: string }
  | { type: "ServerFailure"; code: string }
  | { type: "InvalidAuthoritySourceClosure"; transition: string }
  | { type: "ShapeRegistrationPendingCatalogueAdmission" } {
  if (!reason || typeof reason !== "object") {
    throw new Error("expected subscription rejection reason");
  }
  const record = reason as {
    type?: unknown;
    detail?: unknown;
    code?: unknown;
    transition?: unknown;
  };
  if (record.type === "UnsupportedShapeCapability" && typeof record.detail === "string") {
    return { type: "UnsupportedShapeCapability", detail: record.detail };
  }
  if (record.type === "ShapeRegistrationPendingCatalogueAdmission") {
    return { type: "ShapeRegistrationPendingCatalogueAdmission" };
  }
  if (record.type === "ServerFailure" && typeof record.code === "string") {
    return { type: "ServerFailure", code: record.code };
  }
  if (record.type === "InvalidAuthoritySourceClosure" && typeof record.transition === "string") {
    return { type: "InvalidAuthoritySourceClosure", transition: record.transition };
  }
  throw new Error("unknown subscription rejection reason");
}

function subscriptionRejectionError(
  reason:
    | { type: "UnsupportedShapeCapability"; detail: string }
    | { type: "ServerFailure"; code: string }
    | { type: "InvalidAuthoritySourceClosure"; transition: string }
    | { type: "ShapeRegistrationPendingCatalogueAdmission" },
): Error {
  const detail =
    reason.type === "UnsupportedShapeCapability"
      ? reason.detail
      : reason.type === "ServerFailure"
        ? reason.code
        : reason.type === "InvalidAuthoritySourceClosure"
          ? reason.transition
          : "catalogue admission pending";
  return new Error(`Subscription rejected: ${reason.type}: ${detail}`);
}

function subscriptionSource(
  subscription: ReadableStream<unknown> | Subscription,
): ReadableStreamDefaultReader<unknown> | Subscription {
  const maybeReadable = subscription as Partial<ReadableStream<unknown>>;
  if (typeof maybeReadable.getReader === "function") {
    return maybeReadable.getReader();
  }
  return subscription as Subscription;
}

function isReadableSubscriptionReader(
  source: ReadableStreamDefaultReader<unknown> | Subscription,
): source is ReadableStreamDefaultReader<unknown> {
  return "read" in source && typeof source.read === "function";
}

function runtimeDeltaFromRows(
  rows: RowState[],
  previousRows: RowState[] = [],
  schema?: WasmSchema,
  outputColumns: SubscriptionOutputColumns | null = null,
): RuntimeSubscriptionDelta {
  const previousByKey = new Map(
    previousRows.map((row, index) => [rowStateKey(row), { row, index }]),
  );
  const nextKeys = new Set<string>();
  const added: RowState[] = [];
  const updated: RowState[] = [];
  const removed: Array<{ id: string; index: number; resultKeyBytes?: Uint8Array }> = [];
  const rowIndexByKey = indexRowsByKey(rows);

  rows.forEach((row, index) => {
    const key = rowStateKey(row);
    nextKeys.add(key);
    const previous = previousByKey.get(key);
    if (!previous) {
      added.push(row);
      return;
    }
    if (previous.index !== index || !rowValuesEqual(previous.row.values, row.values)) {
      updated.push(row);
    }
  });

  previousRows.forEach((row, index) => {
    if (!nextKeys.has(rowStateKey(row))) {
      removed.push({ id: row.id, index, resultKeyBytes: row.resultKeyBytes });
    }
  });

  return runtimeDeltaFromChanges(added, updated, removed, rowIndexByKey, schema, outputColumns);
}

function runtimeResetDeltaFromRows(
  rows: RowState[],
  schema: WasmSchema,
  outputColumns: SubscriptionOutputColumns | null = null,
): RuntimeSubscriptionDelta {
  return {
    ...runtimeDeltaFromChanges(rows, [], [], indexRowsByKey(rows), schema, outputColumns),
    reset: true,
  };
}

function subscriptionDeltaPayloadBytes(
  delta: NativeSubscriptionDelta,
  terminalOperations?: NativeTerminalOperation[],
): number {
  const rowBytes = delta.added
    .concat(delta.updated)
    .reduce(
      (sum, batch) =>
        sum +
        batch.rows.reduce((rowSum, row) => rowSum + row.raw.byteLength + row.rowId.byteLength, 0),
      0,
    );
  const occurrenceBytes = delta.addedOccurrenceKeys
    .concat(delta.updatedOccurrenceKeys, delta.removedOccurrenceKeys)
    .reduce((sum, key) => sum + key.byteLength, 0);
  const terminalBytes =
    terminalOperations?.reduce(
      (sum, operation) => sum + nativeTerminalOperationBytes(operation),
      0,
    ) ?? 0;
  return rowBytes + occurrenceBytes + terminalBytes;
}

function nativeTerminalOperationBytes(operation: NativeTerminalOperation): number {
  const rootKeyBytes = operation.root_key.length;
  const pathBytes = operation.path.reduce((sum, segment) => {
    if ("Collection" in segment) {
      return sum + utf8ByteLength(segment.Collection);
    }
    return sum + segment.Key.length;
  }, 0);
  const editBytes =
    "Insert" in operation.edit
      ? operation.edit.Insert.key.length + operation.edit.Insert.value.length
      : "Update" in operation.edit
        ? operation.edit.Update.key.length + operation.edit.Update.value.length
        : "Remove" in operation.edit
          ? operation.edit.Remove.key.length
          : operation.edit.Move.key.length;
  return rootKeyBytes + pathBytes + editBytes;
}

function utf8ByteLength(value: string): number {
  return new TextEncoder().encode(value).byteLength;
}

function runtimeDeltaFromChanges(
  added: RowState[],
  updated: RowState[],
  removed: Array<{ id: string; index: number; resultKeyBytes?: Uint8Array }>,
  rowIndexByKey: Map<string, number>,
  schema?: WasmSchema,
  outputColumns: SubscriptionOutputColumns | null = null,
): RuntimeSubscriptionDelta {
  return {
    added: added.map((row) => ({
      sourceId: row.id,
      occurrenceKey: row.resultKeyBytes ?? legacyResultKey(row.id),
      index: rowIndexByKey.get(rowStateKey(row)) ?? 0,
      row: runtimeSubscriptionRow(row, schema, outputColumns),
    })),
    updated: updated.map((row) => ({
      sourceId: row.id,
      occurrenceKey: row.resultKeyBytes ?? legacyResultKey(row.id),
      index: rowIndexByKey.get(rowStateKey(row)) ?? 0,
      row: runtimeSubscriptionRow(row, schema, outputColumns),
    })),
    removed: removed.map((row) => ({
      sourceId: row.id,
      occurrenceKey: row.resultKeyBytes ?? legacyResultKey(row.id),
      index: row.index,
    })),
  };
}

function runtimeSubscriptionRow(
  row: RowState,
  schema: WasmSchema | undefined,
  outputColumns: SubscriptionOutputColumns | null = null,
): WasmRow {
  const physicalColumns =
    outputColumns && row.table === outputColumns.rootTable
      ? outputColumns.rootColumns
      : schema?.[row.table]?.columns;
  const columns = physicalColumns ? logicalStorageColumns(physicalColumns) : undefined;
  if (!columns) {
    throw new Error(`missing schema for subscription row table ${row.table}`);
  }
  const values = valuesForNativeFrame(row, columns).map((value, index) =>
    runtimeSubscriptionValue(value, columns[index]!.column_type),
  );
  const result: WasmRow = { id: row.id, values };
  Object.defineProperty(result, "valuesByColumn", {
    value: new Map(columns.map((column, index) => [column.name, values[index]!])),
    configurable: true,
  });
  return result;
}

function runtimeSubscriptionValue(value: Value, type: ColumnType): Value {
  if (value.type === "Null") return value;
  if (type.type === "Array" && value.type === "Array") {
    return {
      type: "Array",
      value: value.value.map((entry) => runtimeSubscriptionValue(entry, type.element)),
    };
  }
  if (type.type === "Row" && value.type === "Row") {
    return { type: "Row", value: runtimeSubscriptionNestedRow(value.value, type.columns) };
  }
  if (type.type === "EnumPayload" && value.type === "Enum") {
    const payload = type.cases.find((entry) => entry.name === value.value.case)?.fields;
    if (!payload) return value;
    return {
      type: "Enum",
      value: {
        case: value.value.case,
        values: value.value.values.map((entry, index) =>
          payload[index] ? runtimeSubscriptionValue(entry, payload[index]!.column_type) : entry,
        ),
      },
    };
  }
  return value;
}

function runtimeSubscriptionNestedRow(
  row: { id?: string; values: Value[] },
  columns: readonly ColumnDescriptor[],
): { id?: string; values: Value[] } {
  const named = (row as { valuesByColumn?: Map<string, Value> }).valuesByColumn;
  const values = columns.map((column, index) => {
    const value = named?.get(column.name) ?? row.values[index];
    const resolved =
      value ??
      (column.column_type.type === "Array" ? { type: "Array", value: [] } : { type: "Null" });
    return runtimeSubscriptionValue(resolved, column.column_type);
  });
  const result = { id: row.id, values };
  Object.defineProperty(result, "valuesByColumn", {
    value: new Map(columns.map((column, index) => [column.name, values[index]!])),
    configurable: true,
  });
  return result;
}

function valuesForNativeFrame(row: RowState, columns: readonly ColumnDescriptor[]): Value[] {
  if (!row.valuesByColumn) {
    return row.values.slice(0, columns.length);
  }
  const values: Value[] = [];
  values.length = columns.length;
  for (let index = 0; index < columns.length; index += 1) {
    const column = columns[index]!;
    const value =
      row.valuesByColumn.get(column.name) ??
      (column.column_type.type === "Array" ? { type: "Array", value: [] } : { type: "Null" });
    values[index] = value;
  }
  return values;
}

function subscriptionRowsRequireBufferedPublication(
  rows: RowState[],
  schema: WasmSchema,
  outputColumns: SubscriptionOutputColumns | null,
): boolean {
  return rows.some((row) => {
    const columns =
      outputColumns && row.table === outputColumns.rootTable
        ? outputColumns.rootColumns
        : schema[row.table]?.columns;
    if (!columns) return false;
    return valuesForNativeFrame(row, logicalStorageColumns(columns)).some(
      (value, index) =>
        value.type === "Null" &&
        logicalStorageColumns(columns)[index]?.nullable === false &&
        logicalStorageColumns(columns)[index]?.column_type.type !== "Array",
    );
  });
}

function legacyResultKey(id: string): Uint8Array {
  return Uint8Array.from([1, ...parseUuid(id)]);
}

function rowValuesEqual(left: Value[], right: Value[]): boolean {
  if (left.length !== right.length) return false;
  return left.every((value, index) => valueEqual(value, right[index]));
}

function valueEqual(left: Value, right: Value | undefined): boolean {
  if (!right || left.type !== right.type) return false;
  switch (left.type) {
    case "Bytea":
      return right.type === "Bytea" && bytesEqual(left.value, right.value);
    case "Array":
      return right.type === "Array" && rowValuesEqual(left.value, right.value);
    case "Enum":
      return (
        right.type === "Enum" &&
        left.value.case === right.value.case &&
        rowValuesEqual(left.value.values, right.value.values)
      );
    case "Null":
      return right.type === "Null";
    case "Boolean":
    case "Text":
    case "Uuid":
    case "Integer":
    case "BigInt":
    case "Double":
    case "Timestamp":
    case "Row":
      return "value" in right && left.value === right.value;
  }
}

function bytesEqual(left: Uint8Array, right: Uint8Array): boolean {
  if (left.length !== right.length) return false;
  return left.every((byte, index) => byte === right[index]);
}

export function parseUuid(value: string): Uint8Array {
  const hex = value.replaceAll("-", "");
  if (!/^[0-9a-fA-F]{32}$/.test(hex)) throw new Error(`invalid uuid ${value}`);
  const bytes = new Uint8Array(16);
  for (let i = 0; i < 16; i += 1) {
    bytes[i] = Number.parseInt(hex.slice(i * 2, i * 2 + 2), 16);
  }
  return bytes;
}

export function formatUuid(bytes: Uint8Array): string {
  return (
    byteHex[bytes[0]!] +
    byteHex[bytes[1]!] +
    byteHex[bytes[2]!] +
    byteHex[bytes[3]!] +
    "-" +
    byteHex[bytes[4]!] +
    byteHex[bytes[5]!] +
    "-" +
    byteHex[bytes[6]!] +
    byteHex[bytes[7]!] +
    "-" +
    byteHex[bytes[8]!] +
    byteHex[bytes[9]!] +
    "-" +
    byteHex[bytes[10]!] +
    byteHex[bytes[11]!] +
    byteHex[bytes[12]!] +
    byteHex[bytes[13]!] +
    byteHex[bytes[14]!] +
    byteHex[bytes[15]!]
  );
}

function readU32Le(bytes: Uint8Array, offset: number): number {
  return (
    bytes[offset]! |
    (bytes[offset + 1]! << 8) |
    (bytes[offset + 2]! << 16) |
    (bytes[offset + 3]! << 24)
  );
}

function bytesKey(bytes: Uint8Array): string {
  return Array.from(bytes, (byte) => String.fromCharCode(byte)).join("");
}

/** Deterministic cache-key encoding for JSON-derived session claims. */
function canonicalJson(value: unknown): string {
  if (value === null || typeof value === "boolean" || typeof value === "string") {
    return JSON.stringify(value);
  }
  if (typeof value === "number") return Number.isFinite(value) ? JSON.stringify(value) : "null";
  if (Array.isArray(value)) return `[${value.map(canonicalJson).join(",")}]`;
  if (isRecord(value)) {
    return `{${Object.keys(value)
      .sort()
      .map((key) => `${JSON.stringify(key)}:${canonicalJson(value[key])}`)
      .join(",")}}`;
  }
  return "null";
}

function sameBytes(left: Uint8Array, right: Uint8Array): boolean {
  return left.length === right.length && left.every((byte, index) => byte === right[index]);
}

function publicFieldName(name: string): string {
  return name.startsWith("user_") ? name.slice("user_".length) : name;
}

function isInternalField(name?: string): boolean {
  return (
    name === "row_uuid" ||
    name === "tx_node_id" ||
    name === "tx_time" ||
    name === "schema_version" ||
    name === "parents" ||
    name === "authored_columns"
  );
}

function isHiddenIncludeColumn(name: string): boolean {
  return name.startsWith(HIDDEN_INCLUDE_COLUMN_PREFIX);
}

function assertBytes(value: unknown, label: string): Uint8Array {
  if (value instanceof Uint8Array) return value;
  if (Array.isArray(value)) return Uint8Array.from(value);
  throw new Error(`expected ${label} bytes`);
}

function reportAsyncRuntimeError(error: unknown): void {
  queueMicrotask(() => {
    throw error;
  });
}
