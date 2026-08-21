/**
 * JazzClient - High-level TypeScript client for Jazz.
 *
 * Wraps the WASM runtime and provides a clean API for CRUD operations,
 * subscriptions, and sync.
 */

import type { AppContext, RuntimeSourcesConfig, Session } from "./context.js";
import type { InsertValues, Value, SubscriptionWireDelta, WasmSchema } from "../drivers/types.js";
import { normalizeRuntimeSchema } from "../drivers/schema-wire.js";
import type { AuthFailureReason } from "./auth-state.js";
import { resolveClientSessionStateSync } from "./client-session.js";
import { mapAuthReason } from "./auth-state.js";
import {
  resolveRuntimeConfigSyncInitInput,
  resolveRuntimeConfigWasmUrl,
} from "./runtime-config.js";
import { httpUrlToWs } from "./url.js";
import { PostcardWriter } from "./native-runtime/native-codec.js";

function encodeBranchColumnValue(value: Value): Uint8Array {
  const writer = new PostcardWriter();
  switch (value.type) {
    case "Integer":
      if (
        !Number.isSafeInteger(value.value) ||
        value.value < -0x80000000 ||
        value.value > 0x7fffffff
      ) {
        throw new Error("branch Integer values must be signed 32-bit integers");
      }
      writer.enumUnit(14); // groove::Value::I32
      writer.i64(value.value);
      break;
    case "BigInt": {
      if (typeof value.value === "number" && !Number.isSafeInteger(value.value)) {
        throw new Error("branch BigInt values supplied as numbers must be safe integers");
      }
      const integer = BigInt(value.value);
      if (integer < -(1n << 63n) || integer > (1n << 63n) - 1n) {
        throw new Error("branch BigInt values must be signed 64-bit integers");
      }
      writer.enumUnit(13); // groove::Value::I64
      writer.i64(integer);
      break;
    }
    case "Uuid": {
      writer.enumUnit(8); // groove::Value::Uuid
      const hex = value.value.replaceAll("-", "");
      if (!/^[0-9a-fA-F]{32}$/.test(hex)) throw new Error(`invalid branch UUID ${value.value}`);
      writer.bytes(
        Uint8Array.from({ length: 16 }, (_, index) =>
          Number.parseInt(hex.slice(index * 2, index * 2 + 2), 16),
        ),
      );
      break;
    }
    case "Text":
      writer.enumUnit(6); // groove::Value::String
      writer.string(value.value);
      break;
    default:
      throw new Error(
        `branch columns currently require Integer, BigInt, Text, or Uuid values; got ${value.type}`,
      );
  }
  return writer.finish();
}

type WireBranchSelector = { values: Record<string, number[]> };
type WireBranchViewBase =
  | { Current: WireBranchSelector }
  | { Snapshot: { branch: WireBranchSelector; snapshot: unknown } };

function encodeBranchSelector(value: BranchSelector): WireBranchSelector {
  return {
    values: Object.fromEntries(
      Object.entries(value.values).map(([name, branchValue]) => [
        name,
        Array.from(encodeBranchColumnValue(branchValue)),
      ]),
    ),
  };
}

function encodeBranchViewBase(base: BranchViewBase | undefined): WireBranchViewBase | undefined {
  return base?.kind === "current"
    ? { Current: encodeBranchSelector(base.branch) }
    : base?.kind === "snapshot"
      ? { Snapshot: { branch: encodeBranchSelector(base.branch), snapshot: base.snapshot } }
      : undefined;
}

/**
 * Minimal request shape supported by backend request helpers.
 *
 * Works with common server frameworks (Express, Fastify, Hono, Web Request wrappers)
 * as long as Authorization headers are exposed through `header(name)` or `headers`.
 */
export interface RequestLike {
  header?: (name: string) => string | undefined;
  headers?: Headers | Record<string, string | string[] | undefined>;
}

/**
 * Common interface for the runtime backing `JazzClient`.
 */
export interface Runtime {
  insert(
    table: string,
    values: InsertValues,
    write_context_json?: string | null,
    object_id?: string | null,
  ): InsertResult;
  restore(
    table: string,
    object_id: string,
    values: InsertValues,
    write_context_json?: string | null,
  ): InsertResult;
  update(
    table: string,
    object_id: string,
    values: Record<string, Value>,
    write_context_json?: string | null,
  ): MutationReceipt;
  upsert(
    table: string,
    object_id: string,
    values: InsertValues,
    write_context_json?: string | null,
  ): MutationReceipt;
  delete(table: string, object_id: string, write_context_json?: string | null): MutationReceipt;
  canInsertLocally?(table: string, values: InsertValues, session?: Session): PermissionAdvice;
  canReadLocally?(table: string, objectId: string, session?: Session): PermissionAdvice;
  canUpdateLocally?(
    table: string,
    objectId: string,
    values: Record<string, Value>,
    session?: Session,
  ): PermissionAdvice;
  canDeleteLocally?(table: string, objectId: string, session?: Session): PermissionAdvice;
  requestInsertPermissionAdvice?(
    table: string,
    values: InsertValues,
    session?: Session,
  ): Promise<PermissionAdvice>;
  requestReadPermissionAdvice?(
    table: string,
    objectId: string,
    session?: Session,
  ): Promise<PermissionAdvice>;
  requestUpdatePermissionAdvice?(
    table: string,
    objectId: string,
    values: Record<string, Value>,
    session?: Session,
  ): Promise<PermissionAdvice>;
  requestDeletePermissionAdvice?(
    table: string,
    objectId: string,
    session?: Session,
  ): Promise<PermissionAdvice>;
  onMutationError(callback: (event: MutationErrorEvent) => void): void;
  waitForTransaction(batchId: BatchId | Promise<BatchId>, tier: string): Promise<void>;
  query(
    query_json: string,
    session_json?: string | null,
    tier?: string | null,
    options_json?: string | null,
  ): Promise<any>;
  createSubscription(
    query_json: string,
    session_json?: string | null,
    tier?: string | null,
    options_json?: string | null,
  ): number;
  executeSubscription(handle: number, on_update: Function): void;
  unsubscribe(handle: number): void;
  close?(): void | Promise<void>;
  clearClientStorage?(): Promise<void>;
  /** Connect to a Jazz server over WebSocket (Rust transport). */
  connect(url: string, auth_json: string): void;
  /**
   * Disconnect from the Jazz server and drop the transport handle.
   *
   * Resolves once the runtime has completed the disconnect. For worker-backed
   * runtimes, this includes a round-trip in which the worker performs the
   * disconnect before replying.
   */
  disconnect(options?: { rejectWaiters?: boolean }): Promise<void>;
  /** Push updated auth credentials into the live Rust transport. */
  updateAuth(auth_json: string): void;
  /** Register a callback invoked when the Rust transport rejects the JWT. */
  onAuthFailure(callback: (reason: string) => void): void;
}

/**
 * Advisory result for a permission preflight. `allowed` and `denied` are
 * final only when a trusted-serving authority evaluated the request;
 * `unknown` means that a local replica or unavailable authority cannot decide.
 */
export type PermissionAdvice = "allowed" | "denied" | "unknown";

export interface TransactionalRuntime extends Runtime {
  beginTransaction(
    transactionKind: TransactionKind,
    id: OpenBatchId,
    sessionJson?: string | null,
  ): OpenBatchId;
  commitTransaction(id: OpenBatchId): Promise<BatchId>;
  rollbackTransaction(id: OpenBatchId): Promise<boolean>;
}

declare const openBatchIdBrand: unique symbol;
declare const batchIdBrand: unique symbol;

/** Identity of a mutable batch. Invalid after commit or rollback. */
export type OpenBatchId = string & { readonly [openBatchIdBrand]: true };
/** Immutable identity assigned to a successfully committed batch. */
export type BatchId = string & { readonly [batchIdBrand]: true };

/** Generate a coordination-free UUIDv7 identity for a new mutable batch. */
export function createOpenBatchId(): OpenBatchId {
  const bytes = crypto.getRandomValues(new Uint8Array(16));
  const timestamp = Date.now();
  bytes[0] = Math.floor(timestamp / 2 ** 40) & 0xff;
  bytes[1] = Math.floor(timestamp / 2 ** 32) & 0xff;
  bytes[2] = Math.floor(timestamp / 2 ** 24) & 0xff;
  bytes[3] = Math.floor(timestamp / 2 ** 16) & 0xff;
  bytes[4] = Math.floor(timestamp / 2 ** 8) & 0xff;
  bytes[5] = timestamp & 0xff;
  bytes[6] = (bytes[6]! & 0x0f) | 0x70;
  bytes[8] = (bytes[8]! & 0x3f) | 0x80;
  const hex = [...bytes].map((byte) => byte.toString(16).padStart(2, "0")).join("");
  return hex as OpenBatchId;
}

/**
 * Authentication configuration for connecting to a Jazz server.
 *
 * Maps directly to the Rust `AuthConfig` struct in `jazz-tools/src/websocket_prelude_auth.rs`.
 * All fields are optional; supply only the ones relevant to your auth mode.
 */
export interface AuthConfig {
  /** JWT bearer token for user authentication. */
  jwt_token?: string;
  /** Backend service secret for server-to-server calls. */
  backend_secret?: string;
  /** Admin secret for privileged sync and `/admin/*` catalogue operations. */
  admin_secret?: string;
  /** Opaque session payload forwarded by a backend proxy. */
  backend_session?: unknown;
}

/**
 * Persistence tier for durability guarantees.
 *
 * - `local`: Persisted in local durable storage
 * - `edge`: Persisted at edge server
 * - `global`: Persisted at global server
 */
export type DurabilityTier = "local" | "edge" | "global";
/**
 * Controls when a write is visible to subscriptions.
 *
 * - With `"immediate"`, your own local writes appear in the subscription while it's still waiting for
 * the tier to confirm the initial snapshot (only once the subscription has settled at least once).
 * - With `"deferred"`, all delivery is held until the tier confirms.
 * Default is `"immediate"`.
 */
export type LocalUpdatesMode = "immediate" | "deferred";
/**
 * Controls where the subscription reads data from.
 *
 * - With `"full"`, the subscription is sent to upstream servers, which push matching data back.
 * - With `"local-only"`, only local storage is queried and no server communication happens.
 */
export type QueryPropagation = "full" | "local-only";
/**
 * Whether this query should be shown in the inspector.
 * Useful for helpers and framework internals that create subscriptions
 * but should stay out of the DB inspector.
 * Defaults to `"public"`.
 */
export type QueryVisibility = "public" | "hidden_from_live_query_list";

/** Named values selecting one exact branch-local row coordinate. */
export interface BranchSelector {
  values: Record<string, Value>;
}

/** A live or application-resolved frozen base underneath a branch head. */
export type BranchViewBase =
  | { kind: "current"; branch: BranchSelector }
  | { kind: "snapshot"; branch: BranchSelector; snapshot: unknown };

/** User-facing head-over-base branch view. */
export interface BranchView {
  head: BranchSelector;
  base?: BranchViewBase;
}

export interface QueryExecutionOptions {
  tier?: DurabilityTier;
  localUpdates?: LocalUpdatesMode;
  propagation?: QueryPropagation;
  visibility?: QueryVisibility;
  /** Admit exact-head history, falling back to an optional live or frozen base. */
  branch?: BranchView;
}

type InternalQueryExecutionOptions = QueryExecutionOptions & {
  openBatchId?: OpenBatchId;
  runtimeSettledTier?: DurabilityTier | null;
};

export interface ResolvedQueryExecutionOptions {
  tier: DurabilityTier;
  localUpdates: LocalUpdatesMode;
  propagation: QueryPropagation;
  visibility: QueryVisibility;
  branch?: BranchView;
}

type ResolvedInternalQueryExecutionOptions = ResolvedQueryExecutionOptions & {
  openBatchId?: OpenBatchId;
};

interface TimestampOverrideOptions {
  updatedAt?: number;
}

/**
 * Selects the transaction semantics used for grouped writes.
 *
 * - `mergeable`: eventually-consistent writes that merge with concurrent writes.
 * - `exclusive`: serializable writes that are validated as one unit by the authority.
 */
export type TransactionKind = "mergeable" | "exclusive";

export type TransactionFate =
  | {
      kind: "missing";
      transactionId: BatchId;
    }
  | {
      kind: "rejected";
      transactionId: BatchId;
      code: string;
      reason: string;
    }
  | {
      kind: "accepted";
      transactionId: BatchId;
      confirmedTier: DurabilityTier;
    };

export interface LocalTransactionRecord {
  transactionId: BatchId;
  kind: TransactionKind;
  sealed: boolean;
  latestSettlement: TransactionFate;
  encodedRecord?: Uint8Array;
}

/**
 * A rejected write emitted by {@link JazzClient.onMutationError}.
 *
 * The event is a fallback for writes whose rejection was not handled by an
 * active {@link MutationResult.wait} call.
 */
export interface MutationErrorEvent {
  code: string;
  reason: string;
  transaction: LocalTransactionRecord;
}

export interface InsertOptions extends TimestampOverrideOptions {
  id?: string;
  branch?: BranchSelector;
}

export interface UpdateOptions extends TimestampOverrideOptions {
  branch?: BranchView;
}

export interface DeleteOptions extends TimestampOverrideOptions {
  branch?: BranchView;
}

export interface RestoreOptions extends TimestampOverrideOptions {
  branch?: BranchSelector;
}

/**
 * Query row result.
 */
export interface Row {
  id: string;
  values: Value[];
}

export type WriteReceipt =
  | { readonly kind: "committed"; readonly batchId: BatchId | Promise<BatchId> }
  | { readonly kind: "staged"; readonly openBatchId: OpenBatchId };

export type InsertResult = Row & WriteReceipt;
export type MutationReceipt = WriteReceipt;

interface WriteContextPayload {
  session?: Session;
  attribution?: string;
  updated_at?: number;
  batch_id?: string;
  target_branch_name?: string;
  branch_view?: { head: WireBranchSelector; base?: WireBranchViewBase };
}

/**
 * Subscription callback type.
 */
export type SubscriptionCallback = (delta: SubscriptionWireDelta) => void;

export interface ConnectRuntimeOptions {
  onAuthFailure?: (reason: AuthFailureReason) => void;
}

type QueryExecutionDefaultsContext = {
  serverUrl?: string;
  defaultDurabilityTier?: DurabilityTier;
};

export function resolveDefaultDurabilityTier(
  context: QueryExecutionDefaultsContext,
): DurabilityTier {
  if (context.defaultDurabilityTier) {
    return context.defaultDurabilityTier;
  }

  if (isBrowserRuntime()) {
    return "local";
  }

  // In non-browser environments, default to edge when connected to a server.
  // For local/in-memory runtimes without a server, keep local semantics.
  return context.serverUrl ? "edge" : "local";
}

export function resolveEffectiveQueryExecutionOptions(
  context: QueryExecutionDefaultsContext,
  options?: QueryExecutionOptions,
): ResolvedQueryExecutionOptions {
  return {
    tier: options?.tier ?? resolveDefaultDurabilityTier(context),
    localUpdates: options?.localUpdates ?? "immediate",
    propagation: options?.propagation ?? "full",
    visibility: options?.visibility ?? "public",
    branch: options?.branch,
  };
}

function isBrowserRuntime(): boolean {
  return typeof window !== "undefined" && typeof document !== "undefined";
}

function getScheduler(): (task: () => void) => void {
  if ("scheduler" in globalThis) {
    return (task: () => void) => {
      // See: https://developer.mozilla.org/en-US/docs/Web/API/Scheduler/postTask
      // @ts-ignore Scheduler is not yet provided by the dom library
      void globalThis.scheduler.postTask(task, { priority: "user-visible" });
    };
  }

  // Wrap rather than returning queueMicrotask directly: the native function
  // throws "Illegal invocation" when called without globalThis as receiver.
  return (task: () => void) => queueMicrotask(task);
}

function encodeQueryExecutionOptions(options: InternalQueryExecutionOptions): string | undefined {
  const payload: {
    propagation?: QueryPropagation;
    local_updates?: LocalUpdatesMode;
    transaction_batch_id?: string;
    read_view?: {
      source: {
        BranchView: {
          head: WireBranchSelector;
          base?:
            | { Current: WireBranchSelector }
            | { Snapshot: { branch: WireBranchSelector; snapshot: unknown } };
        };
      };
    };
  } = {};
  if ((options.propagation ?? "full") !== "full") {
    payload.propagation = options.propagation;
  }
  if ((options.localUpdates ?? "immediate") !== "immediate") {
    payload.local_updates = options.localUpdates;
  }
  if (options.openBatchId) {
    payload.transaction_batch_id = options.openBatchId;
  }
  if (options.branch) {
    const base = options.branch.base;
    payload.read_view = {
      source: {
        BranchView: {
          head: encodeBranchSelector(options.branch.head),
          base: encodeBranchViewBase(base),
        },
      },
    };
  }

  if (
    !payload.propagation &&
    !payload.local_updates &&
    !payload.transaction_batch_id &&
    !payload.read_view
  ) {
    return undefined;
  }

  return JSON.stringify(payload);
}

function normalizeSubscriptionCallbackArgs(
  args: unknown[],
): Error | SubscriptionWireDelta | string | undefined {
  if (args.length === 2 && args[0] instanceof Error) {
    return args[0];
  }

  if (args.length === 1) {
    return args[0] as SubscriptionWireDelta | string;
  }

  if (args.length === 2 && args[0] == null) {
    return args[1] as SubscriptionWireDelta | string | undefined;
  }

  console.error("Invalid subscription callback arguments", args);
  return undefined;
}

function requireTransactionalRuntime(runtime: Runtime): TransactionalRuntime {
  if (
    typeof (runtime as Partial<TransactionalRuntime>).beginTransaction === "function" &&
    typeof (runtime as Partial<TransactionalRuntime>).commitTransaction === "function" &&
    typeof (runtime as Partial<TransactionalRuntime>).rollbackTransaction === "function"
  ) {
    return runtime as TransactionalRuntime;
  }

  throw new Error("This Jazz runtime does not support transactions");
}

function committedBatchId(result: WriteReceipt): BatchId | Promise<BatchId> {
  if (result.kind !== "committed") {
    throw new Error(`Runtime returned staged batch ${result.openBatchId} for an ordinary write`);
  }
  return result.batchId;
}

function normalizeUpdatedAt(updatedAt?: number): number | undefined {
  if (updatedAt === undefined) {
    return undefined;
  }
  if (!Number.isFinite(updatedAt) || !Number.isInteger(updatedAt) || updatedAt < 0) {
    throw new Error("Invalid updatedAt override. Expected a non-negative integer.");
  }
  return updatedAt;
}

function rejectionFromRuntimeWaitError(error: unknown): PersistedWriteRejectedError | null {
  if (!error || typeof error !== "object") {
    return null;
  }
  const candidate = error as {
    kind?: unknown;
    transactionId?: unknown;
    code?: unknown;
    reason?: unknown;
  };
  if (candidate.kind !== "rejected") {
    return null;
  }
  if (
    typeof candidate.code !== "string" ||
    typeof candidate.reason !== "string" ||
    typeof candidate.transactionId !== "string"
  ) {
    return null;
  }
  return new PersistedWriteRejectedError(
    candidate.transactionId as BatchId,
    candidate.code,
    candidate.reason,
  );
}

/**
 * Error returned when a write fails to be persisted at a given durability tier.
 */
export class PersistedWriteRejectedError extends Error {
  readonly name = "PersistedWriteRejectedError";

  constructor(
    readonly transactionId: BatchId,
    readonly code: string,
    readonly reason: string,
  ) {
    super(`Persisted transaction ${transactionId} was rejected (${code}): ${reason}`);
  }
}

/**
 * The result of a mutation.
 */
export class MutationResult<T, TKind extends TransactionKind = "mergeable"> {
  readonly #client: JazzClient;
  readonly #kind: TKind;
  readonly transactionId: Promise<BatchId>;

  constructor(
    readonly value: T,
    transactionId: BatchId | Promise<BatchId>,
    client: JazzClient,
    kind: TKind,
  ) {
    this.transactionId = Promise.resolve(transactionId);
    this.#client = client;
    this.#kind = kind;
  }

  /**
   * Wait for the mutation to be confirmed.
   *
   * Mergeable mutations require a durability tier. Exclusive mutations are
   * accepted or rejected by the authority, so callers do not pass options.
   *
   * Rejects with a {@link PersistedWriteRejectedError} if the write is rejected.
   */
  async wait(
    ...args: [TKind] extends ["exclusive"] ? [] : [options: { tier: DurabilityTier }]
  ): Promise<T> {
    if (this.#kind === "exclusive") {
      await this.#client.waitForExclusiveTransaction(await this.transactionId);
      return this.value;
    }

    const [options] = args as [options: { tier: DurabilityTier }];
    await this.#client.waitForTransaction(this.transactionId, options.tier);
    return this.value;
  }
}

/**
 * High-level Jazz client.
 */
export class JazzClient {
  private runtime: Runtime;
  private scheduler: (task: () => void) => void;
  private context: AppContext;
  private resolvedSession: Session | null;
  private defaultDurabilityTier: DurabilityTier;
  private shutdownPromise: Promise<void> | null = null;

  private resolveSessionFromContext(): Session | null {
    return resolveClientSessionStateSync({
      appId: this.context.appId,
      jwtToken: this.context.jwtToken,
      cookieSession: this.context.cookieSession,
    }).session;
  }

  private buildTransportAuthPayload(): {
    jwt_token: string | null;
    admin_secret?: string;
    backend_secret?: string;
    backend_session?: Session;
  } {
    const payload: {
      jwt_token: string | null;
      admin_secret?: string;
      backend_secret?: string;
      backend_session?: Session;
    } = { jwt_token: this.context.jwtToken ?? null };
    if (this.context.adminSecret) {
      payload.admin_secret = this.context.adminSecret;
    }
    if (this.context.backendSecret) {
      payload.backend_secret = this.context.backendSecret;
      if (this.context.cookieSession) {
        payload.backend_session = this.context.cookieSession;
      }
    }
    return payload;
  }

  private constructor(
    runtime: Runtime,
    context: AppContext,
    defaultDurabilityTier: DurabilityTier,
    runtimeOptions?: ConnectRuntimeOptions,
  ) {
    this.runtime = runtime;
    this.scheduler = getScheduler();
    this.context = context;
    this.defaultDurabilityTier = defaultDurabilityTier;
    this.resolvedSession = this.resolveSessionFromContext();

    if (runtimeOptions?.onAuthFailure) {
      const handler = runtimeOptions.onAuthFailure;
      this.runtime.onAuthFailure((reason: string) => {
        handler(mapAuthReason(reason));
      });
    }

    this.runtime.onMutationError((event) => {
      console.error("Unhandled Jazz mutation error", event);
    });
  }

  /**
   * Create client from a pre-constructed runtime.
   *
   * RuntimeSource implementations use this after selecting the platform runtime.
   *
   * @param runtime A runtime implementing the Runtime interface
   * @param context Application context
   * @returns Connected JazzClient instance
   */
  static connectWithRuntime(
    runtime: Runtime,
    context: AppContext,
    runtimeOptions?: ConnectRuntimeOptions,
  ): JazzClient {
    return new JazzClient(runtime, context, resolveDefaultDurabilityTier(context), runtimeOptions);
  }

  beginTransaction(kind: TransactionKind, session?: Session, attribution?: string): OpenBatchId {
    const id = createOpenBatchId();
    const effectiveSession = this.resolveWriteSession(session, attribution);
    return requireTransactionalRuntime(this.runtime).beginTransaction(
      kind,
      id,
      this.encodeWriteContext(effectiveSession, attribution),
    );
  }

  onMutationError(listener: (event: MutationErrorEvent) => void): void {
    this.runtime.onMutationError(listener);
  }

  commitTransaction(id: OpenBatchId): Promise<MutationResult<void>> {
    return requireTransactionalRuntime(this.runtime)
      .commitTransaction(id)
      .then((batchId) => new MutationResult(undefined, batchId, this, "mergeable"));
  }

  rollbackTransaction(id: OpenBatchId): Promise<boolean> {
    return requireTransactionalRuntime(this.runtime).rollbackTransaction(id);
  }

  /**
   * Enable backend-scoped sync auth for this client.
   *
   * In backend mode, sync/event transport uses `X-Jazz-Backend-Secret` instead
   * of end-user auth headers and intentionally does not send admin headers.
   */
  asBackend(): JazzClient {
    if (!this.context.backendSecret) {
      throw new Error("backendSecret required for backend mode");
    }
    if (!this.context.serverUrl) {
      throw new Error("serverUrl required for backend mode");
    }
    return this;
  }

  updateAuthToken(jwtToken?: string): void {
    this.context.jwtToken = jwtToken;
    this.resolvedSession = this.resolveSessionFromContext();
    // Push the refreshed credentials into the Rust transport.
    // Carry forward admin/backend secrets from context — omitting them here
    // would deserialise to None on the Rust side and silently erase any
    // privileged credentials the transport was connected with.
    this.runtime.updateAuth(JSON.stringify(this.buildTransportAuthPayload()));
  }

  updateCookieSession(cookieSession?: Session): void {
    this.context.cookieSession = cookieSession;
    this.resolvedSession = this.resolveSessionFromContext();
    this.runtime.updateAuth(JSON.stringify(this.buildTransportAuthPayload()));
  }

  private normalizeQueryExecutionOptions(
    options?: InternalQueryExecutionOptions,
  ): ResolvedInternalQueryExecutionOptions {
    const resolved = resolveEffectiveQueryExecutionOptions(
      { ...this.context, defaultDurabilityTier: this.defaultDurabilityTier },
      options,
    );
    if (!options?.openBatchId) {
      return resolved;
    }
    return {
      ...resolved,
      openBatchId: options.openBatchId,
    };
  }

  private encodeWriteContext(
    session?: Session,
    attribution?: string,
    openBatchId?: OpenBatchId,
    updatedAt?: number,
    branch?: BranchView,
  ): string | undefined {
    if (
      !session &&
      attribution === undefined &&
      !openBatchId &&
      updatedAt === undefined &&
      !branch
    ) {
      return undefined;
    }
    if (
      attribution === undefined &&
      session &&
      !openBatchId &&
      updatedAt === undefined &&
      !branch
    ) {
      return JSON.stringify(session);
    }

    const payload: WriteContextPayload = {};
    if (session) {
      payload.session = session;
    }
    if (attribution !== undefined) {
      payload.attribution = attribution;
    }
    if (updatedAt !== undefined) {
      payload.updated_at = normalizeUpdatedAt(updatedAt);
    }
    if (openBatchId) {
      payload.batch_id = openBatchId;
    }
    if (branch) {
      payload.branch_view = {
        head: encodeBranchSelector(branch.head),
        base: encodeBranchViewBase(branch.base),
      };
    }
    return JSON.stringify(payload);
  }

  private resolveWriteSession(session?: Session, attribution?: string): Session | undefined {
    if (session) {
      return session;
    }
    if (attribution !== undefined) {
      return undefined;
    }
    return this.resolvedSession ?? undefined;
  }

  /**
   * Insert a new row into a table without waiting for durability.
   */
  insert(
    table: string,
    values: InsertValues,
    options?: InsertOptions,
    session?: Session,
    attribution?: string,
  ): MutationResult<Row> {
    const row = this.insertInternal(table, values, options, session, attribution);
    return new MutationResult(row, committedBatchId(row), this, "mergeable");
  }

  /**
   * @internal
   */
  insertInternal(
    table: string,
    values: InsertValues,
    options?: InsertOptions,
    session?: Session,
    attribution?: string,
    openBatchId?: OpenBatchId,
  ): InsertResult {
    const effectiveSession = this.resolveWriteSession(session, attribution);
    const writeContext = this.encodeWriteContext(
      effectiveSession,
      attribution,
      openBatchId,
      options?.updatedAt,
      options?.branch ? { head: options.branch } : undefined,
    );
    const row = this.runtime.insert(table, values, writeContext, options?.id);
    return {
      ...row,
      values: row.values as Value[],
    };
  }

  /**
   * Restore a soft-deleted row with a caller-supplied id without waiting for durability.
   */
  restore(
    table: string,
    objectId: string,
    values: InsertValues,
    options?: RestoreOptions,
    session?: Session,
    attribution?: string,
  ): MutationResult<Row> {
    const row = this.restoreInternal(table, objectId, values, options, session, attribution);
    return new MutationResult(row, committedBatchId(row), this, "mergeable");
  }

  /**
   * @internal
   */
  restoreInternal(
    table: string,
    objectId: string,
    values: InsertValues,
    options?: RestoreOptions,
    session?: Session,
    attribution?: string,
    openBatchId?: OpenBatchId,
  ): InsertResult {
    const effectiveSession = this.resolveWriteSession(session, attribution);
    const writeContext = this.encodeWriteContext(
      effectiveSession,
      attribution,
      openBatchId,
      options?.updatedAt,
      options?.branch ? { head: options.branch } : undefined,
    );
    const row = this.runtime.restore(table, objectId, values, writeContext);
    return {
      ...row,
      values: row.values as Value[],
    };
  }

  /**
   * Create or update a row with a caller-supplied id without waiting for durability.
   */
  upsert(
    table: string,
    objectId: string,
    values: InsertValues,
    options?: TimestampOverrideOptions,
    session?: Session,
    attribution?: string,
  ): MutationResult<void> {
    const result = this.upsertInternal(table, objectId, values, options, session, attribution);
    return new MutationResult(undefined, committedBatchId(result), this, "mergeable");
  }

  /**
   * @internal
   */
  upsertInternal(
    table: string,
    objectId: string,
    values: InsertValues,
    options?: TimestampOverrideOptions,
    session?: Session,
    attribution?: string,
    openBatchId?: OpenBatchId,
  ): MutationReceipt {
    const effectiveSession = this.resolveWriteSession(session, attribution);
    const writeContext = this.encodeWriteContext(
      effectiveSession,
      attribution,
      openBatchId,
      options?.updatedAt,
    );
    return this.runtime.upsert(table, objectId, values, writeContext);
  }

  /**
   * Execute a query and return all matching rows.
   *
   * @param query JSON-encoded runtime query specification
   * @param options Optional read durability options
   * @returns Array of matching rows
   */
  async query(
    query: string,
    options?: InternalQueryExecutionOptions,
    session?: Session,
  ): Promise<Row[]> {
    const normalizedOptions = this.normalizeQueryExecutionOptions(options);
    const effectiveSession = session ?? this.resolvedSession;
    const sessionJson = effectiveSession ? JSON.stringify(effectiveSession) : undefined;
    const optionsJson = encodeQueryExecutionOptions(normalizedOptions);
    const results = await this.runtime.query(
      query,
      sessionJson,
      options?.runtimeSettledTier === null
        ? undefined
        : (options?.runtimeSettledTier ?? normalizedOptions.tier),
      optionsJson,
    );
    return results as Row[];
  }

  /**
   * Update a row by ID without waiting for durability.
   */
  update(
    table: string,
    objectId: string,
    updates: Record<string, Value>,
    options?: UpdateOptions,
    session?: Session,
    attribution?: string,
  ): MutationResult<void> {
    const result = this.updateInternal(
      table,
      objectId,
      updates,
      options?.updatedAt,
      session,
      attribution,
      undefined,
      options?.branch,
    );
    return new MutationResult(undefined, committedBatchId(result), this, "mergeable");
  }

  /**
   * @internal
   */
  updateInternal(
    table: string,
    objectId: string,
    updates: Record<string, Value>,
    updatedAt?: number,
    session?: Session,
    attribution?: string,
    openBatchId?: OpenBatchId,
    branch?: BranchView,
  ): MutationReceipt {
    const effectiveSession = this.resolveWriteSession(session, attribution);
    const writeContext = this.encodeWriteContext(
      effectiveSession,
      attribution,
      openBatchId,
      updatedAt,
      branch,
    );
    return this.runtime.update(table, objectId, updates, writeContext);
  }

  /**
   * Delete a row by ID without waiting for durability.
   */
  delete(
    table: string,
    objectId: string,
    options?: DeleteOptions,
    session?: Session,
    attribution?: string,
  ): MutationResult<void> {
    const result = this.deleteInternal(
      table,
      objectId,
      options?.updatedAt,
      session,
      attribution,
      undefined,
      options?.branch,
    );
    return new MutationResult(undefined, committedBatchId(result), this, "mergeable");
  }

  canInsertLocally(table: string, values: InsertValues, session?: Session): PermissionAdvice {
    if (!this.runtime.canInsertLocally) {
      throw new Error("Runtime does not support write-policy dry-run insert checks.");
    }
    return this.runtime.canInsertLocally(
      table,
      values,
      session ?? this.resolvedSession ?? undefined,
    );
  }

  canReadLocally(table: string, objectId: string, session?: Session): PermissionAdvice {
    if (!this.runtime.canReadLocally) {
      throw new Error("Runtime does not support read-policy dry-run checks.");
    }
    return this.runtime.canReadLocally(
      table,
      objectId,
      session ?? this.resolvedSession ?? undefined,
    );
  }

  canUpdateLocally(
    table: string,
    objectId: string,
    values: Record<string, Value>,
    session?: Session,
  ): PermissionAdvice {
    if (!this.runtime.canUpdateLocally) {
      throw new Error("Runtime does not support write-policy dry-run update checks.");
    }
    return this.runtime.canUpdateLocally(
      table,
      objectId,
      values,
      session ?? this.resolvedSession ?? undefined,
    );
  }

  canDeleteLocally(table: string, objectId: string, session?: Session): PermissionAdvice {
    if (!this.runtime.canDeleteLocally) {
      throw new Error("Runtime does not support write-policy dry-run delete checks.");
    }
    return this.runtime.canDeleteLocally(
      table,
      objectId,
      session ?? this.resolvedSession ?? undefined,
    );
  }

  requestInsertPermissionAdvice(
    table: string,
    values: InsertValues,
    session?: Session,
  ): Promise<PermissionAdvice> {
    return (
      this.runtime.requestInsertPermissionAdvice?.(
        table,
        values,
        session ?? this.resolvedSession ?? undefined,
      ) ?? Promise.resolve("unknown")
    );
  }

  requestReadPermissionAdvice(
    table: string,
    objectId: string,
    session?: Session,
  ): Promise<PermissionAdvice> {
    return (
      this.runtime.requestReadPermissionAdvice?.(
        table,
        objectId,
        session ?? this.resolvedSession ?? undefined,
      ) ?? Promise.resolve("unknown")
    );
  }

  requestUpdatePermissionAdvice(
    table: string,
    objectId: string,
    values: Record<string, Value>,
    session?: Session,
  ): Promise<PermissionAdvice> {
    return (
      this.runtime.requestUpdatePermissionAdvice?.(
        table,
        objectId,
        values,
        session ?? this.resolvedSession ?? undefined,
      ) ?? Promise.resolve("unknown")
    );
  }

  requestDeletePermissionAdvice(
    table: string,
    objectId: string,
    session?: Session,
  ): Promise<PermissionAdvice> {
    return (
      this.runtime.requestDeletePermissionAdvice?.(
        table,
        objectId,
        session ?? this.resolvedSession ?? undefined,
      ) ?? Promise.resolve("unknown")
    );
  }

  /**
   * @internal
   */
  deleteInternal(
    table: string,
    objectId: string,
    updatedAt?: number,
    session?: Session,
    attribution?: string,
    openBatchId?: OpenBatchId,
    branch?: BranchView,
  ): MutationReceipt {
    const effectiveSession = this.resolveWriteSession(session, attribution);
    const writeContext = this.encodeWriteContext(
      effectiveSession,
      attribution,
      openBatchId,
      updatedAt,
      branch,
    );
    return this.runtime.delete(table, objectId, writeContext);
  }

  /**
   * Subscribe to a query and receive updates when results change.
   *
   * @param query JSON-encoded runtime query specification
   * @param callback Called with delta whenever results change
   * @param options Optional read durability options
   * @returns Subscription ID for unsubscribing
   */
  subscribe(
    query: string,
    callback: SubscriptionCallback,
    options?: QueryExecutionOptions,
    session?: Session,
  ): number {
    const normalizedOptions = this.normalizeQueryExecutionOptions(options);
    const effectiveSession = session ?? this.resolvedSession;
    const sessionJson = effectiveSession ? JSON.stringify(effectiveSession) : undefined;
    const optionsJson = encodeQueryExecutionOptions(normalizedOptions);

    const handle = this.runtime.createSubscription(
      query,
      sessionJson,
      normalizedOptions.tier,
      optionsJson,
    );

    this.runtime.executeSubscription(handle, (...args: unknown[]) => {
      const deltaJsonOrObject = normalizeSubscriptionCallbackArgs(args);
      if (deltaJsonOrObject === undefined) {
        return;
      }
      if (deltaJsonOrObject instanceof Error) {
        throw deltaJsonOrObject;
      }

      const delta: SubscriptionWireDelta =
        typeof deltaJsonOrObject === "string" ? JSON.parse(deltaJsonOrObject) : deltaJsonOrObject;
      callback(delta);
    });

    return handle;
  }

  /**
   * Unsubscribe from a query.
   *
   * @param subscriptionId ID returned from subscribe()
   */
  unsubscribe(subscriptionId: number): void {
    this.runtime.unsubscribe(subscriptionId);
  }

  /**
   * Connect to a Jazz server over WebSocket using the Rust transport layer.
   *
   * Accepts an HTTP/HTTPS server URL (e.g. "http://localhost:4000") and
   * converts it to the corresponding WebSocket `/ws` endpoint URL before
   * passing it to the underlying Rust runtime's `connect()`.  Already-WS URLs
   * are passed through unchanged.
   *
   * @param url  Server URL — http(s):// or ws(s)://. `/apps/<appId>/ws` is appended automatically.
   * @param auth Authentication credentials for the connection.
   */
  connectTransport(url: string, auth: AuthConfig): void {
    this.runtime.connect(httpUrlToWs(url, this.context.appId), JSON.stringify(auth));
  }

  /**
   * Temporarily disconnect from the Jazz server without closing local runtime state.
   */
  async disconnectTransport(): Promise<void> {
    await this.runtime.disconnect({ rejectWaiters: false });
  }

  /**
   * Get the current schema.
   */
  getSchema(): WasmSchema {
    return normalizeRuntimeSchema(this.context.schema);
  }

  /** @internal Connection managers use this to attach native peer transports. */
  getRuntime(): Runtime {
    return this.runtime;
  }

  async waitForTransaction(
    batchId: BatchId | Promise<BatchId>,
    tier: DurabilityTier,
  ): Promise<void> {
    try {
      await this.runtime.waitForTransaction(batchId, tier);
    } catch (error) {
      throw this.normalizeTransactionWaitError(error);
    }
  }

  /** @internal */
  async waitForExclusiveTransaction(batchId: BatchId): Promise<void> {
    await this.waitForTransaction(batchId, this.context.serverUrl ? "global" : "local");
  }

  private normalizeTransactionWaitError(error: unknown): Error {
    return (
      rejectionFromRuntimeWaitError(error) ??
      (error instanceof Error ? error : new Error(String(error)))
    );
  }

  /**
   * Shutdown the client and release resources.
   */
  async shutdown(): Promise<void> {
    if (this.shutdownPromise) {
      return await this.shutdownPromise;
    }

    this.shutdownPromise = (async () => {
      // Close runtime if it supports explicit shutdown.
      if (this.runtime.close) {
        await this.runtime.close();
      } else {
        this.runtime.disconnect({ rejectWaiters: false });
      }
    })();

    return await this.shutdownPromise;
  }

  async clearClientStorage(): Promise<void> {
    if (!this.runtime.clearClientStorage) {
      throw new Error("Runtime does not support client storage reset.");
    }

    await this.runtime.clearClientStorage();
  }
}

/**
 * WASM module type for sync client creation.
 * This is the type of the jazz-wasm module after dynamic import.
 */
export type WasmModule = typeof import("jazz-wasm");

async function tryLoadNodePackagedWasmBinary(): Promise<Uint8Array | null> {
  const moduleBuiltin = process.getBuiltinModule?.("module");
  const fsBuiltin = process.getBuiltinModule?.("fs");
  const pathBuiltin = process.getBuiltinModule?.("path");

  if (!moduleBuiltin || !fsBuiltin || !pathBuiltin) {
    return null;
  }

  const { createRequire } = moduleBuiltin;
  const { existsSync, readFileSync } = fsBuiltin;
  const { dirname, resolve } = pathBuiltin;

  const require = createRequire(import.meta.url);
  const packageJsonPath = require.resolve("jazz-wasm/package.json");
  const packageDir = dirname(packageJsonPath);
  const wasmPath = resolve(packageDir, "pkg/jazz_wasm_bg.wasm");

  if (!existsSync(wasmPath)) {
    return null;
  }

  return readFileSync(wasmPath);
}

/**
 * Load and initialize the WASM module.
 *
 * Exported so that `createDb()` can pre-load the module for sync mutations.
 */
export async function loadWasmModule(runtime?: RuntimeSourcesConfig): Promise<WasmModule> {
  // Cast to any — wasm-bindgen glue exports (default, initSync) aren't in .d.ts
  const wasmModule: any = await import("jazz-wasm");
  const syncInitInput = resolveRuntimeConfigSyncInitInput(runtime);

  if (syncInitInput) {
    wasmModule.initSync(syncInitInput);
    return wasmModule;
  }

  // In Node.js, we need to read the .wasm file and use initSync.
  // In browsers/React Native, the default fetch-based init works (or default()).
  // Use try/catch so we skip the Node path when node:* modules are unavailable (e.g. RN).
  let nodeInitDone = false;
  if (typeof process !== "undefined" && process.versions?.node) {
    try {
      const wasmBinary = await tryLoadNodePackagedWasmBinary();
      if (wasmBinary) {
        wasmModule.initSync({ module: wasmBinary });
        nodeInitDone = true;
      }
    } catch {
      // Node modules unavailable (e.g. React Native with process polyfill)
    }
  }
  if (!nodeInitDone && typeof wasmModule.default === "function") {
    const wasmUrl =
      typeof location !== "undefined"
        ? resolveRuntimeConfigWasmUrl(import.meta.url, location.href, runtime)
        : null;

    if (wasmUrl) {
      await wasmModule.default({ module_or_path: wasmUrl });
    } else {
      await wasmModule.default();
    }
  }

  return wasmModule;
}
