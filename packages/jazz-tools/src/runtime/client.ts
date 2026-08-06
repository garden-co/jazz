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
  ): MutationResult;
  upsert(
    table: string,
    object_id: string,
    values: InsertValues,
    write_context_json?: string | null,
  ): MutationResult;
  delete(table: string, object_id: string, write_context_json?: string | null): MutationResult;
  canInsert?(table: string, values: InsertValues, session?: Session): boolean;
  canRead?(table: string, objectId: string, session?: Session): boolean;
  canUpdate?(
    table: string,
    objectId: string,
    values: Record<string, Value>,
    session?: Session,
  ): boolean;
  canDelete?(table: string, objectId: string, session?: Session): boolean;
  waitForTransaction(transactionId: string, tier: string): Promise<void>;
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
  /** Disconnect from the Jazz server and drop the transport handle. */
  disconnect(options?: { rejectWaiters?: boolean }): void | Promise<void>;
  /** Push updated auth credentials into the live Rust transport. */
  updateAuth(auth_json: string): void;
  /** Register a callback invoked when the Rust transport rejects the JWT. */
  onAuthFailure(callback: (reason: string) => void): void;
}

export interface TransactionalRuntime extends Runtime {
  beginTransaction(transactionKind: TransactionKind): string;
  commitTransaction(transactionId: string): void;
  rollbackTransaction(transactionId: string): boolean;
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
export interface QueryExecutionOptions {
  tier?: DurabilityTier;
  localUpdates?: LocalUpdatesMode;
  /**
   * Whether this subscription should cause upstream forwarding.
   *
   * Defaults to true. Set to false for inspector/helper subscriptions that
   * should observe local state without opening remote coverage.
   */
  propagate?: boolean;
  propagation?: QueryPropagation;
  visibility?: QueryVisibility;
  /**
   * In dual-mode clients, route this subscription over the API-level
   * async subscription channel instead of the local main-thread node.
   * Queries and writes are unaffected.
   */
  subscriptionMode?: "sync" | "async";
}

type InternalQueryExecutionOptions = QueryExecutionOptions & {
  transactionId?: string;
  runtimeSettledTier?: DurabilityTier | null;
};

export interface ResolvedQueryExecutionOptions {
  tier: DurabilityTier;
  localUpdates: LocalUpdatesMode;
  propagation: QueryPropagation;
  visibility: QueryVisibility;
}

type ResolvedInternalQueryExecutionOptions = ResolvedQueryExecutionOptions & {
  transactionId?: string;
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
      transactionId: TransactionId;
    }
  | {
      kind: "rejected";
      transactionId: TransactionId;
      code: string;
      reason: string;
    }
  | {
      kind: "accepted";
      transactionId: TransactionId;
      confirmedTier: DurabilityTier;
    };

export interface LocalTransactionRecord {
  transactionId: TransactionId;
  kind: TransactionKind;
  sealed: boolean;
  latestSettlement: TransactionFate | null;
  encodedRecord?: Uint8Array;
}

export interface CreateOptions extends TimestampOverrideOptions {
  id?: string;
}

export interface UpsertOptions extends TimestampOverrideOptions {
  id: string;
}

export interface UpdateOptions extends TimestampOverrideOptions {}

export interface DeleteOptions extends TimestampOverrideOptions {}

export interface RestoreOptions extends TimestampOverrideOptions {}

/**
 * Query row result.
 */
export interface Row {
  id: string;
  values: Value[];
}

export interface InsertResult extends Row {
  transactionId: TransactionId;
}

export interface MutationResult {
  transactionId: TransactionId;
}

interface WriteContextPayload {
  session?: Session;
  attribution?: string;
  updated_at?: number;
  batch_id?: string;
  target_branch_name?: string;
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
    propagation: options?.propagation ?? (options?.propagate === false ? "local-only" : "full"),
    visibility: options?.visibility ?? "public",
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
  } = {};
  if ((options.propagation ?? "full") !== "full") {
    payload.propagation = options.propagation;
  }
  if ((options.localUpdates ?? "immediate") !== "immediate") {
    payload.local_updates = options.localUpdates;
  }
  if (options.transactionId) {
    payload.transaction_batch_id = options.transactionId;
  }

  if (!payload.propagation && !payload.local_updates && !payload.transaction_batch_id) {
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

type TransactionId = string;

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
  return new PersistedWriteRejectedError(candidate.transactionId, candidate.code, candidate.reason);
}

/**
 * Error returned when a write fails to be persisted at a given durability tier.
 */
export class PersistedWriteRejectedError extends Error {
  readonly name = "PersistedWriteRejectedError";

  constructor(
    readonly transactionId: TransactionId,
    readonly code: string,
    readonly reason: string,
  ) {
    super(`Persisted transaction ${transactionId} was rejected (${code}): ${reason}`);
  }
}

/**
 * Returned by upsert, update, delete, and transaction operations.
 * Allows waiting for the write to be persisted at a given durability tier.
 */
export class WriteHandle<T = void> {
  readonly #client: JazzClient;

  constructor(
    readonly transactionId: TransactionId,
    client: JazzClient,
  ) {
    this.#client = client;
  }

  /**
   * Wait for the write to be persisted at a given durability tier.
   *
   * Rejects with a {@link PersistedWriteRejectedError} if the write is rejected.
   */
  async wait(options: { tier: DurabilityTier }): Promise<T> {
    return this.#client.waitForTransaction(this.transactionId, options.tier) as Promise<T>;
  }

  protected client(): JazzClient {
    return this.#client;
  }
}

/**
 * Returned by insert operations and auto-committed transactions.
 * Allows getting the inserted value and waiting for the write
 * to be persisted at a given durability tier.
 */
export class WriteResult<T> extends WriteHandle<T> {
  constructor(
    readonly value: T,
    transactionId: TransactionId,
    client: JazzClient,
  ) {
    super(transactionId, client);
  }

  /**
   * Wait for the write to be persisted at a given durability tier.
   *
   * Rejects with a {@link PersistedWriteRejectedError} if the write is rejected.
   * @returns the inserted row.
   */
  override async wait(options: { tier: DurabilityTier }): Promise<T> {
    await super.wait(options);
    return this.value;
  }

  mapValue<U>(transformValue: (value: T) => U): WriteResult<U> {
    return new WriteResult(transformValue(this.value), this.transactionId, this.client());
  }
}

/**
 * Returned by explicitly-committed exclusive transactions.
 *
 * Exclusive transactions are accepted or rejected by the global authority, so
 * callers do not choose a durability tier when waiting for confirmation.
 */
export class ExclusiveWriteHandle extends WriteHandle<void> {
  /**
   * Wait for the exclusive transaction to be accepted or rejected by the authority.
   *
   * Rejects with a {@link PersistedWriteRejectedError} if the transaction is rejected.
   */
  override async wait(): Promise<void> {
    await this.client().waitForExclusiveTransaction(this.transactionId);
  }
}

/**
 * Returned by auto-committed exclusive transactions.
 */
export class ExclusiveWriteResult<T> extends WriteResult<T> {
  /**
   * Wait for the exclusive transaction to be accepted or rejected by the authority.
   *
   * Rejects with a {@link PersistedWriteRejectedError} if the transaction is rejected.
   * @returns the callback result.
   */
  override async wait(): Promise<T> {
    await this.client().waitForExclusiveTransaction(this.transactionId);
    return this.value;
  }

  override mapValue<U>(transformValue: (value: T) => U): ExclusiveWriteResult<U> {
    return new ExclusiveWriteResult(transformValue(this.value), this.transactionId, this.client());
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

  beginTransaction(kind: TransactionKind): TransactionId {
    return requireTransactionalRuntime(this.runtime).beginTransaction(kind);
  }

  commitTransaction(transactionId: TransactionId): WriteHandle {
    requireTransactionalRuntime(this.runtime).commitTransaction(transactionId);
    return new WriteHandle(transactionId, this);
  }

  rollbackTransaction(transactionId: TransactionId): void {
    requireTransactionalRuntime(this.runtime).rollbackTransaction(transactionId);
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
    if (!options?.transactionId) {
      return resolved;
    }
    return {
      ...resolved,
      transactionId: options.transactionId,
    };
  }

  private encodeWriteContext(
    session?: Session,
    attribution?: string,
    transactionId?: TransactionId,
    updatedAt?: number,
  ): string | undefined {
    if (!session && attribution === undefined && !transactionId && updatedAt === undefined) {
      return undefined;
    }
    if (attribution === undefined && session && !transactionId && updatedAt === undefined) {
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
    if (transactionId) {
      payload.batch_id = transactionId;
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
    options?: CreateOptions,
    session?: Session,
    attribution?: string,
  ): WriteResult<Row> {
    const row = this.insertInternal(table, values, options, session, attribution);
    return new WriteResult(row, row.transactionId, this);
  }

  /**
   * @internal
   */
  insertInternal(
    table: string,
    values: InsertValues,
    options?: CreateOptions,
    session?: Session,
    attribution?: string,
    transactionId?: TransactionId,
  ): InsertResult {
    const effectiveSession = this.resolveWriteSession(session, attribution);
    const writeContext = this.encodeWriteContext(
      effectiveSession,
      attribution,
      transactionId,
      options?.updatedAt,
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
  ): WriteResult<Row> {
    const row = this.restoreInternal(table, objectId, values, options, session, attribution);
    return new WriteResult(row, row.transactionId, this);
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
    transactionId?: TransactionId,
  ): InsertResult {
    const effectiveSession = this.resolveWriteSession(session, attribution);
    const writeContext = this.encodeWriteContext(
      effectiveSession,
      attribution,
      transactionId,
      options?.updatedAt,
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
    values: InsertValues,
    options: UpsertOptions,
    session?: Session,
    attribution?: string,
  ): WriteHandle {
    const result = this.upsertInternal(table, values, options, session, attribution);
    return new WriteHandle(result.transactionId, this);
  }

  /**
   * @internal
   */
  upsertInternal(
    table: string,
    values: InsertValues,
    options: UpsertOptions,
    session?: Session,
    attribution?: string,
    transactionId?: TransactionId,
  ): MutationResult {
    const effectiveSession = this.resolveWriteSession(session, attribution);
    const writeContext = this.encodeWriteContext(
      effectiveSession,
      attribution,
      transactionId,
      options.updatedAt,
    );
    return this.runtime.upsert(table, options.id, values, writeContext);
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
  ): WriteHandle {
    const result = this.updateInternal(
      table,
      objectId,
      updates,
      options?.updatedAt,
      session,
      attribution,
      undefined,
    );
    return new WriteHandle(result.transactionId, this);
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
    transactionId?: TransactionId,
  ): MutationResult {
    const effectiveSession = this.resolveWriteSession(session, attribution);
    const writeContext = this.encodeWriteContext(
      effectiveSession,
      attribution,
      transactionId,
      updatedAt,
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
  ): WriteHandle {
    const result = this.deleteInternal(table, objectId, options?.updatedAt, session, attribution);
    return new WriteHandle(result.transactionId, this);
  }

  canInsert(table: string, values: InsertValues, session?: Session): boolean {
    if (!this.runtime.canInsert) {
      throw new Error("Runtime does not support write-policy dry-run insert checks.");
    }
    return this.runtime.canInsert(table, values, session ?? this.resolvedSession ?? undefined);
  }

  canRead(table: string, objectId: string, session?: Session): boolean {
    if (!this.runtime.canRead) {
      throw new Error("Runtime does not support read-policy dry-run checks.");
    }
    return this.runtime.canRead(table, objectId, session ?? this.resolvedSession ?? undefined);
  }

  canUpdate(
    table: string,
    objectId: string,
    values: Record<string, Value>,
    session?: Session,
  ): boolean {
    if (!this.runtime.canUpdate) {
      throw new Error("Runtime does not support write-policy dry-run update checks.");
    }
    return this.runtime.canUpdate(
      table,
      objectId,
      values,
      session ?? this.resolvedSession ?? undefined,
    );
  }

  canDelete(table: string, objectId: string, session?: Session): boolean {
    if (!this.runtime.canDelete) {
      throw new Error("Runtime does not support write-policy dry-run delete checks.");
    }
    return this.runtime.canDelete(table, objectId, session ?? this.resolvedSession ?? undefined);
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
    transactionId?: TransactionId,
  ): MutationResult {
    const effectiveSession = this.resolveWriteSession(session, attribution);
    const writeContext = this.encodeWriteContext(
      effectiveSession,
      attribution,
      transactionId,
      updatedAt,
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

  async waitForTransaction(transactionId: TransactionId, tier: DurabilityTier): Promise<void> {
    try {
      await this.runtime.waitForTransaction(transactionId, tier);
    } catch (error) {
      throw this.normalizeTransactionWaitError(error);
    }
  }

  /** @internal */
  async waitForExclusiveTransaction(transactionId: TransactionId): Promise<void> {
    await this.waitForTransaction(transactionId, this.context.serverUrl ? "global" : "local");
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
