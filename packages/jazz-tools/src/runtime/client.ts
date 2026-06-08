/**
 * JazzClient - High-level TypeScript client for Jazz.
 *
 * Wraps the WASM runtime and provides a clean API for CRUD operations,
 * subscriptions, and sync.
 */

import type { AppContext, RuntimeSourcesConfig, Session } from "./context.js";
import type { InsertValues, Value, SubscriptionWireDelta, WasmSchema } from "../drivers/types.js";
import { normalizeRuntimeSchema, serializeRuntimeSchema } from "../drivers/schema-wire.js";
import type { AuthFailureReason } from "./sync-transport.js";
import { resolveClientSessionStateSync } from "./client-session.js";
import { mapAuthReason } from "./auth-state.js";
import { translateQuery } from "./query-adapter.js";
import { isHiddenIncludeColumnName, resolveSelectedColumns } from "./select-projection.js";
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
 * Common interface for WASM and NAPI runtimes.
 *
 * Both `WasmRuntime` (from jazz-wasm) and `NapiRuntime` (from jazz-napi)
 * satisfy this interface, allowing `JazzClient` to work with either backend.
 */
export interface Runtime {
  insert(
    table: string,
    values: InsertValues,
    write_context_json?: string | null,
    object_id?: string | null,
  ): DirectInsertResult;
  restore(
    table: string,
    object_id: string,
    values: InsertValues,
    write_context_json?: string | null,
  ): DirectInsertResult;
  update(
    object_id: string,
    values: Record<string, Value>,
    write_context_json?: string | null,
  ): DirectMutationResult;
  upsert(
    table: string,
    object_id: string,
    values: InsertValues,
    write_context_json?: string | null,
  ): DirectMutationResult;
  delete(object_id: string, write_context_json?: string | null): DirectMutationResult;
  onMutationError(callback: (event: MutationErrorEvent) => void): void;
  beginBatch(batch_mode: BatchMode): string;
  commitBatch(batch_id: string): void;
  waitForBatch(batch_id: string, tier: string): Promise<void>;
  rollbackBatch(batch_id: string): boolean;
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
  /**
   * Construct a Rust-owned worker bridge attached to this runtime. Returns
   * an opaque handle that the TS `WorkerBridge` adapter wraps. WASM-only.
   * Options are parsed at attach time; `bridge.init()` is parameter-less.
   */
  createWorkerBridge?(worker: Worker, options: object): unknown;
  /** Drive a synchronous batched tick. Used by callers that need to flush
   * pending state before a synchronous teardown. */
  batchedTick?(): void;
  getSchema(): any;
  getSchemaHash(): string;
  close?(): void | Promise<void>;
  /** Connect to a Jazz server over WebSocket (Rust transport). */
  connect?(url: string, auth_json: string): void;
  /** Disconnect from the Jazz server and drop the transport handle. */
  disconnect?(): void;
  /** Push updated auth credentials into the live Rust transport. */
  updateAuth?(auth_json: string): void;
  /** Register a callback invoked when the Rust transport rejects the JWT. */
  onAuthFailure?(callback: (reason: string) => void): void;
}

/**
 * Authentication configuration for connecting to a Jazz server.
 *
 * Maps directly to the Rust `AuthConfig` struct in `jazz-tools/src/transport_manager.rs`.
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
  propagation?: QueryPropagation;
  visibility?: QueryVisibility;
}

type InternalQueryExecutionOptions = QueryExecutionOptions & {
  transactionBatchId?: string;
  runtimeSettledTier?: DurabilityTier | null;
};

export interface ResolvedQueryExecutionOptions {
  tier: DurabilityTier;
  localUpdates: LocalUpdatesMode;
  propagation: QueryPropagation;
  visibility: QueryVisibility;
}

type ResolvedInternalQueryExecutionOptions = ResolvedQueryExecutionOptions & {
  transactionBatchId?: string;
};

interface TimestampOverrideOptions {
  updatedAt?: number;
}

export type BatchMode = "direct" | "transactional";

export type BatchFate =
  | {
      kind: "missing";
      batchId: BatchId;
    }
  | {
      kind: "rejected";
      batchId: BatchId;
      code: string;
      reason: string;
    }
  | {
      kind: "durableDirect";
      batchId: BatchId;
      confirmedTier: DurabilityTier;
    }
  | {
      kind: "acceptedTransaction";
      batchId: BatchId;
      confirmedTier: DurabilityTier;
    };

export interface LocalBatchRecord {
  batchId: BatchId;
  mode: BatchMode;
  sealed: boolean;
  latestSettlement: BatchFate | null;
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
 * A mutation error event emitted by {@link JazzClient.onMutationError}.
 * Contains enough information to understand the cause of the error and
 * correlate it with a specific mutation.
 */
export interface MutationErrorEvent {
  code: string;
  reason: string;
  batch: LocalBatchRecord;
}

/**
 * Query row result.
 */
export interface Row {
  id: string;
  values: Value[];
}

export interface DirectInsertResult extends Row {
  batchId: BatchId;
}

export interface DirectMutationResult {
  batchId: BatchId;
}

interface WriteContextPayload {
  session?: Session;
  attribution?: string;
  updated_at?: number;
  batch_mode?: BatchMode;
  batch_id?: string;
  target_branch_name?: string;
}

/**
 * Subscription callback type.
 */
export type SubscriptionCallback = (delta: SubscriptionWireDelta) => void;

export interface ConnectSyncRuntimeOptions {
  useBinaryEncoding?: boolean;
  onAuthFailure?: (reason: AuthFailureReason) => void;
  nonDurableClientRuntime?: boolean;
}

/**
 * QueryBuilder-compatible input accepted by query and subscribe APIs.
 */
export interface QueryInput {
  _build(): string;
  /** Optional schema metadata available on generated QueryBuilder objects. */
  _schema?: WasmSchema;
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
  };
}

type RelationIrNode = Record<string, unknown>;
type ArraySubqueryPlan = {
  table: string;
  selectColumns: string[];
  nested: ArraySubqueryPlan[];
};

function resolveQueryJson(query: string | QueryInput): string {
  if (typeof query === "string") {
    return query;
  }

  const builtQuery = query._build();
  const schema = query._schema;
  if (!schema || typeof schema !== "object" || Array.isArray(schema)) {
    return builtQuery;
  }

  // Query payloads already in runtime form include relation_ir and should pass through unchanged.
  try {
    const parsed = JSON.parse(builtQuery) as Record<string, unknown>;
    if (parsed && typeof parsed === "object" && "relation_ir" in parsed) {
      return builtQuery;
    }
  } catch {
    return builtQuery;
  }

  return translateQuery(builtQuery, schema);
}

function resolveRelationIrOutputTable(node: unknown): string | null {
  if (!node || typeof node !== "object") {
    return null;
  }

  const relation = node as RelationIrNode;

  if ("TableScan" in relation) {
    const tableScan = relation.TableScan as { table?: unknown } | undefined;
    return typeof tableScan?.table === "string" ? tableScan.table : null;
  }

  if ("Filter" in relation) {
    return resolveRelationIrOutputTable(
      (relation.Filter as { input?: unknown } | undefined)?.input,
    );
  }

  if ("OrderBy" in relation) {
    return resolveRelationIrOutputTable(
      (relation.OrderBy as { input?: unknown } | undefined)?.input,
    );
  }

  if ("Limit" in relation) {
    return resolveRelationIrOutputTable((relation.Limit as { input?: unknown } | undefined)?.input);
  }

  if ("Offset" in relation) {
    return resolveRelationIrOutputTable(
      (relation.Offset as { input?: unknown } | undefined)?.input,
    );
  }

  if ("Project" in relation) {
    return resolveRelationIrOutputTable(
      (relation.Project as { input?: unknown } | undefined)?.input,
    );
  }

  if ("Gather" in relation) {
    const gather = relation.Gather as { seed?: unknown } | undefined;
    return resolveRelationIrOutputTable(gather?.seed);
  }

  return null;
}

function parseArraySubqueryPlans(value: unknown): ArraySubqueryPlan[] {
  if (!Array.isArray(value)) {
    return [];
  }

  const plans: ArraySubqueryPlan[] = [];
  for (const entry of value) {
    if (typeof entry !== "object" || entry === null) {
      continue;
    }
    const plan = entry as {
      table?: unknown;
      select_columns?: unknown;
      nested_arrays?: unknown;
    };
    if (typeof plan.table !== "string") {
      continue;
    }
    plans.push({
      table: plan.table,
      selectColumns: Array.isArray(plan.select_columns)
        ? plan.select_columns.filter((column): column is string => typeof column === "string")
        : [],
      nested: parseArraySubqueryPlans(plan.nested_arrays),
    });
  }

  return plans;
}

function resolveQueryAlignmentPlan(queryJson: string): {
  outputTable: string | null;
  arraySubqueries: ArraySubqueryPlan[];
  selectColumns: string[];
} {
  try {
    const parsed = JSON.parse(queryJson) as {
      table?: unknown;
      relation_ir?: unknown;
      array_subqueries?: unknown;
      select_columns?: unknown;
    };
    return {
      outputTable:
        typeof parsed.table === "string"
          ? parsed.table
          : resolveRelationIrOutputTable(parsed.relation_ir),
      arraySubqueries: parseArraySubqueryPlans(parsed.array_subqueries),
      selectColumns: Array.isArray(parsed.select_columns)
        ? parsed.select_columns.filter((column): column is string => typeof column === "string")
        : [],
    };
  } catch {
    return {
      outputTable: null,
      arraySubqueries: [],
      selectColumns: [],
    };
  }
}

function resolveNodeTier(tier: AppContext["tier"]): string | undefined {
  if (!tier) return undefined;
  if (Array.isArray(tier)) {
    return tier[0];
  }
  return tier;
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
  if (options.transactionBatchId) {
    payload.transaction_batch_id = options.transactionBatchId;
  }

  if (!payload.propagation && !payload.local_updates && !payload.transaction_batch_id) {
    return undefined;
  }

  return JSON.stringify(payload);
}

function normalizeSubscriptionCallbackArgs(
  args: unknown[],
): SubscriptionWireDelta | string | undefined {
  if (args.length === 1) {
    return args[0] as SubscriptionWireDelta | string;
  }

  if (args.length === 2 && args[0] == null) {
    return args[1] as SubscriptionWireDelta | string | undefined;
  }

  console.error("Invalid subscription callback arguments", args);
  return undefined;
}

type BatchId = string;

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
    batchId?: unknown;
    code?: unknown;
    reason?: unknown;
  };
  if (candidate.kind !== "rejected") {
    return null;
  }
  if (
    typeof candidate.code !== "string" ||
    typeof candidate.reason !== "string" ||
    typeof candidate.batchId !== "string"
  ) {
    return null;
  }
  return new PersistedWriteRejectedError(candidate.batchId, candidate.code, candidate.reason);
}

/**
 * Error returned when a write fails to be persisted at a given durability tier.
 */
export class PersistedWriteRejectedError extends Error {
  readonly name = "PersistedWriteRejectedError";

  constructor(
    readonly batchId: BatchId,
    readonly code: string,
    readonly reason: string,
  ) {
    super(`Persisted batch ${batchId} was rejected (${code}): ${reason}`);
  }
}

/**
 * Returned by upsert, update, and delete operations, and explicitly-committed transactions.
 * Allows waiting for the write to be persisted at a given durability tier.
 */
export class WriteHandle<T = void> {
  readonly #client: JazzClient;

  constructor(
    readonly batchId: BatchId,
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
    return this.#client.waitForBatch(this.batchId, options.tier) as Promise<T>;
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
    batchId: BatchId,
    client: JazzClient,
  ) {
    super(batchId, client);
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
    return new WriteResult(transformValue(this.value), this.batchId, this.client());
  }
}

function isPromiseLike<T>(value: T | PromiseLike<T>): value is PromiseLike<T> {
  return (
    value !== null &&
    (typeof value === "object" || typeof value === "function") &&
    typeof (value as PromiseLike<T>).then === "function"
  );
}

type RunInBatchResult<TResult> =
  TResult extends PromiseLike<unknown>
    ? Promise<WriteResult<Awaited<TResult>>>
    : WriteResult<TResult>;

export type Scoped<TBatchOrTx> = Omit<TBatchOrTx, "commit" | "rollback">;

function createBatchScope<TBatchOrTx extends object>(batchOrTx: TBatchOrTx): Scoped<TBatchOrTx> {
  return new Proxy(batchOrTx, {
    get(target, property) {
      if (property === "commit" || property === "rollback") {
        return undefined;
      }

      const value = Reflect.get(target, property, target);
      return typeof value === "function" ? value.bind(target) : value;
    },
    has(target, property) {
      if (property === "commit" || property === "rollback") {
        return false;
      }

      return Reflect.has(target, property);
    },
    set(target, property, value) {
      return Reflect.set(target, property, value, target);
    },
  }) as Scoped<TBatchOrTx>;
}

function rollback(batchOrTx: { rollback: () => void }): void {
  try {
    batchOrTx.rollback();
  } catch {
    // Preserve the original callback error.
  }
}

export function runInBatch<
  TBatchOrTx extends { commit(): WriteHandle; rollback: () => void },
  TResult,
>(
  batchOrTx: TBatchOrTx,
  callback: (target: Scoped<TBatchOrTx>) => TResult,
  client: JazzClient | (() => JazzClient),
): RunInBatchResult<TResult> {
  let value: TResult;
  try {
    const scope = createBatchScope(batchOrTx);
    value = callback(scope);
  } catch (error) {
    rollback(batchOrTx);
    throw error;
  }
  const resultClient = typeof client === "function" ? client : () => client;
  if (isPromiseLike(value)) {
    return value.then(
      (resolvedValue) => {
        const committed = batchOrTx.commit();
        return new WriteResult(
          resolvedValue as Awaited<TResult>,
          committed.batchId,
          resultClient(),
        );
      },
      (error) => {
        rollback(batchOrTx);
        throw error;
      },
    ) as RunInBatchResult<TResult>;
  }
  const committed = batchOrTx.commit();
  return new WriteResult(value, committed.batchId, resultClient()) as RunInBatchResult<TResult>;
}

abstract class BatchHandleBase {
  constructor(
    private readonly client: JazzClient,
    readonly batchId: BatchId,
    private readonly session?: Session,
    private readonly attribution?: string,
  ) {}

  commit(): WriteHandle {
    return this.client.commitBatch(this.batchId);
  }

  rollback(): void {
    this.client.rollbackBatch(this.batchId);
  }

  create(table: string, values: InsertValues, options?: CreateOptions): Row {
    const row = this.client.createInternal(
      table,
      values,
      options,
      this.session,
      this.attribution,
      this.batchId,
    );
    return row;
  }

  restore(table: string, objectId: string, values: InsertValues, options?: RestoreOptions): Row {
    const row = this.client.restoreInternal(
      table,
      objectId,
      values,
      options,
      this.session,
      this.attribution,
      this.batchId,
    );
    return row;
  }

  upsert(table: string, values: InsertValues, options: UpsertOptions): void {
    this.client.upsertInternal(
      table,
      values,
      options,
      this.session,
      this.attribution,
      this.batchId,
    );
  }

  update(objectId: string, updates: Record<string, Value>): void {
    this.client.updateInternal(
      objectId,
      updates,
      undefined,
      this.session,
      this.attribution,
      this.batchId,
    );
  }

  delete(objectId: string): void {
    this.client.deleteInternal(objectId, undefined, this.session, this.attribution, this.batchId);
  }

  async query(query: string | QueryInput, options?: QueryExecutionOptions): Promise<Row[]> {
    return this.client.query(
      query,
      {
        ...options,
        localUpdates: "deferred",
        transactionBatchId: this.batchId,
      },
      this.session,
    );
  }
}

/**
 * Transactions group a set of writes that should settle together after an authority validates them.
 *
 * Data read and written through this transaction is scoped to it, and will only be
 * globally visible once it's committed and accepted by the authority.
 */
export class Transaction extends BatchHandleBase {
  constructor(client: JazzClient, batchId: BatchId, session?: Session, attribution?: string) {
    super(client, batchId, session, attribution);
  }
}

/**
 * Direct batches group a set of writes under one batch id and publish them when committed.
 */
export class DirectBatch extends BatchHandleBase {
  constructor(client: JazzClient, batchId: BatchId, session?: Session, attribution?: string) {
    super(client, batchId, session, attribution);
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
  private cachedRuntimeSchemaHash: string | null = null;
  private cachedRuntimeSchema: WasmSchema | null = null;

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
  } {
    const payload: {
      jwt_token: string | null;
      admin_secret?: string;
      backend_secret?: string;
    } = { jwt_token: this.context.jwtToken ?? null };
    if (this.context.adminSecret) {
      payload.admin_secret = this.context.adminSecret;
    }
    if (this.context.backendSecret) {
      payload.backend_secret = this.context.backendSecret;
    }
    return payload;
  }

  private constructor(
    runtime: Runtime,
    context: AppContext,
    defaultDurabilityTier: DurabilityTier,
    runtimeOptions?: ConnectSyncRuntimeOptions,
  ) {
    this.runtime = runtime;
    this.scheduler = getScheduler();
    this.context = context;
    this.defaultDurabilityTier = defaultDurabilityTier;
    this.resolvedSession = this.resolveSessionFromContext();

    if (runtimeOptions?.onAuthFailure) {
      const handler = runtimeOptions.onAuthFailure;
      this.runtime.onAuthFailure?.((reason: string) => {
        handler(mapAuthReason(reason));
      });
    }

    this.runtime.onMutationError((event) => {
      console.error("Unhandled Jazz mutation error", event);
    });
  }

  /**
   * Create client synchronously with a pre-loaded WASM module.
   *
   * Use this after loading WASM via `loadWasmModule()` to avoid
   * async client creation. This enables sync mutations in the Db class.
   *
   * @param wasmModule Pre-loaded WASM module from loadWasmModule()
   * @param context Application context with driver and schema
   * @returns Connected JazzClient instance (created synchronously)
   */
  static connectSync(
    wasmModule: WasmModule,
    context: AppContext,
    runtimeOptions?: ConnectSyncRuntimeOptions,
  ): JazzClient {
    // Create WASM runtime (storage is now synchronous in-memory)
    const schemaJson = serializeRuntimeSchema(context.schema);
    const runtime = new wasmModule.WasmRuntime(
      schemaJson,
      context.appId,
      context.env ?? "dev",
      context.userBranch ?? "main",
      resolveNodeTier(context.tier),
      runtimeOptions?.useBinaryEncoding ?? false,
      runtimeOptions?.nonDurableClientRuntime ?? false,
    );

    return new JazzClient(runtime, context, resolveDefaultDurabilityTier(context), runtimeOptions);
  }

  /**
   * Create client from a pre-constructed runtime (e.g., NapiRuntime).
   *
   * This allows server-side apps to use the native NAPI backend directly
   * without WASM loading.
   *
   * @param runtime A runtime implementing the Runtime interface
   * @param context Application context
   * @returns Connected JazzClient instance
   */
  static connectWithRuntime(
    runtime: Runtime,
    context: AppContext,
    runtimeOptions?: ConnectSyncRuntimeOptions,
  ): JazzClient {
    return new JazzClient(runtime, context, resolveDefaultDurabilityTier(context), runtimeOptions);
  }

  private createBatch(batchMode: BatchMode): BatchId {
    return this.runtime.beginBatch(batchMode);
  }

  beginTransaction(session?: Session, attribution?: string): Transaction {
    return new Transaction(
      this,
      this.createBatch("transactional"),
      this.resolveWriteSession(session, attribution),
      attribution,
    );
  }

  beginBatch(session?: Session, attribution?: string): DirectBatch {
    return new DirectBatch(
      this,
      this.createBatch("direct"),
      this.resolveWriteSession(session, attribution),
      attribution,
    );
  }

  onMutationError(listener: (event: MutationErrorEvent) => void): void {
    this.runtime.onMutationError(listener);
  }

  commitBatch(batchId: BatchId): WriteHandle {
    this.runtime.commitBatch(batchId);
    return new WriteHandle(batchId, this);
  }

  rollbackBatch(batchId: BatchId): void {
    this.runtime.rollbackBatch(batchId);
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
    // Push the refreshed credentials into the Rust transport. `updateAuth`
    // is optional on the Runtime interface because not every binding exposes
    // it yet; bindings that do will route this to TransportControl::UpdateAuth.
    // Carry forward admin/backend secrets from context — omitting them here
    // would deserialise to None on the Rust side and silently erase any
    // privileged credentials the transport was connected with.
    this.runtime.updateAuth?.(JSON.stringify(this.buildTransportAuthPayload()));
  }

  updateCookieSession(cookieSession?: Session): void {
    this.context.cookieSession = cookieSession;
    this.resolvedSession = this.resolveSessionFromContext();
    this.runtime.updateAuth?.(JSON.stringify(this.buildTransportAuthPayload()));
  }

  private normalizeQueryExecutionOptions(
    options?: InternalQueryExecutionOptions,
  ): ResolvedInternalQueryExecutionOptions {
    const resolved = resolveEffectiveQueryExecutionOptions(
      { ...this.context, defaultDurabilityTier: this.defaultDurabilityTier },
      options,
    );
    if (!options?.transactionBatchId) {
      return resolved;
    }
    return {
      ...resolved,
      transactionBatchId: options.transactionBatchId,
    };
  }

  private encodeWriteContext(
    session?: Session,
    attribution?: string,
    batchId?: BatchId,
    updatedAt?: number,
  ): string | undefined {
    if (!session && attribution === undefined && !batchId && updatedAt === undefined) {
      return undefined;
    }
    if (attribution === undefined && session && !batchId && updatedAt === undefined) {
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
    if (batchId) {
      payload.batch_id = batchId;
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

  private alignRowValuesToDeclaredSchema(
    table: string,
    values: Value[],
    runtimeSchema?: WasmSchema,
    arraySubqueries: ArraySubqueryPlan[] = [],
    selectColumns: string[] = [],
  ): Value[] {
    const effectiveRuntimeSchema = runtimeSchema ?? this.getSchema();
    const declaredTable = this.context.schema[table];
    const runtimeTable = effectiveRuntimeSchema[table];

    if (!declaredTable || !runtimeTable) {
      return values;
    }

    const projectedVisibleColumnCount =
      selectColumns.length > 0
        ? resolveSelectedColumns(table, this.context.schema, selectColumns).filter(
            (columnName) => !isHiddenIncludeColumnName(columnName),
          ).length
        : 0;

    if (projectedVisibleColumnCount > 0) {
      if (values.length < projectedVisibleColumnCount) {
        return values;
      }

      const projectedValues = values.slice(0, projectedVisibleColumnCount);
      const trailingValues = values.slice(projectedVisibleColumnCount);
      if (arraySubqueries.length === 0) {
        return projectedValues.concat(trailingValues);
      }

      const alignedTrailingValues = trailingValues.map((value, index) => {
        const plan = arraySubqueries[index];
        if (!plan) {
          return value;
        }
        return this.alignIncludedValueToDeclaredSchema(value, plan, effectiveRuntimeSchema);
      });

      return projectedValues.concat(alignedTrailingValues);
    }

    if (values.length < runtimeTable.columns.length) {
      return values;
    }

    const valuesByColumn = new Map<string, Value>();
    for (let index = 0; index < runtimeTable.columns.length; index += 1) {
      const column = runtimeTable.columns[index];
      if (!column) {
        return values;
      }
      const value = values[index];
      if (value === undefined) {
        return values;
      }
      valuesByColumn.set(column.name, value);
    }

    const reorderedValues: Value[] = [];
    for (const column of declaredTable.columns) {
      const value = valuesByColumn.get(column.name);
      if (value === undefined) {
        return values;
      }
      reorderedValues.push(value);
    }

    const trailingValues = values.slice(runtimeTable.columns.length);
    if (arraySubqueries.length === 0) {
      return reorderedValues.concat(trailingValues);
    }

    const alignedTrailingValues = trailingValues.map((value, index) => {
      const plan = arraySubqueries[index];
      if (!plan) {
        return value;
      }
      return this.alignIncludedValueToDeclaredSchema(value, plan, effectiveRuntimeSchema);
    });

    return reorderedValues.concat(alignedTrailingValues);
  }

  private alignIncludedValueToDeclaredSchema(
    value: Value,
    plan: ArraySubqueryPlan,
    runtimeSchema?: WasmSchema,
  ): Value {
    const effectiveRuntimeSchema = runtimeSchema ?? this.getSchema();
    if (value.type !== "Array") {
      return value;
    }

    return {
      ...value,
      value: value.value.map((entry) => {
        if (entry.type !== "Row") {
          return entry;
        }

        return {
          ...entry,
          value: {
            ...entry.value,
            values: this.alignRowValuesToDeclaredSchema(
              plan.table,
              entry.value.values,
              effectiveRuntimeSchema,
              plan.nested,
              plan.selectColumns,
            ),
          },
        };
      }),
    };
  }

  private alignQueryRowsToDeclaredSchema(
    queryJson: string,
    rows: Row[],
    runtimeSchema?: WasmSchema,
  ): Row[] {
    const effectiveRuntimeSchema = runtimeSchema ?? this.getSchema();
    const { outputTable, arraySubqueries, selectColumns } = resolveQueryAlignmentPlan(queryJson);
    if (!outputTable) {
      return rows;
    }

    return rows.map((row) => ({
      ...row,
      values: this.alignRowValuesToDeclaredSchema(
        outputTable,
        row.values,
        effectiveRuntimeSchema,
        arraySubqueries,
        selectColumns,
      ),
    }));
  }

  private alignSubscriptionDeltaToDeclaredSchema(
    queryJson: string,
    delta: SubscriptionWireDelta,
    runtimeSchema?: WasmSchema,
  ): SubscriptionWireDelta {
    const effectiveRuntimeSchema = runtimeSchema ?? this.getSchema();
    const { outputTable, arraySubqueries, selectColumns } = resolveQueryAlignmentPlan(queryJson);
    if (!outputTable || !Array.isArray(delta)) {
      return delta;
    }

    return delta.map((change) => {
      if ((change.kind === 0 || change.kind === 2) && change.row) {
        return {
          ...change,
          row: {
            ...change.row,
            values: this.alignRowValuesToDeclaredSchema(
              outputTable,
              change.row.values as Value[],
              effectiveRuntimeSchema,
              arraySubqueries,
              selectColumns,
            ),
          },
        };
      }

      return change;
    });
  }

  /**
   * Insert a new row into a table without waiting for durability.
   */
  create(
    table: string,
    values: InsertValues,
    options?: CreateOptions,
    session?: Session,
    attribution?: string,
  ): WriteResult<Row> {
    const row = this.createInternal(table, values, options, session, attribution);
    return new WriteResult(row, row.batchId, this);
  }

  /**
   * @internal
   */
  createInternal(
    table: string,
    values: InsertValues,
    options?: CreateOptions,
    session?: Session,
    attribution?: string,
    batchId?: BatchId,
  ): DirectInsertResult {
    const effectiveSession = this.resolveWriteSession(session, attribution);
    const writeContext = this.encodeWriteContext(
      effectiveSession,
      attribution,
      batchId,
      options?.updatedAt,
    );
    const row = this.runtime.insert(table, values, writeContext, options?.id);
    return {
      ...row,
      values: this.alignRowValuesToDeclaredSchema(
        table,
        row.values as Value[],
        this.context.schema,
      ),
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
    return new WriteResult(row, row.batchId, this);
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
    batchId?: BatchId,
  ): DirectInsertResult {
    const effectiveSession = this.resolveWriteSession(session, attribution);
    const writeContext = this.encodeWriteContext(
      effectiveSession,
      attribution,
      batchId,
      options?.updatedAt,
    );
    const row = this.runtime.restore(table, objectId, values, writeContext);
    return {
      ...row,
      values: this.alignRowValuesToDeclaredSchema(
        table,
        row.values as Value[],
        this.context.schema,
      ),
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
    return new WriteHandle(result.batchId, this);
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
    batchId?: BatchId,
  ): DirectMutationResult {
    const effectiveSession = this.resolveWriteSession(session, attribution);
    const writeContext = this.encodeWriteContext(
      effectiveSession,
      attribution,
      batchId,
      options.updatedAt,
    );
    return this.runtime.upsert(table, options.id, values, writeContext);
  }

  /**
   * Execute a query and return all matching rows.
   *
   * @param query Query builder or JSON-encoded query specification
   * @param options Optional read durability options
   * @returns Array of matching rows
   */
  async query(
    query: string | QueryInput,
    options?: InternalQueryExecutionOptions,
    session?: Session,
    runtimeSchema?: WasmSchema,
  ): Promise<Row[]> {
    const normalizedOptions = this.normalizeQueryExecutionOptions(options);
    const queryJson = resolveQueryJson(query);
    const effectiveSession = session ?? this.resolvedSession;
    const sessionJson = effectiveSession ? JSON.stringify(effectiveSession) : undefined;
    const optionsJson = encodeQueryExecutionOptions(normalizedOptions);
    const effectiveRuntimeSchema = runtimeSchema ?? this.getSchema();
    const results = await this.runtime.query(
      queryJson,
      sessionJson,
      options?.runtimeSettledTier === null
        ? undefined
        : (options?.runtimeSettledTier ?? normalizedOptions.tier),
      optionsJson,
    );
    return this.alignQueryRowsToDeclaredSchema(queryJson, results as Row[], effectiveRuntimeSchema);
  }

  /**
   * Update a row by ID without waiting for durability.
   */
  update(
    objectId: string,
    updates: Record<string, Value>,
    options?: UpdateOptions,
    session?: Session,
    attribution?: string,
  ): WriteHandle {
    const result = this.updateInternal(
      objectId,
      updates,
      options?.updatedAt,
      session,
      attribution,
      undefined,
    );
    return new WriteHandle(result.batchId, this);
  }

  /**
   * @internal
   */
  updateInternal(
    objectId: string,
    updates: Record<string, Value>,
    updatedAt?: number,
    session?: Session,
    attribution?: string,
    batchId?: BatchId,
  ): DirectMutationResult {
    const effectiveSession = this.resolveWriteSession(session, attribution);
    const writeContext = this.encodeWriteContext(effectiveSession, attribution, batchId, updatedAt);
    return this.runtime.update(objectId, updates, writeContext);
  }

  /**
   * Delete a row by ID without waiting for durability.
   */
  delete(
    objectId: string,
    options?: DeleteOptions,
    session?: Session,
    attribution?: string,
  ): WriteHandle {
    const result = this.deleteInternal(objectId, options?.updatedAt, session, attribution);
    return new WriteHandle(result.batchId, this);
  }

  /**
   * @internal
   */
  deleteInternal(
    objectId: string,
    updatedAt?: number,
    session?: Session,
    attribution?: string,
    batchId?: BatchId,
  ): DirectMutationResult {
    const effectiveSession = this.resolveWriteSession(session, attribution);
    const writeContext = this.encodeWriteContext(effectiveSession, attribution, batchId, updatedAt);
    return this.runtime.delete(objectId, writeContext);
  }

  /**
   * Subscribe to a query and receive updates when results change.
   *
   * @param query Query builder or JSON-encoded query specification
   * @param callback Called with delta whenever results change
   * @param options Optional read durability options
   * @returns Subscription ID for unsubscribing
   */
  subscribe(
    query: string | QueryInput,
    callback: SubscriptionCallback,
    options?: QueryExecutionOptions,
    session?: Session,
    runtimeSchema?: WasmSchema,
  ): number {
    const normalizedOptions = this.normalizeQueryExecutionOptions(options);
    const effectiveSession = session ?? this.resolvedSession;
    const sessionJson = effectiveSession ? JSON.stringify(effectiveSession) : undefined;
    const queryJson = resolveQueryJson(query);
    const optionsJson = encodeQueryExecutionOptions(normalizedOptions);
    const effectiveRuntimeSchema = runtimeSchema ?? this.getSchema();

    // Uses the runtime's 2-phase subscribe API: `createSubscription` allocates
    // a handle synchronously (zero work), then `executeSubscription` is deferred
    // via the scheduler so compilation + first tick run outside the caller's
    // synchronous stack (e.g. outside a React render).
    const handle = this.runtime.createSubscription(
      queryJson,
      sessionJson,
      normalizedOptions.tier,
      optionsJson,
    );

    this.scheduler(() => {
      this.runtime.executeSubscription(handle, (...args: unknown[]) => {
        const deltaJsonOrObject = normalizeSubscriptionCallbackArgs(args);
        if (deltaJsonOrObject === undefined) {
          return;
        }

        const delta: SubscriptionWireDelta =
          typeof deltaJsonOrObject === "string" ? JSON.parse(deltaJsonOrObject) : deltaJsonOrObject;
        callback(
          this.alignSubscriptionDeltaToDeclaredSchema(queryJson, delta, effectiveRuntimeSchema),
        );
      });
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
    if (!this.runtime.connect) {
      throw new Error("Underlying runtime does not support connect()");
    }
    this.runtime.connect(httpUrlToWs(url, this.context.appId), JSON.stringify(auth));
  }

  /**
   * Get the current schema.
   */
  getSchema(): WasmSchema {
    const schemaHash = this.runtime.getSchemaHash();
    if (this.cachedRuntimeSchemaHash === schemaHash && this.cachedRuntimeSchema) {
      return this.cachedRuntimeSchema;
    }

    const schema = normalizeRuntimeSchema(this.runtime.getSchema());
    this.cachedRuntimeSchemaHash = schemaHash;
    this.cachedRuntimeSchema = schema;
    return schema;
  }

  /**
   * Get the underlying runtime (for WorkerBridge).
   * @internal
   */
  getRuntime(): Runtime {
    return this.runtime;
  }

  async waitForBatch(batchId: BatchId, tier: DurabilityTier): Promise<void> {
    try {
      await this.runtime.waitForBatch(batchId, tier);
    } catch (error) {
      throw this.normalizeBatchWaitError(error);
    }
  }

  private normalizeBatchWaitError(error: unknown): Error {
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
      // Disconnect Rust-owned transport if present.
      this.runtime.disconnect?.();

      // Close runtime if it supports explicit shutdown (e.g., NapiRuntime).
      if (this.runtime.close) {
        await this.runtime.close();
      }
    })();

    return await this.shutdownPromise;
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
