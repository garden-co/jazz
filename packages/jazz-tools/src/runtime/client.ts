/**
 * JazzClient - High-level TypeScript client for Jazz.
 *
 * Wraps the WASM runtime and provides a clean API for CRUD operations,
 * subscriptions, and sync.
 */

import type { AppContext, RuntimeSourcesConfig, Session } from "./context.js";
import type { InsertValues, Value, RowDelta, WasmSchema } from "../drivers/types.js";
import { normalizeRuntimeSchema, serializeRuntimeSchema } from "../drivers/schema-wire.js";
import {
  sendSyncPayload,
  generateClientId,
  buildEndpointUrl,
  applyUserAuthHeaders,
  createRuntimeSyncStreamController,
  createSyncOutboxRouter,
  isExpectedFetchAbortError,
  SyncAuthError,
  type SyncStreamController,
  type SyncAuth,
  type AuthFailureReason,
  type RuntimeSyncOutboxCallback,
} from "./sync-transport.js";
import { resolveLocalAuthDefaults } from "./local-auth.js";
import { resolveClientSessionStateSync } from "./client-session.js";
import { translateQuery } from "./query-adapter.js";
import { isHiddenIncludeColumnName, resolveSelectedColumns } from "./select-projection.js";
import {
  resolveRuntimeConfigSyncInitInput,
  resolveRuntimeConfigWasmUrl,
} from "./runtime-config.js";

/**
 * Minimal request shape supported by `JazzClient.forRequest()`.
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
  insert(table: string, values: InsertValues): Row;
  insertWithSession?(table: string, values: InsertValues, write_context_json?: string | null): Row;
  insertDurable(table: string, values: InsertValues, tier: string): Promise<Row>;
  insertDurableWithSession?(
    table: string,
    values: InsertValues,
    write_context_json: string | null | undefined,
    tier: string,
  ): Promise<Row>;
  update(object_id: string, values: any): void;
  updateWithSession?(object_id: string, values: any, write_context_json?: string | null): void;
  updateDurable(object_id: string, values: any, tier: string): Promise<void>;
  updateDurableWithSession?(
    object_id: string,
    values: any,
    write_context_json: string | null | undefined,
    tier: string,
  ): Promise<void>;
  delete(object_id: string): void;
  deleteWithSession?(object_id: string, write_context_json?: string | null): void;
  deleteDurable(object_id: string, tier: string): Promise<void>;
  deleteDurableWithSession?(
    object_id: string,
    write_context_json: string | null | undefined,
    tier: string,
  ): Promise<void>;
  query(
    query_json: string,
    session_json?: string | null,
    tier?: string | null,
    options_json?: string | null,
  ): Promise<any>;
  subscribe(
    query_json: string,
    on_update: Function,
    session_json?: string | null,
    tier?: string | null,
    options_json?: string | null,
  ): number;
  createSubscription(
    query_json: string,
    session_json?: string | null,
    tier?: string | null,
    options_json?: string | null,
  ): number;
  executeSubscription(handle: number, on_update: Function): void;
  unsubscribe(handle: number): void;
  onSyncMessageReceived(payload: Uint8Array | string, seq?: number | null): void;
  onSyncMessageToSend(callback: RuntimeSyncOutboxCallback): void;
  addServer(serverCatalogueStateHash?: string | null, nextSyncSeq?: number | null): void;
  removeServer(): void;
  addClient(): string;
  getSchema(): any;
  getSchemaHash(): string;
  close?(): void | Promise<void>;
  setClientRole?(client_id: string, role: string): void;
  onSyncMessageReceivedFromClient?(client_id: string, payload: Uint8Array | string): void;
}

/**
 * Persistence tier for durability guarantees.
 *
 * - `worker`: Persisted in web worker / local storage
 * - `edge`: Persisted at edge server
 * - `global`: Persisted at global server
 */
export type DurabilityTier = "worker" | "edge" | "global";
export type LocalUpdatesMode = "immediate" | "deferred";
export type QueryPropagation = "full" | "local-only";
export type QueryVisibility = "public" | "hidden_from_live_query_list";
export interface QueryExecutionOptions {
  tier?: DurabilityTier;
  localUpdates?: LocalUpdatesMode;
  propagation?: QueryPropagation;
  visibility?: QueryVisibility;
}

export interface ResolvedQueryExecutionOptions {
  tier: DurabilityTier;
  localUpdates: LocalUpdatesMode;
  propagation: QueryPropagation;
  visibility: QueryVisibility;
}

export interface WriteDurabilityOptions {
  tier?: DurabilityTier;
}

/**
 * Query row result.
 */
export interface Row {
  id: string;
  values: Value[];
}

interface WriteContextPayload {
  session?: Session;
  attribution?: string;
}

/**
 * Subscription callback type.
 */
export type SubscriptionCallback = (delta: RowDelta) => void;

export interface ConnectSyncRuntimeOptions {
  useBinaryEncoding?: boolean;
  onAuthFailure?: (reason: AuthFailureReason) => void;
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
    return "worker";
  }

  // In non-browser environments, default to edge when connected to a server.
  // For local/in-memory runtimes without a server, keep worker semantics.
  return context.serverUrl ? "edge" : "worker";
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

function encodeQueryExecutionOptions(options: QueryExecutionOptions): string | undefined {
  const payload: { propagation?: QueryPropagation; local_updates?: LocalUpdatesMode } = {};
  if ((options.propagation ?? "full") !== "full") {
    payload.propagation = options.propagation;
  }
  if ((options.localUpdates ?? "immediate") !== "immediate") {
    payload.local_updates = options.localUpdates;
  }

  if (!payload.propagation && !payload.local_updates) {
    return undefined;
  }

  return JSON.stringify(payload);
}

function readHeader(request: RequestLike, name: string): string | undefined {
  const lower = name.toLowerCase();

  const fromMethod = request.header?.(name) ?? request.header?.(lower);
  if (typeof fromMethod === "string") {
    return fromMethod;
  }

  const headers = request.headers;
  if (!headers) {
    return undefined;
  }

  if (typeof Headers !== "undefined" && headers instanceof Headers) {
    return headers.get(name) ?? headers.get(lower) ?? undefined;
  }

  const record = headers as Record<string, string | string[] | undefined>;
  const raw = record[name] ?? record[lower];
  if (Array.isArray(raw)) {
    return raw[0];
  }
  return raw;
}

function normalizeSubscriptionCallbackArgs(args: unknown[]): RowDelta | string | undefined {
  if (args.length === 1) {
    return args[0] as RowDelta | string;
  }

  if (args.length === 2 && args[0] == null) {
    return args[1] as RowDelta | string | undefined;
  }

  console.error("Invalid subscription callback arguments", args);
  return undefined;
}

function decodeBase64Url(value: string): string {
  const base64 = value.replace(/-/g, "+").replace(/_/g, "/");
  const padded = base64 + "=".repeat((4 - (base64.length % 4)) % 4);

  if (typeof atob === "function") {
    return atob(padded);
  }
  if (typeof Buffer !== "undefined") {
    return Buffer.from(padded, "base64").toString("utf8");
  }

  throw new Error("No base64 decoder available in this runtime");
}

export function sessionFromRequest(request: RequestLike): Session {
  const authHeader = readHeader(request, "authorization");
  if (!authHeader?.startsWith("Bearer ")) {
    throw new Error("Missing or invalid Authorization header");
  }

  const token = authHeader.slice("Bearer ".length).trim();
  const parts = token.split(".");
  if (parts.length < 2) {
    throw new Error("Invalid JWT format");
  }
  const payloadPart = parts[1];
  if (payloadPart === undefined) {
    throw new Error("Invalid JWT format");
  }

  let payload: unknown;
  try {
    payload = JSON.parse(decodeBase64Url(payloadPart));
  } catch {
    throw new Error("Invalid JWT payload");
  }

  if (!payload || typeof payload !== "object" || Array.isArray(payload)) {
    throw new Error("Invalid JWT payload");
  }

  const typedPayload = payload as { sub?: unknown; claims?: unknown };
  if (typeof typedPayload.sub !== "string" || typedPayload.sub.length === 0) {
    throw new Error("JWT payload missing sub");
  }

  const claims =
    typedPayload.claims &&
    typeof typedPayload.claims === "object" &&
    !Array.isArray(typedPayload.claims)
      ? (typedPayload.claims as Record<string, unknown>)
      : {};

  return { user_id: typedPayload.sub, claims };
}

/**
 * Session-scoped client for backend operations.
 *
 * Created by `JazzClient.forSession()`. Allows backend applications
 * to perform operations as a specific user via header-based authentication.
 */
export class SessionClient {
  private client: JazzClient;
  private session: Session;

  constructor(client: JazzClient, session: Session) {
    this.client = client;
    this.session = session;
  }

  /**
   * Create a new row as this session's user.
   */
  async create(table: string, values: InsertValues): Promise<string> {
    if (!this.client.getServerUrl()) {
      throw new Error("No server connection");
    }

    const response = await this.client.sendRequest(
      this.client.getRequestUrl("/sync/object"),
      "POST",
      {
        table,
        values,
        schema_context: this.client.getSchemaContext(),
      },
      this.session,
    );

    if (!response.ok) {
      throw new Error(`Create failed: ${response.statusText}`);
    }

    const result = await response.json();
    return result.object_id;
  }

  /**
   * Update a row as this session's user.
   */
  async update(objectId: string, updates: Record<string, Value>): Promise<void> {
    if (!this.client.getServerUrl()) {
      throw new Error("No server connection");
    }

    const updateArray = Object.entries(updates);

    const response = await this.client.sendRequest(
      this.client.getRequestUrl("/sync/object"),
      "PUT",
      {
        object_id: objectId,
        updates: updateArray,
        schema_context: this.client.getSchemaContext(),
      },
      this.session,
    );

    if (!response.ok) {
      throw new Error(`Update failed: ${response.statusText}`);
    }
  }

  /**
   * Delete a row as this session's user.
   */
  async delete(objectId: string): Promise<void> {
    if (!this.client.getServerUrl()) {
      throw new Error("No server connection");
    }

    const response = await this.client.sendRequest(
      this.client.getRequestUrl("/sync/object/delete"),
      "POST",
      {
        object_id: objectId,
        schema_context: this.client.getSchemaContext(),
      },
      this.session,
    );

    if (!response.ok) {
      throw new Error(`Delete failed: ${response.statusText}`);
    }
  }

  /**
   * Query as this session's user.
   */
  async query(query: string | QueryInput, options?: QueryExecutionOptions): Promise<Row[]> {
    return this.client.queryInternal(query, this.session, options);
  }

  /**
   * Subscribe to a query as this session's user.
   */
  subscribe(
    query: string | QueryInput,
    callback: SubscriptionCallback,
    options?: QueryExecutionOptions,
  ): number {
    return this.client.subscribeInternal(query, callback, this.session, options);
  }
}

/**
 * High-level Jazz client.
 */
export class JazzClient {
  private runtime: Runtime;
  private streamController: SyncStreamController;
  private serverClientId: string = generateClientId();
  private scheduler: (task: () => void) => void;
  private context: AppContext;
  private resolvedSession: Session | null;
  private defaultDurabilityTier: DurabilityTier;
  private useBackendSyncAuth = false;
  private readonly onAuthFailure?: (reason: AuthFailureReason) => void;
  private syncStarted = false;
  private remoteSyncConnected: boolean;
  private pendingRemoteSyncWaiters: Array<{
    resolve: () => void;
    reject: (error: Error) => void;
  }> = [];
  private readonly inFlightServerSyncs = new Set<Promise<void>>();
  private shuttingDown = false;
  private shutdownPromise: Promise<void> | null = null;

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
    this.onAuthFailure = runtimeOptions?.onAuthFailure;
    this.remoteSyncConnected = !context.serverUrl;
    this.resolvedSession = resolveClientSessionStateSync({
      appId: context.appId,
      jwtToken: context.jwtToken,
      localAuthMode: context.localAuthMode,
      localAuthToken: context.localAuthToken,
    }).session;
    this.streamController = createRuntimeSyncStreamController({
      getRuntime: () => this.runtime,
      getAuth: () => this.getSyncAuth(),
      getSchemaHash: () => this.runtime.getSchemaHash(),
      getClientId: () => this.serverClientId,
      setClientId: (clientId) => {
        this.serverClientId = clientId;
      },
      onConnected: () => {
        this.remoteSyncConnected = true;
        this.resolvePendingRemoteSyncWaiters();
      },
      onDisconnected: () => {
        this.remoteSyncConnected = false;
      },
      onAuthFailure: (reason) => {
        this.remoteSyncConnected = false;
        this.rejectPendingRemoteSyncWaiters(new Error(`Sync auth failed: ${reason}`));
        this.onAuthFailure?.(reason);
      },
    });
  }

  /**
   * Connect to Jazz with the given context.
   *
   * @param context Application context with driver and schema
   * @returns Connected JazzClient instance
   */
  static async connect(
    context: AppContext,
    runtimeOptions?: ConnectSyncRuntimeOptions,
  ): Promise<JazzClient> {
    const resolvedContext = resolveLocalAuthDefaults(context);

    // Load WASM module dynamically
    const wasmModule = await loadWasmModule(resolvedContext.runtimeSources);

    // Create WASM runtime (storage is now synchronous in-memory)
    const schemaJson = serializeRuntimeSchema(resolvedContext.schema);
    const runtime = new wasmModule.WasmRuntime(
      schemaJson,
      resolvedContext.appId,
      resolvedContext.env ?? "dev",
      resolvedContext.userBranch ?? "main",
      resolveNodeTier(resolvedContext.tier),
    );

    const client = new JazzClient(
      runtime,
      resolvedContext,
      resolveDefaultDurabilityTier(resolvedContext),
      runtimeOptions,
    );

    // Set up sync if server URL provided
    if (resolvedContext.serverUrl) {
      client.setupSync(resolvedContext.serverUrl, resolvedContext.serverPathPrefix);
    }

    return client;
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
    const resolvedContext = resolveLocalAuthDefaults(context);

    // Create WASM runtime (storage is now synchronous in-memory)
    const schemaJson = serializeRuntimeSchema(resolvedContext.schema);
    const runtime = new wasmModule.WasmRuntime(
      schemaJson,
      resolvedContext.appId,
      resolvedContext.env ?? "dev",
      resolvedContext.userBranch ?? "main",
      resolveNodeTier(resolvedContext.tier),
      runtimeOptions?.useBinaryEncoding ?? false,
    );

    const client = new JazzClient(
      runtime,
      resolvedContext,
      resolveDefaultDurabilityTier(resolvedContext),
      runtimeOptions,
    );

    // Set up sync if server URL provided
    if (resolvedContext.serverUrl) {
      client.setupSync(resolvedContext.serverUrl, resolvedContext.serverPathPrefix);
    }

    return client;
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
    const client = new JazzClient(
      runtime,
      context,
      resolveDefaultDurabilityTier(context),
      runtimeOptions,
    );

    // Set up sync if server URL provided
    if (context.serverUrl) {
      client.setupSync(context.serverUrl, context.serverPathPrefix);
    }

    return client;
  }

  /**
   * Create a session-scoped client for backend operations.
   *
   * This allows backend applications to perform operations as a specific user.
   * Requires `backendSecret` to be configured in the `AppContext`.
   *
   * @param session Session to impersonate
   * @returns SessionClient for performing operations as the given user
   * @throws Error if backendSecret is not configured
   *
   * @example
   * ```typescript
   * const userSession = { user_id: "user-123", claims: {} };
   * const userClient = client.forSession(userSession);
   * const id = await userClient.create("todos", {
   *   title: { type: "Text", value: "Buy milk" },
   *   done: { type: "Boolean", value: false },
   * });
   * ```
   */
  forSession(session: Session): SessionClient {
    if (!this.context.backendSecret) {
      throw new Error("backendSecret required for session impersonation");
    }
    if (!this.context.serverUrl) {
      throw new Error("serverUrl required for session impersonation");
    }
    return new SessionClient(this, session);
  }

  /**
   * Create a session-scoped client from an authenticated HTTP request.
   *
   * Extracts `Authorization: Bearer <jwt>` and maps payload fields:
   * - `sub` -> `session.user_id`
   * - `claims` -> `session.claims` (defaults to `{}`)
   *
   * This helper only extracts payload fields and does not validate JWT signatures.
   * JWT verification should happen in your auth middleware before request handling.
   */
  forRequest(request: RequestLike): SessionClient {
    return this.forSession(sessionFromRequest(request));
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
    this.useBackendSyncAuth = true;
    this.streamController.updateAuth();
    return this;
  }

  updateAuthToken(jwtToken?: string): void {
    this.context.jwtToken = jwtToken;
    this.resolvedSession = resolveClientSessionStateSync({
      appId: this.context.appId,
      jwtToken,
      localAuthMode: this.context.localAuthMode,
      localAuthToken: this.context.localAuthToken,
    }).session;
    this.streamController.updateAuth();
  }

  private getSyncAuth(): SyncAuth {
    if (this.useBackendSyncAuth) {
      return {
        backendSecret: this.context.backendSecret,
        adminSecret: this.context.adminSecret,
      };
    }

    return {
      jwtToken: this.context.jwtToken,
      localAuthMode: this.context.localAuthMode,
      localAuthToken: this.context.localAuthToken,
      adminSecret: this.context.adminSecret,
    };
  }

  private normalizeQueryExecutionOptions(
    options?: QueryExecutionOptions,
  ): ResolvedQueryExecutionOptions {
    return resolveEffectiveQueryExecutionOptions(
      { ...this.context, defaultDurabilityTier: this.defaultDurabilityTier },
      options,
    );
  }

  private resolveWriteTier(options?: WriteDurabilityOptions): DurabilityTier {
    return options?.tier ?? this.defaultDurabilityTier;
  }

  private encodeWriteContext(session?: Session, attribution?: string): string | undefined {
    if (!session && attribution === undefined) {
      return undefined;
    }
    if (attribution === undefined && session) {
      return JSON.stringify(session);
    }

    const payload: WriteContextPayload = {};
    if (session) {
      payload.session = session;
    }
    if (attribution !== undefined) {
      payload.attribution = attribution;
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

  private requireSessionWriteMethod<
    T extends keyof Pick<
      Runtime,
      | "insertWithSession"
      | "insertDurableWithSession"
      | "updateWithSession"
      | "updateDurableWithSession"
      | "deleteWithSession"
      | "deleteDurableWithSession"
    >,
  >(method: T): NonNullable<Runtime[T]> {
    const runtimeMethod = this.runtime[method];
    if (!runtimeMethod) {
      throw new Error(`${String(method)} is not supported by this runtime`);
    }
    return runtimeMethod.bind(this.runtime) as NonNullable<Runtime[T]>;
  }

  private alignRowValuesToDeclaredSchema(
    table: string,
    values: Value[],
    runtimeSchema = this.getSchema(),
    arraySubqueries: ArraySubqueryPlan[] = [],
    selectColumns: string[] = [],
  ): Value[] {
    const declaredTable = this.context.schema[table];
    const runtimeTable = runtimeSchema[table];

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
        return this.alignIncludedValueToDeclaredSchema(value, plan, runtimeSchema);
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
      return this.alignIncludedValueToDeclaredSchema(value, plan, runtimeSchema);
    });

    return reorderedValues.concat(alignedTrailingValues);
  }

  private alignIncludedValueToDeclaredSchema(
    value: Value,
    plan: ArraySubqueryPlan,
    runtimeSchema = this.getSchema(),
  ): Value {
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
              runtimeSchema,
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
    runtimeSchema = this.getSchema(),
  ): Row[] {
    const { outputTable, arraySubqueries, selectColumns } = resolveQueryAlignmentPlan(queryJson);
    if (!outputTable) {
      return rows;
    }

    return rows.map((row) => ({
      ...row,
      values: this.alignRowValuesToDeclaredSchema(
        outputTable,
        row.values,
        runtimeSchema,
        arraySubqueries,
        selectColumns,
      ),
    }));
  }

  private alignSubscriptionDeltaToDeclaredSchema(
    queryJson: string,
    delta: RowDelta,
    runtimeSchema = this.getSchema(),
  ): RowDelta {
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
              runtimeSchema,
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
  create(table: string, values: InsertValues): Row {
    return this.createInternal(table, values);
  }

  /**
   * Insert a new row into a table with an optional session for policy checks.
   * @internal
   */
  createInternal(
    table: string,
    values: InsertValues,
    session?: Session,
    attribution?: string,
  ): Row {
    const effectiveSession = this.resolveWriteSession(session, attribution);
    const row =
      effectiveSession || attribution !== undefined
        ? this.requireSessionWriteMethod("insertWithSession")(
            table,
            values,
            this.encodeWriteContext(effectiveSession, attribution),
          )
        : this.runtime.insert(table, values);
    return {
      ...row,
      values: this.alignRowValuesToDeclaredSchema(table, row.values as Value[], this.getSchema()),
    };
  }

  /**
   * Insert a new row into a table and wait for durability at the requested tier.
   */
  async createDurable(
    table: string,
    values: InsertValues,
    options?: WriteDurabilityOptions,
  ): Promise<Row> {
    return this.createDurableInternal(table, values, undefined, undefined, options);
  }

  /**
   * Insert a new row into a table and wait for durability, optionally scoped to a session.
   * @internal
   */
  async createDurableInternal(
    table: string,
    values: InsertValues,
    session?: Session,
    attribution?: string,
    options?: WriteDurabilityOptions,
  ): Promise<Row> {
    const tier = this.resolveWriteTier(options);
    const effectiveSession = this.resolveWriteSession(session, attribution);
    const row =
      effectiveSession || attribution !== undefined
        ? await this.requireSessionWriteMethod("insertDurableWithSession")(
            table,
            values,
            this.encodeWriteContext(effectiveSession, attribution),
            tier,
          )
        : await this.runtime.insertDurable(table, values, tier);
    return {
      ...row,
      values: this.alignRowValuesToDeclaredSchema(table, row.values as Value[], this.getSchema()),
    };
  }

  /**
   * Execute a query and return all matching rows.
   *
   * @param query Query builder or JSON-encoded query specification
   * @param options Optional read durability options
   * @returns Array of matching rows
   */
  async query(query: string | QueryInput, options?: QueryExecutionOptions): Promise<Row[]> {
    return this.queryInternal(query, this.resolvedSession ?? undefined, options);
  }

  /**
   * Internal query with optional session and read durability options.
   * @internal
   */
  async queryInternal(
    query: string | QueryInput,
    session?: Session,
    options?: QueryExecutionOptions,
  ): Promise<Row[]> {
    const normalizedOptions = this.normalizeQueryExecutionOptions(options);
    await this.waitForRemoteReadAvailability(normalizedOptions.tier);
    const queryJson = resolveQueryJson(query);
    const sessionJson = session ? JSON.stringify(session) : undefined;
    const optionsJson = encodeQueryExecutionOptions(normalizedOptions);
    const runtimeSchema = this.getSchema();
    const results = await this.runtime.query(
      queryJson,
      sessionJson,
      normalizedOptions.tier,
      optionsJson,
    );
    return this.alignQueryRowsToDeclaredSchema(queryJson, results as Row[], runtimeSchema);
  }

  /**
   * Update a row by ID without waiting for durability.
   */
  update(objectId: string, updates: Record<string, Value>): void {
    this.updateInternal(objectId, updates);
  }

  /**
   * Update a row by ID without waiting for durability, optionally scoped to a session.
   * @internal
   */
  updateInternal(
    objectId: string,
    updates: Record<string, Value>,
    session?: Session,
    attribution?: string,
  ): void {
    const effectiveSession = this.resolveWriteSession(session, attribution);
    if (effectiveSession || attribution !== undefined) {
      this.requireSessionWriteMethod("updateWithSession")(
        objectId,
        updates,
        this.encodeWriteContext(effectiveSession, attribution),
      );
      return;
    }
    this.runtime.update(objectId, updates);
  }

  /**
   * Update a row by ID and wait for durability at the requested tier.
   */
  async updateDurable(
    objectId: string,
    updates: Record<string, Value>,
    options?: WriteDurabilityOptions,
  ): Promise<void> {
    await this.updateDurableInternal(objectId, updates, undefined, undefined, options);
  }

  /**
   * Update a row by ID and wait for durability, optionally scoped to a session.
   * @internal
   */
  async updateDurableInternal(
    objectId: string,
    updates: Record<string, Value>,
    session?: Session,
    attribution?: string,
    options?: WriteDurabilityOptions,
  ): Promise<void> {
    const tier = this.resolveWriteTier(options);
    const effectiveSession = this.resolveWriteSession(session, attribution);
    if (effectiveSession || attribution !== undefined) {
      await this.requireSessionWriteMethod("updateDurableWithSession")(
        objectId,
        updates,
        this.encodeWriteContext(effectiveSession, attribution),
        tier,
      );
      return;
    }
    await this.runtime.updateDurable(objectId, updates, tier);
  }

  /**
   * Delete a row by ID without waiting for durability.
   */
  delete(objectId: string): void {
    this.deleteInternal(objectId);
  }

  /**
   * Delete a row by ID without waiting for durability, optionally scoped to a session.
   * @internal
   */
  deleteInternal(objectId: string, session?: Session, attribution?: string): void {
    const effectiveSession = this.resolveWriteSession(session, attribution);
    if (effectiveSession || attribution !== undefined) {
      this.requireSessionWriteMethod("deleteWithSession")(
        objectId,
        this.encodeWriteContext(effectiveSession, attribution),
      );
      return;
    }
    this.runtime.delete(objectId);
  }

  /**
   * Delete a row by ID and wait for durability at the requested tier.
   */
  async deleteDurable(objectId: string, options?: WriteDurabilityOptions): Promise<void> {
    await this.deleteDurableInternal(objectId, undefined, undefined, options);
  }

  /**
   * Delete a row by ID and wait for durability, optionally scoped to a session.
   * @internal
   */
  async deleteDurableInternal(
    objectId: string,
    session?: Session,
    attribution?: string,
    options?: WriteDurabilityOptions,
  ): Promise<void> {
    const tier = this.resolveWriteTier(options);
    const effectiveSession = this.resolveWriteSession(session, attribution);
    if (effectiveSession || attribution !== undefined) {
      await this.requireSessionWriteMethod("deleteDurableWithSession")(
        objectId,
        this.encodeWriteContext(effectiveSession, attribution),
        tier,
      );
      return;
    }
    await this.runtime.deleteDurable(objectId, tier);
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
  ): number {
    return this.subscribeInternal(query, callback, this.resolvedSession ?? undefined, options);
  }

  /**
   * Internal subscribe with optional session and read durability options.
   *
   * Uses the runtime's 2-phase subscribe API: `createSubscription` allocates
   * a handle synchronously (zero work), then `executeSubscription` is deferred
   * via the scheduler so compilation + first tick run outside the caller's
   * synchronous stack (e.g. outside a React render).
   *
   * @internal
   */
  subscribeInternal(
    query: string | QueryInput,
    callback: SubscriptionCallback,
    session?: Session,
    options?: QueryExecutionOptions,
  ): number {
    const normalizedOptions = this.normalizeQueryExecutionOptions(options);
    const sessionJson = session ? JSON.stringify(session) : undefined;
    const queryJson = resolveQueryJson(query);
    const optionsJson = encodeQueryExecutionOptions(normalizedOptions);
    const runtimeSchema = this.getSchema();

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

        const delta: RowDelta =
          typeof deltaJsonOrObject === "string" ? JSON.parse(deltaJsonOrObject) : deltaJsonOrObject;
        callback(this.alignSubscriptionDeltaToDeclaredSchema(queryJson, delta, runtimeSchema));
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
   * Get the current schema.
   */
  getSchema(): WasmSchema {
    return normalizeRuntimeSchema(this.runtime.getSchema());
  }

  /**
   * Get the underlying runtime (for WorkerBridge).
   * @internal
   */
  getRuntime(): Runtime {
    return this.runtime;
  }

  /**
   * Get the server URL (for SessionClient).
   * @internal
   */
  getServerUrl(): string | undefined {
    return this.context.serverUrl;
  }

  /**
   * Build a fully-qualified endpoint URL against the configured server.
   * @internal
   */
  getRequestUrl(path: string): string {
    if (!this.context.serverUrl) {
      throw new Error("No server connection");
    }
    return buildEndpointUrl(this.context.serverUrl, path, this.context.serverPathPrefix);
  }

  /**
   * Get schema context for server requests.
   * @internal
   */
  getSchemaContext(): {
    env: string;
    schema_hash: string;
    user_branch: string;
  } {
    return {
      env: this.context.env ?? "dev",
      schema_hash: this.runtime.getSchemaHash(),
      user_branch: this.context.userBranch ?? "main",
    };
  }

  /**
   * Send an HTTP request with appropriate auth headers.
   * @internal
   */
  async sendRequest(
    url: string,
    method: string,
    body: unknown,
    session?: Session,
  ): Promise<Response> {
    const headers: Record<string, string> = {
      "Content-Type": "application/json",
    };

    // Priority 1: Backend impersonation (via SessionClient)
    if (session && this.context.backendSecret) {
      headers["X-Jazz-Backend-Secret"] = this.context.backendSecret;
      headers["X-Jazz-Session"] = btoa(JSON.stringify(session));
    }
    // Priority 2: frontend auth (JWT or local anonymous/demo token headers)
    else {
      applyUserAuthHeaders(headers, {
        jwtToken: this.context.jwtToken,
        localAuthMode: this.context.localAuthMode,
        localAuthToken: this.context.localAuthToken,
      });
    }

    return fetch(url, {
      method,
      headers,
      body: JSON.stringify(body),
    });
  }

  /**
   * Shutdown the client and release resources.
   */
  async shutdown(): Promise<void> {
    this.resolvePendingRemoteSyncWaiters();
    if (this.shutdownPromise) {
      return await this.shutdownPromise;
    }

    this.shutdownPromise = (async () => {
      this.shuttingDown = true;

      // Stop accepting new server-bound outbox work before tearing down sync.
      this.runtime.onSyncMessageToSend(() => undefined);
      this.streamController.stop();
      await this.waitForInFlightServerSyncs();

      // Close runtime if it supports explicit shutdown (e.g., NapiRuntime).
      if (this.runtime.close) {
        await this.runtime.close();
      }
    })();

    return await this.shutdownPromise;
  }

  private setupSync(serverUrl: string, serverPathPrefix?: string): void {
    this.syncStarted = true;
    this.runtime.onSyncMessageToSend(
      createSyncOutboxRouter({
        logPrefix: "[client] ",
        retryServerPayloads: true,
        onServerPayload: (payload, isCatalogue) =>
          this.sendSyncMessage(payload as string, isCatalogue),
        onServerPayloadError: (error) => {
          if (error instanceof SyncAuthError) {
            this.streamController.notifyAuthFailure(error.reason);
            return;
          }

          const isExpectedAbort = isExpectedFetchAbortError(error);
          if (!isExpectedAbort) {
            console.error("Sync POST error:", error);
            this.streamController.notifyTransportFailure();
          }
        },
      }),
    );

    // Connect to binary stream for incoming messages
    this.streamController.start(serverUrl, serverPathPrefix);
  }

  private resolvePendingRemoteSyncWaiters(): void {
    if (this.pendingRemoteSyncWaiters.length === 0) {
      return;
    }

    const waiters = this.pendingRemoteSyncWaiters.splice(0);
    for (const waiter of waiters) {
      waiter.resolve();
    }
  }

  private rejectPendingRemoteSyncWaiters(error: Error): void {
    if (this.pendingRemoteSyncWaiters.length === 0) {
      return;
    }

    const waiters = this.pendingRemoteSyncWaiters.splice(0);
    for (const waiter of waiters) {
      waiter.reject(error);
    }
  }

  private async waitForRemoteReadAvailability(tier: DurabilityTier): Promise<void> {
    if (
      !this.syncStarted ||
      !this.context.serverUrl ||
      tier === "worker" ||
      this.remoteSyncConnected
    ) {
      return;
    }

    await new Promise<void>((resolve, reject) => {
      this.pendingRemoteSyncWaiters.push({ resolve, reject });
    });
  }

  private async waitForInFlightServerSyncs(): Promise<void> {
    while (this.inFlightServerSyncs.size > 0) {
      await Promise.allSettled(this.inFlightServerSyncs);
    }
  }

  private trackServerSync(pending: Promise<void>): Promise<void> {
    let tracked: Promise<void>;
    tracked = pending.finally(() => {
      this.inFlightServerSyncs.delete(tracked);
    });
    this.inFlightServerSyncs.add(tracked);
    return tracked;
  }

  private sendSyncMessage(payloadJson: string, isCatalogue: boolean): Promise<void> {
    if (this.shuttingDown) {
      return Promise.resolve();
    }

    const serverUrl = this.streamController.getServerUrl();
    if (!serverUrl) return Promise.resolve();

    return this.trackServerSync(
      sendSyncPayload(
        serverUrl,
        payloadJson,
        isCatalogue,
        {
          ...this.getSyncAuth(),
          clientId: this.serverClientId,
          pathPrefix: this.streamController.getPathPrefix(),
        },
        "[client] ",
      ),
    );
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
