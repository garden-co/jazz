/**
 * JazzClient - High-level TypeScript client for Jazz.
 *
 * Wraps the WASM runtime and provides a clean API for CRUD operations,
 * subscriptions, and sync.
 */

import type { AppContext, Session } from "./context.js";
import type { Value, RowDelta, WasmSchema } from "../drivers/types.js";
import { serializeRuntimeSchema } from "../drivers/schema-wire.js";
import {
  sendSyncPayload,
  generateClientId,
  buildEndpointUrl,
  applyUserAuthHeaders,
  createRuntimeSyncStreamController,
  createSyncOutboxRouter,
  isExpectedFetchAbortError,
  linkExternalIdentity as sendLinkExternalIdentityRequest,
  type SyncStreamController,
  type SyncAuth,
  type LinkExternalResponse,
  type RuntimeSyncOutboxCallback,
} from "./sync-transport.js";
import { resolveLocalAuthDefaults } from "./local-auth.js";
import { resolveJwtSession } from "./client-session.js";
import { translateQuery } from "./query-adapter.js";

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
  insert(table: string, values: any): Row;
  insertDurable(table: string, values: any, tier: string): Promise<Row>;
  update(object_id: string, values: any): void;
  updateDurable(object_id: string, values: any, tier: string): Promise<void>;
  delete(object_id: string): void;
  deleteDurable(object_id: string, tier: string): Promise<void>;
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
  onSyncMessageReceived(payload: Uint8Array | string): void;
  onSyncMessageToSend(callback: RuntimeSyncOutboxCallback): void;
  addServer(): void;
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
export interface QueryExecutionOptions {
  tier?: DurabilityTier;
  localUpdates?: LocalUpdatesMode;
  propagation?: QueryPropagation;
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

/**
 * Subscription callback type.
 */
export type SubscriptionCallback = (delta: RowDelta) => void;

export interface LinkExternalIdentityOptions {
  jwtToken?: string;
  localAuthMode?: "anonymous" | "demo";
  localAuthToken?: string;
}

export type LinkExternalIdentityResult = LinkExternalResponse;

export interface ConnectSyncRuntimeOptions {
  useBinaryEncoding?: boolean;
}

/**
 * QueryBuilder-compatible input accepted by query and subscribe APIs.
 */
export interface QueryInput {
  _build(): string;
  /** Optional schema metadata available on generated QueryBuilder objects. */
  _schema?: WasmSchema;
}

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

  return queueMicrotask;
}

function resolveDefaultDurabilityTier(context: AppContext): DurabilityTier {
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

function sessionFromRequest(request: RequestLike): Session {
  const authHeader = readHeader(request, "authorization");
  if (!authHeader?.startsWith("Bearer ")) {
    throw new Error("Missing or invalid Authorization header");
  }

  const token = authHeader.slice("Bearer ".length).trim();
  const parts = token.split(".");
  if (parts.length < 2) {
    throw new Error("Invalid JWT format");
  }

  let payload: unknown;
  try {
    payload = JSON.parse(decodeBase64Url(parts[1]));
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
  async create(table: string, values: Value[]): Promise<string> {
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

    // Convert updates object to array of tuples
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

  private constructor(
    runtime: Runtime,
    context: AppContext,
    defaultDurabilityTier: DurabilityTier,
  ) {
    this.runtime = runtime;
    this.scheduler = getScheduler();
    this.context = context;
    this.defaultDurabilityTier = defaultDurabilityTier;
    this.resolvedSession = resolveJwtSession(context.jwtToken ?? "");
    this.streamController = createRuntimeSyncStreamController({
      getRuntime: () => this.runtime,
      getAuth: () => this.getSyncAuth(),
      getClientId: () => this.serverClientId,
      setClientId: (clientId) => {
        this.serverClientId = clientId;
      },
    });
  }

  /**
   * Connect to Jazz with the given context.
   *
   * @param context Application context with driver and schema
   * @returns Connected JazzClient instance
   */
  static async connect(context: AppContext): Promise<JazzClient> {
    const resolvedContext = resolveLocalAuthDefaults(context);

    // Load WASM module dynamically
    const wasmModule = await loadWasmModule();

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
  static connectWithRuntime(runtime: Runtime, context: AppContext): JazzClient {
    const client = new JazzClient(runtime, context, resolveDefaultDurabilityTier(context));

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
   * const id = await userClient.create("todos", [{ type: "Text", value: "Buy milk" }]);
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

  private getSyncAuth(): SyncAuth {
    if (this.useBackendSyncAuth) {
      return {
        backendSecret: this.context.backendSecret,
      };
    }

    return {
      jwtToken: this.context.jwtToken,
      localAuthMode: this.context.localAuthMode,
      localAuthToken: this.context.localAuthToken,
      adminSecret: this.context.adminSecret,
    };
  }

  private normalizeQueryExecutionOptions(options?: QueryExecutionOptions): QueryExecutionOptions {
    return {
      tier: options?.tier ?? this.defaultDurabilityTier,
      localUpdates: options?.localUpdates ?? "immediate",
      propagation: options?.propagation ?? "full",
    };
  }

  private resolveWriteTier(options?: WriteDurabilityOptions): DurabilityTier {
    return options?.tier ?? this.defaultDurabilityTier;
  }

  /**
   * Insert a new row into a table without waiting for durability.
   */
  create(table: string, values: Value[]): Row {
    return this.runtime.insert(table, values);
  }

  /**
   * Insert a new row into a table and wait for durability at the requested tier.
   */
  async createDurable(
    table: string,
    values: Value[],
    options?: WriteDurabilityOptions,
  ): Promise<Row> {
    const tier = this.resolveWriteTier(options);
    return this.runtime.insertDurable(table, values, tier);
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
    const queryJson = resolveQueryJson(query);
    const sessionJson = session ? JSON.stringify(session) : undefined;
    const optionsJson = encodeQueryExecutionOptions(normalizedOptions);
    const results = await this.runtime.query(
      queryJson,
      sessionJson,
      normalizedOptions.tier,
      optionsJson,
    );
    return results as Row[];
  }

  /**
   * Update a row by ID and wait for durability at the requested tier.
   */
  async update(
    objectId: string,
    updates: Record<string, Value>,
    options?: WriteDurabilityOptions,
  ): Promise<void> {
    const tier = this.resolveWriteTier(options);
    await this.runtime.updateDurable(objectId, updates, tier);
  }

  /**
   * Delete a row by ID and wait for durability at the requested tier.
   */
  async delete(objectId: string, options?: WriteDurabilityOptions): Promise<void> {
    const tier = this.resolveWriteTier(options);
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

    const handle = this.runtime.createSubscription(
      queryJson,
      sessionJson,
      normalizedOptions.tier,
      optionsJson,
    );

    this.scheduler(() => {
      this.runtime.executeSubscription(handle, (deltaJsonOrObject: RowDelta | string) => {
        const delta: RowDelta =
          typeof deltaJsonOrObject === "string" ? JSON.parse(deltaJsonOrObject) : deltaJsonOrObject;
        callback(delta);
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
    return this.runtime.getSchema() as WasmSchema;
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
   * Link an anonymous/demo local principal to an external JWT identity.
   *
   * Requires all three auth fields:
   * - `jwtToken`
   * - `localAuthMode`
   * - `localAuthToken`
   *
   * Values default to the current AppContext auth fields unless overridden.
   */
  async linkExternalIdentity(
    options: LinkExternalIdentityOptions = {},
  ): Promise<LinkExternalIdentityResult> {
    if (!this.context.serverUrl) {
      throw new Error("No server connection");
    }

    const jwtToken = options.jwtToken ?? this.context.jwtToken;
    const localAuthMode = options.localAuthMode ?? this.context.localAuthMode;
    const localAuthToken = options.localAuthToken ?? this.context.localAuthToken;

    if (!jwtToken) {
      throw new Error("linkExternalIdentity requires jwtToken");
    }
    if (!localAuthMode) {
      throw new Error("linkExternalIdentity requires localAuthMode");
    }
    if (!localAuthToken) {
      throw new Error("linkExternalIdentity requires localAuthToken");
    }

    return sendLinkExternalIdentityRequest(
      this.context.serverUrl,
      {
        jwtToken,
        localAuthMode,
        localAuthToken,
        pathPrefix: this.context.serverPathPrefix,
      },
      "[client] ",
    );
  }

  /**
   * Shutdown the client and release resources.
   */
  async shutdown(): Promise<void> {
    this.streamController.stop();

    // Close runtime if it supports explicit shutdown (e.g., NapiRuntime).
    if (this.runtime.close) {
      await this.runtime.close();
    }
  }

  private setupSync(serverUrl: string, serverPathPrefix?: string): void {
    this.runtime.onSyncMessageToSend(
      createSyncOutboxRouter({
        logPrefix: "[client] ",
        retryServerPayloads: true,
        onServerPayload: (payload, isCatalogue) =>
          this.sendSyncMessage(payload as string, isCatalogue),
        onServerPayloadError: (error) => {
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

  private async sendSyncMessage(payloadJson: string, isCatalogue: boolean): Promise<void> {
    const serverUrl = this.streamController.getServerUrl();
    if (!serverUrl) return;

    await sendSyncPayload(
      serverUrl,
      payloadJson,
      isCatalogue,
      {
        ...this.getSyncAuth(),
        clientId: this.serverClientId,
        pathPrefix: this.streamController.getPathPrefix(),
      },
      "[client] ",
    );
  }
}

/**
 * WASM module type for sync client creation.
 * This is the type of the jazz-wasm module after dynamic import.
 */
export type WasmModule = typeof import("jazz-wasm");

/**
 * Load and initialize the WASM module.
 *
 * Exported so that `createDb()` can pre-load the module for sync mutations.
 */
export async function loadWasmModule(): Promise<WasmModule> {
  // Cast to any — wasm-bindgen glue exports (default, initSync) aren't in .d.ts
  const wasmModule: any = await import("jazz-wasm");

  // In Node.js, we need to read the .wasm file and use initSync.
  // In browsers/React Native, the default fetch-based init works (or default()).
  // Use try/catch so we skip the Node path when node:* modules are unavailable (e.g. RN).
  let nodeInitDone = false;
  if (typeof process !== "undefined" && process.versions?.node) {
    try {
      const { existsSync, readFileSync } = await import("node:fs");
      const { createRequire } = await import("node:module");
      const { dirname, resolve } = await import("node:path");

      const require = createRequire(import.meta.url);
      const packageJsonPath = require.resolve("jazz-wasm/package.json");
      const packageDir = dirname(packageJsonPath);
      const wasmPath = resolve(packageDir, "pkg/jazz_wasm_bg.wasm");

      if (existsSync(wasmPath)) {
        wasmModule.initSync({ module: readFileSync(wasmPath) });
        nodeInitDone = true;
      }
    } catch {
      // Node modules unavailable (e.g. React Native with process polyfill)
    }
  }
  if (!nodeInitDone && typeof wasmModule.default === "function") {
    await wasmModule.default();
  }

  return wasmModule;
}
