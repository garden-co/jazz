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
  type LinkExternalResponse,
} from "./sync-transport.js";
import { resolveLocalAuthDefaults } from "./local-auth.js";
import { resolveJwtSession } from "./client-session.js";

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
  insert(table: string, values: any): string;
  update(object_id: string, values: any): void;
  delete(object_id: string): void;
  query(
    query_json: string,
    session_json?: string | null,
    settled_tier?: string | null,
    options_json?: string | null,
  ): Promise<any>;
  subscribe(
    query_json: string,
    on_update: Function,
    session_json?: string | null,
    settled_tier?: string | null,
    options_json?: string | null,
  ): number;
  unsubscribe(handle: number): void;
  insertWithAck(table: string, values: any, tier: string): Promise<string>;
  updateWithAck(object_id: string, values: any, tier: string): Promise<void>;
  deleteWithAck(object_id: string, tier: string): Promise<void>;
  onSyncMessageReceived(message_json: string): void;
  onSyncMessageToSend(callback: Function): void;
  addServer(): void;
  removeServer(): void;
  addClient(): string;
  getSchema(): any;
  getSchemaHash(): string;
  close?(): void | Promise<void>;
  setClientRole?(client_id: string, role: string): void;
  onSyncMessageReceivedFromClient?(client_id: string, message_json: string): void;
}

/**
 * Persistence tier for durability guarantees.
 *
 * - `worker`: Persisted in web worker / local storage
 * - `edge`: Persisted at edge server
 * - `core`: Persisted at core server
 */
export type PersistenceTier = "worker" | "edge" | "core";
export type QueryPropagation = "full" | "local-only";
export interface QueryExecutionOptions {
  settledTier?: PersistenceTier;
  propagation?: QueryPropagation;
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

/**
 * QueryBuilder-compatible input accepted by query and subscribe APIs.
 */
export interface QueryInput {
  _build(): string;
}

function resolveQueryJson(query: string | QueryInput): string {
  return typeof query === "string" ? query : query._build();
}

function normalizeQueryExecutionOptions(options?: QueryExecutionOptions): QueryExecutionOptions {
  if (!options) {
    return { propagation: "full" };
  }

  return {
    settledTier: options.settledTier,
    propagation: options.propagation ?? "full",
  };
}

function encodeQueryExecutionOptions(options: QueryExecutionOptions): string | undefined {
  if ((options.propagation ?? "full") === "full") {
    return undefined;
  }

  return JSON.stringify({
    propagation: options.propagation,
  });
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
    return this.client.queryInternal(resolveQueryJson(query), this.session, options);
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
  private subscriptions = new Map<number, SubscriptionCallback>();
  private context: AppContext;
  private resolvedSession: Session | null;

  private constructor(runtime: Runtime, context: AppContext) {
    this.runtime = runtime;
    this.context = context;
    this.resolvedSession = resolveJwtSession(context.jwtToken ?? "");
    this.streamController = createRuntimeSyncStreamController({
      getRuntime: () => this.runtime,
      getAuth: () => ({
        jwtToken: this.context.jwtToken,
        localAuthMode: this.context.localAuthMode,
        localAuthToken: this.context.localAuthToken,
      }),
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
      resolvedContext.tier,
    );

    const client = new JazzClient(runtime, resolvedContext);

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
  static connectSync(wasmModule: WasmModule, context: AppContext): JazzClient {
    const resolvedContext = resolveLocalAuthDefaults(context);

    // Create WASM runtime (storage is now synchronous in-memory)
    const schemaJson = serializeRuntimeSchema(resolvedContext.schema);
    const runtime = new wasmModule.WasmRuntime(
      schemaJson,
      resolvedContext.appId,
      resolvedContext.env ?? "dev",
      resolvedContext.userBranch ?? "main",
      resolvedContext.tier,
    );

    const client = new JazzClient(runtime, resolvedContext);

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
    const client = new JazzClient(runtime, context);

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
   * Insert a new row into a table (sync, fire-and-forget).
   *
   * @param table Table name
   * @param values Array of column values
   * @returns The new row's ID (UUID string)
   */
  create(table: string, values: Value[]): string {
    return this.runtime.insert(table, values);
  }

  /**
   * Insert a row and wait for acknowledgement at the specified tier.
   *
   * @param table Table name
   * @param values Array of column values
   * @param tier Acknowledgement tier to wait for
   * @returns Promise resolving to the new row's ID when the tier acknowledges
   */
  async createWithAck(table: string, values: Value[], tier: PersistenceTier): Promise<string> {
    return this.runtime.insertWithAck(table, values, tier);
  }

  /**
   * Execute a query and return all matching rows.
   *
   * @param query Query builder or JSON-encoded query specification
   * @param settledTier Optional tier to hold delivery until confirmed
   * @returns Array of matching rows
   */
  async query(query: string | QueryInput, options?: QueryExecutionOptions): Promise<Row[]> {
    return this.queryInternal(resolveQueryJson(query), this.resolvedSession ?? undefined, options);
  }

  /**
   * Internal query with optional session and settled tier.
   * @internal
   */
  async queryInternal(
    queryJson: string,
    session?: Session,
    options?: QueryExecutionOptions,
  ): Promise<Row[]> {
    const normalizedOptions = normalizeQueryExecutionOptions(options);
    const sessionJson = session ? JSON.stringify(session) : undefined;
    const optionsJson = encodeQueryExecutionOptions(normalizedOptions);
    const results = await this.runtime.query(
      queryJson,
      sessionJson,
      normalizedOptions.settledTier,
      optionsJson,
    );
    return results as Row[];
  }

  /**
   * Update a row by ID (sync, fire-and-forget).
   *
   * @param objectId Row ID (UUID string)
   * @param updates Object mapping column names to new values
   */
  update(objectId: string, updates: Record<string, Value>): void {
    this.runtime.update(objectId, updates);
  }

  /**
   * Update a row and wait for acknowledgement at the specified tier.
   */
  async updateWithAck(
    objectId: string,
    updates: Record<string, Value>,
    tier: PersistenceTier,
  ): Promise<void> {
    await this.runtime.updateWithAck(objectId, updates, tier);
  }

  /**
   * Delete a row by ID (sync, fire-and-forget).
   *
   * @param objectId Row ID (UUID string)
   */
  delete(objectId: string): void {
    this.runtime.delete(objectId);
  }

  /**
   * Delete a row and wait for acknowledgement at the specified tier.
   */
  async deleteWithAck(objectId: string, tier: PersistenceTier): Promise<void> {
    await this.runtime.deleteWithAck(objectId, tier);
  }

  /**
   * Subscribe to a query and receive updates when results change.
   *
   * @param query Query builder or JSON-encoded query specification
   * @param callback Called with delta whenever results change
   * @param settledTier Optional tier to hold initial delivery until confirmed
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
   * Internal subscribe with optional session and settled tier.
   * @internal
   */
  subscribeInternal(
    query: string | QueryInput,
    callback: SubscriptionCallback,
    session?: Session,
    options?: QueryExecutionOptions,
  ): number {
    const normalizedOptions = normalizeQueryExecutionOptions(options);
    const sessionJson = session ? JSON.stringify(session) : undefined;
    const queryJson = resolveQueryJson(query);
    const optionsJson = encodeQueryExecutionOptions(normalizedOptions);
    const subId = this.runtime.subscribe(
      queryJson,
      (deltaJsonOrObject: RowDelta | string) => {
        // WASM runtime passes delta as JSON string, need to parse it
        const delta: RowDelta =
          typeof deltaJsonOrObject === "string" ? JSON.parse(deltaJsonOrObject) : deltaJsonOrObject;
        callback(delta);
      },
      sessionJson,
      normalizedOptions.settledTier,
      optionsJson,
    );
    this.subscriptions.set(subId, callback);
    return subId;
  }

  /**
   * Unsubscribe from a query.
   *
   * @param subscriptionId ID returned from subscribe()
   */
  unsubscribe(subscriptionId: number): void {
    this.runtime.unsubscribe(subscriptionId);
    this.subscriptions.delete(subscriptionId);
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

    // Close driver if it supports it
    if (this.context.driver?.close) {
      await this.context.driver.close();
    }

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
        onServerPayload: (payload) => this.sendSyncMessage(payload),
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

  private async sendSyncMessage(payload: unknown): Promise<void> {
    const serverUrl = this.streamController.getServerUrl();
    if (!serverUrl) return;

    await sendSyncPayload(serverUrl, payload, {
      jwtToken: this.context.jwtToken,
      localAuthMode: this.context.localAuthMode,
      localAuthToken: this.context.localAuthToken,
      adminSecret: this.context.adminSecret,
      clientId: this.serverClientId,
      pathPrefix: this.streamController.getPathPrefix(),
    });
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
