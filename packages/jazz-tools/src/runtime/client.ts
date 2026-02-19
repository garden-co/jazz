/**
 * JazzClient - High-level TypeScript client for Jazz.
 *
 * Wraps the WASM runtime and provides a clean API for CRUD operations,
 * subscriptions, and sync.
 */

import type { AppContext, Session } from "./context.js";
import type { Value, RowDelta, WasmSchema } from "../drivers/types.js";
import {
  sendSyncPayload,
  readBinaryFrames,
  generateClientId,
  buildEventsUrl,
  buildEndpointUrl,
  applyUserAuthHeaders,
  linkExternalIdentity as sendLinkExternalIdentityRequest,
  type LinkExternalResponse,
} from "./sync-transport.js";

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
  ): Promise<any>;
  subscribe(
    query_json: string,
    on_update: Function,
    session_json?: string | null,
    settled_tier?: string | null,
  ): number;
  unsubscribe(handle: number): void;
  insertPersisted(table: string, values: any, tier: string): Promise<string>;
  updatePersisted(object_id: string, values: any, tier: string): Promise<void>;
  deletePersisted(object_id: string, tier: string): Promise<void>;
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
  async query(queryJson: string): Promise<Row[]> {
    return this.client.queryInternal(queryJson, this.session);
  }

  /**
   * Subscribe to a query as this session's user.
   */
  subscribe(queryJson: string, callback: SubscriptionCallback): number {
    return this.client.subscribeInternal(queryJson, callback, this.session);
  }
}

/**
 * High-level Jazz client.
 */
export class JazzClient {
  private runtime: Runtime;
  private streamAbortController: AbortController | null = null;
  private reconnectTimer: ReturnType<typeof setTimeout> | null = null;
  private reconnectAttempt = 0;
  private streamConnecting = false;
  private streamAttached = false;
  private serverClientId: string = generateClientId();
  private activeServerUrl: string | null = null;
  private activeServerPathPrefix: string | undefined;
  private subscriptions = new Map<number, SubscriptionCallback>();
  private context: AppContext;

  private constructor(runtime: Runtime, context: AppContext) {
    this.runtime = runtime;
    this.context = context;
  }

  /**
   * Connect to Jazz with the given context.
   *
   * @param context Application context with driver and schema
   * @returns Connected JazzClient instance
   */
  static async connect(context: AppContext): Promise<JazzClient> {
    // Load WASM module dynamically
    const wasmModule = await loadWasmModule();

    // Create WASM runtime (storage is now synchronous in-memory)
    const schemaJson = JSON.stringify(context.schema);
    const runtime = new wasmModule.WasmRuntime(
      schemaJson,
      context.appId,
      context.env ?? "dev",
      context.userBranch ?? "main",
      context.tier,
    );

    const client = new JazzClient(runtime, context);

    // Set up sync if server URL provided
    if (context.serverUrl) {
      client.setupSync(context.serverUrl, context.serverPathPrefix);
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
    // Create WASM runtime (storage is now synchronous in-memory)
    const schemaJson = JSON.stringify(context.schema);
    const runtime = new wasmModule.WasmRuntime(
      schemaJson,
      context.appId,
      context.env ?? "dev",
      context.userBranch ?? "main",
      context.tier,
    );

    const client = new JazzClient(runtime, context);

    // Set up sync if server URL provided
    if (context.serverUrl) {
      client.setupSync(context.serverUrl, context.serverPathPrefix);
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
   * Insert a row and wait for persistence at the specified tier.
   *
   * @param table Table name
   * @param values Array of column values
   * @param tier Persistence tier to wait for
   * @returns Promise resolving to the new row's ID when the tier acks
   */
  async createPersisted(table: string, values: Value[], tier: PersistenceTier): Promise<string> {
    return this.runtime.insertPersisted(table, values, tier);
  }

  /**
   * Execute a query and return all matching rows.
   *
   * @param queryJson JSON-encoded query specification
   * @param settledTier Optional tier to hold delivery until confirmed
   * @returns Array of matching rows
   */
  async query(queryJson: string, settledTier?: PersistenceTier): Promise<Row[]> {
    return this.queryInternal(queryJson, undefined, settledTier);
  }

  /**
   * Internal query with optional session and settled tier.
   * @internal
   */
  async queryInternal(
    queryJson: string,
    session?: Session,
    settledTier?: PersistenceTier,
  ): Promise<Row[]> {
    const sessionJson = session ? JSON.stringify(session) : undefined;
    const results = await this.runtime.query(queryJson, sessionJson, settledTier);
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
   * Update a row and wait for persistence at the specified tier.
   */
  async updatePersisted(
    objectId: string,
    updates: Record<string, Value>,
    tier: PersistenceTier,
  ): Promise<void> {
    await this.runtime.updatePersisted(objectId, updates, tier);
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
   * Delete a row and wait for persistence at the specified tier.
   */
  async deletePersisted(objectId: string, tier: PersistenceTier): Promise<void> {
    await this.runtime.deletePersisted(objectId, tier);
  }

  /**
   * Subscribe to a query and receive updates when results change.
   *
   * @param queryJson JSON-encoded query specification
   * @param callback Called with delta whenever results change
   * @param settledTier Optional tier to hold initial delivery until confirmed
   * @returns Subscription ID for unsubscribing
   */
  subscribe(
    queryJson: string,
    callback: SubscriptionCallback,
    settledTier?: PersistenceTier,
  ): number {
    return this.subscribeInternal(queryJson, callback, undefined, settledTier);
  }

  /**
   * Internal subscribe with optional session and settled tier.
   * @internal
   */
  subscribeInternal(
    queryJson: string,
    callback: SubscriptionCallback,
    session?: Session,
    settledTier?: PersistenceTier,
  ): number {
    const sessionJson = session ? JSON.stringify(session) : undefined;
    const subId = this.runtime.subscribe(
      queryJson,
      (deltaJsonOrObject: RowDelta | string) => {
        // WASM runtime passes delta as JSON string, need to parse it
        const delta: RowDelta =
          typeof deltaJsonOrObject === "string" ? JSON.parse(deltaJsonOrObject) : deltaJsonOrObject;
        callback(delta);
      },
      sessionJson,
      settledTier,
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
    return this.runtime.getSchema();
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
  getSchemaContext(): { env: string; schema_hash: string; user_branch: string } {
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
    this.activeServerUrl = null;
    this.detachServer();
    if (this.reconnectTimer) {
      clearTimeout(this.reconnectTimer);
      this.reconnectTimer = null;
    }

    // Abort stream connection
    if (this.streamAbortController) {
      this.streamAbortController.abort();
      this.streamAbortController = null;
    }

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
    this.activeServerUrl = serverUrl;
    this.activeServerPathPrefix = serverPathPrefix;

    // Set up outgoing message handler
    this.runtime.onSyncMessageToSend((envelope: string) => {
      // Envelope is now {destination, payload}
      const parsed = JSON.parse(envelope);
      const payload = parsed.payload;

      // Only send server-bound messages
      if (parsed.destination && "Server" in parsed.destination) {
        void this.sendSyncMessage(serverUrl, payload).catch((error) => {
          console.error("Sync POST error:", error);
          this.detachServer();
          this.scheduleReconnect();
        });
      }
    });

    // Connect to binary stream for incoming messages
    this.connectStream();
  }

  private async sendSyncMessage(serverUrl: string, payload: any): Promise<void> {
    await sendSyncPayload(serverUrl, payload, {
      jwtToken: this.context.jwtToken,
      localAuthMode: this.context.localAuthMode,
      localAuthToken: this.context.localAuthToken,
      adminSecret: this.context.adminSecret,
      clientId: this.serverClientId,
      pathPrefix: this.activeServerPathPrefix,
    });
  }

  private detachServer(): void {
    if (!this.streamAttached) return;
    this.runtime.removeServer();
    this.streamAttached = false;
  }

  private attachServer(): void {
    // Re-attach every time the stream reconnects so query subscriptions replay.
    if (this.streamAttached) {
      this.runtime.removeServer();
    }
    this.runtime.addServer();
    this.streamAttached = true;
    this.reconnectAttempt = 0;
  }

  private scheduleReconnect(): void {
    if (!this.activeServerUrl) return;
    if (this.reconnectTimer) return;

    const baseMs = 300;
    const maxMs = 10_000;
    const jitterMs = Math.floor(Math.random() * 200);
    const delayMs = Math.min(maxMs, baseMs * 2 ** this.reconnectAttempt) + jitterMs;
    this.reconnectAttempt += 1;

    this.reconnectTimer = setTimeout(() => {
      this.reconnectTimer = null;
      this.connectStream();
    }, delayMs);
  }

  /**
   * Connect to binary streaming endpoint for incoming messages.
   *
   * Uses length-prefixed binary frames over a long-lived HTTP response.
   * Supports auth via Authorization header (unlike EventSource).
   */
  private async connectStream(): Promise<void> {
    if (this.streamConnecting || !this.activeServerUrl) return;
    this.streamConnecting = true;

    const serverUrl = this.activeServerUrl;
    const headers: Record<string, string> = {
      Accept: "application/octet-stream",
    };
    applyUserAuthHeaders(headers, {
      jwtToken: this.context.jwtToken,
      localAuthMode: this.context.localAuthMode,
      localAuthToken: this.context.localAuthToken,
    });

    this.streamAbortController = new AbortController();

    try {
      const eventsUrl = buildEventsUrl(serverUrl, this.serverClientId, this.activeServerPathPrefix);

      const response = await fetch(eventsUrl, {
        headers,
        signal: this.streamAbortController.signal,
      });

      if (!response.ok) {
        console.error(`Stream connect failed: ${response.status}`);
        this.detachServer();
        this.streamConnecting = false;
        this.scheduleReconnect();
        return;
      }

      const reader = response.body!.getReader();
      let connected = false;
      await readBinaryFrames(reader, {
        onSyncMessage: (json) => this.runtime.onSyncMessageReceived(json),
        onConnected: (clientId) => {
          this.serverClientId = clientId;
          if (!connected) {
            connected = true;
            this.attachServer();
          }
        },
      });
    } catch (e: any) {
      if (e?.name === "AbortError") return; // Intentional shutdown
      console.error("Stream error:", e);
    } finally {
      this.streamConnecting = false;
    }

    // Reconnect after delay (unless aborted)
    if (this.streamAbortController && !this.streamAbortController.signal.aborted) {
      this.detachServer();
      this.scheduleReconnect();
    }
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

  // In Node.js, we need to read the .wasm file and use initSync
  // In browsers, the default fetch-based init works
  if (typeof process !== "undefined" && process.versions?.node) {
    const { readFileSync } = await import("node:fs");
    const { fileURLToPath } = await import("node:url");
    const { dirname, join } = await import("node:path");

    // Find the .wasm file relative to the jazz-wasm package
    const wasmPath = join(
      dirname(fileURLToPath(import.meta.url)),
      "../../node_modules/jazz-wasm/pkg/jazz_wasm_bg.wasm",
    );
    const wasmBytes = readFileSync(wasmPath);
    wasmModule.initSync(wasmBytes);
  } else if (typeof wasmModule.default === "function") {
    // In browsers without a bundler WASM plugin, call the init function.
    // With vite-plugin-wasm, init happens at import time and default is not a function.
    await wasmModule.default();
  }

  return wasmModule;
}
