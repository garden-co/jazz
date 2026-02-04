/**
 * JazzClient - High-level TypeScript client for Jazz.
 *
 * Wraps the WASM runtime and provides a clean API for CRUD operations,
 * subscriptions, and sync.
 */

import type { AppContext, Session } from "./context.js";
import type { Value, RowDelta, WasmSchema, StorageRequest } from "../drivers/types.js";
import type { WasmRuntime as GrooveWasmRuntime } from "groove-wasm";

// Re-type the WasmRuntime to work with our local types
type WasmRuntime = GrooveWasmRuntime;

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
    const serverUrl = this.client.getServerUrl();
    if (!serverUrl) {
      throw new Error("No server connection");
    }

    const response = await this.client.sendRequest(
      `${serverUrl}/sync/object`,
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
    const serverUrl = this.client.getServerUrl();
    if (!serverUrl) {
      throw new Error("No server connection");
    }

    // Convert updates object to array of tuples
    const updateArray = Object.entries(updates);

    const response = await this.client.sendRequest(
      `${serverUrl}/sync/object`,
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
    const serverUrl = this.client.getServerUrl();
    if (!serverUrl) {
      throw new Error("No server connection");
    }

    const response = await this.client.sendRequest(
      `${serverUrl}/sync/object/delete`,
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
  private runtime: WasmRuntime;
  private sseConnection: EventSource | null = null;
  private subscriptions = new Map<number, SubscriptionCallback>();
  private context: AppContext;

  private constructor(runtime: WasmRuntime, context: AppContext) {
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

    // Create storage callback that bridges WASM requests to the JS driver
    // With tsify, requests come as typed JS objects (not JSON strings)
    const storageCallback = async (request: StorageRequest) => {
      try {
        // Driver expects an array of requests
        const responses = await context.driver.process([request]);
        // Return first response as object (not JSON string)
        if (responses.length > 0) {
          runtime.onStorageResponse(responses[0]);
        }
      } catch (e) {
        console.error("Storage callback error:", e);
      }
    };

    // Create WASM runtime with storage callback
    const schemaJson = JSON.stringify(context.schema);
    const runtime = new wasmModule.WasmRuntime(
      storageCallback,
      schemaJson,
      context.appId,
      context.env ?? "dev",
      context.userBranch ?? "main",
    );

    const client = new JazzClient(runtime, context);

    // Set up sync if server URL provided
    if (context.serverUrl) {
      client.setupSync(context.serverUrl);
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
    // Create storage callback that bridges WASM requests to the JS driver
    // With tsify, requests come as typed JS objects (not JSON strings)
    // Note: runtime is accessed via closure after creation
    let runtime: WasmRuntime;
    const storageCallback = async (request: StorageRequest) => {
      try {
        // Driver expects an array of requests
        const responses = await context.driver.process([request]);
        // Return first response as object (not JSON string)
        if (responses.length > 0) {
          runtime.onStorageResponse(responses[0]);
        }
      } catch (e) {
        console.error("Storage callback error:", e);
      }
    };

    // Create WASM runtime with storage callback
    const schemaJson = JSON.stringify(context.schema);
    runtime = new wasmModule.WasmRuntime(
      storageCallback,
      schemaJson,
      context.appId,
      context.env ?? "dev",
      context.userBranch ?? "main",
    );

    const client = new JazzClient(runtime, context);

    // Set up sync if server URL provided
    if (context.serverUrl) {
      client.setupSync(context.serverUrl);
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
   * Insert a new row into a table.
   *
   * @param table Table name
   * @param values Array of column values
   * @returns The new row's ID (UUID string)
   */
  create(table: string, values: Value[]): string {
    return this.runtime.insert(table, values);
  }

  /**
   * Execute a query and return all matching rows.
   *
   * @param queryJson JSON-encoded query specification
   * @returns Array of matching rows
   */
  async query(queryJson: string): Promise<Row[]> {
    return this.queryInternal(queryJson, undefined);
  }

  /**
   * Internal query with optional session.
   * @internal
   */
  async queryInternal(queryJson: string, session?: Session): Promise<Row[]> {
    const sessionJson = session ? JSON.stringify(session) : undefined;
    const results = await this.runtime.query(queryJson, sessionJson);
    return results as Row[];
  }

  /**
   * Update a row by ID.
   *
   * @param objectId Row ID (UUID string)
   * @param updates Object mapping column names to new values
   */
  update(objectId: string, updates: Record<string, Value>): void {
    this.runtime.update(objectId, updates);
  }

  /**
   * Delete a row by ID.
   *
   * @param objectId Row ID (UUID string)
   */
  delete(objectId: string): void {
    this.runtime.delete(objectId);
  }

  /**
   * Subscribe to a query and receive updates when results change.
   *
   * @param queryJson JSON-encoded query specification
   * @param callback Called with delta whenever results change
   * @returns Subscription ID for unsubscribing
   */
  subscribe(queryJson: string, callback: SubscriptionCallback): number {
    return this.subscribeInternal(queryJson, callback, undefined);
  }

  /**
   * Internal subscribe with optional session.
   * @internal
   */
  subscribeInternal(queryJson: string, callback: SubscriptionCallback, session?: Session): number {
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
   * Get the server URL (for SessionClient).
   * @internal
   */
  getServerUrl(): string | undefined {
    return this.context.serverUrl;
  }

  /**
   * Get schema context for server requests.
   * @internal
   */
  getSchemaContext(): { env: string; schema_hash: string; user_branch: string } {
    // TODO: Compute actual schema hash
    return {
      env: this.context.env ?? "dev",
      schema_hash: "0".repeat(64), // Placeholder - should compute from schema
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
    // Priority 2: Frontend JWT auth
    else if (this.context.jwtToken) {
      headers["Authorization"] = `Bearer ${this.context.jwtToken}`;
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
    // Close SSE connection
    if (this.sseConnection) {
      this.sseConnection.close();
      this.sseConnection = null;
    }

    // Close driver if it supports it
    if (this.context.driver.close) {
      await this.context.driver.close();
    }
  }

  private setupSync(serverUrl: string): void {
    // Set up outgoing message handler
    this.runtime.onSyncMessageToSend((message: string) => {
      this.sendSyncMessage(serverUrl, message);
    });

    // Connect to SSE endpoint for incoming messages
    this.connectSSE(serverUrl);

    // Register server connection
    this.runtime.addServer();
  }

  private async sendSyncMessage(serverUrl: string, message: string): Promise<void> {
    try {
      const headers: Record<string, string> = {
        "Content-Type": "application/json",
      };

      // Check if this is a catalogue sync - add admin header
      const parsed = JSON.parse(message);
      if (this.isCataloguePayload(parsed)) {
        if (this.context.adminSecret) {
          headers["X-Jazz-Admin-Secret"] = this.context.adminSecret;
        }
      }
      // Otherwise use JWT if available
      else if (this.context.jwtToken) {
        headers["Authorization"] = `Bearer ${this.context.jwtToken}`;
      }

      const response = await fetch(`${serverUrl}/sync`, {
        method: "POST",
        headers,
        body: message,
      });

      if (!response.ok) {
        console.error("Sync send error:", response.statusText);
      }
    } catch (e) {
      console.error("Sync send error:", e);
    }
  }

  /**
   * Check if a sync payload is for a catalogue object (schema or lens).
   */
  private isCataloguePayload(payload: {
    payload?: { ObjectUpdated?: { metadata?: { metadata?: Record<string, string> } } };
  }): boolean {
    const metadata = payload?.payload?.ObjectUpdated?.metadata?.metadata;
    if (metadata) {
      const type = metadata["type"];
      return type === "catalogue_schema" || type === "catalogue_lens";
    }
    return false;
  }

  private connectSSE(serverUrl: string): void {
    const eventsUrl = `${serverUrl}/events`;
    this.sseConnection = new EventSource(eventsUrl);

    this.sseConnection.onmessage = (event) => {
      try {
        const data = JSON.parse(event.data);

        // Handle different event types
        if (data.type === "SyncUpdate") {
          // Pass the payload to the runtime
          this.runtime.onSyncMessageReceived(JSON.stringify(data.payload));
        }
      } catch (e) {
        console.error("SSE parse error:", e);
      }
    };

    this.sseConnection.onerror = (e) => {
      console.error("SSE error:", e);
      // Attempt to reconnect after a delay
      setTimeout(() => {
        if (this.sseConnection) {
          this.sseConnection.close();
          this.connectSSE(serverUrl);
        }
      }, 5000);
    };
  }
}

/**
 * WASM module type for sync client creation.
 * This is the type of the groove-wasm module after dynamic import.
 */
export type WasmModule = typeof import("groove-wasm");

/**
 * Load and initialize the WASM module.
 *
 * Exported so that `createDb()` can pre-load the module for sync mutations.
 */
export async function loadWasmModule(): Promise<WasmModule> {
  const wasmModule = await import("groove-wasm");

  // In Node.js, we need to read the .wasm file and use initSync
  // In browsers, the default fetch-based init works
  if (typeof process !== "undefined" && process.versions?.node) {
    const { readFileSync } = await import("node:fs");
    const { fileURLToPath } = await import("node:url");
    const { dirname, join } = await import("node:path");

    // Find the .wasm file relative to the groove-wasm package
    const wasmPath = join(
      dirname(fileURLToPath(import.meta.url)),
      "../../node_modules/groove-wasm/groove_wasm_bg.wasm",
    );
    const wasmBytes = readFileSync(wasmPath);
    wasmModule.initSync(wasmBytes);
  } else {
    await wasmModule.default();
  }

  return wasmModule;
}
