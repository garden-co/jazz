/**
 * JazzClient - High-level TypeScript client for Jazz.
 *
 * Wraps the WASM runtime and provides a clean API for CRUD operations,
 * subscriptions, and sync.
 */
import type { AppContext, Session } from "./context.js";
import type { Value, RowDelta, WasmSchema } from "../drivers/types.js";
import { type LinkExternalResponse } from "./sync-transport.js";
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
  ): Promise<any>;
  subscribe(
    query_json: string,
    on_update: Function,
    session_json?: string | null,
    settled_tier?: string | null,
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
/**
 * Session-scoped client for backend operations.
 *
 * Created by `JazzClient.forSession()`. Allows backend applications
 * to perform operations as a specific user via header-based authentication.
 */
export declare class SessionClient {
  private client;
  private session;
  constructor(client: JazzClient, session: Session);
  /**
   * Create a new row as this session's user.
   */
  create(table: string, values: Value[]): Promise<string>;
  /**
   * Update a row as this session's user.
   */
  update(objectId: string, updates: Record<string, Value>): Promise<void>;
  /**
   * Delete a row as this session's user.
   */
  delete(objectId: string): Promise<void>;
  /**
   * Query as this session's user.
   */
  query(query: string | QueryInput): Promise<Row[]>;
  /**
   * Subscribe to a query as this session's user.
   */
  subscribe(query: string | QueryInput, callback: SubscriptionCallback): number;
}
/**
 * High-level Jazz client.
 */
export declare class JazzClient {
  private runtime;
  private streamController;
  private serverClientId;
  private subscriptions;
  private context;
  private resolvedSession;
  private constructor();
  /**
   * Connect to Jazz with the given context.
   *
   * @param context Application context with driver and schema
   * @returns Connected JazzClient instance
   */
  static connect(context: AppContext): Promise<JazzClient>;
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
  static connectSync(wasmModule: WasmModule, context: AppContext): JazzClient;
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
  static connectWithRuntime(runtime: Runtime, context: AppContext): JazzClient;
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
  forSession(session: Session): SessionClient;
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
  forRequest(request: RequestLike): SessionClient;
  /**
   * Insert a new row into a table (sync, fire-and-forget).
   *
   * @param table Table name
   * @param values Array of column values
   * @returns The new row's ID (UUID string)
   */
  create(table: string, values: Value[]): string;
  /**
   * Insert a row and wait for acknowledgement at the specified tier.
   *
   * @param table Table name
   * @param values Array of column values
   * @param tier Acknowledgement tier to wait for
   * @returns Promise resolving to the new row's ID when the tier acknowledges
   */
  createWithAck(table: string, values: Value[], tier: PersistenceTier): Promise<string>;
  /**
   * Execute a query and return all matching rows.
   *
   * @param query Query builder or JSON-encoded query specification
   * @param settledTier Optional tier to hold delivery until confirmed
   * @returns Array of matching rows
   */
  query(query: string | QueryInput, settledTier?: PersistenceTier): Promise<Row[]>;
  /**
   * Internal query with optional session and settled tier.
   * @internal
   */
  queryInternal(
    queryJson: string,
    session?: Session,
    settledTier?: PersistenceTier,
  ): Promise<Row[]>;
  /**
   * Update a row by ID (sync, fire-and-forget).
   *
   * @param objectId Row ID (UUID string)
   * @param updates Object mapping column names to new values
   */
  update(objectId: string, updates: Record<string, Value>): void;
  /**
   * Update a row and wait for acknowledgement at the specified tier.
   */
  updateWithAck(
    objectId: string,
    updates: Record<string, Value>,
    tier: PersistenceTier,
  ): Promise<void>;
  /**
   * Delete a row by ID (sync, fire-and-forget).
   *
   * @param objectId Row ID (UUID string)
   */
  delete(objectId: string): void;
  /**
   * Delete a row and wait for acknowledgement at the specified tier.
   */
  deleteWithAck(objectId: string, tier: PersistenceTier): Promise<void>;
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
    settledTier?: PersistenceTier,
  ): number;
  /**
   * Internal subscribe with optional session and settled tier.
   * @internal
   */
  subscribeInternal(
    query: string | QueryInput,
    callback: SubscriptionCallback,
    session?: Session,
    settledTier?: PersistenceTier,
  ): number;
  /**
   * Unsubscribe from a query.
   *
   * @param subscriptionId ID returned from subscribe()
   */
  unsubscribe(subscriptionId: number): void;
  /**
   * Get the current schema.
   */
  getSchema(): WasmSchema;
  /**
   * Get the underlying runtime (for WorkerBridge).
   * @internal
   */
  getRuntime(): Runtime;
  /**
   * Get the server URL (for SessionClient).
   * @internal
   */
  getServerUrl(): string | undefined;
  /**
   * Build a fully-qualified endpoint URL against the configured server.
   * @internal
   */
  getRequestUrl(path: string): string;
  /**
   * Get schema context for server requests.
   * @internal
   */
  getSchemaContext(): {
    env: string;
    schema_hash: string;
    user_branch: string;
  };
  /**
   * Send an HTTP request with appropriate auth headers.
   * @internal
   */
  sendRequest(url: string, method: string, body: unknown, session?: Session): Promise<Response>;
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
  linkExternalIdentity(options?: LinkExternalIdentityOptions): Promise<LinkExternalIdentityResult>;
  /**
   * Shutdown the client and release resources.
   */
  shutdown(): Promise<void>;
  private setupSync;
  private sendSyncMessage;
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
export declare function loadWasmModule(): Promise<WasmModule>;
//# sourceMappingURL=client.d.ts.map
