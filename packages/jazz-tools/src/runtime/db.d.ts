/**
 * High-level database class for typed queries and mutations.
 *
 * Connects QueryBuilder to JazzClient for actual query execution.
 * Handles query translation, execution, and result transformation.
 *
 * Key design: Mutations are SYNC after WASM pre-loading.
 * - createDb() is async (pre-loads WASM module)
 * - insert/update/deleteFrom are sync (operate on in-memory WASM runtime)
 * - all/one are async (need storage I/O for queries)
 */
import type { WasmSchema, StorageDriver } from "../drivers/types.js";
import type { Session } from "./context.js";
import {
  JazzClient,
  type WasmModule,
  type PersistenceTier,
  type QueryExecutionOptions,
  type QueryPropagation,
} from "./client.js";
import { type SubscriptionDelta } from "./subscription-manager.js";
/**
 * Configuration for creating a Db instance.
 */
export interface DbConfig {
  /** Application identifier (used for isolation) */
  appId: string;
  /** Storage driver implementation (optional — storage is in-memory by default) */
  driver?: StorageDriver;
  /** Optional server URL for sync */
  serverUrl?: string;
  /** Optional route prefix for multi-tenant servers (e.g. `/apps/<appId>`). */
  serverPathPrefix?: string;
  /** Environment (e.g., "dev", "prod") */
  env?: string;
  /** User branch name (default: "main") */
  userBranch?: string;
  /** JWT token for server authentication */
  jwtToken?: string;
  /**
   * Local auth mode for client-generated identities.
   *
   * Browser clients default to `"anonymous"` when no other auth is configured.
   */
  localAuthMode?: "anonymous" | "demo";
  /**
   * Client-generated auth token for anonymous/demo identity.
   *
   * If omitted while local auth is active in browser, Jazz generates and
   * persists a per-app device token in localStorage.
   */
  localAuthToken?: string;
  /** Admin secret for catalogue sync */
  adminSecret?: string;
  /** Database name for OPFS persistence (browser only, default: appId) */
  dbName?: string;
}
/**
 * Interface that QueryBuilder classes implement.
 * Generated builders expose these internal properties for Db to use.
 */
export interface QueryBuilder<T> {
  /** Table name for this query */
  readonly _table: string;
  /** Schema reference for translation and transformation */
  readonly _schema: WasmSchema;
  /** Build and return the query as JSON */
  _build(): string;
  /** @internal Phantom brand — enables TypeScript to infer T from usage */
  readonly _rowType: T;
}
export interface QueryOptions extends QueryExecutionOptions {
  propagation?: QueryPropagation;
}
/**
 * Interface for table proxies used with mutations.
 * Generated table constants implement this interface.
 *
 * @typeParam T - The row type (e.g., `{ id: string; title: string; done: boolean }`)
 * @typeParam Init - The init type for inserts (e.g., `{ title: string; done: boolean }`)
 */
export interface TableProxy<T, Init> {
  /** Table name */
  readonly _table: string;
  /** Schema reference */
  readonly _schema: WasmSchema;
  /** @internal Phantom brand — enables TypeScript to infer T from usage */
  readonly _rowType: T;
  /** @internal Phantom brand — enables TypeScript to infer Init from usage */
  readonly _initType: Init;
}
/**
 * High-level database interface for typed queries and mutations.
 *
 * Usage:
 * ```typescript
 * const db = await createDb({ appId: "my-app", driver });
 *
 * // Sync mutations (after WASM is pre-loaded)
 * const id = db.insert(app.todos, { title: "Buy milk", done: false });
 * db.update(app.todos, id, { done: true });
 * db.deleteFrom(app.todos, id);
 *
 * // Async queries (need storage I/O)
 * const todos = await db.all(app.todos.where({ done: false }));
 * const todo = await db.one(app.todos.where({ id }));
 *
 * // Subscriptions
 * const unsubscribe = db.subscribeAll(app.todos, (delta) => {
 *   console.log("All todos:", delta.all);
 *   console.log("Changes:", delta.delta);
 * });
 * ```
 */
export declare class Db {
  private clients;
  private config;
  private wasmModule;
  private workerBridge;
  private worker;
  private bridgeReady;
  private primaryDbName;
  private workerDbName;
  private leaderElection;
  private leaderElectionUnsubscribe;
  private tabRole;
  private tabId;
  private currentLeaderTabId;
  private currentLeaderTerm;
  private syncChannel;
  private readonly leaderPeerIds;
  private activeRemoteLeaderTabId;
  private workerReconfigure;
  private isShuttingDown;
  private lifecycleHooksAttached;
  private readonly onSyncChannelMessage;
  private readonly onVisibilityChange;
  private readonly onPageHide;
  private readonly onPageFreeze;
  private readonly onPageResume;
  /**
   * Protected constructor - use createDb() in regular app code.
   */
  protected constructor(config: DbConfig, wasmModule: WasmModule | null);
  /**
   * Create a Db instance with pre-loaded WASM module.
   * @internal Use createDb() instead.
   */
  static create(config: DbConfig): Promise<Db>;
  /**
   * Create a Db instance backed by a dedicated worker with OPFS persistence.
   *
   * The main thread runs an in-memory WASM runtime.
   * The worker runs a persistent WASM runtime (OPFS).
   * WorkerBridge wires them together via postMessage.
   *
   * @internal Use createDb() instead — it auto-detects browser.
   */
  static createWithWorker(config: DbConfig): Promise<Db>;
  /**
   * Get or create a JazzClient for the given schema.
   * Synchronous because WASM module is pre-loaded.
   *
   * In worker mode, the first call per schema also initializes the
   * WorkerBridge (async). Subsequent calls are sync.
   */
  protected getClient(schema: WasmSchema): JazzClient;
  /**
   * Wait for the worker bridge to be initialized (if in worker mode).
   * No-op if not using a worker.
   */
  private ensureBridgeReady;
  private attachWorkerBridge;
  private buildWorkerBridgeOptions;
  private adoptLeaderSnapshot;
  private openSyncChannel;
  private closeSyncChannel;
  private postSyncChannelMessage;
  private attachLifecycleHooks;
  private detachLifecycleHooks;
  private sendLifecycleHint;
  private logLeaderDebug;
  private handleSyncChannelMessage;
  private handleFollowerSync;
  private handleLeaderSync;
  private handleFollowerClose;
  private handleWorkerPeerSync;
  private sendFollowerClose;
  private applyBridgeRoutingForCurrentLeader;
  private onLeaderElectionChange;
  private enqueueWorkerReconfigure;
  private restartWorkerWithCurrentDbName;
  private currentWorkerNamespace;
  private shutdownWorkerAndClientsForStorageReset;
  private removeOpfsNamespaceFile;
  private static resolveWorkerDbNameForSnapshot;
  private static spawnWorker;
  /**
   * Insert a new row into a table.
   *
   * This is a **synchronous** operation - the row is created immediately
   * in the local WASM runtime. Sync to server happens asynchronously.
   *
   * @param table Table proxy from generated app module
   * @param data Init object with column values
   * @returns The new row's ID (UUID string)
   *
   * @example
   * ```typescript
   * const id = db.insert(app.todos, { title: "Buy milk", done: false });
   * ```
   */
  insert<T, Init>(table: TableProxy<T, Init>, data: Init): string;
  /**
   * Insert a new row and wait for acknowledgement at the specified tier.
   *
   * @param table Table proxy from generated app module
   * @param data Init object with column values
   * @param tier Acknowledgement tier to wait for
   * @returns Promise resolving to the new row's ID when the tier acknowledges
   *
   * @example
   * ```typescript
   * const id = await db.insertWithAck(app.todos, { title: "Buy milk", done: false }, "edge");
   * ```
   */
  insertWithAck<T, Init>(
    table: TableProxy<T, Init>,
    data: Init,
    tier: PersistenceTier,
  ): Promise<string>;
  /**
   * @deprecated Use insertWithAck().
   */
  insertPersisted<T, Init>(
    table: TableProxy<T, Init>,
    data: Init,
    tier: PersistenceTier,
  ): Promise<string>;
  /**
   * Update an existing row.
   *
   * This is a **synchronous** operation - the row is updated immediately
   * in the local WASM runtime. Sync to server happens asynchronously.
   *
   * @param table Table proxy from generated app module
   * @param id Row ID to update
   * @param data Partial object with fields to update
   *
   * @example
   * ```typescript
   * db.update(app.todos, id, { done: true });
   * ```
   */
  update<T, Init>(table: TableProxy<T, Init>, id: string, data: Partial<Init>): void;
  /**
   * Update an existing row and wait for acknowledgement at the specified tier.
   *
   * @param table Table proxy from generated app module
   * @param id Row ID to update
   * @param data Partial object with fields to update
   * @param tier Acknowledgement tier to wait for
   */
  updateWithAck<T, Init>(
    table: TableProxy<T, Init>,
    id: string,
    data: Partial<Init>,
    tier: PersistenceTier,
  ): Promise<void>;
  /**
   * @deprecated Use updateWithAck().
   */
  updatePersisted<T, Init>(
    table: TableProxy<T, Init>,
    id: string,
    data: Partial<Init>,
    tier: PersistenceTier,
  ): Promise<void>;
  /**
   * Delete a row.
   *
   * This is a **synchronous** operation - the row is deleted immediately
   * in the local WASM runtime. Sync to server happens asynchronously.
   *
   * @param table Table proxy from generated app module
   * @param id Row ID to delete
   *
   * @example
   * ```typescript
   * db.deleteFrom(app.todos, id);
   * ```
   */
  deleteFrom<T, Init>(table: TableProxy<T, Init>, id: string): void;
  /**
   * Delete a row and wait for acknowledgement at the specified tier.
   *
   * @param table Table proxy from generated app module
   * @param id Row ID to delete
   * @param tier Acknowledgement tier to wait for
   */
  deleteFromWithAck<T, Init>(
    table: TableProxy<T, Init>,
    id: string,
    tier: PersistenceTier,
  ): Promise<void>;
  /**
   * @deprecated Use deleteFromWithAck().
   */
  deleteFromPersisted<T, Init>(
    table: TableProxy<T, Init>,
    id: string,
    tier: PersistenceTier,
  ): Promise<void>;
  /**
   * Delete browser OPFS storage for this Db's active namespace and reopen a clean worker.
   *
   * This only deletes `${namespace}.opfsbtree` for the current namespace and does not touch
   * localStorage-based auth or synthetic-user state.
   *
   * Behavior:
   * - Browser worker-backed Db only (throws in non-browser/non-worker runtimes)
   * - Leader tab only (throws on follower tabs and asks to close other tabs)
   * - Serializes with worker reconfigure operations
   * - Tears down worker + clients, deletes OPFS file, respawns worker
   * - If file deletion fails, still respawns worker and then rethrows the deletion error
   */
  deleteClientStorage(): Promise<void>;
  /**
   * Execute a query and return all matching rows as typed objects.
   *
   * @param query QueryBuilder instance (e.g., app.todos.where({done: false}))
   * @returns Array of typed objects matching the query
   */
  all<T>(query: QueryBuilder<T>, options?: QueryOptions): Promise<T[]>;
  /**
   * Execute a query and return the first matching row, or null.
   *
   * @param query QueryBuilder instance
   * @param settledTier Optional tier to hold delivery until confirmed
   * @returns First matching typed object, or null if none found
   */
  one<T>(query: QueryBuilder<T>, options?: QueryOptions): Promise<T | null>;
  /**
   * Subscribe to a query and receive updates when results change.
   *
   * The callback receives a SubscriptionDelta with:
   * - `all`: Complete current result set
   * - `delta`: Ordered list of row-level changes
   *
   * @param query QueryBuilder instance
   * @param callback Called with delta whenever results change
   * @returns Unsubscribe function
   *
   * @example
   * ```typescript
   * const unsubscribe = db.subscribeAll(app.todos, (delta) => {
   *   setTodos(delta.all);
   *   for (const change of delta.delta) {
   *     if (change.kind === 0) {
   *       console.log("New row:", change.row);
   *     }
   *   }
   * });
   *
   * // Later: stop receiving updates
   * unsubscribe();
   * ```
   */
  subscribeAll<
    T extends {
      id: string;
    },
  >(
    query: QueryBuilder<T>,
    callback: (delta: SubscriptionDelta<T>) => void,
    options?: QueryOptions,
    session?: Session,
  ): () => void;
  /**
   * Shutdown the Db and release all resources.
   * Closes all memoized JazzClient connections and the worker.
   */
  shutdown(): Promise<void>;
}
/**
 * Create a new Db instance with the given configuration.
 *
 * This is an **async** factory function that pre-loads the WASM module.
 * After creation, mutations (insert/update/deleteFrom) are synchronous.
 *
 * In browser environments, automatically uses a dedicated worker for
 * OPFS persistence. In Node.js, uses in-memory storage.
 *
 * @param config Database configuration
 * @returns Promise resolving to Db instance ready for queries and mutations
 *
 * @example
 * ```typescript
 * const db = await createDb({
 *   appId: "my-app",
 *   schema: mySchema,
 * });
 * ```
 */
export declare function createDb(config: DbConfig): Promise<Db>;
//# sourceMappingURL=db.d.ts.map
