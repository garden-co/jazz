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

import type { WasmSchema, WasmRow, StorageDriver } from "../drivers/types.js";
import { JazzClient, loadWasmModule, type WasmModule, type PersistenceTier } from "./client.js";
import { WorkerBridge } from "./worker-bridge.js";
import { translateQuery } from "./query-adapter.js";
import { transformRows } from "./row-transformer.js";
import { toValueArray, toUpdateRecord } from "./value-converter.js";
import { SubscriptionManager, type SubscriptionDelta } from "./subscription-manager.js";

/**
 * Configuration for creating a Db instance.
 */
export interface DbConfig {
  /** Application identifier (used for isolation) */
  appId: string;
  /** Optional sync client ID (UUID) */
  clientId?: string;
  /** Storage driver implementation (optional — storage is in-memory by default) */
  driver?: StorageDriver;
  /** Optional server URL for sync */
  serverUrl?: string;
  /** Environment (e.g., "dev", "prod") */
  env?: string;
  /** User branch name (default: "main") */
  userBranch?: string;
  /** JWT token for server authentication */
  jwtToken?: string;
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
 *   console.log("Added:", delta.added);
 * });
 * ```
 */
export class Db {
  private clients = new Map<string, JazzClient>();
  private config: DbConfig;
  private wasmModule: WasmModule;
  private workerBridge: WorkerBridge | null = null;
  private worker: Worker | null = null;
  private bridgeReady: Promise<void> | null = null;

  /**
   * Private constructor - use createDb() factory function.
   */
  private constructor(config: DbConfig, wasmModule: WasmModule) {
    this.config = config;
    this.wasmModule = wasmModule;
  }

  /**
   * Create a Db instance with pre-loaded WASM module.
   * @internal Use createDb() instead.
   */
  static async create(config: DbConfig): Promise<Db> {
    const wasmModule = await loadWasmModule();
    return new Db(config, wasmModule);
  }

  /**
   * Create a Db instance backed by a dedicated worker with OPFS persistence.
   *
   * The main thread runs an in-memory WASM runtime.
   * The worker runs a persistent WASM runtime (OPFS).
   * WorkerBridge wires them together via postMessage.
   *
   * @internal Use createDb() instead — it auto-detects browser.
   */
  static async createWithWorker(config: DbConfig): Promise<Db> {
    const wasmModule = await loadWasmModule();
    const db = new Db(config, wasmModule);

    // Spawn dedicated worker
    const worker = new Worker(new URL("../worker/groove-worker.ts", import.meta.url), {
      type: "module",
    });
    db.worker = worker;

    // Wait for worker to load WASM
    await new Promise<void>((resolve, reject) => {
      const timeout = setTimeout(() => reject(new Error("Worker WASM load timeout")), 15000);
      const handler = (event: MessageEvent) => {
        if (event.data.type === "ready") {
          clearTimeout(timeout);
          worker.removeEventListener("message", handler);
          resolve();
        } else if (event.data.type === "error") {
          clearTimeout(timeout);
          worker.removeEventListener("message", handler);
          reject(new Error(event.data.message));
        }
      };
      worker.addEventListener("message", handler);
      worker.addEventListener("error", (e) => {
        clearTimeout(timeout);
        reject(new Error(`Worker load error: ${e.message}`));
      });
    });

    return db;
  }

  /**
   * Get or create a JazzClient for the given schema.
   * Synchronous because WASM module is pre-loaded.
   *
   * In worker mode, the first call per schema also initializes the
   * WorkerBridge (async). Subsequent calls are sync.
   */
  private getClient(schema: WasmSchema): JazzClient {
    // Use stringified schema as cache key
    const key = JSON.stringify(schema);

    if (!this.clients.has(key)) {
      // Create in-memory runtime (works for both direct and worker mode)
      const client = JazzClient.connectSync(this.wasmModule, {
        appId: this.config.appId,
        clientId: this.config.clientId,
        schema,
        driver: this.config.driver,
        // In worker mode, don't connect to server directly — worker handles it
        serverUrl: this.worker ? undefined : this.config.serverUrl,
        env: this.config.env,
        userBranch: this.config.userBranch,
        jwtToken: this.config.jwtToken,
        adminSecret: this.config.adminSecret,
      });

      // In worker mode, set up the bridge for this client
      if (this.worker && !this.workerBridge) {
        const bridge = new WorkerBridge(this.worker, client.getRuntime());
        this.workerBridge = bridge;

        // Initialize worker — store promise so async methods can await it
        this.bridgeReady = bridge
          .init({
            schemaJson: JSON.stringify(schema),
            appId: this.config.appId,
            clientId: this.config.clientId,
            env: this.config.env ?? "dev",
            userBranch: this.config.userBranch ?? "main",
            dbName: this.config.dbName ?? this.config.appId,
            serverUrl: this.config.serverUrl,
            jwtToken: this.config.jwtToken,
            adminSecret: this.config.adminSecret,
          })
          .then(() => {})
          .catch((e) => console.error("Worker bridge init error:", e));
      }

      this.clients.set(key, client);
    }

    return this.clients.get(key)!;
  }

  /**
   * Wait for the worker bridge to be initialized (if in worker mode).
   * No-op if not using a worker.
   */
  private async ensureBridgeReady(): Promise<void> {
    if (this.bridgeReady) {
      await this.bridgeReady;
    }
  }

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
  insert<T, Init>(table: TableProxy<T, Init>, data: Init): string {
    const client = this.getClient(table._schema);
    const values = toValueArray(data as Record<string, unknown>, table._schema, table._table);
    return client.create(table._table, values);
  }

  /**
   * Insert a new row and wait for persistence at the specified tier.
   *
   * @param table Table proxy from generated app module
   * @param data Init object with column values
   * @param tier Persistence tier to wait for
   * @returns Promise resolving to the new row's ID when the tier acks
   *
   * @example
   * ```typescript
   * const id = await db.insertPersisted(app.todos, { title: "Buy milk", done: false }, "edge");
   * ```
   */
  async insertPersisted<T, Init>(
    table: TableProxy<T, Init>,
    data: Init,
    tier: PersistenceTier,
  ): Promise<string> {
    const client = this.getClient(table._schema);
    await this.ensureBridgeReady();
    const values = toValueArray(data as Record<string, unknown>, table._schema, table._table);
    return client.createPersisted(table._table, values, tier);
  }

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
  update<T, Init>(table: TableProxy<T, Init>, id: string, data: Partial<Init>): void {
    const client = this.getClient(table._schema);
    const updates = toUpdateRecord(data as Record<string, unknown>, table._schema, table._table);
    client.update(id, updates);
  }

  /**
   * Update an existing row and wait for persistence at the specified tier.
   *
   * @param table Table proxy from generated app module
   * @param id Row ID to update
   * @param data Partial object with fields to update
   * @param tier Persistence tier to wait for
   */
  async updatePersisted<T, Init>(
    table: TableProxy<T, Init>,
    id: string,
    data: Partial<Init>,
    tier: PersistenceTier,
  ): Promise<void> {
    const client = this.getClient(table._schema);
    await this.ensureBridgeReady();
    const updates = toUpdateRecord(data as Record<string, unknown>, table._schema, table._table);
    await client.updatePersisted(id, updates, tier);
  }

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
  deleteFrom<T, Init>(table: TableProxy<T, Init>, id: string): void {
    const client = this.getClient(table._schema);
    client.delete(id);
  }

  /**
   * Delete a row and wait for persistence at the specified tier.
   *
   * @param table Table proxy from generated app module
   * @param id Row ID to delete
   * @param tier Persistence tier to wait for
   */
  async deleteFromPersisted<T, Init>(
    table: TableProxy<T, Init>,
    id: string,
    tier: PersistenceTier,
  ): Promise<void> {
    const client = this.getClient(table._schema);
    await this.ensureBridgeReady();
    await client.deletePersisted(id, tier);
  }

  /**
   * Execute a query and return all matching rows as typed objects.
   *
   * @param query QueryBuilder instance (e.g., app.todos.where({done: false}))
   * @returns Array of typed objects matching the query
   */
  async all<T>(query: QueryBuilder<T>, settledTier?: PersistenceTier): Promise<T[]> {
    const client = this.getClient(query._schema);
    const wasmQuery = translateQuery(query._build(), query._schema);
    const rows = await client.query(wasmQuery, settledTier);
    return transformRows<T>(rows, query._schema, query._table);
  }

  /**
   * Execute a query and return the first matching row, or null.
   *
   * @param query QueryBuilder instance
   * @param settledTier Optional tier to hold delivery until confirmed
   * @returns First matching typed object, or null if none found
   */
  async one<T>(query: QueryBuilder<T>, settledTier?: PersistenceTier): Promise<T | null> {
    const results = await this.all(query, settledTier);
    return results[0] ?? null;
  }

  /**
   * Subscribe to a query and receive updates when results change.
   *
   * The callback receives a SubscriptionDelta with:
   * - `all`: Complete current result set
   * - `added`: Items added in this update
   * - `updated`: Items modified in this update
   * - `removed`: Items removed in this update
   *
   * @param query QueryBuilder instance
   * @param callback Called with delta whenever results change
   * @returns Unsubscribe function
   *
   * @example
   * ```typescript
   * const unsubscribe = db.subscribeAll(app.todos, (delta) => {
   *   setTodos(delta.all);
   *   if (delta.added.length > 0) {
   *     console.log("New todos:", delta.added);
   *   }
   * });
   *
   * // Later: stop receiving updates
   * unsubscribe();
   * ```
   */
  subscribeAll<T extends { id: string }>(
    query: QueryBuilder<T>,
    callback: (delta: SubscriptionDelta<T>) => void,
    settledTier?: PersistenceTier,
  ): () => void {
    const manager = new SubscriptionManager<T>();
    const client = this.getClient(query._schema);
    const wasmQuery = translateQuery(query._build(), query._schema);

    const transform = (row: WasmRow): T => {
      return transformRows<T>([row], query._schema, query._table)[0];
    };

    const subId = client.subscribe(
      wasmQuery,
      (delta) => {
        const typedDelta = manager.handleDelta(delta, transform);
        callback(typedDelta);
      },
      settledTier,
    );

    // Return unsubscribe function
    return () => {
      client.unsubscribe(subId);
      manager.clear();
    };
  }

  /**
   * Shutdown the Db and release all resources.
   * Closes all memoized JazzClient connections and the worker.
   */
  async shutdown(): Promise<void> {
    // Ensure bridge init has completed before sending shutdown —
    // otherwise the worker may still be opening OPFS handles
    await this.ensureBridgeReady();

    // Shutdown worker bridge — waits for OPFS handles to be released
    if (this.workerBridge && this.worker) {
      await this.workerBridge.shutdown(this.worker);
      this.workerBridge = null;
    }

    for (const client of this.clients.values()) {
      await client.shutdown();
    }
    this.clients.clear();

    if (this.worker) {
      this.worker.terminate();
      this.worker = null;
    }
  }
}

/**
 * Check if running in a browser environment with Worker support.
 */
function isBrowser(): boolean {
  return typeof Worker !== "undefined" && typeof window !== "undefined";
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
export async function createDb(config: DbConfig): Promise<Db> {
  if (isBrowser()) {
    return Db.createWithWorker(config);
  }
  return Db.create(config);
}
