/**
 * High-level database class for typed queries.
 *
 * Connects QueryBuilder to JazzClient for actual query execution.
 * Handles query translation, execution, and result transformation.
 */

import type { WasmSchema, StorageDriver } from "../drivers/types.js";
import { JazzClient } from "./client.js";
import { translateQuery } from "./query-adapter.js";
import { transformRows } from "./row-transformer.js";

/**
 * Configuration for creating a Db instance.
 */
export interface DbConfig {
  /** Application identifier (used for isolation) */
  appId: string;
  /** Storage driver implementation */
  driver: StorageDriver;
  /** Optional server URL for sync */
  serverUrl?: string;
  /** Environment (e.g., "dev", "prod") */
  env?: string;
  /** User branch name (default: "main") */
  userBranch?: string;
}

/**
 * Interface that QueryBuilder classes implement.
 * Generated builders expose these internal properties for Db to use.
 */
export interface QueryBuilder<_T> {
  /** Table name for this query */
  readonly _table: string;
  /** Schema reference for translation and transformation */
  readonly _schema: WasmSchema;
  /** Build and return the query as JSON */
  _build(): string;
}

/**
 * High-level database interface for typed queries.
 *
 * Usage:
 * ```typescript
 * const db = createDb({ appId: "my-app", driver });
 * const todos = await db.all(app.todos.where({ done: false }));
 * const todo = await db.one(app.todos.where({ id }));
 * ```
 */
export class Db {
  private clients = new Map<string, JazzClient>();
  private config: DbConfig;

  constructor(config: DbConfig) {
    this.config = config;
  }

  /**
   * Get or create a JazzClient for the given schema.
   * Memoizes clients per schema to reuse connections.
   */
  private async getClient(schema: WasmSchema): Promise<JazzClient> {
    // Use stringified schema as cache key
    const key = JSON.stringify(schema);

    if (!this.clients.has(key)) {
      const client = await JazzClient.connect({
        appId: this.config.appId,
        schema,
        driver: this.config.driver,
        serverUrl: this.config.serverUrl,
        env: this.config.env,
        userBranch: this.config.userBranch,
      });
      this.clients.set(key, client);
    }

    return this.clients.get(key)!;
  }

  /**
   * Execute a query and return all matching rows as typed objects.
   *
   * @param query QueryBuilder instance (e.g., app.todos.where({done: false}))
   * @returns Array of typed objects matching the query
   */
  async all<T>(query: QueryBuilder<T>): Promise<T[]> {
    const client = await this.getClient(query._schema);
    const wasmQuery = translateQuery(query._build(), query._schema);
    const rows = await client.query(wasmQuery);
    return transformRows<T>(rows, query._schema, query._table);
  }

  /**
   * Execute a query and return the first matching row, or null.
   *
   * @param query QueryBuilder instance
   * @returns First matching typed object, or null if none found
   */
  async one<T>(query: QueryBuilder<T>): Promise<T | null> {
    const results = await this.all(query);
    return results[0] ?? null;
  }

  /**
   * Shutdown the Db and release all resources.
   * Closes all memoized JazzClient connections.
   */
  async shutdown(): Promise<void> {
    for (const client of this.clients.values()) {
      await client.shutdown();
    }
    this.clients.clear();
  }
}

/**
 * Create a new Db instance with the given configuration.
 *
 * @param config Database configuration
 * @returns Db instance ready for queries
 */
export function createDb(config: DbConfig): Db {
  return new Db(config);
}
