import type { SQLiteDatabaseDriverAsync } from "cojson";
import sqlite3InitModule, {
  type Database,
  type SqlValue,
} from "@sqlite.org/sqlite-wasm";

/**
 * SQLite Wasm storage driver for Jazz.
 *
 * Implements `SQLiteDatabaseDriverAsync` using the OO1 API from
 * `@sqlite.org/sqlite-wasm`. The module is initialized via the default
 * export `sqlite3InitModule`, then a `Database` (in-memory) or
 * `OpfsSAHPoolDatabase` (OPFS-backed) is created.
 *
 * When `useOPFS` is `true` (default), the driver attempts to install
 * the OPFS SAH pool VFS and open a persistent database. If OPFS is
 * unavailable (e.g. missing COOP/COEP headers), it falls back to an
 * in-memory database.
 *
 * All `Database.exec()` calls are synchronous — the async interface
 * required by `SQLiteDatabaseDriverAsync` simply wraps them in promises.
 *
 * @example
 * ```typescript
 * const driver = new SqliteWasmDriver("my-app.sqlite3");
 * await driver.initialize();
 * ```
 */
export class SqliteWasmDriver implements SQLiteDatabaseDriverAsync {
  private db!: Database;
  private initialized = false;
  private readonly filename: string;
  private readonly useOPFS: boolean;

  /**
   * @param filename - Database file name for OPFS storage (default: `"jazz-cojson.sqlite3"`)
   * @param useOPFS - Whether to attempt OPFS persistence (default: `true`)
   */
  constructor(filename = "jazz-cojson.sqlite3", useOPFS = true) {
    this.filename = filename;
    this.useOPFS = useOPFS;
  }

  /**
   * Initialize the SQLite Wasm module and open the database.
   * Must be called before any other method. Subsequent calls are no-ops.
   */
  async initialize(): Promise<void> {
    if (this.initialized) return;

    const sqlite3 = await sqlite3InitModule();

    if (this.useOPFS) {
      try {
        const poolUtil = await sqlite3.installOpfsSAHPoolVfs({});
        this.db = new poolUtil.OpfsSAHPoolDb(this.filename);
        this.initialized = true;
        return;
      } catch {
        console.warn(
          "OPFS SAH pool not available, falling back to in-memory storage",
        );
      }
    }

    this.db = new sqlite3.oo1.DB(":memory:");
    this.initialized = true;
  }

  /**
   * Execute a SQL statement that does not return rows.
   *
   * @param sql - The SQL statement to execute
   * @param params - Bind parameters
   */
  async run(sql: string, params: unknown[]): Promise<void> {
    this.db.exec(sql, {
      bind: params as SqlValue[],
    });
  }

  /**
   * Execute a SQL query and return all matching rows.
   *
   * @param sql - The SQL query to execute
   * @param params - Bind parameters
   * @returns Array of row objects
   */
  async query<T>(sql: string, params: unknown[]): Promise<T[]> {
    return this.db.exec(sql, {
      bind: params as SqlValue[],
      returnValue: "resultRows",
      rowMode: "object",
    }) as T[];
  }

  /**
   * Execute a SQL query and return the first matching row, or `undefined`.
   *
   * @param sql - The SQL query to execute
   * @param params - Bind parameters
   * @returns The first row, or `undefined` if no rows match
   */
  async get<T>(sql: string, params: unknown[]): Promise<T | undefined> {
    const rows = await this.query<T>(sql, params);
    return rows[0];
  }

  /**
   * Execute a callback inside a database transaction.
   * Automatically commits on success or rolls back on error.
   *
   * @param callback - Function to execute within the transaction
   */
  async transaction(
    callback: (tx: SQLiteDatabaseDriverAsync) => unknown,
  ): Promise<unknown> {
    this.db.exec("BEGIN TRANSACTION");
    try {
      const result = await callback(this);
      this.db.exec("COMMIT");
      return result;
    } catch (error) {
      this.db.exec("ROLLBACK");
      throw error;
    }
  }

  /**
   * Close the database connection.
   */
  async closeDb(): Promise<void> {
    this.db.close();
  }

  /**
   * Read the current schema migration version from `PRAGMA user_version`.
   *
   * @returns The current migration version number
   */
  async getMigrationVersion(): Promise<number> {
    const rows = this.db.exec("PRAGMA user_version", {
      returnValue: "resultRows",
      rowMode: "object",
    }) as Array<Record<string, SqlValue>>;
    const row = rows[0];
    return typeof row?.["user_version"] === "number" ? row["user_version"] : 0;
  }

  /**
   * Persist the schema migration version via `PRAGMA user_version`.
   *
   * @param version - The migration version to set
   */
  async saveMigrationVersion(version: number): Promise<void> {
    this.db.exec(`PRAGMA user_version = ${version}`);
  }
}
