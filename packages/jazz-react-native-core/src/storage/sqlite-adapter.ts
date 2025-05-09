import { getMigrationQueries } from "./sqlite-migrations.js";

export type SQLRow = Record<string, unknown>;
export type SQLResult = {
  rows: SQLRow[];
  insertId?: number;
  rowsAffected: number;
};

export type Mode = "sync" | "async";

export interface SQLiteAdapter {
  /**
   * Initialize the adapter
   */
  initialize(): Promise<void>;

  /**
   * Asynchronously execute an SQL statement
   * @param sql The SQL query text
   * @param params The parameters to bind to the SQL statement
   * @returns A promise with rows and optional insertId
   */
  executeAsync(sql: string, params?: unknown[]): Promise<SQLResult>;

  /**
   * Synchronously execute an SQL statement, if supported
   * @param sql The SQL query text
   * @param params The parameters to bind to the SQL statement
   * @returns The resulting rows
   */
  executeSync(sql: string, params?: unknown[]): { rows: SQLRow[] };

  /**
   * Asynchronously run a set of operations inside a transaction
   * @param callback A callback that returns a void promise
   */
  transactionAsync(callback: () => Promise<void>): Promise<void>;

  /**
   * Synchronously run a set of operations inside a transaction
   * @param callback A callback that returns a void
   */
  transactionSync(callback: () => void): void;
}

export abstract class SQLiteAdapterBase implements SQLiteAdapter {
  protected dbName: string;
  protected initializationPromise: Promise<void> | null = null;
  protected isInitialized = false;

  constructor(dbName: string) {
    this.dbName = dbName;
  }

  public async initialize(): Promise<void> {
    await this.ensureInitialized();
  }

  protected async ensureInitialized() {
    if (this.isInitialized) return;

    if (!this.initializationPromise) {
      this.initializationPromise = (async () => {
        try {
          await this.initializeInternal();
          this.isInitialized = true;
        } catch (error) {
          this.initializationPromise = null;
          throw error;
        }
      })();
    }

    await this.initializationPromise;
  }

  /**
   * Raw database execution, used before isInitialized becomes true
   */
  protected abstract rawDbExecuteAsync(
    sql: string,
    params?: unknown[],
  ): Promise<SQLResult>;

  private async initializeInternal() {
    try {
      await this.open();

      // Apply pending migrations
      const { rows } = await this.rawDbExecuteAsync("PRAGMA user_version");
      const currentVersion = Number(rows[0]?.user_version) ?? 0;
      const migrationQueries = getMigrationQueries(currentVersion);
      for (const sql of migrationQueries) {
        await this.rawDbExecuteAsync(sql);
      }
    } catch (error) {
      console.error("[SQLiteAdapterBase] ❌ initialization failed:", error);
      throw error;
    }
  }

  public abstract executeAsync(
    sql: string,
    params?: unknown[],
  ): Promise<SQLResult>;
  public abstract executeSync(
    sql: string,
    params?: unknown[],
  ): { rows: SQLRow[] };

  public abstract transactionAsync(
    callback: () => Promise<void>,
  ): Promise<void>;

  /**
   * Synchronously run a set of operations inside a transaction
   */
  public transactionSync(callback: () => void): void {
    if (!this.isInitialized) {
      throw new Error("Database not initialized. Call initialize() first.");
    }
    this.executeSync("BEGIN TRANSACTION");
    try {
      callback();
      this.executeSync("COMMIT");
    } catch (error) {
      this.executeSync("ROLLBACK");
      throw error;
    }
  }

  protected abstract open(): Promise<void>;
  protected abstract close(): Promise<void>;
  protected abstract delete(): Promise<void>;
}
