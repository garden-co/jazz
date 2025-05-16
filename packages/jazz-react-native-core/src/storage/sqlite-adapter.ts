import { getMigrationQueries } from "./sqlite-migrations.js";

export type SQLRow = Record<string, unknown>;
export type SQLResult = {
  rows: SQLRow[];
  insertId?: number;
  rowsAffected: number;
};

export interface SQLiteAdapter {
  /**
   * Initialize the adapter
   */
  initialize(): void;

  /**
   * Synchronously execute an SQL statement, if supported
   * @param sql The SQL query text
   * @param params The parameters to bind to the SQL statement
   * @returns The resulting rows
   */
  executeSync(sql: string, params?: unknown[]): { rows: SQLRow[] };

  /**
   * Synchronously run a set of operations inside a transaction
   * @param callback A callback that returns a void
   */
  transactionSync(callback: () => void): void;
}

export abstract class SQLiteAdapterBase implements SQLiteAdapter {
  protected dbName: string;
  protected initialization: void | undefined = undefined;
  protected isInitialized = false;

  constructor(dbName: string) {
    this.dbName = dbName;
  }

  public initialize(): void {
    this.ensureInitialized();
  }

  protected ensureInitialized() {
    if (this.isInitialized) return;

    if (!this.initialization) {
      this.initialization = (() => {
        try {
          this.initializeInternal();
          this.isInitialized = true;
        } catch (error) {
          this.initialization = undefined;
          throw error;
        }
      })();
    }

    this.initialization;
  }

  /**
   * Raw database execution, used before isInitialized becomes true
   */
  protected abstract rawDbExecute(sql: string, params?: unknown[]): SQLResult;

  private initializeInternal() {
    try {
      this.open();

      // Apply pending migrations
      const { rows } = this.rawDbExecute("PRAGMA user_version");
      const currentVersion = Number(rows[0]?.user_version) ?? 0;
      const migrationQueries = getMigrationQueries(currentVersion);
      for (const sql of migrationQueries) {
        this.rawDbExecute(sql);
      }
    } catch (error) {
      console.error("[SQLiteAdapterBase] ❌ initialization failed:", error);
      throw error;
    }
  }

  public abstract executeSync(
    sql: string,
    params?: unknown[],
  ): { rows: SQLRow[] };

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

  protected abstract open(): void;
  protected abstract close(): void;
  protected abstract delete(): void;
}
