import { deleteDatabaseAsync, openDatabaseAsync } from "expo-sqlite";
import type { SQLiteBindValue, SQLiteDatabase } from "expo-sqlite";
import type { SQLResult, SQLRow, SQLiteAdapter } from "jazz-react-native-core";

const SQLITE_CONSTRAINT = 6;
const SQLITE_SYNTAX_ERR = 1;

export class ExpoSQLiteAdapter implements SQLiteAdapter {
  private db: SQLiteDatabase | null = null;
  private dbName: string;
  private initializationPromise: Promise<void> | null = null;
  private isInitialized = false;

  public constructor(dbName: string = "jazz-storage") {
    this.dbName = dbName;
  }

  private async ensureInitialized() {
    // Return immediately if already initialized
    if (this.isInitialized) {
      return;
    }

    // If initialization is in progress, wait for it
    if (this.initializationPromise) {
      await this.initializationPromise;
      return;
    }

    // Start initialization
    this.initializationPromise = this.initializeInternal();
    try {
      await this.initializationPromise;
      this.isInitialized = true;
    } catch (error) {
      // Clear the promise on failure so future attempts can retry
      this.initializationPromise = null;
      throw error;
    }
  }

  private async initializeInternal() {
    try {
      const db = await openDatabaseAsync(this.dbName, {
        useNewConnection: true
      });

      // Verify database connection before proceeding
      const statement = await db.prepareAsync("SELECT 1");
      try {
        const result = await statement.executeAsync();
        const rows = await result.getAllAsync();
        if (!rows || rows.length === 0) {
          throw new Error("Database connection test failed");
        }
      } finally {
        await statement.finalizeAsync();
      }

      this.db = db;

      // Schema version check and migrations
      const { rows } = await this.executeSql("PRAGMA user_version");
      const oldVersion = Number(rows[0]?.user_version) ?? 0;

      if (oldVersion === 0) {
        await this.executeSql(
          `CREATE TABLE IF NOT EXISTS transactions (
            ses INTEGER,
            idx INTEGER,
            tx TEXT NOT NULL,
            PRIMARY KEY (ses, idx)
          ) WITHOUT ROWID;`,
        );

        await this.executeSql(
          `CREATE TABLE IF NOT EXISTS sessions (
            rowID INTEGER PRIMARY KEY,
            coValue INTEGER NOT NULL,
            sessionID TEXT NOT NULL,
            lastIdx INTEGER,
            lastSignature TEXT,
            UNIQUE (sessionID, coValue)
          );`,
        );

        await this.executeSql(
          `CREATE INDEX IF NOT EXISTS sessionsByCoValue ON sessions (coValue);`,
        );

        await this.executeSql(
          `CREATE TABLE IF NOT EXISTS coValues (
            rowID INTEGER PRIMARY KEY,
            id TEXT NOT NULL UNIQUE,
            header TEXT NOT NULL UNIQUE
          );`,
        );

        await this.executeSql(
          `CREATE INDEX IF NOT EXISTS coValuesByID ON coValues (id);`,
        );

        await this.executeSql("PRAGMA user_version = 1");
      }

      if (oldVersion <= 2) {
        await this.executeSql(
          `CREATE TABLE IF NOT EXISTS signatureAfter (
            ses INTEGER,
            idx INTEGER,
            signature TEXT NOT NULL,
            PRIMARY KEY (ses, idx)
          ) WITHOUT ROWID;`,
        );

        await this.executeSql(
          `ALTER TABLE sessions ADD COLUMN bytesSinceLastSignature INTEGER;`,
        );

        await this.executeSql("PRAGMA user_version = 3");
      }

      console.log("[ExpoSQLiteAdapter] Initialization complete");
    } catch (e) {
      console.error("[ExpoSQLiteAdapter] Initialization failed:", e);
      throw new Error(
        `Failed to initialize ExpoSQLiteAdapter: ${e instanceof Error ? e.message : String(e)}`,
      );
    }
  }

  private async executeSql(
    sql: string,
    params?: unknown[],
  ): Promise<SQLResult> {
    if (!this.db) {
      throw new Error("Database not initialized");
    }

    try {
      const statement = await this.db.prepareAsync(sql);
      try {
        const result = await statement.executeAsync(
          params?.map((p) => p as SQLiteBindValue) ?? [],
        );
        const rows = await result.getAllAsync();
        return {
          rows: rows as SQLRow[],
          insertId: result.lastInsertRowId,
          rowsAffected: result.changes,
        };
      } finally {
        await statement.finalizeAsync();
      }
    } catch (error) {
      console.error(
        `[ExpoSQLiteAdapter] SQL Error: ${error instanceof Error ? error.message : String(error)} in query: ${sql}`,
      );
      if (error instanceof Error) {
        if ((error as any).code === SQLITE_CONSTRAINT) {
          throw new Error(`Constraint violation: ${error.message}`);
        } else if ((error as any).code === SQLITE_SYNTAX_ERR) {
          throw new Error(`SQL syntax error: ${error.message}`);
        }
      }
      throw error;
    }
  }

  public async initialize(): Promise<void> {
    await this.ensureInitialized();
  }

  public async execute(sql: string, params?: unknown[]): Promise<SQLResult> {
    await this.ensureInitialized();
    return this.executeSql(sql, params);
  }

  public executeSync(sql: string, params?: unknown[]): { rows: SQLRow[] } {
    if (!this.db || !this.isInitialized) {
      throw new Error("Database not initialized");
    }

    try {
      const statement = this.db.prepareSync(sql);
      try {
        const result = statement.executeSync(
          params?.map((p) => p as SQLiteBindValue) ?? [],
        );
        const rows = result.getAllSync();
        return { rows: rows as SQLRow[] };
      } finally {
        statement.finalizeSync();
      }
    } catch (error) {
      console.error(
        `[ExpoSQLiteAdapter] SQL Error: ${error instanceof Error ? error.message : String(error)} in query: ${sql}`,
      );
      if (error instanceof Error) {
        if ((error as any).code === SQLITE_CONSTRAINT) {
          throw new Error(`Constraint violation: ${error.message}`);
        } else if ((error as any).code === SQLITE_SYNTAX_ERR) {
          throw new Error(`SQL syntax error: ${error.message}`);
        }
      }
      throw error;
    }
  }

  public async transaction(callback: () => Promise<void>): Promise<void> {
    await this.ensureInitialized();
    if (!this.db) throw new Error("Database not initialized");

    await this.db.withTransactionAsync(async () => {
      await callback();
    });
  }

  public async close(): Promise<void> {
    if (this.db) {
      await this.db.closeAsync();
      this.db = null;
      this.isInitialized = false;
      this.initializationPromise = null;
    }
  }

  public async delete(): Promise<void> {
    if (this.db) {
      const dbName = this.dbName;
      await this.close();
      await deleteDatabaseAsync(dbName);
    }
  }
}
