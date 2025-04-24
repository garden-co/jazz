import { deleteDatabaseAsync, openDatabaseAsync } from "expo-sqlite";
import type { SQLiteBindValue, SQLiteDatabase } from "expo-sqlite";
import type { SQLResult, SQLRow } from "jazz-react-native-core";
import { SQLiteAdapterBase } from "jazz-react-native-core";

const SQLITE_CONSTRAINT = 6;
const SQLITE_SYNTAX_ERR = 1;

export class ExpoSQLiteAdapter extends SQLiteAdapterBase {
  private db: SQLiteDatabase | null = null;

  public constructor(dbName: string = "jazz-storage") {
    super(dbName);
  }

  public async executeAsync(
    sql: string,
    params?: unknown[],
  ): Promise<SQLResult> {
    await this.ensureInitialized();
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

  public executeSync(sql: string, params?: unknown[]): { rows: SQLRow[] } {
    if (!this.isInitialized || !this.db) {
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

  public async transactionAsync(callback: () => Promise<void>): Promise<void> {
    await this.ensureInitialized();
    if (!this.db) {
      throw new Error("Database not initialized");
    }

    await this.db.withTransactionAsync(callback);
  }

  protected async open(): Promise<void> {
    try {
      const db = await openDatabaseAsync(this.dbName);

      // Verify connection
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
    } catch (e) {
      console.error("[ExpoSQLiteAdapter] open failed:", e);
      throw new Error(
        `Failed to open ExpoSQLiteAdapter: ${e instanceof Error ? e.message : String(e)}`,
      );
    }
  }

  protected async close(): Promise<void> {
    if (this.db) {
      await this.db.closeAsync();
      this.db = null;
      this.isInitialized = false;
      this.initializationPromise = null;
    }
  }

  protected async delete(): Promise<void> {
    if (this.db) {
      await this.close();
    }
    await deleteDatabaseAsync(this.dbName);
  }
}
