import { deleteDatabaseSync, openDatabaseSync } from "expo-sqlite";
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

  protected open(): void {
    try {
      const db = openDatabaseSync(this.dbName, {
        useNewConnection: true,
      });
      db.execSync("PRAGMA journal_mode = WAL");
      this.db = db;
    } catch (e) {
      console.error("[ExpoSQLiteAdapter] open failed:", e);
      throw new Error(
        `Failed to open ExpoSQLiteAdapter: ${e instanceof Error ? e.message : String(e)}`,
      );
    }
  }

  protected close(): void {
    if (this.db) {
      this.db.closeSync();
      this.db = null;
      this.isInitialized = false;
      this.initialization = undefined;
    }
  }

  protected delete(): void {
    if (this.db) {
      this.close();
    }
    deleteDatabaseSync(this.dbName);
  }

  protected rawDbExecute(sql: string, params?: unknown[]): SQLResult {
    if (!this.db) {
      throw new Error("Database not opened");
    }
    // Use prepared statements to get rows for PRAGMA and SELECT
    const statement = this.db.prepareSync(sql);
    try {
      const result = statement.executeSync(
        params?.map((p) => p as SQLiteBindValue) ?? [],
      );
      const rows = result.getAllSync();
      return {
        rows: rows as SQLRow[],
        insertId: result.lastInsertRowId,
        rowsAffected: result.changes,
      };
    } finally {
      statement.finalizeSync();
    }
  }
}
