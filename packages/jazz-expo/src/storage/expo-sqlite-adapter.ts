import { openDatabaseSync } from "expo-sqlite";
import type { SQLiteBindValue, SQLiteDatabase } from "expo-sqlite";
import { type SQLiteDatabaseDriver } from "jazz-react-native-core";

export class ExpoSQLiteAdapter implements SQLiteDatabaseDriver {
  private db: SQLiteDatabase | null = null;
  private dbName: string;

  public constructor(dbName: string = "jazz-storage") {
    this.dbName = dbName;
    const db = openDatabaseSync(this.dbName, {
      useNewConnection: true,
    });
    db.execSync("PRAGMA journal_mode = WAL");
    this.db = db;
  }

  public query<T>(sql: string, params?: unknown[]): T[] {
    if (!this.db) {
      throw new Error("Database not initialized");
    }

    const statement = this.db.prepareSync(sql);
    try {
      const result = statement.executeSync(
        params?.map((p) => p as SQLiteBindValue) ?? [],
      );
      return result.getAllSync() as T[];
    } finally {
      statement.finalizeSync();
    }
  }

  public run(sql: string, params?: unknown[]) {
    if (!this.db) {
      throw new Error("Database not initialized");
    }

    const statement = this.db.prepareSync(sql);
    try {
      statement.executeSync(params?.map((p) => p as SQLiteBindValue) ?? []);
    } finally {
      statement.finalizeSync();
    }
  }

  public transaction(callback: () => unknown) {
    this.run("BEGIN TRANSACTION");

    try {
      callback();
      this.run("COMMIT");
    } catch (error) {
      this.run("ROLLBACK");
      throw error;
    }
  }
}
