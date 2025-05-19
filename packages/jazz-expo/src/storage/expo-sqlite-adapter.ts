import { openDatabaseAsync } from "expo-sqlite";
import type { SQLiteBindValue, SQLiteDatabase } from "expo-sqlite";
import { type SQLiteDatabaseDriverAsync } from "jazz-react-native-core";

export class ExpoSQLiteAdapter implements SQLiteDatabaseDriverAsync {
  private db: SQLiteDatabase | null = null;
  private dbName: string;

  public constructor(dbName: string = "jazz-storage") {
    this.dbName = dbName;
  }

  public async initialize(): Promise<void> {
    const db = await openDatabaseAsync(this.dbName, {
      useNewConnection: true,
    });
    await db.execAsync("PRAGMA journal_mode = WAL");
    this.db = db;
  }

  public async query<T>(sql: string, params?: unknown[]): Promise<T[]> {
    if (!this.db) {
      throw new Error("Database not initialized");
    }

    const result = await this.db.getAllAsync(
      sql,
      params?.map((p) => p as SQLiteBindValue) ?? [],
    );

    return result as T[];
  }

  public async get<T>(sql: string, params?: unknown[]): Promise<T | undefined> {
    if (!this.db) {
      throw new Error("Database not initialized");
    }

    const result = await this.db.getFirstAsync(
      sql,
      params?.map((p) => p as SQLiteBindValue) ?? [],
    );

    return (result as T) ?? undefined;
  }

  public async run(sql: string, params?: unknown[]) {
    if (!this.db) {
      throw new Error("Database not initialized");
    }

    await this.db.runAsync(sql, params?.map((p) => p as SQLiteBindValue) ?? []);
  }

  public async transaction(callback: () => unknown) {
    if (!this.db) {
      throw new Error("Database not initialized");
    }

    await this.db.withTransactionAsync(async () => {
      await callback();
    });
  }
}
