import * as opSQLite from "@op-engineering/op-sqlite";
import {
  ANDROID_DATABASE_PATH,
  IOS_LIBRARY_PATH,
} from "@op-engineering/op-sqlite";
import { Platform } from "react-native";

type OPSQLiteDB = ReturnType<typeof opSQLite.open>;

import { type SQLiteDatabaseDriverAsync } from "jazz-react-native-core";

export class OPSQLiteAdapter implements SQLiteDatabaseDriverAsync {
  private db: OPSQLiteDB | null = null;
  private dbName: string;

  public constructor(dbName: string = "jazz-storage") {
    this.dbName = dbName;
  }

  public async initialize(): Promise<void> {
    const dbPath =
      Platform.OS === "ios" ? IOS_LIBRARY_PATH : ANDROID_DATABASE_PATH;

    const db = (this.db = opSQLite.open({
      name: this.dbName,
      location: dbPath,
    }));

    await db.execute("PRAGMA journal_mode=WAL");
  }

  public async query<T>(sql: string, params?: unknown[]): Promise<T[]> {
    if (!this.db) {
      throw new Error("Database not initialized");
    }

    const result = await this.db.execute(sql, params as any[]);

    return result.rows as T[];
  }

  public async get<T>(sql: string, params?: unknown[]): Promise<T | undefined> {
    if (!this.db) {
      throw new Error("Database not initialized");
    }

    const result = await this.db.execute(sql, params as any[]);

    return result.rows[0] as T | undefined;
  }

  public async run(sql: string, params?: unknown[]) {
    if (!this.db) {
      throw new Error("Database not initialized");
    }

    await this.db.executeRaw(sql, params as any[]);
  }

  public async transaction(callback: () => unknown) {
    await this.run("BEGIN TRANSACTION");

    try {
      await callback();
      await this.run("COMMIT");
    } catch (error) {
      await this.run("ROLLBACK");
      throw error;
    }
  }

  public async closeDb(): Promise<void> {
    if (!this.db) {
      throw new Error("Database not initialized");
    }

    this.db.close();
    this.db = null;
  }
}
