import * as opSQLite from "@op-engineering/op-sqlite";
import {
  ANDROID_DATABASE_PATH,
  IOS_LIBRARY_PATH,
} from "@op-engineering/op-sqlite";
import { Platform } from "react-native";

type OPSQLiteDB = ReturnType<typeof opSQLite.open>;

import { type SQLiteDatabaseDriverAsync } from "jazz-tools/react-native-core";

export class OPSQLiteAdapter implements SQLiteDatabaseDriverAsync {
  private db: OPSQLiteDB | opSQLite.Transaction | null = null;
  private dbName: string;

  static withDB(db: OPSQLiteDB | opSQLite.Transaction): OPSQLiteAdapter {
    const adapter = new OPSQLiteAdapter();
    adapter.db = db;
    return adapter;
  }

  public constructor(dbName: string = "jazz-storage") {
    this.dbName = dbName;
  }

  public async initialize(): Promise<void> {
    if (this.db) {
      return;
    }

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

    "executeRaw" in this.db
      ? await this.db.executeRaw(sql, params as any[])
      : await this.db.execute(sql, params as any[]);
  }

  public async transaction(callback: (tx: OPSQLiteAdapter) => unknown) {
    if (!this.db) {
      throw new Error("Database not initialized");
    }
    if (!("transaction" in this.db)) {
      throw new Error("Cannot perform nested transactions");
    }

    await this.db.transaction(async (tx) => {
      try {
        await callback(OPSQLiteAdapter.withDB(tx));
        await tx.commit();
      } catch (error) {
        await tx.rollback();
        throw error;
      }
    });
  }

  public async closeDb(): Promise<void> {
    // Keeping the database open and reusing the same connection over multiple ctx instances.
  }
}
