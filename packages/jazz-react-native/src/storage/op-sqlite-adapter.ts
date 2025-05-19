import * as opSQLite from "@op-engineering/op-sqlite";
import {
  ANDROID_DATABASE_PATH,
  IOS_LIBRARY_PATH,
} from "@op-engineering/op-sqlite";
import { Platform } from "react-native";

type OPSQLiteDB = ReturnType<typeof opSQLite.open>;

import { type SQLiteDatabaseDriver } from "jazz-react-native-core";

export class OPSQLiteAdapter implements SQLiteDatabaseDriver {
  private db: OPSQLiteDB | null = null;
  private dbName: string;

  public constructor(dbName: string = "jazz-storage") {
    this.dbName = dbName;
    const dbPath =
      Platform.OS === "ios" ? IOS_LIBRARY_PATH : ANDROID_DATABASE_PATH;

    const db = (this.db = opSQLite.open({
      name: this.dbName,
      location: dbPath,
    }));
    db.execute("PRAGMA journal_mode=WAL");
  }

  public query<T>(sql: string, params?: unknown[]): T[] {
    if (!this.db) {
      throw new Error("Database not initialized");
    }

    return this.db.executeSync(sql, params as any[]).rows as T[];
  }

  public run(sql: string, params?: unknown[]) {
    if (!this.db) {
      throw new Error("Database not initialized");
    }

    this.db.executeRaw(sql, params as any[]);
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
