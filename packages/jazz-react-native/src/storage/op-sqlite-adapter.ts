import * as opSQLite from "@op-engineering/op-sqlite";
import {
  ANDROID_DATABASE_PATH,
  IOS_LIBRARY_PATH,
} from "@op-engineering/op-sqlite";
import type { SQLResult, SQLRow } from "jazz-react-native-core";
import { SQLiteAdapterBase } from "jazz-react-native-core";
import { Platform } from "react-native";

type OPSQLiteDB = ReturnType<typeof opSQLite.open>;

export class OPSQLiteAdapter extends SQLiteAdapterBase {
  private db: OPSQLiteDB | null = null;
  private dbPath: string;

  public constructor(dbName: string = "jazz-storage") {
    super(dbName);
    this.dbPath =
      Platform.OS === "ios" ? IOS_LIBRARY_PATH : ANDROID_DATABASE_PATH;
  }

  public executeSync(sql: string, params?: unknown[]): { rows: SQLRow[] } {
    if (!this.isInitialized || !this.db) {
      throw new Error("Database not initialized. Call initialize() first.");
    }
    const result = this.db.executeSync(sql, params as any[]);
    return {
      rows: result.rows as SQLRow[],
    };
  }

  protected open() {
    try {
      // Open database first
      this.db = opSQLite.open({
        name: this.dbName,
        location: this.dbPath,
      });

      const db = this.db;
      if (!db) throw new Error("Failed to open database");

      db.execute("PRAGMA journal_mode=WAL");
    } catch (e) {
      console.error("[OPSQLiteAdapter] open failed:", e);
      throw new Error(
        `Failed to open OPSQLiteAdapter: ${e instanceof Error ? e.message : String(e)}`,
      );
    }
  }

  protected close(): void {
    if (this.db) {
      this.db.close();
      this.isInitialized = false;
      this.initialization = undefined;
    }
  }

  protected delete(): void {
    if (this.db) {
      this.close();
      this.db.delete();
    }
  }

  protected rawDbExecute(sql: string, params?: unknown[]): SQLResult {
    if (!this.db) {
      throw new Error("Database not opened");
    }
    const result = this.db.executeSync(sql, params as any[]);
    return {
      rows: result.rows as SQLRow[],
      insertId: result.insertId,
      rowsAffected: result.rowsAffected,
    };
  }
}
