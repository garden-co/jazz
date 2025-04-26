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

  public async executeAsync(
    sql: string,
    params?: unknown[],
  ): Promise<SQLResult> {
    await this.ensureInitialized();

    const db = this.db;
    if (!db) {
      throw new Error("Database not available after initialization");
    }

    try {
      const result = await db.execute(sql, params as any[]);
      return {
        rows: result.rows as SQLRow[],
        insertId:
          result.rowsAffected > 0
            ? (result.rows[0]?.rowid as number)
            : undefined,
        rowsAffected: result.rowsAffected,
      };
    } catch (error) {
      console.error("[OPSQLiteAdapter] SQL execution error:", error);
      throw error;
    }
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

  public async transactionAsync(callback: () => Promise<void>): Promise<void> {
    if (!this.db) {
      await this.ensureInitialized();
    }
    await this.db!.transaction(callback);
  }

  protected async open() {
    try {
      // Open database first
      this.db = opSQLite.open({
        name: this.dbName,
        location: this.dbPath,
      });

      const db = this.db;
      if (!db) throw new Error("Failed to open database");

      await db.execute("PRAGMA journal_mode=WAL");
    } catch (e) {
      console.error("[OPSQLiteAdapter] open failed:", e);
      throw new Error(
        `Failed to open OPSQLiteAdapter: ${e instanceof Error ? e.message : String(e)}`,
      );
    }
  }

  protected async close(): Promise<void> {
    if (this.db) {
      this.db.close();
      this.isInitialized = false;
      this.initializationPromise = null;
    }
    return Promise.resolve();
  }

  protected async delete(): Promise<void> {
    if (this.db) {
      await this.close();
      this.db.delete();
    }
    return Promise.resolve();
  }

  protected async rawDbExecuteAsync(
    sql: string,
    params?: unknown[],
  ): Promise<SQLResult> {
    if (!this.db) {
      throw new Error("Database not opened");
    }
    const result = await this.db.execute(sql, params as any[]);
    return {
      rows: result.rows as SQLRow[],
      insertId: result.insertId,
      rowsAffected: result.rowsAffected,
    };
  }
}
