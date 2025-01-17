import * as opSQLite from "@op-engineering/op-sqlite";
import {
  ANDROID_DATABASE_PATH,
  IOS_LIBRARY_PATH,
} from "@op-engineering/op-sqlite";
import type {
  SQLResult,
  SQLRow,
  SQLiteAdapter,
} from "cojson-storage-rn-sqlite";
import { Platform } from "react-native";

type OPSQLiteDB = ReturnType<typeof opSQLite.open>;

export class OPSQLiteAdapter implements SQLiteAdapter {
  private db: OPSQLiteDB | null = null;
  private dbName: string;
  private dbPath: string;
  private initializationPromise: Promise<void> | null = null;
  private isInitialized = false;

  constructor(dbName: string) {
    this.dbName = dbName;
    this.dbPath =
      Platform.OS === "ios" ? IOS_LIBRARY_PATH : ANDROID_DATABASE_PATH;
  }

  private async ensureInitialized() {
    // Return immediately if already initialized
    if (this.isInitialized) {
      console.log("[OPSQLiteAdapter] already initialized");
      return;
    }

    // If initialization is in progress, wait for it
    if (this.initializationPromise) {
      console.log("[OPSQLiteAdapter] waiting for initialization");
      await this.initializationPromise;
      console.log("[OPSQLiteAdapter] initialization complete");
      return;
    }

    // Start initialization
    this.initializationPromise = this.initializeInternal();
    try {
      console.log("[OPSQLiteAdapter] waiting for initialization");
      await this.initializationPromise;
      console.log("[OPSQLiteAdapter] initialization complete");
      this.isInitialized = true;
    } catch (error) {
      // Clear the promise on failure so future attempts can retry
      this.initializationPromise = null;
      throw error;
    }
  }

  private async initializeInternal() {
    console.log("[OPSQLiteAdapter] initializing");
    try {
      this.db = opSQLite.open({
        name: this.dbName,
        location: this.dbPath,
      });
      console.log("[OPSQLiteAdapter] initialization complete");
    } catch (e) {
      throw new Error(
        "@op-engineering/op-sqlite is not installed. Please install it to use OPSQLiteAdapter.",
      );
    }

    await this.execute("PRAGMA journal_mode=WAL");
    const { rows } = await this.execute("PRAGMA user_version");
    console.log("[OPSQLiteAdapter] user_version", rows);
    const oldVersion = Number(rows[0]?.user_version) ?? 0;

    if (oldVersion === 0) {
      await this.execute(
        `CREATE TABLE IF NOT EXISTS transactions (
          ses INTEGER,
          idx INTEGER,
          tx TEXT NOT NULL,
          PRIMARY KEY (ses, idx)
        ) WITHOUT ROWID;`,
      );

      console.log("[OPSQLiteAdapter] creating sessions table");

      await this.execute(
        `CREATE TABLE IF NOT EXISTS sessions (
          rowID INTEGER PRIMARY KEY,
          coValue INTEGER NOT NULL,
          sessionID TEXT NOT NULL,
          lastIdx INTEGER,
          lastSignature TEXT,
          UNIQUE (sessionID, coValue)
        );`,
      );

      console.log("[OPSQLiteAdapter] creating sessionsByCoValue index");

      await this.execute(
        `CREATE INDEX IF NOT EXISTS sessionsByCoValue ON sessions (coValue);`,
      );

      console.log("[OPSQLiteAdapter] creating coValues table");

      await this.execute(
        `CREATE TABLE IF NOT EXISTS coValues (
          rowID INTEGER PRIMARY KEY,
          id TEXT NOT NULL UNIQUE,
          header TEXT NOT NULL UNIQUE
        );`,
      );

      console.log("[OPSQLiteAdapter] creating coValuesByID index");

      await this.execute(
        `CREATE INDEX IF NOT EXISTS coValuesByID ON coValues (id);`,
      );

      console.log("[OPSQLiteAdapter] setting user_version to 1");

      await this.execute("PRAGMA user_version = 1");
    }

    if (oldVersion <= 2) {
      console.log("[OPSQLiteAdapter] creating signatureAfter table");

      await this.execute(
        `CREATE TABLE IF NOT EXISTS signatureAfter (
          ses INTEGER,
          idx INTEGER,
          signature TEXT NOT NULL,
          PRIMARY KEY (ses, idx)
        ) WITHOUT ROWID;`,
      );

      console.log(
        "[OPSQLiteAdapter] adding bytesSinceLastSignature column to sessions table",
      );

      await this.execute(
        `ALTER TABLE sessions ADD COLUMN bytesSinceLastSignature INTEGER;`,
      );

      await this.execute("PRAGMA user_version = 3");

      console.log("[OPSQLiteAdapter] setting user_version to 3");
    }
  }

  async initialize(): Promise<void> {
    console.log("[OPSQLiteAdapter] initializing");
    await this.ensureInitialized();
    console.log("[OPSQLiteAdapter] initialization complete");
  }

  async execute(sql: string, params?: unknown[]): Promise<SQLResult> {
    console.log("[OPSQLiteAdapter] executing", sql);
    if (!this.db) {
      await this.ensureInitialized();
    }
    try {
      console.log("[OPSQLiteAdapter] executing", sql);
      const result = await this.db!.execute(sql, params as any[]);
      console.log("[OPSQLiteAdapter] execution complete");
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

  executeSync(sql: string, params?: unknown[]): { rows: SQLRow[] } {
    if (!this.isInitialized || !this.db) {
      throw new Error("Database not initialized. Call initialize() first.");
    }
    console.log("[OPSQLiteAdapter] executing sync", sql);
    const result = this.db.executeSync(sql, params as any[]);
    console.log("[OPSQLiteAdapter] execution complete");
    return {
      rows: result.rows as SQLRow[],
    };
  }

  async transaction(callback: () => Promise<void>): Promise<void> {
    if (!this.db) {
      await this.ensureInitialized();
    }
    console.log("[OPSQLiteAdapter] transaction: calling callback");
    await this.db!.transaction(callback);
    console.log("[OPSQLiteAdapter] transaction: callback complete");
  }
}
