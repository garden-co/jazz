import type {
  SQLResult,
  SQLRow,
  SQLiteAdapter,
} from "cojson-storage-rn-sqlite";
import type { Database, SQLStatementArg } from "expo-sqlite";
import * as SQLite from "expo-sqlite";

const SQLITE_CONSTRAINT = 6;
const SQLITE_SYNTAX_ERR = 1;

export class ExpoSQLiteAdapter implements SQLiteAdapter {
  private db: Database | null = null;
  private dbName: string;
  private initializationPromise: Promise<void> | null = null;
  private isInitialized = false;

  constructor(dbName: string) {
    this.dbName = dbName;
  }

  private async ensureInitialized() {
    // Return immediately if already initialized
    if (this.isInitialized) {
      console.log("[ExpoSQLiteAdapter] already initialized");
      return;
    }

    // If initialization is in progress, wait for it
    if (this.initializationPromise) {
      console.log("[ExpoSQLiteAdapter] waiting for initialization");
      await this.initializationPromise;
      console.log("[ExpoSQLiteAdapter] initialization complete");
      return;
    }

    // Start initialization
    this.initializationPromise = this.initializeInternal();
    try {
      console.log("[ExpoSQLiteAdapter] starting initialization");
      await this.initializationPromise;
      console.log("[ExpoSQLiteAdapter] initialization complete");
      this.isInitialized = true;
    } catch (error) {
      // Clear the promise on failure so future attempts can retry
      this.initializationPromise = null;
      throw error;
    }
  }

  private async initializeInternal() {
    try {
      console.log("[ExpoSQLiteAdapter] Opening database:", this.dbName);
      this.db = SQLite.openDatabase(this.dbName);
      // Check if database is accessible
      const testResult = await this.executeSql("SELECT 1");
      if (!testResult.rows || testResult.rows.length === 0) {
        throw new Error("Database connection test failed");
      }

      console.log("[ExpoSQLiteAdapter] Checking schema version");
      const { rows } = await this.executeSql("PRAGMA user_version");
      const oldVersion = Number(rows[0]?.user_version) ?? 0;

      if (oldVersion === 0) {
        console.log("[ExpoSQLiteAdapter] Creating initial schema");
        await this.executeSql(
          `CREATE TABLE IF NOT EXISTS transactions (
            ses INTEGER,
            idx INTEGER,
            tx TEXT NOT NULL,
            PRIMARY KEY (ses, idx)
          ) WITHOUT ROWID;`,
        );

        console.log("[ExpoSQLiteAdapter] Creating sessions table");
        await this.executeSql(
          `CREATE TABLE IF NOT EXISTS sessions (
            rowID INTEGER PRIMARY KEY,
            coValue INTEGER NOT NULL,
            sessionID TEXT NOT NULL,
            lastIdx INTEGER,
            lastSignature TEXT,
            UNIQUE (sessionID, coValue)
          );`,
        );

        console.log("[ExpoSQLiteAdapter] Creating sessionsByCoValue index");
        await this.executeSql(
          `CREATE INDEX IF NOT EXISTS sessionsByCoValue ON sessions (coValue);`,
        );

        console.log("[ExpoSQLiteAdapter] Creating coValues table");
        await this.executeSql(
          `CREATE TABLE IF NOT EXISTS coValues (
            rowID INTEGER PRIMARY KEY,
            id TEXT NOT NULL UNIQUE,
            header TEXT NOT NULL UNIQUE
          );`,
        );

        console.log("[ExpoSQLiteAdapter] Creating coValuesByID index");
        await this.executeSql(
          `CREATE INDEX IF NOT EXISTS coValuesByID ON coValues (id);`,
        );

        console.log("[ExpoSQLiteAdapter] Setting user_version to 1");
        await this.executeSql("PRAGMA user_version = 1");
      }

      if (oldVersion <= 2) {
        console.log("[ExpoSQLiteAdapter] Upgrading to schema version 3");
        await this.executeSql(
          `CREATE TABLE IF NOT EXISTS signatureAfter (
            ses INTEGER,
            idx INTEGER,
            signature TEXT NOT NULL,
            PRIMARY KEY (ses, idx)
          ) WITHOUT ROWID;`,
        );

        console.log(
          "[ExpoSQLiteAdapter] Adding bytesSinceLastSignature column",
        );
        await this.executeSql(
          `ALTER TABLE sessions ADD COLUMN bytesSinceLastSignature INTEGER;`,
        );

        await this.executeSql("PRAGMA user_version = 3");
      }

      console.log("[ExpoSQLiteAdapter] Initialization complete");
    } catch (e) {
      console.error("[ExpoSQLiteAdapter] Initialization failed:", e);
      throw new Error(
        `Failed to initialize ExpoSQLiteAdapter: ${e instanceof Error ? e.message : String(e)}`,
      );
    }
  }

  private async executeSql(
    sql: string,
    params?: unknown[],
  ): Promise<SQLResult> {
    if (!this.db) {
      throw new Error("Database not initialized. Call initialize() first.");
    }

    return new Promise((resolve, reject) => {
      this.db!.transaction(
        (tx) => {
          tx.executeSql(
            sql,
            params as SQLStatementArg[],
            (_, result) => {
              resolve({
                rows: [...Array(result.rows.length)].map((_, i) =>
                  result.rows.item(i),
                ),
                insertId: result.insertId,
                rowsAffected: result.rowsAffected,
              });
            },
            (_, error) => {
              console.error(
                `[ExpoSQLiteAdapter] SQL Error: ${error.message} in query: ${sql}`,
              );
              if (error.code === SQLITE_CONSTRAINT) {
                reject(new Error(`Constraint violation: ${error.message}`));
              } else if (error.code === SQLITE_SYNTAX_ERR) {
                reject(new Error(`SQL syntax error: ${error.message}`));
              } else {
                reject(error);
              }
              return false;
            },
          );
        },
        (error) => {
          reject(new Error(`Transaction error: ${error.message}`));
        },
      );
    });
  }

  async initialize(): Promise<void> {
    await this.ensureInitialized();
  }

  async execute(sql: string, params?: unknown[]): Promise<SQLResult> {
    if (!this.db) {
      await this.ensureInitialized();
    }
    return this.executeSql(sql, params);
  }

  executeSync(sql: string, params?: unknown[]): { rows: SQLRow[] } {
    throw new Error("Synchronous execution is not supported by expo-sqlite");
  }

  async transaction(callback: () => Promise<void>): Promise<void> {
    await this.ensureInitialized();

    return new Promise((resolve, reject) => {
      this.db!.transaction(async (tx) => {
        // Override execute to use transaction context
        const originalExecute = this.execute.bind(this);
        this.execute = async (sql: string, params?: unknown[]) => {
          return new Promise((resolve, reject) => {
            tx.executeSql(
              sql,
              params as SQLStatementArg[],
              (_, result) => {
                resolve({
                  rows: [...Array(result.rows.length)].map((_, i) =>
                    result.rows.item(i),
                  ),
                  insertId: result.insertId,
                  rowsAffected: result.rowsAffected,
                });
              },
              (_, error) => {
                reject(error);
                return false;
              },
            );
          });
        };

        try {
          await callback();
          resolve();
        } catch (error) {
          reject(error);
        } finally {
          // Restore original execute
          this.execute = originalExecute;
        }
      }, reject);
    });
  }

  async close(): Promise<void> {
    // expo-sqlite doesn't support closing databases
    this.db = null;
    this.isInitialized = false;
    this.initializationPromise = null;
  }

  async delete(): Promise<void> {
    if (this.db) {
      await this.close();
      // expo-sqlite doesn't support deleting databases
      this.db = null;
    }
  }
}
