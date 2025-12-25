import Database, { type Database as DatabaseT } from "better-sqlite3";
import type { SQLiteDatabaseDriver } from "cojson";
import { getSqliteStorage } from "cojson";
import { DatabaseSync } from "node:sqlite";

export class BetterSqliteDriver implements SQLiteDatabaseDriver {
  private readonly db: DatabaseT;

  constructor(filename: string) {
    const db = new Database(filename);
    this.db = db;
    db.pragma("journal_mode = WAL");
  }

  run(sql: string, params: unknown[]) {
    this.db.prepare(sql).run(params);
  }

  query<T>(sql: string, params: unknown[]): T[] {
    return this.db.prepare(sql).all(params) as T[];
  }

  get<T>(sql: string, params: unknown[]): T | undefined {
    return this.db.prepare(sql).get(params) as T | undefined;
  }

  transaction(callback: () => unknown) {
    return this.db.transaction(callback)();
  }

  closeDb() {
    this.db.close();
  }
}

export function getBetterSqliteStorage(filename: string) {
  const db = new BetterSqliteDriver(filename);

  return getSqliteStorage(db);
}

export class NodeSqliteDriver implements SQLiteDatabaseDriver {
  private db: DatabaseSync;
  private readonly filename: string;

  constructor(filename: string) {
    this.filename = filename;
    this.db = new DatabaseSync(this.filename);
    this.db.exec("PRAGMA journal_mode = WAL");
  }

  run(sql: string, params: unknown[]) {
    const stmt = this.db.prepare(sql);
    stmt.run(...(params as any[]));
  }

  query<T>(sql: string, params: unknown[]): T[] {
    const stmt = this.db.prepare(sql);
    return stmt.all(...(params as any[])) as T[];
  }

  get<T>(sql: string, params: unknown[]): T | undefined {
    const stmt = this.db.prepare(sql);
    return stmt.get(...(params as any[])) as T | undefined;
  }

  transaction(callback: () => unknown) {
    this.run("BEGIN IMMEDIATE", []);
    try {
      const result = callback();
      this.run("COMMIT", []);
      return result;
    } catch (error) {
      this.run("ROLLBACK", []);
      throw error;
    }
  }

  closeDb() {
    this.db.close();
  }
}

export function getNodeSqliteStorage(filename: string) {
  const db = new NodeSqliteDriver(filename);
  return getSqliteStorage(db);
}
