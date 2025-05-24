import Database, { type Database as DatabaseT } from "better-sqlite3";
import type { SQLiteDatabaseDriver } from "cojson-storage";

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
}
