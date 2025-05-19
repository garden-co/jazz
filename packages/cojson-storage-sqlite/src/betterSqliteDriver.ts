import Database, { type Database as DatabaseT } from "better-sqlite3";
import {
  type SQLiteDatabaseDriver,
  type TransactionRow,
  getSQLiteMigrationQueries,
} from "cojson-storage";

export class BetterSqliteDriver implements SQLiteDatabaseDriver {
  private readonly db: DatabaseT;

  constructor(filename: string) {
    this.db = new Database(filename);
  }

  initialize() {
    const db = this.db;

    db.pragma("journal_mode = WAL");

    const oldVersion = (
      db.pragma("user_version") as [{ user_version: number }]
    )[0].user_version as number;

    const migrations = getSQLiteMigrationQueries(oldVersion);

    for (const migration of migrations) {
      db.prepare(migration).run();
    }
  }

  run(sql: string, params: unknown[]) {
    this.db.prepare(sql).run(params);
  }

  query<T>(sql: string, params: unknown[]): T[] {
    return this.db.prepare(sql).all(params) as T[];
  }

  transaction(callback: () => unknown) {
    return this.db.transaction(callback)();
  }
}
