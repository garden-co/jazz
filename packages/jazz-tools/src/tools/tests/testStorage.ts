import { randomUUID } from "node:crypto";
import { unlinkSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { getSqliteStorageAsync, SQLiteDatabaseDriverAsync } from "cojson";
import Database, { type Database as DatabaseT } from "libsql";
import { onTestFinished } from "vitest";

class LibSQLSqliteAsyncDriver implements SQLiteDatabaseDriverAsync {
  private readonly db: DatabaseT;

  constructor(filename: string) {
    this.db = new Database(filename, {});
  }

  async initialize() {
    await this.db.pragma("journal_mode = WAL");
  }

  async run(sql: string, params: unknown[]) {
    this.db.prepare(sql).run(params);
  }

  async query<T>(sql: string, params: unknown[]): Promise<T[]> {
    return this.db.prepare(sql).all(params) as T[];
  }

  async get<T>(sql: string, params: unknown[]): Promise<T | undefined> {
    return this.db.prepare(sql).get(params) as T | undefined;
  }

  async transaction(callback: () => unknown) {
    await this.run("BEGIN TRANSACTION", []);

    try {
      await callback();
      await this.run("COMMIT", []);
    } catch (error) {
      await this.run("ROLLBACK", []);
    }
  }

  async closeDb() {
    this.db.close();
  }
}

export async function createAsyncStorage({ filename }: { filename?: string }) {
  const storage = await getSqliteStorageAsync(
    new LibSQLSqliteAsyncDriver(getDbPath(filename)),
  );

  onTestFinished(() => {
    storage.close();
  });

  return storage;
}

export function getDbPath(defaultDbPath?: string) {
  const dbPath = defaultDbPath ?? join(tmpdir(), `test-${randomUUID()}.db`);

  if (!defaultDbPath) {
    onTestFinished(() => {
      unlinkSync(dbPath);
    });
  }

  return dbPath;
}
