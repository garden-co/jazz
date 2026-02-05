import { randomUUID } from "node:crypto";
import { unlinkSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { SQLiteDatabaseDriverAsync, getSqliteStorageAsync } from "cojson";
import Database, { type Database as DatabaseT } from "libsql";
import { onTestFinished } from "vitest";

/** Cleanup functions registered by createAsyncStorage; run by the runner hook (registered first so it runs last). */
const storageCleanupFns: Array<() => void | Promise<void>> = [];

function registerStorageCleanup(fn: () => void | Promise<void>): void {
  storageCleanupFns.push(fn);
}

/** Call from beforeEach so this hook is registered first and thus runs last (LIFO), after node shutdown hooks. */
export function registerStorageCleanupRunner(): void {
  // Clear cleanup functions from previous test
  storageCleanupFns.length = 0;
  onTestFinished(async () => {
    for (const fn of storageCleanupFns) {
      await fn();
    }
  });
}
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

  async transaction(callback: (tx: LibSQLSqliteAsyncDriver) => unknown) {
    await this.run("BEGIN TRANSACTION", []);

    try {
      await callback(this);
      await this.run("COMMIT", []);
    } catch (error) {
      await this.run("ROLLBACK", []);
      throw error;
    }
  }

  async closeDb() {
    this.db.close();
  }
}

function unlinkIfExists(path: string): void {
  try {
    unlinkSync(path);
  } catch (error: unknown) {
    const err = error as NodeJS.ErrnoException;
    if (err?.code !== "ENOENT") {
      console.error(error);
    }
  }
}

function deleteDb(dbPath: string): void {
  unlinkIfExists(dbPath);
  unlinkIfExists(`${dbPath}-wal`);
  unlinkIfExists(`${dbPath}-shm`);
}

export async function createAsyncStorage({ filename }: { filename?: string }) {
  const dbPath = getDbPath(filename);
  const storage = await getSqliteStorageAsync(
    new LibSQLSqliteAsyncDriver(dbPath),
  );

  registerStorageCleanup(async () => {
    await storage.close();
    deleteDb(dbPath);
  });

  return storage;
}

export function getDbPath(defaultDbPath?: string) {
  return defaultDbPath ?? join(tmpdir(), `test-${randomUUID()}.db`);
}
