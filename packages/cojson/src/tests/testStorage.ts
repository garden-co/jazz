import { randomUUID } from "node:crypto";
import { unlinkSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import Database, { type Database as DatabaseT } from "libsql";
import { onTestFinished } from "vitest";
import { RawCoID, StorageAPI } from "../exports";
import { SQLiteDatabaseDriver } from "../storage";
import { getSqliteStorage } from "../storage/sqlite";
import {
  SQLiteDatabaseDriverAsync,
  getSqliteStorageAsync,
} from "../storage/sqliteAsync";
import { SyncMessagesLog, SyncTestMessage } from "./testUtils";

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

class LibSQLSqliteSyncDriver implements SQLiteDatabaseDriver {
  private readonly db: DatabaseT;

  constructor(filename: string) {
    this.db = new Database(filename, {});
  }

  initialize() {
    this.db.pragma("journal_mode = WAL");
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
    this.run("BEGIN TRANSACTION", []);

    try {
      callback();
      this.run("COMMIT", []);
    } catch (error) {
      this.run("ROLLBACK", []);
    }
  }

  closeDb() {
    this.db.close();
  }
}

export async function createAsyncStorage({
  filename,
  nodeName = "client",
  storageName = "storage",
}: { filename?: string; nodeName: string; storageName: string }) {
  const storage = await getSqliteStorageAsync(
    new LibSQLSqliteAsyncDriver(getDbPath(filename)),
  );

  onTestFinished(() => {
    storage.close();
  });

  trackStorageMessages(storage, nodeName, storageName);

  return storage;
}

export function createSyncStorage({
  filename,
  nodeName = "client",
  storageName = "storage",
}: { filename?: string; nodeName: string; storageName: string }) {
  const storage = getSqliteStorage(
    new LibSQLSqliteSyncDriver(getDbPath(filename)),
  );

  trackStorageMessages(storage, nodeName, storageName);

  return storage;
}

function getDbPath(defaultDbPath?: string) {
  const dbPath = defaultDbPath ?? join(tmpdir(), `test-${randomUUID()}.db`);

  if (!defaultDbPath) {
    onTestFinished(() => {
      unlinkSync(dbPath);
    });
  }

  return dbPath;
}

function trackStorageMessages(
  storage: StorageAPI,
  nodeName: string,
  storageName: string,
) {
  const originalStore = storage.store;
  const originalLoad = storage.load;

  storage.store = function (data, correctionCallback) {
    SyncMessagesLog.add({
      from: nodeName,
      to: storageName,
      msg: data,
    });

    return originalStore.call(storage, data, (correction) => {
      SyncMessagesLog.add({
        from: storageName,
        to: nodeName,
        msg: {
          action: "known",
          isCorrection: true,
          ...correction,
        },
      });

      const correctionMessages = correctionCallback(correction);

      if (correctionMessages) {
        for (const msg of correctionMessages) {
          SyncMessagesLog.add({
            from: nodeName,
            to: storageName,
            msg,
          });
        }
      }

      return correctionMessages;
    });
  };

  storage.load = function (id, callback, done) {
    SyncMessagesLog.add({
      from: nodeName,
      to: storageName,
      msg: {
        action: "load",
        id: id as RawCoID,
        sessions: {},
        header: false,
      },
    });

    return originalLoad.call(
      storage,
      id,
      (msg) => {
        SyncMessagesLog.add({
          from: storageName,
          to: nodeName,
          msg,
        });

        return callback(msg);
      },
      (found) => {
        if (!found) {
          SyncMessagesLog.add({
            from: storageName,
            to: nodeName,
            msg: {
              action: "known",
              id: id as RawCoID,
              sessions: {},
              header: false,
            },
          });
        }

        return done?.(found);
      },
    );
  };
}
