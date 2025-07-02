import { StorageApiSync } from "../storageSync.js";
import { SQLiteClient } from "./client.js";
import { getSQLiteMigrationQueries } from "./sqliteMigrations.js";
import type { SQLiteDatabaseDriver } from "./types.js";

export type { SQLiteDatabaseDriver };

export function getSqliteStorage(db: SQLiteDatabaseDriver) {
  const rows = db.query<{ user_version: string }>("PRAGMA user_version", []);
  const userVersion = Number(rows[0]?.user_version) ?? 0;

  const migrations = getSQLiteMigrationQueries(userVersion);

  for (const migration of migrations) {
    db.run(migration, []);
  }

  return new StorageApiSync(new SQLiteClient(db));
}
