export { SQLiteClientAsync } from "./client.js";
export type { SQLiteDatabaseDriverAsync } from "./types.js";

import { getSQLiteMigrationQueries } from "../sqlite/sqliteMigrations.js";
import { StorageApiAsync } from "../storageAsync.js";
import { SQLiteClientAsync } from "./client.js";
import type { SQLiteDatabaseDriverAsync } from "./types.js";

export async function getSqliteStorageAsync(db: SQLiteDatabaseDriverAsync) {
  await db.initialize();

  const rows = await db.query<{ user_version: string }>(
    "PRAGMA user_version",
    [],
  );
  const userVersion = Number(rows[0]?.user_version) ?? 0;

  const migrations = getSQLiteMigrationQueries(userVersion);

  for (const migration of migrations) {
    await db.run(migration, []);
  }

  return new StorageApiAsync(new SQLiteClientAsync(db));
}
