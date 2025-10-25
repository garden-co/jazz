export { SQLiteClientAsync } from "./client.js";
export type { SQLiteDatabaseDriverAsync } from "./types.js";

import { getSQLiteMigrationQueries } from "../sqlite/sqliteMigrations.js";
import { StorageApiAsync } from "../storageAsync.js";
import { SQLiteClientAsync } from "./client.js";
import type { SQLiteDatabaseDriverAsync } from "./types.js";

export async function getSqliteStorageAsync(db: SQLiteDatabaseDriverAsync) {
  await db.initialize();

  let userVersion: number;

  if (db.getMigrationVersion) {
    userVersion = await db.getMigrationVersion();
  } else {
    const rows = await db.query<{ user_version: string }>(
      "PRAGMA user_version",
      [],
    );
    userVersion = rows[0]?.user_version ? Number(rows[0].user_version) : 0;
  }

  const migrations = getSQLiteMigrationQueries(userVersion);

  for (const migration of migrations) {
    for (const query of migration.queries) {
      await db.run(query, []);
    }
    if (db.saveMigrationVersion) {
      await db.saveMigrationVersion(migration.version);
    } else {
      await db.run(`PRAGMA user_version = ${migration.version};`, []);
    }
  }

  return new StorageApiAsync(new SQLiteClientAsync(db));
}
