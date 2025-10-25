import { StorageApiSync } from "../storageSync.js";
import { SQLiteClient } from "./client.js";
import { getSQLiteMigrationQueries } from "./sqliteMigrations.js";
import type { SQLiteDatabaseDriver } from "./types.js";

export type { SQLiteDatabaseDriver };

export function getSqliteStorage(db: SQLiteDatabaseDriver) {
  let userVersion: number;

  if (db.getMigrationVersion) {
    userVersion = db.getMigrationVersion();
  } else {
    const rows = db.query<{ user_version: string }>("PRAGMA user_version", []);
    userVersion = rows[0]?.user_version ? Number(rows[0].user_version) : 0;
  }

  const migrations = getSQLiteMigrationQueries(userVersion);

  for (const migration of migrations) {
    for (const query of migration.queries) {
      db.run(query, []);
    }
    if (db.saveMigrationVersion) {
      db.saveMigrationVersion(migration.version);
    } else {
      db.run(`PRAGMA user_version = ${migration.version};`, []);
    }
  }

  return new StorageApiSync(new SQLiteClient(db));
}
