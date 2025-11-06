import type { SQLiteDatabaseDriver } from "cojson";
import { getSqliteStorage } from "cojson";
import type { DurableObjectStorage } from "@cloudflare/workers-types";

export class DurableObjectSqlDriver implements SQLiteDatabaseDriver {
  private readonly storage: DurableObjectStorage;

  constructor(durableObjectStorage: DurableObjectStorage) {
    this.storage = durableObjectStorage;
  }

  run(sql: string, params: unknown[]) {
    this.storage.sql.exec(sql, ...params);
  }

  query<T>(sql: string, params: unknown[]): T[] {
    return this.storage.sql.exec(sql, ...params).toArray() as T[];
  }

  get<T>(sql: string, params: unknown[]): T | undefined {
    const res = this.storage.sql.exec(sql, ...params);
    if (res.rowsRead === 0) {
      return undefined;
    }
    return res.one() as T;
  }

  transaction(callback: () => unknown) {
    return this.storage.transactionSync(callback);
  }

  closeDb() {
    return;
  }

  getMigrationVersion(): number {
    this.run(
      "CREATE TABLE IF NOT EXISTS _migration_version (version INTEGER);",
      [],
    );
    return (
      this.get<{ version: number }>(
        "SELECT max(version) as version FROM _migration_version;",
        [],
      )?.version ?? 0
    );
  }

  saveMigrationVersion(version: number): void {
    this.run("INSERT INTO _migration_version (version) VALUES (?);", [version]);
  }
}

export function getDurableObjectSqlStorage(
  durableObjectStorage: DurableObjectStorage,
) {
  const db = new DurableObjectSqlDriver(durableObjectStorage);

  return getSqliteStorage(db);
}
