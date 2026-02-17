export const migrations: Record<number, string[]> = {
  1: [
    `CREATE TABLE IF NOT EXISTS transactions (
      ses INTEGER,
      idx INTEGER,
      tx TEXT NOT NULL,
      PRIMARY KEY (ses, idx)
    ) WITHOUT ROWID;`,
    `CREATE TABLE IF NOT EXISTS sessions (
      rowID INTEGER PRIMARY KEY,
      coValue INTEGER NOT NULL,
      sessionID TEXT NOT NULL,
      lastIdx INTEGER,
      lastSignature TEXT,
      UNIQUE (sessionID, coValue)
    );`,
    "CREATE INDEX IF NOT EXISTS sessionsByCoValue ON sessions (coValue);",
    `CREATE TABLE IF NOT EXISTS coValues (
      rowID INTEGER PRIMARY KEY,
      id TEXT NOT NULL UNIQUE,
      header TEXT NOT NULL UNIQUE
    );`,
    "CREATE INDEX IF NOT EXISTS coValuesByID ON coValues (id);",
  ],
  3: [
    `CREATE TABLE IF NOT EXISTS signatureAfter (
      ses INTEGER,
      idx INTEGER,
      signature TEXT NOT NULL,
      PRIMARY KEY (ses, idx)
    ) WITHOUT ROWID;`,
    "ALTER TABLE sessions ADD COLUMN bytesSinceLastSignature INTEGER;",
  ],
  4: [
    `CREATE TABLE IF NOT EXISTS unsynced_covalues (
      rowID INTEGER PRIMARY KEY,
      co_value_id TEXT NOT NULL,
      peer_id TEXT NOT NULL,
      UNIQUE (co_value_id, peer_id)
    );`,
    "CREATE INDEX IF NOT EXISTS idx_unsynced_covalues_co_value_id ON unsynced_covalues(co_value_id);",
  ],
  5: [
    `CREATE TABLE IF NOT EXISTS deletedCoValues (
      coValueID TEXT PRIMARY KEY,
      status INTEGER NOT NULL DEFAULT 0
    ) WITHOUT ROWID;`,
    "CREATE INDEX IF NOT EXISTS deletedCoValuesByStatus ON deletedCoValues (status);",
  ],
  6: [
    `CREATE TABLE IF NOT EXISTS storageReconciliationLocks (
      key TEXT PRIMARY KEY,
      holderSessionId TEXT NOT NULL,
      acquiredAt INTEGER NOT NULL,
      expiresAt INTEGER NOT NULL,
      lastProcessedOffset INTEGER NOT NULL DEFAULT 0,
      releasedAt INTEGER
    ) WITHOUT ROWID;`,
  ],
  7: ["ALTER TABLE storageReconciliationLocks DROP COLUMN expiresAt;"],
};

type Migration = {
  version: number;
  queries: string[];
};

export function getSQLiteMigrationQueries(version: number): Migration[] {
  return Object.keys(migrations)
    .map((k) => Number.parseInt(k, 10))
    .filter((v) => v > version)
    .sort((a, b) => a - b)
    .map((v) => ({ version: v, queries: migrations[v] ?? [] }));
}
