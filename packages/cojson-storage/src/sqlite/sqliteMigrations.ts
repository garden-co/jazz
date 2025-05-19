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
    "PRAGMA user_version = 1;",
  ],
  3: [
    `CREATE TABLE IF NOT EXISTS signatureAfter (
      ses INTEGER,
      idx INTEGER,
      signature TEXT NOT NULL,
      PRIMARY KEY (ses, idx)
    ) WITHOUT ROWID;`,
    "ALTER TABLE sessions ADD COLUMN bytesSinceLastSignature INTEGER;",
    "PRAGMA user_version = 3;",
  ],
};

export function getSQLiteMigrationQueries(version: number): string[] {
  return Object.keys(migrations)
    .map((k) => Number.parseInt(k, 10))
    .filter((v) => v > version)
    .sort((a, b) => a - b)
    .flatMap((v) => migrations[v] ?? []);
}
