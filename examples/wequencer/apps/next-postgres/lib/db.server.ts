import pg from "pg";

export const DATABASE_URL =
  process.env.DATABASE_URL ?? "postgres://postgres:pass@127.0.0.1:5432/wequencer_postgres";

/** Wequencer's tables (public schema), for the API routes and the profile bootstrap. */
export const pool = new pg.Pool({ connectionString: DATABASE_URL });

export type Db = Pick<pg.PoolClient, "query">;
export const CHANNEL = "wequencer_changes";

export async function rows<T>(db: Db, sql: string, params: unknown[] = []): Promise<T[]> {
  return (await db.query(sql, params)).rows as T[];
}
export async function one<T>(db: Db, sql: string, params: unknown[] = []): Promise<T | undefined> {
  return (await rows<T>(db, sql, params))[0];
}

/** What a committed write changed, as `NOTIFY`'d to the live server (lib/live.server.ts). */
export type Changes = { changes: [topic: string, tables: string[]][]; revoke: [account: string, topic: string][] };

export type Tx = Db & {
  /** Marks tables as changed under a topic; subscribers hear about it after COMMIT. */
  changed(topic: string, ...tables: string[]): void;
  /** Takes an account off a topic after COMMIT (e.g. removed from a session). */
  revoke(account: string, topic: string): void;
};

/**
 * Runs `fn` in a transaction. What it changed is sent with `pg_notify` inside
 * the same transaction, so Postgres delivers it only if the write commits.
 */
export async function transaction<T>(fn: (tx: Tx) => Promise<T>): Promise<T> {
  const client = await pool.connect();
  const changes = new Map<string, Set<string>>();
  const revoked: [string, string][] = [];
  const tx: Tx = Object.assign(client, {
    changed(topic: string, ...tables: string[]) {
      const set = changes.get(topic) ?? new Set();
      for (const table of tables) set.add(table);
      changes.set(topic, set);
    },
    revoke(account: string, topic: string) {
      revoked.push([account, topic]);
    },
  });
  try {
    await client.query("BEGIN");
    const result = await fn(tx);
    const payload: Changes = { changes: [...changes].map(([t, s]) => [t, [...s]]), revoke: revoked };
    if (payload.changes.length || payload.revoke.length)
      await client.query("SELECT pg_notify($1, $2)", [CHANNEL, JSON.stringify(payload)]);
    await client.query("COMMIT");
    return result;
  } catch (error) {
    await client.query("ROLLBACK");
    throw error;
  } finally {
    client.release();
  }
}
