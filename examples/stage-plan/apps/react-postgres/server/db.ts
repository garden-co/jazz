// The app's Postgres pool and a transaction helper that collects what changed,
// so the live hub can tell subscribers once the transaction has committed.
import pg from "pg";
import { publish, revoke } from "./live.js";

export const DATABASE_URL =
  process.env.DATABASE_URL ?? "postgres://postgres:pass@127.0.0.1:5432/stage_plan_postgres";
export const pool = new pg.Pool({ connectionString: DATABASE_URL });

export type Db = Pick<pg.PoolClient, "query">;

export async function rows<T>(db: Db, sql: string, params: unknown[] = []): Promise<T[]> {
  return (await db.query(sql, params)).rows as T[];
}
export async function one<T>(db: Db, sql: string, params: unknown[] = []): Promise<T | undefined> {
  return (await rows<T>(db, sql, params))[0];
}

export type Tx = Db & {
  /** Marks tables as changed under a topic; subscribers hear about it after COMMIT. */
  changed(topic: string, ...tables: string[]): void;
  /** Takes an account off a topic after COMMIT (e.g. removed from a show's crew). */
  revoke(account: string, topic: string): void;
};

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
  let result: T;
  try {
    await client.query("BEGIN");
    result = await fn(tx);
    await client.query("COMMIT");
  } catch (error) {
    await client.query("ROLLBACK");
    throw error;
  } finally {
    client.release();
  }
  for (const [topic, tables] of changes) publish(topic, [...tables]);
  for (const [account, topic] of revoked) revoke(account, topic);
  return result;
}
