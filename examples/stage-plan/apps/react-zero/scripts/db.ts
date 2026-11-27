// Local Postgres for this app: (re)creates the database and applies db/*.sql.
import { readdirSync, readFileSync, rmSync } from "node:fs";
import pg from "pg";

export const DATABASE_URL =
  process.env.ZERO_UPSTREAM_DB ?? "postgres://postgres:pass@127.0.0.1:5432/stage_plan_zero";

/** Drops and recreates the database (and zero-cache's replica), then migrates. */
export async function resetDatabase(replicaFile = process.env.ZERO_REPLICA_FILE ?? ".zero/replica.db") {
  const url = new URL(DATABASE_URL);
  const name = url.pathname.slice(1);
  url.pathname = "/postgres";
  const admin = new pg.Client({ connectionString: url.toString() });
  await admin.connect();
  await admin.query(
    "SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE datname = $1 AND pid <> pg_backend_pid()",
    [name],
  );
  // zero-cache's replication slots pin the database; drop them with it.
  await admin.query(
    "SELECT pg_drop_replication_slot(slot_name) FROM pg_replication_slots WHERE database = $1",
    [name],
  );
  await admin.query(`DROP DATABASE IF EXISTS "${name}"`);
  await admin.query(`CREATE DATABASE "${name}"`);
  await admin.end();
  for (const suffix of ["", "-wal", "-shm", "-wal2"]) rmSync(replicaFile + suffix, { force: true });
  await migrate();
}

export async function migrate() {
  const client = new pg.Client({ connectionString: DATABASE_URL });
  await client.connect();
  for (const file of readdirSync("db").filter((f) => f.endsWith(".sql")).sort()) {
    await client.query(readFileSync(`db/${file}`, "utf8"));
  }
  await client.end();
}
