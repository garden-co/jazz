// Local Postgres for this app: (re)creates the database, Better Auth's tables
// (its own migrations, in the "auth" schema) and Wequencer's (db/*.sql).
import { readdirSync, readFileSync } from "node:fs";
import { getMigrations } from "better-auth/db/migration";
import pg from "pg";

export const DATABASE_URL =
  process.env.DATABASE_URL ?? "postgres://postgres:pass@127.0.0.1:5432/wequencer_postgres";

/** Drops and recreates the database, then migrates. */
export async function resetDatabase() {
  const url = new URL(DATABASE_URL);
  const name = url.pathname.slice(1);
  url.pathname = "/postgres";
  const admin = new pg.Client({ connectionString: url.toString() });
  await admin.connect();
  await admin.query(`DROP DATABASE IF EXISTS "${name}" WITH (FORCE)`);
  await admin.query(`CREATE DATABASE "${name}"`);
  await admin.end();
  await migrate();
}

export async function migrate() {
  const client = new pg.Client({ connectionString: DATABASE_URL });
  await client.connect();
  await client.query('CREATE SCHEMA IF NOT EXISTS "auth"');
  for (const file of readdirSync("db").filter((f) => f.endsWith(".sql")).sort()) {
    await client.query(readFileSync(`db/${file}`, "utf8"));
  }
  await client.end();
  const { authOptions } = await import("../lib/auth");
  await (await getMigrations(authOptions)).runMigrations();
}
