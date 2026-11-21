// The app's Neon database: drops StagePlan's tables and applies db/*.sql.
// (The database itself stays: Realtime is enabled on the project's branch.)
import { readdirSync, readFileSync } from "node:fs";
import pg from "pg";

// Neon URLs say sslmode=require; verify-full keeps the same checks without pg's warning.
export const DATABASE_URL = process.env.DATABASE_URL!.replace("sslmode=require", "sslmode=verify-full");
const TABLES = ["activity", "comments", "tasks", "showInvites", "showCrew", "shows", "crew", "checklistItems"];

export async function resetDatabase() {
  const client = new pg.Client({ connectionString: DATABASE_URL });
  await client.connect();
  await client.query(TABLES.map((table) => `DROP TABLE IF EXISTS "${table}" CASCADE;`).join("\n"));
  await client.end();
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
