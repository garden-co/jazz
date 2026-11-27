// The app's Neon database: drops BigLabel's tables and applies db/*.sql.
// (The database stays: Realtime and Managed Better Auth are on the branch,
// and the auth users live in its "neon_auth" schema.)
import { readdirSync, readFileSync } from "node:fs";
import pg from "pg";

// Neon URLs say sslmode=require; verify-full keeps the same checks without pg's warning.
export const DATABASE_URL = process.env.DATABASE_URL!.replace("sslmode=require", "sslmode=verify-full");
const TABLES = [
  "release_assignments",
  "release_teams",
  "releases",
  "catalogues",
  "artists",
  "team_assignments",
  "teams",
  "memberships",
  "person_emails",
  "people",
  "organizations",
];

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
