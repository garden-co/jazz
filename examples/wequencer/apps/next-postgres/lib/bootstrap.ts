import { createHash } from "node:crypto";
import { CHANNEL, pool, type Changes } from "@/lib/db.server";

/** Provision the signed-in profile outside the application's read path (idempotent). */
export async function ensureProfile(author: string, displayName: string) {
  const { rows } = await pool.query<{ id: string }>(
    `INSERT INTO profiles (id, author, "displayName") VALUES ($1, $2, $3)
     ON CONFLICT (author) DO UPDATE SET "displayName" = EXCLUDED."displayName"
     RETURNING id`,
    [profileId(author), author, displayName],
  );
  const changed: Changes = { changes: [[`me:${author}`, ["profiles"]]], revoke: [] };
  await pool.query("SELECT pg_notify($1, $2)", [CHANNEL, JSON.stringify(changed)]);
  return rows[0]!;
}

function profileId(author: string): string {
  const hex = createHash("sha256").update(author).digest("hex");
  return `${hex.slice(0, 8)}-${hex.slice(8, 12)}-5${hex.slice(13, 16)}-a${hex.slice(17, 20)}-${hex.slice(20, 32)}`;
}
