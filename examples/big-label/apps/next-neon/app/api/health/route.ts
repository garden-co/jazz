import { pool } from "../../../src/lib/db.server";

export const runtime = "nodejs";
export const dynamic = "force-dynamic";

// The harness's rtt: one round trip to the Neon database through the Next server.
export async function GET() {
  return Response.json((await pool.query("SELECT 1 AS ok")).rows[0]);
}
