import { createRealtime } from "@neon/realtime/server";
import { getUser } from "../../../src/lib/auth";
import { DATABASE_URL } from "../../../src/lib/db.server";
import { liveQueries } from "../../../src/lib/live-queries.server";

export const runtime = "nodejs";
const noStore = { "Cache-Control": "private, no-store" };

let realtime: ReturnType<typeof createRealtime> | undefined;
const getRealtime = () =>
  (realtime ??= createRealtime({ secret: process.env.NEON_REALTIME_SECRET!, db: new URL(DATABASE_URL).pathname.slice(1) }));

// Seals a named live query for the signed-in user. Sealing (and renewal) is a
// read, so it's a Route Handler: Next runs Server Actions one at a time.
export async function POST(request: Request) {
  const user = await getUser();
  if (!user) return Response.json({ error: "UNAUTHENTICATED" }, { status: 401, headers: noStore });
  const { name, args } = await request.json();
  const build = liveQueries[name];
  if (!build) return Response.json({ error: `No query ${name}` }, { status: 404, headers: noStore });
  try {
    return Response.json(await getRealtime().seal({ query: build(user.id, args) }), { headers: noStore });
  } catch (error) {
    return Response.json({ error: String(error) }, { status: 400, headers: noStore });
  }
}
