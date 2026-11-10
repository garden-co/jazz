import { auth } from "@/src/lib/auth";
import { jazz } from "@/src/lib/jazz.server";
import { ensureOperator, ensureSeed } from "@/src/seed";

export const runtime = "nodejs";

/**
 * The only privileged step: seed the demo warehouses once, and staff the
 * signed-in operator on the warehouse they chose. It runs server-side with
 * backend authority, never from a query hook or with a browser-held secret,
 * and both halves are idempotent, so the console may call it on every open.
 */
export async function POST(request: Request) {
  const session = await auth.api.getSession({ headers: request.headers });
  if (!session?.user) return Response.json({ error: "sign in required" }, { status: 401 });
  // The same session cookie names the operator's Jazz account.
  const backend = await jazz();
  const accountId = await backend.accountFor(request);
  if (!accountId) return Response.json({ error: "account required" }, { status: 401 });

  const body = (await request.json().catch(() => ({}))) as { warehouseId?: unknown };
  const db = backend.db;
  await ensureSeed(db);
  if (typeof body.warehouseId !== "string") return Response.json({ ok: true });
  const membership = await ensureOperator(db, {
    accountId,
    name: session.user.name,
    warehouseId: body.warehouseId,
  });
  return Response.json({ ok: true, ...membership });
}
