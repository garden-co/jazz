import { accountRegistryUrl } from "jazz-tools";
import { resolveRequestSession } from "jazz-tools/backend";
import { auth } from "@/src/lib/auth";
import { backendJazzClient } from "@/src/lib/auth-jazz-client";
import { appOrigin, jazzAppId, jazzServerUrl } from "@/src/lib/config";
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
  const jazzSession = await resolveRequestSession(request, {
    appId: jazzAppId,
    accountRegistry: accountRegistryUrl(jazzServerUrl, jazzAppId),
    jwksUrl: `${appOrigin}/api/auth/jwks`,
    jwtIssuer: appOrigin,
    jwtAudience: appOrigin,
  });
  if (jazzSession.user_id !== session.user.id)
    return Response.json({ error: "session identity mismatch" }, { status: 401 });
  if (!jazzSession.account_id) return Response.json({ error: "account required" }, { status: 401 });

  const body = (await request.json().catch(() => ({}))) as { warehouseId?: unknown };
  const db = (await backendJazzClient()).db;
  await ensureSeed(db);
  if (typeof body.warehouseId !== "string") return Response.json({ ok: true });
  const membership = await ensureOperator(db, {
    accountId: jazzSession.account_id,
    name: session.user.name,
    warehouseId: body.warehouseId,
  });
  return Response.json({ ok: true, ...membership });
}
