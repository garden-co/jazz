import { accountRegistryUrl } from "jazz-tools";
import { resolveRequestSession } from "jazz-tools/backend";
import { auth } from "@/src/lib/auth";
import { redeemInvite } from "@/src/lib/join";
import { BootstrapConflictError } from "@/src/lib/retry";

export const runtime = "nodejs";

export async function POST(request: Request) {
  const session = await auth.api.getSession({ headers: request.headers });
  if (!session?.user) return Response.json({ error: "sign in required" }, { status: 401 });
  const body = (await request.json().catch(() => null)) as { token?: unknown } | null;
  const token = typeof body?.token === "string" ? body.token : "";
  if (!/^[0-9a-f-]{36}$/.test(token))
    return Response.json({ error: "invalid invite" }, { status: 400 });
  const appId = process.env.NEXT_PUBLIC_JAZZ_APP_ID!;
  const serverUrl = process.env.NEXT_PUBLIC_JAZZ_SERVER_URL!;
  const origin = process.env.NEXT_PUBLIC_APP_ORIGIN ?? "http://127.0.0.1:3000";
  const jazzSession = await resolveRequestSession(request, {
    appId,
    accountRegistry: accountRegistryUrl(serverUrl, appId),
    jwksUrl: `${origin}/api/auth/jwks`,
    jwtIssuer: origin,
  });
  if (jazzSession.user_id !== session.user.id)
    return Response.json({ error: "session identity mismatch" }, { status: 401 });
  if (!jazzSession.account_id) return Response.json({ error: "account required" }, { status: 401 });
  try {
    const joined = await redeemInvite(jazzSession.account_id, token);
    if (!joined) return Response.json({ error: "invite not found" }, { status: 404 });
    return Response.json({ ok: true, canvasId: joined.canvasId });
  } catch (error) {
    if (error instanceof BootstrapConflictError)
      return Response.json({ error: "busy, retry" }, { status: 503 });
    throw error;
  }
}
