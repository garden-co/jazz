import { accountRegistryUrl } from "jazz-tools";
import { resolveRequestSession } from "jazz-tools/backend";
import { auth } from "@/src/lib/auth";
import { configuredIssuer } from "@/src/lib/identity";

export type RequestAccount = { account: string; displayName: string };

/**
 * The one auth model: Better Auth owns the browser session and signs a JWT;
 * Jazz maps that JWT's issuer and subject to an account. A server route acts
 * only when both agree on who is calling.
 */
export async function requireRequestAccount(request: Request): Promise<RequestAccount | Response> {
  const session = await auth.api.getSession({ headers: request.headers });
  if (!session?.user) return Response.json({ error: "sign in required" }, { status: 401 });
  const appId = process.env.NEXT_PUBLIC_JAZZ_APP_ID ?? "band-book-local";
  const serverUrl = process.env.NEXT_PUBLIC_JAZZ_SERVER_URL ?? "http://127.0.0.1:4200";
  const jazzSession = await resolveRequestSession(request, {
    appId,
    accountRegistry: accountRegistryUrl(serverUrl, appId),
    jwksUrl: `${configuredIssuer}/api/auth/jwks`,
    jwtIssuer: configuredIssuer,
  });
  if (jazzSession.user_id !== session.user.id)
    return Response.json({ error: "session identity mismatch" }, { status: 401 });
  if (!jazzSession.account_id) return Response.json({ error: "account required" }, { status: 401 });
  return { account: jazzSession.account_id, displayName: session.user.name };
}
