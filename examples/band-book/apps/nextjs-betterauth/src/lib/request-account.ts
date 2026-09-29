import { accountRegistryUrl } from "jazz-tools";
import { resolveRequestSession } from "jazz-tools/backend";
import { appOrigin, jazzAppId, jazzServerUrl, jwtAudience, jwtIssuer } from "@/src/lib/config";

export type RequestAccount = { account: string; displayName: string };

/**
 * The one auth model: Better Auth signs a JWT, Jazz maps its issuer and
 * subject to an account. The browser sends that JWT as a bearer; core verifies
 * it against Better Auth's JWKS and the account registry.
 *
 * The bootstrap and invite routes write with backend authority on the caller's
 * behalf, so they need the caller's account id itself. `client.forRequest()`
 * verifies the same bearer but returns a scoped Db that does not expose the
 * account it resolved, so these routes use core's `resolveRequestSession`.
 */
export async function requireRequestAccount(request: Request): Promise<RequestAccount | Response> {
  let session;
  try {
    session = await resolveRequestSession(request, {
      appId: jazzAppId,
      accountRegistry: accountRegistryUrl(jazzServerUrl, jazzAppId),
      jwksUrl: `${appOrigin}/api/auth/jwks`,
      jwtIssuer,
      jwtAudience,
    });
  } catch {
    return Response.json({ error: "sign in required" }, { status: 401 });
  }
  if (!session.account_id) return Response.json({ error: "account required" }, { status: 401 });
  const name = session.claims?.name;
  return {
    account: session.account_id,
    displayName: typeof name === "string" && name.trim() ? name : "Band member",
  };
}
