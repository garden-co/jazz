import { accountRegistryUrl } from "jazz-tools";
import { resolveRequestSession } from "jazz-tools/backend";
import { auth } from "./auth";
import { serverConfig } from "./config";
import { CheckoutError } from "./orders";

/**
 * Resolve the signed-in shopper behind a store API request. The request must
 * carry both the Better Auth session cookie and its JWT as a bearer; Jazz
 * verifies the JWT and resolves the account it is assigned to. Guests (local
 * first accounts) browse and fill carts, but orders need a signed-in account.
 */
export async function requireShopper(request: Request): Promise<{ account: string }> {
  const session = await auth.api.getSession({ headers: request.headers });
  if (!session?.user) throw new CheckoutError("Sign in to check out.", 401);
  const jazzSession = await resolveRequestSession(request, {
    appId: serverConfig.appId,
    accountRegistry: accountRegistryUrl(serverConfig.serverUrl, serverConfig.appId),
    jwksUrl: `${serverConfig.origin}/api/auth/jwks`,
    jwtIssuer: serverConfig.origin,
    allowLocalFirstAuth: false,
  }).catch(() => {
    throw new CheckoutError("Your session could not be verified. Sign in again.", 401);
  });
  if (jazzSession.user_id !== session.user.id || !jazzSession.account_id)
    throw new CheckoutError("Your session could not be verified. Sign in again.", 401);
  return { account: jazzSession.account_id };
}

/** Map domain errors to JSON responses without leaking internals. */
export async function jsonRoute(run: () => Promise<unknown>): Promise<Response> {
  try {
    return Response.json(await run());
  } catch (error) {
    if (error instanceof CheckoutError)
      return Response.json({ error: error.message }, { status: error.status });
    console.error("[jamazon]", error);
    return Response.json({ error: "Something went wrong. Try again." }, { status: 500 });
  }
}
