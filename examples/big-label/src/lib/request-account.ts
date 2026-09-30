import { accountRegistryUrl } from "jazz-tools";
import { isRequestAuthenticationError, resolveRequestSession } from "jazz-tools/backend";

/**
 * Verifies the Better Auth JWT on a request and resolves its Jazz account.
 * Resolves null when the caller's credentials are missing or rejected, so
 * routes answer 401. A failure on the server's side, such as an unreachable
 * JWKS endpoint or account registry, is rethrown and becomes a logged 500.
 */
export async function requestAccount(request: Request) {
  const appId = process.env.NEXT_PUBLIC_JAZZ_APP_ID!;
  const serverUrl = process.env.NEXT_PUBLIC_JAZZ_SERVER_URL!;
  const origin = process.env.NEXT_PUBLIC_APP_ORIGIN ?? "http://127.0.0.1:3000";
  let session;
  try {
    session = await resolveRequestSession(request, {
      appId,
      accountRegistry: accountRegistryUrl(serverUrl, appId),
      jwksUrl: `${origin}/api/auth/jwks`,
      jwtIssuer: origin,
    });
  } catch (error) {
    if (isRequestAuthenticationError(error)) return null;
    throw error;
  }
  if (!session.account_id) return null;
  const { name, email } = session.claims;
  const displayName =
    typeof name === "string" && name ? name : typeof email === "string" && email ? email : null;
  return {
    accountId: session.account_id,
    displayName: displayName ?? session.user_id,
    email: typeof email === "string" && email ? email : null,
  };
}
