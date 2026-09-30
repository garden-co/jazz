import { accountRegistryUrl } from "jazz-tools";
import { resolveRequestSession } from "jazz-tools/backend";

/**
 * Verifies the Better Auth JWT on a request and resolves its Jazz account.
 * Resolves null when the request carries no bearer token Jazz accepts, so
 * routes answer 401 rather than failing with a 500.
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
  } catch {
    return null;
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
