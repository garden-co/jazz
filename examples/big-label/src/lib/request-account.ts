import { accountRegistryUrl } from "jazz-tools";
import { resolveRequestSession } from "jazz-tools/backend";

/** Verifies the Better Auth JWT on a request and resolves its Jazz account. */
export async function requestAccount(request: Request) {
  const appId = process.env.NEXT_PUBLIC_JAZZ_APP_ID!;
  const serverUrl = process.env.NEXT_PUBLIC_JAZZ_SERVER_URL!;
  const origin = process.env.NEXT_PUBLIC_APP_ORIGIN ?? "http://127.0.0.1:3000";
  const session = await resolveRequestSession(request, {
    appId,
    accountRegistry: accountRegistryUrl(serverUrl, appId),
    jwksUrl: `${origin}/api/auth/jwks`,
    jwtIssuer: origin,
  });
  if (!session.account_id) return null;
  const { name, email } = session.claims;
  const displayName =
    typeof name === "string" && name ? name : typeof email === "string" && email ? email : null;
  return { accountId: session.account_id, displayName: displayName ?? session.user_id };
}
