import { accountRegistryUrl } from "jazz-tools";
import { resolveRequestSession } from "jazz-tools/backend";
import { ensureProfile } from "@/lib/bootstrap";

export const runtime = "nodejs";

export async function POST(request: Request) {
  const appId = process.env.NEXT_PUBLIC_JAZZ_APP_ID!;
  const serverUrl = process.env.NEXT_PUBLIC_JAZZ_SERVER_URL!;
  const origin = process.env.NEXT_PUBLIC_APP_ORIGIN ?? "http://127.0.0.1:3000";
  const session = await resolveRequestSession(request, {
    appId,
    accountRegistry: accountRegistryUrl(serverUrl, appId),
    jwksUrl: `${origin}/api/auth/jwks`,
    jwtIssuer: origin,
  });
  if (!session.account_id) return Response.json({ error: "account required" }, { status: 401 });
  const profile = await ensureProfile(session.account_id, session.user_id);
  return Response.json({ profileId: profile.id });
}
