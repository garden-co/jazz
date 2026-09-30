import { accountRegistryUrl } from "jazz-tools";
import { resolveRequestSession } from "jazz-tools/backend";
import { ensureProfile } from "@/lib/bootstrap";
import { APP_ORIGIN } from "@/lib/app-origin";

export const runtime = "nodejs";

export async function POST(request: Request) {
  const appId = process.env.NEXT_PUBLIC_JAZZ_APP_ID!;
  const serverUrl = process.env.NEXT_PUBLIC_JAZZ_SERVER_URL!;
  const session = await resolveRequestSession(request, {
    appId,
    accountRegistry: accountRegistryUrl(serverUrl, appId),
    jwksUrl: `${APP_ORIGIN}/api/auth/jwks`,
    jwtIssuer: APP_ORIGIN,
    jwtAudience: APP_ORIGIN,
  });
  if (!session.account_id) return Response.json({ error: "account required" }, { status: 401 });
  // Better Auth's JWT carries the user's name; it becomes the display name
  // bandmates see beside the presence avatars.
  const name = (session.claims as Record<string, unknown> | undefined)?.name;
  const profile = await ensureProfile(
    session.account_id,
    typeof name === "string" && name.trim() ? name.trim() : "Bandmate",
  );
  return Response.json({ profileId: profile.id });
}
