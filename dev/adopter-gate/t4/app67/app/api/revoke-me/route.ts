import { auth, jazz } from "@/jazz.server";

// Eval helper: deletes every Better Auth session of the caller server-side (without
// clearing this browser's cookie) and calls jazz.revokeUser on this instance.
export async function POST(request: Request) {
  const session = await auth.api.getSession({ headers: request.headers });
  if (!session) return new Response("signed out", { status: 401 });
  const userId = session.user.id;
  await auth.api.revokeSessions({ headers: request.headers });
  let revoked: unknown = null;
  try {
    revoked = await jazz.revokeUser(userId);
  } catch (e) {
    revoked = String(e);
  }
  return Response.json({ userId, revokedAt: new Date().toISOString(), revoked: revoked ?? null });
}
