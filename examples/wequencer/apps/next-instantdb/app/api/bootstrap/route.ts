import { ensureProfile } from "@/lib/bootstrap";
import { adminDb } from "@/lib/instant.server";

export const runtime = "nodejs";

export async function POST(request: Request) {
  // The caller's Instant token names them; a signed-out request has no account.
  const token = request.headers.get("authorization")?.replace(/^Bearer /, "");
  const user = token ? await adminDb.auth.verifyToken(token).catch(() => null) : null;
  if (!user) return Response.json({ error: "account required" }, { status: 401 });
  // The sign-up name becomes the name bandmates see beside the presence avatars.
  const { $users } = await adminDb.query({ $users: { $: { where: { id: user.id } } } });
  const name = $users[0]?.name?.trim();
  const profile = await ensureProfile(user.id, name || "Bandmate");
  return Response.json({ profileId: profile.id });
}
