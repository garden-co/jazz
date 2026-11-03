import { auth } from "@/lib/auth";
import { ensureProfile } from "@/lib/bootstrap";
import { jazz } from "@/lib/jazz.server";

export const runtime = "nodejs";

export async function POST(request: Request) {
  // The Better Auth session cookie names the caller (see `authenticate` in
  // lib/jazz.server.ts); a signed-out request has no account.
  const accountId = await (await jazz()).accountFor(request);
  if (!accountId) return Response.json({ error: "account required" }, { status: 401 });
  // The Better Auth display name becomes the name bandmates see beside the
  // presence avatars.
  const session = await auth.api.getSession({ headers: request.headers });
  const name = session?.user.name.trim();
  const profile = await ensureProfile(accountId, name || "Bandmate");
  return Response.json({ profileId: profile.id });
}
