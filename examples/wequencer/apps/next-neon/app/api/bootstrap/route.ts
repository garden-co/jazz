import { getAuth } from "@/lib/auth";
import { ensureProfile } from "@/lib/bootstrap";

export const runtime = "nodejs";

export async function POST() {
  // The Managed Better Auth session cookie names the caller.
  const { data: session } = await getAuth().getSession();
  if (!session?.user) return Response.json({ error: "account required" }, { status: 401 });
  // The Better Auth display name becomes the name bandmates see beside the
  // presence avatars.
  const name = session.user.name.trim();
  const profile = await ensureProfile(session.user.id, name || "Bandmate");
  return Response.json({ profileId: profile.id });
}
