import { auth } from "../../../src/lib/auth";
import { ensureProfile } from "../../../src/lib/bootstrap";
export const runtime = "nodejs";
export async function POST(request: Request) {
  const session = await auth.api.getSession({ headers: request.headers });
  if (!session?.user) return Response.json({ error: "sign in required" }, { status: 401 });
  await ensureProfile(session.user.id, session.user.name);
  return Response.json({ ready: true });
}
