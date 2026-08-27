import { auth } from "@/src/lib/auth";
import { ensurePersonalCanvas } from "@/src/lib/bootstrap";
import { configuredIssuer } from "@/src/lib/identity";

export const runtime = "nodejs";

export async function POST(request: Request) {
  const session = await auth.api.getSession({ headers: request.headers });
  if (!session?.user) return Response.json({ error: "sign in required" }, { status: 401 });
  await ensurePersonalCanvas(configuredIssuer, session.user.id, session.user.name);
  return Response.json({ ok: true });
}
