import { auth } from "../../../src/lib/auth";
import { ensureAccountBootstrap } from "../../../src/lib/bootstrap";

export const runtime = "nodejs";

export async function POST(request: Request) {
  const session = await auth.api.getSession({ headers: request.headers });
  if (!session?.user) return Response.json({ error: "sign in required" }, { status: 401 });
  await ensureAccountBootstrap(session.user.id);
  return Response.json({ subject: session.user.id });
}
