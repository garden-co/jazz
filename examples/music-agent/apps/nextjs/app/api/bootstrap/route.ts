import { after } from "next/server";
import { runTurn } from "@/src/agent/runner";
import { auth } from "@/src/lib/auth";
import { backendJazzClient } from "@/src/lib/backend-jazz-client";
import { bootstrapWorkspace } from "@/src/server/bootstrap";

export const runtime = "nodejs";

/** First open: find the Jazz account behind the Better Auth user, then seed. */
export async function POST(request: Request) {
  const session = await auth.api.getSession({ headers: request.headers });
  if (!session?.user) return Response.json({ error: "sign in required" }, { status: 401 });
  const account = await (await backendJazzClient()).accountFor(request);
  if (!account) return Response.json({ error: "account required" }, { status: 401 });
  const firstReply = await bootstrapWorkspace(account, session.user.id, session.user.name);
  if (firstReply) after(() => runTurn(firstReply));
  return Response.json({ ok: true });
}
