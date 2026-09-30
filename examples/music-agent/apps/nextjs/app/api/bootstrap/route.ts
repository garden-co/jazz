import { after } from "next/server";
import { accountRegistryUrl } from "jazz-tools";
import { resolveRequestSession } from "@/src/lib/backend-jazz-client";
import { runTurn } from "@/src/agent/runner";
import { appOrigin } from "@/src/lib/app-origin";
import { jazzAppId, jazzServerUrl } from "@/src/lib/jazz-env";
import { auth } from "@/src/lib/auth";
import { bootstrapWorkspace } from "@/src/server/bootstrap";

export const runtime = "nodejs";

/** First open: verify the Jazz account behind the Better Auth user, then seed. */
export async function POST(request: Request) {
  const session = await auth.api.getSession({ headers: request.headers });
  if (!session?.user) return Response.json({ error: "sign in required" }, { status: 401 });
  const jazzSession = await resolveRequestSession(request, {
    appId: jazzAppId,
    accountRegistry: accountRegistryUrl(jazzServerUrl, jazzAppId),
    jwksUrl: `${appOrigin}/api/auth/jwks`,
    jwtIssuer: appOrigin,
    jwtAudience: appOrigin,
  });
  if (jazzSession.user_id !== session.user.id)
    return Response.json({ error: "session identity mismatch" }, { status: 401 });
  if (!jazzSession.account_id) return Response.json({ error: "account required" }, { status: 401 });
  const firstReply = await bootstrapWorkspace(
    jazzSession.account_id,
    session.user.id,
    session.user.name,
  );
  if (firstReply) after(() => runTurn(firstReply));
  return Response.json({ ok: true });
}
