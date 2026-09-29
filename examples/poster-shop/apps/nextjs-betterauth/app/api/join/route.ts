import { redeemInvite } from "@/src/lib/join";
import { verifiedRequest } from "@/src/lib/request-account";
import { BootstrapConflictError } from "@/src/lib/retry";

export const runtime = "nodejs";

const UUID = /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/;

/** The invite arrives in the body; links carry it in the URL fragment only. */
export async function POST(request: Request) {
  const caller = await verifiedRequest(request);
  if (!caller) return Response.json({ error: "account required" }, { status: 401 });
  const body = (await request.json().catch(() => null)) as {
    canvasId?: unknown;
    token?: unknown;
  } | null;
  const canvasId = typeof body?.canvasId === "string" ? body.canvasId : "";
  const token = typeof body?.token === "string" ? body.token : "";
  if (!UUID.test(canvasId) || !UUID.test(token))
    return Response.json({ error: "invalid invite" }, { status: 400 });
  try {
    const result = await redeemInvite(caller.db, caller.accountId, { canvasId, token });
    if (result === "invalid") return Response.json({ error: "invite not found" }, { status: 404 });
    return Response.json({ ok: true, canvasId, result });
  } catch (error) {
    if (error instanceof BootstrapConflictError)
      return Response.json({ error: "busy, retry" }, { status: 503 });
    throw error;
  }
}
