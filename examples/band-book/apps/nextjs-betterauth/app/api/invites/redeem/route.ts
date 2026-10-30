import { authJazzClient } from "@/src/lib/auth-jazz-client";
import { redeemInvite } from "@/src/lib/invites";
import { requireRequestAccount } from "@/src/lib/request-account";

export const runtime = "nodejs";

export async function POST(request: Request) {
  const caller = await requireRequestAccount(request);
  if (caller instanceof Response) return caller;
  const body = (await request.json().catch(() => null)) as { token?: unknown } | null;
  if (typeof body?.token !== "string" || body.token.length < 16)
    return Response.json({ error: "invalid invite" }, { status: 400 });
  const db = (await authJazzClient()).db;
  const result = await redeemInvite(db, body.token, caller.account, caller.displayName);
  if (!result) return Response.json({ error: "invite not found" }, { status: 404 });
  return Response.json(result);
}
