import type { Jazz } from "jazz-tools/backend";
import { app } from "./recipes/invite/jazz.generated.js";

declare const jazz: Jazz;

// #region invite-redeem-route
export async function POST(req: Request): Promise<Response> {
  const user = await jazz.accountFor(req);
  if (!user) return new Response("Account required", { status: 401 });

  const { chatId, code } = (await req.json()) as { chatId: string; code: string };

  // Preserve the verified caller as author while using backend permissions.
  const backendDb = await jazz.withAttribution(req);
  const result = await backendDb.serializableTransaction(async (tx) => {
    // Checking membership first keeps re-opening a successfully redeemed link idempotent,
    // even after a single-use invite has been consumed.
    const existing = await tx.one(app.chatMembers.where({ chatId, user_id: user }));
    if (existing) return "already-member" as const;

    const invite = await tx.one(app.chatInvites.where({ chatId, code }));
    if (!invite) return "invalid" as const;

    tx.insert(app.chatMembers, { chatId, user_id: user, inviteId: invite.id });
    if (invite.singleUse) tx.delete(app.chatInvites, invite.id);
    return "joined" as const;
  });

  // Serializable transactions settle at the authority, so wait() takes no tier.
  await result.wait();
  if (result.value === "invalid") return new Response("invalid invite", { status: 400 });

  return Response.json({ ok: true });
}
// #endregion invite-redeem-route
