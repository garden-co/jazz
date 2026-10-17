import { randomUUID } from "node:crypto";
import type { Db } from "jazz-tools";
import { app } from "@/schema";
import type { InviteLink } from "@/src/lib/account-enrollment";
import { authorForSession } from "@/src/lib/identity";
import { withBoundedConflictRetry } from "@/src/lib/retry";

export type RedeemResult = "joined" | "already-member" | "invalid";

/**
 * Redeem an invite for the signed-in account, following the invite-links
 * recipe. Runs server-side on a backend-authority `db`. One exclusive
 * transaction reads the private invite, checks for an existing membership,
 * inserts the new one and consumes a single-use invite, so two simultaneous
 * redeems of a single-use link cannot both succeed. Checking membership first
 * keeps re-opening a link idempotent, and an existing membership is never
 * changed, so an admin who opens a viewer link stays an admin.
 */
export async function redeemInvite(
  db: Db,
  accountId: string,
  { canvasId, token }: InviteLink,
): Promise<RedeemResult> {
  const memberAuthor = authorForSession(accountId);
  return await withBoundedConflictRetry(async () => {
    const write = await db.exclusiveTransaction(async (tx): Promise<RedeemResult> => {
      const existing = await tx.one(app.canvasMembers.where({ canvasId, memberAuthor }));
      if (existing) return "already-member";
      const invite = await tx.one(app.canvasInvites.where({ canvasId, token }));
      if (!invite) return "invalid";
      tx.insert(
        app.canvasMembers,
        { canvasId, memberAuthor, role: invite.role },
        { id: randomUUID() },
      );
      if (invite.singleUse) tx.delete(app.canvasInvites, invite.id);
      return "joined";
    });
    await write.wait();
    return write.value;
  });
}
