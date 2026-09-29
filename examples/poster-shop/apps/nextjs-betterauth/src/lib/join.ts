import { randomUUID } from "node:crypto";
import { app } from "@/schema";
import { authJazzClient } from "@/src/lib/auth-jazz-client";
import { authorForSession } from "@/src/lib/identity";
import { withBoundedConflictRetry } from "@/src/lib/retry";

/**
 * Redeem an invite token for the signed-in account. Runs server-side with
 * backend authority; the exclusive transaction keeps it idempotent, and an
 * existing membership is never downgraded.
 */
export async function redeemInvite(
  accountId: string,
  token: string,
): Promise<{ canvasId: string } | null> {
  const memberAuthor = authorForSession(accountId);
  const db = (await authJazzClient()).db;
  return withBoundedConflictRetry(async () => {
    const write = await db.exclusiveTransaction(async (tx) => {
      const invite = await tx.one(app.canvasInvites.where({ token }));
      if (!invite) return null;
      const existing = await tx.one(
        app.canvasMembers.where({ canvasId: invite.canvasId, memberAuthor }),
      );
      if (!existing)
        tx.insert(
          app.canvasMembers,
          { canvasId: invite.canvasId, memberAuthor, role: invite.role },
          { id: randomUUID() },
        );
      return { canvasId: invite.canvasId };
    });
    return await write.wait();
  });
}
