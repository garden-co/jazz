import type { Db } from "jazz-tools";
import { app, type WorkspaceRole } from "@/schema";
import { retryOnConflict } from "./retry";

const WORKSPACE_RANK: Record<WorkspaceRole, number> = { guest: 0, viewer: 1, member: 2, owner: 3 };

export type RedeemResult = { workspaceId: string; pageId: string | null };

/**
 * Turn an invite token into the membership or page grant it describes, for
 * the calling account. Runs on the server with backend authority, because the
 * invitee cannot read the invite (only people who share can). Redeeming twice
 * is a no-op, and a redemption never lowers access someone already has.
 */
export async function redeemInvite(
  db: Db,
  token: string,
  account: string,
  displayName: string,
): Promise<RedeemResult | null> {
  return await retryOnConflict(async () => {
    const write = await db.exclusiveTransaction(async (tx) => {
      const invite = await tx.one(app.invites.where({ token }));
      if (!invite) return null;
      const { workspaceId, pageId } = invite;
      // Deleting a page deletes its invites too; this guards the window in
      // between, so a link never grants access to a page that is gone.
      if (pageId !== null && !(await tx.one(app.pages.where({ id: pageId })))) return null;
      const member = await tx.one(app.members.where({ workspaceId, account }));
      if (pageId === null) {
        const role = invite.role === "member" ? "member" : "viewer";
        if (!member) tx.insert(app.members, { workspaceId, account, displayName, role });
        else if (WORKSPACE_RANK[role] > WORKSPACE_RANK[member.role])
          tx.update(app.members, member.id, { role });
        return { workspaceId, pageId };
      }
      if (!member) tx.insert(app.members, { workspaceId, account, displayName, role: "guest" });
      const role = invite.role === "editor" ? "editor" : "viewer";
      const grant = await tx.one(app.pageGrants.where({ pageId, account }));
      if (!grant) tx.insert(app.pageGrants, { workspaceId, pageId, account, role });
      else if (grant.role === "viewer" && role === "editor")
        tx.update(app.pageGrants, grant.id, { role });
      return { workspaceId, pageId };
    });
    return await write.wait();
  });
}

/** 32 random bytes, URL-safe. Generated in the browser that creates the invite. */
export function createInviteToken(): string {
  const bytes = crypto.getRandomValues(new Uint8Array(32));
  return btoa(String.fromCharCode(...bytes))
    .replaceAll("+", "-")
    .replaceAll("/", "_")
    .replace(/=+$/, "");
}
