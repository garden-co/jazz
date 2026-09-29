import type { Db } from "jazz-tools";
import { app, type FolderRole } from "../schema.js";

export interface Invite {
  folderId: string;
  role: FolderRole;
  code: string;
}

/**
 * Create an invite for a folder the caller owns. The code is a bearer
 * capability: anyone holding the link can join with its role until the owner
 * revokes the invite.
 */
export function createInvite(db: Db, folderId: string, role: FolderRole): Invite {
  const code = crypto.randomUUID();
  db.insert(app.folderInvites, { folder_id: folderId, code, role });
  return { folderId, role, code };
}

/** The code lives in the URL fragment so it stays out of server and referrer logs. */
export function inviteLink(invite: Invite, base = window.location.href): string {
  const url = new URL(base);
  url.hash = `/join/${invite.folderId}/${invite.role}/${invite.code}`;
  return url.toString();
}

export function parseInviteHash(hash: string): Invite | undefined {
  const match = /^#\/join\/([0-9a-f-]{36})\/(viewer|editor)\/([0-9a-f-]{36})$/i.exec(hash);
  if (!match) return undefined;
  return { folderId: match[1]!, role: match[2] as FolderRole, code: match[3]! };
}

/**
 * Join a shared folder. The membership row carries the code and role; the
 * sync server accepts it only when a matching invite exists, so this waits for
 * the global tier before reporting success.
 */
export async function redeemInvite(db: Db, invite: Invite, userId: string): Promise<void> {
  const existing = await db.all(
    app.folderMembers.where({ folder_id: invite.folderId, user_id: userId }),
  );
  if (existing.length > 0) return;
  await db
    .insert(app.folderMembers, {
      folder_id: invite.folderId,
      user_id: userId,
      role: invite.role,
      invite_code: invite.code,
    })
    .wait({ tier: "global" });
}
