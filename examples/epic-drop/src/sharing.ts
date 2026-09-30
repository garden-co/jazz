import type { Db } from "jazz-tools";
import { app, type FolderRole } from "../schema.js";

export interface Invite {
  folderId: string;
  /** The folder's owner. The joiner cannot read the folder yet, so the link carries it. */
  ownerId: string;
  role: FolderRole;
  code: string;
}

/**
 * Create an invite for a folder the caller owns. The code is a bearer
 * capability: anyone holding the link can join with its role until the owner
 * revokes the invite. Wait on `write` before handing the link out, so it
 * works as soon as someone opens it.
 */
export function createInvite(db: Db, folder: { id: string; owner_id: string }, role: FolderRole) {
  const code = crypto.randomUUID();
  const write = db.insert(app.folderInvites, { folder_id: folder.id, code, role });
  const invite: Invite = { folderId: folder.id, ownerId: folder.owner_id, role, code };
  return { invite, write };
}

/** The code lives in the URL fragment so it stays out of server and referrer logs. */
export function inviteLink(invite: Invite, base = window.location.href): string {
  const url = new URL(base);
  url.hash = `/join/${invite.folderId}/${invite.ownerId}/${invite.role}/${invite.code}`;
  return url.toString();
}

const UUID = "[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}";
const INVITE_HASH = new RegExp(`^#/join/(${UUID})/(${UUID})/(viewer|editor)/(${UUID})$`, "i");

export function parseInviteHash(hash: string): Invite | undefined {
  const match = INVITE_HASH.exec(hash);
  if (!match) return undefined;
  return {
    folderId: match[1]!,
    ownerId: match[2]!,
    role: match[3] as FolderRole,
    code: match[4]!,
  };
}

/**
 * Join a shared folder. The membership row carries the code, role and folder
 * owner; the sync server accepts it only when they match a live invite, so
 * this waits for the global tier before reporting success.
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
      folder_owner_id: invite.ownerId,
    })
    .wait({ tier: "global" });
}
