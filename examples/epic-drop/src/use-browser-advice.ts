import { useDb } from "jazz-tools/react";
import { app } from "../schema.js";
import type { FolderIndex } from "./folders.js";
import { offered, useAdvice, type Advice } from "./use-advice.js";

type ItemKind = "file" | "folder";

interface BrowserAdviceInput {
  index: FolderIndex;
  userId: string | undefined;
  /** The open folder. */
  folderId: string | undefined;
  /** The rows in the open folder. */
  entries: readonly { kind: ItemKind; id: string }[];
  /** Changes whenever folders or memberships change, which can change the answers. */
  revision: string;
}

/**
 * What the current user may do in the browser, as Jazz answers it. While an
 * answer is pending, the user's own folders count as editable so the app
 * stays usable offline; anything else waits for the answer.
 */
export function useBrowserAdvice({
  index,
  userId,
  folderId,
  entries,
  revision,
}: BrowserAdviceInput) {
  const db = useDb();
  const checks: Record<string, () => Promise<Advice>> = {};

  if (userId) {
    for (const folder of index.byId.values()) {
      checks[`upload:${folder.id}`] = () =>
        db.canInsert(app.files, {
          folder_id: folder.id,
          name: "upload",
          content_type: "application/octet-stream",
          size_bytes: 0,
          owner_id: userId,
          contents: new Uint8Array(),
        });
    }
    if (folderId) {
      checks[`subfolder:${folderId}`] = () =>
        db.canInsert(app.folders, { name: "New folder", owner_id: userId, parent_id: folderId });
    }
  }

  const items = [...entries, ...(folderId ? [{ kind: "folder" as const, id: folderId }] : [])];
  for (const { kind, id } of items) {
    if (kind === "file") {
      checks[`edit:file:${id}`] = () => db.canUpdate(app.files, id, { name: "renamed" });
      checks[`delete:file:${id}`] = () => db.canDelete(app.files, id);
    } else {
      checks[`edit:folder:${id}`] = () => db.canUpdate(app.folders, id, { name: "renamed" });
      checks[`delete:folder:${id}`] = () => db.canDelete(app.folders, id);
      checks[`share:${id}`] = () =>
        db.canInsert(app.folderInvites, { folder_id: id, code: "check", role: "viewer" });
    }
  }

  const advice = useAdvice(checks, revision);
  const may = (key: string, ownFolderId: string | undefined) => {
    const answer = advice[key];
    return answer === undefined ? index.isInOwnTree(ownFolderId) : offered(answer);
  };
  const containing = (kind: ItemKind, id: string) => (kind === "folder" ? id : folderId);

  return {
    canUpload: (id: string) => may(`upload:${id}`, id),
    canCreateSubfolder: (id: string) => may(`subfolder:${id}`, id),
    canEdit: (kind: ItemKind, id: string) => may(`edit:${kind}:${id}`, containing(kind, id)),
    canDelete: (kind: ItemKind, id: string) => may(`delete:${kind}:${id}`, containing(kind, id)),
    canShare: (id: string) => {
      const answer = advice[`share:${id}`];
      const folder = index.byId.get(id);
      return answer === undefined ? !!folder && index.isMine(folder) : offered(answer);
    },
  };
}

export type BrowserAdvice = ReturnType<typeof useBrowserAdvice>;
