import { useDb } from "jazz-tools/react";
import { app } from "../schema.js";
import type { FolderIndex } from "./folders.js";
import { useAdvice, type Advice } from "./use-advice.js";

type ItemKind = "file" | "folder";
type Membership = { folder_id: string; role: string };

// ---------------------------------------------------------------------------
// Fallback for "unknown" advice. TEMPORARY until
// https://github.com/garden-co/jazz/issues/3769 is fixed.
//
// Jazz answers "unknown" for any check whose policy uses bounded recursive
// `allowedTo` (as every folder rule here does), so `canInsert(folders)` is
// "unknown" even for the folder's owner. Treating that as "denied" would hide
// Upload and New folder from owners, so for "unknown" only, the UI uses the
// hints below. Pending answers never use them: an action appears once Jazz
// has answered. The sync server decides every write either way, and a
// rejected one shows up through `db.onMutationError`.
// ---------------------------------------------------------------------------

/** Whether to offer an action, given Jazz's answer and the fallback for "unknown". */
export function offerAction(answer: Advice | undefined, fallbackForUnknown: () => boolean) {
  if (answer === "allowed") return true;
  if (answer === "unknown") return fallbackForUnknown();
  return false; // "denied", or still pending
}

/** Fallback hints for "unknown" advice; see the note above. */
export function unknownAdviceFallback(
  index: FolderIndex,
  userId: string | undefined,
  memberships: readonly Membership[],
) {
  const editsInside = (id: string | undefined) =>
    index.isInOwnTree(id) ||
    index
      .path(id)
      .some((f) => memberships.some((m) => m.folder_id === f.id && m.role === "editor"));
  return {
    editsInside,
    /** A folder is deleted by its owner or by someone who edits its parent. */
    deletesFolder: (id: string) => {
      const folder = index.byId.get(id);
      return !!folder && (index.isMine(folder) || editsInside(folder.parent_id ?? undefined));
    },
    shares: (id: string) => {
      const folder = index.byId.get(id);
      return !!folder && index.isMine(folder);
    },
    /** A folder moves by its owner; a file by its uploader or the tree's owner. */
    moves: (kind: ItemKind, ownerId: string, containingFolderId: string | undefined) => {
      const containing = containingFolderId ? index.byId.get(containingFolderId) : undefined;
      return ownerId === userId || (kind === "file" && !!containing && index.isMine(containing));
    },
  };
}

/** Move destinations for "unknown" advice: the caller's own folders, or the top level for a folder they own. */
export function fallbackMovesInto(
  index: FolderIndex,
  item: { kind: ItemKind; id: string },
  target: string | null,
) {
  const folder = index.byId.get(target ?? item.id);
  return !!folder && index.isMine(folder) && (target !== null || item.kind === "folder");
}

// ---------------------------------------------------------------------------

/** Asks Jazz whether `item` may move into `target` (`null` is the top level). */
export function checkMove(
  db: ReturnType<typeof useDb>,
  item: { kind: ItemKind; id: string },
  target: string | null,
): Promise<Advice> {
  if (item.kind === "folder") return db.canUpdate(app.folders, item.id, { parent_id: target });
  return target === null
    ? Promise.resolve("denied")
    : db.canUpdate(app.files, item.id, { folder_id: target });
}

interface BrowserAdviceInput {
  index: FolderIndex;
  userId: string | undefined;
  /** The open folder. */
  folderId: string | undefined;
  /** The rows in the open folder. */
  entries: readonly { kind: ItemKind; id: string; ownerId: string }[];
  /** The caller's own memberships, for the "unknown" fallback. */
  memberships: readonly Membership[];
  /** Changes whenever folders or memberships change, which can change the answers. */
  revision: string;
}

/** What the current user may do in the browser, as Jazz answers it. */
export function useBrowserAdvice({
  index,
  userId,
  folderId,
  entries,
  memberships,
  revision,
}: BrowserAdviceInput) {
  const db = useDb();
  const fallback = unknownAdviceFallback(index, userId, memberships);
  const checks: Record<string, () => Promise<Advice>> = {};

  if (userId) {
    // Every folder in the side tree is a drop target, so each needs an upload
    // answer. Answers are cached until folders or memberships change, so this
    // is one check per folder per change, not per render.
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
    const open = folderId ? index.byId.get(folderId) : undefined;
    if (open) {
      // A subfolder belongs to the owner of its tree.
      checks[`subfolder:${open.id}`] = () =>
        db.canInsert(app.folders, {
          name: "New folder",
          owner_id: open.owner_id,
          parent_id: open.id,
        });
    }
  }

  // Move is offered when Jazz allows moving the item to one likely place: a
  // folder with a parent to the top level, anything else into the first of the
  // caller's own folders it could go to. The Move dialog then checks each
  // destination.
  const probeTarget = (kind: ItemKind, id: string): string | null | undefined => {
    const folder = kind === "folder" ? index.byId.get(id) : undefined;
    if (folder?.parent_id) return null;
    return index
      .moveCandidates(kind === "folder" ? id : undefined)
      .find((candidate) => candidate.id !== folderId && index.isMine(candidate))?.id;
  };

  const items = [
    ...entries,
    ...(folderId ? [{ kind: "folder" as const, id: folderId, ownerId: "" }] : []),
  ];
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
    const target = probeTarget(kind, id);
    if (target !== undefined)
      checks[`move:${kind}:${id}`] = () => checkMove(db, { kind, id }, target);
  }

  const advice = useAdvice(checks, revision);
  const containing = (kind: ItemKind, id: string) => (kind === "folder" ? id : folderId);

  return {
    canUpload: (id: string) => offerAction(advice[`upload:${id}`], () => fallback.editsInside(id)),
    canCreateSubfolder: (id: string) =>
      offerAction(advice[`subfolder:${id}`], () => fallback.editsInside(id)),
    canEdit: (kind: ItemKind, id: string) =>
      offerAction(advice[`edit:${kind}:${id}`], () => fallback.editsInside(containing(kind, id))),
    canDelete: (kind: ItemKind, id: string) =>
      offerAction(advice[`delete:${kind}:${id}`], () =>
        kind === "file" ? fallback.editsInside(folderId) : fallback.deletesFolder(id),
      ),
    canMove: (kind: ItemKind, id: string, ownerId: string) =>
      offerAction(advice[`move:${kind}:${id}`], () => fallback.moves(kind, ownerId, folderId)),
    canShare: (id: string) => offerAction(advice[`share:${id}`], () => fallback.shares(id)),
  };
}

export type BrowserAdvice = ReturnType<typeof useBrowserAdvice>;
