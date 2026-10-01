import { useDb } from "jazz-tools/react";
import { AlertDialog } from "@astryxdesign/core/AlertDialog";
import { useToast } from "@astryxdesign/core/Toast";
import { app } from "../../schema.js";
import { deleteFolderTree, type Folder, type FolderIndex } from "../folders.js";
import type { Invite } from "../sharing.js";
import { JoinDialog } from "./JoinDialog.js";
import { MoveDialog, type MoveItem } from "./MoveDialog.js";
import { NameDialog } from "./NameDialog.js";
import { ShareDialog } from "./ShareDialog.js";

export type DialogState =
  | { type: "new-folder"; parentId: string | null }
  | { type: "rename"; target: MoveItem }
  | { type: "move"; target: MoveItem }
  | { type: "delete"; target: MoveItem }
  | { type: "share"; folder: Folder }
  | { type: "profile" };

interface BrowserDialogsProps {
  dialog: DialogState | undefined;
  invite: Invite | undefined;
  index: FolderIndex;
  revision: string;
  userId: string | undefined;
  profile: { id: string; name: string } | undefined;
  onClose: () => void;
  onCloseInvite: () => void;
  onOpenFolder: (folderId: string) => void;
  onMove: (item: MoveItem, target: string | null) => void;
  onDeleted: (item: MoveItem) => void;
}

/** Every dialog the browser opens, and the writes they make. */
export function BrowserDialogs({
  dialog,
  invite,
  index,
  revision,
  userId,
  profile,
  onClose,
  onCloseInvite,
  onOpenFolder,
  onMove,
  onDeleted,
}: BrowserDialogsProps) {
  const db = useDb();
  const showToast = useToast();
  const renameTarget = dialog?.type === "rename" ? dialog.target : undefined;
  const deleteTarget = dialog?.type === "delete" ? dialog.target : undefined;

  return (
    <>
      <NameDialog
        isOpen={dialog?.type === "new-folder"}
        title="New folder"
        label="Folder name"
        initialValue=""
        actionLabel="Create"
        onClose={onClose}
        onSubmit={(name) => {
          if (!userId || dialog?.type !== "new-folder") return;
          // A subfolder belongs to the owner of its tree, whoever creates it.
          const parent = dialog.parentId ? index.byId.get(dialog.parentId) : undefined;
          const created = db.insert(app.folders, {
            name,
            owner_id: parent?.owner_id ?? userId,
            parent_id: dialog.parentId,
          });
          onOpenFolder(created.value.id);
        }}
      />
      <NameDialog
        isOpen={renameTarget !== undefined}
        title={`Rename ${renameTarget?.kind ?? ""}`}
        label="Name"
        initialValue={renameTarget?.name ?? ""}
        actionLabel="Rename"
        onClose={onClose}
        onSubmit={(name) => {
          if (!renameTarget) return;
          if (renameTarget.kind === "file") db.update(app.files, renameTarget.id, { name });
          else db.update(app.folders, renameTarget.id, { name });
        }}
      />
      <NameDialog
        isOpen={dialog?.type === "profile"}
        title="Your name"
        label="Name shown to people you share with"
        initialValue={profile?.name ?? ""}
        actionLabel="Save"
        onClose={onClose}
        onSubmit={(name) => {
          if (userId) db.upsert(app.profiles, userId, { name });
        }}
      />
      <MoveDialog
        item={dialog?.type === "move" ? dialog.target : undefined}
        index={index}
        revision={revision}
        onClose={onClose}
        onMove={(target) => {
          if (dialog?.type === "move") onMove(dialog.target, target);
        }}
      />
      <AlertDialog
        isOpen={deleteTarget !== undefined}
        onOpenChange={(open) => !open && onClose()}
        title={`Delete ${deleteTarget?.name ?? ""}?`}
        description={
          deleteTarget?.kind === "folder"
            ? "This deletes the folder with all of its subfolders and files, for everyone it is shared with."
            : "This deletes the file for everyone who can see this folder."
        }
        actionLabel="Delete"
        actionVariant="destructive"
        onAction={async () => {
          if (!deleteTarget) return;
          if (deleteTarget.kind === "file") {
            db.delete(app.files, deleteTarget.id);
          } else {
            try {
              await deleteFolderTree(db, index, deleteTarget.id);
            } catch (error) {
              showToast({
                type: "error",
                body: `The folder was not deleted: ${(error as Error).message}`,
              });
              return;
            }
          }
          onDeleted(deleteTarget);
          onClose();
        }}
      />
      <ShareDialog
        folder={dialog?.type === "share" ? dialog.folder : undefined}
        userId={userId}
        onClose={onClose}
      />
      <JoinDialog
        invite={invite}
        userId={userId}
        onClose={onCloseInvite}
        onJoined={(joined) => {
          onCloseInvite();
          onOpenFolder(joined);
        }}
      />
    </>
  );
}
