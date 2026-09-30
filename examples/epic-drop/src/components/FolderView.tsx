import { Badge } from "@astryxdesign/core/Badge";
import { BreadcrumbItem, Breadcrumbs } from "@astryxdesign/core/Breadcrumbs";
import { Button } from "@astryxdesign/core/Button";
import { EmptyState } from "@astryxdesign/core/EmptyState";
import { FileInput } from "@astryxdesign/core/FileInput";
import { MoreMenu } from "@astryxdesign/core/MoreMenu";
import { HStack, StackItem, VStack } from "@astryxdesign/core/Stack";
import { Heading } from "@astryxdesign/core/Text";
import { MAX_FOLDER_DEPTH } from "../../schema.js";
import type { DropPayload } from "../drag.js";
import type { Folder, FolderIndex } from "../folders.js";
import type { BrowserAdvice } from "../use-browser-advice.js";
import type { UploadTask } from "../use-uploads.js";
import { FileTable, type Entry, type EntryAction } from "./FileTable.js";
import { UploadQueue } from "./UploadQueue.js";

export type FolderAction = "new-folder" | "share" | "rename" | "move" | "delete";

interface FolderViewProps {
  folder: Folder;
  index: FolderIndex;
  entries: Entry[];
  may: BrowserAdvice;
  uploads: {
    tasks: readonly UploadTask[];
    start: (files: readonly File[], folderId: string) => void;
    cancel: (id: string) => void;
    dismiss: (id: string) => void;
  };
  isCompact: boolean;
  onOpenFolder: (folderId: string) => void;
  onFolderAction: (action: FolderAction) => void;
  onEntryAction: (entry: Entry, action: EntryAction) => void;
  onDrop: (folderId: string, payload: DropPayload) => void;
}

/** One open folder: its path, heading and actions, the upload dropzone and its contents. */
export function FolderView({
  folder,
  index,
  entries,
  may,
  uploads,
  isCompact,
  onOpenFolder,
  onFolderAction,
  onEntryAction,
  onDrop,
}: FolderViewProps) {
  const canUpload = may.canUpload(folder.id);
  const canNest = index.depth(folder.id) < MAX_FOLDER_DEPTH && may.canCreateSubfolder(folder.id);
  const access = index.isInOwnTree(folder.id) ? undefined : canUpload ? "Can edit" : "Can view";

  const menu = [
    ...(may.canEdit("folder", folder.id)
      ? [{ label: "Rename folder", onClick: () => onFolderAction("rename") }]
      : []),
    ...(may.canMove("folder", folder.id, folder.owner_id)
      ? [{ label: "Move folder", onClick: () => onFolderAction("move") }]
      : []),
    ...(may.canDelete("folder", folder.id)
      ? [
          { type: "divider" as const },
          {
            label: "Delete folder",
            variant: "destructive" as const,
            onClick: () => onFolderAction("delete"),
          },
        ]
      : []),
  ];
  if (menu[0]?.type === "divider") menu.shift();

  return (
    <VStack gap={5}>
      <VStack gap={2}>
        <Breadcrumbs label="Folder path">
          {index.path(folder.id).map((step) => (
            <BreadcrumbItem
              key={step.id}
              isCurrent={step.id === folder.id}
              onClick={() => onOpenFolder(step.id)}
            >
              {step.name}
            </BreadcrumbItem>
          ))}
        </Breadcrumbs>
        <HStack gap={3} vAlign="center" wrap="wrap">
          <StackItem size="fill">
            <HStack gap={2} vAlign="center">
              <Heading level={1} maxLines={1}>
                {folder.name}
              </Heading>
              {access && <Badge variant="info" label={access} />}
            </HStack>
          </StackItem>
          <HStack gap={2} vAlign="center">
            {may.canShare(folder.id) && (
              <Button label="Share" onClick={() => onFolderAction("share")} />
            )}
            {canNest && <Button label="New folder" onClick={() => onFolderAction("new-folder")} />}
            {menu.length > 0 && (
              <MoreMenu
                label="Folder actions"
                alignment="end"
                presentation="adaptive"
                items={menu}
              />
            )}
          </HStack>
        </HStack>
      </VStack>
      {canUpload && (
        <FileInput
          label="Upload files"
          isLabelHidden
          mode="dropzone"
          isMultiple
          value={null}
          placeholder="Drop files here or browse"
          description="Files stream into Jazz in chunks, so large files never sit in memory whole."
          onChange={(picked) => {
            const list = Array.isArray(picked) ? picked : picked ? [picked] : [];
            if (list.length > 0) uploads.start(list, folder.id);
          }}
        />
      )}
      <UploadQueue tasks={uploads.tasks} onCancel={uploads.cancel} onDismiss={uploads.dismiss} />
      {entries.length > 0 ? (
        <FileTable
          entries={entries}
          canEdit={(entry) => may.canEdit(entry.kind, entry.id)}
          canMove={(entry) => may.canMove(entry.kind, entry.id, entry.ownerId)}
          canDelete={(entry) => may.canDelete(entry.kind, entry.id)}
          canShare={(entry) => entry.kind === "folder" && may.canShare(entry.id)}
          canDropOn={may.canUpload}
          isCompact={isCompact}
          onAction={onEntryAction}
          onDropOnFolder={onDrop}
        />
      ) : (
        <EmptyState
          title="This folder is empty"
          description={
            canUpload
              ? "Drop files above, or create a subfolder."
              : "Files the owner adds here will appear for you too."
          }
        />
      )}
    </VStack>
  );
}
