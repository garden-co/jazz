import * as React from "react";
import { FolderPlus, Share2, X } from "lucide-react";
import { useAll, useDb, useSession } from "jazz-tools/react";
import { AlertDialog } from "@astryxdesign/core/AlertDialog";
import { AppShell } from "@astryxdesign/core/AppShell";
import { Avatar } from "@astryxdesign/core/Avatar";
import { Badge } from "@astryxdesign/core/Badge";
import { BreadcrumbItem, Breadcrumbs } from "@astryxdesign/core/Breadcrumbs";
import { Button } from "@astryxdesign/core/Button";
import { Dialog, DialogHeader } from "@astryxdesign/core/Dialog";
import { EmptyState } from "@astryxdesign/core/EmptyState";
import { FileInput } from "@astryxdesign/core/FileInput";
import { useMediaQuery } from "@astryxdesign/core/hooks";
import { Icon } from "@astryxdesign/core/Icon";
import { IconButton } from "@astryxdesign/core/IconButton";
import { Layout, LayoutContent, LayoutPanel } from "@astryxdesign/core/Layout";
import { MoreMenu } from "@astryxdesign/core/MoreMenu";
import { SideNav, SideNavSection } from "@astryxdesign/core/SideNav";
import { HStack, StackItem, VStack } from "@astryxdesign/core/Stack";
import { Heading, Text } from "@astryxdesign/core/Text";
import { useToast } from "@astryxdesign/core/Toast";
import { TopNav, TopNavHeading } from "@astryxdesign/core/TopNav";
import { app, MAX_FOLDER_DEPTH } from "../schema.js";
import { fileTableQuery } from "./file-list-query.js";
import { deleteFolderTree, FolderIndex, type Folder } from "./folders.js";
import { readFileBlob, saveBlob } from "./large-values.js";
import { parseInviteHash, type Invite } from "./sharing.js";
import { useEnsureProfile, useNames } from "./profiles.js";
import { useUploads } from "./use-uploads.js";
import type { DropPayload } from "./drag.js";
import { FileTable, type Entry, type EntryAction } from "./components/FileTable.js";
import { FolderTree } from "./components/FolderTree.js";
import { JoinDialog } from "./components/JoinDialog.js";
import { MoveDialog } from "./components/MoveDialog.js";
import { NameDialog } from "./components/NameDialog.js";
import { PreviewPanel, type PreviewFile } from "./components/PreviewPanel.js";
import { ShareDialog } from "./components/ShareDialog.js";
import { UploadQueue } from "./components/UploadQueue.js";

type Target = { kind: "file" | "folder"; id: string; name: string; folderId?: string };

type DialogState =
  | { type: "new-folder"; parentId: string | null }
  | { type: "rename"; target: Target }
  | { type: "move"; target: Target }
  | { type: "delete"; target: Target }
  | { type: "share"; folder: Folder }
  | { type: "profile" };

export function FileBrowser() {
  const db = useDb();
  const showToast = useToast();
  const userId = useSession()?.user.account;
  const profile = useEnsureProfile(userId);
  const isWide = useMediaQuery("(min-width: 1100px)");

  const { data: folders = [] } = useAll(app.folders);
  const { data: memberships = [] } = useAll(
    userId ? app.folderMembers.where({ user_id: userId }) : undefined,
  );
  const index = React.useMemo(
    () => new FolderIndex(folders, userId, memberships),
    [folders, userId, memberships],
  );
  const myRoots = index.roots.filter((folder) => index.isMine(folder));
  const sharedRoots = index.roots.filter((folder) => !index.isMine(folder));

  const [folderId, setFolderId] = React.useState<string>();
  const [previewId, setPreviewId] = React.useState<string>();
  const [dialog, setDialog] = React.useState<DialogState>();
  const [invite, setInvite] = React.useState<Invite | undefined>(() =>
    parseInviteHash(window.location.hash),
  );
  const [isNavOpen, setIsNavOpen] = React.useState(false);

  const folder = folderId ? index.byId.get(folderId) : undefined;
  React.useEffect(() => {
    if (!folderId && index.roots[0]) setFolderId(index.roots[0].id);
  }, [folderId, index]);
  React.useEffect(() => {
    const onHash = () => setInvite(parseInviteHash(window.location.hash));
    window.addEventListener("hashchange", onHash);
    return () => window.removeEventListener("hashchange", onHash);
  }, []);

  const { data: files = [] } = useAll(fileTableQuery(folder?.id));
  const subfolders = folder ? (index.children.get(folder.id) ?? []) : [];
  const nameOf = useNames(
    [...files.map((file) => file.owner_id), ...subfolders.map((sub) => sub.owner_id)],
    userId,
  );
  const uploads = useUploads(userId);

  React.useEffect(
    () =>
      db.onMutationError((event) => {
        showToast({ type: "error", body: `A change was not allowed: ${event.reason}` });
      }),
    [db, showToast],
  );

  const canEdit = index.canEdit(folder?.id);
  const role = index.roleIn(folder?.id);
  const canNest = folder ? index.depth(folder.id) < MAX_FOLDER_DEPTH : true;

  const entries: Entry[] = [
    ...subfolders.map((sub) => ({
      id: sub.id,
      kind: "folder" as const,
      name: sub.name,
      type: "Folder",
      size: 0,
      modified: null,
      owner: nameOf(sub.owner_id),
    })),
    ...files.map((file) => ({
      id: file.id,
      kind: "file" as const,
      name: file.name,
      type: file.content_type,
      size: file.size_bytes,
      modified: file.$updatedAt ?? null,
      owner: nameOf(file.owner_id),
    })),
  ];
  const previewRow = files.find((file) => file.id === previewId);
  const preview: PreviewFile | undefined = previewRow && {
    id: previewRow.id,
    name: previewRow.name,
    content_type: previewRow.content_type,
    size_bytes: previewRow.size_bytes,
    modified: previewRow.$updatedAt ?? null,
    owner: nameOf(previewRow.owner_id),
  };

  function openFolder(id: string) {
    setFolderId(id);
    setPreviewId(undefined);
    setIsNavOpen(false);
  }

  function moveItem(item: { kind: "file" | "folder"; id: string }, target: string | null) {
    if (item.kind === "file") {
      if (target) db.update(app.files, item.id, { folder_id: target });
      return;
    }
    const blocked = new Set([item.id, ...index.descendants(item.id).map((f) => f.id)]);
    if (target && blocked.has(target)) {
      showToast({ body: "A folder cannot move into itself." });
      return;
    }
    db.update(app.folders, item.id, { parent_id: target });
  }

  function handleDrop(target: string, payload: DropPayload) {
    if (payload.kind === "files") uploads.start(payload.files, target);
    else moveItem(payload, target);
  }

  async function download(id: string) {
    const file = files.find((row) => row.id === id);
    if (!file) return;
    try {
      saveBlob(await readFileBlob(db, file.id, file.content_type), file.name);
    } catch (error) {
      showToast({ type: "error", body: `Download failed: ${(error as Error).message}` });
    }
  }

  function handleAction(entry: Entry, action: EntryAction) {
    const target: Target = { kind: entry.kind, id: entry.id, name: entry.name, folderId: folder?.id };
    switch (action) {
      case "open":
        if (entry.kind === "folder") openFolder(entry.id);
        else setPreviewId(entry.id);
        return;
      case "download":
        void download(entry.id);
        return;
      case "share": {
        const shared = index.byId.get(entry.id);
        if (shared) setDialog({ type: "share", folder: shared });
        return;
      }
      default:
        setDialog({ type: action, target });
    }
  }

  const sideNav = (
    <SideNav
      topContent={
        <Button
          label="New folder"
          icon={<Icon icon={FolderPlus} size="sm" />}
          width="100%"
          isDisabled={!userId}
          onClick={() => setDialog({ type: "new-folder", parentId: null })}
        />
      }
    >
      <SideNavSection title="My files">
        {myRoots.length > 0 ? (
          <FolderTree
            label="My files"
            roots={myRoots}
            index={index}
            selectedId={folder?.id}
            onSelect={openFolder}
            onDrop={handleDrop}
          />
        ) : (
          <Text type="supporting" color="secondary">
            No folders yet
          </Text>
        )}
      </SideNavSection>
      <SideNavSection title="Shared with me">
        {sharedRoots.length > 0 ? (
          <FolderTree
            label="Shared with me"
            roots={sharedRoots}
            index={index}
            selectedId={folder?.id}
            onSelect={openFolder}
            onDrop={handleDrop}
          />
        ) : (
          <Text type="supporting" color="secondary">
            Folders others share with you appear here
          </Text>
        )}
      </SideNavSection>
    </SideNav>
  );

  const folderMenu =
    folder && canEdit
      ? [
          {
            label: "Rename folder",
            onClick: () =>
              setDialog({ type: "rename", target: { kind: "folder", id: folder.id, name: folder.name } }),
          },
          {
            label: "Move folder",
            onClick: () =>
              setDialog({ type: "move", target: { kind: "folder", id: folder.id, name: folder.name } }),
          },
          { type: "divider" as const },
          {
            label: "Delete folder",
            variant: "destructive" as const,
            onClick: () =>
              setDialog({ type: "delete", target: { kind: "folder", id: folder.id, name: folder.name } }),
          },
        ]
      : [];

  const content = folder ? (
    <VStack gap={5}>
      <VStack gap={2}>
        <Breadcrumbs label="Folder path">
          {index.path(folder.id).map((step) => (
            <BreadcrumbItem
              key={step.id}
              isCurrent={step.id === folder.id}
              onClick={() => openFolder(step.id)}
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
              {role !== "owner" && (
                <Badge variant="info" label={role === "editor" ? "Can edit" : "Can view"} />
              )}
            </HStack>
          </StackItem>
          <HStack gap={2} vAlign="center">
            {index.isMine(folder) && (
              <Button
                label="Share"
                icon={<Icon icon={Share2} size="sm" />}
                onClick={() => setDialog({ type: "share", folder })}
              />
            )}
            {canEdit && canNest && (
              <Button
                label="New folder"
                icon={<Icon icon={FolderPlus} size="sm" />}
                onClick={() => setDialog({ type: "new-folder", parentId: folder.id })}
              />
            )}
            {folderMenu.length > 0 && (
              <MoreMenu label="Folder actions" alignment="end" presentation="adaptive" items={folderMenu} />
            )}
          </HStack>
        </HStack>
      </VStack>
      {canEdit && (
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
          canEdit={canEdit}
          canShare={(entry) => entry.kind === "folder" && index.byId.get(entry.id)?.owner_id === userId}
          onAction={handleAction}
          onDropOnFolder={handleDrop}
        />
      ) : (
        <EmptyState
          title="This folder is empty"
          description={
            canEdit
              ? "Drop files above, or create a subfolder."
              : "Files the owner adds here will appear for you too."
          }
        />
      )}
    </VStack>
  ) : folderId ? (
    <EmptyState
      headingLevel={1}
      title="This folder is not available"
      description="It may still be syncing, or it was deleted or is no longer shared with you."
      actions={<Button label="Back to my files" onClick={() => setFolderId(undefined)} />}
    />
  ) : (
    <EmptyState
      headingLevel={1}
      title="No folders yet"
      description="Create a folder, then drop files into it. You can share folders with others."
      actions={
        <Button
          label="New folder"
          variant="primary"
          icon={<Icon icon={FolderPlus} size="sm" />}
          isDisabled={!userId}
          onClick={() => setDialog({ type: "new-folder", parentId: null })}
        />
      }
    />
  );

  const previewPanel = preview && isWide && (
    <LayoutPanel width={420} hasDivider padding={5} label="Preview">
      <VStack gap={2}>
        <HStack hAlign="end">
          <IconButton
            label="Close preview"
            variant="ghost"
            size="sm"
            icon={<Icon icon={X} size="sm" />}
            onClick={() => setPreviewId(undefined)}
          />
        </HStack>
        <PreviewPanel file={preview} />
      </VStack>
    </LayoutPanel>
  );

  const renameTarget = dialog?.type === "rename" ? dialog.target : undefined;

  return (
    <AppShell
      height="auto"
      variant="section"
      topNav={
        <TopNav
          label="EpicDrop"
          heading={<TopNavHeading heading="EpicDrop" />}
          endContent={
            <Button
              label={profile?.name ?? "Your name"}
              variant="ghost"
              icon={<Avatar name={profile?.name ?? "?"} size="xsm" tooltip={false} />}
              onClick={() => setDialog({ type: "profile" })}
            />
          }
        />
      }
      sideNav={sideNav}
      mobileNav={{ isOpen: isNavOpen, onOpenChange: setIsNavOpen }}
    >
      <Layout
        height="auto"
        content={
          <LayoutContent isScrollable={false} padding={6}>
            {content}
          </LayoutContent>
        }
        end={previewPanel || undefined}
      />

      <Dialog
        isOpen={preview !== undefined && !isWide}
        onOpenChange={(open) => !open && setPreviewId(undefined)}
        width={640}
        maxHeight="90dvh"
      >
        <VStack gap={3}>
          <DialogHeader title="Preview" onOpenChange={(open) => !open && setPreviewId(undefined)} />
          {preview && !isWide && <PreviewPanel file={preview} headingLevel={3} />}
        </VStack>
      </Dialog>

      <NameDialog
        isOpen={dialog?.type === "new-folder"}
        title="New folder"
        label="Folder name"
        initialValue=""
        actionLabel="Create"
        onClose={() => setDialog(undefined)}
        onSubmit={(name) => {
          if (!userId || dialog?.type !== "new-folder") return;
          const created = db.insert(app.folders, {
            name,
            owner_id: userId,
            parent_id: dialog.parentId,
          });
          openFolder(created.value.id);
        }}
      />
      <NameDialog
        isOpen={renameTarget !== undefined}
        title={`Rename ${renameTarget?.kind ?? ""}`}
        label="Name"
        initialValue={renameTarget?.name ?? ""}
        actionLabel="Rename"
        onClose={() => setDialog(undefined)}
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
        onClose={() => setDialog(undefined)}
        onSubmit={(name) => {
          if (profile) db.update(app.profiles, profile.id, { name });
        }}
      />
      <MoveDialog
        item={dialog?.type === "move" ? dialog.target : undefined}
        index={index}
        onClose={() => setDialog(undefined)}
        onMove={(target) => {
          if (dialog?.type === "move") moveItem(dialog.target, target);
        }}
      />
      <AlertDialog
        isOpen={dialog?.type === "delete"}
        onOpenChange={(open) => !open && setDialog(undefined)}
        title={`Delete ${dialog?.type === "delete" ? dialog.target.name : ""}?`}
        description={
          dialog?.type === "delete" && dialog.target.kind === "folder"
            ? "This deletes the folder with all of its subfolders and files, for everyone it is shared with."
            : "This deletes the file for everyone who can see this folder."
        }
        actionLabel="Delete"
        actionVariant="destructive"
        onAction={async () => {
          if (dialog?.type !== "delete") return;
          const { target } = dialog;
          if (target.kind === "file") {
            db.delete(app.files, target.id);
            if (previewId === target.id) setPreviewId(undefined);
          } else {
            await deleteFolderTree(db, index, target.id);
            if (target.id === folder?.id) setFolderId(undefined);
          }
          setDialog(undefined);
        }}
      />
      <ShareDialog
        folder={dialog?.type === "share" ? dialog.folder : undefined}
        userId={userId}
        onClose={() => setDialog(undefined)}
      />
      <JoinDialog
        invite={invite}
        userId={userId}
        onClose={() => {
          setInvite(undefined);
          history.replaceState(null, "", window.location.pathname + window.location.search);
        }}
        onJoined={(joined) => {
          setInvite(undefined);
          history.replaceState(null, "", window.location.pathname + window.location.search);
          openFolder(joined);
        }}
      />
    </AppShell>
  );
}
