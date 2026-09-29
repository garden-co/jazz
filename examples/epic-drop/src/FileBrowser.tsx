import * as React from "react";
import { useAll, useDb, useSession } from "jazz-tools/react";
import { AppShell } from "@astryxdesign/core/AppShell";
import { Avatar } from "@astryxdesign/core/Avatar";
import { Button } from "@astryxdesign/core/Button";
import { Dialog, DialogHeader } from "@astryxdesign/core/Dialog";
import { EmptyState } from "@astryxdesign/core/EmptyState";
import { useMediaQuery } from "@astryxdesign/core/hooks";
import { Icon } from "@astryxdesign/core/Icon";
import { IconButton } from "@astryxdesign/core/IconButton";
import { Layout, LayoutContent, LayoutPanel } from "@astryxdesign/core/Layout";
import { Spinner } from "@astryxdesign/core/Spinner";
import { SideNav, SideNavSection } from "@astryxdesign/core/SideNav";
import { HStack, VStack } from "@astryxdesign/core/Stack";
import { Text } from "@astryxdesign/core/Text";
import { useToast } from "@astryxdesign/core/Toast";
import { TopNav, TopNavHeading } from "@astryxdesign/core/TopNav";
import { app } from "../schema.js";
import { fileTableQuery } from "./file-list-query.js";
import { FolderIndex } from "./folders.js";
import { readFileBlob, saveBlob } from "./large-values.js";
import { parseInviteHash, type Invite } from "./sharing.js";
import { useEnsureProfile, useNames } from "./profiles.js";
import { useBrowserAdvice } from "./use-browser-advice.js";
import { useUploads } from "./use-uploads.js";
import type { DropPayload } from "./drag.js";
import { BrowserDialogs, type DialogState } from "./components/BrowserDialogs.js";
import type { Entry, EntryAction } from "./components/FileTable.js";
import { FolderTree } from "./components/FolderTree.js";
import { FolderView, type FolderAction } from "./components/FolderView.js";
import { checkMove, type MoveItem } from "./components/MoveDialog.js";
import { PreviewPanel, type PreviewFile } from "./components/PreviewPanel.js";

function clearInviteHash() {
  history.replaceState(null, "", window.location.pathname + window.location.search);
}

export function FileBrowser() {
  const db = useDb();
  const showToast = useToast();
  const userId = useSession()?.user.account ?? undefined;
  const profile = useEnsureProfile(userId);
  const isWide = useMediaQuery("(min-width: 1100px)");

  const { data: folders = [], isLoading: foldersLoading } = useAll(app.folders);
  const { data: memberships = [] } = useAll(
    userId ? app.folderMembers.where({ user_id: userId }) : undefined,
  );
  const index = React.useMemo(() => new FolderIndex(folders, userId), [folders, userId]);
  // Anything that can change what the user may do: folder placement and ownership, and roles.
  const revision = [
    ...folders.map((f) => `${f.id}:${f.parent_id ?? ""}:${f.owner_id}`),
    ...memberships.map((m) => `${m.folder_id}:${m.role}`),
  ]
    .sort()
    .join(",");
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
  React.useEffect(
    () =>
      db.onMutationError((event) => {
        showToast({ type: "error", body: `A change was not allowed: ${event.reason}` });
      }),
    [db, showToast],
  );

  const { data: files = [] } = useAll(fileTableQuery(folder?.id));
  const subfolders = folder ? (index.children.get(folder.id) ?? []) : [];
  const nameOf = useNames(
    [...files.map((file) => file.owner_id), ...subfolders.map((sub) => sub.owner_id)],
    userId,
  );
  const uploads = useUploads(userId);

  const entries: Entry[] = [
    ...subfolders.map((sub) => ({
      id: sub.id,
      kind: "folder" as const,
      name: sub.name,
      type: "Folder",
      size: 0,
      modified: null,
      owner: nameOf(sub.owner_id),
      ownerId: sub.owner_id,
    })),
    ...files.map((file) => ({
      id: file.id,
      kind: "file" as const,
      name: file.name,
      type: file.content_type,
      size: file.size_bytes,
      modified: file.$updatedAt ?? null,
      owner: nameOf(file.owner_id),
      ownerId: file.owner_id,
    })),
  ];
  const may = useBrowserAdvice({
    index,
    userId,
    folderId: folder?.id,
    entries,
    memberships,
    revision,
  });

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

  async function moveItem(item: { kind: "file" | "folder"; id: string }, target: string | null) {
    // A folder cannot go into itself or so deep that inherited access stops
    // reaching it; everything else is Jazz's call.
    const fits =
      target === null ||
      index
        .moveCandidates(item.kind === "folder" ? item.id : undefined)
        .some((candidate) => candidate.id === target);
    if (!fits || (await checkMove(db, item, target)) === "denied") {
      showToast({ body: "That item cannot move into this folder." });
      return;
    }
    if (item.kind === "file") {
      if (target) db.update(app.files, item.id, { folder_id: target });
    } else {
      db.update(app.folders, item.id, { parent_id: target });
    }
  }

  function handleDrop(target: string, payload: DropPayload) {
    if (payload.kind === "files") uploads.start(payload.files, target);
    else if (payload.id !== target) void moveItem(payload, target);
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

  function handleEntryAction(entry: Entry, action: EntryAction) {
    const target: MoveItem = { kind: entry.kind, id: entry.id, name: entry.name, folderId };
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

  function handleFolderAction(action: FolderAction) {
    if (!folder) return;
    const target: MoveItem = { kind: "folder", id: folder.id, name: folder.name };
    if (action === "new-folder") setDialog({ type: "new-folder", parentId: folder.id });
    else if (action === "share") setDialog({ type: "share", folder });
    else setDialog({ type: action, target });
  }

  const tree = (label: string, roots: typeof myRoots) => (
    <FolderTree
      label={label}
      roots={roots}
      index={index}
      selectedId={folder?.id}
      canDropOn={may.canUpload}
      onSelect={openFolder}
      onDrop={handleDrop}
    />
  );
  const sideNav = (
    <SideNav
      topContent={
        <Button
          label="New folder"
          width="100%"
          isDisabled={!userId}
          onClick={() => setDialog({ type: "new-folder", parentId: null })}
        />
      }
    >
      <SideNavSection title="My files">
        {myRoots.length > 0 ? (
          tree("My files", myRoots)
        ) : (
          <Text type="supporting" color="secondary">
            No folders yet
          </Text>
        )}
      </SideNavSection>
      <SideNavSection title="Shared with me">
        {sharedRoots.length > 0 ? (
          tree("Shared with me", sharedRoots)
        ) : (
          <Text type="supporting" color="secondary">
            Folders others share with you appear here
          </Text>
        )}
      </SideNavSection>
    </SideNav>
  );

  const content = folder ? (
    <FolderView
      folder={folder}
      index={index}
      entries={entries}
      may={may}
      uploads={uploads}
      isCompact={preview !== undefined && isWide}
      onOpenFolder={openFolder}
      onFolderAction={handleFolderAction}
      onEntryAction={handleEntryAction}
      onDrop={handleDrop}
    />
  ) : foldersLoading ? (
    <Spinner label="Loading folders" />
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
            icon={<Icon icon="close" size="sm" />}
            onClick={() => setPreviewId(undefined)}
          />
        </HStack>
        <PreviewPanel file={preview} />
      </VStack>
    </LayoutPanel>
  );

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

      <BrowserDialogs
        dialog={dialog}
        invite={invite}
        index={index}
        revision={revision}
        userId={userId}
        profile={profile}
        onClose={() => setDialog(undefined)}
        onCloseInvite={() => {
          setInvite(undefined);
          clearInviteHash();
        }}
        onOpenFolder={openFolder}
        onMove={(item, target) => void moveItem(item, target)}
        onDeleted={(item) => {
          if (item.id === previewId) setPreviewId(undefined);
          if (item.id === folder?.id) setFolderId(undefined);
        }}
      />
    </AppShell>
  );
}
