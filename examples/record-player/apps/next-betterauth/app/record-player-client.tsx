"use client";

import { useEffect, useState } from "react";
import { Upload } from "lucide-react";
import { Badge } from "@astryxdesign/core/Badge";
import { Banner } from "@astryxdesign/core/Banner";
import { Button } from "@astryxdesign/core/Button";
import { EmptyState } from "@astryxdesign/core/EmptyState";
import { Icon } from "@astryxdesign/core/Icon";
import { ProgressBar } from "@astryxdesign/core/ProgressBar";
import { HStack, VStack } from "@astryxdesign/core/Stack";
import { Tab, TabList } from "@astryxdesign/core/TabList";
import { seedDemoLibrary, type UploadProgress } from "../src/upload";
import { AlbumShelf } from "./album-shelf";
import { AlbumView, useAlbumTracks } from "./album-view";
import { formatBytes } from "./format";
import { Invitations, usePendingInvitations } from "./invitations";
import { useAlbums, useStore } from "./library-data";
import { PlayerProvider } from "./player";
import { Playlists } from "./playlists";
import { UploadDialog } from "./upload-dialog";

type View = "library" | "playlists" | "invitations";

export function RecordPlayerClient() {
  const [mounted, setMounted] = useState(false);
  useEffect(() => setMounted(true), []);
  // The Jazz provider is browser-owned. Do not invoke its hooks during Next's
  // static render; the live component mounts once the provider is available.
  return mounted ? <LiveRecordPlayer /> : null;
}

function LiveRecordPlayer() {
  const store = useStore();
  const [view, setView] = useState<View>("library");
  const pending = usePendingInvitations();
  const pendingCount = pending.data?.length ?? 0;

  return (
    <PlayerProvider store={store}>
      <VStack gap={6} data-testid="record-player">
        <TabList value={view} onChange={(value) => setView(value as View)} hasDivider>
          <Tab value="library" label="Library" />
          <Tab value="playlists" label="Playlists" />
          <Tab
            value="invitations"
            label="Invitations"
            endContent={
              pendingCount > 0 ? <Badge variant="info" label={pendingCount} /> : undefined
            }
          />
        </TabList>
        {view === "library" && <Library />}
        {view === "playlists" && <Playlists />}
        {view === "invitations" && <Invitations onAccepted={() => setView("playlists")} />}
      </VStack>
    </PlayerProvider>
  );
}

function Library() {
  const store = useStore();
  const albums = useAlbums();
  const [selectedId, setSelectedId] = useState<string>();
  const [upload, setUpload] = useState<"new" | "add">();
  const [seeding, setSeeding] = useState<UploadProgress>();
  const [seedError, setSeedError] = useState<string>();
  const list = albums.data ?? [];
  const selected = list.find((album) => album.id === selectedId) ?? list[0];
  const selectedTracks = useAlbumTracks(selected);
  const nextOrdinal = Math.max(0, ...(selectedTracks ?? []).map((track) => track.ordinal)) + 1;

  async function seed() {
    setSeedError(undefined);
    setSeeding({ label: "demo library", sentBytes: 0, totalBytes: 1 });
    try {
      await seedDemoLibrary(store, setSeeding);
    } catch (cause) {
      setSeedError(cause instanceof Error ? cause.message : String(cause));
    } finally {
      setSeeding(undefined);
    }
  }

  const seedStatus = (
    <>
      {seeding && (
        <ProgressBar
          label={`Writing ${seeding.label}`}
          value={seeding.sentBytes}
          max={Math.max(1, seeding.totalBytes)}
          hasValueLabel
          formatValueLabel={(value, max) => `${formatBytes(value)} of ${formatBytes(max)}`}
        />
      )}
      {seedError && (
        <Banner status="error" title="Could not add the demo library" description={seedError} />
      )}
    </>
  );

  const dialog = (
    <UploadDialog
      isOpen={upload !== undefined}
      album={upload === "add" ? selected : undefined}
      nextOrdinal={nextOrdinal}
      onClose={() => setUpload(undefined)}
      onUploaded={(albumId) => {
        setUpload(undefined);
        setSelectedId(albumId);
      }}
    />
  );

  if (albums.data && list.length === 0) {
    return (
      <VStack gap={4}>
        <EmptyState
          title="The library is empty"
          description="Upload audio files, or add a small demo library of generated tones."
          actions={
            <HStack gap={2} wrap="wrap" justify="center">
              <Button
                label="Add demo library"
                variant="primary"
                isLoading={seeding !== undefined}
                onClick={() => void seed()}
              />
              <Button
                label="Upload music"
                variant="secondary"
                icon={<Icon icon={Upload} size="sm" />}
                isDisabled={seeding !== undefined}
                onClick={() => setUpload("new")}
              />
            </HStack>
          }
        />
        {seedStatus}
        {dialog}
      </VStack>
    );
  }

  return (
    <VStack gap={6}>
      {seedStatus}
      <HStack justify="end">
        <Button
          label="Upload album"
          variant="secondary"
          icon={<Icon icon={Upload} size="sm" />}
          onClick={() => setUpload("new")}
        />
      </HStack>
      <AlbumShelf albums={list} selectedId={selected?.id} onSelect={setSelectedId} />
      {selected && <AlbumView album={selected} onAddTracks={() => setUpload("add")} />}
      {dialog}
    </VStack>
  );
}
