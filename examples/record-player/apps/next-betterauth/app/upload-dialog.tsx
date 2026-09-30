"use client";

import { useState } from "react";
import { Banner } from "@astryxdesign/core/Banner";
import { Button } from "@astryxdesign/core/Button";
import { Dialog, DialogHeader } from "@astryxdesign/core/Dialog";
import { FileInput } from "@astryxdesign/core/FileInput";
import { Layout, LayoutContent, LayoutFooter } from "@astryxdesign/core/Layout";
import { ProgressBar } from "@astryxdesign/core/ProgressBar";
import { HStack, VStack } from "@astryxdesign/core/Stack";
import { TextInput } from "@astryxdesign/core/TextInput";
import { uploadTracks, type UploadProgress } from "../src/upload";
import { formatBytes } from "./format";
import { useStore, type Album } from "./library-data";

const MAX_COVER_BYTES = 1024 * 1024;

/**
 * Creates an album (or adds to `album`) and streams each audio file into its
 * own track with `insertStreaming`. Progress counts bytes as Jazz consumes
 * them; nothing reads a whole file into memory first.
 */
export function UploadDialog({
  isOpen,
  album,
  nextOrdinal,
  onClose,
  onUploaded,
}: {
  isOpen: boolean;
  album?: Album;
  nextOrdinal: number;
  onClose(): void;
  onUploaded(albumId: string): void;
}) {
  const store = useStore();
  const [title, setTitle] = useState("");
  const [artist, setArtist] = useState("");
  const [cover, setCover] = useState<File | null>(null);
  const [files, setFiles] = useState<File[]>([]);
  const [progress, setProgress] = useState<UploadProgress>();
  const [error, setError] = useState<string>();
  const [createdAlbumId, setCreatedAlbumId] = useState<string>();
  // Files already written; a retry resumes from the first one that failed.
  const [writtenCount, setWrittenCount] = useState(0);
  const [startOrdinal, setStartOrdinal] = useState<number>();
  const isUploading = progress !== undefined;
  const canSubmit = files.length > 0 && (album || title.trim()) && !isUploading;

  function reset() {
    setTitle("");
    setArtist("");
    setCover(null);
    setFiles([]);
    setProgress(undefined);
    setError(undefined);
    setCreatedAlbumId(undefined);
    setWrittenCount(0);
    setStartOrdinal(undefined);
  }

  async function submit() {
    setError(undefined);
    setProgress({ label: "Preparing", sentBytes: 0, totalBytes: 1 });
    try {
      // A retry after a failed upload reuses the album it already created:
      // catalogue rows cannot be deleted (see README, Known limits).
      const albumId =
        album?.id ??
        createdAlbumId ??
        store.createAlbum({
          title: title.trim(),
          artist: artist.trim() || "Unknown artist",
          cover: cover
            ? { bytes: new Uint8Array(await cover.arrayBuffer()), mimeType: cover.type }
            : undefined,
        });
      if (!album) setCreatedAlbumId(albumId);
      // Fixed at the first attempt: the album's live track count grows as we write.
      const start = startOrdinal ?? (album ? nextOrdinal : 1);
      setStartOrdinal(start);
      const firstOrdinal = start + writtenCount;
      await uploadTracks(
        store,
        albumId,
        files.slice(writtenCount),
        firstOrdinal,
        setProgress,
        (index) => setWrittenCount(writtenCount + index + 1),
      );
      reset();
      onUploaded(albumId);
    } catch (cause) {
      setProgress(undefined);
      setError(cause instanceof Error ? cause.message : String(cause));
    }
  }

  const close = () => {
    if (isUploading) return;
    reset();
    onClose();
  };

  return (
    <Dialog isOpen={isOpen} onOpenChange={(open) => !open && close()} purpose="form" width={520}>
      <Layout
        height="auto"
        header={
          <DialogHeader
            title={album ? `Add tracks to ${album.title}` : "Upload an album"}
            subtitle="Audio streams into Jazz as it uploads."
            onOpenChange={(open) => !open && close()}
          />
        }
        content={
          <LayoutContent>
            <VStack gap={4}>
              {!album && (
                <>
                  <TextInput label="Album title" value={title} onChange={setTitle} isRequired />
                  <TextInput label="Artist" value={artist} onChange={setArtist} isOptional />
                  <FileInput
                    label="Cover image"
                    accept="image/*"
                    maxSize={MAX_COVER_BYTES}
                    description="Up to 1 MB. Without one, the album gets a generated cover."
                    value={cover}
                    onChange={(value) =>
                      setCover(Array.isArray(value) ? (value[0] ?? null) : value)
                    }
                    isOptional
                  />
                </>
              )}
              <FileInput
                label="Audio files"
                accept="audio/*"
                isMultiple
                mode="dropzone"
                description="MP3 and WebM start playing while they stream; other formats play once read."
                value={files}
                onChange={(value) => {
                  setFiles(value ? (Array.isArray(value) ? value : [value]) : []);
                  // A new selection starts over (the album, if created, is kept).
                  setWrittenCount(0);
                  setStartOrdinal(undefined);
                }}
              />
              {progress && (
                <ProgressBar
                  label={`Uploading ${progress.label}`}
                  value={progress.sentBytes}
                  max={Math.max(1, progress.totalBytes)}
                  hasValueLabel
                  formatValueLabel={(value, max) => `${formatBytes(value)} of ${formatBytes(max)}`}
                />
              )}
              {error && <Banner status="error" title="Upload failed" description={error} />}
            </VStack>
          </LayoutContent>
        }
        footer={
          <LayoutFooter hasDivider>
            <HStack gap={2} justify="end">
              <Button label="Cancel" variant="secondary" onClick={close} isDisabled={isUploading} />
              <Button
                label="Upload"
                variant="primary"
                isLoading={isUploading}
                isDisabled={!canSubmit}
                onClick={() => void submit()}
              />
            </HStack>
          </LayoutFooter>
        }
      />
    </Dialog>
  );
}
