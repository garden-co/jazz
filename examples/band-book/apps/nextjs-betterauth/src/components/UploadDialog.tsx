"use client";

import { useState } from "react";
import { useDb } from "jazz-tools/react";
import {
  Banner,
  Button,
  Dialog,
  FileInput,
  Heading,
  HStack,
  ProgressBar,
  Text,
  VStack,
} from "@astryxdesign/core";
import { app } from "@/schema";
import { useWorkspace } from "./workspace-context";

/**
 * Upload an image or file into a page. The bytes are streamed into a large
 * value with `insertStreaming`, so a big file is never held in one buffer, and
 * the block that shows it is added once the upload is stored.
 */
export function UploadDialog({
  pageId,
  position,
  onClose,
}: {
  pageId: string;
  position: number;
  onClose: () => void;
}) {
  const db = useDb();
  const { workspace } = useWorkspace();
  const [file, setFile] = useState<File | null>(null);
  const [sent, setSent] = useState<number | null>(null);
  const [error, setError] = useState<string | null>(null);

  async function upload() {
    if (!file) return;
    setError(null);
    setSent(0);
    try {
      let bytesSent = 0;
      const counted = file.stream().pipeThrough(
        new TransformStream<Uint8Array, Uint8Array>({
          transform(chunk, controller) {
            bytesSent += chunk.byteLength;
            setSent(bytesSent);
            controller.enqueue(chunk);
          },
        }),
      );
      const stored = await db.insertStreaming(app.attachments, {
        workspaceId: workspace.id,
        pageId,
        name: file.name,
        mimeType: file.type || "application/octet-stream",
        byteLength: file.size,
        bytes: counted,
      });
      // A transaction cannot stream a large value, so the attachment is stored
      // first and the block that shows it follows. If the authority refuses the
      // block, the attachment is removed again rather than left unreferenced.
      const block = db.insert(app.blocks, {
        workspaceId: workspace.id,
        pageId,
        parentBlockId: null,
        position,
        kind: file.type.startsWith("image/") ? "image" : "file",
        text: file.name,
        checked: false,
        attachmentId: stored.value.id,
      });
      block.wait({ tier: "edge" }).catch(() => db.delete(app.attachments, stored.value.id));
      onClose();
    } catch (cause) {
      setSent(null);
      setError(cause instanceof Error ? cause.message : String(cause));
    }
  }

  return (
    <Dialog
      isOpen
      onOpenChange={(open) => !open && sent === null && onClose()}
      purpose="form"
      padding={4}
      width={480}
    >
      <VStack gap={4}>
        <VStack gap={1}>
          <Heading level={2}>Add an image or file</Heading>
          <Text type="supporting">
            Stage plots, rider PDFs, demo recordings. Images show inline.
          </Text>
        </VStack>
        <FileInput
          label="File"
          isLabelHidden
          mode="dropzone"
          value={file}
          onChange={(value) => setFile(Array.isArray(value) ? (value[0] ?? null) : value)}
          isDisabled={sent !== null}
        />
        {sent !== null && file && (
          <ProgressBar
            label="Uploading"
            value={file.size ? Math.round((sent / file.size) * 100) : 100}
          />
        )}
        {error && (
          <Banner status="error" title="Upload failed" description={error} collapsible={false} />
        )}
        <HStack gap={2} justify="end">
          <Button label="Cancel" variant="ghost" onClick={onClose} isDisabled={sent !== null} />
          <Button
            label="Upload"
            variant="primary"
            isDisabled={!file}
            isLoading={sent !== null}
            onClick={upload}
          />
        </HStack>
      </VStack>
    </Dialog>
  );
}
