"use client";

import { useEffect, useState } from "react";
import { useAll, useDb } from "jazz-tools/react";
import { Button, Item, ProgressBar, Skeleton, Text, VStack } from "@astryxdesign/core";
import { app } from "@/schema";

/** Images up to this size render inline; anything larger downloads on demand. */
const INLINE_IMAGE_LIMIT = 8 * 1024 * 1024;
/** Downloads read the large value one page at a time with a typed range selection. */
const DOWNLOAD_PAGE = 512 * 1024;

export function AttachmentBlock({
  attachmentId,
  kind,
}: {
  attachmentId: string;
  kind: "image" | "file";
}) {
  // Metadata only: listing a page never pulls attachment bytes.
  const { data } = useAll(
    app.attachments.where({ id: attachmentId }).select("name", "mimeType", "byteLength"),
  );
  const meta = data?.[0];
  if (!data) return <Skeleton height={48} width="100%" />;
  if (!meta) return <Text type="supporting">This attachment is not available to you.</Text>;
  if (kind === "image" && meta.byteLength <= INLINE_IMAGE_LIMIT)
    return <InlineImage attachmentId={attachmentId} name={meta.name} mimeType={meta.mimeType} />;
  return (
    <FileRow
      attachmentId={attachmentId}
      name={meta.name}
      mimeType={meta.mimeType}
      byteLength={meta.byteLength}
    />
  );
}

function InlineImage({
  attachmentId,
  name,
  mimeType,
}: {
  attachmentId: string;
  name: string;
  mimeType: string;
}) {
  const { data } = useAll(app.attachments.where({ id: attachmentId }).select("bytes"));
  const bytes = data?.[0]?.bytes;
  const [url, setUrl] = useState<string | null>(null);
  useEffect(() => {
    if (!bytes) return;
    const objectUrl = URL.createObjectURL(new Blob([bytes as BlobPart], { type: mimeType }));
    setUrl(objectUrl);
    return () => URL.revokeObjectURL(objectUrl);
  }, [bytes, mimeType]);
  if (!url) return <Skeleton height={240} width="100%" />;
  return (
    <VStack gap={1}>
      <img className="bb-image" src={url} alt={name} />
      <Text type="supporting">{name}</Text>
    </VStack>
  );
}

function FileRow({
  attachmentId,
  name,
  mimeType,
  byteLength,
}: {
  attachmentId: string;
  name: string;
  mimeType: string;
  byteLength: number;
}) {
  const db = useDb();
  const [received, setReceived] = useState<number | null>(null);

  async function download() {
    const chunks: Uint8Array[] = [];
    setReceived(0);
    for (let from = 0; from < byteLength; from += DOWNLOAD_PAGE) {
      const to = Math.min(byteLength, from + DOWNLOAD_PAGE);
      const [page] = await db.all(
        app.attachments.where({ id: attachmentId }).select({ bytes: { from, to } }),
      );
      if (!page) throw new Error("Attachment is no longer available");
      chunks.push(page.bytes as Uint8Array);
      setReceived(to);
    }
    const url = URL.createObjectURL(new Blob(chunks as BlobPart[], { type: mimeType }));
    const link = document.createElement("a");
    link.href = url;
    link.download = name;
    link.click();
    URL.revokeObjectURL(url);
    setReceived(null);
  }

  return (
    <VStack gap={1}>
      <Item
        label={name}
        description={`${formatBytes(byteLength)} · ${mimeType}`}
        endContent={
          <Button label="Download" size="sm" isLoading={received !== null} clickAction={download} />
        }
      />
      {received !== null && byteLength > DOWNLOAD_PAGE && (
        <ProgressBar label="Downloading" value={Math.round((received / byteLength) * 100)} />
      )}
    </VStack>
  );
}

export function formatBytes(bytes: number): string {
  if (bytes < 1024) return `${bytes} B`;
  if (bytes < 1024 * 1024) return `${(bytes / 1024).toFixed(1)} KB`;
  return `${(bytes / 1024 / 1024).toFixed(1)} MB`;
}
