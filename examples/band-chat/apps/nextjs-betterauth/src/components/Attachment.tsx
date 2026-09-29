"use client";

import { useState } from "react";
import { useAll, useDb } from "jazz-tools/react";
import { Skeleton, Token, useLightbox } from "@astryxdesign/core";
import { app } from "../../schema";
import { downloadBytes, formatBytes, isAudioType, isImageType } from "../lib/attachments";
import { useObjectUrl } from "../lib/use-object-url";
import type { MessageSummary } from "./RoomView";

/** Reads one message's attachment bytes, only while the attachment is on screen. */
function useAttachmentBytes(messageId: string): Uint8Array | undefined {
  const { data } = useAll(app.messages.where({ id: messageId }).select("attachment"));
  return data?.[0]?.attachment ?? undefined;
}

export function Attachment({ message }: { message: MessageSummary }) {
  if (isImageType(message.attachmentType)) return <ImageAttachment message={message} />;
  if (isAudioType(message.attachmentType)) return <AudioAttachment message={message} />;
  return <FileAttachment message={message} />;
}

function ImageAttachment({ message }: { message: MessageSummary }) {
  const bytes = useAttachmentBytes(message.id);
  const src = useObjectUrl(bytes, message.attachmentType);
  const name = message.attachmentName ?? "Image";
  const lightbox = useLightbox({ media: { src: src ?? "", alt: name, caption: name } });
  if (!src) return <Skeleton width={240} height={160} />;
  return (
    <>
      <button type="button" className="attachment-image" onClick={() => lightbox.open()}>
        <img src={src} alt={name} />
      </button>
      {lightbox.element}
    </>
  );
}

function AudioAttachment({ message }: { message: MessageSummary }) {
  const bytes = useAttachmentBytes(message.id);
  const src = useObjectUrl(bytes, message.attachmentType);
  if (!src) return <Skeleton width={240} height={40} />;
  return (
    // eslint-disable-next-line jsx-a11y/media-has-caption -- user-shared audio has no captions
    <audio className="attachment-audio" controls src={src} aria-label={message.attachmentName ?? "Audio"} />
  );
}

function FileAttachment({ message }: { message: MessageSummary }) {
  const db = useDb();
  const [isLoading, setLoading] = useState(false);
  const name = message.attachmentName ?? "Attachment";
  async function download() {
    setLoading(true);
    try {
      const row = await db.one(app.messages.where({ id: message.id }).select("attachment"));
      if (row?.attachment) downloadBytes(row.attachment, name, message.attachmentType);
    } finally {
      setLoading(false);
    }
  }
  return (
    <Token
      label={name}
      description={isLoading ? "Downloading…" : `${formatBytes(message.attachmentSize)} · Download`}
      aria-label={`Download ${name}`}
      onClick={() => void download()}
    />
  );
}
