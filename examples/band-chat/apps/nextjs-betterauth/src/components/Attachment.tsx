"use client";

import { useEffect, useRef, useState } from "react";
import { useAll, useDb } from "jazz-tools/react";
import { Skeleton, Thumbnail, Token, useLightbox } from "@astryxdesign/core";
import { app } from "../../schema";
import { downloadBytes, formatBytes, isAudioType, isImageType } from "../lib/attachments";
import { useObjectUrl } from "../lib/use-object-url";
import type { MessageSummary } from "./RoomView";

/**
 * True once the element has come near the viewport, and from then on. The
 * timeline renders up to a page of messages, so attachment bytes are fetched
 * only for attachments someone scrolls to, never for the whole page on mount.
 */
function useNearViewport<T extends Element>() {
  const ref = useRef<T>(null);
  const [isNear, setNear] = useState(false);
  useEffect(() => {
    const element = ref.current;
    if (isNear || !element) return;
    if (typeof IntersectionObserver === "undefined") {
      setNear(true);
      return;
    }
    const observer = new IntersectionObserver(
      (entries) => {
        if (entries.some((entry) => entry.isIntersecting)) setNear(true);
      },
      { rootMargin: "200px" },
    );
    observer.observe(element);
    return () => observer.disconnect();
  }, [isNear]);
  return [ref, isNear] as const;
}

/** One message's attachment bytes, queried only once `isEnabled` turns true. */
function useAttachmentBytes(messageId: string, isEnabled: boolean): Uint8Array | undefined {
  const { data } = useAll(
    isEnabled ? app.messages.where({ id: messageId }).select("attachment") : undefined,
  );
  return data?.[0]?.attachment ?? undefined;
}

export function Attachment({ message }: { message: MessageSummary }) {
  if (isImageType(message.attachmentType)) return <ImageAttachment message={message} />;
  if (isAudioType(message.attachmentType)) return <AudioAttachment message={message} />;
  return <FileAttachment message={message} />;
}

function ImageAttachment({ message }: { message: MessageSummary }) {
  const [ref, isNear] = useNearViewport<HTMLDivElement>();
  const bytes = useAttachmentBytes(message.id, isNear);
  const src = useObjectUrl(bytes, message.attachmentType);
  const name = message.attachmentName ?? "Image";
  const lightbox = useLightbox({ media: { src: src ?? "", alt: name, caption: name } });
  return (
    <div ref={ref}>
      <Thumbnail
        src={src}
        alt={name}
        label={name}
        isLoading={!src}
        onClick={src ? () => lightbox.open() : undefined}
      />
      {lightbox.element}
    </div>
  );
}

function AudioAttachment({ message }: { message: MessageSummary }) {
  const [ref, isNear] = useNearViewport<HTMLDivElement>();
  const bytes = useAttachmentBytes(message.id, isNear);
  const src = useObjectUrl(bytes, message.attachmentType);
  return (
    <div ref={ref}>
      {src ? (
        // eslint-disable-next-line jsx-a11y/media-has-caption -- user-shared audio has no captions
        <audio
          className="attachment-audio"
          controls
          src={src}
          aria-label={message.attachmentName ?? "Audio"}
        />
      ) : (
        <Skeleton width={240} height={40} />
      )}
    </div>
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
